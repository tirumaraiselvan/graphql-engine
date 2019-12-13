module Hasura.Eventing.HTTP
  ( HTTPErr(..)
  , HTTPResp(..)
  , runHTTP
  , isNetworkError
  , isNetworkErrorHC
  , ExtraContext(..)
  , Invocation(..)
  , Version
  , Response(..)
  , WebhookRequest(..)
  , WebhookResponse(..)
  , ClientError(..)
  , isClientError
  , mkClientErr
  , TriggerMetadata(..)
  , DeliveryInfo(..)
  , logQErr
  , logHTTPErr
  , EventInternalErr(..)
  , mkWebhookReq
  , mkResp
  ) where

import           Data.Either

import qualified Data.Aeson                    as J
import qualified Data.Aeson.Casing             as J
import qualified Data.Aeson.TH                 as J
import qualified Data.ByteString.Lazy          as B
import qualified Data.CaseInsensitive          as CI
import qualified Data.TByteString              as TBS
import qualified Data.Text                     as T
import qualified Data.Text.Encoding            as TE
import qualified Data.Text.Encoding.Error      as TE
import qualified Data.Time.Clock               as Time
import qualified Hasura.Logging                as L
import qualified Network.HTTP.Client           as HTTP
import qualified Network.HTTP.Types            as HTTP

import           Control.Exception             (try)
import           Control.Monad.IO.Class        (MonadIO, liftIO)
import           Control.Monad.Reader          (MonadReader)
import           Data.Has
import           Hasura.Logging
import           Hasura.Prelude
import           Hasura.RQL.DDL.Headers
import           Hasura.RQL.Types.Error        (QErr)
import           Hasura.RQL.Types.EventTrigger

data WebhookRequest
  = WebhookRequest
  { _rqPayload :: J.Value
  , _rqHeaders :: Maybe [HeaderConf]
  , _rqVersion :: T.Text
  }
$(J.deriveToJSON (J.aesonDrop 3 J.snakeCase){J.omitNothingFields=True} ''WebhookRequest)

data WebhookResponse
  = WebhookResponse
  { _wrsBody    :: TBS.TByteString
  , _wrsHeaders :: Maybe [HeaderConf]
  , _wrsStatus  :: Int
  }
$(J.deriveToJSON (J.aesonDrop 4 J.snakeCase){J.omitNothingFields=True} ''WebhookResponse)

newtype ClientError =  ClientError { _ceMessage :: TBS.TByteString}
$(J.deriveToJSON (J.aesonDrop 3 J.snakeCase){J.omitNothingFields=True} ''ClientError)

type Version = T.Text

data Response = ResponseType1 WebhookResponse | ResponseType2 ClientError

instance J.ToJSON Response where
  toJSON (ResponseType1 resp) = J.object
    [ "type" J..= J.String "webhook_response"
    , "data" J..= J.toJSON resp
    ]
  toJSON (ResponseType2 err ) = J.object
    [ "type" J..= J.String "client_error"
    , "data" J..= J.toJSON err
    ]

data Invocation
  = Invocation
  { iEventId  :: EventId
  , iStatus   :: Int
  , iRequest  :: WebhookRequest
  , iResponse :: Response
  }

data ExtraContext
  = ExtraContext
  { elEventCreatedAt :: Time.UTCTime
  , elEventId        :: EventId
  } deriving (Show, Eq)

$(J.deriveJSON (J.aesonDrop 2 J.snakeCase){J.omitNothingFields=True} ''ExtraContext)

data HTTPResp
   = HTTPResp
   { hrsStatus  :: !Int
   , hrsHeaders :: ![HeaderConf]
   , hrsBody    :: !TBS.TByteString
   } deriving (Show, Eq)

$(J.deriveToJSON (J.aesonDrop 3 J.snakeCase){J.omitNothingFields=True} ''HTTPResp)

instance ToEngineLog HTTPResp Hasura where
  toEngineLog resp = (LevelInfo, eventTriggerLogType, J.toJSON resp)

mkHTTPResp :: HTTP.Response B.ByteString -> HTTPResp
mkHTTPResp resp =
  HTTPResp
  { hrsStatus = HTTP.statusCode $ HTTP.responseStatus resp
  , hrsHeaders = map decodeHeader $ HTTP.responseHeaders resp
  , hrsBody = TBS.fromLBS $ HTTP.responseBody resp
  }
  where
    decodeBS = TE.decodeUtf8With TE.lenientDecode
    decodeHeader (hdrName, hdrVal)
      = HeaderConf (decodeBS $ CI.original hdrName) (HVValue (decodeBS hdrVal))

data HTTPRespExtra
  = HTTPRespExtra
  { _hreResponse :: HTTPResp
  , _hreContext  :: Maybe ExtraContext
  }

$(J.deriveToJSON (J.aesonDrop 4 J.snakeCase){J.omitNothingFields=True} ''HTTPRespExtra)

instance ToEngineLog HTTPRespExtra Hasura where
  toEngineLog resp = (LevelInfo, eventTriggerLogType, J.toJSON resp)

data HTTPErr
  = HClient !HTTP.HttpException
  | HParse !HTTP.Status !String
  | HStatus !HTTPResp
  | HOther !String
  deriving (Show)

instance J.ToJSON HTTPErr where
  toJSON err = toObj $ case err of
    (HClient e) -> ("client", J.toJSON $ show e)
    (HParse st e) ->
      ( "parse"
      , J.toJSON (HTTP.statusCode st,  show e)
      )
    (HStatus resp) ->
      ("status", J.toJSON resp)
    (HOther e) -> ("internal", J.toJSON $ show e)
    where
      toObj :: (T.Text, J.Value) -> J.Value
      toObj (k, v) = J.object [ "type" J..= k
                              , "detail" J..= v]
-- encapsulates a http operation
instance ToEngineLog HTTPErr Hasura where
  toEngineLog err = (LevelError, eventTriggerLogType, J.toJSON err)

isNetworkError :: HTTPErr -> Bool
isNetworkError = \case
  HClient he -> isNetworkErrorHC he
  _          -> False

isNetworkErrorHC :: HTTP.HttpException -> Bool
isNetworkErrorHC = \case
  HTTP.HttpExceptionRequest _ (HTTP.ConnectionFailure _) -> True
  HTTP.HttpExceptionRequest _ HTTP.ConnectionTimeout -> True
  HTTP.HttpExceptionRequest _ HTTP.ResponseTimeout -> True
  _ -> False

anyBodyParser :: HTTP.Response B.ByteString -> Either HTTPErr HTTPResp
anyBodyParser resp = do
  let httpResp = mkHTTPResp resp
  if respCode >= HTTP.status200 && respCode < HTTP.status300
    then return httpResp
  else throwError $ HStatus httpResp
  where
    respCode = HTTP.responseStatus resp

data HTTPReq
  = HTTPReq
  { _hrqMethod  :: !String
  , _hrqUrl     :: !String
  , _hrqPayload :: !(Maybe J.Value)
  , _hrqTry     :: !Int
  , _hrqDelay   :: !(Maybe Int)
  } deriving (Show, Eq)

$(J.deriveJSON (J.aesonDrop 4 J.snakeCase){J.omitNothingFields=True} ''HTTPReq)

instance ToEngineLog HTTPReq Hasura where
  toEngineLog req = (LevelInfo, eventTriggerLogType, J.toJSON req)

runHTTP
  :: ( MonadReader r m
     , Has (Logger Hasura) r
     , MonadIO m
     )
  => HTTP.Manager -> HTTP.Request -> Maybe ExtraContext -> m (Either HTTPErr HTTPResp)
runHTTP manager req exLog = do
  logger :: Logger Hasura <- asks getter
  res <- liftIO $ try $ HTTP.httpLbs req manager
  case res of
    Left e     -> unLogger logger $ HClient e
    Right resp -> unLogger logger $ HTTPRespExtra (mkHTTPResp resp) exLog
  return $ either (Left . HClient) anyBodyParser res

newtype EventInternalErr
  = EventInternalErr QErr
  deriving (Show, Eq)

instance L.ToEngineLog EventInternalErr L.Hasura where
  toEngineLog (EventInternalErr qerr) = (L.LevelError, L.eventTriggerLogType, J.toJSON qerr)

data TriggerMetadata
  = TriggerMetadata { tmName :: TriggerName }
  deriving (Show, Eq)

$(J.deriveJSON (J.aesonDrop 2 J.snakeCase){J.omitNothingFields=True} ''TriggerMetadata)

data DeliveryInfo
  = DeliveryInfo
  { diCurrentRetry :: Int
  , diMaxRetries   :: Int
  } deriving (Show, Eq)

$(J.deriveJSON (J.aesonDrop 2 J.snakeCase){J.omitNothingFields=True} ''DeliveryInfo)

mkResp :: Int -> TBS.TByteString -> [HeaderConf] -> Response
mkResp status payload headers =
  let wr = WebhookResponse payload (mkMaybe headers) status
  in ResponseType1 wr

mkClientErr :: TBS.TByteString -> Response
mkClientErr message =
  let cerr = ClientError message
  in ResponseType2 cerr

mkWebhookReq :: J.Value -> [HeaderConf] -> Version -> WebhookRequest
mkWebhookReq payload headers version = WebhookRequest payload (mkMaybe headers) version

isClientError :: Int -> Bool
isClientError status = status >= 1000

mkMaybe :: [a] -> Maybe [a]
mkMaybe [] = Nothing
mkMaybe x  = Just x

logQErr :: ( MonadReader r m, Has (L.Logger L.Hasura) r, MonadIO m) => QErr -> m ()
logQErr err = do
  logger :: L.Logger L.Hasura <- asks getter
  L.unLogger logger $ EventInternalErr err

logHTTPErr
  :: ( MonadReader r m
     , Has (L.Logger L.Hasura) r
     , MonadIO m
     )
  => HTTPErr -> m ()
logHTTPErr err = do
  logger :: L.Logger L.Hasura <- asks getter
  L.unLogger logger err
