module Hasura.Eventing.HTTP
  ( HTTPErr(..)
  , HTTPResp(..)
  , tryWebhook
  , runHTTP
  , isNetworkError
  , isNetworkErrorHC
  , logHTTPForET
  , logHTTPForST
  , ExtraLogContext(..)
  , EventId
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
  , mkWebhookReq
  , mkResp
  , LogEnvHeaders
  , encodeHeader
  , decodeHeader
  , getRetryAfterHeaderFromHTTPErr
  , getRetryAfterHeaderFromResp
  , parseRetryHeaderValue
  ) where

import qualified Data.Aeson                    as J
import qualified Data.Aeson.Casing             as J
import qualified Data.Aeson.TH                 as J
import qualified Data.ByteString               as BS
import qualified Data.ByteString.Lazy          as LBS
import qualified Data.CaseInsensitive          as CI
import qualified Data.TByteString              as TBS
import qualified Data.Text                     as T
import qualified Data.Text.Encoding            as TE
import qualified Data.Text.Encoding.Error      as TE
import qualified Hasura.Logging                as L
import qualified Network.HTTP.Client           as HTTP
import qualified Network.HTTP.Types            as HTTP

import           Control.Exception             (try)
import           Control.Monad.IO.Class        (MonadIO, liftIO)
import           Control.Monad.Reader          (MonadReader)
import           Data.Either
import           Data.Has
import           Hasura.Logging
import           Hasura.Prelude
import           Hasura.RQL.DDL.Headers
import           Hasura.RQL.Types.EventTrigger

type LogEnvHeaders = Bool

retryAfterHeader :: CI.CI T.Text
retryAfterHeader = "Retry-After"

data WebhookRequest
  = WebhookRequest
  { _rqPayload :: J.Value
  , _rqHeaders :: [HeaderConf]
  , _rqVersion :: T.Text
  }
$(J.deriveToJSON (J.aesonDrop 3 J.snakeCase){J.omitNothingFields=True} ''WebhookRequest)

data WebhookResponse
  = WebhookResponse
  { _wrsBody    :: TBS.TByteString
  , _wrsHeaders :: [HeaderConf]
  , _wrsStatus  :: Int
  }
$(J.deriveToJSON (J.aesonDrop 4 J.snakeCase){J.omitNothingFields=True} ''WebhookResponse)

newtype ClientError =  ClientError { _ceMessage :: TBS.TByteString}
$(J.deriveToJSON (J.aesonDrop 3 J.snakeCase){J.omitNothingFields=True} ''ClientError)

type Version = T.Text

-- | There are two types of events: Event (for event triggers) and Scheduled (for scheduled triggers)
data TriggerTypes = Event | Scheduled

data Response = ResponseHTTP WebhookResponse | ResponseError ClientError

instance J.ToJSON Response where
  toJSON (ResponseHTTP resp) = J.object
    [ "type" J..= J.String "webhook_response"
    , "data" J..= J.toJSON resp
    ]
  toJSON (ResponseError err) = J.object
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

data ExtraLogContext
  = ExtraLogContext
  { elcEventId        :: EventId
  } deriving (Show, Eq)

$(J.deriveJSON (J.aesonDrop 3 J.snakeCase){J.omitNothingFields=True} ''ExtraLogContext)

data HTTPResp (a :: TriggerTypes)
   = HTTPResp
   { hrsStatus  :: !Int
   , hrsHeaders :: ![HeaderConf]
   , hrsBody    :: !TBS.TByteString
   } deriving (Show, Eq)

$(J.deriveToJSON (J.aesonDrop 3 J.snakeCase){J.omitNothingFields=True} ''HTTPResp)

instance ToEngineLog (HTTPResp 'Event) Hasura where
  toEngineLog resp = (LevelInfo, eventTriggerLogType, J.toJSON resp)

instance ToEngineLog (HTTPResp 'Scheduled) Hasura where
  toEngineLog resp = (LevelInfo, scheduledTriggerLogType, J.toJSON resp)

data HTTPErr (a :: TriggerTypes)
  = HClient !HTTP.HttpException
  | HParse !HTTP.Status !String
  | HStatus !(HTTPResp a)
  | HOther !String
  deriving (Show)

instance J.ToJSON (HTTPErr a) where
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

instance ToEngineLog (HTTPErr 'Event) Hasura where
  toEngineLog err = (LevelError, eventTriggerLogType, J.toJSON err)

instance ToEngineLog (HTTPErr 'Scheduled) Hasura where
  toEngineLog err = (LevelError, scheduledTriggerLogType, J.toJSON err)

mkHTTPResp :: HTTP.Response LBS.ByteString -> HTTPResp a
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

data HTTPRespExtra (a :: TriggerTypes)
  = HTTPRespExtra
  { _hreResponse :: Either (HTTPErr a) (HTTPResp a)
  , _hreContext  :: Maybe ExtraLogContext
  }

instance J.ToJSON (HTTPRespExtra 'Scheduled) where
  toJSON (HTTPRespExtra resp ctxt) = do
    case resp of
      Left errResp -> J.object ["response" J..= J.toJSON errResp, "context" J..= J.toJSON ctxt]
      Right rsp -> J.object ["response" J..= J.toJSON rsp, "context" J..= J.toJSON ctxt]

instance J.ToJSON (HTTPRespExtra 'Event) where
  toJSON (HTTPRespExtra resp ctxt) = do
    case resp of
      Left errResp -> J.object ["response" J..= J.toJSON errResp, "context" J..= J.toJSON ctxt]
      Right rsp -> J.object ["response" J..= J.toJSON rsp, "context" J..= J.toJSON ctxt]

instance ToEngineLog (HTTPRespExtra 'Event) Hasura where
  toEngineLog resp = (LevelInfo, eventTriggerLogType, J.toJSON resp)

instance ToEngineLog (HTTPRespExtra 'Scheduled) Hasura where
  toEngineLog resp = (LevelInfo, scheduledTriggerLogType, J.toJSON resp)

isNetworkError :: HTTPErr a -> Bool
isNetworkError = \case
  HClient he -> isNetworkErrorHC he
  _          -> False

isNetworkErrorHC :: HTTP.HttpException -> Bool
isNetworkErrorHC = \case
  HTTP.HttpExceptionRequest _ (HTTP.ConnectionFailure _) -> True
  HTTP.HttpExceptionRequest _ HTTP.ConnectionTimeout -> True
  HTTP.HttpExceptionRequest _ HTTP.ResponseTimeout -> True
  _ -> False

anyBodyParser :: HTTP.Response LBS.ByteString -> Either (HTTPErr a) (HTTPResp a)
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

logHTTPForET
  :: ( MonadReader r m
     , Has (Logger Hasura) r
     , MonadIO m
     )
  => Either (HTTPErr 'Event) (HTTPResp 'Event) -> Maybe ExtraLogContext -> m ()
logHTTPForET eitherResp extraLogCtx = do
  logger :: Logger Hasura <- asks getter
  unLogger logger $ HTTPRespExtra eitherResp extraLogCtx

logHTTPForST
  :: ( MonadReader r m
     , Has (Logger Hasura) r
     , MonadIO m
     )
  => Either (HTTPErr 'Scheduled) (HTTPResp 'Scheduled) -> Maybe ExtraLogContext -> m ()
logHTTPForST eitherResp extraLogCtx = do
  logger :: Logger Hasura <- asks getter
  unLogger logger $ HTTPRespExtra eitherResp extraLogCtx

runHTTP :: (MonadIO m) => HTTP.Manager -> HTTP.Request -> m (Either (HTTPErr a) (HTTPResp a))
runHTTP manager req = do
  res <- liftIO $ try $ HTTP.httpLbs req manager
  return $ either (Left . HClient) anyBodyParser res

tryWebhook ::
  ( MonadReader r m
  , Has HTTP.Manager r
  , MonadIO m
  , MonadError (HTTPErr a) m
  )
  => [HTTP.Header]
  -> HTTP.ResponseTimeout
  -> J.Value
  -> String
  -> m (HTTPResp a)
tryWebhook headers timeout payload webhook = do
  initReqE <- liftIO $ try $ HTTP.parseRequest webhook
  manager <- asks getter
  case initReqE of
    Left excp -> throwError $ HClient excp
    Right initReq -> do
      let req =
            initReq
              { HTTP.method = "POST"
              , HTTP.requestHeaders = headers
              , HTTP.requestBody = HTTP.RequestBodyLBS (J.encode payload)
              , HTTP.responseTimeout = timeout
              }
      eitherResp <- runHTTP manager req
      onLeft eitherResp throwError

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
  let wr = WebhookResponse payload headers status
  in ResponseHTTP wr

mkClientErr :: TBS.TByteString -> Response
mkClientErr message =
  let cerr = ClientError message
  in ResponseError cerr

mkWebhookReq :: J.Value -> [HeaderConf] -> Version -> WebhookRequest
mkWebhookReq payload headers = WebhookRequest payload headers

isClientError :: Int -> Bool
isClientError status = status >= 1000

encodeHeader :: EventHeaderInfo -> HTTP.Header
encodeHeader (EventHeaderInfo hconf cache) =
  let (HeaderConf name _) = hconf
      ciname = CI.mk $ TE.encodeUtf8 name
      value = TE.encodeUtf8 cache
   in (ciname, value)

decodeHeader
  :: LogEnvHeaders -> [EventHeaderInfo] -> (HTTP.HeaderName, BS.ByteString)
  -> HeaderConf
decodeHeader logenv headerInfos (hdrName, hdrVal)
  = let name = decodeBS $ CI.original hdrName
        getName ehi = let (HeaderConf name' _) = ehiHeaderConf ehi
                      in name'
        mehi = find (\hi -> getName hi == name) headerInfos
    in case mehi of
         Nothing -> HeaderConf name (HVValue (decodeBS hdrVal))
         Just ehi -> if logenv
                     then HeaderConf name (HVValue (ehiCachedValue ehi))
                     else ehiHeaderConf ehi
   where
     decodeBS = TE.decodeUtf8With TE.lenientDecode

getRetryAfterHeaderFromHTTPErr :: HTTPErr a -> Maybe Text
getRetryAfterHeaderFromHTTPErr (HStatus resp) = getRetryAfterHeaderFromResp resp
getRetryAfterHeaderFromHTTPErr _              = Nothing

getRetryAfterHeaderFromResp :: HTTPResp a -> Maybe Text
getRetryAfterHeaderFromResp resp =
  let mHeader =
        find
          (\(HeaderConf name _) -> CI.mk name == retryAfterHeader)
          (hrsHeaders resp)
   in case mHeader of
        Just (HeaderConf _ (HVValue value)) -> Just value
        _                                   -> Nothing

parseRetryHeaderValue :: T.Text -> Maybe Int
parseRetryHeaderValue hValue =
  let seconds = readMaybe $ T.unpack hValue
   in case seconds of
        Nothing -> Nothing
        Just sec ->
          if sec > 0
            then Just sec
            else Nothing
