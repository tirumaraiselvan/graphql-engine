module Hasura.Eventing.ScheduledTrigger
  ( processScheduledQueue
  , runScheduledEventsGenerator
  ) where

import           Control.Concurrent              (threadDelay)
import           Control.Exception               (try)
import           Data.Has
import           Data.IORef                      (IORef, readIORef)
import           Data.Time.Clock
import           Hasura.Eventing.HTTP
import           Hasura.Prelude
import           Hasura.RQL.DDL.Headers
import           Hasura.RQL.Types.ScheduledTrigger
import           Hasura.RQL.Types
import           Hasura.SQL.DML
import           Hasura.SQL.Types
import           System.Cron
import           Hasura.HTTP

import qualified Data.Aeson                      as J
import qualified Data.Aeson.Casing               as J
import qualified Data.Aeson.TH                   as J
import qualified Data.ByteString.Lazy            as LBS
import qualified Data.TByteString                as TBS
import qualified Data.Text                       as T
import qualified Data.Text.Encoding              as TE
import qualified Database.PG.Query               as Q
import qualified Hasura.Logging                  as L
import qualified Network.HTTP.Client             as HTTP
import qualified Network.HTTP.Types              as HTTP
import qualified Text.Builder                    as TB (run)
import qualified Data.HashMap.Strict             as Map

import           Debug.Trace

invocationVersion :: Version
invocationVersion = "1"

oneSecond :: Int
oneSecond = 1000000

oneMinute :: Int
oneMinute = 60 * oneSecond

oneHour :: Int
oneHour = 60 * oneMinute

type ScheduledEventPayload = J.Value

scheduledEventsTable :: QualifiedTable
scheduledEventsTable =
  QualifiedObject
    hdbCatalogSchema
    (TableName $ T.pack "hdb_scheduled_events")

data ScheduledEvent
  = ScheduledEvent
  { seId            :: !(Maybe Text)
  , seName          :: !TriggerName
  , seWebhook       :: !T.Text
  , sePayload       :: !J.Value
  , seScheduledTime :: !UTCTime
  } deriving (Show, Eq)

$(J.deriveToJSON (J.aesonDrop 2 J.snakeCase){J.omitNothingFields=True} ''ScheduledEvent)

runScheduledEventsGenerator ::
     L.Logger L.Hasura
  -> Q.PGPool
  -> IORef (SchemaCache, SchemaCacheVer)
  -> IO ()
runScheduledEventsGenerator logger pgpool scRef = do
  forever $ do
    traceM "entering scheduled events generator"
    (sc, _) <- liftIO $ readIORef scRef
    let scheduledTriggers = Map.elems $ scScheduledTriggers sc
    runExceptT
      (Q.runTx
         pgpool
         (Q.ReadCommitted, Just Q.ReadWrite)
         (insertScheduledEventsFor scheduledTriggers) ) >>= \case
      Right _ -> pure ()
      Left err ->
        L.unLogger logger $ EventInternalErr $ err500 Unexpected (T.pack $ show err)
    threadDelay (10 * oneSecond)

insertScheduledEventsFor :: [ScheduledTriggerInfo] -> Q.TxE QErr ()
insertScheduledEventsFor scheduledTriggers = do
  currentTime <- liftIO getCurrentTime
  let scheduledEvents = concatMap (generateScheduledEventsFrom currentTime) scheduledTriggers
  case scheduledEvents of
    []     -> pure ()
    events -> do
      let insertScheduledEventsSql = TB.run $ toSQL
            SQLInsert
              { siTable    = scheduledEventsTable
              , siCols     = map (PGCol . T.pack) ["name", "webhook", "payload", "scheduled_time"]
              , siValues   = ValuesExp $ map (toTupleExp . toArr) events
              , siConflict = Just $ DoNothing Nothing
              , siRet      = Nothing
              }
      Q.unitQE defaultTxErrorHandler (Q.fromText insertScheduledEventsSql) () False
  where
    toArr (ScheduledEvent _ n w p t) =
      (triggerNameToTxt n) : w : (TE.decodeUtf8 . LBS.toStrict $ J.encode p) : (pure $ formatTime' t)
    toTupleExp = TupleExp . map SELit

generateScheduledEventsFrom :: UTCTime -> ScheduledTriggerInfo-> [ScheduledEvent]
generateScheduledEventsFrom time ScheduledTriggerInfo{..} =
  let events =
        case stiSchedule of
          OneOff _ -> empty -- one-off scheduled events are generated during creation
          Cron cron ->
            generateScheduledEventsBetween
              time
              (addUTCTime nominalDay time)
              cron
      webhook = wciCachedValue stiWebhookInfo
   in map
      (ScheduledEvent Nothing stiName webhook (fromMaybe J.Null stiPayload))
      events

-- generates events (from, till] according to CronSchedule
generateScheduledEventsBetween :: UTCTime -> UTCTime -> CronSchedule -> [UTCTime]
generateScheduledEventsBetween from till cron = takeWhile ((>=) till) $ go from
  where
    go init =
      case nextMatch cron init of
        Nothing   -> []
        Just next -> next : (go next)

processScheduledQueue ::
     L.Logger L.Hasura
  -> Q.PGPool
  -> HTTP.Manager
  -> IORef (SchemaCache, SchemaCacheVer)
  -> IO ()
processScheduledQueue logger pgpool httpMgr scRef =
  forever $ do
    traceM "entering processor queue"
    (sc, _) <- liftIO $ readIORef scRef
    let scheduledTriggersInfo = scScheduledTriggers sc
    scheduledEventsE <-
      runExceptT $
      Q.runTx pgpool (Q.ReadCommitted, Just Q.ReadWrite) getScheduledEvents
    case scheduledEventsE of
      Right events ->
        sequence_ $
        flip map events $ \ev -> do
          let st' = Map.lookup (seName ev) scheduledTriggersInfo
          case st' of
            Nothing -> traceM "ERROR: couldn't find scheduled trigger in cache"
            Just st -> runReaderT (processScheduledEvent pgpool httpMgr st ev) logger
      Left err -> traceShowM err
    threadDelay (10 * oneSecond)

processScheduledEvent ::
     (MonadReader r m, Has (L.Logger L.Hasura) r, MonadIO m)
  => Q.PGPool
  -> HTTP.Manager
  -> ScheduledTriggerInfo
  -> ScheduledEvent
  -> m ()
processScheduledEvent pgpool httpMgr ScheduledTriggerInfo {..} se@ScheduledEvent {..} = do
  currentTime <- liftIO getCurrentTime
  if diffUTCTime currentTime seScheduledTime > rcstTolerance stiRetryConf
    then processDead'
    else do
      let webhook = wciCachedValue stiWebhookInfo
          timeoutSeconds = rcstTimeoutSec stiRetryConf
          responseTimeout = HTTP.responseTimeoutMicro (timeoutSeconds * 1000000)
          headers = map encodeHeader stiHeaders
          headers' = addDefaultHeaders headers
      res <-
        runExceptT $
        tryWebhook httpMgr responseTimeout headers' sePayload webhook
    -- let decodedHeaders = map (decodeHeader logenv headerInfos) headers
      finally <- either (processError pgpool se) (processSuccess pgpool se) res
      either logQErr return finally
  where
    processDead' =
      processDead pgpool se >>= \case
        Left err -> logQErr err
        Right _ -> pure ()

tryWebhook ::
     ( MonadReader r m
     , Has (L.Logger L.Hasura) r
     , MonadIO m
     , MonadError HTTPErr m
     )
  => HTTP.Manager
  -> HTTP.ResponseTimeout
  -> [HTTP.Header]
  -> ScheduledEventPayload
  -> T.Text
  -> m HTTPResp
tryWebhook httpMgr timeout headers payload webhook = do
  initReqE <- liftIO $ try $ HTTP.parseRequest (T.unpack webhook)
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
      eitherResp <- runHTTP httpMgr req Nothing
      onLeft eitherResp throwError

processError :: (MonadIO m) => Q.PGPool -> ScheduledEvent -> HTTPErr -> m (Either QErr ())
processError pgpool se err = do
  let decodedHeaders = []
      invocation = case err of
       HClient excp -> do
         let errMsg = TBS.fromLBS $ J.encode $ show excp
         mkInvo se 1000 decodedHeaders errMsg []
       HParse _ detail -> do
         let errMsg = TBS.fromLBS $ J.encode detail
         mkInvo se 1001 decodedHeaders errMsg []
       HStatus errResp -> do
         let respPayload = hrsBody errResp
             respHeaders = hrsHeaders errResp
             respStatus = hrsStatus errResp
         mkInvo se respStatus decodedHeaders respPayload respHeaders
       HOther detail -> do
         let errMsg = (TBS.fromLBS $ J.encode detail)
         mkInvo se 500 decodedHeaders errMsg []
  liftIO $
    runExceptT $
    Q.runTx pgpool (Q.RepeatableRead, Just Q.ReadWrite) $ do
      insertInvocation invocation
      markError
  where
    markError =
      Q.unitQE
        defaultTxErrorHandler
        [Q.sql|
        UPDATE hdb_catalog.hdb_scheduled_events
        SET error = 't', locked = 'f'
        WHERE id = $1
      |] (Identity $ seId se) True

processSuccess :: (MonadIO m) => Q.PGPool -> ScheduledEvent -> HTTPResp -> m (Either QErr ())
processSuccess pgpool se resp = do
  let respBody = hrsBody resp
      respHeaders = hrsHeaders resp
      respStatus = hrsStatus resp
      decodedHeaders = []
      invocation = mkInvo se respStatus decodedHeaders respBody respHeaders
  liftIO $
    runExceptT $
    Q.runTx pgpool (Q.RepeatableRead, Just Q.ReadWrite) $ do
      insertInvocation invocation
      markSuccess
  where
    markSuccess =
      Q.unitQE
        defaultTxErrorHandler
        [Q.sql|
        UPDATE hdb_catalog.hdb_scheduled_events
        SET delivered = 't', locked = 'f'
        WHERE id = $1
      |] (Identity $ seId se) True

processDead :: (MonadIO m) => Q.PGPool -> ScheduledEvent -> m (Either QErr ())
processDead pgpool se =
  liftIO $
  runExceptT $ Q.runTx pgpool (Q.RepeatableRead, Just Q.ReadWrite) markDead
  where
    markDead =
      Q.unitQE
        defaultTxErrorHandler
        [Q.sql|
          UPDATE hdb_catalog.hdb_scheduled_events
          SET dead = 't', locked = 'f'
          WHERE id = $1
        |] (Identity $ seId se) False

mkInvo
  :: ScheduledEvent -> Int -> [HeaderConf] -> TBS.TByteString -> [HeaderConf]
  -> Invocation
mkInvo se status reqHeaders respBody respHeaders
  = let resp = if isClientError status
          then mkClientErr respBody
          else mkResp status respBody respHeaders
    in
      Invocation
      (fromMaybe "unknown" $ seId se) -- WARN: should never happen?
      status
      (mkWebhookReq (J.toJSON se) reqHeaders invocationVersion)
      resp

insertInvocation :: Invocation -> Q.TxE QErr ()
insertInvocation invo = do
  Q.unitQE defaultTxErrorHandler [Q.sql|
          INSERT INTO hdb_catalog.hdb_scheduled_event_invocation_logs
          (event_id, status, request, response)
          VALUES ($1, $2, $3, $4)
          |] ( iEventId invo
             , toInt64 $ iStatus invo
             , Q.AltJ $ J.toJSON $ iRequest invo
             , Q.AltJ $ J.toJSON $ iResponse invo) True
  Q.unitQE defaultTxErrorHandler [Q.sql|
          UPDATE hdb_catalog.hdb_scheduled_events
          SET tries = tries + 1
          WHERE id = $1
          |] (Identity $ iEventId invo) True

getScheduledEvents :: Q.TxE QErr [ScheduledEvent]
getScheduledEvents = do
  allSchedules <- map uncurryEvent <$> Q.listQE defaultTxErrorHandler [Q.sql|
      UPDATE hdb_catalog.hdb_scheduled_events
      SET locked = 't'
      WHERE id IN ( SELECT t.id
                    FROM hdb_catalog.hdb_scheduled_events t
                    WHERE ( t.locked = 'f'
                            and t.delivered = 'f'
                            and t.error = 'f'
                            and t.scheduled_time <= now()
                            and t.dead = 'f'
                          )
                    FOR UPDATE SKIP LOCKED
                    )
      RETURNING id, name, webhook, payload, scheduled_time
      |] () True
  pure $ allSchedules
  where uncurryEvent (i, n, w, Q.AltJ p, st) =
          ScheduledEvent
          { seId      = i
          , seName    = n
          , seWebhook = w
          , sePayload = p
          , seScheduledTime = st
          }
