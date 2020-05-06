{-|
= Scheduled Triggers

This module implements the functionality of invoking webhooks during specified
time events aka scheduled events. The scheduled events are the events generated
by the graphql-engine using the scheduled-triggers. Scheduled events are modeled
using rows in Postgres with a @timestamp@ column.

This module implements scheduling and delivery of scheduled events:

1. Scheduling a scheduled event involves creating new scheduled events. New
scheduled events are created based on the cron schedule and the number of
scheduled events that are already present in the scheduled events buffer.
The graphql-engine computes the new scheduled events and writes them to
the database.(Generator)

2. Delivering a scheduled event involves reading undelivered scheduled events
from the database and delivering them to the webhook server. (Processor)

The rationale behind separating the event scheduling and event delivery
mechanism into two different threads is that the scheduling and delivering of
the scheduled events are not directly dependent on each other. The generator
will almost always try to create scheduled events which are supposed to be
delivered in the future (timestamp > current_timestamp) and the processor
will fetch scheduled events of the past (timestamp < current_timestamp). So,
the set of the scheduled events generated by the generator and the processor
will never be the same. The point here is that they're not correlated to each
other. They can be split into different threads for a better performance.

== Implementation

During the startup, two threads are started:

1. Generator: Fetches the list of scheduled triggers from cache and generates
   the scheduled events.

    - Additional events will be generated only if there are fewer than 100
      scheduled events.

    - The upcoming events timestamp will be generated using:

        - cron schedule of the scheduled trigger

        - max timestamp of the scheduled events that already exist or
          current_timestamp(when no scheduled events exist)

        - The timestamp of the scheduled events is stored with timezone because
          `SELECT NOW()` returns timestamp with timezone, so it's good to
          compare two things of the same type.

    This effectively corresponds to doing an INSERT with values containing
    specific timestamp.

2. Processor: Fetches the undelivered events from the database and which have
   the scheduled timestamp lesser than the current timestamp and then
   process them.
-}
module Hasura.Eventing.ScheduledTrigger
  ( runScheduledEventsGenerator
  , processScheduledQueue
  , processOneOffScheduledQueue

  , ScheduledEventSeed(..)
  , generateScheduleTimes
  , insertScheduledEvents
  , ScheduledEventDb(..)
  , ScheduledEventOneOff(..)
  ) where

import           Control.Arrow.Extended            (dup)
import           Control.Concurrent.Extended       (sleep)
import           Data.Has
import           Data.Int                          (Int64)
import           Data.List                         (unfoldr)
import           Data.Time.Clock
import           Hasura.Eventing.HTTP
import           Hasura.HTTP
import           Hasura.Prelude
import           Hasura.RQL.DDL.Headers
import           Hasura.RQL.Types
import           Hasura.Server.Version             (HasVersion)
import           Hasura.RQL.DDL.EventTrigger       (getHeaderInfosFromConf,getWebhookInfoFromConf)
import           Hasura.SQL.DML
import           Hasura.SQL.Types
import           System.Cron

import qualified Data.Aeson                        as J
import qualified Data.Aeson.Casing                 as J
import qualified Data.Aeson.TH                     as J
import qualified Data.HashMap.Strict               as Map
import qualified Data.TByteString                  as TBS
import qualified Data.Text                         as T
import qualified Database.PG.Query                 as Q
import qualified Hasura.Logging                    as L
import qualified Network.HTTP.Client               as HTTP
import qualified Text.Builder                      as TB (run)
import qualified PostgreSQL.Binary.Decoding        as PD

newtype ScheduledTriggerInternalErr
  = ScheduledTriggerInternalErr QErr
  deriving (Show, Eq)

instance L.ToEngineLog ScheduledTriggerInternalErr L.Hasura where
  toEngineLog (ScheduledTriggerInternalErr qerr) =
    (L.LevelError, L.scheduledTriggerLogType, J.toJSON qerr)

scheduledEventsTable :: QualifiedTable
scheduledEventsTable =
  QualifiedObject
    hdbCatalogSchema
    (TableName $ T.pack "hdb_scheduled_events")

data ScheduledEventStatus
  = SESScheduled
  | SESLocked
  | SESDelivered
  | SESError
  | SESDead
  deriving (Show, Eq)

scheduledEventStatusToText :: ScheduledEventStatus -> Text
scheduledEventStatusToText SESScheduled = "scheduled"
scheduledEventStatusToText SESLocked = "locked"
scheduledEventStatusToText SESDelivered = "delivered"
scheduledEventStatusToText SESError = "error"
scheduledEventStatusToText SESDead = "dead"

instance Q.ToPrepArg ScheduledEventStatus where
  toPrepVal = Q.toPrepVal . scheduledEventStatusToText

instance Q.FromCol ScheduledEventStatus where
  fromCol bs = flip Q.fromColHelper bs $ PD.enum $ \case
    "scheduled" -> Just SESScheduled
    "locked"    -> Just SESLocked
    "delivered" -> Just SESDelivered
    "error"     -> Just SESError
    "dead"      -> Just SESDead
    _           -> Nothing

instance J.ToJSON ScheduledEventStatus where
  toJSON = J.String . scheduledEventStatusToText

data ScheduledTriggerStats
  = ScheduledTriggerStats
  { stsName                :: !TriggerName
  , stsUpcomingEventsCount :: !Int
  , stsMaxScheduledTime    :: !UTCTime
  } deriving (Show, Eq)

data ScheduledEventSeed
  = ScheduledEventSeed
  { sesName          :: !TriggerName
  , sesScheduledTime :: !UTCTime
  } deriving (Show, Eq)

data ScheduledEventPartial
  = ScheduledEventPartial
  { sepId            :: !Text
  , sepName          :: !TriggerName
  , sepScheduledTime :: !UTCTime
  , sepPayload       :: !(Maybe J.Value)
  , sepTries         :: !Int
  } deriving (Show, Eq)

data ScheduledEventDb
  = ScheduledEventDb
  { sedbId                :: !Text
  , sedbName              :: !TriggerName
  , sedbScheduledTime     :: !UTCTime
  , sedbAdditionalPayload :: !(Maybe J.Value)
  , sedbStatus            :: !ScheduledEventStatus
  , sedbTries             :: !Int
  } deriving (Show, Eq)

$(J.deriveToJSON (J.aesonDrop 4 J.snakeCase) ''ScheduledEventDb)

data ScheduledEventFull
  = ScheduledEventFull
  { sefId            :: !Text
  , sefName          :: !(Maybe TriggerName)
  -- ^ sefName is the name of the scheduled trigger, it's being
  -- used by both cron scheduled events and one-off scheduled events.
  -- A one-off scheduled event is not associated with a name, so in that
  -- case, 'sefName' will be @Nothing@
  , sefScheduledTime :: !UTCTime
  , sefTries         :: !Int
  , sefWebhook       :: !Text
  , sefPayload       :: !J.Value
  , sefRetryConf     :: !STRetryConf
  , sefHeaders       :: ![EventHeaderInfo]
  , sefComment       :: !(Maybe Text)
  } deriving (Show, Eq)

$(J.deriveToJSON (J.aesonDrop 3 J.snakeCase) {J.omitNothingFields = True} ''ScheduledEventFull)

data ScheduledEventOneOff -- refactor this to scheduledTriggerOneOff
  = ScheduledEventOneOff
  { seoId            :: !Text
  , seoScheduledTime :: !UTCTime
  , seoTries         :: !Int
  , seoWebhook       :: !WebhookConf
  , seoPayload       :: !(Maybe J.Value)
  , seoRetryConf     :: !STRetryConf
  , seoHeaderConf    :: ![HeaderConf]
  , seoComment       :: !(Maybe Text)
  , seoStatus        :: !ScheduledEventStatus
  } deriving (Show, Eq)

$(J.deriveToJSON (J.aesonDrop 3 J.snakeCase) {J.omitNothingFields = True} ''ScheduledEventOneOff)

-- | The 'ScheduledEventType' data type is needed to differentiate
--   between a 'Templated' and 'StandAlone' event because they
--   both have different configurations and they live in different
--   tables.
data ScheduledEventType =
    Templated
  -- ^ Templated scheduled events are those which have a template
  -- defined which will contain the webhook, headers and a retry
  -- configuration and a payload. The payload is a default payload
  -- which is used if creating a new scheduled event doesn't contain
  -- a payload of it's own. Every scheduled event derived using a
  -- template will use the above mentioned configurations. In case of
  -- a 'Templated' Event the configurations are stored in the schema
  -- cache and hence we don't need them while fetching the events.
  | StandAlone
  -- ^ A standalone event will have all the required configurations
  -- with it.
    deriving (Eq, Show)

-- | runScheduledEventsGenerator makes sure that all the scheduled triggers
--   have an adequate buffer of scheduled events.
runScheduledEventsGenerator ::
     L.Logger L.Hasura
  -> Q.PGPool
  -> IO SchemaCache
  -> IO void
runScheduledEventsGenerator logger pgpool getSC = do
  forever $ do
    sc <- getSC
    -- get scheduled triggers from cache
    let scheduledTriggersCache = scScheduledTriggers sc

    -- get scheduled trigger stats from db
    runExceptT
      (Q.runTx pgpool (Q.ReadCommitted, Just Q.ReadOnly) getDeprivedScheduledTriggerStats) >>= \case
      Left err -> L.unLogger logger $
        ScheduledTriggerInternalErr $ err500 Unexpected (T.pack $ show err)
      Right deprivedScheduledTriggerStats -> do
        -- join stats with scheduled triggers and produce @[(ScheduledTriggerInfo, ScheduledTriggerStats)]@
        --scheduledTriggersForHydrationWithStats' <- mapM (withST scheduledTriggers) deprivedScheduledTriggerStats
        scheduledTriggersForHydrationWithStats <-
          catMaybes <$>
          mapM (withST scheduledTriggersCache) deprivedScheduledTriggerStats
        -- insert scheduled events for scheduled triggers that need hydration
        runExceptT
          (Q.runTx pgpool (Q.ReadCommitted, Just Q.ReadWrite) $
          insertScheduledEventsFor scheduledTriggersForHydrationWithStats) >>= \case
          Right _ -> pure ()
          Left err ->
            L.unLogger logger $ ScheduledTriggerInternalErr $ err500 Unexpected (T.pack $ show err)
    sleep (minutes 1)
    where
      getDeprivedScheduledTriggerStats = liftTx $ do
        map uncurryStats <$>
          Q.listQE defaultTxErrorHandler
          [Q.sql|
           SELECT name, upcoming_events_count, max_scheduled_time
            FROM hdb_catalog.hdb_scheduled_events_stats
            WHERE upcoming_events_count < 100
           |] () True

      uncurryStats (n, count, maxTs) = ScheduledTriggerStats n count maxTs

      withST scheduledTriggerCache scheduledTriggerStat = do
        case Map.lookup (stsName scheduledTriggerStat) scheduledTriggerCache of
          Nothing -> do
            L.unLogger logger $
              ScheduledTriggerInternalErr $
                err500 Unexpected $
                "could not find scheduled trigger in the schema cache"
            pure Nothing
          Just scheduledTrigger -> pure $
            Just (scheduledTrigger, scheduledTriggerStat)

insertScheduledEventsFor :: [(ScheduledTriggerInfo, ScheduledTriggerStats)] -> Q.TxE QErr ()
insertScheduledEventsFor scheduledTriggersWithStats = do
  let scheduledEvents = flip concatMap scheduledTriggersWithStats $ \(sti, stats) ->
        generateScheduledEventsFrom (stsMaxScheduledTime stats) sti
  case scheduledEvents of
    []     -> pure ()
    events -> do
      let insertScheduledEventsSql = TB.run $ toSQL
            SQLInsert
              { siTable    = scheduledEventsTable
              , siCols     = map unsafePGCol ["name", "scheduled_time"]
              , siValues   = ValuesExp $ map (toTupleExp . toArr) events
              , siConflict = Just $ DoNothing Nothing
              , siRet      = Nothing
              }
      Q.unitQE defaultTxErrorHandler (Q.fromText insertScheduledEventsSql) () False
  where
    toArr (ScheduledEventSeed n t) = [(triggerNameToTxt n), (formatTime' t)]
    toTupleExp = TupleExp . map SELit

insertScheduledEvents :: [ScheduledEventSeed] -> Q.TxE QErr ()
insertScheduledEvents events = do
  let insertScheduledEventsSql = TB.run $ toSQL
        SQLInsert
          { siTable    = scheduledEventsTable
          , siCols     = map unsafePGCol ["name", "scheduled_time"]
          , siValues   = ValuesExp $ map (toTupleExp . toArr) events
          , siConflict = Just $ DoNothing Nothing
          , siRet      = Nothing
          }
  Q.unitQE defaultTxErrorHandler (Q.fromText insertScheduledEventsSql) () False
  where
    toArr (ScheduledEventSeed n t) = [(triggerNameToTxt n), (formatTime' t)]
    toTupleExp = TupleExp . map SELit

generateScheduledEventsFrom :: UTCTime -> ScheduledTriggerInfo-> [ScheduledEventSeed]
generateScheduledEventsFrom startTime ScheduledTriggerInfo{..} =
  let events =
        case stiSchedule of
          OneOff    -> empty -- one-off scheduled events are created through 'invoke_scheduled_event' API
          Cron cron -> generateScheduleTimes startTime 100 cron -- by default, generate next 100 events
   in map (ScheduledEventSeed stiName) events

-- | Generates next @n events starting @from according to 'CronSchedule'
generateScheduleTimes :: UTCTime -> Int -> CronSchedule -> [UTCTime]
generateScheduleTimes from n cron = take n $ go from
  where
    go = unfoldr (fmap dup . nextMatch cron)

processScheduledQueue
  :: HasVersion
  => L.Logger L.Hasura
  -> LogEnvHeaders
  -> HTTP.Manager
  -> Q.PGPool
  -> IO SchemaCache
  -> IO void
processScheduledQueue logger logEnv httpMgr pgpool getSC =
  forever $ do
    scheduledTriggersInfo <- scScheduledTriggers <$> getSC
    scheduledEventsE <-
      runExceptT $
      Q.runTx pgpool (Q.ReadCommitted, Just Q.ReadWrite) getScheduledEvents
    case scheduledEventsE of
      Right partialEvents ->
        for_ partialEvents $ \(ScheduledEventPartial id' name st payload tries)-> do
          case Map.lookup name scheduledTriggersInfo of
            Nothing ->  logInternalError $
              err500 Unexpected "could not find scheduled trigger in cache"
            Just ScheduledTriggerInfo{..} -> do
              let webhook = wciCachedValue stiWebhookInfo
                  payload' = fromMaybe (fromMaybe J.Null stiPayload) payload -- override if neccessary
                  scheduledEvent =
                      ScheduledEventFull id'
                                         (Just name)
                                         st
                                         tries
                                         webhook
                                         payload'
                                         stiRetryConf
                                         stiHeaders
                                         stiComment
              finally <- runExceptT $
                runReaderT (processScheduledEvent logEnv pgpool scheduledEvent Templated) (logger, httpMgr)
              either logInternalError pure finally
      Left err -> logInternalError err
    sleep (minutes 1)
    where
      logInternalError err = L.unLogger logger $ ScheduledTriggerInternalErr err

processOneOffScheduledQueue
  :: HasVersion
  => L.Logger L.Hasura
  -> LogEnvHeaders
  -> HTTP.Manager
  -> Q.PGPool
  -> IO void
processOneOffScheduledQueue logger logEnv httpMgr pgpool =
  forever $ do
    oneOffScheduledEvents <-
      runExceptT $
      Q.runTx pgpool (Q.ReadCommitted, Just Q.ReadWrite) getOneOffScheduledEvents
    case oneOffScheduledEvents of
      Right oneOffScheduledEvents' ->
        for_ oneOffScheduledEvents' $ \(ScheduledEventOneOff id'
                                                             scheduledTime
                                                             tries
                                                             webhookConf
                                                             payload
                                                             retryConf
                                                             headerConf
                                                             comment
                                                             _ )
          -> do
          webhookInfo <- runExceptT $ getWebhookInfoFromConf webhookConf
          headerInfo <- runExceptT $ getHeaderInfosFromConf headerConf

          case webhookInfo of
            Right webhookInfo' -> do
              case headerInfo of
                Right headerInfo' -> do
                  let webhook = wciCachedValue webhookInfo'
                      payload' = fromMaybe J.Null payload
                      scheduledEvent = ScheduledEventFull id'
                                                          Nothing
                                                          scheduledTime
                                                          tries
                                                          webhook
                                                          payload'
                                                          retryConf
                                                          headerInfo'
                                                          comment
                  finally <- runExceptT $
                    runReaderT (processScheduledEvent logEnv pgpool scheduledEvent StandAlone) (logger, httpMgr)
                  either logInternalError pure finally

                Left headerInfoErr -> logInternalError headerInfoErr

            Left webhookInfoErr -> logInternalError webhookInfoErr

      Left oneOffScheduledEventsErr -> logInternalError oneOffScheduledEventsErr
    sleep (minutes 1)
    where
      logInternalError err = L.unLogger logger $ ScheduledTriggerInternalErr err

processScheduledEvent ::
  ( MonadReader r m
  , Has HTTP.Manager r
  , Has (L.Logger L.Hasura) r
  , HasVersion
  , MonadIO m
  , MonadError QErr m
  )
  => LogEnvHeaders
  -> Q.PGPool
  -> ScheduledEventFull
  -> ScheduledEventType
  -> m ()
processScheduledEvent
  logEnv pgpool se@ScheduledEventFull {..} type' = do
  currentTime <- liftIO getCurrentTime
  if convertDuration (diffUTCTime currentTime sefScheduledTime)
    > unNonNegativeDiffTime (strcToleranceSeconds sefRetryConf)
    then processDead pgpool se type'
    else do
      let timeoutSeconds = round $ unNonNegativeDiffTime
                             $ strcTimeoutSeconds sefRetryConf
          httpTimeout = HTTP.responseTimeoutMicro (timeoutSeconds * 1000000)
          headers = addDefaultHeaders $ map encodeHeader sefHeaders
          extraLogCtx = ExtraLogContext (Just currentTime) sefId
      res <- runExceptT $ tryWebhook headers httpTimeout sefPayload (T.unpack sefWebhook)
      logHTTPForST res extraLogCtx
      let decodedHeaders = map (decodeHeader logEnv sefHeaders) headers
      either
        (processError pgpool se decodedHeaders type')
        (processSuccess pgpool se decodedHeaders type')
        res

processError
  :: (MonadIO m, MonadError QErr m)
  => Q.PGPool -> ScheduledEventFull -> [HeaderConf] -> ScheduledEventType -> HTTPErr a  -> m ()
processError pgpool se decodedHeaders type' err = do
  let invocation = case err of
        HClient excp -> do
          let errMsg = TBS.fromLBS $ J.encode $ show excp
          mkInvocation se 1000 decodedHeaders errMsg []
        HParse _ detail -> do
          let errMsg = TBS.fromLBS $ J.encode detail
          mkInvocation se 1001 decodedHeaders errMsg []
        HStatus errResp -> do
          let respPayload = hrsBody errResp
              respHeaders = hrsHeaders errResp
              respStatus = hrsStatus errResp
          mkInvocation se respStatus decodedHeaders respPayload respHeaders
        HOther detail -> do
          let errMsg = (TBS.fromLBS $ J.encode detail)
          mkInvocation se 500 decodedHeaders errMsg []
  liftExceptTIO $
    Q.runTx pgpool (Q.RepeatableRead, Just Q.ReadWrite) $ do
    insertInvocation invocation type'
    retryOrMarkError se err type'

retryOrMarkError :: ScheduledEventFull -> HTTPErr a -> ScheduledEventType -> Q.TxE QErr ()
retryOrMarkError se@ScheduledEventFull {..} err type' = do
  let mRetryHeader = getRetryAfterHeaderFromHTTPErr err
      mRetryHeaderSeconds = parseRetryHeaderValue =<< mRetryHeader
      triesExhausted = sefTries >= strcNumRetries sefRetryConf
      noRetryHeader = isNothing mRetryHeaderSeconds
  if triesExhausted && noRetryHeader
    then do
      setScheduledEventStatus sefId SESError type'
    else do
      currentTime <- liftIO getCurrentTime
      let delay = fromMaybe (round $ unNonNegativeDiffTime
                             $ strcRetryIntervalSeconds sefRetryConf)
                    $ mRetryHeaderSeconds
          diff = fromIntegral delay
          retryTime = addUTCTime diff currentTime
      setRetry se retryTime type'

{- Note [Scheduled event lifecycle]

A scheduled event can be in one of the five following states at any time:

1. Delivered
2. Cancelled
3. Error
4. Locked
5. Dead

A scheduled event is marked as delivered when the scheduled event is processed
successfully.

A scheduled event is marked as error when while processing the scheduled event
the webhook returns an error and the retries have exhausted (user configurable)
it's marked as error.

A scheduled event will be in the locked state when the graphql-engine fetches it
from the database to process it. After processing the event, the graphql-engine
will unlock it. This state is used to prevent multiple graphql-engine instances
running on the same database to process the same event concurrently.

A scheduled event will be marked as dead, when the difference between the
current time and the scheduled time is greater than the tolerance of the event.

A scheduled event will be in the cancelled state, if the `cancel_scheduled_event`
API is called against a particular scheduled event.

The graphql-engine will not consider those events which have been delivered,
cancelled, marked as error or in the dead state to process.
-}

processSuccess
  :: (MonadIO m, MonadError QErr m)
  => Q.PGPool -> ScheduledEventFull -> [HeaderConf] -> ScheduledEventType -> HTTPResp a -> m ()
processSuccess pgpool se decodedHeaders type' resp = do
  let respBody = hrsBody resp
      respHeaders = hrsHeaders resp
      respStatus = hrsStatus resp
      invocation = mkInvocation se respStatus decodedHeaders respBody respHeaders
  liftExceptTIO $
    Q.runTx pgpool (Q.RepeatableRead, Just Q.ReadWrite) $ do
    insertInvocation invocation type'
    setScheduledEventStatus (sefId se) SESDelivered type'

processDead :: (MonadIO m, MonadError QErr m) => Q.PGPool -> ScheduledEventFull -> ScheduledEventType -> m ()
processDead pgpool se type' =
  liftExceptTIO $
  Q.runTx pgpool (Q.RepeatableRead, Just Q.ReadWrite) $
    setScheduledEventStatus (sefId se) SESDead type'

setRetry :: ScheduledEventFull -> UTCTime -> ScheduledEventType ->  Q.TxE QErr ()
setRetry se time type' =
  case type' of
    Templated ->
      Q.unitQE defaultTxErrorHandler [Q.sql|
        UPDATE hdb_catalog.hdb_scheduled_events
        SET next_retry_at = $1,
        STATUS = 'scheduled'
        WHERE id = $2
        |] (time, sefId se) True
    StandAlone ->
      Q.unitQE defaultTxErrorHandler [Q.sql|
        UPDATE hdb_catalog.hdb_one_off_scheduled_events
        SET next_retry_at = $1,
        STATUS = 'scheduled'
        WHERE id = $2
        |] (time, sefId se) True

mkInvocation
  :: ScheduledEventFull -> Int -> [HeaderConf] -> TBS.TByteString -> [HeaderConf]
  -> (Invocation 'ScheduledType)
mkInvocation se status reqHeaders respBody respHeaders
  = let resp = if isClientError status
          then mkClientErr respBody
          else mkResp status respBody respHeaders
    in
      Invocation
      (sefId se)
      status
      (mkWebhookReq (J.toJSON se) reqHeaders invocationVersionST)
      resp

insertInvocation :: (Invocation 'ScheduledType) -> ScheduledEventType ->  Q.TxE QErr ()
insertInvocation invo type' = do
  case type' of
    Templated -> do
      Q.unitQE defaultTxErrorHandler
        [Q.sql|
         INSERT INTO hdb_catalog.hdb_scheduled_event_invocation_logs
         (event_id, status, request, response)
         VALUES ($1, $2, $3, $4)
        |] ( iEventId invo
             , fromIntegral $ iStatus invo :: Int64
             , Q.AltJ $ J.toJSON $ iRequest invo
             , Q.AltJ $ J.toJSON $ iResponse invo) True
      Q.unitQE defaultTxErrorHandler [Q.sql|
          UPDATE hdb_catalog.hdb_scheduled_events
          SET tries = tries + 1
          WHERE id = $1
          |] (Identity $ iEventId invo) True
    StandAlone -> do
      Q.unitQE defaultTxErrorHandler
        [Q.sql|
         INSERT INTO hdb_catalog.hdb_one_off_scheduled_event_invocation_logs
         (event_id, status, request, response)
         VALUES ($1, $2, $3, $4)
        |] ( iEventId invo
             , fromIntegral $ iStatus invo :: Int64
             , Q.AltJ $ J.toJSON $ iRequest invo
             , Q.AltJ $ J.toJSON $ iResponse invo) True
      Q.unitQE defaultTxErrorHandler [Q.sql|
          UPDATE hdb_catalog.hdb_one_off_scheduled_events
          SET tries = tries + 1
          WHERE id = $1
          |] (Identity $ iEventId invo) True

setScheduledEventStatus :: Text -> ScheduledEventStatus -> ScheduledEventType -> Q.TxE QErr ()
setScheduledEventStatus scheduledEventId status type' =
  case type' of
    Templated -> do
      Q.unitQE defaultTxErrorHandler
       [Q.sql|
        UPDATE hdb_catalog.hdb_scheduled_events
        SET status = $2
        WHERE id = $1
       |] (scheduledEventId, status) True
    StandAlone -> do
      Q.unitQE defaultTxErrorHandler
       [Q.sql|
        UPDATE hdb_catalog.hdb_one_off_scheduled_events
        SET status = $2
        WHERE id = $1
       |] (scheduledEventId, status) True

getScheduledEvents :: Q.TxE QErr [ScheduledEventPartial]
getScheduledEvents = do
  map uncurryEvent <$> Q.listQE defaultTxErrorHandler [Q.sql|
      UPDATE hdb_catalog.hdb_scheduled_events
      SET status = 'locked'
      WHERE id IN ( SELECT t.id
                    FROM hdb_catalog.hdb_scheduled_events t
                    WHERE ( t.status = 'scheduled'
                            and (
                             (t.next_retry_at is NULL and t.scheduled_time <= now()) or
                             (t.next_retry_at is not NULL and t.next_retry_at <= now())
                            )
                          )
                    FOR UPDATE SKIP LOCKED
                    )
      RETURNING id, name, scheduled_time, additional_payload, tries
      |] () True
  where uncurryEvent (i, n, st, p, tries) = ScheduledEventPartial i n st (Q.getAltJ <$> p) tries

getOneOffScheduledEvents :: Q.TxE QErr [ScheduledEventOneOff]
getOneOffScheduledEvents = do
  map uncurryOneOffEvent <$> Q.listQE defaultTxErrorHandler [Q.sql|
      UPDATE hdb_catalog.hdb_one_off_scheduled_events
      SET status = 'locked'
      WHERE id IN ( SELECT t.id
                    FROM hdb_catalog.hdb_one_off_scheduled_events t
                    WHERE ( t.status = 'scheduled'
                            and (
                             (t.next_retry_at is NULL and t.scheduled_time <= now()) or
                             (t.next_retry_at is not NULL and t.next_retry_at <= now())
                            )
                          )
                    FOR UPDATE SKIP LOCKED
                    )
      RETURNING id, webhook_conf, scheduled_time, retry_conf, payload, header_conf, tries, status, comment
      |] () False
  where
    uncurryOneOffEvent ( eventId
                       , webhookConf
                       , scheduledTime
                       , retryConf
                       , payload
                       , headerConf
                       , tries
                       , eventStatus
                       , comment ) =
      ScheduledEventOneOff eventId
                           scheduledTime
                           tries
                           (Q.getAltJ webhookConf)
                           (Q.getAltJ payload)
                           (Q.getAltJ retryConf)
                           (Q.getAltJ headerConf)
                           comment
                           eventStatus

liftExceptTIO :: (MonadError e m, MonadIO m) => ExceptT e IO a -> m a
liftExceptTIO m = liftEither =<< liftIO (runExceptT m)
