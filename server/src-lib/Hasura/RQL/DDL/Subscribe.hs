{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}

module Hasura.RQL.DDL.Subscribe where

import           Data.Aeson
import           Data.Int            (Int64)
import           Hasura.Prelude
import           Hasura.RQL.Types
import           Hasura.SQL.Types
import           System.Environment  (lookupEnv)

import qualified Data.FileEmbed      as FE
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Text           as T
import qualified Data.Text.Encoding  as TE
import qualified Database.PG.Query   as Q
import qualified Text.Ginger         as TG

data Ops = INSERT | UPDATE | DELETE deriving (Show)

data OpVar = OLD | NEW deriving (Show)

type GingerTmplt = TG.Template TG.SourcePos

defaultNumRetries :: Int
defaultNumRetries = 0

defaultRetryInterval :: Int
defaultRetryInterval = 10

parseGingerTmplt :: TG.Source -> Either String GingerTmplt
parseGingerTmplt src = either parseE Right res
  where
    res = runIdentity $ TG.parseGinger' parserOptions src
    parserOptions = TG.mkParserOptions resolver
    resolver = const $ return Nothing
    parseE e = Left $ TG.formatParserError (Just "") e

triggerTmplt :: Maybe GingerTmplt
triggerTmplt = case parseGingerTmplt $(FE.embedStringFile "src-rsr/trigger.sql.j2") of
  Left _      -> Nothing
  Right tmplt -> Just tmplt

getDropFuncSql :: Ops -> TriggerName -> T.Text
getDropFuncSql op trn = "DROP FUNCTION IF EXISTS"
                        <> " hdb_views.notify_hasura_" <> trn <> "_" <> T.pack (show op) <> "()"
                        <> " CASCADE"

getTriggerSql :: Ops -> TriggerId -> TriggerName -> SchemaName -> TableName -> Maybe SubscribeOpSpec -> Maybe T.Text
getTriggerSql op trid trn sn tn spec =
  let globalCtx =  HashMap.fromList [
                    (T.pack "ID", trid)
                  , (T.pack "NAME", trn)
                  , (T.pack "SCHEMA_NAME", getSchemaTxt sn)
                  , (T.pack "TABLE_NAME", getTableTxt tn)]
      opCtx = maybe HashMap.empty (createOpCtx op) spec
      context = HashMap.union globalCtx opCtx
  in
      spec >> renderSql context <$> triggerTmplt
  where
    createOpCtx :: Ops -> SubscribeOpSpec -> HashMap.HashMap T.Text T.Text
    createOpCtx op1 (SubscribeOpSpec columns) = HashMap.fromList [
                                        (T.pack "OPERATION", T.pack $ show op1)
                                      , (T.pack "OLD_DATA_EXPRESSION", renderOldDataExp op1 columns )
                                      , (T.pack "NEW_DATA_EXPRESSION", renderNewDataExp op1 columns )]
    renderOldDataExp :: Ops -> SubscribeColumns -> T.Text
    renderOldDataExp op2 scs = case op2 of
                                 INSERT -> "NULL"
                                 UPDATE -> getRowExpression OLD scs
                                 DELETE -> getRowExpression OLD scs
    renderNewDataExp :: Ops -> SubscribeColumns -> T.Text
    renderNewDataExp op2 scs = case op2 of
                                 INSERT -> getRowExpression NEW scs
                                 UPDATE -> getRowExpression NEW scs
                                 DELETE -> "NULL"
    getRowExpression :: OpVar -> SubscribeColumns -> T.Text
    getRowExpression opVar scs = case scs of
                                    SubCStar -> "row_to_json(" <> T.pack (show opVar) <> ")"
                                    SubCArray cols -> "row_to_json((select r from (select " <> listcols cols opVar <> ") as r))"
                                   where
                                     listcols :: [PGCol] -> OpVar -> T.Text
                                     listcols pgcols var = T.intercalate ", " $ fmap (mkQualified (T.pack $ show var).getPGColTxt) pgcols
                                     mkQualified :: T.Text -> T.Text -> T.Text
                                     mkQualified v col = v <> "." <> col

    renderSql :: HashMap.HashMap T.Text T.Text -> GingerTmplt -> T.Text
    renderSql = TG.easyRender

mkTriggerQ
  :: TriggerId
  -> TriggerName
  -> QualifiedTable
  -> TriggerOpsDef
  -> Q.TxE QErr ()
mkTriggerQ trid trn (QualifiedTable sn tn) (TriggerOpsDef insert update delete) = do
  let msql = getTriggerSql INSERT trid trn sn tn insert
             <> getTriggerSql UPDATE trid trn sn tn update
             <> getTriggerSql DELETE trid trn sn tn delete
  case msql of
    Just sql -> Q.multiQE defaultTxErrorHandler (Q.fromBuilder $ TE.encodeUtf8Builder sql)
    Nothing -> throw500 "no trigger sql generated"

addEventTriggerToCatalog :: QualifiedTable -> EventTriggerDef
               -> Q.TxE QErr TriggerId
addEventTriggerToCatalog (QualifiedTable sn tn) (EventTriggerDef name def webhook rconf mheaders) = do
  ids <- map runIdentity <$> Q.listQE defaultTxErrorHandler [Q.sql|
                                  INSERT into hdb_catalog.event_triggers (name, type, schema_name, table_name, definition, webhook, num_retries, retry_interval, headers)
                                  VALUES ($1, 'table', $2, $3, $4, $5, $6, $7, $8)
                                  RETURNING id
                                  |] (name, sn, tn, Q.AltJ $ toJSON def, webhook, toInt64 $ rcNumRetries rconf, toInt64 $ rcIntervalSec rconf, Q.AltJ $ toJSON mheaders) True

  trid <- getTrid ids
  mkTriggerQ trid name (QualifiedTable sn tn) def
  return trid
  where
    getTrid []    = throw500 "could not create event-trigger"
    getTrid (x:_) = return x
    toInt64 :: (Integral a) => a -> Int64
    toInt64 = fromIntegral


delEventTriggerFromCatalog :: TriggerName -> Q.TxE QErr ()
delEventTriggerFromCatalog trn = do
  Q.unitQE defaultTxErrorHandler [Q.sql|
           DELETE FROM
                  hdb_catalog.event_triggers
           WHERE name = $1
                |] (Identity trn) True
  mapM_ tx [INSERT, UPDATE, DELETE]
  where
    tx :: Ops -> Q.TxE QErr ()
    tx op = Q.multiQE defaultTxErrorHandler (Q.fromBuilder $ TE.encodeUtf8Builder $ getDropFuncSql op trn)

fetchEventTrigger :: TriggerName -> Q.TxE QErr EventTrigger
fetchEventTrigger trn = do
  triggers <- Q.listQE defaultTxErrorHandler [Q.sql|
                                              SELECT e.schema_name, e.table_name, e.name, e.definition::json, e.webhook, e.num_retries, e.retry_interval
                                              FROM hdb_catalog.event_triggers e
                                              WHERE e.name = $1
                                  |] (Identity trn) True
  getTrigger triggers
  where
    getTrigger []    = throw400 NotExists ("could not find event trigger '" <> trn <> "'")
    getTrigger (x:_) = return $ EventTrigger (QualifiedTable sn tn) trn' tDef webhook (RetryConf nr rint)
      where (sn, tn, trn', Q.AltJ tDef, webhook, nr, rint) = x

fetchEvent :: EventId -> Q.TxE QErr (EventId, Bool)
fetchEvent eid = do
  events <- Q.listQE defaultTxErrorHandler [Q.sql|
      SELECT l.id, l.locked
      FROM hdb_catalog.event_log l
      JOIN hdb_catalog.event_triggers e
      ON l.trigger_id = e.id
      WHERE l.id = $1
      |] (Identity eid) True
  event <- getEvent events
  assertEventUnlocked event
  return event
  where
    getEvent []    = throw400 NotExists "event not found"
    getEvent (x:_) = return x

    assertEventUnlocked (_, locked) = when locked $
      throw400 Busy "event is already being processed"

markForDelivery :: EventId -> Q.TxE QErr ()
markForDelivery eid =
  Q.unitQE defaultTxErrorHandler [Q.sql|
          UPDATE hdb_catalog.event_log
          SET
          delivered = 'f',
          error = 'f',
          tries = 0
          WHERE id = $1
          |] (Identity eid) True

subTableP1 :: (P1C m) => CreateEventTriggerQuery -> m (QualifiedTable, EventTriggerDef)
subTableP1 (CreateEventTriggerQuery name qt insert update delete retryConf webhook mheaders) = do
  adminOnly
  ti <- askTabInfo qt
  assertCols ti insert
  assertCols ti update
  assertCols ti delete
  let rconf = fromMaybe (RetryConf defaultNumRetries defaultRetryInterval) retryConf
  return (qt, EventTriggerDef name (TriggerOpsDef insert update delete) webhook rconf mheaders)
  where
    assertCols _ Nothing = return ()
    assertCols ti (Just sos) = do
      let cols = sosColumns sos
      case cols of
        SubCStar         -> return ()
        SubCArray pgcols -> forM_ pgcols (assertPGCol (tiFieldInfoMap ti) "")

subTableP2 :: (P2C m) => QualifiedTable -> EventTriggerDef -> m ()
subTableP2 qt q@(EventTriggerDef name def webhook rconf mheaders) = do
  trid <- liftTx $ addEventTriggerToCatalog qt q
  let headerConfs = fromMaybe [] mheaders
  headers <- liftIO $ getHeadersFromConf headerConfs
  addEventTriggerToCache qt trid name def rconf webhook headers

subTableP2shim :: (P2C m) => (QualifiedTable, EventTriggerDef) -> m RespBody
subTableP2shim (qt, etdef) = do
  subTableP2 qt etdef
  return successMsg

instance HDBQuery CreateEventTriggerQuery where
  type Phase1Res CreateEventTriggerQuery = (QualifiedTable, EventTriggerDef)
  phaseOne = subTableP1
  phaseTwo _ = subTableP2shim
  schemaCachePolicy = SCPReload

unsubTableP1 :: (P1C m) => DeleteEventTriggerQuery -> m ()
unsubTableP1 _ = adminOnly

unsubTableP2 :: (P2C m) => DeleteEventTriggerQuery -> m RespBody
unsubTableP2 (DeleteEventTriggerQuery name) = do
  et <- liftTx $ fetchEventTrigger name
  delEventTriggerFromCache (etTable et) name
  liftTx $ delEventTriggerFromCatalog name
  return successMsg

instance HDBQuery DeleteEventTriggerQuery where
  type Phase1Res DeleteEventTriggerQuery = ()
  phaseOne = unsubTableP1
  phaseTwo q _ = unsubTableP2 q
  schemaCachePolicy = SCPReload

deliverEvent :: (P2C m) => DeliverEventQuery -> m RespBody
deliverEvent (DeliverEventQuery eventId) = do
  _ <- liftTx $ fetchEvent eventId
  liftTx $ markForDelivery eventId
  return successMsg

instance HDBQuery DeliverEventQuery where
  type Phase1Res DeliverEventQuery = ()
  phaseOne _ = adminOnly
  phaseTwo q _ = deliverEvent q
  schemaCachePolicy = SCPNoChange

getHeadersFromConf :: [HeaderConf] -> IO [(HeaderName, Maybe T.Text)]
getHeadersFromConf = mapM getHeader
  where
    getHeader :: HeaderConf -> IO (HeaderName, Maybe T.Text)
    getHeader hconf = case hconf of
      (HeaderConf name (HVValue val)) -> return (name, Just val)
      (HeaderConf name (HVEnv val))   -> do
        mEnv <- lookupEnv (T.unpack val)
        return (name, T.pack <$> mEnv)
