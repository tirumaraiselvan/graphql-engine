module Hasura.RQL.DDL.Metadata
  ( TableMeta

  , ReplaceMetadata(..)
  , runReplaceMetadata

  , ExportMetadata(..)
  , runExportMetadata
  , fetchMetadata

  , ClearMetadata(..)
  , runClearMetadata

  , ReloadMetadata(..)
  , runReloadMetadata

  , DumpInternalState(..)
  , runDumpInternalState

  , GetInconsistentMetadata
  , runGetInconsistentMetadata

  , DropInconsistentMetadata
  , runDropInconsistentMetadata
  ) where

import           Control.Lens                       hiding ((.=))
import           Data.Aeson
import           Data.Aeson.Casing
import           Data.Aeson.TH
import           Language.Haskell.TH.Syntax         (Lift)

import qualified Data.HashMap.Strict                as M
import qualified Data.HashSet                       as HS
import qualified Data.List                          as L
import qualified Data.Text                          as T

import           Hasura.EncJSON
import           Hasura.Prelude
import           Hasura.RQL.Types
import           Hasura.SQL.Types

import qualified Database.PG.Query                  as Q
import qualified Hasura.RQL.DDL.Action              as DA
import qualified Hasura.RQL.DDL.CustomTypes         as DC
import qualified Hasura.RQL.DDL.EventTrigger        as DE
import qualified Hasura.RQL.DDL.Permission          as DP
import qualified Hasura.RQL.DDL.Permission.Internal as DP
import qualified Hasura.RQL.DDL.QueryCollection     as DQC
import qualified Hasura.RQL.DDL.Relationship        as DR
import qualified Hasura.RQL.DDL.RemoteSchema        as DRS
import qualified Hasura.RQL.DDL.Schema              as DS
import qualified Hasura.RQL.Types.EventTrigger      as DTS
import qualified Hasura.RQL.Types.RemoteSchema      as TRS


data TableMeta
  = TableMeta
  { _tmTable               :: !QualifiedTable
  , _tmIsEnum              :: !Bool
  , _tmConfiguration       :: !(TableConfig)
  , _tmObjectRelationships :: ![DR.ObjRelDef]
  , _tmArrayRelationships  :: ![DR.ArrRelDef]
  , _tmInsertPermissions   :: ![DP.InsPermDef]
  , _tmSelectPermissions   :: ![DP.SelPermDef]
  , _tmUpdatePermissions   :: ![DP.UpdPermDef]
  , _tmDeletePermissions   :: ![DP.DelPermDef]
  , _tmEventTriggers       :: ![DTS.EventTriggerConf]
  } deriving (Show, Eq, Lift)

mkTableMeta :: QualifiedTable -> Bool -> TableConfig -> TableMeta
mkTableMeta qt isEnum config =
  TableMeta qt isEnum config [] [] [] [] [] [] []

makeLenses ''TableMeta

instance FromJSON TableMeta where
  parseJSON (Object o) = do
    unless (null unexpectedKeys) $
      fail $ "unexpected keys when parsing TableMetadata : "
      <> show (HS.toList unexpectedKeys)

    TableMeta
     <$> o .: tableKey
     <*> o .:? isEnumKey .!= False
     <*> o .:? configKey .!= emptyTableConfig
     <*> o .:? orKey .!= []
     <*> o .:? arKey .!= []
     <*> o .:? ipKey .!= []
     <*> o .:? spKey .!= []
     <*> o .:? upKey .!= []
     <*> o .:? dpKey .!= []
     <*> o .:? etKey .!= []

    where
      tableKey = "table"
      isEnumKey = "is_enum"
      configKey = "configuration"
      orKey = "object_relationships"
      arKey = "array_relationships"
      ipKey = "insert_permissions"
      spKey = "select_permissions"
      upKey = "update_permissions"
      dpKey = "delete_permissions"
      etKey = "event_triggers"

      unexpectedKeys =
        HS.fromList (M.keys o) `HS.difference` expectedKeySet

      expectedKeySet =
        HS.fromList [ tableKey, isEnumKey, configKey, orKey
                    , arKey , ipKey, spKey, upKey, dpKey, etKey
                    ]

  parseJSON _ =
    fail "expecting an Object for TableMetadata"

$(deriveToJSON (aesonDrop 3 snakeCase){omitNothingFields=True} ''TableMeta)

data ClearMetadata
  = ClearMetadata
  deriving (Show, Eq, Lift)
$(deriveToJSON defaultOptions ''ClearMetadata)

instance FromJSON ClearMetadata where
  parseJSON _ = return ClearMetadata

clearMetadata :: Q.TxE QErr ()
clearMetadata = Q.catchE defaultTxErrorHandler $ do
  Q.unitQ "DELETE FROM hdb_catalog.hdb_function WHERE is_system_defined <> 'true'" () False
  Q.unitQ "DELETE FROM hdb_catalog.hdb_permission WHERE is_system_defined <> 'true'" () False
  Q.unitQ "DELETE FROM hdb_catalog.hdb_relationship WHERE is_system_defined <> 'true'" () False
  Q.unitQ "DELETE FROM hdb_catalog.event_triggers" () False
  Q.unitQ "DELETE FROM hdb_catalog.hdb_table WHERE is_system_defined <> 'true'" () False
  Q.unitQ "DELETE FROM hdb_catalog.remote_schemas" () False
  Q.unitQ "DELETE FROM hdb_catalog.hdb_allowlist" () False
  Q.unitQ "DELETE FROM hdb_catalog.hdb_query_collection WHERE is_system_defined <> 'true'" () False
  Q.unitQ "DELETE FROM hdb_catalog.hdb_custom_types" () False
  Q.unitQ "DELETE FROM hdb_catalog.hdb_action_permission" () False
  Q.unitQ "DELETE FROM hdb_catalog.hdb_action WHERE is_system_defined <> 'true'" () False

runClearMetadata
  :: ( QErrM m, UserInfoM m, CacheRWM m, MonadTx m, MonadIO m
     , HasHttpManager m, HasSystemDefined m, HasSQLGenCtx m
     )
  => ClearMetadata -> m EncJSON
runClearMetadata _ = do
  adminOnly
  liftTx clearMetadata
  DS.buildSchemaCacheStrict
  return successMsg

-- representation of action permission metadata
data ActionPermissionMetadata
  = ActionPermissionMetadata
  { _apmRole       :: !RoleName
  , _apmComment    :: !(Maybe Text)
  , _apmDefinition :: !ActionPermissionDefinition
  } deriving (Show, Eq, Lift)

$(deriveJSON
  (aesonDrop 4 snakeCase){omitNothingFields=True}
  ''ActionPermissionMetadata)

-- representation of action metadata
data ActionMetadata
  = ActionMetadata
  { _amName        :: !ActionName
  , _amComment     :: !(Maybe Text)
  , _amDefinition  :: !ActionDefinitionInput
  , _amPermissions :: ![ActionPermissionMetadata]
  } deriving (Show, Eq, Lift)

$(deriveJSON
  (aesonDrop 3 snakeCase){omitNothingFields=True}
  ''ActionMetadata)

data ReplaceMetadata
  = ReplaceMetadata
  { aqTables           :: ![TableMeta]
  , aqFunctions        :: !(Maybe [QualifiedFunction])
  , aqRemoteSchemas    :: !(Maybe [TRS.AddRemoteSchemaQuery])
  , aqQueryCollections :: !(Maybe [DQC.CreateCollection])
  , aqAllowlist        :: !(Maybe [DQC.CollectionReq])
  , aqCustomTypes      :: !(Maybe CustomTypes)
  , aqActions          :: !(Maybe [ActionMetadata])
  } deriving (Show, Eq, Lift)

$(deriveJSON (aesonDrop 2 snakeCase) ''ReplaceMetadata)

applyQP1
  :: (QErrM m, UserInfoM m)
  => ReplaceMetadata -> m ()
applyQP1 (ReplaceMetadata tables mFunctions mSchemas
          mCollections mAllowlist _ mActions) = do

  adminOnly

  withPathK "tables" $ do

    checkMultipleDecls "tables" $ map _tmTable tables

    -- process each table
    void $ indexedForM tables $ \table -> withTableName (table ^. tmTable) $ do
      let allRels  = map DR.rdName (table ^. tmObjectRelationships) <>
                     map DR.rdName (table ^. tmArrayRelationships)

          insPerms = map DP.pdRole $ table ^. tmInsertPermissions
          selPerms = map DP.pdRole $ table ^. tmSelectPermissions
          updPerms = map DP.pdRole $ table ^. tmUpdatePermissions
          delPerms = map DP.pdRole $ table ^. tmDeletePermissions
          eventTriggers = map DTS.etcName $ table ^. tmEventTriggers

      checkMultipleDecls "relationships" allRels
      checkMultipleDecls "insert permissions" insPerms
      checkMultipleDecls "select permissions" selPerms
      checkMultipleDecls "update permissions" updPerms
      checkMultipleDecls "delete permissions" delPerms
      checkMultipleDecls "event triggers" eventTriggers

  withPathK "functions" $
    checkMultipleDecls "functions" functions

  for_ mSchemas $ \schemas ->
      withPathK "remote_schemas" $
        checkMultipleDecls "remote schemas" $ map TRS._arsqName schemas

  for_ mCollections $ \collections ->
    withPathK "query_collections" $
        checkMultipleDecls "query collections" $ map DQC._ccName collections

  for_ mAllowlist $ \allowlist ->
    withPathK "allowlist" $
        checkMultipleDecls "allow list" $ map DQC._crCollection allowlist

  withPathK "actions" $
    for_ mActions $ \actions ->
    checkMultipleDecls "actions" $ map _amName actions

  where
    withTableName qt = withPathK (qualObjectToText qt)
    functions = fromMaybe [] mFunctions

    checkMultipleDecls t l = do
      let dups = getDups l
      unless (null dups) $
        throw400 AlreadyExists $ "multiple declarations exist for the following " <> t <> " : "
        <> T.pack (show dups)

    getDups l =
      l L.\\ HS.toList (HS.fromList l)

applyQP2
  :: ( UserInfoM m
     , CacheRWM m
     , MonadTx m
     , MonadIO m
     , HasHttpManager m
     , HasSQLGenCtx m
     , HasSystemDefined m
     )
  => ReplaceMetadata
  -> m EncJSON
applyQP2 (ReplaceMetadata tables mFunctions
          mSchemas mCollections mAllowlist mCustomTypes mActions) = do

  liftTx clearMetadata
  DS.buildSchemaCacheStrict

  withPathK "tables" $ do
    -- tables and views
    indexedForM_ tables $ \tableMeta -> do
      let tableName = tableMeta ^. tmTable
          isEnum = tableMeta ^. tmIsEnum
          config = tableMeta ^. tmConfiguration
      void $ DS.trackExistingTableOrViewP2 tableName isEnum config

    -- Relationships
    indexedForM_ tables $ \table -> do
      withPathK "object_relationships" $
        indexedForM_ (table ^. tmObjectRelationships) $ \objRel ->
        DR.objRelP2 (table ^. tmTable) objRel
      withPathK "array_relationships" $
        indexedForM_ (table ^. tmArrayRelationships) $ \arrRel ->
        DR.arrRelP2 (table ^. tmTable) arrRel

    -- Permissions
    indexedForM_ tables $ \table -> do
      let tableName = table ^. tmTable
      tabInfo <- modifyErrAndSet500 ("apply " <> ) $ askTabInfo tableName
      withPathK "insert_permissions" $ processPerms tabInfo $
        table ^. tmInsertPermissions
      withPathK "select_permissions" $ processPerms tabInfo $
        table ^. tmSelectPermissions
      withPathK "update_permissions" $ processPerms tabInfo $
        table ^. tmUpdatePermissions
      withPathK "delete_permissions" $ processPerms tabInfo $
        table ^. tmDeletePermissions

    indexedForM_ tables $ \table ->
      withPathK "event_triggers" $
        indexedForM_ (table ^. tmEventTriggers) $ \etc ->
        DE.subTableP2 (table ^. tmTable) False etc

  -- sql functions
  withPathK "functions" $
    indexedMapM_ (void . DS.trackFunctionP2) functions

  -- query collections
  withPathK "query_collections" $
    indexedForM_ collections $ \c -> do
    liftTx $ DQC.addCollectionToCatalog c

  -- allow list
  withPathK "allowlist" $ do
    indexedForM_ allowlist $ \(DQC.CollectionReq name) -> do
      liftTx $ DQC.addCollectionToAllowlistCatalog name
    -- add to cache
    DQC.refreshAllowlist

  -- remote schemas
  onJust mSchemas $ \schemas ->
    withPathK "remote_schemas" $
      indexedMapM_ (void . DRS.addRemoteSchemaP2) schemas

  traverse_ DC.runSetCustomTypes_ mCustomTypes
  -- build GraphQL Context with Remote schemas
  DS.buildGCtxMap

  for_ mActions $ \actions -> for_ actions $ \action -> do
    let createAction =
          CreateAction (_amName action) (_amDefinition action) (_amComment action)
    DA.runCreateAction_ createAction
    for_ (_amPermissions action) $ \permission -> do
      let createActionPermission = CreateActionPermission (_amName action)
                                   (_apmRole permission) (_apmDefinition permission)
                                   (_apmComment permission)
      DA.runCreateActionPermission_ createActionPermission

  -- build the gctx map again after adding custom types and
  DS.buildGCtxMap

  return successMsg

  where
    functions = fromMaybe [] mFunctions
    collections = fromMaybe [] mCollections
    allowlist = fromMaybe [] mAllowlist
    processPerms tabInfo perms =
      indexedForM_ perms $ \permDef -> do
        permInfo <- DP.addPermP1 tabInfo permDef
        DP.addPermP2 (_tiName tabInfo) permDef permInfo

runReplaceMetadata
  :: ( QErrM m, UserInfoM m, CacheRWM m, MonadTx m
     , MonadIO m, HasHttpManager m, HasSQLGenCtx m
     , HasSystemDefined m
     )
  => ReplaceMetadata -> m EncJSON
runReplaceMetadata q = do
  applyQP1 q
  applyQP2 q

data ExportMetadata
  = ExportMetadata
  deriving (Show, Eq, Lift)

instance FromJSON ExportMetadata where
  parseJSON _ = return ExportMetadata

$(deriveToJSON defaultOptions ''ExportMetadata)

fetchMetadata :: Q.TxE QErr ReplaceMetadata
fetchMetadata = do
  tables <- Q.catchE defaultTxErrorHandler fetchTables
  let tableMetaMap = M.fromList . flip map tables $
        \(schema, name, isEnum, maybeConfig) ->
          let qualifiedName = QualifiedObject schema name
              configuration = maybe emptyTableConfig Q.getAltJ maybeConfig
          in (qualifiedName, mkTableMeta qualifiedName isEnum configuration)

  -- Fetch all the relationships
  relationships <- Q.catchE defaultTxErrorHandler fetchRelationships

  objRelDefs <- mkRelDefs ObjRel relationships
  arrRelDefs <- mkRelDefs ArrRel relationships

  -- Fetch all the permissions
  permissions <- Q.catchE defaultTxErrorHandler fetchPermissions

  -- Parse all the permissions
  insPermDefs <- mkPermDefs PTInsert permissions
  selPermDefs <- mkPermDefs PTSelect permissions
  updPermDefs <- mkPermDefs PTUpdate permissions
  delPermDefs <- mkPermDefs PTDelete permissions

  -- Fetch all event triggers
  eventTriggers <- Q.catchE defaultTxErrorHandler fetchEventTriggers
  triggerMetaDefs <- mkTriggerMetaDefs eventTriggers

  let (_, postRelMap) = flip runState tableMetaMap $ do
        modMetaMap tmObjectRelationships objRelDefs
        modMetaMap tmArrayRelationships arrRelDefs
        modMetaMap tmInsertPermissions insPermDefs
        modMetaMap tmSelectPermissions selPermDefs
        modMetaMap tmUpdatePermissions updPermDefs
        modMetaMap tmDeletePermissions delPermDefs
        modMetaMap tmEventTriggers triggerMetaDefs

  -- fetch all functions
  functions <- map (uncurry QualifiedObject) <$>
    Q.catchE defaultTxErrorHandler fetchFunctions

  -- fetch all custom resolvers
  schemas <- DRS.fetchRemoteSchemas

  -- fetch all collections
  collections <- DQC.fetchAllCollections

  -- fetch allow list
  allowlist <- map DQC.CollectionReq <$> DQC.fetchAllowlist

  mCustomTypes <- fetchCustomTypes

  -- fetch actions
  actions <- fetchActions

  return $ ReplaceMetadata
    (M.elems postRelMap) (Just functions)
    (Just schemas) (Just collections) (Just allowlist)
    mCustomTypes
    (if null actions then Nothing else actions)

  where

    modMetaMap l xs = do
      st <- get
      put $ foldr (\(qt, dfn) b -> b & at qt._Just.l %~ (:) dfn) st xs

    mkPermDefs pt = mapM permRowToDef . filter (\pr -> pr ^. _4 == pt)

    permRowToDef (sn, tn, rn, _, Q.AltJ pDef, mComment) = do
      perm <- decodeValue pDef
      return (QualifiedObject sn tn,  DP.PermDef rn perm mComment)

    mkRelDefs rt = mapM relRowToDef . filter (\rr -> rr ^. _4 == rt)

    relRowToDef (sn, tn, rn, _, Q.AltJ rDef, mComment) = do
      using <- decodeValue rDef
      return (QualifiedObject sn tn, DR.RelDef rn using mComment)

    mkTriggerMetaDefs = mapM trigRowToDef

    trigRowToDef (sn, tn, Q.AltJ configuration) = do
      conf <- decodeValue configuration
      return (QualifiedObject sn tn, conf::EventTriggerConf)

    fetchTables =
      Q.listQ [Q.sql|
                SELECT table_schema, table_name, is_enum, configuration::json
                FROM hdb_catalog.hdb_table
                 WHERE is_system_defined = 'false'
                    |] () False

    fetchRelationships =
      Q.listQ [Q.sql|
                SELECT table_schema, table_name, rel_name, rel_type, rel_def::json, comment
                  FROM hdb_catalog.hdb_relationship
                 WHERE is_system_defined = 'false'
                    |] () False

    fetchPermissions =
      Q.listQ [Q.sql|
                SELECT table_schema, table_name, role_name, perm_type, perm_def::json, comment
                  FROM hdb_catalog.hdb_permission
                 WHERE is_system_defined = 'false'
                    |] () False

    fetchEventTriggers =
     Q.listQ [Q.sql|
              SELECT e.schema_name, e.table_name, e.configuration::json
               FROM hdb_catalog.event_triggers e
              |] () False
    fetchFunctions =
      Q.listQ [Q.sql|
                SELECT function_schema, function_name
                FROM hdb_catalog.hdb_function
                WHERE is_system_defined = 'false'
                    |] () False

    fetchCustomTypes :: Q.TxE QErr (Maybe CustomTypes)
    fetchCustomTypes =
      fmap (Q.getAltJ . runIdentity) <$>
      Q.rawQE defaultTxErrorHandler [Q.sql|
         select custom_types::json from hdb_catalog.hdb_custom_types
                                          |] [] False
    fetchActions =
      Q.getAltJ . runIdentity . Q.getRow <$> Q.rawQE defaultTxErrorHandler [Q.sql|
        select
          coalesce(
            json_agg(
              json_build_object(
                'name', a.action_name,
                'definition', a.action_defn,
                'comment', a.comment,
                'permissions', ap.permissions
              )
            ),
            '[]'
          )
        from
          hdb_catalog.hdb_action as a
          left outer join lateral (
            select
              coalesce(
                json_agg(
                  json_build_object(
                    'role', ap.role_name,
                    'definition', ap.definition,
                    'comment', ap.comment
                  )
                ),
                '[]'
              ) as permissions
            from
              hdb_catalog.hdb_action_permission ap
            where
              ap.action_name = a.action_name
          ) ap on true;
                            |] [] False

runExportMetadata
  :: (QErrM m, UserInfoM m, MonadTx m)
  => ExportMetadata -> m EncJSON
runExportMetadata _ = do
  adminOnly
  encJFromJValue <$> liftTx fetchMetadata

data ReloadMetadata
  = ReloadMetadata
  deriving (Show, Eq, Lift)

instance FromJSON ReloadMetadata where
  parseJSON _ = return ReloadMetadata

$(deriveToJSON defaultOptions ''ReloadMetadata)

runReloadMetadata
  :: ( QErrM m, UserInfoM m, CacheRWM m, MonadTx m, MonadIO m
     , HasHttpManager m, HasSystemDefined m, HasSQLGenCtx m
     )
  => ReloadMetadata -> m EncJSON
runReloadMetadata _ = do
  adminOnly
  DS.buildSchemaCache
  return successMsg

data DumpInternalState
  = DumpInternalState
  deriving (Show, Eq, Lift)

instance FromJSON DumpInternalState where
  parseJSON _ = return DumpInternalState

$(deriveToJSON defaultOptions ''DumpInternalState)

runDumpInternalState
  :: (QErrM m, UserInfoM m, CacheRM m)
  => DumpInternalState -> m EncJSON
runDumpInternalState _ = do
  adminOnly
  encJFromJValue <$> askSchemaCache


data GetInconsistentMetadata
  = GetInconsistentMetadata
  deriving (Show, Eq, Lift)

instance FromJSON GetInconsistentMetadata where
  parseJSON _ = return GetInconsistentMetadata

$(deriveToJSON defaultOptions ''GetInconsistentMetadata)

runGetInconsistentMetadata
  :: (QErrM m, UserInfoM m, CacheRM m)
  => GetInconsistentMetadata -> m EncJSON
runGetInconsistentMetadata _ = do
  adminOnly
  inconsObjs <- scInconsistentObjs <$> askSchemaCache
  return $ encJFromJValue $ object
                [ "is_consistent" .= null inconsObjs
                , "inconsistent_objects" .= inconsObjs
                ]

data DropInconsistentMetadata
 = DropInconsistentMetadata
 deriving(Show, Eq, Lift)

instance FromJSON DropInconsistentMetadata where
  parseJSON _ = return DropInconsistentMetadata

$(deriveToJSON defaultOptions ''DropInconsistentMetadata)

runDropInconsistentMetadata
  :: (QErrM m, UserInfoM m, CacheRWM m, MonadTx m)
  => DropInconsistentMetadata -> m EncJSON
runDropInconsistentMetadata _ = do
  adminOnly
  sc <- askSchemaCache
  let inconsSchObjs = map _moId $ scInconsistentObjs sc
  mapM_ purgeMetadataObj inconsSchObjs
  writeSchemaCache sc{scInconsistentObjs = []}
  return successMsg

purgeMetadataObj :: MonadTx m => MetadataObjId -> m ()
purgeMetadataObj = liftTx . \case
  (MOTable qt)                     -> DS.deleteTableFromCatalog qt
  (MOFunction qf)                  -> DS.delFunctionFromCatalog qf
  (MORemoteSchema rsn)             -> DRS.removeRemoteSchemaFromCatalog rsn
  (MOCustomTypes)                  -> DC.clearCustomTypes
  (MOTableObj qt (MTORel rn _))    -> DR.delRelFromCatalog qt rn
  (MOTableObj qt (MTOPerm rn pt))  -> DP.dropPermFromCatalog qt rn pt
  (MOTableObj _ (MTOTrigger trn))  -> DE.delEventTriggerFromCatalog trn
  (MOAction action)                -> DA.deleteActionFromCatalog action Nothing
  (MOActionPermission action role) -> DA.deleteActionPermissionFromCatalog action role
