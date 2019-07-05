module Hasura.RQL.DDL.RemoteSchema
  ( runAddRemoteSchema
  , resolveRemoteSchemas
  , runRemoveRemoteSchema
  , removeRemoteSchemaFromCache
  , removeRemoteSchemaFromCatalog
  , refreshGCtxMapInSchema
  , fetchRemoteSchemas
  , addRemoteSchemaP1
  , addRemoteSchemaP2
  , runAddRemoteSchemaPermissions
  ) where

import           Hasura.EncJSON
import           Hasura.GraphQL.Utils
import           Hasura.Prelude

import qualified Data.Aeson                    as J
import qualified Data.HashMap.Strict           as Map
import qualified Database.PG.Query             as Q
import qualified Network.HTTP.Client           as HTTP

import           Hasura.GraphQL.RemoteServer
import           Hasura.RQL.DDL.Deps
import           Hasura.RQL.DDL.Remote.Types
import           Hasura.RQL.Types

import qualified Hasura.GraphQL.Context        as GC
import qualified Hasura.GraphQL.Schema         as GS
import qualified Hasura.GraphQL.Validate.Types as VT
import qualified Language.GraphQL.Draft.Syntax as G

runAddRemoteSchema
  :: ( QErrM m, UserInfoM m
     , CacheRWM m, MonadTx m
     , MonadIO m, HasHttpManager m
     )
  => AddRemoteSchemaQuery -> m EncJSON
runAddRemoteSchema q = do
  addRemoteSchemaP1 q >>= addRemoteSchemaP2 q

addRemoteSchemaP1
  :: ( QErrM m, UserInfoM m
     , MonadIO m, HasHttpManager m
     )
  => AddRemoteSchemaQuery -> m RemoteSchemaInfo
addRemoteSchemaP1 q = do
  adminOnly
  httpMgr <- askHttpManager
  rsi <- validateRemoteSchemaDef def
  -- TODO:- Maintain a cache of remote schema with it's GCtx
  void $ fetchRemoteSchema httpMgr name rsi
  return rsi
  where
    AddRemoteSchemaQuery name def _ = q

addRemoteSchemaP2
  :: ( QErrM m
     , CacheRWM m
     , MonadTx m
     )
  => AddRemoteSchemaQuery
  -> RemoteSchemaInfo
  -> m EncJSON
addRemoteSchemaP2 q rsi = do
  addRemoteSchemaToCache name rsi
  liftTx $ addRemoteSchemaToCatalog q
  return successMsg
  where
    name = _arsqName q

refreshGCtxMapInSchema
  :: (CacheRWM m, MonadIO m, MonadError QErr m, HasHttpManager m)
  => m ()
refreshGCtxMapInSchema = do
  sc <- askSchemaCache
  gCtxMap <- GS.mkGCtxMap (scTables sc) (scFunctions sc)
  httpMgr <- askHttpManager
  (mergedGCtxMap, defGCtx) <-
    mergeSchemas (scRemoteResolvers sc) gCtxMap httpMgr
  writeSchemaCache sc { scGCtxMap = mergedGCtxMap
                      , scDefaultRemoteGCtx = defGCtx }

runRemoveRemoteSchema
  :: (QErrM m, UserInfoM m, CacheRWM m, MonadTx m)
  => RemoveRemoteSchemaQuery -> m EncJSON
runRemoveRemoteSchema (RemoveRemoteSchemaQuery rsn)= do
  removeRemoteSchemaP1 rsn
  removeRemoteSchemaP2 rsn

removeRemoteSchemaP1
  :: (UserInfoM m, QErrM m, CacheRM m)
  => RemoteSchemaName -> m ()
removeRemoteSchemaP1 rsn = do
  adminOnly
  sc <- askSchemaCache
  let resolvers = scRemoteResolvers sc
  case Map.lookup rsn resolvers of
    Just _  -> return ()
    Nothing -> throw400 NotExists "no such remote schema"
  let depObjs = getDependentObjs sc remoteSchemaDepId
  when (depObjs /= []) $ reportDeps depObjs
  where
    remoteSchemaDepId = SORemoteSchema rsn

removeRemoteSchemaP2
  :: ( CacheRWM m
     , MonadTx m
     )
  => RemoteSchemaName
  -> m EncJSON
removeRemoteSchemaP2 rsn = do
  removeRemoteSchemaFromCache rsn
  liftTx $ removeRemoteSchemaFromCatalog rsn
  return successMsg

removeRemoteSchemaFromCache
  :: CacheRWM m => RemoteSchemaName -> m ()
removeRemoteSchemaFromCache rsn = do
  sc <- askSchemaCache
  let resolvers = scRemoteResolvers sc
  writeSchemaCache sc {scRemoteResolvers = Map.delete rsn resolvers}

resolveRemoteSchemas
  :: ( MonadError QErr m
     , MonadIO m
     )
  => SchemaCache -> HTTP.Manager -> m SchemaCache
resolveRemoteSchemas sc httpMgr = do
  (mergedGCtxMap, defGCtx) <-
    mergeSchemas (scRemoteResolvers sc) gCtxMap httpMgr
  return $ sc { scGCtxMap = mergedGCtxMap
              , scDefaultRemoteGCtx = defGCtx
              }
  where
    gCtxMap = scGCtxMap sc

addRemoteSchemaToCatalog
  :: AddRemoteSchemaQuery
  -> Q.TxE QErr ()
addRemoteSchemaToCatalog (AddRemoteSchemaQuery name def comment) =
  Q.unitQE defaultTxErrorHandler [Q.sql|
    INSERT into hdb_catalog.remote_schemas
      (name, definition, comment)
      VALUES ($1, $2, $3)
  |] (name, Q.AltJ $ J.toJSON def, comment) True

removeRemoteSchemaFromCatalog :: RemoteSchemaName -> Q.TxE QErr ()
removeRemoteSchemaFromCatalog name =
  Q.unitQE defaultTxErrorHandler [Q.sql|
    DELETE FROM hdb_catalog.remote_schemas
      WHERE name = $1
  |] (Identity name) True


fetchRemoteSchemas :: Q.TxE QErr [AddRemoteSchemaQuery]
fetchRemoteSchemas =
  map fromRow <$> Q.listQE defaultTxErrorHandler
    [Q.sql|
     SELECT name, definition, comment
       FROM hdb_catalog.remote_schemas
     |] () True
  where
    fromRow (n, Q.AltJ def, comm) = AddRemoteSchemaQuery n def comm

runAddRemoteSchemaPermissions
  :: ( QErrM m, UserInfoM m
     , CacheRWM m, MonadTx m
     )
  => RemoteSchemaPermission -> m EncJSON
runAddRemoteSchemaPermissions q = do
  adminOnly
  runAddRemoteSchemaPermissionP1 q
  pure successMsg

runAddRemoteSchemaPermissionP1
  :: (QErrM m, CacheRM m) => RemoteSchemaPermission -> m ()
runAddRemoteSchemaPermissionP1 remoteSchemaPermission = do
  sc <- askSchemaCache
  validateRemoteSchemaPermission sc remoteSchemaPermission

validateRemoteSchemaPermission :: (QErrM m) => SchemaCache -> RemoteSchemaPermission -> m ()
validateRemoteSchemaPermission sc remoteSchemaPerm = do
  -- TODO: Use RemoteSchemaInfo here after rakesh PR merge
  let gCtx = scDefaultRemoteGCtx sc
      types = GS._gTypes gCtx
  case Map.lookup
       (rsPermRemoteSchema remoteSchemaPerm)
       (scRemoteResolvers sc) of
    Nothing  -> throw400 RemoteSchemaError "No such remote schema"
    Just rsi -> case root of
      G.Name "query"        -> validateSelSet types (pure (GC._gQueryRoot gCtx)) rootSelectionSet
      G.Name "mutation"     -> validateSelSet types (GC._gMutRoot gCtx) rootSelectionSet
      G.Name "subscription" -> validateSelSet types (GC._gSubRoot gCtx) rootSelectionSet
      _ -> throw400 Unexpected "expected query, mutation or subscription in root"
  where
    root = rsPermRoot remoteSchemaPerm
    rootSelectionSet = rsPermSelectionSet remoteSchemaPerm

validateSelSet :: (QErrM m) => VT.TypeMap -> Maybe (VT.ObjTyInfo) -> [PermField] -> m ()
validateSelSet types start fields = case start of
  Nothing        -> throw400 NotFound "not an object type"
  Just objTypeInfo -> void $ traverse
                      (\(PermField fName selSet) -> case Map.lookup fName (VT._otiFields objTypeInfo) of
                          Nothing -> throw400 NotFound ("field: " <> showName fName <> " not found")
                          Just objFldInfo -> if isObjType types objFldInfo
                            then
                            validateSelSet types (getTyInfoFromField types objFldInfo) selSet
                            else
                            assertEmpty selSet
                      )
                      fields
  where
    assertEmpty selSet = case selSet of
      [] -> pure ()
      _  -> throw400 Unexpected "only object types can have subfields"
    isObjType types field =
      let baseTy = getBaseTy (VT._fiTy field)
          typeInfo = Map.lookup baseTy types
      in case typeInfo of
           Just (VT.TIObj _) -> True
           _                 -> False
    getTyInfoFromField types field =
      let baseTy = getBaseTy (VT._fiTy field)
          fieldName = VT._fiName field
          typeInfo = Map.lookup baseTy types
       in case typeInfo of
            Just (VT.TIObj objTyInfo) -> pure objTyInfo
