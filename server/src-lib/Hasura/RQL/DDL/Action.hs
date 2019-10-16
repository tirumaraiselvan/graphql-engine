module Hasura.RQL.DDL.Action
  ( CreateAction
  , validateAndCacheAction
  , runCreateAction

  , DropAction
  , runDropAction

  , fetchActions

  , CreateActionPermission
  , validateAndCacheActionPermission
  , runCreateActionPermission

  , DropActionPermission
  , runDropActionPermission
  ) where

import           Hasura.EncJSON
import           Hasura.GraphQL.Utils
import           Hasura.Prelude
import           Hasura.RQL.Types
import           Hasura.SQL.Types

import qualified Hasura.GraphQL.Validate.Types as VT

import qualified Data.Aeson                    as J
import qualified Data.Aeson.Casing             as J
import qualified Data.Aeson.TH                 as J
import qualified Data.HashMap.Strict           as Map
import qualified Database.PG.Query             as Q
import qualified Language.GraphQL.Draft.Syntax as G

import           Language.Haskell.TH.Syntax    (Lift)
-- data RetryConf
--   = RetryConf
--   { _rcNumRetries  :: !Word64
--   , _rcIntervalSec :: !Word64
--   , _rcTimeoutSec  :: !(Maybe Word64)
--   } deriving (Show, Eq, Lift)

-- data WebhookConf
--   = WebhookConf
--   { _wcUrl     :: !Text
--   , _wcTimeout :: !Word64
--   , _wcRetry   :: !RetryConf
--   } deriving (Show, Eq)

getActionInfo
  :: (QErrM m, CacheRM m)
  => ActionName -> m ActionInfo
getActionInfo actionName = do
  actionMap <- scActions <$> askSchemaCache
  case Map.lookup actionName actionMap of
    Just actionInfo -> return actionInfo
    Nothing         ->
      throw400 NotExists $
      "action with name " <> actionName <<> " does not exist"

runCreateAction
  :: ( QErrM m, UserInfoM m
     , CacheRWM m, MonadTx m
     )
  => CreateAction -> m EncJSON
runCreateAction q@(CreateAction actionName actionDefinition comment) = do
  adminOnly
  validateAndCacheAction q
  persistCreateAction
  return successMsg
  where
    persistCreateAction :: (MonadTx m) => m ()
    persistCreateAction = do
      liftTx $ Q.unitQE defaultTxErrorHandler [Q.sql|
        INSERT into hdb_catalog.hdb_action
          (action_name, action_defn, comment)
          VALUES ($1, $2, $3)
      |] (actionName, Q.AltJ actionDefinition, comment) True

validateAndCacheAction
  :: (QErrM m, CacheRWM m)
  => CreateAction -> m ()
validateAndCacheAction q = do
  actionMap <- scActions <$> askSchemaCache
  onJust (Map.lookup actionName actionMap) $
    const $ throw400 AlreadyExists $
    "action with name " <> actionName <<> " already exists"
  actionInfo <- buildActionInfo q
  addActionToCache actionInfo
  where
    actionName  = _caName q

buildActionInfo
  :: (QErrM m, CacheRM m)
  => CreateAction -> m ActionInfo
buildActionInfo q = do
  let inputBaseType = G.getBaseType $ unGraphQLType $ _adInputType actionDefinition
      responseType = unGraphQLType $ _adOutputType actionDefinition
      responseBaseType = G.getBaseType responseType
  inputTypeInfo <- getCustomTypeInfo inputBaseType
  case inputTypeInfo of
    VT.TIScalar _ -> return ()
    VT.TIEnum _   -> return ()
    VT.TIInpObj _ -> return ()
    _ -> throw400 InvalidParams $ "the input type: "
         <> showNamedTy inputBaseType <>
         " should be a scalar/enum/input_object"
  when (hasList responseType) $ throw400 InvalidParams $
    "the output type: " <> G.showGT responseType <> " cannot be a list"
  responseTypeInfo <- getCustomTypeInfo responseBaseType
  case responseTypeInfo of
    VT.TIScalar _ -> return ()
    VT.TIEnum _   -> return ()
    VT.TIObj _    -> return ()
    _ -> throw400 InvalidParams $ "the output type: " <>
         showNamedTy responseBaseType <>
         " should be a scalar/enum/object"
  return $ ActionInfo actionName actionDefinition mempty
  where
    getCustomTypeInfo typeName = do
      customTypes <- scCustomTypes <$> askSchemaCache
      onNothing (Map.lookup typeName customTypes) $
        throw400 NotExists $ "the type: " <> showNamedTy typeName <>
        " is not defined in custom types"
    CreateAction actionName actionDefinition _ = q

    hasList = \case
      G.TypeList _ _  -> True
      G.TypeNamed _ _ -> False

data DropAction
  = DropAction
  { _daName      :: !ActionName
  , _daClearData :: !(Maybe Bool)
  } deriving (Show, Eq, Lift)
$(J.deriveJSON (J.aesonDrop 3 J.snakeCase) ''DropAction)

runDropAction
  :: (QErrM m, UserInfoM m, CacheRWM m, MonadTx m)
  => DropAction -> m EncJSON
runDropAction (DropAction actionName clearDataM)= do
  adminOnly
  void $ getActionInfo actionName
  delActionFromCache actionName
  liftTx $ do
    deleteActionFromCatalog
    when clearData clearActionData
  return successMsg
  where
    -- When clearData is not present we assume that
    -- the data needs to be retained
    clearData = fromMaybe False clearDataM

    deleteActionFromCatalog :: Q.TxE QErr ()
    deleteActionFromCatalog =
      Q.unitQE defaultTxErrorHandler [Q.sql|
          DELETE FROM hdb_catalog.hdb_action
            WHERE action_name = $1
          |] (Identity actionName) True

    clearActionData :: Q.TxE QErr ()
    clearActionData =
      Q.unitQE defaultTxErrorHandler [Q.sql|
          DELETE FROM hdb_catalog.hdb_action_log
            WHERE action_name = $1
          |] (Identity actionName) True

fetchActions :: Q.TxE QErr [CreateAction]
fetchActions =
  map fromRow <$> Q.listQE defaultTxErrorHandler
    [Q.sql|
     SELECT action_name, action_defn, comment
       FROM hdb_catalog.hdb_action
     |] () True
  where
    fromRow (actionName, Q.AltJ definition, comment) =
      CreateAction actionName definition comment

newtype ActionMetadataField
  = ActionMetadataField { unActionMetadataField :: Text }
  deriving (Show, Eq, J.FromJSON, J.ToJSON)


validateAndCacheActionPermission
  :: (QErrM m, CacheRWM m, MonadTx m)
  => CreateActionPermission -> m ()
validateAndCacheActionPermission createActionPermission = do
  actionInfo <- getActionInfo actionName
  onJust (Map.lookup role $ _aiPermissions actionInfo) $ \_ ->
    throw400 AlreadyExists $
    "permission for role: " <> role <<> " is already defined on " <>> actionName
  actionFilter <- buildActionFilter (_apdSelect permissionDefinition)
  addActionPermissionToCache actionName $
    ActionPermissionInfo role actionFilter
  where
    actionName = _capAction createActionPermission
    role = _capRole createActionPermission
    permissionDefinition = _capDefinition createActionPermission
  -- TODO
    buildActionFilter
      :: (QErrM m)
      => ActionPermissionSelect
      -> m AnnBoolExpPartialSQL
    buildActionFilter permission = undefined

runCreateActionPermission
  :: ( QErrM m, UserInfoM m
     , CacheRWM m, MonadTx m
     )
  => CreateActionPermission -> m EncJSON
runCreateActionPermission createActionPermission = do
  adminOnly
  validateAndCacheActionPermission createActionPermission
  persistCreateActionPermission
  return successMsg
  where
    actionName = _capAction createActionPermission
    role = _capRole createActionPermission
    permissionDefinition = _capDefinition createActionPermission
    comment = _capComment createActionPermission

    persistCreateActionPermission :: (MonadTx m) => m ()
    persistCreateActionPermission = do
      liftTx $ Q.unitQE defaultTxErrorHandler [Q.sql|
        INSERT into hdb_catalog.hdb_action_permission
          (action_name, role_name, definition, comment)
          VALUES ($1, $2, $3)
      |] (actionName, role, Q.AltJ permissionDefinition, comment) True

data DropActionPermission
  = DropActionPermission
  { _dapAction :: !ActionName
  , _dapRole   :: !RoleName
  -- , _capIfExists   :: !(Maybe IfExists)
  } deriving (Show, Eq, Lift)
$(J.deriveJSON (J.aesonDrop 4 J.snakeCase) ''DropActionPermission)

runDropActionPermission
  :: ( QErrM m, UserInfoM m
     , CacheRWM m, MonadTx m
     )
  => DropActionPermission -> m EncJSON
runDropActionPermission dropActionPermission = do
  adminOnly
  actionInfo <- getActionInfo actionName
  void $ onNothing (Map.lookup role $ _aiPermissions actionInfo) $
    throw400 NotExists $
    "permission for role: " <> role <<> " is not defined on " <>> actionName
  delActionPermissionFromCache actionName role
  liftTx deleteActionPermissionFromCatalog
  return successMsg
  where
    actionName = _dapAction dropActionPermission
    role = _dapRole dropActionPermission

    deleteActionPermissionFromCatalog :: Q.TxE QErr ()
    deleteActionPermissionFromCatalog =
      Q.unitQE defaultTxErrorHandler [Q.sql|
          DELETE FROM hdb_catalog.hdb_action_permission
            WHERE action_name = $1
              AND role_name = $2
          |] (actionName, role) True