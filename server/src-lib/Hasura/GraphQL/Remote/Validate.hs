{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ApplicativeDo #-}

-- | Validate input queries against remote schemas.

module Hasura.GraphQL.Remote.Validate
  ( getCreateRemoteRelationshipValidation
  , validateRelationship
  , validateRemoteArguments
  , ValidationError(..)
  ) where

import qualified Data.HashMap.Strict as HM
import           Data.List.NonEmpty (NonEmpty(..))
import           Data.Validation
import qualified Hasura.GraphQL.Context as GC
import           Hasura.GraphQL.Remote.Input
import           Hasura.GraphQL.Schema
import qualified Hasura.GraphQL.Schema as GS
import           Hasura.GraphQL.Validate.Types
import qualified Hasura.GraphQL.Validate.Types as VT
import           Hasura.Prelude
import           Hasura.RQL.DDL.Relationship.Types
import           Hasura.RQL.Types
import qualified Language.GraphQL.Draft.Syntax as G

-- | An error validating the remote relationship.
data ValidationError
  = CouldntFindRemoteField G.Name
  | CouldntFindNamespace G.Name
  | CouldntFindTypeForNamespace G.Name
  | InvalidTypeForNamespace G.Name VT.TypeInfo
  | FieldNotFoundInRemoteSchema G.Name
  deriving (Show, Eq)

-- | Get a validation for the remote relationship proposal.
getCreateRemoteRelationshipValidation ::
     (QErrM m, CacheRM m)
  => CreateRemoteRelationship
  -> m (Either ValidationError ())
getCreateRemoteRelationshipValidation createRemoteRelationship = do
  schemaCache <- askSchemaCache
  pure
    (validateRelationship
       createRemoteRelationship
       (scDefaultRemoteGCtx schemaCache))

-- | Validate a remote relationship given a context.
validateRelationship ::
     CreateRemoteRelationship
  -> GC.GCtx
  -> Either ValidationError ()
validateRelationship createRemoteRelationship gctx = do
  objTyInfo <-
    lookupNamespace
      (createRemoteRelationshipNamespace createRemoteRelationship)
      gctx
  objFldInfo <-
    lookupField
      (createRemoteRelationshipRemoteField createRemoteRelationship)
      objTyInfo
  case VT._fiLoc objFldInfo of
    HasuraType ->
      Left
        (FieldNotFoundInRemoteSchema
           (createRemoteRelationshipRemoteField createRemoteRelationship))
    RemoteType {} -> pure ()
  undefined

-- | Lookup the field in the schema.
lookupField ::
     G.Name
  -> VT.ObjTyInfo
  -> Either ValidationError VT.ObjFldInfo
lookupField name =
  maybe (Left (CouldntFindRemoteField name)) pure .
  HM.lookup name .
  VT._otiFields

-- | Lookup the field in the schema.
lookupNamespace ::
     Maybe G.Name
  -> GC.GCtx
  -> Either ValidationError VT.ObjTyInfo
lookupNamespace = maybe viaQueryRoot viaGivenNamespace
  where
    viaQueryRoot = pure . GS._gQueryRoot
    viaGivenNamespace namespace gctx =
      case HM.lookup namespace (VT._otiFields (GS._gQueryRoot gctx)) of
        Nothing -> Left (CouldntFindNamespace namespace)
        Just objFldInfo ->
          case HM.lookup (VT.getBaseTy (VT._fiTy objFldInfo)) (GC._gTypes gctx) of
            Just (VT.TIObj tyObjInfo) -> pure tyObjInfo
            Just typeInfo ->
              Left (InvalidTypeForNamespace namespace typeInfo)
            Nothing -> Left (CouldntFindTypeForNamespace namespace)

-- | Validate remote input arguments against the remote schema.
validateRemoteArguments ::
     RemoteGCtx -> RemoteArguments -> Validation (NonEmpty ValidationError) ()
validateRemoteArguments = undefined
