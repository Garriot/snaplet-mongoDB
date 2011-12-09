module Snap.Snaplet.MongoDB
  ( 
    MonadMongoDB(..)
    
  , MongoDBSnaplet(..)
  , HasMongoDBState(..)

  , MongoValue (..)
  , MongoEntity (..)

  , mongoDBInit

  , insert
  , insertMany
  , insertWith
  , get
  , getMany
  , delete
  , deleteWhere
  , select
  , selectOne
  , count
  , save
  , update
  , updateWhere
   
  , filters
  , updates
   
  , offset
  , limit
  , orderAsc
  , orderDesc

  , objid2bs
  , bs2objid
  , bs2objid'
  , bs2cs
  , getObjId
   
  , module Snap.Snaplet.MongoDB.FilterOps
  , module Snap.Snaplet.MongoDB.MongoEntity
  , module Snap.Snaplet.MongoDB.MongoValue
  , module Snap.Snaplet.MongoDB.Template
  , module Snap.Snaplet.MongoDB.Parse
  , MongoDB.Action
  , MongoDB.MonadIO'
  ) where

import           Snap.Core
import           Snap.Snaplet

import           Prelude hiding (lookup, or)
import           Control.Applicative
import           Control.Monad
import           Control.Monad.MVar
import           Control.Monad.Error
import           Data.Bson ((=:))
import qualified Data.Bson as BSON
import qualified Data.UString as US
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B8
import qualified Data.CompactString.Internal as CSI
import           Data.Maybe (catMaybes, fromJust)
import qualified Data.Map as Map
import           Numeric (showHex, readHex)
import           Safe
import           Snap.Snaplet.MongoDB.FilterOps
import           Snap.Snaplet.MongoDB.MongoEntity
import           Snap.Snaplet.MongoDB.MongoValue
import           Snap.Snaplet.MongoDB.Template
import           Snap.Snaplet.MongoDB.Parse
import qualified Database.MongoDB as MongoDB
import qualified Database.MongoDB.Internal.Util as MongoDB
import           Database.MongoDB.Query (Action, Failure(..), Database, access, master, AccessMode(..))
import qualified Database.MongoDB.Connection as MongoDB
import qualified System.IO.Pool as MPool

import           Control.Monad.CatchIO hiding (Handler)

instance Exception String

------------------------------------------------------------------------------
-- | The 'MonadMongoDB' class. Minimal complete definition:
class MonadIO m => MonadMongoDB m where

  ----------------------------------------------------------------------------
  -- | Run given MongoDB action against the database
  withDB       :: Action IO a -> m (Either Failure a)
  withDBUnsafe :: Action IO a -> m (Either Failure a)



  ----------------------------------------------------------------------------
  -- | Same as 'withDB' but calls 'error' if there is an exception
  withDB' :: Action IO a -> m a
  withDB' run = do
    r <- withDB run 
    either (throw . show) return r


data MongoDBSnaplet = MongoDBSnaplet {
  connPoll :: MPool.Pool IOError MongoDB.Pipe,
  appDatabase :: Database
}

------------------------------------------------------------------------------
-- |
class HasMongoDBState s where
    getMongoDBState :: Handler s s MongoDBSnaplet
    setMongoDBState :: MongoDBSnaplet -> Handler s s ()

    --modifyMongoDBState :: (MongoDBSnaplet -> MongoDBSnaplet) -> s -> s
    --modifyMongoDBState s = setMongoDBState s getMongoDBState

mongoDBInit :: MongoDB.Host -> Int -> US.UString -> SnapletInit b MongoDBSnaplet
mongoDBInit h n db =
  makeSnaplet "mongoDB" "MongoDB abstraction" Nothing $ do
    pool <- liftIO $ MPool.newPool (factoryForHost h) n
    return $ MongoDBSnaplet pool (db)
  where
    factoryForHost :: MongoDB.Host -> MPool.Factory IOError MongoDB.Pipe
    factoryForHost host = MPool.Factory (newRes h) (killRes) (isResExpired)
    
    newRes :: MongoDB.Host -> ErrorT IOError IO MongoDB.Pipe
    newRes = MongoDB.connect
    
    killRes :: MongoDB.Pipe -> IO ()
    killRes = MongoDB.close
    
    isResExpired :: MongoDB.Pipe -> IO Bool
    isResExpired = MongoDB.isClosed

------------------------------------------------------------------------------
-- |
instance HasMongoDBState s => MonadMongoDB (Handler s s) where
  withDB run = do
    (MongoDBSnaplet pool db) <- getMongoDBState
    epipe <- liftIO $ runErrorT $ MPool.aResource pool
    case epipe of
      Left err -> return $ Left $ ConnectionFailure err
      Right pipe -> do
		liftIO (access pipe master db run)

  withDBUnsafe run = do
    (MongoDBSnaplet pool db) <- getMongoDBState
    epipe <- liftIO $ runErrorT $ MPool.aResource pool
    case epipe of
      Left err -> return $ Left $ ConnectionFailure err
      Right pipe -> do
		liftIO (access pipe UnconfirmedWrites db run)


-- | Given a type in the type class 'MongoEntity', insert this as a new document in the database. The function will
-- yield the unique key for the new document.
insert :: (MongoDB.MonadIO' m, MongoEntity a) => a -> MongoDB.Action m (Key a)
insert obj = do
  (BSON.ObjId objId) <- MongoDB.insert (collectionName obj) (fromDocument $ encodeDocument obj)
  pure $! toKey objId


-- | Similar to 'insert', this function inserts multiple documents into the databsae, yielding a unique key for each.
insertMany :: (Functor m, MongoDB.MonadIO' m, MongoEntity a) => [a] -> MongoDB.Action m [Key a]
insertMany []   = pure []
insertMany objs = do
  ids <- MongoDB.insertMany (collectionName $ head objs) (map (fromDocument . encodeDocument) objs)
  forM ids $ \x -> case x of
                    BSON.ObjId objId -> pure $! toKey objId
                    _                -> throwError $ MongoDB.QueryFailure 1000 "Expected object ID as result of 'insertMany'"

-- | Similar to 'insert', but allows you to specify the ID with which the object is to be created.
insertWith :: (Functor m, MongoDB.MonadIO' m, MongoEntity a) => Key a -> a -> MongoDB.Action m (Key a)
insertWith key obj = do
  (BSON.ObjId objId) <- MongoDB.insert (collectionName obj) (("_id" =: fromKey key) : (fromDocument $ encodeDocument obj))
  pure $! toKey objId


-- | Given a unique key for a 'MongoEntity', yield that entity. If no document with the specified ID was found, the
-- function will yield @Nothing@.
get :: (Functor m, MongoDB.MonadIO' m, MongoEntity a) => Key a -> MongoDB.Action m (Maybe a)
get key = do
  let collection = collectionName (dummyFromKey key)
  result <- MongoDB.findOne (MongoDB.select ["_id" =: fromKey key] collection)
  case result of
    Just document -> do
      eObj <- runErrorT (decodeDocument $ toDocument document)
      case eObj of
        Left message -> throwError $ MongoDB.QueryFailure 1001 message
        Right    obj -> pure (Just obj)
    Nothing ->
      pure Nothing

-- | The application of 'get' to many keys.
getMany :: (Functor m, MongoDB.MonadIO' m, MongoEntity a) => [Key a] -> MongoDB.Action m [Maybe a]
getMany = mapM get

-- | Delete the document with the specified key.
delete :: (Functor m, MongoDB.MonadIO' m, MongoEntity a) => Key a -> MongoDB.Action m ()
delete key = do
  let collection = collectionName (dummyFromKey key)
  MongoDB.deleteOne (MongoDB.select ["_id" =: fromKey key] collection)

deleteWhere :: (Functor m, MongoDB.MonadIO' m, MongoEntity a) => Document a -> MongoDB.Action m ()
deleteWhere doc = do
  MongoDB.delete (MongoDB.select (fromDocument doc) (collectionName (dummyFromDocument doc)))

count :: (Functor m, MongoDB.MonadIO' m, MongoEntity a) => Document a -> MongoDB.Action m Int
count doc = do
  MongoDB.count (MongoDB.select (fromDocument doc) (collectionName (dummyFromDocument doc)))

type SelectorOption = MongoDB.Query -> MongoDB.Query

offset :: Int -> SelectorOption
offset n q = q { MongoDB.skip = fromIntegral n }

limit :: Int -> SelectorOption
limit n q = q { MongoDB.limit = fromIntegral n }

orderAsc :: (MongoEntity a, MongoValue v) => (v -> Filter a) -> SelectorOption
orderAsc f q = q { MongoDB.sort = (filterFieldName (f undefined) =: (1 :: Int)) : MongoDB.sort q }

orderDesc :: (MongoEntity a, MongoValue v) => (v -> Filter a) -> SelectorOption
orderDesc f q = q { MongoDB.sort = (filterFieldName (f undefined) =: ((-1) :: Int)) : MongoDB.sort q }


-- | Select all matching documents from a collection in the database. This function yields a list of pairs of the unique
-- key and the 'MongoEntity' for each matching document in the collection.
--
-- The query can be made using either the quasi-quoted mongoDB query format:
--
-- @select [mongo| { userName: #{name}, userPassword: #{password} |] []@
--
-- Or using the filter operations:
--
-- @select (filters [UserName `eq` name, UserPassword `eq` password]) []@
--
-- /Note/: in the quasi-quotation the names of the fields correspond to the fields stored in the /database/, rather than
-- the fields of the record structure.
--
select :: (Functor m, MonadControlIO m, MongoEntity a) => Document a -> [SelectorOption] -> MongoDB.Action m [(Key a, a)]
select document options = do
  cursor  <- MongoDB.find (foldr ($) (MongoDB.select (fromDocument document) (collectionName (dummyFromDocument document))) options)
  objects <- MongoDB.rest cursor
  values  <- mapM fetchKeyValue objects
  return $ catMaybes values

-- | Similar to 'select', only yielding a single result.
selectOne :: (Functor m, MonadControlIO m, MongoEntity a) => Document a -> [SelectorOption] -> MongoDB.Action m (Maybe (Key a, a))
selectOne document options = do
  result <- MongoDB.findOne (foldr ($) (MongoDB.select (fromDocument document) (collectionName (dummyFromDocument document))) options)
  case result of
    Just obj -> fetchKeyValue obj
    Nothing  -> pure Nothing


save :: (Functor m, MongoDB.MonadIO' m, MongoEntity a) => Key a -> a -> MongoDB.Action m ()
save key obj = do
  MongoDB.save (collectionName obj) (("_id" := BSON.ObjId (fromKey key)) : (fromDocument $ encodeDocument obj))


update :: (Functor m, MongoDB.MonadIO' m, MongoEntity a) => Key a -> Document a -> MongoDB.Action m ()
update key updateDoc = do
  MongoDB.modify (MongoDB.select [ "_id" =: (fromKey key) ] (collectionName (dummyFromKey key))) (fromDocument updateDoc)

updateWhere :: (Functor m, MongoDB.MonadIO' m, MongoEntity a) => Document a -> Document a -> MongoDB.Action m ()
updateWhere document updateDoc = do
  MongoDB.modify (MongoDB.select (fromDocument document) (collectionName (dummyFromDocument document))) (fromDocument updateDoc)


filters :: (MongoEntity a) => [FilterOp] -> Document a
filters = toDocument

updates :: (MongoEntity a) => [UpdateOp] -> Document a
updates = toDocument


fetchKeyValue :: (Functor m, MonadControlIO m, MongoEntity a) => BSON.Document -> MongoDB.Action m (Maybe (Key a, a))
fetchKeyValue doc = do
  case BSON.look "_id" doc of
    Just i ->
      case i of
        BSON.ObjId objId -> do
          eObj <- runErrorT (decodeDocument $ toDocument doc)
          case eObj of
            Left message -> throwError $ MongoDB.QueryFailure 1001 message
            Right    obj -> pure $ Just (toKey objId, obj)
        _ -> throwError $ MongoDB.QueryFailure 1000 "Expected _id field to be an ObjectId in result of selection"
    Nothing ->
      throwError $ MongoDB.QueryFailure 1000 "Expected to find an _id field in result of selection"


------------------------------------------------------------------------------
-- | Convert 'ObjectId' into 'ByteString'
objid2bs :: ObjectId -> BS.ByteString
objid2bs (Oid a b) = B8.pack . showHex a . showChar '-' . showHex b $ ""


------------------------------------------------------------------------------
-- | Convert 'ByteString' into 'ObjectId'
bs2objid :: BS.ByteString -> Maybe ObjectId
bs2objid bs = do
  case B8.split '-' bs of
    (a':b':_) -> do
      a <- fmap fst . headMay . readHex . B8.unpack $ a'
      b <- fmap fst . headMay . readHex . B8.unpack $ b'
      return $ Oid a b
    _ -> Nothing

------------------------------------------------------------------------------
-- | Like 'bs2objid', but may blow with an error if the 'ByteString' can't be
-- converted to an 'ObjectId'
bs2objid' :: BS.ByteString -> ObjectId
bs2objid' = fromJust . bs2objid

bs2cs :: BS.ByteString -> US.UString
bs2cs = CSI.CS


------------------------------------------------------------------------------
-- | If the 'Document' has an 'ObjectId' in the given field, return it as
-- 'ByteString'
getObjId :: US.UString -> BSON.Document -> Maybe BS.ByteString
getObjId v d = MongoDB.lookup v d >>= fmap objid2bs


dummyFromKey :: Key a -> a
dummyFromKey _ = undefined

dummyFromDocument :: Document a -> a
dummyFromDocument _ = undefined