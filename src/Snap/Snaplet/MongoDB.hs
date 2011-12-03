module Snap.Snaplet.MongoDB
  ( 
    -- * MongoDB Functionality Inside Snap Monad 
    MonadMongoDB(..)

    -- * Implementation
    
    -- ** Keeping MongoDB State
  , MongoDBSnaplet(..)
  , HasMongoDBState(..)

    -- ** Initializing Your Applications
  , mongoDBInit


    -- * Utility Functions
  , getObjId
  , bs2objid
  , bs2objid'
  , objid2bs
  , lp

  --  -- * Snap.Auth Interface
  --  -- $monadauth
  --, docToAuthUser
  --, authUserToDoc

  , module Snap.Snaplet.MongoDB.Instances

    -- * MongoDB Library 
    -- | Exported for your convenience.
  , module Database.MongoDB
  ) where

import           Control.Applicative
import           Control.Monad
import           Control.Monad.Trans
import           Control.Monad.Reader
import           Control.Monad.Error

import           Data.ByteString.Internal (c2w, w2c)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import           Data.ByteString (ByteString)
import qualified Data.Text as T
import qualified Data.CompactString.Internal as CSI
import qualified Data.CompactString.UTF8 as CS
import           Data.UString (u)
import qualified Data.Map as Map
import           Data.Map (Map)
import           Data.Word (Word8)
import           Data.Time

import           Database.MongoDB
import           Database.MongoDB as DB
import           Database.MongoDB.Query (Database)

import           Numeric (showHex, readHex)
import           Safe

import           Snap.Core

import           Snap.Snaplet.MongoDB.Instances
import           Snap.Snaplet.MongoDB.Utils
import           Snap.Snaplet.MongoDB.Generics (RecKey)

import           System.IO.Pool
import           Snap.Snaplet


-- $monadauth
-- This package gives you free MonadAuthUser instances of your application
-- monad. Once your application becomes MonadMongoDB, if it is also MonadAuth,
-- it will automatically become MonadAuthUser.
--
-- This means you can immediately start using authentication functionality
-- without worrying about schema, fields, etc. This library will take care of
-- that for you.

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
    either (error . show) return r

            

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Implementation
------------------------------------------------------------------------------
------------------------------------------------------------------------------


---- | MongoDB State
--data MongoDBState = MongoDBState
--    { connPool :: Pool IOError Pipe
--    , appDatabase :: Database
--    }


data MongoDBSnaplet = MongoDBSnaplet {
  connPoll :: Pool IOError Pipe,
  appDatabase :: Database
}

------------------------------------------------------------------------------
-- |
class HasMongoDBState s where
    getMongoDBState :: s -> MongoDBSnaplet
    setMongoDBState :: MongoDBSnaplet -> s -> s

    modifyMongoDBState :: (MongoDBSnaplet -> MongoDBSnaplet) -> s -> s
    modifyMongoDBState f s = setMongoDBState (f $ getMongoDBState s) s


------------------------------------------------------------------------------
---- |
--mongoDBInitializer :: Host
--                   -> Int
--                   -> UString
--                   -> Initializer MongoDBState
--mongoDBInitializer h n db = do
--  mongoState <- liftIO $ do
--    pool <- newPool (factoryForHost h) n
--    return $ MongoDBState pool (db)
--  mkInitializer mongoState
--  where
--    factoryForHost :: Host -> Factory IOError Pipe
--    factoryForHost host = Factory (newRes h) (killRes) (isResExpired)
    
--    newRes :: Host -> ErrorT IOError IO Pipe
--    newRes = connect
    
--    killRes :: Pipe -> IO ()
--    killRes = close
    
--    isResExpired :: Pipe -> IO Bool
--    isResExpired = isClosed

mongoDBInit :: Host -> Int -> UString -> SnapletInit b MongoDBSnaplet
mongoDBInit h n db =
  makeSnaplet "mongoDB" "MongoDB abstraction" Nothing $ do
    pool <- liftIO $ newPool (factoryForHost h) n
    return $ MongoDBSnaplet pool (db)
  where
    factoryForHost :: Host -> Factory IOError Pipe
    factoryForHost host = Factory (newRes h) (killRes) (isResExpired)
    
    newRes :: Host -> ErrorT IOError IO Pipe
    newRes = connect
    
    killRes :: Pipe -> IO ()
    killRes = close
    
    isResExpired :: Pipe -> IO Bool
    isResExpired = isClosed

------------------------------------------------------------------------------
-- |
--instance InitializerState MongoDBState where
--  extensionId = const "MongoDB/MongoDB"
--  mkCleanup s = killAll $ connPool s
--  mkReload = const $ return ()


------------------------------------------------------------------------------
-- |
--instance HasMongoDBState s => MonadMongoDB (SnapExtend s) where
--  withDB run = do
--    (MongoDBState pool db) <- asks getMongoDBState
--    epipe <- liftIO $ runErrorT $ aResource pool
--    case epipe of
--      Left err -> return $ Left $ ConnectionFailure err
--      Right pipe -> do
--		liftIO (access pipe master db run)

--  withDBUnsafe run = do
--    (MongoDBState pool db) <- asks getMongoDBState
--    epipe <- liftIO $ runErrorT $ aResource pool
--    case epipe of
--      Left err -> return $ Left $ ConnectionFailure err
--      Right pipe -> do
--		liftIO (access pipe UnconfirmedWrites db run)


------------------------------------------------------------------------------
-- Convenience Functions
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- | Add timestamps to any document.
addTimeStamps :: (MonadMongoDB m) => Document -> m Document
addTimeStamps d = do
  t <- liftIO getCurrentTime
  let tsc = ["created_at" =: t]
  let tsu = ["updated_at" =: t]
  return $ tsu `DB.merge` d `DB.merge` tsc
 

------------------------------------------------------------------------------
-- Snap Auth Interface
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- | Turn a page from the database into 'AuthUser'
--docToAuthUser :: Document -> Maybe AuthUser
--docToAuthUser v = do
--  uid <- DB.lookup "_id" v
--  pass <- DB.lookup "password" v
--  salt <- DB.lookup "salt" v
--  return emptyAuthUser
--            { userId = Just uid 
--            , userEmail = DB.lookup "email" v
--            , userPassword = Just $ Encrypted pass 
--            , userSalt = Just salt
--            , userActivatedAt = DB.lookup "activated_at" v
--            , userSuspendedAt = DB.lookup "suspended_at" v
--            , userPersistenceToken = DB.lookup "persistence_token" v
--            , userCreatedAt = DB.lookup "created_at" v
--            , userUpdatedAt = DB.lookup "updated_at" v
--            , userCurrentLoginAt = DB.lookup "current_login_at" v
--            , userLastLoginAt = DB.lookup "last_login_at" v
--            , userCurrentLoginIp = DB.lookup "current_login_ip" v
--            , userLastLoginIp = DB.lookup "last_login_ip" v
--            , userLoginCount = maybe 0 id $ DB.lookup "login_count" v
--            , userFailedLoginCount = maybe 0 id $ DB.lookup "failed_login_count" v
--            }


------------------------------------------------------------------------------
-- | Turn an 'AuthUser' into a 'Document' ready to be commited to DB.
--authUserToDoc :: AuthUser -> Document
--authUserToDoc usr = fields'
--  where
--    fields' = foldr step [] fields
--    step x acc = maybe acc (: acc) x
--    decidePass (Encrypted x) = Just ("password" =: x)
--    decidePass _ = error "Can't save user without a proper password set"
--    fields = 
--      [ userId usr >>= return . ("_id" =:)    -- only if present
--      , userCreatedAt usr >>= return . ("created_at" =:)  -- only if present
--      , Just $ ("email" =: userEmail usr)
--      , userPassword usr >>= decidePass
--      , Just $ ("salt" =: userSalt usr)
--      , Just $ ("activated_at" =: userActivatedAt usr)
--      , Just $ ("suspended_at" =: userSuspendedAt usr)
--      , Just $ ("persistence_token" =: userPersistenceToken usr)
--      , Just $ ("current_login_at" =: userCurrentLoginAt usr)
--      , Just $ ("last_login_at" =: userLastLoginAt usr)
--      , Just $ ("current_login_ip" =: userCurrentLoginIp usr)
--      , Just $ ("last_login_ip" =: userLastLoginIp usr)
--      , Just $ ("login_count" =: userLoginCount usr)
--      , Just $ ("failed_login_count" =: userFailedLoginCount usr)
--      ]


--instance (MonadAuth m, MonadMongoDB m) => MonadAuthUser m Document where

--  getUserInternal uid = do
--    t' <- fmap u authUserTable
--    r <- withDB' $ findOne (select ["_id" =: uid] t')
--    return $ do
--      d <- r 
--      (,) <$> docToAuthUser d <*> r


--  getUserByRememberToken t = do
--    t' <- fmap u authUserTable
--    r <- withDB' $ findOne (select ["persistence_token" =: t] t')
--    return $ do
--      d <- r
--      (,) <$> docToAuthUser d <*> r


--  getUserExternal (EUId ps) = do
--    lookup_keys <- authAuthenticationKeys
--    t' <- fmap u authUserTable
--    r <- withDB' $ findOne (select (buildConditions lookup_keys) t')
--    return $ do
--      d <- r 
--      (,) <$> docToAuthUser d <*> r
--    where 
--      buildConditions ks = map cond ks
--        where cond k = bs2cs k =: (fmap bs2cs $ lp k ps)


--  saveAuthUser (user, d0) = do
--    t' <- fmap u authUserTable
--    user' <- updateUser
--    d <- addTimeStamps $ authUserToDoc user'
--    let d' = d `DB.merge` d0
--    case userId user of
--      Just _ -> do    -- Existing user
--        withDB' $ save t' d' >> return Nothing
--        return . Just $ user'
--      Nothing -> do   -- New user
--        uid <- withDB' $ insert t' d' 
--        return . Just $ user' { userId = cast' uid }
--    where
--      updateUser = case userPassword user of
--        Just (ClearText x) -> updateUserPass x
--        Nothing -> error "Can't save user without any form of password"
--        _ -> return user
--      updateUserPass x = do
--        (newsalt, newpass) <- mkAuthCredentials x
--        return $ user { userPassword = Just (Encrypted newpass)
--                      , userSalt = Just newsalt }