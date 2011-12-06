{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleContexts #-}
-- |
-- Module : Snap.Snaplet.MongoDB.MongoEntity
-- Description : Provides the MongoEntity type class.
-- Copyright : (C) 2011 Massive Tactical Limited
-- License : BSD3
--
-- Maintainer : Blake Rain <blake.rain@massivetactical.com>
-- Stability : Provisional
-- Portability : Portable
--
-- Provides the MongoEntity type class.
--

module Snap.Snaplet.MongoDB.MongoEntity
       ( MongoEntity (..)
       ) where


import           Control.Applicative
import           Control.Monad.Error
import qualified Data.Bson        as BSON
import           Data.Monoid
import qualified Database.MongoDB as DB
import           Snap.Snaplet.MongoDB.MongoValue
import           Numeric (readHex)
import           Text.Read
import qualified Text.ParserCombinators.ReadP as R
import qualified Text.ParserCombinators.ReadPrec as R (lift, readS_to_Prec)


-- | Any type that is an instance of this type class can be stored and retrieved as an object from a MongoDB collection.
class (Show (Key a), MongoValue (Key a)) => MongoEntity a where
  data Key a
  data Filter a
  data Document a
  
  -- | Convert an 'ObjectId' to a 'Key'.
  toKey               :: ObjectId -> Key a
  -- | Convert a 'Key' to an 'ObjectId'
  fromKey             :: Key a -> ObjectId
  
  -- | Convert a 'BSON.Document' to a 'Document'.
  toDocument          :: BSON.Document -> Document a
  -- | Convert a 'Document' to a 'BSON.Document'.
  fromDocument        :: Document a -> BSON.Document
  
  -- | Yields the name of the collection to which this type belongs.
  collectionName      :: a -> DB.Collection
  -- | Yields the name of the corresponding field in a collection for the given filter.
  filterFieldName     :: Filter a -> BSON.Label
  
  -- | Encode an object into a 'BSON.Document' that can be stored in a collection.
  encodeDocument      :: a -> Document a
  -- | Decode a 'BSON.Document' into this type; possibly failing.
  decodeDocument      :: (Applicative m, Monad m) => Document a -> ErrorT String m a


instance (MongoEntity a) => MongoValue (Key a) where
  toValue = BSON.ObjId . fromKey
  fromValue (BSON.ObjId o) = return $ toKey o
  fromValue v              = expected "ObjectId" v

instance (MongoEntity a) => MongoValue (Document a) where
  toValue = toValue . fromDocument
  fromValue (BSON.Doc doc) = return $ toDocument doc
  fromValue v              = expected "Document" v

instance (MongoEntity a) => Monoid (Document a) where
  mempty = toDocument []
  mappend x y = toDocument (fromDocument x ++ fromDocument y)

instance (MongoEntity a) => Show (Key a) where
  show = show . fromKey

instance (MongoEntity a) => Read (Key a) where
  readPrec = do
    [(x, "")] <- readHex <$> R.lift (R.count 8 R.get)
    y <- R.readS_to_Prec $ const readHex
    return (toKey (BSON.Oid x y))



-- Local Variables:
-- mode                  : Haskell
-- fill-column           : 120
-- default-justification : left
-- End: