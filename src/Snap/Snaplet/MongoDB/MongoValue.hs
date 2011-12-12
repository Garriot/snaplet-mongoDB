{-# LANGUAGE OverloadedStrings, MultiParamTypeClasses, FlexibleInstances, FlexibleContexts, TypeFamilies #-}
{-# LANGUAGE TupleSections, TypeSynonymInstances, RankNTypes, ScopedTypeVariables, IncoherentInstances #-}

module Snap.Snaplet.MongoDB.MongoValue
       ( MongoValue (..)
       , BSON.Value (..)
--       , BSON.Document (..)
       , BSON.Field (..)
       , BSON.ObjectId (..)
       , nullObjectId
       , expected
--       , lookMaybe
       , lookupThrow
       ) where

import           Prelude hiding (lookup, or)
import           Control.Applicative
import           Control.Monad.Error
import           Data.Bson (Field ((:=)), ObjectId (..))
import qualified Data.Bson as BSON
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import           Data.Char
import qualified Data.CompactString.UTF8 as CS
import           Data.Int
import           Data.List (find)
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.Encoding as LT
import           Data.Time
import           Data.Time.Clock.POSIX
import           Text.Printf



-- | This type class represents all types which can be marshalled too and from the BSON format.
class MongoValue a where
  toValue   :: a -> BSON.Value
  fromValue :: (Applicative m, Monad m) => BSON.Value -> ErrorT String m a


expected :: (Monad m) => String -> BSON.Value -> ErrorT String m a
expected what was =
  throwError $ printf "Expected %s; found %s" what (describeType was)
  where
    describeType :: BSON.Value -> String
    describeType (BSON.Float   _) = "Float"
    describeType (BSON.String  _) = "String"
    describeType (BSON.Doc     _) = "Document"
    describeType (BSON.Array   n) = printf "Array (BSON.with %i elements)" (length n)
    describeType (BSON.Bin     _) = "Binary"
    describeType (BSON.Fun     _) = "Function"
    describeType (BSON.Uuid    _) = "UUID"
    describeType (BSON.Md5     _) = "MD5"
    describeType (BSON.UserDef _) = "UserDefined"
    describeType (BSON.ObjId   _) = "ObjectId"
    describeType (BSON.Bool    _) = "Bool"
    describeType (BSON.UTC     _) = "UTCTime"
    describeType (BSON.Null     ) = "null"
    describeType (BSON.RegEx   _) = "RegEx"
    describeType (BSON.JavaScr _) = "JavaScript"
    describeType (BSON.Sym     _) = "Symbol"
    describeType (BSON.Int32   _) = "Int32"
    describeType (BSON.Int64   _) = "Int64"
    describeType (BSON.Stamp   _) = "Stamp"
    describeType (BSON.MinMax  _) = "MinMax"


-- | Represents a null 'ObjectId' (all zeros).
nullObjectId :: ObjectId
nullObjectId = Oid 0 0

lookMaybe :: T.Text -> BSON.Document -> Maybe BSON.Value
lookMaybe name doc =
  let name' = textToCompactString name in maybe Nothing (Just . BSON.value) $ find ((name' ==) . BSON.label) doc

lookupThrow :: (Applicative m, Monad m, MongoValue a) => T.Text -> BSON.Document -> ErrorT String m a
lookupThrow name doc =
  case lookMaybe name doc of
    Just val -> fromValue val
    Nothing  -> throwError $ printf "Could not find field '%s'" (T.unpack name)

instance MongoValue BSON.Value where
  toValue = id
  fromValue = return . id

instance MongoValue BSON.Document where
  toValue                = BSON.Doc
  fromValue (BSON.Doc d) = pure d
  fromValue v            = expected "Document" v

instance MongoValue () where
  toValue _           = BSON.Null
  fromValue BSON.Null = pure ()
  fromValue v         = expected "null" v

instance MongoValue Bool where
  toValue                  = BSON.Bool
  fromValue (BSON.Bool x)  = pure x
  fromValue v              = expected "boolean" v

instance MongoValue BSON.UString where
  toValue                              = BSON.String
  fromValue (BSON.String x           ) = pure x
  fromValue (BSON.Sym (BSON.Symbol x)) = pure x
  fromValue v                          = expected "string or symbol" v

instance MongoValue T.Text where
  toValue                              = BSON.String . textToCompactString
  fromValue (BSON.String x           ) = pure $! compactStringToText x
  fromValue (BSON.Sym (BSON.Symbol x)) = pure $! compactStringToText x
  fromValue v                          = expected "string or symbol" v

instance MongoValue LT.Text where
  toValue                              = BSON.String . lazyTextToCompactString
  fromValue (BSON.String x           ) = pure $! compactStringToLazyText x
  fromValue (BSON.Sym (BSON.Symbol x)) = pure $! compactStringToLazyText x
  fromValue v                          = expected "string or symbol" v

instance MongoValue (Maybe BSON.Value) where
  toValue Nothing  = BSON.Null
  toValue (Just x) = x
  
  fromValue BSON.Null = pure Nothing
  fromValue x         = pure (Just x)

instance (MongoValue a) => MongoValue (Maybe a) where
  toValue Nothing  = BSON.Null
  toValue (Just x) = toValue x
  
  fromValue BSON.Null = pure Nothing
  fromValue x         = Just <$> (fromValue x)


instance (MongoValue a) => MongoValue [a] where
  toValue                  = BSON.Array . map toValue
  fromValue (BSON.Array x) = mapM fromValue x
  fromValue v              = expected "array" v

instance (MongoValue a, MongoValue b) => MongoValue (Either a b) where
  toValue (Left  x) = BSON.Doc [ "_type" := (BSON.String $ BSON.u "Left" ), "value" := toValue x ]
  toValue (Right y) = BSON.Doc [ "_type" := (BSON.String $ BSON.u "Right"), "value" := toValue y ]
  
  fromValue (BSON.Doc doc) = do
    side <- fmap (map toLower) $ BSON.lookup "_type" doc
    case side of
      ("left"  :: String) -> (return .  Left) =<< fromValue =<< BSON.look "value" doc
      ("right" :: String) -> (return . Right) =<< fromValue =<< BSON.look "value" doc
      other              -> throwError $ "Expected either 'left' or 'right', found '" ++ other ++ "'"
  fromValue v = expected "document" v

roundTo :: (RealFrac a) => a -> a -> a
roundTo mult n = fromIntegral (round (n / mult)) * mult

instance MongoValue UTCTime where
  toValue                = BSON.UTC . posixSecondsToUTCTime . roundTo (1 / 1000) . utcTimeToPOSIXSeconds
  fromValue (BSON.UTC x) = pure x
  fromValue v            = expected "UTCTime" v

instance MongoValue POSIXTime where
  toValue                = BSON.UTC . posixSecondsToUTCTime . roundTo (1 / 1000)
  fromValue (BSON.UTC x) = pure $! utcTimeToPOSIXSeconds x
  fromValue v            = expected "UTCTime" v

instance MongoValue BS.ByteString where
  toValue                              = BSON.Bin . BSON.Binary
  fromValue (BSON.Bin (BSON.Binary x)) = pure x
  fromValue v                          = expected "binary" v

instance MongoValue BSL.ByteString where
  toValue                              = BSON.Bin . BSON.Binary . BS.concat . BSL.toChunks
  fromValue (BSON.Bin (BSON.Binary x)) = pure $! BSL.fromChunks [x]
  fromValue v                          = expected "binary" v



fitInt :: forall m b a. (Applicative m, Monad m, PrintfArg a, Integral a, Integral b, Bounded b) => String -> a -> ErrorT String m b
fitInt t n =
  let l = minBound :: b
      h = maxBound :: b
  in if fromIntegral l <= n && n <= fromIntegral h
        then pure $! fromIntegral n
        else throwError $ printf "Integer value %i was out of range for type %s" n t


instance MongoValue Int32 where
  toValue                  = BSON.Int32
  fromValue (BSON.Int32 x) = pure x
  fromValue (BSON.Int64 x) = fitInt "Int32" x
  fromValue (BSON.Float x) = pure $! round x
  fromValue v              = expected "Int32, Int64 or Float" v

instance MongoValue Int64 where
  toValue                  = BSON.Int64
  fromValue (BSON.Int32 x) = pure $! fromIntegral x
  fromValue (BSON.Int64 x) = pure x
  fromValue (BSON.Float x) = pure $! round x
  fromValue v              = expected "Int32, Int64 or Float" v

instance MongoValue Int where
  toValue                  = BSON.Int64 . fromIntegral
  fromValue (BSON.Int32 x) = pure $! fromIntegral x
  fromValue (BSON.Int64 x) = pure $! fromEnum x
  fromValue (BSON.Float x) = pure $! round x
  fromValue v              = expected "Int32, Int64 or Float" v

instance MongoValue Integer where
  toValue                  = BSON.Int64 . fromIntegral
  fromValue (BSON.Int32 x) = pure $! fromIntegral x
  fromValue (BSON.Int64 x) = pure $! fromIntegral x
  fromValue (BSON.Float x) = pure $! round x
  fromValue v              = expected "Int32, Int64 or Float" v

instance MongoValue Float where 
  toValue                  = BSON.Float . realToFrac
  fromValue (BSON.Float x) = pure $! realToFrac x
  fromValue (BSON.Int32 x) = pure $! fromIntegral x
  fromValue (BSON.Int64 x) = pure $! fromIntegral x
  fromValue v              = expected "Int32, Int64 or Float" v

instance MongoValue Double where
  toValue                  = BSON.Float
  fromValue (BSON.Float x) = pure x
  fromValue (BSON.Int32 x) = pure $! fromIntegral x
  fromValue (BSON.Int64 x) = pure $! fromIntegral x
  fromValue v              = expected "Int32 or Int64" v
  
instance MongoValue ObjectId where
  toValue                  = BSON.ObjId
  fromValue (BSON.ObjId x) = pure x
  fromValue v              = expected "ObjectId" v

instance (MongoValue a, MongoValue b) => MongoValue (a, b) where
  toValue (x, y)            = BSON.Array [toValue x, toValue y]
  fromValue v @ (BSON.Array xs) =
    case xs of
      [x, y] -> (,) <$> fromValue x <*> fromValue y
      _      -> expected "Array (with 2 elements)" v
  fromValue v               = expected "Array (with 2 elements)" v

instance (MongoValue a, MongoValue b, MongoValue c) => MongoValue (a, b, c) where
  toValue (x, y, z)         = BSON.Array [toValue x, toValue y, toValue z]
  fromValue v @ (BSON.Array xs) =
    case xs of
      [x, y, z] -> (,,) <$> fromValue x <*> fromValue y <*> fromValue z
      _         -> expected "Array (with 3 elements)" v
  fromValue v               = expected "Array (with 3 elements)" v


-- | Instance of the 'MongoValue' type class for a map of strict 'T.Text' to some value which also an instance of the
-- 'MongoValue' type class. This type class is provided for more efficient storage of maps with textual keys.
--
-- For example, the map @M.fromList [("cat", 1), ("dog", 2), ("mat", 3)]@ would yield the JSON equivalent of:
-- @{ cat: 1, dog: 2, mat: 3 }@.
--
instance (MongoValue val) => MongoValue (M.Map T.Text val) where
  toValue m = BSON.Doc $ map (\(k, v) -> textToCompactString k := toValue v) $ M.toList m
  fromValue (BSON.Doc m) = do
    elements <- mapM (\ (k := v) -> do
                         val <- fromValue v
                         pure (compactStringToText k, val)) m
    pure $! M.fromList elements
  fromValue v = expected "Document" v

-- | Instance of the 'MongoValue' type class for a map of lazy 'LT.Text' to some value which also an instance of the
-- 'MongoValue' type class. This type class is provided for more efficient storage of maps with textual keys.
--
-- For example, the map @M.fromList [("cat", 1), ("dog", 2), ("mat", 3)]@ would yield the JSON equivalent of:
-- @{ cat: 1, dog: 2, mat: 3 }@.
--
instance (MongoValue val) => MongoValue (M.Map LT.Text val) where
  toValue m = BSON.Doc $ map (\(k, v) -> lazyTextToCompactString k := toValue v) $ M.toList m
  fromValue (BSON.Doc m) = do
    elements <- mapM (\ (k := v) -> do
                         val <- fromValue v
                         pure (compactStringToLazyText k, val)) m
    pure $! M.fromList elements
  fromValue v = expected "Document" v


instance (Ord key, MongoValue key, MongoValue val) => MongoValue (M.Map key val) where
  toValue = toValue . M.toList
  fromValue v = M.fromList <$> fromValue v
    


compactStringToText :: CS.CompactString -> T.Text
compactStringToText = T.decodeUtf8 . CS.toByteString

textToCompactString :: T.Text -> CS.CompactString
textToCompactString = CS.fromByteString_ . T.encodeUtf8

compactStringToLazyText :: CS.CompactString -> LT.Text
compactStringToLazyText =
  LT.decodeUtf8 . (\x -> BSL.fromChunks [x]) . CS.toByteString

lazyTextToCompactString :: LT.Text -> CS.CompactString
lazyTextToCompactString =
  CS.fromByteString_ . BS.concat . BSL.toChunks . LT.encodeUtf8

-- Local Variables:
-- mode                  : Haskell
-- fill-column           : 120
-- default-justification : left
-- End:
