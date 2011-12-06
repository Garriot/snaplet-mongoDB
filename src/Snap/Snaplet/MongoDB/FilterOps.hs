{-# LANGUAGE OverloadedStrings #-}
-- |
-- Module      : Snap.Snaplet.MongoDB.FilterOps
-- Description : Provides the filtering operations.
-- Copyright   : (C) 2011 Massive Tactical Limited
-- License     : BSD3
--
-- Maintainer  : Blake Rain <blake.rain@massivetactical.com>
-- Stability   : Provisional
-- Portability : Portable
--
-- Provides the filtering operations.
--

module Snap.Snaplet.MongoDB.FilterOps
       ( FilterOp
         
       , (==?)
       , (/=?)
       , eq, ne, lt, lte, gt, gte
       , or, ors
       , isIn, notIn
               
       , UpdateOp
       , (==:)
       , set
       , inc, dec
       , pop, push, pull, pushAll, pullAll
       , addToSet, addManyToSet
       ) where

import           Prelude hiding (or)
import qualified Data.Bson as BSON
import           Snap.Snaplet.MongoDB.MongoValue
import           Snap.Snaplet.MongoDB.MongoEntity


infix 0 ==?  -- Alias to eq operation
infix 0 /=?  -- Alias to neq operation
infix 0 ==:  -- Alias to set operation

type FilterOp = BSON.Field

(==?), (/=?), (==:) :: (MongoEntity a, MongoValue v) => (v -> Filter a) -> v -> FilterOp
(==?) = eq
(/=?) = ne
(==:) = set

stdFilterDef :: (MongoEntity a, MongoValue v) => BSON.Label -> (v -> Filter a) -> v -> FilterOp
stdFilterDef op f v =
  filterFieldName (f undefined) := BSON.Doc [op := toValue v]

eq, ne, lt, lte, gt, gte :: (MongoEntity a, MongoValue v) => (v -> Filter a) -> v -> FilterOp
eq f v = filterFieldName (f undefined) := toValue v
ne     = stdFilterDef "$ne"
lt     = stdFilterDef "$lt"
lte    = stdFilterDef "$lte"
gt     = stdFilterDef "$gt"
gte    = stdFilterDef "$gte"

or :: BSON.Field -> BSON.Field -> FilterOp
or x y = "$or" := BSON.Doc [x, y]

ors :: [BSON.Field] -> BSON.Field
ors fs = "$or" := BSON.Doc fs

isIn, notIn :: (MongoEntity a, MongoValue v) => (v -> Filter a) -> [v] -> FilterOp
isIn  f vs = filterFieldName (f undefined) := BSON.Doc [  "$in" := BSON.Array (map toValue vs) ]
notIn f vs = filterFieldName (f undefined) := BSON.Doc [ "$nin" := BSON.Array (map toValue vs) ]


type UpdateOp = BSON.Field

set :: (MongoEntity a, MongoValue v) => (v -> Filter a) -> v -> UpdateOp
set f v = "$set" := BSON.Doc [ filterFieldName (f v) := toValue v ]

inc, dec :: (MongoEntity a, Num v, MongoValue v) => (v -> Filter a) -> v -> UpdateOp
inc f v = "$inc" := BSON.Doc [ filterFieldName (f v) := toValue v ]
dec f v = "$dec" := BSON.Doc [ filterFieldName (f v) := toValue v ]

push, addToSet, pull :: (MongoEntity a, MongoValue v) => ([v] -> Filter a) -> v -> UpdateOp
push     f v = "$push" := BSON.Doc [ filterFieldName (f [v]) := toValue v ]
addToSet f v = "$addToSet" := BSON.Doc [ filterFieldName (f undefined) := toValue v ]
pull     f v = "$pull" := BSON.Doc [ filterFieldName (f undefined) := toValue v ]

pushAll, pullAll, addManyToSet :: (MongoEntity a, MongoValue v) => ([v] -> Filter a) -> [v] -> UpdateOp
pushAll      f v = "$pushAll" := BSON.Doc [ filterFieldName (f undefined) := BSON.Array (map toValue v) ]
pullAll      f v = "$pullAll" := BSON.Doc [ filterFieldName (f undefined) := BSON.Array (map toValue v) ]
addManyToSet f v = "$addToSet" := BSON.Doc [ filterFieldName (f undefined) := BSON.Doc [ "$each" := BSON.Array (map toValue v) ] ]

pop :: (MongoEntity a, MongoValue v) => ([v] -> Filter a) -> UpdateOp
pop f = "$pop" := BSON.Doc [ filterFieldName (f undefined) := BSON.Int32 1 ]



-- Local Variables:
-- mode                  : Haskell
-- fill-column           : 120
-- default-justification : left
-- End: