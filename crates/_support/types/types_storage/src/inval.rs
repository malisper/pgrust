use ::types_core::{uint32, Oid};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct PgClassShape {
    pub oid: Oid,
    pub relisshared: bool,
}

// PrepareToInvalidateCacheTuple's per-catcache (cacheId, hashValue, dbId) row, by value across the seam.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct PrepareToInvalidateCacheTuple {
    pub cache_id: i32,
    pub hash_value: uint32,
    pub db_id: Oid,
}
