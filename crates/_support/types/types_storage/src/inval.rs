//! Request/shape vocabulary for `backend/utils/cache/inval.c`.
//!
//! These are the cross-seam value types the cache-invalidation dispatcher
//! exchanges with its catalog/syscache/catcache owners, kept in the shared
//! `types` crate so the seam declarations (which must not depend on a concrete
//! backend crate) can name them. They are faithful decodings of the C
//! `Form_pg_class` fields / catcache-invalidation callback arguments — not
//! opaque handles.

use types_core::{uint32, Oid};

/// The two `Form_pg_class` fields `inval.c` reads via `GETSTRUCT` when a
/// `pg_class` tuple drives a relcache invalidation: the relation's OID and
/// whether it is shared across databases.
///
/// In C the dispatcher reads `classtup->oid` and `classtup->relisshared` from
/// the deformed `Form_pg_class`; the deform is owned by the syscache layer, so
/// this struct is the decoded result that crosses the seam.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct PgClassShape {
    /// `Form_pg_class.oid`.
    pub oid: Oid,
    /// `Form_pg_class.relisshared`.
    pub relisshared: bool,
}

/// One catcache invalidation request produced by `PrepareToInvalidateCacheTuple`
/// (catcache.c). In C the routine invokes a `(cacheId, hashValue, dbId)`
/// callback once per affected catcache; the seam instead returns a `Vec` of
/// these requests for the dispatcher to register, one per callback invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct PrepareToInvalidateCacheTuple {
    /// `cacheId` — the catcache id (`int`).
    pub cache_id: i32,
    /// `hashValue` — the tuple's hash value within that catcache.
    pub hash_value: uint32,
    /// `dbId` — database OID the entry belongs to (`InvalidOid` if shared).
    pub db_id: Oid,
}
