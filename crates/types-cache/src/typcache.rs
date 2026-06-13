//! Type-cache seam-signature vocabulary (`utils/cache/typcache.c`,
//! `utils/typcache.h`): the catalog-row mirrors and opaque planned-constraint
//! token the typcache's outward seams marshal.
//!
//! The cache's own structures (`TypeCacheEntry`, the record/enum/domain side
//! tables) live in the owning crate; only the cross-subsystem signature types
//! belong here.

use alloc::string::String;

use types_core::primitive::Oid;

/// Opaque, implementation-owned planned-constraint `List*`
/// (`List *constraints` of a `DomainConstraintCache`). `0` models NIL.
///
/// The list itself lives in the domain-constraint owner's "Domain constraints"
/// memory context (created lazily by `load_domaintype_info`); the typcache only
/// holds this token and refcounts it.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct ConstraintListHandle(pub u64);

impl ConstraintListHandle {
    /// The empty list (NIL).
    pub const NIL: ConstraintListHandle = ConstraintListHandle(0);

    /// Whether this is NIL.
    #[inline]
    pub fn is_nil(self) -> bool {
        self.0 == 0
    }
}

/// Opaque token for a "Domain constraints" / ref memory context owned by the
/// not-yet-ported domain-constraint planner. The typcache passes it back to
/// the owner's seams to reparent / delete the context.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct DomainCtxHandle(pub u64);

/// Fields read from a `pg_type` row when building a `TypeCacheEntry`.
///
/// `lookup_pg_type` mirrors `SearchSysCache1(TYPEOID, ...)` followed by reading
/// the `Form_pg_type`. `typisdefined` is reported so the caller can raise the
/// "only a shell" error 1:1. `typname` is the catalog `NameData` already
/// decoded to a Rust string (for the shell-type error message).
#[derive(Clone, Debug)]
pub struct PgTypeRow {
    pub typname: String,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: i8,
    pub typstorage: i8,
    pub typtype: i8,
    pub typisdefined: bool,
    pub typrelid: Oid,
    pub typsubscript: Oid,
    pub typelem: Oid,
    pub typarray: Oid,
    pub typcollation: Oid,
}

/// Fields read from a `pg_range` row in `load_rangetype_info`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgRangeRow {
    pub rngsubtype: Oid,
    pub rngcollation: Oid,
    pub rngsubopc: Oid,
    pub rngcanonical: Oid,
    pub rngsubdiff: Oid,
}

/// `DOM_CONSTRAINT_NOTNULL` (execnodes.h `DomainConstraintType`).
pub const DOM_CONSTRAINT_NOTNULL: i32 = 0;
/// `DOM_CONSTRAINT_CHECK`.
pub const DOM_CONSTRAINT_CHECK: i32 = 1;

/// Relcache invalidation callback signature (idiomatic).
pub type RelcacheCallbackFn = fn(relid: Oid);
/// Syscache invalidation callback signature (idiomatic).
pub type SyscacheCallbackFn = fn(cacheid: i32, hashvalue: u32);
