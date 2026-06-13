//! Type-cache vocabulary (`utils/typcache.h`, `utils/cache/typcache.c`):
//! the cached per-type `TypeCacheEntry` (trimmed to the range/multirange
//! selectivity consumers) plus the cross-subsystem seam-signature types the
//! typcache's outward seams marshal.
//!
//! The cache's own internal structures (the record/enum/domain side tables)
//! live in the owning crate; only the cross-subsystem signature types and the
//! shared `TypeCacheEntry` belong here.

extern crate alloc;
use alloc::boxed::Box;
use alloc::string::String;

use types_core::fmgr::FmgrInfo;
use types_core::primitive::Oid;

/// `TypeCacheEntry` (`typcache.h`), trimmed. For a range type
/// `rng_cmp_proc_finfo` / `rng_subdiff_finfo` are the subtype's `cmp` /
/// `subdiff` support functions, `rng_collation` the collation passed to them,
/// and `rngelemtype` the element type's entry. For a multirange type `rngtype`
/// points at the corresponding range type's entry.
#[derive(Clone, Debug, Default)]
pub struct TypeCacheEntry {
    /// `type_id` -- the type's own OID.
    pub type_id: Oid,
    /// `typlen` -- the type's `pg_type.typlen` (`-1` varlena, `-2` cstring).
    pub typlen: i16,
    /// `typbyval` -- the type's `pg_type.typbyval`.
    pub typbyval: bool,
    /// `typalign` -- the type's `pg_type.typalign` (`'c'`/`'s'`/`'i'`/`'d'`).
    pub typalign: i8,
    /// `typstorage` -- the type's `pg_type.typstorage` (`'p'`/`'e'`/`'m'`/`'x'`).
    pub typstorage: i8,
    /// `rng_collation` -- collation for the range's comparison/subdiff calls.
    pub rng_collation: Oid,
    /// `rng_cmp_proc_finfo` -- the subtype's `cmp` support function.
    pub rng_cmp_proc_finfo: FmgrInfo,
    /// `rng_canonical_finfo` -- the range type's optional canonicalization
    /// function (`fn_oid == InvalidOid` when absent, i.e. a continuous subtype).
    pub rng_canonical_finfo: FmgrInfo,
    /// `rng_subdiff_finfo` -- the subtype's optional `subdiff` support function
    /// (`fn_oid == InvalidOid` when absent).
    pub rng_subdiff_finfo: FmgrInfo,
    /// `hash_proc_finfo` -- the type's hash support function (used by
    /// `hash_range` on the range element type's entry; `fn_oid == InvalidOid`
    /// when absent).
    pub hash_proc_finfo: FmgrInfo,
    /// `hash_extended_proc_finfo` -- the type's extended (64-bit, seeded) hash
    /// support function (used by `hash_range_extended`; `fn_oid == InvalidOid`
    /// when absent).
    pub hash_extended_proc_finfo: FmgrInfo,
    /// `rngelemtype` -- the range element type's cache entry (range types only).
    pub rngelemtype: Option<Box<TypeCacheEntry>>,
    /// `rngtype` -- the range type's cache entry (multirange types only).
    pub rngtype: Option<Box<TypeCacheEntry>>,
}

/// Opaque handle to a planned domain CHECK `Expr *` (the output of
/// `stringToNode(conbin)` + `expression_planner()`). The planner is a
/// genuinely-external neighbor (inherited opacity); the typcache stores and
/// hands these handles to `ExecInitExpr` without inspecting them. `0` models a
/// NULL expr (NOT NULL constraints carry no `check_expr`).
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct ExprHandle(pub u64);

impl ExprHandle {
    /// No expression (the NOT NULL constraint case).
    pub const NULL: ExprHandle = ExprHandle(0);

    /// Whether this is the null expr.
    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }
}

/// Opaque handle to a compiled `ExprState *` (the output of `ExecInitExpr`).
/// The executor is a genuinely-external neighbor (inherited opacity). `0`
/// models a NULL exprstate.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct ExprStateHandle(pub u64);

impl ExprStateHandle {
    /// No compiled expression state.
    pub const NULL: ExprStateHandle = ExprStateHandle(0);
}

/// Opaque token for a "Domain constraints" / ref memory context owned by the
/// not-yet-ported memory-context owner. The typcache passes it back to the
/// owner's seams to reparent / delete the context.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct DomainCtxHandle(pub u64);

/// `DomainConstraintState` (execnodes.h) — one compiled domain constraint, in
/// the typcache's in-crate form. `check_expr`/`check_exprstate` are opaque
/// planner/executor handles (inherited opacity); `constrainttype` and `name`
/// are the typcache's own data.
#[derive(Clone, Debug)]
pub struct DomainConstraintState {
    /// `DOM_CONSTRAINT_CHECK` or `DOM_CONSTRAINT_NOTNULL`.
    pub constrainttype: i32,
    pub name: String,
    /// Planned CHECK `Expr *` (NULL for NOT NULL constraints).
    pub check_expr: ExprHandle,
    /// Compiled `ExprState *` (only set by `prep_domain_constraints`).
    pub check_exprstate: ExprStateHandle,
}

/// One raw CHECK constraint row scanned from `pg_constraint` for a domain:
/// the constraint name and its `conbin` node-string (`TextDatumGetCString`).
/// The typcache plans each (`stringToNode` + `expression_planner`) via the
/// `plan_check_expr` seam into the lazily-created "Domain constraints" context.
#[derive(Clone, Debug)]
pub struct DomainCheckConstraintRow {
    pub conname: String,
    pub conbin: String,
}

/// Result of scanning one level of the domain stack
/// (`SearchSysCache1(TYPEOID)` reading `typtype`/`typnotnull`/`typbasetype`),
/// as consumed by the in-crate `load_domaintype_info` orchestration.
#[derive(Clone, Debug, Default)]
pub struct DomainLevelScan {
    /// Whether this catalog row is still a domain (`typtype == 'd'`). When
    /// false the crawl stops.
    pub is_domain: bool,
    /// `typnotnull` of this level's `pg_type` row.
    pub typnotnull: bool,
    /// `typbasetype` — the next type in the domain stack.
    pub typbasetype: Oid,
}

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
