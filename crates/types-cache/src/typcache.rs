//! Type-cache seam-signature vocabulary (`utils/cache/typcache.c`,
//! `utils/typcache.h`): the catalog-row mirrors and opaque planned-constraint
//! token the typcache's outward seams marshal.
//!
//! The cache's own structures (`TypeCacheEntry`, the record/enum/domain side
//! tables) live in the owning crate; only the cross-subsystem signature types
//! belong here.

use alloc::string::String;

use types_core::primitive::Oid;

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
