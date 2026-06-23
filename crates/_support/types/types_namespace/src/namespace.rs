//! `catalog/namespace.h` API vocabulary.

use ::mcx::PgVec;
use ::types_core::Oid;

/// `RVR_MISSING_OK` (`catalog/namespace.h`).
pub const RVR_MISSING_OK: u32 = 1 << 0;
/// `RVR_NOWAIT` (`catalog/namespace.h`).
pub const RVR_NOWAIT: u32 = 1 << 1;
/// `RVR_SKIP_LOCKED` (`catalog/namespace.h`).
pub const RVR_SKIP_LOCKED: u32 = 1 << 2;

/// `struct _FuncCandidateList` (`catalog/namespace.h`).
///
/// The C struct is a singly-linked list whose nodes end in a flexible
/// `Oid args[FLEXIBLE_ARRAY_MEMBER]`, palloc'd in the caller's current
/// context. The owned model is a `PgVec` of nodes in the caller's `Mcx`
/// (the `next` pointer becomes the `Vec` ordering, kept in C list order:
/// most-recently-prepended first) and the flexible `args` array an owned
/// `PgVec<Oid>`.
#[derive(Debug)]
pub struct FuncCandidate<'mcx> {
    /// for internal use of namespace lookup function only.
    pub pathpos: i32,
    /// the function or operator OID (`InvalidOid` marks an ambiguous entry).
    pub oid: Oid,
    /// number of args without padding.
    pub nominalnargs: i32,
    /// number of arg types returned (padded to match requested N).
    pub nargs: i32,
    /// number of args to become variadic array.
    pub nvargs: i32,
    /// number of defaulted args.
    pub ndargs: i32,
    /// argnumbers[i] = which input arg goes at proc's arg i (empty => identity).
    pub argnumbers: PgVec<'mcx, i32>,
    /// arg types — length `nargs`.
    pub args: PgVec<'mcx, Oid>,
}

/// `FuncCandidateList` — owned list of [`FuncCandidate`], in the caller's
/// `Mcx`.
pub type FuncCandidateList<'mcx> = PgVec<'mcx, FuncCandidate<'mcx>>;

/// `TempNamespaceStatus` (`catalog/namespace.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum TempNamespaceStatus {
    /// `TEMP_NAMESPACE_NOT_TEMP` — not a temp namespace.
    NotTemp = 0,
    /// `TEMP_NAMESPACE_IDLE` — a temp namespace, but no backend is using it.
    Idle = 1,
    /// `TEMP_NAMESPACE_IN_USE` — a temp namespace currently in use.
    InUse = 2,
}

pub use TempNamespaceStatus::{
    Idle as TEMP_NAMESPACE_IDLE, InUse as TEMP_NAMESPACE_IN_USE, NotTemp as TEMP_NAMESPACE_NOT_TEMP,
};

/// `SearchPathMatcher` (`catalog/namespace.h`) — a resolved `search_path`
/// for quick re-validation against the active environment. Allocated in the
/// context the caller passed to `GetSearchPathMatcher` (the C `context`
/// argument).
#[derive(Debug)]
pub struct SearchPathMatcher<'mcx> {
    /// OIDs of explicitly named schemas (C: `List` of `Oid`).
    pub schemas: PgVec<'mcx, Oid>,
    /// implicitly prepend `pg_catalog`?
    pub addCatalog: bool,
    /// implicitly prepend temp schema?
    pub addTemp: bool,
    /// for quick detection of equality to active path (private to namespace.c).
    pub generation: u64,
}
