#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]
// This crate owns the relcache entry store. The `RelationIdCache` is a real
// dynahash table (the C `RelationIdCache`, merged C-faithful raw-pointer
// dynahash) whose entries hold a `*mut RelationData` into a heap-owned
// descriptor. That C `Relation`-pointer identity is the substrate this crate
// owns: it stays stable across an in-place `RelationRebuildRelation` content
// swap, exactly as the C pointer does. The raw-pointer cache + the leaked
// `Box<RelationData>` spine require `unsafe`; we use `deny` (not `forbid`) so
// the audited sites opt in with `#[allow(unsafe_code)]`, each carrying its
// discharging invariant inline (mirrors dynahash's own blessed exception).
#![deny(unsafe_code)]

//! `backend/utils/cache/relcache.c` — the relation-descriptor cache (~5.2k C
//! lines): the root of the cache chain (catcache, lsyscache, and many
//! consumers gate on it).
//!
//! # The entry store is OWN logic, not a trimmed seam type
//!
//! The cross-unit value-slice [`types_rel::RelationData`] is what an *open
//! relation* looks like when it crosses a seam into another unit. It is NOT
//! the relcache entry. This crate owns a **real, mutable**
//! [`RelationData`](entry::RelationData) entry carrying the full `rd_*`
//! surface (`rd_refcnt`/`rd_isvalid`/`rd_isnailed`/`rd_rel`/`rd_att`/
//! `rd_indexlist`/`rd_tableam`/`rd_indam`/the derived-cache fields), stored in
//! the real [`RelationIdCache`](core_entry_store) dynahash keyed by `Oid` (the
//! C `RelIdCacheEnt`). The per-backend `eoxact_list`/`in_progress_list`
//! bookkeeping lives alongside it in [`thread_local`] state — a PostgreSQL
//! backend is single-threaded, so the C file-statics map to one thread-local
//! cell (matching the other ported cache crates).
//!
//! # Family decomposition
//!
//! * [`core_entry_store`] — the real entry struct + `RelationIdCache` dynahash
//!   + refcount lifecycle, `RelationIdGetRelation`/`RelationClose`/`Increment`/
//!   `DecrementReferenceCount`, cache insert/delete/lookup, resowner glue.
//!   **Owns + installs the relcache seams.** (REAL — landed in this scaffold.)
//! * [`build`] — `RelationBuildDesc` orchestration (in-crate) + `ScanPgRelation`
//!   (the catalog scan is seamed via `systable_*`/`SearchSysCache`),
//!   `AllocateRelationDesc`, `RelationBuildTupleDesc`, `RelationParseRelOptions`,
//!   `formrdesc`+`BuildHardcodedDescriptor`, `AttrDefaultFetch`,
//!   `CheckNNConstraintFetch`.
//! * [`index`] — `RelationInitIndexAccessInfo`, `IndexSupportInitialize`,
//!   `LookupOpclassInfo` + opclass cache, `InitIndexAmRoutine`,
//!   `InitTableAmRoutine`, `RelationInitTableAccessMethod`,
//!   `RelationReloadIndexInfo`/`Nailed`, `RelationInitPhysicalAddr`.
//! * [`invalidate`] — `RelationClearRelation`/`Rebuild`/`Flush`/`Invalidate`/
//!   `Destroy` swap-contents, `RelationForgetRelation`,
//!   `RelationCacheInvalidate[Entry]`, `AtEOXact`/`AtEOSubXact_RelationCache`.
//! * [`derived`] — `RelationGetFKeyList`/`IndexList`/`StatExtList`/
//!   `PrimaryKeyIndex`/`ReplicaIndex`/`IndexExpressions`/`IndexPredicate`/
//!   `IndexAttrBitmap`/`IdentityKeyBitmap`/`ExclusionInfo`,
//!   `BuildPublicationDesc`, `RelationBuildRuleLock`.
//! * [`initfile`] — `RelationBuildLocalRelation`, `RelationSetNewRelfilenumber`,
//!   `RelationCacheInitialize`/`Phase2`/`Phase3`, `load_critical_index`,
//!   `load`/`write_relcache_init_file` BINARY CODEC (reclaimed in-crate),
//!   `RelationCacheInitFilePre`/`PostInvalidate`/`Remove`, `errtable*`.
//!
//! Only genuine cross-unit primitives are seamed: the catalog-scan primitives
//! `RelationBuildDesc`/`ScanPgRelation` use (`systable_beginscan`/`getnext` via
//! `genam`, `SearchSysCache` via `catcache`) route through the owner seam
//! (panic until the owner lands). `RelationBuildDesc`'s own orchestration and
//! the derived-list builders are relcache's OWN logic.

pub mod core_entry_store;
pub mod build;
pub mod index;
pub mod invalidate;
pub mod derived;
pub mod initfile;

mod seams;
pub use seams::init_seams;

// Re-export the real owned entry type.
pub use core_entry_store::entry::{self, RelationData};

use types_core::primitive::Oid;

/// The C `Relation`: a typed pointer to a relcache entry. In this crate it is
/// a raw `*mut RelationData` into the heap-owned descriptor that
/// `RelationIdCache` keeps alive while `rd_refcnt > 0` — the same stable
/// identity as the C pointer across an in-place rebuild.
pub type Relation = *mut RelationData;

/// `MAX_EOXACT_LIST` (relcache.c) — the fixed `eoxact_list[]` bound.
pub(crate) const MAX_EOXACT_LIST: usize = 32;

/// `INITRELCACHESIZE` (relcache.c) — initial `RelationIdCache` size.
pub(crate) const INITRELCACHESIZE: i64 = 400;

/// `REPLICA_IDENTITY_DEFAULT`/`_NOTHING`/`_INDEX`/`_FULL` (pg_class.h).
pub(crate) const REPLICA_IDENTITY_DEFAULT: i8 = b'd' as i8;
pub(crate) const REPLICA_IDENTITY_NOTHING: i8 = b'n' as i8;
pub(crate) const REPLICA_IDENTITY_INDEX: i8 = b'i' as i8;
pub(crate) const REPLICA_IDENTITY_FULL: i8 = b'f' as i8;

/// `RelationGetRelid(relation)` (utils/rel.h) on the owned entry.
#[inline]
pub(crate) fn relation_get_relid(rel: &RelationData) -> Oid {
    rel.rd_id
}

/// `elog(WARNING, msg)` — emit a non-fatal warning report and continue. The
/// build family's fetch routines warn (not error) on missing/null catalog rows
/// just as the C does; `ThrowErrorData` logs a WARNING and returns `Ok`.
pub(crate) fn elog_warning(message: String) -> backend_utils_error::PgResult<()> {
    backend_utils_error::elog(types_error::WARNING, message)
}
