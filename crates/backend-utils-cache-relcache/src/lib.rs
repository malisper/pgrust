#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]
// This crate owns the relcache entry store. The `RelationIdCache` is a
// `thread_local` `RefCell<HashMap<Oid, Rc<RefCell<RelationData>>>>` (the
// `id_cache`): the C `Relation` pointer becomes a copyable `Oid` handle, and
// each descriptor lives in an `Rc<RefCell<..>>` (the safe C-shaped rendering of
// `RelationData *`) whose allocation survives HashMap rehash and the in-place
// `RelationRebuildRelation` content swap (`*cell.borrow_mut() = rebuilt`) —
// exactly the stability the C pointer's identity carries across a rebuild — and
// `Rc::strong_count` is the safe analog of "an external holder pins the
// allocation". Descriptors are reached through the scoped accessors
// `with_rel`/`with_rel_mut` (replacing the `&*ptr`/`&mut *ptr` derefs) and the
// public `with_relation`/`with_relation_mut`/`try_with_relation`; callers that
// pin across rebuilds hold a `RelationRef` RAII guard, and a holder that wants
// C's live shared pointer takes a clone of the cell via
// `relation_id_get_relation_shared`. The entry store carries NO `unsafe` (every
// access is a `RefCell` borrow). (The separate `OpClassCache` in [`index`] is a
// C-faithful raw-pointer dynahash — the inherited `OpClassCacheEnt` file-static
// — with its own audited `#[allow(unsafe_code)]` byte-cast/leaked-slice sites,
// independent of the `RelationData` store.) We use `deny` (not `forbid`) so the
// blessed sites opt in with `#[allow(unsafe_code)]` (cf. `backend-utils-mctx`).
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
//! `rd_indexlist`/`rd_tableam`/`rd_indam`/the derived-cache fields), owned in an
//! `Rc<RefCell<..>>` and stored in the [`RelationIdCache`](core_entry_store) — a
//! `thread_local` `RefCell<HashMap<Oid, Rc<RefCell<RelationData>>>>` (the
//! `id_cache`) keyed by `Oid` (the C `RelIdCacheEnt`). The [`Relation`] handle
//! is the `Oid`; the `Rc`'s allocation survives rehash and the in-place rebuild
//! swap, matching the C pointer's stability invariant, and `Rc::strong_count`
//! is the safe analog of a held `RelationData *` pinning the allocation. The per-backend
//! `eoxact_list`/`in_progress_list` bookkeeping lives alongside it in the same
//! [`thread_local`] cell — a PostgreSQL backend is single-threaded, so the C
//! file-statics map to one thread-local cell (matching the other ported cache
//! crates).
//!
//! # Family decomposition
//!
//! * [`core_entry_store`] — the real entry struct + the `id_cache`
//!   `HashMap<Oid, Rc<RefCell<RelationData>>>` + the `with_rel`/`with_relation`/
//!   `RelationRef` accessor surface + refcount lifecycle,
//!   `RelationIdGetRelation`/`RelationClose`/`Increment`/
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
pub mod plancat_ext;

mod seams;
pub use seams::init_seams;

// Re-export the real owned entry type.
pub use core_entry_store::entry::{self, RelationData};

// Re-export the safe accessor surface + the RAII pin guard (the public API the
// migrating consumers move onto).
pub use core_entry_store::{
    relation_id_get_relation_shared, try_with_relation, with_relation,
    with_relation_mut, RelationRef, RelationClose, RelationIdGetRelation,
    RelationIncrementReferenceCount, RelationDecrementReferenceCount,
};

use types_core::primitive::Oid;

/// The C `Relation`, idiomatically: the relcache entry store is keyed by the
/// relation OID, so the "pointer" callers carry IS the [`Oid`]. A live handle
/// names a descriptor owned by the `id_cache`; resolve it through the scoped
/// accessors ([`with_relation`]/[`with_relation_mut`]/[`try_with_relation`]) or
/// hold a pin across rebuilds with [`RelationRef`], or take C's live shared
/// pointer (a clone of the cache cell) via [`relation_id_get_relation_shared`].
/// The descriptor's `Rc<RefCell<..>>` allocation is stable across rehash and the
/// in-place rebuild swap, so the handle stays valid exactly as the C pointer does.
pub type Relation = Oid;

/// `MAX_EOXACT_LIST` (relcache.c) — the fixed `eoxact_list[]` bound.
pub(crate) const MAX_EOXACT_LIST: usize = 32;

/// `INITRELCACHESIZE` (relcache.c) — initial `RelationIdCache` size. The owned
/// `id_cache` `HashMap` grows on demand, so this is documentary only.
#[allow(dead_code)]
pub(crate) const INITRELCACHESIZE: i64 = 400;

/// `REPLICA_IDENTITY_DEFAULT`/`_NOTHING`/`_INDEX`/`_FULL` (pg_class.h).
pub(crate) const REPLICA_IDENTITY_DEFAULT: i8 = b'd' as i8;
pub(crate) const REPLICA_IDENTITY_NOTHING: i8 = b'n' as i8;
pub(crate) const REPLICA_IDENTITY_INDEX: i8 = b'i' as i8;
pub(crate) const REPLICA_IDENTITY_FULL: i8 = b'f' as i8;

/// `RelationGetRelid(relation)` (utils/rel.h) on the owned entry.
#[inline]
#[allow(dead_code)]
pub(crate) fn relation_get_relid(rel: &RelationData) -> Oid {
    rel.rd_id
}

/// `elog(WARNING, msg)` — emit a non-fatal warning report and continue. The
/// build family's fetch routines warn (not error) on missing/null catalog rows
/// just as the C does; `ThrowErrorData` logs a WARNING and returns `Ok`.
pub(crate) fn elog_warning(message: String) -> backend_utils_error::PgResult<()> {
    backend_utils_error::elog(types_error::WARNING, message)
}
