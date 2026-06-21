//! core-entry-store family — the REAL relcache substrate.
//!
//! Owns the mutable [`entry::RelationData`] descriptor, the real
//! `RelationIdCache` store (the C `RelIdCacheEnt` table keyed by `Oid`,
//! idiomatically a `thread_local` `RefCell<HashMap<Oid, Box<RelationData>>>` —
//! the `id_cache`), the per-backend `eoxact_list`/`in_progress_list` state, and
//! the refcount lifecycle. Nothing here is a placeholder stub.
//!
//! The C `Relation` pointer becomes a copyable [`Oid`] handle ([`crate::Relation`]).
//! The store *owns* each descriptor in an `Rc<RefCell<RelationData>>` (the safe
//! C-shaped rendering of the `RelationData *`): the `Rc` gives a stable heap
//! allocation that survives `HashMap` rehash and the in-place
//! `RelationRebuildRelation` field swap (`*cell.borrow_mut() = rebuilt`), matching
//! the C pointer's stability invariant — and `Rc::strong_count` is the safe
//! analog of "an external holder pins the allocation". Callers reach a descriptor
//! through the scoped accessors [`with_rel`]/[`with_rel_mut`] (crate-internal)
//! and [`with_relation`]/[`with_relation_mut`]/[`try_with_relation`] (public),
//! or hold a pin across rebuilds via the [`RelationRef`] RAII guard. A holder
//! that wants C's live shared pointer takes a clone of the cell via
//! [`relation_id_get_relation_shared`].

/// The owned relcache entry-store type family.
///
/// F0' relocated these types into the standalone `types-relcache-entry` crate
/// (so the relcache seams crate can name `RelationData` in a cross-crate
/// `Rc<RefCell<RelationData>>` seam without a `types-rel` cycle). This module
/// re-exports the whole family at the historical `core_entry_store::entry::*`
/// path so every in-crate `use crate::core_entry_store::entry::X` keeps
/// resolving unchanged.
pub mod entry {
    pub use types_relcache_entry::*;
}

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use backend_utils_error::{ereport, emit_error_report_for, PgError, PgResult};
use types_core::primitive::{Oid, ProcNumber};
use types_core::xact::SubTransactionId;
use types_core::InvalidOid;
use types_error::{ERROR, WARNING};

use crate::MAX_EOXACT_LIST;
pub use entry::RelationData;

/* ==========================================================================
 * Owned per-entry partition-cache payloads (`rd_partkey` / `rd_partcheck`).
 *
 * In C these are node-vocabulary pointers (`PartitionKey rd_partkey`, `List
 * *rd_partcheck`) allocated in the entry's own `rd_partkeycxt`/`rd_partcheckcxt`
 * children of `CacheMemoryContext`, preserved across relcache rebuilds. The
 * owned model holds a lifetime-free deep copy keyed by relation OID (the cache
 * memory is process-lived; this map is the long-lived store). Reads re-project
 * a fresh copy into the caller's `mcx` (the partcache `copyObject` contract).
 * ======================================================================== */

/// Lifetime-free mirror of `PartitionKeyData` (the relcache-owned cache slot).
pub(crate) struct OwnedPartitionKey {
    pub(crate) strategy: types_partition::PartitionStrategy,
    pub(crate) partnatts: i16,
    pub(crate) partattrs: Vec<types_core::primitive::AttrNumber>,
    pub(crate) partexprs: Vec<types_nodes::primnodes::Expr>,
    pub(crate) partopfamily: Vec<Oid>,
    pub(crate) partopcintype: Vec<Oid>,
    pub(crate) partsupfunc: Vec<types_core::fmgr::FmgrInfo>,
    pub(crate) partcollation: Vec<Oid>,
    pub(crate) parttypid: Vec<Oid>,
    pub(crate) parttypmod: Vec<i32>,
    pub(crate) parttyplen: Vec<i16>,
    pub(crate) parttypbyval: Vec<bool>,
    pub(crate) parttypalign: Vec<i8>,
    pub(crate) parttypcoll: Vec<Oid>,
}

/* ==========================================================================
 * `in_progress_list` (relcache.c) — stack of ongoing `RelationBuildDesc`
 * calls, used by the invalidation-restart protocol.
 * ======================================================================== */

/// `InProgressEnt` (relcache.c).
pub(crate) struct InProgressEnt {
    pub(crate) reloid: Oid,
    pub(crate) invalidated: bool,
}

/* ==========================================================================
 * Per-backend relcache state.
 *
 * In C these are file-statics (`RelationIdCache`, `in_progress_list`,
 * `eoxact_list`, `relcacheInvalsReceived`, `criticalRelcachesBuilt`, ...). A
 * PostgreSQL backend is single-threaded, so they map to one `thread_local`
 * cell (matching `inval.c` and the other ported cache crates). The
 * `id_cache` HashMap *owns* every descriptor in an `Rc<RefCell<RelationData>>`.
 * ======================================================================== */

pub(crate) struct RelcacheState {
    /// `RelationIdCache` — the OID→reldesc store. Owns each `RelationData`
    /// in an `Rc<RefCell<..>>` (the stable allocation the C `Relation` pointer
    /// protects; a clone given out to a holder is C's live shared pointer, and
    /// `Rc::strong_count > 1` means an external holder pins it).
    pub(crate) id_cache: HashMap<Oid, Rc<RefCell<RelationData>>>,
    /// `in_progress_list` — stack of ongoing `RelationBuildDesc` calls.
    pub(crate) in_progress_list: Vec<InProgressEnt>,
    /// `eoxact_list[]` (fixed `MAX_EOXACT_LIST` bound, not a heap allocation).
    pub(crate) eoxact_list: Vec<Oid>,
    /// `eoxact_list_overflowed`.
    pub(crate) eoxact_list_overflowed: bool,
    /// `relcacheInvalsReceived`.
    pub(crate) relcache_invals_received: i64,
    /// `criticalRelcachesBuilt`.
    pub(crate) critical_relcaches_built: bool,
    /// `criticalSharedRelcachesBuilt`.
    pub(crate) critical_shared_relcaches_built: bool,

    /// `relation->rd_partkey` cache slots (the relcache-owned partition keys,
    /// keyed by relation OID; C: `rd_partkey` in `rd_partkeycxt`).
    pub(crate) partkey: HashMap<Oid, OwnedPartitionKey>,
    /// `relation->rd_partcheck` cache slots + the `rd_partcheckvalid` flag
    /// (keyed by relation OID; C: `rd_partcheck` in `rd_partcheckcxt`). An empty
    /// `Vec` with `valid = true` is the C NIL (no qual) case.
    pub(crate) partcheck: HashMap<Oid, (bool, Vec<types_nodes::primnodes::Expr>)>,
}

impl RelcacheState {
    fn new() -> Self {
        Self {
            id_cache: HashMap::new(),
            in_progress_list: Vec::new(),
            eoxact_list: Vec::new(),
            eoxact_list_overflowed: false,
            relcache_invals_received: 0,
            critical_relcaches_built: false,
            critical_shared_relcaches_built: false,
            partkey: HashMap::new(),
            partcheck: HashMap::new(),
        }
    }
}

thread_local! {
    pub(crate) static STATE: RefCell<RelcacheState> = RefCell::new(RelcacheState::new());
}

/// Run `f` with mutable access to the per-backend relcache state.
pub(crate) fn with_state<R>(f: impl FnOnce(&mut RelcacheState) -> R) -> R {
    STATE.with(|s| f(&mut s.borrow_mut()))
}

/* ==========================================================================
 * `RelationIdCache` creation (the dynahash half of `RelationCacheInitialize`).
 *
 * In the owned model the store is a `HashMap` that always exists (the C
 * file-static is zero-initialized); `RelationCacheInitialize` no longer has to
 * create it, so this is a documented no-op kept for the C call-site fidelity.
 * ======================================================================== */

/// `RelationCacheInitialize`'s `RelationIdCache = hash_create(...)` half: the
/// owned `HashMap` is constructed with the thread-local and always present, so
/// there is nothing to allocate here (the C dynahash create is subsumed by the
/// `HashMap` being eagerly live). Kept for the call-site in [`crate::initfile`].
pub(crate) fn create_id_cache() -> PgResult<()> {
    Ok(())
}

/* ==========================================================================
 * Small typed scoped accessors over the store + the owned `RelationData`.
 *
 * A live [`Oid`] handle names a descriptor; the store owns it. These helpers
 * borrow it from the store for the duration of `f` (no aliasing: the backend is
 * single-threaded and the cache mutators never re-enter the store while a borrow
 * is live — exactly as the C never frees a relcache entry out from under an
 * active accessor). The re-entrancy contract is caller-enforced: `f` must not
 * re-enter the relcache while the borrow is live.
 * ======================================================================== */

/// Clone the cell named by `rel` out of the store (so the store borrow is
/// released before the cell is borrowed — avoids a nested-borrow hazard when `f`
/// re-enters the store). `None` is the C NULL.
pub(crate) fn cell_of(rel: Oid) -> Option<Rc<RefCell<RelationData>>> {
    with_state(|st| st.id_cache.get(&rel).map(Rc::clone))
}

/// Borrow the descriptor named by `rel` immutably for the duration of `f`
/// (replaces the prior `&*ptr` reads). Panics if the handle is stale, matching a
/// C NULL-deref bug (the cache invariant is a live handle names a present desc).
pub(crate) fn with_rel<R>(rel: Oid, f: impl FnOnce(&RelationData) -> R) -> R {
    let cell = cell_of(rel).expect("relcache: handle names no descriptor");
    let r = cell.borrow();
    f(&r)
}

/// Borrow the descriptor named by `rel` mutably for the duration of `f`
/// (replaces the prior `&mut *ptr` writes). The in-place `borrow_mut` is exactly
/// C's "mutate the fields behind the live `Relation` pointer".
pub(crate) fn with_rel_mut<R>(rel: Oid, f: impl FnOnce(&mut RelationData) -> R) -> R {
    let cell = cell_of(rel).expect("relcache: handle names no descriptor");
    let mut r = cell.borrow_mut();
    f(&mut r)
}

/* ==========================================================================
 * Public Oid-keyed scoped accessors (for the pub entry points).
 * ======================================================================== */

/// Run `f` with the descriptor identified by `oid` borrowed immutably. Errors
/// (loud) if `oid` names no live relcache entry — a caller-contract violation
/// (the relation must already be open/pinned).
pub fn with_relation<R>(oid: Oid, f: impl FnOnce(&RelationData) -> R) -> PgResult<R> {
    match cell_of(oid) {
        Some(cell) => Ok(f(&cell.borrow())),
        None => Err(relcache_missing(oid)),
    }
}

/// Run `f` with the descriptor identified by `oid` borrowed mutably (the
/// in-place field-mutation arm).
pub fn with_relation_mut<R>(oid: Oid, f: impl FnOnce(&mut RelationData) -> R) -> PgResult<R> {
    match cell_of(oid) {
        Some(cell) => Ok(f(&mut cell.borrow_mut())),
        None => Err(relcache_missing(oid)),
    }
}

/// Like [`with_relation`] but yields the C-NULL semantics for a dropped/absent
/// entry (`None`) instead of erroring — for the fetch sites whose C returns NULL
/// when the relation is gone.
pub fn try_with_relation<R>(oid: Oid, f: impl FnOnce(&RelationData) -> R) -> Option<R> {
    cell_of(oid).map(|cell| f(&cell.borrow()))
}

/// Loud error for an Oid that names no present descriptor.
fn relcache_missing(oid: Oid) -> PgError {
    ereport(ERROR)
        .errmsg_internal(format!("relcache: no open relation for oid {oid}"))
        .into_error()
}

/// `relation_open` failing to build/find a relation: the C
/// `relation_open(relid, NoLock)` path elogs `ERROR "could not open relation
/// with OID %u"` when `RelationIdGetRelation` returns NULL.
pub(crate) fn relcache_open_failed(oid: Oid) -> PgError {
    ereport(ERROR)
        .errmsg_internal(format!("could not open relation with OID {oid}"))
        .into_error()
}

/* ==========================================================================
 * RelationRef — the RAII pin guard (the held-pointer analog).
 *
 * The C `Relation` is a `RelationData *` a caller pins (`rd_refcnt++`), holds
 * across re-dereferences, and unpins on close. The pointer stays valid across an
 * in-place `RelationRebuildRelation` (the fields are swapped behind the pointer;
 * the struct's address never moves) because the rebuild never frees/moves a
 * pinned (`rd_refcnt > 0`) entry.
 *
 * `RelationRef` is the safe analog: it owns a +1 on `rd_refcnt` AND a clone of
 * the `id_cache`'s `Rc<RefCell<RelationData>>`. The `Rc` keeps the allocation
 * stable across HashMap rehash and the in-place `*cell.borrow_mut() = rebuilt`
 * swap, exactly as the C pointer's `rd_refcnt > 0` pin does — and there is no
 * `unsafe`: every access goes through `RefCell::borrow`/`borrow_mut`. Construct
 * it with [`RelationRef::open`]; drop it (or let it fall out of scope, including
 * on a `?`/panic unwind) to unpin.
 * ======================================================================== */

/// A RAII pin on an open relation: the [`crate::Relation`] (`RelationData *`)
/// analog for callers that hold a relation across rebuilds. Holds a +1 on the
/// descriptor's `rd_refcnt` and a clone of the cache cell (C's live shared
/// pointer).
pub struct RelationRef {
    /// The relation OID — the cache key and what `Drop` unpins.
    oid: Oid,
    /// A clone of the `id_cache` cell (C's `RelationData *`). Keeps the
    /// allocation live; accesses borrow it through `RefCell`. This clone makes
    /// `Rc::strong_count > 1` while the guard is held — the safe analog of the
    /// pin keeping the allocation alive.
    cell: Rc<RefCell<RelationData>>,
}

impl RelationRef {
    /// `RelationIdGetRelation(oid)` + pin: get-or-build the relcache entry, pin
    /// it (`rd_refcnt += 1` via `RelationIncrementReferenceCount`, done by
    /// `RelationIdGetRelation`), and return the guard. Errors when the relation
    /// has no `pg_class` row (the C `relation_open` "could not open" error).
    pub fn open(oid: Oid) -> PgResult<RelationRef> {
        let handle = RelationIdGetRelation(oid)?;
        if handle == InvalidOid {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("could not open relation with OID {oid}"))
                .into_error());
        }
        debug_assert_eq!(handle, oid);
        let cell = cell_of(oid).expect("relcache: RelationRef pins an absent descriptor");
        Ok(RelationRef { oid, cell })
    }

    /// `RelationIdGetRelation(oid)` + pin, returning BOTH the RAII pin guard and
    /// a clone of C's live shared pointer (the cache cell). The additive
    /// shared-ref open: the returned `Rc` sees in-place rebuilds and keeps
    /// `Rc::strong_count > 1`; the guard tracks the `rd_refcnt` pin and unpins on
    /// drop. Use this when a caller wants to hold the shared cell across calls.
    pub fn open_shared(oid: Oid) -> PgResult<(RelationRef, Rc<RefCell<RelationData>>)> {
        let guard = RelationRef::open(oid)?;
        let cell = Rc::clone(&guard.cell);
        Ok((guard, cell))
    }

    /// A clone of the pinned descriptor's cell (C's live shared `RelationData *`).
    #[inline]
    pub fn cell(&self) -> Rc<RefCell<RelationData>> {
        Rc::clone(&self.cell)
    }

    /// The pinned relation's OID (`RelationGetRelid`).
    #[inline]
    pub fn oid(&self) -> Oid {
        self.oid
    }

    /// Run `f` with the descriptor borrowed immutably (the PREFERRED, momentary
    /// access form: the borrow cannot escape `f`).
    #[inline]
    pub fn with<R>(&self, f: impl FnOnce(&RelationData) -> R) -> R {
        f(&self.cell.borrow())
    }

    /// Run `f` with the descriptor borrowed mutably (the in-place field-mutation
    /// arm). Like [`with`](Self::with), the borrow is scoped to `f`.
    #[inline]
    pub fn with_mut<R>(&mut self, f: impl FnOnce(&mut RelationData) -> R) -> R {
        f(&mut self.cell.borrow_mut())
    }
}

impl Drop for RelationRef {
    /// `RelationClose`-style unpin: `RelationDecrementReferenceCount` only. A
    /// failing decrement cannot be propagated from `Drop`; it is reported.
    fn drop(&mut self) {
        if let Err(e) = RelationDecrementReferenceCount(self.oid) {
            emit_error_report_for(&e);
        }
    }
}

/* ==========================================================================
 * `RelationCacheInsert` / `RelationIdCacheLookup` / `RelationCacheDelete`
 * (relcache.c macros) — the store element operations over the owned `Box`es.
 * ======================================================================== */

/// `RelationCacheInsert(RELATION, replace_allowed)` (relcache.c macro): enter
/// `reldesc` into the store under its `rd_id`. The C asserts `replace_allowed ||
/// !found`; if a collision finds a still-referenced entry it warns (the leak
/// path). Takes ownership of the `Box`.
pub(crate) fn cache_insert(reldesc: Box<RelationData>, replace_allowed: bool) -> PgResult<()> {
    let id = reldesc.rd_id;
    with_state(|st| {
        if let Some(old) = st.id_cache.get(&id) {
            // C: `Assert(replace_allowed)`.
            debug_assert!(replace_allowed);
            let old = old.borrow();
            if old.rd_refcnt != 0 {
                // Still-referenced: C ereport(WARNING) about a leak (the displaced
                // pointer is simply overwritten in C; here the old cell is dropped
                // from the table when we `insert` the replacement — any external
                // holder that still has a clone keeps its allocation alive).
                let name = old.rd_rel.relname.clone();
                emit_error_report_for(
                    &ereport(WARNING)
                        .errmsg_internal(format!(
                            "leaking still-referenced relcache entry for \"{name}\""
                        ))
                        .into_error(),
                );
            }
        }
        // The previous cell (if any) is dropped from the table here — when no
        // external holder retains a clone, this frees the whole owned subsidiary
        // tree (the C `RelationDestroyRelation`/`pfree` cascade). `reldesc` is the
        // fresh build; move it into a new cell.
        st.id_cache.insert(id, Rc::new(RefCell::new(*reldesc)));
    });
    Ok(())
}

/// `RelationIdCacheLookup(ID, RELATION)` (relcache.c macro): is the OID present?
/// Returns the [`Oid`] handle (`None` == the C `NULL`).
pub(crate) fn cache_lookup(id: Oid) -> Option<Oid> {
    with_state(|st| {
        if st.id_cache.contains_key(&id) {
            Some(id)
        } else {
            None
        }
    })
}

/// `RelationCacheDelete(RELATION)` (relcache.c macro): remove the entry for
/// `rd_id` and reclaim the owned descriptor (the C `RelationDestroyRelation`
/// `pfree` tree). Removing the cell from the table drops the cache's `Rc`; when
/// it was the only holder (`strong_count == 1`, the safe analog of "no external
/// reference pins it") the allocation is freed here. If an external holder still
/// retains a clone the allocation survives until that holder drops it — exactly
/// C's "a still-pinned `RelationData *` stays valid after the cache forgets it".
/// The C `elog(ERROR)` if the entry is missing.
pub(crate) fn cache_delete(id: Oid) -> PgResult<()> {
    let removed = with_state(|st| st.id_cache.remove(&id));
    match removed {
        Some(cell) => {
            // Dropping the cache's `Rc` here frees the descriptor iff it was the
            // sole holder (cache-only eviction). `Rc::strong_count(&cell) == 1`
            // at this point means cache-only; > 1 means an external holder pins
            // it (the live-shared-pointer case).
            drop(cell);
            Ok(())
        }
        None => Err(ereport(ERROR)
            .errmsg_internal("trying to delete a reldesc that does not exist")
            .into_error()),
    }
}

/// Collect the [`Oid`] of every live entry in the store (relcache.c's
/// `HASH_SEQ_STATUS` walk). Returned as an owned snapshot so callers that need
/// to delete/rebuild entries (the `RelationCacheInvalidate` / `AtEOXact`
/// whole-cache passes) don't mutate the table while iterating.
pub(crate) fn cache_seq_reldescs() -> Vec<Oid> {
    with_state(|st| st.id_cache.keys().copied().collect())
}

/// `hash_search(RelationIdCache, &relid, HASH_FIND)` returning the entry's
/// handle (the `AtEOXact`/`AtEOSubXact` non-overflow path). `None` is the C NULL.
pub(crate) fn cache_find_reldesc(relid: Oid) -> Option<Oid> {
    cache_lookup(relid)
}

/// `eoxact_list` reset half of `AtEOXact_RelationCache` tail.
pub(crate) fn eoxact_list_reset(st: &mut RelcacheState) {
    st.eoxact_list.clear();
    st.eoxact_list_overflowed = false;
}

/// Collect every `Oid` currently keyed in the store (the C `hash_seq` walk over
/// `RelIdCacheEnt`). Used by the init-file write and the Phase3 finish loop.
pub(crate) fn id_cache_oids(st: &mut RelcacheState) -> Vec<Oid> {
    st.id_cache.keys().copied().collect()
}

/* ==========================================================================
 * `eoxact_list` bookkeeping (relcache.c `EOXactListAdd` macro).
 * ======================================================================== */

/// `EOXactListAdd(rel)` (relcache.c macro): remember `rel->rd_id` for
/// `AtEOXact` cleanup, or set the overflow flag once the fixed list fills.
pub(crate) fn eoxact_list_add(st: &mut RelcacheState, relid: Oid) {
    if st.eoxact_list.len() < MAX_EOXACT_LIST {
        st.eoxact_list.push(relid);
    } else {
        st.eoxact_list_overflowed = true;
    }
}

/* ==========================================================================
 * Reference-count lifecycle (RelationIncrement/DecrementReferenceCount,
 * RelationClose, RelationIdGetRelation). These operate on the `Oid` handle and
 * mutate the owned descriptor in place — REAL logic.
 * ======================================================================== */

/// `RelationIncrementReferenceCount(rel)` (relcache.c): pin the entry
/// (`rd_refcnt += 1`) and register the relation ref with the current resource
/// owner so a transaction/portal abort can release a leaked pin. Mirrors C:
///
/// ```c
/// ResourceOwnerEnlarge(CurrentResourceOwner);
/// relation->rd_refcnt += 1;
/// if (!IsBootstrapProcessingMode())
///     ResourceOwnerRememberRelationRef(CurrentResourceOwner, relation);
/// ```
///
/// The enlarge (which may `ereport` on OOM) runs BEFORE the bump so the
/// subsequent infallible remember cannot fail. The remember is skipped in
/// bootstrap processing mode (no resource owner exists yet).
pub fn RelationIncrementReferenceCount(rel: Oid) -> PgResult<()> {
    backend_utils_cache_relcache_seams::resource_owner_enlarge_relation::call()?;
    with_relation_mut(rel, |rd| rd.rd_refcnt += 1)?;
    if !backend_utils_init_miscinit_seams::is_bootstrap_processing_mode::call() {
        backend_utils_cache_relcache_seams::resource_owner_remember_relation::call(rel);
    }
    Ok(())
}

/// `RelationDecrementReferenceCount(rel)` (relcache.c): drop the pin
/// (`rd_refcnt -= 1`), asserting it was positive, and forget the relation ref
/// with the resource owner. Mirrors C:
///
/// ```c
/// Assert(relation->rd_refcnt > 0);
/// relation->rd_refcnt -= 1;
/// if (!IsBootstrapProcessingMode())
///     ResourceOwnerForgetRelationRef(CurrentResourceOwner, relation);
/// ```
pub fn RelationDecrementReferenceCount(rel: Oid) -> PgResult<()> {
    with_relation_mut(rel, |rd| {
        debug_assert!(rd.rd_refcnt > 0);
        rd.rd_refcnt -= 1;
    })?;
    if !backend_utils_init_miscinit_seams::is_bootstrap_processing_mode::call() {
        backend_utils_cache_relcache_seams::resource_owner_forget_relation::call(rel);
    }
    Ok(())
}

/// `RelationClose(relation)` (relcache.c): drop the relcache reference, then run
/// [`RelationCloseCleanup`] (the immediate-flush-of-dropped-or-invalidated path).
pub fn RelationClose(relation: Oid) -> PgResult<()> {
    RelationDecrementReferenceCount(relation)?;
    RelationCloseCleanup(relation)
}

/// `ResOwnerReleaseRelation(Datum res)` (relcache.c) — the `ReleaseResource`
/// callback of `relref_resowner_desc`, invoked by `ResourceOwnerReleaseAll` for
/// a relcache pin that was still held when the resource owner was released
/// (i.e. a leak on abort, or normal portal/transaction teardown). Mirrors C:
///
/// ```c
/// Relation rel = (Relation) DatumGetPointer(res);
/// rel->rd_refcnt -= 1;
/// RelationCloseCleanup(rel);
/// ```
///
/// Unlike [`RelationDecrementReferenceCount`] this does NOT call
/// `ResourceOwnerForgetRelationRef`: the ref is already being removed from the
/// (currently-releasing) owner's array by `ResourceOwnerReleaseAll`, and the
/// owner forbids Forget once release has started.
pub fn ResOwnerReleaseRelation(relation: Oid) -> PgResult<()> {
    with_relation_mut(relation, |rd| {
        debug_assert!(rd.rd_refcnt > 0);
        rd.rd_refcnt -= 1;
    })?;
    RelationCloseCleanup(relation)
}

/// `RelationCloseCleanup(relation)` (relcache.c). When the relation is no longer
/// open in this session (`RelationHasReferenceCountZero`), the C cleans up stale
/// partition descriptors by deleting the child contexts of `rd_pdcxt`/`rd_pddcxt`.
/// Those partition-descriptor MemoryContexts are not represented on this entry,
/// so the cleanup is a no-op over the fields this family owns.
///
/// The further `RelationClearRelation` call the C makes is guarded by
/// `#ifdef RELCACHE_FORCE_RELEASE`, a debug-only define compiled out of normal
/// builds, so it is intentionally absent here.
///
/// An earlier port reintroduced that clear under a hand-written
/// `rd_refcnt == 0 && (!rd_isvalid || rd_droppedSubid != 0)` condition. That is
/// NOT what the normal C build does: even the `#ifdef RELCACHE_FORCE_RELEASE`
/// path additionally requires `rd_createSubid == InvalidSubTransactionId &&
/// rd_firstRelfilelocatorSubid == InvalidSubTransactionId` (relcache.c:2248). So
/// releasing the last ref on a relation created in (and invalidated by) the
/// *current, aborting* transaction reached `RelationClearRelation`, whose
/// `Assert(rd_createSubid == InvalidSubTransactionId)` then fired *inside*
/// `AbortTransaction` — an abort-in-abort escalation the error-recovery loop
/// could never settle, killing the backend (generated_virtual). Stale/dropped
/// entries are reclaimed by `RelationFlushRelation` / `AtEOXact_RelationCache`,
/// not on the resowner close path.
pub(crate) fn RelationCloseCleanup(_relation: Oid) -> PgResult<()> {
    // Normal build: only the partition-descriptor child-context cleanup, which
    // is a no-op over the fields this family owns (rd_pdcxt/rd_pddcxt are not
    // modeled). The RELCACHE_FORCE_RELEASE clear stays compiled out.
    Ok(())
}

/// `RelationIdGetRelation(relationId)` (relcache.c): the cache lookup + lazy
/// build entry point. Looks up the entry; if valid (and not dropped) pins and
/// revalidates it, else builds a fresh descriptor via
/// [`crate::build::RelationBuildDesc`] and pins it. Returns the [`Oid`] handle,
/// or [`InvalidOid`] (the C `NULL` — no `pg_class` row).
pub fn RelationIdGetRelation(relationId: Oid) -> PgResult<Oid> {
    if let Some(rd) = cache_lookup(relationId) {
        // Return NULL for dropped relations.
        let (dropped, valid) = with_rel(rd, |r| (r.rd_droppedSubid != 0, r.rd_isvalid));
        if dropped {
            debug_assert!(!valid);
            return Ok(InvalidOid);
        }
        RelationIncrementReferenceCount(rd)?;
        if !valid {
            crate::invalidate::RelationRebuildRelation(rd)?;
        }
        return Ok(rd);
    }

    // Not cached: build one and add it.
    let rd = crate::build::RelationBuildDesc(relationId, true)?;
    if rd != InvalidOid {
        RelationIncrementReferenceCount(rd)?;
    }
    Ok(rd)
}

/// `RelationIdGetRelation(relationId)` + hand back C's live shared pointer: the
/// ADDITIVE shared-ref entry point. Identical lookup/build/pin logic as
/// [`RelationIdGetRelation`], but instead of projecting a *copy* of the entry it
/// returns a CLONE of the cache's `Rc<RefCell<RelationData>>` (C's
/// `RelationData *` into the cache). A holder of this clone sees the in-place
/// `*cell.borrow_mut() = rebuilt` rebuild (true C semantics) and makes
/// `Rc::strong_count > 1` (the safe analog of `rd_refcnt > 0` pinning the
/// allocation). The pin is still tracked on `rd_refcnt` so the existing eviction
/// protocol is unchanged; the holder must `RelationClose`/drop a paired
/// `RelationRef` to release it. `Ok(None)` is the C NULL (no `pg_class` row).
///
/// This coexists with the copy-projecting [`RelationIdGetRelation`] +
/// [`crate::build::project_relation_data`] path (still alive for the consumers
/// that have not migrated yet) — both representations are produced from the same
/// cell.
pub fn relation_id_get_relation_shared(
    relation_id: Oid,
) -> PgResult<Option<Rc<RefCell<RelationData>>>> {
    let handle = RelationIdGetRelation(relation_id)?;
    if handle == InvalidOid {
        return Ok(None);
    }
    Ok(cell_of(handle))
}

/// Clone the shared cell for an ALREADY-PINNED entry WITHOUT taking another
/// `rd_refcnt` pin (the dual-carry companion fetch — see the
/// `relation_id_get_relation_cell` seam). Unlike
/// [`relation_id_get_relation_shared`], this does NOT route through
/// [`RelationIdGetRelation`], so it never increments the reference count: the
/// copy path of the same `relation_open` already took the single pin this
/// open's close releases. A miss returns `None` (the entry must already be in
/// the cache, having been built/pinned by the copy fetch moments earlier).
pub fn relation_id_get_relation_cell(
    relation_id: Oid,
) -> PgResult<Option<Rc<RefCell<RelationData>>>> {
    Ok(cell_of(relation_id))
}

/* ==========================================================================
 * Seam-facing scalar reads off the owned entry.
 * ======================================================================== */

/// Owning-backend proc number for a temp relation (`rd_backend`).
pub(crate) fn rd_backend_of(rel: Oid) -> ProcNumber {
    with_rel(rel, |rd| rd.rd_backend)
}

/// `rd_createSubid` read (used by `RelationNeedsWAL`/`RELATION_IS_LOCAL`).
pub(crate) fn rd_create_subid_of(rel: Oid) -> SubTransactionId {
    with_rel(rel, |rd| rd.rd_createSubid)
}

/* ==========================================================================
 * Partition-cache slot read/write (`rd_partkey` / `rd_partcheck`).
 *
 * The partcache owner builds the key/qual in the caller's `mcx`; the relcache
 * owner copies it into the long-lived store (C: `rd_partkeycxt` /
 * `rd_partcheckcxt`) and re-projects a fresh copy on each read (the partcache
 * `copyObject` contract). `Expr`/`PartitionKeyData` sub-arrays are lifetime-free
 * so the deep copy is a by-value field clone.
 * ======================================================================== */

/// Helper: clone a `PgVec<T: Clone>` into an owned `Vec<T>`. (`Clone`, not
/// `Copy`: `partsupfunc` is `Vec<FmgrInfo>`, and `FmgrInfo` carries the erased
/// `fn_expr` so it is no longer `Copy`. C `memcpy`s the `FmgrInfo` array; the
/// owned model clones it.)
fn copy_vec<T: Clone>(v: &mcx::PgVec<'_, T>) -> Vec<T> {
    v.iter().cloned().collect()
}

/// Helper: re-project an owned `&[T: Clone]` into a fresh `PgVec` in `mcx`.
fn project_vec<'mcx, T: Clone>(mcx: mcx::Mcx<'mcx>, src: &[T]) -> mcx::PgVec<'mcx, T> {
    let mut out = mcx::PgVec::new_in(mcx);
    for item in src {
        out.push(item.clone());
    }
    out
}

/// `relation->rd_partkey = key` (the relcache copy into `rd_partkeycxt`). Stores
/// a lifetime-free deep copy keyed by `relid` and sets the entry's presence flag.
pub(crate) fn set_partkey(
    relid: Oid,
    key: &types_partition::PartitionKeyData<'_>,
) -> PgResult<()> {
    let owned = OwnedPartitionKey {
        strategy: key.strategy,
        partnatts: key.partnatts,
        partattrs: copy_vec(&key.partattrs),
        partexprs: key.partexprs.iter().cloned().collect(),
        partopfamily: copy_vec(&key.partopfamily),
        partopcintype: copy_vec(&key.partopcintype),
        partsupfunc: copy_vec(&key.partsupfunc),
        partcollation: copy_vec(&key.partcollation),
        parttypid: copy_vec(&key.parttypid),
        parttypmod: copy_vec(&key.parttypmod),
        parttyplen: copy_vec(&key.parttyplen),
        parttypbyval: copy_vec(&key.parttypbyval),
        parttypalign: copy_vec(&key.parttypalign),
        parttypcoll: copy_vec(&key.parttypcoll),
    };
    with_state(|st| {
        st.partkey.insert(relid, owned);
    });
    // `relation->rd_partkeyvalid`/presence flag on the entry.
    let _ = with_relation_mut(relid, |rd| rd.rd_has_partkey = true);
    Ok(())
}

/// `relation->rd_partkey` read — re-project a fresh copy into `mcx`, or `None`
/// when the slot has not been built (C NULL).
pub(crate) fn get_partkey<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    relid: Oid,
) -> PgResult<Option<types_partition::PartitionKeyData<'mcx>>> {
    with_state(|st| {
        let Some(owned) = st.partkey.get(&relid) else {
            return Ok(None);
        };
        let mut partexprs = mcx::PgVec::new_in(mcx);
        for e in &owned.partexprs {
            partexprs.push(e.clone());
        }
        Ok(Some(types_partition::PartitionKeyData {
            strategy: owned.strategy,
            partnatts: owned.partnatts,
            partattrs: project_vec(mcx, &owned.partattrs),
            partexprs,
            partopfamily: project_vec(mcx, &owned.partopfamily),
            partopcintype: project_vec(mcx, &owned.partopcintype),
            partsupfunc: project_vec(mcx, &owned.partsupfunc),
            partcollation: project_vec(mcx, &owned.partcollation),
            parttypid: project_vec(mcx, &owned.parttypid),
            parttypmod: project_vec(mcx, &owned.parttypmod),
            parttyplen: project_vec(mcx, &owned.parttyplen),
            parttypbyval: project_vec(mcx, &owned.parttypbyval),
            parttypalign: project_vec(mcx, &owned.parttypalign),
            parttypcoll: project_vec(mcx, &owned.parttypcoll),
        }))
    })
}

/// `relation->rd_partcheck = copyObject(result); rd_partcheckvalid = true`
/// (the relcache copy into `rd_partcheckcxt`). `partcheck` is a list of
/// `Expr`-derived CHECK quals (the `Node::Expr` cast); a non-`Expr` `Node` is a
/// contract violation.
pub(crate) fn set_partcheck(
    relid: Oid,
    partcheck: &mcx::PgVec<'_, types_nodes::nodes::Node<'_>>,
) -> PgResult<()> {
    let mut owned: Vec<types_nodes::primnodes::Expr> = Vec::with_capacity(partcheck.len());
    for node in partcheck.iter() {
        match node.as_expr() {
            Some(e) => owned.push(e.clone()),
            None => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!(
                        "rd_partcheck entry is not an expression node (tag {:?})",
                        node.tag()
                    ))
                    .into_error());
            }
        }
    }
    with_state(|st| {
        st.partcheck.insert(relid, (true, owned));
    });
    let _ = with_relation_mut(relid, |rd| rd.rd_partcheckvalid = true);
    Ok(())
}

/// Discard the cached `rd_partcheck` for `relid` (C: the `rd_partcheckcxt`
/// teardown in `RelationDestroyRelation` plus the rebuild dropping the stale
/// partition-constraint tree, which both leave the rebuilt/cleared entry with
/// `rd_partcheckvalid = false`). The partition constraint of a (default)
/// partition depends on its siblings' bounds, so it MUST be recomputed after a
/// relcache invalidation — the side-table is OID-keyed and would otherwise
/// outlive the entry rebuild. Idempotent: a missing slot is a no-op.
pub(crate) fn clear_partcheck(relid: Oid) {
    with_state(|st| {
        st.partcheck.remove(&relid);
    });
    let _ = with_relation_mut(relid, |rd| rd.rd_partcheckvalid = false);
}

/// `relation->rd_partcheck` read + `rd_partcheckvalid` — returns `(valid,
/// copyObject(rd_partcheck))`. When the slot is absent the cache is stale
/// (`valid = false`, empty list) and the caller rebuilds.
pub(crate) fn get_partcheck<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    relid: Oid,
) -> PgResult<(bool, mcx::PgVec<'mcx, types_nodes::nodes::Node<'mcx>>)> {
    with_state(|st| {
        let mut out = mcx::PgVec::new_in(mcx);
        match st.partcheck.get(&relid) {
            Some((valid, exprs)) => {
                for e in exprs {
                    out.push(types_nodes::nodes::Node::mk_expr(mcx, e.clone())?);
                }
                Ok((*valid, out))
            }
            None => Ok((false, out)),
        }
    })
}

/* ==========================================================================
 * rd_amcache — the per-relation access-method-private cache slot.
 *
 * Mirrors C `void *rd_amcache` (utils/rel.h), allocated by each index/table AM
 * in `rd_indexcxt` (a CacheMemoryContext child) and cast back to the AM's own
 * struct on every call. The owned slot lives directly on the relcache entry's
 * `rd_amcache` field as `Option<Box<dyn AmOpaque<'static>>>` (the erased,
 * tag-checked AM-private payload). It is `'static`: the cache survives across
 * queries, so the payload borrows nothing from a per-query `'mcx` arena —
 * exactly the C `rd_indexcxt` lifetime. The AM bodies that *fill* this (SP-GiST
 * `initSpGistState`, hash `_hash_getcachedmetap`, GIN `ginGetCache`, GiST) are
 * not ported here; this is just the slot + accessors they use.
 * ======================================================================== */

/// `rel->rd_amcache = payload` (with the C `pfree(rel->rd_amcache)` of any
/// stale payload subsumed by the `Box` drop). Stores the AM-private cache on
/// the live entry. Errors only if `relid` names no open entry (a contract
/// violation; the relation must be open).
pub fn set_rd_amcache(
    relid: Oid,
    payload: Box<dyn types_tableam::amopaque::AmOpaque<'static> + 'static>,
) -> PgResult<()> {
    with_relation_mut(relid, |rd| rd.rd_amcache = Some(payload))
}

/// `pfree(rel->rd_amcache); rel->rd_amcache = NULL` — clear the AM-private
/// cache (the relcache-invalidation / rebuild lifecycle step). No-op when the
/// entry is absent (`None`), matching the C path that only touches a live
/// descriptor.
pub fn clear_rd_amcache(relid: Oid) {
    let _ = with_relation_mut(relid, |rd| rd.rd_amcache = None);
}

/// Read `rel->rd_amcache` and tag-checked-downcast it to `&T`, running `f` over
/// it; returns `Ok(None)` for the C `rd_amcache == NULL` (slot empty) and a
/// loud error if the downcast tag mismatches (a different AM's payload is
/// cached — the C never does this, so it is a contract violation). The closure
/// form keeps the entry borrow scoped (the payload is owned by the cache cell).
pub fn with_rd_amcache<T, R>(
    relid: Oid,
    f: impl FnOnce(&T) -> R,
) -> PgResult<Option<R>>
where
    T: types_tableam::amopaque::AmOpaqueType<'static>,
{
    with_relation(relid, |rd| match rd.rd_amcache.as_ref() {
        None => Ok(None),
        Some(boxed) => match (boxed.as_ref()).downcast_ref::<T>() {
            Some(t) => Ok(Some(f(t))),
            None => Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "rd_amcache for relation {relid} holds a different AM payload \
                     than the one requested ({})",
                    boxed.am_opaque_type_name()
                ))
                .into_error()),
        },
    })?
}
