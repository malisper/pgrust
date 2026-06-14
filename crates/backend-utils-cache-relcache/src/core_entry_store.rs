//! core-entry-store family — the REAL relcache substrate.
//!
//! Owns the mutable [`entry::RelationData`] descriptor, the real
//! `RelationIdCache` store (the C `RelIdCacheEnt` table keyed by `Oid`,
//! idiomatically a `thread_local` `RefCell<HashMap<Oid, Box<RelationData>>>` —
//! the `id_cache`), the per-backend `eoxact_list`/`in_progress_list` state, and
//! the refcount lifecycle. Nothing here is `todo!()`.
//!
//! The C `Relation` pointer becomes a copyable [`Oid`] handle ([`crate::Relation`]).
//! The store *owns* each descriptor in a `Box<RelationData>`: the `Box` gives a
//! stable heap address that survives `HashMap` rehash and the in-place
//! `RelationRebuildRelation` field swap, matching the C pointer's stability
//! invariant (`rd_refcnt > 0` pins the allocation). Callers reach a descriptor
//! through the scoped accessors [`with_rel`]/[`with_rel_mut`] (crate-internal)
//! and [`with_relation`]/[`with_relation_mut`]/[`try_with_relation`] (public),
//! or hold a pin across rebuilds via the [`RelationRef`] RAII guard.

pub mod entry;

use std::cell::RefCell;
use std::collections::HashMap;

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
 * `id_cache` HashMap *owns* every descriptor in a `Box<RelationData>`.
 * ======================================================================== */

pub(crate) struct RelcacheState {
    /// `RelationIdCache` — the OID→reldesc store. Owns each `RelationData`
    /// in a `Box` (the stable heap address the C `Relation` pointer protects).
    pub(crate) id_cache: HashMap<Oid, Box<RelationData>>,
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

/// Borrow the descriptor named by `rel` immutably for the duration of `f`
/// (replaces the prior `&*ptr` reads). Panics if the handle is stale, matching a
/// C NULL-deref bug (the cache invariant is a live handle names a present desc).
pub(crate) fn with_rel<R>(rel: Oid, f: impl FnOnce(&RelationData) -> R) -> R {
    with_state(|st| {
        let d = st
            .id_cache
            .get(&rel)
            .expect("relcache: handle names no descriptor");
        f(d)
    })
}

/// Borrow the descriptor named by `rel` mutably for the duration of `f`
/// (replaces the prior `&mut *ptr` writes).
pub(crate) fn with_rel_mut<R>(rel: Oid, f: impl FnOnce(&mut RelationData) -> R) -> R {
    with_state(|st| {
        let d = st
            .id_cache
            .get_mut(&rel)
            .expect("relcache: handle names no descriptor");
        f(d)
    })
}

/* ==========================================================================
 * Public Oid-keyed scoped accessors (for the pub entry points).
 * ======================================================================== */

/// Run `f` with the descriptor identified by `oid` borrowed immutably. Errors
/// (loud) if `oid` names no live relcache entry — a caller-contract violation
/// (the relation must already be open/pinned).
pub fn with_relation<R>(oid: Oid, f: impl FnOnce(&RelationData) -> R) -> PgResult<R> {
    with_state(|st| match st.id_cache.get(&oid) {
        Some(d) => Ok(f(d)),
        None => Err(relcache_missing(oid)),
    })
}

/// Run `f` with the descriptor identified by `oid` borrowed mutably (the
/// in-place field-mutation arm).
pub fn with_relation_mut<R>(oid: Oid, f: impl FnOnce(&mut RelationData) -> R) -> PgResult<R> {
    with_state(|st| match st.id_cache.get_mut(&oid) {
        Some(d) => Ok(f(d)),
        None => Err(relcache_missing(oid)),
    })
}

/// Like [`with_relation`] but yields the C-NULL semantics for a dropped/absent
/// entry (`None`) instead of erroring — for the fetch sites whose C returns NULL
/// when the relation is gone.
pub fn try_with_relation<R>(oid: Oid, f: impl FnOnce(&RelationData) -> R) -> Option<R> {
    with_state(|st| st.id_cache.get(&oid).map(|d| f(d)))
}

/// Loud error for an Oid that names no present descriptor.
fn relcache_missing(oid: Oid) -> PgError {
    ereport(ERROR)
        .errmsg_internal(format!("relcache: no open relation for oid {oid}"))
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
 * `RelationRef` is the analog: it owns a +1 on `rd_refcnt` and a raw
 * `NonNull<RelationData>` into the `id_cache` `Box`. The `Box` gives a stable
 * heap address: HashMap rehash moves only the table slot, and the in-place
 * rebuild (`std::mem::swap` behind the `Box`'s `&mut RelationData`) leaves the
 * allocation in place — so the `NonNull` stays valid exactly as the C pointer
 * does (`rd_refcnt > 0` pins the allocation). The three `NonNull::as_ref`/
 * `as_mut` sites below are the crate's ONLY sanctioned `unsafe`.
 * ======================================================================== */

/// Capture the stable [`NonNull<RelationData>`](core::ptr::NonNull) for the
/// descriptor named by `oid`, which MUST be present in the `id_cache`. Creating
/// the `NonNull` takes no `unsafe`: `&**boxed` is the `Box`'s heap-allocation
/// address (the stable identity the pin protects), and `NonNull::from` only
/// records it. Re-derived (a fresh borrow) on EVERY access so the deref's
/// provenance is current even after the cache mutated the same allocation
/// through a `&mut` on its own path (the in-place rebuild swap).
fn current_relptr(oid: Oid) -> core::ptr::NonNull<RelationData> {
    with_state(|st| {
        let boxed = st
            .id_cache
            .get(&oid)
            .expect("relcache: RelationRef pins an absent descriptor");
        core::ptr::NonNull::from(&**boxed)
    })
}

/// A RAII pin on an open relation: the [`crate::Relation`] (`RelationData *`)
/// analog for callers that hold a relation across rebuilds. Holds a +1 on the
/// descriptor's `rd_refcnt` and the stable heap address of the `id_cache`
/// `Box`'s `RelationData`. Construct it with [`RelationRef::open`]; drop it (or
/// let it fall out of scope, including on a `?`/panic unwind) to unpin.
pub struct RelationRef {
    /// The relation OID — the cache key, what each access re-derives from, and
    /// what `Drop` unpins.
    oid: Oid,
    /// The stable address of the `id_cache` `Box`'s `RelationData`, recorded at
    /// `open`. The stability *witness*: each access asserts the freshly
    /// re-derived pointer still equals it. Never itself dereferenced.
    ptr: core::ptr::NonNull<RelationData>,
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
        let ptr = current_relptr(oid);
        Ok(RelationRef { oid, ptr })
    }

    /// The pinned relation's OID (`RelationGetRelid`).
    #[inline]
    pub fn oid(&self) -> Oid {
        self.oid
    }

    /// Run `f` with the descriptor borrowed immutably (the PREFERRED, momentary
    /// access form: the borrow cannot escape `f`).
    #[allow(unsafe_code)]
    #[inline]
    pub fn with<R>(&self, f: impl FnOnce(&RelationData) -> R) -> R {
        STATE.with(|s| {
            let st = s.borrow();
            let boxed = st
                .id_cache
                .get(&self.oid)
                .expect("relcache: RelationRef pins an absent descriptor");
            let fresh = core::ptr::NonNull::from(&**boxed);
            debug_assert_eq!(fresh, self.ptr, "pinned Box moved under RelationRef");
            // SAFETY: this guard holds `rd_refcnt > 0` on `self.oid` (incremented
            // in `open`, decremented only in `Drop`), so the cache neither frees
            // nor moves the `Box<RelationData>` (the in-place rebuild swaps fields
            // behind the same allocation; HashMap rehash moves only the table
            // slot). `fresh` is re-derived from the live store borrow `st` held
            // for this block (current provenance); the `&RelationData` lives only
            // for `f` (momentary read), held under `st`.
            let d: &RelationData = unsafe { fresh.as_ref() };
            f(d)
        })
    }

    /// Run `f` with the descriptor borrowed mutably (the in-place field-mutation
    /// arm). Like [`with`](Self::with), the borrow is scoped to `f`.
    #[allow(unsafe_code)]
    #[inline]
    pub fn with_mut<R>(&mut self, f: impl FnOnce(&mut RelationData) -> R) -> R {
        STATE.with(|s| {
            let mut st = s.borrow_mut();
            let boxed = st
                .id_cache
                .get_mut(&self.oid)
                .expect("relcache: RelationRef pins an absent descriptor");
            let mut fresh = core::ptr::NonNull::from(&mut **boxed);
            debug_assert_eq!(fresh, self.ptr, "pinned Box moved under RelationRef");
            // SAFETY: as `with` — `rd_refcnt > 0` keeps the allocation live and
            // unmoved. `&mut self` proves no other `RelationRef`-derived borrow is
            // live; `fresh` is re-derived from the unique store borrow `st` held
            // for this block (current provenance); the `&mut RelationData` lives
            // only for `f`.
            let d: &mut RelationData = unsafe { fresh.as_mut() };
            f(d)
        })
    }
}

impl core::ops::Deref for RelationRef {
    type Target = RelationData;

    /// Momentary read of the pinned descriptor. The result MUST NOT be held
    /// across any call that can rebuild this rel (the pin keeps the allocation
    /// alive but does NOT serialize against the in-place rebuild swap); prefer
    /// [`with`](RelationRef::with) when a rebuild might intervene.
    #[allow(unsafe_code)]
    #[inline]
    fn deref(&self) -> &RelationData {
        let fresh = current_relptr(self.oid);
        debug_assert_eq!(fresh, self.ptr, "pinned Box moved under RelationRef");
        // SAFETY: `rd_refcnt > 0` (held by this guard) => the cache neither frees
        // nor moves the `Box<RelationData>`, so `fresh` (== `self.ptr`) names a
        // live allocation. The returned borrow is momentary; the caller must not
        // hold it across a rebuild of this rel.
        unsafe { fresh.as_ref() }
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
            if old.rd_refcnt != 0 {
                // Still-referenced: C ereport(WARNING) about a leak (the displaced
                // pointer is simply overwritten in C; here the old `Box` is
                // dropped when we `insert` the replacement).
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
        // The previous `Box` (if any) is dropped here — freeing the whole owned
        // subsidiary tree, the C `RelationDestroyRelation`/`pfree` cascade.
        st.id_cache.insert(id, reldesc);
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
/// `rd_id` and reclaim the owned `Box<RelationData>` (the C
/// `RelationDestroyRelation` `pfree` tree; here a single `Box` drop frees the
/// whole owned descriptor). The C `elog(ERROR)` if the entry is missing.
pub(crate) fn cache_delete(id: Oid) -> PgResult<()> {
    let removed = with_state(|st| st.id_cache.remove(&id));
    match removed {
        Some(d) => {
            // `d` (Box<RelationData>) dropped here: frees all subsidiary data.
            drop(d);
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
/// owner (unless in bootstrap mode). The resource-owner remember half is the
/// per-query-lifecycle RAII glue; until that owner lands it is the documented
/// no-op pin (the refcount itself is authoritative here).
pub fn RelationIncrementReferenceCount(rel: Oid) -> PgResult<()> {
    with_relation_mut(rel, |rd| rd.rd_refcnt += 1)?;
    // ResourceOwnerEnlarge + ResourceOwnerRememberRelationRef: resowner glue
    // (per-query-lifecycle RAII). The refcount above is the authoritative pin.
    Ok(())
}

/// `RelationDecrementReferenceCount(rel)` (relcache.c): drop the pin
/// (`rd_refcnt -= 1`), asserting it was positive, and forget the relation ref
/// with the resource owner (resowner glue, as above).
pub fn RelationDecrementReferenceCount(rel: Oid) -> PgResult<()> {
    with_relation_mut(rel, |rd| {
        debug_assert!(rd.rd_refcnt > 0);
        rd.rd_refcnt -= 1;
    })
}

/// `RelationClose(relation)` (relcache.c): drop the relcache reference, then run
/// [`RelationCloseCleanup`] (the immediate-flush-of-dropped-or-invalidated path).
pub fn RelationClose(relation: Oid) -> PgResult<()> {
    RelationDecrementReferenceCount(relation)?;
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
pub(crate) fn RelationCloseCleanup(relation: Oid) -> PgResult<()> {
    let do_clear = with_rel(relation, |rd| {
        rd.rd_refcnt == 0 && (!rd.rd_isvalid || rd.rd_droppedSubid != 0)
    });
    if do_clear {
        return crate::invalidate::RelationClearRelation(relation);
    }
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

/// Helper: copy a `PgVec<T: Copy>` into an owned `Vec<T>`.
fn copy_vec<T: Copy>(v: &mcx::PgVec<'_, T>) -> Vec<T> {
    v.iter().copied().collect()
}

/// Helper: re-project an owned `&[T: Copy]` into a fresh `PgVec` in `mcx`.
fn project_vec<'mcx, T: Copy>(mcx: mcx::Mcx<'mcx>, src: &[T]) -> mcx::PgVec<'mcx, T> {
    let mut out = mcx::PgVec::new_in(mcx);
    out.extend_from_slice(src);
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
        match node {
            types_nodes::nodes::Node::Expr(e) => owned.push(e.clone()),
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!(
                        "rd_partcheck entry is not an expression node (tag {:?})",
                        other.tag()
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
                    out.push(types_nodes::nodes::Node::Expr(e.clone()));
                }
                Ok((*valid, out))
            }
            None => Ok((false, out)),
        }
    })
}
