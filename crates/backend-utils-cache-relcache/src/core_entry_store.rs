//! core-entry-store family — the REAL relcache substrate.
//!
//! Owns the mutable [`entry::RelationData`] descriptor, the real
//! `RelationIdCache` dynahash (the C `RelIdCacheEnt` store keyed by `Oid`),
//! the per-backend `eoxact_list`/`in_progress_list` state, and the refcount
//! lifecycle. This is the substrate the prior decomposition omitted (it built
//! around a trimmed value-slice instead). Nothing here is `todo!()`.

pub mod entry;

use std::cell::RefCell;

use backend_utils_error::{ereport, PgResult};
use types_error::ERROR;
use backend_utils_hash_dynahash::{hash_create, hash_search, hash_seq_init, hash_seq_search};
use types_core::primitive::{Oid, ProcNumber};
use types_core::xact::SubTransactionId;
use types_hash::hsearch::{HASHACTION, HASHCTL, HASH_BLOBS, HASH_ELEM, HASH_SEQ_STATUS, HTAB};

use crate::{INITRELCACHESIZE, MAX_EOXACT_LIST};
use entry::RelationData;

/* ==========================================================================
 * `RelIdCacheEnt` (relcache.c) — the dynahash element: `{ Oid reloid;
 * Relation reldesc; }`. The C key is the `Oid`; the payload is the
 * `Relation` pointer. We store a raw `*mut RelationData` into a heap-owned
 * `Box<RelationData>` that the cache keeps alive — the C `Relation` pointer.
 * ======================================================================== */

/// `RelIdCacheEnt` (relcache.c). `#[repr(C)]` so dynahash's `HASH_BLOBS`
/// byte-copy of the `Oid` key lands on `reloid` at offset 0, exactly as C.
#[repr(C)]
struct RelIdCacheEnt {
    /// `Oid reloid` — the hash key (offset 0; `keysize == sizeof(Oid)`).
    reloid: Oid,
    /// `Relation reldesc` — the C `Relation` pointer into the owned descriptor.
    reldesc: *mut RelationData,
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
 * cell (matching `inval.c` and the other ported cache crates). The dynahash
 * `RelationIdCache` *owns* every descriptor: a `Box<RelationData>` is leaked
 * into the cache on insert and reclaimed on delete.
 * ======================================================================== */

pub(crate) struct RelcacheState {
    /// `RelationIdCache` — the OID→`Relation` dynahash. `null` until
    /// [`crate::initfile`]'s `RelationCacheInitialize` creates it.
    pub(crate) id_cache: *mut HTAB,
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
}

impl RelcacheState {
    const fn new() -> Self {
        Self {
            id_cache: std::ptr::null_mut(),
            in_progress_list: Vec::new(),
            eoxact_list: Vec::new(),
            eoxact_list_overflowed: false,
            relcache_invals_received: 0,
            critical_relcaches_built: false,
            critical_shared_relcaches_built: false,
        }
    }
}

thread_local! {
    pub(crate) static STATE: RefCell<RelcacheState> = const { RefCell::new(RelcacheState::new()) };
}

/// Run `f` with mutable access to the per-backend relcache state.
pub(crate) fn with_state<R>(f: impl FnOnce(&mut RelcacheState) -> R) -> R {
    STATE.with(|s| f(&mut s.borrow_mut()))
}

/* ==========================================================================
 * `RelationIdCache` creation (the dynahash half of `RelationCacheInitialize`).
 * ======================================================================== */

/// Build the empty `RelationIdCache` dynahash, mirroring
/// `RelationCacheInitialize`: `keysize = sizeof(Oid)`, `entrysize =
/// sizeof(RelIdCacheEnt)`, `HASH_ELEM | HASH_BLOBS`. Stored on the per-backend
/// state. Called from [`crate::initfile`]'s `RelationCacheInitialize`.
pub(crate) fn create_id_cache() -> PgResult<()> {
    let mut ctl = blank_hashctl();
    ctl.keysize = std::mem::size_of::<Oid>();
    ctl.entrysize = std::mem::size_of::<RelIdCacheEnt>();
    let htab = hash_create(
        "Relcache by OID",
        INITRELCACHESIZE,
        &ctl,
        HASH_ELEM | HASH_BLOBS,
    )?;
    with_state(|st| st.id_cache = htab);
    Ok(())
}

fn blank_hashctl() -> HASHCTL {
    HASHCTL {
        num_partitions: 0,
        ssize: 0,
        dsize: 0,
        max_dsize: 0,
        keysize: 0,
        entrysize: 0,
        hash: None,
        match_: None,
        keycopy: None,
        alloc: None,
        hcxt: std::ptr::null_mut(),
        hctl: std::ptr::null_mut(),
    }
}

/* ==========================================================================
 * `RelationCacheInsert` / `RelationIdCacheLookup` / `RelationCacheDelete`
 * (relcache.c macros) — the dynahash element operations over the leaked
 * `Box<RelationData>` descriptors.
 * ======================================================================== */

/// `RelationCacheInsert(RELATION, replace_allowed)` (relcache.c macro): enter
/// `reldesc` into `RelationIdCache` under its `rd_id`. Returns the C
/// `oldrel` (the descriptor a collision found already present) when one
/// exists, else `None`. The C asserts `replace_allowed || !found`. Takes
/// ownership of the `Box`, leaking it into the cache as the stable `Relation`
/// pointer.
#[allow(unsafe_code)]
pub(crate) fn cache_insert(
    reldesc: Box<RelationData>,
    replace_allowed: bool,
) -> PgResult<Option<*mut RelationData>> {
    let reloid = reldesc.rd_id;
    let ptr = Box::into_raw(reldesc);
    with_state(|st| {
        debug_assert!(!st.id_cache.is_null(), "RelationIdCache not initialized");
        let key = reloid.to_ne_bytes();
        let (entry_ptr, found) = hash_search(st.id_cache, key.as_ptr(), HASHACTION::HASH_ENTER)?;
        // SAFETY: `entry_ptr` is the dynahash element buffer sized
        // `sizeof(RelIdCacheEnt)` (set at create); we read/write the
        // `RelIdCacheEnt` payload in place, exactly as the C macro casts it.
        let hentry = unsafe { &mut *(entry_ptr as *mut RelIdCacheEnt) };
        let old = if found {
            // C: `Assert(replace_allowed)`; surface the prior `Relation`.
            debug_assert!(replace_allowed);
            Some(hentry.reldesc)
        } else {
            None
        };
        hentry.reloid = reloid;
        hentry.reldesc = ptr;
        Ok(old)
    })
}

/// `RelationIdCacheLookup(ID, RELATION)` (relcache.c macro): the `HASH_FIND`
/// lookup, returning the cached `Relation` pointer (`None` == the C `NULL`).
#[allow(unsafe_code)]
pub(crate) fn cache_lookup(id: Oid) -> Option<*mut RelationData> {
    with_state(|st| {
        if st.id_cache.is_null() {
            return None;
        }
        let key = id.to_ne_bytes();
        let (entry_ptr, found) =
            hash_search(st.id_cache, key.as_ptr(), HASHACTION::HASH_FIND).ok()?;
        if !found {
            return None;
        }
        // SAFETY: found element buffer is a live `RelIdCacheEnt`.
        let hentry = unsafe { &*(entry_ptr as *const RelIdCacheEnt) };
        Some(hentry.reldesc)
    })
}

/// `RelationCacheDelete(RELATION)` (relcache.c macro): `HASH_REMOVE` the entry
/// for `rd_id` and reclaim the owned `Box<RelationData>` (the C
/// `RelationDestroyRelation` `pfree` tree; here a single `Box` drop frees the
/// whole owned descriptor). The C `elog(ERROR)` if the entry is missing.
#[allow(unsafe_code)]
pub(crate) fn cache_delete(id: Oid) -> PgResult<()> {
    let removed = with_state(|st| -> PgResult<Option<*mut RelationData>> {
        if st.id_cache.is_null() {
            return Ok(None);
        }
        let key = id.to_ne_bytes();
        let (entry_ptr, found) = hash_search(st.id_cache, key.as_ptr(), HASHACTION::HASH_REMOVE)?;
        if !found {
            return Ok(None);
        }
        // SAFETY: removed element buffer is still readable (on freelist).
        let hentry = unsafe { &*(entry_ptr as *const RelIdCacheEnt) };
        Ok(Some(hentry.reldesc))
    })?;
    match removed {
        Some(ptr) => {
            // SAFETY: the cache held the only `Relation` pointer to this
            // descriptor; with the entry removed, reclaiming the `Box` frees
            // the whole owned tree exactly once.
            unsafe { drop(Box::from_raw(ptr)) };
            Ok(())
        }
        None => Err(ereport(ERROR)
            .errmsg_internal("trying to delete a reldesc that does not exist")
            .into_error()),
    }
}

/// Collect the `Relation` pointer of every live `RelIdCacheEnt` in
/// `RelationIdCache` via a `hash_seq_init`/`hash_seq_search` scan (relcache.c's
/// `HASH_SEQ_STATUS` walk). Returned as an owned snapshot so callers that need
/// to delete/rebuild entries (the `RelationCacheInvalidate` / `AtEOXact` whole-
/// cache passes) don't mutate the table while a `hash_seq_search` is live —
/// matching the C requirement that `hash_seq_search` only copes with deletion
/// of the element it is currently visiting.
#[allow(unsafe_code)]
pub(crate) fn cache_seq_reldescs() -> Vec<*mut RelationData> {
    with_state(|st| {
        let mut out = Vec::new();
        if st.id_cache.is_null() {
            return out;
        }
        let mut status = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut status, st.id_cache);
        loop {
            // SAFETY: scan over the live `RelationIdCache`; the returned key
            // pointer is the element buffer (a `RelIdCacheEnt`, `reloid` at
            // offset 0). Null terminates and deregisters the scan.
            let key = match hash_seq_search(&mut status) {
                Ok(p) => p,
                Err(_) => break,
            };
            if key.is_null() {
                break;
            }
            // SAFETY: live element buffer is a `RelIdCacheEnt`.
            let hentry = unsafe { &*(key as *const RelIdCacheEnt) };
            out.push(hentry.reldesc);
        }
        out
    })
}

/// `hash_search(RelationIdCache, &relid, HASH_FIND)` returning the entry's
/// `Relation` (the `AtEOXact`/`AtEOSubXact` non-overflow path). `None` is the
/// C `NULL` (entry not present — nothing to do, per the C comment).
pub(crate) fn cache_find_reldesc(relid: Oid) -> Option<*mut RelationData> {
    cache_lookup(relid)
}

/// `eoxact_list` reset half of `AtEOXact_RelationCache` tail (clear the list and
/// overflow flag once we're out of the transaction).
pub(crate) fn eoxact_list_reset(st: &mut RelcacheState) {
    st.eoxact_list.clear();
    st.eoxact_list_overflowed = false;
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
 * RelationClose, RelationIdGetRelation). These operate on the `Relation`
 * pointer and mutate the owned descriptor in place — REAL logic.
 * ======================================================================== */

/// `RelationIncrementReferenceCount(rel)` (relcache.c): pin the entry
/// (`rd_refcnt += 1`) and register the relation ref with the current resource
/// owner (unless in bootstrap mode). The resource-owner remember half is the
/// per-query-lifecycle RAII glue; until that owner lands it is the documented
/// no-op pin (the refcount itself is authoritative here).
#[allow(unsafe_code)]
pub fn RelationIncrementReferenceCount(rel: *mut RelationData) -> PgResult<()> {
    // SAFETY: callers hold a live `Relation` pointer into a cache-owned (or
    // in-build) descriptor; pinning keeps it live.
    let rd = unsafe { &mut *rel };
    rd.rd_refcnt += 1;
    // ResourceOwnerEnlarge + ResourceOwnerRememberRelationRef: resowner glue
    // (per-query-lifecycle RAII). The owner installs the remember/forget pair
    // when it lands; the refcount above is the authoritative pin.
    Ok(())
}

/// `RelationDecrementReferenceCount(rel)` (relcache.c): drop the pin
/// (`rd_refcnt -= 1`), asserting it was positive, and forget the relation ref
/// with the resource owner (resowner glue, as above).
#[allow(unsafe_code)]
pub fn RelationDecrementReferenceCount(rel: *mut RelationData) -> PgResult<()> {
    // SAFETY: as `RelationIncrementReferenceCount`.
    let rd = unsafe { &mut *rel };
    debug_assert!(rd.rd_refcnt > 0);
    rd.rd_refcnt -= 1;
    Ok(())
}

/// `RelationClose(relation)` (relcache.c): drop the relcache reference, then
/// run [`RelationCloseCleanup`] (the immediate-flush-of-dropped-or-invalidated
/// path). No lock manipulation here (locks release at xact end).
pub fn RelationClose(relation: *mut RelationData) -> PgResult<()> {
    RelationDecrementReferenceCount(relation)?;
    RelationCloseCleanup(relation)
}

/// `RelationCloseCleanup(relation)` (relcache.c): when the entry's refcount has
/// returned to zero and it has been invalidated or dropped, flush it now
/// rather than waiting for the next inval. The clear half is
/// [`crate::invalidate`] logic; this routine's own decision (refcount-zero +
/// invalid/dropped) lives here.
#[allow(unsafe_code)]
pub(crate) fn RelationCloseCleanup(relation: *mut RelationData) -> PgResult<()> {
    // SAFETY: live `Relation` pointer.
    let rd = unsafe { &*relation };
    if rd.rd_refcnt == 0 && (!rd.rd_isvalid || rd.rd_droppedSubid != 0) {
        return crate::invalidate::RelationClearRelation(relation);
    }
    Ok(())
}

/// `RelationIdGetRelation(relationId)` (relcache.c): the cache lookup + lazy
/// build entry point. Looks up the entry; if valid (and not dropped) pins and
/// revalidates it, else builds a fresh descriptor via
/// [`crate::build::RelationBuildDesc`] and pins it. `null` is the C `NULL`
/// (no `pg_class` row).
#[allow(unsafe_code)]
pub fn RelationIdGetRelation(relationId: Oid) -> PgResult<*mut RelationData> {
    if let Some(rd) = cache_lookup(relationId) {
        // SAFETY: cache-owned descriptor, live while in the cache.
        let r = unsafe { &*rd };
        // Return NULL for dropped relations.
        if r.rd_droppedSubid != 0 {
            debug_assert!(!r.rd_isvalid);
            return Ok(std::ptr::null_mut());
        }
        RelationIncrementReferenceCount(rd)?;
        if !r.rd_isvalid {
            crate::invalidate::RelationRebuildRelation(rd)?;
        }
        return Ok(rd);
    }

    // Not cached: build one and add it.
    let rd = crate::build::RelationBuildDesc(relationId, true)?;
    if !rd.is_null() {
        RelationIncrementReferenceCount(rd)?;
    }
    Ok(rd)
}

/* ==========================================================================
 * Seam-facing scalar reads off the owned entry.
 * ======================================================================== */

/// Owning-backend proc number for a temp relation (`rd_backend`).
#[allow(unsafe_code)]
pub(crate) fn rd_backend_of(rel: *mut RelationData) -> ProcNumber {
    // SAFETY: live `Relation` pointer.
    unsafe { (*rel).rd_backend }
}

/// `rd_createSubid` read (used by `RelationNeedsWAL`/`RELATION_IS_LOCAL`).
#[allow(unsafe_code)]
pub(crate) fn rd_create_subid_of(rel: *mut RelationData) -> SubTransactionId {
    // SAFETY: live `Relation` pointer.
    unsafe { (*rel).rd_createSubid }
}
