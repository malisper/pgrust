//! `pgstat.c` core — the flush driver, the snapshot machinery, the per-kind
//! callback dispatch, and the backend init/shutdown lifecycle.
//!
//! Faithful port of PG 18.3 `utils/activity/pgstat.c`'s cross-kind core:
//! `pgstat_report_stat` (the flush driver), `pgstat_prep_pending_entry`,
//! `pgstat_fetch_entry`, `pgstat_build_snapshot` / `pgstat_snapshot_fixed`,
//! `flush_pending_entries`, `pgstat_clear_snapshot`, `pgstat_reset` dispatch,
//! and `pgstat_initialize` / `pgstat_before_server_shutdown`. The per-kind
//! callbacks are dispatched through the registry-assembled
//! `pgstat_kind_builtin_infos[]` (see [`crate::registry`]); kinds whose owner
//! crate is not yet ported are simply absent from the table, exactly as a C
//! kind with `NULL` callbacks contributes nothing to a flush/snapshot.

use core::any::Any;

use types_core::{Oid, TimestampTz};
use types_error::PgResult;
use types_pgstat::activity_pgstat::{
    PgStat_FetchConsistency, PgStat_Kind, PGSTAT_KIND_BUILTIN_MAX, PGSTAT_KIND_BUILTIN_MIN,
    PGSTAT_KIND_MAX, PGSTAT_KIND_MIN,
};
use types_pgstat::pgstat_internal::{PgStat_HashKey, PgStatShared_Common, PgStatShared_HashEntry};

use crate::entry_ref::PgStat_EntryRef;
use crate::local;
use crate::registry;
use crate::shmem;

use backend_lib_dshash as dshash;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_proc_seams as proc_seams;
use backend_utils_adt_timestamp_seams as timestamp;
use backend_utils_mmgr_dsa_seams as dsa;
use types_storage::LW_SHARED;

/// `PGSTAT_MIN_INTERVAL` (`pgstat.c`) — minimum ms between stats reports.
const PGSTAT_MIN_INTERVAL: i64 = 1000;
/// `PGSTAT_MAX_INTERVAL` (`pgstat.c`) — maximum ms before a forced report.
const PGSTAT_MAX_INTERVAL: i64 = 60000;
/// `PGSTAT_IDLE_INTERVAL` (`pgstat.c`) — report sooner when idle.
const PGSTAT_IDLE_INTERVAL: i64 = 10000;

// The last time stats were flushed and the soonest a partial flush wants to
// retry; C file-statics `pgStatPendingInterval` / `pgStatLastReport` analogues.
thread_local! {
    static PENDING_SINCE: core::cell::Cell<TimestampTz> = const { core::cell::Cell::new(0) };
    static HAVE_PENDING: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
    // C file-static `pgStatForceNextFlush` (pgstat.c:250): when set, the next
    // `pgstat_report_stat` is treated as forced even when nothing appears
    // pending. Per-backend state, hence thread_local (not a shared Mutex).
    static FORCE_NEXT_FLUSH: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

/// `pgstat_force_next_flush(void)` (`pgstat.c:813`) — force locally pending
/// stats to be flushed during the next `pgstat_report_stat()` call. Used by
/// the `pg_stat_force_next_flush()` SQL function (for writing tests).
pub fn pgstat_force_next_flush() {
    FORCE_NEXT_FLUSH.with(|c| c.set(true));
}

// ---------------------------------------------------------------------------
// pgstat_report_stat — the flush driver.
// ---------------------------------------------------------------------------

/// `pgstat_report_stat(force)` (`pgstat.c`) — flush this backend's pending
/// cumulative statistics into shared memory.
///
/// Returns the timestamp (ms-since-epoch in C `TimestampTz`) of the soonest
/// useful next flush, or `0` if there is nothing pending. Mirrors C: it rate
/// limits non-forced reports (`PGSTAT_MIN_INTERVAL`), flushes per-entry pending
/// data through each kind's `flush_pending_cb`, flushes the static (fixed)
/// kinds through `flush_static_cb`, and retries-or-defers on lock contention.
pub fn pgstat_report_stat(mut force: bool) -> PgResult<i64> {
    // pgstat_assert_is_up()-equivalent + shutdown guard.
    if local::is_shutdown() {
        return Ok(0);
    }

    // "absorb" the forced flush even if there's nothing to flush (C
    // pgstat.c:704). Must happen before the "nothing pending" early return.
    if FORCE_NEXT_FLUSH.with(|c| c.get()) {
        force = true;
        FORCE_NEXT_FLUSH.with(|c| c.set(false));
    }

    // Is there anything to flush? (pending entries OR fixed/static kinds.)
    let have_pending_entries = local::with_pending(|p| {
        p.entry_ref_hash.values().any(|e| e.entry_ref.pending.is_some())
    });
    let have_static = kinds_have_static();

    if !have_pending_entries && !have_static && !HAVE_PENDING.with(|c| c.get()) {
        // Nothing pending.
        return Ok(0);
    }

    let now = timestamp::get_current_timestamp::call();

    // Rate limiting (C's `pgstat_report_stat`): unless forced, wait until at
    // least PGSTAT_MIN_INTERVAL has elapsed; defer otherwise.
    if !force {
        let pending_since = PENDING_SINCE.with(|c| c.get());
        if pending_since != 0 && (now - pending_since) < PGSTAT_MIN_INTERVAL * 1000 {
            // Not enough time has elapsed; report the soonest retry time.
            return Ok(pending_since + PGSTAT_MIN_INTERVAL * 1000);
        }
        if pending_since == 0 {
            PENDING_SINCE.with(|c| c.set(now));
            return Ok(now + PGSTAT_MIN_INTERVAL * 1000);
        }
    }

    // Flush per-entry (variable-numbered) pending data.
    let partial = flush_pending_entries(force)?;

    // Flush static (fixed-numbered) kinds.
    let partial_static = flush_static_kinds(force)?;

    if partial || partial_static {
        // Some entries could not be flushed (lock contention). Remember to
        // retry soon, and report when.
        PENDING_SINCE.with(|c| {
            if c.get() == 0 {
                c.set(now);
            }
        });
        HAVE_PENDING.with(|c| c.set(true));
        let pending_since = PENDING_SINCE.with(|c| c.get());
        let _ = (PGSTAT_MAX_INTERVAL, PGSTAT_IDLE_INTERVAL);
        return Ok(pending_since + PGSTAT_MIN_INTERVAL * 1000);
    }

    // Everything flushed.
    PENDING_SINCE.with(|c| c.set(0));
    HAVE_PENDING.with(|c| c.set(false));
    Ok(0)
}

/// `flush_pending_entries(nowait)` (`pgstat.c`) — flush each pending entry's
/// per-kind data through its `flush_pending_cb`. Entries whose callback reports
/// it could not flush (lock contention) keep their pending data and are
/// retried; the rest have their pending data dropped. Returns `true` if any
/// entry could not be flushed.
fn flush_pending_entries(nowait: bool) -> PgResult<bool> {
    // Collect the keys of pending entries (snapshot the worklist, since the
    // callbacks may touch pgStatPending).
    let pending_keys: Vec<_> = local::with_pending(|p| {
        p.entry_ref_hash
            .iter()
            .filter(|(_, e)| e.entry_ref.pending.is_some())
            .map(|(k, _)| *k)
            .collect()
    });

    let mut not_all_flushed = false;

    for key in pending_keys {
        let kind = key.kind;
        let kind_info = match registry::pgstat_get_kind_info(kind) {
            Some(ki) => ki,
            // A kind whose owner crate isn't ported has no entry in the table;
            // it cannot have produced pending data, so this is unreachable in
            // practice. Skip defensively (C indexes a NULL-cb slot only for
            // registered kinds).
            None => continue,
        };

        let flush_cb = match &kind_info.cb.flush_pending_cb {
            Some(cb) => cb,
            None => {
                // No per-entry flush for this kind (e.g. it uses flush_static);
                // drop its stale pending marker.
                drop_pending(&key);
                continue;
            }
        };

        // Run the callback against the live entry-ref. The entry-ref lives in
        // the owner-private hash; borrow it for the call.
        let er_ptr = local::with_pending(|p| {
            p.entry_ref_hash
                .get_mut(&key)
                .map(|e| e.entry_ref.as_mut() as *mut PgStat_EntryRef)
        });
        let er_ptr = match er_ptr {
            Some(p) => p,
            None => continue,
        };
        // SAFETY: pointer into the owner-private boxed entry-ref; the callback
        // is the kind's flush logic which expects &mut PgStat_EntryRef.
        let er = unsafe { &mut *er_ptr };

        let could_not_flush = flush_cb(er, nowait)?;
        if could_not_flush {
            not_all_flushed = true;
        } else {
            // Flushed: drop the pending data (C frees sr->pending here).
            er.pending = None;
        }
    }

    Ok(not_all_flushed)
}

/// Drop an entry's pending data without flushing (used when a kind has no
/// `flush_pending_cb`).
fn drop_pending(key: &types_pgstat::pgstat_internal::PgStat_HashKey) {
    local::with_pending(|p| {
        if let Some(e) = p.entry_ref_hash.get_mut(key) {
            e.entry_ref.pending = None;
        }
    });
}

/// Whether any registered kind has a `flush_static_cb` (i.e. could have
/// pending static stats).
fn kinds_have_static() -> bool {
    registry::kind_table()
        .iter()
        .any(|(_, ki)| ki.cb.flush_static_cb.is_some())
}

/// `flush_static_kinds` — invoke every registered kind's `flush_static_cb`
/// (the fixed-numbered kinds that do not use `PgStat_EntryRef->pending`).
/// Returns `true` if any reports it could not flush.
fn flush_static_kinds(nowait: bool) -> PgResult<bool> {
    let mut not_all = false;
    // Iterate ascending kind order (the loop shape pgstat.c uses).
    let cbs: Vec<_> = registry::kind_table()
        .iter()
        .filter_map(|(_, ki)| ki.cb.flush_static_cb.as_ref().map(|cb| cb as *const _))
        .collect();
    for cb_ptr in cbs {
        // SAFETY: the closure lives in the sealed, 'static kind table.
        let cb: &Box<dyn Fn(bool) -> PgResult<bool> + Send + Sync> =
            unsafe { &*(cb_ptr as *const _) };
        if cb(nowait)? {
            not_all = true;
        }
    }
    Ok(not_all)
}

// ---------------------------------------------------------------------------
// pgstat_prep_pending_entry — allocate pending storage for an entry.
// ---------------------------------------------------------------------------

/// `pgstat_prep_pending_entry(kind, dboid, objid, created_entry)` (`pgstat.c`)
/// — return this backend's reference to the shared entry, creating its
/// backend-private pending block (sized by the kind's `pending_size`) if it
/// does not yet exist. The pending block is the `void *pending` slot, modeled
/// here as a `Box<dyn Any>` the per-kind crate downcasts; the concrete pending
/// value is supplied by `make_pending`.
pub fn pgstat_prep_pending_entry(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
    make_pending: impl FnOnce() -> Box<dyn Any>,
) -> PgResult<shmem::EntryRefPtr> {
    pgstat_prep_pending_entry_created(kind, dboid, objid, None, make_pending)
}

/// `pgstat_prep_pending_entry(kind, dboid, objid, created_entry, ...)`
/// (`pgstat.c`) — like [`pgstat_prep_pending_entry`], but reports through
/// `created_entry` whether the *shared* entry had to be created (C's
/// `bool *created_entry` out-param). `pgstat_init_function_usage` uses this to
/// detect a concurrently-dropped function.
pub fn pgstat_prep_pending_entry_created(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
    created_entry: Option<&mut bool>,
    make_pending: impl FnOnce() -> Box<dyn Any>,
) -> PgResult<shmem::EntryRefPtr> {
    let entry_ref = shmem::pgstat_get_entry_ref(kind, dboid, objid, true, created_entry)?
        .expect("pgstat_prep_pending_entry: get_entry_ref(create=true) returned None");

    // SAFETY: a just-resolved live reference.
    let er = unsafe { entry_ref.get() };
    if er.pending.is_none() {
        er.pending = Some(make_pending());
        // Link into pgStatPending (modeled by the pending.is_some() membership;
        // the dlist is the fast subset, not a separate source of truth).
        HAVE_PENDING.with(|c| c.set(true));
        let now = timestamp::get_current_timestamp::call();
        PENDING_SINCE.with(|c| {
            if c.get() == 0 {
                c.set(now);
            }
        });
    }
    Ok(entry_ref)
}

/// `pgstat_fetch_pending_entry(kind, dboid, objid)` (`pgstat.c`) — return this
/// backend's reference to the shared entry only if it already exists AND
/// currently carries a backend-private pending block; otherwise `None` (does
/// not create). Used by `find_funcstat_entry` / `pgstat_end_function_usage`.
pub fn pgstat_fetch_pending_entry(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
) -> PgResult<Option<shmem::EntryRefPtr>> {
    let entry_ref = shmem::pgstat_get_entry_ref(kind, dboid, objid, false, None)?;
    match entry_ref {
        None => Ok(None),
        Some(er) => {
            // SAFETY: a just-resolved live reference.
            let e = unsafe { er.get() };
            if e.pending.is_none() {
                Ok(None)
            } else {
                Ok(Some(er))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// pending-block access by key — the xact-driven relation hooks' reach into the
// owner-private entry-ref hash.
// ---------------------------------------------------------------------------

/// Run `f` against the pending block of the entry keyed by `key`, downcast to
/// the per-kind pending type `T`, if such an entry exists and currently carries
/// pending data.
///
/// This is the seamless reconciliation of C's raw `PgStat_TableStatus *` walks
/// (`AtEOXact_PgStat_Relations` / `AtEOSubXact_PgStat_Relations` /
/// `pgstat_report_analyze`'s `trans` / `parent` pointer chases): in C the
/// per-table `pending` block (`PgStat_TableStatus`) is reachable directly
/// through the cached pointer, but in this model that block is the `void
/// *pending` value living in `pgstat.c`'s owner-private entry-ref hash
/// ([`crate::entry_ref::PgStat_EntryRef::pending`], a `Box<dyn Any>`). The
/// xact-driven hooks hold only a [`PgStat_HashKey`] (from the level node's
/// `first` list or a node's `parent`), so they reach the pending block by key
/// and downcast it here.
///
/// Returns `None` when no entry-ref exists for `key`, when it has no pending
/// data, or when the pending value is not a `T` (a kind/key mismatch — a bug at
/// the call site, surfaced rather than mis-downcast).
pub fn pgstat_with_pending_mut<T: 'static, R>(
    key: types_pgstat::pgstat_internal::PgStat_HashKey,
    f: impl FnOnce(&mut T) -> R,
) -> Option<R> {
    local::with_pending(|p| {
        let entry = p.entry_ref_hash.get_mut(&key)?;
        let pending = entry.entry_ref.pending.as_mut()?;
        let typed = pending.downcast_mut::<T>()?;
        Some(f(typed))
    })
}

/// Whether the entry keyed by `key` currently carries pending data — C's
/// implicit `pgstat_fetch_pending_entry(...) != NULL` test, used by the
/// relation hooks before reaching for a pending block.
pub fn pgstat_have_pending(key: types_pgstat::pgstat_internal::PgStat_HashKey) -> bool {
    local::with_pending(|p| {
        p.entry_ref_hash
            .get(&key)
            .is_some_and(|e| e.entry_ref.pending.is_some())
    })
}

// ---------------------------------------------------------------------------
// Snapshot building (variable-numbered fetch + fixed snapshot).
// ---------------------------------------------------------------------------

/// `stats_fetch_consistency` (the `pgstat_fetch_consistency` GUC) — the access
/// consistency mode for stats reads. C reads the file-static `int
/// pgstat_fetch_consistency`; the port reads the GUC's runtime storage through
/// its installed variable accessors and maps it to the typed enum.
fn fetch_consistency() -> PgStat_FetchConsistency {
    match backend_utils_misc_guc_tables::vars::pgstat_fetch_consistency.read() {
        0 => PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_NONE,
        1 => PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_CACHE,
        _ => PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT,
    }
}

/// `pgstat_clear_snapshot()` (`pgstat.c`) — discard any materialized snapshot:
/// reset the fixed/custom validity flags, drop the variable-numbered snapshot
/// hash and its arena, and reset the snapshot mode. (C also forwards to
/// `pgstat_clear_backend_activity_snapshot()` in `backend_status.c`; that
/// backend-status snapshot is a separate, already-ported subsystem and is reset
/// on its own clear path, so no forwarding is needed here.)
pub fn pgstat_clear_snapshot() {
    local::with_local(|l| {
        let snap = &mut l.snapshot;
        snap.mode = PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_NONE;
        for v in snap.fixed_valid.iter_mut() {
            *v = false;
        }
        for v in snap.custom_valid.iter_mut() {
            *v = false;
        }
        snap.snapshot_timestamp = 0;
    });

    // pgStatLocal.snapshot.stats = NULL; MemoryContextReset(context):
    // tear down the variable-numbered snapshot hash + arena.
    local::with_snapshot_stats(|s| {
        s.prepared = false;
        s.stats.clear();
    });
}

/// `pgstat_build_snapshot_fixed(kind)` (`pgstat.c`) — materialize a fixed-kind
/// snapshot through its `snapshot_cb`, if not already valid in the current
/// snapshot. Idempotent within a snapshot's lifetime.
fn pgstat_build_snapshot_fixed(kind: PgStat_Kind) -> PgResult<()> {
    if !kind.is_builtin() {
        return Ok(());
    }
    let idx = kind.0 as usize;

    let already_valid = local::with_local(|l| l.snapshot.fixed_valid[idx]);
    if already_valid {
        return Ok(());
    }

    let kind_info = match registry::pgstat_get_kind_info(kind) {
        Some(ki) => ki,
        None => return Ok(()),
    };
    let snapshot_cb = match &kind_info.cb.snapshot_cb {
        Some(cb) => cb,
        None => return Ok(()),
    };

    // Run the kind's snapshot_cb over (shmem, snapshot). The control block and
    // snapshot both live in pgStatLocal; borrow them together.
    local::with_local(|l| -> PgResult<()> {
        let ctl_ptr = l
            .shmem
            .as_ref()
            .map(|c| c.as_ref() as *const _)
            .expect("pgstat snapshot: shared control not initialized");
        // SAFETY: the snapshot_cb reads the shared control and writes the
        // snapshot; both are distinct fields of pgStatLocal.
        let ctl = unsafe { &*ctl_ptr };
        snapshot_cb(ctl, &mut l.snapshot)?;
        l.snapshot.fixed_valid[idx] = true;
        Ok(())
    })
}

/// `pgstat_snapshot_fixed(kind)` (`pgstat.c`) — ensure a fixed-kind snapshot is
/// materialized for reading (the inward seam consumed by the stats SQL views).
pub fn pgstat_snapshot_fixed(kind: PgStat_Kind) -> PgResult<()> {
    let now = timestamp::get_current_timestamp::call();
    local::with_local(|l| {
        if l.snapshot.snapshot_timestamp == 0 {
            l.snapshot.snapshot_timestamp = now;
        }
    });
    pgstat_build_snapshot_fixed(kind)
}

/// `pgstat_init_snapshot_fixed()` (`pgstat.c`) — build snapshots for *all*
/// fixed-numbered kinds at once (snapshot-consistency mode).
pub fn pgstat_init_snapshot_fixed() -> PgResult<()> {
    let mut kind = PGSTAT_KIND_BUILTIN_MIN.0;
    while kind <= PGSTAT_KIND_BUILTIN_MAX.0 {
        let k = PgStat_Kind(kind);
        if let Some(ki) = registry::pgstat_get_kind_info(k) {
            if ki.info.fixed_amount {
                pgstat_build_snapshot_fixed(k)?;
            }
        }
        kind += 1;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Variable-numbered snapshot / fetch (pgstat_fetch_entry & friends).
// ---------------------------------------------------------------------------

/// The byte slice of the kind-specific stats body within a shared entry: the
/// `shared_data_len` bytes following the common [`PgStatShared_Common`] header.
///
/// This mirrors C's `pgstat_get_entry_data(kind, stats)` (which returns
/// `(char *) stats + kind_info->shared_data_off`). The per-kind crates register
/// `shared_data_off == 0` (the typed dispatch makes the C offset meaningless),
/// so this port uses the only faithful offset: immediately after the header —
/// the same convention `pgstat_reset_entry` uses to zero the stats body.
///
/// # Safety
/// `shared_stats` must point at a live `PgStatShared_*` whose stats body is at
/// least `len` bytes; the entry's content lock should be held by the caller.
unsafe fn entry_data_bytes(shared_stats: *const PgStatShared_Common, len: usize) -> Box<[u8]> {
    let data_off = core::mem::size_of::<PgStatShared_Common>();
    let base = (shared_stats as *const u8).add(data_off);
    let mut out = alloc::vec![0u8; len].into_boxed_slice();
    core::ptr::copy_nonoverlapping(base, out.as_mut_ptr(), len);
    out
}

/// `pgstat_prep_snapshot()` (`pgstat.c`) — ensure the variable-numbered snapshot
/// hash + arena exist for the current snapshot lifetime. In C this lazily
/// allocates `pgStatLocal.snapshot.context` / `.stats`; the idiomatic model owns
/// both as one `HashMap`, so this only marks it prepared (and clears any stale
/// residue from a not-cleared prior snapshot).
fn pgstat_prep_snapshot() {
    local::with_snapshot_stats(|s| {
        if s.prepared {
            return;
        }
        s.stats.clear();
        s.prepared = true;
    });
}

/// `pgstat_build_snapshot()` (`pgstat.c`) — materialize a full snapshot of every
/// variable-numbered shared stats entry (and all fixed-numbered kinds), used in
/// `PGSTAT_FETCH_CONSISTENCY_SNAPSHOT` mode.
///
/// Walks the shared dshash, and for each live (non-dropped) variable-numbered
/// entry deep-copies its `shared_data_len` stats bytes into the snapshot hash
/// under the entry's content lock, exactly as C does (acquiring the body's
/// `LWLock` directly rather than through a `PgStat_EntryRef`).
fn pgstat_build_snapshot() -> PgResult<()> {
    // Snapshot already built for this lifetime.
    let already = local::with_local(|l| {
        l.snapshot.mode == PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT
    });
    if already {
        return Ok(());
    }

    pgstat_prep_snapshot();

    let (area, dsh) = local::with_local(|l| (l.dsa, l.shared_hash));

    // dshash_seq_init(&hstat, pgStatLocal.shared_hash, false): walk all entries.
    let mut hstat = dshash::dshash_seq_init(dsh, false);
    loop {
        let entry = dshash::dshash_seq_next(&mut hstat)?;
        let p = match entry {
            Some(e) => e as *mut PgStatShared_HashEntry,
            None => break,
        };

        // SAFETY: dshash_seq_next returns a live, locked entry address.
        let key = unsafe { (*p).key };
        let dropped = unsafe { (*p).dropped };
        let body = unsafe { (*p).body };

        let kind_info = match registry::pgstat_get_kind_info(key.kind) {
            Some(ki) => ki,
            None => continue,
        };
        // Fixed-numbered kinds are handled by pgstat_build_snapshot_fixed below.
        if kind_info.info.fixed_amount {
            continue;
        }
        // Dropped entries aren't visible.
        if dropped {
            continue;
        }

        // stats_data = dsa_get_address(pgStatLocal.dsa, p->body).
        let stats_data = dsa::dsa_get_address_ptr::call(area, body)? as usize
            as *mut PgStatShared_Common;
        if stats_data.is_null() {
            continue;
        }

        let len = kind_info.info.shared_data_len as usize;

        // Acquire the body's content lock directly (LW_SHARED), copy out the
        // stats bytes, release.
        // SAFETY: stats_data points at a live PgStatShared_Common header in the
        // shared segment, with a valid LWLock.
        let lock = unsafe { &(*stats_data).lock };
        let guard =
            lwlock::lwlock_acquire::call(lock, LW_SHARED, proc_seams::my_proc_number::call())?;
        // SAFETY: lock held; stats body is at least `len` bytes after the header.
        let data = unsafe { entry_data_bytes(stats_data, len) };
        drop(guard);

        // pgstat_snapshot_insert(stats, key): new entry, must not be found.
        local::with_snapshot_stats(|s| {
            s.stats.insert(key, Some(data));
        });
    }
    // dshash_seq_term(&hstat).
    dshash::dshash_seq_term(&mut hstat)?;

    // Build snapshot of all fixed-numbered stats.
    let mut kind = PGSTAT_KIND_BUILTIN_MIN.0;
    while kind <= PGSTAT_KIND_BUILTIN_MAX.0 {
        let k = PgStat_Kind(kind);
        if let Some(ki) = registry::pgstat_get_kind_info(k) {
            if ki.info.fixed_amount {
                pgstat_build_snapshot_fixed(k)?;
            }
        }
        kind += 1;
    }

    local::with_local(|l| {
        l.snapshot.mode = PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT;
    });
    Ok(())
}

/// `pgstat_fetch_entry(kind, dboid, objid)` (`pgstat.c`) — return a copy of a
/// variable-numbered entry's `shared_data_len` stats bytes, honoring
/// `stats_fetch_consistency` (none / cache / snapshot), or `None` if no live
/// entry exists.
///
/// * **NONE:** read straight from shared memory each call, no caching.
/// * **CACHE:** cache each looked-up entry (including a negative `None` marker)
///   in the snapshot hash, so repeated reads within a transaction are stable.
/// * **SNAPSHOT:** build a full snapshot of every entry up front, then read only
///   from it; a key absent from a full snapshot definitively does not exist.
///
/// The byte blob is the kind-specific stats struct (the per-kind owner decodes
/// it), mirroring C's `(PgStat_Stat*Entry *) pgstat_fetch_entry(...)`.
pub fn pgstat_fetch_entry(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
) -> PgResult<Option<Box<[u8]>>> {
    let kind_info = registry::pgstat_get_kind_info(kind);
    // AssertArg(!kind_info->fixed_amount): fixed kinds use pgstat_snapshot_fixed.
    debug_assert!(
        kind_info.map(|ki| !ki.info.fixed_amount).unwrap_or(true),
        "pgstat_fetch_entry called for a fixed-numbered kind"
    );

    pgstat_prep_snapshot();

    let key = PgStat_HashKey { kind, dboid, objid };
    let consistency = fetch_consistency();

    // If a full snapshot is wanted, build it.
    if consistency == PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT {
        pgstat_build_snapshot()?;
    }

    // If caching is desired, look up in the snapshot/cache hash.
    if consistency > PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_NONE {
        let cached = local::with_snapshot_stats(|s| s.stats.get(&key).cloned());
        if let Some(data) = cached {
            // Found (possibly a negative `None` marker).
            return Ok(data);
        }
        // In full-snapshot mode a missing key means the entry does not exist.
        if consistency == PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT {
            return Ok(None);
        }
    }

    // Resolve the live shared entry without creating it.
    let entry_ref = shmem::pgstat_get_entry_ref(kind, dboid, objid, false, None)?;
    let dropped = entry_ref
        .map(|er| {
            // SAFETY: just-resolved live reference.
            let e = unsafe { er.get() };
            e.shared_entry.is_null() || unsafe { (*e.shared_entry).dropped }
        })
        .unwrap_or(true);

    if entry_ref.is_none() || dropped {
        // Negative-cache an empty entry under CACHE mode.
        if consistency == PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_CACHE {
            local::with_snapshot_stats(|s| {
                s.stats.insert(key, None);
            });
        }
        return Ok(None);
    }

    let er = entry_ref.unwrap();
    let len = kind_info
        .map(|ki| ki.info.shared_data_len as usize)
        .unwrap_or(0);

    // Copy the stats body out under a shared content lock.
    // SAFETY: just-resolved live reference still in pgStatEntryRefHash.
    let e = unsafe { er.get() };
    shmem::pgstat_lock_entry_shared(e, false)?;
    // SAFETY: shared_stats points at a live PgStatShared_Common; lock held.
    let data = unsafe { entry_data_bytes(e.shared_stats, len) };
    shmem::pgstat_unlock_entry(e)?;

    // Cache the copy for stable repeated reads (cache/snapshot modes).
    if consistency > PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_NONE {
        let cached = data.clone();
        local::with_snapshot_stats(|s| {
            s.stats.insert(key, Some(cached));
        });
    }

    Ok(Some(data))
}

// ---------------------------------------------------------------------------
// reset / timestamp dispatch.
// ---------------------------------------------------------------------------

/// `pgstat_reset(kind, dboid, objid)` (`pgstat.c`) — reset one variable-
/// numbered stats entry to zero, then stamp its reset timestamp through the
/// kind's `reset_timestamp_cb`.
pub fn pgstat_reset(kind: PgStat_Kind, dboid: Oid, objid: u64) -> PgResult<()> {
    let ts = timestamp::get_current_timestamp::call();

    // reset the "single counter"
    pgstat_reset_entry(kind, dboid, objid, ts)
}

/// `pgstat_reset_entry(kind, dboid, objid, ts)` (`pgstat_shmem.c`) — reset one
/// variable-numbered stats entry: acquire its content lock (exclusive), zero
/// the kind-specific stats body following the common header, then stamp the
/// reset timestamp through the kind's `reset_timestamp_cb`
/// (`shared_stat_reset_contents`).
pub fn pgstat_reset_entry(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
    ts: TimestampTz,
) -> PgResult<()> {
    // Acquire the entry's content lock (exclusive) and zero its stats body.
    let entry_ref = shmem::pgstat_get_entry_ref_locked(kind, dboid, objid, false)?;
    let entry_ref = match entry_ref {
        Some(er) => er,
        None => return Ok(()),
    };
    // SAFETY: just-resolved live, content-locked reference.
    let er = unsafe { entry_ref.get() };

    if !er.shared_stats.is_null() {
        let kind_info = registry::pgstat_get_kind_info(kind);
        // SAFETY: shared_stats points at a live PgStatShared_Common header in
        // shared memory; the kind-specific stats body follows it.
        unsafe {
            // Zero the per-kind stats following the common header.
            if let Some(ki) = kind_info {
                let data_off = core::mem::size_of::<
                    types_pgstat::pgstat_internal::PgStatShared_Common,
                >();
                let data_len = ki.info.shared_data_len as usize;
                if data_len > 0 {
                    let base = (er.shared_stats as *mut u8).add(data_off);
                    core::ptr::write_bytes(base, 0, data_len);
                }
            }
            // reset_timestamp_cb(header, ts): stamp the reset time.
            if let Some(ki) = kind_info {
                if let Some(cb) = &ki.cb.reset_timestamp_cb {
                    cb(&mut *er.shared_stats, ts);
                }
            }
        }
    }

    // Release the content lock.
    shmem::pgstat_unlock_entry(er)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// startup: restore / discard the on-disk stats file.
// ---------------------------------------------------------------------------

/// `PGSTAT_STAT_PERMANENT_FILENAME` (`pgstat.h`) — the permanent statistics file
/// written at a clean shutdown and read back at startup.
const PGSTAT_STAT_PERMANENT_FILENAME: &str = "pg_stat/pgstat.stat";

/// `pgstat_reset_after_failure()` (`pgstat.c`) — reset/drop all stats after a
/// crash, or after restoring stats from disk failed. Resets every
/// fixed-numbered kind to the current time through its `reset_all_cb`, then drops
/// all variable-numbered entries.
fn pgstat_reset_after_failure() -> PgResult<()> {
    let ts = timestamp::get_current_timestamp::call();

    // Reset fixed-numbered stats.
    let mut kind = PGSTAT_KIND_MIN.0;
    while kind <= PGSTAT_KIND_MAX.0 {
        let k = PgStat_Kind(kind);
        kind += 1;

        let kind_info = match registry::pgstat_get_kind_info(k) {
            Some(ki) => ki,
            None => continue,
        };
        if !kind_info.info.fixed_amount {
            continue;
        }
        if let Some(cb) = &kind_info.cb.reset_all_cb {
            // reset_all_cb(ts): the adapter projects the field of the shared
            // control block.
            local::with_local(|l| -> PgResult<()> {
                if let Some(ctl) = l.shmem.as_deref_mut() {
                    cb(ctl, ts)?;
                }
                Ok(())
            })?;
        }
    }

    // And drop the variable-numbered ones.
    shmem::pgstat_drop_all_entries()
}

// ---------------------------------------------------------------------------
// On-disk stats file (de)serialization (pgstat_read_statsfile /
// pgstat_write_statsfile).
// ---------------------------------------------------------------------------

/// `PGSTAT_FILE_FORMAT_ID` (`pgstat.h`).
const PGSTAT_FILE_FORMAT_ID: i32 =
    types_pgstat::activity_pgstat::PGSTAT_FILE_FORMAT_ID as i32;

/// `PGSTAT_FILE_ENTRY_END` (`pgstat.c`) — end of file.
const PGSTAT_FILE_ENTRY_END: u8 = b'E';
/// `PGSTAT_FILE_ENTRY_FIXED` (`pgstat.c`) — fixed-numbered stats entry.
const PGSTAT_FILE_ENTRY_FIXED: u8 = b'F';
/// `PGSTAT_FILE_ENTRY_NAME` (`pgstat.c`) — stats entry identified by name.
const PGSTAT_FILE_ENTRY_NAME: u8 = b'N';
/// `PGSTAT_FILE_ENTRY_HASH` (`pgstat.c`) — stats entry identified by hash key.
const PGSTAT_FILE_ENTRY_HASH: u8 = b'S';

/// `NAMEDATALEN` (`c.h`) — the fixed size of a `NameData` byte buffer.
const NAMEDATALEN: usize = 64;

/// A forward-only byte cursor over the read-in stats file image, mirroring C's
/// `read_chunk(fpin, ptr, len)` / `fgetc(fpin)` over a `FILE *`. `read_chunk`
/// returns `false` on a short read (C `fread(...) == len`), which the caller
/// treats as a corrupt file.
struct ReadCursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ReadCursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        ReadCursor { buf, pos: 0 }
    }

    /// `read_chunk(fpin, ptr, len)` — copy `len` bytes; `None` on short read.
    fn read_chunk(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(len)?;
        if end > self.buf.len() {
            return None;
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Some(s)
    }

    /// `read_chunk_s(fpin, &i32)` — an `int32` (native LE).
    fn read_i32(&mut self) -> Option<i32> {
        let b = self.read_chunk(4)?;
        Some(i32::from_ne_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// `read_chunk_s(fpin, &PgStat_Kind)` — a `PgStat_Kind` (`uint32`, native LE).
    fn read_kind(&mut self) -> Option<PgStat_Kind> {
        let b = self.read_chunk(4)?;
        Some(PgStat_Kind(u32::from_ne_bytes([b[0], b[1], b[2], b[3]])))
    }

    /// `read_chunk_s(fpin, &PgStat_HashKey)` — the 16-byte key (kind u32, dboid
    /// u32, objid u64), native LE — exactly the `#[repr(C)]` image C writes.
    fn read_hashkey(&mut self) -> Option<PgStat_HashKey> {
        let b = self.read_chunk(16)?;
        let kind = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
        let dboid = u32::from_ne_bytes([b[4], b[5], b[6], b[7]]);
        let objid = u64::from_ne_bytes([
            b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
        ]);
        Some(PgStat_HashKey {
            kind: PgStat_Kind(kind),
            dboid,
            objid,
        })
    }

    /// `fgetc(fpin)` — next byte, or `None` at EOF.
    fn getc(&mut self) -> Option<u8> {
        let c = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(c)
    }

    /// `fseek(fpin, off, SEEK_CUR)` — skip `len` bytes; `false` past EOF.
    fn skip(&mut self, len: usize) -> bool {
        match self.pos.checked_add(len) {
            Some(end) if end <= self.buf.len() => {
                self.pos = end;
                true
            }
            _ => false,
        }
    }

    /// Whether the cursor is at end-of-file (`fgetc(fpin) == EOF`).
    fn at_eof(&self) -> bool {
        self.pos >= self.buf.len()
    }
}

/// `pgstat_get_kind_info` + `pgstat_is_kind_valid` shorthand used by the
/// read/write driver: the per-kind descriptor for a valid, registered kind.
fn lookup_valid_kind(
    kind: PgStat_Kind,
) -> Option<&'static crate::kind_info::PgStat_KindInfoFull> {
    // pgstat_is_kind_valid: builtin range (custom kinds unported); plus a
    // registered descriptor (pgstat_get_kind_info != NULL).
    if !kind.is_builtin() {
        return None;
    }
    registry::pgstat_get_kind_info(kind)
}

/// `pgstat_read_statsfile()` (`pgstat.c`) — read the permanent on-disk stats file
/// into shared memory at server start. Called only by the startup process / in
/// single-user mode, so no locking is required.
///
/// When the file does not exist (`ENOENT`) statistics start from scratch:
/// `pgstat_reset_after_failure()` resets the fixed kinds and returns. Otherwise
/// the file image is decoded into shared memory: a `format_id` header followed by
/// a stream of `PGSTAT_FILE_ENTRY_{FIXED,HASH,NAME}` entries terminated by
/// `PGSTAT_FILE_ENTRY_END`. A fixed entry's `shared_data_len` bytes are deposited
/// into the kind's typed `PgStat_ShmemControl` field through its `read_fixed_cb`;
/// a variable entry creates a fresh shared hash entry and deposits the bytes into
/// its body through `read_var_cb`. Any short read / unknown kind / bad tag /
/// duplicate is a corrupt file: C logs and falls back to
/// `pgstat_reset_after_failure()`, which this port preserves.
fn pgstat_read_statsfile() -> PgResult<()> {
    let statfile = PGSTAT_STAT_PERMANENT_FILENAME;

    // Try to open the stats file. `allocate_file_read` returns `None` for ENOENT
    // (start from scratch); any other open failure is also non-fatal (C
    // ereport(LOG) + pgstat_reset_after_failure).
    let bytes = match backend_storage_file_fd_seams::allocate_file_read::call(statfile) {
        Ok(Some(b)) => b,
        Ok(None) => return pgstat_reset_after_failure(),
        Err(_) => return pgstat_reset_after_failure(),
    };

    match decode_statsfile(&bytes) {
        Ok(()) => {
            // done: C unlinks the permanent file after a successful read so a
            // subsequent crash recovers from scratch rather than stale stats.
            let _ = backend_storage_file_fd_seams::unlink_file::call(statfile);
            Ok(())
        }
        Err(DecodeError) => {
            // error: corrupted statistics file — reset and start fresh, then
            // still unlink (C's `goto done` runs the unlink on the error path).
            pgstat_reset_after_failure()?;
            let _ = backend_storage_file_fd_seams::unlink_file::call(statfile);
            Ok(())
        }
    }
}

/// Sentinel for the C `goto error` path (corrupt file → reset). Carries no
/// detail because C only logs a generic "corrupted statistics file" message.
struct DecodeError;

/// The decode loop of [`pgstat_read_statsfile`]. Returns `Err(DecodeError)` for
/// any corruption (C's `goto error`), `Ok(())` on a clean
/// `PGSTAT_FILE_ENTRY_END`.
fn decode_statsfile(bytes: &[u8]) -> Result<(), DecodeError> {
    let mut cur = ReadCursor::new(bytes);

    // Verify it's of the expected format.
    let format_id = cur.read_i32().ok_or(DecodeError)?;
    if format_id != PGSTAT_FILE_FORMAT_ID {
        // "found incorrect format ID" — corrupt.
        return Err(DecodeError);
    }

    loop {
        let t = match cur.getc() {
            Some(t) => t,
            // C reads a tag with fgetc; EOF here is an unexpected tag.
            None => return Err(DecodeError),
        };

        match t {
            PGSTAT_FILE_ENTRY_FIXED => {
                // entry for fixed-numbered stats
                let kind = cur.read_kind().ok_or(DecodeError)?;
                let info = lookup_valid_kind(kind).ok_or(DecodeError)?;
                if !info.info.fixed_amount {
                    return Err(DecodeError);
                }
                let len = info.info.shared_data_len as usize;
                let data = cur.read_chunk(len).ok_or(DecodeError)?;
                let cb = info.cb.read_fixed_cb.as_ref().ok_or(DecodeError)?;
                local::with_local(|l| -> Result<(), DecodeError> {
                    let ctl = l
                        .shmem
                        .as_deref_mut()
                        .expect("pgstat_read_statsfile: shared control not initialized");
                    cb(ctl, data).map_err(|_| DecodeError)
                })?;
            }
            PGSTAT_FILE_ENTRY_HASH | PGSTAT_FILE_ENTRY_NAME => {
                let key = if t == PGSTAT_FILE_ENTRY_HASH {
                    // normal stats entry, identified by PgStat_HashKey
                    let key = cur.read_hashkey().ok_or(DecodeError)?;
                    let _info = lookup_valid_kind(key.kind).ok_or(DecodeError)?;
                    key
                } else {
                    // stats entry identified by name on disk (e.g. slots)
                    let kind = cur.read_kind().ok_or(DecodeError)?;
                    let name = cur.read_chunk(NAMEDATALEN).ok_or(DecodeError)?;
                    let info = lookup_valid_kind(kind).ok_or(DecodeError)?;
                    let from_name =
                        info.cb.from_serialized_name.as_ref().ok_or(DecodeError)?;
                    let name_str = namedata_to_str(name);
                    match from_name(&name_str) {
                        Some(key) => {
                            debug_assert_eq!(key.kind, kind);
                            key
                        }
                        None => {
                            // skip over data for an entry we don't care about
                            let len = info.info.shared_data_len as usize;
                            if !cur.skip(len) {
                                return Err(DecodeError);
                            }
                            continue;
                        }
                    }
                };

                let info = lookup_valid_kind(key.kind).ok_or(DecodeError)?;
                let len = info.info.shared_data_len as usize;

                // Create a fresh shared entry (no duplicates allowed). This
                // intentionally bypasses the backend-local entry-ref hash.
                let header = match shmem::pgstat_restore_create_entry(key) {
                    Ok(Some(h)) => h,
                    // duplicate stats entry — corrupt.
                    Ok(None) => return Err(DecodeError),
                    // C: ERROR "could not allocate entry" — propagate as decode
                    // failure (the reset fallback discards everything).
                    Err(_) => return Err(DecodeError),
                };

                let data = cur.read_chunk(len).ok_or(DecodeError)?;
                let read_var = info.cb.read_var_cb.as_ref().ok_or(DecodeError)?;
                read_var(header, data).map_err(|_| DecodeError)?;
            }
            PGSTAT_FILE_ENTRY_END => {
                // check that PGSTAT_FILE_ENTRY_END actually signals end of file
                if !cur.at_eof() {
                    return Err(DecodeError);
                }
                return Ok(());
            }
            _ => return Err(DecodeError),
        }
    }
}

/// Decode a fixed `NameData` byte buffer into a `&str` up to the first NUL,
/// mirroring C's `NameStr(name)` view of the on-disk `NameData`.
fn namedata_to_str(name: &[u8]) -> alloc::string::String {
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    alloc::string::String::from_utf8_lossy(&name[..end]).into_owned()
}

/// Encode a name into a fixed `NAMEDATALEN`-byte `NameData` buffer (NUL-padded),
/// mirroring C's `namestrcpy(&name, ...)` before `write_chunk_s(fpout, &name)`.
fn str_to_namedata(name: &str) -> [u8; NAMEDATALEN] {
    let mut buf = [0u8; NAMEDATALEN];
    let src = name.as_bytes();
    let n = src.len().min(NAMEDATALEN - 1);
    buf[..n].copy_from_slice(&src[..n]);
    buf
}

/// `pgstat_write_statsfile()` (`pgstat.c`) — write the current shared statistics
/// out to the permanent on-disk file at a clean shutdown. Called in the last
/// process accessing shared stats (checkpointer / single-user), so no locking is
/// required.
///
/// Builds the file image — `format_id` header, every fixed kind's snapshot bytes
/// (`PGSTAT_FILE_ENTRY_FIXED`), every live variable entry's body bytes
/// (`PGSTAT_FILE_ENTRY_HASH` / `_NAME`), `PGSTAT_FILE_ENTRY_END` — then writes the
/// temp file and durably renames it over the permanent file.
fn pgstat_write_statsfile() -> PgResult<()> {
    let mut out: alloc::vec::Vec<u8> = alloc::vec::Vec::new();

    // Write the file header --- currently just a format ID.
    out.extend_from_slice(&PGSTAT_FILE_FORMAT_ID.to_ne_bytes());

    // Write various stats structs for fixed number of objects.
    let mut kind = PGSTAT_KIND_MIN.0;
    while kind <= PGSTAT_KIND_MAX.0 {
        let k = PgStat_Kind(kind);
        kind += 1;
        let info = match lookup_valid_kind(k) {
            Some(i) => i,
            None => continue,
        };
        if !info.info.fixed_amount || !info.info.write_to_file {
            continue;
        }
        // pgstat_build_snapshot_fixed(kind), then serialize the snapshot field.
        pgstat_build_snapshot_fixed(k)?;
        let write_fixed = info
            .cb
            .write_fixed_cb
            .as_ref()
            .expect("pgstat_write_statsfile: fixed kind missing write_fixed_cb");
        let data = local::with_local(|l| write_fixed(&l.snapshot));
        debug_assert_eq!(data.len(), info.info.shared_data_len as usize);

        out.push(PGSTAT_FILE_ENTRY_FIXED);
        out.extend_from_slice(&k.0.to_ne_bytes());
        out.extend_from_slice(&data);
    }

    // Walk through the variable stats entries.
    shmem::pgstat_for_each_entry(|key, body| -> PgResult<()> {
        let info = match lookup_valid_kind(key.kind) {
            Some(i) => i,
            // discards unknown stats kinds (C elog(WARNING) + continue).
            None => return Ok(()),
        };
        if !info.info.write_to_file {
            return Ok(());
        }

        if let Some(to_name) = info.cb.to_serialized_name.as_ref() {
            // stats entry identified by name on disk (e.g. slots)
            // SAFETY: body is a live PgStatShared_Common header of this kind.
            let name = unsafe { to_name(&key, &*body) };
            out.push(PGSTAT_FILE_ENTRY_NAME);
            out.extend_from_slice(&key.kind.0.to_ne_bytes());
            out.extend_from_slice(&str_to_namedata(&name));
        } else {
            // normal stats entry, identified by PgStat_HashKey
            out.push(PGSTAT_FILE_ENTRY_HASH);
            out.extend_from_slice(&key.kind.0.to_ne_bytes());
            out.extend_from_slice(&key.dboid.to_ne_bytes());
            out.extend_from_slice(&key.objid.to_ne_bytes());
        }

        // Write except the header part of the entry.
        let write_var = info
            .cb
            .write_var_cb
            .as_ref()
            .expect("pgstat_write_statsfile: variable kind missing write_var_cb");
        let data = write_var(body as *const PgStatShared_Common);
        debug_assert_eq!(data.len(), info.info.shared_data_len as usize);
        out.extend_from_slice(&data);
        Ok(())
    })?;

    out.push(PGSTAT_FILE_ENTRY_END);

    // C writes a temp file then `durable_rename`s it over the permanent file for
    // atomicity. The file-fd seam set has no generic durable-rename, so write the
    // permanent file directly (single-user / last-process context, where there is
    // no concurrent reader to observe a partial file). A write failure is
    // non-fatal (C ereport(LOG) + unlink(tmpfile)).
    let _ = backend_storage_file_fd_seams::allocate_file_write::call(
        PGSTAT_STAT_PERMANENT_FILENAME,
        &out,
    );
    Ok(())
}

/// `pgstat_restore_stats()` (`pgstat.c`) — read on-disk stats into memory at
/// server start (clean shutdown path). Called only by the startup process / in
/// single-user mode.
pub fn pgstat_restore_stats() -> PgResult<()> {
    pgstat_read_statsfile()
}

/// `pgstat_discard_stats()` (`pgstat.c`) — remove the permanent stats file. Used
/// only when WAL recovery is needed after a crash. Unlinks the file (a missing
/// file is fine) then resets all stats contents.
pub fn pgstat_discard_stats() -> PgResult<()> {
    // NB: this needs to be done even in single user mode.
    let ret = backend_storage_file_fd_seams::unlink_file::call(PGSTAT_STAT_PERMANENT_FILENAME);
    // C distinguishes ENOENT (DEBUG2 "didn't need to unlink") from other errno
    // (ereport LOG); both are non-fatal and merely logged, so a failed unlink
    // does not affect the reset that follows.
    let _ = ret;

    // Reset stats contents. This sets reset timestamps of fixed-numbered stats
    // to the current time (no variable stats exist yet at startup).
    pgstat_reset_after_failure()
}

// ---------------------------------------------------------------------------
// lifecycle: initialize / shutdown.
// ---------------------------------------------------------------------------

/// `pgstat_initialize()` (`pgstat.c`) — initialize this backend's cumulative
/// statistics state: attach the shared stats system, run each kind's
/// `init_backend_cb`, register the before-shutdown flush, and mark the backend
/// as initialized.
pub fn pgstat_initialize() -> PgResult<()> {
    if local::is_initialized() {
        return Ok(());
    }

    shmem::pgstat_attach_shmem()?;

    // Run each registered kind's per-backend init callback.
    let cbs: Vec<_> = registry::kind_table()
        .iter()
        .filter_map(|(_, ki)| ki.cb.init_backend_cb.as_ref().map(|cb| cb as *const _))
        .collect();
    for cb_ptr in cbs {
        // SAFETY: the closure lives in the sealed 'static kind table.
        let cb: &Box<dyn Fn() -> PgResult<()> + Send + Sync> =
            unsafe { &*(cb_ptr as *const _) };
        cb()?;
    }

    // before_shmem_exit(pgstat_before_server_shutdown, 0).
    backend_storage_ipc_dsm_core_seams::before_shmem_exit::call(
        before_shutdown_trampoline,
        types_tuple::Datum::null(),
    )?;

    local::set_initialized(true);
    Ok(())
}

/// The `before_shmem_exit`-registered trampoline (C `pgstat_before_server_shutdown`).
fn before_shutdown_trampoline(
    code: i32,
    arg: types_tuple::Datum<'static>,
) -> PgResult<()> {
    pgstat_before_server_shutdown(code, arg)
}

/// `pgstat_before_server_shutdown(code, arg)` (`pgstat.c`) — the
/// before_shmem_exit callback: do a final forced flush of pending statistics,
/// then mark the subsystem shut down.
pub fn pgstat_before_server_shutdown(code: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    if local::is_shutdown() {
        return Ok(());
    }
    // flush out our own pending changes before writing out
    pgstat_report_stat(true)?;
    local::set_shutdown(true);

    // Only write out file during normal shutdown (code == 0). Don't even signal
    // that we've shutdown during irregular shutdowns, because the shutdown
    // sequence isn't coordinated to ensure this backend shuts down last.
    if code == 0 {
        local::with_local(|l| {
            if let Some(ctl) = l.shmem.as_deref_mut() {
                ctl.is_shutdown = true;
            }
        });
        pgstat_write_statsfile()?;
    }
    Ok(())
}
