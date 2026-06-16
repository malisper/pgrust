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
};

use crate::entry_ref::PgStat_EntryRef;
use crate::local;
use crate::registry;
use crate::shmem;

use backend_utils_adt_timestamp_seams as timestamp;

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
pub fn pgstat_report_stat(force: bool) -> PgResult<i64> {
    // pgstat_assert_is_up()-equivalent + shutdown guard.
    if local::is_shutdown() {
        return Ok(0);
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
    let entry_ref = shmem::pgstat_get_entry_ref(kind, dboid, objid, true, None)?
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

/// `pgstat_clear_snapshot()` (`pgstat.c`) — discard any materialized snapshot.
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
pub fn pgstat_before_server_shutdown(_code: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    if local::is_shutdown() {
        return Ok(());
    }
    // Final forced flush.
    pgstat_report_stat(true)?;
    local::set_shutdown(true);
    Ok(())
}
