//! Port of `src/backend/utils/activity/pgstat_function.c` (PostgreSQL 18.3).
//!
//! Implementation of function statistics (`PGSTAT_KIND_FUNCTION`, a
//! variable-numbered stats kind). Kept separate from `pgstat.c` to enforce the
//! line between the statistics access/storage implementation and the details
//! about individual kinds of statistics.
//!
//! `total_func_time` (the backend-wide total function time, a per-backend C
//! static) is a `thread_local!` here. `PGSTAT_KIND_FUNCTION`'s
//! `flush_pending_cb` is registered with the pgstat core's
//! `pgstat_kind_builtin_infos[]` table via [`KindInfoBuilder`] from
//! [`init_seams`]; the variable-numbered entry's pending block is the kind's
//! `void *pending` (`PgStat_FunctionCounts`), modeled as the core
//! `PgStat_EntryRef`'s `Box<dyn Any>`. The shared body is
//! `PgStatShared_Function` (cast from the header pointer the core resolves,
//! exactly C's `(PgStatShared_Function *) entry_ref->shared_stats`).
//!
//! C's `fcu->fs` is a `PgStat_FunctionCounts *` into the entry-ref's pending
//! block; that owner-private block can't be carried as a raw pointer here, so
//! `pgstat_end_function_usage` re-resolves it from `(MyDatabaseId, proid)`
//! (`PgStat_FunctionCallUsage.proid`).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod fmgr_builtins;

use core::cell::Cell;

use activity_pgstat::entry_ref::PgStat_EntryRef;
use activity_pgstat::kind_info::KindInfoBuilder;
use activity_pgstat::pgstat_core;
use activity_pgstat::registry;
use activity_pgstat::shmem;
use activity_xact as xact;
use utils_error::ereport;
use init_small_seams::my_database_id;
use ::instr_time::instr_time_set_current;
use types_core::instrument::instr_time;
use types_core::primitive::Oid;
use types_error::error::{ERRCODE_UNDEFINED_FUNCTION, ERROR};
use types_error::PgResult;
use types_pgstat::activity_pgstat::{
    PgStat_Counter, PgStat_FunctionCallUsage, PgStat_FunctionCounts, PgStat_StatFuncEntry,
    PGSTAT_KIND_FUNCTION,
};
use types_pgstat::pgstat_internal::{PgStat_KindInfo, PgStatShared_Function};

thread_local! {
    /// `static instr_time total_func_time;` — total time charged to functions
    /// so far in this backend, used to separate "self" and "other" time charges.
    static TOTAL_FUNC_TIME: Cell<instr_time> = const { Cell::new(instr_time { ticks: 0 }) };
}

// ---------------------------------------------------------------------------
// Transactional create / drop.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_create_function(Oid proid)` — ensure stats are dropped
/// if the transaction aborts.
pub fn pgstat_create_function(proid: Oid) -> PgResult<()> {
    xact::pgstat_create_transactional(PGSTAT_KIND_FUNCTION, my_database_id::call(), proid as u64)
}

/// Port of `void pgstat_drop_function(Oid proid)` — ensure stats are dropped if
/// the transaction commits.
///
/// NB: This is only reliable because `pgstat_init_function_usage` does some
/// extra work.
pub fn pgstat_drop_function(proid: Oid) -> PgResult<()> {
    xact::pgstat_drop_transactional(PGSTAT_KIND_FUNCTION, my_database_id::call(), proid as u64)
}

// ---------------------------------------------------------------------------
// Call-usage tracking (executor-facing).
// ---------------------------------------------------------------------------

/// Port of `void pgstat_init_function_usage(FunctionCallInfo fcinfo,
/// PgStat_FunctionCallUsage *fcu)`. Called by the executor before invoking a
/// function.
///
/// `fn_stats` / `fn_oid` are C's `fcinfo->flinfo->fn_stats` /
/// `fcinfo->flinfo->fn_oid`. Returns the initialized `PgStat_FunctionCallUsage`
/// (C fills the caller's stack value).
pub fn pgstat_init_function_usage(fn_stats: u8, fn_oid: Oid) -> PgResult<PgStat_FunctionCallUsage> {
    let mut fcu = PgStat_FunctionCallUsage::default();

    if pgstat_track_functions() <= (fn_stats as i32) {
        // stats not wanted
        fcu.tracking = false;
        return Ok(fcu);
    }

    let mut created_entry = false;
    let entry_ref = pgstat_core::pgstat_prep_pending_entry_created(
        PGSTAT_KIND_FUNCTION,
        my_database_id::call(),
        fn_oid as u64,
        Some(&mut created_entry),
        || alloc::boxed::Box::new(PgStat_FunctionCounts::default()),
    )?;

    // If no shared entry already existed, check whether the function was deleted
    // concurrently. Executing a statement that just calls a function does not
    // trigger cache-invalidation processing, so this can go unnoticed until
    // here; otherwise we could create a new stats entry for an already-dropped
    // function.
    if created_entry {
        inval_seams::accept_invalidation_messages::call()?;
        if !syscache_seams::procoid_exists::call(fn_oid)? {
            shmem::pgstat_drop_entry(PGSTAT_KIND_FUNCTION, my_database_id::call(), fn_oid as u64)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                .errmsg("function call to dropped function")
                .into_error());
        }
    }

    // pending = entry_ref->pending; fcu->fs = pending;
    // SAFETY: a just-prepared live reference whose pending block is a
    // PgStat_FunctionCounts.
    let er = unsafe { entry_ref.get() };
    let pending = er
        .pending
        .as_ref()
        .expect("pgstat_init_function_usage: pending block missing")
        .downcast_ref::<PgStat_FunctionCounts>()
        .expect("pgstat_init_function_usage: pending is not a PgStat_FunctionCounts");

    fcu.tracking = true;
    fcu.proid = fn_oid;

    // save stats for this function, later used to compensate for recursion
    fcu.save_f_total_time = pending.total_time;
    // save current backend-wide total time
    fcu.save_total = TOTAL_FUNC_TIME.with(|c| c.get());
    // get clock time as of function start
    instr_time_set_current(&mut fcu.start);

    Ok(fcu)
}

/// Port of `void pgstat_end_function_usage(PgStat_FunctionCallUsage *fcu, bool
/// finalize)`. Called by the executor after invoking a function.
///
/// For a set-returning function in value-per-call mode there are multiple
/// init/end pairs for what the user sees as one call; `finalize` is true on the
/// last one.
pub fn pgstat_end_function_usage(fcu: &mut PgStat_FunctionCallUsage, finalize: bool) -> PgResult<()> {
    // stats not wanted? (C: fs == NULL)
    if !fcu.tracking {
        return Ok(());
    }

    // total elapsed time in this function call
    let mut total = instr_time::default();
    instr_time_set_current(&mut total);
    total.subtract(fcu.start);

    // self usage: elapsed minus anything already charged to other calls
    let mut others = TOTAL_FUNC_TIME.with(|c| c.get());
    others.subtract(fcu.save_total);
    let mut self_time = total;
    self_time.subtract(others);

    // update backend-wide total time
    TOTAL_FUNC_TIME.with(|c| {
        let mut t = c.get();
        t.add(self_time);
        c.set(t);
    });

    // Compute the new total_time as the total elapsed time added to the pre-call
    // value of total_time. This avoids double-counting time taken by recursive
    // calls of myself. (Self time already excludes recursive calls.)
    total.add(fcu.save_f_total_time);

    // Re-resolve the pending PgStat_FunctionCounts entry C reached via fcu->fs.
    let entry_ref = pgstat_core::pgstat_fetch_pending_entry(
        PGSTAT_KIND_FUNCTION,
        my_database_id::call(),
        fcu.proid as u64,
    )?
    .expect(
        "pgstat_end_function_usage: pending function-counts entry vanished between \
         init and end (C holds it via fcu->fs)",
    );

    // SAFETY: live reference with a PgStat_FunctionCounts pending block.
    let er = unsafe { entry_ref.get() };
    let fs = er
        .pending
        .as_mut()
        .expect("pgstat_end_function_usage: pending block missing")
        .downcast_mut::<PgStat_FunctionCounts>()
        .expect("pgstat_end_function_usage: pending is not a PgStat_FunctionCounts");

    // update counters in function stats table
    if finalize {
        fs.numcalls += 1;
    }
    fs.total_time = total;
    fs.self_time.add(self_time);

    Ok(())
}

// ---------------------------------------------------------------------------
// Flush + fetch.
// ---------------------------------------------------------------------------

/// Port of `bool pgstat_function_flush_cb(PgStat_EntryRef *entry_ref, bool
/// nowait)` — flush a function entry's pending data into shared memory.
///
/// Returns `Ok(false)` if `nowait` and the lock could not be acquired, else
/// `Ok(true)` after flushing.
pub fn pgstat_function_flush_cb(entry_ref: &mut PgStat_EntryRef, nowait: bool) -> PgResult<bool> {
    let shfuncent = entry_ref.shared_stats as *mut PgStatShared_Function;

    // localent always has non-zero content. Copy it out so its borrow does not
    // collide with the lock/unlock calls' &entry_ref borrow.
    let localent: PgStat_FunctionCounts = *entry_ref
        .pending
        .as_ref()
        .expect("pgstat_function_flush_cb: entry has no pending block")
        .downcast_ref::<PgStat_FunctionCounts>()
        .expect("pgstat_function_flush_cb: pending is not a PgStat_FunctionCounts");

    if !shmem::pgstat_lock_entry(entry_ref, nowait)? {
        return Ok(false);
    }

    // SAFETY: shared body live + locked.
    let shared = unsafe { &mut (*shfuncent).stats };
    shared.numcalls += localent.numcalls;
    shared.total_time += localent.total_time.get_microsec() as PgStat_Counter;
    shared.self_time += localent.self_time.get_microsec() as PgStat_Counter;

    shmem::pgstat_unlock_entry(entry_ref)?;

    Ok(true)
}

/// Port of `PgStat_FunctionCounts *find_funcstat_entry(Oid func_id)` — find any
/// existing pending entry for the function; `None` if none (does not create).
pub fn find_funcstat_entry(func_id: Oid) -> PgResult<Option<PgStat_FunctionCounts>> {
    let entry_ref = pgstat_core::pgstat_fetch_pending_entry(
        PGSTAT_KIND_FUNCTION,
        my_database_id::call(),
        func_id as u64,
    )?;
    match entry_ref {
        None => Ok(None),
        Some(er) => {
            // SAFETY: live reference returned with a non-None pending block.
            let e = unsafe { er.get() };
            let counts = e
                .pending
                .as_ref()
                .expect("find_funcstat_entry: pending block missing")
                .downcast_ref::<PgStat_FunctionCounts>()
                .expect("find_funcstat_entry: pending is not a PgStat_FunctionCounts");
            Ok(Some(*counts))
        }
    }
}

/// Port of `PgStat_StatFuncEntry *pgstat_fetch_stat_funcentry(Oid func_id)` —
/// SQL-callable support function. Returns the collected statistics for one
/// function, or `None`.
pub fn pgstat_fetch_stat_funcentry(func_id: Oid) -> PgResult<Option<PgStat_StatFuncEntry>> {
    let bytes = pgstat_core::pgstat_fetch_entry(
        PGSTAT_KIND_FUNCTION,
        my_database_id::call(),
        func_id as u64,
    )?;
    Ok(bytes.map(|b| decode_func_entry(&b)))
}

/// Decode the `shared_data_len` bytes `pgstat_fetch_entry` copies out into the
/// typed `PgStat_StatFuncEntry` (C's `(PgStat_StatFuncEntry *) ...`).
fn decode_func_entry(bytes: &[u8]) -> PgStat_StatFuncEntry {
    assert_eq!(
        bytes.len(),
        core::mem::size_of::<PgStat_StatFuncEntry>(),
        "pgstat_fetch_stat_funcentry: unexpected stats blob size"
    );
    // SAFETY: the blob is exactly a `PgStat_StatFuncEntry` (a Copy, pointer-free
    // POD), copied byte-for-byte by pgstat_fetch_entry.
    unsafe { core::ptr::read_unaligned(bytes.as_ptr() as *const PgStat_StatFuncEntry) }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// `pgstat_track_functions` GUC (the enum maps to `TRACK_FUNC_OFF/PL/ALL`).
fn pgstat_track_functions() -> i32 {
    guc_tables::vars::pgstat_track_functions.read()
}

// ---------------------------------------------------------------------------
// Kind registration + seam installation.
// ---------------------------------------------------------------------------

/// The `PgStat_KindInfo` metadata for `PGSTAT_KIND_FUNCTION`
/// (`pgstat.c:pgstat_kind_builtin_infos[PGSTAT_KIND_FUNCTION]`).
fn function_kind_info() -> PgStat_KindInfo {
    PgStat_KindInfo {
        fixed_amount: false,
        accessed_across_databases: false,
        write_to_file: true,
        shared_size: core::mem::size_of::<PgStatShared_Function>() as u32,
        snapshot_ctl_off: 0,
        shared_ctl_off: 0,
        shared_data_off: core::mem::offset_of!(PgStatShared_Function, stats) as u32,
        shared_data_len: core::mem::size_of::<PgStat_StatFuncEntry>() as u32,
        pending_size: core::mem::size_of::<PgStat_FunctionCounts>() as u32,
        name: "function",
    }
}

/// Register `PGSTAT_KIND_FUNCTION` and install the pgstat_function.c outward
/// seams.
///
/// Must run before `activity_pgstat::init_seams()` seals the
/// per-kind table.
pub fn init_seams() {
    // Register the per-function pg_stat_get_function_* SQL accessors (pgstatfuncs.c).
    fmgr_builtins::register_pgstat_function_builtins();

    registry::register(
        KindInfoBuilder::new(PGSTAT_KIND_FUNCTION, function_kind_info())
            .flush_pending_cb(pgstat_function_flush_cb)
            // On-disk (de)serialization of the `PgStat_StatFuncEntry` body.
            .read_var_cb(|header, bytes| {
                // SAFETY: header points at a live PgStatShared_Function body.
                let sh = unsafe { &mut *(header as *mut PgStatShared_Function) };
                sh.stats = activity_pgstat::kind_info::pgstat_deserialize_pod::<
                    PgStat_StatFuncEntry,
                >(bytes);
                Ok(())
            })
            .write_var_cb(|header| {
                // SAFETY: header points at a live PgStatShared_Function body.
                let sh = unsafe { &*(header as *const PgStatShared_Function) };
                activity_pgstat::kind_info::pgstat_serialize_pod(&sh.stats)
            }),
    );

    // pgstat_function.c outward seam with a live caller (pg_proc.c).
    pg_proc_seams::pgstat_create_function::set(pgstat_create_function);

    // Function-usage tracking seams, called by the executor's FUSAGE function
    // opcodes (execExprInterp.c) and the set-returning-function path.
    pgstat_function_seams::pgstat_init_function_usage::set(
        pgstat_init_function_usage,
    );
    pgstat_function_seams::pgstat_end_function_usage::set(
        pgstat_end_function_usage,
    );
}
