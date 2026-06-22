//! Port of `backend/executor/instrument.c` — run-time statistics collection
//! for plan-node execution (the data behind `EXPLAIN ANALYZE`,
//! `pg_stat_statements`, and parallel-query usage rollups).
//!
//! C exposes `pgBufferUsage` / `pgWalUsage` as process globals. Those are
//! per-backend session state, so here they are thread-locals reached through
//! accessors (`RefCell` so a reentrant in-place update fails loudly instead
//! of silently clobbering counts). C's file-local `save_pgBufferUsage` /
//! `save_pgWalUsage` are parallel-query-lifecycle state, not backend-lifetime
//! state: here `InstrStartParallelQuery` returns the snapshot as an owned
//! [`ParallelQueryUsageSnapshot`] that `InstrEndParallelQuery` consumes.
//!
//! `instr_time` arithmetic lives on the type in `types_core::instrument`; the
//! monotonic-clock read (`INSTR_TIME_SET_CURRENT*`) comes from
//! `portability_instr_time`.

#![allow(non_snake_case)]

use std::cell::RefCell;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use portability_instr_time::{instr_time_set_current_lazy, pg_clock_gettime_ns};
use types_core::instrument::{
    BufferUsage, Instrumentation, InstrumentOption, WalUsage, INSTRUMENT_BUFFERS,
    INSTRUMENT_TIMER, INSTRUMENT_WAL,
};
use types_error::{PgError, PgResult};

thread_local! {
    /// `BufferUsage pgBufferUsage;` — running, never-reset buffer-access
    /// counters for the current backend.
    static PG_BUFFER_USAGE: RefCell<BufferUsage> = RefCell::new(BufferUsage::default());
    /// `WalUsage pgWalUsage;` — running, never-reset WAL-generation counters.
    static PG_WAL_USAGE: RefCell<WalUsage> = RefCell::new(WalUsage::default());
}

/// Read the backend-global `pgBufferUsage` counter.
pub fn pgBufferUsage() -> BufferUsage {
    PG_BUFFER_USAGE.with(|cell| *cell.borrow())
}

/// Overwrite the backend-global `pgBufferUsage` counter.
pub fn set_pgBufferUsage(usage: BufferUsage) {
    PG_BUFFER_USAGE.with(|cell| *cell.borrow_mut() = usage);
}

/// Mutate the backend-global `pgBufferUsage` counter in place. Panics on
/// reentrant use (the closure must not touch `pgBufferUsage` itself).
pub fn with_pgBufferUsage<R>(f: impl FnOnce(&mut BufferUsage) -> R) -> R {
    PG_BUFFER_USAGE.with(|cell| f(&mut cell.borrow_mut()))
}

/// Read the backend-global `pgWalUsage` counter.
pub fn pgWalUsage() -> WalUsage {
    PG_WAL_USAGE.with(|cell| *cell.borrow())
}

/// Overwrite the backend-global `pgWalUsage` counter.
pub fn set_pgWalUsage(usage: WalUsage) {
    PG_WAL_USAGE.with(|cell| *cell.borrow_mut() = usage);
}

/// Mutate the backend-global `pgWalUsage` counter in place. Panics on
/// reentrant use (the closure must not touch `pgWalUsage` itself).
pub fn with_pgWalUsage<R>(f: impl FnOnce(&mut WalUsage) -> R) -> R {
    PG_WAL_USAGE.with(|cell| f(&mut cell.borrow_mut()))
}

/// Allocate new instrumentation structure(s) (`InstrAlloc`).
///
/// C: `palloc0(n * sizeof(Instrumentation))` in the caller's current context,
/// then fill in the `need_*` / `async_mode` flags when any of the
/// timer/buffers/WAL bits are requested.
pub fn InstrAlloc<'mcx>(
    mcx: Mcx<'mcx>,
    n: i32,
    instrument_options: InstrumentOption,
    async_mode: bool,
) -> PgResult<PgVec<'mcx, Instrumentation>> {
    // C computes `(size_t) n * sizeof(Instrumentation)`: a negative `n`
    // sign-extends to a huge request and palloc's MaxAllocSize gate (now in
    // `mcx::vec_with_capacity_in`) rejects it.
    let n = n as isize as usize;

    // initialize all fields to zeroes, then modify as needed
    let mut instr: PgVec<'mcx, Instrumentation> = vec_with_capacity_in(mcx, n)?;
    instr.resize(n, Instrumentation::default());

    if instrument_options & (INSTRUMENT_BUFFERS | INSTRUMENT_TIMER | INSTRUMENT_WAL) != 0 {
        let need_buffers = instrument_options & INSTRUMENT_BUFFERS != 0;
        let need_wal = instrument_options & INSTRUMENT_WAL != 0;
        let need_timer = instrument_options & INSTRUMENT_TIMER != 0;

        for one in instr.iter_mut() {
            one.need_bufusage = need_buffers;
            one.need_walusage = need_wal;
            one.need_timer = need_timer;
            one.async_mode = async_mode;
        }
    }

    Ok(instr)
}

/// Initialize a pre-allocated instrumentation structure (`InstrInit`).
///
/// memset to zero, then set the three `need_*` flags from the option bits.
/// Exactly like C, `async_mode` is left at its zeroed value.
pub fn InstrInit(instr: &mut Instrumentation, instrument_options: InstrumentOption) {
    *instr = Instrumentation::default();
    instr.need_bufusage = instrument_options & INSTRUMENT_BUFFERS != 0;
    instr.need_walusage = instrument_options & INSTRUMENT_WAL != 0;
    instr.need_timer = instrument_options & INSTRUMENT_TIMER != 0;
}

/// Entry to a plan node (`InstrStartNode`).
pub fn InstrStartNode(instr: &mut Instrumentation) -> PgResult<()> {
    if instr.need_timer && !instr_time_set_current_lazy(&mut instr.starttime) {
        return Err(PgError::error("InstrStartNode called twice in a row"));
    }

    // save buffer usage totals at node entry, if needed
    if instr.need_bufusage {
        instr.bufusage_start = pgBufferUsage();
    }

    if instr.need_walusage {
        instr.walusage_start = pgWalUsage();
    }

    Ok(())
}

/// Exit from a plan node (`InstrStopNode`).
pub fn InstrStopNode(instr: &mut Instrumentation, n_tuples: f64) -> PgResult<()> {
    let save_tuplecount = instr.tuplecount;

    // count the returned tuples
    instr.tuplecount += n_tuples;

    // let's update the time only if the timer was requested
    if instr.need_timer {
        if instr.starttime.is_zero() {
            return Err(PgError::error("InstrStopNode called without start"));
        }

        let endtime = pg_clock_gettime_ns();
        instr.counter.accum_diff(endtime, instr.starttime);

        instr.starttime.set_zero();
    }

    // Add delta of buffer usage since entry to node's totals
    if instr.need_bufusage {
        let now = pgBufferUsage();
        BufferUsageAccumDiff(&mut instr.bufusage, &now, &instr.bufusage_start);
    }

    if instr.need_walusage {
        let now = pgWalUsage();
        WalUsageAccumDiff(&mut instr.walusage, &now, &instr.walusage_start);
    }

    // Is this the first tuple of this cycle?
    if !instr.running {
        instr.running = true;
        instr.firsttuple = instr.counter.get_double();
    } else if instr.async_mode && save_tuplecount < 1.0 {
        // In async mode, if the plan node hadn't emitted any tuples before,
        // this might be the first tuple
        instr.firsttuple = instr.counter.get_double();
    }

    Ok(())
}

/// Update tuple count (`InstrUpdateTupleCount`).
pub fn InstrUpdateTupleCount(instr: &mut Instrumentation, n_tuples: f64) {
    // count the returned tuples
    instr.tuplecount += n_tuples;
}

/// Finish a run cycle for a plan node (`InstrEndLoop`).
pub fn InstrEndLoop(instr: &mut Instrumentation) -> PgResult<()> {
    // Skip if nothing has happened, or already shut down
    if !instr.running {
        return Ok(());
    }

    if !instr.starttime.is_zero() {
        return Err(PgError::error("InstrEndLoop called on running node"));
    }

    // Accumulate per-cycle statistics into totals
    let totaltime = instr.counter.get_double();

    instr.startup += instr.firsttuple;
    instr.total += totaltime;
    instr.ntuples += instr.tuplecount;
    instr.nloops += 1.0;

    // Reset for next cycle (if any)
    instr.running = false;
    instr.starttime.set_zero();
    instr.counter.set_zero();
    instr.firsttuple = 0.0;
    instr.tuplecount = 0.0;

    Ok(())
}

/// Aggregate instrumentation information (`InstrAggNode`): fold `add` into
/// `dst`.
pub fn InstrAggNode(dst: &mut Instrumentation, add: &Instrumentation) {
    if !dst.running && add.running {
        dst.running = true;
        dst.firsttuple = add.firsttuple;
    } else if dst.running && add.running && dst.firsttuple > add.firsttuple {
        dst.firsttuple = add.firsttuple;
    }

    dst.counter.add(add.counter);

    dst.tuplecount += add.tuplecount;
    dst.startup += add.startup;
    dst.total += add.total;
    dst.ntuples += add.ntuples;
    dst.ntuples2 += add.ntuples2;
    dst.nloops += add.nloops;
    dst.nfiltered1 += add.nfiltered1;
    dst.nfiltered2 += add.nfiltered2;

    // Add delta of buffer usage since entry to node's totals
    if dst.need_bufusage {
        BufferUsageAdd(&mut dst.bufusage, &add.bufusage);
    }

    if dst.need_walusage {
        WalUsageAdd(&mut dst.walusage, &add.walusage);
    }
}

/// The `pgBufferUsage` / `pgWalUsage` values noted at parallel executor
/// startup (C: file-local `save_pgBufferUsage` / `save_pgWalUsage`). This is
/// parallel-query-lifecycle state, not backend state: the executor holds it
/// from `InstrStartParallelQuery` until `InstrEndParallelQuery` consumes it.
#[must_use]
#[derive(Clone, Copy, Debug)]
pub struct ParallelQueryUsageSnapshot {
    buf: BufferUsage,
    wal: WalUsage,
}

/// Note current values during parallel executor startup
/// (`InstrStartParallelQuery`).
pub fn InstrStartParallelQuery() -> ParallelQueryUsageSnapshot {
    ParallelQueryUsageSnapshot {
        buf: pgBufferUsage(),
        wal: pgWalUsage(),
    }
}

/// Report usage after parallel executor shutdown (`InstrEndParallelQuery`):
/// the deltas since `snapshot` was taken at `InstrStartParallelQuery`.
pub fn InstrEndParallelQuery(
    snapshot: ParallelQueryUsageSnapshot,
    bufusage: &mut BufferUsage,
    walusage: &mut WalUsage,
) {
    *bufusage = BufferUsage::default();
    let now_buf = pgBufferUsage();
    BufferUsageAccumDiff(bufusage, &now_buf, &snapshot.buf);

    *walusage = WalUsage::default();
    let now_wal = pgWalUsage();
    WalUsageAccumDiff(walusage, &now_wal, &snapshot.wal);
}

/// Accumulate work done by workers in the leader's stats
/// (`InstrAccumParallelQuery`).
pub fn InstrAccumParallelQuery(bufusage: &BufferUsage, walusage: &WalUsage) {
    with_pgBufferUsage(|global| BufferUsageAdd(global, bufusage));
    with_pgWalUsage(|global| WalUsageAdd(global, walusage));
}

// ===========================================================================
// `execParallel-support` surface: the parallel executor calls the
// `Instr*ParallelQuery` family in C's file-static-global form (the snapshot is
// NOT threaded through the call chain; instead `InstrStartParallelQuery` stashes
// `pgBufferUsage`/`pgWalUsage` into `save_pgBufferUsage`/`save_pgWalUsage`).
// These shapes mirror that contract over a raw DSM cursor (the per-worker
// `BufferUsage`/`WalUsage`/`Instrumentation` arrays the leader laid out in the
// segment as plain `sizeof`-strided POD).
// ===========================================================================

thread_local! {
    /// `static BufferUsage save_pgBufferUsage;` (instrument.c) — noted at
    /// `InstrStartParallelQuery`.
    static SAVE_PG_BUFFER_USAGE: RefCell<BufferUsage> = RefCell::new(BufferUsage::default());
    /// `static WalUsage save_pgWalUsage;` (instrument.c).
    static SAVE_PG_WAL_USAGE: RefCell<WalUsage> = RefCell::new(WalUsage::default());
}

/// `InstrStartParallelQuery()` in the file-static-global form: save the running
/// counters (C: `save_pgBufferUsage = pgBufferUsage; save_pgWalUsage = pgWalUsage`).
pub fn instr_start_parallel_query_global() {
    SAVE_PG_BUFFER_USAGE.with(|c| *c.borrow_mut() = pgBufferUsage());
    SAVE_PG_WAL_USAGE.with(|c| *c.borrow_mut() = pgWalUsage());
}

/// `InstrEndParallelQuery(&bufusage[worker], &walusage[worker])` — compute the
/// deltas since `InstrStartParallelQuery` and write them into the worker's DSM
/// `BufferUsage`/`WalUsage` slot. `buf_base`/`wal_base` are the array bases the
/// leader allocated (`sizeof`-strided POD); `worker` indexes into them.
///
/// SAFETY: `buf_base`/`wal_base` address `BufferUsage`/`WalUsage` arrays in the
/// mapped DSM segment with at least `worker + 1` elements.
pub unsafe fn instr_end_parallel_query_slot(buf_base: usize, wal_base: usize, worker: i32) {
    let save_buf = SAVE_PG_BUFFER_USAGE.with(|c| *c.borrow());
    let save_wal = SAVE_PG_WAL_USAGE.with(|c| *c.borrow());

    let mut bufusage = BufferUsage::default();
    BufferUsageAccumDiff(&mut bufusage, &pgBufferUsage(), &save_buf);
    let mut walusage = WalUsage::default();
    WalUsageAccumDiff(&mut walusage, &pgWalUsage(), &save_wal);

    let buf_slot = (buf_base as *mut BufferUsage).add(worker as usize);
    let wal_slot = (wal_base as *mut WalUsage).add(worker as usize);
    core::ptr::write(buf_slot, bufusage);
    core::ptr::write(wal_slot, walusage);
}

/// `InstrAccumParallelQuery(&bufusage[worker], &walusage[worker])` — add a
/// worker's DSM slot into the leader's running `pgBufferUsage`/`pgWalUsage`.
///
/// SAFETY: as `instr_end_parallel_query_slot`.
pub unsafe fn instr_accum_parallel_query_slot(buf_base: usize, wal_base: usize, worker: i32) {
    let bufusage = core::ptr::read((buf_base as *const BufferUsage).add(worker as usize));
    let walusage = core::ptr::read((wal_base as *const WalUsage).add(worker as usize));
    InstrAccumParallelQuery(&bufusage, &walusage);
}

/// `InstrInit(&GetInstrumentationArray(sei)[i], instrument_options)` — initialize
/// the `i`th `Instrumentation` slot in the leader's DSM
/// `SharedExecutorInstrumentation` array. `array_base` is the address of
/// `GetInstrumentationArray(sei)` (the `Instrumentation[]` right after the fixed
/// header); the slots are plain `sizeof(Instrumentation)`-strided POD.
///
/// SAFETY: `array_base` addresses an `Instrumentation` array in the mapped DSM
/// segment with at least `i + 1` elements (the leader reserved
/// `num_workers * num_plan_nodes` of them).
pub unsafe fn instr_init_slot_at(array_base: usize, i: i32, instrument_options: i32) {
    let slot = (array_base as *mut Instrumentation).add(i as usize);
    // The leader's `palloc0`'d DSM chunk leaves the slot zeroed; `InstrInit`
    // mutates a live struct, so start from a zeroed value then init in place.
    core::ptr::write(slot, Instrumentation::default());
    InstrInit(&mut *slot, instrument_options as InstrumentOption);
}

/// `InstrAggNode(&GetInstrumentationArray(sei)[slot], add)` — fold the worker's
/// `Instrumentation` `add` into the `slot`th DSM `Instrumentation` object in the
/// leader's `SharedExecutorInstrumentation` array. `array_base` is the address
/// of `GetInstrumentationArray(sei)`; the slots are plain
/// `sizeof(Instrumentation)`-strided POD. This is the worker-side
/// `ExecParallelReportInstrumentation` per-node write.
///
/// SAFETY: `array_base` addresses an `Instrumentation` array in the mapped DSM
/// segment with at least `slot + 1` elements.
pub unsafe fn instr_agg_node_to_slot_at(array_base: usize, slot: i32, add: &Instrumentation) {
    let p = (array_base as *mut Instrumentation).add(slot as usize);
    // The DSM slot is an unaligned in-place struct; read it out, fold, write
    // back. (`read_unaligned`/`write_unaligned` matches the rest of the DSM
    // header access in the parallel crate.)
    let mut dst = core::ptr::read_unaligned(p);
    InstrAggNode(&mut dst, add);
    core::ptr::write_unaligned(p, dst);
}

/// `GetInstrumentationArray(sei)[slot]` — read the `slot`th DSM
/// `Instrumentation` object out of the leader's `SharedExecutorInstrumentation`
/// array. This is the leader-side `ExecParallelRetrieveInstrumentation`
/// per-worker read.
///
/// SAFETY: as `instr_agg_node_to_slot_at`.
pub unsafe fn instr_read_slot_at(array_base: usize, slot: i32) -> Instrumentation {
    let p = (array_base as *const Instrumentation).add(slot as usize);
    core::ptr::read_unaligned(p)
}

/// `dst += add` (`static BufferUsageAdd`).
fn BufferUsageAdd(dst: &mut BufferUsage, add: &BufferUsage) {
    dst.shared_blks_hit += add.shared_blks_hit;
    dst.shared_blks_read += add.shared_blks_read;
    dst.shared_blks_dirtied += add.shared_blks_dirtied;
    dst.shared_blks_written += add.shared_blks_written;
    dst.local_blks_hit += add.local_blks_hit;
    dst.local_blks_read += add.local_blks_read;
    dst.local_blks_dirtied += add.local_blks_dirtied;
    dst.local_blks_written += add.local_blks_written;
    dst.temp_blks_read += add.temp_blks_read;
    dst.temp_blks_written += add.temp_blks_written;
    dst.shared_blk_read_time.add(add.shared_blk_read_time);
    dst.shared_blk_write_time.add(add.shared_blk_write_time);
    dst.local_blk_read_time.add(add.local_blk_read_time);
    dst.local_blk_write_time.add(add.local_blk_write_time);
    dst.temp_blk_read_time.add(add.temp_blk_read_time);
    dst.temp_blk_write_time.add(add.temp_blk_write_time);
}

/// `dst += add - sub` (`BufferUsageAccumDiff`).
pub fn BufferUsageAccumDiff(dst: &mut BufferUsage, add: &BufferUsage, sub: &BufferUsage) {
    dst.shared_blks_hit += add.shared_blks_hit - sub.shared_blks_hit;
    dst.shared_blks_read += add.shared_blks_read - sub.shared_blks_read;
    dst.shared_blks_dirtied += add.shared_blks_dirtied - sub.shared_blks_dirtied;
    dst.shared_blks_written += add.shared_blks_written - sub.shared_blks_written;
    dst.local_blks_hit += add.local_blks_hit - sub.local_blks_hit;
    dst.local_blks_read += add.local_blks_read - sub.local_blks_read;
    dst.local_blks_dirtied += add.local_blks_dirtied - sub.local_blks_dirtied;
    dst.local_blks_written += add.local_blks_written - sub.local_blks_written;
    dst.temp_blks_read += add.temp_blks_read - sub.temp_blks_read;
    dst.temp_blks_written += add.temp_blks_written - sub.temp_blks_written;
    dst.shared_blk_read_time
        .accum_diff(add.shared_blk_read_time, sub.shared_blk_read_time);
    dst.shared_blk_write_time
        .accum_diff(add.shared_blk_write_time, sub.shared_blk_write_time);
    dst.local_blk_read_time
        .accum_diff(add.local_blk_read_time, sub.local_blk_read_time);
    dst.local_blk_write_time
        .accum_diff(add.local_blk_write_time, sub.local_blk_write_time);
    dst.temp_blk_read_time
        .accum_diff(add.temp_blk_read_time, sub.temp_blk_read_time);
    dst.temp_blk_write_time
        .accum_diff(add.temp_blk_write_time, sub.temp_blk_write_time);
}

/// `dst += add` (`static WalUsageAdd`).
fn WalUsageAdd(dst: &mut WalUsage, add: &WalUsage) {
    // `wal_bytes` is `uint64`; C arithmetic on it is unsigned (modular).
    dst.wal_bytes = dst.wal_bytes.wrapping_add(add.wal_bytes);
    dst.wal_records += add.wal_records;
    dst.wal_fpi += add.wal_fpi;
    dst.wal_buffers_full += add.wal_buffers_full;
}

/// `dst += add - sub` (`WalUsageAccumDiff`).
pub fn WalUsageAccumDiff(dst: &mut WalUsage, add: &WalUsage, sub: &WalUsage) {
    // `wal_bytes` is `uint64`; C arithmetic on it is unsigned (modular).
    dst.wal_bytes = dst
        .wal_bytes
        .wrapping_add(add.wal_bytes.wrapping_sub(sub.wal_bytes));
    dst.wal_records += add.wal_records - sub.wal_records;
    dst.wal_fpi += add.wal_fpi - sub.wal_fpi;
    dst.wal_buffers_full += add.wal_buffers_full - sub.wal_buffers_full;
}

/// Install this unit's outward seams.
pub fn init_seams() {
    backend_executor_instrument_seams::instr_alloc::set(InstrAlloc);
    backend_executor_instrument_seams::instr_start_node::set(InstrStartNode);
    backend_executor_instrument_seams::instr_stop_node::set(InstrStopNode);
    backend_executor_instrument_seams::instr_end_loop::set(InstrEndLoop);
    backend_executor_instrument_seams::instr_update_tuple_count::set(InstrUpdateTupleCount);
    backend_executor_instrument_seams::instr_agg_node::set(|dst, add| InstrAggNode(dst, &add));

    // --- lazy-vacuum driver's pgBufferUsage / pgWalUsage snapshot reads
    //     (vacuumlazy.c BufferUsageAccumDiff / WalUsageAccumDiff logging). These
    //     are this file's backend-global counters; the read seams home in
    //     vacuumlazy-seams. The tuples mirror the fields the driver diffs. ---
    backend_access_heap_vacuumlazy_seams::pg_buffer_usage::set(|| {
        let u = pgBufferUsage();
        Ok((
            u.shared_blks_hit,
            u.shared_blks_read,
            u.shared_blks_dirtied,
            u.local_blks_hit,
            u.local_blks_read,
            u.local_blks_dirtied,
        ))
    });
    backend_access_heap_vacuumlazy_seams::pg_wal_usage::set(|| {
        let w = pgWalUsage();
        Ok((w.wal_records, w.wal_fpi, w.wal_bytes, w.wal_buffers_full))
    });

    install_execparallel_support_instr_seams();
}

/// Install the `Instr*ParallelQuery` family on the parallel executor's
/// `execParallel-support` surface, in C's file-static-global form (the snapshot
/// is stashed in `save_pgBufferUsage`/`save_pgWalUsage`, not threaded). The
/// `buffer_usage`/`wal_usage` cursors are the leader's per-worker
/// `BufferUsage`/`WalUsage` DSM arrays. `instr_init_slot` (which needs the
/// `SharedExecutorInstrumentation` `instrument_offset`) is installed by the
/// parallel crate, which owns that DSM header.
fn install_execparallel_support_instr_seams() {
    use backend_executor_execParallel_support_seams as sup;
    sup::instr_start_parallel_query::set(instr_start_parallel_query_global);
    sup::instr_end_parallel_query::set(|buffer_usage, wal_usage, worker| {
        // SAFETY: the cursors are the DSM `BufferUsage`/`WalUsage` array bases the
        // leader sized for `num_workers` elements; `worker < num_workers`.
        unsafe { instr_end_parallel_query_slot(buffer_usage.0, wal_usage.0, worker) }
    });
    sup::instr_accum_parallel_query::set(|buffer_usage, wal_usage, worker| {
        // SAFETY: as above.
        unsafe { instr_accum_parallel_query_slot(buffer_usage.0, wal_usage.0, worker) }
    });
}

#[cfg(test)]
mod tests;
