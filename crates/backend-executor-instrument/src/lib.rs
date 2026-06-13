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
    backend_executor_instrument_seams::instr_end_loop::set(InstrEndLoop);
    backend_executor_instrument_seams::instr_update_tuple_count::set(InstrUpdateTupleCount);
}

#[cfg(test)]
mod tests;
