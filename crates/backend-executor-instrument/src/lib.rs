//! Port of `backend/executor/instrument.c` — run-time statistics collection
//! for plan-node execution (the data behind `EXPLAIN ANALYZE`,
//! `pg_stat_statements`, and parallel-query usage rollups).
//!
//! C exposes `pgBufferUsage` / `pgWalUsage` as process globals (with
//! file-local `save_pgBufferUsage` / `save_pgWalUsage` snapshots). Those are
//! per-backend session state, so here they are thread-locals reached through
//! accessors.
//!
//! `INSTR_TIME_SET_CURRENT` expands to `pg_clock_gettime_ns()`, a `static
//! inline` in `portability/instr_time.h` reading the OS monotonic clock; it is
//! implemented in-crate via `libc::clock_gettime`. Everything else in the file
//! is pure arithmetic.

#![allow(non_snake_case)]

use std::cell::Cell;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::instrument::{
    instr_time, BufferUsage, Instrumentation, InstrumentOption, WalUsage, INSTRUMENT_BUFFERS,
    INSTRUMENT_TIMER, INSTRUMENT_WAL, NS_PER_S,
};
use types_error::{PgError, PgResult};

/// `MaxAllocSize` (`memutils.h`, `0x3FFFFFFF`): the cap `palloc` enforces on a
/// single request. `InstrAlloc`'s `n` is data-derived, so the C
/// `palloc0(n * sizeof(Instrumentation))` failure surface ("invalid memory
/// alloc request size") is reproduced here.
const MAX_ALLOC_SIZE: usize = 0x3FFF_FFFF;

thread_local! {
    /// `BufferUsage pgBufferUsage;` — running, never-reset buffer-access
    /// counters for the current backend.
    static PG_BUFFER_USAGE: Cell<BufferUsage> = Cell::new(BufferUsage::default());
    /// `static BufferUsage save_pgBufferUsage;` — snapshot taken at parallel
    /// query startup.
    static SAVE_PG_BUFFER_USAGE: Cell<BufferUsage> = Cell::new(BufferUsage::default());
    /// `WalUsage pgWalUsage;` — running, never-reset WAL-generation counters.
    static PG_WAL_USAGE: Cell<WalUsage> = Cell::new(WalUsage::default());
    /// `static WalUsage save_pgWalUsage;` — snapshot taken at parallel query
    /// startup.
    static SAVE_PG_WAL_USAGE: Cell<WalUsage> = Cell::new(WalUsage::default());
}

/// Read the backend-global `pgBufferUsage` counter.
pub fn pgBufferUsage() -> BufferUsage {
    PG_BUFFER_USAGE.with(Cell::get)
}

/// Overwrite the backend-global `pgBufferUsage` counter.
pub fn set_pgBufferUsage(usage: BufferUsage) {
    PG_BUFFER_USAGE.with(|cell| cell.set(usage));
}

/// Mutate the backend-global `pgBufferUsage` counter in place.
pub fn with_pgBufferUsage<R>(f: impl FnOnce(&mut BufferUsage) -> R) -> R {
    PG_BUFFER_USAGE.with(|cell| {
        let mut value = cell.get();
        let result = f(&mut value);
        cell.set(value);
        result
    })
}

/// Read the backend-global `pgWalUsage` counter.
pub fn pgWalUsage() -> WalUsage {
    PG_WAL_USAGE.with(Cell::get)
}

/// Overwrite the backend-global `pgWalUsage` counter.
pub fn set_pgWalUsage(usage: WalUsage) {
    PG_WAL_USAGE.with(|cell| cell.set(usage));
}

/// Mutate the backend-global `pgWalUsage` counter in place.
pub fn with_pgWalUsage<R>(f: impl FnOnce(&mut WalUsage) -> R) -> R {
    PG_WAL_USAGE.with(|cell| {
        let mut value = cell.get();
        let result = f(&mut value);
        cell.set(value);
        result
    })
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
    // In C, a negative or oversized `n * sizeof(...)` request is rejected by
    // palloc's MaxAllocSize gate.
    let per = core::mem::size_of::<Instrumentation>();
    if n < 0 || (n as usize) > MAX_ALLOC_SIZE / per {
        return Err(PgError::error("invalid memory alloc request size"));
    }
    let n = n as usize;

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
        if instr_time_is_zero(instr.starttime) {
            return Err(PgError::error("InstrStopNode called without start"));
        }

        let endtime = pg_clock_gettime_ns();
        instr_time_accum_diff(&mut instr.counter, endtime, instr.starttime);

        instr_time_set_zero(&mut instr.starttime);
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
        instr.firsttuple = instr_time_get_double(instr.counter);
    } else if instr.async_mode && save_tuplecount < 1.0 {
        // In async mode, if the plan node hadn't emitted any tuples before,
        // this might be the first tuple
        instr.firsttuple = instr_time_get_double(instr.counter);
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

    if !instr_time_is_zero(instr.starttime) {
        return Err(PgError::error("InstrEndLoop called on running node"));
    }

    // Accumulate per-cycle statistics into totals
    let totaltime = instr_time_get_double(instr.counter);

    instr.startup += instr.firsttuple;
    instr.total += totaltime;
    instr.ntuples += instr.tuplecount;
    instr.nloops += 1.0;

    // Reset for next cycle (if any)
    instr.running = false;
    instr_time_set_zero(&mut instr.starttime);
    instr_time_set_zero(&mut instr.counter);
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

    instr_time_add(&mut dst.counter, add.counter);

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

/// Note current values during parallel executor startup
/// (`InstrStartParallelQuery`).
pub fn InstrStartParallelQuery() {
    SAVE_PG_BUFFER_USAGE.with(|save| save.set(pgBufferUsage()));
    SAVE_PG_WAL_USAGE.with(|save| save.set(pgWalUsage()));
}

/// Report usage after parallel executor shutdown (`InstrEndParallelQuery`).
pub fn InstrEndParallelQuery(bufusage: &mut BufferUsage, walusage: &mut WalUsage) {
    *bufusage = BufferUsage::default();
    let now_buf = pgBufferUsage();
    let save_buf = SAVE_PG_BUFFER_USAGE.with(Cell::get);
    BufferUsageAccumDiff(bufusage, &now_buf, &save_buf);

    *walusage = WalUsage::default();
    let now_wal = pgWalUsage();
    let save_wal = SAVE_PG_WAL_USAGE.with(Cell::get);
    WalUsageAccumDiff(walusage, &now_wal, &save_wal);
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
    instr_time_add(&mut dst.shared_blk_read_time, add.shared_blk_read_time);
    instr_time_add(&mut dst.shared_blk_write_time, add.shared_blk_write_time);
    instr_time_add(&mut dst.local_blk_read_time, add.local_blk_read_time);
    instr_time_add(&mut dst.local_blk_write_time, add.local_blk_write_time);
    instr_time_add(&mut dst.temp_blk_read_time, add.temp_blk_read_time);
    instr_time_add(&mut dst.temp_blk_write_time, add.temp_blk_write_time);
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
    instr_time_accum_diff(
        &mut dst.shared_blk_read_time,
        add.shared_blk_read_time,
        sub.shared_blk_read_time,
    );
    instr_time_accum_diff(
        &mut dst.shared_blk_write_time,
        add.shared_blk_write_time,
        sub.shared_blk_write_time,
    );
    instr_time_accum_diff(
        &mut dst.local_blk_read_time,
        add.local_blk_read_time,
        sub.local_blk_read_time,
    );
    instr_time_accum_diff(
        &mut dst.local_blk_write_time,
        add.local_blk_write_time,
        sub.local_blk_write_time,
    );
    instr_time_accum_diff(
        &mut dst.temp_blk_read_time,
        add.temp_blk_read_time,
        sub.temp_blk_read_time,
    );
    instr_time_accum_diff(
        &mut dst.temp_blk_write_time,
        add.temp_blk_write_time,
        sub.temp_blk_write_time,
    );
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

// --- instr_time helpers (portability/instr_time.h) -------------------------

/// `pg_clock_gettime_ns()` — read `PG_INSTR_CLOCK` and convert to nanosecond
/// ticks (`tv_sec * NS_PER_S + tv_nsec`). PG picks `CLOCK_MONOTONIC_RAW` on
/// darwin (faster and higher resolution there) and `CLOCK_MONOTONIC`
/// elsewhere. Like the C inline, the (cannot-fail-for-these-args) return code
/// is ignored.
fn pg_clock_gettime_ns() -> instr_time {
    #[cfg(target_os = "macos")]
    const PG_INSTR_CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC_RAW;
    #[cfg(not(target_os = "macos"))]
    const PG_INSTR_CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC;

    let mut tmp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime fills `tmp`; a valid clock id and out pointer.
    unsafe {
        libc::clock_gettime(PG_INSTR_CLOCK, &mut tmp);
    }
    instr_time {
        ticks: (tmp.tv_sec as i64) * NS_PER_S + tmp.tv_nsec as i64,
    }
}

/// `INSTR_TIME_SET_CURRENT_LAZY(t)` — set `t` to the current time only if it
/// is zero; returns whether `t` was set.
fn instr_time_set_current_lazy(time: &mut instr_time) -> bool {
    if instr_time_is_zero(*time) {
        *time = pg_clock_gettime_ns();
        true
    } else {
        false
    }
}

/// `INSTR_TIME_SET_ZERO(t)`.
fn instr_time_set_zero(time: &mut instr_time) {
    time.ticks = 0;
}

/// `INSTR_TIME_IS_ZERO(t)`.
fn instr_time_is_zero(time: instr_time) -> bool {
    time.ticks == 0
}

/// `INSTR_TIME_ADD(x, y)` — `x += y`.
fn instr_time_add(dst: &mut instr_time, add: instr_time) {
    dst.ticks += add.ticks;
}

/// `INSTR_TIME_ACCUM_DIFF(x, y, z)` — `x += (y - z)`.
fn instr_time_accum_diff(dst: &mut instr_time, add: instr_time, sub: instr_time) {
    dst.ticks += add.ticks - sub.ticks;
}

/// `INSTR_TIME_GET_DOUBLE(t)` — ticks (nanoseconds) to seconds.
fn instr_time_get_double(time: instr_time) -> f64 {
    time.ticks as f64 / NS_PER_S as f64
}

/// This crate declares no seams; nothing to install.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
