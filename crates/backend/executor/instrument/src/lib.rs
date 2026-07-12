// instrument.c (the parallel accum path carries WalUsage::default()).

use types_core::instrument::{
    instr_time, BufferUsage, Instrumentation, WalUsage, INSTRUMENT_BUFFERS, INSTRUMENT_TIMER,
    INSTRUMENT_WAL,
};

#[cfg(test)]
mod tests;

// INSTR_TIME_SET_CURRENT (instr_time.h): monotonic ns ticks; +1 keeps a live
// reading distinct from the zero "not started" sentinel, cancels in diffs.
pub fn instr_time_current() -> instr_time {
    use std::sync::OnceLock;
    use std::time::Instant;
    static ANCHOR: OnceLock<Instant> = OnceLock::new();
    let anchor = *ANCHOR.get_or_init(Instant::now);
    instr_time { ticks: anchor.elapsed().as_nanos() as i64 + 1 }
}

// pgBufferUsage (instrument.c): shared/local blks tick in bufmgr::counters
// and temp_blks_* in fd::buffile; the track_io_timing blk_*_time clocks have
// no ported writers (nor an installed GUC backing), so their running totals
// are truly zero. WORKER_CONTRIB is InstrAccumParallelQuery's add — the live
// counters cannot be bumped, so the accumulated worker usage rides as an
// overlay.
pub fn pg_buffer_usage() -> BufferUsage {
    let mut u = BufferUsage {
        shared_blks_hit: bufmgr::counters::shared_blks_hit() as i64,
        shared_blks_read: bufmgr::counters::shared_blks_read() as i64,
        shared_blks_dirtied: bufmgr::counters::shared_blks_dirtied() as i64,
        shared_blks_written: bufmgr::counters::shared_blks_written() as i64,
        local_blks_hit: bufmgr::counters::local_blks_hit() as i64,
        local_blks_read: bufmgr::counters::local_blks_read() as i64,
        local_blks_dirtied: bufmgr::counters::local_blks_dirtied() as i64,
        local_blks_written: bufmgr::counters::local_blks_written() as i64,
        temp_blks_read: fd::buffile::temp_blks_read(),
        temp_blks_written: fd::buffile::temp_blks_written(),
        ..BufferUsage::default()
    };
    WORKER_CONTRIB.with(|c| buffer_usage_add(&mut u, &c.get()));
    u
}

const ZERO_USAGE: BufferUsage = BufferUsage {
    shared_blks_hit: 0,
    shared_blks_read: 0,
    shared_blks_dirtied: 0,
    shared_blks_written: 0,
    local_blks_hit: 0,
    local_blks_read: 0,
    local_blks_dirtied: 0,
    local_blks_written: 0,
    temp_blks_read: 0,
    temp_blks_written: 0,
    shared_blk_read_time: instr_time { ticks: 0 },
    shared_blk_write_time: instr_time { ticks: 0 },
    local_blk_read_time: instr_time { ticks: 0 },
    local_blk_write_time: instr_time { ticks: 0 },
    temp_blk_read_time: instr_time { ticks: 0 },
    temp_blk_write_time: instr_time { ticks: 0 },
};

std::thread_local! {
    static WORKER_CONTRIB: std::cell::Cell<BufferUsage> =
        const { std::cell::Cell::new(ZERO_USAGE) };
}

/// `InstrStartParallelQuery` (worker side): snapshot the running totals.
pub fn instr_start_parallel_query() -> BufferUsage {
    pg_buffer_usage()
}

/// `InstrEndParallelQuery` (worker side): this run's usage.
pub fn instr_end_parallel_query(save: &BufferUsage) -> BufferUsage {
    let mut out = BufferUsage::default();
    buffer_usage_accum_diff(&mut out, &pg_buffer_usage(), save);
    out
}

/// `InstrAccumParallelQuery` (leader side): fold one worker's usage into the
/// leader's totals.
pub fn instr_accum_parallel_query(bufusage: &BufferUsage) {
    WORKER_CONTRIB.with(|c| {
        let mut cur = c.get();
        buffer_usage_add(&mut cur, bufusage);
        c.set(cur);
    });
}

/// `pgWalUsage` read (instrument.h global; owned by xloginsert).
pub fn pg_wal_usage() -> WalUsage {
    transam_xlog_seams::wal_usage::call()
}

/// `InstrInit`.
pub fn instr_init(instr: &mut Instrumentation, instrument_options: i32) {
    *instr = Instrumentation::default();
    instr.need_bufusage = instrument_options & INSTRUMENT_BUFFERS != 0;
    instr.need_timer = instrument_options & INSTRUMENT_TIMER != 0;
    instr.need_walusage = instrument_options & INSTRUMENT_WAL != 0;
}

/// `InstrStartNode`.
pub fn instr_start_node(instr: &mut Instrumentation) {
    if instr.need_timer {
        if !instr.starttime.is_zero() {
            panic!("InstrStartNode called twice in a row");
        }
        instr.starttime = instr_time_current();
    }
    if instr.need_bufusage {
        instr.bufusage_start = pg_buffer_usage();
    }
    if instr.need_walusage {
        instr.walusage_start = transam_xlog_seams::wal_usage::call();
    }
}

/// `InstrStopNode`.
pub fn instr_stop_node(instr: &mut Instrumentation, n_tuples: f64) {
    let save_tuplecount = instr.tuplecount;
    instr.tuplecount += n_tuples;

    if instr.need_timer {
        if instr.starttime.is_zero() {
            panic!("InstrStopNode called without start");
        }
        let endtime = instr_time_current();
        instr.counter.accum_diff(endtime, instr.starttime);
        instr.starttime.set_zero();
    }

    if instr.need_bufusage {
        let current = pg_buffer_usage();
        buffer_usage_accum_diff(&mut instr.bufusage, &current, &instr.bufusage_start);
    }

    if instr.need_walusage {
        let current = transam_xlog_seams::wal_usage::call();
        wal_usage_accum_diff(&mut instr.walusage, &current, &instr.walusage_start);
    }

    if !instr.running {
        instr.running = true;
        instr.firsttuple = instr.counter.get_double();
    } else if instr.async_mode && save_tuplecount < 1.0 {
        instr.firsttuple = instr.counter.get_double();
    }
}

pub fn instr_update_tuple_count(instr: &mut Instrumentation, n_tuples: f64) {
    instr.tuplecount += n_tuples;
}

/// `InstrEndLoop`.
pub fn instr_end_loop(instr: &mut Instrumentation) {
    if !instr.running {
        return;
    }
    if !instr.starttime.is_zero() {
        panic!("InstrEndLoop called on running node");
    }
    let totaltime = instr.counter.get_double();

    instr.startup += instr.firsttuple;
    instr.total += totaltime;
    instr.ntuples += instr.tuplecount;
    instr.nloops += 1.0;

    instr.running = false;
    instr.starttime.set_zero();
    instr.counter.set_zero();
    instr.firsttuple = 0.0;
    instr.tuplecount = 0.0;
}

pub fn instr_agg_node(dst: &mut Instrumentation, add: &Instrumentation) {
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

    if dst.need_bufusage {
        buffer_usage_add(&mut dst.bufusage, &add.bufusage);
    }
    if dst.need_walusage {
        wal_usage_add(&mut dst.walusage, &add.walusage);
    }
}

pub fn buffer_usage_add(dst: &mut BufferUsage, add: &BufferUsage) {
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

pub fn buffer_usage_accum_diff(dst: &mut BufferUsage, add: &BufferUsage, sub: &BufferUsage) {
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
    dst.shared_blk_read_time.accum_diff(add.shared_blk_read_time, sub.shared_blk_read_time);
    dst.shared_blk_write_time.accum_diff(add.shared_blk_write_time, sub.shared_blk_write_time);
    dst.local_blk_read_time.accum_diff(add.local_blk_read_time, sub.local_blk_read_time);
    dst.local_blk_write_time.accum_diff(add.local_blk_write_time, sub.local_blk_write_time);
    dst.temp_blk_read_time.accum_diff(add.temp_blk_read_time, sub.temp_blk_read_time);
    dst.temp_blk_write_time.accum_diff(add.temp_blk_write_time, sub.temp_blk_write_time);
}

pub fn wal_usage_add(dst: &mut WalUsage, add: &WalUsage) {
    dst.wal_bytes = dst.wal_bytes.wrapping_add(add.wal_bytes);
    dst.wal_records += add.wal_records;
    dst.wal_fpi += add.wal_fpi;
    dst.wal_buffers_full += add.wal_buffers_full;
}

pub fn wal_usage_accum_diff(dst: &mut WalUsage, add: &WalUsage, sub: &WalUsage) {
    dst.wal_bytes = dst.wal_bytes.wrapping_add(add.wal_bytes.wrapping_sub(sub.wal_bytes));
    dst.wal_records += add.wal_records - sub.wal_records;
    dst.wal_fpi += add.wal_fpi - sub.wal_fpi;
    dst.wal_buffers_full += add.wal_buffers_full - sub.wal_buffers_full;
}
