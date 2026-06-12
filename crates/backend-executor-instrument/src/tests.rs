use super::*;
use mcx::MemoryContext;
use types_core::instrument::{instr_time, INSTRUMENT_ALL, INSTRUMENT_ROWS, NS_PER_S};

#[test]
fn instr_alloc_initializes_flags() {
    let ctx = MemoryContext::new("test");
    let array = InstrAlloc(ctx.mcx(), 2, INSTRUMENT_TIMER | INSTRUMENT_BUFFERS, true).unwrap();

    assert_eq!(array.len(), 2);
    for instr in array.iter() {
        assert!(instr.need_timer);
        assert!(instr.need_bufusage);
        assert!(!instr.need_walusage);
        assert!(instr.async_mode);
        assert!(!instr.running);
    }
}

#[test]
fn instr_alloc_no_active_option_leaves_flags_zeroed() {
    let ctx = MemoryContext::new("test");
    // Only INSTRUMENT_ROWS set -> the option gate is false, so even
    // async_mode stays zeroed, faithful to the C `if (...)` guard.
    let array = InstrAlloc(ctx.mcx(), 3, INSTRUMENT_ROWS, true).unwrap();
    assert_eq!(array.len(), 3);
    for instr in array.iter() {
        assert!(!instr.need_timer);
        assert!(!instr.need_bufusage);
        assert!(!instr.need_walusage);
        assert!(!instr.async_mode);
    }
}

#[test]
fn instr_alloc_zero_is_empty() {
    let ctx = MemoryContext::new("test");
    let array = InstrAlloc(ctx.mcx(), 0, INSTRUMENT_ALL, false).unwrap();
    assert!(array.is_empty());
}

#[test]
fn instr_alloc_oversize_is_recoverable_error() {
    let ctx = MemoryContext::new("test");
    let err = InstrAlloc(ctx.mcx(), i32::MAX, 0, false).unwrap_err();
    assert!(err.message().starts_with("invalid memory alloc request size"));
    let err = InstrAlloc(ctx.mcx(), -1, 0, false).unwrap_err();
    assert!(err.message().starts_with("invalid memory alloc request size"));
}

#[test]
fn instr_init_sets_only_need_flags() {
    let mut instr = Instrumentation {
        async_mode: true,
        running: true,
        tuplecount: 9.0,
        ..Instrumentation::default()
    };
    InstrInit(&mut instr, INSTRUMENT_TIMER | INSTRUMENT_WAL);
    assert!(instr.need_timer);
    assert!(instr.need_walusage);
    assert!(!instr.need_bufusage);
    // memset(0) wiped everything else, including async_mode.
    assert!(!instr.async_mode);
    assert!(!instr.running);
    assert_eq!(instr.tuplecount, 0.0);
}

#[test]
fn instr_start_stop_and_end_loop_accumulates_tuple_stats() {
    let mut instr = Instrumentation::default();
    InstrInit(&mut instr, INSTRUMENT_TIMER);

    InstrStartNode(&mut instr).unwrap();
    assert_ne!(instr.starttime.ticks, 0);
    InstrStopNode(&mut instr, 3.0).unwrap();
    assert!(instr.running);
    assert_eq!(instr.tuplecount, 3.0);
    assert!(instr.counter.ticks >= 0);
    // starttime reset to zero after stop.
    assert_eq!(instr.starttime.ticks, 0);
    assert_eq!(instr.firsttuple, instr.counter.ticks as f64 / NS_PER_S as f64);

    let counter = instr.counter;
    InstrEndLoop(&mut instr).unwrap();
    assert!(!instr.running);
    assert_eq!(instr.ntuples, 3.0);
    assert_eq!(instr.nloops, 1.0);
    assert_eq!(instr.tuplecount, 0.0);
    assert_eq!(instr.total, counter.ticks as f64 / NS_PER_S as f64);
    assert_eq!(instr.counter.ticks, 0);
}

#[test]
fn instr_error_messages_match_postgres() {
    let mut instr = Instrumentation::default();
    InstrInit(&mut instr, INSTRUMENT_TIMER);

    // Stop without start: starttime is zero.
    let error = InstrStopNode(&mut instr, 1.0).unwrap_err();
    assert_eq!(error.message(), "InstrStopNode called without start");

    InstrStartNode(&mut instr).unwrap();
    // Second start without stop: starttime nonzero -> lazy set returns false.
    let error = InstrStartNode(&mut instr).unwrap_err();
    assert_eq!(error.message(), "InstrStartNode called twice in a row");
}

#[test]
fn instr_end_loop_on_running_node_errors() {
    let mut instr = Instrumentation::default();
    InstrInit(&mut instr, INSTRUMENT_TIMER);
    // First cycle: start + stop sets `running` and zeroes `starttime`.
    InstrStartNode(&mut instr).unwrap();
    InstrStopNode(&mut instr, 1.0).unwrap();
    assert!(instr.running);
    // Re-enter the node without ending the loop: `starttime` becomes nonzero
    // again while `running` is still true — `InstrEndLoop` on a running node.
    InstrStartNode(&mut instr).unwrap();
    let err = InstrEndLoop(&mut instr).unwrap_err();
    assert_eq!(err.message(), "InstrEndLoop called on running node");
}

#[test]
fn instr_end_loop_not_running_is_noop() {
    let mut instr = Instrumentation::default();
    InstrEndLoop(&mut instr).unwrap();
    assert_eq!(instr.nloops, 0.0);
}

#[test]
fn async_mode_refreshes_firsttuple_until_first_tuple_emitted() {
    let mut instr = Instrumentation {
        need_timer: false,
        async_mode: true,
        ..Instrumentation::default()
    };
    // First stop with no tuples: running becomes true, firsttuple recorded.
    instr.counter = instr_time { ticks: 100 };
    InstrStopNode(&mut instr, 0.0).unwrap();
    assert!(instr.running);
    let first = instr.firsttuple;
    assert_eq!(first, 100.0 / NS_PER_S as f64);
    // Still no tuple emitted (save_tuplecount < 1): firsttuple refreshed.
    instr.counter = instr_time { ticks: 200 };
    InstrStopNode(&mut instr, 1.0).unwrap();
    assert_eq!(instr.firsttuple, 200.0 / NS_PER_S as f64);
    // Tuples already emitted: firsttuple stays put.
    instr.counter = instr_time { ticks: 300 };
    InstrStopNode(&mut instr, 1.0).unwrap();
    assert_eq!(instr.firsttuple, 200.0 / NS_PER_S as f64);
}

#[test]
fn instr_update_tuple_count_adds() {
    let mut instr = Instrumentation::default();
    InstrUpdateTupleCount(&mut instr, 4.0);
    InstrUpdateTupleCount(&mut instr, 2.5);
    assert_eq!(instr.tuplecount, 6.5);
}

#[test]
fn instr_agg_node_folds_stats_and_firsttuple() {
    let mut dst = Instrumentation {
        need_bufusage: true,
        need_walusage: true,
        ..Instrumentation::default()
    };
    let add = Instrumentation {
        running: true,
        firsttuple: 1.25,
        counter: instr_time { ticks: 50 },
        tuplecount: 2.0,
        startup: 0.5,
        total: 1.5,
        ntuples: 7.0,
        ntuples2: 1.0,
        nloops: 1.0,
        nfiltered1: 3.0,
        nfiltered2: 4.0,
        bufusage: BufferUsage {
            shared_blks_hit: 11,
            ..BufferUsage::default()
        },
        walusage: WalUsage {
            wal_records: 9,
            ..WalUsage::default()
        },
        ..Instrumentation::default()
    };

    InstrAggNode(&mut dst, &add);
    assert!(dst.running);
    assert_eq!(dst.firsttuple, 1.25);
    assert_eq!(dst.counter.ticks, 50);
    assert_eq!(dst.tuplecount, 2.0);
    assert_eq!(dst.ntuples, 7.0);
    assert_eq!(dst.ntuples2, 1.0);
    assert_eq!(dst.nfiltered1, 3.0);
    assert_eq!(dst.nfiltered2, 4.0);
    assert_eq!(dst.bufusage.shared_blks_hit, 11);
    assert_eq!(dst.walusage.wal_records, 9);
}

#[test]
fn instr_agg_node_keeps_min_firsttuple_when_both_running() {
    let mut dst = Instrumentation {
        running: true,
        firsttuple: 5.0,
        ..Instrumentation::default()
    };
    let add = Instrumentation {
        running: true,
        firsttuple: 2.0,
        ..Instrumentation::default()
    };
    InstrAggNode(&mut dst, &add);
    assert_eq!(dst.firsttuple, 2.0);

    // The reverse: dst already smaller, keep it.
    let mut dst2 = Instrumentation {
        running: true,
        firsttuple: 1.0,
        ..Instrumentation::default()
    };
    let add2 = Instrumentation {
        running: true,
        firsttuple: 9.0,
        ..Instrumentation::default()
    };
    InstrAggNode(&mut dst2, &add2);
    assert_eq!(dst2.firsttuple, 1.0);
}

#[test]
fn buffer_usage_accum_diff_matches_postgres_arithmetic() {
    let add = BufferUsage {
        shared_blks_hit: 10,
        temp_blks_written: 7,
        shared_blk_read_time: instr_time { ticks: 100 },
        ..BufferUsage::default()
    };
    let sub = BufferUsage {
        shared_blks_hit: 3,
        temp_blks_written: 2,
        shared_blk_read_time: instr_time { ticks: 40 },
        ..BufferUsage::default()
    };
    let mut dst = BufferUsage::default();

    BufferUsageAccumDiff(&mut dst, &add, &sub);

    assert_eq!(dst.shared_blks_hit, 7);
    assert_eq!(dst.temp_blks_written, 5);
    assert_eq!(dst.shared_blk_read_time.ticks, 60);
}

#[test]
fn wal_usage_accum_diff_uses_wrapping_bytes() {
    let mut dst = WalUsage::default();
    let add = WalUsage {
        wal_records: 5,
        wal_fpi: 2,
        wal_bytes: 1000,
        wal_buffers_full: 1,
    };
    let sub = WalUsage {
        wal_records: 1,
        wal_fpi: 0,
        wal_bytes: 200,
        wal_buffers_full: 0,
    };
    WalUsageAccumDiff(&mut dst, &add, &sub);
    assert_eq!(dst.wal_records, 4);
    assert_eq!(dst.wal_fpi, 2);
    assert_eq!(dst.wal_bytes, 800);
    assert_eq!(dst.wal_buffers_full, 1);
}

#[test]
fn parallel_query_accumulates_thread_local_usage() {
    set_pgBufferUsage(BufferUsage::default());
    set_pgWalUsage(WalUsage::default());
    let snapshot = InstrStartParallelQuery();

    with_pgBufferUsage(|usage| usage.shared_blks_read = 5);
    with_pgWalUsage(|usage| {
        usage.wal_records = 2;
        usage.wal_bytes = 10;
    });

    let mut buf = BufferUsage::default();
    let mut wal = WalUsage::default();
    InstrEndParallelQuery(snapshot, &mut buf, &mut wal);

    assert_eq!(buf.shared_blks_read, 5);
    assert_eq!(wal.wal_records, 2);
    assert_eq!(wal.wal_bytes, 10);

    // Workers' totals fold into the leader's globals.
    InstrAccumParallelQuery(&buf, &wal);
    assert_eq!(pgBufferUsage().shared_blks_read, 10);
    assert_eq!(pgWalUsage().wal_records, 4);
}

#[test]
fn start_stop_records_buffer_and_wal_deltas() {
    set_pgBufferUsage(BufferUsage::default());
    set_pgWalUsage(WalUsage::default());
    let mut instr = Instrumentation::default();
    InstrInit(&mut instr, INSTRUMENT_BUFFERS | INSTRUMENT_WAL);

    // entry snapshot
    with_pgBufferUsage(|u| u.shared_blks_read = 100);
    with_pgWalUsage(|u| u.wal_bytes = 1000);
    InstrStartNode(&mut instr).unwrap();

    // work happens, globals advance
    with_pgBufferUsage(|u| u.shared_blks_read = 130);
    with_pgWalUsage(|u| u.wal_bytes = 1500);
    InstrStopNode(&mut instr, 1.0).unwrap();

    assert_eq!(instr.bufusage.shared_blks_read, 30);
    assert_eq!(instr.walusage.wal_bytes, 500);
}

#[test]
fn with_accessor_reentrancy_panics_instead_of_clobbering() {
    let result = std::panic::catch_unwind(|| {
        with_pgBufferUsage(|_| {
            pgBufferUsage();
        })
    });
    assert!(result.is_err());
}
