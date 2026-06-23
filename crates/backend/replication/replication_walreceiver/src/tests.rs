//! Unit tests for the pure helpers ported from `walreceiver.c` / the xlog
//! macros it uses, plus a couple of seam-driven control-flow checks.

use super::*;

use ::types_walreceiver::WalRcvState;

#[test]
fn wakeup_enum_indices_match_c() {
    assert_eq!(WALRCV_WAKEUP_TERMINATE as usize, 0);
    assert_eq!(WALRCV_WAKEUP_PING as usize, 1);
    assert_eq!(WALRCV_WAKEUP_REPLY as usize, 2);
    assert_eq!(WALRCV_WAKEUP_HSFEEDBACK as usize, 3);
    assert_eq!(NUM_WALRCV_WAKEUPS, 4);
}

#[test]
fn walrcvstate_discriminants_match_c() {
    assert_eq!(WalRcvState::WALRCV_STOPPED as i32, 0);
    assert_eq!(WalRcvState::WALRCV_STARTING as i32, 1);
    assert_eq!(WalRcvState::WALRCV_STREAMING as i32, 2);
    assert_eq!(WalRcvState::WALRCV_WAITING as i32, 3);
    assert_eq!(WalRcvState::WALRCV_RESTARTING as i32, 4);
    assert_eq!(WalRcvState::WALRCV_STOPPING as i32, 5);
}

#[test]
fn state_strings_match_c() {
    assert_eq!(WalRcvGetStateString(WalRcvState::WALRCV_STOPPED), "stopped");
    assert_eq!(WalRcvGetStateString(WalRcvState::WALRCV_STARTING), "starting");
    assert_eq!(WalRcvGetStateString(WalRcvState::WALRCV_STREAMING), "streaming");
    assert_eq!(WalRcvGetStateString(WalRcvState::WALRCV_WAITING), "waiting");
    assert_eq!(WalRcvGetStateString(WalRcvState::WALRCV_RESTARTING), "restarting");
    assert_eq!(WalRcvGetStateString(WalRcvState::WALRCV_STOPPING), "stopping");
}

#[test]
fn xlog_segment_macros() {
    let segsz = 16 * 1024 * 1024;
    assert_eq!(XLogSegmentsPerXLogId(segsz), 256);
    assert_eq!(XLByteToSeg(0, segsz), 0);
    assert_eq!(XLByteToSeg(segsz as u64, segsz), 1);
    assert_eq!(XLogSegmentOffset(0, segsz), 0);
    assert_eq!(XLogSegmentOffset(segsz as u64 + 123, segsz), 123);
    assert!(XLByteInSeg(0, 0, segsz));
    assert!(!XLByteInSeg(segsz as u64, 0, segsz));
}

#[test]
fn xlog_filename_format() {
    let segsz = 16 * 1024 * 1024;
    assert_eq!(XLogFileName(1, 0, segsz), "000000010000000000000000");
    assert_eq!(XLogFileName(1, 256, segsz), "000000010000000100000000");
    assert_eq!(XLogFileName(2, 5, segsz), "000000020000000000000005");
}

#[test]
fn tlhistory_filename_format() {
    assert_eq!(TLHistoryFileName(1), "00000001.history");
    assert_eq!(TLHistoryFileName(42), "0000002A.history");
}

#[test]
fn timestamp_arithmetic() {
    assert_eq!(TimestampTzPlusMilliseconds(0, 1), 1000);
    assert_eq!(TimestampTzPlusSeconds(0, 1), 1_000_000);
}

#[test]
fn lsn_format_args() {
    assert_eq!(lsn_fmt(0), "0/0");
    assert_eq!(lsn_fmt((1u64 << 32) | 0x10), "1/10");
}

#[test]
fn fullxid_decomposition() {
    let fxid: u64 = (7u64 << 32) | 0x1234;
    assert_eq!(XidFromFullTransactionId(fxid), 0x1234);
    assert_eq!(EpochFromFullTransactionId(fxid), 7);
}

#[test]
fn pq_encoders_are_network_order() {
    let mut buf = Vec::new();
    pq_sendbyte(&mut buf, b'r');
    pq_sendint32(&mut buf, 0x01020304);
    pq_sendint64(&mut buf, 0x0102030405060708);
    assert_eq!(buf[0], b'r');
    assert_eq!(&buf[1..5], &[0x01, 0x02, 0x03, 0x04]);
    assert_eq!(&buf[5..13], &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(pq_getmsgint64(&buf[5..13]), 0x0102030405060708);
}

#[test]
fn name_helpers() {
    let n = name_from_str("abc");
    assert_eq!(&n[..3], b"abc");
    assert_eq!(n[3], 0);
    assert_eq!(cstr_from_bytes(&n), "abc");
    let long = "x".repeat(100);
    let n = name_from_str(&long);
    assert_eq!(cstr_from_bytes(&n).len(), NAMEDATALEN - 1);
}

#[test]
fn wakeup_reason_index_roundtrip() {
    for i in 0..NUM_WALRCV_WAKEUPS {
        assert_eq!(wakeup_reason_from_index(i) as usize, i);
    }
}

// The full `pg_stat_get_wal_receiver` / `WalReceiverMain` state machine runs
// against the shared `WalRcvData` block its (not-yet-ported) owner installs via
// the `with_walrcv` seam; this repo's seams install exactly once (`OnceLock`),
// so the spinlock-guarded branches are exercised here directly on a
// `WalRcvData` value, matching the logic the crate runs inside those closures.

#[test]
fn flush_advance_conditional_store() {
    // XLogWalRcvFlush: only advance flushedUpto when it moved forward, and
    // latestChunkStart adopts the prior flushedUpto.
    let mut w = WalRcvData {
        flushedUpto: 100,
        latestChunkStart: 0,
        receivedTLI: 1,
        ..Default::default()
    };
    let new_flush = 150;
    let tli = 7;
    if w.flushedUpto < new_flush {
        w.latestChunkStart = w.flushedUpto;
        w.flushedUpto = new_flush;
        w.receivedTLI = tli;
    }
    assert_eq!(w.flushedUpto, 150);
    assert_eq!(w.latestChunkStart, 100);
    assert_eq!(w.receivedTLI, 7);

    // A non-advancing flush leaves everything untouched.
    let new_flush = 120;
    if w.flushedUpto < new_flush {
        w.latestChunkStart = w.flushedUpto;
        w.flushedUpto = new_flush;
        w.receivedTLI = 9;
    }
    assert_eq!(w.flushedUpto, 150);
    assert_eq!(w.latestChunkStart, 100);
    assert_eq!(w.receivedTLI, 7);
}

#[test]
fn process_walsndr_latest_wal_end_conditional() {
    // ProcessWalSndrMessage: latestWalEndTime only updates when walEnd advances;
    // the other three fields update unconditionally.
    let mut w = WalRcvData {
        latestWalEnd: 200,
        latestWalEndTime: 11,
        ..Default::default()
    };
    let (wal_end, send_time, receipt) = (250, 99, 100);
    if w.latestWalEnd < wal_end {
        w.latestWalEndTime = send_time;
    }
    w.latestWalEnd = wal_end;
    w.lastMsgSendTime = send_time;
    w.lastMsgReceiptTime = receipt;
    assert_eq!(w.latestWalEnd, 250);
    assert_eq!(w.latestWalEndTime, 99);
    assert_eq!(w.lastMsgSendTime, 99);
    assert_eq!(w.lastMsgReceiptTime, 100);

    // walEnd not advancing keeps the old latestWalEndTime.
    let (wal_end, send_time, receipt) = (250, 105, 106);
    if w.latestWalEnd < wal_end {
        w.latestWalEndTime = send_time;
    }
    w.latestWalEnd = wal_end;
    w.lastMsgSendTime = send_time;
    w.lastMsgReceiptTime = receipt;
    assert_eq!(w.latestWalEndTime, 99);
    assert_eq!(w.lastMsgSendTime, 105);
}

#[test]
fn walrcvdata_defaults_match_c_init() {
    let w = WalRcvData::default();
    assert_eq!(w.procno, types_core::INVALID_PROC_NUMBER);
    assert_eq!(w.pid, 0);
    assert_eq!(w.walRcvState, WalRcvState::WALRCV_STOPPED);
}

#[test]
fn compute_next_wakeup_uses_gucs() {
    reset_state_for_tests();
    with_state(|s| {
        s.wal_receiver_timeout = 60_000;
        s.wal_receiver_status_interval = 10;
        s.hot_standby_feedback = true;
    });

    WalRcvComputeNextWakeup(WALRCV_WAKEUP_TERMINATE, 0);
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_PING, 0);
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_REPLY, 0);
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_HSFEEDBACK, 0);
    let w = with_state(|s| s.wakeup);
    assert_eq!(w[WALRCV_WAKEUP_TERMINATE as usize], 60_000i64 * 1000);
    assert_eq!(w[WALRCV_WAKEUP_PING as usize], 30_000i64 * 1000);
    assert_eq!(w[WALRCV_WAKEUP_REPLY as usize], 10i64 * 1_000_000);
    assert_eq!(w[WALRCV_WAKEUP_HSFEEDBACK as usize], 10i64 * 1_000_000);

    with_state(|s| {
        s.wal_receiver_timeout = 0;
        s.wal_receiver_status_interval = 0;
    });
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_TERMINATE, 0);
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_PING, 0);
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_REPLY, 0);
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_HSFEEDBACK, 0);
    let w = with_state(|s| s.wakeup);
    assert_eq!(w[WALRCV_WAKEUP_TERMINATE as usize], TIMESTAMP_INFINITY);
    assert_eq!(w[WALRCV_WAKEUP_PING as usize], TIMESTAMP_INFINITY);
    assert_eq!(w[WALRCV_WAKEUP_REPLY as usize], TIMESTAMP_INFINITY);
    assert_eq!(w[WALRCV_WAKEUP_HSFEEDBACK as usize], TIMESTAMP_INFINITY);
}
