//! Unit tests for the `xlogdesc.c` port. Records are assembled byte-for-byte
//! in the C struct on-disk layout (offsets verified against the C structs) and
//! the descriptor output verified. The `timestamptz_to_str`
//! (`utils/adt/timestamp.c`) seam is a process-global slot installed once with
//! a fixed-marker test stub, so these tests run single-threaded
//! (`--test-threads=1`).

use super::*;

use core::sync::atomic::{AtomicBool, Ordering};
use mcx::{slice_in, MemoryContext, Mcx, PgString};
use wal::rmgr::XLogReaderState;
use wal::wal::{DecodedXLogRecord, XLogRecord};

const TS: &str = "<ts>";

fn install_seams() {
    static DONE: AtomicBool = AtomicBool::new(false);
    if DONE.swap(true, Ordering::SeqCst) {
        return;
    }
    timestamp_seams::timestamptz_to_str::set(test_timestamptz_to_str);
}

fn test_timestamptz_to_str<'mcx>(mcx: Mcx<'mcx>, _t: TimestampTz) -> PgResult<PgString<'mcx>> {
    let mut s = PgString::new_in(mcx);
    s.try_push_str(TS)?;
    Ok(s)
}

fn desc(info: u8, data: &[u8]) -> alloc::string::String {
    install_seams();
    let ctx = MemoryContext::new("test");
    let blocks = slice_in(ctx.mcx(), &[]).unwrap();
    let decoded = DecodedXLogRecord::new(XLogRecord::new(0, 0, 0, info, 0, 0), data, blocks);
    let reader = XLogReaderState {
        record: Some(decoded),
        ..Default::default()
    };
    let mut buf = PgString::new_in(ctx.mcx());
    xlog_desc_seam(&mut buf, &reader).unwrap();
    buf.as_str().to_string()
}

#[test]
fn identify_all_opcodes() {
    assert_eq!(xlog_identify(XLOG_CHECKPOINT_SHUTDOWN), Some("CHECKPOINT_SHUTDOWN"));
    assert_eq!(xlog_identify(XLOG_CHECKPOINT_ONLINE), Some("CHECKPOINT_ONLINE"));
    assert_eq!(xlog_identify(XLOG_NOOP), Some("NOOP"));
    assert_eq!(xlog_identify(XLOG_NEXTOID), Some("NEXTOID"));
    assert_eq!(xlog_identify(XLOG_SWITCH), Some("SWITCH"));
    assert_eq!(xlog_identify(XLOG_BACKUP_END), Some("BACKUP_END"));
    assert_eq!(xlog_identify(XLOG_PARAMETER_CHANGE), Some("PARAMETER_CHANGE"));
    assert_eq!(xlog_identify(XLOG_RESTORE_POINT), Some("RESTORE_POINT"));
    assert_eq!(xlog_identify(XLOG_FPW_CHANGE), Some("FPW_CHANGE"));
    assert_eq!(xlog_identify(XLOG_END_OF_RECOVERY), Some("END_OF_RECOVERY"));
    assert_eq!(xlog_identify(XLOG_OVERWRITE_CONTRECORD), Some("OVERWRITE_CONTRECORD"));
    assert_eq!(xlog_identify(XLOG_FPI), Some("FPI"));
    assert_eq!(xlog_identify(XLOG_FPI_FOR_HINT), Some("FPI_FOR_HINT"));
    assert_eq!(xlog_identify(XLOG_CHECKPOINT_REDO), Some("CHECKPOINT_REDO"));
    // low nibble masked off
    assert_eq!(xlog_identify(XLOG_NEXTOID | 0x0F), Some("NEXTOID"));
    assert_eq!(xlog_identify(0xC0), None);
}

#[test]
fn wal_level_string_lookup() {
    assert_eq!(get_wal_level_string(0), "minimal");
    assert_eq!(get_wal_level_string(1), "replica");
    assert_eq!(get_wal_level_string(2), "logical");
    assert_eq!(get_wal_level_string(99), "?");
}

/// Assemble an 88-byte CheckPoint body with the verified field offsets.
fn checkpoint_body() -> Vec<u8> {
    let mut d = vec![0u8; 88];
    d[0..8].copy_from_slice(&0x0000_0001_DEAD_BEEFu64.to_ne_bytes()); // redo
    d[8..12].copy_from_slice(&5u32.to_ne_bytes()); // ThisTimeLineID
    d[12..16].copy_from_slice(&4u32.to_ne_bytes()); // PrevTimeLineID
    d[16] = 1; // fullPageWrites
    d[20..24].copy_from_slice(&2i32.to_ne_bytes()); // wal_level = logical
    d[24..32].copy_from_slice(&0x0000_0002_0000_0457u64.to_ne_bytes()); // nextXid (epoch 2, xid 0x457)
    d[32..36].copy_from_slice(&16400u32.to_ne_bytes()); // nextOid
    d[36..40].copy_from_slice(&10u32.to_ne_bytes()); // nextMulti
    d[40..44].copy_from_slice(&20u32.to_ne_bytes()); // nextMultiOffset
    d[44..48].copy_from_slice(&100u32.to_ne_bytes()); // oldestXid
    d[48..52].copy_from_slice(&1u32.to_ne_bytes()); // oldestXidDB
    d[52..56].copy_from_slice(&3u32.to_ne_bytes()); // oldestMulti
    d[56..60].copy_from_slice(&2u32.to_ne_bytes()); // oldestMultiDB
    // time @64..72 not rendered
    d[72..76].copy_from_slice(&50u32.to_ne_bytes()); // oldestCommitTsXid
    d[76..80].copy_from_slice(&60u32.to_ne_bytes()); // newestCommitTsXid
    d[80..84].copy_from_slice(&70u32.to_ne_bytes()); // oldestActiveXid
    d
}

#[test]
fn checkpoint_shutdown() {
    let out = desc(XLOG_CHECKPOINT_SHUTDOWN, &checkpoint_body());
    assert_eq!(
        out,
        "redo 1/DEADBEEF; tli 5; prev tli 4; fpw true; wal_level logical; xid 2:1111; \
         oid 16400; multi 10; offset 20; oldest xid 100 in DB 1; oldest multi 3 in DB 2; \
         oldest/newest commit timestamp xid: 50/60; oldest running xid 70; shutdown"
    );
}

#[test]
fn checkpoint_online_says_online() {
    let out = desc(XLOG_CHECKPOINT_ONLINE, &checkpoint_body());
    assert!(out.ends_with("oldest running xid 70; online"), "{out}");
}

#[test]
fn nextoid() {
    let out = desc(XLOG_NEXTOID, &123456u32.to_ne_bytes());
    assert_eq!(out, "123456");
}

#[test]
fn restore_point() {
    // xl_restore_point { rp_time @0 (i64); rp_name @8 (char[64], NUL-term) }
    let mut d = vec![0u8; 8];
    d.extend_from_slice(b"my point\0");
    let out = desc(XLOG_RESTORE_POINT, &d);
    assert_eq!(out, "my point");
}

#[test]
fn fpi_prints_nothing() {
    assert_eq!(desc(XLOG_FPI, &[]), "");
    assert_eq!(desc(XLOG_FPI_FOR_HINT, &[]), "");
}

#[test]
fn backup_end() {
    let out = desc(XLOG_BACKUP_END, &0x0000_00AB_0000_00CDu64.to_ne_bytes());
    assert_eq!(out, "AB/CD");
}

#[test]
fn parameter_change() {
    // xl_parameter_change: 6 ints @0,4,8,12,16,20; 2 bools @24,25 (sizeof 28)
    let mut d = vec![0u8; 28];
    d[0..4].copy_from_slice(&100i32.to_ne_bytes()); // MaxConnections
    d[4..8].copy_from_slice(&8i32.to_ne_bytes()); // max_worker_processes
    d[8..12].copy_from_slice(&10i32.to_ne_bytes()); // max_wal_senders
    d[12..16].copy_from_slice(&0i32.to_ne_bytes()); // max_prepared_xacts
    d[16..20].copy_from_slice(&64i32.to_ne_bytes()); // max_locks_per_xact
    d[20..24].copy_from_slice(&1i32.to_ne_bytes()); // wal_level = replica
    d[24] = 1; // wal_log_hints = on
    d[25] = 0; // track_commit_timestamp = off
    let out = desc(XLOG_PARAMETER_CHANGE, &d);
    assert_eq!(
        out,
        "max_connections=100 max_worker_processes=8 max_wal_senders=10 \
         max_prepared_xacts=0 max_locks_per_xact=64 wal_level=replica \
         wal_log_hints=on track_commit_timestamp=off"
    );
}

#[test]
fn fpw_change() {
    assert_eq!(desc(XLOG_FPW_CHANGE, &[1]), "true");
    assert_eq!(desc(XLOG_FPW_CHANGE, &[0]), "false");
}

#[test]
fn end_of_recovery() {
    // xl_end_of_recovery { end_time @0 (i64); ThisTLI @8; PrevTLI @12; wal_level @16 }
    let mut d = vec![0u8; 24];
    d[0..8].copy_from_slice(&999i64.to_ne_bytes()); // end_time (stubbed)
    d[8..12].copy_from_slice(&7u32.to_ne_bytes()); // ThisTimeLineID
    d[12..16].copy_from_slice(&6u32.to_ne_bytes()); // PrevTimeLineID
    d[16..20].copy_from_slice(&0i32.to_ne_bytes()); // wal_level = minimal
    let out = desc(XLOG_END_OF_RECOVERY, &d);
    assert_eq!(out, "tli 7; prev tli 6; time <ts>; wal_level minimal");
}

#[test]
fn overwrite_contrecord() {
    // xl_overwrite_contrecord { overwritten_lsn @0 (u64); overwrite_time @8 (i64) }
    let mut d = vec![0u8; 16];
    d[0..8].copy_from_slice(&0x0000_0012_0000_0034u64.to_ne_bytes());
    d[8..16].copy_from_slice(&42i64.to_ne_bytes()); // stubbed
    let out = desc(XLOG_OVERWRITE_CONTRECORD, &d);
    assert_eq!(out, "lsn 12/34; time <ts>");
}

#[test]
fn checkpoint_redo() {
    let out = desc(XLOG_CHECKPOINT_REDO, &2i32.to_ne_bytes());
    assert_eq!(out, "wal_level logical");
}
