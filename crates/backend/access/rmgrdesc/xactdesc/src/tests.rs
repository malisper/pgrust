//! Unit tests for the `xactdesc.c` port.
//!
//! Records are assembled byte-for-byte in the C `xl_xact_*` on-disk layout and
//! the descriptor output verified. The `relpathbackend` (`common/relpath.c`)
//! and `timestamptz_to_str` (`utils/adt/timestamp.c`) seams are process-global
//! `OnceLock` slots installed once with faithful test stubs, so these tests run
//! single-threaded (`--test-threads=1`). Timestamp text is stubbed to a fixed
//! marker so the structural fragments around it can be asserted.

use super::*;

use core::sync::atomic::{AtomicBool, Ordering};
use mcx::{slice_in, MemoryContext, Mcx, PgString};
use ::types_core::primitive::ProcNumber;
use ::wal::wal::{DecodedXLogRecord, XLogRecord};
use wal::{
    XACT_COMPLETION_APPLY_FEEDBACK, XACT_COMPLETION_FORCE_SYNC_COMMIT,
    XACT_COMPLETION_UPDATE_RELCACHE_FILE,
};

const TS: &str = "<ts>";

fn install_seams() {
    static DONE: AtomicBool = AtomicBool::new(false);
    if DONE.swap(true, Ordering::SeqCst) {
        return;
    }
    relpath_seams::relpathbackend::set(test_relpathbackend);
    timestamp_seams::timestamptz_to_str::set(test_timestamptz_to_str);
}

/// `relpathbackend(rlocator, INVALID_PROC_NUMBER, forkno)` for a permanent
/// relation in the default tablespace (1663): the `base/<db>/<rel>` form.
fn test_relpathbackend(
    rlocator: RelFileLocator,
    _backend: ProcNumber,
    forkno: ForkNumber,
) -> String {
    let base = format!("base/{}/{}", rlocator.dbOid, rlocator.relNumber);
    match forkno as i32 {
        0 => base,
        1 => format!("{base}_fsm"),
        2 => format!("{base}_vm"),
        3 => format!("{base}_init"),
        n => format!("{base}_{n}"),
    }
}

fn test_timestamptz_to_str<'mcx>(mcx: Mcx<'mcx>, _t: TimestampTz) -> PgResult<PgString<'mcx>> {
    let mut s = PgString::new_in(mcx);
    s.try_push_str(TS)?;
    Ok(s)
}

fn u32_ne(v: u32) -> [u8; 4] {
    v.to_ne_bytes()
}
fn i32_ne(v: i32) -> [u8; 4] {
    v.to_ne_bytes()
}
fn i64_ne(v: i64) -> [u8; 8] {
    v.to_ne_bytes()
}
fn u64_ne(v: u64) -> [u8; 8] {
    v.to_ne_bytes()
}

/// Build an `XLogReaderState` carrying a decoded record with the given info,
/// payload, and replication origin, then run `xact_desc` into a fresh buffer.
fn desc(info: u8, data: &[u8], origin: RepOriginId) -> Result<String, PgError> {
    install_seams();
    let ctx = MemoryContext::new("test");
    let blocks = slice_in(ctx.mcx(), &[]).unwrap();
    let decoded = DecodedXLogRecord::new(XLogRecord::new(0, 0, 0, info, 0, 0), data, blocks)
        .with_origin(origin);
    let reader = XLogReaderState {
        ReadRecPtr: 0,
        EndRecPtr: 0,
        record: Some(decoded),
        ..Default::default()
    };
    let mut buf = PgString::new_in(ctx.mcx());
    xact_desc(&mut buf, &reader)?;
    Ok(buf.as_str().to_string())
}

// --- xact_identify --------------------------------------------------------

#[test]
fn identify_all_opcodes() {
    assert_eq!(xact_identify(XLOG_XACT_COMMIT), Some("COMMIT"));
    assert_eq!(xact_identify(XLOG_XACT_PREPARE), Some("PREPARE"));
    assert_eq!(xact_identify(XLOG_XACT_ABORT), Some("ABORT"));
    assert_eq!(xact_identify(XLOG_XACT_COMMIT_PREPARED), Some("COMMIT_PREPARED"));
    assert_eq!(xact_identify(XLOG_XACT_ABORT_PREPARED), Some("ABORT_PREPARED"));
    assert_eq!(xact_identify(XLOG_XACT_ASSIGNMENT), Some("ASSIGNMENT"));
    assert_eq!(xact_identify(XLOG_XACT_INVALIDATIONS), Some("INVALIDATION"));
    assert_eq!(xact_identify(XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO), Some("COMMIT"));
    assert_eq!(xact_identify(XLOG_XACT_COMMIT | 0x0F), Some("COMMIT"));
    assert_eq!(xact_identify(0x70), None);
}

// --- commit ---------------------------------------------------------------

#[test]
fn commit_minimal() {
    let data = i64_ne(0).to_vec();
    let out = desc(XLOG_XACT_COMMIT, &data, 0).unwrap();
    assert_eq!(out, TS);
}

#[test]
fn commit_with_subxacts_and_stats() {
    let xinfo = XACT_XINFO_HAS_SUBXACTS | XACT_XINFO_HAS_DROPPED_STATS;
    let mut data = Vec::new();
    data.extend_from_slice(&i64_ne(0));
    data.extend_from_slice(&u32_ne(xinfo));
    data.extend_from_slice(&i32_ne(2)); // nsubxacts
    data.extend_from_slice(&u32_ne(101));
    data.extend_from_slice(&u32_ne(102));
    data.extend_from_slice(&i32_ne(1)); // nitems
    data.extend_from_slice(&i32_ne(5)); // kind
    data.extend_from_slice(&u32_ne(7)); // dboid
    data.extend_from_slice(&u32_ne(9)); // objid_lo
    data.extend_from_slice(&u32_ne(0)); // objid_hi
    let out = desc(XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, &data, 0).unwrap();
    assert!(out.contains("; subxacts: 101 102"), "out = {out}");
    assert!(out.contains("; dropped stats: 5/7/9"), "out = {out}");
}

#[test]
fn commit_stats_objid_high_bits() {
    let mut data = Vec::new();
    data.extend_from_slice(&i64_ne(0));
    data.extend_from_slice(&u32_ne(XACT_XINFO_HAS_DROPPED_STATS));
    data.extend_from_slice(&i32_ne(1));
    data.extend_from_slice(&i32_ne(2)); // kind
    data.extend_from_slice(&u32_ne(3)); // dboid
    data.extend_from_slice(&u32_ne(1)); // objid_lo
    data.extend_from_slice(&u32_ne(1)); // objid_hi
    let out = desc(XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, &data, 0).unwrap();
    assert!(out.contains("; dropped stats: 2/3/4294967297"), "out = {out}");
}

#[test]
fn commit_with_rels() {
    let mut data = Vec::new();
    data.extend_from_slice(&i64_ne(0));
    data.extend_from_slice(&u32_ne(XACT_XINFO_HAS_RELFILELOCATORS));
    data.extend_from_slice(&i32_ne(1)); // nrels
    data.extend_from_slice(&u32_ne(1663)); // spcOid (default tablespace)
    data.extend_from_slice(&u32_ne(5)); // dbOid
    data.extend_from_slice(&u32_ne(16384)); // relNumber
    let out = desc(XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, &data, 0).unwrap();
    assert!(out.contains("; rels: base/5/16384"), "out = {out}");
}

#[test]
fn commit_completion_flags() {
    let xinfo = XACT_COMPLETION_APPLY_FEEDBACK | XACT_COMPLETION_FORCE_SYNC_COMMIT;
    let mut data = Vec::new();
    data.extend_from_slice(&i64_ne(0));
    data.extend_from_slice(&u32_ne(xinfo));
    let out = desc(XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, &data, 0).unwrap();
    assert!(out.contains("; apply_feedback"), "out = {out}");
    assert!(out.contains("; sync"), "out = {out}");
}

#[test]
fn commit_with_invalidations() {
    let xinfo =
        XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_INVALS | XACT_COMPLETION_UPDATE_RELCACHE_FILE;
    let mut data = Vec::new();
    data.extend_from_slice(&i64_ne(0));
    data.extend_from_slice(&u32_ne(xinfo));
    data.extend_from_slice(&u32_ne(7)); // dbId
    data.extend_from_slice(&u32_ne(8)); // tsId
    data.extend_from_slice(&i32_ne(1)); // nmsgs
    let mut m0 = vec![0u8; 16];
    m0[0] = 4u8; // catcache id 4
    data.extend_from_slice(&m0);
    let out = desc(XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, &data, 0).unwrap();
    assert!(out.contains("; relcache init file inval dbid 7 tsid 8"), "out = {out}");
    assert!(out.contains("; inval msgs: catcache 4"), "out = {out}");
}

#[test]
fn commit_twophase_prefix() {
    let mut data = Vec::new();
    data.extend_from_slice(&i64_ne(0));
    data.extend_from_slice(&u32_ne(XACT_XINFO_HAS_TWOPHASE));
    data.extend_from_slice(&u32_ne(12345));
    let out = desc(XLOG_XACT_COMMIT_PREPARED | XLOG_XACT_HAS_INFO, &data, 0).unwrap();
    assert!(out.starts_with("12345: "), "out = {out}");
}

#[test]
fn commit_with_origin() {
    let mut data = Vec::new();
    data.extend_from_slice(&i64_ne(0));
    data.extend_from_slice(&u32_ne(XACT_XINFO_HAS_ORIGIN));
    data.extend_from_slice(&u64_ne(0x0000_0001_0000_ABCD)); // origin_lsn
    data.extend_from_slice(&i64_ne(0)); // origin_timestamp
    let out = desc(XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, &data, 42).unwrap();
    assert!(out.contains("; origin: node 42, lsn 1/ABCD, at "), "out = {out}");
}

// --- abort ----------------------------------------------------------------

#[test]
fn abort_with_subxacts() {
    let mut data = Vec::new();
    data.extend_from_slice(&i64_ne(0));
    data.extend_from_slice(&u32_ne(XACT_XINFO_HAS_SUBXACTS));
    data.extend_from_slice(&i32_ne(1));
    data.extend_from_slice(&u32_ne(999));
    let out = desc(XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO, &data, 0).unwrap();
    assert!(out.contains("; subxacts: 999"), "out = {out}");
}

#[test]
fn abort_origin_before_stats_ordering() {
    let xinfo = XACT_XINFO_HAS_DROPPED_STATS | XACT_XINFO_HAS_ORIGIN;
    let mut data = Vec::new();
    data.extend_from_slice(&i64_ne(0));
    data.extend_from_slice(&u32_ne(xinfo));
    data.extend_from_slice(&i32_ne(1)); // nitems
    data.extend_from_slice(&i32_ne(1)); // kind
    data.extend_from_slice(&u32_ne(2)); // dboid
    data.extend_from_slice(&u32_ne(3)); // objid_lo
    data.extend_from_slice(&u32_ne(0)); // objid_hi
    data.extend_from_slice(&u64_ne(0x0000_0002_0000_0003)); // origin_lsn
    data.extend_from_slice(&i64_ne(0)); // origin_timestamp
    let out = desc(XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO, &data, 7).unwrap();
    let origin_pos = out.find("; origin:").expect("origin present");
    let stats_pos = out.find("; dropped stats:").expect("stats present");
    assert!(origin_pos < stats_pos, "out = {out}");
    assert!(out.contains("lsn 2/3,"), "out = {out}");
}

// --- assignment -----------------------------------------------------------

#[test]
fn assignment_record() {
    let mut data = Vec::new();
    data.extend_from_slice(&u32_ne(500)); // xtop
    data.extend_from_slice(&i32_ne(3)); // nsubxacts
    data.extend_from_slice(&u32_ne(501));
    data.extend_from_slice(&u32_ne(502));
    data.extend_from_slice(&u32_ne(503));
    let out = desc(XLOG_XACT_ASSIGNMENT, &data, 0).unwrap();
    assert_eq!(out, "xtop 500: subxacts: 501 502 503");
}

// --- standalone invalidations ---------------------------------------------

#[test]
fn standalone_invalidations() {
    let mut data = Vec::new();
    data.extend_from_slice(&i32_ne(2)); // nmsgs
    let mut m0 = vec![0u8; 16];
    m0[0] = 9u8; // catcache id 9
    data.extend_from_slice(&m0);
    let mut m1 = vec![0u8; 16];
    m1[0] = types_storage::sinval::SHAREDINVALRELCACHE_ID as u8;
    m1[8..12].copy_from_slice(&u32_ne(2048));
    data.extend_from_slice(&m1);
    let out = desc(XLOG_XACT_INVALIDATIONS, &data, 0).unwrap();
    assert_eq!(out, "; inval msgs: catcache 9 relcache 2048");
}

// --- prepare --------------------------------------------------------------

fn build_prepare(xid: u32, database: u32, gid: &str, subxacts: &[u32]) -> Vec<u8> {
    let mut hdr = vec![0u8; 72];
    hdr[8..12].copy_from_slice(&u32_ne(xid));
    hdr[12..16].copy_from_slice(&u32_ne(database));
    hdr[28..32].copy_from_slice(&i32_ne(subxacts.len() as i32)); // nsubxacts
    hdr[54..56].copy_from_slice(&(gid.len() as u16).to_ne_bytes()); // gidlen
    let mut data = hdr;
    data.extend_from_slice(gid.as_bytes());
    pad_to_maxalign(&mut data, gid.len());
    let start = data.len();
    for &x in subxacts {
        data.extend_from_slice(&u32_ne(x));
    }
    let written = data.len() - start;
    pad_to_maxalign(&mut data, written);
    data
}

fn pad_to_maxalign(data: &mut Vec<u8>, written: usize) {
    let padded = (written + 7) & !7;
    for _ in 0..(padded - written) {
        data.push(0);
    }
}

#[test]
fn prepare_record_basic() {
    let data = build_prepare(777, 12, "my_gid", &[778, 779]);
    let out = desc(XLOG_XACT_PREPARE, &data, 0).unwrap();
    assert!(out.starts_with("gid my_gid: "), "out = {out}");
    assert!(out.contains("; subxacts: 778 779"), "out = {out}");
}

#[test]
fn prepare_record_with_origin() {
    let data = build_prepare(1, 2, "g", &[]);
    let out = desc(XLOG_XACT_PREPARE, &data, 99).unwrap();
    assert!(out.contains("; origin: node 99, lsn 0/0, at "), "out = {out}");
}

#[test]
fn prepare_record_no_origin_when_invalid() {
    let data = build_prepare(1, 2, "g", &[]);
    let out = desc(XLOG_XACT_PREPARE, &data, 0).unwrap();
    assert!(!out.contains("; origin:"), "out = {out}");
}

#[test]
fn prepare_gid_trimmed_at_nul() {
    let mut hdr = vec![0u8; 72];
    hdr[8..12].copy_from_slice(&u32_ne(1));
    hdr[12..16].copy_from_slice(&u32_ne(2));
    hdr[54..56].copy_from_slice(&6u16.to_ne_bytes()); // gidlen = 6
    let mut data = hdr;
    data.extend_from_slice(b"abc\0xy"); // embedded NUL at index 3
    pad_to_maxalign(&mut data, 6);
    let out = desc(XLOG_XACT_PREPARE, &data, 0).unwrap();
    assert!(out.starts_with("gid abc: "), "out = {out}");
}

// --- parse re-exports (frontend reachability) -----------------------------

#[test]
fn parse_commit_record_is_reachable() {
    let mut data = Vec::new();
    data.extend_from_slice(&i64_ne(42));
    data.extend_from_slice(&u32_ne(XACT_XINFO_HAS_SUBXACTS));
    data.extend_from_slice(&i32_ne(1));
    data.extend_from_slice(&u32_ne(7));
    let parsed = parse_commit_record(XLOG_XACT_HAS_INFO, &data).unwrap();
    assert_eq!(parsed.xact_time, 42);
    assert_eq!(parsed.xinfo, XACT_XINFO_HAS_SUBXACTS);
    assert_eq!(parsed.nsubxacts, 1);
}

// --- truncation safety ----------------------------------------------------

#[test]
fn truncated_commit_is_error_not_panic() {
    let data = i64_ne(0).to_vec(); // HAS_INFO set but no xinfo follows
    let err = desc(XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, &data, 0).unwrap_err();
    assert_eq!(err.message(), TRUNCATED);
}

#[test]
fn huge_subxact_count_is_truncated() {
    let mut data = Vec::new();
    data.extend_from_slice(&i64_ne(0));
    data.extend_from_slice(&u32_ne(XACT_XINFO_HAS_SUBXACTS));
    data.extend_from_slice(&i32_ne(i32::MAX)); // huge, no xids follow
    let err = desc(XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, &data, 0).unwrap_err();
    assert_eq!(err.message(), TRUNCATED);
}
