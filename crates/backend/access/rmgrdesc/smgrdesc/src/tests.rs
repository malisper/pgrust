//! Unit tests for the `smgrdesc.c` port.
//!
//! Records are assembled byte-for-byte in the C `xl_smgr_*` on-disk layout and
//! the descriptor output verified. The `relpathbackend` (`common/relpath.c`)
//! seam is a process-global slot installed once with a faithful test stub, so
//! these tests run single-threaded (`--test-threads=1`).

use super::*;

use core::sync::atomic::{AtomicBool, Ordering};
use mcx::{slice_in, MemoryContext, PgString};
use ::types_core::primitive::ProcNumber;
use ::wal::wal::{DecodedXLogRecord, XLogRecord};
use ::wal::rmgr::XLogReaderState;

fn install_seams() {
    static DONE: AtomicBool = AtomicBool::new(false);
    if DONE.swap(true, Ordering::SeqCst) {
        return;
    }
    relpath_seams::relpathbackend::set(test_relpathbackend);
}

/// `relpathbackend(rlocator, INVALID_PROC_NUMBER, forkno)` for a permanent
/// relation: the `base/<db>/<rel>` form with fork suffix.
fn test_relpathbackend(
    rlocator: RelFileLocator,
    _backend: ProcNumber,
    forkno: ForkNumber,
) -> String {
    let base = alloc::format!("base/{}/{}", rlocator.dbOid, rlocator.relNumber);
    match forkno as i32 {
        0 => base,
        1 => alloc::format!("{base}_fsm"),
        2 => alloc::format!("{base}_vm"),
        3 => alloc::format!("{base}_init"),
        n => alloc::format!("{base}_{n}"),
    }
}

fn u32_ne(v: u32) -> [u8; 4] {
    v.to_ne_bytes()
}
fn i32_ne(v: i32) -> [u8; 4] {
    v.to_ne_bytes()
}

fn desc(info: u8, data: &[u8]) -> Result<alloc::string::String, PgError> {
    install_seams();
    let ctx = MemoryContext::new("test");
    let blocks = slice_in(ctx.mcx(), &[]).unwrap();
    let decoded = DecodedXLogRecord::new(XLogRecord::new(0, 0, 0, info, 0, 0), data, blocks);
    let reader = XLogReaderState {
        record: Some(decoded),
        ..Default::default()
    };
    let mut buf = PgString::new_in(ctx.mcx());
    smgr_desc_seam(&mut buf, &reader)?;
    Ok(buf.as_str().to_string())
}

#[test]
fn identify_all_opcodes() {
    assert_eq!(smgr_identify(XLOG_SMGR_CREATE), Some("CREATE"));
    assert_eq!(smgr_identify(XLOG_SMGR_TRUNCATE), Some("TRUNCATE"));
    // low nibble (XLR_INFO_MASK bits) is masked off
    assert_eq!(smgr_identify(XLOG_SMGR_CREATE | 0x0F), Some("CREATE"));
    assert_eq!(smgr_identify(0x00), None);
    assert_eq!(smgr_identify(0x70), None);
}

#[test]
fn create_renders_relpath() {
    // xl_smgr_create { rlocator (spc, db, rel) @0..12; forkNum @12 }
    let mut data = Vec::new();
    data.extend_from_slice(&u32_ne(1663)); // spcOid
    data.extend_from_slice(&u32_ne(5)); // dbOid
    data.extend_from_slice(&u32_ne(16384)); // relNumber
    data.extend_from_slice(&i32_ne(0)); // forkNum = MAIN
    let out = desc(XLOG_SMGR_CREATE, &data).unwrap();
    assert_eq!(out, "base/5/16384");
}

#[test]
fn create_fsm_fork() {
    let mut data = Vec::new();
    data.extend_from_slice(&u32_ne(1663));
    data.extend_from_slice(&u32_ne(5));
    data.extend_from_slice(&u32_ne(16384));
    data.extend_from_slice(&i32_ne(1)); // FSM
    let out = desc(XLOG_SMGR_CREATE, &data).unwrap();
    assert_eq!(out, "base/5/16384_fsm");
}

#[test]
fn truncate_renders_main_fork_and_fields() {
    // xl_smgr_truncate { blkno @0; rlocator @4..16; flags @16 }
    let mut data = Vec::new();
    data.extend_from_slice(&u32_ne(42)); // blkno
    data.extend_from_slice(&u32_ne(1663)); // spcOid
    data.extend_from_slice(&u32_ne(5)); // dbOid
    data.extend_from_slice(&u32_ne(16384)); // relNumber
    data.extend_from_slice(&i32_ne(7)); // flags
    let out = desc(XLOG_SMGR_TRUNCATE, &data).unwrap();
    assert_eq!(out, "base/5/16384 to 42 blocks flags 7");
}
