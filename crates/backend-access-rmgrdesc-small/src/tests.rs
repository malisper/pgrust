use super::*;

use mcx::{MemoryContext, PgString};
use types_core::PgResult;
use types_wal::XLR_INFO_MASK;

fn render(
    f: impl FnOnce(&mut PgString<'_>) -> PgResult<()>,
) -> Result<String, types_core::PgError> {
    let ctx = MemoryContext::new("rmgrdesc-test");
    let mut buf = PgString::new_in(ctx.mcx());
    f(&mut buf)?;
    Ok(buf.as_str().to_string())
}

fn oid(v: u32) -> [u8; 4] {
    v.to_ne_bytes()
}

// --- clogdesc ---

#[test]
fn clog() {
    let mut data = Vec::new();
    data.extend_from_slice(&42i64.to_ne_bytes());
    assert_eq!(
        render(|b| clog_desc(b, CLOG_ZEROPAGE, &data)).unwrap(),
        "page 42"
    );

    let mut data = Vec::new();
    data.extend_from_slice(&100i64.to_ne_bytes());
    data.extend_from_slice(&oid(1234)); // oldestXact
    data.extend_from_slice(&oid(5)); // oldestXactDb (not printed)
    assert_eq!(
        render(|b| clog_desc(b, CLOG_TRUNCATE, &data)).unwrap(),
        "page 100; oldestXact 1234"
    );

    // Unrecognized info appends nothing; insert-flag bits are masked off.
    assert_eq!(render(|b| clog_desc(b, 0x20, &[])).unwrap(), "");
    assert_eq!(
        render(|b| clog_desc(b, CLOG_ZEROPAGE | XLR_INFO_MASK, &42i64.to_ne_bytes())).unwrap(),
        "page 42"
    );

    // Truncated payload is loud.
    assert!(render(|b| clog_desc(b, CLOG_ZEROPAGE, &[0u8; 4])).is_err());

    assert_eq!(clog_identify(CLOG_ZEROPAGE), Some("ZEROPAGE"));
    assert_eq!(clog_identify(CLOG_TRUNCATE | 0x0f), Some("TRUNCATE"));
    assert_eq!(clog_identify(0x20), None);
}

// --- committsdesc ---

#[test]
fn commit_ts() {
    assert_eq!(
        render(|b| commit_ts_desc(b, COMMIT_TS_ZEROPAGE, &7i64.to_ne_bytes())).unwrap(),
        "7"
    );

    let mut data = Vec::new();
    data.extend_from_slice(&9i64.to_ne_bytes());
    data.extend_from_slice(&oid(77));
    assert_eq!(
        render(|b| commit_ts_desc(b, COMMIT_TS_TRUNCATE, &data)).unwrap(),
        "pageno 9, oldestXid 77"
    );

    // identify does NOT mask XLR_INFO_MASK (C switches on the raw byte).
    assert_eq!(commit_ts_identify(COMMIT_TS_ZEROPAGE), Some("ZEROPAGE"));
    assert_eq!(commit_ts_identify(COMMIT_TS_TRUNCATE), Some("TRUNCATE"));
    assert_eq!(commit_ts_identify(COMMIT_TS_TRUNCATE | 0x01), None);
}

// --- dbasedesc ---

#[test]
fn dbase() {
    let mut data = Vec::new();
    data.extend_from_slice(&oid(1)); // db_id
    data.extend_from_slice(&oid(2)); // tablespace_id
    data.extend_from_slice(&oid(3)); // src_db_id
    data.extend_from_slice(&oid(4)); // src_tablespace_id
    assert_eq!(
        render(|b| dbase_desc(b, XLOG_DBASE_CREATE_FILE_COPY, &data)).unwrap(),
        "copy dir 4/3 to 2/1"
    );

    let mut data = Vec::new();
    data.extend_from_slice(&oid(10));
    data.extend_from_slice(&oid(20));
    assert_eq!(
        render(|b| dbase_desc(b, XLOG_DBASE_CREATE_WAL_LOG, &data)).unwrap(),
        "create dir 20/10"
    );

    let mut data = Vec::new();
    data.extend_from_slice(&oid(5)); // db_id
    data.extend_from_slice(&2i32.to_ne_bytes()); // ntablespaces
    data.extend_from_slice(&oid(100));
    data.extend_from_slice(&oid(200));
    assert_eq!(
        render(|b| dbase_desc(b, XLOG_DBASE_DROP, &data)).unwrap(),
        "dir 100/5 200/5"
    );

    // Zero tablespaces: just "dir".
    let mut data = Vec::new();
    data.extend_from_slice(&oid(5));
    data.extend_from_slice(&0i32.to_ne_bytes());
    assert_eq!(
        render(|b| dbase_desc(b, XLOG_DBASE_DROP, &data)).unwrap(),
        "dir"
    );

    assert_eq!(dbase_identify(0x00), Some("CREATE_FILE_COPY"));
    assert_eq!(dbase_identify(0x10), Some("CREATE_WAL_LOG"));
    assert_eq!(dbase_identify(0x20), Some("DROP"));
    assert_eq!(dbase_identify(0x30), None);
}

// --- genericdesc ---

#[test]
fn generic() {
    // Two regions: (offset 3, length 2, 2 data bytes), (offset 9, length 1,
    // 1 data byte). The last region prints without the trailing "; ".
    let mut data = Vec::new();
    data.extend_from_slice(&3u16.to_ne_bytes());
    data.extend_from_slice(&2u16.to_ne_bytes());
    data.extend_from_slice(&[0xAA, 0xBB]);
    data.extend_from_slice(&9u16.to_ne_bytes());
    data.extend_from_slice(&1u16.to_ne_bytes());
    data.extend_from_slice(&[0xCC]);
    assert_eq!(
        render(|b| generic_desc(b, &data)).unwrap(),
        "offset 3, length 2; offset 9, length 1"
    );

    // Empty payload appends nothing.
    assert_eq!(render(|b| generic_desc(b, &[])).unwrap(), "");

    // A region whose length overshoots the payload still prints (the loop
    // exits with ptr >= end, the no-separator branch).
    let mut data = Vec::new();
    data.extend_from_slice(&1u16.to_ne_bytes());
    data.extend_from_slice(&100u16.to_ne_bytes());
    assert_eq!(
        render(|b| generic_desc(b, &data)).unwrap(),
        "offset 1, length 100"
    );

    assert_eq!(generic_identify(0x00), Some("Generic"));
    assert_eq!(generic_identify(0xF0), Some("Generic"));
}

// --- logicalmsgdesc ---

#[test]
fn logicalmsg() {
    let size = core::mem::size_of::<usize>();
    let msg_off = 8 + 2 * size;

    let prefix = b"test\0";
    let payload = [0x01u8, 0xAB, 0x00];

    let mut data = vec![0u8; msg_off];
    data[0..4].copy_from_slice(&oid(0)); // dbId
    data[4] = 1; // transactional
    data[8..8 + size].copy_from_slice(&prefix.len().to_ne_bytes());
    data[8 + size..msg_off].copy_from_slice(&payload.len().to_ne_bytes());
    data.extend_from_slice(prefix);
    data.extend_from_slice(&payload);

    assert_eq!(
        render(|b| logicalmsg_desc(b, XLOG_LOGICAL_MESSAGE, &data)).unwrap(),
        "transactional, prefix \"test\"; payload (3 bytes): 01 AB 00"
    );

    // Non-transactional, empty payload: trailing ": " with nothing after.
    let mut data = vec![0u8; msg_off];
    data[4] = 0;
    data[8..8 + size].copy_from_slice(&prefix.len().to_ne_bytes());
    data[8 + size..msg_off].copy_from_slice(&0usize.to_ne_bytes());
    data.extend_from_slice(prefix);
    assert_eq!(
        render(|b| logicalmsg_desc(b, XLOG_LOGICAL_MESSAGE, &data)).unwrap(),
        "non-transactional, prefix \"test\"; payload (0 bytes): "
    );

    assert_eq!(logicalmsg_identify(0x00), Some("MESSAGE"));
    assert_eq!(logicalmsg_identify(0x0f), Some("MESSAGE"));
    assert_eq!(logicalmsg_identify(0x10), None);
}

// --- relmapdesc ---

#[test]
fn relmap() {
    let mut data = Vec::new();
    data.extend_from_slice(&oid(11)); // dbid
    data.extend_from_slice(&oid(22)); // tsid
    data.extend_from_slice(&512i32.to_ne_bytes()); // nbytes
    assert_eq!(
        render(|b| relmap_desc(b, XLOG_RELMAP_UPDATE, &data)).unwrap(),
        "database 11 tablespace 22 size 512"
    );

    assert_eq!(relmap_identify(0x00), Some("UPDATE"));
    assert_eq!(relmap_identify(0x10), None);
}

// --- rmgrdesc_utils ---

#[test]
fn rmgrdesc_utils_array() {
    assert_eq!(
        render(|b| array_desc(b, &[] as &[u16], offset_elem_desc)).unwrap(),
        " []"
    );
    assert_eq!(
        render(|b| array_desc(b, &[1u16, 2, 3], offset_elem_desc)).unwrap(),
        " [1, 2, 3]"
    );
    assert_eq!(
        render(|b| array_desc(b, &[[1u16, 5], [2, 6]], redirect_elem_desc)).unwrap(),
        " [1->5, 2->6]"
    );
    assert_eq!(
        render(|b| array_desc(b, &[16384u32], oid_elem_desc)).unwrap(),
        " [16384]"
    );
}

// --- seqdesc ---

#[test]
fn seq() {
    let mut data = Vec::new();
    data.extend_from_slice(&oid(1663)); // spcOid
    data.extend_from_slice(&oid(5)); // dbOid
    data.extend_from_slice(&oid(16384)); // relNumber
    assert_eq!(
        render(|b| seq_desc(b, XLOG_SEQ_LOG, &data)).unwrap(),
        "rel 1663/5/16384"
    );
    assert_eq!(render(|b| seq_desc(b, 0x10, &data)).unwrap(), "");

    assert_eq!(seq_identify(XLOG_SEQ_LOG), Some("LOG"));
    assert_eq!(seq_identify(0x10), None);
}

// --- tblspcdesc ---

#[test]
fn tblspc() {
    let mut data = Vec::new();
    data.extend_from_slice(&oid(16385));
    data.extend_from_slice(b"/tmp/ts\0");
    assert_eq!(
        render(|b| tblspc_desc(b, XLOG_TBLSPC_CREATE, &data)).unwrap(),
        "16385 \"/tmp/ts\""
    );

    assert_eq!(
        render(|b| tblspc_desc(b, XLOG_TBLSPC_DROP, &oid(99))).unwrap(),
        "99"
    );

    // Missing NUL terminator is loud, not a wild read.
    let mut data = Vec::new();
    data.extend_from_slice(&oid(1));
    data.extend_from_slice(b"nope");
    assert!(render(|b| tblspc_desc(b, XLOG_TBLSPC_CREATE, &data)).is_err());

    assert_eq!(tblspc_identify(0x00), Some("CREATE"));
    assert_eq!(tblspc_identify(0x10), Some("DROP"));
    assert_eq!(tblspc_identify(0x20), None);
}
