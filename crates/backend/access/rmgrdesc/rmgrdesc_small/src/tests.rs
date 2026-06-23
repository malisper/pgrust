use super::*;

use mcx::{slice_in, MemoryContext, PgString, PgVec};
use types_error::PgResult;
use wal::{DecodedXLogRecord, XLogRecord, XLR_INFO_MASK};

/// Build a `DecodedXLogRecord` carrying `info` and `data` as its main data
/// and render it with `f`.
fn render(
    info: u8,
    data: &[u8],
    f: impl FnOnce(&mut PgString<'_>, &DecodedXLogRecord<'_>) -> PgResult<()>,
) -> Result<String, types_error::PgError> {
    let ctx = MemoryContext::new("rmgrdesc-test");
    let record = DecodedXLogRecord::new(
        XLogRecord::new(0, 0, 0, info, 0, 0),
        data,
        PgVec::new_in(ctx.mcx()),
    );
    let mut buf = PgString::new_in(ctx.mcx());
    f(&mut buf, &record)?;
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
    assert_eq!(render(CLOG_ZEROPAGE, &data, clog_desc).unwrap(), "page 42");

    let mut data = Vec::new();
    data.extend_from_slice(&100i64.to_ne_bytes());
    data.extend_from_slice(&oid(1234)); // oldestXact
    data.extend_from_slice(&oid(5)); // oldestXactDb (not printed)
    assert_eq!(
        render(CLOG_TRUNCATE, &data, clog_desc).unwrap(),
        "page 100; oldestXact 1234"
    );

    // Unrecognized info appends nothing; insert-flag bits are masked off.
    assert_eq!(render(0x20, &[], clog_desc).unwrap(), "");
    assert_eq!(
        render(CLOG_ZEROPAGE | XLR_INFO_MASK, &42i64.to_ne_bytes(), clog_desc).unwrap(),
        "page 42"
    );

    // Truncated payload is loud.
    assert!(render(CLOG_ZEROPAGE, &[0u8; 4], clog_desc).is_err());
    assert!(render(CLOG_TRUNCATE, &[0u8; 12], clog_desc).is_err());

    assert_eq!(clog_identify(CLOG_ZEROPAGE), Some("ZEROPAGE"));
    assert_eq!(clog_identify(CLOG_TRUNCATE | 0x0f), Some("TRUNCATE"));
    assert_eq!(clog_identify(0x20), None);
}

// --- committsdesc ---

#[test]
fn commit_ts() {
    assert_eq!(
        render(COMMIT_TS_ZEROPAGE, &7i64.to_ne_bytes(), commit_ts_desc).unwrap(),
        "7"
    );

    let mut data = Vec::new();
    data.extend_from_slice(&9i64.to_ne_bytes());
    data.extend_from_slice(&oid(77));
    assert_eq!(
        render(COMMIT_TS_TRUNCATE, &data, commit_ts_desc).unwrap(),
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
        render(XLOG_DBASE_CREATE_FILE_COPY, &data, dbase_desc).unwrap(),
        "copy dir 4/3 to 2/1"
    );

    let mut data = Vec::new();
    data.extend_from_slice(&oid(10));
    data.extend_from_slice(&oid(20));
    assert_eq!(
        render(XLOG_DBASE_CREATE_WAL_LOG, &data, dbase_desc).unwrap(),
        "create dir 20/10"
    );

    let mut data = Vec::new();
    data.extend_from_slice(&oid(5)); // db_id
    data.extend_from_slice(&2i32.to_ne_bytes()); // ntablespaces
    data.extend_from_slice(&oid(100));
    data.extend_from_slice(&oid(200));
    assert_eq!(
        render(XLOG_DBASE_DROP, &data, dbase_desc).unwrap(),
        "dir 100/5 200/5"
    );

    // Zero tablespaces: just "dir".
    let mut data = Vec::new();
    data.extend_from_slice(&oid(5));
    data.extend_from_slice(&0i32.to_ne_bytes());
    assert_eq!(render(XLOG_DBASE_DROP, &data, dbase_desc).unwrap(), "dir");

    // A drop record missing one of its declared tablespace ids is loud.
    let mut data = Vec::new();
    data.extend_from_slice(&oid(5));
    data.extend_from_slice(&2i32.to_ne_bytes());
    data.extend_from_slice(&oid(100));
    assert!(render(XLOG_DBASE_DROP, &data, dbase_desc).is_err());

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
        render(0x00, &data, generic_desc).unwrap(),
        "offset 3, length 2; offset 9, length 1"
    );

    // Empty payload appends nothing.
    assert_eq!(render(0x00, &[], generic_desc).unwrap(), "");

    // A region whose length overshoots the payload still prints (the loop
    // exits with ptr >= end, the no-separator branch).
    let mut data = Vec::new();
    data.extend_from_slice(&1u16.to_ne_bytes());
    data.extend_from_slice(&100u16.to_ne_bytes());
    assert_eq!(
        render(0x00, &data, generic_desc).unwrap(),
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
        render(XLOG_LOGICAL_MESSAGE, &data, logicalmsg_desc).unwrap(),
        "transactional, prefix \"test\"; payload (3 bytes): 01 AB 00"
    );

    // Non-transactional, empty payload: trailing ": " with nothing after.
    let mut data = vec![0u8; msg_off];
    data[4] = 0;
    data[8..8 + size].copy_from_slice(&prefix.len().to_ne_bytes());
    data[8 + size..msg_off].copy_from_slice(&0usize.to_ne_bytes());
    data.extend_from_slice(prefix);
    assert_eq!(
        render(XLOG_LOGICAL_MESSAGE, &data, logicalmsg_desc).unwrap(),
        "non-transactional, prefix \"test\"; payload (0 bytes): "
    );

    // A record whose declared sizes overshoot the payload is loud.
    let mut data = vec![0u8; msg_off];
    data[8..8 + size].copy_from_slice(&prefix.len().to_ne_bytes());
    data[8 + size..msg_off].copy_from_slice(&4usize.to_ne_bytes());
    data.extend_from_slice(prefix);
    assert!(render(XLOG_LOGICAL_MESSAGE, &data, logicalmsg_desc).is_err());

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
        render(XLOG_RELMAP_UPDATE, &data, relmap_desc).unwrap(),
        "database 11 tablespace 22 size 512"
    );

    assert_eq!(relmap_identify(0x00), Some("UPDATE"));
    assert_eq!(relmap_identify(0x10), None);
}

// --- rmgrdesc_utils ---

#[test]
fn rmgrdesc_utils_array() {
    fn render_buf(
        f: impl FnOnce(&mut PgString<'_>) -> PgResult<()>,
    ) -> Result<String, types_error::PgError> {
        let ctx = MemoryContext::new("rmgrdesc-test");
        let mut buf = PgString::new_in(ctx.mcx());
        f(&mut buf)?;
        Ok(buf.as_str().to_string())
    }

    assert_eq!(
        render_buf(|b| array_desc(b, &[] as &[u16], offset_elem_desc)).unwrap(),
        " []"
    );
    assert_eq!(
        render_buf(|b| array_desc(b, &[1u16, 2, 3], offset_elem_desc)).unwrap(),
        " [1, 2, 3]"
    );
    assert_eq!(
        render_buf(|b| array_desc(b, &[[1u16, 5], [2, 6]], redirect_elem_desc)).unwrap(),
        " [1->5, 2->6]"
    );
    assert_eq!(
        render_buf(|b| array_desc(b, &[16384u32], oid_elem_desc)).unwrap(),
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
        render(XLOG_SEQ_LOG, &data, seq_desc).unwrap(),
        "rel 1663/5/16384"
    );
    assert_eq!(render(0x10, &data, seq_desc).unwrap(), "");

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
        render(XLOG_TBLSPC_CREATE, &data, tblspc_desc).unwrap(),
        "16385 \"/tmp/ts\""
    );

    assert_eq!(render(XLOG_TBLSPC_DROP, &oid(99), tblspc_desc).unwrap(), "99");

    // Missing NUL terminator is loud, not a wild read.
    let mut data = Vec::new();
    data.extend_from_slice(&oid(1));
    data.extend_from_slice(b"nope");
    assert!(render(XLOG_TBLSPC_CREATE, &data, tblspc_desc).is_err());

    // A non-UTF-8 path renders lossily (C's %s prints the raw bytes).
    let mut data = Vec::new();
    data.extend_from_slice(&oid(2));
    data.extend_from_slice(b"a\xFFb\0");
    assert_eq!(
        render(XLOG_TBLSPC_CREATE, &data, tblspc_desc).unwrap(),
        "2 \"a\u{FFFD}b\""
    );

    assert_eq!(tblspc_identify(0x00), Some("CREATE"));
    assert_eq!(tblspc_identify(0x10), Some("DROP"));
    assert_eq!(tblspc_identify(0x20), None);
}
