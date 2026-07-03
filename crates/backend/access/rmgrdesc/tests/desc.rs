// Expected strings hand-rendered from the C format strings, for record
// types scripts/waldesc-diff.sh cannot yet generate.

use stringinfo::StringInfo;
use xlogreader_seams::{DecodedXLogRecord, XLogReaderState};

fn record_with(info: u8, data: &[u8]) -> XLogReaderState {
    let mut rec = DecodedXLogRecord::default();
    rec.xl_info = info;
    rec.main_data = data.as_ptr();
    rec.main_data_len = data.len() as u32;
    XLogReaderState { record: Some(rec), ..Default::default() }
}

fn run(desc: rmgr::RmDesc, info: u8, data: &[u8]) -> String {
    let ctx = Box::leak(Box::new(mcx::MemoryContext::new("test")));
    let mut buf = StringInfo::new_in(ctx.mcx()).unwrap();
    let record = record_with(info, data);
    desc(&mut buf, &record).unwrap();
    String::from_utf8(buf.as_bytes().to_vec()).unwrap()
}

fn le32(v: u32) -> [u8; 4] {
    v.to_ne_bytes()
}

#[test]
fn clog_truncate() {
    let mut d = vec![];
    d.extend_from_slice(&7i64.to_ne_bytes());
    d.extend_from_slice(&le32(100));
    d.extend_from_slice(&le32(1));
    assert_eq!(run(rmgrdesc::clogdesc::clog_desc, 0x10, &d), "page 7; oldestXact 100");
    assert_eq!(rmgrdesc::clogdesc::clog_identify(0x10), Some("TRUNCATE"));
}

#[test]
fn multixact_create() {
    let mut d = vec![];
    d.extend_from_slice(&le32(5));
    d.extend_from_slice(&le32(10));
    d.extend_from_slice(&le32(2));
    d.extend_from_slice(&le32(200));
    d.extend_from_slice(&le32(3));
    d.extend_from_slice(&le32(201));
    d.extend_from_slice(&le32(5));
    assert_eq!(
        run(rmgrdesc::mxactdesc::multixact_desc, 0x20, &d),
        "5 offset 10 nmembers 2: 200 (forupd) 201 (upd) "
    );
}

#[test]
fn multixact_truncate() {
    let mut d = vec![];
    for v in [9u32, 2, 4, 100, 200] {
        d.extend_from_slice(&le32(v));
    }
    assert_eq!(
        run(rmgrdesc::mxactdesc::multixact_desc, 0x30, &d),
        "offsets [2, 4), members [100, 200)"
    );
}

#[test]
fn relmap_update() {
    let mut d = vec![];
    for v in [5u32, 1663, 512] {
        d.extend_from_slice(&le32(v));
    }
    assert_eq!(
        run(rmgrdesc::relmapdesc::relmap_desc, 0x00, &d),
        "database 5 tablespace 1663 size 512"
    );
}

#[test]
fn dbase_records() {
    let mut d = vec![];
    for v in [16384u32, 1663, 5, 1664] {
        d.extend_from_slice(&le32(v));
    }
    assert_eq!(
        run(rmgrdesc::dbasedesc::dbase_desc, 0x00, &d),
        "copy dir 1664/5 to 1663/16384"
    );
    let mut d = vec![];
    d.extend_from_slice(&le32(16384));
    d.extend_from_slice(&le32(2)); // ntablespaces
    d.extend_from_slice(&le32(1663));
    d.extend_from_slice(&le32(1665));
    assert_eq!(run(rmgrdesc::dbasedesc::dbase_desc, 0x20, &d), "dir 1663/16384 1665/16384");
}

#[test]
fn tblspc_create() {
    let mut d = vec![];
    d.extend_from_slice(&le32(16385));
    d.extend_from_slice(b"/tmp/ts1\0");
    assert_eq!(run(rmgrdesc::tblspcdesc::tblspc_desc, 0x00, &d), "16385 \"/tmp/ts1\"");
    assert_eq!(
        run(rmgrdesc::tblspcdesc::tblspc_desc, 0x10, &le32(16385)),
        "16385"
    );
}

#[test]
fn seq_log() {
    let mut d = vec![];
    for v in [1663u32, 5, 16390] {
        d.extend_from_slice(&le32(v));
    }
    assert_eq!(run(rmgrdesc::seqdesc::seq_desc, 0x00, &d), "rel 1663/5/16390");
    assert_eq!(rmgrdesc::seqdesc::seq_identify(0x00), Some("LOG"));
}

#[test]
fn generic_pages() {
    let mut d = vec![];
    d.extend_from_slice(&24u16.to_ne_bytes());
    d.extend_from_slice(&2u16.to_ne_bytes());
    d.extend_from_slice(&[0xAA, 0xBB]);
    d.extend_from_slice(&96u16.to_ne_bytes());
    d.extend_from_slice(&0u16.to_ne_bytes());
    assert_eq!(
        run(rmgrdesc::genericdesc::generic_desc, 0x00, &d),
        "offset 24, length 2; offset 96, length 0"
    );
    assert_eq!(rmgrdesc::genericdesc::generic_identify(0xFF), Some("Generic"));
}

#[test]
fn heap_delete_infobits() {
    let mut d = vec![];
    d.extend_from_slice(&le32(900));
    d.extend_from_slice(&3u16.to_ne_bytes());
    d.push(heapam_xlog::XLHL_XMAX_EXCL_LOCK | heapam_xlog::XLHL_KEYS_UPDATED);
    d.push(0x01);
    assert_eq!(
        run(rmgrdesc::heapdesc::heap_desc, 0x10, &d),
        "xmax: 900, off: 3, infobits: [EXCL_LOCK, KEYS_UPDATED], flags: 0x01"
    );
    // empty infobits truncation arm
    let mut d = vec![];
    d.extend_from_slice(&le32(900));
    d.extend_from_slice(&3u16.to_ne_bytes());
    d.push(0);
    d.push(0x03);
    assert_eq!(
        run(rmgrdesc::heapdesc::heap_desc, 0x10, &d),
        "xmax: 900, off: 3, infobits: [], flags: 0x03"
    );
}

#[test]
fn standby_running_xacts_subxid_overflow() {
    let mut d = vec![];
    d.extend_from_slice(&le32(1));
    d.extend_from_slice(&le32(0));
    d.extend_from_slice(&le32(1)); // bool + padding
    d.extend_from_slice(&le32(1000));
    d.extend_from_slice(&le32(900));
    d.extend_from_slice(&le32(999));
    d.extend_from_slice(&le32(950));
    assert_eq!(
        run(rmgrdesc::standbydesc::standby_desc, 0x10, &d),
        "nextXid 1000 latestCompletedXid 999 oldestRunningXid 900; 1 xacts: 950; subxid overflowed"
    );
}
