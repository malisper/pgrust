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

#[test]
fn hash_vacuum_one_page() {
    let mut d = vec![];
    d.extend_from_slice(&le32(555)); // snapshotConflictHorizon
    d.extend_from_slice(&3u16.to_ne_bytes()); // ntuples
    d.push(1); // isCatalogRel
    d.push(0); // pad
    assert_eq!(
        run(rmgrdesc::hashdesc::hash_desc, 0xC0, &d),
        "ntuples 3, snapshotConflictHorizon 555, isCatalogRel T"
    );
    assert_eq!(rmgrdesc::hashdesc::hash_identify(0xC0), Some("VACUUM_ONE_PAGE"));
}

#[test]
fn hash_split_allocate_page() {
    let mut d = vec![];
    d.extend_from_slice(&le32(7)); // new_bucket
    d.extend_from_slice(&1u16.to_ne_bytes()); // old_bucket_flag
    d.extend_from_slice(&2u16.to_ne_bytes()); // new_bucket_flag
    d.push(3); // flags = both bits
    assert_eq!(
        run(rmgrdesc::hashdesc::hash_desc, 0x40, &d),
        "new_bucket 7, meta_page_masks_updated T, issplitpoint_changed T"
    );
    assert_eq!(rmgrdesc::hashdesc::hash_identify(0x40), Some("SPLIT_ALLOCATE_PAGE"));
    assert_eq!(rmgrdesc::hashdesc::hash_identify(0x50), Some("SPLIT_PAGE"));
    assert_eq!(rmgrdesc::hashdesc::hash_identify(0xA0), Some("SPLIT_CLEANUP"));
}

#[test]
fn hash_init_meta_page() {
    let mut d = vec![];
    d.extend_from_slice(&100.0f64.to_ne_bytes());
    d.extend_from_slice(&le32(1234)); // procid
    d.extend_from_slice(&75u16.to_ne_bytes()); // ffactor
    d.extend_from_slice(&0u16.to_ne_bytes());
    assert_eq!(
        run(rmgrdesc::hashdesc::hash_desc, 0x00, &d),
        "num_tuples 100, fillfactor 75"
    );
    assert_eq!(rmgrdesc::hashdesc::hash_identify(0x00), Some("INIT_META_PAGE"));
}

#[test]
fn gist_delete_and_page_reuse() {
    let mut d = vec![];
    d.extend_from_slice(&le32(42)); // snapshotConflictHorizon
    d.extend_from_slice(&5u16.to_ne_bytes()); // ntodelete
    d.push(1); // isCatalogRel
    assert_eq!(
        run(rmgrdesc::gistdesc::gist_desc, 0x10, &d),
        "delete: snapshotConflictHorizon 42, nitems: 5, isCatalogRel T"
    );
    assert_eq!(rmgrdesc::gistdesc::gist_identify(0x10), Some("DELETE"));

    let mut d = vec![];
    for v in [1663u32, 5, 16390, 99] {
        d.extend_from_slice(&le32(v));
    }
    let horizon: u64 = (7u64 << 32) | 1000u64;
    d.extend_from_slice(&horizon.to_ne_bytes());
    d.push(0);
    assert_eq!(
        run(rmgrdesc::gistdesc::gist_desc, 0x20, &d),
        "rel 1663/5/16390; blk 99; snapshotConflictHorizon 7:1000, isCatalogRel F"
    );
    assert_eq!(rmgrdesc::gistdesc::gist_identify(0x20), Some("PAGE_REUSE"));
}

#[test]
fn gist_page_split_and_delete() {
    let mut d = vec![0u8; 24];
    d[18..20].copy_from_slice(&4u16.to_ne_bytes()); // npage
    assert_eq!(run(rmgrdesc::gistdesc::gist_desc, 0x30, &d), "page_split: splits to 4 pages");
    assert_eq!(rmgrdesc::gistdesc::gist_identify(0x30), Some("PAGE_SPLIT"));

    let mut d = vec![];
    let delete_xid: u64 = (2u64 << 32) | 55u64;
    d.extend_from_slice(&delete_xid.to_ne_bytes());
    d.extend_from_slice(&9u16.to_ne_bytes());
    assert_eq!(run(rmgrdesc::gistdesc::gist_desc, 0x60, &d), "deleteXid 2:55; downlink 9");
    assert_eq!(rmgrdesc::gistdesc::gist_identify(0x60), Some("PAGE_DELETE"));

    assert_eq!(run(rmgrdesc::gistdesc::gist_desc, 0x00, &[]), "");
    assert_eq!(rmgrdesc::gistdesc::gist_identify(0x00), Some("PAGE_UPDATE"));
    assert_eq!(run(rmgrdesc::gistdesc::gist_desc, 0x70, &[]), "");
    assert_eq!(rmgrdesc::gistdesc::gist_identify(0x70), Some("ASSIGN_LSN"));
}

#[test]
fn replorigin_set_and_drop() {
    let mut d = vec![];
    let remote_lsn: u64 = (1u64 << 32) | 0x2Cu64;
    d.extend_from_slice(&remote_lsn.to_ne_bytes());
    d.extend_from_slice(&3u16.to_ne_bytes()); // node_id
    d.push(1); // force
    assert_eq!(
        run(rmgrdesc::replorigindesc::replorigin_desc, 0x00, &d),
        "set 3; lsn 1/2C; force: 1"
    );
    assert_eq!(rmgrdesc::replorigindesc::replorigin_identify(0x00), Some("SET"));

    assert_eq!(
        run(rmgrdesc::replorigindesc::replorigin_desc, 0x10, &7u16.to_ne_bytes()),
        "drop 7"
    );
    assert_eq!(rmgrdesc::replorigindesc::replorigin_identify(0x10), Some("DROP"));

    // C divergence: replorigin_identify does not mask XLR_INFO_MASK bits.
    assert_eq!(rmgrdesc::replorigindesc::replorigin_identify(0x01), None);
}

#[test]
fn logicalmsg_transactional_and_not() {
    let mut d = vec![];
    d.extend_from_slice(&le32(0)); // dbId
    d.push(1); // transactional
    d.extend_from_slice(&[0u8; 3]); // pad to the u64 prefix_size field
    let prefix = b"myprefix\0";
    let message = b"hello";
    d.extend_from_slice(&(prefix.len() as u64).to_ne_bytes());
    d.extend_from_slice(&(message.len() as u64).to_ne_bytes());
    d.extend_from_slice(prefix);
    d.extend_from_slice(message);
    assert_eq!(
        run(rmgrdesc::logicalmsgdesc::logicalmsg_desc, 0x00, &d),
        "transactional, prefix \"myprefix\"; payload (5 bytes): 68 65 6C 6C 6F"
    );
    assert_eq!(rmgrdesc::logicalmsgdesc::logicalmsg_identify(0x00), Some("MESSAGE"));

    d[4] = 0; // non-transactional
    assert_eq!(
        run(rmgrdesc::logicalmsgdesc::logicalmsg_desc, 0x00, &d),
        "non-transactional, prefix \"myprefix\"; payload (5 bytes): 68 65 6C 6C 6F"
    );
}

#[test]
fn gin_split_and_delete_listpage() {
    let mut d = vec![0u8; 26];
    d[24..26].copy_from_slice(&(gin_vocab::GIN_SPLIT_ROOT | gin_vocab::GIN_INSERT_ISDATA).to_ne_bytes());
    assert_eq!(
        run(rmgrdesc::gindesc::gin_desc, 0x30, &d),
        "isrootsplit: T isdata: T isleaf: F"
    );
    assert_eq!(rmgrdesc::gindesc::gin_identify(0x30), Some("SPLIT"));

    let mut d = vec![0u8; 56 + 4];
    d[56..60].copy_from_slice(&le32(3));
    assert_eq!(run(rmgrdesc::gindesc::gin_desc, 0x80, &d), "ndeleted: 3");
    assert_eq!(rmgrdesc::gindesc::gin_identify(0x80), Some("DELETE_LISTPAGE"));

    assert_eq!(run(rmgrdesc::gindesc::gin_desc, 0x10, &[]), "");
    assert_eq!(rmgrdesc::gindesc::gin_identify(0x10), Some("CREATE_PTREE"));
}

#[test]
fn gin_insert_internal_children() {
    let mut d = vec![0u8; 10];
    // flags = 0 (not data, not leaf) -> children read from main data.
    let left: u32 = 11;
    let right: u32 = 22;
    d[2..4].copy_from_slice(&((left >> 16) as u16).to_ne_bytes());
    d[4..6].copy_from_slice(&(left as u16).to_ne_bytes());
    d[6..8].copy_from_slice(&((right >> 16) as u16).to_ne_bytes());
    d[8..10].copy_from_slice(&(right as u16).to_ne_bytes());
    assert_eq!(
        run(rmgrdesc::gindesc::gin_desc, 0x20, &d),
        "isdata: F isleaf: F children: 11/22"
    );
    assert_eq!(rmgrdesc::gindesc::gin_identify(0x20), Some("INSERT"));
}
