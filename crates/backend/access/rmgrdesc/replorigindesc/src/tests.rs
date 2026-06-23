//! Unit tests for the `replorigindesc.c` port. Records are assembled
//! byte-for-byte in the C `xl_replorigin_*` on-disk layout. No external seams.

use super::*;

use mcx::{slice_in, MemoryContext, PgString};
use ::wal::rmgr::XLogReaderState;
use ::wal::wal::{DecodedXLogRecord, XLogRecord};

fn desc(info: u8, data: &[u8]) -> alloc::string::String {
    let ctx = MemoryContext::new("test");
    let blocks = slice_in(ctx.mcx(), &[]).unwrap();
    let decoded = DecodedXLogRecord::new(XLogRecord::new(0, 0, 0, info, 0, 0), data, blocks);
    let reader = XLogReaderState {
        record: Some(decoded),
        ..Default::default()
    };
    let mut buf = PgString::new_in(ctx.mcx());
    replorigin_desc_seam(&mut buf, &reader).unwrap();
    buf.as_str().to_string()
}

#[test]
fn identify_all_opcodes() {
    assert_eq!(replorigin_identify(XLOG_REPLORIGIN_SET), Some("SET"));
    assert_eq!(replorigin_identify(XLOG_REPLORIGIN_DROP), Some("DROP"));
    assert_eq!(replorigin_identify(0x20), None);
}

#[test]
fn set_renders_node_lsn_force() {
    // xl_replorigin_set { remote_lsn @0 (u64); node_id @8 (u16); force @10 }
    let mut data = Vec::new();
    data.extend_from_slice(&0x0000_0001_2345_6789u64.to_ne_bytes()); // remote_lsn
    data.extend_from_slice(&7u16.to_ne_bytes()); // node_id
    data.push(1); // force = true
    let out = desc(XLOG_REPLORIGIN_SET, &data);
    assert_eq!(out, "set 7; lsn 1/23456789; force: 1");
}

#[test]
fn set_force_false() {
    let mut data = Vec::new();
    data.extend_from_slice(&0u64.to_ne_bytes());
    data.extend_from_slice(&3u16.to_ne_bytes());
    data.push(0); // force = false
    let out = desc(XLOG_REPLORIGIN_SET, &data);
    assert_eq!(out, "set 3; lsn 0/0; force: 0");
}

#[test]
fn drop_renders_node() {
    // xl_replorigin_drop { node_id @0 (u16) }
    let data = 42u16.to_ne_bytes();
    let out = desc(XLOG_REPLORIGIN_DROP, &data);
    assert_eq!(out, "drop 42");
}
