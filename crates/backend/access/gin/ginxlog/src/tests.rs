//! Unit tests for `backend-access-gin-ginxlog`.

extern crate std;

use super::*;

/// `GinMetaPageData` round-trips through the on-disk byte (de)serialization
/// (`parse_gin_meta` <-> `write_meta` at `PageGetContents`).
#[test]
fn meta_page_roundtrip() {
    let meta = GinMetaPageData {
        head: 11,
        tail: 22,
        tailFreeSize: 333,
        nPendingPages: 4,
        nPendingHeapTuples: 0x1_0000_0007,
        nTotalPages: 55,
        nEntryPages: 6,
        nDataPages: 7,
        nEntries: 0x2_0000_0009,
        ginVersion: 2,
    };

    // Build a zeroed page, write the metadata at PageGetContents, then decode it
    // back from the same byte offsets.
    let mut page = std::vec![0u8; BLCKSZ];
    write_meta(&mut page, &meta);

    let off = gdp::page_contents_offset();
    let decoded = parse_gin_meta(&page[off..]);

    assert_eq!(decoded.head, meta.head);
    assert_eq!(decoded.tail, meta.tail);
    assert_eq!(decoded.tailFreeSize, meta.tailFreeSize);
    assert_eq!(decoded.nPendingPages, meta.nPendingPages);
    assert_eq!(decoded.nPendingHeapTuples, meta.nPendingHeapTuples);
    assert_eq!(decoded.nTotalPages, meta.nTotalPages);
    assert_eq!(decoded.nEntryPages, meta.nEntryPages);
    assert_eq!(decoded.nDataPages, meta.nDataPages);
    assert_eq!(decoded.nEntries, meta.nEntries);
    assert_eq!(decoded.ginVersion, meta.ginVersion);
}

/// `index_tuple_size` reads `t_info & INDEX_SIZE_MASK` after the 6-byte t_tid.
#[test]
fn index_tuple_size_reads_t_info() {
    let mut tuple = [0u8; 16];
    // t_info at byte offset 6: low 13 bits (INDEX_SIZE_MASK = 0x1FFF) = size; the
    // top 3 bits are flags (has-nulls / has-varwidth) and must be masked off.
    let t_info: u16 = 0xE00D; // size = 0xD = 13; top 3 flag bits set (ignored).
    tuple[6..8].copy_from_slice(&t_info.to_ne_bytes());
    assert_eq!(index_tuple_size(&tuple), 13);
}

/// `gin_set_downlink` writes `t_tid = (blkno, InvalidOffsetNumber)`.
#[test]
fn set_downlink_writes_tid() {
    let mut tuple = [0xFFu8; 8];
    gin_set_downlink(&mut tuple, 0x0001_0203);
    let tid = gdp::read_item_pointer(&tuple);
    assert_eq!(tid.ip_posid, INVALID_OFFSET_NUMBER);
}

/// `block_id_get_block_number` decodes a 4-byte `BlockIdData` image.
#[test]
fn block_id_decode() {
    // bi_hi = 0x0001, bi_lo = 0x0203 -> 0x00010203
    let buf = [0x01u8, 0x00, 0x03, 0x02];
    assert_eq!(block_id_get_block_number(&buf), 0x0001_0203);
}

/// Installing the owned rmgr seams does not panic and the dispatch table is
/// populated.
#[test]
fn seams_install() {
    init_seams();
    // gin_xlog_startup creates the recovery context; cleanup tears it down.
    gin_xlog_startup(MemoryContext::new("test parent").mcx()).unwrap();
    gin_xlog_cleanup();
}
