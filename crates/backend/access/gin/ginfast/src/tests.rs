//! Unit tests for the ginfast page-byte accessors and size constants that need
//! no buffer manager.

use super::page;
use ::gin::GinMetaPageData;

/// `read_meta`/`write_meta`/`meta_to_bytes` round-trip the metapage fields at
/// the C struct offsets.
#[test]
fn metapage_roundtrip() {
    let mut buf = [0u8; types_core::primitive::BLCKSZ];
    // pd_special at offset 16 (u16) — point it past the contents so opaque
    // accessors are out of the way; not used by meta read/write.
    buf[16..18].copy_from_slice(&((types_core::primitive::BLCKSZ - 8) as u16).to_ne_bytes());

    let meta = GinMetaPageData {
        head: 11,
        tail: 22,
        tailFreeSize: 333,
        nPendingPages: 4,
        nPendingHeapTuples: 5,
        nTotalPages: 6,
        nEntryPages: 7,
        nDataPages: 8,
        nEntries: 9,
        ginVersion: 2,
    };
    page::write_meta(&mut buf, &meta);
    let got = page::read_meta(&buf).expect("read_meta");
    assert_eq!(got, meta);

    let bytes = page::meta_to_bytes(&meta);
    assert_eq!(bytes.len(), page::SIZE_OF_GIN_META_PAGE_DATA);
    // head is the first field of the WAL image.
    assert_eq!(u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]), 11);
}

/// `GIN_PAGE_FREESIZE` and `GIN_LIST_PAGE_SIZE` match the C formulas for the
/// default 8K block size.
#[test]
fn page_size_constants() {
    // BLCKSZ - MAXALIGN(24) - 8 = 8192 - 24 - 8 = 8160.
    assert_eq!(super::GIN_PAGE_FREESIZE, 8160);
    // BLCKSZ - 24 - 8 = 8160 (header not MAXALIGN'd, but 24 is already aligned).
    assert_eq!(super::GIN_LIST_PAGE_SIZE, 8160);
}
