//! Unit tests for the size-macro arithmetic and the page-byte serializers that
//! do not require a live relcache / buffer manager.

use super::*;

#[test]
fn size_macros_match_c() {
    // SGITHDRSZ = MAXALIGN(sizeof(SpGistInnerTupleData)) = MAXALIGN(8) = 8.
    assert_eq!(SGITHDRSZ, 8);
    // SGNTHDRSZ = MAXALIGN(sizeof(SpGistNodeTupleData)) = MAXALIGN(8) = 8.
    assert_eq!(SGNTHDRSZ(), 8);
    // SGDTSIZE = MAXALIGN(sizeof(SpGistDeadTupleData)) = MAXALIGN(16) = 16.
    assert_eq!(SGDTSIZE, 16);
    // SGLTHDRSZ(false) = MAXALIGN(12) = 16; SGLTHDRSZ(true) = MAXALIGN(12+4) = 16.
    assert_eq!(SGLTHDRSZ(false), 16);
    assert_eq!(SGLTHDRSZ(true), 16);
}

#[test]
fn page_capacity_matches_c() {
    // SPGIST_PAGE_CAPACITY = MAXALIGN_DOWN(8192 - 24 - MAXALIGN(8)).
    let expected = (8192usize - 24 - 8) & !7;
    assert_eq!(SPGIST_PAGE_CAPACITY, expected);
}

#[test]
fn opaque_offset_is_last_maxalign_element() {
    assert_eq!(OPAQUE_OFFSET, 8192 - 8);
}

#[test]
fn spgist_init_page_stamps_opaque() {
    let mut page = alloc::vec![0u8; BLCKSZ as usize];
    SpGistInitPage(&mut page, SPGIST_LEAF).unwrap();
    assert_eq!(opaque_flags(&page), SPGIST_LEAF);
    assert!(SpGistPageIsLeaf(&page));
    assert!(!SpGistPageStoresNulls(&page));
    // spgist_page_id is the last half-word of the opaque.
    let id = u16::from_ne_bytes([page[OPAQUE_OFFSET + 6], page[OPAQUE_OFFSET + 7]]);
    assert_eq!(id, SPGIST_PAGE_ID);
}

#[test]
fn metapage_roundtrip() {
    let mut page = alloc::vec![0u8; BLCKSZ as usize];
    SpGistInitMetapage(&mut page).unwrap();
    let m = read_meta(&page);
    assert_eq!(m.magicNumber, SPGIST_MAGIC_NUMBER);
    for i in 0..SPGIST_CACHED_PAGES {
        assert_eq!(m.lastUsedPages.cachedPage[i].blkno, InvalidBlockNumber);
    }
    // pd_lower set just past the metadata.
    let pd_lower = u16::from_ne_bytes([page[OFF_PD_LOWER], page[OFF_PD_LOWER + 1]]);
    assert_eq!(
        pd_lower as usize,
        META_OFFSET + core::mem::size_of::<SpGistMetaPageData>()
    );
}

#[test]
fn inner_type_size_byval_and_byref() {
    let byval = SpGistTypeDesc {
        attbyval: true,
        attlen: 8,
        ..SpGistTypeDesc::default()
    };
    assert_eq!(SpGistGetInnerTypeSize(&byval, &Datum::ByVal(42)), 8);

    let fixed = SpGistTypeDesc {
        attbyval: false,
        attlen: 5,
        ..SpGistTypeDesc::default()
    };
    // MAXALIGN(5) = 8.
    let dummy = Datum::ByVal(0);
    assert_eq!(SpGistGetInnerTypeSize(&fixed, &dummy), 8);
}
