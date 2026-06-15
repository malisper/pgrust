//! Smoke tests for the SP-GiST carrier vocabulary: constants, page-number
//! helpers, GBUF flag helpers, and the on-disk bit-field accessors/setters.

use super::*;

#[test]
fn support_proc_numbers() {
    assert_eq!(SPGIST_CONFIG_PROC, 1);
    assert_eq!(SPGIST_CHOOSE_PROC, 2);
    assert_eq!(SPGIST_PICKSPLIT_PROC, 3);
    assert_eq!(SPGIST_INNER_CONSISTENT_PROC, 4);
    assert_eq!(SPGIST_LEAF_CONSISTENT_PROC, 5);
    assert_eq!(SPGIST_COMPRESS_PROC, 6);
    assert_eq!(SPGIST_OPTIONS_PROC, 7);
    assert_eq!(SPGISTNRequiredProc, 5);
    assert_eq!(SPGISTNProc, 7);
}

#[test]
fn fixed_pages_and_magic() {
    assert_eq!(SPGIST_METAPAGE_BLKNO, 0);
    assert_eq!(SPGIST_ROOT_BLKNO, 1);
    assert_eq!(SPGIST_NULL_BLKNO, 2);
    assert_eq!(SPGIST_LAST_FIXED_BLKNO, 2);

    assert!(SpGistBlockIsRoot(SPGIST_ROOT_BLKNO));
    assert!(SpGistBlockIsRoot(SPGIST_NULL_BLKNO));
    assert!(!SpGistBlockIsRoot(SPGIST_METAPAGE_BLKNO));
    assert!(!SpGistBlockIsRoot(3));

    assert!(SpGistBlockIsFixed(0));
    assert!(SpGistBlockIsFixed(2));
    assert!(!SpGistBlockIsFixed(3));

    assert_eq!(SPGIST_MAGIC_NUMBER, 0xBA0B_ABEE);
    assert_eq!(SPGIST_PAGE_ID, 0xFF82);
    assert_eq!(SPGIST_CACHED_PAGES, 8);
}

#[test]
fn page_flag_bits() {
    assert_eq!(SPGIST_META, 1);
    assert_eq!(SPGIST_DELETED, 2);
    assert_eq!(SPGIST_LEAF, 4);
    assert_eq!(SPGIST_NULLS, 8);
}

#[test]
fn tupstate_values() {
    assert_eq!(SPGIST_LIVE, 0);
    assert_eq!(SPGIST_REDIRECT, 1);
    assert_eq!(SPGIST_DEAD, 2);
    assert_eq!(SPGIST_PLACEHOLDER, 3);
}

#[test]
fn gbuf_flag_helpers() {
    assert_eq!(GBUF_LEAF, 0x03);
    assert_eq!(GBUF_NULLS, 0x04);
    assert!(GBUF_REQ_LEAF(GBUF_LEAF));
    assert!(GBUF_REQ_LEAF(GBUF_LEAF | GBUF_NULLS));
    assert!(!GBUF_REQ_LEAF(0x01));
    assert!(GBUF_REQ_NULLS(GBUF_NULLS));
    assert!(!GBUF_REQ_NULLS(GBUF_LEAF));
    // GBUF_INNER_PARITY(x) == x % 3
    assert_eq!(GBUF_INNER_PARITY(7), 1);
    assert_eq!(GBUF_INNER_PARITY(9), 0);
}

#[test]
fn inner_tuple_bitfields() {
    let mut t = SpGistInnerTupleData::default();
    t.set_tupstate(SPGIST_REDIRECT);
    t.set_allTheSame(true);
    t.set_nNodes(0x1234);
    t.set_prefixSize(0xABCD);
    assert_eq!(t.tupstate(), SPGIST_REDIRECT);
    assert!(t.allTheSame());
    assert_eq!(t.nNodes(), 0x1234);
    assert_eq!(t.prefixSize(), 0xABCD);

    // max values fit the bit fields.
    let mut m = SpGistInnerTupleData::default();
    m.set_nNodes(SGITMAXNNODES);
    m.set_prefixSize(SGITMAXPREFIXSIZE);
    assert_eq!(m.nNodes(), SGITMAXNNODES);
    assert_eq!(m.prefixSize(), SGITMAXPREFIXSIZE);

    // setting one field does not disturb the others.
    let mut n = SpGistInnerTupleData::default();
    n.set_prefixSize(0xFFFF);
    n.set_tupstate(SPGIST_DEAD);
    assert_eq!(n.prefixSize(), 0xFFFF);
    assert_eq!(n.tupstate(), SPGIST_DEAD);
    assert_eq!(n.nNodes(), 0);
    assert!(!n.allTheSame());
}

#[test]
fn leaf_tuple_bitfields() {
    let mut t = SpGistLeafTupleData::default();
    t.set_tupstate(SPGIST_DEAD);
    t.set_size(0x1234_5678 & 0x3FFF_FFFF);
    assert_eq!(t.tupstate(), SPGIST_DEAD);
    assert_eq!(t.size(), 0x1234_5678 & 0x3FFF_FFFF);

    t.set_nextOffset(0x2ABC);
    t.set_hasNullMask(true);
    assert_eq!(t.get_nextOffset(), 0x2ABC);
    assert!(t.get_hasNullMask());

    t.set_hasNullMask(false);
    assert!(!t.get_hasNullMask());
    assert_eq!(t.get_nextOffset(), 0x2ABC);
}

#[test]
fn dead_tuple_bitfields() {
    let mut t = SpGistDeadTupleData::default();
    t.set_tupstate(SPGIST_PLACEHOLDER);
    t.set_size(42);
    assert_eq!(t.tupstate(), SPGIST_PLACEHOLDER);
    assert_eq!(t.size(), 42);
}

#[test]
fn choose_out_result_type() {
    let out = spgChooseOut {
        result: spgChooseOutResult::AddNode(spgChooseOutAddNode {
            nodeLabel: Datum::default(),
            nodeN: 3,
        }),
    };
    assert_eq!(out.resultType(), spgChooseResultType::spgAddNode);
    assert_eq!(spgChooseResultType::spgMatchNode as i32, 1);
    assert_eq!(spgChooseResultType::spgAddNode as i32, 2);
    assert_eq!(spgChooseResultType::spgSplitTuple as i32, 3);
}

#[test]
fn picksplit_in_ntuples() {
    let pin = spgPickSplitIn {
        datums: alloc::vec![Datum::default(), Datum::default(), Datum::default()],
        level: 0,
    };
    assert_eq!(pin.nTuples(), 3);
}

#[test]
fn reloption_defaults() {
    assert_eq!(SPGIST_MIN_FILLFACTOR, 10);
    assert_eq!(SPGIST_DEFAULT_FILLFACTOR, 80);
    assert_eq!(spgKeyColumn, 0);
    assert_eq!(spgFirstIncludeColumn, 1);
}

#[test]
fn cache_and_lup_defaults() {
    let cache = SpGistCache::default();
    assert_eq!(cache.config.prefixType, 0);
    assert_eq!(cache.lastUsedPages.cachedPage.len(), SPGIST_CACHED_PAGES);
    let meta = SpGistMetaPageData::default();
    assert_eq!(meta.magicNumber, 0);
}
