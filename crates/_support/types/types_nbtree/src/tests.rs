use super::*;
use ::mcx::MemoryContext;
use ::types_core::{InvalidBlockNumber, InvalidBuffer};
use ::types_tuple::itemptr::ItemPointerData;

#[test]
fn page_constants_match_c() {
    assert_eq!(BTP_LEAF, 1);
    assert_eq!(BTP_ROOT, 2);
    assert_eq!(BTP_DELETED, 4);
    assert_eq!(BTP_META, 8);
    assert_eq!(BTP_HALF_DEAD, 16);
    assert_eq!(BTP_SPLIT_END, 32);
    assert_eq!(BTP_HAS_GARBAGE, 64);
    assert_eq!(BTP_INCOMPLETE_SPLIT, 128);
    assert_eq!(BTP_HAS_FULLXID, 256);
    assert_eq!(MAX_BT_CYCLE_ID, 0xFF7F);
    assert_eq!(BTREE_METAPAGE, 0);
    assert_eq!(BTREE_MAGIC, 0x053162);
    assert_eq!(BTREE_VERSION, 4);
    assert_eq!(BTREE_MIN_VERSION, 2);
    assert_eq!(BTREE_NOVAC_VERSION, 3);
    assert_eq!(BTMaxItemSize, 2704);
    assert_eq!(BTMaxItemSizeNoHeapTid, 2712);
    assert_eq!(MaxTIDsPerBTreePage, 1358);
    assert_eq!(BTREE_MIN_FILLFACTOR, 10);
    assert_eq!(BTREE_DEFAULT_FILLFACTOR, 90);
    assert_eq!(BTREE_NONLEAF_FILLFACTOR, 70);
    assert_eq!(BTREE_SINGLEVAL_FILLFACTOR, 96);
    assert_eq!(P_NONE, 0);
    assert_eq!(P_HIKEY, 1);
    assert_eq!(P_FIRSTKEY, 2);
    assert_eq!(INDEX_ALT_TID_MASK, 0x2000);
    assert_eq!(BT_OFFSET_MASK, 0x0FFF);
    assert_eq!(BT_STATUS_OFFSET_MASK, 0xF000);
    assert_eq!(BT_PIVOT_HEAP_TID_ATTR, 0x1000);
    assert_eq!(BT_IS_POSTING, 0x2000);
    assert_eq!(BTORDER_PROC, 1);
    assert_eq!(BTSORTSUPPORT_PROC, 2);
    assert_eq!(BTINRANGE_PROC, 3);
    assert_eq!(BTEQUALIMAGE_PROC, 4);
    assert_eq!(BTOPTIONS_PROC, 5);
    assert_eq!(BTSKIPSUPPORT_PROC, 6);
    assert_eq!(BTNProcs, 6);
    assert_eq!(BT_READ, 1);
    assert_eq!(BT_WRITE, 2);
}

#[test]
fn page_predicates() {
    let mut op = BTPageOpaqueData::default();
    assert!(P_LEFTMOST(&op));
    assert!(P_RIGHTMOST(&op));
    assert_eq!(P_FIRSTDATAKEY(&op), P_HIKEY);
    op.btpo_next = 7;
    assert!(!P_RIGHTMOST(&op));
    assert_eq!(P_FIRSTDATAKEY(&op), P_FIRSTKEY);
    op.btpo_flags = BTP_LEAF | BTP_HALF_DEAD;
    assert!(P_ISLEAF(&op));
    assert!(!P_ISROOT(&op));
    assert!(P_ISHALFDEAD(&op));
    assert!(P_IGNORE(&op));
    assert!(!P_ISDELETED(&op));
    op.btpo_flags = BTP_DELETED | BTP_HAS_FULLXID;
    assert!(P_IGNORE(&op));
    assert!(P_HAS_FULLXID(&op));
    assert!(!P_HAS_GARBAGE(&op));
    assert!(!P_INCOMPLETE_SPLIT(&op));
    assert!(!P_ISMETA(&op));
}

#[test]
fn commute_strategy() {
    use ::types_scan::scankey::*;
    assert_eq!(
        BTCommuteStrategyNumber(BTLessStrategyNumber),
        BTGreaterStrategyNumber
    );
    assert_eq!(
        BTCommuteStrategyNumber(BTLessEqualStrategyNumber),
        BTGreaterEqualStrategyNumber
    );
    assert_eq!(
        BTCommuteStrategyNumber(BTEqualStrategyNumber),
        BTEqualStrategyNumber
    );
}

#[test]
fn scan_opaque_alloc_and_pos() {
    let ctx = MemoryContext::new("t");
    let mut so = BTScanOpaqueData::alloc_in(ctx.mcx()).unwrap();
    assert!(!so.qual_ok);
    assert_eq!(so.numberOfKeys, 0);
    assert_eq!(so.markItemIndex, -1);
    assert!(so.keyData.is_empty());
    assert!(so.currTuples.is_none());
    assert!(!BTScanPosIsPinned(&so.currPos));
    assert!(!BTScanPosIsValid(&so.currPos));
    assert!(!BTScanPosIsValid(&so.markPos));

    so.currPos.buf = 3;
    so.currPos.currPage = 42;
    assert!(BTScanPosIsPinned(&so.currPos));
    assert!(BTScanPosIsValid(&so.currPos));

    let item = BTScanPosItem {
        heapTid: ItemPointerData::new(9, 4),
        indexOffset: 11,
        tupleOffset: 0,
    };
    so.currPos.firstItem = 0;
    so.currPos.lastItem = 0;
    so.currPos.set_item(0, item);
    // SAFETY: slot 0 written above, within [firstItem, lastItem].
    assert_eq!(unsafe { so.currPos.item(0) }, item);

    BTScanPosInvalidate(&mut so.currPos);
    assert_eq!(so.currPos.buf, InvalidBuffer);
    assert_eq!(so.currPos.currPage, InvalidBlockNumber);
    assert!(!BTScanPosIsPinned(&so.currPos));
    assert!(!BTScanPosIsValid(&so.currPos));
}

#[test]
fn stack_walk() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let root = ::mcx::leak_in(::mcx::alloc_in(
        mcx,
        BTStackData {
            bts_blkno: 1,
            bts_offset: 2,
            bts_parent: None,
        },
    )
    .unwrap());
    let mut leaf = BTStackData {
        bts_blkno: 10,
        bts_offset: 3,
        bts_parent: Some(root),
    };
    leaf.bts_parent.as_mut().unwrap().bts_offset = 5;
    let mut depth = 0;
    let mut cur = Some(&mut leaf);
    while let Some(s) = cur {
        depth += 1;
        cur = s.bts_parent.as_deref_mut();
    }
    assert_eq!(depth, 2);
}
