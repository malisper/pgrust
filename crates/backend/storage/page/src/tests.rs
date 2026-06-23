use super::*;
use alloc::vec;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use types_storage::bufpage::LP_NORMAL;

/// The `data_checksums_enabled` seam is an `OnceLock` slot that can only be
/// installed once per process, so we install it exactly once pointing at this
/// flag; tests then flip the flag. The mutex serializes the checksum tests so
/// they don't observe each other's flag value.
static SEAM_LOCK: Mutex<()> = Mutex::new(());
static CHECKSUMS_ON: AtomicBool = AtomicBool::new(false);

fn install_seam_once() {
    if !transam_xlog_seams::data_checksums_enabled::is_installed() {
        transam_xlog_seams::data_checksums_enabled::set(|| {
            CHECKSUMS_ON.load(Ordering::SeqCst)
        });
    }
}

fn fresh_page(special: Size) -> [u8; BLCKSZ] {
    let mut bytes = [0_u8; BLCKSZ];
    PageInit(&mut bytes, BLCKSZ, special).unwrap();
    bytes
}

#[test]
fn block_id_and_item_pointer_helpers() {
    let mut blk = BlockIdData::default();
    BlockIdSet(&mut blk, 0x0001_2345);
    assert_eq!(BlockIdGetBlockNumber(&blk), 0x0001_2345);

    let mut left = ItemPointerData::new(7, 9);
    let right = ItemPointerData::new(7, 10);
    assert!(ItemPointerIsValid(Some(&left)));
    assert!(!ItemPointerIsValid(None));
    assert_eq!(ItemPointerGetBlockNumber(&left), 7);
    assert_eq!(ItemPointerGetOffsetNumber(&left), 9);
    assert_eq!(ItemPointerCompare(&left, &right), -1);
    assert_eq!(ItemPointerCompare(&right, &left), 1);
    assert_eq!(ItemPointerCompare(&left, &left), 0);

    ItemPointerInc(&mut left);
    assert!(ItemPointerEquals(&left, &right));
    ItemPointerDec(&mut left);
    assert_eq!(ItemPointerGetOffsetNumber(&left), 9);

    ItemPointerSetMovedPartitions(&mut left);
    assert!(ItemPointerIndicatesMovedPartitions(&left));
    ItemPointerSetInvalid(&mut left);
    assert!(!ItemPointerIsValid(Some(&left)));

    let mut copy = ItemPointerData::default();
    ItemPointerCopy(&right, &mut copy);
    assert!(ItemPointerEquals(&copy, &right));

    let mut p = ItemPointerData::new(1, 1);
    ItemPointerSetBlockNumber(&mut p, 99);
    ItemPointerSetOffsetNumber(&mut p, 5);
    assert_eq!(ItemPointerGetBlockNumber(&p), 99);
    assert_eq!(ItemPointerGetOffsetNumber(&p), 5);
}

#[test]
fn item_pointer_inc_dec_edges() {
    // off == u16::MAX with valid block -> wrap to (blk+1, 0).
    let mut p = ItemPointerData::new(5, u16::MAX);
    ItemPointerInc(&mut p);
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&p), 6);
    assert_eq!(ItemPointerGetOffsetNumberNoCheck(&p), 0);

    // off == 0 with blk != 0 -> (blk-1, u16::MAX).
    let mut q = ItemPointerData::new(6, 0);
    ItemPointerDec(&mut q);
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&q), 5);
    assert_eq!(ItemPointerGetOffsetNumberNoCheck(&q), u16::MAX);

    // Max possible -> Inc is a no-op (InvalidBlockNumber == 0xffffffff).
    let mut max = ItemPointerData::new(InvalidBlockNumber, u16::MAX);
    ItemPointerInc(&mut max);
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&max), InvalidBlockNumber);
    assert_eq!(ItemPointerGetOffsetNumberNoCheck(&max), u16::MAX);

    // Min possible -> Dec is a no-op.
    let mut min = ItemPointerData::new(0, 0);
    ItemPointerDec(&mut min);
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&min), 0);
    assert_eq!(ItemPointerGetOffsetNumberNoCheck(&min), 0);
}

#[test]
fn item_id_round_trip_and_raw_marshal() {
    let mut item = ItemIdData::new(128, LP_NORMAL, 33);
    assert_eq!(ItemIdGetOffset(&item), 128);
    assert_eq!(ItemIdGetFlags(&item), LP_NORMAL);
    assert_eq!(ItemIdGetLength(&item), 33);
    assert!(ItemIdIsUsed(&item));
    assert!(ItemIdIsNormal(&item));
    assert!(ItemIdHasStorage(&item));

    // The on-page raw round-trip must be lossless.
    let raw = item_id_to_raw(&item);
    let back = item_id_from_raw(raw);
    assert_eq!(ItemIdGetOffset(&back), 128);
    assert_eq!(ItemIdGetFlags(&back), LP_NORMAL);
    assert_eq!(ItemIdGetLength(&back), 33);

    ItemIdMarkDead(&mut item);
    assert!(ItemIdIsDead(&item));
    assert_eq!(ItemIdGetLength(&item), 33);
    ItemIdSetUnused(&mut item);
    assert!(!ItemIdIsUsed(&item));
    assert!(!ItemIdHasStorage(&item));

    let mut red = ItemIdData::default();
    ItemIdSetRedirect(&mut red, 42);
    assert!(ItemIdIsRedirected(&red));
    assert_eq!(ItemIdGetRedirect(&red), 42);

    let mut dead = ItemIdData::new(1, LP_NORMAL, 1);
    ItemIdSetDead(&mut dead);
    assert!(ItemIdIsDead(&dead));

    let mut id = ItemIdData::default();
    ItemIdSetNormal(&mut id, 64, 10);
    assert!(ItemIdIsNormal(&id));
    assert_eq!(ItemIdGetOffset(&id), 64);
}

#[test]
fn page_xlog_rec_ptr_round_trip() {
    let mut ptr = PageXLogRecPtr::default();
    PageXLogRecPtrSet(&mut ptr, 0x0123_4567_89ab_cdef);
    assert_eq!(PageXLogRecPtrGet(ptr), 0x0123_4567_89ab_cdef);
}

#[test]
fn page_init_and_layout() {
    let mut bytes = fresh_page(32);
    let page = PageRef::new(&bytes).unwrap();
    assert!(PageIsEmpty(&page));
    assert!(!PageIsNew(&page));
    assert_eq!(PageGetPageSize(&page), BLCKSZ);
    assert_eq!(PageGetPageLayoutVersion(&page), PG_PAGE_LAYOUT_VERSION);
    assert_eq!(PageGetSpecialSize(&page), 32);
    assert_eq!(PageGetMaxOffsetNumber(&page), 0);
    assert_eq!(
        PageGetFreeSpace(&page),
        BLCKSZ - 32 - SizeOfPageHeaderData - ITEM_ID_SIZE
    );
    assert_eq!(PageGetExactFreeSpace(&page), BLCKSZ - 32 - SizeOfPageHeaderData);
    assert_eq!(
        PageGetFreeSpaceForMultipleTuples(&page, 2),
        BLCKSZ - 32 - SizeOfPageHeaderData - 2 * ITEM_ID_SIZE
    );

    // A brand-new (all-zero) page is "new".
    let zero = [0_u8; BLCKSZ];
    assert!(PageIsNew(&PageRef::new(&zero).unwrap()));

    // LSN round-trips through the page header bytes.
    let mut page = PageMut::new(&mut bytes).unwrap();
    PageSetLSN(&mut page, 0xdead_beef_0000_0001);
    assert_eq!(PageGetLSN(&page.as_ref()), 0xdead_beef_0000_0001);
}

#[test]
fn page_flag_and_prune_helpers() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();

    assert!(!PageIsFull(&page.as_ref()));
    PageSetFull(&mut page);
    assert!(PageIsFull(&page.as_ref()));
    PageClearFull(&mut page);
    assert!(!PageIsFull(&page.as_ref()));

    assert!(!PageIsAllVisible(&page.as_ref()));
    PageSetAllVisible(&mut page);
    assert!(PageIsAllVisible(&page.as_ref()));
    PageClearAllVisible(&mut page);
    assert!(!PageIsAllVisible(&page.as_ref()));

    assert!(!PageHasFreeLinePointers(&page.as_ref()));
    PageSetHasFreeLinePointers(&mut page);
    assert!(PageHasFreeLinePointers(&page.as_ref()));
    PageClearHasFreeLinePointers(&mut page);
    assert!(!PageHasFreeLinePointers(&page.as_ref()));

    PageSetPrunable(&mut page, 100);
    PageSetPrunable(&mut page, 50); // smaller wins
    PageSetPrunable(&mut page, 200); // larger ignored
    assert_eq!(page.pd_prune_xid(), 50);
    PageClearPrunable(&mut page);
    assert_eq!(page.pd_prune_xid(), 0);
}

#[test]
fn add_item_and_get_item() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();

    let off = PageAddItemExtended(&mut page, b"abcdef", INVALID_OFFSET_NUMBER, 0).unwrap();
    assert_eq!(off, FIRST_OFFSET_NUMBER);
    assert_eq!(PageGetMaxOffsetNumber(&page.as_ref()), 1);

    let item_id = PageGetItemId(&page.as_ref(), off).unwrap();
    assert_eq!(ItemIdGetLength(&item_id), 6);
    assert_eq!(PageGetItem(&page.as_ref(), &item_id).unwrap(), b"abcdef");

    // Special pointer & contents bounds.
    assert!(PageGetSpecialPointer(&page.as_ref()).is_ok());
    assert!(PageGetContents(&page.as_ref()).is_ok());
}

#[test]
fn add_item_overwrite_and_shuffle() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    PageAddItemExtended(&mut page, b"1111", INVALID_OFFSET_NUMBER, 0).unwrap();
    PageAddItemExtended(&mut page, b"2222", INVALID_OFFSET_NUMBER, 0).unwrap();

    // Insert at offset 2 with shuffle (no PAI_OVERWRITE) -> existing #2 -> #3.
    let off = PageAddItemExtended(&mut page, b"XXXX", 2, 0).unwrap();
    assert_eq!(off, 2);
    assert_eq!(PageGetMaxOffsetNumber(&page.as_ref()), 3);
    let id2 = PageGetItemId(&page.as_ref(), 2).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id2).unwrap(), b"XXXX");
    let id3 = PageGetItemId(&page.as_ref(), 3).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id3).unwrap(), b"2222");
}

#[test]
fn index_delete_overwrite_and_multidelete() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    let a = PageAddItemExtended(&mut page, b"aaaa", INVALID_OFFSET_NUMBER, 0).unwrap();
    let b = PageAddItemExtended(&mut page, b"bbbbbbbb", INVALID_OFFSET_NUMBER, 0).unwrap();
    let c = PageAddItemExtended(&mut page, b"cccc", INVALID_OFFSET_NUMBER, 0).unwrap();
    assert_eq!((a, b, c), (1, 2, 3));

    // Overwrite #2 with a smaller tuple, then verify all are intact.
    assert!(PageIndexTupleOverwrite(&mut page, b, b"BBBB").unwrap());
    let id = PageGetItemId(&page.as_ref(), b).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id).unwrap(), b"BBBB");
    let ida = PageGetItemId(&page.as_ref(), a).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &ida).unwrap(), b"aaaa");
    let idc = PageGetItemId(&page.as_ref(), c).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &idc).unwrap(), b"cccc");

    // Delete #1 (compacts out the line pointer).
    PageIndexTupleDelete(&mut page, a).unwrap();
    assert_eq!(PageGetMaxOffsetNumber(&page.as_ref()), 2);
    let id1 = PageGetItemId(&page.as_ref(), 1).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id1).unwrap(), b"BBBB");
    let id2 = PageGetItemId(&page.as_ref(), 2).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id2).unwrap(), b"cccc");
}

#[test]
fn overwrite_larger_keeps_data() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    PageAddItemExtended(&mut page, b"aaaa", INVALID_OFFSET_NUMBER, 0).unwrap();
    let b = PageAddItemExtended(&mut page, b"bb", INVALID_OFFSET_NUMBER, 0).unwrap();
    PageAddItemExtended(&mut page, b"cccc", INVALID_OFFSET_NUMBER, 0).unwrap();

    // Grow #2 from 2 to 6 bytes; data before it shifts down.
    assert!(PageIndexTupleOverwrite(&mut page, b, b"BBBBBB").unwrap());
    let idb = PageGetItemId(&page.as_ref(), b).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &idb).unwrap(), b"BBBBBB");
    let id1 = PageGetItemId(&page.as_ref(), 1).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id1).unwrap(), b"aaaa");
    let id3 = PageGetItemId(&page.as_ref(), 3).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id3).unwrap(), b"cccc");
}

#[test]
fn multidelete_many_items() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    for i in 0..6_u8 {
        let data = [b'a' + i; 4];
        PageAddItemExtended(&mut page, &data, INVALID_OFFSET_NUMBER, 0).unwrap();
    }
    assert_eq!(PageGetMaxOffsetNumber(&page.as_ref()), 6);

    // Delete items 2, 4, 5 (>2 -> goes through the multi-delete core).
    PageIndexMultiDelete(&mut page, &[2, 4, 5]).unwrap();
    assert_eq!(PageGetMaxOffsetNumber(&page.as_ref()), 3);
    let kept: Vec<Vec<u8>> = (1..=3)
        .map(|o| {
            let id = PageGetItemId(&page.as_ref(), o).unwrap();
            PageGetItem(&page.as_ref(), &id).unwrap().to_vec()
        })
        .collect();
    assert_eq!(kept, vec![b"aaaa".to_vec(), b"cccc".to_vec(), b"ffff".to_vec()]);
}

#[test]
fn delete_no_compact_marks_unused() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    PageAddItemExtended(&mut page, b"aaaa", INVALID_OFFSET_NUMBER, 0).unwrap();
    PageAddItemExtended(&mut page, b"bbbb", INVALID_OFFSET_NUMBER, 0).unwrap();
    PageAddItemExtended(&mut page, b"cccc", INVALID_OFFSET_NUMBER, 0).unwrap();

    // Delete the middle without compaction -> line pointer 2 becomes unused.
    PageIndexTupleDeleteNoCompact(&mut page, 2).unwrap();
    assert_eq!(PageGetMaxOffsetNumber(&page.as_ref()), 3);
    assert!(!PageGetItemId(&page.as_ref(), 2).unwrap().is_used());
    let id1 = PageGetItemId(&page.as_ref(), 1).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id1).unwrap(), b"aaaa");
    let id3 = PageGetItemId(&page.as_ref(), 3).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id3).unwrap(), b"cccc");

    // Delete the last -> line pointer is zapped (array shrinks).
    PageIndexTupleDeleteNoCompact(&mut page, 3).unwrap();
    assert_eq!(PageGetMaxOffsetNumber(&page.as_ref()), 2);
}

#[test]
fn repair_fragmentation_removes_trailing_unused() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    PageAddItemExtended(&mut page, b"aaaa", INVALID_OFFSET_NUMBER, 0).unwrap();
    PageAddItemExtended(&mut page, b"bbbb", INVALID_OFFSET_NUMBER, 0).unwrap();
    PageAddItemExtended(&mut page, b"cccc", INVALID_OFFSET_NUMBER, 0).unwrap();

    // Mark items 2 and 3 unused, then repair.
    for o in [2, 3] {
        let mut id = PageGetItemId(&page.as_ref(), o).unwrap();
        id.set_unused();
        page.set_item_id(o, id).unwrap();
    }

    PageRepairFragmentation(&mut page).unwrap();
    assert_eq!(PageGetMaxOffsetNumber(&page.as_ref()), 1);
    assert!(!PageHasFreeLinePointers(&page.as_ref()));
    let id1 = PageGetItemId(&page.as_ref(), 1).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id1).unwrap(), b"aaaa");
}

#[test]
fn repair_fragmentation_keeps_middle_hole() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    PageAddItemExtended(&mut page, b"aaaa", INVALID_OFFSET_NUMBER, 0).unwrap();
    PageAddItemExtended(&mut page, b"bbbb", INVALID_OFFSET_NUMBER, 0).unwrap();
    PageAddItemExtended(&mut page, b"cccc", INVALID_OFFSET_NUMBER, 0).unwrap();

    // Mark only the middle unused -> the array is NOT truncated, hint set.
    let mut id = PageGetItemId(&page.as_ref(), 2).unwrap();
    id.set_unused();
    page.set_item_id(2, id).unwrap();

    PageRepairFragmentation(&mut page).unwrap();
    assert_eq!(PageGetMaxOffsetNumber(&page.as_ref()), 3);
    assert!(PageHasFreeLinePointers(&page.as_ref()));
    assert!(!PageGetItemId(&page.as_ref(), 2).unwrap().is_used());
    let id3 = PageGetItemId(&page.as_ref(), 3).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id3).unwrap(), b"cccc");
    let id1 = PageGetItemId(&page.as_ref(), 1).unwrap();
    assert_eq!(PageGetItem(&page.as_ref(), &id1).unwrap(), b"aaaa");
}

#[test]
fn repair_fragmentation_corrupt_offset_errors() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    for i in 0..6_u8 {
        let data = [b'a' + i; 4];
        PageAddItemExtended(&mut page, &data, INVALID_OFFSET_NUMBER, 0).unwrap();
    }
    // Point a kept tuple's offset below pd_upper to trip "corrupted line pointer".
    let mut id = PageGetItemId(&page.as_ref(), 4).unwrap();
    id.set_storage(0, id.lp_len());
    page.set_item_id(4, id).unwrap();
    assert!(PageRepairFragmentation(&mut page).is_err());
}

#[test]
fn index_multi_delete_out_of_order_errors() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    for i in 0..6_u8 {
        let data = [b'a' + i; 4];
        PageAddItemExtended(&mut page, &data, INVALID_OFFSET_NUMBER, 0).unwrap();
    }
    // Out-of-order itemnos[] hits the "incorrect index offsets supplied" path.
    assert!(PageIndexMultiDelete(&mut page, &[5, 4, 2]).is_err());
}

#[test]
fn truncate_line_pointer_array() {
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    PageAddItemExtended(&mut page, b"aaaa", INVALID_OFFSET_NUMBER, 0).unwrap();
    PageAddItemExtended(&mut page, b"bbbb", INVALID_OFFSET_NUMBER, 0).unwrap();
    PageAddItemExtended(&mut page, b"cccc", INVALID_OFFSET_NUMBER, 0).unwrap();

    // Mark the two trailing line pointers unused, then truncate.
    for o in [2, 3] {
        let mut id = PageGetItemId(&page.as_ref(), o).unwrap();
        id.set_unused();
        page.set_item_id(o, id).unwrap();
    }
    PageTruncateLinePointerArray(&mut page);
    assert_eq!(PageGetMaxOffsetNumber(&page.as_ref()), 1);
    assert!(!PageHasFreeLinePointers(&page.as_ref()));
}

#[test]
fn temp_page_round_trip() {
    let mut bytes = fresh_page(16);
    let mut page = PageMut::new(&mut bytes).unwrap();
    PageAddItemExtended(&mut page, b"hello", INVALID_OFFSET_NUMBER, 0).unwrap();

    let temp = PageGetTempPageCopy(&page.as_ref()).unwrap();
    assert_eq!(temp.as_bytes().len(), BLCKSZ);

    // CopySpecial: special region is copied, body is freshly PageInit'd.
    let special = PageGetTempPageCopySpecial(&page.as_ref()).unwrap();
    let special_ref = PageRef::new(special.as_bytes()).unwrap();
    assert_eq!(PageGetSpecialSize(&special_ref), 16);

    // A plain (uninitialized) temp page is just BLCKSZ scratch bytes.
    let plain = PageGetTempPage(&page.as_ref()).unwrap();
    assert_eq!(plain.as_bytes().len(), BLCKSZ);

    // Restore the copy onto a fresh page.
    let mut dst = [0_u8; BLCKSZ];
    let mut dst_page = PageMut::new(&mut dst).unwrap();
    PageRestoreTempPage(temp, &mut dst_page).unwrap();
    let id = PageGetItemId(&dst_page.as_ref(), 1).unwrap();
    assert_eq!(PageGetItem(&dst_page.as_ref(), &id).unwrap(), b"hello");
}

#[test]
fn verify_zero_and_initialized_pages() {
    let _guard = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_seam_once();
    CHECKSUMS_ON.store(false, Ordering::SeqCst);
    let zero = [0_u8; BLCKSZ];
    let page = PageRef::new(&zero).unwrap();
    assert_eq!(PageIsVerified(&page, 0, 0).unwrap(), (true, false));

    let bytes = fresh_page(0);
    let page = PageRef::new(&bytes).unwrap();
    assert_eq!(PageIsVerified(&page, 1, 0).unwrap(), (true, false));
}

#[test]
fn verify_with_checksums() {
    let _guard = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_seam_once();
    CHECKSUMS_ON.store(true, Ordering::SeqCst);
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    PageSetChecksumInplace(&mut page, 7);
    assert_eq!(PageIsVerified(&page.as_ref(), 7, 0).unwrap(), (true, false));

    // Corrupt a body byte (not the header) -> checksum mismatch.
    bytes[100] ^= 0xff;
    let page = PageRef::new(&bytes).unwrap();
    let (verified, failure) = PageIsVerified(&page, 7, 0).unwrap();
    assert!(!verified);
    assert!(failure);
    CHECKSUMS_ON.store(false, Ordering::SeqCst);
}

#[test]
fn set_checksum_copy_when_enabled() {
    let _guard = SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_seam_once();
    CHECKSUMS_ON.store(true, Ordering::SeqCst);
    let mut bytes = fresh_page(0);
    let mut page = PageMut::new(&mut bytes).unwrap();
    PageAddItemExtended(&mut page, b"zz", INVALID_OFFSET_NUMBER, 0).unwrap();

    let copy = PageSetChecksumCopy(&page.as_ref(), 3).unwrap();
    let copy_ref = PageRef::new(copy.as_bytes()).unwrap();
    // The copy must verify against block 3.
    assert_eq!(PageIsVerified(&copy_ref, 3, 0).unwrap(), (true, false));
    CHECKSUMS_ON.store(false, Ordering::SeqCst);
}
