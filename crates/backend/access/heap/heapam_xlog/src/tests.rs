//! Unit tests over the pure in-crate logic of `heapam_xlog.c`:
//! `fix_infomask_from_infobits`, the header predicates, and the freeze applier.

use super::*;
use types_tuple::heaptuple::{
    HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY,
};

#[test]
fn fix_infomask_clears_then_sets() {
    // Start with stale xmax/lock bits + KEYS_UPDATED set; they must be cleared.
    let mut im = HEAP_XMAX_IS_MULTI | HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;
    let mut im2 = HEAP_KEYS_UPDATED;
    // infobits == 0 -> all the xmax/lock + keys-updated bits cleared.
    fix_infomask_from_infobits(0, &mut im, &mut im2);
    assert_eq!(im & HEAP_XMAX_IS_MULTI, 0);
    assert_eq!(im & HEAP_XMAX_LOCK_ONLY, 0);
    assert_eq!(im & HEAP_XMAX_KEYSHR_LOCK, 0);
    assert_eq!(im & HEAP_XMAX_EXCL_LOCK, 0);
    assert_eq!(im2 & HEAP_KEYS_UPDATED, 0);
}

#[test]
fn fix_infomask_sets_each_bit() {
    let mut im = 0u16;
    let mut im2 = 0u16;
    fix_infomask_from_infobits(
        XLHL_XMAX_IS_MULTI | XLHL_XMAX_LOCK_ONLY | XLHL_XMAX_EXCL_LOCK | XLHL_XMAX_KEYSHR_LOCK
            | XLHL_KEYS_UPDATED,
        &mut im,
        &mut im2,
    );
    assert_ne!(im & HEAP_XMAX_IS_MULTI, 0);
    assert_ne!(im & HEAP_XMAX_LOCK_ONLY, 0);
    assert_ne!(im & HEAP_XMAX_EXCL_LOCK, 0);
    assert_ne!(im & HEAP_XMAX_KEYSHR_LOCK, 0);
    assert_ne!(im2 & HEAP_KEYS_UPDATED, 0);
}

#[test]
fn xmin_frozen_predicate() {
    let ctx = MemoryContext::new("t");
    let mut hdr = HeapTupleHeaderData::read_on_page(ctx.mcx(), &[0u8; 23]).unwrap();
    assert!(!HeapTupleHeaderXminFrozen(&hdr));
    hdr.t_infomask = HEAP_XMIN_FROZEN;
    assert!(HeapTupleHeaderXminFrozen(&hdr));
}

#[test]
fn speculative_predicate() {
    let ctx = MemoryContext::new("t");
    let mut hdr = HeapTupleHeaderData::read_on_page(ctx.mcx(), &[0u8; 23]).unwrap();
    assert!(!HeapTupleHeaderIsSpeculative(&hdr));
    hdr.t_ctid.ip_posid = SpecTokenOffsetNumber;
    assert!(HeapTupleHeaderIsSpeculative(&hdr));
}

#[test]
fn freeze_tuple_sets_xmax_and_masks() {
    let ctx = MemoryContext::new("t");
    let mut hdr = HeapTupleHeaderData::read_on_page(ctx.mcx(), &[0u8; 23]).unwrap();
    let frz = HeapTupleFreeze {
        xmax: 0,
        t_infomask2: 0x0007,
        t_infomask: HEAP_XMIN_FROZEN,
        frzflags: 0,
        checkflags: 0,
        offset: 0,
    };
    heap_execute_freeze_tuple(&mut hdr, &frz);
    assert_eq!(hdr.t_infomask, HEAP_XMIN_FROZEN);
    assert_eq!(hdr.t_infomask2, 0x0007);
    if let HeapTupleHeaderChoice::THeap(f) = &hdr.t_choice {
        assert_eq!(f.t_xmax, 0);
    } else {
        panic!("expected THeap arm");
    }
}

#[test]
fn build_item_roundtrips_header_and_data() {
    let ctx = MemoryContext::new("t");
    let xlhdr = xl_heap_header { t_infomask2: 0x1234, t_infomask: 0x5678, t_hoff: 24 };
    let mut hdr = new_tuple_header(ctx.mcx(), &xlhdr);
    HeapTupleHeaderSetXmin(&mut hdr, 42);
    let userdata = [9u8, 8, 7, 6, 5];
    let item = build_item(&hdr, &userdata).unwrap();
    assert_eq!(item.len(), SizeofHeapTupleHeader + userdata.len());
    assert_eq!(&item[SizeofHeapTupleHeader..], &userdata);
    // header readback
    let back = HeapTupleHeaderData::read_on_page(ctx.mcx(), &item).unwrap();
    assert_eq!(back.t_infomask2, 0x1234);
    assert_eq!(back.t_infomask, 0x5678);
    assert_eq!(back.t_hoff, 24);
}
