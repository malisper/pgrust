use ::gin_vocab::*;
use ::mcx::MemoryContext;
use ::types_tuple::itemptr::ItemPointerData;

use crate::postinglist::*;

fn tid(blk: u32, off: u16) -> ItemPointerData {
    ItemPointerData::new(blk, off)
}

#[test]
fn posting_list_roundtrip() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let items: Vec<ItemPointerData> = (1..300u32)
        .flat_map(|b| [tid(b, 1), tid(b, 7), tid(b, 291)])
        .collect();
    let (img, n) = ginCompressPostingList(mcx, &items, 8192).unwrap();
    assert_eq!(n, items.len());
    let mut out = mcx::vec_new_in(mcx);
    ginPostingListDecodeAllSegments(&img, &mut out).unwrap();
    assert_eq!(out.as_slice(), items.as_slice());
}

#[test]
fn posting_list_byte_exact_vs_c_layout() {
    // C image: first ItemPointerData {bi_hi=0, bi_lo=1, posid=2} raw, then
    // varbyte deltas of ((blk<<11)|off) words:
    //   (1,2)->(1,3): delta 1 -> 0x01
    //   (1,3)->(2,1): (2<<11|1)-(1<<11|3) = 2046 -> 0xFE 0x0F
    let items = [tid(1, 2), tid(1, 3), tid(2, 1)];
    let ctx = MemoryContext::new_bump("t");
    let (img, n) = ginCompressPostingList(ctx.mcx(), &items, 8192).unwrap();
    assert_eq!(n, 3);
    let expect: &[u8] = &[
        0, 0, // bi_hi
        1, 0, // bi_lo
        2, 0, // posid
        3, 0, // nbytes
        0x01, 0xFE, 0x0F, // varbyte deltas
        0x00, // SHORTALIGN zero pad
    ];
    assert_eq!(img.as_slice(), expect);
    assert_eq!(size_of_gin_posting_list(3), img.len());
}

#[test]
fn posting_list_truncates_at_maxsize() {
    let ctx = MemoryContext::new_bump("t");
    let items: Vec<ItemPointerData> = (1..2000u32).map(|b| tid(b, 1)).collect();
    let (img, n) = ginCompressPostingList(ctx.mcx(), &items, 32).unwrap();
    assert!(n < items.len() && n > 1);
    assert!(img.len() <= 32);
    let mut out = mcx::vec_new_in(ctx.mcx());
    ginPostingListDecodeAllSegments(&img, &mut out).unwrap();
    assert_eq!(out.as_slice(), &items[..n]);
}

#[test]
fn merge_item_pointers_dedups() {
    let ctx = MemoryContext::new_bump("t");
    let a = [tid(1, 1), tid(2, 2), tid(5, 5)];
    let b = [tid(2, 2), tid(3, 3)];
    let m = ginMergeItemPointers(ctx.mcx(), &a, &b).unwrap();
    assert_eq!(
        m.as_slice(),
        &[tid(1, 1), tid(2, 2), tid(3, 3), tid(5, 5)]
    );
    // Disjoint fast paths.
    let m = ginMergeItemPointers(ctx.mcx(), &a[..1], &b).unwrap();
    assert_eq!(m.as_slice(), &[tid(1, 1), tid(2, 2), tid(3, 3)]);
    let m = ginMergeItemPointers(ctx.mcx(), &b, &a[2..]).unwrap();
    assert_eq!(m.as_slice(), &[tid(2, 2), tid(3, 3), tid(5, 5)]);
}

#[test]
fn item_pointer_sentinels_order() {
    let mut min = tid(0, 0);
    item_pointer_set_min(&mut min);
    let mut max = tid(0, 0);
    item_pointer_set_max(&mut max);
    let mut lossy = tid(0, 0);
    item_pointer_set_lossy_page(&mut lossy, 7);
    let exact = tid(7, 100);

    assert!(ginCompareItemPointers(&min, &exact) < 0);
    assert!(ginCompareItemPointers(&exact, &lossy) < 0);
    assert!(ginCompareItemPointers(&lossy, &max) < 0);
    assert!(item_pointer_is_lossy_page(&lossy));
    assert!(!item_pointer_is_lossy_page(&exact));
    assert!(item_pointer_is_min(&min));
}

#[test]
fn wal_record_image_sizes_match_c() {
    assert_eq!(core::mem::size_of::<GinMetaPageData>(), 56);
    assert_eq!(core::mem::size_of::<GinPageOpaqueData>(), 8);
    assert_eq!(core::mem::size_of::<PostingItem>(), 10);
    // sizeof(ginxlogSplit) == 28, sizeof(ginxlogUpdateMeta) == 88,
    // sizeof(ginxlogDeleteListPages) == 64 (asserted by the array types in
    // wal.rs signatures at compile time).
    assert_eq!(GinMaxItemSize, 2712);
    assert_eq!(GinDataPageMaxDataSize, 8192 - 24 - 8 - 8);
    assert_eq!(GinListPageSize, 8192 - 24 - 8);
}

fn one_col_state(col: GinColState) -> GinState {
    let mut cols = [col; GIN_MAX_KEY_COLS];
    cols[0] = col;
    GinState { natts: 1, one_col: true, cols }
}

#[test]
fn build_accumulator_dump_order_and_tids() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let state = one_col_state(GinColState {
        opclass: GinOpclass::JsonbOps,
        elem_cmp: GinElemCmp::None,
        support_collation: 100,
        can_partial_match: false,
        key_byval: false,
        key_len: -1,
    });
    // Keys as 4-byte-header text images (jsonb_ops key form).
    fn key(mcx: ::mcx::Mcx<'_>, s: &[u8]) -> ::datum::Datum {
        let total = 4 + s.len();
        let mut v: ::mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, total).unwrap();
        mcx::vec_append_bytes(
            &mut v,
            &::types_tuple::varatt::set_varsize_4b_word(total as u32).to_ne_bytes(),
        )
        .unwrap();
        mcx::vec_append_bytes(&mut v, s).unwrap();
        let p = v.as_ptr();
        core::mem::forget(v);
        ::datum::Datum::from_usize(p as usize)
    }

    let mut acc = crate::bulk::BuildAccumulator::new(mcx, state);
    let kb = key(mcx, b"\x01bbb");
    let ka = key(mcx, b"\x01aaa");
    acc.insert_entries(&tid(1, 1), 1, &[kb, ka], &[GIN_CAT_NORM_KEY, GIN_CAT_NORM_KEY])
        .unwrap();
    acc.insert_entries(&tid(1, 2), 1, &[ka], &[GIN_CAT_NORM_KEY]).unwrap();
    acc.insert_entries(&tid(2, 1), 1, &[kb], &[GIN_CAT_NORM_KEY]).unwrap();
    // A null-item placeholder sorts after normal keys.
    acc.insert_entries(&tid(3, 1), 1, &[::datum::Datum::null()], &[GIN_CAT_NULL_ITEM])
        .unwrap();

    acc.begin_scan().unwrap();
    let (k1, c1, l1) = acc.next_entry().map(|(_, k, c, l)| (k, c, l.to_vec())).unwrap();
    assert_eq!(c1, GIN_CAT_NORM_KEY);
    let (_, _, _) = (k1, c1, &l1);
    assert_eq!(l1, vec![tid(1, 1), tid(1, 2)]); // "aaa" first, TIDs sorted
    let (_, c2, l2) = acc.next_entry().map(|(_, k, c, l)| (k, c, l.to_vec())).unwrap();
    assert_eq!(c2, GIN_CAT_NORM_KEY);
    assert_eq!(l2, vec![tid(1, 1), tid(2, 1)]);
    let (_, c3, l3) = acc.next_entry().map(|(_, k, c, l)| (k, c, l.to_vec())).unwrap();
    assert_eq!(c3, GIN_CAT_NULL_ITEM);
    assert_eq!(l3, vec![tid(3, 1)]);
    assert!(acc.next_entry().is_none());
    assert_eq!(acc.nentries(), 3);
}

#[test]
fn compare_entries_category_order() {
    let state = one_col_state(GinColState {
        opclass: GinOpclass::JsonbOps,
        elem_cmp: GinElemCmp::None,
        support_collation: 100,
        can_partial_match: false,
        key_byval: false,
        key_len: -1,
    });
    use crate::util::ginCompareEntries;
    let d = ::datum::Datum::null();
    assert!(ginCompareEntries(&state, 1, d, GIN_CAT_EMPTY_QUERY, d, GIN_CAT_NORM_KEY) < 0);
    assert!(ginCompareEntries(&state, 1, d, GIN_CAT_NULL_KEY, d, GIN_CAT_NORM_KEY) > 0);
    assert!(ginCompareEntries(&state, 1, d, GIN_CAT_NULL_ITEM, d, GIN_CAT_EMPTY_ITEM) > 0);
    assert_eq!(
        ginCompareEntries(&state, 1, d, GIN_CAT_NULL_ITEM, d, GIN_CAT_NULL_ITEM),
        0
    );
}
