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

// --- compressed stored-key compares (TOAST_INDEX_HACK class) ---------------
// index_form_tuple inline-compresses varlena keys above the size target, so
// entry-tree compares can see pglz images; C detoasts per compare
// (PG_GETARG_TEXT_PP). These units drive opclass::compare/compare_partial and
// the build accumulator with both flat and compressed forms of the same key.

fn flat_key(mcx: ::mcx::Mcx<'_>, s: &[u8]) -> ::datum::Datum {
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

/// Inline pglz image of `payload` (4B_C header + tcinfo + compressed data),
/// the exact shape index_form_tuple stores for keys above the target.
fn pglz_key(mcx: ::mcx::Mcx<'_>, payload: &[u8]) -> ::datum::Datum {
    use core::mem::MaybeUninit;
    let mut dst: Vec<MaybeUninit<u8>> =
        vec![MaybeUninit::uninit(); pglz::pglz_max_output(payload.len())];
    let clen = pglz::pglz_compress_into(payload, &mut dst, &pglz::PGLZ_STRATEGY_DEFAULT)
        .expect("test payload must compress");
    let total = 8 + clen;
    let mut v: ::mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, total).unwrap();
    mcx::vec_append_bytes(
        &mut v,
        &::types_tuple::varatt::set_varsize_4b_c_word(total as u32).to_ne_bytes(),
    )
    .unwrap();
    // va_tcinfo: raw data size | compression method (pglz = 0) in the top bits.
    mcx::vec_append_bytes(&mut v, &(payload.len() as u32).to_ne_bytes()).unwrap();
    // SAFETY: the first clen bytes were initialized by pglz_compress_into.
    let cbytes = unsafe { core::slice::from_raw_parts(dst.as_ptr().cast::<u8>(), clen) };
    mcx::vec_append_bytes(&mut v, cbytes).unwrap();
    let p = v.as_ptr();
    core::mem::forget(v);
    ::datum::Datum::from_usize(p as usize)
}

fn ts_col() -> GinColState {
    GinColState {
        opclass: GinOpclass::TsvectorOps,
        elem_cmp: GinElemCmp::None,
        support_collation: ::types_core::catalog::C_COLLATION_OID,
        can_partial_match: true,
        key_byval: false,
        key_len: -1,
    }
}

#[test]
fn compare_detoasts_compressed_keys() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    // Two big lexemes distinct only in the tail, sized like real stored keys.
    let mut la = b"ab".repeat(1020);
    let mut lb = la.clone();
    la.extend_from_slice(b"zzz0");
    lb.extend_from_slice(b"zzz1");
    // Round-trip sanity: the built image detoasts back to the payload.
    assert_eq!(crate::opclass::detoast_payload(mcx, pglz_key(mcx, &la)).unwrap(), &la[..]);

    let col = ts_col();
    for (a, b, want) in [(&la, &la, 0), (&la, &lb, -1), (&lb, &la, 1)] {
        let flat = crate::opclass::compare(&col, flat_key(mcx, a), flat_key(mcx, b));
        assert_eq!(flat.signum(), want, "flat/flat baseline");
        // Any mix of compressed sides must agree with the flat baseline.
        for (da, db) in [
            (pglz_key(mcx, a), flat_key(mcx, b)),
            (flat_key(mcx, a), pglz_key(mcx, b)),
            (pglz_key(mcx, a), pglz_key(mcx, b)),
        ] {
            assert_eq!(crate::opclass::compare(&col, da, db).signum(), want);
        }
    }

    // array_ops text and hstore arms take the same detoast gate.
    for opclass in [GinOpclass::ArrayOps, GinOpclass::HstoreOps] {
        let col = GinColState {
            opclass,
            elem_cmp: if opclass == GinOpclass::ArrayOps {
                GinElemCmp::Text
            } else {
                GinElemCmp::None
            },
            support_collation: ::types_core::catalog::C_COLLATION_OID,
            can_partial_match: false,
            key_byval: false,
            key_len: -1,
        };
        assert_eq!(crate::opclass::compare(&col, pglz_key(mcx, &la), flat_key(mcx, &la)), 0);
        assert_eq!(
            crate::opclass::compare(&col, pglz_key(mcx, &la), pglz_key(mcx, &lb)).signum(),
            -1
        );
    }
}

#[test]
fn compare_partial_detoasts_compressed_keys() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let prefix = b"ab".repeat(1020);
    let mut full = prefix.clone();
    full.extend_from_slice(b"zzz2");
    let col = ts_col();
    // Stored key carries the prefix: gin_cmp_prefix must say "match" (0)
    // whether the stored key is flat or compressed.
    let want = crate::opclass::compare_partial(&col, flat_key(mcx, &prefix), flat_key(mcx, &full), 0, ::datum::Datum::null());
    assert_eq!(want, 0);
    assert_eq!(
        crate::opclass::compare_partial(&col, flat_key(mcx, &prefix), pglz_key(mcx, &full), 0, ::datum::Datum::null()),
        0
    );
    // A stored key past the prefix range stops the scan (> 0) in both forms.
    let other = b"zz".repeat(1030);
    let stop = crate::opclass::compare_partial(&col, flat_key(mcx, &prefix), flat_key(mcx, &other), 0, ::datum::Datum::null());
    assert!(stop > 0);
    assert_eq!(
        crate::opclass::compare_partial(&col, flat_key(mcx, &prefix), pglz_key(mcx, &other), 0, ::datum::Datum::null()),
        stop
    );
}

#[test]
fn build_accumulator_compressed_keys_group_and_sort_detoasted() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let mut acc = crate::bulk::BuildAccumulator::new(mcx, one_col_state(ts_col()));
    // Raw-image order and detoasted order disagree on purpose: the pglz
    // image of "yyy..." starts with a header byte above b'z', so a raw-byte
    // sort would put the flat "zz" key first; the detoasted order is y < z.
    let big = b"y".repeat(2100);
    let ka1 = pglz_key(mcx, &big);
    let ka2 = pglz_key(mcx, &big); // separate copy, identical image bytes
    let kb = flat_key(mcx, b"zz");
    acc.insert_entries(&tid(1, 1), 1, &[ka1, kb], &[GIN_CAT_NORM_KEY, GIN_CAT_NORM_KEY])
        .unwrap();
    acc.insert_entries(&tid(2, 1), 1, &[ka2], &[GIN_CAT_NORM_KEY]).unwrap();
    acc.begin_scan().unwrap();
    // Identical compressed images grouped into one entry; "y..." dumps first.
    let (_, _, l1) = acc.next_entry().map(|(_, k, c, l)| (k, c, l.to_vec())).unwrap();
    assert_eq!(l1, vec![tid(1, 1), tid(2, 1)], "compressed key groups + sorts detoasted-first");
    let (_, _, l2) = acc.next_entry().map(|(_, k, c, l)| (k, c, l.to_vec())).unwrap();
    assert_eq!(l2, vec![tid(1, 1)]);
    assert!(acc.next_entry().is_none());
    assert_eq!(acc.nentries(), 2);
}
