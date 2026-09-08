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
    let state = one_col_state(GinColState::jsonb_ops(100));
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
    let state = one_col_state(GinColState::jsonb_ops(100));
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
    GinColState::tsvector_ops(::types_core::catalog::C_COLLATION_OID)
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

    // The bttextcmp arm (text-keyed array_ops, hstore) takes the same
    // detoast gate.
    for col in [
        GinColState::array_ops(GinCompareFn::Text, false, -1),
        GinColState::hstore_ops(::types_core::catalog::C_COLLATION_OID),
    ] {
        let col = GinColState {
            support_collation: ::types_core::catalog::C_COLLATION_OID,
            ..col
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

// A per-thread fake buffer manager shared by every gin unit test that reads
// pages through the bufmgr seams (a seam installs once per process): buffer
// b is page b-1 of the calling thread's page table, pins are counted, content
// locks are no-ops, vacuum delay points are counted.
pub(crate) mod fake_bufmgr {
    use std::cell::{Cell, RefCell};
    use std::sync::Once;

    use ::types_core::Buffer;

    thread_local! {
        static PAGES: RefCell<Vec<core::ptr::NonNull<u8>>> = const { RefCell::new(Vec::new()) };
        static PINS: Cell<i32> = const { Cell::new(0) };
        static DELAY_POINTS: Cell<u32> = const { Cell::new(0) };
    }

    pub(crate) fn install() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            bufmgr_seams::read_buffer::set(|_rel, blkno| {
                PINS.with(|c| c.set(c.get() + 1));
                Ok(blkno as Buffer + 1)
            });
            bufmgr_seams::release_buffer::set(|_buf| {
                PINS.with(|c| c.set(c.get() - 1));
                Ok(())
            });
            bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
            bufmgr_seams::buffer_get_page::set(|buf| {
                PAGES.with(|p| p.borrow()[(buf - 1) as usize])
            });
            vacuum_seams::vacuum_delay_point::set(|_is_analyze| {
                DELAY_POINTS.with(|c| c.set(c.get() + 1));
                Ok(())
            });
            postgres_seams::check_for_interrupts::set(|| Ok(()));
        });
    }

    /// This thread's page table (leaked BLCKSZ images); resets the counters.
    pub(crate) fn set_pages(pages: Vec<core::ptr::NonNull<u8>>) {
        PAGES.with(|p| *p.borrow_mut() = pages);
        PINS.with(|c| c.set(0));
        DELAY_POINTS.with(|c| c.set(0));
    }

    pub(crate) fn pins() -> i32 {
        PINS.with(Cell::get)
    }

    pub(crate) fn delay_points() -> u32 {
        DELAY_POINTS.with(Cell::get)
    }
}

// --- posting-tree leaf vacuum sweep (fake buffer manager) ---

mod posting_tree_vacuum {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    use ::mcx::{Mcx, PgVec};
    use ::types_core::{
        BlockNumber, InvalidBlockNumber, Oid, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
    };
    use ::types_error::PgResult;
    use ::types_nbtree::IndexBulkDeleteResult;
    use ::types_rel::{
        FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
        LOCKMODE, RELKIND_INDEX, REPLICA_IDENTITY_DEFAULT,
    };
    use ::types_tuple::tupdesc::CompactAttribute;
    use ::types_tuple::TupleDescData;

    use crate::util::gin_init_page_bytes;
    use crate::vacuum::{ginVacuumPostingTreeLeaves, GinVacDelete, GinVacuumState};
    use crate::write_opaque_to;

    #[repr(C, align(8))]
    struct FakePage([u8; BLCKSZ]);

    // A compressed, empty posting-tree leaf whose rightlink is `rightlink`.
    fn empty_leaf(rightlink: BlockNumber) -> Box<FakePage> {
        let mut p = Box::new(FakePage([0u8; BLCKSZ]));
        gin_init_page_bytes(&mut p.0, GIN_DATA | GIN_LEAF | GIN_COMPRESSED);
        // GinDataPageSetDataSize(page, 0): pd_lower starts past the
        // rightbound ItemPointer slot.
        p.0[12..14].copy_from_slice(&(GinDataPageDataOffset as u16).to_ne_bytes());
        write_opaque_to(
            &mut p.0,
            &GinPageOpaqueData {
                rightlink,
                maxoff: 0,
                flags: GIN_DATA | GIN_LEAF | GIN_COMPRESSED,
            },
        );
        p
    }

    fn int4_tupdesc(mcx: Mcx<'_>) -> TupleDescData<'_> {
        let mut compact = PgVec::new_in(mcx);
        compact.push(CompactAttribute {
            attcacheoff: Cell::new(-1),
            attlen: 4,
            attbyval: true,
            attispackable: false,
            atthasmissing: false,
            attisdropped: false,
            attgenerated: false,
            attnullability: 0,
            attalignby: 4,
        });
        TupleDescData {
            natts: 1,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs: compact,
            attrs: PgVec::new_in(mcx),
        }
    }

    fn noop_close(_oid: Oid, _mode: LOCKMODE) -> PgResult<()> {
        Ok(())
    }

    fn index_rel(mcx: Mcx<'_>) -> Relation<'_> {
        let mut relname = ::types_tuple::NameData::default();
        relname.namestrcpy("t_gin");
        let mut indkey = PgVec::new_in(mcx);
        indkey.push(1);
        let one = |v: Oid| {
            let mut vec = PgVec::new_in(mcx);
            vec.push(v);
            vec
        };
        let mut indoption = PgVec::new_in(mcx);
        indoption.push(0i16);
        let data = RelationData {
            rd_locator: Cell::new(::types_storage::RelFileLocator::new(1663, 5, 6000)),
            rd_smgr: Default::default(),
            rd_id: 6000,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: Cell::new(true),
            rd_createSubid: Cell::new(0),
            rd_newRelfilelocatorSubid: Cell::new(0),
            rd_firstRelfilelocatorSubid: Cell::new(0),
            rd_droppedSubid: Cell::new(0),
            rd_lockInfo: LockInfoData {
                lockRelId: LockRelId { relId: 6000, dbId: 5 },
            },
            rd_rel: FormData_pg_class {
                relname,
                relnamespace: 2200,
                reltype: 0,
                relowner: 10,
                relam: ::types_core::catalog::GIN_AM_OID,
                relfilenode: 6000,
                reltablespace: 0,
                relpages: 0,
                reltuples: -1.0,
                relallvisible: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relisshared: false,
                relpersistence: RELPERSISTENCE_PERMANENT,
                relkind: RELKIND_INDEX,
                relhassubclass: false,
                relrowsecurity: false,
                relispopulated: true,
                relreplident: REPLICA_IDENTITY_DEFAULT,
                relispartition: false,
                relfrozenxid: 3,
                relminmxid: 1,
            },
            rd_att: Rc::new(int4_tupdesc(mcx)),
            rd_index: Some(FormData_pg_index {
                indexrelid: 6000,
                indrelid: 5999,
                indnatts: 1,
                indnkeyatts: 1,
                indisunique: false,
                indnullsnotdistinct: false,
                indisprimary: false,
                indisexclusion: false,
                indimmediate: true,
                indisvalid: true,
                indisready: true,
                indcheckxmin: false,
                indxmin: 0,
                indkey,
                has_indpred: false,
                indexprs_src: None,
                indpred_src: None,
            }),
            rd_opcintype: one(23),
            rd_opfamily: one(2745),
            rd_indoption: indoption,
            rd_indcollation: one(0),
            rd_options: None,
            pgstat_enabled: Cell::new(false),
            pgstat_link: Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(),
            rd_amcache_gin: Default::default(),
            rd_amcache_spgist: Default::default(),
            rd_support: PgVec::new_in(mcx),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false,
            rd_hasrules: false,
        };
        Relation::open(data, Some(noop_close))
    }

    fn int4_col() -> GinColState {
        GinColState::array_ops(GinCompareFn::Int4, true, 4)
    }

    // upstream 7becb647da74 (18.5): Restore vacuum_delay_point() in GIN
    // posting-tree leaf vacuum. The rightlink sweep of a posting tree must
    // reach a delay/interrupt point between leaf pages, with no buffer lock
    // held (the sibling per-page loops in ginbulkdelete already do).
    #[test]
    fn posting_tree_leaf_sweep_delays_between_pages() {
        super::fake_bufmgr::install();
        // Block 0 stands in for the metapage; the tree is three chained
        // leaves 1 -> 2 -> 3, the root being the leftmost leaf.
        super::fake_bufmgr::set_pages(
            [empty_leaf(InvalidBlockNumber), empty_leaf(2), empty_leaf(3), empty_leaf(InvalidBlockNumber)]
                .into_iter()
                .map(|page| core::ptr::NonNull::from(Box::leak(page)).cast::<u8>())
                .collect(),
        );
        init_small::globals::SetVacuumCostActive(true);

        let ctx = MemoryContext::new("t");
        let rel = index_rel(ctx.mcx());
        let state = one_col_state(int4_col());
        let mut stats = IndexBulkDeleteResult::default();
        let mut gvs = GinVacuumState {
            rel: &rel,
            state: &state,
            delete: GinVacDelete::DeadItems(&[]),
            stats: &mut stats,
        };
        let has_void = ginVacuumPostingTreeLeaves(&mut gvs, 1).unwrap();
        init_small::globals::SetVacuumCostActive(false);

        assert!(has_void, "every leaf is empty");
        assert_eq!(super::fake_bufmgr::pins(), 0, "no pins leaked");
        assert_eq!(
            super::fake_bufmgr::delay_points(),
            2,
            "one vacuum_delay_point per rightlink hop"
        );
    }
}
