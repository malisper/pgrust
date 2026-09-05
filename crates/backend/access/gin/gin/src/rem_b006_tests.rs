//! Audit-18.6 remediation batch b006 (backend/access/gin) witnesses.
//!
//! A per-thread fake index relation + page table driven through the bufmgr /
//! WAL / lock / predicate / freespace seams, so the entry-tree insert path,
//! posting-tree creation, posting-tree leaf vacuum, pending-list insert and
//! pending-list cleanup all run end to end with no shared memory. The WAL
//! seam records `CritSectionCount` at every `XLogInsert`, which is the C
//! contract the critical-section rows assert (xloginsert.c:1245 and
//! xlog.c:2315 both `Assert(CritSectionCount > 0)` under XLogInsert).
#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Once;

use ::bufmgr_seams as bm;
use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::nbtree::itup::{self, ItupBuf};
use ::types_core::{
    BlockNumber, Buffer, InvalidBlockNumber, Oid, OffsetNumber, BLCKSZ, INVALID_PROC_NUMBER,
    RELPERSISTENCE_PERMANENT,
};
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX, REPLICA_IDENTITY_DEFAULT,
};
use ::types_storage::bufpage::PageMut;
use ::types_tuple::itemptr::{FirstOffsetNumber, ItemPointerData};
use ::types_tuple::tupdesc::CompactAttribute;
use ::types_tuple::TupleDescData;

use crate::entrypage::GinFormTuple;
use crate::util::{gin_init_metapage_bytes, gin_init_page_bytes};

pub(crate) mod rig {
    use super::*;

    #[repr(C, align(8))]
    pub(crate) struct FakePage(pub [u8; BLCKSZ]);

    thread_local! {
        /// Blocks the fake relation currently "has" (extend hands out the next).
        static NBLOCKS: Cell<u32> = const { Cell::new(0) };
        /// Pages allocated for this thread (real + spare for extension).
        static NALLOC: Cell<u32> = const { Cell::new(0) };
        /// (record info, CritSectionCount at XLogInsert) per WAL record.
        static WAL: RefCell<Vec<(u8, u32)>> = const { RefCell::new(Vec::new()) };
        /// Make the next xlog_insert_record calls fail (simulated WAL failure).
        static WAL_FAIL: Cell<bool> = const { Cell::new(false) };
        static DIRTY: RefCell<Vec<Buffer>> = const { RefCell::new(Vec::new()) };
        /// Buffers handed out by extend (pinned by the real bufmgr, but not
        /// counted by fake_bufmgr's PINS, which only read_buffer bumps).
        static EXTENDED: Cell<i32> = const { Cell::new(0) };
    }

    pub(crate) fn tid(blk: u32, off: u16) -> ItemPointerData {
        ItemPointerData::new(blk, off)
    }

    /// This thread's MarkBufferDirty ledger entry (also fed by the b084 rig's
    /// seam closure when that rig installed the seam first).
    pub(crate) fn record_dirty(buf: Buffer) {
        DIRTY.with(|d| d.borrow_mut().push(buf));
    }

    /// This thread's XLogInsert ledger entry plus the simulated-WAL-failure
    /// arm (also fed by the b084 rig's seam closure).
    pub(crate) fn record_wal(info: u8) -> PgResult<()> {
        WAL.with(|w| w.borrow_mut().push((info, init_small::globals::CritSectionCount())));
        if WAL_FAIL.with(Cell::get) {
            return Err(Box::new(PgError::error("simulated WAL insert failure")));
        }
        Ok(())
    }

    /// Installs the seams every witness needs (once per process; layered on
    /// tests.rs's fake_bufmgr, whose seams it never re-installs).
    pub(crate) fn install() {
        crate::tests::fake_bufmgr::install();
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            // The b084 rig (rem_b084_tests) records the same two seams into
            // its own process-wide ledgers; a seam is installed once per
            // process, so whichever rig wins the race feeds BOTH recorders.
            if !bm::mark_buffer_dirty::is_installed() {
                bm::mark_buffer_dirty::set(|buf| {
                    record_dirty(buf);
                    crate::rem_b084_tests::record_dirty(buf);
                    Ok(())
                });
            }
            if !bm::buffer_get_block_number::is_installed() {
                bm::buffer_get_block_number::set(|buf| (buf - 1) as BlockNumber);
            }
            if !bm::relation_get_number_of_blocks_in_fork::is_installed() {
                bm::relation_get_number_of_blocks_in_fork::set(|_rel, _fork| {
                    Ok(NBLOCKS.with(Cell::get))
                });
            }
            if !bm::extend_buffered_rel_by::is_installed() {
                bm::extend_buffered_rel_by::set(|_rel, _fork, _strategy, _flags, extend_by| {
                    assert_eq!(extend_by, 1, "gin extends one block at a time");
                    let blk = NBLOCKS.with(|c| {
                        let b = c.get();
                        c.set(b + 1);
                        b
                    });
                    assert!(
                        blk < NALLOC.with(Cell::get),
                        "rig out of spare pages (block {blk})"
                    );
                    EXTENDED.with(|c| c.set(c.get() + 1));
                    Ok((blk as Buffer + 1, 1))
                });
            }
            if !bm::conditional_lock_buffer::is_installed() {
                bm::conditional_lock_buffer::set(|_buf| Ok(true));
            }
            if !bm::overwrite_buffer_page::is_installed() {
                bm::overwrite_buffer_page::set(|buf, page| {
                    let dst = bm::buffer_get_page::call(buf);
                    assert_eq!(page.len(), BLCKSZ);
                    // SAFETY: dst is a leaked BLCKSZ FakePage of this thread.
                    unsafe { core::ptr::copy_nonoverlapping(page.as_ptr(), dst.as_ptr(), BLCKSZ) };
                });
            }
            if !freespace_seams::get_page_with_free_space::is_installed() {
                freespace_seams::get_page_with_free_space::set(|_rel, _n| Ok(InvalidBlockNumber));
            }
            if !freespace_seams::record_page_with_free_space::is_installed() {
                freespace_seams::record_page_with_free_space::set(|_rel, _b, _s| Ok(()));
            }
            if !transam_xlog_seams::xlog_standby_info_active::is_installed() {
                transam_xlog_seams::xlog_standby_info_active::set(|| false);
            }
            if !xloginsert_seams::xlog_insert_record::is_installed() {
                xloginsert_seams::xlog_insert_record::set(|_rmid, info, _flags, _data, _bufs| {
                    crate::rem_b084_tests::record_wal(info);
                    record_wal(info)?;
                    Ok(0x1000)
                });
            }
            if !predicate_seams::check_for_serializable_conflict_in::is_installed() {
                predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
            }
            if !predicate_seams::predicate_lock_page_split::is_installed() {
                predicate_seams::predicate_lock_page_split::set(|_rel, _o, _n| Ok(()));
            }
            if !lock_seams::lock_acquire_extended::is_installed() {
                lock_seams::lock_acquire_extended::set(|_tag, _mode, _s, _dw, _r, _l| {
                    Ok(::types_storage::lock::LOCKACQUIRE_OK)
                });
            }
            if !lock_seams::lock_release::is_installed() {
                lock_seams::lock_release::set(|_tag, _mode, _s| Ok(true));
            }
            guc_tables::vars::gin_pending_list_limit.install_if_absent(
                guc_tables::GucVarAccessors {
                    get: || 4096,
                    set: |_| {},
                },
            );
        });
    }

    pub(crate) fn blank_page() -> Box<FakePage> {
        Box::new(FakePage([0u8; BLCKSZ]))
    }

    /// A GIN page image with the given opaque flags.
    pub(crate) fn gin_page(flags: u16) -> Box<FakePage> {
        let mut p = blank_page();
        gin_init_page_bytes(&mut p.0, flags);
        p
    }

    /// Block 0 metapage image (GinInitMetabuffer shape).
    pub(crate) fn metapage() -> Box<FakePage> {
        let mut p = blank_page();
        gin_init_metapage_bytes(&mut p.0);
        p
    }

    /// Installs this thread's page table: `pages` are blocks 0..n, followed
    /// by `spare` blank pages GinNewBuffer may extend into. Resets counters.
    pub(crate) fn set_pages(pages: Vec<Box<FakePage>>, spare: u32) {
        let n = pages.len() as u32;
        let mut all: Vec<core::ptr::NonNull<u8>> = pages
            .into_iter()
            .map(|p| core::ptr::NonNull::from(Box::leak(p)).cast::<u8>())
            .collect();
        for _ in 0..spare {
            all.push(core::ptr::NonNull::from(Box::leak(blank_page())).cast::<u8>());
        }
        NALLOC.with(|c| c.set(n + spare));
        NBLOCKS.with(|c| c.set(n));
        crate::tests::fake_bufmgr::set_pages(all);
        WAL.with(|w| w.borrow_mut().clear());
        WAL_FAIL.with(|c| c.set(false));
        DIRTY.with(|d| d.borrow_mut().clear());
        EXTENDED.with(|c| c.set(0));
    }

    /// Net pin count: fake_bufmgr's read/release balance plus the pins the
    /// extend seam handed out without counting. Zero when nothing leaked.
    pub(crate) fn net_pins() -> i32 {
        crate::tests::fake_bufmgr::pins() + EXTENDED.with(Cell::get)
    }

    pub(crate) fn page_bytes_of(blkno: BlockNumber) -> &'static mut [u8] {
        let p = bm::buffer_get_page::call(blkno as Buffer + 1);
        // SAFETY: leaked BLCKSZ image owned by this thread's page table.
        unsafe { core::slice::from_raw_parts_mut(p.as_ptr(), BLCKSZ) }
    }

    pub(crate) fn wal_records() -> Vec<(u8, u32)> {
        WAL.with(|w| w.borrow().clone())
    }

    pub(crate) fn set_wal_fail(fail: bool) {
        WAL_FAIL.with(|c| c.set(fail));
    }

    pub(crate) fn dirtied() -> Vec<Buffer> {
        DIRTY.with(|d| d.borrow().clone())
    }

    pub(crate) fn nblocks() -> u32 {
        NBLOCKS.with(Cell::get)
    }

    pub(crate) fn delay_points() -> u32 {
        crate::tests::fake_bufmgr::delay_points()
    }

    /// Every WAL record written so far was emitted with CritSectionCount > 0
    /// (the C contract under XLogInsert); panics with the offending record.
    pub(crate) fn assert_wal_in_crit_section(expect_infos: &[u8]) {
        let recs = wal_records();
        assert!(!recs.is_empty(), "no WAL record was written");
        for info in expect_infos {
            assert!(
                recs.iter().any(|(i, _)| i == info),
                "expected WAL record 0x{info:02x} not written; got {recs:?}"
            );
        }
        for (info, count) in &recs {
            assert!(
                *count > 0,
                "XLogInsert of GIN record 0x{info:02x} outside a critical section (CritSectionCount = {count}); all records: {recs:?}"
            );
        }
    }

    #[derive(Clone, Copy)]
    pub(crate) enum KeyKind {
        Int4,
        Text,
    }

    fn one_col_tupdesc(mcx: Mcx<'_>, kind: KeyKind) -> TupleDescData<'_> {
        let mut compact = PgVec::new_in(mcx);
        compact.push(match kind {
            KeyKind::Int4 => CompactAttribute {
                attcacheoff: Cell::new(-1),
                attlen: 4,
                attbyval: true,
                attispackable: false,
                atthasmissing: false,
                attisdropped: false,
                attgenerated: false,
                attnullability: 0,
                attalignby: 4,
            },
            KeyKind::Text => CompactAttribute {
                attcacheoff: Cell::new(-1),
                attlen: -1,
                attbyval: false,
                attispackable: true,
                atthasmissing: false,
                attisdropped: false,
                attgenerated: false,
                attnullability: 0,
                attalignby: 4,
            },
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

    /// A permanent one-column GIN index relation (rd_createSubid = 0, so
    /// RelationNeedsWAL is true).
    pub(crate) fn index_rel(mcx: Mcx<'_>, kind: KeyKind) -> Relation<'_> {
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
        let opcintype = match kind {
            KeyKind::Int4 => 23,
            KeyKind::Text => 25,
        };
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
            rd_att: Rc::new(one_col_tupdesc(mcx, kind)),
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
                indkey,
                has_indpred: false,
                indexprs_src: None,
                indpred_src: None,
            }),
            rd_opcintype: one(opcintype),
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

    pub(crate) fn one_col_state(kind: KeyKind) -> GinState {
        let col = match kind {
            KeyKind::Int4 => GinColState {
                opclass: GinOpclass::ArrayOps,
                elem_cmp: GinElemCmp::Int4,
                support_collation: 0,
                can_partial_match: false,
                key_byval: true,
                key_len: 4,
            },
            KeyKind::Text => GinColState {
                opclass: GinOpclass::BtreeOps(GinBtreeType::Text),
                elem_cmp: GinElemCmp::None,
                support_collation: 100,
                can_partial_match: true,
                key_byval: false,
                key_len: -1,
            },
        };
        GinState {
            natts: 1,
            one_col: true,
            cols: [col; GIN_MAX_KEY_COLS],
        }
    }

    /// An int4-keyed entry tuple with no posting list (pending-list shape),
    /// t_tid carrying the heap TID.
    pub(crate) fn pending_tuple<'m>(
        mcx: Mcx<'m>,
        rel: &Relation<'_>,
        state: &GinState,
        key: i32,
        heap: ItemPointerData,
    ) -> ItupBuf<'m> {
        let mut t = GinFormTuple(
            mcx,
            rel,
            state,
            1,
            Datum::from_i32(key),
            GIN_CAT_NORM_KEY,
            &[],
            0,
            0,
            true,
        )
        .unwrap()
        .expect("errorTooBig");
        // SAFETY: owned tuple image.
        unsafe { itup::set_t_tid(t.as_mut_ptr(), heap) };
        t
    }

    /// PageAddItem of a whole tuple image onto a raw page image.
    pub(crate) fn add_tuple(page: &mut [u8], t: &ItupBuf<'_>, off: OffsetNumber) -> Option<OffsetNumber> {
        // SAFETY: owned image; true length in t_info.
        let size = unsafe { itup::index_tuple_size(t.as_ptr()) };
        let bytes = unsafe { core::slice::from_raw_parts(t.as_ptr(), size) };
        // SAFETY: exclusive access to a BLCKSZ image.
        let mut pm =
            unsafe { PageMut::from_raw(core::ptr::NonNull::new(page.as_mut_ptr()).unwrap()) };
        pm.add_item(bytes, off, 0)
    }
}

use rig::*;

// --- ginbtree.c:397/564 ginPlaceToPage: START/END_CRIT_SECTION around the
// page update + WAL insert (row a186-candidate-fp-gin-ginbtree-1103e4ee748ef367454c-1).

#[test]
fn entry_insert_wal_record_is_inside_critical_section() {
    install();
    set_pages(vec![metapage(), gin_page(GIN_LEAF)], 0);
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx, KeyKind::Int4);
    let state = one_col_state(KeyKind::Int4);

    crate::insert::ginEntryInsert(mcx, &rel, &state, 1, Datum::from_i32(7), GIN_CAT_NORM_KEY, &[tid(1, 1)], None)
        .unwrap();

    assert_wal_in_crit_section(&[XLOG_GIN_INSERT]);
    assert_eq!(init_small::globals::CritSectionCount(), 0, "critical section left open");
    assert!(dirtied().contains(&2), "root leaf (buffer 2) was not marked dirty");
    assert_eq!(net_pins(), 0, "pins leaked");
}

#[test]
fn entry_root_split_wal_record_is_inside_critical_section() {
    install();
    set_pages(vec![metapage(), gin_page(GIN_LEAF)], 8);
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx, KeyKind::Int4);
    let state = one_col_state(KeyKind::Int4);

    // Fill the root leaf with distinct keys until it splits (root split:
    // GPTP_SPLIT with stack->parent == NULL, three full-page images).
    let mut split = false;
    for k in 0..2000i32 {
        crate::insert::ginEntryInsert(mcx, &rel, &state, 1, Datum::from_i32(k), GIN_CAT_NORM_KEY, &[tid(1, 1)], None)
            .unwrap();
        if wal_records().iter().any(|(i, _)| *i == XLOG_GIN_SPLIT) {
            split = true;
            break;
        }
    }
    assert!(split, "root leaf never split");
    assert_wal_in_crit_section(&[XLOG_GIN_INSERT, XLOG_GIN_SPLIT]);
    assert_eq!(init_small::globals::CritSectionCount(), 0);
    assert_eq!(nblocks(), 4, "root split allocates two new blocks");
}

// An error raised between page modification and WAL insert must not unwind
// as a recoverable Err with the section closed: C PANICs (errstart promotes
// ERROR to PANIC while CritSectionCount > 0); pgrust's xact layer does the
// same promotion through elog::panic_on_crit_section_escape, which needs
// the count still raised when the Err reaches it.
#[test]
fn wal_failure_inside_entry_insert_keeps_critical_section_open() {
    install();
    set_pages(vec![metapage(), gin_page(GIN_LEAF)], 0);
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx, KeyKind::Int4);
    let state = one_col_state(KeyKind::Int4);

    set_wal_fail(true);
    let res = crate::insert::ginEntryInsert(mcx, &rel, &state, 1, Datum::from_i32(7), GIN_CAT_NORM_KEY, &[tid(1, 1)], None);
    set_wal_fail(false);
    let err = res.err().expect("simulated WAL failure must surface");
    assert_eq!(err.message(), "simulated WAL insert failure");
    let open = init_small::globals::CritSectionCount();
    init_small::globals::SetCritSectionCount(0);
    assert!(
        open > 0,
        "Err escaped ginPlaceToPage with the critical section closed (count {open}): the dirty, unlogged root leaf would be treated as a recoverable statement error"
    );
}

// --- gindatapage.c:1834 createPostingTree and :845 ginVacuumPostingTreeLeaf
// (row a186-candidate-fp-gin-gindatapage-9fc535a7a8f839f91646-1).

#[test]
fn create_posting_tree_wal_record_is_inside_critical_section() {
    install();
    set_pages(vec![metapage(), gin_page(GIN_LEAF)], 2);
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx, KeyKind::Int4);
    let items: Vec<ItemPointerData> = (1..=20u32).map(|b| tid(b, 1)).collect();

    let root = crate::datapage::createPostingTree(mcx, &rel, &items, None, 2).unwrap();
    assert_eq!(root, 2, "first extended block");
    assert_wal_in_crit_section(&[XLOG_GIN_CREATE_PTREE]);
    assert_eq!(init_small::globals::CritSectionCount(), 0);
    assert!(crate::GinPageIsCompressed(&crate::opaque_of(page_bytes_of(2))));
}

#[test]
fn vacuum_posting_tree_leaf_wal_record_is_inside_critical_section() {
    install();
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    // A compressed posting-tree leaf holding TIDs (1..=30, 1).
    let mut leaf = gin_page(GIN_DATA | GIN_LEAF | GIN_COMPRESSED);
    let items: Vec<ItemPointerData> = (1..=30u32).map(|b| tid(b, 1)).collect();
    let (seg, n) =
        crate::postinglist::ginCompressPostingList(mcx, &items, crate::datapage::GinPostingListSegmentMaxSize).unwrap();
    assert_eq!(n, items.len());
    leaf.0[GinDataPageDataOffset..GinDataPageDataOffset + seg.len()].copy_from_slice(&seg);
    crate::datapage::set_data_page_data_size(&mut leaf.0, seg.len());
    set_pages(vec![metapage(), gin_page(GIN_LEAF), leaf], 0);

    let rel = index_rel(mcx, KeyKind::Int4);
    let state = one_col_state(KeyKind::Int4);
    let dead = [tid(5, 1), tid(17, 1)];
    let mut stats = ::types_nbtree::IndexBulkDeleteResult::default();
    let mut gvs = crate::vacuum::GinVacuumState {
        rel: &rel,
        state: &state,
        delete: crate::vacuum::GinVacDelete::DeadItems(&dead),
        stats: &mut stats,
    };
    crate::datapage::ginVacuumPostingTreeLeaf(mcx, &mut gvs, 3).unwrap();

    assert_eq!(stats.tuples_removed, 2.0);
    assert_wal_in_crit_section(&[XLOG_GIN_VACUUM_DATA_LEAF_PAGE]);
    assert_eq!(init_small::globals::CritSectionCount(), 0);
    // The recompressed page no longer carries the dead TIDs.
    let mut out = mcx::vec_new_in(mcx);
    crate::datapage::gin_data_leaf_page_get_items(page_bytes_of(2), &tid(0, 0), &mut out).unwrap();
    assert_eq!(out.len(), 28);
    assert!(!out.iter().any(|t| t.ip_blkid.bi_lo == 5 || t.ip_blkid.bi_lo == 17));
}

// --- ginfast.c:71 writeListPage, :302/:328/:372 ginHeapTupleFastInsert,
// :601 shiftList (row a186-candidate-fp-gin-ginfast-40edda9be3cedb939cb4-1)
// and ginfast.c:895/:932/:1005 vacuum_delay_point in ginInsertCleanup
// (row a186-candidate-fp-gin-ginfast-2132ba136da7321b753c-1).

fn fast_insert_rows(mcx: Mcx<'_>, rel: &Relation<'_>, state: &GinState) {
    // Collector 1: 120 heap rows x 5 distinct keys = 600 16-byte tuples
    // (> GinListPageSize with line pointers), so makeSublist spans two
    // pending pages; a row's keys are distinct, as ginExtractEntries
    // guarantees. Collector 2: one more row with two keys, appended to the
    // tail page through the metapage-locked arm.
    let mut c = crate::fast::GinTupleCollector::new(mcx);
    for k in 0..600i32 {
        let t = pending_tuple(mcx, rel, state, k % 5, tid(1, (k / 5 + 1) as u16));
        // SAFETY: owned image.
        c.sumsize += unsafe { itup::index_tuple_size(t.as_ptr()) };
        c.tuples.push(t);
    }
    crate::fast::ginHeapTupleFastInsert(mcx, rel, state, &mut c).unwrap();
    let mut c2 = crate::fast::GinTupleCollector::new(mcx);
    for k in 0..2i32 {
        let t = pending_tuple(mcx, rel, state, k, tid(1, 121));
        // SAFETY: owned image.
        c2.sumsize += unsafe { itup::index_tuple_size(t.as_ptr()) };
        c2.tuples.push(t);
    }
    crate::fast::ginHeapTupleFastInsert(mcx, rel, state, &mut c2).unwrap();
}

#[test]
fn pending_list_insert_wal_records_are_inside_critical_section() {
    install();
    set_pages(vec![metapage(), gin_page(GIN_LEAF)], 8);
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx, KeyKind::Int4);
    let state = one_col_state(KeyKind::Int4);

    fast_insert_rows(mcx, &rel, &state);

    let meta = crate::meta_of(page_bytes_of(0));
    assert_eq!(meta.nPendingPages, 2, "two pending pages expected: {meta:?}");
    assert_eq!(meta.nPendingHeapTuples, 2);
    assert_eq!(meta.head, 2);
    assert_eq!(meta.tail, 3);
    let n_listpage = wal_records().iter().filter(|(i, _)| *i == XLOG_GIN_INSERT_LISTPAGE).count();
    let n_meta = wal_records().iter().filter(|(i, _)| *i == XLOG_GIN_UPDATE_META_PAGE).count();
    assert_eq!((n_listpage, n_meta), (2, 2), "records: {:?}", wal_records());
    assert_wal_in_crit_section(&[XLOG_GIN_INSERT_LISTPAGE, XLOG_GIN_UPDATE_META_PAGE]);
    assert_eq!(init_small::globals::CritSectionCount(), 0);
    assert_eq!(net_pins(), 0, "pins leaked");
}

#[test]
fn pending_list_cleanup_delays_and_logs_inside_critical_section() {
    install();
    set_pages(vec![metapage(), gin_page(GIN_LEAF)], 8);
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx, KeyKind::Int4);
    let state = one_col_state(KeyKind::Int4);
    fast_insert_rows(mcx, &rel, &state);
    // Only cleanup's records and delay points from here on.
    let before = wal_records().len();
    init_small::globals::SetVacuumCostActive(true);
    let dp0 = delay_points();

    let res = crate::fast::ginInsertCleanup(mcx, &rel, &state, true, false, true, None);
    init_small::globals::SetVacuumCostActive(false);
    res.unwrap();

    let recs = &wal_records()[before..];
    // Five distinct keys dumped into the entry tree, then both pending pages
    // deleted in one shiftList batch.
    let n_insert = recs.iter().filter(|(i, _)| *i == XLOG_GIN_INSERT).count();
    let n_delete = recs.iter().filter(|(i, _)| *i == XLOG_GIN_DELETE_LISTPAGE).count();
    assert_eq!((n_insert, n_delete), (5, 1), "cleanup records: {recs:?}");
    assert_wal_in_crit_section(&[XLOG_GIN_DELETE_LISTPAGE]);
    assert_eq!(init_small::globals::CritSectionCount(), 0);
    let meta = crate::meta_of(page_bytes_of(0));
    assert_eq!(meta.nPendingPages, 0);
    assert_eq!(meta.head, InvalidBlockNumber);
    assert_eq!(net_pins(), 0, "pins leaked");

    // ginfast.c: one vacuum_delay_point after each page's processPendingPage
    // (895), one per dumped entry (932), one before reading the next page
    // (1005): page 1 -> 2, page 2 (tail) -> 1 + 5 entries.
    assert_eq!(
        delay_points() - dp0,
        2 + 1 + 5,
        "ginInsertCleanup must reach vacuum_delay_point between pages and entries (ginfast.c:895/932/1005)"
    );
}

// --- ginbtree.c:192 ginStepRight elog(ERROR) rendered as panic
// (row a186-candidate-fp-gin-ginbtree-150bdc9886b5e76524d9-1).

#[test]
fn step_right_onto_different_page_type_is_internal_error() {
    install();
    let mut leaf = gin_page(GIN_LEAF);
    let mut o = crate::opaque_of(&leaf.0);
    o.rightlink = 2;
    crate::write_opaque_to(&mut leaf.0, &o);
    // Block 2: an internal (non-leaf) page — the type mismatch C reports.
    set_pages(vec![metapage(), leaf, gin_page(0)], 0);
    let ctx = MemoryContext::new_bump("t");
    let rel = index_rel(ctx.mcx(), KeyKind::Int4);

    let buf = bm::read_buffer::call(&rel, 1).unwrap();
    let err = crate::btree::ginStepRight(buf, &rel, crate::GIN_SHARE)
        .err()
        .expect("mismatched sibling must be a catchable error");
    assert_eq!(err.message(), "right sibling of GIN page is of different type");
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
}

// --- gindatapage.c:167 GinDataLeafPageGetItems / :195 GetItemsToTbm on a
// pre-9.4 uncompressed leaf (row a186-candidate-fp-gin-gindatapage-d74c16878f331c675166-1):
// pgrust does not carry the uncompressed lane; the refusal must be a typed
// error, never a backend panic.

fn uncompressed_leaf() -> Box<FakePage> {
    let mut p = gin_page(GIN_DATA | GIN_LEAF);
    let items = [tid(1, 1), tid(2, 1), tid(3, 1)];
    let mut at = GinDataPageDataOffset;
    for it in &items {
        // SAFETY: ItemPointerData is a 6-byte POD.
        let b = unsafe { core::slice::from_raw_parts((it as *const ItemPointerData).cast::<u8>(), 6) };
        p.0[at..at + 6].copy_from_slice(b);
        at += 6;
    }
    let mut o = crate::opaque_of(&p.0);
    o.maxoff = items.len() as OffsetNumber;
    crate::write_opaque_to(&mut p.0, &o);
    p
}

#[test]
fn uncompressed_posting_leaf_is_typed_refusal_not_panic() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let page = uncompressed_leaf();

    let mut out = mcx::vec_new_in(mcx);
    let err = crate::datapage::gin_data_leaf_page_get_items(&page.0, &tid(0, 0), &mut out)
        .err()
        .expect("uncompressed leaf must be a catchable error");
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    assert!(err.message().contains("uncompressed"), "{}", err.message());

    let mut tbm = ::tidbitmap::TIDBitmap::new(mcx, 1 << 20);
    let err = crate::datapage::gin_data_leaf_page_get_items_to_tbm(mcx, &page.0, &mut tbm)
        .err()
        .expect("uncompressed leaf must be a catchable error");
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
}

// --- ginentrypage.c:176 ginReadTuple count mismatch elog(ERROR)
// (row a186-candidate-fp-gin-ginentrypage-9c50a5f8aea68864d327-1).

#[test]
fn read_tuple_item_count_mismatch_is_internal_error() {
    install();
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx, KeyKind::Int4);
    let state = one_col_state(KeyKind::Int4);
    let items = [tid(1, 1), tid(2, 1), tid(3, 1)];
    let (seg, n) = crate::postinglist::ginCompressPostingList(mcx, &items, GinMaxItemSize).unwrap();
    assert_eq!(n, 3);
    let mut t = GinFormTuple(mcx, &rel, &state, 1, Datum::from_i32(1), GIN_CAT_NORM_KEY, &seg, seg.len(), 3, false)
        .unwrap()
        .expect("fits");
    // Corrupt the header's item count (t_tid.ip_posid) to 2.
    // SAFETY: owned tuple image.
    unsafe {
        let mut hdr = itup::t_tid(t.as_ptr());
        hdr.ip_posid = 2;
        itup::set_t_tid(t.as_mut_ptr(), hdr);
    }
    let mut out = mcx::vec_new_in(mcx);
    // SAFETY: owned tuple image.
    let err = unsafe { crate::entrypage::ginReadTuple(mcx, t.as_ptr(), &mut out) }
        .err()
        .expect("count mismatch must be a catchable error");
    assert_eq!(
        err.message(),
        "number of items mismatch in GIN entry tuple, 2 in tuple header, 3 decoded"
    );
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
}

// --- ginentrypage.c:731 ginEntryFillRoot elog(ERROR)
// (row a186-candidate-fp-gin-ginentrypage-190e5b2914bfaab845f2-1).

#[test]
fn fill_root_without_free_space_is_internal_error() {
    install();
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx, KeyKind::Int4);
    let state = one_col_state(KeyKind::Int4);
    let mut lpage = gin_page(GIN_LEAF);
    let mut rpage = gin_page(GIN_LEAF);
    let lt = pending_tuple(mcx, &rel, &state, 1, tid(1, 1));
    let rt = pending_tuple(mcx, &rel, &state, 2, tid(1, 2));
    assert!(add_tuple(&mut lpage.0, &lt, FirstOffsetNumber).is_some());
    assert!(add_tuple(&mut rpage.0, &rt, FirstOffsetNumber).is_some());
    // Root image with pd_upper == pd_lower: no room for any downlink.
    let mut root = gin_page(0);
    let lower = [root.0[12], root.0[13]];
    root.0[14..16].copy_from_slice(&lower);

    let err = crate::entrypage::gin_entry_fill_root(mcx, &mut root.0, 3, &lpage.0, 4, &rpage.0)
        .err()
        .expect("full root must be a catchable error");
    assert_eq!(err.message(), "failed to add item to index root page");
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
}

// --- ginentrypage.c:570 entryExecPlaceToPage elog(ERROR)
// (row a186-candidate-fp-gin-ginentrypage-c1f7600a20858b95f2d1-1).

#[test]
fn exec_place_to_page_at_bad_offset_is_internal_error() {
    use crate::btree::GinBt;
    install();
    set_pages(vec![metapage(), gin_page(GIN_LEAF)], 0);
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx, KeyKind::Int4);
    let state = one_col_state(KeyKind::Int4);
    let entry = pending_tuple(mcx, &rel, &state, 1, tid(1, 1));
    let mut btree =
        crate::entrypage::EntryBtree::new(&rel, &state, 1, Datum::from_i32(1), GIN_CAT_NORM_KEY, mcx);
    btree.payload = Some(crate::entrypage::EntryPayload { entry, is_delete: false });

    // Offset 3 on an empty leaf: PageAddItem refuses (offset past maxoff+1).
    let err = btree
        .exec_place_to_page(2, 3, InvalidBlockNumber)
        .err()
        .expect("PageAddItem failure must be a catchable error");
    assert_eq!(err.message(), "failed to add item to index page in \"t_gin\"");
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
}

// --- ginentrypage.c:312 entryLocateEntry key extraction: a corrupt varlena
// key raises ERRCODE_DATA_CORRUPTED from gintuple_get_key; the search
// callbacks must propagate it, not .expect() it into a panic
// (row a186-candidate-fp-gin-ginentrypage-74ab5a1f91c0dfb595e8-1).

#[test]
fn leaf_search_over_corrupt_varlena_key_does_not_panic() {
    install();
    // One 16-byte tuple whose 4-byte varlena header declares ~1GB.
    let mut leaf = gin_page(GIN_LEAF);
    #[repr(C, align(8))]
    struct Buf([u8; 16]);
    let mut b = Buf([0u8; 16]);
    let word = ::types_tuple::varatt::set_varsize_4b_word(0x3FFF_FFFF).to_ne_bytes();
    b.0[8..12].copy_from_slice(&word);
    b.0[6..8].copy_from_slice(&16u16.to_ne_bytes());
    // SAFETY: exclusive access to a BLCKSZ image.
    let mut pm = unsafe { PageMut::from_raw(core::ptr::NonNull::new(leaf.0.as_mut_ptr()).unwrap()) };
    assert!(pm.add_item(&b.0, FirstOffsetNumber, 0).is_some());
    set_pages(vec![metapage(), leaf], 0);

    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx, KeyKind::Text);
    let state = one_col_state(KeyKind::Text);
    let key = Datum::from_usize(b.0.as_ptr() as usize + 8);
    let btree = crate::entrypage::EntryBtree::new(&rel, &state, 1, key, GIN_CAT_NORM_KEY, mcx);

    // A panic here fails the test outright, so no unwind catch is needed
    // (the unwind-policy gate forbids one outside a tests/ tree or tests.rs).
    let res = crate::insert::entry_locate_leaf_pub(&btree, 2);
    let err = res
        .err()
        .expect("corrupt key must raise ERRCODE_DATA_CORRUPTED as a catchable error, not succeed");
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_DATA_CORRUPTED);
}
