//! Seam declarations for the `backend-access-nbtree-core` unit (the combined
//! `nbtsearch.c` / `nbtinsert.c` / `nbtpage.c` / `nbtutils.c` / `nbtdedup.c`
//! module): the cross-module B-tree functions the `nbtree.c` AM entry points
//! call (descent/scan, insertion, page-deletion, VACUUM cycle-id and pending
//! FSM, page-format reads).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! Every seam whose C counterpart takes `Relation` carries
//! `&types_rel::Relation<'mcx>`, the faithful open-handle shape. Allocating
//! seams take `Mcx<'mcx>` and return `PgResult`; reads that can `ereport`
//! return `PgResult`.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::primitive::{BlockNumber, OffsetNumber};
use types_error::PgResult;
use types_nbtree::{
    BTCycleId, BTScanInsert, BTScanOpaqueData, BTStack, BTVacState, BTVacuumPosting,
    IndexUniqueCheck, TmIndexDeleteOp,
};
use types_rel::Relation;
use types_scan::sdir::ScanDirection;
use types_storage::storage::Buffer;
use types_tuple::heaptuple::ItemPointerData;

// === btinsert (nbtinsert.c) ================================================

seam_core::seam!(
    /// `_bt_doinsert(rel, itup, checkUnique, indexUnchanged, heapRel)`
    /// (nbtinsert.c): insert one index tuple, performing unique checking.
    /// Returns whether the tuple was inserted (false on a partial-check
    /// conflict). `Err` carries the unique-violation / corruption ereports.
    pub fn bt_doinsert<'mcx>(
        rel: &Relation<'mcx>,
        itup: &[u8],
        check_unique: IndexUniqueCheck,
        index_unchanged: bool,
        heap_rel: &Relation<'mcx>,
    ) -> PgResult<bool>
);

// === btgettuple / btgetbitmap descent (nbtsearch.c) ========================

seam_core::seam!(
    /// `_bt_first(scan, dir)` (nbtsearch.c): find the first matching tuple and
    /// position the scan; returns whether a tuple was found.
    pub fn bt_first<'mcx>(
        rel: &Relation<'mcx>,
        so: &mut BTScanOpaqueData<'mcx>,
        dir: ScanDirection,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `_bt_next(scan, dir)` (nbtsearch.c): advance the scan and return the
    /// next matching tuple, or false at the end of the current primitive scan.
    pub fn bt_next<'mcx>(
        rel: &Relation<'mcx>,
        so: &mut BTScanOpaqueData<'mcx>,
        dir: ScanDirection,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `_bt_start_prim_scan(scan, dir)` (nbtutils.c): decide whether the array
    /// keys require another primitive index scan, advancing them if so.
    pub fn bt_start_prim_scan<'mcx>(
        rel: &Relation<'mcx>,
        so: &mut BTScanOpaqueData<'mcx>,
        dir: ScanDirection,
    ) -> bool
);

seam_core::seam!(
    /// `_bt_start_array_keys(scan, dir)` (nbtutils.c): reset the scan's array
    /// keys to their initial positions for the given direction.
    pub fn bt_start_array_keys<'mcx>(
        rel: &Relation<'mcx>,
        so: &mut BTScanOpaqueData<'mcx>,
        dir: ScanDirection,
    )
);

seam_core::seam!(
    /// `_bt_killitems(scan)` (nbtutils.c): mark the scan's killed items
    /// LP_DEAD on the current leaf page.
    pub fn bt_killitems<'mcx>(rel: &Relation<'mcx>, so: &mut BTScanOpaqueData<'mcx>)
);

seam_core::seam!(
    /// `so->currPos.items[so->currPos.itemIndex].heapTid` — the heap TID of
    /// the scan's current position (read by btgetbitmap after `_bt_first`).
    pub fn current_heaptid<'mcx>(so: &BTScanOpaqueData<'mcx>) -> ItemPointerData
);

seam_core::seam!(
    /// `(IndexTuple) (so->currTuples + so->currPos.items[itemIndex].tupleOffset)`
    /// — the index tuple at the scan's current position, for index-only scans
    /// (C: `_bt_returnitem` sets `scan->xs_itup` to this pointer-into-workspace).
    /// Returns the contiguous on-disk byte image (sized by `IndexTupleSize`
    /// read from the header), copied out of the per-scan `currTuples` workspace.
    /// `None` when the scan is not index-only (`so->currTuples == NULL`), which
    /// is the C `scan->xs_itup` left unset.
    pub fn current_itup<'mcx>(mcx: Mcx<'mcx>, so: &BTScanOpaqueData<'mcx>)
        -> PgResult<Option<PgVec<'mcx, u8>>>
);

// === btbuildempty / metapage + page helpers (nbtpage.c) ====================

seam_core::seam!(
    /// `_bt_allequalimage(index, debugmessage = false)` (nbtutils.c): are all
    /// index columns "equalimage" (deduplication-safe)? `Err` carries the
    /// opclass-lookup ereports.
    pub fn bt_allequalimage<'mcx>(rel: &Relation<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `smgr_bulk_start_rel(index, INIT_FORKNUM)` + `_bt_initmetapage` at
    /// `BTREE_METAPAGE` + `smgr_bulk_write` + `smgr_bulk_finish` (nbtpage.c /
    /// bulk_write.c): build the empty-index metapage in the init fork. `Err`
    /// carries the smgr write ereports.
    pub fn build_empty_metapage<'mcx>(index: &Relation<'mcx>, allequalimage: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `_bt_getrootheight(rel)` (nbtpage.c): the height of the tree (number of
    /// levels above the leaf), for `btgettreeheight`. `Err` carries the
    /// metapage-read ereports.
    pub fn bt_getrootheight<'mcx>(rel: &Relation<'mcx>) -> PgResult<i32>
);

// === VACUUM cycle-id shmem (nbtutils.c) ====================================

seam_core::seam!(
    /// `_bt_start_vacuum(rel)` (nbtutils.c): register this VACUUM in the
    /// shared `btvacinfo` array and return its cycle ID. `Err` carries the
    /// "multiple active vacuums" / out-of-slots ereports.
    pub fn bt_start_vacuum<'mcx>(rel: &Relation<'mcx>) -> PgResult<BTCycleId>
);

seam_core::seam!(
    /// `_bt_end_vacuum(rel)` (nbtutils.c): release this VACUUM's shared cycle
    /// slot (the `_bt_end_vacuum_callback` cleanup is owned by the seam).
    pub fn bt_end_vacuum<'mcx>(rel: &Relation<'mcx>)
);

// === btvacuumcleanup helpers (nbtpage.c) ===================================

seam_core::seam!(
    /// `_bt_vacuum_needs_cleanup(rel)` (nbtpage.c): decide whether a
    /// cleanup-only `btvacuumscan` is needed. `Err` carries the metapage-read
    /// ereports.
    pub fn bt_vacuum_needs_cleanup<'mcx>(rel: &Relation<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `_bt_set_cleanup_info(rel, num_delpages)` (nbtpage.c): record
    /// `num_delpages` in the metapage for the next `_bt_vacuum_needs_cleanup`.
    /// `Err` carries the metapage-write ereports.
    pub fn bt_set_cleanup_info<'mcx>(rel: &Relation<'mcx>, num_delpages: BlockNumber) -> PgResult<()>
);

seam_core::seam!(
    /// `_bt_pendingfsm_init(rel, vstate, cleanuponly)` (nbtpage.c): size the
    /// `BTVacState` pending-FSM buffer for the run.
    pub fn bt_pendingfsm_init<'mcx>(
        rel: &Relation<'mcx>,
        vstate: &mut BTVacState<'mcx>,
        cleanuponly: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `_bt_pendingfsm_finalize(rel, vstate)` (nbtpage.c): place now-safe
    /// pending pages into the FSM. Carries `heaprel` for the GlobalVis
    /// recompute, which the caller has in scope. `Err` carries write ereports.
    pub fn bt_pendingfsm_finalize<'mcx>(
        rel: &Relation<'mcx>,
        heaprel: &Relation<'mcx>,
        vstate: &mut BTVacState<'mcx>,
    ) -> PgResult<()>
);

// === btvacuumpage page-deletion protocol (nbtpage.c) =======================

seam_core::seam!(
    /// `_bt_lockbuf(rel, buf, BT_READ)` (nbtpage.c): acquire the btree read
    /// lock on a buffer.
    pub fn bt_lockbuf<'mcx>(rel: &Relation<'mcx>, buf: Buffer)
);

seam_core::seam!(
    /// `_bt_relbuf(rel, buf)` (nbtpage.c): release lock and pin on a buffer.
    pub fn bt_relbuf<'mcx>(rel: &Relation<'mcx>, buf: Buffer)
);

seam_core::seam!(
    /// `_bt_checkpage(rel, buf)` (nbtpage.c): sanity-check a freshly read
    /// page. `Err` carries the index-corruption ereport.
    pub fn bt_checkpage<'mcx>(rel: &Relation<'mcx>, buf: Buffer) -> PgResult<()>
);

seam_core::seam!(
    /// `_bt_upgradelockbufcleanup(rel, buf)` (nbtpage.c): trade the read lock
    /// for a cleanup lock on a leaf page.
    pub fn bt_upgradelockbufcleanup<'mcx>(rel: &Relation<'mcx>, buf: Buffer)
);

seam_core::seam!(
    /// `_bt_page_is_recyclable(page, heaprel)` (nbtpage.c): is a deleted page
    /// now safe to recycle (its `safexid` old enough)?
    pub fn bt_page_is_recyclable<'mcx>(page: &[u8], heaprel: &Relation<'mcx>) -> bool
);

seam_core::seam!(
    /// `_bt_delitems_vacuum(rel, buf, deletable, updatable)` (nbtpage.c):
    /// apply the page's deletions/updates and WAL-log them. Consumes the
    /// to-delete offsets and updatable posting metadata. `Err` carries write
    /// ereports.
    pub fn bt_delitems_vacuum<'mcx>(
        rel: &Relation<'mcx>,
        buf: Buffer,
        deletable: PgVec<'mcx, OffsetNumber>,
        updatable: PgVec<'mcx, BTVacuumPosting<'mcx>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `_bt_pagedel(rel, heaprel, buf, vstate)` (nbtpage.c): delete the
    /// half-dead/empty page chain starting at `buf`, maintaining `vstate`
    /// bulk-delete stats; releases the buffer. `Err` carries write ereports.
    pub fn bt_pagedel<'mcx>(
        rel: &Relation<'mcx>,
        heaprel: &Relation<'mcx>,
        buf: Buffer,
        vstate: &mut BTVacState<'mcx>,
    ) -> PgResult<()>
);

// === page-format opaque/tuple reads (nbtree.h inline / bufpage.c) ==========

seam_core::seam!(
    /// `PageIsNew(page)` (bufpage.h): is the page all-zero (never initialized)?
    pub fn page_is_new(page: &[u8]) -> bool
);

seam_core::seam!(
    /// `BTPageGetOpaque(page)` (nbtree.h): read `(btpo_flags, btpo_cycleid,
    /// btpo_next)` from the page's special area.
    pub fn page_opaque(page: &[u8]) -> (u16, BTCycleId, BlockNumber)
);

seam_core::seam!(
    /// `BTPageGetOpaque(page)->btpo_level` (nbtree.h): the tree level recorded
    /// in the page's special area (zero for leaf pages). Read separately from
    /// [`page_opaque`] because amcheck needs the level for downlink-chain
    /// verification while the common callers do not.
    pub fn page_btpo_level(page: &[u8]) -> u32
);

seam_core::seam!(
    /// `opaque->btpo_cycleid = 0` written into the page in the shared buffer.
    pub fn page_clear_cycleid(buf: Buffer)
);

seam_core::seam!(
    /// `PageGetMaxOffsetNumber(page)` (bufpage.h): the highest line-pointer
    /// offset in use on the page.
    pub fn page_get_max_offset_number(page: &[u8]) -> OffsetNumber
);

seam_core::seam!(
    /// `PageGetItem(page, PageGetItemId(page, offnum))` (bufpage.h): the index
    /// tuple at `offnum`, returned as owned bytes in `mcx`. `Err` carries OOM.
    pub fn page_get_item<'mcx>(mcx: Mcx<'mcx>, page: &[u8], offnum: OffsetNumber) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `BTreeTupleIsPivot(itup)` (nbtree.h).
    pub fn tuple_is_pivot(itup: &[u8]) -> bool
);

seam_core::seam!(
    /// `BTreeTupleIsPosting(itup)` (nbtree.h).
    pub fn tuple_is_posting(itup: &[u8]) -> bool
);

seam_core::seam!(
    /// `itup->t_tid` — the heap TID of a regular (non-posting) index tuple.
    pub fn tuple_heap_tid(itup: &[u8]) -> ItemPointerData
);

seam_core::seam!(
    /// `BTreeTupleGetNPosting(itup)` (nbtree.h): the number of TIDs in a
    /// posting-list tuple.
    pub fn tuple_n_posting(itup: &[u8]) -> i32
);

seam_core::seam!(
    /// `BTreeTupleGetPostingN(itup, n)` (nbtree.h): the `n`th heap TID in a
    /// posting-list tuple.
    pub fn tuple_posting_tid(itup: &[u8], n: i32) -> ItemPointerData
);

// === nbtutils.c =============================================================

seam_core::seam!(
    /// `_bt_keep_natts_fast(rel, lastleft, firstright)` (nbtutils.c): a faster,
    /// opclass-oblivious variant of `_bt_keep_natts` based on
    /// `datum_image_eq()`. Returns the number of attributes that must be kept
    /// to distinguish the two tuples; a value `> IndexRelationGetNumberOfKey`
    /// `Attributes(rel)` means the two tuples are equal across all key columns.
    /// `lastleft`/`firstright` are page-item byte slices.
    pub fn bt_keep_natts_fast<'mcx>(
        rel: &Relation<'mcx>,
        lastleft: &[u8],
        firstright: &[u8],
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `_bt_delitems_delete_check(rel, buf, heapRel, delstate)` (nbtpage.c):
    /// ask the tableam which of `delstate`'s TIDs are deletable, then
    /// physically delete the corresponding index entries from `buf`'s page
    /// (WAL-logged inside its own critical section). Consumes `delstate`.
    /// `Err` carries the tableam / WAL `ereport(ERROR)`s.
    pub fn bt_delitems_delete_check<'mcx>(
        rel: &Relation<'mcx>,
        buf: Buffer,
        heap_rel: &Relation<'mcx>,
        delstate: TmIndexDeleteOp<'mcx>,
    ) -> PgResult<()>
);

// === search / scankey / page-format reads consumed by amcheck =============
//
// These are owned by `backend-access-nbtree-core` (nbtsearch.c / nbtutils.c /
// nbtpage.c / nbtdedup.c), which is still `todo`, so every call panics loudly
// until that unit lands and installs them from its `init_seams()` — exactly
// like `bt_keep_natts_fast` / `bt_delitems_delete_check` above. The first
// consumer is `contrib-amcheck-verify-nbtree` (verify_nbtree.c), whose
// per-page invariant engine builds insertion scankeys and descends the tree.

seam_core::seam!(
    /// `_bt_mkscankey(rel, itup)` (nbtutils.c): build an insertion scankey
    /// (`BTScanInsert`) from an index tuple (or with `itup == NULL`, from the
    /// index's leftmost-key template). `Err` carries opclass-lookup ereports.
    pub fn bt_mkscankey<'mcx>(rel: &Relation<'mcx>, itup: Option<&[u8]>) -> PgResult<BTScanInsert<'mcx>>
);

seam_core::seam!(
    /// `_bt_compare(rel, key, page, offnum)` (nbtsearch.c): compare insertion
    /// scankey `key` against the tuple at `offnum` on `page`. Returns `<0`,
    /// `0`, `>0` per the 3-way ORDER semantics. `Err` carries the comparison
    /// support-function ereports.
    pub fn bt_compare<'mcx>(
        rel: &Relation<'mcx>,
        key: &BTScanInsert<'mcx>,
        page: &[u8],
        offnum: OffsetNumber,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `_bt_search(rel, heaprel, key, bufP, access)` (nbtsearch.c): descend
    /// the tree to the leaf page where `key` belongs, returning the parent
    /// `BTStack` and the located buffer (in `*bufP`). `access` is `BT_READ`
    /// (false) or `BT_WRITE` (true). `Err` carries page-read ereports.
    pub fn bt_search<'mcx>(
        rel: &Relation<'mcx>,
        heaprel: &Relation<'mcx>,
        key: &BTScanInsert<'mcx>,
        access_write: bool,
    ) -> PgResult<(BTStack, Buffer)>
);

seam_core::seam!(
    /// `_bt_moveright(rel, heaprel, key, buf, forupdate, stack, access)`
    /// (nbtsearch.c): follow right-links until `buf` is the page on which
    /// `key` belongs (handling concurrent splits). Returns the (possibly new)
    /// buffer. `Err` carries page-read ereports.
    pub fn bt_moveright<'mcx>(
        rel: &Relation<'mcx>,
        heaprel: &Relation<'mcx>,
        key: &BTScanInsert<'mcx>,
        buf: Buffer,
        forupdate: bool,
        access_write: bool,
    ) -> PgResult<Buffer>
);

seam_core::seam!(
    /// `_bt_binsrch(rel, key, buf)` (nbtsearch.c): binary-search the page in
    /// `buf` for the offset where `key` belongs (the bare, non-insertion
    /// variant). `Err` carries comparison-support ereports.
    pub fn bt_binsrch<'mcx>(
        rel: &Relation<'mcx>,
        key: &BTScanInsert<'mcx>,
        buf: Buffer,
    ) -> PgResult<OffsetNumber>
);

seam_core::seam!(
    /// `_bt_binsrch_insert(rel, insertstate)` (nbtsearch.c): binary search for
    /// the insert position of `insertstate->itup`, caching bounds in
    /// `insertstate` and returning the offset. `Err` carries comparison-support
    /// ereports.
    pub fn bt_binsrch_insert<'mcx>(
        rel: &Relation<'mcx>,
        insertstate: &mut types_nbtree::BTInsertStateData<'mcx>,
    ) -> PgResult<OffsetNumber>
);

seam_core::seam!(
    /// `_bt_metaversion(rel, heapkeyspace, allequalimage)` (nbtpage.c): read
    /// the metapage and report whether the index is heapkeyspace and
    /// allequalimage. `Err` carries metapage-read ereports.
    pub fn bt_metaversion<'mcx>(rel: &Relation<'mcx>) -> PgResult<(bool, bool)>
);

seam_core::seam!(
    /// `_bt_check_natts(rel, heapkeyspace, page, offnum)` (nbtutils.c): verify
    /// that the tuple at `offnum` has a sane number of attributes for its page
    /// kind (leaf/internal, pivot/non-pivot). Returns whether the count is
    /// legal. `Err` carries page-read ereports.
    pub fn bt_check_natts<'mcx>(
        rel: &Relation<'mcx>,
        heapkeyspace: bool,
        page: &[u8],
        offnum: OffsetNumber,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `_bt_form_posting(base, htids, nhtids)` (nbtdedup.c): build a posting-
    /// list index tuple from `base` and the heap TID array, returned as owned
    /// bytes in `mcx`. `Err` carries OOM.
    pub fn bt_form_posting<'mcx>(
        mcx: Mcx<'mcx>,
        base: &[u8],
        htids: &[ItemPointerData],
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `_bt_freestack(stack)` (nbtsearch.c): free a `BTStack` chain. With the
    /// owned boxed `BTStack` model this is a drop; the seam exists to mirror
    /// the C call site exactly.
    pub fn bt_freestack(stack: BTStack)
);

seam_core::seam!(
    /// `_bt_allequalimage(index, debugmessage)` (nbtutils.c): are all index
    /// columns "equalimage" (deduplication-safe)? Distinct decl from the
    /// `bt_allequalimage` above (the btbuildempty caller passes no debug
    /// flag); this variant carries the `debugmessage` argument amcheck and
    /// `_bt_allequalimage`'s general callers use. `Err` carries opclass-lookup
    /// ereports.
    pub fn bt_allequalimage_dbg<'mcx>(
        rel: &Relation<'mcx>,
        debugmessage: bool,
    ) -> PgResult<bool>
);
