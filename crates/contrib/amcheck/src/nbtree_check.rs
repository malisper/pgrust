#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use core::ptr::{copy_nonoverlapping, NonNull};
use core::slice::from_raw_parts;

use ::bloomfilter::BloomFilter;
use ::datum::Datum;
use ::detoast::detoast_attr;
use ::execindexing::{table_index_build_scan, BuildIndexInfo, IndexInfo};
use ::mcx::{vec_from_elem_in, Mcx, MemoryContext};
use ::snapmgr::{GetTransactionSnapshot, RegisterSnapshot, Snapshot, UnregisterSnapshot};
use ::types_core::{
    AttrNumber, BlockNumber, ForkNumber, InvalidBlockNumber, Oid, OffsetNumber, XLogRecPtr,
    BLCKSZ, BTREE_AM_OID, INDEX_MAX_KEYS,
};
use ::types_error::{
    PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INDEX_CORRUPTED,
};
use ::types_rel::Relation;
use ::types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};
use ::types_storage::bufpage::{ItemIdData, PageRef, SizeOfPageHeaderData};
use ::types_storage::lock::{AccessShareLock, ShareLock};
use ::types_storage::relfilelocator::RelFileLocatorBackend;
use ::types_storage::ReadBufferMode;
use ::types_tuple::itemptr::{
    ItemPointerData, ItemPointerCompare, ItemPointerGetBlockNumberNoCheck,
    ItemPointerGetOffsetNumberNoCheck, ItemPointerIsValid,
};
use ::types_tuple::varatt::{
    set_varsize_short, varatt_can_make_short, varatt_converted_short_size, varatt_is_1b_e,
    varsize_4b, varsize_any, VARHDRSZ,
};
use ::types_tuple::{TYPSTORAGE_EXTENDED, TYPSTORAGE_MAIN};

use ::types_nbtree::{
    BTMaxItemSize, BTMaxItemSizeNoHeapTid, BTPageOpaqueData, BTREE_MAGIC, BTREE_METAPAGE,
    BTREE_MIN_VERSION, BTREE_VERSION, MaxTIDsPerBTreePage, INDEX_ALT_TID_MASK, P_FIRSTDATAKEY,
    P_HAS_FULLXID, P_HAS_GARBAGE, P_HIKEY, P_IGNORE, P_INCOMPLETE_SPLIT, P_ISDELETED, P_ISHALFDEAD,
    P_ISLEAF, P_ISMETA, P_ISROOT, P_NONE, P_RIGHTMOST,
};

use ::nbtree::amcheck::{
    bt_allequalimage, bt_check_natts, bt_checkpage_ref, bt_compare, bt_rootdescend, page_item,
    page_meta, page_opaque, OrderProcFrame,
};
use ::nbtree::itup::{
    bt_tuple_get_downlink, bt_tuple_get_heap_tid, bt_tuple_get_max_heap_tid, bt_tuple_get_natts,
    bt_tuple_get_nposting, bt_tuple_get_posting_n, bt_tuple_get_posting_offset, bt_tuple_is_pivot,
    bt_tuple_is_posting, index_form_tuple, index_getattr, index_tuple_size, maxalign, set_t_info,
    set_t_tid, t_info, t_tid, ItupBuf, ITup, INDEX_SIZE_MASK,
};
use ::nbtree::{bt_metaversion, bt_mkscankey, BtScanInsert};

use ::bufmgr::{
    buffer_page_ref, GetAccessStrategy, LockBuffer, ReadBufferExtended,
    RelationGetNumberOfBlocksInFork, UnlockReleaseBuffer, BUFFER_LOCK_SHARE,
};

const INVALID_BTREE_LEVEL: u32 = InvalidBlockNumber;
const INTERVAL_BTREE_FAM_OID: Oid = 1982;
const OPAQUE_MAXALIGN: usize = 16;
// C divergence: C's TOAST_INDEX_TARGET is MaxHeapTupleSize/16; here 8160/4 to match nbtree::itup's index_form_tuple.
const TOAST_INDEX_TARGET: usize = 8160 / 4;
const IMK: usize = INDEX_MAX_KEYS as usize;

fn fmt_lsn(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

fn fmt_tid(tid: &ItemPointerData) -> String {
    format!(
        "({},{})",
        ItemPointerGetBlockNumberNoCheck(tid),
        ItemPointerGetOffsetNumberNoCheck(tid)
    )
}

fn index_corrupted(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INDEX_CORRUPTED))
}


struct PageImage {
    words: Box<[u64]>,
}

impl PageImage {
    fn zeroed() -> Self {
        PageImage {
            words: vec![0u64; BLCKSZ / 8].into_boxed_slice(),
        }
    }

    #[inline]
    fn ptr(&self) -> *const u8 {
        self.words.as_ptr().cast()
    }

    #[inline]
    fn ptr_mut(&mut self) -> *mut u8 {
        self.words.as_mut_ptr().cast()
    }

    // SAFETY: the owned image is 8-aligned and BLCKSZ bytes, so the PageRef is valid.
    #[inline]
    fn page(&self) -> PageRef<'_> {
        unsafe { PageRef::from_raw(NonNull::new_unchecked(self.ptr() as *mut u8)) }
    }
}

struct OwnedTuple {
    words: Box<[u64]>,
}

impl OwnedTuple {
    /// SAFETY: `itup` is a live index-tuple image (pin held / private copy).
    unsafe fn from_itup(itup: ITup) -> Self {
        let len = index_tuple_size(itup);
        let mut words = vec![0u64; len.div_ceil(8)].into_boxed_slice();
        copy_nonoverlapping(itup, words.as_mut_ptr().cast(), len);
        OwnedTuple { words }
    }

    #[inline]
    fn as_itup(&self) -> ITup {
        self.words.as_ptr().cast()
    }
}


struct BtreeCheckState<'mcx> {
    mcx: Mcx<'mcx>,
    scratch: MemoryContext,
    rel: Relation<'mcx>,
    heaprel: Relation<'mcx>,
    heapkeyspace: bool,
    readonly: bool,
    heapallindexed: bool,
    rootdescend: bool,
    checkunique: bool,
    checkstrategy: BufferAccessStrategy,
    indexinfo: Option<IndexInfo<'mcx>>,
    snapshot: Option<Snapshot>,
    target: Option<PageImage>,
    targetblock: BlockNumber,
    targetlsn: XLogRecPtr,
    lowkey: Option<OwnedTuple>,
    prevrightlink: BlockNumber,
    previncompletesplit: bool,
    filter: Option<BloomFilter<'mcx>>,
    heaptuplespresent: i64,
}

impl<'mcx> BtreeCheckState<'mcx> {
    #[inline]
    fn target_page(&self) -> PageRef<'_> {
        self.target.as_ref().expect("target page set").page()
    }

    #[inline]
    fn ii_unique(&self) -> bool {
        self.indexinfo.as_ref().map(|i| i.ii_Unique).unwrap_or(false)
    }
}

#[derive(Clone, Copy)]
struct BtreeLevel {
    level: u32,
    leftmost: BlockNumber,
    istruerootlevel: bool,
}

#[derive(Clone, Copy)]
struct BtreeLastVisibleEntry {
    blkno: BlockNumber,
    offset: OffsetNumber,
    posting_index: i32,
    tid: Option<ItemPointerData>,
}


pub(crate) fn bt_index_check_internal(
    mcx: Mcx<'_>,
    indrelid: Oid,
    parentcheck: bool,
    heapallindexed: bool,
    rootdescend: bool,
    checkunique: bool,
) -> PgResult<()> {
    let lockmode = if parentcheck { ShareLock } else { AccessShareLock };
    crate::common::amcheck_lock_relation_and_check(
        mcx,
        indrelid,
        BTREE_AM_OID,
        lockmode,
        |indrel, heaprel, readonly| {
            bt_index_check_callback(
                mcx,
                indrel,
                heaprel,
                readonly,
                heapallindexed,
                rootdescend,
                checkunique,
            )
        },
    )
}

fn bt_index_check_callback<'mcx>(
    mcx: Mcx<'mcx>,
    indrel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    readonly: bool,
    heapallindexed: bool,
    rootdescend: bool,
    checkunique: bool,
) -> PgResult<()> {
    if !::smgr::smgrexists(rel_smgr_key(indrel)?, ForkNumber::MAIN_FORKNUM)? {
        return Err(index_corrupted(format!(
            "index \"{}\" lacks a main relation fork",
            indrel.name()
        )));
    }

    let (heapkeyspace, allequalimage) = bt_metaversion(indrel)?;
    if allequalimage && !heapkeyspace {
        return Err(index_corrupted(format!(
            "index \"{}\" metapage has equalimage field set on unsupported nbtree version",
            indrel.name()
        )));
    }
    if allequalimage && !bt_allequalimage(indrel)? {
        for i in 0..indrel.indnkeyatts() as usize {
            if indrel.rd_opfamily[i] == INTERVAL_BTREE_FAM_OID {
                return Err(Box::new(
                    PgError::error(format!(
                        "index \"{}\" metapage incorrectly indicates that deduplication is safe",
                        indrel.name()
                    ))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                    .with_hint(
                        "This is known of \"interval\" indexes last built on a version predating 2023-11.",
                    ),
                ));
            }
        }
    }

    bt_check_every_level(
        mcx,
        indrel,
        heaprel,
        heapkeyspace,
        readonly,
        heapallindexed,
        rootdescend,
        checkunique,
    )
}

fn rel_smgr_key(rel: &Relation<'_>) -> PgResult<RelFileLocatorBackend> {
    let locator = rel.rd_locator.get();
    ::smgr::smgropen(locator, rel.rd_backend)?;
    Ok(RelFileLocatorBackend {
        locator,
        backend: rel.rd_backend,
    })
}

fn bt_check_every_level<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    heapkeyspace: bool,
    readonly: bool,
    heapallindexed: bool,
    rootdescend: bool,
    checkunique: bool,
) -> PgResult<()> {
    let mut state = BtreeCheckState {
        mcx,
        scratch: MemoryContext::new_bump("amcheck context"),
        rel: rel.alias(),
        heaprel: heaprel.alias(),
        heapkeyspace,
        readonly,
        heapallindexed,
        rootdescend,
        checkunique,
        checkstrategy: GetAccessStrategy(BufferAccessStrategyType::BasBulkread),
        indexinfo: None,
        snapshot: None,
        target: None,
        targetblock: InvalidBlockNumber,
        targetlsn: 0,
        lowkey: None,
        prevrightlink: InvalidBlockNumber,
        previncompletesplit: false,
        filter: None,
        heaptuplespresent: 0,
    };

    if state.heapallindexed {
        let total_pages =
            RelationGetNumberOfBlocksInFork(&state.rel, ForkNumber::MAIN_FORKNUM)? as i64;
        let total_elems = (total_pages * (MaxTIDsPerBTreePage as i64 / 3))
            .max(state.rel.rd_rel.reltuples as i64);
        // C divergence: a fixed seed (C draws pg_prng_uint64); correctness does not depend on the seed within a single add-then-probe pass.
        let seed: u64 = 0;
        let work_mem = ::init_small::globals::maintenance_work_mem();
        state.filter = Some(BloomFilter::create_in(state.mcx, total_elems, work_mem, seed)?);
        state.heaptuplespresent = 0;

        let snap = GetTransactionSnapshot()?;
        state.snapshot = RegisterSnapshot(Some(&snap))?;
        // C divergence: the IsolationUsesXactSnapshot / indcheckxmin serialization guard is not enforced; behaviour-preserving for READ COMMITTED.
    }

    if state.checkunique {
        let indexinfo = BuildIndexInfo(state.mcx, &state.rel)?;
        let need_snapshot = indexinfo.ii_Unique && state.snapshot.is_none();
        state.indexinfo = Some(indexinfo);
        if need_snapshot {
            let snap = GetTransactionSnapshot()?;
            state.snapshot = RegisterSnapshot(Some(&snap))?;
        }
    }

    if state.rootdescend && !state.heapkeyspace {
        return Err(Box::new(
            PgError::error(format!(
                "cannot verify that tuples from index \"{}\" can each be found by an independent index search",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_hint("Only B-Tree version 4 indexes support rootdescend verification."),
        ));
    }

    let metapage = palloc_btree_page(&state, BTREE_METAPAGE)?;
    let metad = page_meta(&metapage.page());
    drop(metapage);
    // C divergence: the DEBUG1 "harmless fast root mismatch" report is omitted.

    let mut previouslevel = INVALID_BTREE_LEVEL;
    let mut current = BtreeLevel {
        level: metad.btm_level,
        leftmost: metad.btm_root,
        istruerootlevel: true,
    };
    while current.leftmost != P_NONE {
        current = bt_check_level_from_leftmost(&mut state, current)?;
        if current.leftmost == InvalidBlockNumber {
            return Err(index_corrupted(format!(
                "index \"{}\" has no valid pages on level below {} or first level",
                state.rel.name(),
                previouslevel
            )));
        }
        previouslevel = current.level;
    }

    if state.heapallindexed {
        let scan_mcx = state.mcx;
        let rel_alias = state.rel.alias();
        let heaprel_alias = state.heaprel.alias();
        let readonly = state.readonly;

        let mut indexinfo = BuildIndexInfo(scan_mcx, &state.rel)?;
        indexinfo.ii_Concurrent = true;
        indexinfo.ii_Unique = false;
        indexinfo.ii_HasExclusion = false;

        let filter = state.filter.as_mut().expect("filter set for heapallindexed");
        let heaptuplespresent = &mut state.heaptuplespresent;
        let scratch = &mut state.scratch;

        // C divergence: table_index_build_scan builds its own heap scan and does not read our registered snapshot (acceptable for committed data).
        table_index_build_scan(
            scan_mcx,
            &heaprel_alias,
            &rel_alias,
            &mut indexinfo,
            true,
            |index, tid, values, isnull, _alive| {
                bt_tuple_present_callback(
                    scratch,
                    filter,
                    heaptuplespresent,
                    &heaprel_alias,
                    readonly,
                    index,
                    tid,
                    values,
                    isnull,
                )
            },
        )?;
        // C divergence: the DEBUG1 "finished verifying presence" report is omitted.
    }

    if let Some(snap) = state.snapshot.as_ref() {
        UnregisterSnapshot(Some(snap));
    }
    Ok(())
}

fn bt_check_level_from_leftmost<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    level: BtreeLevel,
) -> PgResult<BtreeLevel> {
    let mut nextleveldown = BtreeLevel {
        leftmost: InvalidBlockNumber,
        level: INVALID_BTREE_LEVEL,
        istruerootlevel: false,
    };

    let mut leftcurrent: BlockNumber = P_NONE;
    let mut current: BlockNumber = level.leftmost;

    state.prevrightlink = InvalidBlockNumber;
    state.previncompletesplit = false;

    loop {
        state.targetblock = current;
        let page = palloc_btree_page(state, state.targetblock)?;
        state.targetlsn = page.page().lsn();
        state.target = Some(page);

        let opaque = page_opaque(&state.target_page());
        let mut skip_middle = false;

        if P_IGNORE(&opaque) {
            if state.readonly && P_ISDELETED(&opaque) {
                return Err(Box::new(
                    PgError::error(format!(
                        "downlink or sibling link points to deleted block in index \"{}\"",
                        state.rel.name()
                    ))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                    .with_detail(format!(
                        "Block={} left block={} left link from block={}.",
                        current, leftcurrent, opaque.btpo_prev
                    )),
                ));
            }
            if P_RIGHTMOST(&opaque) {
                return Err(index_corrupted(format!(
                    "block {} fell off the end of index \"{}\"",
                    current,
                    state.rel.name()
                )));
            }
            // C divergence: DEBUG1 "concurrently deleted" omitted.
            skip_middle = true;
        } else if nextleveldown.leftmost == InvalidBlockNumber {
            if state.readonly {
                if !bt_leftmost_ignoring_half_dead(state, current, &state.target_page())? {
                    return Err(index_corrupted(format!(
                        "block {} is not leftmost in index \"{}\"",
                        current,
                        state.rel.name()
                    )));
                }
                if level.istruerootlevel && !P_ISROOT(&opaque) && !P_INCOMPLETE_SPLIT(&opaque) {
                    return Err(index_corrupted(format!(
                        "block {} is not true root in index \"{}\"",
                        current,
                        state.rel.name()
                    )));
                }
            }

            if !P_ISLEAF(&opaque) {
                let itemid = page_get_item_id_careful(
                    state,
                    state.targetblock,
                    &state.target_page(),
                    P_FIRSTDATAKEY(&opaque),
                )?;
                let itup = page_item(&state.target_page(), itemid);
                // SAFETY: itup is a live pivot on the (alive) target page copy.
                nextleveldown.leftmost = unsafe { bt_tuple_get_downlink(itup) };
                nextleveldown.level = opaque.btpo_level - 1;
            } else {
                nextleveldown.leftmost = P_NONE;
                nextleveldown.level = INVALID_BTREE_LEVEL;
            }
        }

        if !skip_middle {
            if opaque.btpo_prev != leftcurrent && leftcurrent != P_NONE {
                bt_recheck_sibling_links(state, opaque.btpo_prev, leftcurrent)?;
            }
            if level.level != opaque.btpo_level {
                return Err(Box::new(
                    PgError::error(format!(
                        "leftmost down link for level points to block in index \"{}\" whose level is not one level down",
                        state.rel.name()
                    ))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                    .with_detail(format!(
                        "Block pointed to={} expected level={} level in pointed to block={}.",
                        current, level.level, opaque.btpo_level
                    )),
                ));
            }
            bt_target_page_check(state)?;
        }

        if current == leftcurrent || current == opaque.btpo_prev {
            return Err(index_corrupted(format!(
                "circular link chain found in block {} of index \"{}\"",
                current,
                state.rel.name()
            )));
        }
        leftcurrent = current;
        current = opaque.btpo_next;

        state.lowkey = None;
        if state.readonly && !P_RIGHTMOST(&opaque) {
            let itemid =
                page_get_item_id_careful(state, state.targetblock, &state.target_page(), P_HIKEY)?;
            let itup = page_item(&state.target_page(), itemid);
            // SAFETY: itup is the live high-key tuple on the alive target copy.
            state.lowkey = Some(unsafe { OwnedTuple::from_itup(itup) });
        }

        state.target = None;
        state.scratch.reset();

        if current == P_NONE {
            break;
        }
    }

    Ok(nextleveldown)
}


fn bt_target_page_check(state: &mut BtreeCheckState<'_>) -> PgResult<()> {
    let mut frame = OrderProcFrame::new();
    let mut l_vis = BtreeLastVisibleEntry {
        blkno: InvalidBlockNumber,
        offset: 0,
        posting_index: -1,
        tid: None,
    };

    let (max, topaque) = {
        let tp = state.target_page();
        (tp.max_offset_number(), page_opaque(&tp))
    };
    let is_leaf = P_ISLEAF(&topaque);
    let is_rightmost = P_RIGHTMOST(&topaque);

    if !is_rightmost {
        let _itemid =
            page_get_item_id_careful(state, state.targetblock, &state.target_page(), P_HIKEY)?;
        if !bt_check_natts(&state.rel, state.heapkeyspace, &state.target_page(), P_HIKEY) {
            let itup = page_item(&state.target_page(), _itemid);
            // SAFETY: high-key item on the alive target copy.
            let natts = unsafe { bt_tuple_get_natts(itup, state.rel.indnatts()) };
            return Err(Box::new(
                PgError::error(format!(
                    "wrong number of high key index tuple attributes in index \"{}\"",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Index block={} natts={} block type={} page lsn={}.",
                    state.targetblock,
                    natts,
                    if is_leaf { "heap" } else { "index" },
                    fmt_lsn(state.targetlsn)
                )),
            ));
        }
    }

    let mut offset = P_FIRSTDATAKEY(&topaque);
    while offset <= max {
        let mut unique_checked = false;

        let itemid =
            page_get_item_id_careful(state, state.targetblock, &state.target_page(), offset)?;
        let itup = page_item(&state.target_page(), itemid);
        // SAFETY: itup is a live tuple on the target copy (alive for the loop); the raw pointer survives &mut state calls.
        let tupsize = unsafe { index_tuple_size(itup) };

        if tupsize != itemid.lp_len() as usize {
            return Err(Box::new(
                PgError::error(format!(
                    "index tuple size does not equal lp_len in index \"{}\"",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Index tid=({},{}) tuple size={} lp_len={} page lsn={}.",
                    state.targetblock,
                    offset,
                    tupsize,
                    itemid.lp_len(),
                    fmt_lsn(state.targetlsn)
                ))
                .with_hint("This could be a torn page problem."),
            ));
        }

        if !bt_check_natts(&state.rel, state.heapkeyspace, &state.target_page(), offset) {
            let tid = unsafe { btree_tuple_points_to_tid(itup) };
            let natts = unsafe { bt_tuple_get_natts(itup, state.rel.indnatts()) };
            return Err(Box::new(
                PgError::error(format!(
                    "wrong number of index tuple attributes in index \"{}\"",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Index tid=({},{}) natts={} points to {} tid={} page lsn={}.",
                    state.targetblock,
                    offset,
                    natts,
                    if is_leaf { "heap" } else { "index" },
                    fmt_tid(&tid),
                    fmt_lsn(state.targetlsn)
                )),
            ));
        }

        if offset_is_negative_infinity(&topaque, offset) {
            if !is_leaf && state.readonly {
                bt_child_highkey_check(state, offset, None, topaque.btpo_level)?;
            }
            offset += 1;
            continue;
        }

        if state.rootdescend && is_leaf {
            // SAFETY: guarded by heapkeyspace (checked in bt_check_every_level); itup is a live non-pivot leaf tuple.
            let found = unsafe { bt_rootdescend(&state.rel, itup)? };
            if !found {
                let tid = unsafe { btree_tuple_points_to_tid(itup) };
                return Err(Box::new(
                    PgError::error(format!(
                        "could not find tuple using search from root page in index \"{}\"",
                        state.rel.name()
                    ))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                    .with_detail(format!(
                        "Index tid=({},{}) points to heap tid={} page lsn={}.",
                        state.targetblock,
                        offset,
                        fmt_tid(&tid),
                        fmt_lsn(state.targetlsn)
                    )),
                ));
            }
        }

        if unsafe { bt_tuple_is_posting(itup) } {
            let mut last = unsafe { bt_tuple_get_heap_tid(itup) }.expect("posting has heap tid");
            let nposting = unsafe { bt_tuple_get_nposting(itup) };
            for i in 1..nposting {
                let current_tid = unsafe { bt_tuple_get_posting_n(itup, i) };
                if ItemPointerCompare(&current_tid, &last) <= 0 {
                    return Err(Box::new(
                        PgError::error(format!(
                            "posting list contains misplaced TID in index \"{}\"",
                            state.rel.name()
                        ))
                        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                        .with_detail(format!(
                            "Index tid=({},{}) posting list offset={} page lsn={}.",
                            state.targetblock,
                            offset,
                            i,
                            fmt_lsn(state.targetlsn)
                        )),
                    ));
                }
                last = current_tid;
            }
        }

        let mut skey = bt_mkscankey_pivotsearch(&state.rel, itup)?;
        let skey_keysz = (state.rel.indnkeyatts() as usize)
            .min(unsafe { bt_tuple_get_natts(itup, state.rel.indnatts()) } as usize);

        let heaptid_none = unsafe { bt_tuple_get_heap_tid(itup) }.is_none();
        let lowersizelimit = skey.heapkeyspace && (is_leaf || heaptid_none);
        let limit = if lowersizelimit {
            BTMaxItemSize
        } else {
            BTMaxItemSizeNoHeapTid
        };
        if tupsize > limit {
            let tid = unsafe { btree_tuple_points_to_tid(itup) };
            return Err(Box::new(
                PgError::error(format!(
                    "index row size {} exceeds maximum for index \"{}\"",
                    tupsize,
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Index tid=({},{}) points to {} tid={} page lsn={}.",
                    state.targetblock,
                    offset,
                    if is_leaf { "heap" } else { "index" },
                    fmt_tid(&tid),
                    fmt_lsn(state.targetlsn)
                )),
            ));
        }

        if state.heapallindexed && is_leaf && !itemid.is_dead() {
            fingerprint_leaf_tuple(
                &mut state.scratch,
                state.filter.as_mut().expect("filter set"),
                &state.rel,
                itup,
            )?;
        }

        let scantid_save = skey.scantid;
        if state.heapkeyspace && unsafe { bt_tuple_is_posting(itup) } {
            skey.scantid = Some(unsafe { bt_tuple_get_max_heap_tid(itup) });
        }
        if !is_rightmost {
            let ok = if is_leaf {
                invariant_leq_offset(state, &mut skey, P_HIKEY, &mut frame)?
            } else {
                invariant_l_offset(state, &mut skey, skey_keysz, P_HIKEY, &mut frame)?
            };
            if !ok {
                let tid = unsafe { btree_tuple_points_to_tid(itup) };
                return Err(Box::new(
                    PgError::error(format!(
                        "high key invariant violated for index \"{}\"",
                        state.rel.name()
                    ))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                    .with_detail(format!(
                        "Index tid=({},{}) points to {} tid={} page lsn={}.",
                        state.targetblock,
                        offset,
                        if is_leaf { "heap" } else { "index" },
                        fmt_tid(&tid),
                        fmt_lsn(state.targetlsn)
                    )),
                ));
            }
        }
        skey.scantid = scantid_save;

        if offset + 1 <= max && !invariant_l_offset(state, &mut skey, skey_keysz, offset + 1, &mut frame)? {
            let tid = unsafe { btree_tuple_points_to_tid(itup) };
            let itemid2 = page_get_item_id_careful(
                state,
                state.targetblock,
                &state.target_page(),
                offset + 1,
            )?;
            let itup2 = page_item(&state.target_page(), itemid2);
            let tid2 = unsafe { btree_tuple_points_to_tid(itup2) };
            return Err(Box::new(
                PgError::error(format!(
                    "item order invariant violated for index \"{}\"",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Lower index tid=({},{}) (points to {} tid={}) higher index tid=({},{}) (points to {} tid={}) page lsn={}.",
                    state.targetblock,
                    offset,
                    if is_leaf { "heap" } else { "index" },
                    fmt_tid(&tid),
                    state.targetblock,
                    offset + 1,
                    if is_leaf { "heap" } else { "index" },
                    fmt_tid(&tid2),
                    fmt_lsn(state.targetlsn)
                )),
            ));
        }

        if state.checkunique
            && state.ii_unique()
            && is_leaf
            && !skey.anynullkeys
            && (unsafe { bt_tuple_is_posting(itup) }
                || l_vis.tid.as_ref().map(ItemPointerIsValid).unwrap_or(false))
        {
            bt_entry_unique_check(state, itup, state.targetblock, offset, &mut l_vis)?;
            unique_checked = true;
        }

        if state.checkunique && state.ii_unique() && is_leaf && offset + 1 <= max {
            let scantid = skey.scantid;
            skey.scantid = None;
            let cmp = bt_compare(&state.rel, &mut skey, &state.target_page(), offset + 1, &mut frame)?;
            if cmp != 0 || skey.anynullkeys {
                l_vis = BtreeLastVisibleEntry {
                    blkno: InvalidBlockNumber,
                    offset: 0,
                    posting_index: -1,
                    tid: None,
                };
            } else if !unique_checked {
                bt_entry_unique_check(state, itup, state.targetblock, offset, &mut l_vis)?;
            }
            skey.scantid = scantid;
        }

        if offset == max {
            let mut rightfirstoffset = 0u16;
            let right = bt_right_page_check_scankey(state, &mut rightfirstoffset)?;
            if let Some((mut rightkey, rightpage)) = right {
                if !invariant_g_offset(state, &mut rightkey, max, &mut frame)? {
                    if !state.readonly {
                        let fresh = palloc_btree_page(state, state.targetblock)?;
                        let ignore = P_IGNORE(&page_opaque(&fresh.page()));
                        state.target = Some(fresh);
                        if ignore {
                            return Ok(());
                        }
                    }
                    return Err(Box::new(
                        PgError::error(format!(
                            "cross page item order invariant violated for index \"{}\"",
                            state.rel.name()
                        ))
                        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                        .with_detail(format!(
                            "Last item on page tid=({},{}) page lsn={}.",
                            state.targetblock,
                            offset,
                            fmt_lsn(state.targetlsn)
                        )),
                    ));
                }

                if state.checkunique && state.ii_unique() && is_leaf && !is_rightmost {
                    let rightblock_number = topaque.btpo_next;
                    rightkey.scantid = None;
                    if bt_compare(&state.rel, &mut rightkey, &state.target_page(), max, &mut frame)? == 0
                        && !rightkey.anynullkeys
                    {
                        if !unique_checked {
                            bt_entry_unique_check(state, itup, state.targetblock, offset, &mut l_vis)?;
                        }
                        let rightpage2 = palloc_btree_page(state, rightblock_number)?;
                        let ropaque = page_opaque(&rightpage2.page());
                        if P_IGNORE(&ropaque) {
                            break;
                        }
                        if !P_ISLEAF(&ropaque) {
                            return Err(Box::new(
                                PgError::error(format!(
                                    "right block of leaf block is non-leaf for index \"{}\"",
                                    state.rel.name()
                                ))
                                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                                .with_detail(format!(
                                    "Block={} page lsn={}.",
                                    state.targetblock,
                                    fmt_lsn(state.targetlsn)
                                )),
                            ));
                        }
                        let ritemid = page_get_item_id_careful(
                            state,
                            rightblock_number,
                            &rightpage2.page(),
                            rightfirstoffset,
                        )?;
                        let ritup = page_item(&rightpage2.page(), ritemid);
                        bt_entry_unique_check(state, ritup, rightblock_number, rightfirstoffset, &mut l_vis)?;
                    }
                }
                drop(rightpage);
            }
        }

        if !is_leaf && state.readonly {
            bt_child_check(state, &mut skey, skey_keysz, offset, &mut frame)?;
        }

        offset += 1;
    }

    if !is_leaf && is_rightmost && state.readonly {
        bt_child_highkey_check(state, 0, None, topaque.btpo_level)?;
    }

    Ok(())
}

fn bt_right_page_check_scankey<'mcx>(
    state: &BtreeCheckState<'mcx>,
    rightfirstoffset: &mut OffsetNumber,
) -> PgResult<Option<(BtScanInsert, PageImage)>> {
    let targetnext_start = {
        let tp = state.target_page();
        let op = page_opaque(&tp);
        if P_RIGHTMOST(&op) {
            return Ok(None);
        }
        op.btpo_next
    };

    let mut targetnext = targetnext_start;
    let rightpage;
    loop {
        let page = palloc_btree_page(state, targetnext)?;
        let op = page_opaque(&page.page());
        if !P_IGNORE(&op) || P_RIGHTMOST(&op) {
            rightpage = page;
            break;
        }
        // Deleted/half-dead sibling: step right (C divergence: DEBUG2 omitted).
        targetnext = op.btpo_next;
    }

    let ropaque = page_opaque(&rightpage.page());
    let nline = rightpage.page().max_offset_number();
    let is_leaf = P_ISLEAF(&ropaque);

    let rightitem_offset = if is_leaf && nline >= P_FIRSTDATAKEY(&ropaque) {
        let off = P_FIRSTDATAKEY(&ropaque);
        *rightfirstoffset = off;
        off
    } else if !is_leaf && nline >= P_FIRSTDATAKEY(&ropaque) + 1 {
        P_FIRSTDATAKEY(&ropaque) + 1
    } else {
        // No first item (C divergence: DEBUG2 omitted).
        return Ok(None);
    };

    let ritemid = page_get_item_id_careful(state, targetnext, &rightpage.page(), rightitem_offset)?;
    let firstitup = page_item(&rightpage.page(), ritemid);
    let skey = bt_mkscankey_pivotsearch(&state.rel, firstitup)?;
    Ok(Some((skey, rightpage)))
}

fn offset_is_negative_infinity(opaque: &BTPageOpaqueData, offset: OffsetNumber) -> bool {
    !P_ISLEAF(opaque) && offset == P_FIRSTDATAKEY(opaque)
}

fn invariant_l_offset(
    state: &BtreeCheckState<'_>,
    key: &mut BtScanInsert,
    key_keysz: usize,
    upperbound: OffsetNumber,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let itemid =
        page_get_item_id_careful(state, state.targetblock, &state.target_page(), upperbound)?;
    if !key.heapkeyspace {
        return invariant_leq_offset(state, key, upperbound, frame);
    }

    let cmp = bt_compare(&state.rel, key, &state.target_page(), upperbound, frame)?;
    if cmp == 0 {
        let ritup = page_item(&state.target_page(), itemid);
        let (is_leaf, firstdatakey) = {
            let op = page_opaque(&state.target_page());
            (P_ISLEAF(&op), P_FIRSTDATAKEY(&op))
        };
        let nonpivot = is_leaf && upperbound >= firstdatakey;
        let uppnkeyatts = unsafe { btree_tuple_get_nkeyatts(ritup, &state.rel) };
        let rheaptid = btree_tuple_get_heap_tid_careful(state, ritup, nonpivot)?;
        if key_keysz == uppnkeyatts as usize {
            return Ok(key.scantid.is_none() && rheaptid.is_some());
        }
        return Ok((key_keysz as i32) < uppnkeyatts);
    }
    Ok(cmp < 0)
}

fn invariant_leq_offset(
    state: &BtreeCheckState<'_>,
    key: &mut BtScanInsert,
    upperbound: OffsetNumber,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let cmp = bt_compare(&state.rel, key, &state.target_page(), upperbound, frame)?;
    Ok(cmp <= 0)
}

fn invariant_g_offset(
    state: &BtreeCheckState<'_>,
    key: &mut BtScanInsert,
    lowerbound: OffsetNumber,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let cmp = bt_compare(&state.rel, key, &state.target_page(), lowerbound, frame)?;
    if !key.heapkeyspace {
        return Ok(cmp >= 0);
    }
    Ok(cmp > 0)
}

fn invariant_l_nontarget_offset(
    state: &BtreeCheckState<'_>,
    key: &mut BtScanInsert,
    key_keysz: usize,
    nontargetblock: BlockNumber,
    nontarget: &PageRef<'_>,
    upperbound: OffsetNumber,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let itemid = page_get_item_id_careful(state, nontargetblock, nontarget, upperbound)?;
    let cmp = bt_compare(&state.rel, key, nontarget, upperbound, frame)?;
    if !key.heapkeyspace {
        return Ok(cmp <= 0);
    }
    if cmp == 0 {
        let child = page_item(nontarget, itemid);
        let (is_leaf, firstdatakey) = {
            let op = page_opaque(nontarget);
            (P_ISLEAF(&op), P_FIRSTDATAKEY(&op))
        };
        let nonpivot = is_leaf && upperbound >= firstdatakey;
        let uppnkeyatts = unsafe { btree_tuple_get_nkeyatts(child, &state.rel) };
        let childheaptid = btree_tuple_get_heap_tid_careful(state, child, nonpivot)?;
        if key_keysz == uppnkeyatts as usize {
            return Ok(key.scantid.is_none() && childheaptid.is_some());
        }
        return Ok((key_keysz as i32) < uppnkeyatts);
    }
    Ok(cmp < 0)
}


fn bt_child_check(
    state: &mut BtreeCheckState<'_>,
    targetkey: &mut BtScanInsert,
    targetkey_keysz: usize,
    downlinkoffnum: OffsetNumber,
    frame: &mut OrderProcFrame,
) -> PgResult<()> {
    let itemid =
        page_get_item_id_careful(state, state.targetblock, &state.target_page(), downlinkoffnum)?;
    let itup = page_item(&state.target_page(), itemid);
    let childblock = unsafe { bt_tuple_get_downlink(itup) };

    let target_level = page_opaque(&state.target_page()).btpo_level;
    let child = palloc_btree_page(state, childblock)?;
    let (copaque, maxoffset) = {
        let cp = child.page();
        (page_opaque(&cp), cp.max_offset_number())
    };

    bt_child_highkey_check(state, downlinkoffnum, Some(&child), target_level)?;

    if P_ISDELETED(&copaque) {
        return Err(Box::new(
            PgError::error(format!(
                "downlink to deleted page found in index \"{}\"",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Parent block={} child block={} parent page lsn={}.",
                state.targetblock,
                childblock,
                fmt_lsn(state.targetlsn)
            )),
        ));
    }

    let mut offset = P_FIRSTDATAKEY(&copaque);
    while offset <= maxoffset {
        if offset_is_negative_infinity(&copaque, offset) {
            offset += 1;
            continue;
        }
        if !invariant_l_nontarget_offset(
            state,
            targetkey,
            targetkey_keysz,
            childblock,
            &child.page(),
            offset,
            frame,
        )? {
            return Err(Box::new(
                PgError::error(format!(
                    "down-link lower bound invariant violated for index \"{}\"",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Parent block={} child index tid=({},{}) parent page lsn={}.",
                    state.targetblock,
                    childblock,
                    offset,
                    fmt_lsn(state.targetlsn)
                )),
            ));
        }
        offset += 1;
    }
    Ok(())
}

fn bt_child_highkey_check(
    state: &mut BtreeCheckState<'_>,
    target_downlinkoffnum: OffsetNumber,
    loaded_child: Option<&PageImage>,
    target_level: u32,
) -> PgResult<()> {
    let mut blkno = state.prevrightlink;
    let mut rightsplit = state.previncompletesplit;
    let mut first = true;

    let downlink = if offset_number_is_valid(target_downlinkoffnum) {
        let itemid = page_get_item_id_careful(
            state,
            state.targetblock,
            &state.target_page(),
            target_downlinkoffnum,
        )?;
        let itup = page_item(&state.target_page(), itemid);
        unsafe { bt_tuple_get_downlink(itup) }
    } else {
        P_NONE
    };

    if blkno == InvalidBlockNumber {
        blkno = downlink;
        rightsplit = false;
    }

    loop {
        if blkno == P_NONE && downlink == P_NONE {
            state.prevrightlink = InvalidBlockNumber;
            state.previncompletesplit = false;
            return Ok(());
        }
        if blkno == P_NONE {
            return Err(index_corrupted(format!(
                "can't traverse from downlink {} to downlink {} of index \"{}\"",
                state.prevrightlink,
                downlink,
                state.rel.name()
            )));
        }

        let owned: Option<PageImage> = if blkno == downlink && loaded_child.is_some() {
            None
        } else {
            Some(palloc_btree_page(state, blkno)?)
        };
        let page = match &owned {
            Some(p) => p.page(),
            None => loaded_child.expect("loaded_child present").page(),
        };
        let opaque = page_opaque(&page);

        if first
            && state.prevrightlink == InvalidBlockNumber
            && !bt_leftmost_ignoring_half_dead(state, blkno, &page)?
        {
            return Err(Box::new(
                PgError::error(format!(
                    "the first child of leftmost target page is not leftmost of its level in index \"{}\"",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Target block={} child block={} target page lsn={}.",
                    state.targetblock,
                    blkno,
                    fmt_lsn(state.targetlsn)
                )),
            ));
        }

        if (!P_ISDELETED(&opaque) || P_HAS_FULLXID(&opaque)) && opaque.btpo_level != target_level - 1
        {
            return Err(Box::new(
                PgError::error(format!(
                    "block found while following rightlinks from child of index \"{}\" has invalid level",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Block pointed to={} expected level={} level in pointed to block={}.",
                    blkno,
                    target_level - 1,
                    opaque.btpo_level
                )),
            ));
        }

        if (!first && blkno == state.prevrightlink) || blkno == opaque.btpo_prev {
            return Err(index_corrupted(format!(
                "circular link chain found in block {} of index \"{}\"",
                blkno,
                state.rel.name()
            )));
        }

        if blkno != downlink && !P_IGNORE(&opaque) {
            bt_downlink_missing_check(state, rightsplit, blkno, &page)?;
        }

        rightsplit = P_INCOMPLETE_SPLIT(&opaque);

        if !rightsplit && !P_RIGHTMOST(&opaque) && !P_ISHALFDEAD(&opaque) {
            let hitemid = page_get_item_id_careful(state, blkno, &page, P_HIKEY)?;
            let highkey = page_item(&page, hitemid);

            let pivotkey_offset = if blkno == downlink {
                target_downlinkoffnum + 1
            } else {
                target_downlinkoffnum
            };

            let topaque = page_opaque(&state.target_page());
            let itup_to_match: ITup;
            if !offset_is_negative_infinity(&topaque, pivotkey_offset) {
                let pivotkey_offset = if pivotkey_offset > state.target_page().max_offset_number() {
                    if P_RIGHTMOST(&topaque) {
                        return Err(Box::new(
                            PgError::error(format!(
                                "child high key is greater than rightmost pivot key on target level in index \"{}\"",
                                state.rel.name()
                            ))
                            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                            .with_detail(format!(
                                "Target block={} child block={} target page lsn={}.",
                                state.targetblock,
                                blkno,
                                fmt_lsn(state.targetlsn)
                            )),
                        ));
                    }
                    P_HIKEY
                } else {
                    pivotkey_offset
                };
                let itemid = page_get_item_id_careful(
                    state,
                    state.targetblock,
                    &state.target_page(),
                    pivotkey_offset,
                )?;
                itup_to_match = page_item(&state.target_page(), itemid);
            } else {
                match state.lowkey.as_ref() {
                    None => {
                        return Err(Box::new(
                            PgError::error(format!(
                                "can't find left sibling high key in index \"{}\"",
                                state.rel.name()
                            ))
                            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                            .with_detail(format!(
                                "Target block={} child block={} target page lsn={}.",
                                state.targetblock,
                                blkno,
                                fmt_lsn(state.targetlsn)
                            )),
                        ));
                    }
                    Some(lk) => itup_to_match = lk.as_itup(),
                }
            }

            // SAFETY: highkey / itup_to_match are live tuples on alive images.
            if !unsafe { bt_pivot_tuple_identical(state.heapkeyspace, highkey, itup_to_match) } {
                return Err(Box::new(
                    PgError::error(format!(
                        "mismatch between parent key and child high key in index \"{}\"",
                        state.rel.name()
                    ))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                    .with_detail(format!(
                        "Target block={} child block={} target page lsn={}.",
                        state.targetblock,
                        blkno,
                        fmt_lsn(state.targetlsn)
                    )),
                ));
            }
        }

        if blkno == downlink {
            state.prevrightlink = opaque.btpo_next;
            state.previncompletesplit = rightsplit;
            return Ok(());
        }

        blkno = opaque.btpo_next;
        first = false;
    }
}

fn bt_downlink_missing_check(
    state: &BtreeCheckState<'_>,
    rightsplit: bool,
    blkno: BlockNumber,
    page: &PageRef<'_>,
) -> PgResult<()> {
    let opaque = page_opaque(page);
    if P_ISROOT(&opaque) {
        return Ok(());
    }
    let pagelsn = page.lsn();

    if rightsplit {
        // C divergence: DEBUG1 "harmless interrupted page split" omitted.
        return Ok(());
    }

    if P_ISLEAF(&opaque) {
        return Err(Box::new(
            PgError::error(format!(
                "leaf index block lacks downlink in index \"{}\"",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!("Block={} page lsn={}.", blkno, fmt_lsn(pagelsn))),
        ));
    }

    let mut level = opaque.btpo_level;
    let itemid = page_get_item_id_careful(state, blkno, page, P_FIRSTDATAKEY(&opaque))?;
    let itup = page_item(page, itemid);
    let mut childblk = unsafe { bt_tuple_get_downlink(itup) };

    let mut child;
    let mut copaque;
    loop {
        child = palloc_btree_page(state, childblk)?;
        copaque = page_opaque(&child.page());
        if P_ISLEAF(&copaque) {
            break;
        }
        if copaque.btpo_level != level - 1 {
            return Err(Box::new(
                PgError::error(format!(
                    "downlink points to block in index \"{}\" whose level is not one level down",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Top parent/under check block={} block pointed to={} expected level={} level in pointed to block={}.",
                    blkno,
                    childblk,
                    level - 1,
                    copaque.btpo_level
                )),
            ));
        }
        level = copaque.btpo_level;
        let itemid = page_get_item_id_careful(state, childblk, &child.page(), P_FIRSTDATAKEY(&copaque))?;
        let itup = page_item(&child.page(), itemid);
        childblk = unsafe { bt_tuple_get_downlink(itup) };
    }

    if P_ISDELETED(&copaque) {
        return Err(Box::new(
            PgError::error(format!(
                "downlink to deleted leaf page found in index \"{}\"",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Top parent/target block={} leaf block={} top parent/under check lsn={}.",
                blkno,
                childblk,
                fmt_lsn(pagelsn)
            )),
        ));
    }

    if P_ISHALFDEAD(&copaque) && !P_RIGHTMOST(&copaque) {
        let itemid = page_get_item_id_careful(state, childblk, &child.page(), P_HIKEY)?;
        let itup = page_item(&child.page(), itemid);
        if unsafe { bt_tuple_get_downlink(itup) } == blkno {
            return Ok(());
        }
    }

    Err(Box::new(
        PgError::error(format!(
            "internal index block lacks downlink in index \"{}\"",
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
        .with_detail(format!(
            "Block={} level={} page lsn={}.",
            blkno,
            opaque.btpo_level,
            fmt_lsn(pagelsn)
        )),
    ))
}

// SAFETY: both ITups are live tuple images.
unsafe fn bt_pivot_tuple_identical(heapkeyspace: bool, itup1: ITup, itup2: ITup) -> bool {
    let s1 = index_tuple_size(itup1);
    if s1 != index_tuple_size(itup2) {
        return false;
    }
    let start = if heapkeyspace { 4usize } else { 6usize };
    from_raw_parts(itup1.add(start), s1 - start) == from_raw_parts(itup2.add(start), s1 - start)
}

fn bt_leftmost_ignoring_half_dead(
    state: &BtreeCheckState<'_>,
    start: BlockNumber,
    start_page: &PageRef<'_>,
) -> PgResult<bool> {
    let mut reached = page_opaque(start_page).btpo_prev;
    let mut reached_from = start;
    let mut all_half_dead = true;

    while reached != P_NONE && all_half_dead {
        let page = palloc_btree_page(state, reached)?;
        let op = page_opaque(&page.page());
        all_half_dead = P_ISHALFDEAD(&op)
            && reached != start
            && reached != reached_from
            && op.btpo_next == reached_from;
        if all_half_dead {
            // C divergence: DEBUG1 "harmless interrupted page deletion" omitted.
            reached_from = reached;
            reached = op.btpo_prev;
        }
    }
    Ok(all_half_dead)
}

fn bt_recheck_sibling_links(
    state: &mut BtreeCheckState<'_>,
    mut btpo_prev_from_target: BlockNumber,
    leftcurrent: BlockNumber,
) -> PgResult<()> {
    if !state.readonly {
        let lbuf = ReadBufferExtended(
            &state.rel,
            ForkNumber::MAIN_FORKNUM,
            leftcurrent,
            ReadBufferMode::Normal,
            state.checkstrategy.clone(),
        )?;
        LockBuffer(lbuf, BUFFER_LOCK_SHARE)?;
        bt_checkpage_ref(&state.rel, &buffer_page_ref(lbuf), leftcurrent)?;
        let lopaque = page_opaque(&buffer_page_ref(lbuf));
        if P_ISDELETED(&lopaque) {
            UnlockReleaseBuffer(lbuf)?;
            return Ok(());
        }

        let newtargetblock = lopaque.btpo_next;
        if newtargetblock != leftcurrent {
            let newtargetbuf = ReadBufferExtended(
                &state.rel,
                ForkNumber::MAIN_FORKNUM,
                newtargetblock,
                ReadBufferMode::Normal,
                state.checkstrategy.clone(),
            )?;
            LockBuffer(newtargetbuf, BUFFER_LOCK_SHARE)?;
            bt_checkpage_ref(&state.rel, &buffer_page_ref(newtargetbuf), newtargetblock)?;
            btpo_prev_from_target = page_opaque(&buffer_page_ref(newtargetbuf)).btpo_prev;
            UnlockReleaseBuffer(newtargetbuf)?;
        } else {
            btpo_prev_from_target = InvalidBlockNumber;
        }
        UnlockReleaseBuffer(lbuf)?;

        if btpo_prev_from_target == leftcurrent {
            // C divergence: DEBUG1 "harmless concurrent page split" omitted.
            return Ok(());
        }
        state.targetblock = newtargetblock;
    }

    Err(Box::new(
        PgError::error(format!(
            "left link/right link pair in index \"{}\" not in agreement",
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
        .with_detail(format!(
            "Block={} left block={} left link from block={}.",
            state.targetblock, leftcurrent, btpo_prev_from_target
        )),
    ))
}


enum NormTuple<'m> {
    Same(ITup),
    Owned(ItupBuf<'m>),
}

impl NormTuple<'_> {
    #[inline]
    fn as_itup(&self) -> ITup {
        match self {
            NormTuple::Same(p) => *p,
            NormTuple::Owned(b) => b.as_ptr(),
        }
    }
}

/// SAFETY-free surface: the ITup is a live leaf tuple on the target copy.
fn fingerprint_leaf_tuple(
    scratch: &mut MemoryContext,
    filter: &mut BloomFilter<'_>,
    rel: &Relation<'_>,
    itup: ITup,
) -> PgResult<()> {
    let smcx = scratch.mcx();
    // SAFETY: itup is a live leaf tuple; smcx allocations live in scratch.
    unsafe {
        if bt_tuple_is_posting(itup) {
            for i in 0..bt_tuple_get_nposting(itup) {
                let plain = bt_posting_plain_tuple(smcx, itup, i)?;
                let norm = bt_normalize_tuple(smcx, rel, plain.as_ptr())?;
                let np = norm.as_itup();
                filter.add_element(from_raw_parts(np, index_tuple_size(np)));
            }
        } else {
            let norm = bt_normalize_tuple(smcx, rel, itup)?;
            let np = norm.as_itup();
            filter.add_element(from_raw_parts(np, index_tuple_size(np)));
        }
    }
    Ok(())
}

/// SAFETY: `itup` is a live posting tuple.
unsafe fn bt_posting_plain_tuple<'m>(mcx: Mcx<'m>, itup: ITup, n: usize) -> PgResult<ItupBuf<'m>> {
    let keysize = bt_tuple_get_posting_offset(itup);
    debug_assert_eq!(keysize, maxalign(keysize));
    let mut buf = ItupBuf::with_size(mcx, keysize)?;
    let dst = buf.as_mut_ptr();
    copy_nonoverlapping(itup, dst, keysize);
    let info = (t_info(dst) & !INDEX_SIZE_MASK & !INDEX_ALT_TID_MASK) | keysize as u16;
    set_t_info(dst, info);
    set_t_tid(dst, bt_tuple_get_posting_n(itup, n));
    Ok(buf)
}

/// SAFETY: `itup` is a live non-pivot, non-posting index tuple.
unsafe fn bt_normalize_tuple<'m>(
    mcx: Mcx<'m>,
    rel: &Relation<'m>,
    itup: ITup,
) -> PgResult<NormTuple<'m>> {
    if (t_info(itup) & INDEX_VAR_MASK) == 0 {
        return Ok(NormTuple::Same(itup));
    }

    let tupdesc = rel.descr();
    let natts = tupdesc.natts as usize;
    let mut normalized = [Datum::null(); IMK];
    let mut isnull = [false; IMK];
    let mut formnewtup = false;

    for i in 0..natts {
        let att = tupdesc.attr(i);
        normalized[i] = index_getattr(itup, (i + 1) as AttrNumber, tupdesc, &mut isnull[i]);
        if att.attbyval || att.attlen != -1 || isnull[i] {
            continue;
        }
        let p = normalized[i].as_usize() as *const u8;
        if varatt_is_1b_e(p) {
            return Err(index_corrupted(format!(
                "external varlena datum in tuple that references heap row ({},{}) in index \"{}\"",
                ItemPointerGetBlockNumberNoCheck(&t_tid(itup)),
                ItemPointerGetOffsetNumberNoCheck(&t_tid(itup)),
                rel.name()
            )));
        } else if !varatt_is_compressed(p)
            && varsize_4b(p) > TOAST_INDEX_TARGET
            && (att.attstorage == TYPSTORAGE_EXTENDED || att.attstorage == TYPSTORAGE_MAIN)
        {
            formnewtup = true;
        } else if varatt_is_compressed(p) {
            formnewtup = true;
            let flat = detoast_attr(mcx, from_raw_parts(p, varsize_any(p)))?;
            normalized[i] = Datum::from_usize(flat.leak().as_ptr() as usize);
        } else if varatt_can_make_short(p) {
            let len = varatt_converted_short_size(p);
            let data = vec_from_elem_in(mcx, 0u8, len).leak();
            let dptr = data.as_mut_ptr();
            set_varsize_short(dptr, len);
            copy_nonoverlapping(p.add(VARHDRSZ), dptr.add(1), len - 1);
            formnewtup = true;
            normalized[i] = Datum::from_usize(dptr as usize);
        }
    }

    if !formnewtup {
        return Ok(NormTuple::Same(itup));
    }

    let mut reformed = index_form_tuple(mcx, tupdesc, &normalized[..natts], &isnull[..natts])?;
    set_t_tid(reformed.as_mut_ptr(), t_tid(itup));
    Ok(NormTuple::Owned(reformed))
}

fn bt_tuple_present_callback(
    scratch: &mut MemoryContext,
    filter: &mut BloomFilter<'_>,
    heaptuplespresent: &mut i64,
    heaprel: &Relation<'_>,
    readonly: bool,
    index: &Relation<'_>,
    tid: &ItemPointerData,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<()> {
    let missing;
    {
        let smcx = scratch.mcx();
        // SAFETY: freshly formed index tuple; smcx allocs live in scratch.
        unsafe {
            let mut formed = index_form_tuple(smcx, index.descr(), values, isnull)?;
            set_t_tid(formed.as_mut_ptr(), *tid);
            let itup = formed.as_ptr();
            let norm = bt_normalize_tuple(smcx, index, itup)?;
            let np = norm.as_itup();
            missing = filter.lacks_element(from_raw_parts(np, index_tuple_size(np)));
        }
    }

    if missing {
        let mut err = PgError::error(format!(
            "heap tuple ({},{}) from table \"{}\" lacks matching index tuple within index \"{}\"",
            ItemPointerGetBlockNumberNoCheck(tid),
            ItemPointerGetOffsetNumberNoCheck(tid),
            heaprel.name(),
            index.name()
        ))
        .with_sqlstate(ERRCODE_DATA_CORRUPTED);
        if !readonly {
            err = err.with_hint(
                "Retrying verification using the function bt_index_parent_check() might provide a more specific error.",
            );
        }
        return Err(Box::new(err));
    }

    *heaptuplespresent += 1;
    scratch.reset();
    Ok(())
}


fn heap_entry_is_visible(state: &BtreeCheckState<'_>, tid: &ItemPointerData) -> PgResult<bool> {
    let mcx = state.mcx;
    let mut slot = ::tableam::table_slot_create(mcx, &state.heaprel)?;
    let visible = ::tableam::table_tuple_fetch_row_version(
        mcx,
        &state.heaprel,
        tid,
        &state.snapshot,
        &mut slot,
    )?;
    ::exectuples::exec_clear_tuple(&mut slot, mcx);
    Ok(visible)
}

fn bt_entry_unique_check(
    state: &BtreeCheckState<'_>,
    itup: ITup,
    targetblock: BlockNumber,
    offset: OffsetNumber,
    l_vis: &mut BtreeLastVisibleEntry,
) -> PgResult<()> {
    let mut has_visible_entry = false;

    if unsafe { bt_tuple_is_posting(itup) } {
        for i in 0..unsafe { bt_tuple_get_nposting(itup) } {
            let tid = unsafe { bt_tuple_get_posting_n(itup, i) };
            if heap_entry_is_visible(state, &tid)? {
                has_visible_entry = true;
                if l_vis.tid.as_ref().map(ItemPointerIsValid).unwrap_or(false) {
                    bt_report_duplicate(state, l_vis, &tid, targetblock, offset, i as i32)?;
                }
                if l_vis.blkno != targetblock
                    && l_vis.tid.as_ref().map(ItemPointerIsValid).unwrap_or(false)
                {
                    return Ok(());
                }
                l_vis.blkno = targetblock;
                l_vis.offset = offset;
                l_vis.posting_index = i as i32;
                l_vis.tid = Some(tid);
            }
        }
    } else {
        let tid = unsafe { bt_tuple_get_heap_tid(itup) }.expect("non-pivot has heap tid");
        if heap_entry_is_visible(state, &tid)? {
            has_visible_entry = true;
            if l_vis.tid.as_ref().map(ItemPointerIsValid).unwrap_or(false) {
                bt_report_duplicate(state, l_vis, &tid, targetblock, offset, -1)?;
            }
            l_vis.blkno = targetblock;
            l_vis.offset = offset;
            l_vis.tid = Some(tid);
            l_vis.posting_index = -1;
        }
    }

    if !has_visible_entry && l_vis.blkno != InvalidBlockNumber && l_vis.blkno != targetblock {
        // C divergence: DEBUG1 "index uniqueness can not be checked" omitted.
    }
    Ok(())
}

fn bt_report_duplicate(
    state: &BtreeCheckState<'_>,
    l_vis: &BtreeLastVisibleEntry,
    nexttid: &ItemPointerData,
    nblock: BlockNumber,
    noffset: OffsetNumber,
    nposting: i32,
) -> PgResult<()> {
    let lvis_tid = l_vis.tid.expect("l_vis.tid valid when reporting");
    let htid = format!(
        "tid=({},{})",
        ItemPointerGetBlockNumberNoCheck(&lvis_tid),
        ItemPointerGetOffsetNumberNoCheck(&lvis_tid)
    );
    let nhtid = format!(
        "tid=({},{})",
        ItemPointerGetBlockNumberNoCheck(nexttid),
        ItemPointerGetOffsetNumberNoCheck(nexttid)
    );
    let itid = format!("tid=({},{})", l_vis.blkno, l_vis.offset);
    let nitid = if nblock != l_vis.blkno || noffset != l_vis.offset {
        format!(" tid=({},{})", nblock, noffset)
    } else {
        String::new()
    };
    let pposting = if l_vis.posting_index >= 0 {
        format!(" posting {}", l_vis.posting_index)
    } else {
        String::new()
    };
    let pnposting = if nposting >= 0 {
        format!(" posting {}", nposting)
    } else {
        String::new()
    };

    Err(Box::new(
        PgError::error(format!(
            "index uniqueness is violated for index \"{}\"",
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
        .with_detail(format!(
            "Index {}{} and{}{} (point to heap {} and {}) page lsn={}.",
            itid,
            pposting,
            nitid,
            pnposting,
            htid,
            nhtid,
            fmt_lsn(state.targetlsn)
        )),
    ))
}


fn palloc_btree_page(state: &BtreeCheckState<'_>, blocknum: BlockNumber) -> PgResult<PageImage> {
    let mut img = PageImage::zeroed();

    let buffer = ReadBufferExtended(
        &state.rel,
        ForkNumber::MAIN_FORKNUM,
        blocknum,
        ReadBufferMode::Normal,
        state.checkstrategy.clone(),
    )?;
    LockBuffer(buffer, BUFFER_LOCK_SHARE)?;
    let src = buffer_page_ref(buffer);
    bt_checkpage_ref(&state.rel, &src, blocknum)?;
    // SAFETY: src is a live BLCKSZ page; img holds BLCKSZ bytes.
    unsafe { copy_nonoverlapping(src.as_ptr(), img.ptr_mut(), BLCKSZ) };
    UnlockReleaseBuffer(buffer)?;

    let opaque = page_opaque(&img.page());

    if P_ISMETA(&opaque) && blocknum != BTREE_METAPAGE {
        return Err(index_corrupted(format!(
            "invalid meta page found at block {} in index \"{}\"",
            blocknum,
            state.rel.name()
        )));
    }

    if blocknum == BTREE_METAPAGE {
        let metad = page_meta(&img.page());
        if !P_ISMETA(&opaque) || metad.btm_magic != BTREE_MAGIC {
            return Err(index_corrupted(format!(
                "index \"{}\" meta page is corrupt",
                state.rel.name()
            )));
        }
        if metad.btm_version < BTREE_MIN_VERSION || metad.btm_version > BTREE_VERSION {
            return Err(index_corrupted(format!(
                "version mismatch in index \"{}\": file version {}, current version {}, minimum supported version {}",
                state.rel.name(),
                metad.btm_version,
                BTREE_VERSION,
                BTREE_MIN_VERSION
            )));
        }
        return Ok(img);
    }

    if !P_ISDELETED(&opaque) || P_HAS_FULLXID(&opaque) {
        if P_ISLEAF(&opaque) && opaque.btpo_level != 0 {
            return Err(index_corrupted(format!(
                "invalid leaf page level {} for block {} in index \"{}\"",
                opaque.btpo_level,
                blocknum,
                state.rel.name()
            )));
        }
        if !P_ISLEAF(&opaque) && opaque.btpo_level == 0 {
            return Err(index_corrupted(format!(
                "invalid internal page level 0 for block {} in index \"{}\"",
                blocknum,
                state.rel.name()
            )));
        }
    }

    let maxoffset = img.page().max_offset_number();
    if maxoffset as usize > MAX_INDEX_TUPLES_PER_PAGE {
        return Err(index_corrupted(format!(
            "Number of items on block {} of index \"{}\" exceeds MaxIndexTuplesPerPage ({})",
            blocknum,
            state.rel.name(),
            MAX_INDEX_TUPLES_PER_PAGE
        )));
    }
    if !P_ISLEAF(&opaque) && !P_ISDELETED(&opaque) && maxoffset < P_FIRSTDATAKEY(&opaque) {
        return Err(index_corrupted(format!(
            "internal block {} in index \"{}\" lacks high key and/or at least one downlink",
            blocknum,
            state.rel.name()
        )));
    }
    if P_ISLEAF(&opaque) && !P_ISDELETED(&opaque) && !P_RIGHTMOST(&opaque) && maxoffset < P_HIKEY {
        return Err(index_corrupted(format!(
            "non-rightmost leaf block {} in index \"{}\" lacks high key item",
            blocknum,
            state.rel.name()
        )));
    }
    if !P_ISLEAF(&opaque) && P_ISHALFDEAD(&opaque) {
        return Err(Box::new(
            PgError::error(format!(
                "internal page block {} in index \"{}\" is half-dead",
                blocknum,
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_hint(
                "This can be caused by an interrupted VACUUM in version 9.3 or older, before upgrade. Please REINDEX it.",
            ),
        ));
    }
    if !P_ISLEAF(&opaque) && P_HAS_GARBAGE(&opaque) {
        return Err(index_corrupted(format!(
            "internal page block {} in index \"{}\" has garbage items",
            blocknum,
            state.rel.name()
        )));
    }
    if P_HAS_FULLXID(&opaque) && !P_ISDELETED(&opaque) {
        return Err(index_corrupted(format!(
            "full transaction id page flag appears in non-deleted block {} in index \"{}\"",
            blocknum,
            state.rel.name()
        )));
    }
    if P_ISDELETED(&opaque) && P_ISHALFDEAD(&opaque) {
        return Err(index_corrupted(format!(
            "deleted page block {} in index \"{}\" is half-dead",
            blocknum,
            state.rel.name()
        )));
    }

    Ok(img)
}

fn page_get_item_id_careful(
    state: &BtreeCheckState<'_>,
    block: BlockNumber,
    page: &PageRef<'_>,
    offset: OffsetNumber,
) -> PgResult<ItemIdData> {
    let itemid = page.item_id(offset);
    let lp_off = itemid.lp_off() as usize;
    let lp_len = itemid.lp_len() as usize;

    if lp_off + lp_len > BLCKSZ - OPAQUE_MAXALIGN {
        return Err(Box::new(
            PgError::error(format!(
                "line pointer points past end of tuple space in index \"{}\"",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Index tid=({},{}) lp_off={}, lp_len={} lp_flags={}.",
                block,
                offset,
                lp_off,
                lp_len,
                itemid.lp_flags()
            )),
        ));
    }

    if itemid.is_redirected() || !itemid.is_used() || lp_len == 0 {
        return Err(Box::new(
            PgError::error(format!(
                "invalid line pointer storage in index \"{}\"",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Index tid=({},{}) lp_off={}, lp_len={} lp_flags={}.",
                block,
                offset,
                lp_off,
                lp_len,
                itemid.lp_flags()
            )),
        ));
    }

    Ok(itemid)
}

fn btree_tuple_get_heap_tid_careful(
    state: &BtreeCheckState<'_>,
    itup: ITup,
    nonpivot: bool,
) -> PgResult<Option<ItemPointerData>> {
    let is_pivot = unsafe { bt_tuple_is_pivot(itup) };
    if is_pivot && nonpivot {
        return Err(index_corrupted(format!(
            "block {} or its right sibling block or child block in index \"{}\" has unexpected pivot tuple",
            state.targetblock,
            state.rel.name()
        )));
    }
    if !is_pivot && !nonpivot {
        return Err(index_corrupted(format!(
            "block {} or its right sibling block or child block in index \"{}\" has unexpected non-pivot tuple",
            state.targetblock,
            state.rel.name()
        )));
    }

    let htid = unsafe { bt_tuple_get_heap_tid(itup) };
    if htid.as_ref().map(|t| !ItemPointerIsValid(t)).unwrap_or(true) && nonpivot {
        return Err(index_corrupted(format!(
            "block {} or its right sibling block or child block in index \"{}\" contains non-pivot tuple that lacks a heap TID",
            state.targetblock,
            state.rel.name()
        )));
    }
    Ok(htid)
}

// SAFETY-free surface: itup is a live index tuple.
fn bt_mkscankey_pivotsearch(rel: &Relation<'_>, itup: ITup) -> PgResult<BtScanInsert> {
    let mut skey = bt_mkscankey(rel, Some(itup))?;
    skey.backward = true;
    Ok(skey)
}

/// SAFETY: `itup` is a live data-item tuple.
unsafe fn btree_tuple_points_to_tid(itup: ITup) -> ItemPointerData {
    if !bt_tuple_is_pivot(itup) {
        bt_tuple_get_heap_tid(itup).unwrap_or_else(|| t_tid(itup))
    } else {
        t_tid(itup)
    }
}

/// `BTreeTupleGetNKeyAtts(itup, rel)`. SAFETY: `itup` is a live tuple.
unsafe fn btree_tuple_get_nkeyatts(itup: ITup, rel: &Relation<'_>) -> i32 {
    rel.indnkeyatts().min(bt_tuple_get_natts(itup, rel.indnatts()))
}

#[inline]
fn offset_number_is_valid(offset: OffsetNumber) -> bool {
    offset != 0 && offset <= ::types_storage::bufpage::MaxOffsetNumber
}

const INDEX_VAR_MASK: u16 = 0x4000;
const MAX_INDEX_TUPLES_PER_PAGE: usize =
    (BLCKSZ - SizeOfPageHeaderData) / (16 + core::mem::size_of::<ItemIdData>());

// SAFETY: `p` is a live varlena with at least one readable header byte.
#[inline]
unsafe fn varatt_is_compressed(p: *const u8) -> bool {
    if cfg!(target_endian = "little") {
        (*p & 0x03) == 0x02
    } else {
        (*p & 0xC0) == 0x40
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_format_matches_c() {
        assert_eq!(fmt_lsn(0), "0/0");
        assert_eq!(fmt_lsn(0x0000_00AB_0000_00CD), "AB/CD");
        assert_eq!(fmt_lsn(0xDEAD_BEEF_0000_0001), "DEADBEEF/1");
        assert_eq!(fmt_lsn(0xFFFF_FFFF_FFFF_FFFF), "FFFFFFFF/FFFFFFFF");
    }

    #[test]
    fn tid_format_matches_c() {
        assert_eq!(fmt_tid(&ItemPointerData::new(0, 0)), "(0,0)");
        assert_eq!(fmt_tid(&ItemPointerData::new(42, 7)), "(42,7)");
        assert_eq!(fmt_tid(&ItemPointerData::new(1000000, 1)), "(1000000,1)");
    }

    #[test]
    fn negative_infinity_offset_constants() {
        assert_eq!(OPAQUE_MAXALIGN, 16);
        assert_eq!(BLCKSZ - OPAQUE_MAXALIGN, 8176);
        assert_eq!(MAX_INDEX_TUPLES_PER_PAGE, 408);
    }

    #[test]
    fn offset_validity() {
        assert!(!offset_number_is_valid(0));
        assert!(offset_number_is_valid(1));
        assert!(offset_number_is_valid(::types_storage::bufpage::MaxOffsetNumber));
    }
}
