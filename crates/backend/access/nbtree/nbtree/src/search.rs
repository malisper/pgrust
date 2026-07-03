//! nbtsearch.c: tree descent, per-page binary search, the 3-way insertion-key
//! compare, and the forward/backward scan runtime. Serial scans only —
//! parallel coordination, row compares, and array keys are phase 2.

use core::mem::MaybeUninit;

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::mcx::vec_with_capacity_in;
use ::types_core::{
    BlockNumber, Buffer, InvalidBuffer, Oid, OffsetNumber, BLCKSZ, INDEX_MAX_KEYS,
};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::FmgrInfo;
use ::types_nbtree::{
    BTScanOpaqueData, BTScanPosData, BTScanPosInvalidate, BTScanPosIsPinned, BTScanPosIsValid,
    BTScanPosItem, MaxTIDsPerBTreePage, BTORDER_PROC, BT_READ, P_FIRSTDATAKEY, P_HIKEY,
    P_IGNORE, P_ISDELETED, P_ISLEAF, P_LEFTMOST, P_NONE, P_RIGHTMOST,
};
use ::types_rel::Relation;
use ::types_scan::scankey::{
    ScanKeyData, StrategyNumber, BTEqualStrategyNumber, BTGreaterEqualStrategyNumber,
    BTGreaterStrategyNumber, BTLessEqualStrategyNumber, BTLessStrategyNumber, InvalidStrategy,
    SK_BT_DESC, SK_BT_MAXVAL, SK_BT_MINVAL, SK_BT_NEXT, SK_BT_NULLS_FIRST, SK_BT_PRIOR,
    SK_BT_REQBKWD, SK_BT_REQFWD, SK_ISNULL, SK_ROW_HEADER, SK_SEARCHNOTNULL,
};
use ::types_scan::sdir::{ScanDirection, ScanDirectionIsBackward, ScanDirectionIsForward};
use ::types_snapshot::SnapshotData;
use ::types_storage::bufpage::PageRef;
use ::types_tuple::itemptr::{ItemPointerCompare, ItemPointerData};
use ::types_tuple::TupleDescData;

use crate::fcframe::OrderProcFrame;
use crate::itup::{
    bt_tuple_get_downlink, bt_tuple_get_heap_tid, bt_tuple_get_max_heap_tid, bt_tuple_get_natts,
    bt_tuple_get_nposting, bt_tuple_get_posting_n, bt_tuple_is_pivot, bt_tuple_is_posting,
    index_getattr, index_tuple_size, ITup, INDEX_SIZE_MASK,
};
use crate::page::{
    bt_getbuf, bt_getroot, bt_metaversion, bt_relandgetbuf, bt_relbuf, page_item, page_opaque,
};
use crate::utils::{bt_checkkeys, bt_killitems, bt_set_startikey};
use crate::{check_for_interrupts, unported_phase2};

const INVERT_COMPARE_RESULT: fn(i32) -> i32 = |r| if r < 0 { 1 } else { -r };
const MAXALIGN: fn(usize) -> usize = |l| (l + 7) & !7;

/// # Safety
/// `buf` must be pinned for as long as the returned view is used (BTScanPos
/// ownership contract), locked per C's access rules.
#[inline]
pub(crate) unsafe fn buf_page<'p>(buf: Buffer) -> PageRef<'p> {
    PageRef::from_raw(bufmgr::buffer_get_page::call(buf))
}

/// BTScanPosUnpinIfPinned.
pub(crate) fn pos_unpin_if_pinned(pos: &mut BTScanPosData) -> PgResult<()> {
    if BTScanPosIsPinned(pos) {
        bufmgr::release_buffer::call(pos.buf)?;
        pos.buf = InvalidBuffer;
    }
    Ok(())
}

// IndexScanDescData split-borrowed once per AM entry point.
pub(crate) struct ScanCtx<'a, 'mcx> {
    pub rel: &'a Relation<'mcx>,
    pub so: &'a mut BTScanOpaqueData<'mcx>,
    pub snapshot: Option<&'a SnapshotData<'mcx>>,
    pub ignore_killed_tuples: bool,
    pub input_keys: &'a mut [ScanKeyData],
    pub xs_heaptid: &'a mut ItemPointerData,
    pub xs_itup: &'a mut Option<core::ptr::NonNull<u8>>,
    pub xs_pgstat_index_scans: &'a mut u64,
    pub xs_nsearches: &'a mut u64,
    pub frame: OrderProcFrame,
}

// C BTScanInsertData, stack layout; only the live key prefix is initialized
// (palloc parity).
pub struct BtScanInsert {
    pub heapkeyspace: bool,
    pub allequalimage: bool,
    pub anynullkeys: bool,
    pub nextkey: bool,
    pub backward: bool,
    pub scantid: Option<ItemPointerData>,
    keysz: usize,
    scankeys: [MaybeUninit<ScanKeyData>; INDEX_MAX_KEYS as usize],
}

impl BtScanInsert {
    pub fn new() -> Self {
        BtScanInsert {
            heapkeyspace: true,
            allequalimage: false,
            anynullkeys: false,
            nextkey: false,
            backward: false,
            scantid: None,
            keysz: 0,
            scankeys: [const { MaybeUninit::uninit() }; INDEX_MAX_KEYS as usize],
        }
    }

    #[inline]
    pub fn push(&mut self, key: ScanKeyData) {
        self.scankeys[self.keysz].write(key);
        self.keysz += 1;
    }

    #[inline]
    pub fn keys_mut(&mut self) -> &mut [ScanKeyData] {
        // SAFETY: [0, keysz) written by push; MaybeUninit<T> is layout-T.
        unsafe {
            core::slice::from_raw_parts_mut(self.scankeys.as_mut_ptr().cast(), self.keysz)
        }
    }
}

/// _bt_search, BT_READ arm; `None` for an empty index. C divergence: the
/// parent-page BTStack is insert-only (read callers free it unread) — the
/// insert lane adds the stacked variant.
/// Batched-descent lever (docs/graviton.md §1.5), noted not forced: overlap
/// the child-page fetch with the tail of the parent's binary search; needs
/// bufmgr prefetch support and a measured win.
pub fn bt_search(
    rel: &Relation<'_>,
    key: &mut BtScanInsert,
    frame: &mut OrderProcFrame,
) -> PgResult<Option<BufferPin>> {
    let Some(mut pin) = bt_getroot(rel, BT_READ)? else {
        return Ok(None);
    };

    loop {
        pin = bt_moveright(rel, key, pin, frame)?;

        let child = {
            let page = pin.page();
            let opaque = page_opaque(&page);
            if P_ISLEAF(&opaque) {
                break;
            }
            let offnum = bt_binsrch(rel, key, &page, frame)?;
            // SAFETY: binsrch returns an offset within this page's line array.
            let itup = page_item(&page, unsafe { page.item_id_unchecked(offnum) });
            // SAFETY: pinned+locked page item.
            unsafe {
                debug_assert!(bt_tuple_is_pivot(itup) || !key.heapkeyspace);
                bt_tuple_get_downlink(itup)
            }
        };

        pin = bt_relandgetbuf(rel, Some(pin), child, BT_READ)?;
    }

    Ok(Some(pin))
}

/// _bt_moveright, read arm (forupdate/finish-incomplete-split is insert-lane).
fn bt_moveright(
    rel: &Relation<'_>,
    key: &mut BtScanInsert,
    mut pin: BufferPin,
    frame: &mut OrderProcFrame,
) -> PgResult<BufferPin> {
    let cmpval: i32 = if key.nextkey { 0 } else { 1 };

    loop {
        let next = {
            let page = pin.page();
            let opaque = page_opaque(&page);
            if P_RIGHTMOST(&opaque) {
                if P_IGNORE(&opaque) {
                    return Err(fell_off_the_end(rel));
                }
                return Ok(pin);
            }
            let move_right = if P_IGNORE(&opaque) {
                true
            } else {
                bt_compare(rel, key, &page, P_HIKEY, frame)? >= cmpval
            };
            if !move_right {
                if P_IGNORE(&opaque) {
                    return Err(fell_off_the_end(rel));
                }
                return Ok(pin);
            }
            opaque.btpo_next
        };
        pin = bt_relandgetbuf(rel, Some(pin), next, BT_READ)?;
    }
}

#[cold]
#[inline(never)]
fn fell_off_the_end(rel: &Relation<'_>) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "fell off the end of index \"{}\"",
        rel.name()
    )))
}

/// _bt_binsrch: first offset >= (or >, when nextkey) the scan key.
pub fn bt_binsrch(
    rel: &Relation<'_>,
    key: &mut BtScanInsert,
    page: &PageRef<'_>,
    frame: &mut OrderProcFrame,
) -> PgResult<OffsetNumber> {
    let opaque = page_opaque(page);

    debug_assert!(!key.nextkey || key.scantid.is_none());
    debug_assert!(!P_ISLEAF(&opaque) || key.scantid.is_none());

    let mut low = P_FIRSTDATAKEY(&opaque);
    let mut high = page.max_offset_number();

    if high < low {
        return Ok(low);
    }

    high += 1; // loop invariant: everything at/after high is > (>=) scan key

    let cmpval: i32 = if key.nextkey { 0 } else { 1 };

    while high > low {
        let mid = low + (high - low) / 2;
        let result = bt_compare(rel, key, page, mid, frame)?;
        if result >= cmpval {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    if P_ISLEAF(&opaque) {
        if key.backward {
            return Ok(low - 1);
        }
        return Ok(low);
    }

    debug_assert!(low > P_FIRSTDATAKEY(&opaque));
    Ok(low - 1)
}

/// _bt_compare: insertion scankey vs the tuple at `offnum`; <0 / 0 / >0.
pub fn bt_compare(
    rel: &Relation<'_>,
    key: &mut BtScanInsert,
    page: &PageRef<'_>,
    offnum: OffsetNumber,
    frame: &mut OrderProcFrame,
) -> PgResult<i32> {
    let tupdesc: &TupleDescData<'_> = &rel.rd_att;
    let opaque = page_opaque(page);

    debug_assert!(key.keysz as i32 <= rel.indnkeyatts());
    debug_assert!(key.heapkeyspace || key.scantid.is_none());

    if !P_ISLEAF(&opaque) && offnum == P_FIRSTDATAKEY(&opaque) {
        return Ok(1);
    }

    // SAFETY: caller passes an offnum within this page's line array.
    let itup = page_item(page, unsafe { page.item_id_unchecked(offnum) });
    // SAFETY: pinned+locked page item, offnum from page bounds (caller).
    unsafe {
        let ntupatts = bt_tuple_get_natts(itup, rel.indnatts());

        let ncmpkey = ntupatts.min(key.keysz as i32);
        debug_assert!(key.heapkeyspace || ncmpkey == key.keysz as i32);
        debug_assert!(!bt_tuple_is_posting(itup) || key.allequalimage);

        for scankey in &mut key.keys_mut()[..ncmpkey as usize] {
            let mut is_null = false;
            let datum = index_getattr(itup, scankey.sk_attno, tupdesc, &mut is_null);

            let result: i32 = if scankey.sk_flags & SK_ISNULL != 0 {
                if is_null {
                    0 // NULL "=" NULL
                } else if scankey.sk_flags & SK_BT_NULLS_FIRST != 0 {
                    -1 // NULL "<" NOT_NULL
                } else {
                    1 // NULL ">" NOT_NULL
                }
            } else if is_null {
                if scankey.sk_flags & SK_BT_NULLS_FIRST != 0 {
                    1 // NOT_NULL ">" NULL
                } else {
                    -1 // NOT_NULL "<" NULL
                }
            } else {
                let arg = scankey.sk_argument;
                let mut r = frame.cmp(scankey, datum, arg)?;
                if scankey.sk_flags & SK_BT_DESC == 0 {
                    r = INVERT_COMPARE_RESULT(r);
                }
                r
            };

            if result != 0 {
                return Ok(result);
            }
        }

        if key.keysz as i32 > ntupatts {
            return Ok(1);
        }

        let heap_tid = bt_tuple_get_heap_tid(itup);
        let Some(scantid) = key.scantid.as_ref() else {
            if !key.backward
                && key.keysz as i32 == ntupatts
                && heap_tid.is_none()
                && key.heapkeyspace
            {
                return Ok(1);
            }
            return Ok(0);
        };

        debug_assert!(key.keysz as i32 == rel.indnkeyatts());
        let Some(heap_tid) = heap_tid else {
            return Ok(1); // truncated heap TID is minus infinity
        };

        debug_assert!(ntupatts >= rel.indnkeyatts());
        let result = ItemPointerCompare(scantid, &heap_tid);
        if result <= 0 || !bt_tuple_is_posting(itup) {
            return Ok(result);
        }
        if ItemPointerCompare(scantid, &bt_tuple_get_max_heap_tid(itup)) > 0 {
            return Ok(1);
        }
        Ok(0) // scantid within the posting list's TID range
    }
}

/// _bt_drop_lock_and_maybe_pin.
fn drop_lock_and_maybe_pin(rel: &Relation<'_>, so: &mut BTScanOpaqueData<'_>) -> PgResult<()> {
    if !so.dropPin {
        return bufmgr::lock_buffer::call(so.currPos.buf, bufmgr::BUFFER_LOCK_UNLOCK);
    }
    so.currPos.lsn = bufmgr::buffer_get_lsn_atomic::call(so.currPos.buf);
    let pin = BufferPin::adopt(so.currPos.buf).expect("pinned currPos");
    so.currPos.buf = InvalidBuffer;
    bt_relbuf(rel, pin)
}

/// index_getprocinfo + fmgr_info_copy over the rd_supportinfo rule-5 cache.
pub fn order_procinfo(rel: &Relation<'_>, attno: usize) -> PgResult<FmgrInfo> {
    let mut cache = rel.rd_supportinfo.borrow_mut();
    if cache.is_empty() {
        cache.resize_with(rel.indnkeyatts() as usize, || None);
    }
    let slot = &mut cache[attno - 1];
    if slot.is_none() {
        let opcintype = rel.rd_opcintype[attno - 1];
        let proc = lsyscache::get_opfamily_proc(
            rel.rd_opfamily[attno - 1],
            opcintype,
            opcintype,
            BTORDER_PROC as i16,
        )?;
        if proc == 0 {
            return Err(missing_support_function(rel, opcintype, opcintype, attno));
        }
        *slot = Some(fmgr_core::fmgr_info(proc)?);
    }
    Ok(slot.as_ref().expect("filled above").clone())
}

#[cold]
#[inline(never)]
fn missing_support_function(
    rel: &Relation<'_>,
    lefttype: Oid,
    righttype: Oid,
    attno: usize,
) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "missing support function {BTORDER_PROC}({lefttype},{righttype}) for attribute {attno} of index \"{}\"",
        rel.name()
    )))
}

enum StartKey {
    Data(usize),
    NotNull(ScanKeyData),
}

/// _bt_first.
pub(crate) fn bt_first(ctx: &mut ScanCtx<'_, '_>, dir: ScanDirection) -> PgResult<bool> {
    let rel = ctx.rel;

    debug_assert!(!BTScanPosIsValid(&ctx.so.currPos));

    crate::preprocess::bt_preprocess_keys(rel, ctx.so, ctx.input_keys)?;

    if !ctx.so.qual_ok {
        debug_assert!(!ctx.so.needPrimScan);
        return Ok(false);
    }
    debug_assert!(ctx.so.numArrayKeys == 0, "array lane is phase 2");

    if rel.pgstat_enabled.get() {
        *ctx.xs_pgstat_index_scans += 1;
    }
    *ctx.xs_nsearches += 1;

    let mut start_keys: [Option<StartKey>; INDEX_MAX_KEYS as usize] =
        [const { None }; INDEX_MAX_KEYS as usize];
    let mut keysz: usize = 0;
    let mut strat_total: StrategyNumber = BTEqualStrategyNumber;

    if ctx.so.numberOfKeys > 0 {
        let keys = &ctx.so.keyData[..ctx.so.numberOfKeys as usize];
        let mut curattr = 1;
        let mut bkey: Option<usize> = None;
        let mut implies_nn: Option<usize> = None;

        let mut i = 0usize;
        loop {
            if i >= keys.len() || keys[i].sk_attno != curattr {
                if let Some(bk) = bkey {
                    if keys[bk].sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL) != 0 {
                        unported_phase2("skip-array MINVAL/MAXVAL boundary keys");
                    }
                }

                if bkey.is_none() {
                    if let Some(inn) = implies_nn {
                        let inn_flags = keys[inn].sk_flags;
                        let usable = if inn_flags & SK_BT_NULLS_FIRST != 0 {
                            ScanDirectionIsForward(dir)
                        } else {
                            ScanDirectionIsBackward(dir)
                        };
                        if usable {
                            let mut nn = ScanKeyData::empty();
                            nn.sk_flags = SK_SEARCHNOTNULL
                                | SK_ISNULL
                                | (inn_flags & (SK_BT_DESC | SK_BT_NULLS_FIRST));
                            nn.sk_attno = curattr;
                            nn.sk_strategy = if inn_flags & SK_BT_NULLS_FIRST != 0 {
                                BTGreaterStrategyNumber
                            } else {
                                BTLessStrategyNumber
                            };
                            start_keys[keysz] = Some(StartKey::NotNull(nn));
                            keysz += 1;
                            strat_total = start_keys[keysz - 1]
                                .as_ref()
                                .map(|k| match k {
                                    StartKey::NotNull(nn) => nn.sk_strategy,
                                    StartKey::Data(_) => unreachable!(),
                                })
                                .expect("just written");
                            break; // NOT NULL keys use >/< strategy: done
                        }
                    }
                    break;
                }

                let bk = bkey.expect("checked above");
                start_keys[keysz] = Some(StartKey::Data(bk));
                keysz += 1;

                strat_total = keys[bk].sk_strategy;
                if strat_total == BTGreaterStrategyNumber
                    || strat_total == BTLessStrategyNumber
                {
                    break;
                }
                if keys[bk].sk_flags & (SK_BT_NEXT | SK_BT_PRIOR) != 0 {
                    unported_phase2("skip-array NEXT/PRIOR sentinel keys");
                }

                if i >= keys.len()
                    || keys[i].sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD) == 0
                {
                    break;
                }

                debug_assert!(keys[i].sk_attno == curattr + 1);
                curattr = keys[i].sk_attno;
                bkey = None;
                implies_nn = None;
            }

            if bkey.is_none() {
                let cur = &keys[i];
                match cur.sk_strategy {
                    BTLessStrategyNumber | BTLessEqualStrategyNumber => {
                        if ScanDirectionIsBackward(dir) {
                            bkey = Some(i);
                        } else if implies_nn.is_none() {
                            implies_nn = Some(i);
                        }
                    }
                    BTEqualStrategyNumber => bkey = Some(i),
                    BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => {
                        if ScanDirectionIsForward(dir) {
                            bkey = Some(i);
                        } else if implies_nn.is_none() {
                            implies_nn = Some(i);
                        }
                    }
                    _ => {}
                }
            }
            i += 1;
        }
    }

    if keysz == 0 {
        return bt_endpoint(ctx, dir);
    }

    debug_assert!(keysz <= INDEX_MAX_KEYS as usize);
    let mut inskey = BtScanInsert::new();

    for (i, slot) in start_keys[..keysz].iter().enumerate() {
        let bkey: &ScanKeyData = match slot.as_ref().expect("filled above") {
            StartKey::Data(idx) => &ctx.so.keyData[*idx],
            StartKey::NotNull(nn) => nn,
        };
        debug_assert!(bkey.sk_attno as usize == i + 1);

        if bkey.sk_flags & SK_ROW_HEADER != 0 {
            unported_phase2("row-comparison boundary keys (_bt_first)");
        }

        let sk_func = if bkey.sk_subtype == rel.rd_opcintype[i] || bkey.sk_subtype == 0 {
            order_procinfo(rel, i + 1)?
        } else {
            let cmp_proc = lsyscache::get_opfamily_proc(
                rel.rd_opfamily[i],
                rel.rd_opcintype[i],
                bkey.sk_subtype,
                BTORDER_PROC as i16,
            )?;
            if cmp_proc == 0 {
                return Err(missing_support_function(
                    rel,
                    rel.rd_opcintype[i],
                    bkey.sk_subtype,
                    bkey.sk_attno as usize,
                ));
            }
            fmgr_core::fmgr_info(cmp_proc)?
        };
        inskey.push(ScanKeyData {
            sk_flags: bkey.sk_flags,
            sk_attno: bkey.sk_attno,
            sk_strategy: InvalidStrategy,
            sk_subtype: bkey.sk_subtype,
            sk_collation: bkey.sk_collation,
            sk_func,
            sk_argument: bkey.sk_argument,
        });
    }

    let (heapkeyspace, allequalimage) = bt_metaversion(rel)?;
    inskey.heapkeyspace = heapkeyspace;
    inskey.allequalimage = allequalimage;
    inskey.anynullkeys = false; // unused
    inskey.scantid = None;
    match strat_total {
        BTLessStrategyNumber => {
            inskey.nextkey = false;
            inskey.backward = true;
        }
        BTLessEqualStrategyNumber => {
            inskey.nextkey = true;
            inskey.backward = true;
        }
        BTEqualStrategyNumber => {
            if ScanDirectionIsBackward(dir) {
                inskey.nextkey = true;
                inskey.backward = true;
            } else {
                inskey.nextkey = false;
                inskey.backward = false;
            }
        }
        BTGreaterEqualStrategyNumber => {
            inskey.nextkey = false;
            inskey.backward = false;
        }
        BTGreaterStrategyNumber => {
            inskey.nextkey = true;
            inskey.backward = false;
        }
        other => panic!("unrecognized strat_total: {other}"),
    }

    debug_assert!(ScanDirectionIsBackward(dir) == inskey.backward);
    let mut leaf = bt_search(rel, &mut inskey, &mut ctx.frame)?;

    if leaf.is_none() {
        debug_assert!(!ctx.so.needPrimScan);
        if xact::IsolationIsSerializable() {
            let snap = ctx.snapshot.expect("serializable scan has a snapshot");
            predicate_seams::predicate_lock_relation::call(rel, snap)?;
            leaf = bt_search(rel, &mut inskey, &mut ctx.frame)?;
        }
        if leaf.is_none() {
            return Ok(false);
        }
    }
    let leaf = leaf.expect("checked above");

    let offnum = {
        let page = leaf.page();
        bt_binsrch(rel, &mut inskey, &page, &mut ctx.frame)?
    };
    ctx.so.currPos.buf = leaf.into_buffer();

    if !bt_readfirstpage(ctx, offnum, dir)? {
        return Ok(false);
    }

    bt_returnitem(ctx);
    Ok(true)
}

/// _bt_next.
pub(crate) fn bt_next(ctx: &mut ScanCtx<'_, '_>, dir: ScanDirection) -> PgResult<bool> {
    debug_assert!(BTScanPosIsValid(&ctx.so.currPos));

    if ScanDirectionIsForward(dir) {
        ctx.so.currPos.itemIndex += 1;
        if ctx.so.currPos.itemIndex > ctx.so.currPos.lastItem {
            if !bt_steppage(ctx, dir)? {
                return Ok(false);
            }
        }
    } else {
        ctx.so.currPos.itemIndex -= 1;
        if ctx.so.currPos.itemIndex < ctx.so.currPos.firstItem {
            if !bt_steppage(ctx, dir)? {
                return Ok(false);
            }
        }
    }

    bt_returnitem(ctx);
    Ok(true)
}

pub(crate) struct BtReadPageState<'p> {
    pub minoff: OffsetNumber,
    pub maxoff: OffsetNumber,
    pub page: PageRef<'p>,
    pub firstpage: bool,
    pub forcenonrequired: bool,
    pub startikey: i32,
    pub continuescan: bool,
    // C's finaltup/offnum/skip/rechecks fields drive the array lane (phase 2).
}

/// _bt_readpage: load all matching items from the currPos page.
fn bt_readpage(
    ctx: &mut ScanCtx<'_, '_>,
    dir: ScanDirection,
    mut offnum: OffsetNumber,
    firstpage: bool,
) -> PgResult<bool> {
    let rel = ctx.rel;
    let so = &mut *ctx.so;

    // SAFETY: caller holds pin + read lock on currPos.buf for this whole call.
    let page = unsafe { buf_page(so.currPos.buf) };
    let opaque = page_opaque(&page);
    so.currPos.currPage = bufmgr::buffer_get_block_number::call(so.currPos.buf);
    so.currPos.prevPage = opaque.btpo_prev;
    so.currPos.nextPage = opaque.btpo_next;
    so.currPos.dir = dir;
    so.currPos.nextTupleOffset = 0;

    debug_assert!(if ScanDirectionIsForward(dir) {
        so.currPos.moreRight
    } else {
        so.currPos.moreLeft
    });
    debug_assert!(!P_IGNORE(&opaque));
    debug_assert!(BTScanPosIsPinned(&so.currPos));
    debug_assert!(!so.needPrimScan);

    if let Some(snap) = ctx.snapshot {
        predicate_seams::predicate_lock_page::call(rel, so.currPos.currPage, snap)?;
    }

    let indnatts = rel.indnatts();
    debug_assert!(so.numArrayKeys == 0, "array lane is phase 2");
    let minoff = P_FIRSTDATAKEY(&opaque);
    let maxoff = page.max_offset_number();

    let mut pstate = BtReadPageState {
        minoff,
        maxoff,
        page,
        firstpage,
        forcenonrequired: false,
        startikey: 0,
        continuescan: true,
    };

    let mut item_index: i32;

    if ScanDirectionIsForward(dir) {
        if !pstate.firstpage && minoff < maxoff {
            // SAFETY: page pinned+locked for this call.
            unsafe { bt_set_startikey(rel, so, &mut pstate, &mut ctx.frame)? };
        }

        item_index = 0;
        offnum = offnum.max(minoff);

        while offnum <= maxoff {
            let iid = page.item_id(offnum);

            if ctx.ignore_killed_tuples && iid.is_dead() {
                offnum += 1;
                continue;
            }

            let itup = page_item(&page, iid);
            // SAFETY: pinned+locked page items throughout.
            unsafe {
                debug_assert!(!bt_tuple_is_pivot(itup));

                let passes_quals =
                    bt_checkkeys(rel, so, &mut pstate, false, itup, indnatts, &mut ctx.frame)?;

                if passes_quals {
                    if !bt_tuple_is_posting(itup) {
                        bt_saveitem(so, item_index, offnum, itup);
                        item_index += 1;
                    } else {
                        let tuple_offset =
                            bt_setuppostingitems(so, item_index, offnum, itup);
                        item_index += 1;
                        for i in 1..bt_tuple_get_nposting(itup) {
                            bt_savepostingitem(
                                so,
                                item_index,
                                offnum,
                                bt_tuple_get_posting_n(itup, i),
                                tuple_offset,
                            );
                            item_index += 1;
                        }
                    }
                }
            }
            if !pstate.continuescan {
                break;
            }
            offnum += 1;
        }

        if pstate.continuescan && !so.scanBehind && !P_RIGHTMOST(&opaque) {
            let itup = page_item(&page, page.item_id(P_HIKEY));
            pstate.startikey = 0; // _bt_set_startikey ignores P_HIKEY
            // SAFETY: pinned+locked page item.
            unsafe {
                let truncatt = bt_tuple_get_natts(itup, indnatts);
                bt_checkkeys(rel, so, &mut pstate, false, itup, truncatt, &mut ctx.frame)?;
            }
        }

        if !pstate.continuescan {
            so.currPos.moreRight = false;
        }

        debug_assert!(item_index <= MaxTIDsPerBTreePage as i32);
        so.currPos.firstItem = 0;
        so.currPos.lastItem = item_index - 1;
        so.currPos.itemIndex = 0;
    } else {
        if !pstate.firstpage && minoff < maxoff {
            // SAFETY: page pinned+locked for this call.
            unsafe { bt_set_startikey(rel, so, &mut pstate, &mut ctx.frame)? };
        }

        item_index = MaxTIDsPerBTreePage as i32;
        offnum = offnum.min(maxoff);

        while offnum >= minoff {
            let iid = page.item_id(offnum);
            let mut tuple_alive = true;

            if ctx.ignore_killed_tuples && iid.is_dead() {
                if offnum > minoff {
                    offnum -= 1;
                    continue;
                }
                tuple_alive = false;
            }

            let itup = page_item(&page, iid);
            // SAFETY: pinned+locked page items throughout.
            unsafe {
                debug_assert!(!bt_tuple_is_pivot(itup));

                let passes_quals =
                    bt_checkkeys(rel, so, &mut pstate, false, itup, indnatts, &mut ctx.frame)?;

                if passes_quals && tuple_alive {
                    if !bt_tuple_is_posting(itup) {
                        item_index -= 1;
                        bt_saveitem(so, item_index, offnum, itup);
                    } else {
                        item_index -= 1;
                        let tuple_offset =
                            bt_setuppostingitems(so, item_index, offnum, itup);
                        for i in 1..bt_tuple_get_nposting(itup) {
                            item_index -= 1;
                            bt_savepostingitem(
                                so,
                                item_index,
                                offnum,
                                bt_tuple_get_posting_n(itup, i),
                                tuple_offset,
                            );
                        }
                    }
                }
            }
            if !pstate.continuescan {
                break;
            }
            if offnum == minoff {
                break;
            }
            offnum -= 1;
        }

        if !pstate.continuescan {
            so.currPos.moreLeft = false;
        }

        debug_assert!(item_index >= 0);
        so.currPos.firstItem = item_index;
        so.currPos.lastItem = MaxTIDsPerBTreePage as i32 - 1;
        so.currPos.itemIndex = MaxTIDsPerBTreePage as i32 - 1;
    }

    debug_assert!(!pstate.forcenonrequired);
    Ok(so.currPos.firstItem <= so.currPos.lastItem)
}

/// _bt_saveitem.
///
/// # Safety
/// `itup` is a live non-pivot, non-posting tuple on the locked currPos page.
unsafe fn bt_saveitem(
    so: &mut BTScanOpaqueData<'_>,
    item_index: i32,
    offnum: OffsetNumber,
    itup: ITup,
) {
    debug_assert!(!bt_tuple_is_pivot(itup) && !bt_tuple_is_posting(itup));

    let mut item = BTScanPosItem {
        heapTid: crate::itup::t_tid(itup),
        indexOffset: offnum,
        tupleOffset: 0,
    };
    if let Some(curr_tuples) = so.currTuples.as_mut() {
        let itupsz = index_tuple_size(itup);
        item.tupleOffset = so.currPos.nextTupleOffset as u16;
        // SAFETY: nextTupleOffset stays <= BLCKSZ (page-sourced tuples).
        core::ptr::copy_nonoverlapping(
            itup,
            curr_tuples.as_mut_ptr().add(so.currPos.nextTupleOffset as usize),
            itupsz,
        );
        so.currPos.nextTupleOffset += MAXALIGN(itupsz) as i32;
    }
    so.currPos.set_item(item_index as usize, item);
}

/// _bt_setuppostingitems.
///
/// # Safety
/// `itup` is a live posting tuple on the locked currPos page.
unsafe fn bt_setuppostingitems(
    so: &mut BTScanOpaqueData<'_>,
    item_index: i32,
    offnum: OffsetNumber,
    itup: ITup,
) -> u16 {
    debug_assert!(bt_tuple_is_posting(itup));

    let mut item = BTScanPosItem {
        heapTid: bt_tuple_get_posting_n(itup, 0),
        indexOffset: offnum,
        tupleOffset: 0,
    };
    let mut tuple_offset = 0u16;
    if let Some(curr_tuples) = so.currTuples.as_mut() {
        let itupsz = MAXALIGN(crate::itup::bt_tuple_get_posting_offset(itup));
        item.tupleOffset = so.currPos.nextTupleOffset as u16;
        tuple_offset = item.tupleOffset;
        let base = curr_tuples.as_mut_ptr().add(so.currPos.nextTupleOffset as usize);
        // SAFETY: nextTupleOffset stays <= BLCKSZ.
        core::ptr::copy_nonoverlapping(itup, base, itupsz);
        let info_p = base.add(6).cast::<u16>();
        info_p.write((info_p.read() & !INDEX_SIZE_MASK) | itupsz as u16);
        so.currPos.nextTupleOffset += itupsz as i32;
    }
    so.currPos.set_item(item_index as usize, item);
    tuple_offset
}

/// _bt_savepostingitem.
fn bt_savepostingitem(
    so: &mut BTScanOpaqueData<'_>,
    item_index: i32,
    offnum: OffsetNumber,
    heap_tid: ItemPointerData,
    tuple_offset: u16,
) {
    so.currPos.set_item(
        item_index as usize,
        BTScanPosItem {
            heapTid: heap_tid,
            indexOffset: offnum,
            tupleOffset: if so.currTuples.is_some() { tuple_offset } else { 0 },
        },
    );
}

/// _bt_returnitem: publish items[itemIndex] as the amgettuple result.
fn bt_returnitem(ctx: &mut ScanCtx<'_, '_>) {
    let so = &ctx.so;
    debug_assert!(BTScanPosIsValid(&so.currPos));
    debug_assert!(so.currPos.itemIndex >= so.currPos.firstItem);
    debug_assert!(so.currPos.itemIndex <= so.currPos.lastItem);

    // SAFETY: itemIndex within [firstItem, lastItem] (asserted): written slot.
    let item = unsafe { so.currPos.item(so.currPos.itemIndex as usize) };
    *ctx.xs_heaptid = item.heapTid;
    if let Some(curr_tuples) = so.currTuples.as_ref() {
        *ctx.xs_itup =
            core::ptr::NonNull::new(curr_tuples.as_ptr().wrapping_add(item.tupleOffset as usize)
                as *mut u8);
    }
}

/// _bt_steppage.
fn bt_steppage(ctx: &mut ScanCtx<'_, '_>, dir: ScanDirection) -> PgResult<bool> {
    let so = &mut *ctx.so;
    debug_assert!(BTScanPosIsValid(&so.currPos));

    if so.numKilled > 0 {
        bt_killitems(ctx.rel, so)?;
    }

    if so.markItemIndex >= 0 {
        if BTScanPosIsPinned(&so.currPos) {
            bufmgr::incr_buffer_ref_count::call(so.currPos.buf);
        }
        copy_scanpos(so);
        so.markPos.itemIndex = so.markItemIndex;
        so.markItemIndex = -1;

        if so.needPrimScan {
            unported_phase2("mark/restore across primitive index scans");
        }
    }

    pos_unpin_if_pinned(&mut so.currPos)?;

    let blkno = if ScanDirectionIsForward(dir) {
        so.currPos.nextPage
    } else {
        so.currPos.prevPage
    };
    let lastcurrblkno = so.currPos.currPage;

    if so.currPos.dir != dir {
        so.needPrimScan = false;
    }

    bt_readnextpage(ctx, blkno, lastcurrblkno, dir)
}

// C's offsetof-bounded markPos memcpy: header fields + live items prefix.
fn copy_scanpos(so: &mut BTScanOpaqueData<'_>) {
    let n = (so.currPos.lastItem + 1).max(0) as usize;
    so.markPos.buf = so.currPos.buf;
    so.markPos.currPage = so.currPos.currPage;
    so.markPos.prevPage = so.currPos.prevPage;
    so.markPos.nextPage = so.currPos.nextPage;
    so.markPos.lsn = so.currPos.lsn;
    so.markPos.dir = so.currPos.dir;
    so.markPos.nextTupleOffset = so.currPos.nextTupleOffset;
    so.markPos.moreLeft = so.currPos.moreLeft;
    so.markPos.moreRight = so.currPos.moreRight;
    so.markPos.firstItem = so.currPos.firstItem;
    so.markPos.lastItem = so.currPos.lastItem;
    so.markPos.itemIndex = so.currPos.itemIndex;
    so.markPos.items[..n].copy_from_slice(&so.currPos.items[..n]);
    if let (Some(mark), Some(curr)) = (so.markTuples.as_mut(), so.currTuples.as_ref()) {
        let len = so.currPos.nextTupleOffset as usize;
        // SAFETY: both buffers hold BLCKSZ capacity; len <= BLCKSZ.
        unsafe {
            core::ptr::copy_nonoverlapping(curr.as_ptr(), mark.as_mut_ptr(), len);
        }
    }
}

// btrestrpos's markPos -> currPos restore (the mirror of copy_scanpos).
pub(crate) fn restore_scanpos(so: &mut BTScanOpaqueData<'_>) {
    let n = (so.markPos.lastItem + 1).max(0) as usize;
    so.currPos.buf = so.markPos.buf;
    so.currPos.currPage = so.markPos.currPage;
    so.currPos.prevPage = so.markPos.prevPage;
    so.currPos.nextPage = so.markPos.nextPage;
    so.currPos.lsn = so.markPos.lsn;
    so.currPos.dir = so.markPos.dir;
    so.currPos.nextTupleOffset = so.markPos.nextTupleOffset;
    so.currPos.moreLeft = so.markPos.moreLeft;
    so.currPos.moreRight = so.markPos.moreRight;
    so.currPos.firstItem = so.markPos.firstItem;
    so.currPos.lastItem = so.markPos.lastItem;
    so.currPos.itemIndex = so.markPos.itemIndex;
    let (curr_items, mark_items) = {
        let BTScanOpaqueData { currPos, markPos, .. } = so;
        (&mut currPos.items, &markPos.items)
    };
    curr_items[..n].copy_from_slice(&mark_items[..n]);
    if let (Some(curr), Some(mark)) = (so.currTuples.as_mut(), so.markTuples.as_ref()) {
        let len = so.markPos.nextTupleOffset as usize;
        // SAFETY: both buffers hold BLCKSZ capacity; len <= BLCKSZ.
        unsafe {
            core::ptr::copy_nonoverlapping(mark.as_ptr(), curr.as_mut_ptr(), len);
        }
    }
}

/// _bt_readfirstpage: currPos.buf is pinned and locked on entry.
fn bt_readfirstpage(
    ctx: &mut ScanCtx<'_, '_>,
    offnum: OffsetNumber,
    dir: ScanDirection,
) -> PgResult<bool> {
    ctx.so.numKilled = 0; // just paranoia
    ctx.so.markItemIndex = -1; // ditto

    if ctx.so.needPrimScan {
        unported_phase2("primitive rescans (_bt_readfirstpage)");
    } else if ScanDirectionIsForward(dir) {
        ctx.so.currPos.moreLeft = false;
        ctx.so.currPos.moreRight = true;
    } else {
        ctx.so.currPos.moreLeft = true;
        ctx.so.currPos.moreRight = false;
    }

    if bt_readpage(ctx, dir, offnum, true)? {
        debug_assert!(BTScanPosIsPinned(&ctx.so.currPos));
        drop_lock_and_maybe_pin(ctx.rel, ctx.so)?;
        return Ok(true);
    }

    bufmgr::lock_buffer::call(ctx.so.currPos.buf, bufmgr::BUFFER_LOCK_UNLOCK)?;

    bt_steppage(ctx, dir)
}

/// _bt_readnextpage (serial arm: `seized` parallel handoff is phase 2).
fn bt_readnextpage(
    ctx: &mut ScanCtx<'_, '_>,
    mut blkno: BlockNumber,
    mut lastcurrblkno: BlockNumber,
    dir: ScanDirection,
) -> PgResult<bool> {
    let rel = ctx.rel;

    debug_assert!(ctx.so.currPos.currPage == lastcurrblkno);
    debug_assert!(!BTScanPosIsPinned(&ctx.so.currPos));

    if ScanDirectionIsForward(dir) {
        ctx.so.currPos.moreLeft = true;
    } else {
        ctx.so.currPos.moreRight = true;
    }

    loop {
        if blkno == P_NONE
            || (if ScanDirectionIsForward(dir) {
                !ctx.so.currPos.moreRight
            } else {
                !ctx.so.currPos.moreLeft
            })
        {
            debug_assert!(ctx.so.currPos.currPage == lastcurrblkno);
            BTScanPosInvalidate(&mut ctx.so.currPos);
            return Ok(false);
        }

        debug_assert!(!ctx.so.needPrimScan);

        if ScanDirectionIsForward(dir) {
            check_for_interrupts();
            ctx.so.currPos.buf = bt_getbuf(rel, blkno, BT_READ)?.into_buffer();
        } else {
            match bt_lock_and_validate_left(rel, &mut blkno, lastcurrblkno)? {
                Some(pin) => ctx.so.currPos.buf = pin.into_buffer(),
                None => {
                    BTScanPosInvalidate(&mut ctx.so.currPos);
                    return Ok(false);
                }
            }
        }

        // SAFETY: just pinned+locked above; held until relbuf below.
        let (ignore, opq_next, opq_prev, maxoff) = {
            let page = unsafe { buf_page(ctx.so.currPos.buf) };
            let opaque = page_opaque(&page);
            (
                P_IGNORE(&opaque),
                opaque.btpo_next,
                opaque.btpo_prev,
                page.max_offset_number(),
            )
        };
        lastcurrblkno = blkno;
        if !ignore {
            if ScanDirectionIsForward(dir) {
                let start = {
                    let page = unsafe { buf_page(ctx.so.currPos.buf) };
                    P_FIRSTDATAKEY(&page_opaque(&page))
                };
                if bt_readpage(ctx, dir, start, false)? {
                    break;
                }
                blkno = ctx.so.currPos.nextPage;
            } else {
                if bt_readpage(ctx, dir, maxoff, false)? {
                    break;
                }
                blkno = ctx.so.currPos.prevPage;
            }
        } else {
            blkno = if ScanDirectionIsForward(dir) {
                opq_next
            } else {
                opq_prev
            };
        }

        let pin = BufferPin::adopt(ctx.so.currPos.buf).expect("pinned above");
        ctx.so.currPos.buf = InvalidBuffer;
        bt_relbuf(rel, pin)?;
    }

    debug_assert!(ctx.so.currPos.currPage == blkno);
    debug_assert!(BTScanPosIsPinned(&ctx.so.currPos));
    drop_lock_and_maybe_pin(rel, ctx.so)?;

    Ok(true)
}

/// _bt_lock_and_validate_left: backwards scans' split/deletion-safe left move.
fn bt_lock_and_validate_left(
    rel: &Relation<'_>,
    blkno: &mut BlockNumber,
    mut lastcurrblkno: BlockNumber,
) -> PgResult<Option<BufferPin>> {
    let mut origblkno = *blkno; // detects circular links

    loop {
        check_for_interrupts();
        let mut pin = bt_getbuf(rel, *blkno, BT_READ)?;

        let mut tries = 0;
        loop {
            let (deleted, next, rightmost) = {
                let page = pin.page();
                let opaque = page_opaque(&page);
                (
                    P_ISDELETED(&opaque),
                    opaque.btpo_next,
                    P_RIGHTMOST(&opaque),
                )
            };
            if !deleted && next == lastcurrblkno {
                return Ok(Some(pin));
            }
            tries += 1;
            if rightmost || tries > 4 {
                break;
            }
            *blkno = next;
            pin = bt_relandgetbuf(rel, Some(pin), *blkno, BT_READ)?;
        }

        pin = bt_relandgetbuf(rel, Some(pin), lastcurrblkno, BT_READ)?;
        let deleted = P_ISDELETED(&page_opaque(&pin.page()));
        if deleted {
            loop {
                let (rightmost, next) = {
                    let page = pin.page();
                    let opaque = page_opaque(&page);
                    (P_RIGHTMOST(&opaque), opaque.btpo_next)
                };
                if rightmost {
                    return Err(fell_off_the_end(rel));
                }
                lastcurrblkno = next;
                pin = bt_relandgetbuf(rel, Some(pin), lastcurrblkno, BT_READ)?;
                if !P_ISDELETED(&page_opaque(&pin.page())) {
                    break;
                }
            }
        } else {
            if page_opaque(&pin.page()).btpo_prev == origblkno {
                return Err(Box::new(PgError::error(format!(
                    "could not find left sibling of block {lastcurrblkno} in index \"{}\"",
                    rel.name()
                ))));
            }
        }

        let (leftmost, prev) = {
            let page = pin.page();
            let opaque = page_opaque(&page);
            (P_LEFTMOST(&opaque), opaque.btpo_prev)
        };
        if leftmost {
            bt_relbuf(rel, pin)?;
            return Ok(None);
        }
        *blkno = prev;
        origblkno = prev;
        bt_relbuf(rel, pin)?;
    }
}

/// _bt_get_endpoint, leaf arm: first/last leaf page. Non-leaf levels are only
/// reached from the insert lane (via _bt_gettrueroot) — phase 2.
fn bt_get_endpoint(rel: &Relation<'_>, level: u32, rightmost: bool) -> PgResult<Option<BufferPin>> {
    if level != 0 {
        unported_phase2("_bt_get_endpoint above the leaf level (_bt_gettrueroot)");
    }
    let Some(mut pin) = bt_getroot(rel, BT_READ)? else {
        return Ok(None);
    };

    loop {
        let step = {
            let page = pin.page();
            let opaque = page_opaque(&page);
            if P_IGNORE(&opaque) || (rightmost && !P_RIGHTMOST(&opaque)) {
                if opaque.btpo_next == P_NONE {
                    return Err(fell_off_the_end(rel));
                }
                Some(opaque.btpo_next)
            } else if opaque.btpo_level == level {
                None
            } else if opaque.btpo_level < level {
                return Err(Box::new(
                    PgError::error(format!(
                        "btree level {level} not found in index \"{}\"",
                        rel.name()
                    ))
                    .with_sqlstate(::types_error::ERRCODE_INDEX_CORRUPTED),
                ));
            } else {
                let offnum = if rightmost {
                    page.max_offset_number()
                } else {
                    P_FIRSTDATAKEY(&opaque)
                };
                let itup = page_item(&page, page.item_id(offnum));
                // SAFETY: pinned+locked page item.
                Some(unsafe { bt_tuple_get_downlink(itup) })
            }
        };
        match step {
            Some(blkno) => pin = bt_relandgetbuf(rel, Some(pin), blkno, BT_READ)?,
            None => return Ok(Some(pin)),
        }
    }
}

/// _bt_endpoint: position at the first/last page for a full-index scan.
fn bt_endpoint(ctx: &mut ScanCtx<'_, '_>, dir: ScanDirection) -> PgResult<bool> {
    let rel = ctx.rel;
    debug_assert!(!BTScanPosIsValid(&ctx.so.currPos));
    debug_assert!(!ctx.so.needPrimScan);

    let Some(pin) = bt_get_endpoint(rel, 0, ScanDirectionIsBackward(dir))? else {
        if let Some(snap) = ctx.snapshot {
            predicate_seams::predicate_lock_relation::call(rel, snap)?;
        }
        return Ok(false);
    };

    let start = {
        let page = pin.page();
        let opaque = page_opaque(&page);
        debug_assert!(P_ISLEAF(&opaque));
        if ScanDirectionIsForward(dir) {
            P_FIRSTDATAKEY(&opaque)
        } else {
            debug_assert!(P_RIGHTMOST(&opaque));
            page.max_offset_number()
        }
    };
    ctx.so.currPos.buf = pin.into_buffer();

    if !bt_readfirstpage(ctx, start, dir)? {
        return Ok(false);
    }

    bt_returnitem(ctx);
    Ok(true)
}

/// btgettuple's killed-item bookkeeping + _bt_next dispatch (nbtree.c body).
pub(crate) fn bt_gettuple_continue(
    ctx: &mut ScanCtx<'_, '_>,
    dir: ScanDirection,
    kill_prior_tuple: bool,
) -> PgResult<bool> {
    if kill_prior_tuple {
        if ctx.so.killedItems.capacity() == 0 {
            let mcx = *ctx.so.keyData.allocator();
            let v: ::mcx::PgVec<'_, i32> = vec_with_capacity_in(mcx, MaxTIDsPerBTreePage)?;
            // 'mcx unification: killedItems shares keyData's scan context.
            ctx.so.killedItems = v;
        }
        if (ctx.so.numKilled as usize) < MaxTIDsPerBTreePage {
            ctx.so.killedItems.push(ctx.so.currPos.itemIndex);
            ctx.so.numKilled += 1;
        }
    }

    bt_next(ctx, dir)
}

const _: () = assert!(BLCKSZ == 8192);
