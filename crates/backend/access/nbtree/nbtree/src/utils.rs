//! nbtutils.c, READ side: per-tuple qual checking (_bt_checkkeys /
//! _bt_check_compare), SAOP + skip array-key advancement and primitive-scan
//! scheduling, the startikey page-level precheck, and _bt_killitems.
//! Row compares are phase 2 (preprocessing rejects them).

use ::bufmgr_seams as bufmgr;
use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::{AttrNumber, OffsetNumber, XLogRecPtr};
use ::types_error::PgResult;
use ::types_fmgr::FmgrInfo;
use ::types_nbtree::{
    BTArrayKeyInfo, BTScanOpaqueData, BTScanPosIsPinned, BTScanPosIsValid, BTPageOpaqueData,
    BTP_HAS_GARBAGE, BT_READ, MaxTIDsPerBTreePage, P_FIRSTDATAKEY,
};
use ::types_rel::Relation;
use ::types_scan::scankey::{
    BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
    BTLessEqualStrategyNumber, BTLessStrategyNumber, InvalidStrategy, ScanKeyData,
    SK_BT_INDOPTION_SHIFT, SK_BT_MAXVAL, SK_BT_MINVAL, SK_BT_NEXT, SK_BT_PRIOR, SK_BT_REQBKWD,
    SK_BT_REQFWD, SK_BT_SKIP, SK_ISNULL, SK_ROW_END, SK_ROW_HEADER, SK_ROW_MEMBER,
    SK_SEARCHARRAY, SK_SEARCHNULL, SK_BT_DESC, SK_BT_NULLS_FIRST,
};
use ::types_scan::sdir::{
    BackwardScanDirection, ForwardScanDirection, ScanDirection, ScanDirectionIsBackward,
    ScanDirectionIsForward,
};
use ::types_storage::bufpage::{ItemIdData, PageRef, SizeOfPageHeaderData};
use ::types_tuple::itemptr::{ItemPointerCompare, ItemPointerData, ItemPointerEquals};
use ::types_tuple::varatt::varsize_any;
use ::types_tuple::TupleDescData;

use crate::fcframe::OrderProcFrame;
use crate::itup::{
    bt_tuple_get_heap_tid, bt_tuple_get_natts, bt_tuple_get_nposting, bt_tuple_get_posting_n,
    bt_tuple_is_pivot, bt_tuple_is_posting, index_getattr, ITup,
};
use crate::page::{bt_getbuf, bt_relbuf, page_item, page_opaque, page_special_off};
use crate::search::{BtReadPageState, BtScanInsert};

const INVERT_COMPARE_RESULT: fn(i32) -> i32 = |r| if r < 0 { 1 } else { -r };
const LOOK_AHEAD_REQUIRED_RECHECKS: i32 = 3;
const LOOK_AHEAD_DEFAULT_DISTANCE: i32 = 5;
const NSKIPADVANCES_THRESHOLD: i32 = 3;

#[inline]
fn flip_dir(dir: ScanDirection) -> ScanDirection {
    match dir {
        ForwardScanDirection => BackwardScanDirection,
        BackwardScanDirection => ForwardScanDirection,
        other => other,
    }
}

/// _bt_compare_array_skey: tuple attribute value vs an array element / scan
/// key argument; <0 / 0 / >0.
fn bt_compare_array_skey(
    frame: &mut OrderProcFrame,
    orderproc: &mut FmgrInfo,
    tupdatum: Datum,
    tupnull: bool,
    arrdatum: Datum,
    cur: &ScanKeyData,
) -> PgResult<i32> {
    debug_assert!(cur.sk_strategy == BTEqualStrategyNumber);
    debug_assert!(cur.sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL) == 0);

    if tupnull {
        Ok(if cur.sk_flags & SK_ISNULL != 0 {
            0
        } else if cur.sk_flags & SK_BT_NULLS_FIRST != 0 {
            -1
        } else {
            1
        })
    } else if cur.sk_flags & SK_ISNULL != 0 {
        Ok(if cur.sk_flags & SK_BT_NULLS_FIRST != 0 { 1 } else { -1 })
    } else {
        let mut result = frame.cmp_proc(orderproc, cur.sk_collation, tupdatum, arrdatum)?;
        if cur.sk_flags & SK_BT_DESC != 0 {
            result = INVERT_COMPARE_RESULT(result);
        }
        Ok(result)
    }
}

/// _bt_binsrch_array_skey: first array element >= tupdatum; `cur_elem_trig` = required-array lockstep.
#[allow(clippy::too_many_arguments)]
pub(crate) fn bt_binsrch_array_skey(
    frame: &mut OrderProcFrame,
    orderproc: &mut FmgrInfo,
    cur_elem_trig: bool,
    dir: ScanDirection,
    tupdatum: Datum,
    tupnull: bool,
    array: &BTArrayKeyInfo<'_>,
    cur: &ScanKeyData,
    set_elem_result: &mut i32,
) -> PgResult<i32> {
    let mut low_elem: i32 = 0;
    let mut mid_elem: i32 = -1;
    let mut high_elem: i32 = array.num_elems - 1;
    let mut result: i32 = 0;

    debug_assert!(cur.sk_flags & SK_SEARCHARRAY != 0);
    debug_assert!(cur.sk_flags & (SK_BT_SKIP | SK_ISNULL) == 0);
    debug_assert!(cur.sk_strategy == BTEqualStrategyNumber);

    if cur_elem_trig {
        debug_assert!(cur.sk_flags & SK_BT_REQFWD != 0);

        if ScanDirectionIsForward(dir) {
            low_elem = array.cur_elem + 1;
            if high_elem >= low_elem {
                let arrdatum = array.elem_values[low_elem as usize];
                result = bt_compare_array_skey(frame, orderproc, tupdatum, tupnull, arrdatum, cur)?;
                if result <= 0 {
                    *set_elem_result = result;
                    return Ok(low_elem);
                }
                mid_elem = low_elem;
                low_elem += 1;
            }
            if high_elem < low_elem {
                *set_elem_result = 1;
                return Ok(high_elem);
            }
        } else {
            high_elem = array.cur_elem - 1;
            if high_elem >= low_elem {
                let arrdatum = array.elem_values[high_elem as usize];
                result = bt_compare_array_skey(frame, orderproc, tupdatum, tupnull, arrdatum, cur)?;
                if result >= 0 {
                    *set_elem_result = result;
                    return Ok(high_elem);
                }
                mid_elem = high_elem;
                high_elem -= 1;
            }
            if high_elem < low_elem {
                *set_elem_result = -1;
                return Ok(low_elem);
            }
        }
    }

    while high_elem > low_elem {
        mid_elem = low_elem + (high_elem - low_elem) / 2;
        let arrdatum = array.elem_values[mid_elem as usize];
        result = bt_compare_array_skey(frame, orderproc, tupdatum, tupnull, arrdatum, cur)?;
        if result == 0 {
            low_elem = mid_elem;
            break;
        }
        if result > 0 {
            low_elem = mid_elem + 1;
        } else {
            high_elem = mid_elem;
        }
    }

    if low_elem != mid_elem {
        result = bt_compare_array_skey(
            frame,
            orderproc,
            tupdatum,
            tupnull,
            array.elem_values[low_elem as usize],
            cur,
        )?;
    }
    *set_elem_result = result;
    Ok(low_elem)
}

/// _bt_start_array_keys.
pub(crate) fn bt_start_array_keys(so: &mut BTScanOpaqueData<'_>, dir: ScanDirection) {
    debug_assert!(so.numArrayKeys > 0);
    debug_assert!(so.qual_ok);

    {
        let BTScanOpaqueData { keyData, arrayKeys, .. } = &mut *so;
        for array in arrayKeys.iter_mut() {
            let skey = &mut keyData[array.scan_key as usize];
            debug_assert!(skey.sk_flags & SK_SEARCHARRAY != 0);
            bt_array_set_low_or_high(skey, array, ScanDirectionIsForward(dir));
        }
    }
    so.scanBehind = false;
    so.oppositeDirCheck = false;
}

// datumCopy for skip-array sk_argument. C divergence: C pfrees the previous
// element; here superseded copies stay in the scan-lifetime arena until reset.
fn skip_datum_copy<'mcx>(
    mcx: Mcx<'mcx>,
    value: Datum,
    attbyval: bool,
    attlen: i16,
) -> PgResult<Datum> {
    if attbyval {
        return Ok(value);
    }
    // SAFETY: by-ref skip elements point at live index-tuple attribute bytes
    // of the attribute's declared shape for the duration of the copy.
    unsafe {
        let p = value.as_usize() as *const u8;
        let len = match attlen {
            l if l > 0 => l as usize,
            -1 => varsize_any(p),
            _ => {
                let mut n = 0usize;
                while *p.add(n) != 0 {
                    n += 1;
                }
                n + 1
            }
        };
        let mut v: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, len)?;
        ::mcx::vec_append_bytes(&mut v, core::slice::from_raw_parts(p, len))?;
        Ok(Datum::from_usize(v.leak().as_ptr() as usize))
    }
}

// _bt_array_set_low_or_high.
fn bt_array_set_low_or_high(
    skey: &mut ScanKeyData,
    array: &mut BTArrayKeyInfo<'_>,
    low_not_high: bool,
) {
    debug_assert!(skey.sk_flags & SK_SEARCHARRAY != 0);

    if array.num_elems != -1 {
        debug_assert!(skey.sk_flags & SK_BT_SKIP == 0);
        let set_elem = if low_not_high { 0 } else { array.num_elems - 1 };
        array.cur_elem = set_elem;
        skey.sk_argument = array.elem_values[set_elem as usize];
        return;
    }

    debug_assert!(skey.sk_flags & SK_BT_SKIP != 0);
    skey.sk_argument = Datum::null();
    skey.sk_flags &=
        !(SK_SEARCHNULL | SK_ISNULL | SK_BT_MINVAL | SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR);

    if array.null_elem && (low_not_high == (skey.sk_flags & SK_BT_NULLS_FIRST != 0)) {
        skey.sk_flags |= SK_SEARCHNULL | SK_ISNULL;
    } else if low_not_high {
        skey.sk_flags |= SK_BT_MINVAL;
    } else {
        skey.sk_flags |= SK_BT_MAXVAL;
    }
}

// _bt_skiparray_set_isnull.
fn bt_skiparray_set_isnull(skey: &mut ScanKeyData, array: &BTArrayKeyInfo<'_>) {
    debug_assert!(skey.sk_flags & SK_BT_SKIP != 0 && skey.sk_flags & SK_SEARCHARRAY != 0);
    debug_assert!(array.null_elem && array.low_compare.is_none() && array.high_compare.is_none());
    skey.sk_argument = Datum::null();
    skey.sk_flags &= !(SK_BT_MINVAL | SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR);
    skey.sk_flags |= SK_SEARCHNULL | SK_ISNULL;
}

/// _bt_skiparray_set_element: advance to tupdatum/tupnull, or to the bound set_elem_result picks.
fn bt_skiparray_set_element<'mcx>(
    mcx: Mcx<'mcx>,
    skey: &mut ScanKeyData,
    array: &mut BTArrayKeyInfo<'_>,
    set_elem_result: i32,
    tupdatum: Datum,
    tupnull: bool,
) -> PgResult<()> {
    debug_assert!(skey.sk_flags & SK_BT_SKIP != 0 && skey.sk_flags & SK_SEARCHARRAY != 0);

    if set_elem_result != 0 {
        debug_assert!(!array.null_elem);
        bt_array_set_low_or_high(skey, array, set_elem_result < 0);
        return Ok(());
    }

    if tupnull {
        bt_skiparray_set_isnull(skey, array);
        return Ok(());
    }

    skey.sk_flags &=
        !(SK_SEARCHNULL | SK_ISNULL | SK_BT_MINVAL | SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR);
    skey.sk_argument = skip_datum_copy(mcx, tupdatum, array.attbyval, array.attlen)?;
    Ok(())
}

// _bt_array_decrement / _bt_array_increment; false = no prior (next) element for the direction.
fn bt_array_step<'mcx>(
    mcx: Mcx<'mcx>,
    frame: &mut OrderProcFrame,
    skey: &mut ScanKeyData,
    array: &mut BTArrayKeyInfo<'_>,
    forward: bool,
) -> PgResult<bool> {
    debug_assert!(skey.sk_flags & SK_SEARCHARRAY != 0);
    debug_assert!(if forward {
        skey.sk_flags & (SK_BT_MINVAL | SK_BT_NEXT | SK_BT_PRIOR) == 0
    } else {
        skey.sk_flags & (SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR) == 0
    });

    if array.num_elems != -1 {
        debug_assert!(skey.sk_flags & (SK_BT_SKIP | SK_BT_MINVAL | SK_BT_MAXVAL) == 0);
        let next = if forward { array.cur_elem + 1 } else { array.cur_elem - 1 };
        if next < 0 || next >= array.num_elems {
            return Ok(false);
        }
        array.cur_elem = next;
        skey.sk_argument = array.elem_values[next as usize];
        return Ok(true);
    }

    debug_assert!(skey.sk_flags & SK_BT_SKIP != 0);

    if skey.sk_flags & (if forward { SK_BT_MAXVAL } else { SK_BT_MINVAL }) != 0 {
        return Ok(false);
    }

    let nulls_first = skey.sk_flags & SK_BT_NULLS_FIRST != 0;
    if skey.sk_flags & SK_ISNULL != 0 && (forward != nulls_first) {
        return Ok(false); // NULL already sorts at this end of the range
    }

    // Without skip support, reposition via the NEXT/PRIOR sentinel instead.
    let Some(sksup) = array.sksup else {
        skey.sk_flags |= if forward { SK_BT_NEXT } else { SK_BT_PRIOR };
        return Ok(true);
    };

    if skey.sk_flags & SK_ISNULL != 0 {
        debug_assert!(forward == nulls_first);
        skey.sk_flags &= !(SK_SEARCHNULL | SK_ISNULL);
        let elem = if forward { sksup.low_elem } else { sksup.high_elem };
        skey.sk_argument = skip_datum_copy(mcx, elem, array.attbyval, array.attlen)?;
        return Ok(true);
    }

    let mut flow = false;
    let new_sk_argument = if forward {
        (sksup.increment)(skey.sk_argument, &mut flow)
    } else {
        (sksup.decrement)(skey.sk_argument, &mut flow)
    };
    if flow {
        if array.null_elem && (forward != nulls_first) {
            bt_skiparray_set_isnull(skey, array);
            return Ok(true);
        }
        return Ok(false);
    }

    let bound = if forward { array.high_compare.as_mut() } else { array.low_compare.as_mut() };
    if let Some(bound) = bound {
        let arg = bound.sk_argument;
        if !frame.test(bound, new_sk_argument, arg)? {
            return Ok(false);
        }
    }

    skey.sk_argument = new_sk_argument;
    Ok(true)
}

/// _bt_advance_array_keys_increment: false = exhausted (restored for the opposite direction, as C).
fn bt_advance_array_keys_increment(
    so: &mut BTScanOpaqueData<'_>,
    dir: ScanDirection,
    skip_array_set: &mut bool,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    {
        let BTScanOpaqueData { keyData, arrayKeys, .. } = &mut *so;
        let mcx = *keyData.allocator();
        for array in arrayKeys.iter_mut().rev() {
            let skey = &mut keyData[array.scan_key as usize];
            if array.num_elems == -1 {
                *skip_array_set = true;
            }
            if bt_array_step(mcx, frame, skey, array, ScanDirectionIsForward(dir))? {
                return Ok(true);
            }
            bt_array_set_low_or_high(skey, array, ScanDirectionIsForward(dir));
        }
    }
    bt_start_array_keys(so, flip_dir(dir));
    Ok(false)
}

/// _bt_binsrch_skiparray_skey: -1/0/1 = below/within/above; `sk_flags` from the array's = key.
fn bt_binsrch_skiparray_skey(
    frame: &mut OrderProcFrame,
    cur_elem_trig: bool,
    dir: ScanDirection,
    tupdatum: Datum,
    tupnull: bool,
    array: &mut BTArrayKeyInfo<'_>,
    sk_flags: i32,
    set_elem_result: &mut i32,
) -> PgResult<()> {
    debug_assert!(sk_flags & SK_BT_SKIP != 0 && sk_flags & SK_SEARCHARRAY != 0);
    debug_assert!(sk_flags & SK_BT_REQFWD != 0);
    debug_assert!(array.num_elems == -1);

    if array.null_elem {
        debug_assert!(array.low_compare.is_none() && array.high_compare.is_none());
        *set_elem_result = 0;
        return Ok(());
    }

    if tupnull {
        *set_elem_result = if sk_flags & SK_BT_NULLS_FIRST != 0 { -1 } else { 1 };
        return Ok(());
    }

    *set_elem_result = 0;
    let BTArrayKeyInfo { low_compare, high_compare, .. } = array;
    if ScanDirectionIsForward(dir) {
        if !cur_elem_trig {
            if let Some(k) = low_compare.as_mut() {
                let arg = k.sk_argument;
                if !frame.test(k, tupdatum, arg)? {
                    *set_elem_result = -1;
                    return Ok(());
                }
            }
        }
        if let Some(k) = high_compare.as_mut() {
            let arg = k.sk_argument;
            if !frame.test(k, tupdatum, arg)? {
                *set_elem_result = 1;
            }
        }
    } else {
        if !cur_elem_trig {
            if let Some(k) = high_compare.as_mut() {
                let arg = k.sk_argument;
                if !frame.test(k, tupdatum, arg)? {
                    *set_elem_result = 1;
                    return Ok(());
                }
            }
        }
        if let Some(k) = low_compare.as_mut() {
            let arg = k.sk_argument;
            if !frame.test(k, tupdatum, arg)? {
                *set_elem_result = -1;
            }
        }
    }
    Ok(())
}

/// _bt_tuple_before_array_skeys: too early to advance required arrays?
///
/// # Safety
/// As [`bt_checkkeys`].
#[allow(clippy::too_many_arguments)]
unsafe fn bt_tuple_before_array_skeys(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    dir: ScanDirection,
    tuple: ITup,
    tupnatts: i32,
    readpagetup: bool,
    sktrig: i32,
    mut scan_behind: Option<&mut bool>,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let tupdesc: &TupleDescData<'_> = &rel.rd_att;
    debug_assert!(so.numArrayKeys > 0 && so.numberOfKeys > 0);
    debug_assert!(sktrig == 0 || readpagetup);
    debug_assert!(!readpagetup || scan_behind.is_none());

    if let Some(sb) = scan_behind.as_deref_mut() {
        *sb = false;
    }

    let BTScanOpaqueData { keyData, orderProcs, arrayKeys, numberOfKeys, .. } = &mut *so;
    for ikey in sktrig as usize..*numberOfKeys as usize {
        let cur = &keyData[ikey];
        debug_assert!(!readpagetup || ikey == sktrig as usize);

        if cur.sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD) == 0 {
            debug_assert!(!readpagetup);
            return Ok(false);
        }

        if cur.sk_attno as i32 > tupnatts {
            debug_assert!(!readpagetup);
            if let Some(sb) = scan_behind.as_deref_mut() {
                *sb = true;
            }
            return Ok(false);
        }

        if cur.sk_strategy != BTEqualStrategyNumber {
            if readpagetup {
                return Ok(false);
            }
            continue;
        }

        let mut tupnull = false;
        let tupdatum = index_getattr(tuple, cur.sk_attno, tupdesc, &mut tupnull);

        let result;
        if cur.sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL) == 0 {
            let mut r = bt_compare_array_skey(
                frame,
                &mut orderProcs[ikey],
                tupdatum,
                tupnull,
                cur.sk_argument,
                cur,
            )?;
            if r == 0 {
                if cur.sk_flags & SK_BT_NEXT != 0 {
                    r = -1;
                } else if cur.sk_flags & SK_BT_PRIOR != 0 {
                    r = 1;
                }
                debug_assert!(r == 0 || cur.sk_flags & SK_BT_SKIP != 0);
            }
            result = r;
        } else {
            debug_assert!(if ScanDirectionIsForward(dir) {
                cur.sk_flags & SK_BT_MAXVAL == 0
            } else {
                cur.sk_flags & SK_BT_MINVAL == 0
            });
            let sk_flags = cur.sk_flags;
            let array = arrayKeys
                .iter_mut()
                .find(|a| a.scan_key == ikey as i32)
                .expect("sentinel key has a skip array");
            let mut r = 0;
            bt_binsrch_skiparray_skey(frame, false, dir, tupdatum, tupnull, array, sk_flags, &mut r)?;
            if r == 0 {
                return Ok(false); // in range: time to advance the arrays
            }
            result = r;
        }

        if (ScanDirectionIsForward(dir) && result < 0)
            || (ScanDirectionIsBackward(dir) && result > 0)
        {
            return Ok(true);
        }

        if readpagetup || result != 0 {
            debug_assert!(result != 0);
            return Ok(false);
        }
    }

    debug_assert!(!readpagetup);
    Ok(false)
}

/// _bt_start_prim_scan: true when another primitive index scan was scheduled.
pub(crate) fn bt_start_prim_scan(
    so: &mut BTScanOpaqueData<'_>,
    parallel: Option<&::types_relscan::BTParallelScanShared>,
) -> bool {
    debug_assert!(so.numArrayKeys > 0);
    so.scanBehind = false;
    so.oppositeDirCheck = false;
    if so.needPrimScan {
        return true;
    }
    // The top-level index scan ran out of tuples in this scan direction.
    crate::parallel::bt_parallel_done(so, parallel);
    false
}

/// _bt_advance_array_keys. `pstate` is Some iff `sktrig_required`.
///
/// # Safety
/// As [`bt_checkkeys`]; `pstate.finaltup` (when set) points into the same
/// pinned+locked page.
#[allow(clippy::too_many_arguments)]
unsafe fn bt_advance_array_keys(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    mut pstate: Option<&mut BtReadPageState<'_>>,
    tuple: ITup,
    tupnatts: i32,
    sktrig: i32,
    sktrig_required: bool,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let tupdesc: &TupleDescData<'_> = &rel.rd_att;
    let dir = so.currPos.dir;
    debug_assert!(!so.needPrimScan && !so.scanBehind && !so.oppositeDirCheck);
    debug_assert!(sktrig_required == pstate.is_some());

    if sktrig_required {
        let p = pstate.as_deref_mut().expect("required caller passes pstate");
        p.rechecks = 0;
        p.targetdistance = 0;
    } else if sktrig < so.numberOfKeys - 1
        && so.keyData[so.numberOfKeys as usize - 1].sk_flags & SK_SEARCHARRAY == 0
    {
        // Precheck the least significant key first (non-required trigger only).
        let mut least_sign_ikey = so.numberOfKeys - 1;
        let mut continuescan = true;
        debug_assert!(so.keyData[sktrig as usize].sk_flags & SK_SEARCHARRAY != 0);
        if !bt_check_compare::<false>(
            rel,
            so,
            dir,
            tuple,
            tupnatts,
            false,
            &mut continuescan,
            &mut least_sign_ikey,
            frame,
        )? {
            return Ok(false);
        }
    }

    let mut beyond_end_advance = false;
    let mut skip_array_advanced = false;
    let mut has_required_opposite_direction_only = false;
    let mut all_required_satisfied = true;
    let mut all_satisfied = true;

    {
        let BTScanOpaqueData {
            keyData,
            arrayKeys,
            orderProcs,
            numberOfKeys,
            scanBehind,
            ..
        } = &mut *so;
        let mcx = *keyData.allocator();
        let mut arrayidx = 0usize;

        for ikey in 0..*numberOfKeys as usize {
            let (sk_flags, sk_strategy, sk_attno) = {
                let cur = &keyData[ikey];
                (cur.sk_flags, cur.sk_strategy, cur.sk_attno)
            };
            let mut array_i: Option<usize> = None;

            if sk_strategy == BTEqualStrategyNumber {
                if sk_flags & SK_SEARCHARRAY != 0 {
                    array_i = Some(arrayidx);
                    debug_assert!(arrayKeys[arrayidx].scan_key == ikey as i32);
                    arrayidx += 1;
                }
            } else if (ScanDirectionIsForward(dir) && sk_flags & SK_BT_REQBKWD != 0)
                || (ScanDirectionIsBackward(dir) && sk_flags & SK_BT_REQFWD != 0)
            {
                has_required_opposite_direction_only = true;
            }

            if ikey < sktrig as usize {
                continue;
            }

            let mut required = false;
            if sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD) != 0 {
                required = true;
                if sk_attno as i32 > tupnatts {
                    debug_assert!((sktrig as usize) < ikey);
                    *scanBehind = true;
                }
            }

            if ikey == sktrig as usize && array_i.is_none() {
                debug_assert!(sktrig_required && required && all_required_satisfied);
                beyond_end_advance = true;
                all_satisfied = false;
                all_required_satisfied = false;
                continue;
            } else if sk_strategy != BTEqualStrategyNumber {
                continue;
            } else if !required && array_i.is_none() {
                continue;
            }

            if beyond_end_advance {
                if let Some(ai) = array_i {
                    bt_array_set_low_or_high(
                        &mut keyData[ikey],
                        &mut arrayKeys[ai],
                        ScanDirectionIsBackward(dir),
                    );
                }
                continue;
            }

            if !all_required_satisfied || sk_attno as i32 > tupnatts {
                if let Some(ai) = array_i {
                    bt_array_set_low_or_high(
                        &mut keyData[ikey],
                        &mut arrayKeys[ai],
                        ScanDirectionIsForward(dir),
                    );
                }
                continue;
            }

            let mut tupnull = false;
            let tupdatum = index_getattr(tuple, sk_attno, tupdesc, &mut tupnull);

            let mut result: i32 = 0;
            let mut set_elem: i32 = 0;
            if let Some(ai) = array_i {
                let cur_elem_trig = sktrig_required && ikey == sktrig as usize;
                if arrayKeys[ai].num_elems == -1 {
                    bt_binsrch_skiparray_skey(
                        frame,
                        cur_elem_trig,
                        dir,
                        tupdatum,
                        tupnull,
                        &mut arrayKeys[ai],
                        sk_flags,
                        &mut result,
                    )?;
                } else {
                    set_elem = bt_binsrch_array_skey(
                        frame,
                        &mut orderProcs[ikey],
                        cur_elem_trig,
                        dir,
                        tupdatum,
                        tupnull,
                        &arrayKeys[ai],
                        &keyData[ikey],
                        &mut result,
                    )?;
                }
            } else {
                debug_assert!(required);
                result = bt_compare_array_skey(
                    frame,
                    &mut orderProcs[ikey],
                    tupdatum,
                    tupnull,
                    keyData[ikey].sk_argument,
                    &keyData[ikey],
                )?;
            }

            if sktrig_required
                && required
                && ((ScanDirectionIsForward(dir) && result > 0)
                    || (ScanDirectionIsBackward(dir) && result < 0))
            {
                beyond_end_advance = true;
            }

            debug_assert!(all_required_satisfied && all_satisfied);
            if result != 0 {
                all_satisfied = false;
                if sktrig_required && required {
                    all_required_satisfied = false;
                } else {
                    break;
                }
            }

            if let Some(ai) = array_i {
                let array = &mut arrayKeys[ai];
                if array.num_elems == -1 {
                    bt_skiparray_set_element(
                        mcx,
                        &mut keyData[ikey],
                        array,
                        result,
                        tupdatum,
                        tupnull,
                    )?;
                    skip_array_advanced = true;
                } else if array.cur_elem != set_elem {
                    array.cur_elem = set_elem;
                    keyData[ikey].sk_argument = array.elem_values[set_elem as usize];
                }
            }
        }
    }

    if beyond_end_advance
        && !bt_advance_array_keys_increment(so, dir, &mut skip_array_advanced, frame)?
    {
        // end_toplevel_scan: whole top-level scan is done in this direction.
        let p = pstate.expect("beyond-end advancement implies sktrig_required");
        p.continuescan = false;
        so.needPrimScan = false;
        return Ok(false);
    }

    if sktrig_required && skip_array_advanced {
        pstate
            .as_deref_mut()
            .expect("sktrig_required caller passes pstate")
            .nskipadvances += 1;
    }

    if (sktrig_required && all_required_satisfied) || (!sktrig_required && all_satisfied) {
        let mut nsktrig = sktrig + 1;
        let mut continuescan = true;
        debug_assert!(all_required_satisfied);

        if bt_check_compare::<false>(
            rel,
            so,
            dir,
            tuple,
            tupnatts,
            !sktrig_required,
            &mut continuescan,
            &mut nsktrig,
            frame,
        )? && !so.scanBehind
        {
            debug_assert!(all_satisfied && continuescan);
            if let Some(p) = pstate.as_deref_mut() {
                p.continuescan = true;
            }
            return Ok(true);
        }

        if !continuescan {
            debug_assert!(sktrig_required);
            debug_assert!(
                so.keyData[nsktrig as usize].sk_strategy != BTEqualStrategyNumber
            );
            debug_assert!(!beyond_end_advance);
            let satisfied =
                bt_advance_array_keys(rel, so, pstate, tuple, tupnatts, nsktrig, true, frame)?;
            debug_assert!(!satisfied);
            return Ok(false);
        }
    }

    if !sktrig_required {
        return Ok(false);
    }

    let pstate = pstate.expect("sktrig_required caller passes pstate");

    'new_prim_scan: {
        if !all_required_satisfied && pstate.finaltup == Some(tuple) {
            break 'new_prim_scan;
        }

        if !all_required_satisfied {
            if let Some(finaltup) = pstate.finaltup {
                let mut sb = false;
                let before = bt_tuple_before_array_skeys(
                    rel,
                    so,
                    dir,
                    finaltup,
                    bt_tuple_get_natts(finaltup, rel.indnatts()),
                    false,
                    0,
                    Some(&mut sb),
                    frame,
                )?;
                so.scanBehind = sb;
                if before {
                    break 'new_prim_scan;
                }
            }
        }

        if so.scanBehind {
            // Truncated high key -- _bt_scanbehind_checkkeys recheck scheduled.
        } else if has_required_opposite_direction_only {
            if let Some(finaltup) = pstate.finaltup {
                if !bt_oppodir_checkkeys(rel, so, dir, finaltup, frame)? {
                    break 'new_prim_scan;
                }
            }
        }

        // continue_scan:
        pstate.continuescan = true;
        so.needPrimScan = false;
        if so.scanBehind {
            so.oppositeDirCheck = has_required_opposite_direction_only;
            if ScanDirectionIsForward(dir) {
                pstate.skip = pstate.maxoff + 1;
            }
        }
        return Ok(false);
    }

    // new_prim_scan:
    debug_assert!(pstate.finaltup.is_some());

    if !pstate.firstpage || pstate.nskipadvances > NSKIPADVANCES_THRESHOLD {
        so.scanBehind = true;
        pstate.continuescan = true;
        so.needPrimScan = false;
        so.oppositeDirCheck = has_required_opposite_direction_only;
        if ScanDirectionIsForward(dir) {
            pstate.skip = pstate.maxoff + 1;
        }
        return Ok(false);
    }

    pstate.continuescan = false;
    so.needPrimScan = true;
    Ok(false)
}

/// _bt_checkkeys. `tupnatts` may be < key count (truncated high key). ARRAY_KEYS
/// const-folds the array-advance tail dead in the numArrayKeys==0 instantiation,
/// keeping it inlinable in the readpage loop (runtime bool cost the range lane ~4% instr).
///
/// # Safety
/// `tuple` points at a live index tuple on a page pinned+locked by caller.
pub(crate) unsafe fn bt_checkkeys<const ARRAY_KEYS: bool>(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    pstate: &mut BtReadPageState<'_>,
    tuple: ITup,
    tupnatts: i32,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let dir = so.currPos.dir;
    let mut ikey = pstate.startikey;
    debug_assert!(!so.needPrimScan && !so.scanBehind && !so.oppositeDirCheck);
    debug_assert!(ARRAY_KEYS || so.numArrayKeys == 0);
    debug_assert!(!pstate.forcenonrequired || ARRAY_KEYS);

    let res = bt_check_compare::<ARRAY_KEYS>(
        rel,
        so,
        dir,
        tuple,
        tupnatts,
        pstate.forcenonrequired,
        &mut pstate.continuescan,
        &mut ikey,
        frame,
    )?;

    if !ARRAY_KEYS || pstate.continuescan {
        return Ok(res);
    }

    debug_assert!(!pstate.forcenonrequired);
    if bt_tuple_before_array_skeys(rel, so, dir, tuple, tupnatts, true, ikey, None, frame)? {
        pstate.continuescan = true;
        pstate.rechecks += 1;
        if pstate.rechecks >= LOOK_AHEAD_REQUIRED_RECHECKS {
            bt_checkkeys_look_ahead(rel, so, pstate, tupnatts, frame)?;
        }
        return Ok(false);
    }

    bt_advance_array_keys(rel, so, Some(pstate), tuple, tupnatts, ikey, true, frame)
}

/// _bt_scanbehind_checkkeys: is finaltup still before the start of matches
/// for the current array keys?
///
/// # Safety
/// `finaltup` per [`bt_checkkeys`].
pub(crate) unsafe fn bt_scanbehind_checkkeys(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    dir: ScanDirection,
    finaltup: ITup,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let nfinaltupatts = bt_tuple_get_natts(finaltup, rel.indnatts());
    debug_assert!(so.numArrayKeys > 0);

    let mut sb = false;
    if bt_tuple_before_array_skeys(
        rel,
        so,
        dir,
        finaltup,
        nfinaltupatts,
        false,
        0,
        Some(&mut sb),
        frame,
    )? {
        return Ok(false);
    }

    if sb {
        return Ok(false);
    }

    if !so.oppositeDirCheck {
        return Ok(true);
    }

    bt_oppodir_checkkeys(rel, so, dir, finaltup, frame)
}

// _bt_oppodir_checkkeys: false when an inequality required in the opposite
// direction only isn't satisfied by finaltup.
unsafe fn bt_oppodir_checkkeys(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    dir: ScanDirection,
    finaltup: ITup,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let nfinaltupatts = bt_tuple_get_natts(finaltup, rel.indnatts());
    let flipped = flip_dir(dir);
    let mut ikey = 0;
    let mut continuescan = true;
    debug_assert!(so.numArrayKeys > 0);

    bt_check_compare::<false>(
        rel,
        so,
        flipped,
        finaltup,
        nfinaltupatts,
        false,
        &mut continuescan,
        &mut ikey,
        frame,
    )?;

    if !continuescan && so.keyData[ikey as usize].sk_strategy != BTEqualStrategyNumber {
        return Ok(false);
    }
    Ok(true)
}

/// _bt_check_rowcompare (nbtutils.c): compare row members column-by-column;
/// the deciding member's strategy and requiredness flags drive the result and
/// continuescan, with the C NULL semantics (NULL member keys and NULL tuple
/// values both fail the qual, ending the scan only when the required-direction
/// flags say no later tuple can pass).
///
/// # Safety
/// `header.sk_argument` is the live SK_ROW_END-terminated subkey array
/// (scankey.rs contract); `tuple` as [`bt_checkkeys`].
#[allow(clippy::too_many_arguments)]
unsafe fn bt_check_rowcompare(
    header: &mut ScanKeyData,
    tuple: ITup,
    tupnatts: i32,
    tupdesc: &TupleDescData<'_>,
    dir: ScanDirection,
    forcenonrequired: bool,
    continuescan: &mut bool,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let first = header.sk_argument.as_usize() as *mut ScanKeyData;
    let mut subkey = first;
    let mut cmpresult: i32;

    debug_assert!(header.sk_flags & SK_ROW_HEADER != 0);
    debug_assert!((*subkey).sk_attno == header.sk_attno);
    debug_assert!((*subkey).sk_strategy == header.sk_strategy);

    loop {
        debug_assert!((*subkey).sk_flags & SK_ROW_MEMBER != 0);

        if (*subkey).sk_flags & SK_ISNULL != 0 {
            // A NULL member key never matches (never the first member:
            // preprocessing marks that qual unsatisfiable); all earlier
            // members are required, so look one back for requiredness.
            debug_assert!(subkey != first);
            subkey = subkey.sub(1);
            if forcenonrequired {
                // treating scan's keys as non-required
            } else if (*subkey).sk_flags & SK_BT_REQFWD != 0 && ScanDirectionIsForward(dir) {
                *continuescan = false;
            } else if (*subkey).sk_flags & SK_BT_REQBKWD != 0 && ScanDirectionIsBackward(dir) {
                *continuescan = false;
            }
            return Ok(false);
        }

        if (*subkey).sk_attno as i32 > tupnatts {
            // Truncated high-key attribute could hold any value on the page
            // to the right: assume it passes.
            debug_assert!(bt_tuple_is_pivot(tuple));
            return Ok(true);
        }

        let mut is_null = false;
        let datum = index_getattr(tuple, (*subkey).sk_attno, tupdesc, &mut is_null);

        if is_null {
            if forcenonrequired {
                // treating scan's keys as non-required
            } else if (*subkey).sk_flags & SK_BT_NULLS_FIRST != 0 {
                // NULLs sort first: the lower limit of this attr's range; a
                // required member ends a backward scan. The first member may
                // also use its opposite-direction flag (safe only there).
                let mut reqflags = SK_BT_REQBKWD;
                if subkey == first {
                    reqflags |= SK_BT_REQFWD;
                }
                if (*subkey).sk_flags & reqflags != 0 && ScanDirectionIsBackward(dir) {
                    *continuescan = false;
                }
            } else {
                // NULLs sort last: the upper limit; mirror-image of above.
                let mut reqflags = SK_BT_REQFWD;
                if subkey == first {
                    reqflags |= SK_BT_REQBKWD;
                }
                if (*subkey).sk_flags & reqflags != 0 && ScanDirectionIsForward(dir) {
                    *continuescan = false;
                }
            }
            return Ok(false);
        }

        // Three-way comparison, not a bool operator.
        let arg = (*subkey).sk_argument;
        cmpresult = frame.cmp(&mut *subkey, datum, arg)?;
        if (*subkey).sk_flags & SK_BT_DESC != 0 {
            cmpresult = INVERT_COMPARE_RESULT(cmpresult);
        }
        if cmpresult != 0 {
            break;
        }
        if (*subkey).sk_flags & SK_ROW_END != 0 {
            break;
        }
        subkey = subkey.add(1);
    }

    // subkey is the deciding column (or the last on all-equal).
    let result = match (*subkey).sk_strategy {
        BTLessStrategyNumber => cmpresult < 0,
        BTLessEqualStrategyNumber => cmpresult <= 0,
        BTGreaterEqualStrategyNumber => cmpresult >= 0,
        BTGreaterStrategyNumber => cmpresult > 0,
        other => panic!("unexpected strategy number {other}"),
    };

    if !result && !forcenonrequired {
        // Requiredness is judged at the deciding column.
        if (*subkey).sk_flags & SK_BT_REQFWD != 0 && ScanDirectionIsForward(dir) {
            *continuescan = false;
        } else if (*subkey).sk_flags & SK_BT_REQBKWD != 0 && ScanDirectionIsBackward(dir) {
            *continuescan = false;
        }
    }
    Ok(result)
}

/// _bt_check_compare. const ADVANCE_NONREQUIRED folds the array-advance arm dead
/// in the numArrayKeys==0 instantiation (inlines into bt_checkkeys::<false>).
///
/// # Safety
/// As [`bt_checkkeys`].
#[allow(clippy::too_many_arguments)]
#[inline(always)]
unsafe fn bt_check_compare<const ADVANCE_NONREQUIRED: bool>(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    dir: ScanDirection,
    tuple: ITup,
    tupnatts: i32,
    forcenonrequired: bool,
    continuescan: &mut bool,
    ikey: &mut i32,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let tupdesc: &TupleDescData<'_> = &rel.rd_att;
    *continuescan = true;

    while *ikey < so.numberOfKeys {
        let key = &mut so.keyData[*ikey as usize];
        let mut required_same_dir = false;
        let mut required_opposite_dir_only = false;

        if forcenonrequired {
        } else if ((key.sk_flags & SK_BT_REQFWD) != 0 && ScanDirectionIsForward(dir))
            || ((key.sk_flags & SK_BT_REQBKWD) != 0 && ScanDirectionIsBackward(dir))
        {
            required_same_dir = true;
        } else if ((key.sk_flags & SK_BT_REQFWD) != 0 && ScanDirectionIsBackward(dir))
            || ((key.sk_flags & SK_BT_REQBKWD) != 0 && ScanDirectionIsForward(dir))
        {
            required_opposite_dir_only = true;
        }

        if key.sk_attno as i32 > tupnatts {
            debug_assert!(bt_tuple_is_pivot(tuple));
            *ikey += 1;
            continue;
        }

        if key.sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR) != 0 {
            debug_assert!(key.sk_flags & SK_SEARCHARRAY != 0 && key.sk_flags & SK_BT_SKIP != 0);
            debug_assert!(required_same_dir || forcenonrequired);
            let trig = *ikey;
            return check_compare_sentinel(
                rel, so, tuple, tupnatts, trig, forcenonrequired, continuescan, frame,
            );
        }

        if key.sk_flags & SK_ROW_HEADER != 0 {
            // SAFETY: caller's contract; the walk stays within the header's
            // SK_ROW_END-terminated subkey array.
            if bt_check_rowcompare(
                key,
                tuple,
                tupnatts,
                tupdesc,
                dir,
                forcenonrequired,
                continuescan,
                frame,
            )? {
                *ikey += 1;
                continue;
            }
            return Ok(false);
        }

        let mut is_null = false;
        let datum = index_getattr(tuple, key.sk_attno, tupdesc, &mut is_null);

        if key.sk_flags & SK_ISNULL != 0 {
            let satisfied = if key.sk_flags & SK_SEARCHNULL != 0 {
                is_null
            } else {
                debug_assert!(key.sk_flags & SK_BT_SKIP == 0);
                !is_null
            };
            if satisfied {
                *ikey += 1;
                continue;
            }
            if required_same_dir {
                *continuescan = false;
            } else if key.sk_flags & SK_BT_SKIP != 0 {
                // A NULL element satisfies a nonrequired non-range skip array.
                debug_assert!(forcenonrequired && *ikey > 0);
                *ikey += 1;
                continue;
            }
            return Ok(false);
        }

        if is_null {
            if forcenonrequired && key.sk_flags & SK_BT_SKIP != 0 {
                let trig = *ikey;
                return advance_nonrequired_cold(rel, so, tuple, tupnatts, trig, frame);
            }
            if key.sk_flags & SK_BT_NULLS_FIRST != 0 {
                if (required_same_dir || required_opposite_dir_only)
                    && ScanDirectionIsBackward(dir)
                {
                    *continuescan = false;
                }
            } else {
                if (required_same_dir || required_opposite_dir_only)
                    && ScanDirectionIsForward(dir)
                {
                    *continuescan = false;
                }
            }
            return Ok(false);
        }

        let arg = key.sk_argument;
        if !frame.test(key, datum, arg)? {
            if required_same_dir {
                *continuescan = false;
            } else if ADVANCE_NONREQUIRED
                && key.sk_strategy == BTEqualStrategyNumber
                && key.sk_flags & SK_SEARCHARRAY != 0
            {
                let trig = *ikey;
                return bt_advance_array_keys(
                    rel, so, None, tuple, tupnatts, trig, false, frame,
                );
            }
            return Ok(false);
        }

        *ikey += 1;
    }

    Ok(true)
}

// Cold-outlined skip-array sentinel arm of _bt_check_compare: keeps the hot
// per-tuple loop the same test+jump shape as before skip scan landed.
#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
unsafe fn check_compare_sentinel(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    tuple: ITup,
    tupnatts: i32,
    ikey: i32,
    forcenonrequired: bool,
    continuescan: &mut bool,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    if forcenonrequired {
        return bt_advance_array_keys(rel, so, None, tuple, tupnatts, ikey, false, frame);
    }
    *continuescan = false;
    Ok(false)
}

#[cold]
#[inline(never)]
unsafe fn advance_nonrequired_cold(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    tuple: ITup,
    tupnatts: i32,
    ikey: i32,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    bt_advance_array_keys(rel, so, None, tuple, tupnatts, ikey, false, frame)
}

// _bt_checkkeys_look_ahead: probe a later tuple; set pstate.skip while still before the arrays.
//
// # Safety
// As [`bt_checkkeys`]; pstate.page items live for the call.
unsafe fn bt_checkkeys_look_ahead(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    pstate: &mut BtReadPageState<'_>,
    tupnatts: i32,
    frame: &mut OrderProcFrame,
) -> PgResult<()> {
    let dir = so.currPos.dir;
    debug_assert!(!pstate.forcenonrequired);

    if pstate.offnum < pstate.minoff {
        return Ok(());
    }

    if ScanDirectionIsForward(dir) {
        if pstate.offnum as i32 >= pstate.maxoff as i32 - LOOK_AHEAD_DEFAULT_DISTANCE {
            return Ok(());
        }
    } else if pstate.offnum as i32 <= pstate.minoff as i32 + LOOK_AHEAD_DEFAULT_DISTANCE {
        return Ok(());
    }

    if pstate.targetdistance == 0 {
        pstate.targetdistance = LOOK_AHEAD_DEFAULT_DISTANCE;
    } else if (pstate.targetdistance as usize) < MaxTIDsPerBTreePage / 2 {
        pstate.targetdistance *= 2;
    }

    let aheadoffnum = if ScanDirectionIsForward(dir) {
        (pstate.maxoff as i32).min(pstate.offnum as i32 + pstate.targetdistance)
    } else {
        (pstate.minoff as i32).max(pstate.offnum as i32 - pstate.targetdistance)
    } as OffsetNumber;

    let ahead = page_item(&pstate.page, pstate.page.item_id(aheadoffnum));
    if bt_tuple_before_array_skeys(rel, so, dir, ahead, tupnatts, false, 0, None, frame)? {
        pstate.skip = if ScanDirectionIsForward(dir) {
            aheadoffnum + 1
        } else {
            aheadoffnum - 1
        };
    } else {
        pstate.rechecks = 0;
        pstate.targetdistance = (pstate.targetdistance / 8).max(1);
    }
    Ok(())
}

/// datum_image_eq (datum.c) for index-tuple callers: no external toast, no expanded datums.
///
/// # Safety
/// By-ref datums point at live in-page values of the attribute's type shape.
unsafe fn datum_image_eq(a: Datum, b: Datum, attbyval: bool, attlen: i16) -> bool {
    if attbyval {
        // Compare at attlen width: a formed-then-deformed datum may differ
        // from the original in the upper bits (C 49315de).
        let (x, y) = (a.as_usize(), b.as_usize());
        return match attlen {
            1 => x as u8 == y as u8,
            2 => x as u16 == y as u16,
            4 => x as u32 == y as u32,
            _ => x == y,
        };
    }
    let pa = a.as_usize() as *const u8;
    let pb = b.as_usize() as *const u8;
    if attlen > 0 {
        return core::slice::from_raw_parts(pa, attlen as usize)
            == core::slice::from_raw_parts(pb, attlen as usize);
    }
    if attlen == -1 {
        let la = varsize_any(pa);
        let lb = varsize_any(pb);
        return la == lb
            && core::slice::from_raw_parts(pa, la) == core::slice::from_raw_parts(pb, lb);
    }
    debug_assert!(attlen == -2);
    let mut i = 0;
    loop {
        let (ca, cb) = (*pa.add(i), *pb.add(i));
        if ca != cb {
            return false;
        }
        if ca == 0 {
            return true;
        }
        i += 1;
    }
}

/// _bt_keep_natts_fast: first differing attribute (1-based), capped at keysz+1.
///
/// # Safety
/// As [`bt_checkkeys`] for both tuples.
pub unsafe fn bt_keep_natts_fast(
    rel: &Relation<'_>,
    lastleft: ITup,
    firstright: ITup,
) -> i32 {
    let tupdesc: &TupleDescData<'_> = &rel.rd_att;
    let keysz = rel.indnkeyatts();
    let mut keepnatts = 1;

    for attnum in 1..=keysz {
        let mut null1 = false;
        let mut null2 = false;
        let d1 = index_getattr(lastleft, attnum as i16, tupdesc, &mut null1);
        let d2 = index_getattr(firstright, attnum as i16, tupdesc, &mut null2);
        let att = tupdesc.compact_attr((attnum - 1) as usize);

        if null1 != null2 {
            break;
        }
        if !null1 && !datum_image_eq(d1, d2, att.attbyval, att.attlen) {
            break;
        }
        keepnatts += 1;
    }
    keepnatts
}

/// _bt_set_startikey: skip re-evaluating keys that every tuple on this page
/// provably satisfies (C's page-level precheck; rule-5 fastpath).
///
/// # Safety
/// Page in `pstate` is pinned+locked by caller.
pub(crate) unsafe fn bt_set_startikey(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    pstate: &mut BtReadPageState<'_>,
    frame: &mut OrderProcFrame,
) -> PgResult<()> {
    let tupdesc: &TupleDescData<'_> = &rel.rd_att;
    debug_assert!(!so.scanBehind && !pstate.firstpage && pstate.minoff < pstate.maxoff);
    debug_assert!(pstate.startikey == 0);

    if so.numberOfKeys == 0 {
        return Ok(());
    }

    let firsttup = page_item(&pstate.page, pstate.page.item_id(pstate.minoff));
    let lasttup = page_item(&pstate.page, pstate.page.item_id(pstate.maxoff));

    let firstchangingattnum = bt_keep_natts_fast(rel, firsttup, lasttup);

    let BTScanOpaqueData { keyData, arrayKeys, orderProcs, numberOfKeys, skipScan, .. } =
        &mut *so;
    let mut start_past_saop_eq = false;
    let mut arrayidx = 0usize;
    let mut startikey: i32 = 0;
    while startikey < *numberOfKeys {
        let key = &mut keyData[startikey as usize];

        if key.sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD) == 0 {
            break; // unsafe: key isn't marked required (corner case)
        }
        if key.sk_flags & SK_ROW_HEADER != 0 {
            break; // "unsafe": row compares not supported here
        }
        if key.sk_strategy != BTEqualStrategyNumber {
            // it and no prior attribute has multiple distinct values.
            if key.sk_attno as i32 > firstchangingattnum {
                break;
            }
            let mut firstnull = false;
            let mut lastnull = false;
            let firstdatum = index_getattr(firsttup, key.sk_attno, tupdesc, &mut firstnull);
            let lastdatum = index_getattr(lasttup, key.sk_attno, tupdesc, &mut lastnull);

            if key.sk_flags & SK_ISNULL != 0 {
                if firstnull || lastnull {
                    break;
                }
                startikey += 1;
                continue;
            }
            let arg = key.sk_argument;
            if firstnull || !frame.test(key, firstdatum, arg)? {
                break;
            }
            if lastnull || !frame.test(key, lastdatum, arg)? {
                break;
            }
            startikey += 1;
            continue;
        }

        if key.sk_flags & SK_SEARCHARRAY != 0 {
            let ai = arrayidx;
            debug_assert!(arrayKeys[ai].scan_key == startikey);
            arrayidx += 1;
            if arrayKeys[ai].num_elems == -1 {
                debug_assert!(key.sk_flags & SK_BT_SKIP != 0);
                if arrayKeys[ai].null_elem {
                    // Non-range skip array is satisfied by every tuple.
                    startikey += 1;
                    continue;
                }
                if key.sk_attno as i32 > firstchangingattnum {
                    break;
                }
                let sk_flags = key.sk_flags;
                let mut firstnull = false;
                let mut lastnull = false;
                let firstdatum = index_getattr(firsttup, key.sk_attno, tupdesc, &mut firstnull);
                let lastdatum = index_getattr(lasttup, key.sk_attno, tupdesc, &mut lastnull);
                let mut result = 0;
                bt_binsrch_skiparray_skey(
                    frame,
                    false,
                    ForwardScanDirection,
                    firstdatum,
                    firstnull,
                    &mut arrayKeys[ai],
                    sk_flags,
                    &mut result,
                )?;
                if result != 0 {
                    break;
                }
                bt_binsrch_skiparray_skey(
                    frame,
                    false,
                    ForwardScanDirection,
                    lastdatum,
                    lastnull,
                    &mut arrayKeys[ai],
                    sk_flags,
                    &mut result,
                )?;
                if result != 0 {
                    break;
                }
                startikey += 1;
                continue;
            }
            // SAOP array = key: binary search for a matching element rather
            // than relying on the key's current sk_argument.
            let array = &arrayKeys[ai];
            if key.sk_attno as i32 >= firstchangingattnum {
                break;
            }
            let mut firstnull = false;
            let firstdatum = index_getattr(firsttup, key.sk_attno, tupdesc, &mut firstnull);
            let mut result = 0;
            bt_binsrch_array_skey(
                frame,
                &mut orderProcs[startikey as usize],
                false,
                ::types_scan::sdir::NoMovementScanDirection,
                firstdatum,
                firstnull,
                array,
                key,
                &mut result,
            )?;
            if result != 0 {
                break;
            }
            start_past_saop_eq = true;
            startikey += 1;
            continue;
        }

        if key.sk_attno as i32 >= firstchangingattnum {
            break;
        }
        let mut firstnull = false;
        let firstdatum = index_getattr(firsttup, key.sk_attno, tupdesc, &mut firstnull);
        if key.sk_flags & SK_ISNULL != 0 {
            debug_assert!(key.sk_flags & SK_SEARCHNULL != 0);
            if !firstnull {
                break;
            }
            startikey += 1;
            continue;
        }
        let arg = key.sk_argument;
        if firstnull || !frame.test(key, firstdatum, arg)? {
            break;
        }
        startikey += 1;
    }

    pstate.forcenonrequired = start_past_saop_eq || *skipScan;
    pstate.startikey = startikey;

    debug_assert!(!pstate.forcenonrequired || so.numArrayKeys > 0);
    if pstate.forcenonrequired && pstate.finaltup.is_none() {
        pstate.forcenonrequired = false;
        pstate.startikey = 0;
    }
    Ok(())
}

unsafe fn mark_itemid_dead(page: &PageRef<'_>, offnum: OffsetNumber) {
    let off = SizeOfPageHeaderData + (offnum as usize - 1) * core::mem::size_of::<ItemIdData>();
    let p = page.as_ptr().add(off).cast::<ItemIdData>().cast_mut();
    // SAFETY: in-bounds item id (caller validated offnum <= maxoff); content
    // lock held; hint stores race-tolerated by C's contract.
    let mut iid = p.read();
    iid.mark_dead();
    p.write(iid);
}

unsafe fn set_has_garbage(page: &PageRef<'_>) {
    let off = page_special_off(page) + core::mem::offset_of!(BTPageOpaqueData, btpo_flags);
    let p = page.as_ptr().add(off).cast::<u16>().cast_mut();
    // SAFETY: special area in-bounds; same hint-store contract as above.
    p.write(p.read() | BTP_HAS_GARBAGE);
}

/// _bt_killitems.
pub(crate) fn bt_killitems(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
) -> PgResult<()> {
    let num_killed = so.numKilled as usize;
    debug_assert!(num_killed > 0);
    debug_assert!(BTScanPosIsValid(&so.currPos));

    so.numKilled = 0;

    let (buf, owned) = if !so.dropPin {
        debug_assert!(BTScanPosIsPinned(&so.currPos));
        bufmgr::lock_buffer::call(so.currPos.buf, BT_READ)?;
        (so.currPos.buf, None)
    } else {
        debug_assert!(!BTScanPosIsPinned(&so.currPos));
        let pin = bt_getbuf(rel, so.currPos.currPage, BT_READ)?;

        let latestlsn: XLogRecPtr = bufmgr::buffer_get_lsn_atomic::call(pin.buffer());
        debug_assert!(so.currPos.lsn <= latestlsn);
        if so.currPos.lsn != latestlsn {
            bt_relbuf(rel, pin)?;
            return Ok(());
        }
        (pin.buffer(), Some(pin))
    };

    // SAFETY: pinned (either arm) and locked just above.
    let page = unsafe { PageRef::from_raw(bufmgr::buffer_get_page::call(buf)) };
    let opaque = page_opaque(&page);
    let minoff = P_FIRSTDATAKEY(&opaque);
    let maxoff = page.max_offset_number();
    let mut killedsomething = false;

    for i in 0..num_killed {
        let item_index = so.killedItems[i] as usize;
        debug_assert!(
            item_index >= so.currPos.firstItem as usize
                && item_index <= so.currPos.lastItem as usize
        );
        // SAFETY: killedItems only holds indexes _bt_readpage wrote.
        let mut kitem = unsafe { so.currPos.item(item_index) };
        let mut offnum = kitem.indexOffset;

        if offnum < minoff {
            continue; // pure paranoia
        }
        while offnum <= maxoff {
            let iid = page.item_id(offnum);
            let ituple = page_item(&page, iid);
            let mut killtuple = false;

            // SAFETY: pinned+locked page item.
            unsafe {
                if bt_tuple_is_posting(ituple) {
                    let mut pi = i + 1;
                    let nposting = bt_tuple_get_nposting(ituple);
                    let mut j = 0;
                    while j < nposting {
                        let item = bt_tuple_get_posting_n(ituple, j);
                        if !ItemPointerEquals(&item, &kitem.heapTid) {
                            break;
                        }
                        debug_assert!(kitem.indexOffset == offnum || !so.dropPin);
                        if pi < num_killed {
                            kitem = so.currPos.item(so.killedItems[pi] as usize);
                            pi += 1;
                        }
                        j += 1;
                    }
                    if j == nposting {
                        killtuple = true;
                    }
                } else if ItemPointerEquals(&crate::itup::t_tid(ituple), &kitem.heapTid) {
                    killtuple = true;
                }

                if killtuple && !iid.is_dead() {
                    mark_itemid_dead(&page, offnum);
                    killedsomething = true;
                    break;
                }
            }
            offnum += 1;
        }
    }

    if killedsomething {
        // SAFETY: page pinned+locked; BTP_HAS_GARBAGE is a hint bit.
        unsafe { set_has_garbage(&page) };
        bufmgr::mark_buffer_dirty_hint::call(buf, true)?;
    }

    match owned {
        Some(pin) => bt_relbuf(rel, pin)?,
        // The pin stays owned by so->currPos: drop only the lock.
        None => bufmgr::lock_buffer::call(buf, bufmgr::BUFFER_LOCK_UNLOCK)?,
    }
    Ok(())
}

/// _bt_truncate: pivot tuple for a leaf split, suffix-truncated where the
/// keys distinguish the halves, heap-TID-appended where they don't.
///
/// # Safety
/// Both tuples per [`bt_checkkeys`]; neither is a pivot.
pub unsafe fn bt_truncate<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    rel: &Relation<'_>,
    lastleft: ITup,
    firstright: ITup,
    itup_key: &mut BtScanInsert,
    frame: &mut OrderProcFrame,
) -> PgResult<crate::itup::ItupBuf<'mcx>> {
    use crate::itup::{
        bt_tuple_get_max_heap_tid, bt_tuple_get_posting_offset, bt_tuple_set_natts,
        index_truncate_tuple, maxalign, index_tuple_size, set_t_info, t_info,
        INDEX_SIZE_MASK,
    };

    let tupdesc: &TupleDescData<'_> = &rel.rd_att;
    let nkeyatts = rel.indnkeyatts() as usize;

    debug_assert!(!bt_tuple_is_pivot(lastleft) && !bt_tuple_is_pivot(firstright));

    let keepnatts = bt_keep_natts(rel, lastleft, firstright, itup_key, frame)?;

    let mut pivot = index_truncate_tuple(mcx, tupdesc, firstright, keepnatts.min(nkeyatts))?;

    if bt_tuple_is_posting(pivot.as_ptr()) {
        // straight copy of a posting firstright: chop the posting list here.
        debug_assert!(keepnatts == nkeyatts || keepnatts == nkeyatts + 1);
        debug_assert!(rel.indnatts() as usize == nkeyatts);
        let sz = maxalign(bt_tuple_get_posting_offset(pivot.as_ptr()));
        set_t_info(
            pivot.as_mut_ptr(),
            (t_info(pivot.as_ptr()) & !INDEX_SIZE_MASK) | sz as u16,
        );
    }

    if keepnatts <= nkeyatts {
        bt_tuple_set_natts(pivot.as_mut_ptr(), keepnatts as u16, false);
        return Ok(pivot);
    }

    let newsize =
        maxalign(index_tuple_size(pivot.as_ptr())) + maxalign(core::mem::size_of::<ItemPointerData>());
    let mut tidpivot = crate::itup::ItupBuf::with_size(mcx, newsize)?;
    core::ptr::copy_nonoverlapping(
        pivot.as_ptr(),
        tidpivot.as_mut_ptr(),
        maxalign(index_tuple_size(pivot.as_ptr())),
    );
    set_t_info(
        tidpivot.as_mut_ptr(),
        (t_info(tidpivot.as_ptr()) & !INDEX_SIZE_MASK) | newsize as u16,
    );
    bt_tuple_set_natts(tidpivot.as_mut_ptr(), nkeyatts as u16, true);
    let heaptid_off = newsize - core::mem::size_of::<ItemPointerData>();
    let pivotheaptid = bt_tuple_get_max_heap_tid(lastleft);
    tidpivot
        .as_mut_ptr()
        .add(heaptid_off)
        .cast::<ItemPointerData>()
        .write_unaligned(pivotheaptid);

    debug_assert!(
        ItemPointerCompare(&bt_tuple_get_max_heap_tid(lastleft),
            &bt_tuple_get_heap_tid(firstright).expect("non-pivot")) < 0
    );
    Ok(tidpivot)
}

/// _bt_keep_natts: authoritative (opclass-comparator) variant.
///
/// # Safety
/// As [`bt_truncate`].
unsafe fn bt_keep_natts(
    rel: &Relation<'_>,
    lastleft: ITup,
    firstright: ITup,
    itup_key: &mut BtScanInsert,
    frame: &mut OrderProcFrame,
) -> PgResult<usize> {
    let nkeyatts = rel.indnkeyatts() as usize;
    let tupdesc: &TupleDescData<'_> = &rel.rd_att;

    if !itup_key.heapkeyspace {
        return Ok(nkeyatts);
    }

    let mut keepnatts = 1usize;
    for attnum in 1..=nkeyatts {
        let mut null1 = false;
        let mut null2 = false;
        let d1 = index_getattr(lastleft, attnum as AttrNumber, tupdesc, &mut null1);
        let d2 = index_getattr(firstright, attnum as AttrNumber, tupdesc, &mut null2);

        if null1 != null2 {
            break;
        }
        if !null1 {
            let key = &mut itup_key.keys_mut()[attnum - 1];
            if frame.cmp(key, d1, d2)? != 0 {
                break;
            }
        }
        keepnatts += 1;
    }

    debug_assert!(
        !itup_key.allequalimage || keepnatts == bt_keep_natts_fast(rel, lastleft, firstright) as usize
    );
    Ok(keepnatts)
}

/// _bt_check_third_page: 1/3-of-a-page limit ereport.
///
/// # Safety
/// `newtup` per [`bt_checkkeys`].
#[cold]
#[inline(never)]
pub unsafe fn bt_check_third_page(
    rel: &Relation<'_>,
    heap: &Relation<'_>,
    needheaptidspace: bool,
    page: &PageRef<'_>,
    newtup: ITup,
) -> PgResult<()> {
    use ::types_nbtree::{BTMaxItemSize, BTMaxItemSizeNoHeapTid, P_ISLEAF, BTREE_NOVAC_VERSION, BTREE_VERSION};

    let itemsz = crate::itup::maxalign(crate::itup::index_tuple_size(newtup));
    if itemsz <= BTMaxItemSize {
        return Ok(());
    }
    if !needheaptidspace && itemsz <= BTMaxItemSizeNoHeapTid {
        return Ok(());
    }

    let opaque = page_opaque(page);
    if !P_ISLEAF(&opaque) {
        return Err(Box::new(::types_error::PgError::error(format!(
            "cannot insert oversized tuple of size {itemsz} on internal page of index \"{}\"",
            rel.name()
        ))));
    }

    let tid = crate::itup::bt_tuple_get_heap_tid(newtup).expect("non-pivot new tuple");
    let (version, max) = if needheaptidspace {
        (BTREE_VERSION, BTMaxItemSize)
    } else {
        (BTREE_NOVAC_VERSION, BTMaxItemSizeNoHeapTid)
    };
    Err(Box::new(
        ::types_error::PgError::error(format!(
            "index row size {itemsz} exceeds btree version {version} maximum {max} for index \"{}\"",
            rel.name()
        ))
        .with_sqlstate(::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .with_detail(format!(
            "Index row references tuple ({},{}) in relation \"{}\".",
            ::types_tuple::itemptr::ItemPointerGetBlockNumberNoCheck(&tid),
            tid.ip_posid,
            heap.name()
        ))
        .with_hint(
            "Values larger than 1/3 of a buffer page cannot be indexed.\n\
             Consider a function index of an MD5 hash of the value, \
             or use full text indexing.",
        ),
    ))
}

// btvacinfo: cross-backend shared state, a process static guarded by a Mutex
// standing in for C's shmem area + BtreeVacuumLock. Keyed by (dbOid, relId)
// per C's LockRelId. Bare Vec: shared registry outside any mcx, cold path.
struct BtVacInfo {
    cycle_ctr: ::types_nbtree::BTCycleId,
    vacuums: Vec<(::types_core::Oid, ::types_core::Oid, ::types_nbtree::BTCycleId)>,
}

pgsync::process_global! {
    static BTVACINFO: pgsync::Mutex<BtVacInfo> = pgsync::Mutex::new(BtVacInfo {
        cycle_ctr: 0,
        vacuums: Vec::new(),
    });
}

pub(crate) fn vac_key(rel: &Relation<'_>) -> (::types_core::Oid, ::types_core::Oid) {
    (rel.rd_locator.get().dbOid, rel.rd_id)
}

/// _bt_vacuum_cycleid: 0 when no vacuum is active on this index.
pub(crate) fn bt_vacuum_cycleid(rel: &Relation<'_>) -> ::types_nbtree::BTCycleId {
    let info = BTVACINFO.lock().unwrap();
    let key = vac_key(rel);
    info.vacuums
        .iter()
        .find(|v| (v.0, v.1) == key)
        .map_or(0, |v| v.2)
}

/// _bt_start_vacuum. Caller pairs with bt_end_vacuum even on error exit.
pub(crate) fn bt_start_vacuum(rel: &Relation<'_>) -> PgResult<::types_nbtree::BTCycleId> {
    let mut info = BTVACINFO.lock().unwrap();
    if info.cycle_ctr == 0 {
        // C seeds from time() at shmem init; any nonzero start works.
        // DST P2 (contract §1.2): seed from pg_clock::wall_secs().
        info.cycle_ctr = (pg_clock::wall_secs().max(1) as ::types_nbtree::BTCycleId) | 1;
    }
    info.cycle_ctr = info.cycle_ctr.wrapping_add(1);
    if info.cycle_ctr == 0 || info.cycle_ctr > ::types_nbtree::MAX_BT_CYCLE_ID {
        info.cycle_ctr = 1;
    }
    let result = info.cycle_ctr;
    let key = vac_key(rel);
    if info.vacuums.iter().any(|v| (v.0, v.1) == key) {
        return Err(Box::new(::types_error::PgError::error(format!(
            "multiple active vacuums for index \"{}\"",
            rel.name()
        ))));
    }
    info.vacuums.push((key.0, key.1, result));
    Ok(result)
}

/// _bt_end_vacuum; silent when no entry exists, as C.
pub(crate) fn bt_end_vacuum(rel: &Relation<'_>) {
    bt_end_vacuum_key(vac_key(rel));
}

/// bt_end_vacuum by (db, relid) key: the chunked scan's abort-path release
/// (Q2) runs from a Drop with no Relation handle in scope.
pub(crate) fn bt_end_vacuum_key(key: (::types_core::Oid, ::types_core::Oid)) {
    let mut info = BTVACINFO.lock().unwrap();
    if let Some(i) = info.vacuums.iter().position(|v| (v.0, v.1) == key) {
        info.vacuums.swap_remove(i);
    }
}

/// _bt_mkscankey; `itup: None` is the utility-statement arm. C divergence:
/// Keys past tupnatts are SK_ISNULL with unset arguments, per C.
pub fn bt_mkscankey(rel: &Relation<'_>, itup: Option<ITup>) -> PgResult<BtScanInsert> {
    let tupdesc: &TupleDescData<'_> = &rel.rd_att;
    let indnkeyatts = rel.indnkeyatts();

    let mut key = BtScanInsert::new();
    // SAFETY: caller guarantees `itup` points at a live index tuple.
    let tupnatts = match itup {
        Some(itup) => {
            let (heapkeyspace, allequalimage) = crate::page::bt_metaversion(rel)?;
            key.heapkeyspace = heapkeyspace;
            key.allequalimage = allequalimage;
            unsafe { bt_tuple_get_natts(itup, rel.indnatts()) }
        }
        None => 0,
    };
    debug_assert!(tupnatts <= rel.indnatts());

    key.scantid = match itup {
        Some(itup) if key.heapkeyspace => unsafe { bt_tuple_get_heap_tid(itup) },
        _ => None,
    };

    for i in 0..indnkeyatts as usize {
        let sk_func = crate::search::order_procinfo(rel, i + 1)?;
        let mut is_null = true;
        // Past tupnatts: C's SK_ISNULL key with an unset argument — the
        // utility arm (nbtsort) reads sk_func/sk_collation off these.
        let arg = if (i as i32) < tupnatts as i32 {
            // SAFETY: i < tupnatts implies itup is Some and i+1 <= tupnatts.
            unsafe {
                index_getattr(
                    itup.expect("tupnatts > 0 implies a tuple"),
                    (i + 1) as AttrNumber,
                    tupdesc,
                    &mut is_null,
                )
            }
        } else {
            Datum::null()
        };
        if is_null {
            key.anynullkeys = true;
        }
        let null_flag = if is_null { SK_ISNULL } else { 0 };
        key.push(ScanKeyData {
            sk_flags: null_flag | ((rel.rd_indoption[i] as i32) << SK_BT_INDOPTION_SHIFT),
            sk_attno: (i + 1) as AttrNumber,
            sk_strategy: InvalidStrategy,
            sk_subtype: 0,
            sk_collation: rel.rd_indcollation[i],
            sk_func,
            sk_argument: arg,
        });
    }

    if rel.rd_index.as_ref().is_some_and(|i| i.indnullsnotdistinct) {
        key.anynullkeys = false;
    }
    // C: keysz = Min(indnkeyatts, tupnatts) — a truncated pivot compares only
    // its untruncated prefix; the remaining entries stay initialized for the
    // utility arms.
    key.set_keysz((indnkeyatts as usize).min(tupnatts as usize));
    Ok(key)
}

/// _bt_check_natts (nbtutils.c).
pub fn bt_check_natts(
    rel: &Relation<'_>,
    heapkeyspace: bool,
    page: &PageRef<'_>,
    offnum: OffsetNumber,
) -> bool {
    let natts = rel.indnatts();
    let nkeyatts = rel.indnkeyatts();
    let opaque = page_opaque(page);

    // Deleted/half-dead pages have dummy high keys; cannot be tested reliably.
    if ::types_nbtree::P_IGNORE(&opaque) {
        return true;
    }

    debug_assert!(offnum >= 1 && offnum <= page.max_offset_number());

    // SAFETY: offnum is a live offset on this page image (caller contract).
    let (itup, tupnatts) = unsafe {
        let itup = page_item(page, page.item_id_unchecked(offnum));
        (itup, bt_tuple_get_natts(itup, natts))
    };
    // SAFETY: itup points at a live index tuple on the page image.
    unsafe {
        if !heapkeyspace && bt_tuple_is_posting(itup) {
            return false;
        }
        if bt_tuple_is_posting(itup)
            && (crate::itup::t_tid(itup).ip_posid & ::types_nbtree::BT_PIVOT_HEAP_TID_ATTR) != 0
        {
            return false;
        }
        if natts != nkeyatts && bt_tuple_is_posting(itup) {
            return false;
        }

        if ::types_nbtree::P_ISLEAF(&opaque) {
            if offnum >= P_FIRSTDATAKEY(&opaque) {
                if bt_tuple_is_pivot(itup) {
                    return false;
                }
                return tupnatts == natts;
            }
            debug_assert!(!::types_nbtree::P_RIGHTMOST(&opaque));
            if !heapkeyspace {
                return tupnatts == nkeyatts;
            }
        } else if offnum == P_FIRSTDATAKEY(&opaque) {
            if heapkeyspace {
                return tupnatts == 0;
            }
            // Pre-v11 negative-infinity tuples had P_HIKEY as their offset.
            return tupnatts == 0 || crate::itup::t_tid(itup).ip_posid == ::types_nbtree::P_HIKEY;
        } else if !heapkeyspace {
            return tupnatts == nkeyatts;
        }

        debug_assert!(heapkeyspace);
        if !bt_tuple_is_pivot(itup) {
            return false;
        }
        if bt_tuple_is_posting(itup) {
            return false;
        }
        if bt_tuple_get_heap_tid(itup).is_some() && tupnatts != nkeyatts {
            return false;
        }
        tupnatts > 0 && tupnatts <= nkeyatts
    }
}

/// _bt_allequalimage (nbtutils.c), sans the DEBUG1 message.
pub fn bt_allequalimage(rel: &Relation<'_>) -> PgResult<bool> {
    if rel.indnatts() != rel.indnkeyatts() {
        return Ok(false);
    }
    for i in 0..rel.indnkeyatts() as usize {
        let opfamily = rel.rd_opfamily[i];
        let opcintype = rel.rd_opcintype[i];
        let collation = rel.rd_indcollation[i];
        let equalimageproc = lsyscache::get_opfamily_proc(
            opfamily,
            opcintype,
            opcintype,
            ::types_nbtree::BTEQUALIMAGE_PROC as i16,
        )?;
        if equalimageproc == ::types_core::InvalidOid {
            return Ok(false);
        }
        let mut finfo = fmgr_core::fmgr_info(equalimageproc)?;
        let mut fcinfo = ::types_fmgr::LocalFcinfo::<1>::fresh(collation);
        fcinfo.set_arg(0, Datum::from_oid(opcintype));
        if !finfo.invoke(&mut fcinfo)?.as_bool() {
            return Ok(false);
        }
    }
    Ok(true)
}
