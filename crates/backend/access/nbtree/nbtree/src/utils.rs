//! nbtutils.c, READ side: per-tuple qual checking (_bt_checkkeys /
//! _bt_check_compare), the startikey page-level precheck, and _bt_killitems.
//! Array-key advancement/row compares are phase 2 (preprocessing rejects both,
//! so the branches here are unreachable, not silently wrong).

use ::bufmgr_seams as bufmgr;
use ::datum::Datum;
use ::types_core::{OffsetNumber, XLogRecPtr};
use ::types_error::PgResult;
use ::types_nbtree::{
    BTScanOpaqueData, BTScanPosIsPinned, BTScanPosIsValid, BTPageOpaqueData, BTP_HAS_GARBAGE,
    BT_READ, P_FIRSTDATAKEY,
};
use ::types_rel::Relation;
use ::types_scan::scankey::{
    BTEqualStrategyNumber, SK_BT_MAXVAL, SK_BT_MINVAL, SK_BT_NEXT, SK_BT_PRIOR,
    SK_BT_REQBKWD, SK_BT_REQFWD, SK_BT_SKIP, SK_ISNULL, SK_ROW_HEADER, SK_SEARCHARRAY,
    SK_SEARCHNULL, SK_BT_NULLS_FIRST,
};
use ::types_scan::sdir::{ScanDirection, ScanDirectionIsBackward, ScanDirectionIsForward};
use ::types_storage::bufpage::{ItemIdData, PageRef, SizeOfPageHeaderData};
use ::types_tuple::itemptr::ItemPointerEquals;
use ::types_tuple::varatt::varsize_any;
use ::types_tuple::TupleDescData;

use crate::fcframe::OrderProcFrame;
use crate::itup::{
    bt_tuple_get_nposting, bt_tuple_get_posting_n, bt_tuple_is_pivot, bt_tuple_is_posting,
    index_getattr, ITup,
};
use crate::page::{bt_getbuf, bt_relbuf, page_item, page_opaque, page_special_off};
use crate::search::BtReadPageState;
use crate::unported_phase2;

/// _bt_checkkeys. `tupnatts` is the tuple's own attribute count (may be less
/// than the key count for a truncated high key).
///
/// # Safety
/// `tuple` points at a live index tuple on a page pinned+locked by caller.
pub(crate) unsafe fn bt_checkkeys(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    pstate: &mut BtReadPageState<'_>,
    array_keys: bool,
    tuple: ITup,
    tupnatts: i32,
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let dir = so.currPos.dir;
    let mut ikey = pstate.startikey;
    debug_assert!(!so.needPrimScan && !so.scanBehind && !so.oppositeDirCheck);
    debug_assert!(array_keys || so.numArrayKeys == 0);

    let res = bt_check_compare(
        rel,
        so,
        dir,
        tuple,
        tupnatts,
        array_keys,
        pstate.forcenonrequired,
        &mut pstate.continuescan,
        &mut ikey,
        frame,
    )?;

    if !array_keys || pstate.continuescan {
        return Ok(res);
    }
    unported_phase2("_bt_advance_array_keys (SAOP/skip-scan lane)")
}

/// _bt_check_compare, scalar arm. `advancenonrequired`/array advancement and
/// row compares route to phase-2 panics.
///
/// # Safety
/// As [`bt_checkkeys`].
unsafe fn bt_check_compare(
    rel: &Relation<'_>,
    so: &mut BTScanOpaqueData<'_>,
    dir: ScanDirection,
    tuple: ITup,
    tupnatts: i32,
    advancenonrequired: bool,
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
            unported_phase2("skip-array sentinel keys in _bt_check_compare");
        }

        if key.sk_flags & SK_ROW_HEADER != 0 {
            unported_phase2("_bt_check_rowcompare (row comparison lane)");
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
            }
            return Ok(false);
        }

        if is_null {
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
            } else if advancenonrequired
                && key.sk_strategy == BTEqualStrategyNumber
                && key.sk_flags & SK_SEARCHARRAY != 0
            {
                unported_phase2("_bt_advance_array_keys (non-required array)");
            }
            return Ok(false);
        }

        *ikey += 1;
    }

    Ok(true)
}

/// datum_image_eq (datum.c) trimmed to index-tuple callers: no external toast,
/// no expanded datums on-page.
///
/// # Safety
/// By-ref datums point at live in-page values of the attribute's type shape.
unsafe fn datum_image_eq(a: Datum, b: Datum, attbyval: bool, attlen: i16) -> bool {
    if attbyval {
        return a.as_usize() == b.as_usize();
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
    // cstring
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

/// _bt_keep_natts_fast: first attribute (1-based) whose value differs between
/// the two tuples, capped at keysz+1.
///
/// # Safety
/// As [`bt_checkkeys`] for both tuples.
pub(crate) unsafe fn bt_keep_natts_fast(
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
    debug_assert!(so.numArrayKeys == 0, "array lane is phase 2");

    let firsttup = page_item(&pstate.page, pstate.page.item_id(pstate.minoff));
    let lasttup = page_item(&pstate.page, pstate.page.item_id(pstate.maxoff));

    let firstchangingattnum = bt_keep_natts_fast(rel, firsttup, lasttup);

    let mut startikey: i32 = 0;
    while startikey < so.numberOfKeys {
        let key = &mut so.keyData[startikey as usize];

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

        debug_assert!(key.sk_flags & SK_SEARCHARRAY == 0, "array lane is phase 2");

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

    // forcenonrequired only arises with arrays (phase 2).
    pstate.forcenonrequired = false;
    pstate.startikey = startikey;
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
