//! Parallel btree scan coordination (nbtree.c _bt_parallel_* family).

use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::{BlockNumber, InvalidBlockNumber};
use ::types_error::PgResult;
use ::types_nbtree::{BTScanOpaqueData, BTScanPosInvalidate, BTScanPosIsValid, P_NONE};
use ::types_relscan::{
    BTParallelScanShared, BtParallelArrayElem, BtParallelScanState, BtParallelSkipArg, BtPsState,
};
use ::types_scan::scankey::{
    SK_BT_MAXVAL, SK_BT_MINVAL, SK_BT_SKIP, SK_ISNULL, SK_SEARCHNULL,
};
use ::types_tuple::varatt::varsize_any;

fn lock(shared: &BTParallelScanShared) -> pgsync::MutexGuard<'_, BtParallelScanState> {
    shared.state.lock().unwrap_or_else(|e| e.into_inner())
}

/// btparallelrescan.
pub fn btparallelrescan(shared: &BTParallelScanShared) {
    let mut g = lock(shared);
    g.next_scan_page = InvalidBlockNumber;
    g.last_curr_page = InvalidBlockNumber;
    g.page_status = BtPsState::NotInitialized;
}

/// _bt_parallel_seize: `(status, next_scan_page, last_curr_page)`.
// Parallel-only; outlined so fat LTO keeps it out of the serial descent.
#[cold]
#[inline(never)]
pub(crate) fn bt_parallel_seize(
    so: &mut BTScanOpaqueData<'_>,
    shared: &BTParallelScanShared,
    first: bool,
) -> PgResult<(bool, BlockNumber, BlockNumber)> {
    let mut status = true;
    let mut endscan = false;
    let mut next_scan_page = InvalidBlockNumber;
    let mut last_curr_page = InvalidBlockNumber;

    // Prime currPos so the next _bt_readnextpage treats this backend like a
    // serial backend stepping from last_curr_page to next_scan_page.
    BTScanPosInvalidate(&mut so.currPos);
    so.currPos.moreLeft = true;
    so.currPos.moreRight = true;

    if first {
        so.needPrimScan = false;
        so.scanBehind = false;
        so.oppositeDirCheck = false;
    } else if so.needPrimScan {
        return Ok((false, next_scan_page, last_curr_page));
    }

    let mut g = lock(shared);
    loop {
        match g.page_status {
            BtPsState::Done => {
                status = false;
            }
            BtPsState::Idle if g.next_scan_page == P_NONE => {
                status = false;
                endscan = true;
            }
            BtPsState::NeedPrimscan => {
                debug_assert!(so.numArrayKeys > 0);
                if first {
                    g.page_status = BtPsState::Advancing;
                    restore_arrays(&g.arr_elems, so)?;
                } else {
                    status = false;
                }
                so.needPrimScan = true;
                so.scanBehind = false;
                so.oppositeDirCheck = false;
            }
            BtPsState::Advancing => {
                // permit-s4 row 2: eventcount discipline — capture the
                // epoch UNDER the state lock (any later out-of-Advancing
                // transition bumps it strictly after its own state change),
                // release the lock, park. Spurious/early returns are safe:
                // the loop re-reads page_status under the re-taken lock.
                let seen = shared.lot.epoch();
                drop(g);
                shared.lot.park(seen);
                g = lock(shared);
                continue;
            }
            _ => {
                g.page_status = BtPsState::Advancing;
                debug_assert!(g.next_scan_page != P_NONE);
                next_scan_page = g.next_scan_page;
                last_curr_page = g.last_curr_page;
            }
        }
        break;
    }
    drop(g);

    if endscan {
        bt_parallel_done(so, Some(shared));
    }

    Ok((status, next_scan_page, last_curr_page))
}

/// _bt_parallel_release.
// Parallel-only; outlined so fat LTO keeps it out of the serial descent.
#[cold]
#[inline(never)]
pub(crate) fn bt_parallel_release(
    shared: &BTParallelScanShared,
    next_scan_page: BlockNumber,
    curr_page: BlockNumber,
) {
    debug_assert!(next_scan_page != InvalidBlockNumber);
    {
        let mut g = lock(shared);
        g.next_scan_page = next_scan_page;
        g.last_curr_page = curr_page;
        g.page_status = BtPsState::Idle;
    }
    // Wake EVERY parked seizer (row-2 v1: no notify_one — see the
    // BTParallelScanShared doc); losers re-observe Advancing and re-park.
    shared.lot.wake_all();
}

/// _bt_parallel_done. The serial (None) fast path stays inlinable; the
/// shared-state arm is outlined so fat LTO keeps it out of the serial
/// descent.
#[inline]
pub(crate) fn bt_parallel_done(
    so: &BTScanOpaqueData<'_>,
    parallel: Option<&BTParallelScanShared>,
) {
    debug_assert!(!BTScanPosIsValid(&so.currPos));

    let Some(shared) = parallel else { return };
    bt_parallel_done_shared(so, shared);
}

#[cold]
#[inline(never)]
fn bt_parallel_done_shared(so: &BTScanOpaqueData<'_>, shared: &BTParallelScanShared) {
    if so.needPrimScan {
        return;
    }

    let status_changed = {
        let mut g = lock(shared);
        debug_assert!(g.page_status != BtPsState::NeedPrimscan);
        if g.page_status != BtPsState::Done {
            g.page_status = BtPsState::Done;
            true
        } else {
            false
        }
    };
    if status_changed {
        shared.lot.wake_all();
    }
}

/// _bt_parallel_primscan_schedule: no-op if the shared state was seized since
/// this backend last advanced the scan.
// Parallel-only; outlined so fat LTO keeps it out of the serial descent.
#[cold]
#[inline(never)]
pub(crate) fn bt_parallel_primscan_schedule(
    so: &BTScanOpaqueData<'_>,
    shared: &BTParallelScanShared,
    curr_page: BlockNumber,
) {
    debug_assert!(so.numArrayKeys > 0);

    let mut g = lock(shared);
    if g.last_curr_page == curr_page && g.page_status == BtPsState::Idle {
        g.next_scan_page = InvalidBlockNumber;
        g.last_curr_page = InvalidBlockNumber;
        g.page_status = BtPsState::NeedPrimscan;
        g.arr_elems = serialize_arrays(so);
    }
}

// _bt_parallel_serialize_arrays; caller holds the state lock.
fn serialize_arrays(so: &BTScanOpaqueData<'_>) -> Vec<BtParallelArrayElem> {
    let mut out = Vec::with_capacity(so.numArrayKeys as usize);
    for array in so.arrayKeys.iter() {
        let skey = &so.keyData[array.scan_key as usize];
        if array.num_elems != -1 {
            debug_assert!(skey.sk_flags & SK_BT_SKIP == 0);
            out.push(BtParallelArrayElem::Saop { cur_elem: array.cur_elem });
            continue;
        }
        debug_assert!(skey.sk_flags & SK_BT_SKIP != 0);
        let arg = if skey.sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL) != 0 {
            debug_assert!(skey.sk_argument.as_usize() == 0);
            None
        } else if skey.sk_flags & SK_ISNULL != 0 {
            debug_assert!(skey.sk_flags & SK_SEARCHNULL != 0);
            None
        } else if array.attbyval {
            Some(BtParallelSkipArg::Byval(skey.sk_argument))
        } else {
            // SAFETY: by-ref skip elements point at live scan-arena bytes of
            // the attribute's declared shape (see skip_datum_copy).
            let bytes = unsafe {
                let p = skey.sk_argument.as_usize() as *const u8;
                let len = match array.attlen {
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
                core::slice::from_raw_parts(p, len).to_vec()
            };
            Some(BtParallelSkipArg::Byref(bytes))
        };
        out.push(BtParallelArrayElem::Skip { flags: skey.sk_flags, arg });
    }
    out
}

// _bt_parallel_restore_arrays; caller holds the state lock. C divergence: the
// superseded by-ref sk_argument stays in the scan-lifetime arena (no pfree).
fn restore_arrays(
    elems: &[BtParallelArrayElem],
    so: &mut BTScanOpaqueData<'_>,
) -> PgResult<()> {
    let mcx: Mcx<'_> = *so.keyData.allocator();
    let BTScanOpaqueData { keyData, arrayKeys, .. } = so;
    debug_assert!(elems.len() == arrayKeys.len());
    for (array, elem) in arrayKeys.iter_mut().zip(elems.iter()) {
        let skey = &mut keyData[array.scan_key as usize];
        match elem {
            BtParallelArrayElem::Saop { cur_elem } => {
                debug_assert!(skey.sk_flags & SK_BT_SKIP == 0);
                array.cur_elem = *cur_elem;
                skey.sk_argument = array.elem_values[*cur_elem as usize];
            }
            BtParallelArrayElem::Skip { flags, arg } => {
                skey.sk_argument = Datum::from_usize(0);
                skey.sk_flags = *flags;
                debug_assert!(skey.sk_flags & SK_BT_SKIP != 0);
                match arg {
                    None => {
                        debug_assert!(
                            skey.sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL) != 0
                                || (skey.sk_flags & SK_SEARCHNULL != 0
                                    && skey.sk_flags & SK_ISNULL != 0)
                        );
                    }
                    Some(BtParallelSkipArg::Byval(d)) => skey.sk_argument = *d,
                    Some(BtParallelSkipArg::Byref(bytes)) => {
                        let mut v: ::mcx::PgVec<'_, u8> =
                            ::mcx::vec_with_capacity_in(mcx, bytes.len())?;
                        ::mcx::vec_append_bytes(&mut v, bytes)?;
                        skey.sk_argument = Datum::from_usize(v.leak().as_ptr() as usize);
                    }
                }
            }
        }
    }
    Ok(())
}
