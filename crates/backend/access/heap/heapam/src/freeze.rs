use ::tableam_vocab::{LockTupleMode, VacuumCutoffs};
use ::types_core::xact::{
    FrozenTransactionId, MultiXactIdPrecedes, MultiXactIdPrecedesOrEquals, TransactionIdIsNormal,
    TransactionIdPrecedes, TransactionIdPrecedesOrEquals,
};
use ::types_core::{
    Buffer, InvalidTransactionId, MultiXactId, OffsetNumber, TransactionId, TransactionIdIsValid,
};
use ::types_error::{PgResult, ERRCODE_DATA_CORRUPTED};
use ::types_storage::bufpage::PageRef;
use ::types_storage::multixact::{ISUPDATE_from_mxstatus, MultiXactMember, MultiXactStatus};
use ::types_tuple::htup::{HEAP_LOCKED_UPGRADED, HEAP_XMAX_IS_LOCKED_ONLY};
use ::types_tuple::{
    HeapTupleHeaderData, HEAP_HOT_UPDATED, HEAP_KEYS_UPDATED, HEAP_MOVED, HEAP_MOVED_OFF,
    HEAP_XMAX_BITS, HEAP_XMAX_COMMITTED, HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_INVALID,
    HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY, HEAP_XMAX_SHR_LOCK,
    HEAP_XMIN_FROZEN,
};

pub const XLH_FREEZE_XVAC: u8 = 0x02;
pub const XLH_INVALID_XVAC: u8 = 0x04;

pub const HEAP_FREEZE_CHECK_XMIN_COMMITTED: u8 = 0x01;
pub const HEAP_FREEZE_CHECK_XMAX_ABORTED: u8 = 0x02;

const InvalidMultiXactId: MultiXactId = 0;

fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi != InvalidMultiXactId
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HeapTupleFreeze {
    pub xmax: TransactionId,
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub frzflags: u8,
    pub checkflags: u8,
    pub offset: OffsetNumber,
}

#[derive(Clone, Copy, Debug)]
pub struct HeapPageFreeze {
    pub freeze_required: bool,
    pub FreezePageRelfrozenXid: TransactionId,
    pub FreezePageRelminMxid: MultiXactId,
    pub NoFreezePageRelfrozenXid: TransactionId,
    pub NoFreezePageRelminMxid: MultiXactId,
}

const FRM_NOOP: u16 = 0x0001;
const FRM_INVALIDATE_XMAX: u16 = 0x0002;
const FRM_RETURN_IS_XID: u16 = 0x0004;
const FRM_RETURN_IS_MULTI: u16 = 0x0008;
const FRM_MARK_COMMITTED: u16 = 0x0010;

// C palloc's the member arrays at exactly the length the multixact reports --
// `palloc(length * sizeof(MultiXactMember))` in GetMultiXactIdMembers
// (multixact.c:1569) and `palloc(sizeof(MultiXactMember) * nmembers)` in
// FreezeMultiXactId (heapam.c:6881) -- and imposes NO cap at any level. There is
// no MaxMultiXactMembers in PostgreSQL: MULTIXACT_MEMBERS_PER_PAGE is a page
// granularity, not a per-multixact limit, and one multixact's members are a
// contiguous run in the global 32-bit offset space that freely spans pages.
//
// The member count is bounded only by how many distinct in-progress lockers
// accumulate on one tuple, and MultiXactIdExpand retains every member whose xid
// is still in progress. That is reachable WITHOUT concurrency: a prepared
// transaction keeps a dummy PGPROC in the procarray, so max_prepared_transactions
// sequential rounds of (BEGIN; SELECT ... FOR KEY SHARE; PREPARE TRANSACTION)
// grow one multixact past any fixed cap with only ever one live backend thread.
// A cap enforced by a release `assert!` was a ported-in limit C never had, and a
// reachable release assertion is a crash.
//
// The scratch is a reset-per-call bump context standing in for C's palloc into
// CurrentMemoryContext: there is no per-tuple mcx reachable this deep in the
// freeze path, the same reason KEY_TEST_SCRATCH exists in this crate's lib.rs.
// Capacity is retained across calls, so this is not a context per call.
std::thread_local! {
    static FREEZE_MEMBER_SCRATCH: core::cell::RefCell<::mcx::MemoryContext> =
        core::cell::RefCell::new(::mcx::MemoryContext::new_bump("freeze multixact members"));
}

#[cold]
#[inline(never)]
fn data_corrupted(msg: String) -> PgResult<()> {
    let mut e = ::types_error::PgError::error(msg);
    e.sqlstate = ERRCODE_DATA_CORRUPTED;
    Err(Box::new(e))
}

fn FreezeMultiXactId(
    multi: MultiXactId,
    t_infomask: u16,
    cutoffs: &VacuumCutoffs,
    flags: &mut u16,
    pagefrz: &mut HeapPageFreeze,
) -> PgResult<TransactionId> {
    *flags = 0;
    debug_assert!(t_infomask & HEAP_XMAX_IS_MULTI != 0);

    if !MultiXactIdIsValid(multi) || HEAP_LOCKED_UPGRADED(t_infomask) {
        *flags |= FRM_INVALIDATE_XMAX;
        pagefrz.freeze_required = true;
        return Ok(InvalidTransactionId);
    } else if MultiXactIdPrecedes(multi, cutoffs.relminmxid) {
        data_corrupted(
            format!(
                "found multixact {multi} from before relminmxid {}",
                cutoffs.relminmxid
            ),
        )?;
    } else if MultiXactIdPrecedes(multi, cutoffs.OldestMxact) {
        if multixact_seams::multi_xact_id_is_running::call(
            multi,
            HEAP_XMAX_IS_LOCKED_ONLY(t_infomask),
        )? {
            data_corrupted(
                format!(
                    "multixact {multi} from before multi freeze cutoff {} found to be still running",
                    cutoffs.OldestMxact
                ),
            )?;
        }

        if HEAP_XMAX_IS_LOCKED_ONLY(t_infomask) {
            *flags |= FRM_INVALIDATE_XMAX;
            pagefrz.freeze_required = true;
            return Ok(InvalidTransactionId);
        }

        let update_xact = crate::MultiXactIdGetUpdateXid(multi, t_infomask)?;
        if TransactionIdPrecedes(update_xact, cutoffs.relfrozenxid) {
            data_corrupted(
                format!(
                    "multixact {multi} contains update XID {update_xact} from before relfrozenxid {}",
                    cutoffs.relfrozenxid
                ),
            )?;
        } else if TransactionIdPrecedes(update_xact, cutoffs.OldestXmin) {
            if transam_seams::transaction_id_did_commit::call(update_xact)? {
                data_corrupted(
                    format!(
                        "multixact {multi} contains committed update XID {update_xact} from before removable cutoff {}",
                        cutoffs.OldestXmin
                    ),
                )?;
            }
            *flags |= FRM_INVALIDATE_XMAX;
            pagefrz.freeze_required = true;
            return Ok(InvalidTransactionId);
        }

        *flags |= FRM_RETURN_IS_XID;
        pagefrz.freeze_required = true;
        return Ok(update_xact);
    }

    FREEZE_MEMBER_SCRATCH.with(|cell| {
        let mut ctx = cell
            .try_borrow_mut()
            .expect("FreezeMultiXactId member scratch is not re-entrant");
        ctx.reset();
        let scratch = ctx.mcx();

        let mut members: ::mcx::PgVec<'_, MultiXactMember> = ::mcx::PgVec::new_in(scratch);
        let nres = multixact_seams::get_multi_xact_id_members::call(
            multi,
            false,
            HEAP_XMAX_IS_LOCKED_ONLY(t_infomask),
            &mut |ms| members.extend_from_slice(ms),
        )?;
        if nres <= 0 || members.is_empty() {
            *flags |= FRM_INVALIDATE_XMAX;
            pagefrz.freeze_required = true;
            return Ok(InvalidTransactionId);
        }
        // heapam.c:6881 sizes the replacement array off nmembers; it can never
        // exceed it.
        let mut newmembers: ::mcx::PgVec<'_, MultiXactMember> =
            ::mcx::PgVec::with_capacity_in(members.len(), scratch);
        freeze_multixact_replace(&members, &mut newmembers, multi, cutoffs, flags, pagefrz)
    })
}

// The replacement half of FreezeMultiXactId (heapam.c:6860-6980), split out so
// the member scratch's borrow stays in the caller and this body keeps C's
// structure line for line.
fn freeze_multixact_replace(
    members: &[MultiXactMember],
    newmembers: &mut ::mcx::PgVec<'_, MultiXactMember>,
    multi: MultiXactId,
    cutoffs: &VacuumCutoffs,
    flags: &mut u16,
    pagefrz: &mut HeapPageFreeze,
) -> PgResult<TransactionId> {
    let mut need_replace = false;
    let mut freeze_page_relfrozenxid = pagefrz.FreezePageRelfrozenXid;
    for m in members {
        let xid = m.xid;
        debug_assert!(!TransactionIdPrecedes(xid, cutoffs.relfrozenxid));
        if TransactionIdPrecedes(xid, cutoffs.FreezeLimit) {
            need_replace = true;
            break;
        }
        if TransactionIdPrecedes(xid, freeze_page_relfrozenxid) {
            freeze_page_relfrozenxid = xid;
        }
    }
    if !need_replace {
        need_replace = MultiXactIdPrecedes(multi, cutoffs.MultiXactCutoff);
    }
    if !need_replace {
        *flags |= FRM_NOOP;
        pagefrz.FreezePageRelfrozenXid = freeze_page_relfrozenxid;
        if MultiXactIdPrecedes(multi, pagefrz.FreezePageRelminMxid) {
            pagefrz.FreezePageRelminMxid = multi;
        }
        return Ok(multi);
    }

    let mut has_lockers = false;
    let mut update_xid = InvalidTransactionId;
    let mut update_committed = false;

    for m in members {
        let xid = m.xid;
        debug_assert!(!TransactionIdPrecedes(xid, cutoffs.relfrozenxid));

        if !ISUPDATE_from_mxstatus(m.status) {
            if xact_seams::transaction_id_is_current_transaction_id::call(xid)
                || procarray_seams::transaction_id_is_in_progress::call(xid)?
            {
                if TransactionIdPrecedes(xid, cutoffs.OldestXmin) {
                    data_corrupted(
                        format!(
                            "multixact {multi} contains running locker XID {xid} from before removable cutoff {}",
                            cutoffs.OldestXmin
                        ),
                    )?;
                }
                newmembers.push(*m);
                has_lockers = true;
            }
            continue;
        }

        if TransactionIdIsValid(update_xid) {
            data_corrupted(
                format!("multixact {multi} has two or more updating members"),
            )?;
        }

        // In-progress must be tested before did-commit (heapam_visibility.c races).
        if xact_seams::transaction_id_is_current_transaction_id::call(xid)
            || procarray_seams::transaction_id_is_in_progress::call(xid)?
        {
            update_xid = xid;
        } else if transam_seams::transaction_id_did_commit::call(xid)? {
            update_committed = true;
            update_xid = xid;
        } else {
            continue; // aborted or crashed
        }

        if TransactionIdPrecedes(xid, cutoffs.OldestXmin) {
            data_corrupted(
                format!(
                    "multixact {multi} contains committed update XID {xid} from before removable cutoff {}",
                    cutoffs.OldestXmin
                ),
            )?;
        }
        newmembers.push(*m);
    }

    let newxmax;
    if newmembers.is_empty() {
        *flags |= FRM_INVALIDATE_XMAX;
        newxmax = InvalidTransactionId;
    } else if TransactionIdIsValid(update_xid) && !has_lockers {
        debug_assert!(newmembers.len() == 1);
        *flags |= FRM_RETURN_IS_XID;
        if update_committed {
            *flags |= FRM_MARK_COMMITTED;
        }
        newxmax = update_xid;
    } else {
        newxmax = multixact_seams::multi_xact_id_create_from_members::call(
            &mut newmembers[..],
        )?;
        *flags |= FRM_RETURN_IS_MULTI;
    }

    pagefrz.freeze_required = true;
    Ok(newxmax)
}


pub(crate) fn GetMultiXactIdHintBits(multi: MultiXactId) -> PgResult<(u16, u16)> {
    let mut bits: u16 = HEAP_XMAX_IS_MULTI;
    let mut bits2: u16 = 0;
    let mut has_update = false;
    let mut strongest = LockTupleMode::LockTupleKeyShare;

    multixact_seams::get_multi_xact_id_members::call(multi, false, false, &mut |members| {
        for m in members {
            let mode = match m.status {
                MultiXactStatus::MultiXactStatusForKeyShare => LockTupleMode::LockTupleKeyShare,
                MultiXactStatus::MultiXactStatusForShare => LockTupleMode::LockTupleShare,
                MultiXactStatus::MultiXactStatusForNoKeyUpdate
                | MultiXactStatus::MultiXactStatusNoKeyUpdate => {
                    LockTupleMode::LockTupleNoKeyExclusive
                }
                MultiXactStatus::MultiXactStatusForUpdate
                | MultiXactStatus::MultiXactStatusUpdate => LockTupleMode::LockTupleExclusive,
            };
            if mode > strongest {
                strongest = mode;
            }
            match m.status {
                MultiXactStatus::MultiXactStatusForUpdate => bits2 |= HEAP_KEYS_UPDATED,
                MultiXactStatus::MultiXactStatusNoKeyUpdate => has_update = true,
                MultiXactStatus::MultiXactStatusUpdate => {
                    bits2 |= HEAP_KEYS_UPDATED;
                    has_update = true;
                }
                _ => {}
            }
        }
    })?;

    match strongest {
        LockTupleMode::LockTupleExclusive | LockTupleMode::LockTupleNoKeyExclusive => {
            bits |= HEAP_XMAX_EXCL_LOCK
        }
        LockTupleMode::LockTupleShare => bits |= HEAP_XMAX_SHR_LOCK,
        LockTupleMode::LockTupleKeyShare => bits |= HEAP_XMAX_KEYSHR_LOCK,
    }
    if !has_update {
        bits |= HEAP_XMAX_LOCK_ONLY;
    }
    Ok((bits, bits2))
}

/// Returns (has_freeze_plan, totally_frozen).
pub fn heap_prepare_freeze_tuple(
    tuple: &HeapTupleHeaderData,
    cutoffs: &VacuumCutoffs,
    pagefrz: &mut HeapPageFreeze,
    frz: &mut HeapTupleFreeze,
) -> PgResult<(bool, bool)> {
    let mut xmin_already_frozen = false;
    let mut xmax_already_frozen = false;
    let mut freeze_xmin = false;
    let mut replace_xvac = false;
    let mut replace_xmax = false;
    let mut freeze_xmax = false;

    frz.xmax = tuple.xmax_raw();
    frz.t_infomask2 = tuple.t_infomask2;
    frz.t_infomask = tuple.t_infomask;
    frz.frzflags = 0;
    frz.checkflags = 0;

    let xid = tuple.xmin();
    if !TransactionIdIsNormal(xid) {
        xmin_already_frozen = true;
    } else {
        if TransactionIdPrecedes(xid, cutoffs.relfrozenxid) {
            data_corrupted(
                format!(
                    "found xmin {xid} from before relfrozenxid {}",
                    cutoffs.relfrozenxid
                ),
            )?;
        }
        freeze_xmin = TransactionIdPrecedes(xid, cutoffs.OldestXmin);
        if freeze_xmin {
            frz.checkflags |= HEAP_FREEZE_CHECK_XMIN_COMMITTED;
        }
    }

    let xid = tuple.xvac();
    if TransactionIdIsNormal(xid) {
        debug_assert!(TransactionIdPrecedesOrEquals(cutoffs.relfrozenxid, xid));
        debug_assert!(TransactionIdPrecedes(xid, cutoffs.OldestXmin));
        replace_xvac = true;
        pagefrz.freeze_required = true;
    }

    let xid = frz.xmax;
    if tuple.t_infomask & HEAP_XMAX_IS_MULTI != 0 {
        let mut flags: u16 = 0;
        let newxmax = FreezeMultiXactId(xid, tuple.t_infomask, cutoffs, &mut flags, pagefrz)?;

        if flags & FRM_NOOP != 0 {
            debug_assert!(!MultiXactIdPrecedes(newxmax, cutoffs.MultiXactCutoff));
            debug_assert!(MultiXactIdIsValid(newxmax) && xid == newxmax);
        } else if flags & FRM_RETURN_IS_XID != 0 {
            debug_assert!(!TransactionIdPrecedes(newxmax, cutoffs.OldestXmin));
            frz.t_infomask &= !HEAP_XMAX_BITS;
            frz.xmax = newxmax;
            if flags & FRM_MARK_COMMITTED != 0 {
                frz.t_infomask |= HEAP_XMAX_COMMITTED;
            }
            replace_xmax = true;
        } else if flags & FRM_RETURN_IS_MULTI != 0 {
            debug_assert!(!MultiXactIdPrecedes(newxmax, cutoffs.OldestMxact));
            frz.t_infomask &= !HEAP_XMAX_BITS;
            frz.t_infomask2 &= !HEAP_KEYS_UPDATED;
            let (newbits, newbits2) = GetMultiXactIdHintBits(newxmax)?;
            frz.t_infomask |= newbits;
            frz.t_infomask2 |= newbits2;
            frz.xmax = newxmax;
            replace_xmax = true;
        } else {
            debug_assert!(flags & FRM_INVALIDATE_XMAX != 0);
            debug_assert!(!TransactionIdIsValid(newxmax));
            freeze_xmax = true;
        }
        debug_assert!(pagefrz.freeze_required || (!freeze_xmax && !replace_xmax));
    } else if TransactionIdIsNormal(xid) {
        if TransactionIdPrecedes(xid, cutoffs.relfrozenxid) {
            data_corrupted(
                format!(
                    "found xmax {xid} from before relfrozenxid {}",
                    cutoffs.relfrozenxid
                ),
            )?;
        }
        freeze_xmax = TransactionIdPrecedes(xid, cutoffs.OldestXmin);
        if freeze_xmax && !HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask) {
            frz.checkflags |= HEAP_FREEZE_CHECK_XMAX_ABORTED;
        }
    } else if !TransactionIdIsValid(xid) {
        debug_assert!(tuple.t_infomask & HEAP_XMAX_IS_MULTI == 0);
        xmax_already_frozen = true;
    } else {
        data_corrupted(
            format!(
                "found raw xmax {xid} (infomask 0x{:04x}) not invalid and not multi",
                tuple.t_infomask
            ),
        )?;
    }

    if freeze_xmin {
        debug_assert!(!xmin_already_frozen);
        frz.t_infomask |= HEAP_XMIN_FROZEN;
    }
    if replace_xvac {
        debug_assert!(pagefrz.freeze_required);
        if tuple.t_infomask & HEAP_MOVED_OFF != 0 {
            frz.frzflags |= XLH_INVALID_XVAC;
        } else {
            frz.frzflags |= XLH_FREEZE_XVAC;
        }
    }
    if replace_xmax {
        debug_assert!(!xmax_already_frozen && !freeze_xmax);
        debug_assert!(pagefrz.freeze_required);
    }
    if freeze_xmax {
        debug_assert!(!xmax_already_frozen && !replace_xmax);
        frz.xmax = InvalidTransactionId;
        frz.t_infomask &= !HEAP_XMAX_BITS;
        frz.t_infomask |= HEAP_XMAX_INVALID;
        frz.t_infomask2 &= !HEAP_HOT_UPDATED;
        frz.t_infomask2 &= !HEAP_KEYS_UPDATED;
    }

    let totally_frozen =
        (freeze_xmin || xmin_already_frozen) && (freeze_xmax || xmax_already_frozen);

    if !pagefrz.freeze_required && !(xmin_already_frozen && xmax_already_frozen) {
        pagefrz.freeze_required = heap_tuple_should_freeze(
            tuple,
            cutoffs,
            &mut pagefrz.NoFreezePageRelfrozenXid,
            &mut pagefrz.NoFreezePageRelminMxid,
        )?;
    }

    Ok((
        freeze_xmin || replace_xvac || replace_xmax || freeze_xmax,
        totally_frozen,
    ))
}

pub fn heap_execute_freeze_tuple(tuple: &mut HeapTupleHeaderData, frz: &HeapTupleFreeze) {
    tuple.set_xmax(frz.xmax);
    if frz.frzflags & XLH_FREEZE_XVAC != 0 {
        tuple.set_xvac(FrozenTransactionId);
    }
    if frz.frzflags & XLH_INVALID_XVAC != 0 {
        tuple.set_xvac(InvalidTransactionId);
    }
    tuple.t_infomask = frz.t_infomask;
    tuple.t_infomask2 = frz.t_infomask2;
}

/// # Safety
/// Caller holds the buffer pinned and exclusively locked; `offset` is an
/// LP_NORMAL item of the page.
unsafe fn header_mut_at<'a>(
    page: PageRef<'a>,
    offset: OffsetNumber,
) -> &'a mut HeapTupleHeaderData {
    let lp = page.item_id(offset);
    let (ptr, _len) = page.item_raw(lp);
    // SAFETY: caller contract.
    unsafe { &mut *(ptr.cast_mut().cast::<HeapTupleHeaderData>()) }
}

/// pg_xact sanity checks deferred out of heap_prepare_freeze_tuple (VACUUMs
/// that decide against freezing the page must not pay for them).
pub fn heap_pre_freeze_checks(buffer: Buffer, tuples: &[HeapTupleFreeze]) -> PgResult<()> {
    // SAFETY: caller holds pin + cleanup lock for the whole freeze.
    let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) };
    for frz in tuples {
        // SAFETY: offsets recorded from this page's LP_NORMAL items.
        let htup = unsafe { header_mut_at(page, frz.offset) };

        if frz.checkflags & HEAP_FREEZE_CHECK_XMIN_COMMITTED != 0 {
            let xmin = htup.xmin_raw();
            debug_assert!(!htup.xmin_frozen());
            if !transam_seams::transaction_id_did_commit::call(xmin)? {
                data_corrupted(
                    format!("uncommitted xmin {xmin} needs to be frozen"),
                )?;
            }
        }
        // TransactionIdDidAbort is unreliable for crashed xacts; only check
        // that xmax did not commit.
        if frz.checkflags & HEAP_FREEZE_CHECK_XMAX_ABORTED != 0 {
            let xmax = htup.xmax_raw();
            debug_assert!(TransactionIdIsNormal(xmax));
            if transam_seams::transaction_id_did_commit::call(xmax)? {
                data_corrupted(
                    format!("cannot freeze committed xmax {xmax}"),
                )?;
            }
        }
    }
    Ok(())
}

/// Must be called in a critical section that also dirties the buffer and, if
/// needed, emits WAL.
pub fn heap_freeze_prepared_tuples(buffer: Buffer, tuples: &[HeapTupleFreeze]) {
    // SAFETY: caller holds pin + cleanup lock inside the critical section.
    let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) };
    for frz in tuples {
        // SAFETY: offsets recorded from this page's LP_NORMAL items.
        let htup = unsafe { header_mut_at(page, frz.offset) };
        heap_execute_freeze_tuple(htup, frz);
    }
}

/// Freeze one tuple in place without WAL logging (rewriteheap/CLUSTER lane).
pub fn heap_freeze_tuple(
    tuple: &mut HeapTupleHeaderData,
    relfrozenxid: TransactionId,
    relminmxid: MultiXactId,
    FreezeLimit: TransactionId,
    MultiXactCutoff: MultiXactId,
) -> PgResult<bool> {
    let cutoffs = VacuumCutoffs {
        relfrozenxid,
        relminmxid,
        OldestXmin: FreezeLimit,
        OldestMxact: MultiXactCutoff,
        FreezeLimit,
        MultiXactCutoff,
    };
    let mut pagefrz = HeapPageFreeze {
        freeze_required: true,
        FreezePageRelfrozenXid: FreezeLimit,
        FreezePageRelminMxid: MultiXactCutoff,
        NoFreezePageRelfrozenXid: FreezeLimit,
        NoFreezePageRelminMxid: MultiXactCutoff,
    };
    let mut frz = HeapTupleFreeze::default();
    let (do_freeze, _totally_frozen) =
        heap_prepare_freeze_tuple(tuple, &cutoffs, &mut pagefrz, &mut frz)?;
    if do_freeze {
        heap_execute_freeze_tuple(tuple, &frz);
    }
    Ok(do_freeze)
}

/// Would heap_prepare_freeze_tuple force freezing of this tuple's page?
/// Also ratchets the caller's "no freeze" trackers.
pub fn heap_tuple_should_freeze(
    tuple: &HeapTupleHeaderData,
    cutoffs: &VacuumCutoffs,
    no_freeze_page_relfrozen_xid: &mut TransactionId,
    no_freeze_page_relmin_mxid: &mut MultiXactId,
) -> PgResult<bool> {
    let mut freeze = false;

    let xid = tuple.xmin();
    if TransactionIdIsNormal(xid) {
        debug_assert!(TransactionIdPrecedesOrEquals(cutoffs.relfrozenxid, xid));
        if TransactionIdPrecedes(xid, *no_freeze_page_relfrozen_xid) {
            *no_freeze_page_relfrozen_xid = xid;
        }
        if TransactionIdPrecedes(xid, cutoffs.FreezeLimit) {
            freeze = true;
        }
    }

    let mut xid = InvalidTransactionId;
    let mut multi = InvalidMultiXactId;
    if tuple.t_infomask & HEAP_XMAX_IS_MULTI != 0 {
        multi = tuple.xmax_raw();
    } else {
        xid = tuple.xmax_raw();
    }

    if TransactionIdIsNormal(xid) {
        debug_assert!(TransactionIdPrecedesOrEquals(cutoffs.relfrozenxid, xid));
        if TransactionIdPrecedes(xid, *no_freeze_page_relfrozen_xid) {
            *no_freeze_page_relfrozen_xid = xid;
        }
        if TransactionIdPrecedes(xid, cutoffs.FreezeLimit) {
            freeze = true;
        }
    } else if !MultiXactIdIsValid(multi) {
    } else if HEAP_LOCKED_UPGRADED(tuple.t_infomask) {
        if MultiXactIdPrecedes(multi, *no_freeze_page_relmin_mxid) {
            *no_freeze_page_relmin_mxid = multi;
        }
        freeze = true;
    } else {
        debug_assert!(MultiXactIdPrecedesOrEquals(cutoffs.relminmxid, multi));
        if MultiXactIdPrecedes(multi, *no_freeze_page_relmin_mxid) {
            *no_freeze_page_relmin_mxid = multi;
        }
        if MultiXactIdPrecedes(multi, cutoffs.MultiXactCutoff) {
            freeze = true;
        }

        multixact_seams::get_multi_xact_id_members::call(
            multi,
            false,
            HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask),
            &mut |members| {
                for m in members {
                    let xid = m.xid;
                    debug_assert!(TransactionIdPrecedesOrEquals(cutoffs.relfrozenxid, xid));
                    if TransactionIdPrecedes(xid, *no_freeze_page_relfrozen_xid) {
                        *no_freeze_page_relfrozen_xid = xid;
                    }
                    if TransactionIdPrecedes(xid, cutoffs.FreezeLimit) {
                        freeze = true;
                    }
                }
            },
        )?;
    }

    if tuple.t_infomask & HEAP_MOVED != 0 {
        let xid = tuple.xvac();
        if TransactionIdIsNormal(xid) {
            debug_assert!(TransactionIdPrecedesOrEquals(cutoffs.relfrozenxid, xid));
            if TransactionIdPrecedes(xid, *no_freeze_page_relfrozen_xid) {
                *no_freeze_page_relfrozen_xid = xid;
            }
            freeze = true;
        }
    }

    Ok(freeze)
}
