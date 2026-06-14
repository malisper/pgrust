//! `backend-access-heap-heapam` — the heap access method (`access/heap/heapam.c`).
//!
//! This crate is being ported family-by-family on top of the W1
//! `heapam-visibility`, W2 `visibilitymap`, and `hio` units. **F0** (this
//! commit) lays the skeleton:
//!
//!   * the NET-NEW page/tuple freeze descriptors `HeapTupleFreeze` /
//!     `HeapPageFreeze` (`access/heapam.h`),
//!   * the `BulkInsertStateData` carrier + its `GetBulkInsertState` /
//!     `FreeBulkInsertState` / `ReleaseBulkInsertStatePin` lifecycle
//!     (`access/hio.h` struct, heapam.c functions),
//!   * the shared pure-ish helpers `compute_infobits` / `xmax_infomask_changed`
//!     / `UpdateXmaxHintBits` / `GetMultiXactIdHintBits` and the WAL emitter
//!     `log_heap_new_cid`.
//!
//! Page access goes through this repo's `Buffer`-id-through-seams model
//! (the freespace.c / visibilitymap precedent): the buffer manager owns the
//! shared page; heapam crosses the boundary by `Buffer` id and the
//! `bufmgr-seams` primitives, never an aliasable `Page` pointer. The
//! src-idiomatic crate's bare-`&[u8]` page model is a logic reference only.
//!
//! The cross-family heap-AM entry points (heap_insert/delete/update/lock/...)
//! are declared in `backend-access-heap-heapam-seams`; the per-family fills
//! install them from this crate's `init_seams()`. F0 installs only the seams
//! whose bodies it can already provide (`heap_tuple_get_update_xid`); the rest
//! stay seam-and-panic until their family lands.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod freeze;

use mcx::Mcx;
use types_core::primitive::{
    BlockNumber, InvalidBlockNumber, MultiXactId, OffsetNumber, Oid, TransactionId,
};
use types_core::xact::{CommandId, InvalidCommandId};
use types_core::XLogRecPtr;
use types_error::PgResult;
use types_rel::RelationData;
use types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};
use types_storage::{Buffer, InvalidBuffer};
use types_tableam::tableam::LockTupleMode;
use types_tuple::heaptuple::{
    HeapTupleData, HeapTupleHeaderData, HEAP_COMBOCID, HEAP_KEYS_UPDATED,
    HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK,
    HEAP_XMAX_LOCK_ONLY,
};
use types_xlog_records::heapam_xlog::{xl_heap_new_cid, SizeOfHeapNewCid};
use types_xlog_records::multixact::MultiXactStatus;

// htup_details.h infomask helpers + lock-mask vocabulary already live in the
// W1 visibility crate; reuse them rather than re-deriving.
use backend_access_heap_heapam_visibility::htup::HEAP_XMAX_IS_LOCKED_ONLY;
use types_tuple::heaptuple::HEAP_XMAX_EXCL_LOCK;
use backend_access_heap_heapam_visibility::{HeapTupleHeaderGetUpdateXid, HeapTupleSetHintBits};

// XLHL_* infobits live with the heap rmgr-desc (access/heapam_xlog.h constants).
use backend_rmgrdesc_next::heapdesc::{
    XLHL_KEYS_UPDATED, XLHL_XMAX_EXCL_LOCK, XLHL_XMAX_IS_MULTI, XLHL_XMAX_KEYSHR_LOCK,
    XLHL_XMAX_LOCK_ONLY,
};

use backend_access_heap_heapam_seams as heapam_seam;
use backend_access_transam_multixact_seams as multixact_seam;
use backend_access_transam_xact_seams as xact_seam;
use backend_access_transam_xloginsert_seams as xloginsert_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;

use types_wal::wal::RM_HEAP2_ID;
use backend_rmgrdesc_next::heapdesc::XLOG_HEAP2_NEW_CID;

// ===========================================================================
// HeapTupleFreeze / HeapPageFreeze — NET-NEW descriptors (access/heapam.h).
// ===========================================================================

/// `HeapTupleFreeze` (`access/heapam.h`) — a single tuple's freeze plan,
/// produced by `heap_prepare_freeze_tuple` and executed by
/// `heap_execute_freeze_tuple` / `heap_freeze_prepared_tuples`.
#[derive(Clone, Copy, Debug, Default)]
pub struct HeapTupleFreeze {
    /* Fields describing how to process tuple */
    pub xmax: TransactionId,
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub frzflags: u8,

    /* xmin/xmax check flags */
    pub checkflags: u8,
    /* Page offset number for tuple */
    pub offset: OffsetNumber,
}

/// `HeapPageFreeze` (`access/heapam.h`) — VACUUM's per-page freeze state,
/// updated across each `heap_prepare_freeze_tuple` call. It tracks whether
/// freezing the page is required and the oldest extant XID/MXID under both the
/// "freeze" and "no freeze" plans (for advancing relfrozenxid/relminmxid).
#[derive(Clone, Copy, Debug, Default)]
pub struct HeapPageFreeze {
    /// Is `heap_prepare_freeze_tuple` caller required to freeze the page?
    pub freeze_required: bool,

    /// "Freeze" `NewRelfrozenXid` tracker.
    pub FreezePageRelfrozenXid: TransactionId,
    /// "Freeze" `NewRelminMxid` tracker.
    pub FreezePageRelminMxid: MultiXactId,

    /// "No freeze" `NewRelfrozenXid` tracker.
    pub NoFreezePageRelfrozenXid: TransactionId,
    /// "No freeze" `NewRelminMxid` tracker.
    pub NoFreezePageRelminMxid: MultiXactId,
}

// ===========================================================================
// BulkInsertStateData — bulk-insert carrier (access/hio.h struct).
// ===========================================================================

/// `BulkInsertStateData` (`access/hio.h`): the state carried across a bulk
/// insert. `current_buf` is the current insertion-target page (an
/// `InvalidBuffer` when none is held); `next_free`/`last_free` track pages left
/// unused by the last bulk extension; `already_extended_by` records how many
/// pages this bulk inserter has extended by so far.
#[derive(Clone, Copy, Debug)]
pub struct BulkInsertStateData {
    /// Our `BULKWRITE` strategy object.
    pub strategy: BufferAccessStrategy,
    /// Current insertion target page.
    pub current_buf: Buffer,

    /// Further pages that were unused at the time of the last extension.
    pub next_free: BlockNumber,
    pub last_free: BlockNumber,
    /// Number of pages that this bulk inserter extended by.
    pub already_extended_by: u32,
}

/// `BulkInsertState` (`access/heapam.h`) — the by-value handle callers thread.
/// C uses a `BulkInsertStateData *`; the repo carries the owned struct.
pub type BulkInsertState = BulkInsertStateData;

/// `GetBulkInsertState()` (heapam.c) — prepare a status object for a bulk
/// insert, acquiring a `BAS_BULKWRITE` ring buffer.
pub fn GetBulkInsertState() -> PgResult<BulkInsertState> {
    let bistate = BulkInsertStateData {
        strategy: bufmgr_seam::get_access_strategy::call(BufferAccessStrategyType::BasBulkwrite)?,
        current_buf: InvalidBuffer,
        next_free: InvalidBlockNumber,
        last_free: InvalidBlockNumber,
        already_extended_by: 0,
    };
    Ok(bistate)
}

/// `FreeBulkInsertState(bistate)` (heapam.c) — clean up after finishing a bulk
/// insert: release any held buffer and free the ring buffer.
pub fn FreeBulkInsertState(bistate: &mut BulkInsertState) {
    if bistate.current_buf != InvalidBuffer {
        bufmgr_seam::release_buffer::call(bistate.current_buf);
    }
    bufmgr_seam::free_access_strategy::call(bistate.strategy);
    // C `pfree(bistate)`s the heap allocation; the repo carries the value, so
    // the storage is dropped by the caller.
}

/// `ReleaseBulkInsertStatePin(bistate)` (heapam.c) — release the buffer
/// currently held in `bistate` and reset its bulk-extension state.
pub fn ReleaseBulkInsertStatePin(bistate: &mut BulkInsertState) {
    if bistate.current_buf != InvalidBuffer {
        bufmgr_seam::release_buffer::call(bistate.current_buf);
    }
    bistate.current_buf = InvalidBuffer;

    /*
     * Despite the name, we also reset bulk relation extension state.
     * Otherwise we can end up erroring out due to looking for free space in
     * ->next_free of one partition, even though ->next_free was set when
     * extending another partition.
     */
    bistate.next_free = InvalidBlockNumber;
    bistate.last_free = InvalidBlockNumber;
}

// ===========================================================================
// compute_infobits / xmax_infomask_changed — shared infomask helpers.
// ===========================================================================

/// `compute_infobits(infomask, infomask2)` (heapam.c) — the `infobits_set` byte
/// saved in the delete/update/lock WAL records.
pub fn compute_infobits(infomask: u16, infomask2: u16) -> u8 {
    (if (infomask & HEAP_XMAX_IS_MULTI) != 0 { XLHL_XMAX_IS_MULTI } else { 0 })
        | (if (infomask & HEAP_XMAX_LOCK_ONLY) != 0 { XLHL_XMAX_LOCK_ONLY } else { 0 })
        | (if (infomask & HEAP_XMAX_EXCL_LOCK) != 0 { XLHL_XMAX_EXCL_LOCK } else { 0 })
        // note we ignore HEAP_XMAX_SHR_LOCK here
        | (if (infomask & HEAP_XMAX_KEYSHR_LOCK) != 0 { XLHL_XMAX_KEYSHR_LOCK } else { 0 })
        | (if (infomask2 & HEAP_KEYS_UPDATED) != 0 { XLHL_KEYS_UPDATED } else { 0 })
}

/// `xmax_infomask_changed(new_infomask, old_infomask)` (heapam.c) — whether the
/// relevant xmax status bits changed across a buffer-lock release/reacquire.
pub fn xmax_infomask_changed(new_infomask: u16, old_infomask: u16) -> bool {
    use backend_access_heap_heapam_visibility::htup::HEAP_LOCK_MASK;
    let interesting = HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_LOCK_MASK;
    (new_infomask & interesting) != (old_infomask & interesting)
}

// ===========================================================================
// UpdateXmaxHintBits — set xmax hint bits after the xmax xact ends (heapam.c).
// ===========================================================================

/// `UpdateXmaxHintBits(tuple, buffer, xid)` (heapam.c) — update a tuple's hint
/// bits after we have waited for its XMAX transaction to terminate. On exit,
/// callers may rely only on `XMAX_INVALID`. Not allowed when xmax is a
/// multixact.
pub fn UpdateXmaxHintBits(
    tuple: &mut HeapTupleHeaderData,
    buffer: Buffer,
    xid: TransactionId,
) -> PgResult<()> {
    debug_assert!(tuple.t_infomask & HEAP_XMAX_IS_MULTI == 0);

    if (tuple.t_infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID)) == 0 {
        if !HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask)
            && xact_seam_transaction_id_did_commit(xid)?
        {
            HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, xid)?;
        } else {
            HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId)?;
        }
    }
    Ok(())
}

/// `InvalidTransactionId`.
const InvalidTransactionId: TransactionId = 0;

/// `TransactionIdDidCommit(xid)` — clog lookup through the transam owner seam.
/// The owner threads `TransactionXmin` (C's snapmgr.c global) explicitly; read
/// it from snapmgr and pass it through, mirroring the visibility consumer.
fn xact_seam_transaction_id_did_commit(xid: TransactionId) -> PgResult<bool> {
    let transaction_xmin =
        backend_utils_time_snapmgr_pc_seams::transaction_xmin::call()?;
    backend_access_transam_transam_seams::transaction_id_did_commit::call(xid, transaction_xmin)
}

// ===========================================================================
// GetMultiXactIdHintBits — compute the infomask/infomask2 for a multixact.
// ===========================================================================

/// `TUPLOCK_from_mxstatus(status)` (heapam.c) — the `LockTupleMode` a multixact
/// member's status holds (`MultiXactStatusLock[]`).
fn TUPLOCK_from_mxstatus(status: MultiXactStatus) -> LockTupleMode {
    match status {
        MultiXactStatus::ForKeyShare => LockTupleMode::LockTupleKeyShare,
        MultiXactStatus::ForShare => LockTupleMode::LockTupleShare,
        MultiXactStatus::ForNoKeyUpdate => LockTupleMode::LockTupleNoKeyExclusive,
        MultiXactStatus::ForUpdate => LockTupleMode::LockTupleExclusive,
        MultiXactStatus::NoKeyUpdate => LockTupleMode::LockTupleNoKeyExclusive,
        MultiXactStatus::Update => LockTupleMode::LockTupleExclusive,
    }
}

/// `GetMultiXactIdHintBits(multi, &new_infomask, &new_infomask2)` (heapam.c) —
/// for a (just-created) MultiXactId, compute the hint bits that should be set
/// in the tuple's infomask/infomask2. Returns `(new_infomask, new_infomask2)`.
pub fn GetMultiXactIdHintBits<'mcx>(
    mcx: Mcx<'mcx>,
    multi: MultiXactId,
) -> PgResult<(u16, u16)> {
    let mut bits: u16 = HEAP_XMAX_IS_MULTI;
    let mut bits2: u16 = 0;
    let mut has_update = false;
    let mut strongest = LockTupleMode::LockTupleKeyShare;

    /*
     * We only use this in multis we just created, so they cannot be values
     * pre-pg_upgrade.
     */
    let members =
        multixact_seam::get_multi_xact_id_members::call(mcx, multi, false, false)?;

    for member in members.iter() {
        // Remember the strongest lock mode held by any member of the multixact.
        // The member status is `None` only for out-of-enum on-disk values,
        // which cannot occur for the just-created multis this is called on.
        let status = member
            .status
            .expect("GetMultiXactIdHintBits: multixact member with out-of-range status");
        let mode = TUPLOCK_from_mxstatus(status);
        if (mode as i32) > (strongest as i32) {
            strongest = mode;
        }

        // See what other bits we need.
        match status {
            MultiXactStatus::ForKeyShare
            | MultiXactStatus::ForShare
            | MultiXactStatus::ForNoKeyUpdate => {}
            MultiXactStatus::ForUpdate => {
                bits2 |= HEAP_KEYS_UPDATED;
            }
            MultiXactStatus::NoKeyUpdate => {
                has_update = true;
            }
            MultiXactStatus::Update => {
                bits2 |= HEAP_KEYS_UPDATED;
                has_update = true;
            }
        }
    }

    if strongest == LockTupleMode::LockTupleExclusive
        || strongest == LockTupleMode::LockTupleNoKeyExclusive
    {
        bits |= HEAP_XMAX_EXCL_LOCK;
    } else if strongest == LockTupleMode::LockTupleShare {
        bits |= HEAP_XMAX_SHR_LOCK;
    } else if strongest == LockTupleMode::LockTupleKeyShare {
        bits |= HEAP_XMAX_KEYSHR_LOCK;
    }

    if !has_update {
        bits |= HEAP_XMAX_LOCK_ONLY;
    }

    // C `pfree(members)`s here; the owned PgVec is dropped at scope end.

    Ok((bits, bits2))
}

/// `HEAP_XMAX_SHR_LOCK` (htup_details.h).
const HEAP_XMAX_SHR_LOCK: u16 = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;

// ===========================================================================
// log_heap_new_cid — emit the XLOG_HEAP2_NEW_CID record (heapam.c).
// ===========================================================================

/// `log_heap_new_cid(relation, tup)` (heapam.c) — perform `XLogInsert` of an
/// `XLOG_HEAP2_NEW_CID` record. Only used at `wal_level >= logical`, and only
/// for catalog tuples. Returns the record's LSN.
pub fn log_heap_new_cid(
    relation: &RelationData,
    tup: &HeapTupleData,
) -> PgResult<XLogRecPtr> {
    let hdr = tup
        .t_data
        .as_ref()
        .expect("log_heap_new_cid: tuple has no data");

    debug_assert!(tup.t_tableOid != Oid::default());

    let top_xid = xact_seam::get_top_transaction_id::call()?;

    let cmin;
    let cmax;
    let combocid;

    /*
     * If the tuple got inserted & deleted in the same TX we definitely have a
     * combo CID, set cmin and cmax.
     */
    if hdr.t_infomask & HEAP_COMBOCID != 0 {
        cmin = HeapTupleHeaderGetCmin(hdr);
        cmax = HeapTupleHeaderGetCmax(hdr);
        combocid = HeapTupleHeaderGetRawCommandId(hdr);
    } else {
        /* No combo CID, so only cmin or cmax can be set by this TX */
        if hdr.t_infomask & HEAP_XMAX_INVALID != 0
            || HEAP_XMAX_IS_LOCKED_ONLY(hdr.t_infomask)
        {
            /* Tuple inserted. */
            cmin = HeapTupleHeaderGetRawCommandId(hdr);
            cmax = InvalidCommandId;
        } else {
            /* Tuple from a different tx updated or deleted. */
            cmin = InvalidCommandId;
            cmax = HeapTupleHeaderGetRawCommandId(hdr);
        }
        combocid = InvalidCommandId;
    }

    let xlrec = xl_heap_new_cid {
        top_xid,
        cmin,
        cmax,
        combocid,
        target_locator: relation.rd_locator,
        target_tid: tup.t_self,
    };

    /*
     * Note that we don't need to register the buffer here, because this
     * operation does not modify the page. The insert/update/delete that
     * called us certainly did, but that's WAL-logged separately.
     */
    xloginsert_seam::xlog_begin_insert::call()?;
    let buf = xlrec.to_bytes();
    xloginsert_seam::xlog_register_data::call(&buf[..SizeOfHeapNewCid])?;

    /* will be looked at irrespective of origin */
    let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP2_ID, XLOG_HEAP2_NEW_CID)?;

    Ok(recptr)
}

/// `HeapTupleHeaderGetCmin(hdr)` — combo-CID resolution via the owner seam.
fn HeapTupleHeaderGetCmin(hdr: &HeapTupleHeaderData) -> CommandId {
    backend_utils_time_combocid_seams::heap_tuple_header_get_cmin::call(hdr)
}

/// `HeapTupleHeaderGetCmax(hdr)` — combo-CID resolution via the owner seam.
fn HeapTupleHeaderGetCmax(hdr: &HeapTupleHeaderData) -> CommandId {
    backend_utils_time_combocid_seams::heap_tuple_header_get_cmax::call(hdr)
}

/// `HeapTupleHeaderGetRawCommandId(hdr)` — the raw command id stored in the
/// header (`t_field3.t_cid`), via the types-tuple accessor.
fn HeapTupleHeaderGetRawCommandId(hdr: &HeapTupleHeaderData) -> CommandId {
    types_tuple::heaptuple::HeapTupleHeaderGetRawCommandId(hdr)
}

// ===========================================================================
// init_seams — install the inward seams whose bodies F0 can already provide.
// The remaining heap-AM entry points stay seam-and-panic until their family
// lands and installs them from here.
// ===========================================================================

/// Install the heapam inward seams this F0 skeleton can satisfy, plus the
/// F5 FREEZE family's page-bound entry points (declared on the vacuumlazy
/// owner's `-seams` crate, installed here by the heap AM).
pub fn init_seams() {
    // `HeapTupleGetUpdateXid(htup)` reduces to the visibility crate's
    // `HeapTupleHeaderGetUpdateXid` (header-only multixact resolution).
    heapam_seam::heap_tuple_get_update_xid::set(|tuple| HeapTupleHeaderGetUpdateXid(tuple));

    // F5 FREEZE: `heap_tuple_should_freeze(buffer, offnum, cutoffs, ...)` reads
    // the on-page `HeapTupleHeader` at `offnum` and runs the pure
    // `freeze::heap_tuple_should_freeze`, returning the advanced "no freeze"
    // trackers.
    backend_access_heap_vacuumlazy_seams::heap_tuple_should_freeze::set(
        |buffer, offnum, cutoffs, relfrozen_xid_in, relmin_mxid_in| {
            let ctx = mcx::MemoryContext::new("heap_tuple_should_freeze");
            let mcx = ctx.mcx();
            let tuple = read_on_page_header(mcx, buffer, offnum)?;
            freeze::heap_tuple_should_freeze(
                mcx,
                &tuple,
                &cutoffs,
                relfrozen_xid_in,
                relmin_mxid_in,
            )
        },
    );

    // F5 FREEZE: `heap_tuple_needs_eventual_freeze(buffer, offnum)` reads the
    // on-page header and runs the pure predicate.
    backend_access_heap_vacuumlazy_seams::heap_tuple_needs_eventual_freeze::set(
        |buffer, offnum| {
            let ctx = mcx::MemoryContext::new("heap_tuple_needs_eventual_freeze");
            let mcx = ctx.mcx();
            let tuple = read_on_page_header(mcx, buffer, offnum)?;
            Ok(freeze::heap_tuple_needs_eventual_freeze(&tuple))
        },
    );
}

/// Materialize an on-page `HeapTupleHeader` at `(buffer, offnum)` into `mcx`
/// (C's `(HeapTupleHeader) PageGetItem(page, PageGetItemId(page, offnum))`).
fn read_on_page_header<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: Buffer,
    offnum: OffsetNumber,
) -> PgResult<HeapTupleHeaderData<'mcx>> {
    let mut out: Option<HeapTupleHeaderData<'mcx>> = None;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = backend_storage_page::PageRef::new(page_bytes)?;
        let item_id = backend_storage_page::PageGetItemId(&page, offnum)?;
        let item = backend_storage_page::PageGetItem(&page, &item_id)?;
        out = Some(HeapTupleHeaderData::read_on_page(mcx, item)?);
        Ok(())
    })?;
    Ok(out.expect("with_buffer_page closure must have run"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use backend_access_heap_heapam_visibility::htup::HEAP_XMAX_SHR_LOCK as TT_HEAP_XMAX_SHR_LOCK;

    #[test]
    fn compute_infobits_matches_c() {
        assert_eq!(compute_infobits(0, 0), 0);
        assert_eq!(compute_infobits(HEAP_XMAX_IS_MULTI, 0), XLHL_XMAX_IS_MULTI);
        assert_eq!(compute_infobits(HEAP_XMAX_LOCK_ONLY, 0), XLHL_XMAX_LOCK_ONLY);
        assert_eq!(compute_infobits(HEAP_XMAX_EXCL_LOCK, 0), XLHL_XMAX_EXCL_LOCK);
        assert_eq!(compute_infobits(HEAP_XMAX_KEYSHR_LOCK, 0), XLHL_XMAX_KEYSHR_LOCK);
        assert_eq!(compute_infobits(0, HEAP_KEYS_UPDATED), XLHL_KEYS_UPDATED);
        // HEAP_XMAX_SHR_LOCK is explicitly ignored by compute_infobits.
        assert_eq!(
            compute_infobits(TT_HEAP_XMAX_SHR_LOCK, 0),
            XLHL_XMAX_EXCL_LOCK | XLHL_XMAX_KEYSHR_LOCK
        );
        assert_eq!(
            compute_infobits(HEAP_XMAX_EXCL_LOCK, HEAP_KEYS_UPDATED),
            XLHL_XMAX_EXCL_LOCK | XLHL_KEYS_UPDATED
        );
    }

    #[test]
    fn xmax_infomask_changed_tracks_interesting_bits() {
        // Same interesting bits -> unchanged.
        assert!(!xmax_infomask_changed(HEAP_XMAX_IS_MULTI, HEAP_XMAX_IS_MULTI));
        // Differing interesting bit -> changed.
        assert!(xmax_infomask_changed(HEAP_XMAX_IS_MULTI, 0));
        assert!(xmax_infomask_changed(HEAP_XMAX_LOCK_ONLY, 0));
        // HEAP_XMAX_COMMITTED is not in the interesting mask.
        assert!(!xmax_infomask_changed(HEAP_XMAX_COMMITTED, 0));
    }

    #[test]
    fn tuplock_from_mxstatus_matches_c_table() {
        use LockTupleMode::*;
        assert_eq!(TUPLOCK_from_mxstatus(MultiXactStatus::ForKeyShare), LockTupleKeyShare);
        assert_eq!(TUPLOCK_from_mxstatus(MultiXactStatus::ForShare), LockTupleShare);
        assert_eq!(
            TUPLOCK_from_mxstatus(MultiXactStatus::ForNoKeyUpdate),
            LockTupleNoKeyExclusive
        );
        assert_eq!(TUPLOCK_from_mxstatus(MultiXactStatus::ForUpdate), LockTupleExclusive);
        assert_eq!(
            TUPLOCK_from_mxstatus(MultiXactStatus::NoKeyUpdate),
            LockTupleNoKeyExclusive
        );
        assert_eq!(TUPLOCK_from_mxstatus(MultiXactStatus::Update), LockTupleExclusive);
    }

    #[test]
    fn new_cid_record_round_trips() {
        let rec = xl_heap_new_cid {
            top_xid: 0x1234_5678,
            cmin: 11,
            cmax: 22,
            combocid: 33,
            target_locator: types_storage::storage::RelFileLocator {
                spcOid: 1,
                dbOid: 2,
                relNumber: 3,
            },
            target_tid: types_tuple::heaptuple::ItemPointerData {
                ip_blkid: types_tuple::heaptuple::BlockIdData::new(0x000A_BBCC),
                ip_posid: 7,
            },
        };
        let bytes = rec.to_bytes();
        assert_eq!(bytes.len(), SizeOfHeapNewCid);
        let back = xl_heap_new_cid::from_bytes(&bytes);
        assert_eq!(back.top_xid, rec.top_xid);
        assert_eq!(back.cmin, rec.cmin);
        assert_eq!(back.cmax, rec.cmax);
        assert_eq!(back.combocid, rec.combocid);
        assert_eq!(back.target_locator, rec.target_locator);
        assert_eq!(
            back.target_tid.ip_blkid.block_number(),
            rec.target_tid.ip_blkid.block_number()
        );
        assert_eq!(back.target_tid.ip_posid, rec.target_tid.ip_posid);
    }

    #[test]
    fn bulk_insert_state_struct_shape() {
        // The carrier mirrors C's BulkInsertStateData defaults set in
        // GetBulkInsertState (sans the strategy ring, which needs the seam).
        let bistate = BulkInsertStateData {
            strategy: BufferAccessStrategy::NONE,
            current_buf: InvalidBuffer,
            next_free: InvalidBlockNumber,
            last_free: InvalidBlockNumber,
            already_extended_by: 0,
        };
        assert_eq!(bistate.current_buf, InvalidBuffer);
        assert_eq!(bistate.next_free, InvalidBlockNumber);
    }
}
