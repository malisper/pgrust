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

pub mod catalog_drivers;
pub mod delete;
pub mod fetch;
pub mod freeze;
pub mod index_delete;
pub mod inplace;
pub mod insert;
pub mod lock;
pub mod scan;
pub mod update;

use mcx::Mcx;
use types_core::primitive::{
    InvalidBlockNumber, MultiXactId, OffsetNumber, Oid, TransactionId,
};
use types_core::xact::{CommandId, InvalidCommandId};
use types_core::XLogRecPtr;
use types_error::PgResult;
use types_rel::RelationData;
use types_storage::buf::BufferAccessStrategyType;
use types_storage::{Buffer, InvalidBuffer};
use types_tableam::tableam::LockTupleMode;
use types_tuple::heaptuple::{
    HeapTupleData, HeapTupleHeaderData, HEAP_COMBOCID, HEAP_KEYS_UPDATED,
    HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK,
    HEAP_XMAX_LOCK_ONLY,
};
use types_xlog_records::heapam_xlog::{
    xl_heap_new_cid, xl_heap_visible, SizeOfHeapNewCid, SizeOfHeapVisible,
};
use types_wal::xloginsert::{REGBUF_NO_IMAGE, REGBUF_STANDARD};
use types_xlog_records::multixact::MultiXactStatus;

// htup_details.h infomask helpers + lock-mask vocabulary already live in the
// W1 visibility crate; reuse them rather than re-deriving.
use backend_access_heap_heapam_visibility::htup::HEAP_XMAX_IS_LOCKED_ONLY;
use backend_access_heap_heapam_visibility::htup::HeapTupleHeaderGetRawXmax;
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
use backend_rmgrdesc_next::heapdesc::{XLOG_HEAP2_NEW_CID, XLOG_HEAP2_VISIBLE};

// ===========================================================================
// HeapTupleFreeze / HeapPageFreeze — NET-NEW descriptors (access/heapam.h).
// ===========================================================================

/// `HeapTupleFreeze` / `HeapPageFreeze` (`access/heapam.h`) — the per-tuple and
/// per-page freeze plans. They live in `types-vacuum` (the `access/heapam.h`
/// vocabulary, alongside `VacuumCutoffs`) so the prune/freeze + vacuum seams can
/// carry them across the cycle; re-exported here so the heap AM families keep
/// referring to them by their canonical names.
pub use types_vacuum::vacuum::{HeapPageFreeze, HeapTupleFreeze};

// ===========================================================================
// BulkInsertStateData — bulk-insert carrier (access/hio.h struct).
// ===========================================================================

/// `BulkInsertStateData` (`access/hio.h`): the state carried across a bulk
/// insert. The canonical struct lives in `types-tableam` (the dispatch layer
/// passes it through opaquely and `hio.c`'s `RelationGetBufferForTuple` reads
/// it directly); the heap AM re-exports it so `GetBulkInsertState` /
/// `heap_insert` and the hio page placement share one type.
pub use types_tableam::tableam::BulkInsertStateData;

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
    // Hand the ring handle to FreeAccessStrategy (which drops it); `.take()`
    // leaves the field `None`, matching the implicit invalidation as C `pfree`s
    // the whole BulkInsertStateData.
    bufmgr_seam::free_access_strategy::call(bistate.strategy.take());
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
pub(crate) fn TUPLOCK_from_mxstatus(status: MultiXactStatus) -> LockTupleMode {
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

// ===========================================================================
// log_heap_visible — emit the XLOG_HEAP2_VISIBLE record (heapam.c).
// ===========================================================================

/// `VISIBILITYMAP_XLOG_CATALOG_REL` (heapam_xlog.h) — the VM-bit that flags the
/// heap relation as catalog-accessible during logical decoding; carried only in
/// `xl_heap_visible.flags`.
const VISIBILITYMAP_XLOG_CATALOG_REL: u8 = 0x04;

/// `log_heap_visible(rel, heap_buffer, vm_buffer, snapshotConflictHorizon,
/// vmflags)` (heapam.c) — emit the `XLOG_HEAP2_VISIBLE` WAL record when a
/// visibility-map bit is set during VACUUM. Backup block 0 is the VM buffer;
/// backup block 1 is the heap buffer (registered with `REGBUF_NO_IMAGE` to
/// optimize away the FPI unless `XLogHintBitIsNeeded()`). Returns the record's
/// LSN.
pub fn log_heap_visible(
    rel: &RelationData,
    heap_buffer: Buffer,
    vm_buffer: Buffer,
    snapshot_conflict_horizon: TransactionId,
    vmflags: u8,
) -> PgResult<XLogRecPtr> {
    debug_assert!(heap_buffer != InvalidBuffer);
    debug_assert!(vm_buffer != InvalidBuffer);

    let mut xlrec = xl_heap_visible {
        snapshotConflictHorizon: snapshot_conflict_horizon,
        flags: vmflags,
    };
    if delete::relation_is_accessible_in_logical_decoding(rel) {
        xlrec.flags |= VISIBILITYMAP_XLOG_CATALOG_REL;
    }

    xloginsert_seam::xlog_begin_insert::call()?;
    let buf = xlrec.to_bytes();
    xloginsert_seam::xlog_register_data::call(&buf[..SizeOfHeapVisible])?;

    xloginsert_seam::xlog_register_buffer::call(0, vm_buffer, 0)?;

    let mut flags = REGBUF_STANDARD;
    if !backend_access_transam_xlog_seams::xlog_hint_bit_is_needed::call() {
        flags |= REGBUF_NO_IMAGE;
    }
    xloginsert_seam::xlog_register_buffer::call(1, heap_buffer, flags)?;

    let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP2_ID, XLOG_HEAP2_VISIBLE)?;

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
    // `HeapTupleGetUpdateXid(htup)` is C's
    //   MultiXactIdGetUpdateXid(HeapTupleHeaderGetRawXmax(tup), tup->t_infomask)
    // — it resolves a *multixact* xmax to the member update xid. It must NOT
    // route back through `HeapTupleHeaderGetUpdateXid`: that function's
    // live-multixact branch itself calls `HeapTupleGetUpdateXid` (via this very
    // seam), so wiring the seam to it forms an unbounded self-recursion that
    // stack-overflows the first time a non-lock-only multixact xmax is seen
    // (e.g. combocid's `SELECT ... FOR UPDATE` over a row already locked +
    // updated-then-rolled-back in a subxact). Resolve via the multixact owner.
    heapam_seam::heap_tuple_get_update_xid::set(|tuple| {
        multixact_seam::multi_xact_id_get_update_xid::call(
            HeapTupleHeaderGetRawXmax(tuple),
            tuple.t_infomask,
        )
    });

    // `heap_page_tuple_header(buf, offnum)` — deform the on-page
    // `HeapTupleHeader` at `(buf, offnum)`, the analog of C's
    // `(HeapTupleHeader) PageGetItem(page, PageGetItemId(page, offnum))` for a
    // normal line pointer. Bodied by `read_on_page_header`.
    heapam_seam::heap_page_tuple_header::set(|mcx, buf, offnum| {
        read_on_page_header(mcx, buf, offnum)
    });

    // `log_heap_visible(rel, heap_buffer, vm_buffer, snapshotConflictHorizon,
    // vmflags)` — emit the XLOG_HEAP2_VISIBLE record (heapam.c). Called by
    // visibilitymap_set when a VM bit is set during VACUUM.
    heapam_seam::log_heap_visible::set(|rel, heap_buffer, vm_buffer, horizon, vmflags| {
        log_heap_visible(rel, heap_buffer, vm_buffer, horizon, vmflags)
    });

    // `HeapKeyTest(tuple, RelationGetDescr(rel), nkeys, keys)` (access/valid.h)
    // — the qualified-scan scan-key evaluation (#281).
    heapam_seam::heap_key_test::set(|mcx, tuple, rel, keys| {
        scan::heap_key_test(mcx, tuple, rel, keys)
    });

    // Sequential-scan entry points for the FK phase-3 validation scan
    // (`validateForeignKeyConstraint`'s fire-the-trigger fallback): scankey-less,
    // non-parallel full-table scan under a registered MVCC snapshot.
    heapam_seam::heap_beginscan::set(|mcx, relation, snapshot, flags| {
        scan::heap_beginscan(
            mcx,
            relation,
            Some(snapshot),
            0,
            mcx::vec_with_capacity_in(mcx, 0)?,
            None,
            flags,
        )
    });
    // The seam returns an owned `FormedTuple` (deep-copied into `mcx`) rather
    // than the borrow `heap_getnext` hands back, so the caller can drive its own
    // per-row work after the scan-state borrow ends.
    heapam_seam::heap_getnext::set(|mcx, sscan| {
        match scan::heap_getnext(
            mcx,
            sscan,
            types_scan::sdir::ScanDirection::ForwardScanDirection,
        )? {
            Some(tup) => Ok(Some(tup.clone_in(mcx)?)),
            None => Ok(None),
        }
    });
    heapam_seam::heap_endscan::set(|sscan| scan::heap_endscan(sscan));

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

    // VACUUM: `HeapTupleSatisfiesVacuum(tuple, OldestXmin, buffer)` for the
    // page-resident tuple at `(buffer, offnum)` (vacuumlazy.c
    // heap_page_is_all_visible second-pass recheck). Builds the HeapTupleData
    // off the page, runs HTSV (heapam_visibility.c), returns the HTSV_Result
    // integer.
    backend_access_heap_vacuumlazy_seams::heap_tuple_satisfies_vacuum::set(
        |rel, buffer, offnum, oldest_xmin| {
            let ctx = mcx::MemoryContext::new("heap_tuple_satisfies_vacuum");
            let mcx = ctx.mcx();
            let mut tuple = read_on_page_tuple(mcx, rel, buffer, offnum)?;
            let res = backend_access_heap_heapam_visibility::HeapTupleSatisfiesVacuum(
                &mut tuple,
                oldest_xmin,
                buffer,
            )?;
            Ok(res as i32)
        },
    );

    // VACUUM header reads (vacuumlazy.c heap_page_is_all_visible): the on-page
    // `HeapTupleHeaderXminCommitted` / `HeapTupleHeaderGetXmin` for the tuple at
    // `(buffer, offnum)`.
    backend_access_heap_vacuumlazy_seams::header_xmin_committed::set(|buffer, offnum| {
        let ctx = mcx::MemoryContext::new("header_xmin_committed");
        let tuple = read_on_page_header(ctx.mcx(), buffer, offnum)?;
        // HeapTupleHeaderXminCommitted(htup) (htup_details.h): t_infomask &
        // HEAP_XMIN_COMMITTED.
        Ok((tuple.t_infomask & types_tuple::heaptuple::HEAP_XMIN_COMMITTED) != 0)
    });
    backend_access_heap_vacuumlazy_seams::header_get_xmin::set(|buffer, offnum| {
        let ctx = mcx::MemoryContext::new("header_get_xmin");
        let tuple = read_on_page_header(ctx.mcx(), buffer, offnum)?;
        Ok(backend_access_heap_heapam_visibility::htup::HeapTupleHeaderGetXmin(&tuple))
    });

    // F6 — the heapam tableam `index_delete_tuples` implementation.
    heapam_seam::heap_index_delete_tuples::set(|mcx, rel, delstate| {
        index_delete::heap_index_delete_tuples(mcx, rel, delstate)
    });

    // FETCH — the single-tuple fetch entry points. `heap_fetch` is consumed by
    // the lock family's `heap_lock_updated_tuple_rec` (update-chain walk under
    // SnapshotAny); `heap_hot_search_buffer` by `index_delete`'s
    // `heap_index_delete_tuples` (HOT-chain vacuumability test).
    heapam_seam::heap_fetch::set(|mcx, relation, snapshot, tid, keep_buf| {
        fetch::heap_fetch(mcx, relation, snapshot, tid, keep_buf)
    });
    // `heap_fetch_dirty` — the DIRTY-snapshot variant the heapam-handler DML
    // `heapam_tuple_lock` FIND_LAST_VERSION chase consumes (returns the stamped
    // SnapshotDirty.xmin/xmax).
    heapam_seam::heap_fetch_dirty::set(|mcx, relation, tid| {
        fetch::heap_fetch_dirty(mcx, relation, tid)
    });
    heapam_seam::heap_hot_search_buffer::set(
        |mcx, tid, rel, buf, snapshot, want_all_dead, first_call| {
            fetch::heap_hot_search_buffer(mcx, tid, rel, buf, snapshot, want_all_dead, first_call)
        },
    );

    // F2 INSERT — the cross-family heap-insert entry points.
    heapam_seam::heap_insert::set(|mcx, rel, tup, cid, options, bistate| {
        insert::heap_insert(mcx, rel, tup, cid, options, bistate)
    });
    heapam_seam::simple_heap_insert::set(|mcx, rel, tup| {
        insert::simple_heap_insert(mcx, rel, tup)
    });
    // F2 INSERT — the multi-row heap-insert entry point (consumed by
    // catalog/indexing.c's `CatalogTuplesMultiInsertWithInfo`).
    heapam_seam::heap_multi_insert::set(|mcx, rel, tuples, cid, options, bistate| {
        insert::heap_multi_insert(mcx, rel, tuples, cid, options, bistate)
    });
    heapam_seam::get_bulk_insert_state::set(|| crate::GetBulkInsertState());
    heapam_seam::free_bulk_insert_state::set(|bistate| {
        crate::FreeBulkInsertState(bistate);
        Ok(())
    });

    // F4 LOCK — the lock-wait primitives F0 declared as owned by this family.
    heapam_seam::heap_acquire_tuplock::set(|relation, tid, mode, wait_policy, have_tuple_lock| {
        // The seam returns C's `*have_tuple_lock`; the `acquired` bool is only
        // false under the Skip wait policy (which the blocking delete callers
        // never use), so collapse the pair to `have_tuple_lock`.
        let (_acquired, htl) =
            lock::heap_acquire_tuplock(relation, tid, mode, wait_policy, have_tuple_lock)?;
        Ok(htl)
    });
    heapam_seam::unlock_tuple_tuplock::set(|relation, tid, mode| {
        lock::unlock_tuple_tuplock(relation, tid, mode)
    });
    heapam_seam::does_multi_xact_id_conflict::set(|multi, infomask, lockmode| {
        let ctx = mcx::MemoryContext::new("does_multi_xact_id_conflict");
        let mcx = ctx.mcx();
        let c = lock::DoesMultiXactIdConflict(mcx, multi, infomask, lockmode)?;
        Ok(heapam_seam::MultiXactConflict {
            conflict: c.conflict,
            current_is_member: c.current_is_member,
        })
    });
    heapam_seam::multi_xact_id_wait::set(|multi, status, infomask, rel, tid, oper| {
        let ctx = mcx::MemoryContext::new("multi_xact_id_wait");
        let mcx = ctx.mcx();
        lock::multi_xact_id_wait(mcx, multi, status, infomask, rel, tid, oper)
    });
    // `xact_lock_table_wait` is lmgr's XactLockTableWait; the heap-AM lock
    // family owns the seam declaration and routes it to the lmgr seam.
    heapam_seam::xact_lock_table_wait::set(|xwait, rel, tid, oper| {
        lock::xact_lock_table_wait(xwait, rel, tid, oper)
    });

    // F3 DELETE — the cross-family heap-delete entry points.
    heapam_seam::heap_delete::set(
        |mcx, rel, tid, cid, crosscheck, wait, tmfd, changing_part| {
            delete::heap_delete(mcx, rel, tid, cid, crosscheck, wait, tmfd, changing_part)
        },
    );
    heapam_seam::simple_heap_delete::set(|mcx, rel, tid| {
        delete::simple_heap_delete(mcx, rel, tid)
    });
    heapam_seam::heap_abort_speculative::set(|mcx, rel, tid| {
        inplace::heap_abort_speculative(mcx, rel, tid)
    });

    // F3 UPDATE — the cross-family heap-update entry points.
    heapam_seam::heap_update::set(|mcx, rel, otid, newtup, cid, crosscheck, wait, tmfd| {
        update::heap_update(mcx, rel, otid, newtup, cid, crosscheck, wait, tmfd)
    });
    heapam_seam::simple_heap_update::set(|mcx, rel, otid, tup| {
        update::simple_heap_update(mcx, rel, otid, tup)
    });
    // Cross-family driver seams (catalog_drivers.rs): the bootstrap.c /
    // cluster.c callers and genam.c's AM-generic shim batch a small amount of
    // catalog-scan / tuple-form vocabulary the heap owner already has the
    // substrate for.
    // bootstrap.c InsertOneTuple — CreateTupleDesc + heap_form_tuple +
    // simple_heap_insert.
    heapam_seam::insert_one_tuple::set(|mcx, rel, attrtypes, values, nulls| {
        catalog_drivers::insert_one_tuple(mcx, rel, attrtypes, values, nulls)
    });
    // bootstrap.c populate_typ_list — pg_type catalog-scan driver.
    heapam_seam::read_pg_type::set(|mcx| catalog_drivers::read_pg_type(mcx));
    // cluster.c get_tables_to_cluster — pg_index indisclustered systable scan.
    heapam_seam::scan_indisclustered::set(|mcx| catalog_drivers::scan_indisclustered(mcx));
    // tablecmds.c find_typed_table_dependencies — pg_class reloftype catalog scan.
    heapam_seam::scan_typed_table_dependencies::set(|mcx, type_oid| {
        catalog_drivers::scan_typed_table_dependencies(mcx, type_oid)
    });
    // genam.c index_compute_xid_horizon_for_tuples — AM-generic
    // table_index_delete_tuples() shim over heap_index_delete_tuples.
    heapam_seam::index_compute_xid_horizon_for_tuples::set(|irel, hrel, ibuf, itemnos| {
        catalog_drivers::index_compute_xid_horizon_for_tuples(irel, hrel, ibuf, itemnos)
    });

    // Inplace-update lock/apply/unlock trio (heapam.c, ported in `inplace.rs`),
    // the primitives `systable_inplace_update_{begin,finish,cancel}` drive.
    heapam_seam::heap_inplace_lock::set(|mcx, relation, oldtup, buffer, cb| {
        inplace::heap_inplace_lock(mcx, relation, oldtup, buffer, cb)
    });
    heapam_seam::heap_inplace_update_and_unlock::set(
        |mcx, relation, oldtup, tuple, new_data, buffer| {
            inplace::heap_inplace_update_and_unlock(mcx, relation, oldtup, tuple, new_data, buffer)
        },
    );
    heapam_seam::heap_inplace_unlock::set(|relation, oldtup, buffer| {
        inplace::heap_inplace_unlock(relation, oldtup, buffer)
    });
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

/// Build a `HeapTupleData` for the normal tuple at `(buffer, offnum)` of `rel`
/// — the C `ItemPointerSet(&tuple.t_self, ...); tuple.t_data = (HeapTupleHeader)
/// PageGetItem(...); tuple.t_len = ItemIdGetLength(...); tuple.t_tableOid =
/// RelationGetRelid(rel)` shape `heap_page_is_all_visible` sets up before
/// `HeapTupleSatisfiesVacuum`.
fn read_on_page_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'_>,
    buffer: Buffer,
    offnum: OffsetNumber,
) -> PgResult<HeapTupleData<'mcx>> {
    use types_tuple::heaptuple::ItemPointerData;
    let blockno = bufmgr_seam::buffer_get_block_number::call(buffer);
    let reltableoid = rel.rd_id;
    let mut out: Option<HeapTupleData<'mcx>> = None;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = backend_storage_page::PageRef::new(page_bytes)?;
        let item_id = backend_storage_page::PageGetItemId(&page, offnum)?;
        let item = backend_storage_page::PageGetItem(&page, &item_id)?;
        let htup = HeapTupleHeaderData::read_on_page(mcx, item)?;
        let mut tup = HeapTupleData {
            t_len: backend_storage_page::ItemIdGetLength(&item_id) as u32,
            t_self: ItemPointerData::default(),
            t_tableOid: reltableoid,
            t_data: Some(mcx::alloc_in(mcx, htup)?),
        };
        backend_storage_page::ItemPointerSet(&mut tup.t_self, blockno, offnum);
        out = Some(tup);
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
            strategy: None,
            current_buf: InvalidBuffer,
            next_free: InvalidBlockNumber,
            last_free: InvalidBlockNumber,
            already_extended_by: 0,
        };
        assert_eq!(bistate.current_buf, InvalidBuffer);
        assert_eq!(bistate.next_free, InvalidBlockNumber);
    }
}
