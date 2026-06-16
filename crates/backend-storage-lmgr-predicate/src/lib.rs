//! Faithful port of `src/backend/storage/lmgr/predicate.c` — PostgreSQL's
//! Serializable Snapshot Isolation (SSI) predicate-locking engine, over the
//! real ported shmem / dynahash / LWLock / SLRU substrate.
//!
//! The engine ([`engine`]) is a C-faithful raw-pointer transliteration; the
//! [`internals`] structs are `#[repr(C)]` field-for-field with
//! `storage/predicate_internals.h`; [`ilist_inline`] carries the `lib/ilist.h`
//! inline `dlist` helpers; [`serial`] is the `pg_serial` SLRU.
//!
//! The public C-named entry points take the data the C reads off `Relation`
//! (db OID, rel OID, temp-buffer flag, index `indrelid`); the seam wrappers
//! installed from [`init_seams`] project that out of the relcache by OID.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

#[macro_use]
mod ilist_inline;
pub mod engine;
pub mod globals;
pub mod internals;
pub mod serial;

pub use engine::*;
pub use internals::{
    PredicateLockData, SerCommitSeqNo, SerializableXactScalars, FirstNormalSerCommitSeqNo,
    InvalidSerCommitSeqNo, RecoverySerCommitSeqNo,
};
pub use serial::CheckPointPredicate;

use types_core::primitive::{BlockNumber, Oid};
use types_error::{PgError, PgResult};
use types_snapshot::snapshot::SnapshotData;

// ---------------------------------------------------------------------------
// Relation-field projection from the relcache, by OID.
// ---------------------------------------------------------------------------

/// The subset of `Relation` fields predicate.c reads.
struct RelFields {
    db_oid: Oid,
    rd_id: Oid,
    uses_local_buffers: bool,
    /// `relation->rd_index` — `Some(indrelid)` if this is an index.
    rd_index_indrelid: Option<Oid>,
}

/// Resolve `RelationIdGetRelation(oid)`'s field projection through the shared
/// relcache cell (no `Mcx` needed). `ereport(ERROR)`s on a missing relation,
/// mirroring `RelationIdGetRelation` returning NULL → caller crash.
fn rel_fields(oid: Oid) -> PgResult<RelFields> {
    let cell = backend_utils_cache_relcache_seams::relation_id_get_relation_shared::call(oid)?;
    let cell = cell.ok_or_else(|| {
        PgError::error(format!(
            "could not open relation with OID {oid} for predicate locking"
        ))
    })?;
    let entry = cell.borrow();
    let fields = RelFields {
        db_oid: entry.rd_locator.dbOid,
        rd_id: entry.rd_id,
        uses_local_buffers: entry.uses_local_buffers(),
        rd_index_indrelid: entry.rd_index.as_ref().map(|i| i.indrelid),
    };
    drop(entry);
    // Release the pin taken by relation_id_get_relation_shared.
    backend_utils_cache_relcache_seams::relation_close::call(oid)?;
    Ok(fields)
}

// ---------------------------------------------------------------------------
// Seam installers (init_seams) — adapt the seam decl sigs to the engine.
// ---------------------------------------------------------------------------

/// Install every seam this unit owns. Wired from `crates/seams-init`.
pub fn init_seams() {
    use backend_storage_lmgr_predicate_seams as seams;

    seams::predicate_lock_page::set(|relation, blkno, snapshot| {
        // The page-lock seam carries a real Relation; read its fields directly.
        let (db, rd_id, ult) = relation_fields_from_handle(&relation)?;
        let snap = snapshot
            .as_ref()
            .map(|rc| (**rc).clone())
            .unwrap_or_else(special_snapshot);
        engine::PredicateLockPage(db, rd_id, ult, blkno, &snap)
    });

    seams::predicate_lock_page_split::set(|index_oid, old_blkno, new_blkno| {
        let f = rel_fields(index_oid)?;
        engine::PredicateLockPageSplit(
            f.db_oid,
            f.rd_id,
            f.uses_local_buffers,
            old_blkno,
            new_blkno,
        )
    });

    seams::check_for_serializable_conflict_in_page::set(|index_oid, blkno| {
        let f = rel_fields(index_oid)?;
        engine::CheckForSerializableConflictIn(
            f.db_oid,
            f.rd_id,
            f.uses_local_buffers,
            None,
            blkno,
        )
    });

    seams::get_serializable_transaction_snapshot::set(|snapshot| {
        engine::GetSerializableTransactionSnapshot(snapshot)
    });

    seams::set_serializable_transaction_snapshot::set(|snapshot, sourcevxid, sourcepid| {
        engine::SetSerializableTransactionSnapshot(snapshot, sourcevxid, sourcepid)
    });

    seams::predicate_lock_relation::set(|index_oid, snapshot| {
        let f = rel_fields(index_oid)?;
        engine::PredicateLockRelation(f.db_oid, f.rd_id, f.uses_local_buffers, snapshot)
    });

    seams::check_for_serializable_conflict_out_needed::set(|relation_oid, snapshot| {
        let f = match rel_fields(relation_oid) {
            Ok(f) => f,
            // The predicate is infallible in C; a missing relation simply means
            // no checking is needed.
            Err(_) => return false,
        };
        engine::CheckForSerializableConflictOutNeeded(f.rd_id, f.uses_local_buffers, snapshot)
            .unwrap_or(false)
    });

    seams::heap_check_for_serializable_conflict_out::set(
        |visible, relation_oid, tuple, _buffer, snapshot| {
            // HeapCheckForSerializableConflictOut(visible, rel, tuple, buf, snap):
            // when !visible the tuple's xmax (deleting/locking xact) is the
            // conflict xid; when visible it's the xmin (inserting xact). C's
            // wrapper resolves the top xid via SubTransGetTopmostTransaction;
            // here we pass the tuple's relevant raw xid as the conflict xid.
            let f = rel_fields(relation_oid)?;
            let xid = match &tuple.t_data {
                None => return Ok(()),
                Some(hdr) => {
                    if visible {
                        types_tuple::heaptuple::HeapTupleHeaderGetXmin(hdr)
                    } else {
                        // !visible: the deleting/locking xact is the conflict.
                        match &hdr.t_choice {
                            types_tuple::heaptuple::HeapTupleHeaderChoice::THeap(t_heap) => {
                                t_heap.t_xmax
                            }
                            types_tuple::heaptuple::HeapTupleHeaderChoice::TDatum(_) => {
                                return Ok(())
                            }
                        }
                    }
                }
            };
            if !types_core::xact::TransactionIdIsValid(xid) {
                return Ok(());
            }
            engine::CheckForSerializableConflictOut(f.rd_id, f.uses_local_buffers, xid, snapshot)
        },
    );

    seams::check_for_serializable_conflict_in::set(|index_oid| {
        let f = rel_fields(index_oid)?;
        engine::CheckForSerializableConflictIn(
            f.db_oid,
            f.rd_id,
            f.uses_local_buffers,
            None,
            engine_invalid_block(),
        )
    });

    seams::predicatelock_twophase_recover::set(|xid, info, recdata| {
        engine::predicatelock_twophase_recover(xid, info, recdata)
    });

    seams::register_predicate_locking_xid::set(|xid| engine::RegisterPredicateLockingXid(xid));

    seams::pre_commit_check_for_serialization_failure::set(|| {
        engine::PreCommit_CheckForSerializationFailure()
    });

    seams::at_prepare_predicate_locks::set(|| engine::AtPrepare_PredicateLocks());

    seams::post_prepare_predicate_locks::set(|xid| engine::PostPrepare_PredicateLocks(xid));

    seams::predicate_lock_twophase_finish::set(|xid, is_commit| {
        engine::PredicateLockTwoPhaseFinish(xid, is_commit)
    });

    seams::transfer_predicate_locks_to_heap_relation::set(|relid| {
        let f = rel_fields(relid)?;
        engine::TransferPredicateLocksToHeapRelation(
            f.db_oid,
            f.rd_id,
            f.rd_id,
            f.uses_local_buffers,
            f.rd_index_indrelid,
        )
    });

    seams::predicate_lock_shmem_size::set(|| engine::PredicateLockShmemSize());

    seams::predicate_lock_shmem_init::set(|| engine::PredicateLockShmemInit());

    seams::predicate_lock_tid::set(|relation_oid, tid, snapshot, tuple_xid| {
        let f = rel_fields(relation_oid)?;
        let (blkno, offnum) = item_pointer_parts(&tid);
        engine::PredicateLockTID(
            f.db_oid,
            f.rd_id,
            f.uses_local_buffers,
            f.rd_index_indrelid.is_some(),
            blkno,
            offnum,
            snapshot,
            tuple_xid,
        )
    });

    // GUC variable backing storage owned by predicate.c, read at
    // shmem-sizing time (PredicateLockShmemSize): `serializable_buffers`
    // directly and `max_predicate_locks_per_xact` via the NPREDICATELOCKTARGETENTS
    // macro.
    {
        use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
        vars::serializable_buffers.install(GucVarAccessors {
            get: globals::serializable_buffers,
            set: globals::set_serializable_buffers,
        });
        vars::max_predicate_locks_per_xact.install(GucVarAccessors {
            get: globals::max_predicate_locks_per_xact,
            set: globals::set_max_predicate_locks_per_xact,
        });
    }
}

#[inline]
fn engine_invalid_block() -> BlockNumber {
    types_core::primitive::InvalidBlockNumber
}

/// A non-MVCC sentinel snapshot for the page-lock seam's `None` case
/// (`IsMVCCSnapshot` is false ⇒ SerializationNeededForRead returns early).
fn special_snapshot() -> SnapshotData {
    SnapshotData::sentinel(types_snapshot::snapshot::SnapshotType::SNAPSHOT_NON_VACUUMABLE)
}

/// Read the predicate-relevant fields from a live `Relation` handle.
fn relation_fields_from_handle(rel: &types_rel::Relation<'_>) -> PgResult<(Oid, Oid, bool)> {
    backend_utils_cache_relcache_seams::relation_with_entry(rel, |e| {
        (e.rd_locator.dbOid, e.rd_id, e.uses_local_buffers())
    })
    .ok_or_else(|| PgError::error("predicate lock: relation handle has no relcache entry"))
}

/// Decompose an `ItemPointerData` into (block, offset).
fn item_pointer_parts(
    tid: &types_tuple::heaptuple::ItemPointerData,
) -> (BlockNumber, types_core::primitive::OffsetNumber) {
    (tid.ip_blkid.block_number(), tid.ip_posid)
}
