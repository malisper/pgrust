//! `executor/execReplication.c` — miscellaneous executor routines for logical
//! replication (the apply-side executor).
//!
//! The apply worker (`worker.c`/`tablesync.c`) owns the `EState`, `EPQState`,
//! `ResultRelInfo`, and the search/output `TupleTableSlot`s; the executor-owned
//! structs are real values addressed by id into the [`EStateData`] pools
//! (`RriId`/`SlotId`) — the same model `execIndexing` / `conflict` /
//! `nodeModifyTable` use. The plain `Relation` + standalone-slot routines
//! (`RelationFindReplTupleByIndex` / `RelationFindReplTupleSeq`) take the
//! caller-owned `Relation` and `SlotData` directly, as their C signatures do.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::format;
use alloc::string::ToString;

use mcx::{Mcx, PgVec};

use backend_utils_error::ereport;
use types_error::ErrorLocation;
use types_error::error::{
    ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_T_R_SERIALIZATION_FAILURE, ERRCODE_UNDEFINED_FUNCTION, ERRCODE_WRONG_OBJECT_TYPE,
    ERROR, LOG,
};
use types_error::PgResult;

use types_core::primitive::{AttrNumber, Oid, RegProcedure};
use types_nodes::execnodes::{EPQState, EStateData, RriId, SlotId};
use types_nodes::nodes::CmdType;
use types_rel::Relation;
use types_scan::scankey::{ScanKeyData, SK_ISNULL, SK_SEARCHNULL};
use types_scan::sdir::ScanDirection;
use types_slot::SlotData;
use types_sortsupport::COMPARE_EQ;
use types_tableam::tableam::{
    LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result, TU_UpdateIndexes,
};

use types_replication::conflict::ConflictType;

// Direct (acyclic) callees.
use backend_access_index_indexam as indexam;
use backend_access_table_tableam as tableam;
use backend_executor_execTuples as execTuples;
use backend_replication_logical_conflict as conflict;
use backend_replication_logical_conflict::ConflictTupleInfo;
use backend_replication_logical_relation as logicalrelation;
use backend_utils_adt_format_type::format_type_be_owned;
use backend_utils_misc_guc_tables::vars;

// Inward seams (owned by this unit).
use backend_executor_execReplication_seams as inward;

// Outward seams.
use backend_access_index_amapi_seams as amapi_seams;
use backend_access_table_tableam_seams as tableam_seams;
use backend_access_transam_xact_seams as xact_seams;
use backend_catalog_catalog_seams as catalog_seams;
use backend_catalog_pg_class_seams as pg_class_seams;
use backend_commands_trigger_seams as trigger_seams;
use backend_executor_execIndexing_seams as indexing_seams;
use backend_executor_execMain_seams as execMain_seams;
use backend_executor_execTuples_seams as execTuples_seams;
use backend_executor_nodeModifyTable_seams as modifytable_seams;
use backend_replication_logical_worker_seams as worker_seams;
use backend_storage_lmgr_lmgr_seams as lmgr_seams;
use backend_utils_time_snapmgr_seams as snapmgr_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_cache_relcache_nodexform_seams as relcache_nodexform_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_cache_typcache_seams as typcache_seams;
use backend_utils_fmgr_fmgr_seams as fmgr_seams;

// ---------------------------------------------------------------------------
// Constants from headers.
// ---------------------------------------------------------------------------

/// `RELKIND_RELATION` (`catalog/pg_class.h`) — `'r'`.
const RELKIND_RELATION: u8 = b'r';
/// `RELKIND_PARTITIONED_TABLE` (`catalog/pg_class.h`) — `'p'`.
const RELKIND_PARTITIONED_TABLE: u8 = b'p';
/// `REPLICA_IDENTITY_FULL` (`catalog/pg_class.h`) — `'f'`.
const REPLICA_IDENTITY_FULL: u8 = b'f';

/// `RowExclusiveLock` (`storage/lockdefs.h`) — lock taken on the index.
const ROW_EXCLUSIVE_LOCK: types_storage::lock::LOCKMODE = 3;
/// `NoLock` (`storage/lockdefs.h`).
const NO_LOCK: types_storage::lock::LOCKMODE = 0;

fn err_here() -> ErrorLocation {
    ErrorLocation::new("execReplication.c", 0, "execReplication")
}

/// `elog(ERROR, ...)` internal message helper.
fn elog_internal(msg: alloc::string::String) -> types_error::PgError {
    ereport(ERROR).errmsg_internal(msg).into_error()
}

/// `TransactionIdIsValid(xid)` — `xid != InvalidTransactionId` (transam.h).
#[inline]
fn transaction_id_is_valid(xid: u32) -> bool {
    xid != 0
}

/// `AttributeNumberIsValid(attno)` — `attno != InvalidAttrNumber`.
#[inline]
fn attribute_number_is_valid(attno: AttrNumber) -> bool {
    attno != 0
}

/// `OidIsValid(oid)` — `oid != InvalidOid`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != 0
}

// ===========================================================================
// build_replindex_scan_key
// ===========================================================================

/// Set up the `ScanKey`s for a search in `rel` for a tuple `searchslot` that is
/// set up to match `rel` (*NOT* `idxrel`). Returns the scan keys (length =
/// how many columns to use for the index scan).
///
/// `idxrel` must be a PK, RI, or an index usable for REPLICA IDENTITY FULL.
fn build_replindex_scan_key<'mcx>(
    mcx: Mcx<'mcx>,
    _rel: &Relation<'mcx>,
    idxrel: &Relation<'mcx>,
    searchslot: &SlotData<'mcx>,
) -> PgResult<PgVec<'mcx, ScanKeyData<'mcx>>> {
    // indclassDatum = SysCacheGetAttrNotNull(INDEXRELID, idxrel->rd_indextuple,
    //                                        Anum_pg_index_indclass);
    // opclass = (oidvector *) DatumGetPointer(indclassDatum);
    // int2vector *indkey = &idxrel->rd_index->indkey;
    //
    // The relcache's `rd_indextuple` is the syscache pg_index row; the
    // variable-length `indkey`/`indclass` vectors are read off it (the trimmed
    // `rd_index` only carries the fixed scalars), so this projection mirrors
    // the C's `SysCacheGetAttrNotNull(INDEXRELID, ...)` reads.
    let idxinfo = syscache_seams::search_pg_index_info::call(mcx, idxrel.rd_id)?
        .ok_or_else(|| elog_internal(format!("cache lookup failed for index {}", idxrel.rd_id)))?;
    let opclass = &idxinfo.indclass;
    let indkey = &idxinfo.indkey;

    let mut skey: PgVec<'mcx, ScanKeyData<'mcx>> = PgVec::new_in(mcx);

    // Build scankey for every non-expression attribute in the index.
    //   for (index_attoff = 0;
    //        index_attoff < IndexRelationGetNumberOfKeyAttributes(idxrel);
    //        index_attoff++)
    let nkeyatts = idxrel.indnkeyatts();
    for index_attoff in 0..nkeyatts as usize {
        // int table_attno = indkey->values[index_attoff];
        let table_attno = indkey[index_attoff];

        if !attribute_number_is_valid(table_attno) {
            // XXX: Currently, we don't support expressions in the scan key.
            continue;
        }

        // Load the operator info. We need this to get the equality operator
        // function for the scan key.
        //   optype = get_opclass_input_type(opclass->values[index_attoff]);
        let optype = lsyscache_seams::get_opclass_input_type::call(opclass[index_attoff])?;
        //   opfamily = get_opclass_family(opclass->values[index_attoff]);
        let opfamily = lsyscache_seams::get_opclass_family::call(opclass[index_attoff])?;
        //   eq_strategy = IndexAmTranslateCompareType(COMPARE_EQ,
        //                     idxrel->rd_rel->relam, opfamily, false);
        let eq_strategy = amapi_seams::index_am_translate_cmptype::call(
            COMPARE_EQ,
            idxrel.rd_rel.relam,
            opfamily,
            false,
        )?;
        //   operator = get_opfamily_member(opfamily, optype, optype, eq_strategy);
        let operator =
            lsyscache_seams::get_opfamily_member::call(opfamily, optype, optype, eq_strategy)?;

        if !oid_is_valid(operator) {
            // elog(ERROR, "missing operator %d(%u,%u) in opfamily %u", ...)
            return Err(elog_internal(format!(
                "missing operator {eq_strategy}({optype},{optype}) in opfamily {opfamily}"
            )));
        }

        // regop = get_opcode(operator);
        let regop: RegProcedure = lsyscache_seams::get_opcode::call(operator)?;

        // Initialize the scankey.
        //   ScanKeyInit(&skey[skey_attoff], index_attoff + 1, eq_strategy,
        //               regop, searchslot->tts_values[table_attno - 1]);
        let argument = searchslot.base().tts_values[(table_attno - 1) as usize].clone_in(mcx)?;
        let mut entry = ScanKeyData::empty();
        backend_access_common_scankey::ScanKeyInit(
            &mut entry,
            (index_attoff + 1) as AttrNumber,
            eq_strategy as types_scan::scankey::StrategyNumber,
            regop,
            argument,
        )?;

        // skey[skey_attoff].sk_collation = idxrel->rd_indcollation[index_attoff];
        entry.sk_collation = idxrel.rd_indcollation[index_attoff];

        // Check for null value.
        //   if (searchslot->tts_isnull[table_attno - 1])
        //       skey[skey_attoff].sk_flags |= (SK_ISNULL | SK_SEARCHNULL);
        if searchslot.base().tts_isnull[(table_attno - 1) as usize] {
            entry.sk_flags |= SK_ISNULL | SK_SEARCHNULL;
        }

        skey.try_reserve(1).map_err(|_| mcx.oom(1))?;
        skey.push(entry);
    }

    // There must always be at least one attribute for the index scan.
    debug_assert!(!skey.is_empty());

    Ok(skey)
}

// ===========================================================================
// should_refetch_tuple
// ===========================================================================

/// Check if it is necessary to re-fetch and lock the tuple due to concurrent
/// modifications. Called after invoking `table_tuple_lock`.
fn should_refetch_tuple(res: TM_Result, tmfd: &TM_FailureData) -> PgResult<bool> {
    let mut refetch = false;

    match res {
        TM_Result::TM_Ok => {}
        TM_Result::TM_Updated => {
            // XXX: Improve handling here
            if item_pointer_indicates_moved_partitions(tmfd) {
                ereport(LOG)
                    .errcode(ERRCODE_T_R_SERIALIZATION_FAILURE)
                    .errmsg("tuple to be locked was already moved to another partition due to concurrent update, retrying")
                    .finish(err_here())?;
            } else {
                ereport(LOG)
                    .errcode(ERRCODE_T_R_SERIALIZATION_FAILURE)
                    .errmsg("concurrent update, retrying")
                    .finish(err_here())?;
            }
            refetch = true;
        }
        TM_Result::TM_Deleted => {
            // XXX: Improve handling here
            ereport(LOG)
                .errcode(ERRCODE_T_R_SERIALIZATION_FAILURE)
                .errmsg("concurrent delete, retrying")
                .finish(err_here())?;
            refetch = true;
        }
        TM_Result::TM_Invisible => {
            return Err(elog_internal("attempted to lock invisible tuple".to_string()));
        }
        other => {
            return Err(elog_internal(format!(
                "unexpected table_tuple_lock status: {}",
                other as u32
            )));
        }
    }

    Ok(refetch)
}

/// `ItemPointerIndicatesMovedPartitions(&tmfd->ctid)` (itemptr.h): the failure
/// ctid was set to `MovedPartitionsBlockNumber`/`MovedPartitionsOffsetNumber`
/// to flag a row moved to another partition by a concurrent update.
fn item_pointer_indicates_moved_partitions(tmfd: &TM_FailureData) -> bool {
    // MovedPartitionsOffsetNumber == 0xfffd, MovedPartitionsBlockNumber == InvalidBlockNumber.
    const MOVED_PARTITIONS_OFFSET_NUMBER: u16 = 0xfffd;
    const MOVED_PARTITIONS_BLOCK_NUMBER: u32 = 0xffff_ffff;
    tmfd.ctid.ip_posid == MOVED_PARTITIONS_OFFSET_NUMBER
        && tmfd.ctid.ip_blkid.block_number() == MOVED_PARTITIONS_BLOCK_NUMBER
}

// ===========================================================================
// RelationFindReplTupleByIndex
// ===========================================================================

/// Search `rel` for a tuple using the index `idxoid`. If a matching tuple is
/// found, lock it with `lockmode`, fill `outslot`, and return true.
pub fn RelationFindReplTupleByIndex<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    idxoid: Oid,
    lockmode: LockTupleMode,
    searchslot: &mut SlotData<'mcx>,
    outslot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // TypeCacheEntry **eq = NULL; — lazily allocated per-attribute eq cache. We
    // cache the resolved equality function OID (the analog of caching the
    // TypeCacheEntry pointer in C).
    let mut eq: alloc::vec::Vec<Oid> = alloc::vec::Vec::new();

    // idxrel = index_open(idxoid, RowExclusiveLock);
    let idxrel = indexam::index_open(mcx, idxoid, ROW_EXCLUSIVE_LOCK)?;

    // isIdxSafeToSkipDuplicates = (GetRelationIdentityOrPK(rel) == idxoid);
    let is_idx_safe_to_skip_duplicates =
        logicalrelation::GetRelationIdentityOrPK(rel)? == idxoid;

    // InitDirtySnapshot(snap);
    let snap = types_snapshot::SnapshotData::sentinel(types_snapshot::snapshot::SnapshotType::SNAPSHOT_DIRTY);

    // skey_attoff = build_replindex_scan_key(skey, rel, idxrel, searchslot);
    let skey = build_replindex_scan_key(mcx, rel, &idxrel, searchslot)?;
    let skey_attoff = skey.len() as i32;

    // scan = index_beginscan(rel, idxrel, &snap, NULL, skey_attoff, 0);
    let mut scan = indexam::index_beginscan(mcx, rel, &idxrel, snap, None, skey_attoff, 0)?;

    let mut found;
    'retry: loop {
        found = false;

        // index_rescan(scan, skey, skey_attoff, NULL, 0);
        let keys: alloc::vec::Vec<ScanKeyData<'mcx>> = {
            let mut v = alloc::vec::Vec::new();
            v.try_reserve(skey.len()).map_err(|_| mcx.oom(skey.len()))?;
            for k in skey.iter() {
                v.push(k.clone_in(mcx)?);
            }
            v
        };
        indexam::index_rescan(mcx, &mut scan, &keys, skey_attoff, &[], 0)?;

        // while (index_getnext_slot(scan, ForwardScanDirection, outslot))
        while indexam::index_getnext_slot(mcx, &mut scan, ScanDirection::ForwardScanDirection, outslot)? {
            // Avoid expensive equality check if PK / replica identity index.
            if !is_idx_safe_to_skip_duplicates {
                if eq.is_empty() {
                    // eq = palloc0(sizeof(*eq) * outslot->tts_tupleDescriptor->natts);
                    let natts = slot_natts(outslot);
                    eq = alloc::vec![0 as Oid; natts];
                }

                // if (!tuples_equal(outslot, searchslot, eq)) continue;
                if !tuples_equal(mcx, outslot, searchslot, &mut eq)? {
                    continue;
                }
            }

            // ExecMaterializeSlot(outslot);
            execTuples::slot_store_fetch::ExecMaterializeSlot(mcx, outslot)?;

            // xwait = TransactionIdIsValid(snap.xmin) ? snap.xmin : snap.xmax;
            let snap_now = scan
                .xs_snapshot
                .as_ref()
                .expect("index scan carries the dirty snapshot");
            let xwait = if transaction_id_is_valid(snap_now.xmin) {
                snap_now.xmin
            } else {
                snap_now.xmax
            };

            // If the tuple is locked, wait for locking xact to finish and retry.
            if transaction_id_is_valid(xwait) {
                lmgr_seams::xact_lock_table_wait::call(
                    xwait,
                    rel.name().to_string(),
                    outslot.base().tts_tid,
                    types_storage::lock::XLTW_Oper::None,
                )?;
                continue 'retry;
            }

            // Found our tuple and it's not locked.
            found = true;
            break;
        }

        // Found tuple, try to lock it in the lockmode.
        if found {
            found = lock_found_tuple(mcx, rel, lockmode, outslot)?;
            if !found {
                // should_refetch_tuple requested a retry.
                continue 'retry;
            }
        }

        break;
    }

    // index_endscan(scan);
    indexam::index_endscan(mcx, scan)?;

    // Don't release lock until commit.
    //   index_close(idxrel, NoLock);
    indexam::index_close(idxrel, NO_LOCK)?;

    Ok(found)
}

/// The "Found tuple, try to lock it" block shared by the index/seq scans.
/// Returns `Ok(true)` when the tuple is locked and we are done; `Ok(false)`
/// when `should_refetch_tuple` requested a `goto retry`.
fn lock_found_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    lockmode: LockTupleMode,
    outslot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // PushActiveSnapshot(GetLatestSnapshot());
    let latest = snapmgr_seams::get_latest_snapshot::call()?;
    snapmgr_seams::push_active_snapshot::call(alloc::rc::Rc::new(latest))?;
    let active = snapmgr_seams::get_active_snapshot::call()?;

    // res = table_tuple_lock(rel, &outslot->tts_tid, GetActiveSnapshot(),
    //          outslot, GetCurrentCommandId(false), lockmode, LockWaitBlock,
    //          0 /* don't follow updates */, &tmfd);
    let cid = xact_seams::get_current_command_id::call(false)?;
    let tid = outslot.base().tts_tid;
    let mut tmfd = TM_FailureData::default();
    let active_val = active.map(|s| (*s).clone());
    let res = tableam::table_tuple_lock(
        mcx,
        rel,
        &tid,
        &active_val,
        outslot,
        cid,
        lockmode,
        LockWaitPolicy::LockWaitBlock,
        0,
        &mut tmfd,
    )?;

    // PopActiveSnapshot();
    snapmgr_seams::pop_active_snapshot::call()?;

    // if (should_refetch_tuple(res, &tmfd)) goto retry;
    Ok(!should_refetch_tuple(res, &tmfd)?)
}

// ===========================================================================
// tuples_equal
// ===========================================================================

/// Compare the tuples in `slot1` and `slot2` by checking if they have equal
/// values. `eq` caches the per-attribute equality function OID (`0` =
/// not-yet-resolved, the C `NULL` TypeCacheEntry pointer).
fn tuples_equal<'mcx>(
    mcx: Mcx<'mcx>,
    slot1: &mut SlotData<'mcx>,
    slot2: &mut SlotData<'mcx>,
    eq: &mut [Oid],
) -> PgResult<bool> {
    debug_assert_eq!(slot_natts(slot1), slot_natts(slot2));

    // slot_getallattrs(slot1); slot_getallattrs(slot2);
    execTuples_seams::slot_getallattrs::call(mcx, slot1)?;
    execTuples_seams::slot_getallattrs::call(mcx, slot2)?;

    let natts = slot_natts(slot1);
    for attrnum in 0..natts {
        // att = TupleDescAttr(slot1->tts_tupleDescriptor, attrnum);
        let att = {
            let desc = slot1
                .base()
                .tts_tupleDescriptor
                .as_ref()
                .expect("slot has a tuple descriptor");
            *desc.attr(attrnum)
        };

        // Ignore dropped and generated columns (publisher doesn't send those).
        //   if (att->attisdropped || att->attgenerated) continue;
        if att.attisdropped || att.attgenerated != 0 {
            continue;
        }

        let isnull1 = slot1.base().tts_isnull[attrnum];
        let isnull2 = slot2.base().tts_isnull[attrnum];

        // If one value is NULL and the other is not, certainly not equal.
        if isnull1 != isnull2 {
            return Ok(false);
        }

        // If both are NULL, they can be considered equal.
        if isnull1 || isnull2 {
            continue;
        }

        // typentry = eq[attrnum]; if (typentry == NULL) { ... }
        let mut eq_fn_oid = eq[attrnum];
        if !oid_is_valid(eq_fn_oid) {
            // typentry = lookup_type_cache(att->atttypid, TYPECACHE_EQ_OPR_FINFO);
            // if (!OidIsValid(typentry->eq_opr_finfo.fn_oid)) ereport(ERROR, ...);
            eq_fn_oid = typcache_seams::lookup_element_eq_opr::call(att.atttypid)?;
            if !oid_is_valid(eq_fn_oid) {
                let typename = format_type_be_owned(att.atttypid)?;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_FUNCTION)
                    .errmsg(format!(
                        "could not identify an equality operator for type {typename}"
                    ))
                    .into_error());
            }
            // eq[attrnum] = typentry;
            eq[attrnum] = eq_fn_oid;
        }

        // if (!DatumGetBool(FunctionCall2Coll(&typentry->eq_opr_finfo,
        //         att->attcollation, slot1->tts_values[attrnum],
        //         slot2->tts_values[attrnum]))) return false;
        let arg1 = slot1.base().tts_values[attrnum].clone_in(mcx)?;
        let arg2 = slot2.base().tts_values[attrnum].clone_in(mcx)?;
        let res = fmgr_seams::function_call2_coll_datum::call(
            mcx,
            eq_fn_oid,
            att.attcollation,
            arg1,
            arg2,
        )?;
        if !res.as_bool() {
            return Ok(false);
        }
    }

    Ok(true)
}

/// `slot->tts_tupleDescriptor->natts`.
fn slot_natts(slot: &SlotData<'_>) -> usize {
    slot.base()
        .tts_tupleDescriptor
        .as_ref()
        .expect("slot has a tuple descriptor")
        .natts as usize
}

// ===========================================================================
// RelationFindReplTupleSeq
// ===========================================================================

/// Search `rel` for a tuple using a sequential scan. Stops at the first match.
pub fn RelationFindReplTupleSeq<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    lockmode: LockTupleMode,
    searchslot: &mut SlotData<'mcx>,
    outslot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // Assert(equalTupleDescs(RelationGetDescr(rel), outslot->tts_tupleDescriptor));

    // eq = palloc0(sizeof(*eq) * outslot->tts_tupleDescriptor->natts);
    let natts = slot_natts(outslot);
    let mut eq: alloc::vec::Vec<Oid> = alloc::vec![0 as Oid; natts];

    // InitDirtySnapshot(snap);
    let snap = types_snapshot::SnapshotData::sentinel(types_snapshot::snapshot::SnapshotType::SNAPSHOT_DIRTY);

    // scan = table_beginscan(rel, &snap, 0, NULL);
    let mut scan = tableam_seams::table_beginscan::call(mcx, rel, alloc::rc::Rc::new(snap))?;
    // scanslot = table_slot_create(rel, NULL);
    let mut scanslot = tableam::table_slot_create(mcx, rel)?;

    let mut found;
    'retry: loop {
        found = false;

        // table_rescan(scan, NULL);
        tableam_seams::table_rescan::call(mcx, &mut scan)?;

        // while (table_scan_getnextslot(scan, ForwardScanDirection, scanslot))
        while tableam_seams::table_scan_getnextslot_direction::call(
            mcx,
            &mut scan,
            ScanDirection::ForwardScanDirection,
            &mut scanslot,
        )? {
            // if (!tuples_equal(scanslot, searchslot, eq)) continue;
            if !tuples_equal(mcx, &mut scanslot, searchslot, &mut eq)? {
                continue;
            }

            found = true;
            // ExecCopySlot(outslot, scanslot);
            execTuples::slot_store_fetch::ExecCopySlot(mcx, outslot, &mut scanslot)?;

            // xwait = TransactionIdIsValid(snap.xmin) ? snap.xmin : snap.xmax;
            let snap_now = scan
                .rs_snapshot
                .as_ref()
                .expect("table scan carries the dirty snapshot");
            let xwait = if transaction_id_is_valid(snap_now.xmin) {
                snap_now.xmin
            } else {
                snap_now.xmax
            };

            if transaction_id_is_valid(xwait) {
                lmgr_seams::xact_lock_table_wait::call(
                    xwait,
                    rel.name().to_string(),
                    outslot.base().tts_tid,
                    types_storage::lock::XLTW_Oper::None,
                )?;
                continue 'retry;
            }

            // Found our tuple and it's not locked.
            break;
        }

        // Found tuple, try to lock it in the lockmode.
        if found {
            found = lock_found_tuple(mcx, rel, lockmode, outslot)?;
            if !found {
                continue 'retry;
            }
        }

        break;
    }

    // table_endscan(scan);
    tableam::table_endscan(scan)?;
    // ExecDropSingleTupleTableSlot(scanslot);
    execTuples::exec_init_slots::ExecDropSingleTupleTableSlot(scanslot)?;

    Ok(found)
}

// ===========================================================================
// Conflict detection helpers
// ===========================================================================

/// Build additional index information necessary for conflict detection.
fn BuildConflictIndexInfo<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    conflictindex: Oid,
) -> PgResult<()> {
    // for (int i = 0; i < resultRelInfo->ri_NumIndices; i++)
    let n = estate.result_rel(relinfo).ri_NumIndices;
    for i in 0..n as usize {
        // Relation indexRelation = resultRelInfo->ri_IndexRelationDescs[i];
        let index_relid = {
            let rri = estate.result_rel(relinfo);
            let descs = rri
                .ri_IndexRelationDescs
                .as_ref()
                .expect("ri_IndexRelationDescs set when ri_NumIndices > 0");
            descs[i]
                .as_ref()
                .map(|r| r.rd_id)
                .expect("ri_IndexRelationDescs[i] open")
        };

        // if (conflictindex != RelationGetRelid(indexRelation)) continue;
        if conflictindex != index_relid {
            continue;
        }

        // Take the descriptor + IndexInfo out so we can hand the builder a live
        // `&Relation` / `&mut IndexInfo` without aliasing the pool.
        let index_relation = take_index_desc(estate, relinfo, i)
            .expect("ri_IndexRelationDescs[i] open");
        let mut index_relation_info = take_index_info(estate, relinfo, i);

        // Assert(indexRelationInfo->ii_UniqueOps == NULL);
        debug_assert!(index_relation_info.ii_UniqueOps.is_none());

        // BuildSpeculativeIndexInfo(indexRelation, indexRelationInfo);
        let result = backend_catalog_index_seams::build_speculative_index_info::call(
            &index_relation,
            &mut index_relation_info,
        );

        put_index_desc(estate, relinfo, i, index_relation);
        put_index_info(estate, relinfo, i, index_relation_info);
        result?;
    }
    Ok(())
}

/// Find the tuple that violates the passed unique index (`conflictindex`).
/// Returns `Some(conflictslot)` (locked, pushed into the EState pool) if a
/// conflicting tuple is found, else `None`.
fn FindConflictTuple<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    conflictindex: Oid,
    slot: SlotId,
) -> PgResult<Option<SlotId>> {
    // *conflictslot = NULL;
    let mut conflictslot: Option<SlotId> = None;

    // BuildConflictIndexInfo(resultRelInfo, conflictindex);
    BuildConflictIndexInfo(estate, relinfo, conflictindex)?;

    let arbiter = [conflictindex]; // list_make1_oid(conflictindex)

    'retry: loop {
        // if (ExecCheckIndexConstraints(resultRelInfo, slot, estate,
        //         &conflictTid, &slot->tts_tid, list_make1_oid(conflictindex)))
        let mut conflict_tid = types_tuple::heaptuple::ItemPointerData::default();
        let tupleid = estate.slot(slot).tts_tid;
        let satisfies = indexing_seams::exec_check_index_constraints::call(
            estate,
            relinfo,
            slot,
            &mut conflict_tid,
            &tupleid,
            &arbiter,
        )?;
        if satisfies {
            // if (*conflictslot) ExecDropSingleTupleTableSlot(*conflictslot);
            // In the owned slot-pool model a per-slot drop would shift pool ids;
            // a previously-created conflict slot from a failed retry stays in
            // the pool and is reclaimed at FreeExecutorState (the same
            // compromise execIndexing's standalone slots make).
            // *conflictslot = NULL; return false;
            let _ = conflictslot.take();
            return Ok(None);
        }

        // *conflictslot = table_slot_create(rel, NULL);
        let rel = result_rel_alias(estate, relinfo);
        let cs_data = tableam::table_slot_create(mcx, &rel)?;
        let cs = estate.push_slot_data(cs_data)?;
        conflictslot = Some(cs);

        // PushActiveSnapshot(GetLatestSnapshot());
        let latest = snapmgr_seams::get_latest_snapshot::call()?;
        snapmgr_seams::push_active_snapshot::call(alloc::rc::Rc::new(latest))?;
        let active = snapmgr_seams::get_active_snapshot::call()?;
        let active_val = active.map(|s| (*s).clone());

        // res = table_tuple_lock(rel, &conflictTid, GetActiveSnapshot(),
        //          *conflictslot, GetCurrentCommandId(false), LockTupleShare,
        //          LockWaitBlock, 0, &tmfd);
        let cid = xact_seams::get_current_command_id::call(false)?;
        let mut tmfd = TM_FailureData::default();
        let res = {
            let cs_slot = estate.slot_data_mut(cs);
            tableam::table_tuple_lock(
                mcx,
                &rel,
                &conflict_tid,
                &active_val,
                cs_slot,
                cid,
                LockTupleMode::LockTupleShare,
                LockWaitPolicy::LockWaitBlock,
                0,
                &mut tmfd,
            )?
        };

        // PopActiveSnapshot();
        snapmgr_seams::pop_active_snapshot::call()?;

        // if (should_refetch_tuple(res, &tmfd)) goto retry;
        if should_refetch_tuple(res, &tmfd)? {
            continue 'retry;
        }

        return Ok(Some(cs));
    }
}

/// Check all the unique indexes in `recheck_indexes` for conflict with the
/// tuple in `remoteslot` and report if found.
fn CheckAndReportConflict<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    type_: ConflictType,
    recheck_indexes: &[Oid],
    searchslot: Option<SlotId>,
    remoteslot: Option<SlotId>,
) -> PgResult<()> {
    // List *conflicttuples = NIL;
    let mut conflicttuples: alloc::vec::Vec<ConflictTupleInfo> = alloc::vec::Vec::new();

    let track_commit_timestamp = vars::track_commit_timestamp.read();

    // foreach_oid(uniqueidx, resultRelInfo->ri_onConflictArbiterIndexes)
    let arbiters: alloc::vec::Vec<Oid> = estate
        .result_rel(relinfo)
        .ri_onConflictArbiterIndexes
        .as_ref()
        .map(|v| v.iter().copied().collect())
        .unwrap_or_default();
    for uniqueidx in arbiters {
        // if (list_member_oid(recheckIndexes, uniqueidx) &&
        //     FindConflictTuple(resultRelInfo, estate, uniqueidx, remoteslot,
        //                       &conflictslot))
        if recheck_indexes.contains(&uniqueidx) {
            // remoteslot is the slot to look up the conflicting row with.
            let remote = remoteslot.expect("remoteslot present in conflict reporting");
            if let Some(conflictslot) =
                FindConflictTuple(mcx, estate, relinfo, uniqueidx, remote)?
            {
                // ConflictTupleInfo *conflicttuple = palloc0_object(ConflictTupleInfo);
                // conflicttuple->slot = conflictslot;
                // conflicttuple->indexoid = uniqueidx;
                // GetTupleTransactionInfo(conflictslot, &conflicttuple->xmin,
                //                         &conflicttuple->origin, &conflicttuple->ts);
                let mut info = ConflictTupleInfo {
                    slot: Some(conflictslot),
                    indexoid: uniqueidx,
                    ..ConflictTupleInfo::default()
                };
                {
                    let cs_slot = estate.slot_data_mut(conflictslot);
                    conflict::GetTupleTransactionInfo(
                        mcx,
                        cs_slot,
                        track_commit_timestamp,
                        &mut info.xmin,
                        &mut info.origin,
                        &mut info.ts,
                    )?;
                }

                // conflicttuples = lappend(conflicttuples, conflicttuple);
                conflicttuples
                    .try_reserve(1)
                    .map_err(|_| mcx.oom(1))?;
                conflicttuples.push(info);
            }
        }
    }

    // if (conflicttuples)
    //     ReportApplyConflict(estate, resultRelInfo, ERROR,
    //         list_length(conflicttuples) > 1 ? CT_MULTIPLE_UNIQUE_CONFLICTS : type,
    //         searchslot, remoteslot, conflicttuples);
    if !conflicttuples.is_empty() {
        let reported_type = if conflicttuples.len() > 1 {
            ConflictType::CT_MULTIPLE_UNIQUE_CONFLICTS
        } else {
            type_
        };
        let subid = worker_seams::my_subscription_oid::call();
        conflict::ReportApplyConflict(
            mcx,
            estate,
            relinfo,
            ERROR,
            reported_type,
            searchslot,
            remoteslot,
            &conflicttuples,
            subid,
        )?;
    }

    Ok(())
}

// ===========================================================================
// ExecSimpleRelationInsert / Update / Delete
// ===========================================================================

/// Insert the tuple in `slot` into the relation, update the indexes, and
/// execute any constraints and per-row triggers. Caller opens the indexes.
pub fn ExecSimpleRelationInsert<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    slot: SlotId,
) -> PgResult<()> {
    let mut skip_tuple = false;
    let rel = result_rel_alias(estate, relinfo);

    // For now we support only tables.
    //   Assert(rel->rd_rel->relkind == RELKIND_RELATION);
    debug_assert_eq!(rel.rd_rel.relkind, RELKIND_RELATION);

    // CheckCmdReplicaIdentity(rel, CMD_INSERT);
    CheckCmdReplicaIdentity(&rel, CmdType::CMD_INSERT)?;

    // BEFORE ROW INSERT Triggers
    if trig_insert_before_row(estate, relinfo) {
        // if (!ExecBRInsertTriggers(estate, resultRelInfo, slot)) skip_tuple = true;
        if !trigger_seams::exec_br_insert_triggers::call(estate, relinfo, slot)? {
            skip_tuple = true;
        }
    }

    if !skip_tuple {
        let mut recheck_indexes: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
        let mut conflict = false;

        // Compute stored generated columns.
        if rel_has_generated_stored(&rel) {
            modifytable_seams::exec_compute_stored_generated::call(
                mcx,
                estate,
                relinfo,
                slot,
                CmdType::CMD_INSERT,
            )?;
        }

        // Check the constraints of the tuple.
        if rel_has_constr(&rel) {
            execMain_seams::exec_constraints::call(estate, relinfo, slot)?;
        }
        if rel.rd_rel.relispartition {
            execMain_seams::exec_partition_check::call(estate, relinfo, slot, true)?;
        }

        // OK, store the tuple and create index entries for it.
        //   simple_table_tuple_insert(resultRelInfo->ri_RelationDesc, slot);
        {
            let slot_data = estate.slot_data_mut(slot);
            tableam::simple_table_tuple_insert(mcx, &rel, slot_data)?;
        }

        // conflictindexes = resultRelInfo->ri_onConflictArbiterIndexes;
        let conflictindexes = arbiter_indexes(estate, relinfo);
        let has_conflictindexes = !conflictindexes.is_empty();

        // if (resultRelInfo->ri_NumIndices > 0)
        if estate.result_rel(relinfo).ri_NumIndices > 0 {
            // recheckIndexes = ExecInsertIndexTuples(resultRelInfo, slot, estate,
            //     false, conflictindexes ? true : false, &conflict,
            //     conflictindexes, false);
            recheck_indexes = indexing_seams::exec_insert_index_tuples::call(
                mcx,
                estate,
                relinfo,
                slot,
                false,
                has_conflictindexes,
                Some(&mut conflict),
                &conflictindexes,
                false,
            )?;
        }

        // if (conflict)
        //     CheckAndReportConflict(resultRelInfo, estate, CT_INSERT_EXISTS,
        //         recheckIndexes, NULL, slot);
        if conflict {
            CheckAndReportConflict(
                mcx,
                estate,
                relinfo,
                ConflictType::CT_INSERT_EXISTS,
                &recheck_indexes,
                None,
                Some(slot),
            )?;
        }

        // AFTER ROW INSERT Triggers
        //   ExecARInsertTriggers(estate, resultRelInfo, slot, recheckIndexes, NULL);
        trigger_seams::exec_ar_insert_triggers::call(
            estate,
            relinfo,
            slot,
            &recheck_indexes,
            None,
        )?;

        // list_free(recheckIndexes); — owned PgVec dropped at scope end.
    }

    Ok(())
}

/// Find the `searchslot` tuple and update it with data in `slot`, update the
/// indexes, and execute any constraints and per-row triggers. Caller opens the
/// indexes.
pub fn ExecSimpleRelationUpdate<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    epqstate: &mut EPQState<'mcx>,
    searchslot: SlotId,
    slot: SlotId,
) -> PgResult<()> {
    let mut skip_tuple = false;
    let rel = result_rel_alias(estate, relinfo);
    // ItemPointer tid = &(searchslot->tts_tid);
    let tid = estate.slot(searchslot).tts_tid;

    // We support only non-system tables.
    //   Assert(rel->rd_rel->relkind == RELKIND_RELATION);
    debug_assert_eq!(rel.rd_rel.relkind, RELKIND_RELATION);
    //   Assert(!IsCatalogRelation(rel));
    debug_assert!(!catalog_seams::is_catalog_relation::call(&rel));

    // CheckCmdReplicaIdentity(rel, CMD_UPDATE);
    CheckCmdReplicaIdentity(&rel, CmdType::CMD_UPDATE)?;

    // BEFORE ROW UPDATE Triggers
    if trig_update_before_row(estate, relinfo) {
        // if (!ExecBRUpdateTriggers(estate, epqstate, resultRelInfo, tid, NULL,
        //         slot, NULL, NULL, false)) skip_tuple = true;
        let mut tmfd = TM_FailureData::default();
        if !trigger_seams::exec_br_update_triggers::call(
            estate,
            epqstate,
            relinfo,
            Some(&tid),
            None, // fdw_trigtuple
            slot,
            None, // tmresult
            &mut tmfd,
            false, // is_merge_update
        )? {
            skip_tuple = true;
        }
    }

    if !skip_tuple {
        let mut recheck_indexes: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
        let mut conflict = false;

        // Compute stored generated columns.
        if rel_has_generated_stored(&rel) {
            modifytable_seams::exec_compute_stored_generated::call(
                mcx,
                estate,
                relinfo,
                slot,
                CmdType::CMD_UPDATE,
            )?;
        }

        // Check the constraints of the tuple.
        if rel_has_constr(&rel) {
            execMain_seams::exec_constraints::call(estate, relinfo, slot)?;
        }
        if rel.rd_rel.relispartition {
            execMain_seams::exec_partition_check::call(estate, relinfo, slot, true)?;
        }

        // simple_table_tuple_update(rel, tid, slot, estate->es_snapshot,
        //                           &update_indexes);
        let snapshot = estate.es_snapshot.as_deref().cloned();
        let mut update_indexes = TU_UpdateIndexes::TU_None;
        {
            let slot_data = estate.slot_data_mut(slot);
            tableam::simple_table_tuple_update(
                mcx,
                &rel,
                &tid,
                slot_data,
                &snapshot,
                &mut update_indexes,
            )?;
        }

        // conflictindexes = resultRelInfo->ri_onConflictArbiterIndexes;
        let conflictindexes = arbiter_indexes(estate, relinfo);
        let has_conflictindexes = !conflictindexes.is_empty();

        // if (resultRelInfo->ri_NumIndices > 0 && (update_indexes != TU_None))
        if estate.result_rel(relinfo).ri_NumIndices > 0
            && update_indexes != TU_UpdateIndexes::TU_None
        {
            // recheckIndexes = ExecInsertIndexTuples(resultRelInfo, slot, estate,
            //     true, conflictindexes ? true : false, &conflict,
            //     conflictindexes, (update_indexes == TU_Summarizing));
            recheck_indexes = indexing_seams::exec_insert_index_tuples::call(
                mcx,
                estate,
                relinfo,
                slot,
                true,
                has_conflictindexes,
                Some(&mut conflict),
                &conflictindexes,
                update_indexes == TU_UpdateIndexes::TU_Summarizing,
            )?;
        }

        // if (conflict)
        //     CheckAndReportConflict(resultRelInfo, estate, CT_UPDATE_EXISTS,
        //         recheckIndexes, searchslot, slot);
        if conflict {
            CheckAndReportConflict(
                mcx,
                estate,
                relinfo,
                ConflictType::CT_UPDATE_EXISTS,
                &recheck_indexes,
                Some(searchslot),
                Some(slot),
            )?;
        }

        // AFTER ROW UPDATE Triggers
        //   ExecARUpdateTriggers(estate, resultRelInfo, NULL, NULL, tid, NULL,
        //       slot, recheckIndexes, NULL, false);
        trigger_seams::exec_ar_update_triggers::call(
            estate,
            relinfo,
            None, // src_partinfo
            None, // dst_partinfo
            Some(&tid),
            None, // fdw_trigtuple
            Some(slot),
            &recheck_indexes,
            None, // transition_capture
            false, // is_crosspart_update
        )?;

        // list_free(recheckIndexes); — owned PgVec dropped at scope end.
    }

    Ok(())
}

/// Find the `searchslot` tuple and delete it, and execute any constraints and
/// per-row triggers. Caller opens the indexes.
pub fn ExecSimpleRelationDelete<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    epqstate: &mut EPQState<'mcx>,
    searchslot: SlotId,
) -> PgResult<()> {
    let mut skip_tuple = false;
    let rel = result_rel_alias(estate, relinfo);
    // ItemPointer tid = &searchslot->tts_tid;
    let tid = estate.slot(searchslot).tts_tid;

    // CheckCmdReplicaIdentity(rel, CMD_DELETE);
    CheckCmdReplicaIdentity(&rel, CmdType::CMD_DELETE)?;

    // BEFORE ROW DELETE Triggers
    if trig_delete_before_row(estate, relinfo) {
        // skip_tuple = !ExecBRDeleteTriggers(estate, epqstate, resultRelInfo,
        //     tid, NULL, NULL, NULL, NULL, false);
        let mut tmfd = TM_FailureData::default();
        skip_tuple = !trigger_seams::exec_br_delete_triggers::call(
            estate,
            epqstate,
            relinfo,
            Some(&tid),
            None, // fdw_trigtuple
            None, // epqslot
            None, // tmresult
            &mut tmfd,
            false, // is_merge_delete
        )?;
    }

    if !skip_tuple {
        // OK, delete the tuple.
        //   simple_table_tuple_delete(rel, tid, estate->es_snapshot);
        let snapshot = estate.es_snapshot.as_deref().cloned();
        tableam::simple_table_tuple_delete(mcx, &rel, &tid, &snapshot)?;

        // AFTER ROW DELETE Triggers
        //   ExecARDeleteTriggers(estate, resultRelInfo, tid, NULL, NULL, false);
        trigger_seams::exec_ar_delete_triggers::call(
            estate,
            relinfo,
            Some(&tid),
            None, // fdw_trigtuple
            None, // transition_capture
            false, // is_crosspart_update
        )?;
    }

    Ok(())
}

// ===========================================================================
// CheckCmdReplicaIdentity
// ===========================================================================

/// Check if `cmd` can be executed with `rel`'s current replica identity.
pub fn CheckCmdReplicaIdentity<'mcx>(rel: &Relation<'mcx>, cmd: CmdType) -> PgResult<()> {
    // Skip checking the replica identity for partitioned tables.
    //   if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) return;
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Ok(());
    }

    // We only need to do checks for UPDATE and DELETE.
    //   if (cmd != CMD_UPDATE && cmd != CMD_DELETE) return;
    if cmd != CmdType::CMD_UPDATE && cmd != CmdType::CMD_DELETE {
        return Ok(());
    }

    // RelationBuildPublicationDesc(rel, &pubdesc);
    let pubdesc = relcache_nodexform_seams::relation_build_publication_desc::call(rel)?;

    let relname = rel.name().to_string();

    if cmd == CmdType::CMD_UPDATE && !pubdesc.rf_valid_for_update {
        return Err(repl_ident_error(
            format!("cannot update table \"{relname}\""),
            "Column used in the publication WHERE expression is not part of the replica identity.",
        ));
    } else if cmd == CmdType::CMD_UPDATE && !pubdesc.cols_valid_for_update {
        return Err(repl_ident_error(
            format!("cannot update table \"{relname}\""),
            "Column list used by the publication does not cover the replica identity.",
        ));
    } else if cmd == CmdType::CMD_UPDATE && !pubdesc.gencols_valid_for_update {
        return Err(repl_ident_error(
            format!("cannot update table \"{relname}\""),
            "Replica identity must not contain unpublished generated columns.",
        ));
    } else if cmd == CmdType::CMD_DELETE && !pubdesc.rf_valid_for_delete {
        return Err(repl_ident_error(
            format!("cannot delete from table \"{relname}\""),
            "Column used in the publication WHERE expression is not part of the replica identity.",
        ));
    } else if cmd == CmdType::CMD_DELETE && !pubdesc.cols_valid_for_delete {
        return Err(repl_ident_error(
            format!("cannot delete from table \"{relname}\""),
            "Column list used by the publication does not cover the replica identity.",
        ));
    } else if cmd == CmdType::CMD_DELETE && !pubdesc.gencols_valid_for_delete {
        return Err(repl_ident_error(
            format!("cannot delete from table \"{relname}\""),
            "Replica identity must not contain unpublished generated columns.",
        ));
    }

    // If relation has replica identity we are always good.
    //   if (OidIsValid(RelationGetReplicaIndex(rel))) return;
    if oid_is_valid(backend_utils_cache_relcache::derived::RelationGetReplicaIndex(rel.rd_id)?) {
        return Ok(());
    }

    // REPLICA IDENTITY FULL is also good for UPDATE/DELETE.
    //   if (rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL) return;
    if rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
        return Ok(());
    }

    // This is UPDATE/DELETE and there is no replica identity.
    // Check if the table publishes UPDATES or DELETES.
    if cmd == CmdType::CMD_UPDATE && pubdesc.pubactions.pubupdate {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot update table \"{relname}\" because it does not have a replica identity and publishes updates"
            ))
            .errhint("To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.")
            .into_error());
    } else if cmd == CmdType::CMD_DELETE && pubdesc.pubactions.pubdelete {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot delete from table \"{relname}\" because it does not have a replica identity and publishes deletes"
            ))
            .errhint("To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.")
            .into_error());
    }

    Ok(())
}

/// The shared `ereport(ERROR, errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
/// errmsg(...), errdetail(...))` of `CheckCmdReplicaIdentity`'s checks.
fn repl_ident_error(msg: alloc::string::String, detail: &'static str) -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
        .errmsg(msg)
        .errdetail(detail)
        .into_error()
}

// ===========================================================================
// CheckSubscriptionRelkind
// ===========================================================================

/// Check if we support writing into the specific `relkind`. `nspname`/`relname`
/// are only needed for error reporting.
pub fn CheckSubscriptionRelkind(relkind: u8, nspname: &str, relname: &str) -> PgResult<()> {
    // if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE)
    if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        // ereport(ERROR, errcode(ERRCODE_WRONG_OBJECT_TYPE),
        //   errmsg("cannot use relation \"%s.%s\" as logical replication target",
        //          nspname, relname),
        //   errdetail_relkind_not_supported(relkind));
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "cannot use relation \"{nspname}.{relname}\" as logical replication target"
            ))
            .errdetail(pg_class_seams::errdetail_relkind_not_supported::call(relkind)?)
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// Small helpers — Relation / ResultRelInfo field reads off the EState pool.
// ===========================================================================

/// Alias the result relation's open `Relation`.
fn result_rel_alias<'mcx>(estate: &EStateData<'mcx>, rri: RriId) -> Relation<'mcx> {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .expect("result relation must be open")
        .alias()
}

/// `resultRelInfo->ri_onConflictArbiterIndexes` as an owned `Vec`.
fn arbiter_indexes(estate: &EStateData<'_>, rri: RriId) -> alloc::vec::Vec<Oid> {
    estate
        .result_rel(rri)
        .ri_onConflictArbiterIndexes
        .as_ref()
        .map(|v| v.iter().copied().collect())
        .unwrap_or_default()
}

/// `rel->rd_att->constr != NULL`.
fn rel_has_constr(rel: &Relation<'_>) -> bool {
    rel.rd_att.constr.is_some()
}

/// `rel->rd_att->constr && rel->rd_att->constr->has_generated_stored`.
fn rel_has_generated_stored(rel: &Relation<'_>) -> bool {
    rel.rd_att
        .constr
        .as_ref()
        .map(|c| c.has_generated_stored)
        .unwrap_or(false)
}

/// `resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_insert_before_row`.
fn trig_insert_before_row(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_TrigDesc
        .as_ref()
        .map(|td| td.trig_insert_before_row)
        .unwrap_or(false)
}

/// `resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_update_before_row`.
fn trig_update_before_row(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_TrigDesc
        .as_ref()
        .map(|td| td.trig_update_before_row)
        .unwrap_or(false)
}

/// `resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_delete_before_row`.
fn trig_delete_before_row(estate: &EStateData<'_>, rri: RriId) -> bool {
    estate
        .result_rel(rri)
        .ri_TrigDesc
        .as_ref()
        .map(|td| td.trig_delete_before_row)
        .unwrap_or(false)
}

/// Move the `IndexInfo[i]` out of the pool into an owned local.
fn take_index_info<'mcx>(
    estate: &mut EStateData<'mcx>,
    rri: RriId,
    i: usize,
) -> types_nodes::execnodes::IndexInfo<'mcx> {
    let arr = estate
        .result_rel_mut(rri)
        .ri_IndexRelationInfo
        .as_mut()
        .expect("ri_IndexRelationInfo present");
    core::mem::take(&mut arr[i])
}

/// Write the (possibly mutated) `IndexInfo` back into pool slot `i`.
fn put_index_info<'mcx>(
    estate: &mut EStateData<'mcx>,
    rri: RriId,
    i: usize,
    ii: types_nodes::execnodes::IndexInfo<'mcx>,
) {
    let arr = estate
        .result_rel_mut(rri)
        .ri_IndexRelationInfo
        .as_mut()
        .expect("ri_IndexRelationInfo present");
    arr[i] = ii;
}

/// Take the open index descriptor `i` out of the pool.
fn take_index_desc<'mcx>(
    estate: &mut EStateData<'mcx>,
    rri: RriId,
    i: usize,
) -> Option<Relation<'mcx>> {
    let arr = estate
        .result_rel_mut(rri)
        .ri_IndexRelationDescs
        .as_mut()
        .expect("ri_IndexRelationDescs present");
    arr[i].take()
}

/// Put the open index descriptor back into pool slot `i`.
fn put_index_desc<'mcx>(estate: &mut EStateData<'mcx>, rri: RriId, i: usize, rel: Relation<'mcx>) {
    let arr = estate
        .result_rel_mut(rri)
        .ri_IndexRelationDescs
        .as_mut()
        .expect("ri_IndexRelationDescs present");
    arr[i] = Some(rel);
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install every seam this unit owns: `GetRelationIdentityOrPK` (homed in
/// `relation.c` / `backend-replication-logical-relation` in PG18, exposed
/// through this unit's seam so cross-cycle consumers — `conflict.c` — can reach
/// it).
pub fn init_seams() {
    inward::get_relation_identity_or_pk::set(seam_get_relation_identity_or_pk);
}

fn seam_get_relation_identity_or_pk(rel: &Relation<'_>) -> PgResult<Oid> {
    logicalrelation::GetRelationIdentityOrPK(rel)
}
