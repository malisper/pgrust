//! Port of `src/backend/replication/logical/conflict.c` (PostgreSQL 18.3):
//! support routines for logging conflicts on the subscriber during logical
//! replication.
//!
//! Conventions that differ from the C surface:
//!
//! - `TupleTableSlot *` parameters are [`SlotId`]s into the apply worker's
//!   `EState` slot pool (`None` is the C NULL slot), and `ResultRelInfo *` is
//!   an [`RriId`]; the `EState` is threaded explicitly.
//! - `track_commit_timestamp` (the commit_ts.c GUC global) and
//!   `MySubscription->oid` (the worker.c per-backend global) are explicit
//!   parameters; the callers read them off their own state.
//! - The detail/message strings are error-report construction feeding the
//!   `ereport` builder (which stores `String`), so they are built as plain
//!   `String`s; context-allocated values crossing seams use `mcx`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use backend_utils_error::ereport;
use mcx::Mcx;
use types_core::primitive::{
    AttrNumber, InvalidRepOriginId, Oid, OidIsValid, RepOriginId, TimestampTz, TransactionId,
};
use types_error::{
    ErrorLevel, ErrorLocation, PgResult, SqlState, ERRCODE_T_R_SERIALIZATION_FAILURE,
    ERRCODE_UNIQUE_VIOLATION,
};
use types_nodes::execnodes::{EStateData, RriId, SlotId};
use types_nodes::TupleSlotKind;
use types_rel::Relation;
use types_storage::lock::NoLock;

pub use types_replication::conflict::{ConflictType, CONFLICT_NUM_TYPES};

use ConflictType::*;

use backend_access_index_genam_seams as genam_seams;
use backend_access_index_indexam_seams as indexam_seams;
use backend_access_transam_commit_ts_seams as commit_ts_seams;
use backend_catalog_index_seams as catalog_index_seams;
use backend_executor_execMain_seams as execMain_seams;
use backend_executor_execReplication_seams as execReplication_seams;
use backend_executor_execTuples_seams as execTuples_seams;
use backend_nodes_core_seams as bms_seams;
use backend_replication_logical_origin_seams as origin_seams;
use backend_utils_activity_stat_seams as stat_seams;
use backend_utils_adt_timestamp_seams as timestamp_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;

/// `MinTransactionIdAttributeNumber` (access/sysattr.h) — the xmin system
/// attribute.
const MinTransactionIdAttributeNumber: AttrNumber = -2;

/// `static const char *const ConflictTypeNames[]` — keyed by the
/// `ConflictType` discriminant (designated initializers; index ==
/// discriminant).
pub static CONFLICT_TYPE_NAMES: [&str; CONFLICT_NUM_TYPES] = {
    let mut names: [&str; CONFLICT_NUM_TYPES] = [""; CONFLICT_NUM_TYPES];
    names[CT_INSERT_EXISTS as usize] = "insert_exists";
    names[CT_UPDATE_ORIGIN_DIFFERS as usize] = "update_origin_differs";
    names[CT_UPDATE_EXISTS as usize] = "update_exists";
    names[CT_UPDATE_MISSING as usize] = "update_missing";
    names[CT_DELETE_ORIGIN_DIFFERS as usize] = "delete_origin_differs";
    names[CT_DELETE_MISSING as usize] = "delete_missing";
    names[CT_MULTIPLE_UNIQUE_CONFLICTS as usize] = "multiple_unique_conflicts";
    names
};

/// `ConflictTupleInfo` (replication/conflict.h): one local row that caused a
/// conflict, with its transaction information.
#[derive(Clone, Copy, Debug, Default)]
pub struct ConflictTupleInfo {
    /// `TupleTableSlot *slot` — the conflicting local row, or `None`.
    pub slot: Option<SlotId>,
    /// `Oid indexoid` — the conflicting unique index, or `InvalidOid`.
    pub indexoid: Oid,
    /// `TransactionId xmin` — the local row's inserting/updating xid.
    pub xmin: TransactionId,
    /// `RepOriginId origin` — origin of the local row's last modification.
    pub origin: RepOriginId,
    /// `TimestampTz ts` — commit timestamp of that modification.
    pub ts: TimestampTz,
}

/// There are no inward seams for this unit (consumers can depend on the
/// crate directly), so there is nothing to install.
pub fn init_seams() {}

/// Get the xmin and commit timestamp data (origin and timestamp) associated
/// with the provided local row.
///
/// Return true if the commit timestamp data was found, false otherwise.
///
/// `track_commit_timestamp` is the commit_ts.c GUC global, passed explicitly.
pub fn GetTupleTransactionInfo<'mcx>(
    mcx: Mcx<'mcx>,
    localslot: &mut types_nodes::tuptable::SlotData<'mcx>,
    track_commit_timestamp: bool,
    xmin: &mut TransactionId,
    localorigin: &mut RepOriginId,
    localts: &mut TimestampTz,
) -> PgResult<bool> {
    let (xmin_datum, isnull) =
        execTuples_seams::slot_getsysattr::call(mcx, localslot, MinTransactionIdAttributeNumber)?;
    *xmin = xmin_datum.as_transaction_id();
    debug_assert!(!isnull);
    let _ = isnull;

    // The commit timestamp data is not available if track_commit_timestamp is
    // disabled.
    if !track_commit_timestamp {
        *localorigin = InvalidRepOriginId;
        *localts = 0;
        return Ok(false);
    }

    let (found, ts, nodeid) = commit_ts_seams::transaction_id_get_commit_ts_data::call(*xmin)?;
    *localts = ts;
    *localorigin = nodeid;
    Ok(found)
}

/// This function is used to report a conflict while applying replication
/// changes.
///
/// `searchslot` should contain the tuple used to search the local row to be
/// updated or deleted.
///
/// `remoteslot` should contain the remote new tuple, if any.
///
/// `conflicttuples` is a list of local rows that caused the conflict and the
/// conflict related information.
///
/// The caller must ensure that all the indexes passed in `ConflictTupleInfo`
/// are locked so that we can fetch and display the conflicting key values.
///
/// `subid` is the C `MySubscription->oid` (worker.c per-backend global),
/// passed explicitly. A non-erroring `elevel` (e.g. LOG) returns `Ok(())`
/// after the report, as the C ereport returns; ERROR and above return `Err`.
pub fn ReportApplyConflict<'mcx>(
    mcx: Mcx<'_>,
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    elevel: ErrorLevel,
    type_: ConflictType,
    searchslot: Option<SlotId>,
    remoteslot: Option<SlotId>,
    conflicttuples: &[ConflictTupleInfo],
    subid: Oid,
) -> PgResult<()> {
    let localrel = estate
        .result_rel(relinfo)
        .ri_RelationDesc
        .as_ref()
        .expect("ReportApplyConflict: result relation is open")
        .alias();
    let mut err_detail = String::new();

    // Form errdetail message by combining conflicting tuples information.
    for conflicttuple in conflicttuples {
        errdetail_apply_conflict(
            mcx,
            estate,
            relinfo,
            type_,
            searchslot,
            conflicttuple.slot,
            remoteslot,
            conflicttuple.indexoid,
            conflicttuple.xmin,
            conflicttuple.origin,
            conflicttuple.ts,
            &mut err_detail,
        )?;
    }

    stat_seams::pgstat_report_subscription_conflict::call(subid, type_)?;

    let nspname = lsyscache_seams::get_namespace_name::call(mcx, localrel.rd_rel.relnamespace)?;

    ereport(elevel)
        .errcode(errcode_apply_conflict(type_))
        .errmsg(format!(
            "conflict detected on relation \"{}.{}\": conflict={}",
            // PG's vsnprintf prints "(null)" for a NULL %s argument.
            nspname.as_ref().map(|s| s.as_str()).unwrap_or("(null)"),
            localrel.name(),
            CONFLICT_TYPE_NAMES[type_ as usize],
        ))
        .errdetail_internal(err_detail)
        .finish(ErrorLocation::new("conflict.c", 124, "ReportApplyConflict"))
}

/// Find all unique indexes to check for a conflict and store them into
/// `ResultRelInfo`.
pub fn InitConflictIndexes<'mcx>(
    estate: &mut EStateData<'mcx>,
    rel_info: RriId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // List *uniqueIndexes = NIL;  (lappend_oid pallocs; fallible growth)
    let mut unique_indexes: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);

    let rri = estate.result_rel(rel_info);
    for i in 0..rri.ri_NumIndices as usize {
        let index_relation = rri
            .ri_IndexRelationDescs
            .as_ref()
            .expect("ri_IndexRelationDescs is set when ri_NumIndices > 0")[i]
            .as_ref();

        // if (indexRelation == NULL) continue;
        let Some(index_relation) = index_relation else {
            continue;
        };

        // Detect conflict only for unique indexes
        if !rri
            .ri_IndexRelationInfo
            .as_ref()
            .expect("ri_IndexRelationInfo is set when ri_NumIndices > 0")[i]
            .ii_Unique
        {
            continue;
        }

        // Don't support conflict detection for deferrable index
        if !index_relation
            .rd_index
            .as_ref()
            .expect("an index relation carries rd_index")
            .indimmediate
        {
            continue;
        }

        // uniqueIndexes = lappend_oid(uniqueIndexes,
        //                             RelationGetRelid(indexRelation));
        unique_indexes
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
        unique_indexes.push(index_relation.rd_id);
    }

    estate.result_rel_mut(rel_info).ri_onConflictArbiterIndexes = if unique_indexes.is_empty() {
        None // NIL
    } else {
        Some(unique_indexes)
    };
    Ok(())
}

/// Add SQLSTATE error code to the current conflict report.
fn errcode_apply_conflict(type_: ConflictType) -> SqlState {
    match type_ {
        CT_INSERT_EXISTS | CT_UPDATE_EXISTS | CT_MULTIPLE_UNIQUE_CONFLICTS => {
            ERRCODE_UNIQUE_VIOLATION
        }
        CT_UPDATE_ORIGIN_DIFFERS
        | CT_UPDATE_MISSING
        | CT_DELETE_ORIGIN_DIFFERS
        | CT_DELETE_MISSING => ERRCODE_T_R_SERIALIZATION_FAILURE,
    }
}

/// Add an errdetail() line showing conflict detail.
///
/// The DETAIL line comprises of two parts:
/// 1. Explanation of the conflict type, including the origin and commit
///    timestamp of the existing local row.
/// 2. Display of conflicting key, existing local row, remote new row, and
///    replica identity columns, if any. The remote old row is excluded as its
///    information is covered in the replica identity columns.
fn errdetail_apply_conflict<'mcx>(
    mcx: Mcx<'_>,
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    type_: ConflictType,
    searchslot: Option<SlotId>,
    localslot: Option<SlotId>,
    remoteslot: Option<SlotId>,
    indexoid: Oid,
    localxmin: TransactionId,
    localorigin: RepOriginId,
    localts: TimestampTz,
    err_msg: &mut String,
) -> PgResult<()> {
    let mut err_detail = String::new();

    // First, construct a detailed message describing the type of conflict
    match type_ {
        CT_INSERT_EXISTS | CT_UPDATE_EXISTS | CT_MULTIPLE_UNIQUE_CONFLICTS => {
            // Assert(OidIsValid(indexoid) &&
            //        CheckRelationOidLockedByMe(indexoid, RowExclusiveLock,
            //                                   true)) — the lock half is
            // cassert-only and elided, like Assert in a non-cassert build.
            debug_assert!(OidIsValid(indexoid));

            if localts != 0 {
                if localorigin == InvalidRepOriginId {
                    err_detail.push_str(&format!(
                        "Key already exists in unique index \"{}\", modified locally in transaction {} at {}.",
                        rel_name(mcx, indexoid)?,
                        localxmin,
                        timestamp_seams::timestamptz_to_str::call(mcx, localts)?.as_str(),
                    ));
                } else if let Some(origin_name) =
                    origin_seams::replorigin_by_oid::call(mcx, localorigin, true)?
                {
                    err_detail.push_str(&format!(
                        "Key already exists in unique index \"{}\", modified by origin \"{}\" in transaction {} at {}.",
                        rel_name(mcx, indexoid)?,
                        origin_name.as_str(),
                        localxmin,
                        timestamp_seams::timestamptz_to_str::call(mcx, localts)?.as_str(),
                    ));
                }
                // The origin that modified this row has been removed. This
                // can happen if the origin was created by a different apply
                // worker and its associated subscription and origin were
                // dropped after updating the row, or if the origin was
                // manually dropped by the user.
                else {
                    err_detail.push_str(&format!(
                        "Key already exists in unique index \"{}\", modified by a non-existent origin in transaction {} at {}.",
                        rel_name(mcx, indexoid)?,
                        localxmin,
                        timestamp_seams::timestamptz_to_str::call(mcx, localts)?.as_str(),
                    ));
                }
            } else {
                err_detail.push_str(&format!(
                    "Key already exists in unique index \"{}\", modified in transaction {}.",
                    rel_name(mcx, indexoid)?,
                    localxmin,
                ));
            }
        }

        CT_UPDATE_ORIGIN_DIFFERS => {
            if localorigin == InvalidRepOriginId {
                err_detail.push_str(&format!(
                    "Updating the row that was modified locally in transaction {} at {}.",
                    localxmin,
                    timestamp_seams::timestamptz_to_str::call(mcx, localts)?.as_str(),
                ));
            } else if let Some(origin_name) =
                origin_seams::replorigin_by_oid::call(mcx, localorigin, true)?
            {
                err_detail.push_str(&format!(
                    "Updating the row that was modified by a different origin \"{}\" in transaction {} at {}.",
                    origin_name.as_str(),
                    localxmin,
                    timestamp_seams::timestamptz_to_str::call(mcx, localts)?.as_str(),
                ));
            }
            // The origin that modified this row has been removed.
            else {
                err_detail.push_str(&format!(
                    "Updating the row that was modified by a non-existent origin in transaction {} at {}.",
                    localxmin,
                    timestamp_seams::timestamptz_to_str::call(mcx, localts)?.as_str(),
                ));
            }
        }

        CT_UPDATE_MISSING => {
            err_detail.push_str("Could not find the row to be updated.");
        }

        CT_DELETE_ORIGIN_DIFFERS => {
            if localorigin == InvalidRepOriginId {
                err_detail.push_str(&format!(
                    "Deleting the row that was modified locally in transaction {} at {}.",
                    localxmin,
                    timestamp_seams::timestamptz_to_str::call(mcx, localts)?.as_str(),
                ));
            } else if let Some(origin_name) =
                origin_seams::replorigin_by_oid::call(mcx, localorigin, true)?
            {
                err_detail.push_str(&format!(
                    "Deleting the row that was modified by a different origin \"{}\" in transaction {} at {}.",
                    origin_name.as_str(),
                    localxmin,
                    timestamp_seams::timestamptz_to_str::call(mcx, localts)?.as_str(),
                ));
            }
            // The origin that modified this row has been removed.
            else {
                err_detail.push_str(&format!(
                    "Deleting the row that was modified by a non-existent origin in transaction {} at {}.",
                    localxmin,
                    timestamp_seams::timestamptz_to_str::call(mcx, localts)?.as_str(),
                ));
            }
        }

        CT_DELETE_MISSING => {
            err_detail.push_str("Could not find the row to be deleted.");
        }
    }

    debug_assert!(!err_detail.is_empty());

    let val_desc = build_tuple_value_details(
        mcx, estate, relinfo, type_, searchslot, localslot, remoteslot, indexoid,
    )?;

    // Next, append the key values, existing local row, remote row, and
    // replica identity columns after the message.
    if let Some(val_desc) = val_desc {
        err_detail.push('\n');
        err_detail.push_str(&val_desc);
    }

    // Insert a blank line to visually separate the new detail line from the
    // existing ones.
    if !err_msg.is_empty() {
        err_msg.push('\n');
    }

    err_msg.push_str(&err_detail);
    Ok(())
}

/// `get_rel_name(relid)` rendered as the C `%s` would print it (PG's
/// vsnprintf prints "(null)" for a NULL argument).
fn rel_name(mcx: Mcx<'_>, relid: Oid) -> PgResult<String> {
    Ok(lsyscache_seams::get_rel_name::call(mcx, relid)?
        .map(|s| s.as_str().to_owned())
        .unwrap_or_else(|| "(null)".to_owned()))
}

/// Helper function to build the additional details for conflicting key,
/// existing local row, remote row, and replica identity columns.
///
/// If the return value is `None`, it indicates that the current user lacks
/// permissions to view the columns involved.
fn build_tuple_value_details<'mcx>(
    mcx: Mcx<'_>,
    estate: &mut EStateData<'mcx>,
    relinfo: RriId,
    type_: ConflictType,
    searchslot: Option<SlotId>,
    localslot: Option<SlotId>,
    remoteslot: Option<SlotId>,
    indexoid: Oid,
) -> PgResult<Option<String>> {
    let localrel = estate
        .result_rel(relinfo)
        .ri_RelationDesc
        .as_ref()
        .expect("build_tuple_value_details: result relation is open")
        .alias();
    let relid = localrel.rd_id; // RelationGetRelid
    let mut tuple_value = String::new();

    debug_assert!(searchslot.is_some() || localslot.is_some() || remoteslot.is_some());

    // Report the conflicting key values in the case of a unique constraint
    // violation.
    if type_ == CT_INSERT_EXISTS || type_ == CT_UPDATE_EXISTS || type_ == CT_MULTIPLE_UNIQUE_CONFLICTS
    {
        debug_assert!(OidIsValid(indexoid) && localslot.is_some());

        let desc = build_index_value_desc(mcx, estate, &localrel, localslot, indexoid)?;

        if let Some(desc) = desc {
            tuple_value.push_str(&format!("Key {}", desc.as_str()));
        }
    }

    if let Some(localslot) = localslot {
        // The 'modifiedCols' only applies to the new tuple, hence we pass
        // NULL for the existing local row.
        let desc = execMain_seams::exec_build_slot_value_description::call(
            mcx,
            relid,
            estate.slot(localslot),
            &localrel.rd_att,
            None,
            64,
        )?;

        if let Some(desc) = desc {
            if !tuple_value.is_empty() {
                tuple_value.push_str("; ");
                tuple_value.push_str(&format!("existing local row {}", desc.as_str()));
            } else {
                tuple_value.push_str(&format!("Existing local row {}", desc.as_str()));
            }
        }
    }

    if let Some(remoteslot) = remoteslot {
        // Although logical replication doesn't maintain the bitmap for the
        // columns being inserted, we still use it to create 'modifiedCols'
        // for consistency with other calls to ExecBuildSlotValueDescription.
        //
        // Note that generated columns are formed locally on the subscriber.
        let inserted_cols =
            backend_executor_execUtils::ExecGetInsertedCols(estate, relinfo, mcx)?;
        let updated_cols = backend_executor_execUtils::ExecGetUpdatedCols(estate, relinfo, mcx)?;
        let modified_cols =
            bms_seams::bms_union::call(mcx, inserted_cols.as_deref(), updated_cols.as_deref())?;
        let desc = execMain_seams::exec_build_slot_value_description::call(
            mcx,
            relid,
            estate.slot(remoteslot),
            &localrel.rd_att,
            modified_cols.as_deref(),
            64,
        )?;

        if let Some(desc) = desc {
            if !tuple_value.is_empty() {
                tuple_value.push_str("; ");
                tuple_value.push_str(&format!("remote row {}", desc.as_str()));
            } else {
                tuple_value.push_str(&format!("Remote row {}", desc.as_str()));
            }
        }
    }

    if let Some(searchslot) = searchslot {
        // Note that while index other than replica identity may be used (see
        // IsIndexUsableForReplicaIdentityFull for details) to find the tuple
        // when applying update or delete, such an index scan may not result
        // in a unique tuple and we still compare the complete tuple in such
        // cases, thus such indexes are not used here.
        let replica_index = execReplication_seams::get_relation_identity_or_pk::call(&localrel)?;

        debug_assert!(type_ != CT_INSERT_EXISTS);

        // If the table has a valid replica identity index, build the index
        // key value string. Otherwise, construct the full tuple value for
        // REPLICA IDENTITY FULL cases.
        let desc = if OidIsValid(replica_index) {
            build_index_value_desc(mcx, estate, &localrel, Some(searchslot), replica_index)?
        } else {
            execMain_seams::exec_build_slot_value_description::call(
                mcx,
                relid,
                estate.slot(searchslot),
                &localrel.rd_att,
                None,
                64,
            )?
        };

        if let Some(desc) = desc {
            if !tuple_value.is_empty() {
                tuple_value.push_str("; ");
                if OidIsValid(replica_index) {
                    tuple_value.push_str(&format!("replica identity {}", desc.as_str()));
                } else {
                    tuple_value.push_str(&format!("replica identity full {}", desc.as_str()));
                }
            } else if OidIsValid(replica_index) {
                tuple_value.push_str(&format!("Replica identity {}", desc.as_str()));
            } else {
                tuple_value.push_str(&format!("Replica identity full {}", desc.as_str()));
            }
        }
    }

    if tuple_value.is_empty() {
        return Ok(None);
    }

    tuple_value.push('.');
    Ok(Some(tuple_value))
}

/// Helper function to construct a string describing the contents of an index
/// entry. See `BuildIndexValueDescription` for details.
///
/// The caller must ensure that the index with the OID `indexoid` is locked so
/// that we can fetch and display the conflicting key value.
fn build_index_value_desc<'b, 'mcx>(
    mcx: Mcx<'b>,
    estate: &mut EStateData<'mcx>,
    localrel: &Relation<'_>,
    slot: Option<SlotId>,
    indexoid: Oid,
) -> PgResult<Option<mcx::PgString<'b>>> {
    // if (!tableslot) return NULL;
    let Some(slot) = slot else {
        return Ok(None);
    };
    let mut tableslot = slot;

    // Assert(CheckRelationOidLockedByMe(indexoid, RowExclusiveLock, true)) —
    // cassert-only, elided.

    let index_desc = indexam_seams::index_open::call(mcx, indexoid, NoLock)?;

    // If the slot is a virtual slot, copy it into a heap tuple slot as
    // FormIndexDatum only works with heap tuple slots.
    if estate.slot(slot).tts_ops == TupleSlotKind::Virtual {
        // table_slot_create(localrel, &estate->es_tupleTable): the slot lives
        // in the executor slot pool (query context).
        let query_cxt = estate.es_query_cxt;
        let new_slot = backend_access_table_tableam::table_slot_create(query_cxt, localrel)?;
        tableslot = estate.push_slot_data(new_slot)?;

        // tableslot = ExecCopySlot(tableslot, slot);
        execTuples_seams::exec_copy_slot::call(estate, tableslot, slot)?;
    }

    // Initialize ecxt_scantuple for potential use in FormIndexDatum when
    // index expressions are present.
    let per_tuple_ecxt = backend_executor_execUtils::MakePerTupleExprContext(estate)?;
    estate.ecxt_mut(per_tuple_ecxt).ecxt_scantuple = Some(tableslot);

    // The values/nulls arrays passed to BuildIndexValueDescription should be
    // the results of FormIndexDatum, which are the "raw" input to the index
    // AM.
    let index_info = catalog_index_seams::build_index_info::call(mcx, &index_desc)?;
    let (values, isnull) =
        catalog_index_seams::form_index_datum::call(&index_info, tableslot, estate)?;

    // `form_index_datum` yields the bare scalar words (the AM's raw index input
    // Datums); `build_index_value_description` now takes the canonical unified
    // value (the Datum-unification keystone flipped its edge) — carry each word
    // in the by-value arm.
    let values_canon: Vec<types_tuple::backend_access_common_heaptuple::Datum> = values
        .iter()
        .map(|d| types_tuple::backend_access_common_heaptuple::Datum::ByVal(d.as_usize()))
        .collect();

    let index_value = genam_seams::build_index_value_description::call(
        mcx,
        &index_desc,
        &values_canon,
        &isnull,
    )?;

    // index_close(indexDesc, NoLock); an error above drops the handle (the
    // C abort path releases the relcache reference via the resource owner).
    index_desc.close(NoLock)?;

    Ok(index_value)
}

#[cfg(test)]
mod tests;
