// conflict.c: reporting conflicts detected while applying replication changes
// (ReportApplyConflict, errdetail_apply_conflict, build_tuple_value_details,
// build_index_value_desc, InitConflictIndexes), plus execReplication.c's
// detection half (BuildConflictIndexInfo, FindConflictTuple,
// CheckAndReportConflict). Rendering: the apply path has no ResultRelInfo, so
// the arbiter list (ri_onConflictArbiterIndexes) travels as a Vec<Oid> next
// to the ResultRelIndexState it was derived from.

use datum::Datum;
use elog::ereport;
use mcx::Mcx;
use types_core::{
    InvalidOid, InvalidRepOriginId, Oid, RepOriginId, TimestampTz, TransactionId, INDEX_MAX_KEYS,
};
use types_error::{
    ErrorLevel, PgResult, SqlState, ERRCODE_T_R_SERIALIZATION_FAILURE, ERRCODE_UNIQUE_VIOLATION,
    ERROR,
};
use types_rel::Relation;
use types_slot::{SlotData, TupleSlotKind};
use types_tuple::ItemPointerData;

use crate::apply::{get_tuple_transaction_info, should_refetch_tuple, LockOutcome};
use crate::{loc, my_sub};

// ConflictType (replication/conflict.h); the discriminants index
// pg_stat_subscription_stats' conflict counters.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ConflictType {
    InsertExists = 0,
    UpdateOriginDiffers = 1,
    UpdateExists = 2,
    UpdateMissing = 3,
    DeleteOriginDiffers = 4,
    DeleteMissing = 5,
    MultipleUniqueConflicts = 6,
}

impl ConflictType {
    // ConflictTypeNames (conflict.c:26).
    pub(crate) fn name(self) -> &'static str {
        match self {
            ConflictType::InsertExists => "insert_exists",
            ConflictType::UpdateOriginDiffers => "update_origin_differs",
            ConflictType::UpdateExists => "update_exists",
            ConflictType::UpdateMissing => "update_missing",
            ConflictType::DeleteOriginDiffers => "delete_origin_differs",
            ConflictType::DeleteMissing => "delete_missing",
            ConflictType::MultipleUniqueConflicts => "multiple_unique_conflicts",
        }
    }

    // errcode_apply_conflict (conflict.c:169).
    fn errcode(self) -> SqlState {
        match self {
            ConflictType::InsertExists
            | ConflictType::UpdateExists
            | ConflictType::MultipleUniqueConflicts => ERRCODE_UNIQUE_VIOLATION,
            ConflictType::UpdateOriginDiffers
            | ConflictType::UpdateMissing
            | ConflictType::DeleteOriginDiffers
            | ConflictType::DeleteMissing => ERRCODE_T_R_SERIALIZATION_FAILURE,
        }
    }

    fn is_unique_conflict(self) -> bool {
        matches!(
            self,
            ConflictType::InsertExists
                | ConflictType::UpdateExists
                | ConflictType::MultipleUniqueConflicts
        )
    }
}

// ConflictTupleInfo (replication/conflict.h): one local row that caused the
// conflict and its transaction info. `slot` is None for the *_missing types
// (C's zeroed struct).
pub(crate) struct ConflictTupleInfo<'a, 'mcx> {
    pub slot: Option<&'a mut SlotData<'mcx>>,
    pub indexoid: Oid,
    pub xmin: TransactionId,
    pub origin: RepOriginId,
    pub ts: TimestampTz,
}

impl<'a, 'mcx> ConflictTupleInfo<'a, 'mcx> {
    // C's `ConflictTupleInfo conflicttuple = {0}` for the missing-row reports.
    pub(crate) fn missing() -> Self {
        ConflictTupleInfo { slot: None, indexoid: InvalidOid, xmin: 0, origin: InvalidRepOriginId, ts: 0 }
    }
}

// The origin of a local row's last modification, as errdetail_apply_conflict
// distinguishes it: None = InvalidRepOriginId (modified locally), Some(None) =
// an origin that no longer exists, Some(Some(name)) = a known origin.
pub(crate) type OriginName<'a> = Option<Option<&'a str>>;

// errdetail_apply_conflict's first part (conflict.c:200-275): the sentence
// explaining the conflict type. `localts` is None when the commit timestamp
// is 0 (no commit-ts data); `index_name` is the unique index for the
// *_exists types.
pub(crate) fn conflict_type_detail(
    ty: ConflictType,
    index_name: &str,
    localxmin: TransactionId,
    localts: Option<&str>,
    origin: OriginName<'_>,
) -> String {
    match ty {
        ConflictType::InsertExists
        | ConflictType::UpdateExists
        | ConflictType::MultipleUniqueConflicts => match (localts, origin) {
            (Some(ts), None) => format!(
                "Key already exists in unique index \"{index_name}\", modified locally in transaction {localxmin} at {ts}."
            ),
            (Some(ts), Some(Some(origin_name))) => format!(
                "Key already exists in unique index \"{index_name}\", modified by origin \"{origin_name}\" in transaction {localxmin} at {ts}."
            ),
            // The origin that modified this row has been removed.
            (Some(ts), Some(None)) => format!(
                "Key already exists in unique index \"{index_name}\", modified by a non-existent origin in transaction {localxmin} at {ts}."
            ),
            (None, _) => format!(
                "Key already exists in unique index \"{index_name}\", modified in transaction {localxmin}."
            ),
        },
        ConflictType::UpdateOriginDiffers | ConflictType::DeleteOriginDiffers => {
            let action = if ty == ConflictType::UpdateOriginDiffers { "Updating" } else { "Deleting" };
            let ts = localts.unwrap_or("");
            match origin {
                None => format!(
                    "{action} the row that was modified locally in transaction {localxmin} at {ts}."
                ),
                Some(Some(origin_name)) => format!(
                    "{action} the row that was modified by a different origin \"{origin_name}\" in transaction {localxmin} at {ts}."
                ),
                // The origin that modified this row has been removed.
                Some(None) => format!(
                    "{action} the row that was modified by a non-existent origin in transaction {localxmin} at {ts}."
                ),
            }
        }
        ConflictType::UpdateMissing => "Could not find the row to be updated.".to_string(),
        ConflictType::DeleteMissing => "Could not find the row to be deleted.".to_string(),
    }
}

// ReportApplyConflict (conflict.c:98): the errdetail is built from every
// conflicting tuple, the subscription's conflict counter is bumped, and the
// report is raised at `elevel` (ERROR for the *_exists types, LOG otherwise).
// `searchslot` is the tuple used to find the local row (UPDATE/DELETE);
// `remoteslot` the remote new tuple, if any.
pub(crate) fn report_apply_conflict<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    elevel: ErrorLevel,
    ty: ConflictType,
    mut searchslot: Option<&mut SlotData<'mcx>>,
    mut remoteslot: Option<&mut SlotData<'mcx>>,
    conflicttuples: &mut [ConflictTupleInfo<'_, 'mcx>],
) -> PgResult<()> {
    let mut err_detail = String::new();
    for ct in conflicttuples.iter_mut() {
        errdetail_apply_conflict(
            mcx,
            rel,
            ty,
            searchslot.as_deref_mut(),
            ct.slot.as_deref_mut(),
            remoteslot.as_deref_mut(),
            ct.indexoid,
            ct.xmin,
            ct.origin,
            ct.ts,
            &mut err_detail,
        )?;
    }

    pgstat::subscription::pgstat_report_subscription_conflict(my_sub(|s| s.oid), ty as usize);

    let nspname = lsyscache::get_namespace_name(mcx, rel.rd_rel.relnamespace)?;
    ereport(elevel)
        .errcode(ty.errcode())
        .errmsg(format!(
            "conflict detected on relation \"{}.{}\": conflict={}",
            nspname.as_ref().map(|s| s.as_str()).unwrap_or(""),
            rel.name(),
            ty.name()
        ))
        .errdetail_internal(err_detail)
        .finish(loc("ReportApplyConflict"))
}

// errdetail_apply_conflict (conflict.c:200): the conflict-type sentence, then
// the key / local row / remote row / replica identity values; one blank line
// separates successive tuples' details.
#[allow(clippy::too_many_arguments)]
fn errdetail_apply_conflict<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    ty: ConflictType,
    searchslot: Option<&mut SlotData<'mcx>>,
    localslot: Option<&mut SlotData<'mcx>>,
    remoteslot: Option<&mut SlotData<'mcx>>,
    indexoid: Oid,
    localxmin: TransactionId,
    localorigin: RepOriginId,
    localts: TimestampTz,
    err_msg: &mut String,
) -> PgResult<()> {
    let index_name = if ty.is_unique_conflict() {
        debug_assert!(indexoid != InvalidOid);
        lsyscache::get_rel_name(mcx, indexoid)?.map(|s| s.as_str().to_string()).unwrap_or_default()
    } else {
        String::new()
    };
    // For the *_exists types a zero timestamp selects the "modified in
    // transaction %u" form; the origin-differs types always print it.
    let ts_str;
    let localts_str = if localts != 0 || !ty.is_unique_conflict() {
        ts_str = adt_timestamp::timestamptz_to_str(localts);
        Some(ts_str.as_str())
    } else {
        None
    };
    let origin_name;
    let origin: OriginName<'_> = if localorigin == InvalidRepOriginId {
        None
    } else {
        origin_name = origin::replorigin_by_oid(mcx, localorigin, true)?;
        Some(origin_name.as_deref())
    };
    let mut err_detail = conflict_type_detail(ty, &index_name, localxmin, localts_str, origin);
    debug_assert!(!err_detail.is_empty());

    if let Some(val_desc) =
        build_tuple_value_details(mcx, rel, ty, searchslot, localslot, remoteslot, indexoid)?
    {
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

// build_tuple_value_details (conflict.c:293): None when the current user may
// not view any of the columns involved.
fn build_tuple_value_details<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    ty: ConflictType,
    searchslot: Option<&mut SlotData<'mcx>>,
    localslot: Option<&mut SlotData<'mcx>>,
    remoteslot: Option<&mut SlotData<'mcx>>,
    indexoid: Oid,
) -> PgResult<Option<String>> {
    debug_assert!(searchslot.is_some() || localslot.is_some() || remoteslot.is_some());
    let mut tuple_value = String::new();

    // Report the conflicting key values in the case of a unique constraint
    // violation.
    let mut localslot = localslot;
    if ty.is_unique_conflict() {
        debug_assert!(indexoid != InvalidOid && localslot.is_some());
        if let Some(slot) = localslot.as_deref_mut() {
            if let Some(desc) = build_index_value_desc(mcx, rel, slot, indexoid)? {
                tuple_value.push_str("Key ");
                tuple_value.push_str(&desc);
            }
        }
    }

    if let Some(slot) = localslot {
        // The 'modifiedCols' only applies to the new tuple, hence NULL for
        // the existing local row.
        if let Some(desc) = execpartition::slot_value_description(mcx, rel, slot, None, None)? {
            append_part(&mut tuple_value, "existing local row", "Existing local row", &desc);
        }
    }

    if let Some(slot) = remoteslot {
        // C unions ExecGetInsertedCols/ExecGetUpdatedCols of the apply
        // ResultRelInfo, whose RTE carries no permission info: an empty set.
        if let Some(desc) = execpartition::slot_value_description(mcx, rel, slot, None, None)? {
            append_part(&mut tuple_value, "remote row", "Remote row", &desc);
        }
    }

    if let Some(slot) = searchslot {
        // Only the replica identity (or PK) index describes the search
        // tuple; other usable indexes may not identify the row uniquely.
        let replica_index = logicalrelation::get_relation_identity_or_pk(mcx, rel)?;
        debug_assert!(ty != ConflictType::InsertExists);
        let desc = if replica_index != InvalidOid {
            build_index_value_desc(mcx, rel, slot, replica_index)?
        } else {
            execpartition::slot_value_description(mcx, rel, slot, None, None)?
        };
        if let Some(desc) = desc {
            if replica_index != InvalidOid {
                append_part(&mut tuple_value, "replica identity", "Replica identity", &desc);
            } else {
                append_part(
                    &mut tuple_value,
                    "replica identity full",
                    "Replica identity full",
                    &desc,
                );
            }
        }
    }

    if tuple_value.is_empty() {
        return Ok(None);
    }
    tuple_value.push('.');
    Ok(Some(tuple_value))
}

fn append_part(tuple_value: &mut String, label: &str, label_first: &str, desc: &str) {
    if tuple_value.is_empty() {
        tuple_value.push_str(label_first);
    } else {
        tuple_value.push_str("; ");
        tuple_value.push_str(label);
    }
    tuple_value.push(' ');
    tuple_value.push_str(desc);
}

// build_index_value_desc (conflict.c:434): the index entry's values for the
// row in `slot` (BuildIndexValueDescription over FormIndexDatum's raw
// datums). The caller holds the index locked.
fn build_index_value_desc<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    indexoid: Oid,
) -> PgResult<Option<String>> {
    let index_desc = indexam::index_open(mcx, indexoid, types_rel::NoLock)?;

    // If the slot is a virtual slot, copy it into a heap tuple slot as
    // FormIndexDatum only works with heap tuple slots.
    let mut copied;
    let tableslot: &mut SlotData<'mcx> = if slot.kind() == TupleSlotKind::Virtual {
        copied = tableam_real::table_slot_create(mcx, rel)?;
        exectuples::exec_copy_slot(&mut copied, slot, mcx, mcx)?;
        &mut copied
    } else {
        slot
    };

    let mut index_info = execindexing::BuildIndexInfo(mcx, &index_desc)?;
    let eval_cx = mcx::MemoryContext::new("ConflictIndexValue");
    let mut values = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut isnull = [false; INDEX_MAX_KEYS as usize];
    execindexing::FormIndexDatum(
        mcx,
        eval_cx.mcx(),
        &mut index_info,
        tableslot,
        &mut values,
        &mut isnull,
    )?;
    let index_value = genam::BuildIndexValueDescription(mcx, &index_desc, &values, &isnull)?;
    indexam::index_close(index_desc, types_rel::NoLock)?;
    Ok(index_value)
}

// InitConflictIndexes (conflict.c:142): the unique, non-deferrable indexes
// to check for a conflict — C's ri_onConflictArbiterIndexes of the apply
// ResultRelInfo.
pub(crate) fn init_conflict_indexes(state: &execindexing::ResultRelIndexState<'_>) -> Vec<Oid> {
    let mut unique_indexes = Vec::new();
    for i in 0..state.num_indices() {
        // Detect conflict only for unique indexes.
        if !state.infos[i].ii_Unique {
            continue;
        }
        // Don't support conflict detection for deferrable index.
        let index_form = state.descs[i].rd_index.as_ref().expect("index relation");
        if !index_form.indimmediate {
            continue;
        }
        unique_indexes.push(state.descs[i].rd_id);
    }
    unique_indexes
}

// BuildConflictIndexInfo (execReplication.c:438): the equality operator
// information check_exclusion_or_unique_constraint needs for the arbiter.
fn build_conflict_index_info(
    state: &mut execindexing::ResultRelIndexState<'_>,
    conflictindex: Oid,
) -> PgResult<()> {
    for i in 0..state.num_indices() {
        if state.descs[i].rd_id != conflictindex {
            continue;
        }
        // This would fail if BuildSpeculativeIndexInfo() were called twice
        // for the given index.
        debug_assert!(state.infos[i].ii_UniqueProcs[0] == InvalidOid);
        execindexing::BuildSpeculativeIndexInfo(&state.descs[i], &mut state.infos[i])?;
    }
    Ok(())
}

// FindConflictTuple (execReplication.c:468): the local tuple violating the
// unique index `conflictindex` for the row in `slot` (which is already
// inserted; its own tid is excluded), locked in share mode so it cannot be
// deleted before the caller reads it. A tuple deleted or updated before the
// lock is acquired restarts the search.
fn find_conflict_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    state: &mut execindexing::ResultRelIndexState<'mcx>,
    conflictindex: Oid,
    slot: &mut SlotData<'mcx>,
) -> PgResult<Option<SlotData<'mcx>>> {
    use tableam_real::{LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result};

    // Build additional information required to check constraints
    // violations. See check_exclusion_or_unique_constraint().
    build_conflict_index_info(state, conflictindex)?;

    let eval_cx = mcx::MemoryContext::new("FindConflictTuple");
    let mut existing_slot = tableam_real::table_slot_create(mcx, rel)?;
    loop {
        let mut conflict_tid = ItemPointerData::default();
        let self_tid = slot.base().tts_tid;
        if execindexing::ExecCheckIndexConstraints(
            mcx,
            eval_cx.mcx(),
            state,
            rel,
            slot,
            &mut existing_slot,
            &self_tid,
            &[conflictindex],
            &mut conflict_tid,
        )? {
            return Ok(None);
        }

        let mut conflictslot = tableam_real::table_slot_create(mcx, rel)?;
        snapmgr::PushActiveSnapshot(&snapmgr::GetLatestSnapshot()?)?;
        let mut tmfd = TM_FailureData::default();
        let res = (|| {
            let snap = Some(snapmgr::GetActiveSnapshot());
            let cid = xact::GetCurrentCommandId(false)?;
            tableam_real::table_tuple_lock(
                mcx,
                rel,
                &conflict_tid,
                &snap,
                &mut conflictslot,
                cid,
                LockTupleMode::LockTupleShare,
                LockWaitPolicy::LockWaitBlock,
                0, /* don't follow updates */
                &mut tmfd,
            )
        })();
        snapmgr::PopActiveSnapshot()?;
        let res = res?;
        // TM_SelfModified cannot come back for a row this worker does not
        // own; every other result goes through C's should_refetch_tuple.
        if let TM_Result::TM_SelfModified = res {
            return Ok(Some(conflictslot));
        }
        // tmfd.ctid is only filled for TM_Updated (C reads it in that arm
        // alone; ItemPointerGetOffsetNumber asserts on the zeroed one).
        let moved = matches!(res, TM_Result::TM_Updated)
            && types_tuple::ItemPointerIndicatesMovedPartitions(&tmfd.ctid);
        match should_refetch_tuple(res, moved)? {
            LockOutcome::Retry => continue,
            LockOutcome::Ok => return Ok(Some(conflictslot)),
        }
    }
}

// CheckAndReportConflict (execReplication.c:522): every arbiter index the
// insert flagged for recheck is searched for its conflicting local row; any
// found are reported as one ERROR (CT_MULTIPLE_UNIQUE_CONFLICTS when more
// than one index conflicts).
#[allow(clippy::too_many_arguments)]
pub(crate) fn check_and_report_conflict<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    state: &mut execindexing::ResultRelIndexState<'mcx>,
    ty: ConflictType,
    recheck_indexes: &[Oid],
    conflict_indexes: &[Oid],
    searchslot: Option<&mut SlotData<'mcx>>,
    remoteslot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    struct Found<'mcx> {
        slot: SlotData<'mcx>,
        indexoid: Oid,
        xmin: TransactionId,
        origin: RepOriginId,
        ts: TimestampTz,
    }
    let mut found: Vec<Found<'mcx>> = Vec::new();

    // Check all the unique indexes for conflicts.
    for &uniqueidx in conflict_indexes {
        if !recheck_indexes.contains(&uniqueidx) {
            continue;
        }
        let Some(conflictslot) = find_conflict_tuple(mcx, rel, state, uniqueidx, remoteslot)?
        else {
            continue;
        };
        let (xmin, ctsdata) = get_tuple_transaction_info(&conflictslot)?;
        let (ts, origin) = ctsdata.unwrap_or((0, InvalidRepOriginId));
        found.push(Found { slot: conflictslot, indexoid: uniqueidx, xmin, origin, ts });
    }

    // Report the conflict, if found.
    if found.is_empty() {
        return Ok(());
    }
    let ty = if found.len() > 1 { ConflictType::MultipleUniqueConflicts } else { ty };
    let mut infos: Vec<ConflictTupleInfo<'_, 'mcx>> = found
        .iter_mut()
        .map(|f| ConflictTupleInfo {
            slot: Some(&mut f.slot),
            indexoid: f.indexoid,
            xmin: f.xmin,
            origin: f.origin,
            ts: f.ts,
        })
        .collect();
    report_apply_conflict(mcx, rel, ERROR, ty, searchslot, Some(remoteslot), &mut infos)?;
    Ok(())
}
