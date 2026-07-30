//! commands/constraint.c: unique_key_recheck (deferred exclusion + deferred
//! unique arms).
#![allow(non_snake_case)]

use datum::Datum;
use types_core::{Oid, INDEX_MAX_KEYS};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_trigger::{
    TRIGGER_FIRED_AFTER, TRIGGER_FIRED_BY_INSERT, TRIGGER_FIRED_BY_UPDATE, TRIGGER_FIRED_FOR_ROW,
};
use types_trigger_call::trigger_data_from_fcinfo;

#[track_caller]
#[cold]
#[inline(never)]
fn protocol_err(msg: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("unique_key_recheck: {msg}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

pub fn fc_unique_key_recheck(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: the trigger call machinery keeps the TriggerData live for the
    // duration of the call.
    let Some(td) = (unsafe { trigger_data_from_fcinfo(fcinfo) }) else {
        return Err(protocol_err("not fired by trigger manager"));
    };
    if !TRIGGER_FIRED_AFTER(td.tg_event) || !TRIGGER_FIRED_FOR_ROW(td.tg_event) {
        return Err(protocol_err("must be fired AFTER ROW"));
    }
    let new_row = if TRIGGER_FIRED_BY_INSERT(td.tg_event) {
        td.tg_trigtuple
    } else if TRIGGER_FIRED_BY_UPDATE(td.tg_event) {
        td.tg_newtuple
    } else {
        return Err(protocol_err("must be fired for INSERT or UPDATE"));
    };
    // SAFETY: live tuple per the trigger call contract.
    let checktid = unsafe { new_row.expect("trigger row").as_ref() }.t_self;
    // table_index_fetch_tuple advances this to the live HOT member; the
    // unique arm must keep probing with the original TID — that is the one
    // the index knows about (constraint.c:169-176).
    let mut tmptid = checktid;

    let mcx = fcinfo.result_mcx();
    let trig_rel = td.tg_relation;

    // SnapshotSelf re-find: a dead HOT member resolves to its live successor.
    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        ::types_slot::TupleSlotKind::BufferHeapTuple,
        Some(trig_rel.rd_att.clone()),
    );
    let self_snap = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        mcx,
        ::types_snapshot::SnapshotType::SNAPSHOT_SELF,
    ));
    let mut fetch = tableam::table_index_fetch_begin(trig_rel);
    let mut call_again = false;
    let mut all_dead = false;
    let found = tableam::table_index_fetch_tuple(
        mcx,
        &mut fetch,
        &mut tmptid,
        &mut Some(self_snap),
        &mut slot,
        &mut call_again,
        Some(&mut all_dead),
    )?;
    tableam::table_index_fetch_end(fetch);
    if !found {
        return Ok(Datum::from_usize(0));
    }

    let index_rel = indexam::index_open(
        mcx,
        td.tg_trigger.tgconstrindid,
        ::types_rel::RowExclusiveLock,
    )?;
    let mut index_info = execindexing::BuildIndexInfo(mcx, &index_rel)?;

    let mut values = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut isnull = [false; INDEX_MAX_KEYS as usize];
    execindexing::FormIndexDatum(mcx, mcx, &mut index_info, &mut slot, &mut values, &mut isnull)?;

    if index_info.ii_HasExclusion {
        execindexing::check_exclusion_constraint(
            mcx,
            mcx,
            trig_rel,
            &index_rel,
            &mut index_info,
            &tmptid,
            &values,
            &isnull,
            false,
        )?;
    } else {
        // Deferred unique: re-run the duplicate check against the already-
        // inserted entry (constraint.c:170; the EXISTING mode never inserts).
        let n = index_info.ii_NumIndexAttrs as usize;
        indexam::index_insert(
            mcx,
            &index_rel,
            &values[..n],
            &isnull[..n],
            &checktid,
            trig_rel,
            ::types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_EXISTING,
            false,
            &mut index_info.ii_AmCache,
        )?;
    }

    // C's ExecDropSingleTupleTableSlot (constraint.c:193): the fetched-tuple
    // slot pins a heap buffer; the success path must release it.
    exectuples::exec_clear_tuple(&mut slot, mcx);

    indexam::index_close(index_rel, ::types_rel::RowExclusiveLock)?;
    Ok(Datum::from_usize(0))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const CONSTRAINT_BUILTINS: &[FmgrBuiltin] = &[b(1250, "unique_key_recheck", 1, fc_unique_key_recheck)];
