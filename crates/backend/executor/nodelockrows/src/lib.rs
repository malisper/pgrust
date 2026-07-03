// nodeLockRows.c, non-EPQ core; EvalPlanQual arms are named panic seams.
#![allow(non_snake_case)]

use ::executils::{EStateData, ExecSlotId};
use ::mcx::PgVec;
use ::tableam_vocab::{
    LockTupleMode, TM_FailureData, TM_Result, TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
    TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS,
};
use ::types_error::{PgError, PgResult, ERRCODE_T_R_SERIALIZATION_FAILURE};
use ::types_nodes::list::NodeList;
use ::types_nodes::plannodes::{LockRows, RowMarkType};
use ::types_slot::{TupleSlotKind, EXEC_FLAG_MARK};
use ::types_tuple::ItemPointerData;

pub fn init_seams() {}

#[inline(always)]
fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

pub trait LockRowsChild<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;
}

/// C ExecAuxRowMark; `rti` links to es_rowmarks, `mark_slot` plays
/// EvalPlanQualSlot's per-rel slot (the locked tuple lands there).
pub struct ExecAuxRowMark {
    pub rti: u32,
    pub ctidAttNo: i16,
    pub toidAttNo: i16,
    pub wholeAttNo: i16,
    pub mark_slot: ExecSlotId,
}

pub struct LockRowsState<'mcx> {
    pub plan: &'mcx LockRows<'mcx>,
    pub lr_arowMarks: PgVec<'mcx, ExecAuxRowMark>,
}

/// `ExecFindJunkAttributeInTlist` (execJunk.c).
fn find_junk_attribute_in_tlist(tlist: &NodeList<'_>, name: &str) -> i16 {
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("targetlist cell");
        if tle.resjunk && tle.resname == Some(name) {
            return tle.resno;
        }
    }
    0
}

/// `ExecInitLockRows` minus child linkage (caller inits the outer child with
/// the unmodified eflags, as C does). `outer_tlist` is the outer *plan*
/// targetlist (junk-column resnos live there).
pub fn exec_init_lock_rows<'mcx>(
    node: &'mcx LockRows<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_tlist: &NodeList<'mcx>,
) -> PgResult<LockRowsState<'mcx>> {
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);
    let mut lr_arowMarks: PgVec<'mcx, ExecAuxRowMark> = PgVec::new_in(estate.es_query_cxt);
    for rc_node in &node.rowMarks {
        let rc = rc_node.as_plan_row_mark().expect("rowMarks cell is a PlanRowMark");
        if rc.isParent {
            continue;
        }
        let rte = estate.exec_rt_fetch(rc.rti);
        if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_RELATION
            && !estate.es_unpruned_relids.is_member(rc.rti as i32)
        {
            continue;
        }
        let erm = estate.es_rowmarks[(rc.rti - 1) as usize]
            .expect("InitPlan built the ExecRowMark for every PlanRowMark rti");
        if !erm.markType.requires_row_share_lock() {
            panic!(
                "ExecInitLockRows (nodeLockRows.c): non-locking rowmark \
                 (EvalPlanQualInit aux list) — EPQ lane"
            );
        }
        let ctid_name = format!("ctid{}", erm.rowmarkId);
        let ctidAttNo = find_junk_attribute_in_tlist(outer_tlist, &ctid_name);
        assert!(ctidAttNo != 0, "could not find junk {ctid_name} column");
        let toidAttNo = if erm.rti != erm.prti {
            let toid_name = format!("tableoid{}", erm.rowmarkId);
            let n = find_junk_attribute_in_tlist(outer_tlist, &toid_name);
            assert!(n != 0, "could not find junk {toid_name} column");
            n
        } else {
            0
        };
        let desc = {
            let rel = estate.exec_get_range_table_relation(rc.rti, false)?;
            rel.rd_att.clone()
        };
        let mark_slot =
            estate.exec_init_extra_tuple_slot(Some(desc), TupleSlotKind::BufferHeapTuple);
        lr_arowMarks.push(ExecAuxRowMark {
            rti: rc.rti,
            ctidAttNo,
            toidAttNo,
            wholeAttNo: 0,
            mark_slot,
        });
    }
    Ok(LockRowsState { plan: node, lr_arowMarks })
}

/// `ExecLockRows`; C's goto lnext becomes the labeled outer loop.
pub fn exec_lock_rows<'mcx, C: LockRowsChild<'mcx>>(
    node: &mut LockRowsState<'mcx>,
    child: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    cfi()?;
    'lnext: loop {
        let Some(slot_id) = child.exec_proc(estate)? else {
            return Ok(None);
        };

        for i in 0..node.lr_arowMarks.len() {
            let (rti, ctid_att, toid_att, mark_slot) = {
                let aerm = &node.lr_arowMarks[i];
                (aerm.rti, aerm.ctidAttNo, aerm.toidAttNo, aerm.mark_slot)
            };
            let mut erm = estate.es_rowmarks[(rti - 1) as usize].expect("locking rowmark");
            debug_assert!(toid_att == 0 && erm.rti == erm.prti);
            erm.ermActive = true;

            let mut isnull = false;
            let datum = exectuples::slot_getattr(
                estate.slot_mut(slot_id),
                ctid_att as i32,
                &mut isnull,
            );
            if isnull {
                return Err(internal("ctid is NULL"));
            }
            // SAFETY: the junk ctid datum points at t_self inside the outer
            // slot's tuple, live for this row (heap_getsysattr contract).
            let tid = unsafe { *(datum.as_usize() as *const ItemPointerData) };

            let lockmode = match erm.markType {
                RowMarkType::ROW_MARK_EXCLUSIVE => LockTupleMode::LockTupleExclusive,
                RowMarkType::ROW_MARK_NOKEYEXCLUSIVE => LockTupleMode::LockTupleNoKeyExclusive,
                RowMarkType::ROW_MARK_SHARE => LockTupleMode::LockTupleShare,
                RowMarkType::ROW_MARK_KEYSHARE => LockTupleMode::LockTupleKeyShare,
                other => return Err(internal(&format!("unsupported rowmark type {other:?}"))),
            };
            let mut lockflags = TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS;
            if !xact_seams::isolation_uses_xact_snapshot::call() {
                lockflags |= TUPLE_LOCK_FLAG_FIND_LAST_VERSION;
            }

            let mcx = estate.es_query_cxt;
            let output_cid = estate.es_output_cid;
            let wait_policy = to_am_wait_policy(erm.waitPolicy);
            let mut tmfd = TM_FailureData::default();
            let test = {
                let ::executils::EStateData {
                    es_relations,
                    es_tupleTable,
                    es_snapshot,
                    ..
                } = estate;
                let rel = es_relations[(rti - 1) as usize]
                    .as_ref()
                    .expect("rowmark relation opened at init");
                let mark = &mut es_tupleTable[mark_slot.0 as usize];
                exectuples::exec_clear_tuple(mark, mcx);
                let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
                tableam::table_tuple_lock(
                    mcx,
                    rel,
                    &tid,
                    snapshot,
                    mark,
                    output_cid,
                    lockmode,
                    wait_policy,
                    lockflags,
                    &mut tmfd,
                )?
            };

            match test {
                TM_Result::TM_WouldBlock => continue 'lnext,
                // Halloween guard: self-modified rows are skipped, not re-fetched.
                TM_Result::TM_SelfModified => continue 'lnext,
                TM_Result::TM_Ok => {
                    if tmfd.traversed {
                        panic!(
                            "ExecLockRows (nodeLockRows.c): locked a later row \
                             version — EvalPlanQual recheck; EPQ lane"
                        );
                    }
                }
                TM_Result::TM_Updated => {
                    if xact_seams::isolation_uses_xact_snapshot::call() {
                        return Err(serialization_failure());
                    }
                    return Err(internal(&format!(
                        "unexpected table_tuple_lock status: {}",
                        test as u32
                    )));
                }
                TM_Result::TM_Deleted => {
                    if xact_seams::isolation_uses_xact_snapshot::call() {
                        return Err(serialization_failure());
                    }
                    continue 'lnext;
                }
                TM_Result::TM_Invisible => {
                    return Err(internal("attempted to lock invisible tuple"))
                }
                other => {
                    return Err(internal(&format!(
                        "unrecognized table_tuple_lock status: {}",
                        other as u32
                    )))
                }
            }

            erm.curCtid = tid;
            estate.es_rowmarks[(rti - 1) as usize] = Some(erm);
        }

        return Ok(Some(slot_id));
    }
}

// tableam_vocab carries its own lockoptions.h mirror; values are pinned equal.
fn to_am_wait_policy(p: types_nodes::LockWaitPolicy) -> ::tableam_vocab::LockWaitPolicy {
    match p {
        types_nodes::LockWaitPolicy::LockWaitBlock => {
            ::tableam_vocab::LockWaitPolicy::LockWaitBlock
        }
        types_nodes::LockWaitPolicy::LockWaitSkip => ::tableam_vocab::LockWaitPolicy::LockWaitSkip,
        types_nodes::LockWaitPolicy::LockWaitError => {
            ::tableam_vocab::LockWaitPolicy::LockWaitError
        }
    }
}

#[cold]
#[inline(never)]
fn serialization_failure() -> Box<PgError> {
    Box::new(
        PgError::error("could not serialize access due to concurrent update".to_string())
            .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE),
    )
}

#[cold]
#[inline(never)]
fn internal(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()))
}
