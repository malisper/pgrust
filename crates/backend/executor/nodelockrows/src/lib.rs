// nodeLockRows.c over the shared-estate EPQ (execmain::epq).
#![allow(non_snake_case)]

use ::executils::{EStateData, EpqSubs, ExecSlotId};
use ::mcx::PgVec;
use ::tableam_vocab::{
    LockTupleMode, TM_FailureData, TM_Result, TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
    TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS,
};
use ::types_error::{PgError, PgResult, ERRCODE_T_R_SERIALIZATION_FAILURE};
use ::types_nodes::list::NodeList;
use ::types_nodes::plannodes::{LockRows, RowMarkType};
use ::types_slot::EXEC_FLAG_MARK;
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

pub struct ExecAuxRowMark {
    pub rti: u32,
    pub ctidAttNo: i16,
    pub toidAttNo: i16,
    pub wholeAttNo: i16,
    // C has no slot here (EvalPlanQualSlot at use time); the port makes it at
    // init for the locking marks only — non-locking (EPQ-list) marks never
    // read it.
    pub mark_slot: Option<ExecSlotId>,
}

pub struct LockRowsState<'mcx> {
    pub plan: &'mcx LockRows<'mcx>,
    pub lr_arowMarks: PgVec<'mcx, ExecAuxRowMark>,
    /// C's EvalPlanQualInit aux list: non-locked rels' junk-attr re-fetch
    /// marks, installed into relsubs_rowmark when an EPQ recheck fires.
    pub lr_epq_arowMarks: PgVec<'mcx, ExecAuxRowMark>,
    /// This node's C EPQState.relsubs_* (execmain swaps them live per run).
    pub epq_subs: Option<EpqSubs<'mcx>>,
}

// EvalPlanQualSlot (execMain.c): the markSlot IS the rel's EPQ test slot,
// parked in this owner's relsubs.
fn eval_plan_qual_slot<'mcx>(
    subs_opt: &mut Option<EpqSubs<'mcx>>,
    estate: &mut EStateData<'mcx>,
    rti: u32,
) -> ExecSlotId {
    let mcx = estate.es_query_cxt;
    let subs = ::executils::ensure_epq_subs(subs_opt, mcx, estate.epq_rtsize(), rti);
    let idx = (rti - 1) as usize;
    if let Some(id) = subs.relsubs_slot[idx] {
        return id;
    }
    let (kind, desc) = {
        let rel = estate.es_relations[idx].as_ref().expect("rowmark relation opened");
        (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
    };
    let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
    let id = ExecSlotId(estate.es_tupleTable.len() as u32);
    estate.es_tupleTable.push(slot);
    subs_opt.as_mut().expect("just ensured").relsubs_slot[idx] = Some(id);
    id
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

/// `ExecInitLockRows` minus child linkage; `outer_tlist` is the outer *plan*
/// targetlist (junk-column resnos live there).
pub fn exec_init_lock_rows<'mcx>(
    node: &'mcx LockRows<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_tlist: &NodeList<'mcx>,
) -> PgResult<LockRowsState<'mcx>> {
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);
    let mut lr_arowMarks: PgVec<'mcx, ExecAuxRowMark> = PgVec::new_in(estate.es_query_cxt);
    let mut lr_epq_arowMarks: PgVec<'mcx, ExecAuxRowMark> = PgVec::new_in(estate.es_query_cxt);
    let mut epq_subs: Option<EpqSubs<'mcx>> = None;
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
        // ExecBuildAuxRowMark (execMain.c): ctid junk for every method but
        // COPY; wholerow junk for COPY; tableoid junk for child rels.
        let (ctidAttNo, wholeAttNo) = if erm.markType != RowMarkType::ROW_MARK_COPY {
            let ctid_name = format!("ctid{}", erm.rowmarkId);
            let n = find_junk_attribute_in_tlist(outer_tlist, &ctid_name);
            if n == 0 {
                return Err(internal(&format!("could not find junk {ctid_name} column")));
            }
            (n, 0)
        } else {
            let whole_name = format!("wholerow{}", erm.rowmarkId);
            let n = find_junk_attribute_in_tlist(outer_tlist, &whole_name);
            if n == 0 {
                return Err(internal(&format!("could not find junk {whole_name} column")));
            }
            (0, n)
        };
        let toidAttNo = if erm.rti != erm.prti {
            let toid_name = format!("tableoid{}", erm.rowmarkId);
            let n = find_junk_attribute_in_tlist(outer_tlist, &toid_name);
            if n == 0 {
                return Err(internal(&format!("could not find junk {toid_name} column")));
            }
            n
        } else {
            0
        };
        if erm.markType.requires_row_share_lock() {
            estate.exec_get_range_table_relation(rc.rti, false)?;
            let mark_slot = Some(eval_plan_qual_slot(&mut epq_subs, estate, rc.rti));
            lr_arowMarks.push(ExecAuxRowMark {
                rti: rc.rti,
                ctidAttNo,
                toidAttNo,
                wholeAttNo,
                mark_slot,
            });
        } else {
            lr_epq_arowMarks.push(ExecAuxRowMark {
                rti: rc.rti,
                ctidAttNo,
                toidAttNo,
                wholeAttNo,
                mark_slot: None,
            });
        }
    }
    Ok(LockRowsState { plan: node, lr_arowMarks, lr_epq_arowMarks, epq_subs })
}

/// `ExecLockRows`; C's goto lnext becomes the loop over the extracted
/// per-row body (`lr_accept_row`): pull, lock-and-judge, emit or skip.
pub fn exec_lock_rows<'mcx, C: LockRowsChild<'mcx>>(
    node: &mut LockRowsState<'mcx>,
    child: &mut C,
    estate: &mut EStateData<'mcx>,
    mut epq_eval: impl FnMut(
        &mut Option<EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    cfi()?;
    loop {
        let Some(slot_id) = child.exec_proc(estate)? else {
            return Ok(None);
        };
        if let Some(out) = lr_accept_row(node, estate, slot_id, &mut epq_eval)? {
            return Ok(Some(out));
        }
    }
}

/// One fetched child row through the ExecLockRows body (wave-3 WS-T seam
/// `lr_accept_row` — the `'lnext` loop body of `exec_lock_rows` as a PURE
/// CODE MOVE; both engines drive this exact function: the Volcano/delegation
/// arm through the loop above, the lane's inc-2b TupleOp hosting through
/// `LockRowsOp::accept` in execmain's lanev2/dml.rs). `Some` = the row (or
/// its EPQ-substituted latest version) locked and emitted; `None` = the row
/// was skipped (the former `continue 'lnext` arms: WouldBlock,
/// SelfModified's Halloween guard, a concurrently deleted row under READ
/// COMMITTED, or a failed EPQ recheck) and the caller pulls the next one.
// #[inline(always)]: restore the literal loop-body codegen exec_lock_rows
// had before the extraction (the mt_* seam se2-cost-fix precedent — the
// knob-OFF Volcano arm must not pay for the seam decomposition).
#[inline(always)]
pub fn lr_accept_row<'mcx>(
    node: &mut LockRowsState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
    epq_eval: &mut impl FnMut(
        &mut Option<EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    // The 4-space-deep block below is the former 'lnext loop body verbatim
    // (pure code move): each `continue 'lnext` (row skipped, pull the next)
    // is now `return Ok(None)`, the terminal emits are `Ok(Some(..))`.
    {
        let mut epq_needed = false;

        for i in 0..node.lr_arowMarks.len() {
            let (rti, ctid_att, toid_att, mark_slot) = {
                let aerm = &node.lr_arowMarks[i];
                (
                    aerm.rti,
                    aerm.ctidAttNo,
                    aerm.toidAttNo,
                    aerm.mark_slot.expect("locking mark slot made at init"),
                )
            };
            let mut erm = estate.es_rowmarks[(rti - 1) as usize].expect("locking rowmark");
            // Clear any leftover EPQ test tuple for this rel (C does this
            // before the child check so inactive children are cleared too).
            {
                let mcx = estate.es_query_cxt;
                let mark = &mut estate.es_tupleTable[mark_slot.0 as usize];
                exectuples::exec_clear_tuple(mark, mcx);
            }
            // Child rel of an inherited/partitioned FOR UPDATE: check whether
            // it produced this row (nodeLockRows.c:92-112).
            if erm.rti != erm.prti {
                debug_assert!(toid_att > 0);
                let mut isnull = false;
                let datum = exectuples::slot_getattr(
                    estate.slot_mut(slot_id),
                    toid_att as i32,
                    &mut isnull,
                );
                if isnull {
                    return Err(internal("tableoid is NULL"));
                }
                if datum.as_oid() != erm.relid {
                    // this child is inactive right now
                    erm.ermActive = false;
                    types_tuple::itemptr::ItemPointerSetInvalid(&mut erm.curCtid);
                    estate.es_rowmarks[(rti - 1) as usize] = Some(erm);
                    continue;
                }
            } else {
                debug_assert!(toid_att == 0);
            }
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
                TM_Result::TM_WouldBlock => return Ok(None),
                // Halloween guard: self-modified rows are skipped, not re-fetched.
                TM_Result::TM_SelfModified => return Ok(None),
                TM_Result::TM_Ok => {
                    if tmfd.traversed {
                        epq_needed = true;
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
                    return Ok(None);
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

        if epq_needed {
            // Locked latest versions already sit in the EPQ test slots.
            // C EvalPlanQualStart's relsubs_rowmark loop + EvalPlanQualSetSlot:
            // every non-locked rel must re-return the row it contributed to
            // THIS join output (junk ctid/wholerow re-fetch) — rescanning it
            // re-emits all rows, and a parameterized-inner recheck then
            // consumes the locked rel's test tuple at the wrong outer row and
            // silently skips the row (epqjoin lane).
            {
                let subs = node
                    .epq_subs
                    .as_mut()
                    .expect("locking mark slot made at init created the subs");
                for aerm in node.lr_epq_arowMarks.iter() {
                    let fetch = if aerm.wholeAttNo > 0 {
                        ::executils::EpqRowMarkFetch::Copy { whole_attno: aerm.wholeAttNo }
                    } else {
                        debug_assert!(aerm.ctidAttNo > 0);
                        ::executils::EpqRowMarkFetch::Reference { ctid_attno: aerm.ctidAttNo }
                    };
                    subs.relsubs_rowmark[(aerm.rti - 1) as usize] = Some(fetch);
                }
                subs.origslot = Some(slot_id);
            }
            let input =
                node.lr_arowMarks[0].mark_slot.expect("locking mark slot made at init");
            let Some(epqslot) = epq_eval(&mut node.epq_subs, estate, input)? else {
                // Recheck says the latest version no longer passes: skip.
                return Ok(None);
            };
            return Ok(Some(epqslot));
        }

        Ok(Some(slot_id))
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

#[track_caller]
#[cold]
#[inline(never)]
fn serialization_failure() -> Box<PgError> {
    Box::new(
        PgError::error("could not serialize access due to concurrent update".to_string())
            .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn internal(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()))
}

mcx::forget_safe_nodrop!(ExecAuxRowMark);

mcx::forget_safe_struct!(
    LockRowsState<'_> { plan, lr_arowMarks, lr_epq_arowMarks, epq_subs },
);
