// ExecCallTriggerFunc + TriggerEnabled (trigger.c), including the WHEN-qual
// compile-once cache (C ri_TrigWhenExprs) and the tgattr/modifiedCols gate.
use core::ptr::NonNull;

use mcx::{Mcx, PgBox};
use types_error::{PgError, PgResult, ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED};
use types_fmgr::{FmgrInfo, LocalFcinfo};
use types_nodes::Bitmapset;
use types_nodes::primnodes::{INNER_VAR, OUTER_VAR};
use types_rel::Relation;
use types_slot::SlotData;
use types_trigger::{
    Trigger, TRIGGER_DISABLED, TRIGGER_EVENT_OPMASK, TRIGGER_EVENT_UPDATE,
    TRIGGER_FIRES_ON_REPLICA,
};
use types_trigger_call::TriggerData;
use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
use types_tuple::HeapTupleData;

// Resolve-once carrier for a TriggerDesc's functions (C ri_TrigFunctions).
#[derive(Default)]
pub struct TriggerFmgrCache {
    finfo: Vec<Option<FmgrInfo>>,
}

impl TriggerFmgrCache {
    pub fn get(&mut self, tgindx: usize, tgfoid: types_core::Oid) -> PgResult<&mut FmgrInfo> {
        if self.finfo.len() <= tgindx {
            self.finfo.resize_with(tgindx + 1, || None);
        }
        let slot = &mut self.finfo[tgindx];
        if slot.is_none() {
            *slot = Some(fmgr_seams::fmgr_info::call(tgfoid)?);
        }
        Ok(slot.as_mut().expect("just filled"))
    }
}

// TriggerEnabled's tgenabled gate (SESSION_REPLICATION_ROLE_ORIGIN, the only
// ported role); tgattr/tgqual are the caller's to handle.
pub fn TriggerEnabled(t: &Trigger<'_>) -> bool {
    t.tgenabled != TRIGGER_DISABLED && t.tgenabled != TRIGGER_FIRES_ON_REPLICA
}

// C ri_TrigWhenExprs: one compiled tgqual per trigdesc index, per query.
// Scratch slots serve the tuple-based AFTER save path (C evaluates against
// the executor's trigger slots; the queue only has fetched tuples).
#[derive(Default)]
pub struct TriggerWhenCache<'mcx> {
    states: Vec<Option<PgBox<'mcx, execexpr::ExprState<'mcx>>>>,
    scratch_old: Option<SlotData<'mcx>>,
    scratch_new: Option<SlotData<'mcx>>,
}

// The WHEN/UPDATE-OF half of C TriggerEnabled; borrows of the estate the
// caller owns (slots, updatedCols, query mcx).
pub struct TriggerWhenEval<'a, 'mcx> {
    pub mcx: Mcx<'mcx>,
    pub cache: &'a mut TriggerWhenCache<'mcx>,
    pub modified_cols: Option<&'a Bitmapset<'mcx>>,
}

impl<'a, 'mcx> TriggerWhenEval<'a, 'mcx> {
    fn attr_gate(&self, trigger: &Trigger<'_>, event: u32) -> bool {
        if trigger.tgnattr > 0 && event & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_UPDATE {
            let cols = self
                .modified_cols
                .expect("UPDATE trigger firing path supplies modifiedCols");
            return trigger
                .tgattr
                .iter()
                .any(|&a| cols.is_member(a as i32 - FirstLowInvalidHeapAttributeNumber));
        }
        true
    }

    fn compile(&mut self, idx: usize, trigger: &Trigger<'_>, rel: &Relation<'mcx>) -> PgResult<()> {
        if self.cache.states.len() <= idx {
            self.cache.states.resize_with(idx + 1, || None);
        }
        if self.cache.states[idx].is_some() {
            return Ok(());
        }
        if rel
            .rd_att
            .constr
            .as_ref()
            .is_some_and(|c| c.has_generated_stored || c.has_generated_virtual)
        {
            panic!(
                "TriggerEnabled (trigger.c): expand_generated_columns_in_expr \
                 over a WHEN qual unported (trigger {})",
                trigger.tgname.as_str()
            );
        }
        let tgqual = trigger.tgqual.as_ref().expect("caller checked tgqual");
        let qual = readfuncs::stringToNode(self.mcx, tgqual.as_str())?;
        rewrite_manip::ChangeVarNodes(self.mcx, qual, 1, INNER_VAR, 0)?;
        rewrite_manip::ChangeVarNodes(self.mcx, qual, 2, OUTER_VAR, 0)?;
        let implicit = clauses::make_ands_implicit(self.mcx, Some(qual))?;
        self.cache.states[idx] =
            execexpr::exec_init_qual(self.mcx, &implicit, execexpr::ParamBind::NONE)?;
        Ok(())
    }

    pub fn check(
        &mut self,
        idx: usize,
        trigger: &Trigger<'_>,
        rel: &Relation<'mcx>,
        event: u32,
        old_slot: Option<&mut SlotData<'mcx>>,
        new_slot: Option<&mut SlotData<'mcx>>,
    ) -> PgResult<bool> {
        if !self.attr_gate(trigger, event) {
            return Ok(false);
        }
        if trigger.tgqual.is_none() {
            return Ok(true);
        }
        self.compile(idx, trigger, rel)?;
        let mut slots = execexpr::EvalSlots { scan: None, inner: old_slot, outer: new_slot };
        execexpr::exec_qual(self.cache.states[idx].as_deref_mut(), &mut slots)
    }

    // The AFTER-save-path variant: tuples fetched by ctid, staged in scratch
    // heap slots for the qual (borrowed store, cleared before return).
    pub fn check_tuples(
        &mut self,
        idx: usize,
        trigger: &Trigger<'_>,
        rel: &Relation<'mcx>,
        event: u32,
        old_tup: Option<&HeapTupleData<'_>>,
        new_tup: Option<&HeapTupleData<'_>>,
    ) -> PgResult<bool> {
        if !self.attr_gate(trigger, event) {
            return Ok(false);
        }
        if trigger.tgqual.is_none() {
            return Ok(true);
        }
        self.compile(idx, trigger, rel)?;
        let mcx = self.mcx;
        let stage = |slot: &mut Option<SlotData<'mcx>>, tup: Option<&HeapTupleData<'_>>| {
            let Some(tup) = tup else { return Ok::<_, Box<PgError>>(None) };
            let s = slot.get_or_insert_with(|| {
                exectuples::make_tuple_table_slot(
                    mcx,
                    types_slot::TupleSlotKind::HeapTuple,
                    Some(rel.rd_att.clone()),
                )
            });
            // SAFETY: the image outlives this evaluation; the slot is cleared
            // before the caller's tuple borrow ends.
            let staged = unsafe {
                types_tuple::HeapTupleData::from_raw_parts(
                    tup.header_ptr(),
                    tup.t_len,
                    tup.t_self,
                    tup.t_tableOid,
                )
            };
            exectuples::exec_store_heap_tuple(s, mcx, staged);
            Ok(Some(()))
        };
        let TriggerWhenCache { states, scratch_old, scratch_new } = &mut *self.cache;
        stage(scratch_old, old_tup)?;
        stage(scratch_new, new_tup)?;
        let mut slots = execexpr::EvalSlots {
            scan: None,
            inner: if old_tup.is_some() { scratch_old.as_mut() } else { None },
            outer: if new_tup.is_some() { scratch_new.as_mut() } else { None },
        };
        let ok = execexpr::exec_qual(states[idx].as_deref_mut(), &mut slots)?;
        if let Some(s) = scratch_old.as_mut() {
            exectuples::exec_clear_tuple(s, mcx);
        }
        if let Some(s) = scratch_new.as_mut() {
            exectuples::exec_clear_tuple(s, mcx);
        }
        Ok(ok)
    }
}

// ExecBSTruncateTriggers (trigger.c); ExecAS lives with the queue.
pub fn ExecBSTruncateTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: &types_trigger::TriggerDesc<'static>,
    fmgr: &mut TriggerFmgrCache,
    when: &mut TriggerWhenEval<'_, 'mcx>,
) -> PgResult<()> {
    use types_trigger::{
        TRIGGER_EVENT_BEFORE, TRIGGER_EVENT_TRUNCATE, TRIGGER_TYPE_BEFORE,
        TRIGGER_TYPE_LEVEL_MASK, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_TIMING_MASK,
        TRIGGER_TYPE_TRUNCATE,
    };
    if !trigdesc.trig_truncate_before_statement {
        return Ok(());
    }
    let tg_event = TRIGGER_EVENT_TRUNCATE | TRIGGER_EVENT_BEFORE;
    for (i, trigger) in trigdesc.triggers.iter().enumerate() {
        if trigger.tgtype
            & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | TRIGGER_TYPE_TRUNCATE)
            != TRIGGER_TYPE_STATEMENT | TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_TRUNCATE
        {
            continue;
        }
        if !TriggerEnabled(trigger) {
            continue;
        }
        if !when.check(i, trigger, rel, tg_event, None, None)? {
            continue;
        }
        let finfo = fmgr.get(i, trigger.tgfoid)?;
        let mut tdata = TriggerData::new(tg_event, rel, None, None, trigger);
        if ExecCallTriggerFunc(mcx, &mut tdata, finfo)?.is_some() {
            return Err(Box::new(
                PgError::error("BEFORE STATEMENT trigger cannot return a value".to_string())
                    .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
            ));
        }
    }
    Ok(())
}

pub fn ExecCallTriggerFunc<'a, 'mcx>(
    per_tuple_mcx: Mcx<'_>,
    trigdata: &mut TriggerData<'a, 'mcx>,
    finfo: &mut FmgrInfo,
) -> PgResult<Option<NonNull<HeapTupleData<'a>>>> {
    debug_assert_eq!(finfo.fn_oid, trigdata.tg_trigger.tgfoid);
    let mut fcinfo = LocalFcinfo::<0>::fresh(types_core::InvalidOid);
    fcinfo.context = trigdata.fm_node_ptr();
    // SAFETY: the scratch context outlives this single invocation.
    unsafe { fcinfo.set_result_mcx(per_tuple_mcx) };
    let result = finfo.invoke(&mut fcinfo)?;
    if fcinfo.isnull {
        return Err(returned_null(finfo.fn_oid));
    }
    Ok(NonNull::new(result.as_usize() as *mut HeapTupleData<'a>))
}

#[cold]
#[inline(never)]
fn returned_null(fn_oid: types_core::Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("trigger function {fn_oid} returned null value"))
            .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
    )
}
