// ExecCallTriggerFunc + TriggerEnabled (trigger.c), including the WHEN-qual
// compile-once cache (C ri_TrigWhenExprs) and the tgattr/modifiedCols gate.
use core::cell::Cell;
use core::ptr::NonNull;

use mcx::{Mcx, PgBox};
use types_error::{PgError, PgResult, ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED};
use types_fmgr::{FmgrInfo, LocalFcinfo, TRACK_FUNC_ALL};
use types_nodes::Bitmapset;
use types_nodes::primnodes::{INNER_VAR, OUTER_VAR};
use types_rel::Relation;
use types_slot::SlotData;
use types_trigger::{
    Trigger, TRIGGER_DISABLED, TRIGGER_EVENT_OPMASK, TRIGGER_EVENT_UPDATE,
    TRIGGER_FIRES_ON_ORIGIN, TRIGGER_FIRES_ON_REPLICA,
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

// TriggerEnabled's tgenabled gate (trigger.c:3488-3500); tgattr/tgqual are
// the caller's to handle.
pub fn TriggerEnabled(t: &Trigger<'_>) -> bool {
    if (guc_tables::vars::SessionReplicationRole.get().get)()
        == guc_tables::consts::SESSION_REPLICATION_ROLE_REPLICA
    {
        t.tgenabled != TRIGGER_DISABLED && t.tgenabled != TRIGGER_FIRES_ON_ORIGIN
    } else {
        t.tgenabled != TRIGGER_DISABLED && t.tgenabled != TRIGGER_FIRES_ON_REPLICA
    }
}

// build_generation_expression (rewriteHandler.c:4520), adbin-direct copy
// (rewrite_handler -> execmain -> trigger crate cycle; nodemodifytable
// precedent); cookDefault stored a coerced tree, so re-coercion is a no-op.
fn build_generation_expression<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attrno: usize,
) -> PgResult<types_nodes::Node<'mcx>> {
    let att = rel.rd_att.attr(attrno - 1);
    let constr = rel.rd_att.constr.as_deref().expect("caller checked");
    let adbin = constr
        .defval
        .iter()
        .find(|d| d.adnum == attrno as i16)
        .and_then(|d| d.adbin.as_ref())
        .unwrap_or_else(|| {
            panic!(
                "no generation expression found for column number {} of table \"{}\"",
                attrno,
                String::from_utf8_lossy(rel.rd_rel.relname.name_str())
            )
        });
    let expr = readfuncs::stringToNode(mcx, adbin.as_str())?;
    if att.attcollation != 0 && att.attcollation != nodes_core::node_funcs::expr_collation(expr) {
        return types_nodes::Node::mk(
            mcx,
            types_nodes::primnodes::CollateExpr {
                arg: expr,
                collOid: att.attcollation,
                location: -1,
            },
        );
    }
    Ok(expr)
}

// expand_generated_columns_in_expr (rewriteHandler.c:4493): Vars naming a
// virtual generated column of rel at varno become the generation expression
// (whose Vars are varno 1 == the WHEN qual's OLD position, matching C where
// expansion runs before ChangeVarNodes).
fn expand_generated_columns_in_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: types_nodes::Node<'mcx>,
    rel: &Relation<'mcx>,
    varno: i32,
) -> PgResult<Option<types_nodes::Node<'mcx>>> {
    const VIRTUAL_GEN: i8 = types_core::catalog::ATTRIBUTE_GENERATED_VIRTUAL as i8;
    if !rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_virtual) {
        return Ok(None);
    }
    if let Some(v) = node.as_var() {
        if v.varlevelsup != 0 || v.varno != varno {
            return Ok(None);
        }
        if v.varattno == 0 {
            // ReplaceVarsFromTargetList whole-row arm (rewriteManip.c:1801):
            // a named-rowtype whole-row Var becomes a RowExpr over per-field
            // Vars (dropped columns as NULL int4 consts, expandRTE shape),
            // each field then replaced so virtual columns expand.
            let mut args = types_nodes::list::NodeList::nil();
            for i in 0..rel.rd_att.natts as usize {
                let att = rel.rd_att.attr(i);
                let field = if att.attisdropped {
                    types_nodes::Node::mk_const(
                        mcx,
                        types_core::catalog::INT4OID,
                        -1,
                        0,
                        4,
                        datum::Datum::null(),
                        true,
                        true,
                    )?
                } else if att.attgenerated == VIRTUAL_GEN {
                    let e = build_generation_expression(mcx, rel, i + 1)?;
                    if varno != 1 {
                        rewrite_manip::ChangeVarNodes(mcx, e, 1, varno, 0)?;
                    }
                    e
                } else {
                    types_nodes::Node::mk_var(
                        mcx,
                        varno,
                        (i + 1) as i16,
                        att.atttypid,
                        att.atttypmod,
                        att.attcollation,
                        0,
                    )?
                };
                args.lappend(mcx, field)?;
            }
            return Ok(Some(types_nodes::Node::mk(
                mcx,
                types_nodes::RowExpr {
                    args,
                    row_typeid: v.vartype,
                    row_format: types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                    colnames: types_nodes::list::NodeList::nil(),
                    location: v.location,
                },
            )?));
        }
        if rel.rd_att.attr(v.varattno as usize - 1).attgenerated != VIRTUAL_GEN {
            return Ok(None);
        }
        let e = build_generation_expression(mcx, rel, v.varattno as usize)?;
        let e = if varno != 1 {
            rewrite_manip::ChangeVarNodes(mcx, e, 1, varno, 0)?;
            e
        } else {
            e
        };
        return Ok(Some(e));
    }
    clauses::walker::expression_tree_mutator(mcx, node, &mut |n| {
        expand_generated_columns_in_expr(mcx, n, rel, varno)
    })
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
        let tgqual = trigger.tgqual.as_ref().expect("caller checked tgqual");
        let mut qual = readfuncs::stringToNode(self.mcx, tgqual.as_str())?;
        // trigger.c:3553-3554: virtual generated Vars in the WHEN qual expand
        // to their generation expressions for both OLD and NEW references.
        qual = expand_generated_columns_in_expr(self.mcx, qual, rel, 1)?.unwrap_or(qual);
        qual = expand_generated_columns_in_expr(self.mcx, qual, rel, 2)?.unwrap_or(qual);
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

// ExecBSInsertTriggers (trigger.c), standalone-caller form (COPY FROM); the
// executor's INSERT path fires through nodemodifytable's exec_bs_triggers.
pub fn ExecBSInsertTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: &types_trigger::TriggerDesc<'static>,
    fmgr: &mut TriggerFmgrCache,
    when: &mut TriggerWhenEval<'_, 'mcx>,
) -> PgResult<()> {
    use types_trigger::{
        TRIGGER_EVENT_BEFORE, TRIGGER_EVENT_INSERT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT,
        TRIGGER_TYPE_LEVEL_MASK, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_TIMING_MASK,
    };
    if !trigdesc.trig_insert_before_statement {
        return Ok(());
    }
    let tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_BEFORE;
    for (i, trigger) in trigdesc.triggers.iter().enumerate() {
        if trigger.tgtype & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | TRIGGER_TYPE_INSERT)
            != TRIGGER_TYPE_STATEMENT | TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSERT
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

// ExecBRInsertTriggers (trigger.c), standalone-caller form (COPY FROM); the
// executor's INSERT path fires through nodemodifytable's br_row_triggers.
// false = a trigger returned NULL, suppressing the row.
pub fn ExecBRInsertTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: &types_trigger::TriggerDesc<'static>,
    fmgr: &mut TriggerFmgrCache,
    when: &mut TriggerWhenEval<'_, 'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    insert_row_triggers(mcx, rel, trigdesc, fmgr, when, slot, false)
}

// ExecIRInsertTriggers (trigger.c), standalone-caller form (COPY FROM into a
// view with an INSTEAD OF INSERT row trigger).
pub fn ExecIRInsertTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: &types_trigger::TriggerDesc<'static>,
    fmgr: &mut TriggerFmgrCache,
    when: &mut TriggerWhenEval<'_, 'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    insert_row_triggers(mcx, rel, trigdesc, fmgr, when, slot, true)
}

fn insert_row_triggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: &types_trigger::TriggerDesc<'static>,
    fmgr: &mut TriggerFmgrCache,
    when: &mut TriggerWhenEval<'_, 'mcx>,
    slot: &mut SlotData<'mcx>,
    instead: bool,
) -> PgResult<bool> {
    use types_trigger::{
        TRIGGER_EVENT_BEFORE, TRIGGER_EVENT_INSERT, TRIGGER_EVENT_INSTEAD, TRIGGER_EVENT_ROW,
        TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_LEVEL_MASK,
        TRIGGER_TYPE_ROW, TRIGGER_TYPE_TIMING_MASK,
    };
    let (type_timing, event_timing) = if instead {
        (TRIGGER_TYPE_INSTEAD, TRIGGER_EVENT_INSTEAD)
    } else {
        (TRIGGER_TYPE_BEFORE, TRIGGER_EVENT_BEFORE)
    };
    let tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW | event_timing;
    for (i, trigger) in trigdesc.triggers.iter().enumerate() {
        if trigger.tgtype & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | TRIGGER_TYPE_INSERT)
            != TRIGGER_TYPE_ROW | type_timing | TRIGGER_TYPE_INSERT
        {
            continue;
        }
        if !TriggerEnabled(trigger) {
            continue;
        }
        if !when.check(i, trigger, rel, tg_event, None, Some(slot))? {
            continue;
        }
        // C should_free_trig discipline (trigger.c): a Copied fetch owns the
        // image and must outlive the trigger call; freed after (end of
        // iteration), never before.
        let (img, len, tid, toid, _trig_owned) = {
            let fetched = exectuples::exec_fetch_slot_heap_tuple(slot, true, mcx, mcx)?;
            match fetched {
                exectuples::FetchedHeapTuple::Slot(t) => {
                    (t.header_ptr(), t.t_len, t.t_self, t.t_tableOid, None)
                }
                exectuples::FetchedHeapTuple::Copied(t) => {
                    (t.header_ptr(), t.t_len, t.t_self, t.t_tableOid, Some(t))
                }
            }
        };
        // SAFETY: a materialized query-context image; the slot is not written
        // while this handle lives within this iteration.
        let mut cur = unsafe { HeapTupleData::from_raw_parts(img, len, tid, toid) };
        let cur_nn = NonNull::from(&mut cur);
        let finfo = fmgr.get(i, trigger.tgfoid)?;
        let mut tdata =
            types_trigger_call::TriggerData::from_raw(tg_event, rel, Some(cur_nn), None, trigger);
        let ret = ExecCallTriggerFunc(mcx, &mut tdata, finfo)?;
        match ret {
            None => return Ok(false),
            Some(p) if p == cur_nn => {}
            Some(p) => {
                // SAFETY: the trigger's returned tuple, live in the per-call
                // context; copied into the slot before reuse.
                let returned = unsafe { p.as_ref() };
                let nulled = check_modified_virtual_generated(mcx, rel, returned)?;
                let returned = nulled.as_ref().map_or(returned, |t| t.as_tuple());
                let img = unsafe {
                    core::slice::from_raw_parts(returned.header_ptr(), returned.t_len as usize)
                };
                let mut buf = mcx::vec_with_capacity_in(mcx, img.len())?;
                mcx::vec_append_bytes(&mut buf, img).map_err(|_| mcx.oom(img.len()))?;
                let ptr = buf.as_ptr();
                core::mem::forget(buf);
                // SAFETY: fresh query-context copy of the returned image.
                let copy = unsafe {
                    HeapTupleData::from_raw_parts(
                        ptr,
                        returned.t_len,
                        returned.t_self,
                        returned.t_tableOid,
                    )
                };
                exectuples::exec_force_store_heap_tuple(copy, slot, mcx)?;
            }
        }
    }
    Ok(true)
}

// check_modified_virtual_generated (trigger.c:6735): a trigger-returned tuple
// must not carry a non-null value in a virtual generated column; offending
// columns revert to null. None means the tuple was already clean.
fn check_modified_virtual_generated<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tuple: &HeapTupleData<'_>,
) -> PgResult<Option<heaptuple::HeapTuple<'mcx>>> {
    const VIRTUAL_GEN: i8 = types_core::catalog::ATTRIBUTE_GENERATED_VIRTUAL as i8;
    let tupdesc = &*rel.rd_att;
    if !tupdesc.constr.as_deref().is_some_and(|c| c.has_generated_virtual) {
        return Ok(None);
    }
    let mut cols: mcx::PgVec<'_, i32> = mcx::PgVec::new_in(mcx);
    for i in 0..tupdesc.natts as usize {
        if tupdesc.attr(i).attgenerated == VIRTUAL_GEN
            && !types_tuple::heap_attisnull(tuple, i as i32 + 1, Some(tupdesc))
        {
            cols.push(i as i32 + 1);
        }
    }
    if cols.is_empty() {
        return Ok(None);
    }
    let mut values: mcx::PgVec<'_, datum::Datum> = mcx::vec_with_capacity_in(mcx, cols.len())?;
    let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, cols.len())?;
    for _ in 0..cols.len() {
        values.push(datum::Datum::null());
        isnull.push(true);
    }
    heaptuple::heap_modify_tuple_by_cols(mcx, tuple, tupdesc, &cols, &values, &isnull).map(Some)
}

thread_local! {
    static TRIGGER_DEPTH: Cell<i32> = const { Cell::new(0) };
}

pub fn trigger_depth() -> i32 {
    TRIGGER_DEPTH.with(|c| c.get())
}

// C: MyTriggerDepth++ / MyTriggerDepth-- around FunctionCallInvoke, the
// latter in PG_FINALLY so it runs even when the call errors; Drop gives the
// same guarantee across both the `?` early-return and the panic-unwind path.
struct TriggerDepthGuard;

impl TriggerDepthGuard {
    fn enter() -> Self {
        TRIGGER_DEPTH.with(|c| c.set(c.get() + 1));
        TriggerDepthGuard
    }
}

impl Drop for TriggerDepthGuard {
    fn drop(&mut self) {
        TRIGGER_DEPTH.with(|c| c.set(c.get() - 1));
    }
}

// The returned pointer's image lives in per_tuple_mcx: the 'a in the return
// type overstates validity — it dies at the per-tuple reset, and callers must
// consume or copy it before then (C: SPI trigger returns palloc'd in the
// per-tuple context).
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
    // C: pgstat_init_function_usage's `pgstat_track_functions <= fn_stats`
    // early-out, hoisted to the caller as the crate's API requires.
    let fcu = if finfo.fn_stats < TRACK_FUNC_ALL
        && ::pgstat::function::pgstat_track_functions() > finfo.fn_stats as i32
    {
        Some(::pgstat::function::pgstat_init_function_usage(finfo.fn_oid)?)
    } else {
        None
    };
    let depth_guard = TriggerDepthGuard::enter();
    let result = finfo.invoke(&mut fcinfo)?;
    drop(depth_guard);
    if let Some(fcu) = &fcu {
        ::pgstat::function::pgstat_end_function_usage(fcu, true);
    }
    if fcinfo.isnull {
        return Err(returned_null(finfo.fn_oid));
    }
    Ok(NonNull::new(result.as_usize() as *mut HeapTupleData<'a>))
}

#[track_caller]
#[cold]
#[inline(never)]
fn returned_null(fn_oid: types_core::Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("trigger function {fn_oid} returned null value"))
            .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
    )
}
