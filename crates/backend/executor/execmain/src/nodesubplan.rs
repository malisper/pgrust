//! nodeSubplan.c: initplans (ExecInitSubPlan + ExecSetParamPlan for
//! uncorrelated EXISTS/EXPR/ARRAY/ROWCOMPARE/MULTIEXPR) plus regular SubPlan
//! execution — ExecSubPlan's scan lane (EXISTS/EXPR/ANY/ALL/ARRAY/ROWCOMPARE/
//! correlated-MULTIEXPR with per-call rescan + parParam binding) and the
//! HASHED ANY lane (buildSubPlanHash/findPartialMatch, C's three-valued NULL
//! semantics). CTE expression arms are loud.

use core::ptr::NonNull;
use std::rc::Rc;

use ::datum::NullableDatum;
use ::execexpr::{exec_project, ExprState};
use ::executils::{EStateData, EcxtId, ExecSlotId, SubplanStateCell};
use ::mcx::{Mcx, PgBox, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_CARDINALITY_VIOLATION};
use ::types_fmgr::{FmgrInfo, LocalFcinfo};
use ::types_nodes::bitmapset::Bitmapset;
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::primnodes::{SubLinkType, SubPlan, TargetEntry};
use ::types_nodes::NodeTag;
use ::types_scan::sdir::ScanDirection;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::TupleDescData;
use ::datum as Datum_crate;
use Datum_crate::Datum;

use crate::procnode::{exec_proc_node, with_eval_slots, with_eval_slots_outer, PlanStateNode};

pub(crate) struct SubPlanState<'mcx> {
    sub_link_type: SubLinkType,
    first_col_type: ::types_core::Oid,
    set_param: PgVec<'mcx, i32>,
    /// The subplan's PlanState (es_subplanstates cell); taken out for the
    /// duration of a run so same-plan re-entry is a loud panic, not aliasing.
    ps_cell: core::ptr::NonNull<Option<PlanStateNode<'mcx>>>,
}

// ExecInitSubPlan's NULL-planstate check: fails if the planner mistakenly
// puts a parallel-unsafe subplan into a parallelized subquery; see
// ExecSerializePlan.
fn check_subplan_initialized(cell: NonNull<()>, subplan: &SubPlan<'_>) -> PgResult<()> {
    // SAFETY: es_subplanstates cells are arena-live Option<PlanStateNode>
    // installed by InitPlan; shared read, no take-out here.
    if unsafe { (*cell.cast::<Option<PlanStateNode<'_>>>().as_ptr()).is_none() } {
        return Err(Box::new(PgError::error(format!(
            "subplan \"{}\" was not initialized",
            subplan.plan_name.unwrap_or("?")
        ))));
    }
    Ok(())
}

/// `ExecInitSubPlan` (nodeSubplan.c), initPlan arm: parks the SubPlanState on
/// every setParam so the first param read runs the subplan.
pub(crate) fn exec_init_sub_plan<'mcx>(
    subplan: &SubPlan<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // C: setParam wiring skips CTE_SUBLINK — the cteParam slot carries the
    // leader handshake (nodeCtescan), never an execPlan trampoline.
    if subplan.subLinkType == SubLinkType::CTE_SUBLINK {
        debug_assert!(
            subplan.parParam.is_nil() && !subplan.useHashTable && subplan.testexpr.is_none()
        );
        return Ok(());
    }
    if !subplan.parParam.is_nil() || subplan.useHashTable || subplan.testexpr.is_some() {
        panic!(
            "ExecInitSubPlan (nodeSubplan.c): correlated/hashed/testexpr SubPlan \
             \"{}\" — only uncorrelated initplans are ported",
            subplan.plan_name.unwrap_or("?")
        );
    }
    if !matches!(
        subplan.subLinkType,
        SubLinkType::EXISTS_SUBLINK
            | SubLinkType::EXPR_SUBLINK
            | SubLinkType::ARRAY_SUBLINK
            | SubLinkType::ROWCOMPARE_SUBLINK
            | SubLinkType::MULTIEXPR_SUBLINK
    ) {
        panic!(
            "ExecInitSubPlan (nodeSubplan.c): {:?} initplan not ported",
            subplan.subLinkType
        );
    }
    let cell = estate
        .es_subplanstates
        .get((subplan.plan_id - 1) as usize)
        .unwrap_or_else(|| panic!("subplan \"{}\" was not initialized", subplan.plan_name.unwrap_or("?")))
        .0;
    check_subplan_initialized(cell, subplan)?;

    let mut set_param: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    for id in subplan.setParam.iter() {
        set_param.push(id);
    }
    debug_assert!(
        set_param.len() == 1
            || matches!(
                subplan.subLinkType,
                SubLinkType::ROWCOMPARE_SUBLINK | SubLinkType::MULTIEXPR_SUBLINK
            )
    );

    let mut boxed = ::mcx::alloc_in(
        mcx,
        SubPlanState {
            sub_link_type: subplan.subLinkType,
            first_col_type: subplan.firstColType,
            set_param,
            ps_cell: cell.cast(),
        },
    )?;
    let raw: *mut SubPlanState<'mcx> = &mut *boxed;
    // Forget-on-reset: the arena reclaims the bytes at es_query_cxt reset; the
    // skipped drop is only a PgVec header whose buffer is the same arena.
    core::mem::forget(boxed);
    // SAFETY: raw comes from a live arena allocation.
    let erased = SubplanStateCell(unsafe { core::ptr::NonNull::new_unchecked(raw) }.cast());
    // SAFETY: same allocation, shared read after the forget.
    let sstate: &SubPlanState<'mcx> = unsafe { &*raw };

    for id in sstate.set_param.iter() {
        let pid = *id as usize;
        estate.es_param_exec_vals[pid].exec_plan = true;
        estate.es_param_subplans[pid] = Some(erased);
    }
    Ok(())
}

/// The [`executils::CteProcHook`] impl: one ExecProcNode pull from an
/// es_subplanstates cell (CteScanNext's subplan fetch). Take-out protocol as
/// exec_set_param_plan: same-cell re-entry is a loud panic, not aliasing.
///
/// # Safety
/// `cell` is an es_subplanstates entry installed by InitPlan on this estate.
pub(crate) unsafe fn cte_proc_hook<'mcx>(
    cell: ::executils::SubplanStateCell,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<::executils::ExecSlotId>> {
    // SAFETY: caller contract; the 'mcx erased here is the estate's own.
    let slot = unsafe { &mut *cell.0.cast::<Option<PlanStateNode<'mcx>>>().as_ptr() };
    let mut ps = slot
        .take()
        .unwrap_or_else(|| panic!("recursive CTE plan execution (nodeCtescan.c)"));
    let result = exec_proc_node(&mut ps, estate);
    *slot = Some(ps);
    result
}

/// The [`executils::SubplanHook`] impl; installed once per query in InitPlan.
///
/// # Safety
/// `p` is an es_query_cxt-lifetime SubPlanState installed by
/// [`exec_init_sub_plan`] on the same estate.
pub(crate) unsafe fn subplan_hook(
    p: core::ptr::NonNull<()>,
    estate: &mut EStateData<'_>,
) -> PgResult<()> {
    // SAFETY: caller contract; the 'mcx erased here is the estate's own.
    let sstate = unsafe { &*p.cast::<SubPlanState<'_>>().as_ptr() };
    exec_set_param_plan(sstate, estate)
}

// ExecSetParamPlan (nodeSubplan.c), EXISTS/EXPR arms. Divergence: C copies the
// whole result tuple (curTuple, freed per re-run); with one setParam column a
// datumCopy of that column into es_query_cxt is the same boundary. Re-runs
// (execami rescan_mark_initplans' eager rescan + exec_plan re-mark) leak the
// prior copy into the query arena until executor end, bounded by rescan count.
fn exec_set_param_plan<'mcx>(
    sstate: &SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // SAFETY: es_query_cxt-lifetime cell; exclusive by the take-out protocol
    // (a nested take of the same cell panics below).
    let cell = unsafe { &mut *sstate.ps_cell.as_ptr() };
    let mut ps = cell
        .take()
        .unwrap_or_else(|| panic!("recursive initplan execution (nodeSubplan.c)"));

    let saved_dir = estate.es_direction;
    estate.es_direction = ScanDirection::ForwardScanDirection;

    let result = run_subplan(sstate, &mut ps, estate);

    estate.es_direction = saved_dir;
    *cell = Some(ps);
    result
}

fn run_subplan<'mcx>(
    sstate: &SubPlanState<'mcx>,
    ps: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let mut found = false;
    let mut values: PgVec<'mcx, NullableDatum> = PgVec::new_in(mcx);
    let mut astate = if sstate.sub_link_type == SubLinkType::ARRAY_SUBLINK {
        Some(::arrayfuncs::build::init_array_result_any(mcx, sstate.first_col_type, true)?)
    } else {
        None
    };

    while let Some(slot_id) = exec_proc_node(ps, estate)? {
        match sstate.sub_link_type {
            SubLinkType::EXISTS_SUBLINK => {
                found = true;
                break;
            }
            SubLinkType::ARRAY_SUBLINK => {
                found = true;
                let slot = estate.slot_mut(slot_id);
                let mut disnull = false;
                let dvalue = exectuples::slot_getattr(slot, 1, &mut disnull);
                astate = Some(::arrayfuncs::build::accum_array_result_any(
                    mcx,
                    astate.take(),
                    dvalue,
                    disnull,
                    sstate.first_col_type,
                )?);
            }
            SubLinkType::EXPR_SUBLINK
            | SubLinkType::ROWCOMPARE_SUBLINK
            | SubLinkType::MULTIEXPR_SUBLINK => {
                if found {
                    return Err(too_many_rows());
                }
                found = true;
                values.clear();
                let slot = estate.slot_mut(slot_id);
                for col in 0..sstate.set_param.len() {
                    let (attlen, attbyval) = {
                        let desc = slot
                            .base()
                            .tts_tupleDescriptor
                            .as_ref()
                            .expect("subplan result slot has a descriptor");
                        (desc.attrs[col].attlen, desc.attrs[col].attbyval)
                    };
                    let mut vnull = false;
                    let v = exectuples::slot_getattr(slot, col as i32 + 1, &mut vnull);
                    let v = if vnull || attbyval { v } else { datum_copy_in(mcx, v, attlen)? };
                    values.push(NullableDatum { value: v, isnull: vnull });
                }
            }
            other => unreachable!("{other:?} initplan is loud at init"),
        }
    }

    match sstate.sub_link_type {
        SubLinkType::EXISTS_SUBLINK => {
            let prm = &mut estate.es_param_exec_vals[sstate.set_param[0] as usize];
            prm.exec_plan = false;
            prm.value = Datum::from_bool(found);
            prm.isnull = false;
        }
        SubLinkType::ARRAY_SUBLINK => {
            let bytes = ::arrayfuncs::build::make_array_result_any(
                mcx,
                &astate.expect("astate initialized"),
            )?;
            let prm = &mut estate.es_param_exec_vals[sstate.set_param[0] as usize];
            prm.exec_plan = false;
            prm.value = Datum::from_usize(bytes.leak().as_ptr() as usize);
            prm.isnull = false;
        }
        SubLinkType::EXPR_SUBLINK
        | SubLinkType::ROWCOMPARE_SUBLINK
        | SubLinkType::MULTIEXPR_SUBLINK => {
            for (i, &pid) in sstate.set_param.iter().enumerate() {
                let prm = &mut estate.es_param_exec_vals[pid as usize];
                prm.exec_plan = false;
                if found {
                    prm.value = values[i].value;
                    prm.isnull = values[i].isnull;
                } else {
                    prm.value = Datum::null();
                    prm.isnull = true;
                }
            }
        }
        other => unreachable!("{other:?} initplan is loud at init"),
    }
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_rows() -> Box<PgError> {
    Box::new(
        PgError::error("more than one row returned by a subquery used as an expression".to_string())
            .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION),
    )
}

// datumCopy (datum.c) into es_query_cxt (fold.rs precedent); short and toast
// headers copy verbatim per C, only expanded flattens (no producers: loud).
pub(crate) fn datum_copy_in<'mcx>(mcx: Mcx<'mcx>, value: Datum_crate::Datum, attlen: i16) -> PgResult<Datum_crate::Datum> {
    let p = value.as_usize() as *const u8;
    if p.is_null() {
        return Ok(Datum::null());
    }
    let size = datum_image_size(value, attlen);
    // SAFETY: `size` bytes readable per datum_image_size's arms.
    let src = unsafe { core::slice::from_raw_parts(p, size) };
    let out = ::mcx::slice_in(mcx, src)?;
    Ok(Datum::from_usize(out.leak().as_ptr() as usize))
}

/// UpdateChangedParamSet (execAmi.c) over the estate's SubPlan states: chg ∩
/// allParam accumulates on the subplan (C planstate->chgParam); the next
/// evaluation's rescan carries it into the subplan tree (buildSubPlanHash /
/// ExecScanSubPlan rescan with chgParam) so initplans inside the subplan
/// re-mark their output params. A non-empty accumulated set is also
/// ExecHashSubPlan's rebuild trigger.
pub(crate) fn mark_hashed_subplans_stale<'mcx>(
    estate: &mut EStateData<'mcx>,
    chg: &::types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    for i in 0..estate.es_subplan_expr_states.len() {
        let (p, _) = estate.es_subplan_expr_states[i];
        // SAFETY: entries are SubPlanExprStates leaked on this estate.
        let sstate = unsafe { &mut *p.cast::<SubPlanExprState<'mcx>>().as_ptr() };
        let all_param = &sstate.plan.as_plan().expect("plan node").allParam;
        if !chg.overlap(all_param) {
            continue;
        }
        let parmset = chg.intersect(all_param, mcx)?;
        sstate.chg_param.add_members(mcx, &parmset)?;
        if let Some(h) = sstate.hashed.as_mut() {
            h.built = false;
        }
    }
    Ok(())
}

pub(crate) struct SubPlanExprState<'mcx> {
    sub_link_type: SubLinkType,
    first_col_type: ::types_core::Oid,
    par_param: PgVec<'mcx, i32>,
    param_ids: PgVec<'mcx, i32>,
    /// MULTIEXPR output params (C subplan->setParam); nil otherwise.
    set_param: PgVec<'mcx, i32>,
    /// Accumulated changed params (C SubPlanState planstate->chgParam),
    /// consumed by the next evaluation's rescan of the subplan tree.
    chg_param: ::types_nodes::bitmapset::Bitmapset<'mcx>,
    plan: Node<'mcx>,
    ps_cell: NonNull<Option<PlanStateNode<'mcx>>>,
    testexpr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    cur_buf: PgVec<'mcx, u8>,
    /// ARRAY accumulation scratch, reset per evaluation (C builds in the
    /// caller's per-tuple context).
    array_ctx: Option<::mcx::MemoryContext>,
    hashed: Option<HashedSubPlanState<'mcx>>,
}

struct HashedSubPlanState<'mcx> {
    unknown_eq_false: bool,
    built: bool,
    havehashrows: bool,
    havenullrows: bool,
    hashtable: Option<::execgrouping::TupleHashTable<'mcx>>,
    hashnulls: Option<::execgrouping::TupleHashTable<'mcx>>,
    table_ctx: ::mcx::MemoryContext,
    // C sstate->hashtempcxt: probe-time transient memory (detoast copies of
    // compressed by-ref keys above all), reset after each hashtable lookup
    // (build scan) / probe (ExecHashSubPlan) — never query-lifetime.
    hashtempcxt: ::mcx::MemoryContext,
    key_col_idx: PgVec<'mcx, i16>,
    tab_eq_funcoids: PgVec<'mcx, ::types_core::Oid>,
    tab_hash_funcs: PgVec<'mcx, ::types_core::Oid>,
    tab_collations: PgVec<'mcx, ::types_core::Oid>,
    cur_eq_funcs: PgVec<'mcx, FmgrInfo>,
    lhs_hash_funcs: PgVec<'mcx, FmgrInfo>,
    cross_type: bool,
    proj_left: PgBox<'mcx, ExprState<'mcx>>,
    proj_right: PgBox<'mcx, ExprState<'mcx>>,
    lhs_slot: ExecSlotId,
    rhs_slot: ExecSlotId,
    desc_right: Rc<TupleDescData<'mcx>>,
    probe_slot: SlotData<'mcx>,
}

unsafe fn drop_subplan_expr_state(p: NonNull<()>) {
    // SAFETY: p was leaked by exec_init_sub_plan_expr; runs once at
    // standard_executor_end (droppy hash tables must not forget-on-reset).
    unsafe { core::ptr::drop_in_place(p.cast::<SubPlanExprState<'_>>().as_ptr()) };
}

/// The [`executils::SubplanInitHook`]/`SubplanCompileEnv.init` impl:
/// `ExecInitSubPlan` (nodeSubplan.c), regular-SubPlan arm.
///
/// # Safety
/// `estate_p` is the live estate the expression is being compiled for, with
/// no aliasing borrows during the call; `node` comes from its plan tree.
pub(crate) unsafe fn subplan_expr_init_hook(
    estate_p: NonNull<()>,
    node: Node<'_>,
    agg: Option<::execexpr::AggBind>,
) -> PgResult<NonNull<()>> {
    // SAFETY: caller contract; the erased lifetime is the estate's own.
    let estate = unsafe { &mut *estate_p.cast::<EStateData<'_>>().as_ptr() };
    let node = unsafe { core::mem::transmute::<Node<'_>, Node<'_>>(node) };
    exec_init_sub_plan_expr(node.as_sub_plan().expect("SubPlan node"), estate, agg)
}

fn exec_init_sub_plan_expr<'mcx>(
    subplan: &SubPlan<'mcx>,
    estate: &mut EStateData<'mcx>,
    agg: Option<::execexpr::AggBind>,
) -> PgResult<NonNull<()>> {
    let mcx = estate.es_query_cxt;
    // C ExecSubPlan's sanity check: only a MULTIEXPR SubPlan may carry
    // setParams into the expression lane.
    assert!(
        subplan.setParam.is_nil() || subplan.subLinkType == SubLinkType::MULTIEXPR_SUBLINK,
        "cannot set parent params from subquery"
    );
    let cell = estate
        .es_subplanstates
        .get((subplan.plan_id - 1) as usize)
        .unwrap_or_else(|| {
            panic!("subplan \"{}\" was not initialized", subplan.plan_name.unwrap_or("?"))
        })
        .0;
    check_subplan_initialized(cell, subplan)?;
    let plan = estate
        .es_plannedstmt
        .expect("es_plannedstmt set before plan init")
        .subplans
        .nth((subplan.plan_id - 1) as usize)
        .expect("initialized subplan has a plan cell");

    if !matches!(
        subplan.subLinkType,
        SubLinkType::EXISTS_SUBLINK
            | SubLinkType::EXPR_SUBLINK
            | SubLinkType::ANY_SUBLINK
            | SubLinkType::ALL_SUBLINK
            | SubLinkType::ARRAY_SUBLINK
            | SubLinkType::ROWCOMPARE_SUBLINK
            | SubLinkType::MULTIEXPR_SUBLINK
    ) {
        panic!(
            "ExecInitSubPlan (nodeSubplan.c): {:?} expression SubPlan not ported",
            subplan.subLinkType
        );
    }
    let params = estate.param_bind();
    let nested_rtable = ::executils::subplan_env_rtable(estate);
    let nested_env = ::execexpr::SubplanCompileEnv {
        estate: NonNull::from(&mut *estate).cast(),
        init: Some(subplan_expr_init_hook),
        agg,
        rtable: Some(nested_rtable),
        parent_subplan_tlist: None,
    };
    let testexpr = ::execexpr::exec_init_expr_subplans_agg(
        mcx,
        subplan.testexpr,
        params,
        Some(nested_env),
        agg,
    )?;

    let mut par_param: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    par_param.extend(subplan.parParam.iter());
    let mut param_ids: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    param_ids.extend(subplan.paramIds.iter());
    let mut set_param: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    set_param.extend(subplan.setParam.iter());

    let hashed = if subplan.useHashTable {
        Some(init_hashed_state(subplan, estate, params, agg)?)
    } else {
        None
    };

    let array_ctx = if subplan.subLinkType == SubLinkType::ARRAY_SUBLINK {
        Some(mcx.context().new_child_bump("Subplan Array Context"))
    } else {
        None
    };
    let boxed = ::mcx::alloc_in(
        mcx,
        SubPlanExprState {
            sub_link_type: subplan.subLinkType,
            first_col_type: subplan.firstColType,
            par_param,
            param_ids,
            set_param,
            chg_param: Bitmapset::empty(),
            plan,
            ps_cell: cell.cast(),
            testexpr,
            cur_buf: PgVec::new_in(mcx),
            array_ctx,
            hashed,
        },
    )?;
    let raw: NonNull<SubPlanExprState<'mcx>> = NonNull::from(&*PgBox::leak(boxed));
    estate
        .es_subplan_expr_states
        .push((raw.cast(), drop_subplan_expr_state));
    Ok(raw.cast())
}

fn init_hashed_state<'mcx>(
    subplan: &SubPlan<'mcx>,
    estate: &mut EStateData<'mcx>,
    params: ::execexpr::ParamBind<'mcx>,
    agg: Option<::execexpr::AggBind>,
) -> PgResult<HashedSubPlanState<'mcx>> {
    let mcx = estate.es_query_cxt;
    let testexpr = subplan.testexpr.expect("hashed SubPlan has a testexpr");
    let mut oplist: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    if testexpr.node_tag() == NodeTag::T_OpExpr {
        oplist.push(testexpr);
    } else if let Some(b) = testexpr.as_bool_expr() {
        debug_assert!(b.boolop == ::types_nodes::primnodes::BoolExprType::AND_EXPR);
        oplist.extend(b.args.iter());
    } else {
        panic!("unrecognized testexpr type: {:?}", testexpr.node_tag());
    }

    let ncols = oplist.len();
    let mut key_col_idx: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    let mut tab_eq_funcoids: PgVec<'mcx, ::types_core::Oid> = PgVec::new_in(mcx);
    let mut tab_hash_funcs: PgVec<'mcx, ::types_core::Oid> = PgVec::new_in(mcx);
    let mut tab_collations: PgVec<'mcx, ::types_core::Oid> = PgVec::new_in(mcx);
    let mut cur_eq_funcs: PgVec<'mcx, FmgrInfo> = PgVec::new_in(mcx);
    let mut lhs_hash_funcs: PgVec<'mcx, FmgrInfo> = PgVec::new_in(mcx);
    let mut cross_type = false;
    let mut lefttlist = NodeList::nil();
    let mut righttlist = NodeList::nil();

    for (i, op_node) in oplist.iter().enumerate() {
        let opexpr = op_node.as_op_expr().expect("hashable testexpr arm is an OpExpr");
        debug_assert_eq!(opexpr.args.len(), 2);
        let larg = opexpr.args.nth(0);
        let rarg = opexpr.args.nth(1);
        lefttlist.lappend(
            mcx,
            Node::mk(
                mcx,
                TargetEntry {
                    expr: larg,
                    resno: (i + 1) as i16,
                    resname: None,
                    ressortgroupref: 0,
                    resorigtbl: 0,
                    resorigcol: 0,
                    resjunk: false,
                },
            )?,
        )?;
        righttlist.lappend(
            mcx,
            Node::mk(
                mcx,
                TargetEntry {
                    expr: rarg,
                    resno: (i + 1) as i16,
                    resname: None,
                    ressortgroupref: 0,
                    resorigtbl: 0,
                    resorigcol: 0,
                    resjunk: false,
                },
            )?,
        )?;

        let mut flinfo = fmgr_core::fmgr_info(opexpr.opfuncid)?;
        flinfo.fn_expr = Some(::execexpr::erase_fn_expr(mcx, *op_node)?);
        cur_eq_funcs.push(flinfo);

        let (_, rhs_eq_oper) = lsyscache::get_compatible_hash_operators(opexpr.opno)?
            .unwrap_or_else(|| {
                panic!(
                    "could not find compatible hash operator for operator {}",
                    opexpr.opno
                )
            });
        tab_eq_funcoids.push(lsyscache::get_opcode(rhs_eq_oper)?);
        let (left_hashfn, right_hashfn) = lsyscache::get_op_hash_functions(opexpr.opno)?
            .unwrap_or_else(|| {
                panic!("could not find hash function for hash operator {}", opexpr.opno)
            });
        lhs_hash_funcs.push(fmgr_core::fmgr_info(left_hashfn)?);
        tab_hash_funcs.push(right_hashfn);
        tab_collations.push(opexpr.inputcollid);
        key_col_idx.push((i + 1) as i16);
        if left_hashfn != right_hashfn
            || ::execexpr::expr_type(larg) != ::execexpr::expr_type(rarg)
        {
            cross_type = true;
        }
    }
    debug_assert_eq!(key_col_idx.len(), ncols);

    let desc_left = ::execscan::exec_type_from_tl(mcx, &lefttlist)?;
    let desc_right = ::execscan::exec_type_from_tl(mcx, &righttlist)?;
    let lhs_slot =
        estate.exec_init_extra_tuple_slot(Some(desc_left), TupleSlotKind::Virtual);
    let rhs_slot =
        estate.exec_init_extra_tuple_slot(Some(desc_right.clone()), TupleSlotKind::Virtual);
    let probe_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc_right.clone()));

    let nested_rtable = ::executils::subplan_env_rtable(estate);
    let nested_env = ::execexpr::SubplanCompileEnv {
        estate: NonNull::from(&mut *estate).cast(),
        init: Some(subplan_expr_init_hook),
        agg,
        rtable: Some(nested_rtable),
        parent_subplan_tlist: None,
    };
    let proj_left = match agg {
        Some(a) => ::execexpr::exec_build_agg_projection_info_subplans(
            mcx,
            &lefttlist,
            None,
            a,
            params,
            Some(nested_env),
        )?,
        None => ::execexpr::exec_build_projection_info_subplans(
            mcx,
            &lefttlist,
            None,
            params,
            Some(nested_env),
        )?,
    };
    let proj_right = ::execexpr::exec_build_projection_info(mcx, &righttlist, None, params)?;

    Ok(HashedSubPlanState {
        cross_type,
        lhs_hash_funcs,
        unknown_eq_false: subplan.unknownEqFalse,
        built: false,
        havehashrows: false,
        havenullrows: false,
        hashtable: None,
        hashnulls: None,
        table_ctx: mcx.context().new_child_bump("Subplan HashTable Context"),
        hashtempcxt: mcx.context().new_child_bump("Subplan HashTable Temp Context"),
        key_col_idx,
        tab_eq_funcoids,
        tab_hash_funcs,
        tab_collations,
        cur_eq_funcs,
        proj_left,
        proj_right,
        lhs_slot,
        rhs_slot,
        desc_right,
        probe_slot,
    })
}

/// The [`executils::SubplanEvalHook`] impl: `ExecSubPlan` (nodeSubplan.c).
///
/// # Safety
/// `p` is a SubPlanExprState installed by [`subplan_expr_init_hook`] on the
/// same estate; `ecxt` is the owning node's ExprContext with its slot triple
/// current for the row being evaluated; `outer`, when present, is the owning
/// node's explicit outer row living outside es_tupleTable (it overrides the
/// ecxt outer for testexpr/LHS-projection evals — C econtext->ecxt_outertuple).
pub(crate) unsafe fn subplan_expr_eval_hook<'a, 'b, 'mcx>(
    p: NonNull<()>,
    estate: &'a mut EStateData<'mcx>,
    ecxt: EcxtId,
    outer: Option<&'b mut SlotData<'mcx>>,
) -> PgResult<NullableDatum> {
    // SAFETY: caller contract; the 'mcx erased here is the estate's own.
    let sstate = unsafe { &mut *p.cast::<SubPlanExprState<'_>>().as_ptr() };
    let saved_dir = estate.es_direction;
    estate.es_direction = ScanDirection::ForwardScanDirection;
    let result = if sstate.hashed.is_some() {
        exec_hash_sub_plan(sstate, estate, ecxt, outer)
    } else {
        exec_scan_sub_plan(sstate, estate, ecxt, outer)
    };
    estate.es_direction = saved_dir;
    result
}

fn exec_scan_sub_plan<'mcx>(
    sstate: &mut SubPlanExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    outer: Option<&mut SlotData<'mcx>>,
) -> PgResult<NullableDatum> {
    // SAFETY: es_query_cxt-lifetime cell; exclusive by the take-out protocol.
    let cell = unsafe { &mut *sstate.ps_cell.as_ptr() };
    let mut ps = cell
        .take()
        .unwrap_or_else(|| panic!("recursive subplan execution (nodeSubplan.c)"));
    let result = scan_sub_plan_loop(sstate, &mut ps, estate, ecxt, outer);
    *cell = Some(ps);
    result
}

// C ExecEvalExpr recurses into nested ExecEvalSubPlan; the decomposed
// interpreter surfaces nested SubPlans as suspensions, pumped here through
// ExecSubPlan itself (testexpr / projLeft carry nested SubPlans in C).
fn eval_expr_nested_subplans<'mcx>(
    state: &mut ExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    mut outer: Option<&mut SlotData<'mcx>>,
) -> PgResult<NullableDatum> {
    let mut resume: Option<::execexpr::Resume> = None;
    loop {
        let outcome = {
            let r = resume.take();
            let state = &mut *state;
            with_eval_slots_outer(estate, ecxt, None, outer.as_deref_mut(), move |slots, _, _| {
                ::execexpr::exec_eval_expr_outcome(state, slots, r)
            })?
        };
        match outcome {
            ::execexpr::EvalOutcome::Done(nd) => return Ok(nd),
            ::execexpr::EvalOutcome::Suspended(s) => {
                // SAFETY: the suspension's sstate was installed by
                // subplan_expr_init_hook on this estate (nested compile env).
                let v = unsafe {
                    subplan_expr_eval_hook(s.sstate, estate, ecxt, outer.as_deref_mut())
                }?;
                resume = Some(s.resume_with(v));
            }
        }
    }
}

// [`eval_expr_nested_subplans`] for the hashed lane's LHS projection
// (C ExecProject(projLeft); the lefthand exprs may carry nested SubPlans).
fn project_lhs_nested_subplans<'mcx>(
    proj: &mut ExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    lhs_slot: ExecSlotId,
    mut outer: Option<&mut SlotData<'mcx>>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    proj.arm_result_mcx(mcx);
    exectuples::exec_clear_tuple(estate.slot_mut(lhs_slot), mcx);
    let mut resume: Option<::execexpr::Resume> = None;
    loop {
        let suspended = {
            let r = resume.take();
            let proj = &mut *proj;
            with_eval_slots_outer(
                estate,
                ecxt,
                Some(lhs_slot),
                outer.as_deref_mut(),
                move |slots, rslot, _| {
                    ::execexpr::exec_project_outcome(proj, slots, rslot.expect("lhs slot"), r)
                },
            )?
        };
        match suspended {
            None => {
                exectuples::exec_store_virtual_tuple(estate.slot_mut(lhs_slot));
                return Ok(());
            }
            Some(s) => {
                // SAFETY: as eval_expr_nested_subplans.
                let v = unsafe {
                    subplan_expr_eval_hook(s.sstate, estate, ecxt, outer.as_deref_mut())
                }?;
                resume = Some(s.resume_with(v));
            }
        }
    }
}

fn scan_sub_plan_loop<'mcx>(
    sstate: &mut SubPlanExprState<'mcx>,
    ps: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    mut outer: Option<&mut SlotData<'mcx>>,
) -> PgResult<NullableDatum> {
    let mcx = estate.es_query_cxt;
    let link = sstate.sub_link_type;
    if let Some(te) = sstate.testexpr.as_deref_mut() {
        // C evaluates the testexpr in ecxt_per_tuple_memory (RowExpr allocs).
        // SAFETY: the ExprContext outlives the plan (reset-only).
        unsafe { te.arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx()) };
    }
    // ExecScanSubPlan: parParam ids join the accumulated chgParam before the
    // rescan; the accumulated set is consumed here (C clears node->chgParam
    // at the end of ExecReScan).
    let mut chg = core::mem::replace(&mut sstate.chg_param, Bitmapset::empty());
    for &id in sstate.par_param.iter() {
        chg.add_member(mcx, id)?;
    }
    if chg.is_empty() {
        crate::execami::exec_re_scan(ps, estate)?;
    } else {
        crate::execami::exec_re_scan_with_chg(ps, sstate.plan, estate, &chg)?;
    }

    if link == SubLinkType::ARRAY_SUBLINK {
        return scan_array_sub_plan(sstate, ps, estate);
    }
    let mut found = false;
    let mut result = NullableDatum {
        value: Datum::from_bool(link == SubLinkType::ALL_SUBLINK),
        isnull: false,
    };

    while let Some(slot_id) = exec_proc_node(ps, estate)? {
        match link {
            SubLinkType::EXISTS_SUBLINK => {
                found = true;
                result = NullableDatum { value: Datum::from_bool(true), isnull: false };
                break;
            }
            SubLinkType::EXPR_SUBLINK => {
                if found {
                    return Err(too_many_rows());
                }
                found = true;
                result = store_expr_result(sstate, estate, slot_id)?;
            }
            SubLinkType::MULTIEXPR_SUBLINK => {
                if found {
                    return Err(too_many_rows());
                }
                found = true;
                store_multiexpr_params(sstate, estate, slot_id)?;
            }
            SubLinkType::ROWCOMPARE_SUBLINK => {
                if found {
                    return Err(too_many_rows());
                }
                found = true;
                load_param_ids(sstate, estate, slot_id);
                // C reads the testexpr's pending initplan params only here,
                // per returned row (ExecEvalParamExec) — never on zero rows.
                {
                    let deps = sstate.testexpr.as_deref().unwrap().param_exec_deps();
                    if !deps.is_empty() {
                        ::executils::exec_eval_param_exec_params(estate, deps)?;
                    }
                }
                let testexpr = sstate
                    .testexpr
                    .as_deref_mut()
                    .expect("ROWCOMPARE SubPlan has a testexpr");
                result =
                    eval_expr_nested_subplans(testexpr, estate, ecxt, outer.as_deref_mut())?;
            }
            SubLinkType::ANY_SUBLINK | SubLinkType::ALL_SUBLINK => {
                found = true;
                load_param_ids(sstate, estate, slot_id);
                // As ROWCOMPARE: per-row lazy initplan reads (C
                // ExecEvalParamExec); idempotent after the first row.
                {
                    let deps = sstate.testexpr.as_deref().unwrap().param_exec_deps();
                    if !deps.is_empty() {
                        ::executils::exec_eval_param_exec_params(estate, deps)?;
                    }
                }
                let testexpr = sstate
                    .testexpr
                    .as_deref_mut()
                    .expect("ANY/ALL SubPlan has a testexpr");
                let row =
                    eval_expr_nested_subplans(testexpr, estate, ecxt, outer.as_deref_mut())?;
                if link == SubLinkType::ANY_SUBLINK {
                    if row.isnull {
                        result.isnull = true;
                    } else if row.value.as_bool() {
                        result = NullableDatum { value: Datum::from_bool(true), isnull: false };
                        break;
                    }
                } else if row.isnull {
                    result.isnull = true;
                } else if !row.value.as_bool() {
                    result = NullableDatum { value: Datum::from_bool(false), isnull: false };
                    break;
                }
            }
            other => unreachable!("{other:?} SubPlan is loud at init"),
        }
    }

    if !found
        && matches!(link, SubLinkType::EXPR_SUBLINK | SubLinkType::ROWCOMPARE_SUBLINK)
    {
        result = NullableDatum::null();
    }
    if !found && link == SubLinkType::MULTIEXPR_SUBLINK {
        // C: the dummy result doesn't matter, but the setParams become NULL.
        for &pid in sstate.set_param.iter() {
            let prm = &mut estate.es_param_exec_vals[pid as usize];
            debug_assert!(!prm.exec_plan);
            prm.value = Datum::null();
            prm.isnull = true;
        }
    }
    Ok(result)
}

// The MULTIEXPR leg of ExecScanSubPlan: push the single result row's columns
// out to the setParams. C copies the tuple (node->curTuple, freed on the next
// run) so pass-by-ref outputs stay valid until the next evaluation; cur_buf
// is the same boundary.
fn store_multiexpr_params<'mcx>(
    sstate: &mut SubPlanExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ::executils::ExecSlotId,
) -> PgResult<()> {
    let ncols = sstate.set_param.len();
    let mut total = 0usize;
    for col in 0..ncols {
        let slot = estate.slot_mut(slot_id);
        let (attlen, attbyval) = attr_len_byval(slot, col);
        let mut isnull = false;
        let v = exectuples::slot_getattr(slot, col as i32 + 1, &mut isnull);
        if !isnull && !attbyval {
            total = align8(total) + datum_image_size(v, attlen);
        }
    }
    sstate.cur_buf.clear();
    sstate
        .cur_buf
        .try_reserve(total)
        .map_err(|_| estate.es_query_cxt.oom(total))?;
    let mut off = 0usize;
    for col in 0..ncols {
        let (v, isnull, attlen, attbyval) = {
            let slot = estate.slot_mut(slot_id);
            let (attlen, attbyval) = attr_len_byval(slot, col);
            let mut isnull = false;
            let v = exectuples::slot_getattr(slot, col as i32 + 1, &mut isnull);
            (v, isnull, attlen, attbyval)
        };
        let value = if isnull || attbyval {
            v
        } else {
            let size = datum_image_size(v, attlen);
            off = align8(off);
            debug_assert!(off + size <= total);
            // SAFETY: capacity reserved above; source is a live by-ref datum
            // readable for `size` bytes (datum_image_size's arms).
            unsafe {
                core::ptr::copy_nonoverlapping(
                    v.as_usize() as *const u8,
                    sstate.cur_buf.as_mut_ptr().add(off),
                    size,
                );
            }
            let out = Datum::from_usize(sstate.cur_buf.as_ptr() as usize + off);
            off += size;
            out
        };
        let prm = &mut estate.es_param_exec_vals[sstate.set_param[col] as usize];
        debug_assert!(!prm.exec_plan);
        prm.value = value;
        prm.isnull = isnull;
    }
    // SAFETY: the first `off` bytes were written above (off == total unless
    // trailing columns were null/byval).
    unsafe { sstate.cur_buf.set_len(off) };
    Ok(())
}

fn attr_len_byval(slot: &SlotData<'_>, col: usize) -> (i16, bool) {
    let desc = slot
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .expect("subplan result slot has a descriptor");
    (desc.attrs[col].attlen, desc.attrs[col].attbyval)
}

fn align8(n: usize) -> usize {
    (n + 7) & !7
}

// datumGetSize (datum.c) for a non-null by-ref datum.
fn datum_image_size(value: Datum_crate::Datum, attlen: i16) -> usize {
    let p = value.as_usize() as *const u8;
    debug_assert!(!p.is_null());
    match attlen {
        -1 => {
            // SAFETY: non-null by-ref varlena datum, readable through its header.
            unsafe {
                assert!(
                    !::types_tuple::varatt::varatt_is_external_expanded(p),
                    "nodeSubplan.c: expanded varlena subplan result — \
                     expanded-object flatten arm has no producers"
                );
                ::types_tuple::varatt::varsize_any(p)
            }
        }
        -2 => {
            let mut n = 0usize;
            // SAFETY: non-null NUL-terminated cstring datum.
            while unsafe { *p.add(n) } != 0 {
                n += 1;
            }
            n + 1
        }
        l => {
            debug_assert!(l > 0);
            l as usize
        }
    }
}

// The ARRAY_SUBLINK leg of ExecScanSubPlan: accumulate first-column values in
// the reset-per-call scratch context, return the built array via cur_buf.
fn scan_array_sub_plan<'mcx>(
    sstate: &mut SubPlanExprState<'mcx>,
    ps: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<NullableDatum> {
    let first_col_type = sstate.first_col_type;
    sstate.array_ctx.as_mut().expect("ARRAY scratch context").reset();
    {
        let SubPlanExprState { array_ctx, cur_buf, .. } = &mut *sstate;
        let amcx = array_ctx.as_ref().expect("ARRAY scratch context").mcx();
        let mut astate = ::arrayfuncs::build::init_array_result_any(amcx, first_col_type, true)?;
        while let Some(slot_id) = exec_proc_node(ps, estate)? {
            let mut disnull = false;
            let dvalue = exectuples::slot_getattr(estate.slot_mut(slot_id), 1, &mut disnull);
            astate = ::arrayfuncs::build::accum_array_result_any(
                amcx,
                Some(astate),
                dvalue,
                disnull,
                first_col_type,
            )?;
        }
        let bytes = ::arrayfuncs::build::make_array_result_any(amcx, &astate)?;
        cur_buf.clear();
        cur_buf
            .try_reserve(bytes.len())
            .map_err(|_| estate.es_query_cxt.oom(bytes.len()))?;
        // SAFETY: reserved capacity written, then length set.
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), cur_buf.as_mut_ptr(), bytes.len());
            cur_buf.set_len(bytes.len());
        }
    }
    Ok(NullableDatum {
        value: Datum::from_usize(sstate.cur_buf.as_ptr() as usize),
        isnull: false,
    })
}

fn load_param_ids<'mcx>(
    sstate: &SubPlanExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ::executils::ExecSlotId,
) {
    for (col, &pid) in sstate.param_ids.iter().enumerate() {
        let mut isnull = false;
        let v = exectuples::slot_getattr(estate.slot_mut(slot_id), col as i32 + 1, &mut isnull);
        let prm = &mut estate.es_param_exec_vals[pid as usize];
        debug_assert!(!prm.exec_plan);
        prm.value = v;
        prm.isnull = isnull;
    }
}

fn store_expr_result<'mcx>(
    sstate: &mut SubPlanExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ::executils::ExecSlotId,
) -> PgResult<NullableDatum> {
    let slot = estate.slot_mut(slot_id);
    let (attlen, attbyval) = {
        let desc = slot
            .base()
            .tts_tupleDescriptor
            .as_ref()
            .expect("subplan result slot has a descriptor");
        (desc.attrs[0].attlen, desc.attrs[0].attbyval)
    };
    let mut isnull = false;
    let v = exectuples::slot_getattr(slot, 1, &mut isnull);
    if isnull || attbyval {
        return Ok(NullableDatum { value: v, isnull });
    }
    let p = v.as_usize() as *const u8;
    let size = match attlen {
        // SAFETY: non-null by-ref varlena datum; header readable.
        -1 => unsafe { ::types_tuple::varatt::varsize_any(p) },
        -2 => {
            let mut n = 0usize;
            // SAFETY: non-null NUL-terminated cstring datum.
            while unsafe { *p.add(n) } != 0 {
                n += 1;
            }
            n + 1
        }
        l => {
            debug_assert!(l > 0);
            l as usize
        }
    };
    sstate.cur_buf.clear();
    sstate
        .cur_buf
        .try_reserve(size)
        .map_err(|_| estate.es_query_cxt.oom(size))?;
    // SAFETY: `size` bytes readable per the arms above; reserved capacity
    // written then length set.
    unsafe {
        core::ptr::copy_nonoverlapping(p, sstate.cur_buf.as_mut_ptr(), size);
        sstate.cur_buf.set_len(size);
    }
    Ok(NullableDatum {
        value: Datum::from_usize(sstate.cur_buf.as_ptr() as usize),
        isnull: false,
    })
}

fn exec_hash_sub_plan<'mcx>(
    sstate: &mut SubPlanExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    outer: Option<&mut SlotData<'mcx>>,
) -> PgResult<NullableDatum> {
    debug_assert!(sstate.par_param.is_empty());
    if !sstate.hashed.as_ref().unwrap().built {
        build_sub_plan_hash(sstate, estate)?;
    }
    let h = sstate.hashed.as_mut().unwrap();
    if !h.havehashrows && !h.havenullrows {
        return Ok(NullableDatum { value: Datum::from_bool(false), isnull: false });
    }

    let lhs_slot = h.lhs_slot;
    {
        // C ExecProject(projLeft) reads pending initplan params here — after
        // the empty-tables early return above, never before it.
        let deps = h.proj_left.param_exec_deps();
        if !deps.is_empty() {
            ::executils::exec_eval_param_exec_params(estate, deps)?;
        }
        let h = sstate.hashed.as_mut().unwrap();
        let proj_left = &mut h.proj_left;
        project_lhs_nested_subplans(proj_left, estate, ecxt, lhs_slot, outer)?;
    }

    let result = hash_sub_plan_probe(sstate, estate, lhs_slot);
    // C ExecHashSubPlan: "Also must reset the hashtempcxt after each
    // hashtable lookup."
    sstate.hashed.as_mut().unwrap().hashtempcxt.reset();
    result
}

// ExecHashSubPlan's probe tail, split out so the hashtempcxt reset above
// covers every return path.
fn hash_sub_plan_probe<'mcx>(
    sstate: &mut SubPlanExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    lhs_slot: ExecSlotId,
) -> PgResult<NullableDatum> {
    let mcx = estate.es_query_cxt;
    let h = sstate.hashed.as_mut().unwrap();
    let ncols = h.key_col_idx.len();
    let (no_nulls, all_nulls) = {
        let base = estate.slot(lhs_slot).base();
        let nulls = &base.tts_isnull[..ncols];
        (
            nulls.iter().all(|n| !*n),
            nulls.iter().all(|n| *n),
        )
    };

    if no_nulls {
        if h.havehashrows {
            let found = if h.cross_type {
                let hash = hash_slot_lhs(h, estate, lhs_slot)?;
                find_exact_cross(h, estate, lhs_slot, hash)?
            } else {
                let ht = h.hashtable.as_mut().unwrap();
                // SAFETY: lhs_slot is estate-minted, distinct from the hash
                // table's internals.
                let slot = unsafe { &mut *(estate.slot_mut(lhs_slot) as *mut SlotData<'mcx>) };
                let hash = ht.hash_slot(slot)?;
                ht.lookup(slot, hash, None, mcx)?.0.is_some()
            };
            if found {
                return Ok(NullableDatum { value: Datum::from_bool(true), isnull: false });
            }
        }
        if h.havenullrows && find_partial_match(h, estate, lhs_slot, false)? {
            return Ok(NullableDatum { value: Datum::null(), isnull: true });
        }
        return Ok(NullableDatum { value: Datum::from_bool(false), isnull: false });
    }
    if h.hashnulls.is_none() {
        return Ok(NullableDatum { value: Datum::from_bool(false), isnull: false });
    }
    if all_nulls
        || (h.havenullrows && find_partial_match(h, estate, lhs_slot, false)?)
        || (h.havehashrows && find_partial_match(h, estate, lhs_slot, true)?)
    {
        return Ok(NullableDatum { value: Datum::null(), isnull: true });
    }
    Ok(NullableDatum { value: Datum::from_bool(false), isnull: false })
}

fn build_sub_plan_hash<'mcx>(
    sstate: &mut SubPlanExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    debug_assert!(sstate.sub_link_type == SubLinkType::ANY_SUBLINK);
    {
        let plan_rows = sstate.plan.as_plan().expect("plan node").plan_rows;
        let h = sstate.hashed.as_mut().unwrap();
        let mut nbuckets = clamp_cardinality(plan_rows);
        h.table_ctx.reset();
        h.havehashrows = false;
        h.havenullrows = false;
        if let Some(ht) = h.hashtable.as_mut() {
            ht.reset();
        } else {
            let mut ht = ::execgrouping::build_tuple_hash_table(
                mcx,
                &h.desc_right,
                &h.key_col_idx,
                &h.tab_eq_funcoids,
                &h.tab_hash_funcs,
                &h.tab_collations,
                nbuckets,
                0,
                false,
            )?;
            // C BuildTupleHashTable's tempcxt = sstate->hashtempcxt, reset
            // after each lookup: probe-time detoasts of compressed by-ref
            // keys must not accumulate for the query's lifetime.
            // SAFETY: hashtempcxt lives in the leaked SubPlanExprState —
            // address-stable at build time (exec), dropped with the table
            // at executor end.
            unsafe { ht.set_temp_ctx_raw(h.hashtempcxt.mcx()) };
            h.hashtable = Some(ht);
        }
        if !h.unknown_eq_false {
            if h.key_col_idx.len() == 1 {
                nbuckets = 1;
            } else {
                nbuckets = (nbuckets / 16).max(1);
            }
            if let Some(ht) = h.hashnulls.as_mut() {
                ht.reset();
            } else {
                let mut ht = ::execgrouping::build_tuple_hash_table(
                    mcx,
                    &h.desc_right,
                    &h.key_col_idx,
                    &h.tab_eq_funcoids,
                    &h.tab_hash_funcs,
                    &h.tab_collations,
                    nbuckets,
                    0,
                    false,
                )?;
                // Same tempcxt install as the main table above.
                // SAFETY: same outlives/address-stability argument.
                unsafe { ht.set_temp_ctx_raw(h.hashtempcxt.mcx()) };
                h.hashnulls = Some(ht);
            }
        } else {
            h.hashnulls = None;
        }
        h.built = true;
    }

    // SAFETY: es_query_cxt-lifetime cell; exclusive by the take-out protocol.
    let cell = unsafe { &mut *sstate.ps_cell.as_ptr() };
    let mut ps = cell
        .take()
        .unwrap_or_else(|| panic!("recursive subplan execution (nodeSubplan.c)"));
    let result = build_sub_plan_hash_scan(sstate, &mut ps, estate);
    *cell = Some(ps);
    result
}

fn build_sub_plan_hash_scan<'mcx>(
    sstate: &mut SubPlanExprState<'mcx>,
    ps: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // buildSubPlanHash rescans with the accumulated chgParam so param-bearing
    // nodes (including initplans) inside the subplan reset before the refill.
    let chg = core::mem::replace(&mut sstate.chg_param, Bitmapset::empty());
    if chg.is_empty() {
        crate::execami::exec_re_scan(ps, estate)?;
    } else {
        crate::execami::exec_re_scan_with_chg(ps, sstate.plan, estate, &chg)?;
    }
    while let Some(slot_id) = exec_proc_node(ps, estate)? {
        load_param_ids(sstate, estate, slot_id);
        let h = sstate.hashed.as_mut().unwrap();
        let rhs_slot = h.rhs_slot;
        {
            let proj_right = &mut h.proj_right;
            let mut slots = ::execexpr::EvalSlots { scan: None, inner: None, outer: None };
            let table = &mut estate.es_tupleTable;
            let rslot = &mut table[rhs_slot.0 as usize];
            exec_project(proj_right, &mut slots, rslot, mcx)?;
        }
        let ncols = h.key_col_idx.len();
        let no_nulls = {
            let base = estate.slot(rhs_slot).base();
            base.tts_isnull[..ncols].iter().all(|n| !*n)
        };
        let table_mcx = h.table_ctx.mcx();
        // SAFETY: rhs_slot is estate-minted and distinct from the hash
        // tables' internals; the derived &mut aliases nothing live.
        let slot = unsafe { &mut *(&mut estate.es_tupleTable[rhs_slot.0 as usize] as *mut SlotData<'mcx>) };
        if no_nulls {
            let ht = h.hashtable.as_mut().unwrap();
            let hash = ht.hash_slot(slot)?;
            ht.lookup(slot, hash, Some(table_mcx), mcx)?;
            h.havehashrows = true;
        } else if let Some(ht) = h.hashnulls.as_mut() {
            let hash = ht.hash_slot(slot)?;
            ht.lookup(slot, hash, Some(table_mcx), mcx)?;
            h.havenullrows = true;
        }
        // C buildSubPlanHash: "Also must reset the hashtempcxt after each
        // hashtable lookup."
        h.hashtempcxt.reset();
    }
    Ok(())
}

fn find_partial_match<'mcx>(
    h: &mut HashedSubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    lhs_slot: ExecSlotId,
    main_table: bool,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let ncols = h.key_col_idx.len();
    let ht = if main_table { h.hashtable.as_ref() } else { h.hashnulls.as_ref() }
        .expect("partial-match table exists");
    for ix in 0..ht.num_entries() as u32 {
        let tup = ht.entry_tuple(ix);
        // SAFETY: entry images live in table_ctx until the next rebuild.
        unsafe { exectuples::exec_store_minimal_tuple_ptr(&mut h.probe_slot, mcx, tup) };
        // SAFETY: lhs_slot is estate-minted, distinct from probe_slot (owned
        // by the SubPlanExprState, not in es_tupleTable).
        let lhs = unsafe { &mut *(&mut estate.es_tupleTable[lhs_slot.0 as usize] as *mut SlotData<'mcx>) };
        // Eq-proc detoasts ride hashtempcxt (reset per probe by the caller),
        // C's short-lived-context discipline — never query-lifetime memory.
        if !exec_tuples_unequal(
            h.hashtempcxt.mcx(),
            lhs,
            &mut h.probe_slot,
            ncols,
            &mut h.cur_eq_funcs,
            &h.tab_collations,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

// execTuplesUnequal (nodeSubplan.c): true only if some non-null pair
// compares not-equal; last column first. `temp_mcx`: the short-lived
// context by-ref eq results (detoast copies) land in — the caller resets it
// per probe.
fn exec_tuples_unequal<'mcx>(
    temp_mcx: Mcx<'_>,
    slot1: &mut SlotData<'mcx>,
    slot2: &mut SlotData<'mcx>,
    ncols: usize,
    eqfuncs: &mut [FmgrInfo],
    collations: &[::types_core::Oid],
) -> PgResult<bool> {
    for i in (0..ncols).rev() {
        let att = i as i32 + 1;
        let mut null1 = false;
        let a1 = exectuples::slot_getattr(slot1, att, &mut null1);
        if null1 {
            continue;
        }
        let mut null2 = false;
        let a2 = exectuples::slot_getattr(slot2, att, &mut null2);
        if null2 {
            continue;
        }
        let flinfo = &mut eqfuncs[i];
        let mut fcinfo = LocalFcinfo::<2>::fresh(collations[i]);
        // C execTuplesUnequal: the eq proc detoasts by-ref args via
        // DirectFunctionCall, pallocing in the caller's (short-lived) context.
        unsafe { fcinfo.set_result_mcx(temp_mcx) };
        fcinfo.args[0] = NullableDatum { value: a1, isnull: false };
        fcinfo.args[1] = NullableDatum { value: a2, isnull: false };
        let fn_addr = flinfo.fn_addr;
        let d = fn_addr(Some(flinfo), &mut fcinfo)?;
        debug_assert!(!fcinfo.isnull);
        if !d.as_bool() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn clamp_cardinality(rows: f64) -> usize {
    if rows.is_nan() || rows < 1.0 {
        1
    } else if rows >= usize::MAX as f64 {
        usize::MAX
    } else {
        rows as usize
    }
}


// TupleHashTableHash over the LHS slot with C's lhs_hash_funcs (the probe
// side of FindTupleHashEntry); combine/rotate/murmur mirror
// ExecBuildHash32FromAttrs.
fn hash_slot_lhs<'mcx>(
    h: &mut HashedSubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    lhs_slot: ExecSlotId,
) -> PgResult<u32> {
    let mcx = estate.es_query_cxt;
    let ncols = h.key_col_idx.len();
    let slot = estate.slot_mut(lhs_slot);
    exectuples::slot_getsomeattrs(slot, ncols as i32);
    let mut hash: u32 = 0;
    for i in 0..ncols {
        let (v, isnull) = {
            let base = slot.base();
            (base.tts_values[i], base.tts_isnull[i])
        };
        if i > 0 {
            hash = hash.rotate_left(1);
        }
        if !isnull {
            let flinfo = &mut h.lhs_hash_funcs[i];
            let mut fcinfo = LocalFcinfo::<1>::fresh(h.tab_collations[i]);
            // C ExecBuildHash32FromAttrs: the hash proc detoasts its by-ref
            // arg via DirectFunctionCall — into hashtempcxt (reset per
            // probe), never query-lifetime memory.
            unsafe { fcinfo.set_result_mcx(h.hashtempcxt.mcx()) };
            fcinfo.args[0] = NullableDatum { value: v, isnull: false };
            let fn_addr = flinfo.fn_addr;
            let d = fn_addr(Some(flinfo), &mut fcinfo)?;
            if i == 0 {
                hash = d.as_u32();
            } else {
                hash ^= d.as_u32();
            }
        } else if i == 0 {
            hash = 0;
        }
    }
    Ok(::hashfn::murmurhash32(hash))
}

// FindTupleHashEntry's cross-type exact probe: cur_eq_funcs (the combining
// operators' own functions) compare LHS values against stored RHS tuples.
fn find_exact_cross<'mcx>(
    h: &mut HashedSubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    lhs_slot: ExecSlotId,
    hash: u32,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let ncols = h.key_col_idx.len();
    let HashedSubPlanState {
        hashtable, probe_slot, cur_eq_funcs, tab_collations, hashtempcxt, ..
    } = h;
    let temp_mcx = hashtempcxt.mcx();
    let ht = hashtable.as_ref().expect("hashtable built");
    // SAFETY: lhs_slot is estate-minted, distinct from probe_slot and the
    // hash table's internals.
    let lhs = unsafe { &mut *(&mut estate.es_tupleTable[lhs_slot.0 as usize] as *mut SlotData<'mcx>) };
    let found = ht.find_entry_with(hash, |ix| {
        let tup = ht.entry_tuple(ix);
        // SAFETY: entry images live in table_ctx until the next rebuild.
        unsafe { exectuples::exec_store_minimal_tuple_ptr(probe_slot, mcx, tup) };
        for i in (0..ncols).rev() {
            let att = i as i32 + 1;
            let mut n1 = false;
            let a1 = exectuples::slot_getattr(lhs, att, &mut n1);
            let mut n2 = false;
            let a2 = exectuples::slot_getattr(probe_slot, att, &mut n2);
            // Main-table entries and the probed LHS are all non-null here.
            debug_assert!(!n1 && !n2);
            let flinfo = &mut cur_eq_funcs[i];
            let mut fcinfo = LocalFcinfo::<2>::fresh(tab_collations[i]);
            // C FindTupleHashEntry runs the cross-type eq inside tempcxt:
            // by-ref detoasts land in hashtempcxt (reset per probe).
            unsafe { fcinfo.set_result_mcx(temp_mcx) };
            fcinfo.args[0] = NullableDatum { value: a1, isnull: false };
            fcinfo.args[1] = NullableDatum { value: a2, isnull: false };
            let fn_addr = flinfo.fn_addr;
            let d = fn_addr(Some(flinfo), &mut fcinfo)?;
            if fcinfo.isnull || !d.as_bool() {
                return Ok(false);
            }
        }
        Ok(true)
    })?;
    Ok(found.is_some())
}
