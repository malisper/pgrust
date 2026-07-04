// nodeWindowAgg.c: default-frame fast lane (compiled evaltrans, frame head
// pinned) + C-exact framed lane (ROWS/RANGE/GROUPS offsets, EXCLUDE seeks,
// per-agg carriers, inverse transitions, runCondition pass-through modes).
// Window functions are enum-dispatched (C: fmgr + WindowObject; the set is
// closed here). FILTER and by-ref plain transtypes are loud panics at init.
#![allow(non_snake_case)]

use std::ptr::NonNull;
use std::rc::Rc;

use ::datum::{Datum, NullableDatum};
use ::execexpr::{
    exec_build_agg_trans, exec_build_grouping_equal, exec_build_window_projection_info,
    exec_eval_expr, exec_init_expr, exec_init_qual, exec_project, exec_qual, expr_type, AggBind,
    AggPerGroup, AggTransSpec, EvalSlots, ExprState, WinBind,
};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{vec_with_capacity_in, PgBox, PgVec};
use ::tuplestore::Tuplestore;
use ::types_core::catalog::PROCEDURE_RELATION_ID;
use ::types_core::{Oid, INT8OID};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::{FmgrInfo, LocalFcinfo};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::WindowAgg;
use ::types_nodes::primnodes::WindowFunc;
use ::types_nodes::rawnodes::{
    FRAMEOPTION_DEFAULTS, FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_END_OFFSET,
    FRAMEOPTION_END_OFFSET_PRECEDING, FRAMEOPTION_END_UNBOUNDED_FOLLOWING,
    FRAMEOPTION_EXCLUDE_CURRENT_ROW, FRAMEOPTION_EXCLUDE_GROUP, FRAMEOPTION_EXCLUDE_TIES,
    FRAMEOPTION_EXCLUSION, FRAMEOPTION_GROUPS, FRAMEOPTION_RANGE, FRAMEOPTION_ROWS,
    FRAMEOPTION_START_CURRENT_ROW, FRAMEOPTION_START_OFFSET, FRAMEOPTION_START_OFFSET_PRECEDING,
    FRAMEOPTION_START_UNBOUNDED_PRECEDING,
};
use ::types_nodes::NodeTag;
use ::types_slot::{SlotData, TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

const ACL_EXECUTE: u64 = 1 << 7;
const ACLCHECK_OK: i32 = 0;
const AGGKIND_NORMAL: i8 = b'n' as i8;
const AGGMODIFY_READ_ONLY: i8 = b'r' as i8;

const F_WINDOW_ROW_NUMBER: Oid = 3100;
const F_WINDOW_RANK: Oid = 3101;
const F_WINDOW_DENSE_RANK: Oid = 3102;
const F_WINDOW_PERCENT_RANK: Oid = 3103;
const F_WINDOW_CUME_DIST: Oid = 3104;
const F_WINDOW_NTILE: Oid = 3105;
const F_WINDOW_LAG: Oid = 3106;
const F_WINDOW_LAG_WITH_OFFSET: Oid = 3107;
const F_WINDOW_LAG_WITH_OFFSET_AND_DEFAULT: Oid = 3108;
const F_WINDOW_LEAD: Oid = 3109;
const F_WINDOW_LEAD_WITH_OFFSET: Oid = 3110;
const F_WINDOW_LEAD_WITH_OFFSET_AND_DEFAULT: Oid = 3111;
const F_WINDOW_FIRST_VALUE: Oid = 3112;
const F_WINDOW_LAST_VALUE: Oid = 3113;
const F_WINDOW_NTH_VALUE: Oid = 3114;

const F_INT2_AVG_ACCUM: Oid = 1962;
const F_INT4_AVG_ACCUM: Oid = 1963;
const F_INT2_AVG_ACCUM_INV: Oid = 3570;
const F_INT4_AVG_ACCUM_INV: Oid = 3571;
const F_INT2INT4_SUM: Oid = 3572;

#[derive(Clone, Copy, PartialEq)]
enum WfKind {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    LeadLag { forward: bool, withoffset: bool, withdefault: bool },
    FirstValue,
    LastValue,
    NthValue,
    PlainAgg { aggno: u16 },
}

// C WindowStatePerFuncData + the WindowObject position fields (markptr is
// bookkeeping only: tuplestore_trim is unported, so no mark read pointer).
// Rank/ntile state is C's WinGetPartitionLocalMemory chunk, inline.
struct PerFuncData<'mcx> {
    kind: WfKind,
    wfuncno: u16,
    readptr: i32,
    seekpos: i64,
    markpos: i64,
    rank: i64,
    ntile: i32,
    rows_per_bucket: i64,
    boundary: i64,
    remainder: i64,
    arg1_stable: bool,
    argstates: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
}

// Int8TransTypeData (numeric.c): the {count,sum} pair C wraps in an int8[2]
// ArrayType; inline here (state never leaves the node — DIVERGENCE, rule 4).
#[derive(Clone, Copy, Default)]
struct Int8TransState {
    count: i64,
    sum: i64,
}

enum AggKernel {
    Generic { transfn: FmgrInfo },
    MovingByVal { transfn: FmgrInfo, invtransfn: FmgrInfo },
    MovingIntSum { int2: bool },
}

// C WindowStatePerAggData, byval-transtype closed set (framed lane only; the
// default frame rides the compiled evaltrans program).
struct PerAggData<'mcx> {
    wfuncno: u16,
    num_arguments: i16,
    win_collation: Oid,
    kernel: AggKernel,
    fn_strict: bool,
    has_inverse: bool,
    argstates: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    init_value: NullableDatum,
    trans_value: NullableDatum,
    trans_count: i64,
    int_sum: Int8TransState,
    result_value: NullableDatum,
    restart: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum WaStatus {
    Run,
    PassThrough,
    PassThroughStrict,
    Done,
}

pub struct WindowAggStateData<'mcx> {
    plan: &'mcx WindowAgg<'mcx>,
    frameOptions: i32,
    pub ps_ExprContext: EcxtId,
    tmpcontext: EcxtId,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    part_eq: Option<PgBox<'mcx, ExprState<'mcx>>>,
    ord_eq: Option<PgBox<'mcx, ExprState<'mcx>>>,
    buffer: Option<Tuplestore>,
    scan_slot: SlotData<'mcx>,
    first_part_slot: SlotData<'mcx>,
    first_part_valid: bool,
    agg_row_slot: SlotData<'mcx>,
    agg_row_valid: bool,
    temp_slot_1: SlotData<'mcx>,
    temp_slot_2: SlotData<'mcx>,
    framehead_slot: SlotData<'mcx>,
    frametail_slot: SlotData<'mcx>,
    perfunc: PgVec<'mcx, PerFuncData<'mcx>>,
    evaltrans: Option<PgBox<'mcx, ExprState<'mcx>>>,
    trans_init: PgVec<'mcx, NullableDatum>,
    _pergroup: PgVec<'mcx, AggPerGroup>,
    pergroup_base: NonNull<AggPerGroup>,
    peragg_wfuncno: PgVec<'mcx, u16>,
    peragg: PgVec<'mcx, PerAggData<'mcx>>,
    agg_saved: PgVec<'mcx, NullableDatum>,
    agg_readptr: i32,
    agg_seekpos: i64,
    agg_markpos: i64,
    agg_mark_active: bool,
    agg_values_base: NonNull<Datum>,
    agg_nulls_base: NonNull<bool>,
    numaggs: usize,
    currentpos: i64,
    frameheadpos: i64,
    frametailpos: i64,
    framehead_valid: bool,
    frametail_valid: bool,
    framehead_ptr: i32,
    frametail_ptr: i32,
    currentgroup: i64,
    frameheadgroup: i64,
    frametailgroup: i64,
    groupheadpos: i64,
    grouptailpos: i64,
    grouptail_valid: bool,
    grouptail_ptr: i32,
    aggregatedbase: i64,
    aggregatedupto: i64,
    spooled_rows: i64,
    start_offset_state: Option<PgBox<'mcx, ExprState<'mcx>>>,
    end_offset_state: Option<PgBox<'mcx, ExprState<'mcx>>>,
    start_offset_value: Datum,
    end_offset_value: Datum,
    start_offset_typlen: i16,
    start_offset_byval: bool,
    end_offset_typlen: i16,
    end_offset_byval: bool,
    start_in_range: Option<FmgrInfo>,
    end_in_range: Option<FmgrInfo>,
    runcondition: Option<PgBox<'mcx, ExprState<'mcx>>>,
    qual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    use_pass_through: bool,
    top_window: bool,
    all_first: bool,
    partition_spooled: bool,
    more_partitions: bool,
    next_partition: bool,
    status: WaStatus,
}

#[cold]
#[inline(never)]
fn wfunc_lookup_failed(fnoid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for aggregate {fnoid}")))
}

#[cold]
#[inline(never)]
fn wfunc_permission_denied(fnoid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("permission denied for function {fnoid}"))
            .with_sqlstate(::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
    )
}

#[cold]
#[inline(never)]
fn ntile_arg_error() -> Box<PgError> {
    Box::new(
        PgError::error("argument of ntile must be greater than zero".to_string())
            .with_sqlstate(::types_error::ERRCODE_INVALID_ARGUMENT_FOR_NTILE),
    )
}

#[cold]
#[inline(never)]
fn nth_value_arg_error() -> Box<PgError> {
    Box::new(
        PgError::error("argument of nth_value must be greater than zero".to_string())
            .with_sqlstate(::types_error::ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE),
    )
}

#[cold]
#[inline(never)]
fn frame_offset_null(starting: bool) -> Box<PgError> {
    let which = if starting { "starting" } else { "ending" };
    Box::new(
        PgError::error(format!("frame {which} offset must not be null"))
            .with_sqlstate(::types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

#[cold]
#[inline(never)]
fn frame_offset_negative(starting: bool) -> Box<PgError> {
    let which = if starting { "starting" } else { "ending" };
    Box::new(
        PgError::error(format!("frame {which} offset must not be negative"))
            .with_sqlstate(::types_error::ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
    )
}

#[cold]
#[inline(never)]
fn moving_transfn_returned_null() -> Box<PgError> {
    Box::new(
        PgError::error("moving-aggregate transition function must not return null".to_string())
            .with_sqlstate(::types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

// GetAggInitVal (nodeWindowAgg.c keeps its own copy, as nodeAgg.c does);
// only the int8 arm is live (count/sum transtypes).
fn get_agg_init_val(text: &str, transtype: Oid) -> PgResult<Datum> {
    if transtype != INT8OID {
        panic!(
            "GetAggInitVal (nodeWindowAgg.c): typinput dispatch for transtype {transtype} \
             not ported"
        );
    }
    Ok(Datum::from_i64(::adt_int8::int8in(text, None)?))
}

// datumCopy (datum.c) for by-ref frame offsets; -1 is VARSIZE_ANY (short
// headers included), expanded/toast-pointer sources are loud.
fn datum_copy<'mcx>(mcx: ::mcx::Mcx<'mcx>, value: Datum, typlen: i16) -> PgResult<Datum> {
    let p = value.as_usize() as *const u8;
    if p.is_null() {
        return Ok(Datum::null());
    }
    let size = match typlen {
        -1 => {
            // SAFETY: non-null by-ref varlena datum, readable for its
            // header-declared size.
            unsafe {
                let b0 = *p;
                if b0 == 0x01 {
                    panic!("datum_copy (nodeWindowAgg.c): toast-pointer frame offset unported");
                } else if b0 & 0x01 != 0 {
                    (b0 as usize >> 1) & 0x7F
                } else {
                    datum::VarlenaRef::from_ptr(p).varsize()
                }
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
    };
    // SAFETY: `size` bytes readable per the arms above.
    let src = unsafe { core::slice::from_raw_parts(p, size) };
    let out = ::mcx::slice_in(mcx, src)?;
    Ok(Datum::from_usize(out.leak().as_ptr() as usize))
}

fn collect_window_funcs<'mcx>(
    node: Node<'mcx>,
    out: &mut PgVec<'mcx, (Node<'mcx>, &'mcx WindowFunc<'mcx>)>,
) {
    match node.node_tag() {
        NodeTag::T_WindowFunc => out.push((node, node.as_window_func().unwrap())),
        NodeTag::T_TargetEntry => {
            collect_window_funcs(node.as_target_entry().unwrap().expr, out)
        }
        NodeTag::T_Var | NodeTag::T_Const => {}
        NodeTag::T_FuncExpr => {
            for a in node.as_func_expr().unwrap().args.iter() {
                collect_window_funcs(a, out);
            }
        }
        NodeTag::T_OpExpr => {
            for a in node.as_op_expr().unwrap().args.iter() {
                collect_window_funcs(a, out);
            }
        }
        tag => panic!(
            "ExecInitWindowAgg (nodeWindowAgg.c): WindowAgg tlist node family {tag:?} \
             not ported"
        ),
    }
}

// contain_volatile_functions (clauses.c) over the arg shapes this lane
// admits; unknown tags are loud rather than assumed stable.
fn contain_volatile_functions(node: Node<'_>) -> PgResult<bool> {
    match node.node_tag() {
        NodeTag::T_Var | NodeTag::T_Const | NodeTag::T_Param => Ok(false),
        NodeTag::T_RelabelType => {
            contain_volatile_functions(node.as_relabel_type().unwrap().arg)
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            if lsyscache::func_volatile(f.funcid)? == b'v' as i8 {
                return Ok(true);
            }
            for a in f.args.iter() {
                if contain_volatile_functions(a)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            if lsyscache::func_volatile(o.opfuncid)? == b'v' as i8 {
                return Ok(true);
            }
            for a in o.args.iter() {
                if contain_volatile_functions(a)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        tag => panic!(
            "contain_volatile_functions (clauses.c): node family {tag:?} not ported \
             (window-agg lane)"
        ),
    }
}

// get_fn_expr_arg_stable (fmgr.c): Const or extern Param.
fn arg_is_stable(node: Node<'_>) -> bool {
    match node.node_tag() {
        NodeTag::T_Const => true,
        NodeTag::T_Param => node
            .as_param()
            .map(|p| p.paramkind == ::types_nodes::primnodes::ParamKind::PARAM_EXTERN)
            .unwrap_or(false),
        _ => false,
    }
}

fn build_argstates<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    args: &NodeList<'mcx>,
    params: ::execexpr::ParamBind<'mcx>,
) -> PgResult<PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>> {
    let mut out = PgVec::new_in(mcx);
    for a in args.iter() {
        out.push(exec_init_expr(mcx, Some(a), params)?.expect("window arg ExprState"));
    }
    Ok(out)
}

/// `ExecInitWindowAgg` minus child linkage: the caller (execProcnode's
/// T_WindowAgg arm) inits the outer child and passes its result type.
pub fn exec_init_window_agg<'mcx>(
    node: &'mcx WindowAgg<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_desc: &Rc<TupleDescData<'static>>,
    result_desc: Rc<TupleDescData<'static>>,
) -> PgResult<WindowAggStateData<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    let mcx = estate.es_query_cxt;
    let frameOptions = node.frameOptions;

    debug_assert!(node.plan.qual.is_nil() || node.topWindow);
    let default_frame = frameOptions == FRAMEOPTION_DEFAULTS;

    let tmpcontext = estate.create_expr_context();
    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::Virtual);

    let mut wfuncs: PgVec<'mcx, (Node<'mcx>, &'mcx WindowFunc<'mcx>)> = PgVec::new_in(mcx);
    for tle in node.plan.targetlist.iter() {
        collect_window_funcs(tle, &mut wfuncs);
    }
    // C dedups equal() non-volatile wfuncs onto one wfuncno; equal() has no
    // WindowFunc arm yet, so duplicates each get their own slot (results
    // identical, duplicated evaluation).
    let numfuncs = wfuncs.len();
    let userid = miscinit_seams::get_user_id::call();
    let params = estate.param_bind();

    let mut perfunc: PgVec<'mcx, PerFuncData<'mcx>> = PgVec::new_in(mcx);
    let mut wfuncnos: PgVec<'mcx, (Node<'mcx>, u16)> = vec_with_capacity_in(mcx, numfuncs)?;
    let mut agg_specs_args: PgVec<'mcx, NodeList<'mcx>> = PgVec::new_in(mcx);
    let mut trans_init: PgVec<'mcx, NullableDatum> = PgVec::new_in(mcx);
    let mut trans_fnoid: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut trans_collid: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut trans_typlen: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    let mut peragg_wfuncno: PgVec<'mcx, u16> = PgVec::new_in(mcx);
    let mut peragg: PgVec<'mcx, PerAggData<'mcx>> = PgVec::new_in(mcx);

    for (wfuncno, &(wnode, wfunc)) in wfuncs.iter().enumerate() {
        if wfunc.winref != node.winref {
            panic!(
                "WindowFunc with winref {} assigned to WindowAgg with winref {}",
                wfunc.winref, node.winref
            );
        }
        if wfunc.aggfilter.is_some() {
            panic!("ExecInitWindowAgg (nodeWindowAgg.c): FILTER not ported");
        }
        let aclresult = aclchk_seams::object_aclcheck::call(
            PROCEDURE_RELATION_ID,
            wfunc.winfnoid,
            userid,
            ACL_EXECUTE,
        )?;
        if aclresult != ACLCHECK_OK {
            return Err(wfunc_permission_denied(wfunc.winfnoid));
        }
        wfuncnos.push((wnode, wfuncno as u16));

        let mut argstates: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> = PgVec::new_in(mcx);
        let mut arg1_stable = false;
        let kind = if wfunc.winagg {
            let aggno = agg_specs_args.len() as u16;
            if default_frame {
                initialize_peragg_default(
                    mcx,
                    wfunc,
                    &mut agg_specs_args,
                    &mut trans_init,
                    &mut trans_fnoid,
                    &mut trans_collid,
                    &mut trans_typlen,
                )?;
            } else {
                peragg.push(initialize_peragg_framed(
                    mcx,
                    wfunc,
                    frameOptions,
                    wfuncno as u16,
                    params,
                )?);
                agg_specs_args.push(NodeList::nil());
            }
            peragg_wfuncno.push(wfuncno as u16);
            WfKind::PlainAgg { aggno }
        } else {
            let kind = match wfunc.winfnoid {
                F_WINDOW_ROW_NUMBER => WfKind::RowNumber,
                F_WINDOW_RANK => WfKind::Rank,
                F_WINDOW_DENSE_RANK => WfKind::DenseRank,
                F_WINDOW_PERCENT_RANK => WfKind::PercentRank,
                F_WINDOW_CUME_DIST => WfKind::CumeDist,
                F_WINDOW_NTILE => WfKind::Ntile,
                F_WINDOW_LAG => {
                    WfKind::LeadLag { forward: false, withoffset: false, withdefault: false }
                }
                F_WINDOW_LAG_WITH_OFFSET => {
                    WfKind::LeadLag { forward: false, withoffset: true, withdefault: false }
                }
                F_WINDOW_LAG_WITH_OFFSET_AND_DEFAULT => {
                    WfKind::LeadLag { forward: false, withoffset: true, withdefault: true }
                }
                F_WINDOW_LEAD => {
                    WfKind::LeadLag { forward: true, withoffset: false, withdefault: false }
                }
                F_WINDOW_LEAD_WITH_OFFSET => {
                    WfKind::LeadLag { forward: true, withoffset: true, withdefault: false }
                }
                F_WINDOW_LEAD_WITH_OFFSET_AND_DEFAULT => {
                    WfKind::LeadLag { forward: true, withoffset: true, withdefault: true }
                }
                F_WINDOW_FIRST_VALUE => WfKind::FirstValue,
                F_WINDOW_LAST_VALUE => WfKind::LastValue,
                F_WINDOW_NTH_VALUE => WfKind::NthValue,
                other => panic!(
                    "eval_windowfunction (nodeWindowAgg.c): window function oid {other} \
                     not ported"
                ),
            };
            argstates = build_argstates(mcx, &wfunc.args, params)?;
            if wfunc.args.len() >= 2 {
                arg1_stable = arg_is_stable(wfunc.args.nth(1));
            }
            kind
        };
        perfunc.push(PerFuncData {
            kind,
            wfuncno: wfuncno as u16,
            readptr: -1,
            seekpos: -1,
            markpos: -1,
            rank: 0,
            ntile: 0,
            rows_per_bucket: 0,
            boundary: 0,
            remainder: 0,
            arg1_stable,
            argstates,
        });
    }
    let numaggs = agg_specs_args.len();

    let mut pergroup: PgVec<'mcx, AggPerGroup> = vec_with_capacity_in(mcx, numaggs)?;
    pergroup.resize(
        numaggs,
        AggPerGroup { trans_value: Datum::null(), trans_value_is_null: true, no_trans_value: true },
    );
    let pergroup_base = NonNull::new(pergroup.as_mut_ptr()).unwrap();

    let (agg_values_base, agg_nulls_base) = {
        let ecxt = estate.ecxt_mut(ps_ExprContext);
        ecxt.ecxt_aggvalues.resize(numfuncs, Datum::null());
        ecxt.ecxt_aggnulls.resize(numfuncs, true);
        (
            NonNull::new(ecxt.ecxt_aggvalues.as_mut_ptr()).unwrap(),
            NonNull::new(ecxt.ecxt_aggnulls.as_mut_ptr()).unwrap(),
        )
    };

    let evaltrans = if default_frame && numaggs > 0 {
        let mut specs: PgVec<'mcx, AggTransSpec<'_, 'mcx>> = vec_with_capacity_in(mcx, numaggs)?;
        for aggno in 0..numaggs {
            // SAFETY: aggno < numaggs elements of the once-allocated pergroup.
            let pg = unsafe { NonNull::new_unchecked(pergroup_base.as_ptr().add(aggno)) };
            specs.push(AggTransSpec {
                transfn_oid: trans_fnoid[aggno],
                inputcollid: trans_collid[aggno],
                init_value_is_null: trans_init[aggno].isnull,
                // Empty = get_fn_expr_argtype yields InvalidOid; transfns that
                // need argtypes error loud rather than silently diverge.
                arg_types: &[],
                args: &agg_specs_args[aggno],
                aggfilter: None,
                pergroup: pg,
                ordered: None,
                cur_agg: None,
                transtype_byval: true,
                transtype_len: trans_typlen[aggno],
            });
        }
        // C arms fcinfo->context with the WindowAggState; None makes a
        // context-reading transfn fail loud (non-aggregate-context error).
        Some(exec_build_agg_trans(mcx, &specs, None, params)?)
    } else {
        None
    };

    let bind = AggBind { values: agg_values_base, nulls: agg_nulls_base, naggs: numfuncs as u16, grouping: None };
    let proj = exec_build_window_projection_info(
        mcx,
        &node.plan.targetlist,
        None,
        WinBind { agg: bind, wfuncnos: &wfuncnos },
        params,
    )?;

    let part_eq = build_eq(
        mcx,
        outer_desc,
        node.partNumCols,
        node.partColIdx,
        node.partOperators,
        node.partCollations,
    )?;
    let ord_eq = build_eq(
        mcx,
        outer_desc,
        node.ordNumCols,
        node.ordColIdx,
        node.ordOperators,
        node.ordCollations,
    )?;

    let mk_slot = || {
        exectuples::make_tuple_table_slot(
            mcx,
            TupleSlotKind::MinimalTuple,
            Some(outer_desc.clone()),
        )
    };
    let scan_slot = mk_slot();
    let first_part_slot = mk_slot();
    let agg_row_slot = mk_slot();
    let temp_slot_1 = mk_slot();
    let temp_slot_2 = mk_slot();
    let framehead_slot = mk_slot();
    let frametail_slot = mk_slot();
    let mut agg_saved: PgVec<'mcx, NullableDatum> = vec_with_capacity_in(mcx, numaggs)?;
    agg_saved.resize(numaggs, NullableDatum::null());

    let start_offset_state = exec_init_expr(mcx, node.startOffset, params)?;
    let end_offset_state = exec_init_expr(mcx, node.endOffset, params)?;
    let (start_offset_typlen, start_offset_byval) = match node.startOffset {
        Some(off) => lsyscache::get_typlenbyval(expr_type(off))?,
        None => (0, true),
    };
    let (end_offset_typlen, end_offset_byval) = match node.endOffset {
        Some(off) => lsyscache::get_typlenbyval(expr_type(off))?,
        None => (0, true),
    };
    let runcondition = exec_init_qual(mcx, &node.runCondition, params)?;
    let qual = exec_init_qual(mcx, &node.plan.qual, params)?;
    let start_in_range = if node.startInRangeFunc != 0 {
        Some(fmgr_core::fmgr_info(node.startInRangeFunc)?)
    } else {
        None
    };
    let end_in_range = if node.endInRangeFunc != 0 {
        Some(fmgr_core::fmgr_info(node.endInRangeFunc)?)
    } else {
        None
    };

    Ok(WindowAggStateData {
        plan: node,
        frameOptions,
        ps_ExprContext,
        tmpcontext,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        proj,
        part_eq,
        ord_eq,
        buffer: None,
        scan_slot,
        first_part_slot,
        first_part_valid: false,
        agg_row_slot,
        agg_row_valid: false,
        temp_slot_1,
        temp_slot_2,
        framehead_slot,
        frametail_slot,
        perfunc,
        evaltrans,
        trans_init,
        _pergroup: pergroup,
        pergroup_base,
        peragg_wfuncno,
        peragg,
        agg_saved,
        agg_readptr: -1,
        agg_seekpos: -1,
        agg_markpos: -1,
        agg_mark_active: false,
        agg_values_base,
        agg_nulls_base,
        numaggs,
        currentpos: 0,
        frameheadpos: 0,
        frametailpos: 0,
        framehead_valid: false,
        frametail_valid: false,
        framehead_ptr: -1,
        frametail_ptr: -1,
        currentgroup: 0,
        frameheadgroup: 0,
        frametailgroup: 0,
        groupheadpos: 0,
        grouptailpos: -1,
        grouptail_valid: false,
        grouptail_ptr: -1,
        aggregatedbase: 0,
        aggregatedupto: 0,
        spooled_rows: 0,
        start_offset_state,
        end_offset_state,
        start_offset_value: Datum::null(),
        end_offset_value: Datum::null(),
        start_offset_typlen,
        start_offset_byval,
        end_offset_typlen,
        end_offset_byval,
        start_in_range,
        end_in_range,
        runcondition,
        qual,
        use_pass_through: !node.topWindow || node.partNumCols > 0,
        top_window: node.topWindow,
        all_first: true,
        partition_spooled: false,
        more_partitions: false,
        next_partition: true,
        status: WaStatus::Run,
    })
}

fn build_eq<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    desc: &Rc<TupleDescData<'static>>,
    num_cols: i32,
    col_idx: &[i16],
    operators: &[Oid],
    collations: &[Oid],
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    if num_cols == 0 {
        return Ok(None);
    }
    debug_assert!(col_idx.len() == num_cols as usize);
    let mut eqfuncoids: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_cols as usize)?;
    for &op in operators {
        eqfuncoids.push(lsyscache::get_opcode(op)?);
    }
    Ok(Some(exec_build_grouping_equal(mcx, desc, desc, col_idx, &eqfuncoids, collations)?))
}

// initialize_peragg (nodeWindowAgg.c), byval no-finalfn default-frame slice
// (nodeAgg precedent); invtransfn ignored: the frame head cannot move.
fn initialize_peragg_default<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    wfunc: &'mcx WindowFunc<'mcx>,
    agg_specs_args: &mut PgVec<'mcx, NodeList<'mcx>>,
    trans_init: &mut PgVec<'mcx, NullableDatum>,
    trans_fnoid: &mut PgVec<'mcx, Oid>,
    trans_collid: &mut PgVec<'mcx, Oid>,
    trans_typlen: &mut PgVec<'mcx, i16>,
) -> PgResult<()> {
    let shape = syscache_seams::lookup_pg_aggregate_shape::call(wfunc.winfnoid)?
        .ok_or_else(|| wfunc_lookup_failed(wfunc.winfnoid))?;
    if shape.aggkind != AGGKIND_NORMAL {
        panic!(
            "initialize_peragg (nodeWindowAgg.c): ordered-set/hypothetical aggkind {} \
             cannot be a window aggregate",
            shape.aggkind
        );
    }
    if shape.aggfinalfn != 0 {
        panic!(
            "finalize_windowaggregate (nodeWindowAgg.c): finalfn {} arm not ported",
            shape.aggfinalfn
        );
    }
    let transtype = shape.aggtranstype;
    let (translen, byval) = lsyscache::get_typlenbyval(transtype)?;
    trans_typlen.push(translen);
    if !byval {
        panic!(
            "advance_windowaggregate (nodeWindowAgg.c): by-ref transtype {transtype} \
             not ported"
        );
    }
    let initval = syscache_seams::pg_aggregate_agginitval::call(mcx, wfunc.winfnoid)?
        .ok_or_else(|| wfunc_lookup_failed(wfunc.winfnoid))?;
    trans_init.push(match initval {
        None => NullableDatum::null(),
        Some(text) => NullableDatum { value: get_agg_init_val(&text, transtype)?, isnull: false },
    });
    trans_fnoid.push(shape.aggtransfn);
    trans_collid.push(wfunc.inputcollid);

    // WindowFunc args are bare exprs; the shared trans builder consumes
    // Aggref-shaped TargetEntry cells.
    let mut args = NodeList::nil();
    for (i, arg) in wfunc.args.iter().enumerate() {
        args.lappend(mcx, Node::mk_target_entry(mcx, arg, (i + 1) as i16, None, false)?)?;
    }
    agg_specs_args.push(args);
    Ok(())
}

// initialize_peragg (nodeWindowAgg.c), framed lane: C's moving-aggregate
// selection verbatim, then closed-set kernel dispatch. Component-fn ACL
// checks vs the aggregate owner are skipped (proowner projection unported;
// C divergence, superuser-owned builtins in the live set).
fn initialize_peragg_framed<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    wfunc: &'mcx WindowFunc<'mcx>,
    frame_options: i32,
    wfuncno: u16,
    params: ::execexpr::ParamBind<'mcx>,
) -> PgResult<PerAggData<'mcx>> {
    let shape = syscache_seams::lookup_pg_aggregate_shape::call(wfunc.winfnoid)?
        .ok_or_else(|| wfunc_lookup_failed(wfunc.winfnoid))?;
    if shape.aggkind != AGGKIND_NORMAL {
        panic!(
            "initialize_peragg (nodeWindowAgg.c): ordered-set/hypothetical aggkind {} \
             cannot be a window aggregate",
            shape.aggkind
        );
    }
    let mut volatile = false;
    for a in wfunc.args.iter() {
        if contain_volatile_functions(a)? {
            volatile = true;
        }
    }
    let use_ma_code = if shape.aggminvtransfn == 0 {
        false
    } else if shape.aggmfinalmodify == AGGMODIFY_READ_ONLY
        && shape.aggfinalmodify != AGGMODIFY_READ_ONLY
    {
        true
    } else if frame_options & FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0 {
        false
    } else {
        !volatile
    };
    let (transfn_oid, invtransfn_oid, finalfn_oid, finalmodify, aggtranstype, minit) =
        if use_ma_code {
            (
                shape.aggmtransfn,
                shape.aggminvtransfn,
                shape.aggmfinalfn,
                shape.aggmfinalmodify,
                shape.aggmtranstype,
                true,
            )
        } else {
            (shape.aggtransfn, 0, shape.aggfinalfn, shape.aggfinalmodify, shape.aggtranstype, false)
        };
    if finalmodify != AGGMODIFY_READ_ONLY {
        panic!(
            "initialize_peragg (nodeWindowAgg.c): non-read-only finalfn error arm \
             (format_procedure) not ported; aggregate {}",
            wfunc.winfnoid
        );
    }
    let initval = if minit {
        syscache_seams::pg_aggregate_aggminitval::call(mcx, wfunc.winfnoid)?
    } else {
        syscache_seams::pg_aggregate_agginitval::call(mcx, wfunc.winfnoid)?
    }
    .ok_or_else(|| wfunc_lookup_failed(wfunc.winfnoid))?;

    let kernel;
    let fn_strict;
    let init_value;
    let mut has_inverse = invtransfn_oid != 0;
    match (transfn_oid, invtransfn_oid) {
        (F_INT2_AVG_ACCUM, F_INT2_AVG_ACCUM_INV) | (F_INT4_AVG_ACCUM, F_INT4_AVG_ACCUM_INV) => {
            assert!(
                finalfn_oid == F_INT2INT4_SUM,
                "MovingIntSum kernel: unexpected mfinalfn {finalfn_oid}"
            );
            assert!(
                initval.as_ref().map(|s| s.as_str()) == Some("{0,0}"),
                "MovingIntSum kernel: unexpected minitval {initval:?}"
            );
            kernel = AggKernel::MovingIntSum { int2: transfn_oid == F_INT2_AVG_ACCUM };
            fn_strict = true;
            has_inverse = true;
            init_value = NullableDatum { value: Datum::null(), isnull: false };
        }
        (t, 0) => {
            if finalfn_oid != 0 {
                panic!(
                    "finalize_windowaggregate (nodeWindowAgg.c): finalfn {finalfn_oid} arm \
                     not ported"
                );
            }
            let (_len, byval) = lsyscache::get_typlenbyval(aggtranstype)?;
            if !byval {
                panic!(
                    "advance_windowaggregate (nodeWindowAgg.c): by-ref transtype \
                     {aggtranstype} not ported"
                );
            }
            let transfn = fmgr_core::fmgr_info(t)?;
            fn_strict = transfn.fn_strict;
            kernel = AggKernel::Generic { transfn };
            init_value = match initval {
                Some(ref text) => NullableDatum {
                    value: get_agg_init_val(text.as_str(), aggtranstype)?,
                    isnull: false,
                },
                None => NullableDatum::null(),
            };
        }
        (t, inv) => {
            if finalfn_oid != 0 {
                panic!(
                    "finalize_windowaggregate (nodeWindowAgg.c): moving finalfn \
                     {finalfn_oid} arm not ported"
                );
            }
            let (_len, byval) = lsyscache::get_typlenbyval(aggtranstype)?;
            if !byval {
                panic!(
                    "advance_windowaggregate_base (nodeWindowAgg.c): by-ref moving \
                     transtype {aggtranstype} not ported"
                );
            }
            let transfn = fmgr_core::fmgr_info(t)?;
            let invtransfn = fmgr_core::fmgr_info(inv)?;
            if transfn.fn_strict != invtransfn.fn_strict {
                panic!(
                    "strictness of aggregate's forward and inverse transition functions \
                     must match (aggregate {})",
                    wfunc.winfnoid
                );
            }
            fn_strict = transfn.fn_strict;
            kernel = AggKernel::MovingByVal { transfn, invtransfn };
            init_value = match initval {
                Some(ref text) => NullableDatum {
                    value: get_agg_init_val(text.as_str(), aggtranstype)?,
                    isnull: false,
                },
                None => NullableDatum::null(),
            };
        }
    }
    // C's IsBinaryCoercible guard for strict transfn + NULL initval; only the
    // equal-types case is ported.
    if fn_strict && init_value.isnull {
        let first_type = wfunc.args.first().map(expr_type);
        if first_type != Some(aggtranstype) {
            panic!(
                "initialize_peragg (nodeWindowAgg.c): IsBinaryCoercible input/transtype \
                 check not ported (aggregate {})",
                wfunc.winfnoid
            );
        }
    }

    Ok(PerAggData {
        wfuncno,
        num_arguments: wfunc.args.len() as i16,
        win_collation: wfunc.inputcollid,
        kernel,
        fn_strict,
        has_inverse,
        argstates: build_argstates(mcx, &wfunc.args, params)?,
        init_value,
        trans_value: init_value,
        trans_count: 0,
        int_sum: Int8TransState::default(),
        result_value: NullableDatum::null(),
        restart: false,
    })
}

#[derive(Clone, Copy, PartialEq)]
enum WhichSlot {
    AggRow,
    Temp1,
    Temp2,
}

#[derive(Clone, Copy, PartialEq)]
enum SeekType {
    Current,
    Head,
    Tail,
}

impl<'mcx> WindowAggStateData<'mcx> {
    // prepare_tuplestore (nodeWindowAgg.c). Mark pointers are position
    // bookkeeping only (no tuplestore_trim); the agg read pointer gets
    // BACKWARD capability when the frame head can move (restart re-reads).
    fn prepare_tuplestore(&mut self) {
        debug_assert!(self.buffer.is_none());
        let work_mem = init_small::globals::work_mem();
        let mut buffer = Tuplestore::begin_heap(false, false, work_mem);
        buffer.set_eflags(0);
        if self.numaggs > 0 {
            let mut flags = 0;
            if self.frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING == 0
                || self.frameOptions & FRAMEOPTION_EXCLUSION != 0
            {
                self.agg_mark_active = true;
                flags |= EXEC_FLAG_BACKWARD;
            }
            self.agg_readptr = buffer.alloc_read_pointer(flags);
        }
        for pf in self.perfunc.iter_mut() {
            if !matches!(pf.kind, WfKind::PlainAgg { .. }) {
                pf.readptr = buffer.alloc_read_pointer(EXEC_FLAG_BACKWARD);
            }
        }
        if self.frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS) != 0 {
            if (self.frameOptions & FRAMEOPTION_START_CURRENT_ROW != 0
                && self.plan.ordNumCols != 0)
                || self.frameOptions & FRAMEOPTION_START_OFFSET != 0
            {
                self.framehead_ptr = buffer.alloc_read_pointer(0);
            }
            if (self.frameOptions & FRAMEOPTION_END_CURRENT_ROW != 0
                && self.plan.ordNumCols != 0)
                || self.frameOptions & FRAMEOPTION_END_OFFSET != 0
            {
                self.frametail_ptr = buffer.alloc_read_pointer(0);
            }
        }
        if self.frameOptions & (FRAMEOPTION_EXCLUDE_GROUP | FRAMEOPTION_EXCLUDE_TIES) != 0
            && self.plan.ordNumCols != 0
        {
            self.grouptail_ptr = buffer.alloc_read_pointer(0);
        }
        self.buffer = Some(buffer);
    }

    fn begin_partition<F>(&mut self, estate: &mut EStateData<'mcx>, fetch: &mut F) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        let mcx = estate.es_query_cxt;
        self.partition_spooled = false;
        self.framehead_valid = false;
        self.frametail_valid = false;
        self.grouptail_valid = false;
        self.spooled_rows = 0;
        self.currentpos = 0;
        self.frameheadpos = 0;
        self.frametailpos = 0;
        self.currentgroup = 0;
        self.frameheadgroup = 0;
        self.frametailgroup = 0;
        self.groupheadpos = 0;
        self.grouptailpos = -1;
        self.agg_row_valid = false;
        exectuples::exec_clear_tuple(&mut self.agg_row_slot, mcx);
        exectuples::exec_clear_tuple(&mut self.framehead_slot, mcx);
        exectuples::exec_clear_tuple(&mut self.frametail_slot, mcx);

        if !self.first_part_valid {
            match fetch(estate)? {
                Some(outer_id) => {
                    let outer_slot = estate.slot_mut(outer_id);
                    exectuples::exec_copy_slot(&mut self.first_part_slot, outer_slot, mcx, mcx)?;
                    self.first_part_valid = true;
                }
                None => {
                    self.partition_spooled = true;
                    self.more_partitions = false;
                    return Ok(());
                }
            }
        }
        if self.buffer.is_none() {
            self.prepare_tuplestore();
        }
        self.next_partition = false;

        if self.numaggs > 0 {
            self.agg_markpos = -1;
            self.agg_seekpos = -1;
            self.aggregatedbase = 0;
            self.aggregatedupto = 0;
        }
        for pf in self.perfunc.iter_mut() {
            if !matches!(pf.kind, WfKind::PlainAgg { .. }) {
                pf.seekpos = -1;
                pf.markpos = -1;
                pf.rank = 0;
                pf.ntile = 0;
                pf.rows_per_bucket = 0;
                pf.boundary = 0;
                pf.remainder = 0;
            }
        }
        self.buffer.as_mut().unwrap().puttupleslot(&mut self.first_part_slot, mcx)?;
        self.spooled_rows += 1;
        Ok(())
    }

    // spool_tuples: pos == -1 spools the whole partition (the pass-through
    // and spilled-store arms are unreachable: no runCondition, no spill).
    fn spool_tuples<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        pos: i64,
    ) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        if self.buffer.is_none() || self.partition_spooled {
            return Ok(());
        }
        let mcx = estate.es_query_cxt;
        while self.spooled_rows <= pos || pos == -1 {
            let Some(outer_id) = fetch(estate)? else {
                self.partition_spooled = true;
                self.more_partitions = false;
                break;
            };
            if self.plan.partNumCols > 0 {
                let same = {
                    let outer_slot = estate.slot_mut(outer_id);
                    let mut slots = EvalSlots {
                        scan: None,
                        inner: Some(&mut self.first_part_slot),
                        outer: Some(outer_slot),
                    };
                    exec_qual(self.part_eq.as_deref_mut(), &mut slots)?
                };
                estate.reset_expr_context(self.tmpcontext);
                if !same {
                    let outer_slot = estate.slot_mut(outer_id);
                    exectuples::exec_copy_slot(&mut self.first_part_slot, outer_slot, mcx, mcx)?;
                    self.partition_spooled = true;
                    self.more_partitions = true;
                    break;
                }
            }
            let outer_slot = estate.slot_mut(outer_id);
            self.buffer.as_mut().unwrap().puttupleslot(outer_slot, mcx)?;
            self.spooled_rows += 1;
        }
        Ok(())
    }

    fn release_partition(&mut self, estate: &mut EStateData<'mcx>) {
        let mcx = estate.es_query_cxt;
        // Rank/ntile state lives in perfunc (C: partcontext localmem); byval
        // trans values need no aggcontext reset.
        if let Some(buffer) = self.buffer.as_mut() {
            buffer.clear();
        }
        exectuples::exec_clear_tuple(&mut self.scan_slot, mcx);
        self.agg_row_valid = false;
        exectuples::exec_clear_tuple(&mut self.agg_row_slot, mcx);
        self.partition_spooled = false;
        self.next_partition = true;
    }

    // are_peers: no ORDER BY means all partition rows are peers.
    fn are_peers(
        estate: &mut EStateData<'mcx>,
        ord_eq: Option<&mut ExprState<'mcx>>,
        tmpcontext: EcxtId,
        slot1: &mut SlotData<'mcx>,
        slot2: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        let Some(ord_eq) = ord_eq else {
            return Ok(true);
        };
        let mut slots = EvalSlots { scan: None, inner: Some(slot2), outer: Some(slot1) };
        let r = exec_qual(Some(ord_eq), &mut slots)?;
        estate.reset_expr_context(tmpcontext);
        Ok(r)
    }

    // window_gettupleslot over (readptr, seekpos): borrowed fetches
    // (copy=false) are sound because the store never spills or trims within
    // a partition (C copies to survive both).
    fn gettupleslot_at<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        perfunc_ix: Option<usize>,
        pos: i64,
        which_slot: WhichSlot,
    ) -> PgResult<bool>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        if pos < 0 {
            return Ok(false);
        }
        self.spool_tuples(estate, fetch, pos)?;
        if pos >= self.spooled_rows {
            return Ok(false);
        }
        let (readptr, seekpos, markpos) = match perfunc_ix {
            Some(i) => {
                let pf = &self.perfunc[i];
                (pf.readptr, pf.seekpos, pf.markpos)
            }
            None => (self.agg_readptr, self.agg_seekpos, self.agg_markpos),
        };
        if pos < markpos {
            panic!("cannot fetch row before WindowObject's mark position");
        }
        let mcx = estate.es_query_cxt;
        let buffer = self.buffer.as_mut().unwrap();
        buffer.select_read_pointer(readptr);
        let mut seekpos = seekpos;
        if seekpos < pos - 1 {
            if !buffer.skiptuples(pos - 1 - seekpos, true) {
                panic!("unexpected end of tuplestore");
            }
            seekpos = pos - 1;
        } else if seekpos > pos + 1 {
            if !buffer.skiptuples(seekpos - (pos + 1), false) {
                panic!("unexpected end of tuplestore");
            }
            seekpos = pos + 1;
        } else if seekpos == pos {
            buffer.advance(true);
            seekpos += 1;
        }
        let slot = match which_slot {
            WhichSlot::AggRow => &mut self.agg_row_slot,
            WhichSlot::Temp1 => &mut self.temp_slot_1,
            WhichSlot::Temp2 => &mut self.temp_slot_2,
        };
        if seekpos > pos {
            if !buffer.gettupleslot(false, false, slot, mcx)? {
                panic!("unexpected end of tuplestore");
            }
            seekpos -= 1;
        } else {
            if !buffer.gettupleslot(true, false, slot, mcx)? {
                panic!("unexpected end of tuplestore");
            }
            seekpos += 1;
        }
        debug_assert!(seekpos == pos);
        match perfunc_ix {
            Some(i) => self.perfunc[i].seekpos = seekpos,
            None => self.agg_seekpos = seekpos,
        }
        Ok(true)
    }

    // WinSetMarkPosition minus the mark read pointer (no trim): the read
    // pointer still advances so later fetches never seek before the mark.
    fn set_mark_position(&mut self, perfunc_ix: usize, markpos: i64) {
        let pf = &mut self.perfunc[perfunc_ix];
        if markpos < pf.markpos {
            panic!("cannot move WindowObject's mark position backward");
        }
        pf.markpos = markpos;
        if markpos > pf.seekpos {
            let buffer = self.buffer.as_mut().unwrap();
            buffer.select_read_pointer(pf.readptr);
            buffer.skiptuples(markpos - pf.seekpos, true);
            pf.seekpos = markpos;
        }
    }

    fn set_agg_mark_position(&mut self, markpos: i64) {
        if markpos < self.agg_markpos {
            panic!("cannot move WindowObject's mark position backward");
        }
        self.agg_markpos = markpos;
        if markpos > self.agg_seekpos {
            let buffer = self.buffer.as_mut().unwrap();
            buffer.select_read_pointer(self.agg_readptr);
            buffer.skiptuples(markpos - self.agg_seekpos, true);
            self.agg_seekpos = markpos;
        }
    }

    // WinRowsArePeers over the perfunc's read pointer.
    fn rows_are_peers<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        perfunc_ix: usize,
        pos1: i64,
        pos2: i64,
    ) -> PgResult<bool>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        if self.plan.ordNumCols == 0 {
            return Ok(true);
        }
        if !self.gettupleslot_at(estate, fetch, Some(perfunc_ix), pos1, WhichSlot::Temp1)? {
            panic!("specified position is out of window: {pos1}");
        }
        if !self.gettupleslot_at(estate, fetch, Some(perfunc_ix), pos2, WhichSlot::Temp2)? {
            panic!("specified position is out of window: {pos2}");
        }
        let Self { ref mut temp_slot_1, ref mut temp_slot_2, ref mut ord_eq, tmpcontext, .. } =
            *self;
        Self::are_peers(estate, ord_eq.as_deref_mut(), tmpcontext, temp_slot_1, temp_slot_2)
    }

    // rank_up (windowfuncs.c): peer check against the prior row, then the
    // mark advances to the current row.
    fn rank_up<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        perfunc_ix: usize,
    ) -> PgResult<bool>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        let curpos = self.currentpos;
        let mut up = false;
        if self.perfunc[perfunc_ix].rank == 0 {
            debug_assert!(curpos == 0);
            self.perfunc[perfunc_ix].rank = 1;
        } else {
            debug_assert!(curpos > 0);
            up = !self.rows_are_peers(estate, fetch, perfunc_ix, curpos - 1, curpos)?;
        }
        self.set_mark_position(perfunc_ix, curpos);
        Ok(up)
    }

    // update_frameheadpos (nodeWindowAgg.c).
    fn update_frameheadpos<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
    ) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        if self.framehead_valid {
            return Ok(());
        }
        let fo = self.frameOptions;
        let mcx = estate.es_query_cxt;
        if fo & FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0 {
            self.frameheadpos = 0;
            self.framehead_valid = true;
        } else if fo & FRAMEOPTION_START_CURRENT_ROW != 0 {
            if fo & FRAMEOPTION_ROWS != 0 {
                self.frameheadpos = self.currentpos;
                self.framehead_valid = true;
            } else {
                debug_assert!(fo & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS) != 0);
                if self.plan.ordNumCols == 0 {
                    self.frameheadpos = 0;
                    self.framehead_valid = true;
                    return Ok(());
                }
                if self.frameheadpos == 0 && self.framehead_slot.base().is_empty() {
                    let Self { ref mut buffer, ref mut framehead_slot, framehead_ptr, .. } =
                        *self;
                    let buffer = buffer.as_mut().unwrap();
                    buffer.select_read_pointer(framehead_ptr);
                    if !buffer.gettupleslot(true, false, framehead_slot, mcx)? {
                        panic!("unexpected end of tuplestore");
                    }
                }
                while !self.framehead_slot.base().is_empty() {
                    let peers = {
                        let Self {
                            ref mut framehead_slot,
                            ref mut scan_slot,
                            ref mut ord_eq,
                            tmpcontext,
                            ..
                        } = *self;
                        Self::are_peers(
                            estate,
                            ord_eq.as_deref_mut(),
                            tmpcontext,
                            framehead_slot,
                            scan_slot,
                        )?
                    };
                    if peers {
                        break;
                    }
                    self.frameheadpos += 1;
                    self.spool_tuples(estate, fetch, self.frameheadpos)?;
                    let more_rows = self.frameheadpos < self.spooled_rows;
                    let Self { ref mut buffer, ref mut framehead_slot, framehead_ptr, .. } =
                        *self;
                    let buffer = buffer.as_mut().unwrap();
                    buffer.select_read_pointer(framehead_ptr);
                    if !more_rows || !buffer.gettupleslot(true, false, framehead_slot, mcx)? {
                        exectuples::exec_clear_tuple(framehead_slot, mcx);
                        break;
                    }
                }
                self.framehead_valid = true;
            }
        } else if fo & FRAMEOPTION_START_OFFSET != 0 {
            if fo & FRAMEOPTION_ROWS != 0 {
                let mut offset = self.start_offset_value.as_i64();
                if fo & FRAMEOPTION_START_OFFSET_PRECEDING != 0 {
                    offset = -offset;
                }
                self.frameheadpos = self.currentpos + offset;
                if self.frameheadpos < 0 {
                    self.frameheadpos = 0;
                } else if self.frameheadpos > self.currentpos + 1 {
                    self.spool_tuples(estate, fetch, self.frameheadpos - 1)?;
                    if self.frameheadpos > self.spooled_rows {
                        self.frameheadpos = self.spooled_rows;
                    }
                }
                self.framehead_valid = true;
            } else if fo & FRAMEOPTION_RANGE != 0 {
                debug_assert!(self.plan.ordNumCols == 1);
                let sort_col = self.plan.ordColIdx[0] as i32;
                let mut sub = fo & FRAMEOPTION_START_OFFSET_PRECEDING != 0;
                let mut less = false;
                if !self.plan.inRangeAsc {
                    sub = !sub;
                    less = true;
                }
                if self.frameheadpos == 0 && self.framehead_slot.base().is_empty() {
                    let Self { ref mut buffer, ref mut framehead_slot, framehead_ptr, .. } =
                        *self;
                    let buffer = buffer.as_mut().unwrap();
                    buffer.select_read_pointer(framehead_ptr);
                    if !buffer.gettupleslot(true, false, framehead_slot, mcx)? {
                        panic!("unexpected end of tuplestore");
                    }
                }
                while !self.framehead_slot.base().is_empty() {
                    let stop = {
                        let Self {
                            ref mut framehead_slot,
                            ref mut scan_slot,
                            ref mut start_in_range,
                            start_offset_value,
                            plan,
                            ..
                        } = *self;
                        let mut headisnull = false;
                        let mut currisnull = false;
                        let headval =
                            exectuples::slot_getattr(framehead_slot, sort_col, &mut headisnull);
                        let currval =
                            exectuples::slot_getattr(scan_slot, sort_col, &mut currisnull);
                        if headisnull || currisnull {
                            if plan.inRangeNullsFirst {
                                !headisnull || currisnull
                            } else {
                                headisnull || !currisnull
                            }
                        } else {
                            fmgr_core::function_call5_coll(
                                start_in_range.as_mut().unwrap(),
                                plan.inRangeColl,
                                headval,
                                currval,
                                start_offset_value,
                                Datum::from_bool(sub),
                                Datum::from_bool(less),
                            )?
                            .as_bool()
                        }
                    };
                    if stop {
                        break;
                    }
                    self.frameheadpos += 1;
                    self.spool_tuples(estate, fetch, self.frameheadpos)?;
                    let more_rows = self.frameheadpos < self.spooled_rows;
                    let Self { ref mut buffer, ref mut framehead_slot, framehead_ptr, .. } =
                        *self;
                    let buffer = buffer.as_mut().unwrap();
                    buffer.select_read_pointer(framehead_ptr);
                    if !more_rows || !buffer.gettupleslot(true, false, framehead_slot, mcx)? {
                        exectuples::exec_clear_tuple(framehead_slot, mcx);
                        break;
                    }
                }
                self.framehead_valid = true;
            } else {
                debug_assert!(fo & FRAMEOPTION_GROUPS != 0);
                let offset = self.start_offset_value.as_i64();
                let minheadgroup = if fo & FRAMEOPTION_START_OFFSET_PRECEDING != 0 {
                    self.currentgroup - offset
                } else {
                    self.currentgroup + offset
                };
                if self.frameheadpos == 0 && self.framehead_slot.base().is_empty() {
                    let Self { ref mut buffer, ref mut framehead_slot, framehead_ptr, .. } =
                        *self;
                    let buffer = buffer.as_mut().unwrap();
                    buffer.select_read_pointer(framehead_ptr);
                    if !buffer.gettupleslot(true, false, framehead_slot, mcx)? {
                        panic!("unexpected end of tuplestore");
                    }
                }
                while !self.framehead_slot.base().is_empty() {
                    if self.frameheadgroup >= minheadgroup {
                        break;
                    }
                    {
                        let Self { ref mut temp_slot_2, ref mut framehead_slot, .. } = *self;
                        exectuples::exec_copy_slot(temp_slot_2, framehead_slot, mcx, mcx)?;
                    }
                    self.frameheadpos += 1;
                    self.spool_tuples(estate, fetch, self.frameheadpos)?;
                    let more_rows = self.frameheadpos < self.spooled_rows;
                    let fetched = {
                        let Self { ref mut buffer, ref mut framehead_slot, framehead_ptr, .. } =
                            *self;
                        let buffer = buffer.as_mut().unwrap();
                        buffer.select_read_pointer(framehead_ptr);
                        more_rows && buffer.gettupleslot(true, false, framehead_slot, mcx)?
                    };
                    if !fetched {
                        exectuples::exec_clear_tuple(&mut self.framehead_slot, mcx);
                        break;
                    }
                    let peers = {
                        let Self {
                            ref mut temp_slot_2,
                            ref mut framehead_slot,
                            ref mut ord_eq,
                            tmpcontext,
                            ..
                        } = *self;
                        Self::are_peers(
                            estate,
                            ord_eq.as_deref_mut(),
                            tmpcontext,
                            temp_slot_2,
                            framehead_slot,
                        )?
                    };
                    if !peers {
                        self.frameheadgroup += 1;
                    }
                }
                exectuples::exec_clear_tuple(&mut self.temp_slot_2, mcx);
                self.framehead_valid = true;
            }
        } else {
            unreachable!()
        }
        Ok(())
    }

    // update_frametailpos (nodeWindowAgg.c).
    fn update_frametailpos<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
    ) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        if self.frametail_valid {
            return Ok(());
        }
        let fo = self.frameOptions;
        let mcx = estate.es_query_cxt;
        if fo & FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
            self.spool_tuples(estate, fetch, -1)?;
            self.frametailpos = self.spooled_rows;
            self.frametail_valid = true;
        } else if fo & FRAMEOPTION_END_CURRENT_ROW != 0 {
            if fo & FRAMEOPTION_ROWS != 0 {
                self.frametailpos = self.currentpos + 1;
                self.frametail_valid = true;
            } else {
                debug_assert!(fo & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS) != 0);
                if self.plan.ordNumCols == 0 {
                    self.spool_tuples(estate, fetch, -1)?;
                    self.frametailpos = self.spooled_rows;
                    self.frametail_valid = true;
                    return Ok(());
                }
                if self.frametailpos == 0 && self.frametail_slot.base().is_empty() {
                    let Self { ref mut buffer, ref mut frametail_slot, frametail_ptr, .. } =
                        *self;
                    let buffer = buffer.as_mut().unwrap();
                    buffer.select_read_pointer(frametail_ptr);
                    if !buffer.gettupleslot(true, false, frametail_slot, mcx)? {
                        panic!("unexpected end of tuplestore");
                    }
                }
                while !self.frametail_slot.base().is_empty() {
                    if self.frametailpos > self.currentpos {
                        let peers = {
                            let Self {
                                ref mut frametail_slot,
                                ref mut scan_slot,
                                ref mut ord_eq,
                                tmpcontext,
                                ..
                            } = *self;
                            Self::are_peers(
                                estate,
                                ord_eq.as_deref_mut(),
                                tmpcontext,
                                frametail_slot,
                                scan_slot,
                            )?
                        };
                        if !peers {
                            break;
                        }
                    }
                    self.frametailpos += 1;
                    self.spool_tuples(estate, fetch, self.frametailpos)?;
                    let more_rows = self.frametailpos < self.spooled_rows;
                    let Self { ref mut buffer, ref mut frametail_slot, frametail_ptr, .. } =
                        *self;
                    let buffer = buffer.as_mut().unwrap();
                    buffer.select_read_pointer(frametail_ptr);
                    if !more_rows || !buffer.gettupleslot(true, false, frametail_slot, mcx)? {
                        exectuples::exec_clear_tuple(frametail_slot, mcx);
                        break;
                    }
                }
                self.frametail_valid = true;
            }
        } else if fo & FRAMEOPTION_END_OFFSET != 0 {
            if fo & FRAMEOPTION_ROWS != 0 {
                let mut offset = self.end_offset_value.as_i64();
                if fo & FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
                    offset = -offset;
                }
                self.frametailpos = self.currentpos + offset + 1;
                if self.frametailpos < 0 {
                    self.frametailpos = 0;
                } else if self.frametailpos > self.currentpos + 1 {
                    self.spool_tuples(estate, fetch, self.frametailpos - 1)?;
                    if self.frametailpos > self.spooled_rows {
                        self.frametailpos = self.spooled_rows;
                    }
                }
                self.frametail_valid = true;
            } else if fo & FRAMEOPTION_RANGE != 0 {
                debug_assert!(self.plan.ordNumCols == 1);
                let sort_col = self.plan.ordColIdx[0] as i32;
                let mut sub = fo & FRAMEOPTION_END_OFFSET_PRECEDING != 0;
                let mut less = true;
                if !self.plan.inRangeAsc {
                    sub = !sub;
                    less = false;
                }
                if self.frametailpos == 0 && self.frametail_slot.base().is_empty() {
                    let Self { ref mut buffer, ref mut frametail_slot, frametail_ptr, .. } =
                        *self;
                    let buffer = buffer.as_mut().unwrap();
                    buffer.select_read_pointer(frametail_ptr);
                    if !buffer.gettupleslot(true, false, frametail_slot, mcx)? {
                        panic!("unexpected end of tuplestore");
                    }
                }
                while !self.frametail_slot.base().is_empty() {
                    let stop = {
                        let Self {
                            ref mut frametail_slot,
                            ref mut scan_slot,
                            ref mut end_in_range,
                            end_offset_value,
                            plan,
                            ..
                        } = *self;
                        let mut tailisnull = false;
                        let mut currisnull = false;
                        let tailval =
                            exectuples::slot_getattr(frametail_slot, sort_col, &mut tailisnull);
                        let currval =
                            exectuples::slot_getattr(scan_slot, sort_col, &mut currisnull);
                        if tailisnull || currisnull {
                            if plan.inRangeNullsFirst {
                                !tailisnull
                            } else {
                                !currisnull
                            }
                        } else {
                            !fmgr_core::function_call5_coll(
                                end_in_range.as_mut().unwrap(),
                                plan.inRangeColl,
                                tailval,
                                currval,
                                end_offset_value,
                                Datum::from_bool(sub),
                                Datum::from_bool(less),
                            )?
                            .as_bool()
                        }
                    };
                    if stop {
                        break;
                    }
                    self.frametailpos += 1;
                    self.spool_tuples(estate, fetch, self.frametailpos)?;
                    let more_rows = self.frametailpos < self.spooled_rows;
                    let Self { ref mut buffer, ref mut frametail_slot, frametail_ptr, .. } =
                        *self;
                    let buffer = buffer.as_mut().unwrap();
                    buffer.select_read_pointer(frametail_ptr);
                    if !more_rows || !buffer.gettupleslot(true, false, frametail_slot, mcx)? {
                        exectuples::exec_clear_tuple(frametail_slot, mcx);
                        break;
                    }
                }
                self.frametail_valid = true;
            } else {
                debug_assert!(fo & FRAMEOPTION_GROUPS != 0);
                let offset = self.end_offset_value.as_i64();
                let maxtailgroup = if fo & FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
                    self.currentgroup - offset
                } else {
                    self.currentgroup + offset
                };
                if self.frametailpos == 0 && self.frametail_slot.base().is_empty() {
                    let Self { ref mut buffer, ref mut frametail_slot, frametail_ptr, .. } =
                        *self;
                    let buffer = buffer.as_mut().unwrap();
                    buffer.select_read_pointer(frametail_ptr);
                    if !buffer.gettupleslot(true, false, frametail_slot, mcx)? {
                        panic!("unexpected end of tuplestore");
                    }
                }
                while !self.frametail_slot.base().is_empty() {
                    if self.frametailgroup > maxtailgroup {
                        break;
                    }
                    {
                        let Self { ref mut temp_slot_2, ref mut frametail_slot, .. } = *self;
                        exectuples::exec_copy_slot(temp_slot_2, frametail_slot, mcx, mcx)?;
                    }
                    self.frametailpos += 1;
                    self.spool_tuples(estate, fetch, self.frametailpos)?;
                    let more_rows = self.frametailpos < self.spooled_rows;
                    let fetched = {
                        let Self { ref mut buffer, ref mut frametail_slot, frametail_ptr, .. } =
                            *self;
                        let buffer = buffer.as_mut().unwrap();
                        buffer.select_read_pointer(frametail_ptr);
                        more_rows && buffer.gettupleslot(true, false, frametail_slot, mcx)?
                    };
                    if !fetched {
                        exectuples::exec_clear_tuple(&mut self.frametail_slot, mcx);
                        break;
                    }
                    let peers = {
                        let Self {
                            ref mut temp_slot_2,
                            ref mut frametail_slot,
                            ref mut ord_eq,
                            tmpcontext,
                            ..
                        } = *self;
                        Self::are_peers(
                            estate,
                            ord_eq.as_deref_mut(),
                            tmpcontext,
                            temp_slot_2,
                            frametail_slot,
                        )?
                    };
                    if !peers {
                        self.frametailgroup += 1;
                    }
                }
                exectuples::exec_clear_tuple(&mut self.temp_slot_2, mcx);
                self.frametail_valid = true;
            }
        } else {
            unreachable!()
        }
        Ok(())
    }

    // update_grouptailpos (nodeWindowAgg.c); clobbers temp_slot_2.
    fn update_grouptailpos<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
    ) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        if self.grouptail_valid {
            return Ok(());
        }
        let mcx = estate.es_query_cxt;
        if self.plan.ordNumCols == 0 {
            self.spool_tuples(estate, fetch, -1)?;
            self.grouptailpos = self.spooled_rows;
            self.grouptail_valid = true;
            return Ok(());
        }
        debug_assert!(self.grouptailpos <= self.currentpos);
        self.buffer.as_mut().unwrap().select_read_pointer(self.grouptail_ptr);
        loop {
            self.grouptailpos += 1;
            self.spool_tuples(estate, fetch, self.grouptailpos)?;
            let fetched = {
                let Self { ref mut buffer, ref mut temp_slot_2, .. } = *self;
                buffer.as_mut().unwrap().gettupleslot(true, false, temp_slot_2, mcx)?
            };
            if !fetched {
                break;
            }
            if self.grouptailpos > self.currentpos {
                let peers = {
                    let Self {
                        ref mut temp_slot_2,
                        ref mut scan_slot,
                        ref mut ord_eq,
                        tmpcontext,
                        ..
                    } = *self;
                    Self::are_peers(
                        estate,
                        ord_eq.as_deref_mut(),
                        tmpcontext,
                        temp_slot_2,
                        scan_slot,
                    )?
                };
                if !peers {
                    break;
                }
            }
        }
        exectuples::exec_clear_tuple(&mut self.temp_slot_2, mcx);
        self.grouptail_valid = true;
        Ok(())
    }

    // row_is_in_frame (nodeWindowAgg.c): -1 out of frame and none follow,
    // 0 out of frame, 1 in frame.
    fn row_is_in_frame<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        pos: i64,
        which_slot: WhichSlot,
    ) -> PgResult<i32>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        let fo = self.frameOptions;
        debug_assert!(pos >= 0);
        self.update_frameheadpos(estate, fetch)?;
        if pos < self.frameheadpos {
            return Ok(0);
        }
        if fo & FRAMEOPTION_END_CURRENT_ROW != 0 {
            if fo & FRAMEOPTION_ROWS != 0 {
                if pos > self.currentpos {
                    return Ok(-1);
                }
            } else {
                debug_assert!(fo & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS) != 0);
                if pos > self.currentpos {
                    let peers = {
                        let Self {
                            ref mut agg_row_slot,
                            ref mut temp_slot_1,
                            ref mut temp_slot_2,
                            ref mut scan_slot,
                            ref mut ord_eq,
                            tmpcontext,
                            ..
                        } = *self;
                        let slot = match which_slot {
                            WhichSlot::AggRow => agg_row_slot,
                            WhichSlot::Temp1 => temp_slot_1,
                            WhichSlot::Temp2 => temp_slot_2,
                        };
                        Self::are_peers(estate, ord_eq.as_deref_mut(), tmpcontext, slot, scan_slot)?
                    };
                    if !peers {
                        return Ok(-1);
                    }
                }
            }
        } else if fo & FRAMEOPTION_END_OFFSET != 0 {
            if fo & FRAMEOPTION_ROWS != 0 {
                let mut offset = self.end_offset_value.as_i64();
                if fo & FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
                    offset = -offset;
                }
                if pos > self.currentpos + offset {
                    return Ok(-1);
                }
            } else {
                debug_assert!(fo & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS) != 0);
                self.update_frametailpos(estate, fetch)?;
                if pos >= self.frametailpos {
                    return Ok(-1);
                }
            }
        }
        if fo & FRAMEOPTION_EXCLUDE_CURRENT_ROW != 0 {
            if pos == self.currentpos {
                return Ok(0);
            }
        } else if fo & FRAMEOPTION_EXCLUDE_GROUP != 0
            || (fo & FRAMEOPTION_EXCLUDE_TIES != 0 && pos != self.currentpos)
        {
            if self.plan.ordNumCols == 0 {
                return Ok(0);
            }
            if pos >= self.groupheadpos {
                self.update_grouptailpos(estate, fetch)?;
                if pos < self.grouptailpos {
                    return Ok(0);
                }
            }
        }
        Ok(1)
    }

    // calculate_frame_offsets (nodeWindowAgg.c); by-ref offset values get
    // C's query-lifespan datumCopy (the eval context resets below).
    fn calculate_frame_offsets(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        debug_assert!(self.all_first);
        let fo = self.frameOptions;
        let mcx = estate.es_query_cxt;
        for state in [self.start_offset_state.as_deref(), self.end_offset_state.as_deref()] {
            if let Some(state) = state {
                let deps = state.param_exec_deps();
                if !deps.is_empty() {
                    ::executils::exec_eval_param_exec_params(estate, deps)?;
                }
            }
        }
        if fo & FRAMEOPTION_START_OFFSET != 0 {
            let state = self.start_offset_state.as_deref_mut().expect("startOffset ExprState");
            let mut slots = EvalSlots::default();
            let nd = exec_eval_expr(state, &mut slots)?;
            if nd.isnull {
                return Err(frame_offset_null(true));
            }
            self.start_offset_value = if self.start_offset_byval {
                nd.value
            } else {
                datum_copy(mcx, nd.value, self.start_offset_typlen)?
            };
            if fo & (FRAMEOPTION_ROWS | FRAMEOPTION_GROUPS) != 0 && nd.value.as_i64() < 0 {
                return Err(frame_offset_negative(true));
            }
        }
        if fo & FRAMEOPTION_END_OFFSET != 0 {
            let state = self.end_offset_state.as_deref_mut().expect("endOffset ExprState");
            let mut slots = EvalSlots::default();
            let nd = exec_eval_expr(state, &mut slots)?;
            if nd.isnull {
                return Err(frame_offset_null(false));
            }
            self.end_offset_value = if self.end_offset_byval {
                nd.value
            } else {
                datum_copy(mcx, nd.value, self.end_offset_typlen)?
            };
            if fo & (FRAMEOPTION_ROWS | FRAMEOPTION_GROUPS) != 0 && nd.value.as_i64() < 0 {
                return Err(frame_offset_negative(false));
            }
        }
        estate.reset_expr_context(self.ps_ExprContext);
        self.all_first = false;
        Ok(())
    }

    fn eval_arg_on_slot(
        &mut self,
        perfunc_ix: usize,
        argno: usize,
        which: WhichSlot,
    ) -> PgResult<NullableDatum> {
        let Self {
            ref mut perfunc,
            ref mut agg_row_slot,
            ref mut temp_slot_1,
            ref mut temp_slot_2,
            ..
        } = *self;
        let slot = match which {
            WhichSlot::AggRow => agg_row_slot,
            WhichSlot::Temp1 => temp_slot_1,
            WhichSlot::Temp2 => temp_slot_2,
        };
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(slot) };
        exec_eval_expr(&mut perfunc[perfunc_ix].argstates[argno], &mut slots)
    }

    // WinGetFuncArgCurrent: evaluate on the current row (scan slot).
    fn win_get_func_arg_current(
        &mut self,
        perfunc_ix: usize,
        argno: usize,
    ) -> PgResult<NullableDatum> {
        let Self { ref mut perfunc, ref mut scan_slot, .. } = *self;
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(scan_slot) };
        exec_eval_expr(&mut perfunc[perfunc_ix].argstates[argno], &mut slots)
    }

    // WinGetFuncArgInPartition; the result may borrow tuplestore memory
    // (stable within the partition), so C's numfuncs>1 datumCopy is skipped.
    fn win_get_func_arg_in_partition<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        perfunc_ix: usize,
        argno: usize,
        relpos: i64,
        seektype: SeekType,
        set_mark: bool,
    ) -> PgResult<(NullableDatum, bool)>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        let abs_pos = match seektype {
            SeekType::Current => self.currentpos + relpos,
            SeekType::Head => relpos,
            SeekType::Tail => {
                self.spool_tuples(estate, fetch, -1)?;
                self.spooled_rows - 1 + relpos
            }
        };
        if !self.gettupleslot_at(estate, fetch, Some(perfunc_ix), abs_pos, WhichSlot::Temp1)? {
            return Ok((NullableDatum::null(), true));
        }
        if set_mark {
            self.set_mark_position(perfunc_ix, abs_pos);
        }
        let nd = self.eval_arg_on_slot(perfunc_ix, argno, WhichSlot::Temp1)?;
        Ok((nd, false))
    }

    // WinGetFuncArgInFrame.
    fn win_get_func_arg_in_frame<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        perfunc_ix: usize,
        argno: usize,
        relpos: i64,
        seektype: SeekType,
        set_mark: bool,
    ) -> PgResult<(NullableDatum, bool)>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        let mut abs_pos;
        let mark_pos;
        let exclusion = self.frameOptions & FRAMEOPTION_EXCLUSION;
        match seektype {
            SeekType::Current => {
                panic!("WINDOW_SEEK_CURRENT is not supported for WinGetFuncArgInFrame")
            }
            SeekType::Head => {
                if relpos < 0 {
                    return Ok((NullableDatum::null(), true));
                }
                self.update_frameheadpos(estate, fetch)?;
                abs_pos = self.frameheadpos + relpos;
                mark_pos = abs_pos;
                // Exclusion advances abs_pos only, never mark_pos: peer-group
                // changes must not seek before an already-set mark.
                match exclusion {
                    0 => {}
                    FRAMEOPTION_EXCLUDE_CURRENT_ROW => {
                        if abs_pos >= self.currentpos && self.currentpos >= self.frameheadpos {
                            abs_pos += 1;
                        }
                    }
                    FRAMEOPTION_EXCLUDE_GROUP => {
                        self.update_grouptailpos(estate, fetch)?;
                        if abs_pos >= self.groupheadpos
                            && self.grouptailpos > self.frameheadpos
                        {
                            let overlapstart = self.groupheadpos.max(self.frameheadpos);
                            abs_pos += self.grouptailpos - overlapstart;
                        }
                    }
                    FRAMEOPTION_EXCLUDE_TIES => {
                        self.update_grouptailpos(estate, fetch)?;
                        if abs_pos >= self.groupheadpos
                            && self.grouptailpos > self.frameheadpos
                        {
                            let overlapstart = self.groupheadpos.max(self.frameheadpos);
                            if abs_pos == overlapstart {
                                abs_pos = self.currentpos;
                            } else {
                                abs_pos += self.grouptailpos - overlapstart - 1;
                            }
                        }
                    }
                    _ => panic!("unrecognized frame option state: {:#x}", self.frameOptions),
                }
            }
            SeekType::Tail => {
                if relpos > 0 {
                    return Ok((NullableDatum::null(), true));
                }
                self.update_frametailpos(estate, fetch)?;
                abs_pos = self.frametailpos - 1 + relpos;
                // With exclusion the mark can only go to the frame head: the
                // exclusion may fetch arbitrarily far back within the frame.
                match exclusion {
                    0 => {
                        mark_pos = abs_pos;
                    }
                    FRAMEOPTION_EXCLUDE_CURRENT_ROW => {
                        if abs_pos <= self.currentpos && self.currentpos < self.frametailpos {
                            abs_pos -= 1;
                        }
                        self.update_frameheadpos(estate, fetch)?;
                        if abs_pos < self.frameheadpos {
                            return Ok((NullableDatum::null(), true));
                        }
                        mark_pos = self.frameheadpos;
                    }
                    FRAMEOPTION_EXCLUDE_GROUP => {
                        self.update_grouptailpos(estate, fetch)?;
                        if abs_pos < self.grouptailpos
                            && self.groupheadpos < self.frametailpos
                        {
                            let overlapend = self.grouptailpos.min(self.frametailpos);
                            abs_pos -= overlapend - self.groupheadpos;
                        }
                        self.update_frameheadpos(estate, fetch)?;
                        if abs_pos < self.frameheadpos {
                            return Ok((NullableDatum::null(), true));
                        }
                        mark_pos = self.frameheadpos;
                    }
                    FRAMEOPTION_EXCLUDE_TIES => {
                        self.update_grouptailpos(estate, fetch)?;
                        if abs_pos < self.grouptailpos
                            && self.groupheadpos < self.frametailpos
                        {
                            let overlapend = self.grouptailpos.min(self.frametailpos);
                            if abs_pos == overlapend - 1 {
                                abs_pos = self.currentpos;
                            } else {
                                abs_pos -= overlapend - 1 - self.groupheadpos;
                            }
                        }
                        self.update_frameheadpos(estate, fetch)?;
                        if abs_pos < self.frameheadpos {
                            return Ok((NullableDatum::null(), true));
                        }
                        mark_pos = self.frameheadpos;
                    }
                    _ => panic!("unrecognized frame option state: {:#x}", self.frameOptions),
                }
            }
        }
        if !self.gettupleslot_at(estate, fetch, Some(perfunc_ix), abs_pos, WhichSlot::Temp1)? {
            return Ok((NullableDatum::null(), true));
        }
        if self.row_is_in_frame(estate, fetch, abs_pos, WhichSlot::Temp1)? <= 0 {
            return Ok((NullableDatum::null(), true));
        }
        if set_mark {
            self.set_mark_position(perfunc_ix, mark_pos);
        }
        let nd = self.eval_arg_on_slot(perfunc_ix, argno, WhichSlot::Temp1)?;
        Ok((nd, false))
    }

    fn eval_windowfunction<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
        perfunc_ix: usize,
    ) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        let result: NullableDatum = match self.perfunc[perfunc_ix].kind {
            WfKind::RowNumber => {
                let curpos = self.currentpos;
                self.set_mark_position(perfunc_ix, curpos);
                NullableDatum::value(Datum::from_i64(curpos + 1))
            }
            WfKind::Rank => {
                let up = self.rank_up(estate, fetch, perfunc_ix)?;
                if up {
                    self.perfunc[perfunc_ix].rank = self.currentpos + 1;
                }
                NullableDatum::value(Datum::from_i64(self.perfunc[perfunc_ix].rank))
            }
            WfKind::DenseRank => {
                let up = self.rank_up(estate, fetch, perfunc_ix)?;
                if up {
                    self.perfunc[perfunc_ix].rank += 1;
                }
                NullableDatum::value(Datum::from_i64(self.perfunc[perfunc_ix].rank))
            }
            WfKind::PercentRank => {
                self.spool_tuples(estate, fetch, -1)?;
                let totalrows = self.spooled_rows;
                debug_assert!(totalrows > 0);
                let up = self.rank_up(estate, fetch, perfunc_ix)?;
                if up {
                    self.perfunc[perfunc_ix].rank = self.currentpos + 1;
                }
                if totalrows <= 1 {
                    NullableDatum::value(Datum::from_f64(0.0))
                } else {
                    let rank = self.perfunc[perfunc_ix].rank;
                    NullableDatum::value(Datum::from_f64(
                        (rank - 1) as f64 / (totalrows - 1) as f64,
                    ))
                }
            }
            WfKind::CumeDist => {
                self.spool_tuples(estate, fetch, -1)?;
                let totalrows = self.spooled_rows;
                debug_assert!(totalrows > 0);
                let up = self.rank_up(estate, fetch, perfunc_ix)?;
                if up || self.perfunc[perfunc_ix].rank == 1 {
                    self.perfunc[perfunc_ix].rank = self.currentpos + 1;
                    let mut row = self.perfunc[perfunc_ix].rank;
                    while row < totalrows {
                        if !self.rows_are_peers(estate, fetch, perfunc_ix, row - 1, row)? {
                            break;
                        }
                        self.perfunc[perfunc_ix].rank += 1;
                        row += 1;
                    }
                }
                let rank = self.perfunc[perfunc_ix].rank;
                NullableDatum::value(Datum::from_f64(rank as f64 / totalrows as f64))
            }
            WfKind::Ntile => {
                if self.perfunc[perfunc_ix].ntile == 0 {
                    self.spool_tuples(estate, fetch, -1)?;
                    let total = self.spooled_rows;
                    let nd = self.win_get_func_arg_current(perfunc_ix, 0)?;
                    if nd.isnull {
                        self.write_result(perfunc_ix, NullableDatum::null());
                        return Ok(());
                    }
                    let nbuckets = nd.value.as_i32();
                    if nbuckets <= 0 {
                        return Err(ntile_arg_error());
                    }
                    let pf = &mut self.perfunc[perfunc_ix];
                    pf.ntile = 1;
                    pf.rows_per_bucket = 0;
                    pf.boundary = total / nbuckets as i64;
                    if pf.boundary <= 0 {
                        pf.boundary = 1;
                    } else {
                        pf.remainder = total % nbuckets as i64;
                        if pf.remainder != 0 {
                            pf.boundary += 1;
                        }
                    }
                }
                let pf = &mut self.perfunc[perfunc_ix];
                pf.rows_per_bucket += 1;
                if pf.boundary < pf.rows_per_bucket {
                    if pf.remainder != 0 && pf.ntile as i64 == pf.remainder {
                        pf.remainder = 0;
                        pf.boundary -= 1;
                    }
                    pf.ntile += 1;
                    pf.rows_per_bucket = 1;
                }
                NullableDatum::value(Datum::from_i32(pf.ntile))
            }
            WfKind::LeadLag { forward, withoffset, withdefault } => {
                let (offset, const_offset) = if withoffset {
                    let nd = self.win_get_func_arg_current(perfunc_ix, 1)?;
                    if nd.isnull {
                        self.write_result(perfunc_ix, NullableDatum::null());
                        return Ok(());
                    }
                    (nd.value.as_i32(), self.perfunc[perfunc_ix].arg1_stable)
                } else {
                    (1, true)
                };
                let relpos = if forward { offset as i64 } else { -(offset as i64) };
                let (mut nd, isout) = self.win_get_func_arg_in_partition(
                    estate,
                    fetch,
                    perfunc_ix,
                    0,
                    relpos,
                    SeekType::Current,
                    const_offset,
                )?;
                if isout && withdefault {
                    nd = self.win_get_func_arg_current(perfunc_ix, 2)?;
                }
                nd
            }
            WfKind::FirstValue => {
                let (nd, _isout) = self.win_get_func_arg_in_frame(
                    estate,
                    fetch,
                    perfunc_ix,
                    0,
                    0,
                    SeekType::Head,
                    true,
                )?;
                nd
            }
            WfKind::LastValue => {
                let (nd, _isout) = self.win_get_func_arg_in_frame(
                    estate,
                    fetch,
                    perfunc_ix,
                    0,
                    0,
                    SeekType::Tail,
                    true,
                )?;
                nd
            }
            WfKind::NthValue => {
                let nd = self.win_get_func_arg_current(perfunc_ix, 1)?;
                if nd.isnull {
                    self.write_result(perfunc_ix, NullableDatum::null());
                    return Ok(());
                }
                let nth = nd.value.as_i32();
                let const_offset = self.perfunc[perfunc_ix].arg1_stable;
                if nth <= 0 {
                    return Err(nth_value_arg_error());
                }
                let (nd, _isout) = self.win_get_func_arg_in_frame(
                    estate,
                    fetch,
                    perfunc_ix,
                    0,
                    (nth - 1) as i64,
                    SeekType::Head,
                    const_offset,
                )?;
                nd
            }
            WfKind::PlainAgg { .. } => unreachable!("plain aggs go through eval_windowaggregates"),
        };
        self.write_result(perfunc_ix, result);
        Ok(())
    }

    fn write_result(&mut self, perfunc_ix: usize, result: NullableDatum) {
        let wfuncno = self.perfunc[perfunc_ix].wfuncno as usize;
        self.write_agg_result(wfuncno, result);
    }

    fn write_agg_result(&mut self, wfuncno: usize, result: NullableDatum) {
        // SAFETY: wfuncno < numfuncs elements of the once-allocated arrays.
        unsafe {
            self.agg_values_base.as_ptr().add(wfuncno).write(result.value);
            self.agg_nulls_base.as_ptr().add(wfuncno).write(result.isnull);
        }
    }

    // eval_windowaggregates, default-frame arm: aggregates restart only on
    // the partition's first row; row_is_in_frame collapses to the peer test.
    fn eval_windowaggregates_default<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
    ) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        // Frame unchanged since the previous row: reuse the saved results.
        if self.aggregatedupto > self.currentpos {
            for aggno in 0..self.numaggs {
                let wfuncno = self.peragg_wfuncno[aggno] as usize;
                let saved = self.agg_saved[aggno];
                self.write_agg_result(wfuncno, saved);
            }
            return Ok(());
        }

        if self.currentpos == 0 {
            for (aggno, init) in self.trans_init.iter().enumerate() {
                // SAFETY: aggno < the pergroup array's once-allocated length.
                unsafe {
                    self.pergroup_base.as_ptr().add(aggno).write(AggPerGroup {
                        trans_value: init.value,
                        trans_value_is_null: init.isnull,
                        no_trans_value: init.isnull,
                    });
                }
            }
        }

        // Advance until a row past the current peer group (or partition end).
        loop {
            if !self.agg_row_valid {
                if !self.gettupleslot_at(
                    estate,
                    fetch,
                    None,
                    self.aggregatedupto,
                    WhichSlot::AggRow,
                )? {
                    break;
                }
                self.agg_row_valid = true;
            }
            if self.aggregatedupto > self.currentpos {
                let Self {
                    ref mut agg_row_slot,
                    ref mut scan_slot,
                    ref mut ord_eq,
                    tmpcontext,
                    ..
                } = *self;
                if !Self::are_peers(
                    estate,
                    ord_eq.as_deref_mut(),
                    tmpcontext,
                    agg_row_slot,
                    scan_slot,
                )? {
                    // C leaves agg_row_slot holding this row for the next call.
                    break;
                }
            }
            {
                let mut slots =
                    EvalSlots { scan: None, inner: None, outer: Some(&mut self.agg_row_slot) };
                exec_eval_expr(self.evaltrans.as_mut().unwrap(), &mut slots)?;
            }
            estate.reset_expr_context(self.tmpcontext);
            self.aggregatedupto += 1;
            self.agg_row_valid = false;
        }

        // finalize (no finalfn in the live set) + save for frame reuse; all
        // result types byval, so the save is a plain copy.
        for aggno in 0..self.numaggs {
            let wfuncno = self.peragg_wfuncno[aggno] as usize;
            // SAFETY: as the initialize loop above.
            let pg = unsafe { *self.pergroup_base.as_ptr().add(aggno) };
            let result = NullableDatum { value: pg.trans_value, isnull: pg.trans_value_is_null };
            self.agg_saved[aggno] = result;
            self.write_agg_result(wfuncno, result);
        }
        Ok(())
    }

    // initialize_windowaggregate (framed lane).
    fn initialize_windowaggregate(&mut self, aggno: usize) {
        let pa = &mut self.peragg[aggno];
        pa.trans_value = pa.init_value;
        pa.trans_count = 0;
        pa.int_sum = Int8TransState::default();
        pa.result_value = NullableDatum::null();
    }

    fn eval_agg_args(
        &mut self,
        aggno: usize,
        which: WhichSlot,
        out: &mut [NullableDatum],
    ) -> PgResult<()> {
        let Self {
            ref mut peragg,
            ref mut agg_row_slot,
            ref mut temp_slot_1,
            ref mut temp_slot_2,
            ..
        } = *self;
        let slot = match which {
            WhichSlot::AggRow => agg_row_slot,
            WhichSlot::Temp1 => temp_slot_1,
            WhichSlot::Temp2 => temp_slot_2,
        };
        let pa = &mut peragg[aggno];
        for (i, st) in pa.argstates.iter_mut().enumerate() {
            let mut slots = EvalSlots { scan: None, inner: None, outer: Some(&mut *slot) };
            out[i] = exec_eval_expr(st, &mut slots)?;
        }
        Ok(())
    }

    // advance_windowaggregate (nodeWindowAgg.c), byval closed set.
    fn advance_windowaggregate(&mut self, aggno: usize, which: WhichSlot) -> PgResult<()> {
        let nargs = self.peragg[aggno].num_arguments as usize;
        let mut args = [NullableDatum::null(); 4];
        assert!(nargs < 4);
        self.eval_agg_args(aggno, which, &mut args[..nargs])?;
        let pa = &mut self.peragg[aggno];

        if pa.fn_strict {
            for a in &args[..nargs] {
                if a.isnull {
                    return Ok(());
                }
            }
            if pa.trans_count == 0 && pa.trans_value.isnull {
                pa.trans_value = args[0];
                pa.trans_count = 1;
                return Ok(());
            }
            if pa.trans_value.isnull {
                debug_assert!(!pa.has_inverse);
                return Ok(());
            }
        }

        match &mut pa.kernel {
            AggKernel::MovingIntSum { int2 } => {
                let v = if *int2 {
                    args[0].value.as_i16() as i64
                } else {
                    args[0].value.as_i32() as i64
                };
                pa.int_sum.count += 1;
                pa.int_sum.sum += v;
                pa.trans_count += 1;
            }
            AggKernel::Generic { transfn } | AggKernel::MovingByVal { transfn, .. } => {
                let mut fcinfo = LocalFcinfo::<4>::fresh(pa.win_collation);
                fcinfo.nargs = (nargs + 1) as i16;
                fcinfo.args[0] = pa.trans_value;
                fcinfo.args[1..=nargs].copy_from_slice(&args[..nargs]);
                let newval = transfn.invoke(&mut fcinfo)?;
                if fcinfo.isnull && pa.has_inverse {
                    return Err(moving_transfn_returned_null());
                }
                pa.trans_count += 1;
                pa.trans_value = NullableDatum { value: newval, isnull: fcinfo.isnull };
            }
        }
        Ok(())
    }

    // advance_windowaggregate_base: remove the oldest row via the inverse
    // transition; false forces a restart.
    fn advance_windowaggregate_base(&mut self, aggno: usize) -> PgResult<bool> {
        let nargs = self.peragg[aggno].num_arguments as usize;
        let mut args = [NullableDatum::null(); 4];
        assert!(nargs < 4);
        self.eval_agg_args(aggno, WhichSlot::Temp1, &mut args[..nargs])?;

        if self.peragg[aggno].fn_strict {
            for a in &args[..nargs] {
                if a.isnull {
                    return Ok(true);
                }
            }
        }
        debug_assert!(self.peragg[aggno].trans_count > 0);
        if self.peragg[aggno].trans_value.isnull {
            panic!("aggregate transition value is NULL before inverse transition");
        }
        if self.peragg[aggno].trans_count == 1 {
            self.initialize_windowaggregate(aggno);
            return Ok(true);
        }

        let pa = &mut self.peragg[aggno];
        match &mut pa.kernel {
            AggKernel::MovingIntSum { int2 } => {
                let v = if *int2 {
                    args[0].value.as_i16() as i64
                } else {
                    args[0].value.as_i32() as i64
                };
                pa.int_sum.count -= 1;
                pa.int_sum.sum -= v;
                pa.trans_count -= 1;
            }
            AggKernel::MovingByVal { invtransfn, .. } => {
                let mut fcinfo = LocalFcinfo::<4>::fresh(pa.win_collation);
                fcinfo.nargs = (nargs + 1) as i16;
                fcinfo.args[0] = pa.trans_value;
                fcinfo.args[1..=nargs].copy_from_slice(&args[..nargs]);
                let newval = invtransfn.invoke(&mut fcinfo)?;
                if fcinfo.isnull {
                    return Ok(false);
                }
                pa.trans_count -= 1;
                pa.trans_value = NullableDatum { value: newval, isnull: false };
            }
            AggKernel::Generic { .. } => unreachable!("no inverse transition"),
        }
        Ok(true)
    }

    // finalize_windowaggregate: int2int4_sum kernel or bare transValue.
    fn finalize_windowaggregate(&self, aggno: usize) -> NullableDatum {
        let pa = &self.peragg[aggno];
        match &pa.kernel {
            AggKernel::MovingIntSum { .. } => {
                if pa.int_sum.count == 0 {
                    NullableDatum::null()
                } else {
                    NullableDatum::value(Datum::from_i64(pa.int_sum.sum))
                }
            }
            _ => pa.trans_value,
        }
    }

    // eval_windowaggregates (nodeWindowAgg.c), framed lane: full restart /
    // inverse-transition discipline; any exclusion forces per-row restarts.
    fn eval_windowaggregates_framed<F>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        fetch: &mut F,
    ) -> PgResult<()>
    where
        F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    {
        let numaggs = self.numaggs;
        let fo = self.frameOptions;

        self.update_frameheadpos(estate, fetch)?;
        if self.frameheadpos < self.aggregatedbase {
            panic!("window frame head moved backward");
        }

        if self.aggregatedbase == self.frameheadpos
            && fo & (FRAMEOPTION_END_UNBOUNDED_FOLLOWING | FRAMEOPTION_END_CURRENT_ROW) != 0
            && fo & FRAMEOPTION_EXCLUSION == 0
            && self.aggregatedbase <= self.currentpos
            && self.aggregatedupto > self.currentpos
        {
            for aggno in 0..numaggs {
                let wfuncno = self.peragg[aggno].wfuncno as usize;
                let saved = self.peragg[aggno].result_value;
                self.write_agg_result(wfuncno, saved);
            }
            return Ok(());
        }

        let mut numaggs_restart = 0;
        for aggno in 0..numaggs {
            let restart = self.currentpos == 0
                || (self.aggregatedbase != self.frameheadpos && !self.peragg[aggno].has_inverse)
                || fo & FRAMEOPTION_EXCLUSION != 0
                || self.aggregatedupto <= self.frameheadpos;
            self.peragg[aggno].restart = restart;
            if restart {
                numaggs_restart += 1;
            }
        }

        while numaggs_restart < numaggs && self.aggregatedbase < self.frameheadpos {
            if !self.gettupleslot_at(estate, fetch, None, self.aggregatedbase, WhichSlot::Temp1)?
            {
                panic!("could not re-fetch previously fetched frame row");
            }
            for aggno in 0..numaggs {
                if self.peragg[aggno].restart {
                    continue;
                }
                if !self.advance_windowaggregate_base(aggno)? {
                    self.peragg[aggno].restart = true;
                    numaggs_restart += 1;
                }
            }
            estate.reset_expr_context(self.tmpcontext);
            self.aggregatedbase += 1;
            let mcx = estate.es_query_cxt;
            exectuples::exec_clear_tuple(&mut self.temp_slot_1, mcx);
        }

        self.aggregatedbase = self.frameheadpos;
        if self.agg_mark_active && self.buffer.is_some() {
            self.set_agg_mark_position(self.frameheadpos);
        }

        for aggno in 0..numaggs {
            if self.peragg[aggno].restart {
                self.initialize_windowaggregate(aggno);
            } else if !self.peragg[aggno].result_value.isnull {
                self.peragg[aggno].result_value = NullableDatum::null();
            }
        }

        let aggregatedupto_nonrestarted = self.aggregatedupto;
        if numaggs_restart > 0 && self.aggregatedupto != self.frameheadpos {
            self.aggregatedupto = self.frameheadpos;
            self.agg_row_valid = false;
            let mcx = estate.es_query_cxt;
            exectuples::exec_clear_tuple(&mut self.agg_row_slot, mcx);
        }

        loop {
            if !self.agg_row_valid {
                if !self.gettupleslot_at(
                    estate,
                    fetch,
                    None,
                    self.aggregatedupto,
                    WhichSlot::AggRow,
                )? {
                    break;
                }
                self.agg_row_valid = true;
            }
            let ret =
                self.row_is_in_frame(estate, fetch, self.aggregatedupto, WhichSlot::AggRow)?;
            if ret < 0 {
                break;
            }
            if ret > 0 {
                for aggno in 0..numaggs {
                    if !self.peragg[aggno].restart
                        && self.aggregatedupto < aggregatedupto_nonrestarted
                    {
                        continue;
                    }
                    self.advance_windowaggregate(aggno, WhichSlot::AggRow)?;
                }
            }
            estate.reset_expr_context(self.tmpcontext);
            self.aggregatedupto += 1;
            self.agg_row_valid = false;
        }
        debug_assert!(aggregatedupto_nonrestarted <= self.aggregatedupto);

        for aggno in 0..numaggs {
            let result = self.finalize_windowaggregate(aggno);
            self.peragg[aggno].result_value = result;
            let wfuncno = self.peragg[aggno].wfuncno as usize;
            self.write_agg_result(wfuncno, result);
        }
        Ok(())
    }
}

/// `ExecWindowAgg`.
pub fn exec_window_agg<'mcx, F>(
    state: &mut WindowAggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut fetch_outer: F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    if init_small::globals::InterruptPending() {
        postgres_seams::check_for_interrupts::call()?;
    }
    if state.status == WaStatus::Done {
        return Ok(None);
    }
    if state.all_first {
        state.calculate_frame_offsets(estate)?;
    }
    let fetch = &mut fetch_outer;

    // The runCondition or qual may filter tuples: loop until one survives.
    loop {
        if state.next_partition {
            state.begin_partition(estate, fetch)?;
        } else {
            state.currentpos += 1;
            state.framehead_valid = false;
            state.frametail_valid = false;
        }
        state.spool_tuples(estate, fetch, state.currentpos)?;
        if state.partition_spooled && state.currentpos >= state.spooled_rows {
            state.release_partition(estate);
            if state.more_partitions {
                state.begin_partition(estate, fetch)?;
                debug_assert!(state.spooled_rows > 0);
                state.status = WaStatus::Run;
            } else {
                state.status = WaStatus::Done;
                return Ok(None);
            }
        }

        estate.reset_expr_context(state.ps_ExprContext);

        {
            let mcx = estate.es_query_cxt;
            if state.frameOptions
                & (FRAMEOPTION_GROUPS | FRAMEOPTION_EXCLUDE_GROUP | FRAMEOPTION_EXCLUDE_TIES)
                != 0
                && state.currentpos > 0
            {
                {
                    let WindowAggStateData { ref mut temp_slot_2, ref mut scan_slot, .. } =
                        *state;
                    exectuples::exec_copy_slot(temp_slot_2, scan_slot, mcx, mcx)?;
                }
                {
                    let buffer = state.buffer.as_mut().unwrap();
                    buffer.select_read_pointer(0);
                    if !buffer.gettupleslot(true, false, &mut state.scan_slot, mcx)? {
                        panic!("unexpected end of tuplestore");
                    }
                }
                let peers = {
                    let WindowAggStateData {
                        ref mut temp_slot_2,
                        ref mut scan_slot,
                        ref mut ord_eq,
                        tmpcontext,
                        ..
                    } = *state;
                    WindowAggStateData::are_peers(
                        estate,
                        ord_eq.as_deref_mut(),
                        tmpcontext,
                        temp_slot_2,
                        scan_slot,
                    )?
                };
                if !peers {
                    state.currentgroup += 1;
                    state.groupheadpos = state.currentpos;
                    state.grouptail_valid = false;
                }
                exectuples::exec_clear_tuple(&mut state.temp_slot_2, mcx);
            } else {
                let buffer = state.buffer.as_mut().unwrap();
                buffer.select_read_pointer(0);
                if !buffer.gettupleslot(true, false, &mut state.scan_slot, mcx)? {
                    panic!("unexpected end of tuplestore");
                }
            }
        }

        if state.status == WaStatus::Run {
            for i in 0..state.perfunc.len() {
                if !matches!(state.perfunc[i].kind, WfKind::PlainAgg { .. }) {
                    state.eval_windowfunction(estate, fetch, i)?;
                }
            }
            if state.numaggs > 0 {
                if state.frameOptions == FRAMEOPTION_DEFAULTS {
                    state.eval_windowaggregates_default(estate, fetch)?;
                } else {
                    state.eval_windowaggregates_framed(estate, fetch)?;
                }
            }
        }
        // C force-updates framehead/frametail/grouptail pointers and trims
        // the tuplestore here; trim is unported, so the pointers stay lazy.

        {
            let mcx = estate.es_query_cxt;
            let result_slot = estate.slot_mut(state.ps_ResultTupleSlot);
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut state.scan_slot) };
            exec_project(&mut state.proj, &mut slots, result_slot, mcx)?;
        }

        if state.status == WaStatus::Run {
            let result_id = state.ps_ResultTupleSlot;
            let rc_pass = {
                let WindowAggStateData { ref mut runcondition, ref mut scan_slot, .. } = *state;
                let result_slot = estate.slot_mut(result_id);
                let mut slots = EvalSlots {
                    scan: Some(result_slot),
                    inner: None,
                    outer: Some(scan_slot),
                };
                exec_qual(runcondition.as_deref_mut(), &mut slots)?
            };
            if !rc_pass {
                if state.use_pass_through {
                    // NULLify stale (possibly by-ref) results; the planner
                    // guarantees strict runcondition quals, so the top
                    // window filters these NULLs out.
                    let numfuncs = state.perfunc.len();
                    for wfuncno in 0..numfuncs {
                        state.write_agg_result(wfuncno, NullableDatum::null());
                    }
                    if state.top_window {
                        state.status = WaStatus::PassThroughStrict;
                        continue;
                    }
                    state.status = WaStatus::PassThrough;
                } else {
                    state.status = WaStatus::Done;
                    return Ok(None);
                }
            }
            let qual_pass = {
                let WindowAggStateData { ref mut qual, ref mut scan_slot, .. } = *state;
                let result_slot = estate.slot_mut(result_id);
                let mut slots = EvalSlots {
                    scan: Some(result_slot),
                    inner: None,
                    outer: Some(scan_slot),
                };
                exec_qual(qual.as_deref_mut(), &mut slots)?
            };
            if !qual_pass {
                continue;
            }
            return Ok(Some(state.ps_ResultTupleSlot));
        } else if !state.top_window {
            return Ok(Some(state.ps_ResultTupleSlot));
        }
    }
}

/// `ExecEndWindowAgg` node-local half; the caller ends the outer child.
pub fn exec_end_window_agg(node: &mut WindowAggStateData<'_>) {
    if let Some(buffer) = node.buffer.take() {
        buffer.end();
    }
    node.part_eq = None;
    node.ord_eq = None;
    node.evaltrans = None;
    node.start_offset_state = None;
    node.end_offset_state = None;
    node.start_in_range = None;
    node.end_in_range = None;
    node.runcondition = None;
    node.qual = None;
    node.proj.release_frames();
    node.ps_ResultTupleDesc = None;
    for pf in node.perfunc.iter_mut() {
        pf.argstates.clear();
    }
    for pa in node.peragg.iter_mut() {
        pa.argstates.clear();
        match &mut pa.kernel {
            AggKernel::Generic { transfn } => transfn.fn_extra = None,
            AggKernel::MovingByVal { transfn, invtransfn } => {
                transfn.fn_extra = None;
                invtransfn.fn_extra = None;
            }
            AggKernel::MovingIntSum { .. } => {}
        }
    }
    for slot in [
        &mut node.scan_slot,
        &mut node.first_part_slot,
        &mut node.agg_row_slot,
        &mut node.temp_slot_1,
        &mut node.temp_slot_2,
        &mut node.framehead_slot,
        &mut node.frametail_slot,
    ] {
        slot.base_mut().tts_tupleDescriptor = None;
    }
}

/// show_windowagg_info's tuplestore read; None before the buffer exists.
pub fn storage_stats(
    node: &mut WindowAggStateData<'_>,
) -> Option<types_core::instrument::TuplestoreInstrumentation> {
    node.buffer.as_mut().map(Tuplestore::get_stats)
}

/// `ExecReScanWindowAgg`; the caller (execami) rescans the outer child.
pub fn exec_rescan_window_agg<'mcx>(
    node: &mut WindowAggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    node.status = WaStatus::Run;
    node.all_first = true;
    node.release_partition(estate);
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(&mut node.first_part_slot, mcx);
    node.first_part_valid = false;
    exectuples::exec_clear_tuple(&mut node.temp_slot_1, mcx);
    exectuples::exec_clear_tuple(&mut node.temp_slot_2, mcx);
    exectuples::exec_clear_tuple(&mut node.framehead_slot, mcx);
    exectuples::exec_clear_tuple(&mut node.frametail_slot, mcx);
    let numfuncs = node.perfunc.len();
    let ecxt = estate.ecxt_mut(node.ps_ExprContext);
    for i in 0..numfuncs {
        ecxt.ecxt_aggvalues[i] = Datum::null();
        ecxt.ecxt_aggnulls[i] = false;
    }
}

mcx::forget_safe_nodrop!(WfKind, Int8TransState, WaStatus);

// Exempt: all released in exec_end_window_agg (argstates cleared, offset/
// in_range/eq ExprStates and FmgrInfos taken, buffer ended, slot descs
// cleared).
mcx::forget_safe_struct!(
    PerFuncData<'_> { kind, wfuncno, readptr, seekpos, markpos, rank, ntile,
        rows_per_bucket, boundary, remainder, arg1_stable; argstates },
    PerAggData<'_> { wfuncno, num_arguments, win_collation, fn_strict,
        has_inverse, init_value, trans_value, trans_count, int_sum,
        result_value, restart; argstates, kernel },
    WindowAggStateData<'_> { plan, frameOptions, ps_ExprContext, tmpcontext,
        ps_ResultTupleSlot, first_part_valid, agg_row_valid, perfunc, peragg,
        trans_init, _pergroup, pergroup_base, peragg_wfuncno, agg_saved,
        agg_readptr, agg_seekpos, agg_markpos, agg_mark_active,
        agg_values_base, agg_nulls_base, numaggs, currentpos, frameheadpos,
        frametailpos, framehead_valid, frametail_valid, framehead_ptr,
        frametail_ptr, currentgroup, frameheadgroup, frametailgroup,
        groupheadpos, grouptailpos, grouptail_valid, grouptail_ptr,
        aggregatedbase, aggregatedupto, spooled_rows,
        start_offset_value, end_offset_value, start_offset_typlen,
        start_offset_byval, end_offset_typlen, end_offset_byval,
        use_pass_through, top_window, all_first,
        partition_spooled, more_partitions, next_partition, status;
        ps_ResultTupleDesc, proj, part_eq, ord_eq, buffer, scan_slot,
        first_part_slot, agg_row_slot, temp_slot_1, temp_slot_2,
        framehead_slot, frametail_slot, evaltrans, start_offset_state,
        end_offset_state, start_in_range, end_in_range, runcondition, qual },
);
