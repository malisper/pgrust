use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_nodes::primnodes::{
    SupportRequestOptimizeWindowClause, SupportRequestWFuncMonotonic, MONOTONICFUNC_BOTH,
    MONOTONICFUNC_DECREASING, MONOTONICFUNC_INCREASING, MONOTONICFUNC_NONE,
};
use types_nodes::rawnodes::{
    FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_END_UNBOUNDED_FOLLOWING,
    FRAMEOPTION_EXCLUDE_CURRENT_ROW, FRAMEOPTION_EXCLUSION, FRAMEOPTION_NONDEFAULT,
    FRAMEOPTION_RANGE, FRAMEOPTION_ROWS, FRAMEOPTION_START_UNBOUNDED_PRECEDING,
};

// pg_proc F_COUNT_ — the zero-argument count(*).
const F_COUNT_STAR: Oid = 2803;
use types_nodes::NodeTag;

// C acts on WFuncMonotonic and OptimizeWindowClause requests; NULL for
// everything else. All six window prosupports are monotonically increasing
// and rewrite the frame to ROWS UNBOUNDED PRECEDING..CURRENT ROW; int8inc's
// monotonicity depends on the window clause (count(*) over frame bounds).
fn window_support(fcinfo: &mut Fcinfo, optimize_frame: bool) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let p = a.value.as_usize() as *const NodeTag;
    // SAFETY: prosupport contract — arg points at a live tag-first node.
    let tag = unsafe { *p };
    match tag {
        NodeTag::T_SupportRequestSimplify
        | NodeTag::T_SupportRequestCost
        | NodeTag::T_SupportRequestRows
        | NodeTag::T_SupportRequestSelectivity
        | NodeTag::T_SupportRequestIndexCondition => Ok(Datum::from_usize(0)),
        NodeTag::T_SupportRequestWFuncMonotonic => {
            let req = a.value.as_usize() as *mut SupportRequestWFuncMonotonic;
            // SAFETY: tag checked; caller owns the request node.
            unsafe {
                if optimize_frame {
                    (*req).monotonic = MONOTONICFUNC_INCREASING;
                } else {
                    let mut monotonic = MONOTONICFUNC_NONE;
                    // EXCLUDE can drop rows previously counted for earlier
                    // rows, breaking monotonicity; the only guaranteed case
                    // is EXCLUDE CURRENT ROW + COUNT(*) with no FILTER
                    // (C cf184ec).
                    if (*req).frame_options & FRAMEOPTION_EXCLUSION != 0
                        && ((*req).frame_options & FRAMEOPTION_EXCLUDE_CURRENT_ROW == 0
                            || (*req).winfnoid != F_COUNT_STAR
                            || (*req).agg_has_filter)
                    {
                        (*req).monotonic = MONOTONICFUNC_NONE;
                        return Ok(a.value);
                    }
                    // No ORDER BY and RANGE mode means all rows are peers.
                    if (*req).order_clause_empty && (*req).frame_options & FRAMEOPTION_RANGE != 0 {
                        monotonic = MONOTONICFUNC_BOTH;
                    } else {
                        if (*req).frame_options & FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0 {
                            monotonic |= MONOTONICFUNC_INCREASING;
                        }
                        if (*req).frame_options & FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
                            monotonic |= MONOTONICFUNC_DECREASING;
                        }
                    }
                    (*req).monotonic = monotonic;
                }
            }
            Ok(a.value)
        }
        NodeTag::T_SupportRequestOptimizeWindowClause => {
            if !optimize_frame {
                return Ok(Datum::from_usize(0));
            }
            let req = a.value.as_usize() as *mut SupportRequestOptimizeWindowClause;
            // SAFETY: tag checked; caller owns the request node.
            unsafe {
                (*req).frame_options = FRAMEOPTION_NONDEFAULT
                    | FRAMEOPTION_ROWS
                    | FRAMEOPTION_START_UNBOUNDED_PRECEDING
                    | FRAMEOPTION_END_CURRENT_ROW;
            }
            Ok(a.value)
        }
        other => panic!("window prosupport: request {other:?} unported"),
    }
}

pub fn fc_window_row_number_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    window_support(fcinfo, true)
}

pub fn fc_window_rank_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    window_support(fcinfo, true)
}

pub fn fc_window_dense_rank_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    window_support(fcinfo, true)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, strict: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict, retset: false, func }
}

// int8inc_support (int8.c) handles only WFuncMonotonic; NULL otherwise.
pub fn fc_int8inc_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    window_support(fcinfo, false)
}

pub fn fc_window_percent_rank_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    window_support(fcinfo, true)
}

pub fn fc_window_cume_dist_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    window_support(fcinfo, true)
}

pub fn fc_window_ntile_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    window_support(fcinfo, true)
}

pub const WINDOWFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(6233, "window_row_number_support", 1, true, fc_window_row_number_support),
    b(6234, "window_rank_support", 1, true, fc_window_rank_support),
    b(6235, "window_dense_rank_support", 1, true, fc_window_dense_rank_support),
    b(6236, "int8inc_support", 1, true, fc_int8inc_support),
    b(6306, "window_percent_rank_support", 1, true, fc_window_percent_rank_support),
    b(6307, "window_cume_dist_support", 1, true, fc_window_cume_dist_support),
    b(6308, "window_ntile_support", 1, true, fc_window_ntile_support),
];
