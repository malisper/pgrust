use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_nodes::primnodes::SupportRequestOptimizeWindowClause;
use types_nodes::rawnodes::{
    FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_NONDEFAULT, FRAMEOPTION_ROWS,
    FRAMEOPTION_START_UNBOUNDED_PRECEDING,
};
use types_nodes::NodeTag;

// C acts on WFuncMonotonic (runCondition lane, unported: loud) and
// OptimizeWindowClause requests; NULL for everything else. All six window
// prosupports rewrite to ROWS UNBOUNDED PRECEDING..CURRENT ROW.
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
        other => panic!(
            "window prosupport: request {other:?} unported (runCondition lane)"
        ),
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
