use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_nodes::NodeTag;

// C acts only on WFuncMonotonic/OptimizeWindowClause requests (neither type
// exists here; optimize_window_clauses unported); NULL for everything else.
fn window_support(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
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
        other => panic!(
            "window prosupport: request {other:?} unported (optimize_window_clauses lane)"
        ),
    }
}

pub fn fc_window_row_number_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    window_support(fcinfo)
}

pub fn fc_window_rank_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    window_support(fcinfo)
}

pub fn fc_window_dense_rank_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    window_support(fcinfo)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, strict: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict, retset: false, func }
}

pub fn fc_int8inc_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    window_support(fcinfo)
}

pub const WINDOWFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(6233, "window_row_number_support", 1, true, fc_window_row_number_support),
    b(6234, "window_rank_support", 1, true, fc_window_rank_support),
    b(6235, "window_dense_rank_support", 1, true, fc_window_dense_rank_support),
    b(6236, "int8inc_support", 1, true, fc_int8inc_support),
];
