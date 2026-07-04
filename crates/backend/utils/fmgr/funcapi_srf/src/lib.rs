use std::any::Any;

use datum::Datum;
use fmgr::{ExprDoneCond, FmgrInfo, FunctionCallInfoBaseData, SetFunctionReturnMode};
use nodes::NodeTag;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};

const _: () = assert!(fmgr::RETURN_SET_INFO_TAG == NodeTag::T_ReturnSetInfo as u32);

// C FuncCallContext minus attinmeta/tuple_desc/multi_call_memory_ctx: the
// fn_extra Box owns the cross-call data (fn_mcxt's role).
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    pub user_fctx: Option<Box<dyn Any>>,
}

impl core::fmt::Debug for FuncCallContext {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FuncCallContext")
            .field("call_cntr", &self.call_cntr)
            .field("max_calls", &self.max_calls)
            .field("has_user_fctx", &self.user_fctx.is_some())
            .finish()
    }
}

#[cold]
pub fn srf_context_error() -> Box<PgError> {
    Box::new(
        PgError::error("set-valued function called in context that cannot accept a set")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

fn resultinfo_is_rsinfo(fcinfo: &FunctionCallInfoBaseData) -> bool {
    match fcinfo.resultinfo {
        // SAFETY: fmNodePtr contract — a live node leading with its NodeTag.
        Some(p) => (unsafe { p.as_ref().tag }) == NodeTag::T_ReturnSetInfo as u32,
        None => false,
    }
}

pub fn init_MultiFuncCall<'a>(
    flinfo: &'a mut FmgrInfo,
    fcinfo: &FunctionCallInfoBaseData,
) -> PgResult<&'a mut FuncCallContext> {
    if !resultinfo_is_rsinfo(fcinfo) {
        return Err(srf_context_error());
    }

    if flinfo.has_fn_extra() {
        return Err(Box::new(PgError::error(
            "init_MultiFuncCall cannot be called more than once",
        )));
    }

    // C's shutdown_MultiFuncCall (delete the multi-call context on early
    // exit) is subsumed: the fn_extra Box dies with the FmgrInfo carrier.
    flinfo.set_fn_extra(FuncCallContext { call_cntr: 0, max_calls: 0, user_fctx: None });
    Ok(flinfo.fn_extra_mut::<FuncCallContext>().unwrap())
}

pub fn per_MultiFuncCall(flinfo: &mut FmgrInfo) -> &mut FuncCallContext {
    flinfo
        .fn_extra_mut::<FuncCallContext>()
        .expect("per_MultiFuncCall: no FuncCallContext on fn_extra")
}

pub fn end_MultiFuncCall(flinfo: &mut FmgrInfo) {
    flinfo.fn_extra = None;
}

#[cold]
pub fn no_rsinfo() -> ! {
    panic!("SRF_RETURN: fcinfo.resultinfo is not a ReturnSetInfo");
}

pub fn srf_return_next(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut FunctionCallInfoBaseData,
    result: Datum,
) -> Datum {
    per_MultiFuncCall(flinfo).call_cntr += 1;
    match fcinfo.rsinfo_mut() {
        Some(rsi) => rsi.isDone = ExprDoneCond::ExprMultipleResult,
        None => no_rsinfo(),
    }
    fcinfo.isnull = false;
    result
}

pub fn srf_return_next_null(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> Datum {
    per_MultiFuncCall(flinfo).call_cntr += 1;
    match fcinfo.rsinfo_mut() {
        Some(rsi) => rsi.isDone = ExprDoneCond::ExprMultipleResult,
        None => no_rsinfo(),
    }
    fcinfo.return_null()
}

pub fn srf_return_done(flinfo: &mut FmgrInfo, fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    end_MultiFuncCall(flinfo);
    match fcinfo.rsinfo_mut() {
        Some(rsi) => rsi.isDone = ExprDoneCond::ExprEndResult,
        None => no_rsinfo(),
    }
    fcinfo.return_null()
}
