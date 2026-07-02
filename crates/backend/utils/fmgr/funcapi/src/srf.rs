use std::any::Any;

use fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use nodes::NodeTag;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};

// funcapi.h
pub const MAT_SRF_USE_EXPECTED_DESC: u32 = 0x01;
pub const MAT_SRF_BLESS: u32 = 0x02;

// C FuncCallContext minus attinmeta/tuple_desc (they arrive with the executor
// SRF leg) and multi_call_memory_ctx: the fn_extra Box owns the cross-call
// data, which is fn_mcxt's role.
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    pub user_fctx: Option<Box<dyn Any>>,
}

#[cold]
fn srf_context_error() -> Box<PgError> {
    Box::new(
        PgError::error("set-valued function called in context that cannot accept a set")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

fn resultinfo_is_rsinfo(fcinfo: &FunctionCallInfoBaseData) -> bool {
    match fcinfo.resultinfo {
        // SAFETY: fmNodePtr contract — resultinfo points at a live node
        // leading with its NodeTag.
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

    // C registers shutdown_MultiFuncCall on rsi->econtext to delete the
    // multi-call context on early exit; here the fn_extra Box dies with the
    // FmgrInfo carrier, which is that callback's entire effect.
    flinfo.set_fn_extra(FuncCallContext { call_cntr: 0, max_calls: 0, user_fctx: None });
    Ok(flinfo.fn_extra_mut::<FuncCallContext>().unwrap())
}

pub fn per_MultiFuncCall(flinfo: &mut FmgrInfo) -> &mut FuncCallContext {
    flinfo
        .fn_extra_mut::<FuncCallContext>()
        .expect("per_MultiFuncCall: no FuncCallContext on fn_extra")
}

pub fn end_MultiFuncCall(flinfo: &mut FmgrInfo) {
    // shutdown_MultiFuncCall: unbind from flinfo and delete the multi-call
    // context — one assignment under Box ownership.
    flinfo.fn_extra = None;
}

pub fn InitMaterializedSRF(_fcinfo: &mut FunctionCallInfoBaseData, _flags: u32) -> ! {
    panic!(
        "funcapi InitMaterializedSRF: ReturnSetInfo/tuplestore/per-query-context \
         vocabulary not ported (executor and portal legs absent)"
    );
}
