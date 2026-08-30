use std::any::Any;

use datum::Datum;
use fmgr::{
    ExprDoneCond, FmgrInfo, FnExtra, FunctionCallInfoBaseData, SetFunctionReturnMode,
};
use nodes::NodeTag;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};

const _: () = assert!(fmgr::RETURN_SET_INFO_TAG == NodeTag::T_ReturnSetInfo as u32);

// C FuncCallContext minus attinmeta/tuple_desc/multi_call_memory_ctx: the
// fn_extra carrier owns the cross-call data (fn_mcxt's role).
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    // C's `void *user_fctx`, carried the way FmgrInfo::fn_extra already
    // carries its memo: a thin pointer whose pointee leads with
    // (TypeId, dropper), so a per-call read is a pointer cast — a
    // debug-only TypeId compare, nothing in release.
    //
    // This was `Option<Box<dyn Any>>`, which put a vtable load + an indirect
    // `type_id()` call + a 128-bit TypeId compare on the per-ROW path of
    // every value-per-call SRF (`generate_series`, `unnest`, `regexp_matches`,
    // …) against C's single pointer read. That is the same cost the fmgr
    // layer refuted for `fn_extra` itself and for the same reason; see the
    // FnExtra doc in types/fmgr/src/fcinfo.rs. Read it through
    // [`FuncCallContext::user_fctx_mut`] / [`user_fctx_ref`], which name the
    // expected type in their panic; write it with [`set_user_fctx`].
    pub user_fctx: Option<FnExtra>,
}

impl FuncCallContext {
    /// Install the per-set cross-call state (C's
    /// `funcctx->user_fctx = palloc(...)`, in the multi-call context).
    #[inline]
    pub fn set_user_fctx<T: Any>(&mut self, state: T) {
        self.user_fctx = Some(FnExtra::new(state));
    }

    /// The installed state (C's `fctx = (T *) funcctx->user_fctx`). Panics
    /// when the first call did not install it — a wiring bug, exactly as a
    /// NULL deref would be in C; the message names the expected type.
    #[inline]
    pub fn user_fctx_mut<T: Any>(&mut self) -> &mut T {
        match self.user_fctx.as_mut() {
            Some(x) => x.downcast_mut::<T>(),
            None => no_user_fctx::<T>(),
        }
    }

    /// Shared-borrow [`user_fctx_mut`].
    #[inline]
    pub fn user_fctx_ref<T: Any>(&self) -> &T {
        match self.user_fctx.as_ref() {
            Some(x) => x.downcast_ref::<T>(),
            None => no_user_fctx::<T>(),
        }
    }
}

#[cold]
#[inline(never)]
fn no_user_fctx<T>() -> ! {
    panic!(
        "SRF user_fctx: {} was not installed on the first call",
        core::any::type_name::<T>()
    )
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
