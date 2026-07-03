use std::any::Any;

use datum::Datum;
use fmgr::{
    ExprDoneCond, FmgrInfo, FunctionCallInfoBaseData, SetFunctionReturnMode, SFRM_Materialize,
    SFRM_Materialize_Random,
};
use mcx::Mcx;
use nodes::NodeTag;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_tuple::TupleDescData;

const _: () = assert!(fmgr::RETURN_SET_INFO_TAG == NodeTag::T_ReturnSetInfo as u32);

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

#[cold]
fn no_rsinfo() -> ! {
    panic!("SRF_RETURN: fcinfo.resultinfo is not a ReturnSetInfo");
}

// SRF_RETURN_NEXT minus the macro's `return`: bump call_cntr, flag
// ExprMultipleResult, hand back the row datum.
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

// SRF_RETURN_NEXT with a SQL NULL row value.
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

// SRF_RETURN_DONE: teardown + ExprEndResult + a null result.
pub fn srf_return_done(flinfo: &mut FmgrInfo, fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    end_MultiFuncCall(flinfo);
    match fcinfo.rsinfo_mut() {
        Some(rsi) => rsi.isDone = ExprDoneCond::ExprEndResult,
        None => no_rsinfo(),
    }
    fcinfo.return_null()
}

#[cold]
fn materialize_not_allowed() -> Box<PgError> {
    Box::new(
        PgError::error("materialize mode required, but it is not allowed in this context")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

pub struct MaterializedSRF<'m> {
    pub tupdesc: TupleDescData<'m>,
    store: tuplestore::Tuplestore,
}

impl core::fmt::Debug for MaterializedSRF<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MaterializedSRF")
            .field("natts", &self.tupdesc.natts)
            .field("tuples", &self.store.tuple_count())
            .finish()
    }
}

impl<'m> MaterializedSRF<'m> {
    pub fn putvalues(&mut self, values: &[Datum], isnull: &[bool]) -> PgResult<()> {
        self.store.putvalues(&self.tupdesc, values, isnull)
    }

    // C materialize SRFs `return (Datum) 0` and the executor's Materialize
    // arm ignores the scalar and isnull; C sets returnMode/setResult inside
    // InitMaterializedSRF, here they land when the rows are complete.
    pub fn finish(self, fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
        match fcinfo.rsinfo_mut() {
            Some(rsi) => {
                rsi.returnMode = SetFunctionReturnMode::Materialize;
                rsi.setResult = Some(Box::new(self.store));
            }
            None => no_rsinfo(),
        }
        Datum::from_usize(0)
    }
}

pub fn InitMaterializedSRF<'m>(
    mcx: Mcx<'m>,
    flinfo: &mut FmgrInfo,
    fcinfo: &mut FunctionCallInfoBaseData,
    flags: u32,
) -> PgResult<MaterializedSRF<'m>> {
    let Some(rsinfo) = fcinfo.rsinfo_mut() else {
        return Err(srf_context_error());
    };
    let allowed_modes = rsinfo.allowedModes;
    let expected_desc = rsinfo.expectedDesc;
    if allowed_modes & SFRM_Materialize == 0
        || (flags & MAT_SRF_USE_EXPECTED_DESC != 0 && expected_desc.is_none())
    {
        return Err(materialize_not_allowed());
    }

    // SAFETY: expectedDesc contract — the executor armed it with the scan
    // tupdesc, live for the duration of this call.
    let expected = expected_desc.map(|p| unsafe { p.cast::<TupleDescData<'_>>().as_ref() });
    let tupdesc = if flags & MAT_SRF_USE_EXPECTED_DESC != 0 {
        tupdesc::CreateTupleDescCopy(mcx, expected.expect("checked above"))?
    } else {
        let resolved = crate::get_call_result_type(mcx, flinfo, expected)?;
        if resolved.class != crate::TypeFuncClass::Composite {
            return Err(Box::new(PgError::error("return type must be a row type")));
        }
        resolved.result_tuple_desc.expect("composite result carries a tupdesc")
    };
    // MAT_SRF_BLESS is a no-op: C's BlessTupleDesc registers a record typmod
    // for consumers decoding anonymous record datums; here rows only flow
    // through the tuplestore and are read via the scan tupdesc.

    let random_access = allowed_modes & SFRM_Materialize_Random != 0;
    let store = tuplestore::Tuplestore::begin_heap(
        random_access,
        false,
        init_small::globals::work_mem(),
    );
    Ok(MaterializedSRF { tupdesc, store })
}
