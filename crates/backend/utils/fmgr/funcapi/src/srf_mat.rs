// funcapi.h
pub const MAT_SRF_USE_EXPECTED_DESC: u32 = 0x01;
pub const MAT_SRF_BLESS: u32 = 0x02;

use datum::Datum;
use fmgr::{
    FmgrInfo, FunctionCallInfoBaseData, SetFunctionReturnMode, SFRM_Materialize,
    SFRM_Materialize_Random,
};
use mcx::Mcx;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_tuple::TupleDescData;
use funcapi_srf::{no_rsinfo, srf_context_error};

#[track_caller]
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
