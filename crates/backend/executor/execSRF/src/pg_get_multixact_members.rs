//! `pg_get_multixact_members(xid)` (OID 3819) registered as an executor-frame
//! materialize-mode set-returning function.
//!
//! `multixact.c`'s `pg_get_multixact_members` is a value-per-call SRF emitting
//! one `(xid xid, mode text)` row per member of the given MultiXactId (its
//! `members[]` array, each with a member xid and a lock-mode status string). The
//! member-resolution core (the `GetMultiXactIdMembers` fetch + the
//! `mxstatus_to_string` mapping, with the `MultiXactId < FirstMultiXactId`
//! validity guard) is ported in
//! [`multixact::pg_get_multixact_members`], which hands
//! back a `Vec<(TransactionId, &'static str)>`.
//!
//! Here that core is driven over the executor frame in materialize mode: the
//! member list is resolved once and the whole tuplestore filled.
//! `InitMaterializedSRF` with `MAT_SRF_USE_EXPECTED_DESC` takes the executor's
//! already-resolved `(xid, text)` descriptor (skipping the catalog
//! `get_call_result_type`); the members are appended via
//! `materialized_srf_putvalues`. Registered from
//! [`register_pg_get_multixact_members`] (called by `init_seams`); it bypasses
//! the by-OID builtin registry whose tag-only `resultinfo` cannot carry the live
//! `ReturnSetInfo` (the WONTFIX dual-home).

use mcx::Mcx;
use types_core::Oid;
use nodes::fmgr::FunctionCallInfoBaseData;
use nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_error::PgResult;
use types_tuple::heaptuple::Datum;

use funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};

use crate::register_srf;

/// `pg_get_multixact_members(xid)` (OID 3819).
const PG_GET_MULTIXACT_MEMBERS: Oid = 3819;

/// Register `pg_get_multixact_members` in the executor-frame SRF table.
pub(crate) fn register_pg_get_multixact_members() {
    register_srf(PG_GET_MULTIXACT_MEMBERS, pg_get_multixact_members);
}

/// `pg_get_multixact_members(PG_FUNCTION_ARGS)` (multixact.c) over the executor
/// frame.
fn pg_get_multixact_members<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_get_multixact_members: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: mxid = PG_GETARG_TRANSACTIONID(0). The MultiXactId is a `xid`-typed
    // by-value uint32 argument (`proisstrict => 't'`, so never NULL here).
    let mxid = fcinfo.args[0].value.as_transaction_id();

    // The member-resolution core (validity guard + GetMultiXactIdMembers +
    // mxstatus_to_string). An invalid MultiXactId is a hard `ereport(ERROR)`.
    let members = multixact::pg_get_multixact_members(mxid)?;

    // C: get_call_result_type → the `(xid, text)` row type. Take the executor's
    // already-resolved descriptor.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_get_multixact_members: InitMaterializedSRF establishes fcinfo->resultinfo");

    for (xid, status) in members {
        // values[0] = member xid (`xid`, by-value uint32); C builds it via
        // psprintf("%u")→xidin, equivalently TransactionIdGetDatum.
        // values[1] = mode text (`mxstatus_to_string`→CStringGetTextDatum).
        let values = [
            Datum::from_transaction_id(xid),
            varlena_seams::cstring_to_text_v::call(mcx, status)?,
        ];
        let nulls = [false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}
