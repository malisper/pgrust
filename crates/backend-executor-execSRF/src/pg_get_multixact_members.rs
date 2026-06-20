//! `pg_get_multixact_members(xid)` (OID 3819) registered as an executor-frame
//! materialize-mode set-returning function.
//!
//! `multixact.c`'s `pg_get_multixact_members` is a value-per-call SRF emitting
//! one `(xid xid, mode text)` row per member of the given MultiXactId (its
//! `members[]` array, each with a member xid and a lock-mode status string). The
//! member-resolution core (the `GetMultiXactIdMembers` fetch + the
//! `mxstatus_to_string` mapping, with the `MultiXactId < FirstMultiXactId`
//! validity guard) is ported in
//! [`backend_access_transam_multixact::pg_get_multixact_members`], which hands
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
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_fmgr_funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};

use crate::register_srf;

/// `pg_get_multixact_members(xid)` (OID 3819).
const PG_GET_MULTIXACT_MEMBERS: Oid = 3819;

/// Register `pg_get_multixact_members` in the executor-frame SRF table.
pub(crate) fn register_pg_get_multixact_members() {
    register_srf(PG_GET_MULTIXACT_MEMBERS, pg_get_multixact_members);
}

/// `pg_get_multixact_members(PG_FUNCTION_ARGS)` (multixact.c) over the executor
/// frame.
fn pg_get_multixact_members<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_get_multixact_members: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: mxid = PG_GETARG_TRANSACTIONID(0). The MultiXactId is a `xid`-typed
    // by-value uint32 argument (`proisstrict => 't'`, so never NULL here).
    let mxid = fcinfo.args[0].value.as_transaction_id();

    // The member-resolution core (validity guard + GetMultiXactIdMembers +
    // mxstatus_to_string). An invalid MultiXactId is a hard `ereport(ERROR)`.
    let members = backend_access_transam_multixact::pg_get_multixact_members(mxid)
        .unwrap_or_else(|e| std::panic::panic_any(e));

    // C: get_call_result_type → the `(xid, text)` row type. Take the executor's
    // already-resolved descriptor.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)
        .unwrap_or_else(|e| std::panic::panic_any(e));

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
            backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, status)
                .unwrap_or_else(|e| std::panic::panic_any(e)),
        ];
        let nulls = [false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)
            .unwrap_or_else(|e| std::panic::panic_any(e));
    }

    fcinfo.isnull = true;
    Datum::null()
}
