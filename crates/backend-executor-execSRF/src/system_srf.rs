//! Materialize-mode system / catalog SRFs whose worker body already takes the
//! owned `(mcx, fcinfo)` signature and drives `InitMaterializedSRF` +
//! `materialized_srf_putvalues` internally.
//!
//! For these the executor-frame registration is a thin one-line adapter (exactly
//! like the `json[b]_each` materialize family in [`crate::json_each`]): the body
//! runs the whole result into `rsinfo->setResult` (the materialize tuplestore the
//! executor frame carries on its live `ReturnSetInfo`) and returns SQL NULL; the
//! executor (`ExecMakeTableFunctionResult`) then drains the tuplestore. These are
//! registered here — NOT in the by-OID fmgr-core builtin registry — because that
//! registry's `types_fmgr::PGFunction` frame's `resultinfo` is tag-only (the
//! WONTFIX dual-home) and so cannot carry the live `ReturnSetInfo` the worker's
//! `InitMaterializedSRF` writes into.
//!
//! - `pg_options_to_table(text[])` (OID 2289): foreign.c's option-array → set of
//!   `(option_name, option_value)` rows.
//! - `pg_prepared_statement()` (OID 2510): prepare.c's prepared-statement catalog
//!   scan → set of `(name, statement, prepare_time, parameter_types,
//!   result_types, from_sql, generic_plans, custom_plans)` rows.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::register_srf;

/// `pg_options_to_table(text[])` (OID 2289).
const PG_OPTIONS_TO_TABLE: Oid = 2289;
/// `pg_prepared_statement()` (OID 2510).
const PG_PREPARED_STATEMENT: Oid = 2510;

/// Register the materialize-mode system SRFs in the executor-frame SRF table.
pub(crate) fn register_system_srfs() {
    register_srf(PG_OPTIONS_TO_TABLE, pg_options_to_table);
    register_srf(PG_PREPARED_STATEMENT, pg_prepared_statement);
}

/// The per-query memory context the SRF caller threads onto the executor frame
/// (`fcinfo->fn_mcxt`, the materialize tuplestore + descriptor arena).
fn srf_mcx<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> Mcx<'mcx> {
    fcinfo
        .fn_mcxt
        .expect("system SRF: fn_mcxt set by ExecMakeTableFunctionResult")
}

/// `pg_options_to_table(PG_FUNCTION_ARGS)` (foreign.c) over the executor frame.
fn pg_options_to_table<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    backend_foreign_foreign::pg_options_to_table(mcx, fcinfo)
}

/// `pg_prepared_statement(PG_FUNCTION_ARGS)` (prepare.c) over the executor frame.
fn pg_prepared_statement<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    backend_commands_prepare::pg_prepared_statement(mcx, fcinfo)
}
