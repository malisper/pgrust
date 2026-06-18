//! PostgreSQL 18.3 `src/backend/utils/fmgr/funcapi.c` — utility and convenience
//! functions for fmgr functions that return sets and/or composite types, or
//! deal with VARIADIC inputs.
//!
//! Truth source: `postgres-18.3/src/backend/utils/fmgr/funcapi.c`.
//!
//! The C file (~3204 LOC) is split into the families below, re-derived from the
//! C source. The result-type resolution, polymorphic pseudo-type resolution,
//! `pg_proc`-row projection, and descriptor/VARIADIC families have real bodies
//! ported 1:1 against the C, as does the materialize-mode SRF path
//! (`InitMaterializedSRF`).
//!
//! The **multi-call (value-per-call) SRF protocol** is implemented and wired
//! (#349): the fmgr call frame now carries a typed `FuncCallContext` via the
//! `fn_extra` / `fn_mcxt` channels on the owned `FunctionCallInfoBaseData`, and
//! the `ReturnSetInfo.{econtext,isDone}` carriers are modeled. So
//! `init_MultiFuncCall`, `per_MultiFuncCall`, `end_MultiFuncCall`, and the
//! file-static `shutdown_MultiFuncCall` have real bodies (see [`srf_support`])
//! and are installed by [`init_seams`]. The lone remaining `value_srf_unported`
//! stub exists only for a handful of legacy consumers whose *owners* still lack
//! the executor leg (`ExecMakeFunctionResultSet` / execSRF.c) that builds the
//! call frame and drives the per-call series; each is rewired onto
//! `init/per/end_MultiFuncCall` as its owner lands.
//!
//! Families (one module per cluster of `funcapi.c`):
//!
//!   * [`srf_support`] — the Set Returning Function plumbing:
//!     `InitMaterializedSRF`, `init_MultiFuncCall`, `per_MultiFuncCall`,
//!     `end_MultiFuncCall`, `shutdown_MultiFuncCall`.
//!   * [`result_type`] — result-type / tuple-descriptor resolution:
//!     `get_call_result_type`, `get_expr_result_type`, `get_func_result_type`,
//!     `internal_get_result_type`, `get_expr_result_tupdesc`.
//!   * [`polymorphic`] — polymorphic pseudo-type resolution:
//!     `resolve_anyelement_from_others` and the `any{array,range,multirange}`
//!     siblings, `resolve_polymorphic_tupdesc`, `resolve_polymorphic_argtypes`,
//!     `get_type_func_class`, `get_call_expr_argtype`.
//!   * [`proc_info`] — `pg_proc`-row projection:
//!     `get_func_arg_info`, `get_func_trftypes`, `get_func_input_arg_names`,
//!     `get_func_result_name`, `build_function_result_tupdesc_t`,
//!     `build_function_result_tupdesc_d`.
//!   * [`tupledesc`] — descriptor builders + VARIADIC unpacking:
//!     `RelationNameGetTupleDesc`, `TypeGetTupleDesc`, `extract_variadic_args`.
//!
//! This crate OWNS the inward `backend-utils-fmgr-funcapi-seams` crate (the SRF
//! and `pg_proc`-projection entrypoints other ported crates call). [`init_seams`]
//! installs every owned seam.

// `clippy::result_large_err`: fallible functions return the shared
// `backend_utils_error::PgResult` (== `Result<_, PgError>`); `PgError`'s size is
// the project-wide error contract these ports match.
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod polymorphic;
pub mod proc_info;
pub mod result_type;
pub mod srf_support;
pub mod tupledesc;

/// Install every seam this unit owns into `backend-utils-fmgr-funcapi-seams`.
/// Invoked once at backend startup from `seams-init::init_all()`.
pub fn init_seams() {
    backend_utils_fmgr_funcapi_seams::InitMaterializedSRF::set(srf_support::InitMaterializedSRF);
    backend_utils_fmgr_funcapi_seams::materialized_srf_putvalues::set(
        srf_support::materialized_srf_putvalues,
    );
    backend_utils_fmgr_funcapi_seams::get_func_arg_info::set(proc_info::get_func_arg_info_seam);
    // The composite/record-Datum carrier bridge (task #161) is now buildable:
    // record_from_values forms a tuple and crosses it as a composite record
    // Datum via `backend_access_common_heaptuple::HeapTupleGetDatum`.
    backend_utils_fmgr_funcapi_seams::record_from_values::set(tupledesc::record_from_values);
    backend_utils_fmgr_funcapi_seams::srf_arg0_oid::set(srf_support::srf_arg0_oid);
    backend_utils_fmgr_funcapi_seams::srf_arg_int64::set(srf_support::srf_arg_int64);
    backend_utils_fmgr_funcapi_seams::srf_arg_lsn::set(srf_support::srf_arg_lsn);
    backend_utils_fmgr_funcapi_seams::cstring_get_text_datum::set(
        srf_support::cstring_get_text_datum,
    );
    // The value-per-call SRF protocol (#349): the multi-call FuncCallContext
    // machinery (SRF_FIRSTCALL_INIT / SRF_PERCALL_SETUP / the SRF_RETURN_DONE
    // teardown) over the `fn_extra`/`fn_mcxt` call-frame channels and the
    // `ReturnSetInfo.{econtext,isDone}` carriers. `shutdown_MultiFuncCall` is
    // funcapi-internal (C file-static), driven by `end_MultiFuncCall`.
    backend_utils_fmgr_funcapi_seams::init_MultiFuncCall::set(srf_support::init_MultiFuncCall);
    backend_utils_fmgr_funcapi_seams::per_MultiFuncCall::set(srf_support::per_MultiFuncCall);
    backend_utils_fmgr_funcapi_seams::end_MultiFuncCall::set(srf_support::end_MultiFuncCall);
    // The `value_srf_unported` seam is retired: the value-per-call protocol is
    // now ported. (Its former consumers — pg_partition_tree / pg_lock_status —
    // become wireable through init/per/end_MultiFuncCall as their owners land.)
    // Kept installed with the panic body only while any caller still routes to
    // it; new SRFs use the real machinery above.
    backend_utils_fmgr_funcapi_seams::value_srf_unported::set(srf_support::value_srf_unported);
    backend_utils_fmgr_funcapi_seams::get_expr_result_tupdesc::set(
        result_type::get_expr_result_tupdesc,
    );
    // `get_expr_result_type(expr)` — ExecInitFunctionScan (nodeFunctionscan.c)
    // resolves the SRF's result class / type / tupdesc through this.
    backend_utils_fmgr_funcapi_seams::get_expr_result_type::set(
        result_type::get_expr_result_type,
    );
}
