//! Executor-frame registration of the single-composite-row system builtins that
//! are called via the FROM clause (a function RTE) and therefore dispatch through
//! [`ExecMakeTableFunctionResult`](crate::ExecMakeTableFunctionResult) →
//! [`srf_invoke_by_oid`](crate::srf_invoke_by_oid), NOT the by-OID fmgr-core
//! builtin registry (the function-scan path always consults the executor-frame
//! SRF table):
//!
//!   * `pg_control_system`     (OID 3441)  — pg_controldata.c, 4-column row
//!   * `pg_control_checkpoint` (OID 3442)  — pg_controldata.c, 18-column row
//!   * `pg_control_recovery`   (OID 3443)  — pg_controldata.c, 5-column row
//!   * `pg_control_init`       (OID 3444)  — pg_controldata.c, 12-column row
//!   * `pg_stat_file`          (OID 3307)  — genfile.c, 6-column record
//!   * `pg_stat_file_1arg`     (OID 2623)  — genfile.c, 1-arg variant
//!
//! None of these is set-returning (`proretset => 'f'`): each returns exactly one
//! composite row, exactly like the `json_to_record` family in
//! [`crate::json_record`]. The value-per-call loop stores the single row with
//! `isDone` left at `ExprSingleResult`. The worker bodies build the composite
//! `Datum` (the `record_from_values` → `HeapTupleGetDatum` pipeline) and this
//! unit only adapts the owned `(mcx[, args])` worker signature to the
//! executor-frame [`PGFunction`] ABI and registers each under its `pg_proc` OID.
//!
//! These ALSO carry a by-OID fmgr-core builtin row (registered in their owner
//! crates' `fmgr_builtins`/`register_*`), which the completeness guard tracks and
//! which serves a target-list (scalar-position) call; the FROM-clause form needs
//! this executor-frame registration. The dual home mirrors the `json_record`
//! family.

use mcx::Mcx;
use types_core::Oid;
use datum::varlena::VARHDRSZ;
use types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

use crate::register_srf;

/// `pg_control_system()` (OID 3441).
const PG_CONTROL_SYSTEM: Oid = 3441;
/// `pg_control_checkpoint()` (OID 3442).
const PG_CONTROL_CHECKPOINT: Oid = 3442;
/// `pg_control_recovery()` (OID 3443).
const PG_CONTROL_RECOVERY: Oid = 3443;
/// `pg_control_init()` (OID 3444).
const PG_CONTROL_INIT: Oid = 3444;
/// `pg_stat_file(filename, missing_ok)` (OID 3307).
const PG_STAT_FILE: Oid = 3307;
/// `pg_stat_file_1arg(filename)` (OID 2623).
const PG_STAT_FILE_1ARG: Oid = 2623;

/// Register the single-composite-row system builtins in the executor-frame SRF
/// table.
pub(crate) fn register_control_srfs() {
    register_srf(PG_CONTROL_SYSTEM, pg_control_system);
    register_srf(PG_CONTROL_CHECKPOINT, pg_control_checkpoint);
    register_srf(PG_CONTROL_RECOVERY, pg_control_recovery);
    register_srf(PG_CONTROL_INIT, pg_control_init);
    register_srf(PG_STAT_FILE, pg_stat_file);
    register_srf(PG_STAT_FILE_1ARG, pg_stat_file_1arg);
}

/// The per-query memory context the SRF caller threads onto the executor frame
/// (`fcinfo->fn_mcxt`) — the arena the formed composite result is allocated in.
fn srf_mcx<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> Mcx<'mcx> {
    fcinfo
        .fn_mcxt
        .expect("control SRF: fn_mcxt set by ExecMakeTableFunctionResult")
}

/// `PG_GETARG_TEXT_PP(i)` → `text_to_cstring`: a `text` arg's `VARDATA_ANY`
/// payload on the by-ref lane (header-ful image; skip the 4-byte varlena header),
/// decoded as UTF-8.
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("control SRF: text arg missing from by-ref lane");
    let bytes = &image[VARHDRSZ..];
    core::str::from_utf8(bytes).expect("control SRF: text arg not valid UTF-8")
}

/// `PG_GETARG_BOOL(i)`.
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo
        .args
        .get(i)
        .expect("control SRF: missing bool arg")
        .value
        .as_bool()
}

// ===========================================================================
//  pg_controldata.c — pg_control_* (no arguments).
// ===========================================================================

fn pg_control_system<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    more::pg_controldata::pg_control_system_datum(mcx)
}

fn pg_control_checkpoint<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    more::pg_controldata::pg_control_checkpoint_datum(mcx)
}

fn pg_control_recovery<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    more::pg_controldata::pg_control_recovery_datum(mcx)
}

fn pg_control_init<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    more::pg_controldata::pg_control_init_datum(mcx)
}

// ===========================================================================
//  genfile.c — pg_stat_file (text [, bool]).
// ===========================================================================

fn pg_stat_file<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    let filename = arg_text(fcinfo, 0);
    let missing_ok = arg_bool(fcinfo, 1);
    // C: PG_NARGS() == 2 here.
    misc2::admin::pg_stat_file(mcx, filename, missing_ok, true)
}

fn pg_stat_file_1arg<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    let filename = arg_text(fcinfo, 0);
    misc2::admin::pg_stat_file_1arg(mcx, filename)
}
