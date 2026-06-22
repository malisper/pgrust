//! The `genfile.c` directory-listing SRFs registered as executor-frame
//! materialize-mode set-returning functions.
//!
//! `pg_ls_dir`, `pg_ls_dir_1arg`, `pg_ls_waldir`, `pg_ls_logdir`,
//! `pg_ls_archive_statusdir`, `pg_ls_summariesdir`, `pg_ls_tmpdir_noargs` and
//! `pg_ls_tmpdir_1arg` are `proretset => 't'` functions reached through
//! nodeFunctionscan → [`crate::ExecMakeTableFunctionResult`]. Each fills a
//! materialize tuplestore via `InitMaterializedSRF`; their walk cores
//! (`AllocateDir`/`ReadDir`/`stat` + the per-entry filtering) are ported in
//! [`backend_utils_adt_misc2::admin`]. Here we only marshal the executor-frame
//! arguments and dispatch to those cores.
//!
//! These register in the executor-frame SRF table (the by-OID builtin registry's
//! tag-only `resultinfo` cannot carry the live `ReturnSetInfo`/`expectedDesc`
//! these record functions need — the WONTFIX dual-home, same as
//! `pg_tablespace_databases`).

use types_core::Oid;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_adt_misc2::admin;

use crate::register_srf;

// `pg_proc` OIDs (pg_proc.dat).
const PG_LS_DIR_1ARG: Oid = 2625; // pg_ls_dir(text)
const PG_LS_DIR: Oid = 3297; // pg_ls_dir(text, bool, bool)
const PG_LS_LOGDIR: Oid = 3353;
const PG_LS_WALDIR: Oid = 3354;
const PG_LS_ARCHIVE_STATUSDIR: Oid = 5031;
const PG_LS_SUMMARIESDIR: Oid = 6400;
const PG_LS_TMPDIR_NOARGS: Oid = 5029;
const PG_LS_TMPDIR_1ARG: Oid = 5030;

/// Register the genfile `pg_ls_*` SRFs in the executor-frame SRF table.
pub(crate) fn register_pg_ls_dir() {
    register_srf(PG_LS_DIR_1ARG, pg_ls_dir_1arg);
    register_srf(PG_LS_DIR, pg_ls_dir);
    register_srf(PG_LS_LOGDIR, pg_ls_logdir);
    register_srf(PG_LS_WALDIR, pg_ls_waldir);
    register_srf(PG_LS_ARCHIVE_STATUSDIR, pg_ls_archive_statusdir);
    register_srf(PG_LS_SUMMARIESDIR, pg_ls_summariesdir);
    register_srf(PG_LS_TMPDIR_NOARGS, pg_ls_tmpdir_noargs);
    register_srf(PG_LS_TMPDIR_1ARG, pg_ls_tmpdir_1arg);
}

/// Read a `text` argument off the executor frame as its NUL-free payload string.
///
/// `ExecEvalFuncArgs` marshals a by-reference `text` argument as its detoasted
/// header-ful varlena image in `ref_args[i]`; `VARDATA_ANY` skips the 4-byte
/// header — the same convention `pg_input_error_info` reads (C:
/// `text_to_cstring(PG_GETARG_TEXT_PP(i))`).
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pg_ls_dir: text arg missing from the by-ref lane");
    // `VARDATA_ANY`: skip ONE header byte for a short (1-byte, low-bit-set)
    // header, else `VARHDRSZ`. No-op while `SHORT_VARLENA_PACKING` is off.
    let bytes: &[u8] = match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= 4 => &image[4..],
        _ => &[],
    };
    core::str::from_utf8(bytes).expect("pg_ls_dir: text arg is valid UTF-8")
}

/// `pg_ls_dir(dirname [, missing_ok, include_dot_dirs])` — the C `PG_NARGS() == 3`
/// branch reads the two optional bool args (NULL is treated as `false`).
fn pg_ls_dir<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_ls_dir: fn_mcxt set by the SRF caller");
    let dirname = arg_text(fcinfo, 0).to_string();
    // C: if (!PG_ARGISNULL(1)) missing_ok = PG_GETARG_BOOL(1); else false.
    let missing_ok = !fcinfo.args[1].isnull && fcinfo.args[1].value.as_bool();
    let include_dot_dirs = !fcinfo.args[2].isnull && fcinfo.args[2].value.as_bool();
    admin::pg_ls_dir(mcx, fcinfo, &dirname, missing_ok, include_dot_dirs)
}

/// `pg_ls_dir(dirname)` — the one-argument arity (C `pg_ls_dir_1arg`).
fn pg_ls_dir_1arg<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_ls_dir_1arg: fn_mcxt set by the SRF caller");
    let dirname = arg_text(fcinfo, 0).to_string();
    admin::pg_ls_dir_1arg(mcx, fcinfo, &dirname)
}

fn pg_ls_logdir<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_ls_logdir: fn_mcxt set by the SRF caller");
    admin::pg_ls_logdir(mcx, fcinfo)
}

fn pg_ls_waldir<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_ls_waldir: fn_mcxt set by the SRF caller");
    admin::pg_ls_waldir(mcx, fcinfo)
}

fn pg_ls_archive_statusdir<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_ls_archive_statusdir: fn_mcxt set by the SRF caller");
    admin::pg_ls_archive_statusdir(mcx, fcinfo)
}

fn pg_ls_summariesdir<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_ls_summariesdir: fn_mcxt set by the SRF caller");
    admin::pg_ls_summariesdir(mcx, fcinfo)
}

fn pg_ls_tmpdir_noargs<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_ls_tmpdir_noargs: fn_mcxt set by the SRF caller");
    admin::pg_ls_tmpdir_noargs(mcx, fcinfo)
}

fn pg_ls_tmpdir_1arg<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_ls_tmpdir_1arg: fn_mcxt set by the SRF caller");
    // C: PG_GETARG_OID(0) — by-value oid argument.
    let tablespace = fcinfo.args[0].value.as_oid();
    admin::pg_ls_tmpdir_1arg(mcx, fcinfo, tablespace)
}
