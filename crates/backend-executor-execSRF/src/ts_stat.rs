//! `ts_stat(query[, weights])` (tsvector_op.c) registered as executor-frame
//! materialize-mode set-returning functions.
//!
//! In C these are ValuePerCall SRFs (`ts_stat1` / `ts_stat2`,
//! tsvector_op.c:2667/2692): on the first call `ts_stat_sql` runs the user SQL
//! through an SPI cursor (one `tsvector` column), accumulates every lexeme's
//! `(ndoc, nentry)` into a balanced stat tree (`ts_accum`), and
//! `ts_setup_firstcall` builds the result tuple descriptor; each subsequent call
//! walks the tree (`ts_process_call`) emitting one `(word text, ndoc int4,
//! nentry int4)` row.
//!
//! The owned port drives the same cores —
//! [`backend_utils_adt_tsvector_core::op::ts_stat_sql`] (the SPI-fed stat-tree
//! build, the SPI cursor walk itself behind the `exec_stat_query` seam installed
//! by `backend-executor-spi`), [`ts_setup_firstcall`] and the repeated
//! [`ts_process_call`] tree walk — over the materialize-mode SRF protocol:
//! `InitMaterializedSRF` builds the tuplestore, every walked row is appended via
//! `materialized_srf_putvalues`, and the entry returns SQL NULL.
//!
//! Registered from [`register_ts_stat`] (called by `init_seams`).

use mcx::Mcx;
use types_core::Oid;
use types_nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_fmgr_funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};
use backend_utils_adt_tsvector_core::op::{ts_process_call, ts_setup_firstcall, ts_stat_sql};

use crate::register_srf;

/// `ts_stat(text)` / `ts_stat1` (OID 3689).
const TS_STAT1: Oid = 3689;
/// `ts_stat(text, text)` / `ts_stat2` (OID 3690).
const TS_STAT2: Oid = 3690;

/// Register the `ts_stat` SRFs in the executor-frame SRF table.
pub(crate) fn register_ts_stat() {
    register_srf(TS_STAT1, ts_stat1);
    register_srf(TS_STAT2, ts_stat2);
}

/// `VARDATA_ANY(PG_GETARG_TEXT_PP(i))` — the header-stripped payload of a `text`
/// arg on the by-ref lane.
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    const VARHDRSZ: usize = 4;
    match fcinfo.ref_arg(i) {
        Some(FmgrArgRef::Varlena(b)) => {
            let image = b.as_slice();
            if image.len() >= VARHDRSZ {
                &image[VARHDRSZ..]
            } else {
                &[]
            }
        }
        _ => panic!("ts_stat: text argument missing from the by-ref lane"),
    }
}

/// `parse_int(cstring)` — the int4 input function `BuildTupleFromCStrings` runs
/// over `ts_process_call`'s `"%d"`-formatted `ndoc` / `nentry` bytes.
fn parse_i32(bytes: &[u8]) -> i32 {
    core::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .expect("ts_stat: ndoc/nentry column is a decimal integer")
}

/// Shared body for `ts_stat1` / `ts_stat2`: build the stat tree (optionally
/// weighted) and emit one `(word, ndoc, nentry)` row per tree node.
fn emit_ts_stat<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    mcx: Mcx<'mcx>,
    query: &[u8],
    weights: Option<&[u8]>,
) -> PgResult<Datum<'mcx>> {
    // ts_stat_sql: SPI cursor walk + ts_accum into the balanced stat tree.
    let mut stat = ts_stat_sql(query, weights)?;
    // ts_setup_firstcall: prime the in-order tree walk.
    ts_setup_firstcall(&mut stat)?;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("ts_stat: InitMaterializedSRF establishes fcinfo->resultinfo");

    // while ((result = ts_process_call(funcctx)) != 0) SRF_RETURN_NEXT(...).
    while let Some(row) = ts_process_call(&mut stat) {
        // C: values[0]=lexeme (NUL-terminated), values[1]="%d"(ndoc),
        // values[2]="%d"(nentry), all through BuildTupleFromCStrings; the int4
        // input function parses the decimal text. We build typed Datums directly.
        let word = backend_utils_adt_varlena_seams::cstring_to_text_v::call(
            mcx,
            core::str::from_utf8(&row.col0).expect("ts_stat: word is valid UTF-8 text"),
        )?;
        let ndoc = parse_i32(row.col1.as_deref().expect("ts_stat: ndoc present"));
        let nentry = parse_i32(row.col2.as_deref().expect("ts_stat: nentry present"));
        let values = [word, Datum::from_i32(ndoc), Datum::from_i32(nentry)];
        materialized_srf_putvalues(rsinfo, &values, &[false, false, false])?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}

/// `ts_stat1(PG_FUNCTION_ARGS)` (tsvector_op.c:2667) — `ts_stat(query)`.
fn ts_stat1<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("ts_stat1: fn_mcxt set by ExecMakeTableFunctionResult");
    let query = arg_text(fcinfo, 0).to_vec();
    emit_ts_stat(fcinfo, mcx, &query, None)
}

/// `ts_stat2(PG_FUNCTION_ARGS)` (tsvector_op.c:2692) — `ts_stat(query, weights)`.
fn ts_stat2<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("ts_stat2: fn_mcxt set by ExecMakeTableFunctionResult");
    let query = arg_text(fcinfo, 0).to_vec();
    let weights = arg_text(fcinfo, 1).to_vec();
    emit_ts_stat(fcinfo, mcx, &query, Some(&weights))
}
