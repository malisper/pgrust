//! `pg_get_keywords()` (OID 1686) registered as an executor-frame
//! materialize-mode set-returning function.
//!
//! `misc.c`'s `pg_get_keywords` is a value-per-call SRF emitting one
//! `(word text, catcode "char", barelabel bool, catdesc text, baredesc text)`
//! row per grammar keyword (`ScanKeywords.num_keywords`), building each tuple via
//! `BuildTupleFromCStrings`. The keyword-table render core (the
//! `GetScanKeyword`/`ScanKeywordCategories`/`ScanKeywordBareLabel` walk and the
//! category-letter/description `switch`) is ported in
//! [`misc::pg_get_keywords`], which hands back a
//! `Vec<KeywordRow>`.
//!
//! Here that core is driven over the executor frame in materialize mode (the row
//! set is fixed and known up front, so the whole tuplestore is filled once,
//! emitting the identical rows the C per-call series would). `InitMaterializedSRF`
//! with `MAT_SRF_USE_EXPECTED_DESC` takes the executor's already-resolved
//! `(text, "char", bool, text, text)` descriptor (skipping the catalog
//! `get_call_result_type`), the rows are appended via `materialized_srf_putvalues`
//! in the catalog column order `(word, catcode, barelabel, catdesc, baredesc)`,
//! and the entry point returns SQL NULL. Registered from
//! [`register_pg_get_keywords`] (called by `init_seams`); it bypasses the by-OID
//! builtin registry whose tag-only `resultinfo` cannot carry the live
//! `ReturnSetInfo` (the WONTFIX dual-home).

use ::mcx::Mcx;
use ::types_core::Oid;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use ::types_error::PgResult;
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};

use crate::register_srf;

/// `pg_get_keywords()` (OID 1686).
const PG_GET_KEYWORDS: Oid = 1686;

/// Register `pg_get_keywords` in the executor-frame SRF table.
pub(crate) fn register_pg_get_keywords() {
    register_srf(PG_GET_KEYWORDS, pg_get_keywords);
}

/// `CStringGetTextDatum(s)` over the call's per-query context.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `pg_get_keywords(PG_FUNCTION_ARGS)` (misc.c:417) over the executor frame.
fn pg_get_keywords<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_get_keywords: fn_mcxt set by ExecMakeTableFunctionResult");

    // The keyword-table render core (pure static grammar data).
    let rows = misc::pg_get_keywords();

    // C: get_call_result_type(fcinfo, NULL, &tupdesc). The owned model takes the
    // executor's already-resolved `(text, "char", bool, text, text)` descriptor
    // via MAT_SRF_USE_EXPECTED_DESC.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_get_keywords: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in &rows {
        // C BuildTupleFromCStrings runs each column's input function. The catalog
        // column order is (word text, catcode "char", barelabel bool, catdesc
        // text, baredesc text) — note barelabel precedes catdesc.

        // values[0] = word (textin).
        let word = text_datum(
            mcx,
            core::str::from_utf8(&row.word).expect("pg_get_keywords: keyword is valid UTF-8"),
        )?;

        // values[1] = catcode "char" (charin of the one-letter "U"/"C"/"T"/"R");
        // the "shouldn't be possible" default arm is NULL.
        let (catcode, catcode_null) = match row.catcode {
            Some(letter) => {
                let b = letter.as_bytes()[0] as i8;
                (Datum::from_char(b), false)
            }
            None => (Datum::null(), true),
        };

        // values[2] = barelabel bool (boolin of "true"/"false").
        let barelabel = Datum::from_bool(row.barelabel == "true");

        // values[3] = catdesc text (textin); NULL for the default arm.
        let (catdesc, catdesc_null) = match row.catdesc {
            Some(desc) => (text_datum(mcx, desc)?, false),
            None => (Datum::null(), true),
        };

        // values[4] = baredesc text (textin).
        let baredesc = text_datum(mcx, row.baredesc)?;

        let values = [word, catcode, barelabel, catdesc, baredesc];
        let nulls = [false, catcode_null, false, catdesc_null, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    // C: SRF_RETURN_DONE — the whole set is in the materialize tuplestore.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
