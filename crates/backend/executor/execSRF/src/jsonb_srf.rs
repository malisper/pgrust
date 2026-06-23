//! Executor-frame registration of the materialize-mode `jsonb_array_elements`,
//! `jsonb_array_elements_text`, and `jsonb_object_keys` SRFs (jsonfuncs.c:2207
//! `elements_worker_jsonb` / jsonfuncs.c:568 `jsonb_object_keys`).
//!
//! Unlike the json (text) `array_elements` / `object_keys` SRFs (the
//! value-per-call SRFs in [`crate::json_srf`]), the jsonb variants return their
//! whole result through the materialize protocol: `InitMaterializedSRF` builds
//! the single-column (`jsonb` / `text`) tuplestore on `rsinfo->setResult`, the
//! worker `materialized_srf_putvalues`-es one row per element / key, and the
//! fmgr entry point returns SQL NULL. The full bodies live in
//! [`adt_jsonfuncs::{elements,keys}`]; this unit only adapts the
//! owned `(mcx, fcinfo, ...)` worker signature to the executor-frame
//! [`nodes::execexpr::PGFunction`] ABI (`fn(&mut FunctionCallInfoBaseData)
//! -> Datum`) and registers each under its `pg_proc` OID, exactly as
//! `fmgr_builtins[]` would add an ordinary row.
//!
//! These are dispatched by `ExecMakeTableFunctionResult` through the
//! executor-frame SRF table (the frame whose `resultinfo` carries the live
//! `ReturnSetInfo` the worker reads/writes); the by-OID fmgr-core registry's
//! tag-only `resultinfo` cannot carry it, which is why these are registered
//! here and NOT in `register_jsonfuncs_builtins` (jsonfuncs fmgr_builtins.rs).

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

use crate::register_srf;

/// `jsonb_array_elements(jsonb)` (OID 3219).
const JSONB_ARRAY_ELEMENTS: Oid = 3219;
/// `jsonb_array_elements_text(jsonb)` (OID 3465).
const JSONB_ARRAY_ELEMENTS_TEXT: Oid = 3465;
/// `jsonb_object_keys(jsonb)` (OID 3931).
const JSONB_OBJECT_KEYS: Oid = 3931;
/// `jsonb_path_query(jsonb, jsonpath, jsonb, bool)` (OID 4006).
const JSONB_PATH_QUERY: Oid = 4006;
/// `jsonb_path_query_tz(jsonb, jsonpath, jsonb, bool)` (OID 1179).
const JSONB_PATH_QUERY_TZ: Oid = 1179;

/// `VARHDRSZ` — the uncompressed 4-byte varlena length-word size.
const VARHDRSZ: usize = 4;

/// Register the materialize-mode `jsonb_array_elements[_text]` /
/// `jsonb_object_keys` / `jsonb_path_query[_tz]` SRFs in the executor-frame SRF
/// table.
pub(crate) fn register_jsonb_srfs() {
    register_srf(JSONB_ARRAY_ELEMENTS, jsonb_array_elements);
    register_srf(JSONB_ARRAY_ELEMENTS_TEXT, jsonb_array_elements_text);
    register_srf(JSONB_OBJECT_KEYS, jsonb_object_keys);
    register_srf(JSONB_PATH_QUERY, jsonb_path_query);
    register_srf(JSONB_PATH_QUERY_TZ, jsonb_path_query_tz);
}

/// The per-query memory context the SRF caller threads onto the executor frame
/// (`fcinfo->fn_mcxt`, the materialize tuplestore + descriptor arena).
fn srf_mcx<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> Mcx<'mcx> {
    fcinfo
        .fn_mcxt
        .expect("jsonb SRF: fn_mcxt set by ExecMakeTableFunctionResult")
}

/// `jsonb_array_elements(PG_FUNCTION_ARGS)` (jsonfuncs.c:2207) over the executor
/// frame.
fn jsonb_array_elements<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    adt_jsonfuncs::elements::jsonb_array_elements(mcx, fcinfo)
}

/// `jsonb_array_elements_text(PG_FUNCTION_ARGS)` (jsonfuncs.c:2213) over the
/// executor frame.
fn jsonb_array_elements_text<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    adt_jsonfuncs::elements::jsonb_array_elements_text(mcx, fcinfo)
}

/// `jsonb_object_keys(PG_FUNCTION_ARGS)` (jsonfuncs.c:568) over the executor
/// frame.
fn jsonb_object_keys<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    adt_jsonfuncs::keys::jsonb_object_keys(mcx, fcinfo)
}

// ===========================================================================
// jsonb_path_query / _tz  (jsonpath_exec.c:572 / :578) over the executor frame.
//
// The set-returning `jsonb_path_query(jsonb, jsonpath, jsonb, bool)` runs the
// jsonpath engine over the document, then emits one `jsonb` row per matched
// item through the materialize protocol — mirroring the jsonb_array_elements
// materialize SRF: `InitMaterializedSRF` blesses the executor's 1-column
// (`jsonb`) `value` descriptor, then `materialized_srf_putvalues` appends one
// row per result image. The value cores
// (`jsonpath_exec::jsonb_path_query[_tz]`) return the whole
// result set as a Vec of FULL `jsonb` varlena images (with VARHDRSZ header).
// ===========================================================================

/// `PG_GETARG_JSONB_P(i)`: the FULL on-disk `jsonb` varlena image (4-byte length
/// header + root container) on the by-ref lane. The `executeJsonPath` cores
/// (`jsonb_root`/`JsonbInitBinary`) slice past the header themselves, so the full
/// image is forwarded verbatim (the `jsonb` lane carries a single full varlena).
fn arg_jsonb_image<'a>(fcinfo: &'a FunctionCallInfoBaseData<'_>, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("jsonb_path_query SRF: by-ref `jsonb` arg missing from by-ref lane")
}

/// `PG_GETARG_JSONPATH_P(i)`: the FULL on-disk `jsonpath` varlena. Like the
/// `jsonpath` predicate builtins (cf. `backend-utils-adt-jsonpath-exec`'s
/// `arg_jsonpath_image`), the `jsonpath` by-ref lane carries the full `jsonpath`
/// varlena behind ONE extra leading `VARHDRSZ` word (the canonical-`ByRef`->ABI
/// bridge frames a pass-by-reference arg that way); strip that one leading header
/// to recover the real full `jsonpath` varlena the cores slice into. The strip is
/// short-aware (`vardata_any`) for parity with the sibling `arg_jsonpath_image`:
/// the outer framing word is always 4-byte, so this is a no-op there, but a
/// stored short-headed value reaching this adapter directly would still strip the
/// correct single header byte once `SHORT_VARLENA_PACKING` is on.
fn arg_jsonpath_image<'a>(fcinfo: &'a FunctionCallInfoBaseData<'_>, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("jsonb_path_query SRF: by-ref `jsonpath` arg missing from by-ref lane");
    vardata_any(image)
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image:
/// skip ONE header byte for a short (1-byte, low-bit-set) header, else `VARHDRSZ`.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

/// `PG_GETARG_BOOL(i)`: the `silent` flag arrives as a plain by-value word.
fn arg_bool(fcinfo: &FunctionCallInfoBaseData<'_>, i: usize) -> bool {
    fcinfo.args[i].value.as_usize() != 0
}

/// Drive the materialize protocol for a jsonpath query result set: bless the
/// executor-supplied single-column (`jsonb`) descriptor and put one row per
/// result image. A `SETOF jsonb` is a SCALAR result type, so — exactly like the
/// jsonb_array_elements materialize SRF — `MAT_SRF_USE_EXPECTED_DESC` blesses
/// the executor's 1-column `value` descriptor.
fn put_jsonb_path_rows<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    images: Vec<Vec<u8>>,
) -> types_error::PgResult<()> {
    use funcapi::srf_support::{
        materialized_srf_putvalues, InitMaterializedSRF,
    };

    // InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
    InitMaterializedSRF(
        fcinfo,
        nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC | nodes::funcapi::MAT_SRF_BLESS,
    )?;

    for image in images {
        // jsonb: `image` is already a full varlena (VARHDRSZ + payload), the form
        // a by-ref `jsonb` column value carries (cf. put_element_rows).
        let mut v = mcx::vec_with_capacity_in::<u8>(mcx, image.len())?;
        v.extend_from_slice(&image);
        let values: [Datum<'mcx>; 1] = [Datum::ByRef(v)];
        let nulls = [false];
        let rsi = fcinfo
            .resultinfo
            .as_mut()
            .expect("InitMaterializedSRF set fcinfo->resultinfo");
        materialized_srf_putvalues(rsi, &values, &nulls)?;
    }
    Ok(())
}

/// `jsonb_path_query(PG_FUNCTION_ARGS)` (jsonpath_exec.c:572) over the executor
/// frame.
fn jsonb_path_query<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let jp = arg_jsonpath_image(fcinfo, 1).to_vec();
    let vars = arg_jsonb_image(fcinfo, 2).to_vec();
    let silent = arg_bool(fcinfo, 3);
    let images =
        jsonpath_exec::jsonb_path_query(mcx, &jb, &jp, Some(&vars), silent)?;
    put_jsonb_path_rows(mcx, fcinfo, images)?;
    // PG_RETURN_NULL();
    Ok(Datum::null())
}

/// `jsonb_path_query_tz(PG_FUNCTION_ARGS)` (jsonpath_exec.c:578) over the executor
/// frame.
fn jsonb_path_query_tz<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let jp = arg_jsonpath_image(fcinfo, 1).to_vec();
    let vars = arg_jsonb_image(fcinfo, 2).to_vec();
    let silent = arg_bool(fcinfo, 3);
    let images =
        jsonpath_exec::jsonb_path_query_tz(mcx, &jb, &jp, Some(&vars), silent)?;
    put_jsonb_path_rows(mcx, fcinfo, images)?;
    // PG_RETURN_NULL();
    Ok(Datum::null())
}
