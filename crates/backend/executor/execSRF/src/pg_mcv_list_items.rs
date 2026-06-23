//! `pg_mcv_list_items(pg_mcv_list)` (OID 3427, prosrc `pg_stats_ext_mcvlist_items`)
//! registered as an executor-frame materialize-mode set-returning function.
//!
//! mcv.c's `pg_stats_ext_mcvlist_items` deserializes the stored MCV list and
//! emits one row per MCV item:
//!   `(index int4, values text[], nulls bool[], frequency float8,
//!     base_frequency float8)`.
//! Each item's `values[]` is built by running the per-dimension type's output
//! function (`getTypeOutputInfo` + `FunctionCall1`) on the stored Datum and
//! wrapping the cstring in a `text`; NULL dimensions become NULL array elements.
//! `nulls[]` is the item's per-dimension `isnull` flags as a `bool[]`.
//!
//! The MCV deserialize core lives in [`mcv`]; here it is
//! driven over the executor frame: `InitMaterializedSRF` with
//! `MAT_SRF_USE_EXPECTED_DESC` adopts the executor's already-resolved row type
//! (the C `get_call_result_type`), and one row is appended per item via
//! `materialized_srf_putvalues`. The entry point returns SQL NULL; the executor
//! (`ExecMakeTableFunctionResult` Materialize branch) drains the tuplestore.

use mcx::Mcx;
use types_core::Oid;
use ::nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_error::PgResult;
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_mcv_list_items(pg_mcv_list)` (pg_proc.dat OID 3427).
const PG_STATS_EXT_MCVLIST_ITEMS: Oid = 3427;

/// `TEXTOID` (pg_type.dat) — element type of the `values text[]` column.
const TEXTOID: Oid = 25;
/// `BOOLOID` (pg_type.dat) — element type of the `nulls bool[]` column.
const BOOLOID: Oid = 16;

/// Register `pg_stats_ext_mcvlist_items` in the executor-frame SRF table.
pub(crate) fn register_pg_mcv_list_items() {
    register_srf(PG_STATS_EXT_MCVLIST_ITEMS, pg_stats_ext_mcvlist_items);
}

/// Copy a header-ful varlena image (the array constructor's on-disk bytes) into
/// an `mcx`-owned by-reference Datum, matching the by-ref output lane used by
/// the other materialize SRFs.
fn byref_image<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<Datum<'mcx>> {
    let mut buf = mcx::PgVec::new_in(mcx);
    buf.try_reserve(image.len())
        .map_err(|_| mcx.oom(image.len()))?;
    buf.extend_from_slice(image);
    Ok(Datum::ByRef(buf))
}

/// `pg_stats_ext_mcvlist_items(PG_FUNCTION_ARGS)` (mcv.c:1338) over the executor
/// frame.
fn pg_stats_ext_mcvlist_items<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_stats_ext_mcvlist_items: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: mcvlist = statext_mcv_deserialize(PG_GETARG_BYTEA_P(0)) — the detoasted,
    // header-ful on-disk `pg_mcv_list` (a bytea) image on the by-ref side channel.
    let image: Vec<u8> = match fcinfo.ref_arg(0) {
        Some(FmgrArgRef::Varlena(b)) => b.as_slice().to_vec(),
        _ => panic!("pg_stats_ext_mcvlist_items: pg_mcv_list argument missing from the by-ref lane"),
    };

    // statext_mcv_deserialize takes the full varlena (it reads the varlena
    // header — VARSIZE_ANY / VARDATA_ANY — itself), matching the C
    // `statext_mcv_deserialize(bytea *data)` contract.
    let mcvlist = mcv::statext_mcv_deserialize(mcx, Some(&image))?;

    // C: InitMaterializedSRF(fcinfo, 0). MAT_SRF_USE_EXPECTED_DESC adopts the
    // executor's already-resolved `(int4, text[], bool[], float8, float8)` row
    // type, skipping the catalog get_call_result_type.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let mcvlist = match mcvlist {
        // C: funcctx->max_calls stays 0 when user_fctx == NULL, so no rows.
        None => {
            fcinfo.isnull = true;
            return Ok(Datum::null());
        }
        Some(m) => m,
    };

    let ndim = mcvlist.ndimensions as usize;

    // Cache the per-dimension output function so we don't re-resolve it per item.
    let mut out_funcs: Vec<Oid> = Vec::with_capacity(ndim);
    for d in 0..ndim {
        // C: getTypeOutputInfo(mcvlist->types[i], &outfunc, &isvarlena).
        let (outfunc, _isvarlena) =
            lsyscache_seams::get_type_output_info::call(mcvlist.types[d])?;
        out_funcs.push(outfunc);
    }

    for (call_cntr, item) in mcvlist.items.iter().enumerate() {
        // Build the `values text[]` and `nulls bool[]` element vectors.
        let mut value_elems: Vec<Datum<'mcx>> = Vec::with_capacity(ndim);
        let mut value_nulls: Vec<bool> = Vec::with_capacity(ndim);
        let mut null_elems: Vec<Datum<'mcx>> = Vec::with_capacity(ndim);

        for d in 0..ndim {
            // nulls[] element: BoolGetDatum(item->isnull[i]).
            null_elems.push(Datum::from_bool(item.isnull[d]));

            if !item.isnull[d] {
                // val = FunctionCall1(outfunc, item->values[i]);
                // txt = cstring_to_text(DatumGetPointer(val));
                let cstr = fmgr_seams::oid_output_function_call_datum::call(
                    mcx,
                    out_funcs[d],
                    item.values[d].clone(),
                )?;
                let txt = varlena_seams::cstring_to_text_v::call(
                    mcx,
                    cstr.as_str(),
                )?;
                value_elems.push(txt);
                value_nulls.push(false);
            } else {
                // accumArrayResult(..., isnull=true, TEXTOID): a NULL text element.
                value_elems.push(Datum::null());
                value_nulls.push(true);
            }
        }

        // values[1] = makeArrayResult(astate_values) — text[] (with NULLs).
        // construct_array has no per-element nulls argument, so go through
        // construct_md_array (1-D) carrying the value_nulls bitmap.
        let v_ndims = if value_elems.is_empty() { 0 } else { 1 };
        let v_dims = [value_elems.len() as i32];
        let v_lbs = [1];
        let values_arr = arrayfuncs::construct::construct_md_array_values(
            mcx,
            &value_elems,
            Some(&value_nulls),
            v_ndims,
            &v_dims,
            &v_lbs,
            TEXTOID,
            -1,    // text typlen
            false, // text typbyval
            b'i',  // text typalign ('i')
        )?;
        // values[2] = makeArrayResult(astate_nulls) — bool[] (never NULL).
        let n_ndims = if null_elems.is_empty() { 0 } else { 1 };
        let n_dims = [null_elems.len() as i32];
        let n_lbs = [1];
        let nulls_arr = arrayfuncs::construct::construct_md_array_values(
            mcx,
            &null_elems,
            None,
            n_ndims,
            &n_dims,
            &n_lbs,
            BOOLOID,
            1,     // bool typlen
            true,  // bool typbyval
            b'c',  // bool typalign ('c')
        )?;

        let row = [
            Datum::from_i32(call_cntr as i32),
            byref_image(mcx, &values_arr)?,
            byref_image(mcx, &nulls_arr)?,
            Datum::from_f64(item.frequency),
            Datum::from_f64(item.base_frequency),
        ];
        let row_nulls = [false, false, false, false, false];

        let rsinfo = fcinfo
            .resultinfo
            .as_mut()
            .expect("pg_stats_ext_mcvlist_items: InitMaterializedSRF set fcinfo->resultinfo");
        materialized_srf_putvalues(rsinfo, &row, &row_nulls)?;
    }

    // C: return (Datum) 0 — the set is in the materialize tuplestore.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
