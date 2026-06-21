//! Array-input percentile finalfns: `percentile_disc_multi_final` (3979) and
//! `percentile_cont_float8_multi_final` (3981) (`orderedsetaggs.c:731/848`).
//!
//! These take a `float8[]` of percentiles and return an array (same element type
//! as the sorted column) of the corresponding sampled values. The result array
//! is the same shape as the input percentile array (`construct_md_array` over
//! `ARR_NDIM`/`ARR_DIMS`/`ARR_LBOUND` of the input), with NULL output slots for
//! NULL input percentiles. We reuse the arrayfuncs `array_map_deconstruct`
//! (reads input ndim/dims/lbs + flat float8 elements) and `array_map_build`
//! (`construct_md_array` over the same dims with a null bitmap, supporting both
//! by-value and by-reference result element types) seams.

use types_datum::Datum as Word;
use types_fmgr::boundary::RefPayload;
use types_fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum as CDatum;

use backend_utils_sort_tuplesort_seams as tsort;

use super::{
    arg_isnull, float8_lerp, leak_ctx, ok, perform_or_rescan, raise, ret_null, restash,
    take_group_state, vec_in, with_sortstate_mut, OSAPerGroupState,
};

const FLOAT8OID: types_core::Oid = 701;

/// One percentile's sample plan (C `struct pct_info`).
#[derive(Clone, Copy)]
struct PctInfo {
    first_row: i64,
    second_row: i64,
    proportion: f64,
    idx: usize,
}

/// The deconstructed percentile-array input: its full shape plus the flat
/// `(value, isnull)` element list (C `deconstruct_array_builtin` over the input,
/// keeping `ARR_NDIM`/`ARR_DIMS`/`ARR_LBOUND` for the same-shape result).
struct PercentilesMd {
    ndim: i32,
    dims: alloc::vec::Vec<i32>,
    lbs: alloc::vec::Vec<i32>,
    /// `(percentile value, isnull)` in array order.
    elems: alloc::vec::Vec<(f64, bool)>,
}

/// `setup_pct_info` — compute and sort the per-percentile sample rows.
fn setup_pct_info(
    percentiles: &[(f64, bool)],
    rowcount: i64,
    continuous: bool,
) -> alloc::vec::Vec<PctInfo> {
    let mut pct: alloc::vec::Vec<PctInfo> = alloc::vec::Vec::with_capacity(percentiles.len());
    for (i, (val, isnull)) in percentiles.iter().enumerate() {
        if *isnull {
            pct.push(PctInfo {
                first_row: 0,
                second_row: 0,
                proportion: 0.0,
                idx: i,
            });
            continue;
        }
        let p = *val;
        if p < 0.0 || p > 1.0 || p.is_nan() {
            raise(super::percentile_range_error(p));
        }
        if continuous {
            let rc1 = (rowcount - 1) as f64;
            pct.push(PctInfo {
                first_row: 1 + (p * rc1).floor() as i64,
                second_row: 1 + (p * rc1).ceil() as i64,
                proportion: (p * rc1) - (p * rc1).floor(),
                idx: i,
            });
        } else {
            let row = core::cmp::max(1, (p * rowcount as f64).ceil() as i64);
            pct.push(PctInfo {
                first_row: row,
                second_row: row,
                proportion: 0.0,
                idx: i,
            });
        }
    }
    // Sort by first_row then second_row (C pct_info_cmp).
    pct.sort_by(|a, b| {
        a.first_row
            .cmp(&b.first_row)
            .then(a.second_row.cmp(&b.second_row))
    });
    pct
}

/// Read the percentile-array arg (`float8[]`) off the by-ref lane, preserving its
/// dimensionality and per-element null flags (C `deconstruct_array_builtin` +
/// `ARR_NDIM`/`ARR_DIMS`/`ARR_LBOUND`). Returns `None` when arg1 is NULL.
fn read_percentiles_md(fcinfo: &mut FunctionCallInfoBaseData) -> Option<PercentilesMd> {
    if arg_isnull(fcinfo, 1) {
        return None;
    }
    let mcx = leak_ctx("percentile array");
    let bytes: alloc::vec::Vec<u8> = match fcinfo.ref_arg(1) {
        Some(RefPayload::Varlena(b)) => b.clone(),
        _ => raise(types_error::PgError::error(
            "percentile multi finalfn: percentile array arg has no by-reference payload",
        )),
    };
    let arr = CDatum::ByRef(vec_in(mcx, &bytes));
    let src = ok(backend_utils_adt_arrayfuncs_seams::array_map_deconstruct::call(mcx, arr));
    let elems = src
        .elems
        .iter()
        .map(|(d, isnull)| {
            let v = if *isnull { 0.0 } else { d.as_f64() };
            (v, *isnull)
        })
        .collect();
    Some(PercentilesMd {
        ndim: src.ndim,
        dims: src.dims.iter().copied().collect(),
        lbs: src.lbs.iter().copied().collect(),
        elems,
    })
}

/// Build the result array preserving the input's shape (`construct_md_array`),
/// with a null bitmap for NULL output slots, over result element type `elmtype`
/// with storage attributes `(typlen, typbyval, typalign)`. `array_map_build`
/// takes `DatumV` element values, so both by-value and by-reference element
/// types are expressible.
#[allow(clippy::too_many_arguments)]
fn build_md_result(
    fcinfo: &mut FunctionCallInfoBaseData,
    ndim: i32,
    dims: &[i32],
    lbs: &[i32],
    values: &[CDatum<'static>],
    nulls: &[bool],
    elmtype: types_core::Oid,
    typlen: i16,
    typbyval: bool,
    typalign: i8,
) -> Word {
    let mcx = leak_ctx("percentile result array");
    let img = ok(backend_utils_adt_arrayfuncs_seams::array_map_build::call(
        mcx,
        ndim,
        dims,
        lbs,
        values,
        nulls,
        elmtype,
        typlen,
        typbyval,
        typalign as core::ffi::c_char,
    ));
    fcinfo.set_ref_result(RefPayload::Varlena(img.iter().copied().collect()));
    Word::from_usize(0)
}

/// `construct_empty_array(elmtype)` — a zero-element 1-D array image, returned on
/// the Varlena result lane.
fn empty_array(
    fcinfo: &mut FunctionCallInfoBaseData,
    elmtype: types_core::Oid,
    typlen: i16,
    typbyval: bool,
    typalign: i8,
) -> Word {
    build_md_result(fcinfo, 0, &[], &[], &[], &[], elmtype, typlen, typbyval, typalign)
}

/// `percentile_disc_multi_final(PG_FUNCTION_ARGS)` (3979).
pub fn fc_percentile_disc_multi_final(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    if arg_isnull(fcinfo, 0) {
        return ret_null(fcinfo);
    }
    let mut osastate = take_group_state(fcinfo).expect("percentile_disc_multi: non-null arg0");
    if osastate.number_of_rows == 0 {
        restash(fcinfo, osastate);
        return ret_null(fcinfo);
    }
    let Some(param) = read_percentiles_md(fcinfo) else {
        restash(fcinfo, osastate);
        return ret_null(fcinfo);
    };
    let elmtype = osastate.qstate.sort_col_type;
    let typlen = osastate.qstate.typ_len;
    let typbyval = osastate.qstate.typ_by_val;
    let typalign = osastate.qstate.typ_align;
    if param.elems.is_empty() {
        let r = empty_array(fcinfo, elmtype, typlen, typbyval, typalign);
        restash(fcinfo, osastate);
        return r;
    }

    let pct = setup_pct_info(&param.elems, osastate.number_of_rows, false);
    let n = param.elems.len();
    let mut result_val: alloc::vec::Vec<CDatum<'static>> =
        alloc::vec![CDatum::from_usize(0); n];
    let mut result_null: alloc::vec::Vec<bool> = alloc::vec![true; n];

    // NULLs sort to the front on row=0.
    let mut i = 0usize;
    while i < n && pct[i].first_row == 0 {
        result_null[pct[i].idx] = true;
        i += 1;
    }

    if i < n {
        perform_or_rescan(&mut osastate);
        let mut rownum: i64 = 0;
        let mut val: CDatum<'static> = CDatum::from_usize(0);
        let mut isnull = true;
        while i < n {
            let target_row = pct[i].first_row;
            let idx = pct[i].idx;
            if target_row > rownum {
                let (_got, v, isn) = fetch_skip(&mut osastate, target_row - rownum - 1);
                val = clone_cdatum(&v);
                isnull = isn;
                rownum = target_row;
            }
            result_val[idx] = clone_cdatum(&val);
            result_null[idx] = isnull;
            i += 1;
        }
    }

    let r = build_md_result(
        fcinfo,
        param.ndim,
        &param.dims,
        &param.lbs,
        &result_val,
        &result_null,
        elmtype,
        typlen,
        typbyval,
        typalign,
    );
    restash(fcinfo, osastate);
    r
}

/// `percentile_cont_float8_multi_final(PG_FUNCTION_ARGS)` (3981).
pub fn fc_percentile_cont_float8_multi_final(fcinfo: &mut FunctionCallInfoBaseData) -> Word {
    if arg_isnull(fcinfo, 0) {
        return ret_null(fcinfo);
    }
    let mut osastate = take_group_state(fcinfo).expect("percentile_cont_multi: non-null arg0");
    if osastate.number_of_rows == 0 {
        restash(fcinfo, osastate);
        return ret_null(fcinfo);
    }
    if osastate.qstate.sort_col_type != FLOAT8OID {
        raise(types_error::PgError::error("percentile_cont_multi: type mismatch"));
    }
    let Some(param) = read_percentiles_md(fcinfo) else {
        restash(fcinfo, osastate);
        return ret_null(fcinfo);
    };
    // float8 result element storage attributes (typlen=8, byval=true, align='d').
    let typlen = osastate.qstate.typ_len;
    let typbyval = osastate.qstate.typ_by_val;
    let typalign = osastate.qstate.typ_align;
    if param.elems.is_empty() {
        let r = empty_array(fcinfo, FLOAT8OID, typlen, typbyval, typalign);
        restash(fcinfo, osastate);
        return r;
    }

    let pct = setup_pct_info(&param.elems, osastate.number_of_rows, true);
    let n = param.elems.len();
    let mut result_val: alloc::vec::Vec<CDatum<'static>> =
        alloc::vec![CDatum::from_usize(0); n];
    let mut result_null: alloc::vec::Vec<bool> = alloc::vec![true; n];

    let mut i = 0usize;
    while i < n && pct[i].first_row == 0 {
        result_null[pct[i].idx] = true;
        i += 1;
    }

    if i < n {
        perform_or_rescan(&mut osastate);
        let mut rownum: i64 = 0;
        let mut first_val: f64 = 0.0;
        let mut second_val: f64 = 0.0;
        while i < n {
            let first_row = pct[i].first_row;
            let second_row = pct[i].second_row;
            let idx = pct[i].idx;

            if first_row > rownum {
                let (got, v, isn) = fetch_skip(&mut osastate, first_row - rownum - 1);
                if !got || isn {
                    raise(types_error::PgError::error("missing row in percentile_cont"));
                }
                first_val = v.as_f64();
                rownum = first_row;
                second_val = first_val;
            } else if first_row == rownum {
                first_val = second_val;
            }

            if second_row > rownum {
                let (got, v, isn) = fetch_next(&mut osastate);
                if !got || isn {
                    raise(types_error::PgError::error("missing row in percentile_cont"));
                }
                second_val = v.as_f64();
                rownum += 1;
            }
            debug_assert_eq!(second_row, rownum);

            let out = if second_row > first_row {
                float8_lerp(first_val, second_val, pct[i].proportion)
            } else {
                first_val
            };
            result_val[idx] = CDatum::from_f64(out);
            result_null[idx] = false;
            i += 1;
        }
    }

    let r = build_md_result(
        fcinfo,
        param.ndim,
        &param.dims,
        &param.lbs,
        &result_val,
        &result_null,
        FLOAT8OID,
        typlen,
        typbyval,
        typalign,
    );
    restash(fcinfo, osastate);
    r
}

/// Skip `skip` tuples then fetch the next datum.
fn fetch_skip(osastate: &mut OSAPerGroupState, skip: i64) -> (bool, CDatum<'static>, bool) {
    if skip > 0 {
        let skipped =
            with_sortstate_mut(osastate.sort_id, |s| ok(tsort::tuplesort_skiptuples::call(s, skip, true)));
        if !skipped {
            raise(types_error::PgError::error("missing row in percentile"));
        }
    }
    fetch_next(osastate)
}

fn fetch_next(osastate: &mut OSAPerGroupState) -> (bool, CDatum<'static>, bool) {
    let (found, val, isnull) =
        with_sortstate_mut(osastate.sort_id, |s| ok(tsort::tuplesort_getdatum::call(s, true, true)));
    if !found {
        raise(types_error::PgError::error("missing row in percentile"));
    }
    (found, clone_cdatum(&val), isnull)
}

fn clone_cdatum<'a, 'b>(d: &CDatum<'a>) -> CDatum<'b> {
    match d {
        CDatum::ByVal(w) => CDatum::from_usize(*w),
        CDatum::ByRef(v) => {
            let mcx = leak_ctx("percentile element");
            CDatum::ByRef(super::vec_in(mcx, &v.iter().copied().collect::<alloc::vec::Vec<u8>>()))
        }
        _ => raise(types_error::PgError::error(
            "percentile multi: unexpected datum shape",
        )),
    }
}
