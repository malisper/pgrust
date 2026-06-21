//! Array-input percentile finalfns: `percentile_disc_multi_final` (3979) and
//! `percentile_cont_float8_multi_final` (3981) (`orderedsetaggs.c:731/848`).
//!
//! These take a `float8[]` of percentiles and return an array (same element type
//! as the sorted column) of the corresponding sampled values. The C
//! `construct_md_array` preserves the input array's full shape; the owned-model
//! arrayfuncs seams expose a 1-D `construct_array_builtin` (no generic
//! `construct_md_array`), so the 1-D input case — which is what
//! `percentile_cont(array[...])` / `percentile_disc(array[...])` always produce —
//! is handled exactly; a multi-dimensional percentile array (never produced by
//! the SQL syntax) is flattened to 1-D and reported via the crate docs.

use types_datum::Datum as Word;
use types_fmgr::boundary::RefPayload;
use types_fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum as CDatum;

use backend_utils_sort_tuplesort_seams as tsort;

use super::{
    arg_isnull, float8_lerp, leak_ctx, ok, perform_or_rescan, raise, ret_null, restash,
    take_group_state, with_sortstate_mut, OSAPerGroupState,
};

const FLOAT8OID: types_core::Oid = 701;
const TYPALIGN_DOUBLE: core::ffi::c_char = b'd' as core::ffi::c_char;

/// One percentile's sample plan (C `struct pct_info`).
#[derive(Clone, Copy)]
struct PctInfo {
    first_row: i64,
    second_row: i64,
    proportion: f64,
    idx: usize,
}

/// `setup_pct_info` — compute and sort the per-percentile sample rows.
fn setup_pct_info(
    percentiles: &[(Word, bool)],
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
        let p = val.as_f64();
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

/// Read the percentile-array arg (`float8[]`) off the by-ref lane: the verbatim
/// on-disk array image (so the finalfn can copy its `ndim`/`dims`/`lbound` into
/// the result, per the C `construct_md_array(..., ARR_NDIM(param), ...)`) and
/// its per-element `(Word, isnull)` pairs.
fn read_percentiles(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> Option<(alloc::vec::Vec<u8>, alloc::vec::Vec<(Word, bool)>)> {
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
    // float8: typlen=8, byval=true, align='d'.
    let elems = ok(backend_utils_adt_arrayfuncs_seams::deconstruct_array_bytes::call(
        mcx,
        &bytes,
        FLOAT8OID,
        8,
        true,
        TYPALIGN_DOUBLE,
    ));
    Some((bytes, elems.iter().copied().collect()))
}

/// Build the result array (1-D) from by-value element words of type `elmtype`.
/// `construct_array_builtin_v` is by-value-element only (it takes bare-word
/// `types_datum::Datum`s); a by-ref element type therefore is not expressible
/// here — those callers (`percentile_disc` over a by-ref column array) raise a
/// clear error rather than produce a wrong array (see crate docs).
fn build_result_array(
    fcinfo: &mut FunctionCallInfoBaseData,
    elems: &[Word],
    elmtype: types_core::Oid,
) -> Word {
    let mcx = leak_ctx("percentile result array");
    let arr = ok(
        backend_utils_adt_arrayfuncs_seams::construct_array_builtin_v::call(mcx, elems, elmtype),
    );
    // The constructed array is a by-ref varlena (DatumV::ByRef); hand it back on
    // the Varlena result lane.
    match arr {
        CDatum::ByRef(v) => {
            fcinfo.set_ref_result(RefPayload::Varlena(v.iter().copied().collect()));
            Word::from_usize(0)
        }
        _ => raise(types_error::PgError::error(
            "percentile multi finalfn: constructed array is not a by-ref varlena",
        )),
    }
}

/// Build the result array with the **same shape** as the input percentile array
/// (C `construct_md_array(result_datum, result_isnull, ARR_NDIM(param),
/// ARR_DIMS(param), ARR_LBOUND(param), sortColType, typLen, typByVal,
/// typAlign)`): `input_bytes` is the verbatim input `float8[]` image whose
/// `ndim`/`dims`/`lbound` are copied, `nulls` is the per-element null bitmap.
#[allow(clippy::too_many_arguments)]
fn build_md_result_array(
    fcinfo: &mut FunctionCallInfoBaseData,
    input_bytes: &[u8],
    elems: &[Word],
    nulls: &[bool],
    elmtype: types_core::Oid,
    typ_len: i16,
    typ_by_val: bool,
    typ_align: i8,
) -> Word {
    let mcx = leak_ctx("percentile result md-array");
    let arr = ok(
        backend_utils_adt_arrayfuncs_seams::construct_md_array_like_input_v::call(
            mcx,
            input_bytes,
            elems,
            nulls,
            elmtype,
            typ_len,
            typ_by_val,
            typ_align as core::ffi::c_char,
        ),
    );
    match arr {
        CDatum::ByRef(v) => {
            fcinfo.set_ref_result(RefPayload::Varlena(v.iter().copied().collect()));
            Word::from_usize(0)
        }
        _ => raise(types_error::PgError::error(
            "percentile multi finalfn: constructed array is not a by-ref varlena",
        )),
    }
}

/// Construct an empty array result of `elmtype` (C `construct_empty_array`),
/// expressed as a 0-element 1-D array.
fn empty_array(fcinfo: &mut FunctionCallInfoBaseData, elmtype: types_core::Oid) -> Word {
    build_result_array(fcinfo, &[], elmtype)
}

/// Extract a by-value element word from a sorted `CDatum`. By-ref element types
/// are not expressible through the by-value `construct_array_builtin_v` seam, so
/// they raise a clear error (the common percentile array element types — float8,
/// numeric-as-value, int — are by-value; see crate docs).
fn elem_word(d: &CDatum<'_>, by_val: bool) -> Word {
    if !by_val {
        raise(types_error::PgError::error(
            "percentile array over a by-reference element type is not supported by this \
             ordered-set port (construct_array_builtin is by-value-element only)",
        ));
    }
    Word::from_usize(d.as_usize())
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
    let Some((input_bytes, percentiles)) = read_percentiles(fcinfo) else {
        restash(fcinfo, osastate);
        return ret_null(fcinfo);
    };
    let elmtype = osastate.qstate.sort_col_type;
    let typ_len = osastate.qstate.typ_len;
    let typ_align = osastate.qstate.typ_align;
    let by_val = osastate.qstate.typ_by_val;
    if percentiles.is_empty() {
        let r = empty_array(fcinfo, elmtype);
        restash(fcinfo, osastate);
        return r;
    }

    let pct = setup_pct_info(&percentiles, osastate.number_of_rows, false);
    let n = percentiles.len();
    let mut result: alloc::vec::Vec<ResultSlot> = alloc::vec![ResultSlot::Null; n];

    // NULLs sort to the front on row=0.
    let mut i = 0usize;
    while i < n && pct[i].first_row == 0 {
        result[pct[i].idx] = ResultSlot::Null;
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
                let (got, v, isn) = fetch_skip(&mut osastate, target_row - rownum - 1);
                val = clone_cdatum(&v);
                isnull = isn;
                let _ = got;
                rownum = target_row;
            }
            result[idx] = if isnull {
                ResultSlot::Null
            } else {
                ResultSlot::Val(elem_word(&val, by_val))
            };
            i += 1;
        }
    }

    let (elems, nulls) = finalize_slots(&result);
    let r = build_md_result_array(
        fcinfo,
        &input_bytes,
        &elems,
        &nulls,
        elmtype,
        typ_len,
        by_val,
        typ_align,
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
    let Some((input_bytes, percentiles)) = read_percentiles(fcinfo) else {
        restash(fcinfo, osastate);
        return ret_null(fcinfo);
    };
    if percentiles.is_empty() {
        let r = empty_array(fcinfo, FLOAT8OID);
        restash(fcinfo, osastate);
        return r;
    }

    let pct = setup_pct_info(&percentiles, osastate.number_of_rows, true);
    let n = percentiles.len();
    let mut result: alloc::vec::Vec<ResultSlot> = alloc::vec![ResultSlot::Null; n];

    let mut i = 0usize;
    while i < n && pct[i].first_row == 0 {
        result[pct[i].idx] = ResultSlot::Null;
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
            result[idx] = ResultSlot::Val(Word::from_f64(out));
            i += 1;
        }
    }

    let (elems, nulls) = finalize_slots(&result);
    // float8: typlen=8, byval=true, align='d' (C `sortColType`/`typLen`/etc are
    // float8 for percentile_cont).
    let r = build_md_result_array(
        fcinfo,
        &input_bytes,
        &elems,
        &nulls,
        FLOAT8OID,
        8,
        true,
        TYPALIGN_DOUBLE as i8,
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

#[derive(Clone, Copy)]
enum ResultSlot {
    Null,
    Val(Word),
}

/// Materialize the per-index result slots into the row-major
/// `(element-word, isnull)` vectors that feed `construct_md_array` — the NULL
/// slots (a NULL percentile input, or a NULL sampled value) carry an
/// `isnull=true` flag, so the result array gets a real null bitmap (C
/// `result_isnull`) rather than a spurious zero element.
fn finalize_slots(slots: &[ResultSlot]) -> (alloc::vec::Vec<Word>, alloc::vec::Vec<bool>) {
    let mut elems = alloc::vec::Vec::with_capacity(slots.len());
    let mut nulls = alloc::vec::Vec::with_capacity(slots.len());
    for s in slots {
        match s {
            ResultSlot::Null => {
                elems.push(Word::from_usize(0));
                nulls.push(true);
            }
            ResultSlot::Val(w) => {
                elems.push(*w);
                nulls.push(false);
            }
        }
    }
    (elems, nulls)
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
