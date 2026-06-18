//! The catalog/fmgr/array/typcache *halves* of `json.c`'s (and `jsonb.c`'s)
//! `datum_to_json_internal` / `array_to_json_internal` / `composite_to_json`
//! renderers, exposed to the `json`/`jsonb` crates (this crate's cycle partners,
//! which sit *below* `jsonfuncs` in the dep graph and so can only reach this
//! work through the `backend-utils-adt-jsonfuncs-seams` seam crate).
//!
//! These five seams were declared in the `jsonfuncs` seam crate by the `json.c`
//! porter because `json_categorize_type` (their neighbour) lives here; they are
//! `OidOutputFunctionCall` / `OidFunctionCall1` (fmgr.c), a varlena detoast
//! (`TextDatumGetCString`), `deconstruct_array` (arrayfuncs.c) + the json
//! element classification, and the inline composite walk
//! (`lookup_rowtype_tupdesc` + `heap_deform_tuple` + `json_categorize_type`).
//! `jsonfuncs` already depends on every one of those owners (fmgr-core,
//! arrayfuncs, typcache, the varlena/fmgr seam crates), so it is the faithful
//! provider: this module implements them and `seams_install` wires them.

extern crate alloc;

use alloc::vec::Vec;

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_json::{ArrayForJson, CompositeFieldForJson};
use types_tuple::heaptuple::{HeapTupleHeaderGetTypMod, HeapTupleHeaderGetTypeId};
use types_tuple::Datum;

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_utils_adt_arrayfuncs::foundation;
use backend_utils_adt_varlena_seams as varlena_seams;
use backend_utils_cache_lsyscache::type_::get_typlenbyvalalign;
use backend_utils_cache_typcache::lookup_rowtype_tupdesc;
use backend_utils_fmgr_fmgr_seams as fmgr_seams;

use crate::categorize::json_categorize_type;

/// `OidOutputFunctionCall(outfuncoid, val)` (fmgr.c): the resolved type output
/// function's text representation of `val` (NUL-excluded bytes). Routes to the
/// fmgr-core `oid_output_function_call` seam (the real owner).
pub fn output_function_call<'mcx>(
    mcx: Mcx<'mcx>,
    outfuncoid: Oid,
    val: &Datum<'mcx>,
) -> PgResult<Vec<u8>> {
    let bytes = fmgr_seams::oid_output_function_call::call(mcx, outfuncoid, &val.clone_in(mcx)?)?;
    Ok(bytes.as_slice().to_vec())
}

/// `jsontext = DatumGetTextPP(OidFunctionCall1(outfuncoid, val))` then
/// `VARDATA_ANY`/`VARSIZE_ANY_EXHDR` (json.c `JSONTYPE_CAST` arm): invoke the
/// explicit cast-to-json function and return its `text` result's payload bytes
/// (already JSON). `OidFunctionCall1` runs under the default (invalid)
/// collation, matching `FunctionCall1Coll(..., InvalidOid, ...)`.
pub fn cast_function_call<'mcx>(
    mcx: Mcx<'mcx>,
    outfuncoid: Oid,
    val: &Datum<'mcx>,
) -> PgResult<Vec<u8>> {
    let jsontext = fmgr_seams::function_call1_coll_datum::call(
        mcx,
        outfuncoid,
        types_core::InvalidOid,
        val.clone_in(mcx)?,
    )?;
    // DatumGetTextPP + VARDATA_ANY/VARSIZE_ANY_EXHDR: detoast the text varlena
    // and copy out its NUL-free payload bytes.
    let payload = varlena_seams::text_to_cstring_v::call(mcx, &jsontext)?;
    Ok(payload.as_str().as_bytes().to_vec())
}

/// The special-case text output (`F_TEXTOUT`/`F_VARCHAROUT`/`F_BPCHAROUT`) of
/// `datum_to_json_internal`'s default arm: `TextDatumGetCString(val)` —
/// detoast `val` and return its NUL-free `text` payload bytes (the caller then
/// escapes them via `escape_json_with_len`).
pub fn text_datum_bytes<'mcx>(mcx: Mcx<'mcx>, val: &Datum<'mcx>) -> PgResult<Vec<u8>> {
    let payload = varlena_seams::text_to_cstring_v::call(mcx, val)?;
    Ok(payload.as_str().as_bytes().to_vec())
}

/// The catalog/`array.c` half of `array_to_json_internal` (json.c:473):
/// `get_typlenbyvalalign(element_type)` + `json_categorize_type(element_type)` +
/// `deconstruct_array`. Returns the element classification and the flat
/// element/null vectors plus dimensionality; the structural `[ ... ]` assembly
/// (`array_dim_to_json`) stays in the `json`/`jsonb` crate.
pub fn deconstruct_array<'mcx>(mcx: Mcx<'mcx>, array: &Datum<'mcx>) -> PgResult<ArrayForJson<'mcx>> {
    // ArrayType *v = DatumGetArrayTypeP(array): the array Datum is a by-reference
    // varlena image; detoast it to the (possibly compressed/external) array.
    let v = backend_access_common_detoast_seams::detoast_attr::call(mcx, array.as_ref_bytes())?;

    // element_type = ARR_ELEMTYPE(v); ndim = ARR_NDIM(v); dim = ARR_DIMS(v);
    let element_type = foundation::arr_elemtype(&v);
    let ndim = foundation::arr_ndim(&v);
    let dims = foundation::arr_dims(mcx, &v)?;

    // get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
    let attrs = get_typlenbyvalalign(element_type)?;

    // json_categorize_type(element_type, false, &tcategory, &outfuncoid);
    let (element_tcategory, element_outfuncoid) = json_categorize_type(element_type, false)?;

    // deconstruct_array(v, element_type, typlen, typbyval, typalign, &elements,
    // &nulls, &nitems): walk the array buffer mirroring arrayfuncs.c's
    // `deconstruct_array`, but materialize each element as the canonical
    // `Datum<'mcx>` the json/jsonb renderer drives directly (a by-value element
    // rides `ByVal`; a by-reference element's bytes ride `ByRef`; the C path
    // would hand `OutputFunctionCall` a `PointerGetDatum` into the buffer).
    let elmlen = attrs.typlen as i32;
    let elmbyval = attrs.typbyval;
    let elmalign = attrs.typalign as u8;

    // nelems = ArrayGetNItems(ndim, dims): the flat element count. The overflow
    // guard is enforced upstream by arrayfuncs; mirror the consumers' own
    // dim-product (they recompute it for the `nitems <= 0` early-return).
    let nelems: i64 = if ndim <= 0 {
        0
    } else {
        let mut n: i64 = 1;
        for &d in dims.as_slice() {
            n *= d as i64;
        }
        n
    };

    let mut elements: Vec<Datum<'mcx>> = Vec::with_capacity(nelems.max(0) as usize);
    let mut nulls: Vec<bool> = Vec::with_capacity(nelems.max(0) as usize);

    let mut p = foundation::arr_data_ptr_off(&v);
    let bitmap = foundation::arr_nullbitmap_off(&v);
    let mut bitmap_byte = bitmap;
    let mut bitmask: i32 = 1;

    for _ in 0..nelems {
        let is_null_here = match bitmap_byte {
            Some(b) => (v[b] as i32 & bitmask) == 0,
            None => false,
        };
        if is_null_here {
            elements.push(Datum::null());
            nulls.push(true);
        } else {
            // elems[i] = fetch_att(p, elmbyval, elmlen): a by-value scalar word,
            // or — for a by-reference element — the bytes at the current offset.
            let d: Datum<'mcx> = if elmbyval {
                let word = foundation::fetch_att(&v, p, true, elmlen);
                Datum::ByVal(word.as_usize())
            } else {
                let len = if elmlen > 0 {
                    elmlen as usize
                } else {
                    // varlena (-1) or cstring (-2): att_addlength_pointer reads
                    // the length from the element header.
                    foundation::att_addlength_pointer(p, elmlen, &v, p) - p
                };
                Datum::ByRef(mcx::slice_in(mcx, &v[p..p + len])?)
            };
            elements.push(d);
            nulls.push(false);

            // p = att_addlength_pointer(p, elmlen, p);
            p = foundation::att_addlength_pointer(p, elmlen, &v, p);
            // p = att_align_nominal(p, elmalign);
            p = foundation::att_align_nominal(p, elmalign);
        }

        if let Some(b) = bitmap_byte.as_mut() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                *b += 1;
                bitmask = 1;
            }
        }
    }

    Ok(ArrayForJson {
        ndim,
        dims: dims.as_slice().to_vec(),
        elements,
        nulls,
        element_tcategory,
        element_outfuncoid,
    })
}

/// `deconstruct_array(in_array, TEXTOID, -1, false, TYPALIGN_INT, ...)` for
/// `json_object` (json.c:1405) / `json_object_two_arg` (json.c:1489): walk a
/// `text[]` array image and return its `ndim` / `dims` plus, per element, the
/// header-stripped `text` payload bytes (`VARDATA_ANY` over
/// `VARSIZE_ANY_EXHDR`) or `None` for a SQL-NULL element. Mirrors the per-byte
/// element walk in `deconstruct_array` above but with the fixed text element
/// triple (`typlen=-1`, `typbyval=false`, `typalign='i'`).
pub fn deconstruct_text_array<'mcx>(
    mcx: Mcx<'mcx>,
    array: &Datum<'mcx>,
) -> PgResult<(i32, Vec<i32>, Vec<Option<Vec<u8>>>)> {
    let v = backend_access_common_detoast_seams::detoast_attr::call(mcx, array.as_ref_bytes())?;

    let ndim = foundation::arr_ndim(&v);
    let dims = foundation::arr_dims(mcx, &v)?;

    // text element triple: typlen = -1 (varlena), typbyval = false,
    // typalign = TYPALIGN_INT ('i').
    const ELMLEN: i32 = -1;
    const ELMALIGN: u8 = b'i';

    let nelems: i64 = if ndim <= 0 {
        0
    } else {
        let mut n: i64 = 1;
        for &d in dims.as_slice() {
            n *= d as i64;
        }
        n
    };

    let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(nelems.max(0) as usize);

    let mut p = foundation::arr_data_ptr_off(&v);
    let bitmap = foundation::arr_nullbitmap_off(&v);
    let mut bitmap_byte = bitmap;
    let mut bitmask: i32 = 1;

    for _ in 0..nelems {
        let is_null_here = match bitmap_byte {
            Some(b) => (v[b] as i32 & bitmask) == 0,
            None => false,
        };
        if is_null_here {
            out.push(None);
        } else {
            // VARDATA_ANY(p) / VARSIZE_ANY_EXHDR(p): a 1-byte-header (short)
            // varlena's payload starts at off+1 (len = VARSIZE_1B - 1); a
            // 4-byte-header varlena's payload starts at off+4 (len =
            // VARSIZE_4B - 4). Elements are never externally toasted (the array
            // image was detoasted above).
            let (data_off, data_len) = if foundation::varatt_is_1b(&v, p) {
                (p + 1, foundation::varsize_1b(&v, p) - 1)
            } else {
                (p + 4, foundation::varsize_4b(&v, p) - 4)
            };
            out.push(Some(v[data_off..data_off + data_len].to_vec()));

            p = foundation::att_addlength_pointer(p, ELMLEN, &v, p);
            p = foundation::att_align_nominal(p, ELMALIGN);
        }

        if let Some(b) = bitmap_byte.as_mut() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                *b += 1;
                bitmask = 1;
            }
        }
    }

    Ok((ndim, dims.as_slice().to_vec(), out))
}

/// The catalog/typcache half of `composite_to_json` (json.c:520):
/// `lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId/TypMod)`, the per-attribute
/// `heap_getattr`, and the per-attribute `json_categorize_type`. Returns one
/// entry per *non-dropped* attribute (dropped attributes already skipped,
/// matching the C `if (att->attisdropped) continue;`); the `{ ... }` assembly
/// stays in the `json`/`jsonb` crate.
pub fn walk_composite<'mcx>(
    mcx: Mcx<'mcx>,
    composite: &Datum<'mcx>,
) -> PgResult<Vec<CompositeFieldForJson<'mcx>>> {
    // td = DatumGetHeapTupleHeader(composite); the canonical composite value is
    // a materialized FormedTuple (header + data).
    let tuple = composite
        .as_composite()
        .expect("walk_composite: value is not a composite Datum")
        .clone_in(mcx)?;
    let td = tuple
        .tuple
        .t_data
        .as_ref()
        .expect("walk_composite: composite tuple has no t_data");

    // tupType = HeapTupleHeaderGetTypeId(td); tupTypmod = ...GetTypMod(td);
    let tup_type = HeapTupleHeaderGetTypeId(td);
    let tup_typmod = HeapTupleHeaderGetTypMod(td);

    // tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    let tupdesc = lookup_rowtype_tupdesc(mcx, tup_type, tup_typmod)?;

    // heap_getattr over all attributes — deform the tuple once (the C loop calls
    // heap_getattr per attribute; deforming once is the faithful equivalent).
    let deformed = heap_deform_tuple(mcx, &tuple.tuple, &tupdesc, &tuple.data)?;

    let natts = tupdesc.natts as usize;
    let mut out: Vec<CompositeFieldForJson<'mcx>> = Vec::with_capacity(natts);

    for i in 0..natts {
        let att = tupdesc.attr(i);

        // if (att->attisdropped) continue;
        if att.attisdropped {
            continue;
        }

        let attname = att.attname.name_str().to_vec();

        // val = heap_getattr(tuple, i + 1, tupdesc, &isnull);
        // Attributes beyond the stored natts read as NULL (heap_deform_tuple's
        // missing-attribute handling); guard the index for that case.
        let (val, is_null) = match deformed.get(i) {
            Some((d, n)) => (d.clone_in(mcx)?, *n),
            None => (Datum::null(), true),
        };

        // if (isnull) { JSONTYPE_NULL, InvalidOid } else json_categorize_type(...)
        let (tcategory, outfuncoid) = if is_null {
            (types_json::JsonTypeCategory::JSONTYPE_NULL, types_core::InvalidOid)
        } else {
            json_categorize_type(att.atttypid, false)?
        };

        out.push(CompositeFieldForJson {
            attname,
            val,
            is_null,
            tcategory,
            outfuncoid,
        });
    }

    Ok(out)
}
