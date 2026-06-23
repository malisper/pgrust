//! `arraysubs.c` execution callbacks — the array-type subscripting `exec_setup`
//! method bodies (`array_subscript_fetch`, `array_subscript_fetch_slice`,
//! `array_subscript_assign`, `array_subscript_assign_slice`, and the `_old`
//! fetch variants).
//!
//! In C these are the `SubscriptExecSteps` callbacks `array_exec_setup`
//! installs; the EEOP_SBSREF_* interpreter steps dispatch them. The
//! `exec_setup` selection, the `sbs_check_subscripts` integer conversion, and
//! the workspace layout live in the executor owner (execExpr /
//! execExprInterp); this module provides the array-primitive-backed FETCH /
//! ASSIGN bodies, called by the interpreter through per-callback seams.
//!
//! # Datum boundary
//!
//! The C callbacks read the container from `*op->resvalue` (a `Datum`) and
//! place the result back there. In the owned model the container and result
//! cross as the canonical [`DatumV`] (`ByVal`/`ByRef`). Where the underlying
//! array primitive (`array_get_element` / `array_set_element`) takes/returns a
//! bare machine-word `Datum` (a pointer for by-reference values), we bridge at
//! this boundary exactly as C does: a by-reference value's bytes are addressed
//! by their pointer word, and a by-reference result is reconstructed by reading
//! the element's bytes out of the (already-detoasted, flat) array buffer.

use mcx::{Mcx, PgVec};
use datum::datum::Datum;
use types_error::PgResult;
use types_tuple::heaptuple::Datum as DatumV;

use detoast_seams as detoast_seam;

use crate::construct::construct_empty_array;
use crate::element_slice::{array_get_element, array_get_slice, array_set_element, array_set_slice};
use crate::foundation::att_addlength_pointer;

/// Borrow a canonical container value's flat array bytes (`DatumGetPointer`).
/// A NULL/by-value container has no array buffer; callers guard against that.
fn container_bytes<'a>(container: &'a DatumV<'a>) -> &'a [u8] {
    container.as_ref_bytes()
}

/// `DatumGetArrayTypeP(arraydatum)` — detoast the container's varlena bytes into
/// a flat, full-4-byte-header `ArrayType` image in `mcx`. The C array subscript
/// primitives (`array_get_element`/`array_get_slice`/`array_set_element`/...)
/// all start with `DatumGetArrayTypeP`, which is `PG_DETOAST_DATUM`: it unpacks a
/// SHORT (1-byte) varlena header and inlines/decompresses a toasted value so the
/// fixed-offset header fields (vl_len/ndim/dataoffset/elemtype) read correctly.
///
/// A container read straight out of a heap tuple (e.g. `pg_proc.proallargtypes`,
/// a small `oid[]`) arrives SHORT-header packed; reading its `ArrayType` header
/// without detoasting mis-reads `ndim`/dims and the subscript silently returns
/// NULL. `arraytyplen > 0` is a fixed-length (non-varlena) array type, which is
/// never toasted and must NOT be passed through varlena detoast — it is returned
/// verbatim.
fn detoast_container<'mcx>(
    mcx: Mcx<'mcx>,
    container: &DatumV<'mcx>,
    arraytyplen: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let bytes = container_bytes(container);
    if arraytyplen > 0 {
        let mut v = mcx::vec_with_capacity_in(mcx, bytes.len())?;
        v.extend_from_slice(bytes);
        Ok(v)
    } else {
        detoast_seam::detoast_attr::call(mcx, bytes)
    }
}

/// Normalize a canonical replacement value into a form whose flat bytes live in
/// `mcx` and can be addressed by a bare pointer word (mirroring C, where the
/// replacement value reaching `array_set_element` for a by-reference element is
/// already a `struct varlena *`/`Pointer` Datum). A `ByVal` scalar is its word
/// and needs no buffer; a `ByRef`/`Cstring` already holds flat bytes; a
/// `Composite` is serialized to its `HeapTupleHeader` Datum image (the
/// self-describing varlena image — same block C would hand to the array
/// primitive when assigning a composite array element, e.g.
/// `f4[1].if2[1]` over an array of composite type); an `Expanded` object is
/// flattened to its varlena image. `Internal` has no flat image (C never stores
/// an `internal` value into an array element) and stays a hard error.
fn flatten_replacement<'mcx>(
    mcx: Mcx<'mcx>,
    value: &DatumV<'mcx>,
) -> PgResult<DatumV<'mcx>> {
    match value {
        DatumV::ByVal(w) => Ok(DatumV::ByVal(*w)),
        DatumV::ByRef(b) => {
            let mut v = mcx::vec_with_capacity_in(mcx, b.len())?;
            v.extend_from_slice(b);
            Ok(DatumV::ByRef(v))
        }
        DatumV::Cstring(s) => Ok(DatumV::Cstring(s.clone())),
        DatumV::Composite(_) | DatumV::Expanded(_) => {
            // `as_varlena_bytes()` materializes a Composite to its datum image
            // (Cow::Owned); clone_in flattens Expanded. Use clone_in, which
            // routes Composite→Composite and Expanded→ByRef, then take the flat
            // image. Simpler: materialize the varlena bytes directly.
            let bytes = value.as_varlena_bytes();
            let mut v = mcx::vec_with_capacity_in(mcx, bytes.len())?;
            v.extend_from_slice(&bytes);
            Ok(DatumV::ByRef(v))
        }
        DatumV::Internal(_) => Err(types_error::PgError::error(
            "array_set_element: cannot store an internal-typed value into an array element",
        )),
    }
}

/// Bridge a (already flattened, see [`flatten_replacement`]) canonical
/// replacement value into the bare-word `Datum` the array primitives accept: a
/// by-value scalar is its word; a by-reference value is addressed by its bytes'
/// pointer (mirroring C `DatumGetPointer`). The bytes live in the caller's arena
/// and outlive the call.
fn value_word(value: &DatumV<'_>) -> Datum {
    match value {
        DatumV::ByVal(w) => Datum::from_usize(*w),
        DatumV::ByRef(b) => Datum::from_usize(b.as_ptr() as usize),
        DatumV::Cstring(s) => Datum::from_usize(s.as_ptr() as usize),
        DatumV::Composite(_) | DatumV::Expanded(_) | DatumV::Internal(_) => {
            panic!("arraysubs::value_word: replacement value must be flattened via flatten_replacement first")
        }
    }
}

/// Reconstruct a canonical element `DatumV` from the bare-word result of
/// `array_get_element` over the array buffer `array`. For a by-value element the
/// word is the value; for a by-reference element the word is the in-buffer
/// offset (see `fetch_att`), so we copy the element's bytes (length via
/// `att_addlength_pointer`) into `mcx`.
fn element_from_word<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    word: Datum,
    isnull: bool,
    elmbyval: bool,
    elmlen: i16,
) -> PgResult<DatumV<'mcx>> {
    if isnull {
        return Ok(DatumV::null());
    }
    if elmbyval {
        Ok(DatumV::ByVal(word.as_usize()))
    } else {
        let off = word.as_usize();
        let end = att_addlength_pointer(off, elmlen as i32, array, off);
        let mut v = mcx::vec_with_capacity_in(mcx, end - off)?;
        v.extend_from_slice(&array[off..end]);
        Ok(DatumV::ByRef(v))
    }
}

/// `array_subscript_fetch` (arraysubs.c): fetch one array element. The container
/// is known non-NULL (fetch_strict). Returns `(element, isnull)`.
pub fn array_subscript_fetch<'mcx>(
    mcx: Mcx<'mcx>,
    container: DatumV<'mcx>,
    numupper: i32,
    upperindex: &[i32],
    refattrlength: i16,
    refelemlength: i16,
    refelembyval: bool,
    refelemalign: u8,
) -> PgResult<(DatumV<'mcx>, bool)> {
    // C: *op->resvalue = array_get_element(*op->resvalue, numupper,
    //        workspace->upperindex, refattrlength, refelemlength, refelembyval,
    //        refelemalign, op->resnull);
    // C: DatumGetArrayTypeP(*op->resvalue) — detoast a SHORT-header / toasted
    // container before reading its ArrayType header.
    let array_buf = detoast_container(mcx, &container, refattrlength as i32)?;
    let array = array_buf.as_slice();
    let (word, isnull) = array_get_element(
        mcx,
        array,
        numupper,
        upperindex,
        refattrlength as i32,
        refelemlength as i32,
        refelembyval,
        refelemalign,
    )?;
    let elem = element_from_word(mcx, array, word, isnull, refelembyval, refelemlength)?;
    Ok((elem, isnull))
}

/// `array_subscript_fetch_slice` (arraysubs.c): fetch an array slice. The
/// container is known non-NULL; a slice of a non-null array is never null.
pub fn array_subscript_fetch_slice<'mcx>(
    mcx: Mcx<'mcx>,
    container: DatumV<'mcx>,
    numupper: i32,
    upperindex: &[i32],
    lowerindex: &[i32],
    upperprovided: &[bool],
    lowerprovided: &[bool],
    refattrlength: i16,
    refelemlength: i16,
    refelembyval: bool,
    refelemalign: u8,
) -> PgResult<(DatumV<'mcx>, bool)> {
    // C: *op->resvalue = array_get_slice(*op->resvalue, numupper,
    //        workspace->upperindex, workspace->lowerindex, upperprovided,
    //        lowerprovided, refattrlength, refelemlength, refelembyval,
    //        refelemalign);
    // C: DatumGetArrayTypeP(*op->resvalue) — detoast before reading the header.
    let array_buf = detoast_container(mcx, &container, refattrlength as i32)?;
    let array = array_buf.as_slice();
    let result = array_get_slice(
        mcx,
        array,
        numupper,
        upperindex,
        lowerindex,
        upperprovided,
        lowerprovided,
        refattrlength as i32,
        refelemlength as i32,
        refelembyval,
        refelemalign,
    )?;
    Ok((DatumV::ByRef(result), false))
}

/// `array_subscript_assign` (arraysubs.c): assign one array element, returning
/// the new whole array value (never NULL).
#[allow(clippy::too_many_arguments)]
pub fn array_subscript_assign<'mcx>(
    mcx: Mcx<'mcx>,
    container: DatumV<'mcx>,
    container_null: bool,
    numupper: i32,
    upperindex: &[i32],
    replacevalue: DatumV<'mcx>,
    replacenull: bool,
    refelemtype: types_core::Oid,
    refattrlength: i16,
    refelemlength: i16,
    refelembyval: bool,
    refelemalign: u8,
) -> PgResult<(DatumV<'mcx>, bool)> {
    // C: for a fixed-length array type, both the original array and the value
    //    must be non-NULL, else punt and return the original array.
    if refattrlength > 0 && (container_null || replacenull) {
        return Ok((container, container_null));
    }

    // C: for a varlena array, a NULL original array becomes an empty array.
    let (array_owned, is_null_out): (DatumV<'mcx>, bool) = if container_null {
        (DatumV::ByRef(construct_empty_array(mcx, refelemtype)?), false)
    } else {
        (container, false)
    };

    // C: *op->resvalue = array_set_element(arraySource, numupper, upperindex,
    //        replacevalue, replacenull, refattrlength, refelemlength,
    //        refelembyval, refelemalign);
    // C: DatumGetArrayTypeP(arraySource) — detoast a SHORT-header / toasted
    // source array before array_set_element reads its ArrayType header.
    let array_buf = detoast_container(mcx, &array_owned, refattrlength as i32)?;
    let array = array_buf.as_slice();
    // Materialize a by-reference / composite / expanded replacement into flat
    // arena bytes addressable by a pointer word (C hands `array_set_element` a
    // `Pointer` Datum for a by-reference element). Kept alive across the call.
    let replace_flat = flatten_replacement(mcx, &replacevalue)?;
    let result = array_set_element(
        mcx,
        array,
        numupper,
        upperindex,
        value_word(&replace_flat),
        replacenull,
        refattrlength as i32,
        refelemlength as i32,
        refelembyval,
        refelemalign,
    )?;
    let _ = is_null_out;
    Ok((DatumV::ByRef(result), false))
}

/// `array_subscript_assign_slice` (arraysubs.c): assign an array slice.
#[allow(clippy::too_many_arguments)]
pub fn array_subscript_assign_slice<'mcx>(
    mcx: Mcx<'mcx>,
    container: DatumV<'mcx>,
    container_null: bool,
    numupper: i32,
    upperindex: &[i32],
    lowerindex: &[i32],
    upperprovided: &[bool],
    lowerprovided: &[bool],
    replacevalue: DatumV<'mcx>,
    replacenull: bool,
    refelemtype: types_core::Oid,
    refattrlength: i16,
    refelemlength: i16,
    refelembyval: bool,
    refelemalign: u8,
) -> PgResult<(DatumV<'mcx>, bool)> {
    if refattrlength > 0 && (container_null || replacenull) {
        return Ok((container, container_null));
    }
    let array_owned: DatumV<'mcx> = if container_null {
        DatumV::ByRef(construct_empty_array(mcx, refelemtype)?)
    } else {
        container
    };
    // C: DatumGetArrayTypeP(arraySource) — detoast before reading the header.
    let array_buf = detoast_container(mcx, &array_owned, refattrlength as i32)?;
    let array = array_buf.as_slice();
    // The replacement value for a slice assignment is itself an array (by-ref
    // bytes); a NULL source is a no-op handled inside array_set_slice (it reads
    // an empty slice). Pass an empty buffer when null to avoid touching it.
    // C also detoasts the replacement (DatumGetArrayTypeP(srcArrayDatum)).
    let empty: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    let src_array_buf: PgVec<'mcx, u8> = if replacenull {
        empty
    } else {
        detoast_container(mcx, &replacevalue, refattrlength as i32)?
    };
    let src_array: &[u8] = src_array_buf.as_slice();
    let result = array_set_slice(
        mcx,
        array,
        numupper,
        upperindex,
        lowerindex,
        upperprovided,
        lowerprovided,
        src_array,
        replacenull,
        refattrlength as i32,
        refelemlength as i32,
        refelembyval,
        refelemalign,
    )?;
    Ok((DatumV::ByRef(result), false))
}

/// `array_subscript_fetch_old` (arraysubs.c): fetch the existing element for a
/// nested assignment. Like the regular fetch but must cope with a NULL
/// container (returns NULL) and the result goes to prevvalue/prevnull.
#[allow(clippy::too_many_arguments)]
pub fn array_subscript_fetch_old<'mcx>(
    mcx: Mcx<'mcx>,
    container: DatumV<'mcx>,
    container_null: bool,
    numupper: i32,
    upperindex: &[i32],
    refattrlength: i16,
    refelemlength: i16,
    refelembyval: bool,
    refelemalign: u8,
) -> PgResult<(DatumV<'mcx>, bool)> {
    // C: if (*op->resnull) { prevvalue = 0; prevnull = true; }
    if container_null {
        return Ok((DatumV::null(), true));
    }
    array_subscript_fetch(
        mcx,
        container,
        numupper,
        upperindex,
        refattrlength,
        refelemlength,
        refelembyval,
        refelemalign,
    )
}

/// `array_subscript_fetch_old_slice` (arraysubs.c): fetch the existing slice for
/// a nested assignment. Slices of non-null arrays are never null.
#[allow(clippy::too_many_arguments)]
pub fn array_subscript_fetch_old_slice<'mcx>(
    mcx: Mcx<'mcx>,
    container: DatumV<'mcx>,
    container_null: bool,
    numupper: i32,
    upperindex: &[i32],
    lowerindex: &[i32],
    upperprovided: &[bool],
    lowerprovided: &[bool],
    refattrlength: i16,
    refelemlength: i16,
    refelembyval: bool,
    refelemalign: u8,
) -> PgResult<(DatumV<'mcx>, bool)> {
    if container_null {
        return Ok((DatumV::null(), true));
    }
    array_subscript_fetch_slice(
        mcx,
        container,
        numupper,
        upperindex,
        lowerindex,
        upperprovided,
        lowerprovided,
        refattrlength,
        refelemlength,
        refelembyval,
        refelemalign,
    )
}
