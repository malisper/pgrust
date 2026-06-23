//! Port of `src/backend/utils/adt/arrayutils.c` — subscript / dimension
//! support math, plus the type-aware helper `ArrayGetIntegerTypmods`.
//!
//! The pure-math routines (`ArrayGetOffset`, `ArrayGetNItems[Safe]`,
//! `ArrayCheckBounds[Safe]`, `mda_*`) have no external dependencies and are
//! ported 1:1. `ArrayGetIntegerTypmods` deconstructs a `cstring[]` array and
//! parses each element with `pg_strtoint32`; its element decode goes through
//! the construct/deconstruct port in `backend-utils-adt-arrayfuncs`.

use mcx::Mcx;
use datum::datum::Datum;
use types_error::{
    ereturn, PgResult, SoftErrorContext, ERRCODE_ARRAY_ELEMENT_ERROR, ERRCODE_ARRAY_SUBSCRIPT_ERROR,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};

use utils_error::ereport;

use arrayfuncs::construct::{array_contains_nulls, deconstruct_array_builtin};
use arrayfuncs::foundation::{arr_elemtype, arr_ndim, CSTRINGOID, MAX_ARRAY_SIZE};
use numutils::pg_strtoint32;


/// `ArrayGetOffset(n, dim, lb, indx)` — convert a subscript list into a linear
/// element number (from 0). Caller has range-checked, so no overflow possible.
pub fn array_get_offset(n: i32, dim: &[i32], lb: &[i32], indx: &[i32]) -> i32 {
    let mut scale: i32 = 1;
    let mut offset: i32 = 0;
    let mut i = n - 1;
    while i >= 0 {
        let iu = i as usize;
        offset += (indx[iu] - lb[iu]) * scale;
        scale *= dim[iu];
        i -= 1;
    }
    offset
}

/// `ArrayGetNItems(ndim, dims)` — total element count; thin wrapper over the
/// safe form with no soft-error context (overflow throws).
pub fn array_get_n_items(ndim: i32, dims: &[i32]) -> PgResult<i32> {
    array_get_n_items_safe(ndim, dims, None)
}

/// `ArrayGetNItemsSafe(ndim, dims, escontext)` — total element count, routing
/// overflow / out-of-range into an optional soft-error context (`ereturn`);
/// returns `-1` after a soft error.
pub fn array_get_n_items_safe(
    ndim: i32,
    dims: &[i32],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<i32> {
    if ndim <= 0 {
        return Ok(0);
    }
    let mut ret: i32 = 1;
    for i in 0..ndim as usize {
        // A negative dimension implies that UB-LB overflowed ...
        if dims[i] < 0 {
            return ereturn(escontext.as_deref_mut(), -1, size_exceeds_error());
        }

        let prod: i64 = ret as i64 * dims[i] as i64;

        ret = prod as i32;
        if ret as i64 != prod {
            return ereturn(escontext.as_deref_mut(), -1, size_exceeds_error());
        }
    }
    debug_assert!(ret >= 0);
    if ret as usize > MAX_ARRAY_SIZE {
        return ereturn(escontext, -1, size_exceeds_error());
    }
    Ok(ret)
}

/// `errmsg("array size exceeds the maximum allowed (%d)", (int) MaxArraySize)`
/// with `ERRCODE_PROGRAM_LIMIT_EXCEEDED`.
fn size_exceeds_error() -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .errmsg(format!(
            "array size exceeds the maximum allowed ({})",
            MAX_ARRAY_SIZE as i32
        ))
        .into_error()
}

/// `ArrayCheckBounds(ndim, dims, lb)` — verify proposed lower-bound values;
/// throws on overflow. Thin wrapper over the safe form.
pub fn array_check_bounds(ndim: i32, dims: &[i32], lb: &[i32]) -> PgResult<()> {
    array_check_bounds_safe(ndim, dims, lb, None)?;
    Ok(())
}

/// `ArrayCheckBoundsSafe(ndim, dims, lb, escontext)` — verify that `dims[i] +
/// lb[i]` is computable without overflow, routing the error into an optional
/// soft-error context. Returns `false` after a soft error.
pub fn array_check_bounds_safe(
    ndim: i32,
    dims: &[i32],
    lb: &[i32],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    for i in 0..ndim as usize {
        // C: pg_add_s32_overflow(dims[i], lb[i], &sum)
        if dims[i].checked_add(lb[i]).is_none() {
            let err = ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!("array lower bound is too large: {}", lb[i]))
                .into_error();
            return ereturn(escontext.as_deref_mut(), false, err);
        }
    }
    Ok(true)
}

/// `mda_get_range(n, span, st, endp)` — compute ranges (sub-array dimensions)
/// for an array slice. `span[i] = endp[i] - st[i] + 1`.
pub fn mda_get_range(n: i32, span: &mut [i32], st: &[i32], endp: &[i32]) {
    for i in 0..n as usize {
        span[i] = endp[i] - st[i] + 1;
    }
}

/// `mda_get_prod(n, range, prod)` — compute products of array dimensions
/// (subscript scale factors).
pub fn mda_get_prod(n: i32, range: &[i32], prod: &mut [i32]) {
    prod[(n - 1) as usize] = 1;
    let mut i = n - 2;
    while i >= 0 {
        let iu = i as usize;
        prod[iu] = prod[iu + 1] * range[iu + 1];
        i -= 1;
    }
}

/// `mda_get_offset_values(n, dist, prod, span)` — from whole-array products and
/// sub-array spans, compute the offset distances needed to step through a
/// subarray.
pub fn mda_get_offset_values(n: i32, dist: &mut [i32], prod: &[i32], span: &[i32]) {
    dist[(n - 1) as usize] = 0;
    let mut j = n - 2;
    while j >= 0 {
        let ju = j as usize;
        dist[ju] = prod[ju] - 1;
        for i in (j + 1)..n {
            let iu = i as usize;
            dist[ju] -= (span[iu] - 1) * prod[iu];
        }
        j -= 1;
    }
}

/// `mda_next_tuple(n, curr, span)` — generate the lexicographically next
/// n-tuple in `curr`, where each element is `< span`. Returns -1 if no next
/// tuple exists, else the subscript position (0..n-1) of the dimension advanced.
pub fn mda_next_tuple(n: i32, curr: &mut [i32], span: &[i32]) -> i32 {
    if n <= 0 {
        return -1;
    }

    let last = (n - 1) as usize;
    curr[last] = (curr[last] + 1) % span[last];
    let mut i = n - 1;
    while i != 0 && curr[i as usize] == 0 {
        let iu = i as usize;
        curr[iu - 1] = (curr[iu - 1] + 1) % span[iu - 1];
        i -= 1;
    }

    if i != 0 {
        return i;
    }
    if curr[0] != 0 {
        return 0;
    }

    -1
}

/// `ArrayGetIntegerTypmods(arr, n)` — verify that `arr` is a 1-D `cstring[]`
/// and return its contents converted to integers (C returns a palloc'd
/// `int32 *` and the length via `*n`).
///
/// Element decode (`deconstruct_array_builtin(arr, CSTRINGOID, ...)`) reuses
/// the construct/deconstruct port; for `cstring` the returned by-reference
/// element Datums carry the NUL-terminated string payload, fed to
/// `pg_strtoint32`.
pub fn array_get_integer_typmods<'mcx>(mcx: Mcx<'mcx>, arr: &[u8]) -> PgResult<Vec<i32>> {
    if arr_elemtype(arr) != CSTRINGOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_ARRAY_ELEMENT_ERROR)
            .errmsg("typmod array must be type cstring[]")
            .into_error());
    }
    if arr_ndim(arr) != 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            .errmsg("typmod array must be one-dimensional")
            .into_error());
    }
    if array_contains_nulls(arr) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
            .errmsg("typmod array must not contain nulls")
            .into_error());
    }

    // deconstruct_array_builtin(arr, CSTRINGOID, &elem_values, NULL, n)
    let elems = deconstruct_array_builtin(mcx, arr, CSTRINGOID)?;

    let n = elems.len();
    let mut result: Vec<i32> = Vec::new();
    result.try_reserve(n).map_err(|_| {
        ereport(ERROR)
            .errcode(types_error::ERRCODE_OUT_OF_MEMORY)
            .errmsg("out of memory")
            .into_error()
    })?;
    for &(ev, _isnull) in elems.iter() {
        // C: result[i] = pg_strtoint32(DatumGetCString(elem_values[i]));
        let s = datum_cstring(arr, ev)?;
        result.push(pg_strtoint32(&s)?);
    }

    Ok(result)
}

/// `DatumGetCString(datum)` over a `cstring` element Datum produced by
/// [`deconstruct_array_builtin`].
///
/// `cstring` is `attlen == -2` / pass-by-reference, so `fetch_att` records the
/// element as the in-buffer **offset** into the deconstructed array bytes (the
/// byte-model stand-in for C's `PointerGetDatum(T)` element address — see
/// `arrayfuncs::foundation::fetch_att`). A `cstring` is *not*
/// a varlena and is never TOASTed, so the bytes are read straight out of `arr`
/// at that offset; the payload is NUL-terminated (`DatumGetCString`), and C
/// hands the raw bytes straight to `pg_strtoint32` with no encoding gate, so we
/// do the same.
fn datum_cstring(arr: &[u8], datum: Datum) -> PgResult<String> {
    let off = datum.as_usize();
    let bytes = arr.get(off..).ok_or_else(|| {
        ereport(ERROR)
            .errcode(types_error::error::ERRCODE_DATA_CORRUPTED)
            .errmsg("corrupt cstring element offset in typmod array")
            .into_error()
    })?;
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    Ok(String::from_utf8_lossy(&bytes[..end]).into_owned())
}

/// Install every seam declared in `backend-utils-adt-arrayutils-seams` to its
/// real implementation in this crate.
pub fn init_seams() {
    use arrayutils_seams as seams;

    seams::array_get_offset::set(array_get_offset);
    seams::array_get_n_items::set(array_get_n_items);
    seams::array_check_bounds::set(array_check_bounds);
    seams::mda_get_range::set(mda_get_range);
    seams::mda_get_prod::set(mda_get_prod);
    seams::mda_get_offset_values::set(mda_get_offset_values);
    seams::mda_next_tuple::set(mda_next_tuple);
    seams::array_get_integer_typmods::set(array_get_integer_typmods_seam);
}

/// Seam adapter for [`array_get_integer_typmods`]: copy the typmod list into an
/// `mcx`-charged [`PgVec`] (the seam returns its result in the caller's context).
fn array_get_integer_typmods_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    arr: &[u8],
) -> PgResult<mcx::PgVec<'mcx, i32>> {
    let v = array_get_integer_typmods(mcx, arr)?;
    mcx::slice_in(mcx, &v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_linearizes_subscripts() {
        // 2x3 array, lower bounds [1,1]; element (2,3) -> (2-1)*3 + (3-1)*1 = 5
        let dim = [2, 3];
        let lb = [1, 1];
        let indx = [2, 3];
        assert_eq!(array_get_offset(2, &dim, &lb, &indx), 5);
    }

    #[test]
    fn nitems_multiplies_dims() {
        assert_eq!(array_get_n_items(2, &[2, 3]).unwrap(), 6);
        assert_eq!(array_get_n_items(0, &[]).unwrap(), 0);
    }

    #[test]
    fn nitems_negative_dim_is_error() {
        assert!(array_get_n_items(1, &[-1]).is_err());
    }

    #[test]
    fn nitems_overflow_is_error() {
        assert!(array_get_n_items(2, &[i32::MAX, 2]).is_err());
    }

    #[test]
    fn nitems_safe_routes_to_context() {
        let mut ctx = SoftErrorContext::new(true);
        let r = array_get_n_items_safe(1, &[-1], Some(&mut ctx)).unwrap();
        assert_eq!(r, -1);
        assert!(ctx.error_occurred());
    }

    #[test]
    fn check_bounds_overflow_is_error() {
        assert!(array_check_bounds(1, &[10], &[i32::MAX]).is_err());
        assert!(array_check_bounds(1, &[10], &[1]).is_ok());
    }

    #[test]
    fn mda_range_prod_offsets() {
        let mut span = [0; 2];
        mda_get_range(2, &mut span, &[1, 1], &[2, 3]);
        assert_eq!(span, [2, 3]);

        let mut prod = [0; 2];
        mda_get_prod(2, &[2, 3], &mut prod);
        assert_eq!(prod, [3, 1]);

        let mut dist = [0; 2];
        mda_get_offset_values(2, &mut dist, &prod, &span);
        // dist[n-1] = 0; dist[0] = prod[0]-1 - (span[1]-1)*prod[1] = 2 - 2 = 0
        assert_eq!(dist, [0, 0]);
    }

    #[test]
    fn next_tuple_iterates() {
        let span = [2, 2];
        let mut curr = [0, 0];
        assert_eq!(mda_next_tuple(2, &mut curr, &span), 1);
        assert_eq!(curr, [0, 1]);
        assert_eq!(mda_next_tuple(2, &mut curr, &span), 0);
        assert_eq!(curr, [1, 0]);
        assert_eq!(mda_next_tuple(2, &mut curr, &span), 1);
        assert_eq!(curr, [1, 1]);
        assert_eq!(mda_next_tuple(2, &mut curr, &span), -1);
    }

    /// Hand-build a 1-D, no-NULL `cstring[]` `ArrayType` whose elements are the
    /// given decimal strings, matching the on-disk layout
    /// `array_get_integer_typmods` deconstructs and decodes.
    fn build_cstring_array(strs: &[&str]) -> Vec<u8> {
        use arrayfuncs::foundation::{
            arr_overhead_nonulls, maxalign, set_varsize,
        };
        use array::ARRAYTYPE_HDRSZ;

        let ndim = 1i32;
        let overhead = arr_overhead_nonulls(ndim); // MAXALIGN(hdr + 2*4*ndim)
        let mut buf = vec![0u8; overhead];

        // header fields (ndim @4, dataoffset @8 = 0 → no nulls, elemtype @12)
        buf[4..8].copy_from_slice(&ndim.to_ne_bytes());
        buf[8..12].copy_from_slice(&0i32.to_ne_bytes());
        buf[12..16].copy_from_slice(&CSTRINGOID.to_ne_bytes());
        // dims[0] @ ARRAYTYPE_HDRSZ, lbound[0] @ +4
        buf[ARRAYTYPE_HDRSZ..ARRAYTYPE_HDRSZ + 4]
            .copy_from_slice(&(strs.len() as i32).to_ne_bytes());
        buf[ARRAYTYPE_HDRSZ + 4..ARRAYTYPE_HDRSZ + 8].copy_from_slice(&1i32.to_ne_bytes());

        // cstring elements: NUL-terminated, char-aligned (no inter-element pad).
        for s in strs {
            buf.extend_from_slice(s.as_bytes());
            buf.push(0);
        }
        // pad tail to MAXALIGN like construct_md_array's nbytes does.
        buf.resize(maxalign(buf.len()), 0);
        let total = buf.len();
        set_varsize(&mut buf, total);
        buf
    }

    #[test]
    fn integer_typmods_decode_and_guards() {
        init_seams();
        let root = mcx::MemoryContext::new("test");

        // Real cstring[] → integer typmods (exercises the in-buffer offset
        // decode that replaced the empty-window detoast stub).
        let arr = build_cstring_array(&["10", "0", "255"]);
        let out = array_get_integer_typmods(root.mcx(), &arr).unwrap();
        assert_eq!(out, vec![10, 0, 255]);

        // Wrong element type is rejected (C: ERRCODE_ARRAY_ELEMENT_ERROR).
        let mut wrong = build_cstring_array(&["1"]);
        wrong[12..16].copy_from_slice(&(CSTRINGOID + 1).to_ne_bytes());
        assert!(array_get_integer_typmods(root.mcx(), &wrong).is_err());
    }
}
