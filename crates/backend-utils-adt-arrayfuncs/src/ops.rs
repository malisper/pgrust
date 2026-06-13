//! Operator family: equality / ordering comparison (`array_eq`, `array_ne`,
//! `array_lt`, `array_gt`, `array_le`, `array_ge`, `btarraycmp`, `array_cmp`),
//! hashing (`hash_array`, `hash_array_extended`), and containment
//! (`arrayoverlap`, `arraycontains`, `arraycontained` via
//! `array_contain_compare`).
//!
//! Element equality / comparison / hashing dispatch through the fmgr owner's
//! `element_eq` / `element_cmp` / `element_hash` / `element_hash_extended`
//! seams (the cached typcache support-proc finfos), whose proc OIDs are
//! resolved through the typcache owner's `lookup_element_*` seams (mirroring
//! `lookup_type_cache(elemtype, TYPECACHE_*_FINFO)->*_finfo.fn_oid`); element
//! storage metadata comes from the lsyscache owner's `get_typlenbyvalalign`.

use types_array::ArrayElementDatum;
use types_core::Oid;
use types_datum::datum::Datum;
use types_error::{PgError, PgResult};
use types_error::error::{ERRCODE_DATATYPE_MISMATCH, ERRCODE_UNDEFINED_FUNCTION};

use crate::foundation::{
    arr_data_ptr_off, arr_dim, arr_elemtype, arr_lbound, arr_ndim, arr_nullbitmap_off,
    array_get_isnull, att_addlength_pointer, att_align_nominal, fetch_att, RECORDOID,
};

use backend_utils_adt_arrayutils_seams as arrayutils;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_typcache_seams as typcache;
use backend_utils_fmgr_fmgr_seams as fmgr;

/// `InvalidOid` (`postgres_ext.h`).
const INVALID_OID: Oid = 0;

/// `F_HASH_RECORD` (`fmgroids.h`): the `hash_record` builtin OID. `hash_array`
/// substitutes this when the element type is `record` (the type cache doesn't
/// believe record is hashable, but we're committed to hashing).
const F_HASH_RECORD: Oid = 6192;

/// `format_type_be(element_type)`-style identifier for the "could not identify"
/// error messages. We don't have the catalog name-formatting here, so we render
/// the OID — the surface (`ERRCODE`, message text shape) matches C.
fn format_type_be(element_type: Oid) -> String {
    format!("{}", element_type)
}

/// A faithful translation of the flat-`ArrayType` portion of `array_iter`
/// (arrayfuncs.c): a cursor over an array's element data, yielding each
/// element's value (as the cross-seam [`ArrayElementDatum`]) and null flag.
///
/// The C `array_iter` also carries the deconstructed (`datumptr`/`isnullptr`)
/// path used for expanded arrays; the byte-buffer model here is always the
/// flat on-disk form, so only the `dataptr`/`bitmapptr`/`bitmask` arm is
/// reproduced.
struct ArrayIter<'a> {
    buf: &'a [u8],
    /// Current byte offset of the next element's data (already aligned).
    dataptr: usize,
    /// Byte offset of the null bitmap, or `None` when the array has none.
    nullbitmap: Option<usize>,
}

impl<'a> ArrayIter<'a> {
    /// `array_iter_setup(it, a)` (arrayfuncs.c), flat-array arm.
    fn setup(buf: &'a [u8]) -> Self {
        ArrayIter {
            buf,
            dataptr: arr_data_ptr_off(buf),
            nullbitmap: arr_nullbitmap_off(buf),
        }
    }

    /// `array_iter_next(it, &isnull, i, elmlen, elmbyval, elmalign)`
    /// (arrayfuncs.c), flat-array arm. Returns `(element, isnull)`.
    fn next(
        &mut self,
        i: i32,
        elmlen: i32,
        elmbyval: bool,
        elmalign: u8,
    ) -> (ArrayElementDatum<'a>, bool) {
        // Null elements: bitmap bit clear => NULL (C tests bitmask against the
        // current bitmap byte; array_get_isnull reproduces the same per-element
        // bit test by element index).
        if array_get_isnull(self.buf, self.nullbitmap, i) {
            // ret = 0; *isnull = true; (dataptr is NOT advanced for nulls)
            return (ArrayElementDatum::ByValue(Datum::null()), true);
        }

        let off = self.dataptr;
        let ret: ArrayElementDatum<'a> = if elmbyval {
            ArrayElementDatum::ByValue(fetch_att(self.buf, off, elmbyval, elmlen))
        } else {
            // By-reference: the element's on-disk bytes. The raw (pre-align)
            // end offset of this element is att_addlength_pointer's result.
            let end = att_addlength_pointer(off, elmlen, self.buf, off);
            ArrayElementDatum::ByRef(&self.buf[off..end])
        };

        // Advance the data pointer past this element, then align for the next
        // one (att_addlength_pointer then att_align_nominal in C).
        let after = att_addlength_pointer(off, elmlen, self.buf, off);
        self.dataptr = att_align_nominal(after, elmalign);

        (ret, false)
    }
}

/// `(typlen, typbyval, typalign)` for the element type, from
/// `get_typlenbyvalalign` (lsyscache.c) — the storage triple the C reads off
/// the cached `TypeCacheEntry`.
fn element_storage(element_type: Oid) -> PgResult<(i32, bool, u8)> {
    let s = lsyscache::get_typlenbyvalalign::call(element_type)?;
    Ok((s.typlen as i32, s.typbyval, s.typalign as u8))
}

// ---------------------------------------------------------------------------
// Comparison (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_eq(array1, array2)` (arrayfuncs.c), under the given `collation`.
pub fn array_eq(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    let ndims1 = arr_ndim(array1);
    let ndims2 = arr_ndim(array2);
    let element_type = arr_elemtype(array1);
    let mut result = true;

    if element_type != arr_elemtype(array2) {
        return Err(PgError::error("cannot compare arrays of different element types")
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }

    // Fast path if the arrays do not have the same dimensionality.
    let mut dims_match = ndims1 == ndims2;
    if dims_match {
        for i in 0..ndims1 as usize {
            if arr_dim(array1, i) != arr_dim(array2, i)
                || arr_lbound(array1, i) != arr_lbound(array2, i)
            {
                dims_match = false;
                break;
            }
        }
    }

    if !dims_match {
        result = false;
    } else {
        // Look up the equality operator's underlying function.
        let eq_opr = typcache::lookup_element_eq_opr::call(element_type)?;
        if eq_opr == INVALID_OID {
            return Err(PgError::error(format!(
                "could not identify an equality operator for type {}",
                format_type_be(element_type)
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
        }
        let (typlen, typbyval, typalign) = element_storage(element_type)?;

        let dims1 = collect_dims(array1, ndims1);
        let nitems = arrayutils::array_get_n_items::call(ndims1, &dims1)?;

        let mut it1 = ArrayIter::setup(array1);
        let mut it2 = ArrayIter::setup(array2);

        for i in 0..nitems {
            let (elt1, isnull1) = it1.next(i, typlen, typbyval, typalign);
            let (elt2, isnull2) = it2.next(i, typlen, typbyval, typalign);

            // Two NULLs are equal; NULL and not-NULL are unequal.
            if isnull1 && isnull2 {
                continue;
            }
            if isnull1 || isnull2 {
                result = false;
                break;
            }

            // Apply the operator to the element pair; treat NULL as false.
            let oprresult = fmgr::element_eq::call(eq_opr, collation, elt1, elt2)?;
            if !oprresult {
                result = false;
                break;
            }
        }
    }

    Ok(result)
}

/// `array_ne(array1, array2)` (arrayfuncs.c) — `!array_eq`.
pub fn array_ne(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(!array_eq(array1, array2, collation)?)
}

/// `array_lt(array1, array2)` (arrayfuncs.c) — `array_cmp < 0`.
pub fn array_lt(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(array_cmp(array1, array2, collation)? < 0)
}

/// `array_gt(array1, array2)` (arrayfuncs.c) — `array_cmp > 0`.
pub fn array_gt(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(array_cmp(array1, array2, collation)? > 0)
}

/// `array_le(array1, array2)` (arrayfuncs.c) — `array_cmp <= 0`.
pub fn array_le(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(array_cmp(array1, array2, collation)? <= 0)
}

/// `array_ge(array1, array2)` (arrayfuncs.c) — `array_cmp >= 0`.
pub fn array_ge(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(array_cmp(array1, array2, collation)? >= 0)
}

/// `btarraycmp(array1, array2)` (arrayfuncs.c) — the btree 3-way comparator
/// wrapper over `array_cmp`.
pub fn btarraycmp(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<i32> {
    array_cmp(array1, array2, collation)
}

/// `array_cmp(fcinfo)` (arrayfuncs.c): the 3-way element-wise comparison that
/// backs all the ordering operators.
pub fn array_cmp(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<i32> {
    let ndims1 = arr_ndim(array1);
    let ndims2 = arr_ndim(array2);
    let dims1 = collect_dims(array1, ndims1);
    let dims2 = collect_dims(array2, ndims2);
    let nitems1 = arrayutils::array_get_n_items::call(ndims1, &dims1)?;
    let nitems2 = arrayutils::array_get_n_items::call(ndims2, &dims2)?;
    let element_type = arr_elemtype(array1);
    let mut result: i32 = 0;

    if element_type != arr_elemtype(array2) {
        return Err(PgError::error("cannot compare arrays of different element types")
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }

    let cmp_proc = typcache::lookup_element_cmp_proc::call(element_type)?;
    if cmp_proc == INVALID_OID {
        return Err(PgError::error(format!(
            "could not identify a comparison function for type {}",
            format_type_be(element_type)
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }
    let (typlen, typbyval, typalign) = element_storage(element_type)?;

    let min_nitems = nitems1.min(nitems2);
    let mut it1 = ArrayIter::setup(array1);
    let mut it2 = ArrayIter::setup(array2);

    for i in 0..min_nitems {
        let (elt1, isnull1) = it1.next(i, typlen, typbyval, typalign);
        let (elt2, isnull2) = it2.next(i, typlen, typbyval, typalign);

        // Two NULLs are equal; NULL > not-NULL.
        if isnull1 && isnull2 {
            continue;
        }
        if isnull1 {
            result = 1; // arg1 is greater than arg2
            break;
        }
        if isnull2 {
            result = -1; // arg1 is less than arg2
            break;
        }

        let cmpresult = fmgr::element_cmp::call(cmp_proc, collation, elt1, elt2)?;

        if cmpresult == 0 {
            continue; // equal
        }
        if cmpresult < 0 {
            result = -1; // arg1 is less than arg2
            break;
        } else {
            result = 1; // arg1 is greater than arg2
            break;
        }
    }

    // If arrays contain same data (up to end of shorter one), apply additional
    // rules to sort by dimensionality.
    if result == 0 {
        if nitems1 != nitems2 {
            result = if nitems1 < nitems2 { -1 } else { 1 };
        } else if ndims1 != ndims2 {
            result = if ndims1 < ndims2 { -1 } else { 1 };
        } else {
            for i in 0..ndims1 as usize {
                let d1 = arr_dim(array1, i);
                let d2 = arr_dim(array2, i);
                if d1 != d2 {
                    result = if d1 < d2 { -1 } else { 1 };
                    break;
                }
            }
            if result == 0 {
                for i in 0..ndims1 as usize {
                    let l1 = arr_lbound(array1, i);
                    let l2 = arr_lbound(array2, i);
                    if l1 != l2 {
                        result = if l1 < l2 { -1 } else { 1 };
                        break;
                    }
                }
            }
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Hashing (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `hash_array(array)` (arrayfuncs.c).
pub fn hash_array(array: &[u8], collation: Oid) -> PgResult<u32> {
    let ndims = arr_ndim(array);
    let dims = collect_dims(array, ndims);
    let element_type = arr_elemtype(array);
    let mut result: u32 = 1;

    let mut hash_proc = typcache::lookup_element_hash_proc::call(element_type)?;
    if hash_proc == INVALID_OID && element_type != RECORDOID {
        return Err(PgError::error(format!(
            "could not identify a hash function for type {}",
            format_type_be(element_type)
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }

    // The type cache doesn't believe that record is hashable, but since we're
    // here, we're committed to hashing, so substitute hash_record.
    if element_type == RECORDOID {
        hash_proc = F_HASH_RECORD;
    }

    let (typlen, typbyval, typalign) = element_storage(element_type)?;

    let nitems = arrayutils::array_get_n_items::call(ndims, &dims)?;
    let mut iter = ArrayIter::setup(array);

    for i in 0..nitems {
        let (elt, isnull) = iter.next(i, typlen, typbyval, typalign);

        let elthash: u32 = if isnull {
            // Treat nulls as having hashvalue 0.
            0
        } else {
            fmgr::element_hash::call(hash_proc, collation, elt)?
        };

        // Combine hash values of successive elements: result*31 + elthash,
        // modulo 2^32.
        result = (result << 5).wrapping_sub(result).wrapping_add(elthash);
    }

    Ok(result)
}

/// `hash_array_extended(array, seed)` (arrayfuncs.c).
pub fn hash_array_extended(array: &[u8], collation: Oid, seed: u64) -> PgResult<u64> {
    let ndims = arr_ndim(array);
    let dims = collect_dims(array, ndims);
    let element_type = arr_elemtype(array);
    let mut result: u64 = 1;

    let hash_proc = typcache::lookup_element_hash_extended_proc::call(element_type)?;
    if hash_proc == INVALID_OID {
        return Err(PgError::error(format!(
            "could not identify an extended hash function for type {}",
            format_type_be(element_type)
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }
    let (typlen, typbyval, typalign) = element_storage(element_type)?;

    let nitems = arrayutils::array_get_n_items::call(ndims, &dims)?;
    let mut iter = ArrayIter::setup(array);

    for i in 0..nitems {
        let (elt, isnull) = iter.next(i, typlen, typbyval, typalign);

        let elthash: u64 = if isnull {
            0
        } else {
            fmgr::element_hash_extended::call(hash_proc, collation, elt, seed)?
        };

        result = (result << 5).wrapping_sub(result).wrapping_add(elthash);
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Containment (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `arrayoverlap(array1, array2)` (arrayfuncs.c): whether the two arrays share
/// any element.
pub fn arrayoverlap(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    array_contain_compare(array1, array2, collation, false)
}

/// `arraycontains(array1, array2)` (arrayfuncs.c): whether `array1` contains
/// every element of `array2`.
pub fn arraycontains(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    // C: array_contain_compare(array2, array1, collation, true, ...)
    array_contain_compare(array2, array1, collation, true)
}

/// `arraycontained(array1, array2)` (arrayfuncs.c): `arraycontains(array2,
/// array1)`.
pub fn arraycontained(array1: &[u8], array2: &[u8], collation: Oid) -> PgResult<bool> {
    // C: array_contain_compare(array1, array2, collation, true, ...)
    array_contain_compare(array1, array2, collation, true)
}

/// `array_contain_compare(array1, array2, collation, matchall, fn_extra)`
/// (arrayfuncs.c): the shared engine behind overlap / contains / contained.
///
/// When `matchall` is true, returns true if all members of `array1` are in
/// `array2`. When `matchall` is false, returns true if any members of `array1`
/// are in `array2`.
pub fn array_contain_compare(
    array1: &[u8],
    array2: &[u8],
    collation: Oid,
    matchall: bool,
) -> PgResult<bool> {
    let mut result = matchall;
    let element_type = arr_elemtype(array1);

    if element_type != arr_elemtype(array2) {
        return Err(PgError::error("cannot compare arrays of different element types")
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }

    let eq_opr = typcache::lookup_element_eq_opr::call(element_type)?;
    if eq_opr == INVALID_OID {
        return Err(PgError::error(format!(
            "could not identify an equality operator for type {}",
            format_type_be(element_type)
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }
    let (typlen, typbyval, typalign) = element_storage(element_type)?;

    // Since we probably will need to scan array2 multiple times, deconstruct it
    // into per-element values; we scan array1 the hard way (an iterator), since
    // we very likely won't need to look at all of it. (The C
    // deconstruct_expanded_array path doesn't apply to the flat byte model.)
    let ndims2 = arr_ndim(array2);
    let dims2 = collect_dims(array2, ndims2);
    let nelems2 = arrayutils::array_get_n_items::call(ndims2, &dims2)?;
    let mut values2: Vec<ArrayElementDatum<'_>> = Vec::with_capacity(nelems2 as usize);
    let mut nulls2: Vec<bool> = Vec::with_capacity(nelems2 as usize);
    {
        let mut it2 = ArrayIter::setup(array2);
        for j in 0..nelems2 {
            let (elt2, isnull2) = it2.next(j, typlen, typbyval, typalign);
            values2.push(elt2);
            nulls2.push(isnull2);
        }
    }

    let ndims1 = arr_ndim(array1);
    let dims1 = collect_dims(array1, ndims1);
    let nelems1 = arrayutils::array_get_n_items::call(ndims1, &dims1)?;
    let mut it1 = ArrayIter::setup(array1);

    for i in 0..nelems1 {
        let (elt1, isnull1) = it1.next(i, typlen, typbyval, typalign);

        // The comparison operator is assumed strict, so a NULL can't match
        // anything.
        if isnull1 {
            if matchall {
                result = false;
                break;
            }
            continue;
        }

        let mut found = false;
        for j in 0..nelems2 as usize {
            if nulls2[j] {
                continue; // can't match
            }
            let oprresult = fmgr::element_eq::call(eq_opr, collation, elt1, values2[j])?;
            if oprresult {
                found = true;
                break;
            }
        }

        if found {
            // found a match for elt1
            if !matchall {
                result = true;
                break;
            }
        } else {
            // no match for elt1
            if matchall {
                result = false;
                break;
            }
        }
    }

    Ok(result)
}

/// Collect an array's `ndim` dimension lengths into a `Vec<i32>` (the C
/// `AARR_DIMS(a)` pointer, materialized for `ArrayGetNItems`).
fn collect_dims(a: &[u8], ndim: i32) -> Vec<i32> {
    (0..ndim as usize).map(|i| arr_dim(a, i)).collect()
}
