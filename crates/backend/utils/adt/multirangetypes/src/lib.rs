pub mod builtins;
pub mod io;
#[cfg(test)]
mod corpus_tests;
#[cfg(test)]
mod tests;

use core::cell::RefCell;
use std::rc::Rc;

use ::adt_rangetypes::{
    att_align_nominal, fetch_att, make_range, range_cmp_bounds, range_deserialize,
    range_get_flags, range_has_lbound, range_has_ubound, range_is_empty, ops as range_ops,
    ElemInfo, RangeBound, RangeInfo, RANGE_EMPTY, RANGE_LB_INC, RANGE_LB_INF, RANGE_UB_INC,
    RANGE_UB_INF,
};
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::typcache::{TypeCacheEntry, TYPECACHE_MULTIRANGE_INFO};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_fmgr::FmgrInfo;

// sizeof(MultirangeType): vl_len_ + multirangetypid + rangeCount.
pub const MULTIRANGE_HDRSZ: usize = 12;

const MULTIRANGE_ITEM_OFF_BIT: u32 = 0x80000000;
const MULTIRANGE_ITEM_OFFSET_STRIDE: usize = 4;

#[inline]
const fn item_get_offlen(item: u32) -> u32 {
    item & 0x7FFFFFFF
}

#[inline]
const fn item_has_off(item: u32) -> bool {
    item & MULTIRANGE_ITEM_OFF_BIT != 0
}

pub struct MultirangeInfo {
    pub pin: Option<Rc<TypeCacheEntry>>,
    pub mltrngtypid: Oid,
    pub rng: RangeInfo,
}

#[track_caller]
#[cold]
fn not_a_multirange_type(oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("type {oid} is not a multirange type")))
}

impl MultirangeInfo {
    pub fn lookup(mltrngtypid: Oid) -> PgResult<MultirangeInfo> {
        let e = ::typcache::lookup_type_cache(mltrngtypid, TYPECACHE_MULTIRANGE_INFO)?;
        let Some(rt) = e.rngtype() else {
            return Err(not_a_multirange_type(mltrngtypid));
        };
        let rng = RangeInfo::from_entry(rt)?;
        Ok(MultirangeInfo { pin: Some(e), mltrngtypid, rng })
    }
}

// multirange_get_typcache (multirangetypes.c): fn_extra memo.
pub fn cached_multirange_info<'f>(
    flinfo: &'f mut FmgrInfo,
    mltrngtypid: Oid,
) -> PgResult<&'f mut MultirangeInfo> {
    let need = match flinfo.fn_extra_ref::<MultirangeInfo>() {
        Some(mi) => mi.mltrngtypid != mltrngtypid,
        None => true,
    };
    if need {
        flinfo.set_fn_extra(MultirangeInfo::lookup(mltrngtypid)?);
    }
    Ok(flinfo.fn_extra_mut::<MultirangeInfo>().unwrap())
}

#[inline]
pub fn multirange_type_oid(mr: &[u8]) -> Oid {
    Oid::from_ne_bytes(mr[4..8].try_into().unwrap())
}

#[inline]
pub fn multirange_count(mr: &[u8]) -> u32 {
    u32::from_ne_bytes(mr[8..12].try_into().unwrap())
}

#[inline]
pub fn multirange_is_empty(mr: &[u8]) -> bool {
    multirange_count(mr) == 0
}

#[inline]
fn items_ptr(mr: &[u8]) -> &[u8] {
    &mr[MULTIRANGE_HDRSZ..]
}

#[inline]
fn item(mr: &[u8], i: usize) -> u32 {
    let items = items_ptr(mr);
    u32::from_ne_bytes(items[i * 4..i * 4 + 4].try_into().unwrap())
}

#[inline]
fn flags_off(mr: &[u8]) -> usize {
    MULTIRANGE_HDRSZ + (multirange_count(mr) as usize).saturating_sub(1) * 4
}

#[inline]
pub fn multirange_flags(mr: &[u8], i: usize) -> u8 {
    mr[flags_off(mr) + i]
}

#[inline]
fn boundaries_off(mr: &[u8], elemalign: u8) -> usize {
    let n = multirange_count(mr) as usize;
    att_align_nominal(MULTIRANGE_HDRSZ + n.saturating_sub(1) * 4 + n, elemalign)
}

fn bounds_offset(mr: &[u8], mut i: usize) -> usize {
    let mut offset = 0usize;
    while i > 0 {
        let it = item(mr, i - 1);
        offset += item_get_offlen(it) as usize;
        if item_has_off(it) {
            break;
        }
        i -= 1;
    }
    offset
}

/// multirange_get_bounds (multirangetypes.c).
pub fn multirange_get_bounds(
    rng: &RangeInfo,
    mr: &[u8],
    i: usize,
) -> (RangeBound, RangeBound) {
    debug_assert!(i < multirange_count(mr) as usize);
    let elem = &rng.elem;
    let typlen = elem.typlen as i32;
    let flags = multirange_flags(mr, i);
    let mut off = boundaries_off(mr, elem.typalign) + bounds_offset(mr, i);
    let base = mr.as_ptr();
    debug_assert!(flags & RANGE_EMPTY == 0);

    let lbound = if range_has_lbound(flags) {
        // SAFETY: offsets stay within the serialized multirange image.
        let d = fetch_att(unsafe { base.add(off) }, elem.typbyval, typlen);
        off = arrayfuncs::foundation::att_addlength_pointer(off, typlen, unsafe { base.add(off) });
        d
    } else {
        Datum::from_usize(0)
    };
    let ubound = if range_has_ubound(flags) {
        if !(typlen == -1 && unsafe { *base.add(off) } != 0) {
            off = att_align_nominal(off, elem.typalign);
        }
        // SAFETY: as above.
        fetch_att(unsafe { base.add(off) }, elem.typbyval, typlen)
    } else {
        Datum::from_usize(0)
    };

    (
        RangeBound {
            val: lbound,
            infinite: flags & RANGE_LB_INF != 0,
            inclusive: flags & RANGE_LB_INC != 0,
            lower: true,
        },
        RangeBound {
            val: ubound,
            infinite: flags & RANGE_UB_INF != 0,
            inclusive: flags & RANGE_UB_INC != 0,
            lower: false,
        },
    )
}

/// multirange_get_range (multirangetypes.c): reconstruct the i'th range image.
pub fn multirange_get_range<'m>(
    mcx: Mcx<'m>,
    rng: &RangeInfo,
    mr: &[u8],
    i: usize,
) -> PgResult<PgVec<'m, u8>> {
    debug_assert!(i < multirange_count(mr) as usize);
    let elem = &rng.elem;
    let typlen = elem.typlen as i32;
    let flags = multirange_flags(mr, i);
    let begin = boundaries_off(mr, elem.typalign) + bounds_offset(mr, i);
    let base = mr.as_ptr();
    let mut off = begin;
    if range_has_lbound(flags) {
        // SAFETY: offsets stay within the image.
        off = arrayfuncs::foundation::att_addlength_pointer(off, typlen, unsafe { base.add(off) });
    }
    if range_has_ubound(flags) {
        if !(typlen == -1 && unsafe { *base.add(off) } != 0) {
            off = att_align_nominal(off, elem.typalign);
        }
        off = arrayfuncs::foundation::att_addlength_pointer(off, typlen, unsafe { base.add(off) });
    }
    let boundary_len = off - begin;
    let len = boundary_len + ::adt_rangetypes::RANGE_HDRSZ + 1;

    let mut img: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, len)?;
    img.resize(len, 0);
    img[0..4].copy_from_slice(&::datum::set_varsize_4b(len));
    img[4..8].copy_from_slice(&rng.rngtypid.to_ne_bytes());
    img[8..8 + boundary_len].copy_from_slice(&mr[begin..begin + boundary_len]);
    img[len - 1] = flags;
    Ok(img)
}

// Leak an image into its arena lifetime (bulk-freed with the context, as C).
pub fn leak_image<'m>(v: PgVec<'m, u8>) -> &'m [u8] {
    let (ptr, len) = (v.as_ptr(), v.len());
    core::mem::forget(v);
    // SAFETY: the allocation lives in the mcx arena until reset; forget only
    // skips the vec's own dealloc.
    unsafe { core::slice::from_raw_parts(ptr, len) }
}

// multirange_canonicalize (multirangetypes.c): sort + merge; returns the
// canonical prefix length of `ranges`.
fn multirange_canonicalize<'m, 'r>(
    mcx: Mcx<'m>,
    rng: &mut RangeInfo,
    ranges: &mut PgVec<'_, &'r [u8]>,
) -> PgResult<usize>
where
    'm: 'r,
{
    let rng_cell = RefCell::new(rng);
    let mut sort_err: Option<Box<PgError>> = None;
    ranges.sort_by(|a, b| {
        if sort_err.is_some() {
            return core::cmp::Ordering::Equal;
        }
        let mut rngm = rng_cell.borrow_mut();
        match range_compare(mcx, &mut rngm, a, b) {
            Ok(c) => c.cmp(&0),
            Err(e) => {
                sort_err = Some(e);
                core::cmp::Ordering::Equal
            }
        }
    });
    if let Some(e) = sort_err {
        return Err(e);
    }
    let rng = rng_cell.into_inner();

    let mut out = 0usize;
    let mut last: Option<&'r [u8]> = None;
    for i in 0..ranges.len() {
        let current = ranges[i];
        if range_is_empty(current) {
            continue;
        }
        let Some(prev) = last else {
            ranges[out] = current;
            last = Some(current);
            out += 1;
            continue;
        };
        if range_ops::range_adjacent_internal(mcx, rng, prev, current)? {
            let merged = match range_ops::range_union_internal(mcx, rng, prev, current, false)? {
                range_ops::UnionResult::Input1 => prev,
                range_ops::UnionResult::Input2 => current,
                range_ops::UnionResult::New(img) => leak_image(img),
            };
            ranges[out - 1] = merged;
            last = Some(merged);
        } else if range_ops::range_before_internal(mcx, rng, prev, current)? {
            ranges[out] = current;
            last = Some(current);
            out += 1;
        } else {
            let merged = match range_ops::range_union_internal(mcx, rng, prev, current, true)? {
                range_ops::UnionResult::Input1 => prev,
                range_ops::UnionResult::Input2 => current,
                range_ops::UnionResult::New(img) => leak_image(img),
            };
            ranges[out - 1] = merged;
            last = Some(merged);
        }
    }
    Ok(out)
}

/// range_compare (rangetypes.c qsort callback).
pub fn range_compare(mcx: Mcx<'_>, rng: &mut RangeInfo, r1: &[u8], r2: &[u8]) -> PgResult<i32> {
    let (lower1, upper1, empty1) = range_deserialize(&rng.elem, r1);
    let (lower2, upper2, empty2) = range_deserialize(&rng.elem, r2);
    if empty1 && empty2 {
        Ok(0)
    } else if empty1 {
        Ok(-1)
    } else if empty2 {
        Ok(1)
    } else {
        let mut cmp = range_cmp_bounds(mcx, rng, &lower1, &lower2)?;
        if cmp == 0 {
            cmp = range_cmp_bounds(mcx, rng, &upper1, &upper2)?;
        }
        Ok(cmp)
    }
}

/// make_multirange (multirangetypes.c): canonicalize + byte-exact image.
pub fn make_multirange<'m, 'r>(
    mcx: Mcx<'m>,
    mltrngtypid: Oid,
    rng: &mut RangeInfo,
    ranges: &mut PgVec<'_, &'r [u8]>,
) -> PgResult<PgVec<'m, u8>>
where
    'm: 'r,
{
    let range_count = multirange_canonicalize(mcx, rng, ranges)?;
    let elemalign = rng.elem.typalign;

    let mut size = att_align_nominal(
        MULTIRANGE_HDRSZ + range_count.saturating_sub(1) * 4 + range_count,
        elemalign,
    );
    for r in ranges[..range_count].iter() {
        size += att_align_nominal(r.len() - ::adt_rangetypes::RANGE_HDRSZ - 1, elemalign);
    }

    let mut img: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, size)?;
    img.resize(size, 0);
    img[0..4].copy_from_slice(&::datum::set_varsize_4b(size));
    img[4..8].copy_from_slice(&mltrngtypid.to_ne_bytes());
    img[8..12].copy_from_slice(&(range_count as u32).to_ne_bytes());

    let items_off = MULTIRANGE_HDRSZ;
    let flags_off = MULTIRANGE_HDRSZ + range_count.saturating_sub(1) * 4;
    let begin = att_align_nominal(flags_off + range_count, elemalign);
    let mut ptr = begin;
    let mut prev_offset = 0usize;
    for (i, r) in ranges[..range_count].iter().enumerate() {
        if i > 0 {
            let mut it = (ptr - begin) as u32;
            if i % MULTIRANGE_ITEM_OFFSET_STRIDE != 0 {
                it -= prev_offset as u32;
            } else {
                it |= MULTIRANGE_ITEM_OFF_BIT;
            }
            img[items_off + (i - 1) * 4..items_off + i * 4].copy_from_slice(&it.to_ne_bytes());
            prev_offset = ptr - begin;
        }
        img[flags_off + i] = range_get_flags(r);
        let len = r.len() - ::adt_rangetypes::RANGE_HDRSZ - 1;
        img[ptr..ptr + len]
            .copy_from_slice(&r[::adt_rangetypes::RANGE_HDRSZ..::adt_rangetypes::RANGE_HDRSZ + len]);
        ptr += att_align_nominal(len, elemalign);
    }
    Ok(img)
}

pub fn make_empty_multirange<'m>(
    mcx: Mcx<'m>,
    mltrngtypid: Oid,
    rng: &mut RangeInfo,
) -> PgResult<PgVec<'m, u8>> {
    let mut ranges: PgVec<'_, &[u8]> = ::mcx::vec_with_capacity_in(mcx, 0)?;
    make_multirange(mcx, mltrngtypid, rng, &mut ranges)
}

/// multirange_deserialize: all member ranges as fresh images.
pub fn multirange_deserialize<'m>(
    mcx: Mcx<'m>,
    rng: &RangeInfo,
    mr: &[u8],
) -> PgResult<PgVec<'m, &'m [u8]>> {
    let n = multirange_count(mr) as usize;
    let mut out: PgVec<'m, &'m [u8]> = ::mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        out.push(leak_image(multirange_get_range(mcx, rng, mr, i)?));
    }
    Ok(out)
}

/// multirange_get_union_range (multirangetypes.c).
pub fn multirange_get_union_range<'m>(
    mcx: Mcx<'m>,
    rng: &mut RangeInfo,
    mr: &[u8],
) -> PgResult<PgVec<'m, u8>> {
    if multirange_is_empty(mr) {
        return ::adt_rangetypes::make_empty_range(mcx, rng);
    }
    let (mut lower, _t1) = multirange_get_bounds(rng, mr, 0);
    let (_t2, mut upper) = multirange_get_bounds(rng, mr, multirange_count(mr) as usize - 1);
    Ok(make_range(mcx, rng, &mut lower, &mut upper, false, None)?
        .expect("hard error path returns Some"))
}

fn range_bounds_overlaps(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    lower1: &RangeBound,
    upper1: &RangeBound,
    lower2: &RangeBound,
    upper2: &RangeBound,
) -> PgResult<bool> {
    if range_cmp_bounds(mcx, rng, lower1, lower2)? >= 0 && range_cmp_bounds(mcx, rng, lower1, upper2)? <= 0 {
        return Ok(true);
    }
    if range_cmp_bounds(mcx, rng, lower2, lower1)? >= 0 && range_cmp_bounds(mcx, rng, lower2, upper1)? <= 0 {
        return Ok(true);
    }
    Ok(false)
}

fn range_bounds_contains(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    lower1: &RangeBound,
    upper1: &RangeBound,
    lower2: &RangeBound,
    upper2: &RangeBound,
) -> PgResult<bool> {
    Ok(range_cmp_bounds(mcx, rng, lower1, lower2)? <= 0 && range_cmp_bounds(mcx, rng, upper1, upper2)? >= 0)
}

// multirange_bsearch_match (multirangetypes.c).
fn multirange_bsearch_match<F>(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    mr: &[u8],
    mut cmp: F,
) -> PgResult<bool>
where
    F: FnMut(Mcx<'_>, &mut RangeInfo, &RangeBound, &RangeBound, &mut bool) -> PgResult<i32>,
{
    let mut l = 0u32;
    let mut u = multirange_count(mr);
    while l < u {
        let idx = (l + u) / 2;
        let (lower, upper) = multirange_get_bounds(rng, mr, idx as usize);
        let mut matched = false;
        let comparison = cmp(mcx, rng, &lower, &upper, &mut matched)?;
        if comparison < 0 {
            u = idx;
        } else if comparison > 0 {
            l = idx + 1;
        } else {
            return Ok(matched);
        }
    }
    Ok(false)
}

#[cold]
pub fn multirange_types_do_not_match() -> Box<PgError> {
    Box::new(PgError::error("multirange types do not match"))
}

pub fn multirange_eq_internal(mcx: Mcx<'_>, rng: &mut RangeInfo, mr1: &[u8], mr2: &[u8]) -> PgResult<bool> {
    if multirange_type_oid(mr1) != multirange_type_oid(mr2) {
        return Err(multirange_types_do_not_match());
    }
    let n1 = multirange_count(mr1);
    let n2 = multirange_count(mr2);
    if n1 != n2 {
        return Ok(false);
    }
    for i in 0..n1 as usize {
        let (lower1, upper1) = multirange_get_bounds(rng, mr1, i);
        let (lower2, upper2) = multirange_get_bounds(rng, mr2, i);
        if range_cmp_bounds(mcx, rng, &lower1, &lower2)? != 0
            || range_cmp_bounds(mcx, rng, &upper1, &upper2)? != 0
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn multirange_contains_elem_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    mr: &[u8],
    val: Datum,
) -> PgResult<bool> {
    if multirange_is_empty(mr) {
        return Ok(false);
    }
    multirange_bsearch_match(mcx, rng, mr, |mcx, rng, lower, upper, matched| {
        if !lower.infinite {
            let cmp = ::adt_rangetypes::cmp_elem_vals(mcx, rng, lower.val, val)?;
            if cmp > 0 || (cmp == 0 && !lower.inclusive) {
                return Ok(-1);
            }
        }
        if !upper.infinite {
            let cmp = ::adt_rangetypes::cmp_elem_vals(mcx, rng, upper.val, val)?;
            if cmp < 0 || (cmp == 0 && !upper.inclusive) {
                return Ok(1);
            }
        }
        *matched = true;
        Ok(0)
    })
}

pub fn multirange_contains_range_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    mr: &[u8],
    r: &[u8],
) -> PgResult<bool> {
    if range_is_empty(r) {
        return Ok(true);
    }
    if multirange_is_empty(mr) {
        return Ok(false);
    }
    let (key_lower, key_upper, _empty) = range_deserialize(&rng.elem, r);
    multirange_bsearch_match(mcx, rng, mr, |mcx, rng, lower, upper, matched| {
        if range_cmp_bounds(mcx, rng, &key_upper, lower)? < 0 {
            return Ok(-1);
        }
        if range_cmp_bounds(mcx, rng, &key_lower, upper)? > 0 {
            return Ok(1);
        }
        *matched = range_bounds_contains(mcx, rng, lower, upper, &key_lower, &key_upper)?;
        Ok(0)
    })
}

pub fn range_contains_multirange_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    r: &[u8],
    mr: &[u8],
) -> PgResult<bool> {
    if multirange_is_empty(mr) {
        return Ok(true);
    }
    if range_is_empty(r) {
        return Ok(false);
    }
    let (lower1, upper1, _empty) = range_deserialize(&rng.elem, r);
    let (lower2, _t1) = multirange_get_bounds(rng, mr, 0);
    let (_t2, upper2) = multirange_get_bounds(rng, mr, multirange_count(mr) as usize - 1);
    range_bounds_contains(mcx, rng, &lower1, &upper1, &lower2, &upper2)
}

pub fn multirange_contains_multirange_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    mr1: &[u8],
    mr2: &[u8],
) -> PgResult<bool> {
    let n1 = multirange_count(mr1) as usize;
    let n2 = multirange_count(mr2) as usize;
    if n2 == 0 {
        return Ok(true);
    }
    if n1 == 0 {
        return Ok(false);
    }
    let mut i1 = 0usize;
    let (mut lower1, mut upper1) = multirange_get_bounds(rng, mr1, i1);
    for i2 in 0..n2 {
        let (lower2, upper2) = multirange_get_bounds(rng, mr2, i2);
        while range_cmp_bounds(mcx, rng, &upper1, &lower2)? < 0 {
            i1 += 1;
            if i1 >= n1 {
                return Ok(false);
            }
            (lower1, upper1) = multirange_get_bounds(rng, mr1, i1);
        }
        if !range_bounds_contains(mcx, rng, &lower1, &upper1, &lower2, &upper2)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn range_overlaps_multirange_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    r: &[u8],
    mr: &[u8],
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }
    let (key_lower, key_upper, _empty) = range_deserialize(&rng.elem, r);
    multirange_bsearch_match(mcx, rng, mr, |mcx, rng, lower, upper, matched| {
        if range_cmp_bounds(mcx, rng, &key_upper, lower)? < 0 {
            return Ok(-1);
        }
        if range_cmp_bounds(mcx, rng, &key_lower, upper)? > 0 {
            return Ok(1);
        }
        *matched = true;
        Ok(0)
    })
}

pub fn multirange_overlaps_multirange_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    mr1: &[u8],
    mr2: &[u8],
) -> PgResult<bool> {
    if multirange_is_empty(mr1) || multirange_is_empty(mr2) {
        return Ok(false);
    }
    let n1 = multirange_count(mr1) as usize;
    let n2 = multirange_count(mr2) as usize;
    let mut i1 = 0usize;
    let (mut lower1, mut upper1) = multirange_get_bounds(rng, mr1, i1);
    for i2 in 0..n2 {
        let (lower2, upper2) = multirange_get_bounds(rng, mr2, i2);
        while range_cmp_bounds(mcx, rng, &upper1, &lower2)? < 0 {
            i1 += 1;
            if i1 >= n1 {
                return Ok(false);
            }
            (lower1, upper1) = multirange_get_bounds(rng, mr1, i1);
        }
        if range_bounds_overlaps(mcx, rng, &lower1, &upper1, &lower2, &upper2)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn range_overleft_multirange_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    r: &[u8],
    mr: &[u8],
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }
    let (_l1, upper1, _e) = range_deserialize(&rng.elem, r);
    let (_l2, upper2) = multirange_get_bounds(rng, mr, multirange_count(mr) as usize - 1);
    Ok(range_cmp_bounds(mcx, rng, &upper1, &upper2)? <= 0)
}

pub fn range_overright_multirange_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    r: &[u8],
    mr: &[u8],
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }
    let (lower1, _u1, _e) = range_deserialize(&rng.elem, r);
    let (lower2, _u2) = multirange_get_bounds(rng, mr, 0);
    Ok(range_cmp_bounds(mcx, rng, &lower1, &lower2)? >= 0)
}

pub fn range_before_multirange_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    r: &[u8],
    mr: &[u8],
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }
    let (_l1, upper1, _e) = range_deserialize(&rng.elem, r);
    let (lower2, _u2) = multirange_get_bounds(rng, mr, 0);
    Ok(range_cmp_bounds(mcx, rng, &upper1, &lower2)? < 0)
}

pub fn range_after_multirange_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    r: &[u8],
    mr: &[u8],
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }
    let (lower1, _u1, _e) = range_deserialize(&rng.elem, r);
    let (_l2, upper2) = multirange_get_bounds(rng, mr, multirange_count(mr) as usize - 1);
    Ok(range_cmp_bounds(mcx, rng, &lower1, &upper2)? > 0)
}

pub fn multirange_before_multirange_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    mr1: &[u8],
    mr2: &[u8],
) -> PgResult<bool> {
    if multirange_is_empty(mr1) || multirange_is_empty(mr2) {
        return Ok(false);
    }
    let (_l1, upper1) = multirange_get_bounds(rng, mr1, multirange_count(mr1) as usize - 1);
    let (lower2, _u2) = multirange_get_bounds(rng, mr2, 0);
    Ok(range_cmp_bounds(mcx, rng, &upper1, &lower2)? < 0)
}

pub fn range_adjacent_multirange_internal(
    mcx: Mcx<'_>,
    rng: &mut RangeInfo,
    r: &[u8],
    mr: &[u8],
) -> PgResult<bool> {
    if range_is_empty(r) || multirange_is_empty(mr) {
        return Ok(false);
    }
    let (lower1, upper1, _e) = range_deserialize(&rng.elem, r);
    let range_count = multirange_count(mr) as usize;
    let (mut lower2, mut upper2) = multirange_get_bounds(rng, mr, 0);
    if range_ops::bounds_adjacent(mcx, rng, upper1, lower2)? {
        return Ok(true);
    }
    if range_count > 1 {
        (lower2, upper2) = multirange_get_bounds(rng, mr, range_count - 1);
    }
    let _ = lower2;
    if range_ops::bounds_adjacent(mcx, rng, upper2, lower1)? {
        return Ok(true);
    }
    Ok(false)
}

/// multirange_cmp core.
pub fn multirange_cmp_internal(mcx: Mcx<'_>, rng: &mut RangeInfo, mr1: &[u8], mr2: &[u8]) -> PgResult<i32> {
    if multirange_type_oid(mr1) != multirange_type_oid(mr2) {
        return Err(multirange_types_do_not_match());
    }
    let n1 = multirange_count(mr1) as usize;
    let n2 = multirange_count(mr2) as usize;
    let mut cmp = 0i32;
    for i in 0..n1.max(n2) {
        if i >= n1 {
            cmp = -1;
            break;
        }
        if i >= n2 {
            cmp = 1;
            break;
        }
        let (lower1, upper1) = multirange_get_bounds(rng, mr1, i);
        let (lower2, upper2) = multirange_get_bounds(rng, mr2, i);
        cmp = range_cmp_bounds(mcx, rng, &lower1, &lower2)?;
        if cmp == 0 {
            cmp = range_cmp_bounds(mcx, rng, &upper1, &upper2)?;
        }
        if cmp != 0 {
            break;
        }
    }
    Ok(cmp)
}

/// multirange_minus_internal (multirangetypes.c).
pub fn multirange_minus_internal<'m, 'r>(
    mcx: Mcx<'m>,
    mltrngtypoid: Oid,
    rng: &mut RangeInfo,
    ranges1: &[&'r [u8]],
    ranges2: &[&'r [u8]],
) -> PgResult<PgVec<'m, u8>>
where
    'm: 'r,
{
    let mut ranges3: PgVec<'_, &'r [u8]> =
        ::mcx::vec_with_capacity_in(mcx, ranges1.len() + ranges2.len())?;

    let mut i2 = 0usize;
    let mut r2: Option<&'r [u8]> = ranges2.first().copied();
    for &orig_r1 in ranges1 {
        let mut r1: &'r [u8] = orig_r1;

        while let Some(rr2) = r2 {
            if range_ops::range_before_internal(mcx, rng, rr2, r1)? {
                i2 += 1;
                r2 = ranges2.get(i2).copied();
            } else {
                break;
            }
        }

        while let Some(rr2) = r2 {
            if let Some((out1, rest)) = range_ops::range_split_internal(mcx, rng, r1, rr2)? {
                ranges3.push(leak_image(out1));
                r1 = leak_image(rest);
                i2 += 1;
                r2 = ranges2.get(i2).copied();
            } else if range_ops::range_overlaps_internal(mcx, rng, r1, rr2)? {
                r1 = match range_ops::range_minus_internal(mcx, rng, r1, rr2)? {
                    range_ops::MinusResult::Input1 => r1,
                    range_ops::MinusResult::New(img) => leak_image(img),
                };
                if range_is_empty(r1) || range_ops::range_before_internal(mcx, rng, r1, rr2)? {
                    break;
                } else {
                    i2 += 1;
                    r2 = ranges2.get(i2).copied();
                }
            } else {
                break;
            }
        }

        ranges3.push(r1);
    }

    make_multirange(mcx, mltrngtypoid, rng, &mut ranges3)
}

/// multirange_intersect_internal (multirangetypes.c).
pub fn multirange_intersect_internal<'m, 'r>(
    mcx: Mcx<'m>,
    mltrngtypoid: Oid,
    rng: &mut RangeInfo,
    ranges1: &[&'r [u8]],
    ranges2: &[&'r [u8]],
) -> PgResult<PgVec<'m, u8>>
where
    'm: 'r,
{
    let mut ranges3: PgVec<'_, &'r [u8]> =
        ::mcx::vec_with_capacity_in(mcx, ranges1.len() + ranges2.len())?;

    if ranges1.is_empty() || ranges2.is_empty() {
        return make_multirange(mcx, mltrngtypoid, rng, &mut ranges3);
    }

    let mut i2 = 0usize;
    let mut r2: Option<&'r [u8]> = ranges2.first().copied();
    'outer: for &r1 in ranges1 {
        while let Some(rr2) = r2 {
            if range_ops::range_before_internal(mcx, rng, rr2, r1)? {
                i2 += 1;
                r2 = ranges2.get(i2).copied();
            } else {
                break;
            }
        }

        while let Some(rr2) = r2 {
            if range_ops::range_overlaps_internal(mcx, rng, r1, rr2)? {
                ranges3.push(leak_image(range_ops::range_intersect_internal(mcx, rng, r1, rr2)?));
                if range_ops::range_overleft_internal(mcx, rng, rr2, r1)? {
                    i2 += 1;
                    r2 = ranges2.get(i2).copied();
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if r2.is_none() {
            break 'outer;
        }
    }

    make_multirange(mcx, mltrngtypoid, rng, &mut ranges3)
}

/// hash_multirange (multirangetypes.c).
pub fn hash_multirange_internal(mcx: Mcx<'_>, mi: &mut MultirangeInfo, mr: &[u8]) -> PgResult<u32> {
    let rng = &mut mi.rng;
    let collation = rng.collation;
    range_ops::elem_hash_finfo(rng)?;

    let mut result: u32 = 1;
    for i in 0..multirange_count(mr) as usize {
        let flags = multirange_flags(mr, i);
        let (lower, upper) = multirange_get_bounds(rng, mr, i);
        let lower_hash = if range_has_lbound(flags) {
            ::types_fmgr::function_call1_coll_in(
                rng.elem_hash.as_mut().unwrap(),
                collation,
                mcx,
                lower.val,
            )?
            .as_u32()
        } else {
            0
        };
        let upper_hash = if range_has_ubound(flags) {
            ::types_fmgr::function_call1_coll_in(
                rng.elem_hash.as_mut().unwrap(),
                collation,
                mcx,
                upper.val,
            )?
            .as_u32()
        } else {
            0
        };
        let mut range_hash = ::hashfn::hash_bytes_uint32(flags as u32);
        range_hash ^= lower_hash;
        range_hash = range_hash.rotate_left(1);
        range_hash ^= upper_hash;
        result = (result << 5).wrapping_sub(result).wrapping_add(range_hash);
    }
    Ok(result)
}

/// hash_multirange_extended (multirangetypes.c).
pub fn hash_multirange_extended_internal(
    mcx: Mcx<'_>,
    mi: &mut MultirangeInfo,
    mr: &[u8],
    seed: Datum,
) -> PgResult<u64> {
    let rng = &mut mi.rng;
    let collation = rng.collation;
    range_ops::elem_hash_extended_finfo(rng)?;

    let mut result: u64 = 1;
    for i in 0..multirange_count(mr) as usize {
        let flags = multirange_flags(mr, i);
        let (lower, upper) = multirange_get_bounds(rng, mr, i);
        let lower_hash = if range_has_lbound(flags) {
            ::types_fmgr::function_call2_coll_in(
                rng.elem_hash_extended.as_mut().unwrap(),
                collation,
                mcx,
                lower.val,
                seed,
            )?
            .as_u64()
        } else {
            0
        };
        let upper_hash = if range_has_ubound(flags) {
            ::types_fmgr::function_call2_coll_in(
                rng.elem_hash_extended.as_mut().unwrap(),
                collation,
                mcx,
                upper.val,
                seed,
            )?
            .as_u64()
        } else {
            0
        };
        let mut range_hash = ::hashfn::hash_bytes_uint32_extended(flags as u32, seed.as_u64());
        range_hash ^= lower_hash;
        range_hash = ::hashfn::rotate_high_and_low_32bits(range_hash);
        range_hash ^= upper_hash;
        result = (result << 5).wrapping_sub(result).wrapping_add(range_hash);
    }
    Ok(result)
}
