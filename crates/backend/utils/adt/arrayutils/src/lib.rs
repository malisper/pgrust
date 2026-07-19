#![no_std]

extern crate alloc;

use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

pub const MAXDIM: i32 = 6;
pub const MAX_ALLOC_SIZE: usize = 0x3FFF_FFFF;
// MaxAllocSize / sizeof(Datum): SIZEOF_DATUM is pinned to 8 on every target
// (usize would halve the divisor on wasm32 and double the limit).
pub const MAX_ARRAY_SIZE: i64 = (MAX_ALLOC_SIZE / 8) as i64;

#[inline]
fn add_s32_overflow(a: i32, b: i32, out: &mut i32) -> bool {
    let (v, o) = a.overflowing_add(b);
    *out = v;
    o
}

#[cold]
#[inline(never)]
fn array_size_exceeded() -> PgError {
    PgError::error(alloc::format!(
        "array size exceeds the maximum allowed ({})",
        MAX_ARRAY_SIZE
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

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

#[inline]
pub fn array_get_n_items(ndim: i32, dims: &[i32]) -> PgResult<i32> {
    array_get_n_items_safe(ndim, dims, None)
}

#[inline]
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
        // A negative dimension implies that UB-LB overflowed.
        if dims[i] < 0 {
            return ereturn(escontext.take(), -1, array_size_exceeded());
        }
        let prod: i64 = ret as i64 * dims[i] as i64;
        ret = prod as i32;
        if ret as i64 != prod {
            return ereturn(escontext.take(), -1, array_size_exceeded());
        }
    }
    debug_assert!(ret >= 0);
    if ret as i64 > MAX_ARRAY_SIZE {
        return ereturn(escontext.take(), -1, array_size_exceeded());
    }
    Ok(ret)
}

pub fn array_check_bounds(ndim: i32, dims: &[i32], lb: &[i32]) -> PgResult<()> {
    array_check_bounds_safe(ndim, dims, lb, None)?;
    Ok(())
}

pub fn array_check_bounds_safe(
    ndim: i32,
    dims: &[i32],
    lb: &[i32],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    for i in 0..ndim as usize {
        let mut sum = 0i32;
        if add_s32_overflow(dims[i], lb[i], &mut sum) {
            return ereturn(
                escontext.take(),
                false,
                PgError::error(alloc::format!("array lower bound is too large: {}", lb[i]))
                    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            );
        }
    }
    Ok(true)
}

pub fn mda_get_range(n: i32, span: &mut [i32], st: &[i32], endp: &[i32]) {
    for i in 0..n as usize {
        span[i] = endp[i] - st[i] + 1;
    }
}

pub fn mda_get_prod(n: i32, range: &[i32], prod: &mut [i32]) {
    prod[(n - 1) as usize] = 1;
    let mut i = n - 2;
    while i >= 0 {
        let iu = i as usize;
        prod[iu] = prod[iu + 1] * range[iu + 1];
        i -= 1;
    }
}

pub fn mda_get_offset_values(n: i32, dist: &mut [i32], prod: &[i32], span: &[i32]) {
    dist[(n - 1) as usize] = 0;
    let mut j = n - 2;
    while j >= 0 {
        let ju = j as usize;
        dist[ju] = prod[ju] - 1;
        for i in (j + 1) as usize..n as usize {
            dist[ju] -= (span[i] - 1) * prod[i];
        }
        j -= 1;
    }
}

// Lexicographically-next n-tuple in `curr`, i-th element < i-th of `span`.
// Returns -1 if none, else the advanced subscript position.
pub fn mda_next_tuple(n: i32, curr: &mut [i32], span: &[i32]) -> i32 {
    if n <= 0 {
        return -1;
    }
    let last = (n - 1) as usize;
    curr[last] = (curr[last] + 1) % span[last];
    let mut i = n - 1;
    while i != 0 && curr[i as usize] == 0 {
        curr[(i - 1) as usize] = (curr[(i - 1) as usize] + 1) % span[(i - 1) as usize];
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

#[cfg(test)]
mod tests {
    use super::*;
    extern crate std;

    #[test]
    fn offset_linearizes() {
        // 2x3 array, lb [1,1]; element [2,3] -> (2-1)*3 + (3-1) = 5
        assert_eq!(array_get_offset(2, &[2, 3], &[1, 1], &[2, 3]), 5);
        assert_eq!(array_get_offset(1, &[4], &[1], &[1]), 0);
    }

    #[test]
    fn nitems_products_and_overflow() {
        assert_eq!(array_get_n_items(2, &[2, 3]).unwrap(), 6);
        assert_eq!(array_get_n_items(0, &[]).unwrap(), 0);
        // overflow of int32 product
        let mut esc = SoftErrorContext::new(true);
        let r = array_get_n_items_safe(2, &[100000, 100000], Some(&mut esc)).unwrap();
        assert_eq!(r, -1);
        assert!(esc.error_occurred());
    }

    #[test]
    fn check_bounds_detects_overflow() {
        assert!(array_check_bounds(1, &[10], &[1]).is_ok());
        assert!(array_check_bounds(1, &[10], &[i32::MAX - 5]).is_err());
    }

    #[test]
    fn mda_prod_range_next() {
        let mut prod = [0i32; 2];
        mda_get_prod(2, &[2, 3], &mut prod);
        assert_eq!(prod, [3, 1]);
        let mut span = [0i32; 2];
        mda_get_range(2, &mut span, &[1, 1], &[2, 3]);
        assert_eq!(span, [2, 3]);
        let mut curr = [0i32; 2];
        let adv = mda_next_tuple(2, &mut curr, &[2, 3]);
        assert_eq!((curr, adv), ([0, 1], 1));
    }
}
