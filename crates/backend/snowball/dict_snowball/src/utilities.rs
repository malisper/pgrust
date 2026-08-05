
use core::ffi::{c_int, c_uchar, c_void};
use core::mem::size_of;

use crate::mem::{palloc, pfree, repalloc};
use crate::types::{among, symbol, SN_env};

pub const HEAD: usize = 2usize.wrapping_mul(size_of::<c_int>());
pub const CREATE_SIZE: c_int = 1;

#[inline]
unsafe fn sym_cmp(a: *const symbol, b: *const symbol, n: c_int) -> c_int {
    let n = n as usize;
    let sa = unsafe { core::slice::from_raw_parts(a, n) };
    let sb = unsafe { core::slice::from_raw_parts(b, n) };
    for i in 0..n {
        let d = sa[i] as c_int - sb[i] as c_int;
        if d != 0 {
            return d;
        }
    }
    0
}

pub unsafe fn create_s() -> *mut symbol {
    let mem: *mut c_void = palloc(
        HEAD.wrapping_add(((CREATE_SIZE + 1) as usize).wrapping_mul(size_of::<symbol>())),
    );
    if mem.is_null() {
        return core::ptr::null_mut();
    }
    let p = unsafe { (mem as *mut core::ffi::c_char).add(HEAD) } as *mut symbol;
    unsafe {
        *(p as *mut c_int).offset(-2) = CREATE_SIZE;
        *(p as *mut c_int).offset(-1) = 0;
    }
    p
}

pub unsafe fn lose_s(p: *mut symbol) {
    if p.is_null() {
        return;
    }
    unsafe {
        pfree((p as *mut core::ffi::c_char).sub(HEAD) as *mut c_void);
    }
}

pub unsafe fn skip_utf8(p: *const symbol, mut c: c_int, limit: c_int, mut n: c_int) -> c_int {
    let mut b: c_int;
    if n < 0 {
        return -1;
    }
    while n > 0 {
        if c >= limit {
            return -1;
        }
        let fresh = c;
        c += 1;
        b = unsafe { *p.offset(fresh as isize) } as c_int;
        if b >= 0xc0 {
            while c < limit {
                b = unsafe { *p.offset(c as isize) } as c_int;
                if b >= 0xc0 || b < 0x80 {
                    break;
                }
                c += 1;
            }
        }
        n -= 1;
    }
    c
}

pub unsafe fn skip_b_utf8(p: *const symbol, mut c: c_int, limit: c_int, mut n: c_int) -> c_int {
    let mut b: c_int;
    if n < 0 {
        return -1;
    }
    while n > 0 {
        if c <= limit {
            return -1;
        }
        c -= 1;
        b = unsafe { *p.offset(c as isize) } as c_int;
        if b >= 0x80 {
            while c > limit {
                b = unsafe { *p.offset(c as isize) } as c_int;
                if b >= 0xc0 {
                    break;
                }
                c -= 1;
            }
        }
        n -= 1;
    }
    c
}

unsafe fn get_utf8(p: *const symbol, mut c: c_int, l: c_int, slot: *mut c_int) -> c_int {
    let b0: c_int;
    let b1: c_int;
    let b2: c_int;
    if c >= l {
        return 0;
    }
    let fresh1 = c;
    c += 1;
    b0 = unsafe { *p.offset(fresh1 as isize) } as c_int;
    if b0 < 0xc0 || c == l {
        unsafe { *slot = b0 };
        return 1;
    }
    let fresh2 = c;
    c += 1;
    b1 = unsafe { *p.offset(fresh2 as isize) } as c_int & 0x3f;
    if b0 < 0xe0 || c == l {
        unsafe { *slot = (b0 & 0x1f) << 6 | b1 };
        return 2;
    }
    let fresh3 = c;
    c += 1;
    b2 = unsafe { *p.offset(fresh3 as isize) } as c_int & 0x3f;
    if b0 < 0xf0 || c == l {
        unsafe { *slot = (b0 & 0xf) << 12 | b1 << 6 | b2 };
        return 3;
    }
    unsafe {
        *slot = (b0 & 0x7) << 18 | b1 << 12 | b2 << 6 | *p.offset(c as isize) as c_int & 0x3f;
    }
    4
}

unsafe fn get_b_utf8(p: *const symbol, mut c: c_int, lb: c_int, slot: *mut c_int) -> c_int {
    let mut a: c_int;
    let mut b: c_int;
    if c <= lb {
        return 0;
    }
    c -= 1;
    b = unsafe { *p.offset(c as isize) } as c_int;
    if b < 0x80 || c == lb {
        unsafe { *slot = b };
        return 1;
    }
    a = b & 0x3f;
    c -= 1;
    b = unsafe { *p.offset(c as isize) } as c_int;
    if b >= 0xc0 || c == lb {
        unsafe { *slot = (b & 0x1f) << 6 | a };
        return 2;
    }
    a |= (b & 0x3f) << 6;
    c -= 1;
    b = unsafe { *p.offset(c as isize) } as c_int;
    if b >= 0xe0 || c == lb {
        unsafe { *slot = (b & 0xf) << 12 | a };
        return 3;
    }
    c -= 1;
    unsafe {
        *slot = (*p.offset(c as isize) as c_int & 0x7) << 18 | (b & 0x3f) << 12 | a;
    }
    4
}

pub unsafe fn in_grouping_U(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int = 0;
        let w = unsafe { get_utf8((*z).p, (*z).c, (*z).l, &mut ch) };
        if w == 0 {
            return -1;
        }
        if ch > max
            || {
                ch -= min;
                ch < 0
            }
            || unsafe { *s.offset((ch >> 3) as isize) } as c_int & (0x1 << (ch & 0x7)) == 0
        {
            return w;
        }
        unsafe { (*z).c += w };
        if repeat == 0 {
            break;
        }
    }
    0
}

pub unsafe fn in_grouping_b_U(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int = 0;
        let w = unsafe { get_b_utf8((*z).p, (*z).c, (*z).lb, &mut ch) };
        if w == 0 {
            return -1;
        }
        if ch > max
            || {
                ch -= min;
                ch < 0
            }
            || unsafe { *s.offset((ch >> 3) as isize) } as c_int & (0x1 << (ch & 0x7)) == 0
        {
            return w;
        }
        unsafe { (*z).c -= w };
        if repeat == 0 {
            break;
        }
    }
    0
}

pub unsafe fn out_grouping_U(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int = 0;
        let w = unsafe { get_utf8((*z).p, (*z).c, (*z).l, &mut ch) };
        if w == 0 {
            return -1;
        }
        if !(ch > max
            || {
                ch -= min;
                ch < 0
            }
            || unsafe { *s.offset((ch >> 3) as isize) } as c_int & (0x1 << (ch & 0x7)) == 0)
        {
            return w;
        }
        unsafe { (*z).c += w };
        if repeat == 0 {
            break;
        }
    }
    0
}

pub unsafe fn out_grouping_b_U(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int = 0;
        let w = unsafe { get_b_utf8((*z).p, (*z).c, (*z).lb, &mut ch) };
        if w == 0 {
            return -1;
        }
        if !(ch > max
            || {
                ch -= min;
                ch < 0
            }
            || unsafe { *s.offset((ch >> 3) as isize) } as c_int & (0x1 << (ch & 0x7)) == 0)
        {
            return w;
        }
        unsafe { (*z).c -= w };
        if repeat == 0 {
            break;
        }
    }
    0
}

pub unsafe fn in_grouping(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int;
        if unsafe { (*z).c >= (*z).l } {
            return -1;
        }
        ch = unsafe { *(*z).p.offset((*z).c as isize) } as c_int;
        if ch > max
            || {
                ch -= min;
                ch < 0
            }
            || unsafe { *s.offset((ch >> 3) as isize) } as c_int & (0x1 << (ch & 0x7)) == 0
        {
            return 1;
        }
        unsafe { (*z).c += 1 };
        if repeat == 0 {
            break;
        }
    }
    0
}

pub unsafe fn in_grouping_b(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int;
        if unsafe { (*z).c <= (*z).lb } {
            return -1;
        }
        ch = unsafe { *(*z).p.offset(((*z).c - 1) as isize) } as c_int;
        if ch > max
            || {
                ch -= min;
                ch < 0
            }
            || unsafe { *s.offset((ch >> 3) as isize) } as c_int & (0x1 << (ch & 0x7)) == 0
        {
            return 1;
        }
        unsafe { (*z).c -= 1 };
        if repeat == 0 {
            break;
        }
    }
    0
}

pub unsafe fn out_grouping(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int;
        if unsafe { (*z).c >= (*z).l } {
            return -1;
        }
        ch = unsafe { *(*z).p.offset((*z).c as isize) } as c_int;
        if !(ch > max
            || {
                ch -= min;
                ch < 0
            }
            || unsafe { *s.offset((ch >> 3) as isize) } as c_int & (0x1 << (ch & 0x7)) == 0)
        {
            return 1;
        }
        unsafe { (*z).c += 1 };
        if repeat == 0 {
            break;
        }
    }
    0
}

pub unsafe fn out_grouping_b(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int;
        if unsafe { (*z).c <= (*z).lb } {
            return -1;
        }
        ch = unsafe { *(*z).p.offset(((*z).c - 1) as isize) } as c_int;
        if !(ch > max
            || {
                ch -= min;
                ch < 0
            }
            || unsafe { *s.offset((ch >> 3) as isize) } as c_int & (0x1 << (ch & 0x7)) == 0)
        {
            return 1;
        }
        unsafe { (*z).c -= 1 };
        if repeat == 0 {
            break;
        }
    }
    0
}

pub unsafe fn eq_s(z: *mut SN_env, s_size: c_int, s: *const symbol) -> c_int {
    unsafe {
        if (*z).l - (*z).c < s_size || sym_cmp((*z).p.offset((*z).c as isize), s, s_size) != 0 {
            return 0;
        }
        (*z).c += s_size;
    }
    1
}

pub unsafe fn eq_s_b(z: *mut SN_env, s_size: c_int, s: *const symbol) -> c_int {
    unsafe {
        if (*z).c - (*z).lb < s_size
            || sym_cmp(
                (*z).p.offset((*z).c as isize).offset(-(s_size as isize)),
                s,
                s_size,
            ) != 0
        {
            return 0;
        }
        (*z).c -= s_size;
    }
    1
}

pub unsafe fn eq_v(z: *mut SN_env, p: *const symbol) -> c_int {
    unsafe { eq_s(z, *(p as *mut c_int).offset(-1), p) }
}

pub unsafe fn eq_v_b(z: *mut SN_env, p: *const symbol) -> c_int {
    unsafe { eq_s_b(z, *(p as *mut c_int).offset(-1), p) }
}

pub unsafe fn find_among(z: *mut SN_env, v: *const among, v_size: c_int) -> c_int {
    let mut i: c_int = 0;
    let mut j: c_int = v_size;
    let c: c_int = unsafe { (*z).c };
    let l: c_int = unsafe { (*z).l };
    let q: *const symbol = unsafe { (*z).p.offset(c as isize) };
    let mut w: *const among;
    let mut common_i: c_int = 0;
    let mut common_j: c_int = 0;
    let mut first_key_inspected: c_int = 0;
    loop {
        let k: c_int = i + (j - i >> 1);
        let mut diff: c_int = 0;
        let mut common: c_int = if common_i < common_j {
            common_i
        } else {
            common_j
        };
        w = unsafe { v.offset(k as isize) };
        let mut i2: c_int = common;
        while i2 < unsafe { (*w).s_size } {
            if c + common == l {
                diff = -1;
                break;
            } else {
                diff = unsafe { *q.offset(common as isize) } as c_int
                    - unsafe { *(*w).s.offset(i2 as isize) } as c_int;
                if diff != 0 {
                    break;
                }
                common += 1;
                i2 += 1;
            }
        }
        if diff < 0 {
            j = k;
            common_j = common;
        } else {
            i = k;
            common_i = common;
        }
        if j - i <= 1 {
            if i > 0 {
                break;
            }
            if j == i {
                break;
            }
            if first_key_inspected != 0 {
                break;
            }
            first_key_inspected = 1;
        }
    }
    loop {
        w = unsafe { v.offset(i as isize) };
        if common_i >= unsafe { (*w).s_size } {
            unsafe { (*z).c = c + (*w).s_size };
            if unsafe { (*w).function.is_none() } {
                return unsafe { (*w).result };
            }
            let res = unsafe { ((*w).function.unwrap_unchecked())(z) };
            unsafe { (*z).c = c + (*w).s_size };
            if res != 0 {
                return unsafe { (*w).result };
            }
        }
        i = unsafe { (*w).substring_i };
        if i < 0 {
            return 0;
        }
    }
}

pub unsafe fn find_among_b(z: *mut SN_env, v: *const among, v_size: c_int) -> c_int {
    let mut i: c_int = 0;
    let mut j: c_int = v_size;
    let c: c_int = unsafe { (*z).c };
    let lb: c_int = unsafe { (*z).lb };
    let q: *const symbol = unsafe { (*z).p.offset(c as isize).offset(-1) };
    let mut w: *const among;
    let mut common_i: c_int = 0;
    let mut common_j: c_int = 0;
    let mut first_key_inspected: c_int = 0;
    loop {
        let k: c_int = i + (j - i >> 1);
        let mut diff: c_int = 0;
        let mut common: c_int = if common_i < common_j {
            common_i
        } else {
            common_j
        };
        w = unsafe { v.offset(k as isize) };
        let mut i2: c_int = unsafe { (*w).s_size } - 1 - common;
        while i2 >= 0 {
            if c - common == lb {
                diff = -1;
                break;
            } else {
                diff = unsafe { *q.offset(-common as isize) } as c_int
                    - unsafe { *(*w).s.offset(i2 as isize) } as c_int;
                if diff != 0 {
                    break;
                }
                common += 1;
                i2 -= 1;
            }
        }
        if diff < 0 {
            j = k;
            common_j = common;
        } else {
            i = k;
            common_i = common;
        }
        if j - i <= 1 {
            if i > 0 {
                break;
            }
            if j == i {
                break;
            }
            if first_key_inspected != 0 {
                break;
            }
            first_key_inspected = 1;
        }
    }
    loop {
        w = unsafe { v.offset(i as isize) };
        if common_i >= unsafe { (*w).s_size } {
            unsafe { (*z).c = c - (*w).s_size };
            if unsafe { (*w).function.is_none() } {
                return unsafe { (*w).result };
            }
            let res = unsafe { ((*w).function.unwrap_unchecked())(z) };
            unsafe { (*z).c = c - (*w).s_size };
            if res != 0 {
                return unsafe { (*w).result };
            }
        }
        i = unsafe { (*w).substring_i };
        if i < 0 {
            return 0;
        }
    }
}

unsafe fn increase_size(p: *mut symbol, n: c_int) -> *mut symbol {
    let new_size: c_int = n + 20;
    let mem: *mut c_void = unsafe {
        repalloc(
            (p as *mut core::ffi::c_char).sub(HEAD) as *mut c_void,
            HEAD.wrapping_add(((new_size + 1) as usize).wrapping_mul(size_of::<symbol>())),
        )
    };
    if mem.is_null() {
        unsafe { lose_s(p) };
        return core::ptr::null_mut();
    }
    let q = unsafe { (mem as *mut core::ffi::c_char).add(HEAD) } as *mut symbol;
    unsafe { *(q as *mut c_int).offset(-2) = new_size };
    q
}

pub unsafe fn replace_s(
    z: *mut SN_env,
    c_bra: c_int,
    c_ket: c_int,
    s_size: c_int,
    s: *const symbol,
    adjptr: *mut c_int,
) -> c_int {
    let adjustment: c_int;
    let len: c_int;
    unsafe {
        if (*z).p.is_null() {
            (*z).p = create_s();
            if (*z).p.is_null() {
                return -1;
            }
        }
        adjustment = s_size - (c_ket - c_bra);
        len = *((*z).p as *mut c_int).offset(-1);
        if adjustment != 0 {
            if adjustment + len > *((*z).p as *mut c_int).offset(-2) {
                (*z).p = increase_size((*z).p, adjustment + len);
                if (*z).p.is_null() {
                    return -1;
                }
            }
            core::ptr::copy(
                (*z).p.offset(c_ket as isize) as *const symbol,
                (*z).p.offset(c_ket as isize).offset(adjustment as isize),
                (len - c_ket) as usize,
            );
            *((*z).p as *mut c_int).offset(-1) = adjustment + len;
            (*z).l += adjustment;
            if (*z).c >= c_ket {
                (*z).c += adjustment;
            } else if (*z).c > c_bra {
                (*z).c = c_bra;
            }
        }
        if s_size != 0 {
            core::ptr::copy(s, (*z).p.offset(c_bra as isize), s_size as usize);
        }
        if !adjptr.is_null() {
            *adjptr = adjustment;
        }
    }
    0
}

unsafe fn slice_check(z: *mut SN_env) -> c_int {
    unsafe {
        if (*z).bra < 0
            || (*z).bra > (*z).ket
            || (*z).ket > (*z).l
            || (*z).p.is_null()
            || (*z).l > *((*z).p as *mut c_int).offset(-1)
        {
            return -1;
        }
    }
    0
}

pub unsafe fn slice_from_s(z: *mut SN_env, s_size: c_int, s: *const symbol) -> c_int {
    unsafe {
        if slice_check(z) != 0 {
            return -1;
        }
        replace_s(z, (*z).bra, (*z).ket, s_size, s, core::ptr::null_mut())
    }
}

pub unsafe fn slice_from_v(z: *mut SN_env, p: *const symbol) -> c_int {
    unsafe { slice_from_s(z, *(p as *mut c_int).offset(-1), p) }
}

pub unsafe fn slice_del(z: *mut SN_env) -> c_int {
    unsafe { slice_from_s(z, 0, core::ptr::null()) }
}

pub unsafe fn insert_s(
    z: *mut SN_env,
    bra: c_int,
    ket: c_int,
    s_size: c_int,
    s: *const symbol,
) -> c_int {
    let mut adjustment: c_int = 0;
    unsafe {
        if replace_s(z, bra, ket, s_size, s, &mut adjustment) != 0 {
            return -1;
        }
        if bra <= (*z).bra {
            (*z).bra += adjustment;
        }
        if bra <= (*z).ket {
            (*z).ket += adjustment;
        }
    }
    0
}

pub unsafe fn insert_v(z: *mut SN_env, bra: c_int, ket: c_int, p: *const symbol) -> c_int {
    unsafe { insert_s(z, bra, ket, *(p as *mut c_int).offset(-1), p) }
}

pub unsafe fn slice_to(z: *mut SN_env, mut p: *mut symbol) -> *mut symbol {
    unsafe {
        if slice_check(z) != 0 {
            lose_s(p);
            return core::ptr::null_mut();
        }
        let len: c_int = (*z).ket - (*z).bra;
        if *(p as *mut c_int).offset(-2) < len {
            p = increase_size(p, len);
            if p.is_null() {
                return core::ptr::null_mut();
            }
        }
        core::ptr::copy_nonoverlapping((*z).p.offset((*z).bra as isize), p, len as usize);
        *(p as *mut c_int).offset(-1) = len;
        p
    }
}

pub unsafe fn assign_to(z: *mut SN_env, mut p: *mut symbol) -> *mut symbol {
    unsafe {
        let len: c_int = (*z).l;
        if *(p as *mut c_int).offset(-2) < len {
            p = increase_size(p, len);
            if p.is_null() {
                return core::ptr::null_mut();
            }
        }
        core::ptr::copy_nonoverlapping((*z).p, p, len as usize);
        *(p as *mut c_int).offset(-1) = len;
        p
    }
}

pub unsafe fn len_utf8(mut p: *const symbol) -> c_int {
    let mut size: c_int = unsafe { *(p as *mut c_int).offset(-1) };
    let mut len: c_int = 0;
    loop {
        let fresh = size;
        size -= 1;
        if fresh == 0 {
            break;
        }
        let b: symbol = unsafe { *p };
        p = unsafe { p.offset(1) };
        if b as c_int >= 0xc0 || (b as c_int) < 0x80 {
            len += 1;
        }
    }
    len
}
