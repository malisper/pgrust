//! Port of snowball `src/backend/snowball/libstemmer/utilities.c` (PostgreSQL
//! 18.3): the runtime support routines the generated stemmers call
//! (`find_among`, `slice_*`, `eq_*`, grouping checks, UTF-8 helpers, and the
//! `symbol*` buffer allocator).
//!
//! The algorithm is preserved byte-for-byte from the C — these are exact state
//! machines whose behaviour must match PostgreSQL's. The only changes are
//! mechanical: the C runtime allocator (`palloc`/`repalloc`/`pfree`, see
//! `src/include/snowball/header.h:47-65`) is routed through the in-crate
//! [`crate::mem`] seam, and `memcmp`/`memmove` are expressed with `core` slice /
//! `ptr::copy` operations.
//!
//! ## The `symbol*` buffer layout
//!
//! A snowball working buffer is a `symbol*` (`*mut u8`) whose two `int`s
//! immediately *before* the pointer hold `[capacity, length]`
//! (`src/include/snowball/libstemmer/header.h:9-13`): `HEAD` is `2*sizeof(int)`
//! bytes, `CAPACITY(p)` is `((int*)p)[-2]`, `SIZE(p)` is `((int*)p)[-1]`. All the
//! buffer routines read/write these via negative `c_int` offsets; this is
//! load-bearing and matches the C exactly.

use core::ffi::{c_int, c_uchar, c_void};
use core::mem::size_of;

use crate::mem::{palloc, pfree, repalloc};
use crate::types::{among, symbol, SN_env};

/// Size of the hidden `[capacity, length]` header preceding a `symbol*` buffer.
/// `src/include/snowball/libstemmer/header.h:9` — `#define HEAD 2*sizeof(int)`.
pub const HEAD: usize = 2usize.wrapping_mul(size_of::<c_int>());
/// Initial capacity for a freshly-created buffer.
/// `src/backend/snowball/libstemmer/utilities.c:3` — `#define CREATE_SIZE 1`.
pub const CREATE_SIZE: c_int = 1;

/// `memcmp` over `n` symbols, returning C semantics (0 == equal). Used by the
/// `eq_*` routines, which only test for equality, but kept general for fidelity.
///
/// # Safety
/// `a` and `b` must point at `n` valid symbols.
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

/// Allocate a fresh `symbol*` buffer with the hidden header initialised to
/// `[CREATE_SIZE, 0]`. Returns null on allocation failure. Mirrors C `create_s`
/// (`utilities.c:5-13`).
///
/// # Safety
/// The allocation seam must be installed. The returned pointer owns memory that
/// must eventually be released with [`lose_s`].
pub unsafe fn create_s() -> *mut symbol {
    // utilities.c:7 — malloc(HEAD + (CREATE_SIZE + 1) * sizeof(symbol))
    let mem: *mut c_void = palloc(
        HEAD.wrapping_add(((CREATE_SIZE + 1) as usize).wrapping_mul(size_of::<symbol>())),
    );
    if mem.is_null() {
        return core::ptr::null_mut();
    }
    // utilities.c:9 — p = (symbol *) (HEAD + (char *) mem);
    let p = unsafe { (mem as *mut core::ffi::c_char).add(HEAD) } as *mut symbol;
    unsafe {
        // utilities.c:10 — CAPACITY(p) = CREATE_SIZE;  (= ((int*)p)[-2])
        *(p as *mut c_int).offset(-2) = CREATE_SIZE;
        // utilities.c:11 — SET_SIZE(p, 0);  (= ((int*)p)[-1] = 0)
        *(p as *mut c_int).offset(-1) = 0;
    }
    p
}

/// Free a buffer obtained from [`create_s`] (or grown via [`increase_size`]),
/// accounting for the hidden header. A null pointer is a no-op. Mirrors C
/// `lose_s` (`utilities.c:15-18`).
///
/// # Safety
/// `p` must be null or a live buffer from [`create_s`]/[`increase_size`].
pub unsafe fn lose_s(p: *mut symbol) {
    if p.is_null() {
        return;
    }
    unsafe {
        // utilities.c:17 — free((char *) p - HEAD);
        pfree((p as *mut core::ffi::c_char).sub(HEAD) as *mut c_void);
    }
}

/// Advance the cursor `c` forward by `n` UTF-8 characters within `[.., limit]`,
/// returning the new cursor or -1 on under/overrun. Mirrors C `skip_utf8`
/// (`utilities.c:27-43`).
///
/// # Safety
/// `p` must point at a buffer covering the indices touched between `c` and
/// `limit`.
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

/// Move the cursor `c` backward by `n` UTF-8 characters within `(limit, ..]`,
/// returning the new cursor or -1 on under/overrun. Mirrors C `skip_b_utf8`
/// (`utilities.c:52-67`).
///
/// # Safety
/// `p` must point at a buffer covering the indices touched between `limit` and
/// `c`.
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

/// Decode the UTF-8 character at cursor `c` (forward) into `*slot`, returning
/// its byte width (0 at end of input). Mirrors C `get_utf8`
/// (`utilities.c:71-91`).
///
/// # Safety
/// `p` must cover `[c, l)`; `slot` must be writable.
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

/// Decode the UTF-8 character ending at cursor `c` (backward) into `*slot`,
/// returning its byte width (0 at start of input). Mirrors C `get_b_utf8`
/// (`utilities.c:93-115`).
///
/// # Safety
/// `p` must cover `(lb, c]`; `slot` must be writable.
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

/// UTF-8 forward "char is in grouping `s`" test (with optional `repeat`).
/// Mirrors C `in_grouping_U` (`utilities.c:117-127`).
///
/// # Safety
/// `z` must be a live env; `s` must be a grouping bitset covering `max-min`.
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

/// UTF-8 backward "char is in grouping `s`" test. Mirrors C `in_grouping_b_U`
/// (`utilities.c:129-139`).
///
/// # Safety
/// As [`in_grouping_U`].
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

/// UTF-8 forward "char is *not* in grouping `s`" test. Mirrors C
/// `out_grouping_U` (`utilities.c:141-151`).
///
/// # Safety
/// As [`in_grouping_U`].
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

/// UTF-8 backward "char is *not* in grouping `s`" test. Mirrors C
/// `out_grouping_b_U` (`utilities.c:153-163`).
///
/// # Safety
/// As [`in_grouping_U`].
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

/// Single-byte forward "char is in grouping `s`" test. Mirrors C `in_grouping`
/// (`utilities.c:167-177`).
///
/// # Safety
/// As [`in_grouping_U`].
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

/// Single-byte backward "char is in grouping `s`" test. Mirrors C
/// `in_grouping_b` (`utilities.c:179-189`).
///
/// # Safety
/// As [`in_grouping_U`].
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

/// Single-byte forward "char is *not* in grouping `s`" test. Mirrors C
/// `out_grouping` (`utilities.c:191-201`).
///
/// # Safety
/// As [`in_grouping_U`].
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

/// Single-byte backward "char is *not* in grouping `s`" test. Mirrors C
/// `out_grouping_b` (`utilities.c:203-213`).
///
/// # Safety
/// As [`in_grouping_U`].
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

/// Forward literal match of `s` (length `s_size`) at the cursor, advancing it on
/// success. Returns 1 on match, 0 otherwise. Mirrors C `eq_s`
/// (`utilities.c:215-218`).
///
/// # Safety
/// `z` must be a live env; `s` must point at `s_size` valid symbols.
pub unsafe fn eq_s(z: *mut SN_env, s_size: c_int, s: *const symbol) -> c_int {
    unsafe {
        if (*z).l - (*z).c < s_size || sym_cmp((*z).p.offset((*z).c as isize), s, s_size) != 0 {
            return 0;
        }
        (*z).c += s_size;
    }
    1
}

/// Backward literal match of `s` ending at the cursor, retreating it on success.
/// Mirrors C `eq_s_b` (`utilities.c:220-223`).
///
/// # Safety
/// As [`eq_s`].
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

/// Forward match of a length-prefixed `symbol*` literal `p`. Mirrors C `eq_v`
/// (`utilities.c:225-227`).
///
/// # Safety
/// `p` must be a length-prefixed buffer (`SIZE(p)` readable at `((int*)p)[-1]`).
pub unsafe fn eq_v(z: *mut SN_env, p: *const symbol) -> c_int {
    unsafe { eq_s(z, *(p as *mut c_int).offset(-1), p) }
}

/// Backward match of a length-prefixed `symbol*` literal `p`. Mirrors C `eq_v_b`
/// (`utilities.c:229-231`).
///
/// # Safety
/// As [`eq_v`].
pub unsafe fn eq_v_b(z: *mut SN_env, p: *const symbol) -> c_int {
    unsafe { eq_s_b(z, *(p as *mut c_int).offset(-1), p) }
}

/// Forward binary search of the `among` table `v` (`v_size` entries) at the
/// cursor, running guard functions as needed; returns the matched `result` or 0.
/// Mirrors C `find_among` exactly (`utilities.c:233-294`).
///
/// # Safety
/// `z` must be a live env; `v` must point at `v_size` valid `among` entries with
/// valid `s`/`substring_i` links.
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

/// Backward binary search of the `among` table `v`. Mirrors C `find_among_b`
/// (`utilities.c:298-349`).
///
/// # Safety
/// As [`find_among`].
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

/// Grow a buffer to hold at least `n` symbols (plus slack), preserving the
/// hidden header and copying the data. Returns null (after freeing `p`) on
/// failure. Mirrors C `increase_size` (`utilities.c:355-367`).
///
/// # Safety
/// `p` must be a live buffer from [`create_s`]/this fn.
unsafe fn increase_size(p: *mut symbol, n: c_int) -> *mut symbol {
    // utilities.c:357 — new_size = n + 20;
    let new_size: c_int = n + 20;
    // utilities.c:358 — realloc((char*)p - HEAD, HEAD + (new_size+1)*sizeof(symbol))
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
    // utilities.c:365 — CAPACITY(q) = new_size;
    unsafe { *(q as *mut c_int).offset(-2) = new_size };
    q
}

/// Splice `s` (length `s_size`) into the buffer in place of `[c_bra, c_ket)`,
/// resizing and shifting the tail as needed and adjusting cursor/limit. Writes
/// the net length change to `*adjptr` if non-null. Returns 0 on success, -1 on
/// allocation failure. Mirrors C `replace_s` (`utilities.c:374-403`).
///
/// # Safety
/// `z` must be a live env; `s` must point at `s_size` valid symbols (or be null
/// when `s_size == 0`); `adjptr` must be null or writable.
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
        // utilities.c:382 — adjustment = s_size - (c_ket - c_bra);
        adjustment = s_size - (c_ket - c_bra);
        // utilities.c:383 — len = SIZE(z->p);  (= ((int*)p)[-1])
        len = *((*z).p as *mut c_int).offset(-1);
        if adjustment != 0 {
            // utilities.c:385 — if (adjustment + len > CAPACITY(z->p))
            if adjustment + len > *((*z).p as *mut c_int).offset(-2) {
                (*z).p = increase_size((*z).p, adjustment + len);
                if (*z).p.is_null() {
                    return -1;
                }
            }
            // utilities.c:389-391 — memmove(p + c_ket + adjustment, p + c_ket,
            //                                (len - c_ket) * sizeof(symbol))
            core::ptr::copy(
                (*z).p.offset(c_ket as isize) as *const symbol,
                (*z).p.offset(c_ket as isize).offset(adjustment as isize),
                (len - c_ket) as usize,
            );
            // utilities.c:392 — SET_SIZE(z->p, adjustment + len);
            *((*z).p as *mut c_int).offset(-1) = adjustment + len;
            (*z).l += adjustment;
            if (*z).c >= c_ket {
                (*z).c += adjustment;
            } else if (*z).c > c_bra {
                (*z).c = c_bra;
            }
        }
        // utilities.c:399 — if (s_size) memmove(p + c_bra, s, s_size*sizeof(symbol))
        if s_size != 0 {
            core::ptr::copy(s, (*z).p.offset(c_bra as isize), s_size as usize);
        }
        if !adjptr.is_null() {
            *adjptr = adjustment;
        }
    }
    0
}

/// Validate that the current `[bra, ket)` slice is well-formed against the
/// buffer length. Returns 0 if OK, -1 otherwise. Mirrors C `slice_check`
/// (`utilities.c:405-420`).
///
/// # Safety
/// `z` must be a live env.
unsafe fn slice_check(z: *mut SN_env) -> c_int {
    unsafe {
        if (*z).bra < 0
            || (*z).bra > (*z).ket
            || (*z).ket > (*z).l
            || (*z).p.is_null()
            // utilities.c:411 — z->l > SIZE(z->p)
            || (*z).l > *((*z).p as *mut c_int).offset(-1)
        {
            return -1;
        }
    }
    0
}

/// Replace the current slice with `s` (length `s_size`). Mirrors C `slice_from_s`
/// (`utilities.c:422-425`).
///
/// # Safety
/// `z` must be a live env; `s`/`s_size` as for [`replace_s`].
pub unsafe fn slice_from_s(z: *mut SN_env, s_size: c_int, s: *const symbol) -> c_int {
    unsafe {
        if slice_check(z) != 0 {
            return -1;
        }
        replace_s(z, (*z).bra, (*z).ket, s_size, s, core::ptr::null_mut())
    }
}

/// Replace the current slice with a length-prefixed literal `p`. Mirrors C
/// `slice_from_v` (`utilities.c:427-429`).
///
/// # Safety
/// `p` must be a length-prefixed buffer.
pub unsafe fn slice_from_v(z: *mut SN_env, p: *const symbol) -> c_int {
    unsafe { slice_from_s(z, *(p as *mut c_int).offset(-1), p) }
}

/// Delete the current slice. Mirrors C `slice_del` (`utilities.c:431-433`).
///
/// # Safety
/// `z` must be a live env.
pub unsafe fn slice_del(z: *mut SN_env) -> c_int {
    unsafe { slice_from_s(z, 0, core::ptr::null()) }
}

/// Insert `s` (length `s_size`) at `[bra, ket)`, adjusting the env's `bra`/`ket`
/// for the shift. Mirrors C `insert_s` (`utilities.c:435-442`).
///
/// # Safety
/// `z` must be a live env; `s`/`s_size` as for [`replace_s`].
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

/// Insert a length-prefixed literal `p` at `[bra, ket)`. Mirrors C `insert_v`
/// (`utilities.c:444-446`).
///
/// # Safety
/// `p` must be a length-prefixed buffer.
pub unsafe fn insert_v(z: *mut SN_env, bra: c_int, ket: c_int, p: *const symbol) -> c_int {
    unsafe { insert_s(z, bra, ket, *(p as *mut c_int).offset(-1), p) }
}

/// Copy the current slice into buffer `p` (growing it as needed), storing the
/// length in `p`'s header. Returns the (possibly relocated) `p`, or null on
/// failure. Mirrors C `slice_to` (`utilities.c:448-464`).
///
/// # Safety
/// `z` must be a live env; `p` must be a live buffer from [`create_s`].
pub unsafe fn slice_to(z: *mut SN_env, mut p: *mut symbol) -> *mut symbol {
    unsafe {
        if slice_check(z) != 0 {
            lose_s(p);
            return core::ptr::null_mut();
        }
        let len: c_int = (*z).ket - (*z).bra;
        // utilities.c:455 — if (CAPACITY(p) < len)
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

/// Copy the entire working buffer into `p` (growing it as needed). Mirrors C
/// `assign_to` (`utilities.c:466-476`).
///
/// # Safety
/// `z` must be a live env; `p` must be a live buffer from [`create_s`].
pub unsafe fn assign_to(z: *mut SN_env, mut p: *mut symbol) -> *mut symbol {
    unsafe {
        let len: c_int = (*z).l;
        // utilities.c:468 — if (CAPACITY(p) < len)
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

/// Count UTF-8 characters in a length-prefixed `symbol*` buffer `p`. Mirrors C
/// `len_utf8` (`utilities.c:478-486`).
///
/// # Safety
/// `p` must be a length-prefixed buffer.
pub unsafe fn len_utf8(mut p: *const symbol) -> c_int {
    // utilities.c:479 — size = SIZE(p);
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
