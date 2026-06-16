//! Port of snowball `src/backend/snowball/libstemmer/api.c` (PostgreSQL 18.3):
//! environment lifecycle.
//!
//! `SN_create_env` / `SN_close_env` / `SN_set_current` exactly as in the
//! original C, with the C-runtime `calloc`/`free` routed through the in-crate
//! [`crate::mem`] seam (PostgreSQL's `src/include/snowball/header.h:52-65` maps
//! `calloc(a,b)` -> `palloc0((a)*(b))` and `free` -> `pfree`), and
//! `create_s`/`lose_s`/`replace_s` taken from [`crate::utilities`]. The `goto
//! error` cleanup is expressed as an early-return-on-failure block; semantics are
//! identical.

use core::ffi::c_int;
use core::mem::size_of;

use crate::mem::{palloc0, pfree};
use crate::types::{symbol, SN_env};
use crate::utilities::{create_s, lose_s, replace_s};

/// Allocate and initialise an [`SN_env`] with `S_size` string registers and
/// `I_size` integer registers. Returns null if any allocation fails (after
/// releasing whatever was already allocated), mirroring C `SN_create_env`
/// (`api.c:3-32`).
///
/// # Safety
/// The allocation seam must be installed. The returned environment owns palloc'd
/// memory that must eventually be released with [`SN_close_env`].
#[allow(non_snake_case)]
pub unsafe fn SN_create_env(S_size: c_int, I_size: c_int) -> *mut SN_env {
    // api.c:5 — calloc(1, sizeof(struct SN_env))  ->  palloc0(sizeof)
    let z: *mut SN_env = palloc0(size_of::<SN_env>()) as *mut SN_env;
    if z.is_null() {
        return core::ptr::null_mut();
    }

    // `goto error` in the C is modelled as: do the fallible setup in a block
    // that breaks out to the cleanup path on the first failure.
    let ok = unsafe {
        // api.c:7 — z->p = create_s();
        (*z).p = create_s();
        if (*z).p.is_null() {
            false
        } else if {
            let mut good = true;
            // api.c:9-22 — if (S_size) { z->S = calloc(S_size, sizeof(symbol*)); ... }
            if S_size != 0 {
                (*z).S = palloc0((S_size as usize).wrapping_mul(size_of::<*mut symbol>()))
                    as *mut *mut symbol;
                if (*z).S.is_null() {
                    good = false;
                } else {
                    let mut i: c_int = 0;
                    while i < S_size {
                        *(*z).S.offset(i as isize) = create_s();
                        if (*(*z).S.offset(i as isize)).is_null() {
                            good = false;
                            break;
                        }
                        i += 1;
                    }
                }
            }
            good
        } {
            // api.c:24-28 — if (I_size) { z->I = calloc(I_size, sizeof(int)); ... }
            if I_size != 0 {
                (*z).I = palloc0((I_size as usize).wrapping_mul(size_of::<c_int>())) as *mut c_int;
                !(*z).I.is_null()
            } else {
                true
            }
        } else {
            false
        }
    };

    if ok {
        return z;
    }

    // api.c:29-31 — error: SN_close_env(z, S_size); return NULL;
    unsafe { SN_close_env(z, S_size) };
    core::ptr::null_mut()
}

/// Release an [`SN_env`] previously produced by [`SN_create_env`] with the same
/// `S_size`. A null `z` is a no-op. Mirrors C `SN_close_env` (`api.c:34-50`).
///
/// # Safety
/// `z` must be null or a live environment from [`SN_create_env`], and `S_size`
/// must match the value it was created with.
#[allow(non_snake_case)]
pub unsafe fn SN_close_env(z: *mut SN_env, S_size: c_int) {
    if z.is_null() {
        return;
    }
    unsafe {
        // api.c:37-45 — if (z->S) { for(...) lose_s(z->S[i]); free(z->S); }
        if !(*z).S.is_null() {
            let mut i: c_int = 0;
            while i < S_size {
                lose_s(*(*z).S.offset(i as isize));
                i += 1;
            }
            pfree((*z).S as *mut core::ffi::c_void);
        }
        // api.c:46 — free(z->I);
        pfree((*z).I as *mut core::ffi::c_void);
        // api.c:47 — if (z->p) lose_s(z->p);
        if !(*z).p.is_null() {
            lose_s((*z).p);
        }
        // api.c:48 — free(z);
        pfree(z as *mut core::ffi::c_void);
    }
}

/// Replace the working buffer with `s` (length `size`) and reset the cursor to
/// the start. Returns the `replace_s` error code. Mirrors C `SN_set_current`
/// (`api.c:52-56`).
///
/// # Safety
/// `z` must be a live environment; `s` must point at `size` valid symbols.
#[allow(non_snake_case)]
pub unsafe fn SN_set_current(z: *mut SN_env, size: c_int, s: *const symbol) -> c_int {
    // api.c:54 — int err = replace_s(z, 0, z->l, size, s, NULL);
    let err = unsafe { replace_s(z, 0, (*z).l, size, s, core::ptr::null_mut()) };
    // api.c:55 — z->c = 0;
    unsafe { (*z).c = 0 };
    err
}
