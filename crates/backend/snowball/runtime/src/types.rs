//! Core snowball runtime types: `symbol`, `SN_env`, `among`.
//!
//! Ported verbatim from the snowball runtime headers shipped with PostgreSQL:
//!   * `symbol` — `src/include/snowball/libstemmer/api.h:2`
//!   * `struct SN_env` — `src/include/snowball/libstemmer/api.h:14-19`
//!   * `struct among` — `src/include/snowball/libstemmer/header.h:15-21`
//!
//! These are `#[repr(C)]` because every generated stemmer builds static
//! `among` tables that embed `&SN_env`/function-pointer fields and indexes into
//! `SN_env` by field, and because `dict_snowball.c` reads/writes `SN_env`
//! fields directly. Keeping the layout identical preserves the byte-for-byte
//! state-machine behaviour of the stemmers and the ABI they link against.

use core::ffi::{c_int, c_uchar};

/// A snowball string element.
///
/// `src/include/snowball/libstemmer/api.h:2` — `typedef unsigned char symbol;`.
/// UTF-8 stemmers store raw bytes; single-byte encodings store one character
/// per `symbol`.
#[allow(non_camel_case_types)]
pub type symbol = c_uchar;

/// The snowball environment.
///
/// `src/include/snowball/libstemmer/api.h:14-19`:
/// ```c
/// struct SN_env {
///     symbol * p;
///     int c; int l; int lb; int bra; int ket;
///     symbol * * S;
///     int * I;
/// };
/// ```
///
/// `p` points at the working buffer (which carries a hidden `[capacity,
/// length]` header in the two ints immediately before it — see
/// [`crate::utilities`]); `c` is the cursor, `l` the limit, `lb` the backward
/// limit, `bra`/`ket` the current slice bounds, `S`/`I` the string/integer
/// register arrays.
#[allow(non_camel_case_types)]
#[derive(Copy, Clone)]
#[repr(C)]
pub struct SN_env {
    pub p: *mut symbol,
    pub c: c_int,
    pub l: c_int,
    pub lb: c_int,
    pub bra: c_int,
    pub ket: c_int,
    pub S: *mut *mut symbol,
    pub I: *mut c_int,
}

/// One entry of an `among` table.
///
/// `src/include/snowball/libstemmer/header.h:15-21`:
/// ```c
/// struct among
/// {   int s_size;       /* number of chars in string */
///     const symbol * s; /* search string */
///     int substring_i;  /* index to longest matching substring */
///     int result;       /* result of the lookup */
///     int (* function)(struct SN_env *);
/// };
/// ```
///
/// `substring_i` is the index of this entry's longest proper-prefix entry
/// already in the table (`-1` if none); `function` is an optional guard
/// (modelled as `Option<fn ptr>` so that a NULL guard is `None`, matching the
/// C `w->function == NULL` test).
#[allow(non_camel_case_types)]
#[derive(Copy, Clone)]
#[repr(C)]
pub struct among {
    pub s_size: c_int,
    pub s: *const symbol,
    pub substring_i: c_int,
    pub result: c_int,
    pub function: Option<unsafe fn(*mut SN_env) -> c_int>,
}
