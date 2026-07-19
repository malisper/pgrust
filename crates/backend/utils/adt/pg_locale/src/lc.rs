//! LC_* category ids and newlocale masks, one indirection for the wasm arm.
//!
//! native: re-exports the platform libc crate's values.
//! wasm32: the wasi libc CRATE exposes no locale surface, but wasi-libc
//! itself (musl-derived) ships setlocale/newlocale and the *_l workers as
//! weak symbols with musl's category numbering — these are musl locale.h's
//! values, matching what the linked wasi-libc implementation expects.
//! wasi-libc's locale is C/POSIX-only: setlocale/newlocale fail cleanly for
//! any other name, which is exactly C's behavior on a locale-less libc.

#[cfg(not(target_family = "wasm"))]
pub(crate) use libc::{
    LC_ALL_MASK, LC_COLLATE, LC_COLLATE_MASK, LC_CTYPE, LC_CTYPE_MASK, LC_MESSAGES,
    LC_MESSAGES_MASK, LC_MONETARY, LC_MONETARY_MASK, LC_NUMERIC, LC_NUMERIC_MASK, LC_TIME,
    LC_TIME_MASK,
};

#[cfg(target_family = "wasm")]
mod wasm_lc {
    use core::ffi::c_int;
    // musl locale.h category ids.
    pub(crate) const LC_CTYPE: c_int = 0;
    pub(crate) const LC_NUMERIC: c_int = 1;
    pub(crate) const LC_TIME: c_int = 2;
    pub(crate) const LC_COLLATE: c_int = 3;
    pub(crate) const LC_MONETARY: c_int = 4;
    pub(crate) const LC_MESSAGES: c_int = 5;
    // musl newlocale masks: 1 << category; ALL is the musl catch-all word.
    pub(crate) const LC_CTYPE_MASK: c_int = 1 << LC_CTYPE;
    pub(crate) const LC_NUMERIC_MASK: c_int = 1 << LC_NUMERIC;
    pub(crate) const LC_TIME_MASK: c_int = 1 << LC_TIME;
    pub(crate) const LC_COLLATE_MASK: c_int = 1 << LC_COLLATE;
    pub(crate) const LC_MONETARY_MASK: c_int = 1 << LC_MONETARY;
    pub(crate) const LC_MESSAGES_MASK: c_int = 1 << LC_MESSAGES;
    pub(crate) const LC_ALL_MASK: c_int = 0x7fffffff;
}

#[cfg(target_family = "wasm")]
pub(crate) use wasm_lc::*;
