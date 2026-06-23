//! Allocation helpers for the compiler's owned data-model construction.
//!
//! The PL/pgSQL data model rides on builtin `Vec`/`String`/`Box`; allocations
//! on a `palloc` path are made OOM-fallible via `try_reserve`, with failure
//! mapped to a panic framed as the engine's `ERRCODE_OUT_OF_MEMORY` longjmp at
//! the `catch_unwind` boundary (the same model `pl_gram`/`pl_funcs` use).

/// `ereport(ERROR, errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"))`.
#[cold]
#[inline(never)]
pub(crate) fn oom() -> ! {
    panic!("out of memory (SQLSTATE 53200): PL/pgSQL compile allocation failed");
}

/// Fallible `Vec::push` (the `palloc`-charged twin).
#[inline]
pub(crate) fn vpush<T>(v: &mut Vec<T>, x: T) {
    if v.try_reserve(1).is_err() {
        oom()
    }
    v.push(x);
}

/// Fallible `Vec::reserve`.
#[inline]
pub(crate) fn vreserve<T>(v: &mut Vec<T>, n: usize) {
    if v.try_reserve(n).is_err() {
        oom()
    }
}

/// Fallible `Vec::with_capacity`.
#[inline]
pub(crate) fn vwithcap<T>(n: usize) -> Vec<T> {
    let mut v = Vec::new();
    vreserve(&mut v, n);
    v
}

/// Fallible construction of a `Vec` from a fixed-size array.
#[inline]
pub(crate) fn vfrom<T, const N: usize>(arr: [T; N]) -> Vec<T> {
    let mut v = vwithcap(N);
    for x in arr {
        v.push(x);
    }
    v
}

/// `Box::new` (fixed-size header allocation).
#[inline]
pub(crate) fn boxed<T>(x: T) -> Box<T> {
    Box::new(x)
}

/// Fallible `str -> String` copy (the `pstrdup` twin).
#[inline]
pub(crate) fn sdup(s: &str) -> String {
    let mut d = String::new();
    if d.try_reserve(s.len()).is_err() {
        oom()
    }
    d.push_str(s);
    d
}
