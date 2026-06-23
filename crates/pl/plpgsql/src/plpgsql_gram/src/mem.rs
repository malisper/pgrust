//! Allocation helpers for the grammar's owned data-model construction.
//!
//! The PL/pgSQL data model is kept on the builtin `Vec`/`String`/`Box` types
//! (the `plpgsql::PLpgSQL_*` structs are consumed across the seam by the
//! compiler/executor). The grammar's allocations on a `palloc` path are made
//! OOM-fallible via `try_reserve`; allocation failure maps to a panic framed as
//! an `ErrorResponse` at the engine's `catch_unwind` boundary — the analogue of
//! C's `ereport(ERROR, errcode(ERRCODE_OUT_OF_MEMORY))` longjmp out of palloc.

/// `ereport(ERROR, errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"))`
/// under the panic-as-ErrorResponse model.
#[cold]
#[inline(never)]
pub(crate) fn oom() -> ! {
    panic!("out of memory (SQLSTATE 53200): PL/pgSQL allocation failed");
}

/// Fallible `Vec::push` (the `palloc`-charged twin).
#[inline]
pub(crate) fn vpush<T>(v: &mut Vec<T>, x: T) {
    if v.try_reserve(1).is_err() {
        oom()
    }
    v.push(x);
}

/// Fallible `vec![T::default(); n]`.
#[inline]
pub(crate) fn vzeroed<T: Default + Clone>(n: usize) -> Vec<T> {
    let mut v = Vec::new();
    if v.try_reserve(n).is_err() {
        oom()
    }
    v.resize_with(n, T::default);
    v
}

/// `Box::new` (fixed-size header allocation; the builtin allocator path).
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

/// Fallible `String::push_str`.
#[inline]
pub(crate) fn spush(d: &mut String, s: &str) {
    if d.try_reserve(s.len()).is_err() {
        oom()
    }
    d.push_str(s);
}

/// Fallible `String::push` (single char).
#[inline]
pub(crate) fn spushc(d: &mut String, c: char) {
    let mut buf = [0u8; 4];
    spush(d, c.encode_utf8(&mut buf));
}
