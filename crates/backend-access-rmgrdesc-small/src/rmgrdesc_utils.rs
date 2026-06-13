//! `rmgrdesc_utils.c` — support functions shared by the rmgr descriptor
//! routines: array rendering in the format described in
//! `access/rmgrdesc/README`.
//!
//! C's type-erased `(void *array, size_t elem_size, int count)` triple becomes
//! a typed slice, and the `(elem_desc, data)` callback pair becomes a closure
//! (the C `data` pointer is the closure's captures).

use mcx::PgString;
use types_core::{Oid, OffsetNumber};
use types_error::PgResult;

use crate::util::appendf;

/// `array_desc` — append ` [elem, elem, ...]` (` []` when empty), rendering
/// each element with `elem_desc`.
pub fn array_desc<'mcx, T>(
    buf: &mut PgString<'mcx>,
    array: &[T],
    mut elem_desc: impl FnMut(&mut PgString<'mcx>, &T) -> PgResult<()>,
) -> PgResult<()> {
    if array.is_empty() {
        return buf.try_push_str(" []");
    }
    buf.try_push_str(" [")?;
    for (i, elem) in array.iter().enumerate() {
        elem_desc(buf, elem)?;
        if i < array.len() - 1 {
            buf.try_push_str(", ")?;
        }
    }
    buf.try_push(']')
}

/// `offset_elem_desc` — `%u` over one `OffsetNumber`.
pub fn offset_elem_desc(buf: &mut PgString<'_>, offset: &OffsetNumber) -> PgResult<()> {
    appendf!(buf, "{offset}")
}

/// `redirect_elem_desc` — `%u->%u` over a pair of adjacent `OffsetNumber`s.
pub fn redirect_elem_desc(buf: &mut PgString<'_>, new_offset: &[OffsetNumber; 2]) -> PgResult<()> {
    appendf!(buf, "{}->{}", new_offset[0], new_offset[1])
}

/// `oid_elem_desc` — `%u` over one `Oid`.
pub fn oid_elem_desc(buf: &mut PgString<'_>, relid: &Oid) -> PgResult<()> {
    appendf!(buf, "{relid}")
}

// ── seam-compatible entry points ─────────────────────────────────────────────
// The seams use a type-erased (`&[u8]`, elem_size, count) calling convention
// that mirrors the C `void *array` triple and takes element-size slices through
// a `&mut dyn FnMut` callback.  The typed helpers above are more ergonomic for
// Rust callers; the wrappers below present the exact seam Signature.

/// Seam entry point for `array_desc`: iterate `count` chunks of `elem_size`
/// bytes and render each with `elem_desc`.
pub fn array_desc_seam<'a>(
    buf: &mut PgString<'a>,
    array: &[u8],
    elem_size: usize,
    count: i32,
    elem_desc: &mut dyn FnMut(&mut PgString<'a>, &[u8]) -> PgResult<()>,
) -> PgResult<()> {
    if count == 0 {
        return buf.try_push_str(" []");
    }
    buf.try_push_str(" [")?;
    for i in 0..count as usize {
        let start = i * elem_size;
        let elem = &array[start..start + elem_size];
        elem_desc(buf, elem)?;
        if i < count as usize - 1 {
            buf.try_push_str(", ")?;
        }
    }
    buf.try_push(']')
}

/// Seam entry point for `offset_elem_desc`: takes `OffsetNumber` by value.
pub fn offset_elem_desc_seam(
    buf: &mut PgString<'_>,
    offset: OffsetNumber,
) -> PgResult<()> {
    offset_elem_desc(buf, &offset)
}

/// Seam entry point for `redirect_elem_desc`: takes two `OffsetNumber`s by value.
pub fn redirect_elem_desc_seam(
    buf: &mut PgString<'_>,
    from: OffsetNumber,
    to: OffsetNumber,
) -> PgResult<()> {
    redirect_elem_desc(buf, &[from, to])
}

/// Seam entry point for `oid_elem_desc`: takes `Oid` by value.
pub fn oid_elem_desc_seam(buf: &mut PgString<'_>, relid: Oid) -> PgResult<()> {
    oid_elem_desc(buf, &relid)
}
