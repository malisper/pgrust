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
