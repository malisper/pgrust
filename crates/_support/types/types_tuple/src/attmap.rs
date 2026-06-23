//! Attribute-number map vocabulary (`access/attmap.h`), trimmed.

use ::types_core::primitive::AttrNumber;

use ::mcx::PgVec;

/// `AttrMap` (`access/attmap.h`):
///
/// ```c
/// typedef struct AttrMap {
///     AttrNumber *attnums;
///     int         maplen;
/// } AttrMap;
/// ```
///
/// `attnums[i]` is the 1-based source attribute number feeding output column
/// `i+1`, or 0 for "no source" (NULL / dropped). `maplen` is `attnums.len()`.
/// The map storage is context-allocated (C: `make_attrmap` pallocs in
/// `CurrentMemoryContext`), so it carries the allocator lifetime.
#[derive(Debug)]
pub struct AttrMap<'mcx> {
    /// `AttrNumber *attnums` (length = C `maplen`).
    pub attnums: PgVec<'mcx, AttrNumber>,
}
