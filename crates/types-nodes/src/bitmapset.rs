//! `Bitmapset` (nodes/bitmapset.h), trimmed.
//!
//! Ports so far only test a `Bitmapset *` for NULL-ness (e.g. the executor's
//! `chgParam` checks), so only the storage fields are carried; the set
//! operations stay with their owning unit when it lands.

use mcx::{slice_in, Mcx, PgVec};
use types_core::PgResult;

/// `bitmapword` — the word the bit storage is built from.
pub type bitmapword = u64;

/// `Bitmapset` (nodes/bitmapset.h):
///
/// ```c
/// typedef struct Bitmapset {
///     pg_node_attr(custom_copy_equal, special_read_write, no_query_jumble)
///     NodeTag     type;
///     int         nwords;         /* number of words in array */
///     bitmapword  words[FLEXIBLE_ARRAY_MEMBER];
/// } Bitmapset;
/// ```
///
/// The word storage is context-allocated (C: the `bms_*` constructors palloc
/// in `CurrentMemoryContext`), so the set carries the allocator lifetime.
/// No `Clone`: copying allocates, so it must go through a fallible
/// `bms_copy`-shaped constructor when the owning unit lands.
#[derive(Debug, Eq, PartialEq)]
pub struct Bitmapset<'mcx> {
    /// `int nwords` — number of words in array.
    pub nwords: i32,
    /// `bitmapword words[]` — the bit storage.
    pub words: PgVec<'mcx, bitmapword>,
}

impl Bitmapset<'_> {
    /// `bms_copy(a)` — copy the set into `mcx`. Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Bitmapset<'b>> {
        Ok(Bitmapset {
            nwords: self.nwords,
            words: slice_in(mcx, &self.words)?,
        })
    }
}
