//! `Bitmapset` (nodes/bitmapset.h), trimmed.
//!
//! Ports so far only test a `Bitmapset *` for NULL-ness (e.g. the executor's
//! `chgParam` checks), so only the storage fields are carried; the set
//! operations stay with their owning unit when it lands.

use mcx::PgVec;

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
/// No derived `Clone`: copying allocates, so it goes through the fallible
/// `bms_copy`-shaped [`Bitmapset::clone_in`]. Set *operations* (union,
/// intersect, membership, ...) stay with the owning `nodes/bitmapset.c` unit.
#[derive(Debug, Eq, PartialEq)]
pub struct Bitmapset<'mcx> {
    /// `int nwords` — number of words in array.
    pub nwords: i32,
    /// `bitmapword words[]` — the bit storage.
    pub words: PgVec<'mcx, bitmapword>,
}

impl Bitmapset<'_> {
    /// `bms_copy(a)`-shaped deep copy into `mcx` (C: `palloc` + `memcpy`).
    /// Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: mcx::Mcx<'b>) -> types_core::PgResult<Bitmapset<'b>> {
        Ok(Bitmapset {
            nwords: self.nwords,
            words: mcx::slice_in(mcx, &self.words)?,
        })
    }
}
