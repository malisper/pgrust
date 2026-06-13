//! `Bitmapset` (nodes/bitmapset.h), trimmed.
//!
//! Ports so far only test a `Bitmapset *` for NULL-ness (e.g. the executor's
//! `chgParam` checks), so only the storage fields are carried; the set
//! operations stay with their owning unit when it lands.

use mcx::PgVec;

/// `bitmapword` — the word the bit storage is built from.
pub type bitmapword = u64;

/// `BITS_PER_BITMAPWORD` (nodes/bitmapset.h): the number of bits in one
/// [`bitmapword`]. The 64-bit build (`bitmapword == uint64`).
pub const BITS_PER_BITMAPWORD: usize = 64;

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
    /// `bitmapword words[]` — the bit storage. C's `int nwords` exists only
    /// because the storage is a flexible array member; here the vec knows its
    /// length ([`Bitmapset::nwords`]).
    pub words: PgVec<'mcx, bitmapword>,
}

impl Bitmapset<'_> {
    /// `int nwords` — number of words in array.
    pub fn nwords(&self) -> i32 {
        self.words.len() as i32
    }

    /// `bms_copy(a)`-shaped deep copy into `mcx` (C: `palloc` + `memcpy`).
    /// Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: mcx::Mcx<'b>) -> types_error::PgResult<Bitmapset<'b>> {
        Ok(Bitmapset {
            words: mcx::slice_in(mcx, &self.words)?,
        })
    }
}
