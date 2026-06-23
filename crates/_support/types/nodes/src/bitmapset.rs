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

/// `BMS_Comparison` (nodes/bitmapset.h) — result of `bms_subset_compare`.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum BMS_Comparison {
    /// sets are equal
    BMS_EQUAL = 0,
    /// first set is a subset of the second
    BMS_SUBSET1 = 1,
    /// second set is a subset of the first
    BMS_SUBSET2 = 2,
    /// neither set is a subset of the other
    BMS_DIFFERENT = 3,
}

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

impl<'mcx> Bitmapset<'mcx> {
    /// An empty set (no words) allocated in `mcx` — the owned stand-in for the
    /// C `NULL` `Bitmapset *` where a slot must hold a value (e.g. a
    /// grouped_cols entry for an empty grouping set). `bms_is_empty` of this is
    /// true.
    pub fn empty(mcx: mcx::Mcx<'mcx>) -> types_error::PgResult<Bitmapset<'mcx>> {
        Ok(Bitmapset {
            words: mcx::vec_with_capacity_in(mcx, 0)?,
        })
    }
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

// `Bitmapset` is `pg_node_attr(custom_copy_equal)` in C: `gen_node_support.pl`
// emits no generated `_copyBitmapset`/`_equalBitmapset` body, and
// `copyfuncs.c`/`equalfuncs.c` instead delegate to `bms_copy` / `bms_equal` by
// hand. The owned-tree analogue is these hand-written trait impls (NOT
// `#[derive(PgNode)]`, which would mis-generate over the raw word array): copy
// re-homes the word storage onto the target context exactly like `bms_copy`
// (`palloc` + `memcpy`), and equality is `bms_equal`.

impl<'mcx> node_support::PgNodeCopy for Bitmapset<'mcx> {
    type Bound<'dst> = Bitmapset<'dst>;
    /// `bms_copy(from)` — `_copyBitmapset`. Deep-copy the word storage into the
    /// target context (fallible: allocates).
    fn copy_node_in<'dst>(
        &self,
        dst: mcx::Mcx<'dst>,
    ) -> types_error::PgResult<Self::Bound<'dst>> {
        self.clone_in(dst)
    }
}

impl node_support::PgNodeEqual for Bitmapset<'_> {
    /// `bms_equal(a, b)` — `_equalBitmapset`. Two bitmapsets are equal iff they
    /// have identical membership. The trailing-zero-word normalization that
    /// `bms_equal` performs is preserved by the storage invariant (`bms_*`
    /// constructors never leave trailing all-zero words), so a flat word-vector
    /// comparison is faithful.
    fn equal_node(&self, other: &Self) -> bool {
        self.words.as_slice() == other.words.as_slice()
    }
}
