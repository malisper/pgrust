//! Seam declarations for the datum (de)serialization primitives of the
//! `backend-utils-adt-datum` unit (`utils/adt/datum.c`).
//!
//! These are the raw byte-cursor primitives a few callers (e.g. nbtree's
//! parallel-scan array serialize/restore) use to flatten/unflatten a single
//! `Datum` into a shared-memory region. The owning unit installs them from its
//! `init_seams()` when it lands; until then a call panics loudly. A `*mut u8`
//! cursor models the C `char **start_address` (advanced past the bytes
//! written/read).

use types_core::primitive::Size;
use types_datum::Datum;

seam_core::seam!(
    /// `datumCopy(value, typByVal, typLen)` (datum.c) — make a self-contained
    /// copy of one non-NULL datum: by-value datums are returned verbatim,
    /// by-reference datums are deep-copied (`palloc` in the current context).
    /// The raw-`Datum` form (vs. the byte-model `TupleValue` copy in
    /// `backend-utils-adt-scalar-seams`) used by callers that hold a bare
    /// `Datum` and its `(typByVal, typLen)`, e.g. `copyParamList` (params.c).
    pub fn datum_copy(value: Datum, typ_byval: bool, typ_len: i32) -> Datum
);

seam_core::seam!(
    /// `datumEstimateSpace(value, isnull, typByVal, typLen)` — bytes needed to
    /// serialize one datum (`sizeof(int)` header plus the payload).
    pub fn datum_estimate_space(value: Datum, isnull: bool, typ_byval: bool, typ_len: i32) -> Size
);

seam_core::seam!(
    /// `datumSerialize(value, isnull, typByVal, typLen, &cursor)` — flatten one
    /// datum into the cursor and return the advanced cursor.
    pub fn datum_serialize(
        value: Datum,
        isnull: bool,
        typ_byval: bool,
        typ_len: i32,
        cursor: *mut u8,
    ) -> *mut u8
);

seam_core::seam!(
    /// `datumRestore(&cursor, &isnull)` — read one datum from the cursor;
    /// returns `(value, isnull, advanced_cursor)`.
    pub fn datum_restore(cursor: *mut u8) -> (Datum, bool, *mut u8)
);

seam_core::seam!(
    /// `datum_image_hash(value, typByVal, typLen)` (datum.c) — a hash of the
    /// in-memory image of one datum, keyed off the type's by-value/length
    /// properties. The binary-mode per-key hash leaf used by Memoize's
    /// `MemoizeHash_hash`.
    pub fn datum_image_hash(value: Datum, typ_byval: bool, typ_len: i16) -> types_error::PgResult<u32>
);

seam_core::seam!(
    /// `datum_image_eq(value1, value2, typByVal, typLen)` (datum.c) — whether the
    /// in-memory images of two datums are bit-for-bit equal, keyed off the type's
    /// by-value/length properties. The binary-mode per-key equality leaf used by
    /// Memoize's `MemoizeHash_equal`.
    pub fn datum_image_eq(
        value1: Datum,
        value2: Datum,
        typ_byval: bool,
        typ_len: i16,
    ) -> types_error::PgResult<bool>
);
