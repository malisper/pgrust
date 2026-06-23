//! Seam declarations for the datum (de)serialization primitives of the
//! `backend-utils-adt-datum` unit (`utils/adt/datum.c`).
//!
//! These are the raw byte-cursor primitives a few callers (e.g. nbtree's
//! parallel-scan array serialize/restore) use to flatten/unflatten a single
//! `Datum` into a shared-memory region. The owning unit installs them from its
//! `init_seams()` when it lands; until then a call panics loudly. A `*mut u8`
//! cursor models the C `char **start_address` (advanced past the bytes
//! written/read).
//!
//! # Datum-unification status (Wave 3)
//!
//! Every seam below intentionally speaks the bare-word [`datum::Datum`]
//! shim rather than canonical `types_tuple::Datum<'mcx>`. These are the
//! *sanctioned* bare-word ABI edges of the datum-redesign plan, not unmigrated
//! shim sites:
//!
//! * `datum_serialize` / `datum_restore` / `datum_estimate_space` are the
//!   audited **DSM byte-cursor primitive** (plan Phase 4): they flatten/unflatten
//!   one `Datum` across a `*mut u8` shared-memory cursor (C's
//!   `char **start_address`) for nbtree `_bt_parallel_(de)serialize` and
//!   `copyParamList`/`SerializeParamList`/`RestoreParamList`. A cursor protocol
//!   over raw bytes cannot be made safe and must mint/consume a bare word.
//! * `datum_copy` / `datum_image_eq` / `datum_image_hash` are the bare-`Datum`
//!   companion lane for those same DSM consumers, which hold a bare
//!   `ParamExternData.value` word with no `&Datum` available. The safe
//!   byte-model analogues already exist as
//!   `backend-utils-adt-scalar-seams::{datum_copy, ...}` over
//!   `&Datum<'mcx>`; remaining bare-word consumers (e.g. misc2 `rowtypes`
//!   `record_image_eq`) migrate onto the byte-model seams in their own crates
//!   (plan Phase 0/3), after which these companions are removed (Phase 5).
//!
//! This crate has no internal `Datum` construction/read sites (no `from_usize`,
//! `as_usize`, `Int32GetDatum`, `DatumGet*`): it is pure seam declarations whose
//! every `Datum` reference is one of the edges above. There is nothing to
//! migrate onto `Datum<'mcx>` without diverging from the DSM contract and the
//! owner's installed `*_word` impls in `backend-utils-adt-scalar-datum-core`.

use types_core::primitive::Size;
use datum::Datum;
// The canonical unified value type (Datum-unification keystone). The `*_v`
// seam variants below take/return it by reference; the bare-word `Datum`
// variants are transitional shims kept until every consumer migrates.
use types_tuple::heaptuple::Datum as DatumV;

seam_core::seam!(
    /// `datumCopy(value, typByVal, typLen)` (datum.c) — make a self-contained
    /// copy of one non-NULL datum: by-value datums are returned verbatim,
    /// by-reference datums are deep-copied (`palloc` in the current context).
    /// The raw-`Datum` form (vs. the byte-model `Datum` copy in
    /// `backend-utils-adt-scalar-seams`) used by callers that hold a bare
    /// `Datum` and its `(typByVal, typLen)`, e.g. `copyParamList` (params.c).
    ///
    /// TRANSITIONAL SHIM: superseded by [`datum_copy_v`], which carries the
    /// unified `types_tuple::Datum` value. Kept until callers migrate.
    pub fn datum_copy(value: Datum, typ_byval: bool, typ_len: i32) -> Datum
);

// ---------------------------------------------------------------------------
// Datum-unification keystone: the value-consuming seams gain a `&Datum<'mcx>`
// (the unified enum) variant. These are the migration-target contract; the
// bare-word variants above/below are deprecated shims removed in Cleanup.
//
// By-value arms are returned verbatim; by-reference arms are deep-copied /
// serialized over their byte image. The owner installs these from its
// `init_seams()` once it migrates; until then a call panics loudly.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `datumCopy(value, typByVal, typLen)` (datum.c) over the unified value
    /// type. The migration-target form of [`datum_copy`].
    pub fn datum_copy_v<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        value: &DatumV<'_>,
        typ_byval: bool,
        typ_len: i32,
    ) -> types_error::PgResult<DatumV<'mcx>>
);

seam_core::seam!(
    /// `datumEstimateSpace` over the unified value type. The migration-target
    /// form of [`datum_estimate_space`].
    pub fn datum_estimate_space_v(
        value: &DatumV<'_>,
        isnull: bool,
        typ_byval: bool,
        typ_len: i32,
    ) -> Size
);

seam_core::seam!(
    /// `datumSerialize` over the unified value type. The migration-target form
    /// of [`datum_serialize`].
    pub fn datum_serialize_v(
        value: &DatumV<'_>,
        isnull: bool,
        typ_byval: bool,
        typ_len: i32,
        cursor: *mut u8,
    ) -> *mut u8
);

seam_core::seam!(
    /// `datum_image_hash` over the unified value type. The migration-target
    /// form of [`datum_image_hash`].
    pub fn datum_image_hash_v(
        value: &DatumV<'_>,
        typ_byval: bool,
        typ_len: i16,
    ) -> types_error::PgResult<u32>
);

seam_core::seam!(
    /// `datum_image_eq` over the unified value type. The migration-target form
    /// of [`datum_image_eq`].
    pub fn datum_image_eq_v(
        value1: &DatumV<'_>,
        value2: &DatumV<'_>,
        typ_byval: bool,
        typ_len: i16,
    ) -> types_error::PgResult<bool>
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
