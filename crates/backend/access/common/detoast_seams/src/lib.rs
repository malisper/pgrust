//! Seam declarations for the `backend-access-common-detoast` unit
//! (`access/common/detoast.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. A varlena attribute crosses as its verbatim
//! datum bytes (header included), exactly what C's `struct varlena *` points
//! at.

seam_core::seam!(
    /// `detoast_external_attr(attr)` (access/common/detoast.c): fetch back an
    /// out-of-line or out-of-memory stored attribute into `mcx`, without
    /// decompressing compressed data. `Err` carries the toast-fetch
    /// `ereport(ERROR)`s (`missing chunk number ...` etc.) and OOM. (C: the
    /// result is palloc'd in the current memory context.)
    pub fn detoast_external_attr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        attr: &[u8],
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `detoast_attr(attr)` (access/common/detoast.c): return a de-TOASTed
    /// (fetched back and decompressed) copy of `attr` in `mcx`. `Err` carries
    /// the toast-fetch / decompression `ereport(ERROR)`s and OOM.
    pub fn detoast_attr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        attr: &[u8],
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `pg_detoast_datum_packed(datum)` (fmgr.c): detoast only compressed /
    /// out-of-line-external values, returning a fully in-line varlena image in
    /// `mcx`; a plain (short- or 4-byte-header, uncompressed) value is returned
    /// verbatim. This is the `PG_DETOAST_DATUM_PACKED` step the by-reference
    /// `PG_GETARG_*_PP` argument readers apply before an adt core reads the
    /// payload. `attr` is the verbatim varlena datum bytes (header included).
    /// `Err` carries the toast-fetch / decompression `ereport(ERROR)`s and OOM.
    pub fn pg_detoast_datum_packed<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        attr: &[u8],
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `toast_datum_size(value)` (access/common/detoast.c): the physical on-disk
    /// /compressed storage size of a varlena attribute (the `pg_column_size`
    /// varlena path, varlena.c:5300-5301). For an on-disk-external value this is
    /// the TOAST `extsize` (the toast-pointer overhead is not counted); for an
    /// inline value it is `VARSIZE_ANY`. `attr` is the verbatim varlena datum
    /// bytes (header included). `Err` carries the indirect-detoast / EOH
    /// `ereport(ERROR)` surface.
    pub fn toast_datum_size<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        attr: &[u8],
    ) -> types_error::PgResult<usize>
);

seam_core::seam!(
    /// `VARATT_IS_EXTERNAL_ONDISK(attr)` test + `VARATT_EXTERNAL_GET_POINTER`'s
    /// `va_valueid` extraction (postgres.h / access/common/detoast.c) for
    /// `pg_column_toast_chunk_id` (varlena.c:5403-5408): the TOAST value OID of
    /// an on-disk external varlena, or `None` when the value is not stored
    /// on-disk-external. `attr` is the verbatim varlena datum bytes.
    pub fn toast_chunk_id(attr: &[u8]) -> types_error::PgResult<Option<types_core::Oid>>
);

// ---------------------------------------------------------------------------
// Extension for the `backend-utils-adt-array-typanalyze` unit
// (`utils/adt/array_typanalyze.c`).
//
// `compute_array_stats` calls `toast_raw_datum_size(value)`
// (array_typanalyze.c:327) on each sampled array value to skip arrays wider
// than `ARRAY_WIDTH_THRESHOLD` after detoasting. This crosses into the detoast
// owner; the owning unit installs it from its `init_seams()` when it lands,
// until then a call panics loudly.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `toast_raw_datum_size(value)` (access/common/detoast.c): the *logical*
    /// (decompressed, fully-detoasted) size in bytes of a varlena `Datum`,
    /// including the varlena header. Unlike [`toast_datum_size`] (the physical
    /// on-disk size), this reports what the value occupies once expanded, which
    /// is what `compute_array_stats` compares against `ARRAY_WIDTH_THRESHOLD`.
    /// `value` is the canonical `'mcx` value (a varlena array). `Err` carries
    /// the toast-fetch `ereport(ERROR)` surface.
    pub fn toast_raw_datum_size<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        value: types_tuple::Datum<'mcx>,
    ) -> types_error::PgResult<i64>
);

// ---------------------------------------------------------------------------
// Extension for the `backend-utils-adt-tsvector-typanalyze` unit
// (`tsearch/ts_typanalyze.c`).
//
// `compute_tsvector_stats` accumulates `total_width += VARSIZE_ANY(value)`
// (ts_typanalyze.c:226) over each sampled (raw, possibly toasted) tsvector
// value. This crosses into the detoast owner for the `VARSIZE_ANY` decode; the
// owning unit installs it from its `init_seams()`, until then a call panics
// loudly.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `VARSIZE_ANY(PTR)` (varatt.h): the on-the-wire size in bytes of any
    /// varlena form (short / external-pointer / 4-byte header), without
    /// detoasting. `attr` is the verbatim varlena datum bytes (header
    /// included). `Err` carries the malformed-vartag `ereport(ERROR)`.
    pub fn pg_varsize_any(attr: &[u8]) -> types_error::PgResult<usize>
);
