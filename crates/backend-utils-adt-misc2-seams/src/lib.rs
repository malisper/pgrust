//! Seam declarations for the `backend-utils-adt-misc2` unit (its
//! `expandeddatum.c` expanded-TOAST-object surface).
//!
//! Callers (e.g. `backend-access-common-heaptuple`'s `heap_compute_data_size` /
//! `fill_val`) reach the expanded-object subsystem through these slots. The
//! owning unit installs them from its `init_seams()` when it lands; until then
//! a call panics loudly. The expanded object crosses as the typed
//! [`types_datum::ExpandedObjectRef`] handle (C's `ExpandedObjectHeader *`
//! via `DatumGetEOHP`), not raw bytes.
//!
//! Datum-unification keystone: the value-carrying
//! `make_expanded_object_read_only_internal` seam gains a `_v` migration-target
//! variant that takes/returns the unified [`types_tuple::…::Datum`] enum
//! (aliased [`DatumV`]); the bare-word `types_datum::Datum` variant is a
//! transitional shim kept until every consumer migrates, then removed in
//! Cleanup.

// The canonical unified value type (Datum-unification keystone). The `_v`
// seam variant below takes/returns it; the bare-word `Datum` variant is a
// transitional shim kept until every consumer migrates.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;

seam_core::seam!(
    /// `EOH_get_flat_size(DatumGetEOHP(datum))` (utils/adt/expandeddatum.c):
    /// the number of bytes the expanded object would occupy once flattened.
    /// `Err` carries the expanded-object method's `ereport(ERROR)`s (e.g. the
    /// expanded-array `get_flat_size` raises `array size exceeds the maximum
    /// allowed`).
    pub fn eoh_get_flat_size(
        eoh: types_datum::ExpandedObjectRef<'_>,
    ) -> types_error::PgResult<usize>
);

seam_core::seam!(
    /// `EOH_flatten_into(DatumGetEOHP(datum), data, data_length)`
    /// (utils/adt/expandeddatum.c): flatten the expanded object into `dest`
    /// (which is exactly `EOH_get_flat_size` bytes long). `Err` carries the
    /// expanded-object method's `ereport(ERROR)`s.
    pub fn eoh_flatten_into(
        eoh: types_datum::ExpandedObjectRef<'_>,
        dest: &mut [u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `MakeExpandedObjectReadOnlyInternal(d)` (utils/adt/expandeddatum.c): if
    /// `d` is a read-write expanded-object pointer, return the object's
    /// built-in read-only pointer (`EOHPGetRODatum`, same `eohptr` payload with
    /// the R/O `va_tag`); any other datum is returned verbatim. Reached only on
    /// the non-null, `typlen == -1` branch of the `MakeExpandedObjectReadOnly`
    /// macro (the null / non-varlena short-circuit is the caller's). The
    /// dereference of the `Datum` pointer word is the expandeddatum owner's, so
    /// the transform crosses here rather than in the node. Allocates the R/O
    /// pointer copy in `mcx`; fallible on OOM.
    ///
    /// TRANSITIONAL SHIM: superseded by
    /// [`make_expanded_object_read_only_internal_v`], which carries the unified
    /// `types_tuple::Datum` value. Kept until callers migrate.
    pub fn make_expanded_object_read_only_internal<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        d: types_datum::Datum,
    ) -> types_error::PgResult<types_datum::Datum>
);

seam_core::seam!(
    /// `MakeExpandedObjectReadOnlyInternal(d)` (utils/adt/expandeddatum.c) over
    /// the unified value type — the migration-target form of
    /// [`make_expanded_object_read_only_internal`]. A read-write expanded-object
    /// pointer is rewritten to its built-in read-only pointer
    /// (`EOHPGetRODatum`); any other datum is returned verbatim (a by-value arm
    /// passes through unchanged, a non-expanded by-reference arm is returned as
    /// is). Allocates the R/O pointer copy in `mcx`; fallible on OOM.
    pub fn make_expanded_object_read_only_internal_v<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        d: &DatumV<'_>,
    ) -> types_error::PgResult<DatumV<'mcx>>
);
