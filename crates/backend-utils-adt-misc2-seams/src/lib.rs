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
//! Datum-unification: the value-carrying `make_expanded_object_read_only_internal`
//! seam carries the unified [`types_tuple::…::Datum`] enum (aliased [`Datum`]).
//! The transitional bare-word `types_datum::Datum` variant has been retired (its
//! sole consumer migrated to the unified carrier); `types_datum` survives here
//! only for the opaque [`types_datum::ExpandedObjectRef`] handle, which is a
//! typed pointer, not the bare-word shim newtype.

// The canonical unified value type (Datum-unification keystone). The
// `make_expanded_object_read_only_internal` seam below takes/returns it.
use types_tuple::backend_access_common_heaptuple::Datum;

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
    /// `MakeExpandedObjectReadOnlyInternal(d)` (utils/adt/expandeddatum.c) over
    /// the unified value type. If `d` is a read-write expanded-object pointer,
    /// return the object's built-in read-only pointer (`EOHPGetRODatum`, same
    /// `eohptr` payload with the R/O `va_tag`); any other datum is returned
    /// verbatim (a by-value arm passes through unchanged, a non-expanded
    /// by-reference arm is returned as is). Reached only on the non-null,
    /// `typlen == -1` branch of the `MakeExpandedObjectReadOnly` macro (the null
    /// / non-varlena short-circuit is the caller's). The dereference of the
    /// `Datum` pointer word is the expandeddatum owner's, so the transform
    /// crosses here rather than in the node. Allocates the R/O pointer copy in
    /// `mcx`; fallible on OOM.
    pub fn make_expanded_object_read_only_internal_v<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        d: &Datum<'_>,
    ) -> types_error::PgResult<Datum<'mcx>>
);

/* ---------------------------------------------------------------------------
 * regproc.c identity formatters (`format_procedure*` / `format_operator*`),
 * consumed by objectaddress.c's `getObjectIdentityParts`. The owning unit
 * (`backend-utils-adt-misc2`) installs these from `init_seams()` when it
 * lands; until then a call panics loudly.
 * ------------------------------------------------------------------------- */

seam_core::seam!(
    /// `format_procedure_parts(procedure_oid, &objname, &objargs, missing_ok)`
    /// (regproc.c): the `(objname, objargs)` C-string lists feeding
    /// `get_object_address`. `Ok(None)` is the `missing_ok` "didn't exist"
    /// return. Allocated in `mcx`.
    pub fn format_procedure_parts<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        procedure_oid: types_core::Oid,
        missing_ok: bool,
    ) -> types_error::PgResult<Option<(mcx::PgVec<'mcx, mcx::PgString<'mcx>>, mcx::PgVec<'mcx, mcx::PgString<'mcx>>)>>
);

seam_core::seam!(
    /// `format_operator_parts(operator_oid, &objname, &objargs, missing_ok)`
    /// (regproc.c): the `(objname, objargs)` C-string lists feeding
    /// `get_object_address`. `Ok(None)` is the `missing_ok` "didn't exist"
    /// return. Allocated in `mcx`.
    pub fn format_operator_parts<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        operator_oid: types_core::Oid,
        missing_ok: bool,
    ) -> types_error::PgResult<Option<(mcx::PgVec<'mcx, mcx::PgString<'mcx>>, mcx::PgVec<'mcx, mcx::PgString<'mcx>>)>>
);
