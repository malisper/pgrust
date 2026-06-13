//! The `ExpandedObjectMethods` method-table seams (`utils/expandeddatum.h`).
//!
//! Every expanded object embeds a `const ExpandedObjectMethods *eoh_methods`
//! function table with two entries — `get_flat_size` and `flatten_into` — that
//! `EOH_get_flat_size` / `EOH_flatten_into` (utils/adt/expandeddatum.c) dispatch
//! through. In C the table is a per-type set of function pointers installed by
//! the concrete expanded type via `EOH_init_header`; the keystone dispatchers
//! just chase `eohptr->eoh_methods->get_flat_size(eohptr)` etc.
//!
//! In the owned model the concrete expanded types own those two methods, so
//! they are seams homed here rather than on the keystone (which only forwards
//! to them). The genuine owners are:
//!   - expanded records — `expanded_record_get_flat_size` /
//!     `ER_flatten_into` (utils/adt/expandedrecord.c); the
//!     [`crate`](crate)'s sibling `expandedrecord` family, still scaffolded.
//!   - expanded arrays — `EA_get_flat_size` / `EA_flatten_into`
//!     (utils/adt/array_expanded.c); not yet ported.
//!
//! Until one of those owners lands and installs the slot, calling it panics
//! loudly (`seam not installed: …`) — the faithful stand-in for the fact that
//! the method-table dispatch has no implementation to reach yet, never a
//! silent stub.

seam_core::seam!(
    /// `eohptr->eoh_methods->get_flat_size(eohptr)`
    /// (`EOM_get_flat_size_method`, utils/expandeddatum.h): the concrete
    /// expanded type computes the total flattened size (header included) of the
    /// object behind `eoh`. `Err` carries the method's `ereport(ERROR)`s (e.g.
    /// expanded-array `EA_get_flat_size` raising `array size exceeds the maximum
    /// allowed`).
    pub fn eom_get_flat_size(
        eoh: types_datum::ExpandedObjectRef<'_>,
    ) -> types_error::PgResult<usize>
);

seam_core::seam!(
    /// `eohptr->eoh_methods->flatten_into(eohptr, result, allocated_size)`
    /// (`EOM_flatten_into_method`, utils/expandeddatum.h): the concrete
    /// expanded type serializes the object behind `eoh` into `dest`, which is
    /// exactly the preceding `eom_get_flat_size` bytes long. `Err` carries the
    /// method's `ereport(ERROR)`s.
    pub fn eom_flatten_into(
        eoh: types_datum::ExpandedObjectRef<'_>,
        dest: &mut [u8],
    ) -> types_error::PgResult<()>
);
