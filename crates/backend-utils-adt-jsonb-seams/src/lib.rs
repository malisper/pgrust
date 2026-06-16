//! Seam declarations for the `backend-utils-adt-jsonb` unit
//! (`utils/adt/jsonb.c`) — the SQL-facing `jsonb` type.
//!
//! `jsonb.c` is the on-top layer over the `jsonb_util.c` engine
//! (`backend-utils-adt-jsonb-util`). Most of its catalog/fmgr boundary is
//! already declared by sibling seam crates and reused directly:
//!
//!   * `backend-utils-adt-jsonfuncs-seams` — `categorize_type`,
//!     `output_function_call`, `func_volatile`, `text_datum_bytes`,
//!     `deconstruct_array`, `walk_composite` (the `json_categorize_type`
//!     classification + per-type output/cast fmgr calls + array/composite
//!     catalog half),
//!   * `backend-utils-adt-timestamp-seams::json_encode_datetime` (datetime
//!     rendering),
//!
//! and the numeric float casts go straight to the landed
//! `backend-utils-adt-numeric` crate.
//!
//! This crate declares only the boundaries that have NO existing seam and
//! reach genuinely-external / not-yet-ported subsystems:
//!
//!   * `parse_to_jsonb` — the JSON lexer/parser (jsonapi subsystem) driving the
//!     `jsonb_in_*` semantic actions to assemble on-disk bytes,
//!   * `oid_function_call1` — `OidFunctionCall1` for the `JSONTYPE_CAST` arm,
//!   * `jsonb_datum_bytes` — detoast a `jsonb` `Datum` to its varlena bytes,
//!   * `numeric_int2` / `numeric_int4` / `numeric_int8` — the `numeric`→integer
//!     casts (`DirectFunctionCall1(numeric_intN, ...)`), not yet ported in
//!     `backend-utils-adt-numeric`.
//!
//! The owning unit installs all of these from its `init_seams()` once it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_tuple::Datum;

seam_core::seam!(
    /// `jsonb_from_cstring(char *json, int len, bool unique_keys, ...)` — run
    /// the JSON lexer/parser (`pg_parse_json`) over `json`, driving the
    /// `jsonb_in_*` semantic actions, and return the assembled on-disk `jsonb`
    /// varlena bytes (length header + root container), allocated in `mcx`. The
    /// lexer/parser is the jsonapi subsystem (json's cycle partner), so the
    /// parse loop is owned by the provider. `Err` carries the parse
    /// `ereport(ERROR, "invalid input syntax for type json")`.
    pub fn parse_to_jsonb<'mcx>(
        mcx: Mcx<'mcx>,
        json: &[u8],
        unique_keys: bool,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `OidFunctionCall1(outfuncoid, val)` — the `JSONTYPE_CAST` arm of
    /// `datum_to_jsonb_internal`: invoke the type's cast-to-json function
    /// through the fmgr calling convention, returning the resulting (json/jsonb
    /// text) `Datum`. `Err` carries the called function's `ereport(ERROR)`.
    pub fn oid_function_call1<'mcx>(
        mcx: Mcx<'mcx>,
        outfuncoid: Oid,
        val: &Datum<'mcx>,
    ) -> PgResult<Datum<'mcx>>
);

seam_core::seam!(
    /// `PG_GETARG_JSONB_P(val)` / `DatumGetJsonbP(val)` — detoast a `jsonb`
    /// `Datum` to its on-disk varlena bytes (length header + root container)
    /// for the `JSONTYPE_JSONB` arm of `datum_to_jsonb_internal`.
    pub fn jsonb_datum_bytes<'mcx>(val: &Datum<'mcx>) -> PgResult<alloc::vec::Vec<u8>>
);

seam_core::seam!(
    /// `DirectFunctionCall1(numeric_int2, num)` — cast on-disk `numeric` bytes
    /// `num` to `int2`. `Err` carries "smallint out of range". Not yet ported
    /// in `backend-utils-adt-numeric`.
    pub fn numeric_int2(num: &[u8]) -> PgResult<i16>
);

seam_core::seam!(
    /// `DirectFunctionCall1(numeric_int4, num)` — cast on-disk `numeric` bytes
    /// `num` to `int4`. `Err` carries "integer out of range".
    pub fn numeric_int4(num: &[u8]) -> PgResult<i32>
);

seam_core::seam!(
    /// `DirectFunctionCall1(numeric_int8, num)` — cast on-disk `numeric` bytes
    /// `num` to `int8`. `Err` carries "bigint out of range".
    pub fn numeric_int8(num: &[u8]) -> PgResult<i64>
);

extern crate alloc;
