//! Seam declarations for the `backend-parser-coerce` unit
//! (`parser/parse_coerce.c`), the type-coercion catalog lookups.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

/// `CoercionPathType` (parser/parse_coerce.h): the kind of coercion pathway
/// `find_coercion_pathway` resolved between two types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum CoercionPathType {
    /// `COERCION_PATH_NONE` (0) — failed to find any coercion pathway.
    None = 0,
    /// `COERCION_PATH_FUNC` (1) — apply the specified coercion function.
    Func = 1,
    /// `COERCION_PATH_RELABELTYPE` (2) — binary-compatible cast, no function.
    Relabeltype = 2,
    /// `COERCION_PATH_ARRAYCOERCE` (3) — need an `ArrayCoerceExpr` node.
    Arraycoerce = 3,
    /// `COERCION_PATH_COERCEVIAIO` (4) — need a `CoerceViaIO` node.
    Coerceviaio = 4,
}

seam_core::seam!(
    /// `find_coercion_pathway(targetTypeId, sourceTypeId, COERCION_IMPLICIT,
    /// &funcid)` (parse_coerce.c): determine how to coerce `source_type_id`
    /// to `target_type_id` under implicit context. Returns the pathway kind
    /// and (for `Func`) the coercion function OID, else `InvalidOid`. `Err`
    /// carries catcache-path `ereport(ERROR)`s.
    pub fn find_coercion_pathway_implicit(
        target_type_id: Oid,
        source_type_id: Oid,
    ) -> PgResult<(CoercionPathType, Oid)>
);

seam_core::seam!(
    /// `IsBinaryCoercible(srctype, targettype)` (parse_coerce.c): whether
    /// `srctype` is binary-coercible to `targettype` (identical types, an
    /// existing binary-coercible pg_cast entry, or `targettype` being a
    /// polymorphic/ANY pseudo-type that accepts `srctype`). `Err` carries
    /// catcache-path `ereport(ERROR)`s.
    pub fn is_binary_coercible(srctype: Oid, targettype: Oid) -> PgResult<bool>
);
