//! Seam declarations for the `backend-utils-adt-oid` unit
//! (`utils/adt/oid.c`): the `oid` type's I/O.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use types_parsenodes::Node;

seam_core::seam!(
    /// `oidparse(node)` (oid.c): parse the OID literal carried by a parser
    /// value node — an `Integer` (its `ival`) or a `Float` (re-parsed via
    /// `oidin` because OIDs can exceed `int32`). Any other node tag is an
    /// `elog(ERROR)`. Used by `get_object_address`'s `OBJECT_LARGEOBJECT` arm.
    /// Malformed input raises (`Err`).
    pub fn oidparse(node: &Node) -> PgResult<Oid>
);

seam_core::seam!(
    /// `oidin(cstring)` (oid.c) via `DirectInputFunctionCallSafe(oidin, ...)`:
    /// parse a decimal `cstring` into an `Oid` (`oidin` is
    /// `oidin_subr` → `strtoul` with overflow/garbage rejection).
    ///
    /// `soft = true` models a soft-error `ErrorSaveContext` being supplied:
    /// an out-of-range / malformed value is `Ok(None)` (the C
    /// `DirectInputFunctionCallSafe` returning `false`, leaving the result
    /// `(Datum) 0` = `InvalidOid` in `regproc.c`'s `parseNumericOid`). With
    /// `soft = false` such input propagates as a hard error on `Err`.
    pub fn oidin(s: &str, soft: bool) -> PgResult<Option<Oid>>
);
