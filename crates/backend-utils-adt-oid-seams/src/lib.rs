//! Seam declarations for the `backend-utils-adt-oid` unit
//! (`utils/adt/oid.c`): the `oid` type's I/O.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

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
