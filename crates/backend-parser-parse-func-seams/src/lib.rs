//! Seam declarations for the `backend-parser-parse-func` unit
//! (`parser/parse_func.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use types_opclass::ObjectWithArgs;

seam_core::seam!(
    /// `LookupFuncWithArgs(OBJECT_FUNCTION, func, missing_ok)`
    /// (parse_func.c): resolve an `ObjectWithArgs` describing a plain function
    /// (the only object type opclasscmds.c uses) to its pg_proc OID. With
    /// `missing_ok = false` a missing function raises (`Err`); with
    /// `missing_ok = true` it returns `InvalidOid`.
    pub fn lookup_func_with_args(func: &ObjectWithArgs, missing_ok: bool) -> PgResult<Oid>
);
