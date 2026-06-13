//! Seam declarations for the `backend-parser-parse-func` unit
//! (`parser/parse_func.c`): function-name resolution.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::PgString;
use types_core::Oid;
use types_error::PgResult;
use types_opclass::ObjectWithArgs;

seam_core::seam!(
    /// `LookupFuncName(funcname, nargs, argtypes, missing_ok)`
    /// (parse_func.c): resolve a possibly-qualified function name (a `List *`
    /// of `String` nodes, here the name components) with the given argument
    /// types to a `pg_proc` OID. With `missing_ok = false` a missing function
    /// raises (`Err`); with `missing_ok = true` it returns `InvalidOid`.
    pub fn lookup_func_name(
        funcname: &[PgString<'_>],
        nargs: i32,
        argtypes: &[Oid],
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `LookupFuncWithArgs(OBJECT_FUNCTION, func, missing_ok)`
    /// (parse_func.c): resolve an `ObjectWithArgs` describing a plain function
    /// (the only object type opclasscmds.c uses) to its pg_proc OID. With
    /// `missing_ok = false` a missing function raises (`Err`); with
    /// `missing_ok = true` it returns `InvalidOid`.
    pub fn lookup_func_with_args(func: &ObjectWithArgs, missing_ok: bool) -> PgResult<Oid>
);
