//! Seam declarations for the `backend-parser-parse-func` unit
//! (`parser/parse_func.c`): function-name resolution.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::PgString;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::ObjectType;
use types_opclass::ObjectWithArgs;
use types_parsenodes::ObjectWithArgs as ParseObjectWithArgs;

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

seam_core::seam!(
    /// `LookupFuncWithArgs(objtype, func, missing_ok)` (parse_func.c): the
    /// object-type-aware form `get_object_address` uses for the
    /// `OBJECT_AGGREGATE`/`OBJECT_FUNCTION`/`OBJECT_PROCEDURE`/`OBJECT_ROUTINE`
    /// arms — `objtype` selects which `pg_proc.prokind`s are acceptable and
    /// whether aggregate/window/ordered-set handling applies. `func` crosses as
    /// the parser's own [`ParseObjectWithArgs`] (the `castNode(ObjectWithArgs,
    /// object)` the C switch passes). With `missing_ok = false` a missing
    /// routine raises (`Err`); else `InvalidOid`.
    pub fn lookup_func_with_args_for_objtype(
        objtype: ObjectType,
        func: &ParseObjectWithArgs,
        missing_ok: bool,
    ) -> PgResult<Oid>
);
