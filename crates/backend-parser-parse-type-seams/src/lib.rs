//! Seam declarations for the `backend-parser-parse-type` unit
//! (`parser/parse_type.c`): type-name string parsing, name-list rendering and
//! type-name resolution.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;
use types_opclass::TypeName;
use types_parsenodes::TypeName as ParseTypeName;

seam_core::seam!(
    /// `parseTypeString(str, &typeid, &typmod, escontext)` (parse_type.c):
    /// invoke the full grammar to resolve a type name (handling array syntax,
    /// `DOUBLE PRECISION`, decoration, etc.) to its type OID and typmod.
    ///
    /// The owned model folds the C out-parameters and boolean return into the
    /// result: `Ok(Some((typeid, typmod)))` is the C `true` return, and
    /// `Ok(None)` is the C `false` return — only reachable when a soft-error
    /// `ErrorSaveContext` was supplied (modeled by `soft = true`). With
    /// `soft = false` (no soft-error context) a bad type name propagates as a
    /// hard error on `Err`.
    pub fn parse_type_string(
        string: &str,
        soft: bool,
    ) -> PgResult<Option<(Oid, i32)>>
);

seam_core::seam!(
    /// `NameListToString(names)` (parse_type.c): render a possibly-qualified
    /// name (`List *` of `String`/`A_Star` nodes, here the name components)
    /// into a dotted string, allocated in `mcx` (C: `StringInfo` in the
    /// current context).
    pub fn name_list_to_string<'mcx>(
        mcx: Mcx<'mcx>,
        names: &[PgString<'_>],
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `typenameTypeId(NULL, typeName)` (parse_type.c): resolve a `TypeName`
    /// to its type OID, raising if the type does not exist or is only a shell.
    /// `Err` carries that `ereport(ERROR)` surface.
    pub fn typename_type_id(type_name: &TypeName) -> PgResult<Oid>
);

seam_core::seam!(
    /// `typeTypeId(LookupTypeName(NULL, typeName, NULL, false))` (parse_type.c):
    /// resolve a `TypeName` to its type OID, raising `"type \"%s\" does not
    /// exist"` only when no row exists. Unlike [`typename_type_id`], a *shell*
    /// type is returned (not rejected), matching the `LookupTypeName` path
    /// `AlterTypeOwner` (typecmds.c) uses so shell types can be reassigned.
    /// `Err` carries that `ereport(ERROR)` surface.
    pub fn lookup_type_name_oid_from_names(type_name: &TypeName) -> PgResult<Oid>
);

seam_core::seam!(
    /// `TypeNameToString(typeName)` (parse_type.c): the type name rendered for
    /// an error message, palloc'd in the caller's current context (`mcx`).
    /// `Err` includes OOM from the construction.
    pub fn typename_to_string<'mcx>(
        mcx: Mcx<'mcx>,
        type_name: &TypeName,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `TypeNameToString(typeName)` (parse_type.c), over the raw-parser
    /// `TypeName` node (`nodes/parsenodes.h`) carried in a `DefElem`'s value.
    /// `defGetString`/`defGetTypeLength` render the type name for an error
    /// message, palloc'd in the caller's `mcx`.
    pub fn typename_to_string_node<'mcx>(
        mcx: Mcx<'mcx>,
        type_name: &ParseTypeName,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `LookupTypeNameOid(NULL, typeName, missing_ok)` (parse_type.c): resolve
    /// a raw-parser `TypeName` node to its type OID, returning `InvalidOid`
    /// (the C `InvalidOid` with `missing_ok = true`) when the type does not
    /// exist. With `missing_ok = false`, or for any catalog failure, the error
    /// is carried on `Err`.
    pub fn lookup_type_name_oid(type_name: &ParseTypeName, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `typenameTypeId(NULL, typeName)` (parse_type.c), over the raw-parser
    /// `TypeName` node (`nodes/parsenodes.h`). Used by `check_object_ownership`'s
    /// `OBJECT_CAST` / `OBJECT_TRANSFORM` arms (the source/target type ownership
    /// probe). Raises if the type does not exist or is only a shell.
    pub fn typename_type_id_node(type_name: &ParseTypeName) -> PgResult<Oid>
);

seam_core::seam!(
    /// `typenameTypeId(NULL, typeName)` (parse_type.c), over the owned-tree
    /// `rawnodes::TypeName<'mcx>` the grammar produces (carried in PREPARE's
    /// `argtypes`). The owner bridges it to the resolver-facing `TypeName`.
    /// Raises if the type does not exist or is only a shell.
    pub fn typename_type_id_raw(type_name: &types_nodes::rawnodes::TypeName<'_>) -> PgResult<Oid>
);

seam_core::seam!(
    /// `TypeNameListToString(typenames)` (parse_func.c): render a comma-
    /// separated list of raw-parser `TypeName` nodes (a function/aggregate
    /// argument-type list) for an error message, palloc'd in the caller's
    /// `mcx`. `Err` includes OOM from the construction.
    pub fn type_name_list_to_string<'mcx>(
        mcx: Mcx<'mcx>,
        typenames: &[ParseTypeName],
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `LookupTypeNameOid(NULL, typeName, missing_ok)` (parse_type.c) over the
    /// opclasscmds/function `TypeName` carrier (`types_opclass::TypeName`),
    /// resolving a (possibly schema-qualified) type name to its OID. With
    /// `missing_ok = true` a missing type yields `InvalidOid` (no error); else
    /// it raises. Used by `LookupFuncWithArgs` (parse_func.c) to resolve the
    /// `objargs` of an `ObjectWithArgs`.
    pub fn lookup_type_name_oid_owa(type_name: &TypeName, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `FuncNameAsType(funcname)` (parse_func.c) reduced to its `parse_type.c`
    /// dependency: `LookupTypeNameExtended(NULL,
    /// makeTypeNameFromNameList(funcname), NULL, false, false)` then the
    /// `typisdefined && !typeTypeRelid` filter, returning the matching scalar
    /// type's OID or `InvalidOid` (shell and composite types are ignored). The
    /// name crosses as the `String`-list components. `Err` is reserved for
    /// catalog-path `ereport(ERROR)`s; an absent type is `Ok(InvalidOid)`.
    pub fn func_name_as_type<'mcx>(funcname: &[PgString<'_>]) -> PgResult<Oid>
);

seam_core::seam!(
    /// `typenameTypeId(pstate, defGetTypeName(def))` (sequence.c
    /// `init_params` AS-type leg): the owner runs `defGetTypeName` over the
    /// `DefElem`'s raw-parser `TypeName` value node and resolves it to a type
    /// OID. Raises if the type does not exist or is only a shell. `Err`
    /// carries that surface.
    pub fn typename_type_id_from_defelem(
        def: &types_nodes::ddlnodes::DefElem<'_>,
    ) -> types_error::PgResult<types_core::Oid>
);

seam_core::seam!(
    /// `typeStringToTypeName(str, NULL)` (parse_type.c): parse a type-name string
    /// into a raw-parser `TypeName` node (`raw_parser(RAW_PARSE_TYPE_NAME)`),
    /// rejecting `SETOF`. Used by `pg_get_object_address` (objectaddress.c) to
    /// turn the SQL `text[]` name/args elements of the type-bearing object types
    /// into the `TypeName` node `get_object_address` expects. With the C
    /// `escontext = NULL`, an empty/whitespace or `SETOF` string hard-raises
    /// `"invalid type name \"%s\""`; carried on `Err`.
    pub fn type_string_to_type_name(string: &str) -> PgResult<ParseTypeName>
);
