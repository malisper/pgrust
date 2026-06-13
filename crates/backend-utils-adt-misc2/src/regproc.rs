//! Family `regproc` — `src/backend/utils/adt/regproc.c`.
//!
//! I/O for the `reg*` object-identifier alias types: regproc / regprocedure /
//! regoper / regoperator / regclass / regcollation / regtype / regconfig /
//! regdictionary / regrole / regnamespace, each with in/out/recv/send plus the
//! soft `to_reg*` variants; the `format_procedure*` / `format_operator*`
//! helpers; and the name-parsing utilities `stringToQualifiedNameList`,
//! `parseNameAndArgTypes`, `parseNumericOid`, `parseDashOrOid`.
//!
//! These resolve names against the system catalogs (namespace search, syscache
//! lookups) and allocate result text, so they take `Mcx`, surface
//! `ereport`s as `PgResult`, and reach the catalogs through seams in their real
//! owners (namespace / syscache / lsyscache) — to be wired when this family is
//! filled. Independent of the keystone. Only the representative public surface
//! is enumerated here; the full ~70-function I/O matrix is filled in the port
//! phase.

use mcx::Mcx;
use types_datum::Datum;
use types_error::PgResult;

/// `regprocin(pro_name_or_oid)`.
pub fn regprocin<'mcx>(_mcx: Mcx<'mcx>, _name_or_oid: Option<&str>) -> PgResult<Datum> {
    todo!("regprocin")
}

/// `regprocout(proid)`.
pub fn regprocout<'mcx>(_mcx: Mcx<'mcx>, _proid: u32) -> PgResult<Datum> {
    todo!("regprocout")
}

/// `regclassin(class_name_or_oid)`.
pub fn regclassin<'mcx>(_mcx: Mcx<'mcx>, _name_or_oid: Option<&str>) -> PgResult<Datum> {
    todo!("regclassin")
}

/// `regclassout(classid)`.
pub fn regclassout<'mcx>(_mcx: Mcx<'mcx>, _classid: u32) -> PgResult<Datum> {
    todo!("regclassout")
}

/// `regtypein(typ_name_or_oid)`.
pub fn regtypein<'mcx>(_mcx: Mcx<'mcx>, _name_or_oid: Option<&str>) -> PgResult<Datum> {
    todo!("regtypein")
}

/// `regtypeout(typid)`.
pub fn regtypeout<'mcx>(_mcx: Mcx<'mcx>, _typid: u32) -> PgResult<Datum> {
    todo!("regtypeout")
}

/// `format_procedure(procedure_oid)` — qualified-as-needed signature text.
pub fn format_procedure<'mcx>(_mcx: Mcx<'mcx>, _procedure_oid: u32) -> PgResult<Datum> {
    todo!("format_procedure")
}

/// `format_operator(operator_oid)`.
pub fn format_operator<'mcx>(_mcx: Mcx<'mcx>, _operator_oid: u32) -> PgResult<Datum> {
    todo!("format_operator")
}

/// `stringToQualifiedNameList(string, escontext)` — split a possibly-quoted
/// dotted name into its component identifiers.
pub fn string_to_qualified_name_list<'mcx>(
    _mcx: Mcx<'mcx>,
    _string: &str,
) -> PgResult<alloc::vec::Vec<alloc::string::String>> {
    todo!("stringToQualifiedNameList")
}

/// `parseNameAndArgTypes(string, allowNone, names, nargs, argtypes)`.
pub fn parse_name_and_arg_types<'mcx>(
    _mcx: Mcx<'mcx>,
    _string: &str,
    _allow_none: bool,
) -> PgResult<()> {
    todo!("parseNameAndArgTypes")
}
