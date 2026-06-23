#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `backend/commands/conversioncmds.c` — conversion creation command support
//! code (`CREATE CONVERSION`).
//!
//! The single driver [`CreateConversionCommand`] is implemented in-crate
//! against the owned node tree with identical branch order, permission checks,
//! encoding validation, function lookup, return-type check, the empty-input
//! conversion-function self-test, the final `ConversionCreate`, and every error
//! code / message / SQLSTATE as PostgreSQL 18.3.
//!
//! `QualifiedNameGetCreationNamespace` / `NameListToString`
//! (`backend-catalog-namespace`) are direct deps. Genuine externals cross owner
//! seams: `GetUserId` (miscinit), `object_aclcheck` / `aclcheck_error`
//! (aclchk), `get_namespace_name` (lsyscache), `pg_char_to_encoding` (encnames),
//! `LookupFuncName` (parse_func), `get_func_rettype` (lsyscache), the
//! fmgr/`Datum` empty-input self-test (fmgr), and `ConversionCreate`
//! (pg_conversion, which records its own catalog dependencies — so this file
//! records none separately, exactly as the C does).

use mcx::{Mcx, PgString};

use utils_error::{ereport, PgResult};
use types_error::{ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_UNDEFINED_OBJECT, ERROR};

use ::types_acl::acl::{ACLCHECK_OK, ACL_CREATE, ACL_EXECUTE};
use ::types_catalog::catalog::{NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID};
use ::types_catalog::catalog_dependency::ObjectAddress;
use ::types_core::Oid;
use ::nodes::parsenodes::{CreateConversionStmt, ObjectType};
use ::types_tuple::heaptuple::{BOOLOID, CSTRINGOID, INT4OID, INTERNALOID};
use ::types_wchar::encoding::PG_SQL_ASCII;

use catalog_namespace::{NameListToString, QualifiedNameGetCreationNamespace};
use aclchk_seams::{aclcheck_error, object_aclcheck};
use ::pg_conversion_seams::conversion_create;
use ::parse_func_seams::lookup_func_name;
use lsyscache_seams::{get_func_rettype, get_namespace_name};
use ::fmgr_seams::conversion_proc_empty_input_test;
use ::miscinit_seams::get_user_id;
use ::encnames_seams::pg_char_to_encoding;

/// `static const Oid funcargs[]` — the conversion procedure's argument types:
/// `{INT4OID, INT4OID, CSTRINGOID, INTERNALOID, INT4OID, BOOLOID}`.
const FUNCARGS: [Oid; 6] = [INT4OID, INT4OID, CSTRINGOID, INTERNALOID, INT4OID, BOOLOID];

/*
 * CREATE CONVERSION
 */
pub fn CreateConversionCommand(mcx: Mcx<'_>, stmt: &CreateConversionStmt) -> PgResult<ObjectAddress> {
    // The encoding names come from the parse node as `char *` in C; the owned
    // node carries them as `Option<String>` (a missing name reduces to the
    // empty string, matching the null-`char *` → "" handling, which
    // `pg_char_to_encoding` then reports as a non-existent encoding).
    let from_encoding_name: &str = stmt.for_encoding_name.as_deref().unwrap_or("");
    let to_encoding_name: &str = stmt.to_encoding_name.as_deref().unwrap_or("");

    // The conversion-function name list. `NameListToString` (namespace crate)
    // takes `&[Option<String>]` (a `None` element marks `A_Star`), so the plain
    // `String` components are wrapped; `LookupFuncName` (the seam) takes a slice
    // of `PgString` components.
    let func_name_opt: Vec<Option<String>> =
        stmt.func_name.iter().map(|s| Some(s.clone())).collect();
    let func_name_pg: Vec<PgString<'_>> = stmt
        .func_name
        .iter()
        .map(|s| PgString::from_str_in(s, mcx))
        .collect::<PgResult<Vec<_>>>()?;

    /* Convert list of names to a name and namespace */
    let conversion_name_opt: Vec<Option<String>> = stmt
        .conversion_name
        .iter()
        .map(|s| Some(s.clone()))
        .collect();
    let (namespaceId, conversion_name) =
        QualifiedNameGetCreationNamespace(mcx, &conversion_name_opt)?;

    /* Check we have creation rights in target namespace */
    let aclresult =
        object_aclcheck::call(NAMESPACE_RELATION_ID, namespaceId, get_user_id::call(), ACL_CREATE)?;
    if aclresult != ACLCHECK_OK {
        let nspname = get_namespace_name::call(mcx, namespaceId)?.map(|s| s.as_str().to_string());
        // `aclcheck_error` is `pg_noreturn` in C; the seam always returns `Err`.
        aclcheck_error::call(aclresult, ObjectType::Schema, nspname)?;
    }

    /* Check the encoding names */
    let from_encoding = pg_char_to_encoding::call(from_encoding_name);
    if from_encoding < 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "source encoding \"{from_encoding_name}\" does not exist"
            ))
            .into_error());
    }

    let to_encoding = pg_char_to_encoding::call(to_encoding_name);
    if to_encoding < 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "destination encoding \"{to_encoding_name}\" does not exist"
            ))
            .into_error());
    }

    /*
     * We consider conversions to or from SQL_ASCII to be meaningless.  (If
     * you wish to change this, note that pg_do_encoding_conversion() and its
     * sister functions have hard-wired fast paths for any conversion in which
     * the source or target encoding is SQL_ASCII, so that an encoding
     * conversion function declared for such a case will never be used.)
     */
    if from_encoding == PG_SQL_ASCII || to_encoding == PG_SQL_ASCII {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("encoding conversion to or from \"SQL_ASCII\" is not supported")
            .into_error());
    }

    /*
     * Check the existence of the conversion function. Function name could be
     * a qualified name.
     */
    let funcoid = lookup_func_name::call(&func_name_pg, FUNCARGS.len() as i32, &FUNCARGS, false)?;

    /* Check it returns int4, else it's probably the wrong function */
    if get_func_rettype::call(funcoid)? != INT4OID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "encoding conversion function {} must return type {}",
                NameListToString(mcx, &func_name_opt)?.as_str(),
                "integer"
            ))
            .into_error());
    }

    /* Check we have EXECUTE rights for the function */
    let aclresult =
        object_aclcheck::call(PROCEDURE_RELATION_ID, funcoid, get_user_id::call(), ACL_EXECUTE)?;
    if aclresult != ACLCHECK_OK {
        let funcname = NameListToString(mcx, &func_name_opt)?.as_str().to_string();
        // `aclcheck_error` is `pg_noreturn` in C; the seam always returns `Err`.
        aclcheck_error::call(aclresult, ObjectType::Function, Some(funcname))?;
    }

    /*
     * Check that the conversion function is suitable for the requested source
     * and target encodings. We do that by calling the function with an empty
     * string; the conversion function should throw an error if it can't
     * perform the requested conversion.
     *
     * The fmgr owner builds the two `cstring` Datums (empty source / 1-byte
     * destination buffer) and runs `OidFunctionCall6`, returning
     * `DatumGetInt32(funcresult)`.
     */
    let funcresult = conversion_proc_empty_input_test::call(funcoid, from_encoding, to_encoding)?;

    /*
     * The function should return 0 for empty input. Might as well check that,
     * too.
     */
    if funcresult != 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "encoding conversion function {} returned incorrect result for empty input",
                NameListToString(mcx, &func_name_opt)?.as_str()
            ))
            .into_error());
    }

    /*
     * All seem ok, go ahead (possible failure would be a duplicate conversion
     * name)
     */
    conversion_create::call(
        conversion_name,
        namespaceId,
        get_user_id::call(),
        from_encoding,
        to_encoding,
        funcoid,
        stmt.def,
    )
}

/// `ProcessUtilitySlow`'s `T_CreateConversionStmt` arm (utility.c:1718): decode
/// the arena [`::nodes::nodes::Node`] into the owned
/// [`::nodes::parsenodes::CreateConversionStmt`] (the `List *` of `String`
/// name components flattened to `Vec<String>`) and run `CreateConversionCommand`.
fn create_conversion_command_seam<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    stmt: &::nodes::nodes::Node<'mcx>,
) -> PgResult<ObjectAddress> {
    let ccs = match stmt.as_createconversionstmt() {
        Some(s) => s,
        None => {
            return Err(::types_error::PgError::error(
                "create_conversion_command_seam: statement is not a CreateConversionStmt",
            ))
        }
    };

    // `conversion_name` / `func_name`: `List *` of `String` nodes -> Vec<String>.
    fn name_components(
        list: &[::nodes::nodes::NodePtr<'_>],
        what: &str,
    ) -> PgResult<Vec<String>> {
        let mut out = Vec::with_capacity(list.len());
        for n in list.iter() {
            match n.as_string() {
                Some(s) => out.push(s.sval.as_str().to_string()),
                None => {
                    return Err(::types_error::PgError::error(format!(
                        "CREATE CONVERSION: {what} element is not a String"
                    )))
                }
            }
        }
        Ok(out)
    }

    let owned = CreateConversionStmt {
        conversion_name: name_components(&ccs.conversion_name, "conversion name")?,
        for_encoding_name: ccs.for_encoding_name.as_ref().map(|s| s.as_str().to_string()),
        to_encoding_name: ccs.to_encoding_name.as_ref().map(|s| s.as_str().to_string()),
        func_name: name_components(&ccs.func_name, "function name")?,
        def: ccs.def,
    };
    CreateConversionCommand(mcx, &owned)
}

/// `conversioncmds.c` owns no inward seam, but it installs the
/// `ProcessUtilitySlow` `CREATE CONVERSION` outward arm (utility.c:1718) into
/// `backend-tcop-utility-out-seams` so the dispatcher can reach its
/// already-ported `CreateConversionCommand` driver.
pub fn init_seams() {
    utility_out_seams::create_conversion_command::set(create_conversion_command_seam);
}
