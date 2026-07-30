// conversioncmds.c: CREATE CONVERSION.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::{
    Oid, BOOLOID, CSTRINGOID, INT4OID, INTERNALOID, NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID,
};
use types_error::{PgError, PgResult, SqlState, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_UNDEFINED_OBJECT};
use types_fmgr::direct_function_call6_coll;
use types_nodes::parsenodes::{CreateConversionStmt, ObjectType};
use types_nodes::NodeList;

const PG_SQL_ASCII: i32 = 0;

fn name_list_to_string(names: &NodeList<'_>) -> String {
    let mut out = String::new();
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(n.as_string().expect("qualified name component is a String node").sval);
    }
    out
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid(msg: String, sqlstate: SqlState) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

/// C `CreateConversionCommand`.
pub fn CreateConversionCommand<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateConversionStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let names: Vec<&str> = stmt
        .conversion_name
        .iter()
        .map(|n| n.as_string().expect("qualified name component is a String node").sval)
        .collect();
    let (namespace_id, conversion_name) =
        catalog_namespace::QualifiedNameGetCreationNamespace(mcx, &names)?;

    let aclresult = aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        namespace_id,
        miscinit::GetUserId(),
        adt_acl::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let nspname = lsyscache::get_namespace_name(mcx, namespace_id)?
            .map(|s| s.to_string())
            .unwrap_or_default();
        aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_SCHEMA, &nspname)?;
    }

    let from_encoding_name = stmt.for_encoding_name.expect("FOR encoding name");
    let to_encoding_name = stmt.to_encoding_name.expect("TO encoding name");
    let from_encoding = mbutils::pg_char_to_encoding(from_encoding_name);
    if from_encoding < 0 {
        return Err(invalid(
            format!("source encoding \"{from_encoding_name}\" does not exist"),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    }
    let to_encoding = mbutils::pg_char_to_encoding(to_encoding_name);
    if to_encoding < 0 {
        return Err(invalid(
            format!("destination encoding \"{to_encoding_name}\" does not exist"),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    }

    // C: pg_do_encoding_conversion() hard-wires SQL_ASCII fast paths, so such
    // a conversion function would never be used.
    if from_encoding == PG_SQL_ASCII || to_encoding == PG_SQL_ASCII {
        return Err(invalid(
            "encoding conversion to or from \"SQL_ASCII\" is not supported".into(),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }

    const FUNCARGS: [Oid; 6] = [INT4OID, INT4OID, CSTRINGOID, INTERNALOID, INT4OID, BOOLOID];
    let funcoid = parse_func::LookupFuncName(&stmt.func_name, FUNCARGS.len() as i16, &FUNCARGS, false)?;

    if lsyscache::get_func_rettype(funcoid)? != INT4OID {
        return Err(invalid(
            format!(
                "encoding conversion function {} must return type {}",
                name_list_to_string(&stmt.func_name),
                "integer"
            ),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }

    let aclresult = aclchk::object_aclcheck(
        PROCEDURE_RELATION_ID,
        funcoid,
        miscinit::GetUserId(),
        adt_acl::ACL_EXECUTE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclchk::aclcheck_error(
            aclresult,
            ObjectType::OBJECT_FUNCTION,
            &name_list_to_string(&stmt.func_name),
        )?;
    }

    // C: OidFunctionCall6 with an empty string — the conversion function
    // errors if it can't serve the requested pair, and must return 0 here.
    let finfo = fmgr_seams::fmgr_info::call(funcoid)?;
    let src = [0u8; 1];
    let mut result = [0u8; 1];
    let funcresult = direct_function_call6_coll(
        finfo.fn_addr,
        types_core::InvalidOid,
        Datum::from_i32(from_encoding),
        Datum::from_i32(to_encoding),
        Datum::from_usize(src.as_ptr() as usize),
        Datum::from_usize(result.as_mut_ptr() as usize),
        Datum::from_i32(0),
        Datum::from_bool(false),
    )?;
    if funcresult.as_i32() != 0 {
        return Err(invalid(
            format!(
                "encoding conversion function {} returned incorrect result for empty input",
                name_list_to_string(&stmt.func_name)
            ),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }

    pg_conversion::ConversionCreate(
        mcx,
        conversion_name,
        namespace_id,
        miscinit::GetUserId(),
        from_encoding,
        to_encoding,
        funcoid,
        stmt.def,
    )
}
