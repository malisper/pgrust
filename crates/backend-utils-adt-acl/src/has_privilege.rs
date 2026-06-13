//! The `has_*_privilege` SQL families, `pg_has_role`, and their helpers
//! (`utils/adt/acl.c`).
//!
//! Each object class has a fan of `*_name_name` / `*_name` / `*_name_id` /
//! `*_id` / `*_id_name` / `*_id_id` SQL entrypoints plus a `convert_<obj>_name`
//! and a `convert_<obj>_priv_string` helper. The shared internal checks
//! (`column_privilege_check`, `has_param_priv_byname`, `has_lo_priv_byid`,
//! `pg_role_aclcheck`) live here too.

use crate::{FunctionCallInfo, PrivMap};
use mcx::Mcx;
use types_acl::{
    acl_grant_option_for, AclMode, AclResult, ACLCHECK_NO_PRIV, ACLCHECK_OK, ACLMASK_ANY,
    ACL_ALTER_SYSTEM, ACL_CONNECT, ACL_CREATE, ACL_CREATE_TEMP, ACL_DELETE, ACL_EXECUTE,
    ACL_INSERT, ACL_MAINTAIN, ACL_REFERENCES, ACL_SELECT, ACL_SET, ACL_TRIGGER, ACL_TRUNCATE,
    ACL_UPDATE, ACL_USAGE,
};
use types_core::primitive::{AttrNumber, InvalidAttrNumber, OidIsValid};
use types_core::{
    Oid, DATABASE_RELATION_ID, FOREIGN_DATA_WRAPPER_RELATION_ID, FOREIGN_SERVER_RELATION_ID,
    LANGUAGE_RELATION_ID, NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID, TABLE_SPACE_RELATION_ID,
    TYPE_RELATION_ID,
};
use types_datum::varlena::Bytea;
use types_datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_storage::lock::NoLock;
use types_tuple::access::RELKIND_SEQUENCE;

use port_pgstrcasecmp::pg_strcasecmp;

use backend_catalog_aclchk_seams as aclchk;
use backend_catalog_namespace_seams as namespace;
use backend_catalog_pg_largeobject_seams as pg_largeobject;
use backend_commands_dbcommands_seams as dbcommands;
use backend_commands_proclang_seams as proclang;
use backend_commands_tablespace_seams as tablespace;
use backend_foreign_foreign_seams as foreign;
use backend_utils_adt_regproc_seams as regproc;
use backend_utils_adt_varlena_seams as varlena;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_fmgr_fmgr_seams as fmgr;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_time_snapmgr_seams as snapmgr;

// ===========================================================================
// has_table_privilege family — C: acl.c
// ===========================================================================

/// `has_table_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_table_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let rolename = fmgr::pg_getarg_name::call(fcinfo, 0);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&rolename)?;
    let tableoid = convert_table_name(mcx, &tablename)?;
    let mode = convert_table_priv_string(&priv_type_text)?;

    let aclresult = aclchk::pg_class_aclcheck::call(tableoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_table_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_table_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 0)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;

    let roleid = miscinit::get_user_id::call();
    let tableoid = convert_table_name(mcx, &tablename)?;
    let mode = convert_table_priv_string(&priv_type_text)?;

    let aclresult = aclchk::pg_class_aclcheck::call(tableoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_table_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_table_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let username = fmgr::pg_getarg_name::call(fcinfo, 0);
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&username)?;
    let mode = convert_table_priv_string(&priv_type_text)?;

    let (aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_table_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_table_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;

    let roleid = miscinit::get_user_id::call();
    let mode = convert_table_priv_string(&priv_type_text)?;

    let (aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_table_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_table_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let tableoid = convert_table_name(mcx, &tablename)?;
    let mode = convert_table_priv_string(&priv_type_text)?;

    let aclresult = aclchk::pg_class_aclcheck::call(tableoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_table_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_table_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let mode = convert_table_priv_string(&priv_type_text)?;

    let (aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

// ===========================================================================
// has_sequence_privilege family — C: acl.c
// ===========================================================================

/// C: `text_to_cstring(name)` for an error message — the `text` value has no
/// embedded NUL, so the payload bytes are the C string contents.
fn name_cstr(name: &Bytea) -> alloc::string::String {
    alloc::string::String::from_utf8_lossy(name.data()).into_owned()
}

/// C: `"%s" is not a sequence` (`ERRCODE_WRONG_OBJECT_TYPE`).
fn not_a_sequence(name: &str) -> PgError {
    PgError::error(alloc::format!("\"{name}\" is not a sequence"))
        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
}

/// C: `get_rel_name(oid)` returns NULL for a missing relation; the error
/// messages then print the (possibly empty) name. Mirror that string view.
fn rel_name_str<'a>(name: &'a Option<mcx::PgString<'_>>) -> &'a str {
    match name {
        Some(s) => s.as_str(),
        None => "",
    }
}

/// `has_sequence_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_sequence_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let rolename = fmgr::pg_getarg_name::call(fcinfo, 0);
    let sequencename = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&rolename)?;
    let mode = convert_sequence_priv_string(&priv_type_text)?;
    let sequenceoid = convert_table_name(mcx, &sequencename)?;
    if lsyscache::get_rel_relkind::call(sequenceoid)? != RELKIND_SEQUENCE {
        return Err(not_a_sequence(&name_cstr(&sequencename)));
    }

    let aclresult = aclchk::pg_class_aclcheck::call(sequenceoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_sequence_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_sequence_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let sequencename = fmgr::pg_getarg_text_pp::call(fcinfo, 0)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;

    let roleid = miscinit::get_user_id::call();
    let mode = convert_sequence_priv_string(&priv_type_text)?;
    let sequenceoid = convert_table_name(mcx, &sequencename)?;
    if lsyscache::get_rel_relkind::call(sequenceoid)? != RELKIND_SEQUENCE {
        return Err(not_a_sequence(&name_cstr(&sequencename)));
    }

    let aclresult = aclchk::pg_class_aclcheck::call(sequenceoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_sequence_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_sequence_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let username = fmgr::pg_getarg_name::call(fcinfo, 0);
    let sequenceoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&username)?;
    let mode = convert_sequence_priv_string(&priv_type_text)?;
    let relkind = lsyscache::get_rel_relkind::call(sequenceoid)?;
    if relkind == b'\0' {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    } else if relkind != RELKIND_SEQUENCE {
        let relname = lsyscache::get_rel_name::call(mcx, sequenceoid)?;
        return Err(not_a_sequence(rel_name_str(&relname)));
    }

    let (aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(sequenceoid, roleid, mode)?;
    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_sequence_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_sequence_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let sequenceoid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;

    let roleid = miscinit::get_user_id::call();
    let mode = convert_sequence_priv_string(&priv_type_text)?;
    let relkind = lsyscache::get_rel_relkind::call(sequenceoid)?;
    if relkind == b'\0' {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    } else if relkind != RELKIND_SEQUENCE {
        let relname = lsyscache::get_rel_name::call(mcx, sequenceoid)?;
        return Err(not_a_sequence(rel_name_str(&relname)));
    }

    let (aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(sequenceoid, roleid, mode)?;
    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_sequence_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_sequence_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let sequencename = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let mode = convert_sequence_priv_string(&priv_type_text)?;
    let sequenceoid = convert_table_name(mcx, &sequencename)?;
    if lsyscache::get_rel_relkind::call(sequenceoid)? != RELKIND_SEQUENCE {
        return Err(not_a_sequence(&name_cstr(&sequencename)));
    }

    let aclresult = aclchk::pg_class_aclcheck::call(sequenceoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_sequence_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_sequence_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let sequenceoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let mode = convert_sequence_priv_string(&priv_type_text)?;
    let relkind = lsyscache::get_rel_relkind::call(sequenceoid)?;
    if relkind == b'\0' {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    } else if relkind != RELKIND_SEQUENCE {
        let relname = lsyscache::get_rel_name::call(mcx, sequenceoid)?;
        return Err(not_a_sequence(rel_name_str(&relname)));
    }

    let (aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(sequenceoid, roleid, mode)?;
    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

// ===========================================================================
// has_any_column_privilege family — C: acl.c
// ===========================================================================

/// `has_any_column_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_any_column_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let rolename = fmgr::pg_getarg_name::call(fcinfo, 0);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&rolename)?;
    let tableoid = convert_table_name(mcx, &tablename)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let mut aclresult = aclchk::pg_class_aclcheck::call(tableoid, roleid, mode)?;
    if aclresult != ACLCHECK_OK {
        aclresult = aclchk::pg_attribute_aclcheck_all::call(tableoid, roleid, mode, ACLMASK_ANY)?;
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_any_column_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_any_column_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 0)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;

    let roleid = miscinit::get_user_id::call();
    let tableoid = convert_table_name(mcx, &tablename)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let mut aclresult = aclchk::pg_class_aclcheck::call(tableoid, roleid, mode)?;
    if aclresult != ACLCHECK_OK {
        aclresult = aclchk::pg_attribute_aclcheck_all::call(tableoid, roleid, mode, ACLMASK_ANY)?;
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_any_column_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_any_column_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let username = fmgr::pg_getarg_name::call(fcinfo, 0);
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&username)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let (mut aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if aclresult != ACLCHECK_OK {
        if is_missing {
            return Ok(fmgr::pg_return_null::call(fcinfo));
        }
        let (r, m) =
            aclchk::pg_attribute_aclcheck_all_ext::call(tableoid, roleid, mode, ACLMASK_ANY)?;
        aclresult = r;
        if m {
            return Ok(fmgr::pg_return_null::call(fcinfo));
        }
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_any_column_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_any_column_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;

    let roleid = miscinit::get_user_id::call();
    let mode = convert_column_priv_string(&priv_type_text)?;

    let (mut aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if aclresult != ACLCHECK_OK {
        if is_missing {
            return Ok(fmgr::pg_return_null::call(fcinfo));
        }
        let (r, m) =
            aclchk::pg_attribute_aclcheck_all_ext::call(tableoid, roleid, mode, ACLMASK_ANY)?;
        aclresult = r;
        if m {
            return Ok(fmgr::pg_return_null::call(fcinfo));
        }
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_any_column_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_any_column_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let tableoid = convert_table_name(mcx, &tablename)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let mut aclresult = aclchk::pg_class_aclcheck::call(tableoid, roleid, mode)?;
    if aclresult != ACLCHECK_OK {
        aclresult = aclchk::pg_attribute_aclcheck_all::call(tableoid, roleid, mode, ACLMASK_ANY)?;
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `has_any_column_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_any_column_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let mode = convert_column_priv_string(&priv_type_text)?;

    let (mut aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if aclresult != ACLCHECK_OK {
        if is_missing {
            return Ok(fmgr::pg_return_null::call(fcinfo));
        }
        let (r, m) =
            aclchk::pg_attribute_aclcheck_all_ext::call(tableoid, roleid, mode, ACLMASK_ANY)?;
        aclresult = r;
        if m {
            return Ok(fmgr::pg_return_null::call(fcinfo));
        }
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

// ===========================================================================
// has_column_privilege family — C: acl.c
// ===========================================================================

/// Shared tail: encode a `column_privilege_check` tri-state result.
fn column_priv_return(fcinfo: FunctionCallInfo, privresult: i32) -> Datum {
    if privresult < 0 {
        fmgr::pg_return_null::call(fcinfo)
    } else {
        fmgr::pg_return_bool::call(fcinfo, privresult != 0)
    }
}

/// `has_column_privilege_name_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_name_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let rolename = fmgr::pg_getarg_name::call(fcinfo, 0);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let column = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 3)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&rolename)?;
    let tableoid = convert_table_name(mcx, &tablename)?;
    let colattnum = convert_column_name(mcx, tableoid, &column)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

/// `has_column_privilege_name_name_attnum(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_name_name_attnum(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let rolename = fmgr::pg_getarg_name::call(fcinfo, 0);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let colattnum = fmgr::pg_getarg_int16::call(fcinfo, 2);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 3)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&rolename)?;
    let tableoid = convert_table_name(mcx, &tablename)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

/// `has_column_privilege_name_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_name_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let username = fmgr::pg_getarg_name::call(fcinfo, 0);
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let column = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 3)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&username)?;
    let colattnum = convert_column_name(mcx, tableoid, &column)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

/// `has_column_privilege_name_id_attnum(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_name_id_attnum(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let username = fmgr::pg_getarg_name::call(fcinfo, 0);
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let colattnum = fmgr::pg_getarg_int16::call(fcinfo, 2);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 3)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&username)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

/// `has_column_privilege_id_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_id_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let column = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 3)?;

    let tableoid = convert_table_name(mcx, &tablename)?;
    let colattnum = convert_column_name(mcx, tableoid, &column)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

/// `has_column_privilege_id_name_attnum(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_id_name_attnum(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let colattnum = fmgr::pg_getarg_int16::call(fcinfo, 2);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 3)?;

    let tableoid = convert_table_name(mcx, &tablename)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

/// `has_column_privilege_id_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_id_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let column = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 3)?;

    let colattnum = convert_column_name(mcx, tableoid, &column)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

/// `has_column_privilege_id_id_attnum(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_id_id_attnum(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let colattnum = fmgr::pg_getarg_int16::call(fcinfo, 2);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 3)?;

    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

/// `has_column_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 0)?;
    let column = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = miscinit::get_user_id::call();
    let tableoid = convert_table_name(mcx, &tablename)?;
    let colattnum = convert_column_name(mcx, tableoid, &column)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

/// `has_column_privilege_name_attnum(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_name_attnum(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let tablename = fmgr::pg_getarg_text_pp::call(fcinfo, 0)?;
    let colattnum = fmgr::pg_getarg_int16::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = miscinit::get_user_id::call();
    let tableoid = convert_table_name(mcx, &tablename)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

/// `has_column_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let column = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = miscinit::get_user_id::call();
    let colattnum = convert_column_name(mcx, tableoid, &column)?;
    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

/// `has_column_privilege_id_attnum(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_column_privilege_id_attnum(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let tableoid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let colattnum = fmgr::pg_getarg_int16::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = miscinit::get_user_id::call();
    let mode = convert_column_priv_string(&priv_type_text)?;

    let privresult = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    Ok(column_priv_return(fcinfo, privresult))
}

// ===========================================================================
// object-class families reached through object_aclcheck (acl.c)
// ===========================================================================

/// Shared `object_aclcheck` family driver for the name × text variant.
fn object_priv_name_name(
    fcinfo: FunctionCallInfo,
    classid: Oid,
    convert_name: impl Fn(Mcx<'_>, &Bytea) -> PgResult<Oid>,
    convert_priv: impl Fn(&Bytea) -> PgResult<AclMode>,
) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let username = fmgr::pg_getarg_name::call(fcinfo, 0);
    let objname = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&username)?;
    let objoid = convert_name(mcx, &objname)?;
    let mode = convert_priv(&priv_type_text)?;

    let aclresult = aclchk::object_aclcheck::call(classid, objoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

fn object_priv_name(
    fcinfo: FunctionCallInfo,
    classid: Oid,
    convert_name: impl Fn(Mcx<'_>, &Bytea) -> PgResult<Oid>,
    convert_priv: impl Fn(&Bytea) -> PgResult<AclMode>,
) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let objname = fmgr::pg_getarg_text_pp::call(fcinfo, 0)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;

    let roleid = miscinit::get_user_id::call();
    let objoid = convert_name(mcx, &objname)?;
    let mode = convert_priv(&priv_type_text)?;

    let aclresult = aclchk::object_aclcheck::call(classid, objoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

fn object_priv_name_id(
    fcinfo: FunctionCallInfo,
    classid: Oid,
    convert_priv: impl Fn(&Bytea) -> PgResult<AclMode>,
) -> PgResult<Datum> {
    let username = fmgr::pg_getarg_name::call(fcinfo, 0);
    let objoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = crate::role_membership::get_role_oid_or_public(&username)?;
    let mode = convert_priv(&priv_type_text)?;

    let (aclresult, is_missing) = aclchk::object_aclcheck_ext::call(classid, objoid, roleid, mode)?;
    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

fn object_priv_id(
    fcinfo: FunctionCallInfo,
    classid: Oid,
    convert_priv: impl Fn(&Bytea) -> PgResult<AclMode>,
) -> PgResult<Datum> {
    let objoid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;

    let roleid = miscinit::get_user_id::call();
    let mode = convert_priv(&priv_type_text)?;

    let (aclresult, is_missing) = aclchk::object_aclcheck_ext::call(classid, objoid, roleid, mode)?;
    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

fn object_priv_id_name(
    fcinfo: FunctionCallInfo,
    classid: Oid,
    convert_name: impl Fn(Mcx<'_>, &Bytea) -> PgResult<Oid>,
    convert_priv: impl Fn(&Bytea) -> PgResult<AclMode>,
) -> PgResult<Datum> {
    let mcx = fmgr::pg_call_mcx::call(fcinfo);
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let objname = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let objoid = convert_name(mcx, &objname)?;
    let mode = convert_priv(&priv_type_text)?;

    let aclresult = aclchk::object_aclcheck::call(classid, objoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

fn object_priv_id_id(
    fcinfo: FunctionCallInfo,
    classid: Oid,
    convert_priv: impl Fn(&Bytea) -> PgResult<AclMode>,
) -> PgResult<Datum> {
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let objoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let mode = convert_priv(&priv_type_text)?;

    let (aclresult, is_missing) = aclchk::object_aclcheck_ext::call(classid, objoid, roleid, mode)?;
    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

// --- database ---

/// `has_database_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_database_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_name(fcinfo, DATABASE_RELATION_ID, convert_database_name, convert_database_priv_string)
}
/// `has_database_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_database_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name(fcinfo, DATABASE_RELATION_ID, convert_database_name, convert_database_priv_string)
}
/// `has_database_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_database_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_id(fcinfo, DATABASE_RELATION_ID, convert_database_priv_string)
}
/// `has_database_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_database_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id(fcinfo, DATABASE_RELATION_ID, convert_database_priv_string)
}
/// `has_database_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_database_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_name(fcinfo, DATABASE_RELATION_ID, convert_database_name, convert_database_priv_string)
}
/// `has_database_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_database_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_id(fcinfo, DATABASE_RELATION_ID, convert_database_priv_string)
}

// --- foreign-data wrapper ---

/// `has_foreign_data_wrapper_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_foreign_data_wrapper_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_name(fcinfo, FOREIGN_DATA_WRAPPER_RELATION_ID, convert_foreign_data_wrapper_name, convert_foreign_data_wrapper_priv_string)
}
/// `has_foreign_data_wrapper_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_foreign_data_wrapper_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name(fcinfo, FOREIGN_DATA_WRAPPER_RELATION_ID, convert_foreign_data_wrapper_name, convert_foreign_data_wrapper_priv_string)
}
/// `has_foreign_data_wrapper_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_foreign_data_wrapper_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_id(fcinfo, FOREIGN_DATA_WRAPPER_RELATION_ID, convert_foreign_data_wrapper_priv_string)
}
/// `has_foreign_data_wrapper_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_foreign_data_wrapper_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id(fcinfo, FOREIGN_DATA_WRAPPER_RELATION_ID, convert_foreign_data_wrapper_priv_string)
}
/// `has_foreign_data_wrapper_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_foreign_data_wrapper_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_name(fcinfo, FOREIGN_DATA_WRAPPER_RELATION_ID, convert_foreign_data_wrapper_name, convert_foreign_data_wrapper_priv_string)
}
/// `has_foreign_data_wrapper_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_foreign_data_wrapper_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_id(fcinfo, FOREIGN_DATA_WRAPPER_RELATION_ID, convert_foreign_data_wrapper_priv_string)
}

// --- function ---

/// `has_function_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_function_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_name(fcinfo, PROCEDURE_RELATION_ID, convert_function_name, convert_function_priv_string)
}
/// `has_function_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_function_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name(fcinfo, PROCEDURE_RELATION_ID, convert_function_name, convert_function_priv_string)
}
/// `has_function_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_function_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_id(fcinfo, PROCEDURE_RELATION_ID, convert_function_priv_string)
}
/// `has_function_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_function_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id(fcinfo, PROCEDURE_RELATION_ID, convert_function_priv_string)
}
/// `has_function_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_function_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_name(fcinfo, PROCEDURE_RELATION_ID, convert_function_name, convert_function_priv_string)
}
/// `has_function_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_function_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_id(fcinfo, PROCEDURE_RELATION_ID, convert_function_priv_string)
}

// --- language ---

/// `has_language_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_language_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_name(fcinfo, LANGUAGE_RELATION_ID, convert_language_name, convert_language_priv_string)
}
/// `has_language_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_language_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name(fcinfo, LANGUAGE_RELATION_ID, convert_language_name, convert_language_priv_string)
}
/// `has_language_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_language_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_id(fcinfo, LANGUAGE_RELATION_ID, convert_language_priv_string)
}
/// `has_language_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_language_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id(fcinfo, LANGUAGE_RELATION_ID, convert_language_priv_string)
}
/// `has_language_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_language_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_name(fcinfo, LANGUAGE_RELATION_ID, convert_language_name, convert_language_priv_string)
}
/// `has_language_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_language_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_id(fcinfo, LANGUAGE_RELATION_ID, convert_language_priv_string)
}

// --- schema ---

/// `has_schema_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_schema_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_name(fcinfo, NAMESPACE_RELATION_ID, convert_schema_name, convert_schema_priv_string)
}
/// `has_schema_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_schema_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name(fcinfo, NAMESPACE_RELATION_ID, convert_schema_name, convert_schema_priv_string)
}
/// `has_schema_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_schema_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_id(fcinfo, NAMESPACE_RELATION_ID, convert_schema_priv_string)
}
/// `has_schema_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_schema_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id(fcinfo, NAMESPACE_RELATION_ID, convert_schema_priv_string)
}
/// `has_schema_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_schema_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_name(fcinfo, NAMESPACE_RELATION_ID, convert_schema_name, convert_schema_priv_string)
}
/// `has_schema_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_schema_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_id(fcinfo, NAMESPACE_RELATION_ID, convert_schema_priv_string)
}

// --- server ---

/// `has_server_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_server_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_name(fcinfo, FOREIGN_SERVER_RELATION_ID, convert_server_name, convert_server_priv_string)
}
/// `has_server_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_server_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name(fcinfo, FOREIGN_SERVER_RELATION_ID, convert_server_name, convert_server_priv_string)
}
/// `has_server_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_server_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_id(fcinfo, FOREIGN_SERVER_RELATION_ID, convert_server_priv_string)
}
/// `has_server_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_server_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id(fcinfo, FOREIGN_SERVER_RELATION_ID, convert_server_priv_string)
}
/// `has_server_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_server_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_name(fcinfo, FOREIGN_SERVER_RELATION_ID, convert_server_name, convert_server_priv_string)
}
/// `has_server_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_server_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_id(fcinfo, FOREIGN_SERVER_RELATION_ID, convert_server_priv_string)
}

// --- tablespace ---

/// `has_tablespace_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_tablespace_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_name(fcinfo, TABLE_SPACE_RELATION_ID, convert_tablespace_name, convert_tablespace_priv_string)
}
/// `has_tablespace_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_tablespace_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name(fcinfo, TABLE_SPACE_RELATION_ID, convert_tablespace_name, convert_tablespace_priv_string)
}
/// `has_tablespace_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_tablespace_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_id(fcinfo, TABLE_SPACE_RELATION_ID, convert_tablespace_priv_string)
}
/// `has_tablespace_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_tablespace_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id(fcinfo, TABLE_SPACE_RELATION_ID, convert_tablespace_priv_string)
}
/// `has_tablespace_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_tablespace_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_name(fcinfo, TABLE_SPACE_RELATION_ID, convert_tablespace_name, convert_tablespace_priv_string)
}
/// `has_tablespace_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_tablespace_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_id(fcinfo, TABLE_SPACE_RELATION_ID, convert_tablespace_priv_string)
}

// --- type ---

/// `has_type_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_type_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_name(fcinfo, TYPE_RELATION_ID, convert_type_name, convert_type_priv_string)
}
/// `has_type_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_type_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name(fcinfo, TYPE_RELATION_ID, convert_type_name, convert_type_priv_string)
}
/// `has_type_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_type_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_name_id(fcinfo, TYPE_RELATION_ID, convert_type_priv_string)
}
/// `has_type_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_type_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id(fcinfo, TYPE_RELATION_ID, convert_type_priv_string)
}
/// `has_type_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_type_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_name(fcinfo, TYPE_RELATION_ID, convert_type_name, convert_type_priv_string)
}
/// `has_type_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_type_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    object_priv_id_id(fcinfo, TYPE_RELATION_ID, convert_type_priv_string)
}

// ===========================================================================
// has_parameter_privilege family — C: acl.c
// ===========================================================================

/// `has_parameter_privilege_name_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_parameter_privilege_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let username = fmgr::pg_getarg_name::call(fcinfo, 0);
    let parameter = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let r#priv = convert_parameter_priv_string(&fmgr::pg_getarg_text_pp::call(fcinfo, 2)?)?;
    let roleid = crate::role_membership::get_role_oid_or_public(&username)?;

    let result = has_param_priv_byname(roleid, &parameter, r#priv)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, result))
}

/// `has_parameter_privilege_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_parameter_privilege_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let parameter = fmgr::pg_getarg_text_pp::call(fcinfo, 0)?;
    let r#priv = convert_parameter_priv_string(&fmgr::pg_getarg_text_pp::call(fcinfo, 1)?)?;

    let result = has_param_priv_byname(miscinit::get_user_id::call(), &parameter, r#priv)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, result))
}

/// `has_parameter_privilege_id_name(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_parameter_privilege_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let parameter = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;
    let r#priv = convert_parameter_priv_string(&fmgr::pg_getarg_text_pp::call(fcinfo, 2)?)?;

    let result = has_param_priv_byname(roleid, &parameter, r#priv)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, result))
}

// ===========================================================================
// has_largeobject_privilege family — C: acl.c
// ===========================================================================

/// `has_largeobject_privilege_name_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_largeobject_privilege_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let username = fmgr::pg_getarg_name::call(fcinfo, 0);
    let roleid = crate::role_membership::get_role_oid_or_public(&username)?;
    let lobj_id = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let mode = convert_largeobject_priv_string(&priv_type_text)?;
    let mut is_missing = false;
    let result = has_lo_priv_byid(roleid, lobj_id, mode, &mut is_missing)?;

    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, result))
}

/// `has_largeobject_privilege_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_largeobject_privilege_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let lobj_id = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let roleid = miscinit::get_user_id::call();
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;

    let mode = convert_largeobject_priv_string(&priv_type_text)?;
    let mut is_missing = false;
    let result = has_lo_priv_byid(roleid, lobj_id, mode, &mut is_missing)?;

    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, result))
}

/// `has_largeobject_privilege_id_id(PG_FUNCTION_ARGS)` — SQL privilege check.
pub fn has_largeobject_privilege_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let lobj_id = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let mode = convert_largeobject_priv_string(&priv_type_text)?;
    let mut is_missing = false;
    let result = has_lo_priv_byid(roleid, lobj_id, mode, &mut is_missing)?;

    if is_missing {
        return Ok(fmgr::pg_return_null::call(fcinfo));
    }
    Ok(fmgr::pg_return_bool::call(fcinfo, result))
}

// ===========================================================================
// pg_has_role family — C: acl.c
// ===========================================================================

/// `pg_has_role_name_name(PG_FUNCTION_ARGS)` — SQL role-privilege check.
pub fn pg_has_role_name_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let username = fmgr::pg_getarg_name::call(fcinfo, 0);
    let rolename = fmgr::pg_getarg_name::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = crate::role_membership::get_role_oid(&username, false)?;
    let roleoid = crate::role_membership::get_role_oid(&rolename, false)?;
    let mode = convert_role_priv_string(&priv_type_text)?;

    let aclresult = pg_role_aclcheck(roleoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `pg_has_role_name(PG_FUNCTION_ARGS)` — SQL role-privilege check.
pub fn pg_has_role_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let rolename = fmgr::pg_getarg_name::call(fcinfo, 0);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;

    let roleid = miscinit::get_user_id::call();
    let roleoid = crate::role_membership::get_role_oid(&rolename, false)?;
    let mode = convert_role_priv_string(&priv_type_text)?;

    let aclresult = pg_role_aclcheck(roleoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `pg_has_role_name_id(PG_FUNCTION_ARGS)` — SQL role-privilege check.
pub fn pg_has_role_name_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let username = fmgr::pg_getarg_name::call(fcinfo, 0);
    let roleoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleid = crate::role_membership::get_role_oid(&username, false)?;
    let mode = convert_role_priv_string(&priv_type_text)?;

    let aclresult = pg_role_aclcheck(roleoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `pg_has_role_id(PG_FUNCTION_ARGS)` — SQL role-privilege check.
pub fn pg_has_role_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let roleoid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 1)?;

    let roleid = miscinit::get_user_id::call();
    let mode = convert_role_priv_string(&priv_type_text)?;

    let aclresult = pg_role_aclcheck(roleoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `pg_has_role_id_name(PG_FUNCTION_ARGS)` — SQL role-privilege check.
pub fn pg_has_role_id_name(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let rolename = fmgr::pg_getarg_name::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let roleoid = crate::role_membership::get_role_oid(&rolename, false)?;
    let mode = convert_role_priv_string(&priv_type_text)?;

    let aclresult = pg_role_aclcheck(roleoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

/// `pg_has_role_id_id(PG_FUNCTION_ARGS)` — SQL role-privilege check.
pub fn pg_has_role_id_id(fcinfo: FunctionCallInfo) -> PgResult<Datum> {
    let roleid = fmgr::pg_getarg_oid::call(fcinfo, 0);
    let roleoid = fmgr::pg_getarg_oid::call(fcinfo, 1);
    let priv_type_text = fmgr::pg_getarg_text_pp::call(fcinfo, 2)?;

    let mode = convert_role_priv_string(&priv_type_text)?;

    let aclresult = pg_role_aclcheck(roleoid, roleid, mode)?;
    Ok(fmgr::pg_return_bool::call(fcinfo, aclresult == ACLCHECK_OK))
}

// ===========================================================================
// convert_any_priv_string + per-object convert_*_priv_string (acl.c)
// ===========================================================================

/// `convert_any_priv_string(priv_type_text, privileges)` — split a
/// comma-separated privilege string and OR together the matching
/// [`AclMode`] bits; errors on an unrecognized keyword.
pub fn convert_any_priv_string(priv_type_text: &Bytea, privileges: &[PrivMap]) -> PgResult<AclMode> {
    // C: priv_type = text_to_cstring(priv_type_text). The payload bytes are
    // the cstring contents (no embedded NUL in a SQL text value).
    let priv_type = priv_type_text.data();
    let mut result: AclMode = 0;

    // C iterates `for (chunk = priv_type; chunk; chunk = next_chunk)`,
    // splitting on commas. `next_chunk == NULL` ends the loop.
    let mut chunk_start: Option<usize> = Some(0);
    while let Some(start) = chunk_start {
        // Split string at commas.
        let (chunk_end, next_start) = match priv_type[start..].iter().position(|&c| c == b',') {
            Some(off) => (start + off, Some(start + off + 1)),
            None => (priv_type.len(), None),
        };
        let mut cs = start;
        let mut ce = chunk_end;

        // Drop leading whitespace in this chunk.
        while cs < ce && is_space(priv_type[cs]) {
            cs += 1;
        }
        // Drop trailing whitespace in this chunk.
        while ce > cs && is_space(priv_type[ce - 1]) {
            ce -= 1;
        }
        let chunk = &priv_type[cs..ce];

        // Match to the privileges list.
        let mut matched = false;
        for this_priv in privileges {
            if pg_strcasecmp(this_priv.name.as_bytes(), chunk) == 0 {
                result |= this_priv.value;
                matched = true;
                break;
            }
        }
        if !matched {
            return Err(PgError::error(alloc::format!(
                "unrecognized privilege type: \"{}\"",
                alloc::string::String::from_utf8_lossy(chunk)
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }

        chunk_start = next_start;
    }

    Ok(result)
}

/// C `isspace((unsigned char) c)` in the default C locale, as
/// [`convert_any_priv_string`] uses it.
#[inline]
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// `convert_table_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_table_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let table_priv_map = [
        PrivMap { name: "SELECT", value: ACL_SELECT },
        PrivMap { name: "SELECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_SELECT) },
        PrivMap { name: "INSERT", value: ACL_INSERT },
        PrivMap { name: "INSERT WITH GRANT OPTION", value: acl_grant_option_for(ACL_INSERT) },
        PrivMap { name: "UPDATE", value: ACL_UPDATE },
        PrivMap { name: "UPDATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_UPDATE) },
        PrivMap { name: "DELETE", value: ACL_DELETE },
        PrivMap { name: "DELETE WITH GRANT OPTION", value: acl_grant_option_for(ACL_DELETE) },
        PrivMap { name: "TRUNCATE", value: ACL_TRUNCATE },
        PrivMap { name: "TRUNCATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_TRUNCATE) },
        PrivMap { name: "REFERENCES", value: ACL_REFERENCES },
        PrivMap { name: "REFERENCES WITH GRANT OPTION", value: acl_grant_option_for(ACL_REFERENCES) },
        PrivMap { name: "TRIGGER", value: ACL_TRIGGER },
        PrivMap { name: "TRIGGER WITH GRANT OPTION", value: acl_grant_option_for(ACL_TRIGGER) },
        PrivMap { name: "MAINTAIN", value: ACL_MAINTAIN },
        PrivMap { name: "MAINTAIN WITH GRANT OPTION", value: acl_grant_option_for(ACL_MAINTAIN) },
    ];
    convert_any_priv_string(priv_type_text, &table_priv_map)
}

/// `convert_sequence_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_sequence_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let sequence_priv_map = [
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
        PrivMap { name: "SELECT", value: ACL_SELECT },
        PrivMap { name: "SELECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_SELECT) },
        PrivMap { name: "UPDATE", value: ACL_UPDATE },
        PrivMap { name: "UPDATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_UPDATE) },
    ];
    convert_any_priv_string(priv_type_text, &sequence_priv_map)
}

/// `convert_column_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_column_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let column_priv_map = [
        PrivMap { name: "SELECT", value: ACL_SELECT },
        PrivMap { name: "SELECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_SELECT) },
        PrivMap { name: "INSERT", value: ACL_INSERT },
        PrivMap { name: "INSERT WITH GRANT OPTION", value: acl_grant_option_for(ACL_INSERT) },
        PrivMap { name: "UPDATE", value: ACL_UPDATE },
        PrivMap { name: "UPDATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_UPDATE) },
        PrivMap { name: "REFERENCES", value: ACL_REFERENCES },
        PrivMap { name: "REFERENCES WITH GRANT OPTION", value: acl_grant_option_for(ACL_REFERENCES) },
    ];
    convert_any_priv_string(priv_type_text, &column_priv_map)
}

/// `convert_database_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_database_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let database_priv_map = [
        PrivMap { name: "CREATE", value: ACL_CREATE },
        PrivMap { name: "CREATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_CREATE) },
        PrivMap { name: "TEMPORARY", value: ACL_CREATE_TEMP },
        PrivMap { name: "TEMPORARY WITH GRANT OPTION", value: acl_grant_option_for(ACL_CREATE_TEMP) },
        PrivMap { name: "TEMP", value: ACL_CREATE_TEMP },
        PrivMap { name: "TEMP WITH GRANT OPTION", value: acl_grant_option_for(ACL_CREATE_TEMP) },
        PrivMap { name: "CONNECT", value: ACL_CONNECT },
        PrivMap { name: "CONNECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_CONNECT) },
    ];
    convert_any_priv_string(priv_type_text, &database_priv_map)
}

/// `convert_foreign_data_wrapper_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_foreign_data_wrapper_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let fdw_priv_map = [
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
    ];
    convert_any_priv_string(priv_type_text, &fdw_priv_map)
}

/// `convert_function_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_function_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let function_priv_map = [
        PrivMap { name: "EXECUTE", value: ACL_EXECUTE },
        PrivMap { name: "EXECUTE WITH GRANT OPTION", value: acl_grant_option_for(ACL_EXECUTE) },
    ];
    convert_any_priv_string(priv_type_text, &function_priv_map)
}

/// `convert_language_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_language_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let language_priv_map = [
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
    ];
    convert_any_priv_string(priv_type_text, &language_priv_map)
}

/// `convert_schema_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_schema_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let schema_priv_map = [
        PrivMap { name: "CREATE", value: ACL_CREATE },
        PrivMap { name: "CREATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_CREATE) },
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
    ];
    convert_any_priv_string(priv_type_text, &schema_priv_map)
}

/// `convert_server_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_server_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let server_priv_map = [
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
    ];
    convert_any_priv_string(priv_type_text, &server_priv_map)
}

/// `convert_tablespace_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_tablespace_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let tablespace_priv_map = [
        PrivMap { name: "CREATE", value: ACL_CREATE },
        PrivMap { name: "CREATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_CREATE) },
    ];
    convert_any_priv_string(priv_type_text, &tablespace_priv_map)
}

/// `convert_type_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_type_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let type_priv_map = [
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
    ];
    convert_any_priv_string(priv_type_text, &type_priv_map)
}

/// `convert_parameter_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_parameter_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let parameter_priv_map = [
        PrivMap { name: "SET", value: ACL_SET },
        PrivMap { name: "SET WITH GRANT OPTION", value: acl_grant_option_for(ACL_SET) },
        PrivMap { name: "ALTER SYSTEM", value: ACL_ALTER_SYSTEM },
        PrivMap { name: "ALTER SYSTEM WITH GRANT OPTION", value: acl_grant_option_for(ACL_ALTER_SYSTEM) },
    ];
    convert_any_priv_string(priv_type_text, &parameter_priv_map)
}

/// `convert_largeobject_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_largeobject_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let largeobject_priv_map = [
        PrivMap { name: "SELECT", value: ACL_SELECT },
        PrivMap { name: "SELECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_SELECT) },
        PrivMap { name: "UPDATE", value: ACL_UPDATE },
        PrivMap { name: "UPDATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_UPDATE) },
    ];
    convert_any_priv_string(priv_type_text, &largeobject_priv_map)
}

/// `convert_role_priv_string(priv_type_text)` — parse the `pg_has_role`
/// privilege string. C cheats and uses USAGE for has_privs_of_role, ACL_CREATE
/// for MEMBER, ACL_SET for SET, and the grant-option-of-ACL_CREATE bit for the
/// WITH GRANT/ADMIN OPTION variants. Shared only with `pg_role_aclcheck`.
pub fn convert_role_priv_string(priv_type_text: &Bytea) -> PgResult<AclMode> {
    let role_priv_map = [
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "MEMBER", value: ACL_CREATE },
        PrivMap { name: "SET", value: ACL_SET },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_CREATE) },
        PrivMap { name: "USAGE WITH ADMIN OPTION", value: acl_grant_option_for(ACL_CREATE) },
        PrivMap { name: "MEMBER WITH GRANT OPTION", value: acl_grant_option_for(ACL_CREATE) },
        PrivMap { name: "MEMBER WITH ADMIN OPTION", value: acl_grant_option_for(ACL_CREATE) },
        PrivMap { name: "SET WITH GRANT OPTION", value: acl_grant_option_for(ACL_CREATE) },
        PrivMap { name: "SET WITH ADMIN OPTION", value: acl_grant_option_for(ACL_CREATE) },
    ];
    convert_any_priv_string(priv_type_text, &role_priv_map)
}

// ===========================================================================
// convert_*_name support routines (acl.c)
// ===========================================================================

/// `convert_table_name(name)` — resolve an object name `text` to its OID.
///
/// C: `RangeVarGetRelid(makeRangeVarFromNameList(textToQualifiedNameList(name)),
/// NoLock, false)`.
pub fn convert_table_name(mcx: Mcx<'_>, name: &Bytea) -> PgResult<Oid> {
    let parts = varlena::text_to_qualified_name_list::call(mcx, name.data())?;
    let part_refs: alloc::vec::Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
    let relrv = namespace::make_range_var_from_name_list::call(&part_refs)?;

    // We might not even have permissions on this relation; don't lock it.
    namespace::range_var_get_relid::call(mcx, &relrv, NoLock, false)
}

/// `convert_database_name(name)` — resolve an object name `text` to its OID.
pub fn convert_database_name(mcx: Mcx<'_>, name: &Bytea) -> PgResult<Oid> {
    let _ = mcx;
    let dbname = name_cstr(name);
    dbcommands::get_database_oid::call(&dbname, false)
}

/// `convert_foreign_data_wrapper_name(name)` — resolve an object name `text` to its OID.
pub fn convert_foreign_data_wrapper_name(mcx: Mcx<'_>, name: &Bytea) -> PgResult<Oid> {
    let _ = mcx;
    let fdwstr = name_cstr(name);
    foreign::get_foreign_data_wrapper_oid::call(&fdwstr, false)
}

/// `convert_function_name(name)` — resolve an object name `text` to its OID.
///
/// C: `DatumGetObjectId(DirectFunctionCall1(regprocedurein,
/// CStringGetDatum(funcname)))`, then an explicit `OidIsValid` check.
pub fn convert_function_name(mcx: Mcx<'_>, name: &Bytea) -> PgResult<Oid> {
    let _ = mcx;
    let funcname = name_cstr(name);
    let oid = regproc::regprocedurein::call(&funcname)?;
    if !OidIsValid(oid) {
        return Err(PgError::error(alloc::format!("function \"{funcname}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }
    Ok(oid)
}

/// `convert_language_name(name)` — resolve an object name `text` to its OID.
pub fn convert_language_name(mcx: Mcx<'_>, name: &Bytea) -> PgResult<Oid> {
    let _ = mcx;
    let langname = name_cstr(name);
    proclang::get_language_oid::call(&langname, false)
}

/// `convert_schema_name(name)` — resolve an object name `text` to its OID.
pub fn convert_schema_name(mcx: Mcx<'_>, name: &Bytea) -> PgResult<Oid> {
    let _ = mcx;
    let nspname = name_cstr(name);
    namespace::get_namespace_oid::call(&nspname, false)
}

/// `convert_server_name(name)` — resolve an object name `text` to its OID.
pub fn convert_server_name(mcx: Mcx<'_>, name: &Bytea) -> PgResult<Oid> {
    let _ = mcx;
    let serverstr = name_cstr(name);
    foreign::get_foreign_server_oid::call(&serverstr, false)
}

/// `convert_tablespace_name(name)` — resolve an object name `text` to its OID.
pub fn convert_tablespace_name(mcx: Mcx<'_>, name: &Bytea) -> PgResult<Oid> {
    let _ = mcx;
    let spcname = name_cstr(name);
    tablespace::get_tablespace_oid::call(&spcname, false)
}

/// `convert_type_name(name)` — resolve an object name `text` to its OID.
///
/// C: `DatumGetObjectId(DirectFunctionCall1(regtypein,
/// CStringGetDatum(typname)))`, then an explicit `OidIsValid` check.
pub fn convert_type_name(mcx: Mcx<'_>, name: &Bytea) -> PgResult<Oid> {
    let _ = mcx;
    let typname = name_cstr(name);
    let oid = regproc::regtypein::call(&typname)?;
    if !OidIsValid(oid) {
        return Err(PgError::error(alloc::format!("type \"{typname}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    Ok(oid)
}

/// `convert_column_name(tableoid, column)` — resolve a column name `text` to
/// its attribute number within `tableoid`. Returns `InvalidAttrNumber` for the
/// cases where the caller should return SQL NULL instead of failing.
pub fn convert_column_name(mcx: Mcx<'_>, tableoid: Oid, column: &Bytea) -> PgResult<AttrNumber> {
    let colname = name_cstr(column);

    // We don't use get_attnum() here because it reports that dropped columns
    // don't exist; we must treat dropped columns differently from nonexistent
    // ones. C: SearchSysCache2(ATTNAME, ...).
    match syscache::search_attname_attnum::call(tableoid, &colname)? {
        Some((attnum, attisdropped)) => {
            // We want to return NULL for dropped columns.
            if attisdropped {
                Ok(InvalidAttrNumber)
            } else {
                Ok(attnum)
            }
        }
        None => {
            // If the table OID is bogus, or just dropped, get_rel_name returns
            // NULL; then has_column_privilege should return NULL too, so just
            // return InvalidAttrNumber. Otherwise (table exists, column does
            // not) throw an error.
            let tablename = lsyscache::get_rel_name::call(mcx, tableoid)?;
            if let Some(tablename) = tablename {
                return Err(PgError::error(alloc::format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    colname,
                    tablename.as_str()
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_COLUMN));
            }
            Ok(InvalidAttrNumber)
        }
    }
}

/// `column_privilege_check(tableoid, attnum, roleid, mask)` — shared worker
/// behind the column-privilege SQL functions. Returns C's tri-state result:
/// `1` (has priv), `0` (lacks priv), or `-1` (object/column not found → SQL
/// NULL).
pub fn column_privilege_check(
    tableoid: Oid,
    attnum: AttrNumber,
    roleid: Oid,
    mask: AclMode,
) -> PgResult<i32> {
    // If convert_column_name failed, we can just return -1 immediately.
    if attnum == InvalidAttrNumber {
        return Ok(-1);
    }

    // Check for column-level privileges first. This serves in part as a check
    // on whether the column even exists, so do it before the table check.
    let (aclresult, is_missing) =
        aclchk::pg_attribute_aclcheck_ext::call(tableoid, attnum, roleid, mask)?;
    if aclresult == ACLCHECK_OK {
        return Ok(1);
    } else if is_missing {
        return Ok(-1);
    }

    // Next check if we have the privilege at the table level.
    let (aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(tableoid, roleid, mask)?;
    if aclresult == ACLCHECK_OK {
        Ok(1)
    } else if is_missing {
        Ok(-1)
    } else {
        Ok(0)
    }
}

/// `has_param_priv_byname(roleid, parameter, r#priv)` — configuration-parameter
/// privilege check by parameter name.
pub fn has_param_priv_byname(roleid: Oid, parameter: &Bytea, r#priv: AclMode) -> PgResult<bool> {
    let paramstr = name_cstr(parameter);
    Ok(aclchk::pg_parameter_aclcheck::call(&paramstr, roleid, r#priv)? == ACLCHECK_OK)
}

/// `has_lo_priv_byid(roleid, lobjId, r#priv, is_missing)` — large-object
/// privilege check by OID; sets `is_missing` when the object does not exist.
pub fn has_lo_priv_byid(
    roleid: Oid,
    lobj_id: Oid,
    r#priv: AclMode,
    is_missing: &mut bool,
) -> PgResult<bool> {
    // C: snapshot = (priv & ACL_UPDATE) ? NULL : GetActiveSnapshot().
    let snapshot = if r#priv & ACL_UPDATE != 0 {
        None
    } else {
        snapmgr::get_active_snapshot::call()?
    };

    if !pg_largeobject::large_object_exists_with_snapshot::call(lobj_id, snapshot.clone())? {
        // C: Assert(is_missing != NULL); *is_missing = true; return false.
        *is_missing = true;
        return Ok(false);
    }

    if backend_utils_misc_guc_tables::vars::lo_compat_privileges.read() {
        return Ok(true);
    }

    let aclresult =
        aclchk::pg_largeobject_aclcheck_snapshot::call(lobj_id, roleid, r#priv, snapshot)?;
    Ok(aclresult == ACLCHECK_OK)
}

/// `pg_role_aclcheck(role_oid, roleid, mode)` — quick-and-dirty support for
/// `pg_has_role`. The `mode` bits use the `convert_role_priv_string`
/// convention (ACL_CREATE == MEMBER, grant-option-of-ACL_CREATE == ADMIN).
pub fn pg_role_aclcheck(role_oid: Oid, roleid: Oid, mode: AclMode) -> PgResult<AclResult> {
    if mode & acl_grant_option_for(ACL_CREATE) != 0
        && crate::role_membership::is_admin_of_role(roleid, role_oid)?
    {
        return Ok(ACLCHECK_OK);
    }
    if mode & ACL_CREATE != 0 && crate::role_membership::is_member_of_role(roleid, role_oid)? {
        return Ok(ACLCHECK_OK);
    }
    if mode & ACL_USAGE != 0 && crate::role_membership::has_privs_of_role(roleid, role_oid)? {
        return Ok(ACLCHECK_OK);
    }
    if mode & ACL_SET != 0 && crate::role_membership::member_can_set_role(roleid, role_oid)? {
        return Ok(ACLCHECK_OK);
    }
    Ok(ACLCHECK_NO_PRIV)
}
