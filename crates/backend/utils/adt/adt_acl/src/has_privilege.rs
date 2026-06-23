//! The `has_*_privilege` SQL families, `pg_has_role`, and their helpers
//! (`utils/adt/acl.c`).
//!
//! Each object class has a fan of `*_name_name` / `*_name` / `*_name_id` /
//! `*_id` / `*_id_name` / `*_id_id` SQL entrypoints plus a `convert_<obj>_name`
//! and a `convert_<obj>_priv_string` helper. The shared internal checks
//! (`column_privilege_check`, `has_param_priv_byname`, `has_lo_priv_byid`,
//! `pg_role_aclcheck`) live here too.
//!
//! These are written as pure value cores: each takes its already-decoded
//! arguments (object name as `text` payload bytes, role name as a `&str`, OIDs,
//! attnums) and returns `PgResult<Option<bool>>`, where `Ok(None)` is the SQL
//! NULL result (C: `PG_RETURN_NULL()` on a missing object). The `PG_GETARG_*` /
//! `PG_RETURN_*` marshaling lives in [`crate::fmgr_builtins`].

use crate::PrivMap;
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
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_storage::lock::NoLock;
use types_tuple::access::RELKIND_SEQUENCE;

use pgstrcasecmp::pg_strcasecmp;

use aclchk_seams as aclchk;
use namespace_seams as namespace;
use pg_largeobject_seams as pg_largeobject;
use dbcommands_seams as dbcommands;
use proclang_seams as proclang;
use tablespace_seams as tablespace;
use foreign_seams as foreign;
use regproc_seams as regproc;
use varlena_seams as varlena;
use lsyscache_seams as lsyscache;
use syscache_seams as syscache;
use snapmgr_seams as snapmgr;

// ===========================================================================
// has_table_privilege family — C: acl.c
// ===========================================================================

/// `has_table_privilege` core: role known, table OID known, privilege string.
/// `is_ext` selects the by-OID (`_ext`, may be missing → NULL) path.
fn has_table_priv_byid(roleid: Oid, tableoid: Oid, priv_bytes: &[u8]) -> PgResult<Option<bool>> {
    let mode = convert_table_priv_string(priv_bytes)?;
    let (aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if is_missing {
        return Ok(None);
    }
    Ok(Some(aclresult == ACLCHECK_OK))
}

/// `has_table_privilege` core: role known, table named (always non-missing).
fn has_table_priv_byname(
    mcx: Mcx<'_>,
    roleid: Oid,
    tablename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let tableoid = convert_table_name(mcx, tablename)?;
    let mode = convert_table_priv_string(priv_bytes)?;
    let aclresult = aclchk::pg_class_aclcheck::call(tableoid, roleid, mode)?;
    Ok(Some(aclresult == ACLCHECK_OK))
}

/// `has_table_privilege_name_name` core.
pub fn has_table_privilege_name_name(
    mcx: Mcx<'_>,
    rolename: &str,
    tablename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid_or_public(rolename)?;
    has_table_priv_byname(mcx, roleid, tablename, priv_bytes)
}

/// `has_table_privilege_name` core (current user).
pub fn has_table_privilege_name(
    mcx: Mcx<'_>,
    roleid: Oid,
    tablename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_table_priv_byname(mcx, roleid, tablename, priv_bytes)
}

/// `has_table_privilege_name_id` core.
pub fn has_table_privilege_name_id(
    rolename: &str,
    tableoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid_or_public(rolename)?;
    has_table_priv_byid(roleid, tableoid, priv_bytes)
}

/// `has_table_privilege_id` core (current user, by OID).
pub fn has_table_privilege_id(
    roleid: Oid,
    tableoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_table_priv_byid(roleid, tableoid, priv_bytes)
}

/// `has_table_privilege_id_name` core.
pub fn has_table_privilege_id_name(
    mcx: Mcx<'_>,
    roleid: Oid,
    tablename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_table_priv_byname(mcx, roleid, tablename, priv_bytes)
}

/// `has_table_privilege_id_id` core.
pub fn has_table_privilege_id_id(
    roleid: Oid,
    tableoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_table_priv_byid(roleid, tableoid, priv_bytes)
}

// ===========================================================================
// has_sequence_privilege family — C: acl.c
// ===========================================================================

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

/// Lossy UTF-8 view of a `text` payload for an error message.
fn text_lossy(b: &[u8]) -> alloc::string::String {
    alloc::string::String::from_utf8_lossy(b).into_owned()
}

/// `has_sequence_privilege` core: sequence named.
fn has_sequence_priv_byname(
    mcx: Mcx<'_>,
    roleid: Oid,
    sequencename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let mode = convert_sequence_priv_string(priv_bytes)?;
    let sequenceoid = convert_table_name(mcx, sequencename)?;
    if lsyscache::get_rel_relkind::call(sequenceoid)? != RELKIND_SEQUENCE {
        return Err(not_a_sequence(&text_lossy(sequencename)));
    }
    let aclresult = aclchk::pg_class_aclcheck::call(sequenceoid, roleid, mode)?;
    Ok(Some(aclresult == ACLCHECK_OK))
}

/// `has_sequence_privilege` core: sequence OID known.
fn has_sequence_priv_byid(
    mcx: Mcx<'_>,
    roleid: Oid,
    sequenceoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let mode = convert_sequence_priv_string(priv_bytes)?;
    let relkind = lsyscache::get_rel_relkind::call(sequenceoid)?;
    if relkind == b'\0' {
        return Ok(None);
    } else if relkind != RELKIND_SEQUENCE {
        let relname = lsyscache::get_rel_name::call(mcx, sequenceoid)?;
        return Err(not_a_sequence(rel_name_str(&relname)));
    }
    let (aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(sequenceoid, roleid, mode)?;
    if is_missing {
        return Ok(None);
    }
    Ok(Some(aclresult == ACLCHECK_OK))
}

/// `has_sequence_privilege_name_name` core.
pub fn has_sequence_privilege_name_name(
    mcx: Mcx<'_>,
    rolename: &str,
    sequencename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid_or_public(rolename)?;
    has_sequence_priv_byname(mcx, roleid, sequencename, priv_bytes)
}
/// `has_sequence_privilege_name` core.
pub fn has_sequence_privilege_name(
    mcx: Mcx<'_>,
    roleid: Oid,
    sequencename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_sequence_priv_byname(mcx, roleid, sequencename, priv_bytes)
}
/// `has_sequence_privilege_name_id` core.
pub fn has_sequence_privilege_name_id(
    mcx: Mcx<'_>,
    username: &str,
    sequenceoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid_or_public(username)?;
    has_sequence_priv_byid(mcx, roleid, sequenceoid, priv_bytes)
}
/// `has_sequence_privilege_id` core.
pub fn has_sequence_privilege_id(
    mcx: Mcx<'_>,
    roleid: Oid,
    sequenceoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_sequence_priv_byid(mcx, roleid, sequenceoid, priv_bytes)
}
/// `has_sequence_privilege_id_name` core.
pub fn has_sequence_privilege_id_name(
    mcx: Mcx<'_>,
    roleid: Oid,
    sequencename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_sequence_priv_byname(mcx, roleid, sequencename, priv_bytes)
}
/// `has_sequence_privilege_id_id` core.
pub fn has_sequence_privilege_id_id(
    mcx: Mcx<'_>,
    roleid: Oid,
    sequenceoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_sequence_priv_byid(mcx, roleid, sequenceoid, priv_bytes)
}

// ===========================================================================
// has_any_column_privilege family — C: acl.c
// ===========================================================================

/// `has_any_column_privilege` core: table named (non-missing).
fn has_any_column_priv_byname(
    mcx: Mcx<'_>,
    roleid: Oid,
    tablename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let tableoid = convert_table_name(mcx, tablename)?;
    let mode = convert_column_priv_string(priv_bytes)?;
    let mut aclresult = aclchk::pg_class_aclcheck::call(tableoid, roleid, mode)?;
    if aclresult != ACLCHECK_OK {
        aclresult = aclchk::pg_attribute_aclcheck_all::call(tableoid, roleid, mode, ACLMASK_ANY)?;
    }
    Ok(Some(aclresult == ACLCHECK_OK))
}

/// `has_any_column_privilege` core: table OID known (may be missing → NULL).
fn has_any_column_priv_byid(
    roleid: Oid,
    tableoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let mode = convert_column_priv_string(priv_bytes)?;
    let (mut aclresult, is_missing) = aclchk::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if aclresult != ACLCHECK_OK {
        if is_missing {
            return Ok(None);
        }
        let (r, m) =
            aclchk::pg_attribute_aclcheck_all_ext::call(tableoid, roleid, mode, ACLMASK_ANY)?;
        aclresult = r;
        if m {
            return Ok(None);
        }
    }
    Ok(Some(aclresult == ACLCHECK_OK))
}

/// `has_any_column_privilege_name_name` core.
pub fn has_any_column_privilege_name_name(
    mcx: Mcx<'_>,
    rolename: &str,
    tablename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid_or_public(rolename)?;
    has_any_column_priv_byname(mcx, roleid, tablename, priv_bytes)
}
/// `has_any_column_privilege_name` core.
pub fn has_any_column_privilege_name(
    mcx: Mcx<'_>,
    roleid: Oid,
    tablename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_any_column_priv_byname(mcx, roleid, tablename, priv_bytes)
}
/// `has_any_column_privilege_name_id` core.
pub fn has_any_column_privilege_name_id(
    username: &str,
    tableoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid_or_public(username)?;
    has_any_column_priv_byid(roleid, tableoid, priv_bytes)
}
/// `has_any_column_privilege_id` core.
pub fn has_any_column_privilege_id(
    roleid: Oid,
    tableoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_any_column_priv_byid(roleid, tableoid, priv_bytes)
}
/// `has_any_column_privilege_id_name` core.
pub fn has_any_column_privilege_id_name(
    mcx: Mcx<'_>,
    roleid: Oid,
    tablename: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_any_column_priv_byname(mcx, roleid, tablename, priv_bytes)
}
/// `has_any_column_privilege_id_id` core.
pub fn has_any_column_privilege_id_id(
    roleid: Oid,
    tableoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_any_column_priv_byid(roleid, tableoid, priv_bytes)
}

// ===========================================================================
// has_column_privilege family — C: acl.c
// ===========================================================================

/// Encode a `column_privilege_check` tri-state result as the SQL value
/// (`-1` → NULL, else the boolean).
fn column_priv_value(privresult: i32) -> Option<bool> {
    if privresult < 0 {
        None
    } else {
        Some(privresult != 0)
    }
}

/// `has_column_privilege_name_name_name` core.
pub fn has_column_privilege_name_name_name(
    mcx: Mcx<'_>,
    rolename: &str,
    tablename: &[u8],
    column: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid_or_public(rolename)?;
    let tableoid = convert_table_name(mcx, tablename)?;
    let colattnum = convert_column_name(mcx, tableoid, column)?;
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

/// `has_column_privilege_name_name_attnum` core.
pub fn has_column_privilege_name_name_attnum(
    mcx: Mcx<'_>,
    rolename: &str,
    tablename: &[u8],
    colattnum: AttrNumber,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid_or_public(rolename)?;
    let tableoid = convert_table_name(mcx, tablename)?;
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

/// `has_column_privilege_name_id_name` core.
pub fn has_column_privilege_name_id_name(
    mcx: Mcx<'_>,
    username: &str,
    tableoid: Oid,
    column: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid_or_public(username)?;
    let colattnum = convert_column_name(mcx, tableoid, column)?;
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

/// `has_column_privilege_name_id_attnum` core.
pub fn has_column_privilege_name_id_attnum(
    username: &str,
    tableoid: Oid,
    colattnum: AttrNumber,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid_or_public(username)?;
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

/// `has_column_privilege_id_name_name` core.
pub fn has_column_privilege_id_name_name(
    mcx: Mcx<'_>,
    roleid: Oid,
    tablename: &[u8],
    column: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let tableoid = convert_table_name(mcx, tablename)?;
    let colattnum = convert_column_name(mcx, tableoid, column)?;
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

/// `has_column_privilege_id_name_attnum` core.
pub fn has_column_privilege_id_name_attnum(
    mcx: Mcx<'_>,
    roleid: Oid,
    tablename: &[u8],
    colattnum: AttrNumber,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let tableoid = convert_table_name(mcx, tablename)?;
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

/// `has_column_privilege_id_id_name` core.
pub fn has_column_privilege_id_id_name(
    mcx: Mcx<'_>,
    roleid: Oid,
    tableoid: Oid,
    column: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let colattnum = convert_column_name(mcx, tableoid, column)?;
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

/// `has_column_privilege_id_id_attnum` core.
pub fn has_column_privilege_id_id_attnum(
    roleid: Oid,
    tableoid: Oid,
    colattnum: AttrNumber,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

/// `has_column_privilege_name_name` core (current user).
pub fn has_column_privilege_name_name(
    mcx: Mcx<'_>,
    roleid: Oid,
    tablename: &[u8],
    column: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let tableoid = convert_table_name(mcx, tablename)?;
    let colattnum = convert_column_name(mcx, tableoid, column)?;
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

/// `has_column_privilege_name_attnum` core (current user).
pub fn has_column_privilege_name_attnum(
    mcx: Mcx<'_>,
    roleid: Oid,
    tablename: &[u8],
    colattnum: AttrNumber,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let tableoid = convert_table_name(mcx, tablename)?;
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

/// `has_column_privilege_id_name` core (current user).
pub fn has_column_privilege_id_name(
    mcx: Mcx<'_>,
    roleid: Oid,
    tableoid: Oid,
    column: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let colattnum = convert_column_name(mcx, tableoid, column)?;
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

/// `has_column_privilege_id_attnum` core (current user).
pub fn has_column_privilege_id_attnum(
    roleid: Oid,
    tableoid: Oid,
    colattnum: AttrNumber,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let mode = convert_column_priv_string(priv_bytes)?;
    Ok(column_priv_value(column_privilege_check(tableoid, colattnum, roleid, mode)?))
}

// ===========================================================================
// object-class families reached through object_aclcheck (acl.c)
// ===========================================================================

/// `object_aclcheck` driver: object named (non-missing).
fn object_priv_byname(
    mcx: Mcx<'_>,
    classid: Oid,
    roleid: Oid,
    objname: &[u8],
    priv_bytes: &[u8],
    convert_name: impl Fn(Mcx<'_>, &[u8]) -> PgResult<Oid>,
    convert_priv: impl Fn(&[u8]) -> PgResult<AclMode>,
) -> PgResult<Option<bool>> {
    let objoid = convert_name(mcx, objname)?;
    let mode = convert_priv(priv_bytes)?;
    let aclresult = aclchk::object_aclcheck::call(classid, objoid, roleid, mode)?;
    Ok(Some(aclresult == ACLCHECK_OK))
}

/// `object_aclcheck` driver: object OID known (may be missing → NULL).
fn object_priv_byid(
    classid: Oid,
    roleid: Oid,
    objoid: Oid,
    priv_bytes: &[u8],
    convert_priv: impl Fn(&[u8]) -> PgResult<AclMode>,
) -> PgResult<Option<bool>> {
    let mode = convert_priv(priv_bytes)?;
    let (aclresult, is_missing) = aclchk::object_aclcheck_ext::call(classid, objoid, roleid, mode)?;
    if is_missing {
        return Ok(None);
    }
    Ok(Some(aclresult == ACLCHECK_OK))
}

/// Build the six-variant family of object-privilege cores for one object class.
macro_rules! object_priv_family {
    ($class:expr, $cvt_name:path, $cvt_priv:path,
     $nn:ident, $n:ident, $ni:ident, $i:ident, $in_:ident, $ii:ident) => {
        /// `*_name_name` core.
        pub fn $nn(
            mcx: Mcx<'_>,
            username: &str,
            objname: &[u8],
            priv_bytes: &[u8],
        ) -> PgResult<Option<bool>> {
            let roleid = crate::role_membership::get_role_oid_or_public(username)?;
            object_priv_byname(mcx, $class, roleid, objname, priv_bytes, $cvt_name, $cvt_priv)
        }
        /// `*_name` core (current user).
        pub fn $n(
            mcx: Mcx<'_>,
            roleid: Oid,
            objname: &[u8],
            priv_bytes: &[u8],
        ) -> PgResult<Option<bool>> {
            object_priv_byname(mcx, $class, roleid, objname, priv_bytes, $cvt_name, $cvt_priv)
        }
        /// `*_name_id` core.
        pub fn $ni(
            username: &str,
            objoid: Oid,
            priv_bytes: &[u8],
        ) -> PgResult<Option<bool>> {
            let roleid = crate::role_membership::get_role_oid_or_public(username)?;
            object_priv_byid($class, roleid, objoid, priv_bytes, $cvt_priv)
        }
        /// `*_id` core (current user, by OID).
        pub fn $i(roleid: Oid, objoid: Oid, priv_bytes: &[u8]) -> PgResult<Option<bool>> {
            object_priv_byid($class, roleid, objoid, priv_bytes, $cvt_priv)
        }
        /// `*_id_name` core.
        pub fn $in_(
            mcx: Mcx<'_>,
            roleid: Oid,
            objname: &[u8],
            priv_bytes: &[u8],
        ) -> PgResult<Option<bool>> {
            object_priv_byname(mcx, $class, roleid, objname, priv_bytes, $cvt_name, $cvt_priv)
        }
        /// `*_id_id` core.
        pub fn $ii(roleid: Oid, objoid: Oid, priv_bytes: &[u8]) -> PgResult<Option<bool>> {
            object_priv_byid($class, roleid, objoid, priv_bytes, $cvt_priv)
        }
    };
}

object_priv_family!(
    DATABASE_RELATION_ID, convert_database_name, convert_database_priv_string,
    has_database_privilege_name_name, has_database_privilege_name, has_database_privilege_name_id,
    has_database_privilege_id, has_database_privilege_id_name, has_database_privilege_id_id
);
object_priv_family!(
    FOREIGN_DATA_WRAPPER_RELATION_ID, convert_foreign_data_wrapper_name,
    convert_foreign_data_wrapper_priv_string,
    has_foreign_data_wrapper_privilege_name_name, has_foreign_data_wrapper_privilege_name,
    has_foreign_data_wrapper_privilege_name_id, has_foreign_data_wrapper_privilege_id,
    has_foreign_data_wrapper_privilege_id_name, has_foreign_data_wrapper_privilege_id_id
);
object_priv_family!(
    PROCEDURE_RELATION_ID, convert_function_name, convert_function_priv_string,
    has_function_privilege_name_name, has_function_privilege_name, has_function_privilege_name_id,
    has_function_privilege_id, has_function_privilege_id_name, has_function_privilege_id_id
);
object_priv_family!(
    LANGUAGE_RELATION_ID, convert_language_name, convert_language_priv_string,
    has_language_privilege_name_name, has_language_privilege_name, has_language_privilege_name_id,
    has_language_privilege_id, has_language_privilege_id_name, has_language_privilege_id_id
);
object_priv_family!(
    NAMESPACE_RELATION_ID, convert_schema_name, convert_schema_priv_string,
    has_schema_privilege_name_name, has_schema_privilege_name, has_schema_privilege_name_id,
    has_schema_privilege_id, has_schema_privilege_id_name, has_schema_privilege_id_id
);
object_priv_family!(
    FOREIGN_SERVER_RELATION_ID, convert_server_name, convert_server_priv_string,
    has_server_privilege_name_name, has_server_privilege_name, has_server_privilege_name_id,
    has_server_privilege_id, has_server_privilege_id_name, has_server_privilege_id_id
);
object_priv_family!(
    TABLE_SPACE_RELATION_ID, convert_tablespace_name, convert_tablespace_priv_string,
    has_tablespace_privilege_name_name, has_tablespace_privilege_name,
    has_tablespace_privilege_name_id, has_tablespace_privilege_id,
    has_tablespace_privilege_id_name, has_tablespace_privilege_id_id
);
object_priv_family!(
    TYPE_RELATION_ID, convert_type_name, convert_type_priv_string,
    has_type_privilege_name_name, has_type_privilege_name, has_type_privilege_name_id,
    has_type_privilege_id, has_type_privilege_id_name, has_type_privilege_id_id
);

// ===========================================================================
// has_parameter_privilege family — C: acl.c
// ===========================================================================

/// `has_parameter_privilege_name_name` core.
pub fn has_parameter_privilege_name_name(
    username: &str,
    parameter: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let r#priv = convert_parameter_priv_string(priv_bytes)?;
    let roleid = crate::role_membership::get_role_oid_or_public(username)?;
    Ok(Some(has_param_priv_byname(roleid, parameter, r#priv)?))
}

/// `has_parameter_privilege_name` core (current user).
pub fn has_parameter_privilege_name(
    roleid: Oid,
    parameter: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let r#priv = convert_parameter_priv_string(priv_bytes)?;
    Ok(Some(has_param_priv_byname(roleid, parameter, r#priv)?))
}

/// `has_parameter_privilege_id_name` core.
pub fn has_parameter_privilege_id_name(
    roleid: Oid,
    parameter: &[u8],
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let r#priv = convert_parameter_priv_string(priv_bytes)?;
    Ok(Some(has_param_priv_byname(roleid, parameter, r#priv)?))
}

// ===========================================================================
// has_largeobject_privilege family — C: acl.c
// ===========================================================================

/// `has_largeobject_privilege_name_id` core.
pub fn has_largeobject_privilege_name_id(
    username: &str,
    lobj_id: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid_or_public(username)?;
    has_lo_priv(roleid, lobj_id, priv_bytes)
}

/// `has_largeobject_privilege_id` core (current user).
pub fn has_largeobject_privilege_id(
    roleid: Oid,
    lobj_id: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_lo_priv(roleid, lobj_id, priv_bytes)
}

/// `has_largeobject_privilege_id_id` core.
pub fn has_largeobject_privilege_id_id(
    roleid: Oid,
    lobj_id: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    has_lo_priv(roleid, lobj_id, priv_bytes)
}

fn has_lo_priv(roleid: Oid, lobj_id: Oid, priv_bytes: &[u8]) -> PgResult<Option<bool>> {
    let mode = convert_largeobject_priv_string(priv_bytes)?;
    let mut is_missing = false;
    let result = has_lo_priv_byid(roleid, lobj_id, mode, &mut is_missing)?;
    if is_missing {
        return Ok(None);
    }
    Ok(Some(result))
}

// ===========================================================================
// pg_has_role family — C: acl.c
// ===========================================================================

/// `pg_has_role_name_name` core.
pub fn pg_has_role_name_name(
    username: &str,
    rolename: &str,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid(username, false)?;
    let roleoid = crate::role_membership::get_role_oid(rolename, false)?;
    let mode = convert_role_priv_string(priv_bytes)?;
    Ok(Some(pg_role_aclcheck(roleoid, roleid, mode)? == ACLCHECK_OK))
}

/// `pg_has_role_name` core (current user).
pub fn pg_has_role_name(roleid: Oid, rolename: &str, priv_bytes: &[u8]) -> PgResult<Option<bool>> {
    let roleoid = crate::role_membership::get_role_oid(rolename, false)?;
    let mode = convert_role_priv_string(priv_bytes)?;
    Ok(Some(pg_role_aclcheck(roleoid, roleid, mode)? == ACLCHECK_OK))
}

/// `pg_has_role_name_id` core.
pub fn pg_has_role_name_id(
    username: &str,
    roleoid: Oid,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleid = crate::role_membership::get_role_oid(username, false)?;
    let mode = convert_role_priv_string(priv_bytes)?;
    Ok(Some(pg_role_aclcheck(roleoid, roleid, mode)? == ACLCHECK_OK))
}

/// `pg_has_role_id` core (current user, by OID).
pub fn pg_has_role_id(roleid: Oid, roleoid: Oid, priv_bytes: &[u8]) -> PgResult<Option<bool>> {
    let mode = convert_role_priv_string(priv_bytes)?;
    Ok(Some(pg_role_aclcheck(roleoid, roleid, mode)? == ACLCHECK_OK))
}

/// `pg_has_role_id_name` core.
pub fn pg_has_role_id_name(
    roleid: Oid,
    rolename: &str,
    priv_bytes: &[u8],
) -> PgResult<Option<bool>> {
    let roleoid = crate::role_membership::get_role_oid(rolename, false)?;
    let mode = convert_role_priv_string(priv_bytes)?;
    Ok(Some(pg_role_aclcheck(roleoid, roleid, mode)? == ACLCHECK_OK))
}

/// `pg_has_role_id_id` core.
pub fn pg_has_role_id_id(roleid: Oid, roleoid: Oid, priv_bytes: &[u8]) -> PgResult<Option<bool>> {
    let mode = convert_role_priv_string(priv_bytes)?;
    Ok(Some(pg_role_aclcheck(roleoid, roleid, mode)? == ACLCHECK_OK))
}

// ===========================================================================
// convert_any_priv_string + per-object convert_*_priv_string (acl.c)
// ===========================================================================

/// `convert_any_priv_string(priv_type_text, privileges)` — split a
/// comma-separated privilege string and OR together the matching
/// [`AclMode`] bits; errors on an unrecognized keyword.
pub fn convert_any_priv_string(priv_type: &[u8], privileges: &[PrivMap]) -> PgResult<AclMode> {
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

/// C `isspace((unsigned char) c)` in the default C locale.
#[inline]
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// `convert_table_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_table_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
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
    convert_any_priv_string(priv_type, &table_priv_map)
}

/// `convert_sequence_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_sequence_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
    let sequence_priv_map = [
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
        PrivMap { name: "SELECT", value: ACL_SELECT },
        PrivMap { name: "SELECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_SELECT) },
        PrivMap { name: "UPDATE", value: ACL_UPDATE },
        PrivMap { name: "UPDATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_UPDATE) },
    ];
    convert_any_priv_string(priv_type, &sequence_priv_map)
}

/// `convert_column_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_column_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
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
    convert_any_priv_string(priv_type, &column_priv_map)
}

/// `convert_database_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_database_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
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
    convert_any_priv_string(priv_type, &database_priv_map)
}

/// `convert_foreign_data_wrapper_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_foreign_data_wrapper_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
    let fdw_priv_map = [
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
    ];
    convert_any_priv_string(priv_type, &fdw_priv_map)
}

/// `convert_function_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_function_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
    let function_priv_map = [
        PrivMap { name: "EXECUTE", value: ACL_EXECUTE },
        PrivMap { name: "EXECUTE WITH GRANT OPTION", value: acl_grant_option_for(ACL_EXECUTE) },
    ];
    convert_any_priv_string(priv_type, &function_priv_map)
}

/// `convert_language_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_language_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
    let language_priv_map = [
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
    ];
    convert_any_priv_string(priv_type, &language_priv_map)
}

/// `convert_schema_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_schema_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
    let schema_priv_map = [
        PrivMap { name: "CREATE", value: ACL_CREATE },
        PrivMap { name: "CREATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_CREATE) },
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
    ];
    convert_any_priv_string(priv_type, &schema_priv_map)
}

/// `convert_server_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_server_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
    let server_priv_map = [
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
    ];
    convert_any_priv_string(priv_type, &server_priv_map)
}

/// `convert_tablespace_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_tablespace_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
    let tablespace_priv_map = [
        PrivMap { name: "CREATE", value: ACL_CREATE },
        PrivMap { name: "CREATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_CREATE) },
    ];
    convert_any_priv_string(priv_type, &tablespace_priv_map)
}

/// `convert_type_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_type_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
    let type_priv_map = [
        PrivMap { name: "USAGE", value: ACL_USAGE },
        PrivMap { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(ACL_USAGE) },
    ];
    convert_any_priv_string(priv_type, &type_priv_map)
}

/// `convert_parameter_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_parameter_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
    let parameter_priv_map = [
        PrivMap { name: "SET", value: ACL_SET },
        PrivMap { name: "SET WITH GRANT OPTION", value: acl_grant_option_for(ACL_SET) },
        PrivMap { name: "ALTER SYSTEM", value: ACL_ALTER_SYSTEM },
        PrivMap { name: "ALTER SYSTEM WITH GRANT OPTION", value: acl_grant_option_for(ACL_ALTER_SYSTEM) },
    ];
    convert_any_priv_string(priv_type, &parameter_priv_map)
}

/// `convert_largeobject_priv_string(priv_type_text)` — parse this object class's privilege string.
pub fn convert_largeobject_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
    let largeobject_priv_map = [
        PrivMap { name: "SELECT", value: ACL_SELECT },
        PrivMap { name: "SELECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_SELECT) },
        PrivMap { name: "UPDATE", value: ACL_UPDATE },
        PrivMap { name: "UPDATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_UPDATE) },
    ];
    convert_any_priv_string(priv_type, &largeobject_priv_map)
}

/// `convert_role_priv_string(priv_type_text)` — parse the `pg_has_role`
/// privilege string. C cheats and uses USAGE for has_privs_of_role, ACL_CREATE
/// for MEMBER, ACL_SET for SET, and the grant-option-of-ACL_CREATE bit for the
/// WITH GRANT/ADMIN OPTION variants. Shared only with `pg_role_aclcheck`.
pub fn convert_role_priv_string(priv_type: &[u8]) -> PgResult<AclMode> {
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
    convert_any_priv_string(priv_type, &role_priv_map)
}

// ===========================================================================
// convert_*_name support routines (acl.c)
// ===========================================================================

/// `text_to_cstring`-equivalent view of a `text` payload (no embedded NUL in a
/// SQL `text` value reaching these name-resolution helpers).
fn name_str(name: &[u8]) -> alloc::string::String {
    alloc::string::String::from_utf8_lossy(name).into_owned()
}

/// `convert_table_name(name)` — resolve an object name `text` to its OID.
///
/// C: `RangeVarGetRelid(makeRangeVarFromNameList(textToQualifiedNameList(name)),
/// NoLock, false)`.
pub fn convert_table_name(mcx: Mcx<'_>, name: &[u8]) -> PgResult<Oid> {
    let parts = varlena::text_to_qualified_name_list::call(mcx, name)?;
    let part_refs: alloc::vec::Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
    let relrv = namespace::make_range_var_from_name_list::call(&part_refs)?;

    // We might not even have permissions on this relation; don't lock it.
    namespace::range_var_get_relid::call(mcx, &relrv, NoLock, false)
}

/// `convert_database_name(name)` — resolve an object name `text` to its OID.
pub fn convert_database_name(mcx: Mcx<'_>, name: &[u8]) -> PgResult<Oid> {
    let _ = mcx;
    dbcommands::get_database_oid::call(&name_str(name), false)
}

/// `convert_foreign_data_wrapper_name(name)` — resolve an object name `text` to its OID.
pub fn convert_foreign_data_wrapper_name(mcx: Mcx<'_>, name: &[u8]) -> PgResult<Oid> {
    let _ = mcx;
    foreign::get_foreign_data_wrapper_oid::call(&name_str(name), false)
}

/// `convert_function_name(name)` — resolve an object name `text` to its OID.
///
/// C: `DatumGetObjectId(DirectFunctionCall1(regprocedurein,
/// CStringGetDatum(funcname)))`, then an explicit `OidIsValid` check.
pub fn convert_function_name(mcx: Mcx<'_>, name: &[u8]) -> PgResult<Oid> {
    let _ = mcx;
    let funcname = name_str(name);
    let oid = regproc::regprocedurein::call(&funcname)?;
    if !OidIsValid(oid) {
        return Err(PgError::error(alloc::format!("function \"{funcname}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }
    Ok(oid)
}

/// `convert_language_name(name)` — resolve an object name `text` to its OID.
pub fn convert_language_name(mcx: Mcx<'_>, name: &[u8]) -> PgResult<Oid> {
    let _ = mcx;
    proclang::get_language_oid::call(&name_str(name), false)
}

/// `convert_schema_name(name)` — resolve an object name `text` to its OID.
pub fn convert_schema_name(mcx: Mcx<'_>, name: &[u8]) -> PgResult<Oid> {
    let _ = mcx;
    namespace::get_namespace_oid::call(&name_str(name), false)
}

/// `convert_server_name(name)` — resolve an object name `text` to its OID.
pub fn convert_server_name(mcx: Mcx<'_>, name: &[u8]) -> PgResult<Oid> {
    let _ = mcx;
    foreign::get_foreign_server_oid::call(&name_str(name), false)
}

/// `convert_tablespace_name(name)` — resolve an object name `text` to its OID.
pub fn convert_tablespace_name(mcx: Mcx<'_>, name: &[u8]) -> PgResult<Oid> {
    let _ = mcx;
    tablespace::get_tablespace_oid::call(&name_str(name), false)
}

/// `convert_type_name(name)` — resolve an object name `text` to its OID.
///
/// C: `DatumGetObjectId(DirectFunctionCall1(regtypein,
/// CStringGetDatum(typname)))`, then an explicit `OidIsValid` check.
pub fn convert_type_name(mcx: Mcx<'_>, name: &[u8]) -> PgResult<Oid> {
    let _ = mcx;
    let typname = name_str(name);
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
pub fn convert_column_name(mcx: Mcx<'_>, tableoid: Oid, column: &[u8]) -> PgResult<AttrNumber> {
    let colname = name_str(column);

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
pub fn has_param_priv_byname(roleid: Oid, parameter: &[u8], r#priv: AclMode) -> PgResult<bool> {
    let paramstr = name_str(parameter);
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

    if guc_tables::vars::lo_compat_privileges.read() {
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
