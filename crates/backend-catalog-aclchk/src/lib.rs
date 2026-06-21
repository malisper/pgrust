//! `catalog/aclchk.c` — the privilege-check half (F1).
//!
//! This crate ports the "examine a user's privileges" and "report an aclcheck
//! failure" routines of `aclchk.c`: the `pg_aclmask` relay, the
//! `*_aclmask{,_ext}` family, the `*_aclcheck{,_ext}` family,
//! `object_ownercheck`, the `aclcheck_error{,_col,_type}` reporters,
//! `errorConflictingDefElem`, `string_to_privilege`/`privilege_to_string`,
//! `get_default_acl_internal`/`get_user_default_acl`,
//! `recordDependencyOnNewAcl`, and `has_createrole_privilege` /
//! `has_bypassrls_privilege`.
//!
//! The catalog rows are read through the F0 syscache ACL/owner projection seams
//! (`backend-utils-cache-syscache-seams`) plus the `largeobject_owner_acl`
//! projection in the merged pg_largeobject domain; the ACL bit logic is the
//! merged `backend-utils-adt-acl` `aclmask`/`acldefault`/`aclmerge` (over the
//! `&[AclItem]` + `Mcx` slice model).
//!
//! The GRANT executor, ALTER DEFAULT PRIVILEGES, and pg_init_privs halves stay
//! mirror-and-panic (F2/F3): `remove_role_from_object_acl`,
//! `remove_role_from_init_priv`, `replace_role_in_init_priv` are declared in
//! `backend-catalog-aclchk-seams` but NOT installed here, so the CATALOG row is
//! kept at `scaffold` until those land (the seam-install guard exempts an
//! unfinished owner).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod grant_exec;

use std::rc::Rc;

use mcx::{Mcx, MemoryContext};
use types_acl::{
    AclItem, AclMaskHow, AclMode, AclResult, ACLCHECK_NO_PRIV, ACLCHECK_NOT_OWNER, ACLCHECK_OK,
    ACLMASK_ANY, ACL_ALTER_SYSTEM, ACL_CONNECT, ACL_CREATE, ACL_CREATE_TEMP, ACL_DELETE,
    ACL_EXECUTE, ACL_INSERT, ACL_MAINTAIN, ACL_NO_RIGHTS, ACL_REFERENCES, ACL_SELECT, ACL_SET,
    ACL_TRIGGER, ACL_TRUNCATE, ACL_UPDATE, ACL_USAGE,
};
use types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_tuple::backend_access_common_heaptuple::Datum as TupDatum;
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_OBJECT, ERRCODE_UNDEFINED_SCHEMA,
    ERRCODE_UNDEFINED_TABLE, ERROR,
};
use types_nodes::parsenodes::ObjectType;
use types_snapshot::SnapshotData;

use backend_utils_error::ereport;

use backend_catalog_objectaddress::consts::{
    CollationRelationId, DatabaseRelationId, ForeignDataWrapperRelationId, ForeignServerRelationId,
    LanguageRelationId,
    LargeObjectMetadataRelationId, LargeObjectRelationId, NamespaceRelationId, ProcedureRelationId,
    RelationRelationId, TableSpaceRelationId, TypeRelationId,
};
use backend_catalog_objectaddress::properties::{
    get_object_attnum_acl, get_object_attnum_oid, get_object_attnum_owner,
    get_object_catcache_oid, get_object_class_descr, get_object_oid_index, get_object_type,
};

use backend_commands_user_seams::{has_privs_of_role, superuser_arg};
use backend_utils_adt_acl::acl_ops::aclmask;
use backend_utils_adt_acl::acldefault::acldefault;

// `ACL_ALL_RIGHTS_SCHEMA` (`utils/acl.h`).
const ACL_ALL_RIGHTS_SCHEMA: AclMode = ACL_USAGE | ACL_CREATE;

// `RELKIND_*` (`catalog/pg_class.h`).
const RELKIND_SEQUENCE: i8 = b'S' as i8;
const RELKIND_VIEW: i8 = b'v' as i8;

// `BOOTSTRAP_SUPERUSERID` (`catalog/pg_authid.h`).
const BOOTSTRAP_SUPERUSERID: Oid = 10;

// `ROLE_PG_*` well-known pinned OIDs (`catalog/pg_authid.dat`).
const ROLE_PG_READ_ALL_DATA: Oid = 6181;
const ROLE_PG_WRITE_ALL_DATA: Oid = 6182;
const ROLE_PG_MAINTAIN: Oid = 6337;

/* ===========================================================================
 * Small helpers to bridge the F0 `Option<PgVec<AclItem>>` (None = SQL-null ->
 * build acldefault) to `aclmask`'s `&[AclItem]`.
 * ========================================================================= */

/// `aclmask(acl ? acl : acldefault(objtype, ownerId), roleid, ownerId, mask,
/// how)` — applies the hard-wired default when the stored ACL column is SQL
/// null, exactly as aclchk does inline.
fn aclmask_with_default<'mcx>(
    mcx: Mcx<'mcx>,
    acl: Option<&[AclItem]>,
    objtype: ObjectType,
    roleid: Oid,
    owner_id: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> PgResult<AclMode> {
    match acl {
        Some(items) => aclmask(items, roleid, owner_id, mask, how),
        None => {
            let def = acldefault(mcx, objtype, owner_id)?;
            aclmask(def, roleid, owner_id, mask, how)
        }
    }
}

/* ===========================================================================
 * privilege string<->bit helpers (aclchk.c)
 * ========================================================================= */

/// `string_to_privilege(privname)` (aclchk.c).
pub fn string_to_privilege(privname: &str) -> PgResult<AclMode> {
    Ok(match privname {
        "insert" => ACL_INSERT,
        "select" => ACL_SELECT,
        "update" => ACL_UPDATE,
        "delete" => ACL_DELETE,
        "truncate" => ACL_TRUNCATE,
        "references" => ACL_REFERENCES,
        "trigger" => ACL_TRIGGER,
        "execute" => ACL_EXECUTE,
        "usage" => ACL_USAGE,
        "create" => ACL_CREATE,
        "temporary" => ACL_CREATE_TEMP,
        "temp" => ACL_CREATE_TEMP,
        "connect" => ACL_CONNECT,
        "set" => ACL_SET,
        "alter system" => ACL_ALTER_SYSTEM,
        "maintain" => ACL_MAINTAIN,
        _ => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized privilege type \"{privname}\""))
                .into_error())
        }
    })
}

/// `privilege_to_string(privilege)` (aclchk.c).
pub fn privilege_to_string(privilege: AclMode) -> PgResult<&'static str> {
    Ok(match privilege {
        ACL_INSERT => "INSERT",
        ACL_SELECT => "SELECT",
        ACL_UPDATE => "UPDATE",
        ACL_DELETE => "DELETE",
        ACL_TRUNCATE => "TRUNCATE",
        ACL_REFERENCES => "REFERENCES",
        ACL_TRIGGER => "TRIGGER",
        ACL_EXECUTE => "EXECUTE",
        ACL_USAGE => "USAGE",
        ACL_CREATE => "CREATE",
        ACL_CREATE_TEMP => "TEMP",
        ACL_CONNECT => "CONNECT",
        ACL_SET => "SET",
        ACL_ALTER_SYSTEM => "ALTER SYSTEM",
        ACL_MAINTAIN => "MAINTAIN",
        _ => {
            return Err(PgError::error(format!(
                "unrecognized privilege: {}",
                privilege as i32
            )))
        }
    })
}

/// `errorConflictingDefElem(defel, pstate)` (aclchk.c): always raises.
pub fn errorConflictingDefElem(defname: String) -> PgResult<()> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options".to_string())
        .errdetail(format!("Option \"{defname}\" specified more than once."))
        .into_error())
}

/* ===========================================================================
 * pg_aclmask relay (aclchk.c)
 * ========================================================================= */

/// `pg_aclmask(objtype, object_oid, attnum, roleid, mask, how)` (aclchk.c):
/// relay to the per-object-kind mask routine.
pub fn pg_aclmask(
    mcx: Mcx<'_>,
    objtype: ObjectType,
    object_oid: Oid,
    attnum: AttrNumber,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> PgResult<AclMode> {
    Ok(match objtype {
        ObjectType::Column => {
            pg_class_aclmask(mcx, object_oid, roleid, mask, how)?
                | pg_attribute_aclmask(mcx, object_oid, attnum, roleid, mask, how)?
        }
        ObjectType::Table | ObjectType::Sequence => {
            pg_class_aclmask(mcx, object_oid, roleid, mask, how)?
        }
        ObjectType::Database => object_aclmask(mcx, DatabaseRelationId, object_oid, roleid, mask, how)?,
        ObjectType::Function => {
            object_aclmask(mcx, ProcedureRelationId, object_oid, roleid, mask, how)?
        }
        ObjectType::Language => {
            object_aclmask(mcx, LanguageRelationId, object_oid, roleid, mask, how)?
        }
        ObjectType::Largeobject => {
            pg_largeobject_aclmask_snapshot(mcx, object_oid, roleid, mask, how, None)?
        }
        ObjectType::ParameterAcl => pg_parameter_acl_aclmask(mcx, object_oid, roleid, mask, how)?,
        ObjectType::Schema => {
            object_aclmask(mcx, NamespaceRelationId, object_oid, roleid, mask, how)?
        }
        ObjectType::StatisticExt => {
            return Err(PgError::error(
                "grantable rights not supported for statistics objects",
            ))
        }
        ObjectType::Tablespace => {
            object_aclmask(mcx, TableSpaceRelationId, object_oid, roleid, mask, how)?
        }
        ObjectType::Fdw => {
            object_aclmask(mcx, ForeignDataWrapperRelationId, object_oid, roleid, mask, how)?
        }
        ObjectType::ForeignServer => {
            object_aclmask(mcx, ForeignServerRelationId, object_oid, roleid, mask, how)?
        }
        ObjectType::EventTrigger => {
            return Err(PgError::error(
                "grantable rights not supported for event triggers",
            ))
        }
        ObjectType::Type => object_aclmask(mcx, TypeRelationId, object_oid, roleid, mask, how)?,
        _ => {
            return Err(PgError::error(format!(
                "unrecognized object type: {}",
                objtype as i32
            )))
        }
    })
}

/* ===========================================================================
 * object_aclmask{,_ext} — the generic syscache projection (aclchk.c)
 * ========================================================================= */

/// `object_aclmask(classid, objectid, roleid, mask, how)` (aclchk.c).
pub fn object_aclmask(
    mcx: Mcx<'_>,
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> PgResult<AclMode> {
    object_aclmask_ext(mcx, classid, objectid, roleid, mask, how, None).map(|(r, _)| r)
}

/// `object_aclmask_ext(classid, objectid, roleid, mask, how, is_missing)`
/// (aclchk.c). Returns `(result, is_missing)`; the bool is meaningful only when
/// `want_is_missing` is true (the C `is_missing != NULL`).
pub fn object_aclmask_ext(
    mcx: Mcx<'_>,
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
    want_is_missing: Option<()>,
) -> PgResult<(AclMode, bool)> {
    let is_missing = want_is_missing.is_some();

    /* Special cases */
    if classid == NamespaceRelationId {
        return pg_namespace_aclmask_ext(mcx, objectid, roleid, mask, how, want_is_missing);
    }
    if classid == TypeRelationId {
        return pg_type_aclmask_ext(mcx, objectid, roleid, mask, how, want_is_missing);
    }

    /* Even more special cases */
    debug_assert!(classid != RelationRelationId); /* should use pg_class_acl* */
    debug_assert!(classid != LargeObjectMetadataRelationId); /* should use pg_largeobject_acl* */

    /* Superusers bypass all permission checking. */
    if superuser_arg::call(roleid)? {
        return Ok((mask, false));
    }

    let cacheid = get_object_catcache_oid(classid)?;
    let owner_attnum = get_object_attnum_owner(classid)?;
    let acl_attnum = get_object_attnum_acl(classid)?;

    let proj = backend_utils_cache_syscache_seams::object_owner_acl::call(
        mcx,
        cacheid,
        objectid,
        owner_attnum,
        acl_attnum,
    )?;

    let Some(proj) = proj else {
        if is_missing {
            return Ok((0, true));
        }
        return Err(PgError::error(format!(
            "cache lookup failed for {} {}",
            get_object_class_descr(classid)?,
            objectid
        )));
    };

    let owner_id = proj.owner;
    let objtype = get_object_type(classid, objectid)?;
    let result = aclmask_with_default(
        mcx,
        proj.acl.as_deref(),
        objtype,
        roleid,
        owner_id,
        mask,
        how,
    )?;

    Ok((result, false))
}

/* ===========================================================================
 * pg_attribute_aclmask{,_ext} — column ACL (aclchk.c)
 * ========================================================================= */

/// `pg_attribute_aclmask(table_oid, attnum, roleid, mask, how)` (aclchk.c).
pub fn pg_attribute_aclmask(
    mcx: Mcx<'_>,
    table_oid: Oid,
    attnum: AttrNumber,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> PgResult<AclMode> {
    pg_attribute_aclmask_ext(mcx, table_oid, attnum, roleid, mask, how, None).map(|(r, _)| r)
}

/// `pg_attribute_aclmask_ext(table_oid, attnum, roleid, mask, how, is_missing)`
/// (aclchk.c).
pub fn pg_attribute_aclmask_ext(
    mcx: Mcx<'_>,
    table_oid: Oid,
    attnum: AttrNumber,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
    want_is_missing: Option<()>,
) -> PgResult<(AclMode, bool)> {
    let is_missing = want_is_missing.is_some();

    /* First, get the column's ACL from its pg_attribute entry */
    let att = backend_utils_cache_syscache_seams::pg_attribute_owner_acl::call(
        mcx, table_oid, attnum,
    )?;

    let Some((attisdropped, attacl)) = att else {
        if is_missing {
            return Ok((0, true));
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "attribute {attnum} of relation with OID {table_oid} does not exist"
            ))
            .into_error());
    };

    /* Check dropped columns, too */
    if attisdropped {
        if is_missing {
            return Ok((0, true));
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "attribute {attnum} of relation with OID {table_oid} does not exist"
            ))
            .into_error());
    }

    /*
     * Here we hard-wire knowledge that the default ACL for a column grants no
     * privileges, so that we can fall out quickly in the very common case
     * where attacl is null.
     */
    let Some(acl) = attacl else {
        return Ok((0, false));
    };

    /*
     * Must get the relation's ownerId from pg_class.
     */
    let cls = backend_utils_cache_syscache_seams::pg_class_owner_acl::call(mcx, table_oid)?;
    let Some(cls) = cls else {
        if is_missing {
            return Ok((0, true));
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!("relation with OID {table_oid} does not exist"))
            .into_error());
    };
    let owner_id = cls.relowner;

    let result = aclmask(&acl, roleid, owner_id, mask, how)?;
    Ok((result, false))
}

/* ===========================================================================
 * pg_class_aclmask{,_ext} — table ACL (aclchk.c)
 * ========================================================================= */

/// `pg_class_aclmask(table_oid, roleid, mask, how)` (aclchk.c).
pub fn pg_class_aclmask(
    mcx: Mcx<'_>,
    table_oid: Oid,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> PgResult<AclMode> {
    pg_class_aclmask_ext(mcx, table_oid, roleid, mask, how, None).map(|(r, _)| r)
}

/// `pg_class_aclmask_ext(table_oid, roleid, mask, how, is_missing)` (aclchk.c).
pub fn pg_class_aclmask_ext(
    mcx: Mcx<'_>,
    table_oid: Oid,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
    want_is_missing: Option<()>,
) -> PgResult<(AclMode, bool)> {
    let is_missing = want_is_missing.is_some();
    let mut mask = mask;

    let cls = backend_utils_cache_syscache_seams::pg_class_owner_acl::call(mcx, table_oid)?;
    let Some(cls) = cls else {
        if is_missing {
            return Ok((0, true));
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!("relation with OID {table_oid} does not exist"))
            .into_error());
    };

    /*
     * Deny anyone permission to update a system catalog unless
     * pg_authid.rolsuper is set.
     */
    if (mask & (ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE)) != 0
        && backend_catalog_catalog_seams::is_system_class_by_namespace::call(
            table_oid,
            cls.relnamespace,
        )
        && cls.relkind != RELKIND_VIEW
        && !superuser_arg::call(roleid)?
    {
        mask &= !(ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE);
    }

    /* Otherwise, superusers bypass all permission-checking. */
    if superuser_arg::call(roleid)? {
        return Ok((mask, false));
    }

    /* Normal case: get the relation's ACL from pg_class */
    let owner_id = cls.relowner;
    let objtype = match cls.relkind {
        RELKIND_SEQUENCE => ObjectType::Sequence,
        _ => ObjectType::Table,
    };
    let mut result =
        aclmask_with_default(mcx, cls.acl.as_deref(), objtype, roleid, owner_id, mask, how)?;

    /*
     * Check pg_read_all_data / pg_write_all_data / pg_maintain role
     * memberships for the relevant masks.
     */
    if mask & ACL_SELECT != 0
        && result & ACL_SELECT == 0
        && has_privs_of_role::call(roleid, ROLE_PG_READ_ALL_DATA)?
    {
        result |= ACL_SELECT;
    }

    if mask & (ACL_INSERT | ACL_UPDATE | ACL_DELETE) != 0
        && result & (ACL_INSERT | ACL_UPDATE | ACL_DELETE) == 0
        && has_privs_of_role::call(roleid, ROLE_PG_WRITE_ALL_DATA)?
    {
        result |= mask & (ACL_INSERT | ACL_UPDATE | ACL_DELETE);
    }

    if mask & ACL_MAINTAIN != 0
        && result & ACL_MAINTAIN == 0
        && has_privs_of_role::call(roleid, ROLE_PG_MAINTAIN)?
    {
        result |= ACL_MAINTAIN;
    }

    Ok((result, false))
}

/* ===========================================================================
 * pg_parameter_aclmask / pg_parameter_acl_aclmask (aclchk.c)
 * ========================================================================= */

/// `pg_parameter_aclmask(name, roleid, mask, how)` (aclchk.c).
pub fn pg_parameter_aclmask(
    mcx: Mcx<'_>,
    name: &str,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> PgResult<AclMode> {
    /* Superusers bypass all permission checking. */
    if superuser_arg::call(roleid)? {
        return Ok(mask);
    }

    /* Convert name to the form it should have in pg_parameter_acl... */
    let parname = backend_utils_misc_guc::convert_guc_name_for_parameter_acl(name);

    /* ... and look it up */
    let proj = backend_utils_cache_syscache_seams::parameter_acl_by_name::call(mcx, &parname)?;

    let result = match proj {
        /* If no entry, GUC has no permissions for non-superusers */
        None => ACL_NO_RIGHTS,
        Some(acl) => aclmask_with_default(
            mcx,
            acl.as_deref(),
            ObjectType::ParameterAcl,
            roleid,
            BOOTSTRAP_SUPERUSERID,
            mask,
            how,
        )?,
    };

    Ok(result)
}

/// `pg_parameter_acl_aclmask(acl_oid, roleid, mask, how)` (aclchk.c).
pub fn pg_parameter_acl_aclmask(
    mcx: Mcx<'_>,
    acl_oid: Oid,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
) -> PgResult<AclMode> {
    /* Superusers bypass all permission checking. */
    if superuser_arg::call(roleid)? {
        return Ok(mask);
    }

    let proj = backend_utils_cache_syscache_seams::parameter_acl_by_oid::call(mcx, acl_oid)?;
    let Some(acl) = proj else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("parameter ACL with OID {acl_oid} does not exist"))
            .into_error());
    };

    aclmask_with_default(
        mcx,
        acl.as_deref(),
        ObjectType::ParameterAcl,
        roleid,
        BOOTSTRAP_SUPERUSERID,
        mask,
        how,
    )
}

/* ===========================================================================
 * pg_largeobject_aclmask_snapshot (aclchk.c)
 * ========================================================================= */

/// `pg_largeobject_aclmask_snapshot(lobj_oid, roleid, mask, how, snapshot)`
/// (aclchk.c). The catalog read crosses into the merged pg_largeobject domain
/// (`largeobject_owner_acl`), since pg_largeobject_metadata has no syscache.
pub fn pg_largeobject_aclmask_snapshot(
    mcx: Mcx<'_>,
    lobj_oid: Oid,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
    snapshot: Option<Rc<SnapshotData>>,
) -> PgResult<AclMode> {
    /* Superusers bypass all permission checking. */
    if superuser_arg::call(roleid)? {
        return Ok(mask);
    }

    let proj = backend_catalog_pg_largeobject_seams::largeobject_owner_acl::call(
        mcx, lobj_oid, snapshot,
    )?;
    let Some((owner_id, acl)) = proj else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("large object {lobj_oid} does not exist"))
            .into_error());
    };

    aclmask_with_default(
        mcx,
        acl.as_deref(),
        ObjectType::Largeobject,
        roleid,
        owner_id,
        mask,
        how,
    )
}

/* ===========================================================================
 * pg_namespace_aclmask_ext (aclchk.c)
 * ========================================================================= */

/// `pg_namespace_aclmask_ext(nsp_oid, roleid, mask, how, is_missing)` (aclchk.c).
pub fn pg_namespace_aclmask_ext(
    mcx: Mcx<'_>,
    nsp_oid: Oid,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
    want_is_missing: Option<()>,
) -> PgResult<(AclMode, bool)> {
    let is_missing = want_is_missing.is_some();

    /* Superusers bypass all permission checking. */
    if superuser_arg::call(roleid)? {
        return Ok((mask, false));
    }

    /*
     * If we have been assigned this namespace as a temp namespace, check to
     * make sure we have CREATE TEMP permission on the database.
     */
    if backend_catalog_namespace_seams::is_temp_namespace::call(nsp_oid)? {
        let my_db = backend_utils_init_small_seams::my_database_id::call();
        let (res, _missing) =
            object_aclcheck_ext(mcx, DatabaseRelationId, my_db, roleid, ACL_CREATE_TEMP, want_is_missing)?;
        if res == ACLCHECK_OK {
            return Ok((mask & ACL_ALL_RIGHTS_SCHEMA, false));
        } else {
            return Ok((mask & ACL_USAGE, false));
        }
    }

    let nsp = backend_utils_cache_syscache_seams::pg_namespace_owner_acl::call(mcx, nsp_oid)?;
    let Some(nsp) = nsp else {
        if is_missing {
            return Ok((0, true));
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_SCHEMA)
            .errmsg(format!("schema with OID {nsp_oid} does not exist"))
            .into_error());
    };
    let owner_id = nsp.nspowner;

    let mut result = aclmask_with_default(
        mcx,
        nsp.acl.as_deref(),
        ObjectType::Schema,
        roleid,
        owner_id,
        mask,
        how,
    )?;

    /*
     * Check pg_read_all_data / pg_write_all_data role membership for ACL_USAGE.
     */
    if mask & ACL_USAGE != 0
        && result & ACL_USAGE == 0
        && (has_privs_of_role::call(roleid, ROLE_PG_READ_ALL_DATA)?
            || has_privs_of_role::call(roleid, ROLE_PG_WRITE_ALL_DATA)?)
    {
        result |= ACL_USAGE;
    }

    Ok((result, false))
}

/* ===========================================================================
 * pg_type_aclmask_ext (aclchk.c). The array-element / multirange redirects are
 * resolved INSIDE the F0 projection (pg_type_owner_acl).
 * ========================================================================= */

/// `pg_type_aclmask_ext(type_oid, roleid, mask, how, is_missing)` (aclchk.c).
pub fn pg_type_aclmask_ext(
    mcx: Mcx<'_>,
    type_oid: Oid,
    roleid: Oid,
    mask: AclMode,
    how: AclMaskHow,
    want_is_missing: Option<()>,
) -> PgResult<(AclMode, bool)> {
    let is_missing = want_is_missing.is_some();

    /* Bypass permission checks for superusers */
    if superuser_arg::call(roleid)? {
        return Ok((mask, false));
    }

    let ty = backend_utils_cache_syscache_seams::pg_type_owner_acl::call(mcx, type_oid)?;
    let Some(ty) = ty else {
        if is_missing {
            return Ok((0, true));
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("type with OID {type_oid} does not exist"))
            .into_error());
    };
    let owner_id = ty.typowner;

    let result = aclmask_with_default(
        mcx,
        ty.acl.as_deref(),
        ObjectType::Type,
        roleid,
        owner_id,
        mask,
        how,
    )?;

    Ok((result, false))
}

/* ===========================================================================
 * The *_aclcheck{,_ext} family (aclchk.c)
 * ========================================================================= */

/// `object_aclcheck(classid, objectid, roleid, mode)` (aclchk.c).
pub fn object_aclcheck(
    mcx: Mcx<'_>,
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mode: AclMode,
) -> PgResult<AclResult> {
    object_aclcheck_ext(mcx, classid, objectid, roleid, mode, None).map(|(r, _)| r)
}

/// `object_aclcheck_ext(classid, objectid, roleid, mode, is_missing)` (aclchk.c).
pub fn object_aclcheck_ext(
    mcx: Mcx<'_>,
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mode: AclMode,
    want_is_missing: Option<()>,
) -> PgResult<(AclResult, bool)> {
    let (mask, is_missing) =
        object_aclmask_ext(mcx, classid, objectid, roleid, mode, ACLMASK_ANY, want_is_missing)?;
    let res = if mask != 0 { ACLCHECK_OK } else { ACLCHECK_NO_PRIV };
    Ok((res, is_missing))
}

/// `pg_attribute_aclcheck(table_oid, attnum, roleid, mode)` (aclchk.c).
pub fn pg_attribute_aclcheck(
    mcx: Mcx<'_>,
    table_oid: Oid,
    attnum: AttrNumber,
    roleid: Oid,
    mode: AclMode,
) -> PgResult<AclResult> {
    pg_attribute_aclcheck_ext(mcx, table_oid, attnum, roleid, mode, None).map(|(r, _)| r)
}

/// `pg_attribute_aclcheck_ext(table_oid, attnum, roleid, mode, is_missing)`
/// (aclchk.c).
pub fn pg_attribute_aclcheck_ext(
    mcx: Mcx<'_>,
    table_oid: Oid,
    attnum: AttrNumber,
    roleid: Oid,
    mode: AclMode,
    want_is_missing: Option<()>,
) -> PgResult<(AclResult, bool)> {
    let (mask, is_missing) = pg_attribute_aclmask_ext(
        mcx, table_oid, attnum, roleid, mode, ACLMASK_ANY, want_is_missing,
    )?;
    let res = if mask != 0 { ACLCHECK_OK } else { ACLCHECK_NO_PRIV };
    Ok((res, is_missing))
}

/// `pg_attribute_aclcheck_all(table_oid, roleid, mode, how)` (aclchk.c).
pub fn pg_attribute_aclcheck_all(
    mcx: Mcx<'_>,
    table_oid: Oid,
    roleid: Oid,
    mode: AclMode,
    how: AclMaskHow,
) -> PgResult<AclResult> {
    pg_attribute_aclcheck_all_ext(mcx, table_oid, roleid, mode, how, None).map(|(r, _)| r)
}

/// `pg_attribute_aclcheck_all_ext(table_oid, roleid, mode, how, is_missing)`
/// (aclchk.c).
pub fn pg_attribute_aclcheck_all_ext(
    mcx: Mcx<'_>,
    table_oid: Oid,
    roleid: Oid,
    mode: AclMode,
    how: AclMaskHow,
    want_is_missing: Option<()>,
) -> PgResult<(AclResult, bool)> {
    let is_missing = want_is_missing.is_some();

    /* Must fetch pg_class row to get owner ID and number of attributes. */
    let cls = backend_utils_cache_syscache_seams::pg_class_owner_acl::call(mcx, table_oid)?;
    let Some(cls) = cls else {
        if is_missing {
            return Ok((ACLCHECK_NO_PRIV, true));
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!("relation with OID {table_oid} does not exist"))
            .into_error());
    };
    let owner_id = cls.relowner;
    /* relnatts comes off the same pg_class tuple (pg_class_extra projection). */
    let extra = backend_utils_cache_syscache_seams::pg_class_extra::call(table_oid)?;
    let Some(extra) = extra else {
        if is_missing {
            return Ok((ACLCHECK_NO_PRIV, true));
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!("relation with OID {table_oid} does not exist"))
            .into_error());
    };
    let nattrs = extra.relnatts;

    /*
     * Initialize result in case there are no non-dropped columns.  We want to
     * report failure in such cases for either value of 'how'.
     */
    let mut result = ACLCHECK_NO_PRIV;

    let mut curr_att: AttrNumber = 1;
    while curr_att <= nattrs {
        let att = backend_utils_cache_syscache_seams::pg_attribute_owner_acl::call(
            mcx, table_oid, curr_att,
        )?;

        let attmask = match att {
            /*
             * Lookup failure probably indicates that the table was just
             * dropped, but we'll treat it the same as a dropped column.
             */
            None => {
                curr_att += 1;
                continue;
            }
            Some((true, _)) => {
                /* ignore dropped columns */
                curr_att += 1;
                continue;
            }
            Some((false, attacl)) => match attacl {
                /*
                 * Hard-wire knowledge that the default ACL for a column grants
                 * no privileges.
                 */
                None => 0,
                Some(acl) => aclmask(&acl, roleid, owner_id, mode, ACLMASK_ANY)?,
            },
        };

        if attmask != 0 {
            result = ACLCHECK_OK;
            if how == ACLMASK_ANY {
                break; /* succeed on any success */
            }
        } else {
            result = ACLCHECK_NO_PRIV;
            if how == types_acl::ACLMASK_ALL {
                break; /* fail on any failure */
            }
        }

        curr_att += 1;
    }

    Ok((result, false))
}

/// `pg_class_aclcheck(table_oid, roleid, mode)` (aclchk.c).
pub fn pg_class_aclcheck(
    mcx: Mcx<'_>,
    table_oid: Oid,
    roleid: Oid,
    mode: AclMode,
) -> PgResult<AclResult> {
    pg_class_aclcheck_ext(mcx, table_oid, roleid, mode, None).map(|(r, _)| r)
}

/// `pg_class_aclcheck_ext(table_oid, roleid, mode, is_missing)` (aclchk.c).
pub fn pg_class_aclcheck_ext(
    mcx: Mcx<'_>,
    table_oid: Oid,
    roleid: Oid,
    mode: AclMode,
    want_is_missing: Option<()>,
) -> PgResult<(AclResult, bool)> {
    let (mask, is_missing) =
        pg_class_aclmask_ext(mcx, table_oid, roleid, mode, ACLMASK_ANY, want_is_missing)?;
    let res = if mask != 0 { ACLCHECK_OK } else { ACLCHECK_NO_PRIV };
    Ok((res, is_missing))
}

/// `pg_parameter_aclcheck(name, roleid, mode)` (aclchk.c).
pub fn pg_parameter_aclcheck(
    mcx: Mcx<'_>,
    name: &str,
    roleid: Oid,
    mode: AclMode,
) -> PgResult<AclResult> {
    if pg_parameter_aclmask(mcx, name, roleid, mode, ACLMASK_ANY)? != 0 {
        Ok(ACLCHECK_OK)
    } else {
        Ok(ACLCHECK_NO_PRIV)
    }
}

/// `pg_largeobject_aclcheck_snapshot(lobj_oid, roleid, mode, snapshot)`
/// (aclchk.c).
pub fn pg_largeobject_aclcheck_snapshot(
    mcx: Mcx<'_>,
    lobj_oid: Oid,
    roleid: Oid,
    mode: AclMode,
    snapshot: Option<Rc<SnapshotData>>,
) -> PgResult<AclResult> {
    if pg_largeobject_aclmask_snapshot(mcx, lobj_oid, roleid, mode, ACLMASK_ANY, snapshot)? != 0 {
        Ok(ACLCHECK_OK)
    } else {
        Ok(ACLCHECK_NO_PRIV)
    }
}

/* ===========================================================================
 * object_ownercheck (aclchk.c)
 * ========================================================================= */

/// `object_ownercheck(classid, objectid, roleid)` (aclchk.c).
pub fn object_ownercheck(
    mcx: Mcx<'_>,
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
) -> PgResult<bool> {
    /* Superusers bypass all permission checking. */
    if superuser_arg::call(roleid)? {
        return Ok(true);
    }

    /* For large objects, the catalog to consult is pg_largeobject_metadata */
    let classid = if classid == LargeObjectRelationId {
        LargeObjectMetadataRelationId
    } else {
        classid
    };

    let cacheid = get_object_catcache_oid(classid)?;
    let owner_id: Oid;

    if cacheid != -1 {
        /* we can get the object's tuple from the syscache */
        let owner_attnum = get_object_attnum_owner(classid)?;
        let acl_attnum = get_object_attnum_acl(classid)?;
        let proj = backend_utils_cache_syscache_seams::object_owner_acl::call(
            mcx,
            cacheid,
            objectid,
            owner_attnum,
            acl_attnum,
        )?;
        let Some(proj) = proj else {
            return Err(PgError::error(format!(
                "cache lookup failed for {} {}",
                get_object_class_descr(classid)?,
                objectid
            )));
        };
        owner_id = proj.owner;
    } else {
        /* for catalogs without an appropriate syscache */
        owner_id = scan_owner_for_catalog(mcx, classid, objectid)?;
    }

    has_privs_of_role::call(roleid, owner_id)
}

/// The cache-less `object_ownercheck` fallback (aclchk.c): `table_open` +
/// `systable_beginscan(get_object_oid_index(classid), oid = objectid)`, then
/// `heap_getattr(get_object_attnum_owner(classid))`.
fn scan_owner_for_catalog(mcx: Mcx<'_>, classid: Oid, objectid: Oid) -> PgResult<Oid> {
    use backend_access_common_scankey::ScanKeyInit;
    use backend_access_index_genam_seams as genam;
    use backend_access_table_table::table_open;
    use types_core::fmgr::F_OIDEQ;
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    use types_storage::lock::AccessShareLock;
    use types_tuple::backend_access_common_heaptuple::Datum;

    let oid_attnum = get_object_attnum_oid(classid)?;
    let oid_index = get_object_oid_index(classid)?;
    let owner_attnum = get_object_attnum_owner(classid)?;

    let rel = table_open(mcx, classid, AccessShareLock)?;

    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        oid_attnum,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(objectid),
    )?;
    let keys = [key];

    let mut scan = genam::systable_beginscan::call(&rel, oid_index, true, None, &keys)?;

    let tuple = genam::systable_getnext::call(mcx, scan.desc_mut())?;
    let Some(tuple) = tuple else {
        scan.end()?;
        rel.close(AccessShareLock)?;
        return Err(PgError::error(format!(
            "could not find tuple for {} {}",
            get_object_class_descr(classid)?,
            objectid
        )));
    };

    let cols = backend_access_common_heaptuple::heap_deform_tuple(
        mcx,
        &tuple.tuple,
        &rel.rd_att,
        &tuple.data,
    )?;
    let (owner_val, owner_null) = &cols[(owner_attnum - 1) as usize];
    debug_assert!(!*owner_null);
    let owner_id: Oid = match owner_val {
        Datum::ByVal(v) => *v as u32,
        Datum::ByRef(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => {
            return Err(PgError::error("object_ownercheck: owner column is by-reference"))
        }
    };

    scan.end()?;
    rel.close(AccessShareLock)?;

    Ok(owner_id)
}

/* ===========================================================================
 * get_default_acl_internal / get_user_default_acl (aclchk.c)
 *
 * Full port: looks up the per-role/per-schema `pg_default_acl` entries
 * established by `ALTER DEFAULT PRIVILEGES`, merges them with the hard-wired
 * implicit default, and returns the on-disk `aclitem[]` varlena `Datum` to
 * apply to a newly-created object (or `None` when the built-in default should
 * be used).
 *
 * `recordDependencyOnNewAcl` records `pg_shdepend` dependencies on every role
 * mentioned in that ACL.
 *
 * has_createrole_privilege / has_bypassrls_privilege are already ported and
 * installed by `backend-utils-adt-acl` (role_membership.rs); not re-ported here.
 * ========================================================================= */

/// `get_default_acl_internal(roleId, nsp_oid, objtype)` (aclchk.c): fetch the
/// `pg_default_acl` entry for the given role, namespace and object type,
/// decoding its `defaclacl` aclitem[] column. Returns `None` when no such entry
/// exists (or the column is SQL-NULL).
fn get_default_acl_internal<'mcx>(
    mcx: Mcx<'mcx>,
    role_id: Oid,
    nsp_oid: Oid,
    objtype: i8,
) -> PgResult<Option<&'mcx [AclItem]>> {
    use backend_catalog_objectaddress::consts::Anum_pg_default_acl_defaclacl;
    use backend_utils_cache_syscache::{
        ReleaseSysCache, SearchSysCache3, SysCacheGetAttr, DEFACLROLENSPOBJ,
    };
    use types_cache::SysCacheKey;
    use types_datum::Datum as KeyDatum;

    let tuple = SearchSysCache3(
        mcx,
        DEFACLROLENSPOBJ,
        SysCacheKey::Value(KeyDatum::from_oid(role_id)),
        SysCacheKey::Value(KeyDatum::from_oid(nsp_oid)),
        SysCacheKey::Value(KeyDatum::from_char(objtype)),
    )?;

    let Some(tup) = tuple else {
        return Ok(None);
    };

    // aclDatum = SysCacheGetAttr(DEFACLROLENSPOBJ, tuple,
    //                            Anum_pg_default_acl_defaclacl, &isNull);
    // if (!isNull) result = DatumGetAclPCopy(aclDatum);
    let (acl_datum, is_null) = SysCacheGetAttr(
        mcx,
        DEFACLROLENSPOBJ,
        &tup,
        Anum_pg_default_acl_defaclacl as i32,
    )?;
    let result = if is_null {
        None
    } else {
        let raw = match &acl_datum {
            TupDatum::ByRef(b) => &b[..],
            _ => {
                ReleaseSysCache(tup);
                return Err(PgError::error(
                    "get_default_acl_internal: defaclacl column is not a varlena",
                ));
            }
        };
        Some(&*grant_exec::decode_acl(mcx, raw)?)
    };

    ReleaseSysCache(tup);
    Ok(result)
}

/// `get_user_default_acl(objtype, ownerId, nsp_oid)` (aclchk.c): default
/// permissions for a newly-created object in a schema. Returns `None` when the
/// built-in system defaults should be used (the common case on a fresh
/// cluster).
pub fn get_user_default_acl<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    owner_id: Oid,
    nsp_oid: Oid,
) -> PgResult<Option<TupDatum<'mcx>>> {
    use backend_catalog_objectaddress::consts::{
        DEFACLOBJ_FUNCTION, DEFACLOBJ_LARGEOBJECT, DEFACLOBJ_NAMESPACE, DEFACLOBJ_RELATION,
        DEFACLOBJ_SEQUENCE, DEFACLOBJ_TYPE,
    };
    use backend_utils_adt_acl::acl_ops::{aclequal, aclitemsort, aclmerge};
    use backend_utils_adt_acl::acldefault::acldefault;

    // Use NULL during bootstrap, since pg_default_acl probably isn't there yet.
    if backend_utils_init_miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(None);
    }

    // Check if object type is supported in pg_default_acl.
    let defaclobjtype: i8 = match objtype {
        ObjectType::Table => DEFACLOBJ_RELATION,
        ObjectType::Sequence => DEFACLOBJ_SEQUENCE,
        ObjectType::Function => DEFACLOBJ_FUNCTION,
        ObjectType::Type => DEFACLOBJ_TYPE,
        ObjectType::Schema => DEFACLOBJ_NAMESPACE,
        ObjectType::Largeobject => DEFACLOBJ_LARGEOBJECT,
        _ => return Ok(None),
    };

    // Look up the relevant pg_default_acl entries.
    let glob_acl = get_default_acl_internal(mcx, owner_id, InvalidOid, defaclobjtype)?;
    let schema_acl = get_default_acl_internal(mcx, owner_id, nsp_oid, defaclobjtype)?;

    // Quick out if neither entry exists.
    if glob_acl.is_none() && schema_acl.is_none() {
        return Ok(None);
    }

    // We need to know the hard-wired default value, too.
    let def_acl: &[AclItem] = acldefault(mcx, objtype, owner_id)?;

    // If there's no global entry, substitute the hard-wired default. A missing
    // per-schema entry is the empty ACL, the NULL equivalent for `aclmerge`.
    let glob_acl: &[AclItem] = glob_acl.unwrap_or(def_acl);
    let schema_acl: &[AclItem] = schema_acl.unwrap_or(&[]);

    // Merge in any per-schema privileges.
    let result: &mut [AclItem] = aclmerge(mcx, glob_acl, schema_acl, owner_id)?;

    // For efficiency, we want to return None if the result equals default.
    // This requires sorting both arrays to get an accurate comparison.
    aclitemsort(result);
    // Sort a private copy of the default so callers that share it are unaffected.
    let mut def_sorted: mcx::PgVec<AclItem> = mcx::slice_in(mcx, def_acl)?;
    aclitemsort(&mut def_sorted);
    if aclequal(result, &def_sorted) {
        return Ok(None);
    }

    Ok(Some(grant_exec::acl_to_datum(mcx, result)?))
}

/* ===========================================================================
 * aclcheck_error{,_col,_type} reporters (aclchk.c)
 * ========================================================================= */

/// `aclcheck_error(aclerr, objtype, objectname)` (aclchk.c): the standardized
/// permission-denied / must-be-owner error.
pub fn aclcheck_error(
    aclerr: AclResult,
    objtype: ObjectType,
    objectname: Option<String>,
) -> PgResult<()> {
    let name = objectname.unwrap_or_default();
    match aclerr {
        ACLCHECK_OK => Ok(()),
        ACLCHECK_NO_PRIV => {
            let msg = no_priv_message(objtype)?;
            Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(msg.replace("%s", &name))
                .into_error())
        }
        ACLCHECK_NOT_OWNER => {
            let msg = not_owner_message(objtype)?;
            Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(msg.replace("%s", &name))
                .into_error())
        }
    }
}

fn no_priv_message(objtype: ObjectType) -> PgResult<&'static str> {
    use ObjectType::*;
    Ok(match objtype {
        Aggregate => "permission denied for aggregate %s",
        Collation => "permission denied for collation %s",
        Column => "permission denied for column %s",
        Conversion => "permission denied for conversion %s",
        Database => "permission denied for database %s",
        Domain => "permission denied for domain %s",
        EventTrigger => "permission denied for event trigger %s",
        Extension => "permission denied for extension %s",
        Fdw => "permission denied for foreign-data wrapper %s",
        ForeignServer => "permission denied for foreign server %s",
        ForeignTable => "permission denied for foreign table %s",
        Function => "permission denied for function %s",
        Index => "permission denied for index %s",
        Language => "permission denied for language %s",
        Largeobject => "permission denied for large object %s",
        Matview => "permission denied for materialized view %s",
        Opclass => "permission denied for operator class %s",
        Operator => "permission denied for operator %s",
        Opfamily => "permission denied for operator family %s",
        ParameterAcl => "permission denied for parameter %s",
        Policy => "permission denied for policy %s",
        Procedure => "permission denied for procedure %s",
        Publication => "permission denied for publication %s",
        Routine => "permission denied for routine %s",
        Schema => "permission denied for schema %s",
        Sequence => "permission denied for sequence %s",
        StatisticExt => "permission denied for statistics object %s",
        Subscription => "permission denied for subscription %s",
        Table => "permission denied for table %s",
        Tablespace => "permission denied for tablespace %s",
        TsConfiguration => "permission denied for text search configuration %s",
        TsDictionary => "permission denied for text search dictionary %s",
        Type => "permission denied for type %s",
        View => "permission denied for view %s",
        _ => {
            return Err(PgError::error(format!(
                "unsupported object type: {}",
                objtype as i32
            )))
        }
    })
}

fn not_owner_message(objtype: ObjectType) -> PgResult<&'static str> {
    use ObjectType::*;
    Ok(match objtype {
        Aggregate => "must be owner of aggregate %s",
        Collation => "must be owner of collation %s",
        Conversion => "must be owner of conversion %s",
        Database => "must be owner of database %s",
        Domain => "must be owner of domain %s",
        EventTrigger => "must be owner of event trigger %s",
        Extension => "must be owner of extension %s",
        Fdw => "must be owner of foreign-data wrapper %s",
        ForeignServer => "must be owner of foreign server %s",
        ForeignTable => "must be owner of foreign table %s",
        Function => "must be owner of function %s",
        Index => "must be owner of index %s",
        Language => "must be owner of language %s",
        Largeobject => "must be owner of large object %s",
        Matview => "must be owner of materialized view %s",
        Opclass => "must be owner of operator class %s",
        Operator => "must be owner of operator %s",
        Opfamily => "must be owner of operator family %s",
        Procedure => "must be owner of procedure %s",
        Publication => "must be owner of publication %s",
        Routine => "must be owner of routine %s",
        Sequence => "must be owner of sequence %s",
        Subscription => "must be owner of subscription %s",
        Table => "must be owner of table %s",
        Type => "must be owner of type %s",
        View => "must be owner of view %s",
        Schema => "must be owner of schema %s",
        StatisticExt => "must be owner of statistics object %s",
        Tablespace => "must be owner of tablespace %s",
        TsConfiguration => "must be owner of text search configuration %s",
        TsDictionary => "must be owner of text search dictionary %s",
        /*
         * Special cases: the error message talks about "relation", because
         * that's where the ownership is attached.
         */
        Column | Policy | Rule | Tabconstraint | Trigger => "must be owner of relation %s",
        _ => {
            return Err(PgError::error(format!(
                "unsupported object type: {}",
                objtype as i32
            )))
        }
    })
}

/// `aclcheck_error_col(aclerr, objtype, objectname, colname)` (aclchk.c).
pub fn aclcheck_error_col(
    aclerr: AclResult,
    objtype: ObjectType,
    objectname: Option<String>,
    colname: String,
) -> PgResult<()> {
    match aclerr {
        ACLCHECK_OK => Ok(()),
        ACLCHECK_NO_PRIV => {
            let name = objectname.unwrap_or_default();
            Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "permission denied for column \"{colname}\" of relation \"{name}\""
                ))
                .into_error())
        }
        /* relation msg is OK since columns don't have separate owners */
        ACLCHECK_NOT_OWNER => aclcheck_error(aclerr, objtype, objectname),
    }
}

/// `aclcheck_error_type(aclerr, typeOid)` (aclchk.c): use the element type
/// instead of the array type, format nicely, then delegate to
/// `aclcheck_error`.
pub fn aclcheck_error_type(mcx: Mcx<'_>, aclerr: AclResult, type_oid: Oid) -> PgResult<()> {
    let element_type = backend_utils_cache_lsyscache_seams::get_element_type::call(type_oid)?;
    let effective = match element_type {
        Some(et) if OidIsValid(et) => et,
        _ => type_oid,
    };
    let formatted = backend_utils_adt_format_type_seams::format_type_be::call(mcx, effective)?;
    aclcheck_error(aclerr, ObjectType::Type, Some(formatted.to_string()))
}

/* ===========================================================================
 * Seam installation
 * ========================================================================= */

/// Install this unit's inward seam(s). Wired into `seams-init`'s `init_all`.
///
/// The F1 check-half seams are installed here. The three executor / init-privs
/// seams (`remove_role_from_object_acl`, `remove_role_from_init_priv`,
/// `replace_role_in_init_priv`) stay mirror-and-panic (F2/F3) — the CATALOG row
/// is held at `scaffold` so the seam-install guard exempts them.
/// `has_createrole_privilege(roleid)` (aclchk.c:4169-4185) — true if `roleid`
/// is a superuser or has `rolcreaterole`. Used where an ownership-like test is
/// needed for role creation/alteration.
pub fn has_createrole_privilege(roleid: Oid) -> PgResult<bool> {
    /* Superusers bypass all permission checking. */
    if superuser_arg::call(roleid)? {
        return Ok(true);
    }
    let ctx = MemoryContext::new("has_createrole_privilege");
    let result = match backend_utils_cache_syscache_seams::lookup_authid_by_oid::call(
        ctx.mcx(),
        roleid,
    )? {
        Some(row) => row.rolcreaterole,
        None => false,
    };
    Ok(result)
}

pub fn init_seams() {
    use backend_catalog_aclchk_seams as seam;

    backend_commands_user_seams::has_createrole_privilege::set(has_createrole_privilege);

    seam::object_aclcheck::set(|classid, objectid, roleid, mode| {
        let ctx = MemoryContext::new("object_aclcheck");
        object_aclcheck(ctx.mcx(), classid, objectid, roleid, mode)
    });
    seam::object_aclcheck_ext::set(|classid, objectid, roleid, mode| {
        let ctx = MemoryContext::new("object_aclcheck_ext");
        object_aclcheck_ext(ctx.mcx(), classid, objectid, roleid, mode, Some(()))
    });
    seam::pg_class_aclmask::set(|table_oid, roleid, mask, how| {
        let ctx = MemoryContext::new("pg_class_aclmask");
        pg_class_aclmask(ctx.mcx(), table_oid, roleid, mask, how)
    });
    seam::pg_class_aclcheck::set(|table_oid, roleid, mode| {
        let ctx = MemoryContext::new("pg_class_aclcheck");
        pg_class_aclcheck(ctx.mcx(), table_oid, roleid, mode)
    });
    seam::pg_class_aclcheck_ext::set(|table_oid, roleid, mode| {
        let ctx = MemoryContext::new("pg_class_aclcheck_ext");
        pg_class_aclcheck_ext(ctx.mcx(), table_oid, roleid, mode, Some(()))
    });
    seam::pg_attribute_aclcheck::set(|table_oid, attnum, roleid, mode| {
        let ctx = MemoryContext::new("pg_attribute_aclcheck");
        pg_attribute_aclcheck(ctx.mcx(), table_oid, attnum, roleid, mode)
    });
    seam::pg_attribute_aclcheck_ext::set(|table_oid, attnum, roleid, mode| {
        let ctx = MemoryContext::new("pg_attribute_aclcheck_ext");
        pg_attribute_aclcheck_ext(ctx.mcx(), table_oid, attnum, roleid, mode, Some(()))
    });
    seam::pg_attribute_aclcheck_all::set(|table_oid, roleid, mode, how| {
        let ctx = MemoryContext::new("pg_attribute_aclcheck_all");
        pg_attribute_aclcheck_all(ctx.mcx(), table_oid, roleid, mode, how)
    });
    seam::pg_attribute_aclcheck_all_ext::set(|table_oid, roleid, mode, how| {
        let ctx = MemoryContext::new("pg_attribute_aclcheck_all_ext");
        pg_attribute_aclcheck_all_ext(ctx.mcx(), table_oid, roleid, mode, how, Some(()))
    });
    seam::pg_parameter_aclcheck::set(|name, roleid, mode| {
        let ctx = MemoryContext::new("pg_parameter_aclcheck");
        pg_parameter_aclcheck(ctx.mcx(), &name, roleid, mode)
    });
    seam::pg_largeobject_aclcheck_snapshot::set(|lobj_oid, roleid, mode, snapshot| {
        let ctx = MemoryContext::new("pg_largeobject_aclcheck_snapshot");
        pg_largeobject_aclcheck_snapshot(ctx.mcx(), lobj_oid, roleid, mode, snapshot)
    });
    seam::object_ownercheck::set(|classid, objectid, roleid| {
        let ctx = MemoryContext::new("object_ownercheck");
        object_ownercheck(ctx.mcx(), classid, objectid, roleid)
    });
    seam::aclcheck_error::set(aclcheck_error);
    seam::aclcheck_error_col::set(aclcheck_error_col);
    seam::aclcheck_error_type::set(|aclerr, type_oid| {
        let ctx = MemoryContext::new("aclcheck_error_type");
        aclcheck_error_type(ctx.mcx(), aclerr, type_oid)
    });
    seam::error_conflicting_def_elem::set(errorConflictingDefElem);

    // functioncmds.c (CreateFunction / CreateCast) reaches the namespace/type
    // ACL + ownership checks through its own outward seam crate. Their real
    // owner is aclchk.c; install them here, mirroring the C call shapes
    // (`object_aclcheck(NamespaceRelationId/TypeRelationId, ...)`,
    // `object_ownercheck(TypeRelationId, ...)`,
    // `aclcheck_error(aclresult, OBJECT_SCHEMA, ...)`).
    {
        use backend_commands_functioncmds_seams as fc;
        fc::namespace_aclcheck::set(|namespace_id, role_id, mode| {
            let ctx = MemoryContext::new("functioncmds namespace_aclcheck");
            object_aclcheck(ctx.mcx(), NamespaceRelationId, namespace_id, role_id, mode)
        });
        fc::type_aclcheck::set(|type_id, role_id, mode| {
            let ctx = MemoryContext::new("functioncmds type_aclcheck");
            object_aclcheck(ctx.mcx(), TypeRelationId, type_id, role_id, mode)
        });
        fc::language_aclcheck::set(|lang_oid, role_id, mode| {
            let ctx = MemoryContext::new("functioncmds language_aclcheck");
            object_aclcheck(ctx.mcx(), LanguageRelationId, lang_oid, role_id, mode)
        });
        fc::type_ownercheck::set(|type_id, role_id| {
            let ctx = MemoryContext::new("functioncmds type_ownercheck");
            object_ownercheck(ctx.mcx(), TypeRelationId, type_id, role_id)
        });
        // `object_ownercheck(ProcedureRelationId, funcoid, roleid)` — the CREATE
        // CAST `pg_proc_ownercheck(funcid, ...)` permission check
        // (functioncmds.c cast path). Body owned here; cross-install onto the
        // functioncmds-seams declaration, mirroring `type_ownercheck`.
        fc::proc_ownercheck::set(|func_oid, role_id| {
            let ctx = MemoryContext::new("functioncmds proc_ownercheck");
            object_ownercheck(ctx.mcx(), ProcedureRelationId, func_oid, role_id)
        });
        // `object_aclcheck(ProcedureRelationId, funcoid, roleid, mode)` — the CALL
        // (functioncmds.c `ExecuteCallStmt`) EXECUTE-privilege check and the
        // CREATE CAST function-EXECUTE check. Body owned here; cross-install onto
        // the functioncmds-seams declaration, mirroring `proc_ownercheck`.
        fc::proc_aclcheck::set(|func_oid, role_id, mode| {
            let ctx = MemoryContext::new("functioncmds proc_aclcheck");
            object_aclcheck(ctx.mcx(), ProcedureRelationId, func_oid, role_id, mode)
        });
        fc::aclcheck_error_function::set(|aclresult, objname| {
            aclcheck_error(aclresult, ObjectType::Function, Some(objname))
        });
        // `get_func_name(funcid)` — the procedure name for the CALL ACL error.
        // Body owned by lsyscache (`get_func_name`); cross-install onto the
        // functioncmds-seams declaration.
        fc::get_func_name::set(|func_oid| {
            let ctx = MemoryContext::new("functioncmds get_func_name");
            let name = backend_utils_cache_lsyscache_seams::get_func_name::call(ctx.mcx(), func_oid)?;
            Ok(name.map(|s| s.as_str().to_string()))
        });
        fc::aclcheck_error_schema::set(|aclresult, objname| {
            aclcheck_error(aclresult, ObjectType::Schema, objname)
        });
    }

    // ExecuteGrantStmt (F2): the GRANT/REVOKE executor, bounded to the
    // OBJECT_SCHEMA path. Installed onto the utility slow-path out-seam.
    backend_tcop_utility_out_seams::execute_grant_stmt_slow::set(|mcx, stmt| {
        grant_exec::execute_grant_stmt(mcx, stmt)
    });

    // ExecuteGrantStmt (fast utility path): the same GRANT/REVOKE executor
    // installed onto the non-event-trigger out-seam (utility.c
    // `ProcessUtilitySlow` fast leg / standard_ProcessUtility GrantStmt arm).
    backend_tcop_utility_out_seams::execute_grant_stmt::set(|mcx, stmt| {
        grant_exec::execute_grant_stmt(mcx, stmt)
    });

    // ExecAlterDefaultPrivilegesStmt: the ALTER DEFAULT PRIVILEGES executor.
    // Parses the statement and writes/updates a pg_default_acl row per
    // (role, schema) combination via SetDefaultACL. (`pstate` is only used by
    // the C body for error-position reporting on the unrecognized-option path,
    // which we report without a cursor position.)
    backend_tcop_utility_out_seams::exec_alter_default_privileges_stmt::set(|mcx, _pstate, stmt| {
        grant_exec::exec_alter_default_privileges_stmt(mcx, stmt)
    });

    // `aclnewowner(...)` + `PointerGetDatum`: the on-disk relacl/objacl owner
    // rewrite the catalog owner-change paths (ATExecChangeOwner & friends) use
    // when the ACL column is non-null.
    seam::acl_change_owner_datum::set(|mcx, acl_on_disk, old_owner, new_owner| {
        grant_exec::acl_change_owner_datum(mcx, acl_on_disk, old_owner, new_owner)
    });

    // get_user_default_acl: full default-ACL lookup/merge; the result carries
    // the on-disk aclitem[] varlena Datum in the caller's `mcx`.
    seam::get_user_default_acl::set(|mcx, objtype, owner_id, nsp_oid| {
        get_user_default_acl(mcx, objtype, owner_id, nsp_oid)
    });

    // `recordDependencyOnNewAcl(classId, objectId, objsubId, ownerId, acl)`
    // (aclchk.c): record pg_shdepend dependencies on every role mentioned in a
    // freshly-created object's ACL. The C body is:
    //
    //   if (acl == NULL) return;            /* defaulted ACL: nothing to do */
    //   nmembers = aclmembers(acl, &members);
    //   updateAclDependencies(classId, objectId, objsubId, ownerId,
    //                         0, NULL, nmembers, members);
    //
    // The `acl == NULL` fast path is the common case (a plain CREATE TABLE /
    // CREATE TYPE with no `ALTER DEFAULT PRIVILEGES` in force): the object is
    // created with a defaulted (NULL stored) ACL, so there is nothing to record.
    seam::record_dependency_on_new_acl::set(
        |mcx, class_id, object_id, objsub_id, owner_id, acl| {
            // C: `if (acl == NULL) return;`
            let Some(acl) = acl else {
                return Ok(());
            };
            let raw = match &acl {
                TupDatum::ByRef(b) => &b[..],
                _ => {
                    return Err(PgError::error(
                        "recordDependencyOnNewAcl: ACL is not a varlena",
                    ))
                }
            };
            let items = grant_exec::decode_acl(mcx, raw)?;

            // nmembers = aclmembers(acl, &members);
            let members = backend_utils_adt_acl::acl_ops::aclmembers(mcx, items)?;
            let mut new_members: mcx::PgVec<Oid> =
                mcx::vec_with_capacity_in(mcx, members.len())?;
            for m in members.iter() {
                new_members.push(*m);
            }
            let old_members: mcx::PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 0)?;

            // updateAclDependencies(classId, objectId, objsubId, ownerId,
            //                       0, NULL, nmembers, members);
            backend_catalog_pg_shdepend_seams::updateAclDependencies::call(
                mcx,
                class_id,
                object_id,
                objsub_id,
                owner_id,
                old_members,
                new_members,
            )
        },
    );

    // `RemoveRoleFromObjectACL(roleid, classid, objid)` (aclchk.c:1423) — the
    // DROP OWNED ACL-revoke leg. Now ported: it routes through the GRANT
    // executor (`ExecGrantStmt_oids` / `SetDefaultACL`), both of which are
    // ported in `grant_exec`.
    seam::remove_role_from_object_acl::set(|roleid, classid, objid| {
        let ctx = MemoryContext::new("remove_role_from_object_acl");
        grant_exec::remove_role_from_object_acl(ctx.mcx(), roleid, classid, objid)
    });

    // NOTE (F3 STOP): the two init-privs seams (remove_role_from_init_priv,
    // replace_role_in_init_priv) remain deliberately NOT installed; the CATALOG
    // row is held at `scaffold` so the seam-install guard exempts that
    // unfinished surface.
    // collationcmds.c (ALTER COLLATION owner check) — `object_ownercheck(
    // CollationRelationId, ...)` and `aclcheck_error(ACLCHECK_NOT_OWNER,
    // OBJECT_COLLATION, ...)`.
    backend_commands_collationcmds_seams::collation_ownercheck::set(|coll_oid, roleid| {
        let ctx = MemoryContext::new("collation_ownercheck");
        object_ownercheck(ctx.mcx(), CollationRelationId, coll_oid, roleid)
    });
    backend_commands_collationcmds_seams::aclcheck_error_not_owner_collation::set(|collname| {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            types_nodes::parsenodes::ObjectType::Collation,
            Some(collname),
        )
    });

    // NOTE (F1 STOP): get_user_default_acl, record_dependency_on_new_acl
    // (ArrayType-payload + DEFACLROLENSPOBJ blocked) and the three F2/F3
    // executor/init-privs seams (remove_role_from_object_acl,
    // remove_role_from_init_priv, replace_role_in_init_priv) are deliberately
    // NOT installed; the CATALOG row is held at `scaffold` so the seam-install
    // guard exempts this unfinished surface.
}
