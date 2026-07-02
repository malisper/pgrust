#![allow(non_snake_case)]

use adt_acl::{acldefault, aclmask, AclMaskHow, AclObjectType, ACL_SET};
use cache_syscache::cacheinfo::{DATABASEOID, PARAMETERACLNAME, PROCOID};
use cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheGetAttrNotNull, SysCacheKey,
};
use datum::Datum;
use types_core::catalog::{
    BOOTSTRAP_SUPERUSERID, DATABASE_RELATION_ID, NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID,
    RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_core::Oid;
use types_error::PgResult;

pub const ACLCHECK_OK: i32 = 0;
pub const ACLCHECK_NO_PRIV: i32 = 1;
pub const ACLCHECK_NOT_OWNER: i32 = 2;

const ANUM_PG_DATABASE_DATDBA: i32 = 3;
const ANUM_PG_DATABASE_DATACL: i32 = 18;
const ANUM_PG_PROC_PROOWNER: i32 = 4;
const ANUM_PG_PROC_PROACL: i32 = 30;
const ANUM_PG_PARAMETER_ACL_PARACL: i32 = 3;

pub fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: u64) -> PgResult<i32> {
    if object_aclmask(classid, objectid, roleid, mode, AclMaskHow::AclmaskAny)? != 0 {
        Ok(ACLCHECK_OK)
    } else {
        Ok(ACLCHECK_NO_PRIV)
    }
}

fn object_aclmask(
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mask: u64,
    how: AclMaskHow,
) -> PgResult<u64> {
    match classid {
        NAMESPACE_RELATION_ID => panic!("object_aclmask: pg_namespace_aclmask unported"),
        TYPE_RELATION_ID => panic!("object_aclmask: pg_type_aclmask unported"),
        RELATION_RELATION_ID => panic!("object_aclmask: pg_class should use pg_class_aclmask"),
        _ => {}
    }

    // Superusers bypass all permission checking.
    if superuser::superuser_arg(roleid)? {
        return Ok(mask);
    }

    // objectaddress.c's ObjectProperty table, reduced to the live classids.
    let (cacheid, owner_attnum, acl_attnum, objtype, descr) = match classid {
        DATABASE_RELATION_ID => (
            DATABASEOID,
            ANUM_PG_DATABASE_DATDBA,
            ANUM_PG_DATABASE_DATACL,
            AclObjectType::Database,
            "database",
        ),
        PROCEDURE_RELATION_ID => (
            PROCOID,
            ANUM_PG_PROC_PROOWNER,
            ANUM_PG_PROC_PROACL,
            AclObjectType::Function,
            "function",
        ),
        _ => panic!("object_aclmask: classid {classid} unported (ObjectProperty table)"),
    };

    let Some(tuple) = SearchSysCache1(cacheid, SysCacheKey::Value(Datum::from_oid(objectid)))?
    else {
        return Err(types_error::PgError::error(format!(
            "cache lookup failed for {descr} {objectid}"
        ))
        .into());
    };

    let owner_id = SysCacheGetAttrNotNull(cacheid, &tuple, owner_attnum)?.as_oid();
    let (_, isnull) = SysCacheGetAttr(cacheid, &tuple, acl_attnum)?;
    let result = if isnull {
        aclmask(acldefault(objtype, owner_id).as_slice(), roleid, owner_id, mask, how)?
    } else {
        panic!("object_aclmask: non-NULL {descr} ACL — Acl varlena parsing unported");
    };
    ReleaseSysCache(tuple);
    Ok(result)
}

pub fn pg_parameter_aclcheck(name: &str, roleid: Oid, mode: u64) -> PgResult<i32> {
    if pg_parameter_aclmask(name, roleid, mode, AclMaskHow::AclmaskAny)? != 0 {
        Ok(ACLCHECK_OK)
    } else {
        Ok(ACLCHECK_NO_PRIV)
    }
}

fn pg_parameter_aclmask(name: &str, roleid: Oid, mask: u64, how: AclMaskHow) -> PgResult<u64> {
    if superuser::superuser_arg(roleid)? {
        return Ok(mask);
    }

    let parname = guc::convert_guc_name_for_parameter_acl(name);
    let Some(tuple) = SearchSysCache1(PARAMETERACLNAME, SysCacheKey::Str(&parname))? else {
        // No entry: the GUC has no permissions for non-superusers.
        return Ok(0);
    };

    let (_, isnull) = SysCacheGetAttr(PARAMETERACLNAME, &tuple, ANUM_PG_PARAMETER_ACL_PARACL)?;
    let result = if isnull {
        aclmask(
            acldefault(AclObjectType::ParameterAcl, BOOTSTRAP_SUPERUSERID).as_slice(),
            roleid,
            BOOTSTRAP_SUPERUSERID,
            mask,
            how,
        )?
    } else {
        panic!("pg_parameter_aclmask: non-NULL paracl — Acl varlena parsing unported");
    };
    ReleaseSysCache(tuple);
    Ok(result)
}

fn pg_parameter_aclcheck_set(name: &str, roleid: Oid) -> PgResult<bool> {
    Ok(pg_parameter_aclcheck(name, roleid, ACL_SET)? == ACLCHECK_OK)
}

pub fn init_seams() {
    aclchk_seams::object_aclcheck::set(object_aclcheck);
    aclchk_seams::pg_parameter_aclcheck_set::set(pg_parameter_aclcheck_set);
}

#[cfg(test)]
mod tests;
