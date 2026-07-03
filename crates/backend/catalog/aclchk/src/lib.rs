#![allow(non_snake_case)]

use adt_acl::{
    acldefault, aclmask, has_privs_of_role, AclMaskHow, AclObjectType, ACL_DELETE, ACL_INSERT,
    ACL_MAINTAIN, ACL_SELECT, ACL_SET, ACL_TRUNCATE, ACL_UPDATE, ACL_USAGE,
};
use cache_syscache::cacheinfo::{DATABASEOID, PARAMETERACLNAME, PROCOID, RELOID};
use cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheGetAttrNotNull, SysCacheKey,
};
use datum::Datum;
use types_core::catalog::{
    FirstUnpinnedObjectId, BOOTSTRAP_SUPERUSERID, DATABASE_RELATION_ID, NAMESPACE_RELATION_ID,
    PG_TOAST_NAMESPACE, PROCEDURE_RELATION_ID, RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_TABLE};
use types_rel::{RELKIND_SEQUENCE, RELKIND_VIEW};

pub const ACLCHECK_OK: i32 = 0;
pub const ACLCHECK_NO_PRIV: i32 = 1;
pub const ACLCHECK_NOT_OWNER: i32 = 2;

const ANUM_PG_DATABASE_DATDBA: i32 = 3;
const ANUM_PG_DATABASE_DATACL: i32 = 18;
const ANUM_PG_PROC_PROOWNER: i32 = 4;
const ANUM_PG_PROC_PROACL: i32 = 30;
const ANUM_PG_PARAMETER_ACL_PARACL: i32 = 3;
const ANUM_PG_CLASS_RELNAMESPACE: i32 = 3;
const ANUM_PG_CLASS_RELOWNER: i32 = 6;
const ANUM_PG_CLASS_RELKIND: i32 = 18;
const ANUM_PG_CLASS_RELACL: i32 = 32;
const ROLE_PG_READ_ALL_DATA: Oid = 6181;
const ROLE_PG_WRITE_ALL_DATA: Oid = 6182;
const ROLE_PG_MAINTAIN: Oid = 6337;

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
        // pg_namespace_aclmask_ext's superuser fast path; the nspacl decode
        // (non-superuser roles) is the unported remainder.
        NAMESPACE_RELATION_ID => {
            if superuser::superuser_arg(roleid)? {
                return Ok(mask);
            }
            panic!("object_aclmask: pg_namespace_aclmask nspacl arm unported (non-superuser)")
        }
        TYPE_RELATION_ID => panic!("object_aclmask: pg_type_aclmask unported"),
        // C divergence: C asserts callers use pg_class_aclmask directly; the
        // executor consumes it through the object_aclcheck seam, so route it.
        RELATION_RELATION_ID => return pg_class_aclmask(objectid, roleid, mask, how),
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

pub fn pg_class_aclcheck(table_oid: Oid, roleid: Oid, mode: u64) -> PgResult<i32> {
    if pg_class_aclmask(table_oid, roleid, mode, AclMaskHow::AclmaskAny)? != 0 {
        Ok(ACLCHECK_OK)
    } else {
        Ok(ACLCHECK_NO_PRIV)
    }
}

pub fn pg_class_aclmask(table_oid: Oid, roleid: Oid, mask: u64, how: AclMaskHow) -> PgResult<u64> {
    let mut mask = mask;
    let Some(tuple) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(table_oid)))?
    else {
        return Err(undefined_table(table_oid));
    };

    let relkind = SysCacheGetAttrNotNull(RELOID, &tuple, ANUM_PG_CLASS_RELKIND)?.as_u8();
    // Only rolsuper may write system catalogs (updatable system views exempt).
    const SYSTEM_WRITE: u64 = ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE;
    if mask & SYSTEM_WRITE != 0 {
        // IsSystemClass (catalog.c, unported): toast namespace or pinned oid.
        let relnamespace =
            SysCacheGetAttrNotNull(RELOID, &tuple, ANUM_PG_CLASS_RELNAMESPACE)?.as_oid();
        let is_system_class =
            relnamespace == PG_TOAST_NAMESPACE || table_oid < FirstUnpinnedObjectId;
        if is_system_class && relkind != RELKIND_VIEW && !superuser::superuser_arg(roleid)? {
            mask &= !SYSTEM_WRITE;
        }
    }

    if superuser::superuser_arg(roleid)? {
        ReleaseSysCache(tuple);
        return Ok(mask);
    }

    let owner_id = SysCacheGetAttrNotNull(RELOID, &tuple, ANUM_PG_CLASS_RELOWNER)?.as_oid();
    let (_, isnull) = SysCacheGetAttr(RELOID, &tuple, ANUM_PG_CLASS_RELACL)?;
    let mut result = if isnull {
        let objtype = if relkind == RELKIND_SEQUENCE {
            AclObjectType::Sequence
        } else {
            AclObjectType::Table
        };
        aclmask(acldefault(objtype, owner_id).as_slice(), roleid, owner_id, mask, how)?
    } else {
        panic!("pg_class_aclmask: non-NULL relacl — Acl varlena parsing unported");
    };
    ReleaseSysCache(tuple);

    if mask & ACL_SELECT != 0
        && result & ACL_SELECT == 0
        && has_privs_of_role(roleid, ROLE_PG_READ_ALL_DATA)?
    {
        result |= ACL_SELECT;
    }
    const WRITE: u64 = ACL_INSERT | ACL_UPDATE | ACL_DELETE;
    if mask & WRITE != 0 && result & WRITE == 0 && has_privs_of_role(roleid, ROLE_PG_WRITE_ALL_DATA)?
    {
        result |= mask & WRITE;
    }
    if mask & ACL_MAINTAIN != 0
        && result & ACL_MAINTAIN == 0
        && has_privs_of_role(roleid, ROLE_PG_MAINTAIN)?
    {
        result |= ACL_MAINTAIN;
    }
    Ok(result)
}

#[cold]
#[inline(never)]
fn undefined_table(table_oid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("relation with OID {table_oid} does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_TABLE),
    )
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
