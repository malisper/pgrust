#![allow(non_snake_case)]

use std::cell::RefCell;

use adt_acl::{
    acldefault, aclmask, has_privs_of_role, AclItem, AclMaskHow, AclObjectType, ACL_DELETE,
    ACL_INSERT, ACL_MAINTAIN, ACL_SELECT, ACL_SET, ACL_TRUNCATE, ACL_UPDATE, ACL_USAGE,
};
use cache_syscache::cacheinfo::{
    ATTNUM, AUTHOID, DATABASEOID, EXTENSIONOID, FOREIGNDATAWRAPPEROID, FOREIGNSERVEROID, LANGOID, PARAMETERACLNAME, PARAMETERACLOID, PROCOID, RELOID, TYPEOID,
};
use cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SearchSysCache2, SysCacheGetAttr, SysCacheGetAttrNotNull,
    SysCacheKey,
};
use datum::Datum;
use types_core::catalog::{
    FirstUnpinnedObjectId, BOOTSTRAP_SUPERUSERID, DATABASE_RELATION_ID, EXTENSION_RELATION_ID,
    LANGUAGE_RELATION_ID, NAMESPACE_RELATION_ID, PG_TOAST_NAMESPACE, PROCEDURE_RELATION_ID,
    RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_UNDEFINED_TABLE,
};
use types_nodes::parsenodes::ObjectType;
use types_rel::{RELKIND_SEQUENCE, RELKIND_VIEW};

mod defacl;
pub use defacl::{get_user_default_acl, ExecAlterDefaultPrivilegesStmt, DefaultAclRelationId};
mod grant;
pub use grant::{
    get_rolespec_oid, ExecuteGrantStmt, RemoveRoleFromInitPriv, RemoveRoleFromObjectACL,
};
mod lo;
pub use lo::{object_ownercheck_lo, pg_largeobject_aclcheck_snapshot};

pub const ACLCHECK_OK: i32 = 0;
pub const ACLCHECK_NO_PRIV: i32 = 1;
pub const ACLCHECK_NOT_OWNER: i32 = 2;

const ANUM_PG_DATABASE_DATDBA: i32 = 3;
const ANUM_PG_DATABASE_DATACL: i32 = 18;
const ANUM_PG_PROC_PROOWNER: i32 = 4;
const ANUM_PG_FOREIGN_DATA_WRAPPER_FDWOWNER: i32 = 3;
const ANUM_PG_FOREIGN_DATA_WRAPPER_FDWACL: i32 = 6;
const ANUM_PG_FOREIGN_SERVER_SRVOWNER: i32 = 3;
const ANUM_PG_FOREIGN_SERVER_SRVACL: i32 = 7;
const ANUM_PG_PROC_PROACL: i32 = 30;
const ANUM_PG_TYPE_TYPOWNER: i32 = 4;
pub(crate) const ANUM_PG_TYPE_TYPTYPE: i32 = 7;
const ANUM_PG_TYPE_TYPSUBSCRIPT: i32 = 13;
const ANUM_PG_TYPE_TYPELEM: i32 = 14;
const ANUM_PG_TYPE_TYPACL: i32 = 32;
const ANUM_PG_LANGUAGE_LANOWNER: i32 = 3;
const ANUM_PG_LANGUAGE_LANACL: i32 = 9;
const ANUM_PG_PARAMETER_ACL_PARACL: i32 = 3;
const ANUM_PG_CLASS_RELNAMESPACE: i32 = 3;
const ANUM_PG_CLASS_RELOWNER: i32 = 6;
const ANUM_PG_CLASS_RELKIND: i32 = 18;
pub(crate) const ANUM_PG_CLASS_RELNATTS: i32 = 19;
pub(crate) const ANUM_PG_CLASS_RELACL: i32 = 32;
const ANUM_PG_AUTHID_ROLBYPASSRLS: i32 = 9;
const ANUM_PG_AUTHID_ROLCREATEROLE: i32 = 5;
const ANUM_PG_ATTRIBUTE_ATTISDROPPED: i32 = 17;
const ANUM_PG_ATTRIBUTE_ATTACL: i32 = 22;
const ROLE_PG_READ_ALL_DATA: Oid = 6181;
const ROLE_PG_WRITE_ALL_DATA: Oid = 6182;
const ROLE_PG_MAINTAIN: Oid = 6337;

thread_local! {
    // Decode scratch for stored ACLs (retained capacity; C detoasts+pallocs).
    static ACL_SCRATCH: RefCell<Vec<AclItem>> = const { RefCell::new(Vec::new()) };
}

// DatumGetAclP without the container copy: run `f` over the decoded items of
// a stored aclitem[]. `d` must come from SysCacheGetAttr on a still-held
// tuple (non-null).
pub fn with_acl_datum<R>(
    d: Datum,
    f: impl FnOnce(&[AclItem]) -> PgResult<R>,
) -> PgResult<R> {
    use types_tuple::varatt;
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract — `d` points at a live varlena inside a held
    // catalog tuple.
    let payload: &[u8] = unsafe {
        if varatt::varatt_is_1b_e(p) || (!varatt::varatt_is_1b(p) && !varatt::varatt_is_4b_u(p)) {
            // pg_class/pg_attribute have no toast tables; only inline
            // compression can appear here.
            panic!("aclchk: compressed/external ACL varlena — detoast gap");
        }
        if varatt::varatt_is_1b(p) {
            core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            )
        } else {
            core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ),
                varatt::varsize_4b(p) - varatt::VARHDRSZ,
            )
        }
    };
    let n = adt_acl::varlena::check_acl_payload(payload)?;
    ACL_SCRATCH.with(|s| {
        let mut v = s.borrow_mut();
        v.clear();
        v.reserve(n);
        for i in 0..n {
            v.push(adt_acl::varlena::read_acl_item(payload, i));
        }
        f(&v)
    })
}

pub fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: u64) -> PgResult<i32> {
    if object_aclmask(classid, objectid, roleid, mode, AclMaskHow::AclmaskAny)? != 0 {
        Ok(ACLCHECK_OK)
    } else {
        Ok(ACLCHECK_NO_PRIV)
    }
}

// object_aclcheck_ext (aclchk.c): (AclResult, is_missing).
pub fn object_aclcheck_ext(
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mode: u64,
) -> PgResult<(i32, bool)> {
    let mut is_missing = false;
    let m = object_aclmask_ext(
        classid,
        objectid,
        roleid,
        mode,
        AclMaskHow::AclmaskAny,
        Some(&mut is_missing),
    )?;
    let r = if m != 0 { ACLCHECK_OK } else { ACLCHECK_NO_PRIV };
    Ok((r, is_missing))
}

fn object_aclmask(
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mask: u64,
    how: AclMaskHow,
) -> PgResult<u64> {
    object_aclmask_ext(classid, objectid, roleid, mask, how, None)
}

fn object_aclmask_ext(
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mask: u64,
    how: AclMaskHow,
    is_missing: Option<&mut bool>,
) -> PgResult<u64> {
    match classid {
        NAMESPACE_RELATION_ID => {
            return pg_namespace_aclmask_ext(objectid, roleid, mask, how, is_missing)
        }
        TYPE_RELATION_ID => return pg_type_aclmask_ext(objectid, roleid, mask, how, is_missing),
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
        LANGUAGE_RELATION_ID => (
            LANGOID,
            ANUM_PG_LANGUAGE_LANOWNER,
            ANUM_PG_LANGUAGE_LANACL,
            AclObjectType::Language,
            "language",
        ),
        types_core::catalog::TABLE_SPACE_RELATION_ID => (
            cache_syscache::cacheinfo::TABLESPACEOID,
            3,
            4,
            AclObjectType::Tablespace,
            "tablespace",
        ),
        types_core::FOREIGN_DATA_WRAPPER_RELATION_ID => (
            FOREIGNDATAWRAPPEROID,
            ANUM_PG_FOREIGN_DATA_WRAPPER_FDWOWNER,
            ANUM_PG_FOREIGN_DATA_WRAPPER_FDWACL,
            AclObjectType::Fdw,
            "foreign-data wrapper",
        ),
        types_core::FOREIGN_SERVER_RELATION_ID => (
            FOREIGNSERVEROID,
            ANUM_PG_FOREIGN_SERVER_SRVOWNER,
            ANUM_PG_FOREIGN_SERVER_SRVACL,
            AclObjectType::ForeignServer,
            "foreign server",
        ),
        _ => panic!("object_aclmask: classid {classid} unported (ObjectProperty table)"),
    };

    let Some(tuple) = SearchSysCache1(cacheid, SysCacheKey::Value(Datum::from_oid(objectid)))?
    else {
        if let Some(m) = is_missing {
            *m = true;
            return Ok(0);
        }
        return Err(types_error::PgError::error(format!(
            "cache lookup failed for {descr} {objectid}"
        ))
        .into());
    };

    let owner_id = SysCacheGetAttrNotNull(cacheid, &tuple, owner_attnum)?.as_oid();
    let (acl_datum, isnull) = SysCacheGetAttr(cacheid, &tuple, acl_attnum)?;
    let result = if isnull {
        aclmask(acldefault(objtype, owner_id).as_slice(), roleid, owner_id, mask, how)?
    } else {
        with_acl_datum(acl_datum, |acl| aclmask(acl, roleid, owner_id, mask, how))?
    };
    ReleaseSysCache(tuple);
    Ok(result)
}

#[track_caller]
#[cold]
#[inline(never)]
fn undefined_type(type_oid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("type with OID {type_oid} does not exist"))
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
    )
}

// pg_type_aclmask_ext (aclchk.c): true array types and multiranges consult
// the element / range type's ACL.
fn pg_type_aclmask_ext(
    type_oid: Oid,
    roleid: Oid,
    mask: u64,
    how: AclMaskHow,
    mut is_missing: Option<&mut bool>,
) -> PgResult<u64> {
    const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;
    const TYPTYPE_MULTIRANGE: u8 = b'm';
    const ANUM_PG_RANGE_RNGTYPID: i32 = 1;

    if superuser::superuser_arg(roleid)? {
        return Ok(mask);
    }

    let mut type_oid = type_oid;
    let mut tuple = match SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(type_oid)))? {
        Some(t) => t,
        None => {
            if let Some(m) = is_missing.as_deref_mut() {
                *m = true;
                return Ok(0);
            }
            return Err(undefined_type(type_oid));
        }
    };

    let typelem = SysCacheGetAttrNotNull(TYPEOID, &tuple, ANUM_PG_TYPE_TYPELEM)?.as_oid();
    let typsubscript =
        SysCacheGetAttrNotNull(TYPEOID, &tuple, ANUM_PG_TYPE_TYPSUBSCRIPT)?.as_oid();
    if typelem != Oid::default() && typsubscript == F_ARRAY_SUBSCRIPT_HANDLER {
        ReleaseSysCache(tuple);
        type_oid = typelem;
        tuple = match SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(type_oid)))? {
            Some(t) => t,
            None => {
                if let Some(m) = is_missing.as_deref_mut() {
                    *m = true;
                    return Ok(0);
                }
                return Err(undefined_type(type_oid));
            }
        };
    }

    let typtype = SysCacheGetAttrNotNull(TYPEOID, &tuple, ANUM_PG_TYPE_TYPTYPE)?.as_u8();
    if typtype == TYPTYPE_MULTIRANGE {
        use cache_syscache::cacheinfo::RANGEMULTIRANGE;
        ReleaseSysCache(tuple);
        // get_multirange_range (lsyscache.c): InvalidOid when missing; the
        // pg_type probe below then reports it.
        type_oid = match SearchSysCache1(
            RANGEMULTIRANGE,
            SysCacheKey::Value(Datum::from_oid(type_oid)),
        )? {
            Some(range_tuple) => {
                let r = SysCacheGetAttrNotNull(RANGEMULTIRANGE, &range_tuple, ANUM_PG_RANGE_RNGTYPID)?
                    .as_oid();
                ReleaseSysCache(range_tuple);
                r
            }
            None => Oid::default(),
        };
        tuple = match SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(type_oid)))? {
            Some(t) => t,
            None => {
                if let Some(m) = is_missing.as_deref_mut() {
                    *m = true;
                    return Ok(0);
                }
                return Err(undefined_type(type_oid));
            }
        };
    }

    let owner_id = SysCacheGetAttrNotNull(TYPEOID, &tuple, ANUM_PG_TYPE_TYPOWNER)?.as_oid();
    let (acl_datum, isnull) = SysCacheGetAttr(TYPEOID, &tuple, ANUM_PG_TYPE_TYPACL)?;
    let result = if isnull {
        aclmask(
            acldefault(AclObjectType::Type, owner_id).as_slice(),
            roleid,
            owner_id,
            mask,
            how,
        )?
    } else {
        with_acl_datum(acl_datum, |acl| aclmask(acl, roleid, owner_id, mask, how))?
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

// pg_class_aclcheck_ext (aclchk.c): (AclResult, is_missing).
pub fn pg_class_aclcheck_ext(table_oid: Oid, roleid: Oid, mode: u64) -> PgResult<(i32, bool)> {
    let mut is_missing = false;
    let m = pg_class_aclmask_ext(
        table_oid,
        roleid,
        mode,
        AclMaskHow::AclmaskAny,
        Some(&mut is_missing),
    )?;
    let r = if m != 0 { ACLCHECK_OK } else { ACLCHECK_NO_PRIV };
    Ok((r, is_missing))
}

pub fn pg_class_aclmask(table_oid: Oid, roleid: Oid, mask: u64, how: AclMaskHow) -> PgResult<u64> {
    pg_class_aclmask_ext(table_oid, roleid, mask, how, None)
}

pub fn pg_class_aclmask_ext(
    table_oid: Oid,
    roleid: Oid,
    mask: u64,
    how: AclMaskHow,
    is_missing: Option<&mut bool>,
) -> PgResult<u64> {
    let mut mask = mask;
    let Some(tuple) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(table_oid)))?
    else {
        if let Some(m) = is_missing {
            *m = true;
            return Ok(0);
        }
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
    let (acl_datum, isnull) = SysCacheGetAttr(RELOID, &tuple, ANUM_PG_CLASS_RELACL)?;
    let mut result = if isnull {
        let objtype = if relkind == RELKIND_SEQUENCE {
            AclObjectType::Sequence
        } else {
            AclObjectType::Table
        };
        aclmask(acldefault(objtype, owner_id).as_slice(), roleid, owner_id, mask, how)?
    } else {
        with_acl_datum(acl_datum, |acl| aclmask(acl, roleid, owner_id, mask, how))?
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

#[track_caller]
#[cold]
#[inline(never)]
fn undefined_table(table_oid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("relation with OID {table_oid} does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_TABLE),
    )
}

// pg_namespace_aclmask_ext (aclchk.c).
fn pg_namespace_aclmask_ext(
    nsp_oid: Oid,
    roleid: Oid,
    mask: u64,
    how: AclMaskHow,
    is_missing: Option<&mut bool>,
) -> PgResult<u64> {
    const ANUM_PG_NAMESPACE_NSPOWNER: i32 = 3;
    const ANUM_PG_NAMESPACE_NSPACL: i32 = 4;
    use cache_syscache::cacheinfo::NAMESPACEOID;

    if superuser::superuser_arg(roleid)? {
        return Ok(mask);
    }

    // A temp namespace acts as all-schema-rights with CREATE TEMP on the
    // database, else USAGE only (current user may differ from the creator).
    if catalog_namespace::isTempNamespace(nsp_oid) {
        let db = init_small::globals::MyDatabaseId();
        if object_aclcheck(DATABASE_RELATION_ID, db, roleid, adt_acl::ACL_CREATE_TEMP)?
            == ACLCHECK_OK
        {
            return Ok(mask & adt_acl::ACL_ALL_RIGHTS_SCHEMA);
        }
        return Ok(mask & ACL_USAGE);
    }

    let Some(tuple) = SearchSysCache1(NAMESPACEOID, SysCacheKey::Value(Datum::from_oid(nsp_oid)))?
    else {
        if let Some(m) = is_missing {
            *m = true;
            return Ok(0);
        }
        return Err(Box::new(
            PgError::error(format!("schema with OID {nsp_oid} does not exist"))
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_SCHEMA),
        ));
    };
    let owner_id = SysCacheGetAttrNotNull(NAMESPACEOID, &tuple, ANUM_PG_NAMESPACE_NSPOWNER)?.as_oid();
    let (acl_datum, isnull) = SysCacheGetAttr(NAMESPACEOID, &tuple, ANUM_PG_NAMESPACE_NSPACL)?;
    let mut result = if isnull {
        aclmask(
            acldefault(AclObjectType::Schema, owner_id).as_slice(),
            roleid,
            owner_id,
            mask,
            how,
        )?
    } else {
        with_acl_datum(acl_datum, |acl| aclmask(acl, roleid, owner_id, mask, how))?
    };
    ReleaseSysCache(tuple);

    // pg_read_all_data/pg_write_all_data imply USAGE on every schema.
    if mask & ACL_USAGE != 0
        && result & ACL_USAGE == 0
        && (has_privs_of_role(roleid, ROLE_PG_READ_ALL_DATA)?
            || has_privs_of_role(roleid, ROLE_PG_WRITE_ALL_DATA)?)
    {
        result |= ACL_USAGE;
    }
    Ok(result)
}

pub fn pg_attribute_aclcheck(table_oid: Oid, attnum: i16, roleid: Oid, mode: u64) -> PgResult<i32> {
    if pg_attribute_aclmask_ext(table_oid, attnum, roleid, mode, AclMaskHow::AclmaskAny, None)? != 0
    {
        Ok(ACLCHECK_OK)
    } else {
        Ok(ACLCHECK_NO_PRIV)
    }
}

// pg_attribute_aclcheck_ext (aclchk.c): (AclResult, is_missing).
pub fn pg_attribute_aclcheck_ext(
    table_oid: Oid,
    attnum: i16,
    roleid: Oid,
    mode: u64,
) -> PgResult<(i32, bool)> {
    let mut is_missing = false;
    let m = pg_attribute_aclmask_ext(
        table_oid,
        attnum,
        roleid,
        mode,
        AclMaskHow::AclmaskAny,
        Some(&mut is_missing),
    )?;
    let r = if m != 0 { ACLCHECK_OK } else { ACLCHECK_NO_PRIV };
    Ok((r, is_missing))
}

#[track_caller]
#[cold]
#[inline(never)]
fn undefined_column(attnum: i16, table_oid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "attribute {attnum} of relation with OID {table_oid} does not exist"
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
    )
}

pub fn pg_attribute_aclmask_ext(
    table_oid: Oid,
    attnum: i16,
    roleid: Oid,
    mask: u64,
    how: AclMaskHow,
    is_missing: Option<&mut bool>,
) -> PgResult<u64> {
    let att_tuple = SearchSysCache2(
        ATTNUM,
        SysCacheKey::Value(Datum::from_oid(table_oid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
    )?;
    let Some(att_tuple) = att_tuple else {
        if let Some(m) = is_missing {
            *m = true;
            return Ok(0);
        }
        return Err(undefined_column(attnum, table_oid));
    };

    if SysCacheGetAttrNotNull(ATTNUM, &att_tuple, ANUM_PG_ATTRIBUTE_ATTISDROPPED)?.as_bool() {
        ReleaseSysCache(att_tuple);
        if let Some(m) = is_missing {
            *m = true;
            return Ok(0);
        }
        return Err(undefined_column(attnum, table_oid));
    }

    let (acl_datum, isnull) = SysCacheGetAttr(ATTNUM, &att_tuple, ANUM_PG_ATTRIBUTE_ATTACL)?;
    // Default column ACL grants nothing: fall out fast on the common NULL.
    if isnull {
        ReleaseSysCache(att_tuple);
        return Ok(0);
    }

    let Some(class_tuple) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(table_oid)))?
    else {
        ReleaseSysCache(att_tuple);
        if let Some(m) = is_missing {
            *m = true;
            return Ok(0);
        }
        return Err(undefined_table(table_oid));
    };
    let owner_id = SysCacheGetAttrNotNull(RELOID, &class_tuple, ANUM_PG_CLASS_RELOWNER)?.as_oid();
    ReleaseSysCache(class_tuple);

    let result = with_acl_datum(acl_datum, |acl| aclmask(acl, roleid, owner_id, mask, how))?;
    ReleaseSysCache(att_tuple);
    Ok(result)
}

pub fn pg_attribute_aclcheck_all(
    table_oid: Oid,
    roleid: Oid,
    mode: u64,
    how: AclMaskHow,
) -> PgResult<i32> {
    pg_attribute_aclcheck_all_ext(table_oid, roleid, mode, how, None)
}

pub fn pg_attribute_aclcheck_all_ext(
    table_oid: Oid,
    roleid: Oid,
    mode: u64,
    how: AclMaskHow,
    is_missing: Option<&mut bool>,
) -> PgResult<i32> {
    let Some(class_tuple) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(table_oid)))?
    else {
        if let Some(m) = is_missing {
            *m = true;
            return Ok(ACLCHECK_NO_PRIV);
        }
        return Err(undefined_table(table_oid));
    };
    let owner_id = SysCacheGetAttrNotNull(RELOID, &class_tuple, ANUM_PG_CLASS_RELOWNER)?.as_oid();
    let nattrs = SysCacheGetAttrNotNull(RELOID, &class_tuple, ANUM_PG_CLASS_RELNATTS)?.as_i16();
    ReleaseSysCache(class_tuple);

    // Failure is reported when there are no non-dropped columns, for either
    // value of `how`.
    let mut result = ACLCHECK_NO_PRIV;
    for curr_att in 1..=nattrs {
        let att_tuple = SearchSysCache2(
            ATTNUM,
            SysCacheKey::Value(Datum::from_oid(table_oid)),
            SysCacheKey::Value(Datum::from_i16(curr_att)),
        )?;
        let Some(att_tuple) = att_tuple else {
            continue;
        };
        if SysCacheGetAttrNotNull(ATTNUM, &att_tuple, ANUM_PG_ATTRIBUTE_ATTISDROPPED)?.as_bool() {
            ReleaseSysCache(att_tuple);
            continue;
        }
        let (acl_datum, isnull) = SysCacheGetAttr(ATTNUM, &att_tuple, ANUM_PG_ATTRIBUTE_ATTACL)?;
        let attmask = if isnull {
            0
        } else {
            with_acl_datum(acl_datum, |acl| {
                aclmask(acl, roleid, owner_id, mode, AclMaskHow::AclmaskAny)
            })?
        };
        ReleaseSysCache(att_tuple);

        if attmask != 0 {
            result = ACLCHECK_OK;
            if how == AclMaskHow::AclmaskAny {
                break;
            }
        } else {
            result = ACLCHECK_NO_PRIV;
            if how == AclMaskHow::AclmaskAll {
                break;
            }
        }
    }
    Ok(result)
}

// pg_aclmask (aclchk.c) (restrict_and_check_grant's no-goptions fallback).
pub(crate) fn pg_aclmask_for_grant(
    objtype: ObjectType,
    object_oid: Oid,
    attnum: i16,
    roleid: Oid,
    mask: u64,
) -> PgResult<u64> {
    let how = AclMaskHow::AclmaskAny;
    match objtype {
        ObjectType::OBJECT_COLUMN => Ok(pg_class_aclmask(object_oid, roleid, mask, how)?
            | pg_attribute_aclmask_ext(object_oid, attnum, roleid, mask, how, None)?),
        ObjectType::OBJECT_TABLE | ObjectType::OBJECT_SEQUENCE => {
            pg_class_aclmask(object_oid, roleid, mask, how)
        }
        ObjectType::OBJECT_DATABASE => {
            object_aclmask(DATABASE_RELATION_ID, object_oid, roleid, mask, how)
        }
        ObjectType::OBJECT_FUNCTION => {
            object_aclmask(PROCEDURE_RELATION_ID, object_oid, roleid, mask, how)
        }
        ObjectType::OBJECT_LANGUAGE => {
            object_aclmask(LANGUAGE_RELATION_ID, object_oid, roleid, mask, how)
        }
        ObjectType::OBJECT_LARGEOBJECT => {
            lo::pg_largeobject_aclmask_snapshot_current(object_oid, roleid, mask, how)
        }
        ObjectType::OBJECT_SCHEMA => {
            object_aclmask(NAMESPACE_RELATION_ID, object_oid, roleid, mask, how)
        }
        ObjectType::OBJECT_TYPE => {
            object_aclmask(TYPE_RELATION_ID, object_oid, roleid, mask, how)
        }
        ObjectType::OBJECT_TABLESPACE => object_aclmask(
            types_core::catalog::TABLE_SPACE_RELATION_ID,
            object_oid,
            roleid,
            mask,
            how,
        ),
        ObjectType::OBJECT_PARAMETER_ACL => {
            pg_parameter_acl_aclmask(object_oid, roleid, mask, how)
        }
        ObjectType::OBJECT_FDW => object_aclmask(
            types_core::FOREIGN_DATA_WRAPPER_RELATION_ID,
            object_oid,
            roleid,
            mask,
            how,
        ),
        ObjectType::OBJECT_FOREIGN_SERVER => {
            object_aclmask(types_core::FOREIGN_SERVER_RELATION_ID, object_oid, roleid, mask, how)
        }
        other => panic!("pg_aclmask (aclchk.c): object type {} arm unported", other as i32),
    }
}

// pg_parameter_acl_aclmask (aclchk.c): by pg_parameter_acl OID, unlike
// pg_parameter_aclmask's by-name probe.
fn pg_parameter_acl_aclmask(acl_oid: Oid, roleid: Oid, mask: u64, how: AclMaskHow) -> PgResult<u64> {
    if superuser::superuser_arg(roleid)? {
        return Ok(mask);
    }
    let Some(tuple) =
        SearchSysCache1(PARAMETERACLOID, SysCacheKey::Value(Datum::from_oid(acl_oid)))?
    else {
        return Err(Box::new(
            PgError::error(format!("parameter ACL with OID {acl_oid} does not exist"))
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    };
    let (acl_datum, isnull) =
        SysCacheGetAttr(PARAMETERACLOID, &tuple, ANUM_PG_PARAMETER_ACL_PARACL)?;
    let result = if isnull {
        aclmask(
            acldefault(AclObjectType::ParameterAcl, BOOTSTRAP_SUPERUSERID).as_slice(),
            roleid,
            BOOTSTRAP_SUPERUSERID,
            mask,
            how,
        )?
    } else {
        with_acl_datum(acl_datum, |acl| aclmask(acl, roleid, BOOTSTRAP_SUPERUSERID, mask, how))?
    };
    ReleaseSysCache(tuple);
    Ok(result)
}

pub(crate) fn objtype_from_i32(objtype: i32) -> ObjectType {
    assert!(
        (0..=ObjectType::OBJECT_VIEW as i32).contains(&objtype),
        "aclcheck_error: bad ObjectType {objtype}"
    );
    // SAFETY: ObjectType is repr(u32) and contiguous over the asserted range.
    unsafe { core::mem::transmute::<u32, ObjectType>(objtype as u32) }
}

fn objtype_noun(objtype: ObjectType) -> &'static str {
    use ObjectType::*;
    match objtype {
        OBJECT_AGGREGATE => "aggregate",
        OBJECT_COLLATION => "collation",
        OBJECT_COLUMN => "column",
        OBJECT_CONVERSION => "conversion",
        OBJECT_DATABASE => "database",
        OBJECT_DOMAIN => "domain",
        OBJECT_EVENT_TRIGGER => "event trigger",
        OBJECT_EXTENSION => "extension",
        OBJECT_FDW => "foreign-data wrapper",
        OBJECT_FOREIGN_SERVER => "foreign server",
        OBJECT_FOREIGN_TABLE => "foreign table",
        OBJECT_FUNCTION => "function",
        OBJECT_INDEX => "index",
        OBJECT_LANGUAGE => "language",
        OBJECT_LARGEOBJECT => "large object",
        OBJECT_MATVIEW => "materialized view",
        OBJECT_OPCLASS => "operator class",
        OBJECT_OPERATOR => "operator",
        OBJECT_OPFAMILY => "operator family",
        OBJECT_PARAMETER_ACL => "parameter",
        OBJECT_POLICY => "policy",
        OBJECT_PROCEDURE => "procedure",
        OBJECT_PUBLICATION => "publication",
        OBJECT_ROUTINE => "routine",
        OBJECT_SCHEMA => "schema",
        OBJECT_SEQUENCE => "sequence",
        OBJECT_STATISTIC_EXT => "statistics object",
        OBJECT_SUBSCRIPTION => "subscription",
        OBJECT_TABLE => "table",
        OBJECT_TABLESPACE => "tablespace",
        OBJECT_TSCONFIGURATION => "text search configuration",
        OBJECT_TSDICTIONARY => "text search dictionary",
        OBJECT_TYPE => "type",
        OBJECT_VIEW => "view",
        other => panic!("aclcheck_error: unsupported object type: {}", other as i32),
    }
}

// object_ownercheck (aclchk.c); classes without an arm below are loud.
pub fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> PgResult<bool> {
    if superuser::superuser_arg(roleid)? {
        return Ok(true);
    }
    let owner_id = match classid {
        TYPE_RELATION_ID => {
            let Some(tuple) =
                SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(objectid)))?
            else {
                return Err(Box::new(PgError::error(format!(
                    "cache lookup failed for type {objectid}"
                ))));
            };
            let owner =
                SysCacheGetAttrNotNull(TYPEOID, &tuple, ANUM_PG_TYPE_TYPOWNER)?.as_oid();
            ReleaseSysCache(tuple);
            owner
        }
        RELATION_RELATION_ID => {
            let Some(tuple) =
                SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(objectid)))?
            else {
                return Err(Box::new(PgError::error(format!(
                    "cache lookup failed for relation {objectid}"
                ))));
            };
            let owner =
                SysCacheGetAttrNotNull(RELOID, &tuple, ANUM_PG_CLASS_RELOWNER)?.as_oid();
            ReleaseSysCache(tuple);
            owner
        }
        PublicationRelationId => {
            let Some(tuple) = SearchSysCache1(
                cache_syscache::cacheinfo::PUBLICATIONOID,
                SysCacheKey::Value(Datum::from_oid(objectid)),
            )?
            else {
                return Err(Box::new(PgError::error(format!(
                    "cache lookup failed for publication {objectid}"
                ))));
            };
            let owner = SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::PUBLICATIONOID,
                &tuple,
                ANUM_PG_PUBLICATION_PUBOWNER,
            )?
            .as_oid();
            ReleaseSysCache(tuple);
            owner
        }
        SubscriptionRelationId => {
            let Some(tuple) = SearchSysCache1(
                cache_syscache::cacheinfo::SUBSCRIPTIONOID,
                SysCacheKey::Value(Datum::from_oid(objectid)),
            )?
            else {
                return Err(Box::new(PgError::error(format!(
                    "cache lookup failed for subscription {objectid}"
                ))));
            };
            let owner = SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::SUBSCRIPTIONOID,
                &tuple,
                ANUM_PG_SUBSCRIPTION_SUBOWNER,
            )?
            .as_oid();
            ReleaseSysCache(tuple);
            owner
        }
        types_core::catalog::TABLE_SPACE_RELATION_ID => {
            let Some(tuple) = SearchSysCache1(
                cache_syscache::cacheinfo::TABLESPACEOID,
                SysCacheKey::Value(Datum::from_oid(objectid)),
            )?
            else {
                return Err(Box::new(PgError::error(format!(
                    "cache lookup failed for tablespace {objectid}"
                ))));
            };
            let owner = SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::TABLESPACEOID,
                &tuple,
                3,
            )?
            .as_oid();
            ReleaseSysCache(tuple);
            owner
        }
        DATABASE_RELATION_ID => {
            let Some(tuple) = SearchSysCache1(
                cache_syscache::cacheinfo::DATABASEOID,
                SysCacheKey::Value(Datum::from_oid(objectid)),
            )?
            else {
                return Err(Box::new(PgError::error(format!(
                    "cache lookup failed for database {objectid}"
                ))));
            };
            let owner = SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::DATABASEOID,
                &tuple,
                ANUM_PG_DATABASE_DATDBA,
            )?
            .as_oid();
            ReleaseSysCache(tuple);
            owner
        }
        ConversionRelationId_own => {
            let Some(tuple) = SearchSysCache1(
                cache_syscache::cacheinfo::CONVOID,
                SysCacheKey::Value(Datum::from_oid(objectid)),
            )?
            else {
                return Err(Box::new(PgError::error(format!(
                    "cache lookup failed for conversion {objectid}"
                ))));
            };
            let owner = SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::CONVOID,
                &tuple,
                ANUM_PG_CONVERSION_CONOWNER,
            )?
            .as_oid();
            ReleaseSysCache(tuple);
            owner
        }
        LanguageRelationId_own => {
            let Some(tuple) = SearchSysCache1(
                cache_syscache::cacheinfo::LANGOID,
                SysCacheKey::Value(Datum::from_oid(objectid)),
            )?
            else {
                return Err(Box::new(PgError::error(format!(
                    "cache lookup failed for language {objectid}"
                ))));
            };
            let owner = SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::LANGOID,
                &tuple,
                ANUM_PG_LANGUAGE_LANOWNER,
            )?
            .as_oid();
            ReleaseSysCache(tuple);
            owner
        }
        NAMESPACE_RELATION_ID => {
            let Some(tuple) = SearchSysCache1(
                cache_syscache::cacheinfo::NAMESPACEOID,
                SysCacheKey::Value(Datum::from_oid(objectid)),
            )?
            else {
                return Err(Box::new(PgError::error(format!(
                    "cache lookup failed for namespace {objectid}"
                ))));
            };
            let owner = SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::NAMESPACEOID,
                &tuple,
                ANUM_PG_NAMESPACE_NSPOWNER,
            )?
            .as_oid();
            ReleaseSysCache(tuple);
            owner
        }
        PROCEDURE_RELATION_ID => {
            let Some(tuple) =
                SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(objectid)))?
            else {
                return Err(Box::new(PgError::error(format!(
                    "cache lookup failed for function {objectid}"
                ))));
            };
            let owner =
                SysCacheGetAttrNotNull(PROCOID, &tuple, ANUM_PG_PROC_PROOWNER)?.as_oid();
            ReleaseSysCache(tuple);
            owner
        }
        OperatorRelationId => {
            let Some(tuple) = SearchSysCache1(
                cache_syscache::cacheinfo::OPEROID,
                SysCacheKey::Value(Datum::from_oid(objectid)),
            )?
            else {
                return Err(Box::new(PgError::error(format!(
                    "cache lookup failed for operator {objectid}"
                ))));
            };
            let owner = SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::OPEROID,
                &tuple,
                ANUM_PG_OPERATOR_OPROWNER,
            )?
            .as_oid();
            ReleaseSysCache(tuple);
            owner
        }
        CollationRelationId_own => {
            syscache_owner(cache_syscache::cacheinfo::COLLOID, 4, objectid, "collation")?
        }
        OperatorClassRelationId_own => {
            syscache_owner(cache_syscache::cacheinfo::CLAOID, 5, objectid, "operator class")?
        }
        OperatorFamilyRelationId_own => syscache_owner(
            cache_syscache::cacheinfo::OPFAMILYOID,
            5,
            objectid,
            "operator family",
        )?,
        StatisticExtRelationId_own => syscache_owner(
            cache_syscache::cacheinfo::STATEXTOID,
            5,
            objectid,
            "statistics object",
        )?,
        TSDictionaryRelationId_own => syscache_owner(
            cache_syscache::cacheinfo::TSDICTOID,
            4,
            objectid,
            "text search dictionary",
        )?,
        TSConfigRelationId_own => syscache_owner(
            cache_syscache::cacheinfo::TSCONFIGOID,
            4,
            objectid,
            "text search configuration",
        )?,
        FOREIGN_DATA_WRAPPER_RELATION_ID_own => syscache_owner(
            FOREIGNDATAWRAPPEROID,
            ANUM_PG_FOREIGN_DATA_WRAPPER_FDWOWNER,
            objectid,
            "foreign-data wrapper",
        )?,
        FOREIGN_SERVER_RELATION_ID_own => syscache_owner(
            FOREIGNSERVEROID,
            ANUM_PG_FOREIGN_SERVER_SRVOWNER,
            objectid,
            "foreign server",
        )?,
        EventTriggerRelationId_own => syscache_owner(
            cache_syscache::cacheinfo::EVENTTRIGGEROID,
            4,
            objectid,
            "event trigger",
        )?,
        // C object_ownercheck: extension owner is pg_extension.extowner, read
        // via the EXTENSIONOID syscache (get_object_catcache_oid returns it).
        EXTENSION_RELATION_ID => {
            syscache_owner(EXTENSIONOID, ANUM_PG_EXTENSION_EXTOWNER, objectid, "extension")?
        }
        other => panic!("object_ownercheck (aclchk.c): object class {other} arm unported"),
    };
    has_privs_of_role(roleid, owner_id)
}

fn syscache_owner(cacheid: i32, attnum: i32, objectid: Oid, what: &str) -> PgResult<Oid> {
    let Some(tuple) = SearchSysCache1(cacheid, SysCacheKey::Value(Datum::from_oid(objectid)))?
    else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for {what} {objectid}"
        ))));
    };
    let owner = SysCacheGetAttrNotNull(cacheid, &tuple, attnum)?.as_oid();
    ReleaseSysCache(tuple);
    Ok(owner)
}


const ConversionRelationId_own: Oid = 2607;
const LanguageRelationId_own: Oid = 2612;
const CollationRelationId_own: Oid = 3456;
const OperatorClassRelationId_own: Oid = 2616;
const OperatorFamilyRelationId_own: Oid = 2753;
const StatisticExtRelationId_own: Oid = 3381;
const TSDictionaryRelationId_own: Oid = 3600;
const TSConfigRelationId_own: Oid = 3602;
const FOREIGN_DATA_WRAPPER_RELATION_ID_own: Oid = 2328;
const FOREIGN_SERVER_RELATION_ID_own: Oid = 1417;
const EventTriggerRelationId_own: Oid = 3466;
const ANUM_PG_EXTENSION_EXTOWNER: i32 = 3;
const ANUM_PG_CONVERSION_CONOWNER: i32 = 4;
const PublicationRelationId: Oid = 6104;
const SubscriptionRelationId: Oid = 6100;
const OperatorRelationId: Oid = 2617;
const ANUM_PG_PUBLICATION_PUBOWNER: i32 = 3;
const ANUM_PG_SUBSCRIPTION_SUBOWNER: i32 = 5;
const ANUM_PG_NAMESPACE_NSPOWNER: i32 = 3;
const ANUM_PG_OPERATOR_OPROWNER: i32 = 4;

pub fn has_createrole_privilege(roleid: Oid) -> PgResult<bool> {
    if superuser::superuser_arg(roleid)? {
        return Ok(true);
    }
    match SearchSysCache1(AUTHOID, SysCacheKey::Value(Datum::from_oid(roleid)))? {
        Some(tuple) => {
            let result =
                SysCacheGetAttrNotNull(AUTHOID, &tuple, ANUM_PG_AUTHID_ROLCREATEROLE)?.as_bool();
            ReleaseSysCache(tuple);
            Ok(result)
        }
        None => Ok(false),
    }
}

pub fn has_bypassrls_privilege(roleid: Oid) -> PgResult<bool> {
    if superuser::superuser_arg(roleid)? {
        return Ok(true);
    }
    match SearchSysCache1(AUTHOID, SysCacheKey::Value(Datum::from_oid(roleid)))? {
        Some(tuple) => {
            let result =
                SysCacheGetAttrNotNull(AUTHOID, &tuple, ANUM_PG_AUTHID_ROLBYPASSRLS)?.as_bool();
            ReleaseSysCache(tuple);
            Ok(result)
        }
        None => Ok(false),
    }
}

pub fn aclcheck_error(aclerr: i32, objtype: ObjectType, objectname: &str) -> PgResult<()> {
    match aclerr {
        ACLCHECK_OK => Ok(()),
        ACLCHECK_NO_PRIV => Err(Box::new(
            PgError::error(format!(
                "permission denied for {} {objectname}",
                objtype_noun(objtype)
            ))
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        )),
        ACLCHECK_NOT_OWNER => {
            // C: ownership attaches to the relation for these object types.
            let noun = match objtype {
                ObjectType::OBJECT_COLUMN
                | ObjectType::OBJECT_POLICY
                | ObjectType::OBJECT_RULE
                | ObjectType::OBJECT_TABCONSTRAINT
                | ObjectType::OBJECT_TRIGGER => "relation",
                _ => objtype_noun(objtype),
            };
            Err(Box::new(
                PgError::error(format!("must be owner of {noun} {objectname}"))
                    .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
            ))
        }
        other => Err(Box::new(PgError::error(format!(
            "unrecognized AclResult: {other}"
        )))),
    }
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

    let (acl_datum, isnull) = SysCacheGetAttr(PARAMETERACLNAME, &tuple, ANUM_PG_PARAMETER_ACL_PARACL)?;
    let result = if isnull {
        aclmask(
            acldefault(AclObjectType::ParameterAcl, BOOTSTRAP_SUPERUSERID).as_slice(),
            roleid,
            BOOTSTRAP_SUPERUSERID,
            mask,
            how,
        )?
    } else {
        with_acl_datum(acl_datum, |acl| {
            aclmask(acl, roleid, BOOTSTRAP_SUPERUSERID, mask, how)
        })?
    };
    ReleaseSysCache(tuple);
    Ok(result)
}

fn pg_parameter_aclcheck_set(name: &str, roleid: Oid) -> PgResult<bool> {
    Ok(pg_parameter_aclcheck(name, roleid, ACL_SET)? == ACLCHECK_OK)
}

pub fn init_seams() {
    aclchk_seams::object_aclcheck::set(object_aclcheck);
    aclchk_seams::object_aclcheck_ext::set(object_aclcheck_ext);
    aclchk_seams::object_ownercheck::set(object_ownercheck);
    aclchk_seams::has_lo_priv_byid::set(lo::has_lo_priv_byid);
    aclchk_seams::pg_parameter_aclcheck_set::set(pg_parameter_aclcheck_set);
    aclchk_seams::pg_parameter_aclcheck::set(pg_parameter_aclcheck);
    aclchk_seams::pg_class_aclcheck_ext::set(pg_class_aclcheck_ext);
    aclchk_seams::pg_class_aclmask::set(|table_oid, roleid, mask, how_all| {
        let how = if how_all { AclMaskHow::AclmaskAll } else { AclMaskHow::AclmaskAny };
        pg_class_aclmask(table_oid, roleid, mask, how)
    });
    aclchk_seams::pg_attribute_aclcheck::set(pg_attribute_aclcheck);
    aclchk_seams::pg_attribute_aclcheck_all::set(|table_oid, roleid, mode, how_all| {
        let how = if how_all { AclMaskHow::AclmaskAll } else { AclMaskHow::AclmaskAny };
        pg_attribute_aclcheck_all(table_oid, roleid, mode, how)
    });
    aclchk_seams::pg_attribute_aclcheck_ext::set(pg_attribute_aclcheck_ext);
    aclchk_seams::pg_attribute_aclcheck_all_ext::set(|table_oid, roleid, mode, how_all| {
        let how = if how_all { AclMaskHow::AclmaskAll } else { AclMaskHow::AclmaskAny };
        let mut is_missing = false;
        let r = pg_attribute_aclcheck_all_ext(table_oid, roleid, mode, how, Some(&mut is_missing))?;
        Ok((r, is_missing))
    });
    aclchk_seams::aclcheck_error::set(|aclresult, objtype, objectname| {
        aclcheck_error(aclresult, objtype_from_i32(objtype), objectname)
    });
    pg_shdepend::remove_role_from_object_acl::set(grant::RemoveRoleFromObjectACL);
    pg_shdepend::remove_role_from_init_priv::set(grant::RemoveRoleFromInitPriv);
    pg_shdepend::replace_role_in_init_priv::set(grant::ReplaceRoleInInitPriv);
    defacl::init_seams();
}

#[cfg(test)]
mod tests;
