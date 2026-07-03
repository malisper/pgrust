#![allow(non_snake_case)]

use std::cell::RefCell;

use adt_acl::{
    acldefault, aclmask, has_privs_of_role, AclItem, AclMaskHow, AclObjectType, ACL_DELETE,
    ACL_INSERT, ACL_MAINTAIN, ACL_SELECT, ACL_SET, ACL_TRUNCATE, ACL_UPDATE, ACL_USAGE,
};
use cache_syscache::cacheinfo::{ATTNUM, DATABASEOID, PARAMETERACLNAME, PROCOID, RELOID, TYPEOID};
use cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SearchSysCache2, SysCacheGetAttr, SysCacheGetAttrNotNull,
    SysCacheKey,
};
use datum::Datum;
use types_core::catalog::{
    FirstUnpinnedObjectId, BOOTSTRAP_SUPERUSERID, DATABASE_RELATION_ID, NAMESPACE_RELATION_ID,
    PG_TOAST_NAMESPACE, PROCEDURE_RELATION_ID, RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_UNDEFINED_TABLE,
};
use types_nodes::parsenodes::ObjectType;
use types_rel::{RELKIND_SEQUENCE, RELKIND_VIEW};

mod grant;
pub use grant::ExecuteGrantStmt;
mod lo;
pub use lo::{object_ownercheck, pg_largeobject_aclcheck_snapshot};

pub const ACLCHECK_OK: i32 = 0;
pub const ACLCHECK_NO_PRIV: i32 = 1;
pub const ACLCHECK_NOT_OWNER: i32 = 2;

const ANUM_PG_DATABASE_DATDBA: i32 = 3;
const ANUM_PG_DATABASE_DATACL: i32 = 18;
const ANUM_PG_PROC_PROOWNER: i32 = 4;
const ANUM_PG_PROC_PROACL: i32 = 30;
const ANUM_PG_TYPE_TYPOWNER: i32 = 4;
const ANUM_PG_TYPE_TYPACL: i32 = 32;
const ANUM_PG_PARAMETER_ACL_PARACL: i32 = 3;
const ANUM_PG_CLASS_RELNAMESPACE: i32 = 3;
const ANUM_PG_CLASS_RELOWNER: i32 = 6;
const ANUM_PG_CLASS_RELKIND: i32 = 18;
pub(crate) const ANUM_PG_CLASS_RELNATTS: i32 = 19;
pub(crate) const ANUM_PG_CLASS_RELACL: i32 = 32;
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
pub(crate) fn with_acl_datum<R>(
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

fn object_aclmask(
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mask: u64,
    how: AclMaskHow,
) -> PgResult<u64> {
    match classid {
        NAMESPACE_RELATION_ID => return pg_namespace_aclmask(objectid, roleid, mask, how),
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
        // C divergence: pg_type_aclmask's array-element indirection is not
        // applied; element and array types share default ACLs at initdb.
        TYPE_RELATION_ID => (
            TYPEOID,
            ANUM_PG_TYPE_TYPOWNER,
            ANUM_PG_TYPE_TYPACL,
            AclObjectType::Type,
            "type",
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
    let (acl_datum, isnull) = SysCacheGetAttr(cacheid, &tuple, acl_attnum)?;
    let result = if isnull {
        aclmask(acldefault(objtype, owner_id).as_slice(), roleid, owner_id, mask, how)?
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

#[cold]
#[inline(never)]
fn undefined_table(table_oid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("relation with OID {table_oid} does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_TABLE),
    )
}

// pg_namespace_aclmask_ext (aclchk.c), is_missing arm dropped (no caller).
fn pg_namespace_aclmask(nsp_oid: Oid, roleid: Oid, mask: u64, how: AclMaskHow) -> PgResult<u64> {
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
    let Some(class_tuple) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(table_oid)))?
    else {
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

// pg_aclmask (aclchk.c), reduced to the objtypes reachable from GRANT/REVOKE
// ON TABLE/SEQUENCE (restrict_and_check_grant's no-goptions fallback).
pub(crate) fn pg_aclmask_for_grant(
    objtype: ObjectType,
    object_oid: Oid,
    attnum: i16,
    roleid: Oid,
    mask: u64,
) -> PgResult<u64> {
    match objtype {
        ObjectType::OBJECT_COLUMN => Ok(pg_class_aclmask(
            object_oid,
            roleid,
            mask,
            AclMaskHow::AclmaskAny,
        )? | pg_attribute_aclmask_ext(
            object_oid,
            attnum,
            roleid,
            mask,
            AclMaskHow::AclmaskAny,
            None,
        )?),
        ObjectType::OBJECT_TABLE | ObjectType::OBJECT_SEQUENCE => {
            pg_class_aclmask(object_oid, roleid, mask, AclMaskHow::AclmaskAny)
        }
        other => panic!("pg_aclmask (aclchk.c): object type {} arm unported", other as i32),
    }
}

fn objtype_from_i32(objtype: i32) -> ObjectType {
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
        // C: "must be owner of relation %s" (NOT_OWNER only).
        OBJECT_TRIGGER => "relation",
        other => panic!("aclcheck_error: unsupported object type: {}", other as i32),
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
        ACLCHECK_NOT_OWNER => Err(Box::new(
            PgError::error(format!(
                "must be owner of {} {objectname}",
                objtype_noun(objtype)
            ))
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        )),
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
    aclchk_seams::pg_parameter_aclcheck_set::set(pg_parameter_aclcheck_set);
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
    aclchk_seams::aclcheck_error::set(|aclresult, objtype, objectname| {
        aclcheck_error(aclresult, objtype_from_i32(objtype), objectname)
    });
}

#[cfg(test)]
mod tests;
