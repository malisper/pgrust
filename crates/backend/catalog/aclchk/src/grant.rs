use adt_acl::{
    acl_grant_option_for, acl_option_to_privs, aclconcat, acldefault, aclitem_set_privs_goptions,
    aclmembers, aclupdate, select_best_grantor, varlena::acl_image, AclItem, AclObjectType,
    ACL_ALL_RIGHTS_COLUMN, ACL_ALL_RIGHTS_DATABASE, ACL_ALL_RIGHTS_FDW,
    ACL_ALL_RIGHTS_FOREIGN_SERVER, ACL_ALL_RIGHTS_FUNCTION, ACL_ALL_RIGHTS_LANGUAGE,
    ACL_ALL_RIGHTS_LARGEOBJECT, ACL_ALL_RIGHTS_PARAMETER_ACL, ACL_ALL_RIGHTS_RELATION,
    ACL_ALL_RIGHTS_SCHEMA, ACL_ALL_RIGHTS_SEQUENCE, ACL_ALL_RIGHTS_TABLESPACE,
    ACL_ALL_RIGHTS_TYPE, ACL_ALTER_SYSTEM,
    ACL_CONNECT, ACL_CREATE, ACL_CREATE_TEMP, ACL_DELETE, ACL_EXECUTE, ACL_ID_PUBLIC, ACL_INSERT,
    ACL_MAINTAIN, ACL_MODECHG_ADD, ACL_MODECHG_DEL, ACL_NO_RIGHTS, ACL_REFERENCES, ACL_SELECT,
    ACL_SET, ACL_TRIGGER, ACL_TRUNCATE, ACL_UPDATE, ACL_USAGE,
};
use cache_syscache::cacheinfo::{ATTNAME, ATTNUM, PARAMETERACLOID, RELOID};
use cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SearchSysCache2, SearchSysCacheLocked1, SysCacheGetAttr,
    SysCacheGetAttrNotNull, SysCacheKey,
};
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::catalog::{
    ATTRIBUTE_RELATION_ID, DATABASE_RELATION_ID, LANGUAGE_RELATION_ID, NAMESPACE_RELATION_ID,
    PROCEDURE_RELATION_ID, RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_core::{Oid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_GRANT_OPERATION,
    ERRCODE_SYNTAX_ERROR, ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED,
    ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED, ERRCODE_WRONG_OBJECT_TYPE, WARNING,
};
use types_nodes::parsenodes::{
    AccessPriv, GrantStmt, GrantTargetType, ObjectType, RoleSpec, RoleSpecType,
};
use types_rel::{
    AccessShareLock, RowExclusiveLock, RELKIND_COMPOSITE_TYPE, RELKIND_INDEX,
    RELKIND_PARTITIONED_INDEX, RELKIND_SEQUENCE, RELKIND_VIEW,
};
use types_storage::lock::{InplaceUpdateTupleLock, LOCKTAG};
use types_tuple::ItemPointerData;

use crate::{
    aclcheck_error, pg_aclmask_for_grant, with_acl_datum, ACLCHECK_NO_PRIV,
    ANUM_PG_CLASS_RELACL, ANUM_PG_CLASS_RELNATTS, ANUM_PG_TYPE_TYPTYPE,
};

const ANUM_PG_CLASS_OID: i32 = 1;
const ANUM_PG_CLASS_RELNAME: i32 = 2;
const ANUM_PG_CLASS_RELNAMESPACE: i32 = 3;
const ANUM_PG_CLASS_RELOWNER: i32 = 6;
const ANUM_PG_CLASS_RELKIND: i32 = 18;
const ANUM_PG_PROC_OID: i32 = 1;
const ANUM_PG_PROC_PRONAMESPACE: i32 = 3;
const ANUM_PG_PROC_PROKIND: i32 = 10;
const PROKIND_PROCEDURE: i8 = b'p' as i8;
const ANUM_PG_ATTRIBUTE_ATTNAME: i32 = 2;
const ANUM_PG_ATTRIBUTE_ATTISDROPPED: i32 = 17;
const ANUM_PG_ATTRIBUTE_ATTACL: i32 = 22;
const FIRST_LOW_INVALID_HEAP_ATTNUM: i32 = -7;

struct InternalGrant<'a, 'mcx> {
    is_grant: bool,
    objtype: ObjectType,
    objects: PgVec<'mcx, Oid>,
    all_privs: bool,
    privileges: u64,
    col_privs: PgVec<'mcx, &'a AccessPriv<'a>>,
    grantees: PgVec<'mcx, Oid>,
    grant_option: bool,
    behavior: i32,
}

fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

fn warn(msg: String, sqlstate: types_error::SqlState) -> PgResult<()> {
    elog::ereport(WARNING)
        .errcode(sqlstate)
        .errmsg(msg)
        .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "ExecuteGrantStmt"))
}

// get_rolespec_oid (acl.c).
pub fn get_rolespec_oid(role: &RoleSpec<'_>, missing_ok: bool) -> PgResult<Oid> {
    use RoleSpecType::*;
    match role.roletype {
        ROLESPEC_CSTRING => {
            adt_acl::get_role_oid(role.rolename.unwrap_or_default(), missing_ok)
        }
        ROLESPEC_CURRENT_ROLE | ROLESPEC_CURRENT_USER => Ok(miscinit::GetUserId()),
        ROLESPEC_SESSION_USER => Ok(miscinit::GetSessionUserId()),
        ROLESPEC_PUBLIC => Err(err(
            "role \"public\" does not exist".into(),
            types_error::ERRCODE_UNDEFINED_OBJECT,
        )),
    }
}

pub(crate) fn string_to_privilege(privname: &str) -> PgResult<u64> {
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
        "temporary" | "temp" => ACL_CREATE_TEMP,
        "connect" => ACL_CONNECT,
        "set" => ACL_SET,
        "alter system" => ACL_ALTER_SYSTEM,
        "maintain" => ACL_MAINTAIN,
        _ => {
            return Err(err(
                format!("unrecognized privilege type \"{privname}\""),
                ERRCODE_SYNTAX_ERROR,
            ))
        }
    })
}

pub(crate) fn privilege_to_string(privilege: u64) -> &'static str {
    match privilege {
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
        _ => "multiple privileges",
    }
}

// merge_acl_with_grant (aclchk.c).
#[allow(clippy::too_many_arguments)]
pub(crate) fn merge_acl_with_grant<'mcx>(
    mcx: Mcx<'mcx>,
    old_acl: &[AclItem],
    is_grant: bool,
    grant_option: bool,
    behavior: i32,
    grantees: &[Oid],
    privileges: u64,
    grantor_id: Oid,
    owner_id: Oid,
) -> PgResult<PgVec<'mcx, AclItem>> {
    let modechg = if is_grant { ACL_MODECHG_ADD } else { ACL_MODECHG_DEL };
    let mut new_acl = adt_acl::aclcopy(mcx, old_acl)?;
    for &grantee in grantees {
        // Grant options can only be granted to roles, not PUBLIC: privileges
        // re-granted via PUBLIC could never be cleaned up after a role drop.
        if is_grant && grant_option && grantee == ACL_ID_PUBLIC {
            return Err(err(
                "grant options can only be granted to roles".into(),
                ERRCODE_INVALID_GRANT_OPERATION,
            ));
        }
        let mut aclitem = AclItem {
            ai_grantee: grantee,
            ai_grantor: grantor_id,
            ai_privs: 0,
        };
        // GRANT ... WITH GRANT OPTION grants both; plain REVOKE revokes both,
        // REVOKE GRANT OPTION FOR revokes only the option (SQL spec).
        aclitem_set_privs_goptions(
            &mut aclitem,
            if is_grant || !grant_option { privileges } else { ACL_NO_RIGHTS },
            if !is_grant || grant_option { privileges } else { ACL_NO_RIGHTS },
        );
        new_acl = aclupdate(mcx, &new_acl, &aclitem, modechg, owner_id, behavior)?;
    }
    Ok(new_acl)
}

// restrict_and_check_grant (aclchk.c).
#[allow(clippy::too_many_arguments)]
fn restrict_and_check_grant(
    is_grant: bool,
    avail_goptions: u64,
    all_privs: bool,
    privileges: u64,
    object_id: Oid,
    grantor_id: Oid,
    objtype: ObjectType,
    objname: &str,
    att_number: i16,
    colname: Option<&str>,
) -> PgResult<u64> {
    let whole_mask = match objtype {
        ObjectType::OBJECT_COLUMN => ACL_ALL_RIGHTS_COLUMN,
        ObjectType::OBJECT_TABLE => ACL_ALL_RIGHTS_RELATION,
        ObjectType::OBJECT_SEQUENCE => ACL_ALL_RIGHTS_SEQUENCE,
        ObjectType::OBJECT_DATABASE => ACL_ALL_RIGHTS_DATABASE,
        ObjectType::OBJECT_FUNCTION => ACL_ALL_RIGHTS_FUNCTION,
        ObjectType::OBJECT_LANGUAGE => ACL_ALL_RIGHTS_LANGUAGE,
        ObjectType::OBJECT_LARGEOBJECT => ACL_ALL_RIGHTS_LARGEOBJECT,
        ObjectType::OBJECT_SCHEMA => ACL_ALL_RIGHTS_SCHEMA,
        ObjectType::OBJECT_TABLESPACE => ACL_ALL_RIGHTS_TABLESPACE,
        ObjectType::OBJECT_TYPE => ACL_ALL_RIGHTS_TYPE,
        ObjectType::OBJECT_PARAMETER_ACL => ACL_ALL_RIGHTS_PARAMETER_ACL,
        ObjectType::OBJECT_FDW => ACL_ALL_RIGHTS_FDW,
        ObjectType::OBJECT_FOREIGN_SERVER => ACL_ALL_RIGHTS_FOREIGN_SERVER,
        other => panic!(
            "restrict_and_check_grant (aclchk.c): object type {} arm unported",
            other as i32
        ),
    };

    // Per spec, any privilege at all on the object gets past the hard error.
    if avail_goptions == ACL_NO_RIGHTS
        && pg_aclmask_for_grant(
            objtype,
            object_id,
            att_number,
            grantor_id,
            whole_mask | acl_grant_option_for(whole_mask),
        )? == ACL_NO_RIGHTS
    {
        if let (ObjectType::OBJECT_COLUMN, Some(colname)) = (objtype, colname) {
            return Err(err(
                format!(
                    "permission denied for column {colname} of relation {objname}"
                ),
                types_error::ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
        aclcheck_error(ACLCHECK_NO_PRIV, objtype, objname)?;
    }

    let this_privileges = privileges & acl_option_to_privs(avail_goptions);
    let (code, verb) = if is_grant {
        (ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED, "were granted")
    } else {
        (ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED, "could be revoked")
    };
    if this_privileges == 0 {
        match colname {
            Some(colname) => warn(
                format!(
                    "no privileges {verb} for column \"{colname}\" of relation \"{objname}\""
                ),
                code,
            )?,
            None => warn(format!("no privileges {verb} for \"{objname}\""), code)?,
        }
    } else if !all_privs && this_privileges != privileges {
        match colname {
            Some(colname) => warn(
                format!(
                    "not all privileges {verb} for column \"{colname}\" of relation \"{objname}\""
                ),
                code,
            )?,
            None => warn(format!("not all privileges {verb} for \"{objname}\""), code)?,
        }
    }
    Ok(this_privileges)
}

fn name_attr(cacheid: i32, tuple: &catcache::CatCTuple, attnum: i32) -> PgResult<String> {
    let d = SysCacheGetAttrNotNull(cacheid, tuple, attnum)?;
    // SAFETY: a Name column inside the held tuple: 64 bytes, NUL-terminated.
    let cs = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    Ok(cs.to_string_lossy().into_owned())
}

fn unlock_class_tuple(tid: &ItemPointerData) -> PgResult<()> {
    let tag = LOCKTAG::tuple(
        init_small::globals::MyDatabaseId(),
        RELATION_RELATION_ID,
        types_tuple::ItemPointerGetBlockNumber(tid),
        types_tuple::ItemPointerGetOffsetNumber(tid),
    );
    lock_seams::lock_release::call(tag, InplaceUpdateTupleLock, false)?;
    Ok(())
}

// ExecuteGrantStmt (aclchk.c): GRANT/REVOKE ON TABLE/SEQUENCE objects.
pub fn ExecuteGrantStmt<'mcx>(mcx: Mcx<'mcx>, stmt: &GrantStmt<'_>) -> PgResult<()> {
    if let Some(grantor) = stmt.grantor {
        // The clause is SQL-compatibility only.
        if get_rolespec_oid(grantor, false)? != miscinit::GetUserId() {
            return Err(err(
                "grantor must be current user".into(),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
    }

    let objects = match stmt.targtype {
        GrantTargetType::ACL_TARGET_OBJECT => {
            object_names_to_oids(mcx, stmt.objtype, &stmt.objects, stmt.is_grant)?
        }
        GrantTargetType::ACL_TARGET_ALL_IN_SCHEMA => {
            objects_in_schema_to_oids(mcx, stmt.objtype, &stmt.objects)?
        }
        // ACL_TARGET_DEFAULTS is routed via AlterDefaultPrivileges, never here.
        other => panic!("unrecognized GrantStmt.targtype: {}", other as i32),
    };

    let mut grantees: PgVec<'_, Oid> = mcx::vec_with_capacity_in(mcx, stmt.grantees.len())?;
    for cell in stmt.grantees.iter() {
        let grantee = cell.as_role_spec().expect("grantee RoleSpec");
        let uid = match grantee.roletype {
            RoleSpecType::ROLESPEC_PUBLIC => ACL_ID_PUBLIC,
            _ => get_rolespec_oid(grantee, false)?,
        };
        grantees.push(uid);
    }

    let (all_privileges, errnoun) = match stmt.objtype {
        // GRANT TABLE may target a sequence: test the union, refine later.
        ObjectType::OBJECT_TABLE => (ACL_ALL_RIGHTS_RELATION | ACL_ALL_RIGHTS_SEQUENCE, "relation"),
        ObjectType::OBJECT_SEQUENCE => (ACL_ALL_RIGHTS_SEQUENCE, "sequence"),
        ObjectType::OBJECT_DATABASE => (ACL_ALL_RIGHTS_DATABASE, "database"),
        ObjectType::OBJECT_DOMAIN => (ACL_ALL_RIGHTS_TYPE, "domain"),
        ObjectType::OBJECT_FUNCTION => (ACL_ALL_RIGHTS_FUNCTION, "function"),
        ObjectType::OBJECT_LANGUAGE => (ACL_ALL_RIGHTS_LANGUAGE, "language"),
        ObjectType::OBJECT_LARGEOBJECT => (ACL_ALL_RIGHTS_LARGEOBJECT, "large object"),
        ObjectType::OBJECT_SCHEMA => (ACL_ALL_RIGHTS_SCHEMA, "schema"),
        ObjectType::OBJECT_PROCEDURE => (ACL_ALL_RIGHTS_FUNCTION, "procedure"),
        ObjectType::OBJECT_ROUTINE => (ACL_ALL_RIGHTS_FUNCTION, "routine"),
        ObjectType::OBJECT_TABLESPACE => (ACL_ALL_RIGHTS_TABLESPACE, "tablespace"),
        ObjectType::OBJECT_TYPE => (ACL_ALL_RIGHTS_TYPE, "type"),
        ObjectType::OBJECT_FDW => (ACL_ALL_RIGHTS_FDW, "foreign-data wrapper"),
        ObjectType::OBJECT_FOREIGN_SERVER => (ACL_ALL_RIGHTS_FOREIGN_SERVER, "foreign server"),
        ObjectType::OBJECT_PARAMETER_ACL => (ACL_ALL_RIGHTS_PARAMETER_ACL, "parameter"),
        other => panic!("ExecuteGrantStmt (aclchk.c): unrecognized objtype {}", other as i32),
    };

    let mut istmt = InternalGrant {
        is_grant: stmt.is_grant,
        objtype: stmt.objtype,
        objects,
        all_privs: false,
        privileges: ACL_NO_RIGHTS,
        col_privs: mcx::vec_new_in(mcx),
        grantees,
        grant_option: stmt.grant_option,
        behavior: stmt.behavior as i32,
    };

    if stmt.privileges.is_nil() {
        istmt.all_privs = true;
    } else {
        for cell in stmt.privileges.iter() {
            let privnode = cell.as_access_priv().expect("AccessPriv");
            if !privnode.cols.is_nil() {
                if stmt.objtype != ObjectType::OBJECT_TABLE {
                    return Err(err(
                        "column privileges are only valid for relations".into(),
                        ERRCODE_INVALID_GRANT_OPERATION,
                    ));
                }
                istmt.col_privs.push(privnode);
                continue;
            }
            let priv_name = privnode
                .priv_name
                .expect("AccessPriv node must specify privilege or columns");
            let privilege = string_to_privilege(priv_name)?;
            if privilege & !all_privileges != 0 {
                return Err(err(
                    format!(
                        "invalid privilege type {} for {errnoun}",
                        privilege_to_string(privilege)
                    ),
                    ERRCODE_INVALID_GRANT_OPERATION,
                ));
            }
            istmt.privileges |= privilege;
        }
    }

    exec_grant_stmt_oids(mcx, &mut istmt)
}

// ExecGrantStmt_oids (aclchk.c).
fn exec_grant_stmt_oids<'mcx>(mcx: Mcx<'mcx>, istmt: &mut InternalGrant<'_, '_>) -> PgResult<()> {
    match istmt.objtype {
        ObjectType::OBJECT_TABLE | ObjectType::OBJECT_SEQUENCE => exec_grant_relation(mcx, istmt),
        ObjectType::OBJECT_DATABASE => exec_grant_common(mcx, istmt, &CLASS_DATABASE, None),
        ObjectType::OBJECT_DOMAIN | ObjectType::OBJECT_TYPE => {
            exec_grant_common(mcx, istmt, &CLASS_TYPE, Some(exec_grant_type_check))
        }
        ObjectType::OBJECT_FUNCTION | ObjectType::OBJECT_PROCEDURE | ObjectType::OBJECT_ROUTINE => {
            exec_grant_common(mcx, istmt, &CLASS_PROC, None)
        }
        ObjectType::OBJECT_LANGUAGE => {
            exec_grant_common(mcx, istmt, &CLASS_LANGUAGE, Some(exec_grant_language_check))
        }
        ObjectType::OBJECT_LARGEOBJECT => exec_grant_largeobject(mcx, istmt),
        ObjectType::OBJECT_SCHEMA => exec_grant_common(mcx, istmt, &CLASS_NAMESPACE, None),
        ObjectType::OBJECT_TABLESPACE => exec_grant_common(mcx, istmt, &CLASS_TABLESPACE, None),
        ObjectType::OBJECT_PARAMETER_ACL => exec_grant_parameter(mcx, istmt),
        ObjectType::OBJECT_FDW => exec_grant_common(mcx, istmt, &CLASS_FDW, None),
        ObjectType::OBJECT_FOREIGN_SERVER => {
            exec_grant_common(mcx, istmt, &CLASS_FOREIGN_SERVER, None)
        }
        other => panic!(
            "ExecGrantStmt_oids (aclchk.c): objtype {} unported",
            other as i32
        ),
    }?;

    // C collects after execution so ExecGrant_* can adjust istmt first.
    event_trigger_seams::event_trigger_collect_grant::call(istmt.is_grant, istmt.objtype);
    Ok(())
}

// objectNamesToOids (aclchk.c) + the get_object_address arms it reaches
// (objectaddress.c); every resolved object takes C's AccessShareLock
// (LockDatabaseObject / LockSharedObject), without the C post-lock existence
// recheck loop.
fn object_names_to_oids<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    objnames: &types_nodes::list::NodeList<'_>,
    is_grant: bool,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut objects: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, objnames.len())?;
    match objtype {
        ObjectType::OBJECT_TABLE | ObjectType::OBJECT_SEQUENCE => {
            for cell in objnames.iter() {
                let relvar = cell.as_range_var().expect("RangeVar");
                let rv = rel_vocab::RangeVar {
                    catalogname: relvar.catalogname,
                    schemaname: relvar.schemaname,
                    relname: relvar.relname.unwrap_or_default(),
                    inh: relvar.inh,
                    relpersistence: relvar.relpersistence,
                    location: relvar.location,
                };
                objects.push(catalog_namespace::RangeVarGetRelid(&rv, AccessShareLock, false)?);
            }
        }
        ObjectType::OBJECT_FUNCTION | ObjectType::OBJECT_PROCEDURE | ObjectType::OBJECT_ROUTINE => {
            for cell in objnames.iter() {
                let owa = cell.as_object_with_args().expect("ObjectWithArgs");
                let oid =
                    parse_func_seams::LookupFuncWithArgs::call(objtype as i32, owa, false)?;
                lmgr::LockDatabaseObject(PROCEDURE_RELATION_ID, oid, 0, AccessShareLock)?;
                objects.push(oid);
            }
        }
        ObjectType::OBJECT_DOMAIN | ObjectType::OBJECT_TYPE => {
            for cell in objnames.iter() {
                let typname = cell.as_list().expect("type name List");
                // makeTypeNameFromNameList (makefuncs.c).
                // SAFETY: read-only re-view of the parse tree's arena cells.
                let names = unsafe {
                    types_nodes::list::NodeList::from_raw_parts(
                        typname.as_slice().as_ptr().cast_mut(),
                        typname.len() as u32,
                    )
                };
                let tn = types_nodes::rawnodes::TypeName {
                    names,
                    typemod: -1,
                    location: -1,
                    ..Default::default()
                };
                let oid = parse_utilcmd_seams::LookupTypeNameOid::call(mcx, &tn)?;
                if objtype == ObjectType::OBJECT_DOMAIN {
                    check_is_domain(oid, typname)?;
                }
                lmgr::LockDatabaseObject(TYPE_RELATION_ID, oid, 0, AccessShareLock)?;
                objects.push(oid);
            }
        }
        ObjectType::OBJECT_DATABASE => {
            for cell in objnames.iter() {
                let name = cell.as_string().expect("database name").sval;
                let oid = dbcommands_seams::get_database_oid::call(mcx, name, false)?;
                lmgr::LockSharedObject(DATABASE_RELATION_ID, oid, 0, AccessShareLock)?;
                objects.push(oid);
            }
        }
        ObjectType::OBJECT_TABLESPACE => {
            for cell in objnames.iter() {
                let name = cell.as_string().expect("tablespace name").sval;
                let oid = tablespace_seams::get_tablespace_oid::call(mcx, name, false)?;
                lmgr::LockSharedObject(
                    types_core::catalog::TABLE_SPACE_RELATION_ID,
                    oid,
                    0,
                    AccessShareLock,
                )?;
                objects.push(oid);
            }
        }
        ObjectType::OBJECT_LANGUAGE => {
            for cell in objnames.iter() {
                let name = cell.as_string().expect("language name").sval;
                let oid = adt_acl::get_language_oid(name, false)?;
                lmgr::LockDatabaseObject(LANGUAGE_RELATION_ID, oid, 0, AccessShareLock)?;
                objects.push(oid);
            }
        }
        ObjectType::OBJECT_LARGEOBJECT => {
            let scratch = mcx::MemoryContext::new("objectNamesToOids");
            for cell in objnames.iter() {
                let oid = oidparse(cell)?;
                if !pg_largeobject::LargeObjectExists(scratch.mcx(), oid)? {
                    return Err(err(
                        format!("large object {oid} does not exist"),
                        types_error::ERRCODE_UNDEFINED_OBJECT,
                    ));
                }
                lmgr::LockDatabaseObject(pg_largeobject::LargeObjectRelationId, oid, 0, AccessShareLock)?;
                objects.push(oid);
            }
        }
        ObjectType::OBJECT_SCHEMA => {
            for cell in objnames.iter() {
                let name = cell.as_string().expect("schema name").sval;
                let oid = catalog_namespace::get_namespace_oid(name, false)?;
                lmgr::LockDatabaseObject(NAMESPACE_RELATION_ID, oid, 0, AccessShareLock)?;
                objects.push(oid);
            }
        }
        ObjectType::OBJECT_PARAMETER_ACL => {
            // A GUC rides as its pg_parameter_acl OID, manufactured on GRANT;
            // REVOKE skips GUCs without a row (no privileges to remove).
            for cell in objnames.iter() {
                let parameter = cell.as_string().expect("parameter name").sval;
                let mut parameter_id = pg_parameter_acl::ParameterAclLookup(parameter, true)?;
                if !OidIsValid(parameter_id) && is_grant {
                    parameter_id = pg_parameter_acl::ParameterAclCreate(mcx, parameter)?;
                    xact::CommandCounterIncrement()?;
                }
                if OidIsValid(parameter_id) {
                    objects.push(parameter_id);
                }
            }
        }
        ObjectType::OBJECT_FDW => {
            for cell in objnames.iter() {
                let name = cell.as_string().expect("foreign-data wrapper name").sval;
                let oid = foreigncmds_seams::get_foreign_data_wrapper_oid::call(name, false)?;
                lmgr::LockDatabaseObject(
                    types_core::FOREIGN_DATA_WRAPPER_RELATION_ID,
                    oid,
                    0,
                    AccessShareLock,
                )?;
                objects.push(oid);
            }
        }
        ObjectType::OBJECT_FOREIGN_SERVER => {
            for cell in objnames.iter() {
                let name = cell.as_string().expect("foreign server name").sval;
                let oid = foreigncmds_seams::get_foreign_server_oid::call(name, false)?;
                lmgr::LockDatabaseObject(
                    types_core::FOREIGN_SERVER_RELATION_ID,
                    oid,
                    0,
                    AccessShareLock,
                )?;
                objects.push(oid);
            }
        }
        other => panic!(
            "objectNamesToOids (aclchk.c): objtype {} unported",
            other as i32
        ),
    }
    Ok(objects)
}

// objectsInSchemaToOids (aclchk.c): every object of the type in the named
// schemas; USAGE is checked on the schemas, not the individual objects.
fn objects_in_schema_to_oids<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    nspnames: &types_nodes::list::NodeList<'_>,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut objects: PgVec<'mcx, Oid> = mcx::vec_new_in(mcx);
    for cell in nspnames.iter() {
        let nspname = cell.as_string().expect("schema name").sval;
        let namespace_id = catalog_namespace::LookupExplicitNamespace(nspname, false)?;
        match objtype {
            ObjectType::OBJECT_TABLE => {
                for relkind in [
                    types_rel::RELKIND_RELATION,
                    RELKIND_VIEW,
                    types_rel::RELKIND_MATVIEW,
                    types_rel::RELKIND_FOREIGN_TABLE,
                    types_rel::RELKIND_PARTITIONED_TABLE,
                ] {
                    get_relations_in_namespace(mcx, namespace_id, relkind, &mut objects)?;
                }
            }
            ObjectType::OBJECT_SEQUENCE => {
                get_relations_in_namespace(mcx, namespace_id, RELKIND_SEQUENCE, &mut objects)?;
            }
            ObjectType::OBJECT_FUNCTION
            | ObjectType::OBJECT_PROCEDURE
            | ObjectType::OBJECT_ROUTINE => {
                let rel = table::table_open(mcx, PROCEDURE_RELATION_ID, AccessShareLock)?;
                let key = [oid_scan_key(ANUM_PG_PROC_PRONAMESPACE, namespace_id)];
                let mut scan = genam::systable_beginscan(
                    mcx,
                    &rel,
                    types_core::InvalidOid,
                    false,
                    None,
                    &key,
                )?;
                let desc = rel.descr();
                while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
                    let mut isnull = false;
                    // SAFETY: fixed NOT NULL pg_proc columns under its descriptor.
                    let prokind = unsafe {
                        types_tuple::heap_getattr(tup, ANUM_PG_PROC_PROKIND, desc, &mut isnull)
                    }
                    .as_i8();
                    // OBJECT_FUNCTION includes aggregates and window functions.
                    if (objtype == ObjectType::OBJECT_FUNCTION && prokind == PROKIND_PROCEDURE)
                        || (objtype == ObjectType::OBJECT_PROCEDURE
                            && prokind != PROKIND_PROCEDURE)
                    {
                        continue;
                    }
                    // SAFETY: fixed NOT NULL pg_proc columns under its descriptor.
                    let oid = unsafe {
                        types_tuple::heap_getattr(tup, ANUM_PG_PROC_OID, desc, &mut isnull)
                    }
                    .as_oid();
                    objects.push(oid);
                }
                genam::systable_endscan(mcx, scan)?;
                rel.close(AccessShareLock)?;
            }
            other => panic!("unrecognized GrantStmt.objtype: {}", other as i32),
        }
    }
    Ok(objects)
}

// getRelationsInNamespace (aclchk.c).
fn get_relations_in_namespace<'mcx>(
    mcx: Mcx<'mcx>,
    namespace_id: Oid,
    relkind: u8,
    relations: &mut PgVec<'mcx, Oid>,
) -> PgResult<()> {
    let rel = table::table_open(mcx, RELATION_RELATION_ID, AccessShareLock)?;
    let key = [oid_scan_key(ANUM_PG_CLASS_RELNAMESPACE, namespace_id)];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, types_core::InvalidOid, false, None, &key)?;
    let desc = rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_class columns under its descriptor.
        let kind = unsafe {
            types_tuple::heap_getattr(tup, ANUM_PG_CLASS_RELKIND, desc, &mut isnull)
        }
        .as_i8() as u8;
        if kind != relkind {
            continue;
        }
        // SAFETY: fixed NOT NULL pg_class columns under its descriptor.
        let oid =
            unsafe { types_tuple::heap_getattr(tup, ANUM_PG_CLASS_OID, desc, &mut isnull) }
                .as_oid();
        relations.push(oid);
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(())
}

fn oid_scan_key(attno: i32, oid: Oid) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno as types_core::AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

// get_object_address_type's OBJECT_DOMAIN restriction (objectaddress.c).
fn check_is_domain(type_oid: Oid, typname: &types_nodes::list::NodeList<'_>) -> PgResult<()> {
    const TYPTYPE_DOMAIN: u8 = b'd';
    let Some(tuple) = cache_syscache::SearchSysCache1(
        cache_syscache::cacheinfo::TYPEOID,
        SysCacheKey::Value(Datum::from_oid(type_oid)),
    )?
    else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for type {type_oid}"
        ))));
    };
    let typtype =
        SysCacheGetAttrNotNull(cache_syscache::cacheinfo::TYPEOID, &tuple, ANUM_PG_TYPE_TYPTYPE)?
            .as_u8();
    ReleaseSysCache(tuple);
    if typtype != TYPTYPE_DOMAIN {
        let mut name = String::new();
        for (i, part) in typname.iter().enumerate() {
            if i > 0 {
                name.push('.');
            }
            name.push_str(part.as_string().expect("type name part").sval);
        }
        return Err(err(format!("\"{name}\" is not a domain"), ERRCODE_WRONG_OBJECT_TYPE));
    }
    Ok(())
}

// oidparse (oid.c).
fn oidparse(node: types_nodes::Node<'_>) -> PgResult<Oid> {
    if let Some(i) = node.as_integer() {
        return Ok(i.ival as Oid);
    }
    if let Some(f) = node.as_float() {
        return f.fval.parse::<u32>().map_err(|_| {
            err(
                format!("invalid input syntax for type {}: \"{}\"", "oid", f.fval),
                types_error::ERRCODE_INVALID_TEXT_REPRESENTATION,
            )
        });
    }
    panic!("oidparse: unexpected node type");
}

fn exec_grant_relation<'mcx>(mcx: Mcx<'mcx>, istmt: &mut InternalGrant<'_, '_>) -> PgResult<()> {
    let relation = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let att_relation = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;

    for i in 0..istmt.objects.len() {
        let rel_oid = istmt.objects[i];
        let Some(tuple) =
            SearchSysCacheLocked1(RELOID, SysCacheKey::Value(Datum::from_oid(rel_oid)))?
        else {
            return Err(Box::new(PgError::error(format!(
                "cache lookup failed for relation {rel_oid}"
            ))));
        };
        let relname = name_attr(RELOID, &tuple, ANUM_PG_CLASS_RELNAME)?;
        let relkind = SysCacheGetAttrNotNull(RELOID, &tuple, ANUM_PG_CLASS_RELKIND)?.as_u8();
        let relnatts = SysCacheGetAttrNotNull(RELOID, &tuple, ANUM_PG_CLASS_RELNATTS)?.as_i16();
        let owner_id = SysCacheGetAttrNotNull(RELOID, &tuple, ANUM_PG_CLASS_RELOWNER)?.as_oid();

        if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
            return Err(err(format!("\"{relname}\" is an index"), ERRCODE_WRONG_OBJECT_TYPE));
        }
        if relkind == RELKIND_COMPOSITE_TYPE {
            return Err(err(
                format!("\"{relname}\" is a composite type"),
                ERRCODE_WRONG_OBJECT_TYPE,
            ));
        }
        if istmt.objtype == ObjectType::OBJECT_SEQUENCE && relkind != RELKIND_SEQUENCE {
            return Err(err(
                format!("\"{relname}\" is not a sequence"),
                ERRCODE_WRONG_OBJECT_TYPE,
            ));
        }

        let mut this_privileges = if istmt.all_privs && istmt.privileges == ACL_NO_RIGHTS {
            if relkind == RELKIND_SEQUENCE {
                ACL_ALL_RIGHTS_SEQUENCE
            } else {
                ACL_ALL_RIGHTS_RELATION
            }
        } else {
            istmt.privileges
        };

        if istmt.objtype == ObjectType::OBJECT_TABLE {
            if relkind == RELKIND_SEQUENCE {
                if this_privileges & !ACL_ALL_RIGHTS_SEQUENCE != 0 {
                    warn(
                        format!(
                            "sequence \"{relname}\" only supports USAGE, SELECT, and UPDATE privileges"
                        ),
                        ERRCODE_INVALID_GRANT_OPERATION,
                    )?;
                    this_privileges &= ACL_ALL_RIGHTS_SEQUENCE;
                }
            } else if this_privileges & !ACL_ALL_RIGHTS_RELATION != 0 {
                return Err(err(
                    "invalid privilege type USAGE for table".into(),
                    ERRCODE_INVALID_GRANT_OPERATION,
                ));
            }
        }

        // Column-privilege accumulator, entry [0] = FirstLowInvalidHeapAttributeNumber.
        let num_col_privileges = (relnatts as i32 - FIRST_LOW_INVALID_HEAP_ATTNUM + 1) as usize;
        let mut col_privileges: PgVec<'mcx, u64> = mcx::vec_with_capacity_in(mcx, num_col_privileges)?;
        col_privileges.resize(num_col_privileges, ACL_NO_RIGHTS);
        let mut have_col_privileges = false;

        // Revoking relation privileges that double as column privileges must
        // implicitly revoke them per column too (SQL spec).
        if !istmt.is_grant && (this_privileges & ACL_ALL_RIGHTS_COLUMN) != 0 {
            expand_all_col_privileges(
                rel_oid,
                relkind,
                relnatts,
                this_privileges & ACL_ALL_RIGHTS_COLUMN,
                &mut col_privileges,
            )?;
            have_col_privileges = true;
        }

        let (acl_datum, acl_is_null) = SysCacheGetAttr(RELOID, &tuple, ANUM_PG_CLASS_RELACL)?;
        let old_acl: PgVec<'mcx, AclItem> = if acl_is_null {
            let objtype = if relkind == RELKIND_SEQUENCE {
                AclObjectType::Sequence
            } else {
                AclObjectType::Table
            };
            adt_acl::aclcopy(mcx, acldefault(objtype, owner_id).as_slice())?
        } else {
            with_acl_datum(acl_datum, |acl| adt_acl::aclcopy(mcx, acl))?
        };
        let old_members: Option<PgVec<'mcx, Oid>> = if acl_is_null {
            None
        } else {
            Some(aclmembers(mcx, &old_acl)?)
        };

        let old_rel_acl = adt_acl::aclcopy(mcx, &old_acl)?;
        let otid = tuple.tuple().t_self;

        if this_privileges != ACL_NO_RIGHTS {
            let (grantor_id, avail_goptions) =
                select_best_grantor(miscinit::GetUserId(), this_privileges, &old_acl, owner_id)?;

            let objtype = if relkind == RELKIND_SEQUENCE {
                ObjectType::OBJECT_SEQUENCE
            } else {
                ObjectType::OBJECT_TABLE
            };

            let this_privileges = restrict_and_check_grant(
                istmt.is_grant,
                avail_goptions,
                istmt.all_privs,
                this_privileges,
                rel_oid,
                grantor_id,
                objtype,
                &relname,
                0,
                None,
            )?;

            let new_acl = merge_acl_with_grant(
                mcx,
                &old_acl,
                istmt.is_grant,
                istmt.grant_option,
                istmt.behavior,
                &istmt.grantees,
                this_privileges,
                grantor_id,
                owner_id,
            )?;

            let new_members = aclmembers(mcx, &new_acl)?;

            let natts = relation.descr().natts as usize;
            let mut values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut nulls: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replaces: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            nulls.resize(natts, false);
            replaces.resize(natts, false);
            let aidx = (ANUM_PG_CLASS_RELACL - 1) as usize;
            let acl_img = acl_image(mcx, &new_acl)?;
            values[aidx] = Datum::from_usize(acl_img.as_ptr() as usize);
            replaces[aidx] = true;

            let mut newtuple = heaptuple::heap_modify_tuple(
                mcx,
                &tuple.tuple(),
                relation.descr(),
                &values,
                &nulls,
                &replaces,
            )?;
            catalog_indexing::CatalogTupleUpdate(mcx, &relation, &otid, &mut newtuple)?;
            unlock_class_tuple(&otid)?;

            // recordExtensionInitPriv: no-op outside CREATE EXTENSION, which
            // is unported.

            pg_depend::updateAclDependencies(
                mcx,
                RELATION_RELATION_ID,
                rel_oid,
                0,
                owner_id,
                old_members.as_deref().unwrap_or(&[]),
                &new_members,
            )?;
        } else {
            unlock_class_tuple(&otid)?;
        }

        for col_privs in istmt.col_privs.iter() {
            let mut this_privileges = match col_privs.priv_name {
                None => ACL_ALL_RIGHTS_COLUMN,
                Some(name) => string_to_privilege(name)?,
            };
            if this_privileges & !ACL_ALL_RIGHTS_COLUMN != 0 {
                return Err(err(
                    format!(
                        "invalid privilege type {} for column",
                        privilege_to_string(this_privileges)
                    ),
                    ERRCODE_INVALID_GRANT_OPERATION,
                ));
            }
            if relkind == RELKIND_SEQUENCE && this_privileges & !ACL_SELECT != 0 {
                // Warning, not error, matching the relation-level rule.
                warn(
                    format!("sequence \"{relname}\" only supports SELECT column privileges"),
                    ERRCODE_INVALID_GRANT_OPERATION,
                )?;
                this_privileges &= ACL_SELECT;
            }
            expand_col_privileges(
                &col_privs.cols,
                rel_oid,
                &relname,
                this_privileges,
                &mut col_privileges,
            )?;
            have_col_privileges = true;
        }

        if have_col_privileges {
            for (idx, &privs) in col_privileges.iter().enumerate() {
                if privs == ACL_NO_RIGHTS {
                    continue;
                }
                let attnum = (idx as i32 + FIRST_LOW_INVALID_HEAP_ATTNUM) as i16;
                exec_grant_attribute(
                    mcx,
                    istmt,
                    rel_oid,
                    &relname,
                    attnum,
                    owner_id,
                    privs,
                    &att_relation,
                    &old_rel_acl,
                )?;
            }
        }

        ReleaseSysCache(tuple);

        // Prevent error when processing duplicate objects.
        xact::CommandCounterIncrement()?;
    }

    att_relation.close(RowExclusiveLock)?;
    relation.close(RowExclusiveLock)?;
    Ok(())
}

fn expand_col_privileges(
    colnames: &types_nodes::list::NodeList<'_>,
    rel_oid: Oid,
    relname: &str,
    this_privileges: u64,
    col_privileges: &mut [u64],
) -> PgResult<()> {
    const ANUM_PG_ATTRIBUTE_ATTNUM: i32 = 5;
    for cell in colnames.iter() {
        let colname = cell.as_string().expect("column name").sval;
        // get_attnum (lsyscache.c): SearchSysCacheAttName misses dropped rows.
        let attnum = match cache_syscache::SearchSysCacheAttName(rel_oid, colname)? {
            Some(tuple) => {
                let n =
                    SysCacheGetAttrNotNull(ATTNAME, &tuple, ANUM_PG_ATTRIBUTE_ATTNUM)?.as_i16();
                ReleaseSysCache(tuple);
                n
            }
            None => 0,
        };
        if attnum == 0 {
            return Err(err(
                format!("column \"{colname}\" of relation \"{relname}\" does not exist"),
                types_error::ERRCODE_UNDEFINED_COLUMN,
            ));
        }
        let idx = attnum as i32 - FIRST_LOW_INVALID_HEAP_ATTNUM;
        if idx <= 0 || idx as usize >= col_privileges.len() {
            return Err(Box::new(PgError::error("column number out of range".to_string())));
        }
        col_privileges[idx as usize] |= this_privileges;
    }
    Ok(())
}

fn expand_all_col_privileges(
    rel_oid: Oid,
    relkind: u8,
    relnatts: i16,
    this_privileges: u64,
    col_privileges: &mut [u64],
) -> PgResult<()> {
    for curr_att in (FIRST_LOW_INVALID_HEAP_ATTNUM + 1)..=(relnatts as i32) {
        if curr_att == 0 {
            continue;
        }
        // Views have no system columns.
        if relkind == RELKIND_VIEW && curr_att < 0 {
            continue;
        }
        let Some(att_tuple) = SearchSysCache2(
            ATTNUM,
            SysCacheKey::Value(Datum::from_oid(rel_oid)),
            SysCacheKey::Value(Datum::from_i16(curr_att as i16)),
        )?
        else {
            return Err(Box::new(PgError::error(format!(
                "cache lookup failed for attribute {curr_att} of relation {rel_oid}"
            ))));
        };
        let isdropped =
            SysCacheGetAttrNotNull(ATTNUM, &att_tuple, ANUM_PG_ATTRIBUTE_ATTISDROPPED)?.as_bool();
        ReleaseSysCache(att_tuple);
        if isdropped {
            continue;
        }
        col_privileges[(curr_att - FIRST_LOW_INVALID_HEAP_ATTNUM) as usize] |= this_privileges;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn exec_grant_attribute<'mcx>(
    mcx: Mcx<'mcx>,
    istmt: &InternalGrant<'_, '_>,
    rel_oid: Oid,
    relname: &str,
    attnum: i16,
    owner_id: Oid,
    col_privileges: u64,
    att_relation: &types_rel::Relation<'mcx>,
    old_rel_acl: &[AclItem],
) -> PgResult<()> {
    let Some(attr_tuple) = SearchSysCache2(
        ATTNUM,
        SysCacheKey::Value(Datum::from_oid(rel_oid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
    )?
    else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for attribute {attnum} of relation {rel_oid}"
        ))));
    };
    let attname = name_attr(ATTNUM, &attr_tuple, ANUM_PG_ATTRIBUTE_ATTNAME)?;

    let (acl_datum, isnull) = SysCacheGetAttr(ATTNUM, &attr_tuple, ANUM_PG_ATTRIBUTE_ATTACL)?;
    let old_acl: PgVec<'mcx, AclItem> = if isnull {
        adt_acl::aclcopy(mcx, acldefault(AclObjectType::Column, owner_id).as_slice())?
    } else {
        with_acl_datum(acl_datum, |acl| adt_acl::aclcopy(mcx, acl))?
    };
    let old_members: Option<PgVec<'mcx, Oid>> =
        if isnull { None } else { Some(aclmembers(mcx, &old_acl)?) };

    // select_best_grantor considers table-level bits as well as the
    // per-column ACL (cheap concatenation, duplicates are fine here).
    let merged_acl = aclconcat(mcx, old_rel_acl, &old_acl)?;
    let (grantor_id, avail_goptions) =
        select_best_grantor(miscinit::GetUserId(), col_privileges, &merged_acl, owner_id)?;

    let col_privileges = restrict_and_check_grant(
        istmt.is_grant,
        avail_goptions,
        col_privileges == ACL_ALL_RIGHTS_COLUMN,
        col_privileges,
        rel_oid,
        grantor_id,
        ObjectType::OBJECT_COLUMN,
        relname,
        attnum,
        Some(&attname),
    )?;

    let new_acl = merge_acl_with_grant(
        mcx,
        &old_acl,
        istmt.is_grant,
        istmt.grant_option,
        istmt.behavior,
        &istmt.grantees,
        col_privileges,
        grantor_id,
        owner_id,
    )?;
    let new_members = aclmembers(mcx, &new_acl)?;

    // An empty updated ACL becomes a NULL attacl; if it was already NULL the
    // pg_attribute row needs no update at all (the common relation-level
    // REVOKE path).
    let natts = att_relation.descr().natts as usize;
    let mut values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut nulls: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replaces: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    nulls.resize(natts, false);
    replaces.resize(natts, false);
    let aidx = (ANUM_PG_ATTRIBUTE_ATTACL - 1) as usize;
    let need_update;
    let acl_img;
    if !new_acl.is_empty() {
        acl_img = acl_image(mcx, &new_acl)?;
        values[aidx] = Datum::from_usize(acl_img.as_ptr() as usize);
        need_update = true;
    } else {
        nulls[aidx] = true;
        need_update = !isnull;
    }
    replaces[aidx] = true;

    if need_update {
        let mut newtuple = heaptuple::heap_modify_tuple(
            mcx,
            &attr_tuple.tuple(),
            att_relation.descr(),
            &values,
            &nulls,
            &replaces,
        )?;
        let otid = attr_tuple.tuple().t_self;
        catalog_indexing::CatalogTupleUpdate(mcx, att_relation, &otid, &mut newtuple)?;

        pg_depend::updateAclDependencies(
            mcx,
            RELATION_RELATION_ID,
            rel_oid,
            attnum as i32,
            owner_id,
            old_members.as_deref().unwrap_or(&[]),
            &new_members,
        )?;
    }

    ReleaseSysCache(attr_tuple);
    Ok(())
}

struct GrantClass {
    classid: Oid,
    cacheid: i32,
    owner_attnum: i32,
    acl_attnum: i32,
    name_attnum: i32,
    objtype: ObjectType,
    acl_objtype: AclObjectType,
    default_privs: u64,
    descr: &'static str,
}

const CLASS_DATABASE: GrantClass = GrantClass {
    classid: types_core::catalog::DATABASE_RELATION_ID,
    cacheid: cache_syscache::cacheinfo::DATABASEOID,
    owner_attnum: 3,
    acl_attnum: 18,
    name_attnum: 2,
    objtype: ObjectType::OBJECT_DATABASE,
    acl_objtype: AclObjectType::Database,
    default_privs: adt_acl::ACL_ALL_RIGHTS_DATABASE,
    descr: "database",
};

const CLASS_TABLESPACE: GrantClass = GrantClass {
    classid: types_core::catalog::TABLE_SPACE_RELATION_ID,
    cacheid: cache_syscache::cacheinfo::TABLESPACEOID,
    owner_attnum: 3,
    acl_attnum: 4,
    name_attnum: 2,
    objtype: ObjectType::OBJECT_TABLESPACE,
    acl_objtype: AclObjectType::Tablespace,
    default_privs: adt_acl::ACL_ALL_RIGHTS_TABLESPACE,
    descr: "tablespace",
};

const CLASS_TYPE: GrantClass = GrantClass {
    classid: types_core::catalog::TYPE_RELATION_ID,
    cacheid: cache_syscache::cacheinfo::TYPEOID,
    owner_attnum: 4,
    acl_attnum: 32,
    name_attnum: 2,
    objtype: ObjectType::OBJECT_TYPE,
    acl_objtype: AclObjectType::Type,
    default_privs: adt_acl::ACL_ALL_RIGHTS_TYPE,
    descr: "type",
};

const CLASS_PROC: GrantClass = GrantClass {
    classid: types_core::catalog::PROCEDURE_RELATION_ID,
    cacheid: cache_syscache::cacheinfo::PROCOID,
    owner_attnum: 4,
    acl_attnum: 30,
    name_attnum: 2,
    objtype: ObjectType::OBJECT_FUNCTION,
    acl_objtype: AclObjectType::Function,
    default_privs: adt_acl::ACL_ALL_RIGHTS_FUNCTION,
    descr: "function",
};

const CLASS_LANGUAGE: GrantClass = GrantClass {
    classid: types_core::catalog::LANGUAGE_RELATION_ID,
    cacheid: cache_syscache::cacheinfo::LANGOID,
    owner_attnum: 3,
    acl_attnum: 9,
    name_attnum: 2,
    objtype: ObjectType::OBJECT_LANGUAGE,
    acl_objtype: AclObjectType::Language,
    default_privs: adt_acl::ACL_ALL_RIGHTS_LANGUAGE,
    descr: "language",
};

const CLASS_NAMESPACE: GrantClass = GrantClass {
    classid: types_core::catalog::NAMESPACE_RELATION_ID,
    cacheid: cache_syscache::cacheinfo::NAMESPACEOID,
    owner_attnum: 3,
    acl_attnum: 4,
    name_attnum: 2,
    objtype: ObjectType::OBJECT_SCHEMA,
    acl_objtype: AclObjectType::Schema,
    default_privs: adt_acl::ACL_ALL_RIGHTS_SCHEMA,
    descr: "schema",
};

const CLASS_FDW: GrantClass = GrantClass {
    classid: types_core::FOREIGN_DATA_WRAPPER_RELATION_ID,
    cacheid: cache_syscache::cacheinfo::FOREIGNDATAWRAPPEROID,
    owner_attnum: 3,
    acl_attnum: 6,
    name_attnum: 2,
    objtype: ObjectType::OBJECT_FDW,
    acl_objtype: AclObjectType::Fdw,
    default_privs: ACL_ALL_RIGHTS_FDW,
    descr: "foreign-data wrapper",
};

const CLASS_FOREIGN_SERVER: GrantClass = GrantClass {
    classid: types_core::FOREIGN_SERVER_RELATION_ID,
    cacheid: cache_syscache::cacheinfo::FOREIGNSERVEROID,
    owner_attnum: 3,
    acl_attnum: 7,
    name_attnum: 2,
    objtype: ObjectType::OBJECT_FOREIGN_SERVER,
    acl_objtype: AclObjectType::ForeignServer,
    default_privs: ACL_ALL_RIGHTS_FOREIGN_SERVER,
    descr: "foreign server",
};

// RemoveRoleFromObjectACL (aclchk.c).
pub fn RemoveRoleFromObjectACL<'mcx>(
    mcx: Mcx<'mcx>,
    roleid: Oid,
    classid: Oid,
    objid: Oid,
) -> PgResult<()> {
    if classid == crate::defacl::DefaultAclRelationId {
        return crate::defacl::remove_role_from_default_acl(mcx, roleid, objid);
    }

    let objtype = match classid {
        RELATION_RELATION_ID => ObjectType::OBJECT_TABLE,
        DATABASE_RELATION_ID => ObjectType::OBJECT_DATABASE,
        TYPE_RELATION_ID => ObjectType::OBJECT_TYPE,
        PROCEDURE_RELATION_ID => ObjectType::OBJECT_ROUTINE,
        LANGUAGE_RELATION_ID => ObjectType::OBJECT_LANGUAGE,
        pg_largeobject::LargeObjectRelationId => ObjectType::OBJECT_LARGEOBJECT,
        NAMESPACE_RELATION_ID => ObjectType::OBJECT_SCHEMA,
        types_core::catalog::TABLE_SPACE_RELATION_ID => ObjectType::OBJECT_TABLESPACE,
        types_core::FOREIGN_SERVER_RELATION_ID => ObjectType::OBJECT_FOREIGN_SERVER,
        types_core::FOREIGN_DATA_WRAPPER_RELATION_ID => ObjectType::OBJECT_FDW,
        catalog::ParameterAclRelationId => ObjectType::OBJECT_PARAMETER_ACL,
        other => {
            return Err(Box::new(PgError::error(format!(
                "unexpected object class {other}"
            ))))
        }
    };

    let mut objects: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 1)?;
    objects.push(objid);
    let mut grantees: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 1)?;
    grantees.push(roleid);

    let mut istmt = InternalGrant {
        is_grant: false,
        objtype,
        objects,
        all_privs: true,
        privileges: ACL_NO_RIGHTS,
        col_privs: mcx::vec_new_in(mcx),
        grantees,
        grant_option: false,
        behavior: types_nodes::parsenodes::DropBehavior::DROP_CASCADE as i32,
    };
    exec_grant_stmt_oids(mcx, &mut istmt)
}

const InitPrivsRelationId: Oid = 3394;
const InitPrivsObjIndexId: Oid = 3395;
const Anum_pg_init_privs_objoid: types_core::AttrNumber = 1;
const Anum_pg_init_privs_classoid: types_core::AttrNumber = 2;
const Anum_pg_init_privs_objsubid: types_core::AttrNumber = 3;
const Anum_pg_init_privs_privtype: types_core::AttrNumber = 4;
const Anum_pg_init_privs_initprivs: types_core::AttrNumber = 5;
const Natts_pg_init_privs: usize = 5;

// objectaddress.c's get_object_catcache_oid/get_object_attnum_owner subset;
// hosted here because catalog_objectaddress depends on this crate.
fn init_priv_owner(classid: Oid, objid: Oid) -> PgResult<Oid> {
    let (cacheid, owner_attnum, descr) = if classid == RELATION_RELATION_ID {
        (RELOID, ANUM_PG_CLASS_RELOWNER, "relation")
    } else if classid == TYPE_RELATION_ID {
        (cache_syscache::cacheinfo::TYPEOID, 4, "type")
    } else if classid == CLASS_DATABASE.classid {
        (CLASS_DATABASE.cacheid, CLASS_DATABASE.owner_attnum, CLASS_DATABASE.descr)
    } else if classid == CLASS_TABLESPACE.classid {
        (CLASS_TABLESPACE.cacheid, CLASS_TABLESPACE.owner_attnum, CLASS_TABLESPACE.descr)
    } else if classid == CLASS_PROC.classid {
        (CLASS_PROC.cacheid, CLASS_PROC.owner_attnum, CLASS_PROC.descr)
    } else if classid == CLASS_LANGUAGE.classid {
        (CLASS_LANGUAGE.cacheid, CLASS_LANGUAGE.owner_attnum, CLASS_LANGUAGE.descr)
    } else if classid == CLASS_NAMESPACE.classid {
        (CLASS_NAMESPACE.cacheid, CLASS_NAMESPACE.owner_attnum, CLASS_NAMESPACE.descr)
    } else {
        panic!("RemoveRoleFromInitPriv (aclchk.c): owner lookup for object class {classid} unported")
    };

    let Some(tuple) = SearchSysCache1(cacheid, SysCacheKey::Value(Datum::from_oid(objid)))? else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for {descr} {objid}"
        ))));
    };
    let owner_id = SysCacheGetAttrNotNull(cacheid, &tuple, owner_attnum)?.as_oid();
    ReleaseSysCache(tuple);
    Ok(owner_id)
}

// RemoveRoleFromInitPriv (aclchk.c).
pub fn RemoveRoleFromInitPriv<'mcx>(
    mcx: Mcx<'mcx>,
    roleid: Oid,
    classid: Oid,
    objid: Oid,
    objsubid: i32,
) -> PgResult<()> {
    let rel = table::table_open(mcx, InitPrivsRelationId, RowExclusiveLock)?;
    let desc = rel.descr();

    let keys = [
        pg_largeobject::oid_key(Anum_pg_init_privs_objoid, objid),
        pg_largeobject::oid_key(Anum_pg_init_privs_classoid, classid),
        int4_key(Anum_pg_init_privs_objsubid, objsubid),
    ];
    let mut scan = genam::systable_beginscan(mcx, &rel, InitPrivsObjIndexId, true, None, &keys)?;

    let old = match genam::systable_getnext(mcx, &mut scan)? {
        None => None,
        Some(tup) => {
            let mut isnull = false;
            // SAFETY: fixed catalog columns under the relation's descriptor.
            let acl_datum = unsafe {
                types_tuple::heap_getattr(
                    tup,
                    Anum_pg_init_privs_initprivs as i32,
                    desc,
                    &mut isnull,
                )
            };
            debug_assert!(!isnull);
            let old_acl = with_acl_datum(acl_datum, |acl| adt_acl::aclcopy(mcx, acl))?;
            // SAFETY: as above.
            let privtype = unsafe {
                types_tuple::heap_getattr(
                    tup,
                    Anum_pg_init_privs_privtype as i32,
                    desc,
                    &mut isnull,
                )
            };
            Some((tup.t_self, old_acl, privtype))
        }
    };
    let Some((tid, old_acl, privtype)) = old else {
        genam::systable_endscan(mcx, scan)?;
        return rel.close(RowExclusiveLock);
    };

    let oldmembers = aclmembers(mcx, &old_acl)?;
    let owner_id = init_priv_owner(classid, objid)?;

    let new_acl = merge_acl_with_grant(
        mcx,
        &old_acl,
        false,
        false,
        types_nodes::parsenodes::DropBehavior::DROP_RESTRICT as i32,
        &[roleid],
        adt_acl::ACLITEM_ALL_PRIV_BITS,
        owner_id,
        owner_id,
    )?;

    if new_acl.is_empty() {
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    } else {
        // C heap_modify_tuple; an identical row image is rebuilt instead.
        let acl_img = acl_image(mcx, &new_acl)?;
        let values = [
            Datum::from_oid(objid),
            Datum::from_oid(classid),
            Datum::from_i32(objsubid),
            privtype,
            Datum::from_usize(acl_img.as_ptr() as usize),
        ];
        let nulls = [false; Natts_pg_init_privs];
        let mut newtuple = heaptuple::heap_form_tuple(mcx, desc, &values, &nulls)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &tid, &mut newtuple)?;
    }

    let newmembers = aclmembers(mcx, &new_acl)?;
    pg_shdepend::updateInitAclDependencies(mcx, classid, objid, objsubid, &oldmembers, &newmembers)?;

    genam::systable_endscan(mcx, scan)?;
    xact::CommandCounterIncrement()?;
    rel.close(RowExclusiveLock)
}

// ReplaceRoleInInitPriv (aclchk.c).
pub fn ReplaceRoleInInitPriv<'mcx>(
    mcx: Mcx<'mcx>,
    oldroleid: Oid,
    newroleid: Oid,
    classid: Oid,
    objid: Oid,
    objsubid: i32,
) -> PgResult<()> {
    let rel = table::table_open(mcx, InitPrivsRelationId, RowExclusiveLock)?;
    let desc = rel.descr();

    let keys = [
        pg_largeobject::oid_key(Anum_pg_init_privs_objoid, objid),
        pg_largeobject::oid_key(Anum_pg_init_privs_classoid, classid),
        int4_key(Anum_pg_init_privs_objsubid, objsubid),
    ];
    let mut scan = genam::systable_beginscan(mcx, &rel, InitPrivsObjIndexId, true, None, &keys)?;

    let old = match genam::systable_getnext(mcx, &mut scan)? {
        None => None,
        Some(tup) => {
            let mut isnull = false;
            // SAFETY: fixed catalog columns under the relation's descriptor.
            let acl_datum = unsafe {
                types_tuple::heap_getattr(
                    tup,
                    Anum_pg_init_privs_initprivs as i32,
                    desc,
                    &mut isnull,
                )
            };
            debug_assert!(!isnull);
            let old_acl = with_acl_datum(acl_datum, |acl| adt_acl::aclcopy(mcx, acl))?;
            // SAFETY: as above.
            let privtype = unsafe {
                types_tuple::heap_getattr(
                    tup,
                    Anum_pg_init_privs_privtype as i32,
                    desc,
                    &mut isnull,
                )
            };
            Some((tup.t_self, old_acl, privtype))
        }
    };
    let Some((tid, old_acl, privtype)) = old else {
        genam::systable_endscan(mcx, scan)?;
        return rel.close(RowExclusiveLock);
    };

    // This usage of aclnewowner is a bit off-label when oldroleid isn't the
    // owner, but it does the job fine.
    let new_acl = adt_acl::aclnewowner(mcx, &old_acl, oldroleid, newroleid)?;

    if new_acl.is_empty() {
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    } else {
        // C heap_modify_tuple; an identical row image is rebuilt instead.
        let acl_img = acl_image(mcx, &new_acl)?;
        let values = [
            Datum::from_oid(objid),
            Datum::from_oid(classid),
            Datum::from_i32(objsubid),
            privtype,
            Datum::from_usize(acl_img.as_ptr() as usize),
        ];
        let nulls = [false; Natts_pg_init_privs];
        let mut newtuple = heaptuple::heap_form_tuple(mcx, desc, &values, &nulls)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &tid, &mut newtuple)?;
    }

    let oldmembers = aclmembers(mcx, &old_acl)?;
    let newmembers = aclmembers(mcx, &new_acl)?;
    pg_shdepend::updateInitAclDependencies(mcx, classid, objid, objsubid, &oldmembers, &newmembers)?;

    genam::systable_endscan(mcx, scan)?;

    // prevent error when processing objects multiple times
    xact::CommandCounterIncrement()?;
    rel.close(RowExclusiveLock)
}

fn int4_key(attno: types_core::AttrNumber, v: i32) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_INT4EQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_INT4EQ) failed: {e:?}"));
    key.sk_argument = Datum::from_i32(v);
    key
}

fn unlock_catalog_tuple(cls: &GrantClass, tid: &ItemPointerData) -> PgResult<()> {
    let dbid = if catcache::cache_relisshared(cls.cacheid) {
        0
    } else {
        init_small::globals::MyDatabaseId()
    };
    let tag = LOCKTAG::tuple(
        dbid,
        cls.classid,
        types_tuple::ItemPointerGetBlockNumber(tid),
        types_tuple::ItemPointerGetOffsetNumber(tid),
    );
    lock_seams::lock_release::call(tag, InplaceUpdateTupleLock, false)?;
    Ok(())
}

// ExecGrant_Language_check (aclchk.c).
fn exec_grant_language_check(cls: &GrantClass, tuple: &catcache::CatCTuple) -> PgResult<()> {
    const ANUM_PG_LANGUAGE_LANPLTRUSTED: i32 = 5;
    let trusted =
        SysCacheGetAttrNotNull(cls.cacheid, tuple, ANUM_PG_LANGUAGE_LANPLTRUSTED)?.as_bool();
    if !trusted {
        let lanname = name_attr(cls.cacheid, tuple, 2)?;
        return Err(Box::new(
            elog::ereport(types_error::ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("language \"{lanname}\" is not trusted"))
                .errdetail(
                    "GRANT and REVOKE are not allowed on untrusted languages, because only \
                     superusers can use untrusted languages."
                        .to_string(),
                )
                .into_error(),
        ));
    }
    Ok(())
}

// ExecGrant_Type_check (aclchk.c).
fn exec_grant_type_check(cls: &GrantClass, tuple: &catcache::CatCTuple) -> PgResult<()> {
    const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;
    const TYPTYPE_MULTIRANGE: u8 = b'm';
    let typelem = SysCacheGetAttrNotNull(cls.cacheid, tuple, 14)?.as_oid();
    let typsubscript = SysCacheGetAttrNotNull(cls.cacheid, tuple, 13)?.as_oid();
    if typelem != Oid::default() && typsubscript == F_ARRAY_SUBSCRIPT_HANDLER {
        return Err(Box::new(
            elog::ereport(types_error::ERROR)
                .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                .errmsg("cannot set privileges of array types".to_string())
                .errhint("Set the privileges of the element type instead.".to_string())
                .into_error(),
        ));
    }
    let typtype = SysCacheGetAttrNotNull(cls.cacheid, tuple, 7)?.as_u8();
    if typtype == TYPTYPE_MULTIRANGE {
        return Err(Box::new(
            elog::ereport(types_error::ERROR)
                .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                .errmsg("cannot set privileges of multirange types".to_string())
                .errhint("Set the privileges of the range type instead.".to_string())
                .into_error(),
        ));
    }
    Ok(())
}

// ExecGrant_common (aclchk.c). recordExtensionInitPriv: no-op outside
// CREATE EXTENSION, which is unported.
fn exec_grant_common<'mcx>(
    mcx: Mcx<'mcx>,
    istmt: &mut InternalGrant<'_, '_>,
    cls: &GrantClass,
    object_check: Option<fn(&GrantClass, &catcache::CatCTuple) -> PgResult<()>>,
) -> PgResult<()> {
    if istmt.all_privs && istmt.privileges == ACL_NO_RIGHTS {
        istmt.privileges = cls.default_privs;
    }

    let relation = table::table_open(mcx, cls.classid, RowExclusiveLock)?;

    for i in 0..istmt.objects.len() {
        let objectid = istmt.objects[i];
        let Some(tuple) =
            SearchSysCacheLocked1(cls.cacheid, SysCacheKey::Value(Datum::from_oid(objectid)))?
        else {
            return Err(Box::new(PgError::error(format!(
                "cache lookup failed for {} {objectid}",
                cls.descr
            ))));
        };

        if let Some(check) = object_check {
            check(cls, &tuple)?;
        }

        let owner_id = SysCacheGetAttrNotNull(cls.cacheid, &tuple, cls.owner_attnum)?.as_oid();
        let (acl_datum, acl_is_null) = SysCacheGetAttr(cls.cacheid, &tuple, cls.acl_attnum)?;
        let old_acl: PgVec<'mcx, AclItem> = if acl_is_null {
            adt_acl::aclcopy(mcx, acldefault(cls.acl_objtype, owner_id).as_slice())?
        } else {
            with_acl_datum(acl_datum, |acl| adt_acl::aclcopy(mcx, acl))?
        };
        let old_members: Option<PgVec<'mcx, Oid>> =
            if acl_is_null { None } else { Some(aclmembers(mcx, &old_acl)?) };

        let (grantor_id, avail_goptions) =
            select_best_grantor(miscinit::GetUserId(), istmt.privileges, &old_acl, owner_id)?;

        let objname = name_attr(cls.cacheid, &tuple, cls.name_attnum)?;

        let this_privileges = restrict_and_check_grant(
            istmt.is_grant,
            avail_goptions,
            istmt.all_privs,
            istmt.privileges,
            objectid,
            grantor_id,
            cls.objtype,
            &objname,
            0,
            None,
        )?;

        let new_acl = merge_acl_with_grant(
            mcx,
            &old_acl,
            istmt.is_grant,
            istmt.grant_option,
            istmt.behavior,
            &istmt.grantees,
            this_privileges,
            grantor_id,
            owner_id,
        )?;
        let new_members = aclmembers(mcx, &new_acl)?;

        let natts = relation.descr().natts as usize;
        let mut values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut nulls: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replaces: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, Datum::null());
        nulls.resize(natts, false);
        replaces.resize(natts, false);
        let aidx = (cls.acl_attnum - 1) as usize;
        let acl_img = acl_image(mcx, &new_acl)?;
        values[aidx] = Datum::from_usize(acl_img.as_ptr() as usize);
        replaces[aidx] = true;

        let otid = tuple.tuple().t_self;
        let mut newtuple = heaptuple::heap_modify_tuple(
            mcx,
            &tuple.tuple(),
            relation.descr(),
            &values,
            &nulls,
            &replaces,
        )?;
        catalog_indexing::CatalogTupleUpdate(mcx, &relation, &otid, &mut newtuple)?;
        unlock_catalog_tuple(cls, &otid)?;

        pg_depend::updateAclDependencies(
            mcx,
            cls.classid,
            objectid,
            0,
            owner_id,
            old_members.as_deref().unwrap_or(&[]),
            &new_members,
        )?;

        ReleaseSysCache(tuple);

        xact::CommandCounterIncrement()?;
    }

    relation.close(RowExclusiveLock)?;
    Ok(())
}

// ExecGrant_Largeobject (aclchk.c); pg_largeobject_metadata has no syscache.
fn exec_grant_largeobject<'mcx>(mcx: Mcx<'mcx>, istmt: &mut InternalGrant<'_, '_>) -> PgResult<()> {
    use pg_largeobject::{
        Anum_pg_largeobject_metadata_lomacl, Anum_pg_largeobject_metadata_lomowner,
        Anum_pg_largeobject_metadata_oid, LargeObjectMetadataOidIndexId,
        LargeObjectMetadataRelationId, LargeObjectRelationId,
    };

    if istmt.all_privs && istmt.privileges == ACL_NO_RIGHTS {
        istmt.privileges = adt_acl::ACL_ALL_RIGHTS_LARGEOBJECT;
    }

    let relation = table::table_open(mcx, LargeObjectMetadataRelationId, RowExclusiveLock)?;

    for i in 0..istmt.objects.len() {
        let loid = istmt.objects[i];
        let skey = [pg_largeobject::oid_key(Anum_pg_largeobject_metadata_oid, loid)];
        let mut scan = genam::systable_beginscan(
            mcx,
            &relation,
            LargeObjectMetadataOidIndexId,
            true,
            None,
            &skey,
        )?;
        let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? else {
            return Err(Box::new(PgError::error(format!(
                "could not find tuple for large object {loid}"
            ))));
        };

        let desc = relation.descr();
        let mut isnull = false;
        // SAFETY: fixed catalog columns under the relation's own descriptor.
        let owner_id = unsafe {
            types_tuple::heap_getattr(
                tuple,
                Anum_pg_largeobject_metadata_lomowner as i32,
                desc,
                &mut isnull,
            )
        }
        .as_oid();
        let mut acl_is_null = false;
        // SAFETY: as above.
        let acl_datum = unsafe {
            types_tuple::heap_getattr(
                tuple,
                Anum_pg_largeobject_metadata_lomacl as i32,
                desc,
                &mut acl_is_null,
            )
        };
        let old_acl: PgVec<'mcx, AclItem> = if acl_is_null {
            adt_acl::aclcopy(mcx, acldefault(AclObjectType::LargeObject, owner_id).as_slice())?
        } else {
            with_acl_datum(acl_datum, |acl| adt_acl::aclcopy(mcx, acl))?
        };
        let old_members: Option<PgVec<'mcx, Oid>> =
            if acl_is_null { None } else { Some(aclmembers(mcx, &old_acl)?) };

        let (grantor_id, avail_goptions) =
            select_best_grantor(miscinit::GetUserId(), istmt.privileges, &old_acl, owner_id)?;

        let loname = format!("large object {loid}");
        let this_privileges = restrict_and_check_grant(
            istmt.is_grant,
            avail_goptions,
            istmt.all_privs,
            istmt.privileges,
            loid,
            grantor_id,
            ObjectType::OBJECT_LARGEOBJECT,
            &loname,
            0,
            None,
        )?;

        let new_acl = merge_acl_with_grant(
            mcx,
            &old_acl,
            istmt.is_grant,
            istmt.grant_option,
            istmt.behavior,
            &istmt.grantees,
            this_privileges,
            grantor_id,
            owner_id,
        )?;
        let new_members = aclmembers(mcx, &new_acl)?;

        let natts = desc.natts as usize;
        let mut values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut nulls: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replaces: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, Datum::null());
        nulls.resize(natts, false);
        replaces.resize(natts, false);
        let aidx = (Anum_pg_largeobject_metadata_lomacl - 1) as usize;
        let acl_img = acl_image(mcx, &new_acl)?;
        values[aidx] = Datum::from_usize(acl_img.as_ptr() as usize);
        replaces[aidx] = true;

        let otid = tuple.t_self;
        let mut newtuple =
            heaptuple::heap_modify_tuple(mcx, tuple, desc, &values, &nulls, &replaces)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &relation, &otid, &mut newtuple)?;

        pg_depend::updateAclDependencies(
            mcx,
            LargeObjectRelationId,
            loid,
            0,
            owner_id,
            old_members.as_deref().unwrap_or(&[]),
            &new_members,
        )?;

        genam::systable_endscan(mcx, scan)?;

        xact::CommandCounterIncrement()?;
    }

    relation.close(RowExclusiveLock)?;
    Ok(())
}

fn text_attr(d: Datum) -> String {
    use types_tuple::varatt;
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null text attr datum inside a held catalog tuple; length is
    // read from its own varlena header before slicing.
    unsafe {
        if varatt::varatt_is_1b_e(p) || (!varatt::varatt_is_1b(p) && !varatt::varatt_is_4b_u(p)) {
            panic!("ExecGrant_Parameter: compressed/external parname varlena — detoast gap");
        }
        let (off, len) = if varatt::varatt_is_1b(p) {
            (varatt::VARHDRSZ_SHORT, varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT)
        } else {
            (varatt::VARHDRSZ, varatt::varsize_4b(p) - varatt::VARHDRSZ)
        };
        String::from_utf8_lossy(core::slice::from_raw_parts(p.add(off), len)).into_owned()
    }
}

// ExecGrant_Parameter (aclchk.c). recordExtensionInitPriv: no-op outside
// CREATE EXTENSION, which is unported.
fn exec_grant_parameter<'mcx>(mcx: Mcx<'mcx>, istmt: &mut InternalGrant<'_, '_>) -> PgResult<()> {
    use pg_parameter_acl::{
        Anum_pg_parameter_acl_paracl, Anum_pg_parameter_acl_parname, Natts_pg_parameter_acl,
    };

    if istmt.all_privs && istmt.privileges == ACL_NO_RIGHTS {
        istmt.privileges = ACL_ALL_RIGHTS_PARAMETER_ACL;
    }

    let relation = table::table_open(mcx, catalog::ParameterAclRelationId, RowExclusiveLock)?;

    for i in 0..istmt.objects.len() {
        let parameter_id = istmt.objects[i];
        let Some(tuple) =
            SearchSysCache1(PARAMETERACLOID, SysCacheKey::Value(Datum::from_oid(parameter_id)))?
        else {
            return Err(Box::new(PgError::error(format!(
                "cache lookup failed for parameter ACL {parameter_id}"
            ))));
        };

        let parname = text_attr(SysCacheGetAttrNotNull(
            PARAMETERACLOID,
            &tuple,
            Anum_pg_parameter_acl_parname,
        )?);

        // All parameters belong to the bootstrap superuser.
        let owner_id = types_core::catalog::BOOTSTRAP_SUPERUSERID;

        let (acl_datum, acl_is_null) =
            SysCacheGetAttr(PARAMETERACLOID, &tuple, Anum_pg_parameter_acl_paracl)?;
        let old_acl: PgVec<'mcx, AclItem> = if acl_is_null {
            adt_acl::aclcopy(mcx, acldefault(AclObjectType::ParameterAcl, owner_id).as_slice())?
        } else {
            with_acl_datum(acl_datum, |acl| adt_acl::aclcopy(mcx, acl))?
        };
        let old_members: Option<PgVec<'mcx, Oid>> =
            if acl_is_null { None } else { Some(aclmembers(mcx, &old_acl)?) };

        let (grantor_id, avail_goptions) =
            select_best_grantor(miscinit::GetUserId(), istmt.privileges, &old_acl, owner_id)?;

        let this_privileges = restrict_and_check_grant(
            istmt.is_grant,
            avail_goptions,
            istmt.all_privs,
            istmt.privileges,
            parameter_id,
            grantor_id,
            ObjectType::OBJECT_PARAMETER_ACL,
            &parname,
            0,
            None,
        )?;

        let new_acl = merge_acl_with_grant(
            mcx,
            &old_acl,
            istmt.is_grant,
            istmt.grant_option,
            istmt.behavior,
            &istmt.grantees,
            this_privileges,
            grantor_id,
            owner_id,
        )?;
        let new_members = aclmembers(mcx, &new_acl)?;

        // A default-equal ACL row is degenerate: delete it instead.
        if adt_acl::aclequal(&new_acl, acldefault(AclObjectType::ParameterAcl, owner_id).as_slice())
        {
            catalog_indexing::CatalogTupleDelete(&relation, &tuple.tuple().t_self)?;
        } else {
            let mut values = [Datum::null(); Natts_pg_parameter_acl];
            let nulls = [false; Natts_pg_parameter_acl];
            let mut replaces = [false; Natts_pg_parameter_acl];
            let aidx = (Anum_pg_parameter_acl_paracl - 1) as usize;
            let acl_img = acl_image(mcx, &new_acl)?;
            values[aidx] = Datum::from_usize(acl_img.as_ptr() as usize);
            replaces[aidx] = true;

            let otid = tuple.tuple().t_self;
            let mut newtuple = heaptuple::heap_modify_tuple(
                mcx,
                &tuple.tuple(),
                relation.descr(),
                &values,
                &nulls,
                &replaces,
            )?;
            catalog_indexing::CatalogTupleUpdate(mcx, &relation, &otid, &mut newtuple)?;
        }

        pg_depend::updateAclDependencies(
            mcx,
            catalog::ParameterAclRelationId,
            parameter_id,
            0,
            owner_id,
            old_members.as_deref().unwrap_or(&[]),
            &new_members,
        )?;

        ReleaseSysCache(tuple);

        xact::CommandCounterIncrement()?;
    }

    relation.close(RowExclusiveLock)?;
    Ok(())
}
