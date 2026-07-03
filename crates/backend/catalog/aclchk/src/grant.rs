use adt_acl::{
    acl_grant_option_for, acl_option_to_privs, aclconcat, acldefault, aclitem_set_privs_goptions,
    aclmembers, aclupdate, select_best_grantor, varlena::acl_image, AclItem, AclObjectType,
    ACL_ALL_RIGHTS_COLUMN, ACL_ALL_RIGHTS_RELATION, ACL_ALL_RIGHTS_SEQUENCE, ACL_ALTER_SYSTEM,
    ACL_CONNECT, ACL_CREATE, ACL_CREATE_TEMP, ACL_DELETE, ACL_EXECUTE, ACL_ID_PUBLIC, ACL_INSERT,
    ACL_MAINTAIN, ACL_MODECHG_ADD, ACL_MODECHG_DEL, ACL_NO_RIGHTS, ACL_REFERENCES, ACL_SELECT,
    ACL_SET, ACL_TRIGGER, ACL_TRUNCATE, ACL_UPDATE, ACL_USAGE,
};
use cache_syscache::cacheinfo::{ATTNUM, RELOID};
use cache_syscache::{
    ReleaseSysCache, SearchSysCache2, SearchSysCacheLocked1, SysCacheGetAttr,
    SysCacheGetAttrNotNull, SysCacheKey,
};
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::catalog::{ATTRIBUTE_RELATION_ID, RELATION_RELATION_ID};
use types_core::Oid;
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
    ANUM_PG_CLASS_RELACL, ANUM_PG_CLASS_RELNATTS,
};

const ANUM_PG_CLASS_RELNAME: i32 = 2;
const ANUM_PG_CLASS_RELOWNER: i32 = 6;
const ANUM_PG_CLASS_RELKIND: i32 = 18;
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
    col_privs: PgVec<'mcx, &'a AccessPriv<'mcx>>,
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
        .finish(types_error::ErrorLocation::new("aclchk.c", 0, "ExecuteGrantStmt"))
}

// get_rolespec_oid (acl.c).
fn get_rolespec_oid(role: &RoleSpec<'_>, missing_ok: bool) -> PgResult<Oid> {
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

fn string_to_privilege(privname: &str) -> PgResult<u64> {
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

fn privilege_to_string(privilege: u64) -> &'static str {
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
fn merge_acl_with_grant<'mcx>(
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

// restrict_and_check_grant (aclchk.c), the arms reachable via GRANT/REVOKE ON
// TABLE/SEQUENCE (plus the implicit column expansion).
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
            object_names_to_oids(mcx, stmt.objtype, &stmt.objects)?
        }
        other => panic!(
            "ExecuteGrantStmt (aclchk.c): targtype {} unported (ALL ... IN SCHEMA lane)",
            other as i32
        ),
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

    let all_privileges = match stmt.objtype {
        // GRANT TABLE may target a sequence: test the union, refine later.
        ObjectType::OBJECT_TABLE => ACL_ALL_RIGHTS_RELATION | ACL_ALL_RIGHTS_SEQUENCE,
        ObjectType::OBJECT_SEQUENCE => ACL_ALL_RIGHTS_SEQUENCE,
        other => panic!(
            "ExecuteGrantStmt (aclchk.c): object type {} unported (non-table grant lane)",
            other as i32
        ),
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
                panic!("ExecuteGrantStmt (aclchk.c): column privilege list unported (column-priv lane)");
            }
            let priv_name = privnode
                .priv_name
                .expect("AccessPriv node must specify privilege or columns");
            let privilege = string_to_privilege(priv_name)?;
            if privilege & !all_privileges != 0 {
                return Err(err(
                    format!(
                        "invalid privilege type {} for relation",
                        privilege_to_string(privilege)
                    ),
                    ERRCODE_INVALID_GRANT_OPERATION,
                ));
            }
            istmt.privileges |= privilege;
        }
    }

    exec_grant_relation(mcx, &mut istmt)
}

fn object_names_to_oids<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    objnames: &types_nodes::list::NodeList<'_>,
) -> PgResult<PgVec<'mcx, Oid>> {
    debug_assert!(matches!(objtype, ObjectType::OBJECT_TABLE | ObjectType::OBJECT_SEQUENCE));
    let mut objects: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, objnames.len())?;
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
    Ok(objects)
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
                RELATION_RELATION_ID,
                rel_oid,
                0,
                owner_id,
                old_members.as_deref().unwrap_or(&[]),
                &new_members,
            );
        } else {
            unlock_class_tuple(&otid)?;
        }

        debug_assert!(istmt.col_privs.is_empty());
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
            RELATION_RELATION_ID,
            rel_oid,
            attnum as i32,
            owner_id,
            old_members.as_deref().unwrap_or(&[]),
            &new_members,
        );
    }

    ReleaseSysCache(attr_tuple);
    Ok(())
}
