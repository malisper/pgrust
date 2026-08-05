use adt_acl::{
    acldefault, aclequal, aclitemsort, aclmembers, has_privs_of_role, varlena::acl_image, AclItem,
    AclObjectType, ACL_ALL_RIGHTS_FUNCTION, ACL_ALL_RIGHTS_LARGEOBJECT, ACL_ALL_RIGHTS_RELATION,
    ACL_ALL_RIGHTS_SCHEMA, ACL_ALL_RIGHTS_SEQUENCE, ACL_ALL_RIGHTS_TYPE, ACL_ID_PUBLIC,
    ACL_NO_RIGHTS,
};
use cache_syscache::cacheinfo::DEFACLROLENSPOBJ;
use cache_syscache::{ReleaseSysCache, SearchSysCache3, SysCacheGetAttr, SysCacheGetAttrNotNull, SysCacheKey};
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid, NAMESPACE_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_GRANT_OPERATION,
    ERRCODE_SYNTAX_ERROR,
};
use types_nodes::parsenodes::{
    AlterDefaultPrivilegesStmt, DropBehavior, ObjectType, RoleSpecType,
};
use types_rel::RowExclusiveLock;

use crate::grant::{
    get_rolespec_oid, merge_acl_with_grant, privilege_to_string, string_to_privilege,
};
use crate::with_acl_datum;

pub const DefaultAclRelationId: Oid = 826;
pub const DefaultAclOidIndexId: Oid = 828;
const Anum_pg_default_acl_oid: AttrNumber = 1;
const Anum_pg_default_acl_defaclrole: AttrNumber = 2;
const Anum_pg_default_acl_defaclnamespace: AttrNumber = 3;
const Anum_pg_default_acl_defaclobjtype: AttrNumber = 4;
const Anum_pg_default_acl_defaclacl: AttrNumber = 5;
const Natts_pg_default_acl: usize = 5;

const DEFACLOBJ_RELATION: u8 = b'r';
const DEFACLOBJ_SEQUENCE: u8 = b'S';
const DEFACLOBJ_FUNCTION: u8 = b'f';
const DEFACLOBJ_TYPE: u8 = b'T';
const DEFACLOBJ_NAMESPACE: u8 = b'n';
const DEFACLOBJ_LARGEOBJECT: u8 = b'L';

fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

struct InternalDefaultACL<'mcx> {
    roleid: Oid,
    nspid: Oid,
    is_grant: bool,
    objtype: ObjectType,
    all_privs: bool,
    privileges: u64,
    grantees: PgVec<'mcx, Oid>,
    grant_option: bool,
    behavior: i32,
}

// ExecAlterDefaultPrivilegesStmt (aclchk.c).
pub fn ExecAlterDefaultPrivilegesStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterDefaultPrivilegesStmt<'_>,
) -> PgResult<()> {
    let action = stmt.action.expect("AlterDefaultPrivilegesStmt without action");

    let mut dnspnames = None;
    let mut drolespecs = None;
    for cell in stmt.options.iter() {
        let defel = cell.as_def_elem().expect("DefElem");
        match defel.defname.unwrap_or("") {
            "schemas" => {
                if dnspnames.is_some() {
                    return Err(err("conflicting or redundant options".into(), ERRCODE_SYNTAX_ERROR));
                }
                dnspnames = defel.arg;
            }
            "roles" => {
                if drolespecs.is_some() {
                    return Err(err("conflicting or redundant options".into(), ERRCODE_SYNTAX_ERROR));
                }
                drolespecs = defel.arg;
            }
            other => {
                return Err(Box::new(PgError::error(format!(
                    "option \"{other}\" not recognized"
                ))))
            }
        }
    }
    let nspnames = dnspnames.map(|n| n.as_list().expect("schemas list"));
    let rolespecs = drolespecs.map(|n| n.as_list().expect("roles list"));

    let mut grantees: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, action.grantees.len())?;
    for cell in action.grantees.iter() {
        let grantee = cell.as_role_spec().expect("grantee RoleSpec");
        let uid = match grantee.roletype {
            RoleSpecType::ROLESPEC_PUBLIC => ACL_ID_PUBLIC,
            _ => get_rolespec_oid(grantee, false)?,
        };
        grantees.push(uid);
    }

    let (all_privileges, what) = match action.objtype {
        ObjectType::OBJECT_TABLE => (ACL_ALL_RIGHTS_RELATION, "relation"),
        ObjectType::OBJECT_SEQUENCE => (ACL_ALL_RIGHTS_SEQUENCE, "sequence"),
        ObjectType::OBJECT_FUNCTION => (ACL_ALL_RIGHTS_FUNCTION, "function"),
        ObjectType::OBJECT_PROCEDURE => (ACL_ALL_RIGHTS_FUNCTION, "procedure"),
        ObjectType::OBJECT_ROUTINE => (ACL_ALL_RIGHTS_FUNCTION, "routine"),
        ObjectType::OBJECT_TYPE => (ACL_ALL_RIGHTS_TYPE, "type"),
        ObjectType::OBJECT_SCHEMA => (ACL_ALL_RIGHTS_SCHEMA, "schema"),
        ObjectType::OBJECT_LARGEOBJECT => (ACL_ALL_RIGHTS_LARGEOBJECT, "large object"),
        other => {
            return Err(Box::new(PgError::error(format!(
                "unrecognized GrantStmt.objtype: {}",
                other as i32
            ))))
        }
    };

    let mut iacls = InternalDefaultACL {
        roleid: InvalidOid,
        nspid: InvalidOid,
        is_grant: action.is_grant,
        objtype: action.objtype,
        all_privs: false,
        privileges: ACL_NO_RIGHTS,
        grantees,
        grant_option: action.grant_option,
        behavior: action.behavior as i32,
    };

    if action.privileges.is_nil() {
        iacls.all_privs = true;
    } else {
        for cell in action.privileges.iter() {
            let privnode = cell.as_access_priv().expect("AccessPriv");
            if !privnode.cols.is_nil() {
                return Err(err(
                    "default privileges cannot be set for columns".into(),
                    ERRCODE_INVALID_GRANT_OPERATION,
                ));
            }
            let priv_name = privnode.priv_name.expect("AccessPriv node must specify privilege");
            let privilege = string_to_privilege(priv_name)?;
            if privilege & !all_privileges != 0 {
                return Err(err(
                    format!("invalid privilege type {} for {what}", privilege_to_string(privilege)),
                    ERRCODE_INVALID_GRANT_OPERATION,
                ));
            }
            iacls.privileges |= privilege;
        }
    }

    match rolespecs {
        None => {
            iacls.roleid = miscinit::GetUserId();
            SetDefaultACLsInSchemas(mcx, &mut iacls, nspnames)?;
        }
        Some(rolespecs) => {
            for rolecell in rolespecs.iter() {
                let rolespec = rolecell.as_role_spec().expect("RoleSpec");
                iacls.roleid = get_rolespec_oid(rolespec, false)?;
                if !has_privs_of_role(miscinit::GetUserId(), iacls.roleid)? {
                    return Err(err(
                        "permission denied to change default privileges".into(),
                        ERRCODE_INSUFFICIENT_PRIVILEGE,
                    ));
                }
                SetDefaultACLsInSchemas(mcx, &mut iacls, nspnames)?;
            }
        }
    }
    Ok(())
}

fn SetDefaultACLsInSchemas<'mcx>(
    mcx: Mcx<'mcx>,
    iacls: &mut InternalDefaultACL<'_>,
    nspnames: Option<&types_nodes::list::NodeList<'_>>,
) -> PgResult<()> {
    match nspnames {
        None => {
            iacls.nspid = InvalidOid;
            SetDefaultACL(mcx, iacls)
        }
        Some(nspnames) => {
            for nspcell in nspnames.iter() {
                let nspname = nspcell.as_string().expect("schema name").sval;
                iacls.nspid = catalog_namespace::get_namespace_oid(nspname, false)?;
                SetDefaultACL(mcx, iacls)?;
            }
            Ok(())
        }
    }
}

// RemoveRoleFromObjectACL's pg_default_acl branch (aclchk.c).
pub(crate) fn remove_role_from_default_acl<'mcx>(
    mcx: Mcx<'mcx>,
    roleid: Oid,
    objid: Oid,
) -> PgResult<()> {
    use types_rel::AccessShareLock;

    let rel = table::table_open(mcx, DefaultAclRelationId, AccessShareLock)?;
    let skey = [pg_largeobject::oid_key(Anum_pg_default_acl_oid, objid)];
    let mut scan = genam::systable_beginscan(mcx, &rel, DefaultAclOidIndexId, true, None, &skey)?;
    let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(PgError::error(format!(
            "could not find tuple for default ACL {objid}"
        ))));
    };

    let desc = rel.descr();
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_default_acl columns under the relation's
    // descriptor.
    let (defaclrole, defaclnamespace, defaclobjtype) = unsafe {
        (
            types_tuple::heap_getattr(
                tuple,
                Anum_pg_default_acl_defaclrole as i32,
                desc,
                &mut isnull,
            )
            .as_oid(),
            types_tuple::heap_getattr(
                tuple,
                Anum_pg_default_acl_defaclnamespace as i32,
                desc,
                &mut isnull,
            )
            .as_oid(),
            types_tuple::heap_getattr(
                tuple,
                Anum_pg_default_acl_defaclobjtype as i32,
                desc,
                &mut isnull,
            )
            .as_i8() as u8,
        )
    };

    let objtype = match defaclobjtype {
        DEFACLOBJ_RELATION => ObjectType::OBJECT_TABLE,
        DEFACLOBJ_SEQUENCE => ObjectType::OBJECT_SEQUENCE,
        DEFACLOBJ_FUNCTION => ObjectType::OBJECT_FUNCTION,
        DEFACLOBJ_TYPE => ObjectType::OBJECT_TYPE,
        DEFACLOBJ_NAMESPACE => ObjectType::OBJECT_SCHEMA,
        DEFACLOBJ_LARGEOBJECT => ObjectType::OBJECT_LARGEOBJECT,
        other => {
            return Err(Box::new(PgError::error(format!(
                "unexpected default ACL type: {}",
                other as i32
            ))))
        }
    };

    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;

    let mut grantees: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 1)?;
    grantees.push(roleid);
    let iacls = InternalDefaultACL {
        roleid: defaclrole,
        nspid: defaclnamespace,
        is_grant: false,
        objtype,
        all_privs: true,
        privileges: ACL_NO_RIGHTS,
        grantees,
        grant_option: false,
        behavior: DropBehavior::DROP_CASCADE as i32,
    };
    SetDefaultACL(mcx, &iacls)
}

fn SetDefaultACL<'mcx>(mcx: Mcx<'mcx>, iacls: &InternalDefaultACL<'_>) -> PgResult<()> {
    let mut this_privileges = iacls.privileges;

    let rel = table::table_open(mcx, DefaultAclRelationId, RowExclusiveLock)?;

    // Global entries replace the hard-wired per-type defaults; per-schema
    // entries add onto them, so their baseline is empty.
    let mut def_acl: PgVec<'mcx, AclItem> = if iacls.nspid == InvalidOid {
        let objtype = match iacls.objtype {
            ObjectType::OBJECT_TABLE => AclObjectType::Table,
            ObjectType::OBJECT_SEQUENCE => AclObjectType::Sequence,
            ObjectType::OBJECT_FUNCTION => AclObjectType::Function,
            ObjectType::OBJECT_TYPE => AclObjectType::Type,
            ObjectType::OBJECT_SCHEMA => AclObjectType::Schema,
            ObjectType::OBJECT_LARGEOBJECT => AclObjectType::LargeObject,
            other => panic!("SetDefaultACL: unrecognized object type {}", other as i32),
        };
        adt_acl::aclcopy(mcx, acldefault(objtype, iacls.roleid).as_slice())?
    } else {
        mcx::vec_new_in(mcx)
    };

    let objtype = match iacls.objtype {
        ObjectType::OBJECT_TABLE => {
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_RELATION;
            }
            DEFACLOBJ_RELATION
        }
        ObjectType::OBJECT_SEQUENCE => {
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_SEQUENCE;
            }
            DEFACLOBJ_SEQUENCE
        }
        ObjectType::OBJECT_FUNCTION => {
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_FUNCTION;
            }
            DEFACLOBJ_FUNCTION
        }
        ObjectType::OBJECT_TYPE => {
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_TYPE;
            }
            DEFACLOBJ_TYPE
        }
        ObjectType::OBJECT_SCHEMA => {
            if iacls.nspid != InvalidOid {
                return Err(err(
                    "cannot use IN SCHEMA clause when using GRANT/REVOKE ON SCHEMAS".into(),
                    ERRCODE_INVALID_GRANT_OPERATION,
                ));
            }
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_SCHEMA;
            }
            DEFACLOBJ_NAMESPACE
        }
        ObjectType::OBJECT_LARGEOBJECT => {
            if iacls.nspid != InvalidOid {
                return Err(err(
                    "cannot use IN SCHEMA clause when using GRANT/REVOKE ON LARGE OBJECTS".into(),
                    ERRCODE_INVALID_GRANT_OPERATION,
                ));
            }
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_LARGEOBJECT;
            }
            DEFACLOBJ_LARGEOBJECT
        }
        other => panic!("SetDefaultACL: unrecognized object type {}", other as i32),
    };

    let tuple = SearchSysCache3(
        DEFACLROLENSPOBJ,
        SysCacheKey::Value(Datum::from_oid(iacls.roleid)),
        SysCacheKey::Value(Datum::from_oid(iacls.nspid)),
        SysCacheKey::Value(Datum::from_i8(objtype as i8)),
    )?;

    let mut old_acl: Option<PgVec<'mcx, AclItem>> = None;
    if let Some(tuple) = &tuple {
        let (acl_datum, is_null) =
            SysCacheGetAttr(DEFACLROLENSPOBJ, tuple, Anum_pg_default_acl_defaclacl as i32)?;
        if !is_null {
            old_acl = Some(with_acl_datum(acl_datum, |acl| adt_acl::aclcopy(mcx, acl))?);
        }
    }
    let is_new = tuple.is_none();

    let old_members: Option<PgVec<'mcx, Oid>> = match &old_acl {
        Some(acl) => Some(aclmembers(mcx, acl)?),
        None => None,
    };
    let old_acl = match old_acl {
        Some(acl) => acl,
        None => adt_acl::aclcopy(mcx, &def_acl)?,
    };

    // Grantor of default rights is always the target role itself.
    let mut new_acl = merge_acl_with_grant(
        mcx,
        &old_acl,
        iacls.is_grant,
        iacls.grant_option,
        iacls.behavior,
        &iacls.grantees,
        this_privileges,
        iacls.roleid,
        iacls.roleid,
    )?;

    // A result equal to the type's default needs no pg_default_acl row —
    // and must delete a tracked row (else acldefault() would double-apply).
    aclitemsort(&mut new_acl);
    aclitemsort(&mut def_acl);
    if aclequal(&new_acl, &def_acl) {
        if !is_new {
            let tuple = tuple.as_ref().expect("tracked pg_default_acl row");
            let defacl_oid =
                SysCacheGetAttrNotNull(DEFACLROLENSPOBJ, tuple, Anum_pg_default_acl_oid as i32)?
                    .as_oid();
            dependency_seams::perform_deletion::call(
                mcx,
                DefaultAclRelationId,
                defacl_oid,
                0,
                DropBehavior::DROP_RESTRICT,
                0,
            )?;
        }
    } else {
        let mut values = [Datum::null(); Natts_pg_default_acl];
        let mut nulls = [false; Natts_pg_default_acl];
        let mut replaces = [false; Natts_pg_default_acl];
        let acl_img = acl_image(mcx, &new_acl)?;
        let defacl_oid;

        if is_new {
            defacl_oid = catalog::GetNewOidWithIndex(
                mcx,
                &rel,
                DefaultAclOidIndexId,
                Anum_pg_default_acl_oid,
            )?;
            values[Anum_pg_default_acl_oid as usize - 1] = Datum::from_oid(defacl_oid);
            values[Anum_pg_default_acl_defaclrole as usize - 1] = Datum::from_oid(iacls.roleid);
            values[Anum_pg_default_acl_defaclnamespace as usize - 1] =
                Datum::from_oid(iacls.nspid);
            values[Anum_pg_default_acl_defaclobjtype as usize - 1] =
                Datum::from_i8(objtype as i8);
            values[Anum_pg_default_acl_defaclacl as usize - 1] =
                Datum::from_usize(acl_img.as_ptr() as usize);
            let mut newtuple = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
            catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut newtuple)?;
        } else {
            let tuple = tuple.as_ref().expect("tracked pg_default_acl row");
            defacl_oid =
                SysCacheGetAttrNotNull(DEFACLROLENSPOBJ, tuple, Anum_pg_default_acl_oid as i32)?
                    .as_oid();
            values[Anum_pg_default_acl_defaclacl as usize - 1] =
                Datum::from_usize(acl_img.as_ptr() as usize);
            replaces[Anum_pg_default_acl_defaclacl as usize - 1] = true;
            let mut newtuple = heaptuple::heap_modify_tuple(
                mcx,
                &tuple.tuple(),
                rel.descr(),
                &values,
                &nulls,
                &replaces,
            )?;
            let otid = tuple.tuple().t_self;
            catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtuple)?;
        }

        if is_new {
            pg_depend::recordDependencyOnOwner(mcx, DefaultAclRelationId, defacl_oid, iacls.roleid)?;
            if iacls.nspid != InvalidOid {
                let myself = pg_depend::ObjectAddress {
                    classId: DefaultAclRelationId,
                    objectId: defacl_oid,
                    objectSubId: 0,
                };
                let referenced = pg_depend::ObjectAddress {
                    classId: NAMESPACE_RELATION_ID,
                    objectId: iacls.nspid,
                    objectSubId: 0,
                };
                pg_depend::recordDependencyOn(
                    mcx,
                    &myself,
                    &referenced,
                    pg_depend::DependencyType::Auto,
                )?;
            }
        }

        let new_members = aclmembers(mcx, &new_acl)?;
        pg_depend::updateAclDependencies(
            mcx,
            DefaultAclRelationId,
            defacl_oid,
            0,
            iacls.roleid,
            old_members.as_deref().unwrap_or(&[]),
            &new_members,
        )?;
    }

    if let Some(tuple) = tuple {
        ReleaseSysCache(tuple);
    }
    rel.close(RowExclusiveLock)?;

    // Prevent error when processing duplicate objects.
    xact::CommandCounterIncrement()
}

fn defaclobjtype_for(objtype: ObjectType) -> Option<u8> {
    Some(match objtype {
        ObjectType::OBJECT_TABLE => DEFACLOBJ_RELATION,
        ObjectType::OBJECT_SEQUENCE => DEFACLOBJ_SEQUENCE,
        ObjectType::OBJECT_FUNCTION => DEFACLOBJ_FUNCTION,
        ObjectType::OBJECT_TYPE => DEFACLOBJ_TYPE,
        ObjectType::OBJECT_SCHEMA => DEFACLOBJ_NAMESPACE,
        ObjectType::OBJECT_LARGEOBJECT => DEFACLOBJ_LARGEOBJECT,
        _ => return None,
    })
}

fn get_default_acl_internal<'mcx>(
    mcx: Mcx<'mcx>,
    role_id: Oid,
    nsp_oid: Oid,
    objtype: u8,
) -> PgResult<Option<PgVec<'mcx, AclItem>>> {
    let Some(tuple) = SearchSysCache3(
        DEFACLROLENSPOBJ,
        SysCacheKey::Value(Datum::from_oid(role_id)),
        SysCacheKey::Value(Datum::from_oid(nsp_oid)),
        SysCacheKey::Value(Datum::from_i8(objtype as i8)),
    )?
    else {
        return Ok(None);
    };
    let (acl_datum, is_null) =
        SysCacheGetAttr(DEFACLROLENSPOBJ, &tuple, Anum_pg_default_acl_defaclacl as i32)?;
    let result = if is_null {
        None
    } else {
        Some(with_acl_datum(acl_datum, |acl| adt_acl::aclcopy(mcx, acl))?)
    };
    ReleaseSysCache(tuple);
    Ok(result)
}

// get_user_default_acl (aclchk.c): None means use the built-in defaults.
pub fn get_user_default_acl<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    owner_id: Oid,
    nsp_oid: Oid,
) -> PgResult<Option<PgVec<'mcx, AclItem>>> {
    if miscinit::IsBootstrapProcessingMode() {
        return Ok(None);
    }
    let Some(defaclobjtype) = defaclobjtype_for(objtype) else {
        return Ok(None);
    };

    let glob_acl = get_default_acl_internal(mcx, owner_id, InvalidOid, defaclobjtype)?;
    let schema_acl = get_default_acl_internal(mcx, owner_id, nsp_oid, defaclobjtype)?;
    if glob_acl.is_none() && schema_acl.is_none() {
        return Ok(None);
    }

    let aclobjtype = match objtype {
        ObjectType::OBJECT_TABLE => AclObjectType::Table,
        ObjectType::OBJECT_SEQUENCE => AclObjectType::Sequence,
        ObjectType::OBJECT_FUNCTION => AclObjectType::Function,
        ObjectType::OBJECT_TYPE => AclObjectType::Type,
        ObjectType::OBJECT_SCHEMA => AclObjectType::Schema,
        ObjectType::OBJECT_LARGEOBJECT => AclObjectType::LargeObject,
        _ => unreachable!("defaclobjtype_for filtered"),
    };
    let mut def_acl = adt_acl::aclcopy(mcx, acldefault(aclobjtype, owner_id).as_slice())?;

    let glob_acl = match glob_acl {
        Some(acl) => acl,
        None => adt_acl::aclcopy(mcx, &def_acl)?,
    };
    let mut result = adt_acl::aclmerge(
        mcx,
        &glob_acl,
        schema_acl.as_deref().unwrap_or(&[]),
        owner_id,
    )?;

    aclitemsort(&mut result);
    aclitemsort(&mut def_acl);
    if aclequal(&result, &def_acl) {
        return Ok(None);
    }
    Ok(Some(result))
}

// recordDependencyOnNewAcl (aclchk.c), acl passed as its varlena image.
fn record_dependency_on_new_acl<'mcx>(
    mcx: Mcx<'mcx>,
    class_id: Oid,
    object_id: Oid,
    objsub_id: i32,
    owner_id: Oid,
    acl_img: &[u8],
) -> PgResult<()> {
    use types_tuple::varatt;
    debug_assert!(acl_img.len() >= varatt::VARHDRSZ);
    let payload = &acl_img[varatt::VARHDRSZ..];
    let n = adt_acl::varlena::check_acl_payload(payload)?;
    let mut acl: PgVec<'mcx, AclItem> = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        acl.push(adt_acl::varlena::read_acl_item(payload, i));
    }
    let members = aclmembers(mcx, &acl)?;
    pg_depend::updateAclDependencies(mcx, class_id, object_id, objsub_id, owner_id, &[], &members)
}

fn seam_get_user_default_acl<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: u8,
    owner_id: Oid,
    nsp_oid: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let objtype = match objtype {
        DEFACLOBJ_RELATION => ObjectType::OBJECT_TABLE,
        DEFACLOBJ_SEQUENCE => ObjectType::OBJECT_SEQUENCE,
        DEFACLOBJ_FUNCTION => ObjectType::OBJECT_FUNCTION,
        DEFACLOBJ_TYPE => ObjectType::OBJECT_TYPE,
        DEFACLOBJ_NAMESPACE => ObjectType::OBJECT_SCHEMA,
        DEFACLOBJ_LARGEOBJECT => ObjectType::OBJECT_LARGEOBJECT,
        other => panic!("get_user_default_acl seam: bad DEFACLOBJ char {other}"),
    };
    match get_user_default_acl(mcx, objtype, owner_id, nsp_oid)? {
        Some(acl) => Ok(Some(acl_image(mcx, &acl)?)),
        None => Ok(None),
    }
}

pub fn init_seams() {
    aclchk_seams::get_user_default_acl::set(seam_get_user_default_acl);
    aclchk_seams::record_dependency_on_new_acl::set(record_dependency_on_new_acl);
}
