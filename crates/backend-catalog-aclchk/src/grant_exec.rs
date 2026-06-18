//! `catalog/aclchk.c` — the GRANT/REVOKE executor (F2), bounded to the
//! `OBJECT_SCHEMA` path that `pg_regress test_setup` exercises
//! (`GRANT ALL ON SCHEMA public TO public`).
//!
//! This ports `ExecuteGrantStmt` + `ExecGrantStmt_oids` + `ExecGrant_common`
//! + `merge_acl_with_grant` + `restrict_and_check_grant` + the schema leg of
//! `objectNamesToOids` + the early-out half of `recordExtensionInitPriv`. The
//! ACL bit work is the merged `backend-utils-adt-acl` (`acldefault`,
//! `aclmembers`, `aclupdate`, `select_best_grantor`) over the `&[AclItem]`
//! slice model; the catalog read/write is `SearchSysCacheLocked1` +
//! `SysCacheGetAttr` + `heap_modify_tuple` + `CatalogTupleUpdate`.
//!
//! Object types other than `OBJECT_SCHEMA` go through the same generic
//! `exec_grant_common` once their catalog read/write all-attns and side
//! tables are confirmed; everything that genuinely needs the still-unported
//! halves (`ExecGrant_Relation` column ACLs, `ExecGrant_Largeobject`,
//! `ExecGrant_Parameter`, the `pg_init_privs` writer for CREATE EXTENSION,
//! ALTER DEFAULT PRIVILEGES) panics loudly via the per-type guard below.

use mcx::{Mcx, PgString, PgVec};
use types_acl::{
    AclItem, AclMode, ACLCHECK_NO_PRIV, ACLMASK_ANY, ACL_CREATE, ACL_GRANT_OPTION_FOR,
    ACL_ID_PUBLIC, ACL_NO_RIGHTS, ACL_USAGE,
};
use types_core::primitive::Oid;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_GRANT_OPERATION,
    ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED, ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED, ERROR, WARNING,
};
use types_nodes::ddlnodes::{AccessPriv, RoleSpec as DdlRoleSpec};
use types_nodes::nodes::Node;
use types_nodes::ddlnodes::{ACL_TARGET_ALL_IN_SCHEMA, ACL_TARGET_OBJECT};
use types_nodes::parsenodes::{
    DropBehavior, ObjectType, RoleSpec as ParseRoleSpec, RoleSpecType, OBJECT_SCHEMA,
};

use backend_utils_error::ereport;

use backend_catalog_objectaddress::consts::NamespaceRelationId;
use backend_catalog_objectaddress::properties::{
    get_object_attnum_acl, get_object_attnum_name, get_object_attnum_owner, get_object_catcache_oid,
    get_object_class_descr, get_object_type,
};
use backend_utils_adt_acl::acl_ops::{aclmembers, aclupdate, ACL_MODECHG_ADD, ACL_MODECHG_DEL};
use backend_utils_adt_acl::acldefault::acldefault;
use backend_utils_adt_acl::role_membership::{get_rolespec_oid, select_best_grantor};

use backend_access_table_table::{table_close, table_open};
use backend_access_common_heaptuple::heap_modify_tuple;
use backend_catalog_indexing::keystone::CatalogTupleUpdate;
use backend_utils_cache_syscache::{SearchSysCacheLocked1, SysCacheGetAttr, SysCacheGetAttrNotNull};
use types_cache::syscache::SysCacheKey;
use types_datum::Datum as KeyDatum;
use types_storage::lock::{AccessShareLock, RowExclusiveLock};
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};

use crate::string_to_privilege;

fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/catalog/aclchk.c", lineno, funcname)
}

// `ACL_ALL_RIGHTS_SCHEMA` (`utils/acl.h`).
const ACL_ALL_RIGHTS_SCHEMA: AclMode = ACL_USAGE | ACL_CREATE;

// `aclitem` element layout: `aclitem` type OID 1033, 16-byte fixed,
// pass-by-ref, double-aligned (`pg_type.dat`).
const ACLITEMOID: Oid = 1033;
const SIZEOF_ACLITEM: usize = 16;
const VARHDRSZ: usize = 4;
const ARRAYTYPE_HDRSZ: usize = 16; // vl_len_ + ndim + dataoffset + elemtype

#[inline]
fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

/// `InternalGrant` (`utils/aclchk_internal.h`) — the internal form
/// `ExecuteGrantStmt` builds before dispatching.
struct InternalGrant<'mcx> {
    is_grant: bool,
    objtype: ObjectType,
    objects: PgVec<'mcx, Oid>,
    all_privs: bool,
    privileges: AclMode,
    grantees: PgVec<'mcx, Oid>,
    grant_option: bool,
    behavior: DropBehavior,
}

/// `merge_acl_with_grant(old_acl, is_grant, grant_option, behavior, grantees,
/// privileges, grantorId, ownerId)` (aclchk.c).
#[allow(clippy::too_many_arguments)]
fn merge_acl_with_grant<'mcx>(
    mcx: Mcx<'mcx>,
    old_acl: &[AclItem],
    is_grant: bool,
    grant_option: bool,
    behavior: DropBehavior,
    grantees: &[Oid],
    privileges: AclMode,
    grantor_id: Oid,
    owner_id: Oid,
) -> PgResult<&'mcx mut [AclItem]> {
    let modechg = if is_grant { ACL_MODECHG_ADD } else { ACL_MODECHG_DEL };

    let mut new_acl: &mut [AclItem] = {
        // Start from a private copy so aclupdate's pfree(old)-equivalent churn
        // never frees the caller's old_acl.
        let buf = mcx::vec_with_capacity_in::<AclItem>(mcx, old_acl.len())?;
        let mut buf = buf;
        for it in old_acl {
            buf.push(*it);
        }
        buf.leak()
    };

    for &grantee in grantees {
        // Grant options can only be granted to individual roles, not PUBLIC.
        if is_grant && grant_option && grantee == ACL_ID_PUBLIC {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                .errmsg("grant options can only be granted to roles".to_string())
                .into_error());
        }

        // ACLITEM_SET_PRIVS_GOPTIONS(aclitem, privs, goptions):
        //   privs    = (is_grant || !grant_option) ? privileges : ACL_NO_RIGHTS
        //   goptions = (!is_grant || grant_option) ? privileges : ACL_NO_RIGHTS
        let privs = if is_grant || !grant_option { privileges } else { ACL_NO_RIGHTS };
        let goptions = if !is_grant || grant_option { privileges } else { ACL_NO_RIGHTS };
        let aclitem = AclItem {
            ai_grantee: grantee,
            ai_grantor: grantor_id,
            ai_privs: (privs & 0xFFFF_FFFF) | ((goptions & 0xFFFF_FFFF) << 32),
        };

        new_acl = aclupdate(mcx, new_acl, &aclitem, modechg, owner_id, behavior as i32)?;
    }

    Ok(new_acl)
}

/// `restrict_and_check_grant(is_grant, avail_goptions, all_privs, privileges,
/// objectId, grantorId, objtype, objname, att_number, colname)` (aclchk.c),
/// for the non-column object types reached by the schema slice.
#[allow(clippy::too_many_arguments)]
fn restrict_and_check_grant(
    mcx: Mcx<'_>,
    is_grant: bool,
    avail_goptions: AclMode,
    all_privs: bool,
    privileges: AclMode,
    object_id: Oid,
    grantor_id: Oid,
    objtype: ObjectType,
    objname: &str,
) -> PgResult<AclMode> {
    let whole_mask = match objtype {
        ObjectType::Schema => ACL_ALL_RIGHTS_SCHEMA,
        other => {
            return Err(PgError::error(format!(
                "restrict_and_check_grant: unsupported object type {other:?} in schema-grant slice"
            )));
        }
    };

    // If we found no grant options, consider whether to issue a hard error.
    // Per spec, having any privilege at all on the object will get you by here.
    if avail_goptions == ACL_NO_RIGHTS
        && crate::pg_aclmask(
            mcx,
            objtype,
            object_id,
            0,
            grantor_id,
            whole_mask | ACL_GRANT_OPTION_FOR(whole_mask),
            ACLMASK_ANY,
        )? == ACL_NO_RIGHTS
    {
        crate::aclcheck_error(ACLCHECK_NO_PRIV, objtype, Some(objname.to_string()))?;
    }

    // Restrict the operation to what we can actually grant or revoke, and
    // issue a warning if appropriate.
    // this_privileges = privileges & ACL_OPTION_TO_PRIVS(avail_goptions)
    //   ACL_OPTION_TO_PRIVS(x) = (x >> 32)
    let this_privileges = privileges & (avail_goptions >> 32);
    if is_grant {
        if this_privileges == 0 {
            ereport(WARNING)
                .errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED)
                .errmsg(format!("no privileges were granted for \"{objname}\""))
                .finish(errloc(338, "restrict_and_check_grant"))?;
        } else if !all_privs && this_privileges != privileges {
            ereport(WARNING)
                .errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED)
                .errmsg(format!("not all privileges were granted for \"{objname}\""))
                .finish(errloc(351, "restrict_and_check_grant"))?;
        }
    } else if this_privileges == 0 {
        ereport(WARNING)
            .errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED)
            .errmsg(format!("no privileges could be revoked for \"{objname}\""))
            .finish(errloc(367, "restrict_and_check_grant"))?;
    } else if !all_privs && this_privileges != privileges {
        ereport(WARNING)
            .errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED)
            .errmsg(format!("not all privileges could be revoked for \"{objname}\""))
            .finish(errloc(380, "restrict_and_check_grant"))?;
    }

    Ok(this_privileges)
}

/// Encode an `&[AclItem]` slice as an on-disk `aclitem[]` `ArrayType` varlena
/// (`PointerGetDatum(Acl)`). This is the inverse of the syscache
/// `decode_acl`: a 1-D, no-null array of 16-byte `aclitem`s, the same
/// `ARR_SETUP` C's `allocacl` builds.
fn acl_to_datum<'mcx>(mcx: Mcx<'mcx>, acl: &[AclItem]) -> PgResult<Datum<'mcx>> {
    let n = acl.len();
    let data_off = maxalign(ARRAYTYPE_HDRSZ + 2 * 4); // ndim=1: dims[1] + lbound[1]
    let total = data_off + n * SIZEOF_ACLITEM;

    let mut bytes: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    bytes.resize(total, 0);

    // vl_len_ : SET_VARSIZE_4B(total) = (total << 2) (4-byte, uncompressed).
    let vl = (total as u32) << 2;
    bytes[0..4].copy_from_slice(&vl.to_ne_bytes());
    // ndim = 1
    bytes[4..8].copy_from_slice(&1i32.to_ne_bytes());
    // dataoffset = 0 (no null bitmap)
    bytes[8..12].copy_from_slice(&0i32.to_ne_bytes());
    // elemtype = ACLITEMOID
    bytes[12..16].copy_from_slice(&ACLITEMOID.to_ne_bytes());
    // dims[0] = n
    bytes[16..20].copy_from_slice(&(n as i32).to_ne_bytes());
    // lbound[0] = 1
    bytes[20..24].copy_from_slice(&1i32.to_ne_bytes());

    for (i, it) in acl.iter().enumerate() {
        let off = data_off + i * SIZEOF_ACLITEM;
        bytes[off..off + 4].copy_from_slice(&it.ai_grantee.to_ne_bytes());
        bytes[off + 4..off + 8].copy_from_slice(&it.ai_grantor.to_ne_bytes());
        bytes[off + 8..off + 16].copy_from_slice(&it.ai_privs.to_ne_bytes());
    }

    debug_assert_eq!(VARHDRSZ, 4);
    Ok(Datum::ByRef(bytes))
}

/// `recordExtensionInitPriv(objoid, classoid, objsubid, new_acl)` (aclchk.c) —
/// the early-out half. Outside CREATE EXTENSION / binary upgrade this is a
/// no-op; the `recordExtensionInitPrivWorker` body (the pg_init_privs writer)
/// is the still-unported F3 keystone and panics if ever reached here.
fn record_extension_init_priv(_objoid: Oid, _classoid: Oid, _new_acl: &[AclItem]) -> PgResult<()> {
    // if (!creating_extension && !binary_upgrade_record_init_privs) return;
    let creating_extension = backend_commands_extension_seams::creating_extension::call();
    if !creating_extension {
        return Ok(());
    }
    Err(PgError::error(
        "recordExtensionInitPrivWorker (pg_init_privs writer) not ported — \
         GRANT during CREATE EXTENSION is the F3 keystone",
    ))
}

/// `ExecGrant_common(istmt, classid, default_privs, object_check)` (aclchk.c),
/// specialized to the cases with no `object_check` callback and no per-type
/// catalog quirks (schema is the live one). Reads the catalog tuple under the
/// inplace-update tuple lock, rebuilds the ACL, and writes it back.
fn exec_grant_common(
    mcx: Mcx<'_>,
    istmt: &mut InternalGrant<'_>,
    classid: Oid,
    default_privs: AclMode,
) -> PgResult<()> {
    if istmt.all_privs && istmt.privileges == ACL_NO_RIGHTS {
        istmt.privileges = default_privs;
    }

    let cacheid = get_object_catcache_oid(classid)?;
    let acl_attnum = get_object_attnum_acl(classid)? as i32;
    let owner_attnum = get_object_attnum_owner(classid)? as i32;
    let name_attnum = get_object_attnum_name(classid)? as i32;

    let relation = table_open(mcx, classid, RowExclusiveLock)?;
    let my_db = backend_utils_init_small_seams::my_database_id::call();

    for &objectid in istmt.objects.iter() {
        let locked = SearchSysCacheLocked1(
            mcx,
            my_db,
            cacheid,
            SysCacheKey::Value(KeyDatum::from_oid(objectid)),
        )?;
        let Some((guard, tuple)) = locked else {
            return Err(PgError::error(format!(
                "cache lookup failed for {} {objectid}",
                get_object_class_descr(classid)?
            )));
        };

        // ownerId = DatumGetObjectId(SysCacheGetAttrNotNull(owner)).
        let owner_id = SysCacheGetAttrNotNull(mcx, cacheid, &tuple, owner_attnum)?.as_oid();

        // aclDatum = SysCacheGetAttr(acl); if null -> acldefault(...).
        let (old_acl, noldmembers_vec): (&[AclItem], Option<PgVec<Oid>>);
        let (acl_datum, acl_is_null) = SysCacheGetAttr(mcx, cacheid, &tuple, acl_attnum)?;
        let old_acl_owned: &[AclItem];
        let mut old_members: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
        if acl_is_null {
            old_acl_owned = acldefault(mcx, get_object_type(classid, objectid)?, owner_id)?;
        } else {
            let raw = match &acl_datum {
                Datum::ByRef(b) => &b[..],
                _ => {
                    return Err(PgError::error(
                        "ExecGrant_common: ACL column is not a varlena",
                    ))
                }
            };
            old_acl_owned = decode_acl(mcx, raw)?;
            for m in aclmembers(mcx, old_acl_owned)?.iter() {
                old_members.push(*m);
            }
        }
        old_acl = old_acl_owned;
        noldmembers_vec = if acl_is_null { None } else { Some(old_members) };

        // select_best_grantor(GetUserId(), istmt->privileges, old_acl, ownerId,
        //                      &grantorId, &avail_goptions).
        let user_id = backend_utils_init_miscinit_seams::get_user_id::call();
        let (grantor_id, avail_goptions) =
            select_best_grantor(user_id, istmt.privileges, old_acl, owner_id)?;

        // nameDatum = SysCacheGetAttrNotNull(name).
        let name = read_name(mcx, cacheid, &tuple, name_attnum)?;

        let this_privileges = restrict_and_check_grant(
            mcx,
            istmt.is_grant,
            avail_goptions,
            istmt.all_privs,
            istmt.privileges,
            objectid,
            grantor_id,
            get_object_type(classid, objectid)?,
            &name,
        )?;

        // new_acl = merge_acl_with_grant(...).
        let new_acl = merge_acl_with_grant(
            mcx,
            old_acl,
            istmt.is_grant,
            istmt.grant_option,
            istmt.behavior,
            &istmt.grantees,
            this_privileges,
            grantor_id,
            owner_id,
        )?;

        let mut new_members: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
        for m in aclmembers(mcx, new_acl)?.iter() {
            new_members.push(*m);
        }

        // replaces[acl-1]=true; values[acl-1]=PointerGetDatum(new_acl).
        let natts = relation.rd_att.natts as usize;
        let mut values: PgVec<Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut nulls: PgVec<bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replaces: PgVec<bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        for _ in 0..natts {
            values.push(Datum::ByVal(0));
            nulls.push(false);
            replaces.push(false);
        }
        let aidx = (acl_attnum - 1) as usize;
        replaces[aidx] = true;
        values[aidx] = acl_to_datum(mcx, new_acl)?;

        let mut newtuple =
            heap_modify_tuple(mcx, &tuple, &relation.rd_att, &values, &nulls, &replaces)
                .map_err(|e| PgError::error(format!("heap_modify_tuple failed: {e:?}")))?;

        let otid = tuple.tuple.t_self;
        CatalogTupleUpdate(mcx, &relation, otid, &mut newtuple)?;

        // UnlockTuple(relation, &tuple->t_self, InplaceUpdateTupleLock).
        guard.release()?;

        // Update initial privileges for extensions (no-op outside CREATE
        // EXTENSION / binary upgrade).
        record_extension_init_priv(objectid, classid, new_acl)?;

        // Update the shared dependency ACL info.
        let old_for_dep = match noldmembers_vec {
            Some(v) => v,
            None => mcx::vec_with_capacity_in(mcx, 0)?,
        };
        backend_catalog_pg_shdepend_seams::updateAclDependencies::call(
            mcx, classid, objectid, 0, owner_id, old_for_dep, new_members,
        )?;

        // prevent error when processing duplicate objects.
        backend_access_transam_xact_seams::command_counter_increment::call()?;
    }

    table_close(relation, RowExclusiveLock)?;
    Ok(())
}

/// Inverse of the syscache `decode_acl`: read a 1-D `aclitem[]` array varlena
/// into an `&[AclItem]`. The header layout matches [`acl_to_datum`].
fn decode_acl<'mcx>(mcx: Mcx<'mcx>, on_disk: &[u8]) -> PgResult<&'mcx mut [AclItem]> {
    // DatumGetAclPCopy: detoast first (an inline short varlena round-trips
    // unchanged, but a compressed/external ACL must be expanded).
    let raw = backend_access_common_detoast_seams::detoast_attr::call(mcx, on_disk)?;
    let raw = &raw[..];
    if raw.len() < ARRAYTYPE_HDRSZ {
        return Err(PgError::error("ExecGrant_common: truncated ACL varlena"));
    }
    let ndim = i32::from_ne_bytes([raw[4], raw[5], raw[6], raw[7]]);
    let dataoffset = i32::from_ne_bytes([raw[8], raw[9], raw[10], raw[11]]);
    let n = if ndim >= 1 {
        i32::from_ne_bytes([raw[16], raw[17], raw[18], raw[19]]).max(0) as usize
    } else {
        0
    };
    let data_off = if dataoffset != 0 {
        dataoffset as usize
    } else {
        maxalign(ARRAYTYPE_HDRSZ + 2 * 4 * ndim.max(1) as usize)
    };
    let mut items: PgVec<AclItem> = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        let off = data_off + i * SIZEOF_ACLITEM;
        let b = raw
            .get(off..off + SIZEOF_ACLITEM)
            .ok_or_else(|| PgError::error("ExecGrant_common: truncated aclitem data"))?;
        items.push(AclItem {
            ai_grantee: u32::from_ne_bytes([b[0], b[1], b[2], b[3]]),
            ai_grantor: u32::from_ne_bytes([b[4], b[5], b[6], b[7]]),
            ai_privs: u64::from_ne_bytes([
                b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
            ]),
        });
    }
    Ok(items.leak())
}

/// `NameStr(*DatumGetName(SysCacheGetAttrNotNull(name)))`.
fn read_name(mcx: Mcx<'_>, cacheid: i32, tuple: &FormedTuple<'_>, attnum: i32) -> PgResult<String> {
    let value = SysCacheGetAttrNotNull(mcx, cacheid, tuple, attnum)?;
    let bytes = match &value {
        Datum::ByRef(b) => &b[..],
        _ => return Err(PgError::error("ExecGrant_common: name attribute is by-value")),
    };
    let len = bytes.iter().position(|&c| c == 0).unwrap_or(bytes.len());
    Ok(String::from_utf8_lossy(&bytes[..len]).into_owned())
}

/// `ExecGrantStmt_oids(istmt)` (aclchk.c) — dispatch on object type.
fn exec_grant_stmt_oids(mcx: Mcx<'_>, istmt: &mut InternalGrant<'_>) -> PgResult<()> {
    match istmt.objtype {
        OBJECT_SCHEMA => exec_grant_common(mcx, istmt, NamespaceRelationId, ACL_ALL_RIGHTS_SCHEMA),
        other => Err(PgError::error(format!(
            "GRANT/REVOKE executor not ported for object type {other:?} \
             (schema-grant slice only; full aclchk F2/F3 keystone)"
        ))),
    }
}

/// Bridge the `ddlnodes::RoleSpec` carried by the parse tree to the
/// `parsenodes::RoleSpec` `get_rolespec_oid` consumes.
fn rolespec_oid(role: &DdlRoleSpec<'_>, mcx: Mcx<'_>) -> PgResult<Oid> {
    let rolename = match &role.rolename {
        Some(s) => Some(PgString::from_str_in(s.as_str(), mcx)?),
        None => None,
    };
    let parse_role = ParseRoleSpec { roletype: role.roletype, rolename };
    get_rolespec_oid(&parse_role, false)
}

/// `ExecuteGrantStmt(stmt)` (aclchk.c). The slow-path leg installed for
/// `Node::GrantStmt`.
pub fn execute_grant_stmt(mcx: Mcx<'_>, stmt: &Node<'_>) -> PgResult<()> {
    let Node::GrantStmt(stmt) = stmt else {
        return Err(PgError::error("execute_grant_stmt: not a GrantStmt"));
    };

    // grantor clause: only for SQL compatibility; must be current user.
    if let Some(grantor) = &stmt.grantor {
        let Node::RoleSpec(rs) = &**grantor else {
            return Err(PgError::error("ExecuteGrantStmt: grantor is not a RoleSpec"));
        };
        let grantor_oid = rolespec_oid(rs, mcx)?;
        if grantor_oid != backend_utils_init_miscinit_seams::get_user_id::call() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("grantor must be current user".to_string())
                .into_error());
        }
    }

    // Collect target object OIDs.
    let objects: PgVec<Oid> = match stmt.targtype {
        ACL_TARGET_OBJECT => object_names_to_oids(mcx, stmt.objtype, &stmt.objects, stmt.is_grant)?,
        ACL_TARGET_ALL_IN_SCHEMA => {
            return Err(PgError::error(
                "GRANT ... ALL IN SCHEMA not ported (schema-grant slice)",
            ))
        }
        other => {
            return Err(PgError::error(format!(
                "unrecognized GrantStmt.targtype: {other:?}"
            )))
        }
    };

    // Convert the grantee RoleSpec list into an Oid list (PUBLIC -> ACL_ID_PUBLIC).
    let mut grantees: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, stmt.grantees.len())?;
    for g in stmt.grantees.iter() {
        let Node::RoleSpec(rs) = &**g else {
            return Err(PgError::error("ExecuteGrantStmt: grantee is not a RoleSpec"));
        };
        let uid = match rs.roletype {
            RoleSpecType::Public => ACL_ID_PUBLIC,
            _ => rolespec_oid(rs, mcx)?,
        };
        grantees.push(uid);
    }

    // Convert the privilege list into an AclMode bitmask.
    let (all_privileges, errormsg) = objtype_all_privileges(stmt.objtype)?;
    let (all_privs, privileges) = if stmt.privileges.is_empty() {
        (true, ACL_NO_RIGHTS)
    } else {
        let mut acc = ACL_NO_RIGHTS;
        for p in stmt.privileges.iter() {
            let Node::AccessPriv(privnode) = &**p else {
                return Err(PgError::error("ExecuteGrantStmt: privilege is not an AccessPriv"));
            };
            let AccessPriv { priv_name, cols } = privnode;
            if !cols.is_empty() {
                // Column privileges are only valid for relations.
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                    .errmsg("column privileges are only valid for relations".to_string())
                    .into_error());
            }
            let Some(name) = priv_name else {
                return Err(PgError::error(
                    "AccessPriv node must specify privilege or columns",
                ));
            };
            let priv_bit = string_to_privilege(name.as_str())?;
            if priv_bit & !all_privileges != 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                    .errmsg(errormsg.replace("%s", crate::privilege_to_string(priv_bit)?))
                    .into_error());
            }
            acc |= priv_bit;
        }
        (false, acc)
    };

    let mut istmt = InternalGrant {
        is_grant: stmt.is_grant,
        objtype: stmt.objtype,
        objects,
        all_privs,
        privileges,
        grantees,
        grant_option: stmt.grant_option,
        behavior: stmt.behavior,
    };

    exec_grant_stmt_oids(mcx, &mut istmt)
}

/// `objectNamesToOids(objtype, objnames, is_grant)` (aclchk.c) — the generic
/// `get_object_address` leg (used for OBJECT_SCHEMA and the other
/// `get_object_address`-addressable types).
fn object_names_to_oids<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    objnames: &PgVec<'_, types_nodes::nodes::NodePtr<'_>>,
    _is_grant: bool,
) -> PgResult<PgVec<'mcx, Oid>> {
    // const LOCKMODE lockmode = AccessShareLock;  (taken inside
    // get_namespace_oid's catalog read for the schema leg of the generic
    // get_object_address path; bottoms out in get_namespace_oid here).
    let _lockmode = AccessShareLock;
    match objtype {
        OBJECT_SCHEMA => {
            let mut objects: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, objnames.len())?;
            for name in objnames.iter() {
                // get_object_address(OBJECT_SCHEMA, String(name), ...) ->
                // get_object_address_unqualified -> get_namespace_oid(name, false).
                let Node::String(s) = &**name else {
                    return Err(PgError::error(
                        "objectNamesToOids(OBJECT_SCHEMA): object name is not a String node",
                    ));
                };
                let oid = backend_catalog_namespace_seams::get_namespace_oid::call(
                    s.sval.as_str(),
                    false,
                )?;
                objects.push(oid);
            }
            Ok(objects)
        }
        other => Err(PgError::error(format!(
            "objectNamesToOids not ported for object type {other:?} (schema-grant slice)"
        ))),
    }
}

/// The objtype -> (all_privileges mask, errormsg) table from `ExecuteGrantStmt`,
/// restricted to schema in this slice (other types raise on dispatch anyway).
fn objtype_all_privileges(objtype: ObjectType) -> PgResult<(AclMode, &'static str)> {
    match objtype {
        OBJECT_SCHEMA => Ok((ACL_ALL_RIGHTS_SCHEMA, "invalid privilege type %s for schema")),
        other => Err(PgError::error(format!(
            "GRANT objtype {other:?} not ported (schema-grant slice)"
        ))),
    }
}

