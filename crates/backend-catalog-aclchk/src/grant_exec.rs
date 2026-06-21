//! `catalog/aclchk.c` — the GRANT/REVOKE executor (F2). Covers the
//! `OBJECT_SCHEMA` path (`GRANT ALL ON SCHEMA public TO public`) and the
//! relation path (`OBJECT_TABLE`/`OBJECT_SEQUENCE`, including per-column
//! privileges).
//!
//! This ports `ExecuteGrantStmt` + `ExecGrantStmt_oids` + `ExecGrant_common`
//! + `ExecGrant_Relation` + `ExecGrant_Attribute` + `expand_col_privileges`
//! + `expand_all_col_privileges` + `merge_acl_with_grant`
//! + `restrict_and_check_grant` + the schema/relation legs of
//! `objectNamesToOids` + the early-out half of `recordExtensionInitPriv`. The
//! ACL bit work is the merged `backend-utils-adt-acl` (`acldefault`,
//! `aclmembers`, `aclupdate`, `aclconcat`, `select_best_grantor`) over the
//! `&[AclItem]` slice model; the catalog read/write is `SearchSysCacheLocked1`
//! / `SearchSysCache2` + `SysCacheGetAttr` + `heap_modify_tuple` +
//! `CatalogTupleUpdate`.
//!
//! Object types other than `OBJECT_SCHEMA`/`OBJECT_TABLE`/`OBJECT_SEQUENCE`
//! still raise on dispatch (`ExecGrant_Largeobject`, `ExecGrant_Parameter`,
//! `ExecGrant_common` for the remaining catalog kinds, ALTER DEFAULT
//! PRIVILEGES); the `pg_init_privs` writer for CREATE EXTENSION
//! (`recordExtensionInitPrivWorker`) is the remaining F3 keystone.

use mcx::{Mcx, PgString, PgVec};
use types_acl::{
    AclItem, AclMode, ACLCHECK_NO_PRIV, ACLMASK_ANY, ACL_CREATE, ACL_DELETE, ACL_GRANT_OPTION_FOR,
    ACL_ID_PUBLIC, ACL_INSERT, ACL_MAINTAIN, ACL_NO_RIGHTS, ACL_REFERENCES, ACL_SELECT,
    ACL_TRIGGER, ACL_TRUNCATE, ACL_UPDATE, ACL_USAGE,
};
use types_catalog::pg_attribute::{
    Anum_pg_attribute_attacl, Anum_pg_attribute_attisdropped, Anum_pg_attribute_attname,
    AttributeRelationId,
};
use types_catalog::pg_class::{
    Anum_pg_class_oid, Anum_pg_class_relacl, Anum_pg_class_relkind, Anum_pg_class_relname,
    Anum_pg_class_relnamespace, Anum_pg_class_relnatts, Anum_pg_class_relowner, RelationRelationId,
};
use types_catalog::pg_type::{
    Anum_pg_type_typelem, Anum_pg_type_typsubscript, Anum_pg_type_typtype, TypeRelationId,
};
use types_catalog::pg_proc::{
    Anum_pg_proc_oid, Anum_pg_proc_prokind, Anum_pg_proc_pronamespace, ProcedureRelationId,
    PROKIND_PROCEDURE,
};
use types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber;
use types_core::primitive::{InvalidOid, Oid};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_GRANT_OPERATION,
    ERRCODE_WRONG_OBJECT_TYPE, ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED,
    ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED, ERROR, WARNING,
};
use types_nodes::ddlnodes::{AccessPriv, RoleSpec as DdlRoleSpec};
use types_nodes::nodes::Node;
use types_nodes::ddlnodes::{ACL_TARGET_ALL_IN_SCHEMA, ACL_TARGET_OBJECT};
use types_nodes::parsenodes::{
    DropBehavior, ObjectType, RoleSpec as ParseRoleSpec, RoleSpecType, OBJECT_SCHEMA,
};
use types_tuple::access::{
    RangeVar as AccessRangeVar, RELKIND_COMPOSITE_TYPE, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX,
    RELKIND_SEQUENCE, RELKIND_VIEW,
};

use backend_utils_error::ereport;

use backend_catalog_objectaddress::consts::{
    NamespaceRelationId, DefaultAclOidIndexId, Anum_pg_default_acl_defaclrole,
    Anum_pg_default_acl_defaclnamespace, Anum_pg_default_acl_defaclobjtype,
    Anum_pg_default_acl_defaclacl, DEFACLOBJ_RELATION, DEFACLOBJ_SEQUENCE, DEFACLOBJ_FUNCTION,
    DEFACLOBJ_TYPE, DEFACLOBJ_NAMESPACE, DEFACLOBJ_LARGEOBJECT,
};
use backend_catalog_objectaddress::consts::Anum_pg_default_acl_oid;
use backend_catalog_objectaddress::properties::{
    get_object_attnum_acl, get_object_attnum_name, get_object_attnum_owner, get_object_catcache_oid,
    get_object_class_descr, get_object_type,
};
use backend_utils_adt_acl::acl_ops::{
    aclcopy, aclequal, aclitemsort, aclmembers, aclupdate, make_empty_acl, ACL_MODECHG_ADD,
    ACL_MODECHG_DEL,
};
use backend_utils_adt_acl::acldefault::acldefault;
use backend_utils_adt_acl::role_membership::{get_rolespec_oid, select_best_grantor};

use backend_access_table_table::{table_close, table_open};
use backend_access_common_heaptuple::{heap_form_tuple, heap_modify_tuple};
use backend_catalog_indexing::keystone::{CatalogTupleInsert, CatalogTupleUpdate};
use backend_utils_cache_syscache::{
    SearchSysCache3, SearchSysCacheLocked1, SysCacheGetAttr, SysCacheGetAttrNotNull,
};
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

// `ACL_ALL_RIGHTS_RELATION` / `ACL_ALL_RIGHTS_SEQUENCE` / `ACL_ALL_RIGHTS_COLUMN`
// (`utils/acl.h`).
const ACL_ALL_RIGHTS_RELATION: AclMode = ACL_INSERT
    | ACL_SELECT
    | ACL_UPDATE
    | ACL_DELETE
    | ACL_TRUNCATE
    | ACL_REFERENCES
    | ACL_TRIGGER
    | ACL_MAINTAIN;
const ACL_ALL_RIGHTS_SEQUENCE: AclMode = ACL_USAGE | ACL_SELECT | ACL_UPDATE;
const ACL_ALL_RIGHTS_COLUMN: AclMode = ACL_INSERT | ACL_SELECT | ACL_UPDATE | ACL_REFERENCES;

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
    /// `List *col_privs` — `AccessPriv`s carrying a column list (relations
    /// only). Empty unless column privileges were specified.
    col_privs: PgVec<'mcx, AccessPriv<'mcx>>,
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
    att_number: types_core::AttrNumber,
    colname: Option<&str>,
) -> PgResult<AclMode> {
    let whole_mask = match objtype {
        ObjectType::Column => ACL_ALL_RIGHTS_COLUMN,
        ObjectType::Table => ACL_ALL_RIGHTS_RELATION,
        ObjectType::Sequence => ACL_ALL_RIGHTS_SEQUENCE,
        ObjectType::Schema => ACL_ALL_RIGHTS_SCHEMA,
        ObjectType::Database => ACL_ALL_RIGHTS_DATABASE,
        ObjectType::Type | ObjectType::Domain => ACL_ALL_RIGHTS_TYPE,
        ObjectType::Function | ObjectType::Procedure | ObjectType::Routine => {
            ACL_ALL_RIGHTS_FUNCTION
        }
        ObjectType::Largeobject => ACL_ALL_RIGHTS_LARGEOBJECT,
        ObjectType::Language => ACL_ALL_RIGHTS_LANGUAGE,
        other => {
            return Err(PgError::error(format!(
                "restrict_and_check_grant: unsupported object type {other:?} in grant slice"
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
            att_number,
            grantor_id,
            whole_mask | ACL_GRANT_OPTION_FOR(whole_mask),
            ACLMASK_ANY,
        )? == ACL_NO_RIGHTS
    {
        if objtype == ObjectType::Column {
            if let Some(col) = colname {
                crate::aclcheck_error_col(
                    ACLCHECK_NO_PRIV,
                    objtype,
                    Some(objname.to_string()),
                    col.to_string(),
                )?;
            } else {
                crate::aclcheck_error(ACLCHECK_NO_PRIV, objtype, Some(objname.to_string()))?;
            }
        } else {
            crate::aclcheck_error(ACLCHECK_NO_PRIV, objtype, Some(objname.to_string()))?;
        }
    }

    // Restrict the operation to what we can actually grant or revoke, and
    // issue a warning if appropriate.
    // this_privileges = privileges & ACL_OPTION_TO_PRIVS(avail_goptions)
    //   ACL_OPTION_TO_PRIVS(x) = (x >> 32)
    let this_privileges = privileges & (avail_goptions >> 32);
    let is_col = objtype == ObjectType::Column && colname.is_some();
    let col = colname.unwrap_or("");
    if is_grant {
        if this_privileges == 0 {
            let msg = if is_col {
                format!("no privileges were granted for column \"{col}\" of relation \"{objname}\"")
            } else {
                format!("no privileges were granted for \"{objname}\"")
            };
            ereport(WARNING)
                .errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED)
                .errmsg(msg)
                .finish(errloc(338, "restrict_and_check_grant"))?;
        } else if !all_privs && this_privileges != privileges {
            let msg = if is_col {
                format!(
                    "not all privileges were granted for column \"{col}\" of relation \"{objname}\""
                )
            } else {
                format!("not all privileges were granted for \"{objname}\"")
            };
            ereport(WARNING)
                .errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED)
                .errmsg(msg)
                .finish(errloc(351, "restrict_and_check_grant"))?;
        }
    } else if this_privileges == 0 {
        let msg = if is_col {
            format!("no privileges could be revoked for column \"{col}\" of relation \"{objname}\"")
        } else {
            format!("no privileges could be revoked for \"{objname}\"")
        };
        ereport(WARNING)
            .errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED)
            .errmsg(msg)
            .finish(errloc(367, "restrict_and_check_grant"))?;
    } else if !all_privs && this_privileges != privileges {
        let msg = if is_col {
            format!(
                "not all privileges could be revoked for column \"{col}\" of relation \"{objname}\""
            )
        } else {
            format!("not all privileges could be revoked for \"{objname}\"")
        };
        ereport(WARNING)
            .errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED)
            .errmsg(msg)
            .finish(errloc(380, "restrict_and_check_grant"))?;
    }

    Ok(this_privileges)
}

/// Encode an `&[AclItem]` slice as an on-disk `aclitem[]` `ArrayType` varlena
/// (`PointerGetDatum(Acl)`). This is the inverse of the syscache
/// `decode_acl`: a 1-D, no-null array of 16-byte `aclitem`s, the same
/// `ARR_SETUP` C's `allocacl` builds.
pub(crate) fn acl_to_datum<'mcx>(mcx: Mcx<'mcx>, acl: &[AclItem]) -> PgResult<Datum<'mcx>> {
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

/// Owner-rewrite of an on-disk `aclitem[]` varlena: the combination
/// `aclnewowner(DatumGetAclP(aclDatum), oldOwner, newOwner)` (acl.c) followed by
/// `PointerGetDatum(newAcl)`, as the catalog owner-change paths
/// (`ATExecChangeOwner` & friends) perform when the relacl/objacl column is
/// non-null. Decodes the array into its `AclItem`s, substitutes the new owner
/// OID wherever the old appears as grantor/grantee, and re-encodes the result
/// as a fresh `aclitem[]` varlena `Datum`. Kept here because both
/// [`decode_acl`] and [`acl_to_datum`] (the on-disk codec) live in this unit and
/// `aclnewowner` is reached through this unit's `backend-utils-adt-acl`
/// dependency; the bare `&[AclItem]` model never crosses a crate boundary.
pub fn acl_change_owner_datum<'mcx>(
    mcx: Mcx<'mcx>,
    acl_on_disk: &[u8],
    old_owner_id: Oid,
    new_owner_id: Oid,
) -> PgResult<Datum<'mcx>> {
    let old_acl = decode_acl(mcx, acl_on_disk)?;
    let new_acl = backend_utils_adt_acl::acl_ops::aclnewowner(mcx, old_acl, old_owner_id, new_owner_id)?;
    acl_to_datum(mcx, new_acl)
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
    object_check: Option<fn(Mcx<'_>, i32, &FormedTuple<'_>) -> PgResult<()>>,
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

        // Call the type-specific check function, if any.
        if let Some(check) = object_check {
            check(mcx, cacheid, &tuple)?;
        }

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
            0,
            None,
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

/// `ExecGrant_Type_check(istmt, tuple)` (aclchk.c) — the per-object check
/// `ExecGrant_common` runs for `OBJECT_TYPE`/`OBJECT_DOMAIN`: GRANT/REVOKE is
/// rejected on a "true" array type (set privileges of the element type instead)
/// and on a multirange type (set privileges of the range type instead).
fn exec_grant_type_check(mcx: Mcx<'_>, cacheid: i32, tuple: &FormedTuple<'_>) -> PgResult<()> {
    // F_ARRAY_SUBSCRIPT_HANDLER (catalog/pg_proc_d.h) — OID of the standard
    // array subscripting handler; an array type's typsubscript points here.
    const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;
    // TYPTYPE_MULTIRANGE (catalog/pg_type.h).
    const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;

    let typelem =
        SysCacheGetAttrNotNull(mcx, cacheid, tuple, Anum_pg_type_typelem as i32)?.as_oid();
    let typsubscript =
        SysCacheGetAttrNotNull(mcx, cacheid, tuple, Anum_pg_type_typsubscript as i32)?.as_oid();
    let typtype =
        SysCacheGetAttrNotNull(mcx, cacheid, tuple, Anum_pg_type_typtype as i32)?.as_i8();

    // IsTrueArrayType(typeForm): OidIsValid(typelem) && typsubscript == F_ARRAY_SUBSCRIPT_HANDLER.
    if typelem != InvalidOid && typsubscript == F_ARRAY_SUBSCRIPT_HANDLER {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_GRANT_OPERATION)
            .errmsg("cannot set privileges of array types".to_string())
            .errhint("Set the privileges of the element type instead.".to_string())
            .into_error());
    }
    if typtype == TYPTYPE_MULTIRANGE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_GRANT_OPERATION)
            .errmsg("cannot set privileges of multirange types".to_string())
            .errhint("Set the privileges of the range type instead.".to_string())
            .into_error());
    }
    Ok(())
}

/// `ExecGrant_Language_check(istmt, tuple)` (aclchk.c:2252) — the per-object
/// check `ExecGrant_common` runs for `OBJECT_LANGUAGE`: GRANT/REVOKE is rejected
/// on an untrusted language, since only superusers may use untrusted languages.
fn exec_grant_language_check(mcx: Mcx<'_>, cacheid: i32, tuple: &FormedTuple<'_>) -> PgResult<()> {
    use types_catalog::pg_language::{Anum_pg_language_lanname, Anum_pg_language_lanpltrusted};

    let lanpltrusted =
        SysCacheGetAttrNotNull(mcx, cacheid, tuple, Anum_pg_language_lanpltrusted as i32)?.as_bool();
    if !lanpltrusted {
        let lanname = read_name(mcx, cacheid, tuple, Anum_pg_language_lanname as i32)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("language \"{lanname}\" is not trusted"))
            .errdetail(
                "GRANT and REVOKE are not allowed on untrusted languages, \
                 because only superusers can use untrusted languages."
                    .to_string(),
            )
            .into_error());
    }
    Ok(())
}

/// `ExecGrant_Relation(istmt)` (aclchk.c) — the GRANT/REVOKE executor leg for
/// `OBJECT_TABLE`/`OBJECT_SEQUENCE`. Reads each relation's `pg_class` tuple
/// under the inplace-update tuple lock, validates relkind, computes the
/// supported privilege mask, merges the new ACL into `pg_class.relacl`, writes
/// it back, and updates `pg_init_privs`/`pg_shdepend`. Column privileges
/// (`ExecGrant_Attribute`) are not yet ported and raise loudly if reached.
fn exec_grant_relation(mcx: Mcx<'_>, istmt: &mut InternalGrant<'_>) -> PgResult<()> {
    const RELOID: i32 = 57;
    let acl_attnum = Anum_pg_class_relacl as i32;
    let owner_attnum = Anum_pg_class_relowner as i32;
    let name_attnum = Anum_pg_class_relname as i32;
    let kind_attnum = Anum_pg_class_relkind as i32;
    let natts_attnum = Anum_pg_class_relnatts as i32;

    let relation = table_open(mcx, RelationRelationId, RowExclusiveLock)?;
    let att_relation = table_open(mcx, AttributeRelationId, RowExclusiveLock)?;
    let my_db = backend_utils_init_small_seams::my_database_id::call();

    for &rel_oid in istmt.objects.iter() {
        let locked = SearchSysCacheLocked1(
            mcx,
            my_db,
            RELOID,
            SysCacheKey::Value(KeyDatum::from_oid(rel_oid)),
        )?;
        let Some((guard, tuple)) = locked else {
            return Err(PgError::error(format!(
                "cache lookup failed for relation {rel_oid}"
            )));
        };

        let relkind = SysCacheGetAttrNotNull(mcx, RELOID, &tuple, kind_attnum)?.as_char() as u8;
        let relnatts = SysCacheGetAttrNotNull(mcx, RELOID, &tuple, natts_attnum)?.as_i16();
        let owner_id = SysCacheGetAttrNotNull(mcx, RELOID, &tuple, owner_attnum)?.as_oid();
        let relname = read_name(mcx, RELOID, &tuple, name_attnum)?;

        // Not sensible to grant on an index.
        if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("\"{relname}\" is an index"))
                .into_error());
        }
        // Composite types aren't tables either.
        if relkind == RELKIND_COMPOSITE_TYPE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("\"{relname}\" is a composite type"))
                .into_error());
        }
        // Used GRANT SEQUENCE on a non-sequence?
        if istmt.objtype == ObjectType::Sequence && relkind != RELKIND_SEQUENCE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("\"{relname}\" is not a sequence"))
                .into_error());
        }

        // Adjust the default permissions based on object type.
        let mut this_privileges = if istmt.all_privs && istmt.privileges == ACL_NO_RIGHTS {
            if relkind == RELKIND_SEQUENCE {
                ACL_ALL_RIGHTS_SEQUENCE
            } else {
                ACL_ALL_RIGHTS_RELATION
            }
        } else {
            istmt.privileges
        };

        // The GRANT TABLE syntax can be used for sequences and non-sequences,
        // so look at the relkind to determine the supported permissions.
        if istmt.objtype == ObjectType::Table {
            if relkind == RELKIND_SEQUENCE {
                // For backward compatibility, just warn on invalid sequence
                // permissions when using the non-sequence GRANT syntax.
                if this_privileges & !ACL_ALL_RIGHTS_SEQUENCE != 0 {
                    ereport(WARNING)
                        .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                        .errmsg(format!(
                            "sequence \"{relname}\" only supports USAGE, SELECT, and UPDATE privileges"
                        ))
                        .finish(errloc(1873, "ExecGrant_Relation"))?;
                    this_privileges &= ACL_ALL_RIGHTS_SEQUENCE;
                }
            } else if this_privileges & !ACL_ALL_RIGHTS_RELATION != 0 {
                // USAGE is the only permission supported by sequences but not
                // non-sequences.
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                    .errmsg("invalid privilege type USAGE for table".to_string())
                    .into_error());
            }
        }

        // Set up array in which we'll accumulate any column privilege bits
        // that need modification. The array is indexed such that entry [0]
        // corresponds to FirstLowInvalidHeapAttributeNumber.
        let num_col_privileges =
            (relnatts as i32 - FirstLowInvalidHeapAttributeNumber as i32 + 1) as usize;
        let mut col_privileges: PgVec<AclMode> = mcx::vec_with_capacity_in(mcx, num_col_privileges)?;
        col_privileges.resize(num_col_privileges, ACL_NO_RIGHTS);
        let mut have_col_privileges = false;

        // If we are revoking relation privileges that are also column
        // privileges, we must implicitly revoke them from each column too,
        // per SQL spec.
        if !istmt.is_grant && (this_privileges & ACL_ALL_RIGHTS_COLUMN) != 0 {
            expand_all_col_privileges(
                mcx,
                rel_oid,
                relkind,
                relnatts,
                this_privileges & ACL_ALL_RIGHTS_COLUMN,
                &mut col_privileges,
                num_col_privileges,
            )?;
            have_col_privileges = true;
        }

        // Get owner ID and working copy of existing ACL. If there's no ACL,
        // substitute the proper default.
        let (acl_datum, acl_is_null) = SysCacheGetAttr(mcx, RELOID, &tuple, acl_attnum)?;
        let old_acl: &[AclItem];
        let mut old_members: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
        let has_old_members;
        if acl_is_null {
            let default_objtype = if relkind == RELKIND_SEQUENCE {
                ObjectType::Sequence
            } else {
                ObjectType::Table
            };
            old_acl = acldefault(mcx, default_objtype, owner_id)?;
            has_old_members = false;
        } else {
            let raw = match &acl_datum {
                Datum::ByRef(b) => &b[..],
                _ => return Err(PgError::error("ExecGrant_Relation: ACL column is not a varlena")),
            };
            old_acl = decode_acl(mcx, raw)?;
            for m in aclmembers(mcx, old_acl)?.iter() {
                old_members.push(*m);
            }
            has_old_members = true;
        }

        // Need an extra copy of original rel ACL for column handling.
        let old_rel_acl: &[AclItem] = {
            let buf = mcx::vec_with_capacity_in::<AclItem>(mcx, old_acl.len())?;
            let mut buf = buf;
            for it in old_acl {
                buf.push(*it);
            }
            buf.leak()
        };

        // Handle relation-level privileges, if any were specified.
        if this_privileges != ACL_NO_RIGHTS {
            let user_id = backend_utils_init_miscinit_seams::get_user_id::call();
            let (grantor_id, avail_goptions) =
                select_best_grantor(user_id, this_privileges, old_acl, owner_id)?;

            let objtype = if relkind == RELKIND_SEQUENCE {
                ObjectType::Sequence
            } else {
                ObjectType::Table
            };

            let this_privileges = restrict_and_check_grant(
                mcx,
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

            // Update initial privileges for extensions.
            record_extension_init_priv(rel_oid, RelationRelationId, new_acl)?;

            // Update the shared dependency ACL info.
            let old_for_dep = if has_old_members {
                old_members
            } else {
                mcx::vec_with_capacity_in(mcx, 0)?
            };
            backend_catalog_pg_shdepend_seams::updateAclDependencies::call(
                mcx,
                RelationRelationId,
                rel_oid,
                0,
                owner_id,
                old_for_dep,
                new_members,
            )?;
        } else {
            // UnlockTuple(relation, &tuple->t_self, InplaceUpdateTupleLock).
            guard.release()?;
        }

        // Handle column-level privileges, if any were specified or implied.
        // First expand the user-specified column privileges into the array,
        // then iterate over all nonempty array entries.
        for col_priv in istmt.col_privs.iter() {
            let mut col_priv_privileges = match &col_priv.priv_name {
                None => ACL_ALL_RIGHTS_COLUMN,
                Some(name) => string_to_privilege(name.as_str())?,
            };

            if col_priv_privileges & !ACL_ALL_RIGHTS_COLUMN != 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                    .errmsg(format!(
                        "invalid privilege type {} for column",
                        crate::privilege_to_string(col_priv_privileges)?
                    ))
                    .into_error());
            }

            if relkind == RELKIND_SEQUENCE && (col_priv_privileges & !ACL_SELECT) != 0 {
                // The only column privilege allowed on sequences is SELECT.
                ereport(WARNING)
                    .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                    .errmsg(format!(
                        "sequence \"{relname}\" only supports SELECT column privileges"
                    ))
                    .finish(errloc(2069, "ExecGrant_Relation"))?;
                col_priv_privileges &= ACL_SELECT;
            }

            expand_col_privileges(
                mcx,
                &col_priv.cols,
                rel_oid,
                col_priv_privileges,
                &mut col_privileges,
                num_col_privileges,
            )?;
            have_col_privileges = true;
        }

        if have_col_privileges {
            for i in 0..num_col_privileges {
                if col_privileges[i] == ACL_NO_RIGHTS {
                    continue;
                }
                let attnum = i as i32 + FirstLowInvalidHeapAttributeNumber as i32;
                exec_grant_attribute(
                    mcx,
                    istmt,
                    rel_oid,
                    &relname,
                    attnum as types_core::AttrNumber,
                    owner_id,
                    col_privileges[i],
                    &att_relation,
                    old_rel_acl,
                )?;
            }
        }

        // prevent error when processing duplicate objects.
        backend_access_transam_xact_seams::command_counter_increment::call()?;
    }

    table_close(att_relation, RowExclusiveLock)?;
    table_close(relation, RowExclusiveLock)?;
    Ok(())
}

/// `ExecGrant_Largeobject(istmt)` (aclchk.c:2268) — the GRANT/REVOKE executor
/// leg for `OBJECT_LARGEOBJECT`. There is no syscache for
/// `pg_largeobject_metadata`, so each target object's row is fetched by a
/// `systable_beginscan` on `LargeObjectMetadataOidIndexId`; the `lomacl`
/// column is recomputed via `merge_acl_with_grant` and written back with
/// `heap_modify_tuple` + `CatalogTupleUpdate`.
fn exec_grant_largeobject(mcx: Mcx<'_>, istmt: &mut InternalGrant<'_>) -> PgResult<()> {
    use backend_access_common_scankey::ScanKeyInit;
    use backend_access_index_genam_seams as genam;
    use backend_catalog_objectaddress::consts::{
        Anum_pg_largeobject_metadata_lomacl, Anum_pg_largeobject_metadata_lomowner,
        Anum_pg_largeobject_metadata_oid, LargeObjectMetadataOidIndexId,
        LargeObjectMetadataRelationId, LargeObjectRelationId,
    };
    use backend_access_common_heaptuple::heap_getattr;
    use types_core::fmgr::F_OIDEQ;
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

    if istmt.all_privs && istmt.privileges == ACL_NO_RIGHTS {
        istmt.privileges = ACL_ALL_RIGHTS_LARGEOBJECT;
    }

    let acl_attnum = Anum_pg_largeobject_metadata_lomacl as i32;
    let owner_attnum = Anum_pg_largeobject_metadata_lomowner as i32;

    let relation = table_open(mcx, LargeObjectMetadataRelationId, RowExclusiveLock)?;

    for &loid in istmt.objects.iter() {
        // There's no syscache for pg_largeobject_metadata.
        let mut key = ScanKeyData::empty();
        ScanKeyInit(
            &mut key,
            Anum_pg_largeobject_metadata_oid as i16,
            BTEqualStrategyNumber,
            F_OIDEQ,
            Datum::from_oid(loid),
        )?;
        let keys = [key];

        let mut scan = genam::systable_beginscan::call(
            &relation,
            LargeObjectMetadataOidIndexId,
            true,
            None,
            &keys,
        )?;
        let tuple = genam::systable_getnext::call(mcx, scan.desc_mut())?;
        let Some(tuple) = tuple else {
            scan.end()?;
            table_close(relation, RowExclusiveLock)?;
            return Err(PgError::error(format!(
                "could not find tuple for large object {loid}"
            )));
        };

        // Get owner ID and working copy of existing ACL. If there's no ACL,
        // substitute the proper default.
        let owner_id = heap_getattr(mcx, &tuple, owner_attnum, &relation.rd_att)?.0.as_oid();
        let (acl_datum, acl_is_null) = heap_getattr(mcx, &tuple, acl_attnum, &relation.rd_att)?;

        let old_acl: &[AclItem];
        let mut old_members: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
        let has_old_members;
        if acl_is_null {
            old_acl = acldefault(mcx, ObjectType::Largeobject, owner_id)?;
            has_old_members = false;
        } else {
            let raw = match &acl_datum {
                Datum::ByRef(b) => &b[..],
                _ => {
                    scan.end()?;
                    table_close(relation, RowExclusiveLock)?;
                    return Err(PgError::error(
                        "ExecGrant_Largeobject: lomacl column is not a varlena",
                    ));
                }
            };
            old_acl = decode_acl(mcx, raw)?;
            for m in aclmembers(mcx, old_acl)?.iter() {
                old_members.push(*m);
            }
            has_old_members = true;
        }

        // Determine ID to do the grant as, and available grant options.
        let user_id = backend_utils_init_miscinit_seams::get_user_id::call();
        let (grantor_id, avail_goptions) =
            select_best_grantor(user_id, istmt.privileges, old_acl, owner_id)?;

        // Restrict the privileges to what we can actually grant, and emit the
        // standards-mandated warning and error messages.
        let loname = format!("large object {loid}");
        let this_privileges = restrict_and_check_grant(
            mcx,
            istmt.is_grant,
            avail_goptions,
            istmt.all_privs,
            istmt.privileges,
            loid,
            grantor_id,
            ObjectType::Largeobject,
            &loname,
            0,
            None,
        )?;

        // Generate new ACL.
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

        // finished building new ACL value, now insert it.
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

        // Update initial privileges for extensions.
        record_extension_init_priv(loid, LargeObjectRelationId, new_acl)?;

        // Update the shared dependency ACL info.
        let old_for_dep = if has_old_members {
            old_members
        } else {
            mcx::vec_with_capacity_in(mcx, 0)?
        };
        backend_catalog_pg_shdepend_seams::updateAclDependencies::call(
            mcx,
            LargeObjectRelationId,
            loid,
            0,
            owner_id,
            old_for_dep,
            new_members,
        )?;

        scan.end()?;

        // prevent error when processing duplicate objects.
        backend_access_transam_xact_seams::command_counter_increment::call()?;
    }

    table_close(relation, RowExclusiveLock)?;
    Ok(())
}

/// `expand_col_privileges(colnames, table_oid, this_privileges, col_privileges,
/// num_col_privileges)` (aclchk.c). OR the specified privilege(s) into the
/// per-column array entries for the named columns.
fn expand_col_privileges(
    mcx: Mcx<'_>,
    colnames: &PgVec<'_, types_nodes::nodes::NodePtr<'_>>,
    table_oid: Oid,
    this_privileges: AclMode,
    col_privileges: &mut [AclMode],
    num_col_privileges: usize,
) -> PgResult<()> {
    for cell in colnames.iter() {
        let Some(s) = (**cell).as_string() else {
            return Err(PgError::error(
                "expand_col_privileges: column name is not a String node",
            ));
        };
        let colname = s.sval.as_str();
        let attnum = backend_utils_cache_lsyscache_seams::get_attnum::call(table_oid, colname)?;
        if attnum == 0 {
            let relname = backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, table_oid)?
                .map(|n| n.as_str().to_string())
                .unwrap_or_default();
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{colname}\" of relation \"{relname}\" does not exist"
                ))
                .into_error());
        }
        let idx = (attnum as i32 - FirstLowInvalidHeapAttributeNumber as i32) as usize;
        if idx == 0 || idx >= num_col_privileges {
            return Err(PgError::error("column number out of range"));
        }
        col_privileges[idx] |= this_privileges;
    }
    Ok(())
}

/// `expand_all_col_privileges(table_oid, classForm, this_privileges,
/// col_privileges, num_col_privileges)` (aclchk.c). OR the privilege(s) into
/// the per-column array for each valid (non-dropped) attribute of a relation.
fn expand_all_col_privileges(
    mcx: Mcx<'_>,
    table_oid: Oid,
    relkind: u8,
    relnatts: i16,
    this_privileges: AclMode,
    col_privileges: &mut [AclMode],
    num_col_privileges: usize,
) -> PgResult<()> {
    const ATTNUM: i32 = 7;
    debug_assert!(
        (relnatts as i32 - FirstLowInvalidHeapAttributeNumber as i32) < num_col_privileges as i32
    );
    let mut curr_att = FirstLowInvalidHeapAttributeNumber as i32 + 1;
    while curr_att <= relnatts as i32 {
        if curr_att == 0 {
            curr_att += 1;
            continue;
        }
        // Views don't have any system columns at all.
        if relkind == RELKIND_VIEW && curr_att < 0 {
            curr_att += 1;
            continue;
        }

        let att_tuple = backend_utils_cache_syscache::SearchSysCache2(
            mcx,
            ATTNUM,
            SysCacheKey::Value(KeyDatum::from_oid(table_oid)),
            SysCacheKey::Value(KeyDatum::from_i16(curr_att as i16)),
        )?;
        let Some(att_tuple) = att_tuple else {
            return Err(PgError::error(format!(
                "cache lookup failed for attribute {curr_att} of relation {table_oid}"
            )));
        };
        let isdropped =
            SysCacheGetAttrNotNull(mcx, ATTNUM, &att_tuple, Anum_pg_attribute_attisdropped as i32)?
                .as_bool();

        // ignore dropped columns
        if !isdropped {
            let idx = (curr_att - FirstLowInvalidHeapAttributeNumber as i32) as usize;
            col_privileges[idx] |= this_privileges;
        }
        curr_att += 1;
    }
    Ok(())
}

/// `ExecGrant_Attribute(istmt, relOid, relname, attnum, ownerId,
/// col_privileges, attRelation, old_rel_acl)` (aclchk.c). Merge the column ACL
/// for one attribute and write it back to `pg_attribute.attacl`.
#[allow(clippy::too_many_arguments)]
fn exec_grant_attribute(
    mcx: Mcx<'_>,
    istmt: &InternalGrant<'_>,
    rel_oid: Oid,
    relname: &str,
    attnum: types_core::AttrNumber,
    owner_id: Oid,
    col_privileges: AclMode,
    att_relation: &types_rel::Relation<'_>,
    old_rel_acl: &[AclItem],
) -> PgResult<()> {
    const ATTNUM: i32 = 7;
    let attacl_attnum = Anum_pg_attribute_attacl as i32;

    let att_tuple = backend_utils_cache_syscache::SearchSysCache2(
        mcx,
        ATTNUM,
        SysCacheKey::Value(KeyDatum::from_oid(rel_oid)),
        SysCacheKey::Value(KeyDatum::from_i16(attnum)),
    )?;
    let Some(att_tuple) = att_tuple else {
        return Err(PgError::error(format!(
            "cache lookup failed for attribute {attnum} of relation {rel_oid}"
        )));
    };

    let attname = read_name(mcx, ATTNUM, &att_tuple, Anum_pg_attribute_attname as i32)?;

    // Get working copy of existing ACL. If there's no ACL, substitute the
    // proper default.
    let (acl_datum, acl_is_null) = SysCacheGetAttr(mcx, ATTNUM, &att_tuple, attacl_attnum)?;
    let old_acl: &[AclItem];
    let mut old_members: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    let has_old_members;
    if acl_is_null {
        old_acl = acldefault(mcx, ObjectType::Column, owner_id)?;
        has_old_members = false;
    } else {
        let raw = match &acl_datum {
            Datum::ByRef(b) => &b[..],
            _ => return Err(PgError::error("ExecGrant_Attribute: ACL column is not a varlena")),
        };
        old_acl = decode_acl(mcx, raw)?;
        for m in aclmembers(mcx, old_acl)?.iter() {
            old_members.push(*m);
        }
        has_old_members = true;
    }

    // In select_best_grantor we should consider existing table-level ACL bits
    // as well as the per-column ACL. Build a new ACL that is their
    // concatenation.
    let merged_acl =
        backend_utils_adt_acl::acl_ops::aclconcat(mcx, old_rel_acl, old_acl)?;

    let user_id = backend_utils_init_miscinit_seams::get_user_id::call();
    let (grantor_id, avail_goptions) =
        select_best_grantor(user_id, col_privileges, merged_acl, owner_id)?;

    let col_privileges = restrict_and_check_grant(
        mcx,
        istmt.is_grant,
        avail_goptions,
        col_privileges == ACL_ALL_RIGHTS_COLUMN,
        col_privileges,
        rel_oid,
        grantor_id,
        ObjectType::Column,
        relname,
        attnum,
        Some(&attname),
    )?;

    let new_acl = merge_acl_with_grant(
        mcx,
        old_acl,
        istmt.is_grant,
        istmt.grant_option,
        istmt.behavior,
        &istmt.grantees,
        col_privileges,
        grantor_id,
        owner_id,
    )?;

    let mut new_members: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    for m in aclmembers(mcx, new_acl)?.iter() {
        new_members.push(*m);
    }

    // If the updated ACL is empty, we can set attacl to null, and maybe even
    // avoid an update of the pg_attribute row. We'll come through here multiple
    // times for any relation-level REVOKE even if there were never any column
    // GRANTs.
    let natts = att_relation.rd_att.natts as usize;
    let mut values: PgVec<Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut nulls: PgVec<bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replaces: PgVec<bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    for _ in 0..natts {
        values.push(Datum::ByVal(0));
        nulls.push(false);
        replaces.push(false);
    }
    let aidx = (attacl_attnum - 1) as usize;
    let need_update;
    if new_acl.len() > 0 {
        values[aidx] = acl_to_datum(mcx, new_acl)?;
        need_update = true;
    } else {
        nulls[aidx] = true;
        need_update = !acl_is_null;
    }
    replaces[aidx] = true;

    if need_update {
        let mut newtuple =
            heap_modify_tuple(mcx, &att_tuple, &att_relation.rd_att, &values, &nulls, &replaces)
                .map_err(|e| PgError::error(format!("heap_modify_tuple failed: {e:?}")))?;
        let otid = att_tuple.tuple.t_self;
        CatalogTupleUpdate(mcx, att_relation, otid, &mut newtuple)?;

        // Update initial privileges for extensions.
        record_extension_init_priv(rel_oid, RelationRelationId, new_acl)?;

        // Update the shared dependency ACL info.
        let old_for_dep = if has_old_members {
            old_members
        } else {
            mcx::vec_with_capacity_in(mcx, 0)?
        };
        backend_catalog_pg_shdepend_seams::updateAclDependencies::call(
            mcx,
            RelationRelationId,
            rel_oid,
            attnum as i32,
            owner_id,
            old_for_dep,
            new_members,
        )?;
    }

    Ok(())
}

/// Inverse of the syscache `decode_acl`: read a 1-D `aclitem[]` array varlena
/// into an `&[AclItem]`. The header layout matches [`acl_to_datum`].
pub(crate) fn decode_acl<'mcx>(mcx: Mcx<'mcx>, on_disk: &[u8]) -> PgResult<&'mcx mut [AclItem]> {
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
    use backend_catalog_objectaddress::consts::{DatabaseRelationId, LanguageRelationId};
    match istmt.objtype {
        ObjectType::Table | ObjectType::Sequence => exec_grant_relation(mcx, istmt),
        OBJECT_SCHEMA => {
            exec_grant_common(mcx, istmt, NamespaceRelationId, ACL_ALL_RIGHTS_SCHEMA, None)
        }
        ObjectType::Database => {
            exec_grant_common(mcx, istmt, DatabaseRelationId, ACL_ALL_RIGHTS_DATABASE, None)
        }
        ObjectType::Domain | ObjectType::Type => exec_grant_common(
            mcx,
            istmt,
            TypeRelationId,
            ACL_ALL_RIGHTS_TYPE,
            Some(exec_grant_type_check),
        ),
        ObjectType::Function | ObjectType::Procedure | ObjectType::Routine => {
            exec_grant_common(mcx, istmt, ProcedureRelationId, ACL_ALL_RIGHTS_FUNCTION, None)
        }
        ObjectType::Largeobject => exec_grant_largeobject(mcx, istmt),
        ObjectType::Language => exec_grant_common(
            mcx,
            istmt,
            LanguageRelationId,
            ACL_ALL_RIGHTS_LANGUAGE,
            Some(exec_grant_language_check),
        ),
        other => Err(PgError::error(format!(
            "GRANT/REVOKE executor not ported for object type {other:?} \
             (schema/relation slice; remaining aclchk F2/F3 keystone)"
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

/* ===========================================================================
 * ALTER DEFAULT PRIVILEGES (aclchk.c)
 *
 * `ExecAlterDefaultPrivilegesStmt` parses the statement (target roles, schemas,
 * object type, embedded GrantStmt action), then for each (role, schema)
 * combination calls `SetDefaultACL`, which writes/updates a `pg_default_acl`
 * row holding the default ACL for objects of that type created by that role in
 * that schema. The new ACL is computed via `merge_acl_with_grant` over the
 * action's privileges/grantees and written with
 * `CatalogTupleInsert`/`CatalogTupleUpdate` (or deleted when it collapses back
 * to the hard-wired default) on `pg_default_acl`, recording the owner and
 * (for the per-schema case) namespace dependencies.
 * ========================================================================= */

// `ACL_ALL_RIGHTS_FUNCTION` / `ACL_ALL_RIGHTS_TYPE` / `ACL_ALL_RIGHTS_LARGEOBJECT`
// (`utils/acl.h`).
const ACL_ALL_RIGHTS_FUNCTION: AclMode = types_acl::ACL_EXECUTE;
const ACL_ALL_RIGHTS_TYPE: AclMode = ACL_USAGE;
const ACL_ALL_RIGHTS_LARGEOBJECT: AclMode = ACL_SELECT | ACL_UPDATE;
// `ACL_ALL_RIGHTS_LANGUAGE` (`utils/acl.h`).
const ACL_ALL_RIGHTS_LANGUAGE: AclMode = ACL_USAGE;
// `ACL_ALL_RIGHTS_DATABASE` (`utils/acl.h`).
const ACL_ALL_RIGHTS_DATABASE: AclMode =
    types_acl::ACL_CREATE | types_acl::ACL_CREATE_TEMP | types_acl::ACL_CONNECT;

/// `InternalDefaultACL` (`aclchk.c`) — the internal form
/// `ExecAlterDefaultPrivilegesStmt` builds before dispatching to
/// `SetDefaultACL`.
struct InternalDefaultACL<'mcx> {
    roleid: Oid,
    nspid: Oid,
    is_grant: bool,
    objtype: ObjectType,
    all_privs: bool,
    privileges: AclMode,
    grantees: PgVec<'mcx, Oid>,
    grant_option: bool,
    behavior: DropBehavior,
}

/// `ExecAlterDefaultPrivilegesStmt(pstate, stmt)` (aclchk.c). The slow-path leg
/// installed for `Node::AlterDefaultPrivilegesStmt`.
pub fn exec_alter_default_privileges_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &Node<'mcx>,
) -> PgResult<()> {
    let Some(stmt) = stmt.as_alterdefaultprivilegesstmt() else {
        return Err(PgError::error(
            "exec_alter_default_privileges_stmt: not an AlterDefaultPrivilegesStmt",
        ));
    };
    let Some(action_node) = &stmt.action else {
        return Err(PgError::error(
            "ExecAlterDefaultPrivilegesStmt: missing GrantStmt action",
        ));
    };
    let Some(action) = action_node.as_grantstmt() else {
        return Err(PgError::error(
            "ExecAlterDefaultPrivilegesStmt: action is not a GrantStmt",
        ));
    };

    // Scan the options for the (optional) FOR ROLE and IN SCHEMA lists.
    // Each DefElem's `arg` holds the `List *` (a Vec of String/RoleSpec nodes).
    let mut have_nspnames = false;
    let mut have_rolespecs = false;
    let mut rolespecs: &[types_nodes::nodes::NodePtr<'_>] = &[];
    let mut nspnames: &[types_nodes::nodes::NodePtr<'_>] = &[];

    for opt in stmt.options.iter() {
        let Some(defel) = (**opt).as_defelem() else {
            return Err(PgError::error(
                "ExecAlterDefaultPrivilegesStmt: option is not a DefElem",
            ));
        };
        let defname = defel.defname.as_deref().unwrap_or("");
        if defname == "schemas" {
            if have_nspnames {
                backend_catalog_aclchk_seams::error_conflicting_def_elem::call(
                    defname.to_string(),
                )?;
            }
            have_nspnames = true;
            nspnames = defel_list(defel)?;
        } else if defname == "roles" {
            if have_rolespecs {
                backend_catalog_aclchk_seams::error_conflicting_def_elem::call(
                    defname.to_string(),
                )?;
            }
            have_rolespecs = true;
            rolespecs = defel_list(defel)?;
        } else {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("option \"{defname}\" not recognized"))
                .into_error());
        }
    }

    // Build the per-action InternalDefaultACL skeleton (roleid/nspid filled in
    // per combination below).
    let mut iacls = InternalDefaultACL {
        roleid: InvalidOid,
        nspid: InvalidOid,
        is_grant: action.is_grant,
        objtype: action.objtype,
        all_privs: false,
        privileges: ACL_NO_RIGHTS,
        grantees: mcx::vec_with_capacity_in(mcx, action.grantees.len())?,
        grant_option: action.grant_option,
        behavior: action.behavior,
    };

    // Convert the grantee RoleSpec list to an Oid list (PUBLIC -> ACL_ID_PUBLIC).
    for g in action.grantees.iter() {
        let Some(rs) = (**g).as_rolespec() else {
            return Err(PgError::error(
                "ExecAlterDefaultPrivilegesStmt: grantee is not a RoleSpec",
            ));
        };
        let uid = match rs.roletype {
            RoleSpecType::Public => ACL_ID_PUBLIC,
            _ => rolespec_oid(rs, mcx)?,
        };
        iacls.grantees.push(uid);
    }

    // Per-objtype all-privileges mask + error message.
    let (all_privileges, errormsg): (AclMode, &str) = match action.objtype {
        ObjectType::Table => (ACL_ALL_RIGHTS_RELATION, "invalid privilege type %s for relation"),
        ObjectType::Sequence => (ACL_ALL_RIGHTS_SEQUENCE, "invalid privilege type %s for sequence"),
        ObjectType::Function => (ACL_ALL_RIGHTS_FUNCTION, "invalid privilege type %s for function"),
        ObjectType::Procedure => (ACL_ALL_RIGHTS_FUNCTION, "invalid privilege type %s for procedure"),
        ObjectType::Routine => (ACL_ALL_RIGHTS_FUNCTION, "invalid privilege type %s for routine"),
        ObjectType::Type => (ACL_ALL_RIGHTS_TYPE, "invalid privilege type %s for type"),
        OBJECT_SCHEMA => (ACL_ALL_RIGHTS_SCHEMA, "invalid privilege type %s for schema"),
        ObjectType::Largeobject => {
            (ACL_ALL_RIGHTS_LARGEOBJECT, "invalid privilege type %s for large object")
        }
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized GrantStmt.objtype: {other:?}"))
                .into_error())
        }
    };

    // Convert the privilege list into an AclMode bitmask.
    if action.privileges.is_empty() {
        iacls.all_privs = true;
        iacls.privileges = ACL_NO_RIGHTS;
    } else {
        iacls.all_privs = false;
        iacls.privileges = ACL_NO_RIGHTS;
        for p in action.privileges.iter() {
            let Some(privnode) = (**p).as_accesspriv() else {
                return Err(PgError::error(
                    "ExecAlterDefaultPrivilegesStmt: privilege is not an AccessPriv",
                ));
            };
            if !privnode.cols.is_empty() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                    .errmsg("default privileges cannot be set for columns".to_string())
                    .into_error());
            }
            let Some(name) = &privnode.priv_name else {
                return Err(PgError::error("AccessPriv node must specify privilege"));
            };
            let priv_bit = string_to_privilege(name.as_str())?;
            if priv_bit & !all_privileges != 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                    .errmsg(errormsg.replace("%s", crate::privilege_to_string(priv_bit)?))
                    .into_error());
            }
            iacls.privileges |= priv_bit;
        }
    }

    if rolespecs.is_empty() {
        // Set permissions for myself.
        iacls.roleid = backend_utils_init_miscinit_seams::get_user_id::call();
        set_default_acls_in_schemas(mcx, &mut iacls, nspnames)?;
    } else {
        // Set permissions for the named roles.
        for rolecell in rolespecs.iter() {
            let Some(rolespec) = (**rolecell).as_rolespec() else {
                return Err(PgError::error(
                    "ExecAlterDefaultPrivilegesStmt: role is not a RoleSpec",
                ));
            };
            iacls.roleid = rolespec_oid(rolespec, mcx)?;

            // We insist that calling user be a member of each target role. If
            // he has that, he could become that role anyway via SET ROLE, so
            // FOR ROLE is just a syntactic convenience and doesn't give any
            // special privileges.
            let user_id = backend_utils_init_miscinit_seams::get_user_id::call();
            if !backend_utils_adt_acl::role_membership::has_privs_of_role(user_id, iacls.roleid)? {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg("permission denied to change default privileges".to_string())
                    .into_error());
            }

            set_default_acls_in_schemas(mcx, &mut iacls, nspnames)?;
        }
    }

    Ok(())
}

/// Extract the `List *` carried in a DefElem's `arg` as a node slice. The
/// parser stores the FOR ROLE / IN SCHEMA lists as a `List` node in
/// `defel->arg`.
fn defel_list<'a, 'mcx>(
    defel: &'a types_nodes::ddlnodes::DefElem<'mcx>,
) -> PgResult<&'a [types_nodes::nodes::NodePtr<'mcx>]> {
    match &defel.arg {
        None => Ok(&[]),
        Some(arg) => match (**arg).as_list() {
            Some(items) => Ok(items),
            None => Err(PgError::error(
                "ExecAlterDefaultPrivilegesStmt: DefElem arg is not a List",
            )),
        },
    }
}

/// `SetDefaultACLsInSchemas(iacls, nspnames)` (aclchk.c): set per-object-type
/// default ACLs for a single role, either globally (`nspnames` empty -> nspid
/// = InvalidOid) or for each named schema.
fn set_default_acls_in_schemas<'mcx>(
    mcx: Mcx<'mcx>,
    iacls: &mut InternalDefaultACL<'mcx>,
    nspnames: &[types_nodes::nodes::NodePtr<'_>],
) -> PgResult<()> {
    if nspnames.is_empty() {
        // Set the database-wide permissions.
        iacls.nspid = InvalidOid;
        set_default_acl(mcx, iacls)?;
    } else {
        // Set per-schema permissions.
        for nspcell in nspnames.iter() {
            let Some(s) = (**nspcell).as_string() else {
                return Err(PgError::error(
                    "SetDefaultACLsInSchemas: schema name is not a String node",
                ));
            };
            iacls.nspid = backend_catalog_namespace_seams::get_namespace_oid::call(
                s.sval.as_str(),
                false,
            )?;
            set_default_acl(mcx, iacls)?;
        }
    }
    Ok(())
}

/// `SetDefaultACL(iacls)` (aclchk.c): create, update or remove a
/// `pg_default_acl` entry holding the default ACL described by `iacls`.
fn set_default_acl<'mcx>(mcx: Mcx<'mcx>, iacls: &InternalDefaultACL<'mcx>) -> PgResult<()> {
    use types_catalog::catalog::DEFAULT_ACL_RELATION_ID;

    const DEFACLROLENSPOBJ: i32 = 22;
    let acl_attnum = Anum_pg_default_acl_defaclacl as i32;

    let mut this_privileges = iacls.privileges;

    let rel = table_open(mcx, DEFAULT_ACL_RELATION_ID, RowExclusiveLock)?;

    // Build the hard-wired default ACL the new ACL is compared against. For a
    // per-schema entry the "default" is empty (no implicit privileges); for a
    // global (per-role) entry it is the owner's hard-wired default.
    let def_acl: &[AclItem] = if iacls.nspid == InvalidOid {
        acldefault(mcx, iacls.objtype, iacls.roleid)?
    } else {
        make_empty_acl(mcx)?
    };

    // objtype char + adjust ALL-privileges expansion.
    let objtype: i8 = match iacls.objtype {
        ObjectType::Table => {
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_RELATION;
            }
            DEFACLOBJ_RELATION
        }
        ObjectType::Sequence => {
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_SEQUENCE;
            }
            DEFACLOBJ_SEQUENCE
        }
        ObjectType::Function | ObjectType::Procedure | ObjectType::Routine => {
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_FUNCTION;
            }
            DEFACLOBJ_FUNCTION
        }
        ObjectType::Type => {
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_TYPE;
            }
            DEFACLOBJ_TYPE
        }
        OBJECT_SCHEMA => {
            if iacls.nspid != InvalidOid {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                    .errmsg(
                        "cannot use IN SCHEMA clause when using GRANT/REVOKE ON SCHEMAS"
                            .to_string(),
                    )
                    .into_error());
            }
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_SCHEMA;
            }
            DEFACLOBJ_NAMESPACE
        }
        ObjectType::Largeobject => {
            if iacls.nspid != InvalidOid {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                    .errmsg(
                        "cannot use IN SCHEMA clause when using GRANT/REVOKE ON LARGE OBJECTS"
                            .to_string(),
                    )
                    .into_error());
            }
            if iacls.all_privs && this_privileges == ACL_NO_RIGHTS {
                this_privileges = ACL_ALL_RIGHTS_LARGEOBJECT;
            }
            DEFACLOBJ_LARGEOBJECT
        }
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized object type: {other:?}"))
                .into_error())
        }
    };

    // Look up the existing entry, if any.
    let existing = SearchSysCache3(
        mcx,
        DEFACLROLENSPOBJ,
        SysCacheKey::Value(KeyDatum::from_oid(iacls.roleid)),
        SysCacheKey::Value(KeyDatum::from_oid(iacls.nspid)),
        SysCacheKey::Value(KeyDatum::from_char(objtype)),
    )?;

    // Determine old ACL + members.
    let old_acl: &[AclItem];
    let mut old_members: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    let is_new;
    let existing_oid;
    match &existing {
        Some(tuple) => {
            is_new = false;
            existing_oid = SysCacheGetAttrNotNull(
                mcx,
                DEFACLROLENSPOBJ,
                tuple,
                Anum_pg_default_acl_oid as i32,
            )?
            .as_oid();
            let (acl_datum, acl_is_null) =
                SysCacheGetAttr(mcx, DEFACLROLENSPOBJ, tuple, acl_attnum)?;
            if acl_is_null {
                // Existing row but NULL ACL — treat like a fresh default copy.
                old_acl = aclcopy(mcx, def_acl)?;
            } else {
                let raw = match &acl_datum {
                    Datum::ByRef(b) => &b[..],
                    _ => {
                        return Err(PgError::error(
                            "SetDefaultACL: defaclacl column is not a varlena",
                        ))
                    }
                };
                let decoded = decode_acl(mcx, raw)?;
                for m in aclmembers(mcx, decoded)?.iter() {
                    old_members.push(*m);
                }
                old_acl = decoded;
            }
        }
        None => {
            is_new = true;
            existing_oid = InvalidOid;
            // No old entry — base on the hard-wired default (no old members).
            old_acl = aclcopy(mcx, def_acl)?;
        }
    }

    // Generate new ACL. Grantor and owner are both the target role.
    let new_acl_ref = merge_acl_with_grant(
        mcx,
        old_acl,
        iacls.is_grant,
        iacls.grant_option,
        iacls.behavior,
        &iacls.grantees,
        this_privileges,
        iacls.roleid,
        iacls.roleid,
    )?;

    // aclitemsort + aclequal both new_acl and def_acl (private copies so we can
    // sort in place).
    let new_acl: &mut [AclItem] = {
        let mut buf = mcx::vec_with_capacity_in::<AclItem>(mcx, new_acl_ref.len())?;
        for it in new_acl_ref.iter() {
            buf.push(*it);
        }
        buf.leak()
    };
    let def_acl_sorted: &mut [AclItem] = {
        let mut buf = mcx::vec_with_capacity_in::<AclItem>(mcx, def_acl.len())?;
        for it in def_acl.iter() {
            buf.push(*it);
        }
        buf.leak()
    };
    aclitemsort(new_acl);
    aclitemsort(def_acl_sorted);

    if aclequal(new_acl, def_acl_sorted) {
        // Default permissions = nothing to store. Remove the entry if it exists.
        if !is_new {
            backend_catalog_dependency_seams::perform_deletion::call(
                DEFAULT_ACL_RELATION_ID,
                existing_oid,
                0,
                DropBehavior::Restrict,
                0,
            )?;
        }
    } else {
        let natts = rel.rd_att.natts as usize;
        let mut values: PgVec<Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut nulls: PgVec<bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replaces: PgVec<bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        for _ in 0..natts {
            values.push(Datum::ByVal(0));
            nulls.push(false);
            replaces.push(false);
        }

        let def_acl_oid: Oid;
        if is_new {
            def_acl_oid = backend_catalog_catalog::GetNewOidWithIndex(
                &rel,
                DefaultAclOidIndexId,
                Anum_pg_default_acl_oid as types_core::AttrNumber,
            )?;
            values[(Anum_pg_default_acl_oid - 1) as usize] = Datum::from_oid(def_acl_oid);
            values[(Anum_pg_default_acl_defaclrole - 1) as usize] = Datum::from_oid(iacls.roleid);
            values[(Anum_pg_default_acl_defaclnamespace - 1) as usize] =
                Datum::from_oid(iacls.nspid);
            values[(Anum_pg_default_acl_defaclobjtype - 1) as usize] = Datum::from_char(objtype);
            values[(Anum_pg_default_acl_defaclacl - 1) as usize] = acl_to_datum(mcx, new_acl)?;

            let mut newtuple = heap_form_tuple(mcx, &rel.rd_att, &values, &nulls)
                .map_err(|e| PgError::error(format!("heap_form_tuple failed: {e:?}")))?;
            CatalogTupleInsert(mcx, &rel, &mut newtuple)?;
        } else {
            let tuple = existing.expect("non-new SetDefaultACL has an existing tuple");
            def_acl_oid = existing_oid;
            let aidx = (acl_attnum - 1) as usize;
            replaces[aidx] = true;
            values[aidx] = acl_to_datum(mcx, new_acl)?;

            let mut newtuple =
                heap_modify_tuple(mcx, &tuple, &rel.rd_att, &values, &nulls, &replaces)
                    .map_err(|e| PgError::error(format!("heap_modify_tuple failed: {e:?}")))?;
            let otid = tuple.tuple.t_self;
            CatalogTupleUpdate(mcx, &rel, otid, &mut newtuple)?;
        }

        // Record dependencies (only for a freshly-created entry).
        if is_new {
            backend_catalog_pg_shdepend_seams::recordDependencyOnOwner::call(
                DEFAULT_ACL_RELATION_ID,
                def_acl_oid,
                iacls.roleid,
            )?;
            if iacls.nspid != InvalidOid {
                let myself = types_catalog::catalog_dependency::ObjectAddress {
                    classId: DEFAULT_ACL_RELATION_ID,
                    objectId: def_acl_oid,
                    objectSubId: 0,
                };
                let referenced = types_catalog::catalog_dependency::ObjectAddress {
                    classId: NamespaceRelationId,
                    objectId: iacls.nspid,
                    objectSubId: 0,
                };
                backend_catalog_pg_depend_seams::recordDependencyOn::call(
                    mcx,
                    &myself,
                    &referenced,
                    types_catalog::catalog_dependency::DEPENDENCY_AUTO,
                )?;
            }
        }

        // Update the shared dependency ACL info.
        let mut new_members: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
        for m in aclmembers(mcx, new_acl)?.iter() {
            new_members.push(*m);
        }
        backend_catalog_pg_shdepend_seams::updateAclDependencies::call(
            mcx,
            DEFAULT_ACL_RELATION_ID,
            def_acl_oid,
            0,
            iacls.roleid,
            old_members,
            new_members,
        )?;

        // Post-create / post-alter object-access hooks (no-op in standalone).
        if is_new {
            backend_catalog_objectaccess_seams::invoke_object_post_create_hook::call(
                DEFAULT_ACL_RELATION_ID,
                def_acl_oid,
                0,
            )?;
        } else {
            backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
                DEFAULT_ACL_RELATION_ID,
                def_acl_oid,
                0,
            )?;
        }
    }

    table_close(rel, RowExclusiveLock)?;
    backend_access_transam_xact_seams::command_counter_increment::call()?;
    Ok(())
}

/// `RemoveRoleFromObjectACL(roleid, classid, objid)` (aclchk.c:1423). During
/// `DROP OWNED`, revoke (with CASCADE semantics) every privilege the role holds
/// on the given object. For a `pg_default_acl` row this re-runs
/// `SetDefaultACL` with the role as the lone grantee being removed; for every
/// other catalog it builds an `InternalGrant` REVOKE and runs
/// `ExecGrantStmt_oids`.
pub(crate) fn remove_role_from_object_acl<'mcx>(
    mcx: Mcx<'mcx>,
    roleid: Oid,
    classid: Oid,
    objid: Oid,
) -> PgResult<()> {
    use backend_catalog_objectaddress::consts::{
        DatabaseRelationId, DefaultAclRelationId, ForeignDataWrapperRelationId,
        ForeignServerRelationId, LanguageRelationId, LargeObjectRelationId,
        ParameterAclRelationId, TableSpaceRelationId,
    };

    if classid == DefaultAclRelationId {
        use backend_access_common_scankey::ScanKeyInit;
        use backend_access_index_genam_seams as genam;
        use types_core::fmgr::F_OIDEQ;
        use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

        // first fetch info needed by SetDefaultACL
        let rel = table_open(mcx, DefaultAclRelationId, AccessShareLock)?;

        let mut key = ScanKeyData::empty();
        ScanKeyInit(
            &mut key,
            Anum_pg_default_acl_oid as i16,
            BTEqualStrategyNumber,
            F_OIDEQ,
            Datum::from_oid(objid),
        )?;
        let keys = [key];

        let mut scan =
            genam::systable_beginscan::call(&rel, DefaultAclOidIndexId, true, None, &keys)?;
        let tuple = genam::systable_getnext::call(mcx, scan.desc_mut())?;
        let Some(tuple) = tuple else {
            scan.end()?;
            rel.close(AccessShareLock)?;
            return Err(PgError::error(format!(
                "could not find tuple for default ACL {objid}"
            )));
        };

        let cols = backend_access_common_heaptuple::heap_deform_tuple(
            mcx,
            &tuple.tuple,
            &rel.rd_att,
            &tuple.data,
        )?;
        let defaclrole: Oid = {
            let (v, n) = &cols[(Anum_pg_default_acl_defaclrole - 1) as usize];
            debug_assert!(!*n);
            v.as_oid()
        };
        let defaclnamespace: Oid = {
            let (v, n) = &cols[(Anum_pg_default_acl_defaclnamespace - 1) as usize];
            debug_assert!(!*n);
            v.as_oid()
        };
        let defaclobjtype: i8 = {
            let (v, n) = &cols[(Anum_pg_default_acl_defaclobjtype - 1) as usize];
            debug_assert!(!*n);
            v.as_char()
        };

        let objtype = match defaclobjtype {
            x if x == DEFACLOBJ_RELATION => ObjectType::Table,
            x if x == DEFACLOBJ_SEQUENCE => ObjectType::Sequence,
            x if x == DEFACLOBJ_FUNCTION => ObjectType::Function,
            x if x == DEFACLOBJ_TYPE => ObjectType::Type,
            x if x == DEFACLOBJ_NAMESPACE => ObjectType::Schema,
            x if x == DEFACLOBJ_LARGEOBJECT => ObjectType::Largeobject,
            other => {
                scan.end()?;
                rel.close(AccessShareLock)?;
                return Err(PgError::error(format!(
                    "unexpected default ACL type: {}",
                    other as i32
                )));
            }
        };

        scan.end()?;
        rel.close(AccessShareLock)?;

        let mut grantees: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 1)?;
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
            behavior: DropBehavior::Cascade,
        };

        set_default_acl(mcx, &iacls)?;
        return Ok(());
    }

    let objtype = match classid {
        x if x == RelationRelationId => ObjectType::Table,
        x if x == DatabaseRelationId => ObjectType::Database,
        x if x == TypeRelationId => ObjectType::Type,
        x if x == ProcedureRelationId => ObjectType::Routine,
        x if x == LanguageRelationId => ObjectType::Language,
        x if x == LargeObjectRelationId => ObjectType::Largeobject,
        x if x == NamespaceRelationId => ObjectType::Schema,
        x if x == TableSpaceRelationId => ObjectType::Tablespace,
        x if x == ForeignServerRelationId => ObjectType::ForeignServer,
        x if x == ForeignDataWrapperRelationId => ObjectType::Fdw,
        x if x == ParameterAclRelationId => ObjectType::ParameterAcl,
        other => {
            return Err(PgError::error(format!("unexpected object class {other}")));
        }
    };

    let mut objects: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 1)?;
    objects.push(objid);
    let mut grantees: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 1)?;
    grantees.push(roleid);

    let mut istmt = InternalGrant {
        is_grant: false,
        objtype,
        objects,
        all_privs: true,
        privileges: ACL_NO_RIGHTS,
        col_privs: mcx::vec_with_capacity_in(mcx, 0)?,
        grantees,
        grant_option: false,
        behavior: DropBehavior::Cascade,
    };

    exec_grant_stmt_oids(mcx, &mut istmt)
}

/// `ExecuteGrantStmt(stmt)` (aclchk.c). The slow-path leg installed for
/// `Node::GrantStmt`.
pub fn execute_grant_stmt(mcx: Mcx<'_>, stmt: &Node<'_>) -> PgResult<()> {
    let Some(stmt) = stmt.as_grantstmt() else {
        return Err(PgError::error("execute_grant_stmt: not a GrantStmt"));
    };

    // grantor clause: only for SQL compatibility; must be current user.
    if let Some(grantor) = &stmt.grantor {
        let Some(rs) = (**grantor).as_rolespec() else {
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
            objects_in_schema_to_oids(mcx, stmt.objtype, &stmt.objects)?
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
        let Some(rs) = (**g).as_rolespec() else {
            return Err(PgError::error("ExecuteGrantStmt: grantee is not a RoleSpec"));
        };
        let uid = match rs.roletype {
            RoleSpecType::Public => ACL_ID_PUBLIC,
            _ => rolespec_oid(rs, mcx)?,
        };
        grantees.push(uid);
    }

    // Convert the privilege list into an AclMode bitmask.  Column-level
    // specifications are set aside in col_privs; everything else accumulates
    // into the relation-level mask.
    let (all_privileges, errormsg) = objtype_all_privileges(stmt.objtype)?;
    let mut col_privs: PgVec<AccessPriv> = mcx::vec_with_capacity_in(mcx, 0)?;
    let (all_privs, privileges) = if stmt.privileges.is_empty() {
        (true, ACL_NO_RIGHTS)
    } else {
        let mut acc = ACL_NO_RIGHTS;
        for p in stmt.privileges.iter() {
            let Some(privnode) = (**p).as_accesspriv() else {
                return Err(PgError::error("ExecuteGrantStmt: privilege is not an AccessPriv"));
            };
            // If it's a column-level specification, set it aside in col_privs;
            // but insist it's for a relation.
            if !privnode.cols.is_empty() {
                if stmt.objtype != ObjectType::Table {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                        .errmsg("column privileges are only valid for relations".to_string())
                        .into_error());
                }
                col_privs.push(privnode.clone_in(mcx)?);
                continue;
            }
            let Some(name) = &privnode.priv_name else {
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
        col_privs,
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
        // OBJECT_TABLE | OBJECT_SEQUENCE: don't use get_object_address().  It
        // requires that the specified object type match the actual type of the
        // object, but in GRANT/REVOKE all table-like things are addressed as
        // TABLE.  Resolve the RangeVar to a relOid directly.
        ObjectType::Table | ObjectType::Sequence => {
            let mut objects: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, objnames.len())?;
            for name in objnames.iter() {
                let Some(relvar) = (**name).as_rangevar() else {
                    return Err(PgError::error(
                        "objectNamesToOids(OBJECT_TABLE): object name is not a RangeVar node",
                    ));
                };
                let access_rv = to_access_range_var(relvar);
                let rel_oid = backend_catalog_namespace_seams::range_var_get_relid::call(
                    mcx,
                    &access_rv,
                    AccessShareLock,
                    false,
                )?;
                objects.push(rel_oid);
            }
            Ok(objects)
        }
        OBJECT_SCHEMA => {
            let mut objects: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, objnames.len())?;
            for name in objnames.iter() {
                // get_object_address(OBJECT_SCHEMA, String(name), ...) ->
                // get_object_address_unqualified -> get_namespace_oid(name, false).
                let Some(s) = (**name).as_string() else {
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
        ObjectType::Domain | ObjectType::Type => {
            // The parse representation of types and domains in privilege targets
            // is a name list (List of String), different from the TypeName that
            // get_object_address() expects, so convert here.
            let mut objects: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, objnames.len())?;
            for name in objnames.iter() {
                // typname = (List *) lfirst(cell);
                // tn = makeTypeNameFromNameList(typname);
                let lowered = backend_parser_parse_type::rich_node_to_parse(name)?;
                let types_parsenodes::Node::List(names) = lowered else {
                    return Err(PgError::error(
                        "objectNamesToOids(OBJECT_TYPE): object name is not a List node",
                    ));
                };
                let tn = backend_nodes_makefuncs_seams::make_type_name_from_name_list::call(names)?;
                let tn_node = types_parsenodes::Node::TypeName(tn);

                // address = get_object_address(objtype, (Node *) tn, &relation,
                //                              lockmode, false);
                // Assert(relation == NULL);
                let resolved = backend_catalog_objectaddress_seams::get_object_address::call(
                    mcx,
                    objtype,
                    &tn_node,
                    AccessShareLock,
                    false,
                )?;
                objects.push(resolved.address.objectId);
            }
            Ok(objects)
        }
        _ => {
            // For most object types, we use get_object_address() directly on the
            // parser representation (e.g. ObjectWithArgs for functions).
            let mut objects: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, objnames.len())?;
            for name in objnames.iter() {
                let lowered = backend_parser_parse_type::rich_node_to_parse(name)?;
                let resolved = backend_catalog_objectaddress_seams::get_object_address::call(
                    mcx,
                    objtype,
                    &lowered,
                    AccessShareLock,
                    false,
                )?;
                objects.push(resolved.address.objectId);
            }
            Ok(objects)
        }
    }
}

/// `objectsInSchemaToOids(objtype, nspnames)` (aclchk.c) — find all objects of
/// `objtype` in the named schemas and collect their OIDs. USAGE on the schemas
/// is checked later (by the per-object grant path); there is no privilege
/// checking on the individual objects here.
fn objects_in_schema_to_oids<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    nspnames: &PgVec<'_, types_nodes::nodes::NodePtr<'_>>,
) -> PgResult<PgVec<'mcx, Oid>> {
    use types_catalog::catalog::{
        RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
        RELKIND_SEQUENCE, RELKIND_VIEW,
    };

    let mut objects: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 0)?;

    for cell in nspnames.iter() {
        // nspname = strVal(lfirst(cell)).
        let Some(s) = (**cell).as_string() else {
            return Err(PgError::error(
                "objectsInSchemaToOids: schema name is not a String node",
            ));
        };
        let namespace_id =
            backend_catalog_namespace_seams::lookup_explicit_namespace::call(s.sval.as_str(), false)?;

        match objtype {
            ObjectType::Table => {
                for relkind in [
                    RELKIND_RELATION,
                    RELKIND_VIEW,
                    RELKIND_MATVIEW,
                    RELKIND_FOREIGN_TABLE,
                    RELKIND_PARTITIONED_TABLE,
                ] {
                    let mut objs = get_relations_in_namespace(mcx, namespace_id, relkind)?;
                    objects
                        .try_reserve(objs.len())
                        .map_err(|_| mcx.oom(objs.len() * core::mem::size_of::<Oid>()))?;
                    for oid in objs.drain(..) {
                        objects.push(oid);
                    }
                }
            }
            ObjectType::Sequence => {
                let mut objs = get_relations_in_namespace(mcx, namespace_id, RELKIND_SEQUENCE)?;
                objects
                    .try_reserve(objs.len())
                    .map_err(|_| mcx.oom(objs.len() * core::mem::size_of::<Oid>()))?;
                for oid in objs.drain(..) {
                    objects.push(oid);
                }
            }
            ObjectType::Function | ObjectType::Procedure | ObjectType::Routine => {
                use backend_access_common_scankey::ScanKeyInit;
                use backend_access_index_genam_seams as genam;
                use types_core::fmgr::{F_CHAREQ, F_CHARNE, F_OIDEQ};
                use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

                let rel = table_open(mcx, ProcedureRelationId, AccessShareLock)?;

                let mut key0 = ScanKeyData::empty();
                ScanKeyInit(
                    &mut key0,
                    Anum_pg_proc_pronamespace as i16,
                    BTEqualStrategyNumber,
                    F_OIDEQ,
                    Datum::from_oid(namespace_id),
                )?;

                // OBJECT_FUNCTION includes aggregates and window functions
                // (prokind != PROKIND_PROCEDURE); OBJECT_PROCEDURE filters to
                // prokind == PROKIND_PROCEDURE; OBJECT_ROUTINE takes everything.
                let mut keys: PgVec<ScanKeyData> = mcx::vec_with_capacity_in(mcx, 2)?;
                keys.push(key0);
                if objtype == ObjectType::Function || objtype == ObjectType::Procedure {
                    let mut key1 = ScanKeyData::empty();
                    ScanKeyInit(
                        &mut key1,
                        Anum_pg_proc_prokind as i16,
                        BTEqualStrategyNumber,
                        if objtype == ObjectType::Procedure {
                            F_CHAREQ
                        } else {
                            F_CHARNE
                        },
                        Datum::from_char(PROKIND_PROCEDURE),
                    )?;
                    keys.push(key1);
                }

                // table_beginscan_catalog: heap scan with the keys applied
                // (systable_beginscan with index_ok = false).
                let mut scan =
                    genam::systable_beginscan::call(&rel, ProcedureRelationId, false, None, &keys)?;
                loop {
                    let tuple = genam::systable_getnext::call(mcx, scan.desc_mut())?;
                    let Some(tuple) = tuple else { break };
                    let cols = backend_access_common_heaptuple::heap_deform_tuple(
                        mcx,
                        &tuple.tuple,
                        &rel.rd_att,
                        &tuple.data,
                    )?;
                    let oid = {
                        let (v, n) = &cols[(Anum_pg_proc_oid - 1) as usize];
                        debug_assert!(!*n);
                        v.as_oid()
                    };
                    objects
                        .try_reserve(1)
                        .map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
                    objects.push(oid);
                }
                scan.end()?;
                rel.close(AccessShareLock)?;
            }
            other => {
                return Err(PgError::error(format!(
                    "unrecognized GrantStmt.objtype: {other:?}"
                )));
            }
        }
    }

    Ok(objects)
}

/// `getRelationsInNamespace(namespaceId, relkind)` (aclchk.c) — Oid list of
/// relations in the given namespace filtered by relation kind.
fn get_relations_in_namespace<'mcx>(
    mcx: Mcx<'mcx>,
    namespace_id: Oid,
    relkind: u8,
) -> PgResult<PgVec<'mcx, Oid>> {
    use backend_access_common_scankey::ScanKeyInit;
    use backend_access_index_genam_seams as genam;
    use types_core::fmgr::{F_CHAREQ, F_OIDEQ};
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

    let mut relations: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 0)?;

    let mut key0 = ScanKeyData::empty();
    ScanKeyInit(
        &mut key0,
        Anum_pg_class_relnamespace as i16,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(namespace_id),
    )?;
    let mut key1 = ScanKeyData::empty();
    ScanKeyInit(
        &mut key1,
        Anum_pg_class_relkind as i16,
        BTEqualStrategyNumber,
        F_CHAREQ,
        Datum::from_char(relkind as i8),
    )?;
    let keys = [key0, key1];

    let rel = table_open(mcx, RelationRelationId, AccessShareLock)?;
    // table_beginscan_catalog: heap scan with the keys applied (systable_beginscan
    // with index_ok = false).
    let mut scan = genam::systable_beginscan::call(&rel, RelationRelationId, false, None, &keys)?;
    loop {
        let tuple = genam::systable_getnext::call(mcx, scan.desc_mut())?;
        let Some(tuple) = tuple else { break };
        let cols = backend_access_common_heaptuple::heap_deform_tuple(
            mcx,
            &tuple.tuple,
            &rel.rd_att,
            &tuple.data,
        )?;
        let oid = {
            let (v, n) = &cols[(Anum_pg_class_oid - 1) as usize];
            debug_assert!(!*n);
            v.as_oid()
        };
        relations
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
        relations.push(oid);
    }
    scan.end()?;
    rel.close(AccessShareLock)?;

    Ok(relations)
}

/// The objtype -> (all_privileges mask, errormsg) table from `ExecuteGrantStmt`,
/// restricted to schema in this slice (other types raise on dispatch anyway).
fn objtype_all_privileges(objtype: ObjectType) -> PgResult<(AclMode, &'static str)> {
    match objtype {
        // OBJECT_TABLE: because this might be a sequence, we test both relation
        // and sequence bits, and later do a more limited test when we know the
        // object type.
        ObjectType::Table => Ok((
            ACL_ALL_RIGHTS_RELATION | ACL_ALL_RIGHTS_SEQUENCE,
            "invalid privilege type %s for relation",
        )),
        ObjectType::Sequence => Ok((ACL_ALL_RIGHTS_SEQUENCE, "invalid privilege type %s for sequence")),
        OBJECT_SCHEMA => Ok((ACL_ALL_RIGHTS_SCHEMA, "invalid privilege type %s for schema")),
        ObjectType::Database => {
            Ok((ACL_ALL_RIGHTS_DATABASE, "invalid privilege type %s for database"))
        }
        ObjectType::Domain => Ok((ACL_ALL_RIGHTS_TYPE, "invalid privilege type %s for domain")),
        ObjectType::Type => Ok((ACL_ALL_RIGHTS_TYPE, "invalid privilege type %s for type")),
        ObjectType::Function => Ok((ACL_ALL_RIGHTS_FUNCTION, "invalid privilege type %s for function")),
        ObjectType::Procedure => {
            Ok((ACL_ALL_RIGHTS_FUNCTION, "invalid privilege type %s for procedure"))
        }
        ObjectType::Routine => {
            Ok((ACL_ALL_RIGHTS_FUNCTION, "invalid privilege type %s for routine"))
        }
        ObjectType::Largeobject => Ok((
            ACL_ALL_RIGHTS_LARGEOBJECT,
            "invalid privilege type %s for large object",
        )),
        ObjectType::Language => Ok((
            ACL_ALL_RIGHTS_LANGUAGE,
            "invalid privilege type %s for language",
        )),
        other => Err(PgError::error(format!(
            "GRANT objtype {other:?} not ported (schema/relation slice)"
        ))),
    }
}

/// Convert an owned-tree `rawnodes::RangeVar` to a resolved
/// `types_tuple::access::RangeVar` (precedent: policy/lockcmds
/// `to_access_range_var`).
fn to_access_range_var(rv: &types_nodes::rawnodes::RangeVar<'_>) -> AccessRangeVar {
    AccessRangeVar {
        catalogname: rv.catalogname.as_deref().map(|s| s.into()),
        schemaname: rv.schemaname.as_deref().map(|s| s.into()),
        relname: rv.relname.as_deref().unwrap_or("").into(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

