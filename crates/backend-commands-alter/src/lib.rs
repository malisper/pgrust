//! `backend/commands/alter.c` — drivers for generic ALTER commands
//! (RENAME / SET SCHEMA / OWNER TO / [NO] DEPENDS ON EXTENSION).
//!
//! Faithful branch-for-branch port against the owned node tree. The public
//! command drivers ([`ExecRenameStmt`], [`ExecAlterObjectDependsStmt`],
//! [`ExecAlterObjectSchemaStmt`], [`ExecAlterOwnerStmt`],
//! [`AlterObjectNamespace_oid`], [`AlterObjectOwner_internal`]) and the statics
//! ([`report_name_conflict`], [`report_namespace_conflict`],
//! [`AlterObjectRename_internal`], [`AlterObjectNamespace_internal`]) all live
//! here.
//!
//! The catalog-generic SET SCHEMA / RENAME paths (the `_internal` helpers that
//! operate on an arbitrary simple catalog via `objectaddress` column metadata +
//! a keyed syscache projection) are implemented **in-crate**: the
//! `get_object_catcache_*` / `get_object_attnum_*` metadata block, the keyed
//! `SearchSysCache1` lookup, the permission checks, the duplicate-name
//! friendliness probes, the name/namespace-column `heap_modify_tuple` +
//! `CatalogTupleUpdate`, the `changeDependencyFor` schema edit, and the
//! `InvokeObjectPostAlterHook`. Only the per-type ALTER subroutines the dispatch
//! switches tail-call (RenameRelation, AlterTableNamespace, AlterTypeOwner, …,
//! which live in other command files) cross to their owners; for unported
//! owners those calls panic loudly through the owner's `-seams` crate.
//!
//! The one inexpressible primitive is the generic owner-change tuple write:
//! re-serializing an arbitrary `aclitem[]` varlena back into a modified tuple
//! has no owned-model counterpart at this layer, so
//! [`AlterObjectOwner_internal`]'s tuple write delegates to
//! `backend_catalog_indexing_seams::update_object_owner_tuple` (the same shape
//! as the per-catalog typed owner-tuple writers such as
//! `update_namespace_owner_tuple`). All dispatch, metadata, permission, and
//! dependency logic of the owner path stays in-crate.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use backend_utils_error::ereport;
use mcx::Mcx;
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE, ERROR,
};

use types_acl::{ACLCHECK_OK, ACL_CREATE};
use types_catalog::catalog::{
    COLLATION_RELATION_ID as CollationRelationId, CONVERSION_RELATION_ID as ConversionRelationId,
    DATABASE_RELATION_ID as DatabaseRelationId, EVENT_TRIGGER_RELATION_ID as EventTriggerRelationId,
    FOREIGN_DATA_WRAPPER_RELATION_ID as ForeignDataWrapperRelationId,
    FOREIGN_SERVER_RELATION_ID as ForeignServerRelationId,
    LANGUAGE_RELATION_ID as LanguageRelationId,
    LARGE_OBJECT_METADATA_RELATION_ID as LargeObjectMetadataRelationId,
    LARGE_OBJECT_RELATION_ID as LargeObjectRelationId, NAMESPACE_RELATION_ID as NamespaceRelationId,
    OPERATOR_CLASS_RELATION_ID as OperatorClassRelationId,
    OPERATOR_FAMILY_RELATION_ID as OperatorFamilyRelationId,
    OPERATOR_RELATION_ID as OperatorRelationId, PROCEDURE_RELATION_ID as ProcedureRelationId,
    PUBLICATION_RELATION_ID as PublicationRelationId, RELATION_RELATION_ID as RelationRelationId,
    STATISTIC_EXT_RELATION_ID as StatisticExtRelationId,
    SUBSCRIPTION_RELATION_ID as SubscriptionRelationId, TS_CONFIG_RELATION_ID as TSConfigRelationId,
    TS_DICTIONARY_RELATION_ID as TSDictionaryRelationId, TS_PARSER_RELATION_ID as TSParserRelationId,
    TS_TEMPLATE_RELATION_ID as TSTemplateRelationId, TYPE_RELATION_ID as TypeRelationId,
};
use types_catalog::catalog_dependency::{ObjectAddress, ObjectAddresses, DEPENDENCY_AUTO_EXTENSION};
use types_cache::SysCacheKey;
use types_core::primitive::{InvalidOid, OidIsValid};
use types_core::Oid;
use types_nodes::parsenodes::{
    ObjectType, OBJECT_AGGREGATE, OBJECT_ATTRIBUTE, OBJECT_COLLATION, OBJECT_COLUMN,
    OBJECT_CONVERSION, OBJECT_DATABASE, OBJECT_DOMAIN, OBJECT_DOMCONSTRAINT, OBJECT_EVENT_TRIGGER,
    OBJECT_EXTENSION, OBJECT_FDW, OBJECT_FOREIGN_SERVER, OBJECT_FOREIGN_TABLE, OBJECT_FUNCTION,
    OBJECT_INDEX, OBJECT_LANGUAGE, OBJECT_LARGEOBJECT, OBJECT_MATVIEW, OBJECT_OPCLASS,
    OBJECT_OPERATOR, OBJECT_OPFAMILY, OBJECT_POLICY, OBJECT_PROCEDURE, OBJECT_PUBLICATION,
    OBJECT_ROLE, OBJECT_ROUTINE, OBJECT_RULE, OBJECT_SCHEMA, OBJECT_SEQUENCE, OBJECT_STATISTIC_EXT,
    OBJECT_SUBSCRIPTION, OBJECT_TABCONSTRAINT, OBJECT_TABLE, OBJECT_TABLESPACE, OBJECT_TRIGGER,
    OBJECT_TSCONFIGURATION, OBJECT_TSDICTIONARY, OBJECT_TSPARSER, OBJECT_TSTEMPLATE, OBJECT_TYPE,
    OBJECT_VIEW,
};
use types_parsenodes::{
    AlterObjectDependsStmt, AlterObjectSchemaStmt, AlterOwnerStmt, Node, RenameStmt,
};
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};

use backend_access_common_heaptuple::heap_modify_tuple;
use backend_access_table_table::{table_close, table_open};
use backend_catalog_objectaddress as oa;
use types_storage::lock::{AccessExclusiveLock, NoLock, RowExclusiveLock};

// pg_proc / pg_opclass / pg_opfamily / pg_subscription attribute numbers used
// only by the duplicate-name friendliness probes below (the C reads these off
// `GETSTRUCT(Form_pg_*)`; they are catalog facts).
const Anum_pg_proc_pronargs: i16 = 17;
const Anum_pg_proc_proargtypes: i16 = 20;
const Anum_pg_proc_pronamespace: i16 = 3;
const Anum_pg_proc_proname: i16 = 2;
const Anum_pg_collation_collname: i16 = 2;
const Anum_pg_collation_collnamespace: i16 = 3;
const Anum_pg_opclass_opcname: i16 = 3;
const Anum_pg_opclass_opcnamespace: i16 = 4;
const Anum_pg_opclass_opcmethod: i16 = 2;
const Anum_pg_opfamily_opfname: i16 = 3;
const Anum_pg_opfamily_opfnamespace: i16 = 4;
const Anum_pg_opfamily_opfmethod: i16 = 2;
const Anum_pg_subscription_subpasswordrequired: i16 = 11;
const Anum_pg_publication_oid: i16 = 1;
const Anum_pg_publication_puballtables: i16 = 4;

const InvalidAttrNumber: i16 = 0;

/// Per-class catalog column / catcache metadata block (the C
/// `get_object_catcache_*`/`get_object_attnum_*` reads at the top of the generic
/// update bodies, alter.c:168-172/695-699/930-934). Pure static-table scans over
/// `ObjectProperty[]`.
struct ObjectClassMeta {
    oid_cache_id: i32,
    name_cache_id: i32,
    anum_name: i16,
    anum_namespace: i16,
    anum_owner: i16,
}

fn get_object_class_meta(class_id: Oid) -> PgResult<ObjectClassMeta> {
    Ok(ObjectClassMeta {
        oid_cache_id: oa::properties::get_object_catcache_oid(class_id)?,
        name_cache_id: oa::properties::get_object_catcache_name(class_id)?,
        anum_name: oa::properties::get_object_attnum_name(class_id)?,
        anum_namespace: oa::properties::get_object_attnum_namespace(class_id)?,
        anum_owner: oa::properties::get_object_attnum_owner(class_id)?,
    })
}

/// `ObjectAddressSet(addr, class, object)` (objectaddress.h).
fn object_address_set(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/* ---------------------------------------------------------------------------
 * Node / NameData read helpers — the idiomatic analogue of the C strVal /
 * castNode reads against the polymorphic `Node *object` / `*newowner`, and the
 * GETSTRUCT name/oid reads off a syscache tuple.
 * ------------------------------------------------------------------------- */

/// `strVal(node)`.
fn str_val(node: &Node) -> PgResult<&str> {
    match node.as_string() {
        Some(s) => Ok(s.sval.as_deref().unwrap_or("")),
        None => Err(PgError::error("strVal: String node expected")),
    }
}

/// `strVal(stmt->object)` for the OWNER-TO string-named object kinds.
fn owner_obj_str(stmt: &AlterOwnerStmt) -> PgResult<&str> {
    str_val(
        stmt.object
            .as_deref()
            .ok_or_else(|| PgError::error("ExecAlterOwnerStmt: OWNER TO object must be set"))?,
    )
}

/// `castNode(RoleSpec, node)` view for `get_rolespec_oid` (which reads the
/// `parsenodes::RoleSpec` shape: `roletype` + `rolename`). The
/// `types_parsenodes::RoleSpec` is reprojected into the `types_nodes` view the
/// acl helper consumes (same `roletype`/`rolename`).
fn role_spec_oid(mcx: Mcx<'_>, node: &Node) -> PgResult<Oid> {
    let rs = node
        .as_rolespec()
        .ok_or_else(|| PgError::error("RoleSpec node expected for newowner"))?;
    use types_nodes::parsenodes::RoleSpecType as NRT;
    use types_parsenodes::RoleSpecType as PRT;
    let roletype = match rs.roletype {
        PRT::ROLESPEC_CSTRING => NRT::Cstring,
        PRT::ROLESPEC_CURRENT_ROLE => NRT::CurrentRole,
        PRT::ROLESPEC_CURRENT_USER => NRT::CurrentUser,
        PRT::ROLESPEC_SESSION_USER => NRT::SessionUser,
        PRT::ROLESPEC_PUBLIC => NRT::Public,
    };
    let view = types_nodes::parsenodes::RoleSpec {
        roletype,
        rolename: match &rs.rolename {
            Some(s) => Some(mcx::PgString::from_str_in(s, mcx)?),
            None => None,
        },
    };
    backend_utils_adt_acl::role_membership::get_rolespec_oid(&view, false)
}

/// `DatumGetName(SysCacheGetAttrNotNull(...))` → the `NameStr` text.
fn name_text_of(d: &Datum) -> PgResult<String> {
    match d {
        Datum::ByRef(b) => {
            let len = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            core::str::from_utf8(&b[..len])
                .map(|s| s.to_string())
                .map_err(|_| PgError::error("name column is not valid UTF-8"))
        }
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => Err(PgError::error("name column is by-value")),
    }
}

/// `DatumGetObjectId(SysCacheGetAttrNotNull(...))`.
fn oid_of(d: &Datum) -> Oid {
    d.as_oid()
}

/// `getObjectDescriptionOids(classId, objectId)` for the
/// "must be superuser to rename/set-schema" messages.
fn object_descr(mcx: Mcx<'_>, class_id: Oid, object_id: Oid) -> PgResult<String> {
    Ok(oa::description::get_object_description_oids(mcx, class_id, object_id)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default())
}

/// `get_namespace_name(nspOid)`.
fn namespace_name(mcx: Mcx<'_>, nsp_oid: Oid) -> PgResult<String> {
    Ok(
        backend_utils_cache_lsyscache::namespace_range_index_pubsub::get_namespace_name(mcx, nsp_oid)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default(),
    )
}

/// `aclcheck_error(aclresult, objtype, name)` via the aclchk owner.
fn aclcheck_error(aclerr: types_acl::AclResult, objtype: ObjectType, name: &str) -> PgResult<()> {
    backend_catalog_aclchk::aclcheck_error(aclerr, objtype, Some(name.to_string()))
}

/// `InvokeObjectPostAlterHook(classId, objectId, 0)`.
fn invoke_post_alter_hook(class_id: Oid, object_id: Oid) -> PgResult<()> {
    backend_catalog_objectaccess::invoke_object_post_alter_hook(class_id, object_id, 0, InvalidOid, false)
}

/* ===========================================================================
 * report_name_conflict / report_namespace_conflict (alter.c:75-151)
 * ========================================================================= */

/// `report_name_conflict(classId, name)` (alter.c:75-108).
pub fn report_name_conflict(classId: Oid, name: &str) -> PgResult<()> {
    let msg = if classId == EventTriggerRelationId {
        format!("event trigger \"{name}\" already exists")
    } else if classId == ForeignDataWrapperRelationId {
        format!("foreign-data wrapper \"{name}\" already exists")
    } else if classId == ForeignServerRelationId {
        format!("server \"{name}\" already exists")
    } else if classId == LanguageRelationId {
        format!("language \"{name}\" already exists")
    } else if classId == PublicationRelationId {
        format!("publication \"{name}\" already exists")
    } else if classId == SubscriptionRelationId {
        format!("subscription \"{name}\" already exists")
    } else {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("unsupported object class: {classId}"))
            .into_error());
    };

    Err(ereport(ERROR)
        .errcode(ERRCODE_DUPLICATE_OBJECT)
        .errmsg(msg)
        .into_error())
}

/// `report_namespace_conflict(classId, name, nspOid)` (alter.c:110-151).
pub fn report_namespace_conflict(
    mcx: Mcx<'_>,
    classId: Oid,
    name: &str,
    nspOid: Oid,
) -> PgResult<()> {
    debug_assert!(OidIsValid(nspOid));

    let msg = if classId == ConversionRelationId {
        format!("conversion \"{name}\" already exists in schema \"{}\"", namespace_name(mcx, nspOid)?)
    } else if classId == StatisticExtRelationId {
        format!("statistics object \"{name}\" already exists in schema \"{}\"", namespace_name(mcx, nspOid)?)
    } else if classId == TSParserRelationId {
        format!("text search parser \"{name}\" already exists in schema \"{}\"", namespace_name(mcx, nspOid)?)
    } else if classId == TSDictionaryRelationId {
        format!("text search dictionary \"{name}\" already exists in schema \"{}\"", namespace_name(mcx, nspOid)?)
    } else if classId == TSTemplateRelationId {
        format!("text search template \"{name}\" already exists in schema \"{}\"", namespace_name(mcx, nspOid)?)
    } else if classId == TSConfigRelationId {
        format!("text search configuration \"{name}\" already exists in schema \"{}\"", namespace_name(mcx, nspOid)?)
    } else {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("unsupported object class: {classId}"))
            .into_error());
    };

    Err(ereport(ERROR)
        .errcode(ERRCODE_DUPLICATE_OBJECT)
        .errmsg(msg)
        .into_error())
}

/* ---------------------------------------------------------------------------
 * Shared syscache helpers for the generic update bodies.
 * ------------------------------------------------------------------------- */

fn search_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    object_id: Oid,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    backend_utils_cache_syscache::SearchSysCache1(
        mcx,
        cache_id,
        SysCacheKey::Value(types_datum::Datum::from_oid(object_id)),
    )
}

fn getattr_notnull<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    tup: &FormedTuple<'mcx>,
    anum: i16,
) -> PgResult<Datum<'mcx>> {
    backend_utils_cache_syscache::SysCacheGetAttrNotNull(mcx, cache_id, tup, anum as i32)
}

/// `SearchSysCacheExists2(nameCacheId, name, nspOid)` — name-and-namespace
/// existence probe (the C name-uniqueness friendliness check).
fn name_nsp_exists(
    mcx: Mcx<'_>,
    name_cache_id: i32,
    name: &str,
    nsp_oid: Oid,
) -> PgResult<bool> {
    backend_utils_cache_syscache::SearchSysCacheExists(
        mcx,
        name_cache_id,
        SysCacheKey::Str(name),
        SysCacheKey::Value(types_datum::Datum::from_oid(nsp_oid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

/// `SearchSysCacheExists1(nameCacheId, name)`.
fn name_exists(mcx: Mcx<'_>, name_cache_id: i32, name: &str) -> PgResult<bool> {
    backend_utils_cache_syscache::SearchSysCacheExists(
        mcx,
        name_cache_id,
        SysCacheKey::Str(name),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

/// `SearchSysCacheExists2(SUBSCRIPTIONNAME, MyDatabaseId, new_name)`.
fn subscription_name_exists(mcx: Mcx<'_>, my_database_id: Oid, name: &str) -> PgResult<bool> {
    use backend_utils_cache_syscache::cacheinfo::SUBSCRIPTIONNAME;
    backend_utils_cache_syscache::SearchSysCacheExists(
        mcx,
        SUBSCRIPTIONNAME,
        SysCacheKey::Value(types_datum::Datum::from_oid(my_database_id)),
        SysCacheKey::Str(name),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

/// The shared duplicate-name friendliness probes (alter.c:266-324 for RENAME,
/// 770-802 for SET SCHEMA). `name` is the candidate name, `nsp_oid` the
/// namespace it would live in.
fn check_duplicate_name(
    mcx: Mcx<'_>,
    class_id: Oid,
    cache_id: i32,
    name_cache_id: i32,
    tup: &FormedTuple<'_>,
    new_name: &str,
    nsp_oid: Oid,
    my_database_id: Oid,
) -> PgResult<()> {
    if class_id == ProcedureRelationId {
        let pronargs = getattr_notnull(mcx, cache_id, tup, Anum_pg_proc_pronargs)?.as_i16() as i32;
        let proargtypes_d = getattr_notnull(mcx, cache_id, tup, Anum_pg_proc_proargtypes)?;
        let proargtypes = backend_utils_adt_arrayfuncs_seams::oidvector_to_oids_bytes::call(
            mcx,
            proargtypes_d.as_ref_bytes(),
        )?;
        let pronamespace = oid_of(&getattr_notnull(mcx, cache_id, tup, Anum_pg_proc_pronamespace)?);
        backend_commands_functioncmds::IsThereFunctionInNamespace(
            new_name,
            pronargs,
            &proargtypes,
            pronamespace,
        )?;
    } else if class_id == CollationRelationId {
        let collnamespace =
            oid_of(&getattr_notnull(mcx, cache_id, tup, Anum_pg_collation_collnamespace)?);
        backend_commands_collationcmds::IsThereCollationInNamespace(new_name, collnamespace)?;
    } else if class_id == OperatorClassRelationId {
        let opcmethod = oid_of(&getattr_notnull(mcx, cache_id, tup, Anum_pg_opclass_opcmethod)?);
        let opcnamespace =
            oid_of(&getattr_notnull(mcx, cache_id, tup, Anum_pg_opclass_opcnamespace)?);
        backend_commands_opclasscmds::IsThereOpClassInNamespace(
            mcx, new_name, opcmethod, opcnamespace,
        )?;
    } else if class_id == OperatorFamilyRelationId {
        let opfmethod = oid_of(&getattr_notnull(mcx, cache_id, tup, Anum_pg_opfamily_opfmethod)?);
        let opfnamespace =
            oid_of(&getattr_notnull(mcx, cache_id, tup, Anum_pg_opfamily_opfnamespace)?);
        backend_commands_opclasscmds::IsThereOpFamilyInNamespace(
            mcx, new_name, opfmethod, opfnamespace,
        )?;
    } else if class_id == SubscriptionRelationId {
        if subscription_name_exists(mcx, my_database_id, new_name)? {
            report_name_conflict(class_id, new_name)?;
        }
        // Wake up related replication workers to handle this change quickly.
        // (objectId is handled by the caller; see AlterObjectRename_internal.)
    } else if name_cache_id >= 0 {
        if OidIsValid(nsp_oid) {
            if name_nsp_exists(mcx, name_cache_id, new_name, nsp_oid)? {
                report_namespace_conflict(mcx, class_id, new_name, nsp_oid)?;
            }
        } else if name_exists(mcx, name_cache_id, new_name)? {
            report_name_conflict(class_id, new_name)?;
        }
    }
    Ok(())
}

/* ===========================================================================
 * AlterObjectRename_internal (alter.c:164-364)
 * ========================================================================= */

/// `AlterObjectRename_internal(rel, objectId, new_name)` (alter.c:164-364).
fn AlterObjectRename_internal(
    mcx: Mcx<'_>,
    rel: &types_rel::Relation<'_>,
    class_id: Oid,
    objectId: Oid,
    new_name: &str,
) -> PgResult<()> {
    let meta = get_object_class_meta(class_id)?;
    let my_database_id = backend_commands_tablespace_globals_seams::MyDatabaseId::call()?;

    let Some(oldtup) = search_by_oid(mcx, meta.oid_cache_id, objectId)? else {
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "cache lookup failed for object {objectId} of catalog {class_id}"
            ))
            .into_error());
    };

    let datum = getattr_notnull(mcx, meta.oid_cache_id, &oldtup, meta.anum_name)?;
    let old_name = name_text_of(&datum)?;

    // Get OID of namespace.
    let namespaceId = if meta.anum_namespace > 0 {
        oid_of(&getattr_notnull(mcx, meta.oid_cache_id, &oldtup, meta.anum_namespace)?)
    } else {
        InvalidOid
    };

    // Permission checks ... superusers can always do it.
    if !backend_utils_misc_more::superuser::superuser()? {
        // Fail if object does not have an explicit owner.
        if meta.anum_owner <= 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "must be superuser to rename {}",
                    object_descr(mcx, class_id, objectId)?
                ))
                .into_error());
        }

        // Otherwise, must be owner of the existing object.
        let ownerId = oid_of(&getattr_notnull(mcx, meta.oid_cache_id, &oldtup, meta.anum_owner)?);
        if !backend_utils_adt_acl::role_membership::has_privs_of_role(
            backend_utils_init_miscinit::GetUserId(),
            ownerId,
        )? {
            aclcheck_error(
                types_acl::ACLCHECK_NOT_OWNER,
                oa::properties::get_object_type(class_id, objectId)?,
                &old_name,
            )?;
        }

        // User must have CREATE privilege on the namespace.
        if OidIsValid(namespaceId) {
            let aclresult = backend_catalog_aclchk::object_aclcheck(
                mcx,
                NamespaceRelationId,
                namespaceId,
                backend_utils_init_miscinit::GetUserId(),
                ACL_CREATE,
            )?;
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, OBJECT_SCHEMA, &namespace_name(mcx, namespaceId)?)?;
            }
        }

        if class_id == SubscriptionRelationId {
            // must have CREATE privilege on database
            let aclresult = backend_catalog_aclchk::object_aclcheck(
                mcx,
                DatabaseRelationId,
                my_database_id,
                backend_utils_init_miscinit::GetUserId(),
                ACL_CREATE,
            )?;
            if aclresult != ACLCHECK_OK {
                let dbname =
                    backend_commands_dbcommands_seams::get_database_name::call(mcx, my_database_id)?;
                aclcheck_error(aclresult, OBJECT_DATABASE, dbname.as_deref().unwrap_or(""))?;
            }

            // Don't allow non-superuser modification of a subscription with
            // password_required=false.
            let subpasswordrequired = getattr_notnull(
                mcx,
                meta.oid_cache_id,
                &oldtup,
                Anum_pg_subscription_subpasswordrequired,
            )?
            .as_bool();
            if !subpasswordrequired && !backend_utils_misc_more::superuser::superuser()? {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg("password_required=false is superuser-only")
                    .errhint("Subscriptions with the password_required option set to false may only be created or modified by the superuser.")
                    .into_error());
            }
        }
    }

    // Check for duplicate name (more friendly than unique-index failure).
    check_duplicate_name(
        mcx,
        class_id,
        meta.oid_cache_id,
        meta.name_cache_id,
        &oldtup,
        new_name,
        namespaceId,
        my_database_id,
    )?;

    // The subscription path also wakes the replication workers.
    if class_id == SubscriptionRelationId {
        backend_replication_logical_worker_seams::LogicalRepWorkersWakeupAtCommit::call(objectId)?;
    }

    // Build modified tuple — replace the name column.
    let natts = rel.rd_att.natts as usize;
    let mut values: Vec<Datum> = vec![Datum::null(); natts];
    let nulls: Vec<bool> = vec![false; natts];
    let mut replaces: Vec<bool> = vec![false; natts];
    let mut nameattr = types_tuple::heaptuple::NameData::default();
    nameattr.namestrcpy(new_name);
    values[(meta.anum_name - 1) as usize] =
        Datum::ByRef(mcx::slice_in(mcx, &nameattr.data)?);
    replaces[(meta.anum_name - 1) as usize] = true;
    let mut newtup = heap_modify_tuple(mcx, &oldtup, &rel.rd_att, &values, &nulls, &replaces)?;

    // Perform actual update.
    let otid = oldtup.tuple.t_self;
    backend_catalog_indexing::keystone::CatalogTupleUpdate(mcx, rel, otid, &mut newtup)?;

    invoke_post_alter_hook(class_id, objectId)?;

    // Do post catalog-update tasks.
    if class_id == PublicationRelationId {
        // Invalidate relsynccache entries for the renamed publication.
        let puboid =
            oid_of(&getattr_notnull(mcx, meta.oid_cache_id, &oldtup, Anum_pg_publication_oid)?);
        let puballtables = getattr_notnull(
            mcx,
            meta.oid_cache_id,
            &oldtup,
            Anum_pg_publication_puballtables,
        )?
        .as_bool();
        backend_commands_publicationcmds_seams::InvalidatePubRelSyncCache::call(puboid, puballtables)?;
    }

    Ok(())
}

/* ===========================================================================
 * ExecRenameStmt (alter.c:372-461)
 * ========================================================================= */

/// `ExecRenameStmt(stmt)` (alter.c:372-461).
pub fn ExecRenameStmt(mcx: Mcx<'_>, stmt: &RenameStmt) -> PgResult<ObjectAddress> {
    match stmt.renameType {
        OBJECT_TABCONSTRAINT | OBJECT_DOMCONSTRAINT => {
            backend_commands_tablecmds_seams::RenameConstraint::call(mcx, stmt)
        }

        OBJECT_DATABASE => backend_commands_dbcommands_seams::RenameDatabase::call(
            stmt.subname.as_deref().unwrap_or(""),
            stmt.newname.as_deref().unwrap_or(""),
        ),

        OBJECT_ROLE => backend_commands_user::RenameRole(
            mcx,
            stmt.subname.as_deref().unwrap_or(""),
            stmt.newname.as_deref().unwrap_or(""),
        ),

        OBJECT_SCHEMA => backend_commands_schemacmds::RenameSchema(
            mcx,
            stmt.subname.as_deref().unwrap_or(""),
            stmt.newname.as_deref().unwrap_or(""),
        ),

        OBJECT_TABLESPACE => backend_commands_tablespace::RenameTableSpace(
            mcx,
            stmt.subname.as_deref().unwrap_or(""),
            stmt.newname.as_deref().unwrap_or(""),
        ),

        OBJECT_TABLE | OBJECT_SEQUENCE | OBJECT_VIEW | OBJECT_MATVIEW | OBJECT_INDEX
        | OBJECT_FOREIGN_TABLE => backend_commands_tablecmds_seams::RenameRelation::call(mcx, stmt),

        OBJECT_COLUMN | OBJECT_ATTRIBUTE => {
            backend_commands_tablecmds_seams::renameatt::call(mcx, stmt)
        }

        OBJECT_RULE => {
            let relation = stmt.relation.as_ref().ok_or_else(|| {
                PgError::error("ExecRenameStmt: RULE rename requires a relation")
            })?;
            backend_rewrite_rewriteDefine::RenameRewriteRule(
                mcx,
                relation,
                stmt.subname.as_deref().unwrap_or(""),
                stmt.newname.as_deref().unwrap_or(""),
            )
        }

        OBJECT_TRIGGER => backend_commands_trigger_seams::renametrig::call(mcx, stmt),

        OBJECT_POLICY => backend_commands_policy_seams::rename_policy::call(mcx, stmt),

        OBJECT_DOMAIN | OBJECT_TYPE => backend_commands_typecmds_seams::RenameType::call(mcx, stmt),

        OBJECT_AGGREGATE | OBJECT_COLLATION | OBJECT_CONVERSION | OBJECT_EVENT_TRIGGER
        | OBJECT_FDW | OBJECT_FOREIGN_SERVER | OBJECT_FUNCTION | OBJECT_OPCLASS | OBJECT_OPFAMILY
        | OBJECT_LANGUAGE | OBJECT_PROCEDURE | OBJECT_ROUTINE | OBJECT_STATISTIC_EXT
        | OBJECT_TSCONFIGURATION | OBJECT_TSDICTIONARY | OBJECT_TSPARSER | OBJECT_TSTEMPLATE
        | OBJECT_PUBLICATION | OBJECT_SUBSCRIPTION => {
            let object = stmt.object.as_deref().ok_or_else(|| {
                PgError::error("ExecRenameStmt: object must be set for the generic rename path")
            })?;
            let resolved =
                oa::resolve::get_object_address(mcx, stmt.renameType, object, AccessExclusiveLock, false)?;
            let address = resolved.address;

            let catalog = table_open(mcx, address.classId, RowExclusiveLock)?;
            AlterObjectRename_internal(
                mcx,
                &catalog,
                address.classId,
                address.objectId,
                stmt.newname.as_deref().unwrap_or(""),
            )?;
            table_close(catalog, RowExclusiveLock)?;

            Ok(address)
        }

        other => Err(ereport(ERROR)
            .errmsg_internal(format!("unrecognized rename stmt type: {}", other as i32))
            .into_error()),
    }
}

/* ===========================================================================
 * ExecAlterObjectDependsStmt (alter.c:470-522)
 * ========================================================================= */

/// `ExecAlterObjectDependsStmt(stmt, refAddress)` (alter.c:470-522).
pub fn ExecAlterObjectDependsStmt(
    mcx: Mcx<'_>,
    stmt: &AlterObjectDependsStmt,
    refAddress: Option<&mut ObjectAddress>,
) -> PgResult<ObjectAddress> {
    let object = stmt
        .object
        .as_deref()
        .ok_or_else(|| PgError::error("ExecAlterObjectDependsStmt: object must be set"))?;
    let resolved = oa::resolve::get_object_address_rv(
        mcx,
        stmt.objectType,
        stmt.relation.as_ref(),
        object,
        AccessExclusiveLock,
        false,
    )?;
    let address = resolved.address;
    let rel = resolved.relation;

    // Verify that the user is entitled to run the command.
    backend_catalog_objectaddress::resolve::check_object_ownership(
        backend_utils_init_miscinit::GetUserId(),
        stmt.objectType,
        address,
        object,
        rel.as_ref(),
    )?;

    // If a relation was involved, it was opened and locked; we keep the lock
    // until commit but release the relcache reference here.
    if let Some(rel) = rel {
        table_close(rel, NoLock)?;
    }

    let extname = stmt
        .extname
        .as_deref()
        .ok_or_else(|| PgError::error("ExecAlterObjectDependsStmt: extname must be set"))?;
    let refAddr =
        oa::resolve::get_object_address(mcx, OBJECT_EXTENSION, extname, AccessExclusiveLock, false)?.address;
    if let Some(refAddress) = refAddress {
        *refAddress = refAddr;
    }

    if stmt.remove {
        backend_catalog_pg_depend::deleteDependencyRecordsForSpecific(
            address.classId,
            address.objectId,
            DEPENDENCY_AUTO_EXTENSION.as_char(),
            refAddr.classId,
            refAddr.objectId,
        )?;
    } else {
        // Avoid duplicates.
        let currexts =
            backend_catalog_pg_depend::getAutoExtensionsOfObject(mcx, address.classId, address.objectId)?;
        if !currexts.iter().any(|&o| o == refAddr.objectId) {
            backend_catalog_pg_depend::recordDependencyOn(mcx, &address, &refAddr, DEPENDENCY_AUTO_EXTENSION)?;
        }
    }

    Ok(address)
}

/* ===========================================================================
 * ExecAlterObjectSchemaStmt (alter.c:533-608)
 * ========================================================================= */

/// `ExecAlterObjectSchemaStmt(stmt, oldSchemaAddr)` (alter.c:533-608).
pub fn ExecAlterObjectSchemaStmt(
    mcx: Mcx<'_>,
    stmt: &AlterObjectSchemaStmt,
    oldSchemaAddr: Option<&mut ObjectAddress>,
) -> PgResult<ObjectAddress> {
    let want_old = oldSchemaAddr.is_some();
    let address;
    let oldNspOid: Oid;
    let newschema = stmt.newschema.as_deref().unwrap_or("");

    match stmt.objectType {
        OBJECT_EXTENSION => {
            let extname = str_val(stmt.object.as_deref().ok_or_else(|| {
                PgError::error("ExecAlterObjectSchemaStmt: object must be set for EXTENSION")
            })?)?;
            let (addr, old) = backend_commands_extension_seams::AlterExtensionNamespace::call(
                extname, newschema, want_old,
            )?;
            address = addr;
            oldNspOid = old;
        }

        OBJECT_FOREIGN_TABLE | OBJECT_SEQUENCE | OBJECT_TABLE | OBJECT_VIEW | OBJECT_MATVIEW => {
            let (addr, old) =
                backend_commands_tablecmds_seams::AlterTableNamespace::call(mcx, stmt, want_old)?;
            address = addr;
            oldNspOid = old;
        }

        OBJECT_DOMAIN | OBJECT_TYPE => {
            let names = stmt.object.as_deref().ok_or_else(|| {
                PgError::error("ExecAlterObjectSchemaStmt: object must be set for DOMAIN/TYPE")
            })?;
            let (addr, old) = backend_commands_typecmds_seams::AlterTypeNamespace::call(
                mcx,
                names,
                newschema,
                stmt.objectType,
                want_old,
            )?;
            address = addr;
            oldNspOid = old;
        }

        // generic code path
        OBJECT_AGGREGATE | OBJECT_COLLATION | OBJECT_CONVERSION | OBJECT_FUNCTION
        | OBJECT_OPERATOR | OBJECT_OPCLASS | OBJECT_OPFAMILY | OBJECT_PROCEDURE | OBJECT_ROUTINE
        | OBJECT_STATISTIC_EXT | OBJECT_TSCONFIGURATION | OBJECT_TSDICTIONARY | OBJECT_TSPARSER
        | OBJECT_TSTEMPLATE => {
            let object = stmt.object.as_deref().ok_or_else(|| {
                PgError::error("ExecAlterObjectSchemaStmt: object must be set for the generic path")
            })?;
            let resolved =
                oa::resolve::get_object_address(mcx, stmt.objectType, object, AccessExclusiveLock, false)?;
            address = resolved.address;
            let classId = address.classId;
            let catalog = table_open(mcx, classId, RowExclusiveLock)?;
            let nspOid = backend_catalog_namespace::LookupCreationNamespace(mcx, newschema)?;
            oldNspOid =
                AlterObjectNamespace_internal(mcx, &catalog, classId, address.objectId, nspOid)?;
            table_close(catalog, RowExclusiveLock)?;
        }

        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "unrecognized AlterObjectSchemaStmt type: {}",
                    other as i32
                ))
                .into_error());
        }
    }

    if let Some(oldSchemaAddr) = oldSchemaAddr {
        *oldSchemaAddr = object_address_set(NamespaceRelationId, oldNspOid);
    }

    Ok(address)
}

/* ===========================================================================
 * AlterObjectNamespace_oid (alter.c:624-678)
 * ========================================================================= */

/// `AlterObjectNamespace_oid(classId, objid, nspOid, objsMoved)`
/// (alter.c:624-678).
pub fn AlterObjectNamespace_oid(
    mcx: Mcx<'_>,
    classId: Oid,
    objid: Oid,
    nspOid: Oid,
    objsMoved: &mut ObjectAddresses,
) -> PgResult<Oid> {
    let mut oldNspOid: Oid = InvalidOid;

    if classId == RelationRelationId {
        let rel = backend_access_common_relation::relation_open(mcx, objid, AccessExclusiveLock)?;
        oldNspOid = rel.rd_rel.relnamespace;
        backend_commands_tablecmds_seams::AlterTableNamespaceInternal::call(
            mcx, &rel, oldNspOid, nspOid, objsMoved,
        )?;
        table_close(rel, NoLock)?;
    } else if classId == TypeRelationId {
        oldNspOid =
            backend_commands_typecmds_seams::AlterTypeNamespace_oid::call(objid, nspOid, true, objsMoved)?;
    } else if classId == ProcedureRelationId
        || classId == CollationRelationId
        || classId == ConversionRelationId
        || classId == OperatorRelationId
        || classId == OperatorClassRelationId
        || classId == OperatorFamilyRelationId
        || classId == StatisticExtRelationId
        || classId == TSParserRelationId
        || classId == TSDictionaryRelationId
        || classId == TSTemplateRelationId
        || classId == TSConfigRelationId
    {
        let catalog = table_open(mcx, classId, RowExclusiveLock)?;
        oldNspOid = AlterObjectNamespace_internal(mcx, &catalog, classId, objid, nspOid)?;
        table_close(catalog, RowExclusiveLock)?;
    } else {
        // ignore object types that don't have schema-qualified names
        debug_assert_eq!(
            get_object_class_meta(classId).map(|m| m.anum_namespace).unwrap_or(0),
            InvalidAttrNumber
        );
    }

    Ok(oldNspOid)
}

/* ===========================================================================
 * AlterObjectNamespace_internal (alter.c:691-830)
 * ========================================================================= */

/// `AlterObjectNamespace_internal(rel, objid, nspOid)` (alter.c:691-830).
fn AlterObjectNamespace_internal(
    mcx: Mcx<'_>,
    rel: &types_rel::Relation<'_>,
    class_id: Oid,
    objid: Oid,
    nspOid: Oid,
) -> PgResult<Oid> {
    let meta = get_object_class_meta(class_id)?;
    let my_database_id = backend_commands_tablespace_globals_seams::MyDatabaseId::call()?;

    let Some(tup) = search_by_oid(mcx, meta.oid_cache_id, objid)? else {
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "cache lookup failed for object {objid} of catalog {class_id}"
            ))
            .into_error());
    };

    let name = getattr_notnull(mcx, meta.oid_cache_id, &tup, meta.anum_name)?;
    let oldNspOid = oid_of(&getattr_notnull(mcx, meta.oid_cache_id, &tup, meta.anum_namespace)?);

    // If the object is already in the correct namespace, only fire the hook.
    if oldNspOid == nspOid {
        invoke_post_alter_hook(class_id, objid)?;
        return Ok(oldNspOid);
    }

    // Check basic namespace related issues.
    backend_catalog_namespace::CheckSetNamespace(mcx, oldNspOid, nspOid)?;

    // Permission checks ... superusers can always do it.
    if !backend_utils_misc_more::superuser::superuser()? {
        // Fail if object does not have an explicit owner.
        if meta.anum_owner <= 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "must be superuser to set schema of {}",
                    object_descr(mcx, class_id, objid)?
                ))
                .into_error());
        }

        // Otherwise, must be owner of the existing object.
        let ownerId = oid_of(&getattr_notnull(mcx, meta.oid_cache_id, &tup, meta.anum_owner)?);
        if !backend_utils_adt_acl::role_membership::has_privs_of_role(
            backend_utils_init_miscinit::GetUserId(),
            ownerId,
        )? {
            aclcheck_error(
                types_acl::ACLCHECK_NOT_OWNER,
                oa::properties::get_object_type(class_id, objid)?,
                &name_text_of(&name)?,
            )?;
        }

        // User must have CREATE privilege on new namespace.
        let aclresult = backend_catalog_aclchk::object_aclcheck(
            mcx,
            NamespaceRelationId,
            nspOid,
            backend_utils_init_miscinit::GetUserId(),
            ACL_CREATE,
        )?;
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_SCHEMA, &namespace_name(mcx, nspOid)?)?;
        }
    }

    // Check for duplicate name in the destination namespace.
    if class_id == ProcedureRelationId {
        let proname = name_text_of(&getattr_notnull(mcx, meta.oid_cache_id, &tup, Anum_pg_proc_proname)?)?;
        let pronargs = getattr_notnull(mcx, meta.oid_cache_id, &tup, Anum_pg_proc_pronargs)?.as_i16() as i32;
        let proargtypes_d = getattr_notnull(mcx, meta.oid_cache_id, &tup, Anum_pg_proc_proargtypes)?;
        let proargtypes = backend_utils_adt_arrayfuncs_seams::oidvector_to_oids_bytes::call(
            mcx,
            proargtypes_d.as_ref_bytes(),
        )?;
        backend_commands_functioncmds::IsThereFunctionInNamespace(
            &proname, pronargs, &proargtypes, nspOid,
        )?;
    } else if class_id == CollationRelationId {
        let collname = name_text_of(&getattr_notnull(mcx, meta.oid_cache_id, &tup, Anum_pg_collation_collname)?)?;
        backend_commands_collationcmds::IsThereCollationInNamespace(&collname, nspOid)?;
    } else if class_id == OperatorClassRelationId {
        let opcname = name_text_of(&getattr_notnull(mcx, meta.oid_cache_id, &tup, Anum_pg_opclass_opcname)?)?;
        let opcmethod = oid_of(&getattr_notnull(mcx, meta.oid_cache_id, &tup, Anum_pg_opclass_opcmethod)?);
        backend_commands_opclasscmds::IsThereOpClassInNamespace(mcx, &opcname, opcmethod, nspOid)?;
    } else if class_id == OperatorFamilyRelationId {
        let opfname = name_text_of(&getattr_notnull(mcx, meta.oid_cache_id, &tup, Anum_pg_opfamily_opfname)?)?;
        let opfmethod = oid_of(&getattr_notnull(mcx, meta.oid_cache_id, &tup, Anum_pg_opfamily_opfmethod)?);
        backend_commands_opclasscmds::IsThereOpFamilyInNamespace(mcx, &opfname, opfmethod, nspOid)?;
    } else if meta.name_cache_id >= 0
        && name_nsp_exists(mcx, meta.name_cache_id, &name_text_of(&name)?, nspOid)?
    {
        report_namespace_conflict(mcx, class_id, &name_text_of(&name)?, nspOid)?;
    }
    let _ = my_database_id;

    // Build modified tuple — replace the namespace column.
    let natts = rel.rd_att.natts as usize;
    let mut values: Vec<Datum> = vec![Datum::null(); natts];
    let nulls: Vec<bool> = vec![false; natts];
    let mut replaces: Vec<bool> = vec![false; natts];
    values[(meta.anum_namespace - 1) as usize] = Datum::from_oid(nspOid);
    replaces[(meta.anum_namespace - 1) as usize] = true;
    let mut newtup = heap_modify_tuple(mcx, &tup, &rel.rd_att, &values, &nulls, &replaces)?;

    // Perform actual update.
    let otid = tup.tuple.t_self;
    backend_catalog_indexing::keystone::CatalogTupleUpdate(mcx, rel, otid, &mut newtup)?;

    // Update dependency to point to the new schema.
    if backend_catalog_pg_depend::changeDependencyFor(
        mcx,
        class_id,
        objid,
        NamespaceRelationId,
        oldNspOid,
        nspOid,
    )? != 1
    {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("could not change schema dependency for object {objid}"))
            .into_error());
    }

    invoke_post_alter_hook(class_id, objid)?;

    Ok(oldNspOid)
}

/* ===========================================================================
 * ExecAlterOwnerStmt (alter.c:836-911)
 * ========================================================================= */

/// `ExecAlterOwnerStmt(stmt)` (alter.c:836-911).
pub fn ExecAlterOwnerStmt(mcx: Mcx<'_>, stmt: &AlterOwnerStmt) -> PgResult<ObjectAddress> {
    let newowner_node = stmt
        .newowner
        .as_deref()
        .ok_or_else(|| PgError::error("ExecAlterOwnerStmt: newowner must be set"))?;
    let newowner = role_spec_oid(mcx, newowner_node)?;

    match stmt.objectType {
        OBJECT_DATABASE => {
            backend_commands_dbcommands_seams::AlterDatabaseOwner::call(owner_obj_str(stmt)?, newowner)
        }

        OBJECT_SCHEMA => backend_commands_schemacmds::AlterSchemaOwner(mcx, owner_obj_str(stmt)?, newowner),

        OBJECT_TYPE | OBJECT_DOMAIN => {
            let names = stmt.object.as_deref().ok_or_else(|| {
                PgError::error("ExecAlterOwnerStmt: OWNER TO object must be set")
            })?;
            backend_commands_typecmds_seams::AlterTypeOwner::call(mcx, names, newowner, stmt.objectType)
        }

        OBJECT_FDW => backend_commands_foreigncmds::AlterForeignDataWrapperOwner(
            mcx,
            owner_obj_str(stmt)?,
            newowner,
        ),

        OBJECT_FOREIGN_SERVER => {
            backend_commands_foreigncmds::AlterForeignServerOwner(mcx, owner_obj_str(stmt)?, newowner)
        }

        OBJECT_EVENT_TRIGGER => {
            backend_commands_event_trigger_seams::AlterEventTriggerOwner::call(owner_obj_str(stmt)?, newowner)
        }

        OBJECT_PUBLICATION => {
            backend_commands_publicationcmds_seams::AlterPublicationOwner::call(owner_obj_str(stmt)?, newowner)
        }

        OBJECT_SUBSCRIPTION => {
            backend_commands_subscriptioncmds_seams::AlterSubscriptionOwner::call(owner_obj_str(stmt)?, newowner)
        }

        // Generic cases
        OBJECT_AGGREGATE | OBJECT_COLLATION | OBJECT_CONVERSION | OBJECT_FUNCTION | OBJECT_LANGUAGE
        | OBJECT_LARGEOBJECT | OBJECT_OPERATOR | OBJECT_OPCLASS | OBJECT_OPFAMILY | OBJECT_PROCEDURE
        | OBJECT_ROUTINE | OBJECT_STATISTIC_EXT | OBJECT_TABLESPACE | OBJECT_TSDICTIONARY
        | OBJECT_TSCONFIGURATION => {
            let object = stmt.object.as_deref().ok_or_else(|| {
                PgError::error("ExecAlterOwnerStmt: object must be set for the generic path")
            })?;
            let address =
                oa::resolve::get_object_address(mcx, stmt.objectType, object, AccessExclusiveLock, false)?
                    .address;

            AlterObjectOwner_internal(mcx, address.classId, address.objectId, newowner)?;

            Ok(address)
        }

        other => Err(ereport(ERROR)
            .errmsg_internal(format!("unrecognized AlterOwnerStmt type: {}", other as i32))
            .into_error()),
    }
}

/* ===========================================================================
 * AlterObjectOwner_internal (alter.c:925-1063)
 * ========================================================================= */

/// `AlterObjectOwner_internal(classId, objectId, new_ownerId)`
/// (alter.c:925-1063).
pub fn AlterObjectOwner_internal(
    mcx: Mcx<'_>,
    classId: Oid,
    objectId: Oid,
    new_ownerId: Oid,
) -> PgResult<()> {
    // For large objects, the catalog to modify is pg_largeobject_metadata.
    let catalogId = if classId == LargeObjectRelationId {
        LargeObjectMetadataRelationId
    } else {
        classId
    };
    let anum_oid = oa::properties::get_object_attnum_oid(catalogId)?;
    let anum_owner = oa::properties::get_object_attnum_owner(catalogId)?;
    let anum_namespace = oa::properties::get_object_attnum_namespace(catalogId)?;
    let anum_acl = oa::properties::get_object_attnum_acl(catalogId)?;
    let anum_name = oa::properties::get_object_attnum_name(catalogId)?;

    let rel = table_open(mcx, catalogId, RowExclusiveLock)?;

    // Search tuple and lock it.
    let Some(oldtup) =
        oa::resolve::get_catalog_object_by_oid_extended(mcx, &rel, anum_oid, objectId, true)?
    else {
        table_close(rel, RowExclusiveLock)?;
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "cache lookup failed for object {objectId} of catalog {catalogId}"
            ))
            .into_error());
    };

    // The descriptor of the relation we opened (used to read columns by attnum).
    let old_ownerId = read_attr_oid(mcx, &rel, &oldtup, anum_owner)?;

    let namespaceId = if anum_namespace != InvalidAttrNumber {
        read_attr_oid(mcx, &rel, &oldtup, anum_namespace)?
    } else {
        InvalidOid
    };

    if old_ownerId != new_ownerId {
        // Superusers can bypass permission checks.
        if !backend_utils_misc_more::superuser::superuser()? {
            // must be owner
            if !backend_utils_adt_acl::role_membership::has_privs_of_role(
                backend_utils_init_miscinit::GetUserId(),
                old_ownerId,
            )? {
                let objname = if anum_name != InvalidAttrNumber {
                    name_text_of(&read_attr(mcx, &rel, &oldtup, anum_name)?)?
                } else {
                    format!("{objectId}")
                };
                aclcheck_error(
                    types_acl::ACLCHECK_NOT_OWNER,
                    oa::properties::get_object_type(catalogId, objectId)?,
                    &objname,
                )?;
            }
            // Must be able to become new owner.
            backend_utils_adt_acl::role_membership::check_can_set_role(
                backend_utils_init_miscinit::GetUserId(),
                new_ownerId,
            )?;

            // New owner must have CREATE privilege on namespace.
            if OidIsValid(namespaceId) {
                let aclresult = backend_catalog_aclchk::object_aclcheck(
                    mcx,
                    NamespaceRelationId,
                    namespaceId,
                    new_ownerId,
                    ACL_CREATE,
                )?;
                if aclresult != ACLCHECK_OK {
                    aclcheck_error(aclresult, OBJECT_SCHEMA, &namespace_name(mcx, namespaceId)?)?;
                }
            }
        }

        // Build the modified tuple (owner + recomputed ACL) and CatalogTupleUpdate
        // + UnlockTuple. The generic aclitem[] varlena re-serialization into the
        // tuple is the unported primitive encapsulated by this seam (mirroring
        // the per-catalog typed owner-tuple writers).
        backend_catalog_indexing_seams::update_object_owner_tuple::call(
            &rel, anum_oid, objectId, anum_owner, anum_acl, old_ownerId, new_ownerId,
        )?;

        // Update owner dependency reference (note: classId, not catalogId).
        backend_catalog_pg_shdepend_seams::changeDependencyOnOwner::call(
            classId,
            objectId,
            new_ownerId,
        )?;
    } else {
        // UnlockTuple(rel, &oldtup->t_self, InplaceUpdateTupleLock). The lock was
        // taken by get_catalog_object_by_oid_extended(.., locktuple=true).
        backend_storage_lmgr_lmgr_seams::unlock_tuple::call(
            rel.rd_id,
            oldtup.tuple.t_self,
            types_storage::lock::InplaceUpdateTupleLock,
        )?;
    }

    // Note: the post-alter hook gets classId, not catalogId.
    invoke_post_alter_hook(classId, objectId)?;

    table_close(rel, RowExclusiveLock)?;

    Ok(())
}

/// Read attribute `anum` (1-based) off a tuple held against `rel`'s descriptor
/// (`heap_getattr`, via a one-shot `heap_deform_tuple`). `anum` must be a
/// not-null fixed column the caller guarantees present (the C asserts `!isnull`).
fn read_attr<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
    anum: i16,
) -> PgResult<Datum<'mcx>> {
    let cols =
        backend_access_common_heaptuple::heap_deform_tuple(mcx, &tup.tuple, &rel.rd_att, &tup.data)?;
    let (val, isnull) = &cols[(anum - 1) as usize];
    debug_assert!(!isnull);
    val.clone_in(mcx)
}

fn read_attr_oid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
    anum: i16,
) -> PgResult<Oid> {
    Ok(read_attr(mcx, rel, tup, anum)?.as_oid())
}

/// Install this crate's inward seam ([`backend_commands_alter_seams`]).
///
/// `alter_object_owner_internal` is the generic ALTER OWNER path that
/// `shdepReassignOwned` (pg_shdepend.c) reaches for object classes without a
/// bespoke owner-change routine. The other ALTER drivers (`ExecRenameStmt` /
/// `ExecAlterObjectSchemaStmt` / `ExecAlterOwnerStmt` / …) are called only from
/// the still-unported utility.c, so they need no inward seam yet.
pub fn init_seams() {
    backend_commands_alter_seams::alter_object_owner_internal::set(
        |class_id, object_id, new_owner_id| {
            let ctx = mcx::MemoryContext::new("AlterObjectOwner_internal");
            AlterObjectOwner_internal(ctx.mcx(), class_id, object_id, new_owner_id)
        },
    );
}

#[cfg(test)]
mod tests;
