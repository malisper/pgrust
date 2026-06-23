//! `EventTriggerSQLDropAddObject`'s descriptive-field computation
//! (`commands/event_trigger.c`), split out so it runs where the
//! `ObjectProperty` table, `getObjectIdentityParts` / `getObjectTypeDescription`
//! and the temp-namespace machinery already live (`catalog/objectaddress.c`).
//!
//! `event_trigger.c` keeps the `currentEventTriggerState->SQLDropList` it pushes
//! the result onto; this seam returns the populated descriptive bundle
//! ([`SqlDropObjectInfo`]) and the static `obtain_object_name_namespace` helper
//! it uses, faithful to the PG 18.3 C.

extern crate alloc;

use alloc::string::ToString;

use ::mcx::Mcx;
use ::types_catalog::catalog_dependency::ObjectAddress;
use ::types_catalog::pg_event_trigger::SqlDropObjectInfo;
use ::types_core::{Oid, OidIsValid};
use ::types_error::PgResult;
use ::types_storage::lock::AccessShareLock;

use namespace_seams as namespace;
use lsyscache_seams as lsyscache;
use syscache_seams as syscache;

use crate::consts::{
    AttrDefaultRelationId, InvalidAttrNumber, NamespaceRelationId, PolicyRelationId,
    RelationRelationId, TriggerRelationId,
};
use crate::identity::get_object_identity_parts;
use crate::properties::{
    get_object_attnum_name, get_object_attnum_namespace, get_object_namensp_unique,
    get_object_attnum_oid, is_objectclass_supported,
};
use crate::resolve::get_catalog_object_by_oid;
use crate::type_description::get_object_type_description;

/// `obtain_object_name_namespace(object, obj)` (event_trigger.c). Fills the
/// `schemaname` / `objname` / `istemp` fields of `info` from the object's
/// catalog tuple, skipping objects in *other* backends' temp schemas. Returns
/// `false` for such a skipped object (the C `return false`), `true` otherwise.
fn obtain_object_name_namespace<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    info: &mut SqlDropObjectInfo,
) -> PgResult<bool> {
    // We trust that ObjectProperty contains all object classes that can be
    // schema-qualified. Object classes not in ObjectProperty are left untouched.
    if !is_objectclass_supported(object.classId) {
        return Ok(true);
    }

    // catalog = table_open(object->classId, AccessShareLock);
    let catalog =
        common_relation_seams::relation_open::call(mcx, object.classId, AccessShareLock)?;

    // tuple = get_catalog_object_by_oid(catalog, get_object_attnum_oid(classId), objectId);
    let objtup = get_catalog_object_by_oid(
        mcx,
        &catalog,
        get_object_attnum_oid(object.classId)?,
        object.objectId,
    )?;

    if let Some(tuple) = objtup {
        // Schema column.
        let attnum = get_object_attnum_namespace(object.classId)?;
        if attnum != InvalidAttrNumber {
            if let Some(datum) =
                crate::fmgr_sql::heap_getattr(mcx, &tuple, attnum as i32, &catalog.rd_att)?
            {
                let namespace_id = datum.as_oid();
                // temp objects are only reported if they are my own
                if namespace::is_temp_namespace::call(namespace_id)? {
                    info.schemaname = Some("pg_temp".to_string());
                    info.istemp = true;
                } else if namespace::is_any_temp_namespace::call(mcx, namespace_id)? {
                    // no need to fill any fields of *obj
                    catalog.close(AccessShareLock)?;
                    return Ok(false);
                } else {
                    info.schemaname = lsyscache::get_namespace_name::call(mcx, namespace_id)?
                        .map(|s| s.as_str().to_string());
                    info.istemp = false;
                }
            }
        }

        // Name column (only for namespace-unique classes addressing the whole
        // object, not a sub-object).
        if get_object_namensp_unique(object.classId)? && object.objectSubId == 0 {
            let attnum = get_object_attnum_name(object.classId)?;
            if attnum != InvalidAttrNumber {
                if let Some(datum) =
                    crate::fmgr_sql::heap_getattr(mcx, &tuple, attnum as i32, &catalog.rd_att)?
                {
                    info.objname = Some(crate::fmgr_sql::datum_get_name(&datum));
                }
            }
        }
    }

    catalog.close(AccessShareLock)?;
    Ok(true)
}

/// Fetch a trigger/policy's owning table OID the "hard way" (no syscache for
/// `pg_trigger` / `pg_policy`), mirroring the by-oid `systable` scan in the C
/// `EventTriggerSQLDropAddObject` `TriggerRelationId` / `PolicyRelationId`
/// branches. `attnum_oid` is the OID column, `attnum_relid` the table-OID column.
fn fetch_owning_relid<'mcx>(
    mcx: Mcx<'mcx>,
    class_id: Oid,
    attnum_oid: i16,
    attnum_relid: i16,
    object_id: Oid,
) -> PgResult<Oid> {
    let catalog =
        common_relation_seams::relation_open::call(mcx, class_id, AccessShareLock)?;
    let objtup = get_catalog_object_by_oid(mcx, &catalog, attnum_oid, object_id)?;
    let relid = match objtup {
        Some(tuple) => {
            match crate::fmgr_sql::heap_getattr(mcx, &tuple, attnum_relid as i32, &catalog.rd_att)? {
                Some(d) => d.as_oid(),
                None => ::types_core::InvalidOid,
            }
        }
        None => ::types_core::InvalidOid, // shouldn't happen
    };
    catalog.close(AccessShareLock)?;
    Ok(relid)
}

/// `EventTriggerSQLDropAddObject`'s field computation (`event_trigger.c`),
/// excluding the `currentEventTriggerState->SQLDropList` append the caller owns.
/// `info.report == false` means the object is in another backend's temp schema
/// and must NOT be recorded (the C early `return` / `pfree(obj)`).
pub fn event_trigger_describe_dropped_object<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
) -> PgResult<SqlDropObjectInfo> {
    let mut info = SqlDropObjectInfo {
        report: true,
        ..Default::default()
    };

    if object.classId == NamespaceRelationId {
        // Special handling is needed for temp namespaces.
        if namespace::is_temp_namespace::call(object.objectId)? {
            info.istemp = true;
        } else if namespace::is_any_temp_namespace::call(mcx, object.objectId)? {
            // don't report temp schemas except my own
            info.report = false;
            return Ok(info);
        }
        info.objname = lsyscache::get_namespace_name::call(mcx, object.objectId)?
            .map(|s| s.as_str().to_string());
    } else if object.classId == AttrDefaultRelationId {
        // We treat a column default as temp if its table is temp.
        if let Some((adrelid, adnum)) = syscache::attr_default_column::call(object.objectId)? {
            if OidIsValid(adrelid) {
                let colobject = ObjectAddress {
                    classId: RelationRelationId,
                    objectId: adrelid,
                    objectSubId: adnum as i32,
                };
                if !obtain_object_name_namespace(mcx, &colobject, &mut info)? {
                    info.report = false;
                    return Ok(info);
                }
            }
        }
    } else if object.classId == TriggerRelationId {
        // Similarly, a trigger is temp if its table is temp.
        let relid = fetch_owning_relid(
            mcx,
            TriggerRelationId,
            crate::consts::Anum_pg_trigger_oid,
            crate::consts::Anum_pg_trigger_tgrelid,
            object.objectId,
        )?;
        if OidIsValid(relid) {
            let relobject = ObjectAddress {
                classId: RelationRelationId,
                objectId: relid,
                // Arbitrarily set objectSubId nonzero so as not to fill objname.
                objectSubId: 1,
            };
            if !obtain_object_name_namespace(mcx, &relobject, &mut info)? {
                info.report = false;
                return Ok(info);
            }
        }
    } else if object.classId == PolicyRelationId {
        // Similarly, a policy is temp if its table is temp.
        let relid = fetch_owning_relid(
            mcx,
            PolicyRelationId,
            crate::consts::Anum_pg_policy_oid,
            crate::consts::Anum_pg_policy_polrelid,
            object.objectId,
        )?;
        if OidIsValid(relid) {
            let relobject = ObjectAddress {
                classId: RelationRelationId,
                objectId: relid,
                objectSubId: 1,
            };
            if !obtain_object_name_namespace(mcx, &relobject, &mut info)? {
                info.report = false;
                return Ok(info);
            }
        }
    } else {
        // Generic handling for all other object classes.
        if !obtain_object_name_namespace(mcx, object, &mut info)? {
            // don't report temp objects except my own
            info.report = false;
            return Ok(info);
        }
    }

    // object identity, objname and objargs
    match get_object_identity_parts(mcx, object, false)? {
        Some((identity, parts)) => {
            info.objidentity = Some(identity.as_str().to_string());
            info.addrnames = Some(parts.objname);
            info.addrargs = Some(parts.objargs);
        }
        None => {
            info.objidentity = None;
        }
    }

    // object type
    info.objecttype = get_object_type_description(mcx, object, false)?
        .map(|s| s.as_str().to_string());

    Ok(info)
}

/// The per-command descriptive-field computation `pg_event_trigger_ddl_commands`
/// (`event_trigger.c` ~2123-2166) performs for one `CollectedCommand` whose
/// `addr` is an ordinary object (the `SCT_Simple` / `SCT_AlterTable` /
/// `SCT_AlterOpFamily` / `SCT_CreateOpClass` / `SCT_AlterTSConfig` arm):
/// `getObjectIdentity(addr, true)`, `getObjectTypeDescription(addr, true)`, and
/// the namespace lookup (`is_objectclass_supported` →
/// `get_object_attnum_namespace` → `get_catalog_object_by_oid` →
/// `heap_getattr` → `get_namespace_name_or_temp`).
///
/// Returns `Ok(None)` when `getObjectIdentity` returns NULL (the C
/// `if (identity == NULL) continue;` — the object was dropped in the same
/// command). Otherwise returns `(identity, type, schema)` where `schema` is
/// `None` for a schema-less object class. Unlike the dropped-object form there
/// is no temp-namespace filtering: the C path always records the row.
pub fn event_trigger_describe_command_object<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
) -> PgResult<Option<(String, String, Option<String>)>> {
    use alloc::format;

    // identity = getObjectIdentity(&addr, true);
    // if (identity == NULL) continue;
    let identity = match crate::identity::get_object_identity(mcx, object, true)? {
        Some(s) => s.as_str().to_string(),
        None => return Ok(None),
    };

    // type = getObjectTypeDescription(&addr, true);  /* never NULL */
    let typedesc = get_object_type_description(mcx, object, true)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();

    // Obtain schema name, if any. NULL for schema-less object classes.
    let mut schema: Option<String> = None;
    if is_objectclass_supported(object.classId) {
        let nsp_attnum = get_object_attnum_namespace(object.classId)?;
        if nsp_attnum != InvalidAttrNumber {
            // catalog = table_open(addr.classId, AccessShareLock);
            let catalog = common_relation_seams::relation_open::call(
                mcx,
                object.classId,
                AccessShareLock,
            )?;
            // objtup = get_catalog_object_by_oid(catalog,
            //              get_object_attnum_oid(addr.classId), addr.objectId);
            let objtup = get_catalog_object_by_oid(
                mcx,
                &catalog,
                get_object_attnum_oid(object.classId)?,
                object.objectId,
            )?;
            let tuple = match objtup {
                Some(t) => t,
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(utils_error::ereport(::types_error::ERROR)
                        .errmsg_internal(format!(
                            "cache lookup failed for object {}/{}",
                            object.classId, object.objectId
                        ))
                        .into_error());
                }
            };
            // schema_oid = heap_getattr(objtup, nspAttnum, ...); if (isnull) elog(ERROR)
            match crate::fmgr_sql::heap_getattr(mcx, &tuple, nsp_attnum as i32, &catalog.rd_att)? {
                Some(datum) => {
                    let schema_oid = datum.as_oid();
                    schema = lsyscache::get_namespace_name_or_temp::call(mcx, schema_oid)?
                        .map(|s| s.as_str().to_string());
                }
                None => {
                    catalog.close(AccessShareLock)?;
                    return Err(utils_error::ereport(::types_error::ERROR)
                        .errmsg_internal(format!(
                            "invalid null namespace in object {}/{}/{}",
                            object.classId, object.objectId, object.objectSubId
                        ))
                        .into_error());
                }
            }
            catalog.close(AccessShareLock)?;
        }
    }

    Ok(Some((identity, typedesc, schema)))
}
