// namespace.c temp-namespace creation half. C divergences:
// RecoveryInProgress/IsParallelWorker guards are compile-time absent (no hot
// standby, no parallel workers).
use std::cell::Cell;

use mcx::{Mcx, MemoryContext};
use types_core::{
    InvalidOid, InvalidSubTransactionId, Oid, BOOTSTRAP_SUPERUSERID, DATABASE_RELATION_ID,
    NAMESPACE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_TABLE_DEFINITION, ERROR,
};
use types_nodes::parsenodes::DropBehavior;

use crate::path::invalidate_search_path_cache;
use crate::{
    get_namespace_oid, isAnyTempNamespace, isTempOrTempToastNamespace, my_temp_namespace,
    OidIsValid, BASE_SEARCH_PATH_VALID, MY_TEMP_NAMESPACE, MY_TEMP_NAMESPACE_SUB_ID,
    MY_TEMP_TOAST_NAMESPACE,
};

const ACL_CREATE_TEMP: u64 = 1 << 10;
const ACLCHECK_OK: i32 = 0;
// AclResult (acl.h); the only non-OK value this file raises.
const ACLCHECK_NOT_OWNER: i32 = 2;
const PERFORM_DELETION_INTERNAL: i32 = 0x0001;
const PERFORM_DELETION_QUIETLY: i32 = 0x0004;
const PERFORM_DELETION_SKIP_ORIGINAL: i32 = 0x0008;
const PERFORM_DELETION_SKIP_EXTENSIONS: i32 = 0x0010;

pub const RELPERSISTENCE_PERMANENT: u8 = b'p';
pub const RELPERSISTENCE_TEMP: u8 = b't';

pub fn GetTempTableNamespace(mcx: Mcx<'_>) -> PgResult<Oid> {
    AccessTempTableNamespace(mcx, false)?;
    let oid = my_temp_namespace();
    debug_assert!(OidIsValid(oid));
    Ok(oid)
}

pub fn AccessTempTableNamespace(mcx: Mcx<'_>, force: bool) -> PgResult<()> {
    xact::OrMyXactFlags(types_core::XACT_FLAGS_ACCESSEDTEMPNAMESPACE);
    if !force && OidIsValid(my_temp_namespace()) {
        return Ok(());
    }
    InitTempTableNamespace(mcx)
}

fn InitTempTableNamespace(mcx: Mcx<'_>) -> PgResult<()> {
    debug_assert!(!OidIsValid(my_temp_namespace()));

    let dbid = init_small::globals::MyDatabaseId();
    if aclchk_seams::object_aclcheck::call(
        DATABASE_RELATION_ID,
        dbid,
        miscinit_seams::get_user_id::call(),
        ACL_CREATE_TEMP,
    )? != ACLCHECK_OK
    {
        let dbname = dbcommands_seams::get_database_name::call(dbid)?.unwrap_or_default();
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("permission denied to create temporary tables in database \"{dbname}\""),
            )
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }

    let proc_number = init_small::globals::MyProcNumber();

    let namespace_name = format!("pg_temp_{proc_number}");
    let mut namespace_id = get_namespace_oid(&namespace_name, true)?;
    if !OidIsValid(namespace_id) {
        namespace_id =
            pg_namespace::NamespaceCreate(mcx, &namespace_name, BOOTSTRAP_SUPERUSERID, true)?;
        xact::CommandCounterIncrement()?;
    } else {
        RemoveTempRelations(mcx, namespace_id)?;
    }

    let toast_name = format!("pg_toast_temp_{proc_number}");
    let mut toastspace_id = get_namespace_oid(&toast_name, true)?;
    if !OidIsValid(toastspace_id) {
        toastspace_id =
            pg_namespace::NamespaceCreate(mcx, &toast_name, BOOTSTRAP_SUPERUSERID, true)?;
        xact::CommandCounterIncrement()?;
    }

    MY_TEMP_NAMESPACE.with(|c| c.set(namespace_id));
    MY_TEMP_TOAST_NAMESPACE.with(|c| c.set(toastspace_id));
    crate::advertise_temp_namespace(namespace_id);

    debug_assert_eq!(MY_TEMP_NAMESPACE_SUB_ID.with(Cell::get), InvalidSubTransactionId);
    MY_TEMP_NAMESPACE_SUB_ID.with(|c| c.set(xact::GetCurrentSubTransactionId()));

    BASE_SEARCH_PATH_VALID.with(|c| c.set(false));
    invalidate_search_path_cache();
    Ok(())
}

pub(crate) fn RemoveTempRelations(mcx: Mcx<'_>, temp_namespace_id: Oid) -> PgResult<()> {
    dependency_seams::perform_deletion::call(
        mcx,
        NAMESPACE_RELATION_ID,
        temp_namespace_id,
        0,
        DropBehavior::DROP_CASCADE,
        PERFORM_DELETION_INTERNAL
            | PERFORM_DELETION_QUIETLY
            | PERFORM_DELETION_SKIP_ORIGINAL
            | PERFORM_DELETION_SKIP_EXTENSIONS,
    )
}

pub(crate) fn RemoveTempRelationsCallback(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    if OidIsValid(my_temp_namespace()) {
        xact::AbortOutOfAnyTransaction()?;
        xact::StartTransactionCommand()?;
        let snapshot = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snapshot)?;

        let scratch = MemoryContext::new("RemoveTempRelations");
        let result = RemoveTempRelations(scratch.mcx(), my_temp_namespace());

        snapmgr::PopActiveSnapshot()?;
        result?;
        xact::CommitTransactionCommand()?;
    }
    Ok(())
}

pub fn ResetTempTableNamespace() -> PgResult<()> {
    if OidIsValid(my_temp_namespace()) {
        let scratch = MemoryContext::new("ResetTempTableNamespace");
        RemoveTempRelations(scratch.mcx(), my_temp_namespace())?;
    }
    Ok(())
}

pub fn RangeVarGetCreationNamespace(mcx: Mcx<'_>, rv: &rel_vocab::RangeVar<'_>) -> PgResult<Oid> {
    if let Some(catalogname) = rv.catalogname {
        let dbname = dbcommands_seams::get_database_name::call(init_small::globals::MyDatabaseId())?
            .unwrap_or_default();
        if catalogname != dbname {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "cross-database references are not implemented: \"{}.{}.{}\"",
                        catalogname,
                        rv.schemaname.unwrap_or(""),
                        rv.relname
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
    }

    if let Some(schemaname) = rv.schemaname {
        if schemaname == "pg_temp" {
            AccessTempTableNamespace(mcx, false)?;
            return Ok(my_temp_namespace());
        }
        return get_namespace_oid(schemaname, false);
    }

    if rv.relpersistence == RELPERSISTENCE_TEMP {
        AccessTempTableNamespace(mcx, false)?;
        return Ok(my_temp_namespace());
    }

    crate::path::recomputeNamespacePath()?;
    if crate::BASE_TEMP_CREATION_PENDING.with(Cell::get) {
        AccessTempTableNamespace(mcx, true)?;
        return Ok(my_temp_namespace());
    }
    let namespace_id = crate::BASE_CREATION_NAMESPACE.with(Cell::get);
    if !OidIsValid(namespace_id) {
        return Err(Box::new(
            PgError::new(ERROR, "no schema has been selected to create in".to_string())
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_SCHEMA),
        ));
    }
    Ok(namespace_id)
}

/// `RangeVarGetAndCheckCreationNamespace` (namespace.c).
///
/// Resolves the creation namespace for a new relation named by `rv`, checks
/// `ACL_CREATE` on it, and takes `AccessShareLock` on the namespace object so
/// the namespace cannot be dropped before this transaction commits — without
/// that lock a committed `pg_class` row can end up with `relnamespace` pointing
/// at a no-longer-existent `pg_namespace` (C's comment at namespace.c:730-733).
/// The whole body is C's `for(;;)`: any invalidation processed while looking
/// names up and taking locks restarts the resolve, giving up the locks whose
/// subject changed.
///
/// Returns `(namespace_oid, existing_relation_id, adjusted_relpersistence)`.
/// C's out-parameters become returns because our `RangeVar` is immutable:
/// `existing_relation_id` is `InvalidOid` unless `want_existing` (C's
/// `existing_relation_id != NULL`), and the adjusted persistence is what C
/// writes back into `relation->relpersistence` via
/// `RangeVarAdjustRelationPersistence`.
///
/// `lockmode != NoLock` additionally ownerchecks and locks the pre-existing
/// relation of that name, if any, exactly as C does.
pub fn RangeVarGetAndCheckCreationNamespace(
    mcx: Mcx<'_>,
    rv: &rel_vocab::RangeVar<'_>,
    lockmode: types_rel::LOCKMODE,
    want_existing: bool,
) -> PgResult<(Oid, Oid, u8)> {
    // C checks the catalog name and then ignores it; RangeVarGetCreationNamespace
    // repeats the same check inside the loop, as in C.
    let mut relid;
    let mut nspid;
    let mut oldrelid = InvalidOid;
    let mut oldnspid = InvalidOid;
    let mut retry = false;

    loop {
        let inval_count = sinval::SharedInvalidMessageCounter();

        // Look up creation namespace and check for existing relation.
        nspid = RangeVarGetCreationNamespace(mcx, rv)?;
        debug_assert!(OidIsValid(nspid));
        relid = if want_existing {
            lsyscache::get_relname_relid(rv.relname, nspid)?
        } else {
            InvalidOid
        };

        // In bootstrap processing mode C skips permissions and locking:
        // permissions may not work yet and locking is unnecessary.
        if miscinit_seams::is_bootstrap_processing_mode::call() {
            break;
        }

        // Check namespace permissions.
        let aclresult = aclchk_seams::object_aclcheck::call(
            NAMESPACE_RELATION_ID,
            nspid,
            miscinit_seams::get_user_id::call(),
            types_nodes::parsenodes::ACL_CREATE,
        )?;
        if aclresult != ACLCHECK_OK {
            let nspname = lsyscache::get_namespace_name(mcx, nspid)?;
            aclchk_seams::aclcheck_error::call(
                aclresult,
                types_nodes::parsenodes::ObjectType::OBJECT_SCHEMA as i32,
                nspname.as_ref().map(|s| s.as_str()).unwrap_or(""),
            )?;
        }

        if retry {
            // If nothing changed, we're done.
            if relid == oldrelid && nspid == oldnspid {
                break;
            }
            // If creation namespace has changed, give up old lock.
            if nspid != oldnspid {
                lmgr_seams::unlock_database_object::call(
                    NAMESPACE_RELATION_ID,
                    oldnspid,
                    0,
                    types_rel::AccessShareLock,
                )?;
            }
            // If name points to something different, give up old lock.
            if relid != oldrelid && OidIsValid(oldrelid) && lockmode != types_rel::NoLock {
                lmgr_seams::unlock_relation_oid::call(oldrelid, lockmode)?;
            }
        }

        // Lock namespace.
        if nspid != oldnspid {
            lmgr_seams::lock_database_object::call(
                NAMESPACE_RELATION_ID,
                nspid,
                0,
                types_rel::AccessShareLock,
            )?;
        }

        // Lock relation, if required and we have permission.
        if lockmode != types_rel::NoLock && OidIsValid(relid) {
            if !aclchk_seams::object_ownercheck::call(
                types_core::RELATION_RELATION_ID,
                relid,
                miscinit_seams::get_user_id::call(),
            )? {
                let relkind = lsyscache::get_rel_relkind(relid)? as u8;
                aclchk_seams::aclcheck_error::call(
                    ACLCHECK_NOT_OWNER,
                    get_relkind_objtype(relkind) as i32,
                    rv.relname,
                )?;
            }
            if relid != oldrelid {
                lmgr_seams::lock_relation_oid::call(relid, lockmode)?;
            }
        }

        // If no invalidation messages were processed, we're done!
        if inval_count == sinval::SharedInvalidMessageCounter() {
            break;
        }

        // Something may have changed, so recheck our work.
        retry = true;
        oldrelid = relid;
        oldnspid = nspid;
    }

    let relpersistence = RangeVarAdjustRelationPersistence(rv.relpersistence, nspid)?;
    Ok((nspid, relid, relpersistence))
}

// get_relkind_objtype (tablecmds.c). A pure relkind -> ObjectType mapping;
// carried locally as the rest of the tree does (a dependency edge on the
// commands layer would cycle).
fn get_relkind_objtype(relkind: u8) -> types_nodes::parsenodes::ObjectType {
    use types_nodes::parsenodes::ObjectType;
    match relkind {
        b'r' | b'p' => ObjectType::OBJECT_TABLE,
        b'i' | b'I' => ObjectType::OBJECT_INDEX,
        b'S' => ObjectType::OBJECT_SEQUENCE,
        b'v' => ObjectType::OBJECT_VIEW,
        b'm' => ObjectType::OBJECT_MATVIEW,
        b'f' => ObjectType::OBJECT_FOREIGN_TABLE,
        // C's default arm asserts and falls through to OBJECT_TABLE (a
        // composite type's or toast relation's relkind can reach here).
        _ => ObjectType::OBJECT_TABLE,
    }
}

// QualifiedNameGetCreationNamespace + LookupCreationNamespace (namespace.c).
pub fn QualifiedNameGetCreationNamespace<'a>(
    mcx: Mcx<'_>,
    names: &[&'a str],
) -> PgResult<(Oid, &'a str)> {
    let (schemaname, objname) = crate::DeconstructQualifiedName(names)?;
    if let Some(schemaname) = schemaname {
        if schemaname == "pg_temp" {
            AccessTempTableNamespace(mcx, false)?;
            return Ok((my_temp_namespace(), objname));
        }
        return Ok((get_namespace_oid(schemaname, false)?, objname));
    }
    crate::path::recomputeNamespacePath()?;
    if crate::BASE_TEMP_CREATION_PENDING.with(Cell::get) {
        AccessTempTableNamespace(mcx, true)?;
        return Ok((my_temp_namespace(), objname));
    }
    let namespace_id = crate::BASE_CREATION_NAMESPACE.with(Cell::get);
    if !OidIsValid(namespace_id) {
        return Err(Box::new(
            PgError::new(ERROR, "no schema has been selected to create in".to_string())
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_SCHEMA),
        ));
    }
    Ok((namespace_id, objname))
}

// C mutates newRelation->relpersistence in place; the adjusted value is
// returned instead (callers hold the RangeVar immutably).
pub fn RangeVarAdjustRelationPersistence(relpersistence: u8, nspid: Oid) -> PgResult<u8> {
    match relpersistence {
        RELPERSISTENCE_TEMP => {
            if !isTempOrTempToastNamespace(nspid) {
                let msg = if isAnyTempNamespace(nspid)? {
                    "cannot create relations in temporary schemas of other sessions"
                } else {
                    "cannot create temporary relation in non-temporary schema"
                };
                return Err(invalid_table_definition(msg));
            }
            Ok(relpersistence)
        }
        RELPERSISTENCE_PERMANENT => {
            if isTempOrTempToastNamespace(nspid) {
                Ok(RELPERSISTENCE_TEMP)
            } else if isAnyTempNamespace(nspid)? {
                Err(invalid_table_definition(
                    "cannot create relations in temporary schemas of other sessions",
                ))
            } else {
                Ok(relpersistence)
            }
        }
        _ => {
            if isAnyTempNamespace(nspid)? {
                return Err(invalid_table_definition(
                    "only temporary relations may be created in temporary schemas",
                ));
            }
            Ok(relpersistence)
        }
    }
}

#[track_caller]
#[cold]
fn invalid_table_definition(msg: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, msg.to_string()).with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
    )
}
