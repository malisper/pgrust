// namespace.c temp-namespace creation half. C divergences: MyProc->
// tempNamespaceId is not mirrored (its only reader, autovacuum's
// isTempNamespaceInUse, is unported); RecoveryInProgress/IsParallelWorker
// guards are compile-time absent (no hot standby, no parallel workers).
use std::cell::Cell;

use mcx::{Mcx, MemoryContext};
use types_core::{
    InvalidSubTransactionId, Oid, BOOTSTRAP_SUPERUSERID, DATABASE_RELATION_ID,
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

pub fn ResetTempTableNamespace(mcx: Mcx<'_>) -> PgResult<()> {
    if OidIsValid(my_temp_namespace()) {
        RemoveTempRelations(mcx, my_temp_namespace())?;
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

#[cold]
fn invalid_table_definition(msg: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, msg.to_string()).with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
    )
}
