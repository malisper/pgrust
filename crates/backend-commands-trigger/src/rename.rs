//! `renametrig` family (commands/trigger.c:1468-1707) — `ALTER TRIGGER ...
//! RENAME TO`. Faithful 1:1 port of `renametrig`, `renametrig_internal`,
//! `renametrig_partition`, and the `RangeVarCallbackForRenameTrigger`
//! lock-acquisition callback.

use mcx::{Mcx, MemoryContext};
use types_acl::acl::ACLCHECK_NOT_OWNER;
use types_catalog::pg_trigger as pt;
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
    NOTICE,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessExclusiveLock, NoLock, RowExclusiveLock};
use types_tuple::access::{
    RangeVar, RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW,
};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_catalog_aclchk_seams as aclchk;
use backend_catalog_indexing_seams as indexing;
use backend_utils_error::{elog, ereport};

use backend_access_common_heaptuple::FormedTuple;
use types_catalog::catalog_dependency::ObjectAddress;
use types_rel::Relation;

/// `OidIsValid` test (catalog/pg_class header inline).
fn valid(oid: Oid) -> bool {
    oid != 0
}

/// `ObjectAddressSet(address, classId, objectId)`.
fn addr(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// `namestrcpy(&name, src)` — copy `src` into a zero-filled 64-byte `NameData`
/// image.
fn namestrcpy_image(src: &str) -> [u8; 64] {
    let mut buf = [0u8; 64];
    let bytes = src.as_bytes();
    let n = bytes.len().min(63);
    buf[..n].copy_from_slice(&bytes[..n]);
    buf
}

/// `RangeVarCallbackForRenameTrigger(rv, relid, oldrelid, arg)`
/// (commands/trigger.c:1437-1466). The `RangeVarGetRelidExtended` lock-acquire
/// callback: confirm the relation can have triggers, that the caller owns it,
/// and that it is not a system catalog.
fn range_var_callback_for_rename_trigger(rv: &RangeVar, relid: Oid) -> PgResult<()> {
    // tuple = SearchSysCache1(RELOID, relid); if (!HeapTupleIsValid(tuple)) return;
    // (concurrently dropped — nothing to check, the open below will error).
    let relkind = match backend_utils_cache_lsyscache::relation::get_rel_relkind(relid) {
        Ok(k) => k,
        Err(_) => return Ok(()),
    };

    // only tables and views can have triggers
    if relkind != RELKIND_RELATION
        && relkind != RELKIND_VIEW
        && relkind != RELKIND_FOREIGN_TABLE
        && relkind != RELKIND_PARTITIONED_TABLE
    {
        let detail =
            backend_catalog_pg_class_seams::errdetail_relkind_not_supported::call(relkind)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("relation \"{}\" cannot have triggers", rv.relname))
            .errdetail(detail)
            .into_error());
    }

    // you must own the table to rename one of its triggers
    let userid = backend_utils_init_miscinit::GetUserId();
    if !aclchk::object_ownercheck::call(
        types_catalog::pg_class::RelationRelationId,
        relid,
        userid,
    )? {
        let objtype = backend_catalog_objectaddress::resolve::get_relkind_objtype(relkind);
        aclchk::aclcheck_error::call(ACLCHECK_NOT_OWNER, objtype, Some(rv.relname.clone()))?;
    }

    // if (!allowSystemTableMods && IsSystemClass(relid, form)) ereport(ERROR, ...);
    let relnamespace = backend_utils_cache_lsyscache::relation::get_rel_namespace(relid)?;
    if !backend_utils_init_small::globals::allowSystemTableMods()
        && backend_catalog_catalog::IsSystemClassByNamespace(relid, relnamespace)
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied: \"{}\" is a system catalog",
                rv.relname
            ))
            .into_error());
    }

    Ok(())
}

/// `renametrig(RenameStmt *stmt)` (commands/trigger.c:1467-1581).
pub fn renametrig<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_parsenodes::RenameStmt,
) -> PgResult<ObjectAddress> {
    let relation = stmt.relation.as_ref().ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("renametrig: stmt->relation is NULL")
            .into_error()
    })?;
    let subname = stmt.subname.as_deref().ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("renametrig: stmt->subname is NULL")
            .into_error()
    })?;
    let newname = stmt.newname.as_deref().ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("renametrig: stmt->newname is NULL")
            .into_error()
    })?;

    // relid = RangeVarGetRelidExtended(stmt->relation, AccessExclusiveLock, 0,
    //                                  RangeVarCallbackForRenameTrigger, NULL);
    let mut cb =
        |rv: &RangeVar, relid: Oid, _oldrelid: Oid| range_var_callback_for_rename_trigger(rv, relid);
    let relid = backend_catalog_namespace::RangeVarGetRelidExtended(
        mcx,
        relation,
        AccessExclusiveLock,
        0,
        Some(&mut cb),
    )?;

    // targetrel = relation_open(relid, NoLock); (have lock already)
    let targetrel = backend_access_table_table_seams::table_open::call(mcx, relid, NoLock)?;

    // On partitioned tables, this operation recurses to partitions. Lock all
    // tables upfront.
    if targetrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let _ = backend_catalog_pg_inherits::find_all_inheritors(
            mcx,
            relid,
            AccessExclusiveLock,
            false,
        )?;
    }

    // tgrel = table_open(TriggerRelationId, RowExclusiveLock);
    let tgrel = backend_access_table_table_seams::table_open::call(
        mcx,
        pt::TriggerRelationId,
        RowExclusiveLock,
    )?;

    // Search for the trigger to modify.
    let mut k0 = ScanKeyData::empty();
    ScanKeyInit(
        &mut k0,
        pt::Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;
    let mut k1 = ScanKeyData::empty();
    ScanKeyInit(
        &mut k1,
        pt::Anum_pg_trigger_tgname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        name_datum(mcx, subname)?,
    )?;
    let keys = [k0, k1];

    let mut tgscan = genam_seams::systable_beginscan::call(
        &tgrel,
        pt::TriggerRelidNameIndexId,
        true,
        None,
        &keys,
    )?;

    let tgoid;
    if let Some(tuple) = genam_seams::systable_getnext::call(mcx, tgscan.desc_mut())? {
        // trigform = (Form_pg_trigger) GETSTRUCT(tuple);
        let cols = heap_deform_tuple(mcx, &tuple.tuple, &tgrel.rd_att, &tuple.data)?;
        tgoid = cols[pt::Anum_pg_trigger_oid as usize - 1].0.as_oid();
        let tgparentid = cols[pt::Anum_pg_trigger_tgparentid as usize - 1].0.as_oid();

        // If the trigger descends from a trigger on a parent partitioned table,
        // reject the rename.
        if valid(tgparentid) {
            let parent =
                backend_catalog_partition_seams::get_partition_parent::call(relid, false)?;
            let parent_name = backend_utils_cache_lsyscache::relation::get_rel_name(mcx, parent)?
                .map(|s| s.to_string())
                .unwrap_or_default();
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot rename trigger \"{}\" on table \"{}\"",
                    subname,
                    targetrel.name()
                ))
                .errhint(format!(
                    "Rename the trigger on the partitioned table \"{parent_name}\" instead."
                ))
                .into_error());
        }

        // Rename the trigger on this relation ...
        renametrig_internal(mcx, &tgrel, &targetrel, &cols, &tuple, newname, subname)?;

        // ... and if it is partitioned, recurse to its partitions
        if targetrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            let partdesc =
                backend_partitioning_partdesc::RelationGetPartitionDesc(mcx, &targetrel, true)?;
            for i in 0..partdesc.nparts as usize {
                let partition_id = partdesc.oids[i];
                renametrig_partition(mcx, &tgrel, partition_id, tgoid, newname, subname)?;
            }
        }
    } else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "trigger \"{}\" for table \"{}\" does not exist",
                subname,
                targetrel.name()
            ))
            .into_error());
    }

    let address = addr(pt::TriggerRelationId, tgoid);

    // systable_endscan(tgscan); table_close(tgrel, RowExclusiveLock);
    let _ = tgscan;
    tgrel.close(RowExclusiveLock)?;

    // Close rel, but keep exclusive lock!
    targetrel.close(NoLock)?;

    Ok(address)
}

/// `renametrig_internal(tgrel, targetrel, trigtup, newname, expected_name)`
/// (commands/trigger.c:1582-1648). Update one pg_trigger row's `tgname`.
///
/// `trigcols` is the already-deformed `trigtup` (the caller deformed it for the
/// `tgparentid` check; C re-reads via `GETSTRUCT`).
#[allow(clippy::too_many_arguments)]
fn renametrig_internal<'mcx>(
    mcx: Mcx<'mcx>,
    tgrel: &Relation<'mcx>,
    targetrel: &Relation<'mcx>,
    trigcols: &[(Datum<'mcx>, bool)],
    trigtup: &FormedTuple<'mcx>,
    newname: &str,
    expected_name: &str,
) -> PgResult<()> {
    // If the trigger already has the new name, nothing to do.
    let cur_name =
        name_str(trigcols[pt::Anum_pg_trigger_tgname as usize - 1].0.as_ref_bytes());
    if cur_name == newname {
        return Ok(());
    }

    // Before actually trying the rename, search for triggers with the same name.
    let mut k0 = ScanKeyData::empty();
    ScanKeyInit(
        &mut k0,
        pt::Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(targetrel.rd_id),
    )?;
    let mut k1 = ScanKeyData::empty();
    ScanKeyInit(
        &mut k1,
        pt::Anum_pg_trigger_tgname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        name_datum(mcx, newname)?,
    )?;
    let keys = [k0, k1];

    let mut tgscan = genam_seams::systable_beginscan::call(
        tgrel,
        pt::TriggerRelidNameIndexId,
        true,
        None,
        &keys,
    )?;
    if genam_seams::systable_getnext::call(mcx, tgscan.desc_mut())?.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "trigger \"{}\" for relation \"{}\" already exists",
                newname,
                targetrel.name()
            ))
            .into_error());
    }
    let _ = tgscan;

    // If the trigger has a name different from what we expected, let the user
    // know. (We can proceed anyway, since we must have reached here following a
    // tgparentid link.)
    if cur_name != expected_name {
        elog(
            NOTICE,
            format!(
                "renamed trigger \"{}\" on relation \"{}\"",
                cur_name,
                targetrel.name()
            ),
        )?;
    }

    // namestrcpy(&tgform->tgname, newname); CatalogTupleUpdate(tgrel, &tup->t_self, tup);
    let fields = pt::TriggerFieldUpdate {
        tgname: namestrcpy_image(newname),
    };
    indexing::catalog_tuple_update_pg_trigger::call(tgrel, trigtup.tuple.t_self, &fields)?;

    // InvokeObjectPostAlterHook(TriggerRelationId, tgform->oid, 0); -- no-op.

    // Invalidate relation's relcache entry so other backends rebuild.
    backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcache(targetrel)?;

    Ok(())
}

/// `renametrig_partition(tgrel, partitionId, parentTriggerOid, newname,
/// expected_name)` (commands/trigger.c:1654-1707). Recurse to a partition,
/// find the child trigger linked to `parent_trigger_oid`, rename it, and
/// recurse further if the partition is itself partitioned.
fn renametrig_partition<'mcx>(
    mcx: Mcx<'mcx>,
    tgrel: &Relation<'mcx>,
    partition_id: Oid,
    parent_trigger_oid: Oid,
    newname: &str,
    expected_name: &str,
) -> PgResult<()> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        pt::Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(partition_id),
    )?;
    let keys = [key];

    let mut tgscan = genam_seams::systable_beginscan::call(
        tgrel,
        pt::TriggerRelidNameIndexId,
        true,
        None,
        &keys,
    )?;

    while let Some(tuple) = genam_seams::systable_getnext::call(mcx, tgscan.desc_mut())? {
        let cols = heap_deform_tuple(mcx, &tuple.tuple, &tgrel.rd_att, &tuple.data)?;
        let tgparentid = cols[pt::Anum_pg_trigger_tgparentid as usize - 1].0.as_oid();
        if tgparentid != parent_trigger_oid {
            continue; // not our trigger
        }
        let tgoid = cols[pt::Anum_pg_trigger_oid as usize - 1].0.as_oid();
        let this_tgname =
            name_str(cols[pt::Anum_pg_trigger_tgname as usize - 1].0.as_ref_bytes());

        // partitionRel = table_open(partitionId, NoLock);
        let partition_rel =
            backend_access_table_table_seams::table_open::call(mcx, partition_id, NoLock)?;

        // Rename the trigger on this partition.
        renametrig_internal(
            mcx,
            tgrel,
            &partition_rel,
            &cols,
            &tuple,
            newname,
            expected_name,
        )?;

        // And if this relation is partitioned, recurse to its partitions.
        if partition_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            let partdesc =
                backend_partitioning_partdesc::RelationGetPartitionDesc(mcx, &partition_rel, true)?;
            for i in 0..partdesc.nparts as usize {
                let partoid = partdesc.oids[i];
                renametrig_partition(mcx, tgrel, partoid, tgoid, newname, &this_tgname)?;
            }
        }
        partition_rel.close(NoLock)?;

        // There should be at most one matching tuple.
        break;
    }
    let _ = tgscan;
    Ok(())
}

/// `NameStr(name)` over a deformed `name` Datum's raw bytes: the NUL-terminated
/// cstring as an owned `String`.
fn name_str(image: &[u8]) -> String {
    let end = image.iter().position(|&b| b == 0).unwrap_or(image.len());
    String::from_utf8_lossy(&image[..end]).into_owned()
}

/// `PointerGetDatum(name)` over a NUL-padded name — the by-ref convention
/// `F_NAMEEQ` reads (precedent: create.rs's `tgname` scankey).
fn name_datum<'mcx>(mcx: Mcx<'mcx>, src: &str) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, src.as_bytes())?))
}

/// Install the `renametrig` seam (commands/alter.c `ExecRenameStmt` dispatch).
pub fn init_seams() {
    backend_commands_trigger_seams::renametrig::set(|mcx, stmt| {
        let ctx = MemoryContext::new("renametrig");
        let _ = ctx;
        renametrig(mcx, stmt)
    });
}
