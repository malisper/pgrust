//! `RemoveTriggerById` — the guts of trigger deletion (commands/trigger.c:1292),
//! the per-class `OCLASS_TRIGGER` drop handler `dependency.c`'s `doDeletion`
//! invokes for a `pg_trigger` object.

use mcx::{Mcx, MemoryContext};
use types_catalog::pg_trigger as pt;
use types_core::fmgr::F_OIDEQ;
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessExclusiveLock, NoLock, RowExclusiveLock};
use types_tuple::access::{
    RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW,
};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_utils_error::ereport;

/// `RemoveTriggerById(trigOid)` (trigger.c:1292-1362): open `pg_trigger`, scan
/// by oid over `TriggerOidIndexId`, read the trigger's `tgrelid`, delete the
/// `pg_trigger` tuple, then `AccessExclusiveLock` the owning relation and force
/// an SI relcache inval so all backends rebuild their `rd_trigdesc`.
pub fn RemoveTriggerById<'mcx>(mcx: Mcx<'mcx>, trig_oid: Oid) -> PgResult<()> {
    // tgrel = table_open(TriggerRelationId, RowExclusiveLock);
    let tgrel = backend_access_table_table_seams::table_open::call(
        mcx,
        pt::TriggerRelationId,
        RowExclusiveLock,
    )?;

    // ScanKeyInit(&skey[0], Anum_pg_trigger_oid, BTEqualStrategyNumber, F_OIDEQ, trigOid);
    let mut k0 = ScanKeyData::empty();
    ScanKeyInit(
        &mut k0,
        pt::Anum_pg_trigger_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(trig_oid),
    )?;
    let keys = [k0];

    // tgscan = systable_beginscan(tgrel, TriggerOidIndexId, true, NULL, 1, skey);
    let mut scan =
        genam_seams::systable_beginscan::call(&tgrel, pt::TriggerOidIndexId, true, None, &keys)?;

    // tup = systable_getnext(tgscan);
    // if (!HeapTupleIsValid(tup)) elog(ERROR, "could not find tuple for trigger %u", trigOid);
    let Some(tup) = genam_seams::systable_getnext::call(mcx, scan.desc_mut())? else {
        return Err(PgError::new(
            ERROR,
            format!("could not find tuple for trigger {trig_oid}"),
        ));
    };

    // relid = ((Form_pg_trigger) GETSTRUCT(tup))->tgrelid;
    let cols = heap_deform_tuple(mcx, &tup.tuple, &tgrel.rd_att, &tup.data)?;
    let relid = cols[pt::Anum_pg_trigger_tgrelid as usize - 1].0.as_oid();
    let t_self = tup.tuple.t_self;

    // rel = table_open(relid, AccessExclusiveLock);
    let rel =
        backend_access_table_table_seams::table_open::call(mcx, relid, AccessExclusiveLock)?;

    // Triggers may only exist on tables, views, foreign tables, partitioned tables.
    let relkind = rel.rd_rel.relkind;
    if relkind != RELKIND_RELATION
        && relkind != RELKIND_VIEW
        && relkind != RELKIND_FOREIGN_TABLE
        && relkind != RELKIND_PARTITIONED_TABLE
    {
        let detail = backend_catalog_pg_class_seams::errdetail_relkind_not_supported::call(
            relkind as u8,
        )?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "relation \"{}\" cannot have triggers",
                rel.name()
            ))
            .errdetail(detail)
            .into_error());
    }

    // if (!allowSystemTableMods && IsSystemRelation(rel)) ereport(ERROR, ...);
    if !backend_utils_init_small::globals::allowSystemTableMods()
        && backend_catalog_catalog::IsSystemRelation(&rel)
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied: \"{}\" is a system catalog",
                rel.name()
            ))
            .into_error());
    }

    // CatalogTupleDelete(tgrel, &tup->t_self);
    backend_catalog_indexing_seams::catalog_tuple_delete::call(&tgrel, t_self)?;

    // systable_endscan(tgscan); table_close(tgrel, RowExclusiveLock);
    let _ = scan;
    tgrel.close(RowExclusiveLock)?;

    // Force a relcache inval to make all backends rebuild their relcache entries
    // (C does not try to recompute relhastriggers).
    backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcache(&rel)?;

    // Keep lock on trigger's rel until end of xact.
    rel.close(NoLock)?;

    Ok(())
}

/// Install the `RemoveTriggerById` drop-handler seam (dependency.c `doDeletion`).
pub fn init_seams() {
    // The inward seam carries no `mcx` (the C `RemoveTriggerById(Oid)` allocates
    // in `CurrentMemoryContext`); wrap it in a scratch context.
    backend_commands_trigger_seams::RemoveTriggerById::set(|trig_oid| {
        let ctx = MemoryContext::new("RemoveTriggerById");
        RemoveTriggerById(ctx.mcx(), trig_oid)
    });
}
