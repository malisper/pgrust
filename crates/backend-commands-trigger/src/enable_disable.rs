//! `EnableDisableTrigger` (commands/trigger.c:1707-1840) — the catalog leg of
//! `ALTER TABLE ENABLE/DISABLE [ REPLICA | ALWAYS ] TRIGGER`. Faithful 1:1 port:
//! scan `pg_trigger` for the matching trigger(s), flip `tgenabled`, recurse to
//! partitions for FOR EACH ROW triggers, and broadcast a relcache inval if
//! anything changed.

use mcx::Mcx;
use types_catalog::pg_class::RelationRelationId;
use types_catalog::pg_trigger as pt;
use types_tuple::access::RELKIND_PARTITIONED_TABLE;
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
use types_core::Oid;
use types_error::{PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_UNDEFINED_OBJECT, ERROR};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_catalog_indexing_seams as indexing;
use backend_utils_error::ereport;

use types_rel::Relation;

/// `OidIsValid` test.
fn valid(oid: Oid) -> bool {
    oid != 0
}

/// `NameStr(name)` over a deformed `name` Datum's raw bytes.
fn name_str(image: &[u8]) -> String {
    let end = image.iter().position(|&b| b == 0).unwrap_or(image.len());
    String::from_utf8_lossy(&image[..end]).into_owned()
}

/// `CStringGetDatum(tgname)` over a NUL-padded name — the by-ref convention
/// `F_NAMEEQ` reads (precedent: rename.rs's `name_datum`).
fn name_datum<'mcx>(mcx: Mcx<'mcx>, src: &str) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, src.as_bytes())?))
}

/// `EnableDisableTrigger(rel, tgname, tgparent, fires_when, skip_system,
/// recurse, lockmode)` (commands/trigger.c:1726-1840).
///
/// Changes the `tgenabled` field for the specified trigger(s).
///
/// * `tgname`: name of trigger to process, or `None` to scan all triggers.
/// * `tgparent`: if not zero, process only triggers with this `tgparentid`.
/// * `fires_when`: new value for `tgenabled` (one of the `TRIGGER_FIRES_*` /
///   `TRIGGER_DISABLED` codes).
/// * `skip_system`: if true, skip "system" triggers (constraint triggers).
/// * `recurse`: if true, recurse to partitions.
#[allow(clippy::too_many_arguments)]
pub fn EnableDisableTrigger<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tgname: Option<&str>,
    tgparent: Oid,
    fires_when: i8,
    skip_system: bool,
    recurse: bool,
    lockmode: i32,
) -> PgResult<()> {
    // tgrel = table_open(TriggerRelationId, RowExclusiveLock);
    let tgrel = backend_access_table_table_seams::table_open::call(
        mcx,
        pt::TriggerRelationId,
        RowExclusiveLock,
    )?;

    // ScanKeyInit(&keys[0], tgrelid = RelationGetRelid(rel));
    let mut k0 = ScanKeyData::empty();
    ScanKeyInit(
        &mut k0,
        pt::Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(rel.rd_id),
    )?;

    // if (tgname) ScanKeyInit(&keys[1], tgname = tgname); nkeys = 2; else nkeys = 1;
    let keys: Vec<ScanKeyData> = if let Some(name) = tgname {
        let mut k1 = ScanKeyData::empty();
        ScanKeyInit(
            &mut k1,
            pt::Anum_pg_trigger_tgname,
            BTEqualStrategyNumber,
            F_NAMEEQ,
            name_datum(mcx, name)?,
        )?;
        vec![k0, k1]
    } else {
        vec![k0]
    };

    let mut tgscan = genam_seams::systable_beginscan::call(
        &tgrel,
        pt::TriggerRelidNameIndexId,
        true,
        None,
        &keys,
    )?;

    let mut found = false;
    let mut changed = false;

    while let Some(tuple) = genam_seams::systable_getnext::call(mcx, tgscan.desc_mut())? {
        // oldtrig = (Form_pg_trigger) GETSTRUCT(tuple);
        let cols = heap_deform_tuple(mcx, &tuple.tuple, &tgrel.rd_att, &tuple.data)?;
        let oldtrig_oid = cols[pt::Anum_pg_trigger_oid as usize - 1].0.as_oid();
        let oldtrig_tgparentid = cols[pt::Anum_pg_trigger_tgparentid as usize - 1].0.as_oid();
        let oldtrig_tgtype = cols[pt::Anum_pg_trigger_tgtype as usize - 1].0.as_i16();
        let oldtrig_tgenabled = cols[pt::Anum_pg_trigger_tgenabled as usize - 1].0.as_char();
        let oldtrig_tgisinternal = cols[pt::Anum_pg_trigger_tgisinternal as usize - 1].0.as_bool();
        let oldtrig_tgname =
            name_str(cols[pt::Anum_pg_trigger_tgname as usize - 1].0.as_ref_bytes());

        // if (OidIsValid(tgparent) && tgparent != oldtrig->tgparentid) continue;
        if valid(tgparent) && tgparent != oldtrig_tgparentid {
            continue;
        }

        if oldtrig_tgisinternal {
            // system trigger ... ok to process?
            if skip_system {
                continue;
            }
            if !backend_utils_init_miscinit_seams::superuser::call(mcx)? {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg(format!(
                        "permission denied: \"{oldtrig_tgname}\" is a system trigger"
                    ))
                    .into_error());
            }
        }

        found = true;

        if oldtrig_tgenabled != fires_when {
            // need to change this one ... make a copy to scribble on
            // newtrig->tgenabled = fires_when;
            // CatalogTupleUpdate(tgrel, &newtup->t_self, newtup);
            let fields = pt::TriggerFieldUpdate {
                tgname: None,
                tgparentid: None,
                tgdeferrable: None,
                tginitdeferred: None,
                tgenabled: Some(fires_when),
            };
            indexing::catalog_tuple_update_pg_trigger::call(&tgrel, tuple.tuple.t_self, &fields)?;
            changed = true;
        }

        // When altering FOR EACH ROW triggers on a partitioned table, do the
        // same on the partitions as well, unless ONLY is specified.
        //
        // Note that we recurse even if we didn't change the trigger above,
        // because the partitions' copy of the trigger may have a different
        // value of tgenabled than the parent's trigger and thus might need to
        // be changed.
        if recurse
            && rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
            && pt::TRIGGER_FOR_ROW(oldtrig_tgtype)
        {
            let partdesc =
                backend_partitioning_partdesc::RelationGetPartitionDesc(mcx, rel, true)?;
            for i in 0..partdesc.nparts as usize {
                // part = relation_open(partdesc->oids[i], lockmode);
                let part = backend_access_table_table_seams::table_open::call(
                    mcx,
                    partdesc.oids[i],
                    lockmode,
                )?;
                // Match on child triggers' tgparentid, not their name.
                EnableDisableTrigger(
                    mcx,
                    &part,
                    None,
                    oldtrig_oid,
                    fires_when,
                    skip_system,
                    recurse,
                    lockmode,
                )?;
                // table_close(part, NoLock); -- keep lock till commit
                part.close(types_storage::lock::NoLock)?;
            }
        }

        // InvokeObjectPostAlterHook(TriggerRelationId, oldtrig->oid, 0);
        backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
            pt::TriggerRelationId,
            oldtrig_oid,
            0,
        )?;
    }

    // systable_endscan(tgscan);
    let _ = tgscan;
    // table_close(tgrel, RowExclusiveLock);
    tgrel.close(RowExclusiveLock)?;

    if tgname.is_some() && !found {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "trigger \"{}\" for table \"{}\" does not exist",
                tgname.unwrap(),
                rel.name()
            ))
            .into_error());
    }

    // If we changed anything, broadcast an SI inval message to force each
    // backend (including our own!) to rebuild relation's relcache entry.
    if changed {
        backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcache(rel)?;
    }

    Ok(())
}

/// `ATExecEnableDisableTrigger(rel, trigname, fires_when, skip_system, recurse,
/// lockmode)` (commands/tablecmds.c:17204-17215) — the ALTER TABLE wrapper that
/// dispatches to `EnableDisableTrigger` then fires the post-alter hook on the
/// table itself.
#[allow(clippy::too_many_arguments)]
pub fn ATExecEnableDisableTrigger<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigname: Option<&str>,
    fires_when: i8,
    skip_system: bool,
    recurse: bool,
    lockmode: i32,
) -> PgResult<()> {
    // EnableDisableTrigger(rel, trigname, InvalidOid, fires_when, skip_system,
    //                      recurse, lockmode);
    EnableDisableTrigger(
        mcx,
        rel,
        trigname,
        0, // InvalidOid
        fires_when,
        skip_system,
        recurse,
        lockmode,
    )?;

    // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        RelationRelationId,
        rel.rd_id,
        0,
    )?;

    Ok(())
}
