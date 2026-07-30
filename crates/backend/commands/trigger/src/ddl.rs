// CreateTrigger / RemoveTriggerById / get_trigger_oid / renametrig
// (trigger.c), incl. partitioned-table rename recursion. LOUD: non-superuser
// owner checks.
use datum::Datum;
use mcx::Mcx;
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
use types_core::{InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
    NOTICE,
};
use types_nodes::parsenodes::RenameStmt;
use types_nodes::rawnodes::CreateTrigStmt;
use types_rel::{
    AccessExclusiveLock, NoLock, Relation, RowExclusiveLock, RELKIND_FOREIGN_TABLE,
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};
use types_trigger::TRIGGER_FIRES_ON_ORIGIN;

use crate::catalog::{
    name_arg, relkind_not_supported_detail, scan_key, CreateTriggerFiringOn,
    TRIGGER_OID_INDEX_ID, TRIGGER_RELATION_ID, TRIGGER_RELID_NAME_INDEX_ID,
};

const Anum_pg_trigger_oid: i32 = 1;
const Anum_pg_trigger_tgrelid: i32 = 2;
const Anum_pg_trigger_tgparentid: i32 = 3;
const Anum_pg_trigger_tgname: i32 = 4;

#[track_caller]
#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

pub fn CreateTrigger<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTrigStmt<'mcx>,
    query_string: &str,
) -> PgResult<Oid> {
    CreateTriggerFiringOn(
        mcx,
        stmt,
        Some(query_string),
        InvalidOid,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        None,
        false,
        false,
        TRIGGER_FIRES_ON_ORIGIN,
    )
}

pub fn get_trigger_oid<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    trigname: &str,
    missing_ok: bool,
) -> PgResult<Oid> {
    let tgrel = table::table_open(mcx, TRIGGER_RELATION_ID, types_rel::AccessShareLock)?;
    let cname = name_arg(mcx, trigname)?;
    let keys = [
        scan_key(2, F_OIDEQ, Datum::from_oid(relid)),
        scan_key(4, F_NAMEEQ, Datum::from_usize(cname.as_ptr() as usize)),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &tgrel, TRIGGER_RELID_NAME_INDEX_ID, true, None, &keys)?;
    let oid = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => {
            let mut isnull = false;
            // SAFETY: NOT NULL pg_trigger oid column under its descriptor.
            unsafe { types_tuple::heap_getattr(tup, Anum_pg_trigger_oid, tgrel.descr(), &mut isnull) }
                .as_oid()
        }
        None => {
            if !missing_ok {
                let relname = lsyscache::get_rel_name(mcx, relid)?
                    .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
                return Err(err(
                    format!(
                        "trigger \"{trigname}\" for table \"{}\" does not exist",
                        relname.as_str()
                    ),
                    ERRCODE_UNDEFINED_OBJECT,
                ));
            }
            InvalidOid
        }
    };
    genam::systable_endscan(mcx, scan)?;
    tgrel.close(types_rel::AccessShareLock)?;
    Ok(oid)
}

pub fn RemoveTriggerById<'mcx>(mcx: Mcx<'mcx>, trig_oid: Oid) -> PgResult<()> {
    let tgrel = table::table_open(mcx, TRIGGER_RELATION_ID, RowExclusiveLock)?;
    let key = scan_key(1, F_OIDEQ, Datum::from_oid(trig_oid));
    let mut scan = genam::systable_beginscan(
        mcx,
        &tgrel,
        TRIGGER_OID_INDEX_ID,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("could not find tuple for trigger {trig_oid}"));
    let mut isnull = false;
    // SAFETY: NOT NULL pg_trigger tgrelid column under its descriptor.
    let relid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_trigger_tgrelid, tgrel.descr(), &mut isnull)
    }
    .as_oid();

    let rel = table::table_open(mcx, relid, AccessExclusiveLock)?;
    match rel.rd_rel.relkind {
        RELKIND_RELATION | b'v' | RELKIND_FOREIGN_TABLE | RELKIND_PARTITIONED_TABLE => {}
        other => {
            return Err(Box::new(
                (*err(
                    format!("relation \"{}\" cannot have triggers", rel.name()),
                    ERRCODE_WRONG_OBJECT_TYPE,
                ))
                .with_detail(relkind_not_supported_detail(other as u8).to_string()),
            ));
        }
    }
    if !init_small::globals::allowSystemTableMods() && catalog::IsSystemRelation(&rel) {
        return Err(err(
            format!("permission denied: \"{}\" is a system catalog", rel.name()),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&tgrel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    tgrel.close(RowExclusiveLock)?;

    // C leaves relhastriggers set; a relcache inval rebuilds trigdescs.
    inval::invalidate::CacheInvalidateRelcacheByRelid(relid)?;
    rel.close(NoLock)?;
    Ok(())
}

fn name_datum_str<'a>(d: Datum) -> &'a str {
    // SAFETY: a non-null pg_trigger tgname column is a 64-byte NameData image.
    let bytes = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 64) };
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(64);
    core::str::from_utf8(&bytes[..len]).expect("non-UTF-8 tgname")
}

// renametrig (trigger.c). The RangeVarCallbackForRenameTrigger owner check is
// the superuser fast path; relkind is re-checked on the opened rel.
pub fn renametrig<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'mcx>) -> PgResult<()> {
    if !superuser::superuser_arg(miscinit::GetUserId())? {
        // unported: ALTER TRIGGER owner check for non-superusers
        return Err(err(
            "ALTER TRIGGER ... RENAME as a non-superuser is not supported yet".to_string(),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }
    let rvn = stmt.relation.expect("RenameStmt.relation");
    let rv = rel_vocab::RangeVar {
        catalogname: rvn.catalogname,
        schemaname: rvn.schemaname,
        relname: rvn.relname.expect("RangeVar.relname"),
        inh: rvn.inh,
        relpersistence: rvn.relpersistence,
        location: rvn.location,
    };
    let subname = stmt.subname.expect("RenameStmt.subname");
    let newname = stmt.newname.expect("RenameStmt.newname");

    let targetrel = table::table_openrv(mcx, &rv, AccessExclusiveLock)?;
    match targetrel.rd_rel.relkind {
        RELKIND_RELATION | b'v' | RELKIND_FOREIGN_TABLE | RELKIND_PARTITIONED_TABLE => {}
        other => {
            return Err(Box::new(
                (*err(
                    format!("relation \"{}\" cannot have triggers", rv.relname),
                    ERRCODE_WRONG_OBJECT_TYPE,
                ))
                .with_detail(relkind_not_supported_detail(other as u8).to_string()),
            ));
        }
    }
    if targetrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        pg_inherits::find_all_inheritors(mcx, targetrel.rd_id, AccessExclusiveLock)?;
    }

    let tgrel = table::table_open(mcx, TRIGGER_RELATION_ID, RowExclusiveLock)?;

    let cname = name_arg(mcx, subname)?;
    let keys = [
        scan_key(2, F_OIDEQ, Datum::from_oid(targetrel.rd_id)),
        scan_key(4, F_NAMEEQ, Datum::from_usize(cname.as_ptr() as usize)),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &tgrel, TRIGGER_RELID_NAME_INDEX_ID, true, None, &keys)?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(err(
            format!(
                "trigger \"{subname}\" for table \"{}\" does not exist",
                targetrel.name()
            ),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    };
    let td = tgrel.descr();
    let mut isnull = false;
    // SAFETY (each): NOT NULL pg_trigger columns under its own descriptor.
    let (tgoid, tgparentid) = unsafe {
        (
            types_tuple::heap_getattr(tup, Anum_pg_trigger_oid, td, &mut isnull).as_oid(),
            types_tuple::heap_getattr(tup, Anum_pg_trigger_tgparentid, td, &mut isnull).as_oid(),
        )
    };
    genam::systable_endscan(mcx, scan)?;
    if tgparentid != InvalidOid {
        let parent_relid = pg_inherits::get_partition_parent(mcx, targetrel.rd_id, false)?;
        let parent_name = lsyscache::get_rel_name(mcx, parent_relid)?
            .unwrap_or_else(|| panic!("cache lookup failed for relation {parent_relid}"));
        return Err(Box::new(
            (*err(
                format!(
                    "cannot rename trigger \"{subname}\" on table \"{}\"",
                    targetrel.name()
                ),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ))
            .with_hint(format!(
                "Rename the trigger on the partitioned table \"{}\" instead.",
                parent_name.as_str()
            )),
        ));
    }

    renametrig_internal(mcx, &tgrel, &targetrel, subname, newname, subname)?;

    if targetrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let partdesc = partdesc::RelationGetPartitionDesc(&targetrel, true)?;
        for i in 0..partdesc.nparts {
            renametrig_partition(mcx, &tgrel, partdesc.oids[i], tgoid, newname, subname)?;
        }
    }

    tgrel.close(RowExclusiveLock)?;
    targetrel.close(NoLock)?;
    Ok(())
}

fn renametrig_internal<'mcx>(
    mcx: Mcx<'mcx>,
    tgrel: &Relation<'mcx>,
    targetrel: &Relation<'mcx>,
    actual_name: &str,
    newname: &str,
    expected_name: &str,
) -> PgResult<()> {
    if actual_name == newname {
        return Ok(());
    }

    let newcname = name_arg(mcx, newname)?;
    let dupkeys = [
        scan_key(2, F_OIDEQ, Datum::from_oid(targetrel.rd_id)),
        scan_key(4, F_NAMEEQ, Datum::from_usize(newcname.as_ptr() as usize)),
    ];
    let mut dupscan =
        genam::systable_beginscan(mcx, tgrel, TRIGGER_RELID_NAME_INDEX_ID, true, None, &dupkeys)?;
    if genam::systable_getnext(mcx, &mut dupscan)?.is_some() {
        return Err(err(
            format!(
                "trigger \"{newname}\" for relation \"{}\" already exists",
                targetrel.name()
            ),
            ERRCODE_DUPLICATE_OBJECT,
        ));
    }
    genam::systable_endscan(mcx, dupscan)?;

    if actual_name != expected_name {
        elog_seams::ereport::call(PgError::new(
            NOTICE,
            format!(
                "renamed trigger \"{actual_name}\" on relation \"{}\"",
                targetrel.name()
            ),
        ))?;
    }

    let oldcname = name_arg(mcx, actual_name)?;
    let keys = [
        scan_key(2, F_OIDEQ, Datum::from_oid(targetrel.rd_id)),
        scan_key(4, F_NAMEEQ, Datum::from_usize(oldcname.as_ptr() as usize)),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, tgrel, TRIGGER_RELID_NAME_INDEX_ID, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("trigger \"{actual_name}\" vanished during rename"));
    let td = tgrel.descr();
    let natts = td.natts as usize;
    let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[3] = Datum::from_usize(newcname.as_ptr() as usize);
    repl[3] = true;
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, td, &repl_values, &repl_isnull, &repl)?;
    let tid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, tgrel, &tid, &mut newtup)?;

    inval::invalidate::CacheInvalidateRelcacheByRelid(targetrel.rd_id)?;
    Ok(())
}

fn renametrig_partition<'mcx>(
    mcx: Mcx<'mcx>,
    tgrel: &Relation<'mcx>,
    partition_id: Oid,
    parent_trigger_oid: Oid,
    newname: &str,
    expected_name: &str,
) -> PgResult<()> {
    let key = scan_key(2, F_OIDEQ, Datum::from_oid(partition_id));
    let mut scan = genam::systable_beginscan(
        mcx,
        tgrel,
        TRIGGER_RELID_NAME_INDEX_ID,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let td = tgrel.descr();
    let mut found: Option<(Oid, mcx::PgString<'mcx>)> = None;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): NOT NULL pg_trigger columns under its own descriptor.
        let (tgparentid, tgoid, tgname) = unsafe {
            (
                types_tuple::heap_getattr(tup, Anum_pg_trigger_tgparentid, td, &mut isnull)
                    .as_oid(),
                types_tuple::heap_getattr(tup, Anum_pg_trigger_oid, td, &mut isnull).as_oid(),
                name_datum_str(types_tuple::heap_getattr(tup, Anum_pg_trigger_tgname, td, &mut isnull)),
            )
        };
        if tgparentid != parent_trigger_oid {
            continue;
        }
        found = Some((tgoid, mcx::PgString::from_str_in(tgname, mcx)?));
        break;
    }
    genam::systable_endscan(mcx, scan)?;
    let Some((child_oid, child_name)) = found else {
        return Ok(());
    };

    let partition_rel = table::table_open(mcx, partition_id, NoLock)?;
    renametrig_internal(mcx, tgrel, &partition_rel, child_name.as_str(), newname, expected_name)?;
    if partition_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let partdesc = partdesc::RelationGetPartitionDesc(&partition_rel, true)?;
        for i in 0..partdesc.nparts {
            renametrig_partition(
                mcx,
                tgrel,
                partdesc.oids[i],
                child_oid,
                newname,
                child_name.as_str(),
            )?;
        }
    }
    partition_rel.close(NoLock)?;
    Ok(())
}

const Anum_pg_trigger_tgtype: i32 = 6;
const Anum_pg_trigger_tgenabled: i32 = 7;
const Anum_pg_trigger_tgisinternal: i32 = 8;

// EnableDisableTrigger (trigger.c): tgname None = all triggers; tgparent
// filters partition clones; recursion follows FOR EACH ROW triggers into
// partitions.
#[allow(clippy::too_many_arguments)]
pub fn EnableDisableTrigger<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tgname: Option<&str>,
    tgparent: Oid,
    fires_when: i8,
    skip_system: bool,
    recurse: bool,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<()> {
    let tgrel = table::table_open(mcx, TRIGGER_RELATION_ID, RowExclusiveLock)?;
    let mut keys = vec![scan_key(2, F_OIDEQ, Datum::from_oid(rel.rd_id))];
    let cname;
    if let Some(name) = tgname {
        cname = name_arg(mcx, name)?;
        keys.push(scan_key(4, F_NAMEEQ, Datum::from_usize(cname.as_ptr() as usize)));
    }
    let mut scan =
        genam::systable_beginscan(mcx, &tgrel, TRIGGER_RELID_NAME_INDEX_ID, true, None, &keys)?;
    let td = tgrel.descr();
    let mut found = false;
    let mut changed = false;
    struct Hit {
        oid: Oid,
        tgtype: i16,
        enabled: i8,
    }
    let mut hits: Vec<Hit> = Vec::new();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): NOT NULL pg_trigger columns under its own descriptor.
        let (tgparentid, oid, tgtype, enabled, isinternal, name) = unsafe {
            (
                types_tuple::heap_getattr(tup, Anum_pg_trigger_tgparentid, td, &mut isnull)
                    .as_oid(),
                types_tuple::heap_getattr(tup, Anum_pg_trigger_oid, td, &mut isnull).as_oid(),
                types_tuple::heap_getattr(tup, Anum_pg_trigger_tgtype, td, &mut isnull).as_i16(),
                types_tuple::heap_getattr(tup, Anum_pg_trigger_tgenabled, td, &mut isnull)
                    .as_i8(),
                types_tuple::heap_getattr(tup, Anum_pg_trigger_tgisinternal, td, &mut isnull)
                    .as_bool(),
                name_datum_str(types_tuple::heap_getattr(
                    tup,
                    Anum_pg_trigger_tgname,
                    td,
                    &mut isnull,
                )),
            )
        };
        if tgparent != InvalidOid && tgparent != tgparentid {
            continue;
        }
        if isinternal {
            if skip_system {
                continue;
            }
            if !superuser::superuser_arg(miscinit::GetUserId())? {
                return Err(err(
                    format!("permission denied: \"{name}\" is a system trigger"),
                    ERRCODE_INSUFFICIENT_PRIVILEGE,
                ));
            }
        }
        found = true;
        hits.push(Hit { oid, tgtype, enabled });
    }
    genam::systable_endscan(mcx, scan)?;

    for hit in &hits {
        if hit.enabled != fires_when {
            let key = scan_key(1, F_OIDEQ, Datum::from_oid(hit.oid));
            let mut oscan = genam::systable_beginscan(
                mcx,
                &tgrel,
                TRIGGER_OID_INDEX_ID,
                true,
                None,
                core::slice::from_ref(&key),
            )?;
            let tup = genam::systable_getnext(mcx, &mut oscan)?
                .unwrap_or_else(|| panic!("could not find tuple for trigger {}", hit.oid));
            let natts = td.natts as usize;
            let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            repl_values.resize(natts, Datum::null());
            repl_isnull.resize(natts, false);
            repl.resize(natts, false);
            repl_values[(Anum_pg_trigger_tgenabled - 1) as usize] =
                Datum::from_i8(fires_when);
            repl[(Anum_pg_trigger_tgenabled - 1) as usize] = true;
            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, tup, td, &repl_values, &repl_isnull, &repl)?;
            let tid = tup.t_self;
            genam::systable_endscan(mcx, oscan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &tgrel, &tid, &mut newtup)?;
            changed = true;
        }
        if recurse
            && rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
            && hit.tgtype & types_trigger::TRIGGER_TYPE_ROW != 0
        {
            let partdesc = partdesc::RelationGetPartitionDesc(rel, true)?;
            for i in 0..partdesc.nparts {
                let part = table::table_open(mcx, partdesc.oids[i], lockmode)?;
                EnableDisableTrigger(
                    mcx,
                    &part,
                    None,
                    hit.oid,
                    fires_when,
                    skip_system,
                    recurse,
                    lockmode,
                )?;
                part.close(NoLock)?;
            }
        }
    }
    tgrel.close(RowExclusiveLock)?;

    if tgname.is_some() && !found {
        return Err(err(
            format!(
                "trigger \"{}\" for table \"{}\" does not exist",
                tgname.expect("checked"),
                rel.name()
            ),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    }
    if changed {
        inval::invalidate::CacheInvalidateRelcacheByRelid(rel.rd_id)?;
    }
    Ok(())
}
