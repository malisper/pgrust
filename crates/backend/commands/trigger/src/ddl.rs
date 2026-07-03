// CreateTrigger / RemoveTriggerById / get_trigger_oid / renametrig
// (trigger.c) and the DROP TRIGGER slice of RemoveObjects (dropcmds.c) +
// get_object_address_relobject (objectaddress.c). LOUD: partitioned-table
// recursion, non-superuser owner checks.
use datum::Datum;
use mcx::Mcx;
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
use types_core::{InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_UNDEFINED_OBJECT, ERROR, NOTICE,
};
use types_nodes::parsenodes::{DropStmt, RenameStmt};
use types_nodes::rawnodes::CreateTrigStmt;
use types_rel::{
    AccessExclusiveLock, NoLock, RowExclusiveLock, RELKIND_RELATION,
};
use types_trigger::TRIGGER_FIRES_ON_ORIGIN;

use crate::catalog::{
    name_arg, scan_key, CreateTriggerFiringOn, TRIGGER_OID_INDEX_ID, TRIGGER_RELATION_ID,
    TRIGGER_RELID_NAME_INDEX_ID,
};

const Anum_pg_trigger_oid: i32 = 1;
const Anum_pg_trigger_tgrelid: i32 = 2;
const Anum_pg_trigger_tgparentid: i32 = 3;

#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

pub fn CreateTrigger<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTrigStmt<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    CreateTriggerFiringOn(
        mcx,
        stmt,
        Some(query_string),
        InvalidOid,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        false,
        TRIGGER_FIRES_ON_ORIGIN,
    )?;
    Ok(())
}

// RemoveObjects (dropcmds.c), OBJECT_TRIGGER arm over
// get_object_address_relobject: each object is [rel name parts..., trigname].
pub fn RemoveTriggers<'mcx>(mcx: Mcx<'mcx>, stmt: &DropStmt<'mcx>) -> PgResult<()> {
    for object in stmt.objects.iter() {
        let names = object.as_list().expect("DROP TRIGGER object list");
        let nnames = names.len();
        assert!((2..=4).contains(&nnames), "improper qualified name for DROP TRIGGER");
        let trigname = names.nth(nnames - 1).as_string().expect("trigger name").sval;
        let mut relbuf = [""; 3];
        for i in 0..nnames - 1 {
            relbuf[i] = names.nth(i).as_string().expect("relation name").sval;
        }
        let relnames = &relbuf[..nnames - 1];
        let (schemaname, relname) = match relnames {
            [r] => (None, *r),
            [s, r] => (Some(*s), *r),
            _ => panic!("unported: cross-database DROP TRIGGER qualification"),
        };
        let rv = rel_vocab::RangeVar {
            catalogname: None,
            schemaname,
            relname,
            inh: true,
            relpersistence: b'p',
            location: -1,
        };
        let display = {
            let mut s = String::new();
            for (i, part) in relnames.iter().enumerate() {
                if i > 0 {
                    s.push('.');
                }
                s.push_str(part);
            }
            s
        };
        let rel = table::table_openrv_extended(mcx, &rv, AccessExclusiveLock, stmt.missing_ok)?;
        let Some(rel) = rel else {
            elog_seams::ereport_msg::call(
                NOTICE,
                format!(
                    "trigger \"{trigname}\" for relation \"{display}\" does not exist, skipping"
                ),
                None,
            )?;
            continue;
        };
        let tgoid = get_trigger_oid(mcx, rel.rd_id, trigname, stmt.missing_ok)?;
        rel.close(NoLock)?;
        if tgoid == InvalidOid {
            elog_seams::ereport_msg::call(
                NOTICE,
                format!(
                    "trigger \"{trigname}\" for relation \"{display}\" does not exist, skipping"
                ),
                None,
            )?;
            continue;
        }
        dependency_seams::perform_deletion::call(
            mcx,
            TRIGGER_RELATION_ID,
            tgoid,
            0,
            stmt.behavior,
            0,
        )?;
    }
    Ok(())
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
    if rel.rd_rel.relkind != RELKIND_RELATION {
        panic!(
            "unported: RemoveTriggerById relkind '{}' (views/foreign/partitioned)",
            rel.rd_rel.relkind as u8 as char
        );
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

// renametrig (trigger.c). The RangeVarCallbackForRenameTrigger owner check is
// the superuser fast path; relkind is re-checked on the opened rel.
pub fn renametrig<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'mcx>) -> PgResult<()> {
    if !superuser::superuser_arg(miscinit::GetUserId())? {
        panic!("unported: ALTER TRIGGER owner check for non-superusers");
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
    if targetrel.rd_rel.relkind != RELKIND_RELATION {
        panic!("unported: ALTER TRIGGER on non-plain relation");
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
    // SAFETY: NOT NULL pg_trigger tgparentid column under its descriptor.
    let tgparentid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_trigger_tgparentid, td, &mut isnull)
    }
    .as_oid();
    if tgparentid != InvalidOid {
        panic!("unported: renaming a partition-cloned trigger");
    }
    // renametrig_internal: the tuple carries the expected name (the scan
    // matched it), so the differing-name NOTICE arm is unreachable here.
    if subname == newname {
        genam::systable_endscan(mcx, scan)?;
        tgrel.close(RowExclusiveLock)?;
        targetrel.close(NoLock)?;
        return Ok(());
    }
    genam::systable_endscan(mcx, scan)?;

    let newcname = name_arg(mcx, newname)?;
    let dupkeys = [
        scan_key(2, F_OIDEQ, Datum::from_oid(targetrel.rd_id)),
        scan_key(4, F_NAMEEQ, Datum::from_usize(newcname.as_ptr() as usize)),
    ];
    let mut dupscan =
        genam::systable_beginscan(mcx, &tgrel, TRIGGER_RELID_NAME_INDEX_ID, true, None, &dupkeys)?;
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

    // Re-fetch the row for a modifiable copy (the first scan is closed).
    let mut scan2 =
        genam::systable_beginscan(mcx, &tgrel, TRIGGER_RELID_NAME_INDEX_ID, true, None, &keys)?;
    let tup2 = genam::systable_getnext(mcx, &mut scan2)?
        .unwrap_or_else(|| panic!("trigger \"{subname}\" vanished during rename"));
    let natts = td.natts as usize;
    let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[3] = Datum::from_usize(newcname.as_ptr() as usize);
    repl[3] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, tup2, td, &repl_values, &repl_isnull, &repl)?;
    let tid2 = tup2.t_self;
    genam::systable_endscan(mcx, scan2)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &tgrel, &tid2, &mut newtup)?;

    tgrel.close(RowExclusiveLock)?;
    inval::invalidate::CacheInvalidateRelcacheByRelid(targetrel.rd_id)?;
    targetrel.close(NoLock)?;
    Ok(())
}
