#![allow(non_upper_case_globals)]
// ExecuteTruncate/ExecuteTruncateGuts: RESTRICT, CASCADE, RESTART IDENTITY.
use datum::Datum;
use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE};
use types_nodes::parsenodes::{DropBehavior, ObjectType, TruncateStmt};
use types_rel::{
    AccessExclusiveLock, NoLock, Relation, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

const RM_HEAP_ID: u8 = 10;
const XLOG_HEAP_TRUNCATE: u8 = 0x30;
const XLH_TRUNCATE_CASCADE: u8 = 1 << 0;
const XLH_TRUNCATE_RESTART_SEQS: u8 = 1 << 1;
const XLOG_INCLUDE_ORIGIN: u8 = 0x01;
// offsetof(xl_heap_truncate, relids): dbId(4) + nrelids(4) + flags(1) + pad(3).
const SizeOfHeapTruncate: usize = 12;

fn oid_key(attno: AttrNumber, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

pub fn ExecuteTruncate<'mcx>(mcx: Mcx<'mcx>, stmt: &TruncateStmt<'mcx>) -> PgResult<()> {
    let mut rels: Vec<Relation<'mcx>> = Vec::new();
    let mut relids: Vec<Oid> = Vec::new();
    let mut relids_logged: Vec<Oid> = Vec::new();

    for cell in stmt.relations.iter() {
        let rv = cell.as_range_var().expect("TRUNCATE target is a RangeVar");
        let rv = rel_vocab::RangeVar {
            catalogname: rv.catalogname,
            schemaname: rv.schemaname,
            relname: rv.relname.expect("relation_expr always carries relname"),
            inh: rv.inh,
            relpersistence: rv.relpersistence,
            location: rv.location,
        };

        let mut callback = |_rv: &rel_vocab::RangeVar<'_>, relOid: Oid, _old: Oid| {
            RangeVarCallbackForTruncate(mcx, relOid)
        };
        let myrelid = catalog_namespace::RangeVarGetRelidExtended(
            &rv,
            AccessExclusiveLock,
            0,
            Some(&mut callback),
        )?;

        if relids.contains(&myrelid) {
            continue;
        }
        let rel = table::table_open(mcx, myrelid, NoLock)?;
        truncate_check_activity(&rel)?;

        if heapam::relation_is_logically_logged(&rel) {
            relids_logged.push(myrelid);
        }
        rels.push(rel);
        relids.push(myrelid);

        if rv.inh {
            let children = pg_inherits::find_all_inheritors(mcx, myrelid, AccessExclusiveLock)?;
            for &childrelid in children.iter() {
                if relids.contains(&childrelid) {
                    continue;
                }
                let child = table::table_open(mcx, childrelid, NoLock)?;
                // Other sessions' temp children cannot be processed; C skips
                // them in the recursion (ExecuteTruncate, tablecmds.c).
                if child.rd_rel.relpersistence == types_core::RELPERSISTENCE_TEMP
                    && !child.rd_islocaltemp
                {
                    child.close(AccessExclusiveLock)?;
                    continue;
                }
                // Inherited TRUNCATE checks permissions on the parent only.
                truncate_check_rel(
                    mcx,
                    childrelid,
                    child.rd_rel.relkind,
                    child.namespace(),
                    child.name(),
                )?;
                truncate_check_activity(&child)?;
                if heapam::relation_is_logically_logged(&child) {
                    relids_logged.push(childrelid);
                }
                rels.push(child);
                relids.push(childrelid);
            }
        } else if rels.last().expect("just pushed").rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            return Err(Box::new(
                PgError::new(ERROR, "cannot truncate only a partitioned table".to_string())
                    .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                    .with_hint(
                        "Do not specify the ONLY keyword, or use TRUNCATE ONLY on the partitions directly."
                            .to_string(),
                    ),
            ));
        }
    }

    let n_explicit = rels.len();
    ExecuteTruncateGuts(
        mcx,
        &mut rels,
        &mut relids,
        &mut relids_logged,
        stmt.behavior,
        stmt.restart_seqs,
        false,
    )?;

    debug_assert_eq!(rels.len(), n_explicit);
    for rel in rels {
        rel.close(NoLock)?;
    }
    Ok(())
}

// pub: C exports it in tablecmds.h for logical apply's apply_handle_truncate
// (worker.c) — same linkage here.
pub fn ExecuteTruncateGuts<'mcx>(
    mcx: Mcx<'mcx>,
    rels: &mut Vec<Relation<'mcx>>,
    relids: &mut Vec<Oid>,
    relids_logged: &mut Vec<Oid>,
    behavior: DropBehavior,
    restart_seqs: bool,
    run_as_table_owner: bool,
) -> PgResult<()> {
    let n_explicit = rels.len();

    if behavior == DropBehavior::DROP_CASCADE {
        loop {
            let newrelids = catalog_heap::heap_truncate_find_FKs(mcx, relids)?;
            if newrelids.is_empty() {
                break;
            }
            for &relid in newrelids.iter() {
                let rel = table::table_open(mcx, relid, AccessExclusiveLock)?;
                elog_seams::ereport::call(PgError::new(
                    NOTICE,
                    format!("truncate cascades to table \"{}\"", rel.name()),
                ))?;
                truncate_check_rel(mcx, relid, rel.rd_rel.relkind, rel.namespace(), rel.name())?;
                truncate_check_perms(relid, rel.rd_rel.relkind, rel.name())?;
                truncate_check_activity(&rel)?;
                if heapam::relation_is_logically_logged(&rel) {
                    relids_logged.push(relid);
                }
                rels.push(rel);
                relids.push(relid);
            }
        }
    }

    if behavior == DropBehavior::DROP_RESTRICT {
        catalog_heap::heap_truncate_check_FKs(mcx, rels, false)?;
    }

    // AccessExclusiveLock: ResetSequence needs it; permissions fail before
    // any truncation work.
    let mut seq_relids: Vec<Oid> = Vec::new();
    if restart_seqs {
        for rel in rels.iter() {
            for &seq_relid in pg_depend::getOwnedSequences(mcx, rel.rd_id)?.iter() {
                let seq_rel =
                    relation_seams::relation_open::call(mcx, seq_relid, AccessExclusiveLock)?;
                // This check must match AlterSequence!
                if !aclchk::object_ownercheck(
                    RELATION_RELATION_ID,
                    seq_relid,
                    miscinit::GetUserId(),
                )? {
                    aclchk::aclcheck_error(
                        aclchk::ACLCHECK_NOT_OWNER,
                        ObjectType::OBJECT_SEQUENCE,
                        seq_rel.name(),
                    )?;
                }
                seq_relids.push(seq_relid);
                seq_rel.close(NoLock)?;
            }
        }
    }

    // The BS/AS TRUNCATE trigger bracket (C creates an EState + relinfos;
    // the caches below are its per-statement ri_TrigFunctions/WhenExprs).
    let mut trig_state: Vec<Option<(
        std::rc::Rc<types_trigger::TriggerDesc<'static>>,
        trigger::TriggerFmgrCache,
        trigger::TriggerWhenCache<'mcx>,
    )>> = Vec::with_capacity(rels.len());
    let mut any_triggers = false;
    for rel in rels.iter() {
        let entry = if rel.rd_hastriggers {
            relcache::RelationGetTriggerDesc(rel.rd_id)?.map(|d| {
                any_triggers = true;
                (d, trigger::TriggerFmgrCache::default(), trigger::TriggerWhenCache::default())
            })
        } else {
            None
        };
        trig_state.push(entry);
    }
    if any_triggers {
        trigger::AfterTriggerBeginQuery();
        for (rel, entry) in rels.iter().zip(trig_state.iter_mut()) {
            if let Some((td, fmgr, when_cache)) = entry.as_mut() {
                // Fire BEFORE STATEMENT triggers as the table owner when asked
                // (tablecmds.c:2139, logical replication apply).
                let ucxt = switch_to_table_owner(mcx, rel, run_as_table_owner)?;
                let mut when =
                    trigger::TriggerWhenEval { mcx, cache: when_cache, modified_cols: None };
                let r = trigger::ExecBSTruncateTriggers(mcx, rel, td, fmgr, &mut when);
                restore_user_context(&ucxt)?;
                r?;
            }
        }
    }

    let my_subid = xact::GetCurrentSubTransactionId();
    // ForeignTruncateInfo (tablecmds.c:2168-2201): the foreign tables of each
    // foreign server, so every server truncates its tables in one callback.
    // C keys a hash table by server OID; first-seen order stands in for its
    // iteration order (one server = one entry either way).
    let mut ft_groups: Vec<(Oid, Vec<&Relation<'mcx>>)> = Vec::new();
    for rel in rels.iter() {
        if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            continue;
        }
        if rel.rd_rel.relkind == types_rel::RELKIND_FOREIGN_TABLE {
            let serverid = foreigncmds::foreign::GetForeignServerIdByRelId(rel.rd_id)?;
            match ft_groups.iter_mut().find(|(sid, _)| *sid == serverid) {
                Some((_, group)) => group.push(rel),
                None => ft_groups.push((serverid, vec![rel])),
            }
            continue;
        }
        if rel.rd_createSubid.get() == my_subid
            || rel.rd_newRelfilelocatorSubid.get() == my_subid
        {
            catalog_heap::heap_truncate_one_rel(mcx, rel)?;
        } else {
            predicate_seams::check_table_for_serializable_conflict_in::call(rel)?;
            catalog_index::RelationSetNewRelfilenumber(mcx, rel, rel.rd_rel.relpersistence)?;

            let heap_relid = rel.rd_id;
            let toast_relid = rel.rd_rel.reltoastrelid;
            if toast_relid != InvalidOid {
                let toastrel = table::table_open(mcx, toast_relid, AccessExclusiveLock)?;
                catalog_index::RelationSetNewRelfilenumber(
                    mcx,
                    &toastrel,
                    toastrel.rd_rel.relpersistence,
                )?;
                toastrel.close(NoLock)?;
            }

            catalog_index::reindex_relation(
                mcx,
                heap_relid,
                catalog_index::REINDEX_REL_PROCESS_TOAST,
                &catalog_index::ReindexParams::default(),
                &mut |_index_id| {},
            )?;
        }
        pgstat::relation::pgstat_count_truncate(rel.rd_id, rel.rd_rel.relisshared);
    }
    // Now go through the groups and truncate foreign tables
    // (tablecmds.c:2266-2292): truncate_check_rel has already proved every
    // server's wrapper carries ExecForeignTruncate.
    for (serverid, group) in ft_groups.iter() {
        let kind = foreigncmds::foreign::GetFdwRoutineByServerId(mcx, *serverid)?;
        let routine = foreigncmds::fdw_truncate_routine(kind)
            .expect("truncate_check_rel() has checked that already");
        routine(mcx, group, behavior, restart_seqs)?;
    }
    for &seq_relid in seq_relids.iter() {
        sequence_seams::reset_sequence::call(mcx, seq_relid)?;
    }
    if !relids_logged.is_empty() {
        let mut xlrec = [0u8; SizeOfHeapTruncate];
        xlrec[0..4].copy_from_slice(&init_small::globals::MyDatabaseId().to_ne_bytes());
        xlrec[4..8].copy_from_slice(&(relids_logged.len() as u32).to_ne_bytes());
        let mut flags: u8 = 0;
        if behavior == DropBehavior::DROP_CASCADE {
            flags |= XLH_TRUNCATE_CASCADE;
        }
        if restart_seqs {
            flags |= XLH_TRUNCATE_RESTART_SEQS;
        }
        xlrec[8] = flags;
        let mut logrelids: Vec<u8> = Vec::with_capacity(relids_logged.len() * 4);
        for &relid in relids_logged.iter() {
            logrelids.extend_from_slice(&relid.to_ne_bytes());
        }
        xloginsert::insert_record(
            RM_HEAP_ID,
            XLOG_HEAP_TRUNCATE,
            XLOG_INCLUDE_ORIGIN,
            &[&xlrec, &logrelids],
            &[],
        )?;
    }

    if any_triggers {
        for (rel, entry) in rels.iter().zip(trig_state.iter_mut()) {
            if let Some((td, _, when_cache)) = entry.as_mut() {
                // AFTER STATEMENT triggers are queued as the table owner too
                // (tablecmds.c:2348).
                let ucxt = switch_to_table_owner(mcx, rel, run_as_table_owner)?;
                let mut when =
                    trigger::TriggerWhenEval { mcx, cache: when_cache, modified_cols: None };
                let r = trigger::ExecASTruncateTriggers(rel, td, Some(&mut when));
                restore_user_context(&ucxt)?;
                r?;
            }
        }
        trigger::AfterTriggerEndQuery(None)?;
    }

    for rel in rels.drain(n_explicit..) {
        rel.close(NoLock)?;
    }
    Ok(())
}

// ExecuteTruncateGuts's run_as_table_owner arm (tablecmds.c:2139/2348):
// SwitchToUntrustedUser(rel->rd_rel->relowner) around each trigger bracket.
fn switch_to_table_owner<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    run_as_table_owner: bool,
) -> PgResult<Option<types_core::UserContext>> {
    if !run_as_table_owner {
        return Ok(None);
    }
    let mut ucxt = types_core::UserContext::new(InvalidOid, 0, 0);
    init_small::SwitchToUntrustedUser(mcx, rel.rd_rel.relowner, &mut ucxt)?;
    Ok(Some(ucxt))
}

fn restore_user_context(ucxt: &Option<types_core::UserContext>) -> PgResult<()> {
    if let Some(ucxt) = ucxt {
        init_small::RestoreUserContext(ucxt)?;
    }
    Ok(())
}

// elog(ERROR, "cache lookup failed for relation %u") in
// RangeVarCallbackForTruncate (tablecmds.c): elog's default SQLSTATE is XX000.
#[cold]
#[inline(never)]
fn cache_lookup_failed(relOid: Oid) -> Box<PgError> {
    let msg = format!("cache lookup failed for relation {relOid}");
    Box::new(PgError::error(msg))
}

fn RangeVarCallbackForTruncate<'mcx>(mcx: Mcx<'mcx>, relOid: Oid) -> PgResult<()> {
    if relOid == InvalidOid {
        return Ok(());
    }
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
    let key = [oid_key(1, relOid)];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &key)?;
    // C's SearchSysCache1(RELOID) here runs *before* RangeVarGetRelidExtended
    // takes the AccessExclusiveLock (namespace.c invokes the callback ahead of
    // LockRelationOid), so a concurrent DROP TABLE that commits between the
    // name lookup and this probe genuinely reaches C's "should not happen"
    // elog(ERROR) -- a catchable XX000, not an abort.
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(cache_lookup_failed(relOid));
    };
    let desc = pg_class.descr();
    let get = |attnum: i32| {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_class columns under pg_class's descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
        debug_assert!(!isnull);
        d
    };
    let relname: String = {
        // SAFETY: relname is a NameData at attnum 2 in every pg_class row.
        let name = unsafe { &*(get(2).as_usize() as *const types_tuple::NameData) };
        String::from_utf8_lossy(name.name_str()).into_owned()
    };
    let relnamespace = get(3).as_oid();
    let relkind = get(18).as_i8() as u8;
    genam::systable_endscan(mcx, scan)?;
    pg_class.close(types_rel::AccessShareLock)?;

    truncate_check_rel(mcx, relOid, relkind, relnamespace, &relname)?;
    truncate_check_perms(relOid, relkind, &relname)
}

fn truncate_check_rel(
    mcx: Mcx<'_>,
    relid: Oid,
    relkind: u8,
    relnamespace: Oid,
    relname: &str,
) -> PgResult<()> {
    if relkind == types_rel::RELKIND_FOREIGN_TABLE {
        // Only foreign tables whose wrapper supports TRUNCATE
        // (fdwroutine->ExecForeignTruncate, tablecmds.c:2391-2401). C resolves
        // the routine first, so a handlerless wrapper errors "foreign-data
        // wrapper ... has no handler" before this 0A000.
        let serverid = foreigncmds::foreign::GetForeignServerIdByRelId(relid)?;
        let kind = foreigncmds::foreign::GetFdwRoutineByServerId(mcx, serverid)?;
        if foreigncmds::fdw_truncate_routine(kind).is_none() {
            return Err(Box::new(
                PgError::new(ERROR, format!("cannot truncate foreign table \"{relname}\""))
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
    } else if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        return Err(Box::new(
            PgError::new(ERROR, format!("\"{relname}\" is not a table"))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    // C exempts pg_largeobject during pg_upgrade (relfilenode carryover);
    // object-access hooks (InvokeObjectTruncateHook) are elided repo-wide.
    let is_system =
        catalog::IsCatalogRelationOid(relid) || catalog::IsToastNamespace(relnamespace);
    if is_system
        && !init_small::globals::allowSystemTableMods()
        && (!init_small::globals::IsBinaryUpgrade() || relid != catalog::LargeObjectRelationId)
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("permission denied: \"{relname}\" is a system catalog"),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

fn truncate_check_perms(relid: Oid, relkind: u8, relname: &str) -> PgResult<()> {
    let aclresult =
        aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), adt_acl::ACL_TRUNCATE)?;
    if aclresult != aclchk::ACLCHECK_OK {
        let _ = relkind; // get_relkind_objtype: both reachable relkinds map to OBJECT_TABLE
        aclchk_seams::aclcheck_error::call(aclresult, ObjectType::OBJECT_TABLE as i32, relname)?;
    }
    Ok(())
}

fn truncate_check_activity(rel: &Relation<'_>) -> PgResult<()> {
    if rel.is_other_temp() {
        return Err(Box::new(
            types_error::PgError::error("cannot truncate temporary tables of other sessions")
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    catalog_heap::CheckTableNotInUse(rel, "TRUNCATE")
}

#[cfg(test)]
mod panic_hygiene_tests {
    use super::*;

    // RangeVarGetRelidExtended runs the callback before it takes the
    // AccessExclusiveLock, so a DROP TABLE committing in that window makes the
    // pg_class probe come up empty.  C reports it with
    // elog(ERROR, "cache lookup failed for relation %u") -- catchable XX000.
    // pgrust used to panic!() here, aborting the backend (found by fuzzing).
    #[test]
    fn cache_lookup_failure_is_a_catchable_xx000() {
        let e = cache_lookup_failed(16384);
        assert_eq!(e.message(), "cache lookup failed for relation 16384");
        assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(e.level(), ERROR);
    }
}
