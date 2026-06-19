//! `ExecuteTruncate` (tablecmds.c:1861) + `ExecuteTruncateGuts` (1985) +
//! `truncate_check_rel` (2372) / `truncate_check_perms` (2420) /
//! `truncate_check_activity` (2438) + `RangeVarCallbackForTruncate` (19530).

#![allow(non_snake_case)]

use backend_utils_error::ereport;
use mcx::Mcx;

use types_acl::{ACLCHECK_NOT_OWNER, ACLCHECK_OK, ACL_TRUNCATE};
use types_core::primitive::{Oid, OidIsValid};
use types_core::SubTransactionId;
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE,
};
use types_nodes::ddlnodes::TruncateStmt;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::{DropBehavior, DROP_CASCADE, DROP_RESTRICT, OBJECT_SEQUENCE};
use types_rel::Relation;
use types_storage::lock::{AccessExclusiveLock, NoLock, LOCKMODE};
use types_tuple::access::{
    RangeVar as AccessRangeVar, RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};

use backend_access_common_relation::relation_open;
use backend_access_table_table_seams as table_seam;
use backend_access_transam_xact::GetCurrentSubTransactionId;
use backend_catalog_aclchk_seams as aclchk_seam;
use backend_catalog_namespace::RangeVarGetRelidExtended;
use backend_catalog_objectaddress_seams as objaddr_seam;
use backend_catalog_pg_inherits_seams as inherits_seam;
use backend_commands_tablespace_globals_seams as ts_globals_seam;
use backend_utils_init_miscinit_seams as miscinit_seam;

use backend_commands_tablecmds_seams as seam;

use crate::helpers::{here, to_access_range_var, LargeObjectRelationId, RelationRelationId};

/// `ExecuteTruncate(TruncateStmt *stmt)` (tablecmds.c:1861).
pub fn execute_truncate<'mcx>(mcx: Mcx<'mcx>, stmt: &TruncateStmt<'mcx>) -> PgResult<()> {
    let mut rels: Vec<Relation<'mcx>> = Vec::new();
    let mut relids: Vec<Oid> = Vec::new();
    let mut relids_logged: Vec<Oid> = Vec::new();

    /*
     * Open, exclusive-lock, and check all the explicitly-specified relations.
     */
    for rv_node in stmt.relations.iter() {
        let rv = match rv_node.as_rangevar() {
            Some(rv) => rv,
            None => unreachable!("TruncateStmt relation is a Node::RangeVar"),
        };
        let recurse = rv.inh;
        let lockmode: LOCKMODE = AccessExclusiveLock;

        let access_rv = to_access_range_var(rv);
        let myrelid = {
            let mut cb = |callback_rel: &AccessRangeVar, rel_id: Oid, old_rel_id: Oid| {
                RangeVarCallbackForTruncate(mcx, callback_rel, rel_id, old_rel_id)
            };
            RangeVarGetRelidExtended(mcx, &access_rv, lockmode, 0, Some(&mut cb))?
        };

        /* don't throw error for "TRUNCATE foo, foo" */
        if relids.contains(&myrelid) {
            continue;
        }

        /* open the relation, we already hold a lock on it */
        let rel = table_seam::table_open::call(mcx, myrelid, NoLock)?;

        /*
         * RangeVarGetRelidExtended() has done most checks with its callback,
         * but other checks with the now-opened Relation remain.
         */
        truncate_check_activity(mcx, &rel)?;

        let is_partitioned = rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;
        let is_logged = seam::relation_is_logically_logged::call(&rel)?;
        rels.push(rel);
        relids.push(myrelid);

        /* Log this relation only if needed for logical decoding */
        if is_logged {
            relids_logged.push(myrelid);
        }

        if recurse {
            let children = inherits_seam::find_all_inheritors::call(mcx, myrelid, lockmode)?;

            for &childrelid in children.iter() {
                if relids.contains(&childrelid) {
                    continue;
                }

                /* find_all_inheritors already got lock */
                let rel = table_seam::table_open::call(mcx, childrelid, NoLock)?;

                /*
                 * It is possible that the parent table has children that are
                 * temp tables of other backends; silently ignore them.
                 */
                if seam::relation_is_other_temp::call(&rel)? {
                    rel.close(lockmode)?;
                    continue;
                }

                /*
                 * Inherited TRUNCATE checks permissions on the parent only, so
                 * skip truncate_check_perms here.
                 */
                truncate_check_rel(rel.rd_id, rel.rd_rel.relkind, rel.rd_rel.relnamespace, rel.name())?;
                truncate_check_activity(mcx, &rel)?;

                let is_logged = seam::relation_is_logically_logged::call(&rel)?;
                rels.push(rel);
                relids.push(childrelid);

                if is_logged {
                    relids_logged.push(childrelid);
                }
            }
        } else if is_partitioned {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg("cannot truncate only a partitioned table")
                .errhint("Do not specify the ONLY keyword, or use TRUNCATE ONLY on the partitions directly.")
                .finish(here("ExecuteTruncate"));
        }
    }

    /*
     * We must close the explicit rels after the guts run.  The guts borrows the
     * relids; clone them so the close loop below can run unaffected.
     */
    let n_explicit = rels.len();
    execute_truncate_guts(
        mcx,
        &mut rels,
        relids,
        relids_logged,
        stmt.behavior,
        stmt.restart_seqs,
        false,
    )?;

    /* And close the rels (only the explicit ones; CASCADE rels closed in guts). */
    for rel in rels.into_iter().take(n_explicit) {
        rel.close(NoLock)?;
    }

    Ok(())
}

/// `ExecuteTruncateGuts(explicit_rels, relids, relids_logged, behavior,
/// restart_seqs, run_as_table_owner)` (tablecmds.c:1985).
///
/// `rels` is the explicit-rel working set passed by mutable reference: the
/// CASCADE loop appends to it and closes the appended rels at the end, leaving
/// the explicit ones for the caller (mirroring the C `list_difference_ptr`).
#[allow(clippy::too_many_arguments)]
pub fn execute_truncate_guts<'mcx>(
    mcx: Mcx<'mcx>,
    rels: &mut Vec<Relation<'mcx>>,
    mut relids: Vec<Oid>,
    mut relids_logged: Vec<Oid>,
    behavior: DropBehavior,
    restart_seqs: bool,
    run_as_table_owner: bool,
) -> PgResult<()> {
    let n_explicit = rels.len();
    let mut seq_relids: Vec<Oid> = Vec::new();

    /*
     * In CASCADE mode, suck in all referencing relations as well (fixpoint).
     */
    if behavior == DROP_CASCADE {
        loop {
            let newrelids = seam::heap_truncate_find_fks::call(mcx, &relids)?;
            if newrelids.is_empty() {
                break;
            }

            for &relid in newrelids.iter() {
                let rel = table_seam::table_open::call(mcx, relid, AccessExclusiveLock)?;
                ereport(NOTICE)
                    .errmsg(format!("truncate cascades to table \"{}\"", rel.name()))
                    .finish(here("ExecuteTruncateGuts"))?;
                truncate_check_rel(relid, rel.rd_rel.relkind, rel.rd_rel.relnamespace, rel.name())?;
                truncate_check_perms(relid, rel.rd_rel.relkind, rel.name())?;
                truncate_check_activity(mcx, &rel)?;
                let is_logged = seam::relation_is_logically_logged::call(&rel)?;
                rels.push(rel);
                relids.push(relid);

                if is_logged {
                    relids_logged.push(relid);
                }
            }
        }
    }

    /*
     * Check foreign key references.  In RESTRICT mode (always in Assert builds).
     */
    if cfg!(debug_assertions) || behavior == DROP_RESTRICT {
        seam::heap_truncate_check_fks::call(&relids, false)?;
    }

    /*
     * If we are asked to restart sequences, find and lock them, check perms.
     */
    if restart_seqs {
        for rel in rels.iter() {
            let seqlist = seam::get_owned_sequences::call(mcx, rel.rd_id)?;
            for &seq_relid in seqlist.iter() {
                let seq_rel = relation_open(mcx, seq_relid, AccessExclusiveLock)?;

                /* This check must match AlterSequence! */
                if !aclchk_seam::object_ownercheck::call(
                    RelationRelationId,
                    seq_relid,
                    miscinit_seam::get_user_id::call(),
                )? {
                    aclchk_seam::aclcheck_error::call(
                        ACLCHECK_NOT_OWNER,
                        OBJECT_SEQUENCE,
                        Some(seq_rel.name().to_string()),
                    )?;
                }

                seq_relids.push(seq_relid);

                seq_rel.close(NoLock)?;
            }
        }
    }

    /* Prepare to catch AFTER triggers; fire BEFORE STATEMENT triggers. */
    seam::exec_truncate_fire_before_triggers::call(mcx, &relids, run_as_table_owner)?;

    /*
     * OK, truncate each table.
     */
    let my_subid: SubTransactionId = GetCurrentSubTransactionId();

    /* Foreign-table truncation hash table, keyed by server OID. */
    let mut ft_htab: Vec<ForeignTruncateInfo> = Vec::new();

    for rel in rels.iter() {
        /* Skip partitioned tables as there is nothing to do */
        if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            continue;
        }

        /*
         * Build the lists of foreign tables belonging to each foreign server.
         */
        if rel.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
            let serverid = seam::get_foreign_server_id_by_rel_id::call(rel.rd_id)?;
            match ft_htab.iter_mut().find(|fi| fi.serverid == serverid) {
                Some(fi) => fi.rels.push(rel.rd_id),
                None => ft_htab.push(ForeignTruncateInfo {
                    serverid,
                    rels: vec![rel.rd_id],
                }),
            }
            continue;
        }

        /*
         * Normally, we need a transaction-safe truncation here.  However, if
         * the table was either created in the current (sub)transaction or has a
         * new relfilenumber in the current (sub)transaction, truncate in-place.
         */
        if seam::relation_get_create_subid::call(rel)? == my_subid
            || seam::relation_get_new_relfilelocator_subid::call(rel)? == my_subid
        {
            /* Immediate, non-rollbackable truncation is OK */
            seam::heap_truncate_one_rel::call(mcx, rel)?;
        } else {
            /*
             * This effectively deletes all rows in the table, and may be done
             * in a serializable transaction.
             */
            backend_storage_lmgr_predicate_seams::check_table_for_serializable_conflict_in::call(
                rel,
            )?;

            /*
             * Need the full transaction-safe pushups: new empty storage file.
             */
            backend_utils_cache_relcache_seams::relation_set_new_relfilenumber::call(
                rel.rd_id,
                rel.rd_rel.relpersistence as i8,
            )?;

            let heap_relid = rel.rd_id;

            /* The same for the toast table, if any. */
            let toast_relid = rel.rd_rel.reltoastrelid;
            if OidIsValid(toast_relid) {
                let toastrel = relation_open(mcx, toast_relid, AccessExclusiveLock)?;
                backend_utils_cache_relcache_seams::relation_set_new_relfilenumber::call(
                    toastrel.rd_id,
                    toastrel.rd_rel.relpersistence as i8,
                )?;
                toastrel.close(NoLock)?;
            }

            /* Reconstruct the indexes to match, and we're done. */
            let reindex_params = types_cluster::ReindexParams::default();
            backend_catalog_index_seams::reindex_relation::call(
                mcx,
                heap_relid,
                types_cluster::REINDEX_REL_PROCESS_TOAST,
                reindex_params,
            )?;
        }

        seam::pgstat_count_truncate::call(rel)?;
    }

    /* Now go through the hash table, and truncate foreign tables. */
    for ft_info in ft_htab.iter() {
        seam::exec_foreign_truncate::call(ft_info.serverid, &ft_info.rels, behavior, restart_seqs)?;
    }

    /* Restart owned sequences if we were asked to. */
    for &seq_relid in seq_relids.iter() {
        seam::reset_sequence::call(seq_relid)?;
    }

    /*
     * Write a WAL record to allow this set of actions to be logically decoded.
     */
    if !relids_logged.is_empty() {
        seam::write_heap_truncate_wal::call(
            seam::my_database_id::call()?,
            &relids_logged,
            behavior == DROP_CASCADE,
            restart_seqs,
        )?;
    }

    /* Fire AFTER STATEMENT triggers and tear down the EState. */
    seam::exec_truncate_fire_after_triggers::call(mcx, &relids, run_as_table_owner)?;

    /*
     * Close any rels opened by CASCADE (the C `list_difference_ptr`); the
     * explicit ones stay open for the caller.
     */
    let cascade_rels: Vec<Relation> = rels.split_off(n_explicit);
    for rel in cascade_rels.into_iter() {
        rel.close(NoLock)?;
    }

    Ok(())
}

/// `ForeignTruncateInfo` (tablecmds.c:338) — per-server list of foreign tables.
struct ForeignTruncateInfo {
    serverid: Oid,
    rels: Vec<Oid>,
}

/// `truncate_check_rel(relid, reltuple)` (tablecmds.c:2372). The owned model
/// passes `(relid, relkind, relnamespace, relname)` rather than `Form_pg_class`.
pub(crate) fn truncate_check_rel(
    relid: Oid,
    relkind: u8,
    relnamespace: Oid,
    relname: &str,
) -> PgResult<()> {
    /*
     * Only allow truncate on regular tables, FDW tables supporting TRUNCATE,
     * and partitioned tables.
     */
    if relkind == RELKIND_FOREIGN_TABLE {
        let serverid = seam::get_foreign_server_id_by_rel_id::call(relid)?;
        if !seam::fdw_supports_truncate::call(serverid)? {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!("cannot truncate foreign table \"{relname}\""))
                .finish(here("truncate_check_rel"));
        }
    } else if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("\"{relname}\" is not a table"))
            .finish(here("truncate_check_rel"));
    }

    /*
     * Most system catalogs can't be truncated. pg_largeobject is excepted for
     * pg_upgrade.
     */
    if !ts_globals_seam::allowSystemTableMods::call()?
        && seam::is_system_class_relid::call(relid, relkind, relnamespace)?
        && (!seam::is_binary_upgrade::call()? || relid != LargeObjectRelationId)
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied: \"{relname}\" is a system catalog"))
            .finish(here("truncate_check_rel"));
    }

    seam::invoke_object_truncate_hook::call(relid)?;
    Ok(())
}

/// `truncate_check_perms(relid, reltuple)` (tablecmds.c:2420).
pub(crate) fn truncate_check_perms(relid: Oid, relkind: u8, relname: &str) -> PgResult<()> {
    let aclresult = aclchk_seam::pg_class_aclcheck::call(
        relid,
        miscinit_seam::get_user_id::call(),
        ACL_TRUNCATE,
    )?;
    if aclresult != ACLCHECK_OK {
        aclchk_seam::aclcheck_error::call(
            aclresult,
            objaddr_seam::get_relkind_objtype::call(relkind),
            Some(relname.to_string()),
        )?;
    }
    Ok(())
}

/// `truncate_check_activity(rel)` (tablecmds.c:2438).
pub(crate) fn truncate_check_activity<'mcx>(_mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<()> {
    /* Don't allow truncate on temp tables of other backends. */
    if seam::relation_is_other_temp::call(rel)? {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot truncate temporary tables of other sessions")
            .finish(here("truncate_check_activity"));
    }

    /* Also check for active uses of the relation in the current transaction. */
    crate::smallfns::check_table_not_in_use(rel, "TRUNCATE")
}

/// `RangeVarCallbackForTruncate(relation, relId, oldRelId, arg)`
/// (tablecmds.c:19530).
fn RangeVarCallbackForTruncate(
    _mcx: Mcx<'_>,
    _relation: &AccessRangeVar,
    rel_id: Oid,
    _old_rel_id: Oid,
) -> PgResult<()> {
    /* Nothing to do if the relation was not found. */
    if !OidIsValid(rel_id) {
        return Ok(());
    }

    let info = seam::get_pg_class_drop_info::call(rel_id)?.ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for relation {rel_id}"))
            .into_error()
    })?;

    truncate_check_rel(rel_id, info.relkind, info.relnamespace, &info.relname)?;
    truncate_check_perms(rel_id, info.relkind, &info.relname)?;
    Ok(())
}
