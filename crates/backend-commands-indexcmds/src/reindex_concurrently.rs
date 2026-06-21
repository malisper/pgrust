//! `ReindexRelationConcurrently` (indexcmds.c:3568-4434) — the multi-phase,
//! multi-transaction REINDEX CONCURRENTLY worker. Builds new indexes alongside
//! the old ones, waits out concurrent lockers / older snapshots, validates,
//! swaps each new index in for its old one, marks the old ones dead, and drops
//! them — each phase processed for every index before the next.
//!
//! Faithful contained mappings relative to the C:
//!   * `WaitForLockersMultiple(lockTags, mode, true)` is replayed as a
//!     `WaitForLockers(tag, mode, true)` per locktag in the list. For the
//!     single-relation REINDEX targets the list has one (or, with a TOAST table,
//!     two) entries; waiting per tag drains the conflicting lockers of every tag,
//!     the same end state `WaitForLockersMultiple` reaches.
//!   * `pgstat_progress_update_multi_param` is replayed as the equivalent
//!     sequence of `pgstat_progress_update_param` calls.
//!   * REINDEX VERBOSE's per-index / per-table INFO lines and the `pg_rusage`
//!     timing are not emitted (no `pg_rusage` substrate); the rebuild itself is
//!     identical. VERBOSE output is immaterial to the regression diffs.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::Mcx;

use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{PgResult, ERROR};
use backend_utils_error::ereport;
use types_storage::lock::LOCKTAG;

use types_catalog::catalog::RELATION_RELATION_ID;
use types_catalog::catalog_dependency::ObjectAddress;
use types_nodes::ddlnodes::ReindexStmt;
use types_nodes::parsenodes::DropBehavior;

use types_tuple::access::{
    RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_TOASTVALUE,
};

use backend_utils_misc_guc::{at_eoxact_guc, NewGUCNestLevel};
use backend_utils_misc_guc_seams::restrict_search_path;
use backend_utils_init_miscinit::{GetUserIdAndSecContext, SetUserIdAndSecContext};

use backend_access_index_indexam_seams as indexam_seam;
use backend_access_table_table_seams as table_seam;
use backend_catalog_catalog_seams as catalog_seam;
use backend_catalog_dependency_seams as dependency_seam;
use backend_commands_event_trigger_seams as event_trigger_seam;
use backend_storage_lmgr_lmgr_seams as lmgr_seam;
use backend_storage_lmgr_proc_seams as proc_seam;
use backend_utils_cache_inval_seams as inval_seam;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;
use backend_access_transam_xact_seams as xact_seam;

use types_cluster::{REINDEXOPT_MISSING_OK, REINDEXOPT_VERBOSE};

use crate::choosers::ChooseRelationName;
use crate::{elog_error, here, ReindexParams, WaitForOlderSnapshots};

// progress.h constants (duplicated, as elsewhere in this tree, since
// commands/progress.h has no owned crate).
const PROGRESS_CREATEIDX_COMMAND: i32 = 1;
const PROGRESS_CREATEIDX_INDEX_OID: i32 = 2;
const PROGRESS_CREATEIDX_ACCESS_METHOD_OID: i32 = 4;
const PROGRESS_CREATEIDX_PHASE: i32 = 9;

const PROGRESS_CREATEIDX_COMMAND_REINDEX_CONCURRENTLY: i64 = 3;
const PROGRESS_CREATEIDX_PHASE_BUILD: i64 = 2;
const PROGRESS_CREATEIDX_PHASE_WAIT_1: i64 = 5;
const PROGRESS_CREATEIDX_PHASE_WAIT_2: i64 = 6;
const PROGRESS_CREATEIDX_PHASE_WAIT_3: i64 = 7;
const PROGRESS_CREATEIDX_PHASE_WAIT_4: i64 = 8;
const PROGRESS_CREATEIDX_PHASE_WAIT_5: i64 = 9;
const PROGRESS_CREATEIDX_PHASE_VALIDATE_IDXSCAN: i64 = 11;

// commands/progress.h PROGRESS_COMMAND_CREATE_INDEX.
use backend_utils_activity_small::backend_progress::{
    pgstat_progress_end_command, pgstat_progress_start_command, pgstat_progress_update_param,
};
use types_pgstat::backend_progress::ProgressCommandType;

const RELPERSISTENCE_TEMP_U8: u8 = b't';
// pg_tablespace.h GLOBALTABLESPACE_OID (the shared "pg_global" tablespace).
const GLOBALTABLESPACE_OID: Oid = 1664;

// SECURITY_RESTRICTED_OPERATION (miscadmin.h).
const SECURITY_RESTRICTED_OPERATION: i32 = 1 << 1;

// lock modes (lockdefs.h).
const SHARE_UPDATE_EXCLUSIVE_LOCK: i32 = 4;
const SHARE_LOCK: i32 = 5;
const ACCESS_EXCLUSIVE_LOCK: i32 = 8;
const NO_LOCK: i32 = 0;

/// One index being rebuilt (C `ReindexIndexInfo`).
#[derive(Clone, Copy)]
struct ReindexIndexInfo {
    index_id: Oid,
    table_id: Oid,
    am_id: Oid,
    safe: bool, /* for set_indexsafe_procflags */
}

fn update_multi_param(index_oid: Oid, am_id: Oid, phase: i64) {
    pgstat_progress_update_param(
        PROGRESS_CREATEIDX_COMMAND,
        PROGRESS_CREATEIDX_COMMAND_REINDEX_CONCURRENTLY,
    );
    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, phase);
    pgstat_progress_update_param(PROGRESS_CREATEIDX_INDEX_OID, index_oid as i64);
    pgstat_progress_update_param(PROGRESS_CREATEIDX_ACCESS_METHOD_OID, am_id as i64);
}

/// `ReindexRelationConcurrently(stmt, relationOid, params)` (indexcmds.c).
///
/// `relationOid` can be an index, a table, or a materialized view. Returns
/// `true` if any indexes were rebuilt, `false` otherwise.
pub(crate) fn ReindexRelationConcurrently<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ReindexStmt<'mcx>,
    relation_oid: Oid,
    params: &ReindexParams,
) -> PgResult<bool> {
    let mut heap_relation_ids: Vec<Oid> = Vec::new();
    let mut index_ids: Vec<ReindexIndexInfo> = Vec::new();
    let mut new_index_ids: Vec<ReindexIndexInfo> = Vec::new();
    // (lockrelid) entries, plus the parallel locktag list for the wait phases.
    let mut relation_locks: Vec<types_storage::lock::LockRelId> = Vec::new();
    let mut lock_tags: Vec<LOCKTAG> = Vec::new();

    let relkind = lsyscache::get_rel_relkind::call(relation_oid)?;

    // Extract the list of indexes to rebuild from the relation OID given.
    match relkind {
        RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE => {
            // Track this relation for session locks.
            heap_relation_ids.push(relation_oid);

            if catalog_seam::is_catalog_relation_oid::call(relation_oid) {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!("cannot reindex system catalogs concurrently"))
                    .into_error());
            }

            // Open relation to get its indexes.
            let heap_relation = if (params.options & REINDEXOPT_MISSING_OK) != 0 {
                match table_seam::try_table_open::call(
                    mcx,
                    relation_oid,
                    SHARE_UPDATE_EXCLUSIVE_LOCK,
                )? {
                    Some(r) => r,
                    None => return Ok(false), // relation does not exist
                }
            } else {
                table_seam::table_open::call(mcx, relation_oid, SHARE_UPDATE_EXCLUSIVE_LOCK)?
            };

            if OidIsValid(params.tablespace_oid)
                && catalog_seam::is_system_relation::call(&heap_relation)?
            {
                let name = lsyscache::get_rel_name::call(mcx, relation_oid)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!("cannot move system relation \"{name}\""))
                    .into_error());
            }

            // Add all the valid indexes of the relation to the list.
            let indexes = relcache_seam::relation_get_index_list::call(mcx, &heap_relation)?;
            for &cell_oid in indexes.iter() {
                let index_relation =
                    indexam_seam::index_open::call(mcx, cell_oid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
                let indisvalid = relcache_seam::rd_index_indisvalid::call(&index_relation)?;
                let indisexclusion = relcache_seam::rd_index_indisexclusion::call(&index_relation)?;
                if !indisvalid {
                    warn_skip_invalid(mcx, cell_oid)?;
                } else if indisexclusion {
                    warn_skip_exclusion(mcx, cell_oid)?;
                } else {
                    index_ids.push(ReindexIndexInfo {
                        index_id: cell_oid,
                        table_id: InvalidOid,
                        am_id: InvalidOid,
                        safe: false,
                    });
                }
                index_relation.close(NO_LOCK)?;
            }

            // Also add the toast indexes.
            let toast_oid = relcache_seam::rel_reltoastrelid::call(heap_relation.rd_id)?;
            if OidIsValid(toast_oid) {
                let toast_relation =
                    table_seam::table_open::call(mcx, toast_oid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
                heap_relation_ids.push(toast_oid);

                let toast_indexes =
                    relcache_seam::relation_get_index_list::call(mcx, &toast_relation)?;
                for &cell_oid in toast_indexes.iter() {
                    let index_relation = indexam_seam::index_open::call(
                        mcx,
                        cell_oid,
                        SHARE_UPDATE_EXCLUSIVE_LOCK,
                    )?;
                    let indisvalid =
                        relcache_seam::rd_index_indisvalid::call(&index_relation)?;
                    if !indisvalid {
                        warn_skip_invalid(mcx, cell_oid)?;
                    } else {
                        index_ids.push(ReindexIndexInfo {
                            index_id: cell_oid,
                            table_id: InvalidOid,
                            am_id: InvalidOid,
                            safe: false,
                        });
                    }
                    index_relation.close(NO_LOCK)?;
                }
                toast_relation.close(NO_LOCK)?;
            }

            heap_relation.close(NO_LOCK)?;
        }
        RELKIND_INDEX => {
            let heap_id = backend_catalog_index::IndexGetRelation(
                relation_oid,
                (params.options & REINDEXOPT_MISSING_OK) != 0,
            )?;

            // if relation is missing, leave.
            if !OidIsValid(heap_id) {
                return Ok(false);
            }

            if catalog_seam::is_catalog_relation_oid::call(heap_id) {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!("cannot reindex system catalogs concurrently"))
                    .into_error());
            }

            // Don't allow reindex for an invalid index on a TOAST table.
            let relnamespace = lsyscache::get_rel_namespace::call(relation_oid)?;
            if catalog_seam::is_toast_namespace::call(relnamespace)
                && !get_index_isvalid(mcx, relation_oid)?
            {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!("cannot reindex invalid index on TOAST table"))
                    .into_error());
            }

            // Check if the parent relation can be locked and exists.
            let heap_relation = if (params.options & REINDEXOPT_MISSING_OK) != 0 {
                match table_seam::try_table_open::call(mcx, heap_id, SHARE_UPDATE_EXCLUSIVE_LOCK)? {
                    Some(r) => r,
                    None => return Ok(false),
                }
            } else {
                table_seam::table_open::call(mcx, heap_id, SHARE_UPDATE_EXCLUSIVE_LOCK)?
            };

            if OidIsValid(params.tablespace_oid)
                && catalog_seam::is_system_relation::call(&heap_relation)?
            {
                let name = lsyscache::get_rel_name::call(mcx, relation_oid)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                heap_relation.close(NO_LOCK)?;
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!("cannot move system relation \"{name}\""))
                    .into_error());
            }

            heap_relation.close(NO_LOCK)?;

            // Track the heap relation of this index for session locks. Note that
            // invalid indexes are allowed here.
            heap_relation_ids.push(heap_id);
            index_ids.push(ReindexIndexInfo {
                index_id: relation_oid,
                table_id: InvalidOid,
                am_id: InvalidOid,
                safe: false,
            });
        }
        _ => {
            // Partitioned table / index or any other unsupported relkind.
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("cannot reindex this type of relation concurrently"))
                .into_error());
        }
    }

    // Definitely no indexes, so leave.
    if index_ids.is_empty() {
        return Ok(false);
    }

    // It's not a shared catalog, so refuse to move it to a shared tablespace.
    if params.tablespace_oid == GLOBALTABLESPACE_OID {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "cannot move non-shared relation to tablespace \"pg_global\""
            ))
            .into_error());
    }

    debug_assert!(!heap_relation_ids.is_empty());

    // ----------------------------------------------------------------------
    // Phase 1: create new indexes in the catalog (analogous to DefineIndex).
    // ----------------------------------------------------------------------
    for idx in index_ids.iter_mut() {
        let index_rel = indexam_seam::index_open::call(mcx, idx.index_id, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
        let indrelid = relcache_seam::rd_index_indrelid::call(&index_rel)?
            .expect("reindex concurrently: index has no rd_index");
        let heap_rel = table_seam::table_open::call(mcx, indrelid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;

        // Switch to the table owner's userid; lock down security-restricted
        // operations and make GUC changes local to this command.
        let (save_userid, save_sec_context) = GetUserIdAndSecContext();
        let relowner = relcache_seam::rd_rel_relowner::call(&heap_rel)?;
        SetUserIdAndSecContext(relowner, save_sec_context | SECURITY_RESTRICTED_OPERATION);
        let save_nestlevel = NewGUCNestLevel();
        restrict_search_path::call()?;

        // Determine safety of this index for set_indexsafe_procflags.
        let safe = relcache_seam::relation_get_index_expressions::call(mcx, &index_rel)?.is_none()
            && relcache_seam::relation_get_index_predicate::call(mcx, &index_rel)?.is_none();
        idx.safe = safe;
        idx.table_id = heap_rel.rd_id;
        idx.am_id = relcache_seam::rd_rel_relam::call(&index_rel)?;

        // This function shouldn't be called for temporary relations.
        if relcache_seam::rd_rel_relpersistence::call(&index_rel)? == RELPERSISTENCE_TEMP_U8 as i8 {
            return Err(elog_error(
                "cannot reindex a temporary table concurrently",
            ));
        }

        pgstat_progress_start_command(ProgressCommandType::CreateIndex, idx.table_id);
        update_multi_param(idx.index_id, idx.am_id, 0 /* initializing */);

        // Choose a temporary relation name for the new index.
        let old_name = lsyscache::get_rel_name::call(mcx, idx.index_id)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        let concurrent_name = ChooseRelationName(
            mcx,
            &old_name,
            None,
            "ccnew",
            lsyscache::get_rel_namespace::call(indrelid)?,
            false,
        )?;

        // Choose the new tablespace; indexes of toast tables are not moved.
        let heap_relkind = relcache_seam::rd_rel_relkind::call(&heap_rel)?;
        let tablespaceid = if OidIsValid(params.tablespace_oid)
            && heap_relkind != RELKIND_TOASTVALUE as i8
        {
            params.tablespace_oid
        } else {
            relcache_seam::rd_rel_reltablespace::call(&index_rel)?
        };

        // Create the new index definition based on the given index.
        let new_index_id = backend_catalog_index::index_concurrently_create_copy(
            mcx,
            &heap_rel,
            idx.index_id,
            tablespaceid,
            &concurrent_name,
        )?;

        // Open the new index; a session-level lock is also needed on it.
        let new_index_rel = indexam_seam::index_open::call(mcx, new_index_id, SHARE_UPDATE_EXCLUSIVE_LOCK)?;

        new_index_ids.push(ReindexIndexInfo {
            index_id: new_index_id,
            safe: idx.safe,
            table_id: idx.table_id,
            am_id: idx.am_id,
        });

        // Save lockrelid to protect each relation from drop, then close them.
        relation_locks.push(relcache_seam::rel_lock_relid::call(index_rel.rd_id)?);
        relation_locks.push(relcache_seam::rel_lock_relid::call(new_index_rel.rd_id)?);

        index_rel.close(NO_LOCK)?;
        new_index_rel.close(NO_LOCK)?;

        // Roll back any GUC changes executed by index functions; restore userid.
        at_eoxact_guc(false, save_nestlevel);
        SetUserIdAndSecContext(save_userid, save_sec_context);

        heap_rel.close(NO_LOCK)?;

        // Collect the new index for event triggers (this comes from REINDEX).
        let address = ObjectAddress {
            classId: RELATION_RELATION_ID,
            objectId: new_index_id,
            objectSubId: 0,
        };
        event_trigger_seam::event_trigger_collect_simple_command_reindex::call(
            address,
            ObjectAddress { classId: InvalidOid, objectId: InvalidOid, objectSubId: 0 },
            stmt,
        )?;
    }

    // Save the heap locks for the following visibility checks.
    for &heap_oid in heap_relation_ids.iter() {
        let heap_relation = table_seam::table_open::call(mcx, heap_oid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
        let lockrelid = relcache_seam::rel_lock_relid::call(heap_relation.rd_id)?;
        relation_locks.push(lockrelid);
        lock_tags.push(lmgr_seam::set_locktag_relation::call(lockrelid.dbId, lockrelid.relId));
        heap_relation.close(NO_LOCK)?;
    }

    // Get a session-level lock on each table.
    for &lockrelid in relation_locks.iter() {
        lmgr_seam::lock_relation_id_for_session::call(lockrelid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
    }

    snapmgr_seam::pop_active_snapshot::call()?;
    xact_seam::commit_transaction_command::call()?;
    xact_seam::start_transaction_command::call()?;

    // ----------------------------------------------------------------------
    // Phase 2: build the new indexes (each in its own transaction).
    // ----------------------------------------------------------------------
    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_1);
    wait_for_lockers_multiple(&lock_tags, SHARE_LOCK)?;
    xact_seam::commit_transaction_command::call()?;

    for newidx in new_index_ids.iter() {
        xact_seam::start_transaction_command::call()?;
        backend_access_transam_parallel_rt_seams::check_for_interrupts::call()?;

        if newidx.safe {
            proc_seam::set_indexsafe_procflags::call()?;
        }

        snapmgr_seam::push_active_snapshot::call(alloc::rc::Rc::new(
            snapmgr_seam::get_transaction_snapshot::call()?,
        ))?;

        pgstat_progress_start_command(ProgressCommandType::CreateIndex, newidx.table_id);
        update_multi_param(newidx.index_id, newidx.am_id, PROGRESS_CREATEIDX_PHASE_BUILD);

        backend_catalog_index::index_concurrently_build(mcx, newidx.table_id, newidx.index_id)?;

        snapmgr_seam::pop_active_snapshot::call()?;
        xact_seam::commit_transaction_command::call()?;
    }

    xact_seam::start_transaction_command::call()?;

    // ----------------------------------------------------------------------
    // Phase 3: validate; let new indexes catch up; wait out older snapshots.
    // ----------------------------------------------------------------------
    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_2);
    wait_for_lockers_multiple(&lock_tags, SHARE_LOCK)?;
    xact_seam::commit_transaction_command::call()?;

    for newidx in new_index_ids.iter() {
        xact_seam::start_transaction_command::call()?;
        backend_access_transam_parallel_rt_seams::check_for_interrupts::call()?;

        if newidx.safe {
            proc_seam::set_indexsafe_procflags::call()?;
        }

        // Take the "reference snapshot" validate_index() uses to filter tuples.
        let snapshot = snapmgr_seam::register_snapshot::call(
            snapmgr_seam::get_transaction_snapshot::call()?,
        )?;
        snapmgr_seam::push_active_snapshot::call(alloc::rc::Rc::new(snapshot.clone()))?;

        pgstat_progress_start_command(ProgressCommandType::CreateIndex, newidx.table_id);
        update_multi_param(
            newidx.index_id,
            newidx.am_id,
            PROGRESS_CREATEIDX_PHASE_VALIDATE_IDXSCAN,
        );

        backend_catalog_index::validate_index(
            mcx,
            newidx.table_id,
            newidx.index_id,
            Some(snapshot.clone()),
        )?;

        let limit_xmin = snapshot.xmin;

        snapmgr_seam::pop_active_snapshot::call()?;
        snapmgr_seam::unregister_snapshot::call(snapshot);

        // Commit + start another transaction, then wait before taking a snapshot.
        xact_seam::commit_transaction_command::call()?;
        xact_seam::start_transaction_command::call()?;

        pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_3);
        WaitForOlderSnapshots(mcx, limit_xmin, true)?;

        xact_seam::commit_transaction_command::call()?;
    }

    // ----------------------------------------------------------------------
    // Phase 4: swap each new index with its corresponding old index.
    // ----------------------------------------------------------------------
    xact_seam::start_transaction_command::call()?;
    proc_seam::set_indexsafe_procflags::call()?;

    for (oldidx, newidx) in index_ids.iter().zip(new_index_ids.iter()) {
        backend_access_transam_parallel_rt_seams::check_for_interrupts::call()?;

        // Choose a relation name for the old index.
        let old_rel_name = lsyscache::get_rel_name::call(mcx, oldidx.index_id)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        let old_name = ChooseRelationName(
            mcx,
            &old_rel_name,
            None,
            "ccold",
            lsyscache::get_rel_namespace::call(oldidx.table_id)?,
            false,
        )?;

        // Swapping might involve TOAST access, so ensure a valid snapshot.
        snapmgr_seam::push_active_snapshot::call(alloc::rc::Rc::new(
            snapmgr_seam::get_transaction_snapshot::call()?,
        ))?;

        backend_catalog_index::index_concurrently_swap(
            mcx,
            newidx.index_id,
            oldidx.index_id,
            &old_name,
        )?;

        snapmgr_seam::pop_active_snapshot::call()?;

        // Invalidate the table's relcache so sessions refresh cached plans.
        inval_seam::cache_invalidate_relcache::call(oldidx.table_id)?;

        // CCI so subsequent iterations see the oldName in the catalog.
        xact_seam::command_counter_increment::call()?;
    }

    xact_seam::commit_transaction_command::call()?;
    xact_seam::start_transaction_command::call()?;

    // ----------------------------------------------------------------------
    // Phase 5: mark the old indexes as dead.
    // ----------------------------------------------------------------------
    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_4);
    wait_for_lockers_multiple(&lock_tags, ACCESS_EXCLUSIVE_LOCK)?;

    for oldidx in index_ids.iter() {
        backend_access_transam_parallel_rt_seams::check_for_interrupts::call()?;

        snapmgr_seam::push_active_snapshot::call(alloc::rc::Rc::new(
            snapmgr_seam::get_transaction_snapshot::call()?,
        ))?;

        backend_catalog_index::index_concurrently_set_dead(mcx, oldidx.table_id, oldidx.index_id)?;

        snapmgr_seam::pop_active_snapshot::call()?;
    }

    xact_seam::commit_transaction_command::call()?;
    xact_seam::start_transaction_command::call()?;

    // ----------------------------------------------------------------------
    // Phase 6: drop the old indexes.
    // ----------------------------------------------------------------------
    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_WAIT_5);
    wait_for_lockers_multiple(&lock_tags, ACCESS_EXCLUSIVE_LOCK)?;

    snapmgr_seam::push_active_snapshot::call(alloc::rc::Rc::new(
        snapmgr_seam::get_transaction_snapshot::call()?,
    ))?;

    {
        let mut objects = dependency_seam::new_object_addresses::call()?;
        for idx in index_ids.iter() {
            let object = ObjectAddress {
                classId: RELATION_RELATION_ID,
                objectId: idx.index_id,
                objectSubId: 0,
            };
            dependency_seam::add_exact_object_address::call(object, &mut objects)?;
        }

        // Use PERFORM_DELETION_CONCURRENT_LOCK so that index_drop() uses the
        // right lock level (ShareUpdateExclusiveLock) but the *normal*
        // (non-concurrent) drop path — this is NOT PERFORM_DELETION_CONCURRENTLY.
        dependency_seam::perform_multiple_deletions::call(
            &objects.refs,
            DropBehavior::Restrict,
            dependency_seam::PERFORM_DELETION_CONCURRENT_LOCK
                | dependency_seam::PERFORM_DELETION_INTERNAL,
        )?;
    }

    snapmgr_seam::pop_active_snapshot::call()?;
    xact_seam::commit_transaction_command::call()?;

    // Finally, release the session-level lock on each table.
    for &lockrelid in relation_locks.iter() {
        lmgr_seam::unlock_relation_id_for_session::call(lockrelid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
    }

    // Start a new transaction to finish processing properly.
    xact_seam::start_transaction_command::call()?;

    // REINDEX VERBOSE INFO lines / pg_rusage timing are not emitted (see module
    // doc); the rebuild is complete.
    let _ = (params.options & REINDEXOPT_VERBOSE) != 0;

    pgstat_progress_end_command();

    Ok(true)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// `WaitForLockersMultiple(lockTags, lockmode, true)` — replayed per locktag
/// (see module doc). `progress` is always `true` at the REINDEX CONCURRENTLY
/// call sites.
fn wait_for_lockers_multiple(lock_tags: &[LOCKTAG], lockmode: i32) -> PgResult<()> {
    for &tag in lock_tags.iter() {
        lmgr_seam::wait_for_lockers::call(tag, lockmode, true)?;
    }
    Ok(())
}

/// `get_index_isvalid(indexoid)` (lsyscache.c) via the tablecmds seam, mapping
/// the `Option` (cache miss) to the C `false`/`true` `indisvalid`.
fn get_index_isvalid(_mcx: Mcx<'_>, indexoid: Oid) -> PgResult<bool> {
    Ok(backend_commands_tablecmds_seams::get_index_isvalid::call(indexoid)?.unwrap_or(false))
}

fn warn_skip_invalid(mcx: Mcx<'_>, cell_oid: Oid) -> PgResult<()> {
    use types_error::WARNING;
    let nsp = lsyscache::get_rel_namespace::call(cell_oid)?;
    let nspname = lsyscache::get_namespace_name::call(mcx, nsp)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    let relname = lsyscache::get_rel_name::call(mcx, cell_oid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    ereport(WARNING)
        .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg(format!(
            "skipping reindex of invalid index \"{nspname}.{relname}\""
        ))
        .errhint("Use DROP INDEX or REINDEX INDEX.".to_string())
        .finish(crate::here("ReindexRelationConcurrently"))?;
    Ok(())
}

fn warn_skip_exclusion(mcx: Mcx<'_>, cell_oid: Oid) -> PgResult<()> {
    use types_error::WARNING;
    let nsp = lsyscache::get_rel_namespace::call(cell_oid)?;
    let nspname = lsyscache::get_namespace_name::call(mcx, nsp)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    let relname = lsyscache::get_rel_name::call(mcx, cell_oid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    ereport(WARNING)
        .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(format!(
            "cannot reindex exclusion constraint index \"{nspname}.{relname}\" concurrently, skipping"
        ))
        .finish(crate::here("ReindexRelationConcurrently"))?;
    Ok(())
}
