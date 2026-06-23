//! Idiomatic port of `backend/commands/cluster.c` — CLUSTER a table on an
//! index (also the engine behind VACUUM FULL).
//!
//! Every cluster.c function is present in-crate with identical control flow,
//! branch order, constants, lock levels, SQLSTATEs and messages, and
//! `CommandCounterIncrement` placement. Relations cross as
//! [`::rel::Relation`] handles (opened through the table/index owner's
//! seams; `rd_rel` fields read directly off the handle); the catalog-row copy
//! `swap_relation_files`/`copy_table_data` mutate is the real
//! [`::types_cluster::PgClassForm`]. Outward calls go through each owner's
//! `-seams` crate and panic loudly until the owner lands.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};

use ::mcx::{Mcx, PgVec};
use ::types_cluster::{
    ClusterParams, ReindexParams, CLUOPT_RECHECK, CLUOPT_RECHECK_ISCLUSTERED,
    CLUOPT_VERBOSE, PROGRESS_CLUSTER_COMMAND, PROGRESS_CLUSTER_COMMAND_CLUSTER,
    PROGRESS_CLUSTER_COMMAND_VACUUM_FULL, PROGRESS_CLUSTER_PHASE,
    PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP, PROGRESS_CLUSTER_PHASE_REBUILD_INDEX,
    PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES, PROGRESS_COMMAND_CLUSTER,
    REINDEX_REL_CHECK_CONSTRAINTS, REINDEX_REL_FORCE_INDEXES_PERMANENT,
    REINDEX_REL_FORCE_INDEXES_UNLOGGED, REINDEX_REL_SUPPRESS_INDEX_USE,
};
use ::types_core::{InvalidOid, MultiXactId, Oid, TransactionId};
use ::types_error::PgResult;
use ::rel::Relation;
use ::types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, RowExclusiveLock, NoLock, LOCKMODE,
};
use ::types_tuple::access::{
    RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_TOASTVALUE, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};
use ::types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_INTERNAL};

use ::utils_error::ereport;
use ::types_error::{ErrorLocation, DEBUG2, ERROR, INFO, WARNING};

// Owner seam crates.
use transam_xact_seams as xact;
use dependency_seams as dependency;
use pg_depend_seams as pg_depend;
use lmgr_seams as lmgr;
use namespace_seams as namespace;
use small1_seams as parse_node;
use table_seams as table;
use indexam_seams as indexam;
use relcache_seams as relcache;
use syscache_seams as syscache;
use indexing_seams as indexing;
use inval_seams as inval;
use catalog_seams as catalog;
use heap_seams as heap;
use toasting_seams as toasting;
use toastdesc_seams as toast_internals;
use index_seams as index;
use pg_inherits_seams as pg_inherits;
use tablecmds_seams as tablecmds;
use vacuum_seams as vacuum;
use planner_seams as planner;
use predicate_seams as predicate;
use heapam_seams as heapam;
use access_tableam_seams as tableam;
use relmapper_seams as relmapper;
use objectaccess_seams as objectaccess;
use catalog_perm_seams as acl;
use activity_small_seams as backend_progress;
use snapmgr_seams as snapmgr;
use clean_seams as pg_rusage;
use elog_seams as elog;
use postgres_seams as tcop;
use lsyscache_seams as lsyscache;
use miscinit_seams as miscinit;
use guc_seams as guc;

mod seams_install;
pub use seams_install::init_seams;

// ---------------------------------------------------------------------------
// Constants verified against PostgreSQL 18.3 headers.
// ---------------------------------------------------------------------------

/// `InvalidTransactionId` (access/transam.h).
const InvalidTransactionId: TransactionId = 0;
/// `InvalidMultiXactId` (access/multixact.h).
const InvalidMultiXactId: MultiXactId = 0;

/// `NAMEDATALEN` (pg_config_manual.h).
const NAMEDATALEN: usize = 64;

/// Catalog relation OIDs (catalog/pg_class_d.h, pg_index_d.h, pg_am_d.h).
const RelationRelationId: Oid = 1259;
const IndexRelationId: Oid = 2610;
const AccessMethodRelationId: Oid = 2601;

/// `BTREE_AM_OID` (catalog/pg_am_d.h).
const BTREE_AM_OID: Oid = 403;

/// `SECURITY_RESTRICTED_OPERATION` (miscadmin.h).
const SECURITY_RESTRICTED_OPERATION: i32 = 0x0002;

/// `PERFORM_DELETION_INTERNAL` (catalog/dependency.h).
const PERFORM_DELETION_INTERNAL: i32 = 0x0001;

/// `ErrorLocation` for `ereport(...).finish(...)`.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("cluster.c", 0, funcname)
}

#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

#[inline]
fn RelFileNumberIsValid(rnum: Oid) -> bool {
    rnum != InvalidOid
}

#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdIsNormal(xid)` — `xid >= FirstNormalTransactionId (3)`.
#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= 3
}

#[inline]
fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi != InvalidMultiXactId
}

/// `TransactionIdPrecedes(id1, id2)` — modulo-2^32 comparison (transam.c).
#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    (id1.wrapping_sub(id2) as i32) < 0
}

/// `MultiXactIdPrecedes(multi1, multi2)` — modular MXID comparison (multixact.c).
#[inline]
fn MultiXactIdPrecedes(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    (multi1.wrapping_sub(multi2) as i32) < 0
}

/// `RelToCluster` (cluster.c:64-68) — `{ Oid tableOid; Oid indexOid; }`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RelToCluster {
    tableOid: Oid,
    indexOid: Oid,
}

/// `elog(ERROR, ...)` — internal error (XX000), for the "can't happen"
/// cache-lookup and swap-invariant failures. `finish` at ERROR always yields
/// `Err`, so the `unwrap_err` is infallible; we rewrap into the caller's `T`.
fn elog_error<T>(msg: String) -> PgResult<T> {
    let e = ereport(ERROR)
        .errmsg_internal(msg)
        .finish(here("cluster.c"))
        .expect_err("ereport(ERROR) always yields an Err");
    Err(e)
}

/// `snprintf(name, NAMEDATALEN, ...)` truncation to at most NAMEDATALEN-1 bytes.
fn format_namedata(s: &str) -> String {
    if s.len() >= NAMEDATALEN {
        s[..NAMEDATALEN - 1].to_string()
    } else {
        s.to_string()
    }
}

// ===========================================================================
// cluster   (cluster.c:107)
// ===========================================================================

/// `cluster(ParseState *pstate, ClusterStmt *stmt, bool isTopLevel)`.
pub fn cluster(
    mcx: Mcx<'_>,
    pstate: &::types_cluster::ParseState<'_>,
    stmt: &::types_cluster::ClusterStmt,
    isTopLevel: bool,
) -> PgResult<()> {
    let mut params = ClusterParams::new();
    let mut verbose = false;
    let mut indexOid: Oid = InvalidOid;

    /* Parse option list */
    for opt in &stmt.params {
        if opt.defname == "verbose" {
            verbose = ::define_seams::def_get_boolean::call(
                opt.defname.clone(),
                opt.arg.as_ref().map(map_defelem_arg),
            )?;
        } else {
            return ereport(ERROR)
                .errcode(::types_error::ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized {} option \"{}\"", "CLUSTER", opt.defname))
                .errposition(parse_node::parser_errposition::call(pstate, opt.location)?)
                .finish(here("cluster"));
        }
    }

    params.options = if verbose { CLUOPT_VERBOSE } else { 0 };

    // rel is the opened relation handle for the single-relation case.
    let mut rel: Option<Relation<'_>> = None;

    if let Some(relation) = &stmt.relation {
        /*
         * Find, lock, and check permissions on the table.  We obtain
         * AccessExclusiveLock right away to avoid lock-upgrade hazard in the
         * single-transaction case.
         */
        let tableOid = namespace::range_var_get_relid_maintains_table::call(
            mcx,
            relation,
            AccessExclusiveLock,
        )?;
        let opened = table::table_open::call(mcx, tableOid, NoLock)?;

        /*
         * Reject clustering a remote temp table ... their local buffer
         * manager is not going to cope.
         */
        if relation_is_other_temp(&opened)? {
            return ereport(ERROR)
                .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot cluster temporary tables of other sessions")
                .finish(here("cluster"));
        }

        match &stmt.indexname {
            None => {
                /* We need to find the index that has indisclustered set. */
                for thisIndexOid in relcache::relation_get_index_list::call(mcx, &opened)? {
                    indexOid = thisIndexOid;
                    if lsyscache::get_index_isclustered::call(indexOid)? {
                        break;
                    }
                    indexOid = InvalidOid;
                }

                if !OidIsValid(indexOid) {
                    return ereport(ERROR)
                        .errcode(::types_error::ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!(
                            "there is no previously clustered index for table \"{}\"",
                            relation.relname
                        ))
                        .finish(here("cluster"));
                }
            }
            Some(indexname) => {
                /* The index is expected to be in the same namespace as the relation. */
                indexOid = lsyscache::get_relname_relid::call(
                    indexname,
                    relcache::rd_rel_relnamespace::call(&opened)?,
                )?;
                if !OidIsValid(indexOid) {
                    return ereport(ERROR)
                        .errcode(::types_error::ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!(
                            "index \"{}\" for table \"{}\" does not exist",
                            indexname, relation.relname
                        ))
                        .finish(here("cluster"));
                }
            }
        }

        /* For non-partitioned tables, do what we came here to do. */
        if opened.rd_rel.relkind != RELKIND_PARTITIONED_TABLE {
            cluster_rel(mcx, opened, indexOid, params)?;
            /* cluster_rel closes the relation, but keeps lock */
            return Ok(());
        }

        rel = Some(opened);
    }

    /*
     * By here, we know we are in a multi-table situation.  In order to avoid
     * holding locks for too long, we want to process each table in its own
     * transaction.  This forces us to disallow running inside a user
     * transaction block.
     */
    xact::prevent_in_transaction_block::call(isTopLevel, "CLUSTER")?;

    /*
     * Also, we need a memory context to hold our list of relations. Here the
     * `rtcs` list lives in `mcx` (the caller's PortalContext-equivalent),
     * which is exactly the C `cluster_context` lifetime — survives across the
     * per-relation transactions, freed when the context drops.
     */
    params.options |= CLUOPT_RECHECK;
    let rtcs: PgVec<RelToCluster> = if let Some(opened) = rel {
        debug_assert!(opened.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);
        check_index_is_clusterable(mcx, &opened, indexOid, AccessShareLock)?;
        let list = get_tables_to_cluster_partitioned(mcx, indexOid)?;
        /* close relation, releasing lock on parent table */
        opened.close(AccessExclusiveLock)?;
        list
    } else {
        let list = get_tables_to_cluster(mcx)?;
        params.options |= CLUOPT_RECHECK_ISCLUSTERED;
        list
    };

    /* Do the job. */
    cluster_multiple_rels(mcx, &rtcs, &mut params)?;

    /* Start a new transaction for the cleanup work. */
    xact::start_transaction_command::call()?;

    /* Clean up working storage (the `mcx`-owned `rtcs` drops with the context). */
    Ok(())
}

/// `RELATION_IS_OTHER_TEMP(rel)` (rel.h): a temp relation of another session.
/// `rd_rel->relpersistence == RELPERSISTENCE_TEMP && !rel->rd_islocaltemp`.
fn relation_is_other_temp(rel: &Relation<'_>) -> PgResult<bool> {
    Ok(rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP && !relcache::rd_islocaltemp::call(rel)?)
}

/// Marshal the local `DefElemArg` projection into the define owner's variant.
fn map_defelem_arg(
    a: &::types_cluster::DefElemArg,
) -> ::define_seams::DefElemArg {
    use ::define_seams::DefElemArg as D;
    match a {
        ::types_cluster::DefElemArg::Integer(i) => D::Integer(*i),
        ::types_cluster::DefElemArg::Float(s) => D::Float(s.clone()),
        ::types_cluster::DefElemArg::Boolean(b) => D::Boolean(*b),
        ::types_cluster::DefElemArg::String(s) => D::String(s.clone()),
    }
}

// ===========================================================================
// cluster_multiple_rels   (cluster.c:263)
// ===========================================================================

fn cluster_multiple_rels(
    mcx: Mcx<'_>,
    rtcs: &[RelToCluster],
    params: &mut ClusterParams,
) -> PgResult<()> {
    /* Commit to get out of starting transaction */
    snapmgr::pop_active_snapshot::call()?;
    xact::commit_transaction_command::call()?;

    /* Cluster the tables, each in a separate transaction */
    for rtc in rtcs.iter() {
        /* Start a new transaction for each relation. */
        xact::start_transaction_command::call()?;

        /* functions in indexes may want a snapshot set */
        snapmgr::push_active_snapshot_transaction::call()?;

        let rel = table::table_open::call(mcx, rtc.tableOid, AccessExclusiveLock)?;

        /* Process this table */
        cluster_rel(mcx, rel, rtc.indexOid, *params)?;
        /* cluster_rel closes the relation, but keeps lock */

        snapmgr::pop_active_snapshot::call()?;
        xact::commit_transaction_command::call()?;
    }

    Ok(())
}

// ===========================================================================
// cluster_rel   (cluster.c:311)
// ===========================================================================

/// `cluster_rel(Relation OldHeap, Oid indexOid, ClusterParams *params)`.
/// Consumes the `OldHeap` handle (cluster_rel closes it, keeping the lock).
pub fn cluster_rel(
    mcx: Mcx<'_>,
    OldHeap: Relation<'_>,
    indexOid: Oid,
    params: ClusterParams,
) -> PgResult<()> {
    let tableOid = OldHeap.rd_id;
    let verbose = (params.options & CLUOPT_VERBOSE) != 0;
    let recheck = (params.options & CLUOPT_RECHECK) != 0;

    debug_assert!(lmgr::check_relation_locked_by_me::call(
        tableOid,
        AccessExclusiveLock,
        false
    ));

    /* Check for user-requested abort. */
    tcop::check_for_interrupts::call()?;

    backend_progress::pgstat_progress_start_command::call(PROGRESS_COMMAND_CLUSTER, tableOid)?;
    if OidIsValid(indexOid) {
        backend_progress::pgstat_progress_update_param::call(
            PROGRESS_CLUSTER_COMMAND,
            PROGRESS_CLUSTER_COMMAND_CLUSTER,
        )?;
    } else {
        backend_progress::pgstat_progress_update_param::call(
            PROGRESS_CLUSTER_COMMAND,
            PROGRESS_CLUSTER_COMMAND_VACUUM_FULL,
        )?;
    }

    /*
     * Switch to the table owner's userid, lock down security-restricted
     * operations, and make GUC changes local to this command.
     */
    let (save_userid, save_sec_context) = miscinit::get_user_id_and_sec_context::call();
    miscinit::set_user_id_and_sec_context::call(
        relcache::rd_rel_relowner::call(&OldHeap)?,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    );
    let save_nestlevel = guc::new_guc_nest_level::call();
    guc::restrict_search_path::call()?;

    // The C body uses `goto out`; the inner closure replicates it, then the
    // `out:` cleanup runs unconditionally afterward.
    let mut OldHeap = OldHeap;
    let body_result: PgResult<()> = cluster_rel_body(mcx, &OldHeap, tableOid, indexOid, &params,
        recheck, verbose, save_userid);

    // The C body owns the single `relation_close(OldHeap, ...)`: every success
    // path of `cluster_rel_body` (its `goto out` bailouts and the main
    // `rebuild_relation` leg) already released `OldHeap`'s relcache reference
    // by OID. So on `Ok`, disarm this owned handle's `Drop` to avoid a second
    // refcount release (which would trip the `rd_refcnt > 0` guard and crash
    // the backend — the VACUUM FULL / CLUSTER double-close). On `Err`, the body
    // bailed out *before* closing (the `ereport(ERROR)` legs), so leave `Drop`
    // armed to release the reference on the error path, mirroring C's resowner
    // cleanup at transaction abort.
    if body_result.is_ok() {
        OldHeap.disarm_closer();
    }

    // out:
    match body_result {
        Ok(()) => {
            guc::at_eoxact_guc::call(false, save_nestlevel)?;
            miscinit::set_user_id_and_sec_context::call(save_userid, save_sec_context);
            backend_progress::pgstat_progress_end_command::call()?;
            Ok(())
        }
        Err(body_err) => {
            let _ = guc::at_eoxact_guc::call(false, save_nestlevel);
            miscinit::set_user_id_and_sec_context::call(save_userid, save_sec_context);
            let _ = backend_progress::pgstat_progress_end_command::call();
            Err(body_err)
        }
    }
}

/// The body of `cluster_rel` (everything between the GUC setup and `out:`).
/// Takes `OldHeap` by reference; the relation-close legs use the handle's
/// alias close (refcount release) plus the lock kept to transaction end.
fn cluster_rel_body(
    mcx: Mcx<'_>,
    OldHeap: &Relation<'_>,
    tableOid: Oid,
    indexOid: Oid,
    params: &ClusterParams,
    recheck: bool,
    verbose: bool,
    save_userid: Oid,
) -> PgResult<()> {
    /*
     * Since we may open a new transaction for each relation, recheck that the
     * relation still is what we think it is.
     */
    if recheck {
        /* Check that the user still has privileges for the relation */
        if !cluster_is_permitted_for_relation(mcx, tableOid, save_userid)? {
            table::relation_close::call(tableOid, AccessExclusiveLock)?;
            return Ok(()); // goto out
        }

        /* Silently skip a temp table for a remote session. */
        if relation_is_other_temp(OldHeap)? {
            table::relation_close::call(tableOid, AccessExclusiveLock)?;
            return Ok(()); // goto out
        }

        if OidIsValid(indexOid) {
            /* Check that the index still exists */
            if !syscache::search_syscache_exists_reloid::call(indexOid)? {
                table::relation_close::call(tableOid, AccessExclusiveLock)?;
                return Ok(()); // goto out
            }

            /* Check that the index is still the one with indisclustered set, if needed. */
            if (params.options & CLUOPT_RECHECK_ISCLUSTERED) != 0
                && !lsyscache::get_index_isclustered::call(indexOid)?
            {
                table::relation_close::call(tableOid, AccessExclusiveLock)?;
                return Ok(()); // goto out
            }
        }
    }

    /* We allow VACUUM FULL, but not CLUSTER, on shared catalogs. */
    if OidIsValid(indexOid) && relcache::rd_rel_relisshared::call(&OldHeap)? {
        return ereport(ERROR)
            .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot cluster a shared catalog")
            .finish(here("cluster_rel"));
    }

    /* Don't process temp tables of other backends. */
    if relation_is_other_temp(OldHeap)? {
        if OidIsValid(indexOid) {
            return ereport(ERROR)
                .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot cluster temporary tables of other sessions")
                .finish(here("cluster_rel"));
        } else {
            return ereport(ERROR)
                .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot vacuum temporary tables of other sessions")
                .finish(here("cluster_rel"));
        }
    }

    /* Check for active uses of the relation in the current transaction. */
    tablecmds::check_table_not_in_use::call(
        OldHeap,
        if OidIsValid(indexOid) { "CLUSTER" } else { "VACUUM" },
    )?;

    /* Check heap and index are valid to cluster on */
    let index: Option<Relation<'_>> = if OidIsValid(indexOid) {
        check_index_is_clusterable(mcx, OldHeap, indexOid, AccessExclusiveLock)?;
        Some(indexam::index_open::call(mcx, indexOid, NoLock)?)
    } else {
        None
    };

    /*
     * Quietly ignore the request if this is a materialized view which has not
     * been populated from its query.
     */
    if OldHeap.rd_rel.relkind == RELKIND_MATVIEW && !OldHeap.is_scannable() {
        if let Some(idx) = index {
            idx.close(AccessExclusiveLock)?;
        }
        table::relation_close::call(tableOid, AccessExclusiveLock)?;
        return Ok(()); // goto out
    }

    debug_assert!(
        OldHeap.rd_rel.relkind == RELKIND_RELATION
            || OldHeap.rd_rel.relkind == RELKIND_MATVIEW
            || OldHeap.rd_rel.relkind == RELKIND_TOASTVALUE
    );

    /*
     * All predicate locks on the tuples or pages are about to be made invalid;
     * promote them to relation locks.
     */
    predicate::transfer_predicate_locks_to_heap_relation::call(tableOid)?;

    /* rebuild_relation does all the dirty work (closes OldHeap and index). */
    rebuild_relation(mcx, OldHeap, index, verbose)
}

// ===========================================================================
// check_index_is_clusterable   (cluster.c:494)
// ===========================================================================

/// `check_index_is_clusterable(Relation OldHeap, Oid indexOid, LOCKMODE)`.
pub fn check_index_is_clusterable(
    mcx: Mcx<'_>,
    OldHeap: &Relation<'_>,
    indexOid: Oid,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let OldIndex = indexam::index_open::call(mcx, indexOid, lockmode)?;

    /* Check that index is in fact an index on the given relation */
    let mismatch = match relcache::rd_index_indrelid::call(&OldIndex)? {
        None => true, // rd_index == NULL
        Some(rel) => rel != OldHeap.rd_id,
    };
    if mismatch {
        return ereport(ERROR)
            .errcode(::types_error::ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "\"{}\" is not an index for table \"{}\"",
                OldIndex.name(),
                OldHeap.name()
            ))
            .finish(here("check_index_is_clusterable"));
    }

    /* Index AM must allow clustering */
    if !relcache::rd_indam_amclusterable::call(&OldIndex)? {
        return ereport(ERROR)
            .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "cannot cluster on index \"{}\" because access method does not support clustering",
                OldIndex.name()
            ))
            .finish(here("check_index_is_clusterable"));
    }

    /* Disallow clustering on incomplete (partial) indexes. */
    if relcache::rd_index_has_indpred::call(&OldIndex)? {
        return ereport(ERROR)
            .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "cannot cluster on partial index \"{}\"",
                OldIndex.name()
            ))
            .finish(here("check_index_is_clusterable"));
    }

    /* Disallow if index is left over from a failed CREATE INDEX CONCURRENTLY. */
    if !relcache::rd_index_indisvalid::call(&OldIndex)? {
        return ereport(ERROR)
            .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "cannot cluster on invalid index \"{}\"",
                OldIndex.name()
            ))
            .finish(here("check_index_is_clusterable"));
    }

    /* Drop relcache refcnt on OldIndex, but keep lock */
    OldIndex.close(NoLock)?;

    Ok(())
}

// ===========================================================================
// mark_index_clustered   (cluster.c:554)
// ===========================================================================

/// `mark_index_clustered(Relation rel, Oid indexOid, bool is_internal)`.
pub fn mark_index_clustered(
    mcx: Mcx<'_>,
    rel: &Relation<'_>,
    indexOid: Oid,
    is_internal: bool,
) -> PgResult<()> {
    /* Disallow applying to a partitioned table */
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return ereport(ERROR)
            .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot mark index clustered in partitioned table")
            .finish(here("mark_index_clustered"));
    }

    /* If the index is already marked clustered, no need to do anything. */
    if OidIsValid(indexOid) && lsyscache::get_index_isclustered::call(indexOid)? {
        return Ok(());
    }

    /* Check each index of the relation and set/clear the bit as needed. */
    let pg_index = table::table_open::call(mcx, IndexRelationId, RowExclusiveLock)?;

    for thisIndexOid in relcache::relation_get_index_list::call(mcx, rel)? {
        // SearchSysCacheCopy1(INDEXRELID, thisIndexOid): the writable pg_index
        // tuple's (t_self, {indisclustered, indisvalid}).
        let Some((tid, mut form)) =
            syscache::search_syscache_copy_pg_index::call(mcx, thisIndexOid)?
        else {
            return elog_error(format!("cache lookup failed for index {}", thisIndexOid));
        };

        /* Unset the bit if set.  We know it's wrong because we checked earlier. */
        if form.indisclustered {
            form.indisclustered = false;
            indexing::catalog_tuple_update_pg_index::call(mcx, &pg_index, tid, &form)?;
        } else if thisIndexOid == indexOid {
            /* this was checked earlier, but let's be real sure */
            if !form.indisvalid {
                return elog_error(format!("cannot cluster on invalid index {}", indexOid));
            }
            form.indisclustered = true;
            indexing::catalog_tuple_update_pg_index::call(mcx, &pg_index, tid, &form)?;
        }

        objectaccess::invoke_object_post_alter_hook_arg::call(
            IndexRelationId,
            thisIndexOid,
            0,
            InvalidOid,
            is_internal,
        )?;
        // heap_freetuple(indexTuple): the owned form drops here.
    }

    pg_index.close(RowExclusiveLock)?;
    Ok(())
}

// ===========================================================================
// rebuild_relation   (cluster.c:629)
// ===========================================================================

/// `rebuild_relation(Relation OldHeap, Relation index, bool verbose)`.
/// Consumes `OldHeap` and `index` (closes both, keeps locks).
fn rebuild_relation(
    mcx: Mcx<'_>,
    OldHeap: &Relation<'_>,
    index: Option<Relation<'_>>,
    verbose: bool,
) -> PgResult<()> {
    let tableOid = OldHeap.rd_id;
    let accessMethod = relcache::rd_rel_relam::call(&OldHeap)?;
    let tableSpace = relcache::rd_rel_reltablespace::call(&OldHeap)?;

    debug_assert!(
        lmgr::check_relation_locked_by_me::call(tableOid, AccessExclusiveLock, false)
            && match &index {
                None => true,
                Some(idx) => lmgr::check_relation_locked_by_me::call(
                    idx.rd_id,
                    AccessExclusiveLock,
                    false
                ),
            }
    );

    if let Some(idx) = &index {
        /* Mark the correct index as clustered */
        mark_index_clustered(mcx, OldHeap, idx.rd_id, true)?;
    }

    /* Remember info about rel before closing OldHeap */
    let relpersistence = OldHeap.rd_rel.relpersistence;
    let is_system_catalog = catalog::is_system_relation::call(OldHeap)?;

    /* Create the transient table that will receive the re-ordered data. */
    let OIDNewHeap = make_new_heap(mcx, tableOid, tableSpace, accessMethod, relpersistence, NoLock)?;
    debug_assert!(lmgr::check_relation_oid_locked_by_me::call(
        OIDNewHeap,
        AccessExclusiveLock,
        false
    ));
    let NewHeap = table::table_open::call(mcx, OIDNewHeap, NoLock)?;

    /* Copy the heap data into the new table in the desired order */
    let copied = copy_table_data(mcx, &NewHeap, OldHeap, index.as_ref(), verbose)?;

    /* Close relcache entries, but keep lock until transaction commit */
    table::relation_close::call(tableOid, NoLock)?;
    if let Some(idx) = index {
        idx.close(NoLock)?;
    }

    /* Close the new relation so it can be dropped as soon as the storage is swapped. */
    NewHeap.close(NoLock)?;

    /*
     * Swap the physical files of the target and transient tables, then rebuild
     * the target's indexes and throw away the transient table.
     */
    finish_heap_swap(
        mcx,
        tableOid,
        OIDNewHeap,
        is_system_catalog,
        copied.swap_toast_by_content,
        false,
        true,
        copied.freeze_xid,
        copied.cutoff_multi,
        relpersistence,
    )
}

// ===========================================================================
// make_new_heap   (cluster.c:705)
// ===========================================================================

/// `make_new_heap(Oid OIDOldHeap, Oid NewTableSpace, Oid NewAccessMethod,
/// char relpersistence, LOCKMODE lockmode)`.
pub fn make_new_heap(
    mcx: Mcx<'_>,
    OIDOldHeap: Oid,
    NewTableSpace: Oid,
    NewAccessMethod: Oid,
    relpersistence: u8,
    lockmode: LOCKMODE,
) -> PgResult<Oid> {
    let OldHeap = table::table_open::call(mcx, OIDOldHeap, lockmode)?;

    /*
     * Use reloptions of the old heap for the new heap. fetch_class_reloptions
     * does SearchSysCache1 / SysCacheGetAttr / ReleaseSysCache, erroring
     * "cache lookup failed for relation %u" on a missing tuple.
     */
    let reloptions = syscache::fetch_class_reloptions::call(mcx, OIDOldHeap)?;

    let namespaceid = if relpersistence == RELPERSISTENCE_TEMP {
        namespace::lookup_creation_namespace::call("pg_temp")?
    } else {
        relcache::rd_rel_relnamespace::call(&OldHeap)?
    };

    /* Create the new heap with a temporary name in the same namespace. */
    let NewHeapName = format_namedata(&format!("pg_temp_{}", OIDOldHeap));

    let OIDNewHeap = heap::heap_create_with_catalog_transient::call(
        mcx,
        &NewHeapName,
        namespaceid,
        NewTableSpace,
        relcache::rd_rel_relowner::call(&OldHeap)?,
        NewAccessMethod,
        &OldHeap,
        relpersistence,
        relcache::relation_is_mapped::call(&OldHeap)?,
        reloptions.clone(),
        OIDOldHeap,
    )?;
    debug_assert!(OIDNewHeap != InvalidOid);

    /* Advance command counter so the new relation's catalog tuples are visible. */
    xact::command_counter_increment::call()?;

    /* If necessary, create a TOAST table for the new relation. */
    let toastid = OldHeap.rd_rel.reltoastrelid;
    if OidIsValid(toastid) {
        /* keep the existing toast table's reloptions, if any */
        let toast_reloptions = syscache::fetch_class_reloptions::call(mcx, toastid)?;
        toasting::new_heap_create_toast_table::call(mcx, OIDNewHeap, toast_reloptions, lockmode, toastid)?;
    }

    OldHeap.close(NoLock)?;

    Ok(OIDNewHeap)
}

// ===========================================================================
// copy_table_data   (cluster.c:831)
// ===========================================================================

struct CopyTableDataResult {
    swap_toast_by_content: bool,
    freeze_xid: TransactionId,
    cutoff_multi: MultiXactId,
}

/// `copy_table_data(NewHeap, OldHeap, OldIndex, verbose, *pSwap, *pFreeze, *pCutoff)`.
fn copy_table_data(
    mcx: Mcx<'_>,
    NewHeap: &Relation<'_>,
    OldHeap: &Relation<'_>,
    OldIndex: Option<&Relation<'_>>,
    verbose: bool,
) -> PgResult<CopyTableDataResult> {
    let elevel = if verbose { INFO } else { DEBUG2 };

    let ru0 = pg_rusage::pg_rusage_init::call();

    /* Store a copy of the namespace name for logging purposes */
    let nspname = lsyscache::get_namespace_name::call(mcx, relcache::rd_rel_relnamespace::call(&OldHeap)?)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();

    /*
     * Their tuple descriptors should be exactly alike, but here we only need
     * assume that they have the same number of columns.
     */
    debug_assert!(NewHeap.rd_att.natts == OldHeap.rd_att.natts);

    /*
     * If the OldHeap has a toast table, lock it to keep it from being vacuumed.
     */
    if OidIsValid(OldHeap.rd_rel.reltoastrelid) {
        let _guard = lmgr::lock_relation_oid::call(OldHeap.rd_rel.reltoastrelid, AccessExclusiveLock)?;
        // The lock is held to end of transaction (C `LockRelationOid` without
        // an unlock); keep the guard from releasing on scope exit.
        _guard.keep();
    }

    /* If both tables have TOAST tables, perform toast swap by content. */
    let swap_toast_by_content = if OidIsValid(OldHeap.rd_rel.reltoastrelid)
        && OidIsValid(NewHeap.rd_rel.reltoastrelid)
    {
        /*
         * When doing swap by content, toast pointers written into NewHeap must
         * use the old toast table's OID (rd_toastoid), so toast_save_datum
         * preserves the toast value OIDs.
         */
        relcache::set_rd_toastoid::call(NewHeap, OldHeap.rd_rel.reltoastrelid)?;
        true
    } else {
        false
    };

    /* Compute xids used to freeze and weed out dead tuples and multixacts. */
    let mut cutoffs = vacuum::vacuum_get_cutoffs::call(OldHeap)?;

    /* FreezeXid mustn't go backwards, so take the max. */
    {
        let relfrozenxid = relcache::rd_rel_relfrozenxid::call(&OldHeap)?;
        if TransactionIdIsValid(relfrozenxid)
            && TransactionIdPrecedes(cutoffs.FreezeLimit, relfrozenxid)
        {
            cutoffs.FreezeLimit = relfrozenxid;
        }
    }

    /* MultiXactCutoff, similarly, shouldn't go backwards. */
    {
        let relminmxid = relcache::rd_rel_relminmxid::call(&OldHeap)?;
        if MultiXactIdIsValid(relminmxid)
            && MultiXactIdPrecedes(cutoffs.MultiXactCutoff, relminmxid)
        {
            cutoffs.MultiXactCutoff = relminmxid;
        }
    }

    /* Decide whether to use an indexscan or seqscan-and-optional-sort. */
    let use_sort = match OldIndex {
        Some(idx) if relcache::rd_rel_relam::call(&idx)? == BTREE_AM_OID => {
            planner::plan_cluster_use_sort::call(mcx, OldHeap.rd_id, idx.rd_id)?
        }
        _ => false,
    };

    /* Log what we're doing */
    if OldIndex.is_some() && !use_sort {
        let idx = OldIndex.unwrap();
        elog::ereport_msg::call(
            elevel,
            format!(
                "clustering \"{}.{}\" using index scan on \"{}\"",
                nspname,
                OldHeap.name(),
                idx.name()
            ),
            None,
        )?;
    } else if use_sort {
        elog::ereport_msg::call(
            elevel,
            format!(
                "clustering \"{}.{}\" using sequential scan and sort",
                nspname,
                OldHeap.name()
            ),
            None,
        )?;
    } else {
        elog::ereport_msg::call(
            elevel,
            format!("vacuuming \"{}.{}\"", nspname, OldHeap.name()),
            None,
        )?;
    }

    /* Hand off the actual copying to AM specific function. */
    let copied = tableam::table_relation_copy_for_cluster::call(
        mcx,
        OldHeap,
        NewHeap,
        OldIndex,
        use_sort,
        cutoffs.OldestXmin,
        cutoffs.FreezeLimit,
        cutoffs.MultiXactCutoff,
    )?;
    cutoffs.FreezeLimit = copied.new_frozen_xid;
    cutoffs.MultiXactCutoff = copied.new_cutoff_multi;

    let freeze_xid = cutoffs.FreezeLimit;
    let cutoff_multi = cutoffs.MultiXactCutoff;

    /* Reset rd_toastoid just to be tidy. */
    relcache::set_rd_toastoid::call(NewHeap, InvalidOid)?;

    let num_pages = relcache::relation_get_number_of_blocks::call(NewHeap)?;

    /* Log what we did */
    elog::ereport_msg::call(
        elevel,
        format!(
            "\"{}.{}\": found {:.0} removable, {:.0} nonremovable row versions in {} pages",
            nspname,
            OldHeap.name(),
            copied.tups_vacuumed,
            copied.num_tuples,
            relcache::relation_get_number_of_blocks::call(OldHeap)?
        ),
        Some(format!(
            "{:.0} dead row versions cannot be removed yet.\n{}.",
            copied.tups_recently_dead,
            pg_rusage::pg_rusage_show::call(mcx, ru0)?
        )),
    )?;

    /* Update pg_class to reflect the correct values of pages and tuples. */
    let relRelation = table::table_open::call(mcx, RelationRelationId, RowExclusiveLock)?;

    let Some((tid, mut reltup)) = syscache::search_syscache_copy_pg_class::call(mcx, NewHeap.rd_id)?
    else {
        return elog_error(format!("cache lookup failed for relation {}", NewHeap.rd_id));
    };

    reltup.relpages = num_pages as i32;
    reltup.reltuples = copied.num_tuples as f32;

    /* Don't update the stats for pg_class. See swap_relation_files. */
    if OldHeap.rd_id != RelationRelationId {
        indexing::catalog_tuple_update_pg_class::call(mcx, &relRelation, tid, &reltup)?;
    } else {
        inval::cache_invalidate_relcache_by_pg_class::call(NewHeap.rd_id, &reltup)?;
    }

    /* Clean up (heap_freetuple = drop). */
    relRelation.close(RowExclusiveLock)?;

    /* Make the update visible */
    xact::command_counter_increment::call()?;

    Ok(CopyTableDataResult {
        swap_toast_by_content,
        freeze_xid,
        cutoff_multi,
    })
}

// ===========================================================================
// swap_relation_files   (cluster.c:1063)
// ===========================================================================

/// The caller-owned `Oid mapped_tables[4]` array, modeled as a cursor.
struct MappedTables {
    slots: [Oid; 4],
    next: usize,
}

impl MappedTables {
    fn new() -> Self {
        MappedTables { slots: [InvalidOid; 4], next: 0 }
    }
    /// `*mapped_tables++ = r2`.
    fn push(&mut self, oid: Oid) {
        self.slots[self.next] = oid;
        self.next += 1;
    }
}

/// `swap_relation_files(r1, r2, target_is_pg_class, swap_toast_by_content,
/// is_internal, frozenXid, cutoffMulti, *mapped_tables)`.
fn swap_relation_files(
    mcx: Mcx<'_>,
    r1: Oid,
    r2: Oid,
    target_is_pg_class: bool,
    swap_toast_by_content: bool,
    is_internal: bool,
    frozenXid: TransactionId,
    cutoffMulti: MultiXactId,
    mapped_tables: &mut MappedTables,
) -> PgResult<()> {
    /* We need writable copies of both pg_class tuples. */
    let relRelation = table::table_open::call(mcx, RelationRelationId, RowExclusiveLock)?;

    let Some((tid1, mut relform1)) = syscache::search_syscache_copy_pg_class::call(mcx, r1)? else {
        return elog_error(format!("cache lookup failed for relation {}", r1));
    };
    let Some((tid2, mut relform2)) = syscache::search_syscache_copy_pg_class::call(mcx, r2)? else {
        return elog_error(format!("cache lookup failed for relation {}", r2));
    };

    let mut relfilenumber1 = relform1.relfilenode;
    let mut relfilenumber2 = relform2.relfilenode;
    let relam1 = relform1.relam;
    let relam2 = relform2.relam;

    if RelFileNumberIsValid(relfilenumber1) && RelFileNumberIsValid(relfilenumber2) {
        /* Normal non-mapped relations: swap relfilenumbers, reltablespaces, relpersistence */
        debug_assert!(!target_is_pg_class);

        core::mem::swap(&mut relform1.relfilenode, &mut relform2.relfilenode);
        core::mem::swap(&mut relform1.reltablespace, &mut relform2.reltablespace);
        core::mem::swap(&mut relform1.relam, &mut relform2.relam);
        core::mem::swap(&mut relform1.relpersistence, &mut relform2.relpersistence);

        /* Also swap toast links, if we're swapping by links */
        if !swap_toast_by_content {
            core::mem::swap(&mut relform1.reltoastrelid, &mut relform2.reltoastrelid);
        }
    } else {
        /* Mapped-relation case. Both must be mapped. */
        if RelFileNumberIsValid(relfilenumber1) || RelFileNumberIsValid(relfilenumber2) {
            return elog_error(format!(
                "cannot swap mapped relation \"{}\" with non-mapped relation",
                relform1.relname
            ));
        }

        if relform1.reltablespace != relform2.reltablespace {
            return elog_error(format!(
                "cannot change tablespace of mapped relation \"{}\"",
                relform1.relname
            ));
        }
        if relform1.relpersistence != relform2.relpersistence {
            return elog_error(format!(
                "cannot change persistence of mapped relation \"{}\"",
                relform1.relname
            ));
        }
        if relform1.relam != relform2.relam {
            return elog_error(format!(
                "cannot change access method of mapped relation \"{}\"",
                relform1.relname
            ));
        }
        if !swap_toast_by_content
            && (OidIsValid(relform1.reltoastrelid) || OidIsValid(relform2.reltoastrelid))
        {
            return elog_error(format!(
                "cannot swap toast by links for mapped relation \"{}\"",
                relform1.relname
            ));
        }

        /* Fetch the mappings --- shouldn't fail, but be paranoid */
        relfilenumber1 = relmapper::relation_map_oid_to_filenumber::call(r1, relform1.relisshared)?;
        if !RelFileNumberIsValid(relfilenumber1) {
            return elog_error(format!(
                "could not find relation mapping for relation \"{}\", OID {}",
                relform1.relname, r1
            ));
        }
        relfilenumber2 = relmapper::relation_map_oid_to_filenumber::call(r2, relform2.relisshared)?;
        if !RelFileNumberIsValid(relfilenumber2) {
            return elog_error(format!(
                "could not find relation mapping for relation \"{}\", OID {}",
                relform2.relname, r2
            ));
        }

        /* Send replacement mappings to relmapper. */
        relmapper::relation_map_update_map::call(r1, relfilenumber2, relform1.relisshared, false)?;
        relmapper::relation_map_update_map::call(r2, relfilenumber1, relform2.relisshared, false)?;

        /* Pass OIDs of mapped r2 tables back to caller */
        mapped_tables.push(r2);
    }

    /* Recognize that rel1's relfilenumber (swapped from rel2) is new in this subtransaction. */
    relcache::swap_relfilelocator_subids::call(r1, r2)?;

    /* set rel1's frozen Xid and minimum MultiXid */
    if relform1.relkind != RELKIND_INDEX {
        debug_assert!(!TransactionIdIsValid(frozenXid) || TransactionIdIsNormal(frozenXid));
        relform1.relfrozenxid = frozenXid;
        relform1.relminmxid = cutoffMulti;
    }

    /* swap size statistics too, since new rel has freshly-updated stats */
    {
        core::mem::swap(&mut relform1.relpages, &mut relform2.relpages);
        core::mem::swap(&mut relform1.reltuples, &mut relform2.reltuples);
        core::mem::swap(&mut relform1.relallvisible, &mut relform2.relallvisible);
        core::mem::swap(&mut relform1.relallfrozen, &mut relform2.relallfrozen);
    }

    /* Update the tuples in pg_class --- unless the target is pg_class itself. */
    if !target_is_pg_class {
        let mut indstate = indexing::catalog_open_indexes::call(mcx, &relRelation)?;
        indexing::catalog_tuple_update_with_info_pg_class::call(mcx, &relRelation, tid1, &relform1, &mut indstate)?;
        indexing::catalog_tuple_update_with_info_pg_class::call(mcx, &relRelation, tid2, &relform2, &mut indstate)?;
        indexing::catalog_close_indexes::call(indstate)?;
    } else {
        /* no update ... but we do still need relcache inval */
        inval::cache_invalidate_relcache_by_pg_class::call(r1, &relform1)?;
        inval::cache_invalidate_relcache_by_pg_class::call(r2, &relform2)?;
    }

    /* Update the dependency of the relations to point to their new table AM, if changed. */
    if relam1 != relam2 {
        if pg_depend::changeDependencyFor::call(
            mcx,
            RelationRelationId,
            r1,
            AccessMethodRelationId,
            relam1,
            relam2,
        )? != 1
        {
            return elog_error(format!(
                "could not change access method dependency for relation \"{}.{}\"",
                lsyscache::get_namespace_name::call(mcx, lsyscache::get_rel_namespace::call(r1)?)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default(),
                lsyscache::get_rel_name::call(mcx, r1)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default()
            ));
        }
        if pg_depend::changeDependencyFor::call(
            mcx,
            RelationRelationId,
            r2,
            AccessMethodRelationId,
            relam2,
            relam1,
        )? != 1
        {
            return elog_error(format!(
                "could not change access method dependency for relation \"{}.{}\"",
                lsyscache::get_namespace_name::call(mcx, lsyscache::get_rel_namespace::call(r2)?)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default(),
                lsyscache::get_rel_name::call(mcx, r2)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default()
            ));
        }
    }

    /* Post alter hook for modified relations. */
    objectaccess::invoke_object_post_alter_hook_arg::call(RelationRelationId, r1, 0, InvalidOid, is_internal)?;
    objectaccess::invoke_object_post_alter_hook_arg::call(RelationRelationId, r2, 0, InvalidOid, true)?;

    /* If we have toast tables associated with the relations being swapped, deal with them. */
    if OidIsValid(relform1.reltoastrelid) || OidIsValid(relform2.reltoastrelid) {
        if swap_toast_by_content {
            if OidIsValid(relform1.reltoastrelid) && OidIsValid(relform2.reltoastrelid) {
                /* Recursively swap the contents of the toast tables */
                swap_relation_files(
                    mcx,
                    relform1.reltoastrelid,
                    relform2.reltoastrelid,
                    target_is_pg_class,
                    swap_toast_by_content,
                    is_internal,
                    frozenXid,
                    cutoffMulti,
                    mapped_tables,
                )?;
            } else {
                /* caller messed up */
                return elog_error::<()>(
                    "cannot swap toast files by content when there's only one".to_string(),
                );
            }
        } else {
            /*
             * We swapped the ownership links, so change dependency data to match.
             * We disallow this case for system catalogs.
             */
            if catalog::is_system_class::call(r1, &relform1)? {
                return elog_error::<()>(
                    "cannot swap toast files by links for system catalogs".to_string(),
                );
            }

            /* Delete old dependencies */
            if OidIsValid(relform1.reltoastrelid) {
                let count = pg_depend::deleteDependencyRecordsFor::call(
                    RelationRelationId,
                    relform1.reltoastrelid,
                    false,
                )?;
                if count != 1 {
                    return elog_error(format!(
                        "expected one dependency record for TOAST table, found {}",
                        count
                    ));
                }
            }
            if OidIsValid(relform2.reltoastrelid) {
                let count = pg_depend::deleteDependencyRecordsFor::call(
                    RelationRelationId,
                    relform2.reltoastrelid,
                    false,
                )?;
                if count != 1 {
                    return elog_error(format!(
                        "expected one dependency record for TOAST table, found {}",
                        count
                    ));
                }
            }

            /* Register new dependencies */
            if OidIsValid(relform1.reltoastrelid) {
                let baseobject = ObjectAddress {
                    classId: RelationRelationId,
                    objectId: r1,
                    objectSubId: 0,
                };
                let toastobject = ObjectAddress {
                    classId: RelationRelationId,
                    objectId: relform1.reltoastrelid,
                    objectSubId: 0,
                };
                pg_depend::recordDependencyOn::call(mcx, &toastobject, &baseobject, DEPENDENCY_INTERNAL)?;
            }
            if OidIsValid(relform2.reltoastrelid) {
                let baseobject = ObjectAddress {
                    classId: RelationRelationId,
                    objectId: r2,
                    objectSubId: 0,
                };
                let toastobject = ObjectAddress {
                    classId: RelationRelationId,
                    objectId: relform2.reltoastrelid,
                    objectSubId: 0,
                };
                pg_depend::recordDependencyOn::call(mcx, &toastobject, &baseobject, DEPENDENCY_INTERNAL)?;
            }
        }
    }

    /* If we're swapping two toast tables by content, do the same for their valid index. */
    if swap_toast_by_content
        && relform1.relkind == RELKIND_TOASTVALUE
        && relform2.relkind == RELKIND_TOASTVALUE
    {
        let toastIndex1 = toast_internals::toast_get_valid_index::call(mcx, r1, AccessExclusiveLock)?;
        let toastIndex2 = toast_internals::toast_get_valid_index::call(mcx, r2, AccessExclusiveLock)?;

        swap_relation_files(
            mcx,
            toastIndex1,
            toastIndex2,
            target_is_pg_class,
            swap_toast_by_content,
            is_internal,
            InvalidTransactionId,
            InvalidMultiXactId,
            mapped_tables,
        )?;
    }

    /* Clean up (heap_freetuple = drop). */
    relRelation.close(RowExclusiveLock)?;
    Ok(())
}

// ===========================================================================
// finish_heap_swap   (cluster.c:1445)
// ===========================================================================

/// `finish_heap_swap(...)`.
pub fn finish_heap_swap(
    mcx: Mcx<'_>,
    OIDOldHeap: Oid,
    OIDNewHeap: Oid,
    is_system_catalog: bool,
    swap_toast_by_content: bool,
    check_constraints: bool,
    is_internal: bool,
    frozenXid: TransactionId,
    cutoffMulti: MultiXactId,
    newrelpersistence: u8,
) -> PgResult<()> {
    let mut mapped_tables = MappedTables::new();
    let _reindex_params = ReindexParams::default();

    /* Report that we are now swapping relation files */
    backend_progress::pgstat_progress_update_param::call(
        PROGRESS_CLUSTER_PHASE,
        PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES,
    )?;

    /*
     * Swap the contents of the heap relations (including any toast tables).
     * Also set old heap's relfrozenxid to frozenXid.
     */
    swap_relation_files(
        mcx,
        OIDOldHeap,
        OIDNewHeap,
        OIDOldHeap == RelationRelationId,
        swap_toast_by_content,
        is_internal,
        frozenXid,
        cutoffMulti,
        &mut mapped_tables,
    )?;

    /* If a system catalog, queue a sinval to flush all catcaches at CCI. */
    if is_system_catalog {
        inval::cache_invalidate_catalog::call(OIDOldHeap)?;
    }

    /* Rebuild each index on the relation (but not the toast table). */
    let mut reindex_flags = REINDEX_REL_SUPPRESS_INDEX_USE;
    if check_constraints {
        reindex_flags |= REINDEX_REL_CHECK_CONSTRAINTS;
    }

    /* Ensure the indexes have the same persistence as the parent relation. */
    if newrelpersistence == RELPERSISTENCE_UNLOGGED {
        reindex_flags |= REINDEX_REL_FORCE_INDEXES_UNLOGGED;
    } else if newrelpersistence == RELPERSISTENCE_PERMANENT {
        reindex_flags |= REINDEX_REL_FORCE_INDEXES_PERMANENT;
    }

    /* Report that we are now reindexing relations */
    backend_progress::pgstat_progress_update_param::call(
        PROGRESS_CLUSTER_PHASE,
        PROGRESS_CLUSTER_PHASE_REBUILD_INDEX,
    )?;

    let _ = index::reindex_relation::call(mcx, None, OIDOldHeap, reindex_flags, _reindex_params)?;

    /* Report that we are now doing clean up */
    backend_progress::pgstat_progress_update_param::call(
        PROGRESS_CLUSTER_PHASE,
        PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP,
    )?;

    /*
     * If the relation being rebuilt is pg_class, swap_relation_files couldn't
     * update pg_class's own entry; do it now using the new relation's indices.
     */
    if OIDOldHeap == RelationRelationId {
        let relRelation = table::table_open::call(mcx, RelationRelationId, RowExclusiveLock)?;

        let Some((tid, mut relform)) = syscache::search_syscache_copy_pg_class::call(mcx, OIDOldHeap)?
        else {
            return elog_error(format!("cache lookup failed for relation {}", OIDOldHeap));
        };
        relform.relfrozenxid = frozenXid;
        relform.relminmxid = cutoffMulti;
        indexing::catalog_tuple_update_pg_class::call(mcx, &relRelation, tid, &relform)?;

        relRelation.close(RowExclusiveLock)?;
    }

    /*
     * Destroy new heap with old filenumber. The new relation is local to our
     * transaction and nothing depends on it, so DROP_RESTRICT is OK.
     */
    dependency::perform_deletion::call(
        RelationRelationId,
        OIDNewHeap,
        0,
        types_nodes_drop_restrict(),
        PERFORM_DELETION_INTERNAL,
    )?;
    /* performDeletion does CommandCounterIncrement at end */

    /* Remove any relation mapping entries we set up for the transient table. */
    let mut i = 0usize;
    while i < mapped_tables.slots.len() && OidIsValid(mapped_tables.slots[i]) {
        relmapper::relation_map_remove_mapping::call(mapped_tables.slots[i])?;
        i += 1;
    }

    /*
     * If we did toast swap by links, rename the toast table (whose name still
     * corresponds to the transient table) to prevent user confusion.
     */
    if !swap_toast_by_content {
        let newrel = table::table_open::call(mcx, OIDOldHeap, NoLock)?;
        let reltoastrelid = newrel.rd_rel.reltoastrelid;
        if OidIsValid(reltoastrelid) {
            /* Get the associated valid index to be renamed */
            let toastidx = toast_internals::toast_get_valid_index::call(mcx, reltoastrelid, NoLock)?;

            /* rename the toast table ... */
            let new_toast_name = format_namedata(&format!("pg_toast_{}", OIDOldHeap));
            tablecmds::rename_relation_internal::call(mcx, reltoastrelid, &new_toast_name, true, false)?;

            /* ... and its valid index too. */
            let new_toast_index_name = format_namedata(&format!("pg_toast_{}_index", OIDOldHeap));
            tablecmds::rename_relation_internal::call(mcx, toastidx, &new_toast_index_name, true, true)?;

            /*
             * Reset the relrewrite for the toast. The CCI is required here as
             * we are about to update the tuple updated by RenameRelationInternal.
             */
            xact::command_counter_increment::call()?;
            tablecmds::reset_rel_rewrite::call(reltoastrelid)?;
        }
        newrel.close(NoLock)?;
    }

    /* if it's not a catalog table, clear any missing attribute settings */
    if !is_system_catalog {
        let newrel = table::table_open::call(mcx, OIDOldHeap, NoLock)?;
        heap::relation_clear_missing::call(&newrel)?;
        newrel.close(NoLock)?;
    }

    Ok(())
}

/// `DROP_RESTRICT` as the dependency seam's `DropBehavior`.
fn types_nodes_drop_restrict() -> nodes::parsenodes::DropBehavior {
    nodes::parsenodes::DROP_RESTRICT
}

// ===========================================================================
// get_tables_to_cluster   (cluster.c:1643)
// ===========================================================================

/// `get_tables_to_cluster(MemoryContext cluster_context)`. The `rtcs` list is
/// allocated in `mcx` (the C `cluster_context`). The pg_index `indisclustered`
/// scan crosses as the batched [`heapam::scan_indisclustered`] seam (open +
/// beginscan_catalog + heap_getnext loop + endscan + close), exactly the
/// genam `systable_scan` precedent; the per-row aclcheck + push stays here.
fn get_tables_to_cluster<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, RelToCluster>> {
    let mut rtcs: PgVec<RelToCluster> = PgVec::new_in(mcx);

    /*
     * Get all indexes that have indisclustered set and that the current user
     * has the appropriate privileges for.
     */
    let rows = heapam::scan_indisclustered::call(mcx)?;
    for (indrelid, indexrelid) in rows {
        if !cluster_is_permitted_for_relation(mcx, indrelid, miscinit::get_user_id::call())? {
            continue;
        }
        push_rtc(mcx, &mut rtcs, RelToCluster { tableOid: indrelid, indexOid: indexrelid })?;
    }

    Ok(rtcs)
}

// ===========================================================================
// get_tables_to_cluster_partitioned   (cluster.c:1697)
// ===========================================================================

fn get_tables_to_cluster_partitioned<'mcx>(
    mcx: Mcx<'mcx>,
    indexOid: Oid,
) -> PgResult<PgVec<'mcx, RelToCluster>> {
    let mut rtcs: PgVec<RelToCluster> = PgVec::new_in(mcx);

    /* Do not lock the children until they're processed */
    let inhoids = pg_inherits::find_all_inheritors::call(mcx, indexOid, NoLock)?;

    for indexrelid in inhoids {
        let relid = index::index_get_relation::call(indexrelid, false)?;

        /* consider only leaf indexes */
        if lsyscache::get_rel_relkind::call(indexrelid)? != RELKIND_INDEX {
            continue;
        }

        /*
         * The user may lack privileges to CLUSTER the leaf partition despite
         * having them on the partitioned table; skip those.
         */
        if !cluster_is_permitted_for_relation(mcx, relid, miscinit::get_user_id::call())? {
            continue;
        }

        push_rtc(mcx, &mut rtcs, RelToCluster { tableOid: relid, indexOid: indexrelid })?;
    }

    Ok(rtcs)
}

// ===========================================================================
// cluster_is_permitted_for_relation   (cluster.c:1745)
// ===========================================================================

fn cluster_is_permitted_for_relation(mcx: Mcx<'_>, relid: Oid, userid: Oid) -> PgResult<bool> {
    if acl::pg_class_aclcheck_maintain_ok::call(relid, userid)? {
        return Ok(true);
    }

    ereport(WARNING)
        .errmsg(format!(
            "permission denied to cluster \"{}\", skipping it",
            lsyscache::get_rel_name::call(mcx, relid)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default()
        ))
        .finish(here("cluster_is_permitted_for_relation"))?;
    Ok(false)
}

/// Append one `RelToCluster` to the result list with a fallible (`try_reserve`)
/// grow charged to `mcx`, matching C's `palloc`+`lappend` (OOM => `ERROR`).
fn push_rtc<'mcx>(
    mcx: Mcx<'mcx>,
    rtcs: &mut PgVec<'mcx, RelToCluster>,
    rtc: RelToCluster,
) -> PgResult<()> {
    rtcs.try_reserve(1).map_err(|_| {
        mcx.oom(core::mem::size_of::<RelToCluster>())
    })?;
    rtcs.push(rtc);
    Ok(())
}

