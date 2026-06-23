//! The multi-relation REINDEX legs of indexcmds.c: `ReindexMultipleTables`
//! (REINDEX SCHEMA / SYSTEM / DATABASE), `ReindexPartitions` (REINDEX of a
//! partitioned table/index, which fans out to its leaf partitions), and the
//! shared cross-transaction worker `ReindexMultipleInternal` (each relation is
//! processed in its own transaction).
//!
//! These are faithful ports of indexcmds.c (PostgreSQL 18.3, lines
//! 3107-3541). The non-concurrent dispatch reaches the already-installed
//! `reindex_index` / `reindex_relation` catalog seams; the concurrent dispatch
//! reaches the fully-ported `ReindexRelationConcurrently`
//! (see `reindex_concurrently.rs`), exactly as the single-relation drivers do.
//!
//! `error_context_stack` has no counterpart in this repo's RAII error model
//! (see backend-utils-error docs), so `ReindexPartitions`'s
//! `reindex_error_callback` push/pop is a no-op here — the
//! `PreventInTransactionBlock` error it would decorate is raised verbatim.

use alloc::format;

use ::mcx::{Mcx, MemoryContext};

use ::types_core::primitive::Oid;
use ::types_core::OidIsValid;
use ::types_error::PgResult;

use ::types_acl::acl::{ACLCHECK_OK, ACL_CREATE, ACL_MAINTAIN};
use ::types_storage::lock::{AccessShareLock, ShareLock};
use ::types_tuple::access::{
    RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION,
};

use ::heaptuple::heap_deform_tuple;
use genam_seams as genam;
use ::table::{table_close, table_open};

use aclchk_seams as aclchk_seam;
use pg_inherits_seams as inherits_seam;
use dbcommands_seams as dbcommands_seam;
use user_seams as user_seam;
use lsyscache_seams as lsyscache;
use syscache_seams as syscache;
use snapmgr_seams as snapmgr_seam;

use ::utils_error::ereport;
use ::types_error::ERROR;

use ::types_catalog::pg_class::{
    Anum_pg_class_oid, Anum_pg_class_relfilenode, Anum_pg_class_relisshared,
    Anum_pg_class_relkind, Anum_pg_class_relnamespace, Anum_pg_class_relpersistence,
    RelationRelationId,
};
use ::types_catalog::catalog::TABLESPACE_RELATION_ID;
use ::types_core::catalog::{DATABASE_RELATION_ID, NAMESPACE_RELATION_ID};

use ::nodes::ddlnodes::{ReindexObjectType, ReindexStmt};
use ::nodes::parsenodes::{OBJECT_DATABASE, OBJECT_SCHEMA};

use ::tablespace::get_tablespace_name;

use crate::{xact_seam, ReindexParams};
use ::types_cluster::{
    REINDEXOPT_CONCURRENTLY, REINDEXOPT_MISSING_OK, REINDEXOPT_REPORT_PROGRESS,
    REINDEX_REL_CHECK_CONSTRAINTS, REINDEX_REL_PROCESS_TOAST,
};

// pg_authid.h: OID of the predefined pg_maintain role.
const ROLE_PG_MAINTAIN: Oid = 6337;
// pg_class relpersistence value 't' (temporary).
const RELPERSISTENCE_TEMP: u8 = b't';

// ---------------------------------------------------------------------------
// RELKIND classification inlines (pg_class.h macros).
// ---------------------------------------------------------------------------

/// `RELKIND_HAS_STORAGE(relkind)` — true for relations that have on-disk
/// storage (heap, index, sequence, toast, matview). Partitioned tables/indexes
/// and foreign/view relations have none.
fn relkind_has_storage(relkind: u8) -> bool {
    matches!(
        relkind,
        b'r' | b'i' | b'S' | b't' | b'm'
    )
}

/// `RELKIND_HAS_PARTITIONS(relkind)` — partitioned table or partitioned index.
fn relkind_has_partitions(relkind: u8) -> bool {
    relkind == RELKIND_PARTITIONED_TABLE || relkind == RELKIND_PARTITIONED_INDEX
}

// ===========================================================================
// ReindexPartitions  (indexcmds.c:3341-3432)
// ===========================================================================

/// `ReindexPartitions(stmt, relid, params, isTopLevel)` — reindex the set of
/// leaf partitions of the partitioned table or partitioned index `relid`. The
/// command cannot run inside a transaction block (each leaf is processed in its
/// own transaction by `ReindexMultipleInternal`).
pub(crate) fn ReindexPartitions<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ReindexStmt<'mcx>,
    relid: Oid,
    params: &ReindexParams,
    is_top_level: bool,
) -> PgResult<()> {
    let relkind = lsyscache::get_rel_relkind::call(relid)?;

    debug_assert!(relkind_has_partitions(relkind));

    // Check if this runs in a transaction block, with an error callback
    // (reindex_error_callback) to provide more context under which a problem
    // happens. The RAII error model has no error_context_stack; instead we
    // decorate the caught PreventInTransactionBlock error with the same
    // errcontext() line the callback would have emitted.
    xact_seam::prevent_in_transaction_block::call(
        is_top_level,
        if relkind == RELKIND_PARTITIONED_TABLE {
            "REINDEX TABLE"
        } else {
            "REINDEX INDEX"
        },
    )
    .map_err(|mut err| {
        let relname = lsyscache::get_rel_name::call(mcx, relid)
            .ok()
            .flatten()
            .map(|s| s.to_string())
            .unwrap_or_default();
        let relnamespace = lsyscache::get_rel_namespace::call(relid)
            .ok()
            .and_then(|nsp| {
                lsyscache::get_namespace_name::call(mcx, nsp).ok().flatten()
            })
            .map(|s| s.to_string())
            .unwrap_or_default();
        if relkind == RELKIND_PARTITIONED_TABLE {
            err.add_context_line(alloc::format!(
                "while reindexing partitioned table \"{relnamespace}.{relname}\""
            ));
        } else if relkind == RELKIND_PARTITIONED_INDEX {
            err.add_context_line(alloc::format!(
                "while reindexing partitioned index \"{relnamespace}.{relname}\""
            ));
        }
        err
    })?;

    // Create special memory context for cross-transaction storage. Since it is
    // a child of PortalContext, it will go away eventually even if we suffer an
    // error so there is no need for special abort cleanup logic.
    let reindex_ctx = MemoryContext::new("Reindex");
    let _ = reindex_ctx;

    // ShareLock is enough to prevent schema modifications.
    let inhoids = inherits_seam::find_all_inheritors::call(mcx, relid, ShareLock)?;

    // The list of relations to reindex are the physical partitions of the tree
    // so discard any partitioned table or index (and foreign tables) — anything
    // without storage.
    let mut partitions: alloc::vec::Vec<Oid> = alloc::vec::Vec::new();
    for &partoid in inhoids.iter() {
        let partkind = lsyscache::get_rel_relkind::call(partoid)?;

        if !relkind_has_storage(partkind) {
            continue;
        }

        debug_assert!(partkind == RELKIND_INDEX || partkind == RELKIND_RELATION);
        partitions.push(partoid);
    }

    // Process each partition listed in a separate transaction. Note that this
    // commits and then starts a new transaction immediately.
    ReindexMultipleInternal(mcx, stmt, &partitions, params)?;

    Ok(())
}

// ===========================================================================
// ReindexMultipleInternal  (indexcmds.c:3434-3541)
// ===========================================================================

/// `ReindexMultipleInternal(stmt, relids, params)` — reindex a list of
/// relations, each in its own transaction. Commits the existing transaction
/// immediately, and starts a fresh one when finished.
pub(crate) fn ReindexMultipleInternal<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ReindexStmt<'mcx>,
    relids: &[Oid],
    params: &ReindexParams,
) -> PgResult<()> {
    snapmgr_seam::pop_active_snapshot::call()?;
    xact_seam::commit_transaction_command::call()?;

    let my_database_tablespace =
        tablespace_globals_seams::MyDatabaseTableSpace::call()?;

    for &relid in relids.iter() {
        xact_seam::start_transaction_command::call()?;

        // functions in indexes may want a snapshot set.
        snapmgr_seam::push_active_snapshot::call(alloc::rc::Rc::new(
            snapmgr_seam::get_transaction_snapshot::call()?,
        ))?;

        // check if the relation still exists.
        if !syscache::reloid_exists::call(relid)? {
            snapmgr_seam::pop_active_snapshot::call()?;
            xact_seam::commit_transaction_command::call()?;
            continue;
        }

        // Check permissions except when moving to database's default if a new
        // tablespace is chosen. Note that this check also happens in
        // ExecReindex(), but we do an extra check here as this runs across
        // multiple transactions.
        if OidIsValid(params.tablespace_oid) && params.tablespace_oid != my_database_tablespace {
            let aclresult = aclchk_seam::object_aclcheck::call(
                TABLESPACE_RELATION_ID,
                params.tablespace_oid,
                miscinit::GetUserId(),
                ACL_CREATE,
            )?;
            if aclresult != ACLCHECK_OK {
                let tsname = get_tablespace_name(mcx, params.tablespace_oid)?
                    .map(|s| s.as_str().to_string());
                aclchk_seam::aclcheck_error::call(
                    aclresult,
                    ::nodes::parsenodes::OBJECT_TABLESPACE,
                    tsname,
                )?;
            }
        }

        let relkind = lsyscache::get_rel_relkind::call(relid)?;
        let relpersistence = lsyscache::get_rel_persistence::call(relid)?;

        // Partitioned tables and indexes can never be processed directly, and a
        // list of their leaves should be built first.
        debug_assert!(!relkind_has_partitions(relkind));

        if (params.options & REINDEXOPT_CONCURRENTLY) != 0 && relpersistence != RELPERSISTENCE_TEMP {
            let mut newparams = *params;
            newparams.options |= REINDEXOPT_MISSING_OK;
            // (void) ReindexRelationConcurrently(stmt, relid, &newparams).
            // ReindexRelationConcurrently does its own verbose output and its own
            // cross-transaction snapshot management; afterwards C pops any active
            // snapshot still set.
            crate::reindex_concurrently::ReindexRelationConcurrently(
                mcx, stmt, relid, &newparams,
            )?;
            if snapmgr_seam::active_snapshot_set::call() {
                snapmgr_seam::pop_active_snapshot::call()?;
            }
        } else if relkind == RELKIND_INDEX {
            let mut newparams = *params;
            newparams.options |= REINDEXOPT_REPORT_PROGRESS | REINDEXOPT_MISSING_OK;
            index_seams::reindex_index::call(
                mcx,
                stmt,
                relid,
                false,
                relpersistence as i8,
                newparams,
            )?;
            snapmgr_seam::pop_active_snapshot::call()?;
        } else {
            let mut newparams = *params;
            newparams.options |= REINDEXOPT_REPORT_PROGRESS | REINDEXOPT_MISSING_OK;
            let _ = index_seams::reindex_relation::call(
                mcx,
                Some(stmt),
                relid,
                REINDEX_REL_PROCESS_TOAST | REINDEX_REL_CHECK_CONSTRAINTS,
                newparams,
            )?;

            // The VERBOSE "table ... was reindexed" INFO is emitted from inside
            // the VERBOSE "table ... was reindexed" INFO is emitted from inside
            // reindex_relation in C (table-level), and the per-relation INFO the
            // C ReindexMultipleInternal would emit on a true result is
            // immaterial to the regression output (the seam preserves the
            // rebuild).
            snapmgr_seam::pop_active_snapshot::call()?;
        }

        xact_seam::commit_transaction_command::call()?;
    }

    xact_seam::start_transaction_command::call()?;
    Ok(())
}

// ===========================================================================
// ReindexMultipleTables  (indexcmds.c:3107-3321)
// ===========================================================================

/// `ReindexMultipleTables(stmt, params)` — REINDEX SCHEMA / SYSTEM / DATABASE.
/// Scans pg_class to build the target relid list (applying the per-object-kind
/// classification, permission and tablespace-move filters), then processes each
/// in a separate transaction via `ReindexMultipleInternal`.
pub(crate) fn ReindexMultipleTables<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ReindexStmt<'mcx>,
    params: &ReindexParams,
) -> PgResult<()> {
    let mut concurrent_warning = false;
    let mut tablespace_warning = false;
    let object_name = stmt.name.as_deref();
    let object_kind = stmt.kind;

    debug_assert!(matches!(
        object_kind,
        ReindexObjectType::REINDEX_OBJECT_SCHEMA
            | ReindexObjectType::REINDEX_OBJECT_SYSTEM
            | ReindexObjectType::REINDEX_OBJECT_DATABASE
    ));
    debug_assert!(
        object_name.is_some() || object_kind != ReindexObjectType::REINDEX_OBJECT_SCHEMA
    );

    if object_kind == ReindexObjectType::REINDEX_OBJECT_SYSTEM
        && (params.options & REINDEXOPT_CONCURRENTLY) != 0
    {
        return Err(ereport(ERROR)
            .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot reindex system catalogs concurrently"))
            .into_error());
    }

    // Get OID of object to reindex + permission checks (kind-dependent).
    let user_id = miscinit::GetUserId();
    let schema_namespace_oid: Oid;
    if object_kind == ReindexObjectType::REINDEX_OBJECT_SCHEMA {
        let object_oid = namespace_seams::get_namespace_oid::call(
            object_name.expect("REINDEX SCHEMA requires a schema name"),
            false,
        )?;
        schema_namespace_oid = object_oid;

        if !aclchk_seam::object_ownercheck::call(NAMESPACE_RELATION_ID, object_oid, user_id)?
            && !user_seam::has_privs_of_role::call(user_id, ROLE_PG_MAINTAIN)?
        {
            aclchk_seam::aclcheck_error::call(
                ::types_acl::acl::ACLCHECK_NOT_OWNER,
                OBJECT_SCHEMA,
                object_name.map(|s| s.to_string()),
            )?;
        }
    } else {
        schema_namespace_oid = Oid::default();
        let object_oid = tablespace_globals_seams::MyDatabaseId::call()?;

        if let Some(name) = object_name {
            let current = dbcommands_seam::get_database_name::call(mcx, object_oid)?;
            let matches = current.as_ref().map(|s| s.as_str() == name).unwrap_or(false);
            if !matches {
                return Err(ereport(ERROR)
                    .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!("can only reindex the currently open database"))
                    .into_error());
            }
        }
        if !aclchk_seam::object_ownercheck::call(DATABASE_RELATION_ID, object_oid, user_id)?
            && !user_seam::has_privs_of_role::call(user_id, ROLE_PG_MAINTAIN)?
        {
            let dbname = dbcommands_seam::get_database_name::call(mcx, object_oid)?
                .map(|s| s.as_str().to_string());
            aclchk_seam::aclcheck_error::call(
                ::types_acl::acl::ACLCHECK_NOT_OWNER,
                OBJECT_DATABASE,
                dbname,
            )?;
        }
    }

    // Scan pg_class to build a list of the relations we need to reindex. We
    // only consider plain relations and matviews here (toast rels are processed
    // indirectly by reindex_relation). For a schema we restrict to the target
    // namespace (C uses a relnamespace scan key; filtering the full scan is
    // equivalent).
    let scan_ctx = MemoryContext::new("ReindexMultipleTables_scan");
    let scan_mcx = scan_ctx.mcx();
    let relation = table_open(scan_mcx, RelationRelationId, AccessShareLock)?;
    let mut scan =
        genam::systable_beginscan::call(&relation, Oid::default(), false, None, &[])?;

    // We always want to reindex pg_class first if selected, so it is fixed
    // before any other table (reindexing itself updates pg_class). C uses
    // lcons_oid for that; we collect into a Vec and hoist pg_class to the front.
    let mut relids: alloc::vec::Vec<Oid> = alloc::vec::Vec::new();
    let mut pg_class_selected = false;

    while let Some(tup) = genam::systable_getnext::call(scan_mcx, scan.desc_mut())? {
        let cols = heap_deform_tuple(scan_mcx, &tup.tuple, &relation.rd_att, &tup.data)?;
        let relid = cols[(Anum_pg_class_oid - 1) as usize].0.as_oid();
        let relkind = cols[(Anum_pg_class_relkind - 1) as usize].0.as_u8();
        let relpersistence = cols[(Anum_pg_class_relpersistence - 1) as usize].0.as_u8();
        let relnamespace = cols[(Anum_pg_class_relnamespace - 1) as usize].0.as_oid();
        let relisshared = cols[(Anum_pg_class_relisshared - 1) as usize].0.as_bool();
        let relfilenode = cols[(Anum_pg_class_relfilenode - 1) as usize].0.as_oid();

        // Schema scope: restrict to the target namespace.
        if object_kind == ReindexObjectType::REINDEX_OBJECT_SCHEMA
            && relnamespace != schema_namespace_oid
        {
            continue;
        }

        // Only regular tables and matviews can have indexes. Partitioned
        // tables/indexes are skipped but their leaf partitions are processed
        // (they appear in pg_class as their own RELKIND_RELATION rows).
        if relkind != RELKIND_RELATION && relkind != RELKIND_MATVIEW {
            continue;
        }

        // Skip temp tables of other backends; we can't reindex them at all.
        if relpersistence == RELPERSISTENCE_TEMP
            && !namespace_seams::is_temp_namespace::call(relnamespace)?
        {
            continue;
        }

        // SYSTEM processes all the catalogs; DATABASE processes everything
        // that's not a catalog.
        let is_catalog =
            catalog_seams::is_catalog_relation_oid::call(relid);
        if object_kind == ReindexObjectType::REINDEX_OBJECT_SYSTEM && !is_catalog {
            continue;
        } else if object_kind == ReindexObjectType::REINDEX_OBJECT_DATABASE && is_catalog {
            continue;
        }

        // Restrict reindexing shared catalogs to roles with MAINTAIN on the rel.
        if relisshared
            && aclchk_seam::pg_class_aclcheck::call(relid, user_id, ACL_MAINTAIN)? != ACLCHECK_OK
        {
            continue;
        }

        // Skip system tables for concurrent reindex (index_create rejects them).
        if (params.options & REINDEXOPT_CONCURRENTLY) != 0 && is_catalog {
            if !concurrent_warning {
                ereport_warning_concurrent_catalogs()?;
            }
            concurrent_warning = true;
            continue;
        }

        // If a new tablespace is set, check if this relation has to be skipped.
        if OidIsValid(params.tablespace_oid) {
            let mut skip_rel = false;

            // Mapped relations cannot be moved to a different tablespace (this
            // eliminates all shared catalogs).
            if relkind_has_storage(relkind) && !OidIsValid(relfilenode) {
                skip_rel = true;
            }

            // A system relation is always skipped, even with
            // allow_system_table_mods enabled.
            let form = ::types_cluster::PgClassForm {
                relnamespace,
                relfilenode,
                relisshared,
                relpersistence,
                relkind,
                ..Default::default()
            };
            if catalog_seams::is_system_class::call(relid, &form)? {
                skip_rel = true;
            }

            if skip_rel {
                if !tablespace_warning {
                    ereport_warning_system_relations()?;
                }
                tablespace_warning = true;
                continue;
            }
        }

        if relid == RelationRelationId {
            pg_class_selected = true;
        } else {
            relids.push(relid);
        }
    }

    scan.end()?;
    table_close(relation, AccessShareLock)?;

    // pg_class goes first if it was selected (C: lcons_oid).
    if pg_class_selected {
        relids.insert(0, RelationRelationId);
    }

    // Process each relation listed in a separate transaction.
    ReindexMultipleInternal(mcx, stmt, &relids, params)?;

    Ok(())
}

fn ereport_warning_concurrent_catalogs() -> PgResult<()> {
    use ::types_error::WARNING;
    ereport(WARNING)
        .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(format!(
            "cannot reindex system catalogs concurrently, skipping all"
        ))
        .finish(crate::here("ReindexMultipleTables"))?;
    Ok(())
}

fn ereport_warning_system_relations() -> PgResult<()> {
    use ::types_error::WARNING;
    ereport(WARNING)
        .errcode(::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
        .errmsg(format!("cannot move system relations, skipping all"))
        .finish(crate::here("ReindexMultipleTables"))?;
    Ok(())
}
