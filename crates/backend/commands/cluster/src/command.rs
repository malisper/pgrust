// cluster.c command surface: cluster()/cluster_rel/rebuild_relation/
// copy_table_data + the indisclustered maintenance. VACUUM FULL enters via
// cluster_seams::cluster_rel. Tables with a toast relation keep a named LOUD
// (heaptoast's rd_toastoid preserve lane is unported), which also fences off
// swap-by-content.
use crate::{finish_heap_swap, make_new_heap, oid_key, unported};

use mcx::Mcx;
use types_core::{InvalidOid, Oid, INDEX_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, WARNING};
use types_nodes::parsenodes::ClusterStmt;
use types_rel::{
    AccessExclusiveLock, AccessShareLock, NoLock, Relation, RowExclusiveLock,
    RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_TOASTVALUE,
};
use types_scan::scankey::ScanKeyData;

pub const CLUOPT_VERBOSE: u32 = 0x01;
pub const CLUOPT_RECHECK: u32 = 0x02;
pub const CLUOPT_RECHECK_ISCLUSTERED: u32 = 0x04;

const BTREE_AM_OID: Oid = 403;
const Anum_pg_index_indexrelid: usize = 1;
const Anum_pg_index_indrelid: usize = 2;
const Anum_pg_index_indisclustered: usize = 10;
const Anum_pg_index_indisvalid: usize = 11;
const IndexRelidIndexId: Oid = 2679;
const Natts_pg_index: usize = 21;

struct RelToCluster {
    table_oid: Oid,
    index_oid: Oid,
}

pub fn cluster<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ClusterStmt<'mcx>,
    is_top_level: bool,
) -> PgResult<()> {
    let mut verbose = false;
    for opt_node in stmt.params.iter() {
        let opt = opt_node.as_def_elem().expect("ClusterStmt option is DefElem");
        match opt.defname.unwrap_or("") {
            "verbose" => verbose = explain::defGetBoolean(opt)?,
            name => {
                return Err(Box::new(
                    PgError::new(ERROR, format!("unrecognized CLUSTER option \"{name}\""))
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                ))
            }
        }
    }
    let mut options = if verbose { CLUOPT_VERBOSE } else { 0 };

    if let Some(rv_node) = stmt.relation {
        let rv = rv_node.as_range_var().expect("ClusterStmt.relation is RangeVar");
        let rv = rel_vocab::RangeVar {
            catalogname: rv.catalogname,
            schemaname: rv.schemaname,
            relname: rv.relname.expect("RangeVar.relname"),
            inh: rv.inh,
            relpersistence: rv.relpersistence,
            location: rv.location,
        };
        let mut cb =
            |rv2: &rel_vocab::RangeVar<'_>, rel_id: Oid, old_rel_id: Oid| -> PgResult<()> {
                tablecmds_seams::range_var_callback_maintains_table::call(rv2, rel_id, old_rel_id)
            };
        let table_oid = catalog_namespace::RangeVarGetRelidExtended(
            &rv,
            AccessExclusiveLock,
            0,
            Some(&mut cb),
        )?;
        let rel = table::table_open(mcx, table_oid, NoLock)?;

        // RELATION_IS_OTHER_TEMP: const-false single-backend.

        let index_oid = if let Some(indexname) = stmt.indexname {
            let idx = lsyscache::get_relname_relid(indexname, rel.rd_rel.relnamespace)?;
            if idx == InvalidOid {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "index \"{indexname}\" for table \"{}\" does not exist",
                            rv.relname
                        ),
                    )
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
                ));
            }
            idx
        } else {
            let mut found = InvalidOid;
            for &idx in relcache::RelationGetIndexList(mcx, table_oid)?.iter() {
                if lsyscache::get_index_isclustered(idx)? {
                    found = idx;
                    break;
                }
            }
            if found == InvalidOid {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "there is no previously clustered index for table \"{}\"",
                            rv.relname
                        ),
                    )
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
                ));
            }
            found
        };

        if rel.rd_rel.relkind != RELKIND_PARTITIONED_TABLE {
            let params = ClusterParams { options };
            return cluster_rel(mcx, rel, index_oid, &params);
        }
        unported("cluster: partitioned tables (get_tables_to_cluster_partitioned)");
    }

    // Multi-relation CLUSTER: each table in its own transaction.
    xact::PreventInTransactionBlock(is_top_level, "CLUSTER")?;
    options |= CLUOPT_RECHECK | CLUOPT_RECHECK_ISCLUSTERED;
    let rtcs = get_tables_to_cluster(mcx)?;
    let params = ClusterParams { options };
    cluster_multiple_rels(mcx, &rtcs, &params)?;
    xact::StartTransactionCommand()?;
    Ok(())
}

#[derive(Clone, Copy)]
pub struct ClusterParams {
    pub options: u32,
}

fn cluster_multiple_rels<'mcx>(
    mcx: Mcx<'mcx>,
    rtcs: &[RelToCluster],
    params: &ClusterParams,
) -> PgResult<()> {
    if snapmgr::ActiveSnapshotSet() {
        snapmgr::PopActiveSnapshot()?;
    }
    xact::CommitTransactionCommand()?;
    for rtc in rtcs {
        xact::StartTransactionCommand()?;
        let snapshot = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snapshot)?;
        let rel = table::table_open(mcx, rtc.table_oid, AccessExclusiveLock)?;
        cluster_rel(mcx, rel, rtc.index_oid, params)?;
        snapmgr::PopActiveSnapshot()?;
        xact::CommitTransactionCommand()?;
    }
    Ok(())
}

pub fn cluster_rel<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap: Relation<'mcx>,
    index_oid: Oid,
    params: &ClusterParams,
) -> PgResult<()> {
    let table_oid = old_heap.rd_id;
    let verbose = params.options & CLUOPT_VERBOSE != 0;
    let recheck = params.options & CLUOPT_RECHECK != 0;
    if verbose {
        unported("cluster_rel: VERBOSE (pg_rusage lane)");
    }
    postgres_seams::check_for_interrupts::call()?;

    let guard = miscinit::SecContextGuard::security_restricted(old_heap.rd_rel.relowner);
    let save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    let result = (|| -> PgResult<()> {
        if recheck {
            if !cluster_is_permitted_for_relation(mcx, table_oid, miscinit::GetUserId())? {
                return old_heap.close(NoLock);
            }
            if index_oid != InvalidOid {
                if lsyscache::get_rel_name(mcx, index_oid)?.is_none() {
                    return old_heap.close(NoLock);
                }
                if params.options & CLUOPT_RECHECK_ISCLUSTERED != 0
                    && !lsyscache::get_index_isclustered(index_oid)?
                {
                    return old_heap.close(NoLock);
                }
            }
        }

        if index_oid != InvalidOid && old_heap.rd_rel.relisshared {
            return Err(feature_err("cannot cluster a shared catalog"));
        }
        // RELATION_IS_OTHER_TEMP: const-false single-backend.
        catalog_heap::CheckTableNotInUse(
            &old_heap,
            if index_oid != InvalidOid { "CLUSTER" } else { "VACUUM" },
        )?;

        let index = if index_oid != InvalidOid {
            check_index_is_clusterable(mcx, &old_heap, index_oid, AccessExclusiveLock)?;
            Some(indexam::index_open(mcx, index_oid, NoLock)?)
        } else {
            None
        };

        if old_heap.rd_rel.relkind == RELKIND_MATVIEW {
            unported("cluster_rel: materialized views (RelationIsPopulated)");
        }
        debug_assert!(matches!(
            old_heap.rd_rel.relkind,
            RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE
        ));

        if xact::IsolationIsSerializable() {
            unported("cluster_rel: TransferPredicateLocksToHeapRelation (predicate.c)");
        }

        rebuild_relation(mcx, old_heap, index, verbose)
    })();

    guc::AtEOXact_GUC(false, save_nestlevel);
    guard.restore();
    result
}

pub fn check_index_is_clusterable<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap: &Relation<'mcx>,
    index_oid: Oid,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<()> {
    let old_index = indexam::index_open(mcx, index_oid, lockmode)?;
    let form = old_index.rd_index.as_ref();
    if form.map(|f| f.indrelid) != Some(old_heap.rd_id) {
        let err = Box::new(
            PgError::new(
                ERROR,
                format!(
                    "\"{}\" is not an index for table \"{}\"",
                    old_index.name(),
                    old_heap.name()
                ),
            )
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        );
        return Err(err);
    }
    let form = form.unwrap();
    // amclusterable: btree only among the ported AMs (hash is not clusterable).
    if old_index.rd_rel.relam != BTREE_AM_OID {
        return Err(feature_err(&format!(
            "cannot cluster on index \"{}\" because access method does not support clustering",
            old_index.name()
        )));
    }
    if form.has_indpred {
        return Err(feature_err(&format!(
            "cannot cluster on partial index \"{}\"",
            old_index.name()
        )));
    }
    if !form.indisvalid {
        return Err(feature_err(&format!(
            "cannot cluster on invalid index \"{}\"",
            old_index.name()
        )));
    }
    indexam::index_close(old_index, NoLock)
}

pub fn mark_index_clustered<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    index_oid: Oid,
    _is_internal: bool,
) -> PgResult<()> {
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Err(feature_err("cannot mark index clustered in partitioned table"));
    }
    if index_oid != InvalidOid && lsyscache::get_index_isclustered(index_oid)? {
        return Ok(());
    }

    let pg_index = table::table_open(mcx, INDEX_RELATION_ID, RowExclusiveLock)?;
    let desc = pg_index.descr();
    for &this_index in relcache::RelationGetIndexList(mcx, rel.rd_id)?.iter() {
        let key = [oid_key(Anum_pg_index_indexrelid, this_index)];
        let mut scan =
            genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &key)?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for index {this_index}"));
        let get_bool = |anum: usize| {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_index bool columns under its descriptor.
            let d = unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) };
            debug_assert!(!isnull);
            d.as_bool()
        };
        let indisclustered = get_bool(Anum_pg_index_indisclustered);
        let write = if indisclustered {
            Some(false)
        } else if this_index == index_oid {
            if !get_bool(Anum_pg_index_indisvalid) {
                panic!("cannot cluster on invalid index {index_oid}");
            }
            Some(true)
        } else {
            None
        };
        if let Some(v) = write {
            let mut values = [datum::Datum::null(); Natts_pg_index];
            let isnull = [false; Natts_pg_index];
            let mut replace = [false; Natts_pg_index];
            values[Anum_pg_index_indisclustered - 1] = datum::Datum::from_bool(v);
            replace[Anum_pg_index_indisclustered - 1] = true;
            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
            let otid = tup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &pg_index, &otid, &mut newtup)?;
        } else {
            genam::systable_endscan(mcx, scan)?;
        }
    }
    pg_index.close(RowExclusiveLock)
}

fn rebuild_relation<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap: Relation<'mcx>,
    index: Option<Relation<'mcx>>,
    verbose: bool,
) -> PgResult<()> {
    let table_oid = old_heap.rd_id;
    let relpersistence = old_heap.rd_rel.relpersistence;
    let is_system_catalog = catalog::IsSystemRelation(&old_heap);

    if let Some(ref index) = index {
        mark_index_clustered(mcx, &old_heap, index.rd_id, true)?;
    }

    // rd_options reloptions are not copied by our heap_create_with_catalog.
    if old_heap.rd_options.is_some() {
        unported("make_new_heap: reloptions copy");
    }
    let oid_new_heap = make_new_heap(mcx, table_oid, relpersistence, NoLock)?;
    let new_heap = table::table_open(mcx, oid_new_heap, NoLock)?;

    let (frozen_xid, cutoff_multi) =
        copy_table_data(mcx, &new_heap, &old_heap, index.as_ref(), verbose)?;

    old_heap.close(NoLock)?;
    if let Some(index) = index {
        indexam::index_close(index, NoLock)?;
    }
    new_heap.close(NoLock)?;

    finish_heap_swap(
        mcx,
        table_oid,
        oid_new_heap,
        is_system_catalog,
        false,
        false,
        true,
        frozen_xid,
        cutoff_multi,
        relpersistence,
    )
}

// copy_table_data (cluster.c) + heapam_relation_copy_for_cluster
// (heapam_handler.c, hosted here: heapam_handler cannot see indexam without
// cycling through tableam). Returns (FreezeXid, MultiXactCutoff).
fn copy_table_data<'mcx>(
    mcx: Mcx<'mcx>,
    new_heap: &Relation<'mcx>,
    old_heap: &Relation<'mcx>,
    old_index: Option<&Relation<'mcx>>,
    verbose: bool,
) -> PgResult<(u32, u32)> {
    debug_assert!(new_heap.rd_att.natts == old_heap.rd_att.natts);
    let _ = verbose;

    if old_heap.rd_rel.reltoastrelid != InvalidOid
        || new_heap.rd_rel.reltoastrelid != InvalidOid
    {
        unported("copy_table_data: toast rewrite (rd_toastoid preserve lane)");
    }

    // C memsets VacuumParams to zero: freeze ages 0 = freeze aggressively.
    let params = tableam_vocab::VacuumParams {
        options: 0,
        freeze_min_age: 0,
        freeze_table_age: 0,
        multixact_freeze_min_age: 0,
        multixact_freeze_table_age: 0,
        is_wraparound: false,
        log_min_duration: 0,
        index_cleanup: tableam_vocab::VacOptValue::Unspecified,
        truncate: tableam_vocab::VacOptValue::Unspecified,
        toast_parent: InvalidOid,
        max_eager_freeze_failure_rate: 0.0,
        nworkers: 0,
    };
    let (_aggressive, mut cutoffs) = commands_vacuum::vacuum_get_cutoffs(old_heap, &params)?;

    // FreezeLimit / MultiXactCutoff must not go backwards from the rel's own
    // horizons.
    {
        let relfrozenxid = old_heap.rd_rel.relfrozenxid;
        if types_core::xact::TransactionIdIsValid(relfrozenxid)
            && types_core::xact::TransactionIdPrecedes(cutoffs.FreezeLimit, relfrozenxid)
        {
            cutoffs.FreezeLimit = relfrozenxid;
        }
        let relminmxid = old_heap.rd_rel.relminmxid;
        if relminmxid != 0 && types_core::xact::MultiXactIdPrecedes(cutoffs.MultiXactCutoff, relminmxid)
        {
            cutoffs.MultiXactCutoff = relminmxid;
        }
    }

    let use_sort = match old_index {
        Some(index) if index.rd_rel.relam == BTREE_AM_OID => {
            planner::cluster::plan_cluster_use_sort(mcx, old_heap.rd_id, index.rd_id)?
        }
        _ => false,
    };

    let (num_tuples, _tups_vacuumed, _tups_recently_dead) = crate::copy::copy_for_cluster(
        mcx,
        old_heap,
        new_heap,
        old_index,
        use_sort,
        cutoffs.OldestXmin,
        &mut cutoffs.FreezeLimit,
        &mut cutoffs.MultiXactCutoff,
    )?;

    let num_pages = bufmgr::RelationGetNumberOfBlocksInFork(
        new_heap,
        types_core::ForkNumber::MAIN_FORKNUM,
    )?;

    // Update the transient rel's pg_class stats (not for pg_class itself).
    assert!(old_heap.rd_id != types_core::RELATION_RELATION_ID);
    {
        let rel_relation =
            table::table_open(mcx, types_core::RELATION_RELATION_ID, RowExclusiveLock)?;
        let desc = rel_relation.descr();
        let key = [oid_key(1, new_heap.rd_id)];
        let mut scan = genam::systable_beginscan(
            mcx,
            &rel_relation,
            catalog::ClassOidIndexId,
            true,
            None,
            &key,
        )?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for relation {}", new_heap.rd_id));
        let natts = desc.natts as usize;
        let mut values: mcx::PgVec<'_, datum::Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, datum::Datum::null());
        isnull.resize(natts, false);
        replace.resize(natts, false);
        values[crate::Anum_pg_class_relpages - 1] = datum::Datum::from_i32(num_pages as i32);
        replace[crate::Anum_pg_class_relpages - 1] = true;
        values[crate::Anum_pg_class_reltuples - 1] = datum::Datum::from_f32(num_tuples as f32);
        replace[crate::Anum_pg_class_reltuples - 1] = true;
        let mut newtup =
            heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
        let otid = tup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel_relation, &otid, &mut newtup)?;
        rel_relation.close(RowExclusiveLock)?;
    }
    xact::CommandCounterIncrement()?;

    Ok((cutoffs.FreezeLimit, cutoffs.MultiXactCutoff))
}

fn get_tables_to_cluster<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<RelToCluster>> {
    let ind_relation = table::table_open(mcx, INDEX_RELATION_ID, AccessShareLock)?;
    let mut entry = ScanKeyData::empty();
    entry.sk_attno = Anum_pg_index_indisclustered as types_core::AttrNumber;
    entry.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    entry.sk_collation = 0;
    entry.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_BOOLEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_BOOLEQ) failed: {e:?}"));
    entry.sk_argument = datum::Datum::from_bool(true);

    let mut scan =
        genam::systable_beginscan(mcx, &ind_relation, InvalidOid, false, None, &[entry])?;
    let desc = ind_relation.descr();
    let mut rtcs = Vec::new();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let get_oid = |anum: usize| {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_index oid columns under its descriptor.
            let d = unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) };
            d.as_oid()
        };
        let indrelid = get_oid(Anum_pg_index_indrelid);
        if !cluster_is_permitted_for_relation(mcx, indrelid, miscinit::GetUserId())? {
            continue;
        }
        rtcs.push(RelToCluster {
            table_oid: indrelid,
            index_oid: get_oid(Anum_pg_index_indexrelid),
        });
    }
    genam::systable_endscan(mcx, scan)?;
    ind_relation.close(AccessShareLock)?;
    Ok(rtcs)
}

fn cluster_is_permitted_for_relation<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    userid: Oid,
) -> PgResult<bool> {
    if aclchk::pg_class_aclcheck(relid, userid, adt_acl::ACL_MAINTAIN)? == aclchk::ACLCHECK_OK {
        return Ok(true);
    }
    elog_seams::ereport::call(
        PgError::new(
            WARNING,
            format!(
                "permission denied to cluster \"{}\", skipping it",
                lsyscache::get_rel_name(mcx, relid)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default()
            ),
        ),
    )?;
    Ok(false)
}

#[cold]
#[inline(never)]
fn feature_err(msg: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, msg.to_string()).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

pub fn init_seams() {
    cluster_seams::cluster_rel::set(seam_cluster_rel);
}

fn seam_cluster_rel<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap: Relation<'mcx>,
    index_oid: Oid,
    options: u32,
) -> PgResult<()> {
    cluster_rel(mcx, old_heap, index_oid, &ClusterParams { options })
}
