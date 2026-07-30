// cluster.c command surface: cluster()/cluster_rel/rebuild_relation/
// copy_table_data + the indisclustered maintenance. VACUUM FULL enters via
// cluster_seams::cluster_rel. Toasted tables rewrite with value-OID
// preservation and swap toast by content (C's rd_toastoid protocol).
use crate::{finish_heap_swap, make_new_heap, oid_key};

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

        // cluster.c:155 — the by-name gate. Their local buffer manager cannot
        // cope; C's comment at cluster.c:369 relies on this catching every
        // attempt to cluster a remote temp table by name.
        if rel.is_other_temp() {
            return Err(feature_err("cannot cluster temporary tables of other sessions"));
        }

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

        // Partitioned table: an index name was given, so indisclustered
        // needs no recheck, but the index must be clusterable.
        xact::PreventInTransactionBlock(is_top_level, "CLUSTER")?;
        options |= CLUOPT_RECHECK;
        check_index_is_clusterable(mcx, &rel, index_oid, AccessShareLock)?;
        let rtcs = get_tables_to_cluster_partitioned(mcx, index_oid)?;
        rel.close(AccessExclusiveLock)?;
        let params = ClusterParams { options };
        cluster_multiple_rels(mcx, &rtcs, &params)?;
        xact::StartTransactionCommand()?;
        return Ok(());
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
    rtcs: &mcx::PgVec<'mcx, RelToCluster>,
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
    postgres_seams::check_for_interrupts::call()?;

    let guard = miscinit::SecContextGuard::security_restricted(old_heap.rd_rel.relowner);
    let save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    let result = (|| -> PgResult<()> {
        if recheck {
            // save_userid: the sec context already switched to the table owner.
            if !cluster_is_permitted_for_relation(mcx, table_oid, guard.saved().0)? {
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
        // cluster.c:412-425, the two-armed guard. Note the VACUUM arm is
        // effectively dead: vacuum_rel skips other-session temp rels first
        // (commands/vacuum lib.rs, porting vacuum.c:2158), so VACUUM never
        // reaches here. Ported anyway to keep the site C-shaped.
        if old_heap.is_other_temp() {
            return Err(feature_err(if index_oid != InvalidOid {
                "cannot cluster temporary tables of other sessions"
            } else {
                "cannot vacuum temporary tables of other sessions"
            }));
        }
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
            // unported: cluster_rel materialized views (RelationIsPopulated)
            return Err(feature_err("clustering a materialized view is not supported yet"));
        }
        debug_assert!(matches!(
            old_heap.rd_rel.relkind,
            RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE
        ));

        predicate_seams::transfer_predicate_locks_to_heap_relation::call(&old_heap)?;

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

    let oid_new_heap =
        make_new_heap(mcx, table_oid, old_heap.rd_rel.reltablespace, old_heap.rd_rel.relam, relpersistence, NoLock)?;
    let new_heap = table::table_open(mcx, oid_new_heap, NoLock)?;

    let (frozen_xid, cutoff_multi, swap_toast_by_content) =
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
        swap_toast_by_content,
        false,
        true,
        frozen_xid,
        cutoff_multi,
        relpersistence,
    )
}

// copy_table_data (cluster.c) + heapam_relation_copy_for_cluster
// (heapam_handler.c, hosted here: heapam_handler cannot see indexam without
// cycling through tableam). Returns (FreezeXid, MultiXactCutoff,
// swap_toast_by_content).
fn copy_table_data<'mcx>(
    mcx: Mcx<'mcx>,
    new_heap: &Relation<'mcx>,
    old_heap: &Relation<'mcx>,
    old_index: Option<&Relation<'mcx>>,
    verbose: bool,
) -> PgResult<(u32, u32, bool)> {
    debug_assert!(new_heap.rd_att.natts == old_heap.rd_att.natts);
    let ru0 = pg_rusage::pg_rusage_init();
    let nspname = lsyscache::get_namespace_name(mcx, old_heap.namespace())?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();

    // Keep autovacuum off the old toast table for the whole rewrite: it could
    // remove DEAD toast tuples still referenced by RECENTLY_DEAD main tuples
    // we copy.
    if old_heap.rd_rel.reltoastrelid != InvalidOid {
        lmgr::LockRelationOid(old_heap.rd_rel.reltoastrelid, AccessExclusiveLock)?;
    }

    // Both heaps toasted: swap toast by content, and toast pointers written
    // into new_heap carry the old toast table's OID with value OIDs preserved
    // (C's NewHeap->rd_toastoid, threaded to toast_save_datum). Old-only
    // toast (droppable columns) falls back to swap by links.
    let swap_toast_by_content = old_heap.rd_rel.reltoastrelid != InvalidOid
        && new_heap.rd_rel.reltoastrelid != InvalidOid;
    let toastoid =
        if swap_toast_by_content { old_heap.rd_rel.reltoastrelid } else { InvalidOid };

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

    let elevel = if verbose { types_error::INFO } else { types_error::DEBUG2 };
    let what = match old_index {
        Some(index) if !use_sort => format!(
            "clustering \"{}.{}\" using index scan on \"{}\"",
            nspname,
            old_heap.name(),
            index.name()
        ),
        _ if use_sort => format!(
            "clustering \"{}.{}\" using sequential scan and sort",
            nspname,
            old_heap.name()
        ),
        _ => format!("vacuuming \"{}.{}\"", nspname, old_heap.name()),
    };
    elog::ereport(elevel)
        .errmsg(what)
        .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "copy_table_data"))?;

    let (num_tuples, tups_vacuumed, tups_recently_dead) = crate::copy::copy_for_cluster(
        mcx,
        old_heap,
        new_heap,
        old_index,
        use_sort,
        cutoffs.OldestXmin,
        &mut cutoffs.FreezeLimit,
        &mut cutoffs.MultiXactCutoff,
        toastoid,
    )?;

    let num_pages = bufmgr::RelationGetNumberOfBlocksInFork(
        new_heap,
        types_core::ForkNumber::MAIN_FORKNUM,
    )?;

    let old_pages = bufmgr::RelationGetNumberOfBlocksInFork(
        old_heap,
        types_core::ForkNumber::MAIN_FORKNUM,
    )?;
    elog::ereport(elevel)
        .errmsg(format!(
            "\"{}.{}\": found {:.0} removable, {:.0} nonremovable row versions in {} pages",
            nspname,
            old_heap.name(),
            tups_vacuumed,
            num_tuples,
            old_pages,
        ))
        .errdetail(format!(
            "{:.0} dead row versions cannot be removed yet.\n{}.",
            tups_recently_dead,
            pg_rusage::pg_rusage_show(&ru0).as_str(),
        ))
        .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "copy_table_data"))?;

    // Update the transient rel's pg_class stats. When rebuilding pg_class
    // itself the update would scribble on the data we're about to discard;
    // relcache inval alone suffices (cluster.c:1010-1026).
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
        if old_heap.rd_id != types_core::RELATION_RELATION_ID {
            catalog_indexing::CatalogTupleUpdate(mcx, &rel_relation, &otid, &mut newtup)?;
        } else {
            inval::invalidate::CacheInvalidateRelcacheByTuple(newtup.as_tuple())?;
        }
        rel_relation.close(RowExclusiveLock)?;
    }
    xact::CommandCounterIncrement()?;

    Ok((cutoffs.FreezeLimit, cutoffs.MultiXactCutoff, swap_toast_by_content))
}

fn get_tables_to_cluster<'mcx>(mcx: Mcx<'mcx>) -> PgResult<mcx::PgVec<'mcx, RelToCluster>> {
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
    let mut rtcs: mcx::PgVec<'mcx, RelToCluster> = mcx::PgVec::new_in(mcx);
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

fn get_tables_to_cluster_partitioned<'mcx>(
    mcx: Mcx<'mcx>,
    index_oid: Oid,
) -> PgResult<mcx::PgVec<'mcx, RelToCluster>> {
    // Children stay unlocked until each is processed.
    let inhoids = pg_inherits::find_all_inheritors(mcx, index_oid, NoLock)?;
    let mut rtcs: mcx::PgVec<'mcx, RelToCluster> = mcx::PgVec::new_in(mcx);
    for &indexrelid in inhoids.iter() {
        if lsyscache::get_rel_relkind(indexrelid)? as u8 != types_rel::RELKIND_INDEX {
            continue;
        }
        let relid = catalog_index::IndexGetRelation(mcx, indexrelid, false)?;
        if !cluster_is_permitted_for_relation(mcx, relid, miscinit::GetUserId())? {
            continue;
        }
        rtcs.push(RelToCluster { table_oid: relid, index_oid: indexrelid });
    }
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

#[track_caller]
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
