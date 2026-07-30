// matview.c — REFRESH MATERIALIZED VIEW (incl. CONCURRENTLY) + the CREATE
// arm's datafill (PG 18.3).
#![allow(non_snake_case, non_upper_case_globals)]

use core::cell::Cell;

use datum::Datum;
use matview_seams::TransientRelState;
use mcx::Mcx;
use types_core::catalog::RELATION_RELATION_ID;
use types_core::fmgr::F_OIDEQ;
use types_core::{AttrNumber, Oid, RegProcedure};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_SYNTAX_ERROR, ERROR,
};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::Query;
use types_nodes::rawnodes::RefreshMatViewStmt;
use types_portal::{
    ParamListHandle, QueryCompletion, QueryEnvHandle, CMDTAG_REFRESH_MATERIALIZED_VIEW,
    CMDTAG_SELECT, CURSOR_OPT_PARALLEL_OK,
};
use types_core::{SECURITY_LOCAL_USERID_CHANGE, SECURITY_RESTRICTED_OPERATION};
use types_error::ERRCODE_CARDINALITY_VIOLATION;
use types_rel::{
    AccessExclusiveLock, AccessShareLock, ExclusiveLock, NoLock, Relation, RowExclusiveLock,
    RELKIND_MATVIEW,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_slot::SlotData;
use types_tuple::TupleDescData;

const Anum_pg_class_oid: AttrNumber = 1;
const Anum_pg_class_relispopulated: usize = 26;
const CLASS_OID_INDEX_ID: Oid = 2662;

pub fn init_seams() {
    matview_seams::transientrel_startup::set(transientrel_startup);
    matview_seams::transientrel_receive::set(transientrel_receive);
    matview_seams::transientrel_shutdown::set(transientrel_shutdown);
    matview_seams::matview_maintenance_is_enabled::set(MatViewIncrementalMaintenanceIsEnabled);
    matview_seams::exec_refresh_mat_view::set(ExecRefreshMatView);
    matview_seams::refresh_mat_view_by_oid::set(RefreshMatViewByOid);
    matview_seams::set_mat_view_populated_state::set(SetMatViewPopulatedState);
}

thread_local! {
    static MATVIEW_MAINTENANCE_DEPTH: Cell<i32> = const { Cell::new(0) };
}

pub fn MatViewIncrementalMaintenanceIsEnabled() -> bool {
    MATVIEW_MAINTENANCE_DEPTH.with(Cell::get) > 0
}

fn OpenMatViewIncrementalMaintenance() {
    MATVIEW_MAINTENANCE_DEPTH.with(|d| d.set(d.get() + 1));
}

fn CloseMatViewIncrementalMaintenance() {
    MATVIEW_MAINTENANCE_DEPTH.with(|d| d.set(d.get() - 1));
    debug_assert!(MATVIEW_MAINTENANCE_DEPTH.with(Cell::get) >= 0);
}

pub fn ExecRefreshMatView<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &RefreshMatViewStmt<'mcx>,
    query_string: &str,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<Oid> {
    let lockmode = if stmt.concurrent { ExclusiveLock } else { AccessExclusiveLock };
    let rv_node = stmt.relation.expect("RefreshMatViewStmt.relation");
    let rv = rel_vocab::RangeVar {
        catalogname: rv_node.catalogname,
        schemaname: rv_node.schemaname,
        relname: rv_node.relname.expect("RangeVar.relname"),
        inh: rv_node.inh,
        relpersistence: rv_node.relpersistence,
        location: rv_node.location,
    };
    let mut cb = |rv2: &rel_vocab::RangeVar<'_>, rel_id: Oid, old_rel_id: Oid| -> PgResult<()> {
        tablecmds_seams::range_var_callback_maintains_table::call(rv2, rel_id, old_rel_id)
    };
    let matview_oid =
        catalog_namespace::RangeVarGetRelidExtended(&rv, lockmode, 0, Some(&mut cb))?;
    RefreshMatViewByOid(mcx, matview_oid, false, stmt.skipData, stmt.concurrent, query_string, qc)?;
    Ok(matview_oid)
}

pub fn RefreshMatViewByOid<'mcx>(
    mcx: Mcx<'mcx>,
    matview_oid: Oid,
    is_create: bool,
    skip_data: bool,
    concurrent: bool,
    query_string: &str,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    let matview_rel = table::table_open(mcx, matview_oid, NoLock)?;
    let relowner = matview_rel.rd_rel.relowner;

    let (save_userid, save_sec_context) = miscinit::GetUserIdAndSecContext();
    miscinit::SetUserIdAndSecContext(
        relowner,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    );
    let save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    if matview_rel.rd_rel.relkind != RELKIND_MATVIEW {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("\"{}\" is not a materialized view", matview_rel.name()))
            .into_error()
            .into());
    }
    if concurrent && !matview_rel.rd_rel.relispopulated {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("CONCURRENTLY cannot be used when the materialized view is not populated")
            .into_error()
            .into());
    }
    if concurrent && skip_data {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("CONCURRENTLY and WITH NO DATA options cannot be used together")
            .into_error()
            .into());
    }

    let rules = relcache::RelationGetRules(mcx, matview_oid)?;
    let rules = match rules {
        Some(r) if !r.rules.is_empty() => r,
        _ => {
            return Err(internal(format!(
                "materialized view \"{}\" is missing rewrite information",
                matview_rel.name()
            )))
        }
    };
    if rules.rules.len() > 1 {
        return Err(internal(format!(
            "materialized view \"{}\" has too many rules",
            matview_rel.name()
        )));
    }
    let rule = &rules.rules[0];
    if !rule.is_instead || rule.event != CmdType::CMD_SELECT as i32 {
        return Err(internal(format!(
            "the rule for materialized view \"{}\" is not a SELECT INSTEAD OF rule",
            matview_rel.name()
        )));
    }

    if concurrent {
        debug_assert!(!is_create);
        let mut has_unique = false;
        for &idx_oid in relcache::RelationGetIndexList(mcx, matview_oid)?.iter() {
            let idx = indexam::index_open(mcx, idx_oid, AccessShareLock)?;
            if is_usable_unique_index(mcx, &idx)? {
                has_unique = true;
            }
            idx.close(AccessShareLock)?;
        }
        if !has_unique {
            let nspname = lsyscache::get_namespace_name(mcx, matview_rel.rd_rel.relnamespace)?;
            let qualified = ruleutils::quote_qualified_identifier(
                nspname.as_ref().map(|s| s.as_str()),
                matview_rel.name(),
            );
            return Err(elog::ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("cannot refresh materialized view \"{qualified}\" concurrently"))
                .errhint(
                    "Create a unique index with no WHERE clause on one or more columns \
                     of the materialized view.",
                )
                .into_error()
                .into());
        }
    }

    let data_query_node = readfuncs::stringToNode(mcx, rule.action_src.as_str())?;
    let actions = data_query_node.as_list().expect("ev_action is a List");
    if actions.len() != 1 {
        return Err(internal(format!(
            "the rule for materialized view \"{}\" is not a single action",
            matview_rel.name()
        )));
    }
    let data_query_node = actions.nth(0);

    catalog_heap::CheckTableNotInUse(
        &matview_rel,
        if is_create { "CREATE MATERIALIZED VIEW" } else { "REFRESH MATERIALIZED VIEW" },
    )?;

    SetMatViewPopulatedState(mcx, &matview_rel, !skip_data)?;

    let relpersistence = if concurrent {
        types_core::RELPERSISTENCE_TEMP
    } else {
        matview_rel.rd_rel.relpersistence
    };

    // C: tableSpace = concurrent ? GetDefaultTablespace(TEMP) : rd_rel->reltablespace.
    let table_space = if concurrent {
        fd::PrepareTempTablespaces()?;
        fd::GetNextTempTableSpace()
    } else {
        matview_rel.rd_rel.reltablespace
    };
    let oid_new_heap = commands_cluster::make_new_heap(
        mcx,
        matview_oid,
        table_space,
        matview_rel.rd_rel.relam,
        relpersistence,
        ExclusiveLock,
    )?;

    let mut processed: u64 = 0;
    if !skip_data {
        let mut dest =
            tcop_dest::DestReceiver::TransientRel(TransientRelState::new(mcx, oid_new_heap));
        processed =
            refresh_matview_datafill(mcx, &mut dest, data_query_node, query_string, is_create)?;
    }

    if concurrent {
        let old_depth = MATVIEW_MAINTENANCE_DEPTH.with(Cell::get);
        if let Err(e) =
            refresh_by_match_merge(mcx, matview_oid, oid_new_heap, relowner, save_sec_context)
        {
            MATVIEW_MAINTENANCE_DEPTH.with(|d| d.set(old_depth));
            return Err(e);
        }
        debug_assert_eq!(MATVIEW_MAINTENANCE_DEPTH.with(Cell::get), old_depth);
    } else {
        refresh_by_heap_swap(mcx, matview_oid, oid_new_heap, relpersistence)?;
        pgstat::relation::pgstat_count_truncate(matview_oid, matview_rel.rd_rel.relisshared);
        if !skip_data {
            pgstat::relation::pgstat_count_heap_insert(
                matview_oid,
                matview_rel.rd_rel.relisshared,
                processed as i64,
            );
        }
    }

    matview_rel.close(NoLock)?;

    guc::AtEOXact_GUC(false, save_nestlevel);
    miscinit::SetUserIdAndSecContext(save_userid, save_sec_context);

    if let Some(qc) = qc {
        qc.commandTag = if is_create { CMDTAG_SELECT } else { CMDTAG_REFRESH_MATERIALIZED_VIEW };
        qc.nprocessed = processed;
    }
    Ok(())
}

// SetMatViewPopulatedState (matview.c); CatalogTupleUpdate queues the
// relcache inval, CommandCounterIncrement makes the new state visible.
pub fn SetMatViewPopulatedState<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    newstate: bool,
) -> PgResult<()> {
    debug_assert!(rel.rd_rel.relkind == RELKIND_MATVIEW);
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_class_oid, F_OIDEQ, Datum::from_oid(rel.rd_id))];
    let mut scan = genam::systable_beginscan(mcx, &pg_class, CLASS_OID_INDEX_ID, true, None, &keys)?;
    let tup = match genam::systable_getnext(mcx, &mut scan)? {
        Some(t) => t,
        None => return Err(internal(format!("cache lookup failed for relation {}", rel.rd_id))),
    };
    let natts = pg_class.descr().natts as usize;
    let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[Anum_pg_class_relispopulated - 1] = Datum::from_bool(newstate);
    repl[Anum_pg_class_relispopulated - 1] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, tup, pg_class.descr(), &repl_values, &repl_isnull, &repl)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_class, &otid, &mut newtup)?;
    pg_class.close(RowExclusiveLock)?;
    xact::CommandCounterIncrement()?;
    Ok(())
}

// refresh_matview_datafill (matview.c). The rule tree came off a fresh
// stringToNode read, so it is this call's modifiable copy (C copyObject).
fn refresh_matview_datafill<'mcx>(
    mcx: Mcx<'mcx>,
    dest: &mut tcop_dest::DestReceiver<'mcx>,
    query_node: types_nodes::Node<'mcx>,
    query_string: &str,
    is_create: bool,
) -> PgResult<u64> {
    rewrite_handler::AcquireRewriteLocks(
        mcx,
        query_node.as_query().expect("rule action is a Query"),
        true,
        false,
    )?;
    // SAFETY: freshly deserialized tree; this take is its only live access.
    let query: Query<'mcx> = unsafe { query_node.with_mut::<Query, _>(core::mem::take) }
        .expect("rule action is a Query");

    let rewritten = rewrite_handler::QueryRewrite(mcx, query)?;
    if rewritten.len() != 1 {
        return Err(internal(format!(
            "unexpected rewrite result for {}",
            if is_create { "CREATE MATERIALIZED VIEW " } else { "REFRESH MATERIALIZED VIEW" }
        )));
    }
    let query = rewritten.into_iter().next().expect("checked above");

    let plan = postgres::simple_query::pg_plan_query(
        mcx,
        mcx::leak_in(mcx::alloc_in(mcx, query)?),
        query_string,
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )?
    .expect("planner handles CMD_SELECT");

    snapmgr::PushCopiedSnapshot(&snapmgr::GetActiveSnapshot())?;
    snapmgr::UpdateActiveSnapshotCommandId()?;

    let qd = execmain_seams::create_query_desc::call(
        &plan,
        query_string,
        Some(snapmgr::GetActiveSnapshot()),
        None,
        types_dest::CommandDest::TransientRel,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )?;

    execmain_seams::executor_start::call(qd, 0)?;
    execmain_seams::executor_run::call(
        qd,
        types_scan::sdir::ScanDirection::ForwardScanDirection,
        0,
        dest,
    )?;
    let processed = execmain_seams::query_desc_es_processed::call(qd);
    execmain_seams::executor_finish::call(qd)?;
    execmain_seams::executor_end::call(qd)?;
    execmain_seams::free_query_desc::call(qd);
    snapmgr::PopActiveSnapshot()?;

    Ok(processed)
}

// _SPI_error_callback (spi.c) shape, attached at this call site: SPI has no
// error-context stack; plpgsql attaches its own copy the same way.
fn spi_exec_expect(query: &str, expected: i32) -> PgResult<()> {
    if spi::SPI_exec(query, 0).map_err(|mut e| {
        e.add_context_line(format!("SQL statement \"{query}\""));
        e
    })? != expected
    {
        return Err(internal(format!("SPI_exec failed: {query}")));
    }
    Ok(())
}

fn qualified_name<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<String> {
    let nspname = lsyscache::get_namespace_name(mcx, rel.rd_rel.relnamespace)?;
    Ok(ruleutils::quote_qualified_identifier(
        nspname.as_ref().map(|s| s.as_str()),
        rel.name(),
    ))
}

// refresh_by_match_merge (matview.c): full-join diff into a SPI temp table,
// then set-based DELETE + INSERT against the matview under ExclusiveLock.
fn refresh_by_match_merge<'mcx>(
    mcx: Mcx<'mcx>,
    matview_oid: Oid,
    temp_oid: Oid,
    relowner: Oid,
    save_sec_context: i32,
) -> PgResult<()> {
    use core::fmt::Write;

    let matview_rel = table::table_open(mcx, matview_oid, NoLock)?;
    let matviewname = qualified_name(mcx, &matview_rel)?;
    let temp_rel = table::table_open(mcx, temp_oid, NoLock)?;
    let tempname = qualified_name(mcx, &temp_rel)?;
    let diffname = format!("{tempname}_2");
    let relnatts = matview_rel.descr().natts as usize;

    spi::SPI_connect()?;

    spi_exec_expect(&format!("ANALYZE {tempname}"), spi::SPI_OK_UTILITY)?;

    let dupcheck = format!(
        "SELECT newdata.*::{tempname} FROM {tempname} newdata \
         WHERE newdata.* IS NOT NULL AND EXISTS \
         (SELECT 1 FROM {tempname} newdata2 WHERE newdata2.* IS NOT NULL \
         AND newdata2.* OPERATOR(pg_catalog.*=) newdata.* \
         AND newdata2.ctid OPERATOR(pg_catalog.<>) \
         newdata.ctid)"
    );
    if spi::SPI_execute(&dupcheck, false, 1).map_err(|mut e| {
        e.add_context_line(format!("SQL statement \"{dupcheck}\""));
        e
    })? != spi::SPI_OK_SELECT
    {
        return Err(internal(format!("SPI_exec failed: {dupcheck}")));
    }
    if spi::SPI_processed() > 0 {
        let h = spi::SPI_tuptable().expect("SELECT leaves a tuptable");
        let row = spi::tuptable_with(h, |t| {
            spi::SPI_getvalue(mcx, &t.vals[0], &t.tupdesc, 1)
                .map(|v| String::from_utf8_lossy(v.unwrap_or_default()).into_owned())
        })?;
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_CARDINALITY_VIOLATION)
            .errmsg(format!(
                "new data for materialized view \"{}\" contains duplicate rows without any null columns",
                matview_rel.name()
            ))
            .errdetail(format!("Row: {row}"))
            .into_error()
            .into());
    }

    // Temp-table creation is barred inside SECURITY_RESTRICTED_OPERATION.
    miscinit::SetUserIdAndSecContext(relowner, save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
    let create_diff = format!("CREATE TEMP TABLE {diffname} (tid pg_catalog.tid)");
    let r = spi_exec_expect(&create_diff, spi::SPI_OK_UTILITY);
    miscinit::SetUserIdAndSecContext(relowner, save_sec_context | SECURITY_RESTRICTED_OPERATION);
    r?;
    spi_exec_expect(
        &format!("ALTER TABLE {diffname} ADD COLUMN newdata {tempname}"),
        spi::SPI_OK_UTILITY,
    )?;

    let mut querybuf = format!(
        "INSERT INTO {diffname} \
         SELECT mv.ctid AS tid, newdata.*::{tempname} AS newdata \
         FROM {matviewname} mv FULL JOIN {tempname} newdata ON ("
    );

    let mut op_used_for_qual: mcx::PgVec<'_, Oid> = mcx::vec_with_capacity_in(mcx, relnatts)?;
    op_used_for_qual.resize(relnatts, types_core::InvalidOid);
    let mut found_unique_index = false;

    for &idx_oid in relcache::RelationGetIndexList(mcx, matview_oid)?.iter() {
        let index_rel = indexam::index_open(mcx, idx_oid, RowExclusiveLock)?;
        if is_usable_unique_index(mcx, &index_rel)? {
            let form = index_rel.rd_index.as_ref().expect("usable index has rd_index");
            for i in 0..form.indnkeyatts as usize {
                let attnum = form.indkey[i];
                let opclass = syscache_seams::pg_index_indclass_element::call(idx_oid, i as i32)?
                    .ok_or_else(|| internal(format!("cache lookup failed for index {idx_oid}")))?;
                let attr = &matview_rel.descr().attrs[attnum as usize - 1];
                let attrtype = attr.atttypid;

                let opfamily = lsyscache::get_opclass_family(opclass)?;
                let opcintype = lsyscache::get_opclass_input_type(opclass)?;
                let op = lsyscache::amop::get_opfamily_member_for_cmptype(
                    opfamily,
                    opcintype,
                    opcintype,
                    types_pathnodes::COMPARE_EQ,
                )?;
                if op == types_core::InvalidOid {
                    return Err(internal(format!(
                        "missing equality operator for ({opcintype},{opcintype}) in opfamily {opfamily}"
                    )));
                }

                if op_used_for_qual[attnum as usize - 1] == op {
                    continue;
                }
                op_used_for_qual[attnum as usize - 1] = op;

                if found_unique_index {
                    querybuf.push_str(" AND ");
                }
                let attname = String::from_utf8_lossy(attr.attname.name_str()).into_owned();
                let leftop = ruleutils::quote_qualified_identifier(Some("newdata"), &attname);
                let rightop = ruleutils::quote_qualified_identifier(Some("mv"), &attname);
                ruleutils::generate_operator_clause(
                    mcx, &mut querybuf, &leftop, attrtype, op, &rightop, attrtype,
                )?;
                found_unique_index = true;
            }
        }
        // Keep the lock: the DML below needs it.
        index_rel.close(NoLock)?;
    }

    if !found_unique_index {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "could not find suitable unique index on materialized view \"{}\"",
                matview_rel.name()
            ))
            .into_error()
            .into());
    }

    write!(
        querybuf,
        " AND newdata.* OPERATOR(pg_catalog.*=) mv.*) \
         WHERE newdata.* IS NULL OR mv.* IS NULL \
         ORDER BY tid"
    )
    .expect("String write");

    spi_exec_expect(&querybuf, spi::SPI_OK_INSERT)?;

    spi_exec_expect(&format!("ANALYZE {diffname}"), spi::SPI_OK_UTILITY)?;

    OpenMatViewIncrementalMaintenance();

    // Deletes must come before inserts.
    let delete = format!(
        "DELETE FROM {matviewname} mv WHERE ctid OPERATOR(pg_catalog.=) ANY \
         (SELECT diff.tid FROM {diffname} diff \
         WHERE diff.tid IS NOT NULL \
         AND diff.newdata IS NULL)"
    );
    spi_exec_expect(&delete, spi::SPI_OK_DELETE)?;

    let insert = format!(
        "INSERT INTO {matviewname} SELECT (diff.newdata).* \
         FROM {diffname} diff WHERE tid IS NULL"
    );
    spi_exec_expect(&insert, spi::SPI_OK_INSERT)?;

    CloseMatViewIncrementalMaintenance();
    temp_rel.close(NoLock)?;
    matview_rel.close(NoLock)?;

    spi_exec_expect(&format!("DROP TABLE {diffname}, {tempname}"), spi::SPI_OK_UTILITY)?;

    if spi::SPI_finish()? != spi::SPI_OK_FINISH {
        return Err(internal("SPI_finish failed".to_string()));
    }
    Ok(())
}

// refresh_by_heap_swap (matview.c).
fn refresh_by_heap_swap<'mcx>(
    mcx: Mcx<'mcx>,
    matview_oid: Oid,
    oid_new_heap: Oid,
    relpersistence: u8,
) -> PgResult<()> {
    commands_cluster::finish_heap_swap(
        mcx,
        matview_oid,
        oid_new_heap,
        false,
        false,
        true,
        true,
        procarray::RecentXmin(),
        multixact::ReadNextMultiXactId()?,
        relpersistence,
    )
}

// is_usable_unique_index (matview.c): unique, immediate, valid, no
// predicate, no expression columns. The predicate test must go through
// RelationGetIndexPredicate: eval_const_expressions can fold a partial
// index's predicate to constant TRUE (== NIL), making it usable.
fn is_usable_unique_index(mcx: Mcx<'_>, index_rel: &Relation<'_>) -> PgResult<bool> {
    let Some(form) = index_rel.rd_index.as_ref() else { return Ok(false) };
    Ok(form.indisunique
        && form.indimmediate
        && form.indisvalid
        && form.indnatts > 0
        && form.indkey.iter().all(|&k| k > 0)
        && execindexing::RelationGetIndexPredicate(mcx, index_rel)?.is_nil())
}

fn transientrel_startup<'mcx>(
    state: &mut TransientRelState<'mcx>,
    _operation: i32,
    _typeinfo: &TupleDescData<'_>,
) -> PgResult<()> {
    // C's heap_create_with_catalog leaves the new heap AccessExclusive-locked;
    // ours does not, so the lock moves to this first open (same end state).
    let rel = table::table_open(state.mcx, state.transientoid, AccessExclusiveLock)?;
    state.output_cid = xact::GetCurrentCommandId(true)?;
    // C adds TABLE_INSERT_FROZEN; the frozen insert's visibilitymap_pin lane
    // is unported (hio.rs) — rows carry a live committed xmin instead, same
    // visibility, page vm/PD_ALL_VISIBLE bits diverge until that lane lands.
    state.ti_options = tableam_vocab::TABLE_INSERT_SKIP_FSM;
    state.bistate = Some(heapam::GetBulkInsertState());
    // W1 multi-insert buffering (PGRUST_CTAS_MULTIINSERT, default OFF).
    state.mibuf = tableam::write_buffer::write_buffer_begin(&rel);
    state.rel = Some(rel);
    Ok(())
}

fn transientrel_receive<'mcx>(
    state: &mut TransientRelState<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let rel = state.rel.as_ref().expect("transientrel_startup ran");
    if let Some(buf) = state.mibuf.as_mut() {
        tableam::write_buffer::write_buffer_receive(
            state.mcx,
            rel,
            buf,
            slot,
            state.output_cid,
            state.ti_options,
            state.bistate.as_mut(),
        )?;
        return Ok(true);
    }
    tableam::table_tuple_insert(
        state.mcx,
        rel,
        slot,
        state.output_cid,
        state.ti_options,
        state.bistate.as_mut(),
    )?;
    Ok(true)
}

fn transientrel_shutdown<'mcx>(state: &mut TransientRelState<'mcx>) -> PgResult<()> {
    // Tail flush (success path only — an erroring statement drops the
    // buffered copies unflushed, and the aborted xact kills the rest).
    if let Some(mut buf) = state.mibuf.take() {
        if let Some(rel) = state.rel.as_ref() {
            tableam::write_buffer::write_buffer_flush(
                state.mcx,
                rel,
                &mut buf,
                state.output_cid,
                state.ti_options,
                state.bistate.as_mut(),
            )?;
        }
    }
    // FreeBulkInsertState: the pin/strategy guards release on drop.
    drop(state.bistate.take());
    if let Some(rel) = state.rel.as_ref() {
        tableam::table_finish_bulk_insert(rel, state.ti_options)?;
    }
    if let Some(rel) = state.rel.take() {
        table::table_close(rel, NoLock)?;
    }
    Ok(())
}

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

#[track_caller]
#[cold]
#[inline(never)]
fn internal(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg))
}
