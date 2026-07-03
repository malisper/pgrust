// matview.c — REFRESH MATERIALIZED VIEW + the CREATE arm's datafill
// (PG 18.3). refresh_by_match_merge (CONCURRENTLY body) is loud: it needs
// SPI + temp tables.
#![allow(non_snake_case, non_upper_case_globals)]

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
use types_core::SECURITY_RESTRICTED_OPERATION;
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
}

pub fn ExecRefreshMatView<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &RefreshMatViewStmt<'mcx>,
    query_string: &str,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
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
    RefreshMatViewByOid(mcx, matview_oid, false, stmt.skipData, stmt.concurrent, query_string, qc)
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
            let idx = table::table_open(mcx, idx_oid, AccessShareLock)?;
            if is_usable_unique_index(&idx) {
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
                .errmsg(format!("cannot refresh materialized view {qualified} concurrently"))
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

    // Concurrent refresh fills a temp-tablespace RELPERSISTENCE_TEMP heap;
    // the loud match-merge panic below keeps that arm unreachable.
    let relpersistence = matview_rel.rd_rel.relpersistence;

    let oid_new_heap = commands_cluster::make_new_heap(mcx, matview_oid, relpersistence, ExclusiveLock)?;

    let mut processed: u64 = 0;
    if !skip_data {
        let mut dest =
            tcop_dest::DestReceiver::TransientRel(TransientRelState::new(mcx, oid_new_heap));
        processed =
            refresh_matview_datafill(mcx, &mut dest, data_query_node, query_string, is_create)?;
    }

    if concurrent {
        panic!(
            "refresh_by_match_merge (matview.c): REFRESH MATERIALIZED VIEW CONCURRENTLY \
             needs SPI + temp diff table — unit backend-commands-matview"
        );
    }
    refresh_by_heap_swap(mcx, matview_oid, oid_new_heap, relpersistence)?;
    pgstat::relation::pgstat_count_truncate(matview_oid, matview_rel.rd_rel.relisshared);
    if !skip_data {
        pgstat::relation::pgstat_count_heap_insert(
            matview_oid,
            matview_rel.rd_rel.relisshared,
            processed as i64,
        );
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
        query,
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
// predicate, no expression columns.
fn is_usable_unique_index(index_rel: &Relation<'_>) -> bool {
    let Some(form) = index_rel.rd_index.as_ref() else { return false };
    form.indisunique
        && form.indimmediate
        && form.indisvalid
        && !form.has_indpred
        && form.indnatts > 0
        && form.indkey.iter().all(|&k| k > 0)
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
    state.rel = Some(rel);
    Ok(())
}

fn transientrel_receive<'mcx>(
    state: &mut TransientRelState<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let rel = state.rel.as_ref().expect("transientrel_startup ran");
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

#[cold]
#[inline(never)]
fn internal(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg))
}
