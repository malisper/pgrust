use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{Oid, BLCKSZ};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_nodes::list::{IntList, NodeList};
use types_nodes::{FdwKind, FdwRoutine, Node};
use types_pathnodes::{PathId, RelId, RinfoId};
use types_tuple::{SizeofHeapTupleHeader, MAXALIGN};

use executils::EStateData;
use nodeforeignscan::{FdwExecRoutine, ForeignScanState};
use planner::fdwplan::FdwPlanRoutine;
use planner::run::PlannerRun;

use crate::deparse;
use crate::relinfo::{attach_fpinfo, fpinfo, PgFdwRelationInfo};

const DEFAULT_FDW_STARTUP_COST: f64 = 100.0;
const DEFAULT_FDW_TUPLE_COST: f64 = 0.2;

// ---------- handler ----------

fn fc_postgres_fdw_handler(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    static ROUTINE: FdwRoutine = FdwRoutine::new(FdwKind::PostgresFdw);
    Ok(Datum::from_usize(&ROUTINE as *const FdwRoutine as usize))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "postgres_fdw_handler" => fc_postgres_fdw_handler,
        "postgres_fdw_validator" => crate::option::fc_postgres_fdw_validator,
        "postgres_fdw_get_connections" => crate::connection::fc_postgres_fdw_get_connections,
        "postgres_fdw_get_connections_1_2" => {
            crate::connection::fc_postgres_fdw_get_connections_1_2
        }
        "postgres_fdw_disconnect" => crate::connection::fc_postgres_fdw_disconnect,
        "postgres_fdw_disconnect_all" => crate::connection::fc_postgres_fdw_disconnect_all,
        _ => return None,
    })
}

// ---------- option application ----------

fn apply_server_options<'mcx>(
    fp: &mut PgFdwRelationInfo<'mcx>,
    mcx: Mcx<'mcx>,
    server: &foreigncmds::foreign::ForeignServer<'mcx>,
) -> PgResult<()> {
    let mut opts: Vec<(String, String)> = Vec::new();
    for o in server.options.iter() {
        opts.push((o.name.to_string(), o.require_value()?.to_string()));
    }
    for (name, value) in opts {
        match name.as_str() {
            "use_remote_estimate" => fp.use_remote_estimate = parse_bool(&value)?,
            "fdw_startup_cost" => {
                if let guc::units::ParseNum::Ok(v) = guc::units::parse_real(&value, 0) {
                    fp.fdw_startup_cost = v;
                }
            }
            "fdw_tuple_cost" => {
                if let guc::units::ParseNum::Ok(v) = guc::units::parse_real(&value, 0) {
                    fp.fdw_tuple_cost = v;
                }
            }
            "extensions" => {
                fp.shippable_extensions = crate::option::extract_extension_list(mcx, &value, false)?;
            }
            "fetch_size" => {
                if let guc::units::ParseNum::Ok(v) = guc::units::parse_int(&value, 0) {
                    fp.fetch_size = v;
                }
            }
            "async_capable" => fp.async_capable = parse_bool(&value)?,
            _ => {}
        }
    }
    Ok(())
}

fn apply_table_options<'mcx>(
    fp: &mut PgFdwRelationInfo<'mcx>,
    table: &foreigncmds::foreign::ForeignTable<'mcx>,
) -> PgResult<()> {
    let mut opts: Vec<(String, String)> = Vec::new();
    for o in table.options.iter() {
        opts.push((o.name.to_string(), o.require_value()?.to_string()));
    }
    for (name, value) in opts {
        match name.as_str() {
            "use_remote_estimate" => fp.use_remote_estimate = parse_bool(&value)?,
            "fetch_size" => {
                if let guc::units::ParseNum::Ok(v) = guc::units::parse_int(&value, 0) {
                    fp.fetch_size = v;
                }
            }
            "async_capable" => fp.async_capable = parse_bool(&value)?,
            _ => {}
        }
    }
    Ok(())
}

fn parse_bool(value: &str) -> PgResult<bool> {
    // defGetBoolean's string forms (commands_define); options are already
    // validator-checked, so this only meets true/false/on/off/1/0/yes/no.
    Ok(matches!(
        value.to_ascii_lowercase().as_str(),
        "true" | "t" | "on" | "1" | "yes" | "y"
    ))
}

// ---------- GetForeignRelSize ----------

fn postgres_get_foreign_rel_size<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    foreigntableid: Oid,
) -> PgResult<()> {
    let mcx = run.mcx;
    let mut fp = PgFdwRelationInfo::new(mcx);
    fp.pushdown_safe = true;
    let table = foreigncmds::foreign::GetForeignTable(mcx, foreigntableid)?;
    fp.serverid = table.serverid;
    let server = foreigncmds::foreign::GetForeignServer(mcx, table.serverid)?;

    fp.use_remote_estimate = false;
    fp.fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
    fp.fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
    fp.fetch_size = 100;
    fp.async_capable = false;
    apply_server_options(&mut fp, mcx, &server)?;
    apply_table_options(&mut fp, &table)?;

    if fp.use_remote_estimate {
        // Remote-estimate mode needs a live connection at plan time (phase 2).
        return Err(remote_estimate_unported());
    }

    // classifyConditions over baserestrictinfo.
    let baserestrict: Vec<RinfoId> =
        run.root.rel(rel_id).baserestrictinfo.iter().copied().collect();
    let mut remote_conds = PgVec::new_in(mcx);
    let mut local_conds = PgVec::new_in(mcx);
    attach_fpinfo(mcx, run.root.rel_mut(rel_id), fp)?;
    deparse::classify_conditions(run, rel_id, &baserestrict, &mut remote_conds, &mut local_conds)?;

    // attrs_used: reltarget exprs + local_conds clauses.
    let varno = run.root.rel(rel_id).relid as i32;
    let mut attrs_used = types_nodes::Bitmapset::empty();
    let expr_ids: Vec<types_pathnodes::NodeId> =
        run.pathtarget(run.rel_reltarget_id(rel_id)).exprs.iter().copied().collect();
    for id in expr_ids {
        vars::pull_varattnos(mcx, *run.root.expr_node(id), varno, &mut attrs_used)?;
    }
    for &ri in local_conds.iter() {
        let clause = run.root.rinfo(ri).clause;
        vars::pull_varattnos(mcx, *run.root.expr_node(clause), varno, &mut attrs_used)?;
    }

    let varrelid = run.root.rel(rel_id).relid as i32;
    let local_slice: Vec<RinfoId> = local_conds.iter().copied().collect();
    let local_conds_sel = planner::clausesel::clauselist_selectivity(
        run,
        &local_slice,
        varrelid,
        types_pathnodes::JOIN_INNER,
        None,
    )?;
    let mut local_cost = types_pathnodes::QualCost::default();
    for &ri in &local_slice {
        let clause = run.root.rinfo(ri).clause;
        let node = *run.root.expr_node(clause);
        let c = planner::costsize::cost_qual_eval_node(Some(&mut *run), node)?;
        local_cost.startup += c.startup;
        local_cost.per_tuple += c.per_tuple;
    }

    // 10-page hack for never-ANALYZEd tables (plancat.c parity).
    {
        let r = run.root.rel(rel_id);
        if r.tuples < 0.0 {
            let width = run.pathtarget(run.rel_reltarget_id(rel_id)).width;
            let tuple_den = width as i64 + MAXALIGN(SizeofHeapTupleHeader) as i64;
            let tuples = (10 * BLCKSZ as i64) as f64 / tuple_den as f64;
            let r = run.root.rel_mut(rel_id);
            r.pages = 10;
            r.tuples = tuples;
        }
    }
    planner::costsize::set_baserel_size_estimates(run, rel_id)?;

    let (fdw_startup_cost, fdw_tuple_cost) = {
        let fp = fpinfo(run.root.rel(rel_id)).borrow();
        (fp.fdw_startup_cost, fp.fdw_tuple_cost)
    };
    let (rows, width, startup_cost, total_cost) =
        estimate_base_cost_size(run, rel_id, local_conds_sel, fdw_startup_cost, fdw_tuple_cost);

    {
        let fpc = fpinfo(run.root.rel(rel_id));
        let mut fpm = fpc.borrow_mut();
        fpm.remote_conds = remote_conds;
        fpm.local_conds = local_conds;
        fpm.attrs_used = attrs_used;
        fpm.local_conds_sel = local_conds_sel;
        fpm.local_conds_cost = local_cost;
        fpm.retrieved_rows = -1.0;
        fpm.rel_startup_cost = -1.0;
        fpm.rel_total_cost = -1.0;
        fpm.rows = rows;
        fpm.width = width;
        fpm.startup_cost = startup_cost;
        fpm.total_cost = total_cost;
        fpm.relation_name = mcx_str(mcx, &format!("{varno}"))?;
        fpm.relation_index = varno;
    }
    Ok(())
}

// estimate_path_cost_size, base-relation local branch (no remote estimate,
// no pathkeys/LIMIT): seqscan-shaped run cost + FDW network/startup factors.
fn estimate_base_cost_size(
    run: &PlannerRun<'_>,
    rel_id: RelId,
    local_conds_sel: f64,
    fdw_startup_cost: f64,
    fdw_tuple_cost: f64,
) -> (f64, i32, f64, f64) {
    let r = run.root.rel(rel_id);
    let rows = r.rows;
    let width = run.pathtarget(run.rel_reltarget_id(rel_id)).width;
    let tuples = r.tuples;
    let pages = r.pages;
    let baserestrict_startup = r.baserestrictcost.startup;
    let baserestrict_per_tuple = r.baserestrictcost.per_tuple;

    let mut retrieved_rows =
        planner::costsize::clamp_row_est(rows / local_conds_sel.max(1e-9));
    if retrieved_rows > tuples {
        retrieved_rows = tuples;
    }
    let mut startup_cost = 0.0;
    let mut run_cost = planner::costsize::gucs::seq_page_cost() * pages as f64;
    startup_cost += baserestrict_startup;
    let cpu_per_tuple = planner::costsize::gucs::cpu_tuple_cost() + baserestrict_per_tuple;
    run_cost += cpu_per_tuple * tuples;
    let mut total_cost = startup_cost + run_cost;

    // FDW factors.
    startup_cost += fdw_startup_cost;
    total_cost += fdw_startup_cost;
    total_cost += fdw_tuple_cost * retrieved_rows;
    total_cost += planner::costsize::gucs::cpu_tuple_cost() * retrieved_rows;
    (rows, width, startup_cost, total_cost)
}

fn mcx_str<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: bytes copied verbatim from a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

// ---------- GetForeignPaths ----------

fn postgres_get_foreign_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    _foreigntableid: Oid,
) -> PgResult<()> {
    let mcx = run.mcx;
    let (rows, disabled_nodes, startup_cost, total_cost) = {
        let fp = fpinfo(run.root.rel(rel_id)).borrow();
        (fp.rows, fp.disabled_nodes, fp.startup_cost, fp.total_cost)
    };
    let required_outer =
        planner::relnode::relids_copy(mcx, &run.root.rel(rel_id).lateral_relids);
    let path = planner::pathnode::create_foreignscan_path(
        run,
        rel_id,
        None,
        rows,
        disabled_nodes,
        startup_cost,
        total_cost,
        PgVec::new_in(mcx),
        &required_outer,
        None,
        PgVec::new_in(mcx),
        PgVec::new_in(mcx),
    )?;
    planner::pathnode::add_path(run, rel_id, path);
    // No parameterized/pathkey paths without remote estimates.
    Ok(())
}

// ---------- GetForeignPlan ----------

fn postgres_get_foreign_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    _foreigntableid: Oid,
    _best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: PgVec<'mcx, RinfoId>,
    outer_plan: Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let reloptkind = run.root.rel(rel_id).reloptkind;
    if reloptkind != types_pathnodes::RELOPT_BASEREL
        && reloptkind != types_pathnodes::RELOPT_OTHER_MEMBER_REL
    {
        return Err(join_upper_unported());
    }
    let scan_relid = run.root.rel(rel_id).relid;

    // Split scan_clauses into remote and local (extract_actual_clauses shape
    // plus the remote/local decision).
    let (remote_set, local_set): (Vec<RinfoId>, Vec<RinfoId>) = {
        let fp = fpinfo(run.root.rel(rel_id)).borrow();
        (fp.remote_conds.iter().copied().collect(), fp.local_conds.iter().copied().collect())
    };
    let mut remote_rinfos: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    let mut local_exprs: NodeList<'mcx> = NodeList::nil();
    let mut remote_recheck: NodeList<'mcx> = NodeList::nil();
    for &ri in scan_clauses.iter() {
        if run.root.rinfo(ri).pseudoconstant {
            continue;
        }
        let clause = run.root.rinfo(ri).clause;
        let clause_node = *run.root.expr_node(clause);
        if remote_set.contains(&ri) {
            remote_rinfos.push(ri);
            remote_recheck.lappend(mcx, clause_node)?;
        } else if local_set.contains(&ri) {
            local_exprs.lappend(mcx, clause_node)?;
        } else if deparse::is_foreign_expr(run, rel_id, clause_node)? {
            remote_rinfos.push(ri);
            remote_recheck.lappend(mcx, clause_node)?;
        } else {
            local_exprs.lappend(mcx, clause_node)?;
        }
    }

    // Deparse the remote SELECT, collecting params.
    let remote_ids: Vec<RinfoId> = remote_rinfos.iter().copied().collect();
    let (sql, retrieved_attrs, params) =
        deparse::deparse_select_stmt_for_rel(run, rel_id, &remote_ids, Some(PgVec::new_in(mcx)))?;

    // fdw_exprs = params_list (Vars/Params to transmit).
    let mut fdw_exprs: NodeList<'mcx> = NodeList::nil();
    if let Some(params) = params {
        for p in params.iter() {
            fdw_exprs.lappend(mcx, *p)?;
        }
    }

    // fdw_private = [ SELECT sql, retrieved_attrs (Int list), fetch_size ].
    let fetch_size = fpinfo(run.root.rel(rel_id)).borrow().fetch_size;
    let mut fdw_private: NodeList<'mcx> = NodeList::nil();
    fdw_private.lappend(mcx, Node::mk_string(mcx, mcx_str(mcx, sql.as_str())?)?)?;
    let mut ra: IntList<'mcx> = IntList::nil();
    for &a in retrieved_attrs.iter() {
        ra.lappend(mcx, a)?;
    }
    fdw_private.lappend(mcx, Node::mk_int_list(mcx, ra)?)?;
    fdw_private.lappend(mcx, Node::mk_integer(mcx, fetch_size)?)?;

    planner::createplan::make_foreignscan(
        mcx,
        tlist,
        local_exprs,
        scan_relid,
        fdw_exprs,
        fdw_private,
        NodeList::nil(),
        remote_recheck,
        outer_plan,
    )
}

// ---------- execution (phase 2: src/exec.rs over src/connection.rs) ----------

#[track_caller]
#[cold]
fn remote_estimate_unported() -> Box<PgError> {
    Box::new(
        PgError::error(
            "postgres_fdw: use_remote_estimate is not yet supported (phase 3: \
             plan-time remote EXPLAIN)",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
fn join_upper_unported() -> Box<PgError> {
    Box::new(
        PgError::error("postgres_fdw: foreign join/aggregate pushdown is phase 3")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

// postgresExplainForeignScan: "Remote SQL" from fdw_private, gated on
// es->verbose exactly as C (FdwExplainFlags carries the ExplainState bits).
fn explain_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    flags: types_nodes::FdwExplainFlags,
    emit: &mut dyn FnMut(&str, types_nodes::FdwExplainProp<'_>) -> PgResult<()>,
) -> PgResult<()> {
    if !flags.verbose {
        return Ok(());
    }
    // fdw_private[FdwScanPrivateSelectSql] = the remote SELECT.
    if let Some(sql) = node.plan.fdw_private.iter().next().and_then(|n| n.as_string()) {
        emit("Remote SQL", types_nodes::FdwExplainProp::Text(sql.sval))?;
    }
    Ok(())
}

static PLAN_ROUTINE: FdwPlanRoutine = FdwPlanRoutine {
    get_foreign_rel_size: postgres_get_foreign_rel_size,
    get_foreign_paths: postgres_get_foreign_paths,
    get_foreign_plan: postgres_get_foreign_plan,
};

static EXEC_ROUTINE: FdwExecRoutine = FdwExecRoutine {
    begin: crate::exec::begin_foreign_scan,
    iterate: crate::exec::iterate_foreign_scan,
    rescan: crate::exec::rescan_foreign_scan,
    end: crate::exec::end_foreign_scan,
    explain: Some(explain_foreign_scan),
};

pub fn install() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: crate::LIBRARY,
        lookup,
        pg_init: None,
    });
    planner::fdwplan::install_fdw_plan_routine(FdwKind::PostgresFdw, &PLAN_ROUTINE);
    nodeforeignscan::install_fdw_exec_routine(FdwKind::PostgresFdw, &EXEC_ROUTINE);
}
