//! `contrib/file_fdw/file_fdw.c` — foreign-data wrapper for server-side flat
//! files. PROGRAM sources are unported (no COPY FROM PROGRAM lane): the
//! validator accepts the option (C parity), the scan/analyze paths are loud.
#![allow(non_snake_case)]

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{
    BlockNumber, Oid, ATTRIBUTE_RELATION_ID, BLCKSZ, FOREIGN_TABLE_RELATION_ID,
};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED,
    ERRCODE_FDW_INVALID_OPTION_NAME, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_SYNTAX_ERROR, ERROR, NOTICE,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_nodes::parsenodes::{DefElem, DefElemAction};
use types_nodes::{FdwKind, FdwRoutine, Node, NodeList};
use types_pathnodes::{PathNode, PathId, RelId, RinfoId};
use types_rel::Relation;
use types_tuple::{HeapTupleData, SizeofHeapTupleHeader, MAXALIGN};

use commands_analyze::{AcquireSampleRowsFn, FdwAnalyzeRoutine};
use copy_cmd::CopyFromState;
use executils::EStateData;
use foreigncmds::foreign::{GetForeignColumnOptions, GetForeignDataWrapper, GetForeignServer, GetForeignTable};
use nodeforeignscan::{FdwExecRoutine, ForeignScanState};
use planner::fdwplan::FdwPlanRoutine;
use planner::run::PlannerRun;
use types_slot::EXEC_FLAG_EXPLAIN_ONLY;

const LIBRARY: &str = "file_fdw";

const ROLE_PG_READ_SERVER_FILES: Oid = 4569;
const ROLE_PG_EXECUTE_SERVER_PROGRAM: Oid = 4571;

// valid_options[] (file_fdw.c): option name -> catalog it may appear in.
const VALID_OPTIONS: &[(&str, Oid)] = &[
    ("filename", FOREIGN_TABLE_RELATION_ID),
    ("program", FOREIGN_TABLE_RELATION_ID),
    ("format", FOREIGN_TABLE_RELATION_ID),
    ("header", FOREIGN_TABLE_RELATION_ID),
    ("delimiter", FOREIGN_TABLE_RELATION_ID),
    ("quote", FOREIGN_TABLE_RELATION_ID),
    ("escape", FOREIGN_TABLE_RELATION_ID),
    ("null", FOREIGN_TABLE_RELATION_ID),
    ("default", FOREIGN_TABLE_RELATION_ID),
    ("encoding", FOREIGN_TABLE_RELATION_ID),
    ("on_error", FOREIGN_TABLE_RELATION_ID),
    ("log_verbosity", FOREIGN_TABLE_RELATION_ID),
    ("reject_limit", FOREIGN_TABLE_RELATION_ID),
    ("force_not_null", ATTRIBUTE_RELATION_ID),
    ("force_null", ATTRIBUTE_RELATION_ID),
];

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

// FileFdwPlanState (file_fdw.c). As in C it rides RelOptInfo.fdw_private
// (per-RelOptInfo identity: self-joins and nested subplans each get their
// own), encoded as a value-node list since fdw_private is an expr-arena
// NodeId; it is freed with the planner run.
#[derive(Clone)]
struct FileFdwPlanState {
    filename: String,
    is_program: bool,
    pages: BlockNumber,
    ntuples: f64,
}

// FileFdwExecutionState (file_fdw.c). SAFETY (lifetime restamp): every 'static
// here is es_query_cxt-lived; the state is dropped at end-scan, before that
// context resets (executils 'static-restamp precedent).
struct FileFdwExecutionState {
    filename: &'static str,
    options: NodeList<'static>,
    cstate: Option<CopyFromState<'static, 'static>>,
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: bytes copied verbatim from a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

fn mk_string<'mcx>(mcx: Mcx<'mcx>, sval: &'mcx str) -> PgResult<Node<'mcx>> {
    Node::mk(mcx, types_nodes::String { sval })
}

fn mk_def_elem<'mcx>(
    mcx: Mcx<'mcx>,
    defname: &'mcx str,
    arg: Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        DefElem {
            defnamespace: None,
            defname: Some(defname),
            arg,
            defaction: DefElemAction::DEFELEM_UNSPEC,
            location: -1,
        },
    )
}

fn fc_file_fdw_handler(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    static ROUTINE: FdwRoutine = FdwRoutine::new(FdwKind::FileFdw);
    Ok(Datum::from_usize(&ROUTINE as *const FdwRoutine as usize))
}

fn is_valid_option(option: &str, context: Oid) -> bool {
    VALID_OPTIONS.iter().any(|&(n, ctx)| ctx == context && n == option)
}

const MAX_LEVENSHTEIN_STRLEN: usize = 255;

// The ClosestMatch hint arm of file_fdw_validator (initClosestMatch et al,
// varlena.c; foreigncmds postgresql_fdw_validator precedent).
#[cold]
fn invalid_option_error(mcx: Mcx<'_>, name: &str, catalog: Oid) -> PgResult<Box<PgError>> {
    let mut min_d = -1i32;
    let mut closest: Option<&str> = None;
    let mut has_valid_options = false;
    for &(n, ctx) in VALID_OPTIONS {
        if ctx != catalog {
            continue;
        }
        has_valid_options = true;
        if name.is_empty() || name.len() > MAX_LEVENSHTEIN_STRLEN || n.len() > MAX_LEVENSHTEIN_STRLEN
        {
            continue;
        }
        let dist = varlena::levenshtein::varstr_levenshtein_less_equal(
            mcx,
            name.as_bytes(),
            n.as_bytes(),
            1,
            1,
            1,
            4,
            true,
        )?;
        if dist <= 4 && dist as usize <= name.len() / 2 && (min_d == -1 || dist < min_d) {
            min_d = dist;
            closest = Some(n);
        }
    }
    let mut e = PgError::error(format!("invalid option \"{name}\""))
        .with_sqlstate(ERRCODE_FDW_INVALID_OPTION_NAME);
    e = if has_valid_options {
        match closest {
            Some(m) => e.with_hint(format!("Perhaps you meant the option \"{m}\".")),
            None => e,
        }
    } else {
        e.with_hint("There are no valid options in this context.")
    };
    Ok(Box::new(e))
}

#[track_caller]
#[cold]
fn conflicting_options(hint: Option<&str>) -> Box<PgError> {
    let mut e =
        PgError::error("conflicting or redundant options").with_sqlstate(ERRCODE_SYNTAX_ERROR);
    if let Some(h) = hint {
        e = e.with_hint(h);
    }
    Box::new(e)
}

#[track_caller]
#[cold]
fn option_permission_denied(option: &str, role: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "permission denied to set the \"{option}\" option of a file_fdw foreign table"
        ))
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
        .with_detail(format!(
            "Only roles with privileges of the \"{role}\" role may set this option."
        )),
    )
}

fn fc_file_fdw_validator(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let options = foreigncmds::options::untransform_options(mcx, Some(fcinfo.arg(0)))?;
    let catalog = fcinfo.arg(1).as_oid();

    let mut filename: Option<&str> = None;
    let mut force_not_null = false;
    let mut force_null = false;
    let mut other_options: NodeList<'_> = NodeList::nil();

    for opt in options.iter() {
        if !is_valid_option(opt.name, catalog) {
            return Err(invalid_option_error(mcx, opt.name, catalog)?);
        }
        match opt.name {
            "filename" | "program" => {
                if filename.is_some() {
                    return Err(conflicting_options(None));
                }
                let userid = miscinit::GetUserId();
                if opt.name == "filename"
                    && !acl_seams::has_privs_of_role::call(userid, ROLE_PG_READ_SERVER_FILES)?
                {
                    return Err(option_permission_denied("filename", "pg_read_server_files"));
                }
                if opt.name == "program"
                    && !acl_seams::has_privs_of_role::call(userid, ROLE_PG_EXECUTE_SERVER_PROGRAM)?
                {
                    return Err(option_permission_denied("program", "pg_execute_server_program"));
                }
                // C: defGetString(def) — errors on a value-less option.
                filename = Some(opt.require_value()?);
            }
            // Boolean-validated here, discarded; re-fetched per column later
            // by get_file_fdw_attribute_options.
            "force_not_null" | "force_null" => {
                let (seen, hint) = if opt.name == "force_not_null" {
                    (
                        &mut force_not_null,
                        "Option \"force_not_null\" supplied more than once for a column.",
                    )
                } else {
                    (
                        &mut force_null,
                        "Option \"force_null\" supplied more than once for a column.",
                    )
                };
                if *seen {
                    return Err(conflicting_options(Some(hint)));
                }
                *seen = true;
                // C keeps the DefElem's NULL arg; defGetBoolean(NULL) = true.
                let arg = opt.value.map(|v| mk_string(mcx, v)).transpose()?;
                let def = mk_def_elem(mcx, opt.name, arg)?;
                commands_define::defGetBoolean(def.as_def_elem().expect("DefElem"))?;
            }
            _ => {
                let arg = opt.value.map(|v| mk_string(mcx, v)).transpose()?;
                other_options.lappend(mcx, mk_def_elem(mcx, opt.name, arg)?)?;
            }
        }
    }

    copy_cmd::ProcessCopyOptions(true, &other_options, None)?;

    if catalog == FOREIGN_TABLE_RELATION_ID && filename.is_none() {
        return Err(Box::new(
            PgError::error("either filename or program is required for file_fdw foreign tables")
                .with_sqlstate(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
        ));
    }
    Ok(Datum::null())
}

// get_file_fdw_attribute_options (file_fdw.c): per-column force_not_null /
// force_null, combined into the single-DefElem form COPY expects.
fn get_file_fdw_attribute_options<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<NodeList<'mcx>> {
    let rel = table::table_open(mcx, relid, types_rel::lock::AccessShareLock)?;
    let natts = rel.rd_att.natts;

    let mut fnncolumns: NodeList<'mcx> = NodeList::nil();
    let mut fncolumns: NodeList<'mcx> = NodeList::nil();
    for attnum in 1..=natts {
        let attr = rel.rd_att.attr(attnum as usize - 1);
        if attr.attisdropped {
            continue;
        }
        let column_options = GetForeignColumnOptions(mcx, relid, attnum as i16)?;
        for opt in column_options.iter() {
            if opt.name != "force_not_null" && opt.name != "force_null" {
                continue;
            }
            let arg = opt.value.map(|v| mk_string(mcx, v)).transpose()?;
            let def = mk_def_elem(mcx, str_in(mcx, opt.name)?, arg)?;
            if commands_define::defGetBoolean(def.as_def_elem().expect("DefElem"))? {
                // SAFETY: catalog names are server-encoding UTF-8.
                let attname = unsafe {
                    core::str::from_utf8_unchecked(mcx::slice_borrow_in(
                        mcx,
                        attr.attname.name_str(),
                    )?)
                };
                let target =
                    if opt.name == "force_not_null" { &mut fnncolumns } else { &mut fncolumns };
                target.lappend(mcx, mk_string(mcx, attname)?)?;
            }
        }
    }
    table::table_close(rel, types_rel::lock::AccessShareLock)?;

    let mut options: NodeList<'mcx> = NodeList::nil();
    if !fnncolumns.is_nil() {
        options.lappend(mcx, mk_def_elem(mcx, "force_not_null", Some(Node::mk_list(mcx, fnncolumns)?))?)?;
    }
    if !fncolumns.is_nil() {
        options.lappend(mcx, mk_def_elem(mcx, "force_null", Some(Node::mk_list(mcx, fncolumns)?))?)?;
    }
    Ok(options)
}

// fileGetOptions (file_fdw.c): wrapper + server + table + per-column options,
// with the (unique, table-level) filename/program option split off.
fn file_get_options<'mcx>(
    mcx: Mcx<'mcx>,
    foreigntableid: Oid,
) -> PgResult<(&'mcx str, bool, NodeList<'mcx>)> {
    let table = GetForeignTable(mcx, foreigntableid)?;
    let server = GetForeignServer(mcx, table.serverid)?;
    let wrapper = GetForeignDataWrapper(mcx, server.fdwid)?;

    let mut filename: Option<&'mcx str> = None;
    let mut is_program = false;
    let mut options: NodeList<'mcx> = NodeList::nil();
    for opt in wrapper.options.iter().chain(server.options.iter()).chain(table.options.iter()) {
        if filename.is_none() && (opt.name == "filename" || opt.name == "program") {
            // C: defGetString(def) — errors on a value-less option.
            filename = Some(opt.require_value()?);
            is_program = opt.name == "program";
            continue;
        }
        let arg = opt.value.map(|v| mk_string(mcx, v)).transpose()?;
        options.lappend(mcx, mk_def_elem(mcx, str_in(mcx, opt.name)?, arg)?)?;
    }
    for def in get_file_fdw_attribute_options(mcx, foreigntableid)?.iter() {
        options.lappend(mcx, def)?;
    }

    let Some(filename) = filename else {
        panic!("either filename or program is required for file_fdw foreign tables");
    };
    Ok((filename, is_program, options))
}

// estimate_size (file_fdw.c).
fn estimate_size(
    run: &mut PlannerRun<'_>,
    rel_id: RelId,
    fdw_private: &mut FileFdwPlanState,
) -> PgResult<()> {
    let size = if fdw_private.is_program {
        None
    } else {
        std::fs::metadata(&fdw_private.filename).ok().map(|m| m.len())
    }
    .unwrap_or(10 * BLCKSZ as u64);

    let pages = (size.div_ceil(BLCKSZ as u64) as BlockNumber).max(1);
    fdw_private.pages = pages;

    let (rel_tuples, rel_pages, width) = {
        let r = run.root.rel(rel_id);
        let width = run.pathtarget(run.rel_reltarget_id(rel_id)).width;
        (r.tuples, r.pages, width)
    };
    let ntuples = if rel_tuples >= 0.0 && rel_pages > 0 {
        let density = rel_tuples / rel_pages as f64;
        planner::costsize::clamp_row_est(density * pages as f64)
    } else {
        let tuple_width = MAXALIGN(width.max(0) as usize) + MAXALIGN(SizeofHeapTupleHeader);
        planner::costsize::clamp_row_est(size as f64 / tuple_width as f64)
    };
    fdw_private.ntuples = ntuples;

    let rinfos: Vec<RinfoId> = run.root.rel(rel_id).baserestrictinfo.iter().copied().collect();
    let sel = planner::clausesel::clauselist_selectivity(
        run,
        &rinfos,
        0,
        types_pathnodes::JOIN_INNER,
        None,
    )?;
    run.root.rel_mut(rel_id).rows = planner::costsize::clamp_row_est(ntuples * sel);
    Ok(())
}

// estimate_costs (file_fdw.c): cost_seqscan-alike with 10x per-tuple CPU for
// record parsing.
fn estimate_costs(
    run: &PlannerRun<'_>,
    rel_id: RelId,
    fdw_private: &FileFdwPlanState,
) -> (f64, f64) {
    let r = run.root.rel(rel_id);
    let mut run_cost = planner::costsize::gucs::seq_page_cost() * fdw_private.pages as f64;
    let startup_cost = r.baserestrictcost.startup;
    let cpu_per_tuple =
        planner::costsize::gucs::cpu_tuple_cost() * 10.0 + r.baserestrictcost.per_tuple;
    run_cost += cpu_per_tuple * fdw_private.ntuples;
    (startup_cost, startup_cost + run_cost)
}

fn file_get_foreign_rel_size(
    run: &mut PlannerRun<'_>,
    rel_id: RelId,
    foreigntableid: Oid,
) -> PgResult<()> {
    let (filename, is_program, _options) = file_get_options(run.mcx, foreigntableid)?;
    let mut state = FileFdwPlanState {
        filename: filename.to_string(),
        is_program,
        pages: 0,
        ntuples: 0.0,
    };
    estimate_size(run, rel_id, &mut state)?;
    let mcx = run.mcx;
    let mut items: NodeList<'_> = NodeList::nil();
    items.lappend(mcx, Node::mk_string(mcx, str_in(mcx, &state.filename)?)?)?;
    items.lappend(mcx, Node::mk_boolean(mcx, state.is_program)?)?;
    items.lappend(mcx, Node::mk_float(mcx, str_in(mcx, &state.pages.to_string())?)?)?;
    items.lappend(mcx, Node::mk_float(mcx, str_in(mcx, &format!("{:?}", state.ntuples))?)?)?;
    let node = Node::mk_list(mcx, items)?;
    run.root.rel_mut(rel_id).fdw_private = run.intern_expr(node);
    Ok(())
}

// Decode fileGetForeignRelSize's fdw_private value-node list; the shape is
// ours alone, so any mismatch is LOUD.
fn decode_plan_state(node: Node<'_>) -> FileFdwPlanState {
    let items = node.as_list().expect("file_fdw fdw_private: value-node list");
    let mut it = items.iter();
    let filename = it
        .next()
        .and_then(|n| n.as_string())
        .expect("file_fdw fdw_private: filename")
        .sval
        .to_string();
    let is_program =
        it.next().and_then(|n| n.as_boolean()).expect("file_fdw fdw_private: is_program").boolval;
    let pages: f64 = it
        .next()
        .and_then(|n| n.as_float())
        .expect("file_fdw fdw_private: pages")
        .fval
        .parse()
        .expect("file_fdw fdw_private: pages parses");
    let ntuples: f64 = it
        .next()
        .and_then(|n| n.as_float())
        .expect("file_fdw fdw_private: ntuples")
        .fval
        .parse()
        .expect("file_fdw fdw_private: ntuples parses");
    FileFdwPlanState { filename, is_program, pages: pages as BlockNumber, ntuples }
}

// check_selective_binary_conversion (file_fdw.c). Some(columns) = convert
// only these columns (possibly none, e.g. COUNT(*)); None = convert all.
fn check_selective_binary_conversion<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    foreigntableid: Oid,
) -> PgResult<Option<NodeList<'mcx>>> {
    let mcx = run.mcx;

    let table = GetForeignTable(mcx, foreigntableid)?;
    for opt in table.options.iter() {
        if opt.name == "format" {
            // C: defGetString(def) — errors on a value-less option.
            if opt.require_value()? == "binary" {
                return Ok(None);
            }
            break;
        }
    }

    let varno = run.root.rel(rel_id).relid as i32;
    let mut attrs_used = types_nodes::Bitmapset::empty();
    let expr_ids: Vec<types_pathnodes::NodeId> =
        run.pathtarget(run.rel_reltarget_id(rel_id)).exprs.iter().copied().collect();
    for id in expr_ids {
        vars::pull_varattnos(mcx, *run.root.expr_node(id), varno, &mut attrs_used)?;
    }
    let rinfo_ids: Vec<RinfoId> = run.root.rel(rel_id).baserestrictinfo.iter().copied().collect();
    for rid in rinfo_ids {
        let clause = run.root.rinfo(rid).clause;
        vars::pull_varattnos(mcx, *run.root.expr_node(clause), varno, &mut attrs_used)?;
    }

    const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
    let rel = table::table_open(mcx, foreigntableid, types_rel::lock::AccessShareLock)?;
    let mut columns: NodeList<'mcx> = NodeList::nil();
    let mut has_wholerow = false;
    for attidx in attrs_used.iter() {
        let attnum = attidx + FLIHAN;
        if attnum == 0 {
            has_wholerow = true;
            break;
        }
        if attnum < 0 {
            continue;
        }
        let attr = rel.rd_att.attr(attnum as usize - 1);
        if attr.attisdropped || attr.attgenerated != 0 {
            continue;
        }
        // SAFETY: catalog names are server-encoding UTF-8.
        let attname = unsafe {
            core::str::from_utf8_unchecked(mcx::slice_borrow_in(mcx, attr.attname.name_str())?)
        };
        columns.lappend(mcx, mk_string(mcx, attname)?)?;
    }
    let mut numattrs = 0usize;
    for i in 0..rel.rd_att.natts as usize {
        if !rel.rd_att.attr(i).attisdropped {
            numattrs += 1;
        }
    }
    table::table_close(rel, types_rel::lock::AccessShareLock)?;

    if has_wholerow || numattrs == columns.len() {
        return Ok(None);
    }
    Ok(Some(columns))
}

fn file_get_foreign_paths(
    run: &mut PlannerRun<'_>,
    rel_id: RelId,
    foreigntableid: Oid,
) -> PgResult<()> {
    let mcx = run.mcx;
    let fdw_private = {
        let id = run.root.rel(rel_id).fdw_private;
        decode_plan_state(*run.root.expr_node(id))
    };

    let mut coptions: PgVec<'_, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    if let Some(columns) = check_selective_binary_conversion(run, rel_id, foreigntableid)? {
        let arg = if columns.is_nil() { None } else { Some(Node::mk_list(mcx, columns)?) };
        let def = mk_def_elem(mcx, "convert_selectively", arg)?;
        coptions.push(run.intern_expr(def));
    }

    let (startup_cost, total_cost) = estimate_costs(run, rel_id, &fdw_private);

    // No pushdown; required parameterization can still arise from LATERAL
    // refs in the tlist.
    let required_outer = planner::relnode::relids_copy(mcx, &run.root.rel(rel_id).lateral_relids);
    let rows = run.root.rel(rel_id).rows;
    let path = planner::pathnode::create_foreignscan_path(
        run,
        rel_id,
        None,
        rows,
        0,
        startup_cost,
        total_cost,
        PgVec::new_in(mcx),
        &required_outer,
        None,
        PgVec::new_in(mcx),
        coptions,
    )?;
    planner::pathnode::add_path(run, rel_id, path);
    Ok(())
}

fn file_get_foreign_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel_id: RelId,
    _foreigntableid: Oid,
    best_path: PathId,
    tlist: NodeList<'mcx>,
    scan_clauses: PgVec<'mcx, RinfoId>,
    outer_plan: Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let scan_relid = run.root.rel(rel_id).relid;

    // No native qual evaluation: everything stays in the plan qual list.
    let qpqual = planner::createplan::extract_actual_clauses(run, &scan_clauses);

    let fdw_private_ids: Vec<types_pathnodes::NodeId> = match run.root.path(best_path) {
        PathNode::ForeignPath(fp) => fp.fdw_private.iter().copied().collect(),
        other => panic!("fileGetForeignPlan: pathtype {}", other.base().pathtype),
    };
    let mut fdw_private: NodeList<'mcx> = NodeList::nil();
    for id in fdw_private_ids {
        fdw_private.lappend(mcx, *run.root.expr_node(id))?;
    }

    planner::createplan::make_foreignscan(
        mcx,
        tlist,
        qpqual,
        scan_relid,
        NodeList::nil(),
        fdw_private,
        NodeList::nil(),
        NodeList::nil(),
        outer_plan,
    )
}

fn festate<'a>(node: &'a mut ForeignScanState<'_>) -> Option<&'a mut FileFdwExecutionState> {
    node.fdw_state.as_mut().and_then(|s| s.downcast_mut::<FileFdwExecutionState>())
}

fn begin_copy<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    filename: &'mcx str,
    options: &NodeList<'mcx>,
) -> PgResult<CopyFromState<'static, 'static>> {
    let cstate = copy_cmd::BeginCopyFrom(
        mcx,
        rel,
        NodeList::nil(),
        Some(filename),
        &NodeList::nil(),
        options,
        None,
    )?;
    // SAFETY: 'static restamp of es_query_cxt-lived state; dropped at
    // end-scan, before the context resets (see FileFdwExecutionState).
    Ok(unsafe {
        core::mem::transmute::<CopyFromState<'mcx, 'mcx>, CopyFromState<'static, 'static>>(cstate)
    })
}

fn file_begin_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<()> {
    // EXPLAIN (no ANALYZE): fdw_state stays None.
    if eflags & EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return Ok(());
    }
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("foreign scan has a relation");
    let (filename, is_program, mut options) = file_get_options(mcx, rel.rd_id)?;
    if is_program {
        panic!(
            "file_fdw: program option (COPY FROM PROGRAM, OpenPipeStream lane) is unported \
             for table \"{}\"",
            rel.name()
        );
    }
    for def in node.plan.fdw_private.iter() {
        options.lappend(mcx, def)?;
    }
    let cstate = begin_copy(mcx, rel, filename, &options)?;
    // SAFETY: same es_query_cxt restamp as begin_copy.
    let (filename, options) = unsafe {
        (
            core::mem::transmute::<&'mcx str, &'static str>(filename),
            core::mem::transmute::<NodeList<'mcx>, NodeList<'static>>(options),
        )
    };
    node.fdw_state =
        Some(Box::new(FileFdwExecutionState { filename, options, cstate: Some(cstate) }));
    Ok(())
}

#[track_caller]
#[cold]
fn reject_limit_exceeded(limit: i64) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "skipped more than REJECT_LIMIT ({limit}) rows due to data type incompatibility"
        ))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

fn file_iterate_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let scan_slot = node.ss.ss_ScanTupleSlot;
    let ecxt = node.ss.ps_ExprContext;
    let qmcx = estate.es_query_cxt;
    // SAFETY: reset-only per-tuple context (exec_scan resets it before the
    // next fetch), outlives each returned row through projection.
    let row_mcx = unsafe {
        core::mem::transmute::<Mcx<'_>, Mcx<'static>>(estate.ecxt(ecxt).per_tuple_mcx())
    };

    exectuples::exec_clear_tuple(estate.slot_mut(scan_slot), qmcx);
    let cstate = festate(node)
        .expect("fdw_state set by BeginForeignScan")
        .cstate
        .as_mut()
        .expect("cstate live between begin and end");

    loop {
        let found = {
            let base = estate.slot_mut(scan_slot).base_mut();
            match cstate.next_copy_from(row_mcx, &mut base.tts_values, &mut base.tts_isnull) {
                Ok(found) => found,
                // C reaches this through CopyFromErrorCallback.
                Err(e) => return Err(copy_cmd::copy_from_error_context(cstate, e)),
            }
        };
        if !found {
            return Ok(false);
        }
        if cstate.soft_error_occurred() {
            // Soft error: skip the tuple and rearm the ErrorSaveContext.
            cstate.reset_soft_error();
            postgres_seams::check_for_interrupts::call()?;
            estate.ecxt_mut(ecxt).reset();
            if cstate.opts.reject_limit > 0
                && cstate.num_errors() > cstate.opts.reject_limit as u64
            {
                // C raises inside the CopyFromErrorCallback scope.
                return Err(copy_cmd::copy_from_error_context(
                    cstate,
                    reject_limit_exceeded(cstate.opts.reject_limit),
                ));
            }
            continue;
        }
        exectuples::exec_store_virtual_tuple(estate.slot_mut(scan_slot));
        return Ok(true);
    }
}

fn file_rescan_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("foreign scan has a relation");
    let f = node
        .fdw_state
        .as_mut()
        .expect("fdw_state set by BeginForeignScan")
        .downcast_mut::<FileFdwExecutionState>()
        .expect("file_fdw execution state");
    if let Some(cstate) = f.cstate.take() {
        copy_cmd::EndCopyFrom(cstate)?;
    }
    // SAFETY: undoes the begin-time 'static restamp (same es_query_cxt).
    let options =
        unsafe { core::mem::transmute::<&NodeList<'static>, &NodeList<'mcx>>(&f.options) };
    f.cstate = Some(begin_copy(mcx, rel, f.filename, options)?);
    Ok(())
}

fn skipped_rows_notice(cstate: &CopyFromState<'_, '_>) -> PgResult<()> {
    if cstate.opts.on_error == copy_cmd::CopyOnErrorChoice::Ignore
        && cstate.num_errors() > 0
        && cstate.opts.log_verbosity >= copy_cmd::CopyLogVerbosityChoice::Default
    {
        let n = cstate.num_errors();
        let msg = if n == 1 {
            format!("{n} row was skipped due to data type incompatibility")
        } else {
            format!("{n} rows were skipped due to data type incompatibility")
        };
        elog::ereport(NOTICE).errmsg(msg).finish(loc("fileEndForeignScan"))?;
    }
    Ok(())
}

fn file_end_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // fdw_state None: EXPLAIN; nothing to do.
    let Some(f) = festate(node) else { return Ok(()) };
    if let Some(cstate) = f.cstate.take() {
        skipped_rows_notice(&cstate)?;
        copy_cmd::EndCopyFrom(cstate)?;
    }
    Ok(())
}

fn file_explain_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    flags: types_nodes::FdwExplainFlags,
    emit: &mut dyn FnMut(&str, types_nodes::FdwExplainProp<'_>) -> PgResult<()>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("foreign scan has a relation");
    let (filename, is_program, _options) = file_get_options(mcx, rel.rd_id)?;

    let label = if is_program { "Foreign Program" } else { "Foreign File" };
    emit(label, types_nodes::FdwExplainProp::Text(filename))?;

    // C file_fdw.c fileExplainForeignScan: "Suppress file size if we're not
    // showing cost details" — costs-gated; file_fdw reads no verbose bit.
    if flags.costs && !is_program {
        if let Ok(md) = std::fs::metadata(filename) {
            emit(
                "Foreign File Size",
                types_nodes::FdwExplainProp::Integer { value: md.len() as i64, unit: "b" },
            )?;
        }
    }
    Ok(())
}

fn file_analyze_foreign_table<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
) -> PgResult<Option<(AcquireSampleRowsFn, BlockNumber)>> {
    let (filename, is_program, _options) = file_get_options(mcx, relation.rd_id)?;

    if is_program {
        return Ok(None);
    }

    let size = match std::fs::metadata(filename) {
        Ok(md) => md.len(),
        Err(e) => {
            elog::ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not stat file \"{filename}\": %m"))
                .finish(loc("fileAnalyzeForeignTable"))?;
            unreachable!()
        }
    };
    let totalpages = (size.div_ceil(BLCKSZ as u64) as BlockNumber).max(1);
    Ok(Some((file_acquire_sample_rows, totalpages)))
}

fn form_sample_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &types_tuple::TupleDescData<'_>,
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<HeapTupleData<'mcx>> {
    let owned = heaptuple::heap_form_tuple(mcx, tupdesc, values, nulls)?;
    let (ptr, len, tid, oid) = (
        owned.image().as_ptr(),
        owned.as_tuple().t_len,
        owned.as_tuple().t_self,
        owned.as_tuple().t_tableOid,
    );
    core::mem::forget(owned);
    // SAFETY: the image lives in `mcx` until context teardown (analyze
    // copy_slot_tuple precedent); nothing else writes it.
    Ok(unsafe { HeapTupleData::from_raw_parts(ptr, len, tid, oid) })
}

// file_acquire_sample_rows (file_fdw.c): Vitter reservoir over the file.
fn file_acquire_sample_rows<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    elevel: types_error::ErrorLevel,
    rows: &mut PgVec<'mcx, HeapTupleData<'mcx>>,
    targrows: i32,
    totalrows: &mut f64,
    totaldeadrows: &mut f64,
) -> PgResult<i32> {
    debug_assert!(targrows > 0);
    let base = rows.len();
    let mut numrows: i32 = 0;
    let mut rowstoskip = -1.0f64;

    let tupdesc = &onerel.rd_att;
    let natts = tupdesc.natts as usize;
    let mut values = vec![Datum::null(); natts];
    let mut nulls = vec![true; natts];

    let (filename, is_program, options) = file_get_options(mcx, onerel.rd_id)?;
    if is_program {
        panic!(
            "file_fdw: program option (COPY FROM PROGRAM, OpenPipeStream lane) is unported \
             for table \"{}\"",
            onerel.name()
        );
    }
    let mut cstate = copy_cmd::BeginCopyFrom(
        mcx,
        onerel,
        NodeList::nil(),
        Some(filename),
        &NodeList::nil(),
        &options,
        None,
    )?;

    // C's "file_fdw temporary context": rows are read here, then copied into
    // the caller's context by heap_form_tuple.
    let mut tupcontext = MemoryContext::new("file_fdw temporary context");

    let mut rstate = commands_analyze::sampling::reservoir_init_selection_state(
        pg_prng::global_prng(|p| p.next_u64()),
        targrows as u32,
    );

    *totalrows = 0.0;
    *totaldeadrows = 0.0;
    loop {
        postgres_seams::check_for_interrupts::call()?;

        tupcontext.reset();
        // SAFETY: reset-only context; heap_form_tuple copies out before the
        // next reset.
        let row_mcx = unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'mcx>>(tupcontext.mcx()) };
        let found = match cstate.next_copy_from(row_mcx, &mut values, &mut nulls) {
            Ok(found) => found,
            Err(e) => return Err(copy_cmd::copy_from_error_context(&cstate, e)),
        };
        if !found {
            break;
        }

        if cstate.soft_error_occurred() {
            // Soft error: skip the tuple; not counted in totalrows.
            cstate.reset_soft_error();
            continue;
        }

        if numrows < targrows {
            rows.push(form_sample_tuple(mcx, tupdesc, &values, &nulls)?);
            numrows += 1;
        } else {
            if rowstoskip < 0.0 {
                rowstoskip = commands_analyze::sampling::reservoir_get_next_s(
                    &mut rstate,
                    *totalrows,
                    targrows as u32,
                );
            }
            if rowstoskip <= 0.0 {
                let k = (targrows as f64
                    * commands_analyze::sampling::sampler_random_fract(&mut rstate.randstate))
                    as usize;
                debug_assert!(k < targrows as usize);
                // C heap_freetuple's the replaced copy; here it stays in the
                // analyze arena until teardown (native acquire precedent).
                rows[base + k] = form_sample_tuple(mcx, tupdesc, &values, &nulls)?;
            }
            rowstoskip -= 1.0;
        }
        *totalrows += 1.0;
    }

    skipped_rows_notice(&cstate)?;
    copy_cmd::EndCopyFrom(cstate)?;

    elog::ereport(elevel)
        .errmsg(format!(
            "\"{}\": file contains {:.0} rows; {} rows in sample",
            onerel.name(),
            *totalrows,
            numrows
        ))
        .finish(loc("file_acquire_sample_rows"))?;

    Ok(numrows)
}

static PLAN_ROUTINE: FdwPlanRoutine = FdwPlanRoutine {
    get_foreign_rel_size: file_get_foreign_rel_size,
    get_foreign_paths: file_get_foreign_paths,
    get_foreign_plan: file_get_foreign_plan,
};

static EXEC_ROUTINE: FdwExecRoutine = FdwExecRoutine {
    begin: file_begin_foreign_scan,
    iterate: file_iterate_foreign_scan,
    rescan: file_rescan_foreign_scan,
    end: file_end_foreign_scan,
    explain: Some(file_explain_foreign_scan),
};

static ANALYZE_ROUTINE: FdwAnalyzeRoutine =
    FdwAnalyzeRoutine { analyze_foreign_table: file_analyze_foreign_table };

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "file_fdw_handler" => fc_file_fdw_handler,
        "file_fdw_validator" => fc_file_fdw_validator,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
    planner::fdwplan::install_fdw_plan_routine(FdwKind::FileFdw, &PLAN_ROUTINE);
    nodeforeignscan::install_fdw_exec_routine(FdwKind::FileFdw, &EXEC_ROUTINE);
    commands_analyze::install_fdw_analyze_routine(FdwKind::FileFdw, &ANALYZE_ROUTINE);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_option_matrix() {
        assert!(is_valid_option("filename", FOREIGN_TABLE_RELATION_ID));
        assert!(is_valid_option("force_not_null", ATTRIBUTE_RELATION_ID));
        assert!(!is_valid_option("force_not_null", FOREIGN_TABLE_RELATION_ID));
        assert!(!is_valid_option("filename", ATTRIBUTE_RELATION_ID));
        assert!(!is_valid_option("force_quote", FOREIGN_TABLE_RELATION_ID));
        assert!(!is_valid_option("filename", types_core::FOREIGN_SERVER_RELATION_ID));
    }

    #[test]
    fn handler_returns_tagged_static_routine() {
        let mut fcinfo = types_fmgr::LocalFcinfo::<0>::new(0);
        let d = fc_file_fdw_handler(None, &mut fcinfo).unwrap();
        let p = d.as_usize() as *const FdwRoutine;
        // SAFETY: the handler returns a pointer to a static.
        let routine = unsafe { &*p };
        assert_eq!(routine.kind, FdwKind::FileFdw);
        assert_eq!(routine.tag, types_nodes::NodeTag::T_FdwRoutine);
    }
}
