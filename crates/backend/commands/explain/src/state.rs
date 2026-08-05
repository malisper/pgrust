#![allow(non_snake_case, non_camel_case_types)]

use elog::ereport;
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERROR};
use types_nodes::list::NodeList;
use types_nodes::plannodes::PlannedStmt;

use define::{defGetBoolean, defGetString};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum ExplainFormat {
    #[default]
    EXPLAIN_FORMAT_TEXT = 0,
    EXPLAIN_FORMAT_XML = 1,
    EXPLAIN_FORMAT_JSON = 2,
    EXPLAIN_FORMAT_YAML = 3,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum ExplainSerializeOption {
    #[default]
    EXPLAIN_SERIALIZE_NONE = 0,
    EXPLAIN_SERIALIZE_TEXT = 1,
    EXPLAIN_SERIALIZE_BINARY = 2,
}

pub use ExplainFormat::*;
pub use ExplainSerializeOption::*;

// ExplainWorkersState (explain.c): per-worker set-aside output buffers,
// swapped in around per-worker detail writes; the open slot holds the main
// buffer until the matching close.
pub struct WorkersState<'mcx> {
    pub num_workers: usize,
    pub worker_inited: PgVec<'mcx, bool>,
    pub worker_str: PgVec<'mcx, Option<stringinfo::StringInfo<'mcx>>>,
    pub worker_state_save: PgVec<'mcx, i32>,
}

pub struct ExplainState<'mcx> {
    pub str: StringInfo<'mcx>,
    pub verbose: bool,
    pub analyze: bool,
    pub costs: bool,
    pub buffers: bool,
    pub wal: bool,
    pub timing: bool,
    pub summary: bool,
    pub memory: bool,
    pub settings: bool,
    pub generic: bool,
    /// pgrust-only EXPLAIN (ENGINE) (single-executor Phase 0.2): annotate
    /// each node with the engine that owns it (lane / spine / fused-arm /
    /// runtime) and the lane RefuseReason for spine nodes. Requires ANALYZE
    /// in increment 1 (the static preview is inc-2). NOTE the PG18
    /// BUFFERS-defaults-to-ANALYZE footgun: bare EXPLAIN (ENGINE, ANALYZE)
    /// carries INSTRUMENT_TIMER|BUFFERS, which the runtime arms refuse —
    /// runtime attribution needs EXPLAIN (ENGINE, ANALYZE, TIMING OFF,
    /// BUFFERS OFF).
    pub engine: bool,
    pub serialize: ExplainSerializeOption,
    pub format: ExplainFormat,
    pub indent: i32,
    /// JSON/YAML per-group emission state (C's integer list; head = last).
    pub grouping_stack: PgVec<'mcx, i32>,
    // C's ExplainPrintPlan(es, queryDesc) argument: live while the plan walk
    // may read per-node Instrumentation, NULL otherwise.
    pub qd: types_portal::QueryDescHandle,
    pub pstmt: Option<&'mcx PlannedStmt<'mcx>>,
    pub rtable: Option<&'mcx NodeList<'mcx>>,
    pub rtable_size: i32,
    pub rtable_names: PgVec<'mcx, Option<&'mcx str>>,
    pub hide_workers: bool,
    pub workers_state: Option<WorkersState<'mcx>>,
    /// pgrust-only (runtime-cost-model design §5 step 2): the m5 cost-shadow
    /// sample of the planning that produced this plan, printed as one
    /// "M5 Cost Route" line. Filled ONLY by standard_ExplainOneQuery (fresh
    /// plan — the prepared-statement path deliberately never fills it: a
    /// cached plan's sample belongs to whoever planned it) and ONLY while
    /// `PGRUST_M5_COST_EXPLAIN` is armed; None keeps EXPLAIN output
    /// byte-identical to today.
    pub m5_cost_route: Option<planner::m5_suppress::cost_shadow::ExplainSample>,
    /// C's void*-per-extension slot array, indexed by GetExplainExtensionId.
    pub extension_state: PgVec<'mcx, Option<&'mcx (dyn core::any::Any + 'static)>>,
    /// plan_ids already displayed (C printed_subplans).
    pub printed_subplans: types_nodes::bitmapset::Bitmapset<'mcx>,
    pub deparse_cxt: Option<ruleutils::PlanDeparse<'mcx>>,
}

pub fn NewExplainState(mcx: Mcx<'_>) -> PgResult<ExplainState<'_>> {
    Ok(ExplainState {
        str: StringInfo::new_in(mcx)?,
        verbose: false,
        analyze: false,
        costs: true,
        buffers: false,
        wal: false,
        timing: false,
        summary: false,
        memory: false,
        settings: false,
        generic: false,
        engine: false,
        serialize: EXPLAIN_SERIALIZE_NONE,
        format: EXPLAIN_FORMAT_TEXT,
        indent: 0,
        grouping_stack: PgVec::new_in(mcx),
        qd: types_portal::QueryDescHandle::NULL,
        pstmt: None,
        printed_subplans: types_nodes::bitmapset::Bitmapset::empty(),
        rtable: None,
        rtable_size: 0,
        rtable_names: PgVec::new_in(mcx),
        hide_workers: false,
        workers_state: None,
        m5_cost_route: None,
        extension_state: PgVec::new_in(mcx),
        deparse_cxt: None,
    })
}

// explain_state.c extension surface. C keeps these in per-backend statics;
// one backend = one thread here.
pub type ExplainOptionHandler =
    for<'a> fn(&mut ExplainState<'a>, &types_nodes::parsenodes::DefElem<'a>, Mcx<'a>) -> PgResult<()>;
pub type ExplainPerNodeHook = for<'a> fn(
    types_nodes::Node<'a>,
    Option<&str>,
    Option<&str>,
    &mut ExplainState<'a>,
) -> PgResult<()>;
pub type ExplainPerPlanHook =
    for<'a> fn(&'a PlannedStmt<'a>, &mut ExplainState<'a>, &str) -> PgResult<()>;

thread_local! {
    static EXTENSION_NAMES: std::cell::RefCell<Vec<&'static str>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static EXTENSION_OPTIONS: std::cell::RefCell<Vec<(&'static str, ExplainOptionHandler)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static PER_NODE_HOOK: std::cell::Cell<Option<ExplainPerNodeHook>> =
        const { std::cell::Cell::new(None) };
    static PER_PLAN_HOOK: std::cell::Cell<Option<ExplainPerPlanHook>> =
        const { std::cell::Cell::new(None) };
}

pub fn GetExplainExtensionId(extension_name: &'static str) -> usize {
    EXTENSION_NAMES.with_borrow_mut(|names| {
        if let Some(i) = names.iter().position(|n| *n == extension_name) {
            return i;
        }
        names.push(extension_name);
        names.len() - 1
    })
}

pub fn GetExplainExtensionState<'mcx>(
    es: &ExplainState<'mcx>,
    extension_id: usize,
) -> Option<&'mcx (dyn core::any::Any + 'static)> {
    es.extension_state.get(extension_id).copied().flatten()
}

pub fn SetExplainExtensionState<'mcx>(
    es: &mut ExplainState<'mcx>,
    extension_id: usize,
    opaque: &'mcx (dyn core::any::Any + 'static),
) {
    while es.extension_state.len() <= extension_id {
        es.extension_state.push(None);
    }
    es.extension_state[extension_id] = Some(opaque);
}

pub fn RegisterExtensionExplainOption(option_name: &'static str, handler: ExplainOptionHandler) {
    EXTENSION_OPTIONS.with_borrow_mut(|opts| {
        if let Some(e) = opts.iter_mut().find(|(n, _)| *n == option_name) {
            e.1 = handler;
        } else {
            opts.push((option_name, handler));
        }
    });
}

fn ApplyExtensionExplainOption<'mcx>(
    es: &mut ExplainState<'mcx>,
    opt: &types_nodes::parsenodes::DefElem<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<bool> {
    let handler = EXTENSION_OPTIONS.with_borrow(|opts| {
        opts.iter()
            .find(|(n, _)| Some(*n) == opt.defname)
            .map(|(_, h)| *h)
    });
    match handler {
        Some(h) => h(es, opt, mcx).map(|()| true),
        None => Ok(false),
    }
}

pub fn explain_per_node_hook() -> Option<ExplainPerNodeHook> {
    PER_NODE_HOOK.get()
}

pub fn set_explain_per_node_hook(hook: Option<ExplainPerNodeHook>) {
    PER_NODE_HOOK.set(hook);
}

pub fn explain_per_plan_hook() -> Option<ExplainPerPlanHook> {
    PER_PLAN_HOOK.get()
}

pub fn set_explain_per_plan_hook(hook: Option<ExplainPerPlanHook>) {
    PER_PLAN_HOOK.set(hook);
}

#[cold]
fn bad_option_value(defname: &str, value: &str, pos: i32) -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(format!(
            "unrecognized value for EXPLAIN option \"{defname}\": \"{value}\""
        ))
        .errposition(pos)
        .into_error()
}

// parser_errposition over the utility statement's query string (C passes the
// ParseState down; only its p_sourcetext is needed here).
fn opt_errposition(query_string: &str, location: i32) -> i32 {
    parser_small1::parser_errposition_source(
        Some(query_string.as_bytes()),
        location,
        mbutils::GetDatabaseEncoding(),
    )
}

#[cold]
fn requires_analyze(option: &str) -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(format!("EXPLAIN option {option} requires ANALYZE"))
        .into_error()
}

// C signature takes a ParseState; only its p_sourcetext feeds the error
// cursor positions, so the query string stands in for it.
pub fn ParseExplainOptionList<'mcx>(
    es: &mut ExplainState<'mcx>,
    mcx: Mcx<'mcx>,
    options: &NodeList<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let mut timing_set = false;
    let mut buffers_set = false;
    let mut summary_set = false;

    for opt_node in options.iter() {
        let opt = opt_node.as_def_elem().expect("EXPLAIN options are DefElems");
        let defname = opt.defname.unwrap_or("");
        match defname {
            "analyze" => es.analyze = defGetBoolean(opt)?,
            "verbose" => es.verbose = defGetBoolean(opt)?,
            "costs" => es.costs = defGetBoolean(opt)?,
            "buffers" => {
                buffers_set = true;
                es.buffers = defGetBoolean(opt)?;
            }
            "wal" => es.wal = defGetBoolean(opt)?,
            "settings" => es.settings = defGetBoolean(opt)?,
            "generic_plan" => es.generic = defGetBoolean(opt)?,
            "timing" => {
                timing_set = true;
                es.timing = defGetBoolean(opt)?;
            }
            "summary" => {
                summary_set = true;
                es.summary = defGetBoolean(opt)?;
            }
            "memory" => es.memory = defGetBoolean(opt)?,
            // pgrust-only (no C counterpart): per-node engine attribution.
            "engine" => es.engine = defGetBoolean(opt)?,
            "serialize" => {
                if opt.arg.is_some() {
                    es.serialize = match defGetString(mcx, opt)? {
                        "off" | "none" => EXPLAIN_SERIALIZE_NONE,
                        "text" => EXPLAIN_SERIALIZE_TEXT,
                        "binary" => EXPLAIN_SERIALIZE_BINARY,
                        other => {
                            return Err(bad_option_value(
                                defname,
                                other,
                                opt_errposition(query_string, opt.location),
                            )
                            .into())
                        }
                    };
                } else {
                    es.serialize = EXPLAIN_SERIALIZE_TEXT;
                }
            }
            "format" => {
                es.format = match defGetString(mcx, opt)? {
                    "text" => EXPLAIN_FORMAT_TEXT,
                    "xml" => EXPLAIN_FORMAT_XML,
                    "json" => EXPLAIN_FORMAT_JSON,
                    "yaml" => EXPLAIN_FORMAT_YAML,
                    other => {
                        return Err(bad_option_value(
                            defname,
                            other,
                            opt_errposition(query_string, opt.location),
                        )
                        .into())
                    }
                };
            }
            other => {
                if !ApplyExtensionExplainOption(es, opt, mcx)? {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!("unrecognized EXPLAIN option \"{other}\""))
                        .errposition(opt_errposition(query_string, opt.location))
                        .into_error()
                        .into());
                }
            }
        }
    }

    if es.wal && !es.analyze {
        return Err(requires_analyze("WAL").into());
    }
    es.timing = if timing_set { es.timing } else { es.analyze };
    es.buffers = if buffers_set { es.buffers } else { es.analyze };
    if es.timing && !es.analyze {
        return Err(requires_analyze("TIMING").into());
    }
    if es.serialize != EXPLAIN_SERIALIZE_NONE && !es.analyze {
        return Err(requires_analyze("SERIALIZE").into());
    }
    // Increment-1 scope (integration contract, WS-C amendment 1): the
    // ENGINE attribution is observed at the executor's admission
    // chokepoints, which need a real execution; the side-effect-free static
    // preview is a ledgered inc-2 item.
    if es.engine && !es.analyze {
        return Err(requires_analyze("ENGINE").into());
    }
    if es.generic && es.analyze {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("EXPLAIN options ANALYZE and GENERIC_PLAN cannot be used together".to_string())
            .into_error()
            .into());
    }
    es.summary = if summary_set { es.summary } else { es.analyze };
    // explain_validate_options_hook: no plugin surface exists.
    Ok(())
}
