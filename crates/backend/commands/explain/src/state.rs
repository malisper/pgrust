#![allow(non_snake_case, non_camel_case_types)]

use elog::ereport;
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERROR};
use types_nodes::list::NodeList;
use types_nodes::plannodes::PlannedStmt;

use crate::options::{defGetBoolean, defGetString};

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

// Omitted vs C's ExplainState: grouping_stack (non-text formats are loud),
// deparse_cxt/printed_subplans (ruleutils + SubPlan lanes are loud),
// workers_state (ANALYZE is loud), extension_state (no extension options
// registered in this build; ApplyExtensionExplainOption always misses).
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
    pub serialize: ExplainSerializeOption,
    pub format: ExplainFormat,
    pub indent: i32,
    // C's ExplainPrintPlan(es, queryDesc) argument: live while the plan walk
    // may read per-node Instrumentation, NULL otherwise.
    pub qd: types_portal::QueryDescHandle,
    pub pstmt: Option<&'mcx PlannedStmt<'mcx>>,
    pub rtable: Option<&'mcx NodeList<'mcx>>,
    pub rtable_size: i32,
    pub rtable_names: PgVec<'mcx, Option<&'mcx str>>,
    pub hide_workers: bool,
    /// plan_ids already displayed (C printed_subplans).
    pub printed_subplans: types_nodes::bitmapset::Bitmapset<'mcx>,
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
        serialize: EXPLAIN_SERIALIZE_NONE,
        format: EXPLAIN_FORMAT_TEXT,
        indent: 0,
        qd: types_portal::QueryDescHandle::NULL,
        pstmt: None,
        printed_subplans: types_nodes::bitmapset::Bitmapset::empty(),
        rtable: None,
        rtable_size: 0,
        rtable_names: PgVec::new_in(mcx),
        hide_workers: false,
    })
}

#[cold]
fn bad_option_value(defname: &str, value: &str) -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(format!(
            "unrecognized value for EXPLAIN option \"{defname}\": \"{value}\""
        ))
        .into_error()
}

#[cold]
fn requires_analyze(option: &str) -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(format!("EXPLAIN option {option} requires ANALYZE"))
        .into_error()
}

// C signature also takes a ParseState for error cursor positions; positions
// are omitted repo-wide in ereports today.
pub fn ParseExplainOptionList<'mcx>(
    es: &mut ExplainState<'mcx>,
    mcx: Mcx<'mcx>,
    options: &NodeList<'mcx>,
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
            "serialize" => {
                if opt.arg.is_some() {
                    es.serialize = match defGetString(mcx, opt)? {
                        "off" | "none" => EXPLAIN_SERIALIZE_NONE,
                        "text" => EXPLAIN_SERIALIZE_TEXT,
                        "binary" => EXPLAIN_SERIALIZE_BINARY,
                        other => return Err(bad_option_value(defname, other).into()),
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
                    other => return Err(bad_option_value(defname, other).into()),
                };
            }
            // ApplyExtensionExplainOption: no extension options registered.
            other => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("unrecognized EXPLAIN option \"{other}\""))
                    .into_error()
                    .into())
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
