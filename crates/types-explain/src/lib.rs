//! EXPLAIN output-state vocabulary (`commands/explain_state.h`), trimmed to the
//! fields the formatter consumes.
//!
//! The owner of this state is `explain_state.c` (`NewExplainState`,
//! `ParseExplainOptionList`, the extension registry); this crate holds only the
//! type. The node-tree fields of the C `ExplainState` (`pstmt`, `rtable`,
//! `deparse_cxt`, `printed_subplans`, `workers_state`, `extension_state`) are
//! intentionally not yet present — they reference the plan-node knot and the
//! per-worker formatting state, which their owning ports will add when they land
//! (extend, never restructure).

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_camel_case_types)]

use mcx::{Mcx, PgString, PgVec};

/// `typedef enum ExplainFormat` (commands/explain_state.h) — output format.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ExplainFormat {
    #[default]
    EXPLAIN_FORMAT_TEXT,
    EXPLAIN_FORMAT_XML,
    EXPLAIN_FORMAT_JSON,
    EXPLAIN_FORMAT_YAML,
}

/// `typedef enum ExplainSerializeOption` (commands/explain_state.h) — serialize
/// the query's output?
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ExplainSerializeOption {
    #[default]
    EXPLAIN_SERIALIZE_NONE,
    EXPLAIN_SERIALIZE_TEXT,
    EXPLAIN_SERIALIZE_BINARY,
}

/// `typedef struct ExplainState` (commands/explain_state.h) — the EXPLAIN output
/// state. Trimmed to the output buffer, option flags, format, and grouping state
/// the formatter (`explain_format.c`) reads and writes; the plan-tree/per-worker
/// fields are added by their owning ports.
///
/// `str` (C `StringInfo`, always non-NULL during formatting) is the
/// context-allocated [`PgString`]; `grouping_stack` (C integer `List`) is the
/// context-allocated [`PgVec`] of `i32`.
#[derive(Debug)]
pub struct ExplainState<'mcx> {
    /// `StringInfo str` — output buffer.
    pub str: PgString<'mcx>,
    /* options */
    /// `bool verbose` — be verbose.
    pub verbose: bool,
    /// `bool analyze` — print actual times.
    pub analyze: bool,
    /// `bool costs` — print estimated costs.
    pub costs: bool,
    /// `bool buffers` — print buffer usage.
    pub buffers: bool,
    /// `bool wal` — print WAL usage.
    pub wal: bool,
    /// `bool timing` — print detailed node timing.
    pub timing: bool,
    /// `bool summary` — print total planning and execution timing.
    pub summary: bool,
    /// `bool memory` — print planner's memory usage information.
    pub memory: bool,
    /// `bool settings` — print modified settings.
    pub settings: bool,
    /// `bool generic` — generate a generic plan.
    pub generic: bool,
    /// `ExplainSerializeOption serialize` — serialize the query's output?
    pub serialize: ExplainSerializeOption,
    /// `ExplainFormat format` — output format.
    pub format: ExplainFormat,
    /* state for output formatting --- not reset for each new plan tree */
    /// `int indent` — current indentation level.
    pub indent: i32,
    /// `List *grouping_stack` — format-specific grouping state (integer list).
    pub grouping_stack: PgVec<'mcx, i32>,
    /// `bool hide_workers` — set if we find an invisible Gather.
    pub hide_workers: bool,
    /// `int rtable_size` — length of rtable excluding the RTE_GROUP entry.
    pub rtable_size: i32,
}

impl<'mcx> ExplainState<'mcx> {
    /// A fresh formatting state charged to `mcx`, mirroring the zeroed
    /// `NewExplainState` (empty buffer, `EXPLAIN_FORMAT_TEXT`, indent 0, empty
    /// grouping stack).
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        ExplainState {
            str: PgString::new_in(mcx),
            verbose: false,
            analyze: false,
            costs: false,
            buffers: false,
            wal: false,
            timing: false,
            summary: false,
            memory: false,
            settings: false,
            generic: false,
            serialize: ExplainSerializeOption::EXPLAIN_SERIALIZE_NONE,
            format: ExplainFormat::EXPLAIN_FORMAT_TEXT,
            indent: 0,
            grouping_stack: PgVec::new_in(mcx),
            hide_workers: false,
            rtable_size: 0,
        }
    }
}
