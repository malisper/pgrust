//! EXPLAIN output-state vocabulary (`commands/explain_state.h`), trimmed to the
//! fields the formatter consumes plus the extension-state slots owned by
//! `explain_state.c`.
//!
//! The owner of this state is `explain_state.c` (`NewExplainState`,
//! `ParseExplainOptionList`, the extension registry); this crate holds the
//! type. The node-tree fields of the C `ExplainState` (`pstmt`, `rtable`,
//! `deparse_cxt`, `printed_subplans`, `workers_state`) are intentionally not yet
//! present — they reference the plan-node knot and the per-worker formatting
//! state, which their owning ports will add when they land (extend, never
//! restructure). The `extension_state`/`extension_state_allocated` slots ARE
//! present: `explain_state.c` owns them (`GetExplainExtensionState` /
//! `SetExplainExtensionState`).

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

/// One slot of the C `void **extension_state` array — an extension's opaque
/// private state pointer (`SetExplainExtensionState` stores it,
/// `GetExplainExtensionState` returns it). `void *` is genuinely opaque (an
/// extension's own struct), so this is an opaque handle, not an invented type;
/// `None` is the C `NULL` slot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct ExtensionStateHandle(pub u64);

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
    /* extensions */
    /// `void **extension_state` — per-extension opaque state slots, indexed by
    /// the id `GetExplainExtensionId` hands out. A `None` slot is the C `NULL`.
    pub extension_state: PgVec<'mcx, Option<ExtensionStateHandle>>,
    /// `int extension_state_allocated` — allocated length of `extension_state`.
    pub extension_state_allocated: i32,
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
            extension_state: PgVec::new_in(mcx),
            extension_state_allocated: 0,
        }
    }
}
