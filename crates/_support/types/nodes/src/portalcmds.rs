//! Parse/plan-tree vocabulary consumed by the portalcmds unit
//! (`nodes/parsenodes.h`, `nodes/plannodes.h`, `nodes/params.h`,
//! `nodes/queryjumble.h`, `parser/parse_node.h`), trimmed to consumed fields.

use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;

use crate::nodes::CmdType;

// Cursor option bitmask (nodes/parsenodes.h) — values verified against
// PostgreSQL 18.3.
pub const CURSOR_OPT_BINARY: i32 = 0x0001;
pub const CURSOR_OPT_SCROLL: i32 = 0x0002;
pub const CURSOR_OPT_NO_SCROLL: i32 = 0x0004;
pub const CURSOR_OPT_INSENSITIVE: i32 = 0x0008;
pub const CURSOR_OPT_ASENSITIVE: i32 = 0x0010;
pub const CURSOR_OPT_HOLD: i32 = 0x0020;
pub const CURSOR_OPT_FAST_PLAN: i32 = 0x0100;
pub const CURSOR_OPT_GENERIC_PLAN: i32 = 0x0200;
pub const CURSOR_OPT_CUSTOM_PLAN: i32 = 0x0400;
pub const CURSOR_OPT_PARALLEL_OK: i32 = 0x0800;

/// `Query` (`nodes/parsenodes.h`), trimmed to the fields portalcmds reads.
/// The analyzed query is otherwise an opaque pass-through value threaded
/// through the jumble/rewrite/plan seams; only `commandType` is inspected
/// here (after rewriting).
///
/// K1 phase 2 decision: this stays a *distinct, documented* `Rc`-token rather
/// than re-exporting the canonical [`crate::copy_query::Query`]. They are two
/// trimmed views of the same C `Query`, but with incompatible models: the
/// canonical one is arena-lifetimed (`Query<'mcx>`) and field-bearing, whereas
/// this token is a refcounted by-value pass-through (no `'mcx`) consumed by
/// `postgres-seams` / `queryjumble-seams` / `rewritehandler-seams` by value.
/// Unifying onto one definition would force a `'mcx` (and a different field
/// set) onto those by-value consumers — a behavior/signature change out of
/// scope for this re-export pass. Both collapse into the central node model in
/// a later K1 keystone.
pub struct Query {
    /// `CmdType commandType` — select|insert|update|delete|merge|utility.
    pub commandType: CmdType,
    /// The remainder of the query tree, owned by the parser/rewriter and
    /// passed opaquely through the planning pipeline.
    pub payload: Rc<RefCell<QueryPayload>>,
}

impl Query {
    pub fn new(command_type: CmdType) -> Self {
        Query {
            commandType: command_type,
            payload: Rc::new(RefCell::new(QueryPayload::default())),
        }
    }
}

/// The not-yet-modeled remainder of a `Query` (rtable, targetList, jointree,
/// ...). portalcmds never inspects it; the parser/analyzer/rewriter own it.
#[derive(Default)]
pub struct QueryPayload {
    _private: (),
}

/// One constant location recorded by `JumbleQuery` (`LocationLen` in
/// `nodes/queryjumble.h`). The post-parse-analyze hook reads these to build a
/// normalized query string (replacing constants with `$n`).
#[derive(Clone, Copy, Debug)]
pub struct JumbleLocationLen {
    /// start offset in query text
    pub location: i32,
    /// length in bytes, or -1 to ignore
    pub length: i32,
    /// Does this location represent a squashed list?
    pub squashed: bool,
    /// Is this location a PARAM_EXTERN parameter?
    pub extern_param: bool,
}

/// `JumbleState` (`nodes/queryjumble.h`) — produced by `JumbleQuery`, consumed
/// as the third argument of `post_parse_analyze_hook`. Carries the
/// constant-location array (the fields a hook such as pg_stat_statements reads
/// to normalize a query); the internal `jumble`/`pending_nulls` working buffer
/// is private to the jumble owner and not represented here.
pub struct JumbleState {
    /// `JumbleState.clocations` — locations of constants that may be replaced.
    pub clocations: Vec<JumbleLocationLen>,
    /// `JumbleState.highest_extern_param_id`.
    pub highest_extern_param_id: i32,
    /// `JumbleState.has_squashed_lists`.
    pub has_squashed_lists: bool,
}

impl JumbleState {
    /// Construct an empty placeholder (the `JumbleQuery` seam's return value
    /// for paths that compute a query-id but record no constant locations, e.g.
    /// the DECLARE-CURSOR pass-through).
    pub fn opaque() -> Self {
        JumbleState {
            clocations: Vec::new(),
            highest_extern_param_id: 0,
            has_squashed_lists: false,
        }
    }

    /// `clocations_count` — number of recorded constant locations.
    pub fn clocations_count(&self) -> usize {
        self.clocations.len()
    }
}

/// `DeclareCursorStmt` (`nodes/parsenodes.h`) — `DECLARE CURSOR`.
pub struct DeclareCursorStmt {
    /// `char *portalname`.
    pub portalname: Option<String>,
    /// `int options` — cursor option bits.
    pub options: i32,
    /// `Node *query` — the analyzed `Query` (`castNode(Query, ...)`).
    pub query: Option<Box<Query>>,
}

/// `FetchDirection` (`nodes/parsenodes.h`). Canonically defined in
/// `crate::ddlnodes`; re-exported here so both modules share one type.
pub use crate::ddlnodes::FetchDirection;
pub use crate::ddlnodes::{FETCH_ABSOLUTE, FETCH_BACKWARD, FETCH_FORWARD, FETCH_RELATIVE};

/// `FetchStmt` (`nodes/parsenodes.h`) — `FETCH` (also `MOVE`).
pub struct FetchStmt {
    /// `FetchDirection direction`.
    pub direction: FetchDirection,
    /// `long howMany` — number of rows, or position argument.
    pub howMany: i64,
    /// `char *portalname`.
    pub portalname: Option<String>,
    /// `bool ismove` — true if `MOVE`.
    pub ismove: bool,
}

/// `ParseState` (`parser/parse_node.h`), trimmed to the field portalcmds reads.
pub struct ParseState {
    /// `const char *p_sourcetext` — source text, or `None` if not available.
    pub p_sourcetext: Option<String>,
}

/// `ParamListInfo` (`nodes/params.h`) — the bound parameter list passed to a
/// query. Re-exported from the canonical [`crate::params`] vocabulary: this is
/// the same shared-by-`Rc` value the executor reads (`params[id-1]` /
/// `numParams`), not an opaque placeholder. portalcmds copies it into the
/// portal context and threads it into `PortalStart`. `None` is the C NULL.
pub use crate::params::{ParamListInfo, ParamListInfoData};
