//! Parse/plan-tree vocabulary consumed by the portalcmds unit
//! (`nodes/parsenodes.h`, `nodes/plannodes.h`, `nodes/params.h`,
//! `nodes/queryjumble.h`, `parser/parse_node.h`), trimmed to consumed fields.

use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::String;
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

/// `JumbleState` (`nodes/queryjumble.h`) — produced by `JumbleQuery`, consumed
/// only as the third argument of `post_parse_analyze_hook`. Opaque to
/// portalcmds (`None` = the C `NULL`).
pub struct JumbleState {
    _private: (),
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
