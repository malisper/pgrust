//! (`parser/analyze.c` / `tcop/postgres.c`'s rewrite wrappers).
//!
//! Consumer slices:
//!  * COPY-(query)-TO: the driver's `pg_analyze_and_rewrite_fixedparams` call.
//!  * portalcmds: the `post_parse_analyze_hook` runner it invokes after
//!    (re)jumbling the cursor query. The hook is a per-backend function pointer
//!    extensions install (NULL by default); the owner runs it (a no-op when
//!    unset, `if (post_parse_analyze_hook) ...`).
//!  * PREPARE/EXECUTE: parse-analyze + rewrite of the raw statement with
//!    varparam deduction, plus the throwaway ParseState the EXPLAIN-EXECUTE
//!    driver builds to carry `p_sourcetext`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::copy_query::Query as CopyQuery;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::RawStmt;
use types_nodes::portalcmds::{JumbleState, ParseState as PortalcmdsParseState, Query};

/// The result of `pg_analyze_and_rewrite_varparams`: the rewritten `List *` of
/// `Query *` (owned nodes in `mcx`) plus the possibly grown/replaced parameter
/// OID array (the C function takes `&argtypes`/`&nargs` in/out).
pub struct AnalyzedVarparams<'mcx> {
    /// The rewritten query list.
    pub query_list: mcx::PgVec<'mcx, Node<'mcx>>,
    /// The resolved parameter OID array (may differ from the input).
    pub arg_types: mcx::PgVec<'mcx, Oid>,
}

seam_core::seam!(
    /// `pg_analyze_and_rewrite_fixedparams(parsetree, query_string, paramTypes,
    /// numParams, queryEnv)` (tcop/postgres.c): parse-analyze and rewrite a raw
    /// statement, returning the list of rewritten `Query`s (also acquiring the
    /// source tables' locks). COPY passes no parameters and a NULL query
    /// environment. The rewritten queries are allocated in `mcx`. `Err` carries
    /// any analysis/rewrite `ereport(ERROR)`.
    pub fn pg_analyze_and_rewrite_fixedparams<'mcx>(
        mcx: Mcx<'mcx>,
        parsetree: &RawStmt<'mcx>,
        query_string: &str,
    ) -> PgResult<PgVec<'mcx, CopyQuery<'mcx>>>
);

seam_core::seam!(
    /// `if (post_parse_analyze_hook) (*post_parse_analyze_hook)(pstate, query,
    /// jstate);` (the call site analyze.c owns the hook for). Runs extension
    /// code; can `ereport(ERROR)`. `jstate` is `None` when query-id is off.
    pub fn run_post_parse_analyze_hook(
        pstate: &PortalcmdsParseState,
        query: &Query,
        jstate: Option<&JumbleState>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `pg_analyze_and_rewrite_varparams(parsetree, query_string, &paramTypes,
    /// &numParams, NULL)` (parser/analyze.c via the rewriter): parse-analyze
    /// the raw statement deducing unknown parameter types from context, then
    /// rewrite. Allocates / can `ereport(ERROR)`.
    pub fn analyze_and_rewrite_varparams<'mcx>(
        mcx: Mcx<'mcx>,
        raw_stmt: &RawStmt<'mcx>,
        query_string: &str,
        arg_types: &[Oid],
    ) -> PgResult<AnalyzedVarparams<'mcx>>
);

seam_core::seam!(
    /// `make_parsestate(parentParseState)` (parser/parse_node.c) — allocate and
    /// initialize a new `ParseState`. `palloc0` image with the two nonzero
    /// starts (`p_next_resno = 1`, `p_resolve_unknowns = true`); when `parent`
    /// is `Some`, the source text, the five parser hooks (+ ref-hook state), and
    /// the query environment are copied from it. `None` is the C `NULL`. The
    /// EXPLAIN-EXECUTE driver builds a top-level state (`None` parent) and then
    /// sets `p_sourcetext` itself. Allocates.
    pub fn make_parsestate<'mcx>(
        mcx: Mcx<'mcx>,
        parent: Option<&types_nodes::parsestmt::ParseState<'mcx>>,
    ) -> PgResult<mcx::PgBox<'mcx, types_nodes::parsestmt::ParseState<'mcx>>>
);

seam_core::seam!(
    /// `parse_sub_analyze(parseTree, parentParseState, parentCTE,
    /// locked_from_parent, resolve_unknowns)` (parser/analyze.c) — analyze a
    /// delimited subquery (a sub-SELECT in FROM, or a CTE/sublink body) under a
    /// child `ParseState` linked to `parent`, returning the transformed `Query`.
    /// The FROM sub-select caller passes `parentCTE = NULL` (not modeled here;
    /// the owner builds the child state from `parent`). Can `ereport(ERROR)`.
    pub fn parse_sub_analyze<'mcx>(
        mcx: Mcx<'mcx>,
        parse_tree: &Node<'mcx>,
        parent: &mut types_nodes::parsestmt::ParseState<'mcx>,
        locked_from_parent: bool,
        resolve_unknowns: bool,
    ) -> PgResult<CopyQuery<'mcx>>
);
