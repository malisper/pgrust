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
    /// `pg_analyze_and_rewrite_fixedparams(parsetree, query_string, paramTypes,
    /// numParams, queryEnv)` (tcop/postgres.c) — the VALUE form that threads the
    /// caller's `paramTypes` array (the count is `param_types.len()` matching the
    /// C `numParams`). Unlike the no-param
    /// [`pg_analyze_and_rewrite_fixedparams`] above (which COPY uses with an empty
    /// param array), plancache's `RevalidateCachedQuery` fixedparams branch
    /// (plancache.c:810) MUST pass `plansource->param_types`; this seam carries
    /// them. The rewritten queries are allocated in `mcx`. `queryEnv` is `NULL`
    /// on this path. `Err` carries any analysis/rewrite `ereport(ERROR)`. This is
    /// the param-threading VALUE counterpart plancache's F0 de-handle switches to.
    pub fn pg_analyze_and_rewrite_fixedparams_params<'mcx>(
        mcx: Mcx<'mcx>,
        parsetree: &RawStmt<'mcx>,
        query_string: &str,
        param_types: &[Oid],
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
    /// delimited sub-statement in a child `ParseState` built off
    /// `parent_pstate`, returning the resulting `Query` (wrapped as a
    /// `Node::Query`, mirroring the C `(Node *) query`). `parent_cte` is the C
    /// `CommonTableExpr *parentCTE` (`Some` for a CTE body — `parse_cte`'s
    /// `analyzeCTE` passes the cte; `None` for a FROM sub-SELECT — parse_clause's
    /// `transformRangeSubselect`). The owner `parser/analyze.c` is not yet
    /// ported. `Err` carries the analysis `ereport(ERROR)` surface. Allocates.
    pub fn parse_sub_analyze<'mcx>(
        mcx: Mcx<'mcx>,
        parse_tree: &Node<'mcx>,
        parent_pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
        parent_cte: Option<&types_nodes::rawnodes::CommonTableExpr<'mcx>>,
        locked_from_parent: bool,
        resolve_unknowns: bool,
    ) -> PgResult<mcx::PgBox<'mcx, Node<'mcx>>>
);
