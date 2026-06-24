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

use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::copy_query::Query as CopyQuery;
use ::nodes::nodes::Node;
use ::nodes::parsestmt::RawStmt;
use ::nodes::portalcmds::{JumbleState, ParseState as PortalcmdsParseState, Query};

/// The result of `pg_analyze_and_rewrite_varparams`: the rewritten `List *` of
/// `Query *` (owned nodes in `mcx`) plus the possibly grown/replaced parameter
/// OID array (the C function takes `&argtypes`/`&nargs` in/out).
pub struct AnalyzedVarparams<'mcx> {
    /// The rewritten query list.
    pub query_list: ::mcx::PgVec<'mcx, Node<'mcx>>,
    /// The resolved parameter OID array (may differ from the input).
    pub arg_types: ::mcx::PgVec<'mcx, Oid>,
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
    /// `stmt_requires_parse_analysis(raw_parse_tree)` (analyze.c) — VALUE form
    /// over the owned `RawStmt`. True when parse analysis does more than wrap a
    /// CMD_UTILITY Query. plancache's `StmtPlanRequiresRevalidation` calls it on
    /// the stored owned raw statement (the de-handle replaces the handle pc-seam).
    pub fn stmt_requires_parse_analysis_value(raw: &RawStmt) -> PgResult<bool>
);

seam_core::seam!(
    /// `analyze_requires_snapshot(raw_parse_tree)` (analyze.c) — VALUE form over
    /// the owned `RawStmt`. True when parse analysis requires a snapshot to be
    /// set. plancache's `BuildingPlanRequiresSnapshot` calls it on the stored
    /// owned raw statement.
    pub fn analyze_requires_snapshot_value(raw: &RawStmt) -> PgResult<bool>
);

seam_core::seam!(
    /// `query_requires_rewrite_plan(query)` (analyze.c) — VALUE form over the
    /// owned `Query`. True unless the Query is a no-op CMD_UTILITY the
    /// rewriter/planner ignore. plancache calls it on each stored owned analyzed
    /// Query of the querytree list.
    pub fn query_requires_rewrite_plan_value(query: &CopyQuery) -> PgResult<bool>
);

seam_core::seam!(
    /// `if (post_parse_analyze_hook) (*post_parse_analyze_hook)(pstate, query,
    /// jstate);` (the call site analyze.c owns the hook for). Runs extension
    /// code; can `ereport(ERROR)`. `jstate` is `None` when query-id is off.
    ///
    /// This is the inward seam every parse-analyze call site rides; the owner's
    /// installed body consults the settable [`post_parse_analyze_hook_present`] /
    /// [`call_post_parse_analyze_hook`] slot below (the `if (post_parse_analyze_hook)`
    /// guard).
    pub fn run_post_parse_analyze_hook(
        pstate: &PortalcmdsParseState,
        query: &Query,
        jstate: Option<&JumbleState>,
    ) -> PgResult<()>
);

// ===========================================================================
// `post_parse_analyze_hook` (analyze.c): the loadable-module interposition
// point called at the end of parse analysis. In C it is a per-process
// function-pointer global, NULL by default; the call site runs `if
// (post_parse_analyze_hook) post_parse_analyze_hook(pstate, query, jstate);`.
// Modeled like `shmem_request_hook` (miscinit.c): a per-backend thread-local
// `Cell<Option<fn>>` with `set_post_parse_analyze_hook` /
// `post_parse_analyze_hook_present` / `call_post_parse_analyze_hook`. The slot
// lives in this `-seams` crate so a hook-installing module (e.g.
// pg_stat_statements) can register without a dependency cycle. The owner's
// `run_post_parse_analyze_hook` body consults this slot; with no hook set it is
// a no-op — byte-identical to today.
// ===========================================================================

/// `post_parse_analyze_hook_type` (analyze.h): `void (*)(ParseState *pstate,
/// Query *query, JumbleState *jstate)`.
pub type PostParseAnalyzeHook =
    fn(pstate: &PortalcmdsParseState, query: &Query, jstate: Option<&JumbleState>) -> PgResult<()>;

/// The subset of `Query` fields the canonical parse-analysis path exposes to a
/// `post_parse_analyze_hook` (the by-value `portalcmds::Query` token does not
/// carry these; the field-bearing `copy_query::Query<'mcx>` does). Mirrors the
/// `(pstate->p_sourcetext, query)` data a hook such as pg_stat_statements reads.
#[derive(Clone, Copy)]
pub struct PostParseAnalyzeQueryInfo<'a> {
    /// `pstate->p_sourcetext`.
    pub source_text: &'a str,
    /// `query->queryId`.
    pub query_id: i64,
    /// `query->stmt_location`.
    pub stmt_location: i32,
    /// `query->stmt_len`.
    pub stmt_len: i32,
    /// `query->utilityStmt != NULL` — whether this is a utility statement.
    pub is_utility: bool,
    /// `IsA(query->utilityStmt, ExecuteStmt)` — whether the utility statement is
    /// an `EXECUTE` (the hook clears the queryId for these).
    pub utility_is_execute: bool,
}

/// `post_parse_analyze_hook` over the canonical field-bearing `Query<'mcx>`
/// path. Distinct from [`PostParseAnalyzeHook`] (the by-value portalcmds token
/// path) because the two `Query` views are incompatible; this carries the
/// concrete scalar fields + the real `JumbleState` clocations.
pub type PostParseAnalyzeCanonicalHook =
    fn(info: PostParseAnalyzeQueryInfo<'_>, jstate: Option<&JumbleState>) -> PgResult<()>;

thread_local! {
    /// `post_parse_analyze_hook_type post_parse_analyze_hook = NULL;` (analyze.c).
    static POST_PARSE_ANALYZE_HOOK: std::cell::Cell<Option<PostParseAnalyzeHook>> =
        const { std::cell::Cell::new(None) };

    /// Canonical-path variant of `post_parse_analyze_hook` (the field-bearing
    /// `Query<'mcx>` parse-analysis path). Same NULL-by-default semantics.
    static POST_PARSE_ANALYZE_CANONICAL_HOOK:
        std::cell::Cell<Option<PostParseAnalyzeCanonicalHook>> =
        const { std::cell::Cell::new(None) };
}

/// `post_parse_analyze_hook != NULL` for the canonical-path variant.
pub fn post_parse_analyze_canonical_hook_present() -> bool {
    POST_PARSE_ANALYZE_CANONICAL_HOOK.with(|c| c.get().is_some())
}
/// Register a module's canonical-path `post_parse_analyze_hook`.
pub fn set_post_parse_analyze_canonical_hook(
    hook: Option<PostParseAnalyzeCanonicalHook>,
) -> Option<PostParseAnalyzeCanonicalHook> {
    POST_PARSE_ANALYZE_CANONICAL_HOOK.with(|c| c.replace(hook))
}
/// Invoke the registered canonical-path `post_parse_analyze_hook`. Panics if
/// none is registered (callers guard with
/// [`post_parse_analyze_canonical_hook_present`]).
pub fn call_post_parse_analyze_canonical_hook(
    info: PostParseAnalyzeQueryInfo<'_>,
    jstate: Option<&JumbleState>,
) -> PgResult<()> {
    match POST_PARSE_ANALYZE_CANONICAL_HOOK.with(std::cell::Cell::get) {
        Some(hook) => hook(info, jstate),
        None => panic!("call_post_parse_analyze_canonical_hook() called with no hook registered"),
    }
}

/// `post_parse_analyze_hook != NULL` — whether a module registered a
/// post-parse-analyze hook.
pub fn post_parse_analyze_hook_present() -> bool {
    POST_PARSE_ANALYZE_HOOK.with(|c| c.get().is_some())
}
/// Register a module's `post_parse_analyze_hook` (the `post_parse_analyze_hook =
/// my_hook` assignment in `_PG_init`).
pub fn set_post_parse_analyze_hook(
    hook: Option<PostParseAnalyzeHook>,
) -> Option<PostParseAnalyzeHook> {
    POST_PARSE_ANALYZE_HOOK.with(|c| c.replace(hook))
}
/// Invoke the registered `post_parse_analyze_hook(pstate, query, jstate)`.
/// Panics if none is registered (the caller guards with
/// [`post_parse_analyze_hook_present`], mirroring C's `if
/// (post_parse_analyze_hook)`).
pub fn call_post_parse_analyze_hook(
    pstate: &PortalcmdsParseState,
    query: &Query,
    jstate: Option<&JumbleState>,
) -> PgResult<()> {
    match POST_PARSE_ANALYZE_HOOK.with(std::cell::Cell::get) {
        Some(hook) => hook(pstate, query, jstate),
        None => panic!("call_post_parse_analyze_hook() called with no hook registered"),
    }
}

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
        parent: Option<&::nodes::parsestmt::ParseState<'mcx>>,
    ) -> PgResult<::mcx::PgBox<'mcx, ::nodes::parsestmt::ParseState<'mcx>>>
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
        parent_pstate: &mut ::nodes::parsestmt::ParseState<'mcx>,
        parent_cte: Option<&::nodes::rawnodes::CommonTableExpr<'mcx>>,
        locked_from_parent: bool,
        resolve_unknowns: bool,
    ) -> PgResult<::mcx::PgBox<'mcx, Node<'mcx>>>
);
