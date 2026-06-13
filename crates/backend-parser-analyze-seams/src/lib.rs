//! Seam declarations for the `backend-parser-analyze` unit
//! (`parser/analyze.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::nodes::Node;

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
    /// `pg_analyze_and_rewrite_varparams(parsetree, query_string, &paramTypes,
    /// &numParams, NULL)` (parser/analyze.c via the rewriter): parse-analyze
    /// the raw statement deducing unknown parameter types from context, then
    /// rewrite. Allocates / can `ereport(ERROR)`.
    pub fn analyze_and_rewrite_varparams<'mcx>(
        mcx: Mcx<'mcx>,
        raw_stmt: &Node<'mcx>,
        query_string: &str,
        arg_types: &[Oid],
    ) -> PgResult<AnalyzedVarparams<'mcx>>
);

seam_core::seam!(
    /// `make_parsestate(NULL)` (parser/parse_node.c) — a fresh ParseState. The
    /// EXPLAIN-EXECUTE driver builds one only to carry `p_sourcetext`; this
    /// returns the source-text-bearing minimal state. Allocates.
    pub fn make_parsestate<'mcx>(
        mcx: Mcx<'mcx>,
        source_text: &str,
    ) -> PgResult<mcx::PgBox<'mcx, types_nodes::parsestmt::ParseState<'mcx>>>
);
