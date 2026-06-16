//! Seam declarations for the `backend-tcop-utility` unit (`tcop/utility.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::CommandTag;

seam_core::seam!(
    /// `CreateCommandTag(parsetree)` (utility.c) — the `CommandTag` for a raw
    /// parse-tree node (the PREPARE'd query). Pure classification, but reads
    /// the node tree; cannot `ereport` for well-formed nodes.
    pub fn create_command_tag<'mcx>(query: &Node<'mcx>) -> PgResult<CommandTag>
);

seam_core::seam!(
    /// `ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
    /// queryEnv, dest, qc)` (utility.c) — execute a utility (non-optimizable)
    /// statement. `pquery.c`'s `PortalRunUtility` drives it for the portal's
    /// `PlannedStmt`. The owned model drops the C `queryEnv` argument (as
    /// `QueryDesc::create` does). The receiver is the router-keyed
    /// [`DestReceiverHandle`]; `qc` is filled in place. Can `ereport(ERROR)`.
    pub fn process_utility<'mcx>(
        pstmt: &types_nodes::nodeindexscan::PlannedStmt<'mcx>,
        query_string: &str,
        read_only_tree: bool,
        context: types_nodes::parsestmt::ProcessUtilityContext,
        params: types_nodes::portalcmds::ParamListInfo,
        dest: types_nodes::parsestmt::DestReceiverHandle,
        qc: &mut types_portal::QueryCompletion,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `UtilityReturnsTuples(parsetree)` (utility.c) — does running this utility
    /// statement produce a result set? `ChoosePortalStrategy`'s `CMD_UTILITY`
    /// leg uses it to pick `PORTAL_UTIL_SELECT`. Pure classification over the
    /// utility node; cannot `ereport` for well-formed nodes.
    pub fn utility_returns_tuples<'mcx>(stmt: &Node<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `UtilityTupleDescriptor(parsetree)` (utility.c) — the result tuple
    /// descriptor a tuple-returning utility statement produces (`None` when it
    /// returns no tuples). `PortalStart`'s `PORTAL_UTIL_SELECT` leg uses it.
    /// Allocates the descriptor in `mcx`; fallible.
    pub fn utility_tuple_descriptor<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &Node<'mcx>,
    ) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>>
);
