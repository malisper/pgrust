//! Partition-bound transforms (`parse_utilcmd.c`).
//!
//! `transformPartitionBound` / `transformPartitionRangeBounds` /
//! `validateInfiniteBounds` / `transformPartitionBoundValue` all compute over
//! the `PartitionBoundSpec` parsenode plus the parent's `PartitionKey` from the
//! relcache/partcache and the expression / coercion / planner-evaluation engine
//! (`transformExpr` / `coerce_to_target_type` / `expression_planner` /
//! `evaluate_expr`). None of that is reachable from this crate, so
//! `transformPartitionBound` is routed through the outward seam, and
//! `transformPartitionCmd` reads the parent's relkind through that same path.

use types_core::Oid;
use types_error::PgResult;

use types_nodes::nodes::{ntag, Node};
use types_nodes::parsestmt::ParseState;

use backend_parser_parse_utilcmd_outward_seams as sx;

use crate::core::{CreateStmtContext, NodePtr};

/// `transformPartitionBound` — transform a partition `FOR VALUES` bound. The
/// `PartitionBoundSpec` parsenode and the parent's `PartitionKey` are reached
/// through the seam; the bound and the result are carried as the bound `Node`.
pub fn transformPartitionBound<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    parent_relid: Oid,
    spec: NodePtr<'mcx>,
) -> PgResult<NodePtr<'mcx>> {
    sx::transformPartitionBound::call(mcx, pstate, parent_relid, spec)
}

/// `transformPartitionCmd` — analyze the ATTACH/DETACH PARTITION command. The
/// parent relkind dispatch (RELKIND_PARTITIONED_TABLE vs _INDEX vs the error
/// cases) and the actual bound transform read the parent relation from the
/// relcache, which is reached only behind the seam. On a successful ATTACH with
/// a bound, `cxt.partbound` is set. A `None` bound is a no-op (the C
/// `cmd->bound != NULL` guard for the partitioned-table case).
pub fn transformPartitionCmd<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    cmd: &Node<'mcx>,
) -> PgResult<()> {
    let mcx = cxt.mcx;
    let bound = match cmd.node_tag() {
        ntag::T_PartitionCmd => match cmd.expect_partitioncmd().bound.as_deref() {
            Some(b) => Some(mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
            None => None,
        },
        _ => unreachable!("transformPartitionCmd: not a PartitionCmd node: {}", cmd.node_tag()),
    };

    if let Some(bound) = bound {
        cxt.partbound = Some(transformPartitionBound(mcx, &cxt.pstate, cxt.rel_oid, bound)?);
    }
    Ok(())
}
