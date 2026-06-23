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

use alloc::string::ToString;

use ::types_core::Oid;
use ::types_error::{PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, ERROR};

use ::nodes::nodes::{ntag, Node};
use ::nodes::parsestmt::ParseState;

use ::types_tuple::access::{
    RELKIND_INDEX, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};

use ::lsyscache::relation::get_rel_name;
use ::lsyscache_seams::get_rel_relkind;
use ::utils_error::ereport;

use parse_utilcmd_outward_seams as sx;

use crate::core::{CreateStmtContext, NodePtr};

/// `transformPartitionBound` — transform a partition `FOR VALUES` bound. The
/// `PartitionBoundSpec` parsenode and the parent's `PartitionKey` are reached
/// through the seam; the bound and the result are carried as the bound `Node`.
pub fn transformPartitionBound<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
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

    // switch (parentRel->rd_rel->relkind) — the parent relation is already open
    // and locked, so reading the relkind through the syscache is consistent.
    let relkind = get_rel_relkind::call(cxt.rel_oid)?;
    match relkind {
        RELKIND_PARTITIONED_TABLE => {
            // transform the partition bound, if any
            if let Some(bound) = bound {
                cxt.partbound =
                    Some(transformPartitionBound(mcx, &mut cxt.pstate, cxt.rel_oid, bound)?);
                // NB: `&mut cxt.pstate` deref-coerces `PgBox<ParseState>` to
                // `&mut ParseState`.
            }
            Ok(())
        }
        RELKIND_PARTITIONED_INDEX => {
            // A partitioned index cannot have a partition bound set. ALTER INDEX
            // prevents that with its grammar, but not ALTER TABLE.
            if bound.is_some() {
                return partition_cmd_error(mcx, cxt.rel_oid, "is not a partitioned table");
            }
            Ok(())
        }
        RELKIND_RELATION => {
            // the table must be partitioned
            partition_cmd_error(mcx, cxt.rel_oid, "is not partitioned table")
        }
        RELKIND_INDEX => {
            // the index must be partitioned
            partition_cmd_error(mcx, cxt.rel_oid, "is not partitioned index")
        }
        _ => {
            // parser shouldn't let this case through
            Err(ereport(ERROR)
                .errmsg_internal(alloc::format!(
                    "\"{}\" is not a partitioned table or index",
                    rel_name_or_empty(mcx, cxt.rel_oid)?
                ))
                .into_error())
        }
    }
}

/// Build the `errcode(ERRCODE_INVALID_OBJECT_DEFINITION)` errors raised by the
/// relkind dispatch in `transformPartitionCmd`. `kind` selects the message form:
/// `"is not a partitioned table"` (partitioned index), `"is not partitioned
/// table"` (plain relation → `table "%s" is not partitioned`), or `"is not
/// partitioned index"` (plain index → `index "%s" is not partitioned`).
fn partition_cmd_error<'mcx>(mcx: mcx::Mcx<'mcx>, relid: Oid, kind: &str) -> PgResult<()> {
    let relname = rel_name_or_empty(mcx, relid)?;
    let msg = match kind {
        "is not partitioned table" => alloc::format!("table \"{relname}\" is not partitioned"),
        "is not partitioned index" => alloc::format!("index \"{relname}\" is not partitioned"),
        _ => alloc::format!("\"{relname}\" is not a partitioned table"),
    };
    Err(ereport(ERROR)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(msg)
        .into_error())
}

/// `RelationGetRelationName(parentRel)` — the parent relation's name.
fn rel_name_or_empty<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    relid: Oid,
) -> PgResult<alloc::string::String> {
    Ok(get_rel_name(mcx, relid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default())
}
