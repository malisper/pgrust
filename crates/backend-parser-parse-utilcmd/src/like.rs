//! LIKE / OF-type helpers (`parse_utilcmd.c`).
//!
//! These leaves are entirely relcache / catalog bound: `transformTableLikeClause`
//! reads a source relation's `TupleDesc`, defaults, constraints, identity,
//! storage, compression and comments; `transformOfType` reads a composite type's
//! rowtype. None of that machinery is reachable from this crate, so the
//! catalog-resident work is routed through the outward seams; the in-crate
//! wrappers marshal the `CreateStmtContext` accumulators around the seam.

use types_error::PgResult;

use types_nodes::nodes::Node;

use backend_parser_parse_utilcmd_outward_seams as sx;

use crate::core::{CreateStmtContext, NodePtr};

/// `transformTableLikeClause` — expand `LIKE <srctable>` into recreated column
/// definitions, routing the relcache reads through the seam and folding the
/// generated columns / check constraints / alist statements / deferred
/// LIKE-postprocessing back into the context.
pub fn transformTableLikeClause<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    table_like_clause: NodePtr<'mcx>,
) -> PgResult<()> {
    let mcx = cxt.mcx;

    if !matches!(table_like_clause.as_ref(), Node::TableLikeClause(_)) {
        unreachable!(
            "transformTableLikeClause: not a TableLikeClause node: {}",
            table_like_clause.node_tag()
        );
    }

    let relation = match cxt.relation.as_deref() {
        Some(n) => mcx::alloc_in(mcx, n.clone_in(mcx)?)?,
        None => {
            return Err(types_error::PgError::error(
                "transformTableLikeClause: requires cxt.relation",
            ))
        }
    };

    let (columns, ckconstraints, alist, like_postproc) = sx::transformTableLikeClause::call(
        mcx,
        &cxt.pstate,
        relation,
        table_like_clause,
        cxt.isforeign,
    )?;

    cxt.columns.extend(columns);
    cxt.ckconstraints.extend(ckconstraints);
    cxt.alist.extend(alist);
    cxt.likeclauses.extend(like_postproc);
    Ok(())
}

/// `transformOfType` — expand an `OF typename` clause into inherited column
/// definitions (composite-type rowtype read through the seam).
pub fn transformOfType<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    of_typename: NodePtr<'mcx>,
) -> PgResult<()> {
    let mcx = cxt.mcx;
    let columns = sx::transformOfType::call(mcx, &cxt.pstate, of_typename)?;
    cxt.columns.extend(columns);
    Ok(())
}
