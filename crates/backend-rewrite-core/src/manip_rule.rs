//! The rule-action node-manipulation primitives of `rewriteManip.c` (and the
//! one jointree-list helper that lives in `rewriteHandler.c`) that the RIR / DML
//! rule engine consumes:
//!
//! * [`CombineRangeTables`] (rewriteManip.c:347) — append one query's RTEs +
//!   `RTEPermissionInfo`s onto another's, fixing `perminfoindex`.
//! * [`AddQual`] (rewriteManip.c:1148) — AND a qualifier into a `Query`'s WHERE.
//! * [`AddInvertedQual`] (rewriteManip.c:1216) — AND the `IS NOT TRUE` inversion
//!   of a qualifier into a `Query`'s WHERE.
//! * [`adjustJoinTreeList`] (rewriteHandler.c:712) — copy a query's jointree
//!   fromlist, optionally dropping a top-level `RangeTblRef` for `rt_index`.
//!
//! `contain_vars_of_level` (named in the task) is NOT ported here: it is an
//! `optimizer/util/var.c` function, already faithfully ported and exported as
//! [`backend_optimizer_util_vars::var::contain_vars_of_level`]. Adding a second
//! copy here would be a duplicate-owner contract divergence; the rule engine
//! calls the var.c owner directly.

#![allow(non_snake_case)]

use backend_nodes_core::makefuncs::make_and_qual;
use backend_utils_error::ereport;
use mcx::{alloc_in, Mcx, PgBox, PgVec};
use types_core::primitive::Index;
use types_error::error::{ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_error::{PgError, PgResult};
use types_nodes::copy_query::Query;
use types_nodes::nodes::{CmdType, Node, NodePtr};
use types_nodes::parsenodes::{RTEPermissionInfo, RangeTblEntry};
use types_nodes::primnodes::{BoolTestType, BooleanTest, Expr};

use crate::walkers::checkExprHasSubLink;

// ===========================================================================
// CombineRangeTables (rewriteManip.c:347)
// ===========================================================================

/// `CombineRangeTables(dst_rtable, dst_perminfos, src_rtable, src_perminfos)`
/// (rewriteManip.c:347) — adds the RTEs of `src_rtable` into `*dst_rtable`, and
/// the `RTEPermissionInfo`s of `src_perminfos` into `*dst_perminfos`, updating
/// `perminfoindex` of the RTEs in `src_rtable` to point at the perminfos'
/// indexes in the combined `*dst_perminfos`.
///
/// In the owned model the four C `List *` arguments are the components of two
/// queries' range-table state. The destination lists are taken by `&mut` and the
/// source lists are taken by value (mirroring `list_concat`, which destructively
/// appends `src` onto `dst`); `src` mutation of `perminfoindex` therefore
/// happens on the owned RTEs before they are pushed.
pub fn CombineRangeTables<'mcx>(
    dst_rtable: &mut PgVec<'mcx, RangeTblEntry<'mcx>>,
    dst_perminfos: &mut PgVec<'mcx, RTEPermissionInfo<'mcx>>,
    mut src_rtable: PgVec<'mcx, RangeTblEntry<'mcx>>,
    src_perminfos: PgVec<'mcx, RTEPermissionInfo<'mcx>>,
) {
    // int offset = list_length(*dst_perminfos);
    let offset = dst_perminfos.len() as Index;

    if offset > 0 {
        // foreach RTE in src_rtable: if perminfoindex > 0 then += offset
        for rte in src_rtable.iter_mut() {
            if rte.perminfoindex > 0 {
                rte.perminfoindex += offset;
            }
        }
    }

    // *dst_perminfos = list_concat(*dst_perminfos, src_perminfos);
    dst_perminfos.extend(src_perminfos);
    // *dst_rtable = list_concat(*dst_rtable, src_rtable);
    dst_rtable.extend(src_rtable);
}

// ===========================================================================
// AddQual (rewriteManip.c:1148)
// ===========================================================================

/// `AddQual(parsetree, qual)` (rewriteManip.c:1148) — add the given qualifier
/// condition to the query's WHERE clause.
///
/// C copies the qual (`copyObject(qual)`); over the owned model the incoming
/// `qual` is borrowed and its analyzed `Expr` is cloned into `mcx`. `qual ==
/// NULL` maps to `None`.
pub fn AddQual<'mcx>(
    parsetree: &mut Query<'mcx>,
    qual: Option<&Node<'mcx>>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // if (qual == NULL) return;
    let Some(qual) = qual else {
        return Ok(());
    };

    if parsetree.commandType == CmdType::CMD_UTILITY {
        /*
         * There's noplace to put the qual on a utility statement.
         *
         * If it's a NOTIFY, silently ignore the qual; extra NOTIFY events are
         * harmless. If it isn't a NOTIFY, error out.
         */
        if matches!(parsetree.utilityStmt.as_deref(), Some(Node::NotifyStmt(_))) {
            return Ok(());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("conditional utility statements are not implemented")
                .into_error());
        }
    }

    if parsetree.setOperations.is_some() {
        /*
         * There's noplace to put the qual on a setop statement, either.
         */
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")
            .into_error());
    }

    /* INTERSECT wants the original, but we need to copy - Jan */
    // copy = copyObject(qual);
    let copy: Node<'mcx> = qual.clone_in(mcx)?;
    // The qual is an ordinary boolean expression node (`Node::Expr`).
    let copy_expr: Expr = match copy {
        Node::Expr(e) => e,
        // Faithful AddQual is only ever handed an expression qual; anything else
        // is an internal error (C would mishandle a non-expr Node likewise).
        _ => return Err(PgError::error("AddQual: qual is not an expression")),
    };

    // parsetree->jointree->quals =
    //     make_and_qual(parsetree->jointree->quals, copy);
    let jointree = parsetree
        .jointree
        .as_deref_mut()
        .ok_or_else(|| PgError::error("AddQual: query has no jointree"))?;

    // Existing quals are carried as Node::Expr; pull out the Expr (or None).
    let existing: Option<Expr> = match jointree.quals.take() {
        None => None,
        Some(n) => match &*n {
            Node::Expr(e) => Some(e.clone()),
            _ => return Err(PgError::error("AddQual: existing quals are not an expression")),
        },
    };

    let combined = make_and_qual(existing, Some(copy_expr));
    jointree.quals = match combined {
        None => None,
        Some(e) => Some(node_box(Node::Expr(e), mcx)?),
    };

    /*
     * We had better not have stuck an aggregate into the WHERE clause.
     * (C: Assert(!contain_aggs_of_level(copy, 0)); release builds skip it.)
     */

    /*
     * Make sure query is marked correctly if added qual has sublinks. Need not
     * search qual when query is already marked.
     */
    if !parsetree.hasSubLinks {
        // Re-borrow the freshly stored copy to probe for SubLinks.
        if let Some(quals) = parsetree
            .jointree
            .as_deref()
            .and_then(|jt| jt.quals.as_deref())
        {
            parsetree.hasSubLinks = checkExprHasSubLink(quals);
        }
    }

    Ok(())
}

// ===========================================================================
// AddInvertedQual (rewriteManip.c:1216)
// ===========================================================================

/// `AddInvertedQual(parsetree, qual)` (rewriteManip.c:1216) — invert the given
/// clause (`x IS NOT TRUE`, not just `NOT x`, so NULL is handled correctly) and
/// add it to the WHERE qualifications of the query.
pub fn AddInvertedQual<'mcx>(
    parsetree: &mut Query<'mcx>,
    qual: Option<&Node<'mcx>>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // if (qual == NULL) return;
    let Some(qual) = qual else {
        return Ok(());
    };

    /* Need not copy input qual, because AddQual will... */
    // invqual->arg = (Expr *) qual; AddQual copies, so clone here too: the
    // arg must be an owned Expr inside the BooleanTest. C aliases the caller's
    // node and relies on AddQual's copyObject; over the owned model we clone the
    // arg expression into the BooleanTest now.
    let arg_expr: Expr = match qual.clone_in(mcx)? {
        Node::Expr(e) => e,
        _ => return Err(PgError::error("AddInvertedQual: qual is not an expression")),
    };

    // invqual = makeNode(BooleanTest);
    // invqual->arg = (Expr *) qual;
    // invqual->booltesttype = IS_NOT_TRUE;
    // invqual->location = -1;
    let invqual = Expr::BooleanTest(BooleanTest {
        arg: Some(alloc::boxed::Box::new(arg_expr)),
        booltesttype: BoolTestType::IS_NOT_TRUE,
        location: -1,
    });

    // AddQual(parsetree, (Node *) invqual);
    let invnode = node_box(Node::Expr(invqual), mcx)?;
    AddQual(parsetree, Some(&invnode), mcx)
}

// ===========================================================================
// adjustJoinTreeList (rewriteHandler.c:712)
// ===========================================================================

/// `adjustJoinTreeList(parsetree, removert, rt_index)` (rewriteHandler.c:712) —
/// copy the query's jointree fromlist, and optionally remove any occurrence of
/// the given `rt_index` as a top-level `RangeTblRef` join item. Returns a
/// freshly copied jointree list sharing no nodes with the original.
pub fn adjustJoinTreeList<'mcx>(
    parsetree: &Query<'mcx>,
    removert: bool,
    rt_index: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    // List *newjointree = copyObject(parsetree->jointree->fromlist);
    let jointree = parsetree
        .jointree
        .as_deref()
        .ok_or_else(|| PgError::error("adjustJoinTreeList: query has no jointree"))?;

    let mut newjointree: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    for item in jointree.fromlist.iter() {
        newjointree.push(node_box(item.clone_in(mcx)?, mcx)?);
    }

    if removert {
        // foreach(l, newjointree): if IsA(rtr, RangeTblRef) && rtr->rtindex ==
        //   rt_index: foreach_delete_current(newjointree, l); break;
        let mut found: Option<usize> = None;
        for (i, item) in newjointree.iter().enumerate() {
            if let Node::RangeTblRef(rtr) = &**item {
                if rtr.rtindex == rt_index {
                    found = Some(i);
                    break;
                }
            }
        }
        if let Some(i) = found {
            newjointree.remove(i);
        }
    }

    Ok(newjointree)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Allocate a [`Node`] into `mcx` as a [`NodePtr`] (`PgBox<Node>`).
fn node_box<'mcx>(node: Node<'mcx>, mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    alloc_in(mcx, node)
}
