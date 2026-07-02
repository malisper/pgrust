//! nodeFuncs.c recursion engine over the opaque `Node`, hosted here until
//! backend-nodes-core lands (only walker/mutator halves; C divergences below).
//!
//! C walkers receive inline-`List` fields and `Query`/`FromExpr` sub-structs
//! as bare `Node *`; this vocabulary stores lists by value and
//! `Query.jointree`/`RangeTblEntry.subquery` by reference, so list fields are
//! walked element-wise (no walker call on the `List` itself) and query-valued
//! refs dispatch through [`NodeWalker::visit_query_ref`]. Identical semantics
//! for every walker that does not special-case `T_List`/`T_FromExpr` at the
//! jointree top; a future walker that does must override the ref hooks.
//!
//! The mutator is identity-preserving: `None` means "unchanged, share the
//! input" where C flat-copies every visited node. Sound because sealed nodes
//! are immutable and the input tree shares the output tree's arena lifetime.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{FromExpr, FuncExpr, OpExpr, TargetEntry};
use types_nodes::{Node, NodeList, NodeTag};

pub const QTW_IGNORE_RT_SUBQUERIES: u32 = 0x01;
pub const QTW_IGNORE_CTE_SUBQUERIES: u32 = 0x02;
pub const QTW_IGNORE_RC_SUBQUERIES: u32 = 0x03;
pub const QTW_IGNORE_JOINALIASES: u32 = 0x04;
pub const QTW_IGNORE_RANGE_TABLE: u32 = 0x08;
pub const QTW_EXAMINE_RTES_BEFORE: u32 = 0x10;
pub const QTW_EXAMINE_RTES_AFTER: u32 = 0x20;
pub const QTW_DONT_COPY_QUERY: u32 = 0x40;
pub const QTW_EXAMINE_SORTGROUP: u32 = 0x80;
pub const QTW_IGNORE_GROUPEXPRS: u32 = 0x100;

#[cold]
#[inline(never)]
pub fn deferred(what: &str, tag: NodeTag) -> ! {
    panic!("optimizer/util deferred arm: {what} ({tag:?}) — node vocabulary unported");
}

pub trait NodeWalker<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool>;

    /// Receives `RangeTblEntry.subquery` (stored as `&Query`, not a `Node`).
    /// Default mirrors expression_tree_walker's `T_Query` no-op; walkers that
    /// descend into subqueries override this together with their `T_Query` arm.
    fn visit_query_ref(&mut self, _q: &'mcx Query<'mcx>) -> PgResult<bool> {
        Ok(false)
    }
}

pub fn walk_list<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    list: &NodeList<'mcx>,
    w: &mut W,
) -> PgResult<bool> {
    for n in list {
        if w.visit(n)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn walk_opt<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    node: Option<Node<'mcx>>,
    w: &mut W,
) -> PgResult<bool> {
    match node {
        Some(n) => w.visit(n),
        None => Ok(false),
    }
}

fn walk_from_expr<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    f: &'mcx FromExpr<'mcx>,
    w: &mut W,
) -> PgResult<bool> {
    Ok(walk_list(&f.fromlist, w)? || walk_opt(f.quals, w)?)
}

pub fn expression_tree_walker<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    node: Node<'mcx>,
    w: &mut W,
) -> PgResult<bool> {
    match node.node_tag() {
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_CoerceToDomainValue
        | NodeTag::T_SetToDefault
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_NextValueExpr
        | NodeTag::T_RangeTblRef
        | NodeTag::T_SortGroupClause
        | NodeTag::T_CTESearchClause
        | NodeTag::T_MergeSupportFunc => Ok(false),
        NodeTag::T_FuncExpr => {
            let f = node.as_variant::<FuncExpr>().unwrap();
            walk_list(&f.args, w)
        }
        NodeTag::T_OpExpr => {
            let o = node.as_variant::<OpExpr>().unwrap();
            walk_list(&o.args, w)
        }
        NodeTag::T_TargetEntry => {
            let te = node.as_variant::<TargetEntry>().unwrap();
            w.visit(te.expr)
        }
        NodeTag::T_FromExpr => {
            let f = node.as_variant::<FromExpr>().unwrap();
            walk_from_expr(f, w)
        }
        NodeTag::T_Query => Ok(false),
        NodeTag::T_List => walk_list(node.as_list().unwrap(), w),
        other => deferred("expression_tree_walker", other),
    }
}

pub fn query_tree_walker<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    query: &'mcx Query<'mcx>,
    w: &mut W,
    flags: u32,
) -> PgResult<bool> {
    if walk_list(&query.targetList, w)?
        || walk_list(&query.withCheckOptions, w)?
        || walk_opt(query.onConflict, w)?
        || walk_list(&query.mergeActionList, w)?
        || walk_opt(query.mergeJoinCondition, w)?
        || walk_list(&query.returningList, w)?
    {
        return Ok(true);
    }
    if let Some(jt) = query.jointree {
        if walk_from_expr(jt, w)? {
            return Ok(true);
        }
    }
    if walk_opt(query.setOperations, w)?
        || walk_opt(query.havingQual, w)?
        || walk_opt(query.limitOffset, w)?
        || walk_opt(query.limitCount, w)?
    {
        return Ok(true);
    }
    if flags & QTW_EXAMINE_SORTGROUP != 0 {
        if walk_list(&query.groupClause, w)?
            || walk_list(&query.windowClause, w)?
            || walk_list(&query.sortClause, w)?
            || walk_list(&query.distinctClause, w)?
        {
            return Ok(true);
        }
    } else if !query.windowClause.is_nil() {
        // C walks each WindowClause's startOffset/endOffset here.
        deferred("query_tree_walker: windowClause offsets", NodeTag::T_WindowClause);
    }
    if flags & QTW_IGNORE_CTE_SUBQUERIES == 0 && walk_list(&query.cteList, w)? {
        return Ok(true);
    }
    if flags & QTW_IGNORE_RANGE_TABLE == 0 && range_table_walker(&query.rtable, w, flags)? {
        return Ok(true);
    }
    Ok(false)
}

pub fn range_table_walker<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    rtable: &NodeList<'mcx>,
    w: &mut W,
    flags: u32,
) -> PgResult<bool> {
    for rte in rtable {
        if range_table_entry_walker(rte, w, flags)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn range_table_entry_walker<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    rte_node: Node<'mcx>,
    w: &mut W,
    flags: u32,
) -> PgResult<bool> {
    let rte: &RangeTblEntry<'mcx> = rte_node
        .as_range_tbl_entry()
        .unwrap_or_else(|| panic!("rtable element is not a RangeTblEntry: {:?}", rte_node));
    if flags & QTW_EXAMINE_RTES_BEFORE != 0 && w.visit(rte_node)? {
        return Ok(true);
    }
    let hit = match rte.rtekind {
        RTEKind::RTE_RELATION => walk_opt(rte.tablesample, w)?,
        RTEKind::RTE_SUBQUERY => {
            if flags & QTW_IGNORE_RT_SUBQUERIES == 0 {
                match rte.subquery {
                    Some(q) => w.visit_query_ref(q)?,
                    None => false,
                }
            } else {
                false
            }
        }
        RTEKind::RTE_JOIN => {
            flags & QTW_IGNORE_JOINALIASES == 0 && walk_list(&rte.joinaliasvars, w)?
        }
        RTEKind::RTE_FUNCTION => walk_list(&rte.functions, w)?,
        RTEKind::RTE_TABLEFUNC => walk_opt(rte.tablefunc, w)?,
        RTEKind::RTE_VALUES => walk_list(&rte.values_lists, w)?,
        RTEKind::RTE_CTE | RTEKind::RTE_NAMEDTUPLESTORE | RTEKind::RTE_RESULT => false,
        RTEKind::RTE_GROUP => {
            flags & QTW_IGNORE_GROUPEXPRS == 0 && walk_list(&rte.groupexprs, w)?
        }
    };
    if hit {
        return Ok(true);
    }
    if walk_list(&rte.securityQuals, w)? {
        return Ok(true);
    }
    if flags & QTW_EXAMINE_RTES_AFTER != 0 && w.visit(rte_node)? {
        return Ok(true);
    }
    Ok(false)
}

pub fn query_or_expression_tree_walker<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    node: Node<'mcx>,
    w: &mut W,
    flags: u32,
) -> PgResult<bool> {
    match node.as_query() {
        Some(q) => query_tree_walker(q, w, flags),
        None => w.visit(node),
    }
}

/// Apply `checker` to every function OID the node itself calls.
pub fn check_functions_in_node<'mcx, F>(node: Node<'mcx>, checker: &mut F) -> PgResult<bool>
where
    F: FnMut(Oid) -> PgResult<bool>,
{
    match node.node_tag() {
        NodeTag::T_FuncExpr => checker(node.as_func_expr().unwrap().funcid),
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            // C set_opfuncid memo-writes into the node; sealed nodes are
            // immutable, so an unset opfuncid re-derives per visit (cold: the
            // parser fills it; only stored-rule trees arrive unset).
            let opfuncid = if o.opfuncid == 0 {
                lsyscache::operator::get_opcode(o.opno)?
            } else {
                o.opfuncid
            };
            checker(opfuncid)
        }
        t @ (NodeTag::T_Aggref
        | NodeTag::T_WindowFunc
        | NodeTag::T_DistinctExpr
        | NodeTag::T_NullIfExpr
        | NodeTag::T_ScalarArrayOpExpr
        | NodeTag::T_CoerceViaIO
        | NodeTag::T_RowCompareExpr) => deferred("check_functions_in_node", t),
        _ => Ok(false),
    }
}

/// Identity-preserving (module doc): `Ok(None)` = unchanged, share input.
pub fn expression_tree_mutator<'mcx, F>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    m: &mut F,
) -> PgResult<Option<Node<'mcx>>>
where
    F: FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
{
    match node.node_tag() {
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_CoerceToDomainValue
        | NodeTag::T_SetToDefault
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_NextValueExpr
        | NodeTag::T_RangeTblRef
        | NodeTag::T_SortGroupClause => Ok(None),
        NodeTag::T_FuncExpr => {
            let f = node.as_variant::<FuncExpr>().unwrap();
            match mutate_list(mcx, &f.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    FuncExpr {
                        funcid: f.funcid,
                        funcresulttype: f.funcresulttype,
                        funcretset: f.funcretset,
                        funcvariadic: f.funcvariadic,
                        funcformat: f.funcformat,
                        funccollid: f.funccollid,
                        inputcollid: f.inputcollid,
                        args,
                        location: f.location,
                    },
                )?)),
            }
        }
        NodeTag::T_OpExpr => {
            let o = node.as_variant::<OpExpr>().unwrap();
            match mutate_list(mcx, &o.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    OpExpr {
                        opno: o.opno,
                        opfuncid: o.opfuncid,
                        opresulttype: o.opresulttype,
                        opretset: o.opretset,
                        opcollid: o.opcollid,
                        inputcollid: o.inputcollid,
                        args,
                        location: o.location,
                    },
                )?)),
            }
        }
        NodeTag::T_TargetEntry => {
            let te = node.as_variant::<TargetEntry>().unwrap();
            match m(te.expr)? {
                None => Ok(None),
                Some(expr) => Ok(Some(Node::mk(
                    mcx,
                    TargetEntry {
                        expr,
                        resno: te.resno,
                        resname: te.resname,
                        ressortgroupref: te.ressortgroupref,
                        resorigtbl: te.resorigtbl,
                        resorigcol: te.resorigcol,
                        resjunk: te.resjunk,
                    },
                )?)),
            }
        }
        NodeTag::T_FromExpr => {
            let f = node.as_variant::<FromExpr>().unwrap();
            let fromlist = mutate_list(mcx, &f.fromlist, m)?;
            let quals = match f.quals {
                Some(q) => m(q)?.map(Some),
                None => None,
            };
            if fromlist.is_none() && quals.is_none() {
                return Ok(None);
            }
            let fromlist = match fromlist {
                Some(l) => l,
                None => f.fromlist.clone_in(mcx)?,
            };
            Ok(Some(Node::mk(
                mcx,
                FromExpr { fromlist, quals: quals.unwrap_or(f.quals) },
            )?))
        }
        NodeTag::T_List => match mutate_list(mcx, node.as_list().unwrap(), m)? {
            None => Ok(None),
            Some(l) => Ok(Some(Node::mk_list(mcx, l)?)),
        },
        other => deferred("expression_tree_mutator", other),
    }
}

/// Element-wise mutate; allocates a new list only after the first change.
pub fn mutate_list<'mcx, F>(
    mcx: Mcx<'mcx>,
    list: &NodeList<'mcx>,
    m: &mut F,
) -> PgResult<Option<NodeList<'mcx>>>
where
    F: FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
{
    let mut out: Option<NodeList<'mcx>> = None;
    for (i, n) in list.iter().enumerate() {
        let replaced = m(n)?;
        if replaced.is_some() && out.is_none() {
            out = Some(list.clone_in(mcx)?);
        }
        if let (Some(new), Some(l)) = (replaced, out.as_mut()) {
            l.as_mut_slice()[i] = new;
        }
    }
    Ok(out)
}
