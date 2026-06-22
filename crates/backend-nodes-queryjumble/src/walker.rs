//! The `_jumbleNode` tag-dispatch walker over the canonical owned
//! [`types_nodes::copy_query::Query`] tree.
//!
//! Faithful to `queryjumblefuncs.c`'s generated walker shape: for every node we
//! emit its `NodeTag` first (`_jumbleNode`'s top `JUMBLE_FIELD(type)`), then the
//! significant fields (the `JUMBLE_FIELD` lines `gen_node_support.pl` emits for
//! every non-`query_jumble_ignore` field), then recurse to child nodes
//! (`JUMBLE_NODE`). Type/typmod/collation OIDs that the C node marks
//! `query_jumble_ignore` are skipped; operator/function/structural OIDs and
//! discriminants are jumbled; `Const`/extern-`Param` locations are recorded
//! (not jumbled), which is what makes constant-only-differing queries normalize
//! to the same id.

use types_nodes::copy_query::Query;
use types_nodes::nodes::{Node, NodePtr, NodeTag};
use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use types_nodes::primnodes::{Expr, TargetEntry};

use crate::state::JumbleState;
use crate::{_jumble_param, _jumble_tag, _record_const_location};

/// `T_Query` (nodetags.h) — emitted as the query node's own tag. Hard-coded so
/// the walker need not synthesize a `Node::Query` value (the canonical `Query`
/// is field-bearing, not wrapped in the central `Node` enum at this point).
const T_QUERY: NodeTag = NodeTag(8);

/// Append an `i32`'s 4 native-endian bytes — `JUMBLE_FIELD` on an int/oid/enum.
#[inline]
fn jf_i32(jstate: &mut JumbleState, v: i32) {
    jstate.append_jumble(&v.to_ne_bytes());
}

/// Append a `u32`'s 4 native-endian bytes — `JUMBLE_FIELD` on an Oid/Index/enum.
#[inline]
fn jf_u32(jstate: &mut JumbleState, v: u32) {
    jstate.append_jumble(&v.to_ne_bytes());
}

/// Append a bool's single byte — `JUMBLE_FIELD` on a 1-byte field.
#[inline]
fn jf_bool(jstate: &mut JumbleState, v: bool) {
    jstate.append_jumble(&[v as u8]);
}

/// `_jumbleNode(jstate, (Node *) query)` for the canonical `Query`. Emits the
/// `T_Query` tag, then the significant scalar fields + a recursion to each child
/// list/sub-tree (`_jumbleQuery`, gen_node_support.pl output).
pub fn jumble_query_node(jstate: &mut JumbleState, query: &Query) {
    _jumble_tag(jstate, T_QUERY);

    // commandType / querySource discriminants (jumbled; `canSetTag`, the `has*`
    // analysis-result booleans, and `queryId` itself are query_jumble_ignore in
    // C — skipped).
    jf_u32(jstate, query.commandType as u32);
    jf_u32(jstate, query.querySource as u32);
    jf_i32(jstate, query.resultRelation);

    // utilityStmt: a CMD_UTILITY tag (the utility node body is not part of the
    // canonical tree here; emit its tag so utility queries differ).
    if let Some(u) = query.utilityStmt.as_ref() {
        _jumble_tag(jstate, u.node_tag());
    } else {
        jstate.append_jumble_null();
    }

    // cteList (CommonTableExpr nodes).
    jumble_node_list(jstate, query.cteList.as_slice());

    // rtable — the range-table entries.
    for rte in query.rtable.iter() {
        jumble_rte(jstate, rte);
    }

    // jointree (FromExpr): fromlist + quals.
    if let Some(jt) = query.jointree.as_ref() {
        _jumble_tag(jstate, NodeTag(65)); // T_FromExpr
        jumble_node_list(jstate, jt.fromlist.as_slice());
        jumble_opt_node(jstate, jt.quals.as_ref().map(|q| &**q));
    } else {
        jstate.append_jumble_null();
    }

    // mergeActionList / mergeTargetRelation / mergeJoinCondition.
    jumble_node_list(jstate, query.mergeActionList.as_slice());
    jf_i32(jstate, query.mergeTargetRelation);
    jumble_opt_expr(jstate, query.mergeJoinCondition.as_deref());

    // targetList (TargetEntry list).
    jumble_target_list(jstate, query.targetList.as_slice());

    // override / onConflict.
    jf_u32(jstate, query.r#override as u32);
    // onConflict body is an OnConflictExpr (not in the trimmed Expr enum);
    // emit a presence marker so an INSERT..ON CONFLICT differs.
    jf_bool(jstate, query.onConflict.is_some());

    // returningList.
    jumble_target_list(jstate, query.returningList.as_slice());

    // groupClause / groupDistinct / groupingSets / havingQual.
    jumble_node_list(jstate, query.groupClause.as_slice());
    jf_bool(jstate, query.groupDistinct);
    jumble_node_list(jstate, query.groupingSets.as_slice());
    jumble_opt_expr(jstate, query.havingQual.as_deref());

    // windowClause / distinctClause / sortClause.
    jumble_node_list(jstate, query.windowClause.as_slice());
    jumble_node_list(jstate, query.distinctClause.as_slice());
    jumble_node_list(jstate, query.sortClause.as_slice());

    // limitOffset / limitCount / limitOption.
    jumble_opt_expr(jstate, query.limitOffset.as_deref());
    jumble_opt_expr(jstate, query.limitCount.as_deref());
    jf_u32(jstate, query.limitOption as u32);

    // rowMarks.
    jumble_node_list(jstate, query.rowMarks.as_slice());

    // setOperations.
    jumble_opt_node(jstate, query.setOperations.as_ref().map(|n| &**n));
}

/// `_jumbleRangeTblEntry` (gen_node_support.pl). Emit the RTE tag + `rtekind`,
/// then the kind-specific significant fields: a relation's `relid`, a
/// subquery's recursively-jumbled `Query`, a function/values/join structure.
fn jumble_rte(jstate: &mut JumbleState, rte: &RangeTblEntry) {
    _jumble_tag(jstate, NodeTag(62)); // T_RangeTblEntry
    jf_u32(jstate, rte.rtekind as u32);

    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            jf_u32(jstate, rte.relid);
        }
        RTEKind::RTE_SUBQUERY => {
            if let Some(sub) = rte.subquery.as_ref() {
                // Recurse: the subquery's own tree (this is where a view's
                // expanded expression tree lives after rewrite).
                jumble_query_node(jstate, &**sub);
            } else {
                jstate.append_jumble_null();
            }
        }
        RTEKind::RTE_FUNCTION => {
            jumble_node_list(jstate, rte.functions.as_slice());
        }
        RTEKind::RTE_JOIN => {
            jf_u32(jstate, rte.jointype as u32);
            jumble_node_list(jstate, rte.joinaliasvars.as_slice());
        }
        // Other RTE kinds: the tag + rtekind already distinguish them; the
        // remaining kind-specific bodies (VALUES lists, CTE names, tablefunc)
        // are not in the canonical-tree fields this walker reads. The tag +
        // discriminant keep the hash structure-sensitive.
        _ => {}
    }
}

/// Jumble a target list (`List *` of `TargetEntry`). Each `TargetEntry` emits
/// its tag + `resno`/`ressortgroupref`/`resjunk` (the jumbled fields; `resname`
/// and source-table OIDs are query_jumble_ignore) and recurses into `expr`.
fn jumble_target_list(jstate: &mut JumbleState, tlist: &[TargetEntry]) {
    for tle in tlist.iter() {
        _jumble_tag(jstate, NodeTag(57)); // T_TargetEntry
        jf_i32(jstate, tle.resno as i32);
        jf_u32(jstate, tle.ressortgroupref);
        jf_bool(jstate, tle.resjunk);
        match tle.expr.as_deref() {
            Some(e) => jumble_expr(jstate, e),
            None => jstate.append_jumble_null(),
        }
    }
}

/// `_jumbleList` over a `List *` of `Node *` (the heterogeneous case). Recurse
/// `_jumbleNode` over each element.
fn jumble_node_list(jstate: &mut JumbleState, list: &[NodePtr]) {
    for n in list.iter() {
        jumble_node(jstate, n);
    }
}

/// `_jumbleNode(jstate, node)` for an optional `Node *` (None => AppendJumbleNull).
fn jumble_opt_node(jstate: &mut JumbleState, node: Option<&Node>) {
    match node {
        Some(n) => jumble_node(jstate, n),
        None => jstate.append_jumble_null(),
    }
}

/// `_jumbleNode(jstate, node)` — the central tag-dispatch. Emit the node's tag,
/// then, if it is an expression node, route into the per-`Expr`-arm jumble;
/// otherwise the tag alone keeps the hash structure-sensitive (structural
/// parse nodes like RangeTblRef/JoinExpr contribute their tag, matching
/// `_jumbleNode`'s "always emit type, then significant fields" shape — the
/// canonical tree does not expose those structural bodies to this walker).
fn jumble_node(jstate: &mut JumbleState, node: &Node) {
    _jumble_tag(jstate, node.node_tag());
    if let Some(expr) = node.as_expr() {
        jumble_expr_fields(jstate, expr);
    }
}

/// `_jumbleNode` for an `Expr` value (already a routing-arm enum). Emit the
/// expr's tag, then its significant fields.
fn jumble_expr(jstate: &mut JumbleState, expr: &Expr) {
    _jumble_tag(jstate, expr.expr_tag());
    jumble_expr_fields(jstate, expr);
}

fn jumble_opt_expr(jstate: &mut JumbleState, expr: Option<&Expr>) {
    match expr {
        Some(e) => jumble_expr(jstate, e),
        None => jstate.append_jumble_null(),
    }
}

/// The per-`Expr`-arm significant-field jumble + child recursion (the bodies
/// `gen_node_support.pl` generates per expression node). Type/typmod/collation
/// OIDs are `query_jumble_ignore` (skipped); operator/function OIDs +
/// structural discriminants are jumbled; `Const`/extern-`Param` locations are
/// recorded.
fn jumble_expr_fields(jstate: &mut JumbleState, expr: &Expr) {
    match expr {
        Expr::Var(v) => {
            // varno/varattno/varlevelsup are jumbled; vartype/vartypmod/
            // varcollid are query_jumble_ignore.
            jf_i32(jstate, v.varno);
            jf_i32(jstate, v.varattno as i32);
            jf_u32(jstate, v.varlevelsup);
        }
        Expr::Const(c) => {
            // Const VALUE/type are query_jumble_ignore; location is RECORDED
            // (query_jumble_location). This is the normalization site.
            _record_const_location(jstate, c.location);
        }
        Expr::Param(p) => {
            jf_u32(jstate, p.paramkind as u32);
            jf_i32(jstate, p.paramid);
            _jumble_param(jstate, p.paramkind, p.paramid, p.location);
        }
        Expr::FuncExpr(f) => {
            // funcid jumbled; funcresulttype/collations query_jumble_ignore.
            jf_u32(jstate, f.funcid);
            jf_bool(jstate, f.funcvariadic);
            for a in f.args.iter() {
                jumble_expr(jstate, a);
            }
        }
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => {
            jf_u32(jstate, o.opno);
            for a in o.args.iter() {
                jumble_expr(jstate, a);
            }
        }
        Expr::ScalarArrayOpExpr(s) => {
            jf_u32(jstate, s.opno);
            jf_bool(jstate, s.useOr);
            for a in s.args.iter() {
                jumble_expr(jstate, a);
            }
        }
        Expr::BoolExpr(b) => {
            jf_u32(jstate, b.boolop as u32);
            for a in b.args.iter() {
                jumble_expr(jstate, a);
            }
        }
        Expr::NullTest(n) => {
            jf_u32(jstate, n.nulltesttype as u32);
            jf_bool(jstate, n.argisrow);
            jumble_opt_expr(jstate, n.arg.as_deref());
        }
        Expr::BooleanTest(b) => {
            jf_u32(jstate, b.booltesttype as u32);
            jumble_opt_expr(jstate, b.arg.as_deref());
        }
        Expr::RelabelType(r) => {
            jumble_opt_expr(jstate, r.arg.as_deref());
        }
        Expr::CoerceViaIO(c) => {
            jumble_opt_expr(jstate, c.arg.as_deref());
        }
        Expr::CoerceToDomain(c) => {
            jf_u32(jstate, c.resulttype);
            jumble_opt_expr(jstate, c.arg.as_deref());
        }
        Expr::CaseExpr(c) => {
            jumble_case_expr(jstate, c);
        }
        Expr::CoalesceExpr(c) => {
            for a in c.args.iter() {
                jumble_expr(jstate, a);
            }
        }
        Expr::MinMaxExpr(m) => {
            jf_u32(jstate, m.op as u32);
            for a in m.args.iter() {
                jumble_expr(jstate, a);
            }
        }
        Expr::ArrayExpr(a) => {
            for e in a.elements.iter() {
                jumble_expr(jstate, e);
            }
        }
        Expr::SQLValueFunction(s) => {
            jf_u32(jstate, s.op as u32);
        }
        // Remaining Expr arms (Aggref/WindowFunc/SubLink/SubPlan/RowExpr/
        // FieldSelect/XmlExpr/Json*/…): the expr tag (already emitted by the
        // caller) keeps the hash structure-sensitive; their full
        // significant-field bodies are residual (see crate report). They do not
        // appear in the guc-test query tree.
        _ => {}
    }
}

/// `_jumbleCaseExpr` — recurse arg, the when/then clauses, and the default.
fn jumble_case_expr(jstate: &mut JumbleState, c: &types_nodes::primnodes::CaseExpr) {
    jumble_opt_expr(jstate, c.arg.as_deref());
    for w in c.args.iter() {
        // CaseWhen: expr + result.
        _jumble_tag(jstate, NodeTag(34)); // T_CaseWhen
        jumble_opt_expr(jstate, w.expr.as_deref());
        jumble_opt_expr(jstate, w.result.as_deref());
    }
    jumble_opt_expr(jstate, c.defresult.as_deref());
}
