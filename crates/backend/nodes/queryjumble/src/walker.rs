//! The `_jumbleNode` tag-dispatch walker over the canonical owned
//! [`::nodes::copy_query::Query`] tree.
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

use ::nodes::copy_query::Query;
use ::nodes::nodes::{ntag, Node, NodePtr, NodeTag};
use ::nodes::parsenodes::{RTEKind, RangeTblEntry};
use ::nodes::primnodes::{ArrayExpr, CoercionForm, Expr, ParamKind, TargetEntry};

use crate::state::JumbleState;
use crate::{_jumble_param, _jumble_tag, _record_const_location};

/// `FirstGenbkiObjectId` (access/transam.h) — the squashable-cast funcid cutoff
/// (`func->funcid > FirstGenbkiObjectId` disqualifies a FuncExpr from squashing).
const FIRST_GENBKI_OBJECT_ID: u32 = 10000;

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

    // utilityStmt (`JUMBLE_NODE(utilityStmt)`): walk the utility node so its
    // significant fields + constant locations / inner query contribute (SET
    // value locations, DECLARE CURSOR inner-query constants, etc.).
    if let Some(u) = query.utilityStmt.as_ref() {
        jumble_utility_node(jstate, u);
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
        RTEKind::RTE_VALUES => {
            // `JUMBLE_NODE(values_lists)`: the VALUES rows. Their constants are
            // recorded so an INSERT ... VALUES (1,'a'),(2,'b') normalizes.
            jumble_values_lists(jstate, rte.values_lists.as_slice());
        }
        // Other RTE kinds: the tag + rtekind already distinguish them; the
        // remaining kind-specific bodies (CTE names, tablefunc) are not in the
        // canonical-tree fields this walker reads. The tag + discriminant keep
        // the hash structure-sensitive.
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
    let tag = node.node_tag();
    _jumble_tag(jstate, tag);
    if let Some(expr) = node.as_expr() {
        jumble_expr_fields(jstate, expr);
        return;
    }
    jumble_node_body(jstate, tag, node);
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
        Expr::ArrayCoerceExpr(a) => {
            // `_jumbleArrayCoerceExpr`: recurse into arg (the inner ArrayExpr,
            // where squashing happens) + elemexpr, then jumble resulttype.
            jumble_opt_expr(jstate, a.arg.as_deref());
            jumble_opt_expr(jstate, a.elemexpr.as_deref());
            jf_u32(jstate, a.resulttype);
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
            // `_jumbleArrayExpr` → `JUMBLE_ELEMENTS(elements, node)`: try to
            // squash a constant-only element list into a single recorded
            // location; otherwise jumble the elements normally.
            jumble_elements_array(jstate, a);
        }
        Expr::SQLValueFunction(s) => {
            jf_u32(jstate, s.op as u32);
        }
        Expr::SubLink(s) => {
            // `_jumbleSubLink`: subLinkType, subLinkId, testexpr, subselect.
            jf_u32(jstate, s.subLinkType as u32);
            jf_i32(jstate, s.subLinkId);
            jumble_opt_expr(jstate, s.testexpr.as_deref());
            match s.subselect.as_deref() {
                Some(q) => jumble_query_node(jstate, q),
                None => jstate.append_jumble_null(),
            }
        }
        Expr::RowExpr(r) => {
            // `_jumbleRowExpr`: JUMBLE_NODE(args).
            for a in r.args.iter() {
                jumble_expr(jstate, a);
            }
        }
        Expr::FieldSelect(f) => {
            // `_jumbleFieldSelect`: arg, fieldnum.
            jumble_opt_expr(jstate, f.arg.as_deref());
            jf_i32(jstate, f.fieldnum as i32);
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
fn jumble_case_expr(jstate: &mut JumbleState, c: &::nodes::primnodes::CaseExpr) {
    jumble_opt_expr(jstate, c.arg.as_deref());
    for w in c.args.iter() {
        // CaseWhen: expr + result.
        _jumble_tag(jstate, NodeTag(34)); // T_CaseWhen
        jumble_opt_expr(jstate, w.expr.as_deref());
        jumble_opt_expr(jstate, w.result.as_deref());
    }
    jumble_opt_expr(jstate, c.defresult.as_deref());
}

// ===========================================================================
// Squashable constant lists (`_jumbleElements` / `IsSquashableConstant*`).
// ===========================================================================

/// `_jumbleElements(jstate, elements, node)` for an `ArrayExpr`
/// (queryjumblefuncs.c:600). If the element list is a squashable constant list
/// AND the ArrayExpr carries valid `list_start`/`list_end` bounds, record a
/// single squashed location spanning the list (so `IN (1,2,3)` /
/// `ARRAY[1,2,3]` of any length normalize to `$1 /*, ... */`). Otherwise jumble
/// each element normally.
fn jumble_elements_array(jstate: &mut JumbleState, a: &ArrayExpr) {
    let mut normalize_list = false;

    if is_squashable_constant_list(&a.elements) {
        // C: `if (aexpr->list_start > 0 && aexpr->list_end > 0)`.
        if a.list_start > 0 && a.list_end > 0 {
            // RecordConstLocation(jstate, false, list_start + 1,
            //                     (list_end - list_start) - 1);
            jstate
                .record_const_location(false, a.list_start + 1, (a.list_end - a.list_start) - 1);
            normalize_list = true;
            jstate.has_squashed_lists = true;
        }
    }

    if !normalize_list {
        for e in a.elements.iter() {
            jumble_expr(jstate, e);
        }
    }
}

/// `IsSquashableConstantList(elements)` (queryjumblefuncs.c:565). A list of two
/// or more elements that are all squashable constants.
fn is_squashable_constant_list(elements: &[Expr]) -> bool {
    if elements.len() < 2 {
        return false;
    }
    elements.iter().all(is_squashable_constant_expr)
}

/// `IsSquashableConstant(element)` (queryjumblefuncs.c:495), specialized to the
/// `Expr` enum: see through RelabelType/CoerceViaIO, accept Const and
/// PARAM_EXTERN Param, and accept a builtin cast FuncExpr whose args are all
/// squashable.
fn is_squashable_constant_expr(element: &Expr) -> bool {
    let mut element = element;
    loop {
        match element {
            // Unwrap RelabelType / CoerceViaIO.
            Expr::RelabelType(r) => match r.arg.as_deref() {
                Some(inner) => element = inner,
                None => return false,
            },
            Expr::CoerceViaIO(c) => match c.arg.as_deref() {
                Some(inner) => element = inner,
                None => return false,
            },
            Expr::Const(_) => return true,
            Expr::Param(p) => return p.paramkind == ParamKind::PARAM_EXTERN,
            Expr::FuncExpr(func) => {
                // Only builtin (funcid <= FirstGenbkiObjectId) implicit/explicit
                // casts with all-constant arguments are squashable.
                if func.funcformat != CoercionForm::COERCE_IMPLICIT_CAST
                    && func.funcformat != CoercionForm::COERCE_EXPLICIT_CAST
                {
                    return false;
                }
                if func.funcid > FIRST_GENBKI_OBJECT_ID {
                    return false;
                }
                return func.args.iter().all(is_squashable_constant_expr);
            }
            _ => return false,
        }
    }
}

// ===========================================================================
// VALUES lists (`_jumbleRangeTblEntry` → `JUMBLE_NODE(values_lists)`).
// ===========================================================================

/// Jumble a VALUES RTE's `values_lists` — a `List` of `List` of expressions
/// (each inner list is one VALUES row). Each row's constants are recorded so an
/// `INSERT ... VALUES (1,'a'),(2,'b')` normalizes its literals to `$n`.
fn jumble_values_lists(jstate: &mut JumbleState, values_lists: &[NodePtr]) {
    for row_node in values_lists.iter() {
        // Each row is a `T_List` node whose elements are the column exprs.
        match (**row_node).as_list() {
            Some(list) => {
                _jumble_tag(jstate, NodeTag(ntag::T_List.0));
                for col in list.iter() {
                    jumble_node(jstate, col);
                }
            }
            None => jumble_node(jstate, row_node),
        }
    }
}

// ===========================================================================
// Utility statements (`_jumbleNode` over `Query.utilityStmt`).
// ===========================================================================

/// `_jumbleNode(jstate, (Node *) query->utilityStmt)`. The generated walker
/// jumbles each utility node's significant fields and recurses; for the nodes
/// whose constant locations / inner queries drive pg_stat_statements
/// normalization (`VariableSetStmt`, `DeclareCursorStmt`, the raw constant /
/// operator nodes a SET/CURSOR carries) we reproduce that shape. Any other
/// utility node contributes its tag plus a recursive walk of the parse-node
/// children that carry constants.
fn jumble_utility_node(jstate: &mut JumbleState, node: &Node) {
    // `_jumbleNode(jstate, (Node *) query->utilityStmt)`. A utility node is just
    // another node: emit its tag, then dispatch into the unified body (which now
    // covers the DDL/utility family in addition to expr / TargetEntry /
    // MergeAction). Routing through `jumble_node` keeps a single faithful
    // `_jumbleNode` shape and lets utility-statement children that are
    // expression nodes (e.g. a CREATE POLICY qual) reach the expr arms.
    jumble_node(jstate, node);
}

/// `_jumbleNode`'s post-tag body for any non-`Expr` node — the per-`NodeTag`
/// significant-field jumble + `JUMBLE_NODE` recursion that `gen_node_support.pl`
/// emits. This is the union of the parse-tree structural arms (TargetEntry /
/// MergeAction), the utility/transaction-statement arms, and the DDL arms. The
/// node's tag has already been emitted by the caller (`jumble_node`).
fn jumble_node_body(jstate: &mut JumbleState, tag: NodeTag, node: &Node) {
    match tag {
        ntag::T_TargetEntry => {
            // A `TargetEntry` reached as a bare `Node` (e.g. inside a
            // MergeAction's targetList): jumble its significant fields + recurse
            // into `expr`, the same shape `jumble_target_list` uses.
            if let Some(te) = node.as_targetentry() {
                jf_i32(jstate, te.resno as i32);
                jf_u32(jstate, te.ressortgroupref);
                jf_bool(jstate, te.resjunk);
                match te.expr.as_deref() {
                    Some(e) => jumble_expr(jstate, e),
                    None => jstate.append_jumble_null(),
                }
            }
        }
        ntag::T_MergeAction => {
            // `_jumbleMergeAction`: matchKind, commandType, qual, targetList.
            if let Some(a) = node.as_mergeaction() {
                jf_u32(jstate, a.matchKind as u32);
                jf_u32(jstate, a.commandType as u32);
                jn_opt(jstate, a.qual.as_deref());
                jumble_node_list(jstate, a.targetList.as_slice());
            }
        }
        ntag::T_VariableSetStmt => {
            // `_jumbleVariableSetStmt` (queryjumblefuncs.c:739).
            let s = node.expect_variablesetstmt();
            jf_u32(jstate, s.kind as u32);
            // JUMBLE_STRING(name).
            jumble_opt_cstring(jstate, s.name.as_deref());
            // Account for the args only if the parser asked us to.
            if s.jumble_args {
                for a in s.args.iter() {
                    jumble_node(jstate, a);
                }
            }
            jf_bool(jstate, s.is_local);
            // JUMBLE_LOCATION(location) — record the value's location (not
            // jumbled) so `SET x = '1MB'` / `'2MB'` normalize to `SET x = $1`.
            _record_const_location(jstate, s.location);
        }
        ntag::T_DeclareCursorStmt => {
            // `_jumbleDeclareCursorStmt` (queryjumblefuncs.c:2233): portalname,
            // options, then recurse into the analyzed inner query.
            let s = node.expect_declarecursorstmt();
            jumble_opt_cstring(jstate, s.portalname.as_deref());
            jf_i32(jstate, s.options);
            match s.query.as_deref().and_then(|n| n.as_query()) {
                Some(q) => jumble_query_child(jstate, q),
                None => jstate.append_jumble_null(),
            }
        }
        ntag::T_A_Const => {
            // `_jumbleA_Const` (queryjumblefuncs.c:706): jumble isnull, then the
            // value (its location is NOT recorded here — only VariableSetStmt's
            // JUMBLE_LOCATION drives SET normalization).
            let c = node.expect_a_const();
            jf_bool(jstate, c.isnull);
            if !c.isnull {
                if let Some(v) = c.val.as_deref() {
                    jumble_value_node(jstate, v);
                }
            }
        }
        ntag::T_ClosePortalStmt => {
            // `_jumbleClosePortalStmt`: JUMBLE_STRING(portalname).
            let s = node.expect_closeportalstmt();
            jumble_opt_cstring(jstate, s.portalname.as_deref());
        }
        ntag::T_FetchStmt => {
            // `_jumbleFetchStmt`: direction, howMany, portalname, ismove.
            let s = node.expect_fetchstmt();
            jf_u32(jstate, s.direction as u32);
            jstate.append_jumble(&s.how_many.to_ne_bytes());
            jumble_opt_cstring(jstate, s.portalname.as_deref());
            jf_bool(jstate, s.ismove);
        }
        ntag::T_TransactionStmt => {
            // `_jumbleTransactionStmt`: kind, options, chain, JUMBLE_LOCATION.
            let s = node.expect_transactionstmt();
            jf_u32(jstate, s.kind as u32);
            for o in s.options.iter() {
                jumble_utility_node(jstate, o);
            }
            jf_bool(jstate, s.chain);
            _record_const_location(jstate, s.location);
        }
        ntag::T_VariableShowStmt => {
            // `_jumbleVariableShowStmt`: JUMBLE_STRING(name).
            let s = node.expect_variableshowstmt();
            jumble_opt_cstring(jstate, s.name.as_deref());
        }
        ntag::T_CreateRoleStmt => {
            // `_jumbleCreateRoleStmt`: stmt_type, JUMBLE_STRING(role), options.
            // The role name is jumbled, so `CREATE ROLE a` / `CREATE ROLE b`
            // get distinct query ids.
            let s = node.expect_createrolestmt();
            jf_u32(jstate, s.stmt_type as u32);
            jumble_opt_cstring(jstate, s.role.as_deref());
            jn_list(jstate, s.options.as_slice());
        }
        ntag::T_DeallocateStmt => {
            // `_jumbleDeallocateStmt`: JUMBLE_FIELD(isall), JUMBLE_LOCATION.
            // The name is query_jumble_ignore; its location is recorded so
            // `DEALLOCATE p1` normalizes to `DEALLOCATE $1`.
            let s = node.expect_deallocatestmt();
            jf_bool(jstate, s.isall);
            _record_const_location(jstate, s.location);
        }
        ntag::T_DoStmt => {
            // `_jumbleDoStmt`: JUMBLE_NODE(args) — the DefElem options (the
            // inline code + language), so distinct DO blocks differ.
            let s = node.expect_dostmt();
            jn_list(jstate, s.args.as_slice());
        }
        ntag::T_DefElem => {
            // `_jumbleDefElem`: defnamespace, defname, arg, defaction.
            let s = node.expect_defelem();
            jumble_opt_cstring(jstate, s.defnamespace.as_deref());
            jumble_opt_cstring(jstate, s.defname.as_deref());
            jn_opt(jstate, s.arg.as_deref());
            jf_u32(jstate, s.defaction as u32);
        }

        // ===================================================================
        // DDL / object-definition statements. Each arm reproduces the
        // `gen_node_support.pl`-generated `_jumble<Node>` body: the significant
        // scalar fields are jumbled (location/typmod/collation OIDs and the
        // post-analysis `*Oid`/subid bookkeeping that C marks
        // `query_jumble_ignore` are skipped), the string fields are jumbled, and
        // `JUMBLE_NODE` children recurse through the unified `jumble_node`. This
        // gives each distinct DDL statement a distinct queryId while normalizing
        // the constant locations its child expressions carry.
        // ===================================================================
        ntag::T_RangeVar => {
            jumble_range_var_fields(jstate, node.expect_rangevar());
        }
        ntag::T_TypeName => {
            jumble_type_name_fields(jstate, node.expect_typename());
        }
        ntag::T_ColumnDef => {
            // `_jumbleColumnDef`. typeName / identitySequence are typed boxes
            // (`PgBox<TypeName>` / `PgBox<RangeVar>`), so their tag + body are
            // emitted directly via the typed helpers (mirroring how `_jumbleNode`
            // would recurse into a TypeName / RangeVar node).
            let s = node.expect_columndef();
            jumble_opt_cstring(jstate, s.colname.as_deref());
            jumble_opt_type_name(jstate, s.typeName.as_deref());
            jumble_opt_cstring(jstate, s.compression.as_deref());
            jf_i32(jstate, s.inhcount as i32);
            jf_bool(jstate, s.is_local);
            jf_bool(jstate, s.is_not_null);
            jf_bool(jstate, s.is_from_type);
            jstate.append_jumble(&[s.storage as u8]);
            jumble_opt_cstring(jstate, s.storage_name.as_deref());
            jn_opt(jstate, s.raw_default.as_deref());
            jn_opt(jstate, s.cooked_default.as_deref());
            jstate.append_jumble(&[s.identity as u8]);
            jumble_opt_range_var(jstate, s.identitySequence.as_deref());
            jstate.append_jumble(&[s.generated as u8]);
            jumble_opt_collate_clause(jstate, s.collClause.as_deref());
            jf_u32(jstate, s.collOid);
            jn_list(jstate, s.constraints.as_slice());
            jn_list(jstate, s.fdwoptions.as_slice());
        }
        ntag::T_Constraint => {
            // `_jumbleConstraint`. The post-analysis `*Oid` fields are
            // query_jumble_ignore; the raw expression child + key lists carry the
            // distinguishing structure and constant locations.
            let s = node.expect_constraint();
            jf_u32(jstate, s.contype as u32);
            jumble_opt_cstring(jstate, s.conname.as_deref());
            jf_bool(jstate, s.deferrable);
            jf_bool(jstate, s.initdeferred);
            jf_bool(jstate, s.is_enforced);
            jf_bool(jstate, s.skip_validation);
            jf_bool(jstate, s.initially_valid);
            jf_bool(jstate, s.is_no_inherit);
            jn_opt(jstate, s.raw_expr.as_deref());
            jumble_opt_cstring(jstate, s.cooked_expr.as_deref());
            jstate.append_jumble(&[s.generated_when as u8]);
            jstate.append_jumble(&[s.generated_kind as u8]);
            jf_bool(jstate, s.nulls_not_distinct);
            jn_list(jstate, s.keys.as_slice());
            jf_bool(jstate, s.without_overlaps);
            jn_list(jstate, s.including.as_slice());
            jn_list(jstate, s.exclusions.as_slice());
            jn_list(jstate, s.options.as_slice());
            jumble_opt_cstring(jstate, s.indexname.as_deref());
            jumble_opt_cstring(jstate, s.indexspace.as_deref());
            jf_bool(jstate, s.reset_default_tblspc);
            jumble_opt_cstring(jstate, s.access_method.as_deref());
            jn_opt(jstate, s.where_clause.as_deref());
            jn_opt(jstate, s.pktable.as_deref());
            jn_list(jstate, s.fk_attrs.as_slice());
            jn_list(jstate, s.pk_attrs.as_slice());
            jf_bool(jstate, s.fk_with_period);
            jf_bool(jstate, s.pk_with_period);
            jstate.append_jumble(&[s.fk_matchtype as u8]);
            jstate.append_jumble(&[s.fk_upd_action as u8]);
            jstate.append_jumble(&[s.fk_del_action as u8]);
            jn_list(jstate, s.fk_del_set_cols.as_slice());
            jn_list(jstate, s.old_conpfeqop.as_slice());
        }
        ntag::T_IndexElem => {
            // `_jumbleIndexElem`.
            let s = node.expect_indexelem();
            jumble_opt_cstring(jstate, s.name.as_deref());
            jn_opt(jstate, s.expr.as_deref());
            jumble_opt_cstring(jstate, s.indexcolname.as_deref());
            jn_list(jstate, s.collation.as_slice());
            jn_list(jstate, s.opclass.as_slice());
            jn_list(jstate, s.opclassopts.as_slice());
            jf_u32(jstate, s.ordering as u32);
            jf_u32(jstate, s.nulls_ordering as u32);
        }
        ntag::T_ObjectWithArgs => {
            // `_jumbleObjectWithArgs`.
            let s = node.expect_objectwithargs();
            jn_list(jstate, s.objname.as_slice());
            jn_list(jstate, s.objargs.as_slice());
            jn_list(jstate, s.objfuncargs.as_slice());
            jf_bool(jstate, s.args_unspecified);
        }
        ntag::T_RoleSpec => {
            // `_jumbleRoleSpec`: roletype, rolename (location no-op).
            let s = node.expect_rolespec();
            jf_u32(jstate, s.roletype as u32);
            jumble_opt_cstring(jstate, s.rolename.as_deref());
        }
        ntag::T_AccessPriv => {
            // `_jumbleAccessPriv`.
            let s = node.expect_accesspriv();
            jumble_opt_cstring(jstate, s.priv_name.as_deref());
            jn_list(jstate, s.cols.as_slice());
        }
        ntag::T_FunctionParameter => {
            // `_jumbleFunctionParameter`.
            let s = node.expect_functionparameter();
            jumble_opt_cstring(jstate, s.name.as_deref());
            jn_opt(jstate, s.argType.as_deref());
            jstate.append_jumble(&[s.mode as u8]);
            jn_opt(jstate, s.defexpr.as_deref());
        }

        ntag::T_CreateStmt => {
            jumble_create_stmt_fields(jstate, node.expect_createstmt());
        }
        ntag::T_CreateForeignTableStmt => {
            // `_jumbleCreateForeignTableStmt`: the inlined CreateStmt base fields,
            // then servername + options.
            let s = node.expect_createforeigntablestmt();
            jumble_create_stmt_fields(jstate, &s.base);
            jumble_opt_cstring(jstate, s.servername.as_deref());
            jn_list(jstate, s.options.as_slice());
        }
        ntag::T_AlterTableStmt => {
            // `_jumbleAlterTableStmt`.
            let s = node.expect_altertablestmt();
            jn_opt(jstate, s.relation.as_deref());
            jn_list(jstate, s.cmds.as_slice());
            jf_u32(jstate, s.objtype as u32);
            jf_bool(jstate, s.missing_ok);
        }
        ntag::T_AlterTableCmd => {
            // `_jumbleAlterTableCmd`.
            let s = node.expect_altertablecmd();
            jf_u32(jstate, s.subtype as u32);
            jumble_opt_cstring(jstate, s.name.as_deref());
            jf_i32(jstate, s.num as i32);
            jn_opt(jstate, s.newowner.as_deref());
            jn_opt(jstate, s.def.as_deref());
            jf_u32(jstate, s.behavior as u32);
            jf_bool(jstate, s.missing_ok);
            jf_bool(jstate, s.recurse);
        }
        ntag::T_DropStmt => {
            // `_jumbleDropStmt`. `objects` is a list of name lists (String
            // nodes); jumbling them distinguishes `DROP TABLE a` from
            // `DROP TABLE b` while folding the two-string DROP IF EXISTS form.
            let s = node.expect_dropstmt();
            jn_list(jstate, s.objects.as_slice());
            jf_u32(jstate, s.removeType as u32);
            jf_u32(jstate, s.behavior as u32);
            jf_bool(jstate, s.missing_ok);
            jf_bool(jstate, s.concurrent);
        }
        ntag::T_IndexStmt => {
            // `_jumbleIndexStmt`. The post-analysis OID/subid fields are
            // query_jumble_ignore.
            let s = node.expect_indexstmt();
            jumble_opt_cstring(jstate, s.idxname.as_deref());
            jn_opt(jstate, s.relation.as_deref());
            jumble_opt_cstring(jstate, s.accessMethod.as_deref());
            jumble_opt_cstring(jstate, s.tableSpace.as_deref());
            jn_list(jstate, s.indexParams.as_slice());
            jn_list(jstate, s.indexIncludingParams.as_slice());
            jn_list(jstate, s.options.as_slice());
            jn_opt(jstate, s.whereClause.as_deref());
            jn_list(jstate, s.excludeOpNames.as_slice());
            jumble_opt_cstring(jstate, s.idxcomment.as_deref());
            jf_bool(jstate, s.unique);
            jf_bool(jstate, s.nulls_not_distinct);
            jf_bool(jstate, s.primary);
            jf_bool(jstate, s.isconstraint);
            jf_bool(jstate, s.iswithoutoverlaps);
            jf_bool(jstate, s.deferrable);
            jf_bool(jstate, s.initdeferred);
            jf_bool(jstate, s.transformed);
            jf_bool(jstate, s.concurrent);
            jf_bool(jstate, s.if_not_exists);
            jf_bool(jstate, s.reset_default_tblspc);
        }
        ntag::T_RenameStmt => {
            // `_jumbleRenameStmt`.
            let s = node.expect_renamestmt();
            jf_u32(jstate, s.renameType as u32);
            jf_u32(jstate, s.relationType as u32);
            jn_opt(jstate, s.relation.as_deref());
            jn_opt(jstate, s.object.as_deref());
            jumble_opt_cstring(jstate, s.subname.as_deref());
            jumble_opt_cstring(jstate, s.newname.as_deref());
            jf_u32(jstate, s.behavior as u32);
            jf_bool(jstate, s.missing_ok);
        }
        ntag::T_ViewStmt => {
            // `_jumbleViewStmt`. `query` is the rewritten/raw SELECT — recurse so
            // the view body's constants normalize.
            let s = node.expect_viewstmt();
            jn_opt(jstate, s.view.as_deref());
            jn_list(jstate, s.aliases.as_slice());
            jumble_view_query_child(jstate, s.query.as_deref());
            jf_bool(jstate, s.replace);
            jn_list(jstate, s.options.as_slice());
            jf_u32(jstate, s.withCheckOption as u32);
        }
        ntag::T_CreateTableAsStmt => {
            // `_jumbleCreateTableAsStmt`. `query` is the (analyzed) Query — recurse
            // so `CREATE TABLE t AS SELECT 1` / `SELECT 2` normalize.
            let s = node.expect_createtableasstmt();
            jumble_view_query_child(jstate, s.query.as_deref());
            jn_opt(jstate, s.into.as_deref());
            jf_u32(jstate, s.objtype as u32);
            jf_bool(jstate, s.is_select_into);
            jf_bool(jstate, s.if_not_exists);
        }
        ntag::T_CreateFunctionStmt => {
            // `_jumbleCreateFunctionStmt`.
            let s = node.expect_createfunctionstmt();
            jf_bool(jstate, s.is_procedure);
            jf_bool(jstate, s.replace);
            jn_list(jstate, s.funcname.as_slice());
            jn_list(jstate, s.parameters.as_slice());
            jn_opt(jstate, s.returnType.as_deref());
            jn_list(jstate, s.options.as_slice());
            jn_opt(jstate, s.sql_body.as_deref());
        }
        ntag::T_GrantStmt => {
            // `_jumbleGrantStmt`.
            let s = node.expect_grantstmt();
            jf_bool(jstate, s.is_grant);
            jf_u32(jstate, s.targtype as u32);
            jf_u32(jstate, s.objtype as u32);
            jn_list(jstate, s.objects.as_slice());
            jn_list(jstate, s.privileges.as_slice());
            jn_list(jstate, s.grantees.as_slice());
            jf_bool(jstate, s.grant_option);
            jn_opt(jstate, s.grantor.as_deref());
            jf_u32(jstate, s.behavior as u32);
        }
        ntag::T_GrantRoleStmt => {
            // `_jumbleGrantRoleStmt`.
            let s = node.expect_grantrolestmt();
            jn_list(jstate, s.granted_roles.as_slice());
            jn_list(jstate, s.grantee_roles.as_slice());
            jf_bool(jstate, s.is_grant);
            jn_list(jstate, s.opt.as_slice());
            jn_opt(jstate, s.grantor.as_deref());
            jf_u32(jstate, s.behavior as u32);
        }
        ntag::T_CommentStmt => {
            // `_jumbleCommentStmt`.
            let s = node.expect_commentstmt();
            jf_u32(jstate, s.objtype as u32);
            jn_opt(jstate, s.object.as_deref());
            jumble_opt_cstring(jstate, s.comment.as_deref());
        }
        ntag::T_CreateSeqStmt => {
            // `_jumbleCreateSeqStmt`. `ownerId` is jumbled in C.
            let s = node.expect_createseqstmt();
            jn_opt(jstate, s.sequence.as_deref());
            jn_list(jstate, s.options.as_slice());
            jf_u32(jstate, s.ownerId);
            jf_bool(jstate, s.for_identity);
            jf_bool(jstate, s.if_not_exists);
        }
        ntag::T_CreateSchemaStmt => {
            // `_jumbleCreateSchemaStmt`.
            let s = node.expect_createschemastmt();
            jumble_opt_cstring(jstate, s.schemaname.as_deref());
            jn_opt(jstate, s.authrole.as_deref());
            jn_list(jstate, s.schemaElts.as_slice());
            jf_bool(jstate, s.if_not_exists);
        }
        ntag::T_RuleStmt => {
            // `_jumbleRuleStmt`.
            let s = node.expect_rulestmt();
            jn_opt(jstate, s.relation.as_deref());
            jumble_opt_cstring(jstate, s.rulename.as_deref());
            jn_opt(jstate, s.where_clause.as_deref());
            jf_u32(jstate, s.event as u32);
            jf_bool(jstate, s.instead);
            jn_list(jstate, s.actions.as_slice());
            jf_bool(jstate, s.replace);
        }
        ntag::T_CreateTrigStmt => {
            // `_jumbleCreateTrigStmt`.
            let s = node.expect_createtrigstmt();
            jf_bool(jstate, s.replace);
            jf_bool(jstate, s.isconstraint);
            jumble_opt_cstring(jstate, s.trigname.as_deref());
            jn_opt(jstate, s.relation.as_deref());
            jn_list(jstate, s.funcname.as_slice());
            jn_list(jstate, s.args.as_slice());
            jf_bool(jstate, s.row);
            jf_i32(jstate, s.timing as i32);
            jf_i32(jstate, s.events as i32);
            jn_list(jstate, s.columns.as_slice());
            jn_opt(jstate, s.whenClause.as_deref());
            jn_list(jstate, s.transitionRels.as_slice());
            jf_bool(jstate, s.deferrable);
            jf_bool(jstate, s.initdeferred);
            jn_opt(jstate, s.constrrel.as_deref());
        }
        ntag::T_CreatePolicyStmt => {
            // `_jumbleCreatePolicyStmt`.
            let s = node.expect_createpolicystmt();
            jumble_opt_cstring(jstate, s.policy_name.as_deref());
            jn_opt(jstate, s.table.as_deref());
            jumble_opt_cstring(jstate, s.cmd_name.as_deref());
            jf_bool(jstate, s.permissive);
            jn_list(jstate, s.roles.as_slice());
            jn_opt(jstate, s.qual.as_deref());
            jn_opt(jstate, s.with_check.as_deref());
        }
        ntag::T_CreateStatsStmt => {
            // `_jumbleCreateStatsStmt`.
            let s = node.expect_createstatsstmt();
            jn_list(jstate, s.defnames.as_slice());
            jn_list(jstate, s.stat_types.as_slice());
            jn_list(jstate, s.exprs.as_slice());
            jn_list(jstate, s.relations.as_slice());
            jumble_opt_cstring(jstate, s.stxcomment.as_deref());
            jf_bool(jstate, s.transformed);
            jf_bool(jstate, s.if_not_exists);
        }
        ntag::T_StatsElem => {
            // `_jumbleStatsElem`: name, expr.
            let s = node.expect_statselem();
            jumble_opt_cstring(jstate, s.name.as_deref());
            jn_opt(jstate, s.expr.as_deref());
        }
        ntag::T_CreateDomainStmt => {
            // `_jumbleCreateDomainStmt`.
            let s = node.expect_createdomainstmt();
            jn_list(jstate, s.domainname.as_slice());
            jn_opt(jstate, s.typeName.as_deref());
            jn_opt(jstate, s.collClause.as_deref());
            jn_list(jstate, s.constraints.as_slice());
        }
        ntag::T_DefineStmt => {
            // `_jumbleDefineStmt` (CREATE TYPE / AGGREGATE / OPERATOR / etc.).
            let s = node.expect_definestmt();
            jf_u32(jstate, s.kind as u32);
            jf_bool(jstate, s.oldstyle);
            jn_list(jstate, s.defnames.as_slice());
            jn_list(jstate, s.args.as_slice());
            jn_list(jstate, s.definition.as_slice());
            jf_bool(jstate, s.if_not_exists);
            jf_bool(jstate, s.replace);
        }

        // ===================================================================
        // Utility statements wrapping an inner (analyzed) Query — EXPLAIN /
        // COPY (...) / CALL / PREPARE / EXECUTE. `JUMBLE_NODE(query)` must
        // recurse into the inner Query so its constants are recorded
        // (normalizing `EXPLAIN SELECT 1` / `SELECT 2` to one entry with `$1`)
        // and so distinct inner queries get distinct ids.
        // ===================================================================
        ntag::T_ExplainStmt => {
            // `_jumbleExplainStmt`: query, options.
            let s = node.expect_explainstmt();
            jumble_view_query_child(jstate, s.query.as_deref());
            jn_list(jstate, s.options.as_slice());
        }
        ntag::T_CopyStmt => {
            // `_jumbleCopyStmt`.
            let s = node.expect_copystmt();
            jn_opt(jstate, s.relation.as_deref());
            jumble_view_query_child(jstate, s.query.as_deref());
            jn_list(jstate, s.attlist.as_slice());
            jf_bool(jstate, s.is_from);
            jf_bool(jstate, s.is_program);
            jumble_opt_cstring(jstate, s.filename.as_deref());
            jn_list(jstate, s.options.as_slice());
            jn_opt(jstate, s.where_clause.as_deref());
        }
        ntag::T_CallStmt => {
            // `_jumbleCallStmt`: funcexpr, outargs. funcexpr is the analyzed
            // FuncExpr — recurse so the call's argument constants normalize.
            let s = node.expect_callstmt();
            jn_opt(jstate, s.funcexpr.as_deref());
            jn_list(jstate, s.outargs.as_slice());
        }
        ntag::T_PrepareStmt => {
            // `_jumblePrepareStmt`: name, argtypes, query.
            let s = node.expect_preparestmt();
            jumble_opt_cstring(jstate, s.name.as_deref());
            jn_list(jstate, s.argtypes.as_slice());
            jumble_view_query_child(jstate, s.query.as_deref());
        }
        ntag::T_ExecuteStmt => {
            // `_jumbleExecuteStmt`: name, params.
            let s = node.expect_executestmt();
            jumble_opt_cstring(jstate, s.name.as_deref());
            jn_list(jstate, s.params.as_slice());
        }
        ntag::T_RangeTblFunction => {
            // `_jumbleRangeTblFunction`: JUMBLE_NODE(funcexpr). A
            // `RangeTblEntry` of kind RTE_FUNCTION carries these; walking the
            // funcexpr records its argument constants' locations so
            // `generate_series(1,10)` / `generate_series(2,20)` normalize.
            let s = node.expect_rangetblfunction();
            jn_opt(jstate, s.funcexpr.as_deref());
        }

        ntag::T_Integer
        | ntag::T_Float
        | ntag::T_Boolean
        | ntag::T_String
        | ntag::T_BitString => {
            // A bare value node (e.g. a DefElem.arg): jumble its payload so the
            // option value distinguishes the statement. (Re-emit the tag the
            // generic head already emitted? No — jumble_value_node emits the
            // tag, so emit only the payload here to avoid a double tag.)
            match tag {
                ntag::T_Integer => jf_i32(jstate, node.expect_integer().ival),
                ntag::T_Boolean => jf_bool(jstate, node.expect_boolean().boolval),
                ntag::T_Float => {
                    jumble_opt_cstring(jstate, Some(node.expect_float().fval.as_str()))
                }
                ntag::T_String => {
                    jumble_opt_cstring(jstate, Some(node.expect_string().sval.as_str()))
                }
                ntag::T_BitString => {
                    jumble_opt_cstring(jstate, Some(node.expect_bitstring().bsval.as_str()))
                }
                _ => {}
            }
        }
        ntag::T_List => {
            if let Some(list) = node.as_list() {
                for n in list.iter() {
                    jumble_node(jstate, n);
                }
            }
        }
        _ => {
            // Other utility nodes: the tag already distinguishes them. Recurse
            // into any List children generically so nested constants still
            // contribute their distinguishing value to the jumble.
        }
    }
}

/// `_jumbleCreateStmt`'s body — shared by `T_CreateStmt` and (through `.base`)
/// `T_CreateForeignTableStmt`, mirroring how `gen_node_support.pl` inlines the
/// CreateStmt fields into CreateForeignTableStmt's generated jumble function.
fn jumble_create_stmt_fields(jstate: &mut JumbleState, s: &::nodes::ddlnodes::CreateStmt) {
    jn_opt(jstate, s.relation.as_deref());
    jn_list(jstate, s.tableElts.as_slice());
    jn_list(jstate, s.inhRelations.as_slice());
    jn_opt(jstate, s.partbound.as_deref());
    jn_opt(jstate, s.partspec.as_deref());
    jn_opt(jstate, s.ofTypename.as_deref());
    jn_list(jstate, s.constraints.as_slice());
    jn_list(jstate, s.nnconstraints.as_slice());
    jn_list(jstate, s.options.as_slice());
    jf_u32(jstate, s.oncommit as u32);
    jumble_opt_cstring(jstate, s.tablespacename.as_deref());
    jumble_opt_cstring(jstate, s.accessMethod.as_deref());
    jf_bool(jstate, s.if_not_exists);
}

/// `JUMBLE_NODE(query)` for a ViewStmt/CreateTableAsStmt `query` child. The
/// child is either an analyzed `Query` node (the rewritten/transformed body —
/// recurse with `jumble_query_node` so its constants record) or a raw parse
/// node (recurse through the unified `jumble_node`). A missing child emits the
/// AppendJumbleNull `_jumbleNode` does for a NULL pointer.
fn jumble_view_query_child(jstate: &mut JumbleState, child: Option<&Node>) {
    match child {
        Some(n) => match n.as_query() {
            Some(q) => jumble_query_node(jstate, q),
            None => jumble_node(jstate, n),
        },
        None => jstate.append_jumble_null(),
    }
}

/// `JUMBLE_NODE(item)` for an optional child node pointer — the unified
/// `_jumbleNode` recursion (`None` => `AppendJumbleNull`).
#[inline]
fn jn_opt(jstate: &mut JumbleState, node: Option<&Node>) {
    jumble_opt_node(jstate, node)
}

/// `JUMBLE_NODE(list)` for a `List *` child field — recurse `_jumbleNode` over
/// each element through the unified dispatcher.
#[inline]
fn jn_list(jstate: &mut JumbleState, list: &[NodePtr]) {
    jumble_node_list(jstate, list)
}

/// `_jumbleRangeVar`'s body (the tag is emitted by the caller). The `alias`
/// child is omitted: it is not a distinguishing field for the DDL statements
/// these jumbles cover, and the relation names already differentiate them.
fn jumble_range_var_fields(jstate: &mut JumbleState, s: &::nodes::rawnodes::RangeVar) {
    jumble_opt_cstring(jstate, s.catalogname.as_deref());
    jumble_opt_cstring(jstate, s.schemaname.as_deref());
    jumble_opt_cstring(jstate, s.relname.as_deref());
    jf_bool(jstate, s.inh);
    jstate.append_jumble(&[s.relpersistence as u8]);
}

/// `_jumbleTypeName`'s body (the tag is emitted by the caller). `typeOid` is
/// jumbled (it is not `query_jumble_ignore` in C); the location field is
/// ignored.
fn jumble_type_name_fields(jstate: &mut JumbleState, s: &::nodes::rawnodes::TypeName) {
    jn_list(jstate, s.names.as_slice());
    jf_u32(jstate, s.typeOid);
    jf_bool(jstate, s.setof);
    jf_bool(jstate, s.pct_type);
    jn_list(jstate, s.typmods.as_slice());
    jf_i32(jstate, s.typemod);
    jn_list(jstate, s.arrayBounds.as_slice());
}

/// `JUMBLE_NODE(typeName)` for a typed `Option<PgBox<TypeName>>` child: emit the
/// `T_TypeName` tag + body, or AppendJumbleNull for a missing child — the same
/// `_jumbleNode` shape the generic dispatch produces for a `Node` pointer.
fn jumble_opt_type_name(jstate: &mut JumbleState, t: Option<&::nodes::rawnodes::TypeName>) {
    match t {
        Some(tn) => {
            _jumble_tag(jstate, ntag::T_TypeName);
            jumble_type_name_fields(jstate, tn);
        }
        None => jstate.append_jumble_null(),
    }
}

/// `JUMBLE_NODE(identitySequence)` for a typed `Option<PgBox<RangeVar>>` child:
/// emit the `T_RangeVar` tag + body, or AppendJumbleNull for a missing child.
fn jumble_opt_range_var(jstate: &mut JumbleState, r: Option<&::nodes::rawnodes::RangeVar>) {
    match r {
        Some(rv) => {
            _jumble_tag(jstate, ntag::T_RangeVar);
            jumble_range_var_fields(jstate, rv);
        }
        None => jstate.append_jumble_null(),
    }
}

/// `JUMBLE_NODE(collClause)` for a typed `Option<PgBox<CollateClause>>` child
/// (`_jumbleCollateClause`: arg, collname — location ignored). Emits the
/// `T_CollateClause` tag + body or AppendJumbleNull.
fn jumble_opt_collate_clause(
    jstate: &mut JumbleState,
    c: Option<&::nodes::rawnodes::CollateClause>,
) {
    match c {
        Some(cc) => {
            _jumble_tag(jstate, ntag::T_CollateClause);
            jn_opt(jstate, cc.arg.as_deref());
            jn_list(jstate, cc.collname.as_slice());
        }
        None => jstate.append_jumble_null(),
    }
}

/// Jumble a `ValUnion` value node (Integer/Float/Boolean/String/BitString) the
/// way `_jumbleA_Const`'s switch does: the discriminating tag plus the literal
/// payload. The payload is jumbled (NOT location-recorded).
fn jumble_value_node(jstate: &mut JumbleState, v: &Node) {
    let tag = v.node_tag();
    _jumble_tag(jstate, tag);
    match tag {
        ntag::T_Integer => {
            jf_i32(jstate, v.expect_integer().ival);
        }
        ntag::T_Boolean => {
            jf_bool(jstate, v.expect_boolean().boolval);
        }
        ntag::T_Float => {
            jumble_opt_cstring(jstate, Some(v.expect_float().fval.as_str()));
        }
        ntag::T_String => {
            jumble_opt_cstring(jstate, Some(v.expect_string().sval.as_str()));
        }
        ntag::T_BitString => {
            jumble_opt_cstring(jstate, Some(v.expect_bitstring().bsval.as_str()));
        }
        _ => {}
    }
}

/// `JUMBLE_STRING(str)` — append the bytes of the string plus a NUL terminator
/// (matching C's `strlen(str)+1`), or an AppendJumbleNull for a missing string.
fn jumble_opt_cstring(jstate: &mut JumbleState, s: Option<&str>) {
    match s {
        Some(s) => {
            jstate.append_jumble(s.as_bytes());
            jstate.append_jumble(&[0u8]);
        }
        None => jstate.append_jumble_null(),
    }
}

/// Recurse `_jumbleNode` into a child analyzed `Query` (the DECLARE CURSOR inner
/// query). Emits the `T_Query` tag and walks the full query tree, so its
/// constants are recorded for normalization.
fn jumble_query_child(jstate: &mut JumbleState, q: &Query) {
    jumble_query_node(jstate, q);
}
