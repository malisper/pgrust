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
            // `_jumbleArrayExpr` → `JUMBLE_ELEMENTS(elements, node)`: try to
            // squash a constant-only element list into a single recorded
            // location; otherwise jumble the elements normally.
            jumble_elements_array(jstate, a);
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
    let tag = node.node_tag();
    _jumble_tag(jstate, tag);

    match tag {
        ntag::T_VariableSetStmt => {
            // `_jumbleVariableSetStmt` (queryjumblefuncs.c:739).
            let s = node.expect_variablesetstmt();
            jf_u32(jstate, s.kind as u32);
            // JUMBLE_STRING(name).
            jumble_opt_cstring(jstate, s.name.as_deref());
            // Account for the args only if the parser asked us to.
            if s.jumble_args {
                for a in s.args.iter() {
                    jumble_utility_node(jstate, a);
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
        ntag::T_List => {
            if let Some(list) = node.as_list() {
                for n in list.iter() {
                    jumble_utility_node(jstate, n);
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
