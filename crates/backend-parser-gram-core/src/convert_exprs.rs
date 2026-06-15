// A_Const + grammar-produced `Expr` leaves + tag naming (included into
// convert.rs).

// ---------------------------------------------------------------------------
// A_Const — the inline ValUnion literal.
// ---------------------------------------------------------------------------

/// `A_Const` carries its value node *inline* in a `union ValUnion`. We discover
/// the active arm via `val.node.type_`, then produce an owned `A_Const` whose
/// `val` is the corresponding `Node` value (or `None` for a NULL constant).
fn conv_a_const<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::A_Const) -> PgResult<Node<'mcx>> {
    let c = unsafe { &*p };
    let val: Option<NodePtr<'mcx>> = if c.isnull {
        None
    } else {
        // The union's first member is a bare `Node` header carrying the tag.
        let inner_tag = unsafe { c.val.node.type_ };
        let node = match inner_tag {
            tags::T_Integer => Node::Integer(tn_val::Integer {
                ival: unsafe { c.val.ival.ival },
            }),
            tags::T_Float => Node::Float(tn_val::Float {
                fval: cstr(mcx, unsafe { c.val.fval.fval })?,
            }),
            tags::T_Boolean => Node::Boolean(tn_val::Boolean {
                boolval: unsafe { c.val.boolval.boolval },
            }),
            tags::T_String => Node::String(tn_val::StringNode {
                sval: cstr(mcx, unsafe { c.val.sval.sval })?,
            }),
            tags::T_BitString => Node::BitString(tn_val::BitString {
                bsval: cstr(mcx, unsafe { c.val.bsval.bsval })?,
            }),
            other => panic!("gram converter: invalid A_Const ValUnion tag {other}"),
        };
        Some(mcx::alloc_in(mcx, node)?)
    };
    Ok(Node::A_Const(tn::A_Const {
        val,
        isnull: c.isnull,
        location: c.location,
    }))
}

// ---------------------------------------------------------------------------
// Grammar-produced `Expr` leaves.
//
// MODEL GAP (not missing converter code): the raw grammar builds these
// `Expr`-derived nodes (`makeBoolExpr`, `CaseExpr`, `CoalesceExpr`, …) with
// *raw* `Node *` children (ColumnRef/A_Expr/A_Const/…). But `types_nodes`'
// `primnodes::Expr` is the *post-analysis* form: e.g. `BoolExpr.args: Vec<Expr>`
// and `CaseExpr.arg: Option<Box<Expr>>` — children are `Expr`, and `Expr` has
// no `ColumnRef`/`A_Expr`/`A_Const` arm (those are `Node` arms). So a raw
// grammar `BoolExpr(args=[ColumnRef, A_Expr])` is NOT representable in the
// current model without misrepresenting its children.
//
// Faithfully converting these therefore needs a raw-expression node model in
// `types-nodes` that does not exist yet (the "parser/planner emit owned values"
// campaign keystone). Until that lands, each such arm is a loud
// mirror-PG-and-panic. The operator/column/function/constant expression core
// the grammar emits as top-level `Node` arms (`A_Expr`/`ColumnRef`/`FuncCall`/
// `A_Const`/`A_ArrayExpr`/`TypeCast`/`A_Indirection`/…) IS fully converted.
// ---------------------------------------------------------------------------

fn conv_expr<'mcx>(_mcx: Mcx<'mcx>, _n: *mut RawNode, tag: u32) -> PgResult<tn_prim::Expr> {
    let name = node_tag_name(tag);
    panic!(
        "gram converter: grammar-produced Expr node tag {tag} ({name}) is not yet \
         representable in types_nodes::primnodes::Expr (the post-analysis Expr \
         model has `Expr` children; the raw grammar builds raw `Node` children). \
         Needs the raw-expression node-model keystone (parser/planner owned-values \
         campaign); parser grammar F1.5+"
    );
}

// ---------------------------------------------------------------------------
// Tag naming (for panic messages).
// ---------------------------------------------------------------------------

fn node_tag_name(tag: u32) -> &'static str {
    match tag {
        tags::T_CaseExpr => "CaseExpr",
        tags::T_CoalesceExpr => "CoalesceExpr",
        tags::T_MinMaxExpr => "MinMaxExpr",
        tags::T_SubLink => "SubLink",
        tags::T_BooleanTest => "BooleanTest",
        tags::T_NullTest => "NullTest",
        tags::T_XmlExpr => "XmlExpr",
        tags::T_RowExpr => "RowExpr",
        tags::T_GroupingFunc => "GroupingFunc",
        tags::T_CollateExpr => "CollateExpr",
        tags::T_SetToDefault => "SetToDefault",
        tags::T_CurrentOfExpr => "CurrentOfExpr",
        tags::T_NamedArgExpr => "NamedArgExpr",
        tags::T_BoolExpr => "BoolExpr",
        tags::T_SQLValueFunction => "SQLValueFunction",
        _ => "unknown/DDL-utility-node",
    }
}
