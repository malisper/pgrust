// Exact port of C's generated queryjumblefuncs.funcs.c/.switch.c plus the
// hand-written custom arms (_jumbleParam/_jumbleA_Const/_jumbleVariableSetStmt/
// _jumbleRangeTblEntry_eref/_jumbleElements). Field selection, order and byte
// widths must match C's JUMBLE_FIELD/JUMBLE_NODE/JUMBLE_STRING expansions —
// queryIds are compared numerically against C.

use types_core::catalog::FirstGenbkiObjectId;
use types_error::PgResult;
use types_nodes::list::{IntList, NodeList, OidList, OptNodeList, XidList};
use types_nodes::parsenodes as q;
use types_nodes::primnodes as p;
use types_nodes::rawnodes as r;
use types_nodes::{Node, NodeTag};

use crate::JumbleState;

impl<'mcx> JumbleState<'mcx> {
    #[inline]
    fn tag(&mut self, t: NodeTag) {
        self.append(&(t as u16 as u32).to_ne_bytes());
    }

    #[inline]
    fn f_bool(&mut self, v: bool) {
        self.append(&[v as u8]);
    }

    #[inline]
    fn f_u8(&mut self, v: u8) {
        self.append(&[v]);
    }

    #[inline]
    fn f_i8(&mut self, v: i8) {
        self.append(&[v as u8]);
    }

    #[inline]
    fn f_i16(&mut self, v: i16) {
        self.append(&v.to_ne_bytes());
    }

    #[inline]
    fn f_u32(&mut self, v: u32) {
        self.append(&v.to_ne_bytes());
    }

    #[inline]
    fn f_i32(&mut self, v: i32) {
        self.append(&v.to_ne_bytes());
    }

    #[inline]
    fn f_u64(&mut self, v: u64) {
        self.append(&v.to_ne_bytes());
    }

    #[inline]
    fn f_i64(&mut self, v: i64) {
        self.append(&v.to_ne_bytes());
    }

    // JUMBLE_STRING: strlen+1 bytes (trailing NUL included); NULL → one
    // pending null.
    fn f_str(&mut self, s: Option<&str>) {
        match s {
            Some(s) => {
                if self.pending_nulls > 0 {
                    self.flush_pending_nulls();
                }
                if !s.is_empty() {
                    self.append_internal(s.as_bytes());
                }
                self.append_internal(&[0]);
            }
            None => self.append_null(),
        }
    }

    fn loc(&mut self, location: i32) -> PgResult<()> {
        self.record_const_location(false, location, -1)
    }
}

type J<'a, 'mcx> = &'a mut JumbleState<'mcx>;

fn node<'mcx>(js: J<'_, 'mcx>, n: Option<Node<'mcx>>) -> PgResult<()> {
    match n {
        Some(n) => jumble_node(js, n),
        None => {
            js.append_null();
            Ok(())
        }
    }
}

fn list<'mcx>(js: J<'_, 'mcx>, l: &NodeList<'mcx>) -> PgResult<()> {
    if l.is_nil() {
        js.append_null();
        return Ok(());
    }
    js.tag(NodeTag::T_List);
    for n in l.iter() {
        jumble_node(js, n)?;
    }
    Ok(())
}

fn opt_list<'mcx>(js: J<'_, 'mcx>, l: &OptNodeList<'mcx>) -> PgResult<()> {
    if l.is_nil() {
        js.append_null();
        return Ok(());
    }
    js.tag(NodeTag::T_List);
    for n in l.iter() {
        node(js, n)?;
    }
    Ok(())
}

fn int_list(js: J<'_, '_>, l: &IntList<'_>) {
    if l.is_nil() {
        js.append_null();
        return;
    }
    js.tag(NodeTag::T_IntList);
    for v in l.iter() {
        js.f_i32(v);
    }
}

fn oid_list(js: J<'_, '_>, l: &OidList<'_>) {
    if l.is_nil() {
        js.append_null();
        return;
    }
    js.tag(NodeTag::T_OidList);
    for v in l.iter() {
        js.f_u32(v);
    }
}

fn xid_list(js: J<'_, '_>, l: &XidList<'_>) {
    if l.is_nil() {
        js.append_null();
        return;
    }
    js.tag(NodeTag::T_XidList);
    for v in l.iter() {
        js.f_u32(v);
    }
}

// C's SelectStmt.distinctClause: plain DISTINCT is list_make1(NIL) — a
// one-NULL-cell List; DISTINCT ON is the expression list.
fn distinct_clause<'mcx>(js: J<'_, 'mcx>, d: &r::DistinctClause<'mcx>) -> PgResult<()> {
    match d {
        r::DistinctClause::None => js.append_null(),
        r::DistinctClause::All => {
            js.tag(NodeTag::T_List);
            js.append_null();
        }
        r::DistinctClause::On(l) => list(js, l)?,
    }
    Ok(())
}

fn alias<'mcx>(js: J<'_, 'mcx>, a: Option<&p::Alias<'mcx>>) -> PgResult<()> {
    let Some(a) = a else {
        js.append_null();
        return Ok(());
    };
    js.tag(NodeTag::T_Alias);
    js.f_str(a.aliasname);
    list(js, &a.colnames)
}

fn range_var<'mcx>(js: J<'_, 'mcx>, rv: Option<&p::RangeVar<'mcx>>) -> PgResult<()> {
    let Some(rv) = rv else {
        js.append_null();
        return Ok(());
    };
    js.tag(NodeTag::T_RangeVar);
    js.f_str(rv.catalogname);
    js.f_str(rv.schemaname);
    js.f_str(rv.relname);
    js.f_bool(rv.inh);
    js.f_u8(rv.relpersistence);
    alias(js, rv.alias)
}

fn role_spec<'mcx>(js: J<'_, 'mcx>, rs: Option<&q::RoleSpec<'mcx>>) {
    let Some(rs) = rs else {
        js.append_null();
        return;
    };
    js.tag(NodeTag::T_RoleSpec);
    js.f_u32(rs.roletype as u32);
    js.f_str(rs.rolename);
}

fn from_expr<'mcx>(js: J<'_, 'mcx>, fe: Option<&p::FromExpr<'mcx>>) -> PgResult<()> {
    let Some(fe) = fe else {
        js.append_null();
        return Ok(());
    };
    js.tag(NodeTag::T_FromExpr);
    list(js, &fe.fromlist)?;
    node(js, fe.quals)
}

fn json_format(js: J<'_, '_>, f: Option<&p::JsonFormat>) {
    let Some(f) = f else {
        js.append_null();
        return;
    };
    js.tag(NodeTag::T_JsonFormat);
    js.f_u32(f.format_type as u32);
    js.f_u32(f.encoding as u32);
}

fn json_returning<'mcx>(js: J<'_, 'mcx>, jr: Option<&p::JsonReturning<'mcx>>) {
    let Some(jr) = jr else {
        js.append_null();
        return;
    };
    js.tag(NodeTag::T_JsonReturning);
    json_format(js, jr.format);
    js.f_u32(jr.typid);
    js.f_i32(jr.typmod);
}

fn func_expr<'mcx>(js: J<'_, 'mcx>, fe: Option<&p::FuncExpr<'mcx>>) -> PgResult<()> {
    let Some(fe) = fe else {
        js.append_null();
        return Ok(());
    };
    js.tag(NodeTag::T_FuncExpr);
    js.f_u32(fe.funcid);
    list(js, &fe.args)
}

fn object_with_args<'mcx>(js: J<'_, 'mcx>, o: Option<&q::ObjectWithArgs<'mcx>>) -> PgResult<()> {
    let Some(o) = o else {
        js.append_null();
        return Ok(());
    };
    js.tag(NodeTag::T_ObjectWithArgs);
    list(js, &o.objname)?;
    opt_list(js, &o.objargs)?;
    list(js, &o.objfuncargs)?;
    js.f_bool(o.args_unspecified);
    Ok(())
}

fn grant_stmt<'mcx>(js: J<'_, 'mcx>, g: Option<&q::GrantStmt<'mcx>>) -> PgResult<()> {
    let Some(g) = g else {
        js.append_null();
        return Ok(());
    };
    js.tag(NodeTag::T_GrantStmt);
    js.f_bool(g.is_grant);
    js.f_u32(g.targtype as u32);
    js.f_u32(g.objtype as u32);
    list(js, &g.objects)?;
    list(js, &g.privileges)?;
    list(js, &g.grantees)?;
    js.f_bool(g.grant_option);
    role_spec(js, g.grantor);
    js.f_u32(g.behavior as u32);
    Ok(())
}

fn variable_set_stmt<'mcx>(js: J<'_, 'mcx>, s: &q::VariableSetStmt<'mcx>) -> PgResult<()> {
    js.tag(NodeTag::T_VariableSetStmt);
    js.f_u32(s.kind as u32);
    js.f_str(s.name);
    if s.jumble_args {
        list(js, &s.args)?;
    }
    js.f_bool(s.is_local);
    js.loc(s.location)
}

fn publication_table<'mcx>(js: J<'_, 'mcx>, t: Option<&q::PublicationTable<'mcx>>) -> PgResult<()> {
    let Some(t) = t else {
        js.append_null();
        return Ok(());
    };
    js.tag(NodeTag::T_PublicationTable);
    range_var(js, t.relation)?;
    node(js, t.whereClause)?;
    list(js, &t.columns)
}

fn select_stmt<'mcx>(js: J<'_, 'mcx>, s: Option<&r::SelectStmt<'mcx>>) -> PgResult<()> {
    let Some(s) = s else {
        js.append_null();
        return Ok(());
    };
    js.tag(NodeTag::T_SelectStmt);
    distinct_clause(js, &s.distinctClause)?;
    node(js, s.intoClause)?;
    list(js, &s.targetList)?;
    list(js, &s.fromClause)?;
    node(js, s.whereClause)?;
    list(js, &s.groupClause)?;
    js.f_bool(s.groupDistinct);
    node(js, s.havingClause)?;
    list(js, &s.windowClause)?;
    list(js, &s.valuesLists)?;
    list(js, &s.sortClause)?;
    node(js, s.limitOffset)?;
    node(js, s.limitCount)?;
    js.f_u32(s.limitOption as u32);
    list(js, &s.lockingClause)?;
    node(js, s.withClause)?;
    js.f_u32(s.op as u32);
    js.f_bool(s.all);
    select_stmt(js, s.larg)?;
    select_stmt(js, s.rarg)
}

fn create_stmt_fields<'mcx>(js: J<'_, 'mcx>, s: &r::CreateStmt<'mcx>) -> PgResult<()> {
    range_var(js, s.relation)?;
    list(js, &s.tableElts)?;
    list(js, &s.inhRelations)?;
    node(js, s.partbound)?;
    node(js, s.partspec)?;
    node(js, s.ofTypename)?;
    list(js, &s.constraints)?;
    list(js, &s.nnconstraints)?;
    list(js, &s.options)?;
    js.f_u32(s.oncommit as u32);
    js.f_str(s.tablespacename);
    js.f_str(s.accessMethod);
    js.f_bool(s.if_not_exists);
    Ok(())
}

// C _jumbleParam: paramtypmod/paramcollid/location ignored; PARAM_EXTERN
// records its location and raises highest_extern_param_id.
fn param(js: J<'_, '_>, e: &p::Param) -> PgResult<()> {
    js.f_u32(e.paramkind as u32);
    js.f_i32(e.paramid);
    js.f_u32(e.paramtype);
    if e.paramkind == p::ParamKind::PARAM_EXTERN {
        js.record_const_location(true, e.location, -1)?;
        if e.paramid > js.highest_extern_param_id {
            js.highest_extern_param_id = e.paramid;
        }
    }
    Ok(())
}

fn a_const(js: J<'_, '_>, e: &r::A_Const<'_>) {
    js.f_bool(e.isnull());
    if let Some(val) = &e.val {
        match val {
            r::ValUnion::Integer(v) => {
                js.tag(NodeTag::T_Integer);
                js.f_i32(v.ival);
            }
            r::ValUnion::Float(v) => {
                js.tag(NodeTag::T_Float);
                js.f_str(Some(v.fval));
            }
            r::ValUnion::Boolean(v) => {
                js.tag(NodeTag::T_Boolean);
                js.f_bool(v.boolval);
            }
            r::ValUnion::String(v) => {
                js.tag(NodeTag::T_String);
                js.f_str(Some(v.sval));
            }
            r::ValUnion::BitString(v) => {
                js.tag(NodeTag::T_BitString);
                js.f_str(Some(v.bsval));
            }
        }
    }
}

// _jumbleRangeTblEntry_eref: only the Alias tag and the table name; column
// names are ignored.
fn rte_eref(js: J<'_, '_>, eref: Option<&p::Alias<'_>>) {
    let eref = eref.expect("RangeTblEntry.eref is never NULL after parse analysis");
    js.tag(NodeTag::T_Alias);
    js.f_str(eref.aliasname);
}

fn is_squashable_constant(element: Node<'_>) -> bool {
    let mut element = element;
    loop {
        match element.node_tag() {
            NodeTag::T_RelabelType => {
                element = element.as_variant::<p::RelabelType>().unwrap().arg;
            }
            NodeTag::T_CoerceViaIO => {
                element = element.as_variant::<p::CoerceViaIO>().unwrap().arg;
            }
            NodeTag::T_Const => return true,
            NodeTag::T_Param => {
                return element.as_variant::<p::Param>().unwrap().paramkind
                    == p::ParamKind::PARAM_EXTERN;
            }
            NodeTag::T_FuncExpr => {
                let func = element.as_variant::<p::FuncExpr>().unwrap();
                if func.funcformat != p::CoercionForm::COERCE_IMPLICIT_CAST
                    && func.funcformat != p::CoercionForm::COERCE_EXPLICIT_CAST
                {
                    return false;
                }
                if func.funcid > FirstGenbkiObjectId {
                    return false;
                }
                for (i, arg) in func.args.iter().enumerate() {
                    if arg.node_tag() != NodeTag::T_Const {
                        if i == 0 && stack_depth::stack_is_too_deep() {
                            return false;
                        } else if !is_squashable_constant(arg) {
                            return false;
                        }
                    }
                }
                return true;
            }
            _ => return false,
        }
    }
}

fn is_squashable_constant_list(elements: &NodeList<'_>) -> bool {
    if elements.len() < 2 {
        return false;
    }
    elements.iter().all(is_squashable_constant)
}

// _jumbleElements: squash an all-constant ArrayExpr element list to a single
// recorded location instead of jumbling each element.
fn elements<'mcx>(js: J<'_, 'mcx>, l: &NodeList<'mcx>, e: &p::ArrayExpr<'mcx>) -> PgResult<()> {
    if is_squashable_constant_list(l) && e.list_start > 0 && e.list_end > 0 {
        js.record_const_location(false, e.list_start + 1, (e.list_end - e.list_start) - 1)?;
        js.has_squashed_lists = true;
        return Ok(());
    }
    list(js, l)
}

pub(crate) fn jumble_query_struct<'mcx>(
    js: J<'_, 'mcx>,
    e: &q::Query<'mcx>,
) -> PgResult<()> {
    js.tag(NodeTag::T_Query);
    js.f_u32(e.commandType as u32);
    node(js, e.utilityStmt)?;
    list(js, &e.cteList)?;
    list(js, &e.rtable)?;
    from_expr(js, e.jointree)?;
    list(js, &e.mergeActionList)?;
    node(js, e.mergeJoinCondition)?;
    list(js, &e.targetList)?;
    node(js, e.onConflict)?;
    list(js, &e.returningList)?;
    list(js, &e.groupClause)?;
    js.f_bool(e.groupDistinct);
    list(js, &e.groupingSets)?;
    node(js, e.havingQual)?;
    list(js, &e.windowClause)?;
    list(js, &e.distinctClause)?;
    list(js, &e.sortClause)?;
    node(js, e.limitOffset)?;
    node(js, e.limitCount)?;
    js.f_u32(e.limitOption as u32);
    list(js, &e.rowMarks)?;
    node(js, e.setOperations)
}

fn query_opt<'mcx>(js: J<'_, 'mcx>, e: Option<&q::Query<'mcx>>) -> PgResult<()> {
    match e {
        Some(e) => jumble_query_struct(js, e),
        None => {
            js.append_null();
            Ok(())
        }
    }
}

#[cold]
#[inline(never)]
fn jumble_gap(tag: NodeTag) -> ! {
    // C's _jumbleNode default arm only warns; here an unhandled tag would
    // silently diverge from C's queryId, so it is loud.
    panic!("_jumbleNode (queryjumblefuncs.c): unhandled node type {tag:?}")
}

fn jumble_node<'mcx>(js: J<'_, 'mcx>, n: Node<'mcx>) -> PgResult<()> {
    stack_depth::check_stack_depth()?;

    macro_rules! cast {
        ($t:ty) => {
            n.as_variant::<$t>().unwrap()
        };
    }

    let t = n.node_tag();
    js.tag(t);
    match t {
        NodeTag::T_Alias => {
            let e = cast!(p::Alias);
            js.f_str(e.aliasname);
            list(js, &e.colnames)?;
        }
        NodeTag::T_RangeVar => {
            let e = cast!(p::RangeVar);
            js.f_str(e.catalogname);
            js.f_str(e.schemaname);
            js.f_str(e.relname);
            js.f_bool(e.inh);
            js.f_u8(e.relpersistence);
            alias(js, e.alias)?;
        }
        NodeTag::T_TableFunc => {
            let e = cast!(p::TableFunc);
            js.f_u32(e.functype as u32);
            node(js, e.docexpr)?;
            node(js, e.rowexpr)?;
            opt_list(js, &e.colexprs)?;
        }
        NodeTag::T_IntoClause => {
            let e = cast!(r::IntoClause);
            node(js, e.rel)?;
            list(js, &e.colNames)?;
            js.f_str(e.accessMethod);
            list(js, &e.options)?;
            js.f_u32(e.onCommit as u32);
            js.f_str(e.tableSpaceName);
            js.f_bool(e.skipData);
        }
        NodeTag::T_Var => {
            let e = cast!(p::Var);
            js.f_i32(e.varno);
            js.f_i16(e.varattno);
            js.f_u32(e.varlevelsup);
            js.f_u32(e.varreturningtype as u32);
        }
        NodeTag::T_Const => {
            let e = cast!(p::Const);
            js.f_u32(e.consttype);
            js.loc(e.location)?;
        }
        NodeTag::T_Param => param(js, cast!(p::Param))?,
        NodeTag::T_Aggref => {
            let e = cast!(p::Aggref);
            js.f_u32(e.aggfnoid);
            list(js, &e.aggdirectargs)?;
            list(js, &e.args)?;
            list(js, &e.aggorder)?;
            list(js, &e.aggdistinct)?;
            node(js, e.aggfilter)?;
        }
        NodeTag::T_GroupingFunc => {
            let e = cast!(p::GroupingFunc);
            int_list(js, &e.refs);
            js.f_u32(e.agglevelsup);
        }
        NodeTag::T_MergeSupportFunc => {
            let e = cast!(p::MergeSupportFunc);
            js.f_u32(e.msftype);
            js.f_u32(e.msfcollid);
        }
        NodeTag::T_WindowFunc => {
            let e = cast!(p::WindowFunc);
            js.f_u32(e.winfnoid);
            list(js, &e.args)?;
            node(js, e.aggfilter)?;
            js.f_u32(e.winref);
        }
        NodeTag::T_SubscriptingRef => {
            let e = cast!(p::SubscriptingRef);
            opt_list(js, &e.refupperindexpr)?;
            opt_list(js, &e.reflowerindexpr)?;
            node(js, e.refexpr)?;
            node(js, e.refassgnexpr)?;
        }
        NodeTag::T_FuncExpr => {
            let e = cast!(p::FuncExpr);
            js.f_u32(e.funcid);
            list(js, &e.args)?;
        }
        NodeTag::T_NamedArgExpr => {
            let e = cast!(p::NamedArgExpr);
            node(js, e.arg)?;
            js.f_i32(e.argnumber);
        }
        NodeTag::T_OpExpr => {
            let e = cast!(p::OpExpr);
            js.f_u32(e.opno);
            list(js, &e.args)?;
        }
        NodeTag::T_DistinctExpr => {
            let e = cast!(p::DistinctExpr);
            js.f_u32(e.opno);
            list(js, &e.args)?;
        }
        NodeTag::T_NullIfExpr => {
            let e = cast!(p::NullIfExpr);
            js.f_u32(e.opno);
            list(js, &e.args)?;
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let e = cast!(p::ScalarArrayOpExpr);
            js.f_u32(e.opno);
            js.f_bool(e.useOr);
            list(js, &e.args)?;
        }
        NodeTag::T_BoolExpr => {
            let e = cast!(p::BoolExpr);
            js.f_u32(e.boolop as u32);
            list(js, &e.args)?;
        }
        NodeTag::T_SubLink => {
            let e = cast!(p::SubLink);
            js.f_u32(e.subLinkType as u32);
            js.f_i32(e.subLinkId);
            node(js, e.testexpr)?;
            jumble_node(js, e.subselect)?;
        }
        NodeTag::T_FieldSelect => {
            let e = cast!(p::FieldSelect);
            jumble_node(js, e.arg)?;
            js.f_i16(e.fieldnum);
        }
        NodeTag::T_FieldStore => {
            let e = cast!(p::FieldStore);
            jumble_node(js, e.arg)?;
            list(js, &e.newvals)?;
        }
        NodeTag::T_RelabelType => {
            let e = cast!(p::RelabelType);
            jumble_node(js, e.arg)?;
            js.f_u32(e.resulttype);
        }
        NodeTag::T_CoerceViaIO => {
            let e = cast!(p::CoerceViaIO);
            jumble_node(js, e.arg)?;
            js.f_u32(e.resulttype);
        }
        NodeTag::T_ArrayCoerceExpr => {
            let e = cast!(p::ArrayCoerceExpr);
            jumble_node(js, e.arg)?;
            node(js, e.elemexpr)?;
            js.f_u32(e.resulttype);
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let e = cast!(p::ConvertRowtypeExpr);
            jumble_node(js, e.arg)?;
            js.f_u32(e.resulttype);
        }
        NodeTag::T_CollateExpr => {
            let e = cast!(p::CollateExpr);
            jumble_node(js, e.arg)?;
            js.f_u32(e.collOid);
        }
        NodeTag::T_CaseExpr => {
            let e = cast!(p::CaseExpr);
            node(js, e.arg)?;
            list(js, &e.args)?;
            node(js, e.defresult)?;
        }
        NodeTag::T_CaseWhen => {
            let e = cast!(p::CaseWhen);
            node(js, e.expr)?;
            node(js, e.result)?;
        }
        NodeTag::T_CaseTestExpr => {
            js.f_u32(cast!(p::CaseTestExpr).typeId);
        }
        NodeTag::T_ArrayExpr => {
            let e = cast!(p::ArrayExpr);
            elements(js, &e.elements, e)?;
        }
        NodeTag::T_RowExpr => {
            list(js, &cast!(p::RowExpr).args)?;
        }
        NodeTag::T_RowCompareExpr => {
            let e = cast!(p::RowCompareExpr);
            js.f_i32(e.cmptype);
            list(js, &e.largs)?;
            list(js, &e.rargs)?;
        }
        NodeTag::T_CoalesceExpr => {
            list(js, &cast!(p::CoalesceExpr).args)?;
        }
        NodeTag::T_MinMaxExpr => {
            let e = cast!(p::MinMaxExpr);
            js.f_u32(e.op as u32);
            list(js, &e.args)?;
        }
        NodeTag::T_SQLValueFunction => {
            let e = cast!(p::SQLValueFunction);
            js.f_u32(e.op as u32);
            js.f_i32(e.typmod);
        }
        NodeTag::T_XmlExpr => {
            let e = cast!(p::XmlExpr);
            js.f_u32(e.op as u32);
            list(js, &e.named_args)?;
            list(js, &e.args)?;
            js.f_bool(e.indent);
        }
        NodeTag::T_JsonFormat => {
            let e = cast!(p::JsonFormat);
            js.f_u32(e.format_type as u32);
            js.f_u32(e.encoding as u32);
        }
        NodeTag::T_JsonReturning => {
            let e = cast!(p::JsonReturning);
            json_format(js, e.format);
            js.f_u32(e.typid);
            js.f_i32(e.typmod);
        }
        NodeTag::T_JsonValueExpr => {
            let e = cast!(p::JsonValueExpr);
            node(js, e.raw_expr)?;
            node(js, e.formatted_expr)?;
            json_format(js, e.format);
        }
        NodeTag::T_JsonConstructorExpr => {
            let e = cast!(p::JsonConstructorExpr);
            js.f_u32(e.r#type as u32);
            list(js, &e.args)?;
            node(js, e.func)?;
            node(js, e.coercion)?;
            json_returning(js, e.returning);
            js.f_bool(e.absent_on_null);
            js.f_bool(e.unique);
        }
        NodeTag::T_JsonIsPredicate => {
            let e = cast!(p::JsonIsPredicate);
            node(js, e.expr)?;
            json_format(js, e.format);
            js.f_u32(e.item_type as u32);
            js.f_bool(e.unique_keys);
        }
        NodeTag::T_JsonBehavior => {
            let e = cast!(p::JsonBehavior);
            js.f_u32(e.btype as u32);
            node(js, e.expr)?;
            js.f_bool(e.coerce);
        }
        NodeTag::T_JsonExpr => {
            let e = cast!(p::JsonExpr);
            js.f_u32(e.op as u32);
            js.f_str(e.column_name);
            node(js, e.formatted_expr)?;
            json_format(js, e.format);
            node(js, e.path_spec)?;
            json_returning(js, e.returning);
            list(js, &e.passing_names)?;
            list(js, &e.passing_values)?;
            node(js, e.on_empty)?;
            node(js, e.on_error)?;
            js.f_bool(e.use_io_coercion);
            js.f_bool(e.use_json_coercion);
            js.f_u32(e.wrapper as u32);
            js.f_bool(e.omit_quotes);
            js.f_u32(e.collation);
        }
        NodeTag::T_NullTest => {
            let e = cast!(p::NullTest);
            node(js, e.arg)?;
            js.f_u32(e.nulltesttype as u32);
        }
        NodeTag::T_BooleanTest => {
            let e = cast!(p::BooleanTest);
            node(js, e.arg)?;
            js.f_u32(e.booltesttype as u32);
        }
        NodeTag::T_MergeAction => {
            let e = cast!(p::MergeAction);
            js.f_u32(e.matchKind as u32);
            js.f_u32(e.commandType as u32);
            node(js, e.qual)?;
            list(js, &e.targetList)?;
        }
        NodeTag::T_CoerceToDomain => {
            let e = cast!(p::CoerceToDomain);
            jumble_node(js, e.arg)?;
            js.f_u32(e.resulttype);
        }
        NodeTag::T_CoerceToDomainValue => {
            js.f_u32(cast!(p::CoerceToDomainValue).typeId);
        }
        NodeTag::T_SetToDefault => {
            js.f_u32(cast!(p::SetToDefault).typeId);
        }
        NodeTag::T_CurrentOfExpr => {
            let e = cast!(p::CurrentOfExpr);
            js.f_u32(e.cvarno);
            js.f_str(e.cursor_name);
            js.f_i32(e.cursor_param);
        }
        NodeTag::T_NextValueExpr => {
            let e = cast!(p::NextValueExpr);
            js.f_u32(e.seqid);
            js.f_u32(e.typeId);
        }
        NodeTag::T_InferenceElem => {
            let e = cast!(p::InferenceElem);
            node(js, e.expr)?;
            js.f_u32(e.infercollid);
            js.f_u32(e.inferopclass);
        }
        NodeTag::T_TargetEntry => {
            let e = cast!(p::TargetEntry);
            jumble_node(js, e.expr)?;
            js.f_i16(e.resno);
            js.f_u32(e.ressortgroupref);
        }
        NodeTag::T_RangeTblRef => {
            js.f_i32(cast!(p::RangeTblRef).rtindex);
        }
        NodeTag::T_JoinExpr => {
            let e = cast!(p::JoinExpr);
            js.f_u32(e.jointype as u32);
            js.f_bool(e.isNatural);
            jumble_node(js, e.larg)?;
            jumble_node(js, e.rarg)?;
            node(js, e.quals)?;
            js.f_i32(e.rtindex);
        }
        NodeTag::T_FromExpr => {
            let e = cast!(p::FromExpr);
            list(js, &e.fromlist)?;
            node(js, e.quals)?;
        }
        NodeTag::T_OnConflictExpr => {
            let e = cast!(p::OnConflictExpr);
            js.f_u32(e.action as u32);
            list(js, &e.arbiterElems)?;
            node(js, e.arbiterWhere)?;
            js.f_u32(e.constraint);
            list(js, &e.onConflictSet)?;
            node(js, e.onConflictWhere)?;
            js.f_i32(e.exclRelIndex);
            list(js, &e.exclRelTlist)?;
        }
        NodeTag::T_Query => {
            let e = cast!(q::Query);
            js.f_u32(e.commandType as u32);
            node(js, e.utilityStmt)?;
            list(js, &e.cteList)?;
            list(js, &e.rtable)?;
            from_expr(js, e.jointree)?;
            list(js, &e.mergeActionList)?;
            node(js, e.mergeJoinCondition)?;
            list(js, &e.targetList)?;
            node(js, e.onConflict)?;
            list(js, &e.returningList)?;
            list(js, &e.groupClause)?;
            js.f_bool(e.groupDistinct);
            list(js, &e.groupingSets)?;
            node(js, e.havingQual)?;
            list(js, &e.windowClause)?;
            list(js, &e.distinctClause)?;
            list(js, &e.sortClause)?;
            node(js, e.limitOffset)?;
            node(js, e.limitCount)?;
            js.f_u32(e.limitOption as u32);
            list(js, &e.rowMarks)?;
            node(js, e.setOperations)?;
        }
        NodeTag::T_TypeName => {
            let e = cast!(r::TypeName);
            list(js, &e.names)?;
            js.f_u32(e.typeOid);
            js.f_bool(e.setof);
            js.f_bool(e.pct_type);
            list(js, &e.typmods)?;
            js.f_i32(e.typemod);
            list(js, &e.arrayBounds)?;
        }
        NodeTag::T_ColumnRef => {
            list(js, &cast!(r::ColumnRef).fields)?;
        }
        NodeTag::T_ParamRef => {
            js.f_i32(cast!(r::ParamRef).number);
        }
        NodeTag::T_A_Expr => {
            let e = cast!(r::A_Expr);
            js.f_u32(e.kind as u32);
            list(js, &e.name)?;
            node(js, e.lexpr)?;
            node(js, e.rexpr)?;
        }
        NodeTag::T_A_Const => a_const(js, cast!(r::A_Const)),
        NodeTag::T_TypeCast => {
            let e = cast!(r::TypeCast);
            node(js, e.arg)?;
            node(js, e.typeName)?;
        }
        NodeTag::T_CollateClause => {
            let e = cast!(r::CollateClause);
            node(js, e.arg)?;
            list(js, &e.collname)?;
        }
        NodeTag::T_RoleSpec => {
            let e = cast!(q::RoleSpec);
            js.f_u32(e.roletype as u32);
            js.f_str(e.rolename);
        }
        NodeTag::T_FuncCall => {
            let e = cast!(r::FuncCall);
            list(js, &e.funcname)?;
            list(js, &e.args)?;
            list(js, &e.agg_order)?;
            node(js, e.agg_filter)?;
            node(js, e.over)?;
            js.f_bool(e.agg_within_group);
            js.f_bool(e.agg_star);
            js.f_bool(e.agg_distinct);
            js.f_bool(e.func_variadic);
            js.f_u32(e.funcformat as u32);
        }
        NodeTag::T_A_Star => {}
        NodeTag::T_A_Indices => {
            let e = cast!(r::A_Indices);
            js.f_bool(e.is_slice);
            node(js, e.lidx)?;
            node(js, e.uidx)?;
        }
        NodeTag::T_A_Indirection => {
            let e = cast!(r::A_Indirection);
            node(js, e.arg)?;
            list(js, &e.indirection)?;
        }
        NodeTag::T_A_ArrayExpr => {
            list(js, &cast!(r::A_ArrayExpr).elements)?;
        }
        NodeTag::T_ResTarget => {
            let e = cast!(r::ResTarget);
            js.f_str(e.name);
            list(js, &e.indirection)?;
            node(js, e.val)?;
        }
        NodeTag::T_SortBy => {
            let e = cast!(r::SortBy);
            node(js, e.node)?;
            js.f_u32(e.sortby_dir as u32);
            js.f_u32(e.sortby_nulls as u32);
            list(js, &e.useOp)?;
        }
        NodeTag::T_WindowDef => {
            let e = cast!(r::WindowDef);
            js.f_str(e.name);
            js.f_str(e.refname);
            list(js, &e.partitionClause)?;
            list(js, &e.orderClause)?;
            js.f_i32(e.frameOptions);
            node(js, e.startOffset)?;
            node(js, e.endOffset)?;
        }
        NodeTag::T_RangeSubselect => {
            let e = cast!(r::RangeSubselect);
            js.f_bool(e.lateral);
            node(js, e.subquery)?;
            alias(js, e.alias)?;
        }
        NodeTag::T_RangeFunction => {
            let e = cast!(r::RangeFunction);
            js.f_bool(e.lateral);
            js.f_bool(e.ordinality);
            js.f_bool(e.is_rowsfrom);
            list(js, &e.functions)?;
            alias(js, e.alias)?;
            list(js, &e.coldeflist)?;
        }
        NodeTag::T_RangeTableFunc => {
            let e = cast!(r::RangeTableFunc);
            js.f_bool(e.lateral);
            node(js, e.docexpr)?;
            node(js, e.rowexpr)?;
            list(js, &e.namespaces)?;
            list(js, &e.columns)?;
            alias(js, e.alias)?;
        }
        NodeTag::T_RangeTableFuncCol => {
            let e = cast!(r::RangeTableFuncCol);
            js.f_str(e.colname);
            node(js, e.typeName)?;
            js.f_bool(e.for_ordinality);
            js.f_bool(e.is_not_null);
            node(js, e.colexpr)?;
            node(js, e.coldefexpr)?;
        }
        NodeTag::T_ColumnDef => {
            let e = cast!(r::ColumnDef);
            js.f_str(e.colname);
            node(js, e.typeName)?;
            js.f_str(e.compression);
            js.f_i16(e.inhcount);
            js.f_bool(e.is_local);
            js.f_bool(e.is_not_null);
            js.f_bool(e.is_from_type);
            js.f_u8(e.storage);
            js.f_str(e.storage_name);
            node(js, e.raw_default)?;
            node(js, e.cooked_default)?;
            js.f_u8(e.identity);
            range_var(js, e.identitySequence)?;
            js.f_u8(e.generated);
            node(js, e.collClause)?;
            js.f_u32(e.collOid);
            list(js, &e.constraints)?;
            list(js, &e.fdwoptions)?;
        }
        NodeTag::T_TableLikeClause => {
            let e = cast!(r::TableLikeClause);
            range_var(js, e.relation)?;
            js.f_u32(e.options);
            js.f_u32(e.relationOid);
        }
        NodeTag::T_IndexElem => {
            let e = cast!(r::IndexElem);
            js.f_str(e.name);
            node(js, e.expr)?;
            js.f_str(e.indexcolname);
            list(js, &e.collation)?;
            list(js, &e.opclass)?;
            list(js, &e.opclassopts)?;
            js.f_u32(e.ordering as u32);
            js.f_u32(e.nulls_ordering as u32);
        }
        NodeTag::T_DefElem => {
            let e = cast!(q::DefElem);
            js.f_str(e.defnamespace);
            js.f_str(e.defname);
            node(js, e.arg)?;
            js.f_u32(e.defaction as u32);
        }
        NodeTag::T_LockingClause => {
            let e = cast!(r::LockingClause);
            list(js, &e.lockedRels)?;
            js.f_u32(e.strength as u32);
            js.f_u32(e.waitPolicy as u32);
        }
        NodeTag::T_XmlSerialize => {
            let e = cast!(r::XmlSerialize);
            js.f_u32(e.xmloption as u32);
            node(js, e.expr)?;
            node(js, e.typeName)?;
            js.f_bool(e.indent);
        }
        NodeTag::T_PartitionElem => {
            let e = cast!(r::PartitionElem);
            js.f_str(e.name);
            node(js, e.expr)?;
            list(js, &e.collation)?;
            list(js, &e.opclass)?;
        }
        NodeTag::T_PartitionSpec => {
            let e = cast!(r::PartitionSpec);
            js.f_u32(e.strategy as u32);
            list(js, &e.partParams)?;
        }
        NodeTag::T_PartitionBoundSpec => {
            let e = cast!(r::PartitionBoundSpec);
            js.f_u8(e.strategy);
            js.f_bool(e.is_default);
            js.f_i32(e.modulus);
            js.f_i32(e.remainder);
            list(js, &e.listdatums)?;
            list(js, &e.lowerdatums)?;
            list(js, &e.upperdatums)?;
        }
        NodeTag::T_PartitionRangeDatum => {
            let e = cast!(r::PartitionRangeDatum);
            js.f_i32(e.kind as i32);
            node(js, e.value)?;
        }
        NodeTag::T_PartitionCmd => {
            let e = cast!(r::PartitionCmd);
            range_var(js, e.name)?;
            node(js, e.bound)?;
            js.f_bool(e.concurrent);
        }
        NodeTag::T_RangeTblEntry => {
            let e = cast!(q::RangeTblEntry);
            rte_eref(js, e.eref);
            js.f_u32(e.rtekind as u32);
            js.f_bool(e.inh);
            node(js, e.tablesample)?;
            query_opt(js, e.subquery)?;
            js.f_u32(e.jointype as u32);
            list(js, &e.functions)?;
            js.f_bool(e.funcordinality);
            node(js, e.tablefunc)?;
            list(js, &e.values_lists)?;
            js.f_str(e.ctename);
            js.f_u32(e.ctelevelsup);
            js.f_str(e.enrname);
            list(js, &e.groupexprs)?;
        }
        NodeTag::T_RangeTblFunction => {
            node(js, cast!(q::RangeTblFunction).funcexpr)?;
        }
        NodeTag::T_WithCheckOption => {
            let e = cast!(q::WithCheckOption);
            js.f_u32(e.kind as u32);
            js.f_str(e.relname);
            js.f_str(e.polname);
            node(js, e.qual)?;
            js.f_bool(e.cascaded);
        }
        NodeTag::T_SortGroupClause => {
            let e = cast!(q::SortGroupClause);
            js.f_u32(e.tleSortGroupRef);
            js.f_u32(e.eqop);
            js.f_u32(e.sortop);
            js.f_bool(e.reverse_sort);
            js.f_bool(e.nulls_first);
        }
        NodeTag::T_GroupingSet => {
            list(js, &cast!(q::GroupingSet).content)?;
        }
        NodeTag::T_WindowClause => {
            let e = cast!(q::WindowClause);
            list(js, &e.partitionClause)?;
            list(js, &e.orderClause)?;
            js.f_i32(e.frameOptions);
            node(js, e.startOffset)?;
            node(js, e.endOffset)?;
            js.f_u32(e.winref);
        }
        NodeTag::T_RowMarkClause => {
            let e = cast!(q::RowMarkClause);
            js.f_u32(e.rti);
            js.f_u32(e.strength as u32);
            js.f_u32(e.waitPolicy as u32);
            js.f_bool(e.pushedDown);
        }
        NodeTag::T_WithClause => {
            let e = cast!(q::WithClause);
            list(js, &e.ctes)?;
            js.f_bool(e.recursive);
        }
        NodeTag::T_InferClause => {
            let e = cast!(r::InferClause);
            list(js, &e.indexElems)?;
            node(js, e.whereClause)?;
            js.f_str(e.conname);
        }
        NodeTag::T_OnConflictClause => {
            let e = cast!(r::OnConflictClause);
            js.f_u32(e.action as u32);
            node(js, e.infer)?;
            list(js, &e.targetList)?;
            node(js, e.whereClause)?;
        }
        NodeTag::T_CTESearchClause => {
            let e = cast!(q::CTESearchClause);
            list(js, &e.search_col_list)?;
            js.f_bool(e.search_breadth_first);
            js.f_str(e.search_seq_column);
        }
        NodeTag::T_CTECycleClause => {
            let e = cast!(q::CTECycleClause);
            list(js, &e.cycle_col_list)?;
            js.f_str(e.cycle_mark_column);
            node(js, e.cycle_mark_value)?;
            node(js, e.cycle_mark_default)?;
            js.f_str(e.cycle_path_column);
            js.f_u32(e.cycle_mark_type);
            js.f_i32(e.cycle_mark_typmod);
            js.f_u32(e.cycle_mark_collation);
            js.f_u32(e.cycle_mark_neop);
        }
        NodeTag::T_CommonTableExpr => {
            let e = cast!(q::CommonTableExpr);
            js.f_str(e.ctename);
            js.f_u32(e.ctematerialized as u32);
            node(js, e.ctequery)?;
        }
        NodeTag::T_MergeWhenClause => {
            let e = cast!(r::MergeWhenClause);
            js.f_u32(e.matchKind as u32);
            js.f_u32(e.commandType as u32);
            js.f_u32(e.r#override as u32);
            node(js, e.condition)?;
            list(js, &e.targetList)?;
            list(js, &e.values)?;
        }
        NodeTag::T_ReturningClause => {
            let e = cast!(r::ReturningClause);
            list(js, &e.options)?;
            list(js, &e.exprs)?;
        }
        NodeTag::T_TriggerTransition => {
            let e = cast!(r::TriggerTransition);
            js.f_str(e.name);
            js.f_bool(e.isNew);
            js.f_bool(e.isTable);
        }
        NodeTag::T_JsonOutput => {
            let e = cast!(r::JsonOutput);
            node(js, e.typeName)?;
            json_returning(js, e.returning);
        }
        NodeTag::T_JsonArgument => {
            let e = cast!(r::JsonArgument);
            node(js, e.val)?;
            js.f_str(e.name);
        }
        NodeTag::T_JsonFuncExpr => {
            let e = cast!(r::JsonFuncExpr);
            js.f_u32(e.op as u32);
            js.f_str(e.column_name);
            node(js, e.context_item)?;
            node(js, e.pathspec)?;
            list(js, &e.passing)?;
            node(js, e.output)?;
            node(js, e.on_empty)?;
            node(js, e.on_error)?;
            js.f_u32(e.wrapper as u32);
            js.f_u32(e.quotes as u32);
        }
        NodeTag::T_JsonKeyValue => {
            let e = cast!(r::JsonKeyValue);
            node(js, e.key)?;
            node(js, e.value)?;
        }
        NodeTag::T_JsonParseExpr => {
            let e = cast!(r::JsonParseExpr);
            node(js, e.expr)?;
            node(js, e.output)?;
            js.f_bool(e.unique_keys);
        }
        NodeTag::T_JsonScalarExpr => {
            let e = cast!(r::JsonScalarExpr);
            node(js, e.expr)?;
            node(js, e.output)?;
        }
        NodeTag::T_JsonSerializeExpr => {
            let e = cast!(r::JsonSerializeExpr);
            node(js, e.expr)?;
            node(js, e.output)?;
        }
        NodeTag::T_JsonObjectConstructor => {
            let e = cast!(r::JsonObjectConstructor);
            list(js, &e.exprs)?;
            node(js, e.output)?;
            js.f_bool(e.absent_on_null);
            js.f_bool(e.unique);
        }
        NodeTag::T_JsonArrayConstructor => {
            let e = cast!(r::JsonArrayConstructor);
            list(js, &e.exprs)?;
            node(js, e.output)?;
            js.f_bool(e.absent_on_null);
        }
        NodeTag::T_JsonArrayQueryConstructor => {
            let e = cast!(r::JsonArrayQueryConstructor);
            node(js, e.query)?;
            node(js, e.output)?;
            json_format(js, e.format);
            js.f_bool(e.absent_on_null);
        }
        NodeTag::T_JsonAggConstructor => {
            let e = cast!(r::JsonAggConstructor);
            node(js, e.output)?;
            node(js, e.agg_filter)?;
            list(js, &e.agg_order)?;
            node(js, e.over)?;
        }
        NodeTag::T_JsonObjectAgg => {
            let e = cast!(r::JsonObjectAgg);
            node(js, e.constructor)?;
            node(js, e.arg)?;
            js.f_bool(e.absent_on_null);
            js.f_bool(e.unique);
        }
        NodeTag::T_JsonArrayAgg => {
            let e = cast!(r::JsonArrayAgg);
            node(js, e.constructor)?;
            node(js, e.arg)?;
            js.f_bool(e.absent_on_null);
        }
        NodeTag::T_InsertStmt => {
            let e = cast!(r::InsertStmt);
            node(js, e.relation)?;
            list(js, &e.cols)?;
            node(js, e.selectStmt)?;
            node(js, e.onConflictClause)?;
            node(js, e.returningClause)?;
            node(js, e.withClause)?;
            js.f_u32(e.r#override as u32);
        }
        NodeTag::T_DeleteStmt => {
            let e = cast!(r::DeleteStmt);
            node(js, e.relation)?;
            list(js, &e.usingClause)?;
            node(js, e.whereClause)?;
            node(js, e.returningClause)?;
            node(js, e.withClause)?;
        }
        NodeTag::T_UpdateStmt => {
            let e = cast!(r::UpdateStmt);
            node(js, e.relation)?;
            list(js, &e.targetList)?;
            node(js, e.whereClause)?;
            list(js, &e.fromClause)?;
            node(js, e.returningClause)?;
            node(js, e.withClause)?;
        }
        NodeTag::T_MergeStmt => {
            let e = cast!(r::MergeStmt);
            node(js, e.relation)?;
            node(js, e.sourceRelation)?;
            node(js, e.joinCondition)?;
            list(js, &e.mergeWhenClauses)?;
            node(js, e.returningClause)?;
            node(js, e.withClause)?;
        }
        NodeTag::T_SelectStmt => {
            let e = cast!(r::SelectStmt);
            distinct_clause(js, &e.distinctClause)?;
            node(js, e.intoClause)?;
            list(js, &e.targetList)?;
            list(js, &e.fromClause)?;
            node(js, e.whereClause)?;
            list(js, &e.groupClause)?;
            js.f_bool(e.groupDistinct);
            node(js, e.havingClause)?;
            list(js, &e.windowClause)?;
            list(js, &e.valuesLists)?;
            list(js, &e.sortClause)?;
            node(js, e.limitOffset)?;
            node(js, e.limitCount)?;
            js.f_u32(e.limitOption as u32);
            list(js, &e.lockingClause)?;
            node(js, e.withClause)?;
            js.f_u32(e.op as u32);
            js.f_bool(e.all);
            select_stmt(js, e.larg)?;
            select_stmt(js, e.rarg)?;
        }
        NodeTag::T_SetOperationStmt => {
            let e = cast!(q::SetOperationStmt);
            js.f_u32(e.op as u32);
            js.f_bool(e.all);
            node(js, e.larg)?;
            node(js, e.rarg)?;
        }
        NodeTag::T_PLAssignStmt => {
            let e = cast!(r::PLAssignStmt);
            js.f_str(Some(e.name));
            list(js, &e.indirection)?;
            js.f_i32(e.nnames);
            node(js, e.val)?;
        }
        NodeTag::T_CreateSchemaStmt => {
            let e = cast!(q::CreateSchemaStmt);
            js.f_str(e.schemaname);
            node(js, e.authrole)?;
            list(js, &e.schemaElts)?;
            js.f_bool(e.if_not_exists);
        }
        NodeTag::T_AlterTableStmt => {
            let e = cast!(q::AlterTableStmt);
            range_var(js, e.relation)?;
            list(js, &e.cmds)?;
            js.f_u32(e.objtype as u32);
            js.f_bool(e.missing_ok);
        }
        NodeTag::T_AlterTableMoveAllStmt => {
            let e = cast!(q::AlterTableMoveAllStmt);
            js.f_str(e.orig_tablespacename);
            js.f_u32(e.objtype as u32);
            list(js, &e.roles)?;
            js.f_str(e.new_tablespacename);
            js.f_bool(e.nowait);
        }
        NodeTag::T_AlterTableCmd => {
            let e = cast!(q::AlterTableCmd);
            js.f_u32(e.subtype as u32);
            js.f_str(e.name);
            js.f_i16(e.num);
            node(js, e.newowner)?;
            node(js, e.def)?;
            js.f_u32(e.behavior as u32);
            js.f_bool(e.missing_ok);
            js.f_bool(e.recurse);
        }
        NodeTag::T_ATAlterConstraint => {
            let e = cast!(q::ATAlterConstraint);
            js.f_str(e.conname);
            js.f_bool(e.alterEnforceability);
            js.f_bool(e.is_enforced);
            js.f_bool(e.alterDeferrability);
            js.f_bool(e.deferrable);
            js.f_bool(e.initdeferred);
            js.f_bool(e.alterInheritability);
            js.f_bool(e.noinherit);
        }
        NodeTag::T_ReplicaIdentityStmt => {
            let e = cast!(q::ReplicaIdentityStmt);
            js.f_u8(e.identity_type);
            js.f_str(e.name);
        }
        NodeTag::T_AlterCollationStmt => {
            let e = cast!(q::AlterCollationStmt);
            list(js, &e.collname)?;
        }
        NodeTag::T_AlterDomainStmt => {
            let e = cast!(q::AlterDomainStmt);
            js.f_u8(e.subtype);
            list(js, &e.typeName)?;
            js.f_str(e.name);
            node(js, e.def)?;
            js.f_u32(e.behavior as u32);
            js.f_bool(e.missing_ok);
        }
        NodeTag::T_GrantStmt => {
            let e = cast!(q::GrantStmt);
            js.f_bool(e.is_grant);
            js.f_u32(e.targtype as u32);
            js.f_u32(e.objtype as u32);
            list(js, &e.objects)?;
            list(js, &e.privileges)?;
            list(js, &e.grantees)?;
            js.f_bool(e.grant_option);
            role_spec(js, e.grantor);
            js.f_u32(e.behavior as u32);
        }
        NodeTag::T_ObjectWithArgs => {
            let e = cast!(q::ObjectWithArgs);
            list(js, &e.objname)?;
            opt_list(js, &e.objargs)?;
            list(js, &e.objfuncargs)?;
            js.f_bool(e.args_unspecified);
        }
        NodeTag::T_AccessPriv => {
            let e = cast!(q::AccessPriv);
            js.f_str(e.priv_name);
            list(js, &e.cols)?;
        }
        NodeTag::T_GrantRoleStmt => {
            let e = cast!(q::GrantRoleStmt);
            list(js, &e.granted_roles)?;
            list(js, &e.grantee_roles)?;
            js.f_bool(e.is_grant);
            list(js, &e.opt)?;
            role_spec(js, e.grantor);
            js.f_u32(e.behavior as u32);
        }
        NodeTag::T_AlterDefaultPrivilegesStmt => {
            let e = cast!(q::AlterDefaultPrivilegesStmt);
            list(js, &e.options)?;
            grant_stmt(js, e.action)?;
        }
        NodeTag::T_CopyStmt => {
            let e = cast!(q::CopyStmt);
            node(js, e.relation)?;
            node(js, e.query)?;
            list(js, &e.attlist)?;
            js.f_bool(e.is_from);
            js.f_bool(e.is_program);
            js.f_str(e.filename);
            list(js, &e.options)?;
            node(js, e.whereClause)?;
        }
        NodeTag::T_VariableSetStmt => {
            let e = cast!(q::VariableSetStmt);
            js.f_u32(e.kind as u32);
            js.f_str(e.name);
            if e.jumble_args {
                list(js, &e.args)?;
            }
            js.f_bool(e.is_local);
            js.loc(e.location)?;
        }
        NodeTag::T_VariableShowStmt => {
            js.f_str(cast!(q::VariableShowStmt).name);
        }
        NodeTag::T_AlterSystemStmt => {
            variable_set_stmt(js, cast!(q::AlterSystemStmt).setstmt)?;
        }
        NodeTag::T_CreateStmt => {
            create_stmt_fields(js, cast!(r::CreateStmt))?;
        }
        NodeTag::T_Constraint => {
            let e = cast!(r::Constraint);
            js.f_u32(e.contype as u32);
            js.f_str(e.conname);
            js.f_bool(e.deferrable);
            js.f_bool(e.initdeferred);
            js.f_bool(e.is_enforced);
            js.f_bool(e.skip_validation);
            js.f_bool(e.initially_valid);
            js.f_bool(e.is_no_inherit);
            node(js, e.raw_expr)?;
            js.f_str(e.cooked_expr);
            js.f_u8(e.generated_when);
            js.f_u8(e.generated_kind);
            js.f_bool(e.nulls_not_distinct);
            list(js, &e.keys)?;
            js.f_bool(e.without_overlaps);
            list(js, &e.including)?;
            list(js, &e.exclusions)?;
            list(js, &e.options)?;
            js.f_str(e.indexname);
            js.f_str(e.indexspace);
            js.f_bool(false); // reset_default_tblspc: unported field
            js.f_str(e.access_method);
            node(js, e.where_clause)?;
            range_var(js, e.pktable)?;
            list(js, &e.fk_attrs)?;
            list(js, &e.pk_attrs)?;
            js.f_bool(e.fk_with_period);
            js.f_bool(e.pk_with_period);
            js.f_u8(e.fk_matchtype);
            js.f_u8(e.fk_upd_action);
            js.f_u8(e.fk_del_action);
            list(js, &e.fk_del_set_cols)?;
            oid_list(js, &e.old_conpfeqop);
            js.f_u32(e.old_pktable_oid);
        }
        NodeTag::T_CreateTableSpaceStmt => {
            let e = cast!(q::CreateTableSpaceStmt);
            js.f_str(e.tablespacename);
            role_spec(js, e.owner);
            js.f_str(e.location);
            list(js, &e.options)?;
        }
        NodeTag::T_DropTableSpaceStmt => {
            let e = cast!(q::DropTableSpaceStmt);
            js.f_str(e.tablespacename);
            js.f_bool(e.missing_ok);
        }
        NodeTag::T_AlterTableSpaceOptionsStmt => {
            let e = cast!(q::AlterTableSpaceOptionsStmt);
            js.f_str(e.tablespacename);
            list(js, &e.options)?;
            js.f_bool(e.isReset);
        }
        NodeTag::T_CreateExtensionStmt => {
            let e = cast!(r::CreateExtensionStmt);
            js.f_str(e.extname);
            js.f_bool(e.if_not_exists);
            list(js, &e.options)?;
        }
        NodeTag::T_AlterExtensionStmt => {
            let e = cast!(r::AlterExtensionStmt);
            js.f_str(e.extname);
            list(js, &e.options)?;
        }
        NodeTag::T_AlterExtensionContentsStmt => {
            let e = cast!(r::AlterExtensionContentsStmt);
            js.f_str(e.extname);
            js.f_i32(e.action);
            js.f_u32(e.objtype as u32);
            node(js, e.object)?;
        }
        NodeTag::T_CreateFdwStmt => {
            let e = cast!(r::CreateFdwStmt);
            js.f_str(e.fdwname);
            list(js, &e.func_options)?;
            list(js, &e.options)?;
        }
        NodeTag::T_AlterFdwStmt => {
            let e = cast!(r::AlterFdwStmt);
            js.f_str(e.fdwname);
            list(js, &e.func_options)?;
            list(js, &e.options)?;
        }
        NodeTag::T_CreateForeignServerStmt => {
            let e = cast!(r::CreateForeignServerStmt);
            js.f_str(e.servername);
            js.f_str(e.servertype);
            js.f_str(e.version);
            js.f_str(e.fdwname);
            js.f_bool(e.if_not_exists);
            list(js, &e.options)?;
        }
        NodeTag::T_AlterForeignServerStmt => {
            let e = cast!(r::AlterForeignServerStmt);
            js.f_str(e.servername);
            js.f_str(e.version);
            list(js, &e.options)?;
            js.f_bool(e.has_version);
        }
        NodeTag::T_CreateForeignTableStmt => {
            let e = cast!(r::CreateForeignTableStmt);
            create_stmt_fields(js, &e.base)?;
            js.f_str(e.servername);
            list(js, &e.options)?;
        }
        NodeTag::T_CreateUserMappingStmt => {
            let e = cast!(r::CreateUserMappingStmt);
            role_spec(js, e.user);
            js.f_str(e.servername);
            js.f_bool(e.if_not_exists);
            list(js, &e.options)?;
        }
        NodeTag::T_AlterUserMappingStmt => {
            let e = cast!(r::AlterUserMappingStmt);
            role_spec(js, e.user);
            js.f_str(e.servername);
            list(js, &e.options)?;
        }
        NodeTag::T_DropUserMappingStmt => {
            let e = cast!(r::DropUserMappingStmt);
            role_spec(js, e.user);
            js.f_str(e.servername);
            js.f_bool(e.missing_ok);
        }
        NodeTag::T_ImportForeignSchemaStmt => {
            let e = cast!(r::ImportForeignSchemaStmt);
            js.f_str(e.server_name);
            js.f_str(e.remote_schema);
            js.f_str(e.local_schema);
            js.f_u32(e.list_type as u32);
            list(js, &e.table_list)?;
            list(js, &e.options)?;
        }
        NodeTag::T_CreatePolicyStmt => {
            let e = cast!(q::CreatePolicyStmt);
            js.f_str(e.policy_name);
            range_var(js, e.table)?;
            js.f_str(e.cmd_name);
            js.f_bool(e.permissive);
            list(js, &e.roles)?;
            node(js, e.qual)?;
            node(js, e.with_check)?;
        }
        NodeTag::T_AlterPolicyStmt => {
            let e = cast!(q::AlterPolicyStmt);
            js.f_str(e.policy_name);
            range_var(js, e.table)?;
            list(js, &e.roles)?;
            node(js, e.qual)?;
            node(js, e.with_check)?;
        }
        NodeTag::T_CreateAmStmt => {
            let e = cast!(q::CreateAmStmt);
            js.f_str(e.amname);
            list(js, &e.handler_name)?;
            js.f_u8(e.amtype);
        }
        NodeTag::T_CreateTrigStmt => {
            let e = cast!(r::CreateTrigStmt);
            js.f_bool(e.replace);
            js.f_bool(e.isconstraint);
            js.f_str(e.trigname);
            range_var(js, e.relation)?;
            list(js, &e.funcname)?;
            list(js, &e.args)?;
            js.f_bool(e.row);
            js.f_i16(e.timing);
            js.f_i16(e.events);
            list(js, &e.columns)?;
            node(js, e.whenClause)?;
            list(js, &e.transitionRels)?;
            js.f_bool(e.deferrable);
            js.f_bool(e.initdeferred);
            range_var(js, e.constrrel)?;
        }
        NodeTag::T_CreateEventTrigStmt => {
            let e = cast!(q::CreateEventTrigStmt);
            js.f_str(e.trigname);
            js.f_str(e.eventname);
            list(js, &e.whenclause)?;
            list(js, &e.funcname)?;
        }
        NodeTag::T_AlterEventTrigStmt => {
            let e = cast!(q::AlterEventTrigStmt);
            js.f_str(e.trigname);
            js.f_i8(e.tgenabled);
        }
        NodeTag::T_CreateRoleStmt => {
            let e = cast!(q::CreateRoleStmt);
            js.f_u32(e.stmt_type as u32);
            js.f_str(e.role);
            list(js, &e.options)?;
        }
        NodeTag::T_AlterRoleStmt => {
            let e = cast!(q::AlterRoleStmt);
            role_spec(js, Some(e.role));
            list(js, &e.options)?;
            js.f_i32(e.action);
        }
        NodeTag::T_AlterRoleSetStmt => {
            let e = cast!(q::AlterRoleSetStmt);
            role_spec(js, e.role);
            js.f_str(e.database);
            variable_set_stmt(js, e.setstmt)?;
        }
        NodeTag::T_DropRoleStmt => {
            let e = cast!(q::DropRoleStmt);
            list(js, &e.roles)?;
            js.f_bool(e.missing_ok);
        }
        NodeTag::T_CreateSeqStmt => {
            let e = cast!(r::CreateSeqStmt);
            range_var(js, e.sequence)?;
            list(js, &e.options)?;
            js.f_u32(e.ownerId);
            js.f_bool(e.for_identity);
            js.f_bool(e.if_not_exists);
        }
        NodeTag::T_AlterSeqStmt => {
            let e = cast!(r::AlterSeqStmt);
            range_var(js, e.sequence)?;
            list(js, &e.options)?;
            js.f_bool(e.for_identity);
            js.f_bool(e.missing_ok);
        }
        NodeTag::T_DefineStmt => {
            let e = cast!(q::DefineStmt);
            js.f_u32(e.kind as u32);
            js.f_bool(e.oldstyle);
            list(js, &e.defnames)?;
            list(js, &e.args)?;
            list(js, &e.definition)?;
            js.f_bool(e.if_not_exists);
            js.f_bool(e.replace);
        }
        NodeTag::T_CreateDomainStmt => {
            let e = cast!(r::CreateDomainStmt);
            list(js, &e.domainname)?;
            node(js, e.typeName)?;
            node(js, e.collClause)?;
            list(js, &e.constraints)?;
        }
        // Arms below surfaced by recovery TAP 027_stream_regress, which runs
        // the whole core regress suite with compute_query_id='regress' +
        // pg_stat_statements (jumbling forced for EVERY statement). Field
        // lists transcribed from C's generated queryjumblefuncs.funcs.c.
        NodeTag::T_CreateRangeStmt => {
            let e = cast!(r::CreateRangeStmt);
            list(js, &e.typeName)?;
            list(js, &e.params)?;
        }
        NodeTag::T_AlterTypeStmt => {
            let e = cast!(r::AlterTypeStmt);
            list(js, &e.typeName)?;
            list(js, &e.options)?;
        }
        NodeTag::T_AlterStatsStmt => {
            let e = cast!(r::AlterStatsStmt);
            list(js, &e.defnames)?;
            node(js, e.stxstattarget)?;
            js.f_bool(e.missing_ok);
        }
        NodeTag::T_CreateConversionStmt => {
            let e = cast!(q::CreateConversionStmt);
            list(js, &e.conversion_name)?;
            js.f_str(e.for_encoding_name);
            js.f_str(e.to_encoding_name);
            list(js, &e.func_name)?;
            js.f_bool(e.def);
        }
        NodeTag::T_CreatePLangStmt => {
            let e = cast!(q::CreatePLangStmt);
            js.f_bool(e.replace);
            js.f_str(e.plname);
            list(js, &e.plhandler)?;
            list(js, &e.plinline)?;
            list(js, &e.plvalidator)?;
            js.f_bool(e.pltrusted);
        }
        NodeTag::T_SecLabelStmt => {
            let e = cast!(q::SecLabelStmt);
            js.f_u32(e.objtype as u32);
            node(js, e.object)?;
            js.f_str(e.provider);
            js.f_str(e.label);
        }
        NodeTag::T_ReturnStmt => {
            let e = cast!(q::ReturnStmt);
            node(js, e.returnval)?;
        }
        NodeTag::T_TableSampleClause => {
            let e = cast!(q::TableSampleClause);
            js.f_u32(e.tsmhandler);
            list(js, &e.args)?;
            node(js, e.repeatable)?;
        }
        NodeTag::T_RangeTableSample => {
            let e = cast!(r::RangeTableSample);
            node(js, e.relation)?;
            list(js, &e.method)?;
            list(js, &e.args)?;
            node(js, e.repeatable)?;
        }
        NodeTag::T_MultiAssignRef => {
            let e = cast!(r::MultiAssignRef);
            node(js, e.source)?;
            js.f_i32(e.colno);
            js.f_i32(e.ncolumns);
        }
        NodeTag::T_ReturningOption => {
            let e = cast!(r::ReturningOption);
            js.f_u32(e.option as u32);
            js.f_str(e.value);
        }
        NodeTag::T_JsonTable => {
            let e = cast!(r::JsonTable);
            node(js, e.context_item)?;
            node(js, e.pathspec)?;
            list(js, &e.passing)?;
            list(js, &e.columns)?;
            node(js, e.on_error)?;
            alias(js, e.alias)?;
            js.f_bool(e.lateral);
        }
        NodeTag::T_JsonTableColumn => {
            let e = cast!(r::JsonTableColumn);
            js.f_u32(e.coltype as u32);
            js.f_str(e.name);
            node(js, e.typeName)?;
            node(js, e.pathspec)?;
            json_format(js, e.format);
            js.f_u32(e.wrapper as u32);
            js.f_u32(e.quotes as u32);
            list(js, &e.columns)?;
            node(js, e.on_empty)?;
            node(js, e.on_error)?;
        }
        NodeTag::T_JsonTablePathSpec => {
            let e = cast!(r::JsonTablePathSpec);
            node(js, e.string)?;
            js.f_str(e.name);
        }
        NodeTag::T_CreateOpClassStmt => {
            let e = cast!(q::CreateOpClassStmt);
            list(js, &e.opclassname)?;
            list(js, &e.opfamilyname)?;
            js.f_str(e.amname);
            node(js, e.datatype)?;
            list(js, &e.items)?;
            js.f_bool(e.isDefault);
        }
        NodeTag::T_CreateOpClassItem => {
            let e = cast!(q::CreateOpClassItem);
            js.f_i32(e.itemtype);
            node(js, e.name)?;
            js.f_i32(e.number);
            list(js, &e.order_family)?;
            list(js, &e.class_args)?;
            node(js, e.storedtype)?;
        }
        NodeTag::T_CreateOpFamilyStmt => {
            let e = cast!(q::CreateOpFamilyStmt);
            list(js, &e.opfamilyname)?;
            js.f_str(e.amname);
        }
        NodeTag::T_AlterOpFamilyStmt => {
            let e = cast!(q::AlterOpFamilyStmt);
            list(js, &e.opfamilyname)?;
            js.f_str(e.amname);
            js.f_bool(e.isDrop);
            list(js, &e.items)?;
        }
        NodeTag::T_DropStmt => {
            let e = cast!(q::DropStmt);
            list(js, &e.objects)?;
            js.f_u32(e.removeType as u32);
            js.f_u32(e.behavior as u32);
            js.f_bool(e.missing_ok);
            js.f_bool(e.concurrent);
        }
        NodeTag::T_TruncateStmt => {
            let e = cast!(q::TruncateStmt);
            list(js, &e.relations)?;
            js.f_bool(e.restart_seqs);
            js.f_u32(e.behavior as u32);
        }
        NodeTag::T_CommentStmt => {
            let e = cast!(q::CommentStmt);
            js.f_u32(e.objtype as u32);
            node(js, e.object)?;
            js.f_str(e.comment);
        }
        NodeTag::T_DeclareCursorStmt => {
            let e = cast!(q::DeclareCursorStmt);
            js.f_str(e.portalname);
            js.f_i32(e.options);
            node(js, e.query)?;
        }
        NodeTag::T_ClosePortalStmt => {
            js.f_str(cast!(q::ClosePortalStmt).portalname);
        }
        NodeTag::T_FetchStmt => {
            let e = cast!(q::FetchStmt);
            js.f_u32(e.direction as u32);
            js.f_i64(e.howMany);
            js.f_str(e.portalname);
            js.f_bool(e.ismove);
        }
        NodeTag::T_IndexStmt => {
            let e = cast!(r::IndexStmt);
            js.f_str(e.idxname);
            range_var(js, e.relation)?;
            js.f_str(e.accessMethod);
            js.f_str(e.tableSpace);
            list(js, &e.indexParams)?;
            list(js, &e.indexIncludingParams)?;
            list(js, &e.options)?;
            node(js, e.whereClause)?;
            list(js, &e.excludeOpNames)?;
            js.f_str(e.idxcomment);
            js.f_u32(e.indexOid);
            js.f_u32(e.oldNumber);
            js.f_u32(e.oldCreateSubid);
            js.f_u32(e.oldFirstRelfilelocatorSubid);
            js.f_bool(e.unique);
            js.f_bool(e.nulls_not_distinct);
            js.f_bool(e.primary);
            js.f_bool(e.isconstraint);
            js.f_bool(e.iswithoutoverlaps);
            js.f_bool(e.deferrable);
            js.f_bool(e.initdeferred);
            js.f_bool(e.transformed);
            js.f_bool(e.concurrent);
            js.f_bool(e.if_not_exists);
            js.f_bool(e.reset_default_tblspc);
        }
        NodeTag::T_CreateStatsStmt => {
            let e = cast!(r::CreateStatsStmt);
            list(js, &e.defnames)?;
            list(js, &e.stat_types)?;
            list(js, &e.exprs)?;
            list(js, &e.relations)?;
            js.f_str(e.stxcomment);
            js.f_bool(e.transformed);
            js.f_bool(e.if_not_exists);
        }
        NodeTag::T_StatsElem => {
            let e = cast!(r::StatsElem);
            js.f_str(e.name);
            node(js, e.expr)?;
        }
        NodeTag::T_CreateFunctionStmt => {
            let e = cast!(q::CreateFunctionStmt);
            js.f_bool(e.is_procedure);
            js.f_bool(e.replace);
            list(js, &e.funcname)?;
            list(js, &e.parameters)?;
            node(js, e.returnType)?;
            list(js, &e.options)?;
            node(js, e.sql_body)?;
        }
        NodeTag::T_FunctionParameter => {
            let e = cast!(q::FunctionParameter);
            js.f_str(e.name);
            node(js, e.argType)?;
            js.f_u32(e.mode as u32);
            node(js, e.defexpr)?;
        }
        NodeTag::T_AlterFunctionStmt => {
            let e = cast!(q::AlterFunctionStmt);
            js.f_u32(e.objtype as u32);
            object_with_args(js, e.func)?;
            list(js, &e.actions)?;
        }
        NodeTag::T_DoStmt => {
            list(js, &cast!(q::DoStmt).args)?;
        }
        NodeTag::T_CallStmt => {
            let e = cast!(r::CallStmt);
            func_expr(js, e.funcexpr)?;
            list(js, &e.outargs)?;
        }
        NodeTag::T_RenameStmt => {
            let e = cast!(q::RenameStmt);
            js.f_u32(e.renameType as u32);
            js.f_u32(e.relationType as u32);
            range_var(js, e.relation)?;
            node(js, e.object)?;
            js.f_str(e.subname);
            js.f_str(e.newname);
            js.f_u32(e.behavior as u32);
            js.f_bool(e.missing_ok);
        }
        NodeTag::T_AlterObjectSchemaStmt => {
            let e = cast!(q::AlterObjectSchemaStmt);
            js.f_u32(e.objectType as u32);
            range_var(js, e.relation)?;
            node(js, e.object)?;
            js.f_str(e.newschema);
            js.f_bool(e.missing_ok);
        }
        NodeTag::T_AlterOwnerStmt => {
            let e = cast!(q::AlterOwnerStmt);
            js.f_u32(e.objectType as u32);
            range_var(js, e.relation)?;
            node(js, e.object)?;
            role_spec(js, e.newowner);
        }
        NodeTag::T_AlterOperatorStmt => {
            let e = cast!(q::AlterOperatorStmt);
            node(js, e.opername)?;
            list(js, &e.options)?;
        }
        NodeTag::T_RuleStmt => {
            let e = cast!(r::RuleStmt);
            range_var(js, e.relation)?;
            js.f_str(Some(e.rulename));
            node(js, e.whereClause)?;
            js.f_u32(e.event as u32);
            js.f_bool(e.instead);
            list(js, &e.actions)?;
            js.f_bool(e.replace);
        }
        NodeTag::T_NotifyStmt => {
            let e = cast!(q::NotifyStmt);
            js.f_str(e.conditionname);
            js.f_str(e.payload);
        }
        NodeTag::T_ListenStmt => {
            js.f_str(cast!(q::ListenStmt).conditionname);
        }
        NodeTag::T_UnlistenStmt => {
            js.f_str(cast!(q::UnlistenStmt).conditionname);
        }
        NodeTag::T_TransactionStmt => {
            let e = cast!(q::TransactionStmt);
            js.f_u32(e.kind as u32);
            list(js, &e.options)?;
            js.f_bool(e.chain);
            js.loc(e.location)?;
        }
        NodeTag::T_CompositeTypeStmt => {
            let e = cast!(r::CompositeTypeStmt);
            range_var(js, e.typevar)?;
            list(js, &e.coldeflist)?;
        }
        NodeTag::T_CreateEnumStmt => {
            let e = cast!(r::CreateEnumStmt);
            list(js, &e.typeName)?;
            list(js, &e.vals)?;
        }
        NodeTag::T_AlterEnumStmt => {
            let e = cast!(r::AlterEnumStmt);
            list(js, &e.typeName)?;
            js.f_str(e.oldVal);
            js.f_str(e.newVal);
            js.f_str(e.newValNeighbor);
            js.f_bool(e.newValIsAfter);
            js.f_bool(e.skipIfNewValExists);
        }
        NodeTag::T_ViewStmt => {
            let e = cast!(r::ViewStmt);
            range_var(js, e.view)?;
            list(js, &e.aliases)?;
            node(js, e.query)?;
            js.f_bool(e.replace);
            list(js, &e.options)?;
            js.f_u32(e.withCheckOption as u32);
        }
        NodeTag::T_LoadStmt => {
            js.f_str(Some(cast!(q::LoadStmt).filename));
        }
        NodeTag::T_CreatedbStmt => {
            let e = cast!(q::CreatedbStmt);
            js.f_str(e.dbname);
            list(js, &e.options)?;
        }
        NodeTag::T_AlterDatabaseStmt => {
            let e = cast!(q::AlterDatabaseStmt);
            js.f_str(e.dbname);
            list(js, &e.options)?;
        }
        NodeTag::T_AlterDatabaseRefreshCollStmt => {
            js.f_str(cast!(q::AlterDatabaseRefreshCollStmt).dbname);
        }
        NodeTag::T_AlterDatabaseSetStmt => {
            let e = cast!(q::AlterDatabaseSetStmt);
            js.f_str(e.dbname);
            node(js, e.setstmt)?;
        }
        NodeTag::T_DropdbStmt => {
            let e = cast!(q::DropdbStmt);
            js.f_str(e.dbname);
            js.f_bool(e.missing_ok);
            list(js, &e.options)?;
        }
        NodeTag::T_ClusterStmt => {
            let e = cast!(q::ClusterStmt);
            node(js, e.relation)?;
            js.f_str(e.indexname);
            list(js, &e.params)?;
        }
        NodeTag::T_VacuumStmt => {
            let e = cast!(q::VacuumStmt);
            list(js, &e.options)?;
            list(js, &e.rels)?;
            js.f_bool(e.is_vacuumcmd);
        }
        NodeTag::T_VacuumRelation => {
            let e = cast!(q::VacuumRelation);
            node(js, e.relation)?;
            js.f_u32(e.oid);
            list(js, &e.va_cols)?;
        }
        NodeTag::T_ExplainStmt => {
            let e = cast!(q::ExplainStmt);
            node(js, e.query)?;
            list(js, &e.options)?;
        }
        NodeTag::T_CreateTableAsStmt => {
            let e = cast!(r::CreateTableAsStmt);
            node(js, e.query)?;
            node(js, e.into)?;
            js.f_u32(e.objtype as u32);
            js.f_bool(e.is_select_into);
            js.f_bool(e.if_not_exists);
        }
        NodeTag::T_RefreshMatViewStmt => {
            let e = cast!(r::RefreshMatViewStmt);
            js.f_bool(e.concurrent);
            js.f_bool(e.skipData);
            range_var(js, e.relation)?;
        }
        NodeTag::T_CheckPointStmt => {}
        NodeTag::T_DiscardStmt => {
            js.f_u32(cast!(q::DiscardStmt).target as u32);
        }
        NodeTag::T_LockStmt => {
            let e = cast!(q::LockStmt);
            list(js, &e.relations)?;
            js.f_i32(e.mode);
            js.f_bool(e.nowait);
        }
        NodeTag::T_ConstraintsSetStmt => {
            let e = cast!(r::ConstraintsSetStmt);
            list(js, &e.constraints)?;
            js.f_bool(e.deferred);
        }
        NodeTag::T_ReindexStmt => {
            let e = cast!(q::ReindexStmt);
            js.f_u32(e.kind as u32);
            node(js, e.relation)?;
            js.f_str(e.name);
            list(js, &e.params)?;
        }
        NodeTag::T_CreateCastStmt => {
            let e = cast!(q::CreateCastStmt);
            node(js, e.sourcetype)?;
            node(js, e.targettype)?;
            node(js, e.func)?;
            js.f_u32(e.context as u32);
            js.f_bool(e.inout);
        }
        NodeTag::T_CreateTransformStmt => {
            let e = cast!(q::CreateTransformStmt);
            js.f_bool(e.replace);
            node(js, e.type_name)?;
            js.f_str(e.lang);
            node(js, e.fromsql)?;
            node(js, e.tosql)?;
        }
        NodeTag::T_PrepareStmt => {
            let e = cast!(q::PrepareStmt);
            js.f_str(e.name);
            list(js, &e.argtypes)?;
            node(js, e.query)?;
        }
        NodeTag::T_ExecuteStmt => {
            let e = cast!(q::ExecuteStmt);
            js.f_str(e.name);
            list(js, &e.params)?;
        }
        NodeTag::T_DeallocateStmt => {
            let e = cast!(q::DeallocateStmt);
            js.f_bool(e.isall);
            js.loc(e.location)?;
        }
        NodeTag::T_DropOwnedStmt => {
            let e = cast!(q::DropOwnedStmt);
            list(js, &e.roles)?;
            js.f_u32(e.behavior as u32);
        }
        NodeTag::T_ReassignOwnedStmt => {
            let e = cast!(q::ReassignOwnedStmt);
            list(js, &e.roles)?;
            role_spec(js, Some(e.newrole));
        }
        NodeTag::T_AlterTSDictionaryStmt => {
            let e = cast!(r::AlterTSDictionaryStmt);
            list(js, &e.dictname)?;
            list(js, &e.options)?;
        }
        NodeTag::T_AlterTSConfigurationStmt => {
            let e = cast!(r::AlterTSConfigurationStmt);
            js.f_u32(e.kind as u32);
            list(js, &e.cfgname)?;
            list(js, &e.tokentype)?;
            list(js, &e.dicts)?;
            js.f_bool(e.r#override);
            js.f_bool(e.replace);
            js.f_bool(e.missing_ok);
        }
        NodeTag::T_PublicationTable => {
            let e = cast!(q::PublicationTable);
            range_var(js, e.relation)?;
            node(js, e.whereClause)?;
            list(js, &e.columns)?;
        }
        NodeTag::T_PublicationObjSpec => {
            let e = cast!(q::PublicationObjSpec);
            js.f_u32(e.pubobjtype as u32);
            js.f_str(e.name);
            publication_table(js, e.pubtable)?;
        }
        NodeTag::T_CreatePublicationStmt => {
            let e = cast!(q::CreatePublicationStmt);
            js.f_str(e.pubname);
            list(js, &e.options)?;
            list(js, &e.pubobjects)?;
            js.f_bool(e.for_all_tables);
        }
        NodeTag::T_AlterPublicationStmt => {
            let e = cast!(q::AlterPublicationStmt);
            js.f_str(e.pubname);
            list(js, &e.options)?;
            list(js, &e.pubobjects)?;
            js.f_bool(e.for_all_tables);
            js.f_u32(e.action as u32);
        }
        NodeTag::T_CreateSubscriptionStmt => {
            let e = cast!(q::CreateSubscriptionStmt);
            js.f_str(e.subname);
            js.f_str(e.conninfo);
            list(js, &e.publication)?;
            list(js, &e.options)?;
        }
        NodeTag::T_AlterSubscriptionStmt => {
            let e = cast!(q::AlterSubscriptionStmt);
            js.f_u32(e.kind as u32);
            js.f_str(e.subname);
            js.f_str(e.conninfo);
            list(js, &e.publication)?;
            list(js, &e.options)?;
        }
        NodeTag::T_DropSubscriptionStmt => {
            let e = cast!(q::DropSubscriptionStmt);
            js.f_str(e.subname);
            js.f_bool(e.missing_ok);
            js.f_u32(e.behavior as u32);
        }
        NodeTag::T_Integer => {
            js.f_i32(n.as_integer().unwrap().ival);
        }
        NodeTag::T_Float => {
            js.f_str(Some(n.as_float().unwrap().fval));
        }
        NodeTag::T_Boolean => {
            js.f_bool(n.as_boolean().unwrap().boolval);
        }
        NodeTag::T_String => {
            js.f_str(Some(n.as_string().unwrap().sval));
        }
        NodeTag::T_BitString => {
            js.f_str(Some(n.as_bitstring().unwrap().bsval));
        }
        // _jumbleList: the tag was already emitted above; only the cells
        // follow.
        NodeTag::T_List => {
            for c in n.as_list().unwrap().iter() {
                jumble_node(js, c)?;
            }
        }
        NodeTag::T_IntList => {
            for v in n.as_int_list().unwrap().iter() {
                js.f_i32(v);
            }
        }
        NodeTag::T_OidList => {
            for v in n.as_oid_list().unwrap().iter() {
                js.f_u32(v);
            }
        }
        NodeTag::T_XidList => {
            for v in n.as_xid_list().unwrap().iter() {
                js.f_u32(v);
            }
        }
        _ => jumble_gap(t),
    }
    Ok(())
}
