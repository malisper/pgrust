use lsyscache::{func_parallel, func_strict, func_volatile, get_func_leakproof};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::primnodes::{Param, ParamKind, ScalarArrayOpExpr};
use types_nodes::{Bitmapset, Node, NodeTag};

use crate::walker::{
    check_functions_in_node, deferred, expression_tree_walker, query_tree_walker, NodeWalker,
};

pub const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;
pub const PROVOLATILE_STABLE: i8 = b's' as i8;
pub const PROVOLATILE_VOLATILE: i8 = b'v' as i8;
pub const PROPARALLEL_SAFE: i8 = b's' as i8;
pub const PROPARALLEL_RESTRICTED: i8 = b'r' as i8;
pub const PROPARALLEL_UNSAFE: i8 = b'u' as i8;
// pg_proc.dat oid 1574: nextval(regclass).
pub const F_NEXTVAL: Oid = 1574;

struct ContainAgg;

impl<'mcx> NodeWalker<'mcx> for ContainAgg {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Aggref | NodeTag::T_GroupingFunc => Ok(true),
            _ => expression_tree_walker(node, self),
        }
    }
}

pub fn contain_agg_clause(clause: Node<'_>) -> PgResult<bool> {
    ContainAgg.visit(clause)
}

struct ContainWindowFunc;

impl<'mcx> NodeWalker<'mcx> for ContainWindowFunc {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_WindowFunc => Ok(true),
            _ => expression_tree_walker(node, self),
        }
    }
}

pub fn contain_window_function(clause: Node<'_>) -> PgResult<bool> {
    ContainWindowFunc.visit(clause)
}

// expression_tree_walker's T_GroupingFunc arm (nodeFuncs.c) hosted per-crate:
// walk args only; refs/cols carry no expressions.
fn walk_grouping_func_args<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    node: Node<'mcx>,
    w: &mut W,
) -> PgResult<bool> {
    for a in &node.as_grouping_func().unwrap().args {
        if w.visit(a)? {
            return Ok(true);
        }
    }
    Ok(false)
}

struct ContainSubplans;

impl<'mcx> NodeWalker<'mcx> for ContainSubplans {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_SubPlan | NodeTag::T_AlternativeSubPlan | NodeTag::T_SubLink => Ok(true),
            NodeTag::T_GroupingFunc => walk_grouping_func_args(node, self),
            _ => expression_tree_walker(node, self),
        }
    }
}

pub fn contain_subplans(clause: Node<'_>) -> PgResult<bool> {
    ContainSubplans.visit(clause)
}

struct ContainMutable;

impl<'mcx> NodeWalker<'mcx> for ContainMutable {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if check_functions_in_node(node, &mut |f| {
            Ok(func_volatile(f)? != PROVOLATILE_IMMUTABLE)
        })? {
            return Ok(true);
        }
        match node.node_tag() {
            NodeTag::T_JsonConstructorExpr => {
                let c = node.as_json_constructor_expr().unwrap();
                let is_jsonb = c.returning.expect("returning").format.expect("format").format_type
                    == types_nodes::primnodes::JsonFormatType::JS_FORMAT_JSONB;
                for arg in &c.args {
                    let typid = nodes_core::node_funcs::expr_type(arg);
                    let immutable = if is_jsonb {
                        adt_jsonb::tojsonb::to_jsonb_is_immutable(typid)?
                    } else {
                        adt_json::tojson::to_json_is_immutable(typid)?
                    };
                    if !immutable {
                        return Ok(true);
                    }
                }
                expression_tree_walker(node, self)
            }
            // C clauses.c:416: a non-Const path is mutable; a null path const
            // is immutable with no subnode recursion; else jspIsMutable over
            // the PASSING variables' expression types.
            NodeTag::T_JsonExpr => {
                let je = node.as_json_expr().unwrap();
                let path = je.path_spec.expect("JsonExpr.path_spec");
                let Some(cnst) = path.as_const() else {
                    return Ok(true);
                };
                debug_assert_eq!(cnst.consttype, 4072, "path_spec is a jsonpath Const");
                if cnst.constisnull {
                    return Ok(false);
                }
                let p = cnst.constvalue.as_usize() as *const u8;
                // Parse-built jsonpath Consts are plain 4B varlenas
                // (jsonpath_in output; never short/toast).
                assert!(
                    // SAFETY: live by-ref varlena datum, header readable.
                    unsafe { *p } & 0x03 == 0,
                    "jsonpath Const with a non-4B varlena header"
                );
                // SAFETY: 4B-header varlena readable for its VARSIZE.
                let image = unsafe { datum::VarlenaRef::from_ptr(p) }.as_bytes();
                let mut vars: Vec<(&[u8], Oid)> = Vec::with_capacity(je.passing_names.len());
                for (name, value) in je.passing_names.iter().zip(je.passing_values.iter()) {
                    vars.push((
                        name.as_string().expect("passing name is a String node").sval.as_bytes(),
                        nodes_core::node_funcs::expr_type(value),
                    ));
                }
                if adt_jsonpath::mutability::jsp_is_mutable(image, &vars)? {
                    return Ok(true);
                }
                expression_tree_walker(node, self)
            }
            // All SQLValueFunction variants are stable; NextValueExpr volatile.
            NodeTag::T_SQLValueFunction | NodeTag::T_NextValueExpr => Ok(true),
            NodeTag::T_GroupingFunc => walk_grouping_func_args(node, self),
            NodeTag::T_Query => query_tree_walker(node.as_query().unwrap(), self, 0),
            _ => expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(
        &mut self,
        q: &'mcx types_nodes::parsenodes::Query<'mcx>,
    ) -> PgResult<bool> {
        query_tree_walker(q, self, 0)
    }
}

pub fn contain_mutable_functions(clause: Node<'_>) -> PgResult<bool> {
    ContainMutable.visit(clause)
}

// expression_planner reduces to eval_const_expressions on this lane, which
// reaches inline_function (fold.rs) exactly as C does (clauses.c:488-495).
pub fn contain_mutable_functions_after_planning<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    expr: Node<'mcx>,
) -> PgResult<bool> {
    let planned = crate::eval_const_expressions(mcx, expr)?;
    contain_mutable_functions(planned)
}

struct ContainVolatile {
    not_nextval: bool,
}

impl<'mcx> NodeWalker<'mcx> for ContainVolatile {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        let not_nextval = self.not_nextval;
        if check_functions_in_node(node, &mut |f| {
            Ok(!(not_nextval && f == F_NEXTVAL) && func_volatile(f)? == PROVOLATILE_VOLATILE)
        })? {
            return Ok(true);
        }
        match node.node_tag() {
            NodeTag::T_NextValueExpr if !self.not_nextval => Ok(true),
            // C caches the verdict on these nodes (has_volatile /
            // has_volatile_expr) — a port requirement at their owning units.
            t @ (NodeTag::T_RestrictInfo | NodeTag::T_PathTarget) => {
                deferred("contain_volatile_functions: volatility cache", t)
            }
            NodeTag::T_GroupingFunc => walk_grouping_func_args(node, self),
            NodeTag::T_Query => query_tree_walker(node.as_query().unwrap(), self, 0),
            _ => expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(
        &mut self,
        q: &'mcx types_nodes::parsenodes::Query<'mcx>,
    ) -> PgResult<bool> {
        query_tree_walker(q, self, 0)
    }
}

pub fn contain_volatile_functions(clause: Node<'_>) -> PgResult<bool> {
    ContainVolatile { not_nextval: false }.visit(clause)
}

pub fn contain_volatile_functions_not_nextval(clause: Node<'_>) -> PgResult<bool> {
    ContainVolatile { not_nextval: true }.visit(clause)
}

pub fn contain_volatile_functions_after_planning(_expr: Node<'_>) -> PgResult<bool> {
    panic!("contain_volatile_functions_after_planning deferred: expression_planner unported");
}

struct MaxParallelHazard {
    max_hazard: i8,
    max_interesting: i8,
    safe_param_ids: Vec<i32>,
}

impl MaxParallelHazard {
    fn test(&mut self, proparallel: i8) -> bool {
        test_hazard(proparallel, self.max_interesting, &mut self.max_hazard)
    }
}

fn test_hazard(proparallel: i8, max_interesting: i8, max_hazard: &mut i8) -> bool {
    match proparallel {
        PROPARALLEL_SAFE => false,
        PROPARALLEL_RESTRICTED => {
            debug_assert!(*max_hazard != PROPARALLEL_UNSAFE);
            *max_hazard = proparallel;
            max_interesting == proparallel
        }
        PROPARALLEL_UNSAFE => {
            *max_hazard = proparallel;
            true
        }
        other => panic!("unrecognized proparallel value \"{}\"", other as u8 as char),
    }
}

impl<'mcx> NodeWalker<'mcx> for MaxParallelHazard {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        let (mi, mh) = (self.max_interesting, &mut self.max_hazard);
        if check_functions_in_node(node, &mut |f| {
            Ok(test_hazard(func_parallel(f)?, mi, mh))
        })? {
            return Ok(true);
        }
        match node.node_tag() {
            // Tag verdict first, then C recurses into payload children we
            // cannot reach yet — the walker's deferred arm keeps that loud.
            NodeTag::T_CoerceToDomain | NodeTag::T_WindowFunc | NodeTag::T_SubLink => {
                if self.test(PROPARALLEL_RESTRICTED) {
                    return Ok(true);
                }
                expression_tree_walker(node, self)
            }
            NodeTag::T_NextValueExpr => Ok(self.test(PROPARALLEL_UNSAFE)),
            NodeTag::T_SubPlan => {
                // The subplan's output params are safe within its testexpr
                // (and only there); args get no such exemption.
                let sp = node.as_sub_plan().unwrap();
                if !sp.parallel_safe && self.test(PROPARALLEL_RESTRICTED) {
                    return Ok(true);
                }
                let save_len = self.safe_param_ids.len();
                self.safe_param_ids.extend(sp.paramIds.iter());
                if let Some(testexpr) = sp.testexpr {
                    if self.visit(testexpr)? {
                        return Ok(true);
                    }
                }
                self.safe_param_ids.truncate(save_len);
                for arg in &sp.args {
                    if self.visit(arg)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            t @ NodeTag::T_RestrictInfo => deferred("max_parallel_hazard_walker", t),
            NodeTag::T_Param => {
                let p: &Param = node.as_param().unwrap();
                if p.paramkind == ParamKind::PARAM_EXTERN {
                    return Ok(false);
                }
                if p.paramkind != ParamKind::PARAM_EXEC
                    || !self.safe_param_ids.contains(&p.paramid)
                {
                    return Ok(self.test(PROPARALLEL_RESTRICTED));
                }
                Ok(false)
            }
            NodeTag::T_GroupingFunc => walk_grouping_func_args(node, self),
            NodeTag::T_Query => self.visit_query_ref(node.as_query().unwrap()),
            _ => expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(
        &mut self,
        q: &'mcx types_nodes::parsenodes::Query<'mcx>,
    ) -> PgResult<bool> {
        self.scan_query(q)
    }
}

impl MaxParallelHazard {
    // Short-borrow body of visit_query_ref: standard_planner's toplevel scan
    // (planner.c:349) runs while the Query is still &mut (pre-seal), where
    // only a reborrow is available; nested Queries arrive as &'mcx.
    fn scan_query<'mcx>(&mut self, q: &types_nodes::parsenodes::Query<'mcx>) -> PgResult<bool> {
        if !q.rowMarks.is_nil() {
            self.max_hazard = PROPARALLEL_UNSAFE;
            return Ok(true);
        }
        query_tree_walker(q, self, 0)
    }
}

pub fn max_parallel_hazard<'mcx>(parse: &types_nodes::parsenodes::Query<'mcx>) -> PgResult<i8> {
    let mut cx = MaxParallelHazard {
        max_hazard: PROPARALLEL_SAFE,
        max_interesting: PROPARALLEL_UNSAFE,
        safe_param_ids: Vec::new(),
    };
    cx.scan_query(parse)?;
    Ok(cx.max_hazard)
}

/// Decomposed PlannerInfo inputs: the glob's maxParallelHazard, whether
/// glob->paramExecTypes is NIL, and the init-plan setParam ids of this
/// level and all parents.
pub fn is_parallel_safe(
    glob_max_parallel_hazard: i8,
    param_exec_types_is_empty: bool,
    safe_param_ids: Vec<i32>,
    node: Node<'_>,
) -> PgResult<bool> {
    if glob_max_parallel_hazard == PROPARALLEL_SAFE && param_exec_types_is_empty {
        return Ok(true);
    }
    let mut cx = MaxParallelHazard {
        max_hazard: PROPARALLEL_SAFE,
        max_interesting: PROPARALLEL_RESTRICTED,
        safe_param_ids,
    };
    Ok(!cx.visit(node)?)
}

struct ContainNonstrict;

impl<'mcx> NodeWalker<'mcx> for ContainNonstrict {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Aggref
            | NodeTag::T_GroupingFunc
            | NodeTag::T_WindowFunc
            | NodeTag::T_DistinctExpr
            | NodeTag::T_NullIfExpr
            | NodeTag::T_SubLink
            | NodeTag::T_SubPlan
            | NodeTag::T_AlternativeSubPlan
            | NodeTag::T_FieldStore
            | NodeTag::T_CaseExpr
            | NodeTag::T_ArrayExpr
            | NodeTag::T_RowExpr
            | NodeTag::T_RowCompareExpr
            | NodeTag::T_CoalesceExpr
            | NodeTag::T_MinMaxExpr
            | NodeTag::T_XmlExpr
            | NodeTag::T_NullTest
            | NodeTag::T_BooleanTest
            | NodeTag::T_JsonConstructorExpr => return Ok(true),
            NodeTag::T_BoolExpr => {
                use types_nodes::primnodes::BoolExprType;
                let b = node.as_bool_expr().unwrap();
                if matches!(b.boolop, BoolExprType::AND_EXPR | BoolExprType::OR_EXPR) {
                    return Ok(true);
                }
            }
            NodeTag::T_SubscriptingRef => {
                // C: subscripting assignment is nonstrict; fetch is strict
                // only per typsubscript — conservatively nonstrict unless the
                // closed array handler (fetch_strict = true).
                let sr = node.as_subscripting_ref().unwrap();
                if sr.refassgnexpr.is_some() {
                    return Ok(true);
                }
            }
            // CoerceViaIO is strict regardless of its I/O functions; look
            // only at its argument (checking the functions could be wrong).
            NodeTag::T_CoerceViaIO => {
                return self.visit(node.as_coerce_via_io().unwrap().arg);
            }
            // C: ArrayCoerceExpr is strict at the array level regardless of
            // the per-element expression.
            NodeTag::T_ArrayCoerceExpr => {
                return self.visit(node.as_array_coerce_expr().unwrap().arg);
            }
            _ => {}
        }
        if check_functions_in_node(node, &mut |f| Ok(!func_strict(f)?))? {
            return Ok(true);
        }
        expression_tree_walker(node, self)
    }
}

pub fn contain_nonstrict_functions(clause: Node<'_>) -> PgResult<bool> {
    ContainNonstrict.visit(clause)
}

struct ContainExecParam<'a> {
    param_ids: &'a [i32],
}

impl<'a, 'mcx> NodeWalker<'mcx> for ContainExecParam<'a> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(p) = node.as_param() {
            return Ok(p.paramkind == ParamKind::PARAM_EXEC && self.param_ids.contains(&p.paramid));
        }
        expression_tree_walker(node, self)
    }
}

pub fn contain_exec_param(clause: Node<'_>, param_ids: &[i32]) -> PgResult<bool> {
    ContainExecParam { param_ids }.visit(clause)
}

struct ContainAnyExecParam;

impl<'mcx> NodeWalker<'mcx> for ContainAnyExecParam {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(p) = node.as_param() {
            return Ok(p.paramkind == ParamKind::PARAM_EXEC);
        }
        expression_tree_walker(node, self)
    }
}

pub fn contain_exec_params(clause: Node<'_>) -> PgResult<bool> {
    ContainAnyExecParam.visit(clause)
}

struct ContainContextDependent {
    casetestexpr_ok: bool,
}

impl<'mcx> NodeWalker<'mcx> for ContainContextDependent {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        // CaseExpr/ArrayCoerceExpr flag scoping lands with their vocab; the
        // walker's deferred arm keeps those trees loud.
        if node.node_tag() == NodeTag::T_CaseTestExpr {
            return Ok(!self.casetestexpr_ok);
        }
        expression_tree_walker(node, self)
    }
}

pub fn contain_context_dependent_node(clause: Node<'_>) -> PgResult<bool> {
    ContainContextDependent { casetestexpr_ok: false }.visit(clause)
}

struct ContainLeakedVars;

impl<'mcx> NodeWalker<'mcx> for ContainLeakedVars {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var
            | NodeTag::T_Const
            | NodeTag::T_Param
            | NodeTag::T_ArrayExpr
            | NodeTag::T_FieldSelect
            | NodeTag::T_FieldStore
            | NodeTag::T_NamedArgExpr
            | NodeTag::T_BoolExpr
            | NodeTag::T_RelabelType
            | NodeTag::T_CollateExpr
            | NodeTag::T_CaseExpr
            | NodeTag::T_CaseTestExpr
            | NodeTag::T_RowExpr
            | NodeTag::T_SQLValueFunction
            | NodeTag::T_NullTest
            | NodeTag::T_BooleanTest
            | NodeTag::T_NextValueExpr
            | NodeTag::T_ReturningExpr
            | NodeTag::T_List => {}
            NodeTag::T_FuncExpr
            | NodeTag::T_OpExpr
            | NodeTag::T_DistinctExpr
            | NodeTag::T_NullIfExpr
            | NodeTag::T_ScalarArrayOpExpr
            | NodeTag::T_CoerceViaIO
            | NodeTag::T_ArrayCoerceExpr => {
                if check_functions_in_node(node, &mut |f| Ok(!get_func_leakproof(f)?))?
                    && var_seams::contain_var_clause::call(node)
                {
                    return Ok(true);
                }
            }
            // C: SubscriptingRef fetch is leakproof for the array handler;
            // assignment (store) is not, but stores never reach quals.
            NodeTag::T_SubscriptingRef => {}
            // C special case: a leaky per-column comparison only matters if
            // that column pair contains Vars.
            NodeTag::T_RowCompareExpr => {
                let rc = node.as_row_compare_expr().unwrap();
                for (i, opno) in rc.opnos.iter().enumerate() {
                    let funcid = lsyscache::operator::get_opcode(opno)?;
                    if !get_func_leakproof(funcid)?
                        && (var_seams::contain_var_clause::call(rc.largs.nth(i))
                            || var_seams::contain_var_clause::call(rc.rargs.nth(i)))
                    {
                        return Ok(true);
                    }
                }
            }
            t @ NodeTag::T_MinMaxExpr => deferred("contain_leaked_vars_walker", t),
            NodeTag::T_CurrentOfExpr => return Ok(false),
            // Unrecognized node: assume it might be leaky (C default arm).
            _ => return Ok(true),
        }
        expression_tree_walker(node, self)
    }
}

pub fn contain_leaked_vars(clause: Node<'_>) -> PgResult<bool> {
    ContainLeakedVars.visit(clause)
}

pub fn is_pseudo_constant_clause(clause: Node<'_>) -> PgResult<bool> {
    Ok(!var_seams::contain_var_clause::call(clause) && !contain_volatile_functions(clause)?)
}

pub fn is_pseudo_constant_clause_relids(
    clause: Node<'_>,
    relids: Option<&Bitmapset<'_>>,
) -> PgResult<bool> {
    let relids_empty = relids.map_or(true, |b| b.is_empty());
    Ok(relids_empty && !contain_volatile_functions(clause)?)
}

/// 1.0 unless the top node is a set-returning call (the SRF rowcount leg
/// needs plancat's get_function_rows — deferred loud).
pub fn expression_returns_set_rows(clause: Option<Node<'_>>) -> PgResult<f64> {
    let Some(clause) = clause else {
        return Ok(1.0);
    };
    if let Some(f) = clause.as_func_expr() {
        if f.funcretset {
            panic!("expression_returns_set_rows deferred: get_function_rows (plancat) unported");
        }
    }
    if let Some(o) = clause.as_op_expr() {
        if o.opretset {
            panic!("expression_returns_set_rows deferred: get_function_rows (plancat) unported");
        }
    }
    Ok(1.0)
}

struct PullParamids<'mcx> {
    mcx: mcx::Mcx<'mcx>,
    result: Bitmapset<'mcx>,
}

impl<'mcx> NodeWalker<'mcx> for PullParamids<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(p) = node.as_param() {
            self.result.add_member(self.mcx, p.paramid)?;
            return Ok(false);
        }
        expression_tree_walker(node, self)
    }
}

pub fn pull_paramids<'mcx>(mcx: mcx::Mcx<'mcx>, expr: Node<'mcx>) -> PgResult<Bitmapset<'mcx>> {
    let mut cx = PullParamids { mcx, result: Bitmapset::empty() };
    cx.visit(expr)?;
    Ok(cx.result)
}

struct ConvertSaop;

impl<'mcx> NodeWalker<'mcx> for ConvertSaop {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if node.node_tag() == NodeTag::T_GroupingFunc {
            return walk_grouping_func_args(node, self);
        }
        if let Some(sa) = node.as_scalar_array_op_expr() {
            const MIN_ARRAY_SIZE_FOR_HASHED_SAOP: i64 = 9;
            if let Some(c) = sa.args.nth(1).as_const() {
                if !c.constisnull {
                    if sa.useOr {
                        if let Some((l, r)) = lsyscache::get_op_hash_functions(sa.opno)? {
                            if l == r {
                                if saop_const_array_nitems(c.constvalue)
                                    >= MIN_ARRAY_SIZE_FOR_HASHED_SAOP
                                {
                                    // SAFETY: caller holds the just-planned tree
                                    // exclusively (fix_opfuncids precedent).
                                    unsafe {
                                        node.with_mut::<ScalarArrayOpExpr, _>(|s| {
                                            s.hashfuncid = l;
                                        })
                                        .unwrap();
                                    }
                                }
                                // C returns false here: matched-const arms
                                // do not descend into this node's children.
                                return Ok(false);
                            }
                        }
                    } else {
                        // NOT IN whose negator is hashable: hash-and-negate.
                        let negator = lsyscache::get_negator(sa.opno)?;
                        if negator != 0 {
                            if let Some((l, r)) = lsyscache::get_op_hash_functions(negator)? {
                                if l == r {
                                    if saop_const_array_nitems(c.constvalue)
                                        >= MIN_ARRAY_SIZE_FOR_HASHED_SAOP
                                    {
                                        let negfuncid = lsyscache::get_opcode(negator)?;
                                        // SAFETY: as above.
                                        unsafe {
                                            node.with_mut::<ScalarArrayOpExpr, _>(|s| {
                                                s.hashfuncid = l;
                                                s.negfuncid = negfuncid;
                                            })
                                            .unwrap();
                                        }
                                    }
                                    return Ok(false);
                                }
                            }
                        }
                    }
                }
            }
        }
        expression_tree_walker(node, self)
    }
}

pub fn convert_saop_to_hashed_saop(node: Node<'_>) -> PgResult<()> {
    ConvertSaop.visit(node)?;
    Ok(())
}

pub fn num_relids(_clause: Node<'_>) -> i32 {
    panic!("NumRelids deferred: needs pull_varnos over PlannerInfo outer_join_rels");
}

pub fn commute_op_expr(_clause: Node<'_>) {
    panic!("CommuteOpExpr deferred: in-place OpExpr commutation (indxpath consumer unported)");
}

pub struct WindowFuncLists<'mcx> {
    pub num_window_funcs: i32,
    pub max_win_ref: u32,
    /// Indexed by winref (0..=max_win_ref); C's windowFuncs array.
    pub window_funcs: mcx::PgVec<'mcx, mcx::PgVec<'mcx, Node<'mcx>>>,
}

struct FindWindowFuncs<'a, 'mcx> {
    lists: &'a mut WindowFuncLists<'mcx>,
}

impl<'mcx> NodeWalker<'mcx> for FindWindowFuncs<'_, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if node.node_tag() == NodeTag::T_WindowFunc {
            let winref = node.as_window_func().unwrap().winref;
            assert!(
                winref <= self.lists.max_win_ref,
                "WindowFunc contains out-of-range winref {winref}"
            );
            self.lists.window_funcs[winref as usize].push(node);
            self.lists.num_window_funcs += 1;
            // C: parser guarantees no window funcs in args/filter; no recurse.
            return Ok(false);
        }
        debug_assert!(node.node_tag() != NodeTag::T_SubLink);
        if node.node_tag() == NodeTag::T_GroupingFunc {
            return walk_grouping_func_args(node, self);
        }
        expression_tree_walker(node, self)
    }
}

pub fn find_window_functions<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    clause: Node<'mcx>,
    max_win_ref: u32,
) -> PgResult<WindowFuncLists<'mcx>> {
    let mut window_funcs = mcx::PgVec::with_capacity_in(max_win_ref as usize + 1, mcx);
    for _ in 0..=max_win_ref {
        window_funcs.push(mcx::PgVec::new_in(mcx));
    }
    let mut lists = WindowFuncLists { num_window_funcs: 0, max_win_ref, window_funcs };
    FindWindowFuncs { lists: &mut lists }.visit(clause)?;
    Ok(lists)
}

// sysattr.h FirstLowInvalidHeapAttributeNumber.
const FIRST_LOW_INVALID_HEAP_ATTR: i32 = -7;

fn strict_opfuncid(o: &types_nodes::primnodes::OpExpr<'_>) -> PgResult<bool> {
    let funcid = if o.opfuncid != 0 { o.opfuncid } else { lsyscache::get_opcode(o.opno)? };
    func_strict(funcid)
}

// is_strict_saop (clauses.c).
fn is_strict_saop(
    sa: &types_nodes::primnodes::ScalarArrayOpExpr<'_>,
    false_ok: bool,
) -> PgResult<bool> {
    let opfuncid = if sa.opfuncid == 0 {
        lsyscache::get_opcode(sa.opno)?
    } else {
        sa.opfuncid
    };
    if !func_strict(opfuncid)? {
        return Ok(false);
    }
    if sa.useOr && false_ok {
        return Ok(true);
    }
    let rightop = sa.args.nth(1);
    if let Some(c) = rightop.as_const() {
        if c.constisnull {
            return Ok(false);
        }
        return Ok(saop_const_array_nitems(c.constvalue) > 0);
    }
    if let Some(a) = rightop.as_array_expr() {
        return Ok(!a.elements.is_nil() && !a.multidims);
    }
    Ok(false)
}

// Header-relative dims read: works for 1B and 4B array images (bound-param
// array consts can be short-form); external/compressed stays loud.
fn saop_const_array_nitems(value: datum::Datum) -> i64 {
    let p = value.as_usize() as *const u8;
    // SAFETY: non-null inline varlena array const, readable per its header.
    let body: &[u8] = unsafe {
        let b0 = *p;
        if b0 & 0x01 == 0x01 {
            assert!(b0 != 0x01, "is_strict_saop: external toast array const");
            let total = ((b0 >> 1) & 0x7F) as usize;
            core::slice::from_raw_parts(p.add(1), total - 1)
        } else {
            assert!(b0 & 0x03 == 0, "is_strict_saop: compressed array const");
            let img = core::slice::from_raw_parts(
                p,
                arrayfuncs::arr_size(core::slice::from_raw_parts(p, 4)),
            );
            &img[4..]
        }
    };
    let rd = |off: usize| i32::from_ne_bytes(body[off..off + 4].try_into().unwrap());
    let ndim = rd(0);
    if ndim == 0 {
        return 0;
    }
    let mut n = 1i64;
    for i in 0..ndim as usize {
        n *= rd(12 + 4 * i) as i64;
    }
    n
}

pub fn find_nonnullable_rels<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    clause: Option<Node<'mcx>>,
) -> PgResult<Bitmapset<'mcx>> {
    find_nonnullable_rels_walker(mcx, clause, true)
}

fn find_nonnullable_rels_walker<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    node: Option<Node<'mcx>>,
    top_level: bool,
) -> PgResult<Bitmapset<'mcx>> {
    let mut result = Bitmapset::empty();
    let Some(node) = node else { return Ok(result) };
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            if var.varlevelsup == 0 {
                result.add_member(mcx, var.varno)?;
            }
        }
        NodeTag::T_List => {
            for item in node.as_list().unwrap() {
                let sub = find_nonnullable_rels_walker(mcx, Some(item), top_level)?;
                result.add_members(mcx, &sub)?;
            }
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            if func_strict(f.funcid)? {
                result = nonnullable_rels_args(mcx, &f.args, false)?;
            }
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            if strict_opfuncid(o)? {
                result = nonnullable_rels_args(mcx, &o.args, false)?;
            }
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            match b.boolop {
                types_nodes::primnodes::BoolExprType::AND_EXPR if top_level => {
                    result = nonnullable_rels_args(mcx, &b.args, true)?;
                }
                types_nodes::primnodes::BoolExprType::AND_EXPR
                | types_nodes::primnodes::BoolExprType::OR_EXPR => {
                    let mut first = true;
                    for item in &b.args {
                        let sub = find_nonnullable_rels_walker(mcx, Some(item), top_level)?;
                        if first {
                            result = sub;
                            first = false;
                        } else {
                            result.int_members(&sub);
                        }
                        if result.is_empty() {
                            break;
                        }
                    }
                }
                types_nodes::primnodes::BoolExprType::NOT_EXPR => {
                    result = nonnullable_rels_args(mcx, &b.args, false)?;
                }
            }
        }
        NodeTag::T_RelabelType => {
            result = find_nonnullable_rels_walker(
                mcx,
                Some(node.as_relabel_type().unwrap().arg),
                top_level,
            )?;
        }
        NodeTag::T_CoerceViaIO => {
            result = find_nonnullable_rels_walker(
                mcx,
                Some(node.as_coerce_via_io().unwrap().arg),
                top_level,
            )?;
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().unwrap();
            if top_level
                && nt.nulltesttype == types_nodes::primnodes::NullTestType::IS_NOT_NULL
                && !nt.argisrow
            {
                result = find_nonnullable_rels_walker(mcx, nt.arg, false)?;
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().unwrap();
            if is_strict_saop(sa, true)? {
                result = nonnullable_rels_args(mcx, &sa.args, false)?;
            }
        }
        NodeTag::T_BooleanTest => {
            use types_nodes::BoolTestType;
            let bt = node.as_boolean_test().unwrap();
            if top_level
                && matches!(
                    bt.booltesttype,
                    BoolTestType::IS_TRUE | BoolTestType::IS_FALSE | BoolTestType::IS_NOT_UNKNOWN
                )
            {
                if let Some(a) = bt.arg {
                    result = find_nonnullable_rels_walker(mcx, Some(a), false)?;
                }
            }
        }
        // C has strictness arms for these; skipping silently would
        // under-reduce vs C (silent plan-shape divergence).
        NodeTag::T_CollateExpr => {
            result = find_nonnullable_rels_walker(
                mcx,
                Some(node.as_collate_expr().unwrap().arg),
                top_level,
            )?;
        }
        // C: ArrayCoerceExpr is strict at the array level; elemexpr ignored.
        // ConvertRowtypeExpr: "not clear this is useful, but it can't hurt".
        NodeTag::T_ArrayCoerceExpr => {
            result = find_nonnullable_rels_walker(
                mcx,
                Some(node.as_array_coerce_expr().unwrap().arg),
                top_level,
            )?;
        }
        NodeTag::T_ConvertRowtypeExpr => {
            result = find_nonnullable_rels_walker(
                mcx,
                Some(node.as_convert_rowtype_expr().unwrap().arg),
                top_level,
            )?;
        }
        NodeTag::T_PlaceHolderVar => {
            let phv = node.as_place_holder_var().unwrap();
            result = find_nonnullable_rels_walker(mcx, Some(phv.phexpr), top_level)?;
            // Singleton syntactic scope behaves like a Var of that rel.
            if phv.phlevelsup == 0 && phv.phrels.num_members() == 1 {
                result.add_members(mcx, &phv.phrels)?;
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            // Strictness transfers from the testexpr only for top-level ANY
            // and any-level ROWCOMPARE sublinks (clauses.c:1649-1652).
            if ((top_level
                && sp.subLinkType == types_nodes::primnodes::SubLinkType::ANY_SUBLINK)
                || sp.subLinkType == types_nodes::primnodes::SubLinkType::ROWCOMPARE_SUBLINK)
                && sp.testexpr.is_some()
            {
                result = find_nonnullable_rels_walker(mcx, sp.testexpr, top_level)?;
            }
        }
        _ => {}
    }
    Ok(result)
}

fn nonnullable_rels_args<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    args: &types_nodes::list::NodeList<'mcx>,
    top_level: bool,
) -> PgResult<Bitmapset<'mcx>> {
    let mut result = Bitmapset::empty();
    for item in args {
        let sub = find_nonnullable_rels_walker(mcx, Some(item), top_level)?;
        result.add_members(mcx, &sub)?;
    }
    Ok(result)
}

/// C multibitmapset: entry `varno` holds attnos offset by
/// `-FIRST_LOW_INVALID_HEAP_ATTR`.
pub type MultiBitmapset<'mcx> = mcx::PgVec<'mcx, Bitmapset<'mcx>>;

#[cold]
#[inline(never)]
fn negative_mbms_index() -> ! {
    // C divergence: elog(ERROR, "negative multibitmapset member index not allowed").
    panic!("negative multibitmapset member index not allowed");
}

pub fn mbms_add_member<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    a: &mut MultiBitmapset<'mcx>,
    listidx: i32,
    bitidx: i32,
) -> PgResult<()> {
    if listidx < 0 || bitidx < 0 {
        negative_mbms_index();
    }
    while a.len() <= listidx as usize {
        a.push(Bitmapset::empty());
    }
    a[listidx as usize].add_member(mcx, bitidx)
}

pub fn mbms_add_members<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    a: &mut MultiBitmapset<'mcx>,
    b: &MultiBitmapset<'mcx>,
) -> PgResult<()> {
    while a.len() < b.len() {
        a.push(Bitmapset::empty());
    }
    for (i, bs) in b.iter().enumerate() {
        a[i].add_members(mcx, bs)?;
    }
    Ok(())
}

/// mbms_overlap_sets: the set of list indexes whose bitmapsets overlap.
pub fn mbms_overlap_sets<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    a: &MultiBitmapset<'mcx>,
    b: &MultiBitmapset<'mcx>,
) -> PgResult<Bitmapset<'mcx>> {
    let mut result = Bitmapset::empty();
    for i in 0..a.len().min(b.len()) {
        if a[i].overlap(&b[i]) {
            result.add_member(mcx, i as i32)?;
        }
    }
    Ok(result)
}

/// mbms_int_members: recycling intersect — reduce a to its intersection with b.
pub fn mbms_int_members<'mcx>(a: &mut MultiBitmapset<'mcx>, b: &MultiBitmapset<'mcx>) {
    a.truncate(b.len());
    for (i, bs) in a.iter_mut().enumerate() {
        bs.int_members(&b[i]);
    }
}

pub fn mbms_is_member(listidx: i32, bitidx: i32, a: &MultiBitmapset<'_>) -> bool {
    if listidx < 0 || bitidx < 0 {
        negative_mbms_index();
    }
    if listidx as usize >= a.len() {
        return false;
    }
    a[listidx as usize].is_member(bitidx)
}

pub fn find_nonnullable_vars<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    clause: Option<Node<'mcx>>,
) -> PgResult<MultiBitmapset<'mcx>> {
    find_nonnullable_vars_walker(mcx, clause, true)
}

fn find_nonnullable_vars_walker<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    node: Option<Node<'mcx>>,
    top_level: bool,
) -> PgResult<MultiBitmapset<'mcx>> {
    let mut result: MultiBitmapset<'mcx> = mcx::PgVec::new_in(mcx);
    let Some(node) = node else { return Ok(result) };
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            if var.varlevelsup == 0 {
                mbms_add_member(
                    mcx,
                    &mut result,
                    var.varno,
                    var.varattno as i32 - FIRST_LOW_INVALID_HEAP_ATTR,
                )?;
            }
        }
        NodeTag::T_List => {
            for item in node.as_list().unwrap() {
                let sub = find_nonnullable_vars_walker(mcx, Some(item), top_level)?;
                mbms_add_members(mcx, &mut result, &sub)?;
            }
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            if func_strict(f.funcid)? {
                result = nonnullable_vars_args(mcx, &f.args, false)?;
            }
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            if strict_opfuncid(o)? {
                result = nonnullable_vars_args(mcx, &o.args, false)?;
            }
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            match b.boolop {
                types_nodes::primnodes::BoolExprType::AND_EXPR if top_level => {
                    result = nonnullable_vars_args(mcx, &b.args, true)?;
                }
                types_nodes::primnodes::BoolExprType::AND_EXPR
                | types_nodes::primnodes::BoolExprType::OR_EXPR => {
                    let mut first = true;
                    for item in &b.args {
                        let sub = find_nonnullable_vars_walker(mcx, Some(item), top_level)?;
                        if first {
                            result = sub;
                            first = false;
                        } else {
                            // mbms_int_members: pairwise intersect + truncate.
                            let n = result.len().min(sub.len());
                            result.truncate(n);
                            for (i, bs) in result.iter_mut().enumerate() {
                                bs.int_members(&sub[i]);
                            }
                        }
                        if result.iter().all(|bs| bs.is_empty()) {
                            break;
                        }
                    }
                }
                types_nodes::primnodes::BoolExprType::NOT_EXPR => {
                    result = nonnullable_vars_args(mcx, &b.args, false)?;
                }
            }
        }
        NodeTag::T_RelabelType => {
            result = find_nonnullable_vars_walker(
                mcx,
                Some(node.as_relabel_type().unwrap().arg),
                top_level,
            )?;
        }
        NodeTag::T_CoerceViaIO => {
            result = find_nonnullable_vars_walker(
                mcx,
                Some(node.as_coerce_via_io().unwrap().arg),
                top_level,
            )?;
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().unwrap();
            if top_level
                && nt.nulltesttype == types_nodes::primnodes::NullTestType::IS_NOT_NULL
                && !nt.argisrow
            {
                result = find_nonnullable_vars_walker(mcx, nt.arg, false)?;
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().unwrap();
            if is_strict_saop(sa, true)? {
                result = nonnullable_vars_args(mcx, &sa.args, false)?;
            }
        }
        NodeTag::T_CollateExpr => {
            result = find_nonnullable_vars_walker(
                mcx,
                Some(node.as_collate_expr().unwrap().arg),
                top_level,
            )?;
        }
        NodeTag::T_BooleanTest => {
            use types_nodes::BoolTestType;
            let bt = node.as_boolean_test().unwrap();
            if top_level
                && matches!(
                    bt.booltesttype,
                    BoolTestType::IS_TRUE | BoolTestType::IS_FALSE | BoolTestType::IS_NOT_UNKNOWN
                )
            {
                if let Some(a) = bt.arg {
                    result = find_nonnullable_vars_walker(mcx, Some(a), false)?;
                }
            }
        }
        NodeTag::T_ArrayCoerceExpr => {
            result = find_nonnullable_vars_walker(
                mcx,
                Some(node.as_array_coerce_expr().unwrap().arg),
                top_level,
            )?;
        }
        NodeTag::T_ConvertRowtypeExpr => {
            result = find_nonnullable_vars_walker(
                mcx,
                Some(node.as_convert_rowtype_expr().unwrap().arg),
                top_level,
            )?;
        }
        NodeTag::T_PlaceHolderVar => {
            let phv = node.as_place_holder_var().unwrap();
            result = find_nonnullable_vars_walker(mcx, Some(phv.phexpr), top_level)?;
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            if ((top_level
                && sp.subLinkType == types_nodes::primnodes::SubLinkType::ANY_SUBLINK)
                || sp.subLinkType == types_nodes::primnodes::SubLinkType::ROWCOMPARE_SUBLINK)
                && sp.testexpr.is_some()
            {
                result = find_nonnullable_vars_walker(mcx, sp.testexpr, top_level)?;
            }
        }
        _ => {}
    }
    Ok(result)
}

fn nonnullable_vars_args<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    args: &types_nodes::list::NodeList<'mcx>,
    top_level: bool,
) -> PgResult<MultiBitmapset<'mcx>> {
    let mut result: MultiBitmapset<'mcx> = mcx::PgVec::new_in(mcx);
    for item in args {
        let sub = find_nonnullable_vars_walker(mcx, Some(item), top_level)?;
        mbms_add_members(mcx, &mut result, &sub)?;
    }
    Ok(result)
}

pub fn find_forced_null_vars<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    node: Option<Node<'mcx>>,
) -> PgResult<MultiBitmapset<'mcx>> {
    let mut result: MultiBitmapset<'mcx> = mcx::PgVec::new_in(mcx);
    let Some(node) = node else { return Ok(result) };
    if let Some(var) = find_forced_null_var(node) {
        mbms_add_member(
            mcx,
            &mut result,
            var.varno,
            var.varattno as i32 - FIRST_LOW_INVALID_HEAP_ATTR,
        )?;
    } else if node.node_tag() == NodeTag::T_List {
        for item in node.as_list().unwrap() {
            let sub = find_forced_null_vars(mcx, Some(item))?;
            mbms_add_members(mcx, &mut result, &sub)?;
        }
    } else if let Some(b) = node.as_bool_expr() {
        if b.boolop == types_nodes::primnodes::BoolExprType::AND_EXPR {
            for item in &b.args {
                let sub = find_forced_null_vars(mcx, Some(item))?;
                mbms_add_members(mcx, &mut result, &sub)?;
            }
        }
    }
    Ok(result)
}

pub fn find_forced_null_var<'mcx>(
    node: Node<'mcx>,
) -> Option<&'mcx types_nodes::primnodes::Var<'mcx>> {
    if let Some(bt) = node.as_boolean_test() {
        if bt.booltesttype != types_nodes::BoolTestType::IS_UNKNOWN {
            return None;
        }
        let var = bt.arg?.as_var()?;
        return if var.varlevelsup == 0 { Some(var) } else { None };
    }
    let nt = node.as_null_test()?;
    if nt.nulltesttype != types_nodes::primnodes::NullTestType::IS_NULL || nt.argisrow {
        return None;
    }
    let var = nt.arg?.as_var()?;
    if var.varlevelsup == 0 {
        Some(var)
    } else {
        None
    }
}

pub fn is_andclause(node: Node<'_>) -> bool {
    matches!(node.as_bool_expr(), Some(b) if b.boolop == types_nodes::primnodes::BoolExprType::AND_EXPR)
}

pub fn is_orclause(node: Node<'_>) -> bool {
    matches!(node.as_bool_expr(), Some(b) if b.boolop == types_nodes::primnodes::BoolExprType::OR_EXPR)
}

pub fn is_notclause(node: Node<'_>) -> bool {
    matches!(node.as_bool_expr(), Some(b) if b.boolop == types_nodes::primnodes::BoolExprType::NOT_EXPR)
}

pub fn make_andclause<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    args: types_nodes::NodeList<'mcx>,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        types_nodes::primnodes::BoolExpr {
            boolop: types_nodes::primnodes::BoolExprType::AND_EXPR,
            args,
            location: -1,
        },
    )
}

pub fn make_orclause<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    args: types_nodes::NodeList<'mcx>,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        types_nodes::primnodes::BoolExpr {
            boolop: types_nodes::primnodes::BoolExprType::OR_EXPR,
            args,
            location: -1,
        },
    )
}

// make_SAOP_expr (clauses.c). None iff coltype has no array type.
pub fn make_saop_expr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    oper: Oid,
    leftexpr: Node<'mcx>,
    coltype: Oid,
    arraycollid: Oid,
    inputcollid: Oid,
    exprs: types_nodes::NodeList<'mcx>,
    have_non_const: bool,
) -> PgResult<Option<Node<'mcx>>> {
    let arraytype = lsyscache::get_array_type(coltype)?;
    if arraytype == 0 {
        return Ok(None);
    }
    let array_node = if have_non_const {
        Node::mk(
            mcx,
            types_nodes::primnodes::ArrayExpr {
                array_typeid: arraytype,
                array_collid: 0,
                element_typeid: coltype,
                elements: exprs,
                multidims: false,
                list_start: 0,
                list_end: 0,
                location: -1,
            },
        )?
    } else {
        let (typlen, typbyval, typalign) = lsyscache::get_typlenbyvalalign(coltype)?;
        let mut elems: mcx::PgVec<'mcx, datum::Datum> = mcx::PgVec::new_in(mcx);
        let mut nulls: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);
        for e in &exprs {
            let c = e.as_const().expect("have_non_const covers non-Const elements");
            elems.push(c.constvalue);
            nulls.push(c.constisnull);
        }
        let dims = [elems.len() as i32];
        let lbs = [1i32];
        let arr = arrayfuncs::construct::construct_md_array(
            mcx,
            &elems,
            Some(&nulls),
            1,
            &dims,
            &lbs,
            coltype,
            typlen as i32,
            typbyval,
            typalign as u8,
        )?;
        Node::mk(
            mcx,
            types_nodes::primnodes::Const {
                consttype: arraytype,
                consttypmod: -1,
                constcollid: arraycollid,
                constlen: -1,
                constvalue: datum::Datum::from_usize(arr.leak().as_ptr() as usize),
                constisnull: false,
                constbyval: false,
                location: -1,
            },
        )?
    };
    Ok(Some(Node::mk(
        mcx,
        ScalarArrayOpExpr {
            opno: oper,
            opfuncid: lsyscache::get_opcode(oper)?,
            hashfuncid: 0,
            negfuncid: 0,
            useOr: true,
            inputcollid,
            args: types_nodes::NodeList::make2(mcx, leftexpr, array_node)?,
            location: -1,
        },
    )?))
}

pub fn make_notclause<'mcx>(mcx: mcx::Mcx<'mcx>, arg: Node<'mcx>) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        types_nodes::primnodes::BoolExpr {
            boolop: types_nodes::primnodes::BoolExprType::NOT_EXPR,
            args: types_nodes::NodeList::make1(mcx, arg)?,
            location: -1,
        },
    )
}

// expression_has_grouping_conflict (clauses.c): would 'expr' distinguish rows
// a grouping mechanism considers equal? get_eqop identifies a grouping column
// by returning a valid eqop for its Var (InvalidOid otherwise). A grouping
// column is provably safe only as a direct operand of a comparison compatible
// with the grouping eqop and, for a nondeterministic collation, under the
// column's own collation; any other reference to a nondeterministic-collation
// grouping column is rejected.
pub fn expression_has_grouping_conflict<'mcx>(
    expr: Node<'mcx>,
    get_eqop: &mut dyn FnMut(&types_nodes::primnodes::Var<'mcx>) -> PgResult<Oid>,
) -> PgResult<bool> {
    let mut w = GroupingConflictWalker { get_eqop };
    NodeWalker::visit(&mut w, expr)
}

struct GroupingConflictWalker<'a, 'mcx> {
    get_eqop: &'a mut dyn FnMut(&types_nodes::primnodes::Var<'mcx>) -> PgResult<Oid>,
}

impl<'a, 'mcx> GroupingConflictWalker<'a, 'mcx> {
    // grouping_check_operand (clauses.c): a direct grouping-column operand is
    // fully handled here (not recursed into); anything else walks normally.
    fn check_operand(&mut self, arg: Node<'mcx>, opno: Oid, inputcollid: Oid) -> PgResult<bool> {
        let mut node = arg;
        while let Some(r) = node.as_relabel_type() {
            node = r.arg;
        }
        if let Some(var) = node.as_var() {
            let grouping_eqop = (self.get_eqop)(var)?;
            if grouping_eqop != types_core::InvalidOid {
                if !lsyscache::equality_ops_are_compatible(opno, grouping_eqop)? {
                    return Ok(true);
                }
                if var.varcollid != types_core::InvalidOid
                    && !lsyscache::get_collation_isdeterministic(var.varcollid)?
                    && inputcollid != var.varcollid
                {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        self.visit(arg)
    }

    fn check_operands(
        &mut self,
        opno: Oid,
        inputcollid: Oid,
        args: &types_nodes::NodeList<'mcx>,
    ) -> PgResult<bool> {
        for arg in args {
            if self.check_operand(arg, opno, inputcollid)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl<'a, 'mcx> NodeWalker<'mcx> for GroupingConflictWalker<'a, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(var) = node.as_var() {
            // A grouping column reaching here was not a direct comparison
            // operand; safe for deterministic collations only.
            return Ok((self.get_eqop)(var)? != types_core::InvalidOid
                && var.varcollid != types_core::InvalidOid
                && !lsyscache::get_collation_isdeterministic(var.varcollid)?);
        }
        if let Some(opexpr) = node.as_op_expr() {
            if lsyscache::op_is_safe_index_member(opexpr.opno)? {
                return self.check_operands(opexpr.opno, opexpr.inputcollid, &opexpr.args);
            }
            // not a btree/hash member: fall through to the generic walk
        } else if let Some(saop) = node.as_scalar_array_op_expr() {
            if lsyscache::op_is_safe_index_member(saop.opno)? {
                return self.check_operands(saop.opno, saop.inputcollid, &saop.args);
            }
        } else if let Some(rcexpr) = node.as_row_compare_expr() {
            // Each column is compared under its own operator and inputcollid.
            for i in 0..rcexpr.largs.len() {
                let opno = rcexpr.opnos.nth(i);
                let collid = rcexpr.inputcollids.nth(i);
                if self.check_operand(rcexpr.largs.nth(i), opno, collid)?
                    || self.check_operand(rcexpr.rargs.nth(i), opno, collid)?
                {
                    return Ok(true);
                }
            }
            return Ok(false);
        } else if let Some(cexpr) = node.as_case_expr() {
            if let Some(cexpr_arg) = cexpr.arg {
                // A simple CASE compares the arg under every WHEN's
                // inputcollid; the WHEN operators are always the type-default
                // "=", so only a collation conflict is possible on the arg.
                let mut arg = cexpr_arg;
                while let Some(r) = arg.as_relabel_type() {
                    arg = r.arg;
                }
                if let Some(var) = arg.as_var() {
                    if (self.get_eqop)(var)? != types_core::InvalidOid
                        && var.varcollid != types_core::InvalidOid
                        && !lsyscache::get_collation_isdeterministic(var.varcollid)?
                    {
                        for cw_node in &cexpr.args {
                            let cw = cw_node.as_case_when().expect("CaseWhen cell");
                            let collid = cw.expr.map_or(
                                types_core::InvalidOid,
                                crate::walker::node_funcs::expr_input_collation,
                            );
                            if collid != types_core::InvalidOid && collid != var.varcollid {
                                return Ok(true);
                            }
                        }
                    }
                } else if self.visit(cexpr_arg)? {
                    return Ok(true);
                }
                for cw_node in &cexpr.args {
                    let cw = cw_node.as_case_when().expect("CaseWhen cell");
                    if let Some(e) = cw.expr {
                        if self.visit(e)? {
                            return Ok(true);
                        }
                    }
                    if let Some(r) = cw.result {
                        if self.visit(r)? {
                            return Ok(true);
                        }
                    }
                }
                return match cexpr.defresult {
                    Some(d) => self.visit(d),
                    None => Ok(false),
                };
            }
        }
        expression_tree_walker(node, self)
    }
}

// make_ands_implicit (clauses.c): explicit AND -> flat list; constant TRUE ->
// NIL; the cell copy shares the arg nodes (C returns the AND's own list).
pub fn make_ands_implicit<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    clause: Option<Node<'mcx>>,
) -> PgResult<types_nodes::NodeList<'mcx>> {
    let Some(clause) = clause else {
        return Ok(types_nodes::NodeList::nil());
    };
    if is_andclause(clause) {
        return clause.as_bool_expr().unwrap().args.clone_in(mcx);
    }
    if let Some(c) = clause.as_const() {
        if !c.constisnull && c.constvalue.as_bool() {
            return Ok(types_nodes::NodeList::nil());
        }
    }
    types_nodes::NodeList::make1(mcx, clause)
}

pub fn make_ands_explicit<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    andclauses: &types_nodes::NodeList<'mcx>,
) -> PgResult<Node<'mcx>> {
    match andclauses.len() {
        0 => crate::fold::make_bool_const(mcx, true, false),
        1 => Ok(andclauses.nth(0)),
        _ => make_andclause(mcx, andclauses.clone_in(mcx)?),
    }
}
