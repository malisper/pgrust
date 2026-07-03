use lsyscache::{func_parallel, func_strict, func_volatile, get_func_leakproof};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::primnodes::{Param, ParamKind};
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

struct ContainSubplans;

impl<'mcx> NodeWalker<'mcx> for ContainSubplans {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_SubPlan | NodeTag::T_AlternativeSubPlan | NodeTag::T_SubLink => Ok(true),
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
            t @ (NodeTag::T_JsonConstructorExpr | NodeTag::T_JsonExpr) => {
                deferred("contain_mutable_functions: json immutability probe", t)
            }
            // All SQLValueFunction variants are stable; NextValueExpr volatile.
            NodeTag::T_SQLValueFunction | NodeTag::T_NextValueExpr => Ok(true),
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

pub fn contain_mutable_functions_after_planning(_expr: Node<'_>) -> PgResult<bool> {
    panic!("contain_mutable_functions_after_planning deferred: expression_planner unported");
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

struct MaxParallelHazard<'a> {
    max_hazard: i8,
    max_interesting: i8,
    safe_param_ids: &'a [i32],
}

impl MaxParallelHazard<'_> {
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

impl<'a, 'mcx> NodeWalker<'mcx> for MaxParallelHazard<'a> {
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
            t @ (NodeTag::T_RestrictInfo | NodeTag::T_SubPlan) => {
                deferred("max_parallel_hazard_walker", t)
            }
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
            NodeTag::T_Query => self.visit_query_ref(node.as_query().unwrap()),
            _ => expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(
        &mut self,
        q: &'mcx types_nodes::parsenodes::Query<'mcx>,
    ) -> PgResult<bool> {
        if !q.rowMarks.is_nil() {
            self.max_hazard = PROPARALLEL_UNSAFE;
            return Ok(true);
        }
        query_tree_walker(q, self, 0)
    }
}

pub fn max_parallel_hazard<'mcx>(parse: &'mcx types_nodes::parsenodes::Query<'mcx>) -> PgResult<i8> {
    let mut cx = MaxParallelHazard {
        max_hazard: PROPARALLEL_SAFE,
        max_interesting: PROPARALLEL_UNSAFE,
        safe_param_ids: &[],
    };
    cx.visit_query_ref(parse)?;
    Ok(cx.max_hazard)
}

/// Decomposed PlannerInfo inputs: the glob's maxParallelHazard, whether
/// glob->paramExecTypes is NIL, and the init-plan setParam ids of this
/// level and all parents.
pub fn is_parallel_safe(
    glob_max_parallel_hazard: i8,
    param_exec_types_is_empty: bool,
    safe_param_ids: &[i32],
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
            t @ (NodeTag::T_SubscriptingRef
            | NodeTag::T_CoerceViaIO
            | NodeTag::T_ArrayCoerceExpr) => {
                deferred("contain_nonstrict_functions_walker", t)
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
            t @ (NodeTag::T_SubscriptingRef
            | NodeTag::T_RowCompareExpr
            | NodeTag::T_MinMaxExpr) => deferred("contain_leaked_vars_walker", t),
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
        // The SAOP arm itself is the walker's deferred T_ScalarArrayOpExpr.
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
        // C has strictness arms for these; skipping silently would
        // under-reduce vs C (silent plan-shape divergence).
        NodeTag::T_ScalarArrayOpExpr
        | NodeTag::T_BooleanTest
        | NodeTag::T_SubPlan
        | NodeTag::T_PlaceHolderVar
        | NodeTag::T_ArrayCoerceExpr
        | NodeTag::T_ConvertRowtypeExpr
        | NodeTag::T_CollateExpr => panic!(
            "find_nonnullable_rels_walker (clauses.c): {:?} strictness arm unported",
            node.node_tag()
        ),
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

pub fn mbms_add_member<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    a: &mut MultiBitmapset<'mcx>,
    listidx: i32,
    bitidx: i32,
) -> PgResult<()> {
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
        NodeTag::T_ScalarArrayOpExpr
        | NodeTag::T_BooleanTest
        | NodeTag::T_SubPlan
        | NodeTag::T_PlaceHolderVar
        | NodeTag::T_ArrayCoerceExpr
        | NodeTag::T_ConvertRowtypeExpr
        | NodeTag::T_CollateExpr => panic!(
            "find_nonnullable_vars_walker (clauses.c): {:?} strictness arm unported",
            node.node_tag()
        ),
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

// BooleanTest IS UNKNOWN arm dead: that tag is loud in the walkers above.
pub fn find_forced_null_var<'mcx>(
    node: Node<'mcx>,
) -> Option<&'mcx types_nodes::primnodes::Var<'mcx>> {
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

// make_ands_implicit (clauses.c): explicit AND -> flat list; constant TRUE ->
// NIL; the AND's arg list is shared, matching C's pointer share.
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
