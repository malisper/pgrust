use crate::RelFileLocator;
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::access::relscan::ScanDirection;
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::plannodes::PlanEstimate;
use crate::include::nodes::primnodes::{
    AggAccum, Expr, JoinType, OrderByEntry, ProjectSetTarget, QueryColumn, RelationDesc,
    SetReturningCall, SortGroupClause, TargetEntry, ToastRelationRef, Var,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelOptKind {
    BaseRel,
    JoinRel,
    UpperRel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathTarget {
    pub exprs: Vec<Expr>,
    pub width: usize,
}

impl PathTarget {
    pub fn new(exprs: Vec<Expr>) -> Self {
        Self {
            width: exprs.len(),
            exprs,
        }
    }

    pub fn from_target_list(target_list: &[TargetEntry]) -> Self {
        Self::new(
            target_list
                .iter()
                .map(|target| target.expr.clone())
                .collect(),
        )
    }

    pub fn from_sort_clause(sort_clause: &[SortGroupClause]) -> Vec<PathKey> {
        sort_clause
            .iter()
            .map(|clause| PathKey {
                expr: clause.expr.clone(),
                descending: clause.descending,
                nulls_first: clause.nulls_first,
            })
            .collect()
    }

    pub fn from_rte(rtindex: usize, rte: &RangeTblEntry) -> Self {
        match &rte.kind {
            RangeTblEntryKind::Join { joinaliasvars, .. } if !joinaliasvars.is_empty() => {
                Self::new(joinaliasvars.clone())
            }
            _ => Self::new(
                rte.desc
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        Expr::Var(Var {
                            varno: rtindex,
                            varattno: index + 1,
                            varlevelsup: 0,
                            vartype: column.sql_type,
                        })
                    })
                    .collect(),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathKey {
    pub expr: Expr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestrictInfo {
    pub clause: Expr,
    pub required_relids: Vec<usize>,
    pub is_pushed_down: bool,
}

impl RestrictInfo {
    pub fn new(clause: Expr) -> Self {
        Self {
            required_relids: expr_relids(&clause),
            clause,
            is_pushed_down: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecialJoinInfo {
    pub jointype: JoinType,
    pub rtindex: usize,
    pub min_lefthand: Vec<usize>,
    pub min_righthand: Vec<usize>,
    pub syn_lefthand: Vec<usize>,
    pub syn_righthand: Vec<usize>,
    pub join_quals: Expr,
}

#[derive(Debug, Clone)]
pub struct RelOptInfo {
    pub relids: Vec<usize>,
    pub reloptkind: RelOptKind,
    pub reltarget: PathTarget,
    pub pathlist: Vec<Path>,
    pub cheapest_total_path: Option<usize>,
    pub baserestrictinfo: Vec<RestrictInfo>,
    pub joininfo: Vec<RestrictInfo>,
    pub rows: f64,
}

impl RelOptInfo {
    pub fn new(relids: Vec<usize>, reloptkind: RelOptKind, reltarget: PathTarget) -> Self {
        Self {
            relids,
            reloptkind,
            reltarget,
            pathlist: Vec::new(),
            cheapest_total_path: None,
            baserestrictinfo: Vec::new(),
            joininfo: Vec::new(),
            rows: 0.0,
        }
    }

    pub fn add_path(&mut self, path: Path) {
        let total_cost = path.plan_info().total_cost.as_f64();
        let next_index = self.pathlist.len();
        let replace_cheapest = self
            .cheapest_total_path
            .and_then(|index| self.pathlist.get(index))
            .map(|current| total_cost < current.plan_info().total_cost.as_f64())
            .unwrap_or(true);
        self.pathlist.push(path);
        if replace_cheapest {
            self.cheapest_total_path = Some(next_index);
        }
        if let Some(best) = self
            .cheapest_total_path
            .and_then(|index| self.pathlist.get(index))
        {
            self.rows = best.plan_info().plan_rows.as_f64();
        }
    }

    pub fn cheapest_total_path(&self) -> Option<&Path> {
        self.cheapest_total_path
            .and_then(|index| self.pathlist.get(index))
    }

    pub fn from_rte(rtindex: usize, rte: &RangeTblEntry) -> Self {
        Self::new(
            vec![rtindex],
            RelOptKind::BaseRel,
            PathTarget::from_rte(rtindex, rte),
        )
    }
}

#[derive(Debug, Clone)]
pub struct PlannerGlobal {
    pub subplans: Vec<crate::include::nodes::plannodes::Plan>,
}

impl PlannerGlobal {
    pub fn new() -> Self {
        Self {
            subplans: Vec::new(),
        }
    }
}

impl Default for PlannerGlobal {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct PlannerInfo {
    pub parse: Query,
    pub simple_rel_array: Vec<Option<RelOptInfo>>,
    pub join_rel_list: Vec<RelOptInfo>,
    pub join_info_list: Vec<SpecialJoinInfo>,
    pub inner_join_clauses: Vec<RestrictInfo>,
    pub processed_tlist: Vec<TargetEntry>,
    pub final_target: PathTarget,
    pub query_pathkeys: Vec<PathKey>,
    pub final_rel: Option<RelOptInfo>,
}

impl PlannerInfo {
    pub fn new(parse: Query) -> Self {
        let final_target = PathTarget::from_target_list(&parse.target_list);
        let query_pathkeys = PathTarget::from_sort_clause(&parse.sort_clause);
        let simple_rel_array = build_simple_rel_array(&parse.rtable);
        let join_info_list = build_special_join_info(parse.jointree.as_ref());
        Self {
            processed_tlist: parse.target_list.clone(),
            final_target,
            query_pathkeys,
            simple_rel_array,
            join_rel_list: Vec::new(),
            join_info_list,
            inner_join_clauses: Vec::new(),
            final_rel: None,
            parse,
        }
    }

    pub fn all_query_relids(&self) -> Vec<usize> {
        let mut relids = self
            .simple_rel_array
            .iter()
            .enumerate()
            .skip(1)
            .filter_map(|(rtindex, rel)| rel.as_ref().map(|_| rtindex))
            .collect::<Vec<_>>();
        relids.sort_unstable();
        relids.dedup();
        relids
    }
}

fn build_simple_rel_array(rtable: &[RangeTblEntry]) -> Vec<Option<RelOptInfo>> {
    let mut simple_rel_array = vec![None];
    simple_rel_array.extend(rtable.iter().enumerate().map(|(index, rte)| match &rte.kind {
        RangeTblEntryKind::Join { .. } => None,
        _ => Some(RelOptInfo::from_rte(index + 1, rte)),
    }));
    simple_rel_array
}

fn build_special_join_info(jointree: Option<&JoinTreeNode>) -> Vec<SpecialJoinInfo> {
    fn walk(node: &JoinTreeNode, joins: &mut Vec<SpecialJoinInfo>) -> Vec<usize> {
        match node {
            JoinTreeNode::RangeTblRef(rtindex) => vec![*rtindex],
            JoinTreeNode::JoinExpr {
                left,
                right,
                kind,
                rtindex,
                quals,
            } => {
                let left_relids = walk(left, joins);
                let right_relids = walk(right, joins);
                if !matches!(kind, JoinType::Inner | JoinType::Cross) {
                    joins.push(SpecialJoinInfo {
                        jointype: *kind,
                        rtindex: *rtindex,
                        min_lefthand: left_relids.clone(),
                        min_righthand: right_relids.clone(),
                        syn_lefthand: left_relids.clone(),
                        syn_righthand: right_relids.clone(),
                        join_quals: quals.clone(),
                    });
                }
                let mut relids = left_relids;
                relids.extend(right_relids);
                relids.sort_unstable();
                relids.dedup();
                relids
            }
        }
    }

    let mut joins = Vec::new();
    if let Some(jointree) = jointree {
        walk(jointree, &mut joins);
    }
    joins
}

fn expr_relids(expr: &Expr) -> Vec<usize> {
    let mut relids = Vec::new();
    collect_expr_relids(expr, &mut relids);
    relids.sort_unstable();
    relids.dedup();
    relids
}

fn collect_expr_relids(expr: &Expr, relids: &mut Vec<usize>) {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => relids.push(var.varno),
        Expr::Aggref(aggref) => {
            for arg in &aggref.args {
                collect_expr_relids(arg, relids);
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_expr_relids(arg, relids);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_expr_relids(arg, relids);
            }
        }
        Expr::Func(func) => {
            for arg in &func.args {
                collect_expr_relids(arg, relids);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_expr_relids(testexpr, relids);
            }
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_expr_relids(testexpr, relids);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_expr_relids(&saop.left, relids);
            collect_expr_relids(&saop.right, relids);
        }
        Expr::Cast(inner, _) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_expr_relids(inner, relids)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_expr_relids(expr, relids);
            collect_expr_relids(pattern, relids);
            if let Some(escape) = escape {
                collect_expr_relids(escape, relids);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_expr_relids(left, relids);
            collect_expr_relids(right, relids);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_expr_relids(element, relids);
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_expr_relids(array, relids);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_expr_relids(lower, relids);
                }
                if let Some(upper) = &subscript.upper {
                    collect_expr_relids(upper, relids);
                }
            }
        }
        Expr::Column(_)
        | Expr::OuterColumn { .. }
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
        Expr::Var(_) => {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Path {
    Result {
        plan_info: PlanEstimate,
    },
    SeqScan {
        plan_info: PlanEstimate,
        source_id: usize,
        rel: RelFileLocator,
        relation_oid: u32,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
    },
    IndexScan {
        plan_info: PlanEstimate,
        source_id: usize,
        rel: RelFileLocator,
        index_rel: RelFileLocator,
        am_oid: u32,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
        index_meta: IndexRelCacheEntry,
        keys: Vec<ScanKeyData>,
        direction: ScanDirection,
    },
    Filter {
        plan_info: PlanEstimate,
        input: Box<Path>,
        predicate: Expr,
    },
    NestedLoopJoin {
        plan_info: PlanEstimate,
        left: Box<Path>,
        right: Box<Path>,
        kind: JoinType,
        on: Expr,
    },
    Projection {
        plan_info: PlanEstimate,
        slot_id: usize,
        input: Box<Path>,
        targets: Vec<TargetEntry>,
    },
    OrderBy {
        plan_info: PlanEstimate,
        input: Box<Path>,
        items: Vec<OrderByEntry>,
    },
    Limit {
        plan_info: PlanEstimate,
        input: Box<Path>,
        limit: Option<usize>,
        offset: usize,
    },
    Aggregate {
        plan_info: PlanEstimate,
        slot_id: usize,
        input: Box<Path>,
        group_by: Vec<Expr>,
        accumulators: Vec<AggAccum>,
        having: Option<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    Values {
        plan_info: PlanEstimate,
        slot_id: usize,
        rows: Vec<Vec<Expr>>,
        output_columns: Vec<QueryColumn>,
    },
    FunctionScan {
        plan_info: PlanEstimate,
        slot_id: usize,
        call: SetReturningCall,
    },
    ProjectSet {
        plan_info: PlanEstimate,
        slot_id: usize,
        input: Box<Path>,
        targets: Vec<ProjectSetTarget>,
    },
}
