use crate::RelFileLocator;
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::access::relscan::ScanDirection;
use crate::include::nodes::parsenodes::SetOperator;
use crate::include::nodes::parsenodes::{Query, QueryRowMark, RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::plannodes::{IndexScanKey, PlanEstimate};
use crate::include::nodes::primnodes::{
    AggAccum, Expr, JoinType, OrderByEntry, ProjectSetTarget, QueryColumn, RelationDesc,
    SetReturningCall, SortGroupClause, TargetEntry, ToastRelationRef, Var, WindowClause,
    user_attrno,
};
use std::rc::Rc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelOptKind {
    BaseRel,
    OtherMemberRel,
    JoinRel,
    UpperRel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRelInfo {
    pub parent_relid: usize,
    pub child_relid: usize,
    pub translated_vars: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathTarget {
    pub exprs: Vec<Expr>,
    pub sortgrouprefs: Vec<usize>,
    pub width: usize,
}

impl PathTarget {
    pub fn new(exprs: Vec<Expr>) -> Self {
        let width = exprs.len();
        Self {
            exprs,
            sortgrouprefs: vec![0; width],
            width,
        }
    }

    pub fn with_sortgrouprefs(exprs: Vec<Expr>, sortgrouprefs: Vec<usize>) -> Self {
        assert_eq!(exprs.len(), sortgrouprefs.len());
        Self {
            width: exprs.len(),
            exprs,
            sortgrouprefs,
        }
    }

    pub fn from_target_list(target_list: &[TargetEntry]) -> Self {
        Self::with_sortgrouprefs(
            target_list
                .iter()
                .map(|target| target.expr.clone())
                .collect(),
            target_list
                .iter()
                .map(|target| target.ressortgroupref)
                .collect(),
        )
    }

    pub fn from_sort_clause(
        sort_clause: &[SortGroupClause],
        target_list: &[TargetEntry],
    ) -> Vec<PathKey> {
        sort_clause
            .iter()
            .map(|clause| {
                let expr = target_list
                    .iter()
                    .find(|target| target.ressortgroupref == clause.tle_sort_group_ref)
                    .map(|target| target.expr.clone())
                    .unwrap_or_else(|| clause.expr.clone());
                PathKey {
                    expr,
                    ressortgroupref: clause.tle_sort_group_ref,
                    descending: clause.descending,
                    nulls_first: clause.nulls_first,
                    collation_oid: clause.collation_oid,
                }
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
                            varattno: user_attrno(index),
                            varlevelsup: 0,
                            vartype: column.sql_type,
                        })
                    })
                    .collect(),
            ),
        }
    }

    pub fn get_pathtarget_sortgroupref(&self, index: usize) -> usize {
        self.sortgrouprefs.get(index).copied().unwrap_or(0)
    }

    pub fn add_column_to_pathtarget(&mut self, expr: Expr, sortgroupref: usize) {
        if let Some(index) = self.exprs.iter().enumerate().find_map(|(index, existing)| {
            let existing_ref = self.sortgrouprefs.get(index).copied().unwrap_or(0);
            (*existing == expr
                && (existing_ref == sortgroupref || existing_ref == 0 || sortgroupref == 0))
                .then_some(index)
        }) {
            if self.sortgrouprefs[index] == 0 && sortgroupref != 0 {
                self.sortgrouprefs[index] = sortgroupref;
            }
            return;
        }
        self.exprs.push(expr);
        self.sortgrouprefs.push(sortgroupref);
        self.width = self.exprs.len();
    }

    pub fn add_new_columns_to_pathtarget<I>(&mut self, exprs: I)
    where
        I: IntoIterator<Item = Expr>,
    {
        for expr in exprs {
            self.add_column_to_pathtarget(expr, 0);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathKey {
    pub expr: Expr,
    pub ressortgroupref: usize,
    pub descending: bool,
    pub nulls_first: Option<bool>,
    pub collation_oid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestrictInfo {
    pub clause: Expr,
    pub required_relids: Vec<usize>,
    pub is_pushed_down: bool,
    pub can_join: bool,
    pub left_relids: Vec<usize>,
    pub right_relids: Vec<usize>,
    pub hashjoin_operator: Option<u32>,
}

impl RestrictInfo {
    pub fn new(clause: Expr, required_relids: Vec<usize>) -> Self {
        Self {
            required_relids,
            clause,
            is_pushed_down: true,
            can_join: false,
            left_relids: Vec::new(),
            right_relids: Vec::new(),
            hashjoin_operator: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecialJoinInfo {
    pub jointype: JoinType,
    pub rtindex: usize,
    pub ojrelid: Option<usize>,
    pub min_lefthand: Vec<usize>,
    pub min_righthand: Vec<usize>,
    pub syn_lefthand: Vec<usize>,
    pub syn_righthand: Vec<usize>,
    pub commute_above_l: Vec<usize>,
    pub commute_above_r: Vec<usize>,
    pub commute_below_l: Vec<usize>,
    pub commute_below_r: Vec<usize>,
    pub lhs_strict: bool,
    pub join_quals: Expr,
}

#[derive(Debug, Clone)]
pub struct RelOptInfo {
    pub relids: Vec<usize>,
    pub reloptkind: RelOptKind,
    pub reltarget: PathTarget,
    pub pathlist: Vec<Path>,
    pub cheapest_startup_path: Option<usize>,
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
            cheapest_startup_path: None,
            cheapest_total_path: None,
            baserestrictinfo: Vec::new(),
            joininfo: Vec::new(),
            rows: 0.0,
        }
    }

    pub fn add_path(&mut self, path: Path) {
        self.pathlist.push(path);
    }

    pub fn cheapest_startup_path(&self) -> Option<&Path> {
        self.cheapest_startup_path
            .and_then(|index| self.pathlist.get(index))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpperRelKind {
    GroupAgg,
    Window,
    ProjectSet,
    Ordered,
    Final,
}

#[derive(Debug, Clone)]
pub struct UpperRelEntry {
    pub kind: UpperRelKind,
    pub relids: Vec<usize>,
    pub reltarget: PathTarget,
    pub rel: RelOptInfo,
}

#[derive(Debug, Clone)]
pub struct PlannerInfo {
    pub parse: Query,
    pub simple_rel_array: Vec<Option<RelOptInfo>>,
    pub append_rel_infos: Vec<Option<AppendRelInfo>>,
    pub join_rel_list: Vec<RelOptInfo>,
    pub upper_rels: Vec<UpperRelEntry>,
    pub join_info_list: Vec<SpecialJoinInfo>,
    pub inner_join_clauses: Vec<RestrictInfo>,
    pub processed_tlist: Vec<TargetEntry>,
    pub scanjoin_target: PathTarget,
    pub group_input_target: PathTarget,
    pub grouped_target: PathTarget,
    pub window_input_target: PathTarget,
    pub sort_input_target: PathTarget,
    pub final_target: PathTarget,
    pub query_pathkeys: Vec<PathKey>,
    pub final_rel: Option<RelOptInfo>,
}

impl PlannerInfo {
    pub fn all_query_relids(&self) -> Vec<usize> {
        let mut relids = self
            .simple_rel_array
            .iter()
            .enumerate()
            .skip(1)
            .filter_map(|(rtindex, rel)| {
                rel.as_ref()
                    .filter(|rel| rel.reloptkind != RelOptKind::OtherMemberRel)
                    .map(|_| rtindex)
            })
            .collect::<Vec<_>>();
        relids.sort_unstable();
        relids.dedup();
        relids
    }
}

#[derive(Debug, Clone)]
pub struct PlannerSubroot(pub Rc<PlannerInfo>);

impl PlannerSubroot {
    pub fn new(root: PlannerInfo) -> Self {
        Self(Rc::new(root))
    }

    pub fn as_ref(&self) -> &PlannerInfo {
        self.0.as_ref()
    }
}

impl PartialEq for PlannerSubroot {
    fn eq(&self, other: &Self) -> bool {
        self.0.parse == other.0.parse
    }
}

impl Eq for PlannerSubroot {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Path {
    Result {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
    },
    Append {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        source_id: usize,
        desc: RelationDesc,
        children: Vec<Path>,
    },
    SeqScan {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        source_id: usize,
        rel: RelFileLocator,
        relation_name: String,
        relation_oid: u32,
        relkind: char,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
    },
    IndexScan {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        source_id: usize,
        rel: RelFileLocator,
        relation_name: String,
        relation_oid: u32,
        index_rel: RelFileLocator,
        index_name: String,
        am_oid: u32,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
        index_desc: RelationDesc,
        index_meta: IndexRelCacheEntry,
        keys: Vec<IndexScanKey>,
        order_by_keys: Vec<IndexScanKey>,
        direction: ScanDirection,
        pathkeys: Vec<PathKey>,
    },
    BitmapIndexScan {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        source_id: usize,
        rel: RelFileLocator,
        relation_oid: u32,
        index_rel: RelFileLocator,
        am_oid: u32,
        desc: RelationDesc,
        index_desc: RelationDesc,
        index_meta: IndexRelCacheEntry,
        keys: Vec<IndexScanKey>,
        index_quals: Vec<Expr>,
    },
    BitmapHeapScan {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        source_id: usize,
        rel: RelFileLocator,
        relation_name: String,
        relation_oid: u32,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
        bitmapqual: Box<Path>,
        recheck_qual: Vec<Expr>,
    },
    Filter {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        input: Box<Path>,
        predicate: Expr,
    },
    NestedLoopJoin {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        output_columns: Vec<QueryColumn>,
        left: Box<Path>,
        right: Box<Path>,
        kind: JoinType,
        restrict_clauses: Vec<RestrictInfo>,
    },
    HashJoin {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        output_columns: Vec<QueryColumn>,
        left: Box<Path>,
        right: Box<Path>,
        kind: JoinType,
        hash_clauses: Vec<RestrictInfo>,
        outer_hash_keys: Vec<Expr>,
        inner_hash_keys: Vec<Expr>,
        restrict_clauses: Vec<RestrictInfo>,
    },
    Projection {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        slot_id: usize,
        input: Box<Path>,
        targets: Vec<TargetEntry>,
    },
    OrderBy {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        input: Box<Path>,
        items: Vec<OrderByEntry>,
    },
    Limit {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        input: Box<Path>,
        limit: Option<usize>,
        offset: usize,
    },
    LockRows {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        input: Box<Path>,
        row_marks: Vec<QueryRowMark>,
    },
    Aggregate {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        slot_id: usize,
        input: Box<Path>,
        group_by: Vec<Expr>,
        accumulators: Vec<AggAccum>,
        having: Option<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    WindowAgg {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        slot_id: usize,
        input: Box<Path>,
        clause: WindowClause,
        output_columns: Vec<QueryColumn>,
    },
    Values {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        slot_id: usize,
        rows: Vec<Vec<Expr>>,
        output_columns: Vec<QueryColumn>,
    },
    FunctionScan {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        slot_id: usize,
        call: SetReturningCall,
    },
    SubqueryScan {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        rtindex: usize,
        subroot: PlannerSubroot,
        query: Box<Query>,
        input: Box<Path>,
        output_columns: Vec<QueryColumn>,
        pathkeys: Vec<PathKey>,
    },
    CteScan {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        slot_id: usize,
        cte_id: usize,
        subroot: PlannerSubroot,
        query: Box<Query>,
        cte_plan: Box<Path>,
        output_columns: Vec<QueryColumn>,
    },
    WorkTableScan {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        slot_id: usize,
        worktable_id: usize,
        output_columns: Vec<QueryColumn>,
    },
    RecursiveUnion {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        slot_id: usize,
        worktable_id: usize,
        distinct: bool,
        anchor_root: PlannerSubroot,
        recursive_root: PlannerSubroot,
        recursive_references_worktable: bool,
        anchor_query: Box<Query>,
        recursive_query: Box<Query>,
        output_columns: Vec<QueryColumn>,
        anchor: Box<Path>,
        recursive: Box<Path>,
    },
    SetOp {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        slot_id: usize,
        op: SetOperator,
        output_columns: Vec<QueryColumn>,
        child_roots: Vec<Option<PlannerSubroot>>,
        children: Vec<Path>,
    },
    ProjectSet {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        slot_id: usize,
        input: Box<Path>,
        targets: Vec<ProjectSetTarget>,
    },
}
