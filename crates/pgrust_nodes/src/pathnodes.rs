use crate::access::ScanDirection;
use crate::parsenodes::SetOperator;
use crate::parsenodes::TableSampleClause;
use crate::parsenodes::{PartitionStrategy, Query, QueryRowMark, RangeTblEntry, RangeTblEntryKind};
use crate::partition::{LoweredPartitionSpec, PartitionBoundSpec};
use crate::plannodes::{
    AggregatePhase, AggregateStrategy, EstimateValue, IndexScanKey, PartitionPrunePlan,
    PlanEstimate,
};
use crate::primnodes::{
    AggAccum, Expr, JoinType, OrderByEntry, ProjectSetTarget, QueryColumn, RelationDesc,
    SetReturningCall, SortGroupClause, TargetEntry, ToastRelationRef, Var, WindowClause,
    user_attrno,
};
use crate::relcache::IndexRelCacheEntry;
use pgrust_core::PgInheritsRow;
use pgrust_core::RelFileLocator;
use std::cell::RefCell;
use std::collections::BTreeMap;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerConfig {
    pub enable_partitionwise_join: bool,
    pub enable_partitionwise_aggregate: bool,
    pub enable_seqscan: bool,
    pub enable_indexscan: bool,
    pub enable_indexonlyscan: bool,
    pub enable_bitmapscan: bool,
    pub enable_nestloop: bool,
    pub enable_hashjoin: bool,
    pub enable_mergejoin: bool,
    pub enable_memoize: bool,
    pub enable_material: bool,
    pub enable_partition_pruning: bool,
    pub constraint_exclusion_on: bool,
    pub constraint_exclusion_partition: bool,
    pub retain_partial_index_filters: bool,
    pub enable_hashagg: bool,
    pub enable_presorted_aggregate: bool,
    pub enable_sort: bool,
    pub enable_parallel_append: bool,
    pub enable_parallel_hash: bool,
    pub force_parallel_gather: bool,
    pub max_parallel_workers: usize,
    pub max_parallel_workers_per_gather: usize,
    pub parallel_leader_participation: bool,
    pub min_parallel_table_scan_size: usize,
    pub min_parallel_index_scan_size: usize,
    pub parallel_setup_cost: EstimateValue,
    pub parallel_tuple_cost: EstimateValue,
    pub fold_constants: bool,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            enable_partitionwise_join: false,
            enable_partitionwise_aggregate: false,
            enable_seqscan: true,
            enable_indexscan: true,
            enable_indexonlyscan: true,
            enable_bitmapscan: true,
            enable_nestloop: true,
            enable_hashjoin: true,
            enable_mergejoin: true,
            enable_memoize: true,
            enable_material: true,
            enable_partition_pruning: true,
            constraint_exclusion_on: false,
            constraint_exclusion_partition: true,
            retain_partial_index_filters: false,
            enable_hashagg: true,
            enable_presorted_aggregate: true,
            enable_sort: true,
            enable_parallel_append: true,
            enable_parallel_hash: true,
            force_parallel_gather: false,
            max_parallel_workers: 8,
            max_parallel_workers_per_gather: 2,
            parallel_leader_participation: true,
            min_parallel_table_scan_size: 8 * 1024 * 1024,
            min_parallel_index_scan_size: 512 * 1024,
            parallel_setup_cost: EstimateValue(1000.0),
            parallel_tuple_cost: EstimateValue(0.1),
            fold_constants: true,
        }
    }
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
                            collation_oid: None,
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
    pub security_level: usize,
    pub leakproof: bool,
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
            security_level: 0,
            leakproof: false,
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
    pub partition_info: Option<PartitionInfo>,
    pub consider_partitionwise_join: bool,
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
            partition_info: None,
            consider_partitionwise_join: false,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    pub strategy: PartitionStrategy,
    pub partattrs: Vec<i16>,
    pub partclass: Vec<u32>,
    pub partcollation: Vec<u32>,
    pub key_exprs: Vec<Expr>,
    pub members: Vec<PartitionMember>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMember {
    pub relids: Vec<usize>,
    pub bound: Option<PartitionBoundSpec>,
}

#[derive(Debug, Clone)]
pub struct PlannerGlobal {
    pub subplans: Vec<crate::plannodes::Plan>,
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
    Distinct,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AggregateLayout {
    pub group_by: Vec<Expr>,
    pub group_by_refs: Vec<usize>,
    pub passthrough_exprs: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerPartitionChildBound {
    pub row: PgInheritsRow,
    pub bound: Option<PartitionBoundSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerIndexExprCacheEntry {
    pub exprs: Vec<Expr>,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct PlannerInfo {
    pub config: PlannerConfig,
    pub parse: Query,
    pub simple_rel_array: Vec<Option<RelOptInfo>>,
    pub append_rel_infos: Vec<Option<AppendRelInfo>>,
    pub join_rel_list: Vec<RelOptInfo>,
    pub upper_rels: Vec<UpperRelEntry>,
    pub join_info_list: Vec<SpecialJoinInfo>,
    pub inner_join_clauses: Vec<RestrictInfo>,
    pub aggregate_layout: AggregateLayout,
    pub processed_tlist: Vec<TargetEntry>,
    pub scanjoin_target: PathTarget,
    pub group_input_target: PathTarget,
    pub grouped_target: PathTarget,
    pub window_input_target: PathTarget,
    pub sort_input_target: PathTarget,
    pub final_target: PathTarget,
    pub query_pathkeys: Vec<PathKey>,
    pub final_rel: Option<RelOptInfo>,
    pub partition_spec_cache: RefCell<BTreeMap<u32, Option<LoweredPartitionSpec>>>,
    pub partition_child_bounds_cache: RefCell<BTreeMap<u32, Vec<PlannerPartitionChildBound>>>,
    pub index_expr_cache: RefCell<BTreeMap<u32, PlannerIndexExprCacheEntry>>,
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
        pathkeys: Vec<PathKey>,
        relids: Vec<usize>,
        source_id: usize,
        desc: RelationDesc,
        child_roots: Vec<Option<PlannerSubroot>>,
        partition_prune: Option<PartitionPrunePlan>,
        children: Vec<Path>,
    },
    MergeAppend {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        source_id: usize,
        desc: RelationDesc,
        items: Vec<OrderByEntry>,
        partition_prune: Option<PartitionPrunePlan>,
        children: Vec<Path>,
    },
    Unique {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        key_indices: Vec<usize>,
        input: Box<Path>,
    },
    SeqScan {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        source_id: usize,
        rel: RelFileLocator,
        relation_name: String,
        relation_oid: u32,
        relkind: char,
        relispopulated: bool,
        toast: Option<ToastRelationRef>,
        tablesample: Option<TableSampleClause>,
        desc: RelationDesc,
        disabled: bool,
    },
    IndexOnlyScan {
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
        index_only: bool,
        pathkeys: Vec<PathKey>,
    },
    BitmapIndexScan {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        source_id: usize,
        rel: RelFileLocator,
        relation_oid: u32,
        index_rel: RelFileLocator,
        index_name: String,
        am_oid: u32,
        desc: RelationDesc,
        index_desc: RelationDesc,
        index_meta: IndexRelCacheEntry,
        keys: Vec<IndexScanKey>,
        index_quals: Vec<Expr>,
    },
    BitmapOr {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        children: Vec<Path>,
    },
    BitmapAnd {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        children: Vec<Path>,
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
        filter_qual: Vec<Expr>,
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
    MergeJoin {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        output_columns: Vec<QueryColumn>,
        left: Box<Path>,
        right: Box<Path>,
        kind: JoinType,
        merge_clauses: Vec<RestrictInfo>,
        outer_merge_keys: Vec<Expr>,
        inner_merge_keys: Vec<Expr>,
        merge_key_descending: Vec<bool>,
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
        display_items: Vec<String>,
    },
    IncrementalSort {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        input: Box<Path>,
        items: Vec<OrderByEntry>,
        presorted_count: usize,
        display_items: Vec<String>,
        presorted_display_items: Vec<String>,
    },
    Limit {
        plan_info: PlanEstimate,
        pathtarget: PathTarget,
        input: Box<Path>,
        limit: Option<Expr>,
        offset: Option<Expr>,
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
        strategy: AggregateStrategy,
        phase: AggregatePhase,
        semantic_accumulators: Option<Vec<AggAccum>>,
        disabled: bool,
        pathkeys: Vec<PathKey>,
        input: Box<Path>,
        group_by: Vec<Expr>,
        group_by_refs: Vec<usize>,
        grouping_sets: Vec<Vec<usize>>,
        passthrough_exprs: Vec<Expr>,
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
        run_condition: Option<Expr>,
        top_qual: Option<Expr>,
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
        table_alias: Option<String>,
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
        cte_name: String,
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
        strategy: crate::plannodes::SetOpStrategy,
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
