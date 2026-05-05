use crate::CommandType;
use crate::access::ScanDirection;
use crate::access::ScanKeyData;
use crate::datum::Value;
use crate::parsenodes::{SelectLockingClause, SetOperator, TableSampleClause};
use crate::partition::{LoweredPartitionSpec, PartitionBoundSpec};
use crate::primnodes::{
    AggAccum, Expr, JoinType, OrderByEntry, ProjectSetTarget, QueryColumn, RelationDesc,
    RelationPrivilegeRequirement, SetReturningCall, TargetEntry, ToastRelationRef, WindowClause,
};
use crate::relcache::IndexRelCacheEntry;
use pgrust_core::RelFileLocator;

#[derive(Debug, Clone, Copy, Default)]
pub struct EstimateValue(pub f64);

impl EstimateValue {
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

impl PartialEq for EstimateValue {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for EstimateValue {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlanEstimate {
    pub startup_cost: EstimateValue,
    pub total_cost: EstimateValue,
    pub plan_rows: EstimateValue,
    pub plan_width: usize,
}

impl PlanEstimate {
    pub fn new(startup_cost: f64, total_cost: f64, plan_rows: f64, plan_width: usize) -> Self {
        Self {
            startup_cost: EstimateValue(startup_cost),
            total_cost: EstimateValue(total_cost),
            plan_rows: EstimateValue(plan_rows),
            plan_width,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecParamSource {
    pub paramid: usize,
    pub expr: Expr,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexScanKeyArgument {
    Const(Value),
    Runtime(Expr),
}

impl IndexScanKeyArgument {
    pub fn as_const(&self) -> Option<&Value> {
        match self {
            IndexScanKeyArgument::Const(value) => Some(value),
            IndexScanKeyArgument::Runtime(_) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateStrategy {
    Plain,
    Sorted,
    Hashed,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregatePhase {
    Complete,
    Partial,
    Finalize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpStrategy {
    Sorted,
    Hashed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexScanKey {
    pub attribute_number: i16,
    pub strategy: u16,
    pub argument: IndexScanKeyArgument,
    pub display_expr: Option<Expr>,
    pub runtime_label: Option<String>,
}

impl IndexScanKey {
    pub fn new(attribute_number: i16, strategy: u16, argument: IndexScanKeyArgument) -> Self {
        Self {
            attribute_number,
            strategy,
            argument,
            display_expr: None,
            runtime_label: None,
        }
    }

    pub fn with_display_expr(mut self, display_expr: Option<Expr>) -> Self {
        self.display_expr = display_expr;
        self
    }

    pub fn with_runtime_label(mut self, runtime_label: Option<String>) -> Self {
        self.runtime_label = runtime_label;
        self
    }

    pub fn const_value(attribute_number: i16, strategy: u16, argument: Value) -> Self {
        Self::new(
            attribute_number,
            strategy,
            IndexScanKeyArgument::Const(argument),
        )
    }

    pub fn to_scan_key(&self, argument: Value) -> ScanKeyData {
        ScanKeyData {
            attribute_number: self.attribute_number,
            strategy: self.strategy,
            argument,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanRowMark {
    pub rtindex: usize,
    pub relation_name: String,
    pub relation_oid: u32,
    pub rel: RelFileLocator,
    pub strength: SelectLockingClause,
    pub nowait: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedStmt {
    pub command_type: CommandType,
    pub depends_on_row_security: bool,
    pub relation_privileges: Vec<RelationPrivilegeRequirement>,
    pub plan_tree: Plan,
    pub subplans: Vec<Plan>,
    pub ext_params: Vec<ExecParamSource>,
}

impl PlannedStmt {
    pub fn columns(&self) -> Vec<QueryColumn> {
        self.plan_tree.columns()
    }

    pub fn column_names(&self) -> Vec<String> {
        self.plan_tree.column_names()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPruneChildDomain {
    pub spec: LoweredPartitionSpec,
    pub sibling_bounds: Vec<PartitionBoundSpec>,
    pub bound: Option<PartitionBoundSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPrunePlan {
    pub spec: LoweredPartitionSpec,
    pub sibling_bounds: Vec<PartitionBoundSpec>,
    pub filter: Expr,
    pub child_bounds: Vec<Option<PartitionBoundSpec>>,
    pub child_domains: Vec<Vec<PartitionPruneChildDomain>>,
    pub subplans_removed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TidScanSource {
    Scalar(Expr),
    Array(Expr),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TidScanCond {
    pub sources: Vec<TidScanSource>,
    pub display_expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TidRangeScanCond {
    pub display_expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    Result {
        plan_info: PlanEstimate,
    },
    Append {
        plan_info: PlanEstimate,
        source_id: usize,
        desc: RelationDesc,
        parallel_aware: bool,
        partition_prune: Option<PartitionPrunePlan>,
        children: Vec<Plan>,
    },
    MergeAppend {
        plan_info: PlanEstimate,
        source_id: usize,
        desc: RelationDesc,
        items: Vec<OrderByEntry>,
        partition_prune: Option<PartitionPrunePlan>,
        children: Vec<Plan>,
    },
    Unique {
        plan_info: PlanEstimate,
        key_indices: Vec<usize>,
        input: Box<Plan>,
    },
    SeqScan {
        plan_info: PlanEstimate,
        source_id: usize,
        parallel_scan_id: Option<usize>,
        rel: RelFileLocator,
        relation_name: String,
        relation_oid: u32,
        relkind: char,
        relispopulated: bool,
        toast: Option<ToastRelationRef>,
        tablesample: Option<TableSampleClause>,
        desc: RelationDesc,
        disabled: bool,
        parallel_aware: bool,
    },
    TidScan {
        plan_info: PlanEstimate,
        source_id: usize,
        rel: RelFileLocator,
        relation_name: String,
        relation_oid: u32,
        relkind: char,
        relispopulated: bool,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
        tid_cond: TidScanCond,
        filter: Option<Expr>,
    },
    TidRangeScan {
        plan_info: PlanEstimate,
        source_id: usize,
        rel: RelFileLocator,
        relation_name: String,
        relation_oid: u32,
        relkind: char,
        relispopulated: bool,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
        tid_range_cond: TidRangeScanCond,
        filter: Option<Expr>,
    },
    IndexOnlyScan {
        plan_info: PlanEstimate,
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
        parallel_aware: bool,
    },
    IndexScan {
        plan_info: PlanEstimate,
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
        parallel_aware: bool,
    },
    BitmapIndexScan {
        plan_info: PlanEstimate,
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
        children: Vec<Plan>,
    },
    BitmapAnd {
        plan_info: PlanEstimate,
        children: Vec<Plan>,
    },
    BitmapHeapScan {
        plan_info: PlanEstimate,
        source_id: usize,
        rel: RelFileLocator,
        relation_name: String,
        relation_oid: u32,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
        bitmapqual: Box<Plan>,
        recheck_qual: Vec<Expr>,
        filter_qual: Vec<Expr>,
        parallel_aware: bool,
    },
    Hash {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        hash_keys: Vec<Expr>,
    },
    Materialize {
        plan_info: PlanEstimate,
        input: Box<Plan>,
    },
    Memoize {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        cache_keys: Vec<Expr>,
        cache_key_labels: Vec<String>,
        key_paramids: Vec<usize>,
        dependent_paramids: Vec<usize>,
        binary_mode: bool,
        single_row: bool,
        est_entries: usize,
    },
    Gather {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        workers_planned: usize,
        single_copy: bool,
    },
    GatherMerge {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        workers_planned: usize,
        items: Vec<OrderByEntry>,
        display_items: Vec<String>,
    },
    NestedLoopJoin {
        plan_info: PlanEstimate,
        left: Box<Plan>,
        right: Box<Plan>,
        kind: JoinType,
        nest_params: Vec<ExecParamSource>,
        join_qual: Vec<Expr>,
        qual: Vec<Expr>,
    },
    HashJoin {
        plan_info: PlanEstimate,
        left: Box<Plan>,
        right: Box<Plan>,
        kind: JoinType,
        hash_clauses: Vec<Expr>,
        hash_keys: Vec<Expr>,
        join_qual: Vec<Expr>,
        qual: Vec<Expr>,
    },
    MergeJoin {
        plan_info: PlanEstimate,
        left: Box<Plan>,
        right: Box<Plan>,
        kind: JoinType,
        merge_clauses: Vec<Expr>,
        outer_merge_keys: Vec<Expr>,
        inner_merge_keys: Vec<Expr>,
        merge_key_descending: Vec<bool>,
        join_qual: Vec<Expr>,
        qual: Vec<Expr>,
    },
    Filter {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        predicate: Expr,
    },
    OrderBy {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        items: Vec<OrderByEntry>,
        display_items: Vec<String>,
    },
    IncrementalSort {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        items: Vec<OrderByEntry>,
        presorted_count: usize,
        display_items: Vec<String>,
        presorted_display_items: Vec<String>,
    },
    Limit {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        limit: Option<Expr>,
        offset: Option<Expr>,
    },
    LockRows {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        row_marks: Vec<PlanRowMark>,
    },
    Projection {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        targets: Vec<TargetEntry>,
    },
    Aggregate {
        plan_info: PlanEstimate,
        strategy: AggregateStrategy,
        phase: AggregatePhase,
        disabled: bool,
        input: Box<Plan>,
        group_by: Vec<Expr>,
        group_by_refs: Vec<usize>,
        grouping_sets: Vec<Vec<usize>>,
        passthrough_exprs: Vec<Expr>,
        accumulators: Vec<AggAccum>,
        semantic_accumulators: Option<Vec<AggAccum>>,
        semantic_output_names: Option<Vec<String>>,
        having: Option<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    WindowAgg {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        clause: WindowClause,
        run_condition: Option<Expr>,
        top_qual: Option<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    FunctionScan {
        plan_info: PlanEstimate,
        call: SetReturningCall,
        table_alias: Option<String>,
    },
    SubqueryScan {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        scan_name: Option<String>,
        filter: Option<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    CteScan {
        plan_info: PlanEstimate,
        cte_id: usize,
        cte_name: String,
        cte_plan: Box<Plan>,
        output_columns: Vec<QueryColumn>,
    },
    WorkTableScan {
        plan_info: PlanEstimate,
        worktable_id: usize,
        output_columns: Vec<QueryColumn>,
    },
    RecursiveUnion {
        plan_info: PlanEstimate,
        worktable_id: usize,
        distinct: bool,
        recursive_references_worktable: bool,
        output_columns: Vec<QueryColumn>,
        anchor: Box<Plan>,
        recursive: Box<Plan>,
    },
    SetOp {
        plan_info: PlanEstimate,
        op: SetOperator,
        strategy: SetOpStrategy,
        output_columns: Vec<QueryColumn>,
        children: Vec<Plan>,
    },
    Values {
        plan_info: PlanEstimate,
        rows: Vec<Vec<Expr>>,
        output_columns: Vec<QueryColumn>,
    },
    ProjectSet {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        targets: Vec<ProjectSetTarget>,
    },
}

impl Plan {
    pub fn plan_info(&self) -> PlanEstimate {
        match self {
            Plan::Result { plan_info }
            | Plan::Append { plan_info, .. }
            | Plan::MergeAppend { plan_info, .. }
            | Plan::Unique { plan_info, .. }
            | Plan::SeqScan { plan_info, .. }
            | Plan::TidScan { plan_info, .. }
            | Plan::TidRangeScan { plan_info, .. }
            | Plan::IndexOnlyScan { plan_info, .. }
            | Plan::IndexScan { plan_info, .. }
            | Plan::BitmapIndexScan { plan_info, .. }
            | Plan::BitmapOr { plan_info, .. }
            | Plan::BitmapAnd { plan_info, .. }
            | Plan::BitmapHeapScan { plan_info, .. }
            | Plan::Hash { plan_info, .. }
            | Plan::Materialize { plan_info, .. }
            | Plan::Memoize { plan_info, .. }
            | Plan::Gather { plan_info, .. }
            | Plan::GatherMerge { plan_info, .. }
            | Plan::NestedLoopJoin { plan_info, .. }
            | Plan::HashJoin { plan_info, .. }
            | Plan::MergeJoin { plan_info, .. }
            | Plan::Filter { plan_info, .. }
            | Plan::OrderBy { plan_info, .. }
            | Plan::IncrementalSort { plan_info, .. }
            | Plan::Limit { plan_info, .. }
            | Plan::LockRows { plan_info, .. }
            | Plan::Projection { plan_info, .. }
            | Plan::Aggregate { plan_info, .. }
            | Plan::WindowAgg { plan_info, .. }
            | Plan::SubqueryScan { plan_info, .. }
            | Plan::CteScan { plan_info, .. }
            | Plan::WorkTableScan { plan_info, .. }
            | Plan::RecursiveUnion { plan_info, .. }
            | Plan::SetOp { plan_info, .. }
            | Plan::FunctionScan { plan_info, .. }
            | Plan::Values { plan_info, .. }
            | Plan::ProjectSet { plan_info, .. } => *plan_info,
        }
    }

    pub fn set_plan_info(&mut self, value: PlanEstimate) {
        match self {
            Plan::Result { plan_info }
            | Plan::Append { plan_info, .. }
            | Plan::MergeAppend { plan_info, .. }
            | Plan::Unique { plan_info, .. }
            | Plan::SeqScan { plan_info, .. }
            | Plan::TidScan { plan_info, .. }
            | Plan::TidRangeScan { plan_info, .. }
            | Plan::IndexOnlyScan { plan_info, .. }
            | Plan::IndexScan { plan_info, .. }
            | Plan::BitmapIndexScan { plan_info, .. }
            | Plan::BitmapOr { plan_info, .. }
            | Plan::BitmapAnd { plan_info, .. }
            | Plan::BitmapHeapScan { plan_info, .. }
            | Plan::Hash { plan_info, .. }
            | Plan::Materialize { plan_info, .. }
            | Plan::Memoize { plan_info, .. }
            | Plan::Gather { plan_info, .. }
            | Plan::GatherMerge { plan_info, .. }
            | Plan::NestedLoopJoin { plan_info, .. }
            | Plan::HashJoin { plan_info, .. }
            | Plan::MergeJoin { plan_info, .. }
            | Plan::Filter { plan_info, .. }
            | Plan::OrderBy { plan_info, .. }
            | Plan::IncrementalSort { plan_info, .. }
            | Plan::Limit { plan_info, .. }
            | Plan::LockRows { plan_info, .. }
            | Plan::Projection { plan_info, .. }
            | Plan::Aggregate { plan_info, .. }
            | Plan::WindowAgg { plan_info, .. }
            | Plan::SubqueryScan { plan_info, .. }
            | Plan::CteScan { plan_info, .. }
            | Plan::WorkTableScan { plan_info, .. }
            | Plan::RecursiveUnion { plan_info, .. }
            | Plan::SetOp { plan_info, .. }
            | Plan::FunctionScan { plan_info, .. }
            | Plan::Values { plan_info, .. }
            | Plan::ProjectSet { plan_info, .. } => *plan_info = value,
        }
    }

    pub fn columns(&self) -> Vec<QueryColumn> {
        match self {
            Plan::Result { .. } => vec![],
            Plan::Append { desc, .. } | Plan::MergeAppend { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                    wire_type_oid: None,
                })
                .collect(),
            Plan::Unique { input, .. } => input.columns(),
            Plan::SeqScan { desc, .. }
            | Plan::TidScan { desc, .. }
            | Plan::TidRangeScan { desc, .. }
            | Plan::IndexOnlyScan { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                    wire_type_oid: None,
                })
                .collect(),
            Plan::IndexScan { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                    wire_type_oid: None,
                })
                .collect(),
            Plan::BitmapIndexScan { .. } | Plan::BitmapOr { .. } | Plan::BitmapAnd { .. } => {
                vec![]
            }
            Plan::BitmapHeapScan { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                    wire_type_oid: None,
                })
                .collect(),
            Plan::Hash { input, .. }
            | Plan::Materialize { input, .. }
            | Plan::Memoize { input, .. }
            | Plan::Gather { input, .. }
            | Plan::GatherMerge { input, .. } => input.columns(),
            Plan::Filter { input, .. }
            | Plan::OrderBy { input, .. }
            | Plan::IncrementalSort { input, .. }
            | Plan::Limit { input, .. }
            | Plan::LockRows { input, .. } => input.columns(),
            Plan::Projection { targets, .. } => targets
                .iter()
                .map(|t| QueryColumn {
                    name: t.name.clone(),
                    sql_type: t.sql_type,
                    wire_type_oid: None,
                })
                .collect(),
            Plan::Aggregate { output_columns, .. } => output_columns.clone(),
            Plan::WindowAgg { output_columns, .. } => output_columns.clone(),
            Plan::SubqueryScan { output_columns, .. } => output_columns.clone(),
            Plan::CteScan { output_columns, .. } => output_columns.clone(),
            Plan::WorkTableScan { output_columns, .. }
            | Plan::RecursiveUnion { output_columns, .. }
            | Plan::SetOp { output_columns, .. } => output_columns.clone(),
            Plan::NestedLoopJoin {
                left, right, kind, ..
            }
            | Plan::HashJoin {
                left, right, kind, ..
            }
            | Plan::MergeJoin {
                left, right, kind, ..
            } => {
                if matches!(kind, JoinType::Semi | JoinType::Anti) {
                    return left.columns();
                }
                let mut cols = left.columns();
                if !matches!(
                    kind,
                    crate::primnodes::JoinType::Semi | crate::primnodes::JoinType::Anti
                ) {
                    cols.extend(right.columns());
                }
                cols
            }
            Plan::FunctionScan { call, .. } => call.output_columns().to_vec(),
            Plan::Values { output_columns, .. } => output_columns.clone(),
            Plan::ProjectSet { targets, .. } => targets
                .iter()
                .map(|target| match target {
                    ProjectSetTarget::Scalar(entry) => QueryColumn {
                        name: entry.name.clone(),
                        sql_type: entry.sql_type,
                        wire_type_oid: None,
                    },
                    ProjectSetTarget::Set { name, sql_type, .. } => QueryColumn {
                        name: name.clone(),
                        sql_type: *sql_type,
                        wire_type_oid: None,
                    },
                })
                .collect(),
        }
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns().into_iter().map(|c| c.name).collect()
    }
}
