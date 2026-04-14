use crate::RelFileLocator;
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::access::relscan::ScanDirection;
use crate::include::access::scankey::ScanKeyData;
use crate::include::executor::execdesc::CommandType;
use crate::include::nodes::parsenodes::Query;
pub use crate::include::nodes::pathnodes::{
    PlannerJoinArraySubscript, PlannerJoinExpr, PlannerOrderByEntry, PlannerProjectSetTarget,
    PlannerTargetEntry,
};
pub use crate::include::nodes::primnodes::{
    AggAccum, AggFunc, BuiltinScalarFunction, ColumnDesc, Expr, ExprArraySubscript, JoinType,
    JsonTableFunction, OrderByEntry, ProjectSetTarget, QueryColumn, RegexTableFunction,
    RelationDesc, ScalarType, SetReturningCall, SortGroupClause, TargetEntry,
    TextSearchTableFunction, ToastRelationRef, Var,
};

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
pub struct PlannedStmt {
    pub command_type: CommandType,
    pub plan_tree: Plan,
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
pub enum Plan {
    Result {
        plan_info: PlanEstimate,
    },
    SeqScan {
        plan_info: PlanEstimate,
        rel: RelFileLocator,
        relation_oid: u32,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
    },
    IndexScan {
        plan_info: PlanEstimate,
        rel: RelFileLocator,
        index_rel: RelFileLocator,
        am_oid: u32,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
        index_meta: IndexRelCacheEntry,
        keys: Vec<ScanKeyData>,
        direction: ScanDirection,
    },
    NestedLoopJoin {
        plan_info: PlanEstimate,
        left: Box<Plan>,
        right: Box<Plan>,
        kind: JoinType,
        on: Expr,
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
    },
    Limit {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        limit: Option<usize>,
        offset: usize,
    },
    Projection {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        targets: Vec<TargetEntry>,
    },
    Aggregate {
        plan_info: PlanEstimate,
        input: Box<Plan>,
        group_by: Vec<Expr>,
        accumulators: Vec<AggAccum>,
        having: Option<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    FunctionScan {
        plan_info: PlanEstimate,
        call: SetReturningCall,
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

// :HACK: Transitional wrapper while pgrust still lets subqueries move around as
// either semantic Query trees or executable Plan trees. PostgreSQL does not use
// a single enum like this: expression subqueries stay as semantic SubLink/Query
// until planning, then become SubPlan references into PlannedStmt.subplans.
// PostgreSQL also identifies functions and aggregates in semantic nodes by OID
// (for example FuncExpr.funcid and Aggref.aggfnoid), not by pgrust-specific
// builtin enums such as BuiltinScalarFunction or AggFunc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeferredSelectPlan {
    Bound(Box<Query>),
    Planned(Box<Plan>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundSelectPlan {
    From(BoundFromPlan),
    Filter {
        input: Box<BoundSelectPlan>,
        predicate: Expr,
    },
    OrderBy {
        input: Box<BoundSelectPlan>,
        items: Vec<OrderByEntry>,
    },
    Limit {
        input: Box<BoundSelectPlan>,
        limit: Option<usize>,
        offset: usize,
    },
    Aggregate {
        input: Box<BoundSelectPlan>,
        group_by: Vec<Expr>,
        accumulators: Vec<AggAccum>,
        having: Option<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    Projection {
        input: Box<BoundSelectPlan>,
        targets: Vec<TargetEntry>,
    },
    ProjectSet {
        input: Box<BoundSelectPlan>,
        targets: Vec<ProjectSetTarget>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundFromPlan {
    Result,
    SeqScan {
        rel: RelFileLocator,
        relation_oid: u32,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
    },
    Values {
        rows: Vec<Vec<Expr>>,
        output_columns: Vec<QueryColumn>,
    },
    FunctionScan {
        call: SetReturningCall,
    },
    NestedLoopJoin {
        left: Box<BoundFromPlan>,
        right: Box<BoundFromPlan>,
        kind: JoinType,
        on: Expr,
    },
    Projection {
        input: Box<BoundFromPlan>,
        targets: Vec<TargetEntry>,
    },
    Subquery(Box<Query>),
}

impl Plan {
    pub fn plan_info(&self) -> PlanEstimate {
        match self {
            Plan::Result { plan_info }
            | Plan::SeqScan { plan_info, .. }
            | Plan::IndexScan { plan_info, .. }
            | Plan::NestedLoopJoin { plan_info, .. }
            | Plan::Filter { plan_info, .. }
            | Plan::OrderBy { plan_info, .. }
            | Plan::Limit { plan_info, .. }
            | Plan::Projection { plan_info, .. }
            | Plan::Aggregate { plan_info, .. }
            | Plan::FunctionScan { plan_info, .. }
            | Plan::Values { plan_info, .. }
            | Plan::ProjectSet { plan_info, .. } => *plan_info,
        }
    }

    pub fn set_plan_info(&mut self, value: PlanEstimate) {
        match self {
            Plan::Result { plan_info }
            | Plan::SeqScan { plan_info, .. }
            | Plan::IndexScan { plan_info, .. }
            | Plan::NestedLoopJoin { plan_info, .. }
            | Plan::Filter { plan_info, .. }
            | Plan::OrderBy { plan_info, .. }
            | Plan::Limit { plan_info, .. }
            | Plan::Projection { plan_info, .. }
            | Plan::Aggregate { plan_info, .. }
            | Plan::FunctionScan { plan_info, .. }
            | Plan::Values { plan_info, .. }
            | Plan::ProjectSet { plan_info, .. } => *plan_info = value,
        }
    }

    pub fn columns(&self) -> Vec<QueryColumn> {
        match self {
            Plan::Result { .. } => vec![],
            Plan::SeqScan { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                })
                .collect(),
            Plan::IndexScan { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                })
                .collect(),
            Plan::Filter { input, .. }
            | Plan::OrderBy { input, .. }
            | Plan::Limit { input, .. } => input.columns(),
            Plan::Projection { targets, .. } => targets
                .iter()
                .map(|t| QueryColumn {
                    name: t.name.clone(),
                    sql_type: t.sql_type,
                })
                .collect(),
            Plan::Aggregate { output_columns, .. } => output_columns.clone(),
            Plan::NestedLoopJoin { left, right, .. } => {
                let mut cols = left.columns();
                cols.extend(right.columns());
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
                    },
                    ProjectSetTarget::Set { name, sql_type, .. } => QueryColumn {
                        name: name.clone(),
                        sql_type: *sql_type,
                    },
                })
                .collect(),
        }
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns().into_iter().map(|c| c.name).collect()
    }
}

impl DeferredSelectPlan {
    pub fn columns(&self) -> Vec<QueryColumn> {
        match self {
            Self::Bound(plan) => plan.columns(),
            Self::Planned(plan) => plan.columns(),
        }
    }
}

impl BoundSelectPlan {
    pub fn columns(&self) -> Vec<QueryColumn> {
        match self {
            Self::From(plan) => plan.columns(),
            Self::Filter { input, .. }
            | Self::OrderBy { input, .. }
            | Self::Limit { input, .. } => input.columns(),
            Self::Aggregate { output_columns, .. } => output_columns.clone(),
            Self::Projection { targets, .. } => targets
                .iter()
                .map(|target| QueryColumn {
                    name: target.name.clone(),
                    sql_type: target.sql_type,
                })
                .collect(),
            Self::ProjectSet { targets, .. } => targets
                .iter()
                .map(|target| match target {
                    ProjectSetTarget::Scalar(entry) => QueryColumn {
                        name: entry.name.clone(),
                        sql_type: entry.sql_type,
                    },
                    ProjectSetTarget::Set { name, sql_type, .. } => QueryColumn {
                        name: name.clone(),
                        sql_type: *sql_type,
                    },
                })
                .collect(),
        }
    }
}

impl BoundFromPlan {
    pub fn columns(&self) -> Vec<QueryColumn> {
        match self {
            Self::Result => Vec::new(),
            Self::SeqScan { desc, .. } => desc
                .columns
                .iter()
                .map(|column| QueryColumn {
                    name: column.name.clone(),
                    sql_type: column.sql_type,
                })
                .collect(),
            Self::Values { output_columns, .. } => output_columns.clone(),
            Self::FunctionScan { call } => call.output_columns().to_vec(),
            Self::NestedLoopJoin { left, right, .. } => {
                let mut columns = left.columns();
                columns.extend(right.columns());
                columns
            }
            Self::Projection { targets, .. } => targets
                .iter()
                .map(|target| QueryColumn {
                    name: target.name.clone(),
                    sql_type: target.sql_type,
                })
                .collect(),
            Self::Subquery(plan) => plan.columns(),
        }
    }
}
