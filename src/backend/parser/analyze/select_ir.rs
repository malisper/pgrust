use super::from_ir::BoundFromPlan;
use crate::backend::executor::{
    AggAccum, Expr, OrderByEntry, ProjectSetTarget, QueryColumn, TargetEntry,
};

#[derive(Debug, Clone)]
pub(crate) enum BoundSelectPlan {
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

impl BoundSelectPlan {
    pub(super) fn columns(&self) -> Vec<QueryColumn> {
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
