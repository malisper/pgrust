use super::from_ir::BoundFromPlan;
use crate::backend::executor::{
    AggAccum, Expr, OrderByEntry, Plan, PlanEstimate, ProjectSetTarget, QueryColumn, TargetEntry,
};

#[derive(Debug, Clone)]
pub(super) enum BoundSelectPlan {
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
    pub(super) fn into_plan(self) -> Plan {
        match self {
            Self::From(plan) => plan.into_plan(),
            Self::Filter { input, predicate } => Plan::Filter {
                plan_info: PlanEstimate::default(),
                input: Box::new(input.into_plan()),
                predicate,
            },
            Self::OrderBy { input, items } => Plan::OrderBy {
                plan_info: PlanEstimate::default(),
                input: Box::new(input.into_plan()),
                items,
            },
            Self::Limit {
                input,
                limit,
                offset,
            } => Plan::Limit {
                plan_info: PlanEstimate::default(),
                input: Box::new(input.into_plan()),
                limit,
                offset,
            },
            Self::Aggregate {
                input,
                group_by,
                accumulators,
                having,
                output_columns,
            } => Plan::Aggregate {
                plan_info: PlanEstimate::default(),
                input: Box::new(input.into_plan()),
                group_by,
                accumulators,
                having,
                output_columns,
            },
            Self::Projection { input, targets } => Plan::Projection {
                plan_info: PlanEstimate::default(),
                input: Box::new(input.into_plan()),
                targets,
            },
            Self::ProjectSet { input, targets } => Plan::ProjectSet {
                plan_info: PlanEstimate::default(),
                input: Box::new(input.into_plan()),
                targets,
            },
        }
    }

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
