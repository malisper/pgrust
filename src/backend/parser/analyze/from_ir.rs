use super::select_ir::BoundSelectPlan;
use crate::backend::executor::{
    Expr, Plan, PlanEstimate, QueryColumn, RelationDesc, SetReturningCall, TargetEntry,
    ToastRelationRef,
};
use crate::include::nodes::plannodes::JoinType;
use crate::RelFileLocator;

#[derive(Debug, Clone)]
pub(super) enum BoundFromPlan {
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
    Subquery(Box<BoundSelectPlan>),
}

impl BoundFromPlan {
    pub(super) fn into_plan(self) -> Plan {
        match self {
            Self::Result => Plan::Result {
                plan_info: PlanEstimate::default(),
            },
            Self::SeqScan {
                rel,
                relation_oid,
                toast,
                desc,
            } => Plan::SeqScan {
                plan_info: PlanEstimate::default(),
                rel,
                relation_oid,
                toast,
                desc,
            },
            Self::Values {
                rows,
                output_columns,
            } => Plan::Values {
                plan_info: PlanEstimate::default(),
                rows,
                output_columns,
            },
            Self::FunctionScan { call } => Plan::FunctionScan {
                plan_info: PlanEstimate::default(),
                call,
            },
            Self::NestedLoopJoin {
                left,
                right,
                kind,
                on,
            } => Plan::NestedLoopJoin {
                plan_info: PlanEstimate::default(),
                left: Box::new(left.into_plan()),
                right: Box::new(right.into_plan()),
                kind,
                on,
            },
            Self::Projection { input, targets } => Plan::Projection {
                plan_info: PlanEstimate::default(),
                input: Box::new(input.into_plan()),
                targets,
            },
            Self::Subquery(plan) => plan.into_plan(),
        }
    }

    pub(super) fn columns(&self) -> Vec<QueryColumn> {
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
