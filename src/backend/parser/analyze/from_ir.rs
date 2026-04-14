use super::select_ir::BoundSelectPlan;
use crate::RelFileLocator;
use crate::backend::executor::{
    Expr, QueryColumn, RelationDesc, SetReturningCall, TargetEntry, ToastRelationRef,
};
use crate::include::nodes::plannodes::JoinType;

#[derive(Debug, Clone)]
pub(crate) enum BoundFromPlan {
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
