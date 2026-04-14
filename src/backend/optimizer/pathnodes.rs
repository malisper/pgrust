use crate::RelFileLocator;
use crate::backend::executor::{
    Expr, Plan, PlanEstimate, ProjectSetTarget, QueryColumn, RelationDesc,
    TargetEntry, ToastRelationRef,
};
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::access::relscan::ScanDirection;
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::plannodes::{AggAccum, JoinType, SetReturningCall};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerPath {
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
    Filter {
        plan_info: PlanEstimate,
        input: Box<PlannerPath>,
        predicate: Expr,
    },
    NestedLoopJoin {
        plan_info: PlanEstimate,
        left: Box<PlannerPath>,
        right: Box<PlannerPath>,
        kind: JoinType,
        on: Expr,
    },
    Projection {
        plan_info: PlanEstimate,
        input: Box<PlannerPath>,
        targets: Vec<TargetEntry>,
    },
    OrderBy {
        plan_info: PlanEstimate,
        input: Box<PlannerPath>,
        items: Vec<crate::backend::executor::OrderByEntry>,
    },
    Limit {
        plan_info: PlanEstimate,
        input: Box<PlannerPath>,
        limit: Option<usize>,
        offset: usize,
    },
    Aggregate {
        plan_info: PlanEstimate,
        input: Box<PlannerPath>,
        group_by: Vec<Expr>,
        accumulators: Vec<AggAccum>,
        having: Option<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    Values {
        plan_info: PlanEstimate,
        rows: Vec<Vec<Expr>>,
        output_columns: Vec<QueryColumn>,
    },
    FunctionScan {
        plan_info: PlanEstimate,
        call: SetReturningCall,
    },
    ProjectSet {
        plan_info: PlanEstimate,
        input: Box<PlannerPath>,
        targets: Vec<ProjectSetTarget>,
    },
}

impl PlannerPath {
    pub fn from_plan(plan: Plan) -> Self {
        match plan {
            Plan::Result { plan_info } => Self::Result { plan_info },
            Plan::SeqScan {
                plan_info,
                rel,
                relation_oid,
                toast,
                desc,
            } => Self::SeqScan {
                plan_info,
                rel,
                relation_oid,
                toast,
                desc,
            },
            Plan::IndexScan {
                plan_info,
                rel,
                index_rel,
                am_oid,
                toast,
                desc,
                index_meta,
                keys,
                direction,
            } => Self::IndexScan {
                plan_info,
                rel,
                index_rel,
                am_oid,
                toast,
                desc,
                index_meta,
                keys,
                direction,
            },
            Plan::Filter {
                plan_info,
                input,
                predicate,
            } => Self::Filter {
                plan_info,
                input: Box::new(Self::from_plan(*input)),
                predicate,
            },
            Plan::NestedLoopJoin {
                plan_info,
                left,
                right,
                kind,
                on,
            } => Self::NestedLoopJoin {
                plan_info,
                left: Box::new(Self::from_plan(*left)),
                right: Box::new(Self::from_plan(*right)),
                kind,
                on,
            },
            Plan::Projection {
                plan_info,
                input,
                targets,
            } => Self::Projection {
                plan_info,
                input: Box::new(Self::from_plan(*input)),
                targets,
            },
            Plan::OrderBy {
                plan_info,
                input,
                items,
            } => Self::OrderBy {
                plan_info,
                input: Box::new(Self::from_plan(*input)),
                items,
            },
            Plan::Limit {
                plan_info,
                input,
                limit,
                offset,
            } => Self::Limit {
                plan_info,
                input: Box::new(Self::from_plan(*input)),
                limit,
                offset,
            },
            Plan::Aggregate {
                plan_info,
                input,
                group_by,
                accumulators,
                having,
                output_columns,
            } => Self::Aggregate {
                plan_info,
                input: Box::new(Self::from_plan(*input)),
                group_by,
                accumulators,
                having,
                output_columns,
            },
            Plan::Values {
                plan_info,
                rows,
                output_columns,
            } => Self::Values {
                plan_info,
                rows,
                output_columns,
            },
            Plan::FunctionScan { plan_info, call } => Self::FunctionScan { plan_info, call },
            Plan::ProjectSet {
                plan_info,
                input,
                targets,
            } => Self::ProjectSet {
                plan_info,
                input: Box::new(Self::from_plan(*input)),
                targets,
            },
        }
    }

    pub fn into_plan(self) -> Plan {
        match self {
            Self::Result { plan_info } => Plan::Result { plan_info },
            Self::SeqScan {
                plan_info,
                rel,
                relation_oid,
                toast,
                desc,
            } => Plan::SeqScan {
                plan_info,
                rel,
                relation_oid,
                toast,
                desc,
            },
            Self::IndexScan {
                plan_info,
                rel,
                index_rel,
                am_oid,
                toast,
                desc,
                index_meta,
                keys,
                direction,
            } => Plan::IndexScan {
                plan_info,
                rel,
                index_rel,
                am_oid,
                toast,
                desc,
                index_meta,
                keys,
                direction,
            },
            Self::Filter {
                plan_info,
                input,
                predicate,
            } => Plan::Filter {
                plan_info,
                input: Box::new(input.into_plan()),
                predicate,
            },
            Self::NestedLoopJoin {
                plan_info,
                left,
                right,
                kind,
                on,
            } => Plan::NestedLoopJoin {
                plan_info,
                left: Box::new(left.into_plan()),
                right: Box::new(right.into_plan()),
                kind,
                on,
            },
            Self::Projection {
                plan_info,
                input,
                targets,
            } => Plan::Projection {
                plan_info,
                input: Box::new(input.into_plan()),
                targets,
            },
            Self::OrderBy {
                plan_info,
                input,
                items,
            } => Plan::OrderBy {
                plan_info,
                input: Box::new(input.into_plan()),
                items,
            },
            Self::Limit {
                plan_info,
                input,
                limit,
                offset,
            } => Plan::Limit {
                plan_info,
                input: Box::new(input.into_plan()),
                limit,
                offset,
            },
            Self::Aggregate {
                plan_info,
                input,
                group_by,
                accumulators,
                having,
                output_columns,
            } => Plan::Aggregate {
                plan_info,
                input: Box::new(input.into_plan()),
                group_by,
                accumulators,
                having,
                output_columns,
            },
            Self::Values {
                plan_info,
                rows,
                output_columns,
            } => Plan::Values {
                plan_info,
                rows,
                output_columns,
            },
            Self::FunctionScan { plan_info, call } => Plan::FunctionScan { plan_info, call },
            Self::ProjectSet {
                plan_info,
                input,
                targets,
            } => Plan::ProjectSet {
                plan_info,
                input: Box::new(input.into_plan()),
                targets,
            },
        }
    }

    pub fn plan_info(&self) -> PlanEstimate {
        match self {
            Self::Result { plan_info }
            | Self::SeqScan { plan_info, .. }
            | Self::IndexScan { plan_info, .. }
            | Self::Filter { plan_info, .. }
            | Self::NestedLoopJoin { plan_info, .. }
            | Self::Projection { plan_info, .. }
            | Self::OrderBy { plan_info, .. }
            | Self::Limit { plan_info, .. }
            | Self::Aggregate { plan_info, .. }
            | Self::Values { plan_info, .. }
            | Self::FunctionScan { plan_info, .. }
            | Self::ProjectSet { plan_info, .. } => *plan_info,
        }
    }

    pub fn columns(&self) -> Vec<QueryColumn> {
        match self {
            Self::Result { .. } => Vec::new(),
            Self::SeqScan { desc, .. } | Self::IndexScan { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                })
                .collect(),
            Self::Filter { input, .. } | Self::OrderBy { input, .. } | Self::Limit { input, .. } => {
                input.columns()
            }
            Self::Projection { targets, .. } => targets
                .iter()
                .map(|t| QueryColumn {
                    name: t.name.clone(),
                    sql_type: t.sql_type,
                })
                .collect(),
            Self::Aggregate { output_columns, .. } => output_columns.clone(),
            Self::NestedLoopJoin { left, right, .. } => {
                let mut cols = left.columns();
                cols.extend(right.columns());
                cols
            }
            Self::FunctionScan { call, .. } => call.output_columns().to_vec(),
            Self::Values { output_columns, .. } => output_columns.clone(),
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
