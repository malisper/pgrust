use crate::RelFileLocator;
use crate::backend::executor::{
    Expr, Plan, PlanEstimate, ProjectSetTarget, QueryColumn, RelationDesc, ToastRelationRef,
};
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::access::relscan::ScanDirection;
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::parsenodes::SubqueryComparisonOp;
use crate::include::nodes::plannodes::{AggAccum, ExprArraySubscript, JoinType, SetReturningCall};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerJoinExpr {
    InputColumn(usize),
    LeftColumn(usize),
    RightColumn(usize),
    OuterColumn {
        depth: usize,
        index: usize,
    },
    Const(crate::include::nodes::datum::Value),
    Add(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Sub(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    BitAnd(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    BitOr(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    BitXor(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Shl(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Shr(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Mul(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Div(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Mod(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Concat(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    UnaryPlus(Box<PlannerJoinExpr>),
    Negate(Box<PlannerJoinExpr>),
    BitNot(Box<PlannerJoinExpr>),
    Cast(Box<PlannerJoinExpr>, crate::backend::parser::SqlType),
    Eq(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    NotEq(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Lt(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    LtEq(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Gt(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    GtEq(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    RegexMatch(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Like {
        expr: Box<PlannerJoinExpr>,
        pattern: Box<PlannerJoinExpr>,
        escape: Option<Box<PlannerJoinExpr>>,
        case_insensitive: bool,
        negated: bool,
    },
    Similar {
        expr: Box<PlannerJoinExpr>,
        pattern: Box<PlannerJoinExpr>,
        escape: Option<Box<PlannerJoinExpr>>,
        negated: bool,
    },
    And(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Or(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    Not(Box<PlannerJoinExpr>),
    IsNull(Box<PlannerJoinExpr>),
    IsNotNull(Box<PlannerJoinExpr>),
    IsDistinctFrom(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    IsNotDistinctFrom(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    ArrayLiteral {
        elements: Vec<PlannerJoinExpr>,
        array_type: crate::backend::parser::SqlType,
    },
    ArrayOverlap(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbContains(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbContained(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbExists(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbExistsAny(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbExistsAll(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbPathExists(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbPathMatch(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    ScalarSubquery(Box<Plan>),
    ExistsSubquery(Box<Plan>),
    Coalesce(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    AnySubquery {
        left: Box<PlannerJoinExpr>,
        op: SubqueryComparisonOp,
        subquery: Box<Plan>,
    },
    AllSubquery {
        left: Box<PlannerJoinExpr>,
        op: SubqueryComparisonOp,
        subquery: Box<Plan>,
    },
    AnyArray {
        left: Box<PlannerJoinExpr>,
        op: SubqueryComparisonOp,
        right: Box<PlannerJoinExpr>,
    },
    AllArray {
        left: Box<PlannerJoinExpr>,
        op: SubqueryComparisonOp,
        right: Box<PlannerJoinExpr>,
    },
    ArraySubscript {
        array: Box<PlannerJoinExpr>,
        subscripts: Vec<PlannerJoinArraySubscript>,
    },
    Random,
    JsonGet(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonGetText(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonPath(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonPathText(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    FuncCall {
        func_oid: u32,
        func: crate::include::nodes::plannodes::BuiltinScalarFunction,
        args: Vec<PlannerJoinExpr>,
        func_variadic: bool,
    },
    CurrentDate,
    CurrentTime {
        precision: Option<i32>,
    },
    CurrentTimestamp {
        precision: Option<i32>,
    },
    LocalTime {
        precision: Option<i32>,
    },
    LocalTimestamp {
        precision: Option<i32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerJoinArraySubscript {
    pub is_slice: bool,
    pub lower: Option<PlannerJoinExpr>,
    pub upper: Option<PlannerJoinExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerTargetEntry {
    pub name: String,
    pub expr: PlannerJoinExpr,
    pub sql_type: crate::backend::parser::SqlType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerOrderByEntry {
    pub expr: PlannerJoinExpr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

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
        predicate: PlannerJoinExpr,
    },
    NestedLoopJoin {
        plan_info: PlanEstimate,
        left: Box<PlannerPath>,
        right: Box<PlannerPath>,
        kind: JoinType,
        on: PlannerJoinExpr,
    },
    Projection {
        plan_info: PlanEstimate,
        input: Box<PlannerPath>,
        targets: Vec<PlannerTargetEntry>,
    },
    OrderBy {
        plan_info: PlanEstimate,
        input: Box<PlannerPath>,
        items: Vec<PlannerOrderByEntry>,
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
                predicate: PlannerJoinExpr::from_input_expr(&predicate),
            },
            Plan::NestedLoopJoin {
                plan_info,
                left,
                right,
                kind,
                on,
            } => {
                let left = Self::from_plan(*left);
                let right = Self::from_plan(*right);
                let left_width = left.columns().len();
                Self::NestedLoopJoin {
                    plan_info,
                    left: Box::new(left),
                    right: Box::new(right),
                    kind,
                    on: PlannerJoinExpr::from_expr(&on, left_width),
                }
            }
            Plan::Projection {
                plan_info,
                input,
                targets,
            } => Self::Projection {
                plan_info,
                input: Box::new(Self::from_plan(*input)),
                targets: targets
                    .into_iter()
                    .map(PlannerTargetEntry::from_target_entry)
                    .collect(),
            },
            Plan::OrderBy {
                plan_info,
                input,
                items,
            } => Self::OrderBy {
                plan_info,
                input: Box::new(Self::from_plan(*input)),
                items: items
                    .into_iter()
                    .map(PlannerOrderByEntry::from_order_by_entry)
                    .collect(),
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
                predicate: predicate.into_input_expr(),
            },
            Self::NestedLoopJoin {
                plan_info,
                left,
                right,
                kind,
                on,
            } => {
                let left_width = left.columns().len();
                Plan::NestedLoopJoin {
                    plan_info,
                    left: Box::new(left.into_plan()),
                    right: Box::new(right.into_plan()),
                    kind,
                    on: on.into_expr(left_width),
                }
            }
            Self::Projection {
                plan_info,
                input,
                targets,
            } => Plan::Projection {
                plan_info,
                input: Box::new(input.into_plan()),
                targets: targets
                    .into_iter()
                    .map(PlannerTargetEntry::into_target_entry)
                    .collect(),
            },
            Self::OrderBy {
                plan_info,
                input,
                items,
            } => Plan::OrderBy {
                plan_info,
                input: Box::new(input.into_plan()),
                items: items
                    .into_iter()
                    .map(PlannerOrderByEntry::into_order_by_entry)
                    .collect(),
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

impl PlannerTargetEntry {
    pub(crate) fn from_target_entry(target: crate::backend::executor::TargetEntry) -> Self {
        Self {
            name: target.name,
            expr: PlannerJoinExpr::from_input_expr(&target.expr),
            sql_type: target.sql_type,
        }
    }

    pub(crate) fn into_target_entry(self) -> crate::backend::executor::TargetEntry {
        crate::backend::executor::TargetEntry {
            name: self.name,
            expr: self.expr.into_input_expr(),
            sql_type: self.sql_type,
        }
    }
}

impl PlannerOrderByEntry {
    pub(crate) fn from_order_by_entry(item: crate::backend::executor::OrderByEntry) -> Self {
        Self {
            expr: PlannerJoinExpr::from_input_expr(&item.expr),
            descending: item.descending,
            nulls_first: item.nulls_first,
        }
    }

    pub(crate) fn into_order_by_entry(self) -> crate::backend::executor::OrderByEntry {
        crate::backend::executor::OrderByEntry {
            expr: self.expr.into_input_expr(),
            descending: self.descending,
            nulls_first: self.nulls_first,
        }
    }
}

impl PlannerJoinExpr {
    pub fn from_input_expr(expr: &Expr) -> Self {
        match expr {
            Expr::Column(index) => Self::InputColumn(*index),
            Expr::OuterColumn { depth, index } => Self::OuterColumn {
                depth: *depth,
                index: *index,
            },
            Expr::Const(value) => Self::Const(value.clone()),
            Expr::Add(left, right) => Self::Add(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Sub(left, right) => Self::Sub(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::BitAnd(left, right) => Self::BitAnd(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::BitOr(left, right) => Self::BitOr(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::BitXor(left, right) => Self::BitXor(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Shl(left, right) => Self::Shl(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Shr(left, right) => Self::Shr(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Mul(left, right) => Self::Mul(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Div(left, right) => Self::Div(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Mod(left, right) => Self::Mod(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Concat(left, right) => Self::Concat(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::UnaryPlus(inner) => Self::UnaryPlus(Box::new(Self::from_input_expr(inner))),
            Expr::Negate(inner) => Self::Negate(Box::new(Self::from_input_expr(inner))),
            Expr::BitNot(inner) => Self::BitNot(Box::new(Self::from_input_expr(inner))),
            Expr::Cast(inner, sql_type) => {
                Self::Cast(Box::new(Self::from_input_expr(inner)), *sql_type)
            }
            Expr::Eq(left, right) => Self::Eq(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::NotEq(left, right) => Self::NotEq(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Lt(left, right) => Self::Lt(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::LtEq(left, right) => Self::LtEq(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Gt(left, right) => Self::Gt(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::GtEq(left, right) => Self::GtEq(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::RegexMatch(left, right) => Self::RegexMatch(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Self::Like {
                expr: Box::new(Self::from_input_expr(expr)),
                pattern: Box::new(Self::from_input_expr(pattern)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_input_expr(inner))),
                case_insensitive: *case_insensitive,
                negated: *negated,
            },
            Expr::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Self::Similar {
                expr: Box::new(Self::from_input_expr(expr)),
                pattern: Box::new(Self::from_input_expr(pattern)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_input_expr(inner))),
                negated: *negated,
            },
            Expr::And(left, right) => Self::And(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Or(left, right) => Self::Or(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::Not(inner) => Self::Not(Box::new(Self::from_input_expr(inner))),
            Expr::IsNull(inner) => Self::IsNull(Box::new(Self::from_input_expr(inner))),
            Expr::IsNotNull(inner) => Self::IsNotNull(Box::new(Self::from_input_expr(inner))),
            Expr::IsDistinctFrom(left, right) => Self::IsDistinctFrom(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::IsNotDistinctFrom(left, right) => Self::IsNotDistinctFrom(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::ArrayLiteral {
                elements,
                array_type,
            } => Self::ArrayLiteral {
                elements: elements.iter().map(Self::from_input_expr).collect(),
                array_type: *array_type,
            },
            Expr::ArrayOverlap(left, right) => Self::ArrayOverlap(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbContains(left, right) => Self::JsonbContains(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbContained(left, right) => Self::JsonbContained(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbExists(left, right) => Self::JsonbExists(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbExistsAny(left, right) => Self::JsonbExistsAny(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbExistsAll(left, right) => Self::JsonbExistsAll(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbPathExists(left, right) => Self::JsonbPathExists(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonbPathMatch(left, right) => Self::JsonbPathMatch(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::ScalarSubquery(plan) => Self::ScalarSubquery(plan.clone()),
            Expr::ExistsSubquery(plan) => Self::ExistsSubquery(plan.clone()),
            Expr::Coalesce(left, right) => Self::Coalesce(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::AnySubquery { left, op, subquery } => Self::AnySubquery {
                left: Box::new(Self::from_input_expr(left)),
                op: *op,
                subquery: subquery.clone(),
            },
            Expr::AllSubquery { left, op, subquery } => Self::AllSubquery {
                left: Box::new(Self::from_input_expr(left)),
                op: *op,
                subquery: subquery.clone(),
            },
            Expr::AnyArray { left, op, right } => Self::AnyArray {
                left: Box::new(Self::from_input_expr(left)),
                op: *op,
                right: Box::new(Self::from_input_expr(right)),
            },
            Expr::AllArray { left, op, right } => Self::AllArray {
                left: Box::new(Self::from_input_expr(left)),
                op: *op,
                right: Box::new(Self::from_input_expr(right)),
            },
            Expr::ArraySubscript { array, subscripts } => Self::ArraySubscript {
                array: Box::new(Self::from_input_expr(array)),
                subscripts: subscripts
                    .iter()
                    .map(|subscript| PlannerJoinArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript
                            .lower
                            .as_ref()
                            .map(Self::from_input_expr),
                        upper: subscript
                            .upper
                            .as_ref()
                            .map(Self::from_input_expr),
                    })
                    .collect(),
            },
            Expr::Random => Self::Random,
            Expr::JsonGet(left, right) => Self::JsonGet(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonGetText(left, right) => Self::JsonGetText(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonPath(left, right) => Self::JsonPath(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::JsonPathText(left, right) => Self::JsonPathText(
                Box::new(Self::from_input_expr(left)),
                Box::new(Self::from_input_expr(right)),
            ),
            Expr::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Self::FuncCall {
                func_oid: *func_oid,
                func: *func,
                args: args.iter().map(Self::from_input_expr).collect(),
                func_variadic: *func_variadic,
            },
            Expr::CurrentDate => Self::CurrentDate,
            Expr::CurrentTime { precision } => Self::CurrentTime {
                precision: *precision,
            },
            Expr::CurrentTimestamp { precision } => Self::CurrentTimestamp {
                precision: *precision,
            },
            Expr::LocalTime { precision } => Self::LocalTime {
                precision: *precision,
            },
            Expr::LocalTimestamp { precision } => Self::LocalTimestamp {
                precision: *precision,
            },
        }
    }

    pub fn into_input_expr(self) -> Expr {
        match self {
            Self::InputColumn(index) => Expr::Column(index),
            Self::LeftColumn(index) => Expr::Column(index),
            Self::RightColumn(index) => Expr::Column(index),
            Self::OuterColumn { depth, index } => Expr::OuterColumn { depth, index },
            Self::Const(value) => Expr::Const(value),
            Self::Add(left, right) => Expr::Add(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Sub(left, right) => Expr::Sub(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::BitAnd(left, right) => Expr::BitAnd(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::BitOr(left, right) => Expr::BitOr(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::BitXor(left, right) => Expr::BitXor(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Shl(left, right) => Expr::Shl(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Shr(left, right) => Expr::Shr(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Mul(left, right) => Expr::Mul(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Div(left, right) => Expr::Div(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Mod(left, right) => Expr::Mod(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Concat(left, right) => Expr::Concat(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::UnaryPlus(inner) => Expr::UnaryPlus(Box::new(inner.into_input_expr())),
            Self::Negate(inner) => Expr::Negate(Box::new(inner.into_input_expr())),
            Self::BitNot(inner) => Expr::BitNot(Box::new(inner.into_input_expr())),
            Self::Cast(inner, sql_type) => Expr::Cast(Box::new(inner.into_input_expr()), sql_type),
            Self::Eq(left, right) => Expr::Eq(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::NotEq(left, right) => Expr::NotEq(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Lt(left, right) => Expr::Lt(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::LtEq(left, right) => Expr::LtEq(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Gt(left, right) => Expr::Gt(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::GtEq(left, right) => Expr::GtEq(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::RegexMatch(left, right) => Expr::RegexMatch(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Expr::Like {
                expr: Box::new(expr.into_input_expr()),
                pattern: Box::new(pattern.into_input_expr()),
                escape: escape.map(|inner| Box::new(inner.into_input_expr())),
                case_insensitive,
                negated,
            },
            Self::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Expr::Similar {
                expr: Box::new(expr.into_input_expr()),
                pattern: Box::new(pattern.into_input_expr()),
                escape: escape.map(|inner| Box::new(inner.into_input_expr())),
                negated,
            },
            Self::And(left, right) => Expr::And(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Or(left, right) => Expr::Or(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::Not(inner) => Expr::Not(Box::new(inner.into_input_expr())),
            Self::IsNull(inner) => Expr::IsNull(Box::new(inner.into_input_expr())),
            Self::IsNotNull(inner) => Expr::IsNotNull(Box::new(inner.into_input_expr())),
            Self::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::ArrayLiteral {
                elements,
                array_type,
            } => Expr::ArrayLiteral {
                elements: elements
                    .into_iter()
                    .map(|element| element.into_input_expr())
                    .collect(),
                array_type,
            },
            Self::ArrayOverlap(left, right) => Expr::ArrayOverlap(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::JsonbContains(left, right) => Expr::JsonbContains(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::JsonbContained(left, right) => Expr::JsonbContained(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::JsonbExists(left, right) => Expr::JsonbExists(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::JsonbExistsAny(left, right) => Expr::JsonbExistsAny(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::JsonbExistsAll(left, right) => Expr::JsonbExistsAll(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::JsonbPathExists(left, right) => Expr::JsonbPathExists(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::JsonbPathMatch(left, right) => Expr::JsonbPathMatch(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::ScalarSubquery(plan) => Expr::ScalarSubquery(plan),
            Self::ExistsSubquery(plan) => Expr::ExistsSubquery(plan),
            Self::Coalesce(left, right) => Expr::Coalesce(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::AnySubquery { left, op, subquery } => Expr::AnySubquery {
                left: Box::new(left.into_input_expr()),
                op,
                subquery,
            },
            Self::AllSubquery { left, op, subquery } => Expr::AllSubquery {
                left: Box::new(left.into_input_expr()),
                op,
                subquery,
            },
            Self::AnyArray { left, op, right } => Expr::AnyArray {
                left: Box::new(left.into_input_expr()),
                op,
                right: Box::new(right.into_input_expr()),
            },
            Self::AllArray { left, op, right } => Expr::AllArray {
                left: Box::new(left.into_input_expr()),
                op,
                right: Box::new(right.into_input_expr()),
            },
            Self::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
                array: Box::new(array.into_input_expr()),
                subscripts: subscripts
                    .into_iter()
                    .map(|subscript| ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript.lower.map(|expr| expr.into_input_expr()),
                        upper: subscript.upper.map(|expr| expr.into_input_expr()),
                    })
                    .collect(),
            },
            Self::Random => Expr::Random,
            Self::JsonGet(left, right) => Expr::JsonGet(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::JsonGetText(left, right) => Expr::JsonGetText(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::JsonPath(left, right) => Expr::JsonPath(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::JsonPathText(left, right) => Expr::JsonPathText(
                Box::new(left.into_input_expr()),
                Box::new(right.into_input_expr()),
            ),
            Self::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Expr::FuncCall {
                func_oid,
                func,
                args: args.into_iter().map(|arg| arg.into_input_expr()).collect(),
                func_variadic,
            },
            Self::CurrentDate => Expr::CurrentDate,
            Self::CurrentTime { precision } => Expr::CurrentTime { precision },
            Self::CurrentTimestamp { precision } => Expr::CurrentTimestamp { precision },
            Self::LocalTime { precision } => Expr::LocalTime { precision },
            Self::LocalTimestamp { precision } => Expr::LocalTimestamp { precision },
        }
    }

    pub fn from_expr(expr: &Expr, left_width: usize) -> Self {
        match expr {
            Expr::Column(index) if *index < left_width => Self::LeftColumn(*index),
            Expr::Column(index) => Self::RightColumn(index - left_width),
            Expr::OuterColumn { depth, index } => Self::OuterColumn {
                depth: *depth,
                index: *index,
            },
            Expr::Const(value) => Self::Const(value.clone()),
            Expr::Add(left, right) => Self::Add(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Sub(left, right) => Self::Sub(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::BitAnd(left, right) => Self::BitAnd(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::BitOr(left, right) => Self::BitOr(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::BitXor(left, right) => Self::BitXor(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Shl(left, right) => Self::Shl(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Shr(left, right) => Self::Shr(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Mul(left, right) => Self::Mul(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Div(left, right) => Self::Div(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Mod(left, right) => Self::Mod(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Concat(left, right) => Self::Concat(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::UnaryPlus(inner) => Self::UnaryPlus(Box::new(Self::from_expr(inner, left_width))),
            Expr::Negate(inner) => Self::Negate(Box::new(Self::from_expr(inner, left_width))),
            Expr::BitNot(inner) => Self::BitNot(Box::new(Self::from_expr(inner, left_width))),
            Expr::Cast(inner, sql_type) => {
                Self::Cast(Box::new(Self::from_expr(inner, left_width)), *sql_type)
            }
            Expr::Eq(left, right) => Self::Eq(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::NotEq(left, right) => Self::NotEq(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Lt(left, right) => Self::Lt(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::LtEq(left, right) => Self::LtEq(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Gt(left, right) => Self::Gt(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::GtEq(left, right) => Self::GtEq(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::RegexMatch(left, right) => Self::RegexMatch(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Self::Like {
                expr: Box::new(Self::from_expr(expr, left_width)),
                pattern: Box::new(Self::from_expr(pattern, left_width)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_expr(inner, left_width))),
                case_insensitive: *case_insensitive,
                negated: *negated,
            },
            Expr::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Self::Similar {
                expr: Box::new(Self::from_expr(expr, left_width)),
                pattern: Box::new(Self::from_expr(pattern, left_width)),
                escape: escape
                    .as_ref()
                    .map(|inner| Box::new(Self::from_expr(inner, left_width))),
                negated: *negated,
            },
            Expr::And(left, right) => Self::And(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Or(left, right) => Self::Or(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::Not(inner) => Self::Not(Box::new(Self::from_expr(inner, left_width))),
            Expr::IsNull(inner) => Self::IsNull(Box::new(Self::from_expr(inner, left_width))),
            Expr::IsNotNull(inner) => Self::IsNotNull(Box::new(Self::from_expr(inner, left_width))),
            Expr::IsDistinctFrom(left, right) => Self::IsDistinctFrom(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::IsNotDistinctFrom(left, right) => Self::IsNotDistinctFrom(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::ArrayLiteral {
                elements,
                array_type,
            } => Self::ArrayLiteral {
                elements: elements
                    .iter()
                    .map(|element| Self::from_expr(element, left_width))
                    .collect(),
                array_type: *array_type,
            },
            Expr::ArrayOverlap(left, right) => Self::ArrayOverlap(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbContains(left, right) => Self::JsonbContains(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbContained(left, right) => Self::JsonbContained(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbExists(left, right) => Self::JsonbExists(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbExistsAny(left, right) => Self::JsonbExistsAny(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbExistsAll(left, right) => Self::JsonbExistsAll(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbPathExists(left, right) => Self::JsonbPathExists(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonbPathMatch(left, right) => Self::JsonbPathMatch(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::ScalarSubquery(plan) => Self::ScalarSubquery(plan.clone()),
            Expr::ExistsSubquery(plan) => Self::ExistsSubquery(plan.clone()),
            Expr::Coalesce(left, right) => Self::Coalesce(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::AnySubquery { left, op, subquery } => Self::AnySubquery {
                left: Box::new(Self::from_expr(left, left_width)),
                op: *op,
                subquery: subquery.clone(),
            },
            Expr::AllSubquery { left, op, subquery } => Self::AllSubquery {
                left: Box::new(Self::from_expr(left, left_width)),
                op: *op,
                subquery: subquery.clone(),
            },
            Expr::AnyArray { left, op, right } => Self::AnyArray {
                left: Box::new(Self::from_expr(left, left_width)),
                op: *op,
                right: Box::new(Self::from_expr(right, left_width)),
            },
            Expr::AllArray { left, op, right } => Self::AllArray {
                left: Box::new(Self::from_expr(left, left_width)),
                op: *op,
                right: Box::new(Self::from_expr(right, left_width)),
            },
            Expr::ArraySubscript { array, subscripts } => Self::ArraySubscript {
                array: Box::new(Self::from_expr(array, left_width)),
                subscripts: subscripts
                    .iter()
                    .map(|subscript| PlannerJoinArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript
                            .lower
                            .as_ref()
                            .map(|expr| Self::from_expr(expr, left_width)),
                        upper: subscript
                            .upper
                            .as_ref()
                            .map(|expr| Self::from_expr(expr, left_width)),
                    })
                    .collect(),
            },
            Expr::Random => Self::Random,
            Expr::JsonGet(left, right) => Self::JsonGet(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonGetText(left, right) => Self::JsonGetText(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonPath(left, right) => Self::JsonPath(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::JsonPathText(left, right) => Self::JsonPathText(
                Box::new(Self::from_expr(left, left_width)),
                Box::new(Self::from_expr(right, left_width)),
            ),
            Expr::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Self::FuncCall {
                func_oid: *func_oid,
                func: *func,
                args: args.iter().map(|arg| Self::from_expr(arg, left_width)).collect(),
                func_variadic: *func_variadic,
            },
            Expr::CurrentDate => Self::CurrentDate,
            Expr::CurrentTime { precision } => Self::CurrentTime {
                precision: *precision,
            },
            Expr::CurrentTimestamp { precision } => Self::CurrentTimestamp {
                precision: *precision,
            },
            Expr::LocalTime { precision } => Self::LocalTime {
                precision: *precision,
            },
            Expr::LocalTimestamp { precision } => Self::LocalTimestamp {
                precision: *precision,
            },
        }
    }

    pub fn into_expr(self, left_width: usize) -> Expr {
        match self {
            Self::InputColumn(index) => Expr::Column(index),
            Self::LeftColumn(index) => Expr::Column(index),
            Self::RightColumn(index) => Expr::Column(left_width + index),
            Self::OuterColumn { depth, index } => Expr::OuterColumn { depth, index },
            Self::Const(value) => Expr::Const(value),
            Self::Add(left, right) => Expr::Add(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Sub(left, right) => Expr::Sub(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::BitAnd(left, right) => Expr::BitAnd(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::BitOr(left, right) => Expr::BitOr(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::BitXor(left, right) => Expr::BitXor(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Shl(left, right) => Expr::Shl(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Shr(left, right) => Expr::Shr(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Mul(left, right) => Expr::Mul(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Div(left, right) => Expr::Div(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Mod(left, right) => Expr::Mod(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Concat(left, right) => Expr::Concat(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::UnaryPlus(inner) => Expr::UnaryPlus(Box::new(inner.into_expr(left_width))),
            Self::Negate(inner) => Expr::Negate(Box::new(inner.into_expr(left_width))),
            Self::BitNot(inner) => Expr::BitNot(Box::new(inner.into_expr(left_width))),
            Self::Cast(inner, sql_type) => Expr::Cast(Box::new(inner.into_expr(left_width)), sql_type),
            Self::Eq(left, right) => Expr::Eq(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::NotEq(left, right) => Expr::NotEq(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Lt(left, right) => Expr::Lt(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::LtEq(left, right) => Expr::LtEq(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Gt(left, right) => Expr::Gt(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::GtEq(left, right) => Expr::GtEq(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::RegexMatch(left, right) => Expr::RegexMatch(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Expr::Like {
                expr: Box::new(expr.into_expr(left_width)),
                pattern: Box::new(pattern.into_expr(left_width)),
                escape: escape.map(|inner| Box::new(inner.into_expr(left_width))),
                case_insensitive,
                negated,
            },
            Self::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Expr::Similar {
                expr: Box::new(expr.into_expr(left_width)),
                pattern: Box::new(pattern.into_expr(left_width)),
                escape: escape.map(|inner| Box::new(inner.into_expr(left_width))),
                negated,
            },
            Self::And(left, right) => Expr::And(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Or(left, right) => Expr::Or(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::Not(inner) => Expr::Not(Box::new(inner.into_expr(left_width))),
            Self::IsNull(inner) => Expr::IsNull(Box::new(inner.into_expr(left_width))),
            Self::IsNotNull(inner) => Expr::IsNotNull(Box::new(inner.into_expr(left_width))),
            Self::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::ArrayLiteral {
                elements,
                array_type,
            } => Expr::ArrayLiteral {
                elements: elements
                    .into_iter()
                    .map(|element| element.into_expr(left_width))
                    .collect(),
                array_type,
            },
            Self::ArrayOverlap(left, right) => Expr::ArrayOverlap(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbContains(left, right) => Expr::JsonbContains(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbContained(left, right) => Expr::JsonbContained(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbExists(left, right) => Expr::JsonbExists(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbExistsAny(left, right) => Expr::JsonbExistsAny(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbExistsAll(left, right) => Expr::JsonbExistsAll(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbPathExists(left, right) => Expr::JsonbPathExists(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonbPathMatch(left, right) => Expr::JsonbPathMatch(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::ScalarSubquery(plan) => Expr::ScalarSubquery(plan),
            Self::ExistsSubquery(plan) => Expr::ExistsSubquery(plan),
            Self::Coalesce(left, right) => Expr::Coalesce(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::AnySubquery { left, op, subquery } => Expr::AnySubquery {
                left: Box::new(left.into_expr(left_width)),
                op,
                subquery,
            },
            Self::AllSubquery { left, op, subquery } => Expr::AllSubquery {
                left: Box::new(left.into_expr(left_width)),
                op,
                subquery,
            },
            Self::AnyArray { left, op, right } => Expr::AnyArray {
                left: Box::new(left.into_expr(left_width)),
                op,
                right: Box::new(right.into_expr(left_width)),
            },
            Self::AllArray { left, op, right } => Expr::AllArray {
                left: Box::new(left.into_expr(left_width)),
                op,
                right: Box::new(right.into_expr(left_width)),
            },
            Self::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
                array: Box::new(array.into_expr(left_width)),
                subscripts: subscripts
                    .into_iter()
                    .map(|subscript| ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript.lower.map(|expr| expr.into_expr(left_width)),
                        upper: subscript.upper.map(|expr| expr.into_expr(left_width)),
                    })
                    .collect(),
            },
            Self::Random => Expr::Random,
            Self::JsonGet(left, right) => Expr::JsonGet(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonGetText(left, right) => Expr::JsonGetText(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonPath(left, right) => Expr::JsonPath(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::JsonPathText(left, right) => Expr::JsonPathText(
                Box::new(left.into_expr(left_width)),
                Box::new(right.into_expr(left_width)),
            ),
            Self::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Expr::FuncCall {
                func_oid,
                func,
                args: args.into_iter().map(|arg| arg.into_expr(left_width)).collect(),
                func_variadic,
            },
            Self::CurrentDate => Expr::CurrentDate,
            Self::CurrentTime { precision } => Expr::CurrentTime { precision },
            Self::CurrentTimestamp { precision } => Expr::CurrentTimestamp { precision },
            Self::LocalTime { precision } => Expr::LocalTime { precision },
            Self::LocalTimestamp { precision } => Expr::LocalTimestamp { precision },
        }
    }

    pub fn swap_inputs(&self) -> Self {
        match self {
            Self::InputColumn(index) => Self::InputColumn(*index),
            Self::LeftColumn(index) => Self::RightColumn(*index),
            Self::RightColumn(index) => Self::LeftColumn(*index),
            Self::OuterColumn { depth, index } => Self::OuterColumn {
                depth: *depth,
                index: *index,
            },
            Self::Const(value) => Self::Const(value.clone()),
            Self::Add(left, right) => Self::Add(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Sub(left, right) => Self::Sub(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::BitAnd(left, right) => Self::BitAnd(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::BitOr(left, right) => Self::BitOr(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::BitXor(left, right) => Self::BitXor(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Shl(left, right) => Self::Shl(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Shr(left, right) => Self::Shr(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Mul(left, right) => Self::Mul(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Div(left, right) => Self::Div(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Mod(left, right) => Self::Mod(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Concat(left, right) => Self::Concat(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::UnaryPlus(inner) => Self::UnaryPlus(Box::new(inner.swap_inputs())),
            Self::Negate(inner) => Self::Negate(Box::new(inner.swap_inputs())),
            Self::BitNot(inner) => Self::BitNot(Box::new(inner.swap_inputs())),
            Self::Cast(inner, sql_type) => Self::Cast(Box::new(inner.swap_inputs()), *sql_type),
            Self::Eq(left, right) => Self::Eq(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::NotEq(left, right) => Self::NotEq(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Lt(left, right) => Self::Lt(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::LtEq(left, right) => Self::LtEq(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Gt(left, right) => Self::Gt(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::GtEq(left, right) => Self::GtEq(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::RegexMatch(left, right) => Self::RegexMatch(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Self::Like {
                expr: Box::new(expr.swap_inputs()),
                pattern: Box::new(pattern.swap_inputs()),
                escape: escape.as_ref().map(|inner| Box::new(inner.swap_inputs())),
                case_insensitive: *case_insensitive,
                negated: *negated,
            },
            Self::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Self::Similar {
                expr: Box::new(expr.swap_inputs()),
                pattern: Box::new(pattern.swap_inputs()),
                escape: escape.as_ref().map(|inner| Box::new(inner.swap_inputs())),
                negated: *negated,
            },
            Self::And(left, right) => Self::And(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Or(left, right) => Self::Or(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::Not(inner) => Self::Not(Box::new(inner.swap_inputs())),
            Self::IsNull(inner) => Self::IsNull(Box::new(inner.swap_inputs())),
            Self::IsNotNull(inner) => Self::IsNotNull(Box::new(inner.swap_inputs())),
            Self::IsDistinctFrom(left, right) => Self::IsDistinctFrom(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::IsNotDistinctFrom(left, right) => Self::IsNotDistinctFrom(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::ArrayLiteral {
                elements,
                array_type,
            } => Self::ArrayLiteral {
                elements: elements.iter().map(Self::swap_inputs).collect(),
                array_type: *array_type,
            },
            Self::ArrayOverlap(left, right) => Self::ArrayOverlap(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::JsonbContains(left, right) => Self::JsonbContains(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::JsonbContained(left, right) => Self::JsonbContained(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::JsonbExists(left, right) => Self::JsonbExists(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::JsonbExistsAny(left, right) => Self::JsonbExistsAny(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::JsonbExistsAll(left, right) => Self::JsonbExistsAll(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::JsonbPathExists(left, right) => Self::JsonbPathExists(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::JsonbPathMatch(left, right) => Self::JsonbPathMatch(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::ScalarSubquery(plan) => Self::ScalarSubquery(plan.clone()),
            Self::ExistsSubquery(plan) => Self::ExistsSubquery(plan.clone()),
            Self::Coalesce(left, right) => Self::Coalesce(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::AnySubquery { left, op, subquery } => Self::AnySubquery {
                left: Box::new(left.swap_inputs()),
                op: *op,
                subquery: subquery.clone(),
            },
            Self::AllSubquery { left, op, subquery } => Self::AllSubquery {
                left: Box::new(left.swap_inputs()),
                op: *op,
                subquery: subquery.clone(),
            },
            Self::AnyArray { left, op, right } => Self::AnyArray {
                left: Box::new(left.swap_inputs()),
                op: *op,
                right: Box::new(right.swap_inputs()),
            },
            Self::AllArray { left, op, right } => Self::AllArray {
                left: Box::new(left.swap_inputs()),
                op: *op,
                right: Box::new(right.swap_inputs()),
            },
            Self::ArraySubscript { array, subscripts } => Self::ArraySubscript {
                array: Box::new(array.swap_inputs()),
                subscripts: subscripts
                    .iter()
                    .map(|subscript| PlannerJoinArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript.lower.as_ref().map(Self::swap_inputs),
                        upper: subscript.upper.as_ref().map(Self::swap_inputs),
                    })
                    .collect(),
            },
            Self::Random => Self::Random,
            Self::JsonGet(left, right) => Self::JsonGet(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::JsonGetText(left, right) => Self::JsonGetText(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::JsonPath(left, right) => Self::JsonPath(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::JsonPathText(left, right) => Self::JsonPathText(
                Box::new(left.swap_inputs()),
                Box::new(right.swap_inputs()),
            ),
            Self::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Self::FuncCall {
                func_oid: *func_oid,
                func: *func,
                args: args.iter().map(Self::swap_inputs).collect(),
                func_variadic: *func_variadic,
            },
            Self::CurrentDate => Self::CurrentDate,
            Self::CurrentTime { precision } => Self::CurrentTime {
                precision: *precision,
            },
            Self::CurrentTimestamp { precision } => Self::CurrentTimestamp {
                precision: *precision,
            },
            Self::LocalTime { precision } => Self::LocalTime {
                precision: *precision,
            },
            Self::LocalTimestamp { precision } => Self::LocalTimestamp {
                precision: *precision,
            },
        }
    }
}
