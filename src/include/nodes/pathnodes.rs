use crate::RelFileLocator;
use crate::backend::parser::{SqlType, SubqueryComparisonOp};
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::access::relscan::ScanDirection;
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::plannodes::{DeferredSelectPlan, PlanEstimate};
use crate::include::nodes::primnodes::{
    AggAccum, BuiltinScalarFunction, JoinType, QueryColumn, RelationDesc, SetReturningCall,
    ToastRelationRef,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerJoinExpr {
    InputColumn(usize),
    SyntheticColumn {
        slot_id: usize,
        index: usize,
    },
    BaseColumn {
        source_id: usize,
        relation_oid: u32,
        index: usize,
    },
    LeftColumn(usize),
    RightColumn(usize),
    OuterColumn {
        depth: usize,
        index: usize,
    },
    Const(Value),
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
    Cast(Box<PlannerJoinExpr>, SqlType),
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
        array_type: SqlType,
    },
    ArrayOverlap(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbContains(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbContained(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbExists(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbExistsAny(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbExistsAll(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbPathExists(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    JsonbPathMatch(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    ScalarSubquery(Box<DeferredSelectPlan>),
    ExistsSubquery(Box<DeferredSelectPlan>),
    Coalesce(Box<PlannerJoinExpr>, Box<PlannerJoinExpr>),
    AnySubquery {
        left: Box<PlannerJoinExpr>,
        op: SubqueryComparisonOp,
        subquery: Box<DeferredSelectPlan>,
    },
    AllSubquery {
        left: Box<PlannerJoinExpr>,
        op: SubqueryComparisonOp,
        subquery: Box<DeferredSelectPlan>,
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
        func: BuiltinScalarFunction,
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
    pub sql_type: SqlType,
    pub resno: usize,
    pub ressortgroupref: usize,
    pub resjunk: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerOrderByEntry {
    pub expr: PlannerJoinExpr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerProjectSetTarget {
    Scalar(PlannerTargetEntry),
    Set {
        name: String,
        call: SetReturningCall,
        sql_type: SqlType,
        column_index: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerPath {
    Result {
        plan_info: PlanEstimate,
    },
    SeqScan {
        plan_info: PlanEstimate,
        source_id: usize,
        rel: RelFileLocator,
        relation_oid: u32,
        toast: Option<ToastRelationRef>,
        desc: RelationDesc,
    },
    IndexScan {
        plan_info: PlanEstimate,
        source_id: usize,
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
        slot_id: usize,
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
        slot_id: usize,
        input: Box<PlannerPath>,
        group_by: Vec<PlannerJoinExpr>,
        accumulators: Vec<AggAccum>,
        having: Option<PlannerJoinExpr>,
        output_columns: Vec<QueryColumn>,
    },
    Values {
        plan_info: PlanEstimate,
        slot_id: usize,
        rows: Vec<Vec<PlannerJoinExpr>>,
        output_columns: Vec<QueryColumn>,
    },
    FunctionScan {
        plan_info: PlanEstimate,
        slot_id: usize,
        call: SetReturningCall,
    },
    ProjectSet {
        plan_info: PlanEstimate,
        slot_id: usize,
        input: Box<PlannerPath>,
        targets: Vec<PlannerProjectSetTarget>,
    },
}
