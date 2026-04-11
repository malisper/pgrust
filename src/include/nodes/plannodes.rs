use crate::backend::parser::{SqlType, SqlTypeKind, SubqueryComparisonOp};
use crate::include::access::htup::AttributeDesc;
use crate::include::nodes::datum::Value;
use crate::RelFileLocator;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarType {
    Int16,
    Int32,
    Int64,
    BitString,
    Bytea,
    Float32,
    Float64,
    Numeric,
    Json,
    Jsonb,
    JsonPath,
    Text,
    Bool,
    Array(Box<ScalarType>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDesc {
    pub name: String,
    pub storage: AttributeDesc,
    pub ty: ScalarType,
    pub sql_type: SqlType,
    pub default_expr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationDesc {
    pub columns: Vec<ColumnDesc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryColumn {
    pub name: String,
    pub sql_type: SqlType,
}

impl QueryColumn {
    pub fn text(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql_type: SqlType::new(SqlTypeKind::Text),
        }
    }
}

impl RelationDesc {
    pub fn attribute_descs(&self) -> Vec<AttributeDesc> {
        self.columns.iter().map(|c| c.storage.clone()).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetEntry {
    pub name: String,
    pub expr: Expr,
    pub sql_type: SqlType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByEntry {
    pub expr: Expr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Variance,
    Stddev,
    Min,
    Max,
    JsonAgg,
    JsonbAgg,
    JsonObjectAgg,
    JsonbObjectAgg,
}

impl AggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Variance => "variance",
            AggFunc::Stddev => "stddev",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::JsonAgg => "json_agg",
            AggFunc::JsonbAgg => "jsonb_agg",
            AggFunc::JsonObjectAgg => "json_object_agg",
            AggFunc::JsonbObjectAgg => "jsonb_object_agg",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinScalarFunction {
    Random,
    GetDatabaseEncoding,
    ToJson,
    ToJsonb,
    ArrayToJson,
    JsonBuildArray,
    JsonBuildObject,
    JsonObject,
    JsonTypeof,
    JsonArrayLength,
    JsonExtractPath,
    JsonExtractPathText,
    JsonbTypeof,
    JsonbArrayLength,
    JsonbExtractPath,
    JsonbExtractPathText,
    JsonbBuildArray,
    JsonbBuildObject,
    JsonbPathExists,
    JsonbPathMatch,
    JsonbPathQueryArray,
    JsonbPathQueryFirst,
    Left,
    Repeat,
    Length,
    Lower,
    BpcharToText,
    Position,
    Substring,
    Overlay,
    GetBit,
    SetBit,
    BitCount,
    ConvertFrom,
    Md5,
    ToChar,
    ToNumber,
    Abs,
    Log,
    Log10,
    Gcd,
    Lcm,
    Div,
    Mod,
    Scale,
    MinScale,
    TrimScale,
    NumericInc,
    Factorial,
    PgLsn,
    Trunc,
    Round,
    WidthBucket,
    Ceil,
    Ceiling,
    Floor,
    Sign,
    Sqrt,
    Cbrt,
    Power,
    Exp,
    Ln,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    Sind,
    Cosd,
    Tand,
    Cotd,
    Asind,
    Acosd,
    Atand,
    Atan2d,
    Float4Send,
    Float8Send,
    Erf,
    Erfc,
    Gamma,
    Lgamma,
    BoolEq,
    BoolNe,
    BitcastIntegerToFloat4,
    BitcastBigintToFloat8,
    PgInputIsValid,
    PgInputErrorMessage,
    PgInputErrorDetail,
    PgInputErrorHint,
    PgInputErrorSqlState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonTableFunction {
    ObjectKeys,
    Each,
    EachText,
    ArrayElements,
    ArrayElementsText,
    JsonbObjectKeys,
    JsonbEach,
    JsonbEachText,
    JsonbArrayElements,
    JsonbArrayElementsText,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggAccum {
    pub func: AggFunc,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub sql_type: SqlType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(usize),
    OuterColumn {
        depth: usize,
        index: usize,
    },
    Const(Value),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    BitAnd(Box<Expr>, Box<Expr>),
    BitOr(Box<Expr>, Box<Expr>),
    BitXor(Box<Expr>, Box<Expr>),
    Shl(Box<Expr>, Box<Expr>),
    Shr(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Mod(Box<Expr>, Box<Expr>),
    Concat(Box<Expr>, Box<Expr>),
    UnaryPlus(Box<Expr>),
    Negate(Box<Expr>),
    BitNot(Box<Expr>),
    Cast(Box<Expr>, SqlType),
    Eq(Box<Expr>, Box<Expr>),
    NotEq(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    LtEq(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    GtEq(Box<Expr>, Box<Expr>),
    RegexMatch(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsDistinctFrom(Box<Expr>, Box<Expr>),
    IsNotDistinctFrom(Box<Expr>, Box<Expr>),
    ArrayLiteral {
        elements: Vec<Expr>,
        array_type: SqlType,
    },
    ArrayOverlap(Box<Expr>, Box<Expr>),
    JsonbContains(Box<Expr>, Box<Expr>),
    JsonbContained(Box<Expr>, Box<Expr>),
    JsonbExists(Box<Expr>, Box<Expr>),
    JsonbExistsAny(Box<Expr>, Box<Expr>),
    JsonbExistsAll(Box<Expr>, Box<Expr>),
    JsonbPathExists(Box<Expr>, Box<Expr>),
    JsonbPathMatch(Box<Expr>, Box<Expr>),
    ScalarSubquery(Box<Plan>),
    ExistsSubquery(Box<Plan>),
    AnySubquery {
        left: Box<Expr>,
        op: SubqueryComparisonOp,
        subquery: Box<Plan>,
    },
    AllSubquery {
        left: Box<Expr>,
        op: SubqueryComparisonOp,
        subquery: Box<Plan>,
    },
    AnyArray {
        left: Box<Expr>,
        op: SubqueryComparisonOp,
        right: Box<Expr>,
    },
    AllArray {
        left: Box<Expr>,
        op: SubqueryComparisonOp,
        right: Box<Expr>,
    },
    Random,
    JsonGet(Box<Expr>, Box<Expr>),
    JsonGetText(Box<Expr>, Box<Expr>),
    JsonPath(Box<Expr>, Box<Expr>),
    JsonPathText(Box<Expr>, Box<Expr>),
    FuncCall {
        func: BuiltinScalarFunction,
        args: Vec<Expr>,
    },
    CurrentTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    Result,
    SeqScan {
        rel: RelFileLocator,
        desc: RelationDesc,
    },
    NestedLoopJoin {
        left: Box<Plan>,
        right: Box<Plan>,
        on: Expr,
    },
    Filter {
        input: Box<Plan>,
        predicate: Expr,
    },
    OrderBy {
        input: Box<Plan>,
        items: Vec<OrderByEntry>,
    },
    Limit {
        input: Box<Plan>,
        limit: Option<usize>,
        offset: usize,
    },
    Projection {
        input: Box<Plan>,
        targets: Vec<TargetEntry>,
    },
    Aggregate {
        input: Box<Plan>,
        group_by: Vec<Expr>,
        accumulators: Vec<AggAccum>,
        having: Option<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    GenerateSeries {
        start: Expr,
        stop: Expr,
        step: Expr,
        output: QueryColumn,
    },
    Values {
        rows: Vec<Vec<Expr>>,
        output_columns: Vec<QueryColumn>,
    },
    Unnest {
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    JsonTableFunction {
        kind: JsonTableFunction,
        arg: Expr,
        output_columns: Vec<QueryColumn>,
    },
}

impl Plan {
    pub fn columns(&self) -> Vec<QueryColumn> {
        match self {
            Plan::Result => vec![],
            Plan::SeqScan { desc, .. } => desc
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
            Plan::GenerateSeries { output, .. } => vec![output.clone()],
            Plan::Values { output_columns, .. } => output_columns.clone(),
            Plan::Unnest { output_columns, .. } => output_columns.clone(),
            Plan::JsonTableFunction { output_columns, .. } => output_columns.clone(),
        }
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns().into_iter().map(|c| c.name).collect()
    }
}
