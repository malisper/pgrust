use crate::RelFileLocator;
use crate::backend::parser::{SqlType, SqlTypeKind, SubqueryComparisonOp};
use crate::include::access::htup::AttributeDesc;
use crate::include::catalog::{
    builtin_scalar_function_for_proc_oid, proc_oid_for_builtin_scalar_function,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::plannodes::DeferredSelectPlan;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarType {
    Int16,
    Int32,
    Int64,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    BitString,
    Bytea,
    Point,
    Lseg,
    Path,
    Line,
    Box,
    Polygon,
    Circle,
    Float32,
    Float64,
    Numeric,
    Json,
    Jsonb,
    JsonPath,
    TsVector,
    TsQuery,
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
    pub attstattarget: i16,
    pub not_null_constraint_oid: Option<u32>,
    pub attrdef_oid: Option<u32>,
    pub default_expr: Option<String>,
    pub missing_default_value: Option<Value>,
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
    pub resno: usize,
    pub ressortgroupref: usize,
    pub resjunk: bool,
}

impl TargetEntry {
    pub fn new(name: impl Into<String>, expr: Expr, sql_type: SqlType, resno: usize) -> Self {
        Self {
            name: name.into(),
            expr,
            sql_type,
            resno,
            ressortgroupref: 0,
            resjunk: false,
        }
    }

    pub fn with_sort_group_ref(mut self, ressortgroupref: usize) -> Self {
        self.ressortgroupref = ressortgroupref;
        self
    }

    pub fn as_resjunk(mut self) -> Self {
        self.resjunk = true;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByEntry {
    pub expr: Expr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortGroupClause {
    pub expr: Expr,
    pub tle_sort_group_ref: usize,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToastRelationRef {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
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
    ArrayAgg,
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
            AggFunc::ArrayAgg => "array_agg",
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
    JsonStripNulls,
    JsonTypeof,
    JsonArrayLength,
    JsonExtractPath,
    JsonExtractPathText,
    JsonbObject,
    JsonbStripNulls,
    JsonbPretty,
    JsonbTypeof,
    JsonbArrayLength,
    JsonbExtractPath,
    JsonbExtractPathText,
    JsonbBuildArray,
    JsonbBuildObject,
    JsonbDelete,
    JsonbDeletePath,
    JsonbSet,
    JsonbSetLax,
    JsonbInsert,
    JsonbPathExists,
    JsonbPathMatch,
    JsonbPathQueryArray,
    JsonbPathQueryFirst,
    BTrim,
    LTrim,
    RTrim,
    RegexpMatch,
    RegexpLike,
    RegexpReplace,
    RegexpCount,
    RegexpInstr,
    RegexpSubstr,
    RegexpSplitToArray,
    SimilarSubstring,
    Initcap,
    Concat,
    ConcatWs,
    Format,
    Left,
    Right,
    LPad,
    RPad,
    Repeat,
    Strpos,
    Length,
    ArrayNdims,
    ArrayDims,
    ArrayLower,
    ArrayFill,
    StringToArray,
    ArrayToString,
    ArrayLength,
    Cardinality,
    ArrayPosition,
    ArrayPositions,
    ArrayRemove,
    ArrayReplace,
    ArraySort,
    Lower,
    Unistr,
    Ascii,
    Chr,
    QuoteLiteral,
    Replace,
    SplitPart,
    Translate,
    BpcharToText,
    Position,
    Substring,
    Overlay,
    Reverse,
    GetBit,
    SetBit,
    GetByte,
    SetByte,
    BitCount,
    Encode,
    Decode,
    ConvertFrom,
    Md5,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
    Crc32,
    Crc32c,
    ToChar,
    ToNumber,
    Now,
    TransactionTimestamp,
    StatementTimestamp,
    ClockTimestamp,
    TimeOfDay,
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
    GeoPoint,
    GeoBox,
    GeoLine,
    GeoLseg,
    GeoPath,
    GeoPolygon,
    GeoCircle,
    GeoArea,
    GeoCenter,
    GeoPolyCenter,
    GeoBoundBox,
    GeoDiagonal,
    GeoLength,
    GeoRadius,
    GeoDiameter,
    GeoNpoints,
    GeoPclose,
    GeoPopen,
    GeoIsOpen,
    GeoIsClosed,
    GeoSlope,
    GeoIsVertical,
    GeoIsHorizontal,
    GeoHeight,
    GeoWidth,
    GeoEq,
    GeoNe,
    GeoLt,
    GeoLe,
    GeoGt,
    GeoGe,
    GeoSame,
    GeoDistance,
    GeoClosestPoint,
    GeoIntersection,
    GeoIntersects,
    GeoParallel,
    GeoPerpendicular,
    GeoContains,
    GeoContainedBy,
    GeoOverlap,
    GeoLeft,
    GeoOverLeft,
    GeoRight,
    GeoOverRight,
    GeoBelow,
    GeoOverBelow,
    GeoAbove,
    GeoOverAbove,
    GeoAdd,
    GeoSub,
    GeoMul,
    GeoDiv,
    GeoPointX,
    GeoPointY,
    BoolEq,
    BoolNe,
    TsMatch,
    ToTsVector,
    ToTsQuery,
    PlainToTsQuery,
    PhraseToTsQuery,
    WebSearchToTsQuery,
    TsLexize,
    TsQueryAnd,
    TsQueryOr,
    TsQueryNot,
    TsVectorConcat,
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
    JsonbPathQuery,
    JsonbObjectKeys,
    JsonbEach,
    JsonbEachText,
    JsonbArrayElements,
    JsonbArrayElementsText,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegexTableFunction {
    Matches,
    SplitToTable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextSearchTableFunction {
    TokenType,
    Parse,
    Debug,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetReturningCall {
    GenerateSeries {
        func_oid: u32,
        func_variadic: bool,
        start: Expr,
        stop: Expr,
        step: Expr,
        output: QueryColumn,
    },
    Unnest {
        func_oid: u32,
        func_variadic: bool,
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    JsonTableFunction {
        func_oid: u32,
        func_variadic: bool,
        kind: JsonTableFunction,
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    RegexTableFunction {
        func_oid: u32,
        func_variadic: bool,
        kind: RegexTableFunction,
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    TextSearchTableFunction {
        kind: TextSearchTableFunction,
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
    },
}

impl SetReturningCall {
    pub fn output_columns(&self) -> &[QueryColumn] {
        match self {
            SetReturningCall::GenerateSeries { output, .. } => std::slice::from_ref(output),
            SetReturningCall::Unnest { output_columns, .. }
            | SetReturningCall::JsonTableFunction { output_columns, .. }
            | SetReturningCall::RegexTableFunction { output_columns, .. }
            | SetReturningCall::TextSearchTableFunction { output_columns, .. } => output_columns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectSetTarget {
    Scalar(TargetEntry),
    Set {
        name: String,
        call: SetReturningCall,
        sql_type: SqlType,
        column_index: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggAccum {
    pub aggfnoid: u32,
    pub agg_variadic: bool,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub sql_type: SqlType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Cross,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Var {
    pub varno: usize,
    pub varattno: usize,
    pub varlevelsup: usize,
    pub vartype: SqlType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoolExprType {
    And,
    Or,
    Not,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoolExpr {
    pub boolop: BoolExprType,
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpExprKind {
    Add,
    Sub,
    BitAnd,
    BitOr,
    BitXor,
    Shl,
    Shr,
    Mul,
    Div,
    Mod,
    Concat,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    RegexMatch,
    ArrayOverlap,
    JsonbContains,
    JsonbContained,
    JsonbExists,
    JsonbExistsAny,
    JsonbExistsAll,
    JsonbPathExists,
    JsonbPathMatch,
    JsonGet,
    JsonGetText,
    JsonPath,
    JsonPathText,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpExpr {
    pub opno: u32,
    pub opfuncid: u32,
    pub op: OpExprKind,
    pub opresulttype: SqlType,
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuncExpr {
    pub funcid: u32,
    pub funcresulttype: Option<SqlType>,
    pub funcvariadic: bool,
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubLinkType {
    ExistsSubLink,
    AllSubLink(SubqueryComparisonOp),
    AnySubLink(SubqueryComparisonOp),
    ExprSubLink,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubLink {
    pub sublink_type: SubLinkType,
    pub testexpr: Option<Box<Expr>>,
    pub subselect: Box<DeferredSelectPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarArrayOpExpr {
    pub op: SubqueryComparisonOp,
    pub use_or: bool,
    pub left: Box<Expr>,
    pub right: Box<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Var(Var),
    Column(usize),
    OuterColumn {
        depth: usize,
        index: usize,
    },
    Const(Value),
    Op(Box<OpExpr>),
    Bool(Box<BoolExpr>),
    Func(Box<FuncExpr>),
    SubLink(Box<SubLink>),
    ScalarArrayOp(Box<ScalarArrayOpExpr>),
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
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        case_insensitive: bool,
        negated: bool,
    },
    Similar {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        negated: bool,
    },
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
    ScalarSubquery(Box<DeferredSelectPlan>),
    ExistsSubquery(Box<DeferredSelectPlan>),
    Coalesce(Box<Expr>, Box<Expr>),
    AnySubquery {
        left: Box<Expr>,
        op: SubqueryComparisonOp,
        subquery: Box<DeferredSelectPlan>,
    },
    AllSubquery {
        left: Box<Expr>,
        op: SubqueryComparisonOp,
        subquery: Box<DeferredSelectPlan>,
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
    ArraySubscript {
        array: Box<Expr>,
        subscripts: Vec<ExprArraySubscript>,
    },
    Random,
    JsonGet(Box<Expr>, Box<Expr>),
    JsonGetText(Box<Expr>, Box<Expr>),
    JsonPath(Box<Expr>, Box<Expr>),
    JsonPathText(Box<Expr>, Box<Expr>),
    FuncCall {
        func_oid: u32,
        func: BuiltinScalarFunction,
        args: Vec<Expr>,
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

impl Expr {
    pub fn into_pg_semantic_shape(self) -> Self {
        match self {
            Expr::Var(_)
            | Expr::Column(_)
            | Expr::OuterColumn { .. }
            | Expr::Const(_)
            | Expr::Random
            | Expr::CurrentDate
            | Expr::CurrentTime { .. }
            | Expr::CurrentTimestamp { .. }
            | Expr::LocalTime { .. }
            | Expr::LocalTimestamp { .. } => self,
            Expr::Op(op) => Expr::Op(Box::new(OpExpr {
                args: op
                    .args
                    .into_iter()
                    .map(Expr::into_pg_semantic_shape)
                    .collect(),
                ..*op
            })),
            Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
                args: bool_expr
                    .args
                    .into_iter()
                    .map(Expr::into_pg_semantic_shape)
                    .collect(),
                ..*bool_expr
            })),
            Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
                args: func
                    .args
                    .into_iter()
                    .map(Expr::into_pg_semantic_shape)
                    .collect(),
                ..*func
            })),
            Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
                testexpr: sublink
                    .testexpr
                    .map(|expr| Box::new(expr.into_pg_semantic_shape())),
                ..*sublink
            })),
            Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
                left: Box::new(saop.left.into_pg_semantic_shape()),
                right: Box::new(saop.right.into_pg_semantic_shape()),
                ..*saop
            })),
            Expr::Add(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Add,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Sub(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Sub,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::BitAnd(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::BitAnd,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::BitOr(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::BitOr,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::BitXor(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::BitXor,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Shl(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Shl,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Shr(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Shr,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Mul(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Mul,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Div(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Div,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Mod(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Mod,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Concat(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Concat,
                opresulttype: SqlType::new(SqlTypeKind::Text),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::UnaryPlus(inner) => Expr::UnaryPlus(Box::new(inner.into_pg_semantic_shape())),
            Expr::Negate(inner) => Expr::Negate(Box::new(inner.into_pg_semantic_shape())),
            Expr::BitNot(inner) => Expr::BitNot(Box::new(inner.into_pg_semantic_shape())),
            Expr::Cast(inner, ty) => Expr::Cast(Box::new(inner.into_pg_semantic_shape()), ty),
            Expr::Eq(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Eq,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::NotEq(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::NotEq,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Lt(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Lt,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::LtEq(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::LtEq,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Gt(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::Gt,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::GtEq(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::GtEq,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::RegexMatch(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::RegexMatch,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Expr::Like {
                expr: Box::new(expr.into_pg_semantic_shape()),
                pattern: Box::new(pattern.into_pg_semantic_shape()),
                escape: escape.map(|expr| Box::new(expr.into_pg_semantic_shape())),
                case_insensitive,
                negated,
            },
            Expr::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Expr::Similar {
                expr: Box::new(expr.into_pg_semantic_shape()),
                pattern: Box::new(pattern.into_pg_semantic_shape()),
                escape: escape.map(|expr| Box::new(expr.into_pg_semantic_shape())),
                negated,
            },
            Expr::And(left, right) => Expr::Bool(Box::new(BoolExpr {
                boolop: BoolExprType::And,
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Or(left, right) => Expr::Bool(Box::new(BoolExpr {
                boolop: BoolExprType::Or,
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::Not(inner) => Expr::Bool(Box::new(BoolExpr {
                boolop: BoolExprType::Not,
                args: vec![inner.into_pg_semantic_shape()],
            })),
            Expr::IsNull(inner) => Expr::IsNull(Box::new(inner.into_pg_semantic_shape())),
            Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(inner.into_pg_semantic_shape())),
            Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
                Box::new(left.into_pg_semantic_shape()),
                Box::new(right.into_pg_semantic_shape()),
            ),
            Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
                Box::new(left.into_pg_semantic_shape()),
                Box::new(right.into_pg_semantic_shape()),
            ),
            Expr::ArrayLiteral {
                elements,
                array_type,
            } => Expr::ArrayLiteral {
                elements: elements
                    .into_iter()
                    .map(Expr::into_pg_semantic_shape)
                    .collect(),
                array_type,
            },
            Expr::ArrayOverlap(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::ArrayOverlap,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::JsonbContains(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::JsonbContains,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::JsonbContained(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::JsonbContained,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::JsonbExists(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::JsonbExists,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::JsonbExistsAny(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::JsonbExistsAny,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::JsonbExistsAll(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::JsonbExistsAll,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::JsonbPathExists(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::JsonbPathExists,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::JsonbPathMatch(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::JsonbPathMatch,
                opresulttype: SqlType::new(SqlTypeKind::Bool),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::ScalarSubquery(plan) => Expr::SubLink(Box::new(SubLink {
                sublink_type: SubLinkType::ExprSubLink,
                testexpr: None,
                subselect: plan,
            })),
            Expr::ExistsSubquery(plan) => Expr::SubLink(Box::new(SubLink {
                sublink_type: SubLinkType::ExistsSubLink,
                testexpr: None,
                subselect: plan,
            })),
            Expr::Coalesce(left, right) => Expr::Coalesce(
                Box::new(left.into_pg_semantic_shape()),
                Box::new(right.into_pg_semantic_shape()),
            ),
            Expr::AnySubquery { left, op, subquery } => Expr::SubLink(Box::new(SubLink {
                sublink_type: SubLinkType::AnySubLink(op),
                testexpr: Some(Box::new(left.into_pg_semantic_shape())),
                subselect: subquery,
            })),
            Expr::AllSubquery { left, op, subquery } => Expr::SubLink(Box::new(SubLink {
                sublink_type: SubLinkType::AllSubLink(op),
                testexpr: Some(Box::new(left.into_pg_semantic_shape())),
                subselect: subquery,
            })),
            Expr::AnyArray { left, op, right } => {
                Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
                    op,
                    use_or: true,
                    left: Box::new(left.into_pg_semantic_shape()),
                    right: Box::new(right.into_pg_semantic_shape()),
                }))
            }
            Expr::AllArray { left, op, right } => {
                Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
                    op,
                    use_or: false,
                    left: Box::new(left.into_pg_semantic_shape()),
                    right: Box::new(right.into_pg_semantic_shape()),
                }))
            }
            Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
                array: Box::new(array.into_pg_semantic_shape()),
                subscripts: subscripts
                    .into_iter()
                    .map(|subscript| ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript.lower.map(Expr::into_pg_semantic_shape),
                        upper: subscript.upper.map(Expr::into_pg_semantic_shape),
                    })
                    .collect(),
            },
            Expr::JsonGet(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::JsonGet,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::JsonGetText(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::JsonGetText,
                opresulttype: SqlType::new(SqlTypeKind::Text),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::JsonPath(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::JsonPath,
                opresulttype: binary_result_type(&left, &right),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::JsonPathText(left, right) => Expr::Op(Box::new(OpExpr {
                opno: 0,
                opfuncid: 0,
                op: OpExprKind::JsonPathText,
                opresulttype: SqlType::new(SqlTypeKind::Text),
                args: vec![
                    left.into_pg_semantic_shape(),
                    right.into_pg_semantic_shape(),
                ],
            })),
            Expr::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Expr::Func(Box::new(FuncExpr {
                funcid: if func_oid != 0 {
                    func_oid
                } else {
                    proc_oid_for_builtin_scalar_function(func).unwrap_or_else(|| {
                        panic!(
                            "builtin scalar function {:?} lacks pg_proc OID mapping",
                            func
                        )
                    })
                },
                funcresulttype: None,
                funcvariadic: func_variadic,
                args: args.into_iter().map(Expr::into_pg_semantic_shape).collect(),
            })),
        }
    }

    pub fn into_legacy_shape(self) -> Self {
        match self {
            Expr::Op(op) => {
                let op = *op;
                op.op.into_legacy_expr(op.args)
            }
            Expr::Bool(bool_expr) => {
                let bool_expr = *bool_expr;
                let mut args = bool_expr.args.into_iter().map(Expr::into_legacy_shape);
                match bool_expr.boolop {
                    BoolExprType::And => match (args.next(), args.next()) {
                        (Some(left), Some(right)) => Expr::And(Box::new(left), Box::new(right)),
                        (Some(single), None) => single,
                        _ => Expr::Const(Value::Null),
                    },
                    BoolExprType::Or => match (args.next(), args.next()) {
                        (Some(left), Some(right)) => Expr::Or(Box::new(left), Box::new(right)),
                        (Some(single), None) => single,
                        _ => Expr::Const(Value::Null),
                    },
                    BoolExprType::Not => args
                        .next()
                        .map(|inner| Expr::Not(Box::new(inner)))
                        .unwrap_or(Expr::Const(Value::Null)),
                }
            }
            Expr::Func(func) => {
                let func = *func;
                Expr::FuncCall {
                    func_oid: func.funcid,
                    func: builtin_scalar_function_for_proc_oid(func.funcid).unwrap_or_else(|| {
                        panic!(
                            "semantic FuncExpr {:?} lacks builtin implementation mapping",
                            func.funcid
                        )
                    }),
                    args: func.args.into_iter().map(Expr::into_legacy_shape).collect(),
                    func_variadic: func.funcvariadic,
                }
            }
            Expr::SubLink(sublink) => {
                let sublink = *sublink;
                match sublink.sublink_type {
                    SubLinkType::ExprSubLink => Expr::ScalarSubquery(sublink.subselect),
                    SubLinkType::ExistsSubLink => Expr::ExistsSubquery(sublink.subselect),
                    SubLinkType::AnySubLink(op) => Expr::AnySubquery {
                        left: Box::new(
                            sublink
                                .testexpr
                                .map(|expr| expr.into_legacy_shape())
                                .unwrap_or(Expr::Const(Value::Null)),
                        ),
                        op,
                        subquery: sublink.subselect,
                    },
                    SubLinkType::AllSubLink(op) => Expr::AllSubquery {
                        left: Box::new(
                            sublink
                                .testexpr
                                .map(|expr| expr.into_legacy_shape())
                                .unwrap_or(Expr::Const(Value::Null)),
                        ),
                        op,
                        subquery: sublink.subselect,
                    },
                }
            }
            Expr::ScalarArrayOp(saop) => {
                let saop = *saop;
                if saop.use_or {
                    Expr::AnyArray {
                        left: Box::new(saop.left.into_legacy_shape()),
                        op: saop.op,
                        right: Box::new(saop.right.into_legacy_shape()),
                    }
                } else {
                    Expr::AllArray {
                        left: Box::new(saop.left.into_legacy_shape()),
                        op: saop.op,
                        right: Box::new(saop.right.into_legacy_shape()),
                    }
                }
            }
            Expr::UnaryPlus(inner) => Expr::UnaryPlus(Box::new(inner.into_legacy_shape())),
            Expr::Negate(inner) => Expr::Negate(Box::new(inner.into_legacy_shape())),
            Expr::BitNot(inner) => Expr::BitNot(Box::new(inner.into_legacy_shape())),
            Expr::Cast(inner, ty) => Expr::Cast(Box::new(inner.into_legacy_shape()), ty),
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Expr::Like {
                expr: Box::new(expr.into_legacy_shape()),
                pattern: Box::new(pattern.into_legacy_shape()),
                escape: escape.map(|expr| Box::new(expr.into_legacy_shape())),
                case_insensitive,
                negated,
            },
            Expr::Similar {
                expr,
                pattern,
                escape,
                negated,
            } => Expr::Similar {
                expr: Box::new(expr.into_legacy_shape()),
                pattern: Box::new(pattern.into_legacy_shape()),
                escape: escape.map(|expr| Box::new(expr.into_legacy_shape())),
                negated,
            },
            Expr::IsNull(inner) => Expr::IsNull(Box::new(inner.into_legacy_shape())),
            Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(inner.into_legacy_shape())),
            Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::ArrayLiteral {
                elements,
                array_type,
            } => Expr::ArrayLiteral {
                elements: elements.into_iter().map(Expr::into_legacy_shape).collect(),
                array_type,
            },
            Expr::Coalesce(left, right) => Expr::Coalesce(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
                array: Box::new(array.into_legacy_shape()),
                subscripts: subscripts
                    .into_iter()
                    .map(|subscript| ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript.lower.map(Expr::into_legacy_shape),
                        upper: subscript.upper.map(Expr::into_legacy_shape),
                    })
                    .collect(),
            },
            Expr::Add(left, right) => Expr::Add(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Sub(left, right) => Expr::Sub(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::BitAnd(left, right) => Expr::BitAnd(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::BitOr(left, right) => Expr::BitOr(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::BitXor(left, right) => Expr::BitXor(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Shl(left, right) => Expr::Shl(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Shr(left, right) => Expr::Shr(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Mul(left, right) => Expr::Mul(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Div(left, right) => Expr::Div(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Mod(left, right) => Expr::Mod(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Concat(left, right) => Expr::Concat(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Eq(left, right) => Expr::Eq(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::NotEq(left, right) => Expr::NotEq(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Lt(left, right) => Expr::Lt(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::LtEq(left, right) => Expr::LtEq(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Gt(left, right) => Expr::Gt(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::GtEq(left, right) => Expr::GtEq(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::RegexMatch(left, right) => Expr::RegexMatch(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::And(left, right) => Expr::And(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Or(left, right) => Expr::Or(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::Not(inner) => Expr::Not(Box::new(inner.into_legacy_shape())),
            Expr::ArrayOverlap(left, right) => Expr::ArrayOverlap(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::JsonbContains(left, right) => Expr::JsonbContains(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::JsonbContained(left, right) => Expr::JsonbContained(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::JsonbExists(left, right) => Expr::JsonbExists(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::JsonbExistsAny(left, right) => Expr::JsonbExistsAny(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::JsonbExistsAll(left, right) => Expr::JsonbExistsAll(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::JsonbPathExists(left, right) => Expr::JsonbPathExists(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::JsonbPathMatch(left, right) => Expr::JsonbPathMatch(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::ScalarSubquery(plan) => Expr::ScalarSubquery(plan),
            Expr::ExistsSubquery(plan) => Expr::ExistsSubquery(plan),
            Expr::AnySubquery { left, op, subquery } => Expr::AnySubquery {
                left: Box::new(left.into_legacy_shape()),
                op,
                subquery,
            },
            Expr::AllSubquery { left, op, subquery } => Expr::AllSubquery {
                left: Box::new(left.into_legacy_shape()),
                op,
                subquery,
            },
            Expr::AnyArray { left, op, right } => Expr::AnyArray {
                left: Box::new(left.into_legacy_shape()),
                op,
                right: Box::new(right.into_legacy_shape()),
            },
            Expr::AllArray { left, op, right } => Expr::AllArray {
                left: Box::new(left.into_legacy_shape()),
                op,
                right: Box::new(right.into_legacy_shape()),
            },
            Expr::JsonGet(left, right) => Expr::JsonGet(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::JsonGetText(left, right) => Expr::JsonGetText(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::JsonPath(left, right) => Expr::JsonPath(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::JsonPathText(left, right) => Expr::JsonPathText(
                Box::new(left.into_legacy_shape()),
                Box::new(right.into_legacy_shape()),
            ),
            Expr::FuncCall {
                func_oid,
                func,
                args,
                func_variadic,
            } => Expr::FuncCall {
                func_oid,
                func,
                args: args.into_iter().map(Expr::into_legacy_shape).collect(),
                func_variadic,
            },
            Expr::Var(_)
            | Expr::Column(_)
            | Expr::OuterColumn { .. }
            | Expr::Const(_)
            | Expr::Random
            | Expr::CurrentDate
            | Expr::CurrentTime { .. }
            | Expr::CurrentTimestamp { .. }
            | Expr::LocalTime { .. }
            | Expr::LocalTimestamp { .. } => self,
        }
    }
}

impl OpExprKind {
    fn into_legacy_expr(self, mut args: Vec<Expr>) -> Expr {
        let right = args.pop().map(Expr::into_legacy_shape);
        let left = args.pop().map(Expr::into_legacy_shape);
        match (self, left, right) {
            (Self::Add, Some(left), Some(right)) => Expr::Add(Box::new(left), Box::new(right)),
            (Self::Sub, Some(left), Some(right)) => Expr::Sub(Box::new(left), Box::new(right)),
            (Self::BitAnd, Some(left), Some(right)) => {
                Expr::BitAnd(Box::new(left), Box::new(right))
            }
            (Self::BitOr, Some(left), Some(right)) => Expr::BitOr(Box::new(left), Box::new(right)),
            (Self::BitXor, Some(left), Some(right)) => {
                Expr::BitXor(Box::new(left), Box::new(right))
            }
            (Self::Shl, Some(left), Some(right)) => Expr::Shl(Box::new(left), Box::new(right)),
            (Self::Shr, Some(left), Some(right)) => Expr::Shr(Box::new(left), Box::new(right)),
            (Self::Mul, Some(left), Some(right)) => Expr::Mul(Box::new(left), Box::new(right)),
            (Self::Div, Some(left), Some(right)) => Expr::Div(Box::new(left), Box::new(right)),
            (Self::Mod, Some(left), Some(right)) => Expr::Mod(Box::new(left), Box::new(right)),
            (Self::Concat, Some(left), Some(right)) => {
                Expr::Concat(Box::new(left), Box::new(right))
            }
            (Self::Eq, Some(left), Some(right)) => Expr::Eq(Box::new(left), Box::new(right)),
            (Self::NotEq, Some(left), Some(right)) => Expr::NotEq(Box::new(left), Box::new(right)),
            (Self::Lt, Some(left), Some(right)) => Expr::Lt(Box::new(left), Box::new(right)),
            (Self::LtEq, Some(left), Some(right)) => Expr::LtEq(Box::new(left), Box::new(right)),
            (Self::Gt, Some(left), Some(right)) => Expr::Gt(Box::new(left), Box::new(right)),
            (Self::GtEq, Some(left), Some(right)) => Expr::GtEq(Box::new(left), Box::new(right)),
            (Self::RegexMatch, Some(left), Some(right)) => {
                Expr::RegexMatch(Box::new(left), Box::new(right))
            }
            (Self::ArrayOverlap, Some(left), Some(right)) => {
                Expr::ArrayOverlap(Box::new(left), Box::new(right))
            }
            (Self::JsonbContains, Some(left), Some(right)) => {
                Expr::JsonbContains(Box::new(left), Box::new(right))
            }
            (Self::JsonbContained, Some(left), Some(right)) => {
                Expr::JsonbContained(Box::new(left), Box::new(right))
            }
            (Self::JsonbExists, Some(left), Some(right)) => {
                Expr::JsonbExists(Box::new(left), Box::new(right))
            }
            (Self::JsonbExistsAny, Some(left), Some(right)) => {
                Expr::JsonbExistsAny(Box::new(left), Box::new(right))
            }
            (Self::JsonbExistsAll, Some(left), Some(right)) => {
                Expr::JsonbExistsAll(Box::new(left), Box::new(right))
            }
            (Self::JsonbPathExists, Some(left), Some(right)) => {
                Expr::JsonbPathExists(Box::new(left), Box::new(right))
            }
            (Self::JsonbPathMatch, Some(left), Some(right)) => {
                Expr::JsonbPathMatch(Box::new(left), Box::new(right))
            }
            (Self::JsonGet, Some(left), Some(right)) => {
                Expr::JsonGet(Box::new(left), Box::new(right))
            }
            (Self::JsonGetText, Some(left), Some(right)) => {
                Expr::JsonGetText(Box::new(left), Box::new(right))
            }
            (Self::JsonPath, Some(left), Some(right)) => {
                Expr::JsonPath(Box::new(left), Box::new(right))
            }
            (Self::JsonPathText, Some(left), Some(right)) => {
                Expr::JsonPathText(Box::new(left), Box::new(right))
            }
            (_, Some(left), None) => left,
            _ => Expr::Const(Value::Null),
        }
    }
}

// :HACK: This is only a migration hint while pgrust is moving from legacy
// operator-per-enum expression nodes to PostgreSQL-shaped semantic nodes.
// The planner/executor do not rely on these result types yet, so a best-effort
// approximation is sufficient for now.
fn binary_result_type(left: &Expr, right: &Expr) -> SqlType {
    expr_sql_type_hint(left)
        .or_else(|| expr_sql_type_hint(right))
        .unwrap_or(SqlType::new(SqlTypeKind::Text))
}

fn expr_sql_type_hint(expr: &Expr) -> Option<SqlType> {
    match expr {
        Expr::Var(var) => Some(var.vartype),
        Expr::Const(value) => value_sql_type_hint(value),
        Expr::Cast(_, ty) => Some(*ty),
        Expr::ArrayLiteral { array_type, .. } => Some(*array_type),
        Expr::Coalesce(left, right) => {
            expr_sql_type_hint(left).or_else(|| expr_sql_type_hint(right))
        }
        Expr::Op(op) => Some(op.opresulttype),
        Expr::Func(func) => func.funcresulttype,
        Expr::ScalarArrayOp(_) => Some(SqlType::new(SqlTypeKind::Bool)),
        Expr::Add(left, right)
        | Expr::Sub(left, right)
        | Expr::BitAnd(left, right)
        | Expr::BitOr(left, right)
        | Expr::BitXor(left, right)
        | Expr::Shl(left, right)
        | Expr::Shr(left, right)
        | Expr::Mul(left, right)
        | Expr::Div(left, right)
        | Expr::Mod(left, right)
        | Expr::JsonGet(left, right)
        | Expr::JsonPath(left, right) => {
            expr_sql_type_hint(left).or_else(|| expr_sql_type_hint(right))
        }
        Expr::Concat(_, _) | Expr::JsonGetText(_, _) | Expr::JsonPathText(_, _) => {
            Some(SqlType::new(SqlTypeKind::Text))
        }
        Expr::Bool(_)
        | Expr::Eq(_, _)
        | Expr::NotEq(_, _)
        | Expr::Lt(_, _)
        | Expr::LtEq(_, _)
        | Expr::Gt(_, _)
        | Expr::GtEq(_, _)
        | Expr::RegexMatch(_, _)
        | Expr::And(_, _)
        | Expr::Or(_, _)
        | Expr::Not(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsDistinctFrom(_, _)
        | Expr::IsNotDistinctFrom(_, _)
        | Expr::ArrayOverlap(_, _)
        | Expr::JsonbContains(_, _)
        | Expr::JsonbContained(_, _)
        | Expr::JsonbExists(_, _)
        | Expr::JsonbExistsAny(_, _)
        | Expr::JsonbExistsAll(_, _)
        | Expr::JsonbPathExists(_, _)
        | Expr::JsonbPathMatch(_, _)
        | Expr::ExistsSubquery(_)
        | Expr::AnySubquery { .. }
        | Expr::AllSubquery { .. }
        | Expr::AnyArray { .. }
        | Expr::AllArray { .. }
        | Expr::SubLink(_) => Some(SqlType::new(SqlTypeKind::Bool)),
        Expr::FuncCall { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Column(_)
        | Expr::OuterColumn { .. }
        | Expr::UnaryPlus(_)
        | Expr::Negate(_)
        | Expr::BitNot(_)
        | Expr::Like { .. }
        | Expr::Similar { .. }
        | Expr::ArraySubscript { .. }
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => None,
    }
}

fn value_sql_type_hint(value: &Value) -> Option<SqlType> {
    match value {
        Value::Int16(_) => Some(SqlType::new(SqlTypeKind::Int2)),
        Value::Int32(_) => Some(SqlType::new(SqlTypeKind::Int4)),
        Value::Int64(_) => Some(SqlType::new(SqlTypeKind::Int8)),
        Value::Date(_) => Some(SqlType::new(SqlTypeKind::Date)),
        Value::Time(_) => Some(SqlType::new(SqlTypeKind::Time)),
        Value::TimeTz(_) => Some(SqlType::new(SqlTypeKind::TimeTz)),
        Value::Timestamp(_) => Some(SqlType::new(SqlTypeKind::Timestamp)),
        Value::TimestampTz(_) => Some(SqlType::new(SqlTypeKind::TimestampTz)),
        Value::Bit(_) => Some(SqlType::new(SqlTypeKind::Bit)),
        Value::Bytea(_) => Some(SqlType::new(SqlTypeKind::Bytea)),
        Value::Point(_) => Some(SqlType::new(SqlTypeKind::Point)),
        Value::Lseg(_) => Some(SqlType::new(SqlTypeKind::Lseg)),
        Value::Path(_) => Some(SqlType::new(SqlTypeKind::Path)),
        Value::Line(_) => Some(SqlType::new(SqlTypeKind::Line)),
        Value::Box(_) => Some(SqlType::new(SqlTypeKind::Box)),
        Value::Polygon(_) => Some(SqlType::new(SqlTypeKind::Polygon)),
        Value::Circle(_) => Some(SqlType::new(SqlTypeKind::Circle)),
        Value::Float64(_) => Some(SqlType::new(SqlTypeKind::Float8)),
        Value::Numeric(_) => Some(SqlType::new(SqlTypeKind::Numeric)),
        Value::Json(_) => Some(SqlType::new(SqlTypeKind::Json)),
        Value::Jsonb(_) => Some(SqlType::new(SqlTypeKind::Jsonb)),
        Value::JsonPath(_) => Some(SqlType::new(SqlTypeKind::JsonPath)),
        Value::TsVector(_) => Some(SqlType::new(SqlTypeKind::TsVector)),
        Value::TsQuery(_) => Some(SqlType::new(SqlTypeKind::TsQuery)),
        Value::Text(_) | Value::TextRef(_, _) => Some(SqlType::new(SqlTypeKind::Text)),
        Value::InternalChar(_) => Some(SqlType::new(SqlTypeKind::InternalChar)),
        Value::Bool(_) => Some(SqlType::new(SqlTypeKind::Bool)),
        Value::Array(_) | Value::PgArray(_) | Value::Null => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprArraySubscript {
    pub is_slice: bool,
    pub lower: Option<Expr>,
    pub upper: Option<Expr>,
}
