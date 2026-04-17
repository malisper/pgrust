use crate::RelFileLocator;
use crate::backend::parser::{SqlType, SqlTypeKind, SubqueryComparisonOp};
use crate::include::access::htup::AttributeDesc;
use crate::include::catalog::{
    RECORD_TYPE_OID, builtin_scalar_function_for_proc_oid, proc_oid_for_builtin_scalar_function,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::Query;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarType {
    Int16,
    Int32,
    Int64,
    Money,
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
    Record,
    Bool,
    Array(Box<ScalarType>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDesc {
    pub name: String,
    pub storage: AttributeDesc,
    pub ty: ScalarType,
    pub sql_type: SqlType,
    pub dropped: bool,
    pub attstattarget: i16,
    pub attinhcount: i16,
    pub attislocal: bool,
    pub not_null_constraint_oid: Option<u32>,
    pub not_null_constraint_name: Option<String>,
    pub not_null_constraint_validated: bool,
    pub not_null_primary_key_owned: bool,
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

    pub fn visible_column_indexes(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| (!column.dropped).then_some(index))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetEntry {
    pub name: String,
    pub expr: Expr,
    pub sql_type: SqlType,
    pub resno: usize,
    pub ressortgroupref: usize,
    pub input_resno: Option<usize>,
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
            input_resno: None,
            resjunk: false,
        }
    }

    pub fn with_sort_group_ref(mut self, ressortgroupref: usize) -> Self {
        self.ressortgroupref = ressortgroupref;
        self
    }

    pub fn with_input_resno(mut self, input_resno: usize) -> Self {
        self.input_resno = Some(input_resno);
        self
    }

    pub fn with_input_resno_opt(mut self, input_resno: Option<usize>) -> Self {
        self.input_resno = input_resno;
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
    pub ressortgroupref: usize,
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
    StringAgg,
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
            AggFunc::StringAgg => "string_agg",
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
    PgTypeof,
    CashLarger,
    CashSmaller,
    CashWords,
    ToJson,
    ToJsonb,
    ArrayToJson,
    RowToJson,
    JsonPopulateRecord,
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
    DatePart,
    DateTrunc,
    IsFinite,
    MakeDate,
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
    JsonPopulateRecordSet {
        func_oid: u32,
        func_variadic: bool,
        args: Vec<Expr>,
        row_columns: Option<Vec<QueryColumn>>,
        output_columns: Vec<QueryColumn>,
        recordset: bool,
        return_record_value: bool,
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
    UserDefined {
        proc_oid: u32,
        func_variadic: bool,
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
            | SetReturningCall::JsonPopulateRecordSet { output_columns, .. }
            | SetReturningCall::RegexTableFunction { output_columns, .. }
            | SetReturningCall::TextSearchTableFunction { output_columns, .. }
            | SetReturningCall::UserDefined { output_columns, .. } => output_columns,
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
    pub filter: Option<Expr>,
    pub distinct: bool,
    pub sql_type: SqlType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggref {
    pub aggfnoid: u32,
    pub aggtype: SqlType,
    pub aggvariadic: bool,
    pub aggdistinct: bool,
    pub args: Vec<Expr>,
    pub aggfilter: Option<Expr>,
    pub agglevelsup: usize,
    pub aggno: usize,
}

pub type AttrNumber = i32;

pub const SELF_ITEM_POINTER_ATTR_NO: AttrNumber = -1;
pub const TABLE_OID_ATTR_NO: AttrNumber = -6;
pub const OUTER_VAR: usize = usize::MAX;
pub const INNER_VAR: usize = usize::MAX - 1;
pub const INDEX_VAR: usize = usize::MAX - 2;
pub const ROWID_VAR: usize = usize::MAX - 3;

pub const fn is_special_varno(varno: usize) -> bool {
    varno >= ROWID_VAR
}

pub const fn is_executor_special_varno(varno: usize) -> bool {
    varno >= ROWID_VAR
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamKind {
    Exec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Param {
    pub paramkind: ParamKind,
    pub paramid: usize,
    pub paramtype: SqlType,
}

pub const fn user_attrno(index: usize) -> AttrNumber {
    index as AttrNumber + 1
}

pub const fn attrno_index(attno: AttrNumber) -> Option<usize> {
    if attno > 0 {
        Some((attno - 1) as usize)
    } else {
        None
    }
}

pub const fn is_system_attr(attno: AttrNumber) -> bool {
    attno < 0
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
    pub varattno: AttrNumber,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseWhen {
    pub expr: Expr,
    pub result: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseExpr {
    pub casetype: SqlType,
    pub arg: Option<Box<Expr>>,
    pub args: Vec<CaseWhen>,
    pub defresult: Box<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseTestExpr {
    pub type_id: SqlType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpExprKind {
    UnaryPlus,
    Negate,
    BitNot,
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
    pub implementation: ScalarFunctionImpl,
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionImpl {
    Builtin(BuiltinScalarFunction),
    UserDefined { proc_oid: u32 },
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
    pub subselect: Box<Query>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubPlan {
    pub sublink_type: SubLinkType,
    pub testexpr: Option<Box<Expr>>,
    pub first_col_type: Option<SqlType>,
    pub plan_id: usize,
    pub par_param: Vec<usize>,
    pub args: Vec<Expr>,
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
    Param(Param),
    Const(Value),
    Aggref(Box<Aggref>),
    Op(Box<OpExpr>),
    Bool(Box<BoolExpr>),
    Case(Box<CaseExpr>),
    CaseTest(Box<CaseTestExpr>),
    Func(Box<FuncExpr>),
    SubLink(Box<SubLink>),
    SubPlan(Box<SubPlan>),
    ScalarArrayOp(Box<ScalarArrayOpExpr>),
    Cast(Box<Expr>, SqlType),
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
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsDistinctFrom(Box<Expr>, Box<Expr>),
    IsNotDistinctFrom(Box<Expr>, Box<Expr>),
    ArrayLiteral {
        elements: Vec<Expr>,
        array_type: SqlType,
    },
    Row {
        fields: Vec<(String, Expr)>,
    },
    Coalesce(Box<Expr>, Box<Expr>),
    ArraySubscript {
        array: Box<Expr>,
        subscripts: Vec<ExprArraySubscript>,
    },
    Random,
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
    pub fn op(op: OpExprKind, opresulttype: SqlType, args: Vec<Expr>) -> Self {
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op,
            opresulttype,
            args,
        }))
    }

    pub fn op_auto(op: OpExprKind, args: Vec<Expr>) -> Self {
        let opresulttype = match op {
            OpExprKind::Eq
            | OpExprKind::NotEq
            | OpExprKind::Lt
            | OpExprKind::LtEq
            | OpExprKind::Gt
            | OpExprKind::GtEq
            | OpExprKind::RegexMatch
            | OpExprKind::ArrayOverlap
            | OpExprKind::JsonbContains
            | OpExprKind::JsonbContained
            | OpExprKind::JsonbExists
            | OpExprKind::JsonbExistsAny
            | OpExprKind::JsonbExistsAll
            | OpExprKind::JsonbPathExists
            | OpExprKind::JsonbPathMatch => SqlType::new(SqlTypeKind::Bool),
            OpExprKind::Concat | OpExprKind::JsonGetText | OpExprKind::JsonPathText => {
                SqlType::new(SqlTypeKind::Text)
            }
            _ => {
                let left = args.first();
                let right = args.get(1).or(left);
                match (left, right) {
                    (Some(left), Some(right)) => binary_result_type(left, right),
                    (Some(inner), None) => {
                        expr_sql_type_hint(inner).unwrap_or(SqlType::new(SqlTypeKind::Text))
                    }
                    _ => SqlType::new(SqlTypeKind::Text),
                }
            }
        };
        Self::op(op, opresulttype, args)
    }

    pub fn unary_op(op: OpExprKind, opresulttype: SqlType, arg: Expr) -> Self {
        Self::op(op, opresulttype, vec![arg])
    }

    pub fn binary_op(op: OpExprKind, opresulttype: SqlType, left: Expr, right: Expr) -> Self {
        Self::op(op, opresulttype, vec![left, right])
    }

    pub fn bool_expr(boolop: BoolExprType, args: Vec<Expr>) -> Self {
        Expr::Bool(Box::new(BoolExpr { boolop, args }))
    }

    pub fn and(left: Expr, right: Expr) -> Self {
        Self::bool_expr(BoolExprType::And, vec![left, right])
    }

    pub fn or(left: Expr, right: Expr) -> Self {
        Self::bool_expr(BoolExprType::Or, vec![left, right])
    }

    pub fn not(inner: Expr) -> Self {
        Self::bool_expr(BoolExprType::Not, vec![inner])
    }

    pub fn func(
        funcid: u32,
        funcresulttype: Option<SqlType>,
        funcvariadic: bool,
        args: Vec<Expr>,
    ) -> Self {
        let implementation = builtin_scalar_function_for_proc_oid(funcid)
            .map(ScalarFunctionImpl::Builtin)
            .unwrap_or(ScalarFunctionImpl::UserDefined { proc_oid: funcid });
        Self::func_with_impl(funcid, funcresulttype, funcvariadic, implementation, args)
    }

    pub fn func_with_impl(
        funcid: u32,
        funcresulttype: Option<SqlType>,
        funcvariadic: bool,
        implementation: ScalarFunctionImpl,
        args: Vec<Expr>,
    ) -> Self {
        Expr::Func(Box::new(FuncExpr {
            funcid,
            funcresulttype,
            funcvariadic,
            implementation,
            args,
        }))
    }

    pub fn aggref(
        aggfnoid: u32,
        aggtype: SqlType,
        aggvariadic: bool,
        aggdistinct: bool,
        args: Vec<Expr>,
        aggfilter: Option<Expr>,
        aggno: usize,
    ) -> Self {
        Expr::Aggref(Box::new(Aggref {
            aggfnoid,
            aggtype,
            aggvariadic,
            aggdistinct,
            args,
            aggfilter,
            agglevelsup: 0,
            aggno,
        }))
    }

    pub fn builtin_func(
        func: BuiltinScalarFunction,
        funcresulttype: Option<SqlType>,
        funcvariadic: bool,
        args: Vec<Expr>,
    ) -> Self {
        let funcid = proc_oid_for_builtin_scalar_function(func).unwrap_or_else(|| {
            panic!(
                "builtin scalar function {:?} lacks pg_proc OID mapping",
                func
            )
        });
        Self::func_with_impl(
            funcid,
            funcresulttype,
            funcvariadic,
            ScalarFunctionImpl::Builtin(func),
            args,
        )
    }

    pub fn resolved_builtin_func(
        func: BuiltinScalarFunction,
        funcid: u32,
        funcresulttype: Option<SqlType>,
        funcvariadic: bool,
        args: Vec<Expr>,
    ) -> Self {
        if funcid != 0 {
            Self::func_with_impl(
                funcid,
                funcresulttype,
                funcvariadic,
                ScalarFunctionImpl::Builtin(func),
                args,
            )
        } else {
            Self::builtin_func(func, funcresulttype, funcvariadic, args)
        }
    }

    pub fn user_defined_func(
        funcid: u32,
        funcresulttype: Option<SqlType>,
        funcvariadic: bool,
        args: Vec<Expr>,
    ) -> Self {
        Self::func_with_impl(
            funcid,
            funcresulttype,
            funcvariadic,
            ScalarFunctionImpl::UserDefined { proc_oid: funcid },
            args,
        )
    }

    pub fn scalar_array_op(
        op: SubqueryComparisonOp,
        use_or: bool,
        left: Expr,
        right: Expr,
    ) -> Self {
        Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            op,
            use_or,
            left: Box::new(left),
            right: Box::new(right),
        }))
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
        Expr::Param(param) => Some(param.paramtype),
        Expr::Const(value) => value_sql_type_hint(value),
        Expr::Aggref(aggref) => Some(aggref.aggtype),
        Expr::Cast(_, ty) => Some(*ty),
        Expr::ArrayLiteral { array_type, .. } => Some(*array_type),
        Expr::Row { .. } => Some(SqlType::record(RECORD_TYPE_OID)),
        Expr::Coalesce(left, right) => {
            expr_sql_type_hint(left).or_else(|| expr_sql_type_hint(right))
        }
        Expr::Case(case_expr) => Some(case_expr.casetype),
        Expr::CaseTest(case_test) => Some(case_test.type_id),
        Expr::Op(op) => Some(op.opresulttype),
        Expr::Func(func) => func.funcresulttype,
        Expr::ScalarArrayOp(_) => Some(SqlType::new(SqlTypeKind::Bool)),
        Expr::Bool(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsDistinctFrom(_, _)
        | Expr::IsNotDistinctFrom(_, _) => Some(SqlType::new(SqlTypeKind::Bool)),
        Expr::SubLink(sublink)
            if matches!(
                sublink.sublink_type,
                SubLinkType::ExistsSubLink
                    | SubLinkType::AnySubLink(_)
                    | SubLinkType::AllSubLink(_)
            ) =>
        {
            Some(SqlType::new(SqlTypeKind::Bool))
        }
        Expr::SubLink(sublink) => sublink
            .subselect
            .target_list
            .first()
            .map(|target| target.sql_type),
        Expr::SubPlan(subplan)
            if matches!(
                subplan.sublink_type,
                SubLinkType::ExistsSubLink
                    | SubLinkType::AnySubLink(_)
                    | SubLinkType::AllSubLink(_)
            ) =>
        {
            Some(SqlType::new(SqlTypeKind::Bool))
        }
        Expr::SubPlan(subplan) => subplan.first_col_type,
        Expr::Like { .. }
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
        Value::Money(_) => Some(SqlType::new(SqlTypeKind::Money)),
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
        Value::Record(_) => Some(SqlType::record(RECORD_TYPE_OID)),
        Value::Array(_) | Value::PgArray(_) | Value::Null => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprArraySubscript {
    pub is_slice: bool,
    pub lower: Option<Expr>,
    pub upper: Option<Expr>,
}
