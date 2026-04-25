use crate::RelFileLocator;
use crate::backend::parser::{
    SqlType, SqlTypeKind, SubqueryComparisonOp, XmlOption, XmlStandalone,
};
use crate::include::access::htup::AttributeDesc;
use crate::include::catalog::{
    builtin_scalar_function_for_proc_oid, builtin_window_function_for_proc_oid,
    proc_oid_for_builtin_scalar_function, proc_oid_for_builtin_window_function,
};
use crate::include::nodes::datum::{MultirangeTypeRef, RangeTypeRef, RecordDescriptor, Value};
use crate::include::nodes::parsenodes::{ColumnGeneratedKind, Query};

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
    Interval,
    BitString,
    Bytea,
    Inet,
    Cidr,
    Point,
    Lseg,
    Path,
    Line,
    Box,
    Polygon,
    Circle,
    Range(RangeTypeRef),
    Multirange(MultirangeTypeRef),
    Float32,
    Float64,
    Numeric,
    Json,
    Jsonb,
    JsonPath,
    Xml,
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
    pub collation_oid: u32,
    pub not_null_constraint_oid: Option<u32>,
    pub not_null_constraint_name: Option<String>,
    pub not_null_constraint_validated: bool,
    pub not_null_constraint_is_local: bool,
    pub not_null_constraint_inhcount: i16,
    pub not_null_constraint_no_inherit: bool,
    pub not_null_primary_key_owned: bool,
    pub attrdef_oid: Option<u32>,
    pub default_expr: Option<String>,
    pub default_sequence_oid: Option<u32>,
    pub generated: Option<ColumnGeneratedKind>,
    pub identity: Option<crate::include::nodes::parsenodes::ColumnIdentityKind>,
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
    pub wire_type_oid: Option<u32>,
}

impl QueryColumn {
    pub fn text(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql_type: SqlType::new(SqlTypeKind::Text),
            wire_type_oid: None,
        }
    }

    pub fn with_wire_type_oid(mut self, wire_type_oid: Option<u32>) -> Self {
        self.wire_type_oid = wire_type_oid;
        self
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
    pub collation_oid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortGroupClause {
    pub expr: Expr,
    pub tle_sort_group_ref: usize,
    pub descending: bool,
    pub nulls_first: Option<bool>,
    pub collation_oid: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToastRelationRef {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    AnyValue,
    Sum,
    Avg,
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
    RegrCount,
    RegrSxx,
    RegrSyy,
    RegrSxy,
    RegrAvgX,
    RegrAvgY,
    RegrR2,
    RegrSlope,
    RegrIntercept,
    CovarPop,
    CovarSamp,
    Corr,
    BoolAnd,
    BoolOr,
    BitAnd,
    BitOr,
    BitXor,
    Min,
    Max,
    StringAgg,
    ArrayAgg,
    JsonAgg,
    JsonbAgg,
    JsonObjectAgg,
    JsonbObjectAgg,
    RangeAgg,
    XmlAgg,
    RangeIntersectAgg,
}

impl AggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::AnyValue => "any_value",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::VarPop => "var_pop",
            AggFunc::VarSamp => "var_samp",
            AggFunc::StddevPop => "stddev_pop",
            AggFunc::StddevSamp => "stddev_samp",
            AggFunc::RegrCount => "regr_count",
            AggFunc::RegrSxx => "regr_sxx",
            AggFunc::RegrSyy => "regr_syy",
            AggFunc::RegrSxy => "regr_sxy",
            AggFunc::RegrAvgX => "regr_avgx",
            AggFunc::RegrAvgY => "regr_avgy",
            AggFunc::RegrR2 => "regr_r2",
            AggFunc::RegrSlope => "regr_slope",
            AggFunc::RegrIntercept => "regr_intercept",
            AggFunc::CovarPop => "covar_pop",
            AggFunc::CovarSamp => "covar_samp",
            AggFunc::Corr => "corr",
            AggFunc::BoolAnd => "bool_and",
            AggFunc::BoolOr => "bool_or",
            AggFunc::BitAnd => "bit_and",
            AggFunc::BitOr => "bit_or",
            AggFunc::BitXor => "bit_xor",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::StringAgg => "string_agg",
            AggFunc::ArrayAgg => "array_agg",
            AggFunc::JsonAgg => "json_agg",
            AggFunc::JsonbAgg => "jsonb_agg",
            AggFunc::JsonObjectAgg => "json_object_agg",
            AggFunc::JsonbObjectAgg => "jsonb_object_agg",
            AggFunc::RangeAgg => "range_agg",
            AggFunc::XmlAgg => "xmlagg",
            AggFunc::RangeIntersectAgg => "range_intersect_agg",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HypotheticalAggFunc {
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
}

impl HypotheticalAggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            HypotheticalAggFunc::Rank => "rank",
            HypotheticalAggFunc::DenseRank => "dense_rank",
            HypotheticalAggFunc::PercentRank => "percent_rank",
            HypotheticalAggFunc::CumeDist => "cume_dist",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinWindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
}

impl BuiltinWindowFunction {
    pub fn name(&self) -> &'static str {
        match self {
            BuiltinWindowFunction::RowNumber => "row_number",
            BuiltinWindowFunction::Rank => "rank",
            BuiltinWindowFunction::DenseRank => "dense_rank",
            BuiltinWindowFunction::PercentRank => "percent_rank",
            BuiltinWindowFunction::CumeDist => "cume_dist",
            BuiltinWindowFunction::Ntile => "ntile",
            BuiltinWindowFunction::Lag => "lag",
            BuiltinWindowFunction::Lead => "lead",
            BuiltinWindowFunction::FirstValue => "first_value",
            BuiltinWindowFunction::LastValue => "last_value",
            BuiltinWindowFunction::NthValue => "nth_value",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinScalarFunction {
    Random,
    RandomNormal,
    CurrentDatabase,
    Version,
    PgBackendPid,
    PgPartitionRoot,
    GetDatabaseEncoding,
    PgMyTempSchema,
    PgRustInternalBinaryCoercible,
    PgRustTestOpclassOptionsFunc,
    PgRustTestFdwHandler,
    PgRustTestEncSetup,
    PgRustTestEncConversion,
    CurrentSetting,
    PgNotify,
    PgNotificationQueueUsage,
    PgTypeof,
    PgColumnCompression,
    PgColumnSize,
    PgStatGetCheckpointerNumTimed,
    PgStatGetCheckpointerNumRequested,
    PgStatGetCheckpointerNumPerformed,
    PgStatGetCheckpointerBuffersWritten,
    PgStatGetCheckpointerSlruWritten,
    PgStatGetCheckpointerWriteTime,
    PgStatGetCheckpointerSyncTime,
    PgStatGetCheckpointerStatResetTime,
    PgStatForceNextFlush,
    PgStatGetSnapshotTimestamp,
    PgStatClearSnapshot,
    PgStatHaveStats,
    PgStatGetNumscans,
    PgStatGetLastscan,
    PgStatGetTuplesReturned,
    PgStatGetTuplesFetched,
    PgStatGetTuplesInserted,
    PgStatGetTuplesUpdated,
    PgStatGetTuplesDeleted,
    PgStatGetLiveTuples,
    PgStatGetDeadTuples,
    PgStatGetBlocksFetched,
    PgStatGetBlocksHit,
    PgStatGetXactNumscans,
    PgStatGetXactTuplesReturned,
    PgStatGetXactTuplesFetched,
    PgStatGetXactTuplesInserted,
    PgStatGetXactTuplesUpdated,
    PgStatGetXactTuplesDeleted,
    PgStatGetFunctionCalls,
    PgStatGetFunctionTotalTime,
    PgStatGetFunctionSelfTime,
    PgStatGetXactFunctionCalls,
    PgStatGetXactFunctionTotalTime,
    PgStatGetXactFunctionSelfTime,
    TextToRegClass,
    RegClassToText,
    RegTypeToText,
    RegRoleToText,
    CashLarger,
    CashSmaller,
    CashWords,
    XmlComment,
    XmlIsWellFormed,
    XmlIsWellFormedDocument,
    XmlIsWellFormedContent,
    ToJson,
    ToJsonb,
    ArrayToJson,
    RowToJson,
    JsonBuildArray,
    JsonBuildObject,
    JsonObject,
    JsonPopulateRecord,
    JsonPopulateRecordValid,
    JsonToRecord,
    JsonStripNulls,
    JsonTypeof,
    JsonArrayLength,
    JsonExtractPath,
    JsonExtractPathText,
    JsonbObject,
    JsonbPopulateRecord,
    JsonbPopulateRecordValid,
    JsonbToRecord,
    JsonbStripNulls,
    JsonbPretty,
    JsonbTypeof,
    JsonbArrayLength,
    JsonbExtractPath,
    JsonbExtractPathText,
    JsonbBuildArray,
    JsonbBuildObject,
    JsonbContains,
    JsonbContained,
    JsonbDelete,
    JsonbDeletePath,
    JsonbExists,
    JsonbExistsAny,
    JsonbExistsAll,
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
    ArrayUpper,
    ArrayFill,
    StringToArray,
    ArrayToString,
    ArrayLength,
    Cardinality,
    ArrayAppend,
    ArrayPrepend,
    ArrayCat,
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
    RegProcedureToText,
    BpcharToText,
    Position,
    Substring,
    Overlay,
    ToBin,
    ToOct,
    ToHex,
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
    NextVal,
    CurrVal,
    SetVal,
    PgGetSerialSequence,
    PgGetAcl,
    PgGetUserById,
    ObjDescription,
    PgDescribeObject,
    PgGetExpr,
    PgGetConstraintDef,
    PgGetIndexDef,
    PgGetViewDef,
    PgGetTriggerDef,
    PgTriggerDepth,
    PgGetStatisticsObjDef,
    PgGetStatisticsObjDefColumns,
    PgGetStatisticsObjDefExpressions,
    PgStatisticsObjIsVisible,
    PgRelationIsPublishable,
    PgIndexAmHasProperty,
    PgIndexHasProperty,
    PgIndexColumnHasProperty,
    PgSizePretty,
    PgSizeBytes,
    PgAdvisoryLock,
    PgAdvisoryXactLock,
    PgAdvisoryLockShared,
    PgAdvisoryXactLockShared,
    PgTryAdvisoryLock,
    PgTryAdvisoryXactLock,
    PgTryAdvisoryLockShared,
    PgTryAdvisoryXactLockShared,
    PgAdvisoryUnlock,
    PgAdvisoryUnlockShared,
    PgAdvisoryUnlockAll,
    LoCreate,
    LoUnlink,
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
    Int4Pl,
    Int8Inc,
    Int8IncAny,
    Int4AvgAccum,
    Int8Avg,
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
    Float8Accum,
    Float8Combine,
    Float8RegrAccum,
    Float8RegrCombine,
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
    RangeConstructor,
    RangeIsEmpty,
    RangeLower,
    RangeUpper,
    RangeLowerInc,
    RangeUpperInc,
    RangeLowerInf,
    RangeUpperInf,
    RangeContains,
    RangeContainedBy,
    RangeOverlap,
    RangeStrictLeft,
    RangeStrictRight,
    RangeOverLeft,
    RangeOverRight,
    RangeAdjacent,
    RangeUnion,
    RangeIntersect,
    RangeDifference,
    RangeMerge,
    BoolEq,
    BoolNe,
    BoolAndStateFunc,
    BoolOrStateFunc,
    TsMatch,
    ToTsVector,
    JsonbToTsVector,
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
pub enum JsonRecordFunction {
    PopulateRecord,
    PopulateRecordSet,
    ToRecord,
    ToRecordSet,
    JsonbPopulateRecord,
    JsonbPopulateRecordSet,
    JsonbToRecord,
    JsonbToRecordSet,
}

impl JsonRecordFunction {
    pub fn name(&self) -> &'static str {
        match self {
            JsonRecordFunction::PopulateRecord => "json_populate_record",
            JsonRecordFunction::PopulateRecordSet => "json_populate_recordset",
            JsonRecordFunction::ToRecord => "json_to_record",
            JsonRecordFunction::ToRecordSet => "json_to_recordset",
            JsonRecordFunction::JsonbPopulateRecord => "jsonb_populate_record",
            JsonRecordFunction::JsonbPopulateRecordSet => "jsonb_populate_recordset",
            JsonRecordFunction::JsonbToRecord => "jsonb_to_record",
            JsonRecordFunction::JsonbToRecordSet => "jsonb_to_recordset",
        }
    }

    pub fn is_set_returning(&self) -> bool {
        matches!(
            self,
            JsonRecordFunction::PopulateRecordSet
                | JsonRecordFunction::ToRecordSet
                | JsonRecordFunction::JsonbPopulateRecordSet
                | JsonRecordFunction::JsonbToRecordSet
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegexTableFunction {
    Matches,
    SplitToTable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringTableFunction {
    StringToTable,
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
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    Unnest {
        func_oid: u32,
        func_variadic: bool,
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    JsonTableFunction {
        func_oid: u32,
        func_variadic: bool,
        kind: JsonTableFunction,
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    JsonRecordFunction {
        func_oid: u32,
        func_variadic: bool,
        kind: JsonRecordFunction,
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
        record_type: Option<SqlType>,
        with_ordinality: bool,
    },
    RegexTableFunction {
        func_oid: u32,
        func_variadic: bool,
        kind: RegexTableFunction,
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    StringTableFunction {
        func_oid: u32,
        func_variadic: bool,
        kind: StringTableFunction,
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    PartitionTree {
        func_oid: u32,
        func_variadic: bool,
        relid: Expr,
        output_columns: Vec<QueryColumn>,
    },
    PartitionAncestors {
        func_oid: u32,
        func_variadic: bool,
        relid: Expr,
        output_columns: Vec<QueryColumn>,
    },
    PgLockStatus {
        func_oid: u32,
        func_variadic: bool,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    TextSearchTableFunction {
        kind: TextSearchTableFunction,
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    UserDefined {
        proc_oid: u32,
        func_variadic: bool,
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
}

impl SetReturningCall {
    pub fn output_columns(&self) -> &[QueryColumn] {
        match self {
            SetReturningCall::GenerateSeries { output_columns, .. }
            | SetReturningCall::Unnest { output_columns, .. }
            | SetReturningCall::JsonTableFunction { output_columns, .. }
            | SetReturningCall::JsonRecordFunction { output_columns, .. }
            | SetReturningCall::RegexTableFunction { output_columns, .. }
            | SetReturningCall::StringTableFunction { output_columns, .. }
            | SetReturningCall::PartitionTree { output_columns, .. }
            | SetReturningCall::PartitionAncestors { output_columns, .. }
            | SetReturningCall::PgLockStatus { output_columns, .. }
            | SetReturningCall::TextSearchTableFunction { output_columns, .. }
            | SetReturningCall::UserDefined { output_columns, .. } => output_columns,
        }
    }

    pub fn set_output_columns(&mut self, output_columns: Vec<QueryColumn>) {
        match self {
            SetReturningCall::GenerateSeries {
                output_columns: existing,
                ..
            }
            | SetReturningCall::Unnest {
                output_columns: existing,
                ..
            }
            | SetReturningCall::JsonTableFunction {
                output_columns: existing,
                ..
            }
            | SetReturningCall::JsonRecordFunction {
                output_columns: existing,
                ..
            }
            | SetReturningCall::RegexTableFunction {
                output_columns: existing,
                ..
            }
            | SetReturningCall::StringTableFunction {
                output_columns: existing,
                ..
            }
            | SetReturningCall::PartitionTree {
                output_columns: existing,
                ..
            }
            | SetReturningCall::PartitionAncestors {
                output_columns: existing,
                ..
            }
            | SetReturningCall::PgLockStatus {
                output_columns: existing,
                ..
            }
            | SetReturningCall::TextSearchTableFunction {
                output_columns: existing,
                ..
            }
            | SetReturningCall::UserDefined {
                output_columns: existing,
                ..
            } => {
                *existing = output_columns;
            }
        }
    }

    pub fn with_ordinality(&self) -> bool {
        match self {
            SetReturningCall::GenerateSeries {
                with_ordinality, ..
            }
            | SetReturningCall::Unnest {
                with_ordinality, ..
            }
            | SetReturningCall::JsonTableFunction {
                with_ordinality, ..
            }
            | SetReturningCall::JsonRecordFunction {
                with_ordinality, ..
            }
            | SetReturningCall::RegexTableFunction {
                with_ordinality, ..
            }
            | SetReturningCall::StringTableFunction {
                with_ordinality, ..
            }
            | SetReturningCall::PgLockStatus {
                with_ordinality, ..
            }
            | SetReturningCall::TextSearchTableFunction {
                with_ordinality, ..
            }
            | SetReturningCall::UserDefined {
                with_ordinality, ..
            } => *with_ordinality,
            SetReturningCall::PartitionTree { .. }
            | SetReturningCall::PartitionAncestors { .. } => false,
        }
    }

    pub fn map_exprs(self, mut map: impl FnMut(Expr) -> Expr) -> Self {
        match self {
            SetReturningCall::GenerateSeries {
                func_oid,
                func_variadic,
                start,
                stop,
                step,
                output_columns,
                with_ordinality,
            } => SetReturningCall::GenerateSeries {
                func_oid,
                func_variadic,
                start: map(start),
                stop: map(stop),
                step: map(step),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::PartitionTree {
                func_oid,
                func_variadic,
                relid,
                output_columns,
            } => SetReturningCall::PartitionTree {
                func_oid,
                func_variadic,
                relid: map(relid),
                output_columns,
            },
            SetReturningCall::PartitionAncestors {
                func_oid,
                func_variadic,
                relid,
                output_columns,
            } => SetReturningCall::PartitionAncestors {
                func_oid,
                func_variadic,
                relid: map(relid),
                output_columns,
            },
            SetReturningCall::PgLockStatus {
                func_oid,
                func_variadic,
                output_columns,
                with_ordinality,
            } => SetReturningCall::PgLockStatus {
                func_oid,
                func_variadic,
                output_columns,
                with_ordinality,
            },
            SetReturningCall::Unnest {
                func_oid,
                func_variadic,
                args,
                output_columns,
                with_ordinality,
            } => SetReturningCall::Unnest {
                func_oid,
                func_variadic,
                args: args.into_iter().map(map).collect(),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::JsonTableFunction {
                func_oid,
                func_variadic,
                kind,
                args,
                output_columns,
                with_ordinality,
            } => SetReturningCall::JsonTableFunction {
                func_oid,
                func_variadic,
                kind,
                args: args.into_iter().map(map).collect(),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::JsonRecordFunction {
                func_oid,
                func_variadic,
                kind,
                args,
                output_columns,
                record_type,
                with_ordinality,
            } => SetReturningCall::JsonRecordFunction {
                func_oid,
                func_variadic,
                kind,
                args: args.into_iter().map(map).collect(),
                output_columns,
                record_type,
                with_ordinality,
            },
            SetReturningCall::RegexTableFunction {
                func_oid,
                func_variadic,
                kind,
                args,
                output_columns,
                with_ordinality,
            } => SetReturningCall::RegexTableFunction {
                func_oid,
                func_variadic,
                kind,
                args: args.into_iter().map(map).collect(),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::StringTableFunction {
                func_oid,
                func_variadic,
                kind,
                args,
                output_columns,
                with_ordinality,
            } => SetReturningCall::StringTableFunction {
                func_oid,
                func_variadic,
                kind,
                args: args.into_iter().map(map).collect(),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::TextSearchTableFunction {
                kind,
                args,
                output_columns,
                with_ordinality,
            } => SetReturningCall::TextSearchTableFunction {
                kind,
                args: args.into_iter().map(map).collect(),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::UserDefined {
                proc_oid,
                func_variadic,
                args,
                output_columns,
                with_ordinality,
            } => SetReturningCall::UserDefined {
                proc_oid,
                func_variadic,
                args: args.into_iter().map(map).collect(),
                output_columns,
                with_ordinality,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectSetTarget {
    Scalar(TargetEntry),
    Set {
        name: String,
        source_expr: Expr,
        call: SetReturningCall,
        sql_type: SqlType,
        column_index: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggAccum {
    pub aggfnoid: u32,
    pub agg_variadic: bool,
    pub direct_args: Vec<Expr>,
    pub args: Vec<Expr>,
    pub order_by: Vec<OrderByEntry>,
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
    pub direct_args: Vec<Expr>,
    pub args: Vec<Expr>,
    pub aggorder: Vec<OrderByEntry>,
    pub aggfilter: Option<Expr>,
    pub agglevelsup: usize,
    pub aggno: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByEntry>,
    pub frame: WindowFrame,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFuncKind {
    Aggregate(Aggref),
    Builtin(BuiltinWindowFunction),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowFuncExpr {
    pub kind: WindowFuncKind,
    pub winref: usize,
    pub winno: usize,
    pub args: Vec<Expr>,
    pub result_type: SqlType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowClause {
    pub spec: WindowSpec,
    pub functions: Vec<WindowFuncExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    OffsetPreceding(WindowFrameOffset),
    CurrentRow,
    OffsetFollowing(WindowFrameOffset),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowFrameOffset {
    pub expr: Expr,
    pub offset_type: SqlType,
    pub in_range_func: Option<u32>,
}

impl WindowFrameOffset {
    pub fn rows_or_groups(expr: Expr) -> Self {
        let offset_type = expr_sql_type_hint(&expr).unwrap_or(SqlType::new(SqlTypeKind::Int8));
        Self {
            expr,
            offset_type,
            in_range_func: None,
        }
    }

    pub fn with_expr(self, expr: Expr) -> Self {
        Self { expr, ..self }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowFrame {
    pub mode: crate::include::nodes::parsenodes::WindowFrameMode,
    pub start_bound: WindowFrameBound,
    pub end_bound: WindowFrameBound,
}

impl WindowFrame {
    pub fn default_range() -> Self {
        Self {
            mode: crate::include::nodes::parsenodes::WindowFrameMode::Range,
            start_bound: WindowFrameBound::UnboundedPreceding,
            end_bound: WindowFrameBound::CurrentRow,
        }
    }
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
    Semi,
    Anti,
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
    ArrayContains,
    ArrayContained,
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
    pub collation_oid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuncExpr {
    pub funcid: u32,
    pub funcname: Option<String>,
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
    ArraySubLink,
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
    pub collation_oid: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmlExprOp {
    Concat,
    Element,
    Forest,
    Parse,
    Pi,
    Root,
    Serialize,
    IsDocument,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XmlExpr {
    pub op: XmlExprOp,
    pub name: Option<String>,
    pub named_args: Vec<Expr>,
    pub arg_names: Vec<String>,
    pub args: Vec<Expr>,
    pub xml_option: Option<XmlOption>,
    pub indent: Option<bool>,
    pub target_type: Option<SqlType>,
    pub standalone: Option<XmlStandalone>,
}

impl XmlExpr {
    pub fn child_exprs(&self) -> impl Iterator<Item = &Expr> {
        self.named_args.iter().chain(self.args.iter())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetReturningExpr {
    pub name: String,
    pub call: SetReturningCall,
    pub sql_type: SqlType,
    pub column_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Var(Var),
    Param(Param),
    Const(Value),
    Aggref(Box<Aggref>),
    WindowFunc(Box<WindowFuncExpr>),
    Op(Box<OpExpr>),
    Bool(Box<BoolExpr>),
    Case(Box<CaseExpr>),
    CaseTest(Box<CaseTestExpr>),
    Func(Box<FuncExpr>),
    SetReturning(Box<SetReturningExpr>),
    SubLink(Box<SubLink>),
    SubPlan(Box<SubPlan>),
    ScalarArrayOp(Box<ScalarArrayOpExpr>),
    Xml(Box<XmlExpr>),
    Cast(Box<Expr>, SqlType),
    Collate {
        expr: Box<Expr>,
        collation_oid: u32,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        case_insensitive: bool,
        negated: bool,
        collation_oid: Option<u32>,
    },
    Similar {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        negated: bool,
        collation_oid: Option<u32>,
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
        descriptor: RecordDescriptor,
        fields: Vec<(String, Expr)>,
    },
    FieldSelect {
        expr: Box<Expr>,
        field: String,
        field_type: SqlType,
    },
    Coalesce(Box<Expr>, Box<Expr>),
    ArraySubscript {
        array: Box<Expr>,
        subscripts: Vec<ExprArraySubscript>,
    },
    Random,
    CurrentUser,
    SessionUser,
    CurrentRole,
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
        Self::op_with_collation(op, opresulttype, args, None)
    }

    pub fn op_with_collation(
        op: OpExprKind,
        opresulttype: SqlType,
        args: Vec<Expr>,
        collation_oid: Option<u32>,
    ) -> Self {
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op,
            opresulttype,
            args,
            collation_oid,
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
            | OpExprKind::ArrayContains
            | OpExprKind::ArrayContained
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
            funcname: None,
            funcresulttype,
            funcvariadic,
            implementation,
            args,
        }))
    }

    pub fn set_returning(
        name: String,
        call: SetReturningCall,
        sql_type: SqlType,
        column_index: usize,
    ) -> Self {
        Expr::SetReturning(Box::new(SetReturningExpr {
            name,
            call,
            sql_type,
            column_index,
        }))
    }

    pub fn aggref(
        aggfnoid: u32,
        aggtype: SqlType,
        aggvariadic: bool,
        aggdistinct: bool,
        direct_args: Vec<Expr>,
        args: Vec<Expr>,
        aggorder: Vec<OrderByEntry>,
        aggfilter: Option<Expr>,
        aggno: usize,
    ) -> Self {
        Expr::Aggref(Box::new(Aggref {
            aggfnoid,
            aggtype,
            aggvariadic,
            aggdistinct,
            direct_args,
            args,
            aggorder,
            aggfilter,
            agglevelsup: 0,
            aggno,
        }))
    }

    pub fn window_func(
        kind: WindowFuncKind,
        winref: usize,
        winno: usize,
        args: Vec<Expr>,
        result_type: SqlType,
    ) -> Self {
        Expr::WindowFunc(Box::new(WindowFuncExpr {
            kind,
            winref,
            winno,
            args,
            result_type,
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
        funcname: Option<String>,
        funcresulttype: Option<SqlType>,
        funcvariadic: bool,
        args: Vec<Expr>,
    ) -> Self {
        let mut expr = Self::func_with_impl(
            funcid,
            funcresulttype,
            funcvariadic,
            ScalarFunctionImpl::UserDefined { proc_oid: funcid },
            args,
        );
        if let Expr::Func(func) = &mut expr {
            func.funcname = funcname;
        }
        expr
    }

    pub fn builtin_window_func(
        func: BuiltinWindowFunction,
        winref: usize,
        winno: usize,
        args: Vec<Expr>,
        result_type: SqlType,
    ) -> Self {
        let proc_oid = proc_oid_for_builtin_window_function(func).unwrap_or_else(|| {
            panic!(
                "builtin window function {:?} lacks pg_proc OID mapping",
                func
            )
        });
        Self::window_func(
            WindowFuncKind::Builtin(builtin_window_function_for_proc_oid(proc_oid).unwrap_or(func)),
            winref,
            winno,
            args,
            result_type,
        )
    }

    pub fn scalar_array_op(
        op: SubqueryComparisonOp,
        use_or: bool,
        left: Expr,
        right: Expr,
    ) -> Self {
        Self::scalar_array_op_with_collation(op, use_or, left, right, None)
    }

    pub fn scalar_array_op_with_collation(
        op: SubqueryComparisonOp,
        use_or: bool,
        left: Expr,
        right: Expr,
        collation_oid: Option<u32>,
    ) -> Self {
        Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            op,
            use_or,
            left: Box::new(left),
            right: Box::new(right),
            collation_oid,
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

fn array_subscript_result_type(array: &Expr, subscripts: &[ExprArraySubscript]) -> Option<SqlType> {
    let array_type = expr_sql_type_hint(array)?;
    let element_type = match array_type.kind {
        SqlTypeKind::Int2Vector => SqlType::new(SqlTypeKind::Int2),
        SqlTypeKind::OidVector => SqlType::new(SqlTypeKind::Oid),
        _ => array_type.element_type(),
    };
    if subscripts.iter().any(|subscript| subscript.is_slice) {
        Some(SqlType::array_of(element_type))
    } else {
        Some(element_type)
    }
}

pub fn expr_sql_type_hint(expr: &Expr) -> Option<SqlType> {
    match expr {
        Expr::Var(var) => Some(var.vartype),
        Expr::Param(param) => Some(param.paramtype),
        Expr::Const(value) => value_sql_type_hint(value),
        Expr::Aggref(aggref) => Some(aggref.aggtype),
        Expr::WindowFunc(window_func) => Some(window_func.result_type),
        Expr::Cast(_, ty) => Some(*ty),
        Expr::ArrayLiteral { array_type, .. } => Some(*array_type),
        Expr::Row { descriptor, .. } => Some(descriptor.sql_type()),
        Expr::FieldSelect { field_type, .. } => Some(*field_type),
        Expr::CurrentUser | Expr::SessionUser | Expr::CurrentRole => {
            Some(SqlType::new(SqlTypeKind::Name))
        }
        Expr::Xml(xml) => Some(match xml.op {
            XmlExprOp::Serialize => xml.target_type.unwrap_or(SqlType::new(SqlTypeKind::Text)),
            XmlExprOp::IsDocument => SqlType::new(SqlTypeKind::Bool),
            _ => SqlType::new(SqlTypeKind::Xml),
        }),
        Expr::Coalesce(left, right) => {
            expr_sql_type_hint(left).or_else(|| expr_sql_type_hint(right))
        }
        Expr::Case(case_expr) => Some(case_expr.casetype),
        Expr::CaseTest(case_test) => Some(case_test.type_id),
        Expr::Op(op) => Some(op.opresulttype),
        Expr::Func(func) => func.funcresulttype,
        Expr::SetReturning(srf) => Some(srf.sql_type),
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
        Expr::SubLink(sublink) if matches!(sublink.sublink_type, SubLinkType::ArraySubLink) => {
            Some(SqlType::array_of(
                sublink
                    .subselect
                    .target_list
                    .first()
                    .map(|target| target.sql_type)
                    .unwrap_or(SqlType::new(SqlTypeKind::Text)),
            ))
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
        Expr::SubPlan(subplan) if matches!(subplan.sublink_type, SubLinkType::ArraySubLink) => {
            Some(SqlType::array_of(
                subplan
                    .first_col_type
                    .unwrap_or(SqlType::new(SqlTypeKind::Text)),
            ))
        }
        Expr::SubPlan(subplan) => subplan.first_col_type,
        Expr::Collate { expr, .. } => expr_sql_type_hint(expr),
        Expr::ArraySubscript { array, subscripts } => {
            array_subscript_result_type(array, subscripts)
        }
        Expr::Like { .. }
        | Expr::Similar { .. }
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => None,
    }
}

pub fn set_returning_call_exprs(call: &SetReturningCall) -> Vec<&Expr> {
    match call {
        SetReturningCall::GenerateSeries {
            start, stop, step, ..
        } => vec![start, stop, step],
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => vec![relid],
        SetReturningCall::PgLockStatus { .. } => Vec::new(),
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::JsonRecordFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::StringTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => args.iter().collect(),
    }
}

pub fn expr_contains_set_returning(expr: &Expr) -> bool {
    match expr {
        Expr::SetReturning(_) => true,
        Expr::Aggref(aggref) => {
            aggref.direct_args.iter().any(expr_contains_set_returning)
                || aggref.args.iter().any(expr_contains_set_returning)
                || aggref
                    .aggorder
                    .iter()
                    .any(|entry| expr_contains_set_returning(&entry.expr))
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_contains_set_returning)
        }
        Expr::WindowFunc(window_func) => window_func.args.iter().any(expr_contains_set_returning),
        Expr::Op(op) => op.args.iter().any(expr_contains_set_returning),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_set_returning),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|arg| expr_contains_set_returning(arg))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_set_returning(&arm.expr)
                        || expr_contains_set_returning(&arm.result)
                })
                || expr_contains_set_returning(&case_expr.defresult)
        }
        Expr::Func(func) => func.args.iter().any(expr_contains_set_returning),
        Expr::ScalarArrayOp(op) => {
            expr_contains_set_returning(&op.left) || expr_contains_set_returning(&op.right)
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_contains_set_returning),
        Expr::Cast(inner, _) => expr_contains_set_returning(inner),
        Expr::Collate { expr, .. } => expr_contains_set_returning(expr),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_set_returning(expr)
                || expr_contains_set_returning(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|escape| expr_contains_set_returning(escape))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_contains_set_returning(inner),
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            expr_contains_set_returning(left) || expr_contains_set_returning(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_set_returning),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, field)| expr_contains_set_returning(field)),
        Expr::FieldSelect { expr, .. } => expr_contains_set_returning(expr),
        Expr::Coalesce(left, right) => {
            expr_contains_set_returning(left) || expr_contains_set_returning(right)
        }
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_set_returning(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_set_returning)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_set_returning)
                })
        }
        Expr::SubLink(_)
        | Expr::SubPlan(_)
        | Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

pub fn set_returning_call_contains_set_returning(call: &SetReturningCall) -> bool {
    set_returning_call_exprs(call)
        .into_iter()
        .any(expr_contains_set_returning)
}

fn value_sql_type_hint(value: &Value) -> Option<SqlType> {
    value.sql_type_hint()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprArraySubscript {
    pub is_slice: bool,
    pub lower: Option<Expr>,
    pub upper: Option<Expr>,
}
