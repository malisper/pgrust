use crate::RelFileLocator;
use crate::backend::parser::{
    SqlType, SqlTypeKind, SubqueryComparisonOp, XmlOption, XmlRootVersion, XmlStandalone,
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
    Uuid,
    Inet,
    Cidr,
    MacAddr,
    MacAddr8,
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
    PgLsn,
    Text,
    Enum,
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
    pub attacl: Option<Vec<String>>,
    pub attrdef_oid: Option<u32>,
    pub default_expr: Option<String>,
    pub default_sequence_oid: Option<u32>,
    pub generated: Option<ColumnGeneratedKind>,
    pub identity: Option<crate::include::nodes::parsenodes::ColumnIdentityKind>,
    pub missing_default_value: Option<Value>,
    pub fdw_options: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationDesc {
    pub columns: Vec<ColumnDesc>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelationPrivilegeMask {
    pub select: bool,
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
}

impl RelationPrivilegeMask {
    pub fn select() -> Self {
        Self {
            select: true,
            ..Self::default()
        }
    }

    pub fn insert() -> Self {
        Self {
            insert: true,
            ..Self::default()
        }
    }

    pub fn update() -> Self {
        Self {
            update: true,
            ..Self::default()
        }
    }

    pub fn delete() -> Self {
        Self {
            delete: true,
            ..Self::default()
        }
    }

    pub fn merge_actions(insert: bool, update: bool, delete: bool) -> Self {
        Self {
            select: true,
            insert,
            update,
            delete,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationPrivilegeRequirement {
    pub relation_oid: u32,
    pub relation_name: String,
    pub relkind: char,
    pub check_as_user_oid: Option<u32>,
    pub required: RelationPrivilegeMask,
    pub selected_columns: Vec<usize>,
    pub inserted_columns: Vec<usize>,
    pub updated_columns: Vec<usize>,
}

impl RelationPrivilegeRequirement {
    pub fn new(
        relation_oid: u32,
        relation_name: impl Into<String>,
        relkind: char,
        required: RelationPrivilegeMask,
    ) -> Self {
        Self {
            relation_oid,
            relation_name: relation_name.into(),
            relkind,
            check_as_user_oid: None,
            required,
            selected_columns: Vec::new(),
            inserted_columns: Vec::new(),
            updated_columns: Vec::new(),
        }
    }

    pub fn checked_as(mut self, role_oid: Option<u32>) -> Self {
        self.check_as_user_oid = role_oid;
        self
    }
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
    JsonObjectAggUnique,
    JsonObjectAggUniqueStrict,
    JsonbObjectAgg,
    JsonbObjectAggUnique,
    JsonbObjectAggUniqueStrict,
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
            AggFunc::JsonObjectAggUnique => "json_object_agg_unique",
            AggFunc::JsonObjectAggUniqueStrict => "json_object_agg_unique_strict",
            AggFunc::JsonbObjectAgg => "jsonb_object_agg",
            AggFunc::JsonbObjectAggUnique => "jsonb_object_agg_unique",
            AggFunc::JsonbObjectAggUniqueStrict => "jsonb_object_agg_unique_strict",
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
pub enum OrderedSetAggFunc {
    PercentileDisc,
    PercentileDiscMulti,
    PercentileCont,
    PercentileContMulti,
    Mode,
}

impl OrderedSetAggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            OrderedSetAggFunc::PercentileDisc | OrderedSetAggFunc::PercentileDiscMulti => {
                "percentile_disc"
            }
            OrderedSetAggFunc::PercentileCont | OrderedSetAggFunc::PercentileContMulti => {
                "percentile_cont"
            }
            OrderedSetAggFunc::Mode => "mode",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HashFunctionKind {
    Bool,
    Int2,
    Int4,
    Int8,
    Oid,
    InternalChar,
    Name,
    Text,
    Varchar,
    BpChar,
    Float4,
    Float8,
    Numeric,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    TimeTz,
    Bytea,
    OidVector,
    AclItem,
    Inet,
    MacAddr,
    MacAddr8,
    Array,
    Interval,
    Uuid,
    PgLsn,
    Enum,
    Jsonb,
    Range,
    Multirange,
    Record,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinScalarFunction {
    Random,
    RandomNormal,
    SetSeed,
    Pi,
    CurrentDatabase,
    CurrentSchemas,
    Version,
    PgBackendPid,
    PgPartitionRoot,
    SatisfiesHashPartition,
    PgGetPartKeyDef,
    PgTableIsVisible,
    PgTypeIsVisible,
    PgOperatorIsVisible,
    PgOpclassIsVisible,
    PgOpfamilyIsVisible,
    PgConversionIsVisible,
    PgTsParserIsVisible,
    PgTsDictIsVisible,
    PgTsTemplateIsVisible,
    PgTsConfigIsVisible,
    GetDatabaseEncoding,
    UnicodeVersion,
    UnicodeAssigned,
    Normalize,
    IsNormalized,
    PgCharToEncoding,
    PgEncodingToChar,
    PgMyTempSchema,
    PgRustInternalBinaryCoercible,
    PgRustTestOpclassOptionsFunc,
    PgRustTestFdwHandler,
    PgRustTestEncSetup,
    PgRustTestEncConversion,
    PgRustTestWidgetIn,
    PgRustTestWidgetOut,
    PgRustTestInt44In,
    PgRustTestInt44Out,
    PgRustTestPtInWidget,
    PgRustTestAtomicOps,
    PgRustIsCatalogTextUniqueIndexOid,
    CurrentSetting,
    SetConfig,
    PgSettingsGetFlags,
    PgNotify,
    PgNotificationQueueUsage,
    PgTypeof,
    PgBaseType,
    PgColumnCompression,
    PgColumnToastChunkId,
    PgColumnSize,
    PgRelationFilenode,
    PgFilenodeRelation,
    PgRelationSize,
    PgTableSize,
    PgNumaAvailable,
    GinCleanPendingList,
    BrinSummarizeNewValues,
    BrinSummarizeRange,
    BrinDesummarizeRange,
    PgTablespaceLocation,
    NumNulls,
    NumNonNulls,
    PgLogBackendMemoryContexts,
    HasFunctionPrivilege,
    HasTablePrivilege,
    HasSequencePrivilege,
    HasAnyColumnPrivilege,
    HasColumnPrivilege,
    HasLargeObjectPrivilege,
    PgHasRole,
    RowSecurityActive,
    PgCurrentLogfile,
    PgReadFile,
    PgReadBinaryFile,
    PgStatFile,
    PgWalfileName,
    PgWalfileNameOffset,
    PgSplitWalfileName,
    PgControlSystem,
    PgControlCheckpoint,
    PgControlRecovery,
    PgControlInit,
    PgReplicationOriginCreate,
    GistTranslateCmpTypeCommon,
    TestCanonicalizePath,
    TestRelpath,
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
    PgStatReset,
    PgStatResetShared,
    PgStatResetSingleTableCounters,
    PgStatResetSingleFunctionCounters,
    PgStatResetBackendStats,
    PgStatResetSlru,
    PgStatResetReplicationSlot,
    PgStatResetSubscriptionStats,
    PgStatGetBackendPid,
    PgStatGetBackendWal,
    PgStatGetReplicationSlot,
    PgStatGetSubscriptionStats,
    ShobjDescription,
    PgStatGetNumscans,
    PgStatGetLastscan,
    PgStatGetTuplesReturned,
    PgStatGetTuplesFetched,
    PgStatGetTuplesInserted,
    PgStatGetTuplesUpdated,
    PgStatGetTuplesHotUpdated,
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
    PgRestoreRelationStats,
    PgClearRelationStats,
    PgRestoreAttributeStats,
    PgClearAttributeStats,
    TextToRegClass,
    ToRegProc,
    ToRegProcedure,
    ToRegOper,
    ToRegOperator,
    ToRegClass,
    ToRegType,
    ToRegTypeMod,
    ToRegRole,
    ToRegNamespace,
    ToRegCollation,
    FormatType,
    HasForeignDataWrapperPrivilege,
    HasServerPrivilege,
    RegProcToText,
    RegClassToText,
    RegTypeToText,
    RegRoleToText,
    CashLarger,
    CashSmaller,
    CashWords,
    UnsupportedXmlFeature,
    XmlComment,
    XmlText,
    XmlIsWellFormed,
    XmlIsWellFormedDocument,
    XmlIsWellFormedContent,
    XPath,
    XPathExists,
    ToJson,
    ToJsonb,
    SqlJsonConstructor,
    SqlJsonScalar,
    SqlJsonSerialize,
    SqlJsonObject,
    SqlJsonArray,
    SqlJsonIsJson,
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
    JsonbConcat,
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
    JsonExists,
    JsonValue,
    JsonQuery,
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
    Casefold,
    TextCat,
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
    OctetLength,
    BitLength,
    ArrayNdims,
    ArrayDims,
    ArrayLower,
    ArrayUpper,
    ArrayFill,
    StringToArray,
    ArrayToString,
    ArrayLength,
    Cardinality,
    ArrayIn,
    ArrayAppend,
    ArrayPrepend,
    ArrayCat,
    AnyRangeIn,
    ArrayLarger,
    ArrayPosition,
    ArrayPositions,
    ArrayRemove,
    ArrayReplace,
    TrimArray,
    ArrayShuffle,
    ArraySample,
    ArrayReverse,
    ArraySort,
    Lower,
    Upper,
    Unistr,
    Ascii,
    Chr,
    ParseIdent,
    QuoteIdent,
    QuoteLiteral,
    QuoteNullable,
    Replace,
    SplitPart,
    Translate,
    RegOperToText,
    RegOperatorToText,
    RegProcedureToText,
    RegCollationToText,
    BpcharToText,
    Position,
    Substring,
    Overlay,
    ToBin,
    ToOct,
    ToHex,
    Reverse,
    TextStartsWith,
    GetBit,
    SetBit,
    GetByte,
    SetByte,
    BitCount,
    Encode,
    Decode,
    Convert,
    ConvertFrom,
    ConvertTo,
    Md5,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
    Crc32,
    Crc32c,
    ToChar,
    ToDate,
    ToNumber,
    Now,
    TransactionTimestamp,
    StatementTimestamp,
    ClockTimestamp,
    TimeOfDay,
    PgSleep,
    Timezone,
    NextVal,
    IdentityNextVal,
    CurrVal,
    LastVal,
    CurrTid2,
    SetVal,
    PgGetSerialSequence,
    PgSequenceParameters,
    PgSequenceLastValue,
    PgGetSequenceData,
    PgGetAcl,
    MakeAclItem,
    TxidCurrent,
    TxidCurrentIfAssigned,
    TxidCurrentSnapshot,
    TxidSnapshotXmin,
    TxidSnapshotXmax,
    TxidVisibleInSnapshot,
    TxidStatus,
    PgGetUserById,
    ObjDescription,
    PgDescribeObject,
    PgIdentifyObject,
    PgIdentifyObjectAsAddress,
    PgGetObjectAddress,
    PgEventTriggerTableRewriteOid,
    PgEventTriggerTableRewriteReason,
    PgGetFunctionArguments,
    PgGetFunctionIdentityArguments,
    PgGetFunctionArgDefault,
    PgGetFunctionDef,
    PgGetFunctionResult,
    PgGetExpr,
    PgGetConstraintDef,
    PgGetPartitionConstraintDef,
    PgGetIndexDef,
    PgGetRuleDef,
    PgGetViewDef,
    PgGetTriggerDef,
    PgTriggerDepth,
    PgGetStatisticsObjDef,
    PgGetStatisticsObjDefColumns,
    PgGetStatisticsObjDefExpressions,
    PgStatisticsObjIsVisible,
    PgFunctionIsVisible,
    PgRelationIsUpdatable,
    PgColumnIsUpdatable,
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
    LoOpen,
    LoClose,
    LoRead,
    LoWrite,
    LoLseek,
    LoLseek64,
    LoTell,
    LoTell64,
    LoTruncate,
    LoTruncate64,
    LoCreat,
    LoFromBytea,
    LoGet,
    LoPut,
    LoImport,
    LoExport,
    DatePart,
    Extract,
    DateTrunc,
    DateBin,
    DateAdd,
    DateSubtract,
    Age,
    JustifyDays,
    JustifyHours,
    JustifyInterval,
    IsFinite,
    MakeInterval,
    MakeDate,
    MakeTime,
    MakeTimestamp,
    MakeTimestampTz,
    TimestampTzConstructor,
    ToTimestamp,
    IntervalHash,
    HashValue(HashFunctionKind),
    HashValueExtended(HashFunctionKind),
    Abs,
    Log,
    Log10,
    Gcd,
    Lcm,
    Greatest,
    Least,
    Div,
    Mod,
    Scale,
    MinScale,
    TrimScale,
    NumericInc,
    Int4Pl,
    Int4Mi,
    Int4Smaller,
    Int4Sum,
    Int8Inc,
    Int8IncAny,
    Int4AvgAccum,
    Int8Avg,
    Factorial,
    PgLsn,
    Trunc,
    MacAddrEq,
    MacAddrNe,
    MacAddrLt,
    MacAddrLe,
    MacAddrGt,
    MacAddrGe,
    MacAddrCmp,
    MacAddrNot,
    MacAddrAnd,
    MacAddrOr,
    MacAddrTrunc,
    MacAddrToMacAddr8,
    MacAddr8Eq,
    MacAddr8Ne,
    MacAddr8Lt,
    MacAddr8Le,
    MacAddr8Gt,
    MacAddr8Ge,
    MacAddr8Cmp,
    MacAddr8Not,
    MacAddr8And,
    MacAddr8Or,
    MacAddr8Trunc,
    MacAddr8ToMacAddr,
    MacAddr8Set7Bit,
    HashMacAddr,
    HashMacAddrExtended,
    HashMacAddr8,
    HashMacAddr8Extended,
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
    Sin,
    Cos,
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
    GeoBoxHigh,
    GeoBoxLow,
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
    NetworkHost,
    NetworkAbbrev,
    NetworkBroadcast,
    NetworkNetwork,
    NetworkMasklen,
    NetworkFamily,
    NetworkNetmask,
    NetworkHostmask,
    NetworkSetMasklen,
    NetworkSameFamily,
    NetworkMerge,
    NetworkSubnet,
    NetworkSubnetEq,
    NetworkSupernet,
    NetworkSupernetEq,
    NetworkOverlap,
    BoolEq,
    BoolNe,
    UuidIn,
    UuidOut,
    UuidRecv,
    UuidSend,
    UuidEq,
    UuidNe,
    UuidLt,
    UuidLe,
    UuidGt,
    UuidGe,
    UuidCmp,
    Xid8Cmp,
    UuidHash,
    UuidHashExtended,
    GenRandomUuid,
    UuidV7,
    UuidExtractVersion,
    UuidExtractTimestamp,
    BoolAndStateFunc,
    BoolOrStateFunc,
    TsMatch,
    ToTsVector,
    JsonToTsVector,
    JsonbToTsVector,
    ToTsQuery,
    PlainToTsQuery,
    PhraseToTsQuery,
    WebSearchToTsQuery,
    TsLexize,
    TsHeadline,
    TsQueryAnd,
    TsQueryOr,
    TsQueryNot,
    TsQueryPhrase,
    TsQueryContains,
    TsQueryContainedBy,
    TsQueryNumnode,
    TsRewrite,
    TsVectorIn,
    TsVectorOut,
    TsQueryIn,
    TsQueryOut,
    TsVectorConcat,
    TsVectorStrip,
    TsVectorDelete,
    TsVectorToArray,
    ArrayToTsVector,
    TsVectorSetWeight,
    TsVectorFilter,
    TsRank,
    TsRankCd,
    BitcastIntegerToFloat4,
    BitcastBigintToFloat8,
    EnumFirst,
    EnumLast,
    EnumRange,
    PgInputIsValid,
    PgInputErrorMessage,
    PgInputErrorDetail,
    PgInputErrorHint,
    PgInputErrorSqlState,
    PgRustDomainCheckUpperLessThan,
    AmValidate,
    BtEqualImage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonTableFunction {
    ObjectKeys,
    Each,
    EachText,
    ArrayElements,
    ArrayElementsText,
    JsonbPathQuery,
    JsonbPathQueryTz,
    JsonbObjectKeys,
    JsonbEach,
    JsonbEachText,
    JsonbArrayElements,
    JsonbArrayElementsText,
}

// Legacy PostgreSQL JSON SRFs are represented by `JsonTableFunction` above.
// SQL/JSON JSON_TABLE uses the separate planned structures below so the two
// table-function families do not acquire misleading shared semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlJsonTablePassingArg {
    pub name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlJsonQueryFunctionKind {
    Exists,
    Value,
    Query,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlJsonQueryFunction {
    pub kind: SqlJsonQueryFunctionKind,
    pub context: Expr,
    pub path: Expr,
    pub passing: Vec<SqlJsonTablePassingArg>,
    pub result_type: SqlType,
    pub result_format_json: bool,
    pub wrapper: SqlJsonTableWrapper,
    pub quotes: SqlJsonTableQuotes,
    pub on_empty: SqlJsonTableBehavior,
    pub on_error: SqlJsonTableBehavior,
}

impl SqlJsonQueryFunction {
    pub fn child_exprs(&self) -> Vec<&Expr> {
        let mut exprs = Vec::with_capacity(2 + self.passing.len() + 2);
        exprs.push(&self.context);
        exprs.push(&self.path);
        exprs.extend(self.passing.iter().map(|arg| &arg.expr));
        push_sql_json_behavior_expr(&self.on_empty, &mut exprs);
        push_sql_json_behavior_expr(&self.on_error, &mut exprs);
        exprs
    }

    pub fn map_exprs(self, mut map: impl FnMut(Expr) -> Expr) -> Self {
        let SqlJsonQueryFunction {
            kind,
            context,
            path,
            passing,
            result_type,
            result_format_json,
            wrapper,
            quotes,
            on_empty,
            on_error,
        } = self;
        SqlJsonQueryFunction {
            kind,
            context: map(context),
            path: map(path),
            passing: passing
                .into_iter()
                .map(|arg| SqlJsonTablePassingArg {
                    name: arg.name,
                    expr: map(arg.expr),
                })
                .collect(),
            result_type,
            result_format_json,
            wrapper,
            quotes,
            on_empty: on_empty.map_exprs(&mut map),
            on_error: on_error.map_exprs(&mut map),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlJsonTableColumn {
    pub name: String,
    pub sql_type: SqlType,
    pub kind: SqlJsonTableColumnKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlJsonTableColumnKind {
    Ordinality,
    Scalar {
        path: String,
        on_empty: SqlJsonTableBehavior,
        on_error: SqlJsonTableBehavior,
    },
    Formatted {
        path: String,
        format_json: bool,
        wrapper: SqlJsonTableWrapper,
        quotes: SqlJsonTableQuotes,
        on_empty: SqlJsonTableBehavior,
        on_error: SqlJsonTableBehavior,
    },
    Exists {
        path: String,
        on_error: SqlJsonTableBehavior,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlJsonTableBehavior {
    Null,
    Error,
    Empty,
    EmptyArray,
    EmptyObject,
    Default(Expr),
    True,
    False,
    Unknown,
}

impl SqlJsonTableBehavior {
    fn map_exprs(self, map: &mut dyn FnMut(Expr) -> Expr) -> Self {
        match self {
            SqlJsonTableBehavior::Default(expr) => SqlJsonTableBehavior::Default(map(expr)),
            other => other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlJsonTableWrapper {
    Unspecified,
    Without,
    Conditional,
    Unconditional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlJsonTableQuotes {
    Unspecified,
    Keep,
    Omit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlJsonTablePlan {
    PathScan {
        path: String,
        path_name: String,
        column_indexes: Vec<usize>,
        error_on_error: bool,
        child: Option<Box<SqlJsonTablePlan>>,
    },
    SiblingJoin {
        left: Box<SqlJsonTablePlan>,
        right: Box<SqlJsonTablePlan>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlJsonTable {
    pub context: Expr,
    pub root_path: String,
    pub root_path_name: String,
    pub passing: Vec<SqlJsonTablePassingArg>,
    pub columns: Vec<SqlJsonTableColumn>,
    pub plan: SqlJsonTablePlan,
    pub output_columns: Vec<QueryColumn>,
    pub on_error: SqlJsonTableBehavior,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlXmlTable {
    pub namespaces: Vec<SqlXmlTableNamespace>,
    pub row_path: Expr,
    pub document: Expr,
    pub columns: Vec<SqlXmlTableColumn>,
    pub output_columns: Vec<QueryColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlXmlTableNamespace {
    pub name: Option<String>,
    pub uri: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlXmlTableColumn {
    pub name: String,
    pub sql_type: SqlType,
    pub kind: SqlXmlTableColumnKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlXmlTableColumnKind {
    Ordinality,
    Regular {
        path: Option<Expr>,
        default: Option<Expr>,
        not_null: bool,
    },
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
    Stat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetReturningCall {
    RowsFrom {
        items: Vec<RowsFromItem>,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    GenerateSeries {
        func_oid: u32,
        func_variadic: bool,
        start: Expr,
        stop: Expr,
        step: Expr,
        timezone: Option<Expr>,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    GenerateSubscripts {
        func_oid: u32,
        func_variadic: bool,
        array: Expr,
        dimension: Expr,
        reverse: Option<Expr>,
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
    SqlJsonTable(SqlJsonTable),
    SqlXmlTable(SqlXmlTable),
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
        with_ordinality: bool,
    },
    PartitionAncestors {
        func_oid: u32,
        func_variadic: bool,
        relid: Expr,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    PgLockStatus {
        func_oid: u32,
        func_variadic: bool,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    PgStatProgressCopy {
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    PgSequences {
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    InformationSchemaSequences {
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
    TxidSnapshotXip {
        func_oid: u32,
        func_variadic: bool,
        arg: Expr,
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
        function_name: String,
        func_variadic: bool,
        args: Vec<Expr>,
        inlined_expr: Option<Box<Expr>>,
        output_columns: Vec<QueryColumn>,
        with_ordinality: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsFromItem {
    pub source: RowsFromSource,
    pub column_definitions: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowsFromSource {
    Function(SetReturningCall),
    Project {
        output_exprs: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
        display_sql: Option<String>,
    },
}

impl RowsFromItem {
    pub fn output_columns(&self) -> &[QueryColumn] {
        match &self.source {
            RowsFromSource::Function(call) => call.output_columns(),
            RowsFromSource::Project { output_columns, .. } => output_columns,
        }
    }
}

impl SetReturningCall {
    pub fn output_columns(&self) -> &[QueryColumn] {
        match self {
            SetReturningCall::RowsFrom { output_columns, .. }
            | SetReturningCall::GenerateSeries { output_columns, .. }
            | SetReturningCall::GenerateSubscripts { output_columns, .. }
            | SetReturningCall::Unnest { output_columns, .. }
            | SetReturningCall::JsonTableFunction { output_columns, .. }
            | SetReturningCall::SqlJsonTable(SqlJsonTable { output_columns, .. })
            | SetReturningCall::SqlXmlTable(SqlXmlTable { output_columns, .. })
            | SetReturningCall::JsonRecordFunction { output_columns, .. }
            | SetReturningCall::RegexTableFunction { output_columns, .. }
            | SetReturningCall::StringTableFunction { output_columns, .. }
            | SetReturningCall::PartitionTree { output_columns, .. }
            | SetReturningCall::PartitionAncestors { output_columns, .. }
            | SetReturningCall::PgLockStatus { output_columns, .. }
            | SetReturningCall::PgStatProgressCopy { output_columns, .. }
            | SetReturningCall::PgSequences { output_columns, .. }
            | SetReturningCall::InformationSchemaSequences { output_columns, .. }
            | SetReturningCall::TxidSnapshotXip { output_columns, .. }
            | SetReturningCall::TextSearchTableFunction { output_columns, .. }
            | SetReturningCall::UserDefined { output_columns, .. } => output_columns,
        }
    }

    pub fn set_output_columns(&mut self, output_columns: Vec<QueryColumn>) {
        match self {
            SetReturningCall::RowsFrom {
                output_columns: existing,
                ..
            }
            | SetReturningCall::GenerateSeries {
                output_columns: existing,
                ..
            }
            | SetReturningCall::GenerateSubscripts {
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
            | SetReturningCall::SqlJsonTable(SqlJsonTable {
                output_columns: existing,
                ..
            })
            | SetReturningCall::SqlXmlTable(SqlXmlTable {
                output_columns: existing,
                ..
            })
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
            | SetReturningCall::PgStatProgressCopy {
                output_columns: existing,
                ..
            }
            | SetReturningCall::PgSequences {
                output_columns: existing,
                ..
            }
            | SetReturningCall::InformationSchemaSequences {
                output_columns: existing,
                ..
            }
            | SetReturningCall::TxidSnapshotXip {
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
            SetReturningCall::RowsFrom {
                with_ordinality, ..
            }
            | SetReturningCall::GenerateSeries {
                with_ordinality, ..
            }
            | SetReturningCall::GenerateSubscripts {
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
            | SetReturningCall::PartitionTree {
                with_ordinality, ..
            }
            | SetReturningCall::PartitionAncestors {
                with_ordinality, ..
            }
            | SetReturningCall::PgLockStatus {
                with_ordinality, ..
            }
            | SetReturningCall::PgStatProgressCopy {
                with_ordinality, ..
            }
            | SetReturningCall::PgSequences {
                with_ordinality, ..
            }
            | SetReturningCall::InformationSchemaSequences {
                with_ordinality, ..
            }
            | SetReturningCall::TxidSnapshotXip {
                with_ordinality, ..
            }
            | SetReturningCall::TextSearchTableFunction {
                with_ordinality, ..
            }
            | SetReturningCall::UserDefined {
                with_ordinality, ..
            } => *with_ordinality,
            SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_) => false,
        }
    }

    pub fn map_exprs(self, mut map: impl FnMut(Expr) -> Expr) -> Self {
        self.map_exprs_dyn(&mut map)
    }

    fn map_exprs_dyn(self, map: &mut dyn FnMut(Expr) -> Expr) -> Self {
        match self {
            SetReturningCall::RowsFrom {
                items,
                output_columns,
                with_ordinality,
            } => SetReturningCall::RowsFrom {
                items: items
                    .into_iter()
                    .map(|item| RowsFromItem {
                        source: match item.source {
                            RowsFromSource::Function(call) => {
                                RowsFromSource::Function(call.map_exprs_dyn(map))
                            }
                            RowsFromSource::Project {
                                output_exprs,
                                output_columns,
                                display_sql,
                            } => RowsFromSource::Project {
                                output_exprs: output_exprs
                                    .into_iter()
                                    .map(|expr| map(expr))
                                    .collect(),
                                output_columns,
                                display_sql,
                            },
                        },
                        column_definitions: item.column_definitions,
                    })
                    .collect(),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::GenerateSeries {
                func_oid,
                func_variadic,
                start,
                stop,
                step,
                timezone,
                output_columns,
                with_ordinality,
            } => SetReturningCall::GenerateSeries {
                func_oid,
                func_variadic,
                start: map(start),
                stop: map(stop),
                step: map(step),
                timezone: timezone.map(|timezone| map(timezone)),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::GenerateSubscripts {
                func_oid,
                func_variadic,
                array,
                dimension,
                reverse,
                output_columns,
                with_ordinality,
            } => SetReturningCall::GenerateSubscripts {
                func_oid,
                func_variadic,
                array: map(array),
                dimension: map(dimension),
                reverse: reverse.map(|reverse| map(reverse)),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::PartitionTree {
                func_oid,
                func_variadic,
                relid,
                output_columns,
                with_ordinality,
            } => SetReturningCall::PartitionTree {
                func_oid,
                func_variadic,
                relid: map(relid),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::PartitionAncestors {
                func_oid,
                func_variadic,
                relid,
                output_columns,
                with_ordinality,
            } => SetReturningCall::PartitionAncestors {
                func_oid,
                func_variadic,
                relid: map(relid),
                output_columns,
                with_ordinality,
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
            SetReturningCall::PgStatProgressCopy {
                output_columns,
                with_ordinality,
            } => SetReturningCall::PgStatProgressCopy {
                output_columns,
                with_ordinality,
            },
            SetReturningCall::PgSequences {
                output_columns,
                with_ordinality,
            } => SetReturningCall::PgSequences {
                output_columns,
                with_ordinality,
            },
            SetReturningCall::InformationSchemaSequences {
                output_columns,
                with_ordinality,
            } => SetReturningCall::InformationSchemaSequences {
                output_columns,
                with_ordinality,
            },
            SetReturningCall::TxidSnapshotXip {
                func_oid,
                func_variadic,
                arg,
                output_columns,
                with_ordinality,
            } => SetReturningCall::TxidSnapshotXip {
                func_oid,
                func_variadic,
                arg: map(arg),
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
                args: args.into_iter().map(|arg| map(arg)).collect(),
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
                args: args.into_iter().map(|arg| map(arg)).collect(),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::SqlJsonTable(table) => {
                SetReturningCall::SqlJsonTable(map_sql_json_table_exprs(table, map))
            }
            SetReturningCall::SqlXmlTable(table) => {
                SetReturningCall::SqlXmlTable(map_sql_xml_table_exprs(table, map))
            }
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
                args: args.into_iter().map(|arg| map(arg)).collect(),
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
                args: args.into_iter().map(|arg| map(arg)).collect(),
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
                args: args.into_iter().map(|arg| map(arg)).collect(),
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
                args: args.into_iter().map(|arg| map(arg)).collect(),
                output_columns,
                with_ordinality,
            },
            SetReturningCall::UserDefined {
                proc_oid,
                function_name,
                func_variadic,
                args,
                inlined_expr,
                output_columns,
                with_ordinality,
            } => SetReturningCall::UserDefined {
                proc_oid,
                function_name,
                func_variadic,
                args: args.into_iter().map(|arg| map(arg)).collect(),
                inlined_expr: inlined_expr.map(|expr| Box::new(map(*expr))),
                output_columns,
                with_ordinality,
            },
        }
    }

    pub fn try_map_exprs<E>(self, mut map: impl FnMut(Expr) -> Result<Expr, E>) -> Result<Self, E> {
        self.try_map_exprs_dyn(&mut map)
    }

    fn try_map_exprs_dyn<E>(self, map: &mut dyn FnMut(Expr) -> Result<Expr, E>) -> Result<Self, E> {
        Ok(match self {
            SetReturningCall::RowsFrom {
                items,
                output_columns,
                with_ordinality,
            } => SetReturningCall::RowsFrom {
                items: items
                    .into_iter()
                    .map(|item| {
                        Ok(RowsFromItem {
                            source: match item.source {
                                RowsFromSource::Function(call) => {
                                    RowsFromSource::Function(call.try_map_exprs_dyn(map)?)
                                }
                                RowsFromSource::Project {
                                    output_exprs,
                                    output_columns,
                                    display_sql,
                                } => RowsFromSource::Project {
                                    output_exprs: output_exprs
                                        .into_iter()
                                        .map(|expr| map(expr))
                                        .collect::<Result<Vec<_>, E>>()?,
                                    output_columns,
                                    display_sql,
                                },
                            },
                            column_definitions: item.column_definitions,
                        })
                    })
                    .collect::<Result<Vec<_>, E>>()?,
                output_columns,
                with_ordinality,
            },
            SetReturningCall::SqlJsonTable(table) => {
                SetReturningCall::SqlJsonTable(try_map_sql_json_table_exprs(table, map)?)
            }
            SetReturningCall::SqlXmlTable(table) => {
                SetReturningCall::SqlXmlTable(try_map_sql_xml_table_exprs(table, map)?)
            }
            SetReturningCall::GenerateSeries {
                func_oid,
                func_variadic,
                start,
                stop,
                step,
                timezone,
                output_columns,
                with_ordinality,
            } => SetReturningCall::GenerateSeries {
                func_oid,
                func_variadic,
                start: map(start)?,
                stop: map(stop)?,
                step: map(step)?,
                timezone: timezone.map(|timezone| map(timezone)).transpose()?,
                output_columns,
                with_ordinality,
            },
            SetReturningCall::GenerateSubscripts {
                func_oid,
                func_variadic,
                array,
                dimension,
                reverse,
                output_columns,
                with_ordinality,
            } => SetReturningCall::GenerateSubscripts {
                func_oid,
                func_variadic,
                array: map(array)?,
                dimension: map(dimension)?,
                reverse: reverse.map(|reverse| map(reverse)).transpose()?,
                output_columns,
                with_ordinality,
            },
            SetReturningCall::PartitionTree {
                func_oid,
                func_variadic,
                relid,
                output_columns,
                with_ordinality,
            } => SetReturningCall::PartitionTree {
                func_oid,
                func_variadic,
                relid: map(relid)?,
                output_columns,
                with_ordinality,
            },
            SetReturningCall::PartitionAncestors {
                func_oid,
                func_variadic,
                relid,
                output_columns,
                with_ordinality,
            } => SetReturningCall::PartitionAncestors {
                func_oid,
                func_variadic,
                relid: map(relid)?,
                output_columns,
                with_ordinality,
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
            SetReturningCall::PgStatProgressCopy {
                output_columns,
                with_ordinality,
            } => SetReturningCall::PgStatProgressCopy {
                output_columns,
                with_ordinality,
            },
            SetReturningCall::PgSequences {
                output_columns,
                with_ordinality,
            } => SetReturningCall::PgSequences {
                output_columns,
                with_ordinality,
            },
            SetReturningCall::InformationSchemaSequences {
                output_columns,
                with_ordinality,
            } => SetReturningCall::InformationSchemaSequences {
                output_columns,
                with_ordinality,
            },
            SetReturningCall::TxidSnapshotXip {
                func_oid,
                func_variadic,
                arg,
                output_columns,
                with_ordinality,
            } => SetReturningCall::TxidSnapshotXip {
                func_oid,
                func_variadic,
                arg: map(arg)?,
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
                args: args
                    .into_iter()
                    .map(|arg| map(arg))
                    .collect::<Result<Vec<_>, E>>()?,
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
                args: args
                    .into_iter()
                    .map(|arg| map(arg))
                    .collect::<Result<Vec<_>, E>>()?,
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
                args: args
                    .into_iter()
                    .map(|arg| map(arg))
                    .collect::<Result<Vec<_>, E>>()?,
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
                args: args
                    .into_iter()
                    .map(|arg| map(arg))
                    .collect::<Result<Vec<_>, E>>()?,
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
                args: args
                    .into_iter()
                    .map(|arg| map(arg))
                    .collect::<Result<Vec<_>, E>>()?,
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
                args: args
                    .into_iter()
                    .map(|arg| map(arg))
                    .collect::<Result<Vec<_>, E>>()?,
                output_columns,
                with_ordinality,
            },
            SetReturningCall::UserDefined {
                proc_oid,
                function_name,
                func_variadic,
                args,
                inlined_expr,
                output_columns,
                with_ordinality,
            } => SetReturningCall::UserDefined {
                proc_oid,
                function_name,
                func_variadic,
                args: args
                    .into_iter()
                    .map(|arg| map(arg))
                    .collect::<Result<Vec<_>, E>>()?,
                inlined_expr: inlined_expr
                    .map(|expr| map(*expr).map(Box::new))
                    .transpose()?,
                output_columns,
                with_ordinality,
            },
        })
    }
}

fn try_map_sql_json_table_exprs<E>(
    table: SqlJsonTable,
    map: &mut dyn FnMut(Expr) -> Result<Expr, E>,
) -> Result<SqlJsonTable, E> {
    Ok(SqlJsonTable {
        context: map(table.context)?,
        passing: table
            .passing
            .into_iter()
            .map(|arg| {
                Ok(SqlJsonTablePassingArg {
                    name: arg.name,
                    expr: map(arg.expr)?,
                })
            })
            .collect::<Result<Vec<_>, E>>()?,
        columns: table
            .columns
            .into_iter()
            .map(|column| {
                Ok(SqlJsonTableColumn {
                    name: column.name,
                    sql_type: column.sql_type,
                    kind: try_map_sql_json_table_column_kind(column.kind, map)?,
                })
            })
            .collect::<Result<Vec<_>, E>>()?,
        on_error: try_map_sql_json_behavior(table.on_error, map)?,
        root_path: table.root_path,
        root_path_name: table.root_path_name,
        plan: table.plan,
        output_columns: table.output_columns,
    })
}

fn try_map_sql_json_table_column_kind<E>(
    kind: SqlJsonTableColumnKind,
    map: &mut dyn FnMut(Expr) -> Result<Expr, E>,
) -> Result<SqlJsonTableColumnKind, E> {
    Ok(match kind {
        SqlJsonTableColumnKind::Ordinality => SqlJsonTableColumnKind::Ordinality,
        SqlJsonTableColumnKind::Scalar {
            path,
            on_empty,
            on_error,
        } => SqlJsonTableColumnKind::Scalar {
            path,
            on_empty: try_map_sql_json_behavior(on_empty, map)?,
            on_error: try_map_sql_json_behavior(on_error, map)?,
        },
        SqlJsonTableColumnKind::Formatted {
            path,
            format_json,
            wrapper,
            quotes,
            on_empty,
            on_error,
        } => SqlJsonTableColumnKind::Formatted {
            path,
            format_json,
            wrapper,
            quotes,
            on_empty: try_map_sql_json_behavior(on_empty, map)?,
            on_error: try_map_sql_json_behavior(on_error, map)?,
        },
        SqlJsonTableColumnKind::Exists { path, on_error } => SqlJsonTableColumnKind::Exists {
            path,
            on_error: try_map_sql_json_behavior(on_error, map)?,
        },
    })
}

fn try_map_sql_json_behavior<E>(
    behavior: SqlJsonTableBehavior,
    map: &mut dyn FnMut(Expr) -> Result<Expr, E>,
) -> Result<SqlJsonTableBehavior, E> {
    Ok(match behavior {
        SqlJsonTableBehavior::Default(expr) => SqlJsonTableBehavior::Default(map(expr)?),
        other => other,
    })
}

fn map_sql_json_table_exprs(
    table: SqlJsonTable,
    map: &mut dyn FnMut(Expr) -> Expr,
) -> SqlJsonTable {
    SqlJsonTable {
        context: map(table.context),
        passing: table
            .passing
            .into_iter()
            .map(|arg| SqlJsonTablePassingArg {
                name: arg.name,
                expr: map(arg.expr),
            })
            .collect(),
        columns: table
            .columns
            .into_iter()
            .map(|column| SqlJsonTableColumn {
                name: column.name,
                sql_type: column.sql_type,
                kind: map_sql_json_table_column_kind(column.kind, map),
            })
            .collect(),
        on_error: table.on_error.map_exprs(map),
        root_path: table.root_path,
        root_path_name: table.root_path_name,
        plan: table.plan,
        output_columns: table.output_columns,
    }
}

fn map_sql_json_table_column_kind(
    kind: SqlJsonTableColumnKind,
    map: &mut dyn FnMut(Expr) -> Expr,
) -> SqlJsonTableColumnKind {
    match kind {
        SqlJsonTableColumnKind::Ordinality => SqlJsonTableColumnKind::Ordinality,
        SqlJsonTableColumnKind::Scalar {
            path,
            on_empty,
            on_error,
        } => SqlJsonTableColumnKind::Scalar {
            path,
            on_empty: on_empty.map_exprs(map),
            on_error: on_error.map_exprs(map),
        },
        SqlJsonTableColumnKind::Formatted {
            path,
            format_json,
            wrapper,
            quotes,
            on_empty,
            on_error,
        } => SqlJsonTableColumnKind::Formatted {
            path,
            format_json,
            wrapper,
            quotes,
            on_empty: on_empty.map_exprs(map),
            on_error: on_error.map_exprs(map),
        },
        SqlJsonTableColumnKind::Exists { path, on_error } => SqlJsonTableColumnKind::Exists {
            path,
            on_error: on_error.map_exprs(map),
        },
    }
}

fn try_map_sql_xml_table_exprs<E>(
    table: SqlXmlTable,
    map: &mut dyn FnMut(Expr) -> Result<Expr, E>,
) -> Result<SqlXmlTable, E> {
    Ok(SqlXmlTable {
        namespaces: table
            .namespaces
            .into_iter()
            .map(|namespace| {
                Ok(SqlXmlTableNamespace {
                    name: namespace.name,
                    uri: map(namespace.uri)?,
                })
            })
            .collect::<Result<Vec<_>, E>>()?,
        row_path: map(table.row_path)?,
        document: map(table.document)?,
        columns: table
            .columns
            .into_iter()
            .map(|column| {
                Ok(SqlXmlTableColumn {
                    name: column.name,
                    sql_type: column.sql_type,
                    kind: try_map_sql_xml_table_column_kind(column.kind, map)?,
                })
            })
            .collect::<Result<Vec<_>, E>>()?,
        output_columns: table.output_columns,
    })
}

fn try_map_sql_xml_table_column_kind<E>(
    kind: SqlXmlTableColumnKind,
    map: &mut dyn FnMut(Expr) -> Result<Expr, E>,
) -> Result<SqlXmlTableColumnKind, E> {
    Ok(match kind {
        SqlXmlTableColumnKind::Ordinality => SqlXmlTableColumnKind::Ordinality,
        SqlXmlTableColumnKind::Regular {
            path,
            default,
            not_null,
        } => SqlXmlTableColumnKind::Regular {
            path: path.map(&mut *map).transpose()?,
            default: default.map(&mut *map).transpose()?,
            not_null,
        },
    })
}

fn map_sql_xml_table_exprs(table: SqlXmlTable, map: &mut dyn FnMut(Expr) -> Expr) -> SqlXmlTable {
    SqlXmlTable {
        namespaces: table
            .namespaces
            .into_iter()
            .map(|namespace| SqlXmlTableNamespace {
                name: namespace.name,
                uri: map(namespace.uri),
            })
            .collect(),
        row_path: map(table.row_path),
        document: map(table.document),
        columns: table
            .columns
            .into_iter()
            .map(|column| SqlXmlTableColumn {
                name: column.name,
                sql_type: column.sql_type,
                kind: map_sql_xml_table_column_kind(column.kind, map),
            })
            .collect(),
        output_columns: table.output_columns,
    }
}

fn map_sql_xml_table_column_kind(
    kind: SqlXmlTableColumnKind,
    map: &mut dyn FnMut(Expr) -> Expr,
) -> SqlXmlTableColumnKind {
    match kind {
        SqlXmlTableColumnKind::Ordinality => SqlXmlTableColumnKind::Ordinality,
        SqlXmlTableColumnKind::Regular {
            path,
            default,
            not_null,
        } => SqlXmlTableColumnKind::Regular {
            path: path.map(&mut *map),
            default: default.map(&mut *map),
            not_null,
        },
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
        ressortgroupref: usize,
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
pub struct GroupingKeyExpr {
    pub expr: Box<Expr>,
    pub ref_id: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingFuncExpr {
    pub args: Vec<Expr>,
    pub refs: Vec<usize>,
    pub agglevelsup: usize,
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
    pub ignore_nulls: bool,
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
    pub exclusion: crate::include::nodes::parsenodes::WindowFrameExclusion,
}

impl WindowFrame {
    pub fn default_range() -> Self {
        Self {
            mode: crate::include::nodes::parsenodes::WindowFrameMode::Range,
            start_bound: WindowFrameBound::UnboundedPreceding,
            end_bound: WindowFrameBound::CurrentRow,
            exclusion: crate::include::nodes::parsenodes::WindowFrameExclusion::NoOthers,
        }
    }
}

pub type AttrNumber = i32;

pub const SELF_ITEM_POINTER_ATTR_NO: AttrNumber = -1;
pub const XMIN_ATTR_NO: AttrNumber = -2;
pub const XMAX_ATTR_NO: AttrNumber = -3;
pub const TABLE_OID_ATTR_NO: AttrNumber = -6;
pub const OUTER_VAR: usize = usize::MAX;
pub const INNER_VAR: usize = usize::MAX - 1;
pub const INDEX_VAR: usize = usize::MAX - 2;
pub const ROWID_VAR: usize = usize::MAX - 3;
pub const RULE_OLD_VAR: usize = usize::MAX - 4;
pub const RULE_NEW_VAR: usize = usize::MAX - 5;

pub const fn is_special_varno(varno: usize) -> bool {
    varno >= RULE_NEW_VAR
}

pub const fn is_executor_special_varno(varno: usize) -> bool {
    varno >= ROWID_VAR
}

pub const fn is_rule_pseudo_varno(varno: usize) -> bool {
    varno == RULE_OLD_VAR || varno == RULE_NEW_VAR
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamKind {
    Exec,
    External,
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
    pub collation_oid: Option<u32>,
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
    pub collation_oid: Option<u32>,
    pub display_args: Option<Vec<FuncCallDisplayArg>>,
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuncCallDisplayArg {
    pub name: Option<String>,
    pub expr: Expr,
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
    RowCompareSubLink(SubqueryComparisonOp),
    ExprSubLink,
    ArraySubLink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubqueryComparison {
    pub opno: u32,
    pub opfuncid: u32,
    pub op: OpExprKind,
    pub left_type: SqlType,
    pub right_type: SqlType,
    pub collation_oid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubLink {
    pub sublink_type: SubLinkType,
    pub testexpr: Option<Box<Expr>>,
    pub comparison: Option<SubqueryComparison>,
    pub subselect: Box<Query>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubPlan {
    pub sublink_type: SubLinkType,
    pub testexpr: Option<Box<Expr>>,
    pub comparison: Option<SubqueryComparison>,
    pub first_col_type: Option<SqlType>,
    pub target_width: usize,
    pub target_attnos: Vec<Option<usize>>,
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
    pub root_version: XmlRootVersion,
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
    GroupingKey(Box<GroupingKeyExpr>),
    GroupingFunc(Box<GroupingFuncExpr>),
    WindowFunc(Box<WindowFuncExpr>),
    Op(Box<OpExpr>),
    Bool(Box<BoolExpr>),
    Case(Box<CaseExpr>),
    CaseTest(Box<CaseTestExpr>),
    Func(Box<FuncExpr>),
    SqlJsonQueryFunction(Box<SqlJsonQueryFunction>),
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
    User,
    SessionUser,
    SystemUser,
    CurrentRole,
    CurrentCatalog,
    CurrentSchema,
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
        let collation_oid = scalar_function_collation_oid(implementation, &args);
        Expr::Func(Box::new(FuncExpr {
            funcid,
            funcname: None,
            funcresulttype,
            funcvariadic,
            implementation,
            collation_oid,
            display_args: None,
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
        ignore_nulls: bool,
    ) -> Self {
        Expr::WindowFunc(Box::new(WindowFuncExpr {
            kind,
            winref,
            winno,
            args,
            result_type,
            ignore_nulls,
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
        ignore_nulls: bool,
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
            ignore_nulls,
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
        Expr::GroupingKey(grouping_key) => expr_sql_type_hint(&grouping_key.expr),
        Expr::GroupingFunc(_) => Some(SqlType::new(SqlTypeKind::Int4)),
        Expr::WindowFunc(window_func) => Some(window_func.result_type),
        Expr::Cast(_, ty) => Some(*ty),
        Expr::ArrayLiteral { array_type, .. } => Some(*array_type),
        Expr::Row { descriptor, .. } => Some(descriptor.sql_type()),
        Expr::FieldSelect { field_type, .. } => Some(*field_type),
        Expr::CurrentUser
        | Expr::User
        | Expr::SessionUser
        | Expr::SystemUser
        | Expr::CurrentRole => Some(SqlType::new(SqlTypeKind::Name)),
        Expr::CurrentCatalog | Expr::CurrentSchema => Some(SqlType::new(SqlTypeKind::Text)),
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
        Expr::SqlJsonQueryFunction(func) => Some(func.result_type),
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
                    | SubLinkType::RowCompareSubLink(_)
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
                    | SubLinkType::RowCompareSubLink(_)
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

pub fn expr_collation_oid_hint(expr: &Expr) -> Option<u32> {
    match expr {
        Expr::Var(var) => var.collation_oid,
        Expr::Collate { collation_oid, .. } => Some(*collation_oid),
        Expr::Cast(inner, _) => expr_collation_oid_hint(inner),
        Expr::Func(func) => func.collation_oid,
        Expr::Op(op) => op.collation_oid,
        Expr::Coalesce(left, right) => {
            expr_collation_oid_hint(left).or_else(|| expr_collation_oid_hint(right))
        }
        Expr::Case(case_expr) => case_expr
            .arg
            .as_deref()
            .and_then(expr_collation_oid_hint)
            .or_else(|| {
                case_expr
                    .args
                    .iter()
                    .find_map(|when| expr_collation_oid_hint(&when.result))
            })
            .or_else(|| expr_collation_oid_hint(&case_expr.defresult)),
        Expr::SubLink(sublink) => sublink
            .subselect
            .target_list
            .first()
            .and_then(|target| expr_collation_oid_hint(&target.expr)),
        _ => None,
    }
}

fn scalar_function_collation_oid(implementation: ScalarFunctionImpl, args: &[Expr]) -> Option<u32> {
    let ScalarFunctionImpl::Builtin(func) = implementation else {
        return None;
    };
    if !matches!(
        func,
        BuiltinScalarFunction::Lower
            | BuiltinScalarFunction::Upper
            | BuiltinScalarFunction::Initcap
            | BuiltinScalarFunction::Casefold
            | BuiltinScalarFunction::RegexpLike
    ) {
        return None;
    }
    args.iter().find_map(expr_collation_oid_hint).or_else(|| {
        args.iter()
            .any(|arg| {
                expr_sql_type_hint(arg).is_some_and(|ty| {
                    matches!(
                        ty.element_type().kind,
                        SqlTypeKind::Text | SqlTypeKind::Varchar | SqlTypeKind::Char
                    )
                })
            })
            .then_some(crate::include::catalog::DEFAULT_COLLATION_OID)
    })
}

pub fn set_returning_call_exprs(call: &SetReturningCall) -> Vec<&Expr> {
    match call {
        SetReturningCall::RowsFrom { items, .. } => items
            .iter()
            .flat_map(|item| match &item.source {
                RowsFromSource::Function(call) => set_returning_call_exprs(call),
                RowsFromSource::Project { output_exprs, .. } => output_exprs.iter().collect(),
            })
            .collect(),
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            ..
        } => {
            let mut exprs = vec![start, stop, step];
            if let Some(timezone) = timezone {
                exprs.push(timezone);
            }
            exprs
        }
        SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            ..
        } => {
            let mut exprs = vec![array, dimension];
            if let Some(reverse) = reverse {
                exprs.push(reverse);
            }
            exprs
        }
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => vec![relid],
        SetReturningCall::PgLockStatus { .. }
        | SetReturningCall::PgStatProgressCopy { .. }
        | SetReturningCall::PgSequences { .. }
        | SetReturningCall::InformationSchemaSequences { .. } => Vec::new(),
        SetReturningCall::TxidSnapshotXip { arg, .. } => vec![arg],
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::JsonRecordFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::StringTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. } => args.iter().collect(),
        SetReturningCall::UserDefined {
            args, inlined_expr, ..
        } => {
            let mut exprs = args.iter().collect::<Vec<_>>();
            if let Some(inlined_expr) = inlined_expr.as_deref() {
                exprs.push(inlined_expr);
            }
            exprs
        }
        SetReturningCall::SqlJsonTable(table) => {
            let mut exprs = Vec::with_capacity(1 + table.passing.len());
            exprs.push(&table.context);
            exprs.extend(table.passing.iter().map(|arg| &arg.expr));
            for column in &table.columns {
                match &column.kind {
                    SqlJsonTableColumnKind::Scalar {
                        on_empty, on_error, ..
                    }
                    | SqlJsonTableColumnKind::Formatted {
                        on_empty, on_error, ..
                    } => {
                        push_sql_json_behavior_expr(on_empty, &mut exprs);
                        push_sql_json_behavior_expr(on_error, &mut exprs);
                    }
                    SqlJsonTableColumnKind::Exists { on_error, .. } => {
                        push_sql_json_behavior_expr(on_error, &mut exprs);
                    }
                    SqlJsonTableColumnKind::Ordinality => {}
                }
            }
            push_sql_json_behavior_expr(&table.on_error, &mut exprs);
            exprs
        }
        SetReturningCall::SqlXmlTable(table) => {
            let mut exprs = Vec::with_capacity(2 + table.namespaces.len() + table.columns.len());
            exprs.push(&table.row_path);
            exprs.push(&table.document);
            exprs.extend(table.namespaces.iter().map(|namespace| &namespace.uri));
            for column in &table.columns {
                if let SqlXmlTableColumnKind::Regular { path, default, .. } = &column.kind {
                    if let Some(path) = path {
                        exprs.push(path);
                    }
                    if let Some(default) = default {
                        exprs.push(default);
                    }
                }
            }
            exprs
        }
    }
}

fn push_sql_json_behavior_expr<'a>(behavior: &'a SqlJsonTableBehavior, exprs: &mut Vec<&'a Expr>) {
    if let SqlJsonTableBehavior::Default(expr) = behavior {
        exprs.push(expr);
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
        Expr::GroupingKey(grouping_key) => expr_contains_set_returning(&grouping_key.expr),
        Expr::GroupingFunc(grouping_func) => {
            grouping_func.args.iter().any(expr_contains_set_returning)
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
        Expr::SqlJsonQueryFunction(func) => {
            expr_contains_set_returning(&func.context)
                || expr_contains_set_returning(&func.path)
                || func
                    .passing
                    .iter()
                    .any(|arg| expr_contains_set_returning(&arg.expr))
                || sql_json_behavior_contains_set_returning(&func.on_empty)
                || sql_json_behavior_contains_set_returning(&func.on_error)
        }
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
        | Expr::User
        | Expr::SessionUser
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn sql_json_behavior_contains_set_returning(behavior: &SqlJsonTableBehavior) -> bool {
    match behavior {
        SqlJsonTableBehavior::Default(expr) => expr_contains_set_returning(expr),
        _ => false,
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
