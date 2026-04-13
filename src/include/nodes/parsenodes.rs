use crate::include::nodes::datum::Value;
use crate::include::nodes::plannodes::AggFunc;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UngroupedColumnClause {
    SelectTarget,
    Having,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    UnexpectedEof,
    UnexpectedToken {
        expected: &'static str,
        actual: String,
    },
    InvalidInteger(String),
    InvalidNumeric(String),
    UnknownTable(String),
    UnknownColumn(String),
    EmptySelectList,
    UnsupportedQualifiedName(String),
    InvalidInsertTargetCount {
        expected: usize,
        actual: usize,
    },
    TableAlreadyExists(String),
    TableDoesNotExist(String),
    UnsupportedType(String),
    UndefinedOperator {
        op: &'static str,
        left_type: String,
        right_type: String,
    },
    UngroupedColumn {
        display_name: String,
        token: String,
        clause: UngroupedColumnClause,
    },
    AggInWhere,
    SubqueryMustReturnOneColumn,
    UnknownConfigurationParameter(String),
    UnrecognizedParameter(String),
    TablesDeclaredWithOidsNotSupported,
    ActiveSqlTransaction(&'static str),
    OnCommitOnlyForTempTables,
    TempTableInNonTempSchema(String),
    OnlyTemporaryRelationsInTemporarySchemas(String),
    NoSchemaSelectedForCreate,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedEof => write!(f, "unexpected end of input"),
            ParseError::UnexpectedToken { actual, .. } => write!(f, "{actual}"),
            ParseError::InvalidInteger(value) => write!(f, "invalid integer: {value}"),
            ParseError::InvalidNumeric(value) => write!(f, "invalid numeric: {value}"),
            ParseError::UnknownTable(name) => write!(f, "unknown table: {name}"),
            ParseError::UnknownColumn(name) => write!(f, "unknown column: {name}"),
            ParseError::EmptySelectList => {
                write!(f, "SELECT requires a target list or FROM clause")
            }
            ParseError::UnsupportedQualifiedName(name) => {
                write!(f, "unsupported qualified name: {name}")
            }
            ParseError::InvalidInsertTargetCount { expected, actual } => write!(
                f,
                "INSERT has {actual} values but target list requires {expected}"
            ),
            ParseError::TableAlreadyExists(name) => write!(f, "table already exists: {name}"),
            ParseError::TableDoesNotExist(name) => write!(f, "table does not exist: {name}"),
            ParseError::UnsupportedType(name) => write!(f, "unsupported type: {name}"),
            ParseError::UndefinedOperator {
                op,
                left_type,
                right_type,
            } => {
                write!(f, "operator does not exist: {left_type} {op} {right_type}")
            }
            ParseError::UngroupedColumn { display_name, .. } => {
                write!(
                    f,
                    "column \"{display_name}\" must appear in the GROUP BY clause or be used in an aggregate function"
                )
            }
            ParseError::AggInWhere => {
                write!(f, "aggregate functions are not allowed in WHERE")
            }
            ParseError::SubqueryMustReturnOneColumn => {
                write!(f, "subquery must return only one column")
            }
            ParseError::UnknownConfigurationParameter(name) => {
                write!(f, "unrecognized configuration parameter \"{name}\"")
            }
            ParseError::UnrecognizedParameter(name) => {
                write!(f, "unrecognized parameter \"{name}\"")
            }
            ParseError::TablesDeclaredWithOidsNotSupported => {
                write!(f, "tables declared WITH OIDS are not supported")
            }
            ParseError::ActiveSqlTransaction(stmt) => {
                write!(f, "{stmt} cannot run inside a transaction block")
            }
            ParseError::OnCommitOnlyForTempTables => {
                write!(f, "ON COMMIT can only be used on temporary tables")
            }
            ParseError::TempTableInNonTempSchema(_name) => {
                write!(
                    f,
                    "cannot create temporary relation in non-temporary schema"
                )
            }
            ParseError::OnlyTemporaryRelationsInTemporarySchemas(name) => {
                let _ = name;
                write!(
                    f,
                    "only temporary relations may be created in temporary schemas"
                )
            }
            ParseError::NoSchemaSelectedForCreate => {
                write!(f, "no schema has been selected to create in")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Do(DoStatement),
    Explain(ExplainStatement),
    Select(SelectStatement),
    Values(ValuesStatement),
    Analyze(AnalyzeStatement),
    Set(SetStatement),
    Reset(ResetStatement),
    CreateTable(CreateTableStatement),
    CreateTableAs(CreateTableAsStatement),
    CreateIndex(CreateIndexStatement),
    AlterTableAddColumn(AlterTableAddColumnStatement),
    AlterTableSet(AlterTableSetStatement),
    CommentOnTable(CommentOnTableStatement),
    DropTable(DropTableStatement),
    TruncateTable(TruncateTableStatement),
    Vacuum(VacuumStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoStatement {
    pub language: Option<String>,
    pub code: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TablePersistence {
    Permanent,
    Temporary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCommitAction {
    PreserveRows,
    DeleteRows,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetStatement {
    pub name: String,
    pub value: String,
    pub is_local: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetStatement {
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainStatement {
    pub analyze: bool,
    pub buffers: bool,
    pub timing: bool,
    pub statement: Box<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTarget {
    pub table_name: String,
    pub columns: Vec<String>,
    pub only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzeStatement {
    pub targets: Vec<MaintenanceTarget>,
    pub verbose: bool,
    pub skip_locked: bool,
    pub buffer_usage_limit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub with: Vec<CommonTableExpr>,
    pub from: Option<FromItem>,
    pub targets: Vec<SelectItem>,
    pub where_clause: Option<SqlExpr>,
    pub group_by: Vec<SqlExpr>,
    pub having: Option<SqlExpr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValuesStatement {
    pub with: Vec<CommonTableExpr>,
    pub rows: Vec<Vec<SqlExpr>>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommonTableExpr {
    pub name: String,
    pub column_names: Vec<String>,
    pub body: CteBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CteBody {
    Select(Box<SelectStatement>),
    Values(ValuesStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromItem {
    Table {
        name: String,
    },
    Values {
        rows: Vec<Vec<SqlExpr>>,
    },
    FunctionCall {
        name: String,
        args: Vec<SqlFunctionArg>,
    },
    DerivedTable(Box<SelectStatement>),
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        kind: JoinKind,
        on: Option<SqlExpr>,
    },
    Alias {
        source: Box<FromItem>,
        alias: String,
        column_aliases: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlFunctionArg {
    pub name: Option<String>,
    pub value: SqlExpr,
}

impl SqlFunctionArg {
    pub fn positional(value: SqlExpr) -> Self {
        Self { name: None, value }
    }
}

pub fn function_arg_values(args: &[SqlFunctionArg]) -> impl Iterator<Item = &SqlExpr> {
    args.iter().map(|arg| &arg.value)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectItem {
    pub output_name: String,
    pub expr: SqlExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByItem {
    pub expr: SqlExpr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStatement {
    pub with: Vec<CommonTableExpr>,
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertSource {
    Values(Vec<Vec<SqlExpr>>),
    DefaultValues,
    Select(Box<SelectStatement>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub schema_name: Option<String>,
    pub table_name: String,
    pub persistence: TablePersistence,
    pub on_commit: OnCommitAction,
    pub columns: Vec<ColumnDef>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableAsStatement {
    pub schema_name: Option<String>,
    pub table_name: String,
    pub persistence: TablePersistence,
    pub on_commit: OnCommitAction,
    pub column_names: Vec<String>,
    pub query: SelectStatement,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexStatement {
    pub unique: bool,
    pub index_name: String,
    pub table_name: String,
    pub using_method: Option<String>,
    pub columns: Vec<IndexColumnDef>,
    pub include_columns: Vec<String>,
    pub predicate: Option<SqlExpr>,
    pub options: Vec<RelOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexColumnDef {
    pub name: String,
    pub collation: Option<String>,
    pub opclass: Option<String>,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

impl From<&str> for IndexColumnDef {
    fn from(value: &str) -> Self {
        Self {
            name: value.to_string(),
            collation: None,
            opclass: None,
            descending: false,
            nulls_first: None,
        }
    }
}

impl From<String> for IndexColumnDef {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableSetStatement {
    pub table_name: String,
    pub options: Vec<RelOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAddColumnStatement {
    pub table_name: String,
    pub column: ColumnDef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnTableStatement {
    pub table_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelOption {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub if_exists: bool,
    pub table_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncateTableStatement {
    pub table_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacuumStatement {
    pub targets: Vec<MaintenanceTarget>,
    pub analyze: bool,
    pub full: bool,
    pub verbose: bool,
    pub skip_locked: bool,
    pub buffer_usage_limit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: SqlType,
    pub default_expr: Option<String>,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlTypeKind {
    Int2,
    Int2Vector,
    Int4,
    Int8,
    Name,
    Oid,
    OidVector,
    Bit,
    VarBit,
    Bytea,
    Float4,
    Float8,
    Numeric,
    Json,
    Jsonb,
    JsonPath,
    Text,
    Bool,
    Timestamp,
    PgNodeTree,
    InternalChar,
    Char,
    Varchar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlType {
    pub kind: SqlTypeKind,
    pub typmod: i32,
    pub is_array: bool,
}

impl SqlType {
    pub const NO_TYPEMOD: i32 = -1;
    pub const VARHDRSZ: i32 = 4;

    pub const fn new(kind: SqlTypeKind) -> Self {
        Self {
            kind,
            typmod: Self::NO_TYPEMOD,
            is_array: false,
        }
    }

    pub const fn with_char_len(kind: SqlTypeKind, len: i32) -> Self {
        Self {
            kind,
            typmod: Self::VARHDRSZ + len,
            is_array: false,
        }
    }

    pub const fn with_bit_len(kind: SqlTypeKind, len: i32) -> Self {
        Self {
            kind,
            typmod: Self::VARHDRSZ + len,
            is_array: false,
        }
    }

    pub const fn with_numeric_precision_scale(precision: i32, scale: i32) -> Self {
        Self {
            kind: SqlTypeKind::Numeric,
            typmod: Self::VARHDRSZ + ((precision << 16) | (scale & 0xffff)),
            is_array: false,
        }
    }

    pub const fn array_of(mut elem: SqlType) -> Self {
        elem.is_array = true;
        elem
    }

    pub const fn element_type(self) -> Self {
        Self {
            kind: self.kind,
            typmod: self.typmod,
            is_array: false,
        }
    }

    pub const fn char_len(self) -> Option<i32> {
        if self.typmod >= Self::VARHDRSZ {
            Some(self.typmod - Self::VARHDRSZ)
        } else {
            None
        }
    }

    pub const fn bit_len(self) -> Option<i32> {
        if self.typmod >= Self::VARHDRSZ {
            Some(self.typmod - Self::VARHDRSZ)
        } else {
            None
        }
    }

    pub fn numeric_precision_scale(self) -> Option<(i32, i32)> {
        if self.kind != SqlTypeKind::Numeric || self.typmod < Self::VARHDRSZ {
            None
        } else {
            let packed = self.typmod - Self::VARHDRSZ;
            let precision = (packed >> 16) & 0xffff;
            let scale = ((packed & 0xffff) as i16) as i32;
            Some((precision, scale))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStatement {
    pub with: Vec<CommonTableExpr>,
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStatement {
    pub with: Vec<CommonTableExpr>,
    pub table_name: String,
    pub where_clause: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub column: String,
    pub expr: SqlExpr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryComparisonOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlExpr {
    Column(String),
    Default,
    Const(Value),
    IntegerLiteral(String),
    NumericLiteral(String),
    Add(Box<SqlExpr>, Box<SqlExpr>),
    Sub(Box<SqlExpr>, Box<SqlExpr>),
    BitAnd(Box<SqlExpr>, Box<SqlExpr>),
    BitOr(Box<SqlExpr>, Box<SqlExpr>),
    BitXor(Box<SqlExpr>, Box<SqlExpr>),
    Shl(Box<SqlExpr>, Box<SqlExpr>),
    Shr(Box<SqlExpr>, Box<SqlExpr>),
    Mul(Box<SqlExpr>, Box<SqlExpr>),
    Div(Box<SqlExpr>, Box<SqlExpr>),
    Mod(Box<SqlExpr>, Box<SqlExpr>),
    Concat(Box<SqlExpr>, Box<SqlExpr>),
    UnaryPlus(Box<SqlExpr>),
    Negate(Box<SqlExpr>),
    BitNot(Box<SqlExpr>),
    Cast(Box<SqlExpr>, SqlType),
    Eq(Box<SqlExpr>, Box<SqlExpr>),
    NotEq(Box<SqlExpr>, Box<SqlExpr>),
    Lt(Box<SqlExpr>, Box<SqlExpr>),
    LtEq(Box<SqlExpr>, Box<SqlExpr>),
    Gt(Box<SqlExpr>, Box<SqlExpr>),
    GtEq(Box<SqlExpr>, Box<SqlExpr>),
    RegexMatch(Box<SqlExpr>, Box<SqlExpr>),
    And(Box<SqlExpr>, Box<SqlExpr>),
    Or(Box<SqlExpr>, Box<SqlExpr>),
    Not(Box<SqlExpr>),
    IsNull(Box<SqlExpr>),
    IsNotNull(Box<SqlExpr>),
    IsDistinctFrom(Box<SqlExpr>, Box<SqlExpr>),
    IsNotDistinctFrom(Box<SqlExpr>, Box<SqlExpr>),
    ArrayLiteral(Vec<SqlExpr>),
    ArrayOverlap(Box<SqlExpr>, Box<SqlExpr>),
    JsonbContains(Box<SqlExpr>, Box<SqlExpr>),
    JsonbContained(Box<SqlExpr>, Box<SqlExpr>),
    JsonbExists(Box<SqlExpr>, Box<SqlExpr>),
    JsonbExistsAny(Box<SqlExpr>, Box<SqlExpr>),
    JsonbExistsAll(Box<SqlExpr>, Box<SqlExpr>),
    JsonbPathExists(Box<SqlExpr>, Box<SqlExpr>),
    JsonbPathMatch(Box<SqlExpr>, Box<SqlExpr>),
    AggCall {
        func: AggFunc,
        args: Vec<SqlFunctionArg>,
        distinct: bool,
    },
    ScalarSubquery(Box<SelectStatement>),
    Exists(Box<SelectStatement>),
    InSubquery {
        expr: Box<SqlExpr>,
        subquery: Box<SelectStatement>,
        negated: bool,
    },
    QuantifiedSubquery {
        left: Box<SqlExpr>,
        op: SubqueryComparisonOp,
        is_all: bool,
        subquery: Box<SelectStatement>,
    },
    QuantifiedArray {
        left: Box<SqlExpr>,
        op: SubqueryComparisonOp,
        is_all: bool,
        array: Box<SqlExpr>,
    },
    Random,
    JsonGet(Box<SqlExpr>, Box<SqlExpr>),
    JsonGetText(Box<SqlExpr>, Box<SqlExpr>),
    JsonPath(Box<SqlExpr>, Box<SqlExpr>),
    JsonPathText(Box<SqlExpr>, Box<SqlExpr>),
    FuncCall {
        name: String,
        args: Vec<SqlFunctionArg>,
    },
    CurrentTimestamp,
}
