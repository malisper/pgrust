use crate::include::nodes::datum::Value;
use crate::include::nodes::plannodes::AggFunc;
use std::fmt;

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
    UngroupedColumn(String),
    AggInWhere,
    SubqueryMustReturnOneColumn,
    UnknownConfigurationParameter(String),
    ActiveSqlTransaction(&'static str),
    OnCommitOnlyForTempTables,
    TempTableInNonTempSchema(String),
    OnlyTemporaryRelationsInTemporarySchemas(String),
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
            ParseError::UngroupedColumn(name) => {
                write!(
                    f,
                    "column \"{name}\" must appear in the GROUP BY clause or be used in an aggregate function"
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Explain(ExplainStatement),
    Select(SelectStatement),
    Analyze(AnalyzeStatement),
    Set(SetStatement),
    Reset(ResetStatement),
    ShowTables,
    CreateTable(CreateTableStatement),
    CreateTableAs(CreateTableAsStatement),
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
pub enum FromItem {
    Table {
        name: String,
    },
    FunctionCall {
        name: String,
        args: Vec<SqlExpr>,
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
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<SqlExpr>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub schema_name: Option<String>,
    pub table_name: String,
    pub persistence: TablePersistence,
    pub on_commit: OnCommitAction,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableAsStatement {
    pub schema_name: Option<String>,
    pub table_name: String,
    pub persistence: TablePersistence,
    pub on_commit: OnCommitAction,
    pub column_names: Vec<String>,
    pub query: SelectStatement,
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
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlTypeKind {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Json,
    Jsonb,
    JsonPath,
    Text,
    Bool,
    Timestamp,
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

    pub fn numeric_precision_scale(self) -> Option<(i32, i32)> {
        if self.kind != SqlTypeKind::Numeric || self.typmod < Self::VARHDRSZ {
            None
        } else {
            let packed = self.typmod - Self::VARHDRSZ;
            Some(((packed >> 16) & 0xffff, packed & 0xffff))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStatement {
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
    Const(Value),
    IntegerLiteral(String),
    NumericLiteral(String),
    Add(Box<SqlExpr>, Box<SqlExpr>),
    Sub(Box<SqlExpr>, Box<SqlExpr>),
    Mul(Box<SqlExpr>, Box<SqlExpr>),
    Div(Box<SqlExpr>, Box<SqlExpr>),
    Mod(Box<SqlExpr>, Box<SqlExpr>),
    Concat(Box<SqlExpr>, Box<SqlExpr>),
    UnaryPlus(Box<SqlExpr>),
    Negate(Box<SqlExpr>),
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
        args: Vec<SqlExpr>,
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
        args: Vec<SqlExpr>,
    },
    CurrentTimestamp,
}
