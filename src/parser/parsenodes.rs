use crate::executor::{AggFunc, Value};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    UnexpectedEof,
    UnexpectedToken {
        expected: &'static str,
        actual: String,
    },
    InvalidInteger(String),
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
    UngroupedColumn(String),
    AggInWhere,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedEof => write!(f, "unexpected end of input"),
            ParseError::UnexpectedToken { actual, .. } => write!(f, "{actual}"),
            ParseError::InvalidInteger(value) => write!(f, "invalid integer: {value}"),
            ParseError::UnknownTable(name) => write!(f, "unknown table: {name}"),
            ParseError::UnknownColumn(name) => write!(f, "unknown column: {name}"),
            ParseError::EmptySelectList => write!(f, "SELECT requires a target list or FROM clause"),
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
            ParseError::UngroupedColumn(name) => {
                write!(f, "column \"{name}\" must appear in the GROUP BY clause or be used in an aggregate function")
            }
            ParseError::AggInWhere => {
                write!(f, "aggregate functions are not allowed in WHERE")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Explain(ExplainStatement),
    Select(SelectStatement),
    ShowTables,
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainStatement {
    pub analyze: bool,
    pub buffers: bool,
    pub statement: Box<Statement>,
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
    Table(String),
    InnerJoin {
        left_table: String,
        right_table: String,
        on: SqlExpr,
    },
    CrossJoin {
        left_table: String,
        right_table: String,
    },
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
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: SqlType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Int4,
    Text,
    Bool,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlExpr {
    Column(String),
    Const(Value),
    Add(Box<SqlExpr>, Box<SqlExpr>),
    Eq(Box<SqlExpr>, Box<SqlExpr>),
    Lt(Box<SqlExpr>, Box<SqlExpr>),
    Gt(Box<SqlExpr>, Box<SqlExpr>),
    And(Box<SqlExpr>, Box<SqlExpr>),
    Or(Box<SqlExpr>, Box<SqlExpr>),
    Not(Box<SqlExpr>),
    IsNull(Box<SqlExpr>),
    IsNotNull(Box<SqlExpr>),
    IsDistinctFrom(Box<SqlExpr>, Box<SqlExpr>),
    IsNotDistinctFrom(Box<SqlExpr>, Box<SqlExpr>),
    AggCall {
        func: AggFunc,
        arg: Option<Box<SqlExpr>>,
    },
    Random,
}
