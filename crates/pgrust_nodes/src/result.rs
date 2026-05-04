use crate::datum::Value;
use crate::parsenodes::SqlType;
use crate::primnodes::QueryColumn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConstraintTiming {
    Immediate,
    Deferred,
}

#[derive(Debug, Clone)]
pub struct TypedFunctionArg {
    pub value: Value,
    pub sql_type: Option<SqlType>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SessionReplicationRole {
    #[default]
    Origin,
    Replica,
    Local,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementResult {
    Query {
        columns: Vec<QueryColumn>,
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    AffectedRows(usize),
}
