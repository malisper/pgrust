use pgrust_core::ItemPointerData;

use crate::CommandType;
use crate::plannodes::PlannedStmt;
use crate::primnodes::QueryColumn;

/// Executor-local binding for system Vars like `tableoid`.
///
/// PostgreSQL resolves these against dedicated scan/outer/inner slots rather
/// than against projected user-column layouts. pgrust does not mirror that
/// slot/opcode machinery exactly yet, so upper executor nodes carry the active
/// base-relation bindings explicitly and expression evaluation consults them
/// when evaluating a system Var.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemVarBinding {
    pub varno: usize,
    pub table_oid: u32,
    pub tid: Option<ItemPointerData>,
    pub xmin: Option<u32>,
    pub cmin: Option<u32>,
    pub xmax: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryDesc {
    pub operation: CommandType,
    pub planned_stmt: PlannedStmt,
    pub source_text: Option<String>,
}

impl QueryDesc {
    pub fn columns(&self) -> Vec<QueryColumn> {
        self.planned_stmt.columns()
    }

    pub fn column_names(&self) -> Vec<String> {
        self.planned_stmt.column_names()
    }
}

pub fn create_query_desc(planned_stmt: PlannedStmt, source_text: Option<String>) -> QueryDesc {
    QueryDesc {
        operation: planned_stmt.command_type,
        planned_stmt,
        source_text,
    }
}
