use crate::include::nodes::plannodes::PlannedStmt;
pub use pgrust_nodes::CommandType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryDesc {
    pub operation: CommandType,
    pub planned_stmt: PlannedStmt,
    pub source_text: Option<String>,
}

impl QueryDesc {
    pub fn columns(&self) -> Vec<crate::include::nodes::primnodes::QueryColumn> {
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
