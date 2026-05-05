use crate::backend::executor::ExecError;
use crate::backend::parser::{CatalogLookup, ParseOptions, Statement};
use crate::include::executor::execdesc::QueryDesc;
use crate::include::nodes::plannodes::{Plan, PlannedStmt};

/// Root adapter that preserves the historical `ExecError` API while the cache
/// implementation lives in `pgrust_commands`.
#[derive(Clone)]
pub struct PlanCache {
    inner: pgrust_commands::plancache::PlanCache,
}

impl PlanCache {
    pub fn new() -> Self {
        Self {
            inner: pgrust_commands::plancache::PlanCache::new(),
        }
    }

    pub fn get_statement(&self, sql: &str) -> Result<Statement, ExecError> {
        self.inner.get_statement(sql).map_err(ExecError::from)
    }

    pub fn get_statement_with_options(
        &self,
        sql: &str,
        options: ParseOptions,
    ) -> Result<Statement, ExecError> {
        self.inner
            .get_statement_with_options(sql, options)
            .map_err(ExecError::from)
    }

    pub fn get_query_desc(
        &self,
        sql: &str,
        catalog: &dyn CatalogLookup,
    ) -> Result<QueryDesc, ExecError> {
        self.inner
            .get_query_desc(sql, catalog)
            .map_err(ExecError::from)
    }

    pub fn get_planned_stmt(
        &self,
        sql: &str,
        catalog: &dyn CatalogLookup,
    ) -> Result<PlannedStmt, ExecError> {
        self.inner
            .get_planned_stmt(sql, catalog)
            .map_err(ExecError::from)
    }

    pub fn get_plan(&self, sql: &str, catalog: &dyn CatalogLookup) -> Result<Plan, ExecError> {
        self.inner.get_plan(sql, catalog).map_err(ExecError::from)
    }

    pub fn invalidate_all(&self) {
        self.inner.invalidate_all();
    }
}
