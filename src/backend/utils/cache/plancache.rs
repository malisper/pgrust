use crate::backend::executor::ExecError;
use crate::backend::parser::{CatalogLookup, ParseOptions, Statement};
use crate::backend::utils::misc::notices::push_backend_notice_with_hint;
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
        let result = self.inner.get_statement(sql).map_err(ExecError::from);
        replay_parser_notices();
        result
    }

    pub fn get_statement_with_options(
        &self,
        sql: &str,
        options: ParseOptions,
    ) -> Result<Statement, ExecError> {
        let result = self
            .inner
            .get_statement_with_options(sql, options)
            .map_err(ExecError::from);
        replay_parser_notices();
        result
    }

    pub fn get_query_desc(
        &self,
        sql: &str,
        catalog: &dyn CatalogLookup,
    ) -> Result<QueryDesc, ExecError> {
        let result = self
            .inner
            .get_query_desc(sql, catalog)
            .map_err(ExecError::from);
        replay_parser_notices();
        result
    }

    pub fn get_planned_stmt(
        &self,
        sql: &str,
        catalog: &dyn CatalogLookup,
    ) -> Result<PlannedStmt, ExecError> {
        let result = self
            .inner
            .get_planned_stmt(sql, catalog)
            .map_err(ExecError::from);
        replay_parser_notices();
        result
    }

    pub fn get_plan(&self, sql: &str, catalog: &dyn CatalogLookup) -> Result<Plan, ExecError> {
        let result = self.inner.get_plan(sql, catalog).map_err(ExecError::from);
        replay_parser_notices();
        result
    }

    pub fn invalidate_all(&self) {
        self.inner.invalidate_all();
    }
}

fn replay_parser_notices() {
    for notice in pgrust_parser::take_notices() {
        push_backend_notice_with_hint(
            notice.severity,
            notice.sqlstate,
            notice.message,
            notice.detail,
            notice.hint,
            notice.position,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::utils::misc::notices::{
        clear_notices as clear_backend_notices, take_notices as take_backend_notices,
    };

    #[test]
    fn get_statement_replays_parser_warnings_on_error() {
        clear_backend_notices();
        pgrust_parser::clear_notices();

        let cache = PlanCache::new();
        let result = cache.get_statement(
            r#"create aggregate case_agg ("Sfunc1" = int4pl, "Basetype" = int4, "Stype1" = int4)"#,
        );

        assert!(result.is_err());
        let messages = take_backend_notices()
            .into_iter()
            .map(|notice| notice.message)
            .collect::<Vec<_>>();
        assert_eq!(
            messages,
            vec![
                r#"aggregate attribute "Sfunc1" not recognized"#,
                r#"aggregate attribute "Basetype" not recognized"#,
                r#"aggregate attribute "Stype1" not recognized"#,
            ]
        );
    }
}
