use std::collections::HashMap;

use parking_lot::RwLock;

use pgrust_analyze::{CatalogLookup, pg_plan_query};
use pgrust_nodes::plannodes::{Plan, PlannedStmt};
use pgrust_nodes::{QueryDesc, create_query_desc};
use pgrust_parser::{
    ParseError, ParseOptions, Statement, parse_statement, parse_statement_with_options,
};

/// Query plan cache — caches parsed statements and can optionally retain
/// built plans for repeated execution of the same SQL.
/// Like PostgreSQL's CachedPlanSource, but simpler: keyed on SQL string.
/// The current runtime only consumes the parsed-statement path.
pub struct PlanCache {
    cache: RwLock<HashMap<String, CachedEntry>>,
}

struct CachedEntry {
    statement: Statement,
    plan: Option<PlannedStmt>,
    query_desc: Option<QueryDesc>,
}

impl PlanCache {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_statement(&self, sql: &str) -> Result<Statement, ParseError> {
        self.get_statement_with_options(sql, ParseOptions::default())
    }

    pub fn get_statement_with_options(
        &self,
        sql: &str,
        options: ParseOptions,
    ) -> Result<Statement, ParseError> {
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(sql) {
                return Ok(entry.statement.clone());
            }
        }
        let stmt = parse_statement_with_options(sql, options)?;
        let mut cache = self.cache.write();
        cache.entry(sql.to_string()).or_insert(CachedEntry {
            statement: stmt.clone(),
            plan: None,
            query_desc: None,
        });
        Ok(stmt)
    }

    pub fn get_query_desc(
        &self,
        sql: &str,
        catalog: &dyn CatalogLookup,
    ) -> Result<QueryDesc, ParseError> {
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(sql) {
                if let Some(ref query_desc) = entry.query_desc {
                    return Ok(query_desc.clone());
                }
            }
        }
        let planned_stmt = self.get_planned_stmt(sql, catalog)?;
        let query_desc = create_query_desc(planned_stmt, Some(sql.to_string()));
        let mut cache = self.cache.write();
        let entry = cache.entry(sql.to_string()).or_insert(CachedEntry {
            statement: parse_statement(sql)?,
            plan: None,
            query_desc: None,
        });
        if !query_desc.planned_stmt.depends_on_row_security && entry.query_desc.is_none() {
            entry.query_desc = Some(query_desc.clone());
        }
        Ok(query_desc)
    }

    pub fn get_planned_stmt(
        &self,
        sql: &str,
        catalog: &dyn CatalogLookup,
    ) -> Result<PlannedStmt, ParseError> {
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(sql) {
                if let Some(ref plan) = entry.plan {
                    return Ok(plan.clone());
                }
            }
        }
        let stmt = parse_statement(sql)?;
        let plan = match &stmt {
            Statement::Select(select) => Some(pg_plan_query(select, catalog)?),
            Statement::Explain(explain) => {
                if let Statement::Select(select) = &*explain.statement {
                    Some(pg_plan_query(select, catalog)?)
                } else {
                    None
                }
            }
            _ => None,
        };
        let mut cache = self.cache.write();
        let entry = cache.entry(sql.to_string()).or_insert(CachedEntry {
            statement: stmt,
            plan: None,
            query_desc: None,
        });
        if plan
            .as_ref()
            .is_some_and(|planned| !planned.depends_on_row_security)
            && entry.plan.is_none()
        {
            entry.plan = plan.clone();
        }
        Ok(plan.unwrap_or_else(|| unreachable!("get_planned_stmt called for non-SELECT")))
    }

    pub fn get_plan(&self, sql: &str, catalog: &dyn CatalogLookup) -> Result<Plan, ParseError> {
        Ok(self.get_query_desc(sql, catalog)?.planned_stmt.plan_tree)
    }

    pub fn invalidate_all(&self) {
        self.cache.write().clear();
    }
}

impl Clone for PlanCache {
    fn clone(&self) -> Self {
        Self::new()
    }
}
