use std::collections::HashMap;

use parking_lot::RwLock;

use crate::backend::executor::ExecError;
use crate::backend::parser::{CatalogLookup, Statement, build_plan, parse_statement};
use crate::include::nodes::execnodes::Plan;

/// Query plan cache — caches parsed statements and built plans to avoid
/// re-parsing and re-planning on repeated executions of the same SQL.
/// Like PostgreSQL's CachedPlanSource, but simpler: keyed on SQL string,
/// invalidated on any DDL.
pub struct PlanCache {
    cache: RwLock<HashMap<String, CachedEntry>>,
}

struct CachedEntry {
    statement: Statement,
    plan: Option<Plan>,
}

impl PlanCache {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_statement(&self, sql: &str) -> Result<Statement, ExecError> {
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(sql) {
                return Ok(entry.statement.clone());
            }
        }
        let stmt = parse_statement(sql)?;
        let mut cache = self.cache.write();
        cache.entry(sql.to_string()).or_insert(CachedEntry {
            statement: stmt.clone(),
            plan: None,
        });
        Ok(stmt)
    }

    pub fn get_plan(&self, sql: &str, catalog: &dyn CatalogLookup) -> Result<Plan, ExecError> {
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
            Statement::Select(select) => Some(build_plan(select, catalog)?),
            Statement::Explain(explain) => {
                if let Statement::Select(select) = &*explain.statement {
                    Some(build_plan(select, catalog)?)
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
        });
        if plan.is_some() && entry.plan.is_none() {
            entry.plan = plan.clone();
        }
        Ok(plan.unwrap_or_else(|| unreachable!("get_plan called for non-SELECT")))
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
