//! Lexical scope for statement generation: the set of FROM-clause relations
//! (tables, derived tables, CTE references) whose columns an expression may
//! reference, plus an optional chain of enclosing scopes for correlated
//! subqueries. Column references are always alias-qualified, and aliases
//! are unique per statement (crate::stmt::Gen allocates them), so scoping
//! is checkable on the AST (crate::render::scope_errors).

use crate::catalog::{Column, SqlType, Table};

/// One in-scope relation: an alias plus the columns it exposes. Derived
/// tables and CTE references synthesize their column lists from the
/// subquery's select items.
#[derive(Clone, Debug)]
pub struct ScopeRel {
    pub alias: String,
    pub columns: Vec<Column>,
}

impl ScopeRel {
    pub fn from_table(table: &Table, alias: String) -> ScopeRel {
        ScopeRel { alias, columns: table.columns.clone() }
    }
}

/// The relations visible at one query level, plus the enclosing levels
/// (correlated subqueries may reference any of them).
pub struct Scope<'a> {
    pub rels: &'a [ScopeRel],
    pub outer: Option<&'a Scope<'a>>,
}

impl<'a> Scope<'a> {
    /// Columns of `ty` at this level only (alias-qualified draws).
    pub fn columns_of_type(&self, ty: SqlType) -> Vec<(&'a str, &'a Column)> {
        let mut out = Vec::new();
        for rel in self.rels {
            for c in &rel.columns {
                if c.ty == ty {
                    out.push((rel.alias.as_str(), c));
                }
            }
        }
        out
    }

    /// Columns of `ty` on any enclosing level (correlated references).
    pub fn outer_columns_of_type(&self, ty: SqlType) -> Vec<(&'a str, &'a Column)> {
        let mut out = Vec::new();
        let mut cur = self.outer;
        while let Some(s) = cur {
            out.extend(s.columns_of_type(ty));
            cur = s.outer;
        }
        out
    }

    /// Types with at least one column at this level.
    pub fn has_type(&self, ty: SqlType) -> bool {
        self.rels.iter().any(|r| r.columns.iter().any(|c| c.ty == ty))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};

    #[test]
    fn scope_lookup_is_alias_qualified() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let rels = vec![
            ScopeRel::from_table(&cat.tables[0], "t0".to_string()),
            ScopeRel::from_table(&cat.tables[0], "t1".to_string()),
        ];
        let scope = Scope { rels: &rels, outer: None };
        let cols = scope.columns_of_type(SqlType::Int4);
        // Self-join: the same table's int4 columns appear once per alias.
        assert!(cols.iter().any(|(a, _)| *a == "t0"));
        assert!(cols.iter().any(|(a, _)| *a == "t1"));
        assert_eq!(cols.len() % 2, 0);
        assert!(scope.has_type(SqlType::Int4));
        assert!(scope.outer_columns_of_type(SqlType::Int4).is_empty());
    }

    #[test]
    fn outer_chain_is_walked() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let outer_rels = vec![ScopeRel::from_table(&cat.tables[0], "t0".to_string())];
        let outer = Scope { rels: &outer_rels, outer: None };
        let inner_rels = vec![ScopeRel::from_table(&cat.tables[1], "t1".to_string())];
        let inner = Scope { rels: &inner_rels, outer: Some(&outer) };
        let cols = inner.outer_columns_of_type(SqlType::Int4);
        assert!(!cols.is_empty());
        assert!(cols.iter().all(|(a, _)| *a == "t0"));
    }
}
