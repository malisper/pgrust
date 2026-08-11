//! SELECT statement construction and rendering.

use crate::expr::{Expr, ExprGen};

pub struct SelectStmt {
    pub exprs: Vec<Expr>,
    pub table: String,
    pub where_clause: Option<Expr>,
    /// ORDER BY every output column by position, pinning row order so the
    /// differential runner can compare unsorted.
    pub order_by_all: bool,
}

impl SelectStmt {
    pub fn to_sql(&self) -> String {
        let mut out = String::from("SELECT ");
        for (i, e) in self.exprs.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            e.render(&mut out);
        }
        out.push_str(" FROM ");
        out.push_str(&self.table);
        if let Some(w) = &self.where_clause {
            out.push_str(" WHERE ");
            w.render(&mut out);
        }
        if self.order_by_all {
            out.push_str(" ORDER BY ");
            for i in 1..=self.exprs.len() {
                if i > 1 {
                    out.push_str(", ");
                }
                out.push_str(&i.to_string());
            }
        }
        out.push(';');
        out
    }
}

/// One statement from the scalar-expression module. `max_depth` bounds
/// expression nesting.
pub fn gen_select(g: &mut ExprGen, max_depth: u32) -> SelectStmt {
    g.productions.push("select".to_string());
    let ncols = 1 + g.rng.below(4) as usize;
    let mut exprs = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let ty = g.any_type();
        exprs.push(g.gen_typed(ty, max_depth));
    }
    let where_clause = if g.rng.chance(1, 2) {
        g.productions.push("where".to_string());
        Some(g.gen_bool(max_depth))
    } else {
        None
    };
    let order_by_all = g.rng.chance(1, 2);
    if order_by_all {
        g.productions.push("orderby".to_string());
    }
    SelectStmt {
        exprs,
        table: g.table.name.clone(),
        where_clause,
        order_by_all,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;

    #[test]
    fn select_renders_with_order_by() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let t = &cat.tables[0];
        let mut rng = Rng::new(21);
        let mut prods = Vec::new();
        let mut g = ExprGen { rng: &mut rng, table: t, productions: &mut prods };
        for _ in 0..20 {
            let s = gen_select(&mut g, 3);
            let sql = s.to_sql();
            assert!(sql.starts_with("SELECT "));
            assert!(sql.contains(" FROM fz_scalar"));
            assert!(sql.ends_with(';'));
            if s.order_by_all {
                assert!(sql.contains(" ORDER BY 1"));
            }
        }
        assert!(prods.iter().any(|p| p == "select"));
    }
}
