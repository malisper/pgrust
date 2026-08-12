//! Statement AST and SQL rendering: SELECT with CTEs, join trees, derived
//! tables, DISTINCT, GROUP BY (plain / ROLLUP / CUBE / GROUPING SETS),
//! HAVING, named WINDOW clauses, ORDER BY/LIMIT/OFFSET — plus
//! `scope_errors`, the AST-level scoping checker the unit tests run
//! against (every column reference must resolve to a lexically visible
//! alias, aggregates and window functions only where SQL allows them, and
//! grouped statements project only group keys and aggregates).

use crate::catalog::{Catalog, SqlType};
use crate::expr::Expr;

/// One SELECT-list item. `ty` is the expression's type by construction
/// (drives derived-table/CTE column typing and the LIMIT float guard);
/// `alias` names the output column (CTE/derived bodies need names).
#[derive(Clone, Debug)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
    pub ty: SqlType,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinKind {
    fn word(self) -> &'static str {
        match self {
            JoinKind::Inner => "JOIN",
            JoinKind::Left => "LEFT JOIN",
            JoinKind::Right => "RIGHT JOIN",
            JoinKind::Full => "FULL JOIN",
        }
    }
}

#[derive(Clone, Debug)]
pub enum JoinCond {
    On(Expr),
    Using(Vec<String>),
    Natural,
    /// CROSS JOIN (kind is ignored when rendering).
    Cross,
}

#[derive(Clone, Debug)]
pub enum FromItem {
    /// Base table or CTE reference, always aliased.
    Table { name: String, alias: String },
    /// Derived table: `(SELECT ...) AS alias(c0, c1, ...)`.
    Derived { body: Box<SelectStmt>, alias: String, columns: Vec<String> },
    Join { left: Box<FromItem>, right: Box<FromItem>, kind: JoinKind, cond: JoinCond },
}

/// GROUP BY clause forms. Plain expressions render parenthesized so a
/// bare integer-literal key is never read as an output-column ordinal
/// (`GROUP BY 5` is positional; `GROUP BY (5)` is a constant expression).
#[derive(Clone, Debug)]
pub enum GroupBy {
    Plain(Vec<Expr>),
    Rollup(Vec<Expr>),
    Cube(Vec<Expr>),
    Sets(Vec<Vec<Expr>>),
}

impl GroupBy {
    fn render(&self, out: &mut String) {
        out.push_str(" GROUP BY ");
        let list = |out: &mut String, exprs: &[Expr], parens: bool| {
            for (i, e) in exprs.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                if parens {
                    out.push('(');
                }
                e.render(out);
                if parens {
                    out.push(')');
                }
            }
        };
        match self {
            GroupBy::Plain(exprs) => list(out, exprs, true),
            GroupBy::Rollup(exprs) => {
                out.push_str("ROLLUP (");
                list(out, exprs, false);
                out.push(')');
            }
            GroupBy::Cube(exprs) => {
                out.push_str("CUBE (");
                list(out, exprs, false);
                out.push(')');
            }
            GroupBy::Sets(sets) => {
                out.push_str("GROUPING SETS (");
                for (i, set) in sets.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    out.push('(');
                    list(out, set, false);
                    out.push(')');
                }
                out.push(')');
            }
        }
    }

    /// Every expression the clause groups by (union over grouping sets):
    /// the grouped-projection check keys on these.
    fn exprs(&self) -> Vec<&Expr> {
        match self {
            GroupBy::Plain(e) | GroupBy::Rollup(e) | GroupBy::Cube(e) => e.iter().collect(),
            GroupBy::Sets(sets) => sets.iter().flatten().collect(),
        }
    }
}

/// A window definition: OVER (...) body or named WINDOW clause entry.
#[derive(Clone, Debug)]
pub struct WindowDef {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<WinOrderKey>,
    pub frame: Option<Frame>,
}

/// One window ORDER BY key (an expression, unlike the positional
/// statement-level OrderKey).
#[derive(Clone, Debug)]
pub struct WinOrderKey {
    pub expr: Expr,
    pub desc: bool,
    /// None = default, Some(true) = NULLS FIRST, Some(false) = NULLS LAST.
    pub nulls_first: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct Frame {
    /// true = RANGE, false = ROWS.
    pub range: bool,
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(u32),
    CurrentRow,
    Following(u32),
    UnboundedFollowing,
}

impl FrameBound {
    fn render(self, out: &mut String) {
        match self {
            FrameBound::UnboundedPreceding => out.push_str("UNBOUNDED PRECEDING"),
            FrameBound::Preceding(n) => out.push_str(&format!("{} PRECEDING", n)),
            FrameBound::CurrentRow => out.push_str("CURRENT ROW"),
            FrameBound::Following(n) => out.push_str(&format!("{} FOLLOWING", n)),
            FrameBound::UnboundedFollowing => out.push_str("UNBOUNDED FOLLOWING"),
        }
    }
}

impl WindowDef {
    pub fn render(&self, out: &mut String) {
        let mut first = true;
        let mut sep = |out: &mut String| {
            if !first {
                out.push(' ');
            }
            first = false;
        };
        if !self.partition_by.is_empty() {
            sep(out);
            out.push_str("PARTITION BY ");
            for (i, e) in self.partition_by.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                e.render(out);
            }
        }
        if !self.order_by.is_empty() {
            sep(out);
            out.push_str("ORDER BY ");
            for (i, k) in self.order_by.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                k.expr.render(out);
                if k.desc {
                    out.push_str(" DESC");
                }
                match k.nulls_first {
                    Some(true) => out.push_str(" NULLS FIRST"),
                    Some(false) => out.push_str(" NULLS LAST"),
                    None => {}
                }
            }
        }
        if let Some(f) = &self.frame {
            sep(out);
            out.push_str(if f.range { "RANGE BETWEEN " } else { "ROWS BETWEEN " });
            f.start.render(out);
            out.push_str(" AND ");
            f.end.render(out);
        }
    }
}

/// How a window function names its window: inline OVER (...) or a
/// reference to the statement's named WINDOW clause.
#[derive(Clone, Debug)]
pub enum WindowOver {
    Inline(WindowDef),
    Named(String),
}

/// One ORDER BY key, by output-column position.
#[derive(Clone, Debug)]
pub struct OrderKey {
    pub position: usize,
    /// None = default (ASC), Some(true) = DESC, Some(false) = explicit ASC.
    pub desc: Option<bool>,
    /// None = default, Some(true) = NULLS FIRST, Some(false) = NULLS LAST.
    pub nulls_first: Option<bool>,
}

#[derive(Clone, Debug, Default)]
pub struct SelectStmt {
    /// Non-recursive CTEs, top-level statements only.
    pub ctes: Vec<(String, SelectStmt)>,
    pub distinct: bool,
    pub items: Vec<SelectItem>,
    pub from: Option<FromItem>,
    pub where_clause: Option<Expr>,
    /// Top-level statements only (agg module).
    pub group_by: Option<GroupBy>,
    pub having: Option<Expr>,
    /// Named WINDOW clause entries, top-level statements only (win module).
    pub windows: Vec<(String, WindowDef)>,
    pub order_by: Vec<OrderKey>,
    pub limit: Option<String>,
    pub offset: Option<String>,
}

impl FromItem {
    fn render(&self, out: &mut String) {
        match self {
            FromItem::Table { name, alias } => {
                out.push_str(name);
                out.push_str(" AS ");
                out.push_str(alias);
            }
            FromItem::Derived { body, alias, columns } => {
                out.push('(');
                body.render(out);
                out.push_str(") AS ");
                out.push_str(alias);
                out.push('(');
                for (i, c) in columns.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    out.push_str(c);
                }
                out.push(')');
            }
            FromItem::Join { left, right, kind, cond } => {
                left.render(out);
                out.push(' ');
                match cond {
                    JoinCond::Cross => out.push_str("CROSS JOIN"),
                    JoinCond::Natural => {
                        out.push_str("NATURAL ");
                        out.push_str(kind.word());
                    }
                    _ => out.push_str(kind.word()),
                }
                out.push(' ');
                // The generator builds left-deep chains; a join on the right
                // needs parentheses to keep the shape (left-associative
                // parsing would rebalance it).
                if matches!(**right, FromItem::Join { .. }) {
                    out.push('(');
                    right.render(out);
                    out.push(')');
                } else {
                    right.render(out);
                }
                match cond {
                    JoinCond::On(e) => {
                        out.push_str(" ON ");
                        e.render(out);
                    }
                    JoinCond::Using(cols) => {
                        out.push_str(" USING (");
                        for (i, c) in cols.iter().enumerate() {
                            if i > 0 {
                                out.push_str(", ");
                            }
                            out.push_str(c);
                        }
                        out.push(')');
                    }
                    JoinCond::Natural | JoinCond::Cross => {}
                }
            }
        }
    }
}

impl SelectStmt {
    /// Render without a trailing semicolon (subquery position).
    pub fn render(&self, out: &mut String) {
        if !self.ctes.is_empty() {
            out.push_str("WITH ");
            for (i, (name, body)) in self.ctes.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(name);
                out.push_str(" AS (");
                body.render(out);
                out.push(')');
            }
            out.push(' ');
        }
        out.push_str(if self.distinct { "SELECT DISTINCT " } else { "SELECT " });
        for (i, item) in self.items.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            item.expr.render(out);
            if let Some(a) = &item.alias {
                out.push_str(" AS ");
                out.push_str(a);
            }
        }
        if let Some(f) = &self.from {
            out.push_str(" FROM ");
            f.render(out);
        }
        if let Some(w) = &self.where_clause {
            out.push_str(" WHERE ");
            w.render(out);
        }
        if let Some(gb) = &self.group_by {
            gb.render(out);
        }
        if let Some(h) = &self.having {
            out.push_str(" HAVING ");
            h.render(out);
        }
        if !self.windows.is_empty() {
            out.push_str(" WINDOW ");
            for (i, (name, def)) in self.windows.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(name);
                out.push_str(" AS (");
                def.render(out);
                out.push(')');
            }
        }
        if !self.order_by.is_empty() {
            out.push_str(" ORDER BY ");
            for (i, k) in self.order_by.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&k.position.to_string());
                match k.desc {
                    Some(true) => out.push_str(" DESC"),
                    Some(false) => out.push_str(" ASC"),
                    None => {}
                }
                match k.nulls_first {
                    Some(true) => out.push_str(" NULLS FIRST"),
                    Some(false) => out.push_str(" NULLS LAST"),
                    None => {}
                }
            }
        }
        if let Some(l) = &self.limit {
            out.push_str(" LIMIT ");
            out.push_str(l);
        }
        if let Some(o) = &self.offset {
            out.push_str(" OFFSET ");
            out.push_str(o);
        }
    }

    /// Top-level statement text.
    pub fn to_sql(&self) -> String {
        let mut s = String::new();
        self.render(&mut s);
        s.push(';');
        s
    }
}

/// Order-sensitive float aggregates: their accumulation order is
/// plan-dependent, so their result columns are ruled-soft (B1 ruling;
/// crate::ruled b1-float-agg-soft). numeric variants of the same
/// aggregates accumulate exactly and are NOT listed.
pub const SOFT_FLOAT_AGGS: &[&str] =
    &["sum", "avg", "stddev_samp", "stddev_pop", "var_samp", "var_pop"];

/// Output-column indexes whose values are order-sensitive float aggregates
/// (ruled-soft in the differ). Empty for every statement the agg module
/// did not mark.
pub fn soft_float_cols(stmt: &SelectStmt) -> Vec<usize> {
    stmt.items
        .iter()
        .enumerate()
        .filter(|(_, item)| {
            item.ty.is_float()
                && matches!(&item.expr, Expr::Agg { name, .. } if SOFT_FLOAT_AGGS.contains(name))
        })
        .map(|(i, _)| i)
        .collect()
}

// ------------------------------------------------------------------------
// AST scoping checker (test oracle; also handy for debugging generators).
// ------------------------------------------------------------------------

type Env = Vec<(String, Vec<String>)>;

/// Column names a FROM-clause name exposes: a CTE of the current statement
/// or a catalog table.
fn rel_columns(
    name: &str,
    catalog: &Catalog,
    ctes: &[(String, SelectStmt)],
) -> Option<Vec<String>> {
    if let Some((_, body)) = ctes.iter().find(|(n, _)| n == name) {
        return Some(
            body.items
                .iter()
                .map(|i| i.alias.clone().unwrap_or_else(|| "?unnamed?".to_string()))
                .collect(),
        );
    }
    catalog
        .tables
        .iter()
        .find(|t| t.name == name)
        .map(|t| t.columns.iter().map(|c| c.name.clone()).collect())
}

/// Walk a FROM item: append its aliases to `local`, check nested derived
/// bodies (uncorrelated: empty outer env) and ON expressions (own subtree +
/// outer env).
fn check_from(
    item: &FromItem,
    outer: &Env,
    local: &mut Env,
    catalog: &Catalog,
    ctes: &[(String, SelectStmt)],
    errs: &mut Vec<String>,
) {
    match item {
        FromItem::Table { name, alias } => match rel_columns(name, catalog, ctes) {
            Some(cols) => local.push((alias.clone(), cols)),
            None => errs.push(format!("unknown relation {name} (alias {alias})")),
        },
        FromItem::Derived { body, alias, columns } => {
            if body.items.len() != columns.len() {
                errs.push(format!("derived table {alias}: column list arity mismatch"));
            }
            // Plain (non-LATERAL) FROM subqueries must not be correlated.
            check_stmt(body, &Vec::new(), catalog, ctes, false, errs);
            local.push((alias.clone(), columns.clone()));
        }
        FromItem::Join { left, right, cond, .. } => {
            let before = local.len();
            check_from(left, outer, local, catalog, ctes, errs);
            check_from(right, outer, local, catalog, ctes, errs);
            if let JoinCond::On(e) = cond {
                // ON sees the join's own subtree plus enclosing scopes;
                // aggregates and window functions are illegal there.
                let mut env = outer.clone();
                env.extend_from_slice(&local[before..]);
                check_expr(e, &env, catalog, ctes, ExprCtx::plain(), errs);
            }
            if let JoinCond::Using(cols) = cond {
                for c in cols {
                    for (alias, rel_cols) in &local[before..] {
                        if !rel_cols.contains(c) {
                            errs.push(format!("USING ({c}) not present in {alias}"));
                        }
                    }
                }
            }
        }
    }
}

/// Where an expression sits, for aggregate/window legality: aggregates
/// only in select items and HAVING, window functions only in select items,
/// named-window references only against `wins`.
#[derive(Clone, Copy)]
struct ExprCtx<'a> {
    allow_agg: bool,
    allow_window: bool,
    wins: &'a [String],
}

impl ExprCtx<'static> {
    /// WHERE / ON / GROUP BY / window definitions / aggregate arguments.
    fn plain() -> ExprCtx<'static> {
        ExprCtx { allow_agg: false, allow_window: false, wins: &[] }
    }
}

fn check_expr(
    e: &Expr,
    env: &Env,
    catalog: &Catalog,
    ctes: &[(String, SelectStmt)],
    ctx: ExprCtx,
    errs: &mut Vec<String>,
) {
    match e {
        Expr::ColRef { alias, name } => match env.iter().find(|(a, _)| a == alias) {
            None => errs.push(format!("colref {alias}.{name}: alias not in scope")),
            Some((_, cols)) if !cols.contains(name) => {
                errs.push(format!("colref {alias}.{name}: no such column"))
            }
            Some(_) => {}
        },
        Expr::Lit { .. } | Expr::Null { .. } => {}
        Expr::Unary { arg, .. } => check_expr(arg, env, catalog, ctes, ctx, errs),
        Expr::Binary { lhs, rhs, .. } => {
            check_expr(lhs, env, catalog, ctes, ctx, errs);
            check_expr(rhs, env, catalog, ctes, ctx, errs);
        }
        Expr::Func { args, .. } => {
            for a in args {
                check_expr(a, env, catalog, ctes, ctx, errs);
            }
        }
        Expr::Case { cond, then_e, else_e } => {
            check_expr(cond, env, catalog, ctes, ctx, errs);
            check_expr(then_e, env, catalog, ctes, ctx, errs);
            check_expr(else_e, env, catalog, ctes, ctx, errs);
        }
        Expr::Cast { arg, .. } => check_expr(arg, env, catalog, ctes, ctx, errs),
        Expr::IsNull { arg, .. } => check_expr(arg, env, catalog, ctes, ctx, errs),
        Expr::Wrap { arg, .. } => check_expr(arg, env, catalog, ctes, ctx, errs),
        Expr::ScalarSubq { body } => check_stmt(body, env, catalog, ctes, false, errs),
        Expr::InSubq { lhs, body, .. } => {
            check_expr(lhs, env, catalog, ctes, ctx, errs);
            check_stmt(body, env, catalog, ctes, false, errs);
        }
        Expr::Exists { body, .. } => check_stmt(body, env, catalog, ctes, false, errs),
        Expr::Agg { name, star, args, order_within, filter, .. } => {
            if !ctx.allow_agg {
                errs.push(format!("aggregate {name} in an illegal context"));
            }
            if *star && !args.is_empty() {
                errs.push(format!("aggregate {name}: star with arguments"));
            }
            // No nested aggregates or windows inside an aggregate.
            for a in args {
                check_expr(a, env, catalog, ctes, ExprCtx::plain(), errs);
            }
            if let Some((oe, _)) = order_within {
                check_expr(oe, env, catalog, ctes, ExprCtx::plain(), errs);
            }
            if let Some(f) = filter {
                check_expr(f, env, catalog, ctes, ExprCtx::plain(), errs);
            }
        }
        Expr::WindowFunc { name, args, over } => {
            if !ctx.allow_window {
                errs.push(format!("window function {name} in an illegal context"));
            }
            for a in args {
                check_expr(a, env, catalog, ctes, ExprCtx::plain(), errs);
            }
            match over {
                WindowOver::Named(n) => {
                    if !ctx.wins.contains(n) {
                        errs.push(format!("window function {name}: unknown window {n}"));
                    }
                }
                WindowOver::Inline(def) => {
                    check_window_def(def, env, catalog, ctes, errs);
                }
            }
        }
    }
}

fn check_window_def(
    def: &WindowDef,
    env: &Env,
    catalog: &Catalog,
    ctes: &[(String, SelectStmt)],
    errs: &mut Vec<String>,
) {
    for e in &def.partition_by {
        check_expr(e, env, catalog, ctes, ExprCtx::plain(), errs);
    }
    for k in &def.order_by {
        check_expr(&k.expr, env, catalog, ctes, ExprCtx::plain(), errs);
    }
}

/// Does the expression contain an aggregate call at any depth?
fn contains_agg(e: &Expr) -> bool {
    matches!(e, Expr::Agg { .. }) || e.children().into_iter().any(contains_agg)
}

/// Grouped-projection rule: outside aggregate calls, an expression may
/// only reference columns through a subtree that IS one of the group
/// expressions (matched on rendered text — the generator clones group
/// keys verbatim, and PostgreSQL matches structurally).
fn check_grouped(e: &Expr, group_texts: &[String], errs: &mut Vec<String>) {
    if group_texts.contains(&e.to_sql()) {
        return;
    }
    match e {
        Expr::Agg { .. } => {} // aggregate arguments may reference anything
        Expr::ColRef { alias, name } => {
            errs.push(format!("ungrouped column reference {alias}.{name}"))
        }
        Expr::WindowFunc { name, .. } => {
            errs.push(format!("window function {name} in a grouped statement"))
        }
        _ => {
            for c in e.children() {
                check_grouped(c, group_texts, errs);
            }
        }
    }
}

fn check_stmt(
    stmt: &SelectStmt,
    outer: &Env,
    catalog: &Catalog,
    outer_ctes: &[(String, SelectStmt)],
    top: bool,
    errs: &mut Vec<String>,
) {
    if !top {
        // Nested statements never carry ORDER BY/LIMIT/OFFSET (the differ's
        // ordered-compare detection is statement-level and textual), CTEs,
        // grouping, or windows.
        if !stmt.order_by.is_empty() || stmt.limit.is_some() || stmt.offset.is_some() {
            errs.push("nested statement carries ORDER BY/LIMIT/OFFSET".to_string());
        }
        if !stmt.ctes.is_empty() {
            errs.push("nested statement carries CTEs".to_string());
        }
        if stmt.group_by.is_some() || stmt.having.is_some() || !stmt.windows.is_empty() {
            errs.push("nested statement carries GROUP BY/HAVING/WINDOW".to_string());
        }
    }
    let ctes: &[(String, SelectStmt)] = if top { &stmt.ctes } else { outer_ctes };
    for (_, body) in &stmt.ctes {
        // CTE bodies must not reference the enclosing query.
        check_stmt(body, &Vec::new(), catalog, &[], false, errs);
    }
    let mut env = outer.clone();
    if let Some(f) = &stmt.from {
        check_from(f, outer, &mut env, catalog, ctes, errs);
    }
    let win_names: Vec<String> = stmt.windows.iter().map(|(n, _)| n.clone()).collect();
    let item_ctx = ExprCtx { allow_agg: true, allow_window: true, wins: &win_names };
    for item in &stmt.items {
        check_expr(&item.expr, &env, catalog, ctes, item_ctx, errs);
    }
    if let Some(w) = &stmt.where_clause {
        check_expr(w, &env, catalog, ctes, ExprCtx::plain(), errs);
    }
    if let Some(gb) = &stmt.group_by {
        for e in gb.exprs() {
            check_expr(e, &env, catalog, ctes, ExprCtx::plain(), errs);
        }
    }
    if let Some(h) = &stmt.having {
        // Aggregates are legal in HAVING; window functions are not.
        check_expr(
            h,
            &env,
            catalog,
            ctes,
            ExprCtx { allow_agg: true, allow_window: false, wins: &[] },
            errs,
        );
    }
    for (_, def) in &stmt.windows {
        check_window_def(def, &env, catalog, ctes, errs);
    }
    // Grouped statements project only group keys and aggregates; a plain
    // aggregation (no GROUP BY, aggregates present) grants no column refs
    // outside aggregates either. HAVING follows the same rule.
    if stmt.group_by.is_some() || stmt.items.iter().any(|i| contains_agg(&i.expr)) {
        let group_texts: Vec<String> = stmt
            .group_by
            .iter()
            .flat_map(|gb| gb.exprs())
            .map(|e| e.to_sql())
            .collect();
        for item in &stmt.items {
            check_grouped(&item.expr, &group_texts, errs);
        }
        if let Some(h) = &stmt.having {
            check_grouped(h, &group_texts, errs);
        }
    }
    for k in &stmt.order_by {
        if k.position == 0 || k.position > stmt.items.len() {
            errs.push(format!("ORDER BY position {} out of range", k.position));
        }
    }
}

/// All scoping/shape violations in a generated statement (empty = clean):
/// every column reference resolves to a lexically visible alias and column,
/// derived-table/CTE bodies are uncorrelated, USING columns exist on both
/// sides, ORDER BY positions are in range, and nested statements carry no
/// ORDER BY/LIMIT/CTEs.
pub fn scope_errors(stmt: &SelectStmt, catalog: &Catalog) -> Vec<String> {
    let mut errs = Vec::new();
    check_stmt(stmt, &Vec::new(), catalog, &[], true, &mut errs);
    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};

    fn colref(alias: &str, name: &str, ty: SqlType) -> SelectItem {
        SelectItem {
            expr: Expr::ColRef { alias: alias.to_string(), name: name.to_string() },
            alias: None,
            ty,
        }
    }

    #[test]
    fn renders_join_using_and_suffix() {
        let stmt = SelectStmt {
            ctes: vec![],
            items: vec![colref("t0", "k_int", SqlType::Int4)],
            from: Some(FromItem::Join {
                left: Box::new(FromItem::Table {
                    name: "fz_scalar".to_string(),
                    alias: "t0".to_string(),
                }),
                right: Box::new(FromItem::Table {
                    name: "fz_mixed".to_string(),
                    alias: "t1".to_string(),
                }),
                kind: JoinKind::Left,
                cond: JoinCond::Using(vec!["k_int".to_string()]),
            }),
            where_clause: None,
            order_by: vec![OrderKey { position: 1, desc: Some(true), nulls_first: Some(false) }],
            limit: Some("1".to_string()),
            offset: Some("0".to_string()),
            ..Default::default()
        };
        assert_eq!(
            stmt.to_sql(),
            "SELECT t0.k_int FROM fz_scalar AS t0 LEFT JOIN fz_mixed AS t1 \
             USING (k_int) ORDER BY 1 DESC NULLS LAST LIMIT 1 OFFSET 0;"
        );
        let cat = FixtureCatalog.load_catalog().unwrap();
        assert!(scope_errors(&stmt, &cat).is_empty());
    }

    #[test]
    fn renders_cte_and_natural_join() {
        let body = SelectStmt {
            items: vec![SelectItem {
                expr: Expr::ColRef { alias: "t0".to_string(), name: "k_int".to_string() },
                alias: Some("c0".to_string()),
                ty: SqlType::Int4,
            }],
            from: Some(FromItem::Table {
                name: "fz_one".to_string(),
                alias: "t0".to_string(),
            }),
            ..Default::default()
        };
        let stmt = SelectStmt {
            ctes: vec![("w0".to_string(), body)],
            items: vec![colref("t1", "c0", SqlType::Int4)],
            from: Some(FromItem::Table { name: "w0".to_string(), alias: "t1".to_string() }),
            ..Default::default()
        };
        assert_eq!(
            stmt.to_sql(),
            "WITH w0 AS (SELECT t0.k_int AS c0 FROM fz_one AS t0) \
             SELECT t1.c0 FROM w0 AS t1;"
        );
        let cat = FixtureCatalog.load_catalog().unwrap();
        assert!(scope_errors(&stmt, &cat).is_empty());

        let nat = SelectStmt {
            items: vec![colref("t0", "k_text", SqlType::Text)],
            from: Some(FromItem::Join {
                left: Box::new(FromItem::Table {
                    name: "fz_wide".to_string(),
                    alias: "t0".to_string(),
                }),
                right: Box::new(FromItem::Table {
                    name: "fz_empty".to_string(),
                    alias: "t1".to_string(),
                }),
                kind: JoinKind::Full,
                cond: JoinCond::Natural,
            }),
            ..Default::default()
        };
        assert!(nat.to_sql().contains("NATURAL FULL JOIN fz_empty AS t1"));
        assert!(scope_errors(&nat, &cat).is_empty());
    }

    #[test]
    fn scope_errors_catch_violations() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        // Alias not in scope.
        let bad = SelectStmt {
            items: vec![colref("t9", "k_int", SqlType::Int4)],
            from: Some(FromItem::Table {
                name: "fz_scalar".to_string(),
                alias: "t0".to_string(),
            }),
            ..Default::default()
        };
        assert!(!scope_errors(&bad, &cat).is_empty());
        // Column not on the relation.
        let bad = SelectStmt {
            items: vec![colref("t0", "nope", SqlType::Int4)],
            from: Some(FromItem::Table {
                name: "fz_scalar".to_string(),
                alias: "t0".to_string(),
            }),
            ..Default::default()
        };
        assert!(!scope_errors(&bad, &cat).is_empty());
        // Correlated derived table (must be flagged).
        let corr_body = SelectStmt {
            items: vec![colref("t0", "k_int", SqlType::Int4)],
            ..Default::default()
        };
        let bad = SelectStmt {
            items: vec![colref("t0", "k_int", SqlType::Int4)],
            from: Some(FromItem::Join {
                left: Box::new(FromItem::Table {
                    name: "fz_scalar".to_string(),
                    alias: "t0".to_string(),
                }),
                right: Box::new(FromItem::Derived {
                    body: Box::new(corr_body),
                    alias: "t1".to_string(),
                    columns: vec!["c0".to_string()],
                }),
                kind: JoinKind::Inner,
                cond: JoinCond::Cross,
            }),
            ..Default::default()
        };
        assert!(!scope_errors(&bad, &cat).is_empty());
        // ORDER BY out of range.
        let bad = SelectStmt {
            items: vec![colref("t0", "k_int", SqlType::Int4)],
            from: Some(FromItem::Table {
                name: "fz_scalar".to_string(),
                alias: "t0".to_string(),
            }),
            order_by: vec![OrderKey { position: 2, desc: None, nulls_first: None }],
            ..Default::default()
        };
        assert!(!scope_errors(&bad, &cat).is_empty());
    }
}
