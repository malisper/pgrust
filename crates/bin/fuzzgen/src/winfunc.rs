//! Builtin window-FUNCTION drain: the argument- and result-edge regimes of
//! the window functions themselves (nodeWindowAgg's `eval_windowfunction`
//! dispatch and the `WinGetFuncArgIn{Partition,Frame}` seek helpers), plus
//! the `windowfuncs` prosupport surface reached when the planner sees these
//! functions.
//!
//! This complements the `win` module, which drives the always-valid,
//! always-in-range regime (ranking, displacement, positional picks over a
//! total order, and the frame MODE/EXCLUDE matrix). Two things `win` never
//! generates and this module owns:
//!
//!   * `percent_rank()` / `cume_dist()` — absent from `win`'s production
//!     set entirely, so both the `WfKind::PercentRank`/`CumeDist` executor
//!     arms and their prosupport functions are otherwise hollow.
//!   * the ARGUMENT edges that `win` deliberately steers around: a NULL or
//!     out-of-range bucket/nth/offset argument. These take three distinct
//!     paths — a NULL argument yields a NULL result for every row, a
//!     non-positive `ntile`/`nth_value` argument raises the argument error,
//!     and a huge offset/nth seeks out of the partition or frame (NULL, or
//!     the lead/lag default). Every one is deterministic regardless of row
//!     order, which is what lets them ride even the keyless fallback source.
//!
//! Determinism discipline (identical to `win`): a probe whose RESULT
//! depends on the exact within-partition order — `ntile` bucket numbers,
//! in-range displacement, the framed positional picks — is emitted only
//! when a total window order is constructible (every relation carries its
//! unique key). NULL/error-regime probes are order-independent and so are
//! always available. `percent_rank`/`cume_dist` are peer-based (ties share
//! a value), deterministic under any order, and float-typed so the
//! differ's ulp comparison applies.

use crate::catalog::SqlType;
use crate::expr::Expr;
use crate::render::{SelectItem, SelectStmt, WindowOver};
use crate::scope::{Scope, ScopeRel};
use crate::stmt::{order_limit_suffix, Gen};
use crate::win::gen_window_def;

/// Probes whose result is order-independent (NULL for every row, or an
/// argument error): always available, even over the keyless source.
const PLAIN_PROBES: &[&str] = &[
    "winfunc:pct_rank",
    "winfunc:cume_dist",
    "winfunc:ntile_null",
    "winfunc:ntile_err",
    "winfunc:nth_null",
    "winfunc:nth_err",
    "winfunc:leadlag_nulloff",
];

/// Probes whose result depends on the within-partition order: emitted only
/// when a total window order is available.
const TOTAL_PROBES: &[&str] = &[
    "winfunc:ntile_one",
    "winfunc:ntile_big",
    "winfunc:leadlag_negoff",
    "winfunc:leadlag_bigoff",
    "winfunc:leadlag_default",
    "winfunc:nth_big",
    "winfunc:firstlast_frame",
];

/// Window functions whose result is a float surface (ulp compare).
const FLOAT_PROBES: &[&str] = &["winfunc:pct_rank", "winfunc:cume_dist"];

pub fn gen_winfunc_stmt(g: &mut Gen) -> SelectStmt {
    g.fire("select");
    // Single-table source; prefer a unique-keyed table so a total window
    // order (and therefore the order-sensitive probes) is available.
    let table = {
        let keyed: Vec<_> = g.catalog.tables.iter().filter(|t| t.unique_key.is_some()).collect();
        if !keyed.is_empty() && g.rng.chance(3, 4) {
            *g.rng.pick(&keyed)
        } else {
            g.pick_table()
        }
    };
    let alias = g.next_alias();
    let rels = vec![ScopeRel::from_table(table, alias.clone())];
    let mut uks: Vec<(String, String)> = Vec::new();
    if let Some(uk) = &table.unique_key {
        uks.push((alias.clone(), uk.clone()));
    }
    let total_possible = uks.len() == rels.len();
    let from = crate::render::FromItem::Table { name: table.name.clone(), alias };
    let scope = Scope { rels: &rels, outer: None };

    // A plain passthrough column alongside the window column.
    let mut items: Vec<SelectItem> = Vec::new();
    let pty = g.any_type(&scope);
    let pexpr = g.gen_typed(&scope, pty, 2);
    items.push(SelectItem { expr: pexpr, alias: None, ty: pty });

    items.push(gen_winfunc_item(g, &scope, &uks, total_possible));

    let where_clause = if g.rng.chance(1, 2) {
        g.fire("where");
        Some(g.gen_bool(&scope, g.max_depth))
    } else {
        None
    };

    let mut stmt = SelectStmt {
        items,
        from: Some(from),
        where_clause,
        ..Default::default()
    };
    order_limit_suffix(g, &mut stmt);
    stmt
}

/// One window-function edge-probe select item.
fn gen_winfunc_item(
    g: &mut Gen,
    scope: &Scope,
    uks: &[(String, String)],
    total_possible: bool,
) -> SelectItem {
    let mut opts: Vec<&'static str> = PLAIN_PROBES.to_vec();
    if total_possible {
        opts.extend_from_slice(TOTAL_PROBES);
    }
    let picked = g.weights.pick(g.rng, &opts);
    g.fire(picked);
    let needs_total = TOTAL_PROBES.contains(&picked);

    // The OVER definition: order-sensitive probes ride the total order and
    // may carry a frame; order-independent probes take whatever order the
    // source affords (frames add nothing to their outcome).
    let over = if needs_total {
        WindowOver::Inline(gen_window_def(g, scope, uks, true, true))
    } else {
        WindowOver::Inline(gen_window_def(g, scope, uks, total_possible, false))
    };

    let (name, args, ty): (&'static str, Vec<Expr>, SqlType) = match picked {
        "winfunc:pct_rank" => ("percent_rank", Vec::new(), SqlType::Float8),
        "winfunc:cume_dist" => ("cume_dist", Vec::new(), SqlType::Float8),
        // ntile(NULL) -> NULL for every row (no bucketing computed).
        "winfunc:ntile_null" => {
            ("ntile", vec![Expr::Null { ty: SqlType::Int4 }], SqlType::Int4)
        }
        // ntile(0) / ntile(-k) -> "argument of ntile must be greater than
        // zero" (error identity).
        "winfunc:ntile_err" => {
            let n = if g.rng.chance(1, 2) { 0 } else { -(1 + g.rng.below(3) as i64) };
            ("ntile", vec![lit(n)], SqlType::Int4)
        }
        // Single bucket: boundary math with nbuckets == 1.
        "winfunc:ntile_one" => ("ntile", vec![lit(1)], SqlType::Int4),
        // nbuckets far larger than the row count: the boundary<=0 clamp and
        // the remainder-carry path (one row per bucket, trailing empties).
        "winfunc:ntile_big" => {
            let n = 50 + g.rng.below(950) as i64;
            ("ntile", vec![lit(n)], SqlType::Int4)
        }
        // lead/lag(x, NULL) -> NULL for every row.
        "winfunc:leadlag_nulloff" => {
            let (x, xty) = typed_colref(g, scope);
            (leadlag(g), vec![x, Expr::Null { ty: SqlType::Int4 }], xty)
        }
        // Negative offset flips the seek direction (lag<->lead) — exercises
        // the partition seek with a below-current absolute position.
        "winfunc:leadlag_negoff" => {
            let (x, xty) = typed_colref(g, scope);
            (leadlag(g), vec![x, lit(-(1 + g.rng.below(3) as i64))], xty)
        }
        // Offset past the partition edge -> out of partition -> NULL.
        "winfunc:leadlag_bigoff" => {
            let (x, xty) = typed_colref(g, scope);
            (leadlag(g), vec![x, lit(1_000 + g.rng.below(1000) as i64)], xty)
        }
        // Three-arg form: an in-range offset returns the neighbour, an
        // out-of-range one returns the supplied default (isout && default).
        "winfunc:leadlag_default" => {
            let (x, xty) = typed_colref(g, scope);
            let off = if g.rng.chance(1, 2) {
                lit(1 + g.rng.below(3) as i64)
            } else {
                lit(1_000 + g.rng.below(1000) as i64)
            };
            let default = if g.rng.chance(1, 3) {
                Expr::Null { ty: xty }
            } else {
                Expr::Lit { sql: g.gen_literal(xty) }
            };
            (leadlag(g), vec![x, off, default], xty)
        }
        // nth_value(x, NULL) -> NULL for every row.
        "winfunc:nth_null" => {
            let (x, xty) = typed_colref(g, scope);
            ("nth_value", vec![x, Expr::Null { ty: SqlType::Int4 }], xty)
        }
        // nth_value(x, 0) / nth_value(x, -k) -> argument error (identity).
        "winfunc:nth_err" => {
            let (x, xty) = typed_colref(g, scope);
            let n = if g.rng.chance(1, 2) { 0 } else { -(1 + g.rng.below(3) as i64) };
            ("nth_value", vec![x, lit(n)], xty)
        }
        // nth beyond the frame -> NULL (in-frame seek returns "out").
        "winfunc:nth_big" => {
            let (x, xty) = typed_colref(g, scope);
            let n = 50 + g.rng.below(950) as i64;
            ("nth_value", vec![x, lit(n)], xty)
        }
        // first_value/last_value ride the explicit frame in `over`;
        // last_value is only interesting with a full-partition frame, which
        // gen_window_def offers under allow_frame.
        "winfunc:firstlast_frame" => {
            let (x, xty) = typed_colref(g, scope);
            let name = if g.rng.chance(1, 2) { "first_value" } else { "last_value" };
            (name, vec![x], xty)
        }
        other => unreachable!("unknown winfunc production {other}"),
    };

    // percent_rank/cume_dist are always float surfaces; the value functions
    // are float only when their value column is (both correctly flow to the
    // differ's ulp compare via `ty`).
    debug_assert!(
        !FLOAT_PROBES.contains(&picked) || ty == SqlType::Float8,
        "rank-family probe {picked} must be float-typed"
    );

    SelectItem { expr: Expr::WindowFunc { name, args, over }, alias: None, ty }
}

/// A random in-scope column plus its type (for the displacement/default
/// value argument, whose default must be coercible to the value type).
fn typed_colref(g: &mut Gen, scope: &Scope) -> (Expr, SqlType) {
    let ty = g.any_type(scope);
    let cols = scope.columns_of_type(ty);
    debug_assert!(!cols.is_empty(), "any_type returned a type with no columns");
    let (alias, c) = *g.rng.pick(&cols);
    (Expr::ColRef { alias: alias.to_string(), name: c.name.clone() }, ty)
}

/// A bare integer literal (offsets/bucket counts render verbatim; negatives
/// print with their sign, which PostgreSQL parses as a signed constant).
fn lit(n: i64) -> Expr {
    Expr::Lit { sql: n.to_string() }
}

fn leadlag(g: &mut Gen) -> &'static str {
    if g.rng.chance(1, 2) {
        "lag"
    } else {
        "lead"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, CatalogSource, FixtureCatalog};
    use crate::render::{scope_errors, FromItem, WindowDef};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    // Bias every probe family up so the rarer ones reliably appear.
    fn biased() -> WeightTable {
        WeightTable::parse(
            "winfunc:pct_rank=3,winfunc:cume_dist=3,winfunc:ntile_null=3,\
             winfunc:ntile_err=3,winfunc:ntile_one=3,winfunc:ntile_big=3,\
             winfunc:leadlag_nulloff=3,winfunc:leadlag_negoff=3,\
             winfunc:leadlag_bigoff=3,winfunc:leadlag_default=3,\
             winfunc:nth_null=3,winfunc:nth_err=3,winfunc:nth_big=3,\
             winfunc:firstlast_frame=3",
        )
        .unwrap()
    }

    #[test]
    fn winfunc_statements_are_scoped_and_varied() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = biased();
        let mut rng = Rng::new(0x5EED_1234);
        let mut sqls = String::new();
        for i in 0..1200 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmt = gen_winfunc_stmt(&mut g);
            let errs = scope_errors(&stmt, &cat);
            assert!(errs.is_empty(), "stmt {i}: {errs:?}\n{}", stmt.to_sql());
            sqls.push_str(&stmt.to_sql());
            sqls.push('\n');
        }
        for frag in [
            "percent_rank() OVER",
            "cume_dist() OVER",
            "ntile((NULL::int4))",
            "ntile(0)",
            "ntile(1)",
            "nth_value(",
            "(NULL::int4))",
            "lag(",
            "lead(",
            "first_value(",
            "last_value(",
            " OVER (",
        ] {
            assert!(sqls.contains(frag), "winfunc flavor {frag:?} never generated");
        }
        // Both non-positive argument errors and negative offsets appear.
        assert!(sqls.contains("ntile(0)") || sqls.contains("ntile(-"));
        assert!(sqls.contains(", -"), "no negative offset/nth argument generated");
    }

    /// Order-sensitive probes only ride a window ORDER BY that ends with
    /// every relation's unique key (total within partitions) — the same
    /// determinism invariant `win` enforces.
    #[test]
    fn order_sensitive_probes_are_total() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = biased();
        let mut rng = Rng::new(24680);
        let mut checked = 0;
        for _ in 0..1500 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmt = gen_winfunc_stmt(&mut g);
            // Which probe fired?
            let probe = prods.iter().find(|p| p.starts_with("winfunc:")).cloned();
            let Some(probe) = probe else { continue };
            if !TOTAL_PROBES.contains(&probe.as_str()) {
                continue;
            }
            checked += 1;
            let rel_uks = rel_unique_keys(stmt.from.as_ref().unwrap(), &cat);
            let def = window_def_of(&stmt);
            for (alias, uk) in &rel_uks {
                let uk = uk.as_ref().unwrap_or_else(|| {
                    panic!("order-sensitive {probe} over keyless relation {alias}: {}", stmt.to_sql())
                });
                assert!(
                    def.order_by.iter().any(|k| matches!(
                        &k.expr,
                        Expr::ColRef { alias: a, name: n } if a == alias && n == uk
                    )),
                    "window order for {probe} misses unique key {alias}.{uk}: {}",
                    stmt.to_sql()
                );
            }
        }
        assert!(checked > 100, "order-sensitive probes rarely generated ({checked})");
    }

    /// percent_rank/cume_dist are always generated over some ORDER BY (a
    /// bare unordered peer set is a degenerate, low-value shape).
    #[test]
    fn rank_family_probes_appear() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = biased();
        let mut rng = Rng::new(1357);
        let mut pct = 0;
        let mut cume = 0;
        for _ in 0..800 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmt = gen_winfunc_stmt(&mut g);
            let sql = stmt.to_sql();
            if sql.contains("percent_rank()") {
                pct += 1;
            }
            if sql.contains("cume_dist()") {
                cume += 1;
            }
        }
        assert!(pct > 10 && cume > 10, "rank-family probes rare: pct={pct} cume={cume}");
    }

    fn window_def_of(stmt: &SelectStmt) -> &WindowDef {
        for item in &stmt.items {
            if let Expr::WindowFunc { over: WindowOver::Inline(d), .. } = &item.expr {
                return d;
            }
        }
        panic!("no inline window function in statement: {}", stmt.to_sql());
    }

    fn rel_unique_keys(item: &FromItem, cat: &Catalog) -> Vec<(String, Option<String>)> {
        match item {
            FromItem::Table { name, alias } => {
                let t = cat.tables.iter().find(|t| &t.name == name).expect("fixture table");
                vec![(alias.clone(), t.unique_key.clone())]
            }
            FromItem::Join { left, right, .. } => {
                let mut out = rel_unique_keys(left, cat);
                out.extend(rel_unique_keys(right, cat));
                out
            }
            FromItem::Derived { .. } => panic!("winfunc generates no derived tables"),
        }
    }
}
