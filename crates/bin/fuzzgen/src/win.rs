//! Window-functions statement module: ranking (row_number/rank/dense_rank/
//! ntile), displacement (lag/lead with optional offset and default),
//! positional value picks (first_value/last_value/nth_value), and
//! aggregates-over-windows (count/sum/min/max OVER); PARTITION BY 0-2 keys,
//! window ORDER BY, ROWS/RANGE frame clauses, and an occasional named
//! WINDOW clause shared by the statement's window functions.
//!
//! Determinism discipline: position-dependent window functions
//! (row_number, ntile, lag, lead, first_value, last_value, nth_value) and
//! explicit frame clauses are only generated with a window ORDER BY that is
//! total within each partition — the order keys end with the unique key of
//! every relation in the row source (the fixture's per-table unique
//! columns; over a join, the combination of both). Row sources without a
//! unique key (or windows whose order omits one) fall back to the
//! tie-insensitive forms: rank/dense_rank (equal sort keys share a rank)
//! and default-frame aggregates (peer rows enter the frame together, and
//! their accumulation is exact — window sum/avg arguments are restricted
//! to the integer/numeric families precisely so no float accumulation
//! rides on an underdetermined order).

use crate::catalog::{SqlType, Table};
use crate::expr::Expr;
use crate::render::{
    Frame, FrameBound, FromItem, JoinCond, JoinKind, SelectItem, SelectStmt, WinOrderKey,
    WindowDef, WindowOver,
};
use crate::scope::{Scope, ScopeRel};
use crate::stmt::{order_limit_suffix, Gen};

/// Position-dependent window functions: their output depends on the exact
/// row order within each partition, so they require a total window order.
pub const ORDER_SENSITIVE_WINDOW_FNS: &[&str] = &[
    "row_number",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "nth_value",
];

const SENSITIVE_PRODS: &[&str] = &[
    "win:row_number",
    "win:ntile",
    "win:lag",
    "win:lead",
    "win:first_value",
    "win:last_value",
    "win:nth_value",
];

const INSENSITIVE_PRODS: &[&str] =
    &["win:rank", "win:dense_rank", "win:count", "win:sum", "win:min", "win:max"];

const MINMAX_TYPES: &[SqlType] = &[
    SqlType::Int2,
    SqlType::Int4,
    SqlType::Int8,
    SqlType::Float4,
    SqlType::Float8,
    SqlType::Numeric,
    SqlType::Text,
    SqlType::Varchar,
    SqlType::Date,
    SqlType::Timestamp,
];

pub fn gen_win_stmt(g: &mut Gen) -> SelectStmt {
    g.fire("select");
    // Row source: a single table, or a two-table join of unique-keyed
    // tables (the combined unique keys keep a total order available).
    let keyed: Vec<&Table> =
        g.catalog.tables.iter().filter(|t| t.unique_key.is_some()).collect();
    let join = !keyed.is_empty()
        && g.weights.pick(g.rng, &["win:single", "win:join"]) == "win:join";
    let mut rels: Vec<ScopeRel> = Vec::new();
    // (alias, unique-key column) per relation that has one.
    let mut uks: Vec<(String, String)> = Vec::new();
    let from = if join {
        g.fire("win:join");
        let ta = keyed[g.rng.below_usize(keyed.len())];
        let tb = keyed[g.rng.below_usize(keyed.len())];
        let aa = g.next_alias();
        let ab = g.next_alias();
        rels.push(ScopeRel::from_table(ta, aa.clone()));
        rels.push(ScopeRel::from_table(tb, ab.clone()));
        uks.push((aa.clone(), ta.unique_key.clone().expect("keyed")));
        uks.push((ab.clone(), tb.unique_key.clone().expect("keyed")));
        let kind = if g.weights.pick(g.rng, &["win:join:inner", "win:join:left"])
            == "win:join:left"
        {
            g.fire("win:join:left");
            JoinKind::Left
        } else {
            g.fire("win:join:inner");
            JoinKind::Inner
        };
        // Every fixture table carries the k_int join key. Under LEFT JOIN
        // the combined key stays unique: unmatched left rows appear once,
        // NULL-extended, and differ in the left key.
        let on = Expr::Binary {
            op: "=",
            lhs: Box::new(Expr::ColRef { alias: aa, name: "k_int".to_string() }),
            rhs: Box::new(Expr::ColRef { alias: ab, name: "k_int".to_string() }),
        };
        FromItem::Join {
            left: Box::new(FromItem::Table { name: ta.name.clone(), alias: rels[0].alias.clone() }),
            right: Box::new(FromItem::Table { name: tb.name.clone(), alias: rels[1].alias.clone() }),
            kind,
            cond: JoinCond::On(on),
        }
    } else {
        g.fire("win:single");
        let table = g.pick_table();
        let alias = g.next_alias();
        rels.push(ScopeRel::from_table(table, alias.clone()));
        if let Some(uk) = &table.unique_key {
            uks.push((alias.clone(), uk.clone()));
        }
        FromItem::Table { name: table.name.clone(), alias }
    };
    // A total window order is constructible iff every relation has a
    // unique key (fz_scalar deliberately has none — the fallback path).
    let total_possible = uks.len() == rels.len();
    let scope = Scope { rels: &rels, outer: None };

    // Plain output columns alongside the window columns.
    let mut items: Vec<SelectItem> = Vec::new();
    let nplain = 1 + g.rng.below_usize(2);
    for _ in 0..nplain {
        let ty = g.any_type(&scope);
        let expr = g.gen_typed(&scope, ty, 2);
        items.push(SelectItem { expr, alias: None, ty });
    }

    // Occasional named WINDOW clause shared by the window items. Named
    // definitions carry no frame (references cannot extend one) and are
    // total whenever the row source allows.
    let mut windows: Vec<(String, WindowDef)> = Vec::new();
    let named: Option<(String, bool)> =
        if g.weights.pick(g.rng, &["win:named", "win:named:none"]) == "win:named" {
            g.fire("win:named");
            let name = g.next_cte_name();
            let def = gen_window_def(g, &scope, &uks, total_possible, false);
            windows.push((name.clone(), def));
            Some((name, total_possible))
        } else {
            None
        };

    let nwin = 1 + usize::from(g.rng.chance(1, 3));
    for _ in 0..nwin {
        items.push(gen_window_item(g, &scope, &uks, total_possible, named.as_ref()));
    }

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
        windows,
        ..Default::default()
    };
    order_limit_suffix(g, &mut stmt);
    stmt
}

/// One window-function select item.
fn gen_window_item(
    g: &mut Gen,
    scope: &Scope,
    uks: &[(String, String)],
    total_possible: bool,
    named: Option<&(String, bool)>,
) -> SelectItem {
    // Order-sensitive functions are offered only when a total window order
    // is available (inline) or the named window is total.
    let named_total = named.is_some_and(|(_, t)| *t);
    let can_sensitive = if named.is_some() { named_total } else { total_possible };
    let mut opts: Vec<&'static str> = INSENSITIVE_PRODS.to_vec();
    if can_sensitive {
        opts.extend_from_slice(SENSITIVE_PRODS);
    }
    let picked = g.weights.pick(g.rng, &opts);
    g.fire(picked);
    let fname: &'static str = &picked["win:".len()..];
    let sensitive = SENSITIVE_PRODS.contains(&picked);

    let (args, ty) = match picked {
        "win:row_number" | "win:rank" | "win:dense_rank" => (Vec::new(), SqlType::Int8),
        "win:ntile" => {
            let n = 1 + g.rng.below(4);
            (vec![Expr::Lit { sql: n.to_string() }], SqlType::Int4)
        }
        "win:count" => {
            if g.rng.chance(1, 3) {
                // count(*) OVER — rendered through a literal star argument.
                (vec![Expr::Lit { sql: "*".to_string() }], SqlType::Int8)
            } else {
                let t = g.any_type(scope);
                (vec![g.gen_typed(scope, t, 2)], SqlType::Int8)
            }
        }
        "win:sum" => {
            // Integer/numeric only: exact accumulation, safe even when the
            // window order underdetermines the accumulation order.
            let t = *g
                .rng
                .pick(&[SqlType::Int2, SqlType::Int4, SqlType::Int8, SqlType::Numeric]);
            let ty = match t {
                SqlType::Int2 | SqlType::Int4 => SqlType::Int8,
                _ => SqlType::Numeric,
            };
            (vec![g.gen_typed(scope, t, 2)], ty)
        }
        "win:min" | "win:max" => {
            let t = *g.rng.pick(MINMAX_TYPES);
            let ty = if t == SqlType::Varchar { SqlType::Text } else { t };
            (vec![g.gen_typed(scope, t, 2)], ty)
        }
        "win:lag" | "win:lead" => {
            let t = g.any_type(scope);
            let e = g.gen_typed(scope, t, 2);
            let mut args = vec![e];
            match g.rng.below(3) {
                0 => {}
                1 => args.push(Expr::Lit { sql: g.rng.below(4).to_string() }),
                _ => {
                    args.push(Expr::Lit { sql: g.rng.below(4).to_string() });
                    args.push(Expr::Lit { sql: g.gen_literal(t) });
                }
            }
            (args, t)
        }
        "win:first_value" | "win:last_value" => {
            let t = g.any_type(scope);
            (vec![g.gen_typed(scope, t, 2)], t)
        }
        "win:nth_value" => {
            let t = g.any_type(scope);
            let n = 1 + g.rng.below(4);
            (vec![g.gen_typed(scope, t, 2), Expr::Lit { sql: n.to_string() }], t)
        }
        other => unreachable!("unknown window production {}", other),
    };

    let over = match named {
        Some((name, _)) if g.rng.chance(2, 3) => WindowOver::Named(name.clone()),
        _ => {
            // Inline definition: sensitive functions demand totality;
            // insensitive ones get a total order half the time it exists.
            let total = total_possible && (sensitive || g.rng.chance(1, 2));
            WindowOver::Inline(gen_window_def(g, scope, uks, total, total))
        }
    };
    SelectItem { expr: Expr::WindowFunc { name: fname, args, over }, alias: None, ty }
}

/// One window definition. `total` appends every relation's unique key to
/// the ORDER BY, making the order total within each partition; frames are
/// offered only on total inline windows (`allow_frame`).
pub(crate) fn gen_window_def(
    g: &mut Gen,
    scope: &Scope,
    uks: &[(String, String)],
    total: bool,
    allow_frame: bool,
) -> WindowDef {
    debug_assert!(!total || !uks.is_empty());
    let npart = match g.weights.pick(g.rng, &["win:part:0", "win:part:1", "win:part:2"]) {
        "win:part:1" => 1,
        "win:part:2" => 2,
        _ => 0,
    };
    g.fire2("win:part:", &npart.to_string());
    let mut partition_by = Vec::with_capacity(npart);
    for _ in 0..npart {
        partition_by.push(random_colref(g, scope));
    }

    let mut order_by: Vec<WinOrderKey> = Vec::new();
    if total {
        if g.weights.pick(g.rng, &["win:order:extra", "win:order:plain"]) == "win:order:extra" {
            g.fire("win:order:extra");
            let key = random_colref(g, scope);
            order_by.push(gen_order_key(g, key));
        }
        for (alias, col) in uks {
            let key = Expr::ColRef { alias: alias.clone(), name: col.clone() };
            order_by.push(gen_order_key(g, key));
        }
    } else {
        let nkeys = g.rng.below_usize(3);
        for _ in 0..nkeys {
            let key = random_colref(g, scope);
            order_by.push(gen_order_key(g, key));
        }
    }

    let frame = if allow_frame && total {
        match g.weights.pick(g.rng, &["win:frame:none", "win:frame:rows", "win:frame:range"]) {
            "win:frame:rows" => {
                g.fire("win:frame:rows");
                Some(Frame {
                    range: false,
                    start: pick_start(g, true),
                    end: pick_end(g, true),
                })
            }
            "win:frame:range" => {
                g.fire("win:frame:range");
                // RANGE with an offset needs exactly one ORDER BY key of an
                // offsettable type — only the bare single-unique-key order
                // qualifies (the fixture unique keys are int4).
                let offset_ok = order_by.len() == 1;
                Some(Frame {
                    range: true,
                    start: pick_start(g, offset_ok),
                    end: pick_end(g, offset_ok),
                })
            }
            _ => None,
        }
    } else {
        None
    };
    WindowDef { partition_by, order_by, frame }
}

fn gen_order_key(g: &mut Gen, expr: Expr) -> WinOrderKey {
    let desc = g.rng.chance(1, 4);
    let nulls_first = match g.rng.below(6) {
        0 => Some(true),
        1 => Some(false),
        _ => None,
    };
    WinOrderKey { expr, desc, nulls_first }
}

/// Random in-scope column reference (partition/order keys).
pub(crate) fn random_colref(g: &mut Gen, scope: &Scope) -> Expr {
    let ty = g.any_type(scope);
    let cols = scope.columns_of_type(ty);
    debug_assert!(!cols.is_empty(), "any_type returned a type with no columns");
    let (alias, c) = *g.rng.pick(&cols);
    Expr::ColRef { alias: alias.to_string(), name: c.name.clone() }
}

fn pick_start(g: &mut Gen, offset_ok: bool) -> FrameBound {
    let n = if offset_ok { 3 } else { 2 };
    match g.rng.below(n) {
        0 => FrameBound::UnboundedPreceding,
        1 => FrameBound::CurrentRow,
        _ => FrameBound::Preceding(g.rng.below(4) as u32),
    }
}

fn pick_end(g: &mut Gen, offset_ok: bool) -> FrameBound {
    let n = if offset_ok { 3 } else { 2 };
    match g.rng.below(n) {
        0 => FrameBound::CurrentRow,
        1 => FrameBound::UnboundedFollowing,
        _ => FrameBound::Following(g.rng.below(4) as u32),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, CatalogSource, FixtureCatalog};
    use crate::render::scope_errors;
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    #[test]
    fn win_statements_are_scoped_and_varied() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xF4B2);
        let mut sqls = String::new();
        for i in 0..600 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmt = gen_win_stmt(&mut g);
            let errs = scope_errors(&stmt, &cat);
            assert!(errs.is_empty(), "stmt {i}: {errs:?}\n{}", stmt.to_sql());
            sqls.push_str(&stmt.to_sql());
            sqls.push('\n');
        }
        for frag in [
            " OVER (",
            " OVER w",
            " WINDOW w",
            "PARTITION BY ",
            "row_number()",
            "rank()",
            "dense_rank()",
            "ntile(",
            "lag(",
            "lead(",
            "first_value(",
            "last_value(",
            "nth_value(",
            "count(*) OVER",
            "sum(",
            "ROWS BETWEEN ",
            "RANGE BETWEEN ",
            "UNBOUNDED PRECEDING",
            " JOIN ",
        ] {
            assert!(sqls.contains(frag), "window flavor {frag:?} never generated");
        }
    }

    /// The load-bearing determinism invariant: order-sensitive window
    /// functions and explicit frames only ride on a window ORDER BY that
    /// ends with every relation's unique key (total within partitions).
    #[test]
    fn order_sensitive_windows_are_total() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        // Bias toward sensitive functions, frames and named windows.
        let w = WeightTable::parse(
            "win:row_number=5,win:lag=5,win:lead=5,win:first_value=5,win:last_value=5,\
             win:nth_value=5,win:ntile=5,win:named=2,win:frame:rows=3,win:frame:range=3",
        )
        .unwrap();
        let mut rng = Rng::new(4242);
        let mut checked = 0;
        for _ in 0..500 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmt = gen_win_stmt(&mut g);
            let rel_uks = rel_unique_keys(stmt.from.as_ref().unwrap(), &cat);
            for item in &stmt.items {
                let Expr::WindowFunc { name, over, .. } = &item.expr else { continue };
                let def = match over {
                    WindowOver::Inline(d) => d,
                    WindowOver::Named(n) => {
                        &stmt
                            .windows
                            .iter()
                            .find(|(wn, _)| wn == n)
                            .unwrap_or_else(|| panic!("unresolved window {n}"))
                            .1
                    }
                };
                let needs_total =
                    ORDER_SENSITIVE_WINDOW_FNS.contains(name) || def.frame.is_some();
                if !needs_total {
                    continue;
                }
                checked += 1;
                for (alias, uk) in &rel_uks {
                    let uk = uk.as_ref().unwrap_or_else(|| {
                        panic!(
                            "order-sensitive {name} over keyless relation {alias}: {}",
                            stmt.to_sql()
                        )
                    });
                    assert!(
                        def.order_by.iter().any(|k| matches!(
                            &k.expr,
                            Expr::ColRef { alias: a, name: n } if a == alias && n == uk
                        )),
                        "window order for {name} misses unique key {alias}.{uk}: {}",
                        stmt.to_sql()
                    );
                }
                if let Some(f) = &def.frame {
                    let has_offset = matches!(
                        f.start,
                        FrameBound::Preceding(_) | FrameBound::Following(_)
                    ) || matches!(
                        f.end,
                        FrameBound::Preceding(_) | FrameBound::Following(_)
                    );
                    assert!(
                        !(f.range && has_offset) || def.order_by.len() == 1,
                        "RANGE offset frame over a multi-key order: {}",
                        stmt.to_sql()
                    );
                }
            }
        }
        assert!(checked > 100, "order-sensitive windows rarely generated ({checked})");
    }

    /// (alias, unique key) for every base relation in a FROM tree.
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
            FromItem::Derived { .. } => panic!("win module generates no derived tables"),
        }
    }
}
