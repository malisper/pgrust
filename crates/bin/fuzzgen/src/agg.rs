//! Aggregates statement module: plain aggregation, GROUP BY on columns and
//! expressions (multi-key), HAVING over aggregate predicates, DISTINCT and
//! DISTINCT+GROUP BY combos, and ROLLUP / CUBE / GROUPING SETS at low
//! weight. Aggregate calls cover COUNT(*) / COUNT(x) / COUNT(DISTINCT x),
//! SUM/AVG across the integer/numeric/float families, MIN/MAX across the
//! comparable types, bool_and/bool_or, the statistics aggregates
//! (stddev/variance), and the ordered-input aggregates string_agg and
//! array_agg — with an occasional FILTER (WHERE ...) on any of them.
//!
//! Compare-safety disciplines (the F1 differ consumes these statements):
//!
//!   - Ordered-input aggregates (string_agg/array_agg) always carry an
//!     inner ORDER BY on their own first-argument expression: residual
//!     ties are then between equal values, so the aggregated text is
//!     deterministic even though the sort is not total. Argument types
//!     are restricted to text-stable types (equal values render equal
//!     text; float -0/0 and numeric 0/0.00 would not).
//!
//!   - Order-sensitive float aggregates (SUM/AVG/stddev/var over
//!     float4/float8) are generated at low weight and their result
//!     columns are ruled-soft (crate::ruled b1-float-agg-soft, B1
//!     ruling): accumulation order is plan-dependent, and cancelling
//!     subsets make the divergence unbounded in ulp terms, so no fixed
//!     ulp tolerance is sufficient. Statements carrying such columns
//!     never take the ORDER BY/LIMIT suffix (a soft sort key would
//!     underdetermine row order and, under LIMIT, the row set), and
//!     HAVING never uses them (they would underdetermine group
//!     membership). numeric SUM/AVG/stddev accumulate exactly
//!     (order-insensitive) and are weighted higher — the same executor
//!     nodes without the comparator risk.
//!
//!   - GROUP BY and DISTINCT keys are restricted to text-stable types:
//!     grouping keeps one representative value per group, and values
//!     that compare equal with different text would make the
//!     representative's rendering plan-dependent.
//!
//!   - Plain GROUP BY expressions render parenthesized so a bare integer
//!     literal is never read as an output-column ordinal (render::GroupBy).

use crate::catalog::SqlType;
use crate::expr::Expr;
use crate::render::{soft_float_cols, FromItem, GroupBy, SelectItem, SelectStmt};
use crate::scope::{Scope, ScopeRel};
use crate::stmt::{order_limit_suffix, Gen};

/// Types whose equal values always render identical wire text (C locale):
/// safe as GROUP BY / DISTINCT keys and ordered-aggregate arguments.
const STABLE_TYPES: &[SqlType] = &[
    SqlType::Int2,
    SqlType::Int4,
    SqlType::Int8,
    SqlType::Text,
    SqlType::Varchar,
    SqlType::Bool,
    SqlType::Date,
    SqlType::Timestamp,
];

/// Argument types with MIN/MAX support (no bool aggregate min/max).
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

/// One generated aggregate call. Whether a call is ruled-soft is derived
/// from the AST afterwards (render::soft_float_cols), not tracked here.
struct AggPick {
    expr: Expr,
    ty: SqlType,
}

pub fn gen_agg_stmt(g: &mut Gen) -> SelectStmt {
    g.fire("select");
    let table = g.pick_table();
    let alias = g.next_alias();
    let rels = vec![ScopeRel::from_table(table, alias.clone())];
    let from = FromItem::Table { name: table.name.clone(), alias };

    let shape = g.weights.pick(
        g.rng,
        &["agg:plain", "agg:group", "agg:distinct", "agg:rollup", "agg:cube", "agg:groupingsets"],
    );
    g.fire(shape);
    let mut stmt = match shape {
        "agg:plain" => gen_plain(g, &rels, from),
        "agg:group" => gen_grouped(g, &rels, from),
        "agg:distinct" => gen_distinct(g, &rels, from),
        _ => gen_grouping_sets(g, &rels, from, shape),
    };
    // A ruled-soft column may not steer row order or (under LIMIT) row-set
    // membership, so soft statements take no ORDER BY/LIMIT suffix at all.
    if soft_float_cols(&stmt).is_empty() {
        order_limit_suffix(g, &mut stmt);
    }
    stmt
}

/// Plain aggregation, no GROUP BY: one output row of 1-3 aggregates.
fn gen_plain(g: &mut Gen, rels: &[ScopeRel], from: FromItem) -> SelectStmt {
    let scope = Scope { rels, outer: None };
    let naggs = 1 + g.rng.below_usize(3);
    let mut items = Vec::with_capacity(naggs);
    for _ in 0..naggs {
        let a = gen_aggregate(g, &scope, false);
        items.push(SelectItem { expr: a.expr, alias: None, ty: a.ty });
    }
    let where_clause = gen_where(g, &scope);
    let having = maybe_having(g, &scope, &[]);
    SelectStmt { items, from: Some(from), where_clause, having, ..Default::default() }
}

/// GROUP BY on 1-3 keys (columns or text-stable expressions); the keys
/// reappear in the select list next to 1-2 aggregates; DISTINCT sometimes
/// (the DISTINCT+GROUP BY combo); HAVING sometimes.
fn gen_grouped(g: &mut Gen, rels: &[ScopeRel], from: FromItem) -> SelectStmt {
    let scope = Scope { rels, outer: None };
    let nkeys = 1 + g.rng.below_usize(3);
    let mut group_exprs: Vec<(Expr, SqlType)> = Vec::with_capacity(nkeys);
    for _ in 0..nkeys {
        group_exprs.push(gen_group_key(g, &scope));
    }
    let mut items = Vec::new();
    for (e, ty) in &group_exprs {
        // Group keys usually project; PostgreSQL does not require it.
        if g.rng.chance(5, 6) {
            items.push(SelectItem { expr: e.clone(), alias: None, ty: *ty });
        }
    }
    let naggs = 1 + g.rng.below_usize(2);
    for _ in 0..naggs {
        let a = gen_aggregate(g, &scope, false);
        items.push(SelectItem { expr: a.expr, alias: None, ty: a.ty });
    }
    let distinct = g.rng.chance(1, 6);
    let where_clause = gen_where(g, &scope);
    let having = maybe_having(g, &scope, &group_exprs);
    SelectStmt {
        distinct,
        items,
        from: Some(from),
        where_clause,
        group_by: Some(GroupBy::Plain(group_exprs.into_iter().map(|(e, _)| e).collect())),
        having,
        ..Default::default()
    }
}

/// SELECT DISTINCT over 1-3 text-stable expressions (pure deduplication,
/// no aggregates).
fn gen_distinct(g: &mut Gen, rels: &[ScopeRel], from: FromItem) -> SelectStmt {
    let scope = Scope { rels, outer: None };
    let ncols = 1 + g.rng.below_usize(3);
    let mut items = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let ty = *g.rng.pick(STABLE_TYPES);
        let expr = g.gen_typed(&scope, ty, 2);
        items.push(SelectItem { expr, alias: None, ty });
    }
    let where_clause = gen_where(g, &scope);
    SelectStmt { distinct: true, items, from: Some(from), where_clause, ..Default::default() }
}

/// ROLLUP / CUBE / GROUPING SETS over 2-3 distinct text-stable columns.
fn gen_grouping_sets(g: &mut Gen, rels: &[ScopeRel], from: FromItem, shape: &str) -> SelectStmt {
    let scope = Scope { rels, outer: None };
    let stable = stable_columns(&scope);
    // Every fixture table carries the k_int/k_text join keys, so at least
    // two stable columns are always in scope.
    let ncols = (2 + g.rng.below_usize(2)).min(stable.len());
    let mut cols: Vec<(Expr, SqlType)> = Vec::with_capacity(ncols);
    let start = g.rng.below_usize(stable.len());
    for k in 0..ncols {
        let (alias, name, ty) = stable[(start + k) % stable.len()].clone();
        cols.push((Expr::ColRef { alias, name }, ty));
    }
    let group_by = match shape {
        "agg:rollup" => GroupBy::Rollup(cols.iter().map(|(e, _)| e.clone()).collect()),
        "agg:cube" => GroupBy::Cube(cols.iter().map(|(e, _)| e.clone()).collect()),
        _ => {
            // 2-3 subsets by bitmask; one is always the full column set so
            // every projected column is a grouping column of some set, and
            // the empty grand-total set appears sometimes.
            let mut masks: Vec<usize> = vec![(1 << cols.len()) - 1];
            let extra = 1 + g.rng.below_usize(2);
            for _ in 0..extra {
                let m = g.rng.below_usize(1 << cols.len());
                if !masks.contains(&m) {
                    masks.push(m);
                }
            }
            let sets: Vec<Vec<Expr>> = masks
                .iter()
                .map(|m| {
                    cols.iter()
                        .enumerate()
                        .filter(|(i, _)| m & (1 << i) != 0)
                        .map(|(_, (e, _))| e.clone())
                        .collect()
                })
                .collect();
            GroupBy::Sets(sets)
        }
    };
    let mut items: Vec<SelectItem> = cols
        .iter()
        .map(|(e, ty)| SelectItem { expr: e.clone(), alias: None, ty: *ty })
        .collect();
    let naggs = 1 + g.rng.below_usize(2);
    for _ in 0..naggs {
        let a = gen_aggregate(g, &scope, false);
        items.push(SelectItem { expr: a.expr, alias: None, ty: a.ty });
    }
    let where_clause = gen_where(g, &scope);
    let having = maybe_having(g, &scope, &cols);
    SelectStmt {
        items,
        from: Some(from),
        where_clause,
        group_by: Some(group_by),
        having,
        ..Default::default()
    }
}

fn gen_where(g: &mut Gen, scope: &Scope) -> Option<Expr> {
    if g.rng.chance(1, 2) {
        g.fire("where");
        Some(g.gen_bool(scope, g.max_depth))
    } else {
        None
    }
}

/// One GROUP BY key: usually a column reference, sometimes a text-stable
/// expression over the scope.
fn gen_group_key(g: &mut Gen, scope: &Scope) -> (Expr, SqlType) {
    let stable = stable_columns(scope);
    let picked = g.weights.pick(g.rng, &["agg:groupby:col", "agg:groupby:expr"]);
    if picked == "agg:groupby:col" && !stable.is_empty() {
        g.fire("agg:groupby:col");
        let (alias, name, ty) = stable[g.rng.below_usize(stable.len())].clone();
        (Expr::ColRef { alias, name }, ty)
    } else {
        g.fire("agg:groupby:expr");
        let ty = *g.rng.pick(STABLE_TYPES);
        let e = g.gen_typed(scope, ty, 2);
        // A bare-constant group key is read by the parser as an output-
        // column ordinal (integers) or rejected as a non-integer constant —
        // parens are transparent there, and so is unary minus over a
        // literal (`(- 0)` = the constant 0 = "GROUP BY position 0",
        // 42P10). Anchoring any column-free key behind a cast makes it an
        // ordinary expression again.
        let e = if contains_colref(&e) {
            e
        } else {
            Expr::Cast { arg: Box::new(e), to: ty }
        };
        (e, ty)
    }
}

/// Does the expression reference any column? Column-free GROUP BY keys
/// need the constant-ordinal cast anchor (see gen_group_key). Subquery
/// forms count as anchored: the parser never reads them as ordinals.
fn contains_colref(e: &Expr) -> bool {
    match e {
        Expr::ColRef { .. } => true,
        Expr::Lit { .. } | Expr::Null { .. } => false,
        Expr::Unary { arg, .. } => contains_colref(arg),
        Expr::Binary { lhs, rhs, .. } => contains_colref(lhs) || contains_colref(rhs),
        Expr::Func { args, .. } => args.iter().any(contains_colref),
        Expr::Case { cond, then_e, else_e } => {
            contains_colref(cond) || contains_colref(then_e) || contains_colref(else_e)
        }
        Expr::Cast { arg, .. } => contains_colref(arg),
        Expr::IsNull { arg, .. } => contains_colref(arg),
        // T1's literal-only pre/post wrapper (EXTRACT, subscripts, ANY/ALL):
        // column-freedom is decided by the wrapped argument.
        Expr::Wrap { arg, .. } => contains_colref(arg),
        Expr::ScalarSubq { .. }
        | Expr::InSubq { .. }
        | Expr::Exists { .. }
        | Expr::Agg { .. }
        | Expr::WindowFunc { .. } => true,
    }
}

/// All text-stable columns in scope, alias-qualified.
fn stable_columns(scope: &Scope) -> Vec<(String, String, SqlType)> {
    let mut out = Vec::new();
    for &ty in STABLE_TYPES {
        for (alias, c) in scope.columns_of_type(ty) {
            out.push((alias.to_string(), c.name.clone(), ty));
        }
    }
    out
}

/// HAVING sometimes: 1-2 comparison conjuncts over non-soft aggregates
/// (soft aggregates would underdetermine group membership) or group keys.
fn maybe_having(g: &mut Gen, scope: &Scope, group_exprs: &[(Expr, SqlType)]) -> Option<Expr> {
    if g.weights.pick(g.rng, &["agg:having", "agg:having:none"]) != "agg:having" {
        return None;
    }
    g.fire("agg:having");
    let mut pred = gen_having_conjunct(g, scope, group_exprs);
    if g.rng.chance(1, 3) {
        let rhs = gen_having_conjunct(g, scope, group_exprs);
        let op = *g.rng.pick(&["AND", "OR"]);
        pred = Expr::Binary { op, lhs: Box::new(pred), rhs: Box::new(rhs) };
    }
    Some(pred)
}

fn gen_having_conjunct(g: &mut Gen, scope: &Scope, group_exprs: &[(Expr, SqlType)]) -> Expr {
    let use_group = !group_exprs.is_empty()
        && g.weights.pick(g.rng, &["agg:having:group", "agg:having:agg"]) == "agg:having:group";
    let (lhs, ty) = if use_group {
        g.fire("agg:having:group");
        let (e, ty) = group_exprs[g.rng.below_usize(group_exprs.len())].clone();
        (e, ty)
    } else {
        g.fire("agg:having:agg");
        let a = gen_aggregate(g, scope, true);
        (a.expr, a.ty)
    };
    let op = g.pick_cmp_op();
    let rhs = Expr::Lit { sql: g.gen_literal(ty) };
    Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
}

/// One aggregate call. `for_having` excludes ruled-soft float aggregates
/// (group membership must stay deterministic) and array_agg (no comparison
/// against a scalar literal).
fn gen_aggregate(g: &mut Gen, scope: &Scope, for_having: bool) -> AggPick {
    let mut opts: Vec<&'static str> = vec![
        "agg:count_star",
        "agg:count",
        "agg:count_distinct",
        "agg:sum:int",
        "agg:sum:numeric",
        "agg:avg:int",
        "agg:avg:numeric",
        "agg:min",
        "agg:max",
        "agg:bool_and",
        "agg:bool_or",
        "agg:string_agg",
        "agg:stddev:numeric",
    ];
    if !for_having {
        opts.extend_from_slice(&[
            "agg:sum:float",
            "agg:avg:float",
            "agg:stddev:float",
            "agg:array_agg",
        ]);
    }
    let picked = g.weights.pick(g.rng, &opts);
    g.fire(picked);
    let (expr, ty) = match picked {
        "agg:count_star" => (agg("count", true, false, vec![], None), SqlType::Int8),
        "agg:count" | "agg:count_distinct" => {
            let t = g.any_type(scope);
            let arg = g.gen_typed(scope, t, 2);
            (
                agg("count", false, picked == "agg:count_distinct", vec![arg], None),
                SqlType::Int8,
            )
        }
        "agg:sum:int" => {
            let t = *g.rng.pick(&[SqlType::Int2, SqlType::Int4, SqlType::Int8]);
            let arg = g.gen_typed(scope, t, 2);
            let ty = if t == SqlType::Int8 { SqlType::Numeric } else { SqlType::Int8 };
            (agg("sum", false, false, vec![arg], None), ty)
        }
        "agg:sum:numeric" => {
            let arg = g.gen_typed(scope, SqlType::Numeric, 2);
            (agg("sum", false, false, vec![arg], None), SqlType::Numeric)
        }
        "agg:sum:float" => {
            let t = *g.rng.pick(&[SqlType::Float4, SqlType::Float8]);
            let arg = g.gen_typed(scope, t, 2);
            (agg("sum", false, false, vec![arg], None), t)
        }
        "agg:avg:int" => {
            let t = *g.rng.pick(&[SqlType::Int2, SqlType::Int4, SqlType::Int8]);
            let arg = g.gen_typed(scope, t, 2);
            (agg("avg", false, false, vec![arg], None), SqlType::Numeric)
        }
        "agg:avg:numeric" => {
            let arg = g.gen_typed(scope, SqlType::Numeric, 2);
            (agg("avg", false, false, vec![arg], None), SqlType::Numeric)
        }
        "agg:avg:float" => {
            let t = *g.rng.pick(&[SqlType::Float4, SqlType::Float8]);
            let arg = g.gen_typed(scope, t, 2);
            // avg(float4) and avg(float8) both return float8.
            (agg("avg", false, false, vec![arg], None), SqlType::Float8)
        }
        "agg:min" | "agg:max" => {
            let name = if picked == "agg:min" { "min" } else { "max" };
            let t = *g.rng.pick(MINMAX_TYPES);
            let arg = g.gen_typed(scope, t, 2);
            // min/max over varchar resolve through the text aggregate.
            let ty = if t == SqlType::Varchar { SqlType::Text } else { t };
            (agg(name, false, false, vec![arg], None), ty)
        }
        "agg:bool_and" | "agg:bool_or" => {
            let name = if picked == "agg:bool_and" { "bool_and" } else { "bool_or" };
            let arg = g.gen_bool(scope, 2);
            (agg(name, false, false, vec![arg], None), SqlType::Bool)
        }
        "agg:string_agg" => {
            let t = *g.rng.pick(&[SqlType::Text, SqlType::Varchar]);
            let arg = g.gen_typed(scope, t, 2);
            let delim = Expr::Lit { sql: (*g.rng.pick(&["','", "''", "' '", "'|'"])).to_string() };
            let desc = g.rng.chance(1, 3);
            (
                agg_ordered("string_agg", vec![arg.clone(), delim], arg, desc),
                SqlType::Text,
            )
        }
        "agg:array_agg" => {
            let t = *g.rng.pick(STABLE_TYPES);
            let arg = g.gen_typed(scope, t, 2);
            let desc = g.rng.chance(1, 3);
            // The result is an array; SqlType has no array spelling, and the
            // recorded ty only feeds the LIMIT float guard and derived-table
            // typing (neither applies to an array column here) — Text is a
            // harmless stand-in.
            (agg_ordered("array_agg", vec![arg.clone()], arg, desc), SqlType::Text)
        }
        "agg:stddev:numeric" => {
            let name = *g.rng.pick(&["stddev_samp", "stddev_pop", "var_samp", "var_pop"]);
            let t = *g.rng.pick(&[
                SqlType::Int2,
                SqlType::Int4,
                SqlType::Int8,
                SqlType::Numeric,
            ]);
            let arg = g.gen_typed(scope, t, 2);
            (agg(name, false, false, vec![arg], None), SqlType::Numeric)
        }
        "agg:stddev:float" => {
            let name = *g.rng.pick(&["stddev_samp", "stddev_pop", "var_samp", "var_pop"]);
            let arg = g.gen_typed(scope, SqlType::Float8, 2);
            (agg(name, false, false, vec![arg], None), SqlType::Float8)
        }
        other => unreachable!("unknown aggregate production {}", other),
    };
    let expr = if g.weights.pick(g.rng, &["agg:filter", "agg:filter:none"]) == "agg:filter" {
        g.fire("agg:filter");
        let f = g.gen_bool(scope, 2);
        with_filter(expr, f)
    } else {
        expr
    };
    AggPick { expr, ty }
}

fn agg(
    name: &'static str,
    star: bool,
    distinct: bool,
    args: Vec<Expr>,
    filter: Option<Box<Expr>>,
) -> Expr {
    Expr::Agg { name, star, distinct, args, order_within: None, filter }
}

/// Ordered-input aggregate: the inner ORDER BY is always the first
/// argument expression, so ties are between equal values and the result
/// text is deterministic (module-docs discipline).
fn agg_ordered(name: &'static str, args: Vec<Expr>, order_expr: Expr, desc: bool) -> Expr {
    Expr::Agg {
        name,
        star: false,
        distinct: false,
        args,
        order_within: Some((Box::new(order_expr), desc)),
        filter: None,
    }
}

fn with_filter(e: Expr, f: Expr) -> Expr {
    match e {
        Expr::Agg { name, star, distinct, args, order_within, .. } => Expr::Agg {
            name,
            star,
            distinct,
            args,
            order_within,
            filter: Some(Box::new(f)),
        },
        other => other,
    }
}

// ===================================================================
// Q7 agg-tweaks raw families (sql-reachable-queue GEN-GAP-SIBLING chunk,
// 104 remaining fns after gap-010). Every family below was hand-verified
// byte-identical on both engines 2026-08-12 (scratch deck-agg; A = own
// cpg-ref REL_18_3 build, B = pgrust origin/main@cc3b6bc550e) — the only
// divergent lines were EXPLAIN ANALYZE "Memory Usage"/"Sort Method"
// counters, which the differ already masks (Ruled("explain-counter")).
// Scalar float outputs always cast ::text (adtmisc law: fixed literal
// inputs in fixed order accumulate deterministically on one node).
// ===================================================================

use crate::stmt::StmtKind;

const AGGX_SHAPES: &[&str] = &[
    "aggx:ordset",
    "aggx:regr",
    "aggx:minmax2",
    "aggx:hashx",
    "aggx:gsets2",
    "aggx:aggerr",
    "aggx:distinctm",
    "aggx:groupnode",
    "aggx:fdep",
    "aggx:serialize",
    "aggx:bitmapor",
    "aggx:semijoin",
    "aggx:datumsort",
    "aggx:aggddl",
];

/// Registry entry point for the raw agg-breadth families (dispatched from
/// stmt::gen_agg_module under the agg:aggx arm).
pub fn gen_aggx_stmts(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("agg:aggx");
    match g.weights.pick(g.rng, AGGX_SHAPES) {
        "aggx:ordset" => aggx_ordset(g),
        "aggx:regr" => aggx_regr(g),
        "aggx:minmax2" => aggx_minmax2(g),
        "aggx:hashx" => aggx_hashx(g),
        "aggx:gsets2" => aggx_gsets2(g),
        "aggx:aggerr" => aggx_aggerr(g),
        "aggx:distinctm" => aggx_distinctm(g),
        "aggx:groupnode" => aggx_groupnode(g),
        "aggx:fdep" => aggx_fdep(g),
        "aggx:serialize" => aggx_serialize(g),
        "aggx:bitmapor" => aggx_bitmapor(g),
        "aggx:semijoin" => aggx_semijoin(g),
        "aggx:datumsort" => aggx_datumsort(g),
        "aggx:aggddl" => aggx_aggddl(g),
        other => unreachable!("unknown aggx shape {other}"),
    }
}

fn araw(sql: String) -> Vec<StmtKind> {
    vec![StmtKind::Raw(sql)]
}

fn apick<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

/// The deterministic 40-row aggregate fixture bracket (int/text/float8/
/// interval/time/timetz/date columns): wraps one probe statement.
fn aggx_fixture(probes: Vec<String>) -> Vec<StmtKind> {
    let mut v = vec![
        StmtKind::Raw("DROP TABLE IF EXISTS fz_q7ag CASCADE;".to_string()),
        StmtKind::Raw(
            "CREATE TABLE fz_q7ag (a int, b text, f float8, i interval, ts time, tz timetz, d date) WITH (autovacuum_enabled = off);"
                .to_string(),
        ),
        StmtKind::Raw(
            "INSERT INTO fz_q7ag SELECT g, chr(97 + g % 5) || g::text, g * 1.5, (g || ' hours')::interval, ('01:00'::time + (g || ' min')::interval), ('01:00+02'::timetz + (g || ' min')::interval), '2020-01-01'::date + g FROM generate_series(1, 40) g;"
                .to_string(),
        ),
    ];
    v.extend(probes.into_iter().map(StmtKind::Raw));
    v.push(StmtKind::Raw("DROP TABLE fz_q7ag CASCADE;".to_string()));
    v
}

/// Ordered-set + hypothetical-set aggregates (orderedsetaggs.c sweep).
fn aggx_ordset(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:ordset");
    let p25 = format!("0.{}", 1 + g.rng.below(9));
    let probe = match g.rng.below(12) {
        0 => format!("SELECT percentile_cont({p25}) WITHIN GROUP (ORDER BY f)::text FROM fz_q7ag;"),
        1 => format!("SELECT percentile_cont(ARRAY[{p25}, 0.5, 0.75]) WITHIN GROUP (ORDER BY f)::text FROM fz_q7ag;"),
        2 => format!("SELECT percentile_cont({p25}) WITHIN GROUP (ORDER BY i)::text, percentile_cont(ARRAY[0.1, 0.9]) WITHIN GROUP (ORDER BY i)::text FROM fz_q7ag;"),
        3 => format!("SELECT percentile_disc({p25}) WITHIN GROUP (ORDER BY a)::text, percentile_disc(ARRAY[0.2, 0.8]) WITHIN GROUP (ORDER BY b)::text FROM fz_q7ag;"),
        4 => "SELECT mode() WITHIN GROUP (ORDER BY a % 3), mode() WITHIN GROUP (ORDER BY b) FROM fz_q7ag;".to_string(),
        5 => format!("SELECT percentile_cont({p25}) WITHIN GROUP (ORDER BY f)::text, percentile_cont(0.75) WITHIN GROUP (ORDER BY f)::text FROM fz_q7ag;"),
        6 => format!("SELECT rank({}) WITHIN GROUP (ORDER BY a), dense_rank({}) WITHIN GROUP (ORDER BY a) FROM fz_q7ag;", g.rng.below(45), g.rng.below(45)),
        7 => format!("SELECT percent_rank({}) WITHIN GROUP (ORDER BY a)::text, cume_dist({}) WITHIN GROUP (ORDER BY a)::text FROM fz_q7ag;", g.rng.below(45), g.rng.below(45)),
        8 => format!("SELECT rank('{}') WITHIN GROUP (ORDER BY b) FROM fz_q7ag;", apick(g, &["m", "a1", "zz"])),
        9 => format!("SELECT rank({}, '{}') WITHIN GROUP (ORDER BY a, b COLLATE \"C\") FROM fz_q7ag;", g.rng.below(45), apick(g, &["x", "b2"])),
        10 => "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY b COLLATE \"C\") FROM fz_q7ag;".to_string(),
        _ => format!("SELECT rank({}) WITHIN GROUP (ORDER BY a DESC NULLS LAST) FROM fz_q7ag;", g.rng.below(45)),
    };
    aggx_fixture(vec![probe])
}

/// The regr_*/corr/covar two-argument statistics matrix over fixed
/// integer-valued float8 literals (exact accumulation, one node).
fn aggx_regr(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:regr");
    let a = g.rng.below(9);
    let vals = format!("(VALUES (1::float8, 2::float8), (2, 4), (3, 7), (4, {})) v(x, y)", 5 + a);
    let sql = match g.rng.below(3) {
        0 => format!("SELECT corr(y, x)::text, covar_pop(y, x)::text, covar_samp(y, x)::text FROM {vals};"),
        1 => format!(
            "SELECT regr_avgx(y, x)::text, regr_avgy(y, x)::text, regr_count(y, x), regr_intercept(y, x)::text, regr_r2(y, x)::text, regr_slope(y, x)::text, regr_sxx(y, x)::text, regr_sxy(y, x)::text, regr_syy(y, x)::text FROM {vals};"
        ),
        _ => format!("SELECT regr_count(y, x), corr(DISTINCT y, x)::text FROM {vals};"),
    };
    araw(sql)
}

/// min/max + misc aggregate breadth: time/timetz/interval/anyarray keys,
/// array_agg over arrays, any_value, range_agg(multirange), int8_sum,
/// string_agg over bytea.
fn aggx_minmax2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:minmax2");
    match g.rng.below(7) {
        0 => aggx_fixture(vec![
            "SELECT min(ts)::text, max(ts)::text, min(tz)::text, max(tz)::text, min(i)::text, max(i)::text FROM fz_q7ag;".to_string(),
        ]),
        1 => araw(format!(
            "SELECT max(v)::text, min(v)::text FROM (VALUES ('{{1,2}}'::int4[]), ('{{0,{}}}'), ('{{1}}')) t(v);",
            g.rng.below(50)
        )),
        2 => araw(format!(
            "SELECT array_agg(v ORDER BY v)::text FROM (VALUES ('{{1,2}}'::int4[]), ('{{{},4}}'), ('{{0}}')) t(v);",
            g.rng.below(9)
        )),
        3 => araw(format!(
            "SELECT any_value(x) FROM (VALUES ({0}), ({0}), ({0})) v(x);",
            g.rng.below(100)
        )),
        4 => araw(format!(
            "SELECT range_agg(m)::text FROM (VALUES ('{{[1,2]}}'::int4multirange), ('{{[{},9]}}')) v(m);",
            3 + g.rng.below(5)
        )),
        5 => araw(format!(
            "SELECT int8_sum(NULL::numeric, {}::int8)::text, int8_sum(10.5::numeric, 7::int8)::text;",
            g.rng.below(1000)
        )),
        _ => araw(
            "SELECT encode(string_agg(v, '\\x00'::bytea ORDER BY v), 'hex'), encode(string_agg(v, ''::bytea ORDER BY v DESC), 'hex') FROM (VALUES ('\\xaa'::bytea), ('\\xbb'), ('\\x01')) t(v);"
                .to_string(),
        ),
    }
}

/// Extended/64-bit hash function breadth: direct calls (deterministic
/// int8 outputs, identical algorithms both sides) + organic hash-agg
/// grouping brackets on the same key types under enable_sort=off.
fn aggx_hashx(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:hashx");
    let seed = g.rng.below(100);
    match g.rng.below(6) {
        0 => araw(format!(
            "SELECT hash_array_extended('{{1,2,3}}'::int4[], {seed}), hashboolextended(true, {seed}), hashdateextended('2020-06-01'::date, {seed});"
        )),
        1 => araw(format!(
            "SELECT time_hash_extended('12:34:56'::time, {seed}), timetz_hash_extended('12:34:56+02'::timetz, {seed});"
        )),
        2 => araw(format!(
            "SELECT hashmacaddr('08:00:2b:01:02:03'::macaddr), hashmacaddrextended('08:00:2b:01:02:03'::macaddr, {seed}), hashmacaddr8('08:00:2b:01:02:03:04:05'::macaddr8), hashmacaddr8extended('08:00:2b:01:02:03:04:05'::macaddr8, {seed});"
        )),
        3 => araw(format!(
            "SELECT hash_multirange_extended('{{[1,2],[5,7]}}'::int4multirange, {seed}), hash_multirange('{{[1,2]}}'::int4multirange), hash_range_extended('[1,9]'::int4range, {seed});"
        )),
        _ => {
            let inner = match g.rng.below(4) {
                0 => "SELECT m::text FROM (VALUES ('08:00:2b:01:02:03'::macaddr), ('08:00:2b:01:02:03'), ('01:00:2b:01:02:03')) v(m) GROUP BY m ORDER BY m::text;",
                1 => "SELECT r::text FROM (VALUES ('[1,2]'::int4range), ('[1,2]'), ('[3,4]')) v(r) GROUP BY r ORDER BY r::text;",
                2 => "SELECT mr::text FROM (VALUES ('{[1,2]}'::int4multirange), ('{[1,2]}')) v(mr) GROUP BY mr ORDER BY mr::text;",
                _ => "SELECT x::text FROM (VALUES ('12:00'::time), ('12:00'), ('13:30')) v(x) GROUP BY x ORDER BY x::text;",
            };
            vec![
                StmtKind::Raw("SET enable_sort = off;".to_string()),
                StmtKind::Raw(inner.to_string()),
                StmtKind::Raw("RESET enable_sort;".to_string()),
            ]
        }
    }
}

/// Grouping-set tails: degenerate empty sets, multi-set reordering, and
/// outer-level aggregate/GROUPING() references inside subqueries.
fn aggx_gsets2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:gsets2");
    let probe = match g.rng.below(4) {
        0 => "SELECT count(*) FROM fz_q7ag GROUP BY GROUPING SETS ((), ());".to_string(),
        1 => "SELECT a % 2 AS m, b, count(*) FROM fz_q7ag GROUP BY GROUPING SETS ((a % 2, b), (b, a % 2), (a % 2), ()) ORDER BY 1, 2, 3;".to_string(),
        2 => "SELECT (SELECT max(fz_q7ag.a)) FROM fz_q7ag;".to_string(),
        _ => "SELECT (SELECT GROUPING(a) + 1) FROM fz_q7ag GROUP BY a ORDER BY 1 LIMIT 2;".to_string(),
    };
    aggx_fixture(vec![probe])
}

/// Misplaced-aggregate error paths (locate_agg_of_level + the
/// aggregate-in-disallowed-clause checks). Deliberate matched errors.
fn aggx_aggerr(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:aggerr");
    let probe = match g.rng.below(4) {
        0 => "SELECT a FROM fz_q7ag WHERE count(*) > 1;",
        1 => "SELECT x.a FROM fz_q7ag x JOIN fz_q7ag y ON count(*) = 1;",
        2 => "INSERT INTO fz_q7ag (a) VALUES (count(*));",
        _ => "SELECT 1 UNION SELECT count(*) FROM fz_q7ag;",
    };
    aggx_fixture(vec![probe.to_string()])
}

/// Multi-argument DISTINCT aggregates (presorted-distinct-multi arm).
fn aggx_distinctm(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:distinctm");
    let probe = match g.rng.below(3) {
        0 => "SELECT string_agg(DISTINCT b, ',' ORDER BY b, ',') FROM fz_q7ag;",
        1 => "SELECT string_agg(DISTINCT b, ',') FROM (SELECT b FROM fz_q7ag ORDER BY b) s;",
        _ => "SELECT count(DISTINCT b), count(DISTINCT a) FROM (SELECT a, b FROM fz_q7ag ORDER BY b, a) s;",
    };
    aggx_fixture(vec![probe.to_string()])
}

/// Group plan node (GROUP BY sans aggregates, enable_hashagg=off):
/// execution, EXPLAIN key display, and correlated-subplan rescan.
fn aggx_groupnode(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:groupnode");
    let probe = match g.rng.below(3) {
        0 => "SELECT b FROM fz_q7ag GROUP BY b ORDER BY b;",
        1 => "EXPLAIN (COSTS OFF) SELECT b FROM fz_q7ag GROUP BY b;",
        _ => "SELECT (SELECT count(*) FROM (SELECT b FROM fz_q7ag WHERE a <= v.x GROUP BY b) s) FROM (VALUES (0), (5), (11)) v(x) ORDER BY 1;",
    };
    aggx_fixture(vec![
        "SET enable_hashagg = off;".to_string(),
        probe.to_string(),
        "RESET enable_hashagg;".to_string(),
    ])
}

/// GROUP BY primary key functional dependency inside CREATE VIEW
/// (check_functional_grouping / get_primary_key_attnos).
fn aggx_fdep(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:fdep");
    let n = g.rng.below(100);
    vec![
        StmtKind::Raw("DROP TABLE IF EXISTS fz_q7fd CASCADE;".to_string()),
        StmtKind::Raw("CREATE TABLE fz_q7fd (id int PRIMARY KEY, val text, n int) WITH (autovacuum_enabled = off);".to_string()),
        StmtKind::Raw(format!("INSERT INTO fz_q7fd VALUES (1, 'x', {n}), (2, 'y', 6);")),
        StmtKind::Raw(
            "CREATE VIEW fz_q7fdv AS SELECT id, val, count(*) AS c, sum(n) AS s FROM fz_q7fd GROUP BY id;"
                .to_string(),
        ),
        StmtKind::Raw("SELECT id, val, c, s FROM fz_q7fdv ORDER BY id;".to_string()),
        StmtKind::Raw("DROP TABLE fz_q7fd CASCADE;".to_string()),
    ]
}

/// EXPLAIN (ANALYZE, SERIALIZE): the serializeAnalyze DestReceiver and
/// ExplainPrintSerialize summary (counter values masked by the differ;
/// the Serialization line itself compared byte-identical in hand-verify).
fn aggx_serialize(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:serialize");
    let probe = match g.rng.below(3) {
        0 => "EXPLAIN (ANALYZE, SERIALIZE, TIMING OFF, COSTS OFF, SUMMARY OFF, BUFFERS OFF) SELECT a, b FROM fz_q7ag ORDER BY a;",
        1 => "EXPLAIN (ANALYZE, SERIALIZE TEXT, TIMING OFF, COSTS OFF, SUMMARY OFF, BUFFERS OFF) SELECT b FROM fz_q7ag GROUP BY b;",
        _ => "EXPLAIN (ANALYZE, SERIALIZE BINARY, TIMING OFF, COSTS OFF, SUMMARY OFF, BUFFERS OFF) SELECT d FROM fz_q7ag ORDER BY d LIMIT 5;",
    };
    aggx_fixture(vec![probe.to_string()])
}

/// BitmapOr across two btree indexes (+ correlated rescan variant).
fn aggx_bitmapor(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:bitmapor");
    let k = 1 + g.rng.below(20);
    let probe = match g.rng.below(3) {
        0 => format!("SELECT count(*) FROM fz_q7ag WHERE a = {k} OR b = 'c4';"),
        1 => format!("EXPLAIN (COSTS OFF) SELECT count(*) FROM fz_q7ag WHERE a = {k} OR b = 'c4';"),
        _ => "SELECT (SELECT count(*) FROM fz_q7ag WHERE a = v.x OR b = 'c' || v.x::text) FROM (VALUES (1), (2), (3)) v(x) ORDER BY 1;".to_string(),
    };
    aggx_fixture(vec![
        "CREATE INDEX fz_q7ag_ia ON fz_q7ag (a);".to_string(),
        "CREATE INDEX fz_q7ag_ib ON fz_q7ag (b);".to_string(),
        "SET enable_seqscan = off;".to_string(),
        probe,
        "RESET enable_seqscan;".to_string(),
    ])
}

/// Unique-ified IN-subquery semijoin (change_plan_targetlist).
fn aggx_semijoin(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:semijoin");
    let n = 4 + g.rng.below(8);
    let probe = if g.rng.below(2) == 0 {
        vec![format!(
            "SELECT count(*) FROM fz_q7ag WHERE a IN (SELECT x + 0 FROM generate_series(1, {n}) g(x));"
        )]
    } else {
        vec![
            "SET enable_hashjoin = off;".to_string(),
            "SET enable_nestloop = off;".to_string(),
            format!(
                "SELECT count(*) FROM fz_q7ag WHERE a IN (SELECT x * 2 FROM generate_series(1, {n}) g(x));"
            ),
            "RESET enable_hashjoin;".to_string(),
            "RESET enable_nestloop;".to_string(),
        ]
    };
    aggx_fixture(probe)
}

/// Datum tuplesort with abbreviated keys under external-sort pressure
/// (ordered-set aggregate over md5 text, small work_mem).
fn aggx_datumsort(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:datumsort");
    let n = 6000 + g.rng.below(4000);
    vec![
        StmtKind::Raw("SET work_mem = '64kB';".to_string()),
        StmtKind::Raw(format!(
            "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY md5(g::text)) FROM generate_series(1, {n}) g;"
        )),
        StmtKind::Raw("RESET work_mem;".to_string()),
    ]
}

/// CREATE AGGREGATE bracket (aggregate-DDL argument checks) + use + drop.
fn aggx_aggddl(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggx:aggddl");
    let init = g.rng.below(10);
    aggx_fixture(vec![
        format!(
            "CREATE AGGREGATE fz_q7agg (int4) (SFUNC = int4pl, STYPE = int4, INITCOND = '{init}', FINALFUNC = int4larger, FINALFUNC_EXTRA = false);"
        ),
        "SELECT fz_q7agg(a) FROM fz_q7ag;".to_string(),
        "DROP AGGREGATE fz_q7agg (int4);".to_string(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::render::scope_errors;
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    #[test]
    fn agg_statements_are_scoped_and_varied() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xF4B);
        let mut sqls = String::new();
        for i in 0..600 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmt = gen_agg_stmt(&mut g);
            let errs = scope_errors(&stmt, &cat);
            assert!(errs.is_empty(), "stmt {i}: {errs:?}\n{}", stmt.to_sql());
            sqls.push_str(&stmt.to_sql());
            sqls.push('\n');
        }
        for frag in [
            " GROUP BY ",
            " HAVING ",
            "ROLLUP (",
            "CUBE (",
            "GROUPING SETS (",
            " FILTER (WHERE ",
            "string_agg(",
            "array_agg(",
            "count(DISTINCT ",
            "count(*)",
            "SELECT DISTINCT ",
            "sum(",
            "avg(",
            "bool_and(",
            "stddev",
            "var_",
            " ORDER BY ",
        ] {
            assert!(sqls.contains(frag), "aggregate flavor {frag:?} never generated");
        }
    }

    /// Ordered-input aggregates always ORDER BY their own first argument.
    #[test]
    fn ordered_input_aggs_order_by_their_argument() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse("agg:string_agg=20,agg:array_agg=20").unwrap();
        let mut rng = Rng::new(7);
        let mut seen = 0;
        for _ in 0..300 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmt = gen_agg_stmt(&mut g);
            for item in &stmt.items {
                seen += check_ordered_aggs(&item.expr);
            }
            if let Some(h) = &stmt.having {
                seen += check_ordered_aggs(h);
            }
        }
        assert!(seen > 20, "ordered-input aggregates rarely generated ({seen})");
    }

    fn check_ordered_aggs(e: &Expr) -> usize {
        let mut n = 0;
        if let Expr::Agg { name, args, order_within, .. } = e {
            match *name {
                "string_agg" | "array_agg" => {
                    let (oe, _) = order_within
                        .as_ref()
                        .unwrap_or_else(|| panic!("{name} without inner ORDER BY"));
                    assert_eq!(
                        oe.to_sql(),
                        args[0].to_sql(),
                        "{name} inner ORDER BY is not its first argument"
                    );
                    n += 1;
                }
                _ => assert!(order_within.is_none(), "{name} carries an inner ORDER BY"),
            }
        }
        for c in e.children() {
            n += check_ordered_aggs(c);
        }
        n
    }

    /// Group keys are never bare literals (the parser would read them as
    /// output-column ordinals or reject them as non-integer constants).
    #[test]
    fn group_keys_are_never_bare_literals() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(
            "agg:group=50,agg:groupby:expr=50,agg:groupby:col=0",
        )
        .unwrap();
        let mut rng = Rng::new(13);
        let mut keys = 0;
        for _ in 0..300 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmt = gen_agg_stmt(&mut g);
            if let Some(GroupBy::Plain(exprs)) = &stmt.group_by {
                for e in exprs {
                    keys += 1;
                    // Expr::Null renders as a cast (NULL::ty) — already safe.
                    assert!(
                        !matches!(e, Expr::Lit { .. }),
                        "bare literal group key: {}",
                        stmt.to_sql()
                    );
                }
            }
        }
        assert!(keys > 100, "expression group keys rarely generated ({keys})");
    }

    /// Ruled-soft float-aggregate columns: correctly reported, never under
    /// an ORDER BY/LIMIT suffix, and confined to top-level select items.
    #[test]
    fn soft_float_agg_statements_take_no_suffix() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(
            "agg:sum:float=50,agg:avg:float=50,agg:stddev:float=50,\
             orderby:none=0,orderby:total=5,limit=5,limit:none=0",
        )
        .unwrap();
        let mut rng = Rng::new(99);
        let mut soft_stmts = 0;
        for _ in 0..300 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmt = gen_agg_stmt(&mut g);
            let soft = soft_float_cols(&stmt);
            if soft.is_empty() {
                continue;
            }
            soft_stmts += 1;
            assert!(
                stmt.order_by.is_empty() && stmt.limit.is_none() && stmt.offset.is_none(),
                "soft float-agg statement carries an ORDER BY/LIMIT suffix: {}",
                stmt.to_sql()
            );
            for &i in &soft {
                assert!(stmt.items[i].ty.is_float());
                assert!(matches!(&stmt.items[i].expr, Expr::Agg { .. }));
            }
        }
        assert!(soft_stmts > 50, "soft float aggregates rarely generated ({soft_stmts})");
    }
}
