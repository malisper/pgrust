//! DML statement module: INSERT (single/multi-row VALUES, INSERT..SELECT,
//! column-list subsets, DEFAULT, ON CONFLICT DO NOTHING/DO UPDATE,
//! RETURNING), UPDATE (1-3 SET columns, WHERE shapes including rare
//! no-WHERE, UPDATE..FROM, RETURNING), DELETE (WHERE always, USING
//! occasionally, RETURNING).
//!
//! Statement-sequence validity (Griffin-style lightweight state, not full
//! simulation): `DmlState` persists across the session and tracks, per
//! table, a monotonic fresh-pk allocator plus the set of pk values ever
//! written. Fresh pks can never collide (rollbacks and deletes only leave
//! gaps, never reuse); "collide" picks draw from the known set to
//! deliberately hit rows — plausibly-existing for UPDATE/DELETE targeting
//! and ON CONFLICT exercises, rare outside ON CONFLICT so 23505s stay
//! deliberate, not spam. A known pk that a DELETE or ROLLBACK has since
//! removed simply matches zero rows — COUNT-compare fuel, not a defect.
//!
//! Determinism discipline (the probes compare stored state strictly):
//!   - values written to float4/float8 columns are depth-0 leaves (column
//!     copies or literals) so no engine-computed float ever lands in a
//!     table — stored floats stay byte-identical by induction, and the
//!     probes' ulp tolerance never has to absorb compounding drift;
//!   - the pk column is never SET by UPDATE and never DEFAULTed;
//!   - DEFAULT is only generated for nullable columns (the fixture has no
//!     column defaults, so DEFAULT deterministically means NULL);
//!   - UPDATE..FROM pins the joined row uniquely (t1.<pk> = <t0 column>)
//!     so "target row joins multiple source rows" nondeterminism cannot
//!     arise; INSERT..SELECT maps source pks through a reserved block
//!     (base + src pk) so inserted pks are deterministic and fresh;
//!   - at_most_one_row tables are never DML targets (the scalar-subquery
//!     module's single-row invariant is load-bearing);
//!   - no ctid or other system columns (only catalog columns are drawn).

use crate::catalog::{Catalog, Column, SqlType, Table};
use crate::expr::Expr;
use crate::scope::{Scope, ScopeRel};
use crate::stmt::{gen_expr_stmt, Gen, StmtKind};

/// Cap on the per-table known-pk list (plenty for targeting variety).
const KNOWN_CAP: usize = 512;

/// Session-persistent DML metadata, parallel to `catalog.tables`.
#[derive(Clone, Debug)]
pub struct DmlState {
    tables: Vec<TableState>,
}

#[derive(Clone, Debug)]
struct TableState {
    /// Catalog table name (the sync key when DDL reshapes the catalog).
    name: String,
    /// Next fresh pk; monotonic, never reused (rollback/delete-safe).
    next_pk: i64,
    /// Pk values ever written (seeded + generated); a superset of the live
    /// set, good enough for plausible targeting and deliberate collisions.
    known: Vec<i64>,
}

impl TableState {
    fn fresh(t: &Table) -> TableState {
        match &t.pk {
            Some(pk) => TableState {
                name: t.name.clone(),
                next_pk: pk.seeded_max + 1,
                known: (1..=pk.seeded_max).collect(),
            },
            None => TableState { name: t.name.clone(), next_pk: 1, known: Vec::new() },
        }
    }
}

impl DmlState {
    pub fn new(catalog: &Catalog) -> DmlState {
        DmlState { tables: catalog.tables.iter().map(TableState::fresh).collect() }
    }

    /// Re-align to a reshaped catalog (the session loop calls this after
    /// every DDL group): per-name states carry over, new tables (ddl-
    /// created, seeded_max 0) get fresh monotonic allocators, dropped
    /// tables' states fall away. Names are never reused (ddl counters are
    /// monotonic), so a carried-over state can never belong to a different
    /// table of the same name.
    pub fn sync(&mut self, catalog: &Catalog) {
        self.tables = catalog
            .tables
            .iter()
            .map(|t| {
                match self.tables.iter().find(|ts| ts.name == t.name) {
                    Some(ts) => ts.clone(),
                    None => TableState::fresh(t),
                }
            })
            .collect();
    }

    fn alloc(&mut self, ti: usize) -> i64 {
        let t = &mut self.tables[ti];
        let v = t.next_pk;
        t.next_pk += 1;
        if t.known.len() < KNOWN_CAP {
            t.known.push(v);
        }
        v
    }

    /// Reserve `span` pk values at once (INSERT..SELECT block mapping);
    /// returns the block base.
    fn alloc_block(&mut self, ti: usize, span: i64) -> i64 {
        let t = &mut self.tables[ti];
        let base = t.next_pk;
        t.next_pk += span.max(1);
        base
    }
}

impl Gen<'_> {
    pub(crate) fn pk_fresh(&mut self, ti: usize) -> i64 {
        self.dml.alloc(ti)
    }

    /// A pk that was written at some point (may have been deleted or rolled
    /// back since — that is COUNT-compare fuel, not a bug).
    pub(crate) fn pk_known(&mut self, ti: usize) -> Option<i64> {
        let known = &self.dml.tables[ti].known;
        if known.is_empty() {
            None
        } else {
            Some(known[self.rng.below_usize(known.len())])
        }
    }
}

/// `&'a Table` by index, outliving any `&mut Gen` borrow (same trick as
/// `Gen::pick_table`).
pub(crate) fn table_of<'a>(g: &Gen<'a>, ti: usize) -> &'a Table {
    &g.catalog.tables[ti]
}

/// Indices of DML-eligible tables: multi-row (writable without breaking the
/// scalar-subquery single-row invariant) and pk-carrying.
pub(crate) fn eligible_tables(g: &Gen) -> Vec<usize> {
    g.catalog
        .tables
        .iter()
        .enumerate()
        .filter(|(_, t)| t.pk.is_some() && !t.at_most_one_row)
        .map(|(i, _)| i)
        .collect()
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_dml_module(g: &mut Gen) -> Vec<StmtKind> {
    match gen_dml_sql(g) {
        Some(sql) => vec![StmtKind::Raw(sql)],
        // No DML-eligible table (e.g. live catalogs, which carry no pk
        // metadata): degrade to a plain read so the module always produces.
        None => {
            g.fire("dml:fallback_select");
            vec![StmtKind::Select(Box::new(gen_expr_stmt(g)))]
        }
    }
}

/// One DML statement (also the txn module's write-statement generator).
/// None when the catalog exposes no DML-eligible table.
pub fn gen_dml_sql(g: &mut Gen) -> Option<String> {
    let eligible = eligible_tables(g);
    if eligible.is_empty() {
        return None;
    }
    let ti = eligible[g.rng.below_usize(eligible.len())];
    let sql = match g.weights.pick(g.rng, &["dml:insert", "dml:update", "dml:delete"]) {
        "dml:insert" => gen_insert(g, ti),
        "dml:update" => gen_update(g, ti),
        _ => gen_delete(g, ti),
    };
    Some(sql)
}

/// Value expression for a written column, honoring the determinism
/// discipline: float columns get depth-0 leaves; NOT NULL columns are
/// wrapped in coalesce with a literal fallback so accidental NULLs don't
/// turn most writes into 23502 noise (deliberate constraint errors still
/// arise from semantic failures inside the expression).
pub(crate) fn value_expr(g: &mut Gen, scope: &Scope, c: &Column) -> String {
    let depth = if c.ty.is_float() { 0 } else { 2 };
    let e = g.gen_typed(scope, c.ty, depth);
    if c.nullable {
        e.to_sql()
    } else {
        let fallback = Expr::Lit { sql: g.gen_literal(c.ty) };
        Expr::Func { name: "coalesce", args: vec![e, fallback] }.to_sql()
    }
}

/// One `SET` assignment for `c`, possibly through a subscripted target
/// (G2): jsonb subscript writes (`col['k'] = ...`, jsonbsubs.c assign arm,
/// including path creation over NULL/missing keys) and array element/slice
/// assignment (`col[i] = ...`, `col[lo:hi] = ...` — array_set_element /
/// array_set_slice, with the auto-extension semantics as a differential
/// surface). Non-subscriptable columns always take the plain form. All
/// families hand-verified byte-identical on both engines (G2 decks 1-2,
/// 2026-08-11); blocked-path jsonb assigns ("cannot replace existing key")
/// and short-source slice assigns are matched errors on both sides.
pub(crate) fn render_set_assign(g: &mut Gen, scope: &Scope, c: &Column) -> String {
    let sub_ok = matches!(c.ty, SqlType::Jsonb | SqlType::Int4Arr | SqlType::TextArr);
    if !sub_ok
        || g.weights.pick(g.rng, &["dml:set:plain", "dml:set:sub"]) == "dml:set:plain"
    {
        return format!("{} = {}", c.name, value_expr(g, scope, c));
    }
    match c.ty {
        SqlType::Jsonb => {
            let chain = g.rng.chance(1, 4);
            g.fire(if chain { "dml:set:jsonb_chain" } else { "dml:set:jsonb_sub" });
            let mut target = format!("{}[{}]", c.name, g.gen_jsonb_subscript());
            if chain {
                target.push_str(&format!("[{}]", g.gen_jsonb_subscript()));
            }
            // RHS is jsonb; shallow so most writes succeed (deliberate
            // errors still arise from blocked paths / scalar bases).
            let v = g.gen_typed(scope, SqlType::Jsonb, 1);
            format!("{} = {}", target, v.to_sql())
        }
        arr_ty => {
            let el_ty = if arr_ty == SqlType::Int4Arr { SqlType::Int4 } else { SqlType::Text };
            if g.rng.chance(1, 2) {
                g.fire("dml:set:arr_elem");
                // Indexes past the end auto-extend (NULL-padded) — the
                // array_set_element extension surface; negative indexes
                // extend downward.
                let idx = g.rng_i64(-1, 7);
                let v = g.gen_typed(scope, el_ty, 1);
                format!("{}[{}] = {}", c.name, idx, v.to_sql())
            } else {
                g.fire("dml:set:arr_slice");
                let spec = match g.rng.below(4) {
                    0 => {
                        let lo = g.rng_i64(-1, 3);
                        format!("{}:{}", lo, lo + g.rng_i64(0, 2))
                    }
                    1 => format!(":{}", g.rng_i64(1, 3)),
                    2 => format!("{}:", g.rng_i64(1, 4)),
                    _ => "2:4".to_string(),
                };
                let v = g.gen_typed(scope, arr_ty, 1);
                format!("{}[{}] = {}", c.name, spec, v.to_sql())
            }
        }
    }
}

/// Column list for an INSERT: pk and NOT NULL columns always (neither has a
/// default, so omission means a guaranteed 23502), each nullable column
/// with probability 3/4 (column-list subsets).
pub(crate) fn insert_columns<'a>(g: &mut Gen, table: &'a Table) -> Vec<&'a Column> {
    table
        .columns
        .iter()
        .filter(|c| table.is_pk_column(&c.name) || !c.nullable || g.rng.chance(3, 4))
        .collect()
}

/// Pk literal for one inserted row. `conflicty` biases toward known pks
/// (ON CONFLICT exercises); otherwise collisions are rare and deliberate.
/// `used` keeps within-statement pks distinct (a multi-row insert hitting
/// the same key twice is a 21000/23505 both-side error — wasted budget).
/// Tables without a unique pk constraint (`Table::pk_unique` false, e.g.
/// partitioned parents keyed off-pk) always allocate fresh: a collision
/// there would silently insert a duplicate row and break the state probes'
/// pk total order.
fn insert_pk(g: &mut Gen, ti: usize, conflicty: bool, used: &mut Vec<i64>) -> i64 {
    if !table_of(g, ti).pk_unique {
        g.fire("dml:pk:fresh");
        let pk = g.pk_fresh(ti);
        used.push(pk);
        return pk;
    }
    let options: &[&str] = if conflicty {
        &["dml:onconflict:hit", "dml:onconflict:miss"]
    } else {
        &["dml:pk:fresh", "dml:pk:collide"]
    };
    let picked = g.weights.pick(g.rng, options);
    let collide = picked == "dml:onconflict:hit" || picked == "dml:pk:collide";
    let pk = if collide {
        match g.pk_known(ti) {
            Some(k) if !used.contains(&k) => {
                g.fire(picked);
                k
            }
            _ => g.pk_fresh(ti),
        }
    } else {
        g.fire(picked);
        g.pk_fresh(ti)
    };
    used.push(pk);
    pk
}

fn render_returning(g: &mut Gen, scope: &Scope, out: &mut String) {
    if g.weights.pick(g.rng, &["dml:returning", "dml:returning:none"]) != "dml:returning" {
        return;
    }
    g.fire("dml:returning");
    out.push_str(" RETURNING ");
    let n = 1 + g.rng.below_usize(3);
    for i in 0..n {
        if i > 0 {
            out.push_str(", ");
        }
        let ty = g.any_type(scope);
        out.push_str(&g.gen_typed(scope, ty, 2).to_sql());
    }
}

fn gen_insert(g: &mut Gen, ti: usize) -> String {
    let table = table_of(g, ti);
    let pk_col = &table.pk.as_ref().expect("eligible table has pk").column;
    g.fire("dml:insert");
    let shape = g
        .weights
        .pick(g.rng, &["dml:insert:single", "dml:insert:multirow", "dml:insert:select"]);
    g.fire(shape);
    if shape == "dml:insert:select" {
        return gen_insert_select(g, ti);
    }
    let cols = insert_columns(g, table);
    let alias = g.next_alias();
    let mut out = format!("INSERT INTO {} AS {} (", table.name, alias);
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&c.name);
    }
    out.push_str(") VALUES ");

    // ON CONFLICT decided before the rows so pk picks can aim at it.
    // Only offered where a unique constraint on pk actually exists:
    // ON CONFLICT (pk) without a matching unique index is a 42P10.
    let conflict = if table.pk_unique {
        g.weights.pick(
            g.rng,
            &["dml:onconflict:none", "dml:onconflict:nothing", "dml:onconflict:update"],
        )
    } else {
        "dml:onconflict:none"
    };
    let conflicty = conflict != "dml:onconflict:none";
    let nrows = if shape == "dml:insert:multirow" { 2 + g.rng.below_usize(3) } else { 1 };
    let empty = Scope { rels: &[], outer: None };
    let mut used: Vec<i64> = Vec::new();
    for r in 0..nrows {
        if r > 0 {
            out.push_str(", ");
        }
        out.push('(');
        let pk = insert_pk(g, ti, conflicty, &mut used);
        for (i, c) in cols.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            if table.is_pk_column(&c.name) {
                out.push_str(&pk.to_string());
            } else if c.nullable
                && g.weights.pick(g.rng, &["dml:insert:value", "dml:insert:default"])
                    == "dml:insert:default"
            {
                g.fire("dml:insert:default");
                out.push_str("DEFAULT");
            } else {
                out.push_str(&value_expr(g, &empty, c));
            }
        }
        out.push(')');
    }

    if conflicty {
        g.fire(conflict);
        out.push_str(&format!(" ON CONFLICT ({}) ", pk_col));
        if conflict == "dml:onconflict:nothing" {
            out.push_str("DO NOTHING");
        } else {
            // DO UPDATE over the conflicting row (alias) and EXCLUDED.
            out.push_str("DO UPDATE SET ");
            let rels = vec![
                ScopeRel::from_table(table, alias.clone()),
                ScopeRel::from_table(table, "excluded".to_string()),
            ];
            let scope = Scope { rels: &rels, outer: None };
            let set_cols = pick_set_columns(g, table, 2);
            for (i, c) in set_cols.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&render_set_assign(g, &scope, c));
            }
        }
    }

    let rels = vec![ScopeRel::from_table(table, alias)];
    let scope = Scope { rels: &rels, outer: None };
    render_returning(g, &scope, &mut out);
    out.push(';');
    out
}

/// INSERT..SELECT: copy a bounded slice of a source table through
/// deterministic per-row expressions, mapping pks into a freshly reserved
/// block (base + source pk — unique because source pks are). The source is
/// bounded to the seeded pk range so repeated self-inserts grow the table
/// linearly, not geometrically.
fn gen_insert_select(g: &mut Gen, ti: usize) -> String {
    let table = table_of(g, ti);
    let eligible = eligible_tables(g);
    let src_ti = eligible[g.rng.below_usize(eligible.len())];
    let src = table_of(g, src_ti);
    let src_pk = src.pk.as_ref().expect("eligible table has pk");
    let span = src_pk.seeded_max + 1;
    let base = g.dml.alloc_block(ti, span);

    let cols = insert_columns(g, table);
    let src_alias = g.next_alias();
    let rels = vec![ScopeRel::from_table(src, src_alias.clone())];
    let scope = Scope { rels: &rels, outer: None };

    let mut out = format!("INSERT INTO {} (", table.name);
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&c.name);
    }
    out.push_str(") SELECT ");
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        if table.is_pk_column(&c.name) {
            out.push_str(&format!("({}.{} + {})", src_alias, src_pk.column, base));
        } else {
            out.push_str(&value_expr(g, &scope, c));
        }
    }
    out.push_str(&format!(
        " FROM {} AS {} WHERE {}.{} <= {}",
        src.name, src_alias, src_alias, src_pk.column, src_pk.seeded_max
    ));
    if g.rng.chance(1, 2) {
        out.push_str(" AND ");
        out.push_str(&g.gen_bool(&scope, 2).to_sql());
    }
    out.push(';');
    out
}

/// 1..=max_n distinct non-pk SET target columns.
pub(crate) fn pick_set_columns<'a>(g: &mut Gen, table: &'a Table, max_n: usize) -> Vec<&'a Column> {
    let candidates: Vec<&Column> = table
        .columns
        .iter()
        .filter(|c| !table.is_pk_column(&c.name))
        .collect();
    let picked = g.weights.pick(g.rng, &["dml:set:1", "dml:set:2", "dml:set:3"]);
    g.fire(picked);
    let n = match picked {
        "dml:set:1" => 1,
        "dml:set:2" => 2,
        _ => 3,
    }
    .min(max_n)
    .min(candidates.len());
    let mut picked: Vec<usize> = Vec::with_capacity(n);
    while picked.len() < n {
        let i = g.rng.below_usize(candidates.len());
        if !picked.contains(&i) {
            picked.push(i);
        }
    }
    picked.into_iter().map(|i| candidates[i]).collect()
}

/// WHERE for plain UPDATE/DELETE: pk-targeted (plausible rows), a random
/// predicate, or — UPDATE only, low weight — none at all.
fn render_where_plain(
    g: &mut Gen,
    ti: usize,
    alias: &str,
    scope: &Scope,
    allow_none: bool,
    out: &mut String,
) {
    let options: &[&str] = if allow_none {
        &["dml:where:pk", "dml:where:expr", "dml:where:none"]
    } else {
        &["dml:where:pk", "dml:where:expr"]
    };
    match g.weights.pick(g.rng, options) {
        "dml:where:pk" => {
            g.fire("dml:where:pk");
            let table = table_of(g, ti);
            let pk_col = table.pk.as_ref().expect("eligible table has pk").column.clone();
            let k = g.pk_known(ti).unwrap_or(1);
            let op = if g.rng.chance(1, 3) { "<=" } else { "=" };
            out.push_str(&format!(" WHERE {}.{} {} {}", alias, pk_col, op, k));
        }
        "dml:where:expr" => {
            g.fire("dml:where:expr");
            out.push_str(" WHERE ");
            out.push_str(&g.gen_bool(scope, 2).to_sql());
        }
        _ => {
            g.fire("dml:where:none");
        }
    }
}

/// The unique-pin join for UPDATE..FROM / DELETE..USING: `t1.<pk> = t0.<int4
/// col>` guarantees at most one joined row per target row (pk is unique),
/// so which source row supplies SET values is never ambiguous.
fn joined_pin(g: &mut Gen, target: &Table, target_alias: &str, other_ti: usize, other_alias: &str) -> String {
    let other = table_of(g, other_ti);
    let other_pk = &other.pk.as_ref().expect("eligible table has pk").column;
    let int_cols: Vec<&Column> = target
        .columns
        .iter()
        .filter(|c| c.ty == SqlType::Int4)
        .collect();
    let c = int_cols[g.rng.below_usize(int_cols.len())];
    format!("{}.{} = {}.{}", other_alias, other_pk, target_alias, c.name)
}

fn gen_update(g: &mut Gen, ti: usize) -> String {
    let table = table_of(g, ti);
    g.fire("dml:update");
    let alias = g.next_alias();
    let form = g.weights.pick(g.rng, &["dml:update:plain", "dml:update:from"]);

    let mut out = format!("UPDATE {} AS {} SET ", table.name, alias);
    if form == "dml:update:from" {
        g.fire("dml:update:from");
        // Any pk-carrying table works as a read-only join source.
        let sources: Vec<usize> = g
            .catalog
            .tables
            .iter()
            .enumerate()
            .filter(|(_, t)| t.pk.is_some())
            .map(|(i, _)| i)
            .collect();
        let other_ti = sources[g.rng.below_usize(sources.len())];
        let other = table_of(g, other_ti);
        let other_alias = g.next_alias();
        let rels = vec![
            ScopeRel::from_table(table, alias.clone()),
            ScopeRel::from_table(other, other_alias.clone()),
        ];
        let scope = Scope { rels: &rels, outer: None };
        let set_cols = pick_set_columns(g, table, 3);
        for (i, c) in set_cols.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(&render_set_assign(g, &scope, c));
        }
        out.push_str(&format!(" FROM {} AS {}", other.name, other_alias));
        out.push_str(" WHERE ");
        out.push_str(&joined_pin(g, table, &alias, other_ti, &other_alias));
        if g.rng.chance(1, 2) {
            out.push_str(" AND ");
            out.push_str(&g.gen_bool(&scope, 2).to_sql());
        }
        render_returning(g, &scope, &mut out);
    } else {
        let rels = vec![ScopeRel::from_table(table, alias.clone())];
        let scope = Scope { rels: &rels, outer: None };
        let set_cols = pick_set_columns(g, table, 3);
        for (i, c) in set_cols.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(&render_set_assign(g, &scope, c));
        }
        render_where_plain(g, ti, &alias, &scope, true, &mut out);
        render_returning(g, &scope, &mut out);
    }
    out.push(';');
    out
}

fn gen_delete(g: &mut Gen, ti: usize) -> String {
    let table = table_of(g, ti);
    g.fire("dml:delete");
    let alias = g.next_alias();
    let form = g.weights.pick(g.rng, &["dml:delete:plain", "dml:delete:using"]);
    let mut out = format!("DELETE FROM {} AS {}", table.name, alias);
    if form == "dml:delete:using" {
        g.fire("dml:delete:using");
        let sources: Vec<usize> = g
            .catalog
            .tables
            .iter()
            .enumerate()
            .filter(|(_, t)| t.pk.is_some())
            .map(|(i, _)| i)
            .collect();
        let other_ti = sources[g.rng.below_usize(sources.len())];
        let other = table_of(g, other_ti);
        let other_alias = g.next_alias();
        let rels = vec![
            ScopeRel::from_table(table, alias.clone()),
            ScopeRel::from_table(other, other_alias.clone()),
        ];
        let scope = Scope { rels: &rels, outer: None };
        out.push_str(&format!(" USING {} AS {}", other.name, other_alias));
        out.push_str(" WHERE ");
        out.push_str(&joined_pin(g, table, &alias, other_ti, &other_alias));
        if g.rng.chance(1, 2) {
            out.push_str(" AND ");
            out.push_str(&g.gen_bool(&scope, 2).to_sql());
        }
        render_returning(g, &scope, &mut out);
    } else {
        let rels = vec![ScopeRel::from_table(table, alias.clone())];
        let scope = Scope { rels: &rels, outer: None };
        // DELETE always carries a WHERE (an unconditioned DELETE nukes the
        // table and starves the rest of the stream).
        render_where_plain(g, ti, &alias, &scope, false, &mut out);
        render_returning(g, &scope, &mut out);
    }
    out.push(';');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_many(seed: u64, n: usize, spec: &str) -> (Vec<String>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut sqls = Vec::new();
        let mut prods = Vec::new();
        let mut state: Option<DmlState> = None;
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            if let Some(s) = state.take() {
                g.dml = s;
            }
            let sql = gen_dml_sql(&mut g).expect("fixture has eligible tables");
            state = Some(std::mem::replace(&mut g.dml, DmlState::new(&cat)));
            sqls.push(sql);
            prods.extend(p);
        }
        (sqls, prods)
    }

    #[test]
    fn dml_is_deterministic() {
        let (a, _) = gen_many(11, 50, "");
        let (b, _) = gen_many(11, 50, "");
        assert_eq!(a, b);
        let (c, _) = gen_many(12, 50, "");
        assert_ne!(a, c);
    }

    #[test]
    fn dml_variety_and_invariants() {
        let (sqls, prods) = gen_many(0xD41, 800, "");
        let all = sqls.join("\n");
        for frag in [
            "INSERT INTO ",
            "UPDATE ",
            "DELETE FROM ",
            " VALUES ",
            " ON CONFLICT (",
            "DO NOTHING",
            "DO UPDATE SET ",
            " RETURNING ",
            " FROM ",
            " USING ",
            "DEFAULT",
            ") SELECT ",
        ] {
            assert!(all.contains(frag), "DML flavor {frag:?} never generated");
        }
        for p in [
            "dml:insert",
            "dml:insert:single",
            "dml:insert:multirow",
            "dml:insert:select",
            "dml:insert:default",
            "dml:update",
            "dml:update:from",
            "dml:delete",
            "dml:delete:using",
            "dml:onconflict:nothing",
            "dml:onconflict:update",
            "dml:onconflict:hit",
            "dml:pk:fresh",
            "dml:pk:collide",
            "dml:returning",
            "dml:where:pk",
            "dml:where:expr",
            "dml:where:none",
            "dml:set:1",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        for sql in &sqls {
            // Never target the single-row/empty fixtures; never touch
            // system columns; single-line, terminated, paren-balanced.
            for t in ["fz_one", "fz_empty"] {
                assert!(!sql.starts_with(&format!("INSERT INTO {t}")), "{sql}");
                assert!(!sql.starts_with(&format!("UPDATE {t}")), "{sql}");
                assert!(!sql.starts_with(&format!("DELETE FROM {t}")), "{sql}");
            }
            assert!(!sql.contains("ctid"), "system column reference: {sql}");
            assert!(!sql.contains('\n') && sql.ends_with(';'), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
            // DELETE always has a WHERE.
            if sql.starts_with("DELETE FROM ") {
                assert!(sql.contains(" WHERE "), "unguarded DELETE: {sql}");
            }
            // UPDATE never SETs the pk column.
            if let Some(rest) = sql.strip_prefix("UPDATE ") {
                let set = rest.split_once(" SET ").unwrap().1;
                let set = set
                    .split(" WHERE ")
                    .next()
                    .unwrap()
                    .split(" FROM ")
                    .next()
                    .unwrap()
                    .split(" RETURNING ")
                    .next()
                    .unwrap();
                for assign in set.split(", ") {
                    if let Some((col, _)) = assign.split_once(" = ") {
                        if !col.contains('(') {
                            assert!(
                                col != "pk" && col != "id",
                                "UPDATE sets pk column: {sql}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn insert_column_lists_always_carry_pk_and_notnull() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let (sqls, _) = gen_many(77, 400, "dml:insert=1,dml:update=0,dml:delete=0");
        for sql in &sqls {
            let Some(rest) = sql.strip_prefix("INSERT INTO ") else { continue };
            let table_name = rest.split([' ', '(']).next().unwrap();
            let table = cat.tables.iter().find(|t| t.name == table_name).unwrap();
            let cols_txt = rest.split_once('(').unwrap().1.split_once(')').unwrap().0;
            let cols: Vec<&str> = cols_txt.split(", ").collect();
            for c in &table.columns {
                if table.is_pk_column(&c.name) || !c.nullable {
                    assert!(
                        cols.contains(&c.name.as_str()),
                        "{}: required column {} missing from list: {sql}",
                        table.name,
                        c.name
                    );
                }
            }
        }
    }

    #[test]
    fn multirow_insert_pks_are_distinct() {
        // Collision-heavy weights: within one statement pks must still be
        // distinct (avoids 21000 "affect row a second time" spam).
        let (sqls, _) = gen_many(
            5,
            400,
            "dml:insert=1,dml:update=0,dml:delete=0,dml:insert:multirow=10,\
             dml:insert:single=0,dml:insert:select=0,dml:pk:collide=10,dml:pk:fresh=1,\
             dml:onconflict:nothing=5,dml:onconflict:update=5",
        );
        let mut saw_multi = false;
        for sql in &sqls {
            let Some(vals) = sql.split_once(" VALUES ") else { continue };
            // Scan the VALUES section only (ON CONFLICT/RETURNING tails
            // carry their own parenthesized groups).
            let vals = vals.1.split(" ON CONFLICT ").next().unwrap();
            let vals = vals.split(" RETURNING ").next().unwrap();
            // First value of each depth-1 row group is the pk literal
            // (paren-depth scan: value expressions contain nested parens).
            let mut pks: Vec<String> = Vec::new();
            let mut depth = 0u32;
            let mut cur: Option<String> = None;
            for ch in vals.chars() {
                match ch {
                    '(' => {
                        depth += 1;
                        if depth == 1 {
                            cur = Some(String::new());
                        }
                    }
                    ')' => depth = depth.saturating_sub(1),
                    ',' if depth == 1 => {
                        if let Some(pk) = cur.take() {
                            pks.push(pk);
                        }
                    }
                    c => {
                        if depth == 1 {
                            if let Some(s) = cur.as_mut() {
                                s.push(c);
                            }
                        }
                    }
                }
            }
            if pks.len() > 1 {
                saw_multi = true;
                let mut sorted = pks.clone();
                sorted.sort();
                sorted.dedup();
                assert_eq!(sorted.len(), pks.len(), "duplicate pk in one insert: {sql}");
            }
        }
        assert!(saw_multi, "no multi-row insert generated under heavy bias");
    }

    #[test]
    fn state_is_monotonic_and_survives_handoff() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut st = DmlState::new(&cat);
        let a = st.alloc(0);
        let b = st.alloc(0);
        assert_eq!(a, 9, "fz_scalar fresh pks start above seeded_max=8");
        assert_eq!(b, 10);
        let base = st.alloc_block(0, 9);
        assert_eq!(base, 11);
        assert_eq!(st.alloc(0), 20, "block reservation advances the allocator");
        // fz_mixed seeded 1..=6.
        assert_eq!(st.alloc(1), 7);
    }
}
