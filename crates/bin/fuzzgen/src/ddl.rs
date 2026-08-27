//! DDL statement module with a generation-time catalog model.
//!
//! `DdlState` (session-persistent, swapped in and out of `Gen` exactly like
//! `DmlState`) tracks every object this module creates — tables, indexes,
//! sequences, per-table triggers — so later statements are valid by
//! construction: nothing is ever referenced after its drop, names are
//! allocated from monotonic counters and never reused, and fixture tables
//! are never dropped/altered/truncated (other modules depend on them). A
//! 42xxx from a generated statement is a generator bug (zero-42xxx gate).
//!
//! Cross-module reuse (where the deep coverage comes from): the session
//! loop rebuilds an *effective catalog* (fixture tables + live ddl tables)
//! after every ddl group, so the expr/joins/subq/agg/win/dml modules target
//! ddl-created tables in later statements, and `DmlState` grows synced
//! entries for them (name-keyed; see `DmlState::sync`). Every ddl table
//! carries the fixture join-key pair `k_int`/`k_text` plus an int4 `pk`
//! PRIMARY KEY (`unique_key` hint = pk), so join/window/DML invariants hold
//! unchanged. The protected trio {pk, k_int, k_text} is never dropped,
//! renamed, or type-altered.
//!
//! Object lifecycles:
//!   - tables/indexes/sequences persist across groups (state-tracked);
//!     `DdlEvent`s let the session loop derive probe windows so state-sync
//!     probes cover ddl tables that still exist at probe time;
//!   - views and aggregates are create/query-once/drop inside one group
//!     (no cross-group dependency edges, so DROP TABLE can never fail on a
//!     dependent view — the simple option the charter allows);
//!   - trigger functions are created once and never dropped (dropping one
//!     with a live trigger would need dependency tracking for no coverage
//!     gain); triggers die with their table or via DROP TRIGGER.
//!
//! Validity disciplines that earn the zero-42xxx / determinism gates:
//!   - index expressions and partial-index predicates come from a bespoke
//!     IMMUTABLE-only mini-grammar (no `gen_typed` reuse: casts like
//!     date→text are only STABLE and would 42P17 inside CREATE INDEX);
//!   - `(c % 97)` (never `c + 1`) as the int expression-index shape, so
//!     boundary int inserts into indexed tables cannot 22003 at the index;
//!   - the gist expression clamps through `least(c, 2000000000)` so the
//!     range constructor can neither overflow nor invert its bounds;
//!   - UNIQUE indexes always lead with the pk column, so creation can
//!     never fail over existing rows (a failed CREATE INDEX would desync
//!     the state model);
//!   - ALTER TYPE only widens (or casts to text with USING) and skips
//!     columns referenced by live indexes or CHECK constraints (re-coercion
//!     of a dependent expression would 42883/42P17);
//!   - ADD COLUMN NOT NULL always carries a DEFAULT (guaranteed 23502 on a
//!     populated table otherwise);
//!   - CREATE AGGREGATE sticks to commutative/associative int sfuncs
//!     (int4pl/int4larger/int4smaller/int8pl): a textcat-style sfunc would
//!     surface scan-order differences as false findings;
//!   - trigger bodies are deterministic plpgsql one-liners (RETURN
//!     NEW/OLD/NULL, protected-column bumps/appends, TG_OP tags, TG_ARGV,
//!     pk-keyed row suppression); DELETE row triggers only ever touch OLD,
//!     WHEN clauses use NEW for INSERT/UPDATE rows and OLD for DELETE
//!     rows; P2 adds transition-table statement triggers (REFERENCING
//!     NEW/OLD TABLE) and deferred constraint triggers.
//!
//! SET NOT NULL is applied optimistically to the model: if the server
//! rejects it (NULLs present — a matched both-side error), the model's
//! NOT NULL view is *conservative* (DML always supplies such columns via
//! coalesce), so no later statement can become invalid either way.

use crate::catalog::{Catalog, Column, Pk, SqlType, Table, ALL_TYPES};
use crate::stmt::{gen_expr_stmt, Gen, StmtKind};

/// Columns every ddl table carries and the module never drops, renames, or
/// re-types (join keys other modules ride on + the pk probe sort key).
pub const PROTECTED_COLS: &[&str] = &["pk", "k_int", "k_text"];

/// Caps keeping the live-object population bounded over long streams.
const MAX_LIVE_TABLES: usize = 12;
const MAX_LIVE_INDEXES: usize = 24;
const MAX_LIVE_SEQS: usize = 8;
const MAX_COLS: usize = 20;

#[derive(Clone, Debug)]
pub struct DdlTable {
    pub table: Table,
    pub live: bool,
    pub temp: bool,
    /// Columns referenced by a CHECK constraint (never ALTER TYPEd).
    pub checked_cols: Vec<String>,
    /// Live trigger names on this table (die with the table).
    pub triggers: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct DdlIndex {
    pub name: String,
    pub table: String,
    /// Column names the index references (keys, expressions, predicate);
    /// the index dies when any of them is dropped (DROP COLUMN CASCADE).
    pub cols: Vec<String>,
    pub live: bool,
}

#[derive(Clone, Debug)]
pub struct DdlSeq {
    pub name: String,
    pub live: bool,
    /// nextval() has run in this session (currval becomes legal).
    pub has_nextval: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DdlEventKind {
    Created,
    Dropped,
}

/// Table-existence event, resolved by the session loop into a probe window
/// (ddl statement groups that create/drop tables are single-statement, so
/// the group's base index is the event's exact statement index).
#[derive(Clone, Debug)]
pub struct DdlEvent {
    pub table: String,
    pub pk: String,
    pub kind: DdlEventKind,
}

/// Session-persistent DDL catalog model.
#[derive(Clone, Debug, Default)]
pub struct DdlState {
    pub tables: Vec<DdlTable>,
    pub indexes: Vec<DdlIndex>,
    pub seqs: Vec<DdlSeq>,
    /// Tables that have EVER been named as an FK referenced table (X1
    /// foreign keys). Their DROP/TRUNCATE statements render with CASCADE so
    /// a referencing constraint can never make the statement fail and
    /// desync the model. Never pruned: CASCADE on a table whose referencing
    /// constraints are already gone is a no-op.
    pub fk_parents: Vec<String>,
    next_table: u32,
    next_index: u32,
    next_seq: u32,
    next_view: u32,
    next_agg: u32,
    next_trig: u32,
    next_col: u32,
    events: Vec<DdlEvent>,
}

impl DdlState {
    pub fn new() -> DdlState {
        DdlState::default()
    }

    /// Fixture catalog + live ddl tables, in creation order (deterministic).
    /// The session loop feeds this to every later group, which is how other
    /// modules come to target ddl-created tables.
    pub fn extend_catalog(&self, base: &Catalog) -> Catalog {
        let mut cat = base.clone();
        for t in &self.tables {
            if t.live {
                cat.tables.push(t.table.clone());
            }
        }
        cat
    }

    /// Drain pending create/drop events (session loop resolves indices).
    pub fn take_events(&mut self) -> Vec<DdlEvent> {
        std::mem::take(&mut self.events)
    }

    fn live_tables(&self) -> Vec<usize> {
        self.tables
            .iter()
            .enumerate()
            .filter(|(_, t)| t.live)
            .map(|(i, _)| i)
            .collect()
    }

    fn live_indexes(&self) -> Vec<usize> {
        self.indexes
            .iter()
            .enumerate()
            .filter(|(_, x)| x.live)
            .map(|(i, _)| i)
            .collect()
    }

    fn live_seqs(&self) -> Vec<usize> {
        self.seqs
            .iter()
            .enumerate()
            .filter(|(_, s)| s.live)
            .map(|(i, _)| i)
            .collect()
    }

    fn drop_table(&mut self, ti: usize) {
        let name = self.tables[ti].table.name.clone();
        self.tables[ti].live = false;
        self.tables[ti].triggers.clear();
        for x in &mut self.indexes {
            if x.table == name {
                x.live = false;
            }
        }
        self.events.push(DdlEvent {
            table: name,
            pk: "pk".to_string(),
            kind: DdlEventKind::Dropped,
        });
    }
}

fn is_protected(name: &str) -> bool {
    PROTECTED_COLS.contains(&name)
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_ddl_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("ddl");
    let action = g.weights.pick(
        g.rng,
        &[
            "ddl:create_table",
            "ddl:drop_table",
            "ddl:alter",
            "ddl:create_index",
            "ddl:drop_index",
            "ddl:reindex",
            "ddl:view",
            "ddl:seq",
            "ddl:agg",
            "ddl:trigger",
            "ddl:truncate",
            "ddl:fk",
        ],
    );
    match action {
        "ddl:create_table" => vec![gen_create_table(g)],
        "ddl:drop_table" => vec![gen_drop_table(g)],
        "ddl:alter" => vec![gen_alter_table(g)],
        "ddl:create_index" => vec![gen_create_index(g)],
        "ddl:drop_index" => vec![gen_drop_index(g)],
        "ddl:reindex" => vec![gen_reindex(g)],
        "ddl:view" => gen_view_group(g),
        "ddl:seq" => vec![gen_sequence(g)],
        "ddl:agg" => gen_aggregate_group(g),
        "ddl:trigger" => gen_trigger_group(g),
        "ddl:fk" => vec![gen_foreign_key(g)],
        _ => vec![gen_truncate(g)],
    }
}

// ------------------------------------------------------------- tables ----

fn palette_type(g: &mut Gen) -> SqlType {
    ALL_TYPES[g.rng.below_usize(ALL_TYPES.len())]
}

/// Optional single-column CHECK, immutable and mostly-true (occasionally
/// biting on ints — deliberate 23514s, never 42xxx). Int + text only.
fn column_check(g: &mut Gen, name: &str, ty: SqlType) -> Option<String> {
    if !(ty.is_integer() || ty.is_text_family()) {
        return None;
    }
    if g.weights.pick(g.rng, &["ddl:col:check", "ddl:col:check:none"]) != "ddl:col:check" {
        return None;
    }
    g.fire("ddl:col:check");
    Some(if ty.is_integer() {
        if g.rng.chance(1, 4) {
            format!("CHECK ({} <> 7)", name) // bites when a 7 arrives
        } else {
            format!("CHECK ({} > -2000000000)", name)
        }
    } else {
        format!("CHECK (length({}) < 10000)", name)
    })
}

fn gen_create_table(g: &mut Gen) -> StmtKind {
    if g.ddl.live_tables().len() >= MAX_LIVE_TABLES {
        // Population cap: recycle instead of growing without bound.
        g.fire("ddl:cap:tables");
        return gen_drop_table(g);
    }
    g.fire("ddl:create_table");
    let name = format!("fz_ddl_{}", g.ddl.next_table);
    g.ddl.next_table += 1;
    let temp = g.weights.pick(g.rng, &["ddl:table:temp", "ddl:table:plain"]) == "ddl:table:temp";
    if temp {
        g.fire("ddl:table:temp");
    }

    let mut columns = vec![
        Column { name: "pk".to_string(), ty: SqlType::Int4, nullable: false, ddl_type: None },
        Column { name: "k_int".to_string(), ty: SqlType::Int4, nullable: true, ddl_type: None },
        Column { name: "k_text".to_string(), ty: SqlType::Text, nullable: true, ddl_type: None },
    ];
    let mut defs = vec![
        "pk int4 PRIMARY KEY".to_string(),
        "k_int int4".to_string(),
        "k_text text".to_string(),
    ];
    let mut checked = Vec::new();

    let ncols = 2 + g.rng.below_usize(5); // 2-6 palette columns
    for _ in 0..ncols {
        let cname = format!("c{}", g.ddl.next_col);
        g.ddl.next_col += 1;
        let ty = palette_type(g);
        let not_null = g.rng.chance(1, 5);
        let mut def = format!("{} {}", cname, ty.name());
        if not_null {
            def.push_str(" NOT NULL");
        }
        if g.weights.pick(g.rng, &["ddl:col:default", "ddl:col:default:none"])
            == "ddl:col:default"
        {
            g.fire("ddl:col:default");
            def.push_str(&format!(" DEFAULT {}", g.gen_literal(ty)));
        }
        if let Some(check) = column_check(g, &cname, ty) {
            def.push(' ');
            def.push_str(&check);
            checked.push(cname.clone());
        }
        defs.push(def);
        columns.push(Column { name: cname, ty, nullable: !not_null, ddl_type: None });
    }
    // Occasional table-level CHECK over the (protected, monotonic) pk:
    // always satisfied, exercises the multi-constraint path.
    if g.rng.chance(1, 6) {
        defs.push("CHECK (pk >= 0)".to_string());
    }

    let table = Table {
        name: name.clone(),
        columns,
        at_most_one_row: false,
        pk: Some(Pk { column: "pk".to_string(), seeded_max: 0 }),
        unique_key: Some("pk".to_string()),
        pk_unique: true,
    };
    g.ddl.tables.push(DdlTable {
        table,
        live: true,
        temp,
        checked_cols: checked,
        triggers: Vec::new(),
    });
    g.ddl.events.push(DdlEvent {
        table: name.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Created,
    });
    // autovacuum_enabled = off on plain ddl tables (round-18a workspace
    // RB-15 rule): fz_ddl_* enter the effective catalog, take dml.rs
    // churn, and are reachable by explain.rs's compared EXPLAIN
    // (COSTS OFF) wrappers — an autoanalyze landing on one engine could
    // flip a compared plan. TEMP tables are exempt (autovacuum never
    // visits them).
    StmtKind::Raw(format!(
        "CREATE {}TABLE {} ({}){};",
        if temp { "TEMPORARY " } else { "" },
        name,
        defs.join(", "),
        if temp { "" } else { " WITH (autovacuum_enabled = off)" }
    ))
}

fn gen_drop_table(g: &mut Gen) -> StmtKind {
    let live = g.ddl.live_tables();
    if live.is_empty() {
        g.fire("ddl:fallback:create_table");
        return gen_create_table(g);
    }
    g.fire("ddl:drop_table");
    let ti = live[g.rng.below_usize(live.len())];
    let name = g.ddl.tables[ti].table.name.clone();
    let if_exists = g.rng.chance(1, 2);
    // FK-referenced tables drop with CASCADE (takes referencing
    // constraints, never child tables), so the drop cannot fail and
    // desync the model.
    let cascade = g.ddl.fk_parents.contains(&name);
    g.ddl.drop_table(ti);
    StmtKind::Raw(format!(
        "DROP TABLE {}{}{};",
        if if_exists { "IF EXISTS " } else { "" },
        name,
        if cascade { " CASCADE" } else { "" }
    ))
}

fn gen_truncate(g: &mut Gen) -> StmtKind {
    let live = g.ddl.live_tables();
    if live.is_empty() {
        g.fire("ddl:fallback:create_table");
        return gen_create_table(g);
    }
    g.fire("ddl:truncate");
    // 1-2 distinct ddl tables.
    let a = live[g.rng.below_usize(live.len())];
    let mut names = vec![g.ddl.tables[a].table.name.clone()];
    if live.len() > 1 && g.rng.chance(1, 3) {
        let b = live[g.rng.below_usize(live.len())];
        if b != a {
            names.push(g.ddl.tables[b].table.name.clone());
        }
    }
    let restart = g.weights.pick(g.rng, &["ddl:truncate:restart", "ddl:truncate:plain"])
        == "ddl:truncate:restart";
    if restart {
        g.fire("ddl:truncate:restart");
    }
    // Truncating an FK-referenced table needs CASCADE (referencing tables
    // truncate too — deterministic on both sides) or the statement fails.
    let cascade = names.iter().any(|n| g.ddl.fk_parents.contains(n));
    StmtKind::Raw(format!(
        "TRUNCATE TABLE {}{}{};",
        names.join(", "),
        if restart { " RESTART IDENTITY" } else { "" },
        if cascade { " CASCADE" } else { "" }
    ))
}

// -------------------------------------------------------------- alter ----

/// Widening-only ALTER TYPE targets (parser/analyzer-safe without USING;
/// existing values always representable).
fn widen_targets(ty: SqlType) -> &'static [SqlType] {
    match ty {
        SqlType::Int2 => &[SqlType::Int4, SqlType::Int8, SqlType::Numeric],
        SqlType::Int4 => &[SqlType::Int8, SqlType::Numeric],
        SqlType::Int8 => &[SqlType::Numeric],
        SqlType::Float4 => &[SqlType::Float8],
        SqlType::Varchar => &[SqlType::Text],
        SqlType::Text => &[SqlType::Varchar],
        SqlType::Date => &[SqlType::Timestamp],
        _ => &[],
    }
}

fn gen_alter_table(g: &mut Gen) -> StmtKind {
    let live = g.ddl.live_tables();
    if live.is_empty() {
        g.fire("ddl:fallback:create_table");
        return gen_create_table(g);
    }
    g.fire("ddl:alter");
    let ti = live[g.rng.below_usize(live.len())];
    let form = g.weights.pick(
        g.rng,
        &[
            "ddl:alter:add_col",
            "ddl:alter:drop_col",
            "ddl:alter:type",
            "ddl:alter:set_not_null",
            "ddl:alter:drop_not_null",
            "ddl:alter:rename_col",
            "ddl:alter:add_check",
        ],
    );
    match form {
        "ddl:alter:add_col" => alter_add_col(g, ti),
        "ddl:alter:drop_col" => alter_drop_col(g, ti),
        "ddl:alter:type" => alter_type(g, ti),
        "ddl:alter:set_not_null" => alter_set_not_null(g, ti),
        "ddl:alter:drop_not_null" => alter_drop_not_null(g, ti),
        "ddl:alter:rename_col" => alter_rename_col(g, ti),
        _ => alter_add_check(g, ti),
    }
}

/// Non-protected column indices of a ddl table, optionally filtered.
fn alterable_cols(g: &Gen, ti: usize, extra: impl Fn(&Column) -> bool) -> Vec<usize> {
    g.ddl.tables[ti]
        .table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| !is_protected(&c.name) && extra(c))
        .map(|(i, _)| i)
        .collect()
}

fn alter_add_col(g: &mut Gen, ti: usize) -> StmtKind {
    if g.ddl.tables[ti].table.columns.len() >= MAX_COLS {
        return alter_drop_col(g, ti);
    }
    g.fire("ddl:alter:add_col");
    let tname = g.ddl.tables[ti].table.name.clone();
    let cname = format!("c{}", g.ddl.next_col);
    g.ddl.next_col += 1;
    let ty = palette_type(g);
    let form = g.weights.pick(
        g.rng,
        &["ddl:addcol:plain", "ddl:addcol:default", "ddl:addcol:notnull_default"],
    );
    g.fire(form);
    let (suffix, nullable) = match form {
        // NOT NULL without DEFAULT would 23502 on any populated table.
        "ddl:addcol:notnull_default" => {
            (format!(" DEFAULT {} NOT NULL", g.gen_literal(ty)), false)
        }
        "ddl:addcol:default" => (format!(" DEFAULT {}", g.gen_literal(ty)), true),
        _ => (String::new(), true),
    };
    g.ddl.tables[ti]
        .table
        .columns
        .push(Column { name: cname.clone(), ty, nullable, ddl_type: None });
    StmtKind::Raw(format!(
        "ALTER TABLE {} ADD COLUMN {} {}{};",
        tname,
        cname,
        ty.name(),
        suffix
    ))
}

fn alter_drop_col(g: &mut Gen, ti: usize) -> StmtKind {
    let cands = alterable_cols(g, ti, |_| true);
    if cands.is_empty() {
        return alter_add_col(g, ti);
    }
    g.fire("ddl:alter:drop_col");
    let ci = cands[g.rng.below_usize(cands.len())];
    let tname = g.ddl.tables[ti].table.name.clone();
    let cname = g.ddl.tables[ti].table.columns[ci].name.clone();
    g.ddl.tables[ti].table.columns.remove(ci);
    g.ddl.tables[ti].checked_cols.retain(|c| c != &cname);
    // CASCADE takes multi-column CHECKs with it; dependent indexes die.
    for x in &mut g.ddl.indexes {
        if x.table == tname && x.cols.contains(&cname) {
            x.live = false;
        }
    }
    StmtKind::Raw(format!("ALTER TABLE {} DROP COLUMN {} CASCADE;", tname, cname))
}

fn alter_type(g: &mut Gen, ti: usize) -> StmtKind {
    // Skip columns under a CHECK or a live index: re-coercing the dependent
    // expression could 42883/42P17 (both-side, but a generator bug per gate).
    let tname = g.ddl.tables[ti].table.name.clone();
    let indexed: Vec<String> = g
        .ddl
        .indexes
        .iter()
        .filter(|x| x.live && x.table == tname)
        .flat_map(|x| x.cols.clone())
        .collect();
    let checked = g.ddl.tables[ti].checked_cols.clone();
    let cands = alterable_cols(g, ti, |c| {
        !indexed.contains(&c.name) && !checked.contains(&c.name)
    });
    let widenable: Vec<usize> = cands
        .iter()
        .copied()
        .filter(|&i| !widen_targets(g.ddl.tables[ti].table.columns[i].ty).is_empty())
        .collect();
    let textable: Vec<usize> = cands
        .iter()
        .copied()
        .filter(|&i| g.ddl.tables[ti].table.columns[i].ty.is_integer())
        .collect();
    let to_text = match (widenable.is_empty(), textable.is_empty()) {
        (true, true) => return alter_add_col(g, ti),
        (true, false) => true,
        (false, true) => false,
        (false, false) => {
            g.weights.pick(g.rng, &["ddl:type:text", "ddl:type:widen"]) == "ddl:type:text"
        }
    };
    if to_text {
        g.fire("ddl:alter:type");
        g.fire("ddl:type:text");
        let ci = textable[g.rng.below_usize(textable.len())];
        let cname = g.ddl.tables[ti].table.columns[ci].name.clone();
        g.ddl.tables[ti].table.columns[ci].ty = SqlType::Text;
        StmtKind::Raw(format!(
            "ALTER TABLE {} ALTER COLUMN {} TYPE text USING ({})::text;",
            tname, cname, cname
        ))
    } else {
        g.fire("ddl:alter:type");
        g.fire("ddl:type:widen");
        let ci = widenable[g.rng.below_usize(widenable.len())];
        let cname = g.ddl.tables[ti].table.columns[ci].name.clone();
        let targets = widen_targets(g.ddl.tables[ti].table.columns[ci].ty);
        let ty = targets[g.rng.below_usize(targets.len())];
        g.ddl.tables[ti].table.columns[ci].ty = ty;
        StmtKind::Raw(format!(
            "ALTER TABLE {} ALTER COLUMN {} TYPE {};",
            tname,
            cname,
            ty.name()
        ))
    }
}

fn alter_set_not_null(g: &mut Gen, ti: usize) -> StmtKind {
    let cands = alterable_cols(g, ti, |c| c.nullable);
    if cands.is_empty() {
        return alter_add_col(g, ti);
    }
    g.fire("ddl:alter:set_not_null");
    let ci = cands[g.rng.below_usize(cands.len())];
    let tname = g.ddl.tables[ti].table.name.clone();
    let cname = g.ddl.tables[ti].table.columns[ci].name.clone();
    // Optimistic model update (see module docs): a rejected SET NOT NULL
    // (matched 23502 both sides) leaves the model conservative, never wrong.
    g.ddl.tables[ti].table.columns[ci].nullable = false;
    StmtKind::Raw(format!("ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;", tname, cname))
}

fn alter_drop_not_null(g: &mut Gen, ti: usize) -> StmtKind {
    let cands = alterable_cols(g, ti, |c| !c.nullable);
    if cands.is_empty() {
        return alter_add_col(g, ti);
    }
    g.fire("ddl:alter:drop_not_null");
    let ci = cands[g.rng.below_usize(cands.len())];
    let tname = g.ddl.tables[ti].table.name.clone();
    let cname = g.ddl.tables[ti].table.columns[ci].name.clone();
    g.ddl.tables[ti].table.columns[ci].nullable = true;
    StmtKind::Raw(format!("ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;", tname, cname))
}

fn alter_rename_col(g: &mut Gen, ti: usize) -> StmtKind {
    let cands = alterable_cols(g, ti, |_| true);
    if cands.is_empty() {
        return alter_add_col(g, ti);
    }
    g.fire("ddl:alter:rename_col");
    let ci = cands[g.rng.below_usize(cands.len())];
    let tname = g.ddl.tables[ti].table.name.clone();
    let old = g.ddl.tables[ti].table.columns[ci].name.clone();
    let new = format!("c{}", g.ddl.next_col);
    g.ddl.next_col += 1;
    g.ddl.tables[ti].table.columns[ci].name = new.clone();
    for c in &mut g.ddl.tables[ti].checked_cols {
        if c == &old {
            *c = new.clone();
        }
    }
    for x in &mut g.ddl.indexes {
        if x.table == tname {
            for c in &mut x.cols {
                if c == &old {
                    *c = new.clone();
                }
            }
        }
    }
    StmtKind::Raw(format!("ALTER TABLE {} RENAME COLUMN {} TO {};", tname, old, new))
}

fn alter_add_check(g: &mut Gen, ti: usize) -> StmtKind {
    // Any int-family column (protected ones included — they are never
    // type-altered, so the constraint can never block a later ALTER).
    let ints: Vec<usize> = g.ddl.tables[ti]
        .table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.ty.is_integer())
        .map(|(i, _)| i)
        .collect();
    if ints.is_empty() {
        return alter_add_col(g, ti);
    }
    g.fire("ddl:alter:add_check");
    let ci = ints[g.rng.below_usize(ints.len())];
    let tname = g.ddl.tables[ti].table.name.clone();
    let cname = g.ddl.tables[ti].table.columns[ci].name.clone();
    if !g.ddl.tables[ti].checked_cols.contains(&cname) {
        g.ddl.tables[ti].checked_cols.push(cname.clone());
    }
    let not_valid = if g.rng.chance(1, 3) { " NOT VALID" } else { "" };
    StmtKind::Raw(format!(
        "ALTER TABLE {} ADD CHECK ({} IS NULL OR {} > -2000000000){};",
        tname, cname, cname, not_valid
    ))
}

// ------------------------------------------------------------ foreign keys
// X1 gap (ATAddForeignKeyConstraint, gap-report-004 rank 12): FK creation
// via CREATE TABLE ... REFERENCES and ALTER TABLE ADD FOREIGN KEY with the
// full referential-action palette. The RI-firing DML comes for free from
// cross-module reuse: dml/merge/txn target FK child tables through the
// effective catalog and insert arbitrary key values (matched 23503s both
// sides — RI check surfaces are prime differential fuel), and DELETE/UPDATE
// on parents fires the ri_triggers action paths.
//
// Model disciplines:
//   - referenced columns are always the parent's pk (protected: never
//     dropped/renamed/re-typed), so a constraint can never dangle;
//   - FK columns land in `checked_cols`, excluding them from ALTER TYPE
//     (re-coercion under a constraint could fail asymmetrically-shaped);
//     DROP COLUMN CASCADE and RENAME are safe (the constraint follows);
//   - temp-ness must match across the reference (Postgres forbids
//     permanent->temp and temp->permanent references);
//   - referenced tables are recorded in `fk_parents`: their DROP/TRUNCATE
//     render with CASCADE so the statement cannot fail on a dependent
//     constraint and desync the model;
//   - a failed ADD FOREIGN KEY (23503 from existing rows — the
//     RI_Initial_Check surface, deliberately reachable) is a matched
//     both-side error and leaves no model state behind that anything
//     depends on.

/// ON DELETE / ON UPDATE action pair (weighted; SET DEFAULT on the
/// default-less FK columns sets NULL, which they always allow).
fn fk_actions(g: &mut Gen) -> String {
    let del = g.weights.pick(
        g.rng,
        &[
            "ddl:fk:del:noaction",
            "ddl:fk:del:restrict",
            "ddl:fk:del:cascade",
            "ddl:fk:del:setnull",
            "ddl:fk:del:setdefault",
        ],
    );
    g.fire(del);
    let del_sql = match del {
        "ddl:fk:del:restrict" => "RESTRICT",
        "ddl:fk:del:cascade" => "CASCADE",
        "ddl:fk:del:setnull" => "SET NULL",
        "ddl:fk:del:setdefault" => "SET DEFAULT",
        _ => "NO ACTION",
    };
    let upd = g.weights.pick(
        g.rng,
        &["ddl:fk:upd:noaction", "ddl:fk:upd:restrict", "ddl:fk:upd:cascade"],
    );
    g.fire(upd);
    let upd_sql = match upd {
        "ddl:fk:upd:restrict" => "RESTRICT",
        "ddl:fk:upd:cascade" => "CASCADE",
        _ => "NO ACTION",
    };
    format!(" ON DELETE {del_sql} ON UPDATE {upd_sql}")
}

fn mark_fk_parent(g: &mut Gen, name: &str) {
    if !g.ddl.fk_parents.iter().any(|p| p == name) {
        g.ddl.fk_parents.push(name.to_string());
    }
}

fn gen_foreign_key(g: &mut Gen) -> StmtKind {
    let live = g.ddl.live_tables();
    if live.is_empty() {
        g.fire("ddl:fallback:create_table");
        return gen_create_table(g);
    }
    g.fire("ddl:fk");
    let mut form = g.weights.pick(
        g.rng,
        &["ddl:fk:create_child", "ddl:fk:alter_add", "ddl:fk:self"],
    );
    if g.ddl.live_tables().len() >= MAX_LIVE_TABLES && form != "ddl:fk:alter_add" {
        form = "ddl:fk:alter_add"; // respect the population cap
    }
    match form {
        "ddl:fk:alter_add" => {
            g.fire("ddl:fk:alter_add");
            // Child first, then a temp-compatible parent (self always is).
            let ci = live[g.rng.below_usize(live.len())];
            let child_temp = g.ddl.tables[ci].temp;
            let parents: Vec<usize> = live
                .iter()
                .copied()
                .filter(|&pi| g.ddl.tables[pi].temp == child_temp)
                .collect();
            let pi = parents[g.rng.below_usize(parents.len())];
            let child = g.ddl.tables[ci].table.name.clone();
            let parent = g.ddl.tables[pi].table.name.clone();
            // FK over the protected k_int column: always present, never
            // re-typed. Existing random k_int values make the validation
            // scan (RI_Initial_Check) bite as a matched 23503 — that error
            // path is a target, not a hazard; NOT VALID skips it.
            let actions = fk_actions(g);
            let not_valid = if g
                .weights
                .pick(g.rng, &["ddl:fk:notvalid", "ddl:fk:valid"])
                == "ddl:fk:notvalid"
            {
                g.fire("ddl:fk:notvalid");
                " NOT VALID"
            } else {
                g.fire("ddl:fk:valid");
                ""
            };
            if !g.ddl.tables[ci].checked_cols.iter().any(|c| c == "k_int") {
                g.ddl.tables[ci].checked_cols.push("k_int".to_string());
            }
            mark_fk_parent(g, &parent);
            StmtKind::Raw(format!(
                "ALTER TABLE {child} ADD FOREIGN KEY (k_int) REFERENCES {parent} (pk){actions}{not_valid};"
            ))
        }
        _ => {
            // CREATE TABLE with a REFERENCES column: a fresh ddl-shaped
            // child (other modules will DML it via the effective catalog).
            let selfref = form == "ddl:fk:self";
            g.fire(if selfref { "ddl:fk:self" } else { "ddl:fk:create_child" });
            let name = format!("fz_ddl_{}", g.ddl.next_table);
            g.ddl.next_table += 1;
            let (parent, temp) = if selfref {
                (name.clone(), false)
            } else {
                let pi = live[g.rng.below_usize(live.len())];
                (g.ddl.tables[pi].table.name.clone(), g.ddl.tables[pi].temp)
            };
            let cname = format!("c{}", g.ddl.next_col);
            g.ddl.next_col += 1;
            let actions = fk_actions(g);
            let columns = vec![
                Column { name: "pk".to_string(), ty: SqlType::Int4, nullable: false, ddl_type: None },
                Column { name: "k_int".to_string(), ty: SqlType::Int4, nullable: true, ddl_type: None },
                Column { name: "k_text".to_string(), ty: SqlType::Text, nullable: true, ddl_type: None },
                Column { name: cname.clone(), ty: SqlType::Int4, nullable: true, ddl_type: None },
            ];
            let table = Table {
                name: name.clone(),
                columns,
                at_most_one_row: false,
                pk: Some(Pk { column: "pk".to_string(), seeded_max: 0 }),
                unique_key: Some("pk".to_string()),
                pk_unique: true,
            };
            g.ddl.tables.push(DdlTable {
                table,
                live: true,
                temp,
                // FK column: exclude from ALTER TYPE via checked_cols.
                checked_cols: vec![cname.clone()],
                triggers: Vec::new(),
            });
            g.ddl.events.push(DdlEvent {
                table: name.clone(),
                pk: "pk".to_string(),
                kind: DdlEventKind::Created,
            });
            mark_fk_parent(g, &parent);
            StmtKind::Raw(format!(
                "CREATE {}TABLE {} (pk int4 PRIMARY KEY, k_int int4, k_text text, \
                 {} int4 REFERENCES {} (pk){}){};",
                if temp { "TEMPORARY " } else { "" },
                name,
                cname,
                parent,
                actions,
                if temp { "" } else { " WITH (autovacuum_enabled = off)" }
            ))
        }
    }
}

// ------------------------------------------------------------ indexes ----

/// Every live table (fixtures + ddl) as (name, columns) pairs.
fn indexable_tables(g: &Gen) -> Vec<(String, Vec<Column>)> {
    g.catalog
        .tables
        .iter()
        .map(|t| (t.name.clone(), t.columns.clone()))
        .collect()
}

/// Whether `name` is a ddl-created table currently live.
fn is_live_ddl_table(g: &Gen, name: &str) -> bool {
    g.ddl.tables.iter().any(|t| t.live && t.table.name == name)
}

/// Immutable partial-index predicate over one column.
fn partial_pred(g: &mut Gen, cols: &[Column]) -> (String, Vec<String>) {
    let c = &cols[g.rng.below_usize(cols.len())];
    let pred = if c.ty.is_integer() && g.rng.chance(1, 2) {
        format!("{} > 10", c.name)
    } else if c.ty.is_text_family() && g.rng.chance(1, 2) {
        format!("{} <> ''", c.name)
    } else if c.ty == SqlType::Bool && g.rng.chance(1, 2) {
        c.name.clone()
    } else if g.rng.chance(1, 2) {
        format!("{} IS NOT NULL", c.name)
    } else {
        format!("{} IS NULL", c.name)
    };
    (pred, vec![c.name.clone()])
}

fn gen_create_index(g: &mut Gen) -> StmtKind {
    if g.ddl.live_indexes().len() >= MAX_LIVE_INDEXES {
        g.fire("ddl:cap:indexes");
        return gen_drop_index(g);
    }
    g.fire("ddl:create_index");
    let tables = indexable_tables(g);
    let (tname, cols) = tables[g.rng.below_usize(tables.len())].clone();

    let am = g.weights.pick(
        g.rng,
        &[
            "ddl:idx:btree",
            "ddl:idx:hash",
            "ddl:idx:gin",
            "ddl:idx:gist",
            "ddl:idx:spgist",
            "ddl:idx:brin",
        ],
    );
    let int4_cols: Vec<&Column> = cols.iter().filter(|c| c.ty == SqlType::Int4).collect();
    let text_cols: Vec<&Column> = cols.iter().filter(|c| c.ty.is_text_family()).collect();
    // json (fz_rich fixture) has no equality operator at all: no btree, no
    // hash, and no GIN array_ops over json[] elements. Every other type in
    // the union palette (T1 rich types included) has btree + hash defaults.
    let key_cols: Vec<&Column> = cols.iter().filter(|c| c.ty != SqlType::Json).collect();
    let gin_cols: Vec<&Column> = cols
        .iter()
        .filter(|c| !c.ty.is_float() && c.ty != SqlType::Json)
        .collect();
    // brin: minmax default opclasses cover the scalar palette EXCEPT bool,
    // and none exist for json/jsonb or arrays ("data type ... has no
    // default operator class for brin" — a failed CREATE INDEX would
    // desync the model and cascade 42P01s).
    let brin_cols: Vec<&Column> = cols
        .iter()
        .filter(|c| {
            !matches!(
                c.ty,
                SqlType::Bool
                    | SqlType::Json
                    | SqlType::Jsonb
                    | SqlType::TextArr
                    | SqlType::Int4Arr
            )
        })
        .collect();

    let mut used: Vec<String> = Vec::new();
    let mut unique = false;
    // (am keyword or empty for btree default, key list)
    let (using, keys): (&str, String) = match am {
        "ddl:idx:hash" => {
            g.fire(am);
            let c = key_cols[g.rng.below_usize(key_cols.len())];
            used.push(c.name.clone());
            ("hash", c.name.clone())
        }
        "ddl:idx:gin" if !gin_cols.is_empty() => {
            g.fire(am);
            let c = gin_cols[g.rng.below_usize(gin_cols.len())];
            used.push(c.name.clone());
            ("gin", format!("(ARRAY[{}])", c.name))
        }
        "ddl:idx:gist" if !int4_cols.is_empty() => {
            g.fire(am);
            // Clamped so the range can neither overflow nor invert.
            let c = int4_cols[g.rng.below_usize(int4_cols.len())];
            used.push(c.name.clone());
            (
                "gist",
                format!(
                    "(int4range(least({}, 2000000000), least({}, 2000000000) + 10))",
                    c.name, c.name
                ),
            )
        }
        "ddl:idx:spgist" if !text_cols.is_empty() => {
            g.fire(am);
            let c = text_cols[g.rng.below_usize(text_cols.len())];
            used.push(c.name.clone());
            ("spgist", c.name.clone())
        }
        "ddl:idx:brin" if !brin_cols.is_empty() => {
            g.fire(am);
            let n = 1 + g.rng.below_usize(2.min(brin_cols.len()));
            let mut ks = Vec::new();
            for _ in 0..n {
                let c = brin_cols[g.rng.below_usize(brin_cols.len())];
                if !used.contains(&c.name) {
                    used.push(c.name.clone());
                    ks.push(c.name.clone());
                }
            }
            ("brin", ks.join(", "))
        }
        _ => {
            // btree (also the fallback when a specialized AM found no
            // suitable column on the picked table).
            g.fire("ddl:idx:btree");
            unique = is_live_ddl_table(g, &tname)
                && g.weights.pick(g.rng, &["ddl:idx:unique", "ddl:idx:plain"])
                    == "ddl:idx:unique";
            if unique {
                g.fire("ddl:idx:unique");
                // Leading pk keeps creation infallible over existing rows.
                used.push("pk".to_string());
                let mut ks = vec!["pk".to_string()];
                if g.rng.chance(1, 2) {
                    let c = key_cols[g.rng.below_usize(key_cols.len())];
                    if !used.contains(&c.name) {
                        used.push(c.name.clone());
                        ks.push(c.name.clone());
                    }
                }
                ("", ks.join(", "))
            } else if g.weights.pick(g.rng, &["ddl:idx:expr", "ddl:idx:cols"])
                == "ddl:idx:expr"
            {
                g.fire("ddl:idx:expr");
                // Immutable expression key: `% 97` cannot overflow, lower()
                // is immutable; anything else degrades to the plain column.
                let c = key_cols[g.rng.below_usize(key_cols.len())];
                used.push(c.name.clone());
                let k = if c.ty.is_integer() {
                    format!("(({} % 97))", c.name)
                } else if c.ty.is_text_family() {
                    format!("(lower({}))", c.name)
                } else {
                    c.name.clone()
                };
                ("", k)
            } else {
                let n = 1 + g.rng.below_usize(3.min(key_cols.len()));
                let mut ks = Vec::new();
                for _ in 0..n {
                    let c = key_cols[g.rng.below_usize(key_cols.len())];
                    if !used.contains(&c.name) {
                        used.push(c.name.clone());
                        ks.push(c.name.clone());
                    }
                }
                ("", ks.join(", "))
            }
        }
    };

    let name = format!("fz_idx_{}", g.ddl.next_index);
    g.ddl.next_index += 1;
    let mut sql = format!(
        "CREATE {}INDEX {} ON {}{} ({})",
        if unique { "UNIQUE " } else { "" },
        name,
        tname,
        if using.is_empty() { String::new() } else { format!(" USING {}", using) },
        keys
    );
    if g.weights.pick(g.rng, &["ddl:idx:partial", "ddl:idx:partial:none"]) == "ddl:idx:partial"
    {
        g.fire("ddl:idx:partial");
        let (pred, pcols) = partial_pred(g, &cols);
        sql.push_str(&format!(" WHERE {}", pred));
        for c in pcols {
            if !used.contains(&c) {
                used.push(c);
            }
        }
    }
    sql.push(';');
    g.ddl.indexes.push(DdlIndex { name, table: tname, cols: used, live: true });
    StmtKind::Raw(sql)
}

fn gen_drop_index(g: &mut Gen) -> StmtKind {
    let live = g.ddl.live_indexes();
    if live.is_empty() {
        g.fire("ddl:fallback:create_index");
        return gen_create_index(g);
    }
    g.fire("ddl:drop_index");
    let xi = live[g.rng.below_usize(live.len())];
    let name = g.ddl.indexes[xi].name.clone();
    let if_exists = g.rng.chance(1, 3);
    g.ddl.indexes[xi].live = false;
    StmtKind::Raw(format!(
        "DROP INDEX {}{};",
        if if_exists { "IF EXISTS " } else { "" },
        name
    ))
}

fn gen_reindex(g: &mut Gen) -> StmtKind {
    g.fire("ddl:reindex");
    let concurrent = g
        .weights
        .pick(g.rng, &["ddl:reindex:concurrent", "ddl:reindex:plain"])
        == "ddl:reindex:concurrent";
    if concurrent {
        g.fire("ddl:reindex:concurrent");
    }
    let opt = if concurrent { "(CONCURRENTLY) " } else { "" };
    let live = g.ddl.live_indexes();
    let form = g.weights.pick(g.rng, &["ddl:reindex:index", "ddl:reindex:table"]);
    if form == "ddl:reindex:index" && !live.is_empty() {
        g.fire("ddl:reindex:index");
        let xi = live[g.rng.below_usize(live.len())];
        let name = g.ddl.indexes[xi].name.clone();
        StmtKind::Raw(format!("REINDEX {}INDEX {};", opt, name))
    } else {
        g.fire("ddl:reindex:table");
        let tables = indexable_tables(g);
        let (tname, _) = &tables[g.rng.below_usize(tables.len())];
        StmtKind::Raw(format!("REINDEX {}TABLE {};", opt, tname))
    }
}

// -------------------------------------------------------------- views ----

/// Create/query-once/drop inside one group: no cross-group dependency
/// edges, so table drops can never fail on a dependent view.
fn gen_view_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("ddl:view");
    let name = format!("fz_v_{}", g.ddl.next_view);
    g.ddl.next_view += 1;
    let src = g.weights.pick(g.rng, &["ddl:view:expr", "ddl:view:join"]);
    g.fire(src);
    let select = if src == "ddl:view:join" {
        crate::join::gen_join_stmt(g)
    } else {
        gen_expr_stmt(g)
    };
    let body = select.to_sql();
    let body = body.trim_end_matches(';');
    // Explicit output column list: generated select items are unnamed
    // expressions and would all deparse as "?column?" (42701 on CREATE).
    let collist: Vec<String> = (1..=select.items.len()).map(|i| format!("v{}", i)).collect();
    let query = match g.weights.pick(g.rng, &["ddl:view:star", "ddl:view:count"]) {
        "ddl:view:count" => {
            g.fire("ddl:view:count");
            format!("SELECT count(*) FROM {};", name)
        }
        _ => {
            g.fire("ddl:view:star");
            format!("SELECT * FROM {};", name)
        }
    };
    vec![
        StmtKind::Raw(format!(
            "CREATE VIEW {} ({}) AS {};",
            name,
            collist.join(", "),
            body
        )),
        StmtKind::Raw(query),
        StmtKind::Raw(format!("DROP VIEW {};", name)),
    ]
}

// ---------------------------------------------------------- sequences ----

fn gen_sequence(g: &mut Gen) -> StmtKind {
    let live = g.ddl.live_seqs();
    let mut action = g
        .weights
        .pick(g.rng, &["ddl:seq:create", "ddl:seq:use", "ddl:seq:drop"]);
    if live.is_empty() {
        action = "ddl:seq:create";
    } else if action == "ddl:seq:create" && live.len() >= MAX_LIVE_SEQS {
        action = "ddl:seq:use";
    }
    match action {
        "ddl:seq:create" => {
            g.fire("ddl:seq:create");
            let name = format!("fz_seq_{}", g.ddl.next_seq);
            g.ddl.next_seq += 1;
            // Fixed-safe option templates (start always within bounds).
            let opts = match g.rng.below(5) {
                0 => "".to_string(),
                1 => " INCREMENT BY 5 START WITH 100".to_string(),
                2 => " INCREMENT BY -3 MINVALUE -500 MAXVALUE 500 START WITH 0 CYCLE"
                    .to_string(),
                3 => format!(" MINVALUE 1 MAXVALUE {} CYCLE", 5 + g.rng.below(20)),
                _ => format!(" CACHE {}", 1 + g.rng.below(10)),
            };
            g.ddl.seqs.push(DdlSeq { name: name.clone(), live: true, has_nextval: false });
            StmtKind::Raw(format!("CREATE SEQUENCE {}{};", name, opts))
        }
        "ddl:seq:drop" => {
            g.fire("ddl:seq:drop");
            let si = live[g.rng.below_usize(live.len())];
            let name = g.ddl.seqs[si].name.clone();
            let if_exists = g.rng.chance(1, 3);
            g.ddl.seqs[si].live = false;
            StmtKind::Raw(format!(
                "DROP SEQUENCE {}{};",
                if if_exists { "IF EXISTS " } else { "" },
                name
            ))
        }
        _ => {
            let si = live[g.rng.below_usize(live.len())];
            let name = g.ddl.seqs[si].name.clone();
            let mut form = g.weights.pick(
                g.rng,
                &["ddl:seq:nextval", "ddl:seq:currval", "ddl:seq:setval"],
            );
            // currval before any nextval is a 55000 — deterministic but
            // pure noise; redirect to the nextval that legalizes it.
            if form == "ddl:seq:currval" && !g.ddl.seqs[si].has_nextval {
                form = "ddl:seq:nextval";
            }
            g.fire(form);
            match form {
                "ddl:seq:currval" => StmtKind::Raw(format!("SELECT currval('{}');", name)),
                "ddl:seq:setval" => {
                    let v = g.rng.range_i64(1, 100);
                    let called = if g.rng.chance(1, 2) { "true" } else { "false" };
                    StmtKind::Raw(format!("SELECT setval('{}', {}, {});", name, v, called))
                }
                _ => {
                    g.ddl.seqs[si].has_nextval = true;
                    StmtKind::Raw(format!("SELECT nextval('{}');", name))
                }
            }
        }
    }
}

// ---------------------------------------------------------- aggregates ----

/// Create/use-once/drop in one group. Commutative/associative int sfuncs
/// only: an order-sensitive sfunc would turn legal scan-order differences
/// into false findings.
fn gen_aggregate_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("ddl:agg");
    let name = format!("fz_agg_{}", g.ddl.next_agg);
    g.ddl.next_agg += 1;
    let variant = g.weights.pick(
        g.rng,
        &["ddl:agg:sum4", "ddl:agg:max4", "ddl:agg:min4", "ddl:agg:sum8"],
    );
    g.fire(variant);
    let (in_ty, sfunc, stype, initcond) = match variant {
        "ddl:agg:max4" => ("int4", "int4larger", "int4", None),
        "ddl:agg:min4" => ("int4", "int4smaller", "int4", None),
        "ddl:agg:sum8" => ("int8", "int8pl", "int8", Some("0")),
        _ => ("int4", "int4pl", "int4", Some("0")),
    };
    let init = match initcond {
        Some(v) if g.rng.chance(1, 2) => format!(", initcond = '{}'", v),
        _ => String::new(),
    };
    let create = format!(
        "CREATE AGGREGATE {}({}) (sfunc = {}, stype = {}{});",
        name, in_ty, sfunc, stype, init
    );
    // Use it over some int column (cast pins the polymorphic-free input;
    // ::int8 can never overflow, ::int4 from wider ints is a matched
    // both-side 22003 at worst).
    let mut cands: Vec<(String, String)> = Vec::new();
    for t in &g.catalog.tables {
        for c in &t.columns {
            if c.ty.is_integer() {
                cands.push((t.name.clone(), c.name.clone()));
            }
        }
    }
    let (tname, cname) = cands[g.rng.below_usize(cands.len())].clone();
    let use_stmt = format!("SELECT {}(({})::{}) FROM {};", name, cname, in_ty, tname);
    let drop = format!("DROP AGGREGATE {}({});", name, in_ty);
    vec![StmtKind::Raw(create), StmtKind::Raw(use_stmt), StmtKind::Raw(drop)]
}

// ------------------------------------------------------------ triggers ----

/// Deterministic plpgsql triggers on ddl tables (pgrust ships plpgsql via
/// crates/pl/plpgsql — languages sql/internal/C/plpgsql are the carve-ruled
/// set). Trigger functions are created per trigger and never dropped.
fn gen_trigger_group(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.ddl.live_tables();
    if live.is_empty() {
        g.fire("ddl:fallback:create_table");
        return vec![gen_create_table(g)];
    }
    // Drop an existing trigger occasionally.
    let with_triggers: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&ti| !g.ddl.tables[ti].triggers.is_empty())
        .collect();
    if !with_triggers.is_empty()
        && g.weights.pick(g.rng, &["ddl:trigger:create", "ddl:trigger:drop"])
            == "ddl:trigger:drop"
    {
        g.fire("ddl:trigger:drop");
        let ti = with_triggers[g.rng.below_usize(with_triggers.len())];
        let n = g.rng.below_usize(g.ddl.tables[ti].triggers.len());
        let tg = g.ddl.tables[ti].triggers.remove(n);
        let tname = g.ddl.tables[ti].table.name.clone();
        return vec![StmtKind::Raw(format!("DROP TRIGGER {} ON {};", tg, tname))];
    }

    g.fire("ddl:trigger:create");
    let ti = live[g.rng.below_usize(live.len())];
    let tname = g.ddl.tables[ti].table.name.clone();
    let n = g.ddl.next_trig;
    g.ddl.next_trig += 1;
    let fn_name = format!("fz_tgf_{}", n);
    let tg_name = format!("fz_tg_{}", n);

    let timing = g.weights.pick(
        g.rng,
        &[
            "ddl:trigger:before_row",
            "ddl:trigger:after_row",
            "ddl:trigger:stmt",
            "ddl:trigger:transition",
            "ddl:trigger:constraint",
        ],
    );
    g.fire(timing);

    // Transition-table statement trigger (P2): AFTER <event> REFERENCING
    // NEW/OLD TABLE, statement-level, body scans the transition relation.
    if timing == "ddl:trigger:transition" {
        let event = g.weights.pick(
            g.rng,
            &["ddl:trigger:insert", "ddl:trigger:update", "ddl:trigger:delete"],
        );
        g.fire(event);
        let (event_sql, refclause, reltab) = match event {
            "ddl:trigger:update" => (
                "UPDATE",
                "REFERENCING OLD TABLE AS fz_ot NEW TABLE AS fz_nt",
                "fz_nt",
            ),
            "ddl:trigger:delete" => ("DELETE", "REFERENCING OLD TABLE AS fz_ot", "fz_ot"),
            _ => ("INSERT", "REFERENCING NEW TABLE AS fz_nt", "fz_nt"),
        };
        let create_fn = format!(
            "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $fzt$ \
             DECLARE c int4; BEGIN SELECT count(*)::int4 INTO c FROM {}; \
             IF c < 0 THEN RAISE EXCEPTION 'fz neg'; END IF; RETURN NULL; END; $fzt$;",
            fn_name, reltab
        );
        let create_tg = format!(
            "CREATE TRIGGER {} AFTER {} ON {} {} FOR EACH STATEMENT EXECUTE FUNCTION {}();",
            tg_name, event_sql, tname, refclause, fn_name
        );
        g.ddl.tables[ti].triggers.push(tg_name);
        return vec![StmtKind::Raw(create_fn), StmtKind::Raw(create_tg)];
    }

    // Deferred constraint trigger (P2): AFTER row, DEFERRABLE INITIALLY
    // DEFERRED — the return value of an AFTER row trigger is ignored.
    if timing == "ddl:trigger:constraint" {
        let event = g.weights.pick(
            g.rng,
            &["ddl:trigger:insert", "ddl:trigger:update", "ddl:trigger:delete"],
        );
        g.fire(event);
        let event_sql = match event {
            "ddl:trigger:update" => "UPDATE",
            "ddl:trigger:delete" => "DELETE",
            _ => "INSERT",
        };
        let create_fn = format!(
            "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $fzt$ \
             BEGIN RETURN NULL; END; $fzt$;",
            fn_name
        );
        let create_tg = format!(
            "CREATE CONSTRAINT TRIGGER {} AFTER {} ON {} DEFERRABLE INITIALLY DEFERRED \
             FOR EACH ROW EXECUTE FUNCTION {}();",
            tg_name, event_sql, tname, fn_name
        );
        g.ddl.tables[ti].triggers.push(tg_name);
        return vec![StmtKind::Raw(create_fn), StmtKind::Raw(create_tg)];
    }

    let event = g.weights.pick(
        g.rng,
        &[
            "ddl:trigger:insert",
            "ddl:trigger:update",
            "ddl:trigger:insupd",
            "ddl:trigger:delete",
        ],
    );
    g.fire(event);
    let event_sql = match event {
        "ddl:trigger:update" => "UPDATE",
        "ddl:trigger:insupd" => "INSERT OR UPDATE",
        "ddl:trigger:delete" => "DELETE",
        _ => "INSERT",
    };
    let is_delete = event == "ddl:trigger:delete";
    let mut args = "()".to_string();
    let (timing_kw, row, body) = match timing {
        "ddl:trigger:before_row" => {
            // P2 body palette: NEW/OLD mutation, TG_OP tags, TG_ARGV, row
            // suppression by pk arithmetic (pk is protected + monotonic, so
            // the suppressed set is deterministic and probe-visible).
            let body: String = if is_delete {
                match g.weights.pick(g.rng, &["ddl:tgbody:ret", "ddl:tgbody:suppress"]) {
                    "ddl:tgbody:suppress" => {
                        g.fire("ddl:tgbody:suppress");
                        "BEGIN IF OLD.pk % 7 = 0 THEN RETURN NULL; END IF; RETURN OLD; END;"
                            .to_string()
                    }
                    _ => {
                        g.fire("ddl:tgbody:ret");
                        "BEGIN RETURN OLD; END;".to_string()
                    }
                }
            } else {
                let form = g.weights.pick(
                    g.rng,
                    &[
                        "ddl:tgbody:ret",
                        "ddl:tgbody:bump",
                        "ddl:tgbody:tgop",
                        "ddl:tgbody:args",
                        "ddl:tgbody:suppress",
                    ],
                );
                g.fire(form);
                match form {
                    "ddl:tgbody:bump" => {
                        "BEGIN NEW.k_int := coalesce(NEW.k_int, 0) + 1; RETURN NEW; END;"
                            .to_string()
                    }
                    "ddl:tgbody:tgop" => {
                        // k_text is protected text: TG_OP tag appends are
                        // deterministic and probe-visible.
                        "BEGIN NEW.k_text := coalesce(NEW.k_text, '') || left(TG_OP, 1); \
                         RETURN NEW; END;"
                            .to_string()
                    }
                    "ddl:tgbody:args" => {
                        args = format!("('t{}', '{}')", n, 1 + g.rng.below(9));
                        "BEGIN NEW.k_text := coalesce(NEW.k_text, '') || TG_ARGV[0]; \
                         NEW.k_int := coalesce(NEW.k_int, 0) + TG_ARGV[1]::int4; \
                         RETURN NEW; END;"
                            .to_string()
                    }
                    "ddl:tgbody:suppress" => {
                        "BEGIN IF NEW.pk % 7 = 0 THEN RETURN NULL; END IF; RETURN NEW; END;"
                            .to_string()
                    }
                    _ => "BEGIN RETURN NEW; END;".to_string(),
                }
            };
            ("BEFORE", true, body)
        }
        "ddl:trigger:after_row" => ("AFTER", true, "BEGIN RETURN NULL; END;".to_string()),
        _ => ("AFTER", false, "BEGIN RETURN NULL; END;".to_string()),
    };
    // WHEN only where the transition row is in scope: NEW for row-level
    // INSERT/UPDATE, OLD for row-level DELETE.
    let when = if row && g.rng.chance(1, 3) {
        if is_delete {
            " WHEN (OLD.k_int IS NOT NULL)"
        } else {
            " WHEN (NEW.k_int IS NOT NULL)"
        }
    } else {
        ""
    };
    let create_fn = format!(
        "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $fzt$ {} $fzt$;",
        fn_name, body
    );
    let for_each = if row { "FOR EACH ROW" } else { "FOR EACH STATEMENT" };
    let create_tg = format!(
        "CREATE TRIGGER {} {} {} ON {} {}{} EXECUTE FUNCTION {}{};",
        tg_name, timing_kw, event_sql, tname, for_each, when, fn_name, args
    );
    g.ddl.tables[ti].triggers.push(tg_name);
    vec![StmtKind::Raw(create_fn), StmtKind::Raw(create_tg)]
}

impl Table {
    /// Test helper: the shape every ddl-created table must keep so the
    /// other modules' invariants (join keys, probe sort key, DML
    /// eligibility) hold.
    #[cfg(test)]
    pub(crate) fn table_is_ddl_shape(&self) -> bool {
        self.pk.as_ref().is_some_and(|p| p.column == "pk" && p.seeded_max == 0)
            && !self.at_most_one_row
            && self.unique_key.as_deref() == Some("pk")
            && ["pk", "k_int", "k_text"]
                .iter()
                .all(|n| self.columns.iter().any(|c| &c.name == n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Session-shaped harness: persistent DdlState across groups, effective
    /// catalog rebuilt after every group (what session.rs does).
    fn gen_many(seed: u64, n: usize, spec: &str) -> (Vec<String>, Vec<String>, DdlState) {
        let base = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut state = DdlState::new();
        let mut sqls = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let eff = state.extend_catalog(&base);
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &eff, &w, &mut p, 3);
            std::mem::swap(&mut g.ddl, &mut state);
            let kinds = gen_ddl_module(&mut g);
            std::mem::swap(&mut g.ddl, &mut state);
            assert!(!kinds.is_empty());
            for k in kinds {
                sqls.push(k.to_sql());
            }
            prods.extend(p);
        }
        (sqls, prods, state)
    }

    #[test]
    fn ddl_is_deterministic() {
        let (a, _, _) = gen_many(21, 200, "");
        let (b, _, _) = gen_many(21, 200, "");
        assert_eq!(a, b);
        let (c, _, _) = gen_many(22, 200, "");
        assert_ne!(a, c);
    }

    #[test]
    fn ddl_variety() {
        let (sqls, prods, _) = gen_many(0xDD1, 1500, "");
        let all = sqls.join("\n");
        for frag in [
            "CREATE TABLE fz_ddl_",
            "CREATE TEMPORARY TABLE fz_ddl_",
            "DROP TABLE ",
            "IF EXISTS",
            "ALTER TABLE ",
            " ADD COLUMN ",
            " DROP COLUMN ",
            " TYPE ",
            " SET NOT NULL",
            " DROP NOT NULL",
            " RENAME COLUMN ",
            " ADD CHECK ",
            "CREATE INDEX fz_idx_",
            "CREATE UNIQUE INDEX ",
            "USING hash",
            "USING gin",
            "USING gist",
            "USING spgist",
            "USING brin",
            " WHERE ",
            "DROP INDEX ",
            "REINDEX ",
            "(CONCURRENTLY)",
            "CREATE VIEW fz_v_",
            "DROP VIEW ",
            "CREATE SEQUENCE fz_seq_",
            "nextval(",
            "setval(",
            "DROP SEQUENCE ",
            "CREATE AGGREGATE fz_agg_",
            "DROP AGGREGATE ",
            "CREATE FUNCTION fz_tgf_",
            "CREATE TRIGGER fz_tg_",
            "TRUNCATE TABLE ",
            " DEFAULT ",
            "CHECK (",
        ] {
            assert!(all.contains(frag), "ddl flavor {frag:?} never generated");
        }
        for p in [
            "ddl:create_table",
            "ddl:drop_table",
            "ddl:alter",
            "ddl:alter:add_col",
            "ddl:alter:drop_col",
            "ddl:alter:type",
            "ddl:alter:rename_col",
            "ddl:create_index",
            "ddl:idx:btree",
            "ddl:idx:spgist",
            "ddl:drop_index",
            "ddl:reindex",
            "ddl:view",
            "ddl:seq:create",
            "ddl:seq:nextval",
            "ddl:agg",
            "ddl:trigger:create",
            "ddl:truncate",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        for sql in &sqls {
            assert!(!sql.contains('\n') && sql.ends_with(';'), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
        }
    }

    /// The zero-42xxx discipline, statically: replay the stream against a
    /// text-level model and assert no statement references a dead or
    /// never-created object, fixture tables are never dropped/altered/
    /// truncated, and protected columns never leave.
    #[test]
    fn references_are_always_live_and_fixtures_untouched() {
        let (sqls, _, _) = gen_many(0xF00D, 2000, "");
        let mut live_tables: Vec<String> = Vec::new();
        let mut dead_tables: Vec<String> = Vec::new();
        let mut live_indexes: Vec<String> = Vec::new();
        let mut live_seqs: Vec<String> = Vec::new();
        let word = |hay: &str, name: &str| {
            // name occurrence not followed by an identifier char.
            hay.match_indices(name).any(|(i, _)| {
                hay[i + name.len()..]
                    .chars()
                    .next()
                    .is_none_or(|c| !c.is_alphanumeric() && c != '_')
            })
        };
        for sql in &sqls {
            for t in ["fz_scalar", "fz_mixed", "fz_wide", "fz_empty", "fz_rich", "fz_one"] {
                assert!(!sql.starts_with(&format!("DROP TABLE {t}")), "{sql}");
                assert!(!sql.starts_with(&format!("DROP TABLE IF EXISTS {t}")), "{sql}");
                assert!(!sql.starts_with(&format!("ALTER TABLE {t}")), "{sql}");
                assert!(!sql.contains(&format!("TRUNCATE TABLE {t}")), "{sql}");
            }
            for dead in &dead_tables {
                assert!(!word(sql, dead), "reference to dropped table {dead}: {sql}");
            }
            // Protected columns never dropped/renamed/re-typed.
            for c in PROTECTED_COLS {
                assert!(!sql.contains(&format!("DROP COLUMN {c} ")), "{sql}");
                assert!(!sql.contains(&format!("RENAME COLUMN {c} ")), "{sql}");
                assert!(!sql.contains(&format!("ALTER COLUMN {c} TYPE")), "{sql}");
            }
            if let Some(rest) = sql
                .strip_prefix("CREATE TABLE ")
                .or_else(|| sql.strip_prefix("CREATE TEMPORARY TABLE "))
            {
                let name = rest.split(' ').next().unwrap().to_string();
                assert!(
                    !live_tables.contains(&name) && !dead_tables.contains(&name),
                    "table name reused: {sql}"
                );
                live_tables.push(name);
            } else if let Some(rest) = sql
                .strip_prefix("DROP TABLE IF EXISTS ")
                .or_else(|| sql.strip_prefix("DROP TABLE "))
            {
                // FK-referenced tables drop with CASCADE.
                let name = rest
                    .trim_end_matches(';')
                    .trim_end_matches(" CASCADE")
                    .to_string();
                assert!(live_tables.contains(&name), "drop of non-live table: {sql}");
                live_tables.retain(|t| t != &name);
                dead_tables.push(name);
            } else if let Some(rest) = sql
                .strip_prefix("CREATE INDEX ")
                .or_else(|| sql.strip_prefix("CREATE UNIQUE INDEX "))
            {
                let name = rest.split(' ').next().unwrap().to_string();
                let tname = sql.split(" ON ").nth(1).unwrap().split([' ', '(']).next();
                let tname = tname.unwrap().to_string();
                assert!(
                    live_tables.contains(&tname)
                        || ["fz_scalar", "fz_mixed", "fz_wide", "fz_empty", "fz_rich", "fz_one"]
                            .contains(&tname.as_str()),
                    "index on dead table: {sql}"
                );
                live_indexes.push(name);
            } else if let Some(rest) = sql
                .strip_prefix("DROP INDEX IF EXISTS ")
                .or_else(|| sql.strip_prefix("DROP INDEX "))
            {
                let name = rest.trim_end_matches(';').to_string();
                assert!(live_indexes.contains(&name), "drop of non-live index: {sql}");
                live_indexes.retain(|x| x != &name);
            } else if let Some(rest) = sql
                .strip_prefix("REINDEX (CONCURRENTLY) INDEX ")
                .or_else(|| sql.strip_prefix("REINDEX INDEX "))
            {
                let name = rest.trim_end_matches(';').to_string();
                assert!(live_indexes.contains(&name), "reindex of non-live index: {sql}");
            } else if let Some(rest) = sql
                .strip_prefix("REINDEX (CONCURRENTLY) TABLE ")
                .or_else(|| sql.strip_prefix("REINDEX TABLE "))
            {
                let name = rest.trim_end_matches(';').to_string();
                assert!(
                    live_tables.contains(&name)
                        || ["fz_scalar", "fz_mixed", "fz_wide", "fz_empty", "fz_rich", "fz_one"]
                            .contains(&name.as_str()),
                    "reindex of non-live table: {sql}"
                );
            } else if let Some(rest) = sql.strip_prefix("CREATE SEQUENCE ") {
                live_seqs.push(rest.split([' ', ';']).next().unwrap().to_string());
            } else if let Some(rest) = sql
                .strip_prefix("DROP SEQUENCE IF EXISTS ")
                .or_else(|| sql.strip_prefix("DROP SEQUENCE "))
            {
                let name = rest.trim_end_matches(';').to_string();
                assert!(live_seqs.contains(&name), "drop of non-live sequence: {sql}");
                live_seqs.retain(|s| s != &name);
            } else if sql.contains("nextval('") || sql.contains("currval('") || sql.contains("setval('") {
                let name = sql.split('\'').nth(1).unwrap().to_string();
                assert!(live_seqs.contains(&name), "use of non-live sequence: {sql}");
            }
            // A DROP INDEX target killed implicitly (table drop / column
            // drop CASCADE) must never be referenced again — enforced by
            // never seeing an unknown index name above.
        }
    }

    #[test]
    fn extend_catalog_tracks_live_set_and_events_pair_up() {
        let base = FixtureCatalog.load_catalog().unwrap();
        let (_, _, state) = gen_many(0xCAFE, 600, "");
        let eff = state.extend_catalog(&base);
        let live_n = state.tables.iter().filter(|t| t.live).count();
        assert_eq!(eff.tables.len(), base.tables.len() + live_n);
        assert!(live_n <= MAX_LIVE_TABLES);
        // Every ddl table in the effective catalog is DML-eligible and
        // probe-able and carries the protected trio.
        for t in &eff.tables[base.tables.len()..] {
            assert!(t.table_is_ddl_shape(), "{:?}", t.name);
        }
    }

    #[test]
    fn views_and_aggregates_are_group_local() {
        let (sqls, _, _) = gen_many(
            0xBEEF,
            400,
            "ddl:view=10,ddl:agg=10,ddl:create_table=1,ddl:drop_table=0,ddl:alter=0,\
             ddl:create_index=0,ddl:drop_index=0,ddl:reindex=0,ddl:seq=0,ddl:trigger=0,\
             ddl:truncate=0",
        );
        let mut open_view: Option<String> = None;
        let mut open_agg: Option<String> = None;
        for sql in &sqls {
            if let Some(rest) = sql.strip_prefix("CREATE VIEW ") {
                assert!(open_view.is_none());
                open_view = Some(rest.split(' ').next().unwrap().to_string());
            } else if let Some(rest) = sql.strip_prefix("DROP VIEW ") {
                let name = rest.trim_end_matches(';');
                assert_eq!(open_view.take().as_deref(), Some(name), "{sql}");
            } else if let Some(rest) = sql.strip_prefix("CREATE AGGREGATE ") {
                assert!(open_agg.is_none());
                open_agg = Some(rest.split('(').next().unwrap().to_string());
            } else if let Some(rest) = sql.strip_prefix("DROP AGGREGATE ") {
                let name = rest.split('(').next().unwrap();
                assert_eq!(open_agg.take().as_deref(), Some(name), "{sql}");
            }
        }
        assert!(open_view.is_none() && open_agg.is_none(), "unclosed group at end");
        assert!(sqls.iter().any(|s| s.starts_with("CREATE VIEW ")));
        assert!(sqls.iter().any(|s| s.starts_with("CREATE AGGREGATE ")));
    }

    #[test]
    fn unique_indexes_lead_with_pk() {
        let (sqls, _, _) = gen_many(
            3,
            800,
            "ddl:create_index=10,ddl:idx:unique=10,ddl:idx:plain=0,ddl:create_table=3",
        );
        let mut saw = false;
        for sql in &sqls {
            if let Some(rest) = sql.strip_prefix("CREATE UNIQUE INDEX ") {
                saw = true;
                let keys = rest.split(" (").nth(1).unwrap();
                assert!(keys.starts_with("pk"), "unique index without leading pk: {sql}");
                assert!(sql.contains(" ON fz_ddl_"), "unique index off a ddl table: {sql}");
            }
        }
        assert!(saw, "no unique index generated under heavy bias");
    }

    /// Foreign keys (X1): the action palette fires, references only ever
    /// name a live ddl table's protected pk, temp-ness matches across every
    /// reference (Postgres forbids mixing), and every referenced table's
    /// DROP/TRUNCATE carries CASCADE so it can never fail on a dependent
    /// constraint and desync the model.
    #[test]
    fn foreign_keys_reference_live_pks_and_parents_drop_cascade() {
        let (sqls, prods, state) = gen_many(
            0xFC0,
            1200,
            "ddl:fk=10,ddl:create_table=3,ddl:drop_table=2,ddl:truncate=1,ddl:alter=1,\
             ddl:create_index=0,ddl:drop_index=0,ddl:reindex=0,ddl:view=0,ddl:seq=0,\
             ddl:agg=0,ddl:trigger=0",
        );
        let mut saw_create_ref = false;
        let mut saw_alter_add = false;
        // Which tables were ever named as an FK parent, replayed textually.
        let mut parents: Vec<String> = Vec::new();
        let mut temp_of: std::collections::HashMap<String, bool> =
            std::collections::HashMap::new();
        for sql in &sqls {
            if let Some(rest) = sql
                .strip_prefix("CREATE TABLE ")
                .or_else(|| sql.strip_prefix("CREATE TEMPORARY TABLE "))
            {
                let name = rest.split(' ').next().unwrap().to_string();
                temp_of.insert(name, sql.starts_with("CREATE TEMPORARY"));
            }
            if let Some(p) = sql.find(" REFERENCES ") {
                let parent = sql[p + " REFERENCES ".len()..]
                    .split([' ', '('])
                    .next()
                    .unwrap()
                    .to_string();
                // Always the protected pk column.
                assert!(
                    sql[p..].contains(&format!("REFERENCES {parent} (pk)")),
                    "FK not over pk: {sql}"
                );
                // Actions always fully spelled.
                assert!(sql.contains(" ON DELETE ") && sql.contains(" ON UPDATE "), "{sql}");
                if sql.starts_with("ALTER TABLE ") {
                    saw_alter_add = true;
                    assert!(sql.contains(" ADD FOREIGN KEY (k_int) "), "{sql}");
                    let child = sql["ALTER TABLE ".len()..].split(' ').next().unwrap();
                    assert_eq!(
                        temp_of.get(child).copied(),
                        temp_of.get(&parent).copied(),
                        "temp-ness mismatch across FK: {sql}"
                    );
                } else {
                    saw_create_ref = true;
                    let child = sql
                        .strip_prefix("CREATE TABLE ")
                        .or_else(|| sql.strip_prefix("CREATE TEMPORARY TABLE "))
                        .unwrap()
                        .split(' ')
                        .next()
                        .unwrap();
                    if child != parent {
                        assert_eq!(
                            temp_of.get(child).copied(),
                            temp_of.get(&parent).copied(),
                            "temp-ness mismatch across FK: {sql}"
                        );
                    }
                }
                if !parents.contains(&parent) {
                    parents.push(parent);
                }
            }
            // Once a table is an FK parent, its DROP/TRUNCATE cascades.
            for verb in ["DROP TABLE IF EXISTS ", "DROP TABLE "] {
                if let Some(rest) = sql.strip_prefix(verb) {
                    let name = rest.trim_end_matches(';').trim_end_matches(" CASCADE");
                    if parents.iter().any(|p| p == name) {
                        assert!(sql.contains(" CASCADE"), "FK parent dropped bare: {sql}");
                    }
                }
            }
            if let Some(rest) = sql.strip_prefix("TRUNCATE TABLE ") {
                // Exact names only: fz_ddl_5 must not match fz_ddl_50.
                let list = rest
                    .trim_end_matches(';')
                    .trim_end_matches(" CASCADE")
                    .trim_end_matches(" RESTART IDENTITY");
                let named: Vec<&str> = list.split(", ").collect();
                if parents.iter().any(|p| named.contains(&p.as_str())) {
                    assert!(sql.contains(" CASCADE"), "FK parent truncated bare: {sql}");
                }
            }
        }
        assert!(saw_create_ref, "no CREATE TABLE ... REFERENCES generated");
        assert!(saw_alter_add, "no ALTER TABLE ADD FOREIGN KEY generated");
        for p in [
            "ddl:fk",
            "ddl:fk:create_child",
            "ddl:fk:alter_add",
            "ddl:fk:self",
            "ddl:fk:valid",
            "ddl:fk:notvalid",
            "ddl:fk:del:cascade",
            "ddl:fk:del:setnull",
            "ddl:fk:del:setdefault",
            "ddl:fk:del:restrict",
            "ddl:fk:upd:cascade",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        // FK-created children keep the ddl shape other modules rely on.
        assert!(!state.fk_parents.is_empty());
        for t in state.tables.iter().filter(|t| t.live) {
            assert!(t.table.table_is_ddl_shape(), "{}", t.table.name);
        }
    }

    #[test]
    fn trigger_statements_are_well_formed() {
        let (sqls, prods, _) = gen_many(
            9,
            900,
            "ddl:trigger=10,ddl:create_table=3,ddl:drop_table=0,ddl:alter=0,\
             ddl:create_index=0,ddl:drop_index=0,ddl:reindex=0,ddl:view=0,ddl:seq=0,\
             ddl:agg=0,ddl:truncate=0",
        );
        let mut saw_tg = false;
        for (i, sql) in sqls.iter().enumerate() {
            if sql.starts_with("CREATE FUNCTION fz_tgf_") {
                assert!(sql.contains("RETURNS trigger LANGUAGE plpgsql"), "{sql}");
                assert!(sql.contains("$fzt$"), "{sql}");
                // Its CREATE TRIGGER follows immediately (same group).
                let next = &sqls[i + 1];
                assert!(
                    next.starts_with("CREATE TRIGGER fz_tg_")
                        || next.starts_with("CREATE CONSTRAINT TRIGGER fz_tg_"),
                    "{next}"
                );
                saw_tg = true;
                // DELETE row triggers never reference NEW (transition
                // statement triggers scan fz_nt/fz_ot, never NEW/OLD rows).
                if next.contains(" DELETE ON ") {
                    assert!(!next.contains("WHEN (NEW"), "{next}");
                    assert!(!sql.contains("NEW."), "DELETE trigger body uses NEW: {sql}");
                }
                // Transition triggers are statement-level and their body
                // reads the relation the REFERENCING clause names.
                if next.contains("REFERENCING") {
                    assert!(next.contains("FOR EACH STATEMENT"), "{next}");
                    let reltab = if next.contains("NEW TABLE AS fz_nt") { "fz_nt" } else { "fz_ot" };
                    assert!(sql.contains(&format!("FROM {reltab}")), "{sql}");
                }
                // Constraint triggers are row-level and deferred.
                if next.starts_with("CREATE CONSTRAINT TRIGGER") {
                    assert!(next.contains("DEFERRABLE INITIALLY DEFERRED"), "{next}");
                    assert!(next.contains("FOR EACH ROW"), "{next}");
                }
                // TG_ARGV bodies always come with trigger arguments.
                if sql.contains("TG_ARGV") {
                    assert!(!next.ends_with("()) ;") && !next.contains("FUNCTION fz_tgf_() "), "{next}");
                    let tail = next.rsplit("EXECUTE FUNCTION ").next().unwrap();
                    assert!(tail.contains("('t"), "TG_ARGV body without args: {next}");
                }
            }
        }
        assert!(saw_tg, "no trigger generated under heavy bias");
        for p in [
            "ddl:trigger:transition",
            "ddl:trigger:constraint",
            "ddl:tgbody:tgop",
            "ddl:tgbody:args",
            "ddl:tgbody:suppress",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }
}
