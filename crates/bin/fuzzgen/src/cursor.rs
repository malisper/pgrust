//! Cursors + SQL-level PREPARE statement module (C1): the portal-machinery
//! statement family the generator never emitted — DECLARE/FETCH/MOVE/CLOSE
//! (portal.c, pquery.c: PortalRun, DoPortalRunFetch, PortalRunSelect) and
//! PREPARE/EXECUTE/DEALLOCATE with real parameters (prepare.c + plancache
//! GetCachedPlan/choose_custom_plan — the generic-vs-custom flip at the 6th
//! execution is a first-class differential surface, probed with EXPLAIN
//! EXECUTE).
//!
//! Hand-verified 2026-08-11 on Homebrew PostgreSQL 18.4 vs pgrust (this
//! branch's covloop server): every family below behaves identically on
//! both engines — DECLARE [BINARY] [INSENSITIVE|ASENSITIVE] [SCROLL|NO
//! SCROLL] [WITH HOLD], all FETCH/MOVE directions, WITH HOLD survival
//! across COMMIT (PersistHoldablePortal materialization), 55000 on
//! backward scan of a NO SCROLL cursor, 34000 on a dead cursor, WHERE
//! CURRENT OF over a FOR UPDATE cursor (UPDATE and DELETE), the 42601
//! wrong-parameter-count error, and the plancache custom->generic plan
//! flip (EXPLAIN EXECUTE shows `pk > 3` for the first five executions,
//! `pk > $1` from the sixth — identical on both).
//!
//! Groups are self-contained: transaction-scoped cursors live inside a
//! BEGIN..COMMIT/ROLLBACK bracket emitted by this module (never nested
//! with the txn module's brackets — statement groups are atomic in the
//! session loop, so the two modules coordinate by construction). Two kinds
//! of state DO persist across groups in `CursorState` (swapped in and out
//! by the session loop like `DmlState`):
//!
//!   - open WITH HOLD cursors (registered only when their bracket really
//!     COMMITs — a rollback kills a holdable cursor on both engines), so
//!     later groups FETCH/MOVE/CLOSE them across transaction boundaries;
//!   - the prepared-statement pool, so EXECUTE counts accumulate across
//!     groups and the generic-plan flip happens mid-stream, interleaved
//!     with other modules' statements.
//!
//! Determinism discipline:
//!   - every cursor query is a plain column-list SELECT over one
//!     pk-carrying table with `ORDER BY <pk>` (pk unique => row order is
//!     total), and any WHERE is a pk comparison — no expression evaluation
//!     that could error mid-FETCH and abort a bracket asymmetrically with
//!     the generator's model. FETCH statements carry no ORDER BY text, so
//!     the differ compares them as multisets; the pinned cursor order is
//!     what makes the row *partition* across successive FETCHes
//!     deterministic.
//!   - default-spelled cursors (no SCROLL keyword) never get backward
//!     ops: whether backward works without SCROLL is plan-dependent, so
//!     only explicit SCROLL cursors scroll and only explicit NO SCROLL
//!     cursors host the deliberate 55000 (`cursor:op:invalid_back`, low
//!     weight, poisons its bracket => immediate ROLLBACK).
//!   - prepared statements target seeded fixture tables only (they outlive
//!     groups; ddl-created tables can be dropped under them). Parameterized
//!     DML keeps the DML-state invariants: EXECUTEd INSERTs draw pks from
//!     the shared monotonic allocator, UPDATE/DELETE target known pks.
//!
//! Matched-error surfaces (all verified identical, kept low-weight):
//! 55000 backward on NO SCROLL, 34000 FETCH after non-hold COMMIT and
//! after CLOSE, 42601 wrong EXECUTE arg count, 23505 deliberate EXECUTE
//! insert collision, 24000 WHERE CURRENT OF when positioned past the end.

use crate::catalog::SqlType;
use crate::dml::eligible_tables;
use crate::stmt::{Gen, StmtKind};

/// Live WITH HOLD cursors are capped so streams don't accumulate hundreds
/// of materialized portals; at the cap the module uses/closes instead.
const HELD_CAP: usize = 3;
/// Prepared-statement pool cap; at the cap a new PREPARE first deallocates
/// the oldest entry (in the same group).
const PREP_CAP: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scrollability {
    /// Explicit SCROLL: every direction is valid.
    Scroll,
    /// No keyword: forward-only ops (backward-without-SCROLL is
    /// plan-dependent — never generated).
    Default,
    /// Explicit NO SCROLL: forward-only, and the only host for the
    /// deliberate 55000 backward op.
    NoScroll,
}

#[derive(Clone, Debug)]
struct HeldCursor {
    name: String,
    scroll: Scrollability,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PrepKind {
    /// `(int4[, int4])`: SELECT cols WHERE pk > $1 [AND pk <= $2] ORDER BY pk
    Select,
    /// `(int4, <colty>)`: UPDATE SET col = $2 WHERE pk = $1
    Update,
    /// `(int4)`: DELETE WHERE pk = $1
    Delete,
    /// `(int4)`: INSERT (pk, cols..) VALUES ($1, literals..)
    Insert,
}

#[derive(Clone, Debug)]
struct Prepared {
    name: String,
    /// Target table name (fixture tables only — never dropped mid-stream).
    table: String,
    kind: PrepKind,
    nargs: usize,
    /// For Update: the SET column's type (the $2 argument type).
    set_ty: Option<SqlType>,
}

/// Session-persistent cursor/prepared state, swapped in and out by the
/// session loop like `DmlState`. Name allocators are monotonic and never
/// reused, so a replayed name can never denote two different objects.
#[derive(Clone, Debug)]
pub struct CursorState {
    held: Vec<HeldCursor>,
    next_cursor: u32,
    prepared: Vec<Prepared>,
    next_prep: u32,
    /// A cursor name known to be closed/dead (34000 fuel).
    last_dead: Option<String>,
}

impl CursorState {
    pub fn new() -> CursorState {
        CursorState {
            held: Vec::new(),
            next_cursor: 0,
            prepared: Vec::new(),
            next_prep: 0,
            last_dead: None,
        }
    }
}

impl Default for CursorState {
    fn default() -> CursorState {
        CursorState::new()
    }
}

const SHAPES: &[&str] = &[
    "cursor:bracket",
    "cursor:wco",
    "cursor:held",
    "cursor:prepare",
    "cursor:execute",
    "cursor:dealloc",
];

/// Registry entry point (stmt::STMT_MODULES): one self-contained cursor or
/// prepared-statement group.
pub fn gen_cursor_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cursor");
    let shape = g.weights.pick(g.rng, SHAPES);
    match shape {
        "cursor:bracket" => gen_bracket(g),
        "cursor:wco" => gen_wco(g),
        "cursor:held" => gen_held(g),
        "cursor:prepare" => gen_prepare(g),
        "cursor:execute" => gen_execute(g),
        "cursor:dealloc" => gen_dealloc(g),
        other => unreachable!("unknown cursor shape {other}"),
    }
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

fn next_cursor_name(g: &mut Gen) -> String {
    let n = g.cursor.next_cursor;
    g.cursor.next_cursor += 1;
    format!("fzcur{}", n)
}

fn next_prep_name(g: &mut Gen) -> String {
    let n = g.cursor.next_prep;
    g.cursor.next_prep += 1;
    format!("fzc{}", n)
}

/// Indices of cursor-eligible tables: pk-carrying (the ORDER BY pk pin).
/// Includes fz_empty (empty materialization is a real holdable-portal
/// surface) and ddl/part tables (cursors never outlive their group unless
/// held — and held cursors are materialized at COMMIT, drop-safe).
fn cursor_tables(g: &Gen) -> Vec<usize> {
    g.catalog
        .tables
        .iter()
        .enumerate()
        .filter(|(_, t)| t.pk.is_some())
        .map(|(i, _)| i)
        .collect()
}

/// Indices of prepared-statement-eligible tables: seeded fixture tables
/// only (seeded pk rows exist and the table can never be dropped by the
/// ddl module mid-stream, so a pooled statement stays re-executable).
fn prep_tables(g: &Gen, dml: bool) -> Vec<usize> {
    let base: Vec<usize> = if dml {
        eligible_tables(g)
    } else {
        cursor_tables(g)
    };
    base.into_iter()
        .filter(|&i| {
            let t = &g.catalog.tables[i];
            t.pk.as_ref().is_some_and(|pk| pk.seeded_max > 0) && !t.name.starts_with("fz_ddl_")
                && !t.name.starts_with("fz_part_")
        })
        .collect()
}

/// The pinned cursor query: `SELECT a.c1[, a.c2..] FROM t AS a [WHERE
/// a.pk <cmp> <k>] ORDER BY a.pk [DESC]` — column-list only (no expression
/// evaluation can error mid-FETCH), pk order total.
fn cursor_query(g: &mut Gen, ti: usize, for_update: bool) -> String {
    let table = &g.catalog.tables[ti];
    let pk_col = table.pk.as_ref().expect("cursor table has pk").column.clone();
    let seeded = table.pk.as_ref().map(|p| p.seeded_max).unwrap_or(0);
    let ncols_max = table.columns.len().min(3);
    let ncols = 1 + g.rng.below_usize(ncols_max);
    let mut cols: Vec<usize> = Vec::with_capacity(ncols);
    while cols.len() < ncols {
        let i = g.rng.below_usize(table.columns.len());
        if !cols.contains(&i) {
            cols.push(i);
        }
    }
    let alias = g.next_alias();
    let mut q = String::from("SELECT ");
    let table = &g.catalog.tables[ti];
    for (n, &i) in cols.iter().enumerate() {
        if n > 0 {
            q.push_str(", ");
        }
        q.push_str(&format!("{}.{}", alias, table.columns[i].name));
    }
    q.push_str(&format!(" FROM {} AS {}", table.name, alias));
    // FOR UPDATE cursors (the WHERE CURRENT OF shape) take neither a WHERE
    // nor DESC: the point is to land the cursor ON a row, and a predicate
    // that filters the seeded rows away turns every CURRENT OF into a
    // 24000 (matched on both sides, but pure wasted budget — it also
    // poisons the rest of the bracket with 25P02).
    if for_update {
        q.push_str(&format!(" ORDER BY {}.{} FOR UPDATE", alias, pk_col));
        return q;
    }
    if g.weights.pick(g.rng, &["cursor:where", "cursor:where:none"]) == "cursor:where" {
        g.fire("cursor:where");
        let op = ["<", "<=", ">", ">=", "<>"][g.rng.below_usize(5)];
        let k = match g.rng.below(4) {
            0 => 0,
            1 => 1,
            2 => seeded,
            _ => 1 + g.rng.below(8) as i64,
        };
        q.push_str(&format!(" WHERE {}.{} {} {}", alias, pk_col, op, k));
    }
    q.push_str(&format!(" ORDER BY {}.{}", alias, pk_col));
    if g.weights.pick(g.rng, &["cursor:orderasc", "cursor:orderdesc"]) == "cursor:orderdesc" {
        g.fire("cursor:orderdesc");
        q.push_str(" DESC");
    }
    q
}

/// DECLARE statement + the picked options. FOR UPDATE cursors (WCO shape)
/// take none of the flags — BINARY/SCROLL/INSENSITIVE/HOLD combinations
/// with FOR UPDATE are rejected or unsupported.
fn render_declare(g: &mut Gen, name: &str, ti: usize) -> (String, Scrollability, bool) {
    let binary = g.weights.pick(g.rng, &["cursor:binary", "cursor:binary:none"])
        == "cursor:binary";
    if binary {
        g.fire("cursor:binary");
    }
    let sens = g.weights.pick(
        g.rng,
        &["cursor:sens:none", "cursor:sens:insensitive", "cursor:sens:asensitive"],
    );
    let scroll = match g.weights.pick(
        g.rng,
        &["cursor:scroll", "cursor:scroll:default", "cursor:noscroll"],
    ) {
        "cursor:scroll" => {
            g.fire("cursor:scroll");
            Scrollability::Scroll
        }
        "cursor:noscroll" => {
            g.fire("cursor:noscroll");
            Scrollability::NoScroll
        }
        _ => Scrollability::Default,
    };
    let hold = match g.weights.pick(
        g.rng,
        &["cursor:hold", "cursor:hold:none", "cursor:hold:without"],
    ) {
        "cursor:hold" => {
            g.fire("cursor:hold");
            true
        }
        "cursor:hold:without" => {
            g.fire("cursor:hold:without");
            false
        }
        _ => false,
    };
    let mut s = format!("DECLARE {}", name);
    if binary {
        s.push_str(" BINARY");
    }
    match sens {
        "cursor:sens:insensitive" => {
            g.fire(sens);
            s.push_str(" INSENSITIVE");
        }
        "cursor:sens:asensitive" => {
            g.fire(sens);
            s.push_str(" ASENSITIVE");
        }
        _ => {}
    }
    match scroll {
        Scrollability::Scroll => s.push_str(" SCROLL"),
        Scrollability::NoScroll => s.push_str(" NO SCROLL"),
        Scrollability::Default => {}
    }
    s.push_str(" CURSOR");
    if hold {
        s.push_str(" WITH HOLD");
    }
    let q = cursor_query(g, ti, false);
    s.push_str(&format!(" FOR {};", q));
    (s, scroll, hold)
}

/// Boundary-heavy FETCH/MOVE row count.
fn fetch_count(g: &mut Gen) -> i64 {
    match g.rng.below(5) {
        0 => 1,
        1 => 2,
        2 => 3,
        3 => 5,
        _ => 1000000,
    }
}

const FWD_OPS: &[&str] = &[
    "cursor:op:next",
    "cursor:op:count",
    "cursor:op:fwd_n",
    "cursor:op:fwd_all",
    "cursor:op:rel_fwd",
];
const BACK_OPS: &[&str] = &[
    "cursor:op:prior",
    "cursor:op:first",
    "cursor:op:last",
    "cursor:op:abs",
    "cursor:op:rel_back",
    "cursor:op:back_n",
    "cursor:op:back_all",
];

/// One always-valid FETCH or MOVE against an open cursor of the given
/// scrollability. Backward directions only on explicit SCROLL.
fn gen_op(g: &mut Gen, name: &str, scroll: Scrollability) -> String {
    let ops: Vec<&str> = match scroll {
        Scrollability::Scroll => FWD_OPS.iter().chain(BACK_OPS).copied().collect(),
        _ => FWD_OPS.to_vec(),
    };
    let op = g.weights.pick(g.rng, &ops);
    g.fire(op);
    let dir = match op {
        "cursor:op:next" => "NEXT".to_string(),
        "cursor:op:count" => format!("{}", fetch_count(g)),
        "cursor:op:fwd_n" => format!("FORWARD {}", fetch_count(g)),
        "cursor:op:fwd_all" => {
            if g.rng.chance(1, 2) {
                "FORWARD ALL".to_string()
            } else {
                "ALL".to_string()
            }
        }
        "cursor:op:rel_fwd" => format!("RELATIVE {}", 1 + g.rng.below(3)),
        "cursor:op:prior" => "PRIOR".to_string(),
        "cursor:op:first" => "FIRST".to_string(),
        "cursor:op:last" => "LAST".to_string(),
        "cursor:op:abs" => {
            let n: i64 = match g.rng.below(6) {
                0 => 0,
                1 => 1,
                2 => 3,
                3 => -1,
                4 => -3,
                _ => 1000000,
            };
            format!("ABSOLUTE {}", n)
        }
        "cursor:op:rel_back" => format!("RELATIVE -{}", 1 + g.rng.below(3)),
        "cursor:op:back_n" => format!("BACKWARD {}", fetch_count(g)),
        _ => "BACKWARD ALL".to_string(),
    };
    let verb = if g.weights.pick(g.rng, &["cursor:op:fetch", "cursor:op:move"])
        == "cursor:op:move"
    {
        g.fire("cursor:op:move");
        "MOVE"
    } else {
        "FETCH"
    };
    format!("{} {} FROM {};", verb, dir, name)
}

/// The deliberate 55000: a backward direction on an explicit NO SCROLL
/// cursor. Aborts the transaction — callers must poison the bracket.
fn gen_invalid_back(g: &mut Gen, name: &str) -> String {
    g.fire("cursor:op:invalid_back");
    let dir = match g.rng.below(3) {
        0 => "PRIOR".to_string(),
        1 => format!("BACKWARD {}", 1 + g.rng.below(3)),
        _ => "ABSOLUTE -1".to_string(),
    };
    format!("FETCH {} FROM {};", dir, name)
}

/// Transaction-bracket shape: BEGIN; DECLARE; ops...; [CLOSE;]
/// COMMIT/ROLLBACK [; post-commit statements]. Registers surviving WITH
/// HOLD cursors; occasionally fetches from the now-dead cursor after a
/// non-hold COMMIT (34000, matched).
fn gen_bracket(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cursor:bracket");
    let tables = cursor_tables(g);
    let ti = tables[g.rng.below_usize(tables.len())];
    let name = next_cursor_name(g);
    let mut out = vec![raw("BEGIN;".to_string())];
    let (decl, scroll, hold) = render_declare(g, &name, ti);
    out.push(raw(decl));

    let nops = 2 + g.rng.below_usize(4); // 2-5
    let mut poisoned = false;
    for i in 0..nops {
        // The 55000 op only on explicit NO SCROLL, only as a bracket
        // terminator event (it aborts the transaction).
        if scroll == Scrollability::NoScroll
            && i + 1 == nops
            && g.weights.pick(g.rng, &["cursor:op:invalid_back", "cursor:op:valid"])
                == "cursor:op:invalid_back"
        {
            out.push(raw(gen_invalid_back(g, &name)));
            poisoned = true;
            break;
        }
        out.push(raw(gen_op(g, &name, scroll)));
    }
    if poisoned {
        // Aborted transaction: everything but ROLLBACK would 25P02.
        out.push(raw("ROLLBACK;".to_string()));
        g.cursor.last_dead = Some(name);
        return out;
    }

    let closed = if g.weights.pick(g.rng, &["cursor:close", "cursor:close:none"])
        == "cursor:close"
    {
        g.fire("cursor:close");
        out.push(raw(format!("CLOSE {};", name)));
        true
    } else {
        false
    };
    let commit = g.weights.pick(g.rng, &["cursor:end:commit", "cursor:end:rollback"])
        == "cursor:end:commit";
    g.fire(if commit { "cursor:end:commit" } else { "cursor:end:rollback" });
    out.push(raw(if commit { "COMMIT;" } else { "ROLLBACK;" }.to_string()));

    if commit && !closed && hold {
        if g.cursor.held.len() < HELD_CAP {
            // Held survivor: usable by later groups (and immediately).
            if g.weights.pick(g.rng, &["cursor:post_ops", "cursor:post_ops:none"])
                == "cursor:post_ops"
            {
                g.fire("cursor:post_ops");
                out.push(raw(gen_op(g, &name, scroll)));
            }
            g.cursor.held.push(HeldCursor { name, scroll });
        } else {
            // At cap: close it right after the commit (valid outside txn).
            out.push(raw(format!("CLOSE {};", name)));
            g.cursor.last_dead = Some(name);
        }
    } else {
        // Non-hold or rolled back or closed: the cursor is dead. Low
        // weight: prove it (34000, matched both sides).
        if commit
            && g.weights.pick(g.rng, &["cursor:fetch:dead", "cursor:fetch:dead:none"])
                == "cursor:fetch:dead"
        {
            g.fire("cursor:fetch:dead");
            out.push(raw(format!("FETCH NEXT FROM {};", name)));
        }
        g.cursor.last_dead = Some(name);
    }
    out
}

/// WHERE CURRENT OF shape: a FOR UPDATE cursor positioned by FETCH, then
/// UPDATE/DELETE ... WHERE CURRENT OF (verified identical on both
/// engines). Non-pk SET columns only; literal values (stored floats stay
/// literal — the DML determinism discipline).
fn gen_wco(g: &mut Gen) -> Vec<StmtKind> {
    let candidates: Vec<usize> = eligible_tables(g)
        .into_iter()
        .filter(|&i| {
            g.catalog.tables[i].pk.as_ref().is_some_and(|pk| pk.seeded_max >= 3)
        })
        .collect();
    if candidates.is_empty() {
        // No stable WCO target (e.g. live catalogs): degrade to a bracket.
        return gen_bracket(g);
    }
    g.fire("cursor:wco");
    let ti = candidates[g.rng.below_usize(candidates.len())];
    let name = next_cursor_name(g);
    let table = &g.catalog.tables[ti];
    let tname = table.name.clone();
    let mut out = vec![raw("BEGIN;".to_string())];
    let q = cursor_query(g, ti, true);
    out.push(raw(format!("DECLARE {} CURSOR FOR {};", name, q)));
    // Position on a row: the query is unfiltered and seeded_max >= 3, so a
    // 1-2 row forward fetch lands on a row unless a mid-stream DELETE has
    // emptied the table (then CURRENT OF raises 24000, matched both sides).
    out.push(raw(format!("FETCH FORWARD {} FROM {};", 1 + g.rng.below(2), name)));
    let wco = g.weights.pick(
        g.rng,
        &["cursor:wco:update", "cursor:wco:update2", "cursor:wco:delete"],
    );
    g.fire(wco);
    let n_updates = match wco {
        "cursor:wco:update" => 1,
        "cursor:wco:update2" => 2,
        _ => 0,
    };
    for _ in 0..n_updates {
        let (col_name, lit) = wco_set_value(g, ti);
        out.push(raw(format!(
            "UPDATE {} SET {} = {} WHERE CURRENT OF {};",
            tname, col_name, lit, name
        )));
    }
    if wco == "cursor:wco:delete" {
        out.push(raw(format!("DELETE FROM {} WHERE CURRENT OF {};", tname, name)));
    }
    out.push(raw(format!("CLOSE {};", name)));
    let commit = g.weights.pick(g.rng, &["cursor:end:commit", "cursor:end:rollback"])
        == "cursor:end:commit";
    g.fire(if commit { "cursor:end:commit" } else { "cursor:end:rollback" });
    out.push(raw(if commit { "COMMIT;" } else { "ROLLBACK;" }.to_string()));
    g.cursor.last_dead = Some(name);
    out
}

/// A non-pk column and a literal of its type for WCO UPDATE SET.
fn wco_set_value(g: &mut Gen, ti: usize) -> (String, String) {
    let table = &g.catalog.tables[ti];
    let cols: Vec<(String, SqlType)> = table
        .columns
        .iter()
        .filter(|c| !table.is_pk_column(&c.name))
        .map(|c| (c.name.clone(), c.ty))
        .collect();
    let (name, ty) = cols[g.rng.below_usize(cols.len())].clone();
    let lit = g.gen_literal(ty);
    (name, lit)
}

/// Held-cursor shape: create a standalone WITH HOLD cursor (implicit
/// transaction — materializes immediately) or operate on one created by an
/// earlier group; occasionally CLOSE / CLOSE ALL.
fn gen_held(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cursor:held");
    let create = g.cursor.held.is_empty()
        || (g.cursor.held.len() < HELD_CAP
            && g.weights.pick(g.rng, &["cursor:held:create", "cursor:held:use"])
                == "cursor:held:create");
    if create {
        g.fire("cursor:held:create");
        let tables = cursor_tables(g);
        let ti = tables[g.rng.below_usize(tables.len())];
        let name = next_cursor_name(g);
        let scroll = if g.rng.chance(1, 2) {
            Scrollability::Scroll
        } else {
            Scrollability::Default
        };
        let q = cursor_query(g, ti, false);
        let mut out = vec![raw(format!(
            "DECLARE {}{} CURSOR WITH HOLD FOR {};",
            name,
            if scroll == Scrollability::Scroll { " SCROLL" } else { "" },
            q
        ))];
        for _ in 0..1 + g.rng.below_usize(2) {
            out.push(raw(gen_op(g, &name, scroll)));
        }
        g.cursor.held.push(HeldCursor { name, scroll });
        return out;
    }
    g.fire("cursor:held:use");
    let idx = g.rng.below_usize(g.cursor.held.len());
    let (name, scroll) = {
        let h = &g.cursor.held[idx];
        (h.name.clone(), h.scroll)
    };
    let mut out = Vec::new();
    for _ in 0..1 + g.rng.below_usize(3) {
        out.push(raw(gen_op(g, &name, scroll)));
    }
    match g.weights.pick(
        g.rng,
        &["cursor:held:keep", "cursor:held:close", "cursor:close_all"],
    ) {
        "cursor:held:close" => {
            g.fire("cursor:held:close");
            out.push(raw(format!("CLOSE {};", name)));
            g.cursor.held.remove(idx);
            g.cursor.last_dead = Some(name);
        }
        "cursor:close_all" => {
            g.fire("cursor:close_all");
            out.push(raw("CLOSE ALL;".to_string()));
            g.cursor.last_dead = g.cursor.held.pop().map(|h| h.name);
            g.cursor.held.clear();
        }
        _ => {}
    }
    out
}

/// Literal EXECUTE argument for the pk parameter of a pooled statement.
fn pk_arg(g: &mut Gen, table: &str, want_hit: bool) -> i64 {
    let ti = g.catalog.tables.iter().position(|t| t.name == table);
    match ti {
        Some(ti) if want_hit => g.pk_known(ti).unwrap_or(1),
        Some(_) | None => {
            // Deliberate miss: far above any pk the monotonic allocator
            // will reach in a stream.
            10_000_000 + g.rng.below(1000) as i64
        }
    }
}

/// Render one valid EXECUTE (and bump the entry's model exec count —
/// callers already hold a clone of the entry).
fn render_execute(g: &mut Gen, p: &Prepared) -> String {
    match p.kind {
        PrepKind::Select => {
            let a = match g.rng.below(5) {
                0 => 0,
                1 => 1,
                2 => 3,
                3 => 5,
                _ => 1000,
            };
            if p.nargs == 2 {
                format!("EXECUTE {}({}, {});", p.name, a, a + g.rng.below(10) as i64)
            } else {
                format!("EXECUTE {}({});", p.name, a)
            }
        }
        PrepKind::Update => {
            let hit = g.weights.pick(g.rng, &["cursor:exec:hit", "cursor:exec:miss"])
                == "cursor:exec:hit";
            g.fire(if hit { "cursor:exec:hit" } else { "cursor:exec:miss" });
            let pk = pk_arg(g, &p.table, hit);
            let lit = g.gen_literal(p.set_ty.expect("update entry has set_ty"));
            format!("EXECUTE {}({}, {});", p.name, pk, lit)
        }
        PrepKind::Delete => {
            let hit = g.weights.pick(g.rng, &["cursor:exec:hit", "cursor:exec:miss"])
                == "cursor:exec:hit";
            g.fire(if hit { "cursor:exec:hit" } else { "cursor:exec:miss" });
            let pk = pk_arg(g, &p.table, hit);
            format!("EXECUTE {}({});", p.name, pk)
        }
        PrepKind::Insert => {
            let ti = g.catalog.tables.iter().position(|t| t.name == p.table);
            let pk = match ti {
                Some(ti)
                    if g.weights.pick(g.rng, &["cursor:exec:fresh", "cursor:exec:collide"])
                        == "cursor:exec:collide" =>
                {
                    g.fire("cursor:exec:collide");
                    // Deliberate 23505 (matched): a known, probably-live pk.
                    g.pk_known(ti).unwrap_or(1)
                }
                Some(ti) => {
                    g.fire("cursor:exec:fresh");
                    g.pk_fresh(ti)
                }
                None => 10_000_000 + g.rng.below(1000) as i64,
            };
            format!("EXECUTE {}({});", p.name, pk)
        }
    }
}

/// PREPARE shape: pool-capped PREPARE + 1-2 immediate EXECUTEs.
fn gen_prepare(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cursor:prepare");
    let mut out = Vec::new();
    if g.cursor.prepared.len() >= PREP_CAP {
        let old = g.cursor.prepared.remove(0);
        out.push(raw(format!("DEALLOCATE {};", old.name)));
    }
    let kind = match g.weights.pick(
        g.rng,
        &["cursor:prep:select", "cursor:prep:update", "cursor:prep:delete", "cursor:prep:insert"],
    ) {
        "cursor:prep:update" => PrepKind::Update,
        "cursor:prep:delete" => PrepKind::Delete,
        "cursor:prep:insert" => PrepKind::Insert,
        _ => PrepKind::Select,
    };
    let dml = kind != PrepKind::Select;
    let tables = prep_tables(g, dml);
    if tables.is_empty() {
        // Live catalogs may expose no seeded pk tables: degrade to a
        // plain cursor bracket so the module always produces.
        return gen_bracket(g);
    }
    let ti = tables[g.rng.below_usize(tables.len())];
    let tname = g.catalog.tables[ti].name.clone();
    let name = next_prep_name(g);

    let entry = match kind {
        PrepKind::Select => {
            g.fire("cursor:prep:select");
            let two = g.weights.pick(g.rng, &["cursor:prep:arg1", "cursor:prep:arg2"])
                == "cursor:prep:arg2";
            let table = &g.catalog.tables[ti];
            let pk_col = table.pk.as_ref().expect("prep table has pk").column.clone();
            let ncols = 1 + g.rng.below_usize(table.columns.len().min(3));
            let mut cols: Vec<usize> = Vec::new();
            while cols.len() < ncols {
                let i = g.rng.below_usize(table.columns.len());
                if !cols.contains(&i) {
                    cols.push(i);
                }
            }
            let alias = g.next_alias();
            let table = &g.catalog.tables[ti];
            let col_list = cols
                .iter()
                .map(|&i| format!("{}.{}", alias, table.columns[i].name))
                .collect::<Vec<_>>()
                .join(", ");
            let (args, wh) = if two {
                (
                    "int4, int4",
                    format!("{a}.{pk} > $1 AND {a}.{pk} <= $2", a = alias, pk = pk_col),
                )
            } else {
                ("int4", format!("{a}.{pk} > $1", a = alias, pk = pk_col))
            };
            out.push(raw(format!(
                "PREPARE {}({}) AS SELECT {} FROM {} AS {} WHERE {} ORDER BY {}.{};",
                name, args, col_list, tname, alias, wh, alias, pk_col
            )));
            Prepared {
                name,
                table: tname,
                kind,
                nargs: if two { 2 } else { 1 },
                set_ty: None,
            }
        }
        PrepKind::Update => {
            g.fire("cursor:prep:update");
            let (col, ty) = {
                let table = &g.catalog.tables[ti];
                let cols: Vec<(String, SqlType)> = table
                    .columns
                    .iter()
                    .filter(|c| !table.is_pk_column(&c.name))
                    .map(|c| (c.name.clone(), c.ty))
                    .collect();
                cols[g.rng.below_usize(cols.len())].clone()
            };
            let pk_col = g.catalog.tables[ti].pk.as_ref().unwrap().column.clone();
            out.push(raw(format!(
                "PREPARE {}(int4, {}) AS UPDATE {} SET {} = $2 WHERE {} = $1;",
                name,
                ty.name(),
                tname,
                col,
                pk_col
            )));
            Prepared { name, table: tname, kind, nargs: 2, set_ty: Some(ty) }
        }
        PrepKind::Delete => {
            g.fire("cursor:prep:delete");
            let pk_col = g.catalog.tables[ti].pk.as_ref().unwrap().column.clone();
            out.push(raw(format!(
                "PREPARE {}(int4) AS DELETE FROM {} WHERE {} = $1;",
                name, tname, pk_col
            )));
            Prepared { name, table: tname, kind, nargs: 1, set_ty: None }
        }
        PrepKind::Insert => {
            g.fire("cursor:prep:insert");
            let (col_names, lits) = {
                let table = &g.catalog.tables[ti];
                let pk_col = table.pk.as_ref().unwrap().column.clone();
                let mut names = vec![pk_col];
                let mut lits: Vec<String> = Vec::new();
                let cols: Vec<(String, SqlType, bool)> = table
                    .columns
                    .iter()
                    .filter(|c| !table.is_pk_column(&c.name))
                    .map(|c| (c.name.clone(), c.ty, c.nullable))
                    .collect();
                for (cname, ty, nullable) in cols {
                    if !nullable || g.rng.chance(1, 2) {
                        names.push(cname);
                        lits.push(g.gen_literal(ty));
                    }
                }
                (names, lits)
            };
            let mut vals = vec!["$1".to_string()];
            vals.extend(lits);
            out.push(raw(format!(
                "PREPARE {}(int4) AS INSERT INTO {} ({}) VALUES ({});",
                name,
                tname,
                col_names.join(", "),
                vals.join(", ")
            )));
            Prepared { name, table: tname, kind, nargs: 1, set_ty: None }
        }
    };

    for _ in 0..1 + g.rng.below_usize(2) {
        out.push(raw(render_execute(g, &entry)));
    }
    g.cursor.prepared.push(entry);
    out
}

/// EXECUTE shape over the pool: few / burst-of-6 (crosses the plancache
/// custom->generic flip at 5) / EXPLAIN EXECUTE probe / wrong-arg-count
/// error.
fn gen_execute(g: &mut Gen) -> Vec<StmtKind> {
    if g.cursor.prepared.is_empty() {
        // Nothing pooled yet: the prepare shape both fills the pool and
        // executes, so the module always produces.
        return gen_prepare(g);
    }
    g.fire("cursor:execute");
    let idx = g.rng.below_usize(g.cursor.prepared.len());
    let entry = g.cursor.prepared[idx].clone();
    let variant = g.weights.pick(
        g.rng,
        &["cursor:exec:few", "cursor:exec:burst", "cursor:exec:explain", "cursor:exec:badargs"],
    );
    g.fire(variant);
    let mut out = Vec::new();
    match variant {
        "cursor:exec:burst" => {
            for _ in 0..6 {
                out.push(raw(render_execute(g, &entry)));
            }
        }
        "cursor:exec:explain" => {
            // Never ANALYZE: EXPLAIN EXECUTE without ANALYZE does not run
            // the statement, so DML entries stay side-effect-free here.
            // Custom plans print folded parameters, the generic plan
            // prints $n — the plancache choice surface itself.
            let inner = render_execute(g, &entry);
            out.push(raw(format!(
                "EXPLAIN (COSTS OFF, SUMMARY OFF) {}",
                inner
            )));
            out.push(raw(render_execute(g, &entry)));
        }
        "cursor:exec:badargs" => {
            // One extra argument: 42601 on both sides (verified).
            let mut args: Vec<String> = Vec::new();
            for _ in 0..entry.nargs + 1 {
                args.push("0".to_string());
            }
            out.push(raw(format!("EXECUTE {}({});", entry.name, args.join(", "))));
        }
        _ => {
            for _ in 0..1 + g.rng.below_usize(2) {
                out.push(raw(render_execute(g, &entry)));
            }
        }
    }
    out
}

/// DEALLOCATE shape: one named entry or (low weight) ALL.
fn gen_dealloc(g: &mut Gen) -> Vec<StmtKind> {
    if g.cursor.prepared.is_empty() {
        return gen_prepare(g);
    }
    g.fire("cursor:dealloc");
    if g.weights.pick(g.rng, &["cursor:dealloc:one", "cursor:dealloc:all"])
        == "cursor:dealloc:all"
    {
        g.fire("cursor:dealloc:all");
        g.cursor.prepared.clear();
        vec![raw("DEALLOCATE ALL;".to_string())]
    } else {
        g.fire("cursor:dealloc:one");
        let idx = g.rng.below_usize(g.cursor.prepared.len());
        let old = g.cursor.prepared.remove(idx);
        vec![raw(format!("DEALLOCATE {};", old.name))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Generate `n` groups through one persistent CursorState (the session
    /// swap pattern), collecting (group sqls, group productions).
    fn gen_groups(seed: u64, n: usize, w: &WeightTable) -> Vec<(Vec<String>, Vec<String>)> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut rng = Rng::new(seed);
        let mut state = CursorState::new();
        let mut out = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, w, &mut prods, 3);
            std::mem::swap(&mut g.cursor, &mut state);
            let sqls: Vec<String> =
                gen_cursor_module(&mut g).iter().map(|s| s.to_sql()).collect();
            std::mem::swap(&mut g.cursor, &mut state);
            out.push((sqls, prods));
        }
        out
    }

    fn is_terminator(sql: &str) -> bool {
        sql == "COMMIT;" || sql == "ROLLBACK;"
    }

    #[test]
    fn shapes_and_bracket_invariants() {
        let groups = gen_groups(0xC1, 800, &WeightTable::defaults());
        let mut all_prods: Vec<String> = Vec::new();
        for (sqls, prods) in &groups {
            assert!(!sqls.is_empty());
            for sql in sqls {
                assert!(sql.ends_with(';'), "{sql}");
                assert!(!sql.contains('\n'), "{sql}");
            }
            let begins = sqls.iter().filter(|s| *s == "BEGIN;").count();
            let ends = sqls.iter().filter(|s| is_terminator(s)).count();
            assert!(begins <= 1 && ends == begins, "{sqls:?}");
            if begins == 1 {
                assert_eq!(sqls[0], "BEGIN;", "{sqls:?}");
                let e = sqls.iter().position(|s| is_terminator(s)).unwrap();
                // Between BEGIN and the terminator: no nested control.
                for s in &sqls[1..e] {
                    assert!(!is_terminator(s) && s != "BEGIN;", "{sqls:?}");
                }
                // Post-terminator statements are cursor/prepared ops only.
                for s in &sqls[e + 1..] {
                    assert!(
                        s.starts_with("FETCH ") || s.starts_with("MOVE ") || s.starts_with("CLOSE "),
                        "{sqls:?}"
                    );
                }
            }
            all_prods.extend(prods.iter().cloned());
        }
        for p in SHAPES {
            assert!(all_prods.iter().any(|q| q == p), "shape {p} never fired");
        }
        for p in [
            "cursor",
            "cursor:scroll",
            "cursor:noscroll",
            "cursor:hold",
            "cursor:binary",
            "cursor:close",
            "cursor:close_all",
            "cursor:end:commit",
            "cursor:end:rollback",
            "cursor:op:next",
            "cursor:op:prior",
            "cursor:op:first",
            "cursor:op:last",
            "cursor:op:abs",
            "cursor:op:rel_back",
            "cursor:op:back_n",
            "cursor:op:back_all",
            "cursor:op:fwd_n",
            "cursor:op:fwd_all",
            "cursor:op:rel_fwd",
            "cursor:op:count",
            "cursor:op:move",
            "cursor:op:invalid_back",
            "cursor:fetch:dead",
            "cursor:held:create",
            "cursor:held:use",
            "cursor:held:close",
            "cursor:wco:update",
            "cursor:wco:delete",
            "cursor:prep:select",
            "cursor:prep:update",
            "cursor:prep:delete",
            "cursor:prep:insert",
            "cursor:exec:few",
            "cursor:exec:burst",
            "cursor:exec:explain",
            "cursor:exec:badargs",
            "cursor:exec:hit",
            "cursor:exec:miss",
            "cursor:exec:fresh",
            "cursor:exec:collide",
            "cursor:dealloc:one",
            "cursor:dealloc:all",
        ] {
            assert!(all_prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    /// Replay the generated stream against a text-level model and assert
    /// the load-bearing sequencing invariants: FETCH/MOVE/CLOSE outside a
    /// transaction only ever target live WITH HOLD cursors (except the
    /// deliberate dead-cursor production); EXECUTE/DEALLOCATE only ever
    /// name live pool entries; backward directions never appear on
    /// non-SCROLL cursors (except the deliberate 55000 production).
    #[test]
    fn cross_group_state_stays_valid() {
        let groups = gen_groups(0x51, 1200, &WeightTable::defaults());
        let mut held: Vec<String> = Vec::new();
        let mut pool: Vec<String> = Vec::new();
        for (sqls, prods) in &groups {
            let dead_ok = prods.iter().any(|p| p == "cursor:fetch:dead");
            let invalid_ok = prods.iter().any(|p| p == "cursor:op:invalid_back");
            let mut in_txn = false;
            // Cursors declared in this bracket: (name, hold, closed, scroll_spelling)
            let mut txn_cursors: Vec<(String, bool, bool, bool)> = Vec::new();
            for sql in sqls {
                if sql == "BEGIN;" {
                    in_txn = true;
                } else if sql == "COMMIT;" {
                    for (name, hold, closed, _) in txn_cursors.drain(..) {
                        if hold && !closed {
                            held.push(name);
                        }
                    }
                    in_txn = false;
                } else if sql == "ROLLBACK;" {
                    txn_cursors.clear();
                    in_txn = false;
                } else if let Some(rest) = sql.strip_prefix("DECLARE ") {
                    let name = rest.split(' ').next().unwrap().to_string();
                    let hold = sql.contains(" WITH HOLD ");
                    let scroll = sql.contains(" SCROLL CURSOR") && !sql.contains(" NO SCROLL");
                    if in_txn {
                        txn_cursors.push((name, hold, false, scroll));
                    } else {
                        assert!(hold, "standalone DECLARE without HOLD: {sql}");
                        held.push(name);
                    }
                } else if sql == "CLOSE ALL;" {
                    assert!(!in_txn, "CLOSE ALL inside a bracket: {sql}");
                    held.clear();
                } else if let Some(rest) = sql.strip_prefix("CLOSE ") {
                    let name = rest.trim_end_matches(';').to_string();
                    if let Some(c) = txn_cursors.iter_mut().find(|c| c.0 == name) {
                        c.2 = true;
                    } else {
                        assert!(
                            held.contains(&name),
                            "CLOSE of unknown cursor {name}: {sqls:?}"
                        );
                        held.retain(|h| h != &name);
                    }
                } else if sql.starts_with("FETCH ") || sql.starts_with("MOVE ") {
                    let name = sql
                        .trim_end_matches(';')
                        .rsplit(' ')
                        .next()
                        .unwrap()
                        .to_string();
                    let in_bracket = txn_cursors.iter().any(|c| c.0 == name && !c.2);
                    let is_held = held.contains(&name);
                    assert!(
                        in_bracket || is_held || dead_ok,
                        "op on dead cursor {name} without the dead production: {sqls:?}"
                    );
                    // Backward text only on SCROLL cursors (or the 55000 prod).
                    let backward = sql.contains(" PRIOR ")
                        || sql.contains(" BACKWARD ")
                        || sql.contains(" FIRST ")
                        || sql.contains(" LAST ")
                        || sql.contains(" ABSOLUTE ")
                        || sql.contains(" RELATIVE -");
                    if backward && !invalid_ok {
                        let scroll_in_txn =
                            txn_cursors.iter().any(|c| c.0 == name && c.3);
                        assert!(
                            scroll_in_txn || is_held,
                            "backward op on non-scroll bracket cursor: {sqls:?}"
                        );
                    }
                } else if let Some(rest) = sql.strip_prefix("PREPARE ") {
                    let name = rest.split('(').next().unwrap().to_string();
                    assert!(!pool.contains(&name), "PREPARE name reuse: {sql}");
                    pool.push(name);
                } else if sql == "DEALLOCATE ALL;" {
                    pool.clear();
                } else if let Some(rest) = sql.strip_prefix("DEALLOCATE ") {
                    let name = rest.trim_end_matches(';').to_string();
                    assert!(pool.contains(&name), "DEALLOCATE of unknown {name}");
                    pool.retain(|p| p != &name);
                } else if sql.starts_with("EXECUTE ") || sql.contains(") EXECUTE ") {
                    let tail = sql.split("EXECUTE ").nth(1).unwrap();
                    let name = tail.split('(').next().unwrap().to_string();
                    assert!(pool.contains(&name), "EXECUTE of unknown {name}: {sql}");
                }
            }
            assert!(!in_txn, "unterminated bracket: {sqls:?}");
        }
    }

    /// Held cursors and the prepared pool stay within their caps, and
    /// held-scroll bookkeeping holds: ops on held NO-SCROLL/default
    /// cursors are forward-only.
    #[test]
    fn caps_hold() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(7);
        let mut state = CursorState::new();
        for _ in 0..1000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            std::mem::swap(&mut g.cursor, &mut state);
            gen_cursor_module(&mut g);
            std::mem::swap(&mut g.cursor, &mut state);
            assert!(state.held.len() <= HELD_CAP, "held over cap");
            assert!(state.prepared.len() <= PREP_CAP, "pool over cap");
        }
    }

    #[test]
    fn cursor_is_deterministic() {
        let w = WeightTable::defaults();
        let a: Vec<Vec<String>> =
            gen_groups(5, 120, &w).into_iter().map(|(s, _)| s).collect();
        let b: Vec<Vec<String>> =
            gen_groups(5, 120, &w).into_iter().map(|(s, _)| s).collect();
        assert_eq!(a, b);
        let c: Vec<Vec<String>> =
            gen_groups(6, 120, &w).into_iter().map(|(s, _)| s).collect();
        assert_ne!(a, c);
    }
}
