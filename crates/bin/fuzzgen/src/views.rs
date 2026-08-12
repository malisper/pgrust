//! Views and rules statement module (V1): CREATE [OR REPLACE] [TEMP] VIEW,
//! DML through auto-updatable views, WITH [LOCAL|CASCADED] CHECK OPTION,
//! non-updatable views (their write attempts are matched-error surfaces),
//! CREATE MATERIALIZED VIEW + REFRESH [CONCURRENTLY], explicit CREATE RULE
//! on plain tables, and pg_get_viewdef/pg_get_ruledef deparse probes.
//!
//! The structural target is the uncovered rewriter from gap-report-004:
//! `rewriteTargetView` (rank 13 — DML against views). (Rank 2,
//! `raw_expression_tree_walker_impl`, was chartered here on the guess
//! that CREATE RULE fires it; source verification showed its only
//! production callers are parse_cte.c's recursive-CTE checks, so it
//! belongs to a WITH RECURSIVE subq-lane — see
//! docs/fuzzing/findings-v1-views.md.) Also targeted: the rewriteHandler
//! neighbourhood rewriteTargetView gates (`view_query_is_auto_updatable`,
//! `view_cols_are_auto_updatable`, `relation_is_updatable`,
//! `adjust_view_column_set`, `error_view_not_updatable`,
//! `rewriteRuleAction`, `CopyAndAddInvertedQual`) and ruleutils
//! (`pg_get_viewdef_worker`, `pg_get_ruledef_worker`).
//!
//! `ViewsState` (session-persistent, swapped in and out of `Gen` like
//! `DmlState`/`DdlState`/`PartState`) tracks every object this module
//! creates, so statements are valid by construction (zero-42xxx
//! discipline).
//!
//! What is registered where — the load-bearing split:
//!   - each *registered view* sits over its own private base table
//!     (`fz_vb_N`, NOT registered) and is pushed into the effective catalog
//!     as a DML-eligible `Table`. OTHER modules then SELECT from it
//!     (fireRIRrules / ApplyRetrieveRule) and INSERT/UPDATE/DELETE/MERGE
//!     through it (rewriteTargetView) — that cross-module traffic is where
//!     the deep rewriter coverage comes from. One DML surface per base
//!     table: registering both the view and its base (or two views over one
//!     base) would give two independent pk allocators over one key space
//!     and turn the stream into 23505 noise;
//!   - the *base table* carries the state probe window (`DdlEvent`), not
//!     the view: the event resolves to the group's first statement, which
//!     is the base CREATE, and probing the base is strictly stronger — it
//!     sees rows a view's qual hides;
//!   - materialized views and rule tables are probe-registered the same way
//!     (their CREATE leads the group) but never enter the catalog:
//!     a matview is not writable, and a table carrying rules turns
//!     generic DML into noise (ON CONFLICT + rules is an unconditional
//!     error, RETURNING through DO INSTEAD NOTHING likewise). This module
//!     drives their DML itself, with hand-verifiable values;
//!   - nested (view-over-view) and non-updatable views are group-local:
//!     created, exercised, and dropped inside one statement group, so no
//!     cross-group dependency edge can make a later DROP fail.
//!
//! Validity/parity disciplines (every family below was hand-verified
//! byte-identical on pgrust and Homebrew PostgreSQL 18.4 before this
//! module was written — 2026-08-11 probe decks):
//!   - registered views expose EVERY base column as a plain column
//!     reference over a single base relation: that is exactly the
//!     auto-updatable shape, so generic DML through them succeeds instead
//!     of piling up matched "cannot insert into view" errors;
//!   - every base column except pk is nullable, so column-subset INSERTs
//!     (the dml module's, and this module's two-column nested-view writes)
//!     can never 23502;
//!   - a registered view's qual is either always-true (`pk >= 0`) or
//!     *hiding but unenforced* (`k_int IS NOT NULL AND k_int < 1000` with
//!     no WITH CHECK OPTION): cross-module DML can then never trip a check
//!     option. WITH CHECK OPTION on a registered view is only ever paired
//!     with the always-true qual;
//!   - the deliberate check-option violations come from the group-local
//!     nested views, whose rows this module writes itself: a LOCAL nested
//!     view accepts a row failing the parent qual, a CASCADED one rejects
//!     it (44000 on both engines). That pair is the LOCAL/CASCADED
//!     discriminator, and it only bites where the parent qual is the
//!     hiding one;
//!   - module-local writes allocate pks from a high block (1_000_000+) so
//!     they can never collide with the DmlState allocator that owns the
//!     registered view's key space (it starts just above the seed rows and
//!     advances one row at a time);
//!   - rule actions are deterministic single commands (or a two-command
//!     DO ALSO list) over small integers: no expression that can 22003,
//!     no ordering-sensitive read. Rules fire in rulename order, which is
//!     the same on both engines;
//!   - rule-table reads sort by every output column (`ORDER BY a, b` on
//!     the log), so any residual tie is between identical rows;
//!   - a REFRESH MATERIALIZED VIEW CONCURRENTLY is only ever emitted for a
//!     matview that already carries its unique index and already holds
//!     data (Postgres rejects both otherwise).

use crate::catalog::{Catalog, Column, Pk, SqlType, Table, ALL_TYPES};
use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

/// Live-population caps (each object costs statements to maintain).
const MAX_LIVE_VIEWS: usize = 3;
const MAX_LIVE_MATVIEWS: usize = 2;
const MAX_LIVE_RULETABS: usize = 2;
/// Rules per rule table.
const MAX_RULES_PER_TABLE: usize = 4;
/// Pk block for this module's own writes: far above anything the DmlState
/// allocator can reach in a session, so module-local rows never collide
/// with cross-module DML through the same view.
const LOCAL_PK_BASE: i64 = 1_000_000;

/// Short, quote-free text values for module-local writes.
const TEXT_VOCAB: &[&str] = &["a", "b", "c", "x", "y", ""];

/// A registered view's qual: what the view hides, and whether a WITH CHECK
/// OPTION may be attached to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewQual {
    /// No WHERE at all.
    None,
    /// `pk >= 0` — true for every row anyone can write (pk allocators are
    /// monotonic from 1). Safe to carry a check option.
    PkNonNeg,
    /// `k_int IS NOT NULL AND k_int < 1000` — really hides rows. NEVER
    /// carries a check option: cross-module DML writes arbitrary k_int.
    HideKint,
}

impl ViewQual {
    fn sql(self) -> Option<&'static str> {
        match self {
            ViewQual::None => None,
            ViewQual::PkNonNeg => Some("pk >= 0"),
            ViewQual::HideKint => Some("k_int IS NOT NULL AND k_int < 1000"),
        }
    }

    /// May a WITH CHECK OPTION ride on this qual without turning ordinary
    /// cross-module DML into check-option errors?
    fn wco_safe(self) -> bool {
        !matches!(self, ViewQual::HideKint)
    }

    fn prod(self) -> &'static str {
        match self {
            ViewQual::None => "views:qual:none",
            ViewQual::PkNonNeg => "views:qual:pk",
            ViewQual::HideKint => "views:qual:hide",
        }
    }
}

/// One registered view over its private base table.
#[derive(Clone, Debug)]
pub struct ViewObj {
    /// The view (the catalog-registered DML surface).
    pub table: Table,
    /// The private base table (probe-registered, never in the catalog).
    pub base: String,
    /// Base column definitions, in order (the view exposes all of them).
    pub cols: Vec<Column>,
    pub qual: ViewQual,
    /// WITH [LOCAL|CASCADED] CHECK OPTION, when present.
    pub wco: Option<&'static str>,
    pub temp: bool,
    pub live: bool,
    /// Next group-local nested-view suffix.
    next_nested: u32,
}

#[derive(Clone, Debug)]
pub struct MatViewObj {
    pub name: String,
    /// Source relation (a fixture table or a registered view).
    pub source: String,
    /// A unique index exists (REFRESH CONCURRENTLY prerequisite).
    pub has_unique_index: bool,
    /// The matview holds data (WITH NO DATA until first plain REFRESH).
    pub populated: bool,
    pub live: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuleKind {
    /// ON INSERT DO ALSO INSERT INTO log VALUES (NEW.pk + 1, lower(...)).
    InsertAlsoLog,
    /// ON INSERT DO ALSO (two INSERTs) — multi-command action list.
    InsertAlsoMulti,
    /// ON INSERT WHERE NEW.pk % 2 = 0 DO ALSO INSERT ... SELECT (a rule
    /// action carrying a subquery over the event relation).
    InsertAlsoSelect,
    /// ON INSERT DO INSTEAD INSERT INTO log ... (redirect).
    InsertInsteadRedirect,
    /// ON INSERT DO INSTEAD NOTHING.
    InsertInsteadNothing,
    /// ON UPDATE WHERE NEW.k_int > OLD.k_int DO ALSO INSERT INTO log ...
    UpdateCondAlso,
    /// ON UPDATE WHERE OLD.pk = <k> DO INSTEAD NOTHING (conditional
    /// INSTEAD — the shape that makes a view non-auto-updatable).
    UpdateCondInsteadNothing,
    /// ON DELETE DO INSTEAD NOTHING.
    DeleteInsteadNothing,
    /// ON DELETE DO ALSO NOTHING.
    DeleteAlsoNothing,
}

impl RuleKind {
    fn prod(self) -> &'static str {
        match self {
            RuleKind::InsertAlsoLog => "views:rule:ins_also",
            RuleKind::InsertAlsoMulti => "views:rule:ins_also_multi",
            RuleKind::InsertAlsoSelect => "views:rule:ins_also_select",
            RuleKind::InsertInsteadRedirect => "views:rule:ins_instead_redirect",
            RuleKind::InsertInsteadNothing => "views:rule:ins_instead_nothing",
            RuleKind::UpdateCondAlso => "views:rule:upd_cond_also",
            RuleKind::UpdateCondInsteadNothing => "views:rule:upd_cond_instead",
            RuleKind::DeleteInsteadNothing => "views:rule:del_instead_nothing",
            RuleKind::DeleteAlsoNothing => "views:rule:del_also_nothing",
        }
    }

    /// An unconditional DO INSTEAD on INSERT: no RETURNING may be used on
    /// the table afterwards, and INSERTs stop adding rows.
    fn insert_instead(self) -> bool {
        matches!(self, RuleKind::InsertInsteadRedirect | RuleKind::InsertInsteadNothing)
    }
}

#[derive(Clone, Debug)]
pub struct RuleObj {
    pub name: String,
    pub kind: RuleKind,
    pub live: bool,
}

/// A plain table carrying explicit rules, plus its log sink.
#[derive(Clone, Debug)]
pub struct RuleTab {
    pub table: String,
    pub log: String,
    pub rules: Vec<RuleObj>,
    /// Monotonic pk allocator for this module's writes to the table.
    next_pk: i64,
    pub live: bool,
}

impl RuleTab {
    fn live_rules(&self) -> Vec<usize> {
        self.rules
            .iter()
            .enumerate()
            .filter(|(_, r)| r.live)
            .map(|(i, _)| i)
            .collect()
    }

    /// Any live unconditional DO INSTEAD rule on INSERT.
    fn has_insert_instead(&self) -> bool {
        self.rules.iter().any(|r| r.live && r.kind.insert_instead())
    }
}

/// Session-persistent views/rules catalog model.
#[derive(Clone, Debug, Default)]
pub struct ViewsState {
    pub views: Vec<ViewObj>,
    pub matviews: Vec<MatViewObj>,
    pub ruletabs: Vec<RuleTab>,
    next_view: u32,
    next_mv: u32,
    next_ruletab: u32,
    next_rule: u32,
    next_col: u32,
    next_local_pk: i64,
    events: Vec<DdlEvent>,
}

impl ViewsState {
    pub fn new() -> ViewsState {
        ViewsState::default()
    }

    /// Base catalog + live registered views. The session loop feeds this to
    /// every later group: cross-module SELECTs engage the RIR rewriter and
    /// cross-module DML engages rewriteTargetView.
    pub fn extend_catalog(&self, base: &Catalog) -> Catalog {
        let mut cat = base.clone();
        for v in &self.views {
            if v.live {
                cat.tables.push(v.table.clone());
            }
        }
        cat
    }

    /// Drain pending create/drop events (the session loop resolves them
    /// into probe windows, exactly like DdlState's).
    pub fn take_events(&mut self) -> Vec<DdlEvent> {
        std::mem::take(&mut self.events)
    }

    fn live_views(&self) -> Vec<usize> {
        self.views
            .iter()
            .enumerate()
            .filter(|(_, v)| v.live)
            .map(|(i, _)| i)
            .collect()
    }

    fn live_matviews(&self) -> Vec<usize> {
        self.matviews
            .iter()
            .enumerate()
            .filter(|(_, m)| m.live)
            .map(|(i, _)| i)
            .collect()
    }

    fn live_ruletabs(&self) -> Vec<usize> {
        self.ruletabs
            .iter()
            .enumerate()
            .filter(|(_, r)| r.live)
            .map(|(i, _)| i)
            .collect()
    }

    /// Fresh module-local pk, high above the DmlState allocator's reach.
    fn local_pk(&mut self) -> i64 {
        self.next_local_pk += 1;
        LOCAL_PK_BASE + self.next_local_pk
    }
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_views_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("views");
    let action = g.weights.pick(
        g.rng,
        &[
            "views:create",
            "views:replace",
            "views:drop",
            "views:nested",
            "views:readonly",
            "views:matview",
            "views:rule",
            "views:deparse",
            "views:fieldsel",
        ],
    );
    // The chosen arm fires its own production (part.rs convention): a
    // capped or fallback arm records what actually ran, not what was drawn.
    match action {
        "views:create" => gen_create_view(g),
        "views:replace" => gen_replace_view(g),
        "views:drop" => gen_drop_view(g),
        "views:nested" => gen_nested_view(g),
        "views:readonly" => gen_readonly_view(g),
        "views:matview" => gen_matview(g),
        "views:rule" => gen_rule_group(g),
        "views:fieldsel" => gen_fieldsel(g),
        _ => gen_deparse(g),
    }
}

/// G2 deparse-tail group: a group-local view whose SELECT list references
/// FIELDS of a whole-row/RECORD Var (composite-returning subqueries,
/// RECORD-returning functions with AS-typed columns, whole-row `.*`
/// expansion), then pg_get_viewdef probes — the shapes that force
/// ruleutils' `get_name_for_var_field` (gap-007 rank 12) when the deparser
/// resolves the field name through the range table. Group-local like the
/// readonly views: created, deparsed, read, dropped in one group, so no
/// cross-group dependency edge exists. Every shape hand-verified
/// byte-identical on both engines (G2 deck 3, 2026-08-11).
fn gen_fieldsel(g: &mut Gen) -> Vec<StmtKind> {
    let cands = safe_source_tables(g);
    if cands.is_empty() {
        g.fire("views:fallback:create");
        return gen_create_view(g);
    }
    let src = cands[g.rng.below_usize(cands.len())].clone();
    let name = format!("fz_vf_{}", g.views.next_view);
    g.views.next_view += 1;
    let shape = g.weights.pick(
        g.rng,
        &[
            "views:fs:wholerow",
            "views:fs:recfn",
            "views:fs:nestedrec",
            "views:fs:anonrec",
            "views:fs:star",
        ],
    );
    g.fire(shape);
    let (create, read) = match shape {
        // Whole-row Var of a real table through a subquery; the view's
        // SELECT list projects its fields.
        "views:fs:wholerow" => (
            format!(
                "CREATE VIEW {n} (a, b) AS SELECT (t).k_int, (t).k_text FROM (SELECT s0 AS t \
                 FROM {src} s0) s;",
                n = name
            ),
            format!("SELECT * FROM {} ORDER BY a NULLS LAST, b NULLS LAST;", name),
        ),
        // RECORD-returning function with AS (a, b) column definitions.
        "views:fs:recfn" => (
            format!(
                "CREATE VIEW {n} AS SELECT r.a, r.b FROM \
                 jsonb_to_record('{{\"a\":1,\"b\":\"x\"}}'::jsonb) AS r(a int, b text);",
                n = name
            ),
            format!("SELECT * FROM {};", name),
        ),
        // Record field resolved through a second subquery level.
        "views:fs:nestedrec" => (
            format!(
                "CREATE VIEW {n} (aa) AS SELECT (s.r).a FROM (SELECT r FROM \
                 jsonb_to_record('{{\"a\":7}}'::jsonb) AS r(a int)) s;",
                n = name
            ),
            format!("SELECT * FROM {};", name),
        ),
        // Whole-row Var of an anonymous subquery (RECORD blessed at exec).
        "views:fs:anonrec" => (
            format!(
                "CREATE VIEW {n} (c1, c2) AS SELECT (v.r).f1, (v.r).f2 FROM (SELECT r FROM \
                 (SELECT 1 AS f1, 'z'::text AS f2) r) v;",
                n = name
            ),
            format!("SELECT * FROM {};", name),
        ),
        // Whole-row `.*` expansion through a subquery (every base column
        // becomes a field selection).
        _ => (
            format!(
                "CREATE VIEW {n} AS SELECT (w).* FROM (SELECT s0 AS w FROM {src} s0) s;",
                n = name
            ),
            format!("SELECT pk, k_int, k_text FROM {} ORDER BY pk;", name),
        ),
    };
    let mut stmts = vec![StmtKind::Raw(create)];
    // The deparse probes are the point: plain and pretty forms both walk
    // get_name_for_var_field for every field reference.
    stmts.push(StmtKind::Raw(format!(
        "SELECT pg_get_viewdef('{}'::regclass);",
        name
    )));
    if g.rng.chance(1, 2) {
        g.fire("views:fs:pretty");
        stmts.push(StmtKind::Raw(format!(
            "SELECT pg_get_viewdef('{}'::regclass, true);",
            name
        )));
    }
    stmts.push(StmtKind::Raw(read));
    stmts.push(StmtKind::Raw(format!("DROP VIEW {};", name)));
    stmts
}

/// Relations this module may build a permanent view or matview over: those
/// carrying the protected trio {pk, k_int, k_text}, owned by something that
/// will not drop them out from under a dependent object, and permanent.
///   - ddl tables (`fz_ddl_`) and partitioned parents (`fz_part_`) are
///     excluded: their modules drop them with a plain DROP TABLE, which
///     would fail (2BP01) on a dependent matview. Fixture tables are never
///     dropped, and this module's own registered views drop their dependent
///     matviews first;
///   - TEMP registered views are excluded: a materialized view may not read
///     a temporary relation at all (0A000), and the failed CREATE would
///     leave every later reference to the matview a 42P01.
fn safe_source_tables(g: &Gen) -> Vec<String> {
    g.catalog
        .tables
        .iter()
        .filter(|t| {
            !t.name.starts_with("fz_ddl_")
                && !t.name.starts_with("fz_part_")
                && !g.views.views.iter().any(|v| v.temp && v.table.name == t.name)
                && t.pk.as_ref().is_some_and(|p| p.column == "pk")
                && ["k_int", "k_text"].iter().all(|k| t.columns.iter().any(|c| &c.name == k))
        })
        .map(|t| t.name.clone())
        .collect()
}

/// Live registered views a permanent object may depend on (never TEMP).
fn permanent_views(g: &Gen) -> Vec<usize> {
    g.views
        .views
        .iter()
        .enumerate()
        .filter(|(_, v)| v.live && !v.temp)
        .map(|(i, _)| i)
        .collect()
}

// -------------------------------------------------- registered views ----

/// CREATE the private base table, seed it, then CREATE the view over it and
/// register the view in the catalog. The base CREATE leads the group, so
/// the probe window this pushes resolves to a statement after which the
/// base really exists.
fn gen_create_view(g: &mut Gen) -> Vec<StmtKind> {
    if g.views.live_views().len() >= MAX_LIVE_VIEWS {
        g.fire("views:cap:views");
        return gen_drop_view(g);
    }
    g.fire("views:create");
    let n = g.views.next_view;
    g.views.next_view += 1;
    let base = format!("fz_vb_{}", n);
    let name = format!("fz_vw_{}", n);

    // Column set: the protected trio plus 1-3 palette columns. Everything
    // but pk is nullable (column-subset INSERTs must never 23502).
    let mut cols = vec![
        Column { name: "pk".to_string(), ty: SqlType::Int4, nullable: false, ddl_type: None },
        Column { name: "k_int".to_string(), ty: SqlType::Int4, nullable: true, ddl_type: None },
        Column { name: "k_text".to_string(), ty: SqlType::Text, nullable: true, ddl_type: None },
    ];
    let mut defs = vec![
        "pk int4 PRIMARY KEY".to_string(),
        "k_int int4".to_string(),
        "k_text text".to_string(),
    ];
    let ncols = 1 + g.rng.below_usize(3);
    for _ in 0..ncols {
        let cname = format!("vc{}", g.views.next_col);
        g.views.next_col += 1;
        let ty = ALL_TYPES[g.rng.below_usize(ALL_TYPES.len())];
        defs.push(format!("{} {}", cname, ty.name()));
        cols.push(Column { name: cname, ty, nullable: true, ddl_type: None });
    }

    let mut stmts =
        vec![StmtKind::Raw(format!("CREATE TABLE {} ({});", base, defs.join(", ")))];

    // Seed rows: pks dense from 1, so the DmlState allocator (seeded_max+1)
    // starts exactly above them.
    let nrows = 2 + g.rng.below_usize(3);
    for i in 1..=nrows as i64 {
        let mut vals: Vec<String> = Vec::with_capacity(cols.len());
        vals.push(i.to_string());
        vals.push(if g.rng.chance(1, 5) {
            "NULL".to_string()
        } else {
            g.rng.range_i64(-20, 20).to_string()
        });
        vals.push(if g.rng.chance(1, 5) {
            "NULL".to_string()
        } else {
            format!("'{}'", TEXT_VOCAB[g.rng.below_usize(TEXT_VOCAB.len())])
        });
        for c in cols.iter().skip(3) {
            vals.push(if g.rng.chance(1, 4) {
                "NULL".to_string()
            } else {
                g.gen_literal(c.ty)
            });
        }
        stmts.push(StmtKind::Raw(format!(
            "INSERT INTO {} VALUES ({});",
            base,
            vals.join(", ")
        )));
    }

    let qual = match g.weights.pick(
        g.rng,
        &["views:qual:none", "views:qual:pk", "views:qual:hide"],
    ) {
        "views:qual:pk" => ViewQual::PkNonNeg,
        "views:qual:hide" => ViewQual::HideKint,
        _ => ViewQual::None,
    };
    g.fire(qual.prod());
    // A check option only ever rides on a qual no ordinary write can fail.
    let wco = if qual.wco_safe()
        && g.weights.pick(g.rng, &["views:wco:on", "views:wco:none"]) == "views:wco:on"
    {
        let form = g.weights.pick(g.rng, &["views:wco:local", "views:wco:cascaded"]);
        g.fire(form);
        Some(if form == "views:wco:local" { "LOCAL" } else { "CASCADED" })
    } else {
        None
    };
    let temp = g.weights.pick(g.rng, &["views:temp", "views:plain"]) == "views:temp";
    if temp {
        g.fire("views:temp");
    }
    stmts.push(StmtKind::Raw(create_view_sql(
        &name,
        &cols,
        &base,
        qual,
        wco,
        temp,
        false,
    )));

    let table = Table {
        name: name.clone(),
        columns: cols.clone(),
        at_most_one_row: false,
        pk: Some(Pk { column: "pk".to_string(), seeded_max: nrows as i64 }),
        unique_key: Some("pk".to_string()),
        // The base pk is a real PRIMARY KEY and the view is a plain
        // projection of it, so ON CONFLICT (pk) and deliberate collisions
        // through the view are legal (hand-verified on both engines).
        pk_unique: true,
    };
    g.views.views.push(ViewObj {
        table,
        base: base.clone(),
        cols,
        qual,
        wco,
        temp,
        live: true,
        next_nested: 0,
    });
    // The BASE carries the probe window (it leads the group and sees rows
    // the view's qual hides).
    g.views.events.push(DdlEvent {
        table: base,
        pk: "pk".to_string(),
        kind: DdlEventKind::Created,
    });
    stmts
}

/// `CREATE [OR REPLACE] [TEMP] VIEW name (cols) AS SELECT cols FROM src
/// [WHERE qual] [WITH ... CHECK OPTION];`
fn create_view_sql(
    name: &str,
    cols: &[Column],
    src: &str,
    qual: ViewQual,
    wco: Option<&'static str>,
    temp: bool,
    replace: bool,
) -> String {
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    let mut out = format!(
        "CREATE {}{}VIEW {} AS SELECT {} FROM {}",
        if replace { "OR REPLACE " } else { "" },
        if temp { "TEMP " } else { "" },
        name,
        names.join(", "),
        src
    );
    if let Some(q) = qual.sql() {
        out.push_str(" WHERE ");
        out.push_str(q);
    }
    if let Some(w) = wco {
        out.push_str(&format!(" WITH {} CHECK OPTION", w));
    }
    out.push(';');
    out
}

/// CREATE OR REPLACE VIEW over a live registered view: same column list and
/// base (a replacement may not change either), new qual/check option. The
/// visible row set changes; stored state does not.
fn gen_replace_view(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.views.live_views();
    if live.is_empty() {
        g.fire("views:fallback:create");
        return gen_create_view(g);
    }
    g.fire("views:replace");
    let vi = live[g.rng.below_usize(live.len())];
    let qual = match g.weights.pick(
        g.rng,
        &["views:qual:none", "views:qual:pk", "views:qual:hide"],
    ) {
        "views:qual:pk" => ViewQual::PkNonNeg,
        "views:qual:hide" => ViewQual::HideKint,
        _ => ViewQual::None,
    };
    g.fire(qual.prod());
    let wco = if qual.wco_safe()
        && g.weights.pick(g.rng, &["views:wco:on", "views:wco:none"]) == "views:wco:on"
    {
        let form = g.weights.pick(g.rng, &["views:wco:local", "views:wco:cascaded"]);
        g.fire(form);
        Some(if form == "views:wco:local" { "LOCAL" } else { "CASCADED" })
    } else {
        None
    };
    let v = &mut g.views.views[vi];
    v.qual = qual;
    v.wco = wco;
    let (name, cols, base, temp) = (v.table.name.clone(), v.cols.clone(), v.base.clone(), v.temp);
    let sql = create_view_sql(&name, &cols, &base, qual, wco, temp, true);
    let read = format!("SELECT * FROM {} ORDER BY pk;", name);
    vec![StmtKind::Raw(sql), StmtKind::Raw(read)]
}

/// DROP the view, then its private base table. The DROP VIEW leads the
/// group, so the probe window closes one statement before the base really
/// goes away (conservative by construction).
fn gen_drop_view(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.views.live_views();
    if live.is_empty() {
        // Nothing to drop: create instead (an empty population is below the
        // cap, so this never recurses back).
        g.fire("views:fallback:create");
        return gen_create_view(g);
    }
    g.fire("views:drop");
    let vi = live[g.rng.below_usize(live.len())];
    g.views.views[vi].live = false;
    let name = g.views.views[vi].table.name.clone();
    let base = g.views.views[vi].base.clone();
    // Any matview reading this view dies with it (DROP VIEW would fail on
    // the dependency otherwise), so drop the matview first.
    let mut stmts: Vec<StmtKind> = Vec::new();
    let dependents: Vec<usize> = g
        .views
        .matviews
        .iter()
        .enumerate()
        .filter(|(_, m)| m.live && m.source == name)
        .map(|(i, _)| i)
        .collect();
    for mi in dependents {
        g.views.matviews[mi].live = false;
        let mv = g.views.matviews[mi].name.clone();
        g.views.events.push(DdlEvent {
            table: mv.clone(),
            pk: "pk".to_string(),
            kind: DdlEventKind::Dropped,
        });
        stmts.push(StmtKind::Raw(format!("DROP MATERIALIZED VIEW {};", mv)));
    }
    g.views.events.push(DdlEvent {
        table: base.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    stmts.push(StmtKind::Raw(format!("DROP VIEW {};", name)));
    stmts.push(StmtKind::Raw(format!("DROP TABLE {};", base)));
    stmts
}

// ------------------------------------------------------ nested views ----

/// Group-local view-over-view carrying a check option, exercised with three
/// module-owned writes and dropped in the same group:
///   - a row satisfying every qual (always succeeds);
///   - a row failing the nested view's own qual (44000 under either check
///     option);
///   - a row failing only the PARENT's qual: accepted under LOCAL, rejected
///     under CASCADED. That pair is the LOCAL/CASCADED discriminator, and
///     it only bites when the parent view carries the hiding qual.
fn gen_nested_view(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.views.live_views();
    if live.is_empty() {
        g.fire("views:fallback:create");
        return gen_create_view(g);
    }
    g.fire("views:nested");
    let vi = live[g.rng.below_usize(live.len())];
    let parent = g.views.views[vi].table.name.clone();
    let cols = g.views.views[vi].cols.clone();
    let nested = format!("{}_n{}", parent, g.views.views[vi].next_nested);
    g.views.views[vi].next_nested += 1;
    let form = g.weights.pick(g.rng, &["views:wco:local", "views:wco:cascaded"]);
    g.fire(form);
    let wco = if form == "views:wco:local" { "LOCAL" } else { "CASCADED" };

    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    let mut stmts = vec![StmtKind::Raw(format!(
        "CREATE VIEW {} AS SELECT {} FROM {} WHERE k_int IS NULL OR k_int < 100 WITH {} CHECK \
         OPTION;",
        nested,
        names.join(", "),
        parent,
        wco
    ))];
    // Satisfies every qual: k_int small and non-NULL, pk positive.
    let pk_ok = g.views.local_pk();
    stmts.push(StmtKind::Raw(format!(
        "INSERT INTO {} (pk, k_int) VALUES ({}, {});",
        nested,
        pk_ok,
        g.rng.range_i64(0, 20)
    )));
    // Fails the nested view's own qual under either check option.
    let pk_bad = g.views.local_pk();
    stmts.push(StmtKind::Raw(format!(
        "INSERT INTO {} (pk, k_int) VALUES ({}, {});",
        nested,
        pk_bad,
        200 + g.rng.range_i64(0, 100)
    )));
    // Fails only the parent qual (k_int NULL passes the nested qual's NULL
    // arm; the parent's hiding qual rejects it) — the LOCAL/CASCADED split.
    let pk_parent = g.views.local_pk();
    stmts.push(StmtKind::Raw(format!(
        "INSERT INTO {} (pk, k_int) VALUES ({}, NULL);",
        nested, pk_parent
    )));
    if g.rng.chance(1, 2) {
        g.fire("views:nested:update");
        stmts.push(StmtKind::Raw(format!(
            "UPDATE {} SET k_int = {} WHERE pk = {};",
            nested,
            300 + g.rng.range_i64(0, 100),
            pk_ok
        )));
    }
    if g.rng.chance(1, 2) {
        g.fire("views:nested:delete");
        stmts.push(StmtKind::Raw(format!(
            "DELETE FROM {} WHERE pk = {};",
            nested, pk_ok
        )));
    }
    stmts.push(StmtKind::Raw(format!(
        "SELECT * FROM {} ORDER BY pk;",
        nested
    )));
    if g.rng.chance(1, 3) {
        g.fire("views:nested:deparse");
        stmts.push(StmtKind::Raw(format!(
            "SELECT pg_get_viewdef('{}'::regclass, true);",
            nested
        )));
    }
    stmts.push(StmtKind::Raw(format!("DROP VIEW {};", nested)));
    stmts
}

// ------------------------------------------------- non-updatable views ----

/// Group-local view that is deliberately NOT auto-updatable (aggregate,
/// join, DISTINCT, or LIMIT), read once and then written to on purpose: the
/// write is a matched-error surface exercising the auto-updatability
/// analysis and its error paths (view_query_is_auto_updatable,
/// error_view_not_updatable) rather than the rewrite itself.
fn gen_readonly_view(g: &mut Gen) -> Vec<StmtKind> {
    if safe_source_tables(g).is_empty() {
        // Live-catalog mode: nothing carries the protected trio yet.
        g.fire("views:fallback:create");
        return gen_create_view(g);
    }
    g.fire("views:readonly");
    // Source: a registered view (a view over a view) or the private base of
    // one; a fixture table when nothing is live yet.
    let live = permanent_views(g);
    let src = if !live.is_empty() && g.rng.chance(1, 2) {
        // This module's own base table (group-local view, so nothing can
        // drop it in between).
        g.views.views[live[g.rng.below_usize(live.len())]].base.clone()
    } else {
        let cands = safe_source_tables(g);
        cands[g.rng.below_usize(cands.len())].clone()
    };
    let name = format!("fz_vro_{}", g.views.next_view);
    g.views.next_view += 1;
    let shape = g.weights.pick(
        g.rng,
        &[
            "views:ro:agg",
            "views:ro:join",
            "views:ro:distinct",
            "views:ro:limit",
        ],
    );
    g.fire(shape);
    let (create, read, write) = match shape {
        "views:ro:join" => (
            format!(
                "CREATE VIEW {} (pk, kt) AS SELECT a.pk, b.k_text FROM {} a JOIN {} b ON \
                 a.k_int = b.k_int;",
                name, src, src
            ),
            // pk is not unique across the join; ordering by every output
            // column leaves ties only between identical rows.
            format!("SELECT * FROM {} ORDER BY pk, kt;", name),
            format!("UPDATE {} SET kt = 'x' WHERE pk >= 0;", name),
        ),
        "views:ro:distinct" => (
            format!("CREATE VIEW {} (k) AS SELECT DISTINCT k_int FROM {};", name, src),
            format!("SELECT * FROM {} ORDER BY k NULLS LAST;", name),
            format!("INSERT INTO {} (k) VALUES (1);", name),
        ),
        "views:ro:limit" => (
            format!(
                "CREATE VIEW {} (pk, k) AS SELECT pk, k_int FROM {} ORDER BY pk LIMIT 3;",
                name, src
            ),
            format!("SELECT * FROM {} ORDER BY pk;", name),
            format!("DELETE FROM {} WHERE pk >= 0;", name),
        ),
        _ => (
            format!(
                "CREATE VIEW {} (k, cnt, mn) AS SELECT k_int, count(*), min(pk) FROM {} GROUP \
                 BY k_int;",
                name, src
            ),
            format!("SELECT * FROM {} ORDER BY k NULLS LAST;", name),
            format!("INSERT INTO {} (k, cnt, mn) VALUES (1, 1, 1);", name),
        ),
    };
    let mut stmts = vec![StmtKind::Raw(create), StmtKind::Raw(read)];
    if g.weights.pick(g.rng, &["views:ro:write", "views:ro:read_only"]) == "views:ro:write" {
        g.fire("views:ro:write");
        stmts.push(StmtKind::Raw(write));
    }
    if g.rng.chance(1, 3) {
        g.fire("views:ro:deparse");
        stmts.push(StmtKind::Raw(format!(
            "SELECT pg_get_viewdef('{}'::regclass, true);",
            name
        )));
    }
    stmts.push(StmtKind::Raw(format!("DROP VIEW {};", name)));
    stmts
}

// ------------------------------------------------ materialized views ----

/// Create / refresh / drop a materialized view. Creation leads the group
/// (the probe window resolves to it) and always installs the unique index
/// REFRESH CONCURRENTLY needs.
fn gen_matview(g: &mut Gen) -> Vec<StmtKind> {
    if g.views.live_matviews().is_empty() && safe_source_tables(g).is_empty() {
        g.fire("views:fallback:create");
        return gen_create_view(g);
    }
    g.fire("views:matview");
    let live = g.views.live_matviews();
    let mut action = g
        .weights
        .pick(g.rng, &["views:mv:create", "views:mv:refresh", "views:mv:drop"]);
    if live.is_empty() {
        action = "views:mv:create";
    } else if action == "views:mv:create" && live.len() >= MAX_LIVE_MATVIEWS {
        g.fire("views:cap:matviews");
        action = "views:mv:refresh";
    }
    match action {
        "views:mv:create" => {
            g.fire("views:mv:create");
            let name = format!("fz_mv_{}", g.views.next_mv);
            g.views.next_mv += 1;
            // Source: a registered view (matview over a view) or a fixture
            // table. Both expose pk/k_int/k_text with a unique pk.
            let vlive = permanent_views(g);
            let source = if !vlive.is_empty() && g.rng.chance(1, 2) {
                g.fire("views:mv:src:view");
                g.views.views[vlive[g.rng.below_usize(vlive.len())]].table.name.clone()
            } else {
                g.fire("views:mv:src:table");
                let cands = safe_source_tables(g);
                cands[g.rng.below_usize(cands.len())].clone()
            };
            let no_data = g.weights.pick(g.rng, &["views:mv:nodata", "views:mv:withdata"])
                == "views:mv:nodata";
            if no_data {
                g.fire("views:mv:nodata");
            }
            let mut stmts = vec![StmtKind::Raw(format!(
                "CREATE MATERIALIZED VIEW {} AS SELECT pk, k_int, k_text FROM {}{};",
                name,
                source,
                if no_data { " WITH NO DATA" } else { "" }
            ))];
            stmts.push(StmtKind::Raw(format!(
                "CREATE UNIQUE INDEX {}_ux ON {} (pk);",
                name, name
            )));
            if no_data {
                // Unpopulated matviews cannot be read or refreshed
                // concurrently; a plain REFRESH fixes both.
                stmts.push(StmtKind::Raw(format!("REFRESH MATERIALIZED VIEW {};", name)));
            }
            stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", name)));
            g.views.matviews.push(MatViewObj {
                name: name.clone(),
                source,
                has_unique_index: true,
                populated: true,
                live: true,
            });
            g.views.events.push(DdlEvent {
                table: name,
                pk: "pk".to_string(),
                kind: DdlEventKind::Created,
            });
            stmts
        }
        "views:mv:drop" => {
            g.fire("views:mv:drop");
            let mi = live[g.rng.below_usize(live.len())];
            g.views.matviews[mi].live = false;
            let name = g.views.matviews[mi].name.clone();
            g.views.events.push(DdlEvent {
                table: name.clone(),
                pk: "pk".to_string(),
                kind: DdlEventKind::Dropped,
            });
            vec![StmtKind::Raw(format!("DROP MATERIALIZED VIEW {};", name))]
        }
        _ => {
            g.fire("views:mv:refresh");
            let mi = live[g.rng.below_usize(live.len())];
            let name = g.views.matviews[mi].name.clone();
            let ok_concurrent =
                g.views.matviews[mi].has_unique_index && g.views.matviews[mi].populated;
            let concurrent = ok_concurrent
                && g.weights.pick(g.rng, &["views:mv:concurrent", "views:mv:plain"])
                    == "views:mv:concurrent";
            if concurrent {
                g.fire("views:mv:concurrent");
            } else {
                g.fire("views:mv:plain");
            }
            g.views.matviews[mi].populated = true;
            vec![
                StmtKind::Raw(format!(
                    "REFRESH MATERIALIZED VIEW {}{};",
                    if concurrent { "CONCURRENTLY " } else { "" },
                    name
                )),
                StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", name)),
            ]
        }
    }
}

// -------------------------------------------------------------- rules ----

/// Rule action SQL for one kind, over `tab`/`log`.
fn rule_sql(g: &mut Gen, name: &str, tab: &str, log: &str, kind: RuleKind) -> String {
    match kind {
        RuleKind::InsertAlsoLog => format!(
            "CREATE RULE {} AS ON INSERT TO {} DO ALSO INSERT INTO {} VALUES (NEW.pk + 1, \
             lower(coalesce(NEW.k_text, 'none')));",
            name, tab, log
        ),
        RuleKind::InsertAlsoMulti => format!(
            "CREATE RULE {} AS ON INSERT TO {} DO ALSO (INSERT INTO {} VALUES (NEW.pk, 'one'); \
             INSERT INTO {} VALUES (NEW.pk + 100, upper(coalesce(NEW.k_text, 'n'))));",
            name, tab, log, log
        ),
        RuleKind::InsertAlsoSelect => format!(
            "CREATE RULE {} AS ON INSERT TO {} WHERE NEW.pk % 2 = 0 DO ALSO INSERT INTO {} \
             SELECT NEW.pk, k_text FROM {} WHERE pk < NEW.pk;",
            name, tab, log, tab
        ),
        RuleKind::InsertInsteadRedirect => format!(
            "CREATE RULE {} AS ON INSERT TO {} DO INSTEAD INSERT INTO {} VALUES (NEW.pk * 2, \
             NEW.k_text);",
            name, tab, log
        ),
        RuleKind::InsertInsteadNothing => {
            format!("CREATE RULE {} AS ON INSERT TO {} DO INSTEAD NOTHING;", name, tab)
        }
        RuleKind::UpdateCondAlso => format!(
            "CREATE RULE {} AS ON UPDATE TO {} WHERE NEW.k_int > OLD.k_int DO ALSO INSERT INTO \
             {} VALUES (NEW.k_int - OLD.k_int, 'up');",
            name, tab, log
        ),
        RuleKind::UpdateCondInsteadNothing => format!(
            "CREATE RULE {} AS ON UPDATE TO {} WHERE OLD.pk = {} DO INSTEAD NOTHING;",
            name,
            tab,
            g.rng.range_i64(1, 4)
        ),
        RuleKind::DeleteInsteadNothing => {
            format!("CREATE RULE {} AS ON DELETE TO {} DO INSTEAD NOTHING;", name, tab)
        }
        RuleKind::DeleteAlsoNothing => {
            format!("CREATE RULE {} AS ON DELETE TO {} DO ALSO NOTHING;", name, tab)
        }
    }
}

fn pick_rule_kind(g: &mut Gen) -> RuleKind {
    match g.weights.pick(
        g.rng,
        &[
            "views:rule:ins_also",
            "views:rule:ins_also_multi",
            "views:rule:ins_also_select",
            "views:rule:ins_instead_redirect",
            "views:rule:ins_instead_nothing",
            "views:rule:upd_cond_also",
            "views:rule:upd_cond_instead",
            "views:rule:del_instead_nothing",
            "views:rule:del_also_nothing",
        ],
    ) {
        "views:rule:ins_also_multi" => RuleKind::InsertAlsoMulti,
        "views:rule:ins_also_select" => RuleKind::InsertAlsoSelect,
        "views:rule:ins_instead_redirect" => RuleKind::InsertInsteadRedirect,
        "views:rule:ins_instead_nothing" => RuleKind::InsertInsteadNothing,
        "views:rule:upd_cond_also" => RuleKind::UpdateCondAlso,
        "views:rule:upd_cond_instead" => RuleKind::UpdateCondInsteadNothing,
        "views:rule:del_instead_nothing" => RuleKind::DeleteInsteadNothing,
        "views:rule:del_also_nothing" => RuleKind::DeleteAlsoNothing,
        _ => RuleKind::InsertAlsoLog,
    }
}

/// Module-local DML on a rule table: small deterministic values only (rule
/// actions do arithmetic on them), and never RETURNING — an unconditional
/// DO INSTEAD rule on INSERT makes RETURNING an error.
fn rule_dml(g: &mut Gen, ti: usize) -> Vec<StmtKind> {
    let tab = g.views.ruletabs[ti].table.clone();
    let mut out = Vec::new();
    let n = 1 + g.rng.below_usize(3);
    for _ in 0..n {
        match g.weights.pick(
            g.rng,
            &["views:rule:dml:insert", "views:rule:dml:update", "views:rule:dml:delete"],
        ) {
            "views:rule:dml:update" => {
                g.fire("views:rule:dml:update");
                let pk = g.rng.range_i64(1, g.views.ruletabs[ti].next_pk.max(2) - 1);
                out.push(StmtKind::Raw(format!(
                    "UPDATE {} SET k_int = {}, k_text = '{}' WHERE pk = {};",
                    tab,
                    g.rng.range_i64(-50, 50),
                    TEXT_VOCAB[g.rng.below_usize(TEXT_VOCAB.len())],
                    pk
                )));
            }
            "views:rule:dml:delete" => {
                g.fire("views:rule:dml:delete");
                let pk = g.rng.range_i64(1, g.views.ruletabs[ti].next_pk.max(2) - 1);
                out.push(StmtKind::Raw(format!("DELETE FROM {} WHERE pk = {};", tab, pk)));
            }
            _ => {
                g.fire("views:rule:dml:insert");
                let pk = g.views.ruletabs[ti].next_pk;
                g.views.ruletabs[ti].next_pk += 1;
                let k_int = if g.rng.chance(1, 6) {
                    "NULL".to_string()
                } else {
                    g.rng.range_i64(-50, 50).to_string()
                };
                let k_text = if g.rng.chance(1, 6) {
                    "NULL".to_string()
                } else {
                    format!("'{}'", TEXT_VOCAB[g.rng.below_usize(TEXT_VOCAB.len())])
                };
                out.push(StmtKind::Raw(format!(
                    "INSERT INTO {} VALUES ({}, {}, {});",
                    tab, pk, k_int, k_text
                )));
            }
        }
    }
    out
}

/// Rule group: create a rule table (with its log sink and 1-3 rules), or
/// extend/exercise an existing one, or drop one. The rule table's CREATE
/// leads a creation group so its probe window resolves correctly.
fn gen_rule_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("views:rule");
    let live = g.views.live_ruletabs();
    let mut action = g
        .weights
        .pick(g.rng, &["views:rule:create", "views:rule:add", "views:rule:drop"]);
    if live.is_empty() {
        action = "views:rule:create";
    } else if action == "views:rule:create" && live.len() >= MAX_LIVE_RULETABS {
        g.fire("views:cap:ruletabs");
        action = "views:rule:add";
    }

    if action == "views:rule:drop" {
        g.fire("views:rule:drop");
        let ti = live[g.rng.below_usize(live.len())];
        let rules = g.views.ruletabs[ti].live_rules();
        // Drop one rule, or the whole table when it has none left.
        if !rules.is_empty() && g.rng.chance(2, 3) {
            let ri = rules[g.rng.below_usize(rules.len())];
            g.views.ruletabs[ti].rules[ri].live = false;
            let rname = g.views.ruletabs[ti].rules[ri].name.clone();
            let tab = g.views.ruletabs[ti].table.clone();
            return vec![StmtKind::Raw(format!("DROP RULE {} ON {};", rname, tab))];
        }
        g.views.ruletabs[ti].live = false;
        let tab = g.views.ruletabs[ti].table.clone();
        let log = g.views.ruletabs[ti].log.clone();
        g.views.events.push(DdlEvent {
            table: tab.clone(),
            pk: "pk".to_string(),
            kind: DdlEventKind::Dropped,
        });
        return vec![
            StmtKind::Raw(format!("DROP TABLE {};", tab)),
            StmtKind::Raw(format!("DROP TABLE {};", log)),
        ];
    }

    if action == "views:rule:add" {
        g.fire("views:rule:add");
        let ti = live[g.rng.below_usize(live.len())];
        let mut stmts = Vec::new();
        if g.views.ruletabs[ti].live_rules().len() < MAX_RULES_PER_TABLE {
            let kind = pick_rule_kind(g);
            g.fire(kind.prod());
            let rname = format!("fz_rule_{}", g.views.next_rule);
            g.views.next_rule += 1;
            let (tab, log) =
                (g.views.ruletabs[ti].table.clone(), g.views.ruletabs[ti].log.clone());
            stmts.push(StmtKind::Raw(rule_sql(g, &rname, &tab, &log, kind)));
            g.views.ruletabs[ti].rules.push(RuleObj { name: rname, kind, live: true });
        }
        stmts.extend(rule_dml(g, ti));
        stmts.extend(rule_reads(g, ti));
        return stmts;
    }

    // Create a fresh rule table.
    g.fire("views:rule:create");
    let n = g.views.next_ruletab;
    g.views.next_ruletab += 1;
    let tab = format!("fz_rt_{}", n);
    let log = format!("fz_rl_{}", n);
    let mut stmts = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {} (pk int4 PRIMARY KEY, k_int int4, k_text text);",
            tab
        )),
        StmtKind::Raw(format!("CREATE TABLE {} (a int4, b text);", log)),
    ];
    g.views.ruletabs.push(RuleTab {
        table: tab.clone(),
        log: log.clone(),
        rules: Vec::new(),
        next_pk: 1,
        live: true,
    });
    let ti = g.views.ruletabs.len() - 1;
    let nrules = 1 + g.rng.below_usize(3);
    for _ in 0..nrules {
        let kind = pick_rule_kind(g);
        g.fire(kind.prod());
        let rname = format!("fz_rule_{}", g.views.next_rule);
        g.views.next_rule += 1;
        stmts.push(StmtKind::Raw(rule_sql(g, &rname, &tab, &log, kind)));
        g.views.ruletabs[ti].rules.push(RuleObj { name: rname, kind, live: true });
    }
    g.views.events.push(DdlEvent {
        table: tab,
        pk: "pk".to_string(),
        kind: DdlEventKind::Created,
    });
    stmts.extend(rule_dml(g, ti));
    stmts.extend(rule_reads(g, ti));
    stmts
}

/// The rule group's compare surface: the table (pk-ordered, total) and the
/// log (ordered by every output column, so ties are between identical
/// rows). A RETURNING read rides along only where no unconditional DO
/// INSTEAD rule on INSERT exists (it would be an error otherwise).
fn rule_reads(g: &mut Gen, ti: usize) -> Vec<StmtKind> {
    let tab = g.views.ruletabs[ti].table.clone();
    let log = g.views.ruletabs[ti].log.clone();
    let mut out = vec![
        StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", tab)),
        StmtKind::Raw(format!("SELECT * FROM {} ORDER BY a, b;", log)),
    ];
    if !g.views.ruletabs[ti].has_insert_instead() && g.rng.chance(1, 4) {
        g.fire("views:rule:returning");
        let pk = g.views.ruletabs[ti].next_pk;
        g.views.ruletabs[ti].next_pk += 1;
        out.push(StmtKind::Raw(format!(
            "INSERT INTO {} VALUES ({}, {}, 'r') RETURNING pk, k_int;",
            tab,
            pk,
            g.rng.range_i64(-50, 50)
        )));
    }
    out
}

// ------------------------------------------------------------ deparse ----

/// Deparse probes: pg_get_viewdef in its three argument forms and
/// pg_get_ruledef over a rule table's whole (rulename-ordered) rule set.
/// The deparsed text is a strict differential surface — ruleutils output
/// must match the C implementation character for character.
fn gen_deparse(g: &mut Gen) -> Vec<StmtKind> {
    let views = g.views.live_views();
    let mvs = g.views.live_matviews();
    let rts = g.views.live_ruletabs();
    if views.is_empty() && mvs.is_empty() && rts.is_empty() {
        g.fire("views:fallback:create");
        return gen_create_view(g);
    }
    g.fire("views:deparse");
    // Which object to deparse: view / matview / rules (only live ones).
    let mut options: Vec<&str> = Vec::new();
    if !views.is_empty() {
        options.push("views:deparse:view");
    }
    if !mvs.is_empty() {
        options.push("views:deparse:matview");
    }
    if !rts.is_empty() {
        options.push("views:deparse:rules");
    }
    let choice = g.weights.pick(g.rng, &options);
    g.fire(choice);
    match choice {
        "views:deparse:rules" => {
            let ti = rts[g.rng.below_usize(rts.len())];
            let tab = g.views.ruletabs[ti].table.clone();
            let pretty = if g.rng.chance(1, 2) { ", true" } else { "" };
            vec![
                StmtKind::Raw(format!(
                    "SELECT r.rulename, pg_get_ruledef(r.oid{}) FROM pg_rewrite r JOIN pg_class \
                     c ON c.oid = r.ev_class WHERE c.relname = '{}' ORDER BY r.rulename;",
                    pretty, tab
                )),
                StmtKind::Raw(format!(
                    "SELECT r.rulename, r.ev_type, r.is_instead FROM pg_rewrite r JOIN pg_class \
                     c ON c.oid = r.ev_class WHERE c.relname = '{}' ORDER BY r.rulename;",
                    tab
                )),
            ]
        }
        "views:deparse:matview" => {
            let mi = mvs[g.rng.below_usize(mvs.len())];
            let name = g.views.matviews[mi].name.clone();
            vec![StmtKind::Raw(format!(
                "SELECT pg_get_viewdef('{}'::regclass, true);",
                name
            ))]
        }
        _ => {
            let vi = views[g.rng.below_usize(views.len())];
            let name = g.views.views[vi].table.name.clone();
            // The three pg_get_viewdef forms: plain, pretty-print boolean,
            // and the wrap-column integer.
            let form = g.weights.pick(
                g.rng,
                &["views:deparse:plain", "views:deparse:pretty", "views:deparse:wrap"],
            );
            g.fire(form);
            let call = match form {
                "views:deparse:pretty" => {
                    format!("pg_get_viewdef('{}'::regclass, true)", name)
                }
                "views:deparse:wrap" => format!(
                    "pg_get_viewdef('{}'::regclass, {})",
                    name,
                    [0, 20, 40, 80][g.rng.below_usize(4)]
                ),
                _ => format!("pg_get_viewdef('{}'::regclass)", name),
            };
            let mut out = vec![StmtKind::Raw(format!("SELECT {};", call))];
            if g.rng.chance(1, 3) {
                g.fire("views:deparse:updatable");
                out.push(StmtKind::Raw(format!(
                    "SELECT pg_relation_is_updatable('{}'::regclass, true);",
                    name
                )));
            }
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::ddl::DdlState;
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Session-shaped harness: persistent ViewsState across groups, the
    /// effective catalog rebuilt after every group.
    fn gen_many(seed: u64, n: usize, spec: &str) -> (Vec<String>, Vec<String>, ViewsState) {
        let base = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut views = ViewsState::new();
        let mut ddl = DdlState::new();
        let mut sqls = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let eff = views.extend_catalog(&ddl.extend_catalog(&base));
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &eff, &w, &mut p, 3);
            std::mem::swap(&mut g.views, &mut views);
            std::mem::swap(&mut g.ddl, &mut ddl);
            let kinds = gen_views_module(&mut g);
            std::mem::swap(&mut g.views, &mut views);
            std::mem::swap(&mut g.ddl, &mut ddl);
            assert!(!kinds.is_empty());
            for k in kinds {
                sqls.push(k.to_sql());
            }
            prods.extend(p);
        }
        (sqls, prods, views)
    }

    #[test]
    fn views_is_deterministic() {
        let (a, _, _) = gen_many(77, 300, "");
        let (b, _, _) = gen_many(77, 300, "");
        assert_eq!(a, b);
        let (c, _, _) = gen_many(78, 300, "");
        assert_ne!(a, c);
    }

    /// Every flavor the module claims really appears in a long stream.
    #[test]
    fn views_variety() {
        let (sqls, prods, _) = gen_many(0x1E77, 1500, "");
        let all = sqls.join("\n");
        for frag in [
            "CREATE TABLE fz_vb_",
            "CREATE VIEW fz_vw_",
            "CREATE TEMP VIEW fz_vw_",
            "CREATE OR REPLACE VIEW fz_vw_",
            "WITH LOCAL CHECK OPTION",
            "WITH CASCADED CHECK OPTION",
            "WHERE pk >= 0",
            "WHERE k_int IS NOT NULL AND k_int < 1000",
            "DROP VIEW fz_vw_",
            "DROP TABLE fz_vb_",
            "CREATE MATERIALIZED VIEW fz_mv_",
            "WITH NO DATA",
            "REFRESH MATERIALIZED VIEW fz_mv_",
            "REFRESH MATERIALIZED VIEW CONCURRENTLY fz_mv_",
            "DROP MATERIALIZED VIEW fz_mv_",
            "CREATE UNIQUE INDEX fz_mv_",
            "CREATE RULE fz_rule_",
            "DO ALSO INSERT INTO fz_rl_",
            "DO INSTEAD NOTHING",
            "DO INSTEAD INSERT INTO fz_rl_",
            "ON INSERT TO fz_rt_",
            "ON UPDATE TO fz_rt_",
            "ON DELETE TO fz_rt_",
            "DROP RULE fz_rule_",
            "pg_get_viewdef(",
            "pg_get_ruledef(",
            "pg_relation_is_updatable(",
            "GROUP BY k_int;",
            "SELECT DISTINCT k_int",
            "ORDER BY pk LIMIT 3;",
            "RETURNING pk, k_int;",
        ] {
            assert!(all.contains(frag), "views flavor {frag:?} never generated");
        }
        for p in [
            "views:create",
            "views:replace",
            "views:drop",
            "views:nested",
            "views:readonly",
            "views:matview",
            "views:rule",
            "views:deparse",
            "views:qual:none",
            "views:qual:pk",
            "views:qual:hide",
            "views:wco:local",
            "views:wco:cascaded",
            "views:temp",
            "views:mv:create",
            "views:mv:refresh",
            "views:mv:concurrent",
            "views:mv:drop",
            "views:mv:nodata",
            "views:rule:create",
            "views:rule:add",
            "views:rule:drop",
            "views:rule:ins_also",
            "views:rule:ins_also_multi",
            "views:rule:ins_also_select",
            "views:rule:ins_instead_redirect",
            "views:rule:ins_instead_nothing",
            "views:rule:upd_cond_also",
            "views:rule:upd_cond_instead",
            "views:rule:del_instead_nothing",
            "views:rule:del_also_nothing",
            "views:ro:agg",
            "views:ro:join",
            "views:ro:distinct",
            "views:ro:limit",
            "views:deparse:plain",
            "views:deparse:pretty",
            "views:deparse:wrap",
            "views:deparse:rules",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        for sql in &sqls {
            assert!(!sql.contains('\n') && sql.ends_with(';'), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
        }
    }

    /// Zero-42xxx discipline, statically: replay the stream against a
    /// text-level model — nothing references a relation before its CREATE
    /// or after its DROP, names are never reused, a view is always dropped
    /// before its base table, and group-local objects (nested and
    /// non-updatable views) never outlive their group.
    #[test]
    fn references_are_live() {
        let (sqls, _, _) = gen_many(0x5EED, 2000, "");
        use std::collections::HashMap;
        let mut live: HashMap<String, bool> = HashMap::new();
        // Fixture tables exist from the start and are never dropped.
        for t in &FixtureCatalog.load_catalog().unwrap().tables {
            live.insert(t.name.clone(), true);
        }
        // A relation reference is any fz_-prefixed identifier in the text.
        let refs = |sql: &str| -> Vec<String> {
            let mut out = Vec::new();
            let b = sql.as_bytes();
            let mut i = 0;
            while i < b.len() {
                if b[i..].starts_with(b"fz_") {
                    let mut j = i;
                    while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                        j += 1;
                    }
                    out.push(sql[i..j].to_string());
                    i = j;
                } else {
                    i += 1;
                }
            }
            out
        };
        for sql in &sqls {
            if let Some(rest) = sql.strip_prefix("CREATE UNIQUE INDEX ") {
                let name = rest.split(' ').next().unwrap().to_string();
                let target = rest.split(" ON ").nth(1).unwrap().split(' ').next().unwrap();
                assert_eq!(live.get(target), Some(&true), "index over dead relation: {sql}");
                live.insert(name, true);
                continue;
            }
            if sql.starts_with("CREATE RULE ") || sql.starts_with("DROP RULE ") {
                // Rule statements name a rule and its event relation; the
                // relation reference is checked by the generic arm below.
                for r in refs(sql) {
                    if r.starts_with("fz_rule_") {
                        continue;
                    }
                    assert_eq!(live.get(&r), Some(&true), "rule over dead relation: {sql}");
                }
                continue;
            }
            // Creations next: a CREATE may reference its own new name.
            let created: Option<String> = ["CREATE TABLE ", "CREATE VIEW ", "CREATE TEMP VIEW ", "CREATE MATERIALIZED VIEW "]
                .iter()
                .find_map(|p| sql.strip_prefix(p))
                .map(|rest| rest.split([' ', '(']).next().unwrap().to_string());
            if let Some(name) = &created {
                assert_ne!(live.get(name), Some(&true), "relation name reused: {sql}");
                // Everything it reads must be live.
                for r in refs(sql) {
                    if &r == name || r.starts_with("fz_rule_") {
                        continue;
                    }
                    assert_eq!(live.get(&r), Some(&true), "create over dead relation: {sql}");
                }
                live.insert(name.clone(), true);
                continue;
            }
            if let Some(rest) = sql.strip_prefix("CREATE OR REPLACE VIEW ") {
                let name = rest.split(' ').next().unwrap().to_string();
                assert_eq!(live.get(&name), Some(&true), "replace of dead view: {sql}");
                continue;
            }
            if let Some(rest) = sql
                .strip_prefix("DROP TABLE ")
                .or_else(|| sql.strip_prefix("DROP VIEW "))
                .or_else(|| sql.strip_prefix("DROP MATERIALIZED VIEW "))
            {
                let name = rest.trim_end_matches(';').to_string();
                assert_eq!(live.get(&name), Some(&true), "drop of dead relation: {sql}");
                // A base table may only be dropped once its view is gone.
                if let Some(n) = name.strip_prefix("fz_vb_") {
                    let view = format!("fz_vw_{}", n);
                    assert_ne!(
                        live.get(&view),
                        Some(&true),
                        "base dropped while its view lives: {sql}"
                    );
                }
                live.insert(format!("{}_ux", name), false);
                live.insert(name, false);
                continue;
            }
            // Every other statement: all referenced relations must be live.
            for r in refs(sql) {
                if r.starts_with("fz_rule_") {
                    continue;
                }
                assert_eq!(live.get(&r), Some(&true), "reference to dead relation: {sql}");
            }
        }
        // Group-local objects are all closed out at the end of the stream.
        for (name, alive) in &live {
            if name.starts_with("fz_vro_") || name.contains("_n") && name.starts_with("fz_vw_") {
                assert!(!alive, "group-local view {name} outlived its group");
            }
        }
    }

    /// The check-option discipline: a WITH CHECK OPTION never rides on the
    /// hiding qual (ordinary cross-module DML would then error), and every
    /// nested view — the deliberate violation surface — always does.
    #[test]
    fn check_options_only_where_safe() {
        let (sqls, _, _) = gen_many(0xC0DE, 1500, "");
        for sql in &sqls {
            if !sql.contains("CHECK OPTION") {
                continue;
            }
            if sql.contains("FROM fz_vw_") {
                // Nested view over a registered view: violations are this
                // module's own, deliberate business.
                continue;
            }
            assert!(
                !sql.contains("k_int IS NOT NULL AND k_int < 1000"),
                "check option on the hiding qual: {sql}"
            );
        }
        // Every nested view carries a check option.
        for sql in &sqls {
            if sql.starts_with("CREATE VIEW fz_vw_") && sql.contains("_n") {
                assert!(sql.contains("CHECK OPTION"), "nested view without a check option: {sql}");
            }
        }
    }

    /// Registered views are DML-eligible with sound metadata, and their
    /// private base tables never enter the catalog (one DML surface per key
    /// space).
    #[test]
    fn extend_catalog_registers_views_only() {
        let base = FixtureCatalog.load_catalog().unwrap();
        let (_, _, state) = gen_many(0xCA71, 500, "");
        let eff = state.extend_catalog(&base);
        let live_n = state.views.iter().filter(|v| v.live).count();
        assert!(live_n > 0, "no live view after 500 groups");
        assert!(live_n <= MAX_LIVE_VIEWS);
        assert_eq!(eff.tables.len(), base.tables.len() + live_n);
        for v in state.views.iter().filter(|v| v.live) {
            let t = &v.table;
            assert!(t.name.starts_with("fz_vw_"));
            // DML-eligible: pk-carrying, multi-row, unique pk.
            let pk = t.pk.as_ref().expect("registered view carries pk metadata");
            assert_eq!(pk.column, "pk");
            assert!(pk.seeded_max >= 2, "{}: seed rows must be dense from 1", t.name);
            assert!(!t.at_most_one_row);
            assert!(t.pk_unique);
            assert_eq!(t.unique_key.as_deref(), Some("pk"));
            // Auto-updatable shape: every base column exposed, all but pk
            // nullable (column-subset INSERTs must never 23502).
            assert_eq!(t.columns.len(), v.cols.len());
            for c in t.columns.iter().skip(1) {
                assert!(c.nullable, "{}.{} must be nullable", t.name, c.name);
            }
            for key in ["pk", "k_int", "k_text"] {
                assert!(t.columns.iter().any(|c| c.name == key), "{} missing {key}", t.name);
            }
            // The base is NOT registered.
            assert!(
                !eff.tables.iter().any(|x| x.name == v.base),
                "base table {} leaked into the catalog",
                v.base
            );
        }
    }

    /// Probe windows land on relations whose CREATE leads their group: the
    /// event's table is always the base table / matview / rule table, never
    /// a view (the session resolves an event to the group's base index).
    #[test]
    fn probe_events_target_group_leading_relations() {
        let base = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xB0B);
        let mut views = ViewsState::new();
        let mut ddl = DdlState::new();
        let mut created = 0usize;
        for _ in 0..600 {
            let eff = views.extend_catalog(&base);
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &eff, &w, &mut p, 3);
            std::mem::swap(&mut g.views, &mut views);
            std::mem::swap(&mut g.ddl, &mut ddl);
            let kinds = gen_views_module(&mut g);
            std::mem::swap(&mut g.views, &mut views);
            std::mem::swap(&mut g.ddl, &mut ddl);
            let sqls: Vec<String> = kinds.iter().map(|k| k.to_sql()).collect();
            for ev in views.take_events() {
                assert!(
                    ev.table.starts_with("fz_vb_")
                        || ev.table.starts_with("fz_mv_")
                        || ev.table.starts_with("fz_rt_"),
                    "probe event on a non-probeable relation: {}",
                    ev.table
                );
                assert_eq!(ev.pk, "pk");
                if ev.kind == DdlEventKind::Created {
                    created += 1;
                    // The creating statement leads the group.
                    assert!(
                        sqls[0].starts_with(&format!("CREATE TABLE {} ", ev.table))
                            || sqls[0]
                                .starts_with(&format!("CREATE MATERIALIZED VIEW {} ", ev.table)),
                        "created relation {} does not lead its group: {}",
                        ev.table,
                        sqls[0]
                    );
                }
            }
        }
        assert!(created > 0, "no probeable relation created in 600 groups");
    }

    /// Module-local writes can never collide with the DmlState allocator
    /// that owns a registered view's key space.
    #[test]
    fn local_writes_use_the_high_pk_block() {
        let (sqls, _, _) = gen_many(0x9111, 800, "");
        let mut saw = false;
        for sql in &sqls {
            // Nested-view writes are the only module-local writes into a
            // registered view's key space.
            let Some(rest) = sql.strip_prefix("INSERT INTO fz_vw_") else { continue };
            if !rest.contains("_n") {
                continue;
            }
            let pk: i64 = rest
                .split_once("VALUES (")
                .unwrap()
                .1
                .split(',')
                .next()
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            assert!(pk > LOCAL_PK_BASE, "module-local pk inside the DmlState range: {sql}");
            saw = true;
        }
        assert!(saw, "no nested-view write in 800 groups");
    }

    /// RETURNING is never generated on a rule table carrying an
    /// unconditional DO INSTEAD rule on INSERT (that combination is an
    /// unconditional error).
    #[test]
    fn no_returning_under_insert_instead() {
        let (sqls, _, _) = gen_many(0x8E77, 1500, "");
        use std::collections::HashMap;
        // rule name -> event relation, for rules that are unconditional DO
        // INSTEAD on INSERT (the shape that outlaws RETURNING).
        let mut instead: HashMap<String, String> = HashMap::new();
        for sql in &sqls {
            if sql.contains(" ON INSERT TO ") && sql.contains(" DO INSTEAD ") {
                let rule = sql
                    .strip_prefix("CREATE RULE ")
                    .unwrap()
                    .split(' ')
                    .next()
                    .unwrap()
                    .to_string();
                let tab = sql
                    .split(" ON INSERT TO ")
                    .nth(1)
                    .unwrap()
                    .split(' ')
                    .next()
                    .unwrap()
                    .to_string();
                instead.insert(rule, tab);
            }
            if let Some(rest) = sql.strip_prefix("DROP RULE ") {
                instead.remove(rest.split(' ').next().unwrap());
            }
            if let Some(rest) = sql.strip_prefix("DROP TABLE ") {
                let gone = rest.trim_end_matches(';').to_string();
                instead.retain(|_, t| t != &gone);
            }
            if sql.contains(" RETURNING ") {
                let tab = sql
                    .strip_prefix("INSERT INTO ")
                    .unwrap()
                    .split(' ')
                    .next()
                    .unwrap()
                    .to_string();
                assert!(
                    !instead.values().any(|t| t == &tab),
                    "RETURNING on a table with an INSTEAD insert rule: {sql}"
                );
            }
        }
    }

    /// A materialized view is never built over a TEMP view: matviews may
    /// not read a temporary relation at all (0A000), and the failed CREATE
    /// would leave every later reference to the matview a 42P01 (found by
    /// the first views-only differential smoke leg). Plain views over temp
    /// views are fine — Postgres silently makes them temp too (verified on
    /// both engines), which is why only matviews are pinned here.
    #[test]
    fn matviews_never_read_temp_views() {
        let (sqls, _, _) = gen_many(0x7E77, 2000, "");
        use std::collections::HashSet;
        let mut temp: HashSet<String> = HashSet::new();
        let mut saw_temp = false;
        let mut saw_matview = false;
        for sql in &sqls {
            if let Some(rest) = sql.strip_prefix("CREATE TEMP VIEW ") {
                temp.insert(rest.split(' ').next().unwrap().to_string());
                saw_temp = true;
                continue;
            }
            if let Some(rest) = sql.strip_prefix("CREATE OR REPLACE TEMP VIEW ") {
                temp.insert(rest.split(' ').next().unwrap().to_string());
                continue;
            }
            if let Some(rest) = sql.strip_prefix("DROP VIEW ") {
                temp.remove(rest.trim_end_matches(';'));
                continue;
            }
            let Some(rest) = sql.strip_prefix("CREATE MATERIALIZED VIEW ") else { continue };
            saw_matview = true;
            let src = rest.rsplit(" FROM ").next().unwrap();
            for t in &temp {
                assert!(
                    !src.split([' ', ';', ',']).any(|w| w == t.as_str()),
                    "materialized view over the temp view {t}: {sql}"
                );
            }
        }
        assert!(saw_temp, "no TEMP view in 2000 groups");
        assert!(saw_matview, "no materialized view in 2000 groups");
    }

    /// REFRESH ... CONCURRENTLY only ever targets a populated matview that
    /// carries its unique index.
    #[test]
    fn concurrent_refresh_is_always_legal() {
        let (sqls, _, _) = gen_many(0xFEED, 1500, "");
        use std::collections::HashMap;
        // matview -> (has unique index, populated)
        let mut mv: HashMap<String, (bool, bool)> = HashMap::new();
        for sql in &sqls {
            if let Some(rest) = sql.strip_prefix("CREATE MATERIALIZED VIEW ") {
                let name = rest.split(' ').next().unwrap().to_string();
                mv.insert(name, (false, !sql.contains("WITH NO DATA")));
            } else if let Some(rest) = sql.strip_prefix("CREATE UNIQUE INDEX ") {
                let name = rest.split(" ON ").nth(1).unwrap().split(' ').next().unwrap();
                if let Some(e) = mv.get_mut(name) {
                    e.0 = true;
                }
            } else if let Some(rest) =
                sql.strip_prefix("REFRESH MATERIALIZED VIEW CONCURRENTLY ")
            {
                let name = rest.trim_end_matches(';');
                let e = mv.get(name).unwrap_or_else(|| panic!("refresh of unknown {name}"));
                assert!(e.0, "CONCURRENTLY without a unique index: {sql}");
                assert!(e.1, "CONCURRENTLY on an unpopulated matview: {sql}");
            } else if let Some(rest) = sql.strip_prefix("REFRESH MATERIALIZED VIEW ") {
                if let Some(e) = mv.get_mut(rest.trim_end_matches(';')) {
                    e.1 = true;
                }
            } else if let Some(rest) = sql.strip_prefix("DROP MATERIALIZED VIEW ") {
                mv.remove(rest.trim_end_matches(';'));
            }
            // Reads of an unpopulated matview are an error too.
            if let Some(rest) = sql.strip_prefix("SELECT * FROM fz_mv_") {
                let name = format!("fz_mv_{}", rest.split(' ').next().unwrap());
                if let Some(e) = mv.get(&name) {
                    assert!(e.1, "read of an unpopulated matview: {sql}");
                }
            }
        }
    }
}
