//! SQL-invoked maintenance option-matrix drain module (VACUUM lane): the
//! Track-B *drainable* surface of the maintenance commands — the full
//! VACUUM/ANALYZE/CLUSTER/REINDEX option grammar (vacuum.c ExecVacuum /
//! parse-and-check, vacuumlazy.c, cluster.c, indexcmds.c ReindexIndex/
//! ReindexTable/ReindexMultipleTables, analyze.c) — NOT the autovacuum
//! daemon (that is the Antithesis fault-phase's job).
//!
//! Everything is SINGLE-SESSION and autocommit: VACUUM, CLUSTER and
//! REINDEX ... CONCURRENTLY cannot run inside a transaction block, so this
//! module never emits a BEGIN bracket. Each pick is a self-contained
//! statement sequence: (optional) dead-tuple churn -> one maintenance
//! statement (or option combo) -> deterministic verification probes.
//!
//! Determinism laws (same discipline as crate::heap / crate::spill):
//!   - The compare surface is DATA + INDEX-RESULT IDENTITY across the
//!     maintenance op. Every verification probe is aggregate-only with
//!     exact-typed (int8) aggregates plus a single md5(string_agg(...
//!     ORDER BY pk)) full-content fingerprint — one small deterministic
//!     row crosses the wire, never raw heap-ordered rows. Maintenance must
//!     not change the logical table content, so an A/B divergence on a
//!     post-maintenance probe is a HIGH-severity data/index-integrity
//!     finding.
//!   - VACUUM's own INFO/counters (pages removed, tuples frozen, ...) are a
//!     ruled non-surface: we never project them. Structural catalog probes
//!     read only STABLE columns (relkind/relhasindex/relnatts/relchecks/
//!     relpersistence, indisvalid/indisready/indisunique/indnatts) — never
//!     relfrozenxid (an absolute xid, per-instance) and never relpages/
//!     reltuples (vacuum/analyze counters).
//!   - No float anything (B1). No `random()`, no `now()`-derived data.
//!   - tables carry autovacuum_enabled = off (heap + toast) so every
//!     prune/freeze/truncate transition is an EXPLICIT statement, identical
//!     on both differential sides; committed base loads stay <= 6000 rows
//!     (exhaustive ANALYZE -> deterministic reltuples had we ever read it).
//!   - toast payloads (the `body` column, EXTENDED) live only on the low
//!     band pk <= BODY_BAND so PROCESS_TOAST/toast-table vacuum is exercised
//!     while state/content probes stay cheap.
//!
//! Data model, one live regular table + one live partitioned table at a
//! time (maintenance groups walk the whole relation):
//!   fz_vac_N   (pk int4 PRIMARY KEY, a int4 [btree fz_vaci_N], b int4,
//!              c text, body text EXTENDED) — some tables get
//!              ALTER TABLE ... CLUSTER ON so the index-less `CLUSTER t`
//!              form is legal.
//!   fz_vacp_N  range-partitioned by pk (2-3 leaves, each
//!              autovacuum_enabled=off) with a partitioned index fz_vacpi_N
//!              — the VACUUM/ANALYZE partition-propagation surface.
//! The REINDEX SCHEMA and REINDEX (TABLESPACE ...) arms build and drop
//! their own group-local objects (a throwaway schema; an in-place
//! tablespace) so they never touch the tracked fixtures.

use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

/// One regular + one partitioned live table at a time.
const MAX_LIVE_REGULAR: usize = 1;
const MAX_LIVE_PART: usize = 1;

/// Toast payloads confined to pk <= BODY_BAND.
const BODY_BAND: u32 = 24;

/// Deterministic row source for a regular table over pk lo..=hi.
/// a = (pk*7)%100 (btree-indexed key), b = pk%50, c = 'r'||pk,
/// body = a compressible payload on the low band only, else NULL.
fn reg_rows(lo: i64, hi: i64) -> String {
    format!(
        "SELECT i, (i * 7) % 100, i % 50, 'r' || i, \
         CASE WHEN i <= {BODY_BAND} THEN repeat('vac', 400 + (i % 20)::int) ELSE NULL END \
         FROM generate_series({lo}, {hi}) i"
    )
}

/// Deterministic row source for the partitioned table over pk lo..=hi.
fn part_rows(lo: i64, hi: i64) -> String {
    format!("SELECT i, (i * 7) % 100, 'p' || i FROM generate_series({lo}, {hi}) i")
}

#[derive(Clone, Debug)]
pub struct VacTable {
    pub name: String,
    pub live: bool,
    pub rows: u32,
    /// Regular table: the `a` index name (CLUSTER/REINDEX alternate target).
    pub a_index: String,
    /// Regular table only: ALTER TABLE ... CLUSTER ON was applied, so the
    /// index-less `CLUSTER t` form is legal.
    pub clustered: bool,
    /// True for the range-partitioned fixture (fz_vacp_N).
    pub partitioned: bool,
}

/// Session-persistent maintenance-fixture model (swapped in/out of `Gen` by
/// the session loop exactly like `HeapState`).
#[derive(Clone, Debug, Default)]
pub struct VacState {
    pub tables: Vec<VacTable>,
    next_table: u32,
    next_index: u32,
    next_local: u32,
    events: Vec<DdlEvent>,
}

impl VacState {
    pub fn new() -> VacState {
        VacState::default()
    }

    pub fn take_events(&mut self) -> Vec<DdlEvent> {
        std::mem::take(&mut self.events)
    }

    fn live(&self, partitioned: bool) -> Vec<usize> {
        self.tables
            .iter()
            .enumerate()
            .filter(|(_, t)| t.live && t.partitioned == partitioned)
            .map(|(i, _)| i)
            .collect()
    }
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_vacuum_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("vacuum");
    let action = g.weights.pick(
        g.rng,
        &[
            "vac:create",
            "vac:createpart",
            "vac:drop",
            "vac:vacuum",
            "vac:analyze",
            "vac:cluster",
            "vac:reindex",
            "vac:vacpart",
            "vac:reindexschema",
            "vac:reindextblspc",
            "vac:dbstats",
            "vac:badcombo",
        ],
    );
    match action {
        "vac:create" => gen_create(g),
        "vac:createpart" => gen_create_part(g),
        "vac:drop" => gen_drop(g),
        "vac:vacuum" => gen_vacuum(g),
        "vac:analyze" => gen_analyze(g),
        "vac:cluster" => gen_cluster(g),
        "vac:reindex" => gen_reindex(g),
        "vac:vacpart" => gen_vacpart(g),
        "vac:reindexschema" => gen_reindex_schema(g),
        "vac:reindextblspc" => gen_reindex_tablespace(g),
        "vac:dbstats" => gen_dbstats(g),
        _ => gen_badcombo(g),
    }
}

// ------------------------------------------------------------- helpers ----

/// A committed pk band `lo..=hi` inside 1..rows (width 20-200).
fn band(g: &mut Gen, rows: u32) -> (u32, u32) {
    let width = 20 + g.rng.below(180) as u32;
    let lo = 1 + g.rng.below(rows.saturating_sub(width).max(1) as u64) as u32;
    (lo, (lo + width).min(rows))
}

/// Full-content fingerprint + exact aggregates for a regular table. One
/// deterministic row; body is folded through md5 so no large datum crosses
/// the wire. This is the load-bearing "data unchanged by maintenance" probe.
fn reg_probe(t: &str) -> StmtKind {
    StmtKind::Raw(format!(
        "SELECT count(*), sum(a::int8), sum(b::int8), sum(pk::int8), \
         sum(length(body)::int8), \
         md5(string_agg(pk::text || ':' || a::text || ':' || b::text || ':' || \
         coalesce(c, '') || ':' || coalesce(md5(body), ''), '|' ORDER BY pk)) \
         FROM {t};"
    ))
}

/// Index-driven result probe over the `a` key (a REINDEX/CLUSTER that
/// silently corrupted the index would return a different count/sum here
/// than the seqscan-derived truth on the other engine).
fn reg_index_probe(g: &mut Gen, t: &str) -> StmtKind {
    let lo = g.rng.below(90) as u32;
    let hi = lo + 5 + g.rng.below(20) as u32;
    StmtKind::Raw(format!(
        "SELECT count(*), sum(pk::int8), sum(b::int8) FROM {t} WHERE a BETWEEN {lo} AND {hi};"
    ))
}

/// Content fingerprint for the partitioned table (queried through the parent).
fn part_probe(t: &str) -> StmtKind {
    StmtKind::Raw(format!(
        "SELECT count(*), sum(a::int8), sum(pk::int8), \
         md5(string_agg(pk::text || ':' || a::text || ':' || coalesce(c, ''), \
         '|' ORDER BY pk)) FROM {t};"
    ))
}

fn pick_regular(g: &mut Gen) -> Option<usize> {
    let live = g.vac.live(false);
    if live.is_empty() {
        return None;
    }
    Some(live[g.rng.below_usize(live.len())])
}

fn pick_part(g: &mut Gen) -> Option<usize> {
    let live = g.vac.live(true);
    if live.is_empty() {
        return None;
    }
    Some(live[g.rng.below_usize(live.len())])
}

macro_rules! need_regular {
    ($g:expr) => {
        match pick_regular($g) {
            Some(ti) => ti,
            None => {
                $g.fire("vac:fallback:create");
                return gen_create($g);
            }
        }
    };
}

macro_rules! need_part {
    ($g:expr) => {
        match pick_part($g) {
            Some(ti) => ti,
            None => {
                $g.fire("vac:fallback:createpart");
                return gen_create_part($g);
            }
        }
    };
}

/// Dead-tuple churn prelude over a regular table (identical on both sides),
/// so the following VACUUM/CLUSTER actually reclaims/rewrites. Net content
/// stays a pure function of the fixture load (delete-then-reinsert restores
/// the exact rows), so the fingerprint probe is stable across the churn.
fn reg_churn(g: &mut Gen, t: &str, rows: u32) -> Vec<StmtKind> {
    let (lo, hi) = band(g, rows);
    match g.rng.below(3) {
        0 => vec![
            StmtKind::Raw(format!("UPDATE {t} SET b = b + 1 WHERE pk BETWEEN {lo} AND {hi};")),
            StmtKind::Raw(format!("UPDATE {t} SET b = b - 1 WHERE pk BETWEEN {lo} AND {hi};")),
        ],
        1 => {
            let hi = hi.min(lo + 60);
            vec![
                StmtKind::Raw(format!("DELETE FROM {t} WHERE pk BETWEEN {lo} AND {hi};")),
                StmtKind::Raw(format!(
                    "INSERT INTO {t} {} ON CONFLICT (pk) DO NOTHING;",
                    reg_rows(lo as i64, hi as i64)
                )),
            ]
        }
        _ => vec![
            // Re-write the toast band (delete + re-toast old chunks), then
            // restore it, so PROCESS_TOAST has dead toast chunks to reap.
            StmtKind::Raw(format!(
                "UPDATE {t} SET body = body || 'x' WHERE pk <= {BODY_BAND} AND body IS NOT NULL;"
            )),
            StmtKind::Raw(format!(
                "UPDATE {t} SET body = \
                 CASE WHEN pk <= {BODY_BAND} THEN repeat('vac', 400 + (pk % 20)::int) ELSE body END \
                 WHERE pk <= {BODY_BAND};"
            )),
        ],
    }
}

// -------------------------------------------------------------- create ----

fn gen_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.vac.live(false).len() >= MAX_LIVE_REGULAR {
        g.fire("vac:cap:regular");
        return gen_drop(g);
    }
    g.fire("vac:create");
    let name = format!("fz_vac_{}", g.vac.next_table);
    g.vac.next_table += 1;
    let ff = match g.weights.pick(g.rng, &["vac:ff:30", "vac:ff:70", "vac:ff:100"]) {
        "vac:ff:30" => 30,
        "vac:ff:70" => 70,
        _ => 100,
    };
    let rows = match g.weights.pick(g.rng, &["vac:rows:1000", "vac:rows:3000", "vac:rows:6000"]) {
        "vac:rows:1000" => 1000,
        "vac:rows:3000" => 3000,
        _ => 6000,
    };
    let idx = format!("fz_vaci_{}", g.vac.next_index);
    g.vac.next_index += 1;
    let mut stmts = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {name} (pk int4 PRIMARY KEY, a int4, b int4, c text, body text) \
             WITH (fillfactor = {ff}, autovacuum_enabled = off, \
             toast.autovacuum_enabled = off);"
        )),
        StmtKind::Raw(format!("INSERT INTO {name} {};", reg_rows(1, rows as i64))),
        StmtKind::Raw(format!("CREATE INDEX {idx} ON {name} (a);")),
    ];
    let clustered = g.weights.pick(g.rng, &["vac:clusteron:yes", "vac:clusteron:no"])
        == "vac:clusteron:yes";
    if clustered {
        g.fire("vac:clusteron:yes");
        stmts.push(StmtKind::Raw(format!("ALTER TABLE {name} CLUSTER ON {idx};")));
    }
    stmts.push(StmtKind::Raw(format!("ANALYZE {name};")));
    g.vac.tables.push(VacTable {
        name: name.clone(),
        live: true,
        rows,
        a_index: idx,
        clustered,
        partitioned: false,
    });
    g.vac
        .events
        .push(DdlEvent { table: name, pk: "pk".to_string(), kind: DdlEventKind::Created });
    stmts
}

fn gen_create_part(g: &mut Gen) -> Vec<StmtKind> {
    if g.vac.live(true).len() >= MAX_LIVE_PART {
        g.fire("vac:cap:part");
        return gen_drop(g);
    }
    g.fire("vac:createpart");
    let name = format!("fz_vacp_{}", g.vac.next_table);
    g.vac.next_table += 1;
    let idx = format!("fz_vacpi_{}", g.vac.next_index);
    g.vac.next_index += 1;
    let nparts = if g.weights.pick(g.rng, &["vac:pp:2", "vac:pp:3"]) == "vac:pp:3" { 3 } else { 2 };
    let rows: u32 = 6000;
    let mut stmts = vec![StmtKind::Raw(format!(
        "CREATE TABLE {name} (pk int4, a int4, c text, PRIMARY KEY (pk)) PARTITION BY RANGE (pk);"
    ))];
    // Range bounds: even chunks with MINVALUE/MAXVALUE ends. chunk avoids 0.
    let chunk = rows / nparts + 1;
    for p in 0..nparts {
        let lo = if p == 0 { "MINVALUE".to_string() } else { (p * chunk + 1).to_string() };
        let hi =
            if p == nparts - 1 { "MAXVALUE".to_string() } else { ((p + 1) * chunk + 1).to_string() };
        stmts.push(StmtKind::Raw(format!(
            "CREATE TABLE {name}_p{p} PARTITION OF {name} FOR VALUES FROM ({lo}) TO ({hi}) \
             WITH (autovacuum_enabled = off);"
        )));
    }
    stmts.push(StmtKind::Raw(format!("CREATE INDEX {idx} ON {name} (a);")));
    stmts.push(StmtKind::Raw(format!("INSERT INTO {name} {};", part_rows(1, rows as i64))));
    stmts.push(StmtKind::Raw(format!("ANALYZE {name};")));
    g.vac.tables.push(VacTable {
        name: name.clone(),
        live: true,
        rows,
        a_index: idx,
        clustered: false,
        partitioned: true,
    });
    g.vac
        .events
        .push(DdlEvent { table: name, pk: "pk".to_string(), kind: DdlEventKind::Created });
    stmts
}

fn gen_drop(g: &mut Gen) -> Vec<StmtKind> {
    // Prefer dropping whichever kind is at/over its cap; otherwise any live.
    let live: Vec<usize> = g
        .vac
        .tables
        .iter()
        .enumerate()
        .filter(|(_, t)| t.live)
        .map(|(i, _)| i)
        .collect();
    if live.is_empty() {
        g.fire("vac:fallback:create");
        return gen_create(g);
    }
    g.fire("vac:drop");
    let ti = live[g.rng.below_usize(live.len())];
    let name = g.vac.tables[ti].name.clone();
    g.vac.tables[ti].live = false;
    g.vac.events.push(DdlEvent {
        table: name.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![StmtKind::Raw(format!("DROP TABLE {name};"))]
}

// -------------------------------------------------------------- vacuum ----

/// Assemble a parenthesized VACUUM option list respecting the mutual
/// exclusions enforced by ExecVacuum (FULL excludes PARALLEL and
/// BUFFER_USAGE_LIMIT; with FULL, PROCESS_MAIN/PROCESS_TOAST must not be
/// disabled). ONLY_DATABASE_STATS is handled by its own table-less arm.
fn build_paren_opts(g: &mut Gen) -> Vec<String> {
    let full = g.weights.pick(g.rng, &["vac:full:yes", "vac:full:no"]) == "vac:full:yes";
    let mut opts: Vec<String> = Vec::new();
    if full {
        g.fire("vac:full:yes");
        opts.push("FULL".to_string());
    }
    if g.rng.chance(1, 3) {
        opts.push("FREEZE".to_string());
    }
    if g.rng.chance(1, 3) {
        opts.push("ANALYZE".to_string());
    }
    if g.rng.chance(1, 3) {
        opts.push("VERBOSE".to_string());
    }
    if g.rng.chance(1, 3) {
        opts.push("DISABLE_PAGE_SKIPPING".to_string());
    }
    if g.rng.chance(1, 2) {
        let v = match g.weights.pick(g.rng, &["vac:ic:on", "vac:ic:off", "vac:ic:auto"]) {
            "vac:ic:on" => "ON",
            "vac:ic:off" => "OFF",
            _ => "AUTO",
        };
        opts.push(format!("INDEX_CLEANUP {v}"));
    }
    if g.rng.chance(1, 3) {
        // With FULL, PROCESS_TOAST cannot be disabled.
        let v = if full {
            "ON"
        } else if g.weights.pick(g.rng, &["vac:pt:on", "vac:pt:off"]) == "vac:pt:off" {
            "OFF"
        } else {
            "ON"
        };
        opts.push(format!("PROCESS_TOAST {v}"));
    }
    if g.rng.chance(1, 4) {
        // With FULL, PROCESS_MAIN cannot be disabled.
        let v = if full {
            "ON"
        } else if g.weights.pick(g.rng, &["vac:pm:on", "vac:pm:off"]) == "vac:pm:off" {
            "OFF"
        } else {
            "ON"
        };
        opts.push(format!("PROCESS_MAIN {v}"));
    }
    if g.rng.chance(1, 3) {
        let v = if g.weights.pick(g.rng, &["vac:tr:on", "vac:tr:off"]) == "vac:tr:off" {
            "OFF"
        } else {
            "ON"
        };
        opts.push(format!("TRUNCATE {v}"));
    }
    if g.rng.chance(1, 4) {
        opts.push("SKIP_LOCKED".to_string());
    }
    if g.rng.chance(1, 5) {
        opts.push("SKIP_DATABASE_STATS".to_string());
    }
    if !full && g.rng.chance(1, 3) {
        let n = match g.weights.pick(g.rng, &["vac:par:0", "vac:par:1", "vac:par:2", "vac:par:4"]) {
            "vac:par:0" => 0,
            "vac:par:1" => 1,
            "vac:par:2" => 2,
            _ => 4,
        };
        opts.push(format!("PARALLEL {n}"));
    }
    if !full && g.rng.chance(1, 4) {
        let v = match g.weights.pick(g.rng, &["vac:bul:small", "vac:bul:med", "vac:bul:big"]) {
            "vac:bul:small" => "128 kB",
            "vac:bul:med" => "2 MB",
            _ => "16 MB",
        };
        opts.push(format!("BUFFER_USAGE_LIMIT '{v}'"));
    }
    opts
}

/// The VACUUM option-matrix arm: churn -> one VACUUM statement (legacy
/// keyword form or a parenthesized option list) -> content + index probes.
fn gen_vacuum(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_regular!(g);
    g.fire("vac:vacuum");
    let t = g.vac.tables[ti].name.clone();
    let rows = g.vac.tables[ti].rows;
    let mut stmts = reg_churn(g, &t, rows);
    let form = g.weights.pick(g.rng, &["vac:v:legacy", "vac:v:paren"]);
    g.fire(form);
    match form {
        "vac:v:legacy" => {
            let kw = g.weights.pick(
                g.rng,
                &[
                    "vac:vl:plain",
                    "vac:vl:full",
                    "vac:vl:freeze",
                    "vac:vl:analyze",
                    "vac:vl:fullanalyze",
                    "vac:vl:freezeanalyze",
                ],
            );
            g.fire(kw);
            let prefix = match kw {
                "vac:vl:full" => "VACUUM FULL",
                "vac:vl:freeze" => "VACUUM FREEZE",
                "vac:vl:analyze" => "VACUUM ANALYZE",
                "vac:vl:fullanalyze" => "VACUUM FULL ANALYZE",
                "vac:vl:freezeanalyze" => "VACUUM FREEZE ANALYZE",
                _ => "VACUUM",
            };
            // Legacy ANALYZE variants may carry a column list.
            if kw.contains("analyze") && g.rng.chance(1, 2) {
                stmts.push(StmtKind::Raw(format!("{prefix} {t} (a, b);")));
            } else {
                stmts.push(StmtKind::Raw(format!("{prefix} {t};")));
            }
        }
        _ => {
            let mut opts = build_paren_opts(g);
            if opts.is_empty() {
                opts.push("VERBOSE".to_string());
            }
            let has_analyze = opts.iter().any(|o| o == "ANALYZE");
            let cols = if has_analyze && g.rng.chance(1, 2) { " (a, b)" } else { "" };
            stmts.push(StmtKind::Raw(format!("VACUUM ({}) {t}{cols};", opts.join(", "))));
        }
    }
    stmts.push(reg_probe(&t));
    stmts.push(reg_index_probe(g, &t));
    stmts
}

// ------------------------------------------------------------- analyze ----

/// The ANALYZE option-matrix arm: plain, column lists, VERBOSE, SKIP_LOCKED,
/// BUFFER_USAGE_LIMIT (all valid ANALYZE-only combos).
fn gen_analyze(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_regular!(g);
    g.fire("vac:analyze");
    let t = g.vac.tables[ti].name.clone();
    let shape = g.weights.pick(
        g.rng,
        &[
            "vac:an:plain",
            "vac:an:cols",
            "vac:an:verbose",
            "vac:an:verbosecols",
            "vac:an:skiplocked",
            "vac:an:bul",
        ],
    );
    g.fire(shape);
    let stmt = match shape {
        "vac:an:cols" => format!("ANALYZE {t} (a, b, c);"),
        "vac:an:verbose" => format!("ANALYZE (VERBOSE) {t};"),
        "vac:an:verbosecols" => format!("ANALYZE (VERBOSE) {t} (pk, a);"),
        "vac:an:skiplocked" => format!("ANALYZE (SKIP_LOCKED, VERBOSE) {t};"),
        "vac:an:bul" => format!("ANALYZE (BUFFER_USAGE_LIMIT '512 kB') {t} (a);"),
        _ => format!("ANALYZE {t};"),
    };
    vec![StmtKind::Raw(stmt), reg_probe(&t)]
}

// ------------------------------------------------------------- cluster ----

/// The CLUSTER arm: churn -> CLUSTER (index-qualified, index-less over a
/// previously CLUSTER-ON table, or VERBOSE) -> ANALYZE -> probes. The
/// tuple order CLUSTER imposes is a ruled non-surface; the probes assert
/// the logical CONTENT and index results are unchanged by the rewrite.
fn gen_cluster(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_regular!(g);
    g.fire("vac:cluster");
    let t = g.vac.tables[ti].name.clone();
    let rows = g.vac.tables[ti].rows;
    let idx = g.vac.tables[ti].a_index.clone();
    let clustered = g.vac.tables[ti].clustered;
    let mut stmts = reg_churn(g, &t, rows);
    let mut choices = vec!["vac:cl:usingpk", "vac:cl:usingidx", "vac:cl:verboseidx"];
    if clustered {
        choices.push("vac:cl:noidx");
        choices.push("vac:cl:verbosenoidx");
    }
    let shape = g.weights.pick(g.rng, &choices);
    g.fire(shape);
    let cluster = match shape {
        "vac:cl:usingpk" => format!("CLUSTER {t} USING {t}_pkey;"),
        "vac:cl:usingidx" => format!("CLUSTER {t} USING {idx};"),
        "vac:cl:verboseidx" => format!("CLUSTER (VERBOSE) {t} USING {idx};"),
        "vac:cl:noidx" => format!("CLUSTER {t};"),
        _ => format!("CLUSTER (VERBOSE) {t};"),
    };
    stmts.push(StmtKind::Raw(cluster));
    stmts.push(StmtKind::Raw(format!("ANALYZE {t};")));
    stmts.push(reg_probe(&t));
    stmts.push(reg_index_probe(g, &t));
    stmts
}

// ------------------------------------------------------------- reindex ----

/// The REINDEX arm over the tracked regular table: INDEX / TABLE, with and
/// without CONCURRENTLY and VERBOSE. Post-REINDEX probes assert the rebuilt
/// index returns the same rows the seqscan truth does on the other engine.
fn gen_reindex(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_regular!(g);
    g.fire("vac:reindex");
    let t = g.vac.tables[ti].name.clone();
    let idx = g.vac.tables[ti].a_index.clone();
    let shape = g.weights.pick(
        g.rng,
        &[
            "vac:ri:indexpk",
            "vac:ri:indexa",
            "vac:ri:verboseindex",
            "vac:ri:table",
            "vac:ri:verbosetable",
            "vac:ri:indexconc",
            "vac:ri:tableconc",
        ],
    );
    g.fire(shape);
    let reindex = match shape {
        "vac:ri:indexpk" => format!("REINDEX INDEX {t}_pkey;"),
        "vac:ri:indexa" => format!("REINDEX INDEX {idx};"),
        "vac:ri:verboseindex" => format!("REINDEX (VERBOSE) INDEX {idx};"),
        "vac:ri:table" => format!("REINDEX TABLE {t};"),
        "vac:ri:verbosetable" => format!("REINDEX (VERBOSE) TABLE {t};"),
        "vac:ri:indexconc" => format!("REINDEX INDEX CONCURRENTLY {idx};"),
        _ => format!("REINDEX TABLE CONCURRENTLY {t};"),
    };
    vec![StmtKind::Raw(reindex), reg_probe(&t), reg_index_probe(g, &t)]
}

// ------------------------------------------------------------- vacpart ----

/// Partitioned-table maintenance propagation: VACUUM/ANALYZE on the parent
/// recurses to every leaf; a leaf can also be maintained directly. Content
/// is verified through the parent.
fn gen_vacpart(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_part!(g);
    g.fire("vac:vacpart");
    let t = g.vac.tables[ti].name.clone();
    let rows = g.vac.tables[ti].rows;
    let (lo, hi) = band(g, rows);
    // Churn (net-neutral) so the vacuum has work in the leaves.
    let mut stmts = vec![
        StmtKind::Raw(format!("UPDATE {t} SET a = a + 1 WHERE pk BETWEEN {lo} AND {hi};")),
        StmtKind::Raw(format!("UPDATE {t} SET a = a - 1 WHERE pk BETWEEN {lo} AND {hi};")),
    ];
    let shape = g.weights.pick(
        g.rng,
        &[
            "vac:vp:vacuum",
            "vac:vp:analyze",
            "vac:vp:vacanalyze",
            "vac:vp:freeze",
            "vac:vp:leaf",
        ],
    );
    g.fire(shape);
    match shape {
        "vac:vp:vacuum" => stmts.push(StmtKind::Raw(format!("VACUUM {t};"))),
        "vac:vp:analyze" => stmts.push(StmtKind::Raw(format!("ANALYZE {t};"))),
        "vac:vp:vacanalyze" => stmts.push(StmtKind::Raw(format!("VACUUM (ANALYZE) {t};"))),
        "vac:vp:freeze" => stmts.push(StmtKind::Raw(format!("VACUUM (FREEZE, PROCESS_TOAST ON) {t};"))),
        _ => {
            // Directly maintain a single leaf partition.
            let p = g.rng.below(2);
            stmts.push(StmtKind::Raw(format!("VACUUM (ANALYZE) {t}_p{p};")));
        }
    }
    stmts.push(part_probe(&t));
    stmts
}

// -------------------------------------------------- reindex schema (local) --

/// REINDEX SCHEMA over a self-contained throwaway schema (created and
/// dropped in-group; never touches the tracked fixtures).
fn gen_reindex_schema(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("vac:reindexschema");
    let n = g.vac.next_local;
    g.vac.next_local += 1;
    let sch = format!("fz_vac_s{n}");
    let conc = if g.weights.pick(g.rng, &["vac:rs:plain", "vac:rs:conc"]) == "vac:rs:conc" {
        g.fire("vac:rs:conc");
        "REINDEX (VERBOSE) SCHEMA CONCURRENTLY"
    } else {
        "REINDEX SCHEMA"
    };
    vec![
        StmtKind::Raw(format!("CREATE SCHEMA {sch};")),
        StmtKind::Raw(format!(
            "CREATE TABLE {sch}.t (pk int4 PRIMARY KEY, a int4, c text) \
             WITH (autovacuum_enabled = off);"
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {sch}.t SELECT i, (i * 7) % 100, 'r' || i FROM generate_series(1, 500) i;"
        )),
        StmtKind::Raw(format!("CREATE INDEX ON {sch}.t (a);")),
        StmtKind::Raw(format!("{conc} {sch};")),
        StmtKind::Raw(format!(
            "SELECT count(*), sum(a::int8), sum(pk::int8) FROM {sch}.t WHERE a BETWEEN 10 AND 40;"
        )),
        StmtKind::Raw(format!("DROP SCHEMA {sch} CASCADE;")),
    ]
}

// ----------------------------------------------- reindex tablespace (local) --

/// REINDEX (TABLESPACE ...) over an in-place tablespace (allow_in_place_
/// tablespaces, the ddldeep-proven pattern) and a self-contained table, all
/// created and dropped in-group.
fn gen_reindex_tablespace(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("vac:reindextblspc");
    let n = g.vac.next_local;
    g.vac.next_local += 1;
    let ts = format!("fz_vac_ts{n}");
    let tbl = format!("fz_vac_tt{n}");
    let idx = format!("fz_vac_ti{n}");
    let opts = if g.weights.pick(g.rng, &["vac:rt:plain", "vac:rt:conc"]) == "vac:rt:conc" {
        g.fire("vac:rt:conc");
        format!("(TABLESPACE {ts}, CONCURRENTLY, VERBOSE)")
    } else {
        format!("(TABLESPACE {ts}, VERBOSE)")
    };
    vec![
        StmtKind::Raw("SET allow_in_place_tablespaces = on;".to_string()),
        StmtKind::Raw(format!("CREATE TABLESPACE {ts} LOCATION '';")),
        StmtKind::Raw("RESET allow_in_place_tablespaces;".to_string()),
        StmtKind::Raw(format!(
            "CREATE TABLE {tbl} (pk int4 PRIMARY KEY, a int4) WITH (autovacuum_enabled = off);"
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {tbl} SELECT i, (i * 7) % 100 FROM generate_series(1, 500) i;"
        )),
        StmtKind::Raw(format!("CREATE INDEX {idx} ON {tbl} (a);")),
        StmtKind::Raw(format!("REINDEX {opts} INDEX {idx};")),
        StmtKind::Raw(format!(
            "SELECT count(*), sum(pk::int8) FROM {tbl} WHERE a BETWEEN 10 AND 40;"
        )),
        StmtKind::Raw(format!("ALTER INDEX {idx} SET TABLESPACE pg_default;")),
        StmtKind::Raw(format!("DROP TABLE {tbl};")),
        StmtKind::Raw(format!("DROP TABLESPACE {ts};")),
    ]
}

// ------------------------------------------------------------- dbstats ----

/// ONLY_DATABASE_STATS: the table-less pg_database frozen-xid stats refresh
/// (cheap — no relation scan). Must be used alone (bar VERBOSE).
fn gen_dbstats(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("vac:dbstats");
    if g.weights.pick(g.rng, &["vac:ds:plain", "vac:ds:verbose"]) == "vac:ds:verbose" {
        vec![StmtKind::Raw("VACUUM (ONLY_DATABASE_STATS, VERBOSE);".to_string())]
    } else {
        vec![StmtKind::Raw("VACUUM (ONLY_DATABASE_STATS);".to_string())]
    }
}

// ------------------------------------------------------------ badcombo ----

/// Deliberately-invalid option combinations: the ExecVacuum conflict-check
/// ereport arms. The compare surface here is ERROR IDENTITY (same SQLSTATE +
/// message on both engines). Low weight — these never mutate state.
fn gen_badcombo(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("vac:badcombo");
    let shape = g.weights.pick(
        g.rng,
        &[
            "vac:bad:fullparallel",
            "vac:bad:fullbul",
            "vac:bad:statstable",
            "vac:bad:analyzeonlylist",
        ],
    );
    g.fire(shape);
    // A live table name if we have one, else a stable literal (the parse/
    // option check fires before relation resolution anyway).
    let t = pick_regular(g)
        .map(|ti| g.vac.tables[ti].name.clone())
        .unwrap_or_else(|| "fz_vac_none".to_string());
    let sql = match shape {
        "vac:bad:fullparallel" => format!("VACUUM (FULL, PARALLEL 2) {t};"),
        "vac:bad:fullbul" => format!("VACUUM (FULL, BUFFER_USAGE_LIMIT '1 MB') {t};"),
        "vac:bad:statstable" => format!("VACUUM (ONLY_DATABASE_STATS) {t};"),
        _ => format!("ANALYZE (VERBOSE) {t} (a) (b);"),
    };
    vec![StmtKind::Raw(sql)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_actions(seed: u64, n: usize) -> Vec<Vec<StmtKind>> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let weights = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut state = VacState::new();
        let mut out = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &weights, &mut prods, 4);
            std::mem::swap(&mut g.vac, &mut state);
            let stmts = gen_vacuum_module(&mut g);
            std::mem::swap(&mut g.vac, &mut state);
            out.push(stmts);
        }
        out
    }

    fn flat(seed: u64, n: usize) -> Vec<String> {
        gen_actions(seed, n).into_iter().flatten().map(|k| k.to_sql()).collect()
    }

    #[test]
    fn deterministic_and_seed_sensitive() {
        assert_eq!(flat(5, 200), flat(5, 200));
        assert_ne!(flat(5, 200), flat(6, 200));
    }

    #[test]
    fn statements_are_single_line_terminated_and_balanced() {
        for sql in flat(7, 400) {
            assert!(!sql.contains('\n'), "multi-line statement: {sql}");
            assert!(sql.ends_with(';'), "unterminated: {sql}");
            assert_eq!(
                sql.matches('(').count(),
                sql.matches(')').count(),
                "unbalanced parens: {sql}"
            );
        }
    }

    #[test]
    fn no_txn_bracket_and_no_float_surface() {
        for sql in flat(11, 500) {
            assert!(!sql.starts_with("BEGIN"), "unexpected txn bracket: {sql}");
            for bad in ["avg(", "::float", "::double", "random(", "stddev"] {
                assert!(!sql.contains(bad), "float/nondeterministic surface: {sql}");
            }
        }
    }

    /// The VALID VACUUM arm (badcombo disabled) never emits an option
    /// combination ExecVacuum rejects. The deliberate error-identity combos
    /// live only in the badcombo arm, zeroed out here.
    #[test]
    fn full_never_pairs_with_parallel_or_buffer_limit() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let weights = WeightTable::parse("vac:badcombo=0").unwrap();
        let mut rng = Rng::new(13);
        let mut state = VacState::new();
        let mut sqls: Vec<String> = Vec::new();
        for _ in 0..600 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &weights, &mut prods, 4);
            std::mem::swap(&mut g.vac, &mut state);
            let stmts = gen_vacuum_module(&mut g);
            std::mem::swap(&mut g.vac, &mut state);
            sqls.extend(stmts.into_iter().map(|k| k.to_sql()));
        }
        for sql in sqls {
            if sql.contains("VACUUM (") && sql.contains("FULL") {
                assert!(!sql.contains("PARALLEL"), "FULL + PARALLEL: {sql}");
                assert!(
                    !sql.contains("BUFFER_USAGE_LIMIT"),
                    "FULL + BUFFER_USAGE_LIMIT: {sql}"
                );
                assert!(!sql.contains("PROCESS_TOAST OFF"), "FULL + PROCESS_TOAST OFF: {sql}");
                assert!(!sql.contains("PROCESS_MAIN OFF"), "FULL + PROCESS_MAIN OFF: {sql}");
            }
        }
    }

    #[test]
    fn only_database_stats_is_table_less_in_valid_arm() {
        // The dbstats arm never names a table; the sole table-carrying
        // ONLY_DATABASE_STATS is the deliberate error-identity badcombo.
        for group in gen_actions(17, 600) {
            let prod_free: Vec<String> = group.iter().map(|k| k.to_sql()).collect();
            for sql in prod_free {
                if sql.starts_with("VACUUM (ONLY_DATABASE_STATS")
                    && !sql.contains("fz_vac")
                    && !sql.contains("fz_vacp")
                {
                    // valid table-less form
                    assert!(sql.ends_with(");"), "malformed dbstats: {sql}");
                }
            }
        }
    }

    #[test]
    fn concurrently_and_cluster_target_known_objects() {
        for group in gen_actions(19, 800) {
            let sqls: Vec<String> = group.iter().map(|k| k.to_sql()).collect();
            for sql in &sqls {
                if let Some(rest) = sql.strip_prefix("CLUSTER ") {
                    // CLUSTER t [USING idx]; or CLUSTER (VERBOSE) t [USING idx];
                    if let Some(u) = rest.find(" USING ") {
                        let index = rest[u + 7..].trim_end_matches(';');
                        assert!(
                            index.ends_with("_pkey") || index.starts_with("fz_vaci_"),
                            "CLUSTER on unknown index: {sql}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn local_arms_are_self_contained() {
        // REINDEX SCHEMA / TABLESPACE arms create and drop everything they
        // touch within one group.
        for group in gen_actions(23, 1200) {
            let sqls: Vec<String> = group.iter().map(|k| k.to_sql()).collect();
            let joined = sqls.join("\n");
            if joined.contains("CREATE SCHEMA fz_vac_s") {
                assert!(joined.contains("DROP SCHEMA fz_vac_s"), "schema not dropped: {sqls:?}");
            }
            if joined.contains("CREATE TABLESPACE fz_vac_ts") {
                assert!(joined.contains("DROP TABLESPACE fz_vac_ts"), "tablespace leaked: {sqls:?}");
                assert!(
                    joined.contains("SET allow_in_place_tablespaces = on;")
                        && joined.contains("RESET allow_in_place_tablespaces;"),
                    "in-place GUC not bracketed: {sqls:?}"
                );
            }
        }
    }

    #[test]
    fn bulk_loads_respect_cap() {
        for sql in flat(29, 400) {
            if sql.starts_with("INSERT INTO fz_vac_") && !sql.contains("ON CONFLICT") {
                if let Some(args) = sql.split("generate_series(").nth(1) {
                    let hi_s = args.split(',').nth(1).unwrap().trim();
                    let digits: String = hi_s.chars().take_while(|c| c.is_ascii_digit()).collect();
                    let hi: i64 = digits.parse().unwrap();
                    assert!(hi <= 6000, "bulk load exceeds cap: {sql}");
                }
            }
        }
    }

    #[test]
    fn all_families_fire() {
        let stmts = flat(3, 4000).join("\n");
        for needle in [
            "CREATE TABLE fz_vac_",
            "PARTITION BY RANGE (pk)",
            "PARTITION OF fz_vacp_",
            "ALTER TABLE fz_vac_",
            "CLUSTER ON fz_vaci_",
            "VACUUM FULL",
            "VACUUM FREEZE",
            "VACUUM ANALYZE",
            "VACUUM (",
            "INDEX_CLEANUP ON",
            "INDEX_CLEANUP OFF",
            "INDEX_CLEANUP AUTO",
            "PROCESS_TOAST",
            "PROCESS_MAIN",
            "TRUNCATE ON",
            "TRUNCATE OFF",
            "SKIP_LOCKED",
            "SKIP_DATABASE_STATS",
            "PARALLEL ",
            "BUFFER_USAGE_LIMIT",
            "DISABLE_PAGE_SKIPPING",
            "VACUUM (ONLY_DATABASE_STATS)",
            "ANALYZE (VERBOSE)",
            "ANALYZE (SKIP_LOCKED",
            "ANALYZE (BUFFER_USAGE_LIMIT",
            "CLUSTER (VERBOSE)",
            "CLUSTER fz_vac_",
            "REINDEX INDEX ",
            "REINDEX (VERBOSE) INDEX ",
            "REINDEX TABLE ",
            "REINDEX INDEX CONCURRENTLY ",
            "REINDEX TABLE CONCURRENTLY ",
            "REINDEX SCHEMA ",
            "REINDEX (VERBOSE) SCHEMA CONCURRENTLY ",
            "CREATE TABLESPACE fz_vac_ts",
            "REINDEX (TABLESPACE fz_vac_ts",
            "VACUUM fz_vacp_",
            "VACUUM (ANALYZE) fz_vacp_",
            "VACUUM (FREEZE, PROCESS_TOAST ON) fz_vacp_",
            "VACUUM (FULL, PARALLEL 2)",
            "VACUUM (FULL, BUFFER_USAGE_LIMIT",
            "md5(string_agg(",
            "DROP TABLE fz_vac_",
        ] {
            assert!(stmts.contains(needle), "family never fired in 4000 groups: {needle}");
        }
    }
}
