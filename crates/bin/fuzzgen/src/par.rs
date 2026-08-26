//! Parallel-query exercise module (Q1): the sql-reachable-queue rank-1
//! `parallel-query` chunk (401 fns / 6345 lines: radixtree.h shared
//! TidStore, nodeHash.c parallel hash, gininsert.c/brin.c/nbtsort.c
//! parallel index builds, vacuumparallel.c, execParallel.c,
//! nodeGatherMerge.c, sharedtuplestore.c, buffile/fileset spills, numeric/
//! interval serial+combine aggregate arms). The rig's C-parity GUC pin
//! (runner::C_PARITY_GUC_PIN) keeps both engines at C-default parallel
//! costing, so at default settings the planner never engages workers on
//! fuzz-sized tables — this module force-engages them with explicit GUC
//! brackets applied IDENTICALLY on both sides.
//!
//! Two forcing routes, both bracket-scoped inside one statement group:
//!   - cost route: parallel_setup_cost=0 + parallel_tuple_cost=0 +
//!     min_parallel_table_scan_size=0 (+ min_parallel_index_scan_size=0
//!     for index scans) — real multi-worker plans (Workers Planned: 2
//!     under the pinned max_parallel_workers_per_gather=2).
//!   - debug route: debug_parallel_query=on — a single-worker Gather over
//!     ordinary statements, pushing whole plans through the worker
//!     serialization/launch/instrumentation paths (execParallel.c,
//!     readfuncs), including parallel-restricted shapes the cost route
//!     never gathers (e.g. array_agg runs below the Gather here).
//! Every bracket SET has its RESET in the same group; the diffrunner's
//! GucPinned wrapper re-applies the C-parity pin after each RESET on BOTH
//! sides identically, so the pin invariant survives the brackets.
//!
//! Determinism laws honored (worker row order is nondeterministic):
//!   - every row-returning parallel SELECT is order-normalized: top-level
//!     ORDER BY pk (the primary key — a total order) or ORDER BY
//!     k_int, pk (still total); everything else is aggregate-only or
//!     GROUP BY x ORDER BY x with exact-typed (int/numeric/interval)
//!     aggregates whose results are accumulation-order-independent. No
//!     float aggregates (B1: plan-dependent accumulation order).
//!   - ANALYZE determinism: tables stay <= 24000 rows, under the
//!     default_statistics_target=100 sample of 30000 rows, so ANALYZE
//!     sees every row — identical stats, identical plans on both sides.
//!   - EXPLAIN probes are COSTS OFF and never ANALYZE (Workers Launched
//!     is runtime state; Workers Planned in the plan tree IS compared —
//!     under the C-parity pin a worker-count divergence is signal).
//!
//! Every statement family below was hand-verified on both engines before
//! the module landed (scratchpad/q1-handprobe.sql, 2026-08-11): both carry
//! debug_parallel_query and the parallel cost/maintenance GUCs, EXPLAIN
//! (ANALYZE) showed `Workers Launched: 2` on both sides for the cost-route
//! scan/join/agg shapes, parallel btree/BRIN/GIN index builds engage
//! (`parallel workers = 2` per the build paths' worker-count rule), and
//! VACUUM (PARALLEL 2) accepts the two-index tables.
//!
//! Data model: `fz_par_N` tables, one live at a time (they are big).
//!   pk int4 PRIMARY KEY, k_int int4 (0..500), k2 int4 (0..97, the join
//!   key), num numeric, ival interval, txt text — every value a pure
//!   integer formula of pk (identical on both sides by construction).
//!   Two secondary btree indexes from birth (k_int, k2) make every table
//!   VACUUM (PARALLEL)-eligible; par:idxbuild adds parallel-built btree/
//!   BRIN/GIN indexes on top. par:massdel deletes a contiguous pk band
//!   (the dead-tid fuel for the shared-TidStore parallel vacuum);
//!   par:vacuum consumes it with VACUUM (PARALLEL 2) under a small
//!   maintenance_work_mem.

use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

/// One live table at a time: a create group is a ~24k-row bulk load plus
/// three index builds plus ANALYZE.
const MAX_LIVE_TABLES: usize = 1;

/// Data-formula moduli (shared with the predicate literal pools — queries
/// must select real rows).
const KINT_M: u64 = 500; // k_int in 0..500
const K2_M: u64 = 97; // k2 in 0..97 (join key: ~fanout rows/97 per value)
const NUM_M: u64 = 100000; // num = ((i*13) % 100000) / 100  (numeric(7,2)-ish)
const TXT_M: u64 = 1009; // txt suffix in 0..1009
const IVAL_M: u64 = 50000; // ival minutes in 0..50000

fn row_source(lo: i64, hi: i64) -> String {
    format!(
        "SELECT i, (i * 13) % {KINT_M}, (i * 31) % {K2_M}, \
         (((i * 13) % {NUM_M})::numeric) / 100, \
         ((i * 37) % {IVAL_M}) * interval '1 minute', \
         'p' || ((i * 23) % {TXT_M}) \
         FROM generate_series({lo}, {hi}) i"
    )
}

#[derive(Clone, Debug)]
pub struct ParTable {
    pub name: String,
    pub live: bool,
    /// Highest pk ever inserted (recycle inserts go above it).
    pub next_pk: i64,
    /// Contiguous deleted-and-not-yet-vacuumed pk bands (massdel pushes,
    /// vacuum drains): the dead-tid bookkeeping that prices the parallel
    /// vacuum, and the conflict-free source for recycle re-inserts.
    pub dead_bands: Vec<(i64, i64)>,
    /// Extra parallel-built indexes (par:idxbuild), dropped only with the
    /// table.
    pub extra_indexes: Vec<String>,
}

/// Session-persistent parallel-table model (swapped in and out of `Gen`
/// by the session loop exactly like `IdxState`).
#[derive(Clone, Debug, Default)]
pub struct ParState {
    pub tables: Vec<ParTable>,
    next_table: u32,
    next_index: u32,
    events: Vec<DdlEvent>,
}

impl ParState {
    pub fn new() -> ParState {
        ParState::default()
    }

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
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_par_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("par");
    let action = g.weights.pick(
        g.rng,
        &[
            "par:create",
            "par:drop",
            "par:agg",
            "par:join",
            "par:scan",
            "par:gm",
            "par:debug",
            "par:idxbuild",
            "par:massdel",
            "par:vacuum",
            "par:explain",
        ],
    );
    match action {
        "par:create" => gen_create(g),
        "par:drop" => gen_drop(g),
        "par:agg" => gen_agg(g),
        "par:join" => gen_join(g),
        "par:scan" => gen_scan(g),
        "par:gm" => gen_gather_merge(g),
        "par:debug" => gen_debug(g),
        "par:idxbuild" => gen_idxbuild(g),
        "par:massdel" => gen_massdel(g),
        "par:vacuum" => gen_vacuum(g),
        _ => gen_explain(g),
    }
}

// ------------------------------------------------------------ brackets ----

/// Cost-route bracket: zero the parallel engagement thresholds around
/// `body`. `index_scans` also zeroes min_parallel_index_scan_size (the
/// parallel index/index-only/bitmap scan gate). SET and RESET live in one
/// statement group — both sides always see identical GUC state, and the
/// GucPinned wrapper restores the C-parity pin after the RESETs.
fn cost_bracket(body: Vec<StmtKind>, index_scans: bool) -> Vec<StmtKind> {
    let mut stmts = vec![
        StmtKind::Raw("SET parallel_setup_cost = 0;".to_string()),
        StmtKind::Raw("SET parallel_tuple_cost = 0;".to_string()),
        StmtKind::Raw("SET min_parallel_table_scan_size = 0;".to_string()),
    ];
    if index_scans {
        stmts.push(StmtKind::Raw("SET min_parallel_index_scan_size = 0;".to_string()));
    }
    stmts.extend(body);
    if index_scans {
        stmts.push(StmtKind::Raw("RESET min_parallel_index_scan_size;".to_string()));
    }
    stmts.push(StmtKind::Raw("RESET min_parallel_table_scan_size;".to_string()));
    stmts.push(StmtKind::Raw("RESET parallel_tuple_cost;".to_string()));
    stmts.push(StmtKind::Raw("RESET parallel_setup_cost;".to_string()));
    stmts
}

/// Debug-route bracket: debug_parallel_query=on forces a single-worker
/// Gather over any parallel-safe statement (execParallel serialization +
/// worker execution of ordinary plans).
fn debug_bracket(body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut stmts = vec![StmtKind::Raw("SET debug_parallel_query = on;".to_string())];
    stmts.extend(body);
    stmts.push(StmtKind::Raw("RESET debug_parallel_query;".to_string()));
    stmts
}

// -------------------------------------------------------------- create ----

fn gen_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.par.live_tables().len() >= MAX_LIVE_TABLES {
        g.fire("par:cap:tables");
        return gen_drop(g);
    }
    g.fire("par:create");
    let name = format!("fz_par_{}", g.par.next_table);
    g.par.next_table += 1;
    // Sizes stay under the 30000-row default_statistics_target sample so
    // ANALYZE is exhaustive (deterministic stats/plans); big enough that
    // parallel scans/builds move real data through the shared queues.
    let rows = match g.weights.pick(g.rng, &["par:rows:8000", "par:rows:16000", "par:rows:24000"])
    {
        "par:rows:8000" => 8000,
        "par:rows:16000" => 16000,
        _ => 24000,
    };
    let i1 = format!("fz_pari_{}", g.par.next_index);
    let i2 = format!("fz_pari_{}", g.par.next_index + 1);
    g.par.next_index += 2;
    let stmts = vec![
        // autovacuum_enabled = off (round-14, seed 1270990599324922574;
        // same rule as exd's RB-15 pin): the bulk INSERT alone crosses the
        // insert-autovacuum threshold, and a one-engine autoanalyze between
        // A's and B's execution flips a later compared EXPLAIN plan (the
        // fz_par_0 self-join row-count 14-vs-12 finding). Stats must change
        // only via the batch's own ANALYZE.
        StmtKind::Raw(format!(
            "CREATE TABLE {name} (pk int4 PRIMARY KEY, k_int int4, k2 int4, \
             num numeric, ival interval, txt text) WITH (autovacuum_enabled = off);"
        )),
        StmtKind::Raw(format!("INSERT INTO {name} {};", row_source(1, rows))),
        // Two secondary indexes from birth: every live table is VACUUM
        // (PARALLEL)-eligible (>= 2 non-pk indexes once the pk index is
        // counted; the PARALLEL gate wants >= 2 indexes total).
        StmtKind::Raw(format!("CREATE INDEX {i1} ON {name} (k_int);")),
        StmtKind::Raw(format!("CREATE INDEX {i2} ON {name} (k2);")),
        StmtKind::Raw(format!("ANALYZE {name};")),
    ];
    g.par.tables.push(ParTable {
        name: name.clone(),
        live: true,
        next_pk: rows,
        dead_bands: Vec::new(),
        extra_indexes: vec![i1, i2],
    });
    g.par.events.push(DdlEvent { table: name, pk: "pk".to_string(), kind: DdlEventKind::Created });
    stmts
}

fn gen_drop(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.par.live_tables();
    if live.is_empty() {
        // Never recurses: an empty population is below the cap by definition.
        g.fire("par:fallback:create");
        return gen_create(g);
    }
    g.fire("par:drop");
    let ti = live[g.rng.below_usize(live.len())];
    let name = g.par.tables[ti].name.clone();
    g.par.tables[ti].live = false;
    g.par.events.push(DdlEvent {
        table: name.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![StmtKind::Raw(format!("DROP TABLE {};", name))]
}

fn pick_live(g: &mut Gen) -> Option<usize> {
    let live = g.par.live_tables();
    if live.is_empty() {
        return None;
    }
    Some(live[g.rng.below_usize(live.len())])
}

// -------------------------------------------------------------- predicates -

/// A selective-but-nonempty predicate from the data-formula pools.
fn pred(g: &mut Gen) -> String {
    match g.rng.below(3) {
        0 => {
            let a = g.rng.below(KINT_M - 100);
            format!("k_int BETWEEN {a} AND {}", a + 20 + g.rng.below(120))
        }
        1 => format!("k2 = {}", g.rng.below(K2_M)),
        _ => format!("txt < 'p{}'", g.rng.below(TXT_M)),
    }
}

// ----------------------------------------------------------------- agg ----

/// Forced parallel aggregation: partial/finalize paths plus the numeric /
/// int8 / interval serialize+combine arms (numeric.c 18 fns,
/// timestamp.c interval_avg_* combine arms). Exact-typed aggregates only —
/// results are accumulation-order-independent, so plain compare is fair.
fn gen_agg(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("par:fallback:create");
        return gen_create(g);
    };
    g.fire("par:agg");
    let t = g.par.tables[ti].name.clone();
    let shape = g.weights.pick(g.rng, &["par:agg:plain", "par:agg:group", "par:agg:filtered"]);
    g.fire(shape);
    // cardinality(array_agg())/length(string_agg()) reduce the order-
    // nondeterministic parallel-combined aggregates to order-independent
    // scalars: the array_agg/string_agg serialize+combine arms
    // (array_userfuncs.c, varlena.c) run, the compare stays fair.
    let aggs = "count(*), sum(k_int), min(pk), max(pk), sum(num), avg(num), \
                var_samp(num), stddev_samp(num), avg(ival), sum(ival), \
                avg(k_int::int8), sum(pk::int8), cardinality(array_agg(k2)), \
                length(string_agg(txt, ','))";
    let body = match shape {
        "par:agg:plain" => format!("SELECT {aggs} FROM {t};"),
        "par:agg:group" => {
            format!("SELECT k2, {aggs} FROM {t} GROUP BY k2 ORDER BY k2;")
        }
        _ => {
            let p = pred(g);
            format!("SELECT {aggs} FROM {t} WHERE {p};")
        }
    };
    cost_bracket(vec![StmtKind::Raw(body)], false)
}

// ---------------------------------------------------------------- join ----

/// Forced parallel hash join (shared build). The nestloop/mergejoin-off
/// sub-bracket forces the hash path; the work_mem variant forces
/// multi-batch shared builds (sharedtuplestore + fileset/buffile spill
/// files, the nodeHashjoin batch barriers).
fn gen_join(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("par:fallback:create");
        return gen_create(g);
    };
    g.fire("par:join");
    let t = g.par.tables[ti].name.clone();
    let spill = g.weights.pick(g.rng, &["par:join:spill", "par:join:mem"]) == "par:join:spill";
    if spill {
        g.fire("par:join:spill");
    }
    let shape = g.weights.pick(g.rng, &["par:join:count", "par:join:agg", "par:join:rows"]);
    g.fire(shape);
    let p = pred(g);
    let body = match shape {
        "par:join:count" => format!(
            "SELECT count(*) FROM {t} a JOIN {t} b ON a.k2 = b.k2 AND a.pk < b.pk WHERE a.{p};"
        ),
        "par:join:agg" => format!(
            "SELECT count(*), sum(a.k_int + b.k_int), min(a.pk), max(b.pk) \
             FROM {t} a JOIN {t} b ON a.k2 = b.k2 WHERE a.{p} AND b.k_int < 250;"
        ),
        _ => format!(
            "SELECT a.pk, b.pk FROM {t} a JOIN {t} b ON a.k2 = b.k2 \
             WHERE a.{p} AND b.txt < 'p200' ORDER BY a.pk, b.pk LIMIT 100;"
        ),
    };
    let mut inner = vec![
        StmtKind::Raw("SET enable_nestloop TO off;".to_string()),
        StmtKind::Raw("SET enable_mergejoin TO off;".to_string()),
    ];
    if spill {
        inner.push(StmtKind::Raw("SET work_mem = '64kB';".to_string()));
    }
    inner.push(StmtKind::Raw(body));
    if spill {
        inner.push(StmtKind::Raw("RESET work_mem;".to_string()));
    }
    inner.push(StmtKind::Raw("RESET enable_mergejoin;".to_string()));
    inner.push(StmtKind::Raw("RESET enable_nestloop;".to_string()));
    cost_bracket(inner, false)
}

// ---------------------------------------------------------------- scan ----

/// Forced parallel index / index-only / bitmap heap scans (the nodeIndexscan
/// / nodeIndexonlyscan / nodeBitmapHeapscan parallel arms). Aggregate-only
/// projections: no row-order exposure at all.
fn gen_scan(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("par:fallback:create");
        return gen_create(g);
    };
    g.fire("par:scan");
    let t = g.par.tables[ti].name.clone();
    let a = g.rng.below(KINT_M - 200);
    let b = a + 50 + g.rng.below(200);
    let shape = g.weights.pick(g.rng, &["par:scan:idx", "par:scan:ionly", "par:scan:bitmap"]);
    g.fire(shape);
    let (body, extra_off) = match shape {
        // Parallel index scan: seqscan+bitmap off leaves btree amgettuple.
        "par:scan:idx" => (
            format!("SELECT count(*), sum(pk::int8) FROM {t} WHERE k_int BETWEEN {a} AND {b};"),
            vec!["enable_seqscan", "enable_bitmapscan"],
        ),
        // Parallel index-only scan: the k_int index covers the projection
        // (VACUUM elsewhere in the stream keeps the VM populated; heap
        // fetches are correct either way).
        "par:scan:ionly" => (
            format!("SELECT count(*), sum(k_int::int8) FROM {t} WHERE k_int BETWEEN {a} AND {b};"),
            vec!["enable_seqscan", "enable_bitmapscan"],
        ),
        // Parallel bitmap heap scan: seqscan+indexscan off leaves the
        // bitmap path (the leader builds the TIDBitmap, workers share it).
        _ => (
            format!("SELECT count(*), sum(k2) FROM {t} WHERE k_int BETWEEN {a} AND {b};"),
            vec!["enable_seqscan", "enable_indexscan"],
        ),
    };
    let mut inner: Vec<StmtKind> = extra_off
        .iter()
        .map(|guc| StmtKind::Raw(format!("SET {guc} TO off;")))
        .collect();
    inner.push(StmtKind::Raw(body));
    for guc in extra_off.iter().rev() {
        inner.push(StmtKind::Raw(format!("RESET {guc};")));
    }
    cost_bracket(inner, true)
}

// ------------------------------------------------------------- gather-merge

/// Gather Merge: parallel input sorted per-worker, merged ordered in the
/// leader (nodeGatherMerge worker arms + binaryheap merge). ORDER BY
/// (k_int, pk) is a total order — strict ordered compare stays fair.
fn gen_gather_merge(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("par:fallback:create");
        return gen_create(g);
    };
    g.fire("par:gm");
    let t = g.par.tables[ti].name.clone();
    let p = pred(g);
    let n = 50 + g.rng.below(150);
    let body =
        format!("SELECT pk, k_int FROM {t} WHERE {p} ORDER BY k_int, pk LIMIT {n};");
    cost_bracket(vec![StmtKind::Raw(body)], false)
}

// --------------------------------------------------------------- debug ----

/// debug_parallel_query route: ordinary statements forced through a
/// one-worker Gather — plan-tree serialization (readfuncs.funcs.c worker
/// side), worker snapshot/param transport, and parallel-restricted
/// aggregates (array_agg/string_agg run below the Gather since the whole
/// plan moves to the worker).
fn gen_debug(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("par:fallback:create");
        return gen_create(g);
    };
    g.fire("par:debug");
    let t = g.par.tables[ti].name.clone();
    let shape = g.weights.pick(
        g.rng,
        &[
            "par:debug:rows",
            "par:debug:arrayagg",
            "par:debug:distinct",
            "par:debug:winagg",
            "par:debug:mjoin",
            "par:debug:memoize",
        ],
    );
    g.fire(shape);
    let p = pred(g);
    match shape {
        // Forced merge join / memoized nestloop under the Gather: widens
        // the worker-side plan-tree read surface (readfuncs.funcs.c) to
        // node types the cost route never serializes.
        "par:debug:mjoin" => {
            let body = format!(
                "SELECT a.pk, b.pk FROM {t} a JOIN {t} b ON a.k2 = b.k2 \
                 WHERE a.{p} AND b.pk <= 500 ORDER BY a.pk, b.pk LIMIT 100;"
            );
            let mut inner = vec![
                StmtKind::Raw("SET enable_hashjoin TO off;".to_string()),
                StmtKind::Raw("SET enable_nestloop TO off;".to_string()),
                StmtKind::Raw(body),
                StmtKind::Raw("RESET enable_nestloop;".to_string()),
                StmtKind::Raw("RESET enable_hashjoin;".to_string()),
            ];
            inner = debug_bracket(inner);
            return inner;
        }
        "par:debug:memoize" => {
            let body = format!(
                "SELECT count(*), sum(b.k_int) FROM {t} a JOIN {t} b ON a.k2 = b.k2 \
                 WHERE a.pk <= 400;"
            );
            let mut inner = vec![
                StmtKind::Raw("SET enable_hashjoin TO off;".to_string()),
                StmtKind::Raw("SET enable_mergejoin TO off;".to_string()),
                StmtKind::Raw(body),
                StmtKind::Raw("RESET enable_mergejoin;".to_string()),
                StmtKind::Raw("RESET enable_hashjoin;".to_string()),
            ];
            inner = debug_bracket(inner);
            return inner;
        }
        _ => {}
    }
    let body = match shape {
        "par:debug:rows" => {
            let n = 50 + g.rng.below(100);
            format!("SELECT pk, k_int, num FROM {t} WHERE {p} ORDER BY pk LIMIT {n};")
        }
        // array_agg/string_agg over a pk-ordered subquery: deterministic
        // aggregate input order, exercised under the forced Gather.
        "par:debug:arrayagg" => format!(
            "SELECT array_agg(k_int ORDER BY pk), string_agg(txt, ',' ORDER BY pk) \
             FROM (SELECT k_int, txt, pk FROM {t} WHERE {p} ORDER BY pk LIMIT 200) s;"
        ),
        "par:debug:distinct" => {
            format!("SELECT DISTINCT k2 FROM {t} WHERE {p} ORDER BY k2;")
        }
        _ => format!(
            "SELECT k2, sum(k_int) FROM {t} WHERE {p} GROUP BY k2 ORDER BY k2 LIMIT 40;"
        ),
    };
    debug_bracket(vec![StmtKind::Raw(body)])
}

// ------------------------------------------------------------- idxbuild ---

/// Parallel index builds: btree (nbtsort.c parallel arms + parallel
/// tuplesort spills under a small maintenance_work_mem), BRIN
/// (brin.c _brin_*_parallel), GIN (gininsert.c parallel arms, PG18).
/// min_parallel_table_scan_size=0 makes plan_create_index_workers grant
/// workers at fuzz table sizes; max_parallel_maintenance_workers=2 pins
/// the same worker budget on both sides.
fn gen_idxbuild(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("par:fallback:create");
        return gen_create(g);
    };
    g.fire("par:idxbuild");
    let t = g.par.tables[ti].name.clone();
    let am = g.weights.pick(g.rng, &["par:ib:btree", "par:ib:brin", "par:ib:gin"]);
    g.fire(am);
    let iname = format!("fz_pari_{}", g.par.next_index);
    g.par.next_index += 1;
    let create = match am {
        "par:ib:btree" => match g.rng.below(3) {
            0 => format!("CREATE INDEX {iname} ON {t} (k_int, pk);"),
            1 => format!("CREATE INDEX {iname} ON {t} (txt);"),
            _ => format!("CREATE INDEX {iname} ON {t} (num);"),
        },
        "par:ib:brin" => format!("CREATE INDEX {iname} ON {t} USING brin (k_int, pk);"),
        _ => format!("CREATE INDEX {iname} ON {t} USING gin ((ARRAY[k_int, k2]));"),
    };
    // plan_create_index_workers demotes the worker request until
    // maintenance_work_mem / (workers + 1) >= 32MB, so a small mwm forces
    // a SERIAL build (witnessed: 2MB built serially on both engines).
    // 96MB grants the full 2 workers; 64MB grants 1 (leader + worker) —
    // two live parallel-build regimes, both engaging the parallel arms.
    let two_workers = g.weights.pick(g.rng, &["par:ib:w2", "par:ib:w1"]) == "par:ib:w2";
    g.fire(if two_workers { "par:ib:w2" } else { "par:ib:w1" });
    let mwm = if two_workers { "96MB" } else { "64MB" };
    let stmts = vec![
        StmtKind::Raw("SET max_parallel_maintenance_workers = 2;".to_string()),
        StmtKind::Raw("SET min_parallel_table_scan_size = 0;".to_string()),
        StmtKind::Raw(format!("SET maintenance_work_mem = '{mwm}';")),
        StmtKind::Raw(create),
        StmtKind::Raw("RESET maintenance_work_mem;".to_string()),
        StmtKind::Raw("RESET min_parallel_table_scan_size;".to_string()),
        StmtKind::Raw("RESET max_parallel_maintenance_workers;".to_string()),
    ];
    g.par.tables[ti].extra_indexes.push(iname);
    stmts
}

// -------------------------------------------------------------- massdel ---

/// Mass DELETE of a contiguous pk band: the dead-tid fuel for the shared-
/// TidStore parallel vacuum (radixtree.h node growth needs volume). The
/// band is tracked exactly, so recycle re-inserts (below) are
/// conflict-free by construction.
fn gen_massdel(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("par:fallback:create");
        return gen_create(g);
    };
    let t = g.par.tables[ti].clone();
    // Recycle first if a vacuumed band is pending re-insert; otherwise
    // delete a fresh band from the live range.
    if !t.dead_bands.is_empty() && g.rng.chance(1, 3) {
        g.fire("par:recycle");
        let (lo, hi) = g.par.tables[ti].dead_bands.remove(0);
        return vec![StmtKind::Raw(format!(
            "INSERT INTO {} {};",
            t.name,
            row_source(lo, hi)
        ))];
    }
    g.fire("par:massdel");
    // A band of 2000-6000 pks somewhere in 1..next_pk. Overlap with an
    // already-dead band just deletes fewer rows — still correct, so the
    // bands are merged conservatively via containment check.
    let span = 2000 + g.rng.below(4000) as i64;
    let max_lo = (t.next_pk - span).max(1);
    let lo = 1 + g.rng.below(max_lo as u64) as i64;
    let hi = (lo + span - 1).min(t.next_pk);
    let overlaps = t.dead_bands.iter().any(|&(a, b)| lo <= b && a <= hi);
    if !overlaps {
        g.par.tables[ti].dead_bands.push((lo, hi));
    }
    vec![StmtKind::Raw(format!(
        "DELETE FROM {} WHERE pk BETWEEN {} AND {};",
        t.name, lo, hi
    ))]
}

// --------------------------------------------------------------- vacuum ---

/// VACUUM (PARALLEL 2): parallel index vacuuming over the shared TidStore
/// (vacuumparallel.c + radixtree.h + the parallel bulkdelete arms of
/// nbtree/brin/gin). min_parallel_index_scan_size=0 makes every index
/// participate regardless of size; a small maintenance_work_mem keeps the
/// dead-tid store hot. Plain VACUUM/VACUUM ANALYZE variants keep the VM
/// populated for the index-only scan family.
fn gen_vacuum(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("par:fallback:create");
        return gen_create(g);
    };
    g.fire("par:vacuum");
    let t = g.par.tables[ti].name.clone();
    let form = g.weights.pick(g.rng, &["par:vac:parallel", "par:vac:plain", "par:vac:analyze"]);
    g.fire(form);
    match form {
        "par:vac:parallel" => {
            let n = 1 + g.rng.below(2); // PARALLEL 1 or 2
            vec![
                StmtKind::Raw("SET min_parallel_index_scan_size = 0;".to_string()),
                StmtKind::Raw("SET maintenance_work_mem = '1MB';".to_string()),
                StmtKind::Raw(format!("VACUUM (PARALLEL {n}) {t};")),
                StmtKind::Raw("RESET maintenance_work_mem;".to_string()),
                StmtKind::Raw("RESET min_parallel_index_scan_size;".to_string()),
            ]
        }
        "par:vac:plain" => vec![StmtKind::Raw(format!("VACUUM {t};"))],
        _ => vec![StmtKind::Raw(format!("VACUUM ANALYZE {t};"))],
    }
}

// -------------------------------------------------------------- explain ---

/// EXPLAIN (COSTS OFF) plan-shape probes under the cost route: verifies
/// both planners GATHER the same shapes with the same Workers Planned
/// (compared strictly — a divergence under the C-parity pin is signal,
/// not the ruled P1-A default-skew family). Never ANALYZE: Workers
/// Launched is runtime state.
fn gen_explain(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("par:fallback:create");
        return gen_create(g);
    };
    g.fire("par:explain");
    let t = g.par.tables[ti].name.clone();
    let p = pred(g);
    let shape = g.weights.pick(g.rng, &["par:ex:agg", "par:ex:join", "par:ex:gm"]);
    g.fire(shape);
    let body = match shape {
        "par:ex:agg" => format!("EXPLAIN (COSTS OFF) SELECT count(*), sum(num) FROM {t} WHERE {p};"),
        "par:ex:join" => format!(
            "EXPLAIN (COSTS OFF) SELECT count(*) FROM {t} a JOIN {t} b ON a.k2 = b.k2 WHERE a.{p};"
        ),
        _ => format!(
            "EXPLAIN (COSTS OFF) SELECT pk FROM {t} WHERE {p} ORDER BY k_int, pk LIMIT 50;"
        ),
    };
    cost_bracket(vec![StmtKind::Raw(body)], false)
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
        let mut state = ParState::new();
        let mut out = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &weights, &mut prods, 4);
            std::mem::swap(&mut g.par, &mut state);
            let stmts = gen_par_module(&mut g);
            std::mem::swap(&mut g.par, &mut state);
            out.push(stmts);
        }
        out
    }

    fn flat(seed: u64, n: usize) -> Vec<String> {
        gen_actions(seed, n)
            .into_iter()
            .flatten()
            .map(|k| k.to_sql())
            .collect()
    }

    #[test]
    fn brackets_are_balanced() {
        // Every SET in a group has a matching RESET in the SAME group, so
        // both sides always leave a group with identical GUC state.
        for group in gen_actions(7, 300) {
            let mut open: Vec<String> = Vec::new();
            for k in &group {
                let sql = k.to_sql();
                if let Some(rest) = sql.strip_prefix("SET ") {
                    let name = rest.split([' ', '=']).next().unwrap().to_string();
                    open.push(name);
                } else if let Some(rest) = sql.strip_prefix("RESET ") {
                    let name = rest.trim_end_matches(';').to_string();
                    let pos = open.iter().rposition(|n| *n == name);
                    assert!(pos.is_some(), "RESET {name} without SET in group: {sql}");
                    open.remove(pos.unwrap());
                }
            }
            assert!(open.is_empty(), "unclosed SETs at group end: {open:?}");
        }
    }

    #[test]
    fn row_returning_parallel_selects_are_order_normalized() {
        // Any SELECT emitted under a forcing bracket that projects rows
        // (not aggregate-only) must carry a top-level ORDER BY ending in
        // pk — worker row order is nondeterministic.
        for sql in flat(11, 400) {
            if !sql.starts_with("SELECT ") && !sql.starts_with("EXPLAIN") {
                continue;
            }
            if sql.starts_with("SELECT pk") || sql.contains("SELECT a.pk") {
                assert!(
                    sql.contains("ORDER BY"),
                    "row-returning parallel SELECT without ORDER BY: {sql}"
                );
            }
        }
    }

    #[test]
    fn vacuum_parallel_only_on_two_index_tables() {
        // Every VACUUM (PARALLEL n) target was created with two secondary
        // indexes (the create group emits them before any vacuum can
        // fire), and n is 1 or 2.
        let stmts = flat(13, 500);
        let mut created: Vec<String> = Vec::new();
        for sql in &stmts {
            if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
                created.push(rest.split(' ').next().unwrap().to_string());
            }
            if sql.starts_with("VACUUM (PARALLEL ") {
                let n: u32 = sql["VACUUM (PARALLEL ".len()..]
                    .chars()
                    .next()
                    .unwrap()
                    .to_digit(10)
                    .unwrap();
                assert!(n == 1 || n == 2, "bad parallel degree: {sql}");
                let t = sql.rsplit(' ').next().unwrap().trim_end_matches(';');
                assert!(created.iter().any(|c| c == t), "vacuum of unknown table: {sql}");
            }
        }
    }

    #[test]
    fn recycle_reinserts_only_vacuumed_bands() {
        // Structural: a recycle INSERT re-inserts exactly a previously
        // deleted band (pk uniqueness can never be violated). Tracked via
        // the DELETE statements' literal bands.
        let stmts = flat(29, 600);
        let mut deleted: Vec<(i64, i64)> = Vec::new();
        for sql in &stmts {
            if sql.starts_with("DELETE FROM fz_par_") {
                let tail = sql.split("BETWEEN ").nth(1).unwrap();
                let lo: i64 = tail.split(' ').next().unwrap().parse().unwrap();
                let hi: i64 =
                    tail.split("AND ").nth(1).unwrap().trim_end_matches(';').parse().unwrap();
                deleted.push((lo, hi));
            }
            if sql.starts_with("INSERT INTO fz_par_") && sql.contains("generate_series(") {
                let args = sql.split("generate_series(").nth(1).unwrap();
                let lo: i64 = args.split(',').next().unwrap().trim().parse().unwrap();
                // Initial bulk loads start at 1 with no prior delete;
                // recycle inserts must match a deleted band's lower edge.
                if lo != 1 {
                    assert!(
                        deleted.iter().any(|&(a, _)| a == lo),
                        "recycle INSERT from {lo} without matching DELETE: {sql}"
                    );
                }
            }
        }
    }

    #[test]
    fn deterministic_and_seed_sensitive() {
        assert_eq!(flat(5, 100), flat(5, 100));
        assert_ne!(flat(5, 100), flat(6, 100));
    }

    #[test]
    fn all_families_fire() {
        let stmts = flat(3, 800).join("\n");
        for needle in [
            "SET debug_parallel_query = on;",
            "SET parallel_setup_cost = 0;",
            "VACUUM (PARALLEL ",
            "SET max_parallel_maintenance_workers = 2;",
            "SET maintenance_work_mem = '96MB';",
            "SET maintenance_work_mem = '64MB';",
            "USING brin (k_int, pk)",
            "USING gin ((ARRAY[k_int, k2]))",
            "SET work_mem = '64kB';",
            "EXPLAIN (COSTS OFF)",
            "DELETE FROM fz_par_",
            "GROUP BY k2 ORDER BY k2",
            "cardinality(array_agg(k2))",
            "SET enable_hashjoin TO off;",
        ] {
            assert!(stmts.contains(needle), "family never fired in 800 groups: {needle}");
        }
    }

    /// Round-14 (seed 1270990599324922574), same rule as exd RB-15: every
    /// par CREATE TABLE pins autovacuum_enabled = off. The 8000-24000-row
    /// bulk loads cross the insert-autovacuum threshold on their own, and a
    /// one-engine autoanalyze landing between A's and B's execution flips a
    /// later compared EXPLAIN plan.
    #[test]
    fn creates_pin_autovacuum_off() {
        let mut seen = 0;
        for sql in flat(21, 600) {
            if sql.starts_with("CREATE TABLE ") {
                assert!(
                    sql.contains("autovacuum_enabled = off"),
                    "par fixture does not pin autovacuum off: `{sql}`"
                );
                seen += 1;
            }
        }
        assert!(seen > 0, "no CREATE TABLE generated in 600 statements");
    }

    #[test]
    fn analyze_determinism_row_cap() {
        // Bulk loads never exceed the 30000-row exhaustive ANALYZE sample.
        for sql in flat(17, 400) {
            if sql.starts_with("INSERT INTO fz_par_") {
                let args = sql.split("generate_series(").nth(1).unwrap();
                let tail = args.split(',').nth(1).unwrap().trim();
                let digits: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
                let hi: i64 = digits.parse().unwrap();
                assert!(hi <= 24000, "bulk load exceeds ANALYZE-exhaustive cap: {sql}");
            }
        }
    }
}
