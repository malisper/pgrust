//! Spill / alternate-execution-arm drain module (LD5): the
//! `executor-spill` chunk of docs/fuzzing/line-drain-queue.tsv (145 rows —
//! nodeAgg.c HashAgg spill + sort-fallback arms, nodeHash.c /
//! nodeHashjoin.c serial multi-batch, tuplesort.c external merge +
//! random-access tape paths, tuplestore.c spill + multi-read-pointer
//! machinery, logtape.c, nodeMaterial.c / nodeMemoize.c /
//! nodeIncrementalSort.c residue, tuplesortvariants.c CLUSTER sort). The
//! default corpus runs everything in-memory: the rig's fuzz-sized tables
//! fit any 4MB work_mem, so every spill/fallback arm is dark. This module
//! force-engages them with explicit GUC brackets (work_mem='64kB',
//! hash_mem_multiplier=1, enable_* toggles, maintenance_work_mem for
//! CLUSTER) applied IDENTICALLY on both differential sides — the spill
//! path and the in-memory path must produce identical results, so any
//! divergence here is a high-value finding.
//!
//! Determinism laws honored (same discipline as crate::par):
//!   - every row-returning statement carries a TOTAL order (ORDER BY
//!     ending in pk, or the full grouped-key list); everything else is
//!     aggregate-only with exact-typed (int/numeric/text-length)
//!     aggregates whose results are accumulation-order-independent. No
//!     float aggregates (B1).
//!   - ANALYZE determinism: tables stay <= 24000 rows, under the
//!     default_statistics_target sample of 30000 rows — exhaustive
//!     ANALYZE, identical stats, identical plans on both sides.
//!   - every bracket SET has its RESET in the same statement group; the
//!     runner's GucPinned wrapper re-applies the C-parity pin after
//!     RESETs identically on both sides.
//!   - CLUSTER/VACUUM FULL rewrite heap order, which is a non-surface
//!     (COPY order ruling): all probes and row-returning statements are
//!     explicitly ordered.
//!
//! Data model: `fz_sp_N` tables, one live at a time.
//!   pk int4 PRIMARY KEY, k_int int4 (0..500), k2 int4 (0..97, join key),
//!   kskew int4 (85% zero, else 1..1000 — the MCV fuel for the skew hash
//!   join), num numeric, txt text, pad text (~40-52 chars — widens tuples
//!   so 64kB work_mem spills after ~hundreds of rows, not thousands).
//!   Every value is a pure integer formula of pk — identical on both
//!   sides by construction. Secondary btree indexes on k_int and k2 from
//!   birth (k_int carries the incremental-sort prefix and the CLUSTER
//!   target; k2 the memoize/material join).

use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

/// One live table at a time (create groups are ~20k-row bulk loads).
const MAX_LIVE_TABLES: usize = 1;

const KINT_M: u64 = 500; // k_int in 0..500 (40+ dups per value at 20k rows)
const K2_M: u64 = 97; // k2 in 0..97 (join key, ~200 rows per value)
const NUM_M: u64 = 100000; // num = ((i*13) % 100000) / 100
const TXT_M: u64 = 1009; // txt suffix in 0..1009
const SKEW_M: u64 = 1000; // kskew nonzero values in 0..1000

fn row_source(lo: i64, hi: i64) -> String {
    format!(
        "SELECT i, (i * 13) % {KINT_M}, (i * 31) % {K2_M}, \
         CASE WHEN i % 20 < 17 THEN 0 ELSE 1 + (i * 7) % {SKEW_M} END, \
         (((i * 13) % {NUM_M})::numeric) / 100, \
         'p' || ((i * 23) % {TXT_M}), \
         repeat('ab', 20 + (i % 7)::int) \
         FROM generate_series({lo}, {hi}) i"
    )
}

#[derive(Clone, Debug)]
pub struct SpillTable {
    pub name: String,
    pub live: bool,
    /// The k_int index name (the CLUSTER target and incremental-sort
    /// prefix path).
    pub kint_index: String,
}

/// Session-persistent spill-table model (swapped in and out of `Gen` by
/// the session loop exactly like `ParState`).
#[derive(Clone, Debug, Default)]
pub struct SpillState {
    pub tables: Vec<SpillTable>,
    next_table: u32,
    next_index: u32,
    next_cursor: u32,
    events: Vec<DdlEvent>,
}

impl SpillState {
    pub fn new() -> SpillState {
        SpillState::default()
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
pub fn gen_spill_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("spill");
    let action = g.weights.pick(
        g.rng,
        &[
            "spill:create",
            "spill:drop",
            "spill:sort",
            "spill:scroll",
            "spill:hold",
            "spill:hj",
            "spill:mj",
            "spill:hashagg",
            "spill:groupagg",
            "spill:window",
            "spill:cte",
            "spill:material",
            "spill:memoize",
            "spill:incsort",
            "spill:cluster",
            "spill:hashidx",
            "spill:rescan",
            "spill:explain",
        ],
    );
    match action {
        "spill:create" => gen_create(g),
        "spill:drop" => gen_drop(g),
        "spill:sort" => gen_sort(g),
        "spill:scroll" => gen_scroll(g),
        "spill:hold" => gen_hold(g),
        "spill:hj" => gen_hashjoin(g),
        "spill:mj" => gen_mergejoin(g),
        "spill:hashagg" => gen_hashagg(g),
        "spill:groupagg" => gen_groupagg(g),
        "spill:window" => gen_window(g),
        "spill:cte" => gen_cte(g),
        "spill:material" => gen_material(g),
        "spill:memoize" => gen_memoize(g),
        "spill:incsort" => gen_incsort(g),
        "spill:cluster" => gen_cluster(g),
        "spill:hashidx" => gen_hashidx(g),
        "spill:rescan" => gen_rescan(g),
        _ => gen_explain(g),
    }
}

// ------------------------------------------------------------ brackets ----

/// Wrap `body` in SET/RESET pairs for `gucs` (name, value), RESETs in
/// reverse order, all in ONE statement group so both sides always leave
/// the group with identical GUC state.
fn bracket(gucs: &[(&str, &str)], body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut stmts: Vec<StmtKind> = gucs
        .iter()
        .map(|(n, v)| StmtKind::Raw(format!("SET {n} = {v};")))
        .collect();
    stmts.extend(body);
    for (n, _) in gucs.iter().rev() {
        stmts.push(StmtKind::Raw(format!("RESET {n};")));
    }
    stmts
}

// -------------------------------------------------------------- create ----

fn gen_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.spill.live_tables().len() >= MAX_LIVE_TABLES {
        g.fire("spill:cap:tables");
        return gen_drop(g);
    }
    g.fire("spill:create");
    let name = format!("fz_sp_{}", g.spill.next_table);
    g.spill.next_table += 1;
    // <= 24000: exhaustive ANALYZE (deterministic stats/plans); >= 8000:
    // even the "big" work_mem-bracketed shapes truly spill.
    let rows = match g.weights.pick(g.rng, &["spill:rows:8000", "spill:rows:16000", "spill:rows:20000"]) {
        "spill:rows:8000" => 8000,
        "spill:rows:16000" => 16000,
        _ => 20000,
    };
    let i1 = format!("fz_spi_{}", g.spill.next_index);
    let i2 = format!("fz_spi_{}", g.spill.next_index + 1);
    let i3 = format!("fz_spi_{}", g.spill.next_index + 2);
    g.spill.next_index += 3;
    let stmts = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {name} (pk int4 PRIMARY KEY, k_int int4, k2 int4, \
             kskew int4, num numeric, txt text, pad text);"
        )),
        StmtKind::Raw(format!("INSERT INTO {name} {};", row_source(1, rows))),
        StmtKind::Raw(format!("CREATE INDEX {i1} ON {name} (k_int);")),
        StmtKind::Raw(format!("CREATE INDEX {i2} ON {name} (k2);")),
        // kskew index: the giant-prefix-group incremental sort needs a
        // presorted path on the 85%-zero column.
        StmtKind::Raw(format!("CREATE INDEX {i3} ON {name} (kskew);")),
        StmtKind::Raw(format!("ANALYZE {name};")),
    ];
    g.spill.tables.push(SpillTable { name: name.clone(), live: true, kint_index: i1 });
    g.spill
        .events
        .push(DdlEvent { table: name, pk: "pk".to_string(), kind: DdlEventKind::Created });
    stmts
}

fn gen_drop(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.spill.live_tables();
    if live.is_empty() {
        // Never recurses: an empty population is below the cap by definition.
        g.fire("spill:fallback:create");
        return gen_create(g);
    }
    g.fire("spill:drop");
    let ti = live[g.rng.below_usize(live.len())];
    let name = g.spill.tables[ti].name.clone();
    g.spill.tables[ti].live = false;
    g.spill.events.push(DdlEvent {
        table: name.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![StmtKind::Raw(format!("DROP TABLE {};", name))]
}

fn pick_live(g: &mut Gen) -> Option<usize> {
    let live = g.spill.live_tables();
    if live.is_empty() {
        return None;
    }
    Some(live[g.rng.below_usize(live.len())])
}

macro_rules! need_table {
    ($g:expr) => {
        match pick_live($g) {
            Some(ti) => ti,
            None => {
                $g.fire("spill:fallback:create");
                return gen_create($g);
            }
        }
    };
}

// ---------------------------------------------------------------- sort ----

/// External merge sort (tuplesort.c dumptuples/mergeruns/logtape.c): full
/// sorts of the whole table under work_mem='64kB'. No LIMIT on the
/// spilling shapes (a bound flips tuplesort into the in-memory top-N
/// heap); one deliberate LIMIT variant covers the bounded-heap arms.
fn gen_sort(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:sort");
    let t = g.spill.tables[ti].name.clone();
    let shape = g.weights.pick(
        g.rng,
        &[
            "spill:sort:rows",
            "spill:sort:wrap",
            "spill:sort:desc",
            "spill:sort:bounded",
            "spill:sort:abbrev",
            "spill:sort:grow",
        ],
    );
    g.fire(shape);
    // W5 r2 (spill:sort:grow): a NARROW sort at 256kB work_mem lets the
    // memtuples array double repeatedly before the spill flips state —
    // the grow_memtuples growth/clamp edges 64kB can never reach (at 64kB
    // the first growth attempt already busts availMem).
    if shape == "spill:sort:grow" {
        let body = format!(
            "SELECT count(*), sum(x.pk::int8) FROM \
             (SELECT pk FROM {t} ORDER BY (pk * 37) % 24001, pk) x;"
        );
        return bracket(&[("work_mem", "'256kB'")], vec![StmtKind::Raw(body)]);
    }
    let body = match shape {
        // Row-returning external sort, total order.
        // Keys deliberately unindexed (txt/num): an indexed prefix flips
        // the plan to Incremental Sort (witnessed) and the full external
        // merge never runs.
        "spill:sort:rows" => {
            let key = if g.rng.chance(1, 2) { "txt, pk" } else { "num, pk" };
            format!("SELECT pk, k_int FROM {t} ORDER BY {key};")
        }
        // Count-wrapped sorted subquery: the sort runs (subquery ORDER BY
        // is preserved), nothing crosses the wire but one row.
        "spill:sort:wrap" => format!(
            "SELECT count(*), sum(pk::int8) FROM \
             (SELECT pk FROM {t} ORDER BY txt DESC, pk DESC) s;"
        ),
        "spill:sort:desc" => {
            format!("SELECT pk, k2 FROM {t} ORDER BY txt DESC, pk DESC;")
        }
        // Abbreviated-key abort: pad has only 7 distinct values, so the
        // text abbreviation cardinality collapses and consider_abort_common
        // fires the removeabbrev_* walk mid-sort.
        "spill:sort:abbrev" => format!(
            "SELECT count(*), sum(pk::int8) FROM \
             (SELECT pk FROM {t} ORDER BY pad DESC, pk) s;"
        ),
        // Bounded top-N (sort_bounded_heap arms — deliberately NOT a spill).
        _ => {
            let n = 20 + g.rng.below(80);
            format!("SELECT pk, k_int FROM {t} ORDER BY num, pk LIMIT {n};")
        }
    };
    bracket(&[("work_mem", "'64kB'")], vec![StmtKind::Raw(body)])
}

// -------------------------------------------------------------- scroll ----

/// Random-access external sort (tuplesort_gettuple_common backward/random
/// arms, tuplesort_rescan, LogicalTapeFreeze): a SCROLL cursor over a
/// spilled total-order sort, driven forward, backward and absolute inside
/// one transaction bracket. SET LOCAL scopes the work_mem to the bracket.
fn gen_scroll(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:scroll");
    let t = g.spill.tables[ti].name.clone();
    let cur = format!("fz_spc_{}", g.spill.next_cursor);
    g.spill.next_cursor += 1;
    let a = 100 + g.rng.below(900);
    let b = 10 + g.rng.below(200);
    let abs = 1000 + g.rng.below(5000);
    // W5 r2: an EMPTY spilling sort under the same scroll choreography —
    // the tuplesort_gettuple_common / tuplestore empty-scan edge lines
    // (FETCH FORWARD/BACKWARD/ABSOLUTE over zero tuples) that a populated
    // cursor can never reach.
    let pred = if g.rng.chance(1, 6) {
        g.fire("spill:scroll:emptypred");
        " WHERE pk < 0"
    } else {
        ""
    };
    let mut stmts = vec![
        StmtKind::Raw("BEGIN;".to_string()),
        StmtKind::Raw("SET LOCAL work_mem = '64kB';".to_string()),
        StmtKind::Raw(format!(
            "DECLARE {cur} SCROLL CURSOR FOR SELECT pk, k_int FROM {t}{pred} ORDER BY k_int, pk;"
        )),
        StmtKind::Raw(format!("FETCH FORWARD {a} FROM {cur};")),
        StmtKind::Raw(format!("FETCH BACKWARD {b} FROM {cur};")),
        StmtKind::Raw(format!("MOVE ABSOLUTE {abs} IN {cur};")),
        StmtKind::Raw(format!("FETCH FORWARD 25 FROM {cur};")),
        StmtKind::Raw(format!("FETCH ABSOLUTE {b} FROM {cur};")),
        StmtKind::Raw(format!("FETCH BACKWARD 5 FROM {cur};")),
    ];
    if g.rng.chance(1, 2) {
        g.fire("spill:scroll:tail");
        stmts.push(StmtKind::Raw(format!("MOVE FORWARD ALL IN {cur};")));
        stmts.push(StmtKind::Raw(format!("FETCH BACKWARD 17 FROM {cur};")));
        stmts.push(StmtKind::Raw(format!("MOVE ABSOLUTE 0 IN {cur};")));
        stmts.push(StmtKind::Raw(format!("FETCH FORWARD 3 FROM {cur};")));
    }
    if g.rng.chance(1, 2) {
        // W5 r2: pure backward MOVEs (skip, no fetch) over the spilled
        // random-access sort — the backward skiptuples/seek edges.
        g.fire("spill:scroll:backmove");
        stmts.push(StmtKind::Raw(format!("MOVE BACKWARD 30 IN {cur};")));
        stmts.push(StmtKind::Raw(format!("MOVE BACKWARD ALL IN {cur};")));
        stmts.push(StmtKind::Raw(format!("FETCH FORWARD 2 FROM {cur};")));
    }
    stmts.push(StmtKind::Raw(format!("CLOSE {cur};")));
    stmts.push(StmtKind::Raw("COMMIT;".to_string()));
    stmts
}

// ---------------------------------------------------------------- hold ----

/// WITH HOLD cursor persisted across COMMIT: PersistHoldablePortal fills a
/// holdStore tuplestore under the bracket's 64kB work_mem (SET LOCAL is
/// still live at pre-commit persist time), so the post-commit FETCHes read
/// back from spilled tuplestore state — the tuplestore_gettuple /
/// read-pointer tape arms without any server restart.
fn gen_hold(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:hold");
    let t = g.spill.tables[ti].name.clone();
    let cur = format!("fz_sph_{}", g.spill.next_cursor);
    g.spill.next_cursor += 1;
    let n = 50 + g.rng.below(300);
    vec![
        StmtKind::Raw("BEGIN;".to_string()),
        StmtKind::Raw("SET LOCAL work_mem = '64kB';".to_string()),
        StmtKind::Raw(format!(
            "DECLARE {cur} SCROLL CURSOR WITH HOLD FOR \
             SELECT pk, txt FROM {t} ORDER BY pk;"
        )),
        StmtKind::Raw("COMMIT;".to_string()),
        StmtKind::Raw(format!("FETCH FORWARD {n} FROM {cur};")),
        StmtKind::Raw(format!("FETCH BACKWARD 40 FROM {cur};")),
        StmtKind::Raw(format!("MOVE FORWARD ALL IN {cur};")),
        StmtKind::Raw(format!("FETCH BACKWARD 11 FROM {cur};")),
        // W5 r2: holdStore seek matrix — FIRST/LAST are absolute seeks on
        // the spilled tuplestore (tuplestore_select/copy_read_pointer +
        // backward tape walks), MOVE BACKWARD is a backward skip.
        StmtKind::Raw(format!("MOVE BACKWARD 25 IN {cur};")),
        StmtKind::Raw(format!("FETCH FIRST FROM {cur};")),
        StmtKind::Raw(format!("FETCH LAST FROM {cur};")),
        StmtKind::Raw(format!("MOVE BACKWARD ALL IN {cur};")),
        StmtKind::Raw(format!("FETCH FORWARD 7 FROM {cur};")),
        StmtKind::Raw(format!("CLOSE {cur};")),
    ]
}

// ------------------------------------------------------------ hash join ---

/// Serial multi-batch hash join (ExecHashIncreaseNumBatches,
/// ExecHashJoinNewBatch, batch-file save/load) plus the skew-hash and
/// outer/semi/anti fill arms, forced via nestloop+mergejoin off and
/// work_mem='64kB' over dup-heavy self-joins.
fn gen_hashjoin(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:hj");
    let t = g.spill.tables[ti].name.clone();
    let shape = g.weights.pick(
        g.rng,
        &[
            "spill:hj:inner",
            "spill:hj:skew",
            "spill:hj:rows",
            "spill:hj:outer",
            "spill:hj:antisemi",
            "spill:hj:right",
            "spill:hj:bigtuple",
            "spill:hj:growbuckets",
            "spill:hj:empty",
        ],
    );
    g.fire(shape);
    // W5 r2 (spill:hj:growbuckets): the ONE shape that must NOT batch —
    // ExecHashIncreaseNumBuckets only runs while nbatch==1. Roomy
    // work_mem + a stats-blind always-true build-side filter
    // (length(pad) > 39, planner defaults it to ~1/3 selectivity): the
    // underestimated build triples past nbuckets and the bucket array
    // doubles mid-build instead of batching.
    if shape == "spill:hj:growbuckets" {
        let body = format!(
            "SELECT count(*), sum(b.k_int::int8) FROM {t} a \
             JOIN (SELECT pk, k2, k_int FROM {t} WHERE length(pad) > 39) b \
             ON a.k2 = b.k2 WHERE a.pk <= 2000;"
        );
        return bracket(
            &[
                ("enable_nestloop", "off"),
                ("enable_mergejoin", "off"),
                ("work_mem", "'16MB'"),
                ("hash_mem_multiplier", "1"),
            ],
            vec![StmtKind::Raw(body)],
        );
    }
    let body = match shape {
        // Dup-heavy inner join: both sides too big for 64kB — batch growth
        // during build + batch files on both sides.
        "spill:hj:inner" => {
            let a = 100 + g.rng.below(150);
            format!(
                "SELECT count(*), sum((a.k_int + b.k_int)::int8), min(a.pk), max(b.pk) \
                 FROM {t} a JOIN {t} b ON a.k2 = b.k2 AND a.pk < b.pk \
                 WHERE a.k_int < {a} AND b.k_int < 300;"
            )
        }
        // Skew hash (ExecHashBuildSkewHash): the probe side's join key has
        // a dominating MCV (kskew: 85% zero), the build side is the small
        // distinct set — stats-driven skew buckets under multi-batch.
        "spill:hj:skew" => format!(
            "SELECT count(*), sum(a.pk::int8), sum(length(b.pad)) FROM {t} a \
             JOIN (SELECT kskew, pad FROM {t} WHERE kskew > 0) b \
             ON a.kskew = b.kskew;"
        ),
        // Row-returning, total order.
        "spill:hj:rows" => {
            let n = 100 + g.rng.below(200);
            format!(
                "SELECT a.pk, b.pk FROM {t} a JOIN {t} b ON a.k2 = b.k2 \
                 WHERE a.pk <= {n} AND b.txt < 'p150' ORDER BY a.pk, b.pk;"
            )
        }
        // LEFT/FULL fill arms (HJ_FILL_*_TUPLE) over a spilled build.
        "spill:hj:outer" => {
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT count(*), count(b.pk) FROM {t} a \
                     LEFT JOIN {t} b ON a.k2 = b.k2 AND b.k_int < 4 \
                     WHERE a.pk <= 6000;"
                )
            } else {
                format!(
                    "SELECT count(*), count(a.pk), count(b.pk) FROM {t} a \
                     FULL JOIN (SELECT pk, k2 FROM {t} WHERE k_int < 120) b \
                     ON a.pk = b.pk WHERE a.k_int < 250 OR a.k_int IS NULL;"
                )
            }
        }
        // W5 r2: RIGHT JOIN / right-semi / right-anti (JOIN_RIGHT_* fill
        // and match-flag arms — the hashed side carries the fill duty).
        "spill:hj:right" => match g.rng.below(3) {
            0 => format!(
                "SELECT count(*), count(a.pk) FROM \
                 (SELECT pk, k2 FROM {t} WHERE k_int < 40) a \
                 RIGHT JOIN {t} b ON a.k2 = b.k2 WHERE b.pk <= 6000;"
            ),
            // Tiny distinct outer vs huge EXISTS side: the planner hashes
            // the big side (Right Semi Join under 64kB, multi-batch).
            1 => format!(
                "SELECT count(*) FROM (SELECT DISTINCT k2 FROM {t} WHERE k_int < 30) a \
                 WHERE EXISTS (SELECT 1 FROM {t} b WHERE b.k2 = a.k2);"
            ),
            _ => format!(
                "SELECT count(*) FROM (SELECT DISTINCT k2 FROM {t} WHERE k_int < 30) a \
                 WHERE NOT EXISTS (SELECT 1 FROM {t} b WHERE b.k2 = a.k2 AND b.k_int > 490);"
            ),
        },
        // W5 r2: oversized build tuples (> HASH_CHUNK_THRESHOLD) — the
        // dense_alloc separate-chunk arm plus oversized batch-file writes.
        // The wide value is computed in-flight (never toasted), ~8.3kB.
        // Build side pk<=120 / outer a.pk<=1200: the ~8.3kB value still
        // exceeds HASH_CHUNK_THRESHOLD (separate-chunk arm + oversized
        // batch-file writes fire at ANY row count), trimmed so the join
        // finishes under the 10s debug-B statement_timeout (W5 r2: the
        // original 300x5000 shape timed out on debug B — 57014, not a
        // result diff).
        "spill:hj:bigtuple" => format!(
            "SELECT count(*), sum(length(b.big))::int8 FROM {t} a \
             JOIN (SELECT k2, repeat('q', 8300 + (pk % 41)::int) AS big \
                   FROM {t} WHERE pk <= 120) b \
             ON a.k2 = b.k2 WHERE a.pk <= 1200;"
        ),
        // W5 r2: empty-side early-out arms (empty build hashtable /
        // outer-relation-empty checks in ExecHashJoinImpl).
        "spill:hj:empty" => {
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT count(*) FROM {t} a JOIN \
                     (SELECT pk, k2 FROM {t} WHERE k_int < 0) b ON a.k2 = b.k2;"
                )
            } else {
                format!(
                    "SELECT count(*) FROM (SELECT pk, k2 FROM {t} WHERE k_int < 0) a \
                     JOIN {t} b ON a.k2 = b.k2;"
                )
            }
        }
        // Semi/anti arms over a spilled hash table.
        _ => {
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT count(*) FROM {t} a WHERE EXISTS \
                     (SELECT 1 FROM {t} b WHERE b.k2 = a.k2 AND b.k_int < 60);"
                )
            } else {
                format!(
                    "SELECT count(*) FROM {t} a WHERE NOT EXISTS \
                     (SELECT 1 FROM {t} b WHERE b.k2 = a.k2 AND b.k_int < 3);"
                )
            }
        }
    };
    bracket(
        &[
            ("enable_nestloop", "off"),
            ("enable_mergejoin", "off"),
            ("work_mem", "'64kB'"),
            ("hash_mem_multiplier", "1"),
        ],
        vec![StmtKind::Raw(body)],
    )
}

// ----------------------------------------------------------- merge join ---

/// Merge join over spilled sorts with mark/restore (tuplesort_markpos /
/// tuplesort_restorepos on tape): dup-heavy equality keys force restores;
/// enable_material=off keeps the mark/restore on the Sort node itself.
fn gen_mergejoin(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:mj");
    let t = g.spill.tables[ti].name.clone();
    // Wide bands + pad carried through the sort: the per-side sort input
    // must exceed 64kB or the merge inputs quicksort in memory (witnessed
    // at narrow bands).
    let lo = 40 + g.rng.below(40);
    let hi = lo + 120 + g.rng.below(80);
    let body = if g.rng.chance(1, 2) {
        g.fire("spill:mj:inner");
        format!(
            "SELECT count(*), sum(length(a.pad)), sum((a.pk + b.pk)::int8) \
             FROM {t} a JOIN {t} b ON a.k_int = b.k_int \
             WHERE a.k_int BETWEEN {lo} AND {hi} AND b.k_int BETWEEN {lo} AND {hi};"
        )
    } else {
        g.fire("spill:mj:full");
        format!(
            "SELECT count(*), count(a.pk), count(b.pk) FROM \
             (SELECT pk, k_int, pad FROM {t} WHERE k_int BETWEEN {lo} AND {hi}) a \
             FULL JOIN (SELECT pk, k_int FROM {t} WHERE k_int >= {}) b \
             ON a.k_int = b.k_int;",
            lo + 60
        )
    };
    // Two mark/restore regimes, both over spilled random-access sorts:
    // material off = tuplesort_markpos/restorepos on tape (witnessed:
    // "external sort" method); material on = the planner's Materialize
    // absorb above the spilled sort (witnessed).
    let material_off = g.rng.chance(1, 2);
    if material_off {
        g.fire("spill:mj:nomat");
        bracket(
            &[
                ("enable_hashjoin", "off"),
                ("enable_nestloop", "off"),
                ("enable_material", "off"),
                ("work_mem", "'64kB'"),
            ],
            vec![StmtKind::Raw(body)],
        )
    } else {
        g.fire("spill:mj:mat");
        bracket(
            &[
                ("enable_hashjoin", "off"),
                ("enable_nestloop", "off"),
                ("work_mem", "'64kB'"),
            ],
            vec![StmtKind::Raw(body)],
        )
    }
}

// -------------------------------------------------------------- hashagg ---

/// HashAgg disk spill (hashagg_spill_*, agg_refill_hash_table,
/// hashagg_recompile_expressions, batch refill loops): group counts far
/// above what 64kB holds, hash_mem_multiplier=1, results wrapped in an
/// outer order-independent aggregate. enable_sort=off steers the planner
/// to the hashed strategy on the wrapped shapes.
fn gen_hashagg(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:hashagg");
    let t = g.spill.tables[ti].name.clone();
    let shape = g.weights.pick(
        g.rng,
        &["spill:ha:wrap", "spill:ha:distinct", "spill:ha:gsets", "spill:ha:mixed"],
    );
    g.fire(shape);
    // W5 r2 (spill:ha:mixed): grouping sets with BOTH strategies live —
    // enable_sort stays ON so the planner builds an AGG_MIXED plan (a
    // sorted rollup chain plus a hashed set), the multi-phase
    // ExecInitAgg / initialize_phase arms the sort-off variant shadows.
    if shape == "spill:ha:mixed" {
        let body = format!(
            "SELECT count(*), sum(c) FROM \
             (SELECT k2, k_int, (pk % 512) AS p9, count(*) AS c FROM {t} \
              GROUP BY GROUPING SETS (ROLLUP (k2, k_int), (p9))) s;"
        );
        return bracket(
            &[("work_mem", "'64kB'"), ("hash_mem_multiplier", "1")],
            vec![StmtKind::Raw(body)],
        );
    }
    let body = match shape {
        // ~8k spilled groups, outer wrap order-independent.
        "spill:ha:wrap" => {
            let m = 4096 + g.rng.below(3) * 2048; // 4096/6144/8192 groups
            format!(
                "SELECT count(*), sum(cnt), sum(s), min(gk), max(gk) FROM \
                 (SELECT (pk * 17) % {m} AS gk, count(*) AS cnt, \
                  sum(k_int)::int8 AS s FROM {t} GROUP BY 1) g;"
            )
        }
        // Hashed DISTINCT spill.
        "spill:ha:distinct" => format!(
            "SELECT count(*) FROM (SELECT DISTINCT (pk * 7) % 9973, txt FROM {t}) s;"
        ),
        // Grouping sets: the mixed hashed/sorted strategies + spill.
        _ => format!(
            "SELECT count(*), sum(c) FROM \
             (SELECT k2, (pk % 512) AS p9, count(*) AS c FROM {t} \
              GROUP BY GROUPING SETS ((k2), (p9), (k2, p9))) s;"
        ),
    };
    bracket(
        &[
            ("work_mem", "'64kB'"),
            ("hash_mem_multiplier", "1"),
            ("enable_sort", "off"),
        ],
        vec![StmtKind::Raw(body)],
    )
}

// ------------------------------------------------------------- groupagg ---

/// Sort-based aggregation under spill (the hashagg fallback surface plus
/// process_ordered_aggregate_single/multi): DISTINCT aggregates,
/// multi-input ordered aggregates, ordered-set aggregates
/// (tuplesort_skiptuples), all with enable_hashagg=off + 64kB work_mem.
fn gen_groupagg(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:groupagg");
    let t = g.spill.tables[ti].name.clone();
    let shape = g.weights.pick(
        g.rng,
        &[
            "spill:ga:group",
            "spill:ga:distinct",
            "spill:ga:oset",
            "spill:ga:filter",
            "spill:ga:hypo",
            "spill:ga:dpad",
        ],
    );
    g.fire(shape);
    let body = match shape {
        // W5 r2: FILTER + strict-transition NULL fuel — advance_
        // transition_function's null-transValue and skipped-input arms
        // (NULLIF makes real NULLs; FILTER gates rows per aggregate).
        "spill:ga:filter" => format!(
            "SELECT (pk % 71) AS gk, sum(k_int) FILTER (WHERE k_int % 7 = 0), \
             count(DISTINCT NULLIF(k_int, 3)), max(NULLIF(txt, 'p5')), \
             count(k_int) FILTER (WHERE k_int > 497) \
             FROM {t} GROUP BY 1 ORDER BY 1;"
        ),
        // W5 r2: hypothetical-set aggregates — the direct-args +
        // sorted-input walk (process_ordered_aggregate_multi with the
        // hypothetical extra column, AggGetTempMemoryContext). Ratio
        // outputs are single exact divisions — deterministic on both
        // sides (not accumulation-order float, B1-safe).
        "spill:ga:hypo" => format!(
            "SELECT rank(250, 'p500') WITHIN GROUP (ORDER BY k_int, txt), \
             dense_rank(120) WITHIN GROUP (ORDER BY k_int), \
             percent_rank(60) WITHIN GROUP (ORDER BY k_int), \
             cume_dist(77) WITHIN GROUP (ORDER BY k_int) FROM {t};"
        ),
        // W5 r2: whole-table DATUM sorts over LOW-cardinality text — pad
        // has only 7 distinct values, so the abbreviated-key cardinality
        // collapse fires removeabbrev_datum mid-sort (the heap-sort twin
        // is spill:sort:abbrev).
        "spill:ga:dpad" => {
            format!("SELECT count(DISTINCT pad), count(DISTINCT txt) FROM {t};")
        }
        // Per-group ordered/DISTINCT transitions over sorted groups; the
        // string_agg input carries TWO sort columns (ordered-multi arm).
        // Unindexed grouping expression: GROUP BY k2 rides the k2 index
        // into an in-memory incremental sort (witnessed); (pk % 89) forces
        // the full external merge under the GroupAggregate.
        "spill:ga:group" => format!(
            "SELECT (pk % 89) AS gk, count(DISTINCT k_int), \
             length(string_agg(txt, ',' ORDER BY k_int, pk)), \
             cardinality(array_agg(DISTINCT (pk % 61))) \
             FROM {t} GROUP BY 1 ORDER BY 1;"
        ),
        // Whole-table DISTINCT aggregate: one giant sort + dedup walk.
        "spill:ga:distinct" => {
            format!("SELECT count(DISTINCT txt), count(DISTINCT pk) FROM {t};")
        }
        // Ordered-set aggregates: percentile_disc walks the sorted group
        // via tuplesort_skiptuples; mode() takes the dedup walk.
        _ => format!(
            "SELECT (pk % 53) AS gk, \
             percentile_disc(0.5) WITHIN GROUP (ORDER BY k_int), \
             mode() WITHIN GROUP (ORDER BY txt) \
             FROM {t} GROUP BY 1 ORDER BY 1;"
        ),
    };
    bracket(
        &[("enable_hashagg", "off"), ("work_mem", "'64kB'")],
        vec![StmtKind::Raw(body)],
    )
}

// --------------------------------------------------------------- window ---

/// Window aggregation over spilled tuplestores: big partitions under 64kB
/// force the windowagg buffer to disk, and moving frames keep multiple
/// read pointers live (tuplestore_alloc/select/copy_read_pointer,
/// skiptuples). All outputs exact-typed, total ORDER BY pk.
fn gen_window(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:window");
    let t = g.spill.tables[ti].name.clone();
    let shape = g.weights.pick(g.rng, &["spill:win:rowsframe", "spill:win:part", "spill:win:range"]);
    g.fire(shape);
    let body = match shape {
        // Wide moving frame with pad carried through the buffer: the
        // windowagg tuplestore spills (witnessed: Storage: Disk, heavy
        // temp re-reads from the frame pointers). Aggregate-wrapped —
        // nothing order-sensitive crosses the wire.
        "spill:win:rowsframe" => {
            // Frame + cap sized to spill the windowagg tuplestore (pad-wide
            // tuples spill at 64kB after a few hundred rows) while staying
            // well under the differential/coverage 10s statement_timeout on
            // a DEBUG B build (W5 r2: the original 700-1300 frame over 6500
            // rows blew the timeout on debug B — 57014, not a result diff).
            let p = 150 + g.rng.below(200);
            let f = 150 + g.rng.below(200);
            let cap = 1500 + g.rng.below(1000);
            format!(
                "SELECT max(s), min(s) FROM (SELECT sum(length(pad)) OVER \
                 (ORDER BY pk ROWS BETWEEN {p} PRECEDING AND {f} FOLLOWING) AS s \
                 FROM {t} WHERE pk <= {cap}) w;"
            )
        }
        // Row-returning variant over a smaller cap: per-partition running
        // frames + rank family, total ORDER BY pk.
        "spill:win:part" => {
            let cap = 1200 + g.rng.below(600);
            format!(
                "SELECT pk, sum(pk::int8) OVER (PARTITION BY k2 ORDER BY pk \
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
                 row_number() OVER (PARTITION BY k2 ORDER BY pk), \
                 lag(k_int, 3) OVER (ORDER BY pk) \
                 FROM {t} WHERE pk <= {cap} ORDER BY pk;"
            )
        }
        // RANGE frame with EXCLUDE over the spilled buffer; frame sums are
        // membership-based (order-independent) — deterministic.
        _ => {
            // Trimmed to spill-but-fast on debug B (see win:rowsframe note).
            let cap = 1500 + g.rng.below(1000);
            format!(
                "SELECT max(s) FROM (SELECT sum(length(pad)) OVER (ORDER BY k_int \
                 RANGE BETWEEN 25 PRECEDING AND 25 FOLLOWING EXCLUDE TIES) AS s \
                 FROM {t} WHERE pk <= {cap}) w;"
            )
        }
    };
    bracket(&[("work_mem", "'64kB'")], vec![StmtKind::Raw(body)])
}

// ------------------------------------------------------------------ cte ---

/// Materialized CTE scanned twice: one shared tuplestore, two read
/// pointers, spilled at 64kB (tuplestore_puttuple_common state flips +
/// per-pointer tape reads).
fn gen_cte(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:cte");
    let t = g.spill.tables[ti].name.clone();
    let cap = 6000 + g.rng.below(6000);
    let body = format!(
        "WITH w AS MATERIALIZED (SELECT pk, k_int, txt FROM {t} WHERE pk <= {cap}) \
         SELECT count(*), sum(a.k_int)::int8 + sum(b.k_int)::int8 \
         FROM w a JOIN w b ON a.pk = b.pk;"
    );
    bracket(&[("work_mem", "'64kB'")], vec![StmtKind::Raw(body)])
}

// ------------------------------------------------------------- material ---

/// Nestloop with a Materialize inner rescanned per outer row
/// (nodeMaterial + tuplestore_rescan). The planner always materializes
/// the SMALLER side (join order is not pinnable here — witnessed), so
/// this family exercises the in-memory material rescan arms; the SPILLED
/// material + mark/restore-absorb path comes from the mj:mat variant,
/// and the spilled-tuplestore read arms from the window/cte/hold
/// families.
fn gen_material(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:material");
    let t = g.spill.tables[ti].name.clone();
    let outer = 10 + g.rng.below(20);
    let body = format!(
        "SELECT count(*), sum(b.k_int) FROM {t} a JOIN {t} b ON a.k2 = b.k2 \
         WHERE a.pk <= {outer};"
    );
    bracket(
        &[
            ("enable_hashjoin", "off"),
            ("enable_mergejoin", "off"),
            ("enable_memoize", "off"),
            ("work_mem", "'64kB'"),
        ],
        vec![StmtKind::Raw(body)],
    )
}

// -------------------------------------------------------------- memoize ---

/// Memoize under a starved cache (hash_mem = 64kB): wide per-key entry
/// sets overflow the cache and force entry eviction (cache_reduce_memory
/// and the ExecMemoize miss/evict arms).
fn gen_memoize(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:memoize");
    let t = g.spill.tables[ti].name.clone();
    // Eviction fires independent of row count (the stats-blind filter
    // undersizes the cache regardless); cap trimmed so the starved-cache
    // join finishes under the 10s debug-B statement_timeout (W5 r2: the
    // original 1500-3000 cap timed out on debug B — 57014, not a result
    // diff).
    let cap = 800 + g.rng.below(700);
    // The stats-blind `length(b.pad) > 39` filter (always true, planner
    // defaults it to 1/3 selectivity) undersizes the estimated cache
    // entries, so the planner keeps the Memoize node at 64kB while the
    // actual entries overflow it — witnessed: Evictions: 1996/2000
    // probes (cache_reduce_memory + the miss/evict arms). A plain column
    // key either fits the cache or drops the node entirely.
    let body = format!(
        "SELECT count(*), sum(length(b.pad)) FROM {t} a \
         JOIN {t} b ON a.k2 = b.k2 AND length(b.pad) > 39 WHERE a.pk <= {cap};"
    );
    bracket(
        &[
            ("enable_hashjoin", "off"),
            ("enable_mergejoin", "off"),
            ("work_mem", "'64kB'"),
            ("hash_mem_multiplier", "1"),
        ],
        vec![StmtKind::Raw(body)],
    )
}

// -------------------------------------------------------------- incsort ---

/// Incremental sort (nodeIncrementalSort.c): the k_int index supplies the
/// presorted prefix (enable_seqscan=off), the pk suffix is sorted per
/// prefix group — prefix-key group detection, per-group sort resets, and
/// the bounded variant's mode switching, under 64kB.
fn gen_incsort(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:incsort");
    let t = g.spill.tables[ti].name.clone();
    let body = match g.rng.below(3) {
        0 => {
            g.fire("spill:is:wrap");
            format!(
                "SELECT count(*), sum(pk::int8) FROM \
                 (SELECT pk FROM {t} ORDER BY k_int, pk) s;"
            )
        }
        // Giant prefix group: kskew is 85% zero, so the zero group's
        // suffix sort is a per-group EXTERNAL merge inside the
        // incremental sort (witnessed: Pre-sorted Groups external).
        1 => {
            g.fire("spill:is:skewgroup");
            format!(
                "SELECT count(*), sum(pk::int8) FROM \
                 (SELECT pk FROM {t} ORDER BY kskew, pk) s;"
            )
        }
        _ => {
            g.fire("spill:is:bounded");
            let n = 200 + g.rng.below(800);
            format!("SELECT pk, k_int FROM {t} ORDER BY k_int, pk LIMIT {n};")
        }
    };
    bracket(
        &[("enable_seqscan", "off"), ("work_mem", "'64kB'")],
        vec![StmtKind::Raw(body)],
    )
}

// -------------------------------------------------------------- cluster ---

/// CLUSTER through the tuplesort_begin_cluster path (tuplesortvariants.c
/// cluster arms + heapam rewrite) under a small maintenance_work_mem so
/// the cluster sort itself spills; occasional VACUUM FULL takes the
/// no-index rewrite route. Physical order changes are a non-surface (all
/// reads elsewhere are ordered); ANALYZE right after re-pins stats.
fn gen_cluster(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:cluster");
    let t = g.spill.tables[ti].name.clone();
    let idx = g.spill.tables[ti].kint_index.clone();
    if g.rng.chance(1, 4) {
        g.fire("spill:cluster:vacfull");
        return vec![
            StmtKind::Raw(format!("VACUUM FULL {t};")),
            StmtKind::Raw(format!("ANALYZE {t};")),
        ];
    }
    let target = if g.rng.chance(1, 3) {
        format!("{t}_pkey")
    } else {
        idx
    };
    bracket(
        &[("maintenance_work_mem", "'1MB'")],
        vec![
            StmtKind::Raw(format!("CLUSTER {t} USING {target};")),
            StmtKind::Raw(format!("ANALYZE {t};")),
        ],
    )
}

// -------------------------------------------------------------- hashidx ---

/// Sorted hash-index build (tuplesort_begin_index_hash +
/// comparetup_index_hash + the hashsort spill) under a small
/// maintenance_work_mem; the index is dropped in the same group so the
/// table keeps its pinned plan-relevant index set.
fn gen_hashidx(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:hashidx");
    let t = g.spill.tables[ti].name.clone();
    let iname = format!("fz_sphi_{}", g.spill.next_index);
    g.spill.next_index += 1;
    let col = if g.rng.chance(1, 2) { "txt" } else { "k_int" };
    bracket(
        &[("maintenance_work_mem", "'1MB'")],
        vec![
            StmtKind::Raw(format!("CREATE INDEX {iname} ON {t} USING hash ({col});")),
            StmtKind::Raw(format!("DROP INDEX {iname};")),
        ],
    )
}

// --------------------------------------------------------------- rescan ---

/// Nestloop-driven rescans of spilled subplans: ExecReScanAgg over a
/// spilled hash aggregation (hashagg_reset_spill_state — the hash table
/// must be rebuilt, not replayed) and ExecReScanHashJoin over a
/// multi-batch join. The outer VALUES row set keeps the rescan count
/// small and deterministic.
fn gen_rescan(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:rescan");
    let t = g.spill.tables[ti].name.clone();
    let body = if g.rng.chance(1, 2) {
        g.fire("spill:rescan:agg");
        format!(
            "SELECT count(*), sum(i.c) FROM (VALUES (0), (1), (2)) v(x) \
             JOIN (SELECT (pk * 17) % 4096 AS gk, count(*) AS c FROM {t} GROUP BY 1) i \
             ON i.gk % 3 = v.x;"
        )
    } else {
        g.fire("spill:rescan:hj");
        format!(
            "SELECT count(*) FROM (VALUES (0), (1)) v(x) \
             JOIN (SELECT a.pk AS p FROM {t} a JOIN {t} b ON a.k2 = b.k2 \
                   WHERE a.k_int < 25 AND b.k_int < 120) hj \
             ON hj.p % 2 = v.x;"
        )
    };
    bracket(
        &[
            ("enable_material", "off"),
            ("enable_memoize", "off"),
            ("work_mem", "'64kB'"),
            ("hash_mem_multiplier", "1"),
        ],
        vec![StmtKind::Raw(body)],
    )
}

// -------------------------------------------------------------- explain ---

/// EXPLAIN (COSTS OFF) plan-shape probes under the spill brackets: both
/// planners must pick the same spilling plan shape (strict compare — a
/// shape divergence under identical GUCs is signal).
fn gen_explain(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("spill:explain");
    let t = g.spill.tables[ti].name.clone();
    let shape = g.weights.pick(g.rng, &["spill:ex:hj", "spill:ex:ha", "spill:ex:incsort"]);
    g.fire(shape);
    let (gucs, body): (&[(&str, &str)], String) = match shape {
        "spill:ex:hj" => (
            &[
                ("enable_nestloop", "off"),
                ("enable_mergejoin", "off"),
                ("work_mem", "'64kB'"),
            ],
            format!(
                "EXPLAIN (COSTS OFF) SELECT count(*) FROM {t} a JOIN {t} b \
                 ON a.k2 = b.k2 WHERE a.k_int < 100;"
            ),
        ),
        "spill:ex:ha" => (
            &[
                ("work_mem", "'64kB'"),
                ("hash_mem_multiplier", "1"),
                ("enable_sort", "off"),
            ],
            format!(
                "EXPLAIN (COSTS OFF) SELECT count(*) FROM \
                 (SELECT (pk * 17) % 8192, count(*) FROM {t} GROUP BY 1) s;"
            ),
        ),
        _ => (
            &[("enable_seqscan", "off"), ("work_mem", "'64kB'")],
            format!(
                "EXPLAIN (COSTS OFF) SELECT pk FROM {t} ORDER BY k_int, pk LIMIT 100;"
            ),
        ),
    };
    bracket(gucs, vec![StmtKind::Raw(body)])
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
        let mut state = SpillState::new();
        let mut out = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &weights, &mut prods, 4);
            std::mem::swap(&mut g.spill, &mut state);
            let stmts = gen_spill_module(&mut g);
            std::mem::swap(&mut g.spill, &mut state);
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
        // Every SET in a group has a matching RESET in the SAME group
        // (SET LOCAL is transaction-scoped: it must live between BEGIN
        // and COMMIT in its group instead).
        for group in gen_actions(7, 400) {
            let mut open: Vec<String> = Vec::new();
            let mut in_txn = false;
            for k in &group {
                let sql = k.to_sql();
                if sql == "BEGIN;" {
                    in_txn = true;
                } else if sql == "COMMIT;" || sql == "ROLLBACK;" {
                    in_txn = false;
                }
                if let Some(rest) = sql.strip_prefix("SET LOCAL ") {
                    assert!(in_txn, "SET LOCAL outside txn bracket: {rest}");
                } else if let Some(rest) = sql.strip_prefix("SET ") {
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
            assert!(!in_txn, "unclosed transaction bracket at group end");
        }
    }

    #[test]
    fn row_returning_selects_are_totally_ordered() {
        // Any row-returning SELECT (projecting pk columns, not wrapped in
        // an outer aggregate) must carry a top-level ORDER BY: spilled
        // execution must not expose row order.
        for sql in flat(11, 500) {
            if !sql.starts_with("SELECT pk") && !sql.contains("SELECT a.pk, b.pk") {
                continue;
            }
            assert!(sql.contains("ORDER BY"), "row-returning spill SELECT without ORDER BY: {sql}");
        }
    }

    #[test]
    fn no_float_aggregates() {
        // B1: no accumulation-order-sensitive float aggregates anywhere in
        // the module output.
        for sql in flat(13, 600) {
            for bad in ["avg(num)", "sum(num)", "stddev", "var_samp", "percentile_cont",
                        "::float", "::double"] {
                assert!(!sql.contains(bad), "float-order-sensitive aggregate: {sql}");
            }
        }
    }

    #[test]
    fn cursors_are_closed_and_transactions_bracketed() {
        // Every DECLARE has a CLOSE in the same group; non-HOLD cursor
        // groups begin with BEGIN and end with COMMIT.
        for group in gen_actions(17, 500) {
            let sqls: Vec<String> = group.iter().map(|k| k.to_sql()).collect();
            let declares = sqls.iter().filter(|s| s.starts_with("DECLARE ")).count();
            let closes = sqls.iter().filter(|s| s.starts_with("CLOSE ")).count();
            assert_eq!(declares, closes, "unbalanced DECLARE/CLOSE: {sqls:?}");
            if declares > 0 {
                assert_eq!(sqls[0], "BEGIN;", "cursor group must open a txn: {sqls:?}");
                assert!(
                    sqls.iter().any(|s| s == "COMMIT;"),
                    "cursor group must commit: {sqls:?}"
                );
            }
        }
    }

    #[test]
    fn cluster_targets_only_known_indexes() {
        // CLUSTER always names the table's own pk or k_int index, and is
        // followed by ANALYZE (stats re-pin) in the same group.
        for group in gen_actions(23, 600) {
            let sqls: Vec<String> = group.iter().map(|k| k.to_sql()).collect();
            for (i, sql) in sqls.iter().enumerate() {
                if let Some(rest) = sql.strip_prefix("CLUSTER ") {
                    let mut it = rest.trim_end_matches(';').split(" USING ");
                    let table = it.next().unwrap();
                    let index = it.next().expect("CLUSTER without USING");
                    assert!(
                        index == format!("{table}_pkey") || index.starts_with("fz_spi_"),
                        "CLUSTER on unknown index: {sql}"
                    );
                    assert!(
                        sqls[i + 1..].iter().any(|s| s.starts_with("ANALYZE ")),
                        "CLUSTER without trailing ANALYZE: {sqls:?}"
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
    fn analyze_determinism_row_cap() {
        // Bulk loads never exceed the exhaustive-ANALYZE sample.
        for sql in flat(19, 400) {
            if sql.starts_with("INSERT INTO fz_sp_") {
                let args = sql.split("generate_series(").nth(1).unwrap();
                let tail = args.split(',').nth(1).unwrap().trim();
                let digits: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
                let hi: i64 = digits.parse().unwrap();
                assert!(hi <= 24000, "bulk load exceeds ANALYZE-exhaustive cap: {sql}");
            }
        }
    }

    #[test]
    fn all_families_fire() {
        let stmts = flat(3, 1200).join("\n");
        for needle in [
            "SET work_mem = '64kB';",
            "SET hash_mem_multiplier = 1;",
            "SET enable_hashagg = off;",
            "SET enable_sort = off;",
            "SET enable_material = off;",
            "SET enable_memoize = off;",
            "SET enable_seqscan = off;",
            "SET maintenance_work_mem = '1MB';",
            "SET LOCAL work_mem = '64kB';",
            "DECLARE fz_spc_",
            "WITH HOLD",
            "FETCH BACKWARD",
            "MOVE ABSOLUTE",
            "CLUSTER fz_sp_",
            "VACUUM FULL fz_sp_",
            "GROUPING SETS",
            "WITHIN GROUP",
            "string_agg(txt, ',' ORDER BY k_int, pk)",
            "WITH w AS MATERIALIZED",
            "EXCLUDE TIES",
            "ROWS BETWEEN",
            "EXPLAIN (COSTS OFF)",
            "FULL JOIN",
            "NOT EXISTS",
            "kskew, pad FROM",
            "ORDER BY k_int, pk LIMIT",
            "USING hash (",
            "ORDER BY pad DESC, pk",
        ] {
            assert!(stmts.contains(needle), "family never fired in 1200 groups: {needle}");
        }
    }
}
