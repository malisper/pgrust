//! Index-AM exercise module (I1): dedicated data tables sized to force real
//! access-method behavior, index creation across all five non-btree AMs with
//! their data-driven operator classes, query shapes that actually engage each
//! AM (bracketed with SET enable_seqscan TO off / RESET applied identically
//! on both sides), and enough UPDATE/DELETE/INSERT/REINDEX/VACUUM churn to
//! touch the page-split and cleanup paths. The structural target is
//! gap-report-004 rank 3 `doPickSplit` (SP-GiST insert split — fires on page
//! split, so insert volume is the fuel) plus the data-driven internals of
//! SP-GiST/GiST/GIN/BRIN generally (`spg_range_quad_inner_consistent`,
//! `inet_spg_consistent_bitmap`, `gist_box_picksplit`, posting lists, ranges
//! summarization): the D1 lane proved all five AMs exist DDL-wise, but tiny
//! fixture tables and index-blind queries never reach their internals.
//!
//! Every statement family here was hand-verified byte-identical on pgrust
//! and C Postgres 18 before the module was written (2026-08-11 probe,
//! scratchpad/i1-handprobe.sql shape): all opclasses below exist on both
//! sides (kd_point_ops, inet_inclusion_ops included), KNN ordering with a pk
//! tiebreak matches, and EXPLAIN (COSTS OFF) picks the same plans.
//!
//! Design points:
//!   - `IdxState` (session-persistent, swapped in and out of `Gen` like
//!     `DmlState`/`DdlState`/`PartState`) tracks every table and index this
//!     module creates, so statements are valid by construction (zero-42xxx
//!     discipline). Tables are NOT registered into the shared catalog: their
//!     column types (point/box/int4range/inet/tsvector) are outside the
//!     generic type system. The module emits its own AM-engaging queries;
//!     the runner's pk-ordered state probes ride on `DdlEvent`s.
//!   - Data is generated deterministically from the seed via one
//!     INSERT..SELECT over generate_series with pure integer formulas —
//!     identical on both sides by construction, and bulky enough (400-1800
//!     rows, plus churn batches) that index builds and inserts genuinely
//!     split pages (verify with coverage, not hope).
//!   - Query brackets SET/RESET enable_seqscan (and, for amgettuple
//!     families, sometimes enable_bitmapscan) inside one statement group, so
//!     both sides always see identical GUC state. A runner probe landing
//!     mid-bracket sees the same GUCs on both sides — still a fair compare.
//!     The util module twiddles the same GUCs independently, so a bracket's
//!     RESET can restore the session default over a util-set value rather
//!     than the value in force at SET time; that lands identically on both
//!     sides (both replay the same statement stream), so it costs coverage
//!     at worst, never compare fairness.
//!   - Predicate literals draw from the same moduli as the data formulas,
//!     so queries actually select rows (an always-empty rowset exercises
//!     nothing).
//!   - ORDER BY pk on every row-returning query (pk is the primary key: a
//!     total order, strict ordered compare). KNN queries order by distance
//!     then pk, so distance ties cannot become tie-order noise.
//!   - EXPLAIN (COSTS OFF) probes verify index scans are actually chosen —
//!     and that plan choice matches C. Plan divergence here is signal.
//!   - No DROP INDEX (the ddl module owns index-lifecycle churn); indexes
//!     die only with their table, so REINDEX targets are live by
//!     construction. ANALYZE runs right after the initial load; tables stay
//!     under the statistics-target row limit, so ANALYZE sees every row and
//!     the stats (hence plans) are deterministic on both sides.
//!
//! B2 extension (gap-006 ranks 6/7 + the I1 doPickSplit residual), three
//! additional production families, each hand-verified on both engines
//! first (2026-08-11 probe, scratchpad/b2-handprobe.sql):
//!   - `idx:bt` — btree page-deletion depth (`_bt_pagedel` /
//!     `_bt_unlink_halfdead_page`): dedicated `fz_btd_*` tables (int4-pk
//!     depth-2 trees at 3000/6000 rows; low-fanout 95-char-text-key
//!     depth-3 trees at 9000 rows), mass DELETEs of large contiguous pk
//!     ranges (whole leaf pages — for depth-3 trees whole internal-page
//!     subtrees — go empty), VACUUM (page deletion lives in btbulkdelete),
//!     and recycle bursts that re-insert exactly-deleted pk intervals so
//!     the insert path reuses pages the previous vacuum freed. `BtTable`
//!     tracks the deleted intervals (disjoint, exact), so recycle inserts
//!     are conflict-free by construction.
//!   - `idx:adv` — adversarial picksplit distributions for the SP-GiST /
//!     GiST residual: `fz_adv_*` tables loaded with thousands of identical
//!     keys (the all-the-same fallback), collinear points (one axis), and
//!     near-page-size text values behind a 440-char shared prefix (the
//!     oversize-leaf / suffixing paths), indexed with SP-GiST quad + kd +
//!     text radix and GiST point + polygon, split both at build time and
//!     via post-index insert bursts.
//!   - `idx:stats` — the PG18 statistics-import surface
//!     (`attribute_statistics_update`): pg_restore_relation_stats /
//!     pg_restore_attribute_stats kwarg calls with plausible payloads over
//!     the live `fz_btd_*` tables, pg_clear_*_stats, EXPLAIN (COSTS OFF)
//!     planner probes (injected stats must steer plans identically), and
//!     error fuel (mismatched array lengths, malformed array literals,
//!     out-of-range fractions, negative page counts) whose t/f results are
//!     compared. PG18 ships only the restore/clear variants — there is no
//!     pg_set_*_stats (verified in pg_proc on both engines).

//! Q6 gist-paths extension (sql-reachable-queue chunk `gist-paths`), every
//! family hand-verified byte-identical on both engines first (2026-08-11,
//! scratchpad/q6-hv3-gist.sql):
//!   - `idx:gist` — a dedicated rich-GiST table (`fz_gst_*`): every range
//!     flavor with a real subdiff penalty fn (int8/num/ts/tstz/date),
//!     int4multirange (multirange_ops), tsquery (tsquery_gist), inet
//!     (network_gist incl. the fetch fn — addr is bijective in pk so
//!     index-only ORDER BY addr is a total order), tsvector with an
//!     explicit siglen (tsgistidx sig paths), a multicolumn gist(pt, bx)
//!     index (gistsplit secondary-split/dont-care fuel) and a spgist box
//!     index (geo_spgist 4D strategies). UNLOGGED variant covers
//!     gistbuildempty/spgbuildempty + fake-LSN paths. Mass contiguous
//!     DELETE + VACUUM empties whole leaf pages (gistvacuum page deletion
//!     + integerset), and exact-interval re-inserts land on freed pages.
//!   - `idx:gist:buffered` — self-contained groups building gist indexes
//!     WITH (buffering = on) over >4096-tuple point tables (the stats-then-
//!     switch path into the gistbuild/gistbuildbuffers buffered machinery).
//!   - `idx:gist:excl` — self-contained EXCLUDE USING gist groups with a
//!     deterministic conflicting insert (matched 23P01 both sides; the
//!     recheck path) and an ON CONFLICT ON CONSTRAINT DO NOTHING arm.
//!   - `idx:gist:prop` — pg_index_column_has_property probes over the live
//!     gist/spgist indexes (gistproperty/spgproperty), boolean-comparable.

use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

/// Live-table population cap: each table is heavy (create + bulk load +
/// ~6-15 indexes + ANALYZE per create group).
const MAX_LIVE_TABLES: usize = 2;

/// Fixed schema every idx table carries. pk is the probe sort key and the
/// churn handle; the payload columns cover every AM's natural input type.
const COLS_SQL: &str = "pk int4 PRIMARY KEY, pt point, bx box, rng int4range, addr inet, \
                        tsv tsvector, doc jsonb, arr int4[], txt text, ts timestamp, k_int int4";

/// Data-formula moduli (shared by the row generator and the predicate
/// literal pools — queries must hit real data).
const PT_MX: u64 = 211; // pt.x in 0..211
const PT_MY: u64 = 223; // pt.y in 0..223
const RNG_M: u64 = 1900; // range lower bound in 0..1900
const WORD_M: u64 = 211; // tsvector vocabulary w0..w210
const COMMON_M: u64 = 7; // tsvector vocabulary common0..common6
const JSON_A: u64 = 89; // doc->'a' in 0..89
const JSON_TAG: u64 = 43; // doc->>'tag' in v0..v42
const ARR_M: u64 = 97; // array elements in 0..97
const TXT_P: u64 = 7; // txt prefix p0_..p6_
const TXT_M: u64 = 1009; // txt suffix in 0..1009
const KINT_M: u64 = 500; // k_int in 0..500
const TS_M: u64 = 100000; // ts minutes offset in 0..100000

/// Live-population caps for the B2 families (each `bt` create is a bulk
/// load + secondary index; each `adv` create is three bulk loads + five
/// AM builds).
const MAX_BT_TABLES: usize = 2;
const MAX_ADV_TABLES: usize = 1;

/// bt family data moduli / shapes.
const BT_KINT_M: u64 = 500; // k_int in 0..500 (pure fn of pk)
const BT_KEY_PAD: u64 = 95; // text keys 'k' + 95 digits: ~76/leaf, depth 3 at 9000 rows

/// Shared long prefix for the adversarial oversize-text batches (the
/// SP-GiST radix suffixing fuel; 40 * 11 = 440 chars).
const ADV_PREFIX_SQL: &str = "repeat('longprefix_', 40)";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BtShape {
    Int,
    Text,
}

#[derive(Clone, Debug)]
pub struct BtTable {
    pub name: String,
    pub shape: BtShape,
    pub live: bool,
    /// Highest pk ever inserted (initial load; recycle never extends it).
    pub next_pk: i64,
    /// Disjoint, sorted, *exactly* deleted pk intervals: massdel unions
    /// in, recycle pops one back out as a re-insert. Every pk outside
    /// these intervals and <= next_pk is live, so recycle inserts are
    /// conflict-free by construction.
    pub deleted: Vec<(i64, i64)>,
}

#[derive(Clone, Debug)]
pub struct AdvTable {
    pub name: String,
    pub live: bool,
    pub next_pk: i64,
    /// Per-table all-the-same coordinates (also the line-batch y), so two
    /// tables' constant keys differ.
    pub sx: u64,
    pub sy: u64,
}

/// Q6 gist family caps / moduli.
const MAX_GIST_TABLES: usize = 1;
const GST_R8_M: u64 = 1900; // int8range lower bound in 0..1900
const GST_NUM_M: u64 = 800; // numrange lower (quarters) in 0..200
const GST_MIN_M: u64 = 100000; // ts/tstz minute offsets in 0..100000
const GST_DAY_M: u64 = 1500; // daterange day offsets in 0..1500
const GST_MR2_M: u64 = 2100; // multirange second-arm lower in 0..2100
const GST_WORD_M: u64 = 211; // tsquery/tsvector vocabulary w0..w210

#[derive(Clone, Debug)]
pub struct GistTable {
    pub name: String,
    pub live: bool,
    pub next_pk: i64,
    /// Disjoint, sorted, exactly deleted pk intervals (same discipline as
    /// BtTable::deleted): delvac unions in, reinsert pops one back out.
    pub deleted: Vec<(i64, i64)>,
    /// Indexes on this table (kind tag, column count) for property probes.
    pub indexes: Vec<IdxIndex>,
}

#[derive(Clone, Debug)]
pub struct IdxIndex {
    pub name: String,
    /// The production that created it (coverage bookkeeping / tests).
    pub kind: &'static str,
}

#[derive(Clone, Debug)]
pub struct IdxTable {
    pub name: String,
    pub live: bool,
    /// Highest pk inserted so far; churn batches allocate above it.
    pub next_pk: i64,
    /// Indexes on this table (die only with the table).
    pub indexes: Vec<IdxIndex>,
}

/// Session-persistent index-AM catalog model.
#[derive(Clone, Debug, Default)]
pub struct IdxState {
    pub tables: Vec<IdxTable>,
    pub bt: Vec<BtTable>,
    pub adv: Vec<AdvTable>,
    pub gist: Vec<GistTable>,
    next_table: u32,
    next_index: u32,
    next_bt: u32,
    next_bt_index: u32,
    next_adv: u32,
    next_gist: u32,
    next_gist_index: u32,
    events: Vec<DdlEvent>,
}

impl IdxState {
    pub fn new() -> IdxState {
        IdxState::default()
    }

    /// Drain pending create/drop events (the session loop resolves them
    /// into probe windows, exactly like DdlState's / PartState's).
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

    fn live_bt(&self) -> Vec<usize> {
        self.bt
            .iter()
            .enumerate()
            .filter(|(_, t)| t.live)
            .map(|(i, _)| i)
            .collect()
    }

    fn live_adv(&self) -> Vec<usize> {
        self.adv
            .iter()
            .enumerate()
            .filter(|(_, t)| t.live)
            .map(|(i, _)| i)
            .collect()
    }

    fn live_gist(&self) -> Vec<usize> {
        self.gist
            .iter()
            .enumerate()
            .filter(|(_, t)| t.live)
            .map(|(i, _)| i)
            .collect()
    }
}

/// Union a closed interval into a disjoint sorted interval set (adjacent
/// intervals merge: exact-deletion bookkeeping, not geometry).
fn interval_union(set: &mut Vec<(i64, i64)>, lo: i64, hi: i64) {
    let (mut lo, mut hi) = (lo, hi);
    let mut out: Vec<(i64, i64)> = Vec::with_capacity(set.len() + 1);
    for &(a, b) in set.iter() {
        if b + 1 < lo || hi + 1 < a {
            out.push((a, b)); // disjoint, keep
        } else {
            lo = lo.min(a); // overlapping/adjacent, absorb
            hi = hi.max(b);
        }
    }
    out.push((lo, hi));
    out.sort();
    *set = out;
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_idx_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("idx");
    let action = g.weights.pick(
        g.rng,
        &[
            "idx:create",
            "idx:drop",
            "idx:query",
            "idx:explain",
            "idx:knn",
            "idx:churn",
            "idx:bt",
            "idx:adv",
            "idx:stats",
            "idx:gist",
        ],
    );
    match action {
        "idx:create" => gen_create(g),
        "idx:drop" => gen_drop(g),
        "idx:query" => gen_query(g),
        "idx:explain" => gen_explain(g),
        "idx:knn" => gen_knn(g),
        "idx:bt" => gen_bt(g),
        "idx:adv" => gen_adv(g),
        "idx:stats" => gen_stats(g),
        "idx:gist" => gen_gist(g),
        _ => gen_churn(g),
    }
}

// --------------------------------------------------------------- data ----

/// The deterministic row source for pks lo..=hi: one SELECT over
/// generate_series with pure integer formulas (identical on both engines by
/// construction; every value is a function of the pk alone, so churn
/// re-inserts are self-consistent too).
fn row_source(lo: i64, hi: i64) -> String {
    format!(
        "SELECT i, point((i * 17) % {PT_MX}, (i * 31) % {PT_MY}), \
         box(point((i * 7) % 97, (i * 11) % 89), \
         point((i * 7) % 97 + 1 + (i % 13), (i * 11) % 89 + 1 + (i % 17))), \
         int4range((i * 5) % {RNG_M}, (i * 5) % {RNG_M} + 1 + (i % 50)), \
         ('10.' || ((i * 7) % 256) || '.' || ((i * 13) % 256) || '.' || (i % 256))::inet, \
         to_tsvector('english', 'w' || (i % {WORD_M}) || ' w' || ((i * 13) % {WORD_M}) || \
         ' w' || ((i * 29) % {WORD_M}) || ' common' || (i % {COMMON_M})), \
         jsonb_build_object('a', i % {JSON_A}, 'b', (i * 3) % 13, 'tag', 'v' || (i % {JSON_TAG})), \
         ARRAY[i % {ARR_M}, (i * 5) % {ARR_M}, (i * 11) % {ARR_M}], \
         'p' || (i % {TXT_P}) || '_' || ((i * 23) % {TXT_M}), \
         timestamp '2020-01-01 00:00:00' + ((i * 37) % {TS_M}) * interval '1 minute', \
         (i * 13) % {KINT_M} FROM generate_series({lo}, {hi}) i"
    )
}

// ------------------------------------------------------------- create ----

/// Optional (weighted) index shapes beyond the always-created core set.
/// Kind tag, USING clause tail. Every one hand-verified on both engines.
const EXTRA_INDEXES: &[(&str, &str)] = &[
    ("idx:x:kdpt", "USING spgist (pt kd_point_ops)"),
    ("idx:x:spgrange", "USING spgist (rng)"),
    ("idx:x:spginet", "USING spgist (addr)"),
    ("idx:x:gistpt", "USING gist (pt)"),
    ("idx:x:gistrange", "USING gist (rng)"),
    ("idx:x:gisttsv", "USING gist (tsv)"),
    ("idx:x:ginarr", "USING gin (arr)"),
    ("idx:x:gintsv", "USING gin (tsv)"),
    ("idx:x:ginpath", "USING gin (doc jsonb_path_ops)"),
    ("idx:x:brints", "USING brin (ts)"),
    ("idx:x:brininet", "USING brin (addr inet_inclusion_ops)"),
    ("idx:x:spgbox", "USING spgist (bx)"),
    ("idx:x:hashtxt", "USING hash (txt)"),
    ("idx:x:hashint", "USING hash (k_int)"),
    ("idx:x:btmulti", "(k_int, txt)"),
    ("idx:x:partial", "USING gist (bx) WHERE k_int > 250"),
    ("idx:x:expr", "(((k_int % 7)))"),
];

fn gen_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.idx.live_tables().len() >= MAX_LIVE_TABLES {
        g.fire("idx:cap:tables");
        return gen_drop(g);
    }
    g.fire("idx:create");
    let name = format!("fz_iam_{}", g.idx.next_table);
    g.idx.next_table += 1;
    let rows = match g.weights.pick(g.rng, &["idx:rows:400", "idx:rows:900", "idx:rows:1800"]) {
        "idx:rows:400" => 400,
        "idx:rows:900" => 900,
        _ => 1800,
    };

    // autovacuum_enabled = off on EVERY idx fixture (round-18a, seed
    // 4001601682479221144 on fz_btd_; same rule as exd RB-15 / par
    // round-14): bulk loads cross autovacuum_vacuum_insert_threshold and
    // mass-delete churn crosses the autoanalyze threshold, so an
    // autovacuum landing on exactly ONE engine flips a later compared
    // EXPLAIN (COSTS OFF) plan — and on the B2 injected-stats probes it
    // silently overwrites the injected stats. Blanket across all six
    // fixture families; see creates_pin_autovacuum_off.
    let mut stmts = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {} ({}) WITH (autovacuum_enabled = off);",
            name, COLS_SQL
        )),
        StmtKind::Raw(format!("INSERT INTO {} {};", name, row_source(1, rows))),
    ];

    // Core index set: always present, so every query family has its AM to
    // engage and every table build drives the SP-GiST/GiST/GIN/BRIN insert
    // paths (doPickSplit fires during these builds).
    let mut indexes: Vec<IdxIndex> = Vec::new();
    let add = |g: &mut Gen, stmts: &mut Vec<StmtKind>, indexes: &mut Vec<IdxIndex>,
                   kind: &'static str,
                   tail: &str| {
        let iname = format!("fz_iami_{}", g.idx.next_index);
        g.idx.next_index += 1;
        stmts.push(StmtKind::Raw(format!("CREATE INDEX {} ON {} {};", iname, name, tail)));
        indexes.push(IdxIndex { name: iname, kind });
    };
    for (kind, tail) in [
        ("idx:core:spgpt", "USING spgist (pt)"),
        ("idx:core:spgtxt", "USING spgist (txt)"),
        ("idx:core:gistbx", "USING gist (bx)"),
        ("idx:core:gindoc", "USING gin (doc)"),
        ("idx:core:brinint", "USING brin (k_int)"),
    ] {
        g.fire(kind);
        add(g, &mut stmts, &mut indexes, kind, tail);
    }
    // Weighted extras (each an independent include/skip pick, so covloop
    // can steer any single opclass).
    for (kind, tail) in EXTRA_INDEXES {
        if g.weights.pick(g.rng, &[kind, "idx:x:skip"]) == *kind {
            g.fire(kind);
            add(g, &mut stmts, &mut indexes, kind, tail);
        }
    }
    // Deterministic stats: tables stay far below statistics_target * 300
    // rows, so ANALYZE samples every row — identical stats, identical plans.
    stmts.push(StmtKind::Raw(format!("ANALYZE {};", name)));

    g.idx.tables.push(IdxTable { name: name.clone(), live: true, next_pk: rows, indexes });
    g.idx.events.push(DdlEvent {
        table: name,
        pk: "pk".to_string(),
        kind: DdlEventKind::Created,
    });
    stmts
}

// --------------------------------------------------------------- drop ----

fn gen_drop(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.idx.live_tables();
    if live.is_empty() {
        // Never recurses: an empty population is below the cap by definition.
        g.fire("idx:fallback:create");
        return gen_create(g);
    }
    g.fire("idx:drop");
    let ti = live[g.rng.below_usize(live.len())];
    let name = g.idx.tables[ti].name.clone();
    g.idx.tables[ti].live = false;
    g.idx.events.push(DdlEvent {
        table: name.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![StmtKind::Raw(format!("DROP TABLE {};", name))]
}

// ---------------------------------------------------------- predicates ----

/// One AM-engaging predicate for the picked family, with literals drawn
/// from the data-formula pools. Returns (family production, predicate SQL,
/// eligible for the bitmap-off bracket — amgettuple families only: GIN and
/// BRIN are bitmap-only, forcing bitmap off there just yields an expensive
/// seqscan).
fn family_pred(g: &mut Gen) -> (&'static str, String, bool) {
    let fam = g.weights.pick(
        g.rng,
        &[
            "idx:fam:point",
            "idx:fam:box",
            "idx:fam:boxdir",
            "idx:fam:range",
            "idx:fam:inet",
            "idx:fam:tsv",
            "idx:fam:jsonb",
            "idx:fam:array",
            "idx:fam:brin",
            "idx:fam:hash",
        ],
    );
    let pred = match fam {
        "idx:fam:point" => match g.rng.below(3) {
            0 => {
                let x1 = g.rng.below(PT_MX - 30);
                let y1 = g.rng.below(PT_MY - 30);
                let x2 = x1 + 1 + g.rng.below(90);
                let y2 = y1 + 1 + g.rng.below(90);
                format!("pt <@ box(point({x1}, {y1}), point({x2}, {y2}))")
            }
            1 => format!("pt << point({}, 0)", g.rng.below(PT_MX)),
            _ => format!("pt >> point({}, 0)", g.rng.below(PT_MX)),
        },
        "idx:fam:box" => {
            let x1 = g.rng.below(97);
            let y1 = g.rng.below(89);
            match g.rng.below(3) {
                0 => {
                    let w = 1 + g.rng.below(40);
                    let h = 1 + g.rng.below(40);
                    format!(
                        "bx && box(point({x1}, {y1}), point({}, {}))",
                        x1 + w,
                        y1 + h
                    )
                }
                1 => {
                    let w = 20 + g.rng.below(80);
                    let h = 20 + g.rng.below(80);
                    format!(
                        "bx <@ box(point({x1}, {y1}), point({}, {}))",
                        x1 + w,
                        y1 + h
                    )
                }
                _ => format!("bx @> box(point({x1}, {y1}), point({x1}, {y1}))"),
            }
        }
        // Directional box strategies (verified q6-hv3): engage the gist box
        // core index AND the weighted spgist box index (geo_spgist 4D
        // quadrant paths above4D/left4D/overLeft4D & friends).
        "idx:fam:boxdir" => {
            let x = g.rng.below(97);
            let y = g.rng.below(89);
            match g.rng.below(4) {
                0 => format!("bx << box(point({x}, 0), point({}, 1))", x + 1),
                1 => format!("bx |>> box(point(0, {y}), point(1, {}))", y + 1),
                2 => format!("bx &< box(point({x}, 0), point({}, 2))", x + 2),
                _ => format!("bx <<| box(point(0, {y}), point(2, {}))", y + 1),
            }
        }
        "idx:fam:range" => match g.rng.below(3) {
            0 => format!("rng @> {}", g.rng.below(RNG_M + 60)),
            1 => {
                let a = g.rng.below(RNG_M);
                format!("rng && int4range({a}, {})", a + 1 + g.rng.below(150))
            }
            _ => {
                let a = g.rng.below(RNG_M);
                format!("rng <@ int4range({a}, {})", a + 100 + g.rng.below(500))
            }
        },
        "idx:fam:inet" => match g.rng.below(3) {
            0 => format!("addr <<= inet '10.{}.0.0/16'", g.rng.below(256)),
            1 => format!(
                "addr = inet '10.{}.{}.{}'",
                g.rng.below(256),
                g.rng.below(256),
                g.rng.below(256)
            ),
            _ => format!("addr < inet '10.{}.0.0'", g.rng.below(256)),
        },
        "idx:fam:tsv" => {
            let a = g.rng.below(WORD_M);
            let b = g.rng.below(WORD_M);
            match g.rng.below(4) {
                0 => format!("tsv @@ to_tsquery('english', 'w{a} & common{}')", g.rng.below(COMMON_M)),
                1 => format!("tsv @@ to_tsquery('english', 'w{a} | w{b}')"),
                2 => format!("tsv @@ to_tsquery('english', 'w{a} & !w{b}')"),
                _ => format!("tsv @@ plainto_tsquery('english', 'common{}')", g.rng.below(COMMON_M)),
            }
        }
        "idx:fam:jsonb" => match g.rng.below(5) {
            0 => format!("doc @> '{{\"a\": {}}}'", g.rng.below(JSON_A)),
            1 => format!("doc @> '{{\"tag\": \"v{}\"}}'", g.rng.below(JSON_TAG)),
            2 => format!("doc ? '{}'", ["a", "b", "tag", "zz"][g.rng.below_usize(4)]),
            3 => "doc ?| ARRAY['a', 'zz']".to_string(),
            _ => "doc ?& ARRAY['a', 'tag']".to_string(),
        },
        "idx:fam:array" => match g.rng.below(2) {
            0 => format!(
                "arr && ARRAY[{}, {}]",
                g.rng.below(ARR_M),
                g.rng.below(ARR_M)
            ),
            _ => format!("arr @> ARRAY[{}]", g.rng.below(ARR_M)),
        },
        "idx:fam:brin" => match g.rng.below(3) {
            0 => {
                let a = g.rng.below(KINT_M);
                format!("k_int BETWEEN {a} AND {}", a + g.rng.below(150))
            }
            1 => {
                let a = g.rng.below(TS_M - 20000);
                let b = a + 1 + g.rng.below(40000);
                format!(
                    "ts BETWEEN timestamp '2020-01-01 00:00:00' + {a} * interval '1 minute' \
                     AND timestamp '2020-01-01 00:00:00' + {b} * interval '1 minute'"
                )
            }
            _ => format!("addr <<= inet '10.{}.0.0/16'", g.rng.below(256)),
        },
        _ => {
            // idx:fam:hash
            if g.rng.chance(1, 2) {
                format!(
                    "txt = 'p{}_{}'",
                    g.rng.below(TXT_P),
                    g.rng.below(TXT_M)
                )
            } else {
                format!("k_int = {}", g.rng.below(KINT_M))
            }
        }
    };
    let gettuple = matches!(
        fam,
        "idx:fam:point"
            | "idx:fam:box"
            | "idx:fam:boxdir"
            | "idx:fam:range"
            | "idx:fam:inet"
            | "idx:fam:hash"
    );
    g.fire(fam);
    (fam, pred, gettuple)
}

/// Pick a live table index, or None (caller falls back to create).
fn pick_live(g: &mut Gen) -> Option<usize> {
    let live = g.idx.live_tables();
    if live.is_empty() {
        return None;
    }
    Some(live[g.rng.below_usize(live.len())])
}

/// Wrap `body` statements in the seqscan-off bracket (plus, optionally, the
/// bitmap-off bracket for amgettuple families). SET and RESET live in the
/// same statement group, applied to both sides identically.
fn bracket(g: &mut Gen, body: Vec<StmtKind>, allow_bitmap_off: bool) -> Vec<StmtKind> {
    let bitmap_off = allow_bitmap_off
        && g.weights.pick(g.rng, &["idx:bitmap:off", "idx:bitmap:on"]) == "idx:bitmap:off";
    let mut stmts = vec![StmtKind::Raw("SET enable_seqscan TO off;".to_string())];
    if bitmap_off {
        g.fire("idx:bitmap:off");
        stmts.push(StmtKind::Raw("SET enable_bitmapscan TO off;".to_string()));
    }
    stmts.extend(body);
    if bitmap_off {
        stmts.push(StmtKind::Raw("RESET enable_bitmapscan;".to_string()));
    }
    stmts.push(StmtKind::Raw("RESET enable_seqscan;".to_string()));
    stmts
}

// -------------------------------------------------------------- query ----

fn gen_query(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("idx:fallback:create");
        return gen_create(g);
    };
    g.fire("idx:query");
    let table = g.idx.tables[ti].name.clone();
    let (_, pred, gettuple) = family_pred(g);
    let shape = g.weights.pick(g.rng, &["idx:q:rows", "idx:q:count"]);
    g.fire(shape);
    let body = match shape {
        // pk is the primary key: ORDER BY pk is a total order -> strict
        // ordered compare.
        "idx:q:rows" => format!("SELECT pk FROM {} WHERE {} ORDER BY pk;", table, pred),
        _ => format!("SELECT count(*) FROM {} WHERE {};", table, pred),
    };
    bracket(g, vec![StmtKind::Raw(body)], gettuple)
}

// ------------------------------------------------------------ explain ----

/// EXPLAIN (COSTS OFF) probe: verifies an index scan is actually chosen and
/// that plan choice matches C — plan divergence here is signal (the module
/// exists to compare exactly this surface).
fn gen_explain(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("idx:fallback:create");
        return gen_create(g);
    };
    g.fire("idx:explain");
    let table = g.idx.tables[ti].name.clone();
    let body = if g.rng.chance(1, 5) {
        let (x, y, k) = knn_params(g);
        format!(
            "EXPLAIN (COSTS OFF) SELECT pk FROM {} ORDER BY pt <-> point({}, {}), pk LIMIT {};",
            table, x, y, k
        )
    } else {
        let (_, pred, _) = family_pred(g);
        format!("EXPLAIN (COSTS OFF) SELECT count(*) FROM {} WHERE {};", table, pred)
    };
    bracket(g, vec![StmtKind::Raw(body)], false)
}

// ---------------------------------------------------------------- knn ----

fn knn_params(g: &mut Gen) -> (u64, u64, u64) {
    (g.rng.below(PT_MX), g.rng.below(PT_MY), 5 + g.rng.below(20))
}

/// KNN ordering: distance then pk, so distance ties (equidistant points are
/// common on an integer grid) stay a total order — deterministic compare.
fn gen_knn(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("idx:fallback:create");
        return gen_create(g);
    };
    g.fire("idx:knn");
    let table = g.idx.tables[ti].name.clone();
    let (x, y, k) = knn_params(g);
    let body = format!(
        "SELECT pk FROM {} ORDER BY pt <-> point({}, {}), pk LIMIT {};",
        table, x, y, k
    );
    bracket(g, vec![StmtKind::Raw(body)], false)
}

// -------------------------------------------------------------- churn ----

/// UPDATE/DELETE/INSERT volume plus REINDEX/VACUUM: the page-split (insert
/// paths through full pages), posting-list update, and cleanup
/// (vacuumLeafPage & friends) fuel.
fn gen_churn(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live(g) else {
        g.fire("idx:fallback:create");
        return gen_create(g);
    };
    g.fire("idx:churn");
    let table = g.idx.tables[ti].name.clone();
    let form = g.weights.pick(
        g.rng,
        &[
            "idx:churn:update",
            "idx:churn:delete",
            "idx:churn:insert",
            "idx:churn:reindex",
            "idx:churn:vacuum",
        ],
    );
    g.fire(form);
    match form {
        "idx:churn:update" => {
            // Rewrite indexed payloads on a pk stripe; every new value is a
            // pure function of pk (deterministic both sides).
            let m = 3 + g.rng.below(9);
            let r = g.rng.below(m);
            let set = match g.rng.below(3) {
                0 => format!(
                    "pt = point((pk * 29) % {PT_MX}, (pk * 41) % {PT_MY}), \
                     doc = jsonb_build_object('a', (pk * 7) % {JSON_A}, 'tag', 'u' || (pk % {JSON_TAG}))"
                ),
                1 => format!(
                    "tsv = to_tsvector('english', 'w' || ((pk * 3) % {WORD_M}) || ' common' || (pk % {COMMON_M})), \
                     rng = int4range((pk * 11) % {RNG_M}, (pk * 11) % {RNG_M} + 5)"
                ),
                _ => format!(
                    "arr = ARRAY[(pk * 13) % {ARR_M}, (pk * 17) % {ARR_M}], \
                     txt = 'p' || ((pk * 5) % {TXT_P}) || '_' || ((pk * 37) % {TXT_M}), \
                     k_int = (pk * 19) % {KINT_M}"
                ),
            };
            vec![StmtKind::Raw(format!(
                "UPDATE {} SET {} WHERE pk % {} = {};",
                table, set, m, r
            ))]
        }
        "idx:churn:delete" => {
            let m = 5 + g.rng.below(11);
            let r = g.rng.below(m);
            vec![StmtKind::Raw(format!(
                "DELETE FROM {} WHERE pk % {} = {};",
                table, m, r
            ))]
        }
        "idx:churn:insert" => {
            let n = 200 + g.rng.below(301) as i64;
            let lo = g.idx.tables[ti].next_pk + 1;
            let hi = g.idx.tables[ti].next_pk + n;
            g.idx.tables[ti].next_pk = hi;
            vec![StmtKind::Raw(format!(
                "INSERT INTO {} {};",
                table,
                row_source(lo, hi)
            ))]
        }
        "idx:churn:reindex" => {
            if g.rng.chance(1, 3) {
                vec![StmtKind::Raw(format!("REINDEX TABLE {};", table))]
            } else {
                let xs = &g.idx.tables[ti].indexes;
                let name = xs[g.rng.below_usize(xs.len())].name.clone();
                vec![StmtKind::Raw(format!("REINDEX INDEX {};", name))]
            }
        }
        _ => vec![StmtKind::Raw(format!("VACUUM {};", table))],
    }
}

// ------------------------------------------------- B2: btree depth ----

/// Deterministic bt row source (every column a pure function of pk, so
/// recycle re-inserts reproduce exactly the rows a massdel removed).
fn bt_row_source(shape: BtShape, lo: i64, hi: i64) -> String {
    match shape {
        BtShape::Int => format!(
            "SELECT i, (i * 13) % {BT_KINT_M} FROM generate_series({lo}, {hi}) i"
        ),
        BtShape::Text => format!(
            "SELECT i, 'k' || lpad(i::text, {BT_KEY_PAD}, '0') FROM generate_series({lo}, {hi}) i"
        ),
    }
}

/// A text-shape key literal expression for the numeric key `v` (same
/// padding as the row source, so predicates land inside real key space).
fn bt_key_expr(v: i64) -> String {
    format!("'k' || lpad('{v}', {BT_KEY_PAD}, '0')")
}

fn gen_bt(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("idx:bt");
    let action = g.weights.pick(
        g.rng,
        &[
            "idx:bt:create",
            "idx:bt:massdel",
            "idx:bt:delvac",
            "idx:bt:vacuum",
            "idx:bt:recycle",
            "idx:bt:query",
            "idx:bt:drop",
        ],
    );
    match action {
        "idx:bt:create" => bt_create(g),
        "idx:bt:massdel" => bt_massdel(g),
        "idx:bt:delvac" => bt_delvac(g),
        "idx:bt:vacuum" => bt_vacuum(g),
        "idx:bt:recycle" => bt_recycle(g),
        "idx:bt:query" => bt_query(g),
        _ => bt_drop(g),
    }
}

fn pick_live_bt(g: &mut Gen) -> Option<usize> {
    let live = g.idx.live_bt();
    if live.is_empty() {
        return None;
    }
    Some(live[g.rng.below_usize(live.len())])
}

fn bt_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.idx.live_bt().len() >= MAX_BT_TABLES {
        g.fire("idx:bt:cap");
        return bt_massdel(g);
    }
    g.fire("idx:bt:create");
    let name = format!("fz_btd_{}", g.idx.next_bt);
    g.idx.next_bt += 1;
    let iname = format!("fz_btdi_{}", g.idx.next_bt_index);
    g.idx.next_bt_index += 1;
    let shape = if g.weights.pick(g.rng, &["idx:bt:shape:int", "idx:bt:shape:text"])
        == "idx:bt:shape:int"
    {
        g.fire("idx:bt:shape:int");
        BtShape::Int
    } else {
        g.fire("idx:bt:shape:text");
        BtShape::Text
    };
    // Depth: int4 pks fan out ~360/page (depth 2 above ~400 rows); the
    // 96-char text keys fan out ~76/page, so 9000 rows build a depth-3
    // tree whose internal pages have internal children (verified with
    // pageinspect bt_metap on the hand-probe shape).
    let (cols, sec, rows) = match shape {
        BtShape::Int => {
            let rows = match g.weights.pick(g.rng, &["idx:bt:rows:3000", "idx:bt:rows:6000"]) {
                "idx:bt:rows:3000" => {
                    g.fire("idx:bt:rows:3000");
                    3000
                }
                _ => {
                    g.fire("idx:bt:rows:6000");
                    6000
                }
            };
            ("pk int4 PRIMARY KEY, k_int int4", "k_int", rows)
        }
        BtShape::Text => ("pk int4 PRIMARY KEY, t text NOT NULL", "t", 9000),
    };
    // autovacuum_enabled = off (round-18a, seed 4001601682479221144; same
    // rule as exd RB-15 / par round-14): the 3000-9000-row bulk load
    // crosses autovacuum_vacuum_insert_threshold on its own, and an
    // autovacuum landing on exactly ONE engine flips a later compared
    // EXPLAIN (COSTS OFF) plan (verified flap: 5-row Bitmap Heap Scan vs
    // 3-row Index Only Scan across the VACUUM-state ladder).
    let stmts = vec![
        StmtKind::Raw(format!("CREATE TABLE {} ({}) WITH (autovacuum_enabled = off);", name, cols)),
        StmtKind::Raw(format!("CREATE INDEX {} ON {} ({});", iname, name, sec)),
        StmtKind::Raw(format!("INSERT INTO {} {};", name, bt_row_source(shape, 1, rows))),
        StmtKind::Raw(format!("ANALYZE {};", name)),
    ];
    g.idx.bt.push(BtTable {
        name: name.clone(),
        shape,
        live: true,
        next_pk: rows,
        deleted: Vec::new(),
    });
    g.idx.events.push(DdlEvent { table: name, pk: "pk".to_string(), kind: DdlEventKind::Created });
    stmts
}

/// Mass contiguous DELETE: wide enough (a quarter to a half of the pk
/// space) that whole leaf pages — and, on the depth-3 text shape, whole
/// internal-page subtrees — go completely empty. A third of the deletes
/// anchor at the right end (rightmost-page deletion is a distinct path).
fn bt_del_stmt(g: &mut Gen, ti: usize) -> (String, StmtKind) {
    let t = &g.idx.bt[ti];
    let (name, np) = (t.name.clone(), t.next_pk);
    let len = np / 4 + g.rng.below((np / 4) as u64) as i64;
    let (lo, hi) = if g.rng.chance(1, 3) {
        (np - len + 1, np)
    } else {
        let lo = 1 + g.rng.below((np - len) as u64) as i64;
        (lo, lo + len - 1)
    };
    interval_union(&mut g.idx.bt[ti].deleted, lo, hi);
    let stmt = StmtKind::Raw(format!(
        "DELETE FROM {} WHERE pk BETWEEN {} AND {};",
        name, lo, hi
    ));
    (name, stmt)
}

fn bt_massdel(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_bt(g) else {
        g.fire("idx:bt:fallback:create");
        return bt_create(g);
    };
    g.fire("idx:bt:massdel");
    let (_, stmt) = bt_del_stmt(g, ti);
    vec![stmt]
}

/// The paired form: DELETE + VACUUM in one group, so btbulkdelete always
/// sees the freshly-emptied pages (a lone massdel can be recycled over
/// before any vacuum production lands on the same table — the pairing is
/// the page-deletion guarantee, not a hope).
fn bt_delvac(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_bt(g) else {
        g.fire("idx:bt:fallback:create");
        return bt_create(g);
    };
    g.fire("idx:bt:delvac");
    let (name, stmt) = bt_del_stmt(g, ti);
    vec![stmt, StmtKind::Raw(format!("VACUUM {};", name))]
}

/// Page deletion itself happens here: VACUUM runs btbulkdelete, which
/// calls _bt_pagedel / _bt_mark_page_halfdead / _bt_unlink_halfdead_page
/// on the pages the mass delete emptied.
fn bt_vacuum(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_bt(g) else {
        g.fire("idx:bt:fallback:create");
        return bt_create(g);
    };
    g.fire("idx:bt:vacuum");
    vec![StmtKind::Raw(format!("VACUUM {};", g.idx.bt[ti].name))]
}

/// Re-insert one exactly-deleted interval: after a vacuum this is the
/// page-recycle path (the burst allocates from pages the deletion pass
/// freed); before one it exercises reuse of half-dead space.
fn bt_recycle(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_bt(g) else {
        g.fire("idx:bt:fallback:create");
        return bt_create(g);
    };
    let Some((lo, hi)) = g.idx.bt[ti].deleted.pop() else {
        g.fire("idx:bt:recycle:empty");
        return bt_massdel(g);
    };
    g.fire("idx:bt:recycle");
    let t = &g.idx.bt[ti];
    vec![StmtKind::Raw(format!(
        "INSERT INTO {} {};",
        t.name,
        bt_row_source(t.shape, lo, hi)
    ))]
}

fn bt_query(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_bt(g) else {
        g.fire("idx:bt:fallback:create");
        return bt_create(g);
    };
    g.fire("idx:bt:query");
    let t = &g.idx.bt[ti];
    let (name, np, shape) = (t.name.clone(), t.next_pk, t.shape);
    let shape_prod = g.weights.pick(
        g.rng,
        &["idx:bt:q:pkrange", "idx:bt:q:pkcount", "idx:bt:q:sec", "idx:bt:q:seccount"],
    );
    g.fire(shape_prod);
    let body = match shape_prod {
        "idx:bt:q:pkrange" => {
            let a = 1 + g.rng.below(np as u64) as i64;
            let b = a + g.rng.below(300) as i64;
            format!("SELECT pk FROM {} WHERE pk BETWEEN {} AND {} ORDER BY pk;", name, a, b)
        }
        "idx:bt:q:pkcount" => {
            let a = 1 + g.rng.below((np / 2) as u64) as i64;
            let b = a + g.rng.below(np as u64) as i64;
            format!("SELECT count(*) FROM {} WHERE pk BETWEEN {} AND {};", name, a, b)
        }
        "idx:bt:q:sec" => match shape {
            BtShape::Int => format!(
                "SELECT pk FROM {} WHERE k_int = {} ORDER BY pk;",
                name,
                g.rng.below(BT_KINT_M)
            ),
            BtShape::Text => {
                let a = 1 + g.rng.below(np as u64) as i64;
                let b = a + g.rng.below(120) as i64;
                format!(
                    "SELECT pk FROM {} WHERE t BETWEEN {} AND {} ORDER BY pk;",
                    name,
                    bt_key_expr(a),
                    bt_key_expr(b)
                )
            }
        },
        _ => match shape {
            BtShape::Int => {
                let a = g.rng.below(BT_KINT_M);
                format!(
                    "SELECT count(*) FROM {} WHERE k_int BETWEEN {} AND {};",
                    name,
                    a,
                    a + g.rng.below(30)
                )
            }
            BtShape::Text => format!(
                "SELECT count(*) FROM {} WHERE t < {};",
                name,
                bt_key_expr(1 + g.rng.below(np as u64) as i64)
            ),
        },
    };
    bracket(g, vec![StmtKind::Raw(body)], true)
}

fn bt_drop(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_bt(g) else {
        g.fire("idx:bt:fallback:create");
        return bt_create(g);
    };
    g.fire("idx:bt:drop");
    let name = g.idx.bt[ti].name.clone();
    g.idx.bt[ti].live = false;
    g.idx.events.push(DdlEvent {
        table: name.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![StmtKind::Raw(format!("DROP TABLE {};", name))]
}

// -------------------------------------- B2: adversarial picksplit ----

/// A big deterministic (possibly self-intersecting — legal) polygon
/// literal: n vertices on integer formulas, ~6 bytes each, so 200-360
/// vertices give a 1.5-2.5KB datum.
fn big_poly_literal(n: u64) -> String {
    let mut s = String::from("(");
    for j in 0..n {
        if j > 0 {
            s.push(',');
        }
        s.push_str(&format!("({},{})", (j * 17) % 211, (j * 31) % 223));
    }
    s.push(')');
    s
}

/// The three adversarial insert batches, as (pk-range row source) SQL.
fn adv_same_source(t: &AdvTable, lo: i64, hi: i64) -> String {
    format!(
        "SELECT i, point({}, {}), 'same_{}_key_constant_key_constant', \
         polygon '((0,0),(4,0),(4,4),(0,4))' FROM generate_series({lo}, {hi}) i",
        t.sx, t.sy, t.name
    )
}

fn adv_line_source(t: &AdvTable, lo: i64, hi: i64) -> String {
    format!(
        "SELECT i, point((i * 7) % 997, {}), 'line_' || (i % 11), \
         polygon '((1,1),(3,1),(2,5))' FROM generate_series({lo}, {hi}) i",
        t.sy
    )
}

fn adv_big_source(poly: &str, lo: i64, hi: i64) -> String {
    format!(
        "SELECT i, point(1, i % 50), {ADV_PREFIX_SQL} || repeat('y', (i * 37) % 2000) || '_' || i, \
         polygon '{poly}' FROM generate_series({lo}, {hi}) i"
    )
}

fn gen_adv(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("idx:adv");
    let action = g.weights.pick(
        g.rng,
        &["idx:adv:create", "idx:adv:insert", "idx:adv:query", "idx:adv:churn", "idx:adv:drop"],
    );
    match action {
        "idx:adv:create" => adv_create(g),
        "idx:adv:insert" => adv_insert(g),
        "idx:adv:query" => adv_query(g),
        "idx:adv:churn" => adv_churn(g),
        _ => adv_drop(g),
    }
}

fn pick_live_adv(g: &mut Gen) -> Option<usize> {
    let live = g.idx.live_adv();
    if live.is_empty() {
        return None;
    }
    Some(live[g.rng.below_usize(live.len())])
}

fn adv_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.idx.live_adv().len() >= MAX_ADV_TABLES {
        g.fire("idx:adv:cap");
        return adv_query(g);
    }
    g.fire("idx:adv:create");
    let name = format!("fz_adv_{}", g.idx.next_adv);
    g.idx.next_adv += 1;
    let t = AdvTable {
        name: name.clone(),
        live: true,
        next_pk: 0,
        sx: g.rng.below(50),
        sy: g.rng.below(50),
    };
    let same_n = 900 + g.rng.below(600) as i64;
    let line_n = 700 + g.rng.below(500) as i64;
    let big_n = 220 + g.rng.below(120) as i64;
    let poly = big_poly_literal(200 + g.rng.below(160));
    let mut stmts = vec![StmtKind::Raw(format!(
        "CREATE TABLE {} (pk int4 PRIMARY KEY, pt point, txt text, poly polygon) \
         WITH (autovacuum_enabled = off);",
        name
    ))];
    // Build-path splits: load all three distributions, then index.
    stmts.push(StmtKind::Raw(format!("INSERT INTO {} {};", name, adv_same_source(&t, 1, same_n))));
    stmts.push(StmtKind::Raw(format!(
        "INSERT INTO {} {};",
        name,
        adv_line_source(&t, same_n + 1, same_n + line_n)
    )));
    stmts.push(StmtKind::Raw(format!(
        "INSERT INTO {} {};",
        name,
        adv_big_source(&poly, same_n + line_n + 1, same_n + line_n + big_n)
    )));
    for (kind, tail) in [
        ("idx:adv:x:spgpt", "USING spgist (pt)"),
        ("idx:adv:x:kdpt", "USING spgist (pt kd_point_ops)"),
        ("idx:adv:x:spgtxt", "USING spgist (txt)"),
        ("idx:adv:x:gistpt", "USING gist (pt)"),
        ("idx:adv:x:gistpoly", "USING gist (poly)"),
    ] {
        g.fire(kind);
        let iname = format!("fz_advi_{}", g.idx.next_index);
        g.idx.next_index += 1;
        stmts.push(StmtKind::Raw(format!("CREATE INDEX {} ON {} {};", iname, name, tail)));
    }
    stmts.push(StmtKind::Raw(format!("ANALYZE {};", name)));
    let mut t = t;
    t.next_pk = same_n + line_n + big_n;
    g.idx.adv.push(t);
    g.idx.events.push(DdlEvent { table: name, pk: "pk".to_string(), kind: DdlEventKind::Created });
    stmts
}

/// Post-index insert burst: the *insert-path* splits (doPickSplit under
/// spgdoinsert / gistSplit under gistdoinsert), as opposed to the
/// build-path splits adv_create drives.
fn adv_insert(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_adv(g) else {
        g.fire("idx:adv:fallback:create");
        return adv_create(g);
    };
    let shape = g.weights.pick(g.rng, &["idx:adv:ins:same", "idx:adv:ins:line", "idx:adv:ins:big"]);
    g.fire(shape);
    let n = match shape {
        "idx:adv:ins:same" => 400 + g.rng.below(400) as i64,
        "idx:adv:ins:line" => 300 + g.rng.below(300) as i64,
        _ => 100 + g.rng.below(100) as i64,
    };
    let lo = g.idx.adv[ti].next_pk + 1;
    let hi = g.idx.adv[ti].next_pk + n;
    g.idx.adv[ti].next_pk = hi;
    let t = &g.idx.adv[ti];
    let src = match shape {
        "idx:adv:ins:same" => adv_same_source(t, lo, hi),
        "idx:adv:ins:line" => adv_line_source(t, lo, hi),
        _ => {
            let poly = big_poly_literal(200 + g.rng.below(160));
            adv_big_source(&poly, lo, hi)
        }
    };
    vec![StmtKind::Raw(format!("INSERT INTO {} {};", t.name, src))]
}

fn adv_query(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_adv(g) else {
        g.fire("idx:adv:fallback:create");
        return adv_create(g);
    };
    g.fire("idx:adv:query");
    let (name, sx, sy) = {
        let t = &g.idx.adv[ti];
        (t.name.clone(), t.sx, t.sy)
    };
    let shape = g.weights.pick(
        g.rng,
        &[
            "idx:adv:q:box",
            "idx:adv:q:left",
            "idx:adv:q:txteq",
            "idx:adv:q:txtlt",
            "idx:adv:q:poly",
            "idx:adv:q:knn",
            "idx:adv:q:explain",
        ],
    );
    g.fire(shape);
    let body = match shape {
        "idx:adv:q:box" => {
            let x1 = g.rng.below(60);
            let y1 = g.rng.below(60);
            let q = format!(
                "pt <@ box(point({x1}, {y1}), point({}, {}))",
                x1 + 1 + g.rng.below(50),
                y1 + 1 + g.rng.below(50)
            );
            if g.rng.chance(1, 2) {
                format!("SELECT count(*) FROM {} WHERE {};", name, q)
            } else {
                format!("SELECT pk FROM {} WHERE {} ORDER BY pk LIMIT 40;", name, q)
            }
        }
        "idx:adv:q:left" => {
            let x = g.rng.below(997);
            if g.rng.chance(1, 2) {
                format!("SELECT count(*) FROM {} WHERE pt << point({}, 0);", name, x)
            } else {
                format!("SELECT count(*) FROM {} WHERE pt >> point({}, 0);", name, x)
            }
        }
        // The all-the-same constant: an equality that selects thousands
        // of identical keys through the radix / quad trees.
        "idx:adv:q:txteq" => format!(
            "SELECT count(*) FROM {} WHERE txt = 'same_{}_key_constant_key_constant';",
            name, name
        ),
        // Range probe at the shared-prefix boundary (the oversize-text
        // radix surface; the standing B2-F1 divergence lives here).
        "idx:adv:q:txtlt" => {
            let b = ["'longprefix_a'", "'longprefix_z'", "'line_5'", "'same_z'"]
                [g.rng.below_usize(4)];
            format!("SELECT count(*) FROM {} WHERE txt < {};", name, b)
        }
        "idx:adv:q:poly" => {
            let x1 = g.rng.below(200);
            let y1 = g.rng.below(200);
            format!(
                "SELECT count(*) FROM {} WHERE poly && polygon '(({x1},{y1}),({},{y1}),({},{}))';",
                name,
                x1 + 1 + g.rng.below(30),
                x1 + 1,
                y1 + 1 + g.rng.below(30)
            )
        }
        "idx:adv:q:knn" => format!(
            "SELECT pk FROM {} ORDER BY pt <-> point({}, {}), pk LIMIT {};",
            name,
            g.rng.below(997),
            sy + g.rng.below(10),
            5 + g.rng.below(15)
        ),
        _ => format!(
            "EXPLAIN (COSTS OFF) SELECT count(*) FROM {} WHERE pt <@ box(point({sx}, {sy}), point({}, {}));",
            name,
            sx + 5,
            sy + 5
        ),
    };
    bracket(g, vec![StmtKind::Raw(body)], true)
}

/// Delete/vacuum churn over the adversarial distributions: spgvacuumscan
/// over all-the-same chains and long suffix chains, then more insert
/// bursts land on the vacuumed structure.
fn adv_churn(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_adv(g) else {
        g.fire("idx:adv:fallback:create");
        return adv_create(g);
    };
    let name = g.idx.adv[ti].name.clone();
    let form = g.weights.pick(
        g.rng,
        &["idx:adv:churn:delete", "idx:adv:churn:vacuum", "idx:adv:churn:reindex"],
    );
    g.fire(form);
    match form {
        "idx:adv:churn:delete" => {
            let m = 3 + g.rng.below(8);
            let r = g.rng.below(m);
            vec![StmtKind::Raw(format!("DELETE FROM {} WHERE pk % {} = {};", name, m, r))]
        }
        "idx:adv:churn:vacuum" => vec![StmtKind::Raw(format!("VACUUM {};", name))],
        _ => vec![StmtKind::Raw(format!("REINDEX TABLE {};", name))],
    }
}

fn adv_drop(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_adv(g) else {
        g.fire("idx:adv:fallback:create");
        return adv_create(g);
    };
    g.fire("idx:adv:drop");
    let name = g.idx.adv[ti].name.clone();
    g.idx.adv[ti].live = false;
    g.idx.events.push(DdlEvent {
        table: name.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![StmtKind::Raw(format!("DROP TABLE {};", name))]
}

// ------------------------------------------ B2: statistics import ----

/// The PG18 pg_restore_*_stats / pg_clear_*_stats surface
/// (attribute_statistics_update / relation_statistics_update), driven
/// over the live fz_btd_* tables. Injected stats are catalog state read
/// back by pg_stats probes on the runner side and steer EXPLAIN probes;
/// the calls' own t/f results are compared too. Both engines verified to
/// ship exactly the restore/clear quartet (no pg_set_*_stats in PG18).
fn gen_stats(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_bt(g) else {
        g.fire("idx:bt:fallback:create");
        return bt_create(g);
    };
    g.fire("idx:stats");
    let t = &g.idx.bt[ti];
    let (name, shape, np) = (t.name.clone(), t.shape, t.next_pk);
    let action = g.weights.pick(
        g.rng,
        &[
            "idx:stats:rel",
            "idx:stats:attr",
            "idx:stats:clear",
            "idx:stats:probe",
            "idx:stats:err",
        ],
    );
    g.fire(action);
    let sql = match action {
        "idx:stats:rel" => {
            let relpages = [10i64, 400, 5000][g.rng.below_usize(3)];
            let reltuples = [1000i64, 50000, 1000000][g.rng.below_usize(3)];
            format!(
                "SELECT pg_restore_relation_stats('schemaname', 'public', 'relname', '{}', \
                 'version', 180000::integer, 'relpages', {}::integer, 'reltuples', {}::real, \
                 'relallvisible', {}::integer, 'relallfrozen', {}::integer);",
                name,
                relpages,
                reltuples,
                relpages * 3 / 4,
                relpages / 2
            )
        }
        "idx:stats:attr" => stats_attr(g, &name, shape),
        "idx:stats:clear" => {
            if g.rng.chance(1, 3) {
                format!("SELECT pg_clear_relation_stats('public', '{}');", name)
            } else {
                let col = match shape {
                    BtShape::Int => ["pk", "k_int"][g.rng.below_usize(2)],
                    BtShape::Text => ["pk", "t"][g.rng.below_usize(2)],
                };
                format!(
                    "SELECT pg_clear_attribute_stats('public', '{}', '{}', false);",
                    name, col
                )
            }
        }
        "idx:stats:probe" => {
            // Unbracketed on purpose: whether the injected stats flip the
            // plan is exactly the differential surface.
            match shape {
                BtShape::Int => match g.rng.below(3) {
                    0 => format!(
                        "EXPLAIN (COSTS OFF) SELECT * FROM {} WHERE k_int = {};",
                        name,
                        g.rng.below(BT_KINT_M)
                    ),
                    1 => {
                        let a = g.rng.below(BT_KINT_M);
                        format!(
                            "EXPLAIN (COSTS OFF) SELECT count(*) FROM {} WHERE k_int BETWEEN {} AND {};",
                            name,
                            a,
                            a + g.rng.below(80)
                        )
                    }
                    _ => format!(
                        "EXPLAIN (COSTS OFF) SELECT * FROM {} WHERE pk < {};",
                        name,
                        1 + g.rng.below(np as u64)
                    ),
                },
                BtShape::Text => format!(
                    "EXPLAIN (COSTS OFF) SELECT count(*) FROM {} WHERE t < {};",
                    name,
                    bt_key_expr(1 + g.rng.below(np as u64) as i64)
                ),
            }
        }
        _ => {
            let fuel = g.weights.pick(
                g.rng,
                &[
                    "idx:stats:err:mismatch",
                    "idx:stats:err:badlit",
                    "idx:stats:err:range",
                    "idx:stats:err:negpages",
                ],
            );
            g.fire(fuel);
            match fuel {
                // Array-length mismatch: C warns and returns f (the
                // standing B2-F2 acceptance divergence lives here).
                "idx:stats:err:mismatch" => format!(
                    "SELECT pg_restore_attribute_stats('schemaname', 'public', 'relname', '{}', \
                     'attname', 'pk', 'inherited', false, 'version', 180000::integer, \
                     'most_common_vals', '{{1,2,3}}'::text, 'most_common_freqs', '{{0.1}}'::real[]);",
                    name
                ),
                "idx:stats:err:badlit" => format!(
                    "SELECT pg_restore_attribute_stats('schemaname', 'public', 'relname', '{}', \
                     'attname', 'pk', 'inherited', false, 'version', 180000::integer, \
                     'histogram_bounds', '{{bad,,\"lit'::text);",
                    name
                ),
                "idx:stats:err:range" => format!(
                    "SELECT pg_restore_attribute_stats('schemaname', 'public', 'relname', '{}', \
                     'attname', 'pk', 'inherited', false, 'version', 180000::integer, \
                     'null_frac', 7.5::real);",
                    name
                ),
                _ => format!(
                    "SELECT pg_restore_relation_stats('schemaname', 'public', 'relname', '{}', \
                     'version', 180000::integer, 'relpages', -7::integer);",
                    name
                ),
            }
        }
    };
    vec![StmtKind::Raw(sql)]
}

/// One plausible pg_restore_attribute_stats call: consistent-length MCV
/// arrays drawn from the table's real value pools, ascending histogram
/// bounds, quantized correlation — everything a literal, identical on
/// both sides.
fn stats_attr(g: &mut Gen, name: &str, shape: BtShape) -> String {
    let variant = g.weights.pick(
        g.rng,
        &["idx:stats:attr:full", "idx:stats:attr:min", "idx:stats:attr:hist"],
    );
    g.fire(variant);
    let null_frac = ["0", "0.1", "0.5"][g.rng.below_usize(3)];
    let n_distinct = ["-1", "-0.5", "25", "500"][g.rng.below_usize(4)];
    let corr = ["-1", "-0.5", "-0.25", "0", "0.25", "0.5", "0.9", "1"][g.rng.below_usize(8)];
    let (col, mcv, hist) = match shape {
        BtShape::Int => {
            let col = ["pk", "k_int"][g.rng.below_usize(2)];
            let v = g.rng.below(400);
            let mcv = format!("'{{{},{},{}}}'::text", v, v + 13, v + 26);
            let s = g.rng.below(200);
            let d = 1 + g.rng.below(60);
            let hist = format!(
                "'{{{},{},{},{},{}}}'::text",
                s,
                s + d,
                s + 2 * d,
                s + 3 * d,
                s + 4 * d
            );
            (col, mcv, hist)
        }
        BtShape::Text => {
            let col = ["pk", "t"][g.rng.below_usize(2)];
            if col == "pk" {
                let v = g.rng.below(400);
                (
                    col,
                    format!("'{{{},{},{}}}'::text", v, v + 7, v + 19),
                    format!("'{{{},{},{}}}'::text", v, v + 100, v + 500),
                )
            } else {
                let a = 1 + g.rng.below(4000) as i64;
                let key = |v: i64| format!("k{:0>width$}", v, width = BT_KEY_PAD as usize);
                (
                    col,
                    format!("'{{{},{},{}}}'::text", key(a), key(a + 17), key(a + 41)),
                    format!("'{{{},{},{}}}'::text", key(a), key(a + 900), key(a + 2900)),
                )
            }
        }
    };
    let base = format!(
        "SELECT pg_restore_attribute_stats('schemaname', 'public', 'relname', '{}', \
         'attname', '{}', 'inherited', false, 'version', 180000::integer",
        name, col
    );
    match variant {
        "idx:stats:attr:full" => format!(
            "{}, 'null_frac', {}::real, 'avg_width', {}::integer, 'n_distinct', {}::real, \
             'most_common_vals', {}, 'most_common_freqs', '{{0.01,0.005,0.0025}}'::real[], \
             'histogram_bounds', {}, 'correlation', {}::real);",
            base,
            null_frac,
            4 + g.rng.below(90),
            n_distinct,
            mcv,
            hist,
            corr
        ),
        "idx:stats:attr:min" => format!(
            "{}, 'null_frac', {}::real, 'n_distinct', {}::real);",
            base, null_frac, n_distinct
        ),
        _ => format!(
            "{}, 'histogram_bounds', {}, 'correlation', {}::real);",
            base, hist, corr
        ),
    }
}

// ------------------------------------------------- Q6: gist paths ----

/// Deterministic rich-gist row source: every column a pure function of pk
/// (identical on both engines by construction; exact-interval re-inserts
/// reproduce exactly the rows a delvac removed). addr is bijective in pk
/// (pk < 65536 always holds at our volumes), so ORDER BY addr is a total
/// order — the index-only-scan compare handle.
fn gist_row_source(lo: i64, hi: i64) -> String {
    format!(
        "SELECT i, point((i * 17) % 211, (i * 31) % 223), \
         box(point((i * 7) % 97, (i * 11) % 89), point((i * 7) % 97 + 1 + (i % 13), (i * 11) % 89 + 1 + (i % 17))), \
         int8range((i * 5) % {GST_R8_M}, (i * 5) % {GST_R8_M} + 1 + (i % 50)), \
         numrange(((i * 3) % {GST_NUM_M})::numeric / 4, ((i * 3) % {GST_NUM_M})::numeric / 4 + 2 + (i % 9)), \
         tsrange(timestamp '2020-01-01' + ((i * 37) % {GST_MIN_M}) * interval '1 minute', \
         timestamp '2020-01-01' + ((i * 37) % {GST_MIN_M} + 30 + i % 200) * interval '1 minute'), \
         tstzrange(timestamptz '2020-01-01 00:00:00+00' + ((i * 41) % {GST_MIN_M}) * interval '1 minute', \
         timestamptz '2020-01-01 00:00:00+00' + ((i * 41) % {GST_MIN_M} + 45 + i % 100) * interval '1 minute'), \
         daterange(date '2020-01-01' + (i * 3) % {GST_DAY_M}, date '2020-01-01' + (i * 3) % {GST_DAY_M} + 1 + i % 40), \
         int4multirange(int4range((i * 5) % {GST_R8_M}, (i * 5) % {GST_R8_M} + 1 + i % 20), \
         int4range((i * 13) % {GST_MR2_M}, (i * 13) % {GST_MR2_M} + 1 + i % 30)), \
         to_tsquery('english', 'w' || (i % {GST_WORD_M}) || (CASE WHEN i % 3 = 0 THEN ' & w' || ((i * 13) % {GST_WORD_M}) ELSE '' END)), \
         ('10.' || ((i / 256) % 256) || '.' || (i % 256) || '.' || ((i * 7) % 250))::inet, \
         to_tsvector('english', 'w' || (i % {GST_WORD_M}) || ' w' || ((i * 13) % {GST_WORD_M}) || \
         ' w' || ((i * 29) % {GST_WORD_M}) || ' common' || (i % 7)) \
         FROM generate_series({lo}, {hi}) i"
    )
}

/// Registry entry point for the Q6 gist-paths family.
fn gen_gist(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("idx:gist");
    let action = g.weights.pick(
        g.rng,
        &[
            "idx:gist:create",
            "idx:gist:query",
            "idx:gist:ios",
            "idx:gist:prop",
            "idx:gist:delvac",
            "idx:gist:reinsert",
            "idx:gist:buffered",
            "idx:gist:excl",
            "idx:gist:drop",
        ],
    );
    match action {
        "idx:gist:create" => gist_create(g),
        "idx:gist:query" => gist_query(g),
        "idx:gist:ios" => gist_ios(g),
        "idx:gist:prop" => gist_prop(g),
        "idx:gist:delvac" => gist_delvac(g),
        "idx:gist:reinsert" => gist_reinsert(g),
        "idx:gist:buffered" => gist_buffered(g),
        "idx:gist:excl" => gist_excl(g),
        _ => gist_drop(g),
    }
}

fn pick_live_gist(g: &mut Gen) -> Option<usize> {
    let live = g.idx.live_gist();
    if live.is_empty() {
        return None;
    }
    Some(live[g.rng.below_usize(live.len())])
}

/// Core index set: always present, one per gist-paths target file. Kind
/// tag, USING tail, indexed-column count (for property probes).
const GIST_CORE_INDEXES: &[(&str, &str, u32)] = &[
    ("idx:gist:x:r8", "USING gist (r8)", 1),
    ("idx:gist:x:num", "USING gist (rn)", 1),
    ("idx:gist:x:mr", "USING gist (mr)", 1),
    ("idx:gist:x:tsq", "USING gist (tsq)", 1),
    ("idx:gist:x:inet", "USING gist (addr inet_ops)", 1),
    ("idx:gist:x:tsv", "USING gist (tsv tsvector_ops (siglen = 100))", 1),
    ("idx:gist:x:mc", "USING gist (pt, bx)", 2),
    ("idx:gist:x:spgbx", "USING spgist (bx)", 1),
];

/// Weighted extras (each an independent include/skip pick).
const GIST_EXTRA_INDEXES: &[(&str, &str, u32)] = &[
    ("idx:gist:x:ts", "USING gist (rt)", 1),
    ("idx:gist:x:tstz", "USING gist (rz)", 1),
    ("idx:gist:x:date", "USING gist (rd)", 1),
    ("idx:gist:x:ff", "USING gist (bx) WITH (fillfactor = 40)", 1),
];

fn gist_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.idx.live_gist().len() >= MAX_GIST_TABLES {
        g.fire("idx:gist:cap");
        return gist_query(g);
    }
    g.fire("idx:gist:create");
    let name = format!("fz_gst_{}", g.idx.next_gist);
    g.idx.next_gist += 1;
    // UNLOGGED variant: gistbuildempty/spgbuildempty + fake-LSN paths.
    let unlogged = g.weights.pick(g.rng, &["idx:gist:unlogged", "idx:gist:logged"])
        == "idx:gist:unlogged";
    if unlogged {
        g.fire("idx:gist:unlogged");
    }
    let rows = match g.weights.pick(g.rng, &["idx:gist:rows:1200", "idx:gist:rows:2400"]) {
        "idx:gist:rows:1200" => 1200,
        _ => 2400,
    };
    let mut stmts = vec![
        StmtKind::Raw(format!(
            "CREATE {}TABLE {} (pk int4 PRIMARY KEY, pt point, bx box, r8 int8range, \
             rn numrange, rt tsrange, rz tstzrange, rd daterange, mr int4multirange, \
             tsq tsquery, addr inet, tsv tsvector) WITH (autovacuum_enabled = off);",
            if unlogged { "UNLOGGED " } else { "" },
            name
        )),
        StmtKind::Raw(format!("INSERT INTO {} {};", name, gist_row_source(1, rows))),
    ];
    let mut indexes: Vec<IdxIndex> = Vec::new();
    let add = |g: &mut Gen, stmts: &mut Vec<StmtKind>, indexes: &mut Vec<IdxIndex>,
                   kind: &'static str,
                   tail: &str| {
        let iname = format!("fz_gsti_{}", g.idx.next_gist_index);
        g.idx.next_gist_index += 1;
        stmts.push(StmtKind::Raw(format!("CREATE INDEX {} ON {} {};", iname, name, tail)));
        indexes.push(IdxIndex { name: iname, kind });
    };
    for (kind, tail, _) in GIST_CORE_INDEXES {
        g.fire(kind);
        add(g, &mut stmts, &mut indexes, kind, tail);
    }
    for (kind, tail, _) in GIST_EXTRA_INDEXES {
        if g.weights.pick(g.rng, &[kind, "idx:gist:x:skip"]) == *kind {
            g.fire(kind);
            add(g, &mut stmts, &mut indexes, kind, tail);
        }
    }
    stmts.push(StmtKind::Raw(format!("ANALYZE {};", name)));
    g.idx.gist.push(GistTable {
        name: name.clone(),
        live: true,
        next_pk: rows,
        deleted: Vec::new(),
        indexes,
    });
    g.idx.events.push(DdlEvent { table: name, pk: "pk".to_string(), kind: DdlEventKind::Created });
    stmts
}

/// One gist-engaging predicate over the rich table, literals drawn from
/// the row-formula pools (every shape hand-verified, q6-hv3).
fn gist_pred(g: &mut Gen) -> &'static str {
    g.weights.pick(
        g.rng,
        &[
            "idx:gq:r8",
            "idx:gq:num",
            "idx:gq:ts",
            "idx:gq:mr",
            "idx:gq:tsq",
            "idx:gq:inet",
            "idx:gq:tsv",
            "idx:gq:boxdir",
        ],
    )
}

fn gist_pred_sql(g: &mut Gen, fam: &str) -> String {
    match fam {
        "idx:gq:r8" => {
            let a = g.rng.below(GST_R8_M);
            match g.rng.below(5) {
                0 => format!("r8 && int8range({a}, {})", a + 1 + g.rng.below(300)),
                1 => format!("r8 @> {}::int8", g.rng.below(GST_R8_M + 50)),
                2 => format!("r8 -|- int8range({a}, {})", a + 1 + g.rng.below(50)),
                3 => format!("r8 << int8range({a}, {})", a + 1 + g.rng.below(10)),
                _ => format!("r8 >> int8range({a}, {})", a + 1 + g.rng.below(10)),
            }
        }
        "idx:gq:num" => {
            let a = g.rng.below(GST_NUM_M / 4);
            if g.rng.chance(1, 2) {
                format!("rn @> {a}.5::numeric")
            } else {
                format!("rn && numrange({a}, {})", a + 1 + g.rng.below(20))
            }
        }
        "idx:gq:ts" => {
            let a = g.rng.below(GST_MIN_M - 30000);
            let w = 500 + g.rng.below(30000);
            match g.rng.below(3) {
                0 => format!(
                    "rt && tsrange(timestamp '2020-01-01' + {a} * interval '1 minute', \
                     timestamp '2020-01-01' + {} * interval '1 minute')",
                    a + w
                ),
                1 => format!(
                    "rz @> (timestamptz '2020-01-01 00:00:00+00' + {a} * interval '1 minute')"
                ),
                _ => format!(
                    "rd <@ daterange(date '2020-01-01' + {}, date '2020-01-01' + {})",
                    g.rng.below(400),
                    900 + g.rng.below(700)
                ),
            }
        }
        "idx:gq:mr" => {
            let a = g.rng.below(GST_R8_M);
            if g.rng.chance(1, 2) {
                format!("mr && int4range({a}, {})", a + 1 + g.rng.below(200))
            } else {
                format!("mr @> {}", g.rng.below(GST_MR2_M))
            }
        }
        "idx:gq:tsq" => {
            let a = g.rng.below(GST_WORD_M);
            if g.rng.chance(1, 2) {
                format!("tsq @> 'w{a}'::tsquery")
            } else {
                format!(
                    "tsq <@ ('w{a} & w{} & w{}'::tsquery)",
                    g.rng.below(GST_WORD_M),
                    g.rng.below(GST_WORD_M)
                )
            }
        }
        "idx:gq:inet" => match g.rng.below(3) {
            0 => format!("addr <<= inet '10.{}.0.0/16'", g.rng.below(10)),
            1 => format!("addr && inet '10.{}.0.0/15'", g.rng.below(10)),
            _ => format!("addr > inet '10.{}.0.0'", g.rng.below(10)),
        },
        "idx:gq:tsv" => format!(
            "tsv @@ to_tsquery('english', 'w{} & common{}')",
            g.rng.below(GST_WORD_M),
            g.rng.below(7)
        ),
        _ => {
            // idx:gq:boxdir (spgist box 4D strategies + gist box).
            let x = g.rng.below(97);
            let y = g.rng.below(89);
            match g.rng.below(4) {
                0 => format!("bx << box(point({x}, 0), point({}, 1))", x + 1),
                1 => format!("bx |>> box(point(0, {y}), point(1, {}))", y + 1),
                2 => format!("bx &< box(point({x}, 0), point({}, 2))", x + 2),
                _ => format!("bx <<| box(point(0, {y}), point(2, {}))", y + 1),
            }
        }
    }
}

fn gist_query(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_gist(g) else {
        g.fire("idx:gist:fallback:create");
        return gist_create(g);
    };
    g.fire("idx:gist:query");
    let table = g.idx.gist[ti].name.clone();
    // Low-weight direct gtsvector I/O probes (matched output, q6-hv3).
    if g.weights.pick(g.rng, &["idx:gq:gtsv", "idx:gq:pred"]) == "idx:gq:gtsv" {
        g.fire("idx:gq:gtsv");
        let body = if g.rng.chance(1, 2) {
            "SELECT '1'::gtsvector;".to_string()
        } else {
            "SELECT gtsvectorin('1');".to_string()
        };
        return vec![StmtKind::Raw(body)];
    }
    let fam = gist_pred(g);
    g.fire(fam);
    let pred = gist_pred_sql(g, fam);
    // KNN through the multicolumn gist(pt, bx) index: distance then pk.
    if g.weights.pick(g.rng, &["idx:gq:knnbox", "idx:gq:plain"]) == "idx:gq:knnbox" {
        g.fire("idx:gq:knnbox");
        let body = format!(
            "SELECT pk FROM {} ORDER BY bx <-> point({}, {}), pk LIMIT {};",
            table,
            g.rng.below(97),
            g.rng.below(89),
            5 + g.rng.below(15)
        );
        return bracket(g, vec![StmtKind::Raw(body)], false);
    }
    let shape = g.weights.pick(g.rng, &["idx:gq:rows", "idx:gq:count"]);
    g.fire(shape);
    let body = match shape {
        "idx:gq:rows" => format!(
            "SELECT pk FROM {} WHERE {} ORDER BY pk LIMIT {};",
            table,
            pred,
            20 + g.rng.below(40)
        ),
        _ => format!("SELECT count(*) FROM {} WHERE {};", table, pred),
    };
    bracket(g, vec![StmtKind::Raw(body)], true)
}

/// Index-only scan through the inet_ops gist index (its fetch function):
/// seqscan AND bitmapscan forced off, addr bijective in pk so ORDER BY
/// addr is a strict total order.
fn gist_ios(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_gist(g) else {
        g.fire("idx:gist:fallback:create");
        return gist_create(g);
    };
    g.fire("idx:gist:ios");
    let table = g.idx.gist[ti].name.clone();
    let k = g.rng.below(10);
    let n = 10 + g.rng.below(20);
    let sel = format!(
        "SELECT addr FROM {} WHERE addr <<= inet '10.{}.0.0/16' ORDER BY addr LIMIT {};",
        table, k, n
    );
    let mut body = vec![StmtKind::Raw(sel.clone())];
    if g.rng.chance(1, 3) {
        body.push(StmtKind::Raw(format!("EXPLAIN (COSTS OFF) {}", sel)));
    }
    let mut stmts = vec![
        StmtKind::Raw("SET enable_seqscan TO off;".to_string()),
        StmtKind::Raw("SET enable_bitmapscan TO off;".to_string()),
    ];
    stmts.extend(body);
    stmts.push(StmtKind::Raw("RESET enable_bitmapscan;".to_string()));
    stmts.push(StmtKind::Raw("RESET enable_seqscan;".to_string()));
    stmts
}

/// pg_index_column_has_property probes over the live table's gist/spgist
/// indexes (gistproperty/spgproperty; boolean/NULL outputs, comparable).
fn gist_prop(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_gist(g) else {
        g.fire("idx:gist:fallback:create");
        return gist_create(g);
    };
    g.fire("idx:gist:prop");
    let t = &g.idx.gist[ti];
    let ix = &t.indexes[g.rng.below_usize(t.indexes.len())];
    let iname = ix.name.clone();
    let ncols: u32 = if ix.kind == "idx:gist:x:mc" { 2 } else { 1 };
    let col = 1 + g.rng.below(ncols as u64);
    let prop = ["distance_orderable", "returnable", "search_array", "search_nulls"]
        [g.rng.below_usize(4)];
    vec![StmtKind::Raw(format!(
        "SELECT pg_index_column_has_property('{}'::regclass, {}, '{}');",
        iname, col, prop
    ))]
}

/// Mass contiguous DELETE + VACUUM in one group: whole gist/spgist leaf
/// pages go empty and the vacuum pass deletes them (gistdeletepage,
/// gistvacuum page recycling, the integerset dead-TID machinery).
fn gist_delvac(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_gist(g) else {
        g.fire("idx:gist:fallback:create");
        return gist_create(g);
    };
    g.fire("idx:gist:delvac");
    let t = &g.idx.gist[ti];
    let (name, np) = (t.name.clone(), t.next_pk);
    let len = np / 4 + g.rng.below((np / 4) as u64) as i64;
    let (lo, hi) = if g.rng.chance(1, 3) {
        (np - len + 1, np)
    } else {
        let lo = 1 + g.rng.below((np - len) as u64) as i64;
        (lo, lo + len - 1)
    };
    interval_union(&mut g.idx.gist[ti].deleted, lo, hi);
    vec![
        StmtKind::Raw(format!("DELETE FROM {} WHERE pk BETWEEN {} AND {};", name, lo, hi)),
        StmtKind::Raw(format!("VACUUM {};", name)),
    ]
}

/// Re-insert one exactly-deleted interval: after the paired vacuum this
/// allocates from pages the deletion pass freed (gist page reuse + the
/// fake-LSN path on the unlogged variant).
fn gist_reinsert(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_gist(g) else {
        g.fire("idx:gist:fallback:create");
        return gist_create(g);
    };
    let Some((lo, hi)) = g.idx.gist[ti].deleted.pop() else {
        g.fire("idx:gist:reinsert:empty");
        return gist_delvac(g);
    };
    g.fire("idx:gist:reinsert");
    let name = g.idx.gist[ti].name.clone();
    vec![StmtKind::Raw(format!("INSERT INTO {} {};", name, gist_row_source(lo, hi)))]
}

/// Self-contained buffered-build group: >4096 tuples so buffering=on flips
/// from the stats phase into the buffered build (gistInitBuffering,
/// gistbufferinginserttuples, gistEmptyAllBuffers, gistbuildbuffers.c).
fn gist_buffered(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("idx:gist:buffered");
    let name = format!("fz_gbuf_{}", g.idx.next_gist);
    g.idx.next_gist += 1;
    let rows = if g.rng.chance(1, 2) { 6000 } else { 9000 };
    let mut stmts = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {} (pk int4 PRIMARY KEY, pt point) \
             WITH (autovacuum_enabled = off);",
            name
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {} SELECT i, point((i * 17) % 701, (i * 31) % 733) \
             FROM generate_series(1, {}) i;",
            name, rows
        )),
        StmtKind::Raw(format!(
            "CREATE INDEX fz_gbufi_{} ON {} USING gist (pt) WITH (buffering = on);",
            g.idx.next_gist_index, name
        )),
    ];
    g.idx.next_gist_index += 1;
    if g.rng.chance(1, 3) {
        g.fire("idx:gist:buffered:off");
        stmts.push(StmtKind::Raw(format!(
            "CREATE INDEX fz_gbufi_{} ON {} USING gist (pt) WITH (buffering = off);",
            g.idx.next_gist_index, name
        )));
        g.idx.next_gist_index += 1;
    }
    stmts.push(StmtKind::Raw(format!("ANALYZE {};", name)));
    let x1 = g.rng.below(500);
    let y1 = g.rng.below(500);
    stmts.push(StmtKind::Raw("SET enable_seqscan TO off;".to_string()));
    stmts.push(StmtKind::Raw(format!(
        "SELECT count(*) FROM {} WHERE pt <@ box(point({x1}, {y1}), point({}, {}));",
        name,
        x1 + 50 + g.rng.below(200),
        y1 + 50 + g.rng.below(200)
    )));
    stmts.push(StmtKind::Raw(format!(
        "SELECT pk FROM {} ORDER BY pt <-> point({}, {}), pk LIMIT {};",
        name,
        g.rng.below(701),
        g.rng.below(733),
        5 + g.rng.below(15)
    )));
    stmts.push(StmtKind::Raw("RESET enable_seqscan;".to_string()));
    stmts.push(StmtKind::Raw(format!("DROP TABLE {};", name)));
    stmts
}

/// Self-contained EXCLUDE USING gist group: non-overlapping load, one
/// deterministically CONFLICTING insert (matched 23P01 — the constraint
/// recheck path), an ON CONFLICT ON CONSTRAINT DO NOTHING arm, and a
/// weighted second (-|-) exclusion constraint over the same rows.
fn gist_excl(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("idx:gist:excl");
    let name = format!("fz_gex_{}", g.idx.next_gist);
    g.idx.next_gist += 1;
    let n = 200 + g.rng.below(201);
    let c1 = 1 + g.rng.below(n - 1);
    let c2 = 1 + g.rng.below(n - 1);
    let mut stmts = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {} (pk int4 PRIMARY KEY, rng int4range, \
             EXCLUDE USING gist (rng WITH &&)) WITH (autovacuum_enabled = off);",
            name
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {} SELECT i, int4range(i * 10, i * 10 + 8) \
             FROM generate_series(1, {}) i;",
            name, n
        )),
        // Always overlaps row c1: matched serialization of the recheck error.
        StmtKind::Raw(format!(
            "INSERT INTO {} SELECT 9001, int4range({}, {}) FROM generate_series(1, 1) i;",
            name,
            c1 * 10 + 5,
            c1 * 10 + 7
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {} SELECT 9002, int4range({}, {}) FROM generate_series(1, 1) i \
             ON CONFLICT ON CONSTRAINT {}_rng_excl DO NOTHING;",
            name,
            c2 * 10 + 5,
            c2 * 10 + 7,
            name
        )),
        StmtKind::Raw(format!("SELECT count(*) FROM {};", name)),
    ];
    if g.rng.chance(1, 3) {
        g.fire("idx:gist:excl:adj");
        stmts.push(StmtKind::Raw(format!(
            "ALTER TABLE {} ADD CONSTRAINT {}_adj EXCLUDE USING gist (rng WITH -|-);",
            name, name
        )));
    }
    stmts.push(StmtKind::Raw(format!("DROP TABLE {};", name)));
    stmts
}

fn gist_drop(g: &mut Gen) -> Vec<StmtKind> {
    let Some(ti) = pick_live_gist(g) else {
        g.fire("idx:gist:fallback:create");
        return gist_create(g);
    };
    g.fire("idx:gist:drop");
    let name = g.idx.gist[ti].name.clone();
    g.idx.gist[ti].live = false;
    g.idx.events.push(DdlEvent {
        table: name.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![StmtKind::Raw(format!("DROP TABLE {};", name))]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Session-shaped harness: persistent IdxState across groups.
    fn gen_many(seed: u64, n: usize, spec: &str) -> (Vec<Vec<String>>, Vec<String>, IdxState) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut idx = IdxState::new();
        let mut groups = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            std::mem::swap(&mut g.idx, &mut idx);
            let kinds = gen_idx_module(&mut g);
            std::mem::swap(&mut g.idx, &mut idx);
            assert!(!kinds.is_empty());
            groups.push(kinds.iter().map(|k| k.to_sql()).collect());
            prods.extend(p);
        }
        (groups, prods, idx)
    }

    fn flat(groups: &[Vec<String>]) -> Vec<String> {
        groups.iter().flatten().cloned().collect()
    }

    #[test]
    fn idx_is_deterministic() {
        let (a, _, _) = gen_many(51, 300, "");
        let (b, _, _) = gen_many(51, 300, "");
        assert_eq!(a, b);
        let (c, _, _) = gen_many(52, 300, "");
        assert_ne!(a, c);
    }

    /// Round-18a (seed 4001601682479221144), same rule as exd RB-15 / par
    /// round-14: every idx CREATE TABLE pins autovacuum_enabled = off.
    /// Bulk loads (up to 9000 rows) cross the insert-autovacuum threshold
    /// on their own, massdel churn crosses the autoanalyze threshold, and
    /// an autovacuum landing on exactly ONE engine flips a later compared
    /// EXPLAIN (COSTS OFF) plan — verified flap on fz_btd_: 5-row Bitmap
    /// Heap Scan vs 3-row Index Only Scan across the VACUUM-state ladder.
    /// Blanket across all six fixture families (the B2 injected-stats
    /// probes additionally rely on autoanalyze never overwriting the
    /// injected stats).
    #[test]
    fn creates_pin_autovacuum_off() {
        let (groups, _, _) = gen_many(0x18A, 1500, "");
        let mut seen = 0;
        for sql in flat(&groups) {
            if sql.starts_with("CREATE TABLE ") || sql.starts_with("CREATE UNLOGGED TABLE ") {
                assert!(
                    sql.contains("autovacuum_enabled = off"),
                    "idx fixture does not pin autovacuum off: `{sql}`"
                );
                seen += 1;
            }
        }
        assert!(seen > 0, "no CREATE TABLE generated in 1500 groups");
    }

    #[test]
    fn idx_variety() {
        let (groups, prods, _) = gen_many(0x1DA, 1200, "");
        let all = flat(&groups).join("\n");
        for frag in [
            "CREATE TABLE fz_iam_",
            "pt point, bx box, rng int4range, addr inet",
            "generate_series(1, ",
            "USING spgist (pt)",
            "USING spgist (pt kd_point_ops)",
            "USING spgist (txt)",
            "USING spgist (rng)",
            "USING spgist (addr)",
            "USING gist (bx)",
            "USING gist (pt)",
            "USING gist (rng)",
            "USING gist (tsv)",
            "USING gin (doc)",
            "USING gin (doc jsonb_path_ops)",
            "USING gin (arr)",
            "USING gin (tsv)",
            "USING brin (k_int)",
            "USING brin (ts)",
            "USING brin (addr inet_inclusion_ops)",
            "USING hash (txt)",
            "USING hash (k_int)",
            "(k_int, txt);",
            "WHERE k_int > 250;",
            "(((k_int % 7)));",
            "ANALYZE fz_iam_",
            "SET enable_seqscan TO off;",
            "RESET enable_seqscan;",
            "SET enable_bitmapscan TO off;",
            "RESET enable_bitmapscan;",
            " <@ box(point(",
            " && box(point(",
            "rng @> ",
            "int4range(",
            "addr <<= inet '10.",
            "to_tsquery('english', ",
            "plainto_tsquery('english', ",
            "doc @> '{\"a\": ",
            "doc ? '",
            "doc ?| ARRAY['a', 'zz']",
            "arr && ARRAY[",
            "arr @> ARRAY[",
            "k_int BETWEEN ",
            "ts BETWEEN timestamp '2020-01-01 00:00:00' + ",
            "txt = 'p",
            "ORDER BY pk;",
            "SELECT count(*) FROM fz_iam_",
            " ORDER BY pt <-> point(",
            "EXPLAIN (COSTS OFF) SELECT",
            "UPDATE fz_iam_",
            "DELETE FROM fz_iam_",
            "REINDEX TABLE fz_iam_",
            "REINDEX INDEX fz_iami_",
            "VACUUM fz_iam_",
            "DROP TABLE fz_iam_",
        ] {
            assert!(all.contains(frag), "idx flavor {frag:?} never generated");
        }
        for p in [
            "idx:create",
            "idx:drop",
            "idx:query",
            "idx:explain",
            "idx:knn",
            "idx:churn",
            "idx:core:spgpt",
            "idx:core:spgtxt",
            "idx:core:gistbx",
            "idx:core:gindoc",
            "idx:core:brinint",
            "idx:x:kdpt",
            "idx:x:brininet",
            "idx:fam:point",
            "idx:fam:box",
            "idx:fam:range",
            "idx:fam:inet",
            "idx:fam:tsv",
            "idx:fam:jsonb",
            "idx:fam:array",
            "idx:fam:brin",
            "idx:fam:hash",
            "idx:q:rows",
            "idx:q:count",
            "idx:bitmap:off",
            "idx:churn:update",
            "idx:churn:delete",
            "idx:churn:insert",
            "idx:churn:reindex",
            "idx:churn:vacuum",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        for sql in flat(&groups) {
            assert!(!sql.contains('\n') && sql.ends_with(';'), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
        }
    }

    /// B2 families: every production and statement shape appears, and the
    /// generated SQL carries the structural fuel each family exists for
    /// (mass contiguous deletes, all-the-same / collinear / oversize
    /// batches, restore/clear/probe stats calls).
    #[test]
    fn b2_variety() {
        let (groups, prods, _) = gen_many(
            0xB2,
            1600,
            "idx:bt=8,idx:adv=8,idx:stats=8,idx:bt:drop=3,idx:adv:drop=2",
        );
        let all = flat(&groups).join("\n");
        for frag in [
            // bt family
            "CREATE TABLE fz_btd_",
            "pk int4 PRIMARY KEY, k_int int4",
            "pk int4 PRIMARY KEY, t text NOT NULL",
            "CREATE INDEX fz_btdi_",
            "'k' || lpad(i::text, 95, '0')",
            "generate_series(1, 3000)",
            "generate_series(1, 6000)",
            "generate_series(1, 9000)",
            " WHERE pk BETWEEN ",
            "VACUUM fz_btd_",
            "ANALYZE fz_btd_",
            "DROP TABLE fz_btd_",
            "'k' || lpad('",
            "SELECT count(*) FROM fz_btd_",
            // adv family
            "CREATE TABLE fz_adv_",
            "pk int4 PRIMARY KEY, pt point, txt text, poly polygon",
            "CREATE INDEX fz_advi_",
            "USING spgist (pt)",
            "USING spgist (pt kd_point_ops)",
            "USING spgist (txt)",
            "USING gist (pt)",
            "USING gist (poly)",
            "_key_constant_key_constant'",
            "'line_' || (i % 11)",
            "repeat('longprefix_', 40) || repeat('y', (i * 37) % 2000)",
            "txt < 'longprefix_a'",
            "poly && polygon '((",
            "ORDER BY pt <-> point(",
            "DELETE FROM fz_adv_",
            "VACUUM fz_adv_",
            "REINDEX TABLE fz_adv_",
            "DROP TABLE fz_adv_",
            // stats family
            "pg_restore_relation_stats('schemaname', 'public', 'relname', 'fz_btd_",
            "pg_restore_attribute_stats('schemaname', 'public', 'relname', 'fz_btd_",
            "'relallfrozen', ",
            "'most_common_vals', '{",
            "'most_common_freqs', '{0.01,0.005,0.0025}'::real[]",
            "'histogram_bounds', '{",
            "'correlation', ",
            "pg_clear_relation_stats('public', 'fz_btd_",
            "pg_clear_attribute_stats('public', 'fz_btd_",
            "EXPLAIN (COSTS OFF) SELECT * FROM fz_btd_",
            "'most_common_freqs', '{0.1}'::real[]",
            "'{bad,,\"lit'::text",
            "'null_frac', 7.5::real",
            "'relpages', -7::integer",
        ] {
            assert!(all.contains(frag), "b2 flavor {frag:?} never generated");
        }
        for p in [
            "idx:bt",
            "idx:bt:create",
            "idx:bt:massdel",
            "idx:bt:vacuum",
            "idx:bt:recycle",
            "idx:bt:query",
            "idx:bt:drop",
            "idx:bt:shape:int",
            "idx:bt:shape:text",
            "idx:bt:q:pkrange",
            "idx:bt:q:pkcount",
            "idx:bt:q:sec",
            "idx:bt:q:seccount",
            "idx:adv",
            "idx:adv:create",
            "idx:adv:ins:same",
            "idx:adv:ins:line",
            "idx:adv:ins:big",
            "idx:adv:query",
            "idx:adv:q:box",
            "idx:adv:q:left",
            "idx:adv:q:txteq",
            "idx:adv:q:txtlt",
            "idx:adv:q:poly",
            "idx:adv:q:knn",
            "idx:adv:q:explain",
            "idx:adv:churn:delete",
            "idx:adv:churn:vacuum",
            "idx:adv:churn:reindex",
            "idx:adv:drop",
            "idx:stats",
            "idx:stats:rel",
            "idx:stats:attr",
            "idx:stats:attr:full",
            "idx:stats:attr:min",
            "idx:stats:attr:hist",
            "idx:stats:clear",
            "idx:stats:probe",
            "idx:stats:err",
            "idx:stats:err:mismatch",
            "idx:stats:err:badlit",
            "idx:stats:err:range",
            "idx:stats:err:negpages",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        for sql in flat(&groups) {
            assert!(!sql.contains('\n') && sql.ends_with(';'), "{sql}");
        }
    }

    /// Q6 gist-paths family: every production and statement shape appears,
    /// and the SQL carries the structural fuel each surface exists for.
    #[test]
    fn gist_variety() {
        let (groups, prods, _) = gen_many(
            0x6157,
            1600,
            "idx:gist=10,idx:gist:drop=2,idx:gist:unlogged=2,idx:gq:gtsv=1",
        );
        let all = flat(&groups).join("\n");
        for frag in [
            "CREATE TABLE fz_gst_",
            "CREATE UNLOGGED TABLE fz_gst_",
            "rn numrange, rt tsrange, rz tstzrange, rd daterange, mr int4multirange",
            "CREATE INDEX fz_gsti_",
            "USING gist (r8)",
            "USING gist (rn)",
            "USING gist (mr)",
            "USING gist (tsq)",
            "USING gist (addr inet_ops)",
            "USING gist (tsv tsvector_ops (siglen = 100))",
            "USING gist (pt, bx)",
            "USING spgist (bx)",
            "USING gist (rt)",
            "USING gist (rz)",
            "USING gist (rd)",
            "USING gist (bx) WITH (fillfactor = 40)",
            "ANALYZE fz_gst_",
            "r8 && int8range(",
            "r8 -|- int8range(",
            ".5::numeric",
            "rt && tsrange(timestamp '2020-01-01' + ",
            "rz @> (timestamptz '2020-01-01 00:00:00+00' + ",
            "rd <@ daterange(date '2020-01-01' + ",
            "mr && int4range(",
            "'::tsquery",
            "addr <<= inet '10.",
            "addr && inet '10.",
            "to_tsquery('english', 'w",
            "bx << box(point(",
            "bx |>> box(point(",
            "bx &< box(point(",
            "bx <<| box(point(",
            "ORDER BY bx <-> point(",
            "SELECT addr FROM fz_gst_",
            "ORDER BY addr LIMIT ",
            "pg_index_column_has_property('fz_gsti_",
            "'1'::gtsvector",
            "gtsvectorin('1')",
            "DELETE FROM fz_gst_",
            "VACUUM fz_gst_",
            "CREATE TABLE fz_gbuf_",
            "WITH (buffering = on)",
            "WITH (buffering = off)",
            "DROP TABLE fz_gbuf_",
            "CREATE TABLE fz_gex_",
            "EXCLUDE USING gist (rng WITH &&)",
            "ON CONFLICT ON CONSTRAINT fz_gex_",
            "_rng_excl DO NOTHING",
            "EXCLUDE USING gist (rng WITH -|-)",
            "DROP TABLE fz_gex_",
            "DROP TABLE fz_gst_",
        ] {
            assert!(all.contains(frag), "gist flavor {frag:?} never generated");
        }
        for p in [
            "idx:gist",
            "idx:gist:create",
            "idx:gist:unlogged",
            "idx:gist:query",
            "idx:gist:ios",
            "idx:gist:prop",
            "idx:gist:delvac",
            "idx:gist:reinsert",
            "idx:gist:buffered",
            "idx:gist:excl",
            "idx:gist:drop",
            "idx:gist:x:r8",
            "idx:gist:x:num",
            "idx:gist:x:mr",
            "idx:gist:x:tsq",
            "idx:gist:x:inet",
            "idx:gist:x:tsv",
            "idx:gist:x:mc",
            "idx:gist:x:spgbx",
            "idx:gist:x:ts",
            "idx:gist:x:tstz",
            "idx:gist:x:date",
            "idx:gist:x:ff",
            "idx:gq:r8",
            "idx:gq:num",
            "idx:gq:ts",
            "idx:gq:mr",
            "idx:gq:tsq",
            "idx:gq:inet",
            "idx:gq:tsv",
            "idx:gq:boxdir",
            "idx:gq:knnbox",
            "idx:gq:gtsv",
            "idx:gq:rows",
            "idx:gq:count",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        for sql in flat(&groups) {
            assert!(!sql.contains('\n') && sql.ends_with(';'), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
        }
    }

    /// Zero-42xxx discipline, statically: no statement references a dead or
    /// never-created relation/index, GUC brackets are balanced inside one
    /// group (every SET has its RESET later in the same group), REINDEX
    /// targets live objects, and insert batches never overlap in pk space.
    #[test]
    fn model_replay_holds() {
        let (groups, _, _) = gen_many(0xF1DE, 2000, "");
        use std::collections::HashMap;
        let mut live: HashMap<String, bool> = HashMap::new();
        // index -> table
        let mut idx_table: HashMap<String, String> = HashMap::new();
        // table -> max pk bound inserted (from generate_series hi)
        let mut max_pk: HashMap<String, i64> = HashMap::new();
        // bt table -> deleted-interval model (mirrors BtTable::deleted)
        let mut bt_deleted: HashMap<String, Vec<(i64, i64)>> = HashMap::new();
        for group in &groups {
            // Bracket balance within the group.
            for guc in ["enable_seqscan", "enable_bitmapscan"] {
                let sets = group.iter().filter(|s| *s == &format!("SET {guc} TO off;")).count();
                let resets = group.iter().filter(|s| *s == &format!("RESET {guc};")).count();
                assert_eq!(sets, resets, "unbalanced {guc} bracket: {group:?}");
                if sets == 1 {
                    let si = group.iter().position(|s| s.starts_with(&format!("SET {guc}"))).unwrap();
                    let ri = group.iter().position(|s| s.starts_with(&format!("RESET {guc}"))).unwrap();
                    assert!(si < ri, "RESET before SET: {group:?}");
                }
            }
            for sql in group {
                if let Some(rest) = sql
                    .strip_prefix("CREATE TABLE ")
                    .or_else(|| sql.strip_prefix("CREATE UNLOGGED TABLE "))
                {
                    let name = rest.split(' ').next().unwrap().to_string();
                    assert!(!live.contains_key(&name), "table name reused: {sql}");
                    live.insert(name, true);
                } else if let Some(rest) = sql.strip_prefix("DROP TABLE ") {
                    let name = rest.trim_end_matches(';').to_string();
                    assert_eq!(live.get(&name), Some(&true), "drop of dead table: {sql}");
                    live.insert(name, false);
                } else if let Some(rest) = sql.strip_prefix("CREATE INDEX ") {
                    let iname = rest.split(' ').next().unwrap().to_string();
                    let tname = sql.split(" ON ").nth(1).unwrap().split([' ', '(']).next().unwrap();
                    assert_eq!(live.get(tname), Some(&true), "index on dead table: {sql}");
                    assert!(!idx_table.contains_key(&iname), "index name reused: {sql}");
                    idx_table.insert(iname, tname.to_string());
                } else if let Some(rest) = sql.strip_prefix("REINDEX INDEX ") {
                    let iname = rest.trim_end_matches(';');
                    let t = idx_table.get(iname).expect("reindex of unknown index");
                    assert_eq!(live.get(t), Some(&true), "reindex on dead table: {sql}");
                } else if let Some(rest) = sql
                    .strip_prefix("REINDEX TABLE ")
                    .or_else(|| sql.strip_prefix("VACUUM "))
                    .or_else(|| sql.strip_prefix("ANALYZE "))
                {
                    let name = rest.trim_end_matches(';');
                    assert_eq!(live.get(name), Some(&true), "utility on dead table: {sql}");
                } else {
                    // Any other statement (query, DML, stats call, EXPLAIN)
                    // referencing a module table must reference a live one.
                    // Index names (fz_iami_/fz_btdi_/fz_advi_) don't match
                    // these prefixes: the digit run starts right after the
                    // family prefix's own underscore.
                    for prefix in ["fz_iam_", "fz_btd_", "fz_adv_", "fz_gst_", "fz_gbuf_", "fz_gex_"] {
                        for part in sql.split(prefix).skip(1) {
                            let digits: String =
                                part.chars().take_while(|c| c.is_ascii_digit()).collect();
                            if digits.is_empty() {
                                continue; // fz_btdi_/fz_advi_/fz_iami_ index names
                            }
                            let name = format!("{prefix}{digits}");
                            assert_eq!(
                                live.get(&name),
                                Some(&true),
                                "reference to dead table: {sql}"
                            );
                        }
                    }
                }
                // Mass deletes feed the bt/gist deleted-interval model.
                if let Some((pfx, rest)) = sql
                    .strip_prefix("DELETE FROM fz_btd_")
                    .map(|r| ("fz_btd_", r))
                    .or_else(|| sql.strip_prefix("DELETE FROM fz_gst_").map(|r| ("fz_gst_", r)))
                {
                    if sql.contains(" WHERE pk BETWEEN ") {
                        let tname = format!(
                            "{pfx}{}",
                            rest.chars().take_while(|c| c.is_ascii_digit()).collect::<String>()
                        );
                        let tail = sql.split(" WHERE pk BETWEEN ").nth(1).unwrap();
                        let lo: i64 = tail.split(' ').next().unwrap().parse().unwrap();
                        let hi: i64 =
                            tail.split(" AND ").nth(1).unwrap().trim_end_matches(';').parse().unwrap();
                        assert!(1 <= lo && lo <= hi, "{sql}");
                        assert!(hi <= *max_pk.get(&tname).unwrap_or(&0), "delete past load: {sql}");
                        interval_union(bt_deleted.entry(tname).or_default(), lo, hi);
                    }
                }
                // Insert batches: strictly increasing pk ranges per table,
                // EXCEPT bt recycle bursts, which must re-insert exactly one
                // previously-deleted interval (conflict-free by that fact).
                if let Some(rest) = sql.strip_prefix("INSERT INTO ") {
                    let tname = rest.split(' ').next().unwrap().to_string();
                    let series = sql.split("generate_series(").nth(1).unwrap();
                    let lo: i64 = series.split(',').next().unwrap().trim().parse().unwrap();
                    let hi: i64 = series
                        .split(", ")
                        .nth(1)
                        .unwrap()
                        .split(')')
                        .next()
                        .unwrap()
                        .trim()
                        .parse()
                        .unwrap();
                    assert!(lo <= hi, "{sql}");
                    let prev = *max_pk.get(&tname).unwrap_or(&0);
                    if lo > prev {
                        max_pk.insert(tname, hi);
                    } else if tname.starts_with("fz_gex_") {
                        // Self-contained exclusion fuel: the conflicting
                        // inserts deliberately reuse pk space (constant pk
                        // 9001/9002 rows; one errors, one is DO NOTHING).
                    } else {
                        assert!(
                            tname.starts_with("fz_btd_") || tname.starts_with("fz_gst_"),
                            "pk batch overlap (prev max {prev}): {sql}"
                        );
                        let set = bt_deleted.entry(tname).or_default();
                        let pos = set
                            .iter()
                            .position(|&(a, b)| a == lo && b == hi)
                            .unwrap_or_else(|| panic!("recycle of a non-deleted interval: {sql}"));
                        set.remove(pos);
                    }
                }
            }
        }
    }

    #[test]
    fn events_pair_up_and_population_capped() {
        let (groups, _, state) = gen_many(0xCAB, 800, "");
        assert!(state.live_tables().len() <= MAX_LIVE_TABLES);
        assert!(state.live_bt().len() <= MAX_BT_TABLES);
        assert!(state.live_adv().len() <= MAX_ADV_TABLES);
        assert!(state.live_gist().len() <= MAX_GIST_TABLES);
        // Every event table corresponds to a CREATE/DROP statement; each
        // family respects its own live-population cap.
        let all = flat(&groups);
        let mut open: Vec<String> = Vec::new();
        let fam_of = |name: &str| {
            for p in ["fz_btd_", "fz_adv_", "fz_iam_", "fz_gst_", "fz_gbuf_", "fz_gex_"] {
                if name.starts_with(p) {
                    return p;
                }
            }
            panic!("unexpected table name {name}");
        };
        let cap_of = |fam: &str| match fam {
            "fz_btd_" => MAX_BT_TABLES,
            "fz_adv_" => MAX_ADV_TABLES,
            // fz_gbuf_/fz_gex_ are self-contained (created and dropped in
            // one group), so at most one is ever open.
            "fz_gst_" | "fz_gbuf_" | "fz_gex_" => MAX_GIST_TABLES,
            _ => MAX_LIVE_TABLES,
        };
        for sql in &all {
            if let Some(rest) = sql
                .strip_prefix("CREATE TABLE ")
                .or_else(|| sql.strip_prefix("CREATE UNLOGGED TABLE "))
            {
                let name = rest.split([' ', '(']).next().unwrap().to_string();
                let fam = fam_of(&name);
                open.push(name.clone());
                assert!(
                    open.iter().filter(|t| fam_of(t) == fam).count() <= cap_of(fam),
                    "cap breached: {sql}"
                );
            } else if let Some(rest) = sql.strip_prefix("DROP TABLE ") {
                let name = rest.trim_end_matches(';');
                open.retain(|t| t != name);
            }
        }
        // Core indexes present on every create group.
        for g in &groups {
            if g[0].starts_with("CREATE TABLE fz_iam_") {
                let joined = g.join("\n");
                for core in [
                    "USING spgist (pt)",
                    "USING spgist (txt)",
                    "USING gist (bx)",
                    "USING gin (doc)",
                    "USING brin (k_int)",
                ] {
                    assert!(joined.contains(core), "create group missing core index {core}");
                }
                assert!(joined.contains("ANALYZE "), "create group missing ANALYZE");
            }
        }
    }
}
