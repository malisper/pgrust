//! Executor-residue drain module (LD9): the `executor-residue` chunk of
//! docs/fuzzing/line-drain-queue.tsv (341 rows / ~3,074 hollow lines —
//! the non-spill executor node arms LD5/LD2 left dark): nodeAppend/
//! nodeMergeAppend exec-time partition pruning, nodeSubplan hashed/scan
//! arms, nodeWindowAgg frame-option matrix (RANGE/GROUPS offsets + all
//! EXCLUDE variants + moving-aggregate inverse transitions), nodeSetOp,
//! nodeLockRows, nodeModifyTable (RETURNING old/new, ON CONFLICT,
//! cross-partition moves, MERGE matched/not-matched-by-source arms,
//! generated columns incl. PG18 VIRTUAL), nodeTidscan/nodeTidrangescan,
//! nodeTableFuncscan via JSON_TABLE (the rig builds are --without-libxml,
//! so XMLTABLE is a shared error surface, not a drain target),
//! nodeNamedtuplestore via transition tables, execCurrent WHERE CURRENT
//! OF, functions.c SQL-language function arms, nodeLimit WITH TIES /
//! backward arms, nodeIndexscan row-compare/SAOP key arms, ExecReScan
//! variety via LATERAL nestloop rescans, and nodeMergejoin fill arms.
//!
//! EvalPlanQual re-check arms are NOT drained here: EPQ engages only when
//! a concurrently-committed update invalidates a fetched tuple, which a
//! single-session stream cannot produce. Ruled FAULT-ONLY/CONCURRENCY —
//! see docs/fuzzing/findings-ld9.md (Antithesis inventory note). The same
//! applies to choose_next_subplan_for_worker (parallel-worker append
//! choice under concurrent workers; the par module's parallel legs may
//! graze it but worker scheduling decides).
//!
//! Determinism laws (LD5/spill discipline):
//!   - every row-returning statement carries a TOTAL order (ORDER BY
//!     ending in a unique key) OR projects only peer-invariant values
//!     (frame aggregates under RANGE/GROUPS: a peer group enters or
//!     leaves a frame as a unit, and EXCLUDE CURRENT ROW/TIES only pivot
//!     on the current row's OWN value, so SUM/COUNT results are
//!     independent of intra-peer order). Positional window functions
//!     (first/last/nth_value, lead/lag) only ever ride a total ORDER BY.
//!   - no float surfaces; numeric/int aggregates only (B1).
//!   - every mutating family runs inside BEGIN..ROLLBACK, so fixture
//!     data is bit-stable across groups on both sides (identity/sequence
//!     advances survive rollback but advance identically on both sides).
//!   - fixture tables stay well under the 30k default_statistics_target
//!     sample: ANALYZE is exhaustive, stats identical, plans identical.
//!   - every bracket SET has its RESET in the same group (GucPinned
//!     re-pins after RESETs identically on both sides).
//!   - ctid projections are never compared directly: TidScan probes
//!     resolve ctids via a self-subquery keyed on pk and project row
//!     values; TidRangeScan probes aggregate over page-prefix ranges
//!     (page layout is C-parity by construction; hand-verified before
//!     mass legs — see findings-ld9.md).
//!
//! Fixture suite (one live at a time; created/dropped as a unit):
//!   fz_xr{N}   bulk heap: pk PK, a int (0..250, ~0.1% NULL), b int
//!              (0..37), c numeric(2dp), t text; idx (a), (b,a); 6000 rows.
//!   fz_xrd{N}  dim: dk PK 1..240, da int (0..25), dt text.
//!   fz_xrp{N}  RANGE(pk) parent: p0 [0,1500), p1 [1500,3000) ATTACHed
//!              from a standalone table carrying a dropped column (the
//!              attrmap-conversion arm), p2 [3000,4500) sub-partitioned
//!              HASH(a) x2, pd DEFAULT; 5200 rows; idx (pk), (a).
//!   fz_xrc{N}  conflict target: k PK, u UNIQUE, v, w; 800 rows.
//!   fz_xrg{N}  generated: id IDENTITY PK, x, gs STORED, gv VIRTUAL.
//!   fz_xrv{N}  CHECK OPTION view over fz_xr (b < 30).
//!   fz_xrt{N}+fz_xrl{N}+fz_xrtf{N}() transition-table AFTER STATEMENT
//!              triggers logging aggregates into fz_xrl.

use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

const MAX_LIVE_SUITES: usize = 1;

/// One fixture suite (all tables share the numeric suffix).
#[derive(Clone, Debug)]
pub struct ExrSuite {
    pub n: u32,
    pub live: bool,
}

/// Session-persistent executor-residue fixture model (swapped in and out
/// of `Gen` by the session loop exactly like `SpillState`).
#[derive(Clone, Debug, Default)]
pub struct ExrState {
    pub suites: Vec<ExrSuite>,
    next_suite: u32,
    next_obj: u32,
    events: Vec<DdlEvent>,
}

impl ExrState {
    pub fn new() -> ExrState {
        ExrState::default()
    }

    pub fn take_events(&mut self) -> Vec<DdlEvent> {
        std::mem::take(&mut self.events)
    }

    fn live_suites(&self) -> Vec<usize> {
        self.suites
            .iter()
            .enumerate()
            .filter(|(_, s)| s.live)
            .map(|(i, _)| i)
            .collect()
    }
}

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

/// Wrap `body` in SET/RESET pairs (RESETs reversed) in ONE group.
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

/// Wrap `body` in BEGIN..ROLLBACK (mutating families; fixtures stay
/// bit-stable).
fn rollback(body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut stmts = vec![StmtKind::Raw("BEGIN;".to_string())];
    stmts.extend(body);
    stmts.push(StmtKind::Raw("ROLLBACK;".to_string()));
    stmts
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

/// Pick a live suite or synthesize the create group.
macro_rules! need_suite {
    ($g:expr) => {{
        let live = $g.exr.live_suites();
        if live.is_empty() {
            $g.fire("exr:fallback:create");
            return gen_create($g);
        }
        live[$g.rng.below_usize(live.len())]
    }};
}

const SHAPES: &[&str] = &[
    "exr:create",
    "exr:drop",
    "exr:prune",
    "exr:winframe",
    "exr:subplan",
    "exr:setop",
    "exr:lockrows",
    "exr:dml",
    "exr:merge",
    "exr:tid",
    "exr:tfunc",
    "exr:ntstore",
    "exr:currentof",
    "exr:sqlfn",
    "exr:limit",
    "exr:iscan",
    "exr:rescan",
    "exr:mj",
    "exr:parappend",
    "exr:excl",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_exr_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("exr");
    match g.weights.pick(g.rng, SHAPES) {
        "exr:create" => gen_create(g),
        "exr:drop" => gen_drop(g),
        "exr:prune" => gen_prune(g),
        "exr:winframe" => gen_winframe(g),
        "exr:subplan" => gen_subplan(g),
        "exr:setop" => gen_setop(g),
        "exr:lockrows" => gen_lockrows(g),
        "exr:dml" => gen_dml(g),
        "exr:merge" => gen_merge(g),
        "exr:tid" => gen_tid(g),
        "exr:tfunc" => gen_tfunc(g),
        "exr:ntstore" => gen_ntstore(g),
        "exr:currentof" => gen_currentof(g),
        "exr:sqlfn" => gen_sqlfn(g),
        "exr:limit" => gen_limit(g),
        "exr:iscan" => gen_iscan(g),
        "exr:rescan" => gen_rescan(g),
        "exr:parappend" => gen_parappend(g),
        "exr:excl" => gen_excl(g),
        _ => gen_mj(g),
    }
}

// -------------------------------------------------------------- create ----

fn gen_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.exr.live_suites().len() >= MAX_LIVE_SUITES {
        g.fire("exr:cap:suites");
        return gen_drop(g);
    }
    g.fire("exr:create");
    let n = g.exr.next_suite;
    g.exr.next_suite += 1;
    let xr = format!("fz_xr{n}");
    let xrd = format!("fz_xrd{n}");
    let xrp = format!("fz_xrp{n}");
    let xrc = format!("fz_xrc{n}");
    let xrg = format!("fz_xrg{n}");
    let xrv = format!("fz_xrv{n}");
    let xrt = format!("fz_xrt{n}");
    let xrl = format!("fz_xrl{n}");
    let tf = format!("fz_xrtf{n}");
    let stmts = vec![
        // bulk heap
        raw(format!(
            "CREATE TABLE {xr} (pk int4 PRIMARY KEY, a int4, b int4, c numeric, t text);"
        )),
        raw(format!(
            "INSERT INTO {xr} SELECT i, CASE WHEN i % 997 = 0 THEN NULL ELSE (i * 13) % 250 END, \
             (i * 7) % 37, (((i * 11) % 5000)::numeric) / 100, 'r' || ((i * 3) % 101) \
             FROM generate_series(1, 6000) i;"
        )),
        raw(format!("CREATE INDEX fz_xria{n} ON {xr} (a);")),
        raw(format!("CREATE INDEX fz_xriba{n} ON {xr} (b, a);")),
        // dim
        raw(format!(
            "CREATE TABLE {xrd} (dk int4 PRIMARY KEY, da int4, dt text);"
        )),
        raw(format!(
            "INSERT INTO {xrd} SELECT i, (i * 5) % 25, 'd' || (i % 13) FROM generate_series(1, 240) i;"
        )),
        // partitioned parent (RANGE + attach-with-dropped-col + HASH sub + DEFAULT)
        raw(format!(
            "CREATE TABLE {xrp} (pk int4 NOT NULL, a int4, t text) PARTITION BY RANGE (pk);"
        )),
        raw(format!(
            "CREATE TABLE {xrp}_p0 PARTITION OF {xrp} FOR VALUES FROM (0) TO (1500);"
        )),
        raw(format!(
            "CREATE TABLE {xrp}_p1 (pk int4 NOT NULL, junk int4, a int4, t text);"
        )),
        raw(format!("ALTER TABLE {xrp}_p1 DROP COLUMN junk;")),
        raw(format!(
            "ALTER TABLE {xrp} ATTACH PARTITION {xrp}_p1 FOR VALUES FROM (1500) TO (3000);"
        )),
        raw(format!(
            "CREATE TABLE {xrp}_p2 PARTITION OF {xrp} FOR VALUES FROM (3000) TO (4500) PARTITION BY HASH (a);"
        )),
        raw(format!(
            "CREATE TABLE {xrp}_p2a PARTITION OF {xrp}_p2 FOR VALUES WITH (MODULUS 2, REMAINDER 0);"
        )),
        raw(format!(
            "CREATE TABLE {xrp}_p2b PARTITION OF {xrp}_p2 FOR VALUES WITH (MODULUS 2, REMAINDER 1);"
        )),
        raw(format!("CREATE TABLE {xrp}_pd PARTITION OF {xrp} DEFAULT;")),
        raw(format!(
            "INSERT INTO {xrp} SELECT i, (i * 13) % 64, 'p' || (i % 23) FROM generate_series(0, 5199) i;"
        )),
        raw(format!("CREATE INDEX fz_xrpi{n} ON {xrp} (pk);")),
        raw(format!("CREATE INDEX fz_xrpia{n} ON {xrp} (a);")),
        // conflict target
        raw(format!(
            "CREATE TABLE {xrc} (k int4 PRIMARY KEY, u int4 UNIQUE, v int4, w int4);"
        )),
        raw(format!(
            "INSERT INTO {xrc} SELECT i, i * 2, i % 50, i % 7 FROM generate_series(1, 800) i;"
        )),
        // generated columns (STORED + PG18 VIRTUAL) over identity pk
        raw(format!(
            "CREATE TABLE {xrg} (id int4 GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, \
             x int4, gs int4 GENERATED ALWAYS AS (x * 3) STORED, \
             gv int4 GENERATED ALWAYS AS (x + 7) VIRTUAL);"
        )),
        raw(format!(
            "INSERT INTO {xrg} (x) SELECT i * 2 FROM generate_series(1, 150) i;"
        )),
        // WITH CHECK OPTION view
        raw(format!(
            "CREATE VIEW {xrv} AS SELECT pk, a, b FROM {xr} WHERE b < 30 WITH CASCADED CHECK OPTION;"
        )),
        // transition-table triggers
        raw(format!(
            "CREATE TABLE {xrt} (k int4 PRIMARY KEY, v int4);"
        )),
        raw(format!(
            "INSERT INTO {xrt} SELECT i, i % 40 FROM generate_series(1, 400) i;"
        )),
        raw(format!(
            "CREATE TABLE {xrl} (id int4 GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, \
             tag text, n int8, s int8);"
        )),
        raw(format!(
            "CREATE FUNCTION {tf}() RETURNS trigger LANGUAGE plpgsql AS $fz$ \
             BEGIN \
               IF TG_OP = 'INSERT' THEN \
                 INSERT INTO {xrl} (tag, n, s) SELECT 'ins', count(*), coalesce(sum(v), 0) FROM newtab; \
               ELSIF TG_OP = 'UPDATE' THEN \
                 INSERT INTO {xrl} (tag, n, s) \
                   SELECT 'upd', (SELECT count(*) FROM oldtab), coalesce((SELECT sum(v) FROM newtab), 0); \
               ELSE \
                 INSERT INTO {xrl} (tag, n, s) SELECT 'del', count(*), coalesce(sum(v), 0) FROM oldtab; \
               END IF; \
               RETURN NULL; \
             END $fz$;"
        )),
        raw(format!(
            "CREATE TRIGGER fz_xrti{n} AFTER INSERT ON {xrt} \
             REFERENCING NEW TABLE AS newtab FOR EACH STATEMENT EXECUTE FUNCTION {tf}();"
        )),
        raw(format!(
            "CREATE TRIGGER fz_xrtu{n} AFTER UPDATE ON {xrt} \
             REFERENCING OLD TABLE AS oldtab NEW TABLE AS newtab FOR EACH STATEMENT EXECUTE FUNCTION {tf}();"
        )),
        raw(format!(
            "CREATE TRIGGER fz_xrtd{n} AFTER DELETE ON {xrt} \
             REFERENCING OLD TABLE AS oldtab FOR EACH STATEMENT EXECUTE FUNCTION {tf}();"
        )),
        raw(format!("ANALYZE {xr}, {xrd}, {xrp}, {xrc}, {xrg}, {xrt};")),
    ];
    g.exr.suites.push(ExrSuite { n, live: true });
    // Register the bulk table for the session probe windows (the other
    // fixtures are module-internal; their lifecycles pair create/drop in
    // this module alone).
    g.exr.events.push(DdlEvent {
        table: xr,
        pk: "pk".to_string(),
        kind: DdlEventKind::Created,
    });
    stmts
}

fn gen_drop(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.exr.live_suites();
    if live.is_empty() {
        g.fire("exr:fallback:create");
        return gen_create(g);
    }
    g.fire("exr:drop");
    let si = live[g.rng.below_usize(live.len())];
    let n = g.exr.suites[si].n;
    g.exr.suites[si].live = false;
    g.exr.events.push(DdlEvent {
        table: format!("fz_xr{n}"),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![
        // CASCADE folds the dependent CHECK OPTION view into the table
        // drop; the group leads with DROP TABLE <bulk table> so the
        // session probe window resolves to it.
        raw(format!(
            "DROP TABLE fz_xr{n}, fz_xrd{n}, fz_xrp{n}, fz_xrc{n}, fz_xrg{n}, fz_xrt{n}, fz_xrl{n} CASCADE;"
        )),
        raw(format!("DROP FUNCTION fz_xrtf{n}();")),
    ]
}

// --------------------------------------------------------------- prune ----

/// Exec-time partition pruning: generic plans over the RANGE/HASH tree
/// (nodeAppend/nodeMergeAppend runtime pruning incl. the all-pruned
/// no-subplan arm), plus nestloop join-driven per-rescan pruning.
fn gen_prune(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:prune");
    let n = g.exr.suites[si].n;
    let p = format!("fz_xrp{n}");
    let d = format!("fz_xrd{n}");
    let ps = format!("fz_xps{}", g.exr.next_obj);
    g.exr.next_obj += 1;
    let shape = g.weights.pick(
        g.rng,
        &["exr:prune:eq", "exr:prune:range", "exr:prune:merge", "exr:prune:join", "exr:prune:hash"],
    );
    let (prep, execs): (String, Vec<String>) = match shape {
        // pk = $1: exactly one partition survives; no-match value prunes ALL.
        "exr:prune:eq" => (
            format!("PREPARE {ps} (int) AS SELECT pk, a, t FROM {p} WHERE pk = $1 ORDER BY pk;"),
            vec![
                format!("EXECUTE {ps}({});", g.rng.below(1500)),
                format!("EXECUTE {ps}({});", 1500 + g.rng.below(1500)),
                format!("EXECUTE {ps}({});", 4500 + g.rng.below(700)),
                format!("EXECUTE {ps}(-{});", 1 + g.rng.below(100)), // default part range? negative -> default
                format!("EXECUTE {ps}(NULL);"),                     // all pruned
            ],
        ),
        // pk BETWEEN $1 AND $2: subset survives; inverted range prunes all.
        "exr:prune:range" => (
            format!(
                "PREPARE {ps} (int, int) AS SELECT count(*), min(pk), max(pk) FROM {p} \
                 WHERE pk BETWEEN $1 AND $2;"
            ),
            vec![
                format!("EXECUTE {ps}({}, {});", g.rng.below(1000), 1400 + g.rng.below(2000)),
                format!("EXECUTE {ps}({}, {});", 2900 + g.rng.below(200), 4400 + g.rng.below(900)),
                format!("EXECUTE {ps}(4000, 100);"),
            ],
        ),
        // ordered + LIMIT: MergeAppend with runtime pruning.
        "exr:prune:merge" => (
            format!(
                "PREPARE {ps} (int, int) AS SELECT pk, a FROM {p} WHERE pk >= $1 AND pk < $2 \
                 ORDER BY pk LIMIT 40;"
            ),
            vec![
                format!("EXECUTE {ps}({}, {});", 1000 + g.rng.below(600), 3200 + g.rng.below(1500)),
                format!("EXECUTE {ps}({}, {});", g.rng.below(200), 900 + g.rng.below(400)),
                format!("EXECUTE {ps}(5000, 5000);"),
            ],
        ),
        // HASH subtree pruning on a = $1 combined with the range key.
        "exr:prune:hash" => (
            format!(
                "PREPARE {ps} (int) AS SELECT count(*), min(pk) FROM {p} \
                 WHERE pk >= 3000 AND pk < 4500 AND a = $1;"
            ),
            vec![
                format!("EXECUTE {ps}({});", g.rng.below(64)),
                format!("EXECUTE {ps}({});", g.rng.below(64)),
                format!("EXECUTE {ps}(NULL);"),
            ],
        ),
        // nestloop join: per-rescan pruning of the inner Append.
        _ => (
            format!(
                "PREPARE {ps} (int) AS SELECT d.dk, p.pk, p.a FROM {d} d \
                 JOIN {p} p ON p.pk = d.dk * 20 WHERE d.dk <= $1 ORDER BY d.dk, p.pk;"
            ),
            vec![
                format!("EXECUTE {ps}({});", 3 + g.rng.below(9)),
                format!("EXECUTE {ps}(0);"),
            ],
        ),
    };
    g.fire(shape);
    let mut body = vec![raw(prep)];
    body.extend(execs.into_iter().map(raw));
    body.push(raw(format!("DEALLOCATE {ps};")));
    let mut gucs: Vec<(&str, &str)> = vec![("plan_cache_mode", "force_generic_plan")];
    if shape == "exr:prune:join" || shape == "exr:prune:merge" {
        gucs.push(("enable_hashjoin", "off"));
        gucs.push(("enable_mergejoin", "off"));
    }
    bracket(&gucs, body)
}

// ------------------------------------------------------------ winframe ----

const EXCLUDES: &[&str] = &[
    "",
    " EXCLUDE CURRENT ROW",
    " EXCLUDE GROUP",
    " EXCLUDE TIES",
    " EXCLUDE NO OTHERS",
];

/// nodeWindowAgg frame-option matrix. Aggregates ride non-total single-col
/// ORDER BY (peer-invariant results); positional functions ride a total
/// ORDER BY (a, pk).
fn gen_winframe(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:winframe");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xr{n}");
    let lo = 1 + g.rng.below(4000);
    let hi = lo + 400 + g.rng.below(800);
    let shape = g.weights.pick(
        g.rng,
        &["exr:wf:rangeint", "exr:wf:rangenum", "exr:wf:groups", "exr:wf:rows", "exr:wf:pos", "exr:wf:moving"],
    );
    g.fire(shape);
    let exc = pick_str(g, EXCLUDES);
    let sql = match shape {
        // RANGE offset PRECEDING/FOLLOWING on the int order key.
        "exr:wf:rangeint" => {
            let o1 = 1 + g.rng.below(20);
            let o2 = 1 + g.rng.below(20);
            let frame = pick_str(g, &[
                "RANGE BETWEEN {o1} PRECEDING AND {o2} FOLLOWING",
                "RANGE BETWEEN {o1} PRECEDING AND CURRENT ROW",
                "RANGE BETWEEN CURRENT ROW AND {o2} FOLLOWING",
                "RANGE BETWEEN {o1} FOLLOWING AND {o2} FOLLOWING",
                "RANGE BETWEEN UNBOUNDED PRECEDING AND {o2} FOLLOWING",
                "RANGE BETWEEN {o1} PRECEDING AND UNBOUNDED FOLLOWING",
            ])
            .replace("{o1}", &o1.to_string())
            .replace("{o2}", &o2.to_string());
            format!(
                "SELECT pk, sum(b) OVER w, count(*) OVER w FROM {t} \
                 WHERE pk BETWEEN {lo} AND {hi} AND a IS NOT NULL \
                 WINDOW w AS (ORDER BY a {frame}{exc}) ORDER BY pk;"
            )
        }
        // RANGE offset on the numeric order key (in_range numeric arm).
        "exr:wf:rangenum" => {
            let o = pick_str(g, &["0.5", "1.25", "3.75", "10.0"]);
            let frame = pick_str(g, &[
                "RANGE BETWEEN {o} PRECEDING AND {o} FOLLOWING",
                "RANGE BETWEEN {o} PRECEDING AND CURRENT ROW",
                "RANGE BETWEEN CURRENT ROW AND {o} FOLLOWING",
                "RANGE BETWEEN UNBOUNDED PRECEDING AND {o} PRECEDING",
            ])
            .replace("{o}", o);
            format!(
                "SELECT pk, sum(b) OVER w, count(*) OVER w FROM {t} \
                 WHERE pk BETWEEN {lo} AND {hi} \
                 WINDOW w AS (ORDER BY c {frame}{exc}) ORDER BY pk;"
            )
        }
        // GROUPS mode (peer-group offsets).
        "exr:wf:groups" => {
            let o1 = g.rng.below(4);
            let o2 = g.rng.below(4);
            let frame = pick_str(g, &[
                "GROUPS BETWEEN {o1} PRECEDING AND {o2} FOLLOWING",
                "GROUPS BETWEEN {o1} PRECEDING AND CURRENT ROW",
                "GROUPS BETWEEN CURRENT ROW AND {o2} FOLLOWING",
                "GROUPS BETWEEN UNBOUNDED PRECEDING AND {o2} PRECEDING",
                "GROUPS BETWEEN {o1} FOLLOWING AND UNBOUNDED FOLLOWING",
            ])
            .replace("{o1}", &o1.to_string())
            .replace("{o2}", &o2.to_string());
            format!(
                "SELECT pk, sum(b) OVER w, count(*) OVER w FROM {t} \
                 WHERE pk BETWEEN {lo} AND {hi} AND a IS NOT NULL \
                 WINDOW w AS (PARTITION BY b ORDER BY a {frame}{exc}) ORDER BY pk;"
            )
        }
        // ROWS offsets with aggregates (exclusion under ROWS).
        "exr:wf:rows" => {
            let o1 = 1 + g.rng.below(15);
            let o2 = 1 + g.rng.below(15);
            let frame = pick_str(g, &[
                "ROWS BETWEEN {o1} PRECEDING AND {o2} FOLLOWING",
                "ROWS BETWEEN {o1} FOLLOWING AND {o2} FOLLOWING",
                "ROWS BETWEEN UNBOUNDED PRECEDING AND {o1} PRECEDING",
                "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            ])
            .replace("{o1}", &o1.to_string())
            .replace("{o2}", &o2.to_string());
            format!(
                "SELECT pk, sum(b) OVER w, min(pk) OVER w FROM {t} \
                 WHERE pk BETWEEN {lo} AND {hi} \
                 WINDOW w AS (ORDER BY a, pk {frame}{exc}) ORDER BY pk;"
            )
        }
        // Positional functions in-frame (WinGetFuncArgInFrame arms) under a
        // TOTAL order; all three frame modes carry offsets or boundaries.
        "exr:wf:pos" => {
            let o1 = 1 + g.rng.below(8);
            let o2 = 1 + g.rng.below(8);
            let nth = 1 + g.rng.below(6);
            let frame = pick_str(g, &[
                "ROWS BETWEEN {o1} PRECEDING AND {o2} FOLLOWING",
                "ROWS BETWEEN {o1} FOLLOWING AND {o2} FOLLOWING",
                "ROWS BETWEEN UNBOUNDED PRECEDING AND {o1} PRECEDING",
                "GROUPS BETWEEN {o1} PRECEDING AND {o2} FOLLOWING",
                "GROUPS BETWEEN CURRENT ROW AND {o2} FOLLOWING",
                "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            ])
            .replace("{o1}", &o1.to_string())
            .replace("{o2}", &o2.to_string());
            format!(
                "SELECT pk, first_value(pk) OVER w, last_value(pk) OVER w, \
                 nth_value(pk, {nth}) OVER w, lead(pk, 2) OVER w, lag(pk, 3, -1) OVER w \
                 FROM {t} WHERE pk BETWEEN {lo} AND {hi} \
                 WINDOW w AS (ORDER BY a, pk {frame}{exc}) ORDER BY pk;"
            )
        }
        // Moving aggregates: inverse-transition arms (numeric + int8 + avg).
        _ => {
            let o1 = 2 + g.rng.below(30);
            format!(
                "SELECT pk, sum(c) OVER w, sum(pk::int8) OVER w, avg(b) OVER w, count(c) OVER w \
                 FROM {t} WHERE pk BETWEEN {lo} AND {hi} \
                 WINDOW w AS (ORDER BY pk ROWS BETWEEN {o1} PRECEDING AND CURRENT ROW) \
                 ORDER BY pk;"
            )
        }
    };
    vec![raw(sql)]
}

// ------------------------------------------------------------- subplan ----

/// nodeSubplan arms: hashed subplans (incl. the NULL table), per-row
/// scanned subplans, multi-column combining ops, correlated rescans.
fn gen_subplan(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:subplan");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xr{n}");
    let d = format!("fz_xrd{n}");
    let lo = 1 + g.rng.below(4500);
    let hi = lo + 200 + g.rng.below(600);
    let shape = g.weights.pick(
        g.rng,
        &["exr:sp:hashed", "exr:sp:nullhash", "exr:sp:multicol", "exr:sp:scan", "exr:sp:corr", "exr:sp:any"],
    );
    g.fire(shape);
    let sql = match shape {
        // Targetlist IN: stays a (hashed) SubPlan; count both branches.
        "exr:sp:hashed" => format!(
            "SELECT pk, b IN (SELECT da FROM {d}) AS m FROM {t} \
             WHERE pk BETWEEN {lo} AND {hi} ORDER BY pk;"
        ),
        // NOT IN with NULLs on both sides: hashed subplan + null-table arms.
        "exr:sp:nullhash" => format!(
            "SELECT count(*) FROM {t} WHERE pk BETWEEN {lo} AND {hi} \
             AND a NOT IN (SELECT CASE WHEN dk % 59 = 0 THEN NULL ELSE da * 7 END FROM {d});"
        ),
        // Multi-column row IN / NOT IN (hashed cross-check arms).
        "exr:sp:multicol" => {
            let neg = if g.rng.chance(1, 2) { "NOT " } else { "" };
            format!(
                "SELECT count(*) FROM {t} WHERE pk BETWEEN {lo} AND {hi} \
                 AND (b, a) {neg}IN (SELECT dk % 37, da * 10 FROM {d});"
            )
        }
        // Unhashable combining op: per-row ExecScanSubPlan.
        "exr:sp:scan" => {
            let op = pick_str(g, &["> ANY", ">= ALL", "< ANY", "<= ALL"]);
            format!(
                "SELECT count(*) FROM {t} WHERE pk BETWEEN {lo} AND {hi} \
                 AND a {op} (SELECT da * 9 FROM {d} WHERE dk < 40);"
            )
        }
        // Correlated EXISTS / scalar sub: rescan per outer row.
        "exr:sp:corr" => {
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT count(*) FROM {t} x WHERE pk BETWEEN {lo} AND {hi} \
                     AND EXISTS (SELECT 1 FROM {d} WHERE da = x.b AND dk > x.b);"
                )
            } else {
                format!(
                    "SELECT pk, (SELECT max(dk) FROM {d} WHERE da = x.b) FROM {t} x \
                     WHERE pk BETWEEN {lo} AND {hi} ORDER BY pk;"
                )
            }
        }
        // = ANY under a tiny work_mem bracket (hashed-vs-scan cost flip).
        _ => {
            return bracket(
                &[("work_mem", "'64kB'")],
                vec![raw(format!(
                    "SELECT count(*) FROM {t} WHERE pk BETWEEN {lo} AND {hi} \
                     AND t = ANY (SELECT 'r' || (dk % 101) FROM {d});"
                ))],
            );
        }
    };
    vec![raw(sql)]
}

// --------------------------------------------------------------- setop ----

/// nodeSetOp: INTERSECT/EXCEPT [ALL], hashed and sorted strategies.
fn gen_setop(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:setop");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xr{n}");
    let op = pick_str(g, &["INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]);
    let lo1 = 1 + g.rng.below(3000);
    let hi1 = lo1 + 500 + g.rng.below(1500);
    let lo2 = 1 + g.rng.below(3000);
    let hi2 = lo2 + 500 + g.rng.below(1500);
    // NULL-bearing a and duplicate-bearing b exercise tuple-match arms.
    let sql = format!(
        "SELECT a, b FROM {t} WHERE pk BETWEEN {lo1} AND {hi1} \
         {op} \
         SELECT a, b FROM {t} WHERE pk BETWEEN {lo2} AND {hi2} \
         ORDER BY 1 NULLS LAST, 2;"
    );
    let strat = g.weights.pick(g.rng, &["exr:so:hash", "exr:so:sort"]);
    g.fire(strat);
    if strat == "exr:so:sort" {
        bracket(&[("enable_hashagg", "off")], vec![raw(sql)])
    } else {
        vec![raw(sql)]
    }
}

// ------------------------------------------------------------ lockrows ----

/// nodeLockRows: every lock strength, SKIP LOCKED / NOWAIT, OF <rel>,
/// locked joins and LIMIT-under-lock. Single-session: rows are never
/// contended (the EPQ re-check arms are concurrency-only; ruled).
fn gen_lockrows(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:lockrows");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xr{n}");
    let d = format!("fz_xrd{n}");
    let strength = pick_str(g, &["UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"]);
    let wait = pick_str(g, &["", " SKIP LOCKED", " NOWAIT"]);
    let lo = 1 + g.rng.below(5000);
    let hi = lo + 20 + g.rng.below(200);
    let shape = g.weights.pick(g.rng, &["exr:lr:plain", "exr:lr:join", "exr:lr:limit"]);
    g.fire(shape);
    let body = match shape {
        "exr:lr:plain" => format!(
            "SELECT pk, a FROM {t} WHERE pk BETWEEN {lo} AND {hi} ORDER BY pk \
             FOR {strength}{wait};"
        ),
        "exr:lr:join" => format!(
            "SELECT x.pk, d.dk FROM {t} x JOIN {d} d ON d.dk = x.b + 1 \
             WHERE x.pk BETWEEN {lo} AND {hi} ORDER BY x.pk, d.dk \
             FOR {strength} OF x{wait};"
        ),
        _ => format!(
            "SELECT pk, t FROM {t} WHERE pk >= {lo} ORDER BY pk LIMIT 25 \
             FOR {strength}{wait};"
        ),
    };
    rollback(vec![raw(body)])
}

// ----------------------------------------------------------------- dml ----

/// nodeModifyTable non-MERGE arms: RETURNING old/new, ON CONFLICT
/// DO NOTHING/UPDATE (arbiter columns, named constraint, WHERE-clauses),
/// cross-partition UPDATE moves, generated-column DML (STORED + VIRTUAL),
/// identity OVERRIDING, WITH CHECK OPTION views, DELETE USING/UPDATE FROM.
fn gen_dml(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:dml");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xr{n}");
    let c = format!("fz_xrc{n}");
    let p = format!("fz_xrp{n}");
    let gt = format!("fz_xrg{n}");
    let v = format!("fz_xrv{n}");
    let d = format!("fz_xrd{n}");
    let shape = g.weights.pick(
        g.rng,
        &[
            "exr:dml:oldnew",
            "exr:dml:conflict",
            "exr:dml:conflictwhere",
            "exr:dml:crosspart",
            "exr:dml:gen",
            "exr:dml:wco",
            "exr:dml:fromusing",
        ],
    );
    g.fire(shape);
    let k = 1 + g.rng.below(700);
    let body: Vec<StmtKind> = match shape {
        // PG18 RETURNING old/new (with and without aliasing).
        "exr:dml:oldnew" => vec![
            raw(format!(
                "UPDATE {t} SET a = a + 5, c = c + 1 WHERE pk BETWEEN {k} AND {} \
                 RETURNING pk, old.a, new.a, old.c, new.c;",
                k + 30
            )),
            raw(format!(
                "INSERT INTO {t} VALUES (90001, 1, 2, 3.5, 'x') \
                 RETURNING WITH (OLD AS o, NEW AS nn) pk, o.a, nn.a, nn.t;"
            )),
            raw(format!(
                "DELETE FROM {t} WHERE pk BETWEEN {k} AND {} RETURNING old.pk, old.t, new.pk;",
                k + 10
            )),
        ],
        // ON CONFLICT arbiter arms.
        "exr:dml:conflict" => vec![
            raw(format!(
                "INSERT INTO {c} VALUES ({k}, {k} * 2, -1, -1) ON CONFLICT (k) DO NOTHING;"
            )),
            raw(format!(
                "INSERT INTO {c} VALUES (5000 + {k}, {k} * 2, -1, -1) ON CONFLICT (u) DO UPDATE \
                 SET v = excluded.v + {c}.v RETURNING k, u, v, w;"
            )),
            raw(format!(
                "INSERT INTO {c} VALUES (2000 + {k}, {k} * 2, 0, 0) \
                 ON CONFLICT ON CONSTRAINT {c}_u_key DO UPDATE SET w = {c}.w + 1 \
                 RETURNING k, old.w, new.w;"
            )),
        ],
        // Conditional DO UPDATE (conflict WHERE arms both ways).
        "exr:dml:conflictwhere" => vec![
            raw(format!(
                "INSERT INTO {c} SELECT i, i * 2, 99, 99 FROM generate_series({k}, {}) i \
                 ON CONFLICT (k) DO UPDATE SET v = excluded.v WHERE {c}.w < 3 \
                 RETURNING k, v, w;",
                k + 25
            )),
            raw(format!(
                "INSERT INTO {c} VALUES ({k}, {k} * 2, 7, 7) ON CONFLICT (k) DO UPDATE \
                 SET w = 100 WHERE false RETURNING k;"
            )),
        ],
        // Cross-partition move (delete+insert route): clear the landing
        // range first so the move COMPLETES (a collision would abort the
        // bracket and the route arms would never run), then a move into
        // the DEFAULT partition (negative keys — always free).
        "exr:dml:crosspart" => {
            let m = g.rng.below(1400);
            vec![
                raw(format!(
                    "DELETE FROM {p} WHERE pk BETWEEN {} AND {};",
                    m + 1600,
                    m + 1640
                )),
                raw(format!(
                    "UPDATE {p} SET pk = pk + 1600 WHERE pk BETWEEN {m} AND {} \
                     RETURNING pk, a;",
                    m + 40
                )),
                raw(format!(
                    "UPDATE {p} SET pk = -pk - 1 WHERE pk BETWEEN {} AND {} \
                     RETURNING old.pk, new.pk;",
                    m + 100,
                    m + 115
                )),
            ]
        }
        // Generated columns: recompute on INSERT/UPDATE, VIRTUAL projection,
        // identity OVERRIDING arms, generated-column error arm (matched).
        "exr:dml:gen" => vec![
            raw(format!(
                "INSERT INTO {gt} (x) VALUES ({k}) RETURNING id, x, gs, gv;"
            )),
            raw(format!(
                "INSERT INTO {gt} (id, x) OVERRIDING SYSTEM VALUE VALUES (9000 + {k}, 4) \
                 RETURNING id, gs, gv;"
            )),
            raw(format!(
                "UPDATE {gt} SET x = x + 10 WHERE id BETWEEN {} AND {} \
                 RETURNING id, old.gs, new.gs, old.gv, new.gv;",
                k % 150,
                k % 150 + 8
            )),
            raw(format!(
                "SELECT id, gv FROM {gt} WHERE id <= 20 ORDER BY id;"
            )),
            // ERROR arm last: it aborts the rollback bracket (matched).
            raw(format!("INSERT INTO {gt} (x, gs) VALUES (1, 1);")),
        ],
        // WITH CHECK OPTION: pass + violation (matched 44000).
        "exr:dml:wco" => vec![
            raw(format!(
                "INSERT INTO {v} VALUES (91000 + {k}, 5, 10) RETURNING pk, a, b;"
            )),
            raw(format!(
                "UPDATE {v} SET b = b + 1 WHERE pk BETWEEN {k} AND {} RETURNING pk, new.b;",
                k + 5
            )),
            // ERROR arms last: the first WCO violation aborts the rollback
            // bracket (both matched; the trailing statements are 25P02
            // no-ops by design).
            raw(format!("INSERT INTO {v} VALUES (92000 + {k}, 5, 35);")), // ERROR: WCO
            raw(format!(
                "UPDATE {v} SET b = 34 WHERE pk BETWEEN {k} AND {};",
                k + 5
            )), // ERROR: WCO
        ],
        // UPDATE ... FROM / DELETE ... USING with RETURNING both rels.
        _ => vec![
            raw(format!(
                "UPDATE {t} x SET a = d.da FROM {d} d WHERE d.dk = x.pk AND x.pk <= 240 \
                 AND x.pk BETWEEN {k} % 240 AND {k} % 240 + 30 \
                 RETURNING x.pk, d.dk, old.a, new.a;"
            )),
            raw(format!(
                "DELETE FROM {t} x USING {d} d WHERE d.dk = x.pk AND d.da < 5 \
                 RETURNING x.pk, d.da;"
            )),
        ],
    };
    rollback(body)
}

// --------------------------------------------------------------- merge ----

/// MERGE arm matrix: WHEN MATCHED [AND cond] UPDATE/DELETE/DO NOTHING,
/// NOT MATCHED [BY TARGET] INSERT, NOT MATCHED BY SOURCE UPDATE/DELETE,
/// RETURNING merge_action(), MERGE into partitioned target.
fn gen_merge(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:merge");
    let n = g.exr.suites[si].n;
    let c = format!("fz_xrc{n}");
    let d = format!("fz_xrd{n}");
    let p = format!("fz_xrp{n}");
    let k = 1 + g.rng.below(600);
    let shape = g.weights.pick(g.rng, &["exr:mg:full", "exr:mg:bysource", "exr:mg:part"]);
    g.fire(shape);
    let body = match shape {
        "exr:mg:full" => vec![raw(format!(
            "MERGE INTO {c} tgt USING (SELECT dk, da FROM {d} WHERE dk BETWEEN {k} % 200 AND {k} % 200 + 60) src \
             ON tgt.k = src.dk \
             WHEN MATCHED AND tgt.v < 10 THEN UPDATE SET v = src.da \
             WHEN MATCHED AND tgt.w = 6 THEN DELETE \
             WHEN MATCHED THEN DO NOTHING \
             WHEN NOT MATCHED AND src.da > 20 THEN INSERT VALUES (src.dk + 5000, src.dk * 2 + 10000, src.da, 0) \
             WHEN NOT MATCHED THEN DO NOTHING \
             RETURNING merge_action(), tgt.k, tgt.v, tgt.w;"
        ))],
        "exr:mg:bysource" => vec![raw(format!(
            "MERGE INTO {c} tgt USING (SELECT dk FROM {d} WHERE dk < 60) src ON tgt.k = src.dk \
             WHEN MATCHED THEN UPDATE SET w = tgt.w + 1 \
             WHEN NOT MATCHED BY SOURCE AND tgt.k < {} THEN DELETE \
             WHEN NOT MATCHED BY SOURCE AND tgt.k < {} THEN UPDATE SET v = -1 \
             WHEN NOT MATCHED BY TARGET THEN INSERT VALUES (src.dk + 6000, src.dk * 2 + 12000, 1, 1) \
             RETURNING merge_action(), tgt.k, old.v, new.v;",
            80 + k % 40,
            200 + k % 100
        ))],
        _ => vec![raw(format!(
            "MERGE INTO {p} tgt USING (SELECT i AS mk FROM generate_series({k}, {}) i) src \
             ON tgt.pk = src.mk \
             WHEN MATCHED AND tgt.a < 32 THEN UPDATE SET t = 'm' \
             WHEN MATCHED THEN DELETE \
             WHEN NOT MATCHED THEN INSERT VALUES (src.mk + 5300, src.mk % 64, 'i') \
             RETURNING merge_action(), tgt.pk, tgt.t;",
            k + 50
        ))],
    };
    rollback(body)
}

// ----------------------------------------------------------------- tid ----

/// nodeTidscan / nodeTidrangescan. ctids are always RESOLVED at run time
/// from pk (never literal beyond page-prefix ranges) and never projected.
fn gen_tid(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:tid");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xr{n}");
    let k1 = 1 + g.rng.below(5900);
    let k2 = 1 + g.rng.below(5900);
    let pg = 1 + g.rng.below(30);
    let shape = g.weights.pick(g.rng, &["exr:tid:eq", "exr:tid:in", "exr:tid:range", "exr:tid:back"]);
    g.fire(shape);
    match shape {
        // ctid = <subquery>: TidScan with one live ctid.
        "exr:tid:eq" => vec![raw(format!(
            "SELECT pk, a, b FROM {t} WHERE ctid = (SELECT ctid FROM {t} WHERE pk = {k1});"
        ))],
        // ctid = ANY(array of resolved ctids) + OR arm.
        "exr:tid:in" => vec![
            raw(format!(
                "SELECT pk, t FROM {t} WHERE ctid = ANY (ARRAY(SELECT ctid FROM {t} \
                 WHERE pk IN ({k1}, {k2}, 17))) ORDER BY pk;"
            )),
            raw(format!(
                "SELECT pk FROM {t} WHERE ctid = (SELECT ctid FROM {t} WHERE pk = {k1}) \
                 OR ctid = (SELECT ctid FROM {t} WHERE pk = {k2}) ORDER BY pk;"
            )),
        ],
        // TidRangeScan over a page prefix (aggregate probe; layout is
        // C-parity — hand-verified before mass legs).
        "exr:tid:range" => vec![raw(format!(
            "SELECT count(*), min(pk), max(pk) FROM {t} \
             WHERE ctid >= '(0,1)'::tid AND ctid < '({pg},1)'::tid;"
        ))],
        // Backward fetch over a TidRangeScan cursor.
        _ => vec![
            raw("BEGIN;".to_string()),
            raw(format!(
                "DECLARE fz_xtc{n} SCROLL CURSOR FOR SELECT pk FROM {t} \
                 WHERE ctid >= '(2,1)'::tid AND ctid < '({},1)'::tid;",
                2 + pg
            )),
            raw(format!("FETCH FORWARD 30 FROM fz_xtc{n};")),
            raw(format!("FETCH BACKWARD 10 FROM fz_xtc{n};")),
            raw(format!("FETCH FORWARD ALL FROM fz_xtc{n};")),
            raw(format!("FETCH BACKWARD ALL FROM fz_xtc{n};")),
            raw(format!("CLOSE fz_xtc{n};")),
            raw("COMMIT;".to_string()),
        ],
    }
}

// --------------------------------------------------------------- tfunc ----

/// nodeTableFuncscan via JSON_TABLE: nested paths, ordinality, EXISTS /
/// FORMAT JSON columns, ON EMPTY/ON ERROR behaviors (incl. matched error
/// arms). The rig builds carry no libxml, so XMLTABLE is out of scope.
fn gen_tfunc(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("exr:tfunc");
    let shape = g.weights.pick(g.rng, &["exr:tf:nested", "exr:tf:opts", "exr:tf:err", "exr:tf:lateral"]);
    g.fire(shape);
    let k = g.rng.below(50);
    match shape {
        "exr:tf:nested" => vec![raw(format!(
            "SELECT * FROM JSON_TABLE( \
               '[{{\"id\": {k}, \"tags\": [1, 2, 3], \"kids\": [{{\"n\": \"a\"}}, {{\"n\": \"b\"}}]}}, \
                 {{\"id\": {}, \"tags\": [], \"kids\": []}}]', \
               '$[*]' COLUMNS ( \
                 ord FOR ORDINALITY, \
                 id int PATH '$.id', \
                 NESTED PATH '$.tags[*]' COLUMNS (tag int PATH '$'), \
                 NESTED PATH '$.kids[*]' COLUMNS (kn text PATH '$.n') \
               )) jt ORDER BY ord, tag NULLS FIRST, kn NULLS FIRST;",
            k + 1
        ))],
        "exr:tf:opts" => vec![raw(format!(
            "SELECT * FROM JSON_TABLE( \
               '{{\"a\": {k}, \"b\": \"zz\", \"c\": [7, 8]}}', '$' COLUMNS ( \
                 a int PATH '$.a' DEFAULT -1 ON EMPTY, \
                 miss int PATH '$.nope' DEFAULT -7 ON EMPTY DEFAULT -9 ON ERROR, \
                 be boolean EXISTS PATH '$.b', \
                 cj jsonb FORMAT JSON PATH '$.c', \
                 cw text PATH '$.c' WITH WRAPPER \
               )) jt;"
        ))],
        "exr:tf:err" => vec![
            raw(format!(
                "SELECT * FROM JSON_TABLE('{{\"a\": \"notint\"}}', '$' COLUMNS \
                 (a int PATH '$.a' ERROR ON ERROR)) jt;"
            )), // ERROR (matched)
            raw(format!(
                "SELECT * FROM JSON_TABLE('{{\"a\": \"notint\"}}', '$' COLUMNS \
                 (a int PATH '$.a' NULL ON ERROR, b int PATH '$.a' DEFAULT {k} ON ERROR)) jt;"
            )),
        ],
        _ => vec![raw(format!(
            "SELECT s.i, jt.v FROM generate_series(1, 4) s(i), \
             JSON_TABLE(('[' || s.i || ',' || s.i * 2 || ']')::jsonb, '$[*]' \
               COLUMNS (v int PATH '$')) jt ORDER BY s.i, jt.v;"
        ))],
    }
}

// ------------------------------------------------------------- ntstore ----

/// nodeNamedtuplestore: statement triggers with transition tables; the
/// trigger function aggregates newtab/oldtab into the log, probed with a
/// total order, all inside a rollback bracket.
fn gen_ntstore(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:ntstore");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xrt{n}");
    let l = format!("fz_xrl{n}");
    let k = 1 + g.rng.below(300);
    let shape = g.weights.pick(g.rng, &["exr:nt:ins", "exr:nt:upd", "exr:nt:del", "exr:nt:mix"]);
    g.fire(shape);
    let dml: Vec<StmtKind> = match shape {
        "exr:nt:ins" => vec![raw(format!(
            "INSERT INTO {t} SELECT 1000 + i, i % 11 FROM generate_series({k}, {}) i;",
            k + 60
        ))],
        "exr:nt:upd" => vec![raw(format!(
            "UPDATE {t} SET v = v + 3 WHERE k BETWEEN {k} AND {};",
            k + 80
        ))],
        "exr:nt:del" => vec![raw(format!(
            "DELETE FROM {t} WHERE k BETWEEN {k} AND {};",
            k + 40
        ))],
        _ => vec![
            raw(format!(
                "INSERT INTO {t} SELECT 2000 + i, i FROM generate_series(1, 25) i;"
            )),
            raw(format!("UPDATE {t} SET v = -v WHERE k BETWEEN {k} AND {};", k + 30)),
            raw(format!("DELETE FROM {t} WHERE k % 17 = {};", k % 17)),
            // empty transition tables (zero-row DML still fires the triggers)
            raw(format!("UPDATE {t} SET v = 0 WHERE false;")),
        ],
    };
    let mut body = dml;
    body.push(raw(format!("SELECT tag, n, s FROM {l} ORDER BY id;")));
    rollback(body)
}

// ----------------------------------------------------------- currentof ----

/// execCurrentOf: UPDATE/DELETE WHERE CURRENT OF over plain and
/// partitioned targets (FOR UPDATE and plain cursors).
fn gen_currentof(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:currentof");
    let n = g.exr.suites[si].n;
    // The partitioned variant always locks (FOR UPDATE cursors resolve
    // CURRENT OF via the erm ctid, so any plan shape works); the plain
    // variant sometimes runs unlocked, which requires — and pins, via the
    // SET LOCALs below — a simply-updatable index scan.
    let (t, key, part) = if g.rng.chance(1, 3) {
        g.fire("exr:co:part");
        (format!("fz_xrp{n}"), "pk", true)
    } else {
        g.fire("exr:co:plain");
        (format!("fz_xr{n}"), "pk", false)
    };
    let cur = format!("fz_xoc{}", g.exr.next_obj);
    g.exr.next_obj += 1;
    let k = 1 + g.rng.below(4000);
    let forup = if part || g.rng.chance(1, 2) { " FOR UPDATE" } else { "" };
    rollback(vec![
        // Pin the cursor to an index path: a Sort-topped cursor is not
        // simply updatable and WHERE CURRENT OF raises 24000 (that arm
        // stays reachable via the partitioned variant's multi-part scans).
        raw("SET LOCAL enable_seqscan = off;".to_string()),
        raw("SET LOCAL enable_sort = off;".to_string()),
        raw(format!(
            "DECLARE {cur} CURSOR FOR SELECT {key}, a FROM {t} \
             WHERE {key} BETWEEN {k} AND {} ORDER BY {key}{forup};",
            k + 50
        )),
        raw(format!("FETCH 3 FROM {cur};")),
        raw(format!(
            "UPDATE {t} SET a = a + 100 WHERE CURRENT OF {cur} \
             RETURNING {key}, a;"
        )),
        raw(format!("FETCH 2 FROM {cur};")),
        raw(format!("DELETE FROM {t} WHERE CURRENT OF {cur} RETURNING {key};")),
        raw(format!("FETCH 1 FROM {cur};")),
        raw(format!("CLOSE {cur};")),
    ])
}

// --------------------------------------------------------------- sqlfn ----

/// functions.c arms: SQL-language functions (quoted + BEGIN ATOMIC),
/// polymorphic argtypes, RETURNS TABLE/SETOF, VARIADIC, qualified param
/// refs, composite-return coercion, set-returning FROM with LIMIT.
fn gen_sqlfn(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:sqlfn");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xr{n}");
    let f = format!("fz_xfn{}", g.exr.next_obj);
    g.exr.next_obj += 1;
    let shape = g.weights.pick(
        g.rng,
        &["exr:fn:poly", "exr:fn:table", "exr:fn:atomic", "exr:fn:variadic", "exr:fn:comp", "exr:fn:setof", "exr:fn:dml"],
    );
    g.fire(shape);
    let k = 1 + g.rng.below(1000);
    let (mk, calls, dropf): (String, Vec<String>, String) = match shape {
        "exr:fn:poly" => (
            format!(
                "CREATE FUNCTION {f}(x anyelement, y anyelement) RETURNS anyelement \
                 LANGUAGE sql IMMUTABLE AS 'SELECT CASE WHEN x >= y THEN x ELSE y END';"
            ),
            vec![
                format!("SELECT {f}({k}, 7), {f}({k}::int8, 9::int8), {f}('a'::text, 'b'), {f}(1.5::numeric, 2.25);"),
                format!("SELECT {f}(ARRAY[1, {k}], ARRAY[2, 3]);"),
            ],
            format!("DROP FUNCTION {f}(anyelement, anyelement);"),
        ),
        "exr:fn:table" => (
            format!(
                "CREATE FUNCTION {f}(lim int) RETURNS TABLE (opk int, ob int) \
                 LANGUAGE sql STABLE AS $q$ SELECT pk, b FROM {t} WHERE pk <= lim \
                 ORDER BY pk $q$;"
            ),
            vec![
                format!("SELECT * FROM {f}({}) ORDER BY opk;", 5 + k % 40),
                format!("SELECT opk FROM {f}(200) ORDER BY opk LIMIT 7;"),
                format!("SELECT (x).opk, (x).ob FROM (SELECT {f}(4) x) s ORDER BY 1;"),
            ],
            format!("DROP FUNCTION {f}(int);"),
        ),
        "exr:fn:atomic" => (
            format!(
                "CREATE FUNCTION {f}(a int, b int DEFAULT 11) RETURNS int LANGUAGE sql \
                 BEGIN ATOMIC SELECT {f}.a * 100 + {f}.b; END;"
            ),
            vec![
                format!("SELECT {f}({k});"),
                format!("SELECT {f}({k}, 3), {f}(b => 5, a => 2);"),
            ],
            format!("DROP FUNCTION {f}(int, int);"),
        ),
        "exr:fn:variadic" => (
            format!(
                "CREATE FUNCTION {f}(sep text, VARIADIC xs int[]) RETURNS text \
                 LANGUAGE sql IMMUTABLE AS $q$ SELECT array_to_string(xs, sep) $q$;"
            ),
            vec![
                format!("SELECT {f}('-', 1, 2, {k});"),
                format!("SELECT {f}('+', VARIADIC ARRAY[{k}, 5]);"),
            ],
            format!("DROP FUNCTION {f}(text, int[]);"),
        ),
        // Composite return with column-coercion (check_sql_stmt_retval).
        "exr:fn:comp" => (
            format!(
                "CREATE FUNCTION {f}(k int, OUT s int8, OUT lbl text) LANGUAGE sql \
                 AS $q$ SELECT k + 1, 'v' || k $q$;"
            ),
            vec![
                format!("SELECT * FROM {f}({k});"),
                format!("SELECT ({f}({k})).s;"),
            ],
            format!("DROP FUNCTION {f}(int);"),
        ),
        // DML-returning SQL function (check_sql_stmt_retval DML arms):
        // the call is a same-pk conflict no-op, so the fixture stays
        // bit-stable without a bracket.
        "exr:fn:dml" => (
            format!(
                "CREATE FUNCTION {f}(nk int) RETURNS int4 LANGUAGE sql VOLATILE \
                 AS $q$ INSERT INTO {t} VALUES (nk, 1, 2, 3.5, 'x') \
                 ON CONFLICT (pk) DO NOTHING RETURNING pk $q$;"
            ),
            vec![
                format!("SELECT {f}({});", 1 + k % 5000), // conflict -> NULL
            ],
            format!("DROP FUNCTION {f}(int);"),
        ),
        // SETOF + lazy eval halted by LIMIT (fmgr_sql resume arms) + the
        // abandoned-mid-scan targetlist SRF (ShutdownSQLFunction).
        _ => (
            format!(
                "CREATE FUNCTION {f}(lim int) RETURNS SETOF {t} LANGUAGE sql STABLE \
                 AS $q$ SELECT * FROM {t} WHERE pk <= lim ORDER BY pk $q$;"
            ),
            vec![
                format!("SELECT pk, b FROM {f}(300) ORDER BY pk LIMIT 5;"),
                format!("SELECT count(*) FROM {f}({});", 20 + k % 200),
                format!("SELECT ({f}(40)).pk LIMIT 3;"),
            ],
            format!("DROP FUNCTION {f}(int);"),
        ),
    };
    let mut stmts = vec![raw(mk)];
    stmts.extend(calls.into_iter().map(raw));
    stmts.push(raw(dropf));
    stmts
}

// --------------------------------------------------------------- limit ----

/// nodeLimit: WITH TIES, OFFSET beyond input, LIMIT 0/NULL, backward
/// fetch through a Limit node.
fn gen_limit(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:limit");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xr{n}");
    let lo = 1 + g.rng.below(5000);
    let shape = g.weights.pick(g.rng, &["exr:lim:ties", "exr:lim:edge", "exr:lim:back"]);
    g.fire(shape);
    match shape {
        // WITH TIES needs ORDER BY; the tie column is duplicate-heavy and
        // the projection stays peer-invariant (the tie key itself + count).
        "exr:lim:ties" => {
            let m = 5 + g.rng.below(30);
            vec![raw(format!(
                "SELECT b, count(*) FROM (SELECT b FROM {t} WHERE pk >= {lo} \
                 ORDER BY b FETCH FIRST {m} ROWS WITH TIES) s GROUP BY b ORDER BY b;"
            ))]
        }
        "exr:lim:edge" => vec![
            raw(format!(
                "SELECT pk FROM {t} WHERE pk >= {lo} ORDER BY pk LIMIT 0;"
            )),
            raw(format!(
                "SELECT pk FROM {t} WHERE pk >= {lo} ORDER BY pk LIMIT NULL OFFSET 3;"
            )),
            raw(format!(
                "SELECT pk FROM {t} WHERE pk BETWEEN {lo} AND {} ORDER BY pk \
                 LIMIT 5 OFFSET 100000;",
                lo + 50
            )),
            raw(format!(
                "SELECT pk FROM {t} WHERE pk BETWEEN {lo} AND {} ORDER BY pk \
                 OFFSET 10 ROWS FETCH NEXT 4 ROWS ONLY;",
                lo + 50
            )),
        ],
        _ => {
            let cur = format!("fz_xlc{}", g.exr.next_obj);
            g.exr.next_obj += 1;
            vec![
                raw("BEGIN;".to_string()),
                raw(format!(
                    "DECLARE {cur} SCROLL CURSOR FOR SELECT pk FROM {t} \
                     WHERE pk >= {lo} ORDER BY pk LIMIT 25 OFFSET 5;"
                )),
                raw(format!("FETCH FORWARD ALL FROM {cur};")),
                raw(format!("FETCH BACKWARD 12 FROM {cur};")),
                raw(format!("FETCH FORWARD 6 FROM {cur};")),
                raw(format!("FETCH BACKWARD ALL FROM {cur};")),
                raw(format!("CLOSE {cur};")),
                raw("COMMIT;".to_string()),
            ]
        }
    }
}

// --------------------------------------------------------------- iscan ----

/// nodeIndexscan key arms: row comparisons, SAOP arrays (empty/NULL
/// elements), IS NULL keys, DESC scans — under enable_seqscan/bitmapscan
/// off so the plain index path takes them.
fn gen_iscan(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:iscan");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xr{n}");
    let shape = g.weights.pick(g.rng, &["exr:is:rowcmp", "exr:is:saop", "exr:is:null", "exr:is:desc"]);
    g.fire(shape);
    let b0 = g.rng.below(37);
    let a0 = g.rng.below(250);
    let body = match shape {
        "exr:is:rowcmp" => {
            let op = pick_str(g, &[">", ">=", "<", "<="]);
            vec![raw(format!(
                "SELECT b, a, pk FROM {t} WHERE (b, a) {op} ({b0}, {a0}) \
                 ORDER BY b, a, pk LIMIT 60;"
            ))]
        }
        "exr:is:saop" => vec![
            raw(format!(
                "SELECT count(*), min(pk) FROM {t} WHERE b = ANY (ARRAY[{b0}, {}, NULL, {b0}]);",
                (b0 + 5) % 37
            )),
            raw(format!(
                "SELECT count(*) FROM {t} WHERE b = ANY ('{{}}'::int4[]);"
            )),
            raw(format!(
                "SELECT count(*) FROM {t} WHERE b = ANY (ARRAY[{b0}]) AND a = ANY (ARRAY[{a0}, {}]);",
                (a0 + 13) % 250
            )),
        ],
        "exr:is:null" => vec![
            raw(format!(
                "SELECT pk FROM {t} WHERE a IS NULL ORDER BY pk LIMIT 30;"
            )),
            raw(format!(
                "SELECT count(*) FROM {t} WHERE a IS NOT NULL AND a < 3;"
            )),
        ],
        _ => vec![raw(format!(
            "SELECT b, a, pk FROM {t} WHERE b <= {b0} ORDER BY b DESC, a DESC NULLS FIRST, pk DESC LIMIT 50;"
        ))],
    };
    bracket(
        &[("enable_seqscan", "off"), ("enable_bitmapscan", "off")],
        body,
    )
}

// -------------------------------------------------------------- rescan ----

/// ExecReScan variety: LATERAL subqueries under forced nestloop rescans
/// (Sort/Agg/Group/SetOp/Limit/WindowAgg inner nodes without Material).
fn gen_rescan(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:rescan");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xr{n}");
    let d = format!("fz_xrd{n}");
    let inner = g.weights.pick(
        g.rng,
        &["exr:rs:sort", "exr:rs:agg", "exr:rs:setop", "exr:rs:win", "exr:rs:limit"],
    );
    g.fire(inner);
    let lim = 3 + g.rng.below(12);
    let sub = match inner {
        "exr:rs:sort" => format!(
            "SELECT pk, a FROM {t} WHERE b = d.dk % 37 ORDER BY a NULLS LAST, pk LIMIT {lim}"
        ),
        "exr:rs:agg" => format!(
            "SELECT count(*) AS cnt, min(pk) AS mp FROM {t} WHERE b = d.dk % 37"
        ),
        "exr:rs:setop" => format!(
            "SELECT a FROM {t} WHERE b = d.dk % 37 INTERSECT SELECT da FROM {d} ORDER BY 1 NULLS LAST LIMIT {lim}"
        ),
        "exr:rs:win" => format!(
            "SELECT pk, sum(b) OVER (ORDER BY pk ROWS 2 PRECEDING) AS sb FROM {t} \
             WHERE b = d.dk % 37 ORDER BY pk LIMIT {lim}"
        ),
        _ => format!(
            "SELECT pk FROM {t} WHERE b = d.dk % 37 ORDER BY pk LIMIT {lim} OFFSET 2"
        ),
    };
    let dk = 4 + g.rng.below(20);
    bracket(
        &[
            ("enable_hashjoin", "off"),
            ("enable_mergejoin", "off"),
            ("enable_material", "off"),
            ("enable_memoize", "off"),
        ],
        vec![raw(format!(
            "SELECT d.dk, s.* FROM {d} d LEFT JOIN LATERAL ({sub}) s ON true \
             WHERE d.dk <= {dk} ORDER BY d.dk, s.{};",
            match inner {
                "exr:rs:agg" => "cnt, s.mp".to_string(),
                "exr:rs:setop" => "a NULLS LAST".to_string(),
                "exr:rs:win" => "pk".to_string(),
                _ => "pk".to_string(),
            }
        ))],
    )
}

// ----------------------------------------------------------- parappend ----

/// Parallel Append: forced-parallel scans of the partition tree drain the
/// choose_next_subplan_for_leader/for_worker arms (workers flush their
/// own gcov counters at exit) plus the DSM setup/reinit machinery via a
/// rescan variant. Exact-typed aggregate probes only (worker scheduling
/// order is a non-surface).
fn gen_parappend(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:parappend");
    let n = g.exr.suites[si].n;
    let p = format!("fz_xrp{n}");
    let d = format!("fz_xrd{n}");
    let shape = g.weights.pick(g.rng, &["exr:pa:agg", "exr:pa:mixed", "exr:pa:rescan"]);
    g.fire(shape);
    let body = match shape {
        // Parallel Append of partial per-partition scans.
        "exr:pa:agg" => vec![raw(format!(
            "SELECT count(*), min(pk), max(pk), sum(a::int8) FROM {p} WHERE a <> {};",
            g.rng.below(64)
        ))],
        // Mixed UNION ALL append (partial + partial across distinct rels).
        "exr:pa:mixed" => vec![raw(format!(
            "SELECT count(*), sum(x.v::int8) FROM (SELECT a AS v FROM {p} \
             UNION ALL SELECT da FROM {d}) x WHERE x.v >= {};",
            g.rng.below(20)
        ))],
        // Gather rescanned under a nestloop: ExecReScanAppend on the
        // parallel plan + ReInitializeDSM arms.
        _ => vec![raw(format!(
            "SELECT s.i, (SELECT count(*) FROM {p} WHERE a <> s.i) FROM \
             generate_series(1, 3) s(i) ORDER BY s.i;"
        ))],
    };
    bracket(
        &[
            ("parallel_setup_cost", "0"),
            ("parallel_tuple_cost", "0"),
            ("min_parallel_table_scan_size", "0"),
            ("max_parallel_workers_per_gather", "2"),
        ],
        body,
    )
}

// ---------------------------------------------------------------- excl ----

/// execIndexing exclusion-constraint arms: in-group table with a btree
/// EXCLUDE constraint — passing inserts, violating inserts (matched
/// 23P01), ON CONFLICT DO NOTHING over the exclusion arbiter, and a
/// deferred variant checked at COMMIT.
fn gen_excl(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("exr:excl");
    let t = format!("fz_xre{}", g.exr.next_obj);
    g.exr.next_obj += 1;
    let k = 1 + g.rng.below(400);
    let deferred = g.rng.chance(1, 3);
    let con = if deferred {
        g.fire("exr:excl:deferred");
        "EXCLUDE USING btree (u WITH =) DEFERRABLE INITIALLY DEFERRED"
    } else {
        g.fire("exr:excl:immediate");
        "EXCLUDE USING btree (u WITH =)"
    };
    let mut stmts = vec![
        raw(format!(
            "CREATE TABLE {t} (k int4 PRIMARY KEY, u int4, {con});"
        )),
        raw(format!(
            "INSERT INTO {t} SELECT i, i * 3 FROM generate_series(1, 200) i;"
        )),
    ];
    if deferred {
        stmts.push(raw("BEGIN;".to_string()));
        stmts.push(raw(format!(
            "INSERT INTO {t} VALUES (900 + {k}, {k} * 3);" // violation held to COMMIT
        )));
        stmts.push(raw(format!(
            "UPDATE {t} SET u = -u WHERE k = 900 + {k};" // violation cured
        )));
        stmts.push(raw("COMMIT;".to_string()));
        stmts.push(raw(format!(
            "SELECT count(*), min(u) FROM {t} WHERE k > 900;"
        )));
    } else {
        stmts.push(raw(format!(
            "INSERT INTO {t} VALUES (700 + {k}, -{k});"
        )));
        stmts.push(raw(format!(
            "INSERT INTO {t} VALUES (800 + {k}, {k} * 3);" // ERROR 23P01 (matched)
        )));
        stmts.push(raw(format!(
            "INSERT INTO {t} VALUES (850 + {k}, {k} * 3) ON CONFLICT DO NOTHING;"
        )));
        stmts.push(raw(format!(
            "SELECT count(*), min(k) FROM {t} WHERE u = {k} * 3;"
        )));
    }
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// ------------------------------------------------------------------ mj ----

/// nodeMergejoin fill arms: forced merge joins with duplicate-heavy keys,
/// FULL/LEFT/RIGHT fill, sorted-by-index vs explicit-sort inputs.
fn gen_mj(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr:mj");
    let n = g.exr.suites[si].n;
    let t = format!("fz_xr{n}");
    let d = format!("fz_xrd{n}");
    let jt = pick_str(g, &["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"]);
    let lo = 1 + g.rng.below(3000);
    let hi = lo + 300 + g.rng.below(1200);
    let shape = g.weights.pick(g.rng, &["exr:mj:dup", "exr:mj:dim"]);
    g.fire(shape);
    let sql = match shape {
        // dup-heavy self-join on b (0..37 over thousands of rows).
        "exr:mj:dup" => format!(
            "SELECT x.b, count(*), min(x.pk), min(y.pk) FROM \
             (SELECT b, pk FROM {t} WHERE pk BETWEEN {lo} AND {}) x \
             {jt} (SELECT b, pk FROM {t} WHERE pk BETWEEN {} AND {hi}) y \
             ON x.b = y.b GROUP BY x.b ORDER BY x.b;",
            lo + 400,
            hi - 400
        ),
        _ => format!(
            "SELECT count(*), min(x.pk), max(d.dk) FROM {t} x {jt} {d} d ON x.a = d.da * 10 \
             AND x.pk BETWEEN {lo} AND {hi};"
        ),
    };
    bracket(
        &[
            ("enable_hashjoin", "off"),
            ("enable_nestloop", "off"),
        ],
        vec![raw(sql)],
    )
}
