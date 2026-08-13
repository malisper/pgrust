//! Executor-residue drain, round 2 (EXEC-RESIDUE): the arms that LD2/LD4/
//! LD9 + the exr module left dark because exr emits every one of these
//! operators only at the TOP of the plan, where it is executed exactly
//! once. The residue (docs/fuzzing/sql-reachable-queue.tsv, gap-report-008
//! call-graph sweep, cpg-ref REL_18_3@62d6c7d) is the RESCAN half of each
//! node — reached only when the operator sits on the inner side of a
//! correlated LATERAL / nestloop and is actually re-executed per outer row:
//!
//!   * ExecReScanSetOp            (nodeSetOp.c)            — INTERSECT/EXCEPT
//!     rescanned; plus the sorted/hashed retrieval internals themselves
//!     (setop_retrieve_sorted / setop_load_group / set_output_count /
//!     setop_compare_slots / setop_fill_hash_table / setop_retrieve_hash_table
//!     / ExecGetCommonChildSlotOps) which the top-level exr:setop weight
//!     under-drives.
//!   * ExecReScanRecursiveUnion   (nodeRecursiveunion.c)  — WITH RECURSIVE
//!     UNION (distinct → the visited-row hashtable, build_hash_table)
//!     rescanned under a correlated outer; drags the worktable rescan with
//!     it (nodeWorktablescan.c first-scan-vs-rescan).
//!   * ExecReScanWindowAgg        (nodeWindowAgg.c)       — a window
//!     aggregate rescanned under LATERAL (frame/peer spool torn down and
//!     rebuilt each outer row).
//!   * ExecReScanNamedTuplestoreScan (nodeNamedtuplestorescan.c) — an AFTER
//!     STATEMENT trigger whose function RESCANS the transition table (a
//!     correlated sub-select over newtab/oldtab re-reads the tuplestore per
//!     outer row). exr:ntstore only ever scans the transition table once.
//!   * the plan-serialization arms (_outSetOp/_outProjectSet/_outWorkTableScan
//!     /_outRecursiveUnion/_outWindowAgg via debug_print_plan; the _read*
//!     twins via debug_parallel_query) — pure serialization surface, driven
//!     by the toggle GUCs.
//!
//! NOT drained here (ruled FAULT-ONLY/CONCURRENCY, matching exr's EPQ note):
//!   * WorkTableScanRecheck / NamedTuplestoreScanRecheck — the Recheck
//!     callbacks fire only under EvalPlanQual, i.e. a concurrently-committed
//!     update invalidating a fetched tuple. A single-session stream cannot
//!     produce that. (A correlated rescan may still graze the surrounding
//!     ExecReScan* arm, which IS drained.)
//!   * XMLTABLE (nodeTableFuncscan libxml arm) — rig builds are
//!     --without-libxml; a shared error surface, not a drain target. The
//!     JSON_TABLE half of nodeTableFuncscan is already drained by exr:tfunc.
//!
//! Determinism laws (identical to exr; see crate::exr):
//!   - every row-returning statement carries a TOTAL order (ORDER BY ending
//!     in the outer PK and, inside the lateral, a total inner order); the
//!     window/setop projections are peer-invariant (aggregate results, or
//!     total-ordered positional values).
//!   - no float surfaces: int4/int8/numeric only (numeric is exact).
//!   - the transition-table family runs inside BEGIN..ROLLBACK; the fixture
//!     is bit-stable across groups.
//!   - every SET LOCAL rides inside its own group (the enclosing
//!     transaction) and is discarded at group end, or is paired with RESET.
//!   - debug_print_plan writes at LOG to the SERVER log only; the group
//!     pins client_min_messages=warning so the plan dump never reaches the
//!     client result set (no diff surface).
//!   - fixtures stay well under default_statistics_target so ANALYZE is
//!     exhaustive and plans are identical on both sides.

use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

const MAX_LIVE_SUITES: usize = 1;

/// One fixture suite (all tables share the numeric suffix).
#[derive(Clone, Debug)]
pub struct Exr2Suite {
    pub n: u32,
    pub live: bool,
}

/// Session-persistent round-2 residue fixture model (swapped in and out of
/// `Gen` by the session loop exactly like `ExrState`).
#[derive(Clone, Debug, Default)]
pub struct Exr2State {
    pub suites: Vec<Exr2Suite>,
    next_suite: u32,
    events: Vec<DdlEvent>,
}

impl Exr2State {
    pub fn new() -> Exr2State {
        Exr2State::default()
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

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

/// Wrap `body` in BEGIN..ROLLBACK (SET LOCALs and mutations discard at
/// ROLLBACK; fixtures stay bit-stable).
fn rollback(body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut stmts = vec![raw("BEGIN;".to_string())];
    stmts.extend(body);
    stmts.push(raw("ROLLBACK;".to_string()));
    stmts
}

/// Pick a live suite or synthesize the create group.
macro_rules! need_suite {
    ($g:expr) => {{
        let live = $g.exr2.live_suites();
        if live.is_empty() {
            $g.fire("exr2:fallback:create");
            return gen_create($g);
        }
        live[$g.rng.below_usize(live.len())]
    }};
}

const SHAPES: &[&str] = &[
    "exr2:create",
    "exr2:drop",
    "exr2:setop",
    "exr2:setoprescan",
    "exr2:recursive",
    "exr2:winrescan",
    "exr2:ntrescan",
    "exr2:serial",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_exr2_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("exr2");
    match g.weights.pick(g.rng, SHAPES) {
        "exr2:create" => gen_create(g),
        "exr2:drop" => gen_drop(g),
        "exr2:setop" => gen_setop(g),
        "exr2:setoprescan" => gen_setop_rescan(g),
        "exr2:recursive" => gen_recursive(g),
        "exr2:winrescan" => gen_win_rescan(g),
        "exr2:ntrescan" => gen_nt_rescan(g),
        _ => gen_serial(g),
    }
}

// -------------------------------------------------------------- create ----

fn gen_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.exr2.live_suites().len() >= MAX_LIVE_SUITES {
        g.fire("exr2:cap:suites");
        return gen_drop(g);
    }
    g.fire("exr2:create");
    let n = g.exr2.next_suite;
    g.exr2.next_suite += 1;
    let t = format!("fz_x2{n}");
    let o = format!("fz_x2o{n}");
    let tt = format!("fz_x2t{n}");
    let l = format!("fz_x2l{n}");
    let tf = format!("fz_x2tf{n}");
    let stmts = vec![
        // base heap: small, duplicate-heavy b (setop/window peer groups),
        // sparse NULLs in a (NOT IN / sum-ignores-NULL arms).
        raw(format!(
            "CREATE TABLE {t} (pk int4 PRIMARY KEY, a int4, b int4, c numeric);"
        )),
        raw(format!(
            "INSERT INTO {t} SELECT i, \
             CASE WHEN i % 101 = 0 THEN NULL ELSE (i * 7) % 40 END, \
             (i * 3) % 12, (((i * 5) % 900)::numeric) / 100 \
             FROM generate_series(1, 1200) i;"
        )),
        raw(format!("CREATE INDEX fz_x2ib{n} ON {t} (b);")),
        raw(format!("CREATE INDEX fz_x2iba{n} ON {t} (b, a);")),
        // outer driver for the correlated-LATERAL rescan probes: tiny, so
        // the inner node is rescanned a bounded number of times.
        raw(format!(
            "CREATE TABLE {o} (ok int4 PRIMARY KEY, g int4);"
        )),
        raw(format!(
            "INSERT INTO {o} SELECT i, (i * 2) % 9 FROM generate_series(1, 24) i;"
        )),
        // transition-table trigger target + log.
        raw(format!(
            "CREATE TABLE {tt} (k int4 PRIMARY KEY, v int4);"
        )),
        raw(format!(
            "INSERT INTO {tt} SELECT i, i % 20 FROM generate_series(1, 300) i;"
        )),
        raw(format!(
            "CREATE TABLE {l} (id int4 GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, \
             tag text, n int8, s int8);"
        )),
        // The trigger fn RESCANS the transition table: the correlated
        // sub-select over newtab/oldtab re-reads the tuplestore per outer
        // row (ExecReScanNamedTuplestoreScan). Aggregating the rank sum is
        // order-independent, so the logged value is engine-stable.
        raw(format!(
            "CREATE FUNCTION {tf}() RETURNS trigger LANGUAGE plpgsql AS $fz$ \
             BEGIN \
               IF TG_OP = 'INSERT' THEN \
                 INSERT INTO {l} (tag, n, s) SELECT 'ins', count(*), \
                   coalesce(sum((SELECT count(*) FROM newtab n2 WHERE n2.v <= n1.v)), 0) \
                   FROM newtab n1; \
               ELSIF TG_OP = 'UPDATE' THEN \
                 INSERT INTO {l} (tag, n, s) SELECT 'upd', count(*), \
                   coalesce(sum((SELECT count(*) FROM oldtab o2 WHERE o2.v <= n1.v)), 0) \
                   FROM newtab n1; \
               ELSE \
                 INSERT INTO {l} (tag, n, s) SELECT 'del', count(*), \
                   coalesce(sum((SELECT count(*) FROM oldtab o2 WHERE o2.v <= o1.v)), 0) \
                   FROM oldtab o1; \
               END IF; \
               RETURN NULL; \
             END $fz$;"
        )),
        raw(format!(
            "CREATE TRIGGER fz_x2ti{n} AFTER INSERT ON {tt} \
             REFERENCING NEW TABLE AS newtab FOR EACH STATEMENT EXECUTE FUNCTION {tf}();"
        )),
        raw(format!(
            "CREATE TRIGGER fz_x2tu{n} AFTER UPDATE ON {tt} \
             REFERENCING OLD TABLE AS oldtab NEW TABLE AS newtab FOR EACH STATEMENT EXECUTE FUNCTION {tf}();"
        )),
        raw(format!(
            "CREATE TRIGGER fz_x2td{n} AFTER DELETE ON {tt} \
             REFERENCING OLD TABLE AS oldtab FOR EACH STATEMENT EXECUTE FUNCTION {tf}();"
        )),
        raw(format!("ANALYZE {t}, {o}, {tt};")),
    ];
    g.exr2.suites.push(Exr2Suite { n, live: true });
    g.exr2.events.push(DdlEvent {
        table: t,
        pk: "pk".to_string(),
        kind: DdlEventKind::Created,
    });
    stmts
}

fn gen_drop(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.exr2.live_suites();
    if live.is_empty() {
        g.fire("exr2:fallback:create");
        return gen_create(g);
    }
    g.fire("exr2:drop");
    let si = live[g.rng.below_usize(live.len())];
    let n = g.exr2.suites[si].n;
    g.exr2.suites[si].live = false;
    g.exr2.events.push(DdlEvent {
        table: format!("fz_x2{n}"),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![
        raw(format!(
            "DROP TABLE fz_x2{n}, fz_x2o{n}, fz_x2t{n}, fz_x2l{n} CASCADE;"
        )),
        raw(format!("DROP FUNCTION fz_x2tf{n}();")),
    ]
}

// --------------------------------------------------------------- setop ----

/// nodeSetOp retrieval internals at the top of the plan: INTERSECT/EXCEPT
/// [ALL], hashed and sorted strategies. The exr:setop weight under-drives
/// these (the whole node reads GEN-GAP in the reachable queue); this keeps a
/// firm floor on ExecInitSetOp/ExecSetOp/setop_fill_hash_table/
/// setop_retrieve_hash_table/setop_retrieve_sorted/setop_load_group/
/// set_output_count/setop_compare_slots/ExecEndSetOp/ExecGetCommonChildSlotOps.
fn gen_setop(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr2:setop");
    let n = g.exr2.suites[si].n;
    let t = format!("fz_x2{n}");
    let op = pick_str(g, &["INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]);
    let b1 = g.rng.below(9);
    let b2 = g.rng.below(9);
    // NULL-bearing a + duplicate-bearing (a,b) exercise the tuple-match and
    // output-count arms; a two-column target drives setop_compare_slots.
    let sql = format!(
        "SELECT a, b FROM {t} WHERE b >= {b1} \
         {op} \
         SELECT a, b FROM {t} WHERE b <= {} \
         ORDER BY 1 NULLS LAST, 2;",
        6 + b2
    );
    let strat = g.weights.pick(g.rng, &["exr2:so:hash", "exr2:so:sort"]);
    g.fire(strat);
    if strat == "exr2:so:sort" {
        // enable_hashagg off forces SETOP_SORTED (setop_retrieve_sorted /
        // setop_load_group / set_output_count).
        vec![
            raw("BEGIN;".to_string()),
            raw("SET LOCAL enable_hashagg = off;".to_string()),
            raw(sql),
            raw("ROLLBACK;".to_string()),
        ]
    } else {
        vec![raw(sql)]
    }
}

// -------------------------------------------------------- setop rescan ----

/// ExecReScanSetOp: an INTERSECT/EXCEPT on the inner side of a correlated
/// LATERAL. enable_material off strips the Materialize that would otherwise
/// cache the inner and skip the rescan; the correlation on o.g forces a
/// nestloop, so the SetOp is torn down and re-run for every outer row.
fn gen_setop_rescan(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr2:setoprescan");
    let n = g.exr2.suites[si].n;
    let t = format!("fz_x2{n}");
    let o = format!("fz_x2o{n}");
    let op = pick_str(g, &["INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]);
    let hashoff = g.rng.chance(1, 2);
    let mut body = vec![
        raw("SET LOCAL enable_material = off;".to_string()),
        // enable_memoize off too: a Memoize above the inner node would
        // cache results per distinct correlation value and collapse the
        // per-outer-row rescans down to one-per-distinct-key. Off ⇒ the
        // inner node is genuinely ExecReScan'd for every outer row.
        raw("SET LOCAL enable_memoize = off;".to_string()),
        raw("SET LOCAL enable_hashjoin = off;".to_string()),
        raw("SET LOCAL enable_mergejoin = off;".to_string()),
    ];
    if hashoff {
        g.fire("exr2:sor:sort");
        body.push(raw("SET LOCAL enable_hashagg = off;".to_string()));
    } else {
        g.fire("exr2:sor:hash");
    }
    body.push(raw(format!(
        "SELECT o.ok, s.a FROM {o} o JOIN LATERAL ( \
           SELECT a FROM {t} WHERE b = o.g \
           {op} \
           SELECT a FROM {t} WHERE b = o.g + 1 \
         ) s ON true \
         ORDER BY o.ok, s.a NULLS LAST;"
    )));
    rollback(body)
}

// ------------------------------------------------------------ recursive ----

/// ExecReScanRecursiveUnion + nodeWorktablescan rescan: a WITH RECURSIVE
/// UNION (distinct → the visited-row hashtable, build_hash_table) on the
/// inner side of a correlated LATERAL, rescanned per outer row. Plus a
/// plain top-level recursive UNION to floor the non-rescan hashtable arms.
fn gen_recursive(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr2:recursive");
    let n = g.exr2.suites[si].n;
    let o = format!("fz_x2o{n}");
    let shape = g.weights.pick(g.rng, &["exr2:rec:lateral", "exr2:rec:plain"]);
    g.fire(shape);
    if shape == "exr2:rec:plain" {
        // UNION (distinct) drives the recursive-union dedup hashtable; the
        // aggregate projection is order-invariant.
        let cap = 30 + g.rng.below(40);
        vec![raw(format!(
            "WITH RECURSIVE r(d) AS ( \
               SELECT 1 \
               UNION \
               SELECT d + 1 FROM r WHERE d < {cap} \
             ) SELECT count(*), min(d), max(d) FROM r;"
        ))]
    } else {
        // Correlated recursive term (seed = o.g, bound = o.g + k): the
        // recursive union is re-executed for each outer row.
        let k = 4 + g.rng.below(6);
        rollback(vec![
            raw("SET LOCAL enable_material = off;".to_string()),
            raw("SET LOCAL enable_hashjoin = off;".to_string()),
            raw("SET LOCAL enable_mergejoin = off;".to_string()),
            raw(format!(
                "SELECT o.ok, r.d FROM {o} o JOIN LATERAL ( \
                   WITH RECURSIVE r(d) AS ( \
                     SELECT o.g \
                     UNION \
                     SELECT d + 1 FROM r WHERE d < o.g + {k} \
                   ) SELECT d FROM r \
                 ) r ON true \
                 ORDER BY o.ok, r.d;"
            )),
        ])
    }
}

// ------------------------------------------------------------ win rescan ----

/// ExecReScanWindowAgg: a window aggregate on the inner side of a correlated
/// LATERAL — the frame/peer spool and read pointers are torn down and
/// rebuilt for each outer row. LIMIT 1 under a total inner order keeps the
/// projected value deterministic; sum(a) ignores the sparse NULLs.
fn gen_win_rescan(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr2:winrescan");
    let n = g.exr2.suites[si].n;
    let t = format!("fz_x2{n}");
    let o = format!("fz_x2o{n}");
    let off = 2 + g.rng.below(4);
    let frame = pick_str(
        g,
        &[
            "ROWS BETWEEN {off} PRECEDING AND CURRENT ROW",
            "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            "GROUPS BETWEEN {off} PRECEDING AND CURRENT ROW",
        ],
    )
    .replace("{off}", &off.to_string());
    rollback(vec![
        raw("SET LOCAL enable_material = off;".to_string()),
        // enable_memoize off too: a Memoize above the inner node would
        // cache results per distinct correlation value and collapse the
        // per-outer-row rescans down to one-per-distinct-key. Off ⇒ the
        // inner node is genuinely ExecReScan'd for every outer row.
        raw("SET LOCAL enable_memoize = off;".to_string()),
        raw("SET LOCAL enable_hashjoin = off;".to_string()),
        raw("SET LOCAL enable_mergejoin = off;".to_string()),
        raw(format!(
            "SELECT o.ok, w.s FROM {o} o JOIN LATERAL ( \
               SELECT sum(a) OVER (ORDER BY pk {frame}) AS s \
               FROM {t} WHERE b = o.g ORDER BY pk LIMIT 1 \
             ) w ON true \
             ORDER BY o.ok, w.s NULLS LAST;"
        )),
    ])
}

// ------------------------------------------------------------ nt rescan ----

/// ExecReScanNamedTuplestoreScan: an AFTER STATEMENT trigger whose function
/// re-reads the transition table via a correlated sub-select (see the
/// trigger fn in gen_create). Every DML variant fires a trigger that
/// rescans newtab/oldtab per row. Runs inside a rollback bracket; the log
/// projection carries a total order.
fn gen_nt_rescan(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr2:ntrescan");
    let n = g.exr2.suites[si].n;
    let tt = format!("fz_x2t{n}");
    let l = format!("fz_x2l{n}");
    let k = 1 + g.rng.below(200);
    let shape = g.weights.pick(g.rng, &["exr2:nt:ins", "exr2:nt:upd", "exr2:nt:del", "exr2:nt:mix"]);
    g.fire(shape);
    let dml: Vec<StmtKind> = match shape {
        "exr2:nt:ins" => vec![raw(format!(
            "INSERT INTO {tt} SELECT 1000 + i, i % 13 FROM generate_series({k}, {}) i;",
            k + 50
        ))],
        "exr2:nt:upd" => vec![raw(format!(
            "UPDATE {tt} SET v = v + 2 WHERE k BETWEEN {k} AND {};",
            k + 70
        ))],
        "exr2:nt:del" => vec![raw(format!(
            "DELETE FROM {tt} WHERE k BETWEEN {k} AND {};",
            k + 35
        ))],
        _ => vec![
            raw(format!(
                "INSERT INTO {tt} SELECT 2000 + i, i % 7 FROM generate_series(1, 20) i;"
            )),
            raw(format!("UPDATE {tt} SET v = -v WHERE k BETWEEN {k} AND {};", k + 25)),
            raw(format!("DELETE FROM {tt} WHERE k % 19 = {};", k % 19)),
        ],
    };
    // enable_material off keeps the correlated tuplestore sub-select a true
    // rescan rather than a cached Material node.
    let mut body = vec![raw("SET LOCAL enable_material = off;".to_string())];
    body.extend(dml);
    body.push(raw(format!("SELECT tag, n, s FROM {l} ORDER BY id;")));
    rollback(body)
}

// --------------------------------------------------------------- serial ----

/// Plan-serialization arms: debug_print_plan serializes the whole plan tree
/// to the SERVER log (_outSetOp/_outProjectSet/_outWorkTableScan/
/// _outRecursiveUnion/_outWindowAgg); debug_parallel_query pushes nodes
/// under a single-copy Gather so the worker deserializes them (_read*
/// twins). client_min_messages=warning keeps the LOG-level plan dump off the
/// client result set, so there is no diff surface — only the ORDER BY'd
/// query results are compared.
fn gen_serial(g: &mut Gen) -> Vec<StmtKind> {
    let si = need_suite!(g);
    g.fire("exr2:serial");
    let n = g.exr2.suites[si].n;
    let t = format!("fz_x2{n}");
    let o = format!("fz_x2o{n}");
    let toggle = g.weights.pick(g.rng, &["exr2:ser:print", "exr2:ser:parallel"]);
    g.fire(toggle);
    let set = if toggle == "exr2:ser:parallel" {
        "SET LOCAL debug_parallel_query = regress;"
    } else {
        "SET LOCAL debug_print_plan = on;"
    };
    rollback(vec![
        raw("SET LOCAL client_min_messages = warning;".to_string()),
        raw(set.to_string()),
        // SetOp (_outSetOp / _readSetOp)
        raw(format!(
            "SELECT a, b FROM {t} WHERE b < 6 \
             INTERSECT \
             SELECT a, b FROM {t} WHERE b > 2 \
             ORDER BY 1 NULLS LAST, 2;"
        )),
        // ProjectSet: set-returning function in the target list
        // (_outProjectSet / _readProjectSet).
        raw(format!(
            "SELECT ok, generate_series(1, g + 1) AS gs FROM {o} ORDER BY ok, gs;"
        )),
        // RecursiveUnion + WorkTableScan (_outRecursiveUnion /
        // _outWorkTableScan).
        raw(
            "WITH RECURSIVE r(d) AS (SELECT 1 UNION ALL SELECT d + 1 FROM r WHERE d < 12) \
             SELECT d FROM r ORDER BY d;"
                .to_string(),
        ),
        // WindowAgg (_outWindowAgg).
        raw(format!(
            "SELECT pk, sum(a) OVER (ORDER BY pk ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) \
             FROM {t} WHERE pk <= 40 ORDER BY pk;"
        )),
    ])
}
