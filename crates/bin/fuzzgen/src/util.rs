//! Utility statement module (E1): the stateless-ish half of the DDL/utility
//! gap (a sibling lane owns CREATE/ALTER/DROP). 19 of gap-report-001's
//! top-40 gaps are DDL/utility; this module takes the paths that need no
//! persistent catalog state:
//!
//!   - SET/RESET over a curated safe GUC list: session-local, deterministic
//!     GUCs only. Both sides always receive the identical SET, so even
//!     output-shaping GUCs (extra_float_digits DOES change float text)
//!     stay symmetric — verified by the C-vs-C null diff. RESET ALL fires
//!     periodically so streams stay comparable over long sessions.
//!   - SHOW: always as a SET-then-SHOW pair for the same GUC, so the shown
//!     value is the one this stream pinned — never an install-default
//!     (pgrust deliberately retunes some memory defaults; comparing those
//!     would be a config diff, not a conformance diff). SHOW ALL is
//!     skipped entirely (version-banner GUCs).
//!   - DISCARD PLANS / DISCARD SEQUENCES (never DISCARD ALL — it would
//!     deallocate prepared statements across group boundaries).
//!   - VACUUM <table> (plain: deterministic empty output) and
//!     ANALYZE <table> (stats sampling reads every row on the small
//!     fixture tables, so both sides compute identical stats — verified
//!     by the null diff). CHECKPOINT at very low weight.
//!   - COMMENT ON TABLE/COLUMN (utility deparse + object-address paths
//!     from the gap report), including IS NULL removal.
//!   - PREPARE/EXECUTE/DEALLOCATE brackets (plancache — the F2b report
//!     showed plancache functions flapping): the many-EXECUTE variant runs
//!     6 executions to cross the custom-plan -> generic-plan flip at 5.
//!     Groups are self-contained (always DEALLOCATE at the end), so the
//!     fixed statement name can never collide across groups.
//!   - `cmm:` session co-draw (sitediff plan §6 conf row / §9 M0; lane
//!     L0.4): `SET client_min_messages = <notice|log|debug1|debug2>` as a
//!     SESSION production — deliberately NOT bracketed by a RESET, so every
//!     later statement of the stream (any module) runs under that level
//!     until the next `util:reset_all` / cfgm `RESET ALL`. `cmm:log`
//!     co-draws `SET log_statement = 'all'` and
//!     `SET log_min_duration_statement = 0` (postgres-1/-2). Sound on the
//!     differential bar: both sides receive the identical SET, so the
//!     NOTICE/LOG/DEBUG messages it unmasks on the wire are exactly the
//!     O-NOTICE plane the plan wants compared (postgres-8 needs debug1/2,
//!     which the cfgm bracket restores before the next statement).
//!
//! Everything here is either a Command-tag statement (SET/RESET/VACUUM/...:
//! compared by tag+count) or a deterministic rowset (SHOW, EXECUTE), so the
//! normal differ applies unchanged.

use crate::stmt::{gen_expr_stmt, Gen, StmtKind};

/// Curated safe GUC list: session-local, deterministic, absorbable by the
/// differ. Think before adding an entry — the bar is "identical SET on
/// both sides yields identical downstream output", null-diff-verified.
/// extra_float_digits is the deliberate edge: it changes float output
/// text, which is safe ONLY because both sides get the identical SET.
const SAFE_GUCS: &[(&str, &[&str])] = &[
    ("work_mem", &["'64kB'", "'256kB'", "'1MB'", "'16MB'"]),
    ("extra_float_digits", &["0", "1", "2", "3", "-3"]),
    ("enable_seqscan", &["on", "off"]),
    ("enable_indexscan", &["on", "off"]),
    ("enable_bitmapscan", &["on", "off"]),
    ("enable_hashjoin", &["on", "off"]),
    ("enable_mergejoin", &["on", "off"]),
    ("enable_nestloop", &["on", "off"]),
    ("enable_sort", &["on", "off"]),
    ("enable_hashagg", &["on", "off"]),
    ("enable_material", &["on", "off"]),
    ("enable_incremental_sort", &["on", "off"]),
    // Q3 nodes-serial weave: the debug_print GUCs dump node trees to the
    // SERVER LOG only (client output unchanged — hand-verified on both
    // engines), so a random SET here multiplies outfuncs coverage over
    // every statement the stream produces until the next RESET [ALL].
    // compute_query_id routes every statement through queryjumblefuncs;
    // its only client-visible surface (EXPLAIN VERBOSE query identifier)
    // is symmetric on both sides and part of the differential bar.
    ("debug_print_parse", &["on", "off"]),
    ("debug_print_rewritten", &["on", "off"]),
    ("debug_print_plan", &["on", "off"]),
    ("compute_query_id", &["on", "off"]),
];

const SHAPES: &[&str] = &[
    "util:set",
    "util:reset",
    "util:reset_all",
    "util:show",
    "util:discard:plans",
    "util:discard:sequences",
    "util:vacuum",
    "util:analyze",
    "util:checkpoint",
    "util:comment:table",
    "util:comment:column",
    "util:prepare",
    "util:sysview",
    "util:cmm",
];

/// `cmm:` levels (sitediff plan §6): the client_min_messages value is the
/// production suffix; `cmm:log` additionally co-draws the two statement-
/// logging GUCs. Each entry is a weight in `weights::PROD_WEIGHTS`.
pub const CMM_LEVELS: &[&str] = &["cmm:notice", "cmm:log", "cmm:debug1", "cmm:debug2"];

/// The `cmm:` session co-draw (see the module docs). Shared with the cfgm
/// module's `cfgm:cmm` shape so the production is reachable from both
/// modules; fires `cmm` and `cmm:<level>`.
pub fn gen_cmm(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("cmm");
    let level = g.weights.pick(g.rng, CMM_LEVELS);
    g.fire(level);
    let value = &level["cmm:".len()..];
    let mut out = vec![StmtKind::Raw(format!("SET client_min_messages = {value};"))];
    if value == "log" {
        out.push(StmtKind::Raw("SET log_statement = 'all';".to_string()));
        out.push(StmtKind::Raw("SET log_min_duration_statement = 0;".to_string()));
    }
    out
}

/// True when `sql` is the first statement of a `cmm:` group (used by the
/// module tests to exempt the one deliberately unbracketed SET).
pub fn is_cmm_set(sql: &str) -> bool {
    sql.starts_with("SET client_min_messages = ")
}

/// System-view probes (X1 gap: pg_lock_status rank 14, pg_stat_get_activity
/// rank 19): SELECTs over pg_locks / pg_stat_activity-shaped views with
/// every row-content-volatile column (pids, oids-of-transients, timings,
/// counters) projected AWAY — only invariantly-stable predicates and
/// columns survive, so the output is deterministic AND identical across
/// engines. The bar for adding an entry is the SAFE_GUCS bar: identical
/// statement on both sides yields identical output on a healthy engine,
/// hand-verified against C Postgres and pgrust before landing.
///
/// Rig note (X1): the pg_stat_*_tables views plan deep enough that a DEBUG
/// pgrust build exhausts a 2MB max_stack_depth and raises 54001 where the
/// C build answers normally. That is the debug-build stack ceiling, not a
/// divergence — the standing rig recipe runs the pgrust server with
/// `-c max_stack_depth=60000` under `ulimit -s 65520` (as every
/// scripts/*-e2e.sh does), and under it these probes agree exactly. A
/// diffrunner leg that reports a 54001 flood is a misconfigured rig — except
/// under the sitediff `unpinned-stack` cell (scripts/sitediff-cell.sh), which
/// drops the pin on purpose to observe that band.
const SYSVIEW_PROBES: &[(&str, &str)] = &[
    // A session scanning pg_locks always holds at least its own lock.
    ("locks_any", "SELECT count(*) > 0 FROM pg_locks;"),
    // FP-10 (round-10): pg_locks is CLUSTER-global — a concurrent driver
    // batch's ungranted lock appears on one side only, so the raw count
    // is not cross-engine comparable (run f13de995...-59-13 saw A=[0]
    // with an unmatched B row). Existence-only projection, per the
    // round-9/10 hba/progress-view precedent; the classifier's
    // instance-config net covers gramwalk-derived raw references.
    ("locks_ungranted", "SELECT count(*) >= 0 FROM pg_locks WHERE NOT granted;"),
    // The scan itself guarantees a relation-lock row; DISTINCT + the
    // equality filter pins the output to exactly one stable value.
    (
        "locks_locktype",
        "SELECT DISTINCT locktype FROM pg_locks WHERE locktype = 'relation';",
    ),
    ("locks_self", "SELECT count(*) > 0 FROM pg_locks WHERE pid = pg_backend_pid();"),
    // Own-backend rows of pg_stat_activity, stable columns only.
    (
        "activity_state",
        "SELECT state FROM pg_stat_activity WHERE pid = pg_backend_pid();",
    ),
    (
        "activity_db",
        "SELECT datname FROM pg_stat_activity WHERE pid = pg_backend_pid();",
    ),
    ("activity_any", "SELECT count(*) > 0 FROM pg_stat_activity;"),
    (
        "statdb_self",
        "SELECT count(*) = 1 FROM pg_stat_database WHERE datname = current_database();",
    ),
];

fn pick_guc(g: &mut Gen) -> (&'static str, &'static str) {
    let (name, vals) = SAFE_GUCS[g.rng.below_usize(SAFE_GUCS.len())];
    (name, vals[g.rng.below_usize(vals.len())])
}

fn comment_text(g: &mut Gen) -> String {
    if g.weights.pick(g.rng, &["util:comment:text", "util:comment:null"])
        == "util:comment:null"
    {
        g.fire("util:comment:null");
        "NULL".to_string()
    } else {
        g.fire("util:comment:text");
        format!("'e1 note {}'", g.rng.below(4))
    }
}

/// Registry entry point (stmt::STMT_MODULES): one utility statement group
/// (usually one statement; SHOW pairs with its SET; PREPARE emits a whole
/// PREPARE/EXECUTE.../DEALLOCATE bracket).
pub fn gen_util_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("util");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    let raw = |s: String| StmtKind::Raw(s);
    match shape {
        "util:set" => {
            let (name, val) = pick_guc(g);
            g.fire2("util:set:", name);
            vec![raw(format!("SET {} TO {};", name, val))]
        }
        "util:reset" => {
            let (name, _) = pick_guc(g);
            vec![raw(format!("RESET {};", name))]
        }
        "util:reset_all" => vec![raw("RESET ALL;".to_string())],
        "util:show" => {
            // SET-then-SHOW pair: the shown value is stream-pinned, never
            // an install default (see module docs).
            let (name, val) = pick_guc(g);
            vec![
                raw(format!("SET {} TO {};", name, val)),
                raw(format!("SHOW {};", name)),
            ]
        }
        "util:discard:plans" => vec![raw("DISCARD PLANS;".to_string())],
        "util:discard:sequences" => vec![raw("DISCARD SEQUENCES;".to_string())],
        "util:vacuum" => {
            let t = g.pick_table().name.clone();
            vec![raw(format!("VACUUM {};", t))]
        }
        "util:analyze" => {
            let t = g.pick_table().name.clone();
            vec![raw(format!("ANALYZE {};", t))]
        }
        "util:checkpoint" => vec![raw("CHECKPOINT;".to_string())],
        "util:sysview" => {
            // One extra dynamic shape rides the curated list: per-table
            // pg_stat_all_tables presence. The predicate resolves through
            // the pg_class join, so it tracks catalog existence (stable and
            // identical on both sides) rather than stats-collector state.
            let n = SYSVIEW_PROBES.len();
            let i = g.rng.below_usize(n + 1);
            if i == n {
                g.fire2("util:sysview:", "stat_table");
                let t = g.pick_table().name.clone();
                vec![raw(format!(
                    "SELECT count(*) > 0 FROM pg_stat_all_tables WHERE relname = '{}';",
                    t
                ))]
            } else {
                let (name, sql) = SYSVIEW_PROBES[i];
                g.fire2("util:sysview:", name);
                vec![raw(sql.to_string())]
            }
        }
        "util:cmm" => gen_cmm(g),
        "util:comment:table" => {
            let t = g.pick_table().name.clone();
            let txt = comment_text(g);
            vec![raw(format!("COMMENT ON TABLE {} IS {};", t, txt))]
        }
        "util:comment:column" => {
            let table = g.pick_table();
            let t = table.name.clone();
            let c = table.columns[g.rng.below_usize(table.columns.len())].name.clone();
            let txt = comment_text(g);
            vec![raw(format!("COMMENT ON COLUMN {}.{} IS {};", t, c, txt))]
        }
        "util:prepare" => {
            let select = gen_expr_stmt(g);
            let executes = if g
                .weights
                .pick(g.rng, &["util:prepare:once", "util:prepare:many"])
                == "util:prepare:many"
            {
                g.fire("util:prepare:many");
                6 // crosses the plancache custom->generic flip at 5
            } else {
                g.fire("util:prepare:once");
                1
            };
            let mut out = vec![raw(format!("PREPARE fzp AS {}", select.to_sql()))];
            for _ in 0..executes {
                out.push(raw("EXECUTE fzp;".to_string()));
            }
            out.push(raw("DEALLOCATE fzp;".to_string()));
            out
        }
        other => unreachable!("unknown util shape {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize, w: &WeightTable) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut rng = Rng::new(seed);
        let mut groups = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, w, &mut prods, 3);
            groups.push(
                gen_util_module(&mut g).iter().map(|s| s.to_sql()).collect::<Vec<_>>(),
            );
            prods_all.extend(prods);
        }
        (groups, prods_all)
    }

    #[test]
    fn shapes_and_invariants() {
        // 2000 groups: util:checkpoint weighs 0.1 of ~20, so a smaller
        // sample can legitimately miss it.
        let (groups, prods) = gen_groups(0xE1u64, 2000, &WeightTable::defaults());
        for group in &groups {
            for sql in group {
                assert!(sql.ends_with(';'), "{sql}");
                assert!(!sql.contains('\n'), "{sql}");
            }
            let first = &group[0];
            if is_cmm_set(first) {
                // cmm: session co-draw — one SET (three under cmm:log), no
                // RESET, only the four levels.
                let level = first
                    .strip_prefix("SET client_min_messages = ")
                    .and_then(|r| r.strip_suffix(';'))
                    .unwrap();
                assert!(
                    CMM_LEVELS.iter().any(|l| &l["cmm:".len()..] == level),
                    "unknown cmm level in {group:?}"
                );
                if level == "log" {
                    assert_eq!(
                        &group[1..],
                        &[
                            "SET log_statement = 'all';".to_string(),
                            "SET log_min_duration_statement = 0;".to_string()
                        ]
                    );
                } else {
                    assert_eq!(group.len(), 1, "{group:?}");
                }
                continue;
            }
            if first.starts_with("PREPARE") {
                // Bracket: PREPARE, then only EXECUTEs, then DEALLOCATE.
                assert_eq!(group.last().unwrap(), "DEALLOCATE fzp;");
                assert!(group.len() >= 3, "{group:?}");
                for sql in &group[1..group.len() - 1] {
                    assert_eq!(sql, "EXECUTE fzp;");
                }
            } else if group.len() == 2 {
                // The only other multi-statement group is SET-then-SHOW
                // over the same GUC.
                assert!(first.starts_with("SET "), "{group:?}");
                let guc = first.strip_prefix("SET ").unwrap().split(' ').next().unwrap();
                assert_eq!(group[1], format!("SHOW {};", guc));
            } else {
                assert_eq!(group.len(), 1, "{group:?}");
            }
            // Curated list only: any SET names a safe GUC.
            for sql in group {
                if let Some(rest) = sql.strip_prefix("SET ") {
                    let name = rest.split(' ').next().unwrap();
                    assert!(
                        SAFE_GUCS.iter().any(|(n, _)| *n == name),
                        "un-curated GUC {name}"
                    );
                }
                // The differ-hostile statements never appear.
                assert!(!sql.starts_with("SHOW ALL"), "{sql}");
                assert!(!sql.starts_with("DISCARD ALL"), "{sql}");
                assert!(!sql.contains("SHOW server_version"), "{sql}");
            }
        }
        for p in SHAPES {
            assert!(prods.iter().any(|q| q == p), "shape {p} never fired");
        }
        for p in ["util", "util:prepare:once", "util:prepare:many", "util:comment:text",
                  "util:comment:null", "cmm"] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    /// `cmm:` co-draw under an exclusive weight: every level fires, the
    /// log level always carries its two statement-logging co-draws, and no
    /// group ever RESETs the level (it is a session production).
    #[test]
    fn cmm_codraw_levels_and_log_companions() {
        let w = WeightTable::parse(
            "util:set=0,util:reset=0,util:reset_all=0,util:show=0,util:discard:plans=0,\
             util:discard:sequences=0,util:vacuum=0,util:analyze=0,util:checkpoint=0,\
             util:comment:table=0,util:comment:column=0,util:sysview=0,util:prepare=0,util:cmm=1",
        )
        .unwrap();
        let (groups, prods) = gen_groups(0xC33, 200, &w);
        for group in &groups {
            assert!(is_cmm_set(&group[0]), "{group:?}");
            assert!(!group.iter().any(|s| s.starts_with("RESET")), "{group:?}");
            let is_log = group[0] == "SET client_min_messages = log;";
            assert_eq!(group.len(), if is_log { 3 } else { 1 }, "{group:?}");
        }
        for level in CMM_LEVELS {
            assert!(prods.iter().any(|p| p == level), "{level} never fired");
        }
    }

    /// Reachability in the M0 cells (plan §9): a default-weight, all-modules
    /// session stream contains the `cmm` production, and the cmm:log
    /// companions ride along in the same group. Same seed + same weights =
    /// same stream, so the assertion is deterministic.
    #[test]
    fn cmm_codraw_reachable_in_default_session() {
        use crate::session::{run_session, SessionConfig};
        use crate::toggles::ToggleVector;
        let cat = FixtureCatalog.load_catalog().unwrap();
        let cfg = SessionConfig {
            seed: 0xC300,
            toggles: ToggleVector::all_on(),
            weights: WeightTable::defaults(),
            budget: 40_000,
            max_depth: 3,
        };
        let stmts = run_session(&cfg, &cat);
        let cmm: Vec<_> = stmts
            .iter()
            .filter(|s| s.productions.iter().any(|p| p == "cmm"))
            .collect();
        assert!(!cmm.is_empty(), "cmm never fired in a 40k default all-on stream");
        assert!(cmm.iter().any(|s| is_cmm_set(&s.sql)), "cmm group without its SET");
        for s in &cmm {
            if s.productions.iter().any(|p| p == "cmm:log") {
                let sql = &s.sql;
                assert!(
                    sql == "SET client_min_messages = log;"
                        || sql == "SET log_statement = 'all';"
                        || sql == "SET log_min_duration_statement = 0;",
                    "unexpected statement in a cmm:log group: {sql}"
                );
            }
        }
    }

    #[test]
    fn prepare_bracket_always_deallocates() {
        let w = WeightTable::parse(
            "util:set=0,util:reset=0,util:reset_all=0,util:show=0,util:discard:plans=0,\
             util:discard:sequences=0,util:vacuum=0,util:analyze=0,util:checkpoint=0,\
             util:comment:table=0,util:comment:column=0,util:sysview=0,util:cmm=0,util:prepare=1",
        )
        .unwrap();
        let (groups, _) = gen_groups(9, 100, &w);
        let mut saw_many = false;
        for group in &groups {
            assert!(group[0].starts_with("PREPARE fzp AS SELECT"), "{group:?}");
            assert_eq!(group.last().unwrap(), "DEALLOCATE fzp;");
            let n_exec = group.iter().filter(|s| *s == "EXECUTE fzp;").count();
            assert!(n_exec == 1 || n_exec == 6, "{group:?}");
            saw_many |= n_exec == 6;
        }
        assert!(saw_many, "many-EXECUTE variant never fired");
    }

    /// System-view probes (X1): only curated statements ever reach the
    /// stream, every one of them projects away the volatile columns (no
    /// bare pid/oid/xid/timing/counter column ever appears in an output
    /// list), and the curated list itself stays inside the view family it
    /// documents.
    #[test]
    fn sysview_probes_are_curated_and_volatility_free() {
        let w = WeightTable::parse(
            "util:set=0,util:reset=0,util:reset_all=0,util:show=0,util:discard:plans=0,\
             util:discard:sequences=0,util:vacuum=0,util:analyze=0,util:checkpoint=0,\
             util:comment:table=0,util:comment:column=0,util:prepare=0,util:cmm=0,util:sysview=1",
        )
        .unwrap();
        let (groups, prods) = gen_groups(0x51E7, 400, &w);
        let mut seen: Vec<String> = Vec::new();
        for group in &groups {
            assert_eq!(group.len(), 1, "{group:?}");
            let sql = &group[0];
            assert!(sql.starts_with("SELECT "), "{sql}");
            assert!(sql.ends_with(';') && !sql.contains('\n'), "{sql}");
            // Curated: either a listed probe or the per-table stat shape.
            let curated = SYSVIEW_PROBES.iter().any(|(_, s)| s == sql)
                || (sql.starts_with("SELECT count(*) > 0 FROM pg_stat_all_tables WHERE relname = '")
                    && sql.ends_with("';"));
            assert!(curated, "un-curated system-view probe: {sql}");
            // Volatile columns are never projected: the select list (up to
            // FROM) may only hold count(*)/boolean tests or the stable
            // columns state/datname/locktype.
            let list = sql["SELECT ".len()..sql.find(" FROM ").unwrap()].to_string();
            for volatile in [
                "pid", "backend_start", "xact_start", "query_start", "state_change",
                "backend_xid", "backend_xmin", "wait_event", "query", "transactionid",
                "virtualtransaction", "objid", "classid", "relation", "n_tup",
                "seq_scan", "last_vacuum", "blks_", "tup_", "xact_commit",
            ] {
                assert!(
                    !list.contains(volatile),
                    "volatile column {volatile} projected: {sql}"
                );
            }
            if !seen.contains(sql) {
                seen.push(sql.clone());
            }
        }
        // Every curated entry fires under an exclusive weight.
        for (name, _) in SYSVIEW_PROBES {
            assert!(
                prods.iter().any(|p| p == &format!("util:sysview:{name}")),
                "probe {name} never fired"
            );
        }
        assert!(
            prods.iter().any(|p| p == "util:sysview:stat_table"),
            "per-table stat probe never fired"
        );
        // FP-10 (round-10): pg_locks / pg_stat_activity row COUNTS are
        // cluster-global live state (a concurrent batch's lock or
        // session lands on one side only), so a bare `count(*)` select
        // list over them is never cross-engine comparable — every count
        // must be folded into a boolean existence shape.
        for (name, s) in SYSVIEW_PROBES {
            if s.contains("pg_locks") || s.contains("pg_stat_activity") {
                let list = &s["SELECT ".len()..s.find(" FROM ").unwrap()];
                assert!(
                    list != "count(*)",
                    "probe {name} projects a raw cluster-global count: {s}"
                );
            }
        }
        // Only pg_locks / pg_stat_* views are touched.
        for sql in &seen {
            let from = &sql[sql.find(" FROM ").unwrap() + 6..];
            let view = from.split([' ', ';']).next().unwrap();
            assert!(
                view == "pg_locks" || view.starts_with("pg_stat_"),
                "probe outside the documented view family: {sql}"
            );
        }
    }

    #[test]
    fn util_is_deterministic() {
        let w = WeightTable::defaults();
        let (a, _) = gen_groups(5, 80, &w);
        let (b, _) = gen_groups(5, 80, &w);
        assert_eq!(a, b);
        let (c, _) = gen_groups(6, 80, &w);
        assert_ne!(a, c);
    }
}
