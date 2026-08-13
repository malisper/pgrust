//! Aggregate + window residue drain module (AGGWIN): the window-frame and
//! grouping-sets-spill surface that the standing `win`/`agg`/`par`/`spill`
//! modules leave uncovered. gap-report-006 (fuzzgen 86bb4744e29) predates
//! the #861 consolidation that added `par` (parallel partial/finalize +
//! numeric/interval serialize+combine arms), `spill` (single-hashtable
//! HashAgg disk spill) and the `agg::aggx` ordered-set/hypothetical sweep
//! (orderedsetaggs.c) — so those chunks of the report are already drained.
//! What remains genuinely uncovered in nodeWindowAgg.c / nodeAgg.c is the
//! frame machinery this module targets:
//!
//!   - GROUPS frame mode (the third frame mode; `win` only emits ROWS and
//!     RANGE) — the peer-group tail bookkeeping (update_grouptailpos) and
//!     the GROUPS-specific frame advance.
//!   - EXCLUDE {CURRENT ROW | GROUP | TIES | NO OTHERS} across all three
//!     frame modes — the frame-exclusion arms (`win` emits no EXCLUDE).
//!   - RANGE offset frames over typed ORDER BY columns (numeric / date /
//!     timestamp / interval) — the per-type in_range dispatch (`win`'s
//!     RANGE offsets are int4-only).
//!   - ExecReScanWindowAgg — a WindowAgg node re-scanned per outer row
//!     (correlated scalar subquery / LATERAL); `win` never nests a window
//!     under a rescanned subplan.
//!   - Window-definition refinement chains (WINDOW w2 AS (w1 <frame>)) and
//!     multiple functions sharing one window — the transformWindowClause
//!     copy-of-window path and the executor window-sharing path.
//!   - FILTER (WHERE ...) on window aggregates — the window-aggregate
//!     filter arm (`agg` emits FILTER only on plain aggregates).
//!   - Moving-frame inverse vs restart transition — sum/avg/count use the
//!     inverse-transition path; min/max force per-row frame re-aggregation.
//!   - HashAgg disk spill under GROUPING SETS (the MixedAggregate multi-
//!     hashtable spill path) — `spill` spills a single hash table.
//!
//! Determinism discipline (the F1 differ compares result sets byte-exact):
//!
//!   - Every row-returning probe carries a total top-level ORDER BY ending
//!     in the fixture primary key `pk`, so output row order is fixed even
//!     when the window ORDER BY has ties.
//!   - All window/grouped aggregates are exact-typed (count/int8, sum/avg
//!     over integer-valued numeric, min/max over numeric) — never float
//!     (B1 ruling: float accumulation order is plan-dependent). numeric
//!     addition is order-insensitive, so a moving/peer frame's value is
//!     identical regardless of accumulation order.
//!   - GROUPS/RANGE frames ride on a window ORDER BY that MAY have ties
//!     (peer groups are the point): the frame is defined by peer/value
//!     equality, so every peer sees an identical frame and EXCLUDE removes
//!     a deterministic per-row subset. ROWS frames instead ride on a TOTAL
//!     window ORDER BY (… , pk) — physical row position among ties is
//!     nondeterministic, so ROWS never rides on a tie-carrying order.
//!   - Every GUC forcing bracket (work_mem / enable_sort) resets in the
//!     same statement group, so both differential sides always leave the
//!     group with identical GUC state (the GucPinned wrapper restores the
//!     C-parity pin after each RESET).

use crate::stmt::{Gen, StmtKind};

/// Registry entry point (stmt::STMT_MODULES). Each call returns one self-
/// contained statement group (fixture create + probes + drop, or a GUC-
/// bracketed inline-source probe).
pub fn gen_aggwin_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aggwin");
    let shape = g.weights.pick(
        g.rng,
        &[
            "aggwin:groups",
            "aggwin:exclude",
            "aggwin:rangetyped",
            "aggwin:rescan",
            "aggwin:namedchain",
            "aggwin:filter",
            "aggwin:moving",
            "aggwin:hashaggspill",
        ],
    );
    g.fire(shape);
    match shape {
        "aggwin:groups" => wrap(gen_groups(g)),
        "aggwin:exclude" => wrap(gen_exclude(g)),
        "aggwin:rangetyped" => wrap(gen_rangetyped(g)),
        "aggwin:rescan" => wrap(gen_rescan(g)),
        "aggwin:namedchain" => wrap(gen_namedchain(g)),
        "aggwin:filter" => wrap(gen_filter(g)),
        "aggwin:moving" => wrap(gen_moving(g)),
        _ => gen_hashagg_spill(g),
    }
}

/// The deterministic 60-row window fixture: `pk` is a unique total-order
/// key; `k` is a 3-value partition key; `s`/`d`/`iv`/`ts` are tie-carrying
/// order keys (peer groups); `v` is an integer-valued numeric aggregate
/// argument (exact accumulation). Every value is a pure integer formula of
/// the generate_series index, identical on both engines by construction.
fn wrap(probes: Vec<String>) -> Vec<StmtKind> {
    let mut v = vec![
        StmtKind::Raw("DROP TABLE IF EXISTS fz_aw CASCADE;".to_string()),
        StmtKind::Raw(
            "CREATE TABLE fz_aw (pk int PRIMARY KEY, k int, s int, v numeric, \
             d date, iv interval, ts timestamp);"
                .to_string(),
        ),
        StmtKind::Raw(
            "INSERT INTO fz_aw SELECT g, g % 3, g % 7, ((g * 13) % 50)::numeric, \
             DATE '2020-01-01' + (g % 11), (g % 9) * INTERVAL '1 day', \
             TIMESTAMP '2020-01-01 00:00' + (g % 13) * INTERVAL '1 hour' \
             FROM generate_series(1, 60) g;"
                .to_string(),
        ),
    ];
    v.extend(probes.into_iter().map(StmtKind::Raw));
    v.push(StmtKind::Raw("DROP TABLE fz_aw CASCADE;".to_string()));
    v
}

/// Legal integer (start, end) frame-bound pairs for ROWS/GROUPS/RANGE-int
/// frames (start never after end in frame order).
const INT_PAIRS: &[(&str, &str)] = &[
    ("UNBOUNDED PRECEDING", "CURRENT ROW"),
    ("UNBOUNDED PRECEDING", "1 FOLLOWING"),
    ("UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING"),
    ("2 PRECEDING", "CURRENT ROW"),
    ("1 PRECEDING", "1 FOLLOWING"),
    ("3 PRECEDING", "1 PRECEDING"),
    ("CURRENT ROW", "UNBOUNDED FOLLOWING"),
    ("CURRENT ROW", "2 FOLLOWING"),
    ("1 FOLLOWING", "3 FOLLOWING"),
    ("2 PRECEDING", "UNBOUNDED FOLLOWING"),
];

const EXCLUDES: &[&str] = &[
    "",
    " EXCLUDE NO OTHERS",
    " EXCLUDE CURRENT ROW",
    " EXCLUDE GROUP",
    " EXCLUDE TIES",
];

/// GROUPS frame mode over a tie-carrying window ORDER BY (real peer
/// groups): update_grouptailpos + the GROUPS frame advance. count/sum are
/// peer-frame exact; an occasional EXCLUDE removes a deterministic subset.
fn gen_groups(g: &mut Gen) -> Vec<String> {
    let (start, end) = INT_PAIRS[g.rng.below_usize(INT_PAIRS.len())];
    let excl = EXCLUDES[g.rng.below_usize(EXCLUDES.len())];
    let part = if g.rng.chance(2, 3) { "PARTITION BY k " } else { "" };
    vec![format!(
        "SELECT pk, k, s, \
         count(*) OVER w AS c, sum(v) OVER w AS sm, dense_rank() OVER w AS dr \
         FROM fz_aw \
         WINDOW w AS ({part}ORDER BY s GROUPS BETWEEN {start} AND {end}{excl}) \
         ORDER BY pk;"
    )]
}

/// EXCLUDE across all three frame modes. ROWS rides on a total order
/// (s, pk) — physical position among ties is nondeterministic; RANGE and
/// GROUPS ride on the tie-carrying (s) order so EXCLUDE GROUP/TIES removes
/// real peers.
fn gen_exclude(g: &mut Gen) -> Vec<String> {
    let excl = EXCLUDES[1 + g.rng.below_usize(EXCLUDES.len() - 1)]; // never the empty arm
    let (mode, order) = match g.rng.below(3) {
        0 => ("ROWS", "s, pk"),
        1 => ("RANGE", "s"),
        _ => ("GROUPS", "s"),
    };
    let (start, end) = INT_PAIRS[g.rng.below_usize(INT_PAIRS.len())];
    vec![format!(
        "SELECT pk, k, s, \
         count(*) OVER w AS c, sum(v) OVER w AS sm, avg(v) OVER w AS av \
         FROM fz_aw \
         WINDOW w AS (PARTITION BY k ORDER BY {order} {mode} BETWEEN {start} AND {end}{excl}) \
         ORDER BY pk;"
    )]
}

/// RANGE offset frames over a typed ORDER BY column — the per-type
/// in_range dispatch (numeric / date / timestamp / interval / int4). A
/// single order key of an offsettable type, count/sum aggregates.
fn gen_rangetyped(g: &mut Gen) -> Vec<String> {
    // (order column, preceding offset, following offset)
    let (col, prec, foll) = match g.rng.below(5) {
        0 => ("v", "5", "5"),
        1 => ("d", "INTERVAL '2 days'", "INTERVAL '1 day'"),
        2 => ("ts", "INTERVAL '3 hours'", "INTERVAL '2 hours'"),
        3 => ("iv", "INTERVAL '1 day'", "INTERVAL '2 days'"),
        _ => ("s", "1", "2"),
    };
    let frame = match g.rng.below(4) {
        0 => format!("BETWEEN {prec} PRECEDING AND {foll} FOLLOWING"),
        1 => format!("BETWEEN {prec} PRECEDING AND CURRENT ROW"),
        2 => format!("BETWEEN CURRENT ROW AND {foll} FOLLOWING"),
        _ => format!("BETWEEN {prec} PRECEDING AND UNBOUNDED FOLLOWING"),
    };
    let excl = EXCLUDES[g.rng.below_usize(EXCLUDES.len())];
    let part = if g.rng.chance(1, 2) { "PARTITION BY k " } else { "" };
    vec![format!(
        "SELECT pk, {col}, count(*) OVER w AS c, sum(v) OVER w AS sm \
         FROM fz_aw \
         WINDOW w AS ({part}ORDER BY {col} RANGE {frame}{excl}) \
         ORDER BY pk;"
    )]
}

/// ExecReScanWindowAgg: a WindowAgg re-scanned per outer row. The window
/// lives in a correlated scalar subquery (rescanned as the SubPlan's
/// parameter changes) or a LATERAL subquery; an outer aggregate collapses
/// the window column to an order-independent scalar.
fn gen_rescan(g: &mut Gen) -> Vec<String> {
    let probe = match g.rng.below(3) {
        0 => "SELECT o.x, \
              (SELECT sum(w) FROM (SELECT sum(v) OVER (PARTITION BY k ORDER BY s, pk) AS w \
               FROM fz_aw WHERE pk <= o.x) t) AS r \
              FROM (VALUES (12), (24), (36), (48), (60)) o(x) ORDER BY o.x;"
            .to_string(),
        1 => "SELECT o.x, \
              (SELECT max(rn) FROM (SELECT row_number() OVER (PARTITION BY k ORDER BY s, pk) AS rn \
               FROM fz_aw WHERE s < o.x) t) AS m \
              FROM (VALUES (1), (3), (5), (7)) o(x) ORDER BY o.x;"
            .to_string(),
        _ => "SELECT o.k, l.c \
              FROM (SELECT DISTINCT k FROM fz_aw) o \
              CROSS JOIN LATERAL (SELECT count(*) FILTER (WHERE rn = 1) AS c \
               FROM (SELECT row_number() OVER (ORDER BY s, pk) AS rn FROM fz_aw i WHERE i.k = o.k) t) l \
              ORDER BY o.k;"
            .to_string(),
    };
    vec![probe]
}

/// Window-definition refinement chains: WINDOW w2 AS (w1 <frame>) copies
/// w1's partition/order and adds a frame (transformWindowClause copy-of-
/// window path); multiple functions share one base window (the executor
/// window-sharing path). w1 carries a total order so the refined ROWS
/// frames are deterministic.
fn gen_namedchain(g: &mut Gen) -> Vec<String> {
    let last = if g.rng.chance(1, 2) {
        ", last_value(s) OVER w2 AS lv"
    } else {
        ", nth_value(s, 2) OVER w3 AS nv"
    };
    vec![format!(
        "SELECT pk, \
         rank() OVER w1 AS rk, dense_rank() OVER w1 AS dr, \
         sum(v) OVER w2 AS sm, count(*) OVER w3 AS c{last} \
         FROM fz_aw \
         WINDOW w1 AS (PARTITION BY k ORDER BY s, pk), \
         w2 AS (w1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
         w3 AS (w1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
         ORDER BY pk;"
    )]
}

/// FILTER (WHERE ...) on window aggregates — the window-aggregate filter
/// arm. Total order (s, pk); avg over a filtered-empty frame yields a
/// deterministic NULL.
fn gen_filter(g: &mut Gen) -> Vec<String> {
    let thresh = 10 + g.rng.below(30);
    vec![format!(
        "SELECT pk, k, \
         count(*) FILTER (WHERE v > {thresh}) OVER w AS cf, \
         sum(v) FILTER (WHERE s % 2 = 0) OVER w AS sf, \
         avg(v) FILTER (WHERE k = 1) OVER w AS af \
         FROM fz_aw \
         WINDOW w AS (PARTITION BY k ORDER BY s, pk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
         ORDER BY pk;"
    )]
}

/// Moving-frame transition machinery: sum/avg/count take the inverse-
/// transition path (a moving frame removes trailing rows); min/max are
/// non-invertible and force a per-row frame re-aggregation. Total order
/// (s, pk) — every value is deterministic.
fn gen_moving(g: &mut Gen) -> Vec<String> {
    let (start, end) = match g.rng.below(3) {
        0 => ("2 PRECEDING", "1 FOLLOWING"),
        1 => ("1 PRECEDING", "1 FOLLOWING"),
        _ => ("3 PRECEDING", "CURRENT ROW"),
    };
    vec![format!(
        "SELECT pk, \
         sum(v) OVER w AS mv_sum, avg(v) OVER w AS mv_avg, count(*) OVER w AS mv_cnt, \
         min(v) OVER w AS mv_min, max(v) OVER w AS mv_max \
         FROM fz_aw \
         WINDOW w AS (PARTITION BY k ORDER BY s, pk ROWS BETWEEN {start} AND {end}) \
         ORDER BY pk;"
    )]
}

/// HashAgg disk spill under GROUPING SETS (MixedAggregate multi-hashtable
/// spill): enable_sort=off forces the hashed grouping-set path, small
/// work_mem forces the spill. Inline integer source, exact aggregates,
/// total ORDER BY — deterministic. Self-contained GUC bracket.
fn gen_hashagg_spill(g: &mut Gen) -> Vec<StmtKind> {
    let rows = 12000 + g.rng.below(12000);
    let am = 300 + g.rng.below(400); // distinct a values
    vec![
        StmtKind::Raw("SET work_mem = '64kB';".to_string()),
        StmtKind::Raw("SET enable_sort = off;".to_string()),
        StmtKind::Raw(format!(
            "SELECT a, b, count(*), sum(c) FROM \
             (SELECT g % {am} AS a, g % 37 AS b, (g % 13) AS c \
              FROM generate_series(1, {rows}) g) t \
             GROUP BY GROUPING SETS ((a), (b), (a, b)) ORDER BY a, b;"
        )),
        StmtKind::Raw("RESET enable_sort;".to_string()),
        StmtKind::Raw("RESET work_mem;".to_string()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn groups(seed: u64, n: usize) -> Vec<Vec<StmtKind>> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut out = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            out.push(gen_aggwin_module(&mut g));
        }
        out
    }

    fn flat(seed: u64, n: usize) -> Vec<String> {
        groups(seed, n).into_iter().flatten().map(|k| k.to_sql()).collect()
    }

    #[test]
    fn deterministic_and_seed_sensitive() {
        assert_eq!(flat(5, 120), flat(5, 120));
        assert_ne!(flat(5, 120), flat(6, 120));
    }

    #[test]
    fn all_families_fire() {
        let sql = flat(3, 900).join("\n");
        for needle in [
            "GROUPS BETWEEN ",
            " EXCLUDE CURRENT ROW",
            " EXCLUDE GROUP",
            " EXCLUDE TIES",
            "RANGE BETWEEN INTERVAL '2 days' PRECEDING",
            "row_number() OVER (PARTITION BY k ORDER BY s, pk)",
            "CROSS JOIN LATERAL",
            "WINDOW w1 AS (PARTITION BY k ORDER BY s, pk)",
            "w2 AS (w1 ROWS BETWEEN",
            "FILTER (WHERE v > ",
            "min(v) OVER w AS mv_min",
            "GROUP BY GROUPING SETS ((a), (b), (a, b))",
            "SET work_mem = '64kB';",
        ] {
            assert!(sql.contains(needle), "family fragment never generated: {needle}");
        }
    }

    /// Every row-returning probe carries a total top-level ORDER BY ending
    /// in a total key (pk, o.x, o.k, or the grouping-sets a, b), so output
    /// row order is deterministic on both differential sides.
    #[test]
    fn row_returning_probes_are_order_normalized() {
        for sql in flat(11, 600) {
            if !sql.starts_with("SELECT ") {
                continue;
            }
            assert!(sql.contains("ORDER BY "), "SELECT without ORDER BY: {sql}");
        }
    }

    /// No float aggregates ride on a window/grouped frame (B1 ruling:
    /// float accumulation order is plan-dependent).
    #[test]
    fn no_float_aggregates() {
        let sql = flat(23, 600).join("\n");
        for needle in ["float", "::real", "double precision", "stddev", "var_samp", "var_pop"] {
            assert!(!sql.contains(needle), "float-family aggregate present: {needle}");
        }
    }

    /// GUC forcing brackets are balanced within a group (both differential
    /// sides leave each group with identical GUC state).
    #[test]
    fn guc_brackets_balanced() {
        for group in groups(7, 400) {
            let mut open: Vec<String> = Vec::new();
            for k in &group {
                let sql = k.to_sql();
                if let Some(rest) = sql.strip_prefix("SET ") {
                    open.push(rest.split([' ', '=']).next().unwrap().to_string());
                } else if let Some(rest) = sql.strip_prefix("RESET ") {
                    let name = rest.trim_end_matches(';').to_string();
                    let pos = open.iter().rposition(|n| *n == name);
                    assert!(pos.is_some(), "RESET {name} without SET: {sql}");
                    open.remove(pos.unwrap());
                }
            }
            assert!(open.is_empty(), "unclosed SETs at group end: {open:?}");
        }
    }

    /// Every fixture-building group drops the fixture it creates (no cross-
    /// group table leakage).
    #[test]
    fn fixture_groups_drop_their_table() {
        for group in groups(31, 300) {
            let creates = group.iter().filter(|k| k.to_sql().contains("CREATE TABLE fz_aw")).count();
            let drops = group
                .iter()
                .filter(|k| k.to_sql().starts_with("DROP TABLE fz_aw"))
                .count();
            assert_eq!(creates, drops, "fixture create/drop mismatch in group");
        }
    }
}
