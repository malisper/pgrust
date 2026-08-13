//! Numeric deep-arithmetic + integer-statistical drain module (NUMERIC
//! lane): the differential SQL drain of the numeric.c residue the existing
//! numeric coverage leaves behind. This module is deliberately scoped to
//! the two clusters no other module reaches (verified against current
//! main), so it adds coverage rather than re-treading numx/xnum/par/exr:
//!
//!   * INTEGER statistical aggregates — var_pop/var_samp/stddev_pop/
//!     stddev_samp over int2/int4/int8. No module aggregates variance over
//!     integer columns (par aggregates var_samp only over `numeric`; numx
//!     var_pop only over numeric/int8-sum), so the int128 "poly" path is
//!     otherwise dark: int2_accum/int4_accum/int8_accum(+_inv),
//!     numeric_poly_sum, numeric_poly_var_pop/var_samp,
//!     numeric_poly_stddev_pop/stddev_samp, and — under forced parallelism
//!     — numeric_poly_combine/numeric_poly_serialize/numeric_poly_
//!     deserialize. Exercised three ways: serial grouped, parallel
//!     (partial+finalize combine), and moving-window (both-edge inverse
//!     transitions -> the int128 discard arm).
//!
//!   * EXTREME PRECISION — the existing suite caps numeric literals near
//!     50 digits / 1e130. This shape drives 300-1500-digit operands through
//!     mul_var (Karatsuba threshold), div_var (large rscale), sqrt_var and
//!     ln_var big paths, plus mod and high-scale rounding.
//!
//! Comparison law: NUMERIC IS EXACT. Every result is compared byte-exact
//! as ::text; integer variance/stddev is a numeric (decimal) value and is
//! accumulation-order-independent, so the parallel and moving arms stay
//! byte-identical regardless of worker split or frame edge order. Any
//! divergence here is a real HIGH-severity bug, never float noise. Error
//! arms are matched on SQLSTATE by diff::classify.
//!
//! Stateless: each pick is a self-contained group. The istat arms create
//! and drop a `fz_num_is` fixture in-group (earm/numx discipline); the
//! xprec arms are single literal statements. Every emitted statement is a
//! single line terminated with `;` (the all-modules scope test rejects
//! embedded newlines), so multi-statement groups push each line as its own
//! Raw entry.

use crate::stmt::{Gen, StmtKind};

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

const SHAPES: &[&str] = &["numeric:istat", "numeric:xprec"];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_numeric_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numeric");
    match g.weights.pick(g.rng, SHAPES) {
        "numeric:istat" => gen_istat(g),
        _ => gen_xprec(g),
    }
}

// --------------------------------------------------------------- istat ----

/// Integer statistical aggregates: the int128 "poly" transition cluster.
///
/// Fixture columns span the three integer widths at magnitudes chosen so
/// the int128 accumulators (sumX, sumX2) never overflow — results stay
/// exact and identical whatever the accumulation order, which is what makes
/// the parallel and moving arms byte-comparable.
fn gen_istat(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numeric:istat");
    let shape = g.weights.pick(g.rng, &["numeric:is:serial", "numeric:is:par", "numeric:is:move"]);
    g.fire(shape);
    let t = "fz_num_is";
    let rows = 2000 + g.rng.below(3000);
    let groups = 3 + g.rng.below(6);
    // int8 magnitudes bounded so sumX2 stays well inside int128.
    let w_scale = pick_str(g, &["8675309", "104729", "2147483", "999983"]);
    let mut stmts: Vec<StmtKind> = Vec::new();
    stmts.push(raw(format!(
        "CREATE TABLE {t} (i int4 PRIMARY KEY, k int4, s int2, m int4, w int8);"
    )));
    stmts.push(raw(format!(
        "INSERT INTO {t} SELECT i, i % {groups}, ((i * 7) % 641 - 320)::int2, \
         ((i * 20117) % 2000000 - 1000000)::int4, ((i % 100000)::int8) * {w_scale} \
         FROM generate_series(1, {rows}) i;"
    )));

    // The full integer-variance projection (poly finalizers over every
    // width and both the population and sample forms).
    let ivar = "var_pop(s)::text, var_samp(s)::text, stddev_pop(s)::text, stddev_samp(s)::text, \
                var_pop(m)::text, var_samp(m)::text, stddev_pop(m)::text, stddev_samp(m)::text, \
                var_pop(w)::text, var_samp(w)::text, stddev_pop(w)::text, stddev_samp(w)::text";

    match shape {
        "numeric:is:serial" => {
            stmts.push(raw(format!(
                "SELECT k, {ivar}, count(*) FROM {t} GROUP BY k ORDER BY k;"
            )));
            // whole-table single group (poly finalize over the full set).
            stmts.push(raw(format!("SELECT {ivar}, count(*) FROM {t};")));
        }
        "numeric:is:par" => {
            let workers = 2 + g.rng.below(3);
            let leader = if g.rng.chance(1, 2) { "on" } else { "off" };
            stmts.push(raw("SET parallel_setup_cost = 0;".to_string()));
            stmts.push(raw("SET parallel_tuple_cost = 0;".to_string()));
            stmts.push(raw("SET min_parallel_table_scan_size = 0;".to_string()));
            stmts.push(raw(format!("SET max_parallel_workers_per_gather = {workers};")));
            stmts.push(raw(format!("SET parallel_leader_participation = {leader};")));
            // Partial+Finalize over a Gather: poly combine/serialize/
            // deserialize. Grouped and scalar both, exact -> byte-identical.
            stmts.push(raw(format!(
                "SELECT k, {ivar}, count(*) FROM {t} GROUP BY k ORDER BY k;"
            )));
            stmts.push(raw(format!("SELECT {ivar}, count(*) FROM {t};")));
            stmts.push(raw("RESET parallel_leader_participation;".to_string()));
            stmts.push(raw("RESET max_parallel_workers_per_gather;".to_string()));
            stmts.push(raw("RESET min_parallel_table_scan_size;".to_string()));
            stmts.push(raw("RESET parallel_tuple_cost;".to_string()));
            stmts.push(raw("RESET parallel_setup_cost;".to_string()));
        }
        _ => {
            // Moving window: the inverse transitions (int*_accum_inv ->
            // int128 discard). A both-edge frame forces discard on entry
            // and exit; a trailing-only frame forces the append-heavy arm.
            let back = 4 + g.rng.below(30);
            let fwd = 1 + g.rng.below(15);
            stmts.push(raw(format!(
                "SELECT i, var_samp(w) OVER f, stddev_pop(m) OVER f, var_pop(s) OVER f, \
                 stddev_samp(w) OVER f FROM {t} \
                 WINDOW f AS (ORDER BY i ROWS BETWEEN {back} PRECEDING AND {fwd} FOLLOWING) \
                 ORDER BY i;"
            )));
            stmts.push(raw(format!(
                "SELECT i, var_pop(w) OVER f, stddev_samp(m) OVER f FROM {t} \
                 WINDOW f AS (ORDER BY i ROWS BETWEEN {back} PRECEDING AND CURRENT ROW) \
                 ORDER BY i;"
            )));
        }
    }
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// --------------------------------------------------------------- xprec ----

/// A deterministic decimal-digit string of `len` digits, first digit
/// nonzero (repeating a fixed pattern; seed-independent for a given len).
fn big_digits(len: usize) -> String {
    const PAT: &[u8] = b"1234567890987654321356724819";
    (0..len).map(|i| PAT[i % PAT.len()] as char).collect()
}

/// Optionally turn an integer digit string into a fractional literal by
/// inserting a decimal point `frac` places from the end.
fn with_scale(digits: &str, frac: usize) -> String {
    let l = digits.len();
    if frac == 0 || frac >= l {
        return digits.to_string();
    }
    format!("{}.{}", &digits[..l - frac], &digits[l - frac..])
}

/// Extreme-precision arithmetic: operands far past the existing suite's
/// ~50-digit ceiling, driving mul_var (Karatsuba), div_var (large rscale),
/// sqrt_var, ln_var and high-scale round/mod.
fn gen_xprec(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numeric:xprec");
    let shape = g.weights.pick(g.rng, &["numeric:xp:mul", "numeric:xp:div", "numeric:xp:trans"]);
    g.fire(shape);
    let la = 300 + g.rng.below_usize(1200);
    let lb = 300 + g.rng.below_usize(1200);
    let a_int = big_digits(la);
    let b_int = big_digits(lb);
    let a_frac = g.rng.below_usize(40);
    let b_frac = g.rng.below_usize(40);
    let a = with_scale(&a_int, a_frac);
    let b = with_scale(&b_int, b_frac);
    let neg = if g.rng.chance(1, 3) { "-" } else { "" };

    let sql = match shape {
        // mul_var Karatsuba + big add/sub across signs and scales.
        "numeric:xp:mul" => format!(
            "SELECT ({neg}'{a}'::numeric * '{b}'::numeric)::text, \
             ('{a}'::numeric + {neg}'{b}'::numeric)::text, \
             ('{a}'::numeric - '{b}'::numeric)::text;"
        ),
        // div_var at a large requested rscale + big mod + high-scale round.
        "numeric:xp:div" => {
            let rs = 40 + g.rng.below(360);
            format!(
                "SELECT round({neg}'{a}'::numeric / '{b}'::numeric, {rs})::text, \
                 ('{a}'::numeric % '{b}'::numeric)::text, \
                 div('{a}'::numeric, '{b}'::numeric)::text;"
            )
        }
        // sqrt_var / ln_var / log big paths over big positive operands.
        _ => {
            let rs = 20 + g.rng.below(200);
            format!(
                "SELECT round(sqrt('{a}'::numeric), {rs})::text, \
                 round(ln('{a}'::numeric), {rs})::text, \
                 round(log('{a}'::numeric), {rs})::text;"
            )
        }
    };
    vec![raw(sql)]
}
