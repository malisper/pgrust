//! Range / multirange operator + function + custom-range-type module
//! (RANGEOPS): the SQL-drainable surface of backend/utils/adt/rangetypes.c
//! and multirangetypes.c — the operator matrix, the constructor + bounds
//! accessor + set-operation function set, canonicalization / empty /
//! infinite edges, and `CREATE TYPE ... AS RANGE` custom range types
//! (subtype + subtype_diff + collation/opclass) exercised through their
//! auto-derived operators and multirange. The recv/textual-input paths were
//! fuzzed separately; this is the operator/function/DDL surface.
//!
//! Scope note: rangetypes_gist.c (the GiST range opclass) is NOT in this
//! module's scope, so the module needs no persistent data table or index —
//! the entire rangetypes.c/multirangetypes.c scalar surface is reachable
//! through literal single-row SELECTs, and the custom-type DDL is emitted as
//! self-contained create/exercise/drop groups (the plpg/coll module
//! pattern: objects are group-local; only the name counters in
//! `RangeopsState` persist, swapped in and out of `Gen` by the session
//! loop).
//!
//! Determinism / differential discipline:
//!   - Every base kind is exercised, including tstzrange/tstzmultirange:
//!     timestamptz `::text` is byte-identical across engines under the
//!     runner's DATETIME_GUC_PIN (TimeZone 'UTC', DateStyle 'ISO, MDY',
//!     applied to BOTH differential sides).
//!   - Range values render deterministically via `::text`; comparison and
//!     bounds-accessor results are scalar/boolean. Aggregates
//!     (range_agg / range_intersect_agg) and unnest are order-independent
//!     by construction (multiranges normalize to a sorted, non-overlapping
//!     canonical form), and the unnest probe still carries a total ORDER BY.
//!   - The set-operation operators `+`/`-` deliberately reach their
//!     "result would not be contiguous" (22000) arms on non-adjacent
//!     operands; both engines raise the identical SQLSTATE, so the error
//!     identity is the compared surface (the dedicated `rangeops:err` arm
//!     also drives the reversed-bound 22000 and malformed-literal 22P02
//!     paths).
//!
//! Paren-balance invariant: the stream-wide textual check counts `(` vs `)`
//! per statement, so the asymmetric bound flags `'(]'` and `'[)'` (each
//! carrying one lone paren) are only ever emitted together in one statement
//! (the `rangeops:flags` arm renders all four flags at once, so the `(]`
//! and `[)` contributions cancel); every other production draws only the
//! paren-neutral flags `'[]'` / `'()'`.

use crate::stmt::{Gen, StmtKind};

/// Group-local custom-range-type name counter. Objects are created and
/// dropped inside a single group; only the counter persists so names stay
/// session-unique (swapped in and out of `Gen` like PlpgState/CollState).
#[derive(Clone, Debug, Default)]
pub struct RangeopsState {
    next_type: u32,
}

impl RangeopsState {
    pub fn new() -> RangeopsState {
        RangeopsState::default()
    }
}

// -------------------------------------------------------------- base kinds ----

/// A built-in base range kind: its range type name, the paired multirange
/// type name, and whether the subtype is discrete (canonicalized to the
/// `[)` normal form — the canonicalization surface).
struct Kind {
    range: &'static str,
    multi: &'static str,
    discrete: bool,
}

const KINDS: &[Kind] = &[
    Kind { range: "int4range", multi: "int4multirange", discrete: true },
    Kind { range: "int8range", multi: "int8multirange", discrete: true },
    Kind { range: "numrange", multi: "nummultirange", discrete: false },
    Kind { range: "daterange", multi: "datemultirange", discrete: true },
    Kind { range: "tsrange", multi: "tsmultirange", discrete: false },
    Kind { range: "tstzrange", multi: "tstzmultirange", discrete: false },
];

fn pick_kind(g: &mut Gen) -> &'static Kind {
    &KINDS[g.rng.below_usize(KINDS.len())]
}

/// Boolean-returning binary operators over (anyrange, anyrange) and
/// (anymultirange, anymultirange) — the shared pg_operator matrix.
const BOOL_OPS: &[&str] =
    &["=", "<>", "<", "<=", ">", ">=", "@>", "<@", "&&", "<<", ">>", "&<", "&>", "-|-"];

/// Range-returning set operators (union / intersection / difference).
const SET_OPS: &[&str] = &["+", "*", "-"];

/// Bounds-accessor / predicate functions (range and multirange).
const BOUND_FNS: &[&str] =
    &["lower", "upper", "isempty", "lower_inc", "upper_inc", "lower_inf", "upper_inf"];

/// Paren-neutral bound flags for random operands (`'()'` contributes one of
/// each paren; `'[]'` none).
fn sym_flag(g: &mut Gen) -> &'static str {
    if g.rng.chance(1, 2) {
        "[]"
    } else {
        "()"
    }
}

// ----------------------------------------------------------------- bounds ----

/// Chronologically-ordered literal pools (index order == value order), so a
/// `lo` index below a `hi` index guarantees `lo <= hi` without parsing.
const DATE_POOL: &[&str] =
    &["'1999-12-31'", "'2020-01-01'", "'2021-06-15'", "'2024-02-29'", "'2030-12-31'"];
const TS_POOL: &[&str] = &[
    "'2020-01-01 00:00:00'",
    "'2021-03-03 12:00:00'",
    "'2022-06-15 08:30:00'",
    "'2030-01-01 23:59:59'",
];
const TSTZ_POOL: &[&str] = &[
    "'2020-01-01 00:00:00+00'",
    "'2021-03-03 12:00:00+00'",
    "'2022-06-15 08:30:00+00'",
    "'2030-01-01 23:59:59+00'",
];

fn ordered_pool(g: &mut Gen, pool: &[&str]) -> (String, String) {
    let n = pool.len();
    let i = g.rng.below_usize(n - 1);
    let j = i + 1 + g.rng.below_usize(n - 1 - i);
    (pool[i].to_string(), pool[j].to_string())
}

/// An ordered `(lo, hi)` bound pair (`lo <= hi`) for the kind.
fn bound_pair(g: &mut Gen, k: &Kind) -> (String, String) {
    match k.range {
        "int4range" | "int8range" => {
            let a = g.rng.range_i64(-30, 60);
            let b = a + g.rng.below(40) as i64;
            (a.to_string(), b.to_string())
        }
        "numrange" => {
            let a = g.rng.range_i64(-20, 20);
            let b = a + 1 + g.rng.below(20) as i64;
            (format!("{}.25", a), format!("{}.75", b))
        }
        "daterange" => ordered_pool(g, DATE_POOL),
        "tsrange" => ordered_pool(g, TS_POOL),
        _ => ordered_pool(g, TSTZ_POOL),
    }
}

/// A single in-range value (for the empty-range `(v, v)` shape).
fn point_val(g: &mut Gen, k: &Kind) -> String {
    match k.range {
        "int4range" | "int8range" => g.rng.range_i64(-10, 30).to_string(),
        "numrange" => format!("{}.5", g.rng.range_i64(-10, 20)),
        "daterange" => DATE_POOL[g.rng.below_usize(DATE_POOL.len())].to_string(),
        "tsrange" => TS_POOL[g.rng.below_usize(TS_POOL.len())].to_string(),
        _ => TSTZ_POOL[g.rng.below_usize(TSTZ_POOL.len())].to_string(),
    }
}

/// A comparable element literal (for the `@>`/`<@` element forms).
fn elem(g: &mut Gen, k: &Kind) -> String {
    match k.range {
        "int4range" => g.rng.range_i64(-30, 70).to_string(),
        "int8range" => format!("{}::int8", g.rng.range_i64(-30, 70)),
        "numrange" => format!("{}.5::numeric", g.rng.range_i64(-20, 30)),
        "daterange" => "'2021-01-01'::date".to_string(),
        "tsrange" => "'2021-06-01 06:00:00'::timestamp".to_string(),
        _ => "'2021-06-01 06:00:00+00'::timestamptz".to_string(),
    }
}

// ------------------------------------------------------------ range values ----

/// A range constructor call: 2-arg, 3-arg (paren-neutral flag), or the
/// empty `(v, v)` form. Bounds are occasionally NULL (infinite edges).
fn range_ctor(g: &mut Gen, k: &Kind) -> String {
    let (lo, hi) = bound_pair(g, k);
    let lo_n = if g.rng.chance(1, 9) { "NULL".to_string() } else { lo };
    let hi_n = if g.rng.chance(1, 9) { "NULL".to_string() } else { hi };
    match g.rng.below(6) {
        0 => {
            let v = point_val(g, k);
            format!("{}({}, {})", k.range, v, v) // empty (v, v)
        }
        1 | 2 => format!("{}({}, {}, '{}')", k.range, lo_n, hi_n, sym_flag(g)),
        _ => format!("{}({}, {})", k.range, lo_n, hi_n),
    }
}

/// A range value: mostly a constructor, occasionally `'empty'` or a
/// paren-neutral text literal (int/num kinds only — the datetime text-input
/// forms need embedded quoting the constructor form sidesteps).
fn range_expr(g: &mut Gen, k: &Kind) -> String {
    match g.rng.below(8) {
        0 => format!("'empty'::{}", k.range),
        1 if matches!(k.range, "int4range" | "int8range" | "numrange") => {
            let (lo, hi) = bound_pair(g, k);
            if g.rng.chance(1, 2) {
                format!("'[{},{}]'::{}", lo, hi, k.range)
            } else {
                format!("'({},{})'::{}", lo, hi, k.range)
            }
        }
        _ => range_ctor(g, k),
    }
}

/// A multirange value: constructor of 0-3 ranges, or `multirange(range)`.
fn mr_expr(g: &mut Gen, k: &Kind) -> String {
    match g.rng.below(5) {
        0 => format!("{}()", k.multi),
        1 => format!("{}({})", k.multi, range_ctor(g, k)),
        2 | 3 => format!("{}({}, {})", k.multi, range_ctor(g, k), range_ctor(g, k)),
        _ => format!("multirange({})", range_ctor(g, k)),
    }
}

fn raw(sql: String) -> Vec<StmtKind> {
    vec![StmtKind::Raw(sql)]
}

// ------------------------------------------------------------- entry point ----

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_rangeops_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("rangeops");
    let action = g.weights.pick(
        g.rng,
        &[
            "rangeops:scalar",
            "rangeops:mr",
            "rangeops:flags",
            "rangeops:agg",
            "rangeops:custom",
            "rangeops:err",
        ],
    );
    g.fire(action);
    match action {
        "rangeops:scalar" => gen_scalar(g),
        "rangeops:mr" => gen_mr(g),
        "rangeops:flags" => gen_flags(g),
        "rangeops:agg" => gen_agg(g),
        "rangeops:custom" => gen_custom(g),
        _ => gen_err(g),
    }
}

// ------------------------------------------------------------ range scalar ----

fn gen_scalar(g: &mut Gen) -> Vec<StmtKind> {
    let k = pick_kind(g);
    let class = g.weights.pick(
        g.rng,
        &["ro:cmp", "ro:setop", "ro:elem", "ro:bounds", "ro:merge", "ro:hash"],
    );
    g.fire(class);
    let sql = match class {
        "ro:cmp" => {
            let op = BOOL_OPS[g.rng.below_usize(BOOL_OPS.len())];
            format!("SELECT {} {} {};", range_expr(g, k), op, range_expr(g, k))
        }
        "ro:setop" => {
            let op = SET_OPS[g.rng.below_usize(SET_OPS.len())];
            format!("SELECT ({} {} {})::text;", range_expr(g, k), op, range_expr(g, k))
        }
        "ro:elem" => {
            if g.rng.chance(1, 2) {
                format!("SELECT {} @> {};", range_expr(g, k), elem(g, k))
            } else {
                format!("SELECT {} <@ {};", elem(g, k), range_expr(g, k))
            }
        }
        "ro:bounds" => {
            let f = BOUND_FNS[g.rng.below_usize(BOUND_FNS.len())];
            let r = range_expr(g, k);
            // lower/upper return the subtype; the rest return bool. Cast the
            // subtype-returning ones to text for a byte-exact scalar.
            if f == "lower" || f == "upper" {
                format!("SELECT ({}({}))::text;", f, r)
            } else {
                format!("SELECT {}({});", f, r)
            }
        }
        "ro:merge" => {
            format!("SELECT range_merge({}, {})::text;", range_expr(g, k), range_expr(g, k))
        }
        _ => format!("SELECT hash_range({});", range_ctor(g, k)),
    };
    raw(sql)
}

// ------------------------------------------------------------- multirange ----

fn gen_mr(g: &mut Gen) -> Vec<StmtKind> {
    let k = pick_kind(g);
    let class = g.weights.pick(
        g.rng,
        &["ro:mr:op", "ro:mr:mixed", "ro:mr:bounds", "ro:mr:unnest", "ro:mr:ctor"],
    );
    g.fire(class);
    let sql = match class {
        "ro:mr:op" => {
            let op = BOOL_OPS[g.rng.below_usize(BOOL_OPS.len())];
            format!("SELECT {} {} {};", mr_expr(g, k), op, mr_expr(g, k))
        }
        "ro:mr:mixed" => {
            // Cross-operand forms: multirange vs range / element.
            match g.rng.below(4) {
                0 => format!("SELECT {} @> {};", mr_expr(g, k), range_ctor(g, k)),
                1 => format!("SELECT {} <@ {};", range_ctor(g, k), mr_expr(g, k)),
                2 => format!("SELECT {} @> {};", mr_expr(g, k), elem(g, k)),
                _ => format!("SELECT {} && {};", mr_expr(g, k), range_ctor(g, k)),
            }
        }
        "ro:mr:bounds" => {
            let m = mr_expr(g, k);
            match g.rng.below(4) {
                0 => format!("SELECT (lower({}))::text, (upper({}))::text;", m, mr_expr(g, k)),
                1 => format!("SELECT isempty({}), lower_inf({}), upper_inf({});", m, mr_expr(g, k), mr_expr(g, k)),
                2 => format!("SELECT range_merge({})::text;", m),
                _ => format!("SELECT hash_multirange({}) IS NOT NULL;", m),
            }
        }
        "ro:mr:unnest" => {
            let a = g.next_alias();
            format!(
                "SELECT {a}.r::text FROM unnest({m}) AS {a}(r) ORDER BY {a}.r;",
                a = a,
                m = mr_expr(g, k)
            )
        }
        _ => {
            // Constructor round-trip and multirange set operations.
            if g.rng.chance(1, 2) {
                format!("SELECT ({})::text;", mr_expr(g, k))
            } else {
                let op = SET_OPS[g.rng.below_usize(SET_OPS.len())];
                format!("SELECT ({} {} {})::text;", mr_expr(g, k), op, mr_expr(g, k))
            }
        }
    };
    raw(sql)
}

// ----------------------------------------------------- flags + canonical ----

/// All four bound flags in one statement (balanced: the `(]` and `[)`
/// lone-paren contributions cancel), plus, for discrete kinds, the
/// empty / infinite-edge witnesses. Discrete kinds show canonicalization
/// directly in the `[]`/`(]` `::text` output.
fn gen_flags(g: &mut Gen) -> Vec<StmtKind> {
    let k = pick_kind(g);
    let (lo, hi) = bound_pair(g, k);
    let mut stmts = vec![StmtKind::Raw(format!(
        "SELECT ({r}({lo}, {hi}, '[]'))::text, ({r}({lo}, {hi}, '(]'))::text, \
         ({r}({lo}, {hi}, '[)'))::text, ({r}({lo}, {hi}, '()'))::text;",
        r = k.range,
        lo = lo,
        hi = hi
    ))];
    // Empty / infinite edges (valid for every kind; NULL bound => infinite).
    stmts.push(StmtKind::Raw(format!(
        "SELECT ({r}(NULL, {hi}))::text, ({r}({lo}, NULL))::text, \
         ('empty'::{r})::text, isempty('empty'::{r});",
        r = k.range,
        lo = lo,
        hi = hi
    )));
    // Discrete-subtype canonicalization witness: `[]` canonicalizes to the
    // `[)` normal form, so upper_inc collapses to false (flags stay
    // paren-neutral). Continuous subtypes have no canonical function.
    if k.discrete {
        g.fire("rangeops:flags:canon");
        stmts.push(StmtKind::Raw(format!(
            "SELECT upper_inc({r}({lo}, {hi}, '[]')), lower_inc({r}({lo}, {hi}, '()'));",
            r = k.range,
            lo = lo,
            hi = hi
        )));
    }
    stmts
}

// ------------------------------------------------------------- aggregates ----

fn gen_agg(g: &mut Gen) -> Vec<StmtKind> {
    let k = pick_kind(g);
    let f = if g.rng.chance(1, 2) { "range_agg" } else { "range_intersect_agg" };
    g.fire2("rangeops:agg:", f);
    let (r1, r2, r3) = (range_ctor(g, k), range_ctor(g, k), range_ctor(g, k));
    raw(format!(
        "SELECT {f}(r)::text FROM (VALUES ({r1}), ({r2}), ({r3})) v(r);",
        f = f,
        r1 = r1,
        r2 = r2,
        r3 = r3
    ))
}

// ------------------------------------------------- custom range types (DDL) ----

/// A self-contained `CREATE TYPE ... AS RANGE` group: create the type,
/// exercise its auto-derived operators / bounds accessors / multirange, then
/// drop it. The multirange type is dropped by the internal dependency when
/// the range type is dropped.
fn gen_custom(g: &mut Gen) -> Vec<StmtKind> {
    let variant = g.weights.pick(g.rng, &["ro:ct:int", "ro:ct:float", "ro:ct:text"]);
    g.fire(variant);
    let t = format!("fz_rng_ct{}", g.rangeops.next_type);
    g.rangeops.next_type += 1;
    let mut stmts = Vec::new();
    match variant {
        "ro:ct:int" => {
            // Discrete int subtype (no custom canonical function — canonical
            // functions have a shell-type chicken-and-egg that neither the
            // PG docs nor the regression suite instantiate; the built-in
            // discrete canonicalization surface is covered by gen_flags).
            stmts.push(StmtKind::Raw(format!("CREATE TYPE {} AS RANGE (subtype = int4);", t)));
            stmts.push(StmtKind::Raw(format!(
                "SELECT ({t}(1, 10))::text, ({t}(1, 10, '[]'))::text, isempty({t}(5, 5));",
                t = t
            )));
            stmts.push(StmtKind::Raw(format!(
                "SELECT {t}(1, 10) @> 5, {t}(1, 10) && {t}(8, 20), \
                 {t}(1, 5) << {t}(8, 12), {t}(1, 5) -|- {t}(5, 9);",
                t = t
            )));
            stmts.push(StmtKind::Raw(format!(
                "SELECT (lower({t}(2, 8)))::text, (upper({t}(2, 8)))::text, \
                 lower_inc({t}(2, 8)), upper_inf({t}(2, 8));",
                t = t
            )));
            stmts.push(StmtKind::Raw(format!(
                "SELECT range_agg(x)::text FROM (VALUES ({t}(1, 3)), ({t}(5, 9))) v(x);",
                t = t
            )));
            stmts.push(StmtKind::Raw(format!("SELECT multirange({t}(1, 4))::text;", t = t)));
        }
        "ro:ct:float" => {
            // Continuous float8 subtype with a built-in subtype_diff.
            stmts.push(StmtKind::Raw(format!(
                "CREATE TYPE {} AS RANGE (subtype = float8, subtype_diff = float8mi);",
                t
            )));
            stmts.push(StmtKind::Raw(format!(
                "SELECT ({t}(1.5, 9.25))::text, isempty({t}(1.5, 1.5)), \
                 {t}(1.5, 9.25) @> 4.0::float8;",
                t = t
            )));
            stmts.push(StmtKind::Raw(format!(
                "SELECT {t}(1, 5) && {t}(4, 9), {t}(1, 5) << {t}(6, 9), \
                 range_merge({t}(1, 3), {t}(7, 9))::text;",
                t = t
            )));
            stmts.push(StmtKind::Raw(format!(
                "SELECT (lower({t}(2, 8)))::text, (upper({t}(2, 8)))::text;",
                t = t
            )));
        }
        _ => {
            // Text subtype: collation "C" or a collation-agnostic opclass.
            let opts = if g.rng.chance(1, 2) {
                "subtype = text, collation = \"C\""
            } else {
                "subtype = text, subtype_opclass = text_pattern_ops"
            };
            stmts.push(StmtKind::Raw(format!("CREATE TYPE {} AS RANGE ({});", t, opts)));
            stmts.push(StmtKind::Raw(format!(
                "SELECT ({t}('a', 'm'))::text, {t}('a', 'm') @> 'f'::text, isempty({t}('x', 'x'));",
                t = t
            )));
            stmts.push(StmtKind::Raw(format!(
                "SELECT {t}('a', 'm') << {t}('p', 'z'), \
                 (lower({t}('a', 'm')))::text, (upper({t}('a', 'm')))::text;",
                t = t
            )));
        }
    }
    stmts.push(StmtKind::Raw(format!("DROP TYPE {};", t)));
    stmts
}

// -------------------------------------------------------------- error arms ----

/// Deterministic both-sides-error probes: reversed bounds (22000), malformed
/// literal (22P02), and non-contiguous set-operation results (22000). Both
/// engines raise the identical SQLSTATE, so error identity is the compared
/// surface.
fn gen_err(g: &mut Gen) -> Vec<StmtKind> {
    let sql = match g.rng.below(5) {
        0 => "SELECT int4range(5, 1);".to_string(),
        1 => "SELECT '[x,y]'::numrange;".to_string(),
        2 => "SELECT (int4range(1, 5) + int4range(10, 20))::text;".to_string(),
        3 => "SELECT (int4range(1, 10) - int4range(3, 4))::text;".to_string(),
        _ => "SELECT '{[1,3]'::int4multirange;".to_string(),
    };
    raw(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Session-shaped harness: persistent RangeopsState across groups.
    fn gen_many(seed: u64, n: usize, spec: &str) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut state = RangeopsState::new();
        let mut groups = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            std::mem::swap(&mut g.rangeops, &mut state);
            let kinds = gen_rangeops_module(&mut g);
            std::mem::swap(&mut g.rangeops, &mut state);
            assert!(!kinds.is_empty());
            groups.push(kinds.iter().map(|k| k.to_sql()).collect());
            prods.extend(p);
        }
        (groups, prods)
    }

    fn flat(groups: &[Vec<String>]) -> Vec<String> {
        groups.iter().flatten().cloned().collect()
    }

    #[test]
    fn rangeops_is_deterministic() {
        let (a, _) = gen_many(0x9E, 500, "");
        let (b, _) = gen_many(0x9E, 500, "");
        assert_eq!(a, b);
        let (c, _) = gen_many(0x9F, 500, "");
        assert_ne!(a, c);
    }

    /// The load-bearing invariant: every statement is single-line,
    /// terminated, and paren-balanced (the asymmetric `(]`/`[)` flags must
    /// cancel within their statement).
    #[test]
    fn rangeops_statements_are_well_formed() {
        let (groups, _) = gen_many(0x2A, 4000, "");
        for sql in flat(&groups) {
            assert!(!sql.contains('\n'), "multi-line: {sql}");
            assert!(sql.ends_with(';'), "unterminated: {sql}");
            assert_eq!(
                sql.matches('(').count(),
                sql.matches(')').count(),
                "unbalanced parens: {sql}"
            );
        }
    }

    #[test]
    fn rangeops_variety() {
        let (groups, prods) = gen_many(0x4A6, 6000, "");
        let all = flat(&groups).join("\n");
        for frag in [
            "int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange",
            "int4multirange", "tstzmultirange",
            " @> ", " <@ ", " && ", " << ", " >> ", " &< ", " &> ", " -|- ",
            "hash_range(", "hash_multirange(", "range_merge(",
            "range_agg(", "range_intersect_agg(",
            "lower_inf(", "upper_inf(", "isempty(",
            "unnest(", "ORDER BY ",
            "'[]'", "'(]'", "'[)'", "'()'", "'empty'::", "(NULL, ",
            "CREATE TYPE fz_rng_ct", "AS RANGE (subtype = int4)",
            "subtype = float8, subtype_diff = float8mi",
            "collation = \"C\"", "subtype_opclass = text_pattern_ops",
            "DROP TYPE fz_rng_ct",
            "SELECT int4range(5, 1);", "'[x,y]'::numrange",
            "multirange(",
        ] {
            assert!(all.contains(frag), "rangeops flavor {frag:?} never generated");
        }
        for p in [
            "rangeops:scalar", "rangeops:mr", "rangeops:flags", "rangeops:agg",
            "rangeops:custom", "rangeops:err",
            "ro:cmp", "ro:setop", "ro:elem", "ro:bounds", "ro:merge", "ro:hash",
            "ro:mr:op", "ro:mr:mixed", "ro:mr:bounds", "ro:mr:unnest", "ro:mr:ctor",
            "ro:ct:int", "ro:ct:float", "ro:ct:text",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    /// Model-replay: custom types are created exactly once and always
    /// dropped in the same group; names never repeat stream-wide.
    #[test]
    fn custom_types_paired_and_unique() {
        let (groups, _) = gen_many(0xC7, 5000, "");
        use std::collections::HashSet;
        let mut created: HashSet<String> = HashSet::new();
        for group in &groups {
            let mut group_created: Vec<String> = Vec::new();
            let mut group_dropped: Vec<String> = Vec::new();
            for sql in group {
                if let Some(rest) = sql.strip_prefix("CREATE TYPE ") {
                    let name = rest.split(' ').next().unwrap().to_string();
                    assert!(created.insert(name.clone()), "type name reused: {name}");
                    group_created.push(name);
                } else if let Some(rest) = sql.strip_prefix("DROP TYPE ") {
                    group_dropped.push(rest.trim_end_matches(';').to_string());
                }
            }
            assert_eq!(group_created, group_dropped, "create/drop mismatch in group: {group:?}");
        }
    }

    /// The flags arm emits all four bound flags in a single statement.
    #[test]
    fn flags_arm_covers_all_four() {
        let (groups, _) = gen_many(0xF1A, 3000, "rangeops:flags=50");
        let saw_all_four = flat(&groups).iter().any(|s| {
            s.contains("'[]'") && s.contains("'(]'") && s.contains("'[)'") && s.contains("'()'")
        });
        assert!(saw_all_four, "no single statement carried all four bound flags");
    }
}
