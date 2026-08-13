//! Scalar-expression opcode saturation module (`expreval:*`).
//!
//! Complements the LD2/W4 `einterp` module. Where `einterp` reaches each
//! rare `EEOP_*` arm of `ExecInterpExpr` at least once, this module
//! *saturates* the specific scalar forms the mutation lane flagged as the
//! executor oracle's biggest surviving gaps — the arms where a wrong value
//! or an XX000/internal-error can hide because the corpus exercises them
//! only shallowly:
//!
//!   - hashed `ScalarArrayOp` (`= ANY(ARRAY[...])` large enough to hash,
//!     and `<> ALL`), plus the small-array linear path, across mixed
//!     operand types and with NULLs in both the scan operand and the array
//!     (`EEOP_HASHED_SCALARARRAYOP` / `EEOP_SCALARARRAYOP`);
//!   - `IS JSON` predicate family — VALUE/SCALAR/ARRAY/OBJECT and
//!     WITH/WITHOUT UNIQUE KEYS, IS NOT JSON, over text and jsonb operands
//!     including invalid text and duplicate-key objects (PG16 `EEOP_IS_JSON`);
//!   - whole-row `Var` — `SELECT t`, `row_to_json(t)`, whole-row in a
//!     predicate and a comparison (`EEOP_WHOLEROW`);
//!   - `IS DISTINCT FROM` / `IS NOT DISTINCT FROM` across scalar type pairs
//!     and composite/ROW operands, exercising the NULL 3VL corners
//!     (`EEOP_DISTINCT` / `EEOP_NOT_DISTINCT`);
//!   - `GREATEST` / `LEAST` over mixed numeric arms with NULL skipping and
//!     the all-NULL → NULL corner (`EEOP_MINMAX`);
//!   - searched and simple `CASE` — nested, no-ELSE (→ NULL), and arm
//!     type-unification (`EEOP_CASE_*` / `EEOP_CASE_TESTVAL`);
//!   - `COALESCE` / `NULLIF` — short-circuit and type unification, plus the
//!     NULLIF both-NULL / equal-args / one-NULL corners
//!     (`EEOP_COALESCE` / `EEOP_NULLIF`-via-CaseTestExpr);
//!   - boolean `AND` / `OR` / `NOT` short-circuit and 3VL truth tables
//!     (`EEOP_BOOL_AND_STEP*` / `EEOP_BOOL_OR_STEP*` / `EEOP_BOOL_NOT_STEP`);
//!   - `ROW` and array comparison operators with NULL elements
//!     (`EEOP_ROWCOMPARE_STEP` and the array-cmp support funcs).
//!
//! Discipline (same as `crate::einterp` / `crate::nodes`):
//!   - every statement is a single line, `;`-terminated, and self-contained
//!     (no persistent objects — operands come from inline VALUES relations,
//!     so nothing is created or dropped);
//!   - **operands are always VALUES columns, never bare literals**, so
//!     `eval_const_expressions` cannot fold the expression away before it
//!     reaches the interpreter (the whole point of the module);
//!   - every projection is `ORDER BY`-stable on a non-null distinct key so
//!     the two-engine row order is identical;
//!   - shapes are valid on BOTH engines and deterministic — no volatile /
//!     time-of-day functions, no float reassociation surface.

use crate::stmt::{Gen, StmtKind};

const SHAPES: &[&str] = &[
    "expreval:saop:hash",
    "expreval:saop:linear",
    "expreval:isjson",
    "expreval:wholerow",
    "expreval:distinct",
    "expreval:greatest",
    "expreval:case",
    "expreval:coalesce",
    "expreval:bool3vl",
    "expreval:rowcmp",
];

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

/// A deterministic 4-row integer/text operand relation with NULLs, keyed by
/// a non-null distinct `k` for stable ordering. Columns:
///   k int (1..4, key), i2 int2, i4 int4, i8 int8, n numeric, t text, bl bool
fn ints(g: &mut Gen) -> String {
    // small deterministic offset so successive picks differ but replay.
    let o = g.rng.below(5) as i64;
    format!(
        "(VALUES \
         (1, {a}::int2, {a}::int4, {a}::int8, {a}.5::numeric, 'a{a}', true), \
         (2, {b}::int2, {b}::int4, {b}::int8, NULL::numeric, NULL, false), \
         (3, NULL::int2, NULL::int4, NULL::int8, {c}.25::numeric, 'c{c}', NULL), \
         (4, {d}::int2, {d}::int4, {d}::int8, {d}.0::numeric, 'a{a}', true)\
         ) x(k, i2, i4, i8, n, t, bl)",
        a = 1 + o,
        b = 7 + o,
        c = 3 + o,
        d = 12 + o,
    )
}

fn body(g: &mut Gen, shape: &str) -> Vec<StmtKind> {
    match shape {
        // ---- hashed ScalarArrayOp: array large enough to force the hashed
        // path (>= 9 const elements + hashable = / <> op), with a NULL in
        // the array and NULLs in the scan operand (3VL membership). -------
        "expreval:saop:hash" => {
            let x = ints(g);
            vec![
                raw(format!(
                    "SELECT k, i4 = ANY (ARRAY[1,2,3,4,5,6,7,8,9,10,11,12,13]), \
                     i4 <> ALL (ARRAY[1,2,3,4,5,6,7,8,9,10,11,12,13]), \
                     i8 = ANY (ARRAY[7,8,9,10,11,12,13,14,15,16,NULL]::int8[]), \
                     n = ANY (ARRAY[1.5,3.25,7.5,12.0,2.5,4.5,5.5,6.5,8.5,9.5]::numeric[]) \
                     FROM {x} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT k, t = ANY (ARRAY['a1','a2','a3','c3','x','y','z','q','w','e','r']) \
                     FROM {x} ORDER BY k;"
                )),
            ]
        }
        // ---- small-array linear SAOP path + mixed operand types + NULL
        // 3VL (NULL operand, NULL member → NULL, not false). -------------
        "expreval:saop:linear" => {
            let x = ints(g);
            vec![
                raw(format!(
                    "SELECT k, i2 = ANY (ARRAY[1,7,12]::int2[]), \
                     i2 <> ALL (ARRAY[1,7]::int2[]), \
                     i4 = ANY (ARRAY[NULL, 7]::int4[]), \
                     bl = ANY (ARRAY[true, NULL]::bool[]) \
                     FROM {x} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT k, t = ANY (ARRAY['a1', NULL, 'c3']), \
                     n <> ALL (ARRAY[1.5, 3.25]::numeric[]) FROM {x} ORDER BY k;"
                )),
            ]
        }
        // ---- IS JSON predicate family (PG16). text operand exercises the
        // full parse (incl invalid text and duplicate keys for UNIQUE KEYS);
        // jsonb operand exercises the already-parsed path. --------------
        "expreval:isjson" => {
            let o = g.rng.below(3);
            let j = format!(
                "(VALUES \
                 (1, '{{\"a\":{o},\"b\":2}}'::text), \
                 (2, '[1,2,3]'), \
                 (3, '\"s{o}\"'), \
                 (4, '{o}'), \
                 (5, 'not json{o}'), \
                 (6, '{{\"a\":1,\"a\":2}}'), \
                 (7, NULL)\
                 ) j(k, s)"
            );
            vec![
                raw(format!(
                    "SELECT k, s IS JSON, s IS NOT JSON, s IS JSON VALUE, \
                     s IS JSON SCALAR, s IS JSON ARRAY, s IS JSON OBJECT \
                     FROM {j} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT k, s IS JSON WITH UNIQUE KEYS, \
                     s IS JSON OBJECT WITHOUT UNIQUE KEYS, \
                     s IS JSON WITH UNIQUE KEYS FROM {j} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT k, (s::jsonb) IS JSON, (s::jsonb) IS JSON ARRAY, \
                     (s::jsonb) IS JSON OBJECT FROM {j} \
                     WHERE k IN (1,2,3,4,6) ORDER BY k;"
                )),
            ]
        }
        // ---- whole-row Var: SELECT of the alias, row_to_json, whole-row in
        // a predicate and a self-comparison; all over a VALUES alias so no
        // object churn. -------------------------------------------------
        "expreval:wholerow" => {
            let x = ints(g);
            vec![
                raw(format!(
                    "SELECT k, row_to_json(x)::text, (x IS NOT NULL), (x IS NULL) \
                     FROM {x} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT k, (x = x), (x IS DISTINCT FROM x) FROM {x} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT count(*) FROM {x} WHERE x IS NOT NULL;"
                )),
            ]
        }
        // ---- IS DISTINCT / IS NOT DISTINCT across scalar type pairs and a
        // composite/ROW operand, hitting the NULL 3VL corners. ----------
        "expreval:distinct" => {
            let x = ints(g);
            vec![
                raw(format!(
                    "SELECT k, i4 IS DISTINCT FROM i8, i4 IS NOT DISTINCT FROM i2, \
                     n IS DISTINCT FROM i4, t IS NOT DISTINCT FROM t, \
                     bl IS DISTINCT FROM (i4 > 0) FROM {x} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT k, i4 IS DISTINCT FROM NULL, NULL::int IS NOT DISTINCT FROM i4, \
                     t IS DISTINCT FROM NULL FROM {x} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT k, ROW(i4, t) IS DISTINCT FROM ROW(i8, t), \
                     ROW(i4, t) IS NOT DISTINCT FROM ROW(i4, t), \
                     ROW(i2, bl) IS DISTINCT FROM ROW(NULL::int2, bl) FROM {x} ORDER BY k;"
                )),
            ]
        }
        // ---- GREATEST/LEAST mixed numeric arms + NULL skipping + all-NULL
        // → NULL corner. ------------------------------------------------
        "expreval:greatest" => {
            let x = ints(g);
            vec![
                raw(format!(
                    "SELECT k, GREATEST(i2, i4, i8), LEAST(i2, i4, i8), \
                     GREATEST(i4, n), LEAST(n, i4, 5), \
                     GREATEST(i4, NULL, i8), LEAST(NULL::int, NULL::int) \
                     FROM {x} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT k, GREATEST(t, 'a5'), LEAST(t, 'a5', 'z') FROM {x} ORDER BY k;"
                )),
            ]
        }
        // ---- searched + simple CASE: nested, no-ELSE (→ NULL), arm type
        // unification (int/numeric/text). -------------------------------
        "expreval:case" => {
            let x = ints(g);
            vec![
                raw(format!(
                    "SELECT k, CASE WHEN i4 IS NULL THEN 'null' WHEN i4 > 5 THEN 'big' \
                     WHEN i4 > 0 THEN 'small' END, \
                     CASE i2 WHEN 1 THEN 'one' WHEN 7 THEN 'seven' ELSE 'other' END, \
                     CASE WHEN bl THEN i4 ELSE n END FROM {x} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT k, CASE i4 WHEN 1 THEN CASE WHEN bl THEN 'a' ELSE 'b' END \
                     WHEN 7 THEN 'seven' END, \
                     CASE WHEN i4 IS NULL THEN 0 ELSE i8 + 1 END FROM {x} ORDER BY k;"
                )),
            ]
        }
        // ---- COALESCE short-circuit + type unify; NULLIF corners. The
        // guarded-division arm is only reached when the guard is NULL/false,
        // so a correct short-circuit never divides by zero; an engine that
        // eagerly evaluates would raise division_by_zero (error identity). -
        "expreval:coalesce" => {
            let x = ints(g);
            vec![
                raw(format!(
                    "SELECT k, COALESCE(i4, i8, 0), COALESCE(n, i4, 0), \
                     COALESCE(t, 'def'), COALESCE(i2, i4, i8, 99) FROM {x} ORDER BY k;"
                )),
                // short-circuit witness: COALESCE stops at first non-null,
                // so 100/i4 is not evaluated when i4 is present-and-nonzero
                // only via the guard column; here i4 first arm guards it.
                raw(format!(
                    "SELECT k, COALESCE(NULLIF(i4, 0), 100 / NULLIF(i4, 0), -1) FROM {x} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT k, NULLIF(i4, i8), NULLIF(i4, 7), NULLIF(t, t), \
                     NULLIF(n, n), NULLIF(NULL::int, i4) FROM {x} ORDER BY k;"
                )),
            ]
        }
        // ---- boolean AND/OR/NOT 3VL truth tables + short-circuit witness
        // (guarded division reached only on the non-short-circuit branch). -
        "expreval:bool3vl" => {
            let x = ints(g);
            vec![
                raw(format!(
                    "SELECT k, (bl AND (i4 > 0)), (bl OR (i4 IS NULL)), NOT bl, \
                     ((i4 IS NULL) AND bl), ((i4 IS NULL) OR bl) FROM {x} ORDER BY k;"
                )),
                // AND short-circuits on false; OR on true. The div arm is
                // guarded so it is never reached when i4 is 0 / NULL.
                raw(format!(
                    "SELECT k, (i4 IS NOT NULL AND i4 <> 0 AND (100 / i4) > 0), \
                     (i4 IS NULL OR i4 = 0 OR (100 / i4) > 0) FROM {x} ORDER BY k;"
                )),
            ]
        }
        // ---- ROW / array comparison operators with NULL elements. -------
        "expreval:rowcmp" => {
            let x = ints(g);
            vec![
                raw(format!(
                    "SELECT k, ROW(i4, i8) = ROW(i2, i8), ROW(i4, t) < ROW(i8, t), \
                     ROW(i4, t) <= ROW(i4, t), ROW(i2, bl) <> ROW(i4, bl) \
                     FROM {x} ORDER BY k;"
                )),
                raw(format!(
                    "SELECT k, ARRAY[i2, i4] = ARRAY[i4, i2], \
                     ARRAY[i4, i8] < ARRAY[i8, i4], \
                     ARRAY[t, 'z'] <= ARRAY['a5', 'z'] FROM {x} ORDER BY k;"
                )),
            ]
        }
        other => unreachable!("expreval: unknown shape {other}"),
    }
}

pub fn gen_expreval_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("expreval");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    body(g, shape)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize) -> Vec<Vec<String>> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut groups = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            groups.push(
                gen_expreval_module(&mut g).iter().map(|s| s.to_sql()).collect::<Vec<_>>(),
            );
        }
        groups
    }

    /// Single-line, ';'-terminated, and self-contained: this module creates
    /// no persistent objects, so no CREATE may appear at all.
    #[test]
    fn groups_are_self_contained() {
        let groups = gen_groups(0xE7E, 600);
        for group in &groups {
            assert!(!group.is_empty(), "empty group");
            for sql in group {
                assert!(sql.ends_with(';'), "{sql}");
                assert!(!sql.contains('\n'), "{sql}");
                assert!(!sql.contains("CREATE "), "unexpected object churn: {sql}");
                assert!(!sql.contains("DROP "), "unexpected object churn: {sql}");
            }
        }
    }

    /// Every projection carries a stable ORDER BY (or is a scalar aggregate)
    /// so the two-engine row order is identical.
    #[test]
    fn selects_are_order_stable() {
        let groups = gen_groups(0x0D1, 600);
        for group in &groups {
            for sql in group {
                let is_select = sql.starts_with("SELECT ");
                if is_select {
                    let ordered = sql.contains(" ORDER BY ");
                    let scalar_agg = sql.contains("count(*)");
                    assert!(
                        ordered || scalar_agg,
                        "unordered multi-row SELECT: {sql}"
                    );
                }
            }
        }
    }

    /// Same seed, same stream — the reproducibility witness.
    #[test]
    fn deterministic_by_seed() {
        assert_eq!(gen_groups(42, 120), gen_groups(42, 120));
    }

    /// Every shape is reachable from the default weight table.
    #[test]
    fn all_shapes_fire() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(7);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..4000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            gen_expreval_module(&mut g);
            for p in prods {
                seen.insert(p);
            }
        }
        for shape in SHAPES {
            assert!(seen.contains(*shape), "shape never fired: {shape}");
        }
    }
}
