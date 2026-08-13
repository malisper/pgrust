//! ruleutils deparse-drain module (Track-B, `pg_get_*def` round-trip): a
//! randomized-but-deterministic sweep of the backend/utils/adt/ruleutils.c
//! deparse surface. Where `objid` (LD1) embeds a FIXED hand-verified deck of
//! deparse probes, this module GENERATES combinatorial variety over the same
//! entry points — the shapes that fixed decks under-cover: the
//! pg_get_viewdef wrap-column path at varied widths, format_type over a
//! full typmod matrix, function-signature deparse
//! (pg_get_function_arguments / _result / _identity_arguments) over varied
//! argument configurations, the pretty/non-pretty matrix on every def
//! function, pg_get_expr over pg_attrdef/pg_constraint, and a viewdef
//! idempotence round-trip (deparse -> re-CREATE -> re-deparse, EXECUTEd in a
//! DO block so a non-idempotent deparse surfaces as an error divergence).
//!
//! Each group is fully self-contained and collision-proof: everything is
//! created inside a fresh `ru_s` schema (dropped-if-exists first, then
//! CREATE, then DROP ... CASCADE at the tail), so a mid-group failure can
//! never strand residue for the next group. Every multi-row deparse probe
//! carries a TOTAL ORDER BY (over a stable catalog key — attnum, conname,
//! relname, stxname, index name) so the row set compares byte-for-byte
//! across the two differential engines regardless of heap/scan order.
//!
//! The comparison is the point: the diffrunner applies each statement to
//! pgrust and to verbatim REL_18_3 C in lockstep and compares the emitted
//! TEXT. Any deparse-text mismatch is a HIGH finding — it breaks pg_dump
//! fidelity. pgrust has known sibling deparse gaps, so this surface is a
//! high-yield drain target.
//!
//! Determinism: shape selection and the idempotence arm draw through
//! `weights` (registered production names, covloop-steerable); all intra-
//! shape variety draws directly from `Gen::rng`. Same seed + same weights =
//! byte-identical stream.

use crate::stmt::{Gen, StmtKind};

/// Registered shape production names (each must appear in
/// `weights::PROD_WEIGHTS`; `weights.pick` debug_asserts otherwise). Also
/// the coverage self-witness sink names fired per group.
const SHAPES: &[&str] = &[
    "ruleutils:viewdef",
    "ruleutils:indexdef",
    "ruleutils:constraintdef",
    "ruleutils:funcdef",
    "ruleutils:partkeydef",
    "ruleutils:statsdef",
    "ruleutils:exprdef",
    "ruleutils:formattype",
    "ruleutils:triggerdef",
    "ruleutils:ruledef",
];

/// Wrap-column widths swept on pg_get_viewdef's integer (wrap) overload —
/// 0 disables wrapping; the rest force the line-break path in ruleutils.c at
/// different targets.
const WRAP_WIDTHS: &[i32] = &[0, 10, 40, 78, 200];

pub fn gen_ruleutils_module(g: &mut Gen) -> Vec<StmtKind> {
    let shape = g.weights.pick(g.rng, SHAPES).to_string();
    g.fire(&shape);
    let mut body: Vec<String> = Vec::new();
    match shape.as_str() {
        "ruleutils:viewdef" => gen_viewdef(g, &mut body),
        "ruleutils:indexdef" => gen_indexdef(g, &mut body),
        "ruleutils:constraintdef" => gen_constraintdef(g, &mut body),
        "ruleutils:funcdef" => gen_funcdef(g, &mut body),
        "ruleutils:partkeydef" => gen_partkeydef(g, &mut body),
        "ruleutils:statsdef" => gen_statsdef(g, &mut body),
        "ruleutils:exprdef" => gen_exprdef(g, &mut body),
        "ruleutils:formattype" => gen_formattype(g, &mut body),
        "ruleutils:triggerdef" => gen_triggerdef(g, &mut body),
        "ruleutils:ruledef" => gen_ruledef(g, &mut body),
        other => unreachable!("unknown ruleutils shape {other}"),
    }
    // Wrap the whole group in a throwaway schema: collision-proof and
    // single-statement cleanup no matter what the shape created.
    let mut out: Vec<String> = Vec::with_capacity(body.len() + 3);
    out.push("DROP SCHEMA IF EXISTS ru_s CASCADE;".to_string());
    out.push("CREATE SCHEMA ru_s;".to_string());
    out.extend(body);
    out.push("DROP SCHEMA ru_s CASCADE;".to_string());
    out.into_iter().map(StmtKind::Raw).collect()
}

/// Deterministic subset of `items`: each included with prob 1/2, but never
/// fewer than `min` (falls back to the first `min`). Order preserved.
fn take_some(g: &mut Gen, items: &[&'static str], min: usize) -> Vec<&'static str> {
    let mut chosen: Vec<&'static str> = items.iter().copied().filter(|_| g.rng.chance(1, 2)).collect();
    if chosen.len() < min {
        chosen = items.iter().copied().take(min).collect();
    }
    chosen
}

// --- viewdef -------------------------------------------------------------

const VIEW_BODIES: &[&str] = &[
    "SELECT a, b, a + b AS s, a * 2 - b AS m FROM ru_s.base",
    "SELECT b, count(*) AS n, sum(a) AS sa, avg(d) AS ad FROM ru_s.base GROUP BY b HAVING count(*) > 0",
    "SELECT a, CASE WHEN a > b THEN upper(c) ELSE lower(c) END AS cc, coalesce(c, 'x') AS cx FROM ru_s.base WHERE a >= 0",
    "SELECT a, rank() OVER (PARTITION BY b ORDER BY a DESC) AS rk, d / 3 AS dd FROM ru_s.base",
    "SELECT DISTINCT b, substring(c FROM 1 FOR 3) AS sub, c || '_' || a::text AS ac FROM ru_s.base",
    "SELECT x.a, y.b AS yb FROM ru_s.base x JOIN ru_s.base y ON x.a = y.b WHERE x.a IS NOT NULL",
    "SELECT a, b FROM ru_s.base WHERE a IN (SELECT b FROM ru_s.base WHERE d > 0) ORDER BY a",
    "SELECT a, b, d FROM ru_s.base UNION ALL SELECT b, a, d + 1 FROM ru_s.base",
    "WITH w AS (SELECT a, b FROM ru_s.base WHERE a < b) SELECT w.a, w.b, w.a + w.b AS t FROM w",
    "SELECT a, ARRAY[a, b, a + b] AS arr, (a, b, c, d, e, f)::ru_s.base AS rowval FROM ru_s.base",
];

fn gen_viewdef(g: &mut Gen, out: &mut Vec<String>) {
    out.push(
        "CREATE TABLE ru_s.base (a int, b int, c text, d numeric(10,2), e date, f bool);".to_string(),
    );
    // 2-4 views, each a distinct body pick.
    let nviews = 2 + g.rng.below_usize(3);
    let mut used = vec![false; VIEW_BODIES.len()];
    let mut count = 0;
    for _ in 0..nviews {
        // pick an unused body deterministically
        let start = g.rng.below_usize(VIEW_BODIES.len());
        let mut idx = None;
        for off in 0..VIEW_BODIES.len() {
            let i = (start + off) % VIEW_BODIES.len();
            if !used[i] {
                idx = Some(i);
                break;
            }
        }
        let Some(i) = idx else { break };
        used[i] = true;
        count += 1;
        let orr = if g.rng.chance(1, 3) { "OR REPLACE " } else { "" };
        out.push(format!("CREATE {orr}VIEW ru_s.v{i} AS {};", VIEW_BODIES[i]));
    }
    debug_assert!(count > 0);
    // Deparse sweep: plain, pretty, and the wrap-column overload at a couple
    // of widths. Total ORDER BY relname so the row set is stable.
    let w1 = *g.rng.pick(WRAP_WIDTHS);
    let w2 = *g.rng.pick(WRAP_WIDTHS);
    out.push(format!(
        "SELECT c.relname, pg_get_viewdef(c.oid), pg_get_viewdef(c.oid, true), \
         pg_get_viewdef(c.oid, {w1}), pg_get_viewdef(c.oid, {w2}) \
         FROM pg_class c WHERE c.relnamespace = 'ru_s'::regnamespace AND c.relkind = 'v' \
         ORDER BY c.relname;"
    ));
    // Text-name overload (schema-qualified string, not oid).
    out.push(
        "SELECT pg_get_viewdef('ru_s.v0', true) WHERE to_regclass('ru_s.v0') IS NOT NULL;"
            .to_string(),
    );
    // Idempotence round-trip: deparse one view, re-create from the text, and
    // require the re-deparse to match. A non-idempotent deparse raises here
    // (error divergence between engines).
    if g.weights.pick(g.rng, &["ruleutils:idem", "ruleutils:noidem"]) == "ruleutils:idem" {
        g.fire("ruleutils:idem");
        out.push(
            "DO $rt$ DECLARE src text; v regclass; BEGIN \
             SELECT c.oid::regclass INTO v FROM pg_class c \
             WHERE c.relnamespace = 'ru_s'::regnamespace AND c.relkind = 'v' \
             ORDER BY c.relname LIMIT 1; \
             EXECUTE 'CREATE VIEW ru_s.rt AS ' || pg_get_viewdef(v); \
             IF pg_get_viewdef(v) <> pg_get_viewdef('ru_s.rt'::regclass) THEN \
             RAISE EXCEPTION 'ruleutils viewdef not idempotent'; END IF; END $rt$;"
                .to_string(),
        );
    }
}

// --- indexdef ------------------------------------------------------------

const INDEX_DEFS: &[&str] = &[
    "CREATE INDEX ON ru_s.t (a);",
    "CREATE INDEX ON ru_s.t (b DESC NULLS FIRST, c);",
    "CREATE INDEX ON ru_s.t ((a + b));",
    "CREATE INDEX ON ru_s.t (lower(c)) WHERE a > 0;",
    "CREATE INDEX ON ru_s.t (c text_pattern_ops);",
    "CREATE UNIQUE INDEX ON ru_s.t (a) INCLUDE (b, c);",
    "CREATE INDEX ON ru_s.t (c COLLATE \"C\");",
    "CREATE INDEX ON ru_s.t (d, (a * 2)) WHERE c IS NOT NULL;",
    "CREATE INDEX ON ru_s.t USING hash (b);",
];

fn gen_indexdef(g: &mut Gen, out: &mut Vec<String>) {
    out.push("CREATE TABLE ru_s.t (a int, b int, c text, d numeric(10,2));".to_string());
    for def in take_some(g, INDEX_DEFS, 2) {
        out.push(def.to_string());
    }
    // Full index def (plain + pretty) plus per-column deparse (colno>0).
    out.push(
        "SELECT i.indexrelid::regclass::text AS n, pg_get_indexdef(i.indexrelid), \
         pg_get_indexdef(i.indexrelid, 0, true), \
         pg_get_indexdef(i.indexrelid, 1, false), \
         pg_get_indexdef(i.indexrelid, 2, true) \
         FROM pg_index i WHERE i.indrelid = 'ru_s.t'::regclass \
         ORDER BY n;"
            .to_string(),
    );
}

// --- constraintdef -------------------------------------------------------

fn gen_constraintdef(g: &mut Gen, out: &mut Vec<String>) {
    out.push("CREATE TABLE ru_s.parent (id int PRIMARY KEY, tag text UNIQUE);".to_string());
    // Column/table constraint variety on the child; all builtin (range &&
    // gist needs no extension).
    let mut cols: Vec<String> = vec![
        "a int PRIMARY KEY".to_string(),
        "b int CHECK (b > 0)".to_string(),
        "c int".to_string(),
        "r int4range".to_string(),
        "pid int".to_string(),
    ];
    let mut tcons: Vec<&'static str> = Vec::new();
    tcons.push("UNIQUE (c)");
    tcons.push("CONSTRAINT ck2 CHECK (a + coalesce(c, 0) < 1000)");
    tcons.push("FOREIGN KEY (pid) REFERENCES ru_s.parent (id) ON DELETE CASCADE ON UPDATE SET NULL");
    if g.rng.chance(1, 2) {
        tcons.push("EXCLUDE USING gist (r WITH &&)");
    }
    if g.rng.chance(1, 2) {
        tcons.push("CHECK (b IS NULL OR b < 100) NO INHERIT");
    }
    // Randomly drop the b check to vary the def set.
    if g.rng.chance(1, 3) {
        cols[1] = "b int".to_string();
    }
    let mut defs = cols;
    for t in tcons {
        defs.push(t.to_string());
    }
    out.push(format!("CREATE TABLE ru_s.t ({});", defs.join(", ")));
    out.push(
        "SELECT conname, pg_get_constraintdef(oid), pg_get_constraintdef(oid, true) \
         FROM pg_constraint WHERE conrelid = 'ru_s.t'::regclass ORDER BY conname;"
            .to_string(),
    );
    // Parent-side constraints too (PK + unique).
    out.push(
        "SELECT conname, pg_get_constraintdef(oid, true) \
         FROM pg_constraint WHERE conrelid = 'ru_s.parent'::regclass ORDER BY conname;"
            .to_string(),
    );
}

// --- funcdef -------------------------------------------------------------

const FUNC_DEFS: &[(&str, &str)] = &[
    ("f1", "CREATE FUNCTION ru_s.f1(a int, b int DEFAULT 5) RETURNS int LANGUAGE sql AS 'SELECT a + b';"),
    ("f2", "CREATE FUNCTION ru_s.f2(VARIADIC arr int[]) RETURNS int LANGUAGE sql AS 'SELECT coalesce(array_length(arr, 1), 0)';"),
    ("f3", "CREATE FUNCTION ru_s.f3(IN a int, OUT s int, OUT d int) LANGUAGE sql AS 'SELECT a + 1, a - 1';"),
    ("f4", "CREATE FUNCTION ru_s.f4(a int) RETURNS TABLE(x int, y text) LANGUAGE sql AS 'SELECT a, ''z''';"),
    ("f5", "CREATE FUNCTION ru_s.f5(a numeric(10,2), b varchar(20) DEFAULT 'hi') RETURNS text LANGUAGE sql IMMUTABLE AS 'SELECT b';"),
    ("f6", "CREATE FUNCTION ru_s.f6(anyelement, anyelement) RETURNS anyelement LANGUAGE sql AS 'SELECT $1';"),
    ("f7", "CREATE FUNCTION ru_s.f7(a int = 1, b int = 2) RETURNS int LANGUAGE sql IMMUTABLE STRICT AS 'SELECT a * b';"),
    ("f8", "CREATE FUNCTION ru_s.f8(a int) RETURNS int LANGUAGE plpgsql AS 'BEGIN RETURN a; END';"),
    ("f9", "CREATE FUNCTION ru_s.f9(n int) RETURNS SETOF int LANGUAGE sql AS 'SELECT generate_series(1, n)';"),
    ("fa", "CREATE FUNCTION ru_s.fa(INOUT x int, IN step int DEFAULT 1) LANGUAGE sql AS 'SELECT x + step';"),
];

fn gen_funcdef(g: &mut Gen, out: &mut Vec<String>) {
    let mut count = 0;
    for (_, def) in FUNC_DEFS {
        if g.rng.chance(1, 2) {
            out.push(def.to_string());
            count += 1;
        }
    }
    if count == 0 {
        out.push(FUNC_DEFS[0].1.to_string());
    }
    // The signature-deparse trio (the gap objid's fixed deck barely touches)
    // plus the full functiondef, swept in name order.
    out.push(
        "SELECT p.proname, pg_get_function_arguments(p.oid), pg_get_function_result(p.oid), \
         pg_get_function_identity_arguments(p.oid), pg_get_functiondef(p.oid) \
         FROM pg_proc p WHERE p.pronamespace = 'ru_s'::regnamespace ORDER BY p.proname, p.oid;"
            .to_string(),
    );
}

// --- partkeydef ----------------------------------------------------------

const PART_DEFS: &[&str] = &[
    "CREATE TABLE ru_s.p_range (a int, b text, c date) PARTITION BY RANGE (a);",
    "CREATE TABLE ru_s.p_list (a int, b text) PARTITION BY LIST (b);",
    "CREATE TABLE ru_s.p_hash (a int, b int) PARTITION BY HASH (a);",
    "CREATE TABLE ru_s.p_expr (a int, b int) PARTITION BY RANGE ((a + b), b);",
    "CREATE TABLE ru_s.p_multi (a int, b text, c timestamp) PARTITION BY RANGE (a, b COLLATE \"C\");",
];

fn gen_partkeydef(g: &mut Gen, out: &mut Vec<String>) {
    for def in take_some(g, PART_DEFS, 2) {
        out.push(def.to_string());
    }
    out.push(
        "SELECT c.relname, pg_get_partkeydef(c.oid) FROM pg_class c \
         WHERE c.relnamespace = 'ru_s'::regnamespace AND c.relkind = 'p' ORDER BY c.relname;"
            .to_string(),
    );
}

// --- statsdef ------------------------------------------------------------

const STATS_DEFS: &[&str] = &[
    "CREATE STATISTICS ru_s.s1 (ndistinct, dependencies) ON a, b FROM ru_s.t;",
    "CREATE STATISTICS ru_s.s2 (mcv) ON b, c FROM ru_s.t;",
    "CREATE STATISTICS ru_s.s3 ON (a + b), (a * d) FROM ru_s.t;",
    "CREATE STATISTICS ru_s.s4 (ndistinct) ON a, (lower(c)) FROM ru_s.t;",
    "CREATE STATISTICS ru_s.s5 ON (a + d) FROM ru_s.t;",
];

fn gen_statsdef(g: &mut Gen, out: &mut Vec<String>) {
    out.push("CREATE TABLE ru_s.t (a int, b int, c text, d int);".to_string());
    for def in take_some(g, STATS_DEFS, 2) {
        out.push(def.to_string());
    }
    out.push(
        "SELECT s.stxname, pg_get_statisticsobjdef(s.oid) FROM pg_statistic_ext s \
         WHERE s.stxnamespace = 'ru_s'::regnamespace ORDER BY s.stxname;"
            .to_string(),
    );
}

// --- exprdef (pg_get_expr over pg_attrdef / pg_constraint) ---------------

fn gen_exprdef(_g: &mut Gen, out: &mut Vec<String>) {
    out.push(
        "CREATE TABLE ru_s.t (\
         a int DEFAULT 7, \
         b text DEFAULT upper('x') || '_d', \
         c timestamp DEFAULT '2020-01-01'::timestamp, \
         d int DEFAULT (1 + 2) * 3, \
         e int, \
         g int GENERATED ALWAYS AS (a * 10) STORED, \
         CHECK (a > 0), \
         CONSTRAINT ck2 CHECK (a + coalesce(e, 0) < 100), \
         CHECK (b IS NOT NULL));"
            .to_string(),
    );
    out.push(
        "SELECT adnum, pg_get_expr(adbin, adrelid) FROM pg_attrdef \
         WHERE adrelid = 'ru_s.t'::regclass ORDER BY adnum;"
            .to_string(),
    );
    out.push(
        "SELECT conname, pg_get_expr(conbin, conrelid) FROM pg_constraint \
         WHERE conrelid = 'ru_s.t'::regclass AND conbin IS NOT NULL ORDER BY conname;"
            .to_string(),
    );
}

// --- formattype ----------------------------------------------------------

const FT_COLS: &[&str] = &[
    "c_int int",
    "c_bigint bigint",
    "c_smallint smallint",
    "c_num numeric(10,2)",
    "c_num2 numeric(38,0)",
    "c_num3 numeric",
    "c_vc varchar(20)",
    "c_ch char(5)",
    "c_text text",
    "c_ts timestamp(3)",
    "c_tstz timestamptz(6)",
    "c_time time(2)",
    "c_intv interval hour to second(4)",
    "c_bit bit(8)",
    "c_vbit varbit(16)",
    "c_arr numeric(6,3)[]",
    "c_intarr int[]",
];

fn gen_formattype(g: &mut Gen, out: &mut Vec<String>) {
    let cols = take_some(g, FT_COLS, 4);
    out.push(format!("CREATE TABLE ru_s.ft ({});", cols.join(", ")));
    // format_type over real typmods pulled from the catalog (attnum-ordered).
    out.push(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a \
         WHERE a.attrelid = 'ru_s.ft'::regclass AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum;"
            .to_string(),
    );
    // Direct calls: NULL typmod (unqualified) and explicit typmods + arrays.
    out.push(
        "SELECT format_type('numeric'::regtype, NULL), format_type('int4'::regtype, NULL), \
         format_type('varchar'::regtype, 24), format_type('bpchar'::regtype, 14), \
         format_type('_int4'::regtype, NULL), format_type('timestamp'::regtype, 3);"
            .to_string(),
    );
}

// --- triggerdef ----------------------------------------------------------

const TRIGGER_DEFS: &[&str] = &[
    "CREATE TRIGGER z1 BEFORE INSERT OR UPDATE ON ru_s.t FOR EACH ROW EXECUTE FUNCTION ru_s.trg();",
    "CREATE TRIGGER z2 AFTER UPDATE OF a, b ON ru_s.t FOR EACH ROW WHEN (old.a IS DISTINCT FROM new.a) EXECUTE FUNCTION ru_s.trg();",
    "CREATE TRIGGER z3 BEFORE DELETE ON ru_s.t FOR EACH STATEMENT EXECUTE FUNCTION ru_s.trg();",
    "CREATE CONSTRAINT TRIGGER z4 AFTER INSERT ON ru_s.t DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ru_s.trg();",
    "CREATE TRIGGER z5 AFTER INSERT ON ru_s.t FOR EACH ROW EXECUTE FUNCTION ru_s.trg('x', '42');",
    "CREATE TRIGGER z6 AFTER TRUNCATE ON ru_s.t FOR EACH STATEMENT EXECUTE FUNCTION ru_s.trg();",
];

fn gen_triggerdef(g: &mut Gen, out: &mut Vec<String>) {
    out.push("CREATE TABLE ru_s.t (a int, b int, c text);".to_string());
    out.push(
        "CREATE FUNCTION ru_s.trg() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';"
            .to_string(),
    );
    for def in take_some(g, TRIGGER_DEFS, 2) {
        out.push(def.to_string());
    }
    out.push(
        "SELECT tgname, pg_get_triggerdef(oid), pg_get_triggerdef(oid, true) FROM pg_trigger \
         WHERE tgrelid = 'ru_s.t'::regclass AND NOT tgisinternal ORDER BY tgname;"
            .to_string(),
    );
}

// --- ruledef -------------------------------------------------------------

const RULE_DEFS: &[&str] = &[
    "CREATE RULE r1 AS ON INSERT TO ru_s.t DO ALSO INSERT INTO ru_s.log VALUES (NEW.a, 'ins');",
    "CREATE RULE r2 AS ON UPDATE TO ru_s.t WHERE OLD.a <> NEW.a DO INSTEAD NOTHING;",
    "CREATE RULE r3 AS ON DELETE TO ru_s.t DO ALSO INSERT INTO ru_s.log VALUES (OLD.a, 'del');",
    "CREATE RULE r4 AS ON INSERT TO ru_s.t WHERE NEW.a > 0 DO ALSO NOTHING;",
    "CREATE RULE r5 AS ON UPDATE TO ru_s.t DO ALSO INSERT INTO ru_s.log VALUES (NEW.a, 'upd');",
];

fn gen_ruledef(g: &mut Gen, out: &mut Vec<String>) {
    out.push("CREATE TABLE ru_s.t (a int, b text);".to_string());
    out.push("CREATE TABLE ru_s.log (a int, tag text);".to_string());
    for def in take_some(g, RULE_DEFS, 2) {
        out.push(def.to_string());
    }
    out.push(
        "SELECT rulename, pg_get_ruledef(oid), pg_get_ruledef(oid, true) FROM pg_rewrite \
         WHERE ev_class = 'ru_s.t'::regclass AND rulename <> '_RETURN' ORDER BY rulename;"
            .to_string(),
    );
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
            groups.push(gen_ruleutils_module(&mut g).iter().map(|s| s.to_sql()).collect::<Vec<_>>());
            prods_all.extend(prods);
        }
        (groups, prods_all)
    }

    /// Every group is self-contained: it opens with the schema guard+create
    /// and ends with the CASCADE drop, and every statement is well-formed.
    #[test]
    fn groups_are_self_contained_and_wellformed() {
        let (groups, _) = gen_groups(0x2ACE, 200, &WeightTable::defaults());
        for group in &groups {
            assert_eq!(group.first().unwrap(), "DROP SCHEMA IF EXISTS ru_s CASCADE;");
            assert_eq!(group.get(1).unwrap(), "CREATE SCHEMA ru_s;");
            assert_eq!(group.last().unwrap(), "DROP SCHEMA ru_s CASCADE;");
            for s in group {
                assert!(s.ends_with(';'), "unterminated: {s}");
                assert!(!s.contains('\n'), "multi-line: {s}");
                assert_eq!(
                    s.matches('(').count(),
                    s.matches(')').count(),
                    "unbalanced parens: {s}"
                );
            }
        }
    }

    /// Every registered shape fires under the default weights.
    #[test]
    fn all_shapes_fire() {
        let (_, prods) = gen_groups(0x5A17, 400, &WeightTable::defaults());
        for shape in SHAPES {
            assert!(prods.iter().any(|p| p == shape), "shape {shape} never fired");
        }
    }

    /// Determinism: same seed + weights = byte-identical stream.
    #[test]
    fn ruleutils_is_deterministic() {
        let w = WeightTable::defaults();
        let (a, _) = gen_groups(11, 60, &w);
        let (b, _) = gen_groups(11, 60, &w);
        assert_eq!(a, b);
    }

    /// The deparse sweeps that read multiple catalog rows all carry an
    /// ORDER BY (row-set stability across the two differential engines).
    #[test]
    fn multirow_probes_are_ordered() {
        let (groups, _) = gen_groups(0x0DE7, 200, &WeightTable::defaults());
        for group in &groups {
            for s in group {
                let is_sweep = s.contains("FROM pg_class")
                    || s.contains("FROM pg_index")
                    || s.contains("FROM pg_constraint")
                    || s.contains("FROM pg_proc")
                    || s.contains("FROM pg_attribute")
                    || s.contains("FROM pg_attrdef")
                    || s.contains("FROM pg_statistic_ext")
                    || s.contains("FROM pg_trigger")
                    || s.contains("FROM pg_rewrite");
                if is_sweep {
                    assert!(s.contains("ORDER BY"), "unordered multi-row sweep: {s}");
                }
            }
        }
    }
}
