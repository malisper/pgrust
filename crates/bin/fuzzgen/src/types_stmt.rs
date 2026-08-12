//! T1 statement-level type shapes: set-returning sources in FROM (unnest,
//! jsonb_each/array_elements, regexp_split_to_table) over LITERAL inputs,
//! plus the G2 jsonfuncs SRF tail: column-driven jsonb_each/_text,
//! jsonb_array_elements/_text, jsonb_object_keys / json_object_keys in
//! LATERAL over fixture jsonb/json columns, and the record-materializing
//! family (jsonb_to_record(set) AS typed columns,
//! jsonb_populate_record(set) over a catalog rowtype).
//!
//! The legacy families are emitted as `StmtKind::Raw` with literal-only
//! sources and no ORDER BY (the differ's multiset compare covers unordered
//! SRF output — SRF emission order is exactly the surface under test).
//! The G2 column-driven families reference catalog tables in LATERAL and
//! are ORDER-BY-wrapped over every output column for determinism (rows
//! multiply across the driving table, so tie-order would otherwise leak);
//! they filter on jsonb_typeof/json_typeof so mixed fixture values (scalar
//! /array/object rows) feed the matching deconstructor instead of turning
//! the whole statement into a both-side error. All families hand-verified
//! byte-identical on both engines (G2 deck 5, 2026-08-11).

use crate::catalog::SqlType;
use crate::stmt::{Gen, StmtKind};

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_types_module(g: &mut Gen) -> Vec<StmtKind> {
    let sql = match g.weights.pick(
        g.rng,
        &[
            "types:unnest",
            "types:unnest_ord",
            "types:jsonb_each",
            "types:jsonb_elements",
            "types:split_table",
            "types:each_col",
            "types:elements_col",
            "types:object_keys",
            "types:recordset",
            "types:populate",
        ],
    ) {
        "types:unnest" => gen_unnest(g, false),
        "types:unnest_ord" => gen_unnest(g, true),
        "types:jsonb_each" => gen_jsonb_each(g),
        "types:jsonb_elements" => gen_jsonb_elements(g),
        "types:split_table" => gen_split_table(g),
        "types:each_col" => gen_each_col(g),
        "types:elements_col" => gen_elements_col(g),
        "types:object_keys" => gen_object_keys(g),
        "types:recordset" => gen_recordset(g),
        _ => gen_populate(g),
    };
    vec![StmtKind::Raw(sql)]
}

fn gen_unnest(g: &mut Gen, with_ordinality: bool) -> String {
    g.fire(if with_ordinality { "types:unnest_ord" } else { "types:unnest" });
    let ty = if g.rng.chance(1, 2) { SqlType::Int4Arr } else { SqlType::TextArr };
    let arr = g.gen_literal(ty);
    let alias = g.next_alias();
    if with_ordinality {
        format!(
            "SELECT {a}.x, {a}.o FROM unnest({arr}) WITH ORDINALITY AS {a}(x, o);",
            a = alias,
            arr = arr
        )
    } else {
        format!("SELECT {a}.x FROM unnest({arr}) AS {a}(x);", a = alias, arr = arr)
    }
}

fn gen_jsonb_each(g: &mut Gen) -> String {
    g.fire("types:jsonb_each");
    let j = g.gen_literal(SqlType::Jsonb);
    let alias = g.next_alias();
    let f = if g.rng.chance(1, 2) { "jsonb_each" } else { "jsonb_each_text" };
    format!("SELECT {a}.k, {a}.v FROM {f}({j}) AS {a}(k, v);", a = alias, f = f, j = j)
}

fn gen_jsonb_elements(g: &mut Gen) -> String {
    g.fire("types:jsonb_elements");
    let j = g.gen_literal(SqlType::Jsonb);
    let alias = g.next_alias();
    let f = if g.rng.chance(1, 2) {
        "jsonb_array_elements"
    } else {
        "jsonb_array_elements_text"
    };
    format!("SELECT {a}.e FROM {f}({j}) AS {a}(e);", a = alias, f = f, j = j)
}

fn gen_split_table(g: &mut Gen) -> String {
    g.fire("types:split_table");
    let text = g.gen_literal(SqlType::Text);
    let pat = g.gen_regex_pattern();
    let alias = g.next_alias();
    format!(
        "SELECT {a}.s FROM regexp_split_to_table({t}, '{p}') AS {a}(s);",
        a = alias,
        t = text,
        p = pat
    )
}

// ------------------------------------------------------------------ G2 ----

/// A (table, pk column, json/jsonb column) triple from the current catalog
/// for the column-driven SRF families. `ty` is Jsonb or Json. None when no
/// catalog table carries both a pk and a column of that type — the caller
/// falls back to a literal-driven form (live catalogs, hostile DDL).
fn pick_json_col(g: &mut Gen, ty: SqlType) -> Option<(String, String, String)> {
    let cands: Vec<(String, String, String)> = g
        .catalog
        .tables
        .iter()
        .filter(|t| t.pk.is_some())
        .flat_map(|t| {
            let pk = t.pk.as_ref().unwrap().column.clone();
            let name = t.name.clone();
            t.columns
                .iter()
                .filter(|c| c.ty == ty)
                .map(move |c| (name.clone(), pk.clone(), c.name.clone()))
                .collect::<Vec<_>>()
        })
        .collect();
    if cands.is_empty() {
        None
    } else {
        Some(cands[g.rng.below_usize(cands.len())].clone())
    }
}

/// jsonb_each / jsonb_each_text in LATERAL over a fixture jsonb column,
/// object-filtered, fully ordered.
fn gen_each_col(g: &mut Gen) -> String {
    let Some((tbl, pk, col)) = pick_json_col(g, SqlType::Jsonb) else {
        return gen_jsonb_each(g);
    };
    g.fire("types:each_col");
    let f = if g.rng.chance(1, 2) { "jsonb_each" } else { "jsonb_each_text" };
    let a = g.next_alias();
    let s = g.next_alias();
    format!(
        "SELECT {a}.{pk}, {s}.k, {s}.v FROM {tbl} {a}, LATERAL {f}({a}.{col}) {s}(k, v) \
         WHERE jsonb_typeof({a}.{col}) = 'object' ORDER BY {a}.{pk}, {s}.k, {s}.v;",
    )
}

/// jsonb_array_elements / _text in LATERAL, array-filtered, fully ordered.
fn gen_elements_col(g: &mut Gen) -> String {
    let Some((tbl, pk, col)) = pick_json_col(g, SqlType::Jsonb) else {
        return gen_jsonb_elements(g);
    };
    g.fire("types:elements_col");
    let f = if g.rng.chance(1, 2) {
        "jsonb_array_elements"
    } else {
        "jsonb_array_elements_text"
    };
    let a = g.next_alias();
    let s = g.next_alias();
    format!(
        "SELECT {a}.{pk}, {s}.e FROM {tbl} {a}, LATERAL {f}({a}.{col}) {s}(e) \
         WHERE jsonb_typeof({a}.{col}) = 'array' ORDER BY {a}.{pk}, {s}.e;",
    )
}

/// jsonb_object_keys (or the json twin over a json column), object-filtered,
/// fully ordered.
fn gen_object_keys(g: &mut Gen) -> String {
    // 1/3 of draws try the json (not jsonb) twin — shared jsonfuncs guts,
    // different iterator entry.
    let json_twin = g.rng.chance(1, 3);
    let ty = if json_twin { SqlType::Json } else { SqlType::Jsonb };
    let Some((tbl, pk, col)) = pick_json_col(g, ty) else {
        return gen_jsonb_each(g);
    };
    g.fire("types:object_keys");
    let (f, tf) = if json_twin {
        ("json_object_keys", "json_typeof")
    } else {
        ("jsonb_object_keys", "jsonb_typeof")
    };
    let a = g.next_alias();
    let s = g.next_alias();
    format!(
        "SELECT {a}.{pk}, {s}.k FROM {tbl} {a}, LATERAL {f}({a}.{col}) {s}(k) \
         WHERE {tf}({a}.{col}) = 'object' ORDER BY {a}.{pk}, {s}.k;",
    )
}

/// Deterministic jsonb array-of-objects literals for the recordset family:
/// missing keys, extra keys, nulls, nested values, empty array.
const RECORDSET_DOCS: &[&str] = &[
    r#"[{"a":1,"b":"x"},{"a":2},{"b":"y","c":9}]"#,
    r#"[{"a":null,"b":null},{"a":-5,"b":""}]"#,
    r#"[{"a":7,"b":"z","extra":{"deep":true}}]"#,
    r#"[]"#,
];

/// jsonb_to_recordset AS (typed cols) — or the single-row jsonb_to_record.
fn gen_recordset(g: &mut Gen) -> String {
    g.fire("types:recordset");
    let a = g.next_alias();
    if g.rng.chance(1, 3) {
        // Single-record form (one row; no ordering needed).
        let doc = *g
            .rng
            .pick(&[r#"{"a":1,"b":"x"}"#, r#"{"a":null,"extra":true}"#, r#"{}"#]);
        format!(
            "SELECT {a}.a, {a}.b FROM jsonb_to_record('{doc}'::jsonb) AS {a}(a int, b text);",
        )
    } else {
        let doc = *g.rng.pick(RECORDSET_DOCS);
        format!(
            "SELECT {a}.a, {a}.b FROM jsonb_to_recordset('{doc}'::jsonb) AS {a}(a int, b text) \
             ORDER BY {a}.a NULLS LAST, {a}.b NULLS LAST;",
        )
    }
}

/// jsonb_populate_record(set) over a catalog rowtype: keys drawn from the
/// table's own int/text column names (so values actually land), plus the
/// all-miss document (every field NULL). Output columns are the drawn keys,
/// fully ordered for the set form.
fn gen_populate(g: &mut Gen) -> String {
    // Any pk-carrying catalog table works as the rowtype; its int4/text
    // columns are the key pool.
    let cands: Vec<(String, Vec<(String, bool)>)> = g
        .catalog
        .tables
        .iter()
        .filter(|t| t.pk.is_some())
        .map(|t| {
            (
                t.name.clone(),
                t.columns
                    .iter()
                    .filter(|c| matches!(c.ty, SqlType::Int4 | SqlType::Text))
                    .map(|c| (c.name.clone(), c.ty == SqlType::Int4))
                    .collect::<Vec<_>>(),
            )
        })
        .filter(|(_, cols)| !cols.is_empty())
        .collect();
    if cands.is_empty() {
        return gen_jsonb_each(g);
    }
    let (tbl, cols) = cands[g.rng.below_usize(cands.len())].clone();
    g.fire("types:populate");
    // 1-2 distinct key columns.
    let c1 = cols[g.rng.below_usize(cols.len())].clone();
    let c2 = cols[g.rng.below_usize(cols.len())].clone();
    let sel: Vec<&(String, bool)> = if c2.0 == c1.0 { vec![&c1] } else { vec![&c1, &c2] };
    // Values are type-matched (an int-typed field never draws a text
    // value): populate's per-field cast errors would otherwise eat
    // both-side-error budget on every fourth draw.
    let field = |g: &mut Gen, c: &(String, bool)| -> String {
        match g.rng.below(4) {
            0 if c.1 => format!("\"{}\":{}", c.0, g.rng.below(9)),
            0 => format!("\"{}\":\"t{}\"", c.0, g.rng.below(4)),
            1 if c.1 => format!("\"{}\":\"{}\"", c.0, g.rng.below(9)),
            1 => format!("\"{}\":\"\"", c.0),
            2 => format!("\"{}\":null", c.0),
            // Key miss: a name no column has.
            _ => format!("\"zz_miss\":{}", g.rng.below(9)),
        }
    };
    let a = g.next_alias();
    let sel_list =
        sel.iter().map(|c| format!("{a}.{c}", c = c.0)).collect::<Vec<_>>().join(", ");
    if g.rng.chance(1, 2) {
        let mut doc = String::from("{");
        for (i, c) in sel.iter().enumerate() {
            if i > 0 {
                doc.push(',');
            }
            doc.push_str(&field(g, c));
        }
        doc.push('}');
        format!(
            "SELECT {sel_list} FROM jsonb_populate_record(NULL::{tbl}, '{doc}'::jsonb) {a};",
        )
    } else {
        // Set form: 2-element array, fully ordered output.
        let mut docs = Vec::new();
        for _ in 0..2 {
            let mut doc = String::from("{");
            for (i, c) in sel.iter().enumerate() {
                if i > 0 {
                    doc.push(',');
                }
                doc.push_str(&field(g, c));
            }
            doc.push('}');
            docs.push(doc);
        }
        let order = sel
            .iter()
            .map(|c| format!("{a}.{c} NULLS LAST", c = c.0))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT {sel_list} FROM jsonb_populate_recordset(NULL::{tbl}, '[{}]'::jsonb) {a} \
             ORDER BY {order};",
            docs.join(","),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_many(seed: u64, n: usize, w: &WeightTable) -> (Vec<String>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut rng = Rng::new(seed);
        let mut sqls = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, w, &mut prods, 3);
            let stmts = gen_types_module(&mut g);
            assert_eq!(stmts.len(), 1);
            sqls.push(stmts[0].to_sql());
            prods_all.extend(prods);
        }
        (sqls, prods_all)
    }

    #[test]
    fn legacy_families_are_literal_only_and_orderless() {
        // Pin the legacy families on; the G2 column-driven families off.
        let w = WeightTable::parse(
            "types:each_col=0,types:elements_col=0,types:object_keys=0,\
             types:recordset=0,types:populate=0",
        )
        .unwrap();
        let (sqls, _) = gen_many(0x71, 300, &w);
        for sql in &sqls {
            assert!(sql.starts_with("SELECT "), "{sql}");
            assert!(!sql.contains("ORDER BY"), "SRF statement carries ORDER BY: {sql}");
            // Literal-only: no fixture table is ever referenced.
            for t in ["fz_scalar", "fz_mixed", "fz_wide", "fz_rich", "fz_one", "fz_empty"] {
                assert!(!sql.contains(t), "table ref in types module: {sql}");
            }
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
        }
    }

    #[test]
    fn g2_column_families_are_filtered_and_ordered() {
        // Pin the G2 families on.
        let w = WeightTable::parse(
            "types:unnest=0,types:unnest_ord=0,types:jsonb_each=0,\
             types:jsonb_elements=0,types:split_table=0",
        )
        .unwrap();
        let (sqls, prods) = gen_many(0xB2, 600, &w);
        for sql in &sqls {
            assert!(sql.starts_with("SELECT "), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
            // Column-driven LATERAL forms: typeof-filtered and fully
            // ordered (rows multiply across the driving table).
            if sql.contains(" LATERAL ") {
                assert!(
                    sql.contains("_typeof(") && sql.contains(" ORDER BY "),
                    "unfiltered/unordered lateral SRF: {sql}"
                );
            }
            // Set-returning record forms are ordered too.
            if sql.contains("jsonb_to_recordset") || sql.contains("populate_recordset") {
                assert!(sql.contains(" ORDER BY "), "unordered recordset: {sql}");
            }
            // No engine-computed floats anywhere near jsonb documents.
            assert!(!sql.contains("float"), "{sql}");
        }
        for p in [
            "types:each_col",
            "types:elements_col",
            "types:object_keys",
            "types:recordset",
            "types:populate",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    #[test]
    fn types_module_is_deterministic() {
        let w = WeightTable::defaults();
        let (a, _) = gen_many(42, 80, &w);
        let (b, _) = gen_many(42, 80, &w);
        assert_eq!(a, b);
        let (c, _) = gen_many(43, 80, &w);
        assert_ne!(a, c);
    }
}
