//! MERGE statement module: `MERGE INTO <target> USING <source> ON <join>`
//! with mixed WHEN arms — `WHEN MATCHED [AND cond] THEN
//! {UPDATE SET|DELETE|DO NOTHING}`, `WHEN NOT MATCHED [BY TARGET] [AND
//! cond] THEN {INSERT|DO NOTHING}`, `WHEN NOT MATCHED BY SOURCE [AND cond]
//! THEN {UPDATE SET|DELETE|DO NOTHING}` (PG17 surface, hand-verified on
//! both engines 2026-08-11), and PG17 `RETURNING` with `merge_action()`.
//! Targets `ExecMergeMatched` and the rest of the MERGE executor surface
//! (docs/fuzzing/gap-report-003.md rank 25).
//!
//! Sources: a VALUES list (`(VALUES ...) AS sN(sk, sv, st)` — key control
//! down to the row), a plain table, or a bounded subquery over a table.
//! The join is always pinned `target.<pk> = <source key>` so a target row
//! can join at most one source row per distinct key — MATCHED actions stay
//! deterministic. The one deliberate exception is the cardinality shape:
//! with low weight the VALUES source repeats a known pk twice under an
//! unconditional MATCHED UPDATE/DELETE arm, so both engines must raise
//! the same 21000 "MERGE command cannot affect row a second time" error
//! (a prime differential surface; if the duplicated row turns out deleted,
//! both sides instead insert twice -> the same 23505 — still matched fuel).
//!
//! Determinism discipline (inherits crate::dml's; probes compare strictly):
//!   - written float columns get depth-0 leaves via `dml::value_expr`;
//!   - the pk column is never SET (dml::pick_set_columns) and INSERT arms
//!     always write the source join key into the pk column — a NOT MATCHED
//!     key can only 23505 when an extra ON conjunct hid an existing row
//!     (both-side error fuel, deterministic);
//!   - NOT MATCHED BY SOURCE UPDATE/DELETE always carry a pk-bound AND
//!     guard (an unguarded arm acts on every unmatched target row — the
//!     DELETE form would nuke the table and starve the stream);
//!   - NOT MATCHED BY SOURCE / NOT MATCHED INSERT arms reference only the
//!     scopes PostgreSQL permits (target-only / source-only respectively);
//!   - fresh source keys draw through the session DmlState allocator, so
//!     an INSERT arm's new pks can never collide with dml-module inserts.

use crate::catalog::{Column, SqlType};
use crate::dml::{eligible_tables, insert_columns, pick_set_columns, table_of, value_expr};
use crate::scope::{Scope, ScopeRel};
use crate::stmt::{gen_expr_stmt, Gen, StmtKind};

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_merge_module(g: &mut Gen) -> Vec<StmtKind> {
    match gen_merge_sql(g) {
        Some(sql) => vec![StmtKind::Raw(sql)],
        // No MERGE-eligible table (live catalogs carry no pk metadata):
        // degrade to a plain read so the module always produces.
        None => {
            g.fire("merge:fallback_select");
            vec![StmtKind::Select(Box::new(gen_expr_stmt(g)))]
        }
    }
}

/// The USING item: its rendered SQL, the scope relation it exposes, and
/// the alias-qualified int4 join-key column.
struct Source {
    using_sql: String,
    rel: ScopeRel,
    key: String,
}

fn int4_col(name: &str, nullable: bool) -> Column {
    Column { name: name.to_string(), ty: SqlType::Int4, nullable, ddl_type: None }
}

fn text_col(name: &str) -> Column {
    Column { name: name.to_string(), ty: SqlType::Text, nullable: true, ddl_type: None }
}

/// One MERGE statement. None when the catalog exposes no eligible target
/// (pk-carrying, multi-row, at least one non-pk column for UPDATE arms).
pub fn gen_merge_sql(g: &mut Gen) -> Option<String> {
    let eligible: Vec<usize> = eligible_tables(g)
        .into_iter()
        .filter(|&ti| {
            let t = table_of(g, ti);
            t.columns.iter().any(|c| !t.is_pk_column(&c.name))
        })
        .collect();
    if eligible.is_empty() {
        return None;
    }
    let ti = eligible[g.rng.below_usize(eligible.len())];
    let table = table_of(g, ti);
    let pk_col = table.pk.as_ref().expect("eligible table has pk").column.clone();
    g.fire("merge");

    let t_alias = g.next_alias();

    // Source form; the cardinality shape only exists for VALUES sources
    // (only there are duplicate join keys constructible on purpose).
    let src_kind = g
        .weights
        .pick(g.rng, &["merge:src:values", "merge:src:table", "merge:src:subq"]);
    g.fire(src_kind);
    let dup_key = if src_kind == "merge:src:values"
        && g.weights.pick(g.rng, &["merge:card:dup", "merge:card:none"]) == "merge:card:dup"
    {
        g.pk_known(ti)
    } else {
        None
    };
    if dup_key.is_some() {
        g.fire("merge:card:dup");
    }
    let source = match src_kind {
        "merge:src:values" => gen_values_source(g, ti, dup_key),
        "merge:src:table" => gen_table_source(g),
        _ => gen_subq_source(g),
    };

    let mut out = format!(
        "MERGE INTO {} AS {} USING {} ON {}.{} = {}",
        table.name, t_alias, source.using_sql, t_alias, pk_col, source.key
    );

    // Scopes: both relations (ON, MATCHED arms, RETURNING), source-only
    // (NOT MATCHED INSERT), target-only (NOT MATCHED BY SOURCE).
    let target_rel = ScopeRel::from_table(table, t_alias.clone());
    let both_rels = vec![target_rel.clone(), source.rel.clone()];
    let source_rels = vec![source.rel.clone()];
    let target_rels = vec![target_rel];

    if g.weights.pick(g.rng, &["merge:on:extra", "merge:on:plain"]) == "merge:on:extra" {
        g.fire("merge:on:extra");
        let scope = Scope { rels: &both_rels, outer: None };
        out.push_str(" AND ");
        out.push_str(&g.gen_bool(&scope, 2).to_sql());
    }

    // WHEN MATCHED arms. Under the cardinality shape there is exactly one,
    // and it is an unconditional UPDATE/DELETE so the 21000 must fire (a
    // second MATCHED arm behind an unconditional one is a 42601
    // "unreachable WHEN clause" — wasted budget).
    let n_matched = if dup_key.is_some() {
        // Burn the draw so dup/non-dup streams stay aligned per-choice.
        let _ = g
            .weights
            .pick(g.rng, &["merge:matched:0", "merge:matched:1", "merge:matched:2"]);
        1
    } else {
        match g
            .weights
            .pick(g.rng, &["merge:matched:0", "merge:matched:1", "merge:matched:2"])
        {
            "merge:matched:0" => 0,
            "merge:matched:1" => 1,
            _ => 2,
        }
    };
    let mut matched_arms: Vec<String> = Vec::new();
    for i in 0..n_matched {
        let forced_hit = dup_key.is_some() && i == 0;
        let action = if forced_hit {
            g.weights.pick(g.rng, &["merge:m:update", "merge:m:delete"])
        } else {
            g.weights
                .pick(g.rng, &["merge:m:update", "merge:m:delete", "merge:m:nothing"])
        };
        g.fire(action);
        // Non-last arms always carry AND (otherwise the later arm is dead);
        // the last arm occasionally does too.
        let mut arm = String::from("WHEN MATCHED");
        if !forced_hit && (i + 1 < n_matched || g.rng.chance(1, 3)) {
            g.fire("merge:m:cond");
            let scope = Scope { rels: &both_rels, outer: None };
            arm.push_str(" AND ");
            arm.push_str(&g.gen_bool(&scope, 2).to_sql());
        }
        arm.push_str(" THEN ");
        match action {
            "merge:m:update" => {
                arm.push_str("UPDATE SET ");
                let set_cols = pick_set_columns(g, table, 2);
                let scope = Scope { rels: &both_rels, outer: None };
                for (j, c) in set_cols.iter().enumerate() {
                    if j > 0 {
                        arm.push_str(", ");
                    }
                    arm.push_str(&c.name);
                    arm.push_str(" = ");
                    arm.push_str(&value_expr(g, &scope, c));
                }
            }
            "merge:m:delete" => arm.push_str("DELETE"),
            _ => arm.push_str("DO NOTHING"),
        }
        matched_arms.push(arm);
    }

    // WHEN NOT MATCHED [BY TARGET] arm.
    let nmt_pick = g
        .weights
        .pick(g.rng, &["merge:nmt:insert", "merge:nmt:nothing", "merge:nmt:none"]);
    let mut nmt_arm: Option<String> = None;
    if nmt_pick != "merge:nmt:none" {
        g.fire(nmt_pick);
        let mut arm = String::from("WHEN NOT MATCHED");
        if g.weights.pick(g.rng, &["merge:nmt:by_target", "merge:nmt:plain"])
            == "merge:nmt:by_target"
        {
            g.fire("merge:nmt:by_target");
            arm.push_str(" BY TARGET");
        }
        if g.rng.chance(1, 4) {
            g.fire("merge:nmt:cond");
            let scope = Scope { rels: &source_rels, outer: None };
            arm.push_str(" AND ");
            arm.push_str(&g.gen_bool(&scope, 2).to_sql());
        }
        arm.push_str(" THEN ");
        if nmt_pick == "merge:nmt:insert" {
            let cols = insert_columns(g, table);
            arm.push_str("INSERT (");
            for (j, c) in cols.iter().enumerate() {
                if j > 0 {
                    arm.push_str(", ");
                }
                arm.push_str(&c.name);
            }
            arm.push_str(") VALUES (");
            let scope = Scope { rels: &source_rels, outer: None };
            for (j, c) in cols.iter().enumerate() {
                if j > 0 {
                    arm.push_str(", ");
                }
                if table.is_pk_column(&c.name) {
                    // Always the join key: a NOT MATCHED key is absent from
                    // the target (barring an extra-ON hide), so pk stays
                    // collision-free and deterministic.
                    arm.push_str(&source.key);
                } else {
                    arm.push_str(&value_expr(g, &scope, c));
                }
            }
            arm.push(')');
        } else {
            arm.push_str("DO NOTHING");
        }
        nmt_arm = Some(arm);
    }

    // WHEN NOT MATCHED BY SOURCE arm (PG17). UPDATE/DELETE always carry a
    // pk-bound guard (see module docs).
    let nmbs_pick = g.weights.pick(
        g.rng,
        &["merge:nmbs:update", "merge:nmbs:delete", "merge:nmbs:nothing", "merge:nmbs:none"],
    );
    let mut nmbs_arm: Option<String> = None;
    if nmbs_pick != "merge:nmbs:none" {
        g.fire(nmbs_pick);
        let mut arm = String::from("WHEN NOT MATCHED BY SOURCE");
        if nmbs_pick != "merge:nmbs:nothing" {
            let k = g.pk_known(ti).unwrap_or(1);
            let op = if g.rng.chance(1, 3) { "<=" } else { "=" };
            arm.push_str(&format!(" AND {}.{} {} {}", t_alias, pk_col, op, k));
        }
        arm.push_str(" THEN ");
        match nmbs_pick {
            "merge:nmbs:update" => {
                arm.push_str("UPDATE SET ");
                let set_cols = pick_set_columns(g, table, 2);
                let scope = Scope { rels: &target_rels, outer: None };
                for (j, c) in set_cols.iter().enumerate() {
                    if j > 0 {
                        arm.push_str(", ");
                    }
                    arm.push_str(&c.name);
                    arm.push_str(" = ");
                    arm.push_str(&value_expr(g, &scope, c));
                }
            }
            "merge:nmbs:delete" => arm.push_str("DELETE"),
            _ => arm.push_str("DO NOTHING"),
        }
        nmbs_arm = Some(arm);
    }

    // Assemble the arm list; at least one WHEN clause is required, so an
    // all-none draw forces an unconditional MATCHED UPDATE. Clause order
    // is semantic (first matching arm wins per row): occasionally emit the
    // NOT MATCHED arm before the MATCHED arms to exercise ordering.
    if matched_arms.is_empty() && nmt_arm.is_none() && nmbs_arm.is_none() {
        g.fire("merge:m:forced");
        let mut arm = String::from("WHEN MATCHED THEN UPDATE SET ");
        let set_cols = pick_set_columns(g, table, 2);
        let scope = Scope { rels: &both_rels, outer: None };
        for (j, c) in set_cols.iter().enumerate() {
            if j > 0 {
                arm.push_str(", ");
            }
            arm.push_str(&c.name);
            arm.push_str(" = ");
            arm.push_str(&value_expr(g, &scope, c));
        }
        matched_arms.push(arm);
    }
    let mut arms: Vec<String> = Vec::new();
    if nmt_arm.is_some() && !matched_arms.is_empty() && g.rng.chance(1, 4) {
        g.fire("merge:order:nmt_first");
        arms.push(nmt_arm.take().unwrap());
    }
    arms.extend(matched_arms);
    arms.extend(nmt_arm);
    arms.extend(nmbs_arm);
    for arm in &arms {
        out.push(' ');
        out.push_str(arm);
    }

    // PG17 RETURNING, sometimes leading with merge_action(). Row order is
    // plan-dependent, but the differ compares non-ORDER BY rowsets as
    // multisets, so this is compare-safe.
    if g.weights.pick(g.rng, &["merge:returning", "merge:returning:none"]) == "merge:returning" {
        g.fire("merge:returning");
        out.push_str(" RETURNING ");
        let scope = Scope { rels: &both_rels, outer: None };
        let mut items: Vec<String> = Vec::new();
        if g.rng.chance(1, 2) {
            g.fire("merge:returning:action");
            items.push("merge_action()".to_string());
        }
        let n_exprs = if items.is_empty() { 1 + g.rng.below_usize(2) } else { g.rng.below_usize(2) };
        for _ in 0..n_exprs {
            let ty = g.any_type(&scope);
            items.push(g.gen_typed(&scope, ty, 2).to_sql());
        }
        out.push_str(&items.join(", "));
    }

    out.push(';');
    Some(out)
}

/// `(VALUES (k, v, t), ...) AS sN(sk, sv, st)`: 1-3 rows, keys drawn
/// known/fresh through the session DmlState (statement-unique unless the
/// cardinality shape supplies `dup_key`, which is emitted twice). The first
/// row's payload cells carry explicit casts so the column types are pinned
/// regardless of literal spelling.
fn gen_values_source(g: &mut Gen, ti: usize, dup_key: Option<i64>) -> Source {
    let alias = g.next_alias();
    let mut keys: Vec<i64> = Vec::new();
    if let Some(k) = dup_key {
        keys.push(k);
        keys.push(k);
    }
    let extra = match g.weights.pick(g.rng, &["merge:rows:1", "merge:rows:2", "merge:rows:3"]) {
        "merge:rows:1" => 1,
        "merge:rows:2" => 2,
        _ => 3,
    };
    for _ in 0..extra {
        if keys.len() >= 4 {
            break;
        }
        let pick = g.weights.pick(g.rng, &["merge:key:known", "merge:key:fresh"]);
        let k = if pick == "merge:key:known" {
            match g.pk_known(ti) {
                Some(k) if !keys.contains(&k) => {
                    g.fire("merge:key:known");
                    k
                }
                _ => g.pk_fresh(ti),
            }
        } else {
            g.fire("merge:key:fresh");
            g.pk_fresh(ti)
        };
        keys.push(k);
    }
    let mut sql = String::from("(VALUES ");
    for (i, k) in keys.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let v = g.gen_literal(SqlType::Int4);
        let t = g.gen_literal(SqlType::Text);
        if i == 0 {
            sql.push_str(&format!("({}, ({})::int4, ({})::text)", k, v, t));
        } else {
            sql.push_str(&format!("({}, {}, {})", k, v, t));
        }
    }
    sql.push_str(&format!(") AS {} (sk, sv, st)", alias));
    let rel = ScopeRel {
        alias: alias.clone(),
        columns: vec![int4_col("sk", false), int4_col("sv", true), text_col("st")],
    };
    Source { using_sql: sql, rel, key: format!("{}.sk", alias) }
}

/// A plain table source: any pk-carrying table, joined on its pk (unique
/// per target row by construction).
fn gen_table_source(g: &mut Gen) -> Source {
    let sources: Vec<usize> = g
        .catalog
        .tables
        .iter()
        .enumerate()
        .filter(|(_, t)| t.pk.is_some())
        .map(|(i, _)| i)
        .collect();
    let sti = sources[g.rng.below_usize(sources.len())];
    let src = table_of(g, sti);
    let src_pk = &src.pk.as_ref().expect("source table has pk").column;
    let alias = g.next_alias();
    Source {
        using_sql: format!("{} AS {}", src.name, alias),
        rel: ScopeRel::from_table(src, alias.clone()),
        key: format!("{}.{}", alias, src_pk),
    }
}

/// A bounded subquery source: `(SELECT <pk> AS sk, <int4 col> AS sv FROM
/// <src> WHERE <pk> <= <bound>) AS sN`. The pk projection keeps the join
/// key unique; the bound keeps the row count controlled as tables grow.
fn gen_subq_source(g: &mut Gen) -> Source {
    let sources: Vec<usize> = g
        .catalog
        .tables
        .iter()
        .enumerate()
        .filter(|(_, t)| t.pk.is_some())
        .map(|(i, _)| i)
        .collect();
    let sti = sources[g.rng.below_usize(sources.len())];
    let src = table_of(g, sti);
    let src_pk = src.pk.as_ref().expect("source table has pk").column.clone();
    let inner = g.next_alias();
    let alias = g.next_alias();
    let int_cols = src.columns_of_type(SqlType::Int4);
    let sv = &int_cols[g.rng.below_usize(int_cols.len())].name;
    let bound = g.pk_known(sti).unwrap_or_else(|| 1 + g.rng.below(8) as i64);
    let sql = format!(
        "(SELECT {inner}.{src_pk} AS sk, {inner}.{sv} AS sv FROM {} AS {inner} \
         WHERE {inner}.{src_pk} <= {bound}) AS {alias}",
        src.name
    );
    let rel = ScopeRel {
        alias: alias.clone(),
        columns: vec![int4_col("sk", false), int4_col("sv", true)],
    };
    Source { using_sql: sql, rel, key: format!("{}.sk", alias) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::dml::DmlState;
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_many(seed: u64, n: usize, spec: &str) -> (Vec<String>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut sqls = Vec::new();
        let mut prods = Vec::new();
        let mut state: Option<DmlState> = None;
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            if let Some(s) = state.take() {
                g.dml = s;
            }
            let sql = gen_merge_sql(&mut g).expect("fixture has eligible tables");
            state = Some(std::mem::replace(&mut g.dml, DmlState::new(&cat)));
            sqls.push(sql);
            prods.extend(p);
        }
        (sqls, prods)
    }

    #[test]
    fn merge_is_deterministic() {
        let (a, _) = gen_many(21, 50, "");
        let (b, _) = gen_many(21, 50, "");
        assert_eq!(a, b);
        let (c, _) = gen_many(22, 50, "");
        assert_ne!(a, c);
    }

    #[test]
    fn merge_variety_and_invariants() {
        let (sqls, prods) = gen_many(0xE86, 900, "");
        let all = sqls.join("\n");
        for frag in [
            "MERGE INTO ",
            " USING (VALUES ",
            " USING (SELECT ",
            "WHEN MATCHED THEN ",
            "WHEN MATCHED AND ",
            "THEN UPDATE SET ",
            "THEN DELETE",
            "THEN DO NOTHING",
            "WHEN NOT MATCHED BY TARGET",
            "WHEN NOT MATCHED BY SOURCE",
            "THEN INSERT (",
            " RETURNING ",
            "merge_action()",
        ] {
            assert!(all.contains(frag), "MERGE flavor {frag:?} never generated");
        }
        for p in [
            "merge",
            "merge:src:values",
            "merge:src:table",
            "merge:src:subq",
            "merge:card:dup",
            "merge:on:extra",
            "merge:m:update",
            "merge:m:delete",
            "merge:m:nothing",
            "merge:m:cond",
            "merge:nmt:insert",
            "merge:nmt:nothing",
            "merge:nmt:by_target",
            "merge:nmbs:update",
            "merge:nmbs:delete",
            "merge:nmbs:nothing",
            "merge:key:known",
            "merge:key:fresh",
            "merge:order:nmt_first",
            "merge:returning",
            "merge:returning:action",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        for sql in &sqls {
            // Never target the single-row/empty fixtures.
            for t in ["fz_one", "fz_empty"] {
                assert!(!sql.starts_with(&format!("MERGE INTO {t} ")), "{sql}");
            }
            assert!(!sql.contains("ctid"), "system column reference: {sql}");
            assert!(!sql.contains('\n') && sql.ends_with(';'), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
            // At least one WHEN clause.
            assert!(sql.contains("WHEN "), "MERGE without WHEN clause: {sql}");
            // NOT MATCHED BY SOURCE UPDATE/DELETE always guarded: the arm
            // text between "BY SOURCE" and "THEN" must contain " AND "
            // unless the action is DO NOTHING.
            let mut rest = sql.as_str();
            while let Some(pos) = rest.find("WHEN NOT MATCHED BY SOURCE") {
                let arm = &rest[pos + "WHEN NOT MATCHED BY SOURCE".len()..];
                let then = arm.find(" THEN ").expect("arm has THEN");
                let action = &arm[then + 6..];
                if action.starts_with("UPDATE") || action.starts_with("DELETE") {
                    assert!(
                        arm[..then].contains(" AND "),
                        "unguarded NOT MATCHED BY SOURCE write: {sql}"
                    );
                }
                rest = &arm[then..];
            }
        }
    }

    #[test]
    fn cardinality_shape_forces_unconditional_write_arm() {
        // Bias hard toward the cardinality shape; every dup statement must
        // open its MATCHED arms with an unconditional UPDATE/DELETE.
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(
            "merge:src:values=1,merge:src:table=0,merge:src:subq=0,\
             merge:card:dup=10,merge:card:none=0",
        )
        .unwrap();
        let mut rng = Rng::new(31);
        let mut saw_dup = false;
        for _ in 0..200 {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            let sql = gen_merge_sql(&mut g).unwrap();
            if !p.iter().any(|q| q == "merge:card:dup") {
                continue;
            }
            saw_dup = true;
            // Duplicate key: the VALUES list's first two rows share a key.
            let vals = sql.split_once(" USING (VALUES (").unwrap().1;
            let k1 = vals.split(',').next().unwrap();
            let second = vals.split_once("), (").unwrap().1;
            let k2 = second.split(',').next().unwrap();
            assert_eq!(k1, k2, "cardinality shape without duplicate key: {sql}");
            // First MATCHED arm unconditional UPDATE/DELETE.
            let arm = sql.split_once("WHEN MATCHED").unwrap().1;
            assert!(
                arm.starts_with(" THEN UPDATE SET ") || arm.starts_with(" THEN DELETE"),
                "cardinality shape without unconditional write arm: {sql}"
            );
        }
        assert!(saw_dup, "cardinality shape never fired under heavy bias");
    }

    #[test]
    fn insert_arm_writes_join_key_into_pk() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let (sqls, _) = gen_many(
            47,
            400,
            "merge:nmt:insert=10,merge:nmt:nothing=0,merge:nmt:none=0",
        );
        let mut saw_insert = false;
        for sql in &sqls {
            let Some(rest) = sql.split_once("THEN INSERT (") else { continue };
            let target = sql.split_once("MERGE INTO ").unwrap().1;
            let tname = target.split(' ').next().unwrap();
            let table = cat.tables.iter().find(|t| t.name == tname).unwrap();
            let (cols_txt, vals) = rest.1.split_once(") VALUES (").unwrap();
            let cols: Vec<&str> = cols_txt.split(", ").collect();
            // pk and NOT NULL columns always present.
            for c in &table.columns {
                if table.is_pk_column(&c.name) || !c.nullable {
                    assert!(
                        cols.contains(&c.name.as_str()),
                        "required column {} missing: {sql}",
                        c.name
                    );
                }
            }
            // First column is the pk and its value is the source join key
            // (sk for values/subq sources, the source pk for table ones).
            let pk_idx = cols
                .iter()
                .position(|c| table.is_pk_column(c))
                .expect("insert lists the pk");
            if pk_idx == 0 {
                let first_val = vals.split(", ").next().unwrap();
                assert!(
                    first_val.contains('.'),
                    "pk value is not a source column reference: {sql}"
                );
            }
            saw_insert = true;
        }
        assert!(saw_insert, "INSERT arm never generated under heavy bias");
    }
}
