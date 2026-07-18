//! Noise grammar (property-first discipline, spec HR2: a construct is here
//! only because the v1 property set / shape classes want it).
//!
//! Screens are enforced AT GENERATION (contract §2.1.3):
//!   R2 — LIMIT/OFFSET is only ever emitted with a same-depth ORDER BY over
//!        the unique key (`id`).
//!   R3/R6 — the grammar contains no volatile or engine-constant-metadata
//!        functions at all; screens::lint is the regex backstop.
//!   R7 — aggregates in compared positions are over exact types unless the
//!        profile is float-lenient, in which case the Sql is tagged.
//!   R1 — nothing here promises result order beyond an explicit unique-key
//!        ORDER BY (relaxed order-law posture).
//!
//! Marks: queries = READ; DML/DDL = MUTATION (nothing ambiguous is generated;
//! anything ambiguous would be MUTATION per the dualexec fail-safe law).

use rand::RngCore;

use crate::gen::prodreg as pr;
use crate::gen::profile::GenProfile;
use crate::gen::schema::{Col, ColType, SchemaState, Table};
use crate::gen::weights::{range_incl, weighted_index};
use crate::plan::{Mark, Sql, SqlFlags};

/// One statement's production sub-path (below the `stmt:*` node), pushed by
/// the emission sites in this module. H5 rung A: the trace records generator
/// DECISIONS at generation time — it never consumes RNG draws and never
/// changes plan bytes (determinism law A3 untouched).
pub type ProdTrace = Vec<String>;

fn sql(text: String, mark: Mark, flags: SqlFlags) -> Sql {
    Sql::new(text, mark, flags).expect("generated SQL is single-line and ';'-terminated")
}

fn literal(rng: &mut dyn RngCore, ty: ColType) -> String {
    match ty {
        ColType::Int | ColType::Bigint => format!("{}", range_incl(rng, 0, 99)),
        ColType::Numeric => format!("{}.{:02}", range_incl(rng, 0, 99), range_incl(rng, 0, 99)),
        ColType::Text => format!("'s{:02}'", range_incl(rng, 0, 49)),
        ColType::Float8 => format!("{}.5", range_incl(rng, 0, 99)),
    }
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

/// DDL variant nodes, index-aligned with the weight array in `gen_ddl`. The
/// array length is the coupling that keeps the registry from going stale: a
/// new DDL arm without a `prodreg` name is a compile error here (H5 rung A).
const DDL_VARIANTS: [&str; 4] =
    [pr::DDL_CREATE_TABLE, pr::DDL_CREATE_INDEX, pr::DDL_RENAME_TABLE, pr::DDL_DROP_TABLE];

/// CREATE TABLE from the profile shape distribution; registers the table.
/// (Production trace: the CALLER pushes `ddl:create-table` — this fn is also
/// the forced first plan item, outside `gen_ddl`'s variant draw.)
pub fn gen_create_table(
    schema: &mut SchemaState,
    rng: &mut dyn RngCore,
    profile: &GenProfile,
) -> Sql {
    let idx = schema.create_table(rng, &profile.table_shape);
    let t = &schema.tables()[idx];
    let mut cols = String::new();
    for c in &t.cols {
        cols.push_str(&format!(", {} {}", c.name, c.ty.sql()));
    }
    sql(
        format!("CREATE TABLE {} (id bigint PRIMARY KEY{});", t.cur_name, cols),
        Mark::Mutation,
        SqlFlags::default(),
    )
}

/// Weighted DDL choice. May mutate schema state. Never drops the last table.
pub fn gen_ddl(
    schema: &mut SchemaState,
    rng: &mut dyn RngCore,
    profile: &GenProfile,
    trace: &mut ProdTrace,
) -> Sql {
    // create-table 4, create-index 3, rename 2, drop 1 (drop only if >1 table)
    let can_drop = schema.tables().len() > 1;
    let has_table = !schema.tables().is_empty();
    let weights: [u64; DDL_VARIANTS.len()] = [
        4u64,
        if has_table { 3 } else { 0 },
        if has_table { 2 } else { 0 },
        if can_drop { 1 } else { 0 },
    ];
    let choice = weighted_index(rng, &weights).expect("create-table weight is nonzero");
    trace.push(DDL_VARIANTS[choice].to_string());
    match choice {
        0 => gen_create_table(schema, rng, profile),
        1 => {
            let idx = schema.pick_table_idx(rng).expect("gated on has_table");
            let iname = schema.next_index_name();
            let t = schema.table_mut(idx);
            t.n_indexes += 1;
            let col = if t.cols.is_empty() {
                "id".to_string()
            } else {
                let ci = range_incl(rng, 0, t.cols.len() as u64 - 1) as usize;
                t.cols[ci].name.clone()
            };
            let name = t.cur_name.clone();
            sql(
                format!("CREATE INDEX {iname} ON {name} ({col});"),
                Mark::Mutation,
                SqlFlags::default(),
            )
        }
        2 => {
            let idx = schema.pick_table_idx(rng).expect("gated on has_table");
            let old = schema.tables()[idx].cur_name.clone();
            let new = schema.rename_table(idx);
            sql(
                format!("ALTER TABLE {old} RENAME TO {new};"),
                Mark::Mutation,
                SqlFlags::default(),
            )
        }
        3 => {
            let idx = schema.pick_table_idx(rng).expect("gated on can_drop");
            let t = schema.drop_table(idx);
            sql(format!("DROP TABLE {};", t.cur_name), Mark::Mutation, SqlFlags::default())
        }
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// DML
// ---------------------------------------------------------------------------

/// DML variant nodes, index-aligned with the weight array in `gen_dml`.
const DML_VARIANTS: [&str; 5] =
    [pr::DML_INSERT, pr::DML_UPDATE, pr::DML_DELETE, pr::DML_TRUNCATE, pr::DML_MERGE];

/// Weighted DML: insert 5 / key-addressed update 3 / key-addressed delete 2 /
/// truncate 1 (the ledger-understood subset, contract §3.1.2) / key-addressed
/// MERGE 2 (H6: semantically a guarded update-or-nothing over one key, so the
/// same understood subset; its plan is the ModifyTable Merge species).
pub fn gen_dml(
    schema: &mut SchemaState,
    rng: &mut dyn RngCore,
    trace: &mut ProdTrace,
) -> Option<Sql> {
    let idx = schema.pick_table_idx(rng)?;
    let weights: [u64; DML_VARIANTS.len()] = [5, 3, 2, 1, 2];
    let choice = weighted_index(rng, &weights).expect("static weights");
    trace.push(DML_VARIANTS[choice].to_string());
    let t = schema.table_mut(idx);
    Some(match choice {
        0 => {
            let key = t.next_key;
            t.next_key += 1;
            let mut cols = String::from("id");
            let mut vals = format!("{key}");
            for c in &t.cols {
                cols.push_str(&format!(", {}", c.name));
                vals.push_str(&format!(", {}", literal(rng, c.ty)));
            }
            sql(
                format!("INSERT INTO {} ({cols}) VALUES ({vals});", t.cur_name),
                Mark::Mutation,
                SqlFlags::default(),
            )
        }
        1 => {
            let key = range_incl(rng, 1, t.next_key.max(1) as u64);
            if t.cols.is_empty() {
                return None;
            }
            let ci = range_incl(rng, 0, t.cols.len() as u64 - 1) as usize;
            let c = &t.cols[ci];
            let lit = literal(rng, c.ty);
            sql(
                format!("UPDATE {} SET {} = {} WHERE id = {};", t.cur_name, c.name, lit, key),
                Mark::Mutation,
                SqlFlags::default(),
            )
        }
        2 => {
            let key = range_incl(rng, 1, t.next_key.max(1) as u64);
            sql(
                format!("DELETE FROM {} WHERE id = {};", t.cur_name, key),
                Mark::Mutation,
                SqlFlags::default(),
            )
        }
        3 => sql(format!("TRUNCATE {};", t.cur_name), Mark::Mutation, SqlFlags::default()),
        4 => {
            // Key-addressed MERGE: matched => update one payload column (or
            // delete when the table has no payload columns); not matched =>
            // do nothing. Same understood effect-space as dml:update /
            // dml:delete, so the ledger fence is not widened.
            let key = range_incl(rng, 1, t.next_key.max(1) as u64);
            let action = if t.cols.is_empty() {
                "DELETE".to_string()
            } else {
                let ci = range_incl(rng, 0, t.cols.len() as u64 - 1) as usize;
                let c = &t.cols[ci];
                format!("UPDATE SET {} = {}", c.name, literal(rng, c.ty))
            };
            sql(
                format!(
                    "MERGE INTO {} USING (VALUES ({key})) AS s(mid) ON {}.id = s.mid WHEN MATCHED THEN {} WHEN NOT MATCHED THEN DO NOTHING;",
                    t.cur_name, t.cur_name, action
                ),
                Mark::Mutation,
                SqlFlags::default(),
            )
        }
        _ => unreachable!(),
    })
}

// ---------------------------------------------------------------------------
// Queries (compared positions — every screen applies here)
// ---------------------------------------------------------------------------

fn pick_payload_col<'t>(t: &'t Table, rng: &mut dyn RngCore) -> Option<&'t Col> {
    if t.cols.is_empty() {
        return None;
    }
    let ci = range_incl(rng, 0, t.cols.len() as u64 - 1) as usize;
    Some(&t.cols[ci])
}

/// A same-typed (non-unique) payload column pair across two tables, for
/// semijoin quals. Over the UNIQUE key the planner reduces a semijoin to a
/// plain inner join (unique inner), so EXISTS/IN over `id` never mints the
/// Semi Join node — measured in the first H6 smoke census.
fn matching_payload_pair<'a>(
    t: &'a Table,
    t2: &'a Table,
    rng: &mut dyn RngCore,
) -> Option<(&'a str, &'a str)> {
    let mut pairs: Vec<(&str, &str)> = Vec::new();
    for ca in &t.cols {
        for cb in &t2.cols {
            if ca.ty == cb.ty {
                pairs.push((ca.name.as_str(), cb.name.as_str()));
            }
        }
    }
    if pairs.is_empty() {
        return None;
    }
    let i = range_incl(rng, 0, pairs.len() as u64 - 1) as usize;
    Some(pairs[i])
}

/// Typed scalar-call expression over an exact-typed column (function-call
/// grammar class, H4: the surface the p9/p5 bug family lives on). Returns
/// (select-expr) for `SELECT id, <expr> FROM ...`.
fn scalar_call(rng: &mut dyn RngCore, t: &Table, trace: &mut ProdTrace) -> String {
    let mut hit = |node: &str, expr: String| {
        trace.push(node.to_string());
        expr
    };
    let exact: Vec<&Col> = t.cols.iter().filter(|c| c.ty.is_exact()).collect();
    let Some(c) = (!exact.is_empty())
        .then(|| exact[range_incl(rng, 0, exact.len() as u64 - 1) as usize])
    else {
        return hit(pr::SC_FALLBACK_ABS_ID, "abs(id)".to_string());
    };
    match c.ty {
        ColType::Int | ColType::Bigint => match range_incl(rng, 0, 3) {
            0 => hit(pr::SC_INT_ABS, format!("abs({})", c.name)),
            1 => hit(pr::SC_INT_MOD, format!("({} % 7)", c.name)),
            2 => hit(pr::SC_INT_COALESCE, format!("coalesce({}, 0)", c.name)),
            _ => hit(
                pr::SC_INT_NULLIF,
                format!("nullif({}, {})", c.name, range_incl(rng, 0, 99)),
            ),
        },
        ColType::Text => match range_incl(rng, 0, 2) {
            0 => hit(pr::SC_TEXT_LENGTH, format!("length({})", c.name)),
            1 => hit(pr::SC_TEXT_UPPER, format!("upper({})", c.name)),
            _ => hit(pr::SC_TEXT_COALESCE, format!("coalesce({}, 's00')", c.name)),
        },
        ColType::Numeric => match range_incl(rng, 0, 1) {
            0 => hit(pr::SC_NUMERIC_ABS, format!("abs({})", c.name)),
            _ => hit(pr::SC_NUMERIC_COALESCE, format!("coalesce({}, 0)", c.name)),
        },
        // unreachable (is_exact gate); trace as the fallback arm honestly.
        ColType::Float8 => hit(pr::SC_FALLBACK_ABS_ID, "abs(id)".to_string()),
    }
}

/// Query variant nodes, index-aligned with the weight array in `gen_query`.
/// The `[&str; N]`-tied weight array is the anti-staleness coupling: adding a
/// variant arm without registering its `prodreg` name is a compile error.
const QUERY_VARIANTS: [&str; 38] = [
    pr::Q_FULL_ORDERED,
    pr::Q_COUNT_STAR,
    pr::Q_EXACT_AGG,
    pr::Q_FILTERED,
    pr::Q_TOPK_LIMIT,
    pr::Q_LIMIT_OFFSET,
    pr::Q_GROUP_COUNT,
    pr::Q_FLOAT_AGG,
    pr::Q_SRF_UNNEST,
    pr::Q_GENERATE_SERIES,
    pr::Q_SCALAR_CALL,
    pr::Q_INNER_JOIN,
    pr::Q_LEFT_JOIN_COALESCE,
    pr::Q_OJ_NEST_COALESCE,
    // H6 grammar arm (indexes 14..): planner-reach widening. Every one of
    // these is a plain differential read — type-correct against the schema
    // model, deterministic result under the R1/R2 screens (explicit ORDER BY
    // or the order-underdetermined tag), no ledger modeling (the H4 join
    // pattern: property noise substitution records them `ledger_op: None`).
    pr::Q_EXISTS_SEMI,
    pr::Q_NOT_EXISTS_ANTI,
    pr::Q_IN_SUBQ,
    pr::Q_SCALAR_SUBQ,
    pr::Q_CTE_MATERIALIZED,
    pr::Q_CTE_RECURSIVE,
    pr::Q_SETOP,
    pr::Q_UNION_ALL_TOPK,
    pr::Q_HAVING,
    pr::Q_GROUP_NOAGG,
    pr::Q_DISTINCT,
    pr::Q_DISTINCT_ON,
    pr::Q_GROUPING_SETS,
    pr::Q_WINDOW,
    pr::Q_VALUES_SCAN,
    pr::Q_SUBQUERY_SCAN,
    pr::Q_PROJECT_SET,
    pr::Q_RESULT,
    pr::Q_TID,
    pr::Q_TABLESAMPLE,
    pr::Q_JSON_TABLE,
    pr::Q_FULL_JOIN,
    pr::Q_OR_QUAL,
    pr::Q_FOR_UPDATE,
];

/// Weighted read-query choice over one table (plus join/SRF classes drawing
/// additional tables from the schema).
pub fn gen_query(
    schema: &SchemaState,
    profile: &GenProfile,
    rng: &mut dyn RngCore,
    trace: &mut ProdTrace,
) -> Option<Sql> {
    let t = schema.pick_table(rng)?;
    // Variants: 0 full-ordered / 1 count / 2 exact-agg / 3 filtered / 4 topk /
    // 5 offset / 6 group / 7 float-agg (float-lenient profiles only) /
    // 8 srf-unnest / 9 generate-series / 10 scalar-call /
    // 11 inner-join / 12 left-join-coalesce / 13 oj-nest-coalesce (p8 shape)
    let float_cols = t.cols_of_type(|ty| ty == ColType::Float8);
    let float_ok = profile.float_lenient && !float_cols.is_empty();
    let mut weights: [u64; QUERY_VARIANTS.len()] = [
        4u64,
        3,
        3,
        4,
        3,
        2,
        2,
        if float_ok { 2 } else { 0 },
        2,
        2,
        // scalar-call carries 10 sub-arms, several column-type-gated: at the
        // H6-widened variant count, weight 2 starved sc:numeric-abs in the
        // query-scarce write-heavy profile (battery reach pin caught it) —
        // 4 keeps every arm covered at the 500-seed budget.
        4,
        2,
        2,
        2,
        // H6 grammar arm: weight 2 each (frequent enough that the 500-seed
        // battery reach pin and the 300-seed teeth corpus cover every
        // variant AND every sub-arm; small enough not to starve the H4
        // join/SRF surface).
        2, // exists-semi
        2, // not-exists-anti
        2, // in-subq
        2, // scalar-subq
        2, // cte-materialized
        2, // cte-recursive
        2, // setop
        2, // union-all-topk
        2, // having
        2, // group-noagg
        2, // distinct
        2, // distinct-on
        2, // grouping-sets
        2, // window
        2, // values-scan
        2, // subquery-scan
        2, // project-set
        2, // result
        2, // tid
        2, // tablesample
        2, // json-table
        2, // full-join
        2, // or-qual
        2, // for-update
    ];
    // H5 reach-gate teeth knob: suppress emission of named productions
    // (weights zeroed) while the reach gate still expects them (see
    // GenProfile::test_disable_productions). Weight arithmetic only —
    // determinism preserved (the knob is profile input, in the profile sha).
    if !profile.test_disable_productions.is_empty() {
        for (i, name) in QUERY_VARIANTS.iter().enumerate() {
            if profile.test_disable_productions.iter().any(|d| d == name) {
                weights[i] = 0;
            }
        }
    }
    let flags = SqlFlags::default();
    let choice = weighted_index(rng, &weights).expect("static nonzero weights");
    trace.push(QUERY_VARIANTS[choice].to_string());
    Some(match choice {
        0 => {
            let mut cols = String::from("id");
            for c in &t.cols {
                cols.push_str(&format!(", {}", c.name));
            }
            sql(
                format!("SELECT {cols} FROM {} ORDER BY id;", t.cur_name),
                Mark::Read,
                flags,
            )
        }
        1 => sql(format!("SELECT count(*) FROM {};", t.cur_name), Mark::Read, flags),
        2 => {
            // R7: exact-type aggregate. `id` is bigint, always available.
            let exact: Vec<&Col> =
                t.cols.iter().filter(|c| c.ty.is_exact() && c.ty != ColType::Text).collect();
            let col = if exact.is_empty() {
                "id"
            } else {
                let ci = range_incl(rng, 0, exact.len() as u64 - 1) as usize;
                exact[ci].name.as_str()
            };
            sql(format!("SELECT sum({col}) FROM {};", t.cur_name), Mark::Read, flags)
        }
        3 => {
            let Some(c) = pick_payload_col(t, rng) else {
                return Some(sql(
                    format!("SELECT id FROM {} ORDER BY id;", t.cur_name),
                    Mark::Read,
                    flags,
                ));
            };
            let op = ["=", "<", ">"][range_incl(rng, 0, 2) as usize];
            let lit = literal(rng, c.ty);
            sql(
                format!(
                    "SELECT id, {} FROM {} WHERE {} {op} {lit} ORDER BY id;",
                    c.name, t.cur_name, c.name
                ),
                Mark::Read,
                flags,
            )
        }
        4 => {
            // R2: LIMIT always with same-depth ORDER BY over the unique key.
            let n = range_incl(rng, 1, 10);
            sql(
                format!("SELECT id FROM {} ORDER BY id LIMIT {n};", t.cur_name),
                Mark::Read,
                flags,
            )
        }
        5 => {
            let n = range_incl(rng, 1, 10);
            let k = range_incl(rng, 0, 10);
            sql(
                format!("SELECT id FROM {} ORDER BY id LIMIT {n} OFFSET {k};", t.cur_name),
                Mark::Read,
                flags,
            )
        }
        6 => {
            let Some(c) = pick_payload_col(t, rng) else {
                return Some(sql(
                    format!("SELECT count(*) FROM {};", t.cur_name),
                    Mark::Read,
                    flags,
                ));
            };
            sql(
                format!(
                    "SELECT {}, count(*) FROM {} GROUP BY {};",
                    c.name, t.cur_name, c.name
                ),
                Mark::Read,
                flags,
            )
        }
        7 => {
            let ci = range_incl(rng, 0, float_cols.len() as u64 - 1) as usize;
            let col = &float_cols[ci].name;
            // R7: float aggregate in a compared position — tagged.
            sql(
                format!("SELECT sum({col}) FROM {};", t.cur_name),
                Mark::Read,
                SqlFlags { order_underdetermined: false, float_lenient: true },
            )
        }
        8 => {
            // SRF FunctionScan (the p2 argcontext class site). Deterministic
            // typed literals; sort-normalized compare makes array order moot.
            let ti = range_incl(rng, 0, 2) as usize;
            const SRF_TYPES: [&str; 3] = [pr::SRF_INT, pr::SRF_TEXT, pr::SRF_NUMERIC];
            trace.push(SRF_TYPES[ti].to_string());
            let ty = [ColType::Int, ColType::Text, ColType::Numeric][ti];
            let (a, b, c) = (literal(rng, ty), literal(rng, ty), literal(rng, ty));
            sql(
                format!("SELECT x FROM unnest(ARRAY[{a}, {b}, {c}]) AS u(x);"),
                Mark::Read,
                flags,
            )
        }
        9 => {
            let lo = range_incl(rng, 0, 20);
            let n = range_incl(rng, 0, 20);
            sql(
                format!("SELECT g FROM generate_series({lo}, {}) AS g;", lo + n),
                Mark::Read,
                flags,
            )
        }
        10 => {
            let expr = scalar_call(rng, t, trace);
            sql(
                format!("SELECT id, {expr} FROM {} ORDER BY id;", t.cur_name),
                Mark::Read,
                flags,
            )
        }
        11 => {
            // INNER join over the FK-ish unique keys (self-join allowed —
            // the p8 witness itself joins one table twice).
            let t2 = schema.pick_table(rng)?;
            sql(
                format!(
                    "SELECT a.id, b.id FROM {} a JOIN {} b ON a.id = b.id ORDER BY a.id;",
                    t.cur_name, t2.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        12 => {
            // LEFT join, optionally with the COALESCE-guarded qual over the
            // nullable side (the OJ nullingrels family's trigger shape).
            let t2 = schema.pick_table(rng)?;
            let filtered = range_incl(rng, 0, 1) == 0;
            // The unfiltered branch is the epsilon-class alternative
            // (`ljc:no-qual`): it emits no token, so it can never appear in a
            // trace — excluded from denominators in prodreg (the k-note trap).
            if filtered {
                trace.push(pr::LJC_QUAL_COALESCE.to_string());
            }
            let qual = if filtered { " WHERE COALESCE(b.id, 0) = 0" } else { "" };
            sql(
                format!(
                    "SELECT a.id, COALESCE(b.id, 0) FROM {} a LEFT JOIN {} b ON a.id = b.id{qual} ORDER BY a.id;",
                    t.cur_name, t2.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        13 => {
            // The p8 nullingrels shape: OJ over a flattenable subquery whose
            // WHERE COALESCE references the nullable side, plus the same
            // guard above the outer join (depth-bounded: 3 relations).
            let t2 = schema.pick_table(rng)?;
            let t3 = schema.pick_table(rng)?;
            sql(
                format!(
                    "SELECT a.id, COALESCE(d.bid, 0) FROM {} a LEFT JOIN (SELECT b.id AS bid FROM {} b LEFT JOIN {} c ON b.id = c.id WHERE COALESCE(c.id, 0) = 0) d ON a.id = d.bid WHERE COALESCE(d.bid, 0) = 0 ORDER BY a.id;",
                    t.cur_name, t2.cur_name, t3.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        // ------------------------------------------------------- H6 variants
        14 => {
            // EXISTS: the semijoin family (Hash/Merge/Nested Loop Semi
            // Join). Qual over a same-typed NON-UNIQUE payload pair — over
            // the pk the planner reduces the semijoin to a plain inner join
            // and the Semi node never appears (first-smoke find). Fallback
            // to the keys when no typed pair exists.
            let t2 = schema.pick_table(rng)?;
            let (ca, cb) = matching_payload_pair(t, t2, rng).unwrap_or(("id", "id"));
            sql(
                format!(
                    "SELECT a.id FROM {} a WHERE EXISTS (SELECT 1 FROM {} b WHERE b.{cb} = a.{ca}) ORDER BY a.id;",
                    t.cur_name, t2.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        15 => {
            // NOT EXISTS: the antijoin family (Hash/Merge/Nested Loop Anti).
            let t2 = schema.pick_table(rng)?;
            sql(
                format!(
                    "SELECT a.id FROM {} a WHERE NOT EXISTS (SELECT 1 FROM {} b WHERE b.id = a.id) ORDER BY a.id;",
                    t.cur_name, t2.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        16 => {
            // IN-subquery: unique-ified semijoin (Unique/HashAggregate over
            // the subquery, or a hashed SubPlan). Same non-unique-pair rule
            // as EXISTS so the semijoin survives planning.
            let t2 = schema.pick_table(rng)?;
            let (ca, cb) = matching_payload_pair(t, t2, rng).unwrap_or(("id", "id"));
            sql(
                format!(
                    "SELECT a.id FROM {} a WHERE a.{ca} IN (SELECT b.{cb} FROM {} b) ORDER BY a.id;",
                    t.cur_name, t2.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        17 => {
            // Scalar subquery in the target list: correlated => SubPlan per
            // row; uncorrelated aggregate => one-shot InitPlan.
            let t2 = schema.pick_table(rng)?;
            const SSQ: [&str; 2] = [pr::SSQ_CORRELATED_COUNT, pr::SSQ_INITPLAN_MAX];
            let arm = range_incl(rng, 0, 1) as usize;
            trace.push(SSQ[arm].to_string());
            let expr = match arm {
                0 => format!("(SELECT count(*) FROM {} b WHERE b.id = a.id)", t2.cur_name),
                _ => format!("(SELECT COALESCE(max(b.id), 0) FROM {} b)", t2.cur_name),
            };
            sql(
                format!("SELECT a.id, {expr} FROM {} a ORDER BY a.id;", t.cur_name),
                Mark::Read,
                flags,
            )
        }
        18 => {
            // Materialized CTE: CTE Scan over its own subplan.
            sql(
                format!(
                    "WITH c AS MATERIALIZED (SELECT id FROM {}) SELECT id FROM c ORDER BY id;",
                    t.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        19 => {
            // Recursive CTE: one recipe elicits Recursive Union AND WorkTable
            // Scan. Constant-bounded, table-independent, deterministic.
            let k = range_incl(rng, 3, 12);
            sql(
                format!(
                    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < {k}) SELECT n FROM r ORDER BY n;"
                ),
                Mark::Read,
                flags,
            )
        }
        20 => {
            // Set operations over the unique keys. UNION dedups (Unique or
            // HashAggregate over Append); INTERSECT/EXCEPT are the
            // SetOp/HashSetOp family; sorted forms appear under
            // enable_hashagg=off arms.
            let t2 = schema.pick_table(rng)?;
            const SO: [&str; 4] = [pr::SO_UNION, pr::SO_UNION_ALL, pr::SO_INTERSECT, pr::SO_EXCEPT];
            let arm = range_incl(rng, 0, 3) as usize;
            trace.push(SO[arm].to_string());
            let op = ["UNION", "UNION ALL", "INTERSECT", "EXCEPT"][arm];
            sql(
                format!(
                    "SELECT id FROM {} {op} SELECT id FROM {} ORDER BY id;",
                    t.cur_name, t2.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        21 => {
            // Ordered UNION ALL + LIMIT over the indexed unique keys: the
            // Merge Append recipe (fractional cost prefers per-child ordered
            // index paths). R2: LIMIT with same-depth ORDER BY.
            let t2 = schema.pick_table(rng)?;
            let n = range_incl(rng, 1, 10);
            sql(
                format!(
                    "SELECT id FROM {} UNION ALL SELECT id FROM {} ORDER BY id LIMIT {n};",
                    t.cur_name, t2.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        22 => {
            // HAVING over a grouped count. The group key is unique in the
            // output, so ORDER BY 1 is a total order.
            let n = range_incl(rng, 1, 3);
            let col = match pick_payload_col(t, rng) {
                Some(c) => c.name.as_str(),
                None => "id",
            };
            sql(
                format!(
                    "SELECT {col}, count(*) FROM {} GROUP BY {col} HAVING count(*) >= {n} ORDER BY 1;",
                    t.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        23 => {
            // GROUP BY with NO aggregate in the target list: the Group node
            // (under enable_hashagg=off arms; HashAggregate otherwise).
            let col = match pick_payload_col(t, rng) {
                Some(c) => c.name.as_str(),
                None => "id",
            };
            sql(
                format!("SELECT {col} FROM {} GROUP BY {col} ORDER BY {col};", t.cur_name),
                Mark::Read,
                flags,
            )
        }
        24 => {
            // DISTINCT + ORDER BY: Unique over Sort under enable_hashagg=off.
            let col = match pick_payload_col(t, rng) {
                Some(c) => c.name.as_str(),
                None => "id",
            };
            sql(
                format!("SELECT DISTINCT {col} FROM {} ORDER BY {col};", t.cur_name),
                Mark::Read,
                flags,
            )
        }
        25 => {
            // DISTINCT ON: always the Unique node over an explicit full sort
            // (deterministic: the (col, id) order is total).
            let col = match pick_payload_col(t, rng) {
                Some(c) => c.name.as_str(),
                None => "id",
            };
            sql(
                format!(
                    "SELECT DISTINCT ON ({col}) {col}, id FROM {} ORDER BY {col}, id;",
                    t.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        26 => {
            // Grouping sets: ROLLUP / CUBE / GROUPING SETS over two keys
            // (MixedAggregate when hashed and sorted set strategies mix).
            // Generated columns are never NULL, so the only NULLs in the
            // output are the grouping-set placeholders — ORDER BY 1, 2 is a
            // total order over the distinct group rows.
            let ca = match pick_payload_col(t, rng) {
                Some(c) => c.name.clone(),
                None => "id".to_string(),
            };
            let cb = "id";
            const GS: [&str; 3] = [pr::GS_ROLLUP, pr::GS_CUBE, pr::GS_SETS];
            let arm = range_incl(rng, 0, 2) as usize;
            trace.push(GS[arm].to_string());
            let spec = match arm {
                0 => format!("ROLLUP({ca}, {cb})"),
                1 => format!("CUBE({ca}, {cb})"),
                _ => format!("GROUPING SETS (({ca}), ({cb}))"),
            };
            sql(
                format!(
                    "SELECT {ca}, {cb}, count(*) FROM {} GROUP BY {spec} ORDER BY 1, 2;",
                    t.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        27 => {
            // Window functions: WindowAgg. All frames are ordered by the
            // unique key, so every window value is deterministic; sums stay
            // on exact-typed id (R7).
            const W: [&str; 4] =
                [pr::W_ROW_NUMBER, pr::W_RANK, pr::W_SUM_OVER, pr::W_SUM_PARTITION];
            let arm = range_incl(rng, 0, 3) as usize;
            trace.push(W[arm].to_string());
            let expr = match arm {
                0 => "row_number() OVER (ORDER BY id)".to_string(),
                1 => "rank() OVER (ORDER BY id)".to_string(),
                2 => "sum(id) OVER (ORDER BY id)".to_string(),
                _ => {
                    let col = match pick_payload_col(t, rng) {
                        Some(c) => c.name.clone(),
                        None => "id".to_string(),
                    };
                    format!("sum(id) OVER (PARTITION BY {col} ORDER BY id)")
                }
            };
            sql(
                format!("SELECT id, {expr} FROM {} ORDER BY id;", t.cur_name),
                Mark::Read,
                flags,
            )
        }
        28 => {
            // VALUES list in FROM: Values Scan.
            let ty = [ColType::Int, ColType::Text, ColType::Numeric]
                [range_incl(rng, 0, 2) as usize];
            let (a, b, c) = (literal(rng, ty), literal(rng, ty), literal(rng, ty));
            sql(
                format!("SELECT x FROM (VALUES ({a}), ({b}), ({c})) v(x) ORDER BY x;"),
                Mark::Read,
                flags,
            )
        }
        29 => {
            // FROM-subquery that cannot be pulled up (inner ORDER BY +
            // LIMIT): Subquery Scan. R2 holds at both depths.
            let n = range_incl(rng, 1, 10);
            let k = range_incl(rng, 0, 5);
            sql(
                format!(
                    "SELECT s.id FROM (SELECT id FROM {} ORDER BY id LIMIT {n}) s WHERE s.id > {k} ORDER BY s.id;",
                    t.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        30 => {
            // Set-returning function in the TARGET LIST (not FROM):
            // ProjectSet. H4 only put SRFs in FROM (Function Scan). Rows tie
            // on id across the SRF expansion — tagged order-underdetermined
            // so the ladder sort-normalizes.
            let m = range_incl(rng, 1, 3);
            sql(
                format!("SELECT id, generate_series(1, {m}) FROM {} ORDER BY id;", t.cur_name),
                Mark::Read,
                SqlFlags { order_underdetermined: true, float_lenient: false },
            )
        }
        31 => {
            // The Result-plan family: FROM-less SELECT, constant-false gate,
            // and the min/max index rewrite (Result + InitPlan Limit over an
            // Index Only Scan on the pk).
            const RES: [&str; 3] = [pr::RES_NO_FROM, pr::RES_WHERE_FALSE, pr::RES_MINMAX];
            let arm = range_incl(rng, 0, 2) as usize;
            trace.push(RES[arm].to_string());
            match arm {
                0 => {
                    let (a, b) = (range_incl(rng, 0, 99), range_incl(rng, 0, 99));
                    sql(format!("SELECT {a} + {b};"), Mark::Read, flags)
                }
                1 => sql(
                    format!("SELECT id FROM {} WHERE false ORDER BY id;", t.cur_name),
                    Mark::Read,
                    flags,
                ),
                _ => {
                    let f = ["min", "max"][range_incl(rng, 0, 1) as usize];
                    sql(format!("SELECT {f}(id) FROM {};", t.cur_name), Mark::Read, flags)
                }
            }
        }
        32 => {
            // Tid Scan / Tid Range Scan. count(*) keeps the output shape
            // physical-layout-honest: the range form covers every page a
            // generated table can reach (rows_max << 4096 pages), the point
            // form compares slot-(0,1) liveness — identical engines place
            // identical heaps on the identical fixture.
            const TID: [&str; 2] = [pr::TID_POINT, pr::TID_RANGE];
            let arm = range_incl(rng, 0, 1) as usize;
            trace.push(TID[arm].to_string());
            match arm {
                0 => {
                    let slot = range_incl(rng, 1, 4);
                    sql(
                        format!(
                            "SELECT count(*) FROM {} WHERE ctid = '(0,{slot})'::tid;",
                            t.cur_name
                        ),
                        Mark::Read,
                        flags,
                    )
                }
                _ => sql(
                    format!(
                        "SELECT count(*) FROM {} WHERE ctid > '(0,0)'::tid AND ctid < '(4096,0)'::tid;",
                        t.cur_name
                    ),
                    Mark::Read,
                    flags,
                ),
            }
        }
        33 => {
            // Sample Scan. Percentage 100 keeps the result layout-independent
            // (every row sampled) while still planning a Sample Scan; the
            // REPEATABLE seed varies for sampler-state diversity.
            const SMP: [&str; 2] = [pr::SMP_BERNOULLI, pr::SMP_SYSTEM];
            let arm = range_incl(rng, 0, 1) as usize;
            trace.push(SMP[arm].to_string());
            let method = ["BERNOULLI", "SYSTEM"][arm];
            let s = range_incl(rng, 1, 9);
            sql(
                format!(
                    "SELECT count(*) FROM {} TABLESAMPLE {method} (100) REPEATABLE ({s});",
                    t.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        34 => {
            // JSON_TABLE in FROM: Table Function Scan. Constant document,
            // deterministic.
            let (a, b, c) = (
                range_incl(rng, 0, 99),
                range_incl(rng, 0, 99),
                range_incl(rng, 0, 99),
            );
            sql(
                format!(
                    "SELECT jt.x FROM json_table('[{a},{b},{c}]', '$[*]' COLUMNS (x int PATH '$')) AS jt ORDER BY jt.x;"
                ),
                Mark::Read,
                flags,
            )
        }
        35 => {
            // FULL OUTER JOIN on the unique keys: Hash/Merge Full Join.
            // COALESCE to -1 (ids start at 1) keeps the two columns non-NULL
            // and the ORDER BY total.
            let t2 = schema.pick_table(rng)?;
            sql(
                format!(
                    "SELECT COALESCE(a.id, -1), COALESCE(b.id, -1) FROM {} a FULL JOIN {} b ON a.id = b.id ORDER BY 1, 2;",
                    t.cur_name, t2.cur_name
                ),
                Mark::Read,
                flags,
            )
        }
        36 => {
            // OR of two potentially-indexed quals: the BitmapOr recipe (id is
            // always pk-indexed; the payload column is indexed whenever a
            // ddl:create-index landed on it).
            let k = range_incl(rng, 1, t.next_key.max(1) as u64);
            let alt = match pick_payload_col(t, rng) {
                Some(c) => format!("{} = {}", c.name, literal(rng, c.ty)),
                None => format!("id = {}", range_incl(rng, 1, t.next_key.max(1) as u64)),
            };
            sql(
                format!("SELECT count(*) FROM {} WHERE id = {k} OR {alt};", t.cur_name),
                Mark::Read,
                flags,
            )
        }
        37 => {
            // SELECT ... FOR UPDATE: LockRows (still a SELECT, so it passes
            // the census EXPLAIN gate). Key-addressed => at most one row,
            // locks drop at statement/tx end, single-session => no waits.
            let k = range_incl(rng, 1, t.next_key.max(1) as u64);
            sql(
                format!("SELECT id FROM {} WHERE id = {k} FOR UPDATE;", t.cur_name),
                Mark::Read,
                flags,
            )
        }
        _ => unreachable!(),
    })
}
