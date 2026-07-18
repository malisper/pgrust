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
const DML_VARIANTS: [&str; 4] =
    [pr::DML_INSERT, pr::DML_UPDATE, pr::DML_DELETE, pr::DML_TRUNCATE];

/// Weighted DML: insert 5 / key-addressed update 3 / key-addressed delete 2 /
/// truncate 1 (the ledger-understood subset, contract §3.1.2).
pub fn gen_dml(
    schema: &mut SchemaState,
    rng: &mut dyn RngCore,
    trace: &mut ProdTrace,
) -> Option<Sql> {
    let idx = schema.pick_table_idx(rng)?;
    let weights: [u64; DML_VARIANTS.len()] = [5, 3, 2, 1];
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
const QUERY_VARIANTS: [&str; 14] = [
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
        2,
        2,
        2,
        2,
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
        _ => unreachable!(),
    })
}
