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

use crate::gen::profile::GenProfile;
use crate::gen::schema::{Col, ColType, SchemaState, Table};
use crate::gen::weights::{range_incl, weighted_index};
use crate::plan::{Mark, Sql, SqlFlags};

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

/// CREATE TABLE from the profile shape distribution; registers the table.
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
pub fn gen_ddl(schema: &mut SchemaState, rng: &mut dyn RngCore, profile: &GenProfile) -> Sql {
    // create-table 4, create-index 3, rename 2, drop 1 (drop only if >1 table)
    let can_drop = schema.tables().len() > 1;
    let has_table = !schema.tables().is_empty();
    let weights = [
        4u64,
        if has_table { 3 } else { 0 },
        if has_table { 2 } else { 0 },
        if can_drop { 1 } else { 0 },
    ];
    match weighted_index(rng, &weights).expect("create-table weight is nonzero") {
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

/// Weighted DML: insert 5 / key-addressed update 3 / key-addressed delete 2 /
/// truncate 1 (the ledger-understood subset, contract §3.1.2).
pub fn gen_dml(schema: &mut SchemaState, rng: &mut dyn RngCore) -> Option<Sql> {
    let idx = schema.pick_table_idx(rng)?;
    let choice = weighted_index(rng, &[5, 3, 2, 1]).expect("static weights");
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

/// Weighted read-query choice over one table.
pub fn gen_query(schema: &SchemaState, profile: &GenProfile, rng: &mut dyn RngCore) -> Option<Sql> {
    let t = schema.pick_table(rng)?;
    // Variants: 0 full-ordered / 1 count / 2 exact-agg / 3 filtered / 4 topk /
    // 5 offset / 6 group / 7 float-agg (float-lenient profiles only)
    let float_cols = t.cols_of_type(|ty| ty == ColType::Float8);
    let float_ok = profile.float_lenient && !float_cols.is_empty();
    let weights = [4u64, 3, 3, 4, 3, 2, 2, if float_ok { 2 } else { 0 }];
    let flags = SqlFlags::default();
    Some(match weighted_index(rng, &weights).expect("static nonzero weights") {
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
        _ => unreachable!(),
    })
}
