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
use crate::gen::schema::{Col, ColType, IndexDef, SchemaState, Table};
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

/// Small-domain literal (H6): values that land inside the modular value
/// domains the bulk loader writes (`g % m`, m in 5..=30; text `'s0' || g%10`).
/// Equality predicates drawn from here actually SELECT rows, which is what
/// makes the planner's index/bitmap choices meaningful after ANALYZE.
fn small_literal(rng: &mut dyn RngCore, ty: ColType) -> String {
    match ty {
        ColType::Int | ColType::Bigint | ColType::Numeric => {
            format!("{}", range_incl(rng, 0, 9))
        }
        ColType::Text => format!("'s0{}'", range_incl(rng, 0, 9)),
        ColType::Float8 => format!("{}.5", range_incl(rng, 0, 9)),
    }
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

/// DDL variant nodes, index-aligned with the weight array in `gen_ddl`. The
/// array length is the coupling that keeps the registry from going stale: a
/// new DDL arm without a `prodreg` name is a compile error here (H5 rung A).
/// The fdw entry stands for the whole setup chain; its emitted statements are
/// traced with their own per-statement nodes (see `gen_fdw_chain`).
const DDL_VARIANTS: [&str; 12] = [
    pr::DDL_CREATE_TABLE,
    pr::DDL_CREATE_INDEX,
    pr::DDL_CREATE_INDEX_MULTI,
    pr::DDL_CREATE_INDEX_EXPR,
    pr::DDL_CREATE_INDEX_PARTIAL,
    pr::DDL_CREATE_INDEX_BRIN,
    pr::DDL_DROP_INDEX,
    pr::DDL_ANALYZE,
    pr::DDL_FDW_TABLE,
    pr::DDL_RENAME_TABLE,
    pr::DDL_DROP_TABLE,
    pr::DDL_INDEX_PAIR,
];

/// One emitted DDL statement: its production sub-path + the SQL.
pub type DdlEmit = (ProdTrace, Sql);

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

/// Column list for an index: prefer payload columns, fall back to `id`.
fn pick_index_col(t: &Table, rng: &mut dyn RngCore) -> String {
    if t.cols.is_empty() {
        "id".to_string()
    } else {
        let ci = range_incl(rng, 0, t.cols.len() as u64 - 1) as usize;
        t.cols[ci].name.clone()
    }
}

/// The file_fdw setup chain (H6, Foreign Scan elicitation). One generator
/// decision emits every statement still missing for a NEW foreign table:
/// extension -> server -> deterministic CSV via `COPY ... TO` an absolute
/// seed-tagged /tmp path (COPY requires absolute paths; both legs write the
/// same deterministic bytes, so reads agree) -> `CREATE FOREIGN TABLE`.
/// IF NOT EXISTS keeps extension/server idempotent across seeds (they are
/// database-global and survive the per-seed schema reset).
fn gen_fdw_chain(schema: &mut SchemaState, rng: &mut dyn RngCore) -> Vec<DdlEmit> {
    let mut out: Vec<DdlEmit> = Vec::new();
    if !schema.fdw_extension_created() {
        schema.mark_fdw_extension();
        out.push((
            vec![pr::DDL_FDW_EXTENSION.to_string()],
            sql(
                "CREATE EXTENSION IF NOT EXISTS file_fdw;".to_string(),
                Mark::Mutation,
                SqlFlags::default(),
            ),
        ));
    }
    if !schema.fdw_server_created() {
        schema.mark_fdw_server();
        out.push((
            vec![pr::DDL_FDW_SERVER.to_string()],
            sql(
                "CREATE SERVER IF NOT EXISTS simharness_fsrv FOREIGN DATA WRAPPER file_fdw;"
                    .to_string(),
                Mark::Mutation,
                SqlFlags::default(),
            ),
        ));
    }
    let rows = range_incl(rng, 5, 60);
    let m = range_incl(rng, 5, 25);
    let ft = schema.create_foreign_table(rows);
    let (name, csv) = (ft.name.clone(), ft.csv_name.clone());
    out.push((
        vec![pr::DDL_FDW_COPY.to_string()],
        sql(
            format!(
                "COPY (SELECT g AS id, (g % {m}) AS a, 's0' || (g % 10) AS b \
                 FROM generate_series(1, {rows}) AS g ORDER BY g) \
                 TO '{csv}' WITH (FORMAT csv);"
            ),
            Mark::Mutation,
            SqlFlags::default(),
        ),
    ));
    out.push((
        vec![pr::DDL_FDW_TABLE.to_string()],
        sql(
            format!(
                "CREATE FOREIGN TABLE {name} (id bigint, a int, b text) \
                 SERVER simharness_fsrv OPTIONS (filename '{csv}', format 'csv');"
            ),
            Mark::Mutation,
            SqlFlags::default(),
        ),
    ));
    out
}

/// Maximum foreign tables per plan (state-op bloat guard).
const MAX_FOREIGN_TABLES: usize = 2;

/// Weighted DDL choice. May mutate schema state. Never drops the last table.
/// Returns one or more consecutive DDL statements (the fdw chain is the only
/// multi-statement arm), each with its own production sub-path.
pub fn gen_ddl(
    schema: &mut SchemaState,
    rng: &mut dyn RngCore,
    profile: &GenProfile,
) -> Vec<DdlEmit> {
    let can_drop = schema.tables().len() > 1;
    let has_table = !schema.tables().is_empty();
    let fdw_ok = schema.foreign_tables().len() < MAX_FOREIGN_TABLES;
    // NOTE: free (non-property) DDL draws are RARE — property footprints
    // consume most of the DDL budget, leaving ~1 draw per plan besides the
    // forced first CREATE TABLE. Arms that need a prior arm's state within
    // the same plan are therefore structurally starved; drop-index emits its
    // own create+drop chain when no index exists yet (measured: 0 drop-index
    // emissions in 300 seeds under the two-draw design).
    let weights: [u64; DDL_VARIANTS.len()] = [
        4u64,                             // create-table
        if has_table { 3 } else { 0 },    // create-index (single-col btree)
        if has_table { 2 } else { 0 },    // create-index-multi
        if has_table { 1 } else { 0 },    // create-index-expr
        if has_table { 1 } else { 0 },    // create-index-partial
        if has_table { 1 } else { 0 },    // create-index-brin
        if has_table { 2 } else { 0 },    // drop-index (chains create if needed)
        if has_table { 3 } else { 0 },    // analyze
        if fdw_ok { 2 } else { 0 },       // fdw chain
        if has_table { 2 } else { 0 },    // rename
        if has_table { 2 } else { 0 },    // drop-table (chains create if last)
        // index-pair: the BitmapAnd substrate chain (two single-col indexes
        // on one table + ANALYZE, one decision — the three-draw version
        // never co-occurs, same starvation as drop-index).
        if schema.tables().iter().any(|t| t.cols.len() >= 2) { 3 } else { 0 },
    ];
    let choice = weighted_index(rng, &weights).expect("create-table weight is nonzero");
    if choice == 8 {
        return gen_fdw_chain(schema, rng);
    }
    let trace = vec![DDL_VARIANTS[choice].to_string()];
    let one = |sql_text: String| {
        vec![(trace.clone(), sql(sql_text, Mark::Mutation, SqlFlags::default()))]
    };
    match choice {
        0 => {
            let s = gen_create_table(schema, rng, profile);
            vec![(trace.clone(), s)]
        }
        1 => {
            // Single-column btree — with two of these + ANALYZE the planner
            // can pick BitmapAnd for two-column equality conjunctions.
            let idx = schema.pick_table_idx(rng).expect("gated on has_table");
            let iname = schema.next_index_name();
            let t = schema.table_mut(idx);
            let col = pick_index_col(t, rng);
            t.indexes.push(IndexDef { name: iname.clone(), cols: vec![col.clone()] });
            one(format!("CREATE INDEX {iname} ON {} ({col});", t.cur_name))
        }
        2 => {
            // Multi-column btree — covers ORDER BY prefixes (Incremental
            // Sort) and covered projections (Index Only Scan).
            let idx = schema.pick_table_idx(rng).expect("gated on has_table");
            let iname = schema.next_index_name();
            let t = schema.table_mut(idx);
            let cols = if t.cols.len() >= 2 {
                let a = range_incl(rng, 0, t.cols.len() as u64 - 1) as usize;
                let mut b = range_incl(rng, 0, t.cols.len() as u64 - 2) as usize;
                if b >= a {
                    b += 1;
                }
                format!("{}, {}", t.cols[a].name, t.cols[b].name)
            } else if t.cols.len() == 1 {
                format!("{}, id", t.cols[0].name)
            } else {
                "id".to_string()
            };
            let key_cols: Vec<String> =
                cols.split(", ").map(|c| c.to_string()).collect();
            t.indexes.push(IndexDef { name: iname.clone(), cols: key_cols });
            one(format!("CREATE INDEX {iname} ON {} ({cols});", t.cur_name))
        }
        3 => {
            // Expression index (typed: abs() for numbers, lower() for text).
            let idx = schema.pick_table_idx(rng).expect("gated on has_table");
            let iname = schema.next_index_name();
            let t = schema.table_mut(idx);
            let expr = match t.cols.first() {
                None => "abs(id)".to_string(),
                Some(c) => match c.ty {
                    ColType::Text => format!("lower({})", c.name),
                    _ => format!("abs({})", c.name),
                },
            };
            t.indexes.push(IndexDef { name: iname.clone(), cols: Vec::new() });
            one(format!("CREATE INDEX {iname} ON {} (({expr}));", t.cur_name))
        }
        4 => {
            // Partial index: WHERE over the small-literal domain so the
            // predicate actually divides the bulk-loaded data.
            let idx = schema.pick_table_idx(rng).expect("gated on has_table");
            let iname = schema.next_index_name();
            let t = schema.table_mut(idx);
            let (col, lit) = match t.cols.first() {
                None => ("id".to_string(), "5".to_string()),
                Some(c) => (c.name.clone(), small_literal(rng, c.ty)),
            };
            t.indexes.push(IndexDef { name: iname.clone(), cols: Vec::new() });
            one(format!(
                "CREATE INDEX {iname} ON {} ({col}) WHERE {col} > {lit};",
                t.cur_name
            ))
        }
        5 => {
            // BRIN index (block-range summaries; always scanned via bitmap).
            let idx = schema.pick_table_idx(rng).expect("gated on has_table");
            let iname = schema.next_index_name();
            let t = schema.table_mut(idx);
            let col = pick_index_col(t, rng);
            t.indexes.push(IndexDef { name: iname.clone(), cols: vec![col.clone()] });
            one(format!("CREATE INDEX {iname} ON {} USING brin ({col});", t.cur_name))
        }
        6 => {
            // Drop a random generator-created index (state churn: plans that
            // were index-shaped must fall back after the drop). When no
            // index exists yet, chain a CREATE INDEX first (free DDL draws
            // are too rare for the create-then-drop pair to meet by chance;
            // see the weights note above).
            let with: Vec<usize> = schema
                .tables()
                .iter()
                .enumerate()
                .filter(|(_, t)| !t.indexes.is_empty())
                .map(|(i, _)| i)
                .collect();
            if with.is_empty() {
                let idx = schema.pick_table_idx(rng).expect("gated on has_table");
                let iname = schema.next_index_name();
                let t = schema.table_mut(idx);
                let col = pick_index_col(t, rng);
                let create = (
                    vec![pr::DDL_CREATE_INDEX.to_string()],
                    sql(
                        format!("CREATE INDEX {iname} ON {} ({col});", t.cur_name),
                        Mark::Mutation,
                        SqlFlags::default(),
                    ),
                );
                // The index is created and immediately dropped: the model
                // never registers it, so no later statement can address it.
                let drop = (
                    vec![pr::DDL_DROP_INDEX.to_string()],
                    sql(format!("DROP INDEX {iname};"), Mark::Mutation, SqlFlags::default()),
                );
                return vec![create, drop];
            }
            let ti = with[range_incl(rng, 0, with.len() as u64 - 1) as usize];
            let t = schema.table_mut(ti);
            let ii = range_incl(rng, 0, t.indexes.len() as u64 - 1) as usize;
            let iname = t.indexes.remove(ii).name;
            one(format!("DROP INDEX {iname};"))
        }
        7 => {
            // ANALYZE: real statistics — the planner's cost thresholds only
            // move once it has seen the bulk-loaded distributions.
            let idx = schema.pick_table_idx(rng).expect("gated on has_table");
            let name = schema.tables()[idx].cur_name.clone();
            one(format!("ANALYZE {name};"))
        }
        9 => {
            let idx = schema.pick_table_idx(rng).expect("gated on has_table");
            let old = schema.tables()[idx].cur_name.clone();
            let new = schema.rename_table(idx);
            one(format!("ALTER TABLE {old} RENAME TO {new};"))
        }
        10 => {
            // Drop a table. Dropping the LAST table would leave queries with
            // nothing to address, and >1 live table is rare (one free DDL
            // draw per plan — see the weights note), so the single-table
            // case chains a fresh CREATE TABLE right behind the drop.
            let idx = schema.pick_table_idx(rng).expect("gated on has_table");
            let dropped = schema.drop_table(idx);
            let drop_stmt = (
                vec![DDL_VARIANTS[10].to_string()],
                sql(format!("DROP TABLE {};", dropped.cur_name), Mark::Mutation, SqlFlags::default()),
            );
            if can_drop {
                return vec![drop_stmt];
            }
            let create = gen_create_table(schema, rng, profile);
            vec![drop_stmt, (vec![pr::DDL_CREATE_TABLE.to_string()], create)]
        }
        11 => {
            // BitmapAnd substrate: two single-column indexes on the SAME
            // table + ANALYZE, as one chained decision. q:two-col-eq biases
            // toward single-col-indexed columns, so once a big bulk load has
            // run the planner can AND the two bitmaps.
            let with: Vec<usize> = schema
                .tables()
                .iter()
                .enumerate()
                .filter(|(_, t)| t.cols.len() >= 2)
                .map(|(i, _)| i)
                .collect();
            let ti = with[range_incl(rng, 0, with.len() as u64 - 1) as usize];
            let ia = schema.next_index_name();
            let ib = schema.next_index_name();
            let t = schema.table_mut(ti);
            let a = range_incl(rng, 0, t.cols.len() as u64 - 1) as usize;
            let mut b = range_incl(rng, 0, t.cols.len() as u64 - 2) as usize;
            if b >= a {
                b += 1;
            }
            let (ca, cb) = (t.cols[a].name.clone(), t.cols[b].name.clone());
            t.indexes.push(IndexDef { name: ia.clone(), cols: vec![ca.clone()] });
            t.indexes.push(IndexDef { name: ib.clone(), cols: vec![cb.clone()] });
            let name = t.cur_name.clone();
            let step = |trace_node: &str, s: String| {
                (vec![trace_node.to_string()], sql(s, Mark::Mutation, SqlFlags::default()))
            };
            vec![
                step(pr::DDL_INDEX_PAIR, format!("CREATE INDEX {ia} ON {name} ({ca});")),
                step(pr::DDL_CREATE_INDEX, format!("CREATE INDEX {ib} ON {name} ({cb});")),
                step(pr::DDL_ANALYZE, format!("ANALYZE {name};")),
            ]
        }
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// DML
// ---------------------------------------------------------------------------

/// DML variant nodes, index-aligned with the weight array in `gen_dml`.
const DML_VARIANTS: [&str; 5] =
    [pr::DML_INSERT, pr::DML_UPDATE, pr::DML_DELETE, pr::DML_TRUNCATE, pr::DML_BULK_INSERT];

/// Bulk-load size tiers (H6 cardinality lever): tiny keeps seq scans
/// winning; medium pushes the planner across its index / bitmap cost
/// thresholds; the top tier scales with the profile's `rows_max` — measured
/// on this planner, BitmapAnd needs roughly >= 5000 rows with ~4%-selective
/// equality columns, so scale-oriented profiles set rows_max accordingly.
/// Every draw is capped by `rows_max`.
fn bulk_rows(rng: &mut dyn RngCore, rows_max: u32) -> u64 {
    let tier = weighted_index(rng, &[4u64, 3, 1]).expect("static weights");
    let cap = rows_max.max(1) as u64;
    let n = match tier {
        0 => range_incl(rng, 8, 32),
        1 => range_incl(rng, 100, 400),
        _ => {
            let lo = (cap / 2).max(500);
            let hi = cap.max(lo);
            range_incl(rng, lo, hi)
        }
    };
    n.min(cap)
}

/// Per-type deterministic bulk value expression over the series variable `g`.
/// Moduli in 5..=30 give each column a modest number of distinct values, so
/// equality predicates over the small-literal domain select real row groups
/// (~3-10% selectivity at the top — the BitmapAnd zone).
fn bulk_expr(rng: &mut dyn RngCore, ty: ColType) -> String {
    let m = range_incl(rng, 5, 30);
    match ty {
        ColType::Int | ColType::Bigint | ColType::Numeric => format!("(g % {m})"),
        ColType::Text => "('s0' || (g % 10))".to_string(),
        ColType::Float8 => format!("((g % {m}) + 0.5)"),
    }
}

/// Weighted DML: insert 5 / key-addressed update 3 / key-addressed delete 2 /
/// truncate 1 / bulk-insert 2 (the ledger-understood subset, contract
/// §3.1.2; bulk-insert is a plain INSERT ... SELECT — H6 cardinality lever).
pub fn gen_dml(
    schema: &mut SchemaState,
    rng: &mut dyn RngCore,
    profile: &GenProfile,
    trace: &mut ProdTrace,
) -> Option<Sql> {
    let idx = schema.pick_table_idx(rng)?;
    let weights: [u64; DML_VARIANTS.len()] = [5, 3, 2, 1, 2];
    let choice = weighted_index(rng, &weights).expect("static weights");
    trace.push(DML_VARIANTS[choice].to_string());
    let rows_max = profile.table_shape.rows_max;
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
            let n = bulk_rows(rng, rows_max);
            let start = t.next_key;
            let end = start + n as i64 - 1;
            t.next_key += n as i64;
            let mut cols = String::from("id");
            let mut exprs = String::from("g");
            for c in &t.cols {
                cols.push_str(&format!(", {}", c.name));
                exprs.push_str(&format!(", {}", bulk_expr(rng, c.ty)));
            }
            sql(
                format!(
                    "INSERT INTO {} ({cols}) SELECT {exprs} FROM generate_series({start}, {end}) AS g;",
                    t.cur_name
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
const QUERY_VARIANTS: [&str; 18] = [
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
    pr::Q_ORDER_PREFIX,
    pr::Q_TWO_COL_EQ,
    pr::Q_COVERED_SELECT,
    pr::Q_FOREIGN_SCAN,
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
    // 11 inner-join / 12 left-join-coalesce / 13 oj-nest-coalesce (p8 shape) /
    // 14 order-prefix / 15 two-col-eq / 16 covered-select /
    // 17 foreign-scan (needs a live foreign table — H6 state shapes)
    let float_cols = t.cols_of_type(|ty| ty == ColType::Float8);
    let float_ok = profile.float_lenient && !float_cols.is_empty();
    let two_cols_ok = t.cols.len() >= 2;
    let foreign_ok = !schema.foreign_tables().is_empty();
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
        3,
        if two_cols_ok { 2 } else { 0 },
        2,
        if foreign_ok { 2 } else { 0 },
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
        14 => {
            // Incremental Sort shape: ORDER BY <payload-col>, id. The suffix
            // `id` (unique) makes the order total, so LIMIT is R2-safe. When
            // an index on the payload column exists, it supplies the prefix
            // order and the planner can finish with an Incremental Sort.
            let Some(c) = pick_payload_col(t, rng) else {
                return Some(sql(
                    format!("SELECT id FROM {} ORDER BY id;", t.cur_name),
                    Mark::Read,
                    flags,
                ));
            };
            let lim = if range_incl(rng, 0, 1) == 0 {
                format!(" LIMIT {}", range_incl(rng, 1, 10))
            } else {
                String::new()
            };
            sql(
                format!(
                    "SELECT id, {} FROM {} ORDER BY {}, id{lim};",
                    c.name, t.cur_name, c.name
                ),
                Mark::Read,
                flags,
            )
        }
        15 => {
            // BitmapAnd shape: equality conjunction over two distinct
            // columns, literals from the bulk-load value domain (so the
            // predicate selects real row groups once stats exist). Bias:
            // when >= 2 columns carry single-column indexes (the index-pair
            // state op plants exactly that), query THOSE — a BitmapAnd can
            // only form over indexed columns.
            let indexed: Vec<&Col> = t
                .cols
                .iter()
                .filter(|c| {
                    t.indexes.iter().any(|ix| ix.cols.len() == 1 && ix.cols[0] == c.name)
                })
                .collect();
            let (ca, cb) = if indexed.len() >= 2 {
                let a = range_incl(rng, 0, indexed.len() as u64 - 1) as usize;
                let mut b = range_incl(rng, 0, indexed.len() as u64 - 2) as usize;
                if b >= a {
                    b += 1;
                }
                (indexed[a], indexed[b])
            } else {
                let a = range_incl(rng, 0, t.cols.len() as u64 - 1) as usize;
                let mut b = range_incl(rng, 0, t.cols.len() as u64 - 2) as usize;
                if b >= a {
                    b += 1;
                }
                (&t.cols[a], &t.cols[b])
            };
            let (la, lb) = (small_literal(rng, ca.ty), small_literal(rng, cb.ty));
            sql(
                format!(
                    "SELECT id FROM {} WHERE {} = {la} AND {} = {lb} ORDER BY id;",
                    t.cur_name, ca.name, cb.name
                ),
                Mark::Read,
                flags,
            )
        }
        16 => {
            // Index Only Scan shape: projection covered by a single-column
            // index (when one exists on this column).
            let Some(c) = pick_payload_col(t, rng) else {
                return Some(sql(
                    format!("SELECT id FROM {} ORDER BY id;", t.cur_name),
                    Mark::Read,
                    flags,
                ));
            };
            let op = ["=", "<", ">"][range_incl(rng, 0, 2) as usize];
            let lit = small_literal(rng, c.ty);
            sql(
                format!(
                    "SELECT {} FROM {} WHERE {} {op} {lit} ORDER BY {};",
                    c.name, t.cur_name, c.name, c.name
                ),
                Mark::Read,
                flags,
            )
        }
        17 => {
            // Foreign Scan shape: read a file_fdw foreign table.
            let ft = schema.pick_foreign(rng).expect("gated on foreign_ok");
            match range_incl(rng, 0, 2) {
                0 => sql(format!("SELECT count(*) FROM {};", ft.name), Mark::Read, flags),
                1 => sql(
                    format!("SELECT id, a, b FROM {} ORDER BY id;", ft.name),
                    Mark::Read,
                    flags,
                ),
                _ => sql(
                    format!(
                        "SELECT id FROM {} WHERE a = {} ORDER BY id;",
                        ft.name,
                        range_incl(rng, 0, 9)
                    ),
                    Mark::Read,
                    flags,
                ),
            }
        }
        _ => unreachable!(),
    })
}
