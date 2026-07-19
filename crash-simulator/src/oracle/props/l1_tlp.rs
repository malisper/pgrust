//! L1 TLP — Ternary Logic Partitioning (Rigger & Su, "Finding Bugs in
//! Database Systems via Query Partitioning", OOPSLA 2020), the reference-free
//! metamorphic oracle: for any predicate p, every row lands in exactly one of
//! the three 3VL partitions (p) / (NOT p) / ((p) IS NULL), so a query and its
//! partition reassembly must agree WITHOUT consulting any reference engine —
//! this is what can catch bugs in stock PostgreSQL itself, where the
//! C-differential oracle is structurally blind (C PG is its answer key).
//!
//! Variants (drawn per instance):
//!   rows      WHERE-TLP — full row multiset: whole == parts reassembled
//!             (`PartitionUnionEq`)
//!   count     COUNT-TLP — |whole| == |T| + |F| + |N| (`ScalarSumEq`)
//!   sum       SUM-TLP over an int column (`ScalarSumEq`, NULL sum == 0)
//!   min/max   MIN/MAX-TLP (`ScalarExtremeEq`)
//!   distinct  DISTINCT-TLP — set(whole) == set-union of the parts' DISTINCT
//!             results (`DistinctUnionEq`; a value may recur across parts)
//!
//! Targets (drawn per instance):
//!   local     property-local kvs table (k bigint PK, v bigint NULL, s text
//!             NULL; ~1/4 NULLs per payload column) — fully ledger-modeled,
//!             so the sim executor answers every probe (parity/teeth tests)
//!   live      a typed-generator table (SchemaView): campaign-mutated data,
//!             richer schemas — probes are None (not sim-answerable; the
//!             checks compare engine results against engine results only)
//!   join      LEFT JOIN over two live tables with the predicate on the
//!             NULLABLE side — join-produced NULLs give the 3VL partitions
//!             genuine mass even when base data has none (the OJ bug family)
//!
//! The predicate grammar (PredSpec) is 3VL end-to-end: comparisons and `%`
//! over int/text columns, IS NULL, and Kleene AND/OR/NOT composition.

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, Value};
use crate::oracle::props::{
    helpers as h, PredColKind, ProfileView, PropertyId, SchemaTable, SchemaView,
};
use crate::oracle::pstep::{
    partition_sql, CmpOp, Mark, NoiseConstraint, PredSpec, ProbeSpec, PropertyInstance, PStep,
    SqlMeta, SqlStep, TriSel,
};

// ---------------------------------------------------------------------------
// Predicate generation (shared with L2 NoREC)
// ---------------------------------------------------------------------------

/// One comparison/IS NULL/`%` leaf over the predicate surface
/// (col index, kind). Every leaf is deterministic and non-volatile.
fn base_pred(rng: &mut impl Rng, surface: &[(usize, PredColKind)]) -> PredSpec {
    let (col, kind) = surface[rng.gen_range(0..surface.len())];
    match kind {
        PredColKind::Text => match rng.gen_range(0u32..4) {
            0 => PredSpec::ColIsNull { col },
            _ => {
                let op = [CmpOp::Lt, CmpOp::Eq, CmpOp::Gt, CmpOp::Ne][rng.gen_range(0usize..4)];
                let lit = Value::Text(format!("s{:02}", rng.gen_range(0u32..50)));
                PredSpec::ColCmp { col, op, lit }
            }
        },
        // Int surface (Other never enters a surface).
        _ => match rng.gen_range(0u32..5) {
            0 => {
                let m = rng.gen_range(2i64..6);
                let r = rng.gen_range(0..m);
                PredSpec::ColModEq { col, m, r }
            }
            1 => PredSpec::ColIsNull { col },
            _ => {
                let op = [CmpOp::Lt, CmpOp::Le, CmpOp::Eq, CmpOp::Ge, CmpOp::Gt, CmpOp::Ne]
                    [rng.gen_range(0usize..6)];
                let lit = Value::Int(rng.gen_range(0i64..100));
                PredSpec::ColCmp { col, op, lit }
            }
        },
    }
}

/// Depth-bounded random predicate: a leaf, NOT leaf, or an AND/OR of two
/// leaves — the SQLancer-style composed-predicate surface.
pub(crate) fn gen_pred(rng: &mut impl Rng, surface: &[(usize, PredColKind)]) -> PredSpec {
    match rng.gen_range(0u32..8) {
        0 | 1 => PredSpec::And(
            Box::new(base_pred(rng, surface)),
            Box::new(base_pred(rng, surface)),
        ),
        2 | 3 => PredSpec::Or(
            Box::new(base_pred(rng, surface)),
            Box::new(base_pred(rng, surface)),
        ),
        4 => PredSpec::Not(Box::new(base_pred(rng, surface))),
        _ => base_pred(rng, surface),
    }
}

/// Predicate surface of a live generator table: `id` plus every int/text
/// payload column, index-aligned with the returned SQL names.
pub(crate) fn live_surface(t: &SchemaTable) -> (Vec<(usize, PredColKind)>, Vec<String>) {
    let mut surface = Vec::new();
    let mut names = Vec::new();
    for c in &t.cols {
        if matches!(c.kind, PredColKind::Int | PredColKind::Text) {
            surface.push((names.len(), c.kind));
            names.push(c.name.clone());
        }
    }
    (surface, names)
}

// ---------------------------------------------------------------------------
// Step builders
// ---------------------------------------------------------------------------

/// `SELECT count(*) FROM {table} WHERE <partition arm>` over the
/// property-local k/v/s naming (also used by X2 IndexInvariance).
pub(crate) fn count_pred(table: &str, pred: &PredSpec, sel: TriSel, slot: u32) -> SqlStep {
    SqlStep {
        sql: format!(
            "SELECT count(*) FROM {table} WHERE {}",
            partition_sql(pred, sel, &h::KVS_COL_NAMES)
        ),
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::CountWherePred { table: table.into(), pred: pred.clone(), sel }),
        stackref: Some(slot),
    }
}

/// A live-table read: no ledger model, no sim probe — the metamorphic law is
/// checked engine-result against engine-result.
fn live_read(sql: String, slot: u32) -> PStep {
    PStep::Sql(SqlStep {
        sql,
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: None,
        stackref: Some(slot),
    })
}

const SELS: [TriSel; 3] = [TriSel::True, TriSel::False, TriSel::Null];

/// TLP variant vocabulary (per-instance draw).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Variant {
    Rows,
    Count,
    Sum,
    Min,
    Max,
    Distinct,
}

fn draw_variant(rng: &mut impl Rng) -> Variant {
    [Variant::Rows, Variant::Count, Variant::Sum, Variant::Min, Variant::Max, Variant::Distinct]
        [rng.gen_range(0usize..6)]
}

fn tlp_assert(variant: Variant, max: bool) -> Check {
    match variant {
        Variant::Rows => Check::PartitionUnionEq { parts: vec![1, 2, 3], whole: 0 },
        Variant::Count | Variant::Sum => Check::ScalarSumEq { parts: vec![1, 2, 3], whole: 0 },
        Variant::Min | Variant::Max => {
            Check::ScalarExtremeEq { parts: vec![1, 2, 3], whole: 0, max }
        }
        Variant::Distinct => Check::DistinctUnionEq { parts: vec![1, 2, 3], whole: 0 },
    }
}

// ---------------------------------------------------------------------------
// Instance generation
// ---------------------------------------------------------------------------

/// Property-local target: kvs table, ledger-modeled, sim-answerable.
fn generate_local(rng: &mut impl Rng) -> PropertyInstance {
    let table = h::fresh_table(rng, "l1");
    let n = rng.gen_range(5..=12);
    let rows = h::gen_rows_kvs(rng, n);
    // Surface: v (int, nullable) and s (text, nullable).
    let surface = [(1usize, PredColKind::Int), (2usize, PredColKind::Text)];
    let pred = gen_pred(rng, &surface);
    let variant = draw_variant(rng);

    let mut steps = vec![
        h::sql(h::create_kvs(&table)),
        h::sql(h::insert_rows_kvs(&table, &rows)),
        PStep::NoiseSlot(NoiseConstraint::MustNotTouch(
            [table.clone()].into_iter().collect(),
        )),
    ];

    let whole_arm = |sel: Option<TriSel>, slot: u32| -> PStep {
        let where_clause = match sel {
            None => String::new(),
            Some(s) => format!(" WHERE {}", partition_sql(&pred, s, &h::KVS_COL_NAMES)),
        };
        let (sql, probe) = match variant {
            Variant::Rows => (
                format!("SELECT k, v, s FROM {table}{where_clause} ORDER BY k"),
                ProbeSpec::RowsWherePred { table: table.clone(), pred: pred.clone(), sel },
            ),
            Variant::Count => match sel {
                None => (
                    format!("SELECT count(*) FROM {table}"),
                    ProbeSpec::CountAll { table: table.clone() },
                ),
                Some(s) => {
                    return h::sql(count_pred(&table, &pred, s, slot));
                }
            },
            Variant::Sum => (
                format!("SELECT sum(v) FROM {table}{where_clause}"),
                ProbeSpec::SumCol {
                    table: table.clone(),
                    col: 1,
                    filter: sel.map(|s| (pred.clone(), s)),
                },
            ),
            Variant::Min | Variant::Max => {
                let f = if variant == Variant::Max { "max" } else { "min" };
                (
                    format!("SELECT {f}(v) FROM {table}{where_clause}"),
                    ProbeSpec::ExtremeCol {
                        table: table.clone(),
                        col: 1,
                        filter: sel.map(|s| (pred.clone(), s)),
                        max: variant == Variant::Max,
                    },
                )
            }
            Variant::Distinct => (
                format!("SELECT DISTINCT s FROM {table}{where_clause}"),
                ProbeSpec::DistinctCol {
                    table: table.clone(),
                    col: 2,
                    filter: sel.map(|s| (pred.clone(), s)),
                },
            ),
        };
        h::sql(SqlStep {
            sql,
            mark: Mark::Read,
            meta: SqlMeta::default(),
            ledger_op: None,
            probe: Some(probe),
            stackref: Some(slot),
        })
    };

    steps.push(whole_arm(None, 0));
    for (i, sel) in SELS.iter().enumerate() {
        steps.push(whole_arm(Some(*sel), (i + 1) as u32));
    }
    steps.push(PStep::Assert(tlp_assert(variant, variant == Variant::Max)));
    steps.push(h::sql(h::drop_table(&table)));

    PropertyInstance {
        property: PropertyId::L1Tlp,
        steps,
        tables: BTreeSet::from([table]),
    }
}

/// Live single-table target: the typed generator's schema, campaign-mutated
/// data. Reference-free — the four reads run against ONE engine and only
/// their mutual consistency is asserted.
fn generate_live(rng: &mut impl Rng, t: &SchemaTable) -> Option<PropertyInstance> {
    let (surface, names) = live_surface(t);
    if surface.is_empty() {
        return None;
    }
    let pred = gen_pred(rng, &surface);
    let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let int_cols: Vec<&str> = surface
        .iter()
        .filter(|(_, k)| *k == PredColKind::Int)
        .map(|(i, _)| name_refs[*i])
        .collect();
    let variant = {
        // Sum/Min/Max need an int column; id is always int, so this is
        // belt-and-braces rather than a real gate.
        let v = draw_variant(rng);
        if int_cols.is_empty() && matches!(v, Variant::Sum | Variant::Min | Variant::Max) {
            Variant::Rows
        } else {
            v
        }
    };
    let tname = &t.name;
    let projection = name_refs.join(", ");
    let agg_col = if int_cols.is_empty() { "id" } else { int_cols[rng.gen_range(0..int_cols.len())] };
    let distinct_col = name_refs[surface[rng.gen_range(0..surface.len())].0];

    let mut steps = vec![PStep::NoiseSlot(NoiseConstraint::MustNotTouch(
        [tname.clone()].into_iter().collect(),
    ))];
    let arm_sql = |sel: Option<TriSel>| -> String {
        let where_clause = match sel {
            None => String::new(),
            Some(s) => format!(" WHERE {}", partition_sql(&pred, s, &name_refs)),
        };
        match variant {
            Variant::Rows => {
                format!("SELECT {projection} FROM {tname}{where_clause} ORDER BY id")
            }
            Variant::Count => format!("SELECT count(*) FROM {tname}{where_clause}"),
            Variant::Sum => format!("SELECT sum({agg_col}) FROM {tname}{where_clause}"),
            Variant::Min => format!("SELECT min({agg_col}) FROM {tname}{where_clause}"),
            Variant::Max => format!("SELECT max({agg_col}) FROM {tname}{where_clause}"),
            Variant::Distinct => {
                format!("SELECT DISTINCT {distinct_col} FROM {tname}{where_clause}")
            }
        }
    };
    steps.push(live_read(arm_sql(None), 0));
    for (i, sel) in SELS.iter().enumerate() {
        steps.push(live_read(arm_sql(Some(*sel)), (i + 1) as u32));
    }
    steps.push(PStep::Assert(tlp_assert(variant, variant == Variant::Max)));

    Some(PropertyInstance {
        property: PropertyId::L1Tlp,
        steps,
        tables: BTreeSet::from([t.birth_id.clone()]),
    })
}

/// Live LEFT JOIN target: predicate over the NULLABLE side's key — the
/// ON-miss rows make the predicate genuinely UNKNOWN even where base data has
/// no NULLs (the outer-join nullingrels bug family's surface).
fn generate_live_join(
    rng: &mut impl Rng,
    t1: &SchemaTable,
    t2: &SchemaTable,
) -> PropertyInstance {
    // Predicate surface: b.id plus b's int/text payload columns (all nullable
    // through the LEFT JOIN).
    let (b_surface, b_names) = live_surface(t2);
    let names: Vec<String> = b_names.iter().map(|n| format!("b.{n}")).collect();
    let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let pred = gen_pred(rng, &b_surface);
    let count_variant = rng.gen_range(0u32..2) == 0;
    let (t1n, t2n) = (&t1.name, &t2.name);

    let arm_sql = |sel: Option<TriSel>| -> String {
        let where_clause = match sel {
            None => String::new(),
            Some(s) => format!(" WHERE {}", partition_sql(&pred, s, &name_refs)),
        };
        if count_variant {
            format!(
                "SELECT count(*) FROM {t1n} a LEFT JOIN {t2n} b ON a.id = b.id{where_clause}"
            )
        } else {
            format!(
                "SELECT a.id, b.id FROM {t1n} a LEFT JOIN {t2n} b ON a.id = b.id{where_clause} ORDER BY a.id"
            )
        }
    };
    let mut steps = vec![PStep::NoiseSlot(NoiseConstraint::MustNotTouch(
        [t1n.clone(), t2n.clone()].into_iter().collect(),
    ))];
    steps.push(live_read(arm_sql(None), 0));
    for (i, sel) in SELS.iter().enumerate() {
        steps.push(live_read(arm_sql(Some(*sel)), (i + 1) as u32));
    }
    steps.push(PStep::Assert(if count_variant {
        Check::ScalarSumEq { parts: vec![1, 2, 3], whole: 0 }
    } else {
        Check::PartitionUnionEq { parts: vec![1, 2, 3], whole: 0 }
    }));

    PropertyInstance {
        property: PropertyId::L1Tlp,
        steps,
        tables: BTreeSet::from([t1.birth_id.clone(), t2.birth_id.clone()]),
    }
}

pub fn generate(
    rng: &mut impl Rng,
    schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    // Target mix when live tables exist: local 2 / live-single 3 / live-join 3.
    // Empty schema (unit/sim tests, plan head) => always local.
    if !schema.tables.is_empty() {
        match rng.gen_range(0u32..8) {
            0 | 1 => {}
            2..=4 => {
                let t = &schema.tables[rng.gen_range(0..schema.tables.len())];
                if let Some(inst) = generate_live(rng, t) {
                    return inst;
                }
            }
            _ => {
                let t1 = &schema.tables[rng.gen_range(0..schema.tables.len())];
                let t2 = &schema.tables[rng.gen_range(0..schema.tables.len())];
                return generate_live_join(rng, t1, t2);
            }
        }
    }
    generate_local(rng)
}
