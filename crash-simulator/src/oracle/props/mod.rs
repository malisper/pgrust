//! Property enum v1 — the serial subset of spec §1.3 / Appendix A
//! (contract §3.1.1): F1–F6 unconditional, F7/F8 behind the engine-hook
//! probe (§0 A5), L1 TLP, L2 NoREC, X1 ArmEquivalence, X2 IndexInvariance,
//! X4 StatementForm, M1 ReadYourWrites — 13 properties (11 unconditional).
//!
//! Each property file owns its generate-side struct + step compilation +
//! checker (as embedded ASSUME/ASSERT checks) end-to-end; WS-GEN calls
//! through `generate()` below.

use rand::Rng;

use crate::oracle::pstep::{IsoLevel, PropertyInstance};

pub mod cursor;
pub mod c1_cursor_walk;
pub mod c2_hold_cursor;
pub mod f1_insert_select;
pub mod f2_update_visibility;
pub mod f3_delete_absence;
pub mod f4_constraint_error;
pub mod f5_ddl_effects;
pub mod f6_agg_consistency;
pub mod f7_memory_baseline;
pub mod f8_resource_baseline;
pub mod l1_tlp;
pub mod l2_norec;
pub mod m1_read_your_writes;
pub mod m2_cross_session;
pub mod s1_spec_conflict;
pub mod x1_arm_equivalence;
pub mod x2_index_invariance;
pub mod x4_statement_form;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PropertyId {
    F1InsertSelect,
    F2UpdateVisibility,
    F3DeleteAbsence,
    F4ConstraintError,
    F5DdlEffects,
    F6AggConsistency,
    F7MemoryBaseline,
    F8ResourceBaseline,
    L1Tlp,
    L2NoRec,
    X1ArmEquivalence,
    X2IndexInvariance,
    X4StatementForm,
    M1ReadYourWrites,
    /// H8 (plan-format v2). C1/C2 are single-session cursor/backward-scan
    /// properties (the p7 surface); M2/S1 are the multi-session estate
    /// (the p3 surface). All four are gated on `needs_sessions()` for the
    /// multi-session pair.
    C1CursorWalk,
    C2HoldCursor,
    M2CrossSession,
    S1SpecConflict,
}

impl PropertyId {
    pub fn as_str(&self) -> &'static str {
        match self {
            PropertyId::F1InsertSelect => "F1-InsertSelect",
            PropertyId::F2UpdateVisibility => "F2-UpdateVisibility",
            PropertyId::F3DeleteAbsence => "F3-DeleteAbsence",
            PropertyId::F4ConstraintError => "F4-ConstraintError",
            PropertyId::F5DdlEffects => "F5-DdlEffects",
            PropertyId::F6AggConsistency => "F6-AggConsistency",
            PropertyId::F7MemoryBaseline => "F7-MemoryBaseline",
            PropertyId::F8ResourceBaseline => "F8-ResourceBaseline",
            PropertyId::L1Tlp => "L1-Tlp",
            PropertyId::L2NoRec => "L2-NoRec",
            PropertyId::X1ArmEquivalence => "X1-ArmEquivalence",
            PropertyId::X2IndexInvariance => "X2-IndexInvariance",
            PropertyId::X4StatementForm => "X4-StatementForm",
            PropertyId::M1ReadYourWrites => "M1-ReadYourWrites",
            PropertyId::C1CursorWalk => "C1-CursorWalk",
            PropertyId::C2HoldCursor => "C2-HoldCursor",
            PropertyId::M2CrossSession => "M2-CrossSession",
            PropertyId::S1SpecConflict => "S1-SpecConflict",
        }
    }

    /// True when the property needs an engine hook (contract §0 A5): absent
    /// hook => auto-skip with a counted line, never a silent pass.
    pub fn hook_gated(&self) -> bool {
        matches!(self, PropertyId::F7MemoryBaseline | PropertyId::F8ResourceBaseline)
    }

    /// True when the property emits H8 session-family steps (plan-format
    /// v2). These are offered only under a profile with `multi_session:
    /// true` (the pool must be configured — the reach gate exempts them
    /// otherwise, exactly like the hook-gated pair).
    pub fn needs_sessions(&self) -> bool {
        matches!(self, PropertyId::M2CrossSession | PropertyId::S1SpecConflict)
    }
}

/// The 13-property v1 set (11 unconditional + F7/F8 hook-gated).
pub fn v1_set() -> Vec<PropertyId> {
    vec![
        PropertyId::F1InsertSelect,
        PropertyId::F2UpdateVisibility,
        PropertyId::F3DeleteAbsence,
        PropertyId::F4ConstraintError,
        PropertyId::F5DdlEffects,
        PropertyId::F6AggConsistency,
        PropertyId::F7MemoryBaseline,
        PropertyId::F8ResourceBaseline,
        PropertyId::L1Tlp,
        PropertyId::L2NoRec,
        PropertyId::X1ArmEquivalence,
        PropertyId::X2IndexInvariance,
        PropertyId::X4StatementForm,
        PropertyId::M1ReadYourWrites,
        PropertyId::C1CursorWalk,
        PropertyId::C2HoldCursor,
        PropertyId::M2CrossSession,
        PropertyId::S1SpecConflict,
    ]
}

pub fn unconditional_v1() -> Vec<PropertyId> {
    v1_set()
        .into_iter()
        .filter(|p| !p.hook_gated() && !p.needs_sessions())
        .collect()
}

/// Schema view passed by WS-GEN. Most v1 properties are self-local (they
/// create and drop their own property-local tables); the metamorphic
/// properties (L1 TLP / L2 NoREC) ALSO target live generator tables through
/// this view — the typed-generator surface (joins, mutated data, indexes) is
/// where the bug-finding power lives. Empty view (unit/sim tests) => the
/// live arms simply never generate.
#[derive(Debug, Clone, Default)]
pub struct SchemaView {
    pub tables: Vec<SchemaTable>,
}

/// Predicate-relevant column kind (the metamorphic predicate grammar only
/// compares int-family and text columns; everything else is Other).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredColKind {
    Int,
    Text,
    Other,
}

#[derive(Debug, Clone)]
pub struct SchemaCol {
    pub name: String,
    pub kind: PredColKind,
}

#[derive(Debug, Clone)]
pub struct SchemaTable {
    /// Current SQL-visible name (valid at this plan position).
    pub name: String,
    /// Stable birth id (`t<N>`) — the table-dependency / shrinker key space.
    pub birth_id: String,
    /// `id` (always first, Int, NOT NULL unique) + payload columns.
    pub cols: Vec<SchemaCol>,
}

/// Profile knobs the oracle-side generation consumes (profiles/*.json schema
/// is WS-RUNNER's; WS-GEN threads the parsed view through).
#[derive(Debug, Clone)]
pub struct ProfileView {
    pub float_lenient: bool,
    pub iso_mix: Vec<IsoLevel>,
    /// Serial-safe arm sets (X1/L2). Parallel-forcing arms are allowed in
    /// profiles but comparisons stay exact-type (contract §3.3, R7).
    pub arm_sets: Vec<Vec<(String, String)>>,
}

impl Default for ProfileView {
    fn default() -> Self {
        ProfileView {
            float_lenient: false,
            iso_mix: vec![
                IsoLevel::ReadCommitted,
                IsoLevel::RepeatableRead,
                IsoLevel::Serializable,
            ],
            arm_sets: vec![
                vec![("work_mem".to_string(), "64kB".to_string())],
                vec![("enable_seqscan".to_string(), "off".to_string())],
            ],
        }
    }
}

/// Registry dispatch: WS-GEN instantiates a property via this single entry
/// point, threading the one ChaCha8 stream (seed/RNG law §1.2).
pub fn generate(
    id: PropertyId,
    rng: &mut impl Rng,
    schema: &SchemaView,
    profile: &ProfileView,
) -> PropertyInstance {
    match id {
        PropertyId::F1InsertSelect => f1_insert_select::generate(rng, schema, profile),
        PropertyId::F2UpdateVisibility => f2_update_visibility::generate(rng, schema, profile),
        PropertyId::F3DeleteAbsence => f3_delete_absence::generate(rng, schema, profile),
        PropertyId::F4ConstraintError => f4_constraint_error::generate(rng, schema, profile),
        PropertyId::F5DdlEffects => f5_ddl_effects::generate(rng, schema, profile),
        PropertyId::F6AggConsistency => f6_agg_consistency::generate(rng, schema, profile),
        PropertyId::F7MemoryBaseline => f7_memory_baseline::generate(rng, schema, profile),
        PropertyId::F8ResourceBaseline => f8_resource_baseline::generate(rng, schema, profile),
        PropertyId::L1Tlp => l1_tlp::generate(rng, schema, profile),
        PropertyId::L2NoRec => l2_norec::generate(rng, schema, profile),
        PropertyId::X1ArmEquivalence => x1_arm_equivalence::generate(rng, schema, profile),
        PropertyId::X2IndexInvariance => x2_index_invariance::generate(rng, schema, profile),
        PropertyId::X4StatementForm => x4_statement_form::generate(rng, schema, profile),
        PropertyId::M1ReadYourWrites => m1_read_your_writes::generate(rng, schema, profile),
        PropertyId::C1CursorWalk => c1_cursor_walk::generate(rng, schema, profile),
        PropertyId::C2HoldCursor => c2_hold_cursor::generate(rng, schema, profile),
        PropertyId::M2CrossSession => m2_cross_session::generate(rng, schema, profile),
        PropertyId::S1SpecConflict => s1_spec_conflict::generate(rng, schema, profile),
    }
}

// ---- shared generation helpers (property-local tables) ----

pub(crate) mod helpers {
    use rand::Rng;
    use std::collections::BTreeSet;

    use crate::oracle::check::{Row, Value};
    use crate::oracle::ledger::{ColumnDef, LedgerOp};
    use crate::oracle::pstep::{Mark, ProbeSpec, SqlMeta, SqlStep, PStep};

    /// Fresh property-local table name (rng-derived; properties drop their
    /// table at the end, so serial reuse is safe even on collision).
    pub fn fresh_table(rng: &mut impl Rng, tag: &str) -> String {
        format!("shp_{tag}_{:05}", rng.gen_range(0u32..100_000))
    }

    /// Standard property-local shape: k bigint PRIMARY KEY, v bigint (nullable).
    pub fn kv_cols() -> Vec<ColumnDef> {
        vec![
            ColumnDef { name: "k".into(), not_null: true },
            ColumnDef { name: "v".into(), not_null: false },
        ]
    }

    pub fn create_kv(table: &str) -> SqlStep {
        SqlStep {
            sql: format!("CREATE TABLE {table} (k bigint PRIMARY KEY, v bigint)"),
            mark: Mark::Mutation,
            meta: SqlMeta::default(),
            ledger_op: Some(LedgerOp::CreateTable {
                table: table.into(),
                cols: kv_cols(),
                key: Some(0),
            }),
            probe: None,
            stackref: None,
        }
    }

    pub fn drop_table(table: &str) -> SqlStep {
        SqlStep {
            sql: format!("DROP TABLE {table}"),
            mark: Mark::Mutation,
            meta: SqlMeta::default(),
            ledger_op: Some(LedgerOp::DropTable { table: table.into() }),
            probe: None,
            stackref: None,
        }
    }

    /// Deterministic k/v rows: keys are distinct, non-negative; v draws in
    /// 0..100 with ~1/4 NULLs (3VL fodder for L1/F6).
    pub fn gen_rows(rng: &mut impl Rng, n: usize) -> Vec<Row> {
        let mut keys = BTreeSet::new();
        while keys.len() < n {
            keys.insert(rng.gen_range(0i64..1_000_000));
        }
        keys.into_iter()
            .map(|k| {
                let v = if rng.gen_range(0u32..4) == 0 {
                    Value::Null
                } else {
                    Value::Int(rng.gen_range(0i64..100))
                };
                Row(vec![Value::Int(k), v])
            })
            .collect()
    }

    pub fn insert_rows(table: &str, rows: &[Row]) -> SqlStep {
        let vals: Vec<String> = rows
            .iter()
            .map(|r| {
                let cols: Vec<String> = r.0.iter().map(|v| v.sql()).collect();
                format!("({})", cols.join(", "))
            })
            .collect();
        SqlStep {
            sql: format!("INSERT INTO {table} (k, v) VALUES {}", vals.join(", ")),
            mark: Mark::Mutation,
            meta: SqlMeta::default(),
            ledger_op: Some(LedgerOp::InsertValues { table: table.into(), rows: rows.to_vec() }),
            probe: None,
            stackref: None,
        }
    }

    /// Metamorphic-property local shape: k bigint PRIMARY KEY, v bigint
    /// (nullable), s text (nullable) — two nullable payload columns so both
    /// int and text predicates get genuine 3VL exercise.
    pub fn kvs_cols() -> Vec<ColumnDef> {
        vec![
            ColumnDef { name: "k".into(), not_null: true },
            ColumnDef { name: "v".into(), not_null: false },
            ColumnDef { name: "s".into(), not_null: false },
        ]
    }

    /// SQL column names of the kvs shape, index-aligned with `kvs_cols` (the
    /// PredSpec col-index -> name mapping for property-local predicates).
    pub const KVS_COL_NAMES: [&str; 3] = ["k", "v", "s"];

    pub fn create_kvs(table: &str) -> SqlStep {
        SqlStep {
            sql: format!("CREATE TABLE {table} (k bigint PRIMARY KEY, v bigint, s text)"),
            mark: Mark::Mutation,
            meta: SqlMeta::default(),
            ledger_op: Some(LedgerOp::CreateTable {
                table: table.into(),
                cols: kvs_cols(),
                key: Some(0),
            }),
            probe: None,
            stackref: None,
        }
    }

    /// Deterministic kvs rows: distinct non-negative keys; v in 0..100 with
    /// ~1/4 NULLs; s in 's00'..'s49' with ~1/4 NULLs (3VL fodder — the
    /// IS NULL partition must be genuinely populated, not decoration).
    pub fn gen_rows_kvs(rng: &mut impl Rng, n: usize) -> Vec<Row> {
        let mut keys = BTreeSet::new();
        while keys.len() < n {
            keys.insert(rng.gen_range(0i64..1_000_000));
        }
        keys.into_iter()
            .map(|k| {
                let v = if rng.gen_range(0u32..4) == 0 {
                    Value::Null
                } else {
                    Value::Int(rng.gen_range(0i64..100))
                };
                let s = if rng.gen_range(0u32..4) == 0 {
                    Value::Null
                } else {
                    Value::Text(format!("s{:02}", rng.gen_range(0u32..50)))
                };
                Row(vec![Value::Int(k), v, s])
            })
            .collect()
    }

    pub fn insert_rows_kvs(table: &str, rows: &[Row]) -> SqlStep {
        let vals: Vec<String> = rows
            .iter()
            .map(|r| {
                let cols: Vec<String> = r.0.iter().map(|v| v.sql()).collect();
                format!("({})", cols.join(", "))
            })
            .collect();
        SqlStep {
            sql: format!("INSERT INTO {table} (k, v, s) VALUES {}", vals.join(", ")),
            mark: Mark::Mutation,
            meta: SqlMeta::default(),
            ledger_op: Some(LedgerOp::InsertValues { table: table.into(), rows: rows.to_vec() }),
            probe: None,
            stackref: None,
        }
    }

    pub fn count_where_key(table: &str, key: &Value, slot: u32) -> SqlStep {
        SqlStep {
            sql: format!("SELECT count(*) FROM {table} WHERE k = {}", key.sql()),
            mark: Mark::Read,
            meta: SqlMeta::default(),
            ledger_op: None,
            probe: Some(ProbeSpec::CountWhereKeyEq { table: table.into(), key: key.clone() }),
            stackref: Some(slot),
        }
    }

    pub fn count_all(table: &str, slot: u32) -> SqlStep {
        SqlStep {
            sql: format!("SELECT count(*) FROM {table}"),
            mark: Mark::Read,
            meta: SqlMeta::default(),
            ledger_op: None,
            probe: Some(ProbeSpec::CountAll { table: table.into() }),
            stackref: Some(slot),
        }
    }

    pub fn select_all(table: &str, slot: u32) -> SqlStep {
        SqlStep {
            sql: format!("SELECT k, v FROM {table}"),
            mark: Mark::Read,
            meta: SqlMeta::default(),
            ledger_op: None,
            probe: Some(ProbeSpec::SelectAll { table: table.into() }),
            stackref: Some(slot),
        }
    }

    pub fn sql(step: SqlStep) -> PStep {
        PStep::Sql(step)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_counts_match_contract() {
        // Contract §3.1.1 enumerated F1–F6, L1/L2, X1/X2/X4, M1 (+F7/F8
        // hook-gated) = 14 total / 12 unconditional at H1. H8 adds the
        // cursor pair (C1/C2, unconditional) and the multi-session estate
        // (M2/S1, session-gated — A1's dropped M2 reinstated on the v2
        // format): 18 total, 14 unconditional (C1/C2 join), F7/F8 hook-
        // gated + M2/S1 session-gated excluded.
        assert_eq!(v1_set().len(), 18);
        assert_eq!(unconditional_v1().len(), 14);
        assert_eq!(v1_set().iter().filter(|p| p.needs_sessions()).count(), 2);
    }
}
