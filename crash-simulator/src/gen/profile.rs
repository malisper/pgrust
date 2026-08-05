//! Generator-side view of the profile JSON (contract §4.2 schema, gen subset).
//!
//! WS-RUNNER owns the full profile schema (profiles/*.json, validator,
//! kill-switches, floors, background policy). This parser reads only the
//! fields generation consumes and IGNORES unknown fields, so runner-owned
//! fields never break it. WS-GEN reviews weight fields per contract §1.1.

use std::collections::BTreeMap;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::plan::PlanError;

#[derive(Debug, Clone, Deserialize)]
pub struct PlanLen {
    pub min: u64,
    pub max: u64,
}

impl Default for PlanLen {
    fn default() -> Self {
        PlanLen { min: 40, max: 80 }
    }
}

/// Statement-kind weights (integer; no float accumulation anywhere in choice
/// arithmetic — determinism law A3).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StatementWeights {
    pub ddl: u64,
    pub dml: u64,
    pub query: u64,
    pub tx: u64,
    pub arm: u64,
    pub fault: u64,
    pub property: u64,
}

impl Default for StatementWeights {
    fn default() -> Self {
        StatementWeights { ddl: 6, dml: 30, query: 40, tx: 12, arm: 4, fault: 2, property: 6 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ColTypeWeights {
    pub int: u64,
    pub bigint: u64,
    pub text: u64,
    pub numeric: u64,
    pub float8: u64,
}

impl Default for ColTypeWeights {
    fn default() -> Self {
        ColTypeWeights { int: 4, bigint: 2, text: 3, numeric: 2, float8: 0 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TableShape {
    pub min_cols: u32,
    pub max_cols: u32,
    pub col_types: ColTypeWeights,
    /// H6: cap on set-based bulk-insert sizes (the cardinality lever). The
    /// runner-side profile's `table_shape.rows_max` maps here.
    pub rows_max: u32,
}

impl Default for TableShape {
    fn default() -> Self {
        TableShape {
            min_cols: 2,
            max_cols: 5,
            col_types: ColTypeWeights::default(),
            rows_max: 400,
        }
    }
}

/// Isolation-level mix for Tx Begin steps.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IsoMix {
    pub rc: u64,
    pub rr: u64,
    pub ser: u64,
}

impl Default for IsoMix {
    fn default() -> Self {
        IsoMix { rc: 70, rr: 20, ser: 10 }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GenProfile {
    pub name: String,
    #[serde(default)]
    pub plan_len: PlanLen,
    #[serde(default)]
    pub statement_weights: StatementWeights,
    #[serde(default)]
    pub table_shape: TableShape,
    #[serde(default)]
    pub iso_mix: IsoMix,
    /// Serial-safe arm SETS: each inner vec is a set of [guc, value] pairs
    /// applied ATOMICALLY as consecutive arm steps (H4 fidelity fix: the p6
    /// replant showed flattening sets into independent single-GUC draws makes
    /// a 4-GUC parallel arm almost never compose in-session). An EMPTY set is
    /// the profile's base-clean control arm and lowers to RESET ALL.
    #[serde(default)]
    pub arm_sets: Vec<Vec<(String, String)>>,
    /// Per-property weights; 0 = kill-switched. Missing = 1.
    #[serde(default)]
    pub property_weights: BTreeMap<String, u64>,
    /// R7: float aggregates in compared positions allowed (and tagged) only
    /// when true.
    #[serde(default)]
    pub float_lenient: bool,
    /// TEST-ONLY reach-gate teeth knob (H5): production node names (see
    /// `gen::prodreg`) whose EMISSION is suppressed while the reach gate
    /// still considers them reachable — simulating a silently-lost
    /// production (the H3 0/9 shape). Part of the profile bytes, so plan
    /// determinism is preserved honestly. Never set in battery profiles.
    #[serde(default)]
    pub test_disable_productions: Vec<String>,
    /// H6 planner-knob swarm block (see `gen::knobs`): when present, the
    /// generator samples per-seed planner-GUC sets and appends them to the
    /// arm-set pool. Sampling reads the generator's one seeded stream, so
    /// determinism law A3 holds: same seed + profile => same knobs.
    #[serde(default)]
    pub planner_knobs: Option<crate::gen::knobs::PlannerKnobs>,
    /// H8: opt in to the multi-session estate (M2/S1 properties + the worker
    /// pool). OFF by default — session-gated properties are weighted 0 when
    /// false (the reach gate then treats them as gated-unreachable, exactly
    /// like the hook-gated F7/F8 pair off a hookless engine). The single-
    /// session cursor properties (C1/C2) are NOT gated by this: they always
    /// generate.
    #[serde(default)]
    pub multi_session: bool,
}

impl GenProfile {
    pub fn from_bytes(bytes: &[u8]) -> Result<(GenProfile, String), PlanError> {
        let profile: GenProfile = serde_json::from_slice(bytes)
            .map_err(|e| PlanError { line: 0, msg: format!("profile parse error: {e}") })?;
        if profile.name.is_empty() || profile.name.chars().any(|c| c.is_whitespace()) {
            return Err(PlanError {
                line: 0,
                msg: "profile name must be non-empty, no whitespace".into(),
            });
        }
        if profile.plan_len.min == 0 || profile.plan_len.min > profile.plan_len.max {
            return Err(PlanError { line: 0, msg: "plan_len must satisfy 0 < min <= max".into() });
        }
        if profile.table_shape.min_cols > profile.table_shape.max_cols {
            return Err(PlanError { line: 0, msg: "table_shape min_cols > max_cols".into() });
        }
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let sha = hasher
            .finalize()
            .iter()
            .fold(String::with_capacity(64), |mut s, b| {
                use std::fmt::Write as _;
                let _ = write!(s, "{b:02x}");
                s
            });
        Ok((profile, sha))
    }
}
