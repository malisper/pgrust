//! Swarm profile schema + validator (contract §4.1.2).
//!
//! Every knob is a distribution; seeds pick a point. H1 pins `connections`
//! to 1 (serial law §0 A1) — the validator rejects anything else.
//! `engagement_floors` are DECLARED here but enforcement is instrument-gated
//! (§0 A4): when the instrument is absent the runner emits
//! `SIMHARNESS|floor-skipped-no-instrument|1` instead of asserting.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub name: String,
    pub connections: u32,
    /// iso level name -> integer weight (BTreeMap: deterministic iteration).
    pub iso_mix: BTreeMap<String, u32>,
    /// statement kind -> integer weight (ddl|dml|query|tx|arm|fault|property).
    pub statement_weights: BTreeMap<String, u32>,
    pub table_shape: TableShape,
    /// Each arm set is a list of "guc=value" strings applied via ARM steps.
    pub arm_sets: Vec<Vec<String>>,
    /// property name -> integer weight.
    pub property_weights: BTreeMap<String, u32>,
    /// Property names disabled outright.
    #[serde(default)]
    pub kill_switches: Vec<String>,
    #[serde(default)]
    pub float_lenient: bool,
    /// Declared floors; enforced only when the instrument is present (§0 A4).
    #[serde(default)]
    pub engagement_floors: BTreeMap<String, u64>,
    /// TEST-ONLY reach-gate teeth knob (H5): production node names whose
    /// EMISSION is suppressed while the reach gate still expects them —
    /// simulating a silently-lost production (the H3 0/9 shape). Validated
    /// against the `gen::prodreg` registry. Never set in battery profiles.
    #[serde(default)]
    pub test_disable_productions: Vec<String>,
    /// H6 planner-knob swarm block (see `gen::knobs`): per-seed sampled
    /// planner-GUC sets appended to the arm-set pool. Seed-deterministic
    /// (same seed + profile bytes = same knob sets).
    #[serde(default)]
    pub planner_knobs: Option<crate::gen::knobs::PlannerKnobs>,
    /// H8: opt in to the multi-session estate (M2/S1). Default false keeps a
    /// profile byte-identical in behavior to pre-H8 for the session-gated
    /// pair (the cursor pair C1/C2 always generates).
    #[serde(default)]
    pub multi_session: bool,
    pub background_policy: BackgroundPolicy,
    pub steps_min: u32,
    pub steps_max: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TableShape {
    pub tables_min: u32,
    pub tables_max: u32,
    pub cols_min: u32,
    pub cols_max: u32,
    pub rows_max: u32,
    /// H6 (H5 find 2 fix): column-type weights, keys in
    /// int|bigint|text|numeric|float8. EMPTY (or absent) = the generator's
    /// defaults. Before this field existed the bridge hardcoded the default
    /// weights (float8=0), so `float_lenient` profiles could never actually
    /// generate a float8 column and the q:float-agg family was vacuous.
    #[serde(default)]
    pub col_types: BTreeMap<String, u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BackgroundPolicy {
    /// "off" by default in every checked-in profile (spec §2.1: autovacuum
    /// timing is a real divergence driver and no H1 property needs it).
    pub autovacuum: String,
}

#[derive(Debug, Clone)]
pub struct LoadedProfile {
    pub profile: Profile,
    /// sha256 hex of the profile file's raw bytes (goes into plan headers).
    pub sha256: String,
    pub path: String,
}

pub fn load_profile(path: &str) -> Result<LoadedProfile, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("profile {}: {}", path, e))?;
    let profile: Profile =
        serde_json::from_slice(&bytes).map_err(|e| format!("profile {}: {}", path, e))?;
    validate(&profile)?;
    let mut h = Sha256::new();
    h.update(&bytes);
    Ok(LoadedProfile { profile, sha256: hex(&h.finalize()), path: path.to_string() })
}

pub fn validate(p: &Profile) -> Result<(), String> {
    if p.name.is_empty() {
        return Err("profile: name must be non-empty".into());
    }
    // H1 pinned `connections` to 1 (serial single-session, §0 A1). H8 lifts
    // that pin ONLY for a profile that opts into the multi-session estate,
    // and only within the addressable session range (session 0 = primary
    // pair, workers 1..=MAX_SESSION_ID). A non-multi_session profile is
    // still serial-only.
    let max_conns = crate::plan::MAX_SESSION_ID + 1;
    if !p.multi_session && p.connections != 1 {
        return Err(format!(
            "profile '{}': connections={} rejected — serial single-session (contract §0 A1); \
             set multi_session=true for the H8 estate, or pin connections to 1",
            p.name, p.connections
        ));
    }
    if p.multi_session && (p.connections < 2 || p.connections > max_conns) {
        return Err(format!(
            "profile '{}': multi_session connections={} out of range (2..={max_conns})",
            p.name, p.connections
        ));
    }
    const ISO: [&str; 3] = ["read-committed", "repeatable-read", "serializable"];
    for k in p.iso_mix.keys() {
        if !ISO.contains(&k.as_str()) {
            return Err(format!("profile '{}': unknown iso level '{}'", p.name, k));
        }
    }
    if p.iso_mix.values().sum::<u32>() == 0 {
        return Err(format!("profile '{}': iso_mix weights sum to zero", p.name));
    }
    const KINDS: [&str; 7] = ["ddl", "dml", "query", "tx", "arm", "fault", "property"];
    for k in p.statement_weights.keys() {
        if !KINDS.contains(&k.as_str()) {
            return Err(format!("profile '{}': unknown statement kind '{}'", p.name, k));
        }
    }
    if p.statement_weights.values().sum::<u32>() == 0 {
        return Err(format!("profile '{}': statement_weights sum to zero", p.name));
    }
    let ts = &p.table_shape;
    if ts.tables_min == 0 || ts.tables_min > ts.tables_max || ts.cols_min > ts.cols_max {
        return Err(format!("profile '{}': bad table_shape", p.name));
    }
    const COL_TYPES: [&str; 5] = ["int", "bigint", "text", "numeric", "float8"];
    for k in ts.col_types.keys() {
        if !COL_TYPES.contains(&k.as_str()) {
            return Err(format!("profile '{}': unknown col type '{}'", p.name, k));
        }
    }
    if !ts.col_types.is_empty() && ts.col_types.values().all(|w| *w == 0) {
        return Err(format!(
            "profile '{}': col_types given but all weights are zero (no column type \
             can be generated)",
            p.name
        ));
    }
    for set in &p.arm_sets {
        for arm in set {
            if !arm.contains('=') {
                return Err(format!("profile '{}': arm '{}' is not guc=value", p.name, arm));
            }
        }
    }
    // A weighted 'arm' kind with no arm sets can never emit a step — the
    // generator would draw it forever without making progress (infinite
    // loop when 'arm' is the only weighted kind). Reject at validation.
    // H6: a `planner_knobs` block guarantees per-seed sampled sets, so it
    // also satisfies the arm requirement.
    if p.statement_weights.get("arm").copied().unwrap_or(0) > 0
        && p.arm_sets.is_empty()
        && p.planner_knobs.is_none()
    {
        return Err(format!(
            "profile '{}': statement kind 'arm' is weighted but arm_sets is empty (no arm can be generated)",
            p.name
        ));
    }
    if let Some(k) = &p.planner_knobs {
        crate::gen::knobs::validate(k, &p.name)?;
    }
    if p.steps_min == 0 || p.steps_min > p.steps_max {
        return Err(format!("profile '{}': bad steps range", p.name));
    }
    // The reach-gate teeth knob must name productions the knob actually
    // HONORS, not merely registered ones. Two failure shapes are rejected:
    // a typo (unregistered), and — the H5 review F1 trap — a registered
    // production whose emission site never consults the knob (`dml:update`
    // used to validate fine, kept emitting, and silently disabled nothing
    // while the teeth test believed otherwise). The knob is honored only at
    // the `gen_query` emission site, so only query-variant names pass.
    if !p.test_disable_productions.is_empty() {
        let prop_names: Vec<String> = crate::oracle::props::v1_set()
            .iter()
            .map(|id| id.as_str().to_string())
            .collect();
        let prop_refs: Vec<&str> = prop_names.iter().map(|s| s.as_str()).collect();
        let reg = crate::gen::prodreg::registry(&prop_refs);
        for n in &p.test_disable_productions {
            if !reg.iter().any(|d| &d.name == n) {
                return Err(format!(
                    "profile '{}': test_disable_productions entry '{}' is not a registered production",
                    p.name, n
                ));
            }
            if !crate::gen::noise::teeth_knob_honored(n) {
                return Err(format!(
                    "profile '{}': test_disable_productions entry '{}' is registered but not honored at any emission site (only gen_query variants are) — it would silently disable nothing",
                    p.name, n
                ));
            }
        }
    }
    if p.background_policy.autovacuum != "off" && p.background_policy.autovacuum != "on" {
        return Err(format!("profile '{}': autovacuum must be on|off", p.name));
    }
    Ok(())
}

pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}
