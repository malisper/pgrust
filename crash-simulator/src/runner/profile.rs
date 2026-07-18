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
    if p.connections != 1 {
        return Err(format!(
            "profile '{}': connections={} rejected — H1 is serial single-session (contract §0 A1); connections is pinned to 1",
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
    if p.statement_weights.get("arm").copied().unwrap_or(0) > 0 && p.arm_sets.is_empty() {
        return Err(format!(
            "profile '{}': statement kind 'arm' is weighted but arm_sets is empty (no arm can be generated)",
            p.name
        ));
    }
    if p.steps_min == 0 || p.steps_min > p.steps_max {
        return Err(format!("profile '{}': bad steps range", p.name));
    }
    // The reach-gate teeth knob must name registered productions — a typo
    // here would silently disable nothing while the test believes otherwise.
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
