//! H6 planner-knob swarm sampling (the GUC arm of the reach program).
//!
//! Background, in plain terms: the H5 census showed the engine only ever
//! produced 18 distinct query-plan SHAPES under the existing profiles,
//! because the planner was always free to pick its favorite strategy (for
//! example, it almost always picks a hash join for our joins). PostgreSQL —
//! and this engine, which implements the same knobs — exposes planner GUCs
//! ("Grand Unified Configuration" settings, i.e. `SET` variables) like
//! `enable_hashjoin` that turn individual planner strategies off. Turning a
//! strategy off forces the planner onto its ALTERNATIVES (nested loop,
//! materialize, sorted aggregation, parallel plans, ...), which are exactly
//! the plan shapes the census never saw. This is "swarm testing over
//! configuration": every seed gets its own randomly-sampled knob
//! configuration, so a campaign as a whole visits many planner modes.
//!
//! Mechanics: a profile may carry a `planner_knobs` block. When it does, the
//! generator samples `sets_per_seed` knob SETS at plan start (from the same
//! seeded RNG stream as everything else — determinism law A3: same seed +
//! same profile bytes = byte-identical plan) and appends them to the
//! profile's static `arm_sets` pool. Arm steps then apply them through the
//! existing atomic arm-set machinery (H4 fidelity: a set is applied as
//! consecutive SET steps, never flattened).
//!
//! Degeneracy guards (a config that disables EVERY strategy in a family is
//! not a planner mode, it is noise — the planner falls back to
//! disabled-cost plans and the sample wastes its arm):
//! - never all of {seqscan, indexscan, bitmapscan} off in one set;
//! - never all of {hashjoin, mergejoin, nestloop} off in one set;
//! - a sampled set is never empty (an empty set would lower to RESET ALL,
//!   which the pool already has as the control arm).
//!
//! Every knob named here was checked against the engine's GUC table
//! (crates/backend/utils/misc/guc_tables/src/tables.rs) — the validator
//! rejects names outside that checked list so a typo cannot silently
//! sample a no-op configuration.

use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::gen::weights::range_incl;

/// Planner-knob swarm configuration (profile JSON block `planner_knobs`).
/// All fields are explicit — checked-in profiles spell out their sampling
/// space so the profile sha pins it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PlannerKnobs {
    /// How many knob sets to sample per seed (each becomes one extra arm
    /// set in that plan's pool). Validator bounds: 1..=8.
    pub sets_per_seed: u32,
    /// Percent chance (0..=100) that each listed boolean knob is included
    /// as `<knob>=off` in a sampled set. Independent per knob per set.
    pub off_percent: u32,
    /// The boolean `enable_*` planner GUCs that participate in sampling.
    /// Must be non-empty, duplicate-free, and drawn from
    /// `IMPLEMENTED_BOOL_KNOBS`.
    pub knobs: Vec<String>,
    /// Percent chance (0..=100) that a sampled set ALSO includes the
    /// parallel-forcing block (`PARALLEL_FORCE_SET` — the H4 parallel-arms
    /// recipe: zero parallel costs, zero size floor, 2 workers).
    pub parallel_percent: u32,
}

/// Boolean planner GUCs verified implemented by the engine
/// (guc_tables/src/tables.rs, QUERY_TUNING_METHOD group). `enable_geqo`,
/// `enable_async_append`, partitionwise and partition-pruning knobs are
/// deliberately absent: the grammar has no partitioned tables or async
/// foreign scans yet, so toggling them cannot change any reachable plan.
pub const IMPLEMENTED_BOOL_KNOBS: [&str; 18] = [
    "enable_seqscan",
    "enable_indexscan",
    "enable_indexonlyscan",
    "enable_bitmapscan",
    "enable_tidscan",
    "enable_hashjoin",
    "enable_mergejoin",
    "enable_nestloop",
    "enable_hashagg",
    "enable_sort",
    "enable_incremental_sort",
    "enable_material",
    "enable_memoize",
    "enable_gathermerge",
    "enable_parallel_hash",
    "enable_parallel_append",
    "enable_presorted_aggregate",
    "enable_distinct_reordering",
];

/// Scan-strategy family: never turn ALL of these off in one sampled set.
/// (`enable_indexonlyscan` is not in the family — it only specializes
/// `enable_indexscan`, it is not an independent way to read a table.)
pub const SCAN_GROUP: [&str; 3] = ["enable_seqscan", "enable_indexscan", "enable_bitmapscan"];

/// Join-strategy family: never turn ALL of these off in one sampled set.
pub const JOIN_GROUP: [&str; 3] = ["enable_hashjoin", "enable_mergejoin", "enable_nestloop"];

/// The parallel-forcing block (H4 parallel-arms prior art, verified against
/// the engine's GUC table): makes parallel plans cost-free so the planner
/// considers them even on tiny tables.
pub const PARALLEL_FORCE_SET: [(&str, &str); 4] = [
    ("parallel_setup_cost", "0"),
    ("parallel_tuple_cost", "0"),
    ("min_parallel_table_scan_size", "0"),
    ("max_parallel_workers_per_gather", "2"),
];

/// Percent draw: true with probability `percent`/100. Integer arithmetic
/// only (determinism law A3).
fn pct(rng: &mut dyn RngCore, percent: u32) -> bool {
    range_incl(rng, 0, 99) < percent as u64
}

/// Sample the per-seed knob sets. Called by the generator at plan start with
/// its one seeded RNG stream, so the result is a pure function of
/// (seed, profile): same seed + same profile = same knob sets, every time.
///
/// Guard order matters and is deterministic:
/// 1. draw each knob off with `off_percent` (in the profile's listed order);
/// 2. if nothing was drawn, force ONE uniformly-chosen knob off (a sampled
///    set must be a real planner point, not a duplicate control arm);
/// 3. for each strategy family, if the set turned the WHOLE family off,
///    re-enable one uniformly-chosen member (degeneracy guard);
/// 4. draw the parallel-forcing block with `parallel_percent`.
/// Step 2 runs before step 3 and adds at most one knob, so it can never
/// re-create a whole-family-off set; step 3 removes at most one member per
/// family, so the set can never become empty again.
pub fn sample_knob_sets(rng: &mut dyn RngCore, cfg: &PlannerKnobs) -> Vec<Vec<(String, String)>> {
    let mut sets = Vec::with_capacity(cfg.sets_per_seed as usize);
    for _ in 0..cfg.sets_per_seed {
        let mut offs: Vec<String> = Vec::new();
        for k in &cfg.knobs {
            if pct(rng, cfg.off_percent) {
                offs.push(k.clone());
            }
        }
        if offs.is_empty() && !cfg.knobs.is_empty() {
            let i = range_incl(rng, 0, cfg.knobs.len() as u64 - 1) as usize;
            offs.push(cfg.knobs[i].clone());
        }
        for group in [&SCAN_GROUP, &JOIN_GROUP] {
            if group.iter().all(|g| offs.iter().any(|o| o == g)) {
                let i = range_incl(rng, 0, group.len() as u64 - 1) as usize;
                let victim = group[i];
                offs.retain(|o| o != victim);
            }
        }
        let mut set: Vec<(String, String)> =
            offs.into_iter().map(|k| (k, "off".to_string())).collect();
        if pct(rng, cfg.parallel_percent) {
            set.extend(
                PARALLEL_FORCE_SET.iter().map(|(k, v)| (k.to_string(), v.to_string())),
            );
        }
        sets.push(set);
    }
    sets
}

/// Validate a `planner_knobs` block (called from the runner profile
/// validator; kept here so the knob list and its checks live together).
pub fn validate(k: &PlannerKnobs, profile_name: &str) -> Result<(), String> {
    if k.sets_per_seed == 0 || k.sets_per_seed > 8 {
        return Err(format!(
            "profile '{}': planner_knobs.sets_per_seed={} out of range 1..=8",
            profile_name, k.sets_per_seed
        ));
    }
    if k.off_percent > 100 || k.parallel_percent > 100 {
        return Err(format!(
            "profile '{}': planner_knobs percents must be 0..=100 (off={}, parallel={})",
            profile_name, k.off_percent, k.parallel_percent
        ));
    }
    if k.knobs.is_empty() {
        return Err(format!("profile '{}': planner_knobs.knobs is empty", profile_name));
    }
    let mut seen: Vec<&str> = Vec::new();
    for knob in &k.knobs {
        if !IMPLEMENTED_BOOL_KNOBS.contains(&knob.as_str()) {
            return Err(format!(
                "profile '{}': planner_knobs knob '{}' is not in the checked implemented list",
                profile_name, knob
            ));
        }
        if seen.contains(&knob.as_str()) {
            return Err(format!(
                "profile '{}': planner_knobs knob '{}' listed twice",
                profile_name, knob
            ));
        }
        seen.push(knob);
    }
    Ok(())
}
