//! Cross-WS trait surface for properties (declared in inc-1 alongside plan.rs,
//! per contract §5 sequencing).
//!
//! WS-ORACLE owns property implementations (src/oracle/, one file per property,
//! generate-side struct + checker together). WS-GEN's generator calls through
//! `PropertyGen` and never sees checker internals.

use std::collections::BTreeSet;

use rand::RngCore;

use crate::gen::profile::GenProfile;
use crate::gen::schema::SchemaState;
use crate::plan::{Sql, Step};

/// Schema capabilities a property may require before it is offered
/// (capabilities gating, contract §2.1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Caps(pub u32);

impl Caps {
    pub const HAS_TABLE: Caps = Caps(1 << 0);
    /// Every generated table carries `id bigint PRIMARY KEY`, so this tracks
    /// HAS_TABLE in v1; kept separate for future shapes without keys.
    pub const HAS_UNIQUE_KEY: Caps = Caps(1 << 1);
    pub const HAS_INDEX: Caps = Caps(1 << 2);
    pub const HAS_INT_COL: Caps = Caps(1 << 3);
    pub const HAS_TEXT_COL: Caps = Caps(1 << 4);
    pub const HAS_FLOAT_COL: Caps = Caps(1 << 5);
    pub const HAS_NUMERIC_COL: Caps = Caps(1 << 6);
    pub const MULTIPLE_TABLES: Caps = Caps(1 << 7);

    pub fn union(self, other: Caps) -> Caps {
        Caps(self.0 | other.0)
    }

    pub fn contains(self, required: Caps) -> bool {
        self.0 & required.0 == required.0
    }
}

/// Expected per-kind step footprint of one property instantiation, consumed
/// against the plan's statement-kind budgets so the configured workload
/// distribution holds across noise and property-embedded queries.
#[derive(Debug, Clone, Copy, Default)]
pub struct Footprint {
    pub ddl: u32,
    pub dml: u32,
    pub query: u32,
    pub tx: u32,
    pub arm: u32,
    pub fault: u32,
}

/// One property instance compiled to steps.
#[derive(Debug, Clone)]
pub struct GeneratedProperty {
    pub steps: Vec<Step>,
    /// Touched-table set in BIRTH-ID space (stable across RENAME — the
    /// table-dependency API the shrinker consumes; see plan::property_table_deps).
    pub tables: BTreeSet<String>,
}

/// Constrained placeholder noise (contract §2.1.2): properties draw real
/// queries from the workload distribution, filtered through a per-property
/// constraint fn with bounded retry.
pub trait NoiseSource {
    /// Bounded-retry draw; `None` when no candidate satisfies `constraint`
    /// within the retry budget.
    fn noise_query(
        &mut self,
        rng: &mut dyn RngCore,
        constraint: &dyn Fn(&Sql) -> bool,
    ) -> Option<Sql>;
}

/// The generate-side face of a property. All randomness comes from the single
/// plan RNG stream threaded in as `rng` (seed law §1.2) — implementations must
/// not construct their own RNGs or consume ambient entropy/clock.
pub trait PropertyGen {
    /// Stable name; appears in the plan's `-- begin property '<name>'` line.
    fn name(&self) -> &'static str;

    /// Property is offered only when the current schema satisfies these.
    fn required_caps(&self) -> Caps;

    /// Expected footprint for budget steering.
    fn footprint(&self) -> Footprint;

    /// Weight in the property-choice distribution. Default: the profile's
    /// `property_weights[name]`, falling back to 1 (0 = kill-switched).
    fn weight(&self, profile: &GenProfile) -> u64 {
        profile.property_weights.get(self.name()).copied().unwrap_or(1)
    }

    /// Compile one instance to steps. `None` = preconditions not satisfiable
    /// right now (not an error; the generator falls through to noise).
    fn generate(
        &self,
        rng: &mut dyn RngCore,
        schema: &SchemaState,
        noise: &mut dyn NoiseSource,
        profile: &GenProfile,
    ) -> Option<GeneratedProperty>;
}
