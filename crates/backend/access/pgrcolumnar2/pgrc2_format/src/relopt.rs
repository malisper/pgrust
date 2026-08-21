//! Reloption vocabulary (spec §17; ruling O-9): hint-primary shred policy
//! with an auto-election floor; per-part permanence until compaction. This
//! is the RULED surface exactly — new reloptions are A-lane amendments.
//! M3-H registers them; M3-D consumes them.

/// Comma-separated shred path hints; syntax = the vendored `jsonb_shred`
/// path grammar (M3-B provenance) — this crate does not parse paths.
pub const RELOPT_SHRED_PATHS: &str = "pgrc2_shred_paths";
pub const RELOPT_SHRED_PATHS_DEFAULT: &str = "";

/// The shred path budget (O-9: budget ownership rides this vocabulary).
pub const RELOPT_SHRED_MAX_PATHS: &str = "pgrc2_shred_max_paths";
pub const RELOPT_SHRED_MAX_PATHS_DEFAULT: i32 = 64;

/// Auto-election floor: a path present in ≥ floor of sampled documents may
/// be elected without a hint.
pub const RELOPT_SHRED_AUTO_FLOOR: &str = "pgrc2_shred_auto_floor";
pub const RELOPT_SHRED_AUTO_FLOOR_DEFAULT: f64 = 0.95;

/// Parsed shred options (the writer-side view).
#[derive(Debug, Clone, PartialEq)]
pub struct ShredOptions {
    pub paths: String,
    pub max_paths: i32,
    pub auto_floor: f64,
}

impl Default for ShredOptions {
    fn default() -> ShredOptions {
        ShredOptions {
            paths: RELOPT_SHRED_PATHS_DEFAULT.to_string(),
            max_paths: RELOPT_SHRED_MAX_PATHS_DEFAULT,
            auto_floor: RELOPT_SHRED_AUTO_FLOOR_DEFAULT,
        }
    }
}
