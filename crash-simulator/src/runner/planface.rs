//! The runner's single import point for the plan format.
//!
//! Pre-integration: re-exports the WS-RUNNER scaffold (`plan_scaffold`).
//! At integration (WS-GEN's frozen `src/plan.rs` merged): change this one
//! line to `pub use crate::plan::*;` and delete `plan_scaffold.rs`.

pub use super::plan_scaffold::*;
