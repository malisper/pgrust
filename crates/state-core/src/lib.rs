//! Per-backend STATE STUBS for not-yet-ported units: thread-local globals +
//! accessors, one module per owning C unit, NOTHING else (the state analog
//! of a decls-only `-seams` crate — see AGENTS.md "When your unit needs
//! something an unported neighbor owns").
//!
//! Layered like `types-*`: this crate holds the scalar layer (deps:
//! `types-core` only); values needing richer types live in the state crate
//! at that type's layer (e.g. `state-pgtz`). A unit's port TAKES OVER its
//! module here — it becomes the sole writer and fills in any remaining
//! globals; until then, boot values mirror the C initializers.

pub mod globals;
pub mod ipc;
pub mod miscinit;
pub mod postmaster;
