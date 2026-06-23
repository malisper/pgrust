//! `shared_dsm_object` — re-export shim.
//!
//! The typed, concurrently-mutated shared-DSM-object primitive
//! (`SharedRef` / `SharedSlice` / `SharedView` + the
//! `estimate`/`place_*`/`attach*`/`with_mut` helpers, plus the
//! [`SharedDsmObject`](types_parallel::SharedDsmObject) marker trait it is
//! bounded on) was relocated DOWN into `types-parallel` so the per-node
//! `repr(C)` node structs that carry a `SharedRef` field (which live in
//! `types-nodes` / `types-execparallel`) can name it without an upward
//! dependency on this crate.
//!
//! This module re-exports the relocated primitive at its historical
//! `transam_parallel::shared_dsm_object::…` path so every
//! existing call site keeps compiling unchanged. See
//! [`types_parallel::shared_dsm_object`] for the full SAFETY contract and the
//! per-item documentation.

pub use types_parallel::shared_dsm_object::*;
