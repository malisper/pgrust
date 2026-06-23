//! Per-node instrumentation vocabulary (executor/instrument.h).
//!
//! The counter vocabulary lives in `types-core::instrument` (lowest layer
//! that holds it); this module re-exports the executor-facing type.

pub use types_core::instrument::Instrumentation;
