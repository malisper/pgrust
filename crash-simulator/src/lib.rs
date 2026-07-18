//! simharness — H1 sim-harness core.
//!
//! Module ownership (contract §1.1/§5):
//!   plan     — WS-GEN (FROZEN after inc-1)
//!   property — WS-GEN declares the cross-WS trait surface (inc-1); WS-ORACLE
//!              implements properties against it in src/oracle/
//!   gen      — WS-GEN
//!   oracle/, vocab — WS-ORACLE (not present yet)
//!   runner/, main.rs — WS-RUNNER (not present yet)

pub mod gen;
pub mod plan;
pub mod property;
