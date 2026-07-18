//! simharness — H1 sim-harness core (contract: harness-h1-contract.md).
//!
//! Module ownership (contract §1.1/§5):
//!   plan     — WS-GEN (FROZEN after inc-1)
//!   property — WS-GEN declares the cross-WS trait surface (inc-1); WS-ORACLE
//!              implements properties against it in src/oracle/
//!   gen      — WS-GEN
//!   oracle/, vocab — WS-ORACLE (Property enum v1, ledger, result-stack
//!              checks, C-differential classifier pinned to
//!              scripts/sqlsmith/triage.py @ ef070d066, wart allowlist,
//!              R5 escalation)
//!   runner/, main.rs — WS-RUNNER (session driver, run loop, bugbase,
//!              shrinker v1, verdict emission)

pub mod bridge;
pub mod gen;
pub mod metrics;
pub mod oracle;
pub mod plan;
pub mod property;
pub mod runner;
pub mod vocab;
