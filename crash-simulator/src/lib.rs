//! simharness — H1 sim-harness core (contract: harness-h1-contract.md).
//!
//! WS-ORACLE modules live here: `vocab` (outcome-class vocabulary pinned to
//! scripts/sqlsmith/triage.py @ ef070d066) and `oracle` (Property enum v1,
//! ledger, result-stack checks, C-differential classifier, wart allowlist,
//! R5 escalation). WS-GEN adds `gen/` + `plan.rs`; WS-RUNNER adds `runner/`
//! + `main.rs`. File-ownership fences per contract §5.

pub mod oracle;
pub mod vocab;
