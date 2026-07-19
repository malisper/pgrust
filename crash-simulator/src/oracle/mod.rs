//! WS-ORACLE: property + differential oracles (contract §3).
//!
//! - `props`      Property enum v1 + per-property generate/check (file per property)
//! - `ledger`     the model oracle — a ledger, not a database (punt fence inside)
//! - `check`      result stack + ASSUME/ASSERT check evaluation
//! - `drive`      evaluation semantics + the ledger-backed sim executor
//! - `classifier` --diff-c ladder, triage.py-pinned (G-O2 parity gate)
//! - `warts`      known-wart allowlist (warts.toml)
//! - `escalation` automated R5 pinned-probe + parity escalation
//! - `pstep`      oracle-side step IR (lowered into WS-GEN's plan.rs steps)

pub mod check;
pub mod classifier;
pub mod drive;
pub mod escalation;
pub mod ledger;
pub mod props;
pub mod pstep;
pub mod warts;

pub use props::PropertyId;
