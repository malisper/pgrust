//! Node-state vocabulary for `backend-executor-nodeTidrangescan`.
//!
//! These types appear in the signatures of the node's seams, so they live in a
//! types crate that both the owning node crate and its `-seams` crate can name.
//!
//! `TidOpExpr` / `TidRangeScanState` mirror `nodeTidrangescan.c`. They were
//! relocated into `types-nodes` (`nodetidrangescan`) so the central
//! `PlanStateNode` dispatch enum can name `TidRangeScanState` as a variant
//! without forcing a `types-nodes -> types-tidrange` cycle — the same move the
//! `nodeTidscan` `TidScanState` followed. This crate re-exports them so existing
//! `tidrange::{...}` paths keep working unchanged.

#![allow(non_snake_case)]

pub use ::nodes::execnodes::ScanStateData;
pub use ::nodes::nodetidrangescan::{
    OperandSide, TidExprType, TidOpExpr, TidRangeScanState,
};
