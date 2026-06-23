//! `BitmapOr` plan node (`plannodes.h`) and `BitmapOrState` executor node
//! (`execnodes.h`).
//!
//! The struct definitions now live in `types-nodes` (`nodebitmapor`) so the
//! central `PlanStateNode` dispatch enum — which lives in `types-nodes` — can
//! name `BitmapOrState` as a variant; all of `BitmapOrState`'s fields are
//! nameable from that layer, so it relocated cleanly. This module re-exports
//! them unchanged so this crate's executor logic compiles as before.

pub use nodes::nodebitmapor::{BitmapOr, BitmapOrState};
