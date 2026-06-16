//! Port of the GiST index build engine: `src/backend/access/gist/gistbuild.c`
//! and `src/backend/access/gist/gistbuildbuffers.c` (PostgreSQL 18.3).
//!
//! [`gistbuild`] drives the table scan that builds a GiST index, choosing
//! between the sorted bottom-up build and the (optionally buffered) insert
//! build; [`gistbuildempty`] writes an empty init-fork index. Both are plain
//! `pub fn`s matching the build-scan contract; the `index.c` owner that
//! dispatches them via the `IndexAmRoutine` `ambuild`/`ambuildempty` slots is
//! not ported yet, so — like the sibling AM builds (`hashbuild`, `spgbuild`,
//! `brinbuild`) — they are exported but not yet wired into a vtable.
//!
//! The buffering build node-buffer engine lives in [`gistbuildbuffers`]; the
//! drivers and callbacks in [`gistbuild`].
//!
//! This crate owns no inward seams (it is a leaf consumer), so it has no
//! companion `-seams` crate and no `init_seams()`.

#![allow(non_snake_case)]

extern crate alloc;

pub mod gistbuild;
pub mod gistbuildbuffers;

pub use gistbuild::{gistbuild, gistbuildempty};

#[cfg(test)]
mod tests;
