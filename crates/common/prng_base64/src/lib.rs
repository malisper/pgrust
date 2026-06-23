//! Port of PostgreSQL's `common-prng-base64` unit.
//!
//! Covers two `src/common/` files:
//! - `base64.c` — base64 encode/decode without whitespace ([`base64`]).
//! - `prng.c` — already ported as the standalone `pg-prng` crate; re-exported
//!   here as [`prng`] so the combined catalog unit's full surface lives in one
//!   place without duplicating the (audited PASS) PRNG implementation.

#![no_std]

#[cfg(test)]
extern crate alloc;

pub mod base64;

/// Re-export of the `pg-prng` crate (`src/common/prng.c`).
pub mod prng {
    pub use prng::*;
}

/// This crate is a pure leaf: no seams to install. Present so the wiring layer
/// can call it uniformly, mirroring every other ported crate.
pub fn init_seams() {}
