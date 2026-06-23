//! Syscache identifiers (`catalog/syscache_ids.h`, a generated header).
//!
//! Trimmed to the IDs ports currently consume; values verified against the
//! PG 18.3 build's generated enum (alphabetical cache-name order).

#![no_std]

pub mod syscache_ids;

pub use syscache_ids::*;
