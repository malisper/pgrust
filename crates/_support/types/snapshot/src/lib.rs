//! Snapshot vocabulary (`utils/snapshot.h`).
//!
//! Carries the snapshot type tag plus the MVCC payload and snapshot-manager
//! bookkeeping the `utils/time/snapmgr.c` owner consumes.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod snapshot;

pub use snapshot::{SnapshotData, SnapshotType};
