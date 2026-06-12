//! Snapshot vocabulary (`utils/snapshot.h`), trimmed.
//!
//! Ports so far consume only the snapshot's type tag (the `IsMVCCSnapshot`
//! test and the `SnapshotAny` identity); the payload model grows when the
//! snapshot-owning unit (`utils/time/snapmgr.c`) lands.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod snapshot;

pub use snapshot::{SnapshotData, SnapshotType};
