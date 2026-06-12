//! Storage/lmgr type vocabulary (`storage/lwlock.h`, `storage/proclist_types.h`,
//! `port/atomics.h`), trimmed to the items ports consume so far.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod storage;

pub use storage::*;
