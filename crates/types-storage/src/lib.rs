//! Storage/lmgr type vocabulary (`storage/lwlock.h`, `storage/proclist_types.h`,
//! `port/atomics.h`, `storage/waiteventset.h`, `storage/latch.h`,
//! `storage/relfilelocator.h`, `storage/sinval.h`), trimmed to the items
//! ports consume so far.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod buf;
pub mod bufpage;
pub mod latch;
pub mod lock;
pub mod relfilelocator;
pub mod sinval;
pub mod storage;
pub mod sync;
pub mod waiteventset;

pub use lock::*;
pub use relfilelocator::*;
pub use sinval::*;
pub use storage::*;
