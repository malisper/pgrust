//! WAL record vocabulary (`access/xlogrecord.h`, `access/xlogreader.h`,
//! `access/rmgr.h`, `access/xact.h`): the record header, decoded-record shapes,
//! and the transaction-record vocabulary shared by the WAL units. Trimmed to
//! the items current ports consume.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod wal;
pub mod xact;

pub use wal::*;
pub use xact::*;
