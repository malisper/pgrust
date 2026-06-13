//! WAL record vocabulary (`access/xlogrecord.h`, `access/xlogreader.h`,
//! `access/rmgr.h`): the record header and decoded-record shapes shared by the
//! WAL units. Trimmed to the items current ports consume.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod rmgr;
pub mod wal;

pub use wal::*;
