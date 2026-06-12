//! WAL record vocabulary (`access/xlogrecord.h`, `access/xlogreader.h`,
//! `access/rmgr.h`, `access/xlogutils.h`): the record header,
//! decoded-record shapes, and recovery-state vocabulary shared by the WAL
//! units. Trimmed to the items current ports consume.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod wal;
pub mod xlogutils;

pub use wal::*;
pub use xlogutils::*;
