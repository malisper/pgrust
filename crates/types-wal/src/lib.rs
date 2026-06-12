//! WAL record vocabulary (`access/xlogrecord.h`, `access/xlogreader.h`,
//! `access/rmgr.h`, plus the per-subsystem `xl_*` record payloads): the record
//! header, decoded-record shapes, and typed record payloads shared by the WAL
//! units. Trimmed to the items current ports consume.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod rmgrdesc;
pub mod wal;

pub use rmgrdesc::*;
pub use wal::*;
