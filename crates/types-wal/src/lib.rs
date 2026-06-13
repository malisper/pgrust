//! WAL record vocabulary (`access/xlogrecord.h`, `access/xlogreader.h`,
//! `access/rmgr.h`, `access/xlogutils.h`, `access/xact.h`, plus the
//! per-subsystem `xl_*` record payloads): the record header, decoded-record
//! shapes, recovery-state vocabulary, the transaction-record vocabulary, and
//! typed record payloads shared by the WAL units. Trimmed to the items
//! current ports consume.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod rmgr;
pub mod rmgrdesc;
pub mod wal;
pub mod xact;
pub mod xact_records;
pub mod xlogutils;

pub use rmgrdesc::*;
pub use wal::*;
pub use xact::*;
pub use xact_records::*;
pub use xlogutils::*;
