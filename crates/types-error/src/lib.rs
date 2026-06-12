//! Error-reporting vocabulary: error levels and SQLSTATEs (`utils/elog.h`,
//! `utils/errcodes.h`) and the owned error value `PgError` / `PgResult`.
//!
//! Trimmed to the items ports consume so far; grow per port, never wholesale.

#![no_std]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod error;
pub mod pg_error;

pub use error::*;
pub use pg_error::*;
