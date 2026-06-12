//! Error-reporting vocabulary: error levels and SQLSTATEs (`utils/elog.h`,
//! `utils/errcodes.h`) plus the owned error value `PgError` and its result
//! alias `PgResult` (the carrier for C `ereport(ERROR, ...)`). Lives in the
//! types stack so seam-crate signatures can name the error channel without
//! pulling in the error-reporting subsystem.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod error;
pub mod pg_error;

pub use error::*;
pub use pg_error::*;
