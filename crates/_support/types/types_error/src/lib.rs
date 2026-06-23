//! Error vocabulary: severity levels and SQLSTATE codes (`utils/elog.h`,
//! `utils/errcodes.h` via `errcodes.txt`) plus the owned error value
//! [`PgError`] and its result alias [`PgResult`].

#![no_std]

extern crate alloc;

pub mod error;
pub mod pg_error;

pub use error::*;
pub use pg_error::*;
