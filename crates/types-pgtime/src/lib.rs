//! Shared broken-down-time vocabulary from `src/include/pgtime.h` and the
//! timezone library's `src/timezone/private.h`: `pg_tm` plus the time-unit
//! constants. Produced by the timezone units (localtime.c/pgtz.c) and
//! consumed across datetime.c, timestamp.c, formatting.c, strftime.c, etc.
//! Trimmed to the items current ports consume.

#![no_std]
#![allow(non_camel_case_types)]

pub mod pgtime;

pub use pgtime::*;
