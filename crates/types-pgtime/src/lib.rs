//! Timezone-library vocabulary (`pgtime.h`, `src/timezone/pgtz.h`,
//! `src/timezone/tzfile.h`): the broken-down time `pg_tm`, the loaded-zone
//! `pg_tz`, and the parsed transition `state` shared by the timezone units
//! (localtime.c, pgtz.c) and the datetime/formatting consumers of `pg_tm`.
//! Trimmed to the items current ports consume.

#![no_std]
#![allow(non_camel_case_types)]

pub mod pgtime;

pub use pgtime::*;
