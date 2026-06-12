//! Cumulative-statistics type vocabulary (`pgstat.h`,
//! `utils/pgstat_internal.h`, `utils/backend_progress.h`), trimmed to the
//! items ports consume so far.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod activity_pgstat;
pub mod backend_progress;
pub mod backend_utils_activity_pgstat_bgwriter;
pub mod wait_event;
