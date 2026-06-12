//! Output-destination vocabulary from `tcop/dest.h`: the `CommandDest` codes
//! shared by elog.c's `whereToSendOutput` and (later) the tcop/dest units.

#![no_std]

pub mod dest;

pub use dest::*;
