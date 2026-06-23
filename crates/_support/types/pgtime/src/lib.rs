//! Timezone-aware broken-down time vocabulary (`pgtime.h`), trimmed to the
//! items ports consume so far.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod pgtime;

pub use pgtime::*;
