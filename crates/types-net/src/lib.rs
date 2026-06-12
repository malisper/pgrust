//! Connection / socket / latch vocabulary (`libpq/pqcomm.h`,
//! `libpq/libpq-be.h`, `libpq/hba.h`, `storage/latch.h`), trimmed to what
//! ports consume.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod net;

pub use net::*;
