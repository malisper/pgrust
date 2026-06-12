//! Connection/socket type vocabulary (`libpq/pqcomm.h`, `libpq/libpq-be.h`,
//! `common/ip.h`), trimmed to the items ports consume so far.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod net;

pub use net::*;
