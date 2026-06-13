//! Parsed relation-option structs shared between the reloptions parser
//! (`access/common/reloptions.c`) and its cache consumers.
//!
//! The C structs begin with a varlena header (`int32 vl_len_`) because the
//! parser returns them as `bytea *`; the owned model passes them by value,
//! so the header is dropped.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod attoptcache;
pub mod relopts;
pub mod tablespace;

pub use attoptcache::*;
pub use relopts::*;
pub use tablespace::*;
