//! Tuple representation: heap/minimal/index tuple layouts, tuple descriptors
//! (`access/htup.h`, `access/htup_details.h`, `access/tupdesc.h`), and the
//! owned formed/deformed tuple model shared with the heaptuple unit.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod access;
pub mod backend_access_common_heaptuple;
pub mod heap;
pub mod heaptuple;
pub mod parse;

pub use access::*;
pub use backend_access_common_heaptuple::*;
pub use heap::*;
pub use heaptuple::*;
pub use parse::*;
