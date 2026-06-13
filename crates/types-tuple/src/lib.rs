//! Tuple representation: heap/minimal/index tuple layouts, tuple descriptors
//! (`access/htup.h`, `access/htup_details.h`, `access/tupdesc.h`), and the
//! owned formed/deformed tuple model shared with the heaptuple unit.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod access;
pub mod attmap;
pub mod backend_access_common_heaptuple;
pub mod backend_access_common_tupdesc;
pub mod heap;
pub mod heaptuple;
pub mod pg_type;
pub mod rel;
pub mod tupconvert;
pub mod toast_helper;

pub use access::*;
pub use attmap::*;
pub use backend_access_common_heaptuple::*;
pub use heap::*;
pub use heaptuple::*;
pub use rel::*;
pub use tupconvert::*;
pub use toast_helper::*;
