//! Shared types, populated incrementally from ../pgrust/src-idiomatic/crates/types
//! as ports need them. Keep the source module structure so later copies land in
//! predictable places. Seam signatures may only use `types`, `std`, and primitives.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_imports)]

extern crate alloc;

pub mod backend_access_common_heaptuple;
pub mod datum;
pub mod fmgr;
pub mod heap;
pub mod heaptuple;
pub mod primitive;
pub mod xact;

pub use datum::*;
pub use fmgr::*;
pub use heap::*;
pub use heaptuple::*;
pub use primitive::*;
pub use xact::*;
