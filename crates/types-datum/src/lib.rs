//! `Datum` and varlena machinery (`postgres.h`, `varatt.h`).

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod datum;
pub mod expandeddatum;

pub use datum::*;
pub use expandeddatum::ExpandedObjectRef;
