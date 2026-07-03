#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod array_build;
pub mod datum;
pub mod expandeddatum;
pub mod varlena;

pub use datum::*;
pub use expandeddatum::{ExpandedObjectHeader, ExpandedObjectMethods};
pub use varlena::{Bytea, Varlena, VarlenaRef, VARHDRSZ};
