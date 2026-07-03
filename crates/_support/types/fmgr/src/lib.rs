#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod fcinfo;
pub mod getarg;
pub mod result;
pub mod soft;

pub use fcinfo::*;
pub use getarg::*;
pub use result::*;
pub use soft::*;

#[cfg(test)]
mod tests;
