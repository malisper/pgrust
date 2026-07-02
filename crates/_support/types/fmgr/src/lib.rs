#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod fcinfo;
pub mod getarg;

pub use fcinfo::*;
pub use getarg::*;

#[cfg(test)]
mod tests;
