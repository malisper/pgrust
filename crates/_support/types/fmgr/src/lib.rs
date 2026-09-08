#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

// A function's address is not an identity: builtin / wrapper identity reads
// the resolution record (FmgrInfo::fn_kind / fn_body), never a pointer compare.
#![deny(unpredictable_function_pointer_comparisons)]

extern crate alloc;

pub mod agg;
pub mod callctx;
pub mod fcinfo;
pub mod getarg;
pub mod overlaps;
pub mod result;
pub mod rsinfo;
pub mod soft;
pub mod wire;

pub use agg::*;
pub use callctx::*;
pub use fcinfo::*;
pub use getarg::*;
pub use overlaps::*;
pub use result::*;
pub use rsinfo::*;
pub use soft::*;
pub use wire::*;

#[cfg(test)]
mod tests;
