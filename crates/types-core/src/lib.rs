//! Base scalar vocabulary: primitive aliases (`c.h`), transaction-system
//! scalars, and compile-time limits (`pg_config_manual.h`). The bottom of the
//! types-crate stack — depends on nothing.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod fmgr;
pub mod primitive;
pub mod xact;

pub use fmgr::*;
pub use primitive::*;
pub use xact::*;
