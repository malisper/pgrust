//! Multibyte / wide-character vocabulary (`mb/pg_wchar.h`).

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod encoding;
pub mod wchar;

pub use wchar::*;
