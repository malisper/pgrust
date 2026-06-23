//! Owned row/group vocabulary for the shared index-AM validation library
//! (`access/index/amvalidate.c` / `access/amvalidate.h`), shared by every AM
//! opclass validator that reaches it.

#![no_std]
#![allow(non_snake_case)]

pub mod index_amvalidate;

pub use index_amvalidate::*;
