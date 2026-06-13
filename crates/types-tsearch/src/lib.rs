//! Full-text-search type vocabulary consumed by the `tsvector_ops` index
//! support functions (`tsginidx.c`, `tsgistidx.c`) and the ranking functions
//! (`tsrank.c`).
//!
//! Sources: `src/include/tsearch/ts_type.h`, `src/include/tsearch/ts_utils.h`,
//! `src/include/access/gin.h`. Only the items the index/rank ports consume are
//! copied here.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

extern crate alloc;

pub mod tsearch;
pub mod gin;
pub mod tsgistidx;
