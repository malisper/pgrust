//! VACUUM vocabulary: the command-layer cutoff/param types
//! (`commands/vacuum.h`), the parallel-vacuum DTOs (`commands/vacuumparallel.c`,
//! `access/genam.h`), and the lazy-vacuum driver's identity handles + seam DTOs
//! (`access/heap/vacuumlazy.c`), trimmed to what the lazy-vacuum port consumes.
//!
//! An opened `Relation` (heap or index) crosses these seams + lives in the
//! `LVRelState` as its bare `RelationGetRelid` [`types_core::Oid`] identity (the
//! portable relcache key); the substrate re-resolves the live relation on
//! demand.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod vacuum;
pub mod vacuumparallel;
pub mod vacuumlazy;
