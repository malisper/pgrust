//! Vocabulary for the PostgreSQL `jsonb` type.
//!
//! [`jsonb`] holds the on-disk ABI surface (`Jsonb`, `JsonbContainer`, `JEntry`,
//! the `jbvType`/`JsonbIteratorToken`/`JsonbIterState` enums and every flag /
//! accessor), mirroring `postgres-18.3/src/include/utils/jsonb.h`.
//! [`backend_utils_adt_jsonb_util`] holds the *in-memory* working types
//! (`JsonbValue`, `JsonbPair`, `JsonbParseState`, `JsonbIterator`) that
//! `jsonb_util.c` operates on. They are never stored on disk and are modeled as
//! idiomatic owned-tree Rust types.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

/// `VARHDRSZ`, the varlena length-header size in bytes (single source of truth).
pub use types_datum::VARHDRSZ;

pub mod backend_utils_adt_jsonb_util;
pub mod jsonb;
pub mod jsonb_gin;
