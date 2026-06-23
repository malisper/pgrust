//! Vocabulary for the PostgreSQL `jsonpath` type.
//!
//! [`jsonpath`] holds the on-disk ABI surface (`JsonPath`, the
//! `JsonPathItemType` node-type enum which becomes part of the on-disk
//! representation, the version/flag header bits, and the XQuery regex-mode
//! flags), mirroring `postgres-18.3/src/include/utils/jsonpath.h`.
//!
//! [`parse`] holds the in-memory working types of `jsonpath.c`
//! (`JsonPathParseItem`, `JsonPathParseResult`, ...). They are never stored on
//! disk and never cross a C ABI boundary, so they are modelled as idiomatic
//! owned Rust types. They live here (rather than in the owning crate) because
//! the gram/scan parser seam names `JsonPathParseResult` in its signature.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod jsonpath;
pub mod parse;
