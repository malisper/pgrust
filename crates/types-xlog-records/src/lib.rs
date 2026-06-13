//! Per-rmgr WAL record shapes: the `xl_*` / `*xlog*` record-body structs the
//! resource managers serialize into WAL (`access/heapam_xlog.h`,
//! `access/brin_xlog.h`, `access/ginxlog.h`, `access/gistxlog.h`,
//! `access/hash_xlog.h`, `access/nbtxlog.h`, `access/spgxlog.h`,
//! `access/multixact.h`, `storage/standbydefs.h`), one module per header,
//! trimmed to the fields ports consume so far.
//!
//! Each struct keeps its C name and field names and carries a checked
//! `from_bytes` constructor that decodes the record body at the C struct's
//! native (aligned) offsets. A record shorter than the struct it claims to
//! hold panics — the C code reads garbage or faults there. Variable-length
//! tails (`FLEXIBLE_ARRAY_MEMBER`s) are exposed as typed array views
//! ([`arrays`]) because WAL bytes are unaligned and cannot be reborrowed as
//! `&[T]`.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

pub mod arrays;
pub mod brin_xlog;
pub(crate) mod bytes;
pub mod ginxlog;
pub mod gistxlog;
pub mod hash_xlog;
pub mod heapam_xlog;
pub mod multixact;
pub mod nbtxlog;
pub mod spgxlog;
pub mod standbydefs;
