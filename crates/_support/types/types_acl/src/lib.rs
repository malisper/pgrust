//! ACL vocabulary: `AclMode` privilege bits (`nodes/parsenodes.h`) and the
//! `AclResult` check outcome (`utils/acl.h`). Trimmed to the items ports
//! currently consume.

#![no_std]
#![allow(non_upper_case_globals)]

pub mod acl;

pub use acl::*;
