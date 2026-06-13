//! Scan vocabulary: scan keys (`access/skey.h`, `access/stratnum.h`), scan
//! direction (`access/sdir.h`), and the opaque handles by which systable scans
//! and snapshots cross seams. Trimmed to the items ports consume so far.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod backend_access_index_genam;
pub mod genam;
pub mod scankey;
pub mod sdir;
pub mod snapshot;

pub use genam::*;
pub use scankey::*;
pub use sdir::*;
pub use snapshot::*;
