//! Scan vocabulary: scan keys (`access/skey.h`, `access/stratnum.h`), scan
//! direction (`access/sdir.h`), and the trimmed systable scan descriptor
//! (`access/genam.h`). Trimmed to the items ports consume so far.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod genam;
pub mod scankey;
pub mod sdir;

pub use genam::*;
pub use scankey::*;
pub use sdir::*;
