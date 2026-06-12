//! Generic system-table scan vocabulary: the trimmed scan-key type
//! (`access/skey.h`) and the deformed-row shape that crosses the
//! systable-scan seam (`access/genam.h` callers).
//!
//! This keeps key construction and row interpretation in the calling unit —
//! exactly where the C `ScanKeyInit` calls and `GETSTRUCT` casts live — while
//! the genam owner only executes the scan.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

pub mod backend_access_index_genam;
pub mod scankey;
