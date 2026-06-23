//! `access/skey.h` — scan-key vocabulary.
//!
//! The table-AM scan descriptor carries scan keys through to the access
//! method's scan callback, which evaluates each key against a fetched tuple
//! (`HeapKeyTest`). That evaluation needs the comparison function (`sk_func`)
//! and the constant to compare against (`sk_argument`), so the table-AM scan
//! key is the same full `access/skey.h` carrier index scans use, re-exported
//! from [`types_scan`] (one canonical `ScanKeyData`, no trimmed sibling).

pub use ::types_scan::scankey::*;
