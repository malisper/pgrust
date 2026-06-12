//! `access/skey.h` — scan-key vocabulary, trimmed.
//!
//! The tableam dispatch layer only carries scan keys through to the access
//! method; nothing here reads them yet, so the comparison-function and
//! argument payload (`sk_func`, `sk_argument`) land with their first
//! consumer.

use types_core::primitive::{AttrNumber, Oid};

/// `StrategyNumber` (`access/stratnum.h`) — a `uint16`.
pub type StrategyNumber = u16;

/// `ScanKeyData` (`access/skey.h`), trimmed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScanKeyData {
    /// `sk_flags` — `SK_*` flags.
    pub sk_flags: i32,
    /// `sk_attno` — table or index column number.
    pub sk_attno: AttrNumber,
    /// `sk_strategy` — operator strategy number.
    pub sk_strategy: StrategyNumber,
    /// `sk_subtype` — strategy subtype.
    pub sk_subtype: Oid,
    /// `sk_collation` — collation to use.
    pub sk_collation: Oid,
}
