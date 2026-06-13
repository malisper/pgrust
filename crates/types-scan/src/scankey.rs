//! Scan-key vocabulary (`access/skey.h`, `access/stratnum.h`).

use types_core::fmgr::FmgrInfo;
use types_core::{AttrNumber, Oid};
use types_datum::Datum;

/// `StrategyNumber` (`access/stratnum.h`).
pub type StrategyNumber = u16;

pub const InvalidStrategy: StrategyNumber = 0;
pub const BTLessStrategyNumber: StrategyNumber = 1;
pub const BTLessEqualStrategyNumber: StrategyNumber = 2;
pub const BTEqualStrategyNumber: StrategyNumber = 3;
pub const BTGreaterEqualStrategyNumber: StrategyNumber = 4;
pub const BTGreaterStrategyNumber: StrategyNumber = 5;

// `sk_flags` bits (`access/skey.h`).
/// `SK_ISNULL` — `sk_argument` is NULL.
pub const SK_ISNULL: i32 = 0x0001;
/// `SK_SEARCHNULL` — scankey represents "col IS NULL".
pub const SK_SEARCHNULL: i32 = 0x0040;

// nbtree-private `sk_flags` bits (`access/nbtree.h`).
/// `SK_BT_SKIP` — skip array on a column without an input `=` condition.
pub const SK_BT_SKIP: i32 = 0x00040000;
/// `SK_BT_MINVAL` — invalid `sk_argument`, use low_compare.
pub const SK_BT_MINVAL: i32 = 0x00080000;
/// `SK_BT_MAXVAL` — invalid `sk_argument`, use high_compare.
pub const SK_BT_MAXVAL: i32 = 0x00100000;

/// `ScanKeyData` (`access/skey.h`) — one search condition for an index or heap
/// scan. `sk_func` is trimmed to the procedure OID ([`FmgrInfo`]); the scan
/// code performs the real fmgr lookup when it consumes the key.
#[derive(Clone, Debug)]
pub struct ScanKeyData {
    pub sk_flags: i32,
    pub sk_attno: AttrNumber,
    pub sk_strategy: StrategyNumber,
    pub sk_subtype: Oid,
    pub sk_collation: Oid,
    pub sk_func: FmgrInfo,
    pub sk_argument: Datum,
}

impl ScanKeyData {
    pub fn empty() -> Self {
        Self {
            sk_flags: 0,
            sk_attno: 0,
            sk_strategy: InvalidStrategy,
            sk_subtype: 0,
            sk_collation: 0,
            sk_func: FmgrInfo::empty(),
            sk_argument: Datum::null(),
        }
    }
}
