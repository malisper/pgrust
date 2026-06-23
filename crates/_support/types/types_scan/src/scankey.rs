//! Scan-key vocabulary (`access/skey.h`, `access/stratnum.h`).

extern crate alloc;

use types_core::fmgr::FmgrInfo;
use types_core::{AttrNumber, Oid};
use types_tuple::heaptuple::Datum;

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
/// `SK_UNARY` — scankey is unary (`sk_argument` unused).
pub const SK_UNARY: i32 = 0x0002;
/// `SK_ROW_HEADER` — row comparison header (`sk_argument` is the subsidiary key
/// array). In the owned model the subsidiary keys live in
/// [`ScanKeyData::sk_subkeys`].
pub const SK_ROW_HEADER: i32 = 0x0004;
/// `SK_ROW_MEMBER` — row comparison member (subsidiary key).
pub const SK_ROW_MEMBER: i32 = 0x0008;
/// `SK_ROW_END` — last subsidiary key of a row comparison.
pub const SK_ROW_END: i32 = 0x0010;
/// `SK_SEARCHARRAY` — scankey represents "col op ANY(array)"; the AM expands.
pub const SK_SEARCHARRAY: i32 = 0x0020;
/// `SK_SEARCHNULL` — scankey represents "col IS NULL".
pub const SK_SEARCHNULL: i32 = 0x0040;
/// `SK_SEARCHNOTNULL` — scankey represents "col IS NOT NULL".
pub const SK_SEARCHNOTNULL: i32 = 0x0080;
/// `SK_ORDER_BY` — scankey is for an ORDER BY (amcanorderbyop) qual.
pub const SK_ORDER_BY: i32 = 0x0100;

// nbtree-private `sk_flags` bits (`access/nbtree.h`).
/// `SK_BT_SKIP` — skip array on a column without an input `=` condition.
pub const SK_BT_SKIP: i32 = 0x00040000;
/// `SK_BT_MINVAL` — invalid `sk_argument`, use low_compare.
pub const SK_BT_MINVAL: i32 = 0x00080000;
/// `SK_BT_MAXVAL` — invalid `sk_argument`, use high_compare.
pub const SK_BT_MAXVAL: i32 = 0x00100000;

/// `SK_BT_INDOPTION_SHIFT` (`access/nbtree.h`) — the per-column `indoption`
/// bits are stored in `sk_flags` shifted up by this amount.
pub const SK_BT_INDOPTION_SHIFT: i32 = 24;
/// `SK_BT_DESC` (`access/nbtree.h`, `= INDOPTION_DESC << SK_BT_INDOPTION_SHIFT`)
/// — values are stored in reverse (descending) order for this key column.
pub const SK_BT_DESC: i32 = 0x0001 << SK_BT_INDOPTION_SHIFT;
/// `SK_BT_NULLS_FIRST` (`access/nbtree.h`,
/// `= INDOPTION_NULLS_FIRST << SK_BT_INDOPTION_SHIFT`) — NULLs sort first
/// instead of last for this key column.
pub const SK_BT_NULLS_FIRST: i32 = 0x0002 << SK_BT_INDOPTION_SHIFT;

/// `ScanKeyData` (`access/skey.h`) — one search condition for an index or heap
/// scan. `sk_func` is trimmed to the procedure OID ([`FmgrInfo`]); the scan
/// code performs the real fmgr lookup when it consumes the key.
#[derive(Clone, Debug)]
pub struct ScanKeyData<'mcx> {
    pub sk_flags: i32,
    pub sk_attno: AttrNumber,
    pub sk_strategy: StrategyNumber,
    pub sk_subtype: Oid,
    pub sk_collation: Oid,
    pub sk_func: FmgrInfo,
    pub sk_argument: Datum<'mcx>,
    /// Subsidiary scankeys of a `SK_ROW_HEADER` row-comparison key. In C this
    /// is `PointerGetDatum(first_sub_key)` stuffed into `sk_argument`; the owned
    /// model can't carry a pointer in the `Datum` enum, so the subsidiary array
    /// is a real typed field (opacity inherited, resolved to its real type).
    /// `None` for every non-row-header key.
    pub sk_subkeys: Option<alloc::vec::Vec<ScanKeyData<'mcx>>>,
}

impl<'mcx> ScanKeyData<'mcx> {
    pub fn empty() -> Self {
        Self {
            sk_flags: 0,
            sk_attno: 0,
            sk_strategy: InvalidStrategy,
            sk_subtype: 0,
            sk_collation: 0,
            sk_func: FmgrInfo::empty(),
            sk_argument: Datum::null(),
            sk_subkeys: None,
        }
    }

    /// Deep-clone this scan key (re-allocating its by-reference `sk_argument`
    /// bytes, and any `sk_subkeys`, in `mcx`), relifetiming it to `'b`. Used by
    /// the scan layers that copy a caller's `ScanKeyData` array into the scan's
    /// own arena (`heap_beginscan`'s `rs_key`, `systable_beginscan`).
    pub fn clone_in<'b>(
        &self,
        mcx: mcx::Mcx<'b>,
    ) -> types_error::PgResult<ScanKeyData<'b>> {
        let sk_subkeys = match &self.sk_subkeys {
            None => None,
            Some(subs) => {
                let mut out = alloc::vec::Vec::with_capacity(subs.len());
                for sub in subs {
                    out.push(sub.clone_in(mcx)?);
                }
                Some(out)
            }
        };
        Ok(ScanKeyData {
            sk_flags: self.sk_flags,
            sk_attno: self.sk_attno,
            sk_strategy: self.sk_strategy,
            sk_subtype: self.sk_subtype,
            sk_collation: self.sk_collation,
            sk_func: self.sk_func.clone(),
            sk_argument: self.sk_argument.clone_in(mcx)?,
            sk_subkeys,
        })
    }
}
