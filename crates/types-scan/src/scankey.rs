//! Scan keys (`access/skey.h`), trimmed to the caller-supplied arguments of
//! `ScanKeyInit` (`access/common/scankey.c`).

use types_core::primitive::{AttrNumber, RegProcedure};
use types_datum::datum::Datum;
use types_hash::hash::StrategyNumber;

/// `BTEqualStrategyNumber` (`access/stratnum.h`) — btree equality.
pub const BTEqualStrategyNumber: StrategyNumber = 3;

/// `ScanKeyData` (`access/skey.h`), trimmed to what `ScanKeyInit` takes from
/// the caller: the attribute the key binds to, the operator strategy, the
/// comparison procedure, and the argument datum. The fields `ScanKeyInit`
/// fills itself (`sk_flags = 0`, `sk_subtype = InvalidOid`,
/// `sk_collation = C_COLLATION_OID`, the `fmgr_info` lookup behind
/// `sk_func`) are the scan owner's to supply.
#[derive(Clone, Copy, Debug)]
pub struct ScanKeyData {
    /// `sk_attno` — table or index column number the key constrains.
    pub sk_attno: AttrNumber,
    /// `sk_strategy` — operator strategy number, e.g.
    /// [`BTEqualStrategyNumber`].
    pub sk_strategy: StrategyNumber,
    /// The `RegProcedure procedure` argument of `ScanKeyInit` — the
    /// comparison function's pg_proc OID (`F_OIDEQ`, `F_INT4EQ`, ...).
    pub sk_func: RegProcedure,
    /// `sk_argument` — the datum to compare against.
    pub sk_argument: Datum,
}
