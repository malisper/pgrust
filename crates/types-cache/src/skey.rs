//! Scan-key initializer record (`access/skey.h`), value form.
//!
//! C's `ScanKeyData` embeds an `FmgrInfo` (a resolved function pointer);
//! across a seam the key crosses as the `ScanKeyInit(&skey, attno, strategy,
//! procedure, argument)` arguments instead — the genam owner re-resolves the
//! comparison procedure with `fmgr_info`.

use types_core::{Oid, RegProcedure};
use types_datum::Datum;

/// `BTEqualStrategyNumber` (`access/stratnum.h`).
pub const BTEqualStrategyNumber: u16 = 3;

/// `F_OIDEQ` (`utils/fmgroids.h`) — `oideq`'s pg_proc OID.
pub const F_OIDEQ: RegProcedure = 184;

/// The arguments of one `ScanKeyInit` call.
#[derive(Clone, Copy, Debug)]
pub struct ScanKeyInit {
    /// `sk_attno` — attribute number of the indexed/scanned column (1-based).
    pub sk_attno: i16,
    /// `sk_strategy` — operator strategy number (`BTEqualStrategyNumber`, ...).
    pub sk_strategy: u16,
    /// `sk_procedure`/`sk_func` — the comparison function's pg_proc OID.
    pub sk_procedure: RegProcedure,
    /// `sk_argument` — the comparison value (pass-by-value word; the cache
    /// scans only ever compare by-value `oid` keys).
    pub sk_argument: Datum,
    /// `sk_subtype` (`InvalidOid` for the plain `ScanKeyInit` form).
    pub sk_subtype: Oid,
    /// `sk_collation` (`InvalidOid` for the plain `ScanKeyInit` form).
    pub sk_collation: Oid,
}
