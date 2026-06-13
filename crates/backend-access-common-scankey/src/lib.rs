//! `access/common/scankey.c` — scan-key initializer.
//!
//! C's `ScanKeyInit(entry, attributeNumber, strategy, procedure, argument)`
//! stamps the search-condition fields and then resolves the comparison
//! function eagerly with `fmgr_info(procedure, &entry->sk_func)`. The struct
//! ([`ScanKeyData`]) is the C-literal carrier in `types-scan`; the eager
//! fmgr resolution is the one piece that crosses a seam, so this initializer
//! lives in its own unit (a leaf consumer of the fmgr seam) rather than in
//! the leaf `types-scan` crate.

#![allow(non_snake_case)]

use backend_utils_fmgr_fmgr_seams as fmgr_seams;
use types_core::catalog::C_COLLATION_OID;
use types_core::{AttrNumber, InvalidOid, RegProcedure};
use types_datum::Datum;
use types_error::PgResult;
use types_scan::scankey::{ScanKeyData, StrategyNumber};

/// `ScanKeyInit(entry, attributeNumber, strategy, procedure, argument)`
/// (`access/common/scankey.c`) — the recommended shorthand for hardwired
/// catalog lookups: flags and subtype zero, collation always
/// `C_COLLATION_OID` (correct for all collation-aware catalog columns,
/// ignored for the rest).
///
/// Stamps the plain fields, then resolves the comparison procedure the way C
/// does (`fmgr_info(procedure, &entry->sk_func)`): the lookup crosses the
/// fmgr seam ([`fmgr_seams::fmgr_info_check`]), preserving C's eager
/// lookup-failure surface. Until the fmgr unit lands that seam panics — which
/// is correct, this is exactly where C does the work.
pub fn ScanKeyInit(
    entry: &mut ScanKeyData,
    attribute_number: AttrNumber,
    strategy: StrategyNumber,
    procedure: RegProcedure,
    argument: Datum,
) -> PgResult<()> {
    entry.sk_flags = 0;
    entry.sk_attno = attribute_number;
    entry.sk_strategy = strategy;
    entry.sk_subtype = InvalidOid;
    entry.sk_collation = C_COLLATION_OID;
    entry.sk_argument = argument;
    // C: fmgr_info(procedure, &entry->sk_func). The resolved FmgrInfo cannot
    // cross the seam (it embeds the C function pointer); the lookup half runs
    // behind the seam and the carrier records the resolved function's OID.
    fmgr_seams::fmgr_info_check::call(procedure)?;
    entry.sk_func = types_core::fmgr::FmgrInfo { fn_oid: procedure, ..Default::default() };
    Ok(())
}
