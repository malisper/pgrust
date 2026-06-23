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

use fmgr_seams as fmgr_seams;
use ::types_core::catalog::C_COLLATION_OID;
use ::types_core::fmgr::FmgrInfo;
use ::types_core::{AttrNumber, InvalidOid, Oid, RegProcedure};
use ::types_error::PgResult;
use ::types_scan::scankey::{ScanKeyData, StrategyNumber, SK_SEARCHNOTNULL, SK_SEARCHNULL};
use types_tuple::heaptuple::Datum;

/// `ScanKeyEntryInitialize(entry, flags, attributeNumber, strategy, subtype,
/// collation, procedure, argument)` (`access/common/scankey.c`) — initialize a
/// scan key entry given all the field values. The target procedure is
/// specified by OID, but may be invalid when `SK_SEARCHNULL`/`SK_SEARCHNOTNULL`
/// is set.
///
/// Stamps the plain fields, then — when the procedure is valid — resolves the
/// comparison procedure the way C does (`fmgr_info(procedure, &entry->sk_func)`):
/// the lookup crosses the fmgr seam ([`fmgr_seams::fmgr_info_check`]),
/// preserving C's eager lookup-failure surface, and the carrier records the
/// resolved function's OID. When the procedure is invalid, C asserts the
/// caller set a NULL-search flag and `MemSet`s `sk_func` to zero; here that is
/// the empty [`FmgrInfo`].
pub fn ScanKeyEntryInitialize<'mcx>(
    entry: &mut ScanKeyData<'mcx>,
    flags: i32,
    attribute_number: AttrNumber,
    strategy: StrategyNumber,
    subtype: Oid,
    collation: Oid,
    procedure: RegProcedure,
    argument: Datum<'mcx>,
) -> PgResult<()> {
    entry.sk_flags = flags;
    entry.sk_attno = attribute_number;
    entry.sk_strategy = strategy;
    entry.sk_subtype = subtype;
    entry.sk_collation = collation;
    entry.sk_argument = argument;
    // C: if (RegProcedureIsValid(procedure)) fmgr_info(...); else { Assert(...);
    // MemSet(&entry->sk_func, 0, ...) }. RegProcedureIsValid is `!= InvalidOid`.
    if procedure != InvalidOid {
        fmgr_seams::fmgr_info_check::call(procedure)?;
        entry.sk_func = FmgrInfo { fn_oid: procedure, ..Default::default() };
    } else {
        debug_assert!(flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL) != 0);
        entry.sk_func = FmgrInfo::empty();
    }
    Ok(())
}

/// `ScanKeyEntryInitializeWithInfo(entry, flags, attributeNumber, strategy,
/// subtype, collation, finfo, argument)` (`access/common/scankey.c`) —
/// initialize a scan key entry using an already-completed `FmgrInfo` lookup
/// record.
///
/// Stamps the plain fields, then `fmgr_info_copy(&entry->sk_func, finfo,
/// CurrentMemoryContext)`. In the owned [`FmgrInfo`] model the carrier holds no
/// per-context subsidiary state (no `fn_extra`/`fn_mcxt`), so the copy that C
/// performs is a plain value copy of the resolved metadata.
pub fn ScanKeyEntryInitializeWithInfo<'mcx>(
    entry: &mut ScanKeyData<'mcx>,
    flags: i32,
    attribute_number: AttrNumber,
    strategy: StrategyNumber,
    subtype: Oid,
    collation: Oid,
    finfo: &FmgrInfo,
    argument: Datum<'mcx>,
) {
    entry.sk_flags = flags;
    entry.sk_attno = attribute_number;
    entry.sk_strategy = strategy;
    entry.sk_subtype = subtype;
    entry.sk_collation = collation;
    entry.sk_argument = argument;
    // C: fmgr_info_copy(&entry->sk_func, finfo, CurrentMemoryContext).
    entry.sk_func = finfo.clone();
}

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
pub fn ScanKeyInit<'mcx>(
    entry: &mut ScanKeyData<'mcx>,
    attribute_number: AttrNumber,
    strategy: StrategyNumber,
    procedure: RegProcedure,
    argument: Datum<'mcx>,
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
    entry.sk_func = ::types_core::fmgr::FmgrInfo { fn_oid: procedure, ..Default::default() };
    Ok(())
}
