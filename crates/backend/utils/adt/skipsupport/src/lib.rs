//! Port of `src/backend/utils/adt/skipsupport.c` (PostgreSQL 18.3).
//!
//! Support routines for B-Tree skip scan: `PrepareSkipSupportFromOpclass`
//! fills in a [`SkipSupportData`](::types_sortsupport::SkipSupportData) given an
//! operator class (opfamily + opcintype). The B-Tree skip-scan preprocessing
//! code (`_bt_preprocess_array_keys`) calls it to obtain the
//! enumerate/increment/decrement support for a skipped attribute.
//!
//! ## Seam shape
//!
//! Two boundaries cross seams here:
//!
//!   * `get_opfamily_proc(opfamily, opcintype, opcintype, BTSKIPSUPPORT_PROC)`
//!     is the catalog `amproc` lookup (`utils/cache/lsyscache.c`), reached
//!     through `lsyscache_seams::get_opfamily_proc`.
//!
//!   * `OidFunctionCall1(skipSupportFunction, PointerGetDatum(sksup))` cannot
//!     cross the by-word fmgr boundary: the type-specific `BTSKIPSUPPORT_PROC`
//!     receives a `SkipSupport` *pointer* and fills its fields (including the
//!     `SkipSupportIncDec` C function pointers, modeled as opaque tokens). So
//!     it is dispatched as a typed `&mut SkipSupportData`, keyed by the
//!     function OID, through `compare_seams::run_skipsupport`
//!     (an inward seam owned by `nbtcompare`, which implements the in-core
//!     `bt*skipsupport` strategy routines — exactly mirroring the existing
//!     `run_sortsupport` dispatch).
//!
//! The single `PrepareSkipSupportFromOpclass` entry point is exposed OUTWARD
//! via `skipsupport_seams::prepare_skip_support_from_opclass`,
//! installed from [`init_seams`]; the (unported) nbtree skip-scan consumer
//! calls it.

#![allow(non_snake_case)]

use ::types_core::primitive::{Oid, OidIsValid};
use ::types_error::PgResult;
use ::types_sortsupport::SkipSupportData;

use compare_seams as nbtcompare;
use skipsupport_seams as seams;
use lsyscache_seams as lsyscache;

/// `BTSKIPSUPPORT_PROC` (`access/nbtree.h`): B-tree support function 6, the
/// skip support function.
const BTSKIPSUPPORT_PROC: i16 = 6;

/// `PrepareSkipSupportFromOpclass(Oid opfamily, Oid opcintype, bool reverse)`
/// (`utils/adt/skipsupport.c`).
///
/// Fill in `SkipSupport` given an operator class (opfamily + opcintype). On
/// success returns the skip support struct (C: allocating in caller's memory
/// context). Otherwise returns `None`, indicating that the operator class has
/// no skip support function (C: `return NULL`).
///
/// ```c
/// SkipSupport
/// PrepareSkipSupportFromOpclass(Oid opfamily, Oid opcintype, bool reverse)
/// {
///     Oid         skipSupportFunction;
///     SkipSupport sksup;
///
///     /* Look for a skip support function */
///     skipSupportFunction = get_opfamily_proc(opfamily, opcintype, opcintype,
///                                             BTSKIPSUPPORT_PROC);
///     if (!OidIsValid(skipSupportFunction))
///         return NULL;
///
///     sksup = palloc(sizeof(SkipSupportData));
///     OidFunctionCall1(skipSupportFunction, PointerGetDatum(sksup));
///
///     if (reverse)
///     {
///         Datum       low_elem = sksup->low_elem;
///         SkipSupportIncDec decrement = sksup->decrement;
///
///         sksup->low_elem = sksup->high_elem;
///         sksup->decrement = sksup->increment;
///
///         sksup->high_elem = low_elem;
///         sksup->increment = decrement;
///     }
///
///     return sksup;
/// }
/// ```
pub fn prepare_skip_support_from_opclass(
    opfamily: Oid,
    opcintype: Oid,
    reverse: bool,
) -> PgResult<Option<SkipSupportData>> {
    // Look for a skip support function.
    let skip_support_function =
        lsyscache::get_opfamily_proc::call(opfamily, opcintype, opcintype, BTSKIPSUPPORT_PROC)?;
    if !OidIsValid(skip_support_function) {
        return Ok(None);
    }

    // C: sksup = palloc(sizeof(SkipSupportData)); — a fresh (zeroed) struct
    // that the BTSKIPSUPPORT_PROC fills entirely.
    let mut sksup = SkipSupportData::new();

    // C: OidFunctionCall1(skipSupportFunction, PointerGetDatum(sksup));
    //
    // The owned `Datum` cannot carry the `SkipSupport` pointer the strategy
    // routine writes through, so the call is dispatched as a typed
    // `&mut SkipSupportData`, keyed by OID. A `false` return means the OID is
    // not one of the in-core nbtcompare skipsupport functions — that is, a
    // skip support builtin reached through fmgr that isn't ported/registered;
    // panic loudly, matching C reaching an undefined function.
    if !nbtcompare::run_skipsupport::call(skip_support_function, &mut sksup) {
        panic!(
            "PrepareSkipSupportFromOpclass: skip support function {} (BTSKIPSUPPORT_PROC) \
             is not a ported in-core skipsupport routine",
            skip_support_function
        );
    }

    if reverse {
        // DESC/reverse case: swap low_elem with high_elem, and swap decrement
        // with increment.
        let low_elem = sksup.low_elem;
        let decrement = sksup.decrement;

        sksup.low_elem = sksup.high_elem;
        sksup.decrement = sksup.increment;

        sksup.high_elem = low_elem;
        sksup.increment = decrement;
    }

    Ok(Some(sksup))
}

/// This crate owns one OUTWARD seam
/// ([`prepare_skip_support_from_opclass`](seams::prepare_skip_support_from_opclass)),
/// the single `utils/adt/skipsupport.c` entry point. The nbtree skip-scan
/// preprocessing code calls it.
pub fn init_seams() {
    seams::prepare_skip_support_from_opclass::set(prepare_skip_support_from_opclass);
}
