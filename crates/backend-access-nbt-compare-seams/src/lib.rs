//! Seam declarations for the `nbtcompare.c` sortsupport / skipsupport strategy
//! routines (`backend-access-nbt-compare`).
//!
//! C's `bt*sortsupport` / `bt*skipsupport` strategy routines mutate a live
//! `SortSupportData` / `SkipSupportData` node by storing C function pointers
//! into its `comparator` / `increment` / `decrement` slots. In this repo those
//! slots are `Copy` tokens
//! ([`SortComparatorId`](types_sortsupport::SortComparatorId) /
//! [`SkipSupportIncDecId`](types_sortsupport::SkipSupportIncDecId)) that only
//! the sort / skip *substrate* knows how to mint and interpret. So the field
//! write is delegated to an install seam.
//!
//! These seams are **OUTWARD**: they are owned by the sort/skip substrate
//! (`utils/sort/sortsupport.c` and the unported `PrepareSkipSupportFromOpclass`
//! owner), NOT by `nbtcompare`. `nbtcompare` *calls* them but does NOT install
//! them — until the substrate lands they panic loudly ("seam not installed"),
//! which faithfully reflects that no skip/sort comparator dispatch is wired yet.
//! The comparison / increment / decrement *kernels* themselves remain pure and
//! in `backend-access-nbt-compare`.

#![allow(non_snake_case)]

use types_core::Oid;
use types_datum::Datum;
use types_sortsupport::{SkipSupportData, SortSupportData};

/// A SortSupport fast comparator: C `int (*comparator)(Datum, Datum,
/// SortSupport)` minus the third `ssup` argument the in-core fast comparators
/// (`btint2fastcmp` / `ssup_datum_int32_cmp` / `ssup_datum_signed_cmp` /
/// `btoidfastcmp`) never read. The two operands are the packed scalar `Datum`s
/// exactly as C passes them; the substrate mints a
/// [`types_sortsupport::SortComparatorId`] token denoting this function pointer.
pub type FastComparator = fn(Datum, Datum) -> i32;

// ===========================================================================
// run_sortsupport — invoke the type's `*sortsupport` strategy routine.
//
// INWARD seam owned by `nbtcompare`. This is the owned-model stand-in for the
// C `OidFunctionCall1(sortfunc, PointerGetDatum(ssup))` in
// `FinishSortSupportFunction` / `PrepareSortSupportFromGistIndexRel`: the C
// type-specific sortsupport function receives the live `SortSupport` and fills
// `ssup->comparator`. An owned `Datum` cannot carry the `SortSupport` pointer,
// so the dispatch crosses as a typed `&mut SortSupportData` here, keyed by the
// function OID, instead of through the (pointer-less) fmgr boundary.
//
// Returns `true` if `sortfunc` is a sortsupport routine this crate implements
// (it ran and may have set `ssup.comparator`); `false` if the OID is not one of
// nbtcompare's in-core sortsupport functions, so the caller can fall through to
// its fmgr path (which loud-fails for an as-yet-unported sortsupport builtin,
// matching C reaching an unregistered/undefined function).
// ===========================================================================

seam_core::seam!(
    /// `OidFunctionCall1(sortfunc, PointerGetDatum(ssup))` for an in-core btree
    /// `*sortsupport` routine: dispatch by OID to `btint2/4/8/oidsortsupport`,
    /// each of which sets `ssup->comparator`. Returns whether `sortfunc` was a
    /// recognized nbtcompare sortsupport function.
    pub fn run_sortsupport(sortfunc: Oid, ssup: &mut SortSupportData<'_>) -> bool
);

// ===========================================================================
// run_skipsupport — invoke the type's `*skipsupport` strategy routine.
//
// INWARD seam owned by `nbtcompare`. This is the owned-model stand-in for the
// C `OidFunctionCall1(skipSupportFunction, PointerGetDatum(sksup))` in
// `PrepareSkipSupportFromOpclass` (utils/adt/skipsupport.c): the C type-specific
// skipsupport function receives the live `SkipSupport` and fills
// `sksup->low_elem` / `high_elem` / `decrement` / `increment`. An owned `Datum`
// cannot carry the `SkipSupport` pointer, so the dispatch crosses as a typed
// `&mut SkipSupportData` here, keyed by the function OID, instead of through the
// (pointer-less) fmgr boundary.
//
// Returns `true` if `skipfunc` is a skipsupport routine this crate implements
// (it ran and filled `sksup`); `false` if the OID is not one of nbtcompare's
// in-core skipsupport functions, so the caller can fall through to its fmgr path
// (which loud-fails for an as-yet-unported skipsupport builtin, matching C
// reaching an unregistered/undefined function).
// ===========================================================================

seam_core::seam!(
    /// `OidFunctionCall1(skipfunc, PointerGetDatum(sksup))` for an in-core btree
    /// `*skipsupport` routine: dispatch by OID to
    /// `btbool/int2/int4/int8/oid/charskipsupport`, each of which fills every
    /// `sksup` field. Returns whether `skipfunc` was a recognized nbtcompare
    /// skipsupport function.
    pub fn run_skipsupport(skipfunc: Oid, sksup: &mut SkipSupportData) -> bool
);

// ===========================================================================
// sortsupport: install the type's fast comparator into `ssup.comparator`.
//
// Each routine corresponds to `ssup->comparator = <fastcmp>;` in C. The
// substrate registers the supplied fast comparator (`btint2fastcmp`,
// `ssup_datum_int32_cmp`, `ssup_datum_signed_cmp` / `btint8fastcmp`,
// `btoidfastcmp`) and stores the resulting token into `ssup.comparator`.
// `nbtcompare` passes the kernel; the substrate mints + interprets the token.
// ===========================================================================

seam_core::seam!(
    /// `ssup->comparator = btint2fastcmp;` (btint2sortsupport).
    pub fn install_sortsupport_int2(ssup: &mut SortSupportData<'_>, cmp: FastComparator)
);

seam_core::seam!(
    /// `ssup->comparator = ssup_datum_int32_cmp;` (btint4sortsupport).
    pub fn install_sortsupport_int4(ssup: &mut SortSupportData<'_>, cmp: FastComparator)
);

seam_core::seam!(
    /// `ssup->comparator = ssup_datum_signed_cmp;` on `SIZEOF_DATUM >= 8` (the
    /// only platform target), else `btint8fastcmp` (btint8sortsupport).
    pub fn install_sortsupport_int8(ssup: &mut SortSupportData<'_>, cmp: FastComparator)
);

seam_core::seam!(
    /// `ssup->comparator = btoidfastcmp;` (btoidsortsupport).
    pub fn install_sortsupport_oid(ssup: &mut SortSupportData<'_>, cmp: FastComparator)
);

// ===========================================================================
// skipsupport: install the type's increment / decrement callbacks.
//
// Each routine corresponds to `sksup->decrement = <type>_decrement;` and
// `sksup->increment = <type>_increment;` in C. The boundary `low_elem` /
// `high_elem` Datums are computed in-crate and stored on the node directly by
// the strategy routine before the install; the substrate only needs to wire
// the increment / decrement callbacks (it mints the
// [`SkipSupportIncDecId`](types_sortsupport::SkipSupportIncDecId) tokens for
// the named type's kernels).
// ===========================================================================

seam_core::seam!(
    /// `sksup->decrement = bool_decrement; sksup->increment = bool_increment;`
    /// (btboolskipsupport).
    pub fn install_skipsupport_bool(sksup: &mut SkipSupportData)
);

seam_core::seam!(
    /// `sksup->decrement = int2_decrement; sksup->increment = int2_increment;`
    /// (btint2skipsupport).
    pub fn install_skipsupport_int2(sksup: &mut SkipSupportData)
);

seam_core::seam!(
    /// `sksup->decrement = int4_decrement; sksup->increment = int4_increment;`
    /// (btint4skipsupport).
    pub fn install_skipsupport_int4(sksup: &mut SkipSupportData)
);

seam_core::seam!(
    /// `sksup->decrement = int8_decrement; sksup->increment = int8_increment;`
    /// (btint8skipsupport).
    pub fn install_skipsupport_int8(sksup: &mut SkipSupportData)
);

seam_core::seam!(
    /// `sksup->decrement = oid_decrement; sksup->increment = oid_increment;`
    /// (btoidskipsupport).
    pub fn install_skipsupport_oid(sksup: &mut SkipSupportData)
);

seam_core::seam!(
    /// `sksup->decrement = char_decrement; sksup->increment = char_increment;`
    /// (btcharskipsupport).
    pub fn install_skipsupport_char(sksup: &mut SkipSupportData)
);
