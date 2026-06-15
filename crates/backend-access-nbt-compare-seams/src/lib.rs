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

use types_sortsupport::{SkipSupportData, SortSupportData};

// ===========================================================================
// sortsupport: install the type's fast comparator into `ssup.comparator`.
//
// Each routine corresponds to `ssup->comparator = <fastcmp>;` in C. The
// substrate registers the appropriate fast comparator (`btint2fastcmp`,
// `ssup_datum_int32_cmp`, `ssup_datum_signed_cmp` / `btint8fastcmp`,
// `btoidfastcmp`) and stores the resulting token into `ssup.comparator`.
// ===========================================================================

seam_core::seam!(
    /// `ssup->comparator = btint2fastcmp;` (btint2sortsupport).
    pub fn install_sortsupport_int2(ssup: &mut SortSupportData<'_>)
);

seam_core::seam!(
    /// `ssup->comparator = ssup_datum_int32_cmp;` (btint4sortsupport).
    pub fn install_sortsupport_int4(ssup: &mut SortSupportData<'_>)
);

seam_core::seam!(
    /// `ssup->comparator = ssup_datum_signed_cmp;` on `SIZEOF_DATUM >= 8` (the
    /// only platform target), else `btint8fastcmp` (btint8sortsupport).
    pub fn install_sortsupport_int8(ssup: &mut SortSupportData<'_>)
);

seam_core::seam!(
    /// `ssup->comparator = btoidfastcmp;` (btoidsortsupport).
    pub fn install_sortsupport_oid(ssup: &mut SortSupportData<'_>)
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
