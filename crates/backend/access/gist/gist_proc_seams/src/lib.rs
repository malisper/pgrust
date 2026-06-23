//! Seam declarations for the `gistproc.c` `gist_point_sortsupport` strategy
//! routine (`backend-access-gist-proc`).
//!
//! C's `gist_point_sortsupport` mutates a live `SortSupportData` node by storing
//! C function pointers into its `comparator` / `abbrev_converter` /
//! `abbrev_abort` / `abbrev_full_comparator` slots:
//!
//! ```c
//! if (ssup->abbreviate) {
//!     ssup->comparator           = ssup_datum_unsigned_cmp;
//!     ssup->abbrev_converter     = gist_bbox_zorder_abbrev_convert;
//!     ssup->abbrev_abort         = gist_bbox_zorder_abbrev_abort;
//!     ssup->abbrev_full_comparator = gist_bbox_zorder_cmp;
//! } else {
//!     ssup->comparator           = gist_bbox_zorder_cmp;
//! }
//! ```
//!
//! In this repo those slots are `Copy` tokens
//! ([`SortComparatorId`](::types_sortsupport::SortComparatorId) /
//! [`AbbrevConverterId`](::types_sortsupport::AbbrevConverterId) /
//! [`AbbrevAbortId`](::types_sortsupport::AbbrevAbortId)) that only the sort
//! *substrate* knows how to mint and interpret. So the field write is delegated
//! to install seams.
//!
//! These seams are **OUTWARD**: they are owned + installed by the sort substrate
//! (`utils/sort/sortsupport.c`, `backend-utils-sort-sortsupport`), NOT by
//! `gist-proc`. `gist_point_sortsupport` *calls* them with its native kernels,
//! exactly mirroring `nbtcompare`'s `install_sortsupport_*` precedent. The
//! z-order comparison / converter *kernels* themselves remain pure and in
//! `backend-access-gist-proc`; the substrate mints + interprets the tokens.
//!
//! `ssup_datum_unsigned_cmp` (the abbreviated-key comparator) is a sortsupport.c
//! primitive, so it is supplied by the substrate itself — `gist_point_sortsupport`
//! only hands over the gist-specific full comparator + converter + abort kernels.

#![allow(non_snake_case)]

use ::types_sortsupport::SortSupportData;
use ::types_tuple::Datum;

/// A GiST box sort comparator kernel: C `int (*comparator)(Datum, Datum,
/// SortSupport)` (`gist_bbox_zorder_cmp`). The two operands are the canonical
/// by-reference `Datum<'_>` (a `ByRef` `BOX` image — a GiST sort key is
/// pass-by-reference, so it crosses as `ByRef`, NOT a bare word); the substrate
/// mints a [`::types_sortsupport::SortComparatorId`] token denoting this function
/// pointer. The kernel never reads the third `ssup` argument, so it is dropped.
pub type GistComparator = fn(Datum<'_>, Datum<'_>) -> i32;

/// A GiST abbreviated-key converter kernel: C `Datum (*abbrev_converter)(Datum
/// original, SortSupport)` (`gist_bbox_zorder_abbrev_convert`). The `original`
/// operand is the canonical by-reference `Datum<'_>` (`ByRef` `BOX` image); the
/// result is the pass-by-value abbreviated key (the Z-order word) carried as a
/// `ByVal` `Datum<'static>`. The kernel never reads the `ssup` argument.
pub type GistAbbrevConverter = fn(Datum<'_>) -> Datum<'static>;

seam_core::seam!(
    /// `ssup->comparator = gist_bbox_zorder_cmp;` — the non-abbreviated arm of
    /// `gist_point_sortsupport`. The substrate registers `cmp` as a GiST
    /// box comparator and stores its [`SortComparatorId`](::types_sortsupport::SortComparatorId)
    /// token into `ssup.comparator`.
    pub fn install_gist_sortsupport_comparator(
        ssup: &mut SortSupportData<'_>,
        cmp: GistComparator,
    )
);

seam_core::seam!(
    /// The abbreviated arm of `gist_point_sortsupport`:
    /// `ssup->comparator = ssup_datum_unsigned_cmp;`
    /// `ssup->abbrev_converter = gist_bbox_zorder_abbrev_convert;`
    /// `ssup->abbrev_abort = gist_bbox_zorder_abbrev_abort;`
    /// `ssup->abbrev_full_comparator = gist_bbox_zorder_cmp;`
    ///
    /// `full_cmp` is the authoritative GiST box comparator (moved into
    /// `abbrev_full_comparator`); `converter` is the Z-order abbreviation
    /// converter. The substrate installs its own `ssup_datum_unsigned_cmp` into
    /// `ssup.comparator` (a sortsupport.c primitive) and the always-`false`
    /// `gist_bbox_zorder_abbrev_abort` into `ssup.abbrev_abort` (a constant the
    /// substrate need not be handed). All four tokens are minted by the
    /// substrate.
    pub fn install_gist_sortsupport_abbrev(
        ssup: &mut SortSupportData<'_>,
        full_cmp: GistComparator,
        converter: GistAbbrevConverter,
    )
);
