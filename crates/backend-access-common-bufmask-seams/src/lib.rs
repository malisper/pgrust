//! Seam declarations for the `backend-access-common-bufmask` unit
//! (`access/common/bufmask.c`): the WAL-consistency page-masking helpers the
//! rmgr `rm_mask` callbacks call. The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.
//!
//! The masks mutate the page in place at fixed `PageHeaderData` offsets (and
//! the hole between `pd_lower` and `pd_upper`), so each seam takes the page
//! bytes by `&mut`.

seam_core::seam!(
    /// `mask_page_lsn_and_checksum(page)` (bufmask.c) — set `pd_lsn` and
    /// `pd_checksum` to `MASK_MARKER` (0). Infallible (fixed-offset writes).
    pub fn mask_page_lsn_and_checksum(page: &mut [u8])
);

seam_core::seam!(
    /// `mask_page_hint_bits(page)` (bufmask.c) — mask the page-level hint bits
    /// that may differ between primary and standby without WAL logging: the
    /// `pd_prune_xid` and the `pd_flags`
    /// `PD_PAGE_FULL`/`PD_HAS_FREE_LINES`/`PD_ALL_VISIBLE` hint bits, all set
    /// to `MASK_MARKER`. Infallible (fixed-offset writes).
    pub fn mask_page_hint_bits(page: &mut [u8])
);

seam_core::seam!(
    /// `mask_unused_space(page)` (bufmask.c) — memset the hole between
    /// `pd_lower` and `pd_upper` to `MASK_MARKER` (0), after a sanity check
    /// that `elog(ERROR)`s on invalid `pd_lower`/`pd_upper`/`pd_special`
    /// (carried on `Err`).
    pub fn mask_unused_space(page: &mut [u8]) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `mask_lp_flags(page)` (bufmask.c) — mask each line pointer's `lp_flags`
    /// (which may be changed without WAL on btree/heap leaf pages) to
    /// `MASK_MARKER`. Infallible (fixed-offset writes).
    pub fn mask_lp_flags(page: &mut [u8])
);

seam_core::seam!(
    /// `mask_page_content(page)` (bufmask.c) — mask the whole page content
    /// (everything past `SizeOfPageHeaderData`, plus `pd_lower`/`pd_upper`) to
    /// `MASK_MARKER`. Used by index AMs (e.g. hash `LH_UNUSED_PAGE`) where the
    /// contents of deleted/unused pages must be almost completely ignored for
    /// consistency checking. Infallible (fixed-offset writes).
    pub fn mask_page_content(page: &mut [u8])
);
