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
    /// `mask_page_hint_bits(page)` (bufmask.c) — clear the `BTP_HAS_GARBAGE` /
    /// hint-bit flags that can differ between primary and standby without WAL
    /// logging: each item id's `LP_DEAD` bit, `pd_flags`'
    /// `PD_PAGE_FULL`/`PD_HAS_FREE_LINES`/`PD_ALL_VISIBLE`, and the heap-tuple
    /// `t_infomask` `HEAP_XMIN/XMAX_*` hint bits. Mutates the page in place;
    /// infallible (fixed-offset writes within the line-pointer / tuple area).
    pub fn mask_page_hint_bits(page: &mut [u8])
);

seam_core::seam!(
    /// `mask_unused_space(page)` (bufmask.c) — memset the hole between
    /// `pd_lower` and `pd_upper` to `MASK_MARKER` (0), after a sanity check
    /// that `elog(ERROR)`s on invalid `pd_lower`/`pd_upper`/`pd_special`
    /// (carried on `Err`).
    pub fn mask_unused_space(page: &mut [u8]) -> types_error::PgResult<()>
);
