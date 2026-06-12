//! Seam declarations for the `backend-access-rmgrdesc-spgdesc` unit (`spgdesc.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `spg_desc(buf, record)` (spgdesc.c) — append the record's description to
    /// `buf` (`rm_desc` slot). Appending allocates, so the C OOM
    /// `ereport(ERROR)` surface is `Err`.
    pub fn spg_desc(buf: &mut String, record: &types_wal::rmgr::XLogReaderState) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `spg_identify(info)` (spgdesc.c) — symbolic name of the record type
    /// (`rm_identify` slot); `None` for an unrecognized info byte.
    pub fn spg_identify(info: u8) -> Option<&'static str>
);
