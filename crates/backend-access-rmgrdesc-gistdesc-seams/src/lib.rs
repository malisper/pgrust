//! Seam declarations for the `backend-access-rmgrdesc-gistdesc` unit (`gistdesc.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `gist_desc(buf, record)` (gistdesc.c) — append the record's description to
    /// `buf` (`rm_desc` slot). Appending allocates, so the C OOM
    /// `ereport(ERROR)` surface is `Err`.
    pub fn gist_desc(buf: &mut String, record: &types_wal::rmgr::XLogReaderState) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `gist_identify(info)` (gistdesc.c) — symbolic name of the record type
    /// (`rm_identify` slot); `None` for an unrecognized info byte.
    pub fn gist_identify(info: u8) -> Option<&'static str>
);
