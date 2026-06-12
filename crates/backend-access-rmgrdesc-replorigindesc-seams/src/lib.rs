//! Seam declarations for the `backend-access-rmgrdesc-replorigindesc` unit (`replorigindesc.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `replorigin_desc(buf, record)` (replorigindesc.c) — append the record's description to
    /// `buf` (`rm_desc` slot). Appending allocates, so the C OOM
    /// `ereport(ERROR)` surface is `Err`.
    pub fn replorigin_desc(buf: &mut String, record: &types_wal::rmgr::XLogReaderState) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `replorigin_identify(info)` (replorigindesc.c) — symbolic name of the record type
    /// (`rm_identify` slot); `None` for an unrecognized info byte.
    pub fn replorigin_identify(info: u8) -> Option<&'static str>
);
