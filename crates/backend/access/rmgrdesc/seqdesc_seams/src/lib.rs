//! Seam declarations for the `backend-access-rmgrdesc-seqdesc` unit (`seqdesc.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `seq_desc(buf, record)` (seqdesc.c) — append the record's description to
    /// `buf` (`rm_desc` slot). Appending allocates, so the C OOM
    /// `ereport(ERROR)` surface is `Err`.
    pub fn seq_desc(buf: &mut mcx::PgString<'_>, record: &wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `seq_identify(info)` (seqdesc.c) — symbolic name of the record type
    /// (`rm_identify` slot); `None` for an unrecognized info byte.
    pub fn seq_identify(info: u8) -> Option<&'static str>
);
