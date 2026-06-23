//! Seam declarations for the `backend-access-rmgrdesc-xactdesc` unit (`xactdesc.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `xact_desc(buf, record)` (xactdesc.c) — append the record's description to
    /// `buf` (`rm_desc` slot). Appending allocates, so the C OOM
    /// `ereport(ERROR)` surface is `Err`.
    pub fn xact_desc(buf: &mut mcx::PgString<'_>, record: &wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `xact_identify(info)` (xactdesc.c) — symbolic name of the record type
    /// (`rm_identify` slot); `None` for an unrecognized info byte.
    pub fn xact_identify(info: u8) -> Option<&'static str>
);

seam_core::seam!(
    /// `ParseCommitRecord(info, xlrec, &parsed)` (xactdesc.c) — parse an
    /// `xl_xact_commit` record body; the port returns the relation locators
    /// removed on commit (`parsed.xlocators[0..nrels]`), allocated in `mcx`
    /// (C points them into the record buffer). `Err` carries the OOM.
    pub fn parse_commit_record<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        info: u8,
        data: &[u8],
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_storage::RelFileLocator>>
);

seam_core::seam!(
    /// `ParseAbortRecord(info, xlrec, &parsed)` (xactdesc.c) — parse an
    /// `xl_xact_abort` record body; the port returns the relation locators
    /// removed on abort (`parsed.xlocators[0..nrels]`), allocated in `mcx`.
    /// `Err` carries the OOM.
    pub fn parse_abort_record<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        info: u8,
        data: &[u8],
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_storage::RelFileLocator>>
);
