//! Seam declarations for the `backend-commands-sequence` unit (`sequence.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `seq_redo(record)` (sequence.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn seq_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `seq_mask(pagedata, blkno)` (sequence.c) — mask page bytes that may differ
    /// between primary and standby for WAL consistency checking (`rm_mask`
    /// slot). The bufmask helpers `elog(ERROR)` on invalid page bounds.
    pub fn seq_mask(pagedata: &mut [u8], blkno: types_core::BlockNumber) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ResetSequenceCaches()` (sequence.c) — `DISCARD ALL` / `DISCARD
    /// SEQUENCES`: drop this backend's cached sequence values so the next
    /// `nextval` re-reads from the sequence relation. May `ereport(ERROR)`.
    pub fn reset_sequence_caches() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `DeleteSequenceTuple(seqid)` (commands/sequence.c): the per-class
    /// `OCLASS_CLASS` sequence-drop handler dependency.c's `doDeletion` invokes
    /// for a sequence relation. Removes its `pg_sequence` row. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn DeleteSequenceTuple(seqid: types_core::primitive::Oid) -> types_error::PgResult<()>
);
