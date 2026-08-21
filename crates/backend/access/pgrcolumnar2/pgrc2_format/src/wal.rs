//! RESERVED — the part-seal WAL record shape (spec §14; ruling O-3).
//!
//! Single-node crash-safe is the ruled v1 scope. This type exists so
//! physical standby/PITR is a later FEATURE, never a format break: the
//! record's shape is fixed on paper. **Constructing or emitting it at M3 is
//! a contract defect** — no encode function exists, nothing references it,
//! and M3-D/M3-K prove the §13.3 ordering without it.
//!
//! Reserved redo semantics: re-establish the manifest generation and its
//! dirents idempotently; part payload bytes ride the file-copy/basebackup
//! story, not this record.

/// The reserved seal-record shape (64 B when it ever gets a wire form).
/// Fields are the table identity, the sealed part's logical identity, and
/// the manifest delta the seal publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SealWalRecordShape {
    pub relfilenumber: u64,
    pub part_file_len: u64,
    pub footer_off: u64,
    pub manifest_prev_gen: u64,
    pub manifest_gen: u64,
    pub manifest_len: u64,
    pub spc: u32,
    pub db: u32,
    pub part_no: u32,
    pub manifest_crc: u32,
}
