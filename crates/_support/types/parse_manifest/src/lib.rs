//! Carrier types for the streaming backup-manifest JSON parser
//! (`common/parse_manifest.c`).
//!
//! `parse_manifest.c` exposes a callback-driven parser: a
//! `JsonManifestParseContext` carries five callbacks (version /
//! system-identifier / per-file / per-wal-range / error), and the incremental
//! driver (`json_parse_manifest_incremental_{init,chunk,shutdown}`) invokes
//! them as it tokenizes the manifest document. `JsonManifestParseIncrementalState`
//! is an incomplete type whose definition is private to `parse_manifest.c`;
//! callers only hold an opaque pointer.
//!
//! The owning unit (`common/parse_manifest.c`) is not ported yet, so the
//! parser state is a registry token the owner maps to the live parser, and the
//! decoded records the parser would feed to the callbacks are carried back to
//! the consumer as these typed values for it to replay through its own
//! `manifest_process_*` callbacks (which is exactly what the C callbacks do).
//! The genuine struct is defined when `parse_manifest.c` lands.

#![no_std]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use ::types_core::{TimeLineID, XLogRecPtr};

/// Opaque handle to a `JsonManifestParseIncrementalState *`
/// (`common/parse_manifest.h` — an incomplete type whose definition is private
/// to `parse_manifest.c`). A registry token the owner maps to the live
/// streaming-parser state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct JsonManifestParseIncrementalStateHandle(pub u64);

/// `pg_checksum_type` (`common/checksum_helper.h`): the checksum algorithm
/// recorded for a file in the backup manifest. The numeric mapping is the
/// persisted on-disk one (`CHECKSUM_TYPE_NONE = 0` .. `CHECKSUM_TYPE_SHA512 =
/// 5`). The incremental-backup `manifest_process_file` callback discards the
/// checksum fields, but they are carried for faithful parity with the C
/// `json_manifest_per_file_callback` signature.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum PgChecksumType {
    /// `CHECKSUM_TYPE_NONE`
    None = 0,
    /// `CHECKSUM_TYPE_CRC32C`
    Crc32c = 1,
    /// `CHECKSUM_TYPE_SHA224`
    Sha224 = 2,
    /// `CHECKSUM_TYPE_SHA256`
    Sha256 = 3,
    /// `CHECKSUM_TYPE_SHA384`
    Sha384 = 4,
    /// `CHECKSUM_TYPE_SHA512`
    Sha512 = 5,
}

/// One record decoded by the `json_manifest_per_file_callback`: the arguments
/// the C parser would pass to `per_file_cb(context, pathname, size,
/// checksum_type, checksum_length, checksum_payload)`.
#[derive(Clone, Debug)]
pub struct ManifestFileRecord {
    pub pathname: String,
    pub size: u64,
    pub checksum_type: PgChecksumType,
    pub checksum_length: i32,
    pub checksum_payload: Vec<u8>,
}

/// One record decoded by the `json_manifest_per_wal_range_callback`: the
/// arguments to `per_wal_range_cb(context, tli, start_lsn, end_lsn)`.
#[derive(Clone, Copy, Debug)]
pub struct WalSummaryRange {
    pub tli: TimeLineID,
    pub start_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
}

/// The records decoded by one streaming-parser step
/// (`json_parse_manifest_incremental_chunk` / `_shutdown`), in document order.
///
/// The streaming JSON parser invokes the four content callbacks in document
/// order across the chunks it processes. The version / system-identifier
/// fields appear at most once in the whole manifest (so a chunk reports
/// `Some(..)` only if it decoded one), while per-file / per-wal-range records
/// append.
#[derive(Clone, Debug, Default)]
pub struct ParsedManifestChunk {
    /// The `json_manifest_version_callback` argument, if seen in this step.
    pub version: Option<i32>,
    /// The `json_manifest_system_identifier_callback` argument, if seen.
    pub system_identifier: Option<u64>,
    /// `json_manifest_per_file_callback` records, in document order.
    pub files: Vec<ManifestFileRecord>,
    /// `json_manifest_per_wal_range_callback` records, in document order.
    pub wal_ranges: Vec<WalSummaryRange>,
}
