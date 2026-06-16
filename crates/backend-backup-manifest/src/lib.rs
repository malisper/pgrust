//! Port of PostgreSQL's `backup_manifest`
//! (`src/backend/backup/backup_manifest.c`, PostgreSQL 18.3).
//!
//! Code for generating and sending a backup manifest. A [`BackupManifestInfo`]
//! accumulates a JSON document (`PostgreSQL-Backup-Manifest-Version` 2)
//! describing every file in a base backup plus the WAL ranges needed to replay
//! it; the document is spooled to a temporary [`BufFile`], SHA-256-checksummed,
//! and finally streamed to the client through a [`Bbsink`].
//!
//! The manifest-construction *logic* lives here, over an owned `PgVec<u8>`
//! StringInfo-equivalent buffer:
//!  * the JSON header / per-file object / WAL-range assembly,
//!  * the running manifest SHA-256 fed every appended byte
//!    (`common-cryptohash`, an external primitive crossed via its seam),
//!  * the per-file checksum finalize + algorithm name (`common-checksum-helper`),
//!  * the timeline-history walk that emits the WAL ranges,
//!  * the buffered flush of the spooled manifest into the sink.
//!
//! Pure in-process helpers are direct deps exactly as the C file calls them
//! directly: `escape_json_with_len` (json), `hex_encode` (encode),
//! `pg_verify_mbstr` (mb), `pg_gmtime`/`pg_strftime` (timezone), and
//! `readTimeLineHistory` (timeline). `GetSystemIdentifier` crosses the xlog
//! seam. The cryptohash context is the genuine external primitive
//! (`common/cryptohash.c`), held as the raw `pg_cryptohash_ctx *` C holds.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::ffi::CStr;

use mcx::{Mcx, PgBox, PgVec};
use types_core::{pg_time_t, Oid, TimeLineID, XLogRecPtr, MAXPGPATH};
use types_crypto::pg_cryptohash_ctx;
use types_crypto::pg_cryptohash_type::PG_SHA256;
use types_error::{ErrorLocation, PgError, PgResult, ERROR};
use types_storage::file::PG_TBLSPC_DIR;
use types_wchar::encoding::PG_UTF8;

use backend_utils_error::ereport;

use backend_backup_sink::{
    bbsink_begin_manifest, bbsink_end_manifest, bbsink_manifest_contents, Bbsink, BbsinkState,
};
use backend_utils_adt_encode::hex_encode;
use backend_utils_adt_json::escape_json_with_len;
use common_wchar::pg_encoding_verifymbstr;

use backend_storage_file_buffile::{
    BufFileClose, BufFileCreateTemp, BufFileReadExact, BufFileSeek, BufFileWrite,
};
use types_nodes::nodehash::BufFile;

use backend_access_transam_timeline::readTimeLineHistory;
use backend_timezone_localtime::pg_gmtime;
use backend_timezone_strftime::pg_strftime;

use common_checksum_helper::{
    pg_checksum_final, pg_checksum_type, pg_checksum_type_name, PgChecksumContext,
    CHECKSUM_TYPE_NONE, PG_CHECKSUM_MAX_LENGTH,
};
use common_cryptohash_seams as cryptohash;
use common_sha2::{PG_SHA256_DIGEST_LENGTH, PG_SHA256_DIGEST_STRING_LENGTH};

const BACKUP_MANIFEST_C: &str = "backup_manifest.c";

/// `SEEK_SET` (stdio.h).
const SEEK_SET: i32 = 0;

/// `InvalidOid` (`postgres_ext.h`).
const InvalidOid: Oid = 0;

/// `OidIsValid(objectId)` (`c.h`).
#[inline]
fn OidIsValid(object_id: Oid) -> bool {
    object_id != InvalidOid
}

/// `XLogRecPtrIsInvalid(r)` (`access/xlogdefs.h`).
#[inline]
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == 0
}

/// `LSN_FORMAT_ARGS(lsn)` (`access/xlogdefs.h`) formatted with `%X/%X`
/// (uppercase hex, no zero padding).
#[inline]
fn lsn_format(lsn: XLogRecPtr) -> alloc_string::String {
    alloc_string::format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

mod alloc_string {
    extern crate alloc;
    pub use alloc::format;
    pub use alloc::string::String;
}

fn err_loc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new(BACKUP_MANIFEST_C, lineno, funcname)
}

/// `elog(ERROR, msg)` — internal error, no specific sqlstate.
fn elog_error(msg: alloc_string::String, lineno: i32, funcname: &'static str) -> PgError {
    ereport(ERROR)
        .errmsg_internal(msg)
        .into_error()
        .with_error_location(err_loc(lineno, funcname))
}

/// C: `typedef enum manifest_option` (`backup/backup_manifest.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackupManifestOption {
    /// `MANIFEST_OPTION_YES`
    Yes,
    /// `MANIFEST_OPTION_NO`
    No,
    /// `MANIFEST_OPTION_FORCE_ENCODE`
    ForceEncode,
}

pub use BackupManifestOption::{
    ForceEncode as MANIFEST_OPTION_FORCE_ENCODE, No as MANIFEST_OPTION_NO,
    Yes as MANIFEST_OPTION_YES,
};

/// C: `typedef struct backup_manifest_info` (`backup/backup_manifest.h`).
///
/// `buffile` is the spool temp file (`BufFile *`); `None` (C's NULL) means
/// "manifest disabled". `manifest_ctx` is the raw cryptohash context pointer C
/// `palloc`s; `None` (NULL) means uninitialized.
pub struct BackupManifestInfo<'mcx> {
    buffile: Option<PgBox<'mcx, BufFile>>,
    checksum_type: pg_checksum_type,
    manifest_ctx: *mut pg_cryptohash_ctx,
    manifest_size: u64,
    force_encode: bool,
    first_file: bool,
    still_checksumming: bool,
}

impl<'mcx> BackupManifestInfo<'mcx> {
    /// A zeroed manifest (`memset(manifest, 0, sizeof(...))`). The base-backup
    /// driver creates one of these to pass to [`InitializeBackupManifest`],
    /// which fully overwrites it; exposed publicly for that consumer.
    pub fn zeroed() -> Self {
        Self {
            buffile: None,
            checksum_type: CHECKSUM_TYPE_NONE,
            manifest_ctx: core::ptr::null_mut(),
            manifest_size: 0,
            force_encode: false,
            first_file: false,
            still_checksumming: false,
        }
    }

    /// `manifest->checksum_type` accessor (the configured per-file checksum
    /// algorithm; part of the public struct).
    pub fn checksum_type(&self) -> pg_checksum_type {
        self.checksum_type
    }

    /// `manifest->manifest_size` accessor.
    pub fn manifest_size(&self) -> u64 {
        self.manifest_size
    }
}

impl Default for BackupManifestInfo<'_> {
    fn default() -> Self {
        Self::zeroed()
    }
}

/// C: `static inline bool IsManifestEnabled(backup_manifest_info *manifest)`.
#[inline]
fn IsManifestEnabled(manifest: &BackupManifestInfo) -> bool {
    manifest.buffile.is_some()
}

// ---------------------------------------------------------------------------
// StringInfo-equivalent buffer helpers (fallible mcx allocation).
// ---------------------------------------------------------------------------

/// `appendStringInfoString` / `appendBinaryStringInfo` — append a byte slice.
fn sb_extend(buf: &mut PgVec<'_, u8>, src: &[u8]) -> PgResult<()> {
    let mcx = *buf.allocator();
    buf.try_reserve(src.len()).map_err(|_| mcx.oom(src.len()))?;
    buf.extend_from_slice(src);
    Ok(())
}

/// `appendStringInfoChar` — append one byte.
fn sb_push(buf: &mut PgVec<'_, u8>, c: u8) -> PgResult<()> {
    let mcx = *buf.allocator();
    buf.try_reserve(1).map_err(|_| mcx.oom(1))?;
    buf.push(c);
    Ok(())
}

/// `enlargeStringInfo(buf, n)` then expose the freshly-grown tail so a writer
/// (`hex_encode` / `pg_strftime`) can fill it in place; returns the start index
/// of the appended region (which the caller truncates back to the bytes used).
fn sb_grow(buf: &mut PgVec<'_, u8>, n: usize) -> PgResult<usize> {
    let mcx = *buf.allocator();
    let start = buf.len();
    buf.try_reserve(n).map_err(|_| mcx.oom(n))?;
    buf.resize(start + n, 0);
    Ok(start)
}

/// C: `InitializeBackupManifest(...)` (backup_manifest.c:56).
///
/// Initialize state so that we can construct a backup manifest.
///
/// NB: Although the checksum type for the data files is configurable, the
/// checksum for the manifest itself always uses SHA-256. See comments in
/// [`SendBackupManifest`].
pub fn InitializeBackupManifest<'mcx>(
    mcx: Mcx<'mcx>,
    manifest: &mut BackupManifestInfo<'mcx>,
    want_manifest: BackupManifestOption,
    manifest_checksum_type: pg_checksum_type,
) -> PgResult<()> {
    *manifest = BackupManifestInfo::zeroed();
    manifest.checksum_type = manifest_checksum_type;

    if want_manifest == MANIFEST_OPTION_NO {
        manifest.buffile = None;
    } else {
        manifest.buffile = Some(BufFileCreateTemp(mcx, false)?);
        manifest.manifest_ctx = cryptohash::pg_cryptohash_create::call(PG_SHA256);
        if cryptohash::pg_cryptohash_init::call(manifest.manifest_ctx) < 0 {
            return Err(elog_error(
                alloc_string::format!("failed to initialize checksum of backup manifest"),
                71,
                "InitializeBackupManifest",
            ));
        }
    }

    manifest.manifest_size = 0;
    manifest.force_encode = want_manifest == MANIFEST_OPTION_FORCE_ENCODE;
    manifest.first_file = true;
    manifest.still_checksumming = true;

    if want_manifest != MANIFEST_OPTION_NO {
        let system_identifier = backend_access_transam_xlog_seams::get_system_identifier::call();
        let s = alloc_string::format!(
            "{{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n\
             \"System-Identifier\": {system_identifier},\n\
             \"Files\": ["
        );
        AppendStringToManifest(manifest, s.as_bytes())?;
    }

    Ok(())
}

/// C: `FreeBackupManifest(backup_manifest_info *manifest)`
/// (backup_manifest.c:91).
pub fn FreeBackupManifest(manifest: &mut BackupManifestInfo) {
    if !manifest.manifest_ctx.is_null() {
        cryptohash::pg_cryptohash_free::call(manifest.manifest_ctx);
        manifest.manifest_ctx = core::ptr::null_mut();
    }
}

/// C: `AddFileToBackupManifest(...)` (backup_manifest.c:101).
///
/// Add an entry to the backup manifest for a file.
pub fn AddFileToBackupManifest<'mcx>(
    mcx: Mcx<'mcx>,
    manifest: &mut BackupManifestInfo,
    spcoid: Oid,
    pathname: &str,
    size: usize,
    mtime: pg_time_t,
    checksum_ctx: &mut PgChecksumContext,
) -> PgResult<()> {
    if !IsManifestEnabled(manifest) {
        return Ok(());
    }

    // If this file is part of a tablespace, the pathname passed to this
    // function will be relative to the tar file that contains it. We want the
    // pathname relative to the data directory (ignoring the intermediate
    // symlink traversal).
    let pathbuf;
    let pathname: &str = if OidIsValid(spcoid) {
        // snprintf(pathbuf, MAXPGPATH, "%s/%u/%s", PG_TBLSPC_DIR, spcoid,
        // pathname): the formatted path, truncated to fit MAXPGPATH bytes.
        let full = alloc_string::format!("{PG_TBLSPC_DIR}/{spcoid}/{pathname}");
        pathbuf = snprintf_truncate(&full, MAXPGPATH);
        &pathbuf
    } else {
        pathname
    };

    // Each file's entry needs to be separated from any entry that follows by a
    // comma, but there's no comma before the first one or after the last one.
    // To make that work, adding a file to the manifest starts by terminating
    // the most recently added line, with a comma if appropriate, but does not
    // terminate the line inserted for this file.
    let mut buf: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    if manifest.first_file {
        sb_push(&mut buf, b'\n')?;
        manifest.first_file = false;
    } else {
        sb_extend(&mut buf, b",\n")?;
    }

    // Write the relative pathname to this file out to the manifest. The
    // manifest is always stored in UTF-8, so we have to encode paths that are
    // not valid in that encoding.
    let path_bytes = pathname.as_bytes();
    let pathlen = path_bytes.len();
    // C: pg_verify_mbstr(PG_UTF8, pathname, pathlen, /*noError=*/true). PG_UTF8
    // is always a valid encoding, and with noError the function reduces to
    // "does the whole string verify as valid", i.e. the verified prefix length
    // equals the full length.
    if !manifest.force_encode
        && pg_encoding_verifymbstr(PG_UTF8, path_bytes) as usize == pathlen
    {
        sb_extend(&mut buf, b"{ \"Path\": ")?;
        escape_json_with_len(&mut buf, path_bytes)?;
        sb_extend(&mut buf, b", ")?;
    } else {
        sb_extend(&mut buf, b"{ \"Encoded-Path\": \"")?;
        // enlargeStringInfo(&buf, 2 * pathlen); buf.len += hex_encode(...).
        let start = sb_grow(&mut buf, 2 * pathlen)?;
        let written = hex_encode(path_bytes, &mut buf[start..]) as usize;
        buf.truncate(start + written);
        sb_extend(&mut buf, b"\", ")?;
    }

    // appendStringInfo(&buf, "\"Size\": %zu, ", size).
    sb_extend(&mut buf, alloc_string::format!("\"Size\": {size}, ").as_bytes())?;

    // Convert last modification time to a string and append it to the manifest.
    // Since it's not clear what time zone to use and since time zone definitions
    // can change, possibly causing confusion, use GMT always.
    //
    // enlargeStringInfo(&buf, 128); buf.len += pg_strftime(&buf.data[buf.len],
    // 128, "%Y-%m-%d %H:%M:%S %Z", pg_gmtime(&mtime)).
    sb_extend(&mut buf, b"\"Last-Modified\": \"")?;
    let tm = pg_gmtime(mtime).ok_or_else(|| {
        elog_error(
            alloc_string::format!("could not convert modification time to GMT"),
            174,
            "AddFileToBackupManifest",
        )
    })?;
    let start = sb_grow(&mut buf, 128)?;
    let fmt: &CStr = c"%Y-%m-%d %H:%M:%S %Z";
    let written = pg_strftime(&mut buf[start..start + 128], fmt, &tm).unwrap_or(0);
    buf.truncate(start + written);
    sb_push(&mut buf, b'"')?;

    // Add checksum information.
    if checksum_ctx.checksum_type() != CHECKSUM_TYPE_NONE {
        let mut checksumbuf = [0u8; PG_CHECKSUM_MAX_LENGTH];

        let checksumlen = pg_checksum_final(checksum_ctx, &mut checksumbuf).map_err(|_| {
            elog_error(
                alloc_string::format!("could not finalize checksum of file \"{pathname}\""),
                186,
                "AddFileToBackupManifest",
            )
        })?;

        sb_extend(
            &mut buf,
            alloc_string::format!(
                ", \"Checksum-Algorithm\": \"{}\", \"Checksum\": \"",
                pg_checksum_type_name(checksum_ctx.checksum_type())
            )
            .as_bytes(),
        )?;
        // enlargeStringInfo(&buf, 2 * checksumlen); buf.len += hex_encode(...).
        let start = sb_grow(&mut buf, 2 * checksumlen)?;
        let written = hex_encode(&checksumbuf[..checksumlen], &mut buf[start..]) as usize;
        buf.truncate(start + written);
        sb_push(&mut buf, b'"')?;
    }

    // Close out the object.
    sb_extend(&mut buf, b" }")?;

    // OK, add it to the manifest.
    AppendStringToManifest(manifest, &buf)?;

    // (C: pfree(buf.data) — Rust drops `buf` automatically.)
    Ok(())
}

/// C: `AddWALInfoToBackupManifest(...)` (backup_manifest.c:212).
///
/// Add information about the WAL that will need to be replayed when restoring
/// this backup to the manifest.
pub fn AddWALInfoToBackupManifest<'mcx>(
    mcx: Mcx<'mcx>,
    manifest: &mut BackupManifestInfo,
    startptr: XLogRecPtr,
    starttli: TimeLineID,
    mut endptr: XLogRecPtr,
    endtli: TimeLineID,
) -> PgResult<()> {
    let mut first_wal_range = true;
    let mut found_start_timeline = false;

    if !IsManifestEnabled(manifest) {
        return Ok(());
    }

    // Terminate the list of files.
    AppendStringToManifest(manifest, b"\n],\n")?;

    // Read the timeline history for the ending timeline. A base backup runs
    // during normal operation, where ArchiveRecoveryRequested is its default
    // `false` (cf. timeline.c init_seams), so we read the pg_wal-path history.
    let timelines = readTimeLineHistory(mcx, endtli, false)?;

    // Start a list of LSN ranges.
    AppendStringToManifest(manifest, b"\"WAL-Ranges\": [\n")?;

    for entry in &timelines {
        // We only care about timelines that were active during the backup.
        // Skip any that ended before the backup started. (Note that if
        // entry->end is InvalidXLogRecPtr, it means that the timeline has not
        // yet ended.)
        if !XLogRecPtrIsInvalid(entry.end) && entry.end < startptr {
            continue;
        }

        // Because the timeline history file lists newer timelines before older
        // ones, the first timeline we encounter that is new enough to matter
        // ought to match the ending timeline of the backup.
        if first_wal_range && endtli != entry.tli {
            return Err(ereport(ERROR)
                .errmsg(alloc_string::format!(
                    "expected end timeline {endtli} but found timeline {}",
                    entry.tli
                ))
                .into_error()
                .with_error_location(err_loc(254, "AddWALInfoToBackupManifest")));
        }

        // If this timeline entry matches with the timeline on which the backup
        // started, WAL needs to be checked from the start LSN of the backup. If
        // this entry refers to a newer timeline, WAL needs to be checked since
        // the beginning of this timeline, so use the LSN where the timeline
        // began.
        let tl_beginptr = if starttli == entry.tli {
            startptr
        } else {
            // If we reach a TLI that has no valid beginning LSN, there can't be
            // any more timelines in the history after this point, so we'd better
            // have arrived at the expected starting TLI. If not, something's
            // gone horribly wrong.
            if XLogRecPtrIsInvalid(entry.begin) {
                return Err(ereport(ERROR)
                    .errmsg(alloc_string::format!(
                        "expected start timeline {starttli} but found timeline {}",
                        entry.tli
                    ))
                    .into_error()
                    .with_error_location(err_loc(278, "AddWALInfoToBackupManifest")));
            }
            entry.begin
        };

        // AppendToManifest(manifest, "%s{ \"Timeline\": %u, \"Start-LSN\":
        // \"%X/%X\", \"End-LSN\": \"%X/%X\" }", ...).
        let s = alloc_string::format!(
            "{}{{ \"Timeline\": {}, \"Start-LSN\": \"{}\", \"End-LSN\": \"{}\" }}",
            if first_wal_range { "" } else { ",\n" },
            entry.tli,
            lsn_format(tl_beginptr),
            lsn_format(endptr),
        );
        AppendStringToManifest(manifest, s.as_bytes())?;

        if starttli == entry.tli {
            found_start_timeline = true;
            break;
        }

        endptr = entry.begin;
        first_wal_range = false;
    }

    // The last entry in the timeline history for the ending timeline should be
    // the ending timeline itself. Verify that this is what we observed.
    if !found_start_timeline {
        return Err(ereport(ERROR)
            .errmsg(alloc_string::format!(
                "start timeline {starttli} not found in history of timeline {endtli}"
            ))
            .into_error()
            .with_error_location(err_loc(305, "AddWALInfoToBackupManifest")));
    }

    // Terminate the list of WAL ranges.
    AppendStringToManifest(manifest, b"\n],\n")?;

    Ok(())
}

/// C: `SendBackupManifest(backup_manifest_info *manifest, bbsink *sink)`
/// (backup_manifest.c:316).
///
/// Finalize the backup manifest, and send it to the client. The repo [`Bbsink`]
/// API threads the shared [`BbsinkState`] explicitly through the `bbsink_*`
/// dispatch helpers, so this takes both `sink` and `state` where the C struct
/// carries the state via a back-pointer.
pub fn SendBackupManifest<'mcx>(
    manifest: &mut BackupManifestInfo,
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
) -> PgResult<()> {
    let mut checksumbuf = [0u8; PG_SHA256_DIGEST_LENGTH];
    let mut checksumstringbuf = [0u8; PG_SHA256_DIGEST_STRING_LENGTH];
    let mut manifest_bytes_done: usize = 0;

    if !IsManifestEnabled(manifest) {
        return Ok(());
    }

    // Append manifest checksum, so that the problems with the manifest itself
    // can be detected.
    //
    // We always use SHA-256 for this, regardless of what algorithm is chosen
    // for checksumming the files. If we ever want to make the checksum
    // algorithm used for the manifest file variable, the client will need a way
    // to figure out which algorithm to use as close to the beginning of the
    // manifest file as possible, to avoid having to read the whole thing twice.
    manifest.still_checksumming = false;
    if cryptohash::pg_cryptohash_final::call(
        manifest.manifest_ctx,
        checksumbuf.as_mut_ptr(),
        checksumbuf.len(),
    ) < 0
    {
        return Err(elog_error(
            alloc_string::format!("failed to finalize checksum of backup manifest"),
            340,
            "SendBackupManifest",
        ));
    }
    AppendStringToManifest(manifest, b"\"Manifest-Checksum\": \"")?;

    hex_encode(&checksumbuf, &mut checksumstringbuf);
    checksumstringbuf[PG_SHA256_DIGEST_STRING_LENGTH - 1] = b'\0';

    // C appends checksumstringbuf as a cstring: the trailing NUL is not part of
    // the appended text.
    let end = checksumstringbuf
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(checksumstringbuf.len());
    AppendStringToManifest(manifest, &checksumstringbuf[..end])?;
    AppendStringToManifest(manifest, b"\"}\n")?;

    // We've written all the data to the manifest file. Rewind the file so that
    // we can read it all back.
    let buffile = manifest
        .buffile
        .as_mut()
        .expect("manifest enabled => buffile set");
    if BufFileSeek(buffile, 0, 0, SEEK_SET)? != 0 {
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg("could not rewind temporary file")
            .into_error()
            .with_error_location(err_loc(355, "SendBackupManifest")));
    }

    // Send the backup manifest.
    bbsink_begin_manifest(sink, state)?;
    while manifest_bytes_done < manifest.manifest_size as usize {
        let bytes_to_read = core::cmp::min(
            sink.buffer_length(),
            manifest.manifest_size as usize - manifest_bytes_done,
        );
        let buffile = manifest
            .buffile
            .as_mut()
            .expect("manifest enabled => buffile set");
        BufFileReadExact(buffile, sink.buffer_slice_mut(bytes_to_read))?;
        bbsink_manifest_contents(sink, state, bytes_to_read)?;
        manifest_bytes_done += bytes_to_read;
    }
    bbsink_end_manifest(sink, state)?;

    // Release resources.
    let buffile = manifest
        .buffile
        .take()
        .expect("manifest enabled => buffile set");
    let mut buffile = buffile;
    BufFileClose(&mut buffile)?;

    Ok(())
}

/// C: `static void AppendStringToManifest(backup_manifest_info *manifest,
/// const char *s)` (backup_manifest.c:383).
///
/// Append a cstring to the manifest. In C `s` is a NUL-terminated `char *` and
/// `len = strlen(s)`; here it is the raw byte slice (no trailing NUL), matching
/// the byte stream that is checksummed and spooled.
fn AppendStringToManifest(manifest: &mut BackupManifestInfo, s: &[u8]) -> PgResult<()> {
    let len = s.len();

    if manifest.still_checksumming
        && cryptohash::pg_cryptohash_update::call(manifest.manifest_ctx, s.as_ptr(), len) < 0
    {
        return Err(elog_error(
            alloc_string::format!("failed to update checksum of backup manifest"),
            392,
            "AppendStringToManifest",
        ));
    }
    let buffile = manifest
        .buffile
        .as_mut()
        .expect("AppendStringToManifest: buffile set");
    BufFileWrite(buffile, s)?;
    manifest.manifest_size += len as u64;

    Ok(())
}

/// Emulate `snprintf(buf, size, "%s", full)`: copy at most `size - 1` bytes and
/// NUL-terminate (the C string is truncated to fit a `size`-byte buffer).
fn snprintf_truncate(full: &str, size: usize) -> alloc_string::String {
    if size == 0 {
        return alloc_string::String::new();
    }
    let max = size - 1;
    if full.len() <= max {
        return full.into();
    }
    // Truncate on a char boundary at or below `max` bytes, matching C's
    // byte-wise truncation while staying valid UTF-8.
    let mut end = max;
    while end > 0 && !full.is_char_boundary(end) {
        end -= 1;
    }
    full[..end].into()
}
