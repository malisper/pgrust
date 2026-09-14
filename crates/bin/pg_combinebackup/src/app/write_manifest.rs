//! C: src/bin/pg_combinebackup/write_manifest.c — write the combined
//! backup_manifest. Byte format pinned to C: version-2 header with
//! System-Identifier, per-file entries with Path/Encoded-Path, Size,
//! Last-Modified (strftime "%Y-%m-%d %H:%M:%S %Z" of gmtime, i.e. "... GMT"),
//! optional Checksum-Algorithm/Checksum, the WAL-Ranges list copied from the
//! final input manifest, and a SHA-256 Manifest-Checksum trailer covering
//! everything up to (not including) the checksum line.

use std::io::Write;
use std::path::{Path, PathBuf};

use manifest::{pg_checksum_type_name, PgChecksumContext, PgChecksumType};
use parse_manifest::ManifestWalRange;

use crate::app::flog::{errno_message, pg_fatal};

pub struct ManifestWriter {
    pathname: PathBuf,
    file: Option<std::fs::File>,
    buf: Vec<u8>,
    first_file: bool,
    still_checksumming: bool,
    manifest_ctx: PgChecksumContext,
}

/// C: create_manifest_writer.
pub fn create_manifest_writer(directory: &Path, system_identifier: u64) -> ManifestWriter {
    let mut mwriter = ManifestWriter {
        pathname: directory.join("backup_manifest"),
        file: None,
        buf: Vec::new(),
        first_file: true,
        still_checksumming: true,
        manifest_ctx: PgChecksumContext::init(PgChecksumType::Sha256),
    };
    mwriter.buf.extend_from_slice(
        format!(
            "{{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n\
             \"System-Identifier\": {system_identifier},\n\
             \"Files\": ["
        )
        .as_bytes(),
    );
    mwriter
}

impl ManifestWriter {
    /// C: add_file_to_manifest.
    pub fn add_file(
        &mut self,
        manifest_path: &[u8],
        size: u64,
        mtime: i64,
        checksum_type: PgChecksumType,
        checksum_payload: &[u8],
    ) {
        if self.first_file {
            self.buf.push(b'\n');
            self.first_file = false;
        } else {
            self.buf.extend_from_slice(b",\n");
        }

        match std::str::from_utf8(manifest_path) {
            Ok(path_str) => {
                self.buf.extend_from_slice(b"{ \"Path\": ");
                escape_json(&mut self.buf, path_str);
                self.buf.extend_from_slice(b", ");
            }
            Err(_) => {
                self.buf.extend_from_slice(b"{ \"Encoded-Path\": \"");
                hex_encode(manifest_path, &mut self.buf);
                self.buf.extend_from_slice(b"\", ");
            }
        }

        self.buf
            .extend_from_slice(format!("\"Size\": {size}, ").as_bytes());

        self.buf.extend_from_slice(b"\"Last-Modified\": \"");
        self.buf
            .extend_from_slice(format_gmtime(mtime).as_bytes());
        self.buf.push(b'"');

        if self.buf.len() > 128 * 1024 {
            self.flush();
        }

        if !checksum_payload.is_empty() {
            self.buf.extend_from_slice(
                format!(
                    ", \"Checksum-Algorithm\": \"{}\", \"Checksum\": \"",
                    pg_checksum_type_name(checksum_type)
                )
                .as_bytes(),
            );
            hex_encode(checksum_payload, &mut self.buf);
            self.buf.push(b'"');
        }

        self.buf.extend_from_slice(b" }");

        if self.buf.len() > 128 * 1024 {
            self.flush();
        }
    }

    /// C: finalize_manifest.
    pub fn finalize(&mut self, wal_ranges: &[ManifestWalRange]) {
        /* Terminate the list of files. */
        self.buf.extend_from_slice(b"\n],\n");

        /* Start a list of LSN ranges. */
        self.buf.extend_from_slice(b"\"WAL-Ranges\": [\n");

        for (i, wal_range) in wal_ranges.iter().enumerate() {
            self.buf.extend_from_slice(
                format!(
                    "{}{{ \"Timeline\": {}, \"Start-LSN\": \"{:X}/{:X}\", \"End-LSN\": \"{:X}/{:X}\" }}",
                    if i == 0 { "" } else { ",\n" },
                    wal_range.tli,
                    (wal_range.start_lsn >> 32) as u32,
                    wal_range.start_lsn as u32,
                    (wal_range.end_lsn >> 32) as u32,
                    wal_range.end_lsn as u32,
                )
                .as_bytes(),
            );
        }

        /* Terminate the list of WAL ranges. */
        self.buf.extend_from_slice(b"\n],\n");

        /* Flush accumulated data and update checksum calculation. */
        self.flush();

        /* Checksum only includes data up to this point. */
        self.still_checksumming = false;

        /* Compute and insert manifest checksum. */
        self.buf.extend_from_slice(b"\"Manifest-Checksum\": \"");
        let mut checksumbuf = [0u8; manifest::PG_CHECKSUM_MAX_LENGTH];
        let len = self.manifest_ctx.finalize(&mut checksumbuf);
        debug_assert_eq!(len, 32); /* PG_SHA256_DIGEST_LENGTH */
        let mut hexbuf = Vec::with_capacity(2 * len);
        hex_encode(&checksumbuf[..len], &mut hexbuf);
        self.buf.extend_from_slice(&hexbuf);
        self.buf.extend_from_slice(b"\"}\n");

        /* Flush the last manifest checksum itself. */
        self.flush();

        /* Close the file. */
        let file = self.file.take().expect("manifest file open after flush");
        if let Err(e) = crate::app::copy_file::close_file(file) {
            pg_fatal!(
                "could not close file \"{}\": {}",
                self.pathname.display(),
                errno_message(&e)
            );
        }
    }

    /// C: flush_manifest.
    fn flush(&mut self) {
        if self.file.is_none() {
            match crate::app::copy_file::open_excl_create(&self.pathname, false) {
                Ok(f) => self.file = Some(f),
                Err(e) => pg_fatal!(
                    "could not open file \"{}\": {}",
                    self.pathname.display(),
                    errno_message(&e)
                ),
            }
        }
        if !self.buf.is_empty() {
            if let Err(e) = self.file.as_mut().unwrap().write_all(&self.buf) {
                pg_fatal!(
                    "could not write file \"{}\": {}",
                    self.pathname.display(),
                    errno_message(&e)
                );
            }
            if self.still_checksumming {
                self.manifest_ctx.update(&self.buf);
            }
            self.buf.clear();
        }
    }
}

/// C: escape_json (write_manifest.c's private copy).
pub fn escape_json(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'"');
    for &b in s.as_bytes() {
        match b {
            0x08 => buf.extend_from_slice(b"\\b"),
            0x0c => buf.extend_from_slice(b"\\f"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b if b < b' ' => buf.extend_from_slice(format!("\\u{:04x}", b).as_bytes()),
            b => buf.push(b),
        }
    }
    buf.push(b'"');
}

/// C: hex_encode (lowercase).
pub fn hex_encode(src: &[u8], dst: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in src {
        dst.push(HEX[(b >> 4) as usize]);
        dst.push(HEX[(b & 0xf) as usize]);
    }
}

/// st_mtime (whole seconds) from file metadata.
pub fn mtime_of(md: &std::fs::Metadata) -> i64 {
    use std::os::unix::fs::MetadataExt;
    md.mtime()
}

/// C: strftime(..., "%Y-%m-%d %H:%M:%S %Z", gmtime(&mtime)) — %Z of gmtime is
/// "GMT" on the platforms we run on.
pub fn format_gmtime(t: i64) -> String {
    let days = t.div_euclid(86400);
    let secs = t.rem_euclid(86400);
    // Civil-from-days (Howard Hinnant's algorithm).
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02} GMT",
        y,
        m,
        d,
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    )
}
