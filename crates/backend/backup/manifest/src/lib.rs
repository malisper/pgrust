
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod checksum;

#[cfg(test)]
mod tests;

use mcx::{Mcx, PgVec};
use types_core::{pg_time_t, Oid, TimeLineID, XLogRecPtr, MAXPGPATH};
use types_error::{PgError, PgResult};

use localtime::pg_gmtime;
use pg_sha2::{PgSha256Ctx, PG_SHA256_DIGEST_LENGTH};
use strftime::pg_strftime;
use timeline::readTimeLineHistory;
use varlena::bytea::hex_encode_into;

pub use checksum::{
    pg_checksum_type_name, PgChecksumContext, PgChecksumType, PG_CHECKSUM_MAX_LENGTH,
};

pub mod seams {
    seam_core::seam!(
        pub fn get_system_identifier() -> u64
    );
}

const PG_TBLSPC_DIR: &str = "pg_tblspc";

const PG_SHA256_DIGEST_STRING_LENGTH: usize = 2 * PG_SHA256_DIGEST_LENGTH + 1;

#[inline]
fn OidIsValid(object_id: Oid) -> bool {
    object_id != 0
}

#[inline]
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == 0
}

fn lsn_format(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

fn err(msg: impl Into<String>) -> Box<PgError> {
    PgError::error(msg).into()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackupManifestOption {
    Yes,
    No,
    ForceEncode,
}

pub use BackupManifestOption::{
    ForceEncode as MANIFEST_OPTION_FORCE_ENCODE, No as MANIFEST_OPTION_NO, Yes as MANIFEST_OPTION_YES,
};

pub struct BackupManifestInfo<'mcx> {
    enabled: bool,
    checksum_type: PgChecksumType,
    manifest_ctx: Option<PgSha256Ctx>,
    buf: Option<PgVec<'mcx, u8>>,
    manifest_size: u64,
    force_encode: bool,
    first_file: bool,
    still_checksumming: bool,
}

impl<'mcx> BackupManifestInfo<'mcx> {
    pub fn zeroed() -> Self {
        Self {
            enabled: false,
            checksum_type: PgChecksumType::None,
            manifest_ctx: None,
            buf: None,
            manifest_size: 0,
            force_encode: false,
            first_file: false,
            still_checksumming: false,
        }
    }

    pub fn checksum_type(&self) -> PgChecksumType {
        self.checksum_type
    }

    pub fn manifest_size(&self) -> u64 {
        self.manifest_size
    }

    pub fn bytes(&self) -> &[u8] {
        self.buf.as_deref().unwrap_or(&[])
    }
}

impl Default for BackupManifestInfo<'_> {
    fn default() -> Self {
        Self::zeroed()
    }
}

#[inline]
fn IsManifestEnabled(manifest: &BackupManifestInfo) -> bool {
    manifest.enabled
}

fn sb_extend(buf: &mut PgVec<'_, u8>, src: &[u8]) -> PgResult<()> {
    let mcx = *buf.allocator();
    buf.try_reserve(src.len()).map_err(|_| mcx.oom(src.len()))?;
    buf.extend_from_slice(src);
    Ok(())
}

fn sb_push(buf: &mut PgVec<'_, u8>, c: u8) -> PgResult<()> {
    let mcx = *buf.allocator();
    buf.try_reserve(1).map_err(|_| mcx.oom(1))?;
    buf.push(c);
    Ok(())
}

fn sb_grow(buf: &mut PgVec<'_, u8>, n: usize) -> PgResult<usize> {
    let mcx = *buf.allocator();
    let start = buf.len();
    buf.try_reserve(n).map_err(|_| mcx.oom(n))?;
    buf.resize(start + n, 0);
    Ok(start)
}

// C's SIMD scan is a search optimization; these per-byte escapes are the exact bytes.
fn escape_json(buf: &mut PgVec<'_, u8>, s: &[u8]) -> PgResult<()> {
    sb_push(buf, b'"')?;
    for &c in s {
        match c {
            0x08 => sb_extend(buf, b"\\b")?,
            0x0c => sb_extend(buf, b"\\f")?,
            b'\n' => sb_extend(buf, b"\\n")?,
            b'\r' => sb_extend(buf, b"\\r")?,
            b'\t' => sb_extend(buf, b"\\t")?,
            b'"' => sb_extend(buf, b"\\\"")?,
            b'\\' => sb_extend(buf, b"\\\\")?,
            _ if c < b' ' => sb_extend(buf, format!("\\u{:04x}", c as i32).as_bytes())?,
            _ => sb_push(buf, c)?,
        }
    }
    sb_push(buf, b'"')
}

fn hex_append(buf: &mut PgVec<'_, u8>, src: &[u8]) -> PgResult<()> {
    let mcx = *buf.allocator();
    buf.try_reserve(2 * src.len()).map_err(|_| mcx.oom(2 * src.len()))?;
    hex_encode_into(src, buf);
    Ok(())
}

pub fn InitializeBackupManifest<'mcx>(
    mcx: Mcx<'mcx>,
    manifest: &mut BackupManifestInfo<'mcx>,
    want_manifest: BackupManifestOption,
    manifest_checksum_type: PgChecksumType,
) -> PgResult<()> {
    *manifest = BackupManifestInfo::zeroed();
    manifest.checksum_type = manifest_checksum_type;

    if want_manifest != MANIFEST_OPTION_NO {
        manifest.enabled = true;
        manifest.buf = Some(PgVec::new_in(mcx));
        manifest.manifest_ctx = Some(PgSha256Ctx::init_sha256());
    }

    manifest.manifest_size = 0;
    manifest.force_encode = want_manifest == MANIFEST_OPTION_FORCE_ENCODE;
    manifest.first_file = true;
    manifest.still_checksumming = true;

    if want_manifest != MANIFEST_OPTION_NO {
        let system_identifier = seams::get_system_identifier::call();
        let s = format!(
            "{{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n\
             \"System-Identifier\": {system_identifier},\n\
             \"Files\": ["
        );
        AppendStringToManifest(manifest, s.as_bytes())?;
    }

    Ok(())
}

pub fn FreeBackupManifest(manifest: &mut BackupManifestInfo) {
    manifest.manifest_ctx = None;
}

pub fn AddFileToBackupManifest(
    manifest: &mut BackupManifestInfo,
    spcoid: Oid,
    pathname: &[u8],
    size: i64,
    mtime: pg_time_t,
    checksum_ctx: &mut PgChecksumContext,
) -> PgResult<()> {
    if !IsManifestEnabled(manifest) {
        return Ok(());
    }

    let pathbuf;
    let pathname: &[u8] = if OidIsValid(spcoid) {
        let mut full = format!("{PG_TBLSPC_DIR}/{spcoid}/").into_bytes();
        full.extend_from_slice(pathname);
        pathbuf = snprintf_truncate(&full, MAXPGPATH);
        &pathbuf
    } else {
        pathname
    };

    let mcx = *manifest.buf.as_ref().expect("manifest enabled").allocator();
    let mut buf: PgVec<'_, u8> = PgVec::new_in(mcx);
    if manifest.first_file {
        sb_push(&mut buf, b'\n')?;
        manifest.first_file = false;
    } else {
        sb_extend(&mut buf, b",\n")?;
    }

    // from_utf8 is RFC-3629-exact, matching C's pg_verify_mbstr(PG_UTF8) acceptance.
    let valid_utf8 = core::str::from_utf8(pathname).is_ok();
    if !manifest.force_encode && valid_utf8 {
        sb_extend(&mut buf, b"{ \"Path\": ")?;
        escape_json(&mut buf, pathname)?;
        sb_extend(&mut buf, b", ")?;
    } else {
        sb_extend(&mut buf, b"{ \"Encoded-Path\": \"")?;
        hex_append(&mut buf, pathname)?;
        sb_extend(&mut buf, b"\", ")?;
    }

    sb_extend(&mut buf, format!("\"Size\": {size}, ").as_bytes())?;

    // GMT always, regardless of session TZ (matches C).
    sb_extend(&mut buf, b"\"Last-Modified\": \"")?;
    let tm = pg_gmtime(mtime).ok_or_else(|| err("could not convert modification time to GMT"))?;
    let start = sb_grow(&mut buf, 128)?;
    let written =
        pg_strftime(&mut buf[start..start + 128], b"%Y-%m-%d %H:%M:%S %Z", &tm).unwrap_or(0);
    buf.truncate(start + written);
    sb_push(&mut buf, b'"')?;

    if checksum_ctx.checksum_type() != PgChecksumType::None {
        let mut checksumbuf = [0u8; PG_CHECKSUM_MAX_LENGTH];
        let checksumlen = checksum_ctx.finalize(&mut checksumbuf);
        sb_extend(
            &mut buf,
            format!(
                ", \"Checksum-Algorithm\": \"{}\", \"Checksum\": \"",
                pg_checksum_type_name(checksum_ctx.checksum_type())
            )
            .as_bytes(),
        )?;
        hex_append(&mut buf, &checksumbuf[..checksumlen])?;
        sb_push(&mut buf, b'"')?;
    }

    sb_extend(&mut buf, b" }")?;

    AppendStringToManifest(manifest, &buf)?;
    Ok(())
}

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

    AppendStringToManifest(manifest, b"\n],\n")?;

    let timelines = readTimeLineHistory(mcx, endtli, false)?;

    AppendStringToManifest(manifest, b"\"WAL-Ranges\": [\n")?;

    for entry in &timelines {
        if !XLogRecPtrIsInvalid(entry.end) && entry.end < startptr {
            continue;
        }

        if first_wal_range && endtli != entry.tli {
            return Err(err(format!(
                "expected end timeline {endtli} but found timeline {}",
                entry.tli
            )));
        }

        let tl_beginptr = if starttli == entry.tli {
            startptr
        } else {
            if XLogRecPtrIsInvalid(entry.begin) {
                return Err(err(format!(
                    "expected start timeline {starttli} but found timeline {}",
                    entry.tli
                )));
            }
            entry.begin
        };

        let s = format!(
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

    if !found_start_timeline {
        return Err(err(format!(
            "start timeline {starttli} not found in history of timeline {endtli}"
        )));
    }

    AppendStringToManifest(manifest, b"\n],\n")?;

    Ok(())
}

pub fn SendBackupManifest<'a>(manifest: &'a mut BackupManifestInfo<'_>) -> PgResult<&'a [u8]> {
    if !IsManifestEnabled(manifest) {
        return Ok(manifest.bytes());
    }

    manifest.still_checksumming = false;
    let digest = manifest
        .manifest_ctx
        .take()
        .ok_or_else(|| err("failed to finalize checksum of backup manifest"))?
        .final_sha256();

    AppendStringToManifest(manifest, b"\"Manifest-Checksum\": \"")?;

    let mut checksumstringbuf = [0u8; PG_SHA256_DIGEST_STRING_LENGTH];
    {
        let mcx = *manifest.buf.as_ref().expect("manifest enabled").allocator();
        let mut tmp: PgVec<'_, u8> = PgVec::new_in(mcx);
        hex_append(&mut tmp, &digest)?;
        checksumstringbuf[..tmp.len()].copy_from_slice(&tmp);
    }
    checksumstringbuf[PG_SHA256_DIGEST_STRING_LENGTH - 1] = b'\0';

    let end = checksumstringbuf
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(checksumstringbuf.len());
    AppendStringToManifest(manifest, &checksumstringbuf[..end])?;
    AppendStringToManifest(manifest, b"\"}\n")?;

    Ok(manifest.bytes())
}

fn AppendStringToManifest(manifest: &mut BackupManifestInfo, s: &[u8]) -> PgResult<()> {
    let len = s.len();
    if manifest.still_checksumming {
        if let Some(ctx) = manifest.manifest_ctx.as_mut() {
            ctx.update(s);
        }
    }
    let buf = manifest.buf.as_mut().expect("manifest enabled");
    sb_extend(buf, s)?;
    manifest.manifest_size += len as u64;
    Ok(())
}

fn snprintf_truncate(full: &[u8], size: usize) -> Vec<u8> {
    if size == 0 {
        return Vec::new();
    }
    let max = size - 1;
    full[..full.len().min(max)].to_vec()
}
