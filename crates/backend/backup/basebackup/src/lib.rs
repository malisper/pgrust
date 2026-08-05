// basebackup.c (PG 18.3) — the base-backup driver. Assembles the bbsink chain
// (client copy sink + optional throttle), drives do_pg_backup_start/stop, walks
// the data directory streaming each file as a tar archive, injects backup_label
// / tablespace_map / pg_control, and emits the backup manifest.
//
// Scope (increment 5, default pg_basebackup -Xstream oracle). Loud contained
// refusals, tagged increment 5, for surface a default backup never engages:
// server-side compression, incremental backups, non-client targets, inline WAL
// inclusion (WAL=true; default pg_basebackup streams WAL on a separate
// connection). Backup-time page-checksum verification is deferred (it only
// counts corruption warnings; it does not alter the streamed bytes).
#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use std::cell::Cell;

use elog::ereport;
use mcx::Mcx;
use repl_gram::{BaseBackupCmd, ReplOption, ReplOptionArg};
use types_core::{Oid, TimeLineID, XLogRecPtr};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERROR, WARNING,
};

use manifest::checksum::{
    PgChecksumContext, PgChecksumType, CHECKSUM_TYPE_CRC32C, CHECKSUM_TYPE_NONE,
};
use manifest::{
    AddFileToBackupManifest, AddWALInfoToBackupManifest, BackupManifestInfo, BackupManifestOption,
    FreeBackupManifest, InitializeBackupManifest, SendBackupManifest,
};
use sink::{
    bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup, bbsink_cleanup,
    bbsink_end_archive, bbsink_end_backup, Bbsink, BbsinkState,
};
// TablespaceInfo is homed in xlogbackup (shared by do_pg_backup_start + the
// sink chain) to avoid a transam_xlog -> sink layering inversion.
use xlogbackup::TablespaceInfo;
use walsender::WalSndState;

const SRCFILE: &str = "src/backend/backup/basebackup.c";

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report OUR source site (call site via track_caller).
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

// SINK_BUFFER_LENGTH = Max(32768, BLCKSZ).
const SINK_BUFFER_LENGTH: usize = if 32768 > types_core::BLCKSZ { 32768 } else { types_core::BLCKSZ };
const TAR_BLOCK_SIZE: usize = 512;

const INVALID_OID: Oid = types_core::InvalidOid;

const BACKUP_LABEL_FILE: &str = "backup_label";
const TABLESPACE_MAP: &str = "tablespace_map";
const XLOG_CONTROL_FILE: &str = "global/pg_control";
const TABLESPACE_VERSION_DIRECTORY: &str = types_storage::file::TABLESPACE_VERSION_DIRECTORY;
const PG_TEMP_FILE_PREFIX: &str = "pgsql_tmp";

const MAX_RATE_LOWER: i64 = 32;
const MAX_RATE_UPPER: i64 = 1_048_576;

const S_IFMT: u32 = 0o170000;
const S_IFDIR: u32 = 0o040000;
const S_IFREG: u32 = 0o100000;
const S_IFLNK: u32 = 0o120000;
fn S_ISDIR(m: u32) -> bool { m & S_IFMT == S_IFDIR }
fn S_ISREG(m: u32) -> bool { m & S_IFMT == S_IFREG }
fn S_ISLNK(m: u32) -> bool { m & S_IFMT == S_IFLNK }

const O_RDONLY: i32 = 0;

// basebackup.c file-statics: backup_started_in_recovery, noverify_checksums,
// total_checksum_failures.
thread_local! {
    static BACKUP_STARTED_IN_RECOVERY: Cell<bool> = const { Cell::new(false) };
    static NOVERIFY_CHECKSUMS: Cell<bool> = const { Cell::new(false) };
    static TOTAL_CHECKSUM_FAILURES: Cell<i64> = const { Cell::new(0) };
}

// excludeDirContents[] — contents excluded, empty dir kept.
const EXCLUDE_DIR_CONTENTS: &[&str] =
    &["pg_stat_tmp", "pg_replslot", "pg_dynshmem", "pg_notify", "pg_serial", "pg_snapshots", "pg_subtrans"];

struct ExcludeListItem {
    name: &'static str,
    match_prefix: bool,
}

const EXCLUDE_FILES: &[ExcludeListItem] = &[
    ExcludeListItem { name: "postgresql.auto.conf.tmp", match_prefix: false },
    ExcludeListItem { name: "current_logfiles.tmp", match_prefix: false },
    ExcludeListItem { name: "pg_internal.init", match_prefix: true },
    ExcludeListItem { name: BACKUP_LABEL_FILE, match_prefix: false },
    ExcludeListItem { name: TABLESPACE_MAP, match_prefix: false },
    ExcludeListItem { name: "backup_manifest", match_prefix: false },
    ExcludeListItem { name: "postmaster.pid", match_prefix: false },
    ExcludeListItem { name: "postmaster.opts", match_prefix: false },
];

// ---------------------------------------------------------------------------
// lstat / readlink / directory listing (C's file primitives, inlined).
// ---------------------------------------------------------------------------

struct LstatInfo {
    size: i64,
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: i64,
}

fn lstat_file(path: &str) -> PgResult<Option<LstatInfo>> {
    let mut st = fd::FileInfo::zeroed();
    if fd::pg_lstat(path, &mut st) != 0 {
        if fd::get_errno() == libc::ENOENT {
            return Ok(None);
        }
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not stat file \"{path}\""))
            .finish(loc("lstat_file"))
            .map(|()| None);
    }
    Ok(Some(LstatInfo {
        size: st.size,
        mode: st.mode,
        uid: st.uid,
        gid: st.gid,
        mtime: st.mtime_sec,
    }))
}

fn read_link(path: &str) -> PgResult<String> {
    // readlink(2) via the fd-crate front; MAXPGPATH-class buffer.
    let mut buf = [0u8; 1024];
    let n = fd::pg_readlink(path, &mut buf);
    if n < 0 {
        ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not read symbolic link \"{path}\""))
            .finish(loc("read_link"))?;
        unreachable!()
    }
    // As in C's sendDir: a target that fills the whole buffer may have been
    // truncated -- error out rather than emit a truncated link target.
    if n as usize >= buf.len() {
        ereport(ERROR)
            .errmsg(format!("symbolic link \"{path}\" target is too long"))
            .finish(loc("read_link"))?;
        unreachable!()
    }
    Ok(String::from_utf8_lossy(&buf[..n as usize]).into_owned())
}

fn read_dir_names(path: &str) -> PgResult<Vec<String>> {
    let mut names = Vec::new();
    fd::with_allocated_dir(path, &mut |name| {
        names.push(name.to_owned());
        Ok(false)
    })?;
    Ok(names)
}

// ---------------------------------------------------------------------------
// tar header (port/tar.c tarCreateHeader), inlined.
// ---------------------------------------------------------------------------

fn print_tar_number(s: &mut [u8], mut val: u64) {
    let len = s.len();
    if val < 1u64 << ((len - 1) * 3) {
        // octal with trailing space
        s[len - 1] = b' ';
        let mut i = len - 1;
        while i > 0 {
            i -= 1;
            s[i] = (val & 7) as u8 + b'0';
            val >>= 3;
        }
    } else {
        // base-256 with leading \200
        s[0] = 0o200;
        let mut i = len;
        while i > 1 {
            i -= 1;
            s[i] = (val & 255) as u8;
            val >>= 8;
        }
    }
}

fn tar_checksum(header: &[u8; TAR_BLOCK_SIZE]) -> u64 {
    // Sum all bytes, treating the checksum field [148,156) as 8 spaces.
    let mut sum: u64 = 8 * b' ' as u64;
    for (i, &b) in header.iter().enumerate() {
        if i < 148 || i >= 156 {
            sum += b as u64;
        }
    }
    sum
}

fn strlcpy(dst: &mut [u8], src: &str) {
    let s = src.as_bytes();
    let n = s.len().min(dst.len().saturating_sub(1));
    dst[..n].copy_from_slice(&s[..n]);
}

enum TarError {
    Ok,
    NameTooLong,
    SymlinkTooLong,
}

// tarCreateHeader (port/tar.c). Returns the 512-byte header + status.
fn tar_create_header(
    filename: &str,
    linktarget: Option<&str>,
    size: i64,
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: i64,
) -> (TarError, [u8; TAR_BLOCK_SIZE]) {
    let mut h = [0u8; TAR_BLOCK_SIZE];
    if filename.len() > 99 {
        return (TarError::NameTooLong, h);
    }
    if let Some(lt) = linktarget {
        if lt.len() > 99 {
            return (TarError::SymlinkTooLong, h);
        }
    }

    strlcpy(&mut h[0..100], filename); // name
    if linktarget.is_some() || S_ISDIR(mode) {
        // directory / symlink-to-directory: trailing slash
        let flen = filename.len().min(99);
        h[flen] = b'/';
    }

    print_tar_number(&mut h[100..108], (mode & 0o7777) as u64);
    print_tar_number(&mut h[108..116], uid as u64);
    print_tar_number(&mut h[116..124], gid as u64);
    let sz = if linktarget.is_some() || S_ISDIR(mode) { 0 } else { size as u64 };
    print_tar_number(&mut h[124..136], sz);
    print_tar_number(&mut h[136..148], mtime as u64);
    // checksum [148,156) computed last

    if let Some(lt) = linktarget {
        h[156] = b'2'; // symlink
        strlcpy(&mut h[157..257], lt);
    } else if S_ISDIR(mode) {
        h[156] = b'5';
    } else {
        h[156] = b'0';
    }

    h[257..262].copy_from_slice(b"ustar");
    h[263..265].copy_from_slice(b"00");
    strlcpy(&mut h[265..297], "postgres");
    strlcpy(&mut h[297..329], "postgres");
    print_tar_number(&mut h[329..337], 0);
    print_tar_number(&mut h[337..345], 0);

    let cksum = tar_checksum(&h);
    print_tar_number(&mut h[148..156], cksum);
    (TarError::Ok, h)
}

// ---------------------------------------------------------------------------
// basebackup_options.
// ---------------------------------------------------------------------------

struct BasebackupOptions {
    label: String,
    progress: bool,
    fastcheckpoint: bool,
    nowait: bool,
    includewal: bool,
    incremental: bool,
    maxrate: u32,
    sendtblspcmapfile: bool,
    send_to_client: bool,
    target_handle: Option<basebackup_target::BaseBackupTargetHandle>,
    manifest: BackupManifestOption,
    manifest_checksum_type: PgChecksumType,
}

impl Default for BasebackupOptions {
    fn default() -> Self {
        Self {
            label: String::new(),
            progress: false,
            fastcheckpoint: false,
            nowait: false,
            includewal: false,
            incremental: false,
            maxrate: 0,
            sendtblspcmapfile: false,
            send_to_client: false,
            target_handle: None,
            manifest: BackupManifestOption::No,
            manifest_checksum_type: CHECKSUM_TYPE_CRC32C,
        }
    }
}

fn opt_string<'a>(o: &'a ReplOption) -> PgResult<&'a str> {
    match &o.arg {
        Some(ReplOptionArg::Str(s)) => Ok(s.as_str()),
        _ => {
            ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("parameter \"{}\" requires a string value", o.name))
                .finish(loc("parse_basebackup_options"))?;
            unreachable!()
        }
    }
}

fn parse_bool_str(s: &str) -> Option<bool> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "yes" | "on" | "1" | "t" | "y" => Some(true),
        "false" | "no" | "off" | "0" | "f" | "n" => Some(false),
        _ => None,
    }
}

fn opt_bool(o: &ReplOption) -> PgResult<bool> {
    let v = match &o.arg {
        None => Some(true),
        Some(ReplOptionArg::Bool(b)) => Some(*b),
        Some(ReplOptionArg::Int(0)) => Some(false),
        Some(ReplOptionArg::Int(1)) => Some(true),
        Some(ReplOptionArg::Int(_)) => None,
        Some(ReplOptionArg::Str(s)) => parse_bool_str(s),
    };
    match v {
        Some(b) => Ok(b),
        None => {
            ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("parameter \"{}\" requires a Boolean value", o.name))
                .finish(loc("parse_basebackup_options"))?;
            unreachable!()
        }
    }
}

fn opt_int(o: &ReplOption) -> PgResult<i64> {
    let v = match &o.arg {
        Some(ReplOptionArg::Int(i)) => Some(i64::from(*i)),
        Some(ReplOptionArg::Str(s)) => s.parse::<i64>().ok(),
        _ => None,
    };
    match v {
        Some(n) => Ok(n),
        None => {
            ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("parameter \"{}\" requires an integer value", o.name))
                .finish(loc("parse_basebackup_options"))?;
            unreachable!()
        }
    }
}

fn strcasecmp(a: &str, b: &str) -> bool {
    pgstrcasecmp::pg_strcasecmp(a.as_bytes(), b.as_bytes()) == 0
}

fn dup_err(name: &str) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!("duplicate option \"{name}\""))
        .finish(loc("parse_basebackup_options"))
}

fn refuse(feature: &str) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(format!("{feature} unported (replication-p1 increment 5)"))
        .finish(loc("parse_basebackup_options"))
}

fn parse_basebackup_options(options: &[ReplOption]) -> PgResult<BasebackupOptions> {
    let mut opt = BasebackupOptions::default();
    NOVERIFY_CHECKSUMS.with(|c| c.set(false));

    let (mut o_label, mut o_progress, mut o_checkpoint, mut o_nowait) = (false, false, false, false);
    let (mut o_wal, mut o_incremental, mut o_maxrate, mut o_tsmap) = (false, false, false, false);
    let (mut o_noverify, mut o_manifest, mut o_manifest_cksums) = (false, false, false);
    let mut o_target = false;
    let mut o_target_detail = false;
    let mut o_compression = false;
    let mut o_compression_detail = false;
    let mut target_str: Option<String> = None;
    let mut target_detail_str: Option<String> = None;
    let mut compression_detail_str: Option<String> = None;

    for o in options {
        let name = o.name.as_str();
        match name {
            "label" => {
                if o_label { dup_err(name)?; }
                opt.label = opt_string(o)?.to_string();
                o_label = true;
            }
            "progress" => {
                if o_progress { dup_err(name)?; }
                opt.progress = opt_bool(o)?;
                o_progress = true;
            }
            "checkpoint" => {
                if o_checkpoint { dup_err(name)?; }
                let v = opt_string(o)?;
                if strcasecmp(v, "fast") {
                    opt.fastcheckpoint = true;
                } else if strcasecmp(v, "spread") {
                    opt.fastcheckpoint = false;
                } else {
                    ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!("unrecognized checkpoint type: \"{v}\""))
                        .finish(loc("parse_basebackup_options"))?;
                }
                o_checkpoint = true;
            }
            "wait" => {
                if o_nowait { dup_err(name)?; }
                opt.nowait = !opt_bool(o)?;
                o_nowait = true;
            }
            "wal" => {
                if o_wal { dup_err(name)?; }
                opt.includewal = opt_bool(o)?;
                o_wal = true;
            }
            "incremental" => {
                if o_incremental { dup_err(name)?; }
                opt.incremental = opt_bool(o)?;
                o_incremental = true;
            }
            "max_rate" => {
                if o_maxrate { dup_err(name)?; }
                let mr = opt_int(o)?;
                if mr < MAX_RATE_LOWER || mr > MAX_RATE_UPPER {
                    ereport(ERROR).errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                        .errmsg(format!(
                            "{} is outside the valid range for parameter \"MAX_RATE\" ({MAX_RATE_LOWER} .. {MAX_RATE_UPPER})",
                            mr as i32
                        ))
                        .finish(loc("parse_basebackup_options"))?;
                }
                opt.maxrate = mr as u32;
                o_maxrate = true;
            }
            "tablespace_map" => {
                if o_tsmap { dup_err(name)?; }
                opt.sendtblspcmapfile = opt_bool(o)?;
                o_tsmap = true;
            }
            "verify_checksums" => {
                if o_noverify { dup_err(name)?; }
                let verify = opt_bool(o)?;
                NOVERIFY_CHECKSUMS.with(|c| c.set(!verify));
                o_noverify = true;
            }
            "manifest" => {
                if o_manifest { dup_err(name)?; }
                let v = opt_string(o)?.to_string();
                opt.manifest = if let Some(b) = parse_bool_str(&v) {
                    if b { BackupManifestOption::Yes } else { BackupManifestOption::No }
                } else if strcasecmp(&v, "force-encode") {
                    BackupManifestOption::ForceEncode
                } else {
                    ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!("unrecognized manifest option: \"{v}\""))
                        .finish(loc("parse_basebackup_options"))?;
                    unreachable!()
                };
                o_manifest = true;
            }
            "manifest_checksums" => {
                if o_manifest_cksums { dup_err(name)?; }
                let v = opt_string(o)?.to_string();
                match parse_checksum_type(v.as_bytes()) {
                    Some(t) => opt.manifest_checksum_type = t,
                    None => {
                        ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR)
                            .errmsg(format!("unrecognized checksum algorithm: \"{v}\""))
                            .finish(loc("parse_basebackup_options"))?;
                    }
                }
                o_manifest_cksums = true;
            }
            "target" => {
                if o_target { dup_err(name)?; }
                target_str = Some(opt_string(o)?.to_string());
                o_target = true;
            }
            "target_detail" => {
                if o_target_detail { dup_err(name)?; }
                target_detail_str = Some(opt_string(o)?.to_string());
                o_target_detail = true;
            }
            "compression" => {
                if o_compression { dup_err(name)?; }
                let v = opt_string(o)?.to_string();
                // parse_compress_algorithm subset: only "none" runs without a
                // compression sink; real algorithms stay a loud refusal.
                if strcasecmp(&v, "none") {
                    // PG_COMPRESSION_NONE: no compression sink layer.
                } else if strcasecmp(&v, "gzip") || strcasecmp(&v, "lz4") || strcasecmp(&v, "zstd")
                {
                    refuse("server-side compression")?;
                } else {
                    ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!("unrecognized compression algorithm: \"{v}\""))
                        .finish(loc("parse_basebackup_options"))?;
                }
                o_compression = true;
            }
            "compression_detail" => {
                if o_compression_detail { dup_err(name)?; }
                compression_detail_str = Some(opt_string(o)?.to_string());
                o_compression_detail = true;
            }
            _ => {
                ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("unrecognized base backup option: \"{name}\""))
                    .finish(loc("parse_basebackup_options"))?;
            }
        }
    }

    if !o_label {
        opt.label = "base backup".to_string();
    }
    if matches!(opt.manifest, BackupManifestOption::No) {
        if o_manifest_cksums {
            ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("manifest checksums require a backup manifest")
                .finish(loc("parse_basebackup_options"))?;
        }
        opt.manifest_checksum_type = CHECKSUM_TYPE_NONE;
    }

    match target_str.as_deref() {
        None => {
            if target_detail_str.is_some() {
                ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("target detail cannot be used without target")
                    .finish(loc("parse_basebackup_options"))?;
            }
            opt.send_to_client = true;
        }
        Some("client") => {
            if target_detail_str.is_some() {
                ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("target \"client\" does not accept a target detail")
                    .finish(loc("parse_basebackup_options"))?;
            }
            opt.send_to_client = true;
        }
        Some(target) => {
            opt.target_handle = Some(basebackup_target::BaseBackupGetTargetHandle(
                target,
                target_detail_str.as_deref(),
            )?);
        }
    }

    if o_compression_detail && !o_compression {
        ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("compression detail cannot be specified unless compression is enabled")
            .finish(loc("parse_basebackup_options"))?;
    }
    if o_compression_detail {
        // Only "none" reaches here; none accepts no detail options
        // (validate_compress_specification wording).
        let detail = compression_detail_str.as_deref().unwrap_or("");
        ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("invalid compression specification: compression algorithm \"none\" does not accept a compression level"))
            .errdetail(format!("Compression detail was \"{detail}\"."))
            .finish(loc("parse_basebackup_options"))?;
    }

    if opt.incremental {
        // Incremental requires a prior UPLOAD_MANIFEST (still unported).
        ereport(ERROR).errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("must UPLOAD_MANIFEST before performing an incremental BASE_BACKUP")
            .finish(loc("parse_basebackup_options"))?;
    }
    Ok(opt)
}

// ===========================================================================
// SendBaseBackup — the BASE_BACKUP entry point.
// ===========================================================================

pub fn SendBaseBackup<'mcx>(mcx: Mcx<'mcx>, cmd: &BaseBackupCmd) -> PgResult<()> {
    if transam_xlog::get_backup_status() == transam_xlog::SessionBackupState::Running {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("a backup is already in progress in this session")
            .finish(loc("SendBaseBackup"));
    }

    let mut opt = parse_basebackup_options(&cmd.options)?;

    walsender::WalSndSetState(WalSndState::Backup);

    if ps_status::update_process_title() {
        let mut msg = format!("sending backup \"{}\"", opt.label);
        if msg.len() > 49 {
            msg.truncate(truncate_char_boundary(&msg, 49));
        }
        ps_status_seams::set_ps_display::call(msg.as_str());
    }

    // Client copy sink; if the target is not 'client' the backup data goes
    // wherever BaseBackupGetSink routes it instead. Server-side compression
    // is refused in parse, so no compression sink layers.
    let mut sink: Box<Bbsink<'mcx>> = backup_copy::bbsink_copystream_new(mcx, opt.send_to_client);
    if let Some(handle) = opt.target_handle.take() {
        sink = basebackup_target::BaseBackupGetSink(mcx, handle, sink)?;
    }
    if opt.maxrate > 0 {
        sink = throttle::bbsink_throttle_new(mcx, sink, opt.maxrate);
    }

    // Set up progress reporting (basebackup.c:1051). Always wrapped, as in C;
    // opt.progress only controls the (unported, inc-5 refusal-free) size
    // estimate, so bytes_total stays invalid — equivalent to --no-estimate-size.
    sink = sink_support::bbsink_progress_new(mcx, sink, opt.progress);

    let mut state = BbsinkState::default();
    // The DestRemoteSimple bridge needs the command mcx during the synchronous
    // result-set sends inside perform_base_backup.
    bcs_bridge::set_backup_mcx(mcx);
    let result = perform_base_backup(mcx, &opt, &mut sink, &mut state);
    bcs_bridge::clear_backup_mcx();

    // PG_FINALLY: always clean up the sink; propagate the primary error first.
    let cleanup = bbsink_cleanup(&mut sink, &mut state);
    result?;
    cleanup
}

fn truncate_char_boundary(s: &str, max: usize) -> usize {
    let mut idx = max.min(s.len());
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}

// ===========================================================================
// perform_base_backup.
// ===========================================================================

fn perform_base_backup<'mcx>(
    mcx: Mcx<'mcx>,
    opt: &BasebackupOptions,
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
) -> PgResult<()> {
    state.tablespaces = Vec::new();
    state.tablespace_num = 0;
    state.bytes_done = 0;
    state.bytes_total = 0;
    state.bytes_total_is_valid = false;

    BACKUP_STARTED_IN_RECOVERY.with(|c| c.set(transam_xlog::RecoveryInProgress()));
    TOTAL_CHECKSUM_FAILURES.with(|c| c.set(0));

    let mut manifest = BackupManifestInfo::zeroed();
    InitializeBackupManifest(mcx, &mut manifest, opt.manifest, opt.manifest_checksum_type)?;

    // do_pg_backup_start (inc-4, C-faithful out-params): fills the tablespace
    // list, the BackupState, and the tablespace_map bytes.
    transam_xlog::register_persistent_abort_backup_handler()?;
    let mut backup_state = xlogbackup::BackupState::default();
    let mut tablespace_map: Vec<u8> = Vec::new();
    sink_support::basebackup_progress_wait_checkpoint();
    transam_xlog::do_pg_backup_start(
        &opt.label,
        opt.fastcheckpoint,
        Some(&mut state.tablespaces),
        &mut backup_state,
        &mut tablespace_map,
    )?;

    state.startptr = backup_state.startpoint;
    state.starttli = backup_state.starttli;

    let mut endptr: XLogRecPtr = 0;
    let mut endtli: TimeLineID = 0;

    let mut body = || -> PgResult<()> {
        // Node for the base directory, sent last.
        state.tablespaces.push(TablespaceInfo {
            oid: INVALID_OID,
            path: None,
            rpath: None,
            size: -1,
        });

        bbsink_begin_backup(sink, state, SINK_BUFFER_LENGTH)?;

        let n = state.tablespaces.len();
        for i in 0..n {
            let (is_pgdata, path, oid) = {
                let ti = &state.tablespaces[i];
                (ti.path.is_none(), ti.path.clone(), ti.oid)
            };

            if is_pgdata {
                bbsink_begin_archive(sink, state, "base.tar")?;

                // backup_label first.
                // build_backup_content_default wasn't in the checked-out inc-4
                // xlogbackup; call the guaranteed-present lower-level fn.
                let backup_label =
                    xlogbackup::build_backup_content(mcx, &backup_state, false, transam_xlog::wal_segment_size())?;
                sendFileWithContent(sink, state, BACKUP_LABEL_FILE, &backup_label, &mut manifest)?;

                let mut sendtblspclinks = true;
                if opt.sendtblspcmapfile {
                    sendFileWithContent(sink, state, TABLESPACE_MAP, &tablespace_map, &mut manifest)?;
                    sendtblspclinks = false;
                }

                sendDir(sink, state, ".", 1, sendtblspclinks, &mut manifest)?;

                // pg_control last.
                let statbuf = match lstat_file(XLOG_CONTROL_FILE)? {
                    Some(s) => s,
                    None => {
                        return ereport(ERROR).errcode_for_file_access()
                            .errmsg(format!("could not stat file \"{XLOG_CONTROL_FILE}\""))
                            .finish(loc("perform_base_backup"));
                    }
                };
                sendFile(sink, state, XLOG_CONTROL_FILE, XLOG_CONTROL_FILE, &statbuf, false, INVALID_OID, None, &mut manifest)?;
            } else {
                let archive_name = format!("{oid}.tar");
                bbsink_begin_archive(sink, state, &archive_name)?;
                sendTablespace(sink, state, path.as_deref().unwrap(), oid, &mut manifest)?;
            }

            // If we're including WAL, and this is the main data directory,
            // don't treat this as the end of the tablespace: the xlog files
            // are appended below and the archive terminated afterwards. Safe
            // because the main data directory is always sent last.
            if opt.includewal && is_pgdata {
                debug_assert!(i == n - 1);
            } else {
                // Terminate the tarfile.
                zero_buffer(sink, 2 * TAR_BLOCK_SIZE);
                bbsink_archive_contents(sink, state, 2 * TAR_BLOCK_SIZE)?;
                // tablespace_num is advanced by the progress sink's end_archive
                // (basebackup_progress.c:139), which is always in the chain.
                bbsink_end_archive(sink, state)?;
            }
        }

        sink_support::basebackup_progress_wait_wal_archive(state);
        transam_xlog::do_pg_backup_stop(&mut backup_state, !opt.nowait)?;
        endptr = backup_state.stoppoint;
        endtli = backup_state.stoptli;
        Ok(())
    };

    // PG_ENSURE_ERROR_CLEANUP(do_pg_abort_backup): abort the backup on failure.
    match body() {
        Ok(()) => {}
        Err(e) => {
            let _ = transam_xlog::do_pg_abort_backup(false);
            return Err(e);
        }
    }

    if opt.includewal {
        // We've left the last tar file "open", so we can now append the
        // required WAL files to it (basebackup.c:409). Scan pg_wal and
        // include all WAL files in the range between startptr and endptr,
        // regardless of the timeline the file is stamped with.
        sink_support::basebackup_progress_transfer_wal();

        let wal_segsz = transam_xlog::wal_segment_size();
        let startsegno = state.startptr / wal_segsz as u64; // XLByteToSeg
        let firstoff = xlog_file_name(state.starttli, startsegno, wal_segsz);
        let endsegno = (endptr - 1) / wal_segsz as u64; // XLByteToPrevSeg
        let lastoff = xlog_file_name(endtli, endsegno, wal_segsz);

        let mut wal_file_list: Vec<String> = Vec::new();
        let mut history_file_list: Vec<String> = Vec::new();
        for name in read_dir_names("pg_wal")? {
            if is_xlog_file_name(&name)
                && name[8..] >= firstoff[8..]
                && name[8..] <= lastoff[8..]
            {
                wal_file_list.push(name);
            } else if transam_xlog::IsTLHistoryFileName(&name) {
                history_file_list.push(name);
            }
        }

        // Check that none of the WAL segments we need were removed.
        transam_xlog::CheckXLogRemoved(startsegno, state.starttli)?;

        // Oldest to newest, to reduce the chance of recycling mid-send.
        wal_file_list.sort_by(|a, b| a[8..].cmp(&b[8..]));

        if wal_file_list.is_empty() {
            return ereport(ERROR)
                .errmsg("could not find any WAL files")
                .finish(loc("perform_base_backup"));
        }

        // Sanity check: first and last segments cover startptr and endptr,
        // with no gaps in between.
        let (_, mut segno) = xlog_from_file_name(&wal_file_list[0], wal_segsz);
        if segno != startsegno {
            let startfname = xlog_file_name(state.starttli, startsegno, wal_segsz);
            return ereport(ERROR)
                .errmsg(format!("could not find WAL file \"{startfname}\""))
                .finish(loc("perform_base_backup"));
        }
        for wal_file_name in &wal_file_list {
            let currsegno = segno;
            let nextsegno = segno + 1;
            let (tli, s) = xlog_from_file_name(wal_file_name, wal_segsz);
            segno = s;
            if !(nextsegno == segno || currsegno == segno) {
                let nextfname = xlog_file_name(tli, nextsegno, wal_segsz);
                return ereport(ERROR)
                    .errmsg(format!("could not find WAL file \"{nextfname}\""))
                    .finish(loc("perform_base_backup"));
            }
        }
        if segno != endsegno {
            let endfname = xlog_file_name(endtli, endsegno, wal_segsz);
            return ereport(ERROR)
                .errmsg(format!("could not find WAL file \"{endfname}\""))
                .finish(loc("perform_base_backup"));
        }

        // Ok, we have everything we need. Send the WAL files.
        for wal_file_name in &wal_file_list {
            let pathbuf = format!("pg_wal/{wal_file_name}");
            let (tli, segno) = xlog_from_file_name(wal_file_name, wal_segsz);

            let fd = match fd::OpenTransientFile(&pathbuf, O_RDONLY) {
                Ok(fd) if fd >= 0 => fd,
                _ => {
                    // Most likely the file was already removed by a
                    // checkpoint; check for a better error message.
                    let e = std::io::Error::last_os_error();
                    transam_xlog::CheckXLogRemoved(segno, tli)?;
                    return ereport(ERROR)
                        .errcode_for_file_access()
                        .errmsg(format!("could not open file \"{pathbuf}\": {e}"))
                        .finish(loc("perform_base_backup"));
                }
            };
            let statbuf = fstat_fd(fd, &pathbuf)?;
            if statbuf.size != wal_segsz as i64 {
                fd::CloseTransientFile(fd);
                transam_xlog::CheckXLogRemoved(segno, tli)?;
                return ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("unexpected WAL file size \"{wal_file_name}\""))
                    .finish(loc("perform_base_backup"));
            }

            // Send the WAL file itself. WAL segments are deliberately not
            // added to the manifest (AddWALInfoToBackupManifest records the
            // range instead).
            _tarWriteHeader(sink, state, &pathbuf, None, &statbuf)?;

            let mut len: i64 = 0;
            loop {
                let want = sink.buffer_length().min((wal_segsz as i64 - len) as usize);
                // SAFETY: buf is a live writable slice; fd is an open file.
                let cnt = {
                    let buf = sink.buffer_slice_mut(want);
                    unsafe {
                        libc::pread(fd, buf.as_mut_ptr().cast(), buf.len(), len as libc::off_t)
                    }
                };
                if cnt < 0 {
                    fd::CloseTransientFile(fd);
                    return ereport(ERROR)
                        .errcode_for_file_access()
                        .errmsg(format!("could not read file \"{pathbuf}\""))
                        .finish(loc("perform_base_backup"));
                }
                if cnt == 0 {
                    break;
                }
                transam_xlog::CheckXLogRemoved(segno, tli)?;
                bbsink_archive_contents(sink, state, cnt as usize)?;
                len += cnt as i64;
                if len == wal_segsz as i64 {
                    break;
                }
            }
            if len != wal_segsz as i64 {
                transam_xlog::CheckXLogRemoved(segno, tli)?;
                fd::CloseTransientFile(fd);
                return ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("unexpected WAL file size \"{wal_file_name}\""))
                    .finish(loc("perform_base_backup"));
            }

            // wal_segment_size is a multiple of TAR_BLOCK_SIZE: no padding.
            fd::CloseTransientFile(fd);

            // Mark file as archived, otherwise files can get archived again
            // after promotion of a new node.
            let done_path = transam_xlog::StatusFilePath(wal_file_name, ".done");
            sendFileWithContent(sink, state, &done_path, b"", &mut manifest)?;
        }

        // Send timeline history files too — small and highly useful for
        // debugging, so include them all, always.
        for fname in &history_file_list {
            let pathbuf = format!("pg_wal/{fname}");
            let statbuf = match lstat_file(&pathbuf)? {
                Some(s) => s,
                None => {
                    return ereport(ERROR)
                        .errcode_for_file_access()
                        .errmsg(format!("could not stat file \"{pathbuf}\""))
                        .finish(loc("perform_base_backup"));
                }
            };
            sendFile(sink, state, &pathbuf, &pathbuf, &statbuf, false, INVALID_OID, None, &mut manifest)?;

            // Unconditionally mark file as archived.
            let done_path = transam_xlog::StatusFilePath(fname, ".done");
            sendFileWithContent(sink, state, &done_path, b"", &mut manifest)?;
        }

        // Properly terminate the tar file.
        zero_buffer(sink, 2 * TAR_BLOCK_SIZE);
        bbsink_archive_contents(sink, state, 2 * TAR_BLOCK_SIZE)?;
        bbsink_end_archive(sink, state)?;
    }

    AddWALInfoToBackupManifest(mcx, &mut manifest, state.startptr, state.starttli, endptr, endtli)?;
    // manifest ships a finalize-and-return-bytes SendBackupManifest; stream the
    // returned bytes through the sink's manifest dispatch (Lane C option (a)).
    let mbytes = SendBackupManifest(&mut manifest)?;
    sink::bbsink_begin_manifest(sink, state)?;
    let mut off = 0usize;
    while off < mbytes.len() {
        let n = sink.buffer_length().min(mbytes.len() - off);
        sink.buffer_slice_mut(n).copy_from_slice(&mbytes[off..off + n]);
        sink::bbsink_manifest_contents(sink, state, n)?;
        off += n;
    }
    sink::bbsink_end_manifest(sink, state)?;
    bbsink_end_backup(sink, state, endptr, endtli)?;

    FreeBackupManifest(&mut manifest);

    let total_checksum_failures = TOTAL_CHECKSUM_FAILURES.with(Cell::get);
    if total_checksum_failures != 0 {
        if total_checksum_failures > 1 {
            let _ = ereport(WARNING)
                .errmsg_plural(
                    format!("{total_checksum_failures} total checksum verification failure"),
                    format!("{total_checksum_failures} total checksum verification failures"),
                    total_checksum_failures as u64,
                )
                .finish(loc("perform_base_backup"));
        }
        return ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg("checksum verification failure during base backup")
            .finish(loc("perform_base_backup"));
    }

    sink_support::basebackup_progress_done();
    Ok(())
}

fn zero_buffer(sink: &mut Bbsink<'_>, len: usize) {
    sink.buffer_slice_mut(len).fill(0);
}

// ---------------------------------------------------------------------------
// WAL filename helpers (xlog_internal.h macros, inlined for the includewal
// section; the transam_xlog copies are pub(crate)).
// ---------------------------------------------------------------------------

// IsXLogFileName: 24 upper-case hex characters.
fn is_xlog_file_name(fname: &str) -> bool {
    fname.len() == 24
        && fname
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'A'..=b'F').contains(&c))
}

// XLogFileName.
fn xlog_file_name(tli: TimeLineID, seg_no: u64, wal_segsz: i32) -> String {
    let per_id = 0x1_0000_0000u64 / wal_segsz as u64;
    format!("{:08X}{:08X}{:08X}", tli, seg_no / per_id, seg_no % per_id)
}

// XLogFromFileName.
fn xlog_from_file_name(fname: &str, wal_segsz: i32) -> (TimeLineID, u64) {
    let per_id = 0x1_0000_0000u64 / wal_segsz as u64;
    let tli = u32::from_str_radix(&fname[0..8], 16).unwrap_or(0);
    let log = u64::from_str_radix(&fname[8..16], 16).unwrap_or(0);
    let seg = u64::from_str_radix(&fname[16..24], 16).unwrap_or(0);
    (tli, log * per_id + seg)
}

// verify_page_checksum (basebackup.c:104): None = page OK (new page, page
// newer than the backup start, or checksum matches); Some(calculated) on a
// mismatch.
fn verify_page_checksum(page: &[u8], start_lsn: XLogRecPtr, blkno: u32) -> Option<u16> {
    // PageIsNew: pd_upper == 0.
    let pd_upper = u16::from_ne_bytes([page[14], page[15]]);
    // PageGetLSN: pd_lsn = { xlogid u32, xrecoff u32 }.
    let lsn = ((u32::from_ne_bytes(page[0..4].try_into().unwrap()) as u64) << 32)
        | u32::from_ne_bytes(page[4..8].try_into().unwrap()) as u64;
    if pd_upper == 0 || lsn >= start_lsn {
        return None;
    }
    let checksum = pg_checksum_page(page, blkno);
    let pd_checksum = u16::from_ne_bytes([page[8], page[9]]);
    if pd_checksum == checksum {
        None
    } else {
        Some(checksum)
    }
}

// pg_checksum_page (storage/checksum_impl.h): FNV-1a-derived block checksum
// computed with pd_checksum treated as zero (same algorithm as bufmgr's
// PageSetChecksumInplace).
fn pg_checksum_page(page: &[u8], blkno: u32) -> u16 {
    const N_SUMS: usize = 32;
    const FNV_PRIME: u32 = 16777619;
    const CHECKSUM_BASE_OFFSETS: [u32; N_SUMS] = [
        0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A, 0x79FF467A, 0x9BB9F8A3, 0x217E7CD2,
        0x83E13D2C, 0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA, 0x7B093B5D, 0x98DAFF3C,
        0xF718902A, 0x0B1C9CDB, 0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE, 0x92BEC979,
        0xCCA6C0B2, 0x304A0979, 0x85AA43D4, 0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E,
        0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756,
    ];
    #[inline(always)]
    fn comp(sum: &mut u32, value: u32) {
        let tmp = *sum ^ value;
        *sum = tmp.wrapping_mul(FNV_PRIME) ^ (tmp >> 17);
    }
    let blcksz = types_core::BLCKSZ;
    debug_assert_eq!(page.len(), blcksz);
    let mut sums = CHECKSUM_BASE_OFFSETS;
    let rows = blcksz / (4 * N_SUMS);
    for row in 0..rows {
        for (lane, sum) in sums.iter_mut().enumerate() {
            let off = (row * N_SUMS + lane) * 4;
            // pd_checksum (bytes 8..10) is computed as zero.
            let v = if off == 8 {
                u32::from_ne_bytes([0, 0, page[10], page[11]])
            } else {
                u32::from_ne_bytes(page[off..off + 4].try_into().unwrap())
            };
            comp(sum, v);
        }
    }
    for _ in 0..2 {
        for sum in sums.iter_mut() {
            comp(sum, 0);
        }
    }
    let mut checksum: u32 = sums.into_iter().fold(0, |a, s| a ^ s);
    checksum ^= blkno;
    (checksum % 65535 + 1) as u16
}

fn fstat_fd(fd: i32, path: &str) -> PgResult<LstatInfo> {
    // SAFETY: zeroed stat is POD; fd is an open file descriptor.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::fstat(fd, &mut st) };
    if rc != 0 {
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not stat file \"{path}\""))
            .finish(loc("fstat_fd"))
            .map(|()| unreachable!());
    }
    Ok(LstatInfo {
        size: st.st_size,
        mode: st.st_mode as u32,
        uid: st.st_uid,
        gid: st.st_gid,
        #[cfg(not(target_family = "wasm"))]
        mtime: st.st_mtime,
        // wasm32: wasi-libc's stat spells it st_mtim (timespec), no
        // st_mtime alias in the libc crate.
        #[cfg(target_family = "wasm")]
        mtime: st.st_mtim.tv_sec as i64,
    })
}

// ===========================================================================
// sendFileWithContent / sendTablespace / sendDir / sendFile.
// ===========================================================================

fn sendFileWithContent(
    sink: &mut Bbsink<'_>,
    state: &mut BbsinkState,
    filename: &str,
    content: &[u8],
    manifest: &mut BackupManifestInfo,
) -> PgResult<()> {
    let mut ctx = checksum_init(manifest.checksum_type(), filename)?;
    let len = content.len();

    let statbuf = LstatInfo {
        size: len as i64,
        mode: pg_file_create_mode(),
        uid: geteuid(),
        gid: getegid(),
        mtime: time_now(),
    };

    _tarWriteHeader(sink, state, filename, None, &statbuf)?;
    checksum_update(&mut ctx, content)?;

    let mut done = 0usize;
    while done < len {
        let nbytes = sink.buffer_length().min(len - done);
        sink.buffer_slice_mut(nbytes).copy_from_slice(&content[done..done + nbytes]);
        bbsink_archive_contents(sink, state, nbytes)?;
        done += nbytes;
    }
    _tarWritePadding(sink, state, len)?;

    AddFileToBackupManifest(manifest, INVALID_OID, filename.as_bytes(), len as i64, statbuf.mtime, &mut ctx)
}

fn sendTablespace(
    sink: &mut Bbsink<'_>,
    state: &mut BbsinkState,
    path: &str,
    spcoid: Oid,
    manifest: &mut BackupManifestInfo,
) -> PgResult<i64> {
    let pathbuf = format!("{path}/{TABLESPACE_VERSION_DIRECTORY}");
    let statbuf = match lstat_file(&pathbuf)? {
        Some(s) => s,
        None => return Ok(0), // tablespace went away — not an error
    };
    let mut size = _tarWriteHeader(sink, state, TABLESPACE_VERSION_DIRECTORY, None, &statbuf)?;
    size += sendDir_spc(sink, state, &pathbuf, path.len() as i32, true, manifest, spcoid)?;
    Ok(size)
}

fn sendDir(
    sink: &mut Bbsink<'_>,
    state: &mut BbsinkState,
    path: &str,
    basepathlen: i32,
    sendtblspclinks: bool,
    manifest: &mut BackupManifestInfo,
) -> PgResult<i64> {
    sendDir_spc(sink, state, path, basepathlen, sendtblspclinks, manifest, INVALID_OID)
}

fn sendDir_spc(
    sink: &mut Bbsink<'_>,
    state: &mut BbsinkState,
    path: &str,
    basepathlen: i32,
    sendtblspclinks: bool,
    manifest: &mut BackupManifestInfo,
    spcoid: Oid,
) -> PgResult<i64> {
    let mut size: i64 = 0;

    // Determine if the current path is a database directory that can contain
    // relations (basebackup.c sendDir head): last path component all digits
    // with parent "./base" or a tablespace version path, or "./global".
    let (is_relation_dir, dboid): (bool, u32) = match path.rfind('/') {
        Some(idx)
            if idx + 1 < path.len()
                && path[idx + 1..].bytes().all(|b| b.is_ascii_digit()) =>
        {
            let parent = &path[..idx];
            if parent == "./base" || parent.ends_with(TABLESPACE_VERSION_DIRECTORY) {
                (true, path[idx + 1..].parse::<u32>().unwrap_or(0))
            } else {
                (false, 0)
            }
        }
        _ => (path == "./global", 0),
    };

    for d_name in read_dir_names(path)? {
        if d_name == "." || d_name == ".." || d_name == ".DS_Store" {
            continue;
        }
        if d_name.starts_with(PG_TEMP_FILE_PREFIX) {
            continue;
        }

        // Promotion mid-backup corrupts the backup.
        if transam_xlog::RecoveryInProgress() != BACKUP_STARTED_IN_RECOVERY.with(Cell::get) {
            return ereport(ERROR).errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("the standby was promoted during online backup")
                .finish(loc("sendDir")).map(|()| 0);
        }

        // Excluded files.
        let mut excluded = false;
        for item in EXCLUDE_FILES {
            let cmplen = if item.match_prefix { item.name.len() } else { item.name.len() + 1 };
            if strncmp(&d_name, item.name, cmplen) == 0 {
                excluded = true;
                break;
            }
        }
        if excluded {
            continue;
        }

        // If there could be non-temporary relation files in this directory,
        // try to parse the filename.
        let mut is_relation_file = false;
        let mut relfilenumber: u32 = 0;
        let mut segno_of: u32 = 0;
        let mut rel_fork = types_core::ForkNumber::MAIN_FORKNUM;
        if is_relation_dir {
            if let Some((num, fork, segno)) =
                fd::reinit::parse_filename_for_nontemp_relation(&d_name)
            {
                is_relation_file = true;
                relfilenumber = num;
                segno_of = segno;
                rel_fork = fork;
            }
        }

        // Exclude all forks for unlogged tables except the init fork: any
        // other fork with a matching _init fork present is skipped.
        if is_relation_file && rel_fork != types_core::ForkNumber::INIT_FORKNUM {
            let init_fork_file = format!("{path}/{relfilenumber}_init");
            if lstat_file(&init_fork_file)?.is_some() {
                continue;
            }
        }

        // Exclude temporary relations.
        if dboid != 0 && fd::looks_like_temp_rel_name(&d_name) {
            continue;
        }

        let pathbuf = format!("{path}/{d_name}");
        if pathbuf == format!("./{XLOG_CONTROL_FILE}") {
            continue; // pg_control sent last
        }

        let mut statbuf = match lstat_file(&pathbuf)? {
            Some(s) => s,
            None => continue, // vanished mid-scan
        };

        // Directories whose contents are excluded (kept as empty dirs).
        let mut excl_contents = false;
        for excl in EXCLUDE_DIR_CONTENTS {
            if &d_name == excl {
                convert_link_to_directory(&mut statbuf);
                size += _tarWriteHeader(sink, state, &pathbuf[basepathlen as usize + 1..], None, &statbuf)?;
                excl_contents = true;
                break;
            }
        }
        if excl_contents {
            continue;
        }

        // pg_wal is included as an empty directory (+ archive_status, summaries).
        if pathbuf == "./pg_wal" {
            convert_link_to_directory(&mut statbuf);
            size += _tarWriteHeader(sink, state, &pathbuf[basepathlen as usize + 1..], None, &statbuf)?;
            size += _tarWriteHeader(sink, state, "pg_wal/archive_status", None, &statbuf)?;
            size += _tarWriteHeader(sink, state, "pg_wal/summaries", None, &statbuf)?;
            continue;
        }

        if path == "./pg_tblspc" && S_ISLNK(statbuf.mode) {
            let linkpath = read_link(&pathbuf)?;
            size += _tarWriteHeader(
                sink, state, &pathbuf[basepathlen as usize + 1..], Some(&linkpath), &statbuf,
            )?;
        } else if S_ISDIR(statbuf.mode) {
            size += _tarWriteHeader(sink, state, &pathbuf[basepathlen as usize + 1..], None, &statbuf)?;

            // Recurse, unless this is a separate tablespace located within PGDATA.
            let mut skip = false;
            let cmp = &pathbuf[2..];
            for ti in state.tablespaces.iter() {
                if let Some(rpath) = &ti.rpath {
                    if rpath == cmp {
                        skip = true;
                        break;
                    }
                }
            }
            if pathbuf == "./pg_tblspc" && !sendtblspclinks {
                skip = true;
            }
            if !skip {
                size += sendDir_spc(sink, state, &pathbuf, basepathlen, sendtblspclinks, manifest, spcoid)?;
            }
        } else if S_ISREG(statbuf.mode) {
            let tarfilename = &pathbuf[basepathlen as usize + 1..];
            let relfile = if is_relation_file {
                Some((relfilenumber, segno_of))
            } else {
                None
            };
            let sent = sendFile(sink, state, &pathbuf, tarfilename, &statbuf, true, spcoid, relfile, manifest)?;
            if sent {
                size += statbuf.size;
                size += tar_padding_bytes_required(statbuf.size as usize) as i64;
                size += TAR_BLOCK_SIZE as i64;
            }
        } else {
            let _ = ereport(WARNING)
                .errmsg(format!("skipping special file \"{pathbuf}\""))
                .finish(loc("sendDir"));
        }
    }

    Ok(size)
}

fn sendFile(
    sink: &mut Bbsink<'_>,
    state: &mut BbsinkState,
    readfilename: &str,
    tarfilename: &str,
    statbuf: &LstatInfo,
    missing_ok: bool,
    spcoid: Oid,
    // Some((relfilenumber, segno)) when the caller parsed a relation
    // filename in a relation directory (checksum verification surface).
    relfile: Option<(u32, u32)>,
    manifest: &mut BackupManifestInfo,
) -> PgResult<bool> {
    let mut ctx = checksum_init(manifest.checksum_type(), readfilename)?;

    let fd = match fd::OpenTransientFile(readfilename, O_RDONLY) {
        Ok(fd) if fd >= 0 => fd,
        _ => {
            if missing_ok && std::io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT) {
                return Ok(false);
            }
            return ereport(ERROR).errcode_for_file_access()
                .errmsg(format!("could not open file \"{readfilename}\""))
                .finish(loc("sendFile")).map(|()| false);
        }
    };

    _tarWriteHeader(sink, state, tarfilename, None, statbuf)?;

    // If we weren't told not to verify checksums, and checksums are enabled
    // for this cluster, and this is a relation file, verify per-block.
    let mut verify_checksum = !NOVERIFY_CHECKSUMS.with(Cell::get)
        && transam_xlog::DataChecksumsEnabled()
        && relfile.is_some();
    let segno = relfile.map(|(_, s)| s).unwrap_or(0);
    let mut checksum_failures: i32 = 0;
    let mut blkno: u32 = 0;
    const BLCKSZ: usize = types_core::BLCKSZ;
    const RELSEG_SIZE: u32 = (1024 * 1024 * 1024) / BLCKSZ as u32;

    let mut bytes_done: i64 = 0;
    loop {
        if bytes_done >= statbuf.size {
            break;
        }
        let want = sink.buffer_length().min((statbuf.size - bytes_done) as usize);
        // buf is a live writable slice; fd is an open regular file.
        let mut cnt = {
            let buf = sink.buffer_slice_mut(want);
            fd::pg_pread(fd, buf, bytes_done)
        };
        if cnt < 0 {
            fd::CloseTransientFile(fd);
            return ereport(ERROR).errcode_for_file_access()
                .errmsg(format!("could not read file \"{readfilename}\""))
                .finish(loc("sendFile")).map(|()| false);
        }

        // read_file_data_into_buffer's per-block verification (basebackup.c).
        if verify_checksum && cnt > 0 && (cnt as usize % BLCKSZ) == 0 {
            let nblocks = cnt as usize / BLCKSZ;
            let abs_base = blkno + segno * RELSEG_SIZE;
            for i in 0..nblocks {
                let expected = {
                    let buf = sink.buffer_slice(cnt as usize);
                    verify_page_checksum(
                        &buf[i * BLCKSZ..(i + 1) * BLCKSZ],
                        state.startptr,
                        abs_base + i as u32,
                    )
                };
                let Some(_) = expected else { continue };

                // Retry the block once: a torn concurrent write may finish
                // and update the page LSN so we then skip it.
                let reread_cnt = {
                    let buf = sink.buffer_slice_mut(cnt as usize);
                    unsafe {
                        libc::pread(
                            fd,
                            buf[i * BLCKSZ..].as_mut_ptr().cast(),
                            BLCKSZ,
                            bytes_done as libc::off_t + (i * BLCKSZ) as libc::off_t,
                        )
                    }
                };
                if reread_cnt == 0 {
                    // Concurrent truncation: keep only the processed blocks.
                    cnt = (BLCKSZ * i) as isize;
                    break;
                }
                let (expected, actual) = {
                    let buf = sink.buffer_slice(cnt as usize);
                    let page = &buf[i * BLCKSZ..(i + 1) * BLCKSZ];
                    (
                        verify_page_checksum(page, state.startptr, abs_base + i as u32),
                        u16::from_ne_bytes([page[8], page[9]]),
                    )
                };
                let Some(expected) = expected else { continue };

                checksum_failures += 1;
                if checksum_failures <= 5 {
                    let _ = ereport(WARNING)
                        .errmsg(format!(
                            "checksum verification failed in file \"{readfilename}\", block {}: calculated {:X} but expected {:X}",
                            abs_base + i as u32, expected, actual
                        ))
                        .finish(loc("sendFile"));
                }
                if checksum_failures == 5 {
                    let _ = ereport(WARNING)
                        .errmsg(format!(
                            "further checksum verification failures in file \"{readfilename}\" will not be reported"
                        ))
                        .finish(loc("sendFile"));
                }
            }
        }

        // Block-level checksums can't be verified on a partial read.
        if verify_checksum && cnt > 0 && (cnt as usize % BLCKSZ) != 0 {
            let _ = ereport(WARNING)
                .errmsg(format!(
                    "could not verify checksum in file \"{readfilename}\", block {blkno}: read buffer size {cnt} and page size {BLCKSZ} differ"
                ))
                .finish(loc("sendFile"));
            verify_checksum = false;
        }

        if cnt == 0 {
            break; // concurrent truncation
        }
        blkno += (cnt as usize / BLCKSZ) as u32;
        let chunk = sink.buffer_slice(cnt as usize).to_vec();
        checksum_update(&mut ctx, &chunk)?;
        bbsink_archive_contents(sink, state, cnt as usize)?;
        bytes_done += cnt as i64;
    }

    // Pad with zeros if truncated during send.
    while bytes_done < statbuf.size {
        let nbytes = sink.buffer_length().min((statbuf.size - bytes_done) as usize);
        zero_buffer(sink, nbytes);
        let chunk = sink.buffer_slice(nbytes).to_vec();
        checksum_update(&mut ctx, &chunk)?;
        bbsink_archive_contents(sink, state, nbytes)?;
        bytes_done += nbytes as i64;
    }

    _tarWritePadding(sink, state, bytes_done as usize)?;
    fd::CloseTransientFile(fd);

    if checksum_failures > 1 {
        // pgstat checksum-failure reporting is monitoring-only and deferred.
        let _ = ereport(WARNING)
            .errmsg_plural(
                format!(
                    "file \"{readfilename}\" has a total of {checksum_failures} checksum verification failure"
                ),
                format!(
                    "file \"{readfilename}\" has a total of {checksum_failures} checksum verification failures"
                ),
                checksum_failures as u64,
            )
            .finish(loc("sendFile"));
    }
    TOTAL_CHECKSUM_FAILURES.with(|c| c.set(c.get() + checksum_failures as i64));

    AddFileToBackupManifest(manifest, spcoid, tarfilename.as_bytes(), statbuf.size, statbuf.mtime, &mut ctx)?;
    Ok(true)
}

// ===========================================================================
// tar header emission + helpers.
// ===========================================================================

fn _tarWriteHeader(
    sink: &mut Bbsink<'_>,
    state: &mut BbsinkState,
    filename: &str,
    linktarget: Option<&str>,
    statbuf: &LstatInfo,
) -> PgResult<i64> {
    let (rc, header) = tar_create_header(
        filename, linktarget, statbuf.size, statbuf.mode, statbuf.uid, statbuf.gid, statbuf.mtime,
    );
    match rc {
        TarError::Ok => {}
        TarError::NameTooLong => {
            return ereport(ERROR)
                .errmsg(format!("file name too long for tar format: \"{filename}\""))
                .finish(loc("_tarWriteHeader")).map(|()| 0);
        }
        TarError::SymlinkTooLong => {
            return ereport(ERROR)
                .errmsg(format!(
                    "symbolic link target too long for tar format: file name \"{}\", target \"{}\"",
                    filename, linktarget.unwrap_or("")
                ))
                .finish(loc("_tarWriteHeader")).map(|()| 0);
        }
    }
    sink.buffer_slice_mut(TAR_BLOCK_SIZE).copy_from_slice(&header);
    bbsink_archive_contents(sink, state, TAR_BLOCK_SIZE)?;
    Ok(TAR_BLOCK_SIZE as i64)
}

fn _tarWritePadding(sink: &mut Bbsink<'_>, state: &mut BbsinkState, len: usize) -> PgResult<()> {
    let pad = tar_padding_bytes_required(len);
    if pad > 0 {
        zero_buffer(sink, pad);
        bbsink_archive_contents(sink, state, pad)?;
    }
    Ok(())
}

fn tar_padding_bytes_required(len: usize) -> usize {
    (len + (TAR_BLOCK_SIZE - 1)) / TAR_BLOCK_SIZE * TAR_BLOCK_SIZE - len
}

fn convert_link_to_directory(statbuf: &mut LstatInfo) {
    if S_ISLNK(statbuf.mode) {
        statbuf.mode = S_IFDIR | pg_dir_create_mode();
    }
}

fn checksum_init(type_: PgChecksumType, _filename: &str) -> PgResult<PgChecksumContext> {
    Ok(PgChecksumContext::init(type_))
}

fn checksum_update(ctx: &mut PgChecksumContext, data: &[u8]) -> PgResult<()> {
    ctx.update(data);
    Ok(())
}

// pg_checksum_parse_type (checksum_helper.c) — case-insensitive algorithm name.
fn parse_checksum_type(name: &[u8]) -> Option<PgChecksumType> {
    match name.to_ascii_uppercase().as_slice() {
        b"NONE" => Some(PgChecksumType::None),
        b"CRC32C" => Some(PgChecksumType::Crc32c),
        b"SHA224" => Some(PgChecksumType::Sha224),
        b"SHA256" => Some(PgChecksumType::Sha256),
        b"SHA384" => Some(PgChecksumType::Sha384),
        b"SHA512" => Some(PgChecksumType::Sha512),
        _ => None,
    }
}

fn strncmp(a: &str, b: &str, n: usize) -> i32 {
    let (ab, bb) = (a.as_bytes(), b.as_bytes());
    for i in 0..n {
        let (ca, cb) = (ab.get(i).copied(), bb.get(i).copied());
        match (ca, cb) {
            (Some(x), Some(y)) if x == y => {
                if x == 0 {
                    return 0;
                }
            }
            (x, y) => return x.unwrap_or(0) as i32 - y.unwrap_or(0) as i32,
        }
    }
    0
}

// File-permission globals for injected files (backup_label, tablespace_map,
// .done markers) and symlink-to-directory conversions: read fd's file_perm
// globals — the exact values the server's own file creation uses. (The
// init_small data_directory_mode copy read stale 0700 in walsender threads;
// pg_basebackup extracts these members with the TAR HEADER modes, so a stale
// header broke the group-permission leg of pg_basebackup/010 subtest 83.)
fn pg_file_create_mode() -> u32 {
    fd::vfd::pg_file_create_mode()
}
fn pg_dir_create_mode() -> u32 {
    fd::vfd::pg_dir_create_mode()
}
#[cfg(not(target_family = "wasm"))]
fn geteuid() -> u32 {
    // SAFETY: geteuid never fails.
    unsafe { libc::geteuid() }
}
#[cfg(not(target_family = "wasm"))]
fn getegid() -> u32 {
    // SAFETY: getegid never fails.
    unsafe { libc::getegid() }
}
// wasm32: WASI has no uids/gids; 0 is the tar-header owner word C would
// emit on a credential-less platform (base backups are postmaster-only —
// unreachable from --single).
#[cfg(target_family = "wasm")]
fn geteuid() -> u32 {
    0
}
#[cfg(target_family = "wasm")]
fn getegid() -> u32 {
    0
}
fn time_now() -> i64 {
    // SAFETY: time(NULL) returns the current unix time.
    pg_clock::wall_secs()
}

// ===========================================================================
// init_seams — install the inward BASE_BACKUP seam walsender dispatches to.
// ===========================================================================

pub fn init_seams() {
    walsender_seams::base_backup::set(send_base_backup_entry);
    // manifest needs C's GetSystemIdentifier; backup_copy needs a flush + the
    // DestRemoteSimple result-set router (SendXlogRecPtrResult/SendTablespaceList).
    manifest::seams::get_system_identifier::set(|| transam_xlog::GetSystemIdentifier());
    backup_copy_seams::pq_flush_if_writable::set(|| pqcomm_seams::pq_flush::call());
    bcs_bridge::install();
}

// Bridge backup_copy's no_std DestRemoteSimple router seams to the real
// exectuples_output result-set path. backup_copy pushes logical rows through the
// opaque-handle seams; we buffer them and materialize + send in end_tup_output.
// Wire output is byte-identical (RowDescription + DataRows + CommandComplete, in
// order) — deferral within the same command is invisible on the wire.
mod bcs_bridge {
    use core::cell::{Cell, RefCell};
    use std::collections::HashMap;
    use std::rc::Rc;

    use backup_copy_seams::{
        DestReceiverHandle, ResultColumn, ResultColumnType, ResultValue, TupOutputState,
    };
    use datum::Datum;
    use mcx::{Mcx, MemoryContext};
    use types_core::{Oid, INT8OID, OIDOID, TEXTOID};
    use types_error::PgResult;

    struct Buffered {
        columns: Vec<ResultColumn>,
        rows: Vec<Vec<Option<ResultValue>>>,
    }

    thread_local! {
        static BACKUP_MCX: Cell<usize> = const { Cell::new(0) };
        static NEXT_ID: Cell<u64> = const { Cell::new(1) };
        static REGISTRY: RefCell<HashMap<u64, Buffered>> = RefCell::new(HashMap::new());
    }

    pub fn set_backup_mcx(mcx: Mcx<'_>) {
        BACKUP_MCX.with(|c| c.set(mcx.context() as *const MemoryContext as usize));
    }
    pub fn clear_backup_mcx() {
        BACKUP_MCX.with(|c| c.set(0));
    }

    fn col_oid(t: ResultColumnType) -> Oid {
        match t {
            ResultColumnType::Text => TEXTOID,
            ResultColumnType::Int8 => INT8OID,
            ResultColumnType::Oid => OIDOID,
        }
    }

    fn create() -> DestReceiverHandle {
        let id = NEXT_ID.with(|c| {
            let v = c.get();
            c.set(v + 1);
            v
        });
        REGISTRY.with(|r| {
            r.borrow_mut()
                .insert(id, Buffered { columns: Vec::new(), rows: Vec::new() });
        });
        DestReceiverHandle(id)
    }

    fn begin(dest: DestReceiverHandle, columns: Vec<ResultColumn>) -> TupOutputState {
        REGISTRY.with(|r| {
            r.borrow_mut().get_mut(&dest.0).expect("bcs_bridge dest").columns = columns;
        });
        TupOutputState { dest }
    }

    fn do_out(tstate: TupOutputState, values: Vec<Option<ResultValue>>) {
        REGISTRY.with(|r| {
            r.borrow_mut()
                .get_mut(&tstate.dest.0)
                .expect("bcs_bridge dest")
                .rows
                .push(values);
        });
    }

    fn end(tstate: TupOutputState) {
        let buf = REGISTRY
            .with(|r| r.borrow_mut().remove(&tstate.dest.0))
            .expect("bcs_bridge dest");
        let p = BACKUP_MCX.with(|c| c.get());
        assert!(p != 0, "bcs_bridge: backup mcx not set");
        // SAFETY: the pointer is the live command mcx set by SendBaseBackup for
        // the synchronous duration of the backup, cleared on return.
        let ctx = unsafe { &*(p as *const MemoryContext) };
        materialize(ctx.mcx(), &buf).expect("bcs_bridge: result-set send");
    }

    fn materialize(mcx: Mcx<'_>, buf: &Buffered) -> PgResult<()> {
        let ncols = buf.columns.len();
        let mut dest = tcop_dest::CreateDestReceiver(types_dest::CommandDest::RemoteSimple);
        let mut td = tupdesc::CreateTemplateTupleDesc(mcx, ncols as i32)?;
        for (i, c) in buf.columns.iter().enumerate() {
            tupdesc::TupleDescInitBuiltinEntry(&mut td, (i + 1) as i16, &c.name, col_oid(c.typ), -1, 0)?;
        }
        let mut tstate = exectuples_output::begin_tup_output_tupdesc(mcx, &mut dest, Rc::new(td))?;
        for row in &buf.rows {
            let mut values = vec![Datum::null(); ncols];
            let mut nulls = vec![false; ncols];
            for (i, v) in row.iter().enumerate() {
                match v {
                    None => nulls[i] = true,
                    Some(ResultValue::Text(s)) => {
                        // varlena_result yields the image (header) pointer; as_bytes()
                        // would point past the header and corrupt the DataRow.
                        values[i] = fmgr::varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?);
                    }
                    Some(ResultValue::Int8(x)) => values[i] = Datum::from_i64(*x),
                    Some(ResultValue::Oid(o)) => values[i] = Datum::from_oid(*o),
                }
            }
            exectuples_output::do_tup_output(&mut tstate, mcx, &values, &nulls)?;
        }
        exectuples_output::end_tup_output(tstate)
    }

    pub fn install() {
        backup_copy_seams::create_dest_remote_simple::set(create);
        backup_copy_seams::begin_tup_output_tupdesc::set(begin);
        backup_copy_seams::do_tup_output::set(do_out);
        backup_copy_seams::end_tup_output::set(end);
    }
}

fn send_base_backup_entry(cmd: BaseBackupCmd) -> PgResult<()> {
    let ctx = mcx::MemoryContext::new("SendBaseBackup");
    SendBaseBackup(ctx.mcx(), &cmd)
}
