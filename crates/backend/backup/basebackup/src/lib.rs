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

fn loc(func: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRCFILE, 0, func)
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

// static bool backup_started_in_recovery (basebackup.c file-static).
thread_local! {
    static BACKUP_STARTED_IN_RECOVERY: Cell<bool> = const { Cell::new(false) };
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
    let c = std::ffi::CString::new(path).map_err(|_| {
        ereport(ERROR).errmsg(format!("invalid path \"{path}\"")).into_error()
    })?;
    // SAFETY: zeroed stat is POD; c is a valid NUL-terminated path.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::lstat(c.as_ptr(), &mut st) };
    if rc != 0 {
        let e = std::io::Error::last_os_error();
        if e.raw_os_error() == Some(libc::ENOENT) {
            return Ok(None);
        }
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not stat file \"{path}\""))
            .finish(loc("lstat_file"))
            .map(|()| None);
    }
    Ok(Some(LstatInfo {
        size: st.st_size as i64,
        mode: st.st_mode as u32,
        uid: st.st_uid as u32,
        gid: st.st_gid as u32,
        mtime: st.st_mtime as i64,
    }))
}

fn read_link(path: &str) -> PgResult<String> {
    match std::fs::read_link(path) {
        Ok(p) => Ok(p.to_string_lossy().into_owned()),
        Err(_) => {
            ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not read symbolic link \"{path}\""))
                .finish(loc("read_link"))?;
            unreachable!()
        }
    }
}

fn read_dir_names(path: &str) -> PgResult<Vec<String>> {
    let rd = match std::fs::read_dir(path) {
        Ok(rd) => rd,
        Err(_) => {
            ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not open directory \"{path}\""))
                .finish(loc("read_dir_names"))?;
            unreachable!()
        }
    };
    let mut names = Vec::new();
    for ent in rd.flatten() {
        names.push(ent.file_name().to_string_lossy().into_owned());
    }
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
    let (mut o_label, mut o_progress, mut o_checkpoint, mut o_nowait) = (false, false, false, false);
    let (mut o_wal, mut o_incremental, mut o_maxrate, mut o_tsmap) = (false, false, false, false);
    let (mut o_noverify, mut o_manifest, mut o_manifest_cksums) = (false, false, false);
    let mut o_target = false;
    let mut target_str: Option<String> = None;

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
                let _ = opt_bool(o)?; // backup-time checksum verification deferred
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
            "compression" | "compression_detail" => {
                refuse("server-side compression")?;
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
        None | Some("client") => opt.send_to_client = true,
        Some(_) => refuse("non-client BASE_BACKUP target")?,
    }

    if opt.incremental {
        // Incremental requires a prior UPLOAD_MANIFEST (still unported).
        ereport(ERROR).errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("must UPLOAD_MANIFEST before performing an incremental BASE_BACKUP")
            .finish(loc("parse_basebackup_options"))?;
    }
    if opt.includewal {
        refuse("BASE_BACKUP WAL inclusion (use -X stream)")?;
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

    let opt = parse_basebackup_options(&cmd.options)?;

    walsender::WalSndSetState(WalSndState::Backup);

    if ps_status::update_process_title() {
        let mut msg = format!("sending backup \"{}\"", opt.label);
        if msg.len() > 49 {
            msg.truncate(truncate_char_boundary(&msg, 49));
        }
        ps_status_seams::set_ps_display::call(msg.as_str());
    }

    // Client copy sink (+ optional throttle). Server targets and compression are
    // refused in parse, so no other sink layers.
    let mut sink: Box<Bbsink<'mcx>> = backup_copy::bbsink_copystream_new(mcx, opt.send_to_client);
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
                sendFile(sink, state, XLOG_CONTROL_FILE, XLOG_CONTROL_FILE, &statbuf, false, &mut manifest)?;
            } else {
                let archive_name = format!("{oid}.tar");
                bbsink_begin_archive(sink, state, &archive_name)?;
                sendTablespace(sink, state, path.as_deref().unwrap(), oid, &mut manifest)?;
            }

            // Terminate the tarfile (includewal is refused, so always here).
            zero_buffer(sink, 2 * TAR_BLOCK_SIZE);
            bbsink_archive_contents(sink, state, 2 * TAR_BLOCK_SIZE)?;
            bbsink_end_archive(sink, state)?;
            state.tablespace_num += 1;
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

    sink_support::basebackup_progress_done();
    Ok(())
}

fn zero_buffer(sink: &mut Bbsink<'_>, len: usize) {
    sink.buffer_slice_mut(len).fill(0);
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
    let _ = spcoid;
    let mut size: i64 = 0;

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
            let sent = sendFile(sink, state, &pathbuf, tarfilename, &statbuf, true, manifest)?;
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

    let mut bytes_done: i64 = 0;
    loop {
        if bytes_done >= statbuf.size {
            break;
        }
        let want = sink.buffer_length().min((statbuf.size - bytes_done) as usize);
        // SAFETY: buf is a live writable slice; fd is an open regular file.
        let cnt = {
            let buf = sink.buffer_slice_mut(want);
            unsafe { libc::pread(fd, buf.as_mut_ptr().cast(), buf.len(), bytes_done as libc::off_t) }
        };
        if cnt < 0 {
            fd::CloseTransientFile(fd);
            return ereport(ERROR).errcode_for_file_access()
                .errmsg(format!("could not read file \"{readfilename}\""))
                .finish(loc("sendFile")).map(|()| false);
        }
        if cnt == 0 {
            break; // concurrent truncation
        }
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

    AddFileToBackupManifest(manifest, INVALID_OID, tarfilename.as_bytes(), statbuf.size, statbuf.mtime, &mut ctx)?;
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

// File-permission / identity globals (basebackup uses these for injected files).
fn pg_file_create_mode() -> u32 {
    init_small::globals::data_directory_mode() as u32 & 0o666
}
fn pg_dir_create_mode() -> u32 {
    init_small::globals::data_directory_mode() as u32
}
fn geteuid() -> u32 {
    // SAFETY: geteuid never fails.
    unsafe { libc::geteuid() }
}
fn getegid() -> u32 {
    // SAFETY: getegid never fails.
    unsafe { libc::getegid() }
}
fn time_now() -> i64 {
    // SAFETY: time(NULL) returns the current unix time.
    unsafe { libc::time(std::ptr::null_mut()) as i64 }
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
