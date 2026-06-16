//! Port of `src/backend/backup/basebackup.c` (PostgreSQL 18.3): the base-backup
//! driver. This is the orchestration layer that ties the whole backup cluster
//! together — it assembles the `bbsink` chain (copy/server target + optional
//! throttle + optional compression + progress), drives `do_pg_backup_start` /
//! `do_pg_backup_stop`, walks the data directory and every auxiliary tablespace
//! streaming each file as a tar archive (with page-checksum verification and
//! the incremental-backup block selection), injects `backup_label`,
//! `tablespace_map`, the WAL files (when `WAL` is requested), and the
//! `pg_control` file, and finally emits the backup manifest.
//!
//! ## Model
//!
//! The C `bbsink` carries a shared `bbs_state` back-pointer; this repo's
//! `backend-backup-sink` threads `&mut BbsinkState` explicitly through every
//! dispatch call instead, so the driver here keeps a single owned
//! [`BbsinkState`] and passes it to each `bbsink_*` call.
//!
//! ## Cross-crate calls
//!
//! Directly called landed owners: the `bbsink_*` chain + leaf sinks
//! (`backend-backup-sink` / `-copy` / `-server` target / `-throttle` / gzip /
//! lz4 / zstd / `-sink-support` progress), the manifest
//! (`backend-backup-manifest`), the incremental engine
//! (`backend-backup-incremental`), the WAL-filename helpers + `CheckXLogRemoved`
//! (`backend-access-transam-xlog`), `build_backup_content`
//! (`backend-access-transam-xlogbackup`), the page checksum
//! (`backend-storage-page` / `-page-checksum`),
//! `parse_filename_for_nontemp_relation` (`backend-storage-file-reinit`),
//! `looks_like_temp_rel_name` (`backend-storage-file-fd`), `defGet*`
//! (`backend-commands-define`), and the checksum helper
//! (`common-checksum-helper`).
//!
//! Seamed (owner unported): the file/dir primitives (`open_transient_file`,
//! `pg_pread`, `read_dir_names` in `backend-storage-file-fd-seams`); `lstat` /
//! `readlink` / `geteuid` / `getegid` / `time` / file-perm globals + the tar
//! header writer + the base-backup `do_pg_backup_start` (with tablespaces) +
//! `do_pg_abort_backup` (`backend-backup-basebackup-seams`); the session /
//! recovery / checksum-enabled flags + `get_backup_status` +
//! `wal_segment_size` + `GetSystemIdentifier` (`backend-access-transam-xlog-seams`);
//! the progress wrappers (`backend-backup-sink-support`); `WalSndSetState`
//! (`backend-replication-walsender-seams`); `set_ps_display` /
//! `update_process_title` (`backend-utils-misc-ps-status-seams`); `parse_bool`
//! (`backend-utils-adt-scalar-seams`); `summarize_wal`
//! (`backend-postmaster-walsummarizer-seams`); the compression-spec parser
//! (`common-compression-seams`).
//!
//! `SendBaseBackup` is exposed inward to walsender via
//! `backend-replication-basebackup-seams::send_base_backup` (installed in
//! [`init_seams`]).

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::Mcx;

use backend_utils_error::ereport;
use types_core::primitive::{
    BlockNumber, ForkNumber, Oid, RelFileNumber, Size, TimeLineID, XLogRecPtr, BLCKSZ,
};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR,
    ERROR, WARNING,
};

use backend_backup_sink::{
    bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup, bbsink_cleanup,
    bbsink_end_archive, bbsink_end_backup, Bbsink, BbsinkState, TablespaceInfo,
};
use backend_backup_manifest::{
    AddFileToBackupManifest, AddWALInfoToBackupManifest, BackupManifestInfo, BackupManifestOption,
    FreeBackupManifest, InitializeBackupManifest, SendBackupManifest, MANIFEST_OPTION_FORCE_ENCODE,
    MANIFEST_OPTION_NO, MANIFEST_OPTION_YES,
};
use backend_backup_incremental::{
    FileBackupMethod, GetIncrementalFileSize, IncrementalBackupInfo, INCREMENTAL_MAGIC,
};

use backend_access_transam_xlog::{
    CheckXLogRemoved, IsTLHistoryFileName, IsXLogFileName, StatusFilePath, XLByteToPrevSeg,
    XLByteToSeg, XLogFileName, XLogFromFileName,
};
use backend_access_transam_xlogbackup::build_backup_content_default;
use backend_storage_file_reinit::parse_filename_for_nontemp_relation;
use backend_storage_file_fd::sync_cleanup::looks_like_temp_rel_name;
use backend_storage_page::{PageGetLSN, PageIsNew, PageRef};
use backend_storage_page_checksum::pg_checksum_page;
use backend_commands_define::{defGetBoolean, defGetInt64, defGetString};
use common_checksum_helper::{
    pg_checksum_init, pg_checksum_parse_type, pg_checksum_type, pg_checksum_update,
    PgChecksumContext, CHECKSUM_TYPE_CRC32C, CHECKSUM_TYPE_NONE,
};
use types_compression::{PgCompressAlgorithm, PgCompressSpecification};
use types_replication::replnodes::BaseBackupCmd;
use types_replication::walsender::WalSndState;

use backend_backup_basebackup_seams as bbseam;
use backend_backup_sink_support as progress;
use backend_access_transam_xlog_seams as xlog;
use backend_replication_walsender_seams as ws;
use backend_utils_misc_ps_status_seams as ps_status;
use backend_utils_adt_scalar_seams as scalar;
use backend_postmaster_walsummarizer_seams as walsummarizer;
use common_compression_seams as compression;
use backend_utils_activity_pgstat_seams as pgstat;

/// The C source path used in `ereport` error locations (`__FILE__`).
const SRCFILE: &str = "src/backend/backup/basebackup.c";

fn here(func: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRCFILE, 0, func)
}

// ---------------------------------------------------------------------------
// Constants (basebackup.c / basebackup.h / the file-name macros).
// ---------------------------------------------------------------------------

/// `SINK_BUFFER_LENGTH` = `Max(32768, BLCKSZ)`.
const SINK_BUFFER_LENGTH: Size = if 32768 > BLCKSZ { 32768 } else { BLCKSZ };

/// `TAR_BLOCK_SIZE` (pgtar.h:17).
const TAR_BLOCK_SIZE: usize = 512;

/// `MAXFNAMELEN` is unused here; WAL names are produced by `XLogFileName`.

/// `RELSEG_SIZE` (relseg size in blocks).
const RELSEG_SIZE: u64 = types_storage::smgr::RELSEG_SIZE as u64;

/// `INIT_FORKNUM` / `InvalidForkNumber` (relpath.h).
const INIT_FORKNUM: ForkNumber = types_core::primitive::INIT_FORKNUM;
const INVALID_FORKNUM: ForkNumber = types_core::primitive::InvalidForkNumber;

/// `InvalidOid`.
const INVALID_OID: Oid = types_core::primitive::InvalidOid;
/// `InvalidRelFileNumber`.
const INVALID_REL_FILE_NUMBER: RelFileNumber = types_core::primitive::InvalidOid;

/// Tablespace OIDs (pg_tablespace_d.h).
const GLOBALTABLESPACE_OID: Oid = 1664;
const DEFAULTTABLESPACE_OID: Oid = 1663;

/// `MAX_RATE_LOWER` / `MAX_RATE_UPPER` (basebackup.h).
const MAX_RATE_LOWER: i64 = 32;
const MAX_RATE_UPPER: i64 = 1_048_576;

/// `PG_TEMP_FILE_PREFIX` (storage/fd.h).
const PG_TEMP_FILE_PREFIX: &str = "pgsql_tmp";

/// `BACKUP_LABEL_FILE` (xlog.h).
const BACKUP_LABEL_FILE: &str = "backup_label";
/// `TABLESPACE_MAP` (xlog.h).
const TABLESPACE_MAP: &str = "tablespace_map";
/// `XLOG_CONTROL_FILE` (xlog_internal.h).
const XLOG_CONTROL_FILE: &str = "global/pg_control";
/// `TABLESPACE_VERSION_DIRECTORY` (catalog/catalog.h).
const TABLESPACE_VERSION_DIRECTORY: &str = types_storage::file::TABLESPACE_VERSION_DIRECTORY;
/// `PG_TBLSPC_DIR`.
const PG_TBLSPC_DIR: &str = "pg_tblspc";
/// `XLOGDIR` is `pg_wal`; paths are assembled with the leading `pg_wal/`.

/// stat-mode helpers (`sys/stat.h`).
const S_IFMT: u32 = 0o170000;
const S_IFDIR: u32 = 0o040000;
const S_IFREG: u32 = 0o100000;
const S_IFLNK: u32 = 0o120000;

#[inline]
fn S_ISDIR(m: u32) -> bool {
    (m & S_IFMT) == S_IFDIR
}
#[inline]
fn S_ISREG(m: u32) -> bool {
    (m & S_IFMT) == S_IFREG
}
#[inline]
fn S_ISLNK(m: u32) -> bool {
    (m & S_IFMT) == S_IFLNK
}

#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != INVALID_OID
}
#[inline]
fn RelFileNumberIsValid(n: RelFileNumber) -> bool {
    n != INVALID_REL_FILE_NUMBER
}

/// O_RDONLY | PG_BINARY for `OpenTransientFile`.
const O_RDONLY: i32 = 0;
#[cfg(windows)]
const PG_BINARY: i32 = 0o100000; // _O_BINARY; not used on Unix builds.
#[cfg(not(windows))]
const PG_BINARY: i32 = 0;
const OPEN_FLAGS: i32 = O_RDONLY | PG_BINARY;

/// ENOENT errno (used by the seam contract via `Ok(None)`).
const _ENOENT: i32 = 2;

// ---------------------------------------------------------------------------
// basebackup_options (the parsed BASE_BACKUP options).
// ---------------------------------------------------------------------------

/// C `typedef struct { ... } basebackup_options`.
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
    #[allow(dead_code)]
    use_copytblspc: bool,
    target_handle: Option<backend_backup_basebackup_target::BaseBackupTargetHandle>,
    manifest: BackupManifestOption,
    compression: PgCompressAlgorithm,
    compression_specification: PgCompressSpecification,
    manifest_checksum_type: pg_checksum_type,
}

impl Default for BasebackupOptions {
    fn default() -> Self {
        // C `MemSet(opt, 0, sizeof(*opt))` then explicit overrides.
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
            use_copytblspc: false,
            target_handle: None,
            manifest: MANIFEST_OPTION_NO,
            compression: PgCompressAlgorithm::None,
            compression_specification: PgCompressSpecification {
                algorithm: PgCompressAlgorithm::None,
                options: 0,
                level: 0,
                workers: 0,
                long_distance: false,
                parse_error: None,
            },
            manifest_checksum_type: CHECKSUM_TYPE_CRC32C,
        }
    }
}

// Backend-local state that mirrors basebackup.c's file-static variables.
// PostgreSQL is single-threaded per backend; these live for the life of one
// `perform_base_backup`.
thread_local! {
    /// `static bool backup_started_in_recovery`.
    static BACKUP_STARTED_IN_RECOVERY: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
    /// `static long long int total_checksum_failures`.
    static TOTAL_CHECKSUM_FAILURES: core::cell::Cell<i64> = const { core::cell::Cell::new(0) };
    /// `static bool noverify_checksums`.
    static NOVERIFY_CHECKSUMS: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

// ---------------------------------------------------------------------------
// exclude lists (basebackup.c:151-225).
// ---------------------------------------------------------------------------

/// `excludeDirContents[]` — directories whose contents are excluded but the
/// (empty) directory itself is kept.
const EXCLUDE_DIR_CONTENTS: &[&str] = &[
    // PG_STAT_TMP_DIR
    "pg_stat_tmp",
    // PG_REPLSLOT_DIR
    "pg_replslot",
    // PG_DYNSHMEM_DIR
    "pg_dynshmem",
    "pg_notify",
    "pg_serial",
    "pg_snapshots",
    "pg_subtrans",
];

/// One element of the file-exclusion list (`struct exclude_list_item`).
struct ExcludeListItem {
    name: &'static str,
    match_prefix: bool,
}

/// `excludeFiles[]` — files excluded from backups.
const EXCLUDE_FILES: &[ExcludeListItem] = &[
    // PG_AUTOCONF_FILENAME ".tmp"
    ExcludeListItem {
        name: "postgresql.auto.conf.tmp",
        match_prefix: false,
    },
    // LOG_METAINFO_DATAFILE_TMP
    ExcludeListItem {
        name: "current_logfiles.tmp",
        match_prefix: false,
    },
    // RELCACHE_INIT_FILENAME
    ExcludeListItem {
        name: "pg_internal.init",
        match_prefix: true,
    },
    ExcludeListItem {
        name: BACKUP_LABEL_FILE,
        match_prefix: false,
    },
    ExcludeListItem {
        name: TABLESPACE_MAP,
        match_prefix: false,
    },
    ExcludeListItem {
        name: "backup_manifest",
        match_prefix: false,
    },
    ExcludeListItem {
        name: "postmaster.pid",
        match_prefix: false,
    },
    ExcludeListItem {
        name: "postmaster.opts",
        match_prefix: false,
    },
];

// ===========================================================================
// SendBaseBackup() — the BASE_BACKUP entry point (basebackup.c:989).
// ===========================================================================

/// `void SendBaseBackup(BaseBackupCmd *cmd, IncrementalBackupInfo *ib)`.
///
/// The `ib` argument originates from walsender's `UPLOAD_MANIFEST` command
/// (the file-static `uploaded_manifest`). That command is not yet ported (it
/// panics in walsender), so the reachable path always passes `ib = None`: a
/// plain `BASE_BACKUP` works fully, and an incremental `BASE_BACKUP` issued
/// without a prior `UPLOAD_MANIFEST` correctly raises the "must UPLOAD_MANIFEST
/// first" error. When `UPLOAD_MANIFEST` lands it will thread the real `ib`.
pub fn SendBaseBackup<'mcx>(
    mcx: Mcx<'mcx>,
    cmd: &BaseBackupCmd,
    mut ib: Option<&mut IncrementalBackupInfo<'mcx>>,
) -> PgResult<()> {
    let status = xlog::get_backup_status::call();

    if status == types_wal::SessionBackupState::Running {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("a backup is already in progress in this session")
            .into_error().with_error_location(here("SendBaseBackup")));
    }

    let mut opt = parse_basebackup_options(mcx, &cmd.options)?;

    ws::wal_snd_set_state::call(WalSndState::WALSNDSTATE_BACKUP);

    if ps_status::update_process_title::call() {
        // snprintf(activitymsg, sizeof(activitymsg), "sending backup \"%s\"", opt.label)
        let mut activitymsg = format!("sending backup \"{}\"", opt.label);
        // C truncates to a 50-byte buffer (49 chars + NUL).
        if activitymsg.len() > 49 {
            activitymsg.truncate(truncate_char_boundary(&activitymsg, 49));
        }
        ps_status::set_ps_display::call(activitymsg);
    }

    // If we're asked to perform an incremental backup and the user has not
    // supplied a manifest, that's an ERROR. If we're asked to perform a full
    // backup and the user did supply a manifest, just ignore it.
    if !opt.incremental {
        ib = None;
    } else if ib.is_none() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("must UPLOAD_MANIFEST before performing an incremental BASE_BACKUP")
            .into_error().with_error_location(here("SendBaseBackup")));
    }

    // If the target is specifically 'client' then set up to stream the backup
    // to the client; otherwise, it's being sent someplace else and should not
    // be sent to the client.
    let mut sink: Box<Bbsink<'mcx>> = backend_backup_copy::bbsink_copystream_new(mcx, opt.send_to_client);
    if let Some(handle) = opt.target_handle.take() {
        sink = backend_backup_basebackup_target::BaseBackupGetSink(mcx, handle, sink)?;
    }

    // Set up network throttling, if client requested it.
    if opt.maxrate > 0 {
        sink = backend_backup_throttle::bbsink_throttle_new(mcx, sink, opt.maxrate);
    }

    // Set up server-side compression, if client requested it.
    match opt.compression {
        PgCompressAlgorithm::Gzip => {
            sink = backend_backup_gzip::bbsink_gzip_new(mcx, sink, &opt.compression_specification);
        }
        PgCompressAlgorithm::Lz4 => {
            sink = backend_backup_lz4::bbsink_lz4_new(mcx, sink, &opt.compression_specification);
        }
        PgCompressAlgorithm::Zstd => {
            sink = backend_backup_zstd::bbsink_zstd_new(mcx, sink, &opt.compression_specification);
        }
        PgCompressAlgorithm::None => {}
    }

    // Set up progress reporting.
    sink = progress::bbsink_progress_new(mcx, sink, opt.progress);

    // Perform the base backup, but make sure we clean up the bbsink even if an
    // error occurs (the C PG_TRY/PG_FINALLY(bbsink_cleanup)).
    let mut state = BbsinkState::default();
    let result = perform_base_backup(mcx, &opt, &mut sink, &mut state, ib.as_deref_mut());

    // PG_FINALLY: always clean up the sink. The cleanup itself can fail; in C
    // an error inside PG_FINALLY would re-raise. We propagate the primary error
    // first, otherwise the cleanup error.
    let cleanup = bbsink_cleanup(&mut sink, &mut state);
    result?;
    cleanup
}

/// Truncate to the largest char boundary `<= max`.
fn truncate_char_boundary(s: &str, max: usize) -> usize {
    let mut idx = max.min(s.len());
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}

// ===========================================================================
// perform_base_backup() (basebackup.c:233).
// ===========================================================================

fn perform_base_backup<'mcx>(
    mcx: Mcx<'mcx>,
    opt: &BasebackupOptions,
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    mut ib: Option<&mut IncrementalBackupInfo<'mcx>>,
) -> PgResult<()> {
    // Initial backup state, insofar as we know it now.
    state.tablespaces = Vec::new();
    state.tablespace_num = 0;
    state.bytes_done = 0;
    state.bytes_total = 0;
    state.bytes_total_is_valid = false;

    // (The C ResourceOwner juggling for the manifest BufFile is handled inside
    // the manifest crate, which owns a real BufFile; nothing to do here.)

    BACKUP_STARTED_IN_RECOVERY.with(|c| c.set(xlog::recovery_in_progress::call()));

    let mut manifest = BackupManifestInfo::zeroed();
    InitializeBackupManifest(mcx, &mut manifest, opt.manifest, opt.manifest_checksum_type)?;

    TOTAL_CHECKSUM_FAILURES.with(|c| c.set(0));

    // Allocate backup related variables.
    progress::basebackup_progress_wait_checkpoint();
    let start = bbseam::do_pg_backup_start_for_basebackup::call(&opt.label, opt.fastcheckpoint)?;
    let mut backup_state = start.state;
    let tablespace_map = start.tablespace_map;
    state.tablespaces = start.tablespaces;

    state.startptr = backup_state.startpoint();
    state.starttli = backup_state.starttli();

    // Once do_pg_backup_start has been called, ensure that any failure causes
    // us to abort the backup so we don't "leak" a backup counter. The whole
    // start..stop region runs inside `run_with_abort_cleanup`, the analog of
    // PG_ENSURE_ERROR_CLEANUP(do_pg_abort_backup, BoolGetDatum(false)).
    let mut endptr: XLogRecPtr = 0;
    let mut endtli: TimeLineID = 0;

    let start_to_stop = run_with_abort_cleanup(|| {
        // If this is an incremental backup, execute preparatory steps.
        if let Some(ib) = ib.as_deref_mut() {
            ib.prepare_for_incremental_backup(&mut backup_state)?;
        }

        // Add a node for the base directory at the end.
        state.tablespaces.push(TablespaceInfo {
            oid: INVALID_OID,
            path: None,
            rpath: None,
            size: Some(-1),
        });

        // Calculate the total backup size by summing up the size of each
        // tablespace.
        if opt.progress {
            progress::basebackup_progress_estimate_backup_size();

            let mut total: u64 = 0;
            let n = state.tablespaces.len();
            for i in 0..n {
                let (is_pgdata, path, oid) = {
                    let ti = &state.tablespaces[i];
                    (ti.path.is_none(), ti.path.clone(), ti.oid)
                };
                let size = if is_pgdata {
                    sendDir(mcx, sink, state, ".", 1, true, true, None, INVALID_OID, None)?
                } else {
                    sendTablespace(mcx, sink, state, path.as_deref().unwrap(), oid, true, None, None)?
                };
                state.tablespaces[i].size = Some(size);
                total = total.wrapping_add(size as u64);
            }
            state.bytes_total = total;
            state.bytes_total_is_valid = true;
        }

        // Notify basebackup sink about start of backup.
        bbsink_begin_backup(sink, state, SINK_BUFFER_LENGTH)?;

        // Send off our tablespaces one by one.
        let n = state.tablespaces.len();
        for i in 0..n {
            let (is_pgdata, path, oid) = {
                let ti = &state.tablespaces[i];
                (ti.path.is_none(), ti.path.clone(), ti.oid)
            };

            if is_pgdata {
                let mut sendtblspclinks = true;

                bbsink_begin_archive(sink, state, "base.tar")?;

                // In the main tar, include the backup_label first...
                let backup_label = build_backup_content_default(&backup_state, false)?;
                sendFileWithContent(mcx, sink, state, BACKUP_LABEL_FILE, &backup_label, &mut manifest)?;

                // Then the tablespace_map file, if required...
                if opt.sendtblspcmapfile {
                    sendFileWithContent(mcx, sink, state, TABLESPACE_MAP, &tablespace_map, &mut manifest)?;
                    sendtblspclinks = false;
                }

                // Then the bulk of the files...
                sendDir(
                    mcx, sink, state, ".", 1, false, sendtblspclinks, Some(&mut manifest),
                    INVALID_OID, ib.as_deref_mut(),
                )?;

                // ... and pg_control after everything else.
                let statbuf = match bbseam::lstat_file::call(XLOG_CONTROL_FILE)? {
                    Some(s) => s,
                    None => {
                        return Err(ereport(ERROR)
                            .errcode_for_file_access()
                            .errmsg(format!("could not stat file \"{XLOG_CONTROL_FILE}\""))
                            .into_error().with_error_location(here("perform_base_backup")));
                    }
                };
                sendFile(
                    mcx, sink, state, XLOG_CONTROL_FILE, XLOG_CONTROL_FILE, &statbuf, false,
                    INVALID_OID, INVALID_OID, INVALID_REL_FILE_NUMBER, 0, &mut manifest, 0, None, 0,
                )?;
            } else {
                let archive_name = format!("{oid}.tar");
                bbsink_begin_archive(sink, state, &archive_name)?;
                sendTablespace(
                    mcx, sink, state, path.as_deref().unwrap(), oid, false, Some(&mut manifest),
                    ib.as_deref_mut(),
                )?;
            }

            // If we're including WAL, and this is the main data directory, we
            // don't treat this as the end of the tablespace. Instead, we will
            // include the xlog files below and stop afterwards.
            if opt.includewal && is_pgdata {
                debug_assert_eq!(i, n - 1, "main data directory must be sent last");
            } else {
                // Properly terminate the tarfile.
                zero_buffer(sink, 2 * TAR_BLOCK_SIZE);
                bbsink_archive_contents(sink, state, 2 * TAR_BLOCK_SIZE)?;
                bbsink_end_archive(sink, state)?;
            }
        }

        progress::basebackup_progress_wait_wal_archive(state);
        backup_state = xlog::do_pg_backup_stop::call(backup_state, !opt.nowait)?;

        endptr = backup_state.stoppoint();
        endtli = backup_state.stoptli();

        Ok(())
    });

    // PG_END_ENSURE_ERROR_CLEANUP runs the cleanup on error; propagate.
    start_to_stop?;

    if opt.includewal {
        // We've left the last tar file "open", so append the WAL files to it.
        send_wal_files(mcx, sink, state, endptr, endtli, &mut manifest)?;
    }

    AddWALInfoToBackupManifest(mcx, &mut manifest, state.startptr, state.starttli, endptr, endtli)?;

    SendBackupManifest(&mut manifest, sink, state)?;

    bbsink_end_backup(sink, state, endptr, endtli)?;

    let total_failures = TOTAL_CHECKSUM_FAILURES.with(core::cell::Cell::get);
    if total_failures != 0 {
        if total_failures > 1 {
            let _ = ereport(WARNING)
                .errmsg(format!(
                    "{total_failures} total checksum verification failures"
                ))
                .finish(here("perform_base_backup"));
        }
        // Free the manifest before raising, mirroring the C ordering note.
        FreeBackupManifest(&mut manifest);
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg("checksum verification failure during base backup")
            .into_error().with_error_location(here("perform_base_backup")));
    }

    // Make sure to free the manifest before the resource owners.
    FreeBackupManifest(&mut manifest);

    // (ReleaseAuxProcessResources(true) is the C resource-owner teardown for the
    // manifest BufFile; the manifest crate owns/drops its BufFile directly.)

    progress::basebackup_progress_done();
    Ok(())
}

/// Mirror of `PG_ENSURE_ERROR_CLEANUP(do_pg_abort_backup, BoolGetDatum(false))`:
/// run `f`, and if it returns `Err`, invoke `do_pg_abort_backup(false)` before
/// propagating. (The C macro also runs the cleanup on a longjmp; here the
/// `PgResult` carries that failure.)
fn run_with_abort_cleanup<F>(f: F) -> PgResult<()>
where
    F: FnOnce() -> PgResult<()>,
{
    match f() {
        Ok(()) => Ok(()),
        Err(e) => {
            // do_pg_abort_backup(code, BoolGetDatum(false)) — emit_warning=false.
            bbseam::do_pg_abort_backup::call(false);
            Err(e)
        }
    }
}

/// Zero the first `len` bytes of the sink's working buffer (C
/// `memset(sink->bbs_buffer, 0, len)`).
fn zero_buffer(sink: &mut Bbsink<'_>, len: usize) {
    let buf = sink.buffer_slice_mut(len);
    buf.fill(0);
}

// ===========================================================================
// The WAL-file appending block (basebackup.c:408-645).
// ===========================================================================

fn send_wal_files<'mcx>(
    mcx: Mcx<'mcx>,
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    endptr: XLogRecPtr,
    endtli: TimeLineID,
    manifest: &mut BackupManifestInfo,
) -> PgResult<()> {
    progress::basebackup_progress_transfer_wal();

    let wal_segment_size = xlog::wal_segment_size::call();

    // I'd rather not worry about timelines here, so scan pg_wal and include all
    // WAL files in the range between 'startptr' and 'endptr'.
    let startsegno = XLByteToSeg(state.startptr, wal_segment_size);
    let firstoff = XLogFileName(state.starttli, startsegno, wal_segment_size);
    let endsegno = XLByteToPrevSeg(endptr, wal_segment_size);
    let lastoff = XLogFileName(endtli, endsegno, wal_segment_size);

    let mut wal_file_list: Vec<String> = Vec::new();
    let mut history_file_list: Vec<String> = Vec::new();

    // `firstoff + 8` / `lastoff + 8`: the log/seg portion (skip the 8-char TLI).
    let firstoff_tail = &firstoff[8..];
    let lastoff_tail = &lastoff[8..];

    for de in backend_storage_file_fd_seams::read_dir_names::call("pg_wal")? {
        if IsXLogFileName(&de) && &de[8..] >= firstoff_tail && &de[8..] <= lastoff_tail {
            wal_file_list.push(de);
        } else if IsTLHistoryFileName(&de) {
            history_file_list.push(de);
        }
    }

    // Before we go any further, check that none of the WAL segments we need
    // were removed.
    CheckXLogRemoved(startsegno, state.starttli);

    // Sort the WAL filenames (oldest to newest), comparing the log/seg portion.
    wal_file_list.sort_by(|a, b| a[8..].cmp(&b[8..]));

    if wal_file_list.is_empty() {
        return Err(ereport(ERROR)
            .errmsg("could not find any WAL files")
            .into_error().with_error_location(here("send_wal_files")));
    }

    // Sanity check: the first and last segment should cover startptr and endptr,
    // with no gaps.
    let (_tli0, mut segno) = XLogFromFileName(&wal_file_list[0], wal_segment_size)?;
    if segno != startsegno {
        let startfname = XLogFileName(state.starttli, startsegno, wal_segment_size);
        return Err(ereport(ERROR)
            .errmsg(format!("could not find WAL file \"{startfname}\""))
            .into_error().with_error_location(here("send_wal_files")));
    }
    for wal_file_name in &wal_file_list {
        let currsegno = segno;
        let nextsegno = segno + 1;
        let (tli, seg) = XLogFromFileName(wal_file_name, wal_segment_size)?;
        segno = seg;
        if !(nextsegno == segno || currsegno == segno) {
            let nextfname = XLogFileName(tli, nextsegno, wal_segment_size);
            return Err(ereport(ERROR)
                .errmsg(format!("could not find WAL file \"{nextfname}\""))
                .into_error().with_error_location(here("send_wal_files")));
        }
    }
    if segno != endsegno {
        let endfname = XLogFileName(endtli, endsegno, wal_segment_size);
        return Err(ereport(ERROR)
            .errmsg(format!("could not find WAL file \"{endfname}\""))
            .into_error().with_error_location(here("send_wal_files")));
    }

    // Ok, we have everything we need. Send the WAL files.
    for wal_file_name in &wal_file_list {
        let pathbuf = format!("pg_wal/{wal_file_name}");
        let (tli, segno) = XLogFromFileName(wal_file_name, wal_segment_size)?;

        let fd = backend_storage_file_fd_seams::open_transient_file::call(&pathbuf, OPEN_FLAGS);
        if fd < 0 {
            // Most likely the file was removed by a checkpoint.
            CheckXLogRemoved(segno, tli);
            return Err(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{pathbuf}\""))
                .into_error().with_error_location(here("send_wal_files")));
        }

        // fstat -> lstat (the file is open; basebackup uses fstat, but the path
        // resolves to the same regular file). Use lstat on the path.
        let statbuf = match bbseam::lstat_file::call(&pathbuf)? {
            Some(s) => s,
            None => {
                backend_storage_file_fd_seams::close_transient_file::call(fd);
                return Err(ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not stat file \"{pathbuf}\""))
                    .into_error().with_error_location(here("send_wal_files")));
            }
        };
        if statbuf.size != wal_segment_size as i64 {
            backend_storage_file_fd_seams::close_transient_file::call(fd);
            CheckXLogRemoved(segno, tli);
            return Err(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("unexpected WAL file size \"{wal_file_name}\""))
                .into_error().with_error_location(here("send_wal_files")));
        }

        // Send the WAL file itself.
        _tarWriteHeader(mcx, sink, state, &pathbuf, None, &statbuf, false)?;

        let mut len: i64 = 0;
        loop {
            let want = core::cmp::min(sink.buffer_length() as i64, wal_segment_size as i64 - len);
            if want <= 0 {
                break;
            }
            let cnt = basebackup_read_file(sink, fd, 0, want as usize, len, &pathbuf, true)?;
            if cnt == 0 {
                break;
            }
            CheckXLogRemoved(segno, tli);
            bbsink_archive_contents(sink, state, cnt as usize)?;
            len += cnt as i64;
            if len == wal_segment_size as i64 {
                break;
            }
        }

        if len != wal_segment_size as i64 {
            backend_storage_file_fd_seams::close_transient_file::call(fd);
            CheckXLogRemoved(segno, tli);
            return Err(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("unexpected WAL file size \"{wal_file_name}\""))
                .into_error().with_error_location(here("send_wal_files")));
        }

        // wal_segment_size is a multiple of TAR_BLOCK_SIZE, so no padding needed.
        backend_storage_file_fd_seams::close_transient_file::call(fd);

        // Mark file as archived (otherwise files can get archived again after
        // promotion of a new node).
        let donepath = StatusFilePath(wal_file_name, ".done");
        sendFileWithContent(mcx, sink, state, &donepath, b"", manifest)?;
    }

    // Send timeline history files too.
    for fname in &history_file_list {
        let pathbuf = format!("pg_wal/{fname}");

        let statbuf = match bbseam::lstat_file::call(&pathbuf)? {
            Some(s) => s,
            None => {
                return Err(ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not stat file \"{pathbuf}\""))
                    .into_error().with_error_location(here("send_wal_files")));
            }
        };

        sendFile(
            mcx, sink, state, &pathbuf, &pathbuf, &statbuf, false, INVALID_OID, INVALID_OID,
            INVALID_REL_FILE_NUMBER, 0, manifest, 0, None, 0,
        )?;

        // Unconditionally mark file as archived.
        let donepath = StatusFilePath(fname, ".done");
        sendFileWithContent(mcx, sink, state, &donepath, b"", manifest)?;
    }

    // Properly terminate the tar file.
    zero_buffer(sink, 2 * TAR_BLOCK_SIZE);
    bbsink_archive_contents(sink, state, 2 * TAR_BLOCK_SIZE)?;
    bbsink_end_archive(sink, state)?;

    Ok(())
}

// ===========================================================================
// parse_basebackup_options() (basebackup.c:697).
// ===========================================================================

fn parse_basebackup_options<'mcx>(
    mcx: Mcx<'mcx>,
    options: &[types_parsenodes::DefElem],
) -> PgResult<BasebackupOptions> {
    let mut opt = BasebackupOptions::default();

    let mut o_label = false;
    let mut o_progress = false;
    let mut o_checkpoint = false;
    let mut o_nowait = false;
    let mut o_wal = false;
    let mut o_incremental = false;
    let mut o_maxrate = false;
    let mut o_tablespace_map = false;
    let mut o_noverify_checksums = false;
    let mut o_manifest = false;
    let mut o_manifest_checksums = false;
    let mut o_target = false;
    let mut o_target_detail = false;
    let mut target_str: Option<String> = None;
    let mut target_detail_str: Option<String> = None;
    let mut o_compression = false;
    let mut o_compression_detail = false;
    let mut compression_detail_str: Option<String> = None;

    let dup = |defname: &str, func: &'static str| -> types_error::PgError {
        ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("duplicate option \"{defname}\""))
            .into_error().with_error_location(here(func))
    };

    for defel in options {
        let defname = defel.defname.as_deref().unwrap_or("");

        if defname == "label" {
            if o_label {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            opt.label = defGetString(mcx, defel)?.to_string();
            o_label = true;
        } else if defname == "progress" {
            if o_progress {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            opt.progress = defGetBoolean(defel)?;
            o_progress = true;
        } else if defname == "checkpoint" {
            let optval = defGetString(mcx, defel)?;
            if o_checkpoint {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            if pg_strcasecmp(&optval, "fast") == 0 {
                opt.fastcheckpoint = true;
            } else if pg_strcasecmp(&optval, "spread") == 0 {
                opt.fastcheckpoint = false;
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("unrecognized checkpoint type: \"{optval}\""))
                    .into_error().with_error_location(here("parse_basebackup_options")));
            }
            o_checkpoint = true;
        } else if defname == "wait" {
            if o_nowait {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            opt.nowait = !defGetBoolean(defel)?;
            o_nowait = true;
        } else if defname == "wal" {
            if o_wal {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            opt.includewal = defGetBoolean(defel)?;
            o_wal = true;
        } else if defname == "incremental" {
            if o_incremental {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            opt.incremental = defGetBoolean(defel)?;
            if opt.incremental && !walsummarizer::summarize_wal::call() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg("incremental backups cannot be taken unless WAL summarization is enabled")
                    .into_error().with_error_location(here("parse_basebackup_options")));
            }
            o_incremental = true;
        } else if defname == "max_rate" {
            if o_maxrate {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            let maxrate = defGetInt64(defel)?;
            if maxrate < MAX_RATE_LOWER || maxrate > MAX_RATE_UPPER {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                    .errmsg(format!(
                        "{} is outside the valid range for parameter \"{}\" ({} .. {})",
                        maxrate as i32, "MAX_RATE", MAX_RATE_LOWER, MAX_RATE_UPPER
                    ))
                    .into_error().with_error_location(here("parse_basebackup_options")));
            }
            opt.maxrate = maxrate as u32;
            o_maxrate = true;
        } else if defname == "tablespace_map" {
            if o_tablespace_map {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            opt.sendtblspcmapfile = defGetBoolean(defel)?;
            o_tablespace_map = true;
        } else if defname == "verify_checksums" {
            if o_noverify_checksums {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            // noverify_checksums = !defGetBoolean(defel)
            let v = defGetBoolean(defel)?;
            NOVERIFY_CHECKSUMS.with(|c| c.set(!v));
            o_noverify_checksums = true;
        } else if defname == "manifest" {
            let optval = defGetString(mcx, defel)?.to_string();
            if o_manifest {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            if let Some(manifest_bool) = scalar::parse_bool::call(&optval) {
                opt.manifest = if manifest_bool {
                    MANIFEST_OPTION_YES
                } else {
                    MANIFEST_OPTION_NO
                };
            } else if pg_strcasecmp(&optval, "force-encode") == 0 {
                opt.manifest = MANIFEST_OPTION_FORCE_ENCODE;
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("unrecognized manifest option: \"{optval}\""))
                    .into_error().with_error_location(here("parse_basebackup_options")));
            }
            o_manifest = true;
        } else if defname == "manifest_checksums" {
            let optval = defGetString(mcx, defel)?.to_string();
            if o_manifest_checksums {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            match pg_checksum_parse_type(optval.as_bytes()) {
                Some(t) => opt.manifest_checksum_type = t,
                None => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!("unrecognized checksum algorithm: \"{optval}\""))
                        .into_error().with_error_location(here("parse_basebackup_options")));
                }
            }
            o_manifest_checksums = true;
        } else if defname == "target" {
            if o_target {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            target_str = Some(defGetString(mcx, defel)?.to_string());
            o_target = true;
        } else if defname == "target_detail" {
            let optval = defGetString(mcx, defel)?.to_string();
            if o_target_detail {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            target_detail_str = Some(optval);
            o_target_detail = true;
        } else if defname == "compression" {
            let optval = defGetString(mcx, defel)?.to_string();
            if o_compression {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            match compression::parse_compress_algorithm::call(&optval) {
                Some(alg) => opt.compression = alg,
                None => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!("unrecognized compression algorithm: \"{optval}\""))
                        .into_error().with_error_location(here("parse_basebackup_options")));
                }
            }
            o_compression = true;
        } else if defname == "compression_detail" {
            if o_compression_detail {
                return Err(dup(defname, "parse_basebackup_options"));
            }
            compression_detail_str = Some(defGetString(mcx, defel)?.to_string());
            o_compression_detail = true;
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized base backup option: \"{defname}\""))
                .into_error().with_error_location(here("parse_basebackup_options")));
        }
    }

    if !o_label {
        opt.label = "base backup".to_string();
    }
    if opt.manifest == MANIFEST_OPTION_NO {
        if o_manifest_checksums {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("manifest checksums require a backup manifest")
                .into_error().with_error_location(here("parse_basebackup_options")));
        }
        opt.manifest_checksum_type = CHECKSUM_TYPE_NONE;
    }

    match target_str.as_deref() {
        None => {
            if target_detail_str.is_some() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("target detail cannot be used without target")
                    .into_error().with_error_location(here("parse_basebackup_options")));
            }
            opt.use_copytblspc = true;
            opt.send_to_client = true;
        }
        Some("client") => {
            if target_detail_str.is_some() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!(
                        "target \"{}\" does not accept a target detail",
                        target_str.as_deref().unwrap()
                    ))
                    .into_error().with_error_location(here("parse_basebackup_options")));
            }
            opt.send_to_client = true;
        }
        Some(t) => {
            opt.target_handle = Some(backend_backup_basebackup_target::BaseBackupGetTargetHandle(
                t,
                target_detail_str.as_deref(),
            )?);
        }
    }

    if o_compression_detail && !o_compression {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("compression detail cannot be specified unless compression is enabled")
            .into_error().with_error_location(here("parse_basebackup_options")));
    }

    if o_compression {
        opt.compression_specification = compression::parse_compress_specification::call(
            opt.compression,
            compression_detail_str.as_deref(),
        );
        if let Some(error_detail) =
            compression::validate_compress_specification::call(&opt.compression_specification)
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("invalid compression specification: {error_detail}"))
                .into_error().with_error_location(here("parse_basebackup_options")));
        }
    }

    Ok(opt)
}

/// `pg_strcasecmp` wrapper returning the C-style `i32` ordering.
fn pg_strcasecmp(a: &str, b: &str) -> i32 {
    port_pgstrcasecmp::pg_strcasecmp(a.as_bytes(), b.as_bytes())
}

// ===========================================================================
// sendFileWithContent() (basebackup.c:1074).
// ===========================================================================

fn sendFileWithContent<'mcx>(
    mcx: Mcx<'mcx>,
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    filename: &str,
    content: &[u8],
    manifest: &mut BackupManifestInfo,
) -> PgResult<()> {
    let mut checksum_ctx = checksum_init(manifest.checksum_type(), filename)?;

    let len = content.len();

    // Construct a stat struct for the file we're injecting in the tar.
    let statbuf = bbseam::LstatInfo {
        size: len as i64,
        mode: bbseam::pg_file_create_mode::call(),
        uid: bbseam::geteuid::call(),
        gid: bbseam::getegid::call(),
        mtime: bbseam::time_now::call(),
    };

    _tarWriteHeader(mcx, sink, state, filename, None, &statbuf, false)?;

    checksum_update(&mut checksum_ctx, content)?;

    let mut bytes_done = 0usize;
    while bytes_done < len {
        let remaining = len - bytes_done;
        let nbytes = core::cmp::min(sink.buffer_length(), remaining);
        let buf = sink.buffer_slice_mut(nbytes);
        buf.copy_from_slice(&content[bytes_done..bytes_done + nbytes]);
        bbsink_archive_contents(sink, state, nbytes)?;
        bytes_done += nbytes;
    }

    _tarWritePadding(sink, state, len)?;

    AddFileToBackupManifest(
        mcx, manifest, INVALID_OID, filename, len, statbuf.mtime, &mut checksum_ctx,
    )
}

// ===========================================================================
// sendTablespace() (basebackup.c:1135).
// ===========================================================================

fn sendTablespace<'mcx>(
    mcx: Mcx<'mcx>,
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    path: &str,
    spcoid: Oid,
    sizeonly: bool,
    mut manifest: Option<&mut BackupManifestInfo>,
    ib: Option<&mut IncrementalBackupInfo<'mcx>>,
) -> PgResult<i64> {
    // 'path' points to the tablespace location, but we only want to include the
    // version directory in it that belongs to us.
    let pathbuf = format!("{path}/{TABLESPACE_VERSION_DIRECTORY}");

    // Store a directory entry in the tar file so we get the permissions right.
    let statbuf = match bbseam::lstat_file::call(&pathbuf)? {
        Some(s) => s,
        // If the tablespace went away while scanning, it's no error.
        None => return Ok(0),
    };

    let mut size = _tarWriteHeader(
        mcx, sink, state, TABLESPACE_VERSION_DIRECTORY, None, &statbuf, sizeonly,
    )?;

    // Send all the files in the tablespace version directory.
    size += sendDir(
        mcx, sink, state, &pathbuf, path.len() as i32, sizeonly, true,
        manifest.as_deref_mut(), spcoid, ib,
    )?;

    Ok(size)
}

// ===========================================================================
// sendDir() (basebackup.c:1188).
// ===========================================================================

fn sendDir<'mcx>(
    mcx: Mcx<'mcx>,
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    path: &str,
    basepathlen: i32,
    sizeonly: bool,
    sendtblspclinks: bool,
    mut manifest: Option<&mut BackupManifestInfo>,
    spcoid: Oid,
    mut ib: Option<&mut IncrementalBackupInfo<'mcx>>,
) -> PgResult<i64> {
    let mut size: i64 = 0;

    // The relative-block-numbers scratch array is allocated only for an
    // incremental backup (mirrors the C palloc; here a Vec sized RELSEG_SIZE).
    let mut relative_block_numbers: Vec<BlockNumber> = if ib.is_some() {
        vec![0; RELSEG_SIZE as usize]
    } else {
        Vec::new()
    };

    // Determine if the current path is a database directory that can contain
    // relations.
    let mut is_relation_dir = false;
    let mut is_global_dir = false;
    let mut dboid: Oid = INVALID_OID;

    let last_dir = last_dir_separator(path);
    if let Some(sep) = last_dir {
        let tail = &path[sep + 1..];
        if !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit()) {
            // C: parentPathLen = lastDir - path (the bytes before the final '/').
            let parent_path_len = sep;
            // Mark path as a database directory if the parent path is either
            // $PGDATA/base or a tablespace version path.
            let tvd = TABLESPACE_VERSION_DIRECTORY;
            // C: strncmp(path, "./base", parentPathLen) == 0.
            let base_match = strncmp(path, "./base", parent_path_len) == 0;
            // C: parentPathLen >= (sizeof(TVD)-1) &&
            //    strncmp(lastDir - (sizeof(TVD)-1), TVD, sizeof(TVD)-1) == 0.
            let tvd_match =
                parent_path_len >= tvd.len() && &path[sep - tvd.len()..sep] == tvd;
            if base_match || tvd_match {
                is_relation_dir = true;
                dboid = atooid(tail);
            }
        }
    } else if path == "./global" {
        is_relation_dir = true;
        is_global_dir = true;
    }

    for d_name in backend_storage_file_fd_seams::read_dir_names::call(path)? {
        let mut relfilenumber: RelFileNumber = INVALID_REL_FILE_NUMBER;
        let mut rel_fork_num: ForkNumber = INVALID_FORKNUM;
        let mut segno: u32 = 0;
        let mut is_relation_file = false;

        // Skip special stuff.
        if d_name == "." || d_name == ".." {
            continue;
        }
        // Skip temporary files.
        if d_name.starts_with(PG_TEMP_FILE_PREFIX) {
            continue;
        }
        // Skip macOS system files.
        if d_name == ".DS_Store" {
            continue;
        }

        // Check for interrupts / promotion mid-backup.
        // (CHECK_FOR_INTERRUPTS is serviced at seam boundaries; explicitly
        // re-check the promotion invariant.)
        if xlog::recovery_in_progress::call() != BACKUP_STARTED_IN_RECOVERY.with(core::cell::Cell::get)
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("the standby was promoted during online backup")
                .errhint(
                    "This means that the backup being taken is corrupt and should not be used. \
                     Try taking another online backup.",
                )
                .into_error().with_error_location(here("sendDir")));
        }

        // Scan for files that should be excluded.
        let mut exclude_found = false;
        for item in EXCLUDE_FILES {
            let cmplen = if item.match_prefix {
                item.name.len()
            } else {
                item.name.len() + 1
            };
            if strncmp(&d_name, item.name, cmplen) == 0 {
                exclude_found = true;
                break;
            }
        }
        if exclude_found {
            continue;
        }

        // If there could be non-temporary relation files in this directory, try
        // to parse the filename.
        if is_relation_dir {
            if let Some(parsed) = parse_filename_for_nontemp_relation(&d_name) {
                is_relation_file = true;
                relfilenumber = parsed.relnumber;
                rel_fork_num = parsed.fork;
                segno = parsed.segno;
            }
        }

        // Exclude all forks for unlogged tables except the init fork.
        if is_relation_file && rel_fork_num != INIT_FORKNUM {
            let init_fork_file = format!("{path}/{relfilenumber}_init");
            if bbseam::lstat_file::call(&init_fork_file)?.is_some() {
                continue;
            }
        }

        // Exclude temporary relations.
        if OidIsValid(dboid) && looks_like_temp_rel_name(&d_name) {
            continue;
        }

        let pathbuf = format!("{path}/{d_name}");

        // Skip pg_control here to back it up last.
        if pathbuf == format!("./{XLOG_CONTROL_FILE}") {
            continue;
        }

        let mut statbuf = match bbseam::lstat_file::call(&pathbuf)? {
            Some(s) => s,
            // If the file went away while scanning, it's not an error.
            None => continue,
        };

        // Scan for directories whose contents should be excluded.
        let mut exclude_found = false;
        for excl in EXCLUDE_DIR_CONTENTS {
            if &d_name == excl {
                convert_link_to_directory(&mut statbuf);
                size += _tarWriteHeader(
                    mcx, sink, state, &pathbuf[basepathlen as usize + 1..], None, &statbuf,
                    sizeonly,
                )?;
                exclude_found = true;
                break;
            }
        }
        if exclude_found {
            continue;
        }

        // We can skip pg_wal, but include it as an empty directory.
        if pathbuf == "./pg_wal" {
            convert_link_to_directory(&mut statbuf);
            size += _tarWriteHeader(
                mcx, sink, state, &pathbuf[basepathlen as usize + 1..], None, &statbuf, sizeonly,
            )?;
            // Also send archive_status and summaries directories.
            size += _tarWriteHeader(
                mcx, sink, state, "./pg_wal/archive_status", None, &statbuf, sizeonly,
            )?;
            size += _tarWriteHeader(
                mcx, sink, state, "./pg_wal/summaries", None, &statbuf, sizeonly,
            )?;
            continue; // don't recurse into pg_wal
        }

        // Allow symbolic links in pg_tblspc only.
        if path == "./pg_tblspc" && S_ISLNK(statbuf.mode) {
            let linkpath = bbseam::read_link::call(&pathbuf)?;
            size += _tarWriteHeader(
                mcx, sink, state, &pathbuf[basepathlen as usize + 1..], Some(&linkpath), &statbuf,
                sizeonly,
            )?;
        } else if S_ISDIR(statbuf.mode) {
            // Store a directory entry in the tar file so we get the permissions
            // right.
            size += _tarWriteHeader(
                mcx, sink, state, &pathbuf[basepathlen as usize + 1..], None, &statbuf, sizeonly,
            )?;

            // Call ourselves recursively for a directory, unless it happens to
            // be a separate tablespace located within PGDATA.
            let mut skip_this_dir = false;
            // Compare against state.tablespaces; skip past leading "./".
            let cmp = &pathbuf[2..];
            for ti in state.tablespaces.iter() {
                if let Some(rpath) = &ti.rpath {
                    if rpath == cmp {
                        skip_this_dir = true;
                        break;
                    }
                }
            }

            // Skip sending directories inside pg_tblspc, if not required.
            if pathbuf == "./pg_tblspc" && !sendtblspclinks {
                skip_this_dir = true;
            }

            if !skip_this_dir {
                size += sendDir(
                    mcx, sink, state, &pathbuf, basepathlen, sizeonly,
                    sendtblspclinks, manifest.as_deref_mut(), spcoid, ib.as_deref_mut(),
                )?;
            }
        } else if S_ISREG(statbuf.mode) {
            let mut sent = false;
            let mut num_blocks_required: u32 = 0;
            let mut truncation_block_length: u32 = 0;
            let tarfilename_owned;
            let mut tarfilename: &str = &pathbuf[basepathlen as usize + 1..];
            let mut method = FileBackupMethod::BackUpFileFully;

            if is_relation_file {
                if let Some(ib_ref) = ib.as_deref_mut() {
                    let (relspcoid, lookup_path) = if OidIsValid(spcoid) {
                        (
                            spcoid,
                            format!("{PG_TBLSPC_DIR}/{spcoid}/{tarfilename}"),
                        )
                    } else {
                        let relspcoid = if is_global_dir {
                            GLOBALTABLESPACE_OID
                        } else {
                            DEFAULTTABLESPACE_OID
                        };
                        (relspcoid, tarfilename.to_string())
                    };

                    method = ib_ref.get_file_backup_method(
                        &lookup_path,
                        dboid,
                        relspcoid,
                        relfilenumber,
                        rel_fork_num,
                        segno,
                        statbuf.size as usize,
                        &mut num_blocks_required,
                        &mut relative_block_numbers,
                        &mut truncation_block_length,
                    )?;
                    if method == FileBackupMethod::BackUpFileIncrementally {
                        statbuf.size = GetIncrementalFileSize(num_blocks_required) as i64;
                        tarfilename_owned = format!(
                            "{}/INCREMENTAL.{}",
                            &path[basepathlen as usize + 1..],
                            d_name
                        );
                        tarfilename = &tarfilename_owned;
                    }
                }
            }

            if !sizeonly {
                let incremental_blocks: Option<&[BlockNumber]> =
                    if method == FileBackupMethod::BackUpFileIncrementally {
                        Some(&relative_block_numbers[..num_blocks_required as usize])
                    } else {
                        None
                    };
                sent = sendFile(
                    mcx, sink, state, &pathbuf, tarfilename, &statbuf, true, dboid, spcoid,
                    relfilenumber, segno, manifest.as_deref_mut().expect("manifest required for sendFile"),
                    num_blocks_required, incremental_blocks, truncation_block_length,
                )?;
            }

            if sent || sizeonly {
                // Add size.
                size += statbuf.size;
                // Pad to a multiple of the tar block size.
                size += tar_padding_bytes_required(statbuf.size as usize) as i64;
                // Size of the header for the file.
                size += TAR_BLOCK_SIZE as i64;
            }
        } else {
            let _ = ereport(WARNING)
                .errmsg(format!("skipping special file \"{pathbuf}\""))
                .finish(here("sendDir"));
        }
    }

    let _ = &mut relative_block_numbers; // pfree analog (dropped at scope end).
    Ok(size)
}

// ===========================================================================
// sendFile() (basebackup.c:1573).
// ===========================================================================

fn sendFile<'mcx>(
    mcx: Mcx<'mcx>,
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    readfilename: &str,
    tarfilename: &str,
    statbuf: &bbseam::LstatInfo,
    missing_ok: bool,
    dboid: Oid,
    spcoid: Oid,
    relfilenumber: RelFileNumber,
    segno: u32,
    manifest: &mut BackupManifestInfo,
    num_incremental_blocks: u32,
    incremental_blocks: Option<&[BlockNumber]>,
    truncation_block_length: u32,
) -> PgResult<bool> {
    // C reads `sink->bbs_state->startptr` in verify_page_checksum; thread it
    // explicitly from the (separately threaded) shared state.
    let start_lsn = state.startptr;
    let mut checksum_ctx = checksum_init(manifest.checksum_type(), readfilename)?;

    let fd = backend_storage_file_fd_seams::open_transient_file::call(readfilename, OPEN_FLAGS);
    if fd < 0 {
        if backend_storage_file_fd_seams::last_errno::call() == _ENOENT && missing_ok {
            return Ok(false);
        }
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{readfilename}\""))
            .into_error().with_error_location(here("sendFile")));
    }

    _tarWriteHeader(mcx, sink, state, tarfilename, None, statbuf, false)?;

    // Checksums are verified in multiples of BLCKSZ.
    debug_assert_eq!(sink.buffer_length() % BLCKSZ, 0);

    // If we weren't told not to verify checksums, and checksums are enabled, and
    // this is a relation file, then verify the checksum.
    let mut verify_checksum = !NOVERIFY_CHECKSUMS.with(core::cell::Cell::get)
        && xlog::data_checksums_enabled::call()
        && RelFileNumberIsValid(relfilenumber);

    let mut blkno: BlockNumber = 0;
    let mut checksum_failures: i32 = 0;
    let mut bytes_done: i64 = 0;
    let mut ibindex: usize = 0;

    // If we're sending an incremental file, write the file header.
    if incremental_blocks.is_some() {
        let mut header_bytes_done: usize = 0;
        // Emit header data: magic, num_blocks, truncation_block_length, blocks.
        let magic = INCREMENTAL_MAGIC;
        push_to_sink(sink, state, &mut checksum_ctx, &mut header_bytes_done, &magic.to_ne_bytes())?;
        push_to_sink(
            sink, state, &mut checksum_ctx, &mut header_bytes_done,
            &num_incremental_blocks.to_ne_bytes(),
        )?;
        push_to_sink(
            sink, state, &mut checksum_ctx, &mut header_bytes_done,
            &truncation_block_length.to_ne_bytes(),
        )?;
        let blocks = incremental_blocks.unwrap();
        let mut block_bytes = Vec::with_capacity(blocks.len() * 4);
        for b in blocks.iter().take(num_incremental_blocks as usize) {
            block_bytes.extend_from_slice(&b.to_ne_bytes());
        }
        push_to_sink(sink, state, &mut checksum_ctx, &mut header_bytes_done, &block_bytes)?;

        // Add padding to align header to a multiple of BLCKSZ, but only if the
        // incremental file has some blocks and the alignment is needed.
        if num_incremental_blocks > 0 && header_bytes_done % BLCKSZ != 0 {
            let paddinglen = BLCKSZ - (header_bytes_done % BLCKSZ);
            let padding = vec![0u8; paddinglen];
            bytes_done += paddinglen as i64;
            push_to_sink(sink, state, &mut checksum_ctx, &mut header_bytes_done, &padding)?;
        }

        // Flush out any data still in the buffer so it's again empty.
        if header_bytes_done > 0 {
            bbsink_archive_contents(sink, state, header_bytes_done)?;
            let buf = sink.buffer_slice(header_bytes_done);
            let chunk = buf.to_vec();
            checksum_update(&mut checksum_ctx, &chunk)?;
        }

        // Update our notion of file position.
        bytes_done += 4; // magic
        bytes_done += 4; // num_incremental_blocks
        bytes_done += 4; // truncation_block_length
        bytes_done += 4 * num_incremental_blocks as i64;
    }

    // Loop until we read the amount of data the caller told us to expect.
    loop {
        let cnt: i64;
        if incremental_blocks.is_none() {
            let remaining = statbuf.size - bytes_done;
            if bytes_done >= statbuf.size {
                break;
            }
            cnt = read_file_data_into_buffer(
                sink, readfilename, fd, bytes_done, remaining as usize,
                blkno + segno * RELSEG_SIZE as u32, start_lsn, verify_checksum, &mut checksum_failures,
            )?;
        } else {
            if ibindex >= num_incremental_blocks as usize {
                break;
            }
            let relative_blkno = incremental_blocks.unwrap()[ibindex];
            ibindex += 1;
            cnt = read_file_data_into_buffer(
                sink, readfilename, fd, relative_blkno as i64 * BLCKSZ as i64, BLCKSZ,
                relative_blkno + segno * RELSEG_SIZE as u32, start_lsn, verify_checksum, &mut checksum_failures,
            )?;
            // Partial read => the relation is being truncated; treat as if the
            // entire block were truncated away.
            if cnt < BLCKSZ as i64 {
                break;
            }
        }

        // If the read length is not a multiple of BLCKSZ, we cannot verify
        // checksums.
        if verify_checksum && (cnt % BLCKSZ as i64 != 0) {
            let _ = ereport(WARNING)
                .errmsg(format!(
                    "could not verify checksum in file \"{readfilename}\", block {blkno}: \
                     read buffer size {cnt} and page size {BLCKSZ} differ"
                ))
                .finish(here("sendFile"));
            verify_checksum = false;
        }

        // If we hit end-of-file, a concurrent truncation must have occurred.
        if cnt == 0 {
            break;
        }

        blkno += (cnt / BLCKSZ as i64) as u32;
        bytes_done += cnt;

        // Archive the data we just read.
        bbsink_archive_contents(sink, state, cnt as usize)?;

        // Also feed it to the checksum machinery.
        let buf = sink.buffer_slice(cnt as usize);
        let chunk = buf.to_vec();
        checksum_update(&mut checksum_ctx, &chunk)?;
    }

    // If the file was truncated while we were sending it, pad with zeros.
    while bytes_done < statbuf.size {
        let remaining = (statbuf.size - bytes_done) as usize;
        let nbytes = core::cmp::min(sink.buffer_length(), remaining);
        zero_buffer(sink, nbytes);
        let buf = sink.buffer_slice(nbytes);
        let chunk = buf.to_vec();
        checksum_update(&mut checksum_ctx, &chunk)?;
        bbsink_archive_contents(sink, state, nbytes)?;
        bytes_done += nbytes as i64;
    }

    // Pad to a block boundary, per tar format requirements.
    _tarWritePadding(sink, state, bytes_done as usize)?;

    backend_storage_file_fd_seams::close_transient_file::call(fd);

    if checksum_failures > 1 {
        let _ = ereport(WARNING)
            .errmsg(format!(
                "file \"{readfilename}\" has a total of {checksum_failures} checksum verification failures"
            ))
            .finish(here("sendFile"));

        pgstat::pgstat_prepare_report_checksum_failure::call(dboid)?;
        pgstat::pgstat_report_checksum_failures_in_db::call(dboid, checksum_failures)?;
    }

    TOTAL_CHECKSUM_FAILURES.with(|c| c.set(c.get() + checksum_failures as i64));

    AddFileToBackupManifest(
        mcx, manifest, spcoid, tarfilename, statbuf.size as usize, statbuf.mtime, &mut checksum_ctx,
    )?;

    Ok(true)
}

// ===========================================================================
// read_file_data_into_buffer() (basebackup.c:1849).
// ===========================================================================

fn read_file_data_into_buffer(
    sink: &mut Bbsink<'_>,
    readfilename: &str,
    fd: i32,
    offset: i64,
    length: usize,
    blkno: BlockNumber,
    start_lsn: XLogRecPtr,
    verify_checksum: bool,
    checksum_failures: &mut i32,
) -> PgResult<i64> {
    // Try to read some more data.
    let want = core::cmp::min(sink.buffer_length(), length);
    let mut cnt = basebackup_read_file(sink, fd, 0, want, offset, readfilename, true)?;

    // Can't verify checksums if read length is not a multiple of BLCKSZ.
    if !verify_checksum || (cnt % BLCKSZ as i64) != 0 {
        return Ok(cnt);
    }

    // Verify checksum for each block.
    let nblocks = (cnt / BLCKSZ as i64) as usize;
    for i in 0..nblocks {
        let page_off = BLCKSZ * i;
        // If the page is OK, go on to the next one.
        let ok = {
            let page = &sink.buffer_slice(cnt as usize)[page_off..page_off + BLCKSZ];
            verify_page_checksum(page, start_lsn, blkno + i as u32)?.0
        };
        if ok {
            continue;
        }

        // Retry the block once on the first failure (re-read into the same
        // buffer slot, `sink->bbs_buffer + BLCKSZ * i`).
        let reread_cnt = basebackup_read_file(
            sink, fd, page_off, BLCKSZ, offset + (BLCKSZ * i) as i64, readfilename, false,
        )?;
        if reread_cnt == 0 {
            // Concurrent truncation: reduce cnt to processed blocks.
            cnt = (BLCKSZ * i) as i64;
            break;
        }

        // If the page now looks OK, go on to the next one.
        let buf_len = cnt.max((page_off + BLCKSZ) as i64) as usize;
        let (ok2, expected2) = {
            let page = &sink.buffer_slice(buf_len)[page_off..page_off + BLCKSZ];
            verify_page_checksum(page, start_lsn, blkno + i as u32)?
        };
        if ok2 {
            continue;
        }

        // Handle checksum failure.
        *checksum_failures += 1;
        let pd_checksum = {
            let page = &sink.buffer_slice(buf_len)[page_off..page_off + BLCKSZ];
            PageRef::new(page)?.pd_checksum()
        };
        if *checksum_failures <= 5 {
            let _ = ereport(WARNING)
                .errmsg(format!(
                    "checksum verification failed in file \"{readfilename}\", block {}: \
                     calculated {:X} but expected {:X}",
                    blkno + i as u32, expected2, pd_checksum
                ))
                .finish(here("read_file_data_into_buffer"));
        }
        if *checksum_failures == 5 {
            let _ = ereport(WARNING)
                .errmsg(format!(
                    "further checksum verification failures in file \"{readfilename}\" will not be reported"
                ))
                .finish(here("read_file_data_into_buffer"));
        }
    }

    Ok(cnt)
}


// ===========================================================================
// push_to_sink() (basebackup.c:1952).
// ===========================================================================

fn push_to_sink(
    sink: &mut Bbsink<'_>,
    state: &mut BbsinkState,
    checksum_ctx: &mut PgChecksumContext,
    bytes_done: &mut usize,
    mut data: &[u8],
) -> PgResult<()> {
    let mut length = data.len();
    while length > 0 {
        let buffer_length = sink.buffer_length();
        // < (not <=) so that exactly filling the buffer triggers a flush.
        if length < buffer_length - *bytes_done {
            let buf = sink.buffer_slice_mut(*bytes_done + length);
            buf[*bytes_done..*bytes_done + length].copy_from_slice(data);
            *bytes_done += length;
            return Ok(());
        }

        // Copy until buffer is full and flush it.
        let bytes_to_copy = buffer_length - *bytes_done;
        {
            let buf = sink.buffer_slice_mut(buffer_length);
            buf[*bytes_done..*bytes_done + bytes_to_copy].copy_from_slice(&data[..bytes_to_copy]);
        }
        data = &data[bytes_to_copy..];
        length -= bytes_to_copy;
        bbsink_archive_contents(sink, state, buffer_length)?;
        let chunk = sink.buffer_slice(buffer_length).to_vec();
        checksum_update(checksum_ctx, &chunk)?;
        *bytes_done = 0;
    }
    Ok(())
}

// ===========================================================================
// verify_page_checksum() (basebackup.c:1993).
// ===========================================================================

/// Returns `(verified, expected_checksum)`. `verified` is true when the page is
/// new / modified-since-start (we skip it) or the checksum matches; false when
/// the checksum mismatches (in which case `expected_checksum` holds the
/// computed value).
fn verify_page_checksum(
    page: &[u8],
    start_lsn: XLogRecPtr,
    blkno: BlockNumber,
) -> PgResult<(bool, u16)> {
    let page_ref = PageRef::new(page)?;

    // Only check pages not modified since the start of the base backup, and skip
    // completely new pages.
    if PageIsNew(&page_ref) || PageGetLSN(&page_ref) >= start_lsn {
        return Ok((true, 0));
    }

    // Perform the actual checksum calculation. pg_checksum_page transiently
    // zeroes pd_checksum, so it needs a mutable [u8; BLCKSZ] copy.
    let mut page_copy: [u8; BLCKSZ] = [0; BLCKSZ];
    page_copy.copy_from_slice(&page[..BLCKSZ]);
    let checksum = pg_checksum_page(&mut page_copy, blkno);

    let pd_checksum = page_ref.pd_checksum();
    if pd_checksum == checksum {
        return Ok((true, 0));
    }
    Ok((false, checksum))
}

// ===========================================================================
// _tarWriteHeader() / _tarWritePadding() (basebackup.c:2021 / 2075).
// ===========================================================================

fn _tarWriteHeader<'mcx>(
    mcx: Mcx<'mcx>,
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    filename: &str,
    linktarget: Option<&str>,
    statbuf: &bbseam::LstatInfo,
    sizeonly: bool,
) -> PgResult<i64> {
    let _ = mcx;
    if !sizeonly {
        debug_assert!(sink.buffer_length() >= TAR_BLOCK_SIZE);

        let header = bbseam::tar_create_header::call(
            filename, linktarget, statbuf.size, statbuf.mode, statbuf.uid, statbuf.gid,
            statbuf.mtime,
        );

        match header.rc {
            bbseam::TarError::Ok => {}
            bbseam::TarError::NameTooLong => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                    .errmsg(format!("file name too long for tar format: \"{filename}\""))
                    .into_error().with_error_location(here("_tarWriteHeader")));
            }
            bbseam::TarError::SymlinkTooLong => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                    .errmsg(format!(
                        "symbolic link target too long for tar format: file name \"{}\", target \"{}\"",
                        filename,
                        linktarget.unwrap_or("")
                    ))
                    .into_error().with_error_location(here("_tarWriteHeader")));
            }
        }

        let buf = sink.buffer_slice_mut(TAR_BLOCK_SIZE);
        buf.copy_from_slice(&header.bytes);
        bbsink_archive_contents(sink, state, TAR_BLOCK_SIZE)?;
    }

    Ok(TAR_BLOCK_SIZE as i64)
}

fn _tarWritePadding(sink: &mut Bbsink<'_>, state: &mut BbsinkState, len: usize) -> PgResult<()> {
    let pad = tar_padding_bytes_required(len);
    debug_assert!(sink.buffer_length() >= TAR_BLOCK_SIZE);
    debug_assert!(pad <= TAR_BLOCK_SIZE);
    if pad > 0 {
        zero_buffer(sink, pad);
        bbsink_archive_contents(sink, state, pad)?;
    }
    Ok(())
}

/// `tarPaddingBytesRequired(len)` (pgtar.h:79).
fn tar_padding_bytes_required(len: usize) -> usize {
    // TYPEALIGN(TAR_BLOCK_SIZE, len) - len.
    (len + (TAR_BLOCK_SIZE - 1)) / TAR_BLOCK_SIZE * TAR_BLOCK_SIZE - len
}

// ===========================================================================
// convert_link_to_directory() (basebackup.c:2098).
// ===========================================================================

fn convert_link_to_directory(statbuf: &mut bbseam::LstatInfo) {
    if S_ISLNK(statbuf.mode) {
        statbuf.mode = S_IFDIR | bbseam::pg_dir_create_mode::call();
    }
}

// ===========================================================================
// basebackup_read_file() (basebackup.c:2115).
// ===========================================================================

fn basebackup_read_file(
    sink: &mut Bbsink<'_>,
    fd: i32,
    buf_offset: usize,
    nbytes: usize,
    offset: i64,
    filename: &str,
    partial_read_ok: bool,
) -> PgResult<i64> {
    // Read into `sink->bbs_buffer + buf_offset` (the C pointer arithmetic).
    let buf = &mut sink.buffer_slice_mut(buf_offset + nbytes)[buf_offset..buf_offset + nbytes];
    let rc = backend_storage_file_fd_seams::pg_pread::call(fd, buf, offset);

    if rc < 0 {
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not read file \"{filename}\""))
            .into_error().with_error_location(here("basebackup_read_file")));
    }
    if !partial_read_ok && rc > 0 && rc as usize != nbytes {
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!(
                "could not read file \"{filename}\": read {rc} of {nbytes}"
            ))
            .into_error().with_error_location(here("basebackup_read_file")));
    }

    Ok(rc as i64)
}

// ===========================================================================
// helpers: checksum init/update wrappers, strncmp, atooid, last_dir_separator.
// ===========================================================================

fn checksum_init(type_: pg_checksum_type, filename: &str) -> PgResult<PgChecksumContext> {
    pg_checksum_init(type_).map_err(|_| {
        ereport(ERROR)
            .errmsg(format!("could not initialize checksum of file \"{filename}\""))
            .into_error().with_error_location(here("sendFile"))
    })
}

fn checksum_update(ctx: &mut PgChecksumContext, data: &[u8]) -> PgResult<()> {
    pg_checksum_update(ctx, data).map_err(|_| {
        ereport(ERROR)
            .errmsg("could not update checksum of base backup")
            .into_error().with_error_location(here("sendFile"))
    })
}

/// `strncmp(a, b, n)` returning C-style ordering of the first `n` bytes.
fn strncmp(a: &str, b: &str, n: usize) -> i32 {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    for i in 0..n {
        let ca = ab.get(i).copied();
        let cb = bb.get(i).copied();
        match (ca, cb) {
            (Some(x), Some(y)) if x == y => {
                if x == 0 {
                    return 0;
                }
            }
            (x, y) => {
                return x.unwrap_or(0) as i32 - y.unwrap_or(0) as i32;
            }
        }
    }
    0
}

/// `atooid(s)` — parse a decimal OID (`strtoul` semantics, value modulo 2^32).
fn atooid(s: &str) -> Oid {
    s.parse::<u32>().unwrap_or(0)
}

/// `last_dir_separator(path)` (common/path.c) — byte index of the last `/`
/// (or `\\` on Windows), or `None` if there is none.
fn last_dir_separator(path: &str) -> Option<usize> {
    let bytes = path.as_bytes();
    let mut found = None;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'/' {
            found = Some(i);
        }
        #[cfg(windows)]
        if b == b'\\' {
            found = Some(i);
        }
    }
    found
}

// ===========================================================================
// init_seams() — install the inward BASE_BACKUP seam.
// ===========================================================================

/// Install the inward seam that walsender's `BASE_BACKUP` command dispatches to.
pub fn init_seams() {
    backend_replication_basebackup_seams::send_base_backup::set(send_base_backup_entry);
}

/// Inward-seam entry. The seam contract carries only the `BaseBackupCmd`; the
/// uploaded-manifest `ib` is supplied by the (still-unported) `UPLOAD_MANIFEST`
/// path, so this passes `None` (see [`SendBaseBackup`]). Runs under the
/// process's current memory context.
fn send_base_backup_entry(cmd: BaseBackupCmd) -> PgResult<()> {
    // The C code runs in the walsender's `CurrentMemoryContext`. The repo has no
    // ambient context, so the entry point owns a context for the duration of the
    // backup (mirroring publicationcmds' inward seams).
    let ctx = mcx::MemoryContext::new("SendBaseBackup");
    SendBaseBackup(ctx.mcx(), &cmd, None)
}
