use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Once;

use super::*;
use blkreftable::BlockRefTable;
use mcx::MemoryContext;
use pg_sha2::PgSha256Ctx;
use types_core::ForkNumber;
use types_storage::RelFileLocator;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// Real C-generated manifest (pg_basebackup, PostgreSQL 18.4 — the Stage-2
// corpus fixture; see parse_manifest/src/tests.rs for provenance).
const C_FIXTURE: &[u8] =
    include_bytes!("../../../../common/parse_manifest/testdata/backup_manifest_pg18");
const C_FIXTURE_SYSID: u64 = 7671867332315642488;
const C_FIXTURE_NFILES: usize = 968;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Append the exact trailer the C server emits: the body (which must end
/// with a newline) is what the SHA-256 covers.
fn with_checksum(body: &str) -> Vec<u8> {
    let mut ctx = PgSha256Ctx::init_sha256();
    ctx.update(body.as_bytes());
    let digest = ctx.final_sha256();
    format!("{body}\"Manifest-Checksum\": \"{}\"}}\n", hex(&digest)).into_bytes()
}

fn tle(tli: TimeLineID, begin: XLogRecPtr, end: XLogRecPtr) -> TimeLineHistoryEntry {
    TimeLineHistoryEntry { tli, begin, end }
}

fn ws(tli: TimeLineID, start_lsn: XLogRecPtr, end_lsn: XLogRecPtr) -> WalSummaryFile {
    WalSummaryFile { tli, start_lsn, end_lsn }
}

/// An ib with the given WAL ranges installed, finalized-shaped.
fn ib_with_ranges(ranges: &[BackupWalRange]) -> IncrementalBackupInfo {
    let mut ib = CreateIncrementalBackupInfo(C_FIXTURE_SYSID);
    ib.manifest_wal_ranges = ranges.to_vec();
    ib.finalized = true;
    ib
}

fn backup_state(starttli: TimeLineID, startpoint: XLogRecPtr) -> BackupState {
    BackupState { starttli, startpoint, ..BackupState::default() }
}

fn errmsg_of<T>(r: PgResult<T>) -> String {
    match r {
        Ok(_) => panic!("expected error"),
        Err(e) => e.message().to_string(),
    }
}

// ---------------------------------------------------------------------------
// Manifest ingestion: Create / Append / Finalize + the version-2 and sysid
// gates (C: manifest_process_version / manifest_process_system_identifier).
// ---------------------------------------------------------------------------

#[test]
fn finalize_builds_file_hash_and_wal_ranges_from_c_manifest() {
    let mut ib = CreateIncrementalBackupInfo(C_FIXTURE_SYSID);
    // Feed in awkward chunk sizes — the accumulate-then-parse contract must
    // be split-point independent.
    for chunk in C_FIXTURE.chunks(7777) {
        ib.AppendIncrementalManifestData(chunk).unwrap();
    }
    ib.FinalizeIncrementalManifest().unwrap();

    assert_eq!(ib.manifest_file_count(), C_FIXTURE_NFILES);
    // Known entry: { "Path": "PG_VERSION", "Size": 3, ... }.
    assert_eq!(ib.manifest_file_lookup(b"PG_VERSION"), Some(3));
    assert_eq!(ib.manifest_file_lookup(b"no/such/file"), None);
    assert_eq!(
        ib.manifest_wal_ranges(),
        &[BackupWalRange { tli: 1, start_lsn: 0x2000028, end_lsn: 0x2000120 }]
    );
    // The accumulation buffer is released on finalize (C pfrees ib->buf).
    assert_eq!(ib.buf.capacity(), 0);
}

#[test]
fn version_1_manifest_rejected_c_exact() {
    let m = with_checksum("{ \"PostgreSQL-Backup-Manifest-Version\": 1,\n\"Files\": [],\n");
    let mut ib = CreateIncrementalBackupInfo(C_FIXTURE_SYSID);
    ib.AppendIncrementalManifestData(&m).unwrap();
    let msg = errmsg_of(ib.FinalizeIncrementalManifest());
    assert_eq!(msg, "backup manifest version 1 does not support incremental backup");
}

#[test]
fn system_identifier_mismatch_rejected_c_exact() {
    let mut ib = CreateIncrementalBackupInfo(31337);
    ib.AppendIncrementalManifestData(C_FIXTURE).unwrap();
    let msg = errmsg_of(ib.FinalizeIncrementalManifest());
    assert_eq!(
        msg,
        format!(
            "system identifier in backup manifest is {C_FIXTURE_SYSID}, but database system identifier is 31337"
        )
    );
}

#[test]
fn corrupt_checksum_rejected_via_stage2_parser() {
    let mut m = C_FIXTURE.to_vec();
    // Flip a byte inside the body (a path character), keeping JSON valid.
    let pos = m.windows(10).position(|w| w == b"PG_VERSION").unwrap();
    m[pos] = b'Q';
    let mut ib = CreateIncrementalBackupInfo(C_FIXTURE_SYSID);
    ib.AppendIncrementalManifestData(&m).unwrap();
    assert_eq!(errmsg_of(ib.FinalizeIncrementalManifest()), "manifest checksum mismatch");
}

#[test]
fn duplicate_manifest_paths_first_entry_wins() {
    // C: backup_file_insert + if (!found) — the first entry is retained.
    let body = concat!(
        "{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n",
        "\"System-Identifier\": 7671867332315642488,\n",
        "\"Files\": [\n",
        "{ \"Path\": \"dup\", \"Size\": 11 },\n",
        "{ \"Path\": \"dup\", \"Size\": 22 }],\n",
        "\"WAL-Ranges\": [\n",
        "{ \"Timeline\": 1, \"Start-LSN\": \"0/1000028\", \"End-LSN\": \"0/1000120\" }],\n",
    );
    let m = with_checksum(body);
    let mut ib = CreateIncrementalBackupInfo(C_FIXTURE_SYSID);
    ib.AppendIncrementalManifestData(&m).unwrap();
    ib.FinalizeIncrementalManifest().unwrap();
    assert_eq!(ib.manifest_file_count(), 1);
    assert_eq!(ib.manifest_file_lookup(b"dup"), Some(11));
}

#[test]
fn append_manifest_bounded_to_max_alloc_size() {
    // A small chunk from empty is always accepted (the normal path).
    check_manifest_capacity(0, 1 << 20).unwrap();

    // C: enlargeStringInfo (stringinfo.c:357) rejects when
    // `needed >= MaxAllocSize - len`, i.e. the buffer may hold at most
    // MaxAllocSize - 1 bytes (one byte is the terminating NUL).
    check_manifest_capacity(0, MAX_ALLOC_SIZE - 1).unwrap();
    check_manifest_capacity(MAX_ALLOC_SIZE - 10, 9).unwrap();

    // Exactly MaxAllocSize is already too much (audit row 6a2ae93a9e31:
    // the port accepted total == MaxAllocSize, C does not).
    let e = check_manifest_capacity(MAX_ALLOC_SIZE - 10, 10).err().unwrap();
    assert_eq!(e.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    check_manifest_capacity(0, MAX_ALLOC_SIZE).err().unwrap();

    // Past the ceiling: a catchable, per-command ERROR
    // (ERRCODE_PROGRAM_LIMIT_EXCEEDED) instead of an unbounded, infallible
    // allocation that would exhaust memory / abort the process — with C's
    // enlargeStringInfo message and detail, byte for byte (stringinfo.c:359).
    let e = check_manifest_capacity(MAX_ALLOC_SIZE - 10, 11).err().unwrap();
    assert_eq!(e.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    assert_eq!(
        e.message(),
        format!("string buffer exceeds maximum allowed length ({MAX_ALLOC_SIZE} bytes)")
    );
    assert_eq!(
        e.detail(),
        Some(
            format!(
                "Cannot enlarge string buffer containing {} bytes by 11 more bytes.",
                MAX_ALLOC_SIZE - 10
            )
            .as_str()
        )
    );
}

// ---------------------------------------------------------------------------
// validate_wal_ranges — the PrepareForIncrementalBackup start-LSN
// determination + timeline sanity checks (pure over expected_tles).
// ---------------------------------------------------------------------------

#[test]
fn prepare_rejects_manifest_without_wal_ranges() {
    let ib = ib_with_ranges(&[]);
    let mut bs = backup_state(1, 0x3000000);
    let msg = errmsg_of(ib.validate_wal_ranges(&[tle(1, 0, InvalidXLogRecPtr)], &mut bs));
    assert_eq!(msg, "manifest contains no required WAL ranges");
}

#[test]
fn prepare_sets_incremental_start_from_single_range() {
    let ib = ib_with_ranges(&[BackupWalRange { tli: 1, start_lsn: 0x2000028, end_lsn: 0x2000120 }]);
    let mut bs = backup_state(1, 0x3000000);
    let history = [tle(1, 0, InvalidXLogRecPtr)];
    let (earliest_tli, earliest_start) = ib.validate_wal_ranges(&history, &mut bs).unwrap();
    assert_eq!((earliest_tli, earliest_start), (1, 0x2000028));
    // Propagated into backup_label's INCREMENTAL FROM LSN/TLI.
    assert_eq!(bs.istartpoint, 0x2000028);
    assert_eq!(bs.istarttli, 1);
}

#[test]
fn prepare_finds_earliest_range_across_timeline_switch() {
    // Server history (readTimeLineHistory order: LATEST first): tli 2 from
    // 0x2000000 onward, tli 1 before that. Prior backup spanned the switch.
    let history = [tle(2, 0x2000000, InvalidXLogRecPtr), tle(1, 0, 0x2000000)];
    let ib = ib_with_ranges(&[
        BackupWalRange { tli: 2, start_lsn: 0x2000000, end_lsn: 0x2000100 },
        BackupWalRange { tli: 1, start_lsn: 0x1000028, end_lsn: 0x2000000 },
    ]);
    let mut bs = backup_state(2, 0x3000000);
    let (earliest_tli, earliest_start) = ib.validate_wal_ranges(&history, &mut bs).unwrap();
    assert_eq!((earliest_tli, earliest_start), (1, 0x1000028));
    assert_eq!((bs.istarttli, bs.istartpoint), (1, 0x1000028));
}

#[test]
fn prepare_rejects_timeline_missing_from_history() {
    let ib = ib_with_ranges(&[BackupWalRange { tli: 7, start_lsn: 0x1000, end_lsn: 0x2000 }]);
    let mut bs = backup_state(1, 0x3000000);
    let msg = errmsg_of(ib.validate_wal_ranges(&[tle(1, 0, InvalidXLogRecPtr)], &mut bs));
    assert_eq!(msg, "timeline 7 found in manifest, but not in this server's history");
}

#[test]
fn prepare_rejects_initial_range_starting_before_timeline() {
    let history = [tle(2, 0x2000000, InvalidXLogRecPtr)];
    let ib = ib_with_ranges(&[BackupWalRange { tli: 2, start_lsn: 0x1000000, end_lsn: 0x2500000 }]);
    let mut bs = backup_state(2, 0x3000000);
    let msg = errmsg_of(ib.validate_wal_ranges(&history, &mut bs));
    assert_eq!(
        msg,
        "manifest requires WAL from initial timeline 2 starting at 0/1000000, but that timeline begins at 0/2000000"
    );
}

#[test]
fn prepare_rejects_continuation_range_with_wrong_begin() {
    let history = [tle(2, 0x2000000, InvalidXLogRecPtr), tle(1, 0, 0x2000000)];
    let ib = ib_with_ranges(&[
        BackupWalRange { tli: 1, start_lsn: 0x1000028, end_lsn: 0x2000000 },
        // Continuation timeline 2 must begin exactly at the switch point.
        BackupWalRange { tli: 2, start_lsn: 0x2000010, end_lsn: 0x2500000 },
    ]);
    let mut bs = backup_state(2, 0x3000000);
    let msg = errmsg_of(ib.validate_wal_ranges(&history, &mut bs));
    assert_eq!(
        msg,
        "manifest requires WAL from continuation timeline 2 starting at 0/2000010, but that timeline begins at 0/2000000"
    );
}

#[test]
fn prepare_rejects_final_range_ending_after_backup_start() {
    let history = [tle(1, 0, InvalidXLogRecPtr)];
    let ib = ib_with_ranges(&[BackupWalRange { tli: 1, start_lsn: 0x1000, end_lsn: 0x5000000 }]);
    let mut bs = backup_state(1, 0x3000000);
    let err = ib.validate_wal_ranges(&history, &mut bs).err().unwrap();
    assert_eq!(
        err.message(),
        "manifest requires WAL from final timeline 1 ending at 0/5000000, but this backup starts at 0/3000000"
    );
    assert_eq!(
        err.hint(),
        Some("This can happen for incremental backups on a standby if there was little activity since the previous backup.")
    );
}

#[test]
fn prepare_rejects_nonfinal_range_with_wrong_end() {
    let history = [tle(2, 0x2000000, InvalidXLogRecPtr), tle(1, 0, 0x2000000)];
    let ib = ib_with_ranges(&[
        BackupWalRange { tli: 2, start_lsn: 0x2000000, end_lsn: 0x2500000 },
        // Non-final timeline 1 must end exactly at the switch point.
        BackupWalRange { tli: 1, start_lsn: 0x1000028, end_lsn: 0x1F00000 },
    ]);
    let mut bs = backup_state(2, 0x3000000);
    let msg = errmsg_of(ib.validate_wal_ranges(&history, &mut bs));
    assert_eq!(
        msg,
        "manifest requires WAL from non-final timeline 1 ending at 0/1F00000, but this server switched timelines at 0/2000000"
    );
}

// ---------------------------------------------------------------------------
// collect_required_summaries — the summarized-WAL coverage cross-check
// (C: the FilterWalSummaries/WalSummariesAreComplete loop).
// ---------------------------------------------------------------------------

#[test]
fn coverage_complete_single_timeline_collects_summaries() {
    let cx = MemoryContext::new("collect-test");
    let history = [tle(1, 0, InvalidXLogRecPtr)];
    let all = [ws(1, 0x2000028, 0x2800000), ws(1, 0x2800000, 0x3000000)];
    let required =
        collect_required_summaries(cx.mcx(), &history, &all, 1, 0x3000000, 1, 0x2000028).unwrap();
    assert_eq!(required.len(), 2);
}

#[test]
fn coverage_missing_summaries_error_c_exact() {
    let cx = MemoryContext::new("collect-test");
    let history = [tle(1, 0, InvalidXLogRecPtr)];
    let msg = errmsg_of(collect_required_summaries(
        cx.mcx(),
        &history,
        &[],
        1,
        0x3000000,
        1,
        0x2000028,
    ));
    assert_eq!(
        msg,
        "WAL summaries are required on timeline 1 from 0/2000028 to 0/3000000, but no summaries for that timeline and LSN range exist"
    );
}

#[test]
fn coverage_incomplete_summaries_error_c_exact_with_detail() {
    let cx = MemoryContext::new("collect-test");
    let history = [tle(1, 0, InvalidXLogRecPtr)];
    // Covers up to 0x2800000, then a gap.
    let all = [ws(1, 0x2000028, 0x2800000), ws(1, 0x2900000, 0x3000000)];
    let err = collect_required_summaries(cx.mcx(), &history, &all, 1, 0x3000000, 1, 0x2000028)
        .err().unwrap();
    assert_eq!(
        err.message(),
        "WAL summaries are required on timeline 1 from 0/2000028 to 0/3000000, but the summaries for that timeline and LSN range are incomplete"
    );
    assert_eq!(
        err.detail(),
        Some("The first unsummarized LSN in this range is 0/2800000.")
    );
}

#[test]
fn coverage_walks_history_back_to_earliest_range_timeline_only() {
    let cx = MemoryContext::new("collect-test");
    // tli 3 current, backup started there; prior backup began on tli 2;
    // tli 1 must NOT be consulted (loop breaks at earliest range tli).
    let history = [
        tle(3, 0x3000000, InvalidXLogRecPtr),
        tle(2, 0x2000000, 0x3000000),
        tle(1, 0, 0x2000000),
    ];
    let all = [
        ws(3, 0x3000000, 0x3800000),
        ws(2, 0x2000028, 0x3000000),
        // tli-1 summaries exist but are irrelevant.
        ws(1, 0, 0x2000000),
    ];
    let required =
        collect_required_summaries(cx.mcx(), &history, &all, 3, 0x3800000, 2, 0x2000028).unwrap();
    let mut tlis: Vec<TimeLineID> = required.iter().map(|w| w.tli).collect();
    tlis.sort_unstable();
    assert_eq!(tlis, vec![2, 3]);
}

// ---------------------------------------------------------------------------
// merge_required_summaries — summary files -> one BlockRefTable
// (C: OpenWalSummaryFile/ReadWalSummary + CreateBlockRefTableReader loop).
// ---------------------------------------------------------------------------

static SCRATCH_N: AtomicU32 = AtomicU32::new(0);

fn fd_setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        if !postgres_seams::check_for_interrupts::is_installed() {
            postgres_seams::check_for_interrupts::set(|| Ok(()));
        }
        aio_seams::pgaio_closing_fd::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
    });
    fd::InitFileAccess();
}

fn scratch_summaries_dir() -> String {
    let n = SCRATCH_N.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "pgrust_incrbb_test_{}_{n}/summaries",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_str().unwrap().to_owned()
}

fn rl(spc: u32, db: u32, rel: u32) -> RelFileLocator {
    RelFileLocator { spcOid: spc, dbOid: db, relNumber: rel }
}

/// Serialize `tab` into `<dir>/<summary filename for ws>` via the
/// blkreftable writer (byte-identical to what the WAL summarizer persists).
fn write_summary_file(dir: &str, w: &WalSummaryFile, tab: &BlockRefTable<'_>) {
    let mut bytes: Vec<u8> = Vec::new();
    tab.write(|chunk: &[u8]| {
        bytes.extend_from_slice(chunk);
        Ok(())
    })
    .unwrap();
    let name = format!(
        "{dir}/{:08X}{:08X}{:08X}{:08X}{:08X}.summary",
        w.tli,
        (w.start_lsn >> 32) as u32,
        w.start_lsn as u32,
        (w.end_lsn >> 32) as u32,
        w.end_lsn as u32
    );
    std::fs::write(name, bytes).unwrap();
}

#[test]
fn merge_reads_and_combines_summary_files() {
    fd_setup();
    let dir = scratch_summaries_dir();
    let cx = MemoryContext::new("merge-test");
    let mcx = cx.mcx();

    // Summary 1: rel 16384 blocks {1, 3}, limit_block for rel 16385.
    let ws1 = ws(1, 0x1000, 0x2000);
    {
        let mut tab = BlockRefTable::new(mcx);
        tab.mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, 1).unwrap();
        tab.mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, 3).unwrap();
        tab.set_limit_block(rl(1663, 5, 16385), ForkNumber::MAIN_FORKNUM, 0);
        write_summary_file(&dir, &ws1, &tab);
    }
    // Summary 2: rel 16384 block {70000} (bitmap-chunk territory is not
    // required — any second block proves the merge unions).
    let ws2 = ws(1, 0x2000, 0x3000);
    {
        let mut tab = BlockRefTable::new(mcx);
        tab.mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, 70000).unwrap();
        write_summary_file(&dir, &ws2, &tab);
    }

    let brtab = merge_required_summaries(mcx, &[ws1, ws2], &dir).unwrap();

    let entry = brtab.get_entry(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM).unwrap();
    let mut blocks = [0u32; 8];
    let n = entry.get_blocks(0, u32::MAX, &mut blocks);
    assert_eq!(&blocks[..n], &[1, 3, 70000]);

    // The db-recreated sentinel style limit_block entry survives the merge.
    let entry = brtab.get_entry(rl(1663, 5, 16385), ForkNumber::MAIN_FORKNUM).unwrap();
    assert_eq!(entry.limit_block(), 0);

    // A missing file errors with C's open error.
    let missing = ws(9, 0x9000, 0xA000);
    let msg = errmsg_of(merge_required_summaries(mcx, &[missing], &dir));
    assert!(msg.starts_with("could not open file"), "{msg}");
}

// Captured ereport lines for the DEBUG1 witness below. A process-wide store
// (not a thread_local): the emit hook is installed on the test thread only,
// so nothing else writes here.
static LOG_LINES: std::sync::Mutex<Vec<(types_error::ErrorLevel, String)>> =
    std::sync::Mutex::new(Vec::new());

fn capture_log_line(err: &types_error::PgError, _output_to_server: &mut bool) {
    LOG_LINES.lock().unwrap().push((err.level(), err.message().to_string()));
}

fn take_log_lines() -> Vec<(types_error::ErrorLevel, String)> {
    std::mem::take(&mut *LOG_LINES.lock().unwrap())
}

// basebackup_incremental.c:584: before each summary file is read,
// ereport(DEBUG1, errmsg_internal("reading WAL summary file \"%s\"",
// FilePathName(wsio.file))) — one line per required summary, naming the
// path the file was opened under (audit row 3fc13907d6c5).
#[test]
fn reading_wal_summary_file_debug1_is_c_exact() {
    fd_setup();
    let dir = scratch_summaries_dir();
    let cx = MemoryContext::new("merge-debug1-test");
    let mcx = cx.mcx();

    let ws1 = ws(1, 0x1000, 0x2000);
    let ws2 = ws(1, 0x2000, 0x3000);
    for w in [&ws1, &ws2] {
        let mut tab = BlockRefTable::new(mcx);
        tab.mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, 1).unwrap();
        write_summary_file(&dir, w, &tab);
    }

    let saved_min = elog::config::log_min_messages();
    elog::config::set_log_min_messages(types_error::DEBUG1);
    let prev = elog::set_emit_log_hook(Some(capture_log_line));
    take_log_lines();
    let result = merge_required_summaries(mcx, &[ws1, ws2], &dir);
    let lines = take_log_lines();
    elog::set_emit_log_hook(prev);
    elog::config::set_log_min_messages(saved_min);
    result.unwrap();

    let debug1: Vec<&str> = lines
        .iter()
        .filter(|(lvl, _)| *lvl == types_error::DEBUG1)
        .map(|(_, m)| m.as_str())
        .collect();
    let want = [
        format!(
            "reading WAL summary file \"{dir}/{:08X}{:08X}{:08X}{:08X}{:08X}.summary\"",
            1, 0, 0x1000, 0, 0x2000
        ),
        format!(
            "reading WAL summary file \"{dir}/{:08X}{:08X}{:08X}{:08X}{:08X}.summary\"",
            1, 0, 0x2000, 0, 0x3000
        ),
    ];
    assert_eq!(debug1, [want[0].as_str(), want[1].as_str()], "all lines: {lines:?}");
}

// ---------------------------------------------------------------------------
// GetFileBackupMethod — the FULL-vs-INCREMENTAL decision ladder, every rung
// in C order (basebackup_incremental.c:663), plus the incremental-file size
// helpers. BLCKSZ = 8192, RELSEG_SIZE = 131072 throughout.
// ---------------------------------------------------------------------------

const B: u64 = BLCKSZ as u64;

/// An ib whose manifest "contains" the given paths (finalized-shaped), and a
/// brtab to thread alongside.
fn ib_with_files(paths: &[&str]) -> IncrementalBackupInfo {
    let mut ib = CreateIncrementalBackupInfo(C_FIXTURE_SYSID);
    for p in paths {
        ib.manifest_files.insert(p.as_bytes().into(), 0);
    }
    ib.finalized = true;
    ib
}

/// Call GetFileBackupMethod with the C out-param shape collapsed to a tuple:
/// (method, num_blocks_required, blocks, truncation_block_length).
#[allow(clippy::too_many_arguments)]
fn method_of(
    ib: &IncrementalBackupInfo,
    brtab: &BlockRefTable<'_>,
    path: &str,
    dboid: Oid,
    spcoid: Oid,
    relfilenumber: RelFileNumber,
    forknum: ForkNumber,
    segno: u32,
    size: u64,
) -> (FileBackupMethod, u32, Vec<BlockNumber>, u32) {
    let mut num_blocks: u32 = 0;
    let mut trunc: u32 = 0;
    let mut blocks = vec![0u32; RELSEG_SIZE as usize];
    let method = ib
        .GetFileBackupMethod(
            brtab, path, dboid, spcoid, relfilenumber, forknum, segno, size,
            &mut num_blocks, &mut blocks, &mut trunc,
        )
        .unwrap();
    blocks.truncate(num_blocks as usize);
    (method, num_blocks, blocks, trunc)
}

#[test]
fn gfbm_odd_size_or_oversized_segment_is_full() {
    let cx = MemoryContext::new("gfbm");
    let mcx = cx.mcx();
    let ib = ib_with_files(&["base/5/16384"]);
    let brtab = BlockRefTable::new(mcx);

    // size % BLCKSZ != 0 -> full.
    let (m, ..) = method_of(&ib, &brtab, "base/5/16384", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 0, B + 1);
    assert_eq!(m, FileBackupMethod::Fully);

    // size / BLCKSZ > RELSEG_SIZE -> full.
    let (m, ..) = method_of(&ib, &brtab, "base/5/16384", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 0, (RELSEG_SIZE as u64 + 1) * B);
    assert_eq!(m, FileBackupMethod::Fully);

    // Exactly RELSEG_SIZE blocks is fine (falls through the first rung).
    let (m, n, _, t) = method_of(&ib, &brtab, "base/5/16384", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 0, RELSEG_SIZE as u64 * B);
    assert_eq!(m, FileBackupMethod::Incrementally);
    assert_eq!((n, t), (0, RELSEG_SIZE));
}

#[test]
fn gfbm_fsm_fork_is_always_full() {
    let cx = MemoryContext::new("gfbm");
    let ib = ib_with_files(&["base/5/16384_fsm"]);
    let brtab = BlockRefTable::new(cx.mcx());
    let (m, ..) = method_of(&ib, &brtab, "base/5/16384_fsm", 5, 1663, 16384,
        ForkNumber::FSM_FORKNUM, 0, 3 * B);
    assert_eq!(m, FileBackupMethod::Fully);
}

#[test]
fn gfbm_file_absent_from_prior_manifest_is_full_incremental_name_counts() {
    let cx = MemoryContext::new("gfbm");
    let brtab = BlockRefTable::new(cx.mcx());

    // Not in the manifest under either name -> full.
    let ib = ib_with_files(&["base/5/99999"]);
    let (m, ..) = method_of(&ib, &brtab, "base/5/16384", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 0, 4 * B);
    assert_eq!(m, FileBackupMethod::Fully);

    // Present only under the INCREMENTAL.* name (prior backup was itself
    // incremental) -> proceeds down the ladder.
    let ib = ib_with_files(&["base/5/INCREMENTAL.16384"]);
    let (m, n, _, t) = method_of(&ib, &brtab, "base/5/16384", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 0, 4 * B);
    assert_eq!(m, FileBackupMethod::Incrementally);
    assert_eq!((n, t), (0, 4));

    // Segmented file: the INCREMENTAL name keeps the .segno suffix.
    let ib = ib_with_files(&["base/5/INCREMENTAL.16384.1"]);
    let (m, ..) = method_of(&ib, &brtab, "base/5/16384.1", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 1, 4 * B);
    assert_eq!(m, FileBackupMethod::Incrementally);
}

#[test]
fn gfbm_db_recreated_sentinel_forces_full() {
    let cx = MemoryContext::new("gfbm");
    let mcx = cx.mcx();
    let ib = ib_with_files(&["base/5/16384"]);
    // The {spc, db, rel=0} MAIN_FORKNUM sentinel: this database was
    // (re)created since the prior backup.
    let mut brtab = BlockRefTable::new(mcx);
    brtab.set_limit_block(rl(1663, 5, 0), ForkNumber::MAIN_FORKNUM, 0);

    let (m, ..) = method_of(&ib, &brtab, "base/5/16384", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 0, 4 * B);
    assert_eq!(m, FileBackupMethod::Fully);
}

#[test]
fn gfbm_no_brtab_entry_zero_length_full_else_zero_block_incremental() {
    let cx = MemoryContext::new("gfbm");
    let brtab = BlockRefTable::new(cx.mcx());
    let ib = ib_with_files(&["base/5/16384"]);

    // Unchanged file of zero length -> full (an incremental file is always
    // longer than zero bytes).
    let (m, ..) = method_of(&ib, &brtab, "base/5/16384", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 0, 0);
    assert_eq!(m, FileBackupMethod::Fully);

    // Unchanged nonzero file -> incremental with zero blocks.
    let (m, n, blocks, t) = method_of(&ib, &brtab, "base/5/16384", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 0, 7 * B);
    assert_eq!(m, FileBackupMethod::Incrementally);
    assert_eq!((n, t), (0, 7));
    assert!(blocks.is_empty());
}

#[test]
fn gfbm_limit_block_at_or_before_segment_start_is_full() {
    let cx = MemoryContext::new("gfbm");
    let mcx = cx.mcx();
    let ib = ib_with_files(&["base/5/16384.1"]);
    let mut brtab = BlockRefTable::new(mcx);
    // Truncated to exactly the segment-1 boundary: limit_block == segno *
    // RELSEG_SIZE -> full.
    brtab.set_limit_block(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, RELSEG_SIZE);

    let (m, ..) = method_of(&ib, &brtab, "base/5/16384.1", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 1, 4 * B);
    assert_eq!(m, FileBackupMethod::Fully);
}

#[test]
fn gfbm_ninety_percent_threshold_exact() {
    let cx = MemoryContext::new("gfbm");
    let mcx = cx.mcx();
    let ib = ib_with_files(&["base/5/16384"]);

    // 10-block file, 9 modified: 9*BLCKSZ > 10*BLCKSZ*0.9 is FALSE (equal),
    // so C sends it INCREMENTALLY — the threshold is strictly greater-than.
    let mut brtab = BlockRefTable::new(mcx);
    for b in 0..9 {
        brtab.mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, b).unwrap();
    }
    let (m, n, blocks, t) = method_of(&ib, &brtab, "base/5/16384", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 0, 10 * B);
    assert_eq!(m, FileBackupMethod::Incrementally);
    assert_eq!(n, 9);
    assert_eq!(blocks, (0..9).collect::<Vec<_>>());
    assert_eq!(t, 10);

    // All 10 of 10 modified: 10*BLCKSZ > 9*BLCKSZ -> FULL.
    let mut brtab = BlockRefTable::new(mcx);
    for b in 0..10 {
        brtab.mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, b).unwrap();
    }
    let (m, ..) = method_of(&ib, &brtab, "base/5/16384", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 0, 10 * B);
    assert_eq!(m, FileBackupMethod::Fully);

    // 3 of 10 modified (<90%) -> incremental, blocks sorted.
    let mut brtab = BlockRefTable::new(mcx);
    for b in [7u32, 2, 5] {
        brtab.mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, b).unwrap();
    }
    let (m, n, blocks, t) = method_of(&ib, &brtab, "base/5/16384", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 0, 10 * B);
    assert_eq!(m, FileBackupMethod::Incrementally);
    assert_eq!((n, t), (3, 10));
    assert_eq!(blocks, vec![2, 5, 7]);
}

#[test]
fn gfbm_segno_relativizes_blocks_and_truncation_uses_limit_block() {
    let cx = MemoryContext::new("gfbm");
    let mcx = cx.mcx();
    let ib = ib_with_files(&["base/5/16384.1"]);

    // Segment 1: absolute blocks RELSEG_SIZE+{1,3} modified (within the
    // 4-block segment file — get_blocks' stop bound is exclusive, so blocks
    // at/after EOF are not returned); the relation grew to limit_block =
    // RELSEG_SIZE + 6 (beyond the current 4-block segment file size), so
    // truncation_block_length is raised to 6.
    let mut brtab = BlockRefTable::new(mcx);
    brtab.set_limit_block(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, RELSEG_SIZE + 6);
    for b in [RELSEG_SIZE + 3, RELSEG_SIZE + 1] {
        brtab.mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, b).unwrap();
    }
    let (m, n, blocks, t) = method_of(&ib, &brtab, "base/5/16384.1", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 1, 4 * B);
    assert_eq!(m, FileBackupMethod::Incrementally);
    assert_eq!(n, 2);
    assert_eq!(blocks, vec![1, 3]); // relative to the segment, sorted
    assert_eq!(t, 6); // raised from 4 to the relative limit block

    // The truncation length is clamped to RELSEG_SIZE.
    let mut brtab = BlockRefTable::new(mcx);
    brtab.set_limit_block(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, 3 * RELSEG_SIZE);
    brtab.mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, RELSEG_SIZE + 2).unwrap();
    let (m, _, _, t) = method_of(&ib, &brtab, "base/5/16384.1", 5, 1663, 16384,
        ForkNumber::MAIN_FORKNUM, 1, 4 * B);
    assert_eq!(m, FileBackupMethod::Incrementally);
    assert_eq!(t, RELSEG_SIZE);
}

#[test]
fn incremental_path_and_size_helpers_match_c() {
    assert_eq!(GetIncrementalFilePath("base/5/16384"), "base/5/INCREMENTAL.16384");
    assert_eq!(GetIncrementalFilePath("base/5/16384.3"), "base/5/INCREMENTAL.16384.3");
    assert_eq!(GetIncrementalFilePath("global/1262"), "global/INCREMENTAL.1262");
    assert_eq!(
        GetIncrementalFilePath("pg_tblspc/16390/PG_18_202506291/5/16400_vm"),
        "pg_tblspc/16390/PG_18_202506291/5/INCREMENTAL.16400_vm"
    );

    // Header: 12 bytes + 4/block, rounded to BLCKSZ only when blocks > 0.
    assert_eq!(GetIncrementalHeaderSize(0), 12);
    assert_eq!(GetIncrementalHeaderSize(1), BLCKSZ);
    assert_eq!(GetIncrementalHeaderSize(2045), BLCKSZ); // 12 + 8180 = 8192 exactly
    assert_eq!(GetIncrementalHeaderSize(2046), 2 * BLCKSZ);
    assert_eq!(GetIncrementalFileSize(0), 12);
    assert_eq!(GetIncrementalFileSize(1), BLCKSZ + BLCKSZ);
    assert_eq!(GetIncrementalFileSize(2045), BLCKSZ + 2045 * BLCKSZ);
}
