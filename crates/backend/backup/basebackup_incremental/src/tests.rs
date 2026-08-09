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
        ib.AppendIncrementalManifestData(chunk);
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
    ib.AppendIncrementalManifestData(&m);
    let msg = errmsg_of(ib.FinalizeIncrementalManifest());
    assert_eq!(msg, "backup manifest version 1 does not support incremental backup");
}

#[test]
fn system_identifier_mismatch_rejected_c_exact() {
    let mut ib = CreateIncrementalBackupInfo(31337);
    ib.AppendIncrementalManifestData(C_FIXTURE);
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
    ib.AppendIncrementalManifestData(&m);
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
    ib.AppendIncrementalManifestData(&m);
    ib.FinalizeIncrementalManifest().unwrap();
    assert_eq!(ib.manifest_file_count(), 1);
    assert_eq!(ib.manifest_file_lookup(b"dup"), Some(11));
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
    let err = ib.validate_wal_ranges(&history, &mut bs).unwrap_err();
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
        .unwrap_err();
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
