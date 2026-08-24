use super::*;

#[test]
fn wal_summary_filename_roundtrip() {
    let ws = WalSummaryFile { tli: 1, start_lsn: 0x0000_0001_0428_0048, end_lsn: 0x0000_0001_0500_0000 };
    let name = format!(
        "{:08X}{:08X}{:08X}{:08X}{:08X}.summary",
        ws.tli,
        (ws.start_lsn >> 32) as u32,
        ws.start_lsn as u32,
        (ws.end_lsn >> 32) as u32,
        ws.end_lsn as u32
    );
    assert_eq!(name, "0000000100000001042800480000000105000000.summary");
    let (tli, start, end) = parse_wal_summary_filename(&name).unwrap();
    assert_eq!((tli, start, end), (ws.tli, ws.start_lsn, ws.end_lsn));
}

#[test]
fn wal_summary_filename_rejects_noise() {
    assert!(parse_wal_summary_filename("temp.summary").is_none());
    assert!(parse_wal_summary_filename("0000000100000001042800480000000105000000.partial").is_none());
    assert!(parse_wal_summary_filename("000000010000000104280048000000010500000g.summary").is_none());
    assert!(parse_wal_summary_filename("0000000100000001042800480000000105000000.summary.tmp").is_none());
}

#[test]
fn require_record_len_rejects_short_payloads() {
    // A short/empty untrusted WAL payload must produce a catchable
    // ERRCODE_DATA_CORRUPTED error rather than a slice-OOB panic.
    let err = require_record_len(&[], 4, "XLOG_CHECKPOINT_REDO", "test").err().unwrap();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);

    let err = require_record_len(&[0u8; 3], 4, "XLOG_CHECKPOINT_REDO", "test").err().unwrap();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);

    // ntablespaces-driven size: 8-byte header claiming one tablespace needs 12.
    let err = require_record_len(&[0u8; 8], 8 + 4, "XLOG_DBASE_DROP", "test").err().unwrap();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);

    // Exactly enough, and more than enough, both pass.
    assert!(require_record_len(&[0u8; 4], 4, "XLOG_CHECKPOINT_REDO", "test").is_ok());
    assert!(require_record_len(&[0u8; 88], 4, "XLOG_CHECKPOINT_REDO", "test").is_ok());
}

#[test]
fn diff_ms_rounds_up_and_clamps() {
    assert_eq!(diff_ms(0, 0), 0);
    assert_eq!(diff_ms(10, 5), 0);
    assert_eq!(diff_ms(0, 1), 1);
    assert_eq!(diff_ms(0, 1000), 1);
    assert_eq!(diff_ms(0, 10_000_000), 10_000);
}

fn ws(tli: TimeLineID, start_lsn: XLogRecPtr, end_lsn: XLogRecPtr) -> WalSummaryFile {
    WalSummaryFile { tli, start_lsn, end_lsn }
}

#[test]
fn summaries_complete_empty_list() {
    // C: empty list -> false with missing_lsn = InvalidXLogRecPtr.
    assert_eq!(WalSummariesAreComplete(&[], 0x1000, 0x2000), (false, InvalidXLogRecPtr));
}

#[test]
fn summaries_complete_contiguous_chain() {
    let list = [ws(1, 0x1000, 0x1800), ws(1, 0x1800, 0x2000)];
    assert_eq!(WalSummariesAreComplete(&list, 0x1000, 0x2000), (true, InvalidXLogRecPtr));
    // Unsorted input: the function sorts a private copy.
    let rev = [ws(1, 0x1800, 0x2000), ws(1, 0x1000, 0x1800)];
    assert_eq!(WalSummariesAreComplete(&rev, 0x1000, 0x2000), (true, InvalidXLogRecPtr));
}

#[test]
fn summaries_complete_reports_first_gap() {
    // Gap between 0x1800 and 0x1900: missing_lsn = end of covered prefix.
    let list = [ws(1, 0x1000, 0x1800), ws(1, 0x1900, 0x2000)];
    assert_eq!(WalSummariesAreComplete(&list, 0x1000, 0x2000), (false, 0x1800));
    // Runs out before end_lsn without a gap.
    let short = [ws(1, 0x1000, 0x1800)];
    assert_eq!(WalSummariesAreComplete(&short, 0x1000, 0x2000), (false, 0x1800));
    // First summary starts after start_lsn: missing_lsn = start_lsn.
    let late = [ws(1, 0x1100, 0x2000)];
    assert_eq!(WalSummariesAreComplete(&late, 0x1000, 0x2000), (false, 0x1000));
}

#[test]
fn summaries_complete_tolerates_overlap_and_containment() {
    // Overlapping ranges must still prove completeness (C comment: "intended
    // to be correct even in case of overlap").
    let list = [ws(1, 0x1000, 0x1a00), ws(1, 0x1400, 0x2000)];
    assert_eq!(WalSummariesAreComplete(&list, 0x1000, 0x2000), (true, InvalidXLogRecPtr));
    // A fully-contained range neither helps nor hurts.
    let contained = [ws(1, 0x1000, 0x1800), ws(1, 0x1200, 0x1600), ws(1, 0x1800, 0x2000)];
    assert_eq!(WalSummariesAreComplete(&contained, 0x1000, 0x2000), (true, InvalidXLogRecPtr));
    // Coverage extending past end_lsn counts.
    let over = [ws(1, 0x0800, 0x2800)];
    assert_eq!(WalSummariesAreComplete(&over, 0x1000, 0x2000), (true, InvalidXLogRecPtr));
}

#[test]
fn summaries_complete_is_timeline_blind() {
    // Documented C behavior: TLIs are ignored; callers filter first.
    let list = [ws(1, 0x1000, 0x1800), ws(2, 0x1800, 0x2000)];
    assert_eq!(WalSummariesAreComplete(&list, 0x1000, 0x2000), (true, InvalidXLogRecPtr));
}

#[test]
fn filter_wal_summaries_bounds_are_inclusive() {
    let cx = MemoryContext::new("filter-wal-summaries-test");
    let mcx = cx.mcx();
    {
        let list = [
            ws(1, 0x1000, 0x1800),
            ws(1, 0x1800, 0x2000),
            ws(2, 0x2000, 0x2800),
            ws(1, 0x2800, 0x3000),
        ];
        // TLI filter.
        let r = FilterWalSummaries(mcx, &list, 2, InvalidXLogRecPtr, InvalidXLogRecPtr);
        assert_eq!(&r[..], &[ws(2, 0x2000, 0x2800)]);
        // tli == 0 means any timeline.
        let r = FilterWalSummaries(mcx, &list, 0, InvalidXLogRecPtr, InvalidXLogRecPtr);
        assert_eq!(r.len(), 4);
        // start_lsn bound is inclusive: a summary ENDING exactly at start_lsn
        // is kept (contrast GetWalSummaries' strict >=).
        let r = FilterWalSummaries(mcx, &list, 0, 0x1800, InvalidXLogRecPtr);
        assert_eq!(r.len(), 4);
        let r = FilterWalSummaries(mcx, &list, 0, 0x1801, InvalidXLogRecPtr);
        assert_eq!(&r[0], &ws(1, 0x1800, 0x2000));
        assert_eq!(r.len(), 3);
        // end_lsn bound is inclusive: a summary STARTING exactly at end_lsn is kept.
        let r = FilterWalSummaries(mcx, &list, 0, InvalidXLogRecPtr, 0x2800);
        assert_eq!(r.len(), 4);
        let r = FilterWalSummaries(mcx, &list, 0, InvalidXLogRecPtr, 0x27ff);
        assert_eq!(r.len(), 3);
    }
}

// GetLatestLSN's recovery arm: C takes max(GetWalRcvFlushRecPtr, replay) —
// flushed-but-unreplayed WAL on a streaming standby advances the summarizer.
#[test]
fn latest_lsn_prefers_further_ahead_flush() {
    // Flush ahead of replay: flush wins, with the flush TLI.
    assert_eq!(
        latest_lsn_from_flush_and_replay((0x2000, 2), (0x1000, 1)),
        (0x2000, 2)
    );
    // Replay ahead (or equal): replay wins, with the replay TLI.
    assert_eq!(
        latest_lsn_from_flush_and_replay((0x1000, 2), (0x3000, 1)),
        (0x3000, 1)
    );
    assert_eq!(
        latest_lsn_from_flush_and_replay((0x1000, 2), (0x1000, 1)),
        (0x1000, 1)
    );
    // No walreceiver: invalid flush LSN reduces to the replay position.
    assert_eq!(latest_lsn_from_flush_and_replay((0, 0), (0x1000, 1)), (0x1000, 1));
}

// ---------------------------------------------------------------------------
// wal_summary_contents_rows_in — the data half of the pg_wal_summary_contents
// SRF (walsummaryfuncs.c): per relation fork, limit_block row first (when
// valid), then modified blocks in blkreftable reader order (NOT sorted).
// ---------------------------------------------------------------------------

mod contents_rows {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Once;

    use super::super::*;
    use walsummarizer_seams::WalSummaryContentsRow;

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
            "pgrust_walsummary_srf_test_{}_{n}/summaries",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_str().unwrap().to_owned()
    }

    fn rl(spc: u32, db: u32, rel: u32) -> RelFileLocator {
        RelFileLocator { spcOid: spc, dbOid: db, relNumber: rel }
    }

    /// Serialize `tab` under the summary filename for (tli, start, end),
    /// byte-identical to what the WAL summarizer persists.
    fn write_summary_file(
        dir: &str,
        tli: TimeLineID,
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        tab: &blkreftable::BlockRefTable<'_>,
    ) {
        let mut bytes: Vec<u8> = Vec::new();
        tab.write(|chunk: &[u8]| {
            bytes.extend_from_slice(chunk);
            Ok(())
        })
        .unwrap();
        let name = format!(
            "{dir}/{:08X}{:08X}{:08X}{:08X}{:08X}.summary",
            tli,
            (start_lsn >> 32) as u32,
            start_lsn as u32,
            (end_lsn >> 32) as u32,
            end_lsn as u32
        );
        std::fs::write(name, bytes).unwrap();
    }

    #[test]
    fn contents_rows_match_c_emission_order() {
        fd_setup();
        let dir = scratch_summaries_dir();
        let cx = MemoryContext::new("contents-test");
        let mcx = cx.mcx();

        let mut tab = blkreftable::BlockRefTable::new(mcx);
        // Blocks marked out of order: reader order (array chunk insertion
        // order) is the SRF's row order, exactly like C — no sorting here.
        tab.mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, 3).unwrap();
        tab.mark_block_modified(rl(1663, 5, 16384), ForkNumber::MAIN_FORKNUM, 1).unwrap();
        // A second fork with a valid limit block and no modified blocks.
        tab.set_limit_block(rl(1663, 5, 16385), ForkNumber::VISIBILITYMAP_FORKNUM, 12);
        write_summary_file(&dir, 1, 0x1000, 0x2000, &tab);

        let rows = wal_summary_contents_rows_in(&dir, 1, 0x1000, 0x2000).unwrap();
        let row = |rel: u32, fork: i16, blk: i64, limit: bool| WalSummaryContentsRow {
            relfilenode: rel,
            reltablespace: 1663,
            reldatabase: 5,
            relforknumber: fork,
            relblocknumber: blk,
            is_limit_block: limit,
        };
        assert_eq!(
            rows,
            vec![
                // rel 16384 MAIN: no limit row (InvalidBlockNumber), blocks
                // in insertion order 3 then 1.
                row(16384, 0, 3, false),
                row(16384, 0, 1, false),
                // rel 16385 VM: only the limit row.
                row(16385, 2, 12, true),
            ]
        );
    }

    #[test]
    fn contents_rows_missing_file_is_c_open_error() {
        fd_setup();
        let dir = scratch_summaries_dir();
        let err = wal_summary_contents_rows_in(&dir, 7, 0x7000, 0x8000).err().unwrap();
        assert!(
            err.message().starts_with("could not open file \""),
            "{}",
            err.message()
        );
    }
}
