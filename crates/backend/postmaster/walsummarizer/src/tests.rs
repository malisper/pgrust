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
fn diff_ms_rounds_up_and_clamps() {
    assert_eq!(diff_ms(0, 0), 0);
    assert_eq!(diff_ms(10, 5), 0);
    assert_eq!(diff_ms(0, 1), 1);
    assert_eq!(diff_ms(0, 1000), 1);
    assert_eq!(diff_ms(0, 10_000_000), 10_000);
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
