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
