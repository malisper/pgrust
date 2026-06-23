use super::*;

#[test]
fn lsn_format_matches_pg() {
    // %X/%X, uppercase hex, no zero padding (LSN_FORMAT_ARGS).
    assert_eq!(lsn_format(0), "0/0");
    assert_eq!(lsn_format(0x1_0000_0000), "1/0");
    assert_eq!(lsn_format(0x0000_0000_ABCD_EF01), "0/ABCDEF01");
    assert_eq!(lsn_format(0x1234_5678_9ABC_DEF0), "12345678/9ABCDEF0");
}

#[test]
fn xlbyte_to_seg_and_back() {
    let seg_sz = 16 * 1024 * 1024; // 16MB
    // restart_lsn in the 3rd segment.
    let lsn = 2 * (seg_sz as u64) + 1234;
    assert_eq!(xlbyte_to_seg(lsn, seg_sz), 2);
    assert_eq!(xlog_segno_offset_to_rec_ptr(2, 0, seg_sz), 2 * (seg_sz as u64));
}

#[test]
fn mb_var_to_segs() {
    let seg_sz = 16 * 1024 * 1024; // 16MB -> 1 seg per 16 MB
    assert_eq!(xlog_mb_var_to_segs(64, seg_sz), 4);
    assert_eq!(xlog_mb_var_to_segs(16, seg_sz), 1);
}

#[test]
fn namedata_roundtrip() {
    let nd = namedata_from_str("my_slot");
    assert_eq!(name_str(&nd), "my_slot");
}
