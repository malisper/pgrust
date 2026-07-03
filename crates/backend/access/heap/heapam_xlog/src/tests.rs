use super::*;

// The write side (heapam::dml) keeps its own copies of the WAL shape
// constants; divergence would silently corrupt replay.
#[test]
fn wal_constants_match_write_side() {
    assert_eq!(XLOG_HEAP_INSERT, heapam::dml::XLOG_HEAP_INSERT);
    assert_eq!(XLOG_HEAP_DELETE, heapam::dml::XLOG_HEAP_DELETE);
    assert_eq!(XLOG_HEAP_UPDATE, heapam::dml::XLOG_HEAP_UPDATE);
    assert_eq!(XLOG_HEAP_HOT_UPDATE, heapam::dml::XLOG_HEAP_HOT_UPDATE);
    assert_eq!(XLOG_HEAP_LOCK, heapam::dml::XLOG_HEAP_LOCK);
    assert_eq!(XLOG_HEAP_INIT_PAGE, heapam::dml::XLOG_HEAP_INIT_PAGE);
    assert_eq!(XLOG_HEAP_INPLACE, heapam::dml::XLOG_HEAP_INPLACE);
    assert_eq!(XLH_INSERT_ALL_VISIBLE_CLEARED, heapam::dml::XLH_INSERT_ALL_VISIBLE_CLEARED);
    assert_eq!(
        XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED,
        heapam::dml::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED
    );
    assert_eq!(
        XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED,
        heapam::dml::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED
    );
    assert_eq!(XLH_LOCK_ALL_FROZEN_CLEARED, heapam::dml::XLH_LOCK_ALL_FROZEN_CLEARED);
    assert_eq!(XLH_DELETE_ALL_VISIBLE_CLEARED, heapam::dml::XLH_DELETE_ALL_VISIBLE_CLEARED);
    assert_eq!(XLH_DELETE_IS_PARTITION_MOVE, heapam::dml::XLH_DELETE_IS_PARTITION_MOVE);
    assert_eq!(XLHL_XMAX_IS_MULTI, heapam::dml::XLHL_XMAX_IS_MULTI);
    assert_eq!(XLHL_XMAX_LOCK_ONLY, heapam::dml::XLHL_XMAX_LOCK_ONLY);
    assert_eq!(XLHL_XMAX_EXCL_LOCK, heapam::dml::XLHL_XMAX_EXCL_LOCK);
    assert_eq!(XLHL_XMAX_KEYSHR_LOCK, heapam::dml::XLHL_XMAX_KEYSHR_LOCK);
    assert_eq!(XLHL_KEYS_UPDATED, heapam::dml::XLHL_KEYS_UPDATED);
}

#[test]
fn fix_infomask_from_infobits_bit_mapping() {
    let (mut im, mut im2) = (0u16, 0u16);
    fix_infomask_from_infobits(XLHL_XMAX_EXCL_LOCK | XLHL_KEYS_UPDATED, &mut im, &mut im2);
    assert_eq!(im, HEAP_XMAX_EXCL_LOCK);
    assert_eq!(im2, HEAP_KEYS_UPDATED);

    let (mut im, mut im2) = (HEAP_XMAX_EXCL_LOCK, HEAP_KEYS_UPDATED);
    fix_infomask_from_infobits(XLHL_XMAX_IS_MULTI | XLHL_XMAX_LOCK_ONLY | XLHL_XMAX_KEYSHR_LOCK, &mut im, &mut im2);
    assert_eq!(im, HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK);
    assert_eq!(im2, 0);
}
