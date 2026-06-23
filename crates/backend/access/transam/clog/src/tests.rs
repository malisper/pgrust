//! Unit tests for clog's pure page/byte/bit arithmetic and page-precedes
//! ordering (the parts that need no SLRU shared state or installed seams).

use super::*;

#[test]
fn page_size_constants_match_c() {
    // BLCKSZ * CLOG_XACTS_PER_BYTE, four xacts per byte, two bits each.
    assert_eq!(CLOG_BITS_PER_XACT, 2);
    assert_eq!(CLOG_XACTS_PER_BYTE, 4);
    assert_eq!(CLOG_XACTS_PER_PAGE, BLCKSZ as u32 * 4);
    assert_eq!(CLOG_XACT_BITMASK, 0b11);
    // CLOG_LSNS_PER_PAGE = CLOG_XACTS_PER_PAGE / 32.
    assert_eq!(CLOG_LSNS_PER_PAGE, (CLOG_XACTS_PER_PAGE / 32) as i32);
}

#[test]
fn page_byte_bit_arithmetic() {
    let per_page = CLOG_XACTS_PER_PAGE;
    // xid 0 is on page 0, byte 0, bindex 0.
    assert_eq!(TransactionIdToPage(0), 0);
    assert_eq!(TransactionIdToPgIndex(0), 0);
    assert_eq!(TransactionIdToByte(0), 0);
    assert_eq!(TransactionIdToBIndex(0), 0);

    // The first xid of page 1.
    let x = per_page;
    assert_eq!(TransactionIdToPage(x), 1);
    assert_eq!(TransactionIdToPgIndex(x), 0);

    // Four xacts per byte: xids 0..=3 share byte 0, xid 4 starts byte 1.
    assert_eq!(TransactionIdToByte(3), 0);
    assert_eq!(TransactionIdToBIndex(3), 3);
    assert_eq!(TransactionIdToByte(4), 1);
    assert_eq!(TransactionIdToBIndex(4), 0);
}

#[test]
fn lsn_index_layout() {
    // Slot stride is CLOG_LSNS_PER_PAGE; within a page the index advances every
    // CLOG_XACTS_PER_LSN_GROUP xids.
    assert_eq!(GetLSNIndex(0, 0), 0);
    assert_eq!(GetLSNIndex(0, CLOG_XACTS_PER_LSN_GROUP - 1), 0);
    assert_eq!(GetLSNIndex(0, CLOG_XACTS_PER_LSN_GROUP), 1);
    assert_eq!(GetLSNIndex(1, 0), CLOG_LSNS_PER_PAGE as usize);
}

#[test]
fn clog_page_precedes_ordering() {
    // Adjacent pages: page 0 precedes page 1.
    assert!(CLOGPagePrecedes(0, 1));
    assert!(!CLOGPagePrecedes(1, 0));
    // A page never precedes itself.
    assert!(!CLOGPagePrecedes(5, 5));
}

#[test]
fn max_allowed_buffers_capped_by_slru() {
    // clog caps its buffer count below the SLRU maximum.
    assert!(clog_max_allowed_buffers() <= SLRU_MAX_ALLOWED_BUFFERS);
    assert!(clog_max_allowed_buffers() > 0);
}
