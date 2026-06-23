//! Unit tests for the node-independent logic of the `async.c` port: the
//! queue-position helpers, `asyncQueueAdvance`, the entry serialization
//! round-trip, the notification hash/match dedup, the transam macros, and the
//! `Async_Notify` argument-validation error paths (with the seams the validation
//! touches installed).

use super::*;
use ::types_async::QueuePosition;

fn pos(page: i64, offset: i32) -> QueuePosition {
    QueuePosition { page, offset }
}

#[test]
fn queue_pos_equal_and_zero() {
    assert!(QUEUE_POS_EQUAL(pos(0, 0), pos(0, 0)));
    assert!(!QUEUE_POS_EQUAL(pos(1, 0), pos(0, 0)));
    assert!(!QUEUE_POS_EQUAL(pos(0, 4), pos(0, 0)));
    assert!(QUEUE_POS_IS_ZERO(pos(0, 0)));
    assert!(!QUEUE_POS_IS_ZERO(pos(0, 4)));
    assert!(!QUEUE_POS_IS_ZERO(pos(1, 0)));
}

#[test]
fn queue_pos_min_max_match_c_ternary() {
    assert_eq!(QUEUE_POS_MIN(pos(1, 0), pos(2, 0)), pos(1, 0));
    assert_eq!(QUEUE_POS_MAX(pos(1, 0), pos(2, 0)), pos(2, 0));
    assert_eq!(QUEUE_POS_MIN(pos(3, 8), pos(3, 4)), pos(3, 4));
    assert_eq!(QUEUE_POS_MAX(pos(3, 8), pos(3, 4)), pos(3, 8));
    assert_eq!(QUEUE_POS_MIN(pos(3, 4), pos(3, 4)), pos(3, 4));
    assert_eq!(QUEUE_POS_MAX(pos(3, 4), pos(3, 4)), pos(3, 4));
}

#[test]
fn page_diff_and_precedes() {
    assert_eq!(asyncQueuePageDiff(10, 3), 7);
    assert_eq!(asyncQueuePageDiff(3, 10), -7);
    assert!(asyncQueuePagePrecedes(3, 10));
    assert!(!asyncQueuePagePrecedes(10, 3));
    assert!(!asyncQueuePagePrecedes(5, 5));
}

#[test]
fn advance_stays_on_page_when_room() {
    let mut p = pos(2, 0);
    let jumped = asyncQueueAdvance(&mut p, 32);
    assert!(!jumped);
    assert_eq!(p, pos(2, 32));
}

#[test]
fn advance_jumps_to_next_page_when_full() {
    let near_end = (QUEUE_PAGESIZE - QUEUEALIGN(AsyncQueueEntryEmptySize)) as i32;
    let mut p = pos(5, near_end);
    let jumped = asyncQueueAdvance(&mut p, 4);
    assert!(jumped);
    assert_eq!(p, pos(6, 0));
}

#[test]
fn entry_to_bytes_layout_matches_field_offsets() {
    let mut qe = blank_entry();
    qe.length = 24;
    qe.dboid = 0x1122_3344;
    qe.xid = 0x5566_7788;
    qe.srcPid = 0x0099_00AA;
    qe.data[0] = b'c';
    qe.data[1] = 0;
    qe.data[2] = b'p';
    qe.data[3] = 0;
    let buf = entry_to_bytes(&qe);
    assert_eq!(read_i32(&buf, 0), 24);
    assert_eq!(read_u32(&buf, 4), 0x1122_3344);
    assert_eq!(read_u32(&buf, 8), 0x5566_7788);
    assert_eq!(read_i32(&buf, 12), 0x0099_00AA);
    assert_eq!(&buf[16..20], &[b'c', 0, b'p', 0]);
}

#[test]
fn cstr_from_reads_until_nul() {
    let buf = b"hello\0world\0".to_vec();
    assert_eq!(cstr_from(&buf, 0), "hello");
    assert_eq!(cstr_from(&buf, 6), "world");
    let buf2 = b"abc".to_vec();
    assert_eq!(cstr_from(&buf2, 0), "abc");
}

fn make_notif(channel: &str, payload: &str) -> Notification {
    let cl = channel.len();
    let pl = payload.len();
    let mut data = vec![0u8; cl + pl + 2];
    data[..cl].copy_from_slice(channel.as_bytes());
    data[cl] = 0;
    data[cl + 1..cl + 1 + pl].copy_from_slice(payload.as_bytes());
    data[cl + 1 + pl] = 0;
    Notification {
        channel_len: cl as u16,
        payload_len: pl as u16,
        data,
    }
}

#[test]
fn match_is_byte_identical_compare() {
    let a = make_notif("ch", "pay");
    let b = make_notif("ch", "pay");
    let c = make_notif("ch", "other");
    let d = make_notif("xx", "pay");
    let ks = core::mem::size_of::<usize>();
    assert_eq!(notification_match(&a, &b, ks), 0);
    assert_eq!(notification_match(&a, &c, ks), 1);
    assert_eq!(notification_match(&a, &d, ks), 1);
}

#[test]
fn hash_equal_for_equal_payloads_stable() {
    let a = make_notif("ch", "pay");
    let b = make_notif("ch", "pay");
    let ks = core::mem::size_of::<usize>();
    assert_eq!(notification_hash(&a, ks), notification_hash(&b, ks));
}

#[test]
fn hashtable_find_and_enter_dedup() {
    let events = vec![make_notif("a", "1"), make_notif("b", "2")];
    let mut ht = NotificationHashTable::new();
    assert!(!ht.enter(&events, &events[0], 0));
    assert!(!ht.enter(&events, &events[1], 1));
    let dup = make_notif("a", "1");
    assert!(ht.find(&events, &dup));
    let absent = make_notif("z", "9");
    assert!(!ht.find(&events, &absent));
}

#[test]
fn transaction_id_is_normal_and_precedes() {
    assert!(!TransactionIdIsNormal(0)); // Invalid
    assert!(!TransactionIdIsNormal(2)); // Frozen
    assert!(TransactionIdIsNormal(3)); // FirstNormal
    assert!(TransactionIdPrecedes(0, 1));
    assert!(TransactionIdPrecedes(5, 6));
    assert!(!TransactionIdPrecedes(6, 5));
}

// --- Async_Notify validation paths (real seams installed) --------------------

fn install_validation_seams() {
    // These ::set calls are idempotent across this crate's test binary because
    // the seam slots are process-local and re-set is permitted in tests via the
    // try-install path; install once per test process.
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        xact_seams::get_current_transaction_nest_level::set(|| 1);
        parallel_seams::is_parallel_worker::set(|| false);
    });
}

#[test]
fn async_notify_rejects_empty_channel() {
    install_validation_seams();
    let err = Async_Notify("", Some("p")).unwrap_err();
    assert_eq!(err.message(), "channel name cannot be empty");
}

#[test]
fn async_notify_rejects_overlong_channel() {
    install_validation_seams();
    let long = "x".repeat(NAMEDATALEN);
    let err = Async_Notify(&long, Some("p")).unwrap_err();
    assert_eq!(err.message(), "channel name too long");
}

#[test]
fn async_notify_rejects_overlong_payload() {
    install_validation_seams();
    let long = "y".repeat(NOTIFY_PAYLOAD_MAX_LENGTH);
    let err = Async_Notify("ch", Some(&long)).unwrap_err();
    assert_eq!(err.message(), "payload string too long");
}
