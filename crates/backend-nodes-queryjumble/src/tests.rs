//! Unit tests for the JumbleState core (the `AppendJumble`/`RecordConstLocation`
//! primitives). The full `_jumbleNode` walker is exercised end-to-end by the
//! guc.sql regression test (queryId reported into pg_stat_activity); here we
//! prove the buffer/location bookkeeping matches queryjumblefuncs.c.

use crate::state::{JumbleState, JUMBLE_SIZE};

#[test]
fn append_grows_buffer() {
    let mut j = JumbleState::new();
    j.append_jumble(&1u32.to_ne_bytes());
    j.append_jumble(&2u64.to_ne_bytes());
    assert_eq!(j.jumble.len(), 12);
}

#[test]
fn buffer_collapses_to_hash_when_full() {
    // Append more than JUMBLE_SIZE bytes; the buffer must collapse to the
    // 8-byte running hash plus the overflow, never exceeding JUMBLE_SIZE while
    // an item is being placed.
    let mut j = JumbleState::new();
    let chunk = [0xABu8; 64];
    for _ in 0..40 {
        j.append_jumble(&chunk); // 2560 bytes total > JUMBLE_SIZE (1024)
    }
    assert!(j.jumble.len() <= JUMBLE_SIZE);
    assert!(!j.jumble.is_empty());
}

#[test]
fn pending_nulls_flush_before_value() {
    let mut j = JumbleState::new();
    j.append_jumble_null();
    j.append_jumble_null();
    assert_eq!(j.pending_nulls, 2);
    // A real append flushes the 2 pending nulls first (4 bytes) then the value.
    j.append_jumble(&7u32.to_ne_bytes());
    assert_eq!(j.pending_nulls, 0);
    assert_eq!(j.jumble.len(), 8); // 4 (null count) + 4 (value)
}

#[test]
fn record_const_location_single_and_squashed() {
    let mut j = JumbleState::new();
    // Single constant (len == -1).
    j.record_const_location(false, 10, -1);
    // Negative location is ignored (unknown).
    j.record_const_location(false, -1, -1);
    // Squashed list (len > -1).
    j.record_const_location(false, 20, 5);
    assert_eq!(j.clocations.len(), 2);
    assert_eq!(j.clocations[0].location, 10);
    assert_eq!(j.clocations[0].length, -1);
    assert!(!j.clocations[0].squashed);
    assert_eq!(j.clocations[1].location, 20);
    assert!(j.clocations[1].squashed);
}
