//! In-crate unit tests for the pure (seam-free) logic of the `origin.c` port.
//!
//! The seam-driven entry points (catalog/syscache, lmgr locks, WAL insert,
//! xact/recovery predicates, the named `ReplicationOriginLock`, the SRF
//! plumbing, the checkpoint I/O, and the per-origin LWLock/CV infrastructure)
//! require process-wide fakes for several owners that are not ported yet, so
//! they are exercised by the `/audit-crate` review against C and by the
//! integration smoke once those owners land. These tests cover the logic this
//! crate computes itself: the reserved-name predicates and the WAL-record
//! decoders.

use super::*;

#[test]
fn reserved_origin_names_match_pg_strcasecmp() {
    // IsReservedOriginName is case-insensitive over "none"/"any".
    assert!(IsReservedOriginName("none"));
    assert!(IsReservedOriginName("NONE"));
    assert!(IsReservedOriginName("any"));
    assert!(IsReservedOriginName("AnY"));
    assert!(!IsReservedOriginName("anything"));
    assert!(!IsReservedOriginName("non"));
    assert!(!IsReservedOriginName("regress_origin"));
}

#[test]
fn reserved_name_matches_pg_prefix() {
    assert!(IsReservedName("pg_"));
    assert!(IsReservedName("pg_origin"));
    assert!(!IsReservedName("regress_origin"));
    assert!(!IsReservedName("p"));
    assert!(!IsReservedName("Pg_origin")); // case-sensitive strncmp
}

#[test]
fn decode_set_matches_c_struct_layout() {
    // xl_replorigin_set { XLogRecPtr remote_lsn@0; RepOriginId node_id@8; bool force@10; }
    let mut data = [0u8; 11];
    data[0..8].copy_from_slice(&0x0102_0304_0506_0708u64.to_ne_bytes());
    data[8..10].copy_from_slice(&0xABCDu16.to_ne_bytes());
    data[10] = 1;
    let x = decode_replorigin_set(&data).unwrap();
    assert_eq!(x.remote_lsn, 0x0102_0304_0506_0708);
    assert_eq!(x.node_id, 0xABCD);
    assert!(x.force);

    // A short record is the corrupt-record PANIC path.
    assert!(decode_replorigin_set(&data[..10]).is_err());
}

#[test]
fn decode_drop_matches_c_struct_layout() {
    // xl_replorigin_drop { RepOriginId node_id@0; }
    let data = 0x1234u16.to_ne_bytes();
    let x = decode_replorigin_drop(&data).unwrap();
    assert_eq!(x.node_id, 0x1234);

    assert!(decode_replorigin_drop(&data[..1]).is_err());
}

#[test]
fn header_constants_match_c() {
    assert_eq!(XLOG_REPLORIGIN_SET, 0x00);
    assert_eq!(XLOG_REPLORIGIN_DROP, 0x10);
    assert_eq!(InvalidRepOriginId, 0);
    assert_eq!(DoNotReplicateId, u16::MAX);
    assert_eq!(MAX_RONAME_LEN, 512);
    assert_eq!(REPLICATION_STATE_MAGIC, 0x1257_DADE);
    assert_eq!(RM_REPLORIGIN_ID, 19);
    assert_eq!(WAIT_EVENT_REPLICATION_ORIGIN_DROP, 0x0800_0000 | 0x30);
}
