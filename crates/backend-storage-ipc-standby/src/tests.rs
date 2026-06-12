//! Unit tests for the in-crate (non-seam) logic of `standby.c`.

use super::*;

#[test]
fn timestamp_plus_milliseconds_matches_c_macro() {
    // TimestampTzPlusMilliseconds(tz, ms) == tz + ms*1000.
    assert_eq!(timestamp_tz_plus_milliseconds(0, 30_000), 30_000_000);
    assert_eq!(timestamp_tz_plus_milliseconds(1_000, 5), 6_000);
}

#[test]
fn set_locktag_relation_fields() {
    let tag = set_locktag_relation(1234, 5678);
    assert_eq!(tag.locktag_field1, 1234);
    assert_eq!(tag.locktag_field2, 5678);
    assert_eq!(tag.locktag_field3, 0);
    assert_eq!(tag.locktag_field4, 0);
    assert_eq!(tag.locktag_type, LOCKTAG_RELATION);
    assert_eq!(tag.locktag_lockmethodid, DEFAULT_LOCKMETHOD as u8);
}

#[test]
fn recovery_conflict_descriptions() {
    assert_eq!(
        get_recovery_conflict_desc(ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK),
        "recovery conflict on lock"
    );
    assert_eq!(
        get_recovery_conflict_desc(ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_SNAPSHOT),
        "recovery conflict on snapshot"
    );
    assert_eq!(
        get_recovery_conflict_desc(ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_BUFFERPIN),
        "recovery conflict on buffer pin"
    );
    assert_eq!(
        get_recovery_conflict_desc(ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK),
        "recovery conflict on buffer deadlock"
    );
    // Not a recovery conflict => "unknown reason".
    assert_eq!(
        get_recovery_conflict_desc(ProcSignalReason::PROCSIG_CATCHUP_INTERRUPT),
        "unknown reason"
    );
}

#[test]
fn timeout_handlers_set_flags() {
    GOT_STANDBY_DEADLOCK_TIMEOUT.set(false);
    StandbyDeadLockHandler();
    assert!(GOT_STANDBY_DEADLOCK_TIMEOUT.get());
    GOT_STANDBY_DEADLOCK_TIMEOUT.set(false);

    GOT_STANDBY_DELAY_TIMEOUT.set(false);
    StandbyTimeoutHandler();
    assert!(GOT_STANDBY_DELAY_TIMEOUT.get());
    GOT_STANDBY_DELAY_TIMEOUT.set(false);

    GOT_STANDBY_LOCK_TIMEOUT.set(false);
    StandbyLockTimeoutHandler();
    assert!(GOT_STANDBY_LOCK_TIMEOUT.get());
    GOT_STANDBY_LOCK_TIMEOUT.set(false);
}

#[test]
fn guc_accessors_roundtrip() {
    assert_eq!(max_standby_archive_delay(), 30 * 1000);
    assert_eq!(max_standby_streaming_delay(), 30 * 1000);
    assert!(!log_recovery_conflict_waits());
    set_max_standby_archive_delay(-1);
    set_max_standby_streaming_delay(0);
    set_log_recovery_conflict_waits(true);
    assert_eq!(max_standby_archive_delay(), -1);
    assert_eq!(max_standby_streaming_delay(), 0);
    assert!(log_recovery_conflict_waits());
    set_max_standby_archive_delay(30 * 1000);
    set_max_standby_streaming_delay(30 * 1000);
    set_log_recovery_conflict_waits(false);
}

#[test]
fn parse_standby_locks_record() {
    // xl_standby_locks { nlocks = 2 } + two xl_standby_lock entries.
    let mut data = Vec::new();
    data.extend_from_slice(&2i32.to_ne_bytes());
    for (xid, db, rel) in [(100u32, 5u32, 17u32), (200, 0, 42)] {
        data.extend_from_slice(&xid.to_ne_bytes());
        data.extend_from_slice(&db.to_ne_bytes());
        data.extend_from_slice(&rel.to_ne_bytes());
    }
    let parsed = parse_xl_standby_locks(&data).unwrap();
    assert_eq!(
        parsed.locks,
        vec![
            xl_standby_lock { xid: 100, dbOid: 5, relOid: 17 },
            xl_standby_lock { xid: 200, dbOid: 0, relOid: 42 },
        ]
    );
}

#[test]
fn parse_running_xacts_record() {
    let mut data = Vec::new();
    data.extend_from_slice(&2i32.to_ne_bytes()); // xcnt
    data.extend_from_slice(&1i32.to_ne_bytes()); // subxcnt
    data.push(1); // subxid_overflow
    data.extend_from_slice(&[0u8; 3]);
    data.extend_from_slice(&1000u32.to_ne_bytes()); // nextXid
    data.extend_from_slice(&900u32.to_ne_bytes()); // oldestRunningXid
    data.extend_from_slice(&999u32.to_ne_bytes()); // latestCompletedXid
    for xid in [901u32, 902, 903] {
        data.extend_from_slice(&xid.to_ne_bytes());
    }
    let parsed = parse_xl_running_xacts(&data).unwrap();
    assert_eq!(parsed.xcnt, 2);
    assert_eq!(parsed.subxcnt, 1);
    assert!(parsed.subxid_overflow);
    assert_eq!(parsed.nextXid, 1000);
    assert_eq!(parsed.oldestRunningXid, 900);
    assert_eq!(parsed.latestCompletedXid, 999);
    assert_eq!(parsed.xids, vec![901, 902, 903]);
}

#[test]
fn parse_invalidations_record() {
    let mut data = Vec::new();
    data.extend_from_slice(&7u32.to_ne_bytes()); // dbId
    data.extend_from_slice(&1663u32.to_ne_bytes()); // tsId
    data.push(1); // relcacheInitFileInval
    data.extend_from_slice(&[0u8; 3]);
    data.extend_from_slice(&1i32.to_ne_bytes()); // nmsgs
    data.extend_from_slice(&[0xABu8; SHARED_INVALIDATION_MESSAGE_SIZE]);
    let parsed = parse_xl_invalidations(&data).unwrap();
    assert_eq!(parsed.dbId, 7);
    assert_eq!(parsed.tsId, 1663);
    assert!(parsed.relcacheInitFileInval);
    assert_eq!(parsed.msgs.len(), 1);
    assert_eq!(parsed.msgs[0].raw, [0xAB; SHARED_INVALIDATION_MESSAGE_SIZE]);
}

#[test]
fn parse_too_short_record_errors() {
    assert!(parse_xl_standby_locks(&[0u8; 2]).is_err());
    assert!(parse_xl_running_xacts(&[0u8; 10]).is_err());
    assert!(parse_xl_invalidations(&[0u8; 10]).is_err());
}
