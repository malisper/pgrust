use crate::*;

// standbydefs.h: xcnt 0, subxcnt 4, overflow 8, nextXid 12, oldest 16, latestCompleted 20.
#[test]
fn running_xacts_header_matches_c_layout() {
    let xids = [900u32, 905];
    let running = procarray::RunningTransactions {
        xids: &xids,
        xcnt: 1,
        subxcnt: 1,
        subxid_overflow: false,
        next_xid: 910,
        oldest_running_xid: 900,
        latest_completed_xid: 899,
        oldest_database_running_xid: 900,
    };
    let hdr = running_xacts_header(&running);
    assert_eq!(hdr.len(), 24);
    assert_eq!(i32::from_ne_bytes(hdr[0..4].try_into().unwrap()), 1);
    assert_eq!(i32::from_ne_bytes(hdr[4..8].try_into().unwrap()), 1);
    assert_eq!(hdr[8], 0);
    assert_eq!(&hdr[9..12], &[0, 0, 0]);
    assert_eq!(u32::from_ne_bytes(hdr[12..16].try_into().unwrap()), 910);
    assert_eq!(u32::from_ne_bytes(hdr[16..20].try_into().unwrap()), 900);
    assert_eq!(u32::from_ne_bytes(hdr[20..24].try_into().unwrap()), 899);

    let overflowed = procarray::RunningTransactions {
        subxid_overflow: true,
        subxcnt: 0,
        ..running
    };
    assert_eq!(running_xacts_header(&overflowed)[8], 1);
}

// standbydefs.h xl_standby_lock: xid 0, dbOid 4, relOid 8; 12 bytes each.
#[test]
fn standby_lock_body_matches_c_layout() {
    let locks = [
        xl_standby_lock { xid: 700, dbOid: 5, relOid: 16384 },
        xl_standby_lock { xid: 701, dbOid: 5, relOid: 16385 },
    ];
    let body = standby_locks_body(&locks);
    assert_eq!(body.len(), 2 * SIZE_OF_XL_STANDBY_LOCK);
    assert_eq!(u32::from_ne_bytes(body[0..4].try_into().unwrap()), 700);
    assert_eq!(u32::from_ne_bytes(body[4..8].try_into().unwrap()), 5);
    assert_eq!(u32::from_ne_bytes(body[8..12].try_into().unwrap()), 16384);
    assert_eq!(u32::from_ne_bytes(body[12..16].try_into().unwrap()), 701);
    assert_eq!(u32::from_ne_bytes(body[20..24].try_into().unwrap()), 16385);
}
