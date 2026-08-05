use super::*;

#[test]
fn disk_state_layout_matches_c() {
    // ReplicationStateOnDisk: RepOriginId @0, XLogRecPtr @8, sizeof 16.
    let b = serialize_disk_state(0x1234, 0x0102030405060708);
    assert_eq!(b.len(), 16);
    assert_eq!(u16::from_ne_bytes(b[0..2].try_into().unwrap()), 0x1234);
    assert_eq!(u64::from_ne_bytes(b[8..16].try_into().unwrap()), 0x0102030405060708);
}

#[test]
fn replorigin_set_record_layout() {
    let b = serialize_replorigin_set(7, 0xDEAD_BEEF, true);
    assert_eq!(b.len(), 16);
    assert_eq!(u64::from_ne_bytes(b[0..8].try_into().unwrap()), 0xDEAD_BEEF);
    assert_eq!(u16::from_ne_bytes(b[8..10].try_into().unwrap()), 7);
    assert_eq!(b[10], 1);
    assert_eq!(serialize_replorigin_drop(9), 9u16.to_ne_bytes());
}

#[test]
fn checkpoint_image_crc_convention() {
    // The file is MAGIC, states..., CRC32C(all prior bytes), matching C's
    // COMP_CRC32C accumulation order.
    let magic = REPLICATION_STATE_MAGIC.to_ne_bytes();
    let s1 = serialize_disk_state(3, 0x1000);
    let s2 = serialize_disk_state(9, 0x2000);
    let mut crc = crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &magic);
    crc = crc32c::pg_comp_crc32c(crc, &s1);
    crc = crc32c::pg_comp_crc32c(crc, &s2);
    let crc = crc32c::fin_crc32c(crc);

    // Startup's reader accumulates the same way: magic then each state.
    let mut rcrc = crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &magic);
    for st in [&s1, &s2] {
        rcrc = crc32c::pg_comp_crc32c(rcrc, &st[..]);
    }
    assert_eq!(crc, crc32c::fin_crc32c(rcrc));
}
