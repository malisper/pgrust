use super::*;

#[test]
fn text_datum_round_trip_keeps_name_exact() {
    // Regression: text_datum_to_string glued the 4-byte varlena header onto
    // every roname (visible to send_repl_origin and
    // pg_show_replication_origin_status).
    let ctx = mcx::MemoryContext::new("test");
    let mcx = ctx.mcx();
    for name in ["test_origin", "pg_16400", &"n".repeat(300)] {
        let d = text_datum(mcx, name).unwrap();
        assert_eq!(text_datum_to_string(mcx, d).unwrap(), name);
    }
}

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

#[test]
fn replorigin_redo_parse_rejects_truncated_records() {
    // Truncated / attacker-authored REPLORIGIN WAL must fail through the
    // ordinary corruption error path, not panic with a slice-out-of-bounds.

    // Valid full-size records still parse exactly as before.
    let set = serialize_replorigin_set(7, 0xDEAD_BEEF, true);
    assert_eq!(parse_replorigin_set(&set).unwrap(), (7, 0xDEAD_BEEF, true));
    let drop = serialize_replorigin_drop(9);
    assert_eq!(parse_replorigin_drop(&drop).unwrap(), 9);

    // Every truncation of a SET record (including empty) returns Err, no panic.
    for len in 0..XL_REPLORIGIN_SET_SIZE {
        assert!(
            parse_replorigin_set(&set[..len]).is_err(),
            "SET len {len} should be rejected"
        );
    }
    // Same for DROP.
    for len in 0..XL_REPLORIGIN_DROP_SIZE {
        assert!(
            parse_replorigin_drop(&drop[..len]).is_err(),
            "DROP len {len} should be rejected"
        );
    }
}

// audit-18.6 b219: origin.c:557 ReplicationOriginShmemInit registers the
// ReplicationStateCtl array as ShmemInitStruct("ReplicationOriginState",
// ReplicationOriginShmemSize()) — offsetof(ReplicationStateCtl, states) = 8
// plus max_active_replication_origins (default 10) x sizeof(ReplicationState)
// = 56 -> 568 bytes at 18.6 (x86-64 and aarch64 Linux alike) — so a re-entry
// finds it (C's `found`) and pg_shmem_allocations lists it.
#[test]
fn shmem_init_registers_replication_origin_state_in_shmem_index() {
    assert_eq!(max_active_replication_origins(), 10);
    assert_eq!(
        ReplicationOriginShmemSize().unwrap(),
        568,
        "origin.c:534 offsetof(ReplicationStateCtl, states) + 10 * sizeof(ReplicationState)"
    );
    ReplicationOriginShmemInit().unwrap();
    // origin.c:566 `found`: a re-entry reuses the block, never re-boots the array.
    ReplicationOriginShmemInit().unwrap();
    let (_, found) = shmem::ShmemInitStruct("ReplicationOriginState", 568).unwrap();
    assert!(
        found,
        "ReplicationOriginShmemInit must register \"ReplicationOriginState\" (568 bytes) in the ShmemIndex"
    );
}
