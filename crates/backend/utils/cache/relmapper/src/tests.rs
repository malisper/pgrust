use super::*;

fn reset_state() {
    with_state(|st| *st = RelMapperState::new());
}

/// The on-disk/serialized ABI must match C exactly: RelMapping is 8 bytes,
/// sizeof(RelMapFile) == 4 + 4 + 64*8 + 4 == 524, offsetof(crc) == 520.
#[test]
fn abi_layout_matches_c() {
    assert_eq!(SIZEOF_RELMAPFILE, 4 + 4 + (MAX_MAPPINGS * 8) + 4);
    assert_eq!(SIZEOF_RELMAPFILE, 524);
    assert_eq!(OFFSETOF_RELMAPFILE_CRC, 520);
}

/// encode/decode round-trips a populated map byte-for-byte.
#[test]
fn encode_decode_round_trip() {
    let mut map = empty_relmap_file();
    map.magic = RELMAPPER_FILEMAGIC;
    apply_map_update(&mut map, 1234, 5678, true).unwrap();
    apply_map_update(&mut map, 4321, 8765, true).unwrap();
    map.crc = 0xdead_beef;
    let image = encode_relmapfile(&map);
    assert_eq!(image.len(), SIZEOF_RELMAPFILE);
    let back = decode_relmapfile(&image);
    assert_eq!(back, map);
    assert_eq!(
        u32::from_ne_bytes(image[OFFSETOF_RELMAPFILE_CRC..].try_into().unwrap()),
        0xdead_beef
    );
}

#[test]
fn oid_and_filenumber_lookups() {
    reset_state();
    assert_eq!(RelationMapOidToFilenumber(1234, false), InvalidRelFileNumber);
    assert_eq!(RelationMapFilenumberToOid(5678, false), InvalidOid);

    with_state(|st| {
        apply_map_update(&mut st.local_map, 1234, 5678, true).unwrap();
        apply_map_update(&mut st.shared_map, 1111, 2222, true).unwrap();
    });

    assert_eq!(RelationMapOidToFilenumber(1234, false), 5678);
    assert_eq!(RelationMapFilenumberToOid(5678, false), 1234);
    assert_eq!(RelationMapOidToFilenumber(1111, true), 2222);
    assert_eq!(RelationMapFilenumberToOid(2222, true), 1111);

    // shared lookup must not see the local entry and vice versa.
    assert_eq!(RelationMapOidToFilenumber(1234, true), InvalidRelFileNumber);
    assert_eq!(RelationMapOidToFilenumber(1111, false), InvalidRelFileNumber);

    // Active updates win over the main map.
    with_state(|st| {
        apply_map_update(&mut st.active_local_updates, 1234, 9999, true).unwrap();
    });
    assert_eq!(RelationMapOidToFilenumber(1234, false), 9999);
}

#[test]
fn apply_replaces_existing_and_grows() {
    let mut map = empty_relmap_file();
    apply_map_update(&mut map, 10, 100, true).unwrap();
    apply_map_update(&mut map, 20, 200, true).unwrap();
    assert_eq!(map.num_mappings, 2);
    apply_map_update(&mut map, 10, 111, true).unwrap();
    assert_eq!(map.num_mappings, 2);
    assert_eq!(map.mappings[0].mapfilenumber, 111);
    let err = apply_map_update(&mut map, 30, 300, false).unwrap_err();
    assert!(err
        .message()
        .contains("attempt to apply a mapping to unmapped relation 30"));
    assert_eq!(err.level(), ERROR);
}

#[test]
fn apply_runs_out_of_space() {
    let mut map = empty_relmap_file();
    for i in 0..MAX_MAPPINGS {
        apply_map_update(&mut map, (i + 1) as Oid, (i + 1) as RelFileNumber, true).unwrap();
    }
    assert_eq!(map.num_mappings as usize, MAX_MAPPINGS);
    let err = apply_map_update(&mut map, 9999, 9999, true).unwrap_err();
    assert!(err.message().contains("ran out of space in relation map"));
}

#[test]
fn merge_bulk_applies() {
    let mut target = empty_relmap_file();
    apply_map_update(&mut target, 1, 10, true).unwrap();
    let mut updates = empty_relmap_file();
    apply_map_update(&mut updates, 1, 11, true).unwrap(); // replace
    apply_map_update(&mut updates, 2, 22, true).unwrap(); // add
    merge_map_updates(&mut target, &updates, true).unwrap();
    assert_eq!(target.num_mappings, 2);
    assert_eq!(target.mappings[0].mapfilenumber, 11);
    assert_eq!(target.mappings[1].mapoid, 2);
    assert_eq!(target.mappings[1].mapfilenumber, 22);
}

#[test]
fn remove_mapping_collapses_last_in() {
    reset_state();
    with_state(|st| {
        apply_map_update(&mut st.active_local_updates, 1, 10, true).unwrap();
        apply_map_update(&mut st.active_local_updates, 2, 20, true).unwrap();
        apply_map_update(&mut st.active_local_updates, 3, 30, true).unwrap();
    });
    RelationMapRemoveMapping(1).unwrap();
    with_state(|st| {
        assert_eq!(st.active_local_updates.num_mappings, 2);
        assert_eq!(st.active_local_updates.mappings[0].mapoid, 3);
        assert_eq!(st.active_local_updates.mappings[0].mapfilenumber, 30);
    });
    let err = RelationMapRemoveMapping(999).unwrap_err();
    assert!(err
        .message()
        .contains("could not find temporary mapping for relation 999"));
}

#[test]
fn cci_activates_pending() {
    reset_state();
    with_state(|st| {
        apply_map_update(&mut st.pending_shared_updates, 1, 10, true).unwrap();
        apply_map_update(&mut st.pending_local_updates, 2, 20, true).unwrap();
    });
    AtCCI_RelationMap().unwrap();
    with_state(|st| {
        assert_eq!(st.pending_shared_updates.num_mappings, 0);
        assert_eq!(st.pending_local_updates.num_mappings, 0);
        assert_eq!(st.active_shared_updates.num_mappings, 1);
        assert_eq!(st.active_local_updates.num_mappings, 1);
        assert_eq!(st.active_shared_updates.mappings[0].mapfilenumber, 10);
        assert_eq!(st.active_local_updates.mappings[0].mapfilenumber, 20);
    });
}

#[test]
fn eoxact_abort_drops_all_updates() {
    reset_state();
    with_state(|st| {
        apply_map_update(&mut st.active_shared_updates, 1, 10, true).unwrap();
        apply_map_update(&mut st.active_local_updates, 2, 20, true).unwrap();
        apply_map_update(&mut st.pending_shared_updates, 3, 30, true).unwrap();
        apply_map_update(&mut st.pending_local_updates, 4, 40, true).unwrap();
    });
    AtEOXact_RelationMap(false, false).unwrap();
    with_state(|st| {
        assert_eq!(st.active_shared_updates.num_mappings, 0);
        assert_eq!(st.active_local_updates.num_mappings, 0);
        assert_eq!(st.pending_shared_updates.num_mappings, 0);
        assert_eq!(st.pending_local_updates.num_mappings, 0);
    });
}

#[test]
fn at_prepare_errors_when_map_modified() {
    reset_state();
    AtPrepare_RelationMap().unwrap();
    with_state(|st| apply_map_update(&mut st.active_local_updates, 1, 10, true).unwrap());
    let err = AtPrepare_RelationMap().unwrap_err();
    assert!(err
        .message()
        .contains("cannot PREPARE a transaction that modified relation mapping"));
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    reset_state();
}

#[test]
fn initialize_clears_maps() {
    with_state(|st| {
        st.shared_map.magic = RELMAPPER_FILEMAGIC;
        st.shared_map.num_mappings = 5;
        st.local_map.magic = RELMAPPER_FILEMAGIC;
    });
    RelationMapInitialize();
    with_state(|st| {
        assert_eq!(st.shared_map.magic, 0);
        assert_eq!(st.local_map.magic, 0);
        assert_eq!(st.shared_map.num_mappings, 0);
    });
}

#[test]
fn estimate_space_is_two_relmapfiles() {
    assert_eq!(EstimateRelationMapSpace(), 2 * SIZEOF_RELMAPFILE);
}

#[test]
fn serialize_restore_round_trip() {
    reset_state();
    with_state(|st| {
        apply_map_update(&mut st.active_shared_updates, 1, 10, true).unwrap();
        apply_map_update(&mut st.active_local_updates, 2, 20, true).unwrap();
    });
    let serialized = SerializeRelationMap();
    reset_state();
    RestoreRelationMap(&serialized).unwrap();
    with_state(|st| {
        assert_eq!(st.active_shared_updates.mappings[0].mapfilenumber, 10);
        assert_eq!(st.active_local_updates.mappings[0].mapfilenumber, 20);
    });
    let err = RestoreRelationMap(&serialized).unwrap_err();
    assert!(err.message().contains("parallel worker has existing mappings"));
}

#[test]
fn relmap_filename_builds() {
    assert_eq!(
        relmap_filename("global", RELMAPPER_FILENAME),
        "global/pg_filenode.map"
    );
    assert_eq!(
        relmap_filename("base/777", RELMAPPER_TEMP_FILENAME),
        "base/777/pg_filenode.map.tmp"
    );
}

/// CRC round-trip: computing over offsetof(crc) bytes then storing into crc
/// must verify equal, exactly as read_relmap_file checks write_relmap_file.
#[test]
fn crc_round_trip() {
    crc32c_seams::comp_crc32c::set(test_comp_crc32c);

    let mut map = empty_relmap_file();
    map.magic = RELMAPPER_FILEMAGIC;
    apply_map_update(&mut map, 42, 4242, true).unwrap();

    let image = encode_relmapfile(&map);
    let crc = relmapfile_crc(&image);
    map.crc = crc;

    let image2 = encode_relmapfile(&map);
    let check = relmapfile_crc(&image2);
    assert!(eq_crc32c(check, map.crc));
}

#[test]
fn xl_relmap_update_header_layout() {
    let hdr = encode_xl_relmap_update(5, 1664, 524);
    assert_eq!(u32::from_ne_bytes(hdr[0..4].try_into().unwrap()), 5);
    assert_eq!(u32::from_ne_bytes(hdr[4..8].try_into().unwrap()), 1664);
    assert_eq!(i32::from_ne_bytes(hdr[8..12].try_into().unwrap()), 524);
}

/// Standard CRC-32C (Castagnoli) reflected, table-free reference.
fn test_comp_crc32c(mut crc: u32, data: &[u8]) -> u32 {
    const POLY: u32 = 0x82f6_3b78;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
        }
    }
    crc
}
