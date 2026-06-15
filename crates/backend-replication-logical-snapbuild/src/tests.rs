//! Unit tests for the seam-free parts of snapbuild: the on-disk
//! serialization round-trip, filename LSN parsing, and the xid helpers.

use super::*;
use std::sync::Once;

static CRC_INSTALLED: Once = Once::new();

/// Install a deterministic checksum so serialize/deserialize agree. Any stable
/// function works for a round-trip test; we mirror the seam's "init+finalize
/// internally" contract by ignoring the seed.
fn install_crc() {
    CRC_INSTALLED.call_once(|| {
        port_crc32c_seams::comp_crc32c::set(|_seed, data| {
            // FNV-1a — deterministic, depends on all bytes.
            let mut h: u32 = 0x811c_9dc5;
            for &b in data {
                h ^= b as u32;
                h = h.wrapping_mul(0x0100_0193);
            }
            h
        });
    });
}

fn sample_builder() -> SnapBuild {
    let mut b = allocate_snapshot_builder(
        ReorderBufferHandle(7),
        100,        // xmin_horizon
        0x1234_5678, // start_lsn
        false,
        false,
        0xABCD,
    );
    b.state = SNAPBUILD_CONSISTENT;
    b.xmin = 500;
    b.xmax = 600;
    b.last_serialized_snapshot = 0x9999;
    b.next_phase_at = InvalidTransactionId;
    b.committed.includes_all_transactions = false;
    b.committed.xip = alloc::vec![510, 505, 520];
    b.committed.xcnt = 3;
    b
}

#[test]
fn ondisk_round_trip() {
    install_crc();
    let b = sample_builder();
    let catchange = alloc::vec![540u32, 550u32];
    let image = ondisk::serialize(&b, catchange.len(), &catchange);

    let restored = ondisk::deserialize(&image, "test.snap").expect("deserialize ok");
    assert_eq!(restored.state, SNAPBUILD_CONSISTENT);
    assert_eq!(restored.xmin, 500);
    assert_eq!(restored.xmax, 600);
    assert_eq!(restored.start_decoding_at, 0x1234_5678);
    assert_eq!(restored.two_phase_at, 0xABCD);
    assert_eq!(restored.initial_xmin_horizon, 100);
    assert!(!restored.building_full_snapshot);
    assert_eq!(restored.last_serialized_snapshot, 0x9999);
    assert!(!restored.committed_includes_all_transactions);
    assert_eq!(restored.committed_xip, alloc::vec![510, 505, 520]);
    assert_eq!(restored.catchange_xip, alloc::vec![540, 550]);
}

#[test]
fn ondisk_bad_magic_is_corrupt() {
    install_crc();
    let b = sample_builder();
    let mut image = ondisk::serialize(&b, 0, &[]);
    image[0] ^= 0xFF; // corrupt the magic
    assert!(ondisk::deserialize(&image, "test.snap").is_err());
}

#[test]
fn ondisk_checksum_mismatch_is_corrupt() {
    install_crc();
    let b = sample_builder();
    let mut image = ondisk::serialize(&b, 0, &[]);
    // flip a byte in the builder image (after the checksummed region start)
    let last = image.len() - 1;
    image[last] ^= 0x01;
    assert!(ondisk::deserialize(&image, "test.snap").is_err());
}

#[test]
fn parse_snap_lsn_round_trips() {
    let lsn: XLogRecPtr = 0x1234_5678_9ABC_DEF0;
    let name = alloc::format!("{}.snap", snap_file_lsn(lsn));
    assert_eq!(parse_snap_lsn(&name), Some(lsn));
    assert_eq!(parse_snap_lsn("notasnap"), None);
    assert_eq!(parse_snap_lsn("nodash.snap"), None);
}

#[test]
fn xid_helpers() {
    let mut arr = alloc::vec![30u32, 10, 20];
    arr.sort_by(xid_cmp);
    assert_eq!(arr, alloc::vec![10, 20, 30]);
    assert!(xid_in_sorted(&arr, 20));
    assert!(!xid_in_sorted(&arr, 25));
    assert!(normal_transaction_id_precedes(10, 20));
    assert!(normal_transaction_id_follows(20, 10));
}

#[test]
fn allocate_initializes_like_c() {
    let b = allocate_snapshot_builder(ReorderBufferHandle(1), 42, 0xFEED, true, true, 0x77);
    assert_eq!(b.state, SNAPBUILD_START);
    assert_eq!(b.initial_xmin_horizon, 42);
    assert_eq!(b.start_decoding_at, 0xFEED);
    assert!(b.building_full_snapshot);
    assert!(b.in_slot_creation);
    assert_eq!(b.two_phase_at, 0x77);
    assert_eq!(b.committed.xcnt_space, 128);
    assert!(b.committed.includes_all_transactions);
    assert!(b.snapshot.is_none());
}
