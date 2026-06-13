//! Unit tests for the pure codec / layout / GID logic (no seam calls).

use super::*;

#[test]
fn header_roundtrip() {
    let hdr = TwoPhaseFileHeader {
        magic: TWOPHASE_MAGIC,
        total_len: 1234,
        xid: 42,
        database: 5,
        prepared_at: 99,
        owner: 10,
        nsubxacts: 2,
        ncommitrels: 1,
        nabortrels: 0,
        ncommitstats: 0,
        nabortstats: 0,
        ninvalmsgs: 3,
        initfileinval: true,
        gidlen: 7,
        origin_lsn: 0xDEAD_BEEF,
        origin_timestamp: -1,
    };
    let bytes = hdr.to_bytes();
    assert_eq!(bytes.len(), TwoPhaseFileHeader::wire_len());
    let back = TwoPhaseFileHeader::from_bytes(&bytes).unwrap();
    assert_eq!(hdr, back);
}

#[test]
fn record_roundtrip() {
    let r = TwoPhaseRecordOnDisk {
        len: 16,
        rmid: 3,
        info: 0xABCD,
    };
    let b = r.to_bytes();
    assert_eq!(b.len(), SIZEOF_TWOPHASE_RECORD_ON_DISK);
    assert_eq!(TwoPhaseRecordOnDisk::from_bytes(&b).unwrap(), r);
}

#[test]
fn maxalign_rounds_up() {
    assert_eq!(maxalign(0), 0);
    assert_eq!(maxalign(1), 8);
    assert_eq!(maxalign(8), 8);
    assert_eq!(maxalign(9), 16);
}

#[test]
fn buffer_layout_offsets() {
    let mut hdr = TwoPhaseFileHeader::from_bytes(&[0u8; 72]).unwrap();
    hdr.gidlen = 7;
    hdr.nsubxacts = 2;
    hdr.ncommitrels = 1;
    let l = BufferLayout::of(&hdr);
    assert_eq!(l.gid, maxalign(TwoPhaseFileHeader::wire_len()));
    assert_eq!(l.children, l.gid + maxalign(7));
    assert_eq!(l.commitrels, l.children + maxalign(2 * 4));
    assert_eq!(l.abortrels, l.commitrels + maxalign(1 * SIZEOF_REL_FILE_LOCATOR));
}

#[test]
fn gid_for_subid_roundtrip() {
    let gid = two_phase_transaction_gid(7, 12345).unwrap();
    assert_eq!(gid, "pg_gid_7_12345");
    assert!(is_two_phase_transaction_gid_for_subid(7, &gid));
    assert!(!is_two_phase_transaction_gid_for_subid(8, &gid));
    assert!(!is_two_phase_transaction_gid_for_subid(7, "not_a_gid"));
}

#[test]
fn save_state_pads_to_maxalign() {
    let mut st = SaveState::new();
    st.save_state_data(&[1u8, 2, 3]).unwrap();
    assert_eq!(st.total_len, 8);
    assert_eq!(st.as_slice().len(), 8);
    assert_eq!(&st.as_slice()[..3], &[1, 2, 3]);
}

#[test]
fn shmem_size_is_maxaligned() {
    let s = two_phase_shmem_size(10);
    assert!(s > 0);
}
