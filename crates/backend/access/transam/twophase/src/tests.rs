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

/// Exercise the flat `#[repr(C)]` shared layout end-to-end over a heap-backed
/// block (no seam calls): build the freelist, pop into the active list, look up
/// by gid, swap-remove, and confirm the slot returns to the freelist. This is
/// the same backing store the real cross-process shmem uses.
#[test]
fn flat_layout_freelist_and_active() {
    const MAX: usize = 4;
    let size = two_phase_shmem_size(MAX);
    let mut block = alloc::vec![0u8; size];
    let mut state = TwoPhaseStateData {
        base: block.as_mut_ptr(),
        max_prepared_xacts: MAX,
    };
    // Initialize header + freelist by hand (init_shared() consults a proc seam
    // for pgprocno; here we link the freelist directly, head = MAX-1).
    state.header_mut().num_prep_xacts = 0;
    state.header_mut().free_head = INVALID_GXACT_IDX;
    for i in 0..MAX {
        let prev = state.header().free_head;
        let g = state.gxact_mut(i);
        *g = GlobalTransactionData::blank(i as ProcNumber);
        g.next = prev;
        state.header_mut().free_head = i as i32;
    }
    assert_eq!(state.num_prep_xacts(), 0);

    // Pop two free slots (C pops the most-recently-inserted head first: 3, 2).
    let a = state.pop_free().unwrap();
    let b = state.pop_free().unwrap();
    assert_eq!(a, MAX - 1);
    assert_eq!(b, MAX - 2);

    // Activate both with GIDs.
    let sa = state.push_active(a);
    state.gxact_mut(a).valid = true;
    state.gxact_mut(a).set_gid("alpha");
    let sb = state.push_active(b);
    state.gxact_mut(b).valid = true;
    state.gxact_mut(b).set_gid("beta");
    assert_eq!(state.num_prep_xacts(), 2);
    assert_eq!(state.prep_xact(sa).gid(), "alpha");
    assert_eq!(state.prep_xact(sb).gid(), "beta");

    // Swap-remove the first active entry; "beta" moves into its slot, and the
    // backing gxact returns to the freelist.
    remove_gxact(&mut state, sa).unwrap();
    assert_eq!(state.num_prep_xacts(), 1);
    assert_eq!(state.prep_xact(0).gid(), "beta");
    // The removed backing slot is now the freelist head again.
    assert_eq!(state.pop_free().unwrap(), a);
}
