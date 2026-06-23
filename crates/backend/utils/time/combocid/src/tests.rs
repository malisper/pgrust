use super::*;

use std::sync::Once;

use ::mcx::{MemoryContext, Mcx, PgVec};
use ::types_core::TransactionId;
use ::types_tuple::heaptuple::{
    HeapTupleField3, HeapTupleFields, HeapTupleHeaderChoice, ItemPointerData, HEAP_XMIN_COMMITTED,
};

/// The current transaction's xid, as seen by the fake xact seam below.
const MY_XID: TransactionId = 1234;

fn install_fake_xact_seam() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        transam_xact_seams::transaction_id_is_current_transaction_id::set(|xid| {
            xid == MY_XID
        });
    });
}

fn make_header<'mcx>(
    mcx: Mcx<'mcx>,
    xmin: TransactionId,
    infomask: u16,
    raw_cid: CommandId,
) -> HeapTupleHeaderData<'mcx> {
    HeapTupleHeaderData {
        t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields {
            t_xmin: xmin,
            t_xmax: 0,
            t_field3: HeapTupleField3::TCid(raw_cid),
        }),
        t_ctid: ItemPointerData::default(),
        t_infomask2: 0,
        t_infomask: infomask,
        t_hoff: 0,
        t_bits: PgVec::new_in(mcx),
    }
}

#[test]
fn get_cmin_cmax_without_combocid_returns_raw() {
    install_fake_xact_seam();
    let top = MemoryContext::new("TopTransactionContext");
    let state = ComboCidState::new();
    let tup = make_header(top.mcx(), MY_XID, 0, 42);
    assert_eq!(HeapTupleHeaderGetCmin(&state, &tup), 42);
    assert_eq!(HeapTupleHeaderGetCmax(&state, &tup), 42);
}

#[test]
fn adjust_cmax_for_current_xact_allocates_combo() {
    install_fake_xact_seam();
    let top = MemoryContext::new("TopTransactionContext");
    let mut state = ComboCidState::new();

    // Inserted by the current transaction at cmin 7, deleted at cmax 9: must
    // produce a combo id that decodes back to (7, 9).
    let tup = make_header(top.mcx(), MY_XID, 0, 7);
    let (combo, iscombo) = HeapTupleHeaderAdjustCmax(&mut state, &tup, 9).unwrap();
    assert!(iscombo);
    assert_eq!(combo, 0);

    let combo_tup = make_header(top.mcx(), MY_XID, HEAP_COMBOCID, combo);
    assert_eq!(HeapTupleHeaderGetCmin(&state, &combo_tup), 7);
    assert_eq!(HeapTupleHeaderGetCmax(&state, &combo_tup), 9);

    // The same (cmin, cmax) pair reuses the same combo id; a new pair gets a
    // new one.
    assert_eq!(GetComboCommandId(&mut state, 7, 9).unwrap(), combo);
    assert_eq!(GetComboCommandId(&mut state, 7, 10).unwrap(), 1);
}

#[test]
fn adjust_cmax_for_other_xact_is_not_combo() {
    install_fake_xact_seam();
    let top = MemoryContext::new("TopTransactionContext");
    let mut state = ComboCidState::new();

    // xmin committed: cheaper test short-circuits, cmax passes through.
    let tup = make_header(top.mcx(), MY_XID, HEAP_XMIN_COMMITTED, 7);
    assert_eq!(
        HeapTupleHeaderAdjustCmax(&mut state, &tup, 9).unwrap(),
        (9, false)
    );

    // xmin from another transaction: same.
    let tup = make_header(top.mcx(), MY_XID + 1, 0, 7);
    assert_eq!(
        HeapTupleHeaderAdjustCmax(&mut state, &tup, 9).unwrap(),
        (9, false)
    );
}

#[test]
fn at_eoxact_resets_state() {
    let top = MemoryContext::new("TopTransactionContext");
    let mut state = ComboCidState::new();
    GetComboCommandId(&mut state, 1, 2).unwrap();
    assert_eq!(state.combo_cids.len(), 1);

    AtEOXact_ComboCid(&mut state);
    assert!(state.combo_cids.is_empty());
    assert!(state.combo_hash.is_none());

    // Combo ids restart from 0 in the "next transaction".
    assert_eq!(GetComboCommandId(&mut state, 3, 4).unwrap(), 0);
}

#[test]
fn serialize_restore_roundtrip() {
    let top = MemoryContext::new("TopTransactionContext");
    let mut state = ComboCidState::new();
    GetComboCommandId(&mut state, 1, 2).unwrap();
    GetComboCommandId(&mut state, 3, 4).unwrap();
    GetComboCommandId(&mut state, 5, 6).unwrap();

    let size = EstimateComboCIDStateSpace(&state).unwrap();
    assert_eq!(size, 4 + 3 * 8);
    let mut buf = vec![0u8; size];
    SerializeComboCIDState(&state, &mut buf).unwrap();

    let mut restored = ComboCidState::new();
    RestoreComboCIDState(&mut restored, &buf).unwrap();
    assert_eq!(restored.combo_cids.len(), 3);
    assert_eq!(GetRealCmin(&restored, 1), 3);
    assert_eq!(GetRealCmax(&restored, 2), 6);
}

#[test]
fn serialize_into_too_small_buffer_errors() {
    let top = MemoryContext::new("TopTransactionContext");
    let mut state = ComboCidState::new();
    GetComboCommandId(&mut state, 1, 2).unwrap();

    let mut buf = vec![0u8; 4];
    let err = SerializeComboCIDState(&state, &mut buf).unwrap_err();
    assert_eq!(err.message(), "not enough space to serialize ComboCID state");
}

/// The installed seams reach the backend-local `thread_local!` state, and the
/// `at_eoxact_combocid` seam resets it. Run on a dedicated thread so the
/// `thread_local!` starts empty regardless of other tests.
#[test]
fn installed_seams_use_thread_local_state_and_reset_at_eoxact() {
    std::thread::spawn(|| {
        install_fake_xact_seam();
        init_seams();
        let top = MemoryContext::new("TopTransactionContext");

        // Insert+delete by current xact: AdjustCmax allocates a combo id that
        // GetCmin/GetCmax decode back to (5, 8), all via the installed seams
        // over the thread_local state.
        let tup = make_header(top.mcx(), MY_XID, 0, 5);
        let (combo, iscombo) =
            combocid_seams::heap_tuple_header_adjust_cmax::call(&tup, 8).unwrap();
        assert!(iscombo);

        let combo_tup = make_header(top.mcx(), MY_XID, HEAP_COMBOCID, combo);
        assert_eq!(
            combocid_seams::heap_tuple_header_get_cmin::call(&combo_tup),
            5
        );
        assert_eq!(
            combocid_seams::heap_tuple_header_get_cmax::call(&combo_tup),
            8
        );

        // End of transaction discards the state; the next combo id restarts at 0.
        combocid_seams::at_eoxact_combocid::call();
        let tup2 = make_header(top.mcx(), MY_XID, 0, 1);
        let (combo2, _) =
            combocid_seams::heap_tuple_header_adjust_cmax::call(&tup2, 2)
                .unwrap();
        assert_eq!(combo2, 0);
    })
    .join()
    .unwrap();
}

#[test]
fn restore_with_corrupt_state_errors() {
    let top = MemoryContext::new("TopTransactionContext");
    let mut state = ComboCidState::new();

    // Claims 2 elements but supplies bytes for only one.
    let mut buf = vec![0u8; 4 + 8];
    buf[0..4].copy_from_slice(&2i32.to_ne_bytes());
    let err = RestoreComboCIDState(&mut state, &buf).unwrap_err();
    assert_eq!(
        err.message(),
        "unexpected command ID while restoring combo CIDs"
    );
}
