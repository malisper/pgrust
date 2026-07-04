use super::*;
use types_core::TransactionId;
use types_tuple::ItemPointerData;

#[repr(align(8))]
struct Image([u8; 32]);

struct TestTuple(Box<Image>);

impl TestTuple {
    fn new(xmin: TransactionId, infomask: u16) -> Self {
        let mut t = TestTuple(Box::new(Image([0; 32])));
        let hdr = t.hdr_mut();
        hdr.set_xmin(xmin);
        hdr.t_infomask = infomask;
        hdr.t_hoff = 24;
        hdr.t_ctid = ItemPointerData::new(0, 1);
        t
    }

    fn hdr_mut(&mut self) -> &mut HeapTupleHeaderData {
        // SAFETY: 32-byte 8-aligned zero-init image, exclusively owned.
        unsafe { &mut *self.0 .0.as_mut_ptr().cast::<HeapTupleHeaderData>() }
    }
}

const XID_CURRENT: TransactionId = 700;

fn setup() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        xact_seams::transaction_id_is_current_transaction_id::set(|xid| xid == XID_CURRENT);
    });
    AtEOXact_ComboCid();
}

#[test]
fn cmin_cmax_passthrough_without_combo_bit() {
    setup();
    let mut t = TestTuple::new(XID_CURRENT, 0);
    t.hdr_mut().set_cmin(5);
    assert_eq!(HeapTupleHeaderGetCmin(t.hdr_mut()), 5);

    t.hdr_mut().set_xmax(XID_CURRENT);
    t.hdr_mut().set_cmax(9, false);
    assert_eq!(HeapTupleHeaderGetCmax(t.hdr_mut()), 9);
}

#[test]
fn adjust_cmax_makes_combo_for_own_insert() {
    setup();
    let mut t = TestTuple::new(XID_CURRENT, 0);
    t.hdr_mut().set_cmin(3);

    let (cmax, iscombo) = HeapTupleHeaderAdjustCmax(t.hdr_mut(), 7).unwrap();
    assert!(iscombo);

    t.hdr_mut().set_xmax(XID_CURRENT);
    t.hdr_mut().set_cmax(cmax, iscombo);
    assert_eq!(HeapTupleHeaderGetCmin(t.hdr_mut()), 3);
    assert_eq!(HeapTupleHeaderGetCmax(t.hdr_mut()), 7);
}

#[test]
fn adjust_cmax_passthrough_for_committed_xmin() {
    setup();
    let mut t = TestTuple::new(XID_CURRENT + 1, types_tuple::HEAP_XMIN_COMMITTED);
    t.hdr_mut().set_cmin(3);

    let (cmax, iscombo) = HeapTupleHeaderAdjustCmax(t.hdr_mut(), 7).unwrap();
    assert!(!iscombo);
    assert_eq!(cmax, 7);
}

#[test]
fn combo_cids_are_reused_and_allocated_in_order() {
    setup();
    let a = GetComboCommandId(1, 2);
    let b = GetComboCommandId(3, 4);
    let c = GetComboCommandId(1, 2);
    assert_eq!(a, 0);
    assert_eq!(b, 1);
    assert_eq!(c, a);
    assert_eq!(GetRealCmin(b), 3);
    assert_eq!(GetRealCmax(b), 4);
}

#[test]
fn serialize_restore_roundtrip_across_thread() {
    setup();
    assert_eq!(GetComboCommandId(1, 2), 0);
    assert_eq!(GetComboCommandId(3, 4), 1);

    let state = SerializeComboCIDState();
    assert_eq!(&state[..], &[(1, 2), (3, 4)]);

    std::thread::spawn(move || {
        RestoreComboCIDState(&state);
        assert_eq!(GetRealCmin(0), 1);
        assert_eq!(GetRealCmax(0), 2);
        assert_eq!(GetRealCmin(1), 3);
        assert_eq!(GetRealCmax(1), 4);
        assert_eq!(GetComboCommandId(3, 4), 1);
        assert_eq!(GetComboCommandId(5, 6), 2);
    })
    .join()
    .unwrap();
}

#[test]
fn restored_state_resolves_tuple_combo_cids_on_worker_thread() {
    setup();
    let mut t = TestTuple::new(XID_CURRENT, 0);
    t.hdr_mut().set_cmin(3);

    let (cmax, iscombo) = HeapTupleHeaderAdjustCmax(t.hdr_mut(), 7).unwrap();
    assert!(iscombo);
    t.hdr_mut().set_xmax(XID_CURRENT);
    t.hdr_mut().set_cmax(cmax, iscombo);

    let state = SerializeComboCIDState();
    std::thread::spawn(move || {
        let mut t = t;
        RestoreComboCIDState(&state);
        assert_eq!(HeapTupleHeaderGetCmin(t.hdr_mut()), 3);
        assert_eq!(HeapTupleHeaderGetCmax(t.hdr_mut()), 7);
    })
    .join()
    .unwrap();
}

#[test]
fn at_eoxact_forgets_combo_state() {
    setup();
    assert_eq!(GetComboCommandId(1, 2), 0);
    assert_eq!(GetComboCommandId(5, 6), 1);
    AtEOXact_ComboCid();
    assert_eq!(GetComboCommandId(5, 6), 0);
}
