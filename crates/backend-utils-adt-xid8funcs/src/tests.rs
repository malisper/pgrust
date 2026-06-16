//! Unit tests for the xid8funcs.c port.

use super::*;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Once;
use types_snapshot::snapshot::{SnapshotData, SnapshotType};

fn fx(v: u64) -> FullTransactionId {
    FullTransactionId { value: v }
}

// ---------------------------------------------------------------------------
// Test seam providers.
//
// The live transaction / snapshot / clog reads are real owner `seam!` slots,
// installed here from non-capturing provider closures that read/write a
// thread-local `RefCell<TestState>` (per-test isolation, since `#[test]`s run on
// separate threads). The process-global seam slots are `OnceLock`s, so we
// install them exactly once for the whole test binary via a `Once`.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct TestState {
    next_fxid: u64,
    oldest_clog: TransactionId,
    top_fxid: u64,
    top_if_any: u64,
    active: Option<Rc<SnapshotData>>,
    in_recovery: bool,
    in_progress: HashSet<TransactionId>,
    committed: HashSet<TransactionId>,
}

thread_local! {
    static STATE: RefCell<TestState> = RefCell::new(TestState::default());
}

fn with_state<R>(f: impl FnOnce(&mut TestState) -> R) -> R {
    STATE.with(|s| f(&mut s.borrow_mut()))
}

fn test_snapshot(xmin: u32, xmax: u32, xip: Vec<u32>) -> Rc<SnapshotData> {
    let mut s = SnapshotData::sentinel(SnapshotType::SNAPSHOT_MVCC);
    s.xmin = xmin;
    s.xmax = xmax;
    s.xcnt = xip.len() as u32;
    s.xip = xip;
    Rc::new(s)
}

static INSTALL: Once = Once::new();

fn install_seams() {
    INSTALL.call_once(|| {
        varsup_seams::read_next_full_transaction_id::set(|| fx(with_state(|st| st.next_fxid)));
        varsup_seams::get_oldest_clog_xid::set(|| with_state(|st| st.oldest_clog));
        xact_seams::get_top_full_transaction_id::set(|| Ok(fx(with_state(|st| st.top_fxid))));
        xact_seams::get_top_full_transaction_id_if_any::set(|| fx(with_state(|st| st.top_if_any)));
        snapmgr_seams::get_active_snapshot::set(|| Ok(with_state(|st| st.active.clone())));
        snapmgr_pc_seams::transaction_xmin::set(|| Ok(0));
        utility_seams::prevent_command_during_recovery::set(|_stmt| {
            if with_state(|st| st.in_recovery) {
                Err(PgError::error("cannot execute during recovery"))
            } else {
                Ok(())
            }
        });
        lwlock_seams::lwlock_acquire_main::set(|offset, _mode| {
            Ok(lwlock_seams::MainLWLockGuard::new(offset, true))
        });
        lwlock_seams::lwlock_release_main::set(|_offset| Ok(()));
        procarray_seams::transaction_id_is_in_progress::set(|xid| {
            Ok(with_state(|st| st.in_progress.contains(&xid)))
        });
        transam_seams::transaction_id_did_commit::set(|xid, _xmin| {
            Ok(with_state(|st| st.committed.contains(&xid)))
        });
    });
}

#[test]
fn snapshot_max_nxip_matches_c() {
    // PG_SNAPSHOT_MAX_NXIP = (MaxAllocSize - offsetof(pg_snapshot, xip)) / 8.
    assert_eq!(PG_SNAPSHOT_MAX_NXIP, (0x3fff_ffff - 24) / 8);
}

#[test]
fn parse_out_roundtrip() {
    let snap = parse_snapshot("10:20:10,12,15", None).unwrap().unwrap();
    assert_eq!(snap.xmin, fx(10));
    assert_eq!(snap.xmax, fx(20));
    assert_eq!(snap.nxip, 3);
    assert_eq!(snap.xip, vec![fx(10), fx(12), fx(15)]);
    assert_eq!(pg_snapshot_out(&snap), "10:20:10,12,15");
}

#[test]
fn parse_empty_xip() {
    let snap = parse_snapshot("3:3:", None).unwrap().unwrap();
    assert_eq!(snap.nxip, 0);
    assert!(snap.xip.is_empty());
    assert_eq!(pg_snapshot_out(&snap), "3:3:");
}

#[test]
fn parse_dedups_adjacent() {
    let snap = parse_snapshot("5:20:5,5,7,7,7", None).unwrap().unwrap();
    assert_eq!(snap.xip, vec![fx(5), fx(7)]);
}

#[test]
fn parse_rejects_bad_format() {
    assert!(parse_snapshot("10", None).is_err()); // missing first colon
    assert!(parse_snapshot("20:10:", None).is_err()); // xmax < xmin
    assert!(parse_snapshot("10:20:5", None).is_err()); // xip < xmin
    assert!(parse_snapshot("10:20:20", None).is_err()); // xip >= xmax
    assert!(parse_snapshot("10:20:15,12", None).is_err()); // out of order
    assert!(parse_snapshot("10:20:12x", None).is_err()); // trailing junk
    assert!(parse_snapshot("0:20:", None).is_err()); // invalid xmin (0)
}

#[test]
fn parse_soft_error_returns_none() {
    let mut ctx = SoftErrorContext::new(true);
    let r = parse_snapshot("bogus", Some(&mut ctx)).unwrap();
    assert!(r.is_none());
    assert!(ctx.error_occurred());
}

#[test]
fn visibility() {
    let snap = parse_snapshot("10:20:12,15", None).unwrap().unwrap();
    assert!(is_visible_fxid(fx(5), &snap)); // < xmin
    assert!(!is_visible_fxid(fx(25), &snap)); // >= xmax
    assert!(!is_visible_fxid(fx(12), &snap)); // in xip -> in progress
    assert!(is_visible_fxid(fx(13), &snap)); // in (xmin,xmax), not in xip
    assert!(pg_visible_in_snapshot(fx(13), &snap));
}

#[test]
fn visibility_bsearch_path() {
    let mut s = String::from("10:200:");
    let mut xips = Vec::new();
    for i in 0..40u64 {
        if i > 0 {
            s.push(',');
        }
        let v = 11 + i * 2; // 11,13,15,... all < 200, ascending
        s.push_str(&v.to_string());
        xips.push(v);
    }
    let snap = parse_snapshot(&s, None).unwrap().unwrap();
    assert!(snap.nxip > USE_BSEARCH_IF_NXIP_GREATER);
    assert!(!is_visible_fxid(fx(xips[10]), &snap)); // an xip -> not visible
    assert!(is_visible_fxid(fx(12), &snap)); // a gap value is visible
}

#[test]
fn sort_snapshot_sorts_and_dedups() {
    let mut snap = PgSnapshot {
        nxip: 5,
        xmin: fx(1),
        xmax: fx(100),
        xip: vec![fx(7), fx(3), fx(7), fx(3), fx(5)],
    };
    sort_snapshot(&mut snap);
    assert_eq!(snap.xip, vec![fx(3), fx(5), fx(7)]);
    assert_eq!(snap.nxip, 3);
}

#[test]
fn send_recv_roundtrip() {
    let snap = parse_snapshot("10:20:12,15", None).unwrap().unwrap();
    let bytes = pg_snapshot_send(&snap);
    let mut cur = Pq8Cursor::new(&bytes);
    let recovered = pg_snapshot_recv(&mut cur).unwrap();
    assert_eq!(recovered.xmin, snap.xmin);
    assert_eq!(recovered.xmax, snap.xmax);
    assert_eq!(recovered.xip, snap.xip);
    assert_eq!(recovered.nxip, snap.nxip);
}

#[test]
fn recv_dedups_and_validates() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&3i32.to_be_bytes()); // nxip
    bytes.extend_from_slice(&10u64.to_be_bytes()); // xmin
    bytes.extend_from_slice(&20u64.to_be_bytes()); // xmax
    bytes.extend_from_slice(&12u64.to_be_bytes());
    bytes.extend_from_slice(&12u64.to_be_bytes()); // dup
    bytes.extend_from_slice(&15u64.to_be_bytes());
    let mut cur = Pq8Cursor::new(&bytes);
    let snap = pg_snapshot_recv(&mut cur).unwrap();
    assert_eq!(snap.xip, vec![fx(12), fx(15)]);
    assert_eq!(snap.nxip, 2);
}

#[test]
fn recv_rejects_out_of_range_xip() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&1i32.to_be_bytes());
    bytes.extend_from_slice(&10u64.to_be_bytes());
    bytes.extend_from_slice(&20u64.to_be_bytes());
    bytes.extend_from_slice(&25u64.to_be_bytes()); // > xmax
    let mut cur = Pq8Cursor::new(&bytes);
    assert!(pg_snapshot_recv(&mut cur).is_err());
}

#[test]
fn varlena_roundtrip() {
    let snap = parse_snapshot("10:20:12,15", None).unwrap().unwrap();
    let bytes = snap.to_varlena_bytes();
    assert_eq!(bytes.len(), PG_SNAPSHOT_SIZE(2));
    assert_eq!(
        (u32::from_ne_bytes(bytes[0..4].try_into().unwrap()) >> 2) as usize,
        bytes.len()
    );
    let recovered = PgSnapshot::from_varlena_bytes(&bytes).unwrap();
    assert_eq!(recovered, snap);
}

#[test]
fn xmin_xmax_xip_accessors() {
    let snap = parse_snapshot("10:20:12,15", None).unwrap().unwrap();
    assert_eq!(pg_snapshot_xmin(&snap), fx(10));
    assert_eq!(pg_snapshot_xmax(&snap), fx(20));
    assert_eq!(pg_snapshot_xip(&snap), vec![fx(12), fx(15)]);
}

#[test]
fn current_xact_id_recovery_guard() {
    install_seams();
    with_state(|st| {
        *st = TestState {
            top_fxid: 42,
            ..Default::default()
        }
    });
    assert_eq!(pg_current_xact_id().unwrap(), fx(42));

    with_state(|st| st.in_recovery = true);
    assert!(pg_current_xact_id().is_err());
}

#[test]
fn current_xact_id_if_assigned() {
    install_seams();
    with_state(|st| {
        *st = TestState {
            top_if_any: 0, // InvalidFullTransactionId -> NULL
            ..Default::default()
        }
    });
    assert_eq!(pg_current_xact_id_if_assigned().unwrap(), None);

    with_state(|st| st.top_if_any = 99);
    assert_eq!(pg_current_xact_id_if_assigned().unwrap(), Some(fx(99)));
}

#[test]
fn current_snapshot_builds_and_sorts() {
    install_seams();
    with_state(|st| {
        *st = TestState {
            next_fxid: 1000,
            active: Some(test_snapshot(100, 110, vec![105, 102, 105])), // unsorted, dup
            ..Default::default()
        }
    });
    let snap = pg_current_snapshot().unwrap();
    assert_eq!(snap.xmin, fx(100));
    assert_eq!(snap.xmax, fx(110));
    assert_eq!(snap.xip, vec![fx(102), fx(105)]); // sorted + deduped
    assert_eq!(snap.nxip, 2);
}

#[test]
fn current_snapshot_no_active_snapshot() {
    install_seams();
    with_state(|st| {
        *st = TestState {
            next_fxid: 1000,
            active: None,
            ..Default::default()
        }
    });
    assert!(pg_current_snapshot().is_err());
}

#[test]
fn xact_status_paths() {
    install_seams();
    let fxid = fx(50);

    // in progress
    with_state(|st| {
        *st = TestState {
            next_fxid: 100,
            oldest_clog: 3,
            ..Default::default()
        };
        st.in_progress.insert(50);
    });
    assert_eq!(pg_xact_status(fxid).unwrap(), Some("in progress"));

    // committed
    with_state(|st| {
        *st = TestState {
            next_fxid: 100,
            oldest_clog: 3,
            ..Default::default()
        };
        st.committed.insert(50);
    });
    assert_eq!(pg_xact_status(fxid).unwrap(), Some("committed"));

    // aborted (neither in progress nor committed)
    with_state(|st| {
        *st = TestState {
            next_fxid: 100,
            oldest_clog: 3,
            ..Default::default()
        }
    });
    assert_eq!(pg_xact_status(fxid).unwrap(), Some("aborted"));

    // too old: oldest_clog == 60 > 50, so not determinable -> NULL
    with_state(|st| {
        *st = TestState {
            next_fxid: 100,
            oldest_clog: 60,
            ..Default::default()
        }
    });
    assert_eq!(pg_xact_status(fxid).unwrap(), None);
}

#[test]
fn xact_status_future_xid_errors() {
    install_seams();
    // fxid in the future (>= next): error.
    with_state(|st| {
        *st = TestState {
            next_fxid: 40,
            oldest_clog: 3,
            ..Default::default()
        }
    });
    assert!(pg_xact_status(fx(50)).is_err());
}

#[test]
fn in_recent_past_nonnormal_xid() {
    install_seams();
    // xid 1 (BootstrapTransactionId) is valid but not normal -> determinable.
    with_state(|st| {
        *st = TestState {
            next_fxid: 100,
            oldest_clog: 3,
            ..Default::default()
        }
    });
    let rp = TransactionIdInRecentPast(fx(1)).unwrap();
    assert!(rp.determinable);
    assert_eq!(rp.extracted_xid, 1);

    // xid 0 (invalid) -> not determinable, extracted 0.
    let rp = TransactionIdInRecentPast(fx(0)).unwrap();
    assert!(!rp.determinable);
    assert_eq!(rp.extracted_xid, 0);
}
