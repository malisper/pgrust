//! Unit tests for the shm_mq port.
//!
//! The seams stand in for the process-latch machinery, the backend identity
//! (`MyProcNumber`), the background-worker probe, and `CHECK_FOR_INTERRUPTS`.
//! The harness installs a cooperating single-process implementation:
//! `set_latch`/`reset_latch` are no-ops, `wait_latch` returns immediately (so
//! the blocking loops make a single extra trip and re-examine queue state),
//! `my_proc_number` is a fixed slot, and `get_background_worker_pid` reports
//! a live worker. The in-crate ring-buffer logic — framing, MAXALIGN
//! chunking, wraparound, the reassembly buffer — runs unchanged over a real
//! (leaked) segment with the genuine in-segment spinlock and atomics.

use super::*;

use std::sync::Once;

/// This test process's `ProcNumber` (both sender and receiver in-process).
const TEST_PROCNO: ProcNumber = 7;

/// Install the cooperating single-process seam implementations exactly once.
fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        backend_utils_init_small_seams::my_proc_number::set(|| TEST_PROCNO);
        proc_latch::set(|procno| LatchHandle::new(procno as usize + 1));
        set_latch::set(|_latch| {});
        reset_latch::set(|_latch| {});
        wait_latch::set(|_latch, _events, _timeout, _wei| Ok(WL_LATCH_SET as i32));
        get_background_worker_pid::set(|_h| (BgwHandleStatus::Started, 1));
        check_for_interrupts::set(|| Ok(()));
    });
}

/// The latch handle a test backend would carry as its `MyLatch`.
fn test_latch() -> LatchHandle {
    LatchHandle::new(TEST_PROCNO as usize + 1)
}

/// A leaked memory context standing in for the attach-time
/// `CurrentMemoryContext` (tests are short; leak is fine).
fn test_mcx() -> Mcx<'static> {
    Box::leak(Box::new(mcx::MemoryContext::new("shm_mq test"))).mcx()
}

/// Allocate an 8-byte-aligned fake DSM segment (leaked; tests are short).
fn make_segment(nbytes: usize) -> NonNull<u8> {
    let words = nbytes.div_ceil(8);
    let v: Vec<u64> = vec![0; words];
    let raw = Box::into_raw(v.into_boxed_slice()) as *mut u8;
    NonNull::new(raw).unwrap()
}

/// Set this process as both sender and receiver of `mq` (single-process test).
fn set_self_both(mq: ShmMq) {
    shm_mq_set_sender(mq, TEST_PROCNO);
    shm_mq_set_receiver(mq, TEST_PROCNO);
}

fn attach(mq: ShmMq) -> ShmMqHandle<'static> {
    shm_mq_attach(mq, test_mcx(), None, None, test_latch()).unwrap()
}

#[test]
fn minimum_size_is_maxaligned_header_plus_one_chunk() {
    let expected = MAXALIGN(mq_ring_member_offset()) + MAXIMUM_ALIGNOF;
    assert_eq!(shm_mq_minimum_size(), expected);
    assert!(shm_mq_minimum_size() >= 8);
}

#[test]
fn nextpower2_matches_postgres() {
    assert_eq!(pg_nextpower2_size_t(1), 1);
    assert_eq!(pg_nextpower2_size_t(2), 2);
    assert_eq!(pg_nextpower2_size_t(3), 4);
    assert_eq!(pg_nextpower2_size_t(8192), 8192);
    assert_eq!(pg_nextpower2_size_t(8193), 16384);
}

#[test]
fn create_initializes_header() {
    let seg = make_segment(1024);
    unsafe {
        let mq = shm_mq_create(seg, 1024);
        assert!(mq.receiver().is_null());
        assert!(mq.sender().is_null());
        assert_eq!(mq.bytes_read(), 0);
        assert_eq!(mq.bytes_written(), 0);
        assert!(!mq.detached());
        let data_offset = MAXALIGN(mq_ring_member_offset());
        assert_eq!(mq.ring_size(), MAXALIGN_DOWN(1024) - data_offset);
        assert_eq!(
            mq.header().mq_ring_offset as Size,
            data_offset - mq_ring_member_offset()
        );
    }
}

#[test]
fn set_receiver_and_sender_record_procnumbers() {
    install_seams();
    let seg = make_segment(1024);
    unsafe {
        let mq = shm_mq_create(seg, 1024);
        shm_mq_set_receiver(mq, TEST_PROCNO);
        assert_eq!(shm_mq_get_receiver(mq), Some(TEST_PROCNO));
        assert_eq!(shm_mq_get_sender(mq), None);
        shm_mq_set_sender(mq, 9);
        assert_eq!(shm_mq_get_sender(mq), Some(9));
    }
}

#[test]
fn send_and_receive_roundtrip_single_process() {
    install_seams();
    let seg = make_segment(1024);
    unsafe {
        let mq = shm_mq_create(seg, 1024);
        set_self_both(mq);

        let mut sh = attach(mq);
        let mut rh = attach(mq);

        let msg = b"hello shm_mq";
        let res = shm_mq_send(&mut sh, msg, false, true).unwrap();
        assert_eq!(res, SHM_MQ_SUCCESS);

        let (rres, payload) = shm_mq_receive(&mut rh, false).unwrap();
        assert_eq!(rres, SHM_MQ_SUCCESS);
        assert_eq!(payload, msg);
    }
}

#[test]
fn send_via_iovec_concatenates_chunks() {
    install_seams();
    let seg = make_segment(1024);
    unsafe {
        let mq = shm_mq_create(seg, 1024);
        set_self_both(mq);

        let mut sh = attach(mq);
        let mut rh = attach(mq);

        // Three uneven chunks force the tmpbuf MAXALIGN-combining path.
        let iov = [
            shm_mq_iovec::new(b"abc"),
            shm_mq_iovec::new(b"defghi"),
            shm_mq_iovec::new(b"jklmnop"),
        ];
        let res = shm_mq_sendv(&mut sh, &iov, false, true).unwrap();
        assert_eq!(res, SHM_MQ_SUCCESS);

        let (rres, payload) = shm_mq_receive(&mut rh, false).unwrap();
        assert_eq!(rres, SHM_MQ_SUCCESS);
        assert_eq!(payload, b"abcdefghijklmnop");
    }
}

#[test]
fn sendv_with_trailing_zero_length_iovec() {
    // Regression: a non-MAXALIGN'd chunk followed by a zero-length iovec
    // drives the tmpbuf path's inner loop to exhaust the iovec array
    // (which_iov == iovcnt). The C do/while then re-checks
    // `mqh_partial_bytes < nbytes` and exits; the port must do the same
    // rather than re-indexing iov[which_iov].
    install_seams();
    let seg = make_segment(1024);
    unsafe {
        let mq = shm_mq_create(seg, 1024);
        set_self_both(mq);

        let mut sh = attach(mq);
        let mut rh = attach(mq);

        let iov = [shm_mq_iovec::new(b"abcd"), shm_mq_iovec::new(b"")];
        let res = shm_mq_sendv(&mut sh, &iov, false, true).unwrap();
        assert_eq!(res, SHM_MQ_SUCCESS);

        let (rres, payload) = shm_mq_receive(&mut rh, false).unwrap();
        assert_eq!(rres, SHM_MQ_SUCCESS);
        assert_eq!(payload, b"abcd");
    }
}

#[test]
fn receive_nowait_on_empty_queue_would_block() {
    install_seams();
    let seg = make_segment(1024);
    unsafe {
        let mq = shm_mq_create(seg, 1024);
        set_self_both(mq);
        let mut rh = attach(mq);

        // Sender is attached (mq_sender set), but no data written yet.
        let (res, payload) = shm_mq_receive(&mut rh, true).unwrap();
        assert_eq!(res, SHM_MQ_WOULD_BLOCK);
        assert!(payload.is_empty());
    }
}

#[test]
fn large_message_wraps_and_reassembles() {
    install_seams();
    // Small ring forces wraparound + reassembly buffer use.
    let seg = make_segment(256);
    unsafe {
        let mq = shm_mq_create(seg, 256);
        set_self_both(mq);

        let mut sh = attach(mq);
        let mut rh = attach(mq);

        // Message bigger than the ring: drive send/receive cooperatively in
        // nowait chunks then drain.
        let msg: Vec<u8> = (0..100u32).map(|i| (i % 251) as u8).collect();

        let mut sent = false;
        let mut got: Option<Vec<u8>> = None;
        for _ in 0..1000 {
            if !sent {
                match shm_mq_send(&mut sh, &msg, true, true).unwrap() {
                    SHM_MQ_SUCCESS => sent = true,
                    SHM_MQ_WOULD_BLOCK => {}
                    SHM_MQ_DETACHED => panic!("unexpected detach"),
                }
            }
            match shm_mq_receive(&mut rh, true).unwrap() {
                (SHM_MQ_SUCCESS, payload) => {
                    got = Some(payload.to_vec());
                    break;
                }
                (SHM_MQ_WOULD_BLOCK, _) => {}
                (SHM_MQ_DETACHED, _) => panic!("unexpected detach"),
            }
        }
        assert!(sent, "message should have been fully sent");
        assert_eq!(got.as_deref(), Some(msg.as_slice()));
    }
}

#[test]
fn multiple_messages_in_sequence() {
    install_seams();
    let seg = make_segment(1024);
    unsafe {
        let mq = shm_mq_create(seg, 1024);
        set_self_both(mq);

        let mut sh = attach(mq);
        let mut rh = attach(mq);

        for n in 0..5u32 {
            let msg = format!("message-{n}");
            assert_eq!(
                shm_mq_send(&mut sh, msg.as_bytes(), false, true).unwrap(),
                SHM_MQ_SUCCESS
            );
            let (res, payload) = shm_mq_receive(&mut rh, false).unwrap();
            assert_eq!(res, SHM_MQ_SUCCESS);
            assert_eq!(payload, msg.as_bytes());
        }
    }
}

#[test]
fn empty_message_roundtrip() {
    install_seams();
    let seg = make_segment(1024);
    unsafe {
        let mq = shm_mq_create(seg, 1024);
        set_self_both(mq);

        let mut sh = attach(mq);
        let mut rh = attach(mq);

        assert_eq!(
            shm_mq_send(&mut sh, b"", false, true).unwrap(),
            SHM_MQ_SUCCESS
        );
        let (res, payload) = shm_mq_receive(&mut rh, false).unwrap();
        assert_eq!(res, SHM_MQ_SUCCESS);
        assert!(payload.is_empty());
    }
}

#[test]
fn detach_marks_queue_detached() {
    install_seams();
    let seg = make_segment(1024);
    unsafe {
        let mq = shm_mq_create(seg, 1024);
        set_self_both(mq);
        let sh = attach(mq);
        shm_mq_detach(sh);
        assert!(mq.detached());
    }
}

#[test]
fn send_after_detach_returns_detached() {
    install_seams();
    let seg = make_segment(1024);
    unsafe {
        let mq = shm_mq_create(seg, 1024);
        set_self_both(mq);

        let mut sh = attach(mq);
        // Detach the queue out from under the sender.
        mq.set_detached();
        let res = shm_mq_send(&mut sh, b"x", false, true).unwrap();
        assert_eq!(res, SHM_MQ_DETACHED);
    }
}

#[test]
fn get_queue_round_trips_handle_to_queue() {
    install_seams();
    let seg = make_segment(1024);
    unsafe {
        let mq = shm_mq_create(seg, 1024);
        set_self_both(mq);
        let sh = attach(mq);
        assert_eq!(shm_mq_get_queue(&sh).base(), mq.base());
    }
}
