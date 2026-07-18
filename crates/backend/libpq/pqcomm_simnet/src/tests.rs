//! SimNet provider battery (sim-cfg only). The seam slots are process-global
//! set-once statics, so this file installs the provider ONCE; the pair state
//! is thread-local, so every #[test] (own thread) gets a fresh universe.
//!
//! The noblock coverage deliberately goes through the CONSUMERS
//! (pq_getbyte_if_available / pq_flush_if_writable / pq_putmessage_noblock)
//! — the wasm-net-seam ledger named "first provider whose consumers can
//! exercise the noblock arms" as the N1/N2 MUST-FIX trigger; these tests are
//! that exercise.

use super::*;

fn install_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        postgres_seams::process_client_read_interrupt::set(|_| Ok(()));
        postgres_seams::process_client_write_interrupt::set(|_| Ok(()));
        pqcomm::init_seams();
        init_transport_seams();
    });
}

/// pq_init through the seam slot (buffers + state), as the wire bring-up does.
fn session_init() {
    install_once();
    reset();
    let cs = simnet_client_socket();
    let _port = pqcomm_seams::pq_init::call(&cs).expect("pq_init");
}

#[test]
fn duplex_roundtrip_then_clean_eof() {
    session_init();
    client_send(b"hello");
    let mut buf = [0u8; 16];
    let n = secure_read(&mut buf).unwrap().unwrap();
    assert_eq!(&buf[..n], b"hello");

    // Server -> client.
    let n = secure_write(b"world").unwrap().unwrap();
    assert_eq!(n, 5);
    assert_eq!(client_recv_all(), b"world");

    // Empty peer buffer + no live writer = clean session end, not a hang.
    client_close();
    let r = secure_read(&mut buf).unwrap().unwrap();
    assert_eq!(r, 0, "dead-writer read must be EOF");
}

#[test]
fn blocking_read_pumps_client_deterministically() {
    session_init();
    // Scripted client: two sends, then finished.
    let mut step = 0;
    install_client_pump(move || {
        step += 1;
        match step {
            1 => {
                client_send(b"first");
                PumpStatus::Progress
            }
            2 => {
                client_send(b"second");
                PumpStatus::Progress
            }
            _ => PumpStatus::Finished,
        }
    });
    let mut buf = [0u8; 32];
    let n = secure_read(&mut buf).unwrap().unwrap();
    assert_eq!(&buf[..n], b"first");
    let n = secure_read(&mut buf).unwrap().unwrap();
    assert_eq!(&buf[..n], b"second");
    // Script exhausted: the park resolves to Finished -> clean EOF.
    let n = secure_read(&mut buf).unwrap().unwrap();
    assert_eq!(n, 0);
}

#[test]
fn blocking_write_backpressure_pumps_the_drain() {
    session_init();
    install_client_pump(|| {
        // Drain whatever the server buffered; never send.
        let _ = client_recv_all();
        PumpStatus::Progress
    });
    // 4x the buffer cap: must complete through pump-driven drains.
    let big = vec![0xABu8; SIMNET_BUF_CAP * 4];
    let mut wrote = 0;
    while wrote < big.len() {
        let n = secure_write(&big[wrote..]).unwrap().unwrap();
        assert!(n > 0);
        wrote += n;
    }
    let _ = client_recv_all();
    let (_, received) = client_transcript();
    assert_eq!(received.len(), big.len());
    assert!(received.iter().all(|&b| b == 0xAB));
}

#[test]
fn write_after_client_close_with_full_buffer_is_epipe() {
    session_init();
    // Fill the buffer exactly to cap, then kill the client.
    let n = secure_write(&vec![1u8; SIMNET_BUF_CAP]).unwrap().unwrap();
    assert_eq!(n, SIMNET_BUF_CAP);
    client_close();
    let r = secure_write(b"more").unwrap();
    assert_eq!(r, Err(libc::EPIPE));
}

/// The noblock READ arm through its consumer: pq_getbyte_if_available.
/// Readiness is a pure function of buffered bytes — no data + live writer =
/// "no byte" (0); data = the byte; dead writer = EOF.
#[test]
fn consumer_pq_getbyte_if_available_noblock_arms() {
    session_init();
    pqcomm::pq_startmsgread().unwrap();

    let mut c = 0u8;
    // Empty + live writer: 0 = "no data now" (EWOULDBLOCK arm).
    assert_eq!(pqcomm::pq_getbyte_if_available(&mut c).unwrap(), 0);

    client_send(b"Q");
    assert_eq!(pqcomm::pq_getbyte_if_available(&mut c).unwrap(), 1);
    assert_eq!(c, b'Q');

    // Dead writer: EOF, not eternal would-block (the N2 class of bug on the
    // fd providers; structural here).
    client_close();
    assert_eq!(pqcomm::pq_getbyte_if_available(&mut c).unwrap(), pqcomm::EOF);
    pqcomm::pq_endmsgread();
}

/// The noblock WRITE arm through its consumers: pq_putmessage_noblock +
/// pq_flush_if_writable against a FULL peer buffer. Must return (buffering /
/// would-block), never park — the N1 class of bug on the fd providers.
#[test]
fn consumer_noblock_write_never_parks_on_full_buffer() {
    session_init();
    // Fill the pair's s2c to cap so the transport cannot accept a byte.
    let n = secure_write(&vec![7u8; SIMNET_BUF_CAP]).unwrap().unwrap();
    assert_eq!(n, SIMNET_BUF_CAP);

    // pq_putmessage_noblock buffers locally (its contract: enlarge, never
    // block) — must succeed instantly with the transport full.
    pqcomm::pq_putmessage_noblock(b'd', &vec![9u8; 4096]).unwrap();

    // pq_flush_if_writable: transport full -> writes nothing, returns 0
    // (would-block), leaves the data pending.
    assert_eq!(pqcomm::pq_flush_if_writable().unwrap(), 0);
    assert!(pqcomm::pq_is_send_pending());

    // Drain the pair; now the flush proceeds to completion.
    let drained = client_recv_all();
    assert_eq!(drained.len(), SIMNET_BUF_CAP);
    assert_eq!(pqcomm::pq_flush_if_writable().unwrap(), 0);
    assert!(!pqcomm::pq_is_send_pending());
    let msg = client_recv_all();
    assert_eq!(msg[0], b'd');
}

#[test]
fn virtual_listen_accept_arms() {
    install_once();
    reset();
    let mut socks = Vec::new();
    pqcomm_seams::listen_server_port::call(None, 5432, None, &mut socks, 64).unwrap();
    assert_eq!(socks, vec![SIMNET_LISTEN_FD]);

    // No pending connection: the accept arm reports, deterministically.
    assert!(pqcomm_seams::accept_connection::call(SIMNET_LISTEN_FD).is_err());

    client_connect();
    let cs = pqcomm_seams::accept_connection::call(SIMNET_LISTEN_FD).unwrap();
    assert_eq!(cs.sock, SIMNET_CONN_FD);
}

/// The determinism gate at unit level: the same session script, run twice on
/// fresh universes, produces byte-identical op logs AND byte-identical
/// client transcripts (op-sequence numbered — the inc-2 fault plan targets
/// these numbers).
#[test]
fn op_log_and_transcript_replay_identity() {
    install_once();

    fn run_script() -> (Vec<String>, Vec<u8>, Vec<u8>) {
        reset();
        let cs = simnet_client_socket();
        let _port = pqcomm_seams::pq_init::call(&cs).expect("pq_init");
        let mut step = 0;
        install_client_pump(move || {
            step += 1;
            match step {
                1 => {
                    client_send(b"QRY one");
                    PumpStatus::Progress
                }
                2 => {
                    let _ = client_recv_all();
                    client_send(b"QRY two");
                    PumpStatus::Progress
                }
                _ => PumpStatus::Finished,
            }
        });
        let mut buf = [0u8; 7];
        // read (parks -> pump 1) / respond / read (parks -> pump 2 drains
        // and sends) / respond / read to EOF.
        let n = secure_read(&mut buf).unwrap().unwrap();
        assert_eq!(n, 7);
        let _ = secure_write(b"RSP one").unwrap().unwrap();
        let n = secure_read(&mut buf).unwrap().unwrap();
        assert_eq!(n, 7);
        let _ = secure_write(b"RSP two").unwrap().unwrap();
        let n = secure_read(&mut buf).unwrap().unwrap();
        assert_eq!(n, 0);
        let _ = client_recv_all();
        let (sent, received) = client_transcript();
        (op_log(), sent, received)
    }

    let (log1, sent1, recv1) = run_script();
    let (log2, sent2, recv2) = run_script();
    assert_eq!(log1, log2, "op logs must be byte-identical across replays");
    assert_eq!(sent1, sent2);
    assert_eq!(recv1, recv2);
    assert!(log1.iter().all(|l| l.starts_with("NETOP seq=")));
    // Sequence numbers are dense from 1 (the fault plan's targeting space).
    for (i, line) in log1.iter().enumerate() {
        assert!(line.contains(&format!("seq={}", i + 1)), "line {i}: {line}");
    }
}

/// A pump that reports Progress while changing nothing is a deterministic
/// panic (deadlock detection), never a hang.
#[test]
fn stalled_pump_panics_deterministically() {
    session_init();
    install_client_pump(|| PumpStatus::Progress);
    let mut buf = [0u8; 4];
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = secure_read(&mut buf);
    }));
    assert!(r.is_err(), "stalled pump must panic, not spin");
}
