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

/// INC-2 review observation 2, the tighten's red: a pump that only CONSULTS
/// (an empty recv bumps op_seq and logs) but moves no byte and changes no
/// liveness is a protocol stall — it must trip the deterministic panic, not
/// spin on op_seq "progress" until an external watchdog kills the process.
#[test]
fn byte_silent_op_consulting_pump_panics() {
    session_init();
    install_client_pump(|| {
        // Consults the plan, bumps op_seq, appends a NETOP line — and moves
        // nothing (s2c is empty).
        let _ = client_recv_all();
        PumpStatus::Progress
    });
    let mut buf = [0u8; 4];
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = secure_read(&mut buf);
    }));
    assert!(r.is_err(), "byte-silent op-consulting pump must panic deterministically");
}

/// SIM-CONVERGE inc-2: a pump that YIELDS (no byte progress, not finished) is
/// a LEGAL cross-session turn-wait — NOT the stall panic. The blocking read
/// re-evaluates until the pump makes real progress and never panics on the
/// no-progress yields. In the real corpus each yield is preceded by a
/// scheduler TimedPark (so a wedged turn reaches SCHEDCEILING, a named
/// verdict); here the bounded yield counter stands in for that park.
#[test]
fn yielded_pump_is_a_legal_turn_wait_not_a_stall() {
    session_init();
    let mut yields_left = 3u32;
    install_client_pump(move || {
        if yields_left > 0 {
            yields_left -= 1;
            // Not my turn: no byte moved, not done. Without the Yielded
            // exemption this is exactly the byte_silent stall panic.
            return PumpStatus::Yielded;
        }
        client_send(b"go");
        PumpStatus::Finished
    });
    let mut buf = [0u8; 8];
    // Must NOT panic on the three yields; resolves to the 2-byte send.
    let n = secure_read(&mut buf).unwrap().unwrap();
    assert_eq!(&buf[..n], b"go", "yields must resolve to real progress, never panic");
}

// ===========================================================================
// INC-2: the transport fault menu.
// ===========================================================================

/// Partial recv: a sticky ShortRead(1) on the server read arm delivers one
/// byte per op; the caller's loop (here explicit, pqcomm's in product)
/// reassembles the identical byte stream. NETFAULT lines are seq-numbered.
#[test]
fn fault_short_read_is_partial_but_lossless() {
    session_init();
    SeededNetFaultPlan::install(
        0x5EED,
        vec![NetFaultRule {
            matcher: NetOpMatch::kind(NetOpKind::Read),
            nth: 1,
            action: NetFaultDecision::ShortRead(1),
            sticky: true,
        }],
    );
    client_send(b"hello world");
    let mut got = Vec::new();
    let mut buf = [0u8; 16];
    while got.len() < 11 {
        let n = secure_read(&mut buf).unwrap().unwrap();
        assert_eq!(n, 1, "sticky ShortRead(1) must cap every read at 1 byte");
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got, b"hello world");
    let log = op_log();
    assert!(log.iter().any(|l| l.contains("NETFAULT") && l.contains("op=Read")));
    assert!(log.iter().any(|l| l.contains("decision=Short")));
}

/// Partial send: a sticky ShortWrite(3) on the server write arm; the
/// caller's loop completes the transfer; the client sees the identical
/// bytes.
#[test]
fn fault_short_write_caller_loop_completes() {
    session_init();
    SeededNetFaultPlan::install(
        0x5EED,
        vec![NetFaultRule {
            matcher: NetOpMatch::kind(NetOpKind::Write),
            nth: 1,
            action: NetFaultDecision::ShortWrite(3),
            sticky: true,
        }],
    );
    let payload = b"the quick brown fox";
    let mut wrote = 0;
    while wrote < payload.len() {
        let n = secure_write(&payload[wrote..]).unwrap().unwrap();
        assert!(n <= 3, "ShortWrite(3) must cap each write");
        wrote += n;
    }
    assert_eq!(client_recv_all(), payload);
}

/// Delayed delivery: a Delay(5) on the client's send stages the message;
/// the server's blocking read Holds (each hold consults, advancing the op
/// clock deterministically) and then delivers the identical bytes — a
/// reorder within the deterministic schedule, never a loss.
#[test]
fn fault_delayed_delivery_holds_then_delivers() {
    session_init();
    SeededNetFaultPlan::install(
        0x5EED,
        vec![NetFaultRule::nth_matching(
            NetOpMatch::kind(NetOpKind::ClientSend),
            1,
            NetFaultDecision::Delay(5),
        )],
    );
    install_client_pump(|| {
        client_send(b"delayed message");
        PumpStatus::Progress
    });
    let mut buf = [0u8; 32];
    let n = secure_read(&mut buf).unwrap().unwrap();
    assert_eq!(&buf[..n], b"delayed message");
    let log = op_log();
    assert!(log.iter().any(|l| l.contains("decision=Delay")), "NETFAULT Delay line");
    assert!(log.iter().any(|l| l.contains("decision=Hold")), "Hold steps while staged");
}

/// Delay preserves stream order: a delayed first send holds a later
/// undelayed send behind it (head-of-line), so the server never sees
/// reordered bytes.
#[test]
fn fault_delay_preserves_head_of_line_order() {
    session_init();
    SeededNetFaultPlan::install(
        0x5EED,
        vec![NetFaultRule::nth_matching(
            NetOpMatch::kind(NetOpKind::ClientSend),
            1,
            NetFaultDecision::Delay(8),
        )],
    );
    client_send(b"first-"); // delayed
    client_send(b"second"); // must queue BEHIND the staged segment
    let mut got = Vec::new();
    let mut buf = [0u8; 32];
    while got.len() < 12 {
        let n = secure_read(&mut buf).unwrap().unwrap();
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got, b"first-second");
}

/// Connection drop mid-message: only the kept prefix of the in-flight bytes
/// survives; the server drains it and then observes EOF (mid-message).
#[test]
fn fault_drop_mid_message_truncates_then_eof() {
    session_init();
    SeededNetFaultPlan::install(
        0x5EED,
        vec![NetFaultRule::nth_matching(
            NetOpMatch::kind(NetOpKind::Read),
            1,
            NetFaultDecision::Drop { keep: 3 },
        )],
    );
    client_send(b"12345678");
    let mut buf = [0u8; 16];
    let n = secure_read(&mut buf).unwrap().unwrap();
    assert_eq!(&buf[..n], b"123", "only the kept in-flight prefix survives the drop");
    let n = secure_read(&mut buf).unwrap().unwrap();
    assert_eq!(n, 0, "mid-message EOF after the drop");
}

/// Hard reset: nothing in flight survives; reads fail ECONNRESET, writes
/// fail EPIPE — deterministically, never a hang.
#[test]
fn fault_reset_read_econnreset_write_epipe() {
    session_init();
    SeededNetFaultPlan::install(
        0x5EED,
        vec![NetFaultRule::nth_matching(
            NetOpMatch::kind(NetOpKind::Read),
            1,
            NetFaultDecision::Reset,
        )],
    );
    client_send(b"never delivered");
    let mut buf = [0u8; 16];
    assert_eq!(secure_read(&mut buf).unwrap(), Err(libc::ECONNRESET));
    assert_eq!(secure_write(b"x").unwrap(), Err(libc::EPIPE));
    let log = op_log();
    assert!(log.iter().any(|l| l.contains("decision=Reset")));
}

/// The SimVfs N5 convention on the wire: a losing rule whose nth firing is
/// consumed by a higher-priority rule logs a SUPPRESSED note (and the
/// firing stays consumed).
#[test]
fn suppressed_rule_firing_is_logged() {
    session_init();
    SeededNetFaultPlan::install(
        0x5EED,
        vec![
            NetFaultRule::nth_matching(
                NetOpMatch::kind(NetOpKind::Read),
                1,
                NetFaultDecision::ShortRead(1),
            ),
            NetFaultRule::nth_matching(
                NetOpMatch::kind(NetOpKind::Read),
                1,
                NetFaultDecision::Drop { keep: 0 },
            ),
        ],
    );
    client_send(b"abc");
    let mut buf = [0u8; 16];
    let n = secure_read(&mut buf).unwrap().unwrap();
    assert_eq!(n, 1, "the first rule (priority) fires");
    let log = op_log();
    assert!(
        log.iter().any(|l| l.contains("SUPPRESSED rule#1")),
        "the losing rule's consumed firing must be logged: {log:?}"
    );
    // The loser's nth was consumed: the next read proceeds unfaulted.
    let n = secure_read(&mut buf).unwrap().unwrap();
    assert_eq!(n, 2);
}

/// Replay identity UNDER A FAULT PLAN: same script + same (seed, rules) ⇒
/// byte-identical op logs (NETPLAN/NETFAULT/NOTE lines included) and
/// transcripts. The fault log IS the op log — a fault run replays from it.
#[test]
fn fault_plan_replay_identity() {
    install_once();

    fn run_script() -> (Vec<String>, Vec<u8>, Vec<u8>) {
        reset();
        let cs = simnet_client_socket();
        let _port = pqcomm_seams::pq_init::call(&cs).expect("pq_init");
        SeededNetFaultPlan::install(
            0xFA_017,
            vec![
                NetFaultRule {
                    matcher: NetOpMatch::kind(NetOpKind::Read),
                    nth: 2,
                    action: NetFaultDecision::ShortRead(2),
                    sticky: false,
                },
                NetFaultRule::nth_matching(
                    NetOpMatch::kind(NetOpKind::ClientSend),
                    2,
                    NetFaultDecision::Delay(4),
                ),
            ],
        );
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
                    client_send(b"QRY two"); // delayed by rule 2
                    PumpStatus::Progress
                }
                _ => PumpStatus::Finished,
            }
        });
        let mut buf = [0u8; 7];
        let mut msg = Vec::new();
        while msg.len() < 7 {
            let n = secure_read(&mut buf).unwrap().unwrap();
            msg.extend_from_slice(&buf[..n]);
        }
        assert_eq!(msg, b"QRY one");
        let _ = secure_write(b"RSP one").unwrap().unwrap();
        let mut msg = Vec::new();
        while msg.len() < 7 {
            let n = secure_read(&mut buf).unwrap().unwrap();
            msg.extend_from_slice(&buf[..n]);
        }
        assert_eq!(msg, b"QRY two");
        let _ = secure_write(b"RSP two").unwrap().unwrap();
        let n = secure_read(&mut buf).unwrap().unwrap();
        assert_eq!(n, 0);
        let _ = client_recv_all();
        let (sent, received) = client_transcript();
        (op_log(), sent, received)
    }

    let (log1, sent1, recv1) = run_script();
    let (log2, sent2, recv2) = run_script();
    assert_eq!(log1, log2, "fault-run op logs must be byte-identical across replays");
    assert_eq!(sent1, sent2);
    assert_eq!(recv1, recv2);
    assert!(log1.iter().any(|l| l.starts_with("NETPLAN seed=0x")));
    assert!(log1.iter().any(|l| l.starts_with("NETFAULT seq=")));
}

/// The PGRUST_SIMNET_FAULTS grammar parses into the intended rules.
#[test]
fn parse_fault_spec_grammar() {
    let (seed, rules) =
        parse_fault_spec("seed=0x5EED Read@12=drop:2 ClientSend@3=delay:9 Read@1!=shortread:1 Any@7=reset Write@4=shortwrite:8");
    assert_eq!(seed, 0x5EED);
    assert_eq!(rules.len(), 5);
    assert_eq!(rules[0].nth, 12);
    assert_eq!(rules[0].action, NetFaultDecision::Drop { keep: 2 });
    assert!(!rules[0].sticky);
    assert_eq!(rules[1].action, NetFaultDecision::Delay(9));
    assert_eq!(rules[2].action, NetFaultDecision::ShortRead(1));
    assert!(rules[2].sticky);
    assert!(rules[3].matcher.kinds.is_none(), "Any matches every kind");
    assert_eq!(rules[3].action, NetFaultDecision::Reset);
    assert_eq!(rules[4].action, NetFaultDecision::ShortWrite(8));
}

/// A Delay beyond SIMNET_MAX_DELAY is a deterministic panic at decision
/// time (unbounded delays are unbounded logs, and saturated ones hangs).
#[test]
fn fault_delay_over_bound_panics() {
    session_init();
    SeededNetFaultPlan::install(
        0x5EED,
        vec![NetFaultRule::nth_matching(
            NetOpMatch::kind(NetOpKind::ClientSend),
            1,
            NetFaultDecision::Delay(SIMNET_MAX_DELAY + 1),
        )],
    );
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        client_send(b"x");
    }));
    assert!(r.is_err(), "over-bound Delay must panic loudly");
}
