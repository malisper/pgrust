//! SimNet implementation (sim-cfg only). See crate docs for the contract.

use std::cell::RefCell;
use std::collections::VecDeque;

use elog::ereport;
use types_error::{ErrorLocation, PgResult, ERROR};
use types_startup::{ClientSocket, Port};

/// Per-direction buffer capacity. Small enough that multi-row results
/// exercise the write-side pump/backpressure arms, large enough for whole
/// protocol messages to move per pump step. Deterministic constant.
pub const SIMNET_BUF_CAP: usize = 64 * 1024;

/// The virtual listen fd minted by the listen_server_port arm.
pub const SIMNET_LISTEN_FD: i32 = 9000;
/// The virtual per-connection fd carried in ClientSocket.
pub const SIMNET_CONN_FD: i32 = 9001;

/// What a pump step reports back to the blocked server op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PumpStatus {
    /// The client may make further progress if pumped again.
    Progress,
    /// The client script is exhausted: nothing more will ever be sent.
    /// Equivalent to the client half-closing its write side.
    Finished,
}

type Pump = Box<dyn FnMut() -> PumpStatus>;

struct SimNetState {
    /// client → server bytes.
    c2s: VecDeque<u8>,
    /// server → client bytes.
    s2c: VecDeque<u8>,
    /// Client write side live (false = server reads drain to EOF).
    client_open: bool,
    /// Server end live (secure_close flips it).
    server_open: bool,
    /// Server-side noblock mode bit (set_port_noblock).
    noblock: Option<bool>,
    /// The in-process client driven at server block points (serial
    /// increment). P3 replaces this with the scheduler.
    pump: Option<Pump>,
    /// Consulted (incremented) by EVERY transport op; the op log speaks
    /// these numbers — the inc-2 fault plan targets them.
    op_seq: u64,
    /// One line per op, byte-stable across same-script replays.
    op_log: Vec<String>,
    /// Client-observed transcript: every byte the client end received
    /// (server→client wire bytes, in order).
    client_received: Vec<u8>,
    /// Every byte the client end sent (client→server wire bytes, in order).
    client_sent: Vec<u8>,
    /// Virtual pending-connection queue for the accept arm.
    pending_accepts: VecDeque<()>,
}

impl SimNetState {
    fn new() -> Self {
        SimNetState {
            c2s: VecDeque::new(),
            s2c: VecDeque::new(),
            client_open: true,
            server_open: true,
            noblock: None,
            pump: None,
            op_seq: 0,
            op_log: Vec::new(),
            client_received: Vec::new(),
            client_sent: Vec::new(),
            pending_accepts: VecDeque::new(),
        }
    }

    fn log(&mut self, op: &str, end: char, want: usize, got: isize, decision: &str) {
        self.op_seq += 1;
        let seq = self.op_seq;
        let c2s = self.c2s.len();
        let s2c = self.s2c.len();
        self.op_log.push(format!(
            "NETOP seq={seq} op={op} end={end} want={want} got={got} c2s={c2s} s2c={s2c} decision={decision}"
        ));
    }
}

thread_local! {
    static SIM: RefCell<SimNetState> = RefCell::new(SimNetState::new());
}

fn with<R>(f: impl FnOnce(&mut SimNetState) -> R) -> R {
    SIM.with(|s| f(&mut s.borrow_mut()))
}

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("pqcomm_simnet.rs", 0, funcname)
}

// ---------------------------------------------------------------------------
// Harness / client-end API (the other half of the duplex pair).
// ---------------------------------------------------------------------------

/// Reset the pair to a fresh state (tests; one session per universe).
pub fn reset() {
    SIM.with(|s| *s.borrow_mut() = SimNetState::new());
}

/// Register the in-process client pump driven at server block points.
pub fn install_client_pump(pump: impl FnMut() -> PumpStatus + 'static) {
    with(|st| st.pump = Some(Box::new(pump)));
}

/// Client end: queue bytes toward the server. Unbounded acceptance is
/// deliberate on this end (the CLIENT is the pump; the server side is where
/// backpressure semantics matter), but the op is logged with the real sizes
/// so the fault plan can target it.
pub fn client_send(bytes: &[u8]) {
    with(|st| {
        st.c2s.extend(bytes.iter().copied());
        st.client_sent.extend_from_slice(bytes);
        st.log("ClientSend", 'C', bytes.len(), bytes.len() as isize, "Proceed");
    });
}

/// Client end: drain everything the server has written so far.
pub fn client_recv_all() -> Vec<u8> {
    with(|st| {
        let got: Vec<u8> = st.s2c.drain(..).collect();
        st.client_received.extend_from_slice(&got);
        st.log("ClientRecv", 'C', 0, got.len() as isize, "Proceed");
        got
    })
}

/// Client end: close the write side. Subsequent server reads drain the
/// remaining bytes, then observe clean EOF.
pub fn client_close() {
    with(|st| {
        st.client_open = false;
        st.log("ClientClose", 'C', 0, 0, "Proceed");
    });
}

/// Queue one virtual pending connection for the accept arm (P3-facing
/// surface; unused by the single-session serial increment's session path).
pub fn client_connect() {
    with(|st| {
        st.pending_accepts.push_back(());
        st.log("ClientConnect", 'C', 0, 0, "Proceed");
    });
}

/// The deterministic op log (fault-plan-aligned line format; see crate docs).
pub fn op_log() -> Vec<String> {
    with(|st| st.op_log.clone())
}

/// Ops consulted so far (the op-sequence counter the op log speaks).
pub fn op_seq() -> u64 {
    with(|st| st.op_seq)
}

/// Full client-observed wire transcript: (bytes sent, bytes received).
pub fn client_transcript() -> (Vec<u8>, Vec<u8>) {
    with(|st| (st.client_sent.clone(), st.client_received.clone()))
}

// ---------------------------------------------------------------------------
// The deterministic park: pump the client at the block point.
// ---------------------------------------------------------------------------

/// Fingerprint of everything a pump step may change; a step that changes
/// nothing is a stall.
fn fingerprint(st: &SimNetState) -> (usize, usize, bool, u64) {
    (st.c2s.len(), st.s2c.len(), st.client_open, st.op_seq)
}

/// Run one client pump step OUTSIDE the state borrow (the pump re-enters
/// through the client_* API). Returns the step's status; `Finished` marks
/// the client write side closed.
fn pump_once(what: &str) -> PumpStatus {
    let (mut pump, before) = with(|st| (st.pump.take(), fingerprint(st)));
    let Some(p) = pump.as_mut() else {
        // No client registered: a blocked op can never make progress. The
        // serial contract turns this into clean EOF semantics, not a hang.
        return PumpStatus::Finished;
    };
    let status = p();
    with(|st| {
        st.pump = pump;
        if status == PumpStatus::Finished {
            st.client_open = false;
        } else if fingerprint(st) == before {
            // Deterministic deadlock detection: a Progress step that moved
            // nothing would spin forever; fail loudly and reproducibly.
            panic!("pqcomm_simnet: client pump stalled during blocking {what} (deterministic deadlock)");
        }
    });
    status
}

// ---------------------------------------------------------------------------
// Server end: the seam-slot implementations.
// ---------------------------------------------------------------------------

/// secure_read over the pair: readiness is a pure function of `c2s` bytes +
/// client liveness. Interrupt-processing shape mirrors the other providers.
pub fn secure_read(buf: &mut [u8]) -> PgResult<Result<usize, i32>> {
    postgres_seams::process_client_read_interrupt::call(false)?;

    let want = buf.len();
    let res = loop {
        enum Step {
            Got(usize),
            Eof,
            WouldBlock,
            Park,
        }
        let step = with(|st| {
            if !st.c2s.is_empty() {
                let n = want.min(st.c2s.len());
                for b in buf.iter_mut().take(n) {
                    *b = st.c2s.pop_front().expect("len checked");
                }
                st.log("Read", 'S', want, n as isize, "Proceed");
                Step::Got(n)
            } else if !st.client_open {
                // Empty peer buffer + no live writer = clean session end.
                st.log("Read", 'S', want, 0, "Eof");
                Step::Eof
            } else if st.noblock.unwrap_or(false) {
                st.log("Read", 'S', want, -1, "WouldBlock");
                Step::WouldBlock
            } else {
                st.log("Read", 'S', want, -1, "Park");
                Step::Park
            }
        });
        match step {
            Step::Got(n) => break Ok(n),
            Step::Eof => break Ok(0),
            Step::WouldBlock => break Err(libc::EWOULDBLOCK),
            Step::Park => {
                // Deterministic park: drive the client; loop re-evaluates
                // readiness (Finished flips client_open → EOF next pass).
                let _ = pump_once("read");
            }
        }
    };

    postgres_seams::process_client_read_interrupt::call(false)?;

    Ok(res)
}

/// secure_write over the pair: capacity is a pure function of `s2c` room.
/// Partial writes are the caller's loop (pqcomm::internal_flush_buffer), as
/// with every provider.
pub fn secure_write(buf: &[u8]) -> PgResult<Result<usize, i32>> {
    postgres_seams::process_client_write_interrupt::call(false)?;

    let want = buf.len();
    let res = loop {
        enum Step {
            Put(usize),
            Pipe,
            WouldBlock,
            Park,
        }
        let step = with(|st| {
            let free = SIMNET_BUF_CAP.saturating_sub(st.s2c.len());
            if free > 0 {
                let n = want.min(free);
                st.s2c.extend(buf[..n].iter().copied());
                st.log("Write", 'S', want, n as isize, "Proceed");
                Step::Put(n)
            } else if !st.client_open {
                // Peer gone with the buffer full: the socket arm's EPIPE.
                st.log("Write", 'S', want, -1, "Pipe");
                Step::Pipe
            } else if st.noblock.unwrap_or(false) {
                st.log("Write", 'S', want, -1, "WouldBlock");
                Step::WouldBlock
            } else {
                st.log("Write", 'S', want, -1, "Park");
                Step::Park
            }
        });
        match step {
            Step::Put(n) => break Ok(n),
            Step::Pipe => break Err(libc::EPIPE),
            Step::WouldBlock => break Err(libc::EWOULDBLOCK),
            Step::Park => {
                let _ = pump_once("write");
            }
        }
    };

    postgres_seams::process_client_write_interrupt::call(false)?;

    Ok(res)
}

fn set_port_noblock(nb: bool) -> bool {
    with(|st| {
        if st.noblock.is_none() {
            // Mirrors the other providers' "no client connection" answer
            // before pq_init.
            return false;
        }
        st.noblock = Some(nb);
        st.log("Noblock", 'S', nb as usize, 0, "Proceed");
        true
    })
}

fn secure_close() {
    with(|st| {
        st.server_open = false;
        st.log("Close", 'S', 0, 0, "Proceed");
    });
}

/// pq_init, sim shape: no socket, no wait set (readiness never parks on the
/// OS — blocking is the deterministic pump above). Zeroed addresses =
/// "client address unknown", as on the stdio provider.
fn pq_init(client_sock: &ClientSocket) -> PgResult<Port> {
    let port = Port::new(client_sock);
    pqcomm::pq_init_buffers()?;
    with(|st| {
        st.noblock = Some(false);
        st.log("Init", 'S', 0, 0, "Proceed");
    });
    Ok(port)
}

fn modify_fe_be_wait_set_latch(_latch: types_storage::latch::LatchHandle) -> PgResult<()> {
    Ok(())
}

/// A ClientSocket bound to the pair (virtual fd; zeroed raddr).
pub fn simnet_client_socket() -> ClientSocket {
    ClientSocket { sock: SIMNET_CONN_FD, raddr: ip_zeroed() }
}

fn ip_zeroed() -> ip::SockAddr {
    ip::SockAddr::zeroed()
}

// ---------------------------------------------------------------------------
// Provider install.
// ---------------------------------------------------------------------------

/// Install the sim-net provider into the transport seam slots — the third
/// provider; same boot-time counterpart shape as
/// `pqcomm_stdio::init_transport_seams` / the socket half of
/// `pqcomm::init_socket_seams`. Exactly one provider installs per process
/// (seam_core's install-twice panic enforces it).
///
/// The listen/accept pair installs VIRTUAL arms (the reason
/// pqcomm::init_seams was split): listen mints [`SIMNET_LISTEN_FD`]; accept
/// pops connections queued by [`client_connect`]. The single-session serial
/// increment's session path does not go through them (no postmaster), but
/// the slots are owned and logged so the P3 scheduler can drive
/// multi-session accepts through the same choke points.
pub fn init_transport_seams() {
    be_secure_seams::secure_read::set(secure_read);
    be_secure_seams::secure_write::set(secure_write);
    be_secure_seams::secure_close::set(secure_close);
    be_secure_seams::set_port_noblock::set(set_port_noblock);
    be_secure_seams::be_tls_get_certificate_hash::set(|| {
        ereport(ERROR)
            .errmsg_internal("channel binding is not supported on the sim-net transport")
            .finish(loc("be_tls_get_certificate_hash"))
            .map(|()| Vec::new())
    });
    pqcomm_seams::pq_init::set(pq_init);
    pqcomm_seams::modify_fe_be_wait_set_latch::set(modify_fe_be_wait_set_latch);
    pqcomm_seams::listen_server_port::set(|_host, _port, _dir, listen_sockets, _max| {
        with(|st| st.log("Listen", 'S', 0, SIMNET_LISTEN_FD as isize, "Proceed"));
        listen_sockets.push(SIMNET_LISTEN_FD);
        Ok(())
    });
    pqcomm_seams::accept_connection::set(|_server_fd| {
        let pending = with(|st| {
            let got = st.pending_accepts.pop_front().is_some();
            st.log("Accept", 'S', 0, if got { SIMNET_CONN_FD as isize } else { -1 }, if got { "Proceed" } else { "WouldBlock" });
            got
        });
        if pending {
            Ok(simnet_client_socket())
        } else {
            Err(Box::new(types_error::PgError::new(
                types_error::LOG,
                "no pending sim-net connection",
            )))
        }
    });
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
