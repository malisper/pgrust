//! pqcomm_simnet: the deterministic SIM-NET transport provider — an
//! in-memory duplex byte-stream pair (client end + server end) installed
//! into the §2.4 transport-provider seam slots (docs/design/dst-and-wasm.md;
//! P4 scoping row). THIRD provider behind the same set-once slots:
//!
//!   1. socket  (native default)   be_secure::init_seams + pqcomm::init_socket_seams
//!   2. stdio   (--stdio-wire)     pqcomm_stdio::init_transport_seams
//!   3. sim-net (--sim-net, HERE)  pqcomm_simnet::init_transport_seams
//!
//! Compiled ONLY under `--cfg pgrust_sim` (law 0.1: never into product
//! builds — the crate is empty otherwise, zero product codegen).
//!
//! DETERMINISM CONTRACT (this increment, single-session serial):
//! - No OS sockets, no fds, no poll: bytes live in two bounded in-memory
//!   queues (client→server, server→client). READINESS IS A PURE FUNCTION OF
//!   BUFFERED BYTES + end liveness — nothing ambient, nothing timed.
//! - Blocking ops park DETERMINISTICALLY: a would-block server op pumps the
//!   registered in-process client exactly at the block point (the serial
//!   stand-in for the P3 scheduler — later the scheduler drives multi-session
//!   interleaving through these SAME choke points; nothing here depends on
//!   it). A would-block READ with an empty peer buffer and NO LIVE WRITER is
//!   a clean session end (Ok(0) EOF), never a hang; a stalled pump (no state
//!   change) is a deterministic panic, never a spin.
//! - EVERY transport op consults an op-sequence counter AND the installed
//!   transport fault plan (inc-2), and appends one line to the op log —
//!   format aligned with the SimVfs fault-plan engine's fault_log
//!   (dst/p4-faults-inc1/2: `KIND seq=N key=value ...`, op_seq incremented
//!   per consult, first-rule-wins seeded rule plans, SUPPRESSED notes, log
//!   byte-stable across same-seed replays):
//!
//!   ```text
//!   NETOP seq=N op=Read end=S want=8192 got=54 c2s=54 s2c=0 decision=Proceed
//!   NETFAULT seq=N op=Read end=S want=8192 decision=Drop { keep: 2 }
//!   ```
//!
//!   The fault menu (`NetFaultDecision`): ShortRead / ShortWrite (partial
//!   recv/send), Delay (delayed delivery — a reorder within the
//!   deterministic schedule, head-of-line order preserved), Drop
//!   (connection drop mid-message, keeping N in-flight bytes), Reset (hard
//!   reset: ECONNRESET/EPIPE). A session under any plan either recovers or
//!   fails cleanly — never a hang, never nondeterministic.
//!
//! The wire bring-up above the slots (backend_startup::wire_session_initialize,
//! PostgresMain) is already transport-blind; the sim consumer never executes
//! any raw-IO site (the lint census carries no row for this crate).

#[cfg(pgrust_sim)]
mod imp;
#[cfg(pgrust_sim)]
pub use imp::*;
