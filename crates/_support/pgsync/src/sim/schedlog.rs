//! The SCHEDOP schedule log (contract §3.3).
//!
//! Op-seq numbered, in-memory ring + dump-on-exit/dump-on-demand.
//! Replayable = re-run the seed (single-run semantics); the log is for
//! diagnosis, not search. EVERY scheduling decision logs, not only picks —
//! the simnet lane's "op log records EVERY op" precedent is deliberately the
//! stronger replay gate: same-seed ⇒ byte-identical SCHEDOP stream is the
//! acceptance smoke.
//!
//! Format, aligned with the NETOP/fault-plan conventions (same seq-space
//! style, one line per op, grep-stable prefix):
//!
//! ```text
//! SCHEDOP <seq> <vpid> <op-class> site=<file:line> [k=v ...]
//! ```
//!
//! `<vpid>` is the acting thread's vpid, `-` for a thread outside the model
//! acting on the registry (an unregistered parent registering a child).
//! `site=-` marks ops with no natural caller site (Grant/Advance emitted
//! inside handoff).
//!
//! Byte-identity discipline: a line may contain ONLY schedule-determined
//! fields — seq, vpid, op-class, `#[track_caller]` site, pick indices,
//! virtual-time values. Never wall time, never addresses, never OS tids.
//!
//! No interior locking: the ring lives inside the scheduler's state mutex,
//! which serializes emissions — seq order IS schedule order.

use std::collections::VecDeque;
use std::panic::Location;

use super::hooks::Vpid;

/// Default ring capacity (lines). Corpora are loom-fast-sized; 64k lines is
/// plenty for diagnosis while bounding memory (no-large-memory-caches law).
pub const DEFAULT_LOG_CAP: usize = 65_536;

pub struct SchedLog {
    seq: u64,
    cap: usize,
    /// When set, every line is ALSO written to stderr as emitted
    /// (PGRUST_SIM_SCHEDLOG=stream) — WS-DEMO's byte-compare capture.
    stream: bool,
    ring: VecDeque<String>,
}

impl SchedLog {
    pub fn new(cap: usize, stream: bool) -> Self {
        SchedLog {
            seq: 0,
            cap: cap.max(1),
            stream,
            ring: VecDeque::new(),
        }
    }

    /// Emit one SCHEDOP line.
    pub fn emit(
        &mut self,
        vpid: Option<Vpid>,
        op: &str,
        site: Option<&'static Location<'static>>,
        extras: &[(&str, String)],
    ) -> u64 {
        let seq = self.seq;
        self.seq += 1;
        let actor = match vpid {
            Some(v) => v.to_string(),
            None => "-".to_string(),
        };
        let mut line = match site {
            Some(l) => format!("SCHEDOP {seq} {actor} {op} site={}:{}", l.file(), l.line()),
            None => format!("SCHEDOP {seq} {actor} {op} site=-"),
        };
        for (k, v) in extras {
            line.push(' ');
            line.push_str(k);
            line.push('=');
            line.push_str(v);
        }
        if self.stream {
            eprintln!("{line}");
        }
        if self.ring.len() == self.cap {
            self.ring.pop_front();
        }
        self.ring.push_back(line);
        seq
    }

    /// The full retained stream, one line per op (dump-on-demand).
    pub fn dump(&self) -> String {
        let mut out = String::new();
        for l in &self.ring {
            out.push_str(l);
            out.push('\n');
        }
        out
    }

    /// The last `n` lines (watchdog/deadlock report tails).
    pub fn tail(&self, n: usize) -> String {
        let skip = self.ring.len().saturating_sub(n);
        let mut out = String::new();
        for l in self.ring.iter().skip(skip) {
            out.push_str(l);
            out.push('\n');
        }
        out
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
}
