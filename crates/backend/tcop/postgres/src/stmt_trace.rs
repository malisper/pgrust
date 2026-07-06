// Per-statement phase timestamps, PGRUST_STMT_TRACE-gated (perf-attribution
// charter; GTRACE precedent in access/transam/parallel). Probes buffer
// thread-locally and flush one line per protocol cycle at ReadyForQuery so
// trace I/O never lands inside a bracketed phase.
use std::cell::RefCell;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

static STATE: AtomicU8 = AtomicU8::new(0); /* 0=unknown 1=off 2=on */

#[inline]
fn enabled() -> bool {
    match STATE.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => init_state(),
    }
}

#[cold]
fn init_state() -> bool {
    let on = std::env::var_os("PGRUST_STMT_TRACE").is_some();
    STATE.store(if on { 2 } else { 1 }, Ordering::Relaxed);
    on
}

thread_local! {
    static BUF: RefCell<Vec<(&'static str, u64)>> = const { RefCell::new(Vec::new()) };
}

fn now_ns() -> u64 {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

#[inline]
pub fn probe(phase: &'static str) {
    if enabled() {
        probe_slow(phase);
    }
}

#[cold]
fn probe_slow(phase: &'static str) {
    let t = now_ns();
    BUF.with(|b| b.borrow_mut().push((phase, t)));
}

// The read-complete probe names the message type so the parser can split
// simple-Q cycles from B/E/S extended cycles.
#[inline]
pub fn probe_read(firstchar: i32) {
    if !enabled() {
        return;
    }
    probe_slow(match firstchar as u8 {
        b'Q' => "read.Q",
        b'P' => "read.P",
        b'B' => "read.B",
        b'E' => "read.E",
        b'S' => "read.S",
        b'D' => "read.D",
        _ => "read.other",
    });
}

#[inline]
pub fn flush() {
    if enabled() {
        flush_slow();
    }
}

#[cold]
fn flush_slow() {
    BUF.with(|b| {
        let mut b = b.borrow_mut();
        if b.is_empty() {
            return;
        }
        let mut line = String::with_capacity(32 + b.len() * 24);
        line.push_str("STMTTRACE");
        for (p, t) in b.iter() {
            line.push(' ');
            line.push_str(p);
            line.push('=');
            line.push_str(&t.to_string());
        }
        b.clear();
        eprintln!("{line}");
    });
}
