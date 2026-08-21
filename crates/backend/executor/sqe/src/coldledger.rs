//! [coldstart] First-touch ledger: every derived structure the engine
//! builds (standing faces, verdict planes, condcache publications, dict
//! handles ...) records (kind, key, wall ns, rep, thread class) here at
//! build time — a Vec push, no print inside the timed window. The official
//! child drains it AFTER its timed reps and prints one `COLDFACE|` line per
//! (kind, rep) aggregate plus a `COLDLEDGER|` total, so the cold rep's
//! wall decomposes into face-build vs the rest without touching the
//! measured region. Serial = built on the query's driving thread (the
//! build is on the wall clock 1:1); par = built inside a pool worker.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread::ThreadId;

/// [FACE-BUILD LAW] Why a derived structure is allowed to build inside
/// the cold rep. Every registered build declares one; `None` fails the
/// official cold leg (the driver's gate) so a new plane cannot silently
/// re-introduce first-touch cost.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Reason {
    /// the query's own claim space / touched parts need it (walk, dict
    /// faces of touched parts, verdict planes over the predicate's dict)
    TouchedByQuery,
    /// building it is cheaper than the plain path it replaces (a zone map
    /// that prunes the scan; a sortedness witness that elects run-collapse)
    CheaperThanPlain,
    /// populate/persist only on the SECOND touch of an identity (condcache)
    SecondTouch,
    /// derived from the flat stats / digest sections, no row data touched
    FlatStatsDerived,
    /// no justification declared — the gate fails the leg
    None,
}

impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Reason::TouchedByQuery => "touched-by-query",
            Reason::CheaperThanPlain => "cheaper-than-plain",
            Reason::SecondTouch => "second-touch",
            Reason::FlatStatsDerived => "flat-stats-derived",
            Reason::None => "none",
        }
    }
}

struct Entry {
    kind: &'static str,
    key: String,
    ns: u64,
    rep: u32,
    serial: bool,
    bytes: u64,
    reason: Reason,
}

static LOG: Mutex<Vec<Entry>> = Mutex::new(Vec::new());
static REP: AtomicU32 = AtomicU32::new(0);
static MAIN: Mutex<Option<ThreadId>> = Mutex::new(None);
static ENABLED: AtomicU64 = AtomicU64::new(1);

/// The rep counter the entries are tagged with (0 = before timing: plan/
/// election-time builds; 1 = the cold rep; 2+ = hot reps).
pub fn set_rep(r: u32) {
    REP.store(r, Ordering::Relaxed);
}

pub fn rep() -> u32 {
    REP.load(Ordering::Relaxed)
}

/// Declare the driving thread (the sqe child's main thread).
pub fn set_main_thread() {
    *MAIN.lock().unwrap() = Some(std::thread::current().id());
}

fn on_main() -> bool {
    (*MAIN.lock().unwrap()).map(|m| m == std::thread::current().id()).unwrap_or(true)
}

/// Record one build. `bytes` = resident payload the build produced (0 if
/// unknown/not applicable).
pub fn note(
    kind: &'static str,
    key: impl Into<String>,
    t0: std::time::Instant,
    bytes: u64,
    reason: Reason,
) {
    if ENABLED.load(Ordering::Relaxed) == 0 {
        return;
    }
    let ns = t0.elapsed().as_nanos() as u64;
    let e = Entry { kind, key: key.into(), ns, rep: rep(), serial: on_main(), bytes, reason };
    LOG.lock().unwrap().push(e);
}

/// Statement-end hygiene for processes that never drain (live backends):
/// drop accumulated entries without formatting anything.
pub fn clear() {
    LOG.lock().unwrap().clear();
}

/// Drain into printable lines (the rig prints them; the census/EXPLAIN
/// surface consumes them at P1-4). Aggregates by (rep, kind): count, serial ms, par ms,
/// bytes; the `--phase`-style detail (`COLDFACE|...|detail`) lists the
/// heaviest 12 individual builds so the attribution names them.
pub fn drain_lines(q: u32, arm: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let v: Vec<Entry> = std::mem::take(&mut *LOG.lock().unwrap());
    if v.is_empty() {
        out.push(format!("COLDLEDGER|q={q}|arm={arm}|builds=0"));
        return out;
    }
    use std::collections::BTreeMap;
    let mut agg: BTreeMap<(u32, &'static str, &'static str), (u64, f64, f64, u64)> = BTreeMap::new();
    for e in &v {
        let a = agg.entry((e.rep, e.kind, e.reason.as_str())).or_insert((0, 0.0, 0.0, 0));
        a.0 += 1;
        if e.serial {
            a.1 += e.ns as f64 / 1e6;
        } else {
            a.2 += e.ns as f64 / 1e6;
        }
        a.3 += e.bytes;
    }
    let mut tot_serial = [0.0f64; 4];
    let mut tot_par = [0.0f64; 4];
    for ((rep, kind, reason), (n, s, p, b)) in &agg {
        out.push(format!(
            "COLDFACE|q={q}|arm={arm}|rep={rep}|kind={kind}|n={n}|serial_ms={s:.3}|par_ms={p:.3}|bytes={b}|reason={reason}"
        ));
        // FACE-BUILD LAW witness: one line per (face, rep) — the official
        // driver fails the leg on reason=none.
        if *rep >= 1 {
            out.push(format!(
                "FACE|{kind}|q={q}|rep={rep}|built_ms={:.3}|bytes={b}|n={n}|reason={reason}",
                s + p
            ));
        }
        let r = (*rep as usize).min(3);
        tot_serial[r] += s;
        tot_par[r] += p;
    }
    let mut heavy: Vec<&Entry> = v.iter().collect();
    heavy.sort_by(|a, b| b.ns.cmp(&a.ns));
    for e in heavy.iter().take(12) {
        out.push(format!(
            "COLDFACE|q={q}|arm={arm}|detail|rep={}|kind={}|key={}|ms={:.3}|serial={}|bytes={}|reason={}",
            e.rep,
            e.kind,
            e.key,
            e.ns as f64 / 1e6,
            e.serial as u8,
            e.bytes,
            e.reason.as_str()
        ));
    }
    out.push(format!(
        "COLDLEDGER|q={q}|arm={arm}|builds={}|plan_serial_ms={:.3}|plan_par_ms={:.3}|cold_serial_ms={:.3}|cold_par_ms={:.3}|hot_serial_ms={:.3}|hot_par_ms={:.3}",
        v.len(),
        tot_serial[0],
        tot_par[0],
        tot_serial[1],
        tot_par[1],
        tot_serial[2] + tot_serial[3],
        tot_par[2] + tot_par[3]
    ));
    out
}
