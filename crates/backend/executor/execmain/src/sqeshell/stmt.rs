//! The sqe statement plane, P2-1 slice: arming posture + the dispatch
//! admission facts. The lanev4 lx4stmt ProgCache substrate (route memo /
//! lowered cache / refusal memo over plancache identity) is a recorded
//! P2-1 remainder — this slice lowers per execution (correct; the memo
//! is a tax optimization measured by the C8 burn-down, not a semantics
//! carrier). The one-relaxed-load OFF/heap arm is preserved: heap
//! statements pay `armed_posture()` (one relaxed load) plus the
//! first-mismatch AM walk, nothing else.

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

/// Posture per R1: the engine serves columnar tables by default; the
/// kill switch turns columnar service into typed EngineDisarmed errors
/// ("kills columnar service loudly rather than silently degrading").
/// `PGRUST_SQE_SLOT=0` bypasses the dispatch slot entirely — a
/// measurement-only lever for armed-vs-off OLTP pairs (heap corpora);
/// it is NOT a lenient columnar mode: bypassed columnar SELECTs run the
/// row-engine correctness scan, which the R1 doctrine forbids as a
/// standing posture.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Posture {
    Armed,
    Killed,
    SlotBypassed,
}

const UNRESOLVED: u8 = 0;
const ARMED: u8 = 1;
const KILLED: u8 = 2;
const BYPASSED: u8 = 3;

static POSTURE: AtomicU8 = AtomicU8::new(UNRESOLVED);

#[cold]
#[inline(never)]
fn resolve_posture() -> u8 {
    let v = if matches!(
        std::env::var("PGRUST_SQE_SLOT").as_deref(),
        Ok("0") | Ok("off")
    ) {
        BYPASSED
    } else if matches!(std::env::var("PGRUST_SQE").as_deref(), Ok("0") | Ok("off")) {
        KILLED
    } else {
        ARMED
    };
    POSTURE.store(v, Relaxed);
    v
}

/// One relaxed load on the steady state (the OLTP-invisibility
/// load-bearing piece — lanev4 lx4stmt:70-100 idiom).
#[inline]
pub fn posture() -> Posture {
    let v = match POSTURE.load(Relaxed) {
        UNRESOLVED => resolve_posture(),
        v => v,
    };
    match v {
        ARMED => Posture::Armed,
        KILLED => Posture::Killed,
        _ => Posture::SlotBypassed,
    }
}

/// Whether the dispatch slot runs at all (heap fast arm: one relaxed
/// load + branch; the AM walk only runs when this is true).
#[inline]
pub fn slot_active() -> bool {
    let v = match POSTURE.load(Relaxed) {
        UNRESOLVED => resolve_posture(),
        v => v,
    };
    v != BYPASSED
}

/// The born-RED seed lever (census-surface §4): a production-compiled
/// refusal seed — when `PGRUST_SQE_SEED_REFUSAL=<variant-key>` is set,
/// `lower()`'s entry gate refuses every columnar statement with the
/// seeded cause through the ordinary constructor. Off (the only
/// production state) costs one cached Option load per lowering.
pub fn seed_refusal() -> Option<super::refusal::RefuseCause> {
    use pgsync::OnceLock;
    static SEED: OnceLock<Option<super::refusal::RefuseCause>> = OnceLock::new();
    *SEED.get_or_init(|| {
        let key = std::env::var("PGRUST_SQE_SEED_REFUSAL").ok()?;
        match super::refusal::RefuseCause::from_seed_key(&key) {
            Some(c) => Some(c),
            None => {
                // Seeding a nonexistent key fails loudly at first use.
                panic!("PGRUST_SQE_SEED_REFUSAL names no refusal variant: {key:?}");
            }
        }
    })
}
