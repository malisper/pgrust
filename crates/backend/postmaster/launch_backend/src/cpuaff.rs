//! cpuaff: CPU affinity for pool threads (increment A of the affinity
//! program). Default OFF; `PGRUST_CPU_AFFINITY=1` (exact spelling) arms it,
//! `PGRUST_CPU_AFFINITY_SET` optionally names the pool core list (e.g.
//! "1-14" or "0,2,4"; default = every core of the boot mask, discovered via
//! sched_getaffinity).
//!
//! Placement policy: rtworker ordinal i and rtgang ordinal i share the i-th
//! core of the affinity set (identity map; ordinals beyond the set WRAP
//! modulo its size). The rtworkers' standby ordinals (>= workers) and the
//! wpool standbys get the FULL set mask, not a single core — a standby runs
//! only on a donated permit while the donor blocks in a syscall, so it needs
//! the freedom to land on any pool core. Everything else in the process
//! (sessions, housekeeping daemons, per-statement helper threads) is
//! deliberately untouched in increment A.
//!
//! Vocabulary: this module says "affinity"/"cpuaff" throughout. The
//! scheduler's `PinBoard`/`submit_pinned` are LOGICAL task-lane terms and
//! are unrelated to OS core placement.
//!
//! Mechanism: libc::sched_setaffinity on the calling thread (Linux only;
//! a no-op elsewhere so laptop dev is unaffected). Fail-open: a failed set
//! degrades loud-once ("running unpinned") and the thread keeps running —
//! affinity is a placement attribute, never a boot blocker.
//!
//! The policy + boards are computed ONCE at rtpool::start (postmaster
//! thread, before any pool thread spawns; the standing gang spawns lazily
//! after that boot point and mirrors the map). Observed assignments are
//! recorded in ordinal-indexed AffinityBoards (the ring-board shape,
//! runtime `rings`), populated from the affine thread itself via a post-set
//! read-back.

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering::Relaxed};

use pgsync::OnceLock;

/// cpu_set_t width (CPU_SETSIZE): the hard upper bound on nameable cores.
const CPU_SET_WIDTH: usize = 1024;

/// The affinity policy: the pool core set (ascending, deduped) plus the
/// worker count that splits singleton ordinals from standby ordinals.
pub struct AffinityPolicy {
    cpus: Vec<usize>,
    workers: usize,
}

impl AffinityPolicy {
    /// rtworker ordinal -> its affinity set: ordinals < workers get the
    /// singleton i-th core (modulo the set size); standby ordinals get the
    /// whole set.
    fn rtworker_set(&self, ordinal: usize) -> &[usize] {
        if ordinal < self.workers {
            std::slice::from_ref(&self.cpus[ordinal % self.cpus.len()])
        } else {
            &self.cpus
        }
    }

    /// rtgang ordinal -> the SAME singleton map as rtworkers (core i hosts
    /// rtworker i and rtgang i; they are permit/engagement-gated so they
    /// never both burn core i steadily).
    fn rtgang_set(&self, ordinal: usize) -> &[usize] {
        std::slice::from_ref(&self.cpus[ordinal % self.cpus.len()])
    }

    /// The full pool core set (standby mask).
    fn pool_set(&self) -> &[usize] {
        &self.cpus
    }
}

// Written once by rtpool::start on the postmaster thread (plain set/get —
// no lazy init, no waiting losers); unset = affinity OFF.
static POLICY: OnceLock<AffinityPolicy> = OnceLock::new();

// AffinityBoards: ordinal-indexed observed singleton assignments (-1 =
// unobserved; standbys/full-mask threads never post). Written by each
// affine thread after its own successful set.
static RTWORKER_BOARD: OnceLock<Vec<AtomicI32>> = OnceLock::new();
static RTGANG_BOARD: OnceLock<Vec<AtomicI32>> = OnceLock::new();

/// Whether `PGRUST_CPU_AFFINITY` arms the feature: exactly "1" (the exact-
/// spelling knob discipline; unset/0/anything-else = OFF).
fn affinity_requested() -> bool {
    std::env::var("PGRUST_CPU_AFFINITY").as_deref() == Ok("1")
}

/// Compute + publish the policy and boards, and print the boot witness.
/// rtpool::start only (postmaster thread, BEFORE any pool spawn); no-op
/// when the knob is off. `gang` sizes the rtgang board (its spawner is
/// installed after rtpool::start and spawns lazily, so the map is always
/// published first).
pub(crate) fn install_from_env(workers: usize, standbys: usize, gang: usize) {
    if !affinity_requested() {
        return;
    }
    let boot = current_thread_cpus();
    if boot.is_empty() {
        degrade_loud_once("the boot cpu set is undiscoverable on this platform");
        return;
    }
    let cpus = match std::env::var("PGRUST_CPU_AFFINITY_SET") {
        Err(_) => boot,
        Ok(raw) => match parse_cpu_list(&raw) {
            None => {
                eprintln!(
                    "pgrust: cpu affinity: PGRUST_CPU_AFFINITY_SET ({raw:?}) is \
                     unparseable; using the boot mask"
                );
                boot
            }
            Some(req) => {
                // Requested cores outside the boot mask would EINVAL at set
                // time; intersect loudly instead (fail-open to the boot mask
                // when nothing survives).
                let inter: Vec<usize> =
                    req.iter().copied().filter(|c| boot.contains(c)).collect();
                if inter.is_empty() {
                    eprintln!(
                        "pgrust: cpu affinity: PGRUST_CPU_AFFINITY_SET ({raw:?}) has no \
                         cores inside the boot mask; using the boot mask"
                    );
                    boot
                } else {
                    if inter.len() < req.len() {
                        eprintln!(
                            "pgrust: cpu affinity: PGRUST_CPU_AFFINITY_SET names cores \
                             outside the boot mask; narrowed to {}",
                            format_cpu_list(&inter)
                        );
                    }
                    inter
                }
            }
        },
    };
    let witness = format_cpu_list(&cpus);
    let _ = POLICY.set(AffinityPolicy { cpus, workers });
    let _ = RTWORKER_BOARD.set((0..workers + standbys).map(|_| AtomicI32::new(-1)).collect());
    let _ = RTGANG_BOARD.set((0..gang).map(|_| AtomicI32::new(-1)).collect());
    // The boot witness (the e2e greps this exact shape).
    eprintln!(
        "pgrust: cpu affinity: {workers} workers on cpus {witness} \
         (gang mirrored; standbys pool-set)"
    );
}

/// Bind the calling rtworker thread per the policy (spawn closure, after
/// the spawn-door bind, before the worker body). No-op when OFF.
pub(crate) fn apply_rtworker(ordinal: usize) {
    let Some(p) = POLICY.get() else { return };
    apply_and_observe(p.rtworker_set(ordinal), RTWORKER_BOARD.get(), ordinal);
}

/// Bind the calling rtgang thread per the policy (same map as rtworkers).
pub(crate) fn apply_rtgang(ordinal: usize) {
    let Some(p) = POLICY.get() else { return };
    apply_and_observe(p.rtgang_set(ordinal), RTGANG_BOARD.get(), ordinal);
}

/// Bind the calling wpool standby to the full pool-set mask (no board slot
/// — wpool standbys have no stable ordinal and never take a singleton).
pub(crate) fn apply_wpool_standby() {
    let Some(p) = POLICY.get() else { return };
    if let Err(e) = set_current_thread_affinity(p.pool_set()) {
        degrade_loud_once(&format!("sched_setaffinity failed (errno {e})"));
    }
}

fn apply_and_observe(set: &[usize], board: Option<&Vec<AtomicI32>>, ordinal: usize) {
    if let Err(e) = set_current_thread_affinity(set) {
        degrade_loud_once(&format!("sched_setaffinity failed (errno {e})"));
        return;
    }
    // Board post: the OBSERVED assignment (post-set read-back), singleton
    // slots only — the standby full-mask arm stays unobserved by design.
    if set.len() == 1 {
        if let (Some(b), [cpu]) = (board, current_thread_cpus().as_slice()) {
            if let Some(slot) = b.get(ordinal) {
                slot.store(*cpu as i32, Relaxed);
            }
        }
    }
}

/// Fail-open degrade, loud once per process (the parallel_engine degrade
/// shape; an atomic swap rather than a Once so no lazy-init site is added).
fn degrade_loud_once(reason: &str) {
    static WARNED: AtomicBool = AtomicBool::new(false);
    if !WARNED.swap(true, Relaxed) {
        eprintln!("pgrust: cpu affinity requested but {reason}; running unpinned");
    }
}

/// Observed rtworker board (ordinal -> observed core; None = unobserved,
/// i.e. OFF / standby full-mask / non-Linux). Diagnostics + tests.
pub fn rtworker_board() -> Option<Vec<Option<u32>>> {
    RTWORKER_BOARD.get().map(|b| b.iter().map(slot_to_cpu).collect())
}

/// Observed rtgang board (same shape as [`rtworker_board`]).
pub fn rtgang_board() -> Option<Vec<Option<u32>>> {
    RTGANG_BOARD.get().map(|b| b.iter().map(slot_to_cpu).collect())
}

fn slot_to_cpu(slot: &AtomicI32) -> Option<u32> {
    let v = slot.load(Relaxed);
    (v >= 0).then_some(v as u32)
}

/// Parse a core list: comma-separated cores and inclusive "a-b" ranges
/// ("1-14", "0,2,4", "0-3,8-11"); whitespace around items tolerated.
/// Output ascending + deduped. None on any malformed item, an empty list,
/// a reversed range, or a core >= the cpu_set_t width.
fn parse_cpu_list(s: &str) -> Option<Vec<usize>> {
    let mut out: Vec<usize> = Vec::new();
    for item in s.split(',') {
        let item = item.trim();
        if let Some((lo, hi)) = item.split_once('-') {
            let (lo, hi) = (lo.trim().parse::<usize>().ok()?, hi.trim().parse::<usize>().ok()?);
            if lo > hi {
                return None;
            }
            out.extend(lo..=hi);
        } else {
            out.push(item.parse::<usize>().ok()?);
        }
    }
    out.sort_unstable();
    out.dedup();
    if out.is_empty() || *out.last().unwrap() >= CPU_SET_WIDTH {
        return None;
    }
    Some(out)
}

/// Render an ascending core list in the kernel's Cpus_allowed_list shape
/// (comma-separated ranges: "0-3,8") — the witness line and the e2e's
/// /proc comparisons share this format.
fn format_cpu_list(cpus: &[usize]) -> String {
    let mut out = String::new();
    let mut i = 0;
    while i < cpus.len() {
        let start = cpus[i];
        let mut end = start;
        while i + 1 < cpus.len() && cpus[i + 1] == end + 1 {
            i += 1;
            end = cpus[i];
        }
        if !out.is_empty() {
            out.push(',');
        }
        if start == end {
            out.push_str(&start.to_string());
        } else {
            out.push_str(&format!("{start}-{end}"));
        }
        i += 1;
    }
    out
}

/// Bind the calling thread to `cpus` (Linux: sched_setaffinity on tid 0 =
/// self; the cpu_set_t is built by CPU_SET semantics). Err = raw errno.
#[cfg(target_os = "linux")]
pub fn set_current_thread_affinity(cpus: &[usize]) -> Result<(), i32> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for &c in cpus {
            if c >= CPU_SET_WIDTH {
                return Err(libc::EINVAL);
            }
            libc::CPU_SET(c, &mut set);
        }
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
        }
    }
}

/// Non-Linux: affinity is a no-op (the whole increment is Linux-only).
#[cfg(not(target_os = "linux"))]
pub fn set_current_thread_affinity(_cpus: &[usize]) -> Result<(), i32> {
    Ok(())
}

/// The calling thread's current cpu set, ascending (Linux:
/// sched_getaffinity; the boot-mask discovery AND the post-set read-back).
#[cfg(target_os = "linux")]
fn current_thread_cpus() -> Vec<usize> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Vec::new();
        }
        (0..CPU_SET_WIDTH).filter(|&c| libc::CPU_ISSET(c, &set)).collect()
    }
}

/// Non-Linux: no affinity syscalls; empty = undiscoverable, so
/// [`install_from_env`] degrades loud rather than publishing a fake map
/// (the policy math itself stays platform-independent for tests).
#[cfg(not(target_os = "linux"))]
fn current_thread_cpus() -> Vec<usize> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_range() {
        assert_eq!(parse_cpu_list("1-14"), Some((1..=14).collect()));
    }

    #[test]
    fn parse_commas() {
        assert_eq!(parse_cpu_list("0,2,4"), Some(vec![0, 2, 4]));
    }

    #[test]
    fn parse_mixed_and_whitespace() {
        assert_eq!(parse_cpu_list(" 0-3, 8 - 11 ,15"), Some(vec![0, 1, 2, 3, 8, 9, 10, 11, 15]));
    }

    #[test]
    fn parse_dedupes_and_sorts() {
        assert_eq!(parse_cpu_list("4,2,2,3,3-5"), Some(vec![2, 3, 4, 5]));
    }

    #[test]
    fn parse_garbage_is_none() {
        for bad in ["", " ", "a", "1,,2", "1-", "-3", "3-1", "1;2", "0-1023,1024", "1024"] {
            assert_eq!(parse_cpu_list(bad), None, "input {bad:?}");
        }
    }

    #[test]
    fn identity_map_and_standby_mask() {
        let p = AffinityPolicy { cpus: (0..8).collect(), workers: 8 };
        assert_eq!(p.rtworker_set(0), &[0]);
        assert_eq!(p.rtworker_set(3), &[3]);
        assert_eq!(p.rtworker_set(7), &[7]);
        // Standby ordinals (>= workers) get the whole set.
        assert_eq!(p.rtworker_set(8), &(0..8).collect::<Vec<_>>()[..]);
        assert_eq!(p.rtworker_set(9), &(0..8).collect::<Vec<_>>()[..]);
        // Gang mirrors the worker map core-for-core.
        assert_eq!(p.rtgang_set(3), p.rtworker_set(3));
    }

    #[test]
    fn ordinal_beyond_set_wraps_modulo() {
        // Set smaller than the pool: ordinals wrap (documented policy).
        let p = AffinityPolicy { cpus: vec![1, 2, 3], workers: 5 };
        assert_eq!(p.rtworker_set(3), &[1]);
        assert_eq!(p.rtworker_set(4), &[2]);
        assert_eq!(p.rtgang_set(4), &[2]);
        assert_eq!(p.rtworker_set(5), &[1, 2, 3]); // standby: full set
    }

    #[test]
    fn format_matches_cpus_allowed_list_shape() {
        assert_eq!(format_cpu_list(&[0, 1, 2, 3]), "0-3");
        assert_eq!(format_cpu_list(&[0, 2, 4]), "0,2,4");
        assert_eq!(format_cpu_list(&[0, 1, 3, 4, 7]), "0-1,3-4,7");
        assert_eq!(format_cpu_list(&[5]), "5");
    }

    #[test]
    fn format_parse_roundtrip() {
        for set in [vec![0, 1, 2, 3], vec![0, 2, 4], vec![1, 2, 3, 8, 9, 15]] {
            assert_eq!(parse_cpu_list(&format_cpu_list(&set)), Some(set));
        }
    }
}
