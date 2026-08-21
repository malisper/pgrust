//! GL-BULKWRITE-1 (#686): the bulk-write dirty-page governor.
//!
//! pgrust-only; no C counterpart. The defect it pays down: a COPY/CTAS whose
//! write volume exceeds the pod's memory envelope kills the server. Buffered
//! relation/part writes dirty page cache charged to the cgroup; dirty pages
//! cannot be reclaimed until writeback completes, and nothing in the write
//! path paces dirty production (the ported WritebackContext machinery boots
//! disabled: backend/bgwriter/checkpoint_flush_after = 0). On a bare box the
//! kernel's global writeback throttling paces the writer; inside a hard
//! `memory.max` the writer outruns reclaim and the OOM-killer takes the
//! postmaster — witnessed 3x on 2026-08-11 (72GB heap CTAS dead at both 27Gi
//! and 28,300Mi; notes: docs/tech-debt.md "Bulk writes OOM the server").
//!
//! The mechanism, hooked at the two funnels every bulk write passes through
//! (fd's `FileWriteV`/`FileZero` for relation files; pgrc2's
//! `RealVfs::pwrite_at` for columnar part files):
//!
//!  1. per-fd WINDOW: written byte ranges accumulate; every `kick` bytes the
//!     window is handed to the kernel with `sync_file_range(WRITE)` (via
//!     `vfs::flush_range`) — asynchronous writeback start, no blocking;
//!  2. global BUDGET: kicked-but-not-yet-waited bytes accumulate in a
//!     process-wide counter; when it crosses `budget` the tripping thread
//!     issues a blocking `fdatasync` on its own file — backpressure that
//!     paces the writer at device speed with bounded outstanding dirty.
//!
//! Sizing is cgroup-honest: `budget = clamp(memory.max/4, 64MiB, 8GiB)`
//! where `memory.max` is the MIN over bounded levels of our own cgroup
//! hierarchy (the memheadroom ancestor-walk lesson: on the CI cluster the leaf
//! reads "max" and the limit lives on the pod slice). No bounded cgroup =
//! disarmed: bare boxes already have global writeback throttling.
//!
//! What this changes and does not change:
//!  - WHAT gets written is untouched — only WHEN pages reach the device.
//!    Output banks stay byte-identical; part checksums/manifests unchanged.
//!  - Durability contracts are untouched: the existing checkpoint/publish
//!    fsyncs remain the barriers of record. The governor's fdatasync only
//!    ADDS earlier writeback. On a governor fdatasync failure the blocking
//!    arm disarms loudly and fail-open (per-fd errseq on kernels >= 4.13
//!    keeps the error visible to the durability fsyncs' own descriptors).
//!  - Small writes never reach `kick` bytes outstanding and pay one atomic
//!    load (disarmed) or one sharded map update (armed) per write.
//!
//! Knob: `PGRUST_BULK_WRITE_GOVERNOR` = unset/`auto` (arm iff a bounded
//! cgroup limit is visible) | `off`/`0` (disarm) | `<N>` (explicit budget in
//! MiB, arms even without a cgroup). Under `--cfg pgrust_sim` the governor
//! is always disarmed: the deterministic harness owns I/O physics and must
//! not see governor-injected ops.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

// THE single lock library (permit-scheduler.md #2): native world re-exports
// std types verbatim; `pgsync::lock` papers over per-world poisoning.
use pgsync::{Mutex, OnceLock};

const MIB: u64 = 1 << 20;
const SHARDS: usize = 8;

/// Wait-log cadence: every blocking wait is rare by construction
/// (one per `budget` bytes written), so each is logged.
fn log_line(msg: &str) {
    elog::write_stderr(&format!("LOG:  bulk-write governor: {msg}\n"));
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Source {
    Cgroup,
    /// No bounded cgroup visible: the VM-rig posture. A saner-than-nothing
    /// fraction of MemTotal (coordinator review flag, 2026-08-11): the box
    /// still has finite memory even without a memory.max ceiling.
    MemTotal,
    Env,
}

impl Source {
    fn as_str(self) -> &'static str {
        match self {
            Source::Cgroup => "cgroup-memory.max",
            Source::MemTotal => "meminfo-memtotal",
            Source::Env => "env",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Config {
    budget: u64,
    kick: u64,
    source: Source,
    /// The cgroup limit the budget was derived from (0 for Source::Env).
    limit: u64,
}

fn budget_from_limit(limit: u64) -> u64 {
    (limit / 4).clamp(64 * MIB, 8 << 30)
}

fn kick_from_budget(budget: u64) -> u64 {
    (budget / 16).clamp(MIB, 32 * MIB)
}

fn resolve() -> Option<Config> {
    // The deterministic sim harness owns I/O physics; never inject ops there.
    if cfg!(pgrust_sim) {
        return None;
    }
    let raw = std::env::var("PGRUST_BULK_WRITE_GOVERNOR").unwrap_or_default();
    let v = raw.trim().to_ascii_lowercase();
    match v.as_str() {
        "0" | "off" => None,
        "" | "auto" => {
            // Runtime-resolved, never baked: the same binary runs on 27Gi
            // pods and unbounded VM rigs. Bounded cgroup limit first; else
            // a conservative fraction of MemTotal (the box is finite even
            // without a ceiling); else disarm.
            if let Some(limit) = cgroup_memory_limit() {
                let budget = budget_from_limit(limit);
                Some(Config {
                    budget,
                    kick: kick_from_budget(budget),
                    source: Source::Cgroup,
                    limit,
                })
            } else {
                let total = meminfo_total()?;
                let budget = (total / 8).clamp(64 * MIB, 8 << 30);
                Some(Config {
                    budget,
                    kick: kick_from_budget(budget),
                    source: Source::MemTotal,
                    limit: total,
                })
            }
        }
        _ => {
            let mb: u64 = v.parse().ok().filter(|&n| n > 0)?;
            let budget = (mb * MIB).max(16 * MIB);
            Some(Config {
                budget,
                kick: kick_from_budget(budget),
                source: Source::Env,
                limit: 0,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Cgroup limit — the memheadroom ancestor walk, limit-only
// ---------------------------------------------------------------------------

/// Tightest bounded memory limit over our own cgroup hierarchy, leaf to
/// mount root (v2 `memory.max`; v1 `memory.limit_in_bytes` when the memory
/// controller lives on v1). `None` = no bounded level visible.
#[cfg(target_os = "linux")]
fn cgroup_memory_limit() -> Option<u64> {
    let self_cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    limit_with(std::path::Path::new("/sys/fs/cgroup"), &self_cgroup)
}

#[cfg(not(target_os = "linux"))]
fn cgroup_memory_limit() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn meminfo_total() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    meminfo_total_of(&s)
}

/// Non-Linux dev boxes: auto stays off (no /proc; the explicit env budget
/// still arms for local testing).
#[cfg(not(target_os = "linux"))]
fn meminfo_total() -> Option<u64> {
    None
}

#[cfg(any(target_os = "linux", test))]
fn meminfo_total_of(meminfo: &str) -> Option<u64> {
    let kb: u64 = meminfo.lines().find_map(|l| {
        l.strip_prefix("MemTotal:")?.trim().strip_suffix("kB")?.trim().parse().ok()
    })?;
    Some(kb * 1024)
}

#[cfg(any(target_os = "linux", test))]
fn read_num(p: &std::path::Path) -> Option<u64> {
    std::fs::read_to_string(p).ok()?.trim().parse().ok()
}

/// Pure core (root injected) so fixture-tree tests cover the postures
/// without a container. v1 spells unlimited as a huge number; v2 as "max"
/// (which fails the numeric parse).
#[cfg(any(target_os = "linux", test))]
fn limit_with(root: &std::path::Path, self_cgroup: &str) -> Option<u64> {
    let v1_rel = self_cgroup.lines().find_map(|l| {
        let mut it = l.splitn(3, ':');
        let _id = it.next()?;
        let ctls = it.next()?;
        let path = it.next()?;
        if ctls.split(',').any(|c| c == "memory") {
            Some(path.trim().trim_start_matches('/').to_string())
        } else {
            None
        }
    });
    let v2_rel = self_cgroup
        .lines()
        .find_map(|l| l.strip_prefix("0::"))
        .map(|p| p.trim().trim_start_matches('/').to_string());

    let (base, rel, v2) = if let Some(rel) = v1_rel {
        (root.join("memory"), rel, false)
    } else if let Some(rel) = v2_rel {
        (root.to_path_buf(), rel, true)
    } else {
        return None;
    };

    let leaf = if rel.is_empty() { base.clone() } else { base.join(&rel) };
    let mut best: Option<u64> = None;
    let mut p = leaf;
    loop {
        let lim = if v2 {
            read_num(&p.join("memory.max"))
        } else {
            read_num(&p.join("memory.limit_in_bytes")).filter(|&l| l < (1u64 << 60))
        };
        if let Some(lim) = lim {
            best = Some(best.map_or(lim, |b: u64| b.min(lim)));
        }
        if p == base {
            break;
        }
        match p.parent() {
            Some(par) if par.starts_with(&base) => p = par.to_path_buf(),
            _ => break,
        }
    }
    best
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Default, Clone, Copy)]
struct Win {
    pending: u64,
    lo: u64,
    hi: u64,
}

struct Gov {
    cfg: Config,
    /// Kicked-but-not-yet-waited bytes, process-wide.
    inflight: AtomicU64,
    /// Per-fd write windows, sharded by fd to keep the armed small-write
    /// cost a short uncontended lock.
    shards: [Mutex<HashMap<i32, Win>>; SHARDS],
    /// A failed blocking wait disarms the wait arm loudly, fail-open.
    wait_disabled: AtomicBool,
    /// Total bytes accounted (governed) since boot.
    noted: AtomicU64,
    kicks: AtomicU64,
    waits: AtomicU64,
    wait_ms_total: AtomicU64,
    /// Census cadence state (last emit instant + noted watermark).
    census: Mutex<CensusState>,
}

#[derive(Default)]
struct CensusState {
    last_emit: Option<Instant>,
    last_noted: u64,
}

static GOV: OnceLock<Option<Gov>> = OnceLock::new();

fn gov() -> Option<&'static Gov> {
    GOV.get_or_init(|| {
        resolve().map(|cfg| Gov {
            cfg,
            inflight: AtomicU64::new(0),
            // Explicit construction: not every pgsync world's Mutex is Default.
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            wait_disabled: AtomicBool::new(false),
            noted: AtomicU64::new(0),
            kicks: AtomicU64::new(0),
            waits: AtomicU64::new(0),
            wait_ms_total: AtomicU64::new(0),
            census: Mutex::new(CensusState::default()),
        })
    })
    .as_ref()
}

/// Postmaster boot wiring (ServerLoop, next to memwatchdog::start): resolve
/// the config once and put the arming decision in the log — the census
/// witness that the governor is live BEFORE any bulk write runs.
pub fn start() {
    match gov() {
        None => log_line(
            "idle: no bounded cgroup memory limit found and \
             PGRUST_BULK_WRITE_GOVERNOR is not an explicit budget",
        ),
        Some(g) => {
            log_line(&format!(
                "armed: budget_mb={} kick_mb={} source={} source_limit_mb={}",
                g.cfg.budget >> 20,
                g.cfg.kick >> 20,
                g.cfg.source.as_str(),
                g.cfg.limit >> 20,
            ));
        }
    }
}

/// Periodic machine-readable census (no-silent-no-op law for governors:
/// an armed governor doing work reports steadily). Piggybacked on the
/// memwatchdog sampler tick; rate-limited here to one line per minute and
/// only while governed bytes advance. Cheap when idle: two atomic loads.
pub fn tick_census() {
    let Some(g) = gov() else { return };
    let noted = g.noted.load(Ordering::Relaxed);
    if noted == 0 {
        return;
    }
    let mut c = pgsync::lock(&g.census);
    if noted == c.last_noted {
        return;
    }
    if let Some(t) = c.last_emit {
        if t.elapsed().as_secs() < 60 {
            return;
        }
    }
    c.last_emit = Some(Instant::now());
    c.last_noted = noted;
    drop(c);
    log_line(&census_string(g, noted));
}

fn census_string(g: &Gov, noted: u64) -> String {
    format!(
        "census governed_mb={} kicks={} waits={} wait_stall_ms={} \
         inflight_mb={} budget_mb={} kick_mb={} source={}",
        noted >> 20,
        g.kicks.load(Ordering::Relaxed),
        g.waits.load(Ordering::Relaxed),
        g.wait_ms_total.load(Ordering::Relaxed),
        g.inflight.load(Ordering::Relaxed) >> 20,
        g.cfg.budget >> 20,
        g.cfg.kick >> 20,
        g.cfg.source.as_str(),
    )
}

// ---------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------

/// Record `len` bytes just written to `fd` at `off`. Called AFTER a
/// successful buffered write, with the descriptor still open. Never errors:
/// every kernel interaction here is a hint or pacing, not correctness.
pub fn note_write(fd: i32, off: u64, len: u64) {
    let Some(g) = gov() else { return };
    if len == 0 {
        return;
    }
    g.noted.fetch_add(len, Ordering::Relaxed);
    let shard = &g.shards[(fd as usize) % SHARDS];
    let (lo, hi, pend) = {
        let mut m = pgsync::lock(shard);
        let w = m.entry(fd).or_default();
        if w.pending == 0 {
            w.lo = off;
            w.hi = off + len;
        } else {
            w.lo = w.lo.min(off);
            w.hi = w.hi.max(off + len);
        }
        w.pending += len;
        if w.pending < g.cfg.kick {
            return;
        }
        let out = (w.lo, w.hi, w.pending);
        *w = Win::default();
        out
    };
    kick_and_maybe_wait(g, fd, lo, hi, pend, true);
}

/// The descriptor is about to be closed: kick any pending window so its
/// bytes keep draining, and forget the fd (numbers are reused). Never
/// blocks — close paths run in cleanup contexts.
pub fn note_close(fd: i32) {
    let Some(g) = gov() else { return };
    let win = {
        let mut m = pgsync::lock(&g.shards[(fd as usize) % SHARDS]);
        m.remove(&fd)
    };
    if let Some(w) = win {
        if w.pending > 0 {
            kick_and_maybe_wait(g, fd, w.lo, w.hi, w.pending, false);
        }
    }
}

fn kick_and_maybe_wait(g: &'static Gov, fd: i32, lo: u64, hi: u64, pend: u64, may_wait: bool) {
    // Asynchronous writeback start; errors are ignored (pure hint, exactly
    // the pg_flush_data posture).
    let _ = vfs::flush_range(fd, lo as libc::off_t, (hi - lo) as libc::off_t);
    g.kicks.fetch_add(1, Ordering::Relaxed);
    let infl = g.inflight.fetch_add(pend, Ordering::Relaxed) + pend;
    if !may_wait || infl < g.cfg.budget || g.wait_disabled.load(Ordering::Relaxed) {
        return;
    }
    // BUDGET reached: block on writeback of our own file. Bulk writers are
    // overwhelmingly sequential into the file they are tripping on, so this
    // waits out the bulk of the outstanding window at device speed.
    let t0 = Instant::now();
    let rc = vfs::fdatasync(fd);
    let ms = t0.elapsed().as_millis() as u64;
    if rc != 0 {
        // Fail-open: log once, disarm the blocking arm, keep the async
        // kicks. The durability fsyncs of record (checkpoint/publish) hold
        // their own descriptors and their own errseq cursors.
        if !g.wait_disabled.swap(true, Ordering::Relaxed) {
            log_line(&format!(
                "blocking writeback failed (errno={}); wait arm disarmed, \
                 async kicks continue",
                vfs::get_errno()
            ));
        }
        return;
    }
    // Forgiveness reset, not subtraction: older files' kicked bytes have
    // been draining for a full budget cycle behind our own fdatasync; a
    // deliberate under-count bounded by concurrent-writer windows. The
    // ceiling stays ~budget + (writers x kick) << memory.max.
    g.inflight.store(0, Ordering::Relaxed);
    g.waits.fetch_add(1, Ordering::Relaxed);
    g.wait_ms_total.fetch_add(ms, Ordering::Relaxed);
    log_line(&format!(
        "backpressure: dirty budget reached, waited fd={fd} stall_ms={ms}; {}",
        census_string(g, g.noted.load(Ordering::Relaxed)),
    ));
}

// ---------------------------------------------------------------------------
// Tests — pure logic over fixture trees (the memheadroom pattern)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    const GIB: u64 = 1 << 30;

    struct Fixture {
        root: PathBuf,
    }

    impl Fixture {
        fn new(name: &str) -> Fixture {
            let root = std::env::temp_dir().join(format!(
                "pgrust-writegov-{}-{}",
                std::process::id(),
                name
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).unwrap();
            Fixture { root }
        }

        fn write(&self, rel: &str, name: &str, content: &str) {
            let d = self.root.join(rel);
            fs::create_dir_all(&d).unwrap();
            fs::write(d.join(name), content).unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    /// The CI cluster posture: leaf reads "max", the limit lives on the pod
    /// slice — the ancestor walk finds it.
    #[test]
    fn v2_limit_on_ancestor() {
        let f = Fixture::new("v2-ancestor");
        let leaf = "kubepods.slice/kubepods-pod1.slice/cri-abc.scope";
        f.write(leaf, "memory.max", "max\n");
        f.write(
            "kubepods.slice/kubepods-pod1.slice",
            "memory.max",
            &(27 * GIB).to_string(),
        );
        assert_eq!(
            limit_with(&f.root, &format!("0::/{leaf}\n")),
            Some(27 * GIB)
        );
    }

    /// Nested bounds: the tightest wins.
    #[test]
    fn v2_nested_min() {
        let f = Fixture::new("v2-nested");
        let leaf = "kubepods.slice/pod.slice/c.scope";
        f.write(leaf, "memory.max", &(100 * GIB).to_string());
        f.write("kubepods.slice/pod.slice", "memory.max", &(27 * GIB).to_string());
        assert_eq!(
            limit_with(&f.root, &format!("0::/{leaf}\n")),
            Some(27 * GIB)
        );
    }

    /// Everything "max": unbounded, disarm.
    #[test]
    fn v2_unbounded_is_none() {
        let f = Fixture::new("v2-unbounded");
        let leaf = "system.slice/pg.service";
        f.write(leaf, "memory.max", "max\n");
        f.write("system.slice", "memory.max", "max\n");
        assert_eq!(limit_with(&f.root, &format!("0::/{leaf}\n")), None);
    }

    /// v1 memory controller: limit_in_bytes with unlimited-as-huge.
    #[test]
    fn v1_limit() {
        let f = Fixture::new("v1");
        f.write("memory/docker/abc", "memory.limit_in_bytes", "9223372036854771712\n");
        f.write("memory/docker", "memory.limit_in_bytes", &(27 * GIB).to_string());
        let cg = "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n0::/docker/abc\n";
        assert_eq!(limit_with(&f.root, cg), Some(27 * GIB));
    }

    /// No cgroup membership: bare box, disarm.
    #[test]
    fn bare_is_none() {
        let f = Fixture::new("bare");
        assert_eq!(limit_with(&f.root, ""), None);
    }

    /// The VM-rig fallback parses MemTotal.
    #[test]
    fn meminfo_total_parses() {
        let mi = "MemTotal:       32000000 kB\nMemFree:  1 kB\n";
        assert_eq!(meminfo_total_of(mi), Some(32_000_000 * 1024));
        assert_eq!(meminfo_total_of("Bogus: 1\n"), None);
    }

    /// The witnessed failure config: 28,300Mi memory.max sizes an ~7 GiB
    /// budget, well inside the envelope; a 16Gi CI pod gets 4 GiB.
    #[test]
    fn budget_sizing() {
        let witnessed = 28_979_200 * 1024u64; // 28300Mi in bytes
        let b = budget_from_limit(witnessed);
        assert_eq!(b, witnessed / 4);
        assert!(b > 6 * GIB && b < 7 * GIB); // ~6.9 GiB at the witnessed cap
        assert_eq!(budget_from_limit(16 * GIB), 4 * GIB);
        // Floors and ceilings.
        assert_eq!(budget_from_limit(64 * MIB), 64 * MIB);
        assert_eq!(budget_from_limit(1 << 45), 8 << 30);
        // Kick scales with budget inside [1MiB, 32MiB].
        assert_eq!(kick_from_budget(4 * GIB), 32 * MIB);
        assert_eq!(kick_from_budget(64 * MIB), 4 * MIB);
        assert_eq!(kick_from_budget(16 * MIB), MIB);
    }
}
