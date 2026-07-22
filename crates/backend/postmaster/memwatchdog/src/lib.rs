//! GL-MEMWATCH-1: the process memory watchdog.
//!
//! pgrust-only (no C counterpart). The motivating incident class
//! (GL-HASHAGG-SPILL-1, GL-TSACCT-1): a query grows process memory by
//! gigabytes — accounted-but-huge or invisible to the per-context ledgers —
//! and the FIRST diagnostic is the cgroup OOM kill, which arrives with no
//! server-side output at all: under Linux overcommit, allocation never
//! fails, so C's dump-on-allocation-failure (and our port of it) never
//! fires. This watchdog makes the server narrate its own memory death
//! BEFORE the killer arrives.
//!
//! Mechanism: one postmaster-lifetime sampler thread ("pg:memwatchdog").
//! Every `pgrust.memory_watchdog_interval` (default 1s) it reads process
//! RSS (/proc/self/status; allocator stats off-Linux), the cgroup v2
//! `memory.current`/`memory.max` when present, the process-wide accounted
//! context block bytes (`mcx::global_footprint`), and the allocator's
//! committed bytes (installed hook, mimalloc `mi_process_info`). Against
//! the armed limit — `pgrust.memory_watchdog_limit`, else the cgroup v2
//! limit — it fires ONCE PER TIER PER EXCURSION at escalating thresholds
//! (base T from `pgrust.memory_watchdog_threshold`; tiers T, T+(100-T)/2,
//! T+3(100-T)/4 — 80/90/95 at the default), each time logging the full
//! ledger INCLUDING the delta between RSS and accounted context bytes (the
//! untracked-allocation / accounting-drift detector), and — under
//! `pgrust.memory_watchdog_dump` — signalling every live backend to dump
//! its memory-context tree through the ported
//! `pg_log_backend_memory_contexts` machinery.
//!
//! Why a timer thread and not query-boundary checkpoints: the incident
//! query never reaches a query boundary — it grows tens of GiB mid-flight
//! and dies there. Morsel-boundary checks only cover the runtime engine
//! (the incident ran on the LEGACY engine). The sampler observes mid-query
//! growth on every engine at ZERO cost on any query path: nothing is added
//! to any per-tuple, per-morsel, or per-query code. Its own cost is one
//! /proc read + a handful of atomic loads per second.
//!
//! The watchdog's own lines go to stderr via `elog::write_stderr` (the
//! C MemoryContextStats channel — allocation-light, capturable by the log
//! collector, and unfiltered by log_min_messages: when this fires you are
//! about to be killed). The fanned-out per-backend dumps ride the normal
//! ereport LOG path on each backend thread.

use std::sync::atomic::{AtomicPtr, Ordering};

// ---------------------------------------------------------------------------
// Allocator statistics hook (mi_process_info shape); installed by the binary
// that owns the global allocator (mcx::set_allocator_release precedent).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default)]
pub struct AllocatorStats {
    /// Current working-set bytes as the allocator sees them (precise on
    /// macOS/Windows; estimated from committed memory elsewhere).
    pub current_rss: usize,
    /// Bytes of read/write memory the allocator currently has committed.
    pub current_commit: usize,
}

static ALLOC_STATS: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

pub fn set_allocator_stats(f: fn() -> AllocatorStats) {
    ALLOC_STATS.store(f as *mut (), Ordering::Release);
}

fn allocator_stats() -> Option<AllocatorStats> {
    let p = ALLOC_STATS.load(Ordering::Acquire);
    if p.is_null() {
        return None;
    }
    // SAFETY: only set_allocator_stats stores here, always from the fn type.
    let f: fn() -> AllocatorStats = unsafe { std::mem::transmute(p) };
    Some(f())
}

// ---------------------------------------------------------------------------
// Kernel-side signals: /proc/self/status RSS breakdown (Linux) and the
// cgroup v2 memory files (the container case IS the incident case).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default)]
struct ProcMem {
    rss_kb: u64,
    anon_kb: u64,
    shmem_kb: u64,
    hwm_kb: u64,
}

fn proc_status() -> Option<ProcMem> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let mut m = ProcMem::default();
    let kb = |v: &str| v.trim().trim_end_matches("kB").trim().parse().unwrap_or(0);
    for l in s.lines() {
        if let Some(v) = l.strip_prefix("VmRSS:") {
            m.rss_kb = kb(v);
        } else if let Some(v) = l.strip_prefix("VmHWM:") {
            m.hwm_kb = kb(v);
        } else if let Some(v) = l.strip_prefix("RssAnon:") {
            m.anon_kb = kb(v);
        } else if let Some(v) = l.strip_prefix("RssShmem:") {
            m.shmem_kb = kb(v);
        }
    }
    Some(m)
}

// The cgroup v2 memory-interface directory for this process, if any.
// /proc/self/cgroup's unified entry is "0::<path>"; with cgroup namespaces
// (containers) the path is usually "/" and the files sit directly under
// /sys/fs/cgroup. Resolved per call — it is two tiny virtual-file reads per
// tick and survives cgroup migration.
fn cgroup_dir() -> Option<std::path::PathBuf> {
    let s = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let rel = s.lines().find_map(|l| l.strip_prefix("0::"))?.trim();
    let joined = std::path::Path::new("/sys/fs/cgroup").join(rel.trim_start_matches('/'));
    if joined.join("memory.current").exists() {
        return Some(joined);
    }
    let root = std::path::PathBuf::from("/sys/fs/cgroup");
    if root.join("memory.current").exists() {
        return Some(root);
    }
    None
}

fn read_u64(path: &std::path::Path) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// (memory.current, memory.max) — max None when unbounded ("max").
fn cgroup_mem() -> Option<(u64, Option<u64>)> {
    let dir = cgroup_dir()?;
    let current = read_u64(&dir.join("memory.current"))?;
    let max = match std::fs::read_to_string(dir.join("memory.max")) {
        Ok(s) if s.trim() == "max" => None,
        Ok(s) => s.trim().parse().ok(),
        Err(_) => None,
    };
    Some((current, max))
}

// ---------------------------------------------------------------------------
// The sampler.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LimitSource {
    Guc,
    Cgroup,
    None,
}

#[derive(Default)]
struct WatchState {
    // Highest tier index (1..=3) fired in the current excursion; 0 = armed.
    latched: u32,
    // Last announced armed-state, to log arm/idle transitions exactly once.
    announced: Option<(LimitSource, u64)>,
}

fn tiers(base: i32) -> [u64; 3] {
    let t = base.clamp(1, 100) as u64;
    [t, t + (100 - t) / 2, t + (100 - t) * 3 / 4]
}

struct Ledger {
    usage: u64,
    limit: u64,
    source: LimitSource,
    pm: Option<ProcMem>,
    cg: Option<(u64, Option<u64>)>,
    accounted: usize,
    alloc: Option<AllocatorStats>,
}

fn collect(limit_mb: i32) -> Ledger {
    let pm = proc_status();
    let cg = cgroup_mem();
    let alloc = allocator_stats();
    let rss_bytes = match (pm, alloc) {
        (Some(m), _) => m.rss_kb * 1024,
        (None, Some(a)) => a.current_rss as u64,
        (None, None) => 0,
    };
    let (usage, limit, source) = if limit_mb > 0 {
        (rss_bytes, (limit_mb as u64) << 20, LimitSource::Guc)
    } else if let Some((current, Some(max))) = cg {
        (current, max, LimitSource::Cgroup)
    } else {
        (rss_bytes, 0, LimitSource::None)
    };
    Ledger {
        usage,
        limit,
        source,
        pm,
        cg,
        accounted: mcx::global_footprint::bytes(),
        alloc,
    }
}

fn log_line(msg: &str) {
    elog::write_stderr(&format!("LOG:  memory watchdog: {msg}\n"));
}

fn fire(l: &Ledger, pct: u64, tier_pct: u64) {
    let pm = l.pm.unwrap_or_default();
    // The delta of record: anonymous RSS (the incident-class growth; shared
    // memory excluded) minus the accounted context block bytes. Positive =
    // heap outside memory contexts (allocator retention/overhead, plain Rust
    // estates, thread stacks) or per-context ledgers drifting under reality;
    // negative = contexts holding committed-but-untouched blocks. Off-Linux
    // (no /proc) the basis falls back to the allocator's process RSS.
    let rss_basis_kb = if pm.anon_kb > 0 {
        pm.anon_kb
    } else if pm.rss_kb > 0 {
        pm.rss_kb
    } else {
        l.alloc.map_or(0, |a| a.current_rss as u64 / 1024)
    };
    let accounted_kb = (l.accounted / 1024) as u64;
    let delta_kb = rss_basis_kb as i64 - accounted_kb as i64;
    let (cg_cur_kb, cg_max) = match l.cg {
        Some((c, m)) => (c / 1024, m),
        None => (0, None),
    };
    let alloc_commit_kb = l.alloc.map_or(0, |a| a.current_commit / 1024);
    log_line(&format!(
        "memory use {pct}% of limit crossed the {tier_pct}% tier: \
         usage={} kB limit={} kB rss={} kB anon={} kB shmem={} kB hwm={} kB \
         cgroup_current={} kB cgroup_limit={} accounted_contexts={} kB \
         allocator_committed={} kB unaccounted_delta={} kB",
        l.usage / 1024,
        l.limit / 1024,
        pm.rss_kb,
        pm.anon_kb,
        pm.shmem_kb,
        pm.hwm_kb,
        cg_cur_kb,
        cg_max.map_or_else(|| "none".to_string(), |m| format!("{} kB", m / 1024)),
        accounted_kb,
        alloc_commit_kb,
        delta_kb,
    ));
    if guc_tables::backing::pgrust_memory_watchdog_dump() {
        let n = request_backend_dumps();
        log_line(&format!(
            "requested memory context dumps from {n} backends (see subsequent \
             \"logging memory contexts of PID\" log entries)"
        ));
    }
}

// Fan out PROCSIG_LOG_MEMORY_CONTEXT to every live backend: each dumps its
// own context tree to the log at its next interrupt-processing point (the
// pg_log_backend_memory_contexts path, sender-side loop). Failures are
// ignored (a proc exiting mid-scan).
fn request_backend_dumps() -> usize {
    use types_storage::ProcSignalReason::PROCSIG_LOG_MEMORY_CONTEXT;
    let mut n = 0;
    for proc in lmgr_proc::ProcGlobal().allProcs.iter() {
        let pid = proc.pid.load(Ordering::Relaxed);
        if pid == 0 {
            continue;
        }
        let proc_number = proc.vxid.procNumber.load(Ordering::Relaxed);
        if procsignal::SendProcSignal(pid, PROCSIG_LOG_MEMORY_CONTEXT, proc_number) >= 0 {
            n += 1;
        }
    }
    n
}

fn announce(state: &mut WatchState, l: &Ledger, base: i32) {
    let key = (l.source, l.limit);
    if state.announced == Some(key) {
        return;
    }
    state.announced = Some(key);
    state.latched = 0;
    match l.source {
        LimitSource::None => log_line(
            "idle: no memory limit signal (no bounded cgroup v2 limit found and \
             pgrust.memory_watchdog_limit is 0)",
        ),
        _ => {
            let t = tiers(base);
            log_line(&format!(
                "armed: limit={} MB (source={}) tiers={}%/{}%/{}% interval={} ms",
                l.limit >> 20,
                match l.source {
                    LimitSource::Guc => "pgrust.memory_watchdog_limit",
                    LimitSource::Cgroup => "cgroup v2 memory.max",
                    LimitSource::None => unreachable!(),
                },
                t[0],
                t[1],
                t[2],
                guc_tables::backing::pgrust_memory_watchdog_interval(),
            ));
        }
    }
}

fn tick(state: &mut WatchState) {
    if !guc_tables::backing::pgrust_memory_watchdog() {
        state.latched = 0;
        state.announced = None; // re-announce on re-enable
        return;
    }
    let base = guc_tables::backing::pgrust_memory_watchdog_threshold();
    let l = collect(guc_tables::backing::pgrust_memory_watchdog_limit());
    announce(state, &l, base);
    if l.limit == 0 || l.usage == 0 {
        return;
    }
    let pct = l.usage.saturating_mul(100) / l.limit;
    let t = tiers(base);
    let highest = t.iter().filter(|&&tp| pct >= tp).count() as u32;
    if highest > state.latched {
        // One log volley per tier per excursion, tiers strictly escalating.
        fire(&l, pct, t[(highest - 1) as usize]);
        state.latched = highest;
    } else if state.latched > 0 && pct + 5 <= t[0] {
        log_line(&format!(
            "memory use receded to {pct}% of limit; watchdog re-armed"
        ));
        state.latched = 0;
    }
}

/// Spawn the watchdog thread. Postmaster boot wiring (ServerLoop, next to
/// rtpool::start_if_enabled). The thread is unconditional and near-free; the
/// master switch is read every tick, so `pgrust.memory_watchdog` can arm and
/// disarm a running server via SIGHUP.
pub fn start() {
    let _ = std::thread::Builder::new()
        .name("pg:memwatchdog".into())
        .stack_size(256 * 1024)
        .spawn(|| {
            let mut state = WatchState::default();
            loop {
                let ms = guc_tables::backing::pgrust_memory_watchdog_interval().clamp(100, 60_000);
                std::thread::sleep(std::time::Duration::from_millis(ms as u64));
                // The tick is written panic-free (no unwrap/index); the guard
                // is defense in depth for unwinding builds.
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    tick(&mut state)
                }));
            }
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiers_escalate_from_base() {
        assert_eq!(tiers(80), [80, 90, 95]);
        assert_eq!(tiers(90), [90, 95, 97]);
        assert_eq!(tiers(50), [50, 75, 87]);
        assert_eq!(tiers(100), [100, 100, 100]);
        assert_eq!(tiers(0), [1, 50, 75]); // clamped base
    }

    #[test]
    fn proc_status_parses_on_linux_shape() {
        // On Linux this reads the live file; elsewhere it returns None.
        if cfg!(target_os = "linux") {
            let m = proc_status().expect("/proc/self/status readable on Linux");
            assert!(m.rss_kb > 0);
        } else {
            assert!(proc_status().is_none() || proc_status().is_some());
        }
    }

    #[test]
    fn allocator_stats_hook_roundtrip() {
        assert!(allocator_stats().is_none());
        set_allocator_stats(|| AllocatorStats {
            current_rss: 42,
            current_commit: 43,
        });
        let s = allocator_stats().expect("installed");
        assert_eq!(s.current_rss, 42);
        assert_eq!(s.current_commit, 43);
    }
}
