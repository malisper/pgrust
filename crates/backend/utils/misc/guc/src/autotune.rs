//! Machine-scaled memory GUC defaults — pgrust public-release boot-time auto-tune.
//! (design + recommended-values table: docs/design/memory-defaults.md)
//!
//! Detects total RAM + core count once at postmaster startup and installs
//! machine-scaled defaults for `shared_buffers` / `work_mem` /
//! `effective_cache_size` / `maintenance_work_mem` and the parallel-worker
//! counts, at source `PGC_S_DYNAMIC_DEFAULT`. That source sits ABOVE the
//! hard-wired boot value but BELOW `postgresql.conf` / `ALTER SYSTEM` / `-c` /
//! environment, so any explicit operator setting still wins — exactly like C's
//! `InitializeShmemGUCs` (ipci) and the `wal_buffers = -1` auto-path. Because
//! it runs AFTER `SelectConfigFiles`, it also reads the operator's configured
//! `max_connections`, so `work_mem` scales down if that was raised.
//!
//! ## Gating (why it defaults OFF)
//! Applied only when `PGRUST_MEM_AUTOTUNE` is set (`1`/`on`/`true`/`yes`).
//! Unset (the default) keeps the stock boot values, so the byte-identical
//! `SHOW ALL` / `pg_settings` conformance suite is unaffected. The public-
//! release start script / container entrypoint sets `PGRUST_MEM_AUTOTUNE=1`.
//! This is the same env-gate idiom the tree already uses for `PGRUST_LANE_V2`,
//! `PGRUST_RUNTIME` and `PGRUST_CONDITION_CACHE`.
//!
//! ## Why the work_mem math differs from stock PostgreSQL / pgtune
//! pgrust is THREAD-per-backend: every backend is a thread inside the single
//! postmaster process, so all backends' `work_mem`, columnar decode arenas and
//! thread stacks share ONE virtual address space. Consequences that force a
//! more conservative budget than pgtune:
//!  * No per-backend OOM isolation. In stock PG the kernel OOM-killer reaps one
//!    runaway *process* and the postmaster survives + recovers; here an
//!    over-commit OOM-kills the whole process and every connection dies. So the
//!    modeled `work_mem` peak must sit well below RAM.
//!  * No total-memory guard exists in the tree (there is no `max_total_memory`
//!    or backend-memory accounting), so the safety must live entirely in the
//!    default value.
//!  * Extra per-query multipliers stock PG's `work_mem` math does not carry:
//!    columnar reader arenas (~2x * needed-columns * one 8192-row granule per
//!    columnar scan, RSS, GUC-unbounded) and per-thread stack reserves
//!    (address space, times `max_connections`).
//!
//! Net model: `work_mem` is budgeted to a conservative 20% of RAM, shared
//! across `max_connections * 3` concurrent memory nodes. Even the all-hash
//! worst case (`hash_mem_multiplier = 2` -> the budget doubles to 40% of RAM)
//! plus `shared_buffers` (25%) stays under two-thirds of RAM, leaving >= 1/3
//! for columnar arenas, thread stacks, the (shared, default-off) condition
//! cache and the OS page cache.

use types_error::{ErrorLocation, PgResult, LOG};
use types_guc::{PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT};

const MIB: u64 = 1024 * 1024;

// --- budget fractions / factors (see module doc for the rationale) ----------
/// Shared buffer pool: 25% of RAM (matches pgtune and the ClickBench PG entry).
const SHARED_BUFFERS_FRACTION: f64 = 0.25;
/// Planner's assumed total data-cache size: 75% of RAM (matches both).
const EFFECTIVE_CACHE_FRACTION: f64 = 0.75;
/// Fraction of RAM budgeted for the SUM of all transient per-query `work_mem`
/// peaks. Deliberately below pgtune's implicit ~25% because a single-process
/// OOM is whole-server-fatal and there is no total-memory guard.
const WORKMEM_BUDGET_FRACTION: f64 = 0.20;
/// Assumed concurrent `work_mem` allocations per active connection (multi-node
/// plans). pgtune uses the same factor of 3.
const WORKMEM_OPS_PER_CONN: f64 = 3.0;
/// `maintenance_work_mem` = RAM / 16 (pgtune), clamped below.
const MAINTENANCE_FRACTION_DIV: u64 = 16;

// --- floors/caps (MB). Floors == the stock boot values, so a tiny box never
//     regresses below stock; caps bound the single-process address space. -----
const SHARED_BUFFERS_FLOOR_MB: i64 = 128; // stock boot value (16384 blocks)
const WORK_MEM_FLOOR_MB: i64 = 4; // stock boot value (4096 kB)
const WORK_MEM_CAP_MB: i64 = 256;
const MAINTENANCE_FLOOR_MB: i64 = 64; // stock boot value (65536 kB)
/// pgrust caps `maintenance_work_mem` at 1 GiB (vs pgtune's 2 GiB): autovacuum
/// runs up to `autovacuum_max_workers` (3) workers that each may draw the full
/// `maintenance_work_mem`, all in the one postmaster address space.
const MAINTENANCE_CAP_MB: i64 = 1024;

/// The computed machine-scaled defaults (all sizes in MB, counts unitless).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryTuning {
    pub shared_buffers_mb: i64,
    pub effective_cache_size_mb: i64,
    pub work_mem_mb: i64,
    pub maintenance_work_mem_mb: i64,
    pub max_worker_processes: i64,
    pub max_parallel_workers: i64,
    pub max_parallel_workers_per_gather: i64,
    pub max_parallel_maintenance_workers: i64,
}

/// Pure, testable core: derive the recommended defaults from detected RAM
/// (bytes), core count, and the effective `max_connections`.
pub fn compute_memory_tuning(ram_bytes: u64, cores: usize, max_connections: i32) -> MemoryTuning {
    let ram = ram_bytes as f64;
    let conns = max_connections.max(1) as f64;
    let cores = cores.max(1) as i64;

    let shared_buffers_mb =
        (((ram * SHARED_BUFFERS_FRACTION) as u64 / MIB) as i64).max(SHARED_BUFFERS_FLOOR_MB);

    // Planner hint only (no allocation); floor at the shared pool so it is
    // never smaller than shared_buffers on a tiny box.
    let effective_cache_size_mb =
        (((ram * EFFECTIVE_CACHE_FRACTION) as u64 / MIB) as i64).max(shared_buffers_mb);

    let work_mem_bytes = (ram * WORKMEM_BUDGET_FRACTION) / (conns * WORKMEM_OPS_PER_CONN);
    let work_mem_mb =
        ((work_mem_bytes as u64 / MIB) as i64).clamp(WORK_MEM_FLOOR_MB, WORK_MEM_CAP_MB);

    let maintenance_work_mem_mb = ((ram_bytes / MAINTENANCE_FRACTION_DIV / MIB) as i64)
        .clamp(MAINTENANCE_FLOOR_MB, MAINTENANCE_CAP_MB);

    // Parallelism (memory-adjacent: each worker is a thread that gets its own
    // work_mem + columnar arenas). Scale to cores; cap per-gather so one query
    // cannot monopolise every core in a multi-user server (the ClickBench
    // single-client harness raises it explicitly).
    let max_worker_processes = (cores + 8).max(8);
    let max_parallel_workers = cores.max(2);
    let max_parallel_workers_per_gather = (cores / 2).clamp(2, 8);
    let max_parallel_maintenance_workers = (cores / 2).clamp(2, 4);

    MemoryTuning {
        shared_buffers_mb,
        effective_cache_size_mb,
        work_mem_mb,
        maintenance_work_mem_mb,
        max_worker_processes,
        max_parallel_workers,
        max_parallel_workers_per_gather,
        max_parallel_maintenance_workers,
    }
}

/// Whether the machine-scaled defaults are requested. Reads the registered
/// `pgrust.mem_autotune` GUC (env-to-guc train); the `PGRUST_MEM_AUTOTUNE`
/// environment variable still seeds this GUC's startup default at boot via
/// `initialize_guc_options_from_environment` (guc/src/store.rs), so the env
/// idiom keeps working while `postgresql.conf` / `ALTER SYSTEM` now also apply.
/// `apply_memory_autotune()` runs after `SelectConfigFiles`, so the value is
/// already resolved when this is read.
pub fn mem_autotune_enabled() -> bool {
    crate::GetConfigOption("pgrust.mem_autotune", true, false)
        .ok()
        .flatten()
        .as_deref()
        == Some("on")
}

/// Total physical RAM in bytes. Linux via `/proc/meminfo` (the primary target;
/// same source the ClickBench PG entry reads); macOS via `sysctl hw.memsize`
/// (dev boxes / `cargo test`). `None` if neither is available.
pub fn detect_total_ram_bytes() -> Option<u64> {
    // Linux / most Unixes expose MemTotal (kB) here; harmless no-op elsewhere.
    if let Ok(s) = std::fs::read_to_string("/proc/meminfo") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                if let Some(tok) = rest.split_whitespace().next() {
                    if let Ok(kb) = tok.parse::<u64>() {
                        return Some(kb.saturating_mul(1024));
                    }
                }
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = std::process::Command::new("/usr/sbin/sysctl")
            .args(["-n", "hw.memsize"])
            .output()
        {
            if let Ok(s) = String::from_utf8(out.stdout) {
                if let Ok(b) = s.trim().parse::<u64>() {
                    return Some(b);
                }
            }
        }
    }
    None
}

/// Detected logical core count (the same primitive the lane/runtime pools use).
pub fn detect_cores() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

fn current_max_connections() -> i32 {
    crate::GetConfigOption("max_connections", true, false)
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<i32>().ok())
        .unwrap_or(100)
}

fn set_dynamic_default(name: &str, value: &str) -> PgResult<()> {
    crate::SetConfigOption(name, Some(value), PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT)
}

fn log_line(msg: String) {
    // Best-effort: a logging hiccup must never abort postmaster boot.
    let _ = elog::ereport(LOG)
        .errmsg_internal(msg)
        .finish(ErrorLocation::new(
            "src/backend/utils/misc/guc/autotune.rs",
            0,
            "apply_memory_autotune",
        ));
}

/// Install the machine-scaled memory/parallel defaults at
/// `PGC_S_DYNAMIC_DEFAULT`. No-op unless `PGRUST_MEM_AUTOTUNE` is set. Call
/// once at postmaster startup, after `SelectConfigFiles` and before shmem
/// sizing locks in `NBuffers`.
pub fn apply_memory_autotune() -> PgResult<()> {
    if !mem_autotune_enabled() {
        return Ok(());
    }
    let Some(ram_bytes) = detect_total_ram_bytes() else {
        log_line(
            "pgrust memory auto-tune: PGRUST_MEM_AUTOTUNE is set but total system RAM could \
             not be detected; keeping stock memory defaults"
                .to_string(),
        );
        return Ok(());
    };
    let cores = detect_cores();
    let max_connections = current_max_connections();
    let t = compute_memory_tuning(ram_bytes, cores, max_connections);

    set_dynamic_default("shared_buffers", &format!("{}MB", t.shared_buffers_mb))?;
    set_dynamic_default(
        "effective_cache_size",
        &format!("{}MB", t.effective_cache_size_mb),
    )?;
    set_dynamic_default("work_mem", &format!("{}MB", t.work_mem_mb))?;
    set_dynamic_default(
        "maintenance_work_mem",
        &format!("{}MB", t.maintenance_work_mem_mb),
    )?;
    set_dynamic_default("max_worker_processes", &t.max_worker_processes.to_string())?;
    set_dynamic_default("max_parallel_workers", &t.max_parallel_workers.to_string())?;
    set_dynamic_default(
        "max_parallel_workers_per_gather",
        &t.max_parallel_workers_per_gather.to_string(),
    )?;
    set_dynamic_default(
        "max_parallel_maintenance_workers",
        &t.max_parallel_maintenance_workers.to_string(),
    )?;

    log_line(format!(
        "pgrust memory auto-tune: RAM={} MiB, cores={}, max_connections={} -> \
         shared_buffers={}MB, effective_cache_size={}MB, work_mem={}MB, \
         maintenance_work_mem={}MB, max_parallel_workers={}, max_parallel_workers_per_gather={}",
        ram_bytes / MIB,
        cores,
        max_connections,
        t.shared_buffers_mb,
        t.effective_cache_size_mb,
        t.work_mem_mb,
        t.maintenance_work_mem_mb,
        t.max_parallel_workers,
        t.max_parallel_workers_per_gather,
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * MIB;

    #[test]
    fn floors_never_regress_below_stock() {
        // Tiny box: everything pinned to the stock boot floors.
        let t = compute_memory_tuning(GIB, 1, 100);
        assert!(t.shared_buffers_mb >= SHARED_BUFFERS_FLOOR_MB);
        assert!(t.work_mem_mb >= WORK_MEM_FLOOR_MB);
        assert!(t.maintenance_work_mem_mb >= MAINTENANCE_FLOOR_MB);
        assert_eq!(t.work_mem_mb, WORK_MEM_FLOOR_MB); // raw well under 4MB
    }

    #[test]
    fn shared_buffers_and_ecs_are_quarter_and_three_quarters() {
        let t = compute_memory_tuning(64 * GIB, 16, 100);
        assert_eq!(t.shared_buffers_mb, 16 * 1024); // 25% of 64 GiB
        assert_eq!(t.effective_cache_size_mb, 48 * 1024); // 75% of 64 GiB
        assert!(t.effective_cache_size_mb > t.shared_buffers_mb);
    }

    #[test]
    fn maintenance_work_mem_capped_at_1gib() {
        // 64 GiB / 16 = 4 GiB, capped to 1 GiB in the single-process model.
        let t = compute_memory_tuning(64 * GIB, 16, 100);
        assert_eq!(t.maintenance_work_mem_mb, MAINTENANCE_CAP_MB);
        // 8 GiB / 16 = 512 MiB, under the cap.
        let s = compute_memory_tuning(8 * GIB, 8, 100);
        assert_eq!(s.maintenance_work_mem_mb, 512);
    }

    #[test]
    fn work_mem_scales_with_ram_and_is_bounded() {
        let w16 = compute_memory_tuning(16 * GIB, 16, 100).work_mem_mb;
        let w64 = compute_memory_tuning(64 * GIB, 16, 100).work_mem_mb;
        let w256 = compute_memory_tuning(256 * GIB, 16, 100).work_mem_mb;
        // 0.20 * RAM / (100 * 3): ~11 MB, ~43 MB, ~175 MB.
        assert_eq!(w16, 10); // 0.20*16GiB/300 floored
        assert_eq!(w64, 43);
        assert!(w256 > w64 && w256 <= WORK_MEM_CAP_MB);
        // A huge box hits the cap.
        assert_eq!(
            compute_memory_tuning(1024 * GIB, 64, 100).work_mem_mb,
            WORK_MEM_CAP_MB
        );
    }

    #[test]
    fn work_mem_shrinks_when_max_connections_rises() {
        // Thread-per-backend: work_mem is divided across the configured
        // connection count, so raising max_connections lowers work_mem.
        let few = compute_memory_tuning(64 * GIB, 16, 100).work_mem_mb;
        let many = compute_memory_tuning(64 * GIB, 16, 500).work_mem_mb;
        assert!(many < few, "want {many} < {few}");
    }

    #[test]
    fn all_hash_worst_case_plus_shared_buffers_stays_under_two_thirds() {
        // work_mem budget = 20% RAM; hash_mem_multiplier=2 doubles it to 40%;
        // plus shared_buffers 25% = 65% < 66.7%, leaving >=1/3 RAM headroom.
        let ram = 64u64 * GIB;
        let t = compute_memory_tuning(ram, 16, 100);
        let work_peak = (t.work_mem_mb as u64) * MIB * (100 * 3) * 2; // conns*ops*hash_mem_multiplier
        let sb = (t.shared_buffers_mb as u64) * MIB;
        assert!(
            work_peak + sb < ram * 2 / 3,
            "peak {} vs ram {}",
            work_peak + sb,
            ram
        );
    }

    #[test]
    fn parallelism_scales_to_cores() {
        let t = compute_memory_tuning(64 * GIB, 16, 100);
        assert_eq!(t.max_parallel_workers, 16);
        assert_eq!(t.max_parallel_workers_per_gather, 8); // 16/2, capped at 8
        assert_eq!(t.max_parallel_maintenance_workers, 4); // 16/2, capped at 4
        assert_eq!(t.max_worker_processes, 24); // 16 + 8
                                                // Small box: per-gather floored at 2, not below.
        let s = compute_memory_tuning(4 * GIB, 2, 100);
        assert_eq!(s.max_parallel_workers_per_gather, 2);
        assert_eq!(s.max_worker_processes, 10);
    }
}
