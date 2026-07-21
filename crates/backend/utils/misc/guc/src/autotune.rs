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
//! ## initdb pinning (why shared_buffers can stay 128MB with autotune on)
//! initdb writes an EXPLICIT `shared_buffers = 128MB` (and `max_connections =
//! 100`) line into every postgresql.conf it generates, so on a stock cluster
//! that value carries source `PGC_S_FILE` and — by the "explicit settings
//! win" contract above — the `PGC_S_DYNAMIC_DEFAULT` auto-tune cannot raise
//! it, while the GUCs initdb leaves commented (work_mem etc.) scale normally.
//! This is deliberate: the config source ladder cannot distinguish initdb's
//! boilerplate from an operator's intent, and silently overriding a conf-file
//! line would break the ladder for everyone else. `apply_memory_autotune`
//! detects any pinned value by read-back and reports it at LOG; the public-
//! release entrypoint (the same script that sets `PGRUST_MEM_AUTOTUNE=1`)
//! must remove/knock out initdb's shared_buffers line — or initdb with
//! `-c shared_buffers=<25% RAM>` — for the documented 25% default to apply
//! (docs/design/memory-defaults.md §"initdb pinning").
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
use types_guc::{GUC_UNIT_BLOCKS, GUC_UNIT_KB, GUC_UNIT_MEMORY, PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT};

const MIB: u64 = 1024 * 1024;

// --- budget fractions / factors (see module doc for the rationale) ----------
/// Shared buffer pool: 25% of RAM (matches pgtune and the public leaderboard's PG config).
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

/// The registered [min, max] of an int GUC from the static settings tables,
/// converted to the unit `MemoryTuning` carries for it: whole MB for memory
/// GUCs (GUC_UNIT_BLOCKS/GUC_UNIT_KB), the raw count otherwise. `value * K`
/// (K = native units per MB) must land inside the registered i32 range, so
/// the MB bounds are ceil(min/K) / floor(max/K). Panics on a missing/non-int
/// name: the tables are static and every caller is unit-tested, so a rename
/// must break loudly rather than silently drop the clamp.
fn registered_bounds(name: &str) -> (i64, i64) {
    let (min, max, flags) = guc_tables::all_settings()
        .find_map(|s| match s {
            guc_tables::GucSetting::Int(i) if i.name == name => {
                Some((i.min as i64, i.max as i64, i.flags))
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("autotune: {name} is not a registered int GUC"));
    let per_mb: i64 = match flags & GUC_UNIT_MEMORY {
        f if f == GUC_UNIT_BLOCKS => MIB as i64 / guc_tables::consts::BLCKSZ as i64,
        f if f == GUC_UNIT_KB => 1024,
        0 => return (min, max), // plain count: bounds already in the field's unit
        f => panic!("autotune: {name} has unhandled memory unit flags {f:#x}"),
    };
    // Manual ceil-div (registered mins are never negative here; asserted so
    // the shortcut can't silently go wrong on a future negative-min GUC).
    assert!(min >= 0 && per_mb > 0);
    ((min + per_mb - 1) / per_mb, max / per_mb)
}

/// Clamp a computed default into its GUC's registered range so the boot-time
/// `SetConfigOption` can never fail range validation (a huge-RAM box would
/// otherwise turn `pgrust.mem_autotune=on` into a boot failure).
fn clamp_to_registered(name: &str, value: i64) -> i64 {
    let (min, max) = registered_bounds(name);
    value.clamp(min, max)
}

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

    // Every computed value is finally clamped into its GUC's REGISTERED
    // [min, max] (clamp_to_registered): the policy floors/caps below express
    // the tuning model, but only the registered range keeps the boot-time
    // SetConfigOption from erroring out — e.g. 25% of a >32 TiB box exceeds
    // shared_buffers' max of i32::MAX/2 blocks, and >1024 cores would push
    // the parallel counts past MAX_PARALLEL_WORKER_LIMIT, either of which
    // would otherwise abort postmaster boot under pgrust.mem_autotune=on.
    let shared_buffers_mb = clamp_to_registered(
        "shared_buffers",
        (((ram * SHARED_BUFFERS_FRACTION) as u64 / MIB) as i64).max(SHARED_BUFFERS_FLOOR_MB),
    );

    // Planner hint only (no allocation); floor at the shared pool so it is
    // never smaller than shared_buffers on a tiny box. (Range note: ecs' max
    // — i32::MAX blocks — is above shared_buffers' max, so the ordering
    // survives the clamp.)
    let effective_cache_size_mb = clamp_to_registered(
        "effective_cache_size",
        (((ram * EFFECTIVE_CACHE_FRACTION) as u64 / MIB) as i64).max(shared_buffers_mb),
    );

    let work_mem_bytes = (ram * WORKMEM_BUDGET_FRACTION) / (conns * WORKMEM_OPS_PER_CONN);
    let work_mem_mb = clamp_to_registered(
        "work_mem",
        ((work_mem_bytes as u64 / MIB) as i64).clamp(WORK_MEM_FLOOR_MB, WORK_MEM_CAP_MB),
    );

    let maintenance_work_mem_mb = clamp_to_registered(
        "maintenance_work_mem",
        ((ram_bytes / MAINTENANCE_FRACTION_DIV / MIB) as i64)
            .clamp(MAINTENANCE_FLOOR_MB, MAINTENANCE_CAP_MB),
    );

    // Parallelism (memory-adjacent: each worker is a thread that gets its own
    // work_mem + columnar arenas). Scale to cores; cap per-gather so one query
    // cannot monopolise every core in a multi-user server (the benchmark
    // single-client harness raises it explicitly).
    let max_worker_processes = clamp_to_registered("max_worker_processes", (cores + 8).max(8));
    let max_parallel_workers = clamp_to_registered("max_parallel_workers", cores.max(2));
    let max_parallel_workers_per_gather =
        clamp_to_registered("max_parallel_workers_per_gather", (cores / 2).clamp(2, 8));
    let max_parallel_maintenance_workers =
        clamp_to_registered("max_parallel_maintenance_workers", (cores / 2).clamp(2, 4));

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
/// same source the public leaderboard's PG config reads); macOS via `sysctl hw.memsize`
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

    // (name, value to set, expected value AFTER a successful set in the GUC's
    // NATIVE units — blocks for GUC_UNIT_BLOCKS, kB for GUC_UNIT_KB, the raw
    // count otherwise; what GetConfigOption's unit-less show returns.)
    let blocks_per_mb = MIB as i64 / guc_tables::consts::BLCKSZ as i64;
    let entries: [(&str, String, i64); 8] = [
        (
            "shared_buffers",
            format!("{}MB", t.shared_buffers_mb),
            t.shared_buffers_mb * blocks_per_mb,
        ),
        (
            "effective_cache_size",
            format!("{}MB", t.effective_cache_size_mb),
            t.effective_cache_size_mb * blocks_per_mb,
        ),
        ("work_mem", format!("{}MB", t.work_mem_mb), t.work_mem_mb * 1024),
        (
            "maintenance_work_mem",
            format!("{}MB", t.maintenance_work_mem_mb),
            t.maintenance_work_mem_mb * 1024,
        ),
        ("max_worker_processes", t.max_worker_processes.to_string(), t.max_worker_processes),
        ("max_parallel_workers", t.max_parallel_workers.to_string(), t.max_parallel_workers),
        (
            "max_parallel_workers_per_gather",
            t.max_parallel_workers_per_gather.to_string(),
            t.max_parallel_workers_per_gather,
        ),
        (
            "max_parallel_maintenance_workers",
            t.max_parallel_maintenance_workers.to_string(),
            t.max_parallel_maintenance_workers,
        ),
    ];
    // PGC_S_DYNAMIC_DEFAULT loses (by design) to any higher-priority source —
    // postgresql.conf / ALTER SYSTEM / -c / environment. That is the "explicit
    // operator setting wins" contract, but it has one systematic surprise:
    // initdb WRITES an explicit `shared_buffers = 128MB` (and
    // `max_connections = 100`) line into every generated postgresql.conf, so
    // on a stock cluster the shared_buffers auto-tune is pinned at 128MB while
    // everything else scales. Detect pinned values by read-back and say so at
    // LOG, naming the fix (docs/design/memory-defaults.md "initdb pinning").
    let mut pinned: Vec<String> = Vec::new();
    for (name, value, expect_native) in &entries {
        set_dynamic_default(name, value)?;
        let now = crate::GetConfigOption(name, true, false)
            .ok()
            .flatten()
            .and_then(|s| s.trim().parse::<i64>().ok());
        if now != Some(*expect_native) {
            pinned.push(format!(
                "{name} (wanted {value}, kept {})",
                now.map_or_else(|| "?".to_string(), |v| v.to_string())
            ));
        }
    }
    if !pinned.is_empty() {
        log_line(format!(
            "pgrust memory auto-tune: {} pinned by an explicit setting (postgresql.conf / \
             ALTER SYSTEM / command line); note initdb writes an explicit shared_buffers line \
             into postgresql.conf — remove or adjust it for the auto-tuned value to apply \
             (docs/design/memory-defaults.md)",
            pinned.join(", "),
        ));
    }

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
    fn absurd_ram_and_cores_stay_within_registered_guc_ranges() {
        // The boot-failure mode this pins: a computed default outside its
        // GUC's registered range makes apply_memory_autotune's
        // SetConfigOption error and aborts postmaster boot. Sweep absurd
        // machines and assert every value (a) sits inside the registered
        // range and (b) fits i32 in the GUC's NATIVE units (blocks/kB), the
        // representation the parse path validates.
        // (name, getter, native units per MB: 128 blocks/MB or 1024 kB/MB)
        let mem_gucs: &[(&str, fn(&MemoryTuning) -> i64, i64)] = &[
            ("shared_buffers", |t| t.shared_buffers_mb, 128),
            ("effective_cache_size", |t| t.effective_cache_size_mb, 128),
            ("work_mem", |t| t.work_mem_mb, 1024),
            ("maintenance_work_mem", |t| t.maintenance_work_mem_mb, 1024),
        ];
        let count_gucs: &[(&str, fn(&MemoryTuning) -> i64)] = &[
            ("max_worker_processes", |t| t.max_worker_processes),
            ("max_parallel_workers", |t| t.max_parallel_workers),
            ("max_parallel_workers_per_gather", |t| t.max_parallel_workers_per_gather),
            ("max_parallel_maintenance_workers", |t| t.max_parallel_maintenance_workers),
        ];
        for ram in [4 * 1024 * GIB, 64 * 1024 * GIB, 1024 * 1024 * GIB] {
            for cores in [16usize, 512, 4096] {
                let t = compute_memory_tuning(ram, cores, 100);
                for (name, get, per_mb) in mem_gucs {
                    let (min, max) = registered_bounds(name);
                    let v = get(&t);
                    assert!(
                        (min..=max).contains(&v),
                        "{name}={v}MB outside registered [{min}, {max}]MB at ram={ram} cores={cores}"
                    );
                    // Native-unit i32 fit (what SetConfigOption validates).
                    assert!(
                        v.checked_mul(*per_mb).is_some_and(|n| n <= i32::MAX as i64),
                        "{name}={v}MB overflows i32 in native units (x{per_mb})"
                    );
                }
                for (name, get) in count_gucs {
                    let (min, max) = registered_bounds(name);
                    let v = get(&t);
                    assert!(
                        (min..=max).contains(&v),
                        "{name}={v} outside registered [{min}, {max}] at ram={ram} cores={cores}"
                    );
                }
                assert!(t.effective_cache_size_mb >= t.shared_buffers_mb);
            }
        }
        // Independent literals (not via registered_bounds) pin the actual
        // registered maxima: shared_buffers i32::MAX/2 blocks -> 8388607 MB;
        // effective_cache_size i32::MAX blocks -> 16777215 MB;
        // max_parallel_workers MAX_PARALLEL_WORKER_LIMIT = 1024.
        let huge = compute_memory_tuning(1024 * 1024 * GIB, 4096, 100); // 1 PiB
        assert_eq!(huge.shared_buffers_mb, 8_388_607);
        assert_eq!(huge.effective_cache_size_mb, 16_777_215);
        assert_eq!(huge.max_parallel_workers, 1024);
        assert_eq!(huge.max_parallel_workers_per_gather, 8); // policy cap holds
        // The review's 4 TiB example: in range, and NOT needlessly clamped
        // (25% = 1 TiB and 75% = 3 TiB both fit their registered maxima).
        let t4 = compute_memory_tuning(4 * 1024 * GIB, 64, 100);
        assert_eq!(t4.shared_buffers_mb, 1024 * 1024);
        assert_eq!(t4.effective_cache_size_mb, 3 * 1024 * 1024);
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
