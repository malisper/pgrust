/*
 * pg_oracle_guard.h: release-effective holder check for the oracle
 * serialization lock (fuzz plumbing, NOT Postgres code).
 *
 * The vendored C oracle TUs carry process-global mutable state and are
 * single-threaded by construction; every Rust entry must hold
 * `oracle_serial()` (fuzz/core/src/lib.rs). That discipline was hand-swept
 * once and 25 test entry points escaped (docs/conformance/
 * scribbler-investigation-2026-08-02.md §4), one of them a proven
 * cross-thread free() generator (pg_tzf_reset). This header makes the
 * invariant self-enforcing at the C entry itself:
 *
 *   - `oracle_serial()` records the holder thread in a process-global
 *     (pg_oracle_guard_enter/exit, csrc/pg_oracle_guard.c);
 *   - each instrumented shim entry opens with
 *     PG_ORACLE_GUARD_CHECK(__func__);
 *   - on a holder mismatch the check calls the Rust hook
 *     `pgf_oracle_guard_violation`, which panics naming the entry AND the
 *     calling thread (== the offending test under cargo test).
 *
 * Deliberately NOT an assert: debug-only asserts have masked release
 * defects in this tree twice (debug-assert masking law) — the check
 * compiles into release builds whenever PG_ORACLE_GUARD_CHECKS is defined
 * (fuzz/core/build.rs defines it for every cc build of these TUs). Builds
 * outside build.rs (CBMC/goto-cc, ad-hoc family rigs) that do not define it
 * get a no-op and need no extra symbols.
 *
 * Static counterpart: scripts/lint-oracle-serial.py (call-graph lint over
 * fuzz/core/src). The lint proves test->extern paths are guarded at all;
 * this check additionally proves the guard is held by the EXECUTING thread
 * (a spawned closure under a spawner-held guard passes the lint but fails
 * here).
 */
#ifndef PG_ORACLE_GUARD_H
#define PG_ORACLE_GUARD_H

#ifdef PG_ORACLE_GUARD_CHECKS
extern void pg_oracle_guard_check(const char *entry);
#define PG_ORACLE_GUARD_CHECK(entry) pg_oracle_guard_check(entry)
#else
#define PG_ORACLE_GUARD_CHECK(entry) ((void) 0)
#endif

#endif /* PG_ORACLE_GUARD_H */
