# Audit: backend-storage-lmgr-s-lock

- Unit: `backend-storage-lmgr-s-lock` (`src/backend/storage/lmgr/s_lock.c` plus the
  platform-independent layer of `src/include/storage/s_lock.h`)
- Branch: `port/backend-storage-lmgr-s-lock` (port commit `3b579f1`)
- Crates: `crates/backend-storage-lmgr-s-lock`,
  `crates/backend-utils-activity-waitevent-seams` (new, declarations only),
  `crates/port-pgsleep-seams` (new, declarations only)
- C sources read: `postgres-18.3/src/backend/storage/lmgr/s_lock.c`,
  `postgres-18.3/src/include/storage/s_lock.h`,
  `postgres-18.3/src/include/utils/wait_classes.h`,
  `postgres-18.3/src/common/pg_prng.c`
- c2rust cross-check: `c2rust-runs/backend-storage-lmgr-s-lock/src/s_lock.rs`
- Auditor: independent re-derivation; did not rely on the port's comments.

## Function inventory and verdicts

Every function definition in s_lock.c and every macro/inline of the s_lock.h
platform-independent layer, cross-checked against the c2rust rendering (which
confirms the build config: aarch64 `__sync_int32` TAS profile, `S_LOCK_TEST`
not defined).

| # | C function / macro (location) | Port location (crates/backend-storage-lmgr-s-lock/src/lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `s_lock_stuck()` (s_lock.c:80) | `s_lock_stuck` (line 239) | MATCH | NULL `func` -> `"(unknown)"` exactly as C; `elog(PANIC, "stuck spinlock detected at %s, %s:%d")` with same arg order (func, file, line), PANIC level 23 matches c2rust's `errstart(23,...)`; PANIC aborts so `-> !` + `unreachable!` is faithful (C PANIC never returns). The port also maps a `None` file to `"(unknown)"`; in C a NULL file into `%s` is undefined, so no defined input diverges. `S_LOCK_TEST` fprintf/exit branch is outside the build config (absent in c2rust), correctly not ported. |
| 2 | `s_lock()` (s_lock.c:99) | `s_lock` (line 252) | MATCH | init_spin_delay; `while TAS_SPIN` loop calling `perform_spin_delay`; `finish_spin_delay`; returns `delays`. Identical to C and c2rust. |
| 3 | `s_unlock()` (s_lock.c:116, `#ifdef USE_DEFAULT_S_UNLOCK`) | `s_unlock` / `Spinlock::unlock` (lines 128, 166) | MATCH | Not compiled on this config (arm/aarch64 defines `S_UNLOCK` as `__sync_lock_release`; absent from c2rust). Port's release-ordered store of 0 implements the platform `S_UNLOCK`; the default out-of-line variant is subsumed. |
| 4 | `perform_spin_delay()` (s_lock.c:124) | `perform_spin_delay` (line 270) | MATCH | `SPIN_DELAY()` first; pre-increment `spins` then `>= spins_per_delay`; pre-increment `delays` then `> NUM_DELAYS` -> stuck; `cur_delay == 0` seeds `MIN_DELAY_USEC`; wait-start(WAIT_EVENT_SPIN_DELAY) / pg_usleep(cur_delay) / wait-end in the same order (seamed, see below); `cur_delay += (int)(cur_delay * pg_prng_double(global) + 0.5)` with truncating cast; wrap to `MIN_DELAY_USEC` when `cur_delay > MAX_DELAY_USEC` (compared at i64 width as in C's int-vs-long compare); `spins = 0`. All identical. |
| 5 | `finish_spin_delay()` (s_lock.c:185) | `finish_spin_delay` (line 324) | MATCH | `cur_delay == 0` -> if `< MAX` then `Min(+100, MAX)`; else if `> MIN` then `Max(-1, MIN)`. Same guards, same clamps (verified against c2rust's expanded Min/Max). |
| 6 | `set_spins_per_delay()` (s_lock.c:204) | `set_spins_per_delay` (line 339) | MATCH | Plain assignment to the backend-local estimate. |
| 7 | `update_spins_per_delay()` (s_lock.c:214) | `update_spins_per_delay` (line 355) | MATCH | `(shared * 15 + local) / 16`, truncating integer division as in C. |
| 8 | `main()` (s_lock.c, `#ifdef S_LOCK_TEST`) | not ported | MATCH (n/a) | Standalone test harness, never compiled into the server (absent from c2rust). In-crate tests cover the same surface. |
| 9 | `static int spins_per_delay` (s_lock.c:74) | `SPINS_PER_DELAY` thread_local (line 73) | MATCH | `DEFAULT_SPINS_PER_DELAY = 100` verified against s_lock.h:720. Backend-private C global -> thread_local per this repo's backend-global convention. |
| 10 | `tas()` aarch64 inline (s_lock.h:256) | `Spinlock::tas` (line 143) | MATCH | C `__sync_lock_test_and_set(lock, 1)` (acquire) -> `AtomicI32::swap(1, Acquire)`; identical to c2rust's rendering. |
| 11 | `S_UNLOCK` arm (s_lock.h:262) | `Spinlock::unlock` (line 128) | MATCH | `__sync_lock_release` (release-ordered store of 0) -> `store(0, Release)`. |
| 12 | `TAS_SPIN(lock)` aarch64 (s_lock.h:272) | `Spinlock::tas_spin` / `tas_spin` (lines 151, 181) | MATCH | `*(lock) ? 1 : TAS(lock)` — non-locking pretest then TAS, exactly as c2rust renders inside `s_lock`. |
| 13 | `spin_delay()` aarch64 inline (s_lock.h:276) | `spin_delay` (line 206) | MATCH | `isb` on aarch64 (same instruction as C/c2rust); portable `spin_loop` hint on other arches (C x86 uses `rep; nop` = PAUSE, which is what `core::hint::spin_loop` emits on x86). |
| 14 | `S_LOCK(lock)` default macro (s_lock.h:664) | `s_lock_macro` (line 189) | MATCH | `TAS(lock) ? s_lock(...) : 0`. |
| 15 | `S_LOCK_FREE(lock)` (s_lock.h:669) | `s_lock_free` / `is_free` (lines 133, 171) | MATCH | `*(lock) == 0`, volatile read -> relaxed atomic load. |
| 16 | `S_INIT_LOCK(lock)` (s_lock.h:694) | `s_init_lock` (line 161) | MATCH | Defined as `S_UNLOCK(lock)`. |
| 17 | `init_spin_delay()` inline (s_lock.h:736) | `init_spin_delay` (line 219) | MATCH | Zeros spins/delays/cur_delay, records file/line/func; returns the struct by value (idiomatic, behavior identical). |

Constants verified against headers (not from memory):
`MIN_SPINS_PER_DELAY=10`, `MAX_SPINS_PER_DELAY=1000`, `NUM_DELAYS=1000`,
`MIN_DELAY_USEC=1000`, `MAX_DELAY_USEC=1000000` (s_lock.c:58-62);
`DEFAULT_SPINS_PER_DELAY=100` (s_lock.h:720);
`WAIT_EVENT_SPIN_DELAY = PG_WAIT_TIMEOUT | 6 = 0x09000006 = 150994950`
(wait_classes.h:25 `PG_WAIT_TIMEOUT 0x09000000U`; c2rust constant 150994950);
PANIC elevel 23 (c2rust `errstart(23, ...)`; `types_error::PANIC = ErrorLevel(23)`).

`pg_prng_double` is taken via a direct dependency on the already-merged
`pg-prng` crate (`global_prng(PgPrng::next_f64)`); spot-checked
`next_f64 = (xoroshiro128ss >> 12) * 2^-52` against `pg_prng.c:268` — identical
[0,1) construction, and the global state maps to `pg_global_prng_state`.

## Seam audit

Outward seams (both new, declaration-only crates with `seam_core::seam!`):

- `port_pgsleep_seams::pg_usleep(i64)` — `src/port/pgsleep.c` is unported
  (CATALOG `port-batch*`/`probe-port-srv-pgsleep` = todo), so a direct dep must
  fail. Call site is a bare `::call(status.cur_delay as i64)` — thin marshal,
  no logic in the seam path. OK.
- `backend_utils_activity_waitevent_seams::pgstat_report_wait_start(u32)` /
  `pgstat_report_wait_end()` — `backend-utils-activity-waitevent` is unported
  (CATALOG = todo). Bare `::call`s with the verified event constant. OK.

Inward seams: none declared for this unit; `init_seams()` is empty and is
called from `seams-init::init_all()` (seams-init/src/lib.rs:16). No `set()`
calls outside the owner anywhere in the workspace except `#[cfg(test)]` test
stubs inside this crate's own test module (established repo pattern; the real
installs belong to the owning units when they land).

No function body was replaced by a seam; all s_lock.c logic lives in this crate.

## Build / test

- `cargo build --workspace`: clean.
- `cargo test -p backend-storage-lmgr-s-lock`: 11/11 pass.
- `cargo clippy` on the three crates: no warnings in these crates.

## Spot-check of MATCH verdicts

Re-derived `perform_spin_delay` (row 4) and `finish_spin_delay` (row 5)
line-by-line against both the C and the c2rust expansion a second time,
including the pre-increment ordering, the truncating float cast, and the
Min/Max clamp directions; all confirmed.

## Verdict

**PASS** — every function MATCH (or correctly outside the build config), all
outward seam calls justified and thin, wiring complete.
