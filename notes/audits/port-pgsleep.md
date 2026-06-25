# Audit: port-pgsleep

- Verdict: **PASS**
- Date: 2026-06-13
- Model: claude-opus-4-8[1m]
- Unit: `port-pgsleep` (CATALOG row `probe-port-srv-pgsleep`, `src/port/pgsleep.c`)
- Branch: `port/port-pgsleep` (port commit `0c1bef32`)
- Crates: `crates/port-pgsleep`, `crates/port-pgsleep-seams` (pre-existing, declarations only)
- C source read: `postgres-18.3/src/port/pgsleep.c`
- c2rust cross-check: `c2rust-runs/probe-port-srv-pgsleep/src/pgsleep.rs`
- Auditor: independent re-derivation from the C and c2rust; did not rely on the port's comments or self-review.

## Function inventory and verdicts

`pgsleep.c` defines exactly one function. c2rust confirms a single `#[no_mangle]
pub unsafe extern "C" fn pg_usleep` and no other definitions, statics, or
inline helpers. The file body is wholly inside the `#if defined(FRONTEND) ||
!defined(WIN32)` guard; this is the non-Windows / frontend build, the variant
this unit targets.

| # | C function (location) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `pg_usleep(long microsec)` (pgsleep.c:40) | `pg_usleep` (crates/port-pgsleep/src/lib.rs:43) | MATCH | See detailed re-derivation below. |

### `pg_usleep` — detailed re-derivation

- Guard `if (microsec > 0)` → `if microsec > 0` (line 43). Identical; non-positive
  microsec returns immediately with no sleep, exactly as C.
- `delay.tv_sec = microsec / 1000000L` → `tv_sec: (microsec / 1_000_000) as c_long`.
  On the LP64 targets (darwin/linux) C `long` is 64-bit, so the port's `i64`
  parameter is width-faithful; the division is computed at the same width and
  the cast to `c_long` matches `struct timespec.tv_sec` (`__darwin_time_t =
  c_long` per c2rust). MATCH.
- `delay.tv_nsec = (microsec % 1000000L) * 1000` → `tv_nsec: ((microsec %
  1_000_000) * 1000) as c_long`. Same operation order and width; for any
  `microsec > 0` the product fits well within `c_long`. MATCH.
- `(void) nanosleep(&delay, NULL)` → `nanosleep(&delay, core::ptr::null_mut())`,
  return value intentionally discarded (matching the C `(void)` cast; nanosleep
  may return early on signal in both). MATCH. The port declares `nanosleep` and a
  `#[repr(C)]` `Timespec { tv_sec: c_long, tv_nsec: c_long }`, which is the real
  libc `struct timespec` layout c2rust emits — no invented opacity.
- `#else SleepEx(...)` (WIN32 arm) — outside this build config and absent from
  c2rust; correctly not ported (this is the non-WIN32 implementation per the
  enclosing `#if`). MATCH (n/a).

`pg_usleep` returns `void` and never `ereport`s; the Rust port is correctly an
infallible `pub fn pg_usleep(microsec: i64)` returning `()`.

## Seam audit

Ownership is by C-source coverage. The only C file in this unit is
`src/port/pgsleep.c`; the sole owned seam crate is `crates/port-pgsleep-seams`.

- `crates/port-pgsleep-seams` declares exactly one seam:
  `pub fn pg_usleep(microsec: i64)` (void return — mirrors the C void/infallible
  failure surface).
- The owning crate installs it: `port_pgsleep::init_seams()` is
  `port_pgsleep_seams::pg_usleep::set(pg_usleep);` and nothing else — a single
  `set()` call, no logic.
- `seams-init::init_all()` calls `port_pgsleep::init_seams()` (added to both
  `crates/seams-init/Cargo.toml` deps and `crates/seams-init/src/lib.rs`).
- No uninstalled declaration; no `set()` outside the owner.
- **Outward seams: none.** The port calls libc `nanosleep` directly (an
  `extern "C"` import), which is acyclic and requires no seam. No branching,
  node construction, or computation crosses any seam path.
- No function body was replaced by a seam call to "somewhere else" — the sleep
  logic lives in this crate.

No seam findings.

## Design conformance (§3b)

- No allocating function/seam: `pg_usleep` performs no allocation, so the
  absence of `Mcx` / `PgResult` is correct (it is genuinely infallible).
- Seam signature mirrors the C failure surface: C `void pg_usleep(long)` never
  `ereport`s → seam `fn pg_usleep(microsec: i64)` with no `PgResult`. Faithful.
- No invented opacity (types.md 6-7): `Timespec` is the concrete libc layout,
  not a stand-in handle; no `void*` layering.
- No shared statics for per-backend globals; no ambient-global seams; no locks
  held across `?`; no registry-shaped side tables; no unledgered divergence
  markers.

No design-conformance findings.

## Build

`cargo build -p port-pgsleep -p port-pgsleep-seams` succeeds on the branch.

## Verdict

Every function `MATCH`, zero seam findings, zero design-conformance findings.
**PASS.** CATALOG row `probe-port-srv-pgsleep` set to `audited`.
