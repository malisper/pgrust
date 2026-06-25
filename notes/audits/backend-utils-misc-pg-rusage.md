# Audit: backend-utils-misc-pg-rusage

- Unit: `backend-utils-misc-pg_rusage`
- C source: `src/backend/utils/misc/pg_rusage.c` (postgres-18.3); header `src/include/utils/pg_rusage.h`
- c2rust rendering: `c2rust-runs/backend-utils-misc-pg_rusage/src/pg_rusage.rs`
- Port: `crates/backend-utils-misc-pg-rusage/src/lib.rs`
- Auditor: independent re-derivation from C + c2rust, 2026-06-12

## Function inventory

The C file defines exactly two functions (no statics, no inline helpers; the
header adds only the `PGRUsage` struct typedef). The c2rust rendering confirms
the same two symbols and nothing else.

| C function (pg_rusage.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `pg_rusage_init` (L26-31) | `pg_rusage_init` (L91-98), helpers `os_getrusage_self` (L74-88), `os_gettimeofday` (L63-68) | MATCH | `getrusage(RUSAGE_SELF)` via `libc::getrusage` with `libc::RUSAGE_SELF` (= 0, matches c2rust constant); `gettimeofday(&tv, NULL)` rendered as `SystemTime::now() - UNIX_EPOCH`, which is the same wall-clock microsecond read. C ignores both return codes; port likewise degrades to zeros / epoch on (practically impossible) failure — same all-zero contents C would read from an unfilled struct. Field set reduced to the three timevals the unit ever reads (tv, ru_utime, ru_stime); the other 14 rusage fields are never touched by any code in this unit, so the reduction is behavior-preserving. |
| `pg_rusage_show` (L39-73) | `pg_rusage_show` (L102-105) + `pg_rusage_show_between` (L111-127), `PgRUsageDelta::between` (L145-158), `elapsed_pair` (L163-173) | MATCH | Same structure: take fresh snapshot `ru1`, then for each of tv / ru_stime / ru_utime apply the borrow-a-second fixup `if end.usec < start.usec { end.sec -= 1; end.usec += 1_000_000 }` — fixups are on independent fields, so the split into `elapsed_pair` preserves C's order-insensitive behavior. Subtractions are individually narrowed to `i32` (C's `(int)` casts), and crucially the `/ 10000` is applied **after** the int cast of the usec difference, exactly matching C's `(int)(a - b) / 10000` precedence (cast binds tighter than `/`). Format string `"CPU: user: %d.%02d s, system: %d.%02d s, elapsed: %d.%02d s"` ported verbatim as `"{}.{:02}"` per field; `{:02}` and `%02d` agree for all i32 values (including negatives: both emit sign then zero-pad to width). The gettext `_()` wrapper is the project-wide systemic deferral. Returns owned `String` instead of C's non-reentrant `static char[100]`; callers in C treat the result as an immediately-consumed read-only string, so ownership change is behavior-preserving (and strictly safer; the 100-byte truncation bound is unreachable: max rendered length is far below 100). |

API-shape notes (non-behavioral): `PgRUsage::new()` / `from_parts()` are added
constructors; `pg_rusage_show_between` is the deterministic core split out for
testability — `pg_rusage_show(ru0)` composes them exactly as the C body does.

## Constants verified

- `RUSAGE_SELF` = 0 (c2rust L52; `libc::RUSAGE_SELF` is 0 on darwin/linux) — MATCH.
- Borrow fixup constants 1 sec / 1_000_000 usec — MATCH (C L49-60).
- Centisecond divisor 10000 — MATCH (C L66/68/70).
- Format string verbatim vs C L64 — MATCH.

## Seam audit

- `crates/backend-utils-misc-pg-rusage` has no seams crate and makes zero seam
  calls. Its only external calls are the OS (`libc::getrusage`,
  `SystemTime::now`), made directly in-crate — correct for a leaf unit; no
  dependency cycle exists to justify a seam.
- `init_seams()` is an empty no-op and is invoked by
  `crates/seams-init/src/lib.rs` (`backend_utils_misc_pg_rusage::init_seams()`),
  with the path dependency declared in `crates/seams-init/Cargo.toml`. No
  uninstalled seams, no out-of-owner `set()` calls.

## Tests / build

- `cargo test -p backend-utils-misc-pg-rusage`: 6/6 pass (borrow fixup, exact
  boundary, truncation toward zero, formatting, live capture).
- `cargo build --workspace`: clean.

## Verdict

**PASS** — both functions MATCH; zero seam findings.
