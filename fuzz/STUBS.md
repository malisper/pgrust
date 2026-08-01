# STUBS.md — the shared stub-pin facility for differential fuzz targets

Michael-ratified 2026-08-01. Four facilities that let a differential fuzz
target pin session state IDENTICALLY on the shipped-Rust side and the
C-oracle side, unlocking state-dependent carves:

| facility       | pins                                                        |
|----------------|-------------------------------------------------------------|
| `stub:guc`     | GUC scalars: extra_float_digits, DateStyle+DateOrder, IntervalStyle, standard_conforming_strings |
| `stub:clock`   | GetCurrentTimestamp-shaped reads, to a fuzzed TimestampTz    |
| `stub:prng`    | the global-prng analog, seeded from the fuzz input           |
| `stub:workmem` | work_mem / maintenance_work_mem ceilings                     |

Code: Rust half `core/src/stubs.rs`; C half
`core/csrc/stubshims/pg_stub_state.{c,h}` (registered in the main
`core/build.rs` cc::Build).

## The contract

1. **One derivation.** The target hands fuzz bytes to a `stubs::*::pin_*`
   function. The pin derives the canonical value ONCE, bounded to the
   setting's legal range (ranges taken from the shipped guc_tables / type
   domains: efd [-15,3], the 5x3 DateStyle/DateOrder pairs, the 4
   IntervalStyles, work_mem [64, MAX_KILOBYTES] kB, timestamps
   [MIN_TIMESTAMP, END_TIMESTAMP)). Because both sides receive the SAME
   derived value, out-of-range fuzz bytes clamp identically by
   construction.
2. **Both sides, always.** The pin writes the Rust-side session seam (the
   real thread-local cell the shipped code reads: adt_float's
   extra_float_digits cell, adt_datetime's style cells, scan_fgram's scs
   cell, or the facility-owned cells for clock/prng/workmem) AND the
   C-side `pg_stub_*` thread-local. A pinned value is part of the compared
   input — never let one side default.
3. **C consumption.** A NEW oracle TU includes
   `csrc/stubshims/pg_stub_state.h` and reads the `pg_stub_*` globals
   (e.g. `#define extra_float_digits pg_stub_extra_float_digits` ahead of
   a verbatim paste) instead of defining another per-TU copy. Vendored C
   is never edited — where a family TU already exposes a
   value-as-argument entry point, a `pg_stub_*_guc` wrapper in the shim TU
   routes the pinned global into it (that is how the controls reach the
   verbatim consumers today).
4. **GUC assign hooks.** The facility pins the parsed, post-assign-hook
   internal values (style/order/istyle enum ints), the family convention
   since datetime_io_diff — legal-range derivation stands in for the hook.
   No shipped assign hook currently in the fuzz core does more than store
   the value; a GUC whose C-side assign hook does real work that the shim
   cannot reproduce must NOT be force-pinned (list it in the target header
   instead).

## Declaring pins in a target

```rust
use crate::stubs;

pub fn my_diff(data: &[u8]) {
    let Some((&b0, rest)) = data.split_first() else { return };
    // 1. declare + set the pins from leading input bytes (both sides):
    let efd = stubs::guc::pin_extra_float_digits(b0);
    let now = stubs::clock::pin_now(i64::from_le_bytes(...));
    stubs::prng::pin_seed(u64::from_le_bytes(...));
    let (wm, _) = stubs::workmem::pin(..., ...);
    // 2. run BOTH sides' state-reading entry points and compare.
}
```

Rust-side reads for driver-passed values: `stubs::clock::now_usecs()`,
`stubs::prng::rust_u64()/rust_double()`, `stubs::workmem::work_mem()/
maintenance_work_mem()`. Fuzz binaries (one target per process) that need
shipped pure code's `timestamp_seams::get_current_timestamp` to resolve to
the pin call `stubs::clock::install_timestamp_seam()` at init — first-wins,
returns whether it installed; never call it from the shared `cargo test`
binary (legacy targets install their own constant with an unguarded
`set()`).

## Must-fail controls (harness-detection-power law)

Every facility ships a control in `core/src/stubs.rs` tests proving the pin
is ALIVE: (a) parity through a REAL verbatim vendored consumer under
matched pins, then (b) a deliberate one-sided mismatch that the comparator
MUST see. Verified by a dead-plane sweep (all C setters temporarily
no-op'd): every control fails, only the pure-arithmetic `clamp_edges`
survives.

| control test                     | vendored C consumer                              |
|----------------------------------|--------------------------------------------------|
| `control_guc_efd_pin`            | float8out_internal_efd (pg_float_io.c)           |
| `control_guc_datestyle_pin`      | EncodeDateTime via pg_tsdiff_timestamp_out       |
| `control_guc_intervalstyle_pin`  | EncodeInterval via pg_tsdiff_interval_out        |
| `control_guc_scs_pin`            | (transport-level only: no vendored C lexer in csrc yet; Rust plane is the real scan_fgram cell) |
| `control_clock_pin`              | pg_stub_get_current_timestamp (+ domain clamp)   |
| `control_prng_pin`               | verbatim xoroshiro128** (pg_pg_prng_io.c) vs shipped pg_prng |
| `control_workmem_pin`            | verbatim bloom_create sizing (pg_libfam_io.c) vs shipped bloomfilter |

## Demonstration wiring

`float_misc_diff` arm 15 (core/src/diff.rs): the extra_float_digits pin
goes through `stubs::guc::pin_extra_float_digits` and both sides run their
GUC-READING output paths — shipped `adt_float::float8out` (reads the
session cell) vs `pg_stub_float8out_guc` (verbatim body reading
`pg_stub_extra_float_digits`) — no efd argument anywhere in the exec.

## Not wired (deliberately)

- `enable_*` planner flags: no planner consumer is linked into the fuzz
  core today; wire through the same pattern (a bool cell + a pg_stub
  global) when a planner-math target lands.
- standard_conforming_strings has no vendored C consumer in csrc yet
  (transport-level control only; see table).
