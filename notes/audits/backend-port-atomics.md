# Audit: backend-port-atomics

- **Unit:** `backend-port-atomics`
- **C source:** `src/backend/port/atomics.c` (PostgreSQL 18.3, 73 lines)
- **c2rust rendering:** `../pgrust/c2rust-runs/backend-port-atomics/src/atomics.rs`
- **Port:** `crates/backend-port-atomics/src/lib.rs`
- **Auditor:** independent re-derivation from the C sources and headers
  (`port/atomics.h`, `port/atomics/fallback.h`, `port/atomics/generic-gcc.h`),
  2026-06-12

## Build-config determination (why this unit is empty)

The entire body of `atomics.c` (everything after the includes) is wrapped in
`#ifdef PG_HAVE_ATOMIC_U64_SIMULATION ... #endif`. Re-derived guard chain:

1. `port/atomics/fallback.h:23` defines `PG_HAVE_ATOMIC_U64_SIMULATION` only
   inside `#if !defined(PG_HAVE_ATOMIC_U64_SUPPORT)`.
2. `port/atomics/generic-gcc.h:98-102` defines `PG_HAVE_ATOMIC_U64_SUPPORT`
   when `HAVE_GCC__ATOMIC_INT64_CAS` (or `__SYNC_INT64_CAS`) is set and
   `PG_DISABLE_64_BIT_ATOMICS` is not. On this build target (aarch64-darwin,
   clang) configure sets `HAVE_GCC__ATOMIC_INT64_CAS`, so native 64-bit
   atomics are used and the simulation guard is inactive.
3. Post-preprocessor ground truth: the c2rust rendering of this unit,
   `../pgrust/c2rust-runs/backend-port-atomics/src/atomics.rs`, is **0 bytes**
   — the translation unit emitted no symbols on this build.

So the function inventory of the *compiled* unit is empty; the three guarded
definitions exist only in the original C, outside the build config (the case
the audit skill explicitly notes: "`#if` branches outside the build config
exist only in the original C").

## Function inventory

Every function definition in `atomics.c`, including the inactive-guard ones:

| # | C function (atomics.c) | Guard | Port location | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `pg_atomic_init_u64_impl` (:23) | `PG_HAVE_ATOMIC_U64_SIMULATION` (inactive) | — (correctly absent) | MATCH (excluded by build config) | Spinlock-emulated init for platforms without native u64 atomics. Not compiled on this build (c2rust output empty); Rust callers use `core::sync::atomic::AtomicU64` natively, which is the exact analogue of the *active* native path. |
| 2 | `pg_atomic_compare_exchange_u64_impl` (:34) | same (inactive) | — (correctly absent) | MATCH (excluded by build config) | Strong CAS emulated under a spinlock. Excluded on this build; native strong CAS (`compare_exchange`) is what the active configuration provides. |
| 3 | `pg_atomic_fetch_add_u64_impl` (:62) | same (inactive) | — (correctly absent) | MATCH (excluded by build config) | Spinlock-emulated fetch-add. Excluded on this build. |

No other definitions exist in the file (verified by reading the full 73-line
source); the cross-check against the c2rust rendering finds nothing to add
(the rendering is empty).

## Crate contents

`crates/backend-port-atomics/src/lib.rs` contains only a doc comment
explaining the above and a no-op `pub fn init_seams() {}`. It exports no
behavior, which is exactly correct for an empty-by-construction unit. No
extraneous logic was introduced.

## Seam audit

- The crate declares **no seams** and there is no
  `backend-port-atomics-seams` crate — correct, since there is no behavior.
- `init_seams()` is an empty function (the degenerate case of "nothing but
  `set()` calls").
- `crates/seams-init/src/lib.rs:10` calls
  `backend_port_atomics::init_seams();` — wired per convention.
- Grep across `crates/` finds no other crate installing or referencing
  atomics seams. No findings.

## Build

`cargo build -p backend-port-atomics -p seams-init` succeeds.

## Verdict

**PASS.** The compiled translation unit is empty on this build configuration
(verified independently via the header guard chain and the empty c2rust
rendering); the port is correspondingly empty, with the no-op `init_seams()`
wired into `seams-init`. No `MISSING`/`PARTIAL`/`DIVERGES` entries; no seam
findings.
