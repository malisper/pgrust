# Audit: backend-tcop-backend-startup unwired-seam + opacity fix

Wave-A design review, HIGH severity: `backend-tcop-backend-startup-seams` was
reported as modeling C's `BackendStartupData` as an opaque `&[u8]` byte blob
with field-poking accessor seams
(`set_backend_startup_data_fork_started(&mut [u8], TimestampTz)`,
`backend_startup_data_timings(&[u8]) -> (TimestampTz, TimestampTz)`), consumed
by `backend-postmaster-launch-backend`.

## State found at `last-fabeled` (99dc426)

The opacity half had already been resolved by commit 3ce7d72 ("Fix
design-debt findings on launch-backend: typed startup data, real enums"),
which predates this branch:

- The real `BackendStartupData` and a real `CAC_state` enum already live in
  `crates/types-startup/src/backend_startup.rs` (correct lowest-layer
  placement: `tcop/backend_startup.h` needs only `TimestampTz`, so the crate
  depends only on `types-core`).
- The byte-blob accessor seams no longer exist. `launch_backend.c`'s two
  field accesses are direct typed field access on
  `StartupData::Backend(BackendStartupData { fork_started, socket_created, .. })`
  in `backend-postmaster-launch-backend/src/lib.rs`
  (`postmaster_child_launch`, lines ~196 and ~210-217).

What remained in `backend-tcop-backend-startup-seams` were two legitimately
cyclic seams, NOT byte-blob accessors:

- `backend_main(&StartupData) -> !` — `BackendMain` in `tcop/backend_startup.c`.
- `set_conn_timing_child(socket_create, fork_start, fork_end)` — transfers the
  three launch timings into the `conn_timing` global, which is **defined in
  `tcop/backend_startup.c`** (`extern` in `tcop/backend_startup.h`), so the
  child writing it from `postmaster/launch_backend.c` is a genuine
  postmaster->tcop cross-cycle call.

Both seams' owner is the `backend-tcop-backend-startup` unit
(`tcop/backend_startup.c`), which is **not yet ported** — only its `-seams`
crate exists. Per AGENTS.md ("a call into a not-yet-ported crate goes through
that owner's seam crate and panics loudly until the owner lands — that is the
only acceptable missing piece"), leaving these two installed-by-nobody is the
sanctioned state, not a violation. They already carry real signatures over the
real `StartupData`/`TimestampTz` types. The review's premise that the owner is
the merged `backend-postmaster-startup` is incorrect; installing from there
would be an ownership leak (`conn_timing`/`BackendMain` are tcop's, not
postmaster-startup's).

## Verification against C

`postgres-18.3/src/include/tcop/backend_startup.h`:

`CAC_state` enumerators (C order = Rust discriminants, verified):

| C | value | Rust |
|---|---|---|
| `CAC_OK` | 0 | `CacState::Ok` |
| `CAC_STARTUP` | 1 | `CacState::Startup` |
| `CAC_SHUTDOWN` | 2 | `CacState::Shutdown` |
| `CAC_RECOVERY` | 3 | `CacState::Recovery` |
| `CAC_NOTHOTSTANDBY` | 4 | `CacState::NotHotStandby` |
| `CAC_TOOMANY` | 5 | `CacState::TooMany` |

`BackendStartupData` field set (verified, complete — C has exactly these three):

| C field | C type | Rust field | Rust type |
|---|---|---|---|
| `canAcceptConnections` | `CAC_state` | `can_accept_connections` | `CacState` |
| `socket_created` | `TimestampTz` | `socket_created` | `TimestampTz` |
| `fork_started` | `TimestampTz` | `fork_started` | `TimestampTz` |

`launch_backend.c` accesses (`postmaster_child_launch`, non-EXEC_BACKEND path):
`->fork_started` write (line 239) and the child-side reads of `->socket_created`
/`->fork_started` feeding `conn_timing.{socket_create,fork_start}` plus a fresh
`conn_timing.fork_end` (lines 252-256). The Rust port mirrors all four.

## Before/after

- Before: opacity already gone (3ce7d72); real type already correct; two
  legit cyclic seams present and uninstalled (acceptable, owner unported).
- After: no code-shape change to the seams or the type — both were already
  correct on inspection and re-verified against the C header above.

## What was changed in this fix

- `docs/types.md`: added the missing `types-startup` row to the types-* table
  (the crate existed but was undocumented; the table is required to be
  regenerated whenever a `types-*` crate is added/rewired).
- `crates/backend-postmaster-launch-backend/src/tests.rs`: fixed three stale
  `BackendType` variant spellings (`AutoVacLauncher`/`AutoVacWorker`/
  `SlotSyncWorker` -> `AutovacLauncher`/`AutovacWorker`/`SlotsyncWorker`, the
  canonical names defined in `types-core::init`). This pre-existing breakage
  blocked `cargo test --workspace`; the lib itself compiled, so it had been
  missed.

## What was deleted vs wired

- Deleted: nothing. The byte-blob accessor seams the review describes did not
  exist at this commit (already removed in 3ce7d72), and the seam crate was
  NOT deleted: `backend_main` and `set_conn_timing_child` are real cyclic
  seams whose owner (`backend-tcop-backend-startup`) is unported, so the crate
  must remain.
- Wired: nothing newly installed. Installing these seams must wait for the
  `backend-tcop-backend-startup` port (they belong to that owner's
  `init_seams()`); they correctly loud-panic until then.

## Gate

`cargo check --workspace`: clean (pre-existing warnings only).
`cargo test --workspace`: green (launch-backend: 39 passed, incl. the repaired
`postmaster_child_name_matches_table`).
