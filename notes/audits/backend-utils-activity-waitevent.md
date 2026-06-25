# Audit: backend-utils-activity-waitevent

- **Verdict:** PASS (after one fix round)
- **Date:** 2026-06-13
- **Model:** Claude Fable 5 (Opus 4.8 1M)
- **Branch:** port/backend-utils-activity-waitevent
- **C sources:** `src/backend/utils/activity/wait_event.c`, `wait_event_funcs.c`
  (+ build-time generated `pgstat_wait_event.c`, `wait_event_funcs_data.c` via
  `generate-wait_event_types.pl` over `wait_event_names.txt`)
- **c2rust oracle:** `../pgrust/c2rust-runs/backend-utils-activity-waitevent/src/`
- **Port crate:** `crates/backend-utils-activity-waitevent/src/lib.rs`
- **Owned seam crate:** `crates/backend-utils-activity-waitevent-seams`

## 1. Function inventory and verdicts

### wait_event.c

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `WaitEventCustomShmemSize` | 103 | lib.rs:774 | MATCH | MAXALIGN(counter) + 2× hash_estimate_size; `add_size` -> PgResult per ereport surface |
| `WaitEventCustomShmemInit` | 119 | lib.rs:794 | MATCH | ShmemInitStruct + `!found` init (nextId=1, SpinLockInit) + two ShmemInitHash (BLOBS keysize=sizeof(uint32); STRINGS keysize=NAMEDATALEN) |
| `WaitEventExtensionNew` | 163 | lib.rs:841 | MATCH | delegates to CustomNew(PG_WAIT_EXTENSION, ..) |
| `WaitEventInjectionPointNew` | 169 | lib.rs:847 | MATCH | delegates to CustomNew(PG_WAIT_INJECTIONPOINT, ..) |
| `WaitEventCustomNew` | 175 | lib.rs:853 | MATCH | name-length elog; SHARED find + release; EXCLUSIVE re-find under lock; spinlock id alloc with `>= MAX_SIZE(128)` limit ereport; HASH_ENTER both tables + strlcpy; class-conflict ereport(DUPLICATE_OBJECT). All lock release paths preserved on the error returns. |
| `GetWaitEventCustomIdentifier` | 276 | lib.rs:946 | MATCH | `== PG_WAIT_EXTENSION` -> "Extension"; SHARED lookup; null -> elog(ERROR) INTERNAL |
| `GetWaitEventCustomNames` | 306 | lib.rs:973 | MATCH | palloc(char**)+count -> idiomatic `Vec<String>`; class-mask filter; lock released (incl. on hash_seq error path) |
| `pgstat_set_wait_event_storage` | 349 | lib.rs:661 | MATCH | redirect to installable shared slot, guarded by restore-on-drop |
| `pgstat_reset_wait_event_storage` | 361 | lib.rs:671 | MATCH | back to thread-local default slot |
| `pgstat_report_wait_start` | hdr | lib.rs:679 | MATCH | store wait_event_info into current slot |
| `pgstat_report_wait_end` | hdr | lib.rs:684 | MATCH | store 0 |
| `pgstat_get_wait_event_type` | 373 | lib.rs:152 | MATCH | `==0`->None; 10-arm class switch + `default "???"` verified against wait_classes.h |
| `pgstat_get_wait_event` | 431 | lib.rs:582 | MATCH | `==0`->None; LWLock/Lock via outward seams; Ext/InjPoint via CustomIdentifier; six generated classes via build-once name lookup; default "unknown wait event" |
| `pgstat_get_wait_activity/bufferpin/client/io/ipc/timeout` (generated) | pgstat_wait_event.c | `wait_event_data()` lib.rs:412 | MATCH | the 6 generated per-class switches reproduced as one build-once `(info)->name` lookup; ids assigned in case-insensitive sorted order (first member = PG_WAIT_<CLASS>); unknown -> "unknown wait event" default. |

### wait_event_funcs.c

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `pg_get_wait_events` | 46 | lib.rs:1051 | MATCH | `waitEventData[]` (generated `wait_event_funcs_data.c`) reproduced as build-once `wait_event_funcs_data()`; then Extension + InjectionPoint custom rows with the exact two format strings. SRF/tuplestore plumbing (`InitMaterializedSRF`/`tuplestore_putvalues`) is the fmgr boundary; row construction is faithful. |

c2rust artifact `tas` (wait_event.rs:369) is an inlined `s_lock.h`/`spin.h` helper, not owned by this unit (lives in backend-storage-lmgr-s-lock). Correctly excluded.

## 2. Generated-table logic (the keystone)

The port re-implements `generate-wait_event_types.pl` at runtime over an
embedded, byte-identical copy of `wait_event_names.txt`:
- `Section: ClassName ... - <Name>` greedy `rsplit_once("- ")` == perl `s/^.*- //`.
- ABI_compatibility lines kept in file order, appended after the case-insensitive
  sort by column-2 (the event symbol) — matches perl's `sort` + post-append.
- name transform: verbatim for LWLock/Lock, else CamelCase per `_`-split part.
- description transform pipeline (substr 1,-2; `<quote>`->`"`; paired SGML strip;
  GUC `<xref linkend="guc-...">`->underscored; drop `; see...`); the `'`->`\'`
  C-source escape is correctly omitted (no runtime content change).

**Independent oracle check:** ran the real perl generator on the source
`wait_event_names.txt`, parsed its `wait_event_funcs_data.c` into a TSV (273 rows),
and diffed against the committed `wait_event_funcs_data.golden.tsv` — identical.
The crate test then compares `wait_event_funcs_data()` against that golden file
row-for-row (273 rows, per-class counts, transform samples). End-to-end verified.

## 3. Constants (verified against C headers, not memory)

- `PG_WAIT_*` class bases (LWLOCK 0x01, LOCK 0x03, BUFFERPIN 0x04, ACTIVITY 0x05,
  CLIENT 0x06, EXTENSION 0x07, IPC 0x08, TIMEOUT 0x09, IO 0x0A, INJECTIONPOINT
  0x0B) — match `include/utils/wait_classes.h`.
- `WAIT_EVENT_CLASS_MASK=0xFF000000`, `ID_MASK=0x0000FFFF` — match wait_event.c.
- `WAIT_EVENT_CUSTOM_HASH_INIT_SIZE=16`, `MAX_SIZE=128`, `INITIAL_ID=1` — match.
- `NAMEDATALEN=64` — match `pg_config_manual.h`.
- `WaitEventCustomLock = 48` — match `include/storage/lwlocklist.h` `PG_LWLOCK(48, WaitEventCustom)`.

## 4. Seam / wiring audit

Owned seam crate: `backend-utils-activity-waitevent-seams` (maps to wait_event.c
+ wait_event_funcs.c). `backend-storage-ipc-waiteventset-seams` belongs to a
different unit (waiteventset.c) and is not owned here.

Outward seams used by the port are all genuine cross-crate, thin marshal+delegate:
- `get_lwlock_identifier`, `lwlock_acquire_main` (lwlock-seams)
- `get_lock_name_from_tag_type` (lmgr-seams)
- `hash_search`/`hash_seq_*`/`hash_estimate_size`/`hash_get_num_entries` (dynahash-seams)
Direct deps: shmem (`ShmemInitStruct`/`ShmemInitHash`/`add_size`), s-lock (`Spinlock`),
error. No logic in any seam path.

### FINDING (fixed): uninstalled owned seams

The seam crate declares four seams; the original `init_seams()` installed only
`pgstat_report_wait_start` / `pgstat_report_wait_end`. The two shmem seams —
`wait_event_custom_shmem_size` and `wait_event_custom_shmem_init` — were declared
and *consumed* by `backend-storage-ipc::ipci_core` (CalculateShmemSize /
CreateOrAttachShmemStructs call them via the bgworker_repl_stats shim) but never
`set()` by this owner. Per audit SKILL §3 (every declaration in an owned seam
crate must be installed) this is an automatic FAIL: ipci shmem init would panic
on the uninstalled seams at runtime.

**Fix:** `init_seams()` now also installs `wait_event_custom_shmem_size ->
WaitEventCustomShmemSize` and `wait_event_custom_shmem_init ->
WaitEventCustomShmemInit` (signatures match exactly: `() -> PgResult<Size>` and
`() -> PgResult<()>`). `init_seams()` remains nothing but `set()` calls, and
`seams-init::init_all()` already calls it (recurrence_guard passes).

## 5. Gate

- `cargo check --workspace` — clean (warnings only).
- `cargo test --workspace` — all pass; no failing `test result:` lines; the 2
  known timeout flakes did not surface.
- crate tests: 10 pass; `seams-init` recurrence_guard passes.

## Verdict: PASS
