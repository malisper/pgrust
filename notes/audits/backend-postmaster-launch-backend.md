# Audit: backend-postmaster-launch-backend

- C source: `src/backend/postmaster/launch_backend.c` (postgres-18.3, 1072 lines)
- c2rust rendering: `../pgrust/c2rust-runs/backend-postmaster-launch-backend/src/launch_backend.rs`
- Port: `crates/backend-postmaster-launch-backend/src/lib.rs` (+ `tests.rs`)
- Audit date: 2026-06-12, branch `port/backend-postmaster-launch-backend` (commit 7b34707)

## Build configuration

The Unix build does not define `EXEC_BACKEND`. The c2rust rendering (which ran
post-preprocessor on the actual build) contains exactly three definitions from
this file: `child_process_kinds`, `PostmasterChildName`,
`postmaster_child_launch` (plus the header-inline `MemoryContextSwitchTo`).
Everything between `#ifdef EXEC_BACKEND` ... `#endif` is outside the build
config and is correctly not ported (documented in the crate's module doc and
the CATALOG note).

## Function inventory

| C function (launch_backend.c) | C lines | In build? | Port location | Verdict | Notes |
|---|---|---|---|---|---|
| `child_process_kinds[]` (static table) | 179–208 | yes | `CHILD_PROCESS_KINDS`, lib.rs:48–161 | MATCH | All 18 entries verified one-by-one against C and c2rust: names byte-identical; `shmem_attach` false only for B_INVALID/B_STANDALONE_BACKEND/B_LOGGER; `main_fn` NULL only for B_INVALID/B_WAL_SENDER/B_STANDALONE_BACKEND; every non-NULL `main_fn` is the owner seam for the same C symbol (BackendMain, AutoVacLauncherMain, AutoVacWorkerMain, BackgroundWorkerMain, ReplSlotSyncWorkerMain, PgArchiverMain, BackgroundWriterMain, CheckpointerMain, IoWorkerMain, StartupProcessMain, WalReceiverMain, WalSummarizerMain, WalWriterMain, SysLoggerMain). Designated-initializer indices match `BackendType` values in `types-core/src/init.rs`, which match `miscadmin.h` (B_INVALID=0 ... B_LOGGER=17, BACKEND_NUM_TYPES=18). |
| `PostmasterChildName` | 210–214 | yes | `postmaster_child_name`, lib.rs:164 | MATCH | Direct table index, same as C/c2rust. |
| `postmaster_child_launch` | 228–295 | yes | `postmaster_child_launch`, lib.rs:183 | MATCH | See detailed comparison below. |
| `IsExternalConnectionBackend` (miscadmin.h macro used here) | miscadmin.h:405 | yes | `is_external_connection_backend`, lib.rs:25 | MATCH | `== B_BACKEND \|\| == B_WAL_SENDER`, verified against header. |
| `internal_forkexec` (non-WIN32) | 306–403 | no (EXEC_BACKEND) | — | N/A | Not in build config; absent from c2rust. |
| `internal_forkexec` (WIN32) | 416–584 | no | — | N/A | |
| `SubPostmasterMain` | 597–715 | no | — | N/A | |
| `save_backend_variables` | 729–807 | no | — | N/A | |
| `write_duplicated_handle` (WIN32) | 814–835 | no | — | N/A | |
| `write_inheritable_socket` (WIN32; non-WIN32 is a macro) | 844–860 | no | — | N/A | |
| `read_inheritable_socket` (WIN32; non-WIN32 is a macro) | 865–899 | no | — | N/A | |
| `read_backend_variables` | 902–993 | no | — | N/A | |
| `restore_backend_variables` | 996–1070 | no | — | N/A | |

## Detailed comparison: `postmaster_child_launch`

Step-by-step against the C (lines 228–295) and c2rust (lines 868–922):

1. `Assert(IsPostmasterEnvironment && !IsUnderPostmaster)` → `debug_assert!`
   over the two `backend-utils-init-small-seams` global-read seams. Same
   assert-build-only semantics.
2. Pre-fork: `IsExternalConnectionBackend(child_type)` →
   `((BackendStartupData *) startup_data)->fork_started = GetCurrentTimestamp()`.
   Port: same predicate, `get_current_timestamp` seam, then
   `set_backend_startup_data_fork_started` seam writes into the blob. Done in
   the parent before fork, as in C (child inherits via fork copy).
3. `pid = fork_process()`; `pid == 0` child branch:
   - timing transfer for external-connection backends: reads
     `socket_created`/`fork_started` from the blob, takes a fresh
     `GetCurrentTimestamp()` as `fork_end`, stores all three into
     `conn_timing` — same fields, same order, via owner seams.
   - `ClosePostmasterPorts(child_type == B_LOGGER)` — same predicate.
   - `InitPostmasterChild()` — seam.
   - `if (!shmem_attach) { dsm_detach_all(); PGSharedMemoryDetach(); }` —
     same table lookup, same call order.
   - `MemoryContextSwitchTo(TopMemoryContext)` →
     `switch_to_top_memory_context` seam, before the client-sock store, so
     `MyClientSocket` lands in TopMemoryContext as in C.
   - `MyPMChildSlot = child_slot` (int → i32).
   - `if (client_sock) { MyClientSocket = palloc(...); memcpy(...); }` →
     `set_my_client_socket(*client_sock)` with `ClientSocket: Copy`
     (palloc+copy happens in the owner; value semantics identical).
   - `main_fn(startup_data, startup_data_len)` + `pg_unreachable()` →
     `main_fn(startup_data)` with `fn(&[u8]) -> !`; the byte slice carries
     pointer+length. A NULL `main_fn` slot (undefined behavior in C, a
     launch that postmaster never performs) is a loud panic — acceptable
     loud equivalent, not a behavior the C exercises.
4. Parent returns `pid` (including `-1` on fork failure, passed through from
   `fork_process`).

No branch, loop, error path, or constant diverges.

## Constants verified against headers

- `BackendType` values B_INVALID=0 ... B_LOGGER=17 and
  `BACKEND_NUM_TYPES = B_LOGGER + 1 = 18`: `miscadmin.h:337–377` vs
  `types-core/src/init.rs` — match (and c2rust's `[child_process_kind; 18]`).
- `IsExternalConnectionBackend`: `miscadmin.h:405–406` — match.
- `ClientSocket { pgsocket sock; SockAddr raddr; }`: `libpq-be.h:248–252` vs
  `types-net/src/net.rs` — match. `SockAddr { struct sockaddr_storage addr;
  socklen_t salen; }`: `pqcomm.h:30–34` — `[u8; 128]` + `u32` mirror
  (`_SS_MAXSIZE` 128 on Darwin/Linux).
- `BackendStartupData` fields `socket_created` / `fork_started`
  (`backend_startup.h:44–60`) and `conn_timing` global
  (`backend_startup.h:25`): the seam signatures carry exactly these fields.

## Seam audit

This unit declares **no inward seams**; `init_seams()` is empty and is called
by `seams-init::init_all()` (uniform convention; nothing to install). No
`set()` calls appear anywhere in this crate or in `seams-init`.

Outward seams (all owners unported, so a direct cargo dependency is
impossible — declaration-only owner seam crates are the only option). Each was
read in full; every one is a bare `seam_core::seam!` declaration with no
logic, branching, or computation:

| Seam crate | Declarations used | C symbol(s) | Thin? |
|---|---|---|---|
| backend-postmaster-fork-process-seams | `fork_process` | `fork_process()` | yes |
| backend-utils-adt-timestamp-seams | `get_current_timestamp` | `GetCurrentTimestamp()` | yes |
| backend-postmaster-postmaster-seams | `close_postmaster_ports` | `ClosePostmasterPorts()` | yes |
| backend-utils-init-miscinit-seams | `init_postmaster_child` | `InitPostmasterChild()` | yes |
| backend-storage-ipc-dsm-core-seams | `dsm_detach_all` | `dsm_detach_all()` | yes |
| backend-port-sysv-shmem-seams | `pg_shared_memory_detach` | `PGSharedMemoryDetach()` | yes |
| backend-utils-mmgr-mcxt-seams | `switch_to_top_memory_context` | `MemoryContextSwitchTo(TopMemoryContext)` | yes |
| backend-utils-init-small-seams | `is_postmaster_environment`, `is_under_postmaster`, `set_my_pm_child_slot`, `set_my_client_socket` | globals.c globals | yes |
| backend-tcop-backend-startup-seams | `backend_main`, `set_backend_startup_data_fork_started`, `backend_startup_data_timings`, `set_conn_timing_child` | `BackendMain`, `BackendStartupData` blob accessors, `conn_timing` | yes |
| backend-postmaster-{autovacuum,bgworker,bgwriter,checkpointer,pgarch,startup,syslogger,walsummarizer,walwriter}-seams, backend-replication-{logical-slotsync,walreceiver}-seams, backend-storage-aio-methods-seams | one `*_main(&[u8]) -> !` each | the corresponding `*Main` entry points | yes |

No findings. Each owner seam crate is named for and documents the unit that
owns the function, so installation responsibility is unambiguous.

## Verification

- `cargo build --workspace` and `cargo test --workspace`: clean, all tests
  pass (5 tests in this crate cover table length/names/shmem_attach/NULL
  slots and the `IsExternalConnectionBackend` predicate).
- Spot re-derivation: `postmaster_child_launch` and the full 18-entry table
  were re-derived line-by-line from the C and c2rust before signing off.

## Verdict

**PASS** — every in-build function is MATCH (with all call-outs SEAMED per
the rules above); zero seam findings.
