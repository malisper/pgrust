# Audit: backend-utils-activity-status

Unit: `src/backend/utils/activity/backend_status.c` (PostgreSQL 18.3, 1348 LOC).
Crate: `crates/backend-utils-activity-status`.
Build config audited against: `USE_SSL` defined, `ENABLE_GSS` NOT defined
(matches the repo's trimmed `types_net::Port`, which carries no GSS state).

Verdict: **PASS** (2026-06-16, Opus 4.8 1M). All 25 functions MATCH; zero seam
findings after fixes.

## Function table

| # | C function | C loc | Port loc | Verdict | Notes |
|---|------------|-------|----------|---------|-------|
| 1 | BackendStatusShmemSize | :81 | lib.rs BackendStatusShmemSize | MATCH | SSL arm included, GSS arm omitted (build config). mul/add_size order identical. |
| 2 | BackendStatusShmemInit | :114 | lib.rs BackendStatusShmemInit | MATCH | array+appname+hostname+activity+ssl buffers; `!found` MemSet + pointer init loops identical; GSS arm omitted. |
| 3 | pgstat_beinit | :245 | lib.rs pgstat_beinit | MATCH | asserts, MyBEEntry = &array[MyProcNumber], on_shmem_exit(hook, 0). |
| 4 | pgstat_bestart_initial | :270 | lib.rs pgstat_bestart_initial | MATCH | C memcpy-local-then-back; port writes fields directly into the entry inside the crit section (only field C reads back is st_changecount, which the protocol bumps — observably identical). All scalar fields + out-of-line string init + SSL-status zero identical. |
| 5 | pgstat_bestart_security | :385 | lib.rs pgstat_bestart_security | MATCH | ssl_bits/version/cipher + peer subject/serial/issuer all filled; peer accessors seam-and-panic (be-secure unported). GSS arm omitted (build). |
| 6 | pgstat_bestart_final | :467 | lib.rs pgstat_bestart_final | MATCH | userid for B_BACKEND/WAL_SENDER/BG_WORKER else Invalid; state=UNDEFINED; pgstat_tracks_backend_bktype/create_backend (seam-and-panic, pgstat_backend unported); application_name report. |
| 7 | pgstat_beshutdown_hook | :509 | lib.rs pgstat_beshutdown_hook | MATCH | st_procpid=0 under protocol; MyBEEntry=NULL. |
| 8 | pgstat_clear_backend_activity_snapshot | :536 | lib.rs pgstat_clear_backend_activity_snapshot | MATCH | drops owned snapshot Vec (= C MemoryContextDelete + reset NULL/0; no separate context needed in owned model). |
| 9 | pgstat_setup_backend_status_context | :551 | — | MATCH (n/a) | C creates the snapshot MemoryContext; the owned-Vec model has no separate context (the Vec IS the allocation, created in read_current_status, dropped in clear). No behavioral analog; not a deferral. |
| 10 | pgstat_report_activity | :572 | lib.rs pgstat_report_activity | MATCH | disabled-state final update incl. `proc->wait_event_info = 0` (pgstat_report_wait_end seam, installed); conn active/idle time accounting; STATE_RUNNING resets query/plan id; activity memcpy+NUL. |
| 11 | pgstat_report_query_id | :686 | lib.rs pgstat_report_query_id | MATCH | null/track guards; top-level-only force guard. |
| 12 | pgstat_report_plan_id | :725 | lib.rs pgstat_report_plan_id | MATCH | same shape as query_id. |
| 13 | pgstat_report_appname | :764 | lib.rs pgstat_report_appname | MATCH | pg_mbcliplen(.., NAMEDATALEN-1) + memcpy + NUL. |
| 14 | pgstat_report_xact_timestamp | :793 | lib.rs pgstat_report_xact_timestamp | MATCH | track/null guard; st_xact_start_timestamp write. |
| 15 | pgstat_read_current_status | :820 | lib.rs pgstat_read_current_status | MATCH | per-slot changecount retry; copy scalar fields + snapshot strings into owned Vecs (= C re-point to local buffers); only valid (procpid>0) entries appended; ProcNumberGetTransactionIds annotation; table in proc_number order. CHECK_FOR_INTERRUPTS elided in retry loop (changecount.rs precedent — interrupts not driven; loop retries). |
| 16 | pgstat_get_backend_current_activity | :996 | lib.rs pgstat_get_backend_current_activity | MATCH | 1..=MaxBackends scan w/ changecount retry; superuser()/GetUserId() permission gate; insufficient-privilege / command-not-enabled / clip_activity branches; not-available fallthrough. |
| 17 | pgstat_get_crashed_backend_activity | :1074 | lib.rs pgstat_get_crashed_backend_activity | MATCH | null guards; activity pointer bounds check (buffer..buffer+size-qsize); empty-string guard; ascii_safe_strlcpy with Min(buflen, qsize). |
| 18 | pgstat_get_my_query_id | :1138 | lib.rs pgstat_get_my_query_id | MATCH | null->0 else st_query_id (no lock). |
| 19 | pgstat_get_my_plan_id | :1158 | lib.rs pgstat_get_my_plan_id | MATCH | null->0 else st_plan_id. |
| 20 | pgstat_get_backend_type_by_proc_number | :1181 | lib.rs pgstat_get_backend_type_by_proc_number | MATCH | direct array[procNumber].st_backendType, no changecount. |
| 21 | cmp_lbestatus | :1200 | lib.rs cmp_lbestatus | MATCH | proc_number1 - proc_number2. |
| 22 | pgstat_get_beentry_by_proc_number | :1223 | lib.rs pgstat_get_beentry_by_proc_number | MATCH | delegates to local_by_proc_number, returns .backend_status. |
| 23 | pgstat_get_local_beentry_by_proc_number | :1248 | lib.rs pgstat_get_local_beentry_by_proc_number | MATCH | read_current_status + bsearch over ordered table. |
| 24 | pgstat_get_local_beentry_by_index | :1279 | lib.rs pgstat_get_local_beentry_by_index | MATCH | read_current_status + 1-based bounds (idx<1 || idx>num -> None). |
| 25 | pgstat_fetch_stat_numbackends | :1299 | lib.rs pgstat_fetch_stat_numbackends | MATCH | read_current_status + localNumBackends (= table.len()). |
| 26 | pgstat_clip_activity | :1315 | lib.rs pgstat_clip_activity | MATCH | pnstrdup cap qsize-1 (stop at NUL), strlen, pg_mbcliplen, truncate. |

## Seam audit

Owned seam crate: `backend-utils-activity-status-seams` — all 16 declarations map
to a backend_status.c function and all 16 are installed by `init_seams()`
(`set()` calls only; the two GUC `install()` calls are this unit's own GUC
variable ownership, not foreign seams).

- `with_my_beentry` reconciliation: backend_progress.c writes only the four
  trimmed fields (changecount + progress) through the seam; the wrapper copies
  them out of the in-segment entry into the trimmed
  `types_pgstat::backend_status::PgBackendStatus` view, runs the consumer
  callback (which brackets with its own AtomicU32 changecount protocol), then
  copies them back. Sound: the entry is written only by this backend,
  synchronously within the callback. Thin marshal — no logic.
- `backend_current_activity -> String`: marshal of the byte-returning core via
  `from_utf8_lossy` (the deadlock-log seam contract is String, infallible).
- `pgstat_report_activity_idle/running(String)`: thin BackendState-discriminant
  adapters over `pgstat_report_activity`.

Outward seams, each a justified cross-crate hop, thin marshal + delegate:
- miscinit-seams: start/end_crit_section, get_session_user_id, superuser,
  get_user_id (real, installed elsewhere).
- xact-seams: get_current_statement_start_timestamp; timestamp-seams:
  get_current_timestamp, timestamp_difference; dsm-core-seams: on_shmem_exit;
  waitevent-seams: pgstat_report_wait_end (all real, installed).
- procarray-seams: `proc_number_get_transaction_ids` — NEW decl, INSTALLED in
  `backend-storage-ipc-procarray::visibility_lookup::init_seams` (the real
  ProcNumberGetTransactionIds owner).
- be-secure-seams: be_tls_get_version/cipher/cipher_bits (real) +
  be_tls_get_peer_subject_name/serial/issuer_name (NEW, seam-and-panic — the
  be-secure peer-cert accessors are unported).
- pgstat-database-seams (NEW): count_conn_active_time/txn_idle_time;
  pgstat-backend-seams (NEW): pgstat_create_backend/tracks_backend_bktype —
  both seam-and-panic (the pgstat producer files are unported). Correct
  panic-on-unported-callee.

Wiring cleanup: `set_conn_timing_auth_start`/`set_conn_timing_auth_end` were
misfiled into status-seams during scaffolding (conn_timing is backend_startup.c's
global, set by postinit.c — not a backend_status.c function). Relocated the two
decls to `backend-tcop-backend-startup-seams`, installed them in
`backend-tcop-backend-startup::init_seams` (routing to `globals::conn_timing::
set_auth_{start,end}`), and updated postinit's two call sites. status-seams now
has zero uninstalled declarations.

## Design conformance

- No type-alias stand-ins for typed pointers/enums (grep clean).
- `static AtomicPtr` file-statics mirror C shmem pointers (faithful-shmem parity,
  as the shmem crate itself does); GUC backing store is `thread_local` (per-backend
  GUC). No shared static for per-backend state.
- `unwrap_or`/`expect` audited: NUL-position fallbacks, no-MyProcPort default
  (= C `if (MyProcPort)`), `try_reserve().expect()` loud-OOM = C palloc-fail.
  `superuser(...).unwrap_or(false)` documented: forced by the no-Mcx/no-Result
  `backend_current_activity` seam contract; conservative redaction is the safe
  default for the permission gate.
- No `for now`/`simplified`/`hack`/`TODO`/`FIXME` markers.

Gate: `cargo check --workspace` green; `cargo test -p backend-utils-activity-status`
6 passed; `cargo test -p seams-init` 2 passed; no-todo-guard clean.
