# Audit: backend-commands-copyto (`src/backend/commands/copyto.c`)

- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Unit:** backend-commands-copyto
- **Branch:** port/backend-commands-copyto
- **c_sources:** `src/backend/commands/copyto.c`
- **Port:** `crates/backend-commands-copyto`
- **c2rust:** `../pgrust/c2rust-runs/backend-commands-copyto/src/copyto.rs`

Independent, from-scratch function-by-function audit per
`.claude/skills/audit-crate/SKILL.md`. Re-derived from the C source, the c2rust
rendering, and the Rust port; the port's self-review, the catalog AUDIT-FIX
note, and the prior PASS/FAIL verdicts were not trusted. This re-audit also
confirms resolution of the three findings from the prior FAIL (commit
`302c7cbe`).

## Top-line verdict: **PASS**

All 34 copyto.c functions are present with logic matching the C exactly
(or `SEAMED` per step 3's thin-marshal rule). The three prior findings
(L1 / S2 / S3) are resolved. Zero outstanding seam or design findings.
`cargo test -p backend-commands-copyto` green (3 tests).

## Function inventory and verdicts

The C file has 34 function definitions (the `CopyToGetRoutine` helper, the
format routines, the low-level send family, the public drivers, the macro
`DUMPSOFAR`, and the four `DestReceiver` callbacks) plus 3
`static const CopyToRoutine` method tables (data, modeled as the
`CopyToRoutineKind` dispatch) and the `BinarySignature` constant.

| # | C function (copyto.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `CopyToGetRoutine` (177) | `copy_to_get_routine` (218) | MATCH | csv → binary → text precedence exact; unit-tested. |
| 2 | `CopyToTextLikeStart` (190) | `copy_to_text_like_start` (234) | MATCH | need_transcoding null_print conversion; header loop with hdr_delim; CSV vs text colname escape; end-of-row. |
| 3 | `CopyToTextLikeOutFunc` (233) | `copy_to_text_like_out_func` (280) | SEAMED | `getTypeOutputInfo` (lsyscache seam) + `fmgr_info` lookup-check (fmgr seam); returns `FmgrInfo{fn_oid}`. Thin. |
| 4 | `CopyToTextOneRow` (245) | dispatch `routine_one_row` Text→`copy_to_text_like_one_row(.,false)` (406) | MATCH | Forwarding wrapper. |
| 5 | `CopyToCSVOneRow` (252) | dispatch Csv→`copy_to_text_like_one_row(.,true)` (407) | MATCH | Forwarding wrapper. |
| 6 | `CopyToTextLikeOneRow` (264) | `copy_to_text_like_one_row` (299) | MATCH | need_delim, null_print_client on isnull, OutputFunctionCall (seam), csv force_quote_flags vs text. |
| 7 | `CopyToTextLikeEnd` (304) | dispatch `routine_end` Text/Csv→`Ok(())` (416) | MATCH | C no-op. |
| 8 | `CopyToBinaryStart` (314) | `copy_to_binary_start` (337) | MATCH | signature (11 bytes), two int32 zeros. |
| 9 | `CopyToBinaryOutFunc` (333) | `copy_to_binary_out_func` (289) | SEAMED | `getTypeBinaryOutputInfo` + fmgr lookup-check. Thin. |
| 10 | `CopyToBinaryOneRow` (345) | `copy_to_binary_one_row` (349) | MATCH | int16 natts, -1 for isnull, SendFunctionCall (seam returns header-stripped payload, so `len()` = VARSIZE-VARHDRSZ), int32 len + data. |
| 11 | `CopyToBinaryEnd` (378) | `copy_to_binary_end` (377) | MATCH | int16 -1 trailer + flush. |
| 12 | `SendCopyBegin` (391) | `send_copy_begin` (426) | MATCH | pqformat message assembly (direct dep); sets COPY_FRONTEND; format/per-col-format loop exact; PqMsg_CopyOutResponse = 'H'. |
| 13 | `SendCopyEnd` (408) | `send_copy_end` (442) | MATCH | Assert len==0 → `debug_assert!`; CopyDone empty message ('c'). |
| 14 | `CopySendData` (427) | `copy_send_data` (451) | MATCH | appendBinaryStringInfo via try_reserve+extend. |
| 15 | `CopySendString` (433) | `copy_send_string` (456) | MATCH | strlen modeled by slice length (caller passes bytes). |
| 16 | `CopySendChar` (439) | `copy_send_char` (461) | MATCH | single-byte append. |
| 17 | `CopySendEndOfRow` (445) | `copy_send_end_of_row` (482) | MATCH | **Finding S2 resolved.** COPY_FILE write is the bare `fd_s::copy_write_file(stream, buf) -> Option<errno>` primitive; the EPIPE/is_program decision, `close_pipe_to_program` re-raise, and the "could not write to COPY program/file: %m" message selection are now in this crate (lib.rs:484-525), matching copyto.c:451-483. Frontend/Callback, bytes_processed, progress update, reset exact. |
| 18 | `CopySendTextLikeEndOfRow` (506) | `copy_send_text_like_end_of_row` (553) | MATCH | non-WIN32 build: '\n' for FILE and FRONTEND, default no-op; then end-of-row. WIN32 `\r\n` branch excluded by build config (documented). |
| 19 | `CopySendInt32` (538) | `copy_send_int32` (568) | MATCH | `pg_hton32` = `to_be_bytes`. |
| 20 | `CopySendInt16` (550) | `copy_send_int16` (574) | MATCH | `pg_hton16` = `to_be_bytes`. |
| 21 | `ClosePipeToProgram` (562) | `close_pipe_to_program` (580) | SEAMED | pclose_rc handling (-1 / nonzero with wait_result_to_str) in fd seam `close_pipe_to_program`; Assert is_program preserved as `debug_assert!`. Thin. |
| 22 | `EndCopy` (587) | `end_copy` (594) | MATCH | is_program→close pipe; else filename→FreeFile (seam); progress_end; context delete = owned drop. |
| 23 | `BeginCopyTo` (623) | `BeginCopyTo` (622) | MATCH | relkind gate (6 branches; SQLSTATEs WRONG_OBJECT_TYPE / FEATURE_NOT_SUPPORTED + hints verified), ProcessCopyOptions seam, routine select, rel vs query branch (analyze/rewrite, NIL / >1 rule errors, SELECT-INTO + utility gate, RETURNING gate, plan, queryRelId double-check via `plan.relationOids`, PushCopiedSnapshot+UpdateActiveSnapshotCommandId, receiver create, CreateQueryDesc+ExecutorStart), CopyGetAttnums, FORCE_QUOTE flags + INVALID_COLUMN_REFERENCE, encoding setup, dest selection (callback/pipe/program/file with INVALID_NAME relative-path gate), progress init. |
| 24 | `EndCopyTo` (1005) | `EndCopyTo` (938) | MATCH | queryDesc!=NULL → end_copy_query seam (ExecutorFinish/End/FreeQueryDesc) + PopActiveSnapshot; then EndCopy. |
| 25 | `DoCopyTo` (1026) | `DoCopyTo` (952) | MATCH | **Finding L1 resolved.** Query branch returns `cstate.receiver_processed` (= C `((DR_copy*)dest)->processed`, lib.rs:1039); `executor_run_copy` seam returns `()`. Scan branch: **Finding S3 resolved** — `table_beginscan` takes the active snapshot explicitly (`get_active_snapshot` seam, lib.rs:999-1001), not ambiently. fe_copy, tupDesc, out_functions lookup, scan loop, CHECK_FOR_INTERRUPTS, per-tuple progress, start/end all MATCH. |
| 26 | `CopyOneRowTo` (1122) | `copy_one_row_to` (1055) | MATCH | rowcontext reset modeled by per-row owned drop (documented); slot_getallattrs (seam) → routine dispatch. |
| 27 | `CopyAttributeOutText` (1147) | `copy_attribute_out_text` (1110) | MATCH | both encoding loops (embeds_ascii / safe), DUMPSOFAR, control-char escape table (b/f/n/r/t/v), delimiter backslash, mblen advance. Byte-exact. |
| 28 | `CopyAttributeOutCSV` (1300) | `copy_attribute_out_csv` (1217) | MATCH | null_print pre-conversion match, single_attr "\\." quote, needs-quoting scan, quote/escape inner loop (no ptr advance on escape, mblen advance), as-is dump. Byte-exact. |
| 29 | `copy_dest_startup` (1389) | (none) | MATCH (no-op) | C body is `/* no-op */`; receiver-handle model has no startup callback. Zero logic. |
| 30 | `copy_dest_receive` (1398) | `copy_dest_receive` (1069) | MATCH | inward seam impl; reaches live cstate via the thread_local registry alias (= C `((DR_copy*)self)->cstate`); CopyOneRowTo + receiver_processed++ + progress. |
| 31 | `copy_dest_shutdown` (1417) | (none) | MATCH (no-op) | C body is `/* no-op */`. Zero logic. |
| 32 | `copy_dest_destroy` (1426) | (none) | MATCH (no-op) | C body is `pfree(self)`; registry slot reclaimed by backend lifetime. Behaviorally inert. |
| 33 | `CreateCopyDestReceiver` (1435) | `CreateCopyDestReceiver` (1101) | MATCH | builds a receiver handle (the C vtable fields are the no-op callbacks above; DestCopyOut implicit; cstate set later via bind). |
| 34 | `DUMPSOFAR()` macro (1140) | `dumpsofar` (1209) | MATCH | flush literal run `[start, ptr)` only when `ptr > start`. |

## Confirmation of the prior FAIL findings (commit `302c7cbe`)

- **L1 (was DIVERGES) — RESOLVED.** `DoCopyTo`'s COPY-(query)-TO branch returns
  `cstate.receiver_processed` (lib.rs:1039), the per-tuple counter that
  `copy_dest_receive` bumps — exactly C's
  `((DR_copy*)cstate->queryDesc->dest)->processed`. The `execMain` seam
  `executor_run_copy(exec_token) -> PgResult<()>` no longer returns a count, so
  the executor's `es_processed` can never be mistaken for the receiver count.
- **S2 (was seam finding) — RESOLVED.** `fd_s::copy_write_file(stream, buf) ->
  PgResult<Option<i32>>` is the bare `fwrite`/`ferror` + errno primitive. The
  EPIPE→`ClosePipeToProgram`→re-raise path and the COPY program/file message
  selection are copyto's own control flow in `copy_send_end_of_row`
  (lib.rs:484-525), matching copyto.c:451-483.
- **S3 (was design finding) — RESOLVED.** `tableam_s::table_beginscan(relation,
  snapshot: Rc<SnapshotData>)` now takes the active snapshot as an explicit
  argument. `DoCopyTo` fetches `GetActiveSnapshot()` via the snapmgr seam
  (`get_active_snapshot`, lib.rs:999) and passes it across — no ambient global
  read inside the AM.

## Seam audit

Ownership is by C-source coverage. This unit's only `c_source` is `copyto.c`,
so the single owned seam crate is `crates/backend-commands-copyto-seams`.

- **Owned seam crate** `backend-commands-copyto-seams`: one declaration,
  `copy_dest_receive` (the inward COPY-OUT receiver re-entry). It is installed
  by this crate's `init_seams()` (lib.rs:1377-1379), which contains nothing but
  the single `set()` call, and `seams-init::init_all()` calls
  `backend_commands_copyto::init_seams()` (seams-init/src/lib.rs:17). No
  uninstalled declaration; no `set()` outside the owner. Clean.

- **Outward seams consumed** (each justified by a real dependency cycle /
  not-yet-ported owner, each a thin marshal+delegate):
  copy-seams (`process_copy_options`, `copy_get_attnums`); parser-analyze-seams
  (`pg_analyze_and_rewrite_fixedparams`); planner-seams (`pg_plan_query`);
  tableam-seams (`table_beginscan` now snapshot-explicit, `table_scan_getnextslot`,
  `table_endscan`); backend-progress-seams (`pgstat_progress_*`); fd-seams
  (`copy_write_file` bare primitive, `open_copy_to_file`, `open_pipe_stream_write`,
  `free_file`, `close_pipe_to_program`, `stdout_stream`); snapmgr-seams
  (`push_copied_active_snapshot`, `update_active_snapshot_command_id`,
  `get_active_snapshot`, `pop_active_snapshot`); execMain-seams
  (`create_query_desc_and_start`, `executor_run_copy` returns `()`,
  `end_copy_query`); execTuples-seams (`slot_getallattrs`,
  `exec_drop_single_tuple_table_slot`); fmgr-seams (`output_function_call`,
  `send_function_call` header-stripped, `fmgr_info_check`); lsyscache-seams
  (`get_type_output_info`, `get_type_binary_output_info`); mbutils-seams
  (`pg_server_to_any`, `pg_get_client_encoding`, `get_database_encoding`,
  `pg_encoding_mblen`, `pg_encoding_is_client_only`); pqcomm-seams
  (`pq_putmessage`); postgres-seams (`check_for_interrupts`); port-path-seams
  (`is_absolute_path`). Direct (non-seam) deps: pqformat (SendCopy message
  assembly), tableam (`table_slot_create`), utils-error (`where_to_send_output`,
  errno/sqlstate helpers).

  No outward seam carries branching, node construction, or computation beyond
  argument/result marshalling. (S2's prior in-seam decision logic is now back in
  this crate.)

## Design conformance

- Allocating functions/seams take `Mcx` and return `PgResult`. OK.
- No invented opacity: `CopyToStateData`, `CopyDest`, `CopyToRoutineKind` are
  real in-crate structs/enums; `CopyFormatOptions`/`CopyHeaderChoice` are real
  `types-copy` types; `QueryDesc`/`ParseState`/`RawStmt` are real
  `types-nodes::copy_query` types. The `ScanToken`/`exec_token`/receiver handles
  are AM/executor-owned opacities crossing seams the C also treats as opaque
  (`TableScanDesc *`, `QueryDesc *`, `DestReceiver *`). OK.
- The COPY-(query)-TO receiver `thread_local` registry models C's per-backend
  `DR_copy.cstate` pointer alias, bound only for the synchronous
  `executor_run_copy` window — per-backend state, not a shared static, carrying
  no logic. Slots are not freed (C `pfree`s the receiver in
  `copy_dest_destroy`); a bounded per-COPY-(query) resource note, not a
  behavioral divergence, matching C semantics for the synchronous run. OK.
- No locks held across `?`. No registry-shaped catalog side table. No unledgered
  divergence markers.

## Auditor spot-check (re-derived MATCH samples)

- `CopySendEndOfRow` error arm re-derived line-for-line against copyto.c:451-483:
  the program-non-EPIPE path still emits "could not write to COPY program: %m"
  (C's `errno==EPIPE` test only gates the `ClosePipeToProgram` call, not the
  message), and the port nests the EPIPE test inside `if is_program` with the
  program ereport firing unconditionally after — exact.
- `CopyAttributeOutCSV` escape inner loop: C does **not** advance `ptr` when
  emitting the escape char (the `if (c==quotec||c==escapec)` block has no
  `ptr++`; advancement happens in the trailing mblen/`ptr++`), and the port
  mirrors this (escape branch sets `start = i` without advancing). Exact.
- `CopyToBinaryOneRow` length field: the seam strips the varlena header, so
  `outputbytes.len()` = C's `VARSIZE(outputbytes) - VARHDRSZ`, sent as the int32
  length and as the data payload. Exact.
- `BINARY_SIGNATURE` = `"PGCOPY\n\377\r\n\0"` byte-for-byte (unit-tested
  `binary_signature_matches_c`).

## Verdict

**PASS.** Every function MATCH or SEAMED (thin). All three prior findings
resolved. Owned seam installed and wired. No design-conformance violations.
The `CATALOG.tsv` row may be set to `audited`.
