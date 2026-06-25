# Audit: backend-replication-libpqwalreceiver

C source: `src/backend/replication/libpqwalreceiver/libpqwalreceiver.c` (1261 LOC).
Port crate: `crates/backend-replication-libpqwalreceiver`.
Outward seam crate (new): `crates/interfaces-libpq-fe-seams` (libpq client +
backend-leaf surface, panic-until-bound — no in-process libpq client exists).
Signature types (new): `crates/types-libpqwalreceiver`.
Inward seams (pre-existing): `crates/backend-replication-libpqwalreceiver-seams`.

## Function inventory

| C fn (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `_PG_init` (124) | lib.rs `_PG_init` | MATCH | double-load → `elog(ERROR)` "already loaded"; AtomicBool stands in for the `WalReceiverFunctions != NULL` global. |
| `libpqrcv_connect` (148) | lib.rs `libpqrcv_connect` | MATCH | key/val assembly, logical-encoding+options, must_use_password ereport (2F003), search-path clear, both bad_connection paths → `ConnectResult{None,err}`. |
| `libpqrcv_check_conninfo` (293) | lib.rs `libpqrcv_check_conninfo` | MATCH | parse-fail → SYNTAX_ERROR; password scan → 2F003. |
| `libpqrcv_get_conninfo` (348) | lib.rs `libpqrcv_get_conninfo` | MATCH | OOM ereport; skip `D`/empty/null; `*`→`********`; String is never "broken" so retval always Some. |
| `libpqrcv_get_senderinfo` (397) | lib.rs `libpqrcv_get_senderinfo` | MATCH | host/port out-params as tuple; `atoi` C-semantics helper. |
| `libpqrcv_identify_system` (421) | lib.rs `libpqrcv_identify_system` | MATCH | TUPLES_OK check, `<3 fields || !=1 row` PROTOCOL_VIOLATION, sysid + pg_strtoint32 tli. |
| `libpqrcv_server_version` (470) | lib.rs `libpqrcv_server_version` | MATCH | passthrough. |
| `libpqrcv_get_dbname_from_conninfo` (481) | lib.rs same | MATCH | delegates to get_option("dbname"). |
| `libpqrcv_get_option_from_conninfo` (493) | lib.rs same | MATCH | last-match-wins over parsed opts. |
| `libpqrcv_startstreaming` (542) | lib.rs `libpqrcv_startstreaming` | MATCH | full physical + logical command builder; version gates (150000/160000/140000); pubnames escape OOM paths; COPY_BOTH→true, COMMAND_OK→false, else PROTOCOL_VIOLATION. |
| `libpqrcv_endstreaming` (648) | lib.rs `libpqrcv_endstreaming` | MATCH | copy-end+flush failure CONNECTION_FAILURE; TUPLES_OK reads next_tli; COPY_OUT endcopy; CommandComplete check; extra-result PROTOCOL_VIOLATION. |
| `libpqrcv_readtimelinehistoryfile` (729) | lib.rs same | MATCH | `!=2 fields||!=1 row` PROTOCOL_VIOLATION; content truncated to PQgetlength. |
| `libpqrcv_disconnect` (778) | lib.rs `libpqrcv_disconnect` | MATCH | disconnect + drop owned recvBuf/conn. |
| `libpqrcv_receive` (802) | lib.rs `libpqrcv_receive` | MATCH | 0→consume_input→retry→wait_fd; -1 end-of-COPY (COMMAND_OK / COPY_IN / orderly-close CONNECTION_BAD→-1 / else PROTOCOL_VIOLATION); `<-1` PROTOCOL_VIOLATION; len>0 in recvBuf. |
| `libpqrcv_send` (894) | lib.rs `libpqrcv_send` | MATCH | put_copy_data<=0 \|\| flush → CONNECTION_FAILURE. |
| `libpqrcv_create_slot` (910) | lib.rs `libpqrcv_create_slot` | MATCH | new(>=150000)/old syntax; TWO_PHASE/FAILOVER/SNAPSHOT arms; PHYSICAL RESERVE_WAL; TUPLES_OK check; lsn via pg_lsn_in(field 1) when want_lsn; snapshot field 2. |
| `libpqrcv_alter_slot` (1025) | lib.rs `libpqrcv_alter_slot` | MATCH | quote_identifier; FAILOVER/TWO_PHASE optional + separator; COMMAND_OK check. |
| `libpqrcv_get_backend_pid` (1065) | lib.rs same | MATCH | passthrough. |
| `libpqrcv_processTuples` (1074) | lib.rs `libpqrcv_processTuples` | MATCH | nfields!=nRetTypes PROTOCOL_VIOLATION; tuplestore+tupledesc build; per-row temp-context switch/reset, CHECK_FOR_INTERRUPTS, BuildTupleFromCStrings, puttuple; delete context. |
| `libpqrcv_exec` (1150) | lib.rs `libpqrcv_exec` | MATCH | MyDatabaseId==Invalid → OBJECT_NOT_IN_PREREQUISITE_STATE; full status switch incl SINGLE_TUPLE/TUPLES_CHUNK→processTuples, COPY_*, COMMAND_OK, EMPTY_QUERY/PIPELINE_* err strings, *_ERROR/BAD_RESPONSE → err+MAKE_SQLSTATE. |
| `stringlist_to_identifierstr` (1232) | lib.rs same | MATCH | comma-join with PQescapeIdentifier; None on escape failure. |
| `PQWalReceiverFunctions` table (98) | walrcv_table.rs `init_seams` | MATCH | the vtable installation; entries are the inward seams. |

21 C functions + the vtable table — all MATCH. No MISSING / PARTIAL / DIVERGES.

## Seam audit

Outward (`interfaces-libpq-fe-seams`): every call is the libpq client API
(`PQ*`/`libpqsrv_*`) or a backend leaf (encoding name, pg_lsn_in,
quote_identifier, pg_strtoint32, MyDatabaseId, work_mem, CHECK_FOR_INTERRUPTS,
tuplestore/tupledesc/memctx, slot getattr). There is no in-process libpq client
crate in the tree, so this surface is genuinely external and panic-until-bound
(sanctioned by the task). All seam bodies in walrcv_table.rs are thin
marshal+delegate: resolve handle → one libpqrcv_* call → convert result. No
branching/computation lives in a seam path.

Inward (`backend-replication-libpqwalreceiver-seams`): 25 declarations, all 25
installed by `walrcv_table::init_seams()` (verified count), which contains only
`set()` calls; `crate::init_seams()` delegates to it; `seams-init::init_all()`
calls `backend_replication_libpqwalreceiver::init_seams()`. Consumers
(walreceiver / slotsync / slotfuncs) resolved. The result-iteration seams
(`make_result_tupslot`/`result_gettupleslot`/`getattr_*`/`exec_clear_tuple`/
`walrcv_clear_result`) are libpqwalreceiver-owned per the repo model (it owns the
WalRcvExecResult registry); their bodies marshal to the tuplestore/slot outward
seams.

`libpqrcv_alter_slot`/`libpqrcv_server_version`/`libpqrcv_check_conninfo` are
fully ported as pub functions but have no inward seam (no current repo consumer
needs the seam) — not a finding (logic is present and callable).

## Design conformance

- Opacity inherited, not invented: PgConnId/PgResultId/WalReceiverConn handles
  mirror the C `PGconn*`/`PGresult*`/`WalReceiverConn*` pointers and the
  pre-existing `types_walreceiver` usize-handle model; no new stand-in for a
  typed value.
- The conn/result/tupslot registries are the C backend-local `palloc`'d objects
  (single-threaded per backend), not a side table papering over a typed value —
  the owned object genuinely cannot cross a `seam!` fn-pointer boundary by value
  and stay addressable across the receive/getattr loop. `static mut` +
  single-thread invariant matches the C memory model (mirrors the slotsync /
  pgoutput file-scope pattern already in tree).
- ereport(ERROR) sites all carry the C SQLSTATE + ERROR severity; `unreachable!()`
  only follows a `.finish(...)?` that has already returned `Err` (the C
  `ereport(ERROR)` never-returns idiom).
- No allocating zero-arg getter seams for foreign globals beyond the C ones
  (work_mem/MyDatabaseId mirror the C macros, parameterless in C too).

## Gate

- `cargo check --workspace`: clean.
- `cargo test -p backend-replication-libpqwalreceiver`: 38 passed.
- `cargo test -p seams-init --no-run`: builds.
- `cargo test -p no-todo-guard`: pass (no todo!/unimplemented!).

## Verdict: PASS
