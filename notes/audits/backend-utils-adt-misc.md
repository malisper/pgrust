# Audit: `backend-utils-adt-misc`

## VERDICT: **PASS** (self-audit; `/audit-crate` still the merge gate)

Port of `src/backend/utils/adt/misc.c` (PG 18.3) — the miscellaneous
SQL-callable utility functions. Every C function is present with its full logic;
verified function-by-function against the C source. (Note: the signal-sending
`pg_terminate_backend`/`pg_cancel_backend` family lives in `signalfuncs.c` in PG
18, NOT misc.c, so it is out of scope here — the prompt's mention of them does
not match the misc.c manifest.)

No `todo!()`/`unimplemented!()` anywhere (no-todo-guard PASS). All cross-crate
calls are real owner / owner-seam calls; the panics that remain are loud
seam-and-panic into genuinely-unported substrate.

## Function-by-function

- `count_nulls` / `pg_num_nulls` / `pg_num_nonnulls` (misc.c:75-187): the
  variadic null-bitmap scan and the separate-args count are ported 1:1
  (`bitmask <<= 1; == 0x100` byte advance preserved). The `fcinfo`-shaped
  argument resolution (variadic? array bitmap? per-arg isnull?) is presented as
  the `CountNullsArgs` enum the fmgr shim fills. Tested (separate, variadic-null,
  no-bitmap, bitmap-scan).
- `current_database` (misc.c:194): `get_database_name(MyDatabaseId)` via
  `backend-commands-dbcommands` (direct dep); `MyDatabaseId` supplied as a
  parameter (the fmgr shim's job). The always-valid `namestrcpy` contract is
  reproduced (a missing name is an internal error).
- `current_query` (misc.c:211): `debug_query_string` supplied by the shim; the
  unset → SQL NULL branch is 1:1.
- `pg_tablespace_databases` / `pg_tablespace_location` (misc.c:223/300): the
  directory walk / `lstat`+`readlink` are misc.c's own SRF data-production into
  fd.c substrate → seamed via `backend-utils-adt-misc-seams` (panic until fd.c
  lands). The empty-tuplestore cases map to empty Vec / `Ok(None)`.
- `pg_sleep` (misc.c:369): the sleep loop, `GetNowFloat` arithmetic, the
  `>=600 / >0 / break` delay branch, `ceil(delay*1000)` are ported 1:1. The
  clock (`get_current_timestamp`), `CHECK_FOR_INTERRUPTS` (miscinit-seams) and
  `WaitLatch(MyLatch,…)`+`ResetLatch(MyLatch)` (latch-seams
  `wait_latch_my_latch`/`reset_latch_my_latch`) are real seams. Added
  `WAIT_EVENT_PG_SLEEP = PG_WAIT_TIMEOUT | 2` to types-pgstat (alphabetical index
  2 of the WaitEventTimeout section, consistent with the existing
  `REGISTER_SYNC_REQUEST | 5`).
- `pg_get_keywords` (misc.c:417): renders the real `common-keywords`
  `ScanKeywords`/`ScanKeywordCategories`/`ScanKeywordBareLabel` table (direct
  dep, pure data) — the full `switch` over category (U/C/T/R + descriptions) and
  the bare-label flag/description are 1:1. Returns `Vec<KeywordRow>`; the SRF
  tuplestore assembly is the deferred fmgr boundary. Tested against kwlist.h
  (select=R/bare, abort=U/bare).
- `pg_get_catalog_foreign_keys` (misc.c:495): the generated `sys_fk_relationships[]`
  table (`system_fk_info.h`) and `array_in` fmgr dispatch are unported → whole
  row set seamed via `backend-utils-adt-misc-seams`.
- `pg_typeof` (misc.c:563): identity over the resolved arg-type OID
  (`get_fn_expr_argtype`, fmgr shim).
- `pg_basetype` (misc.c:582): the domain-stack walk loop is 1:1; the per-step
  `SearchSysCache1(TYPEOID)` projection is a caller-supplied closure (the fmgr
  shim/syscache supplies it), bogus-OID → `None`. Tested (2-deep domain stack +
  base + bogus).
- `pg_collation_for` (misc.c:618): branch order + DATATYPE_MISMATCH message text
  1:1. `type_is_collatable` (lsyscache-seams), `format_type_be`
  (format-type-seams), `generate_collation_name` (ruleutils-seams). The arg-type
  and collation OIDs are shim-supplied parameters.
- `pg_relation_is_updatable` / `pg_column_is_updatable` (misc.c:647/664): the
  system-column short-circuit, the `col = attnum - FirstLowInvalidHeapAttribute
  Number` mapping, and `REQ_EVENTS = (1<<CMD_UPDATE)|(1<<CMD_DELETE) = 0x14` test
  are 1:1; `relation_is_updatable` via rewriteHandler-seams. Tested (system-col
  short-circuit; REQ_EVENTS constant).
- `pg_input_is_valid` / `pg_input_error_info` / `pg_input_is_valid_common`
  (misc.c:688-820): the soft-error plumbing, the `details_wanted` enable, the
  exact C field order of message/detail/hint/`unpack_sql_state(sqlerrcode)` are
  1:1. `parseTypeString` (parse-type-seams, hard-error leg soft=false) +
  `getTypeInputInfo`+`InputFunctionCallSafe` (new `input_is_valid_by_type` fmgr
  seam). `unpack_sql_state` (PGSIXBIT decode) ported in-crate; tested.
- `is_ident_start` / `is_ident_cont` / `scanner_isspace` / `parse_ident`
  (misc.c:827-992): the entire scanner — the quoted-identifier `""`-unescape
  `memmove`→`copy_within`+`pop` loop, the unquoted scan + `downcase_identifier`
  (backend-parser-small1 direct dep, truncate=false length-preserving asserted),
  the `missing_ident`/`after_dot` error selection with exact errdetail text, the
  whitespace skipping and dot/end/strict-trailing handling — ported 1:1. Returns
  the parsed parts; the `accumArrayResult`/`makeArrayResult` text[] assembly is
  the deferred fmgr boundary. Tested (qualified, quoted case-preserve+unescape,
  multiple doubled quotes, whitespace, all 5 error paths, non-strict trailing).
- `pg_current_logfile` (+`_1arg`) (misc.c:999/1091): the format validation
  (stderr/csvlog/jsonlog) with its INVALID_PARAMETER_VALUE message+hint is 1:1
  in-crate; the `current_logfiles` scan is seamed. Tested (bad-format rejection).
- `pg_get_replica_identity_index` (misc.c:1100): `RelationGetReplicaIndex`
  (relcache direct dep, which does the table_open/close internally); the
  `OidIsValid` → NULL branch is 1:1.
- `any_value_transfn` (misc.c:1120): identity over the state datum.

## Seam homing / guard

- This unit owns NO inward seams (its functions are SQL-callable leaves); there
  is no `init_seams()` to wire and none is required.
- `backend-utils-adt-misc-seams` holds the four SRF data-production seams whose
  substrate (fd.c walk, generated `system_fk_info` table + `array_in`) is
  unported. They are exempt from the declared-seam-installed guard while the
  misc owner is `ported` (not `merged`); when it merges, install or move to
  `CONTRACT_RECONCILE_PENDING` once the substrate lands.
- New outward seams added to existing owner-seam crates whose owner dir is
  absent (hence guard-exempt): `relation_is_updatable`
  (rewriteHandler-seams), `generate_collation_name` (ruleutils-seams),
  `input_is_valid_by_type` (fmgr-seams).
- No `CONTRACT_RECONCILE_PENDING` entries added.

## Deviations / deferred (all sanctioned)

- Bare-`PGFunction`/`fcinfo` shim (argument fetch, `get_fn_expr_argtype`,
  `PG_GET_COLLATION`, SRF tuplestore + array assembly) is the deferred fmgr
  boundary per the task; functions that need a resolved arg type / collation /
  db-id / debug_query_string take them as parameters exactly as the shim will
  supply them.
- Result row types (`KeywordRow`/`CatalogForeignKeyRow`/`InputErrorInfo`) use
  `std::Vec`/`String` for the values that escape to the SRF tuplestore (the
  `pstrdup`/`heap_form_tuple`-out analog), matching the data-producing contract;
  query-lifecycle text returns use `PgVec<'mcx, u8>`.

## Gate

`cargo check --workspace` clean (0 errors). `cargo test -p backend-utils-adt-misc`
19/19 pass. no-todo-guard + seams-init guards PASS. `cargo test --workspace`
green modulo the sanctioned flakes (`range_pair_*`, gram-core LALR).
