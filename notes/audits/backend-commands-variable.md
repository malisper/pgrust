# Audit: backend-commands-variable (commands/variable.c)

C source: `../pgrust/postgres-18.3/src/backend/commands/variable.c` (1259 lines).
Port: `crates/backend-commands-variable/src/lib.rs` (+ `-seams`).
Audit independent of the port; re-derived from C + c2rust.

## Architecture note

variable.c is the set of GUC `check_`/`assign_`/`show_` hooks the GUC framework
invokes by function pointer. In this repo the function pointers are the typed
install-once `GucSlot`s in `backend-utils-misc-guc-tables/src/{hooks,slots}.rs`.
The port installs every hook into its slot from `init_seams()` (wired into
`seams-init::init_all`). The slot fn-ptr contract (slots.rs) fixes the hook
signatures: check `fn(&mut <val>, &mut Option<GucHookExtra>, GucSource) ->
PgResult<bool>`; assign `fn(<val>, Option<&GucHookExtra>)` (infallible, C
`void`); show `fn() -> String`. `GucHookExtra = Box<dyn Any + Send>`.

Two forced adaptations vs the raw C, both behavior-preserving:
* **No ambient `Mcx`** in hook fn-ptrs. Hooks needing working memory
  (`SplitIdentifierString`, `pg_clean_ascii`, AUTHNAME lookup) create a
  transient `MemoryContext` and pass its `Mcx`; the result `String`/`Oid` is
  copied out before the context drops (C did the work in CurrentMemoryContext).
* **`Send` extra.** The backend-local `Rc<pg_tz>` is not `Send`, so it cannot
  travel through `GucHookExtra`. The timezone hooks carry the canonical zone
  *name* (`String`) in extra; the assign hook re-resolves through the cached
  `pg_tzset` (a hash hit on the just-validated name). Same installed `pg_tz`.

## Function inventory (36 functions, all present)

| # | C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | check_datestyle | 51 | lib.rs check_datestyle/_inner | MATCH | token loop, conflict detect, DEFAULT recursion via get_config_option_reset_string seam, canonical-string build all faithful; `void**extra` -> typed DateStyleExtra in Box |
| 2 | assign_datestyle | 243 | assign_datestyle | SEAMED | DateStyle/DateOrder are datetime.c globals (unported) -> assign_date_style seam, panics until datetime lands |
| 3 | check_timezone | 260 | check_timezone | MATCH | INTERVAL parse (quote strip, month/day reject), numeric-hours strtod-equiv, name+leap-second path; pg_tz carried as name (Send) |
| 4 | assign_timezone | 380 | assign_timezone | MATCH | re-resolve name -> set_session_timezone + ClearTimeZoneAbbrevCache seams |
| 5 | show_timezone | 391 | show_timezone | SEAMED | pg_get_timezone_name(session_timezone) via seam; "unknown" on empty |
| 6 | check_log_timezone | 417 | check_log_timezone | MATCH | name-only path, leap-second test |
| 7 | assign_log_timezone | 455 | assign_log_timezone | MATCH | re-resolve name -> set_log_timezone seam |
| 8 | show_log_timezone | 464 | show_log_timezone | SEAMED | pg_get_timezone_name(log_timezone) via seam |
| 9 | check_timezone_abbreviations | 486 | check_timezone_abbreviations | MATCH | NULL boot_val -> Ok(true) + debug_assert PGC_S_DEFAULT; load+install fused in load_and_install_tz_abbrevs seam (TimeZoneAbbrevTable not Send/value); failure -> Ok(false) |
| 10 | assign_timezone_abbreviations | 518 | assign_timezone_abbreviations | MATCH | no-op (install happens in check's seam); C `if(!extra) return` no-op preserved |
| 11 | check_transaction_read_only | 545 | check_transaction_read_only | MATCH | full decision tree; XactReadOnly/iso via xact-seams getters, FirstSnapshotSet via snapmgr seam, RecoveryInProgress, InitializingParallelWorker; errcodes match |
| 12 | check_transaction_isolation | 585 | check_transaction_isolation | MATCH | enum check (i32); serializable-in-recovery hint; errcodes match |
| 13 | check_transaction_deferrable | 623 | check_transaction_deferrable | MATCH | parallel-worker bypass; subxact/first-snapshot rejects |
| 14 | check_random_seed | 655 | check_random_seed | MATCH | armed = source >= PGC_S_INTERACTIVE in Box extra |
| 15 | assign_random_seed | 667 | assign_random_seed | MATCH* | fires setseed iff armed. *DIVERGENCE (documented, behavior-preserving): C writes the arm flag false back into the mutable extra to make a rollback re-assign a no-op; the slot hands assign a `&` extra (cannot mutate), so re-arming requires re-running check (which produces a fresh armed flag). A rollback restores the prior value via its own (unarmed-unless-interactive) extra, so seed is not re-applied on rollback — same observable behavior. |
| 16 | show_random_seed | 676 | show_random_seed | MATCH | "unavailable" |
| 17 | check_client_encoding | 687 | check_client_encoding | MATCH | pg_valid_client_encoding/pg_encoding_to_char/PrepareClientEncoding/GetDatabaseEncodingName; parallel-worker + transaction-state branches; UNICODE canonicalize-skip hack |
| 18 | assign_client_encoding | 785 | assign_client_encoding | MATCH | parallel-worker skip; SetClientEncoding+LOG fused in set_client_encoding_logging seam |
| 19 | check_session_authorization | 814 | check_session_authorization | MATCH | NULL boot_val; parallel-worker copy; AUTHNAME lookup -> (oid,rolsuper); superuser_arg(authenticated); PGC_S_TEST NOTICE soft-fail returns Ok(true) |
| 20 | assign_session_authorization | 911 | assign_session_authorization | SEAMED | NULL extra no-op; SetSessionAuthorization seam (miscinit) |
| 21 | check_role | 932 | check_role | MATCH | "none" -> InvalidOid hardwire; parallel-worker copy; member_can_set_role(session_user); PGC_S_TEST NOTICE soft-fail |
| 22 | assign_role | 1025 | assign_role | SEAMED | SetCurrentRoleId seam (miscinit) |
| 23 | show_role | 1033 | show_role | MATCH | !OidIsValid(GetCurrentRoleId) -> "none"; else role_string GUC var (None -> "none") |
| 24 | check_canonical_path | 1058 | check_canonical_path | MATCH | NULL-safe; canonicalize_path seam (common-path) in place |
| 25 | check_application_name | 1079 | check_application_name | MATCH | pg_clean_ascii(MCXT_ALLOC_NO_OOM=0x02); Err -> Ok(false) |
| 26 | assign_application_name | 1107 | assign_application_name | SEAMED | pgstat_report_appname seam |
| 27 | check_cluster_name | 1117 | check_cluster_name | MATCH | shares clean_ascii_name helper |
| 28 | assign_maintenance_io_concurrency | 1145 | assign_maintenance_io_concurrency | MATCH | global write done by GUC framework; hook does `if AmStartupProcess() XLogPrefetchReconfigure()` via am_startup_process + xlog_prefetch_reconfigure seams |
| 29 | assign_io_max_combine_limit | 1163 | assign_io_max_combine_limit | SEAMED | recompute_io_combine_limit(newval, from_max=true) |
| 30 | assign_io_combine_limit | 1168 | assign_io_combine_limit | SEAMED | recompute_io_combine_limit(newval, from_max=false) |
| 31 | show_data_directory_mode | 1181 | show_data_directory_mode | MATCH | "%04o" -> {:04o}; data_directory_mode seam (init-small) |
| 32 | show_log_file_mode | 1193 | show_log_file_mode | MATCH | "%04o"; log_file_mode seam (syslogger) |
| 33 | show_unix_socket_permissions | 1205 | show_unix_socket_permissions | MATCH | "%04o"; unix_socket_permissions seam (pqcomm) |
| 34 | check_bonjour | 1220 | check_bonjour | MATCH | !USE_BONJOUR build -> reject true |
| 35 | check_default_with_oids | 1233 | check_default_with_oids | MATCH | reject true, FEATURE_NOT_SUPPORTED |
| 36 | check_ssl | 1248 | check_ssl | MATCH | !USE_SSL build -> reject true |

`role_auth_extra` struct (C 807) -> `RoleAuthExtra` Box payload. `int myextra[2]`
(datestyle) -> `DateStyleExtra`. `int*` (random seed) -> `RandomSeedExtra`.

## Seam audit

This unit owns `backend-commands-variable-seams` (36 adapter seams) — all 36 are
installed by `backend_commands_variable::init_seams()` (verified declared==
installed, 1:1). Each install body either (a) delegates to a ported owner
(thin marshal: guc get_reset_string, pgtz/localtime/state-pgtz tz, snapmgr
FirstSnapshotSet, miscinit role getters/setters, xlogprefetcher reconfigure,
init-small/syslogger/pqcomm mode getters, syscache lookup_authid_by_name
projecting AuthIdRow->(oid,rolsuper), error NOTICE) or (b) loud
mirror-pg-and-panic for an unported owner (datetime, timestamp interval_in,
float setseed, encnames/mbutils encoding, miscinit current_role_is_superuser,
pgstat, bufmgr io_combine_limit recompute). No subsystem logic lives in any
install body — each is one delegated call or a panic.

Outward seams to other owners (justified cross-crate, thin): xact-seams
(is_transaction_state/is_sub_transaction + NEW xact_read_only/xact_iso_level,
installed by the xact owner), xlog-seams (recovery_in_progress), parallel-seams
(is_parallel_worker/initializing_parallel_worker), acl-seams
(member_can_set_role), superuser-seams (superuser_arg), varlena-seams
(split_identifier_string), mbutils-seams (get_database_encoding_name),
encnames-seams (pg_encoding_to_char), common-path-seams (canonicalize_path),
syscache-seams (lookup_authid_by_name). GUC_check_err* + get_reset_string are
direct calls into the merged `backend-utils-misc-guc` (acyclic). common-string
pg_clean_ascii is a direct call.

New seams added to a complete owner: `xact_read_only`, `xact_iso_level` in
xact-seams, INSTALLED by the xact owner's init_seams (XactReadOnly/XactIsoLevel
getters). No uninstalled seam on a complete owner; recurrence guard green.

## Design conformance

* No invented opacity: the Send-name carrier and (oid,bool) projection are value
  types, not handles/tokens.
* Allocating calls carry `Mcx` + `PgResult` (split_identifier_string,
  pg_clean_ascii, lookup_authid_by_name) via a transient MemoryContext.
* No shared statics for per-backend globals (all such state read through the
  owner's accessors).
* No locks across `?`. No registry side tables.
* The one divergence (assign_random_seed rollback re-arm) is ledgered above.

## Verdict: PASS

All 36 functions MATCH or SEAMED per the rules. The single random-seed-rearm
adaptation is forced by the `&`-extra slot contract and is observably
equivalent. Seam install coverage 1:1; both seams-init guards green; 11 in-crate
logic tests pass; workspace check green.
