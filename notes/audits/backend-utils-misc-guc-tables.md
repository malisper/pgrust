# Audit: backend-utils-misc-guc-tables

Unit: `backend-utils-misc-guc-tables` (`src/backend/utils/misc/guc_tables.c`,
5444 lines, plus the static-data declarations in `src/include/utils/guc.h` /
`src/include/utils/guc_tables.h`, PostgreSQL 18.3).
Crates audited: `crates/backend-utils-misc-guc-tables` (+ its `types-guc`
vocabulary), `crates/backend-utils-misc-guc-file-seams`.
Cross-checked against
`../pgrust/c2rust-runs/backend-utils-misc-guc-tables/src/guc_tables.rs`
(22,199 lines; note the `ConfigureNamesInt` and `ConfigureNamesEnum` arrays'
real contents live in c2rust's `run_static_initializers`, not the zeroed
top-level placeholders).
Auditor: independent re-derivation from the C sources and headers; entry-level
comparison done **mechanically** (see Method), then spot-checked by hand.

## Function inventory

`guc_tables.c` defines **zero functions**. The translation unit consists only
of:

- 14 `StaticAssertDecl`s (length checks on the option/name arrays — discharged
  here by verifying the actual lengths, see below);
- 32 file-local `static const struct config_enum_entry` option arrays;
- 6 `extern const struct config_enum_entry` option-array *references* owned by
  other C modules (`archive_mode_options`, `dynamic_shared_memory_options`,
  `io_method_options`, `recovery_target_action_options`, `wal_level_options`,
  `wal_sync_method_options`);
- 4 name tables (`GucContext_Names`, `GucSource_Names`, `config_group_names`,
  `config_type_names`);
- the 5 `ConfigureNames{Bool,Int,Real,String,Enum}` arrays (115/147/26/75/41
  live entries + 1 zeroed sentinel each in the proven build).

The c2rust rendering confirms the same: no functions, only statics (plus the
compiler-generated `run_static_initializers`). The audit is therefore a
per-data-object, per-entry comparison.

## Method

Both sides were reduced to one normalized line per option-array entry and per
GUC setting (name, numeric context/group, short/long descriptions, numeric
flags, storage-variable symbol, boot value, min/max with f64 bit-exactness for
reals, options-array identity, check/assign/show hook symbols), and diffed:

- ground truth: a parser over the c2rust `guc_tables.rs` that resolves every
  `pub const` (including computed ones such as `MAX_BACKENDS`,
  `SLRU_MAX_ALLOWED_BUFFERS = 1 GiB / BLCKSZ`, `INT_MAX / 2`,
  `Min(...)`-shaped `if/else` expressions, and the transmuted compiled-in
  string defaults `PG_VERSION`/`DEFAULT_PGSOCKET_DIR`/`DEFAULT_EVENT_SOURCE`/
  `DEFAULT_TABLE_ACCESS_METHOD`/`PG_KRB_SRVTAB`), reading Int/Enum entries
  from `run_static_initializers`;
- port: a throwaway dump binary iterating `all_settings()` and every exported
  option array (not committed).

Result: **578 lines vs 578 lines, every line identical** after the fix below.
The single non-semantic diff artifact: the port's `recovery_prefetch` entry
resolves by address to `huge_pages_options` because the two arrays are
content-identical (off/on/try + 6 hidden boolean spellings with values 0/1/2)
and the compiler deduplicates them; the source (`tables.rs:928`) names
`recovery_prefetch_options`, and the contents are equal either way.

## Per-object table

| # | C object (guc_tables.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `bytea_output_options` (:120) | `tables.rs` | MATCH | 2 entries, values 0/1. |
| 2 | `client_message_level_options` (:130) | `tables.rs` | MATCH | 11 entries; DEBUG5..ERROR = 10..21 match elog.h via `types_error`; hidden `debug`(=DEBUG2) and `info`. |
| 3 | `server_message_level_options` (:145) | `tables.rs` | MATCH | 13 entries incl. fatal=22/panic=23; `info` *not* hidden here, matching C. |
| 4 | `intervalstyle_options` (:184) | `tables.rs` | MATCH | 4 entries 0..3. |
| 5 | `icu_validation_level_options` (:194 area) | `tables.rs` | MATCH | 12 entries; leading `disabled` = −1. |
| 6 | `log_error_verbosity_options` (:196) | `tables.rs` | MATCH | terse/default/verbose 0/1/2. |
| 7 | `log_statement_options` (:205) | `tables.rs` | MATCH | none/ddl/mod/all 0..3. |
| 8 | `isolation_level_options` (:216) | `tables.rs` | MATCH | XACT_* 3/2/1/0 order as C. |
| 9 | `session_replication_role_options` (:224) | `tables.rs` | MATCH | origin/replica/local 0/1/2. |
| 10 | `syslog_facility_options` (:233) | `tables.rs` | MATCH | HAVE_SYSLOG branch (8 local0–7 entries, values 128..184 = (16+n)<<3 per sys/syslog.h); matches the proven build. |
| 11 | `track_function_options` (:248) | `tables.rs` | MATCH | none/pl/all 0/1/2. |
| 12 | `stats_fetch_consistency` (:259) | `tables.rs` | MATCH | none/cache/snapshot 0/1/2. |
| 13 | `xmlbinary_options` (:269) | `tables.rs` | MATCH | base64/hex 0/1. |
| 14 | `xmloption_options` (:278) | `tables.rs` | MATCH | content/document 1/0. |
| 15 | `backslash_quote_options` (:287) | `tables.rs` | MATCH | 9 entries; safe_encoding=2, hidden boolean spellings. |
| 16 | `compute_query_id_options` (:300) | `tables.rs` | MATCH | 10 entries; auto=2, regress=3, on=1, off=0 + hidden spellings. |
| 17 | `constraint_exclusion_options` (:314) | `tables.rs` | MATCH | partition=2 default-shaped table; 9 entries. |
| 18 | `synchronous_commit_options` (:327) | `tables.rs` | MATCH | 11 entries; local=1, remote_write=2, on=3 (REMOTE_FLUSH), remote_apply=4 + hidden spellings. |
| 19 | `huge_pages_options` (:342) | `tables.rs` | MATCH | off/on/try 0/1/2 + 6 hidden spellings. |
| 20 | `huge_pages_status_options` (:356) | `tables.rs` | MATCH | off/on/unknown 0/1/3. |
| 21 | `recovery_prefetch_options` (:364) | `tables.rs` | MATCH | identical contents to #19's value shape; port names the right array (see Method note). |
| 22 | `debug_parallel_query_options` (:378) | `tables.rs` | MATCH | off/on/regress 0/1/2 + hidden spellings. |
| 23 | `plan_cache_mode_options` (:392) | `tables.rs` | MATCH | auto/force_generic/force_custom 0/1/2. |
| 24 | `password_encryption_options` (:400 area) | `tables.rs` | MATCH | md5=1, scram-sha-256=2. |
| 25 | `ssl_protocol_versions_info` (:407) | `tables.rs` | MATCH | ""(=PG_TLS_ANY 0), TLSv1..TLSv1.3 = 1..4; restructured as a backing const + two slice statics so `+1` (below) is expressible. |
| 26 | `debug_logical_replication_streaming_options` (:439 area) | `tables.rs` | MATCH | buffered/immediate 0/1. |
| 27 | `recovery_init_sync_method_options` (:440) | `tables.rs` | MATCH | fsync only — HAVE_SYNCFS not defined in the proven (macOS) build. |
| 28 | `shared_memory_options` (:446) | `tables.rs` | MATCH | sysv=1, mmap=2; no `windows` entry (non-WIN32 build). |
| 29 | `default_toast_compression_options` (:459) | `tables.rs` | MATCH | pglz='p'(112), lz4='l'(108); USE_LZ4 defined in proven build. |
| 30 | `wal_compression_options` (:467) | `tables.rs` | MATCH | pglz/lz4/zstd 1/2/3 (USE_LZ4 and USE_ZSTD both defined), on→pglz, off→none + hidden spellings. |
| 31 | `file_copy_method_options` (:484) | `tables.rs` | MATCH | copy=0, clone=1 (HAVE_COPYFILE + COPYFILE_CLONE_FORCE on macOS). |
| 32 | `file_extend_method_options` (:493) | `tables.rs` | MATCH | write_zeros only (no HAVE_POSIX_FALLOCATE). |
| 33 | 6 extern option arrays (:94–104 in c2rust) | `option_set: Some("<name>")` | SEAMED-equivalent | Owned by other C modules; the port carries the C array name for the owning unit to resolve. Exactly the six C externs, verified by name. |
| 34 | `GucContext_Names` (:655) | `lib.rs:35` | MATCH | 7 entries; length == PGC_USERSET+1 (StaticAssert honored). |
| 35 | `GucSource_Names` (:670) | `lib.rs:46` | MATCH | 14 entries incl. the duplicated "default"; length == PGC_S_SESSION+1. |
| 36 | `config_group_names` (:698) | `lib.rs:64` | MATCH | 48 entries; length == DEVELOPER_OPTIONS+1; strings byte-identical. |
| 37 | `config_type_names` (:758) | `lib.rs:116` | MATCH | 5 entries; length == PGC_ENUM+1. |
| 38 | `ConfigureNamesBool` (:771) | `tables.rs::ConfigureNamesBool` | MATCH | 115/115 entries line-identical (name, context, group, descs incl. double-space typography, flags, variable symbol, boot, hooks). Sentinel dropped (idiomatic slice length replaces it). |
| 39 | `ConfigureNamesInt` (:2098) | `tables.rs::ConfigureNamesInt` | MATCH | 147/147 entries; boot/min/max evaluated and compared numerically (e.g. shared_buffers 16384/16/INT_MAX÷2 = 1073741823, max_stack_depth 100/100/MAX_KILOBYTES = INT_MAX, MAX_BACKENDS = 262143, autovacuum_freeze_max_age caps, GUC_UNIT_* in flags). |
| 40 | `ConfigureNamesReal` (:3947) | `tables.rs::ConfigureNamesReal` | MATCH | 26/26 entries; f64 boot/min/max compared bit-exactly (incl. DBL_MAX bounds, GEQO 2.0/1.5/2.0). |
| 41 | `ConfigureNamesString` (:4232) | `tables.rs::ConfigureNamesString` | MATCH | 75/75 entries; NULL boot vals as `None`; compiled-in defaults verified: server_version "18.3", default_table_access_method "heap", unix_socket_directories "/tmp", event_source "PostgreSQL", krb_server_keyfile "" (the proven build's `PG_KRB_SRVTAB` is the empty string), search_path "\"$user\", public". |
| 42 | `ConfigureNamesEnum` (:5070) | `tables.rs::ConfigureNamesEnum` | MATCH (1 fix) | 41/41 entries. **Finding (fixed):** `ssl_min_protocol_version` used the full `ssl_protocol_versions_info`; C uses `ssl_protocol_versions_info + 1` ("don't allow PG_TLS_ANY", guc_tables.c:5392). Fixed to `ssl_protocol_versions_info_without_any` (same backing array, first entry skipped); re-audited from scratch — dump now shows `ssl_protocol_versions_info+1` semantics, matching c2rust's `.offset(1)`. `ssl_max_protocol_version` correctly keeps the full array. |
| 43 | 14 `StaticAssertDecl`s | (lengths) | MATCH | Discharged by the entry-count and index checks above and `tests.rs::table_counts_match_compiled_backend_shape`. |

### Entries compiled out of the proven build (exist only in the original C)

Excluded from both the c2rust ground truth and the port, matching the proven
build configuration (c2rust ran post-preprocessor):

- `DEBUG_NODE_TESTS_ENABLED`: `debug_copy_parse_plan_trees`,
  `debug_write_read_parse_plan_trees`, `debug_raw_expression_coverage_test`
- `BTREE_BUILD_STATS`: `log_btree_build_stats`
- `LOCK_DEBUG`: `trace_locks`, `trace_userlocks`, `trace_lwlocks`,
  `debug_deadlocks`, `trace_lock_oidmin`, `trace_lock_table`
- `TRACE_SYNCSCAN`: `trace_syncscan`
- `DEBUG_BOUNDED_SORT`: `optimize_bounded_sort`
- `WAL_DEBUG`: `wal_debug`

Value-affecting conditionals (`HAVE_SYSLOG` default facility,
`USE_ASSERT_CHECKING` debug_assertions boot, `DISCARD_CACHES_ENABLED`
debug_discard_caches range, `USE_SSL`/`USE_OPENSSL` presets, `USE_LZ4`,
`USE_ZSTD`, `HAVE_SYNCFS`, WIN32 branches) are all covered by the mechanical
diff against the compiled build.

## Constants

- `types-guc` GUC_* flag bits verified character-by-character against
  `utils/guc.h:214-244` (incl. `GUC_NO_RESET` 0x8 / `GUC_NO_RESET_ALL` 0x10,
  `GUC_CUSTOM_PLACEHOLDER` 0x200, the unit nibbles 0x01000000..0x05000000 and
  0x10000000..0x30000000, and the masks). `GucContext`, `GucSource`,
  `config_type`, `config_group` discriminants verified against
  `utils/guc.h` / `utils/guc_tables.h` enum order (0..6, 0..13, 0..4, 0..47).
- `consts.rs` values all flow into the normalized numeric comparison, so each
  is checked against the compiled build (e.g. BLCKSZ 8192, DEF_PGPORT 5432,
  PG_VERSION_NUM 180003, MAX_BACKENDS 262143, MaxAllocSize 0x3fffffff,
  SLRU_MAX_ALLOWED_BUFFERS 131072, TOAST_*_COMPRESSION 112/108, LOG_LOCAL0..7
  128..184, message levels via `types_error`).

## Port-added API (no C counterpart; checked for semantic fidelity)

- `GucBoolSetting`/`GucIntSetting`/... carry exactly the *constant* fields of
  `struct config_bool`/`config_int`/... (guc_tables.h); the runtime fields
  (`status`, `source`, `reset_*`, stack/links, `sourcefile`...) belong to the
  unported GUC core and are correctly absent from this data crate.
- `variable` is the C storage global's name (runtime storage lives with the
  owning subsystem per the repo's thread-local rule); all 404 symbol names
  match the C `&var` arguments.
- Hooks carried by C symbol name and dispatched via `GucHookProvider`; all
  hook names match the C function pointers (verified in the diff). A `None`
  hook accepts/no-ops, matching C's NULL-hook semantics in
  `check_setting`/`assign_setting`.
- `all_settings()` iterates in C declaration order (Bool, Int, Real, String,
  Enum); `find_option` is a metadata-only lookup, documented as such.

## Seam audit

- `crates/backend-utils-misc-guc-file-seams`: declares exactly one seam,
  `process_config_file` (= `ProcessConfigFile`, guc-file.l). Its owner unit
  (`backend-utils-misc-guc-file`) is not yet ported, so the seam is
  intentionally uninstalled and panics loudly — an unported-callee panic, not
  absent logic. Sole production consumer:
  `backend-postmaster-interrupt::SignalHandlerForConfigReload`. The only
  `set()` call anywhere is in `backend-postmaster-interrupt`'s own tests
  (test-local stub), not production wiring.
- `backend-utils-misc-guc-tables` itself defines no functions, installs no
  seams, and correctly has no `init_seams()`; `seams-init::init_all()`
  therefore (correctly) does not reference it. No outward seam calls exist in
  the crate — it is pure data plus dispatch helpers.

## Findings and fixes

1. **DIVERGES (fixed, re-audited):** `ssl_min_protocol_version.options`
   included the leading `"" = PG_TLS_ANY` entry; C passes
   `ssl_protocol_versions_info + 1` so the empty/any spelling is rejected for
   the minimum version. Fixed in `tables.rs` (backing array + 
   `ssl_protocol_versions_info_without_any`), re-dumped and re-diffed clean.

No other findings. Tests: 7/7 pass; full workspace builds.

## Verdict

**PASS** — all 42 data objects MATCH (one after the fix above), the six extern
option arrays are carried by name for their owners, the seam crate is a single
justified forward declaration with no stray installs, and the mechanical
entry-by-entry diff against the c2rust ground truth is clean.
