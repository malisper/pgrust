# Audit: backend-utils-mb-mbutils (utils/mb/mbutils.c)

Independent re-derivation from `postgres-18.3/src/backend/utils/mb/mbutils.c`.
Port crate: `crates/backend-utils-mb-mbutils/src/lib.rs`.

## Per-function table

| C function (line) | Port | Verdict | Notes |
|---|---|---|---|
| `PrepareClientEncoding` (119) | `PrepareClientEncoding` | MATCH | FE-encoding check, startup short-circuit, no-conversion-needed trio, IsTransactionState branch (FindDefaultConversionProc x2 + ConvProcList insert at head), else cached-restore scan. Returns 0/-1. |
| `SetClientEncoding` (217) | `SetClientEncoding` | MATCH | FE check, pending-store before startup, no-conv trio sets ClientEncoding + clears To{Server,Client}ConvProc, else cache scan with found-set / duplicate-remove (foreach_delete_current). |
| `InitializeClientEncoding` (290) | `InitializeClientEncoding` | MATCH | startup_complete assert+set, Prepare+Set with FATAL->Err on failure (FEATURE_NOT_SUPPORTED + same message text), Utf8ToServerConvProc lookup for non-UTF8/non-ASCII server encoding. |
| `pg_get_client_encoding` (345) | `pg_get_client_encoding` | MATCH | ClientEncoding->encoding. |
| `pg_get_client_encoding_name` (354) | `pg_get_client_encoding_name` | MATCH | name via encnames seam. |
| `pg_do_encoding_conversion` (365) | `pg_do_encoding_conversion` | MATCH | empty/same/SQL_ASCII-dest early returns, SQL_ASCII-src validate, !IsTransactionState elog, FindDefaultConversionProc + UNDEFINED_FUNCTION, MaxAllocHugeSize overflow guard, OidFunctionCall6 via `convert_via_proc` seam, len>1e6 + MaxAllocSize repalloc guard. No-conv = Ok(None). |
| `pg_do_encoding_conversion_buf` (478) | `pg_do_encoding_conversion_buf` | MATCH | srclen limited to `(destlen-1)/MAX_CONVERSION_GROWTH`, OidFunctionCall6 via `convert_via_proc_counted` (returns the proc int), output copied + NUL-terminated into dest, returns the proc result. |
| `pg_convert_to` (510) | `pg_convert_to` | MATCH | delegates to pg_convert with DatabaseEncoding name as source. |
| `pg_convert_from` (535) | `pg_convert_from` | MATCH | delegates to pg_convert with DatabaseEncoding name as dest. |
| `pg_convert` (562) | `pg_convert` | MATCH | pg_char_to_encoding for both names, <0 -> INVALID_PARAMETER_VALUE for source/dest, pg_verify_mbstr(src), pg_do_encoding_conversion. Takes the NAME args (the fcinfo bytea/name boundary is the typed-fn form per repo convention). |
| `length_in_encoding` (624) | `length_in_encoding` | MATCH | pg_char_to_encoding, <0 error, pg_verify_mbstr_len. |
| `pg_encoding_max_length_sql` (653) | `pg_encoding_max_length_sql` | MATCH | PG_VALID_ENCODING -> maxmblen, else None (SQL NULL). |
| `pg_client_to_server` (669) | `pg_client_to_server` | MATCH | pg_any_to_server(ClientEncoding). |
| `pg_any_to_server` (685) | `pg_any_to_server` | MATCH | empty, same/SQL_ASCII-validate, SQL_ASCII-db (BE-valid verify else ASCII-only byte scan with CHARACTER_NOT_IN_REPERTOIRE), client-fast-path, general. |
| `pg_server_to_client` (747) | `pg_server_to_client` | MATCH | pg_server_to_any(ClientEncoding). |
| `pg_server_to_any` (758) | `pg_server_to_any` | MATCH | empty, same/SQL_ASCII assume-valid, SQL_ASCII-db verify, client-fast-path, general. |
| `perform_default_encoding_conversion` (792) | `perform_default_encoding_conversion` | MATCH | client/server direction picks src/dest/proc; NULL proc -> Ok(None); MaxAllocHugeSize guard; FunctionCall6 via seam; len>1e6 guard. |
| `pg_unicode_to_server` (873) | `pg_unicode_to_server` | MATCH | invalid-codepoint SYNTAX_ERROR, ASCII trivial, UTF8 reformat, Utf8ToServerConvProc NULL -> FEATURE_NOT_SUPPORTED, else convert via seam. |
| `pg_unicode_to_server_noerror` (935) | `pg_unicode_to_server_noerror` | MATCH | as above but Ok(None) on failure; uses `convert_via_proc_counted` (noError=true) and tests consumed==input. |
| `pg_mb2wchar` (988) | `pg_mb2wchar` | MATCH | strlen + table mb2wchar_with_len for DB encoding. |
| `pg_mb2wchar_with_len` (995) | `pg_mb2wchar_with_len` | MATCH | |
| `pg_encoding_mb2wchar_with_len` (1002) | `pg_encoding_mb2wchar_with_len` | MATCH | allocates (len+1) wchars, truncates to returned count. |
| `pg_wchar2mb` (1010) | `pg_wchar2mb` | MATCH | pg_wchar_strlen + table. |
| `pg_wchar2mb_with_len` (1017) | `pg_wchar2mb_with_len` | MATCH | |
| `pg_encoding_wchar2mb_with_len` (1024) | `pg_encoding_wchar2mb_with_len` | MATCH | worst-case maxmblen*len+1 buffer, truncate. |
| `pg_mblen_cstr` (1043) | `pg_mblen_cstr` | MATCH | mblen + the i in 1..length NUL-scan -> report_invalid_encoding_db. (VALGRIND macros omitted — no-op.) |
| `pg_mblen_range` (1082) | `pg_mblen_range` | MATCH | mblen + range overflow -> report_invalid_encoding_db. |
| `pg_mblen_with_len` (1106) | `pg_mblen_with_len` | MATCH | mblen + length>limit -> report. |
| `pg_mblen_unbounded` (1135) | `pg_mblen_unbounded` | MATCH | bare mblen. |
| `pg_mblen` (1149) | `pg_mblen` | MATCH | alias. |
| `pg_dsplen` (1156) | `pg_dsplen` | MATCH | table dsplen. |
| `pg_mbstrlen` (1163) | `pg_mbstrlen` | MATCH | single-byte strlen fast path, else mblen_cstr loop. |
| `pg_mbstrlen_with_len` (1183) | `pg_mbstrlen_with_len` | MATCH | single-byte returns limit, else mblen_with_len loop. |
| `pg_mbcliplen` (1209) | `pg_mbcliplen` | MATCH | pg_encoding_mbcliplen(DB). |
| `pg_encoding_mbcliplen` (1219) | `pg_encoding_mbcliplen` | MATCH | single-byte cliplen, else mblen loop with clen+l>limit break and clen==limit break. |
| `pg_mbcharcliplen` (1251) | `pg_mbcharcliplen` | MATCH | single-byte cliplen, else nch>limit break loop. |
| `cliplen` (1276) | `cliplen` | MATCH | min(len,limit) then advance to NUL. |
| `SetDatabaseEncoding` (1287) | `SetDatabaseEncoding` | MATCH | BE check -> elog ERROR, store. |
| `SetMessageEncoding` (1297) | `SetMessageEncoding` | MATCH | assert, store. |
| `pg_bind_textdomain_codeset` (1352) / `raw_pg_bind_textdomain_codeset` (1313) | — | N/A | `#ifdef ENABLE_NLS` only; not in the default build config. Omitted (platform/feature-gated). |
| `GetDatabaseEncoding` (1387) | `GetDatabaseEncoding` | MATCH | |
| `GetDatabaseEncodingName` (1393) | `GetDatabaseEncodingName` | MATCH | |
| `getdatabaseencoding` (1399) | `getdatabaseencoding` | MATCH | namein(DatabaseEncoding name). |
| `pg_client_encoding` (1405) | `pg_client_encoding` | MATCH | namein(ClientEncoding name). |
| `PG_char_to_encoding` (1411) | `PG_char_to_encoding` | MATCH | pg_char_to_encoding(NameStr). |
| `PG_encoding_to_char` (1419) | `PG_encoding_to_char` | MATCH | namein(pg_encoding_to_char). |
| `GetMessageEncoding` (1434) | `GetMessageEncoding` | MATCH | |
| `pg_generic_charinc` (1451) | `pg_generic_charinc` | MATCH | last-byte increment loop with mbverifychar==len. |
| `pg_utf8_increment` (1485) | `pg_utf8_increment` | MATCH | C switch fall-through 4->3->2->1 modeled with early returns; `default` (len 5/6) rejected up front WITHOUT touching bytes (regression-tested). 0xED/0xF4 limit table preserved. |
| `pg_eucjp_increment` (1563) | `pg_eucjp_increment` | MATCH | SS2/SS3/JIS-X-0208/ASCII arms with the 0xa1/0xfe/0xdf boundaries. |
| `pg_database_encoding_character_incrementer` (1649) | `pg_database_encoding_character_incrementer` | MATCH | UTF8/EUC_JP/default switch. |
| `pg_database_encoding_max_length` (1672) | `pg_database_encoding_max_length` | MATCH | table maxmblen. |
| `pg_verifymbstr` (1682) | `pg_verifymbstr` | MATCH | pg_verify_mbstr(DB). |
| `pg_verify_mbstr` (1692) | `pg_verify_mbstr` | MATCH | mbverifystr oklen!=len -> report_invalid_encoding(mbstr+oklen). |
| `pg_verify_mbstr_len` (1723) | `pg_verify_mbstr_len` | MATCH | single-byte NUL-scan, else ASCII fast path + mbverifychar loop, noError -> -1. |
| `check_encoding_conversion_args` (1795) | `check_encoding_conversion_args` | MATCH | 5 elog ERROR predicates with the exact messages/order. |
| `report_invalid_encoding` (1824) | `report_invalid_encoding` | MATCH | mblen_or_incomplete + int form; CHARACTER_NOT_IN_REPERTOIRE + hex dump (min(mblen,len,8)). |
| `report_invalid_encoding_int` (1832) | `report_invalid_encoding_int` | MATCH | buf rendering. |
| `report_invalid_encoding_db` (1857) | `report_invalid_encoding_db` | MATCH | DB encoding. |
| `report_untranslatable_char` (1869) | `report_untranslatable_char` | MATCH | UNTRANSLATABLE_CHARACTER + 3-name message. |
| `pgwin32_message_to_UTF16` (1913) | — | N/A | `#ifdef WIN32` only. Omitted. |

## Seam audit

Owned inward seam crate: `backend-utils-mb-mbutils-seams`. `init_seams()` is
`set()`-only and is wired into `seams-init::init_all()`. Installs: pg_verifymbstr,
pg_server_to_client, pg_client_to_server, pg_mbstrlen_with_len, pg_mbcliplen,
pg_mbcharcliplen, pg_mb2wchar_with_len, pg_wchar2mb_with_len, pg_mblen_range,
pg_database_encoding_max_length, get_database_encoding, get_database_encoding_name,
set_database_encoding, initialize_client_encoding, pg_server_to_any,
pg_get_client_encoding, pg_encoding_mblen, pg_encoding_is_client_only,
pg_unicode_to_server, report_invalid_encoding, report_untranslatable_char,
check_encoding_conversion_args.

- `is_encoding_supported_by_icu` is declared in this seam crate but is
  `common/encnames.c` logic (`pg_enc2icu_tbl`), not mbutils.c. Deliberately NOT
  installed here (wrong-homing the ICU table would violate ownership-by-C-source);
  its owner is the unported encnames unit; its sole consumer
  (`recomputeNamespacePath` ICU branch) stays a frontier panic. Recorded as
  DESIGN_DEBT TD-ENCNAMES-ICU + a `CONTRACT_RECONCILE_PENDING` allowlist entry
  (`backend_utils_mb_mbutils`, `is_encoding_supported_by_icu`); the seams-init
  recurrence guard passes.

Outward calls (all thin marshal+delegate, justified by real cycles since the
owners depend on this unit's seam crate):
- `FindDefaultConversionProc` — direct dep on backend-catalog-namespace (acyclic;
  namespace deps only the seam crate). Catalog lookup.
- `is_transaction_state` — xact-seams.
- `pg_char_to_encoding` / `pg_encoding_to_char` — common-encnames-seams.
- `namein` — direct dep on backend-utils-adt-name (acyclic).
- `convert_via_proc` / `convert_via_proc_counted` — NEW seams in
  backend-utils-fmgr-fmgr-seams, owned+installed by backend-utils-fmgr-core
  (the OidFunctionCall6 buffered-conversion dispatch only fmgr can frame). These
  are the C `OidFunctionCall6`/`FunctionCall6` invocations of a conversion proc;
  no logic lives in the seam beyond fcinfo framing.

## Design conformance

- Per-backend C globals (ClientEncoding/DatabaseEncoding/MessageEncoding,
  backend_startup_complete, pending_client_encoding, ConvProcList, To*ConvProc,
  Utf8ToServerConvProc) modeled as `AtomicI32`/`AtomicBool`/`thread_local`
  RefCell — per-backend state, not shared statics. OK.
- ConvProcInfo caches conversion-proc OIDs rather than C's `FmgrInfo`
  (FmgrInfo is Mcx-bound and cannot live in a lifetime-free backend cache).
  Behavior-preserving (by-OID dispatch of a registered conv proc is catalog-free,
  matching C's "safe outside a transaction" cache purpose). Documented.
- Allocating fns/seams take `Mcx` + return `PgResult`; OOM via mcx alloc helpers.
- `format!`/`to_string`/`String` uses are all on error-message construction at
  Err sites or the hex-dump rendering. No allocation on a hot non-error path.
- The three infallible length seams (pg_mbstrlen_with_len/pg_mbcharcliplen/
  pg_mblen_range) wrap fallible bodies with `.expect()` because the seam contract
  (its consumers) is infallible and the C error path (report_invalid_encoding) is
  documented-unreachable for verified callers; surfaced loudly rather than
  silently. Pre-existing seam contract, not introduced.

## Frontier note

Conversion-proc dispatch by OID reaches the fmgr boundary; the typed conv crates
(backend-conv-*) are not registered as fmgr builtins, so a real conversion
dispatch errors at `fmgr_info` — bridging Oid -> typed ConversionResult is a
separate unported concern. mbutils' own logic is complete; only the callee
dispatch is a frontier.

## Verdict: PASS

Every function MATCH or N/A (platform/feature-gated, not in the build config).
Zero seam findings. cargo check --workspace clean; no-todo guard clean;
seams-init recurrence guards pass; 7 in-crate unit tests pass.
