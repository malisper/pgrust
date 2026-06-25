# Audit: backend-utils-adt-pg-locale (pg_locale.c)

C source: `src/backend/utils/adt/pg_locale.c` (1676 LOC). Active profile:
non-WIN32, ICU-disabled (`USE_ICU` off), glibc.

## Function inventory & verdicts

| C fn (line) | Port | Verdict | Notes |
|---|---|---|---|
| `pg_perm_setlocale` (198) | setup.rs | MATCH | non-WIN32 path: setlocale (OS FFI) -> on NULL return Ok(None); LC_CTYPE -> SetMessageEncoding(GetDatabaseEncoding()) (!ENABLE_NLS arm) via env+mb seams; envvar switch via typed LcCategory; setenv != 0 -> Ok(None). WIN32 IsoLocaleName + ENABLE_NLS branches compiled out. FATAL "unrecognized LC category" unreachable through the typed enum (kept as dead helper). |
| `check_locale` (301) | setup.rs `check_locale`/`_inner` | MATCH | non-ASCII in -> WARNING + false; save=setlocale(NULL) (owned clone, immune to scratch reuse); set; capture canonname before restore; restore + WARNING on failure; non-ASCII out check gated on want_canonname (C's canonname!=NULL). |
| `check_locale_monetary/numeric/time` (366/378/390) | setup.rs | MATCH | check_locale(LC_*, newval, NULL) -> validity only. |
| `assign_locale_monetary/numeric/time` (372/384/396) | setup.rs | MATCH | reset CurrentLocaleConvValid / CurrentLCTimeValid; also store the lc_* GUC value (GUC string plumbing unported). |
| `check_locale_messages` (412) | setup.rs | MATCH | "" -> source==PGC_S_DEFAULT; WIN32 accept-blind cfg'd; else check_locale(LC_MESSAGES). |
| `assign_locale_messages` (435) | setup.rs | MATCH | (void) pg_perm_setlocale(LC_MESSAGES, newval), result discarded. |
| `free_struct_lconv` (452) | — | N/A | C frees malloc'd lconv members; the repo CashLconv carrier owns its Strings (no manual free). Folded into the CashLconv model. |
| `struct_lconv_is_valid` (471) | — | N/A | strdup-failure check; the repo path builds CashLconv directly (infallible String) — no NULL members to validate. |
| `db_encoding_convert` (502) | localeconv.rs (non-C path) | SEAMED/PARTIAL-by-design | The C-locale fast path needs no conversion; the non-C path requires pg_any_to_server (env seam, owner unported) — that path Errs/panics loudly, not a silent stub. |
| `strftime_l_win32` (657) | — | N/A | WIN32-only, compiled out. |
| `cache_single_string` (705) | localeconv.rs (non-C path) | SEAMED-by-design | pg_any_to_server + MemoryContextStrdup; only on the non-C LC_TIME path (gated, Errs until the encoding owner lands). |
| `cache_locale_time` (728) | localeconv.rs | MATCH (C-locale) / SEAMED (non-C) | C-locale: arrays stay empty -> getters None (DCH built-in English), valid=true. non-C: needs strftime_l + pg_get_encoding_from_locale + pg_any_to_server (unported) -> Errs loudly. CurrentLCTimeValid short-circuit ported. |
| `search_locale_enum` (914) | — | N/A | WIN32-only. |
| `get_iso_localename` (980) | — | N/A | WIN32-only. |
| `IsoLocaleName` (1054) | — | N/A | WIN32&LC_MESSAGES-only; non-WIN32 the pg_perm_setlocale branch is compiled out. |
| `create_pg_locale` (1075) | cache.rs | MATCH | SearchSysCache1(COLLOID) via catalog seam; dispatch builtin/icu/libc/else-support-error; is_default=false; XOR(collate_is_c, collate-methods) debug_assert; collversion-recorded -> get_collation_actual_version(collcollate for libc / colllocale else) -> None=>"has no actual version" error, mismatch=>WARNING with quote_qualified_identifier(get_namespace_name(...)). |
| `init_database_collation` (1154) | cache.rs | MATCH | Assert(default_locale==NULL) debug_assert; SearchSysCache1(DATABASEOID) -> "cache lookup failed for database"; dispatch on datlocprovider building DEFAULT_COLLATION_OID; is_default=true; publish; set_database_ctype_is_c from datctype (C/POSIX), matching postinit.c. |
| `pg_newlocale_from_collation` (1196) | cache.rs `resolve`/seam | MATCH | DEFAULT->default_locale; C_COLLATION_OID->&c_locale (no catalog); !OidIsValid->cache-lookup-failed; MRU shortcut; hashtable build+insert+intern. Seam copies the flag core into mcx. |
| `get_collation_actual_version` (1254) | version.rs | MATCH | builtin->builtin seam; ICU->None (USE_ICU off, C falls to NULL); libc->in-crate glibc version; else None. |
| `pg_strlower` (1271) | dispatch.rs | MATCH | builtin->strlower_builtin seam; libc->strlower_libc seam; else support error. |
| `pg_strtitle` (1290) | dispatch.rs | MATCH | builtin/libc seam dispatch; else support error. |
| `pg_strupper` (1309) | dispatch.rs | MATCH | builtin/libc seam dispatch; else support error. |
| `pg_strfold` (1328) | dispatch.rs | MATCH | builtin->strfold_builtin; libc->strlower_libc (C "just uses strlower"); else support error. |
| `pg_strcoll` (1353) | dispatch.rs | MATCH | -> pg_strncoll (the len==-1 legs == payload slices). |
| `pg_strncoll` (1373) | dispatch.rs + libc_provider.rs | MATCH | locale->collate->strncoll; reachable only !collate_is_c (libc strncoll_libc, OS strcoll_l); else support error (C's NULL-collate dispatch). |
| `pg_strxfrm_enabled` (1387) | dispatch.rs | MATCH | collate_methods_libc.strxfrm_is_safe=false (non-TRUST_STRXFRM) / builtin no vtable -> false. |
| `pg_strxfrm` (1403) | dispatch.rs | MATCH | -> pg_strnxfrm; libc strnxfrm_libc (OS strxfrm_l, two-pass sizing -> full blob). |
| `pg_strnxfrm` (1428) | dispatch.rs `pg_strxfrm` | MATCH | repo collapses pg_strxfrm/pg_strnxfrm to one collid-keyed seam returning the full blob. |
| `pg_strxfrm_prefix_enabled` (1439) | dispatch.rs | MATCH | strnxfrm_prefix==NULL (libc) -> false. |
| `pg_strxfrm_prefix` / `pg_strnxfrm_prefix` (1450/1475) | dispatch.rs | MATCH | libc/builtin have no prefix method -> support error (C would null-deref; we degrade-to-error). |
| `builtin_locale_encoding` (1486) | lib.rs | MATCH | C/-1, C.UTF-8/PG_UTF8, PG_UNICODE_FAST/PG_UTF8, else WRONG_OBJECT_TYPE. |
| `builtin_validate_locale` (1510) | lib.rs | MATCH | canonicalize C/C.UTF-8(+C.UTF8)/PG_UNICODE_FAST; required-encoding mismatch -> WRONG_OBJECT_TYPE. |
| `icu_language_tag` (1550) | lib.rs | MATCH | USE_ICU off -> #else ereport(FEATURE_NOT_SUPPORTED). |
| `icu_validate_locale` (1608) | lib.rs | MATCH | USE_ICU off -> #else ereport(FEATURE_NOT_SUPPORTED). |

### Provider externs (`pg_locale_libc.c` / `pg_locale_builtin.c` / `pg_locale_icu.c`)

These are calls into *other* catalog units, not pg_locale.c logic:

- libc collation primitives `create_pg_locale_libc` / `make_libc_collator` /
  `strncoll_libc` / `strnxfrm_libc` / `get_collation_actual_version_libc` /
  `char_tolower` — bound to OS FFI in `libc_provider.rs` (the `locale_t` lives in
  this crate's permanent cache, so they are bound where the cache is, per the
  task). `report_newlocale_failure` mirrored. MATCH against `pg_locale_libc.c`.
- builtin `create_pg_locale_builtin` / `strlower_builtin` / … — SEAMED to
  `backend-utils-adt-pg-locale-builtin-seams` (owner unported; panics).
- libc case-mapping `strlower_libc`/`strtitle_libc`/`strupper_libc` — SEAMED to
  `backend-utils-adt-pg-locale-libc-seams` (owner unported; the flag-core seam
  carrier cannot carry `info.lt`, deferred to the libc owner port).
- ICU `create_pg_locale_icu` — direct call into the merged `-icu` crate
  (FEATURE_NOT_SUPPORTED).

## Seam audit

Owned inward seam crate: `backend-utils-adt-pg-locale-seams`. `init_seams()`
installs all 27 declarations: the 24 pg_locale.c entry points
(pg_newlocale_from_collation, init_database_collation, collation_is_c,
collation_is_deterministic, pg_strcoll, pg_strncoll,
pg_strxfrm[_enabled/_prefix/_prefix_enabled], pg_str{lower,upper,title,fold},
char_tolower, get_collation_actual_version, pg_perm_setlocale,
set_database_ctype_is_c, pglc_localeconv, cache_locale_time, localized_*_{days,
months}) plus the 3 `regex_wc_*` probes (regex_wc_isclass/_toupper/_tolower).
The latter are `regc_pg_locale.c`'s non-C-strategy legs (`pg_wc_is*`/
`pg_wc_toupper`/`pg_wc_tolower`): the C-strategy stays hard-wired in the regex
engine, but the BUILTIN/LIBC/ICU strategies reach the locale's provider `info`
union, which this crate's permanent cache owns — so this crate is their owner
and installs them (libc bound to OS FFI `iswXXX_l`/`towXXX_l`; builtin delegated
to backend-utils-adt-pg-locale-builtin-seams `regex_wc_*_builtin`, owner
unported; ICU disabled). The wide-vs-1byte choice reads
`pg_database_encoding_max_length` (mbutils seam). The `init_seams()` body is
`set()` calls only. Wired into `seams-init::init_all()`; both seam-init guards
(`every_seam_installing_crate_is_wired_into_init_all` /
`every_declared_seam_is_installed_by_its_owner`) pass.

Outward seams all justified (would cycle once owners land, or owner unported):
builtin-seams, libc-seams, catalog-seams (syscache COLLOID/DATABASEOID reads),
env-seams (set_message_encoding/pg_get_encoding_from_locale/pg_any_to_server),
mbutils-seams (get_database_encoding). Each is thin marshal+delegate; the
provider-dispatch branching lives in dispatch.rs (a crate), not in a seam path.

## Design conformance

- No invented opacity: `LibcLocale` wraps a real `libc::locale_t`; `LocaleEntry`
  is the real `pg_locale_struct` flag core + owned info union.
- Allocating seams carry `Mcx` + `PgResult` (pg_strxfrm/strlower/...); the
  collid-keyed comparison seams carry no Mcx (match the C value-return shape).
- Per-backend C globals (c_locale/default_locale/cache/MRU/CurrentLocaleConvValid
  /CurrentLCTimeValid/database_ctype_is_c/lc_*) are thread_local, not shared
  statics.
- `freelocale` is a `Drop` impl on `LibcLocale` (no leaked `locale_t`).
- No todo!/unimplemented!. Two `unreachable!` follow `create_pg_locale_icu(...)?`
  which always Errs in the ICU-disabled profile (mirrors C's `result = ...icu();`
  then `#else` ereport).

## Behavior gated on unported owners (loud, not silent)

- Non-C `PGLC_localeconv`: panics (pg_localeconv_r unported); the seam is
  infallible so a structured error is impossible — mirror-and-panic.
- Non-C `cache_locale_time`: Errs (strftime_l + pg_any_to_server unported).
- Building any builtin/libc/non-C collation: seam-panics into the unported
  provider/syscache owners. The C-locale + (once a provider lands) default
  paths are fully live.

## Verdict: PASS

Every pg_locale.c function is MATCH or SEAMED-per-rules; the WIN32/ICU-enabled
branches are outside the active build config; the non-C localeconv/lc_time paths
fail loudly on unported callees (not absent logic). Zero seam findings.
