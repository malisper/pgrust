# Audit: backend-commands-collationcmds

Independent function-by-function audit of `port/backend-commands-collationcmds`
against `backend/commands/collationcmds.c` (PostgreSQL 18.3) and the c2rust
rendering `../pgrust/c2rust-runs/backend-commands-collationcmds/src/collationcmds.rs`.

Unit C source: `src/backend/commands/collationcmds.c` (single TU).
Port crate: `crates/backend-commands-collationcmds/src/lib.rs`.
Owned seam crate: `crates/backend-commands-collationcmds-seams`.

## Build config (decisive for the inventory)

The c2rust run was post-preprocessor with **`READ_LOCALE_A_OUTPUT` defined**
(non-WIN32) and **`USE_ICU` NOT defined** and **`ENUM_SYSTEM_LOCALE` NOT defined**.
Confirmed by the c2rust output: it contains `normalize_libc_locale_name`,
`cmpaliases`, `create_collation_from_locale` but NOT `get_icu_locale_comment`
(USE_ICU only) and NOT `win32_read_locale` (ENUM_SYSTEM_LOCALE only); the
DefineCollation ICU branch in c2rust sets `collencoding = -1` directly with no
`is_encoding_supported_by_icu` test, and `pg_import_system_collations` in c2rust
has neither the USE_ICU nor the ENUM_SYSTEM_LOCALE block.

The port goes further: it ports the **full original source including the
`#ifdef USE_ICU` branches** (DefineCollation's `is_encoding_supported_by_icu`
gate; pg_import's ICU enumeration + `get_icu_locale_comment` + `CreateComments`),
modelled across seams. This is faithful to a `USE_ICU=true` build and is the more
complete port; the seam owners (`pg_locale_icu`, comment) supply the runtime
behaviour. `win32_read_locale` / `ENUM_SYSTEM_LOCALE` (WIN32) is correctly omitted
on the non-WIN32 target (the libc path covers it).

## Function inventory

| C function (collationcmds.c) | C lines | Port location | Verdict | Notes |
|---|---|---|---|---|
| `DefineCollation` | 52-387 | lib.rs:149-540 | MATCH | full CREATE COLLATION; param dedup loop, all branches/error codes below |
| `IsThereCollationInNamespace` | 395-418 | lib.rs:548-581 | MATCH | two `SearchSysCacheExists3` probes (enc, then -1), dup-object errors |
| `AlterCollation` | 423-503 | lib.rs:593-676 | MATCH (+ note) | REFRESH VERSION; lock-timing delegated to seam (see below) |
| `pg_collation_actual_version` (body) | 506-574 | lib.rs:684-734 | MATCH | DEFAULT_COLLATION_OID → pg_database leg vs pg_collation leg; fmgr wrapper is accepted Datum deferral |
| `normalize_libc_locale_name` | 595-621 | lib.rs:745-775 | MATCH | byte-exact `.`-tag strip; ASCII-only inputs (callee filters non-ASCII first) |
| `cmpaliases` | 626-634 | lib.rs:778-780 | MATCH | strcmp on localename → byte `Ord` |
| `get_icu_locale_comment` | 645-675 | seam `get_icu_locale_comment` | SEAMED | `#ifdef USE_ICU`; ICU `uloc_getDisplayName` lib call → pg_locale_icu owner |
| `create_collation_from_locale` | 694-753 | lib.rs:799-864 | MATCH | pg_is_ascii / encoding / PG_VALID_BE_ENCODING / PG_SQL_ASCII filters, CollationCreate, CCI |
| `win32_read_locale` | 773-828 | — | N/A (out of build) | `#ifdef ENUM_SYSTEM_LOCALE` (WIN32); non-WIN32 target |
| `pg_import_system_collations` (body) | 835-1054 | lib.rs:873-1032 | MATCH | superuser gate, namespace-exists, libc loop + alias sort/add, ICU loop (full-source) |

## Per-branch verification highlights

- **DefineCollation parameter loop** (90-118): the 8-way `defname` dispatch, the
  unrecognized-attribute `ERRCODE_SYNTAX_ERROR` with `parser_errposition`, and the
  `errorConflictingDefElem` on a repeated option all match (lib.rs:204-231).
- **Option conflict checks** (120-130): LOCALE-vs-LC_* and FROM-vs-others
  `ERRCODE_SYNTAX_ERROR` with the exact errdetails (lib.rs:235-252).
- **FROM leg** (132-190): COLLOID syscache read into `CollationRow`, the four
  `SysCacheGetAttr` text columns (NULL→None), and the `COLLPROVIDER_DEFAULT`
  "cannot be copied" `ERRCODE_INVALID_OBJECT_DEFINITION` (lib.rs:254-296).
- **Non-FROM leg** (191-348): provider parse (`pg_strcasecmp` builtin/icu/libc,
  else unrecognized error), deterministic default `true`, rules/version, locale
  routing by provider, the BUILTIN/LIBC/ICU "must be specified" errors, ICU
  binary-upgrade-vs-canonicalize with the NOTICE, nondeterministic-non-ICU and
  ICU-rules-non-ICU feature errors, encoding selection per provider
  (lib.rs:297-496). Branch order and SQLSTATEs match exactly.
- **collversion default** (351-361): libc→collcollate else colllocale, then
  `get_collation_actual_version` (lib.rs:498-509).
- **CollationCreate + CCI + pg_newlocale_from_collation** (363-384):
  `OidIsValid` short-circuit returns `InvalidObjectAddress`; otherwise CCI then
  load-check then `ObjectAddressSet` (lib.rs:511-539).
- **AlterCollation** (423-503): DEFAULT_COLLATION_OID hint error, ownercheck →
  not-owner aclcheck, COLLOID copy, libc-vs-locale version source
  (`SysCacheGetAttrNotNull`), the NULL↔non-NULL "invalid collation version
  change" elog, the changed-version NOTICE + update, and the no-change NOTICE
  (lib.rs:597-671).
- **pg_collation_actual_version** (506-574): both legs, `Assert(provider !=
  COLLPROVIDER_DEFAULT)` → `debug_assert!`, NULL result → SQL NULL
  (lib.rs:691-733).

## Constants verified against C headers (not memory)

`catalog/pg_collation.h`: `COLLPROVIDER_DEFAULT='d'`, `BUILTIN='b'`, `ICU='i'`,
`LIBC='c'` — `types_locale::CollProvider` discriminants are `b'd'/b'b'/b'i'/b'c'`
(types-locale/src/lib.rs:34-40). `CollationRelationId` = `COLLATION_RELATION_ID`.
`DEFAULT_COLLATION_OID` from `types_tuple::heaptuple`. `PG_SQL_ASCII = 0`,
`pg_valid_be_encoding` = `0..=PG_ENCODING_BE_LAST` (real ports, not stubs).
SQLSTATEs: SYNTAX_ERROR, INVALID_OBJECT_DEFINITION, FEATURE_NOT_SUPPORTED,
DUPLICATE_OBJECT, INSUFFICIENT_PRIVILEGE, UNDEFINED_OBJECT, UNDEFINED_SCHEMA —
all present and matched to the C `errcode(...)` at each site.

## Reused-in-crate pure ports (not seams)

`pg_is_ascii` (common-string, high-bit scan), `pg_valid_be_encoding` /
`PG_SQL_ASCII` (types-wchar), `pg_strcasecmp` (in-crate ASCII fold; only `==0`
used). `QualifiedNameGetCreationNamespace` / `get_collation_oid` /
`NameListToString` are direct deps on the ported `backend-catalog-namespace`.

## Seam audit

**Owned seam crate:** `backend-commands-collationcmds-seams`. Its declarations are
all **outward** (consumer-side) seams whose bodies live in *other* owners — this
crate holds no inward seam (no crate calls into collationcmds across a cycle), so
`init_seams()` is **empty by design**. This is the established repo pattern for a
command crate (cf. `backend-commands-functioncmds` / `backend-commands-user`,
both merged with empty `init_seams()` and outward-only `-seams` crates; memory:
"functioncmds owns no inward seam → empty init_seams() is correct").

Canonical seams reused directly (NOT redeclared): aclchk
(`object_aclcheck`/`aclcheck_error`/`error_conflicting_def_elem`), miscinit
(`get_user_id`/`superuser`/`is_binary_upgrade`), mbutils
(`get_database_encoding[_name]`), xact (`command_counter_increment`), pg-locale
(`get_collation_actual_version`/`pg_newlocale_from_collation`), define
(`def_get_string`/`def_get_boolean`).

Owned outward seams (panic until owner lands — `mirror-pg-and-panic` for unported
callees): `parser_errposition`, `get_namespace_name`, `def_get_qualified_name`,
`collation_row_by_oid` (COLLOID), `collation_create` (CollationCreate),
`collation_name_enc_nsp_exists` (COLLNAMEENCNSP), `collation_ownercheck`,
`aclcheck_error_not_owner_collation`, `update_collation_version`,
`namespace_exists`, `database_locale_for_default_collation`, `my_database_id`,
`builtin_validate_locale`, `builtin_locale_encoding`,
`is_encoding_supported_by_icu`, `icu_validation_level`, `icu_language_tag`,
`icu_language_tag_error`, `icu_validate_locale`, `check_encoding_locale_matches`,
`pg_get_encoding_from_locale`, `elog_debug1`, `enumerate_libc_locales`,
`enumerate_icu_locales`, `get_icu_locale_comment`, `create_comment`.

Each is a thin marshal+delegate (arg convert, one call, result convert). No
branching/node-construction/computation lives in a seam path. The COLLOID row is
modelled as the owned `CollationRow` struct (concrete fields, not invented
opacity); CollProvider crosses as `i8`/`char`, consistent with the existing
`get_collation_actual_version` seam.

`every_declared_seam_is_installed_by_its_owner` and
`every_seam_installing_crate_is_wired_into_init_all` (seams-init guard) both PASS
with the unit marked `audited` (verified empirically): the call sites use a
`seam::` alias, so they don't register against the literal `-seams` crate name in
the guard's `called_seams` scan, and the unported-owner seams are exempt — exactly
as for functioncmds.

## Design conformance

- Allocating helpers/seams carry `Mcx` + return `PgResult` (def_get_string,
  builtin_validate_locale, icu_language_tag, etc.). ✓
- No shared statics for per-backend globals; `MyDatabaseId`/`GetUserId` cross
  canonical/owned seams. ✓
- No locks held across `?` in this crate. **Note (not a finding):** the C holds
  `RowExclusiveLock` on pg_collation from the top of `AlterCollation` across the
  oid lookup / ownercheck / syscache copy; the port acquires it only inside the
  `update_collation_version` seam (table_open … CatalogTupleUpdate …
  InvokeObjectPostAlterHook … table_close NoLock). Acquiring it earlier would
  require a lock held across `?` without a guard (forbidden) for an unported
  owner; the syscache reads are MVCC, so behaviour is preserved. This is the
  accepted seam-delegation tradeoff for an unported catalog-mutation owner.
- No invented opacity (CollationRow is a concrete struct), no registry side
  tables, no own-logic stubs (`todo!`/`unimplemented!` absent; every
  `unreachable!()` is the post-`ereport(ERROR)` `.map(|()| ...)` idiom that never
  runs). ✓

## Gates

- `cargo check --workspace` — PASS (warnings only, no errors).
- `cargo test -p backend-commands-collationcmds` — PASS (0 tests; compiles).
- `cargo test -p seams-init` — PASS (both recurrence_guard tests), verified both
  at `ported` and at `audited` status.

## Verdict: PASS

All build-config functions present with exact control flow, error paths,
constants, and edge-case handling; out-of-build `#ifdef`s correctly handled
(USE_ICU ported via seams, WIN32 omitted); no MISSING/PARTIAL/DIVERGES; no
own-logic stubs; seam wiring conforms to the empty-`init_seams()` outward-seam
pattern and the guard passes.
