# Audit: backend-utils-adt-pg-locale-builtin (pg_locale_builtin.c + regc_pg_locale.c BUILTIN legs)

Verdict: **PASS**

Sources: `pg_locale_builtin.c`, `regc_pg_locale.c` (BUILTIN cases), c2rust `probe-adt-pg_locale_builtin`.

## Function inventory & verdicts

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `initcap_wbnext` (static) | pg_locale_builtin.c:53 | `word_boundaries` | MATCH | Word boundary at offset 0 (first call) + each `pg_u_isalnum(posix)` flip + `len` sentinel. Port pre-computes the offset list; build loop calls one-per-call, identical sequence. UTF-8 byte offsets equal (`char_indices` vs `utf8_to_unicode`+`unicode_utf8len`). NUL-truncation consistent (src_str + build_case both cut at `\0`). |
| `strlower_builtin` | :79 | `strlower_builtin` | MATCH | `unicode_strlower(src, casemap_full)`; full re-resolved from collid. |
| `strtitle_builtin` | :86 | `strtitle_builtin` | MATCH | posix = `!casemap_full`; boundaries via `word_boundaries`; `unicode_strtitle`. |
| `strupper_builtin` | :104 | `strupper_builtin` | MATCH | `unicode_strupper(src, casemap_full)`. |
| `strfold_builtin` | :112 | `strfold_builtin` | MATCH | `unicode_strfold(src, casemap_full)`. |
| `create_pg_locale_builtin` | :121 | `create_pg_locale_builtin` + `builtin_locale_name` | MATCH (1 minor error-path note) | DEFAULT_COLLATION_OID(=100)→datlocale; else colllocale; builtin_validate_locale(GetDatabaseEncoding(),locstr); provider=BUILTIN, deterministic=true, collate_is_c=true, ctype_is_c=(locstr=="C"), casemap_full=(locstr=="PG_UNICODE_FAST"); is_default left false (consumer sets it). |
| `get_collation_actual_version_builtin` | :169 | same | MATCH | "1" for C / C.UTF-8 / PG_UNICODE_FAST; else ERRCODE_WRONG_OBJECT_TYPE "invalid locale name". Correctly uses `C.UTF-8` (not the `C.UTF8` alias). |
| `pg_wc_isdigit/isalnum/ispunct` BUILTIN | regc_pg_locale.c | `regex_wc_isclass_builtin` Digit/Alnum/Punct | MATCH | take posix = `!casemap_full`. |
| `pg_wc_isalpha/isupper/islower/isgraph/isprint/isspace` BUILTIN | regc_pg_locale.c | `regex_wc_isclass_builtin` rest | MATCH | no posix arg. |
| `pg_wc_toupper` BUILTIN | regc_pg_locale.c:266 | `regex_wc_toupper_builtin` | MATCH | `unicode_uppercase_simple(c)`. |
| `pg_wc_tolower` BUILTIN | regc_pg_locale.c:300 | `regex_wc_tolower_builtin` | MATCH | `unicode_lowercase_simple(c)`. |

## Seam audit

- Owned seam crate: `backend-utils-adt-pg-locale-builtin-seams` (only per-file crate for pg_locale_builtin.c). 9 decls, 9 `set()` in `init_seams()`, nothing else. `seams-init::init_all()` calls `backend_utils_adt_pg_locale_builtin::init_seams()` (lib.rs:539). No uninstalled seam, no set() outside owner.
- Outward seam calls all justified by real cycles to unported/ambient owners: `cc::builtin_validate_locale` (pg_locale.c owner), `mb::get_database_encoding` (mbutils), `catalog::{database,collation}_locale_row` (syscache), `init_small::my_database_id`. Each is thin marshal+delegate. Catalog reads via the catalog-seams facade = the idiomatic analog of `SearchSysCache1`.

## Design conformance

- Allocating fns (`create_pg_locale_builtin`, `get_collation_actual_version_builtin`, `str*_builtin`) take `Mcx<'mcx>` and return `PgResult` — conforms. Flag core via `mcx::alloc_in`, version via `mcx::slice_in`.
- No shared statics, no `todo!`/`unimplemented!`, no `unreachable!`.
- Seam-shape adaptations are the sanctioned "fix the wrong signature" path: trimmed `PgLocaleStruct` carries no `info` union, so `casemap_full` travels in `PgLocaleBuiltinResult` and posix is passed explicitly to `regex_wc_isclass_builtin`; `DatabaseLocaleRow.locale` (datlocale) added with matching projection update; consumer `LocaleInfo::Builtin{casemap_full}`. All declared-and-installed consistently.

## Notes (non-blocking)

- NULL-datlocale error path: C `SysCacheGetAttrNotNull(Anum_pg_database_datlocale)` on a NULL attr throws "unexpected null value in system cache" (internal error); the port returns "cache lookup failed for database %u". Path is only reachable when the database `datlocprovider` is builtin, where initdb/CREATE DATABASE always sets datlocale NOT NULL, so the predicate is effectively unreachable. Low severity, message-only divergence on an unreachable branch.
