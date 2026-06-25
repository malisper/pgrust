# Audit: common-unicode-category (src/common/unicode_category.c)

C source: postgres-18.3/src/common/unicode_category.c + unicode_category_table.h
Port: crates/common-unicode-category/{src/lib.rs, src/tables.rs}

## Function inventory & verdicts

| C fn (line) | port | verdict | notes |
|---|---|---|---|
| range_search (78/481) | lib.rs range_search | MATCH | binary search over disjoint sorted ranges; port uses half-open `min<max` / `max=mid` variant of C's `max>=min` / `max=mid-1` — equivalent for sorted disjoint ranges. |
| unicode_category (85) | lib.rs unicode_category | MATCH | ASCII fast path via unicode_opt_ascii[code]; else binary search; PG_U_UNASSIGNED fallback. |
| pg_u_prop_alphabetic (111) | MATCH | ascii_property OR range_search(unicode_alphabetic). |
| pg_u_prop_lowercase (122) | MATCH | as above, unicode_lowercase. |
| pg_u_prop_uppercase (133) | MATCH | unicode_uppercase. |
| pg_u_prop_cased (144) | MATCH | ascii prop OR (category_mask & Lt) OR lower OR upper. |
| pg_u_prop_case_ignorable (159) | MATCH | unicode_case_ignorable. |
| pg_u_prop_white_space (170) | MATCH | unicode_white_space. |
| pg_u_prop_hex_digit (181) | MATCH | unicode_hex_digit. |
| pg_u_prop_join_control (192) | MATCH | unicode_join_control. |
| pg_u_isdigit (211) | MATCH | posix: '0'..='9'; else cat==Nd. |
| pg_u_isalpha (220) | MATCH | = pg_u_prop_alphabetic. |
| pg_u_isalnum (226) | MATCH | isalpha OR isdigit. |
| pg_u_isword (232) | MATCH | C PG_U_M_MASK|ND|PC spelled out as Mn|Mc|Me|Nd|Pc; equal membership. |
| pg_u_isupper (243) | MATCH | = prop_uppercase. |
| pg_u_islower (249) | MATCH | = prop_lowercase. |
| pg_u_isblank (255) | MATCH | TAB or cat==Zs. |
| pg_u_iscntrl (262) | MATCH | cat==Cc. |
| pg_u_isgraph (268) | MATCH | not (Cc|Cs|Cn) and not isspace. |
| pg_u_isprint (279) | MATCH | not Cc and (isgraph or isblank). |
| pg_u_ispunct (290) | MATCH | posix-alpha excl; P* mask, +S* when posix. |
| pg_u_isspace (311) | MATCH | = prop_white_space. |
| pg_u_isxdigit (317) | MATCH | posix hex ranges; else Nd or prop_hex_digit. |
| unicode_category_string (332) | MATCH | CATEGORY_NAMES table; "Unrecognized" fallback. |
| unicode_category_abbrev (406) | MATCH | CATEGORY_ABBREVS; "??" fallback. |
| category_bit / category_mask (PG_U_CATEGORY_MASK macro) | MATCH | 1<<category. |

Generated tables (unicode_opt_ascii, unicode_categories, unicode_alphabetic,
unicode_lowercase, unicode_uppercase, unicode_case_ignorable, unicode_white_space,
unicode_hex_digit, unicode_join_control) copied verbatim from the idiomatic base
(derived from unicode_category_table.h).

## Seams & wiring
- Owned seam crate: common-unicode-category-seams (unicode_category -> i32).
  Installed by init_seams() (pg_unicode_category u32 widened `as i32`, exactly as
  the C caller reads `int category`). Wired into seams-init::init_all().
- No other owned seam crates for unicode_category.c.

## Design conformance
- pg_wchar re-signed to a crate-local `pub type pg_wchar = types_core::PgWChar`
  alias (no opaque stand-in). No allocation paths (pure table lookups). No statics
  beyond the const generated tables. No panics in owned logic.

## Verdict: PASS
