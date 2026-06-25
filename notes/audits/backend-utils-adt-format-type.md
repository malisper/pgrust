# Audit: backend-utils-adt-format-type

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Claude Opus 4.8 (1M context)
- Branch: `port/backend-utils-adt-format-type`
- C source: `src/backend/utils/adt/format_type.c` (`c_sources = */format_type.c`)
- c2rust reference: `c2rust-runs/backend-utils-adt-sqlhelpers/src/format_type.rs`
  (this unit was carved out of `backend-utils-adt-sqlhelpers`)
- Port crate: `crates/backend-utils-adt-format-type`

This is an independent re-derivation from the C and the c2rust rendering; the
port's comments and self-review were not trusted.

## 1. Function inventory

Every function defined in `format_type.c` (no statics/inlines omitted; c2rust
kept the same set):

| # | C function (location) | Port location | Verdict |
|---|---|---|---|
| 1 | `format_type` (SQL entry, L59-84) | `lib.rs::format_type` L92 | MATCH |
| 2 | `format_type_extended` (L111-334) | `lib.rs::format_type_extended` L120 | MATCH |
| 3 | `format_type_be` (L342-346) | `lib.rs::format_type_be` L362 | MATCH |
| 4 | `format_type_be_qualified` (L352-356) | `lib.rs::format_type_be_qualified` L370 | MATCH |
| 5 | `format_type_with_typemod` (L361-365) | `lib.rs::format_type_with_typemod` L376 | MATCH |
| 6 | `printTypmod` (static, L370-394) | `lib.rs::print_typmod` L386 | MATCH |
| 7 | `type_maximum_size` (L411-440) | `lib.rs::type_maximum_size` L444 | MATCH |
| 8 | `oidvectortypes` (SQL entry, L446-488) | `lib.rs::oidvectortypes` L485 | MATCH |

Helpers in the port that are not standalone C functions: `is_true_array_type`
(the `IsTrueArrayType` macro, `pg_type.h` L334), `cache_lookup_failed`
(the shared `elog(ERROR, "cache lookup failed for type %u")` body), `pstrdup`
(local `pstrdup`-of-literal helper), `push_i32` (the `%d` of `psprintf`). All
inline-faithful.

## 2. Per-function logic comparison

### format_type (SQL entry)
- `PG_ARGISNULL(0)` -> `PG_RETURN_NULL` mirrored by `type_oid: None -> Ok(None)`.
- `flags = FORMAT_TYPE_ALLOW_INVALID`; `PG_ARGISNULL(1)` -> `typemod = -1`,
  else `typemod = arg`, `flags |= FORMAT_TYPE_TYPEMOD_GIVEN`. Mirrored exactly.
- `cstring_to_text`/`PG_RETURN_TEXT_P` is the fmgr boundary; the entry point
  returns the decoded `Option<PgString>`. MATCH.

### format_type_extended (core)
Branch-by-branch:
- `type_oid == InvalidOid`: INVALID_AS_NULL -> `None`; else ALLOW_INVALID ->
  `"-"`; else falls through to syscache lookup (matches C, which does not return
  in the else case). MATCH.
- First `SearchSysCache1(TYPEOID)` miss: INVALID_AS_NULL -> `None`;
  ALLOW_INVALID -> `"???"`; else `elog(ERROR, "cache lookup failed for type %u",
  type_oid)`. Port `cache_lookup_failed(type_oid)` with `ERRCODE_INTERNAL_ERROR`
  (`XX000`), severity ERROR. SQLSTATE/severity match. MATCH.
- True-array test: `IsTrueArrayType(typeform) && typstorage != TYPSTORAGE_PLAIN`.
  `IsTrueArrayType` = `OidIsValid(typelem) && typsubscript ==
  F_ARRAY_SUBSCRIPT_HANDLER`. Verified against `pg_type.h` L334 and against the
  c2rust inline expansion (L965-967). `TYPSTORAGE_PLAIN = 'p'`. MATCH.
- Array element re-lookup miss: same three-way (`None`/`"???[]"`/`elog`). The
  `elog` argument is the original `type_oid` (C reassigns `type_oid =
  array_base_type` only *after*, at L165); port reassigns at L173, after the
  error path, so `cache_lookup_failed(type_oid)` carries the original oid. MATCH.
- `with_typemod = (flags & TYPEMOD_GIVEN) != 0 && typemod >= 0`. MATCH.
- Special-case `switch(type_oid)`: all 17 arms (BIT/BOOL/BPCHAR/FLOAT4/FLOAT8/
  INT2/INT4/INT8/NUMERIC/INTERVAL/TIME/TIMETZ/TIMESTAMP/TIMESTAMPTZ/VARBIT/
  VARCHAR/JSON) reproduced with identical string literals and identical
  with_typemod / TYPEMOD_GIVEN sub-branches, including the deliberate
  *no-assignment* fallthrough for BIT/BPCHAR with `TYPEMOD_GIVEN` set but
  `typemod < 0` (leaves `buf = None`). MATCH.
- Default (buf == NULL) handling: `!(FORCE_QUALIFY) && TypeIsVisible` ->
  `nspname = NULL` else `get_namespace_name_or_temp(typnamespace)`; then
  `quote_qualified_identifier(nspname, NameStr(typname))`; then if with_typemod,
  `printTypmod(buf, ...)`. MATCH.
- `if (is_array) buf = psprintf("%s[]", buf)` -> `buf.try_push_str("[]")`. MATCH.
- `ReleaseSysCache(tuple)` -> owned-model drop of the copied `TypeFormInfo`
  (the C reads are in-place on the cache tuple; the seam copies out, drop is the
  release). MATCH.

### format_type_be / _be_qualified / _with_typemod
Thin wrappers passing `(-1, 0)`, `(-1, FORCE_QUALIFY)`, `(typemod,
TYPEMOD_GIVEN)` respectively. The port unwraps `Option` with
`cache_lookup_failed` — under these flag sets the `None` (INVALID_AS_NULL) paths
are unreachable, so the unwrap can only fire on the genuine cache-miss `elog`
path C would also raise. Behaviorally identical. MATCH x3.

### printTypmod
- `Assert(typmod >= 0)` -> `debug_assert!` (Assert is debug-only in C too). MATCH.
- `typmodout == InvalidOid`: `psprintf("%s(%d)", typname, typmod)` ->
  `typname` + `'('` + decimal(typmod) + `')'`. `push_i32` forms decimal in an
  11-byte stack buffer (i32 min `-2147483648` is 11 chars incl. sign), no heap.
  MATCH.
- else: `DatumGetCString(OidFunctionCall1(typmodout, Int32GetDatum(typmod)))`
  then `psprintf("%s%s", typname, tmstr)`. Delegated to `typmod_out` seam
  (owner: fmgr), which performs the OidFunctionCall1 + DatumGetCString and the
  strict-null `elog`. Port concatenates `typname + tmstr`. MATCH.

### type_maximum_size
- `typemod < 0 -> -1`. MATCH.
- BPCHAR/VARCHAR: `(typemod - VARHDRSZ) * pg_encoding_max_length(
  GetDatabaseEncoding()) + VARHDRSZ`. `pg_database_encoding_max_length` seam
  folds `pg_encoding_max_length(GetDatabaseEncoding())`. `VARHDRSZ = 4`. MATCH.
- NUMERIC: `numeric_maximum_size(typemod)` (seam). MATCH.
- VARBIT/BIT: `(typemod + (BITS_PER_BYTE-1)) / BITS_PER_BYTE + 2*sizeof(int32)`.
  `BITS_PER_BYTE = 8`, `sizeof(int32) = 4`. Integer division/truncation matches
  C. MATCH.
- default `-1`. MATCH.

### oidvectortypes
- C does `check_valid_oidvector` then `numargs = dim1` (fmgr boundary); port
  takes the validated `values: &[Oid]`, `numargs = values.len()`.
- C grows a `20*numargs+1` palloc buffer with manual `left`/`repalloc`
  bookkeeping and `strcat`. The growth math is purely an allocation-sizing
  optimization with no observable effect on output; the port uses a growable
  `PgString` (buffer growth is the allocator's job). The observable behavior —
  per-element `format_type_extended(values[num], -1, ALLOW_INVALID)`, a `", "`
  separator from element 1 on, concatenation in order — is reproduced 1:1.
  MATCH.

## 3. Seam audit

**Owned seam crates (by C-source coverage of `format_type.c`):**
`backend-utils-adt-format-type-seams` — the only seam crate mapping to this
unit's C file.

Its sole declaration, `format_type_be`, is installed by the crate's
`init_seams()` (`seams.rs`), which contains nothing but one `set()` call.
`seams-init::init_all()` calls `backend_utils_adt_format_type::init_seams()`
(`crates/seams-init/src/lib.rs:59`). No uninstalled owned seam; installer is
non-empty. PASS.

**Outward seam calls** (each to a crate owned by an unported neighbor, justified
by a real dependency cycle; each is thin marshal+delegate, no branching/node
construction/computation on the seam path):

| Seam | Owner | Shape | Notes |
|---|---|---|---|
| `type_form` | syscache-seams | `(Mcx, Oid) -> PgResult<Option<TypeFormInfo>>` | projects the full `Form_pg_type` row this unit reads; projection lives in the owner |
| `type_is_visible` | namespace-seams | `(Mcx, Oid) -> PgResult<bool>` | |
| `get_namespace_name_or_temp` | lsyscache-seams | `(Mcx, Oid) -> PgResult<Option<PgString>>` | |
| `quote_qualified_identifier` | ruleutils-seams | `(Mcx, Option<&str>, &str) -> PgResult<PgString>` | reused |
| `typmod_out` | fmgr-seams | `(Mcx, Oid, i32) -> PgResult<PgString>` | OidFunctionCall1 + strict-null elog in owner |
| `pg_database_encoding_max_length` | mbutils-seams | `() -> i32` | pure |
| `numeric_maximum_size` | numeric-seams | `(i32) -> i32` | pure |

No function body was replaced by a seam call to "elsewhere": the entire
formatting decision tree (invalid handling, array deconstruction, the 17-arm
special-case switch, default qualify/quote/typmod path, the size arithmetic, the
oidvector join) lives in this crate. The seams only fetch raw catalog fields and
raw callee results. Zero seam findings.

## 3b. Design conformance

- **No invented opacity (types.md 6-7):** `TypeFormInfo` is a real projection of
  named `Form_pg_type` columns (`typelem`/`typsubscript`/`typstorage`/
  `typmodout`/`typnamespace`/`typname`), not an opaque stand-in handle. The
  unported callees keep their real C identities behind seams. PASS.
- **Allocating seams carry `Mcx` + `PgResult`:** `type_form`, `typmod_out`,
  `get_namespace_name_or_temp`, `quote_qualified_identifier` all do. The two
  pure-arithmetic seams (`numeric_maximum_size`, `pg_database_encoding_max_length`)
  allocate nothing and correctly take no `Mcx`. PASS.
- **No shared statics for per-backend globals; no ambient-global seams; no locks
  held across `?`; no registry-shaped side tables; no unledgered divergence
  markers.** None present. PASS.
- **`PgResult` failure surface mirrors the C ereport surface** (cache-lookup
  `XX000`, OOM via `try_push*`/`from_str_in`). PASS.

## 4. Verdict

All 8 C functions **MATCH**. One owned seam crate, fully installed and wired.
Seven outward seams, all thin and justified. Zero seam findings, zero design
findings. Constants verified against headers (`BITS_PER_BYTE=8`, `VARHDRSZ=4`,
`F_ARRAY_SUBSCRIPT_HANDLER=6179` via `pg_proc.dat`, `TYPSTORAGE_PLAIN='p'`, and
all 17 type OIDs against `types-tuple` + c2rust). Crate builds; 10 unit tests
pass.

**PASS.**
