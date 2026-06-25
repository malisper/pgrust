# Audit: backend-catalog-pg-class

- **Verdict: PASS**
- Date: 2026-06-13
- Model: claude-opus-4-8[1m]
- Unit: `backend-catalog-pg-class` (c_sources: `*/pg_class.c`)
- Branch: `port/backend-catalog-pg-class`

Independent function-by-function audit re-derived from the C source
(`postgres-18.3/src/backend/catalog/pg_class.c`), the c2rust rendering
(`c2rust-runs/backend-catalog-probe-pg_class/src/pg_class.rs`), and the Rust port
(`crates/backend-catalog-pg-class/src/lib.rs`).

## 1. Function inventory

`pg_class.c` contains exactly one function definition. The c2rust run renders
exactly the same single `#[no_mangle]` function — no statics, no inline helpers,
no `#if`-gated alternatives.

| # | C function (location) | Port location | Verdict |
|---|---|---|---|
| 1 | `errdetail_relkind_not_supported(char relkind)` (pg_class.c:23-52) | `backend-catalog-pg-class/src/lib.rs:errdetail_relkind_not_supported` | MATCH |

## 2. Per-function comparison

### `errdetail_relkind_not_supported` — MATCH

- **Control flow:** C is a `switch (relkind)` with 10 explicit `case` arms plus a
  `default`. The port is a `match relkind` with the identical 10 arms plus `_`.
  Every arm has a counterpart; arm ordering is irrelevant (disjoint constants).
- **The 10 recognized arms** each return a fixed errdetail string. All 10 message
  strings were compared byte-for-byte against the C `errdetail(...)` literals and
  the c2rust byte-literals — identical, including the exact words ("TOAST tables",
  "materialized views", "composite types", "partitioned tables/indexes").
- **Constants:** the match keys are the `RELKIND_*` constants imported from
  `types-tuple`. Verified against `src/include/catalog/pg_class.h:167-176`:
  RELATION `'r'`, INDEX `'i'`, SEQUENCE `'S'`, TOASTVALUE `'t'`, VIEW `'v'`,
  MATVIEW `'m'`, COMPOSITE_TYPE `'c'`, FOREIGN_TABLE `'f'`,
  PARTITIONED_TABLE `'p'`, PARTITIONED_INDEX `'I'`. `types-tuple/src/access.rs:11-20`
  defines all ten with exactly these byte values. A port unit test
  (`relkind_constants_match_postgres_header`) re-asserts each value.
- **Error path:** the C `default` arm does
  `elog(ERROR, "unrecognized relkind: '%c'", relkind)`. The port returns
  `Err(PgError::error(format!("unrecognized relkind: '{}'", relkind as char)))`
  with `.with_location(PG_CLASS_C, 49, "errdetail_relkind_not_supported")`.
  - Severity: `PgError::error` → level `ERROR` (21), matching `elog(ERROR, ...)`
    (c2rust shows elevel 21). `elog` has no SQLSTATE argument; it defaults to
    `XX000` internal-error, which the value model represents as a plain
    `PgError::error` with no explicit sqlstate — correct.
  - Message: `unrecognized relkind: '%c'` with `relkind` formatted as a char →
    `relkind as char`. Matches.
  - Location: line 49 is the `elog(ERROR, ...)` statement (verified by reading
    pg_class.c:48-50). Funcname matches. Filename `src/backend/catalog/pg_class.c`
    uses the repo's clean relative-path convention; c2rust's `../src/...` prefix
    is a build-directory artifact, not the canonical path.
- **Return value semantics:** C `errdetail()` returns `int` (always 0) and
  registers the detail text on the in-progress `ereport` as a side effect; the
  recognized arms `return errdetail(...)`. In the repo's value-based error model
  there is no in-progress global ereport, so the function returns the detail
  String by value for the caller to fold into its own ereport. This is the
  established repo convention for `errdetail()`-returning helpers and preserves
  observable behavior (the same detail text reaches the same error). MATCH.
- **Edge cases:** C `char` is the full byte domain; the port takes `u8` (faithful
  for `char` used as a relkind code — no sign-extension concerns since it is only
  compared for equality and formatted as a char). The `default`/`_` covers every
  non-listed byte identically.

Spot re-derivation of this MATCH verdict (per skill step's auditor check): walked
each of the 11 branches against the c2rust output independently of the port's own
comments; messages, constants, severity, message text, and source line all
reconcile with the C and the header. No divergence found.

## 3. Seam and wiring audit

**Owned seam crates (by C-source coverage):** pg_class.c maps to exactly one seam
crate, `crates/backend-catalog-pg-class-seams`. (`backend-commands-matview-deps-seams`
merely mentions "the pg_class catalog" in a doc comment and declares unrelated
matview-dependency seams — no `errdetail_relkind_not_supported` declaration, no
ownership overlap.)

- `backend-catalog-pg-class-seams` declares exactly one seam:
  `errdetail_relkind_not_supported(relkind: u8) -> PgResult<String>`.
- The owning crate's `init_seams()` contains nothing but a single
  `backend_catalog_pg_class_seams::errdetail_relkind_not_supported::set(...)` —
  every declaration installed, no extra logic. No empty installer.
- `crates/seams-init/src/lib.rs:23` calls `backend_catalog_pg_class::init_seams()`.
- The seam is consumed only by `backend-access-table-table`
  (`crates/backend-access-table-table/src/lib.rs`), justifying its existence: the
  helper lives in the catalog layer while a downstream table-AM caller needs it,
  and the seam breaks the cross-layer reach without a dep cycle. The function
  *body* lives in this crate (not delegated elsewhere), so it is genuinely
  MATCH-in-crate, with the seam being only an outward exposure point.

No seam findings.

## 3b. Design conformance

- **No invented opacity:** `relkind: u8` is the faithful representation of C
  `char`; no stand-in handles or void* layering. (types.md rules 6-7: OK.)
- **Allocating function + PgResult:** the function allocates a `String` and
  returns `PgResult<String>`; the `Err` carries OOM/message-build failure per the
  seam doc. No raw allocation without `PgResult`. No `Mcx` is needed because the
  result is an owned by-value `String`, not arena memory.
- **No shared statics / ambient globals:** none introduced; the function is pure
  on its argument. The only global state is the seam slot, set once by the owner.
- **No locks across `?`, no registry-shaped side tables, no unledgered divergence
  markers.** The errdetail-returns-by-value adaptation is documented in the
  module doc comment and is the standard repo convention, not a silent divergence.

No design-conformance findings.

## 4. Verdict

All functions MATCH; zero seam findings; zero design-conformance findings.
`cargo test -p backend-catalog-pg-class -p backend-catalog-pg-class-seams` passes
(3 tests). **PASS.**
