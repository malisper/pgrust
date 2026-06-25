# Audit: backend-utils-misc-queryenvironment

- **Unit:** `backend-utils-misc-queryenvironment`
- **C source:** `src/backend/utils/misc/queryenvironment.c` (PostgreSQL 18.3)
- **c2rust reference:** `../pgrust/c2rust-runs/backend-utils-misc-queryenvironment/src/queryenvironment.rs`
- **Port:** `crates/backend-utils-misc-queryenvironment/src/lib.rs`
- **Seam crates introduced by this unit:** `crates/backend-access-table-table-seams`,
  `crates/backend-utils-cache-relcache-seams`
- **Audit date:** 2026-06-12
- **Auditor:** independent re-derivation from C + c2rust (did not trust port comments)

## 1. Function inventory

`queryenvironment.c` defines exactly six functions (no statics, no inline
helpers, no `#if` branches anywhere in the file). The c2rust rendering keeps
the same six. The port adds one private helper (`enr_index`) shared by
`get_ENR`/`unregister_ENR`, audited as part of both.

| # | C function (queryenvironment.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `create_queryEnv` (l.37) | `create_queryEnv` (l.41) | MATCH | C `palloc0(sizeof(QueryEnvironment))` zero-initializes `namedRelList = NIL`; port returns `QueryEnvironment::default()` (empty `Vec`). Caller owns the value, matching the owned-value convention. |
| 2 | `get_visible_ENR_metadata` (l.43) | `get_visible_ENR_metadata` (l.51) | MATCH | Same control flow: `queryEnv == NULL → NULL` (`None` via `query_env?`), `get_ENR` hit → `&enr->md` (owned clone of `md`), miss → `NULL` (`None`). `Assert(refname != NULL)` is structural (`&str`). The clone-instead-of-alias is the repo's owned-value convention; the only C callers (parse_relation.c `scanNameSpaceForENR` path) read the metadata, so behavior is identical. |
| 3 | `register_ENR` (l.66) | `register_ENR` (l.70) | MATCH | C body is one `lappend` plus two Asserts. Port pushes onto the `Vec` (lappend appends at tail — order preserved, verified by test `get_enr_walk_order_preserved`). `Assert(enr != NULL)` structural; duplicate-name Assert rendered as `debug_assert!` with the same predicate (`get_ENR(queryEnv, enr->md.name) == NULL`). A `None` name skips the debug assert; in C a NULL name would be UB inside `strcmp`, so no defined behavior diverges. |
| 4 | `unregister_ENR` (l.79) | `unregister_ENR` (l.88) | MATCH | C: `get_ENR` then `list_delete(match)` — removes the first cell whose pointer equals the first name match, i.e. the first name match itself; no-op on miss. Port: `enr_index` (first name match) + `Vec::remove`; no-op on miss. NULL-env in C is a quiet no-op (get_ENR returns NULL); port takes `&mut`, non-null structural. |
| 5 | `get_ENR` (l.93) | `get_ENR` (l.99) + `enr_index` (l.109) | MATCH | C: NULL env → NULL; `foreach` over `namedRelList`, return first `strcmp(enr->md.name, name) == 0`; else NULL. Port: `iter().position(|enr| enr.md.name.as_deref() == Some(name))` — first match in list order, `None` on miss. `strcmp == 0` ≡ `&str` equality (C names have no interior NULs). NULL-env leg is structural (`&` borrow); the NULL-env path is still exercised at the only NULL-accepting entry point, `get_visible_ENR_metadata`. |
| 6 | `ENRMetadataGetTupDesc` (l.119) | `ENRMetadataGetTupDesc` (l.122) | MATCH (catalog leg SEAMED) | One-and-only-one Assert rendered as `debug_assert!((reliddesc == InvalidOid) != tupdesc.is_none())` — exact predicate. Branch order identical: `tupdesc != NULL` wins (so the both-filled non-assert build behaves the same); else `table_open(reliddesc, NoLock)` / `rd_att` / `table_close(rel, NoLock)` via seams, returning the descriptor fetched between open and close. |

## 2. Constants (verified against headers, not memory)

| Constant | Header | C value | Port value | OK |
|---|---|---|---|---|
| `NoLock` | `storage/lockdefs.h` l.34 | `0` | `types_tuple::access::NoLock = 0` | yes |
| `ENR_NAMED_TUPLESTORE` | `utils/queryenvironment.h` (first/only enum member) | `0` | `types_tuple::access::ENR_NAMED_TUPLESTORE = 0` | yes |
| `InvalidOid` | `postgres_ext.h` | `0` | `types_core::primitive::InvalidOid = 0` | yes |

Type shapes (`utils/queryenvironment.h`): `EphemeralNamedRelationMetadataData`
{name, reliddesc, tupdesc, enrtype, enrtuples} and
`EphemeralNamedRelationData` {md, reldata} are carried field-for-field in
`types_tuple::access`; `QueryEnvironment { namedRelList }` in
`types_tuple::parse`. `reldata` (`void *`) is an opaque optional handle —
faithful to the C "structure for execution-time access", which this unit never
dereferences.

## 3. Seam audit

Outward seams (this unit calls):

- `backend_access_table_table_seams::table_open` / `table_close` — owner
  `access/table/table.c` (unported; would otherwise be a direct dep on a unit
  that depends back into the catalog/relcache stack). Declarations are pure
  signatures; the call sites in `ENRMetadataGetTupDesc` are marshal + one call
  (Oid + LOCKMODE in, Oid out), no logic in the seam path.
- `backend_utils_cache_relcache_seams::relation_rd_att` — owner
  `utils/cache/relcache.c` (unported); models the `relation->rd_att` field
  read since `Relation` crosses seams as its Oid. Thin: one call, returns the
  `TupleDesc`.
- Calling any of these before the owners land panics loudly
  (`seam_core::seam!` uninstalled-call panic) — acceptable per the
  unported-callee rule; no logic is absent from this crate.

Owned seams / wiring:

- This unit owns no seams. `init_seams()` is empty, exists, and is invoked by
  `seams_init::init_all()` (one line, sorted position correct).
- No `set()` calls anywhere outside seam-core's own tests (grep-verified), so
  no out-of-owner installation and no logic leaked into seam or init crates.

## 4. Build / test gate

- `cargo test -p backend-utils-misc-queryenvironment`: 6 passed, 0 failed.
- `cargo check --workspace`: clean.

## 5. Spot-check of MATCH verdicts

Re-derived `get_ENR` and `ENRMetadataGetTupDesc` line-by-line against both the
C and the c2rust rendering (including c2rust's expanded `foreach` state
machine): list order, first-match semantics, branch order on the
`tupdesc`/`reliddesc` legs, and the lock mode constant all confirmed.

## Verdict

**PASS** — all 6 functions MATCH (catalog leg of `ENRMetadataGetTupDesc`
properly SEAMED), zero seam findings.
