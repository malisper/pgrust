# Audit: backend-access-common-tupdesc

- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** Claude Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)
- **Branch:** `port/backend-access-common-tupdesc`
- **C source:** `src/backend/access/common/tupdesc.c` (`../pgrust/postgres-18.3/`)
- **c2rust:** `../pgrust/c2rust-runs/backend-access-common-toastdesc/src/tupdesc.rs`
- **Port:** `crates/backend-access-common-tupdesc/src/lib.rs`

This unit is the `tupdesc.c` slice of the combined catalog row 17
`backend-access-common-toastdesc` (`detoast.c,toast_internals.c,tupdesc.c`),
ported as its own crate.

## 1. Function inventory and verdicts

Enumerated from the C source (every definition, including statics/inline) and
cross-checked against the c2rust rendering.

| # | C function (tupdesc.c) | Kind | Port location | Verdict |
|---|---|---|---|---|
| 1 | `populate_compact_attribute_internal` | static inline | `populate_compact_attribute_internal` (lib.rs:134) | MATCH |
| 2 | `populate_compact_attribute` | extern | `populate_compact_attribute` (lib.rs:122) | MATCH |
| 3 | `verify_compact_attribute` | extern, `USE_ASSERT_CHECKING`-only | absent | N/A (debug assert) |
| 4 | `CreateTemplateTupleDesc` | extern | `CreateTemplateTupleDesc` (lib.rs:75) | MATCH |
| 5 | `CreateTupleDesc` | extern | `CreateTupleDesc` (lib.rs:104) | MATCH |
| 6 | `CreateTupleDescCopy` | extern | `CreateTupleDescCopy` (lib.rs:179) | MATCH |
| 7 | `CreateTupleDescTruncatedCopy` | extern | `CreateTupleDescTruncatedCopy` (lib.rs:208) | MATCH |
| 8 | `CreateTupleDescCopyConstr` | extern | `CreateTupleDescCopyConstr` (lib.rs:240) | MATCH |
| 9 | `TupleDescCopy` | extern | `TupleDescCopy` (lib.rs:305) | MATCH |
| 10 | `TupleDescCopyEntry` | extern | `TupleDescCopyEntry` (lib.rs:333) | MATCH |
| 11 | `FreeTupleDesc` | extern | `FreeTupleDesc` (lib.rs:362) | MATCH |
| 12 | `IncrTupleDescRefCount` | extern | `IncrTupleDescRefCount` (lib.rs:378) | MATCH (resowner dissolved, see §3b) |
| 13 | `DecrTupleDescRefCount` | extern | `DecrTupleDescRefCount` (lib.rs:393) | MATCH (resowner dissolved, see §3b) |
| 14 | `equalTupleDescs` | extern | `equalTupleDescs` (lib.rs:412) | MATCH |
| 15 | `equalRowTypes` | extern | `equalRowTypes` (lib.rs:567) | MATCH |
| 16 | `hashRowType` | extern | `hashRowType` (lib.rs:605) | MATCH |
| 17 | `TupleDescInitEntry` | extern | `TupleDescInitEntry` (lib.rs:627) | MATCH |
| 18 | `TupleDescInitBuiltinEntry` | extern | `TupleDescInitBuiltinEntry` (lib.rs:647) | MATCH |
| 19 | `TupleDescInitEntryCollation` | extern | `TupleDescInitEntryCollation` (lib.rs:671) | MATCH |
| 20 | `BuildDescFromLists` | extern | `BuildDescFromLists` (lib.rs:687) | MATCH |
| 21 | `TupleDescGetDefault` | extern | `TupleDescGetDefault` (lib.rs:714) | MATCH |
| 22 | `ResOwnerReleaseTupleDesc` | static | dissolved | N/A (resowner registry dissolved, §3b) |
| 23 | `ResOwnerPrintTupleDesc` | static | dissolved | N/A (resowner DebugPrint dissolved, §3b) |
| 24 | `ResourceOwnerRememberTupleDesc` | static inline | dissolved | N/A (resowner registry dissolved, §3b) |
| 25 | `ResourceOwnerForgetTupleDesc` | static inline | dissolved | N/A (resowner registry dissolved, §3b) |

`tupdesc_resowner_desc` (the `ResourceOwnerDesc` static) is the data backing
the four dissolved resowner functions; dissolved with them.

## 2. Per-function notes (spot-checked verdicts)

- **`populate_compact_attribute_internal`** — `attcacheoff=-1`,
  `attispackable = attstorage != TYPSTORAGE_PLAIN`,
  `attgenerated = (attgenerated != '\0')`, the `attnullability` ternary
  (`!attnotnull -> UNRESTRICTED`; else `IsCatalogRelationOid(attrelid) -> VALID`;
  else `UNKNOWN`), and the `attalign` switch (INT->ALIGNOF_INT=4, CHAR->1,
  DOUBLE->ALIGNOF_DOUBLE=8, SHORT->ALIGNOF_SHORT=2, default->elog ERROR) all
  match line-for-line. `IsCatalogRelationOid` is an outward seam
  (catalog.c owner); the `elog(ERROR, "invalid attalign value: %c")` maps to an
  `ERRCODE_INTERNAL_ERROR` `PgError` with the same message. The C `memset(dst,0)`
  is subsumed by building a fresh `CompactAttribute` with every field assigned.

- **`hashRowType`** — `hash_combine(0, hash_uint32(natts))`,
  `hash_combine(s, hash_uint32(tdtypeid))`, then per-attribute
  `hash_combine(s, hash_uint32(atttypid))`. `hash_combine` re-derived against
  `common/hashfn.h:70`: `a ^= b + 0x9e3779b9 + (a<<6) + (a>>2)` — the port's
  `wrapping_add` chain is byte-identical (C unsigned overflow == wrapping).
  `hash_uint32 == hash_bytes_uint32` is the outward `common-hashfn` seam.

- **`equalTupleDescs`** — all 18 per-attribute field comparisons reproduced in
  order; `attname` via `NameStr` byte compare; `atthasmissing` intentionally
  ignored (matches C comment); the `attnotnull -> compare attnullability` guard
  reproduced (C debug `Assert`s on the UNKNOWN invariant dropped, release-build
  no-ops). Constraint block: `has_not_null` / `has_generated_stored` /
  `has_generated_virtual` / `num_defval` / per-`adnum`+`adbin` defval compare /
  `missing` present-and-per-attr (`am_present` + `datumIsEqual` == owned
  `TupleValue` `PartialEq`) / `num_check` / per-check name+bin+enforced+valid+
  noinherit. The `(Some,Some)/(None,None)/_` match reproduces C's
  `constr1!=NULL ... else if constr2!=NULL return false`.

- **`TupleDescInitEntry`** — the type-dependent field writes (`atttypid`,
  `attlen`, `attbyval`, `attalign`, `attstorage`, `attcompression =
  InvalidCompressionMethod`, `attcollation`) and the fixed writes (`attrelid=0`,
  `attnum`, `attndims`, the five cleared constraint flags, `attislocal=true`,
  `attinhcount=0`) match. The cache lookup is reordered *before* the field
  writes (vs C writing fields then `SearchSysCache1`); on the miss path C raises
  `cache lookup failed for type %u` and the partially-written descriptor is
  discarded on abort, so the reorder is observationally equivalent. Lookup is
  the `search_type_attr_info` outward seam (syscache owner) returning the
  `Form_pg_type` slice as `PgTypeInfo`.

- **`TupleDescInitBuiltinEntry`** — the hard-coded builtin table verified
  value-by-value against tupdesc.c:964–1014: TEXT/TEXT[] (len -1, !byval,
  TYPALIGN_INT, EXTENDED, DEFAULT_COLLATION_OID), BOOL (1, byval, CHAR, PLAIN,
  Invalid), INT4 (4, byval, INT, PLAIN, Invalid), INT8 (8,
  `byval=FLOAT8PASSBYVAL=1`, DOUBLE, PLAIN, Invalid), OID (4, byval, INT, PLAIN,
  Invalid), default -> `elog(ERROR, "unsupported type %u")`. Name is required
  (the C `Assert(attributeName != NULL)`), `attcompression =
  InvalidCompressionMethod`.

- **`namestrcpy`** helper re-derived against `utils/adt/name.c`:
  `strncpy(dst, str, NAMEDATALEN)` + forced `dst[NAMEDATALEN-1]='\0'` (truncate
  to NAMEDATALEN-1, zero-pad). The port's `fill(0)` + copy
  `min(len, NAMEDATALEN-1)` is identical on every input length.

- **Constants** verified against PG headers (not memory): RECORDOID=2249,
  BOOLOID=16, INT4OID=23, INT8OID=20, OIDOID=26, TEXTOID=25, TEXTARRAYOID=1009,
  DEFAULT_COLLATION_OID=100, ATTNULLABLE_{UNRESTRICTED='f',UNKNOWN='u',VALID='v'},
  TYPALIGN_{CHAR='c',SHORT='s',INT='i',DOUBLE='d'}, TYPSTORAGE_{PLAIN='p',
  EXTENDED='x'}, ALIGNOF_{SHORT=2,INT=4,DOUBLE=8}, PG_INT16_MAX=32767,
  InvalidCompressionMethod=0, FLOAT8PASSBYVAL=1, NAMEDATALEN.

- C release-build `Assert`s (`natts>=0`, `attno in [1,natts]`, `attdim in
  [0,PG_INT16_MAX]`, `natts<=tupdesc->natts`, `tdrefcount` bounds) are rendered
  as fail-fast `ERRCODE_INTERNAL_ERROR` results / `debug_assert!`s rather than
  release-build UB — the happy path is identical.

## 3. Seam audit

**Owned seam crates (by C-source coverage):** the only seam crate mapping to a
C file in this unit's `c_sources` (`tupdesc.c`) is
`crates/backend-access-common-tupdesc-seams`. It declares four seams:
`hash_row_type`, `equal_row_types`, `create_tupledesc_copy`,
`create_tuple_desc_copy`. All four are installed by this crate's `init_seams()`
(lib.rs:899–904), and `seams-init::init_all()` calls
`backend_access_common_tupdesc::init_seams()` (seams-init/src/lib.rs:11). No
uninstalled owned seam; installer contains nothing but `set()` calls (plus a
thin by-value->`PgBox` adapter `create_tupledesc_copy_seam`, which is one call +
one `alloc_in`, no branching/computation). No `set()` outside the owner. Pass.

**Outward seam calls** (all thin marshal+delegate, each a real cross-unit dep):
- `backend_catalog_catalog_seams::is_catalog_relation_oid` (catalog.c owner) —
  one bool predicate inside `populate_compact_attribute_internal`.
- `backend_utils_cache_syscache_seams::search_type_attr_info` (syscache.c owner)
  — the `SearchSysCache1(TYPEOID,...)` + `GETSTRUCT` projection for
  `TupleDescInitEntry`; returns `PgTypeInfo`, caller raises the `cache lookup
  failed` error on `None`.
- `common_hashfn_seams::hash_bytes_uint32` (hashfn.c owner) — the `hash_uint32`
  primitive.
- `backend_nodes_read_seams::string_to_node` (read.c owner) — `stringToNode` in
  `TupleDescGetDefault`.

No outward seam path carries branching, node construction, or computation beyond
argument/result conversion. No function body was replaced by a "delegate
elsewhere" seam — every tupdesc algorithm lives in this crate.

## 3b. Design conformance

- **Refcount / resource-owner machinery.** The four resowner functions
  (`ResOwnerRelease/PrintTupleDesc`, `ResourceOwnerRemember/ForgetTupleDesc`) and
  the `ResourceOwnerEnlarge`+`Remember`/`Forget` calls inside
  `Incr`/`DecrTupleDescRefCount` are **dissolved**, not silently stubbed. This is
  the sanctioned repo architecture: `docs/query-lifecycle-raii.md` records
  "`CurrentResourceOwner` + the resowner registry -> gone — RAII guards + explicit
  owner values," and `backend-utils-resowner-all` is explicitly not ported as a
  pin registry (a flat global registry is the forbidden ambient-state /
  registry-shaped-side-table anti-pattern, skill §3b). The **load-bearing logic
  — the `tdrefcount` integer and free-at-zero in `Decr` — is fully preserved**;
  only the ambient-global cleanup hook dissolves. Consistent with the relcache
  port's treatment of `tdrefcount`. Not a finding.
- No invented opacity (types.md 6-7): `PgTypeInfo` is a real `Form_pg_type`
  field slice in `types-tuple`, not a stand-in handle; `TupleDescData`/
  `Form_pg_attribute`/`CompactAttribute`/`TupleConstr` are real owned structs.
  The handle-based `tupdesc-pc-seams` are deliberately *not* installed (loud
  panic) rather than resolved behind an invented `TupleDescHandle` — correct per
  "opacity inherited, never introduced."
- Allocating functions take `Mcx` and return `PgResult` (`CreateTemplate...`,
  `Create...Copy*`, `BuildDescFromLists`, `TupleDescGetDefault`,
  `create_tupledesc_copy` seam).
- No shared statics for per-backend globals; no ambient-global seams introduced;
  no locks held across `?`.

## 4. Build / test

`cargo test -p backend-access-common-tupdesc`: 18 unit tests pass, 0 failures.

## Verdict

**PASS.** Every tupdesc.c function is MATCH or properly accounted for (debug
asserts / dissolved resowner registry per the documented RAII architecture). The
single owned seam crate is fully installed and wired into `seams-init`; all
outward seam calls are thin marshal+delegate against real cross-unit deps. Zero
seam findings, zero design-conformance findings.
