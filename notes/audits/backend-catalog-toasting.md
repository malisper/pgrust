# Audit: backend-catalog-toasting

**Verdict: PASS**
Date: 2026-06-13
Model: Claude Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
Auditor: independent from-scratch re-audit (audit-crate SKILL), branch
`port/backend-catalog-toasting`.

Unit: `backend-catalog-toasting` (`src/backend/catalog/toasting.c`, PostgreSQL 18.3)
Crate: `crates/backend-catalog-toasting`
Sources audited (re-derived independently, port comments not trusted):
- C: `pgrust/postgres-18.3/src/backend/catalog/toasting.c`
- c2rust: `pgrust/c2rust-runs/backend-catalog-toasting/src/toasting.rs`
- Rust port: `crates/backend-catalog-toasting/src/lib.rs`

## 1. Function inventory

`toasting.c` defines exactly 7 functions (4 exported, 3 static). Cross-checked
against the c2rust rendering, which kept all 7 plus rendered the two
`access/tableam.h` static-inline dispatch helpers (`table_relation_toast_am` at
c2rust:3163, `table_relation_needs_toast_table` at c2rust:3157) and inlined the
`RelationIsMapped` macro directly into `create_toast_table` (c2rust:3441-3446,
no separate function). All 7 toasting.c functions are present in the port; the
two tableam dispatchers are SEAMED, `RelationIsMapped` is `Relation::is_mapped()`.

| # | C function | C loc | Port loc | Verdict |
|---|---|---|---|---|
| 1 | `AlterTableCreateToastTable` | toasting.c:57 | lib.rs:68 | MATCH |
| 2 | `NewHeapCreateToastTable` | toasting.c:63 | lib.rs:78 | MATCH |
| 3 | `NewRelationCreateToastTable` | toasting.c:70 | lib.rs:89 | MATCH |
| 4 | `BootstrapToastTable` | toasting.c:97 | lib.rs:123 | MATCH |
| 5 | `CheckAndCreateToastTable` (static) | toasting.c:77 | lib.rs:94 | MATCH |
| 6 | `create_toast_table` (static) | toasting.c:126 | lib.rs:166 | MATCH |
| 7 | `needs_toast_table` (static) | toasting.c:400 | lib.rs:424 | MATCH |

## 2. Per-function detail

**1-3 (public variants).** Pure forwarders to `CheckAndCreateToastTable` with
the exact constant arguments: `Alter…` → `(check=true, OIDOldToast=InvalidOid)`;
`NewHeap…` → `(check=false, OIDOldToast)`; `NewRelation…` →
`(AccessExclusiveLock, check=false, InvalidOid)`. Matches c2rust:3177-3205.

**4 `BootstrapToastTable`.** `table_openrv(makeRangeVar(NULL, relName, -1),
AccessExclusiveLock)`; relkind guard (`!= RELKIND_RELATION && != RELKIND_MATVIEW`
→ `elog(ERROR, "\"%s\" is not a table or materialized view")`); calls
`create_toast_table(rel, toastOid, toastIndexOid, (Datum)0, AccessExclusiveLock,
false, InvalidOid)`; if it returns false → `elog(ERROR, "\"%s\" does not require
a toast table")`; `table_close(rel, NoLock)`. Port mirrors all branches, both
ERRORs (severity ERROR, plain elog/`errmsg_internal` → no SQLSTATE, default
XX000), `makeRangeVar` builds a permanent inheritance-enabled RangeVar with no
catalog/schema. Matches c2rust:3227-3312.

**5 `CheckAndCreateToastTable`.** `table_open(relOid, lockmode)`,
`create_toast_table(rel, InvalidOid, InvalidOid, reloptions, lockmode, check,
OIDOldToast)` (return value discarded), `table_close(rel, NoLock)`. Matches
c2rust:3206-3225.

**6 `create_toast_table` (the workhorse).** Verified branch-by-branch against
c2rust:3313-3672:
- early `return false` when `reltoastrelid != InvalidOid`;
- `!IsBinaryUpgrade` → `!needs_toast_table` returns false; else (binary upgrade)
  `!OidIsValid(binary_upgrade_next_toast_pg_class_oid)` returns false. Both
  globals sourced from `backend-utils-init-small::globals` (proper per-backend
  global owner; not a shared static in this crate);
- `check && lockmode != AccessExclusiveLock` → `elog(ERROR,
  "AccessExclusiveLock required to add toast table.")`;
- `snprintf(buf, NAMEDATALEN, "pg_toast_%u"/"pg_toast_%u_index", relOid)` →
  `snprintf_name` truncates to NAMEDATALEN-1 (=63) bytes; toast names are pure
  ASCII so byte==char truncation is identical to C and `String::truncate` is
  always on a char boundary;
- 3-attr template tupledesc (chunk_id OID=26, chunk_seq INT4=23, chunk_data
  BYTEA=17), all three attstorage=`'p'` TYPSTORAGE_PLAIN, attcompression=`'\0'`
  InvalidCompressionMethod;
- namespace: `isTempOrTempToastNamespace(relnamespace) ? GetTempToastNamespace()
  : PG_TOAST_NAMESPACE(=99)`;
- `shared_relation = relisshared`; `mapped_relation = RelationIsMapped(rel)` →
  `is_mapped()` = `RELKIND_HAS_STORAGE(r/i/S/t/m) && relfilenode ==
  InvalidRelFileNumber`, verified identical to the c2rust-inlined macro;
- `heap_create_with_catalog(...)` with the full 21-arg payload incl. `accessmtd
  = table_relation_toast_am(rel)`, `relkind = RELKIND_TOASTVALUE`, `oncommit =
  ONCOMMIT_NOOP`, `use_user_acl=false`, `allow_system_table_mods=true`,
  `is_internal=true`, `relrewrite = OIDOldToast`, `typaddress = NULL`;
  `Assert(toast_relid != InvalidOid)` → `assert!`;
- `CommandCounterIncrement`; `table_open(toast_relid, ShareLock)`;
- IndexInfo built with the exact 2-key unique-primary shape (NumIndexAttrs=2,
  IndexAttrNumbers=[1,2], ii_Am=BTREE_AM_OID=403, ii_Unique=true, all other flags
  false/0); collationIds=[Invalid,Invalid], opclassIds=[OID_BTREE_OPS_OID=1981,
  INT4_BTREE_OPS_OID=1978], coloptions=[0,0];
- `index_create(toast_rel, toast_idxname, toastIndexOid, …, list_make2("chunk_id",
  "chunk_seq"), BTREE_AM_OID, reltablespace, …, (Datum)0,
  INDEX_CREATE_IS_PRIMARY(=1<<0), 0, true, true, NULL)`;
- `table_close(toast_rel, NoLock)`;
- `table_open(RelationRelationId=1259, RowExclusiveLock)`;
- normal vs bootstrap split on `IsBootstrapProcessingMode()`: normal does
  SearchSysCacheCopy1 + GETSTRUCT field write + CatalogTupleUpdate; bootstrap
  does systable_inplace_update_begin/finish + GETSTRUCT write. **Both raise
  `elog(ERROR, "cache lookup failed for relation %u", relOid)` in-crate** when
  the tuple is not valid (lib.rs:365-370 and 384-390);
- `heap_freetuple(reltup)` (owned by the syscache-copy seam — the copy never
  crosses the boundary, only the bool result does);
- `table_close(class_rel, RowExclusiveLock)`;
- `!IsBootstrapProcessingMode()` → build base/toast `ObjectAddress`
  (classId=1259, objectSubId=0) and `recordDependencyOn(&toastobject,
  &baseobject, DEPENDENCY_INTERNAL='i')` via the direct dep
  `backend_catalog_pg_depend`;
- final `CommandCounterIncrement`; `return true`.

**7 `needs_toast_table`.** Three early-false guards in order
(RELKIND_PARTITIONED_TABLE; relisshared && !bootstrap; IsCatalogRelation &&
!bootstrap) then `return table_relation_needs_toast_table(rel)`. Matches
c2rust:3673-3690. `IsCatalogRelation(rel)` = `IsCatalogRelationOid(rd_id)` via
the catalog seam.

## 3. Constants verified against sources (not from memory)

- BYTEAOID=17, INT4OID=23, OIDOID=26 — confirmed vs `pg_type.dat` and
  `types-tuple::heaptuple`.
- PG_TOAST_NAMESPACE=99, BTREE_AM_OID=403, OID_BTREE_OPS_OID=1981,
  INT4_BTREE_OPS_OID=1978, INDEX_CREATE_IS_PRIMARY=1<<0 — confirmed vs
  c2rust:3168-3175.
- RELATION_RELATION_ID=1259 (types-core), DEPENDENCY_INTERNAL='i'
  (types-catalog), lock modes NoLock=0/RowExclusiveLock=3/ShareLock=5/
  AccessExclusiveLock=8 (types-storage) — all match c2rust:3134-3147.
- RELKIND_TOASTVALUE='t', RELKIND_RELATION='r', RELKIND_MATVIEW='m',
  RELKIND_PARTITIONED_TABLE='p' — match.

## 4. Seam audit (step 3)

**Owned seam crates: none.** Ownership is by C-source coverage; the unit's only
C source is `toasting.c`, which maps to no `crates/*-seams` crate. There is no
`backend-catalog-toasting-seams` crate. The crate's `init_seams()` is therefore
correctly empty (lib.rs:477) — no owned, uninstalled seams.

**Outward seam calls** (each justified by a real unported dependency; all thin
marshal+delegate, no branching/computation in the seam path):
- `backend-access-common-toastdesc-seams`: `create_template_tuple_desc`,
  `tuple_desc_init_entry` (tupledesc.c, unported).
- `backend-access-table-tableam-seams`: `table_relation_toast_am`,
  `table_relation_needs_toast_table` — inline `rd_tableam` fn-pointer dispatchers
  that do not ereport in C, so the seams correctly return plain values (not
  `PgResult`) per "seam signatures mirror C failure surface".
- `backend-catalog-heap-seams`: `heap_create_with_catalog` (heap.c, unported).
- `backend-catalog-index-seams`: `index_create` (index.c, unported).
- `backend-catalog-indexing-seams`: `set_pg_class_reltoastrelid` and
  `set_pg_class_reltoastrelid_inplace` (indexing.c / inplace-update, unported).
  These return `bool` = `HeapTupleIsValid(reltup)`; the seam delegates only the
  genuine unported callees (syscache copy / inplace begin+finish, the
  `Form_pg_class` GETSTRUCT field write — caller-shaped projection precedent
  because `Form_pg_class` is a trimmed projection that cannot losslessly reform
  the on-disk tuple — and the `CatalogTupleUpdate`/`…finish`). The
  ScanKey/`F_OIDEQ`/`ClassOidIndexId` plumbing for the inplace key lives inside
  the seam owner, not in toasting. **No logic computed in the seam path.**
- `backend-catalog-catalog-seams`: `is_catalog_relation_oid` (catalog.c).
- `backend-access-transam-xact-seams`: `command_counter_increment`.
- `backend-utils-init-miscinit-seams`: `is_bootstrap_processing_mode`.

**Direct deps** (no cycle, so not seamed): `backend-access-table-table`
(table_open/openrv/close), `backend-catalog-namespace`
(isTempOrTempToastNamespace/GetTempToastNamespace), `backend-catalog-pg-depend`
(recordDependencyOn).

## 5. Previously-failing finding — resolved

The prior audit recorded `create_toast_table` as **PARTIAL**: the
`elog(ERROR, "cache lookup failed for relation %u")` for the
`!HeapTupleIsValid(reltup)` case had been pushed across the
`set_pg_class_reltoastrelid` seam (logic living outside the crate). Independently
re-derived: the seam now returns `bool` (`HeapTupleIsValid`) and **both** the
normal and bootstrap branches raise the `cache lookup failed for relation %u`
`elog(ERROR)` in-crate (lib.rs:365-370, 384-390), with the GETSTRUCT
write/update gated on the same `true` result, exactly mirroring the C control
flow. No absent logic remains. Finding resolved.

## 6. Design conformance (step 3b)

- No invented opacity: `Relation` crosses as the real `types_rel::Relation`;
  pg_class field write stays on the owner's full tuple via the documented
  caller-shaped projection precedent.
- Allocating callees go through `Mcx` + `PgResult` (tupledesc, heap_create,
  index_create, recordDependencyOn, syscache copy all on `Mcx`/return
  `PgResult`); non-allocating non-erroring dispatchers (tableam) correctly do not.
- No shared statics for per-backend globals (binary_upgrade/IsBinaryUpgrade come
  from `backend-utils-init-small`).
- No ambient-global seams, no locks held across `?` without guards (relations
  are RAII handles with explicit `table_close`), no registry-shaped side tables,
  no unledgered divergence markers.

## 7. Build

`cargo build -p backend-catalog-toasting` succeeds clean (no errors;
only unrelated upstream warnings).

## Verdict

**PASS** — all 7 functions MATCH, the two tableam helpers SEAMED per rules,
`RelationIsMapped` faithfully reproduced; zero seam findings (unit owns no seam
crates, `init_seams()` correctly empty); the previously-failing PARTIAL on
`create_toast_table` is resolved (cache-lookup `elog(ERROR)` is in-crate);
constants literal-verified against sources; design-conformant.
