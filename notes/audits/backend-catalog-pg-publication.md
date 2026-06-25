# Audit: backend-catalog-pg-publication

**Verdict: PASS (F1+F2 fully ported; F3 row-builder ported, SQL SRF wrapper STOP'd on the unported per-call SRF protocol)**
Date: 2026-06-15
Model: claude-opus-4-8[1m]

Unit: `backend-catalog-pg-publication`
C source: `src/backend/catalog/pg_publication.c` (PostgreSQL 18.3)
Headers: `pg_publication.h`, `pg_publication_rel.h`, `pg_publication_namespace.h`
Port: `crates/backend-catalog-pg-publication/src/lib.rs`
Seam crate: `crates/backend-catalog-pg-publication-seams/src/lib.rs`

Audit is independent: the function inventory was re-derived from the C source;
each function compared C / Rust; every catalog constant verified field-for-field
against the PG 18.3 headers.

## Carrier model

Follows the established per-catalog mutate-carrier pattern (pg_database #315 /
pg_type #319), NOT the src-idiomatic facade (which routes the substrate through
one uninstalled seam — deliberately not copied). Real `heap_form_tuple` over the
relation's cloned `TupleDesc`; real `systable_beginscan`/`getnext`/`endscan` +
`heap_deform_tuple` for the catalog scans; `CatalogTupleInsert` consumed as a
`pub` keystone fn from `backend-catalog-indexing` (no cycle); syscache reads via
`backend-utils-cache-syscache` value API; dependencies via the pg-depend /
dependency owners.

## MANDATORY 3-catalog constant verification

All transcribed into `types_catalog::pg_publication` and cross-checked vs the
headers:

### pg_publication (`CATALOG(pg_publication,6104,PublicationRelationId)`)
| col | attnum | type | verified |
|-----|--------|------|----------|
| oid | 1 | Oid | ✓ |
| pubname | 2 | NameData | ✓ |
| pubowner | 3 | Oid | ✓ |
| puballtables | 4 | bool | ✓ |
| pubinsert | 5 | bool | ✓ |
| pubupdate | 6 | bool | ✓ |
| pubdelete | 7 | bool | ✓ |
| pubtruncate | 8 | bool | ✓ |
| pubviaroot | 9 | bool | ✓ |
| pubgencols | 10 | char | ✓ |

Natts = 10 ✓. Index OIDs: PublicationObjectIndexId=6110 ✓,
PublicationNameIndexId=6111 ✓. Relation OID 6104 ✓.

### pg_publication_rel (`CATALOG(pg_publication_rel,6106,...)`)
| col | attnum | type | verified |
|-----|--------|------|----------|
| oid | 1 | Oid | ✓ |
| prpubid | 2 | Oid | ✓ |
| prrelid | 3 | Oid | ✓ |
| prqual | 4 | pg_node_tree (varlen) | ✓ |
| prattrs | 5 | int2vector (varlen) | ✓ |

Natts = 5 ✓. Index OIDs: PublicationRelObjectIndexId=6112 ✓,
PublicationRelPrrelidPrpubidIndexId=6113 ✓, PublicationRelPrpubidIndexId=6116 ✓.
Relation OID 6106 ✓.

### pg_publication_namespace (`CATALOG(pg_publication_namespace,6237,...)`)
| col | attnum | type | verified |
|-----|--------|------|----------|
| oid | 1 | Oid | ✓ |
| pnpubid | 2 | Oid | ✓ |
| pnnspid | 3 | Oid | ✓ |

Natts = 3 ✓. Index OIDs: PublicationNamespaceObjectIndexId=6238 ✓,
PublicationNamespacePnnspidPnpubidIndexId=6239 ✓. Relation OID 6237 ✓.

### Other constants
- `PublishGencolsType` { None='n', Stored='s' } ✓ (already in tree; extended with
  `from_char`/`as_char`).
- `PUBLICATION_PART_{ROOT,LEAF,ALL}` → `PublicationPartOpt::{Root,Leaf,All}` ✓.
- Syscache ids: PUBLICATIONOID=51, PUBLICATIONRELMAP=53, PUBLICATIONNAMESPACEMAP=50,
  PUBLICATIONNAME=48 — already registered in syscache `cacheinfo[]`; added the
  identifier constants to `types-syscache/src/syscache_ids.rs` ✓.
- `F_BOOLEQ`=60, `F_CHAREQ`=61 added to `types_core::fmgr` (verified vs
  `pg_proc.dat`) ✓.
- pg_class attnums used by the full-catalog scans verified vs `pg_class.h`:
  oid=1, relnamespace=3, relpersistence=17, relkind=18, relispartition=28 ✓.

## Per-function parity (C → Rust)

| C function | Rust | notes |
|-----------|------|-------|
| check_publication_add_relation | `check_publication_add_relation` | relkind/system-table/temp/unlogged checks; `errdetail_relkind_not_supported` via pg-class seam. MATCH |
| check_publication_add_schema | `check_publication_add_schema` | catalog/toast/temp-namespace checks; `isAnyTempNamespace` via NEW namespace seam. MATCH |
| is_publishable_class | `is_publishable_class` | relkind ∈ {r,p} && !IsCatalogRelationOid && PERMANENT && oid ≥ FirstNormalObjectId. MATCH |
| is_publishable_relation | `is_publishable_relation` | projects the Relation's rd_rel into PgClassRow. MATCH (installed seam) |
| pg_relation_is_publishable | — | SQL-callable; SUPERSEDED — lives in this file but is a thin SearchSysCache(RELOID)+is_publishable_class wrapper; NOT installed (no PG_FUNCTION_ARGS fmgr-dispatch carrier yet, same class as the SRF below). Logic reachable via is_publishable_class. |
| is_ancestor_member_tableinfos | folded into `filter_partitions` (present-set membership) | MATCH |
| filter_partitions | `filter_partitions` | get_rel_relispartition + get_partition_ancestors; drops partitions whose parents are present. MATCH |
| is_schema_publication | `is_schema_publication` | systable scan PUBLICATIONNAMESPACE by pnpubid, first row exists. MATCH (installed) |
| check_and_fetch_column_list | `check_and_fetch_column_list` | alltables short-circuit; SearchSysCache2(PUBLICATIONRELMAP); prattrs → pub_collist_to_bitmapset. MATCH (installed) |
| GetPubPartitionOptionRelations | `GetPubPartitionOptionRelations` | find_all_inheritors(NoLock) for ALL/LEAF; lappend relid otherwise. MATCH (installed) |
| GetTopMostAncestorInPublication | `GetTopMostAncestorInPublication` | level-walk; GetRelationPublications then GetSchemaPublications(get_rel_namespace). Returns (relid, level). MATCH (installed) |
| attnumstoint2vector | `attnumstoint2vector` | bms_next_member loop → int2vector varlena image. MATCH |
| publication_add_relation | `publication_add_relation` | GetPublication, dup-check (SearchSysCacheExists2 → 4-key SearchSysCacheExists w/ UNUSED), check_publication_add_relation, pub_collist_validate, form values[Natts], GetNewOidWithIndex, prqual=nodeToString text / prattrs=int2vector, heap_form_tuple, CatalogTupleInsert, dep on publication (AUTO) + relation (AUTO) + whereClause (recordDependencyOnSingleRelExpr NORMAL) + columns (NORMAL subset), close, InvalidatePublicationRels over PART_ALL hierarchy. MATCH (installed) |
| pub_collist_validate | `pub_collist_validate` | strVal each String node; get_attnum; Invalid/system/virtual-generated/duplicate ereports; bms_add_member. MATCH (installed) |
| pub_collist_to_bitmapset | `pub_collist_to_bitmapset` | reads int2vector dim1 + int16 elements; bms_add_member each. MATCH (installed) |
| pub_form_cols_map | `pub_form_cols_map` | skip dropped; STORED-gencol gating; bms_add_member attnum. MATCH (installed) |
| publication_add_schema | `publication_add_schema` | symmetric to add_relation for pg_publication_namespace; dep on publication + schema (both AUTO); InvalidatePublicationRels over GetSchemaPublicationRelations(PART_ALL). MATCH (installed) |
| GetRelationPublications | `GetRelationPublications` | SearchSysCacheList1(PUBLICATIONRELMAP) → prpubid each. MATCH (installed) |
| GetPublicationRelations | `GetPublicationRelations` | systable scan PUBLICATIONREL by prpubid; GetPubPartitionOptionRelations each prrelid; sort+dedup. MATCH (installed) |
| GetAllTablesPublications | `GetAllTablesPublications` | seq catalog scan (index_ok=false) of pg_publication where puballtables=true. MATCH (installed) |
| GetAllTablesPublicationRelations | `GetAllTablesPublicationRelations` | seq scan pg_class relkind=r (and =p when pubviaroot); is_publishable_class + !(relispartition && pubviaroot) / !relispartition. MATCH (installed) |
| GetPublicationSchemas | `GetPublicationSchemas` | systable scan PUBLICATIONNAMESPACE by pnpubid → pnnspid each. MATCH (installed) |
| GetSchemaPublications | `GetSchemaPublications` | SearchSysCacheList1(PUBLICATIONNAMESPACEMAP) → pnpubid each. MATCH (installed) |
| GetSchemaPublicationRelations | `GetSchemaPublicationRelations` | seq scan pg_class relnamespace=schemaid; is_publishable_class; r → lappend, p → GetPubPartitionOptionRelations + list_concat_unique_oid. MATCH (installed) |
| GetAllSchemaPublicationRelations | `GetAllSchemaPublicationRelations` | GetPublicationSchemas → GetSchemaPublicationRelations each, concat. MATCH (installed) |
| GetPublication | `GetPublication` | SearchSysCache1(PUBLICATIONOID); decode pubname/alltables/actions/viaroot/gencols; elog on miss. MATCH (installed) |
| GetPublicationByName | `GetPublicationByName` | get_publication_oid(name, missing_ok) → GetPublication or None. MATCH (installed) |
| gather_publication_tables (static) + pg_get_publication_tables body | `build_publication_table_rows` | the portable core: per-pubname GetPublicationByName, alltables→GetAllTablesPublicationRelations else relids+schemarelids (PART_ROOT/LEAF) list_concat_unique_oid; published_rel accumulation; viaroot→filter_partitions; per-row pubid/relid + prattrs/prqual from PUBLICATIONRELMAP (skipped for alltables/schema-member) + all-columns int2vector fallback honoring pubgencols. MATCH (installed) |

## STOP: pg_get_publication_tables SQL entry point (F3 wrapper)

The SQL-callable `pg_get_publication_tables(text[])` is a value-per-call SRF
(`SRF_IS_FIRSTCALL`/`SRF_FIRSTCALL_INIT`/`SRF_RETURN_NEXT` over a hand-built
4-column `FuncCallContext` tuple desc). The repo's per-call `FuncCallContext` SRF
protocol is unported and PANICS (`funcapi` srf_support.rs:291/304 — needs the
typed `fn_extra` slot on `FmgrInfo`). The materialized-SRF path does not match
the C structure (it builds the result desc from `expectedDesc`/RECORD, not the
bespoke desc the C constructs). Per the family-STOP rule, the **entire portable
body** is ported as `build_publication_table_rows` (installed seam, returns the
`PublicationTableRow` rows a future SRF owner emits); only the ~30 lines of
fcinfo/SRF glue + the bespoke `CreateTemplateTupleDesc`/`BlessTupleDesc` wrapper
are deferred to whoever lands the per-call SRF keystone. Same class as
`pg_relation_is_publishable` (PG_FUNCTION_ARGS fmgr carrier).

## Outward seams installed / added
- NEW namespace seam `is_any_temp_namespace` declared + installed by the
  namespace owner (`isAnyTempNamespace` had no seam).
- Consumed existing: errdetail_relkind_not_supported (pg-class), get_attnum /
  get_namespace_name / get_rel_relkind / get_rel_namespace /
  get_rel_relispartition / get_publication_oid (lsyscache), recordDependencyOn
  (pg-depend), recordDependencyOnSingleRelExpr (dependency), CatalogTupleInsert
  (indexing keystone), GetNewOidWithIndex / IsCatalog* (catalog),
  find_all_inheritors (pg-inherits), get_partition_ancestors (partition),
  CacheInvalidateRelcacheByRelid (cache-inval), nodeToString (outfuncs),
  bms_* (nodes-core), SearchSysCache* (syscache), table_open (table).

## Inward seams (21, all installed by init_seams; wired into seams-init)
GetPublication, GetPublicationByName, GetRelationPublications,
GetPublicationRelations, GetAllTablesPublications,
GetAllTablesPublicationRelations, GetPublicationSchemas, GetSchemaPublications,
GetSchemaPublicationRelations, GetAllSchemaPublicationRelations,
GetPubPartitionOptionRelations, GetTopMostAncestorInPublication,
is_publishable_relation, is_schema_publication, check_and_fetch_column_list,
pub_collist_validate, pub_collist_to_bitmapset, pub_form_cols_map,
publication_add_relation, publication_add_schema, build_publication_table_rows.

Note: no current in-tree consumer exists (publicationcmds / relcache pubdesc
installer / subscriptioncmds / pgoutput are unported), so the inward seams are
installed-but-unconsumed — live the moment a consumer lands. This crate IS the
keystone those units were blocked on.

## Gate
- `cargo check --workspace`: 0 errors.
- `cargo test -p no-todo-guard`: PASS (no todo!/unimplemented!).
- `cargo test -p seams-init`: both recurrence guards PASS
  (`every_seam_installing_crate_is_wired_into_init_all`,
  `every_declared_seam_is_installed_by_its_owner`).
- The single `unreachable!("guarded above")` mirrors C's `Assert(false)` in
  GetPubPartitionOptionRelations (the Root arm is excluded by the enclosing
  `pub_partopt != Root` guard); not a stub.
