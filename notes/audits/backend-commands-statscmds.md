# Audit: backend-commands-statscmds (statscmds.c)

Audited against `../pgrust/postgres-18.3/src/backend/commands/statscmds.c` (956
lines, 8 functions). Self-audit by the porting agent.

## Function inventory — all 8 covered

| C function | C lines | Rust | Status |
|---|---|---|---|
| `compare_int16` | 48-56 | inline `sort_by(|a,b| (av-bv).cmp(&0))` | ✓ |
| `CreateStatistics` | 61-632 | `CreateStatistics` | ✓ |
| `AlterStatistics` | 637-754 | `AlterStatistics` | ✓ |
| `RemoveStatisticsDataById` | 760-780 | `RemoveStatisticsDataById` | ✓ |
| `RemoveStatisticsById` | 785-829 | `RemoveStatisticsById` | ✓ |
| `ChooseExtendedStatisticName` | 847-876 | `ChooseExtendedStatisticName` | ✓ |
| `ChooseExtendedStatisticNameAddition` | 889-930 | `ChooseExtendedStatisticNameAddition` | ✓ |
| `StatisticsGetRelation` | 936-956 | `StatisticsGetRelation` | ✓ |

## CreateStatistics — branch-for-branch

- single-relation check (ERRCODE_FEATURE_NOT_SUPPORTED) ✓; RangeVar IsA check ✓.
- `relation_openrv(ShareUpdateExclusiveLock)` ✓ (parse-node RangeVar converted to
  the resolved owned `access::RangeVar` for the seam — field-for-field).
- relkind whitelist {RELATION, MATVIEW, FOREIGN_TABLE, PARTITIONED_TABLE} +
  ERRCODE_WRONG_OBJECT_TYPE + `errdetail_relkind_not_supported` ✓.
- `object_ownercheck(RelationRelationId, relid, stxowner)` runs even when
  `check_rights==false` (concurrent-change safety) + `aclcheck_error(NOT_OWNER,
  get_relkind_objtype(relkind), relname)` ✓.
- `!allowSystemTableMods && IsSystemRelation` → ERRCODE_INSUFFICIENT_PRIVILEGE ✓.
- name/namespace decision: `defnames` → `QualifiedNameGetCreationNamespace`,
  else `RelationGetNamespace` + `ChooseExtendedStatisticName(relname,
  NameAddition(exprs), "stat", nsp)` ✓. `namestrcpy` truncation ✓.
- `check_rights` namespace ACL_CREATE check + aclcheck_error(SCHEMA) ✓.
- duplicate-object check `statext_exists`; IF NOT EXISTS → NOTICE + close + return
  InvalidObjectAddress; else ERRCODE_DUPLICATE_OBJECT ✓.
- `numcols > STATS_MAX_DIMENSIONS (8)` → ERRCODE_TOO_MANY_COLUMNS ✓.
- StatsElem classification:
  - column ref (`selem->name`): `SearchSysCacheAttName` (search_syscache_attname
    → (attnum, atttypid)); missing → ERRCODE_UNDEFINED_COLUMN; `attnum<=0` system
    col reject; `attgenerated==ATTRIBUTE_GENERATED_VIRTUAL` reject (read via
    `get_attgenerated` — same syscache, separate projection, behaviour-identical
    to the C single-GETSTRUCT read); `lookup_type_cache(LT_OPR)->lt_opr==0`
    reject with `format_type_be` ✓.
  - parenthesized Var: same three rejections on `var->varattno`/`var->vartype`,
    error attname via `get_attname` ✓.
  - expression: `pull_varattnos(expr,1)` + `bms_next_member` loop applying
    `+ FirstLowInvalidHeapAttributeNumber (-7)`; per-attnum system-col + virtual-
    gen-col rejection; `list_length(exprs)>1` → exprType + LT_OPR reject;
    `stxexprs` append ✓.
- single-expression-kinds prohibition; ndistinct/dependencies/mcv parse +
  unrecognized → ERRCODE_SYNTAX_ERROR; `!requested_type && numcols>=2` builds all;
  `build_expressions = stxexprs != NIL`; `numcols<2 && stxexprs!=1` →
  ERRCODE_INVALID_OBJECT_DEFINITION ✓.
- attnum sort + adjacent-dup → ERRCODE_DUPLICATE_COLUMN ✓.
- O(N^2) duplicate-expression scan via `equal` → ERRCODE_DUPLICATE_COLUMN ✓.
- `stxkeys` int2vector; `stxkind` char[] (d/f/m/e in C order); `stxexprs` text via
  `nodeToString((Node*)stxexprs)` — all packed by the indexing owner's
  `catalog_tuple_insert_pg_statistic_ext` (GetNewOidWithIndex + values[] +
  heap_form_tuple + CatalogTupleInsert). stxstattarget left NULL ✓.
- post-create hook; `CacheInvalidateRelcache(rel)`; close ✓.
- dependency recording: per-column AUTO (RelationRelationId, relid, attnum);
  whole-table AUTO when nattnums==0; `recordDependencyOnSingleRelExpr((Node*)
  stxexprs, relid, NORMAL, AUTO, false)`; namespace NORMAL; owner ✓.
- `CreateComments` when stxcomment set ✓. Returns `myself`.

## AlterStatistics

- `-1`/NULL → default; range clamp: `<0` → ERRCODE_INVALID_PARAMETER_VALUE (too
  low); `>MAX_STATISTICS_TARGET (10000)` → clamp + WARNING ✓.
- `get_statistics_object_oid(missing_ok)`; invalid OID → DeconstructQualifiedName
  + NOTICE (schema-qualified vs not) + return InvalidObjectAddress ✓.
- `table_open(RowExclusiveLock)`; `SearchSysCache1(STATEXTOID)`
  (statext_search_tuple); invalid → `elog(ERROR, "cache lookup failed for
  extended statistics object %u")` ✓; ownership check → aclcheck_error(
  STATISTIC_EXT, NameListToString) ✓.
- `heap_modify_tuple` replacing only stxstattarget (explicit Int16 or NULL) +
  `CatalogTupleUpdate(&newtup->t_self)` (t_self copied from oldtup) ✓; post-alter
  hook; close. No dependency update (only target altered) ✓.

## RemoveStatisticsDataById / RemoveStatisticsById

- DataById: `table_open(StatisticExtDataRelationId, RowExclusiveLock)` +
  `SearchSysCache2(STATEXTDATASTXOID, statsOid, inh)`; delete if found (no error
  when absent); close ✓.
- ById: `table_open(StatisticExtRelationId)`; lookup tuple (else `elog(ERROR)`);
  read `stxrelid`; `table_open(relid, ShareUpdateExclusiveLock)`; delete both data
  rows (inh true/false); `CacheInvalidateRelcacheByRelid`; delete pg_statistic_ext
  tuple; keep user-table lock (close NoLock); close pg_statistic_ext ✓.

## ChooseExtendedStatisticName / Addition / StatisticsGetRelation

- Name: `makeObjectName(name1,name2,modlabel)` loop with `GetSysCacheOid2(
  STATEXTNAMENSP)` conflict probe + `label{++pass}` ✓.
- Addition: `_`-joined StatsElem names (`expr` for unnamed), strlcpy(NAMEDATALEN)
  truncation, break at NAMEDATALEN ✓.
- StatisticsGetRelation: `SearchSysCache1(STATEXTOID)` → stxrelid; missing →
  InvalidOid (missing_ok) or `elog(ERROR)` ✓.

## Constants verified vs headers

StatisticExtRelationId=3381, OidIndex=3380, NameIndex=3997, RelidIndex=3379,
DataRelationId=3429; Natts=9; Anum order oid/stxrelid/stxname/stxnamespace/
stxowner/stxkeys/stxstattarget/stxkind/stxexprs (1..9); STATS_EXT chars d/f/m/e;
STATS_MAX_DIMENSIONS=8; MAX_STATISTICS_TARGET=10000;
FirstLowInvalidHeapAttributeNumber=-7; int2vector layout (24-byte header +
int16[]), SET_VARSIZE `len<<2`.

## Behaviour-preserving decompositions (not divergences)

- `attgenerated` read via a separate `get_attgenerated` projection rather than the
  same GETSTRUCT as `(attnum, atttypid)` — same syscache, identical result.
- pg_statistic_ext tuple Datum packing (int2vector/char[]/text) lives behind the
  indexing owner's `catalog_tuple_insert_pg_statistic_ext` seam, per the
  established per-catalog insert-seam pattern (pg_cast/pg_constraint).
- `expression_tree_walker` gained the C `T_List` case (was missing) so a `List`
  node passed to `recordDependencyOnSingleRelExpr` visits each element.

## Mirror-and-panic (unported neighbours)

- The STATEXT / STATEXTDATA syscache reads (`statext_get_relid`,
  `statext_search_tuple`, `statext_data_search_tuple`, `statext_exists`,
  `get_statext_oid`) are declared in syscache-seams and consumed here; their owner
  (the pg_statistic_ext syscache layer) is unported, so they panic loudly until it
  lands. statscmds' own logic is 100% ported.

## todo!()/unimplemented!()

None.
