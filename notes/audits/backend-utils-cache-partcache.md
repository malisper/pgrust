# Audit: backend-utils-cache-partcache

Unit C sources: `src/backend/utils/cache/partcache.c`, `src/include/utils/partcache.h`.

Ported crates: `backend-utils-cache-partcache`, `types-partition`, plus the
declaration-only seam crates `backend-nodes-nodeFuncs-seams`,
`backend-nodes-makefuncs-seams`, `backend-partitioning-partbounds-seams`.

Audit method: function inventory built from the C source, cross-checked against
the c2rust rendering (`c2rust-runs/backend-utils-cache-partcache/src/partcache.rs`),
then each function compared C ↔ c2rust ↔ Rust port. Constants verified against
headers, not from memory.

## Function inventory and verdicts

`partcache.c` defines exactly 5 functions. `partcache.h` defines 7 inline
accessors. All 12 accounted for.

| C function (location) | Port location | Verdict | Notes |
|---|---|---|---|
| `RelationGetPartitionKey` (partcache.c:51) | lib.rs:70 | MATCH | `relkind != RELKIND_PARTITIONED_TABLE` → `None`; lazy build when key absent; returns cached key. relkind/`'p'`=112 verified vs pg_class.h:175. Cache get/set delegated to relcache owner (cache is relcache-owned) keyed by `rd_id`. |
| `RelationBuildPartitionKey` (partcache.c:77) | lib.rs:99 | MATCH (1 ledgered seam-boundary, below) | PARTRELID lookup + `cache lookup failed for partition key of relation %u` (XX000); strategy validation LIST/RANGE/HASH (`'l'`=108/`'r'`=114/`'h'`=104, verified vs c2rust:2952-2954 + types-partition); per-column loop: CLAOID lookup + `cache lookup failed for opclass %u`, `get_opfamily_proc`, missing-support-function `ereport(ERROR, 42P17)` with `hash`/`btree`+procnum+`format_type_be`; collation copy; `attno!=0` → TupleDescAttr type info else exprType/Typmod/Collation of next partexprs cell with `wrong number of partition key expressions` guard; `get_typlenbyvalalign`. `procnum`=HASHEXTENDED_PROC(2)/BTORDER_PROC(1) verified vs hash.h:356, nbtree.h:717. All palloc0 → fallible allocs. partsupfunc carries `fn_oid` only (eager lookup-failure preserved via `fmgr_info_check`). |
| `RelationGetPartitionQual` (partcache.c:277) | lib.rs:307 | MATCH | `!relispartition` → NIL (empty PgVec); else `generate_partition_qual`. |
| `get_partition_qual_relid` (partcache.c:299) | lib.rs:326 | MATCH | `get_rel_relispartition` guard; `relation_open(AccessShareLock)`; implicit-AND→bool: NIL→None, `len>1`→`makeBoolExpr(AND_EXPR,_,-1)`, else `linitial`; `relation_close(NoLock)` keeps lock. |
| `generate_partition_qual` (partcache.c:337) | lib.rs:378 | MATCH (1 ledgered seam-boundary, below) | `check_stack_depth`; `rd_partcheckvalid` fast path (copy) via relcache seam; `get_partition_parent(relid,true)`; `relation_open(parent, AccessShareLock)`; relpartbound read+parse+`get_qual_from_partbound`; parent recursion `list_concat(parent_qual, my_qual)` (parent first — verified); `map_partition_varattnos(result,1,rel,parent)`; cache write + `rd_partcheckvalid=true`; `relation_close(parent, NoLock)`; returns working copy. |
| `get_partition_strategy` (partcache.h:59) | types-partition lib.rs:160 | MATCH | `key->strategy`. |
| `get_partition_natts` (partcache.h:65) | types-partition lib.rs:165 | MATCH | `key->partnatts`. |
| `get_partition_exprs` (partcache.h:71) | types-partition lib.rs:170 | MATCH | `key->partexprs`. |
| `get_partition_col_attnum` (partcache.h:80) | types-partition lib.rs:175 | MATCH | `key->partattrs[col]`. |
| `get_partition_col_typid` (partcache.h:86) | types-partition lib.rs:180 | MATCH | `key->parttypid[col]`. |
| `get_partition_col_typmod` (partcache.h:92) | types-partition lib.rs:185 | MATCH | `key->parttypmod[col]`. |
| `get_partition_col_collation` (partcache.h:98) | types-partition lib.rs:190 | MATCH | `key->partcollation[col]`. |

## Seam audit (step 3)

Owned seam crates (by C-source coverage; this unit's `c_sources` = partcache.c
only): **none**. There is no `partcache-seams` crate and no inward seam into
partcache, so its `init_seams()` is correctly empty.

The three new seam crates the port introduces declare seams owned by *other*
units (`nodeFuncs.c`, `makefuncs.c`, `partbounds.c`) and contain declarations
only (no install, no logic) — they panic until their owners land. Sanctioned
"call an unported neighbor's function through its `-seams` crate" pattern
(AGENTS.md neighbor table). The relcache partkey/partcheck accessors and the
PARTRELID/CLAOID syscache projections are likewise declared in the owners' seam
crates (relcache.c, syscache.c) and installed by those owners.

Clean outward seams (thin marshal + delegate, no host logic):
`expr_type_info` (nodeFuncs, pure exprType/Typmod/Collation triple);
`make_and_boolexpr` (makefuncs, pure node construction); `get_opfamily_proc`,
`get_typlenbyvalalign`, `get_rel_relispartition` (lsyscache); `format_type_be`;
`fmgr_info_check` (fmgr); `relation_open`/`close` (relation);
`get_partition_parent`/`map_partition_varattnos` (partition);
`check_stack_depth`; `search_opclass` (syscache); relcache partkey/partcheck
get/set.

### Ledgered seam-boundary divergences (accepted)

Two seams bundle a partcache `if`-guard together with an unported-neighbor
sub-pipeline rather than splitting the guard back into partcache:

1. **`open_partrel_tuple` (syscache)** absorbs the partcache.c:140-166
   `partexprs` block: `stringToNode` → `eval_const_expressions` →
   `fix_opfuncids` → `copyObject`, plus the `if (!isnull)` guard.
2. **`qual_from_partbound` (partbounds)** absorbs partcache.c:365-382: the
   RELOID lookup, `cache lookup failed for relation %u`, the `relpartbound`
   `if (!isnull)` guard, and `castNode(PartitionBoundSpec, stringToNode(...))`
   before `get_qual_from_partbound`.

Disposition: accepted, not blocking. Both relocate behavior-identically (same
inputs → same owned `PgVec<Expr>`/`PgVec<Node>` across the boundary), and the
relocated guard selects between calls that are *all* to unported units.
Splitting them back into partcache would require inventing four seam crates
(`string_to_node`/readfuncs, `eval_const_expressions`/optimizer,
`fix_opfuncids`/nodeFuncs) plus an owned post-parse `Node` carrier and a
`PartitionBoundSpec` type — none exist — i.e. stubbing unported logic, which
the audit rules forbid more strongly than this placement. Both are explicitly
ledgered in the seam doc-comments with the exact C line ranges. When the owners
land, the guards should move back into partcache and the seams reduce to pure
`get_qual_from_partbound` / raw-row projections.

## Design conformance (step 3b)

- Allocating functions take `Mcx<'mcx>` and return `PgResult`; every growth is
  fallible (`pg_zeroed`, `vec_with_capacity_in`, `slice_in`, `try_reserve` +
  `mcx.oom`). PASS.
- No shared statics for per-backend globals; partition key/check cache is
  relcache-owned, reached by `rd_id` through the owner's seams. PASS.
- No locks held across `?` without a guard; `Relation` carries close semantics,
  lock intentionally kept (`NoLock`) matching C. PASS.
- `types-partition` defines real `PartitionKeyData`/`PartrelTupleData`/
  `PartKeyOpInfo`/`PartKeyTypeInfo` (trimmed) and strategy/proc constants
  verified against headers — no opaque aliases. PASS.
- `elog(ERROR)` → `PgError::error` (XX000); `ereport(ERROR,
  ERRCODE_INVALID_OBJECT_DEFINITION)` → `.with_sqlstate(42P17)`; OOM via
  `mcx.oom`. PASS.

## Build

`cargo build -p backend-utils-cache-partcache -p types-partition -p
backend-nodes-nodeFuncs-seams -p backend-nodes-makefuncs-seams -p
backend-partitioning-partbounds-seams` — clean.

## Verdict: PASS

All 12 functions MATCH. partcache retains 100% of its own algorithm (strategy
validation, the per-attribute opclass/support-func/type resolution loop, the
implicit-AND→bool conversion, the parent-recursion + relcache caching). All
constants (RELKIND_PARTITIONED_TABLE, the three PARTITION_STRATEGY codes,
HASHEXTENDED_PROC, BTORDER_PROC, ERRCODE_INVALID_OBJECT_DEFINITION) verified
against headers. The two seam-boundary placements are behavior-identical,
inseparable from unported callees, and ledgered; recorded for re-tightening
when nodeFuncs/optimizer/readfuncs/partbounds land.
