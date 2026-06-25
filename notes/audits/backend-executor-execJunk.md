# Audit: backend-executor-execJunk

C source: `src/backend/executor/execJunk.c` (PostgreSQL 18.3).
Port: `crates/backend-executor-execJunk/src/lib.rs`.

## Function inventory

C has 5 public functions, no file-level statics (`newNode`/`list_head` in the
c2rust render are inlined header helpers, not execJunk functions).

| C function (line) | Port | Verdict | Notes |
|---|---|---|---|
| `ExecInitJunkFilter` (60) | `ExecInitJunkFilter` | MATCH | `ExecCleanTypeFromTL` (direct dep on execTuples) → cleanTupType. slot given → `ExecSetSlotDescriptor` (seam); else `MakeSingleTupleTableSlot(..,&TTSOpsVirtual)` modeled as `exec_alloc_table_slot(.., Virtual)` (seam) returning the pool id. `cleanLength = natts`; if >0, push `tle->resno` for each non-junk entry, with the C `Assert(cleanResno == cleanLength)` (debug_assert) plus a surfaced bounds Err on overrun; else empty map (C `cleanMap = NULL`). Builds `JunkFilter { T_JunkFilter, ... }`. Owned model: descriptor deep-cloned for the filter (C shares one `PinTupleDesc`'d pointer). |
| `ExecInitJunkFilterConversion` (137) | `ExecInitJunkFilterConversion` | MATCH | slot path identical. `palloc0` → resize-0 fill then per-i: skip `TupleDescCompactAttr(i)->attisdropped`, else inner `for(;;)` advancing `t` until a non-junk entry, set `cleanMap[i] = tle->resno`. Bounds/list-end guards surface the C's implicit walk overrun. |
| `ExecFindJunkAttribute` (210) | `ExecFindJunkAttribute` | MATCH | Delegates to `…InTlist(jf_targetList, attrName)`. |
| `ExecFindJunkAttributeInTlist` (222) | `ExecFindJunkAttributeInTlist` | MATCH | `resjunk && resname && strcmp==0` → `resno`; else `InvalidAttrNumber` (0). |
| `ExecFilterJunk` (247) | `ExecFilterJunk` | MATCH (slot ops SEAMED) | `cleanLength = natts`; transpose loop: `j==0` → `(Datum)0`/`true`; else `old_values[j-1]`/`old_isnull[j-1]` via `slot_getattr_by_id(slot, j)` (1-based; deforms up to j, equivalent to `slot_getallattrs` + index). Tail `ExecClearTuple` + fill + `ExecStoreVirtualTuple` modeled by the single `store_virtual_values` owner seam. Returns the result-slot id. |

## Seams and wiring

Owned seam crates by C-source coverage: execJunk.c maps to no `*-seams` crate
in this repo (no ported owner crosses a dependency cycle to reach execJunk; its
only consumer, execExpr, parks the junk filter as an address). `init_seams()` is
therefore empty by design — confirmed correct, matching the nodeResult/
functioncmds precedent. It is wired into `seams-init::init_all()`; both
recurrence guards pass (`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`).

Outward seams consumed (all owned by execTuples, justified — execTuples is the
slot machinery owner and is a direct dep only for `ExecCleanTypeFromTL`; the
pool-id slot-payload ops are seam-and-panic until the slot payload model is
wired into the EState pool): `exec_set_slot_descriptor`, `exec_alloc_table_slot`,
`slot_getattr_by_id`, `store_virtual_values`. Each call is thin marshal+delegate;
the clean-map computation and the transpose loop (execJunk's own logic) live in
this crate, not behind a seam.

## Design conformance

- Allocating paths (`vec_with_capacity_in`, descriptor clone) carry `Mcx` +
  `PgResult` (fallible). No infallible allocs on a palloc path.
- No `static`/`Atomic`/`Mutex`; no per-backend globals.
- No invented opacity: `JunkFilter`/`TargetEntry.resno`/`T_JunkFilter`(=385,
  verified vs nodetags.h) are real C-mirrored types/values.
- No `todo!`/`unimplemented!`; no own-logic panic standing in for an error path
  (FATAL/ERROR sites are `Err(PgError)`). Empty `PgVec::new_in` is the
  non-allocating `cleanMap = NULL` case.
- `TargetEntry` gained `resno` (field-for-field with primnodes.h); `resno`
  wired through `makeTargetEntry`.

## Verdict: PASS

All 5 functions MATCH; slot-payload ops correctly SEAMED to their owner; zero
seam findings; design-conformance clean.
