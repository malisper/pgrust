# Audit: backend-executor-execGrouping

C source: `src/backend/executor/execGrouping.c` (543 lines) + the `tuplehash`
instantiation of `src/include/lib/simplehash.h`.
Port: `crates/backend-executor-execGrouping/src/lib.rs`, `src/tuplehash.rs`.
Independent re-derivation from the C; not trusting port comments or the build.

## execGrouping.c functions

| C function | C lines | Verdict | Notes |
|---|---|---|---|
| `execTuplesMatchPrepare` | 57-84 | MATCH | `numCols==0`→None; `get_opcode` per col; `ExecBuildGroupingEqual(desc,desc,NULL,NULL→Virtual,Virtual)`; OOM via `try_reserve`. |
| `execTuplesHashPrepare` | 96-124 | MATCH | `get_opcode`+`get_op_hash_functions`, "could not find hash function" on miss, `Assert(left==right)`→`debug_assert_eq!`, `fmgr_info(right)`. |
| `BuildTupleHashTable` | 166-272 | MATCH | `MAXALIGN(additionalsize)`, `entrysize`, `hash_mem_limit` clamp, `hash_iv=murmurhash32(ParallelWorkerNumber)` iff `use_variable_hash_iv`, `tuplehash_create`, tableslot/exprcontext deferred to first search (behavior-preserving; see below), `ExecBuildHash32FromAttrs`/`ExecBuildGroupingEqual`/`CreateStandaloneExprContext`. JIT/parent observationally a no-op (not modeled). |
| `ResetTupleHashTable` | 279-283 | MATCH | `tuplehash::reset`. |
| `LookupTupleHashEntry` | 300-327 | MATCH | always-create; `local_hash` then `lookup_internal`; returns `(isnew, hash)`. |
| `TupleHashTableHash` | 332-349 | MATCH | sets inputslot + `hash_internal`. |
| `LookupTupleHashEntryHash` | 355-376 | MATCH | create→insert with isnew; non-create→lookup, `None` callback on miss. |
| `FindTupleHashEntry` | 387-410 | SEAMED (mirror-and-panic, acceptable) | see below. |
| `TupleHashTableHash_internal` | 420-458 | MATCH (NULL branch only) | `tuple!=NULL` branch never fires (SH_STORE_HASH); unit-`()` Key models the C NULL "reference inputslot" sentinel. |
| `LookupTupleHashEntry_internal` | 468-514 | MATCH | isnew vs lookup; `ExecCopySlotMinimalTupleExtra` into firstTuple + zeroed additional bytes. |
| `TupleHashTableMatch` | 519-543 | MATCH | inner=input(slot2)/outer=table(slot1); `equal()==ExecQualAndReset` (SH_EQUAL = match==0). |

## simplehash `tuplehash` macros (tuplehash.rs)

| Macro | Verdict | Notes |
|---|---|---|
| `SH_COMPUTE_SIZE`, `pg_nextpower2_64`, `SH_UPDATE_PARAMETERS` | MATCH | max(.,2), nextpow2, overflow guard; sizemask=size-1; fillfactor 0.9 / 0.98. |
| `SH_CREATE` / `SH_RESET` | MATCH | zeroed buckets via `try_reserve`; reset clears each bucket + members=0. |
| `SH_GROW` | MATCH | find-non-wrapped startelem, move (mem::replace) IN_USE entries into new array. |
| `SH_INITIAL_BUCKET`/`SH_NEXT`/`SH_PREV`/`SH_DISTANCE_FROM_OPTIMAL`/`SH_ENTRY_HASH`/`SH_COMPARE_KEYS` | MATCH | SH_STORE_HASH compare = `ahash==hash && SH_EQUAL`. |
| `SH_INSERT_HASH_INTERNAL` | MATCH | grow check, robin-hood displacement, SH_GROW_MAX_MOVE(150)/SH_GROW_MAX_DIB(25) regrow-restart at fill≥0.1; firstTuple NOT set on insert (left to caller). |
| `SH_LOOKUP_HASH_INTERNAL` / `SH_LOOKUP` / `SH_INSERT` | MATCH | |
| `SH_START_ITERATE` / `SH_ITERATE` | MATCH | backward iteration; iterator is the real `{cur,end,done}` triple (no lossy packing). |
| `SH_DESTROY`/`SH_DELETE`/`SH_DELETE_ITEM`/`SH_START_ITERATE_AT`/`SH_STAT` | MISSING (acceptable) | not instantiated/needed by execGrouping (Drop-based free; no deletes; debug-only). |

## FindTupleHashEntry mirror-and-panic justification

The only driver is nodeSubplan's hashed testexpr, which reads
`SubPlanState.lhs_hash_expr` / `cur_eq_comp`. Those are `Opaque` (`'static
Box<dyn Any>`) in this repo and cannot carry an `ExprState<'mcx>`; their
producer (`build_hash_projections_and_exprs` in execExpr) is itself an
unported-owner `panic!`. So the path is unreachable in the current tree. The
port panics with a clear message and the re-home instruction (move those fields
to `Option<PgBox<'mcx, ExprState<'mcx>>>`) for when that owner lands. Per the
"Mirror PG and panic" policy this is acceptable, not a silent stub.

## Deferred slot/exprcontext materialization

C's `BuildTupleHashTable` creates the standalone `tableslot`/`exprcontext`
eagerly with the parent EState in scope. The execGrouping build seam carries no
EState (the table is handed back before any search), so the standalone values
are stashed (`pending_tableslot`/`pending_exprcontext`) and registered into the
EState's slot/exprcontext pools on the first search call (`ensure_materialized`,
which every Lookup/Hash/Find/Scan invokes first). The slots/exprcontext are
unused until a search, so this preserves behavior exactly.

## Seams / wiring

- Owns `backend-executor-execGrouping-seams`; `init_seams()` installs ALL 11
  declarations (build/lookup/lookup_hash/find/hash/reset/init_iter/scan/term_iter
  /exec_tuples_hash_prepare/exec_tuples_match_prepare). Wired into
  `seams-init::init_all`.
- Outward seams (real dep cycles, thin marshal+delegate): execExpr
  (`exec_build_hash32_from_attrs`, `exec_build_grouping_equal`,
  `exec_eval_expr_switch_context`, `exec_qual_and_reset`), execTuples
  (`make_single_tuple_table_slot`, `exec_store_minimal_tuple`,
  `exec_copy_slot_minimal_tuple_extra`), execUtils
  (`create_standalone_expr_context`), lsyscache (`get_opcode`,
  `get_op_hash_functions`), fmgr (`fmgr_info`), nodeHash
  (`get_hash_memory_limit`), parallel (`parallel_worker_number`).
- 4 new owner seams added+installed: execExpr `exec_build_hash32_from_attrs` +
  `exec_build_grouping_equal`; execUtils `create_standalone_expr_context`;
  execTuples `exec_copy_slot_minimal_tuple_extra`. `recurrence_guard`
  (seams-init) confirms all are installed by their owner and wired.

## Design conformance

- Types de-opaqued in `types-nodes::nodeagg` per opacity-inherited (execnodes.h
  exposes the generated `tuplehash` types): `TuplehashHash` real struct,
  `TupleHashEntryData.additional` real `PgVec<u8>`, `TupleHashIterator` real
  triple. No invented handles/registries.
- Allocating functions take `Mcx` and return `PgResult`; OOM via `mcx.oom`.
- No locks across `?`; no shared statics; no ambient-global seams (parallel
  worker number is a seam call into its owner).

## Verdict: PASS

All execGrouping.c functions MATCH (or the one acceptable SEAMED
mirror-and-panic); all instantiated simplehash macros MATCH; uninstantiated
macros are genuinely unused. Zero seam findings. The iterator-packing
truncation flagged during review was fixed by widening `TupleHashIterator` to
the real `{cur,end,done}` triple.
