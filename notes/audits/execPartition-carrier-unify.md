# Audit: execPartition carrier unification (PartitionTupleRouting)

## Scope

Resolve the contract divergence where the owner crate
`backend-executor-execPartition` defined a full `PartitionTupleRouting<'mcx>`
while `types-nodes` carried an empty stub `PartitionTupleRouting { _private: () }`,
the seams referenced the stub, and the routing seams sat uninstalled in
`seams-init::CONTRACT_RECONCILE_PENDING`.

The fix homes the canonical carrier type in `types-nodes` and installs the three
tuple-routing seams against it.

## Unified carrier location

`crates/types-nodes/src/modifytable.rs` (re-exported from `types_nodes` root):
- `PartitionTupleRouting<'mcx>`
- `PartitionDispatchData<'mcx>` (subsidiary, was also owner-local)
- `PartitionDispatchId` (= `usize`)

The owner crate (`backend-executor-execPartition/src/lib.rs`) now
`pub use types_nodes::{PartitionDispatchData, PartitionDispatchId, PartitionTupleRouting};`
so the family modules (`routing_setup`, `routing_find`, `routing_init_info`)
keep reaching them via `crate::` with zero body changes. The TU-private consts
`PARTITION_MAX_KEYS` / `PARTITION_CACHED_FIND_THRESHOLD` stay in the owner crate
(not part of the carrier contract; no external consumer).

## Field-for-field audit

### `struct PartitionTupleRouting` (execPartition.c, opaque in execPartition.h)

| # | C field | C type | Rust field | Rust type | Verdict |
|---|---------|--------|------------|-----------|---------|
| 1 | partition_root | Relation | partition_root | Option<Relation<'mcx>> | OK (NULL = None) |
| 2 | partition_dispatch_info | PartitionDispatch * | partition_dispatch_info | PgVec<PgBox<PartitionDispatchData>> | OK (array of dispatch ptrs) |
| 3 | nonleaf_partitions | ResultRelInfo ** | nonleaf_partitions | PgVec<Option<RriId>> | OK (NULL elem = None; ptr = RriId pool id) |
| 4 | num_dispatch | int | num_dispatch | i32 | OK |
| 5 | max_dispatch | int | max_dispatch | i32 | OK |
| 6 | partitions | ResultRelInfo ** | partitions | PgVec<RriId> | OK (pool ids) |
| 7 | is_borrowed_rel | bool * | is_borrowed_rel | PgVec<bool> | OK (parallel to partitions) |
| 8 | num_partitions | int | num_partitions | i32 | OK |
| 9 | max_partitions | int | max_partitions | i32 | OK |
| 10 | memcxt | MemoryContext | memcxt | Opaque | OK (per-query cxt; owned model threads mcx) |

All 10 fields present, same order, no trims. Identical to the owner's previous
crate-local definition (verbatim move).

### `PartitionDispatchData` (execPartition.c, FLEXIBLE_ARRAY_MEMBER tail)

| # | C field | C type | Rust field | Rust type | Verdict |
|---|---------|--------|------------|-----------|---------|
| 1 | reldesc | Relation | reldesc | Option<Relation<'mcx>> | OK |
| 2 | key | PartitionKey | key | Option<PgBox<PartitionKeyData>> | OK |
| 3 | keystate | List * (ExprState) | keystate | PgVec<PgBox<ExprState>> | OK (NIL = empty) |
| 4 | partdesc | PartitionDesc | partdesc | Option<PgBox<PartitionDescData>> | OK |
| 5 | tupslot | TupleTableSlot * | tupslot | Option<TupleTableSlot> | OK |
| 6 | tupmap | AttrMap * | tupmap | Option<PgBox<AttrMap>> | OK |
| 7 | indexes[FLEXIBLE_ARRAY_MEMBER] | int[] | indexes | PgVec<i32> | OK (flexible tail) |

All 7 fields present, same order, no trims. Verbatim move from the owner.

## Routing fns vs execPartition.c

The routing fn bodies were NOT touched (verbatim from the already-ported owner);
only the carrier type they name moved crate. Signatures verified to match the
seam contract:
- `ExecSetupPartitionTupleRouting(mcx, estate, rel) -> PgResult<PartitionTupleRouting<'mcx>>`
  — owner returns by value (C `palloc0` + return-by-pointer). Seam contract
  hands back `PgBox`; bridged by a thin `seam_exec_setup_partition_tuple_routing`
  adapter in the owner that `alloc_in`s the value (no logic).
- `ExecFindPartition(mcx, mtstate, root, proute, slot, estate) -> PgResult<RriId>`
  — `mcx`-threaded adapter `seam_exec_find_partition` (signature-only shim;
  C `ExecFindPartition` returns `ResultRelInfo *` → `RriId` pool id).
- `ExecCleanupTupleRouting(mtstate, proute) -> PgResult<()>` — installed directly
  (signature already matches the seam).

## Seam installation

`backend-executor-execPartition::init_seams()` now installs all five seams
(previously only the 2 pruning ones):
- exec_init_partition_exec_pruning (pre-existing)
- exec_find_matching_subplans (pre-existing)
- exec_setup_partition_tuple_routing (NEW)
- exec_find_partition (NEW)
- exec_cleanup_tuple_routing (NEW)

The 3 routing entries were removed from
`seams-init::CONTRACT_RECONCILE_PENDING`. `init_seams()` was already wired into
`init_all()` (seams-init line 94).

## Consumers (backend-executor-nodeModifyTable)

`ModifyTableState.mt_partition_tuple_routing` is now
`Option<PgBox<'mcx, PartitionTupleRouting<'mcx>>>` (lifetime added). The 4
consumer files (`init`, `lifecycle`, `merge`, `update`) already `::call` the
seams and store `PgBox` results; the only edit needed was adding `<'mcx>` to the
`proute: &mut PartitionTupleRouting<'mcx>` parameter of
`ExecPrepareTupleRouting` (lifecycle.rs). They now receive the REAL carrier with
real fields.

## Verdict: PASS

Carrier moved field-for-field with zero trims, owner/seam/consumer reconciled
onto one type, all 5 seams installed by their owner (both recurrence guards
green), 3 entries removed from CONTRACT_RECONCILE_PENDING. The partition-tuple-
routing half of execPartition is now reachable through the installed seams.

## Gate

- `cargo check --workspace`: clean (warnings only).
- `cargo test -p no-todo-guard`: ok.
- `cargo test -p seams-init`: both recurrence guards ok (every_declared_seam_is_installed_by_its_owner + every_seam_installing_crate_is_wired_into_init_all).
- `cargo test --workspace`: green except the sanctioned flake
  `backend-optimizer-path-small::range_pair_*` (passes on rerun).
