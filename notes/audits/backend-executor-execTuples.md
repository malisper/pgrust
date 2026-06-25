# Audit: backend-executor-execTuples

- **Unit:** backend-executor-execTuples (the `TupleTableSlot` machinery)
- **C source:** `src/backend/executor/execTuples.c` (~2245 lines, PostgreSQL 18.3)
- **c2rust reference:** `c2rust-runs/backend-executor-execTuples/src/*.rs`
  (type definitions / FFI only; the slot-op bodies were not translated)
- **Branch audited:** `decomp/exectuples-113-finish`
- **Date:** 2026-06-13
- **Model:** Claude Fable 5
- **Verdict: PASS.**

> Supersedes the prior `NEEDS_DECOMP` audit on this unit. That audit was correct
> for its branch state: the keystone had landed the body-bearing carriers
> (`FormedTuple`/`FormedMinimalTuple` in the slot fields + `TupleValue::ByRef`
> in `tts_values`) but had NOT widened the crate's slot-op return / store types
> off the header-only `HeapTuple`/`MinimalTuple`, so ~16 tuple-flow bodies stood
> as own-logic deferral `panic!`s whose callees (heaptuple.c) ARE ported —
> the forbidden deferral. This pass completes the keystone-2 widening and fills
> those bodies with faithful own-logic.

## Keystone-2: the carrier widening (and why it is contained)

The blocker the prior audit named was a perceived "32-consumer contract change"
— widening `ExecFetchSlotMinimalTuple` / `ExecCopySlot*Tuple` / the slot-op
return types would supposedly break consumer crates. **It does not.** On this
branch the `SlotData`-based public API of execTuples
(`ExecStore*`/`ExecFetch*`/`ExecCopy*`/the `tts_*` ops) has **zero external
callers** — the only external reference to `backend_executor_execTuples::` is
`seams-init` calling `init_seams()`. The externally-consumed seams are the
`SlotId`/`es_tupleTable` *pool* bridges (a separate, out-of-scope convergence
campaign, tracked in `CONTRACT_RECONCILE_PENDING`). Therefore the widening from
header-only `HeapTuple`/`MinimalTuple` to the body-bearing
`FormedTuple`/`FormedMinimalTuple` is **entirely internal to this crate** and
breaks no consumer. Verified by grepping every `crates/` reference to the crate
and to each widened entry point.

Widened (return/param types now carry the body bytes, no `.data` dropped):
- ops: `tts_{virtual,heap,minimal,buffer_heap}_copy_heap_tuple`,
  `…_copy_minimal_tuple` (now thread `extra`), `tts_{heap,buffer_heap}_get_heap_tuple`,
  `tts_minimal_get_minimal_tuple`, `tts_{heap,minimal}_store_tuple`
- store/fetch entry points: `ExecStoreHeapTuple`, `ExecStoreMinimalTuple`,
  `ExecForceStore{Heap,Minimal}Tuple`, `ExecFetchSlot{Heap,Minimal}Tuple`,
  `ExecCopySlotHeapTuple`, `ExecCopySlotMinimalTupleExtra`

## Function inventory & verdicts (filled carrier-bearing bodies)

Independently re-derived against execTuples.c. Helpers (heaptuple.c:
`heap_form_tuple` / `heap_copytuple` / `heap_form_minimal_tuple` /
`heap_copy_minimal_tuple` / `minimal_tuple_from_heap_tuple` /
`heap_tuple_from_minimal_tuple` / `heap_deform_tuple`) are ported and
body-bearing (return `FormedTuple`/`FormedMinimalTuple`).

| C function | Rust site | Verdict |
|---|---|---|
| tts_virtual_copy_heap_tuple | slot_ops_vtables | MATCH (Assert + heap_form_tuple) |
| tts_virtual_copy_minimal_tuple | slot_ops_vtables | MATCH (extra threaded) |
| tts_virtual_copyslot | slot_ops_vtables | MATCH (clear→slot_getallattrs→copy values→nvalid→¬EMPTY→materialize) |
| (helper) source_all_attrs | slot_ops_vtables | MATCH — faithful slot_getallattrs over an immutable source; deforms the source body via heap_deform_tuple, pads short tuples NULL; does not mutate the `&` source cache (behaviour-preserving) |
| tts_heap_get_heap_tuple | slot_ops_vtables | MATCH (materialize-if-null; owned clone is the owned-model equivalent of the read-only C borrow; shouldFree=false preserved at the fetch caller) |
| tts_heap_copy_heap_tuple | slot_ops_vtables | MATCH |
| tts_heap_copy_minimal_tuple | slot_ops_vtables | MATCH (extra threaded) |
| tts_heap_store_tuple | slot_ops_vtables | MATCH (clear→nvalid=0→t_self read-before-move→tuple/off→clear EMPTY\|SHOULDFREE→conditional SHOULDFREE) |
| tts_heap_copyslot | slot_ops_vtables | MATCH (ExecCopySlotHeapTuple over `&src` in dst ctx→store shouldFree=true; tableOid propagated) |
| tts_minimal_get_minimal_tuple | slot_ops_vtables | MATCH (materialize-if-null; owned clone, as above) |
| tts_minimal_copy_minimal_tuple | slot_ops_vtables | MATCH (extra threaded) |
| tts_minimal_copy_heap_tuple | slot_ops_vtables | MATCH |
| tts_minimal_store_tuple | slot_ops_vtables | MATCH (clear→asserts→¬EMPTY→nvalid/off=0→mintuple→minhdr view→conditional SHOULDFREE) |
| (helper) set_minimal_minhdr_view | slot_ops_vtables | MATCH (minhdr.t_len = mintuple.t_len + MINIMAL_TUPLE_OFFSET; t_data = mintuple−OFFSET, via heap_tuple_from_minimal_tuple view) |
| tts_minimal_copyslot | slot_ops_vtables | MATCH (ExecCopySlotMinimalTuple over `&src`→store shouldFree=true) |
| (helper) exec_copy_slot_heap_tuple_ref | slot_ops_vtables | MATCH (per-kind copy_heap_tuple dispatch) |
| (helper) exec_copy_slot_minimal_tuple_ref | slot_ops_vtables | MATCH (per-kind copy_minimal_tuple dispatch, extra threaded) |
| tts_buffer_heap_get_heap_tuple | slot_ops_vtables | MATCH |
| tts_buffer_heap_copy_heap_tuple | slot_ops_vtables | MATCH |
| tts_buffer_heap_copy_minimal_tuple | slot_ops_vtables | MATCH (extra threaded) |
| tts_buffer_heap_copyslot | slot_ops_vtables | MATCH (both arms: cross-kind/materialized/virtual→ExecCopySlotHeapTuple+SHOULDFREE; same-buffer→store(transfer_pin=false)+tupdata bookkeeping) |
| ExecStoreHeapTuple | slot_store_fetch | MATCH |
| ExecStoreMinimalTuple | slot_store_fetch | MATCH |
| ExecForceStoreHeapTuple | slot_store_fetch | MATCH (heap/buffer/virtual+minimal arms) |
| ExecForceStoreMinimalTuple | slot_store_fetch | MATCH (minimal fast path; else heap-tuple view→deform→StoreVirtual→materialize-if-shouldFree) |
| ExecFetchSlotHeapTuple | slot_store_fetch | MATCH (materialize-if-requested; get vs copy split→shouldFree) |
| ExecFetchSlotMinimalTuple | slot_store_fetch | MATCH |
| ExecCopySlotHeapTuple / ExecCopySlotMinimalTupleExtra / ExecCopySlot | slot_store_fetch | MATCH |
| (helper) deform_into_slot | slot_store_fetch | MATCH (heap_deform_tuple of a FormedTuple into the slot's value arrays) |
| (helper) tts_buffer_heap_store_tuple | slot_store_fetch | MATCH (SHOULDFREE-free guard; ¬EMPTY; nvalid=0; t_self read-before-move; the same-page pin optimization via real bufmgr seams) |

The remainder of the unit (slot creation/teardown, init/release/clear callbacks,
the `slot_deform` byte state machine, getsysattr/is_current_xact surface,
`ExecTypeFromTL*`/`TupleDescGetAttInMetadata`/`begin/do/end_tup_output` family)
was MATCH in the prior assembly audit and is unchanged on this branch.

## Sanctioned mirror-PG-and-panic (genuinely-unported callees — NOT deferrals)

Per the rubric, panicking on an unported *callee* is acceptable; absent
own-logic whose callee is ported is not. Each remaining panic was verified
(independently) to sit on a genuinely-unported dependency:

| Site | Why sanctioned |
|---|---|
| slot_store_fetch.rs deform_composite_datum_into_slot (`ExecStoreHeapTupleDatum`) | `DatumGetHeapTupleHeader` composite-`Datum` *decode* is unported workspace-wide. Verified: no ported body anywhere in `crates/`; execExprInterp's `eval_composite.rs` row/fieldselect/fieldstore steps panic on the SAME bridge. The bare-word `Datum` (`types-datum`, `Datum(usize)`) has no pointer-to-tuple lane. |
| slot_store_fetch.rs heap_copy_tuple_as_datum_carrier (`ExecFetchSlotHeapTupleDatum`) | `heap_copy_tuple_as_datum` is ported (yields a `FormedTuple` image) but minting that image into a composite `Datum` word (`HeapTupleGetDatum`) is the same unported bridge. |
| slot_deform.rs slot_getattr (by-reference arm) | A by-reference column is C's `PointerGetDatum(tp+off)`; the bare-word `Datum` has no pointer lane and the workspace has no pointer-bytes / datum-arena convention to mint a stable pointer word. The `Datum`-returning `slot_getattr` is the pool-seam contract form (in `CONTRACT_RECONCILE_PENDING`); in-crate callers read the `TupleValue` directly. |
| slot_deform.rs fetch_att_byval (switch default) | DEFENSIVE: the impossible-`attlen` arm (only 1/2/4/8 are valid by-value lengths), mirroring C's `fetch_att`. |

Out of scope (separate `SlotId`/`es_tupleTable` pool-convergence campaign,
explicitly retained as provisional): `exec_init_slots.rs` `slot_getallattrs` /
`exec_clear_tuple` / `exec_copy_slot` / `exec_set_slot_descriptor` /
`exec_store_all_null_tuple` / `slot_getsysattr` seam shims, bound to the trimmed
pool header. Their real bodies exist over the `SlotData` model in
slot_store_fetch/slot_ops_vtables; they remain in `CONTRACT_RECONCILE_PENDING`.

## Seam audit

- Owned seam crate: `backend-executor-execTuples-seams` (the only `X-seams`
  mapping to execTuples.c). `init_seams()` installs 17 seams (all `set()` calls,
  no logic), wired from `seams-init::init_all()`.
- `recurrence_guard` (both checks) passes; the pool seams it does not install
  are the allowlisted `CONTRACT_RECONCILE_PENDING` entries.
- Outward seam calls (eoh_get_flat_size/eoh_flatten_into, release_buffer/
  incr_buffer_ref_count, transaction_id_is_current_transaction_id, the heaptuple
  form/deform direct deps) are thin marshal+delegate. No `set()` outside the
  owner.

## Verification

- `cargo check --workspace`: clean (warnings only, all in pre-existing dep crates).
- `cargo test -p backend-executor-execTuples`: 4/4 carrier round-trip tests pass
  (virtual/heap/minimal kinds, form→store→fetch→copyslot→deform with a
  by-reference varlena column that would fail if any boundary dropped
  `FormedTuple::data`).
- `cargo test -p seams-init`: 2/2 recurrence guards pass.

## Conclusion

The `SlotData` payload model is complete own-logic that MATCHes execTuples.c,
with the body-bearing carriers carried end to end. Zero forbidden own-logic
deferral panics remain (every remaining panic sits on a genuinely-unported
dependency or is a defensive switch default). **PASS.**
