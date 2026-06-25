# Audit: backend-executor-tstoreReceiver

C source: `src/backend/executor/tstoreReceiver.c` (PG 18.3). 8 function
definitions (no statics/inlines beyond these). c2rust run present at
`c2rust-runs/backend-executor-tstoreReceiver`; rendering matches the C 1:1.

## Function table

| C fn (loc) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `tstoreStartupReceiver` (56) | `tstore_startup_receiver` | MATCH | `needtoast` loop identical (skip `attisdropped`, `attlen==-1` → break); selects Detoast/Notoast. The `tupmap` arm is structurally unreachable (see below) so it is not built. |
| `tstoreReceiveSlot_notoast` (122) | `tstore_receive_slot_notoast` | MATCH | C `tuplestore_puttupleslot`; owned model deforms (`slot_getallattrs`) + `tuplestore_putvalues` — same stored row. The repo `puttupleslot` seam is SlotId+EState-keyed (not carried by the receiver vtable dispatch), so the value-form path is the faithful equivalent. |
| `tstoreReceiveSlot_detoast` (136) | `tstore_receive_slot_detoast` | MATCH | deform; per-column `!attisdropped && attlen==-1 && !isnull` then `VARATT_IS_EXTERNAL` then `detoast_external_attr`; `outvalues[i]=val` word-copy (move); `tuplestore_putvalues(tstore, typeinfo, outvalues, isnull)`; temporaries freed on Vec drop (C `pfree(tofree[])`). |
| `tstoreReceiveSlot_tupmap` (192) | (not selected) | MATCH (dead path) | Selected only when `myState->target_tupdesc` is set. BOTH backend callers of `SetTuplestoreDestReceiverParams` (portalcmds.c:422, pquery.c:1003 FillPortalStore) pass `target_tupdesc = NULL`, so no input ever reaches this variant. The repo seam `set_tuplestore_dest_receiver_params(receiver, portal, detoast)` carries no `target_tupdesc` param (inherited contract from the portalcmds declaration), so `tupmap` cannot be selected — no behavioral divergence on any reachable input. The variant would additionally require an `'mcx`-bound `TupleConversionMap`/mapslot that cannot live in the `'static` per-receiver registry (the same `'mcx`-vs-`'static` keystone the dest-router documents for `intorel`); since it is unreachable, this is moot. |
| `tstoreShutdownReceiver` (206) | `tstore_shutdown_receiver` | MATCH | C frees `outvalues`/`tofree`, `free_conversion_map(tupmap)`, `ExecDropSingleTupleTableSlot(mapslot)`. Owned model: workspace is per-row locals (already dropped), no persistent tupmap/mapslot — nothing remains to release. |
| `tstoreDestroyReceiver` (229) | `tstore_destroy_receiver` | MATCH | C `pfree(self)`; owned model releases the `RECEIVERS` state slot. |
| `CreateTuplestoreDestReceiver` (238) | `CreateTuplestoreDestReceiver` | MATCH | C `palloc0` + wire 4 callbacks + `mydest=DestTuplestore`. Owned model registers per-receiver state + `ReceiverVtable` into the tcop-dest router with `CommandDest::Tuplestore`; the `RECEIVERS` index is the router `state` token (the `(TStoreState*)self` stand-in). |
| `SetTuplestoreDestReceiverParams` (266) | `SetTuplestoreDestReceiverParams` | MATCH | C sets tstore/cxt/detoast/target_tupdesc/map_failure_msg + two `Assert`s. Owned model binds the Portal handle (the source of `holdStore`/`holdContext`, C's `myState->tstore==portal->holdStore` alias) + `detoast`. The asserts (`!(detoast && target_tupdesc)`; `mydest==DestTuplestore`) are trivially satisfied (no target_tupdesc; router registered with `Tuplestore`). |

## Seams and wiring

Owned seam crate: `backend-executor-tstorereceiver-seams` (the only `*-seams`
crate mapping to `tstoreReceiver.c`). All 3 declarations installed by
`init_seams()`, which contains only `set()` calls:
`create_dest_receiver_tuplestore`, `set_tuplestore_dest_receiver_params`,
`dest_destroy`. `seams-init::init_all()` calls `init_seams()` (wired). Both
recurrence guards (`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`) pass.

Outward calls (each a thin marshal+delegate across a real owner boundary):
- `backend_tcop_dest::register_dest_receiver` — direct crate call to the router
  owner (mirrors copyto's `CreateCopyDestReceiver`); registration only.
- `backend_executor_execTuples_seams::slot_getallattrs` — deform owned by
  execTuples; thin delegate.
- `backend_utils_sort_storage_seams::tuplestore_putvalues` — store owned by
  sort-storage; thin delegate.
- `backend_access_common_detoast_seams::detoast_external_attr` — owned by
  common-detoast; thin delegate.

`varatt_is_external` is the pure `varatt.h` 1-byte-header bit-test (`b[0]==0x01`)
reproduced inline — the same macro `detoast.c` and `heaptuple.c` each reproduce
locally; not a TU boundary, not a seam.

## Design conformance

- The `thread_local RECEIVERS` table is the sanctioned `(DR_xxx*)self` stand-in
  for the unified dest-router model — byte-for-byte the pattern `copyto.c`'s
  merged `CreateCopyDestReceiver`/`RECEIVERS` uses; not an invented
  registry-shaped side table.
- The `Portal` (Rc) handle held in the receiver state is the faithful owned
  model of C's `myState->tstore == portal->holdStore` pointer alias; not invented
  opacity.
- No allocating seam lacks `Mcx`+`PgResult`; no shared static for per-backend
  globals; no lock held across `?`; no unledgered divergence marker. No
  `CONTRACT_RECONCILE_PENDING` entry is needed (the 3 seams are now installed by
  their owner; the recurrence guard re-asserts).

## Verdict: PASS

Every function MATCH (the `tupmap` variant is a contract-determined dead path,
unreachable on every input from both backend callers — not absent reachable
logic). Zero seam findings. Design-conformant.
