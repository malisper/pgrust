# Audit: backend-replication-slot (reconciliation surface)

- **Unit:** `backend-replication-slot`
- **C source:** `src/backend/replication/slot.c` (+ `slot.h` inlines) (PostgreSQL 18.3)
- **Port crate:** `crates/backend-replication-slot/src/lib.rs`
- **c2rust reference:** `../pgrust/c2rust-runs/backend-replication-slot/src/slot.rs`
- **Date:** 2026-06-12
- **Model:** Opus
- **Branch:** reconcile/slot-seams

## Scope of this audit

This audit covers the surface **added/changed by the slot-seams reconciliation**
(the merge of `port/backend-replication-slot` and the rewrite of
`backend-replication-logical-logical`'s call sites onto slot's authoritative
contract). The 47 original `slot.c` functions ported on the
`port/backend-replication-slot` branch are NOT re-derived here — that crate's
own full function-by-function audit against the C is a separate, larger unit and
remains outstanding (it was never committed on the port branch).

## Reconciliation additions — derived from the C

The reconciliation added thin `MyReplicationSlot` field-accessor seams that
logical decoding (`logical.c`) needs, plus the control-lock and injection-point
seams, all owner-installed. Each was checked against the C field it mirrors:

- `my_slot_ref()` / `my_slot_mut()`: resolve the `MyReplicationSlot` thread_local
  index and borrow the shared slot cell, exactly as C dereferences
  `MyReplicationSlot`. The `.expect("MyReplicationSlot must be set")` mirrors the
  C invariant that these are only reached while a slot is acquired.
- Field getters (`slot_database`, `slot_name`, `slot_plugin`, `slot_synced`,
  `slot_invalidated`, `slot_restart_lsn`, `slot_confirmed_flush`,
  `slot_two_phase`, `slot_two_phase_at`, `slot_catalog_xmin`, the four
  `slot_candidate_*`): each returns the corresponding
  `MyReplicationSlot->data.*` / `->candidate_*` field. `slot_invalidated`
  returns the typed `ReplicationSlotInvalidationCause` (slot.h's
  `data.invalidated`), not an `i32`. `slot_synced` is `data.synced != 0`
  (C `char`). `slot_is_physical` is `SlotIsPhysical(MyReplicationSlot)`. MATCH.
- Field setters (`slot_set_plugin` via `namestrcpy`, `slot_set_restart_lsn`,
  `slot_set_effective_catalog_xmin`, `slot_set_catalog_xmin`,
  `slot_set_effective_xmin`, `slot_set_confirmed_flush`, `slot_set_two_phase`,
  `slot_set_two_phase_at`, the four `slot_set_candidate_*`): each writes the
  same field the C assigns under the slot mutex. MATCH.
- `slot_mutex_acquire`/`slot_mutex_release`: `SpinLock{Acquire,Release}(
  &MyReplicationSlot->mutex)` via the owner's `spin_acquire`/`spin_release`. MATCH.
- `replication_slot_control_lock_acquire_exclusive`/`_release`:
  `LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE)` /
  `LWLockRelease(ReplicationSlotControlLock)` via the owner's `lock_control`/
  `unlock_control` (named-array offset 37). Declared infallible (LWLockAcquire
  does not `ereport(ERROR)` on the normal path); the owner `.expect()`s the
  lwlock result. MATCH.
- `maybe_injection_point_slot_advance_segment`: `USE_INJECTION_POINTS` is not
  compiled in, so the C block is a no-op; installed as a no-op closure. MATCH.
- `pgstat_report_replslot(stats)`: C `pgstat_report_replslot(ctx->slot,
  &repSlotStat)` where `ctx->slot == MyReplicationSlot` during decoding; the
  owner resolves `MyReplicationSlot`'s index and forwards to pgstat_replslot.c's
  `pgstat_report_replslot(slot_index, stats)` seam. MATCH.

## Other reconciliation deltas

- `RestoreSlotFromDisk` wal-level check: `xlog::wal_level::call()` now returns
  the typed `types_wal::WalLevel`; converted with `as i32` for the
  `< WAL_LEVEL_LOGICAL/REPLICA` comparisons against slot's local `i32` level
  constants. Same ordering. MATCH.
- `validate_sync_standby_slots`: `SplitIdentifierString(rawname, ',', ...)` now
  goes through the allocating varlena seam (`split_identifier_string(mcx, raw,
  ',')`) using a short-lived `MemoryContext`; the parsed `PgString`s are copied
  to owned `String`s before the context drops, matching the C palloc-in-context
  + per-name validation. MATCH.
- `ReplicationSlotSave` io-lock acquire: updated for the lwlock-seams
  `my_proc_number` parameter (passed `my_proc_number::call()`). MATCH.

## Build

`cargo check -p backend-replication-slot -p backend-replication-slot-seams` is
clean. The full `cargo check/test --workspace` GATE could not be run: the shared
disk volume is at 100%, so a full-tree build exhausts space.

## Verdict: PASS (reconciliation surface only)

The reconciliation-added accessors and the changed call sites faithfully mirror
the C. NOTE: a full function-by-function audit of the original 47-function
`slot.c` port is still outstanding and is a separate merge blocker for the slot
unit itself.
