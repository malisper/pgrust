# Audit: backend-utils-adt-xid8funcs

Unit: `backend-utils-adt-xid8funcs` (covers `src/backend/utils/adt/xid8funcs.c`,
one file of the bundled `backend-utils-adt-sqlhelpers` catalog unit; split out as
its own crate like the other single-file adt ports).

C source: postgres-18.3 `src/backend/utils/adt/xid8funcs.c` (684 lines).

## Function-by-function

Every function in the C file is accounted for:

| C function | Port | Notes |
|---|---|---|
| `TransactionIdInRecentPast` (96) | `TransactionIdInRecentPast` | Returns `RecentPast{determinable, extracted_xid}`. `*extracted_xid = xid` set unconditionally before early returns. Future-xid → `ERRCODE_INVALID_PARAMETER_VALUE` "transaction ID N is in the future". `now_fullxid` via `varsup::read_next_full_transaction_id`; `oldestClogXid` via new `varsup::get_oldest_clog_xid`; `FullTransactionIdFromAllowableAt` inlined. Caller holds `XactTruncationLock`. |
| `cmp_fxid` (152) | `cmp_fxid` | `FullTransactionIdPrecedes`-based 3-way; matches C `-1/1/0`. |
| `sort_snapshot` (172) | `sort_snapshot` | Only sorts when `nxip > 1`; `sort_by` + `dedup_by` (= qsort + qunique); keeps `nxip` in sync. |
| `is_visible_fxid` (186) | `is_visible_fxid` | `< xmin` → true; `>= xmax` → false; `nxip > 30` (USE_BSEARCH_IF_NXIP_GREATER) bsearch else linear scan; branch order 1:1. |
| `buf_init` (221) | `buf_init` | Seeds xmin/xmax, nxip=0. |
| `buf_add_txid` (236) | `buf_add_txid` | `nxip++` before push (C orders the increment before the realloc). |
| `buf_finalize` (247) | folded into `parse_snapshot` return + `to_varlena_bytes` | The C `SET_VARSIZE` + `pfree(buf)` is the snapshot-to-varlena step; documented. |
| `parse_snapshot` (264) | `parse_snapshot` | xmin/xmax/xip parse with `strtou64`, colon checks, sanity (valid + xmax>=xmin), per-value order checks (`< xmin`/`>= xmax`/`< last_val`), dedup vs `last_val`, comma/EOS handling. `bad_format` → `ERRCODE_INVALID_TEXT_REPRESENTATION` "invalid input syntax for type pg_snapshot: ..." via `ereturn` (soft escontext → `Ok(None)`). |
| `pg_current_xact_id` (333) | `pg_current_xact_id` | `PreventCommandDuringRecovery("pg_current_xact_id()")` (utility seam) then `GetTopFullTransactionId` (new xact seam). |
| `pg_current_xact_id_if_assigned` (351) | `pg_current_xact_id_if_assigned` | `GetTopFullTransactionIdIfAny` (new xact seam); invalid → `None` (NULL). |
| `pg_current_snapshot` (369) | `pg_current_snapshot` | next_fxid via varsup; `GetActiveSnapshot` (snapmgr seam) → None ⇒ "no active snapshot set" (internal elog). `FullTransactionIdFromAllowableAt` over xmin/xmax/xip[xcnt]; `sort_snapshot`. |
| `pg_snapshot_in` (419) | `pg_snapshot_in` | Delegates to `parse_snapshot`. |
| `pg_snapshot_out` (435) | `pg_snapshot_out` | `xmin:xmax:` then comma-joined xips; returns owned String. |
| `pg_snapshot_recv` (467) | `pg_snapshot_recv` | nxip range check (`<0` or `> PG_SNAPSHOT_MAX_NXIP`), xmin/xmax sanity, per-value order (`< last`/`< xmin`/`xmax < cur`), in-place dedup (`i--`/`nxip--` ≡ leave `i`, decrement `nxip`). `bad_format` → `ERRCODE_INVALID_BINARY_REPRESENTATION` "invalid external pg_snapshot data". Big-endian reads via `Pq8Cursor`. |
| `pg_snapshot_send` (533) | `pg_snapshot_send` | Big-endian int4 nxip + u64 xmin/xmax/xip...; returns raw bytes (bytea body). |
| `pg_visible_in_snapshot` (554) | `pg_visible_in_snapshot` | `is_visible_fxid`. |
| `pg_snapshot_xmin` (568) | `pg_snapshot_xmin` | snap.xmin. |
| `pg_snapshot_xmax` (581) | `pg_snapshot_xmax` | snap.xmax. |
| `pg_snapshot_xip` (594) | `pg_snapshot_xip` | SRF; returns the `xip[0..nxip]` value sequence (FuncCallContext glue is the fmgr boundary). |
| `pg_xact_status` (639) | `pg_xact_status` + `pg_xact_status_locked` | `LWLockAcquire(XactTruncationLock, LW_SHARED)` via `lwlock_acquire_main(XACT_TRUNCATION_LOCK, LW_SHARED)` returning a `MainLWLockGuard` (Drop releases on the error path, explicit `release()` on success = C's single LWLockRelease). in-progress (procarray) tested before commit (transam, `transaction_xmin` threaded from snapmgr-pc); "in progress"/"committed"/"aborted" or NULL. |

## Constants verified

- `USE_BSEARCH_IF_NXIP_GREATER = 30`, `MaxAllocSize = 0x3fffffff`,
  `offsetof(pg_snapshot, xip) = 24` (4 varsz + 4 nxip + 8 xmin + 8 xmax),
  `PG_SNAPSHOT_SIZE`/`PG_SNAPSHOT_MAX_NXIP` formulas 1:1.
- `StaticAssertDecl(MAX_BACKENDS*2 <= PG_SNAPSHOT_MAX_NXIP)` reproduced as a
  `const _: () = assert!(...)`.
- SET_VARSIZE_4B header stored as `len << 2` (low tag bits 00) in
  `to_varlena_bytes`.
- SQLSTATEs: `22023` (future xid), `22P02` (text), `22P03` (binary).

## Seams

Consumed (real, landed owners): varsup `read_next_full_transaction_id` +
new `get_oldest_clog_xid`; xact new `get_top_full_transaction_id` /
`get_top_full_transaction_id_if_any`; snapmgr `get_active_snapshot`; snapmgr-pc
`transaction_xmin`; procarray `transaction_id_is_in_progress`; transam
`transaction_id_did_commit`; utility `prevent_command_during_recovery`; lwlock
`lwlock_acquire_main`. `FullTransactionIdFromAllowableAt` is the pure
`access/transam.h` static inline, inlined here (the same treatment twophase.c's
`adjust_to_full_transaction_id` already uses).

Installed by this crate: none (leaf consumer, no cyclic caller). The three new
declarations were added to their real owners' `-seams` crates and installed from
the owners' `init_seams()` (varsup, xact). `seams-init` recurrence guard green.

## Deferrals (project-wide, not divergences)

- fmgr/Datum v1 registry + varlena/`Datum` marshalling: cores expose decoded
  scalars / owned `PgSnapshot`, matching the sibling adt crates.
- `pg_snapshot_xip` `FuncCallContext` SRF glue: core returns the value sequence.

No `todo!`/`unimplemented!`. 21 unit tests pass (parse/out/visibility/sort/
send-recv/varlena/accessors + seam-driven current_xact_id / current_snapshot /
xact_status paths).
