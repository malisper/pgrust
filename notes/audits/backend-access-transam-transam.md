# Audit: backend-access-transam-transam

- Unit: `backend-access-transam-transam` (`src/backend/access/transam/transam.c`)
- Port: `crates/backend-access-transam-transam` (+ new outward seam crates
  `backend-access-transam-clog-seams`, `backend-access-transam-subtrans-seams`,
  `backend-utils-time-snapmgr-seams`)
- C source: `postgres-18.3/src/backend/access/transam/transam.c` (406 lines)
- c2rust: `c2rust-runs/backend-access-transam-transam/src/transam.rs`
- Auditor: independent re-derivation from C + c2rust, 2026-06-12

## Function inventory and verdicts

Every function definition in transam.c (including the one static), plus the
`transam.h` inline predicates the port claims:

| # | C function (transam.c line) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `TransactionLogFetch` (static, 51) | `TransactionLogFetch` (46) | MATCH | Cache-hit check first (including the C quirk that xid 0 hits the freshly-reset cache and returns status 0); special-xid branch: Bootstrap(1)/Frozen(2) → COMMITTED, other non-normal → ABORTED; clog fetch via seam returns `(XidStatus, XLogRecPtr)` standing in for the C out-parameter; cache update skipped for IN_PROGRESS and SUB_COMMITTED, exactly the C predicate. ereport(ERROR) channel from the SLRU read carried as `PgResult`. |
| 2 | `TransactionIdDidCommit` (125) | (83) | MATCH | COMMITTED → true; SUB_COMMITTED → precedes-TransactionXmin → false, else SubTransGetParent; invalid parent → `elog(WARNING, "no pg_subtrans entry for subcommitted XID %u")` then false; else recurse on parent; fallthrough false. WARNING level 19 matches c2rust constant; message text identical (`{transactionId}` renders u32 decimal = `%u`). |
| 3 | `TransactionIdDidAbort` (187) | (132) | MATCH | Mirror of #2 with ABORTED → true and the SUB_COMMITTED defaults inverted (true on old-xid and missing-parent paths), recursion on parent. Verified the inverted defaults against C lines 210-219. |
| 4 | `TransactionIdCommitTree` (239) | (172) | MATCH | One delegate to `TransactionIdSetTreeStatus(xid, xids, COMMITTED, InvalidXLogRecPtr)`; `(nxids, xids*)` → `&[TransactionId]`. |
| 5 | `TransactionIdAsyncCommitTree` (251) | (183) | MATCH | Same with caller-supplied `lsn`. |
| 6 | `TransactionIdAbortTree` (269) | (199) | MATCH | Same with ABORTED + InvalidXLogRecPtr. |
| 7 | `TransactionIdPrecedes` (279) | (227) | MATCH | Non-normal either side → plain unsigned `<`; else `(int32)(id1 - id2) < 0` rendered as `wrapping_sub(..) as i32` — identical to c2rust line 220. |
| 8 | `TransactionIdPrecedesOrEquals` (298) | (239) | MATCH | Same shape, `<=` / `diff <= 0`. |
| 9 | `TransactionIdFollows` (313) | (249) | MATCH | Same shape, `>` / `diff > 0`. |
| 10 | `TransactionIdFollowsOrEquals` (328) | (259) | MATCH | Same shape, `>=` / `diff >= 0`. |
| 11 | `TransactionIdLatest` (344) | (270) | MATCH | `while (--nxids >= 0)` back-to-front scan ⇔ `xids.iter().rev()`; same comparisons, same result for every input including empty array. |
| 12 | `TransactionIdGetCommitLSN` (381) | (296) | MATCH | Cache-hit on xid equality only (no validity check, matching C); non-normal → InvalidXLogRecPtr; else clog fetch, status discarded (`(void)` cast ⇔ `_status`). |
| 13 | `TransactionIdEquals` (transam.h:43) | (210) | MATCH | `id1 == id2`. |
| 14 | `TransactionIdIsValid` (transam.h:41) | (216) | MATCH | `xid != InvalidTransactionId`. |
| 15 | `TransactionIdIsNormal` (transam.h:42) | (222) | MATCH | `xid >= FirstNormalTransactionId`. |

File-scope statics `cachedFetchXid` / `cachedFetchXidStatus` / `cachedCommitLSN`
(C lines 33-35) are per-backend state, ported as `thread_local!` `Cell`s with the
same initial values (Invalid/0/0).

## Constants (verified against headers, not memory)

- `clog.h:27-30`: TRANSACTION_STATUS_ IN_PROGRESS=0x00, COMMITTED=0x01,
  ABORTED=0x02, SUB_COMMITTED=0x03 — `types-core/src/xact.rs:22-25` match.
- `transam.h:31-34`: Invalid=0, Bootstrap=1, Frozen=2, FirstNormal=3 —
  `xact.rs:14-17` match.
- `xlogdefs.h:28`: InvalidXLogRecPtr=0 — `xact.rs:20` matches.
- `TransactionId` = u32, `XLogRecPtr` = u64, `XidStatus` = c_int/i32 — match
  c2rust types.
- WARNING = 19 (`elog.h:46`) — `types-error/src/error.rs:23` matches the level
  c2rust passes to errstart.

## Seam audit

Outward seams (this unit installs nothing inward; no `init_seams()` needed and
none exists — consistent):

| Seam crate | Slot | Used by | Assessment |
|---|---|---|---|
| `backend-access-transam-clog-seams` | `transaction_id_get_status(xid) -> PgResult<(XidStatus, XLogRecPtr)>` | TransactionLogFetch, TransactionIdGetCommitLSN | Thin: the tuple is exactly the C return + out-parameter. No logic in the seam. Owner (clog.c) unported; uninstalled call panics loudly per seam-core. |
| `backend-access-transam-clog-seams` | `transaction_id_set_tree_status(xid, &[xids], status, lsn) -> PgResult<()>` | the three *Tree wrappers | Thin marshal of `(nxids, xids*)` to a slice. No logic. |
| `backend-access-transam-subtrans-seams` | `sub_trans_get_parent(xid) -> PgResult<TransactionId>` | DidCommit/DidAbort | Thin, one call. |
| `backend-utils-time-snapmgr-seams` | `transaction_xmin() -> TransactionId` | DidCommit/DidAbort | Global-variable read seam for snapmgr.c's `TransactionXmin`; pure read, no logic. |

- All four owners (clog, subtrans, snapmgr) are unported catalog units; a
  direct cargo dependency cannot exist. Seams are the established pattern for
  this boundary in the repo.
- No `set()` calls outside the owner anywhere in `crates/` except the port's
  own test fixtures (`src/tests.rs`), which is the sanctioned test pattern.
- `elog(WARNING, ...)` is a direct dependency on `backend-utils-error`
  (already merged), not a seam — correct.
- No body of any transam.c function was replaced by a seam; all logic lives in
  this crate.

## Build and tests

`cargo test -p backend-access-transam-transam -p backend-access-transam-clog-seams
-p backend-access-transam-subtrans-seams -p backend-utils-time-snapmgr-seams`
passes (12 unit tests covering the cache, special xids, subcommit recursion,
wraparound comparisons, tree wrappers, commit-LSN paths).

## Verdict

**PASS** — 15/15 functions MATCH; zero seam findings.
