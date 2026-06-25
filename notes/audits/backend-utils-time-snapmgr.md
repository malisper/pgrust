# Audit: backend-utils-time-snapmgr

- **Date:** 2026-06-13 (independent re-audit; supersedes the 2026-06-12 FAIL below)
- **Model:** Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)
- **Branch:** `port/backend-utils-time-snapmgr`
- **Unit C source:** `src/backend/utils/time/snapmgr.c` (PostgreSQL 18.3)
- **c2rust:** `../pgrust/c2rust-runs/backend-utils-time-snapmgr/src/snapmgr.rs`
- **Port:** `crates/backend-utils-time-snapmgr/{lib.rs,state.rs}`
- **Owned seam crate:** `crates/backend-utils-time-snapmgr-seams`

## Top-line verdict: **PASS**

Independent from-scratch re-audit (2026-06-13). All 49 C functions are MATCH or
properly SEAMED, and all seam declarations in the owned seam crate are now
installed. The two prior **§3b** failures are confirmed resolved, and one
additional seam-wiring FAIL found during this re-audit was fixed and re-audited
in the same round (see "Re-audit resolution" below). `cargo build -p
backend-utils-time-snapmgr` and `cargo build -p seams-init` both succeed clean.

### Re-audit resolution (2026-06-13)

1. **§3b finding #1 (invented opacity `TupleCidHandle(u64)`) — RESOLVED.**
   `state.rs:102` now holds `tuplecid_data: *mut HTAB` using the real
   `types_hash::hsearch::HTAB` struct (a genuine inherited type, not an invented
   handle-newtype). `SetupHistoricSnapshot(SnapHandle, *mut HTAB)` and
   `HistoricSnapshotGetTupleCids() -> *mut HTAB` match the C signatures
   `HTAB *` exactly. `docs/types.md` rule 6 satisfied.
2. **§3b finding #2 (`INVALID_PID = 0`) — RESOLVED.** `lib.rs:64` now
   `const INVALID_PID: i32 = -1`, matching `miscadmin.h:32 #define InvalidPid
   (-1)` (re-verified against the header).
3. **NEW seam-wiring FAIL found this round, FIXED.** The owned seam crate
   declares **15** seams, but `init_seams()` installed only **11** — four
   declarations (`unregister_snapshot`, `get_active_snapshot`,
   `push_active_snapshot`, `pop_active_snapshot`) were uninstalled despite being
   consumed by `backend-access-index-indexam`, `backend-commands-portalcmds`,
   `backend-commands-matview`, `backend-executor-execParallel`, and
   `backend-utils-cache-plancache` (each would panic at runtime). Per skill §3
   an uninstalled owned-seam declaration is an automatic FAIL. **Fix:** the four
   installs were added to `init_seams()` (lib.rs), each a thin marshal+delegate
   (owned `SnapshotData`/bare `Rc<SnapshotData>` ↔ internal `SnapHandle`, one
   owner call, no branching/computation). Re-audited the fixed installer from
   scratch: all 15 declarations now installed, every closure is pure
   marshal+delegate, `seams-init::init_all()` calls `init_seams()`. Clean.

---

## Prior audit (2026-06-12): FAIL

Logic parity is essentially complete (every C function is MATCH or properly
SEAMED). The failure is in **§3b design conformance**: an invented opacity
type (`TupleCidHandle(pub u64)`) standing in for C's `HTAB *tuplecid_data`,
which `docs/types.md` rule 6 explicitly forbids, plus a transcribed-constant
error (`INVALID_PID = 0`, C `InvalidPid = -1`). Per the skill, any design-rule
violation judged against the diff is merge-blocking the same as a logic
finding. (This supersedes a prior PASS audit on this branch, which did not flag
either item.)

## 1. Function inventory + verdicts

49 function definitions enumerated from the C (including statics and inline
helpers), cross-checked against the c2rust run.

| # | C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `ResourceOwnerRememberSnapshot` | :233 | — | N/A (resowner) | resowner registry dissolved into RAII guards per `docs/query-lifecycle-raii.md`; remember/forget is the caller's guard, not snapmgr logic. |
| 2 | `ResourceOwnerForgetSnapshot` | :238 | — | N/A (resowner) | same. |
| 3 | `GetTransactionSnapshot` | :270 | lib.rs:255 | MATCH | Historic short-circuit, FirstSnapshotSet first-call path (Invalidate + asserts + parallel-mode elog), serializable/xact-snapshot copy + regd_count++ + registered_add, else paths all mirrored. |
| 4 | `GetLatestSnapshot` | :352 | lib.rs:327 | MATCH | parallel-mode elog, historic assert, first-call delegate, SecondarySnapshot fetch. |
| 5 | `GetCatalogSnapshot` | :383 | lib.rs:348 | MATCH | historic short-circuit then NonHistoric. |
| 6 | `GetNonHistoricCatalogSnapshot` | :405 | lib.rs:358 | MATCH | refresh predicate `CatalogSnapshot && !RelationInvalidatesSnapshotsOnly && !RelationHasSysCache`; manual registered_add. |
| 7 | `InvalidateCatalogSnapshot` | :453 | lib.rs:384 | MATCH | remove from registered + NULL + SnapshotResetXmin. |
| 8 | `InvalidateCatalogSnapshotConditionally` | :474 | lib.rs:397 | MATCH | `catalog && active==NULL && singular`. |
| 9 | `SnapshotSetCommandId` | :487 | lib.rs:407 | MATCH | first_snapshot_set guard, sets curcid on Current/Secondary; CatalogSnapshot left open (as C). |
| 10 | `SetTransactionSnapshot` | :508 | lib.rs:429 | MATCH | field copy w/ xcnt/subxcnt asserts (procarray max-count seams), restored vs imported xmin install + same SQLSTATE/detail, serializable copy path. curcid not copied; snapXactCompletionCount=0. |
| 11 | `CopySnapshot` | :605 | lib.rs:533 | MATCH | xip copy; subxip copy gated on `subxcnt>0 && (!suboverflowed || takenDuringRecovery)`; refcounts/copied/snapXactCompletionCount reset. |
| 12 | `FreeSnapshot` | :661 | (Rc drop) | MATCH | C `pfree` when both counts 0 modelled by last `Rc` drop; asserts mirrored at Pop/AtSubAbort/UnregisterNoOwner. |
| 13 | `PushActiveSnapshot` | :679 | lib.rs:569 | MATCH | delegates with `GetCurrentTransactionNestLevel()`. |
| 14 | `PushActiveSnapshotWithLevel` | :693 | lib.rs:574 | MATCH | copy-if `==Current || ==Secondary || !copied` via `ptr_eq`; active_count++; push; level assert. |
| 15 | `PushCopiedSnapshot` | :729 | lib.rs:607 | MATCH | force-copy then push. |
| 16 | `UpdateActiveSnapshotCommandId` | :741 | lib.rs:612 | MATCH | asserts active_count==1/regd_count==0; parallel-mode guard on curcid change. |
| 17 | `PopActiveSnapshot` | :772 | lib.rs:637 | MATCH | pop, active_count--, free-when-zero (Rc drop), SnapshotResetXmin. |
| 18 | `GetActiveSnapshot` | :797 | lib.rs:665 | MATCH | top of stack. |
| 19 | `ActiveSnapshotSet` | :809 | lib.rs:676 | MATCH | `!active.is_empty()`. |
| 20 | `RegisterSnapshot` | :821 | lib.rs:688 | MATCH | InvalidSnapshot→`None`; delegates to OnOwner. |
| 21 | `RegisterSnapshotOnOwner` | :834 | lib.rs:694 | MATCH | copy-if-not-copied, regd_count++, registered_add when ==1. `ResourceOwnerEnlarge`/`Remember` intentionally elided (RAII guard at call site, `docs/query-lifecycle-raii.md`). |
| 22 | `UnregisterSnapshot` | :863 | lib.rs:712 | MATCH | NULL→noop; delegates. |
| 23 | `UnregisterSnapshotFromOwner` | :876 | lib.rs:721 | MATCH | resowner Forget is caller's guard; NoOwner core preserved. |
| 24 | `UnregisterSnapshotNoOwner` | :886 | lib.rs:727 | MATCH | regd_count--, remove-when-zero, free + SnapshotResetXmin when both zero. |
| 25 | `xmin_cmp` | :907 | lib.rs:151 (`registered_min_xmin`) | MATCH | pairing-heap min-by-xmin reproduced by a Vec scan using wraparound-aware `TransactionIdPrecedes`; only the min is ever consumed, so observationally equal. |
| 26 | `SnapshotResetXmin` | :934 | lib.rs:754 | MATCH | active-nonempty early return; empty→Invalid; else advance to min xmin if `MyProc->xmin` precedes. `MyProc->xmin` write via proc seam; TransactionXmin local. |
| 27 | `AtSubCommit_Snapshot` | :958 | lib.rs:779 | MATCH | walk top-down (rev iter), stop at `as_level < level`, relabel to `level-1`. |
| 28 | `AtSubAbort_Snapshot` | :979 | lib.rs:793 | MATCH | pop while `as_level >= level`, active_count--, free-when-zero, SnapshotResetXmin. |
| 29 | `AtEOXact_Snapshot` | :1013 | lib.rs:819 | MATCH | FirstXactSnapshot removal, exported-file unlink (WARNING) + registered removal, InvalidateCatalogSnapshot, commit-time leak WARNINGs, state reset, conditional SnapshotResetXmin, final assert. |
| 30 | `ExportSnapshot` | :1112 | lib.rs:899 | MATCH | subxact guard (25001), committed-children, path `%08X-%08X-%d`, copy+pseudo-register, full text serialization incl. addTopXid predicate and sof overflow branch, tmp-write+rename, basename return. |
| 31 | `pg_export_snapshot` | :1289 | lib.rs:1003 | MATCH | wraps ExportSnapshot(GetActiveSnapshot); fmgr text-Datum wrapping is the systemic fmgr deferral. |
| 32 | `parseIntFromText` | :1304 | lib.rs:1061 | MATCH | prefix check, `%d` scan, newline advance; errors SQLSTATE 22P02. |
| 33 | `parseXidFromText` | :1329 | lib.rs:1070 | MATCH | `%u` scan variant. |
| 34 | `parseVxidFromText` | :1354 | lib.rs:1079 | MATCH | `%d/%u` into vxid. |
| 35 | `ImportSnapshot` | :1384 | lib.rs:1131 | MATCH | fresh-xact guard, isolation guard, idstr charset check (incl. empty-string non-special-case), file read/ENOENT, full field parse with xcnt/subxcnt sanity bounds (procarray max-count seams), validity checks, serializable-source restrictions, cross-db check, SetTransactionSnapshot. |
| 36 | `XactHasExportedSnapshots` | :1571 | lib.rs:1290 | MATCH | `!exported.is_empty()`. |
| 37 | `DeleteAllExportedSnapshotFiles` | :1584 | lib.rs:1009 | MATCH | dir read (fd seam, LOG-on-error/skip ./..), unlink each with LOG-on-failure. |
| 38 | `ThereAreNoPriorRegisteredSnapshots` | :1623 | lib.rs:1295 | MATCH | empty or singular. |
| 39 | `HaveRegisteredOrActiveSnapshot` | :1641 | lib.rs:1300 | MATCH | active→true; catalog-only singular→false; else non-empty. |
| 40 | `SetupHistoricSnapshot` | :1666 | lib.rs:1317 | MATCH | sets historic + tuplecid_data (`*mut HTAB`, real type). Signature matches C `(Snapshot, HTAB *)`. |
| 41 | `TeardownHistoricSnapshot` | :1682 | lib.rs:1324 | MATCH | clears both. |
| 42 | `HistoricSnapshotActive` | :1689 | lib.rs:1332 | MATCH | `historic.is_some()`. |
| 43 | `HistoricSnapshotGetTupleCids` | :1695 | lib.rs:1338 | MATCH | returns `*mut HTAB` tuplecid_data; Assert(HistoricSnapshotActive()) mirrored. |
| 44 | `EstimateSnapshotSpace` | :1709 | lib.rs:1356 | MATCH | 24-byte header + xcnt + (subxcnt when not overflow-or-recovery). C `add_size`/`mul_size` overflow-ereport replaced by plain `usize` arithmetic — only differs on impossible (>~10^9-XID) inputs; identical on all reachable inputs. |
| 45 | `SerializeSnapshot` | :1733 | lib.rs:1369 | MATCH | byte-exact LE layout of `SerializedSnapshotData` (xmin,xmax,xcnt,subxcnt,1+1 bool,2 pad,curcid = 24B); subxcnt zeroed on overflow-and-not-recovery; xip/subxip appended. |
| 46 | `RestoreSnapshot` | :1790 | lib.rs:1408 | MATCH | inverse of Serialize; copied=true, counts 0, snapXactCompletionCount=0. |
| 47 | `RestoreTransactionSnapshot` | :1853 | lib.rs:1280 | MATCH | `SetTransactionSnapshot(snapshot, NULL, InvalidPid, source_pgproc)`; `INVALID_PID = -1` now matches `miscadmin.h:32`. |
| 48 | `XidInMVCCSnapshot` | :1869 | lib.rs:1458 | MATCH | range check (xmin/xmax wraparound-aware), non-recovery full/overflow branches with `SubTransGetTopmostTransaction` seam + xmin recheck, recovery branch searching subxip. |
| 49 | `ResOwnerReleaseSnapshot` | :1968 | lib.rs:727 (target) | MATCH | resowner release callback → `UnregisterSnapshotNoOwner`; the callback wiring is the dissolved resowner registry. |

### Spot-re-derivations (auditor self-check)

- **`SerializeSnapshot`/`RestoreSnapshot` 24-byte layout** re-derived from
  field types: `TransactionId`=u32, `CommandId`=u32, two `bool`s, x86-64 4-byte
  alignment → 16 + 2 + 2 pad + 4 = 24. `SERIALIZED_HEADER_LEN=24` and offsets
  (curcid at 20) match. Round-trip is byte-faithful. MATCH holds.
- **`CopySnapshot` subxip predicate** `subxcnt>0 && (!suboverflowed ||
  takenDuringRecovery)` re-checked against C :644-645 — identical. MATCH holds.
- **`xmin_cmp` → `registered_min_xmin`**: only `pairingheap_first` is consumed,
  and the Vec scan keeps `x` when `TransactionIdPrecedes(x, cur)`; the unordered
  set is observationally equal to the heap. MATCH holds.

## 2. Seam / wiring audit (§3)

**Owned seam crates by C-source coverage:** snapmgr.c maps only to
`crates/backend-utils-time-snapmgr-seams`. It declares **15** seams:
`get_catalog_snapshot`, `register_snapshot`, `unregister_snapshot`,
`estimate_snapshot_space`, `serialize_snapshot`, `restore_snapshot`,
`with_transaction_snapshot`, `snapshot_set_command_id`, `at_eoxact_snapshot`,
`at_subcommit_snapshot`, `at_subabort_snapshot`, `xact_has_exported_snapshots`,
`get_active_snapshot`, `push_active_snapshot`, `pop_active_snapshot`.

After the 2026-06-13 fix, `init_seams()` installs **all 15** with nothing but
`set()` calls (each a thin marshal: `SnapHandle` ↔ owned `SnapshotData` /
bare `Rc<SnapshotData>`). `seams-init` (`crates/seams-init/src/lib.rs:71`) calls
`init_seams()`. **No uninstalled declaration, no `set()` outside the owner.**
Clean.

> The 2026-06-12 audit recorded "11 seams … all installed"; that was a false
> green — the seam crate already declared the four active-stack/unregister
> seams (consumed by indexam/portalcmds/matview/execParallel/plancache) and they
> were not installed. The re-audit caught and fixed it.

**Outward seams consumed** (procarray, predicate, proc, subtrans, fd,
init-small) are each justified by a real dependency cycle and are thin
call-and-marshal at the use site. One observation, not a hard finding:
`get_snapshot_data_into` (lib.rs:215) replays procarray.c's
`MyProc->xmin/TransactionXmin/RecentXmin` post-writes (procarray.c:2152-2155)
in snapmgr, branching on `!TransactionIdIsValid(MyProc->xmin)`. Two of the
three globals (`TransactionXmin`, `RecentXmin`) are genuinely snapmgr-owned
(snapmgr.c:158-159), so hosting the write here is defensible; only the
`MyProc->xmin` field is procarray's, set through the proc seam. The single C
atomic write is split across the seam boundary — acceptable given the ownership
split, but noted.

## 3b. Design-conformance findings (RESOLVED — verdicts as of the 2026-06-13 re-audit; see "Re-audit resolution" at top)

1. ~~**FAIL — invented opacity for `HTAB *tuplecid_data` (`docs/types.md` rule 6).**~~ **RESOLVED** — now `*mut HTAB` (real `types_hash` type).
   C's `tuplecid_data` is `HTAB *` — a typed pointer to dynahash's real struct,
   not a `void *` extension slot. The port models it as
   `state.rs:39 TupleCidHandle(pub u64)`, an invented handle-newtype, justified
   in the doc comment as "an opaque token is sufficient." Rule 6 forbids exactly
   this ("no handle-newtypes or stand-in unit structs for types C spells out …
   restructure the crate layering rather than encoding the layering problem into
   a fake type"); the precedent paragraph bills every stand-in to whoever later
   defines the real type. `HTAB` is not yet ported anywhere in the repo, so the
   correct move is to carry the real type (incrementally populated) or an `Rc`
   to it, not mint a `u64`. Touches
   `SetupHistoricSnapshot`/`HistoricSnapshotGetTupleCids`.

2. ~~**FAIL (minor, transcription) — `INVALID_PID` value wrong.**~~ **RESOLVED** — now `const INVALID_PID: i32 = -1`. (Original text below.) lib.rs:63
   previously defined `const INVALID_PID: i32 = 0`; C `miscadmin.h:32` defines
   `#define InvalidPid (-1)`. Used only in `RestoreTransactionSnapshot` (passed
   as `sourcepid`), where `sourceproc = Some(..)`, so the `sourceproc.is_some()`
   install path is taken and `sourcepid` is never read (it appears only in the
   imported-vxid-path error detail). Currently unobservable — but it is exactly
   the transcribed-constant corruption class the skill flags, and would become
   live if a future caller reaches the vxid path with this sentinel. Fix the
   constant.

### Non-findings (ledgered / design-sanctioned)

- **Per-backend state as `thread_local!` instead of the `Ctx`/`SnapshotStack`
  facet** (`docs/query-lifecycle-raii.md`). The CATALOG.tsv row ledgers this as
  deliberate, following the already-merged `xact` neighbor (which also uses
  ambient `at_eoxact`/`at_subcommit`/`snapshot_set_command_id` seams and its own
  thread_local state). Consistent with the merged dependency and AGENTS.md's
  backend-global rule — a known systemic debt, not a fresh unledgered
  introduction.
- **`ResourceOwnerEnlarge/Remember/Forget` elided** from Register/Unregister:
  the resowner registry is intentionally dissolved into RAII guards
  (`docs/query-lifecycle-raii.md` line 16); the `NoOwner` refcount+heap core
  lives in-crate, so this is not MISSING logic.
- **xid arrays `std::Vec` not `PgVec`**: ledgered DESIGN_DEBT in CATALOG,
  xact-precedent divergence.
- **`add_size`/`mul_size` → plain arithmetic** in EstimateSnapshotSpace:
  differs only on inputs that cannot occur (xcnt bounded by
  GetMaxSnapshotXidCount).

## Build

`cargo build -p backend-utils-time-snapmgr` succeeds clean.

## Conclusion (2026-06-13 re-audit)

All 49 C functions are MATCH or properly SEAMED — logic parity is met. The two
prior §3b design-conformance failures are confirmed resolved (`*mut HTAB`
real-type for `tuplecid_data`; `INVALID_PID = -1`). One additional seam-wiring
FAIL surfaced this round — four owned-seam declarations
(`unregister_snapshot`, `get_active_snapshot`, `push_active_snapshot`,
`pop_active_snapshot`) were declared and consumed but never installed by
`init_seams()` — and was fixed in the same round by adding the four thin
marshal+delegate installs and re-audited clean. With all 15 owned-seam
declarations now installed and `seams-init::init_all()` wiring them, the audit
**PASSES**. The unit may merge (do not merge from this audit lane).
