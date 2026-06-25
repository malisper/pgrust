# Audit: backend-storage-lmgr-deadlock

C source: `src/backend/storage/lmgr/deadlock.c` (1162 lines, 13 functions incl.
`DEBUG_DEADLOCK`-only `PrintLockQueue`).
c2rust: `c2rust-runs/backend-storage-lmgr-deadlock/src/deadlock.rs`.
Port crates: `crates/types-deadlock`, `crates/backend-storage-lmgr-deadlock`,
`crates/backend-storage-lmgr-deadlock-seams`.

## Function inventory and verdicts

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `InitDeadLockChecking` (144) | detector.rs `init_dead_lock_checking` (150) | MATCH | All 8 palloc'd arrays sized exactly: visitedProcs/deadlockDetails/topoProcs/beforeConstraints/afterConstraints = MaxBackends; waitOrders = MaxBackends/2; waitOrderProcs = MaxBackends; curConstraints = MaxBackends; possibleConstraints = 4*MaxBackends. StaticAssertStmt(MAX_BACKENDS_BITS <= 32-3) -> compile-time const assert. palloc OOM -> fallible try_reserve_exact -> PgResult. topoProcs is a dedicated buffer (C reuses visitedProcs space; never concurrent, observably identical). |
| 2 | `DeadLockCheck` (220) | detector.rs `dead_lock_check` (230) | MATCH | Reset nCur/nPossible/nWaitOrders/blocking_av; recurse; hard path re-runs FindLockCycle(proc, base=0), elog(FATAL)->Err(FATAL) "deadlock seems to have disappeared", return HardDeadlock. Rearrange loop: Assert(nProcs==count)->debug_assert, dclist reset+push_tail -> wait_procs=procs, ProcLockWakeup seam. Return SOFT/BLOCKED_BY_AV/NO per nWaitOrders>0 / blocking_av set / else. TRACE probe no-op. |
| 3 | `GetBlockingAutoVacuumPgproc` (290) | detector.rs `get_blocking_auto_vacuum_pgproc` (312) | MATCH | take() reads-and-clears, exactly C read-then-NULL. |
| 4 | `DeadLockCheckRecurse` (312) | detector.rs `dead_lock_check_recurse` (326) | MATCH | nEdges<0->true, ==0->false, nCur>=maxCur->true. savedList room test nPossible+nEdges+MaxBackends<=maxPossible increments nPossible. Per-edge: !savedList&&i>0 re-runs TestConfiguration, mismatch->elog(FATAL)->Err. Push curConstraints[nCur]=possible[old+i], recurse; false-> Ok(false) WITHOUT restoring nPossible (matches C early return). End restores nPossible=old, returns true. |
| 5 | `TestConfiguration` (378) | detector.rs `test_configuration` (397) | MATCH | Room check nPossible+MaxBackends>maxPossible -> -1. softEdges base=nPossible. ExpandConstraints fail -> -1. Loop waiter then blocker FindLockCycle; nSoftEdges==0 -> -1 hard; else softFound. startProc checked last. Returns softFound. |
| 6 | `FindLockCycle` (446) | detector.rs `find_lock_cycle` (461) | MATCH | Resets nVisited/nDeadlockDetails/*nSoftEdges, recurse depth 0. soft_edges_base = C softEdges pointer (index into possibleConstraints). |
| 7 | `FindLockCycleRecurse` (457) | detector.rs `find_lock_cycle_recurse` (477) | MATCH | Group-leader redirect; visited scan (i==0 -> deadlock, nDeadlockDetails=depth, true; else false); mark seen; links.next!=NULL && waitLock!=NULL -> is_on_wait_queue && wait_lock.is_some() then RecurseMember; lock-group-members fan-out with memberProc!=checkProc guard. |
| 8 | `FindLockCycleRecurseMember` (536) | detector.rs `find_lock_cycle_recurse_member` (573) | MATCH | LOCKTAG_RELATION_EXTEND(=1) early-out via locktag_type. conflictMask=conflictTab[waitLockMode]. Hard scan: leader!=checkProcLeader, lm 1..=numLockModes, holdMask&LOCKBIT_ON(lm)&&conflictMask&LOCKBIT_ON(lm); recurse fills deadlockDetails[depth]={tag,waitLockMode,pid}; autovacuum checkProc==MyProc && statusFlags&PROC_IS_AUTOVACUUM(0x01) -> blocking_av; break. Soft scan: waitOrders lookup (idx=nWaitOrders default, set on match, test idx<nWaitOrders); hypothetical branch breaks at leader==checkProcLeader; true branch finds lastGroupMember then scans with leader!=checkProcLeader guard. Soft edge appended at possibleConstraints[base+*nSoftEdges]. |
| 9 | `ExpandConstraints` (790) | detector.rs `expand_constraints` (787) | MATCH | Backward scan; dedup lock against existing waitOrders; allocate WAIT_ORDER {lock, procs_off, nProcs=dclist_count}; nWaitOrderProcs advance; TopoSort(lock, constraints, i+1) fail -> false; nWaitOrders++. |
| 10 | `TopoSort` (862) | detector.rs `topo_sort` (850) | MATCH | Fill topoProcs from waitQueue; MemSet before/after[0..queue_size]=0; representative selection (highest-index match -> jj/kk, other group members -> before=-1); before[jj]++, pred/link/afterConstraints list. Backward emit: skip NULLs, find last with before==0, j<0->false, emit whole lock group consecutively into ordering[i-nmatches], decrement predecessors via afterConstraints/link chain. cons[] local copy mirrors C in-place pred/link scratch mutation (no caller reads it back). |
| 11 | `PrintLockQueue` (1053) | detector.rs `print_lock_queue` (1035) | MATCH | Behind debug_deadlock feature (C #ifdef DEBUG_DEADLOCK); prints info/lock/pids + flush. |
| 12 | `DeadLockReport` (1075) | detector.rs `dead_lock_report`/`build_dead_lock_report`/`render_dead_lock_report` (1053) | MATCH | client lines "Process %d waits for %s on %s; blocked by process %d." with nextpid wrap (last->details[0]); DescribeLockTag + GetLockmodeName(lockmethodid, lockmode) seams; logbuf=client copy then per-proc "Process %d: %s" via pgstat_get_backend_current_activity seam; pgstat_report_deadlock seam; ereport(ERROR, errcode 40P01, "deadlock detected", errdetail_internal, errdetail_log, errhint). pg_noreturn -> returns PgError. OOM degrades to bare deadlock error (still reports + ERROR). |
| 13 | `RememberSimpleDeadLock` (1147) | detector.rs `remember_simple_dead_lock` (1181) | MATCH | details[0]={lock.tag, lockmode, proc1.pid}; details[1]={proc2.waitLock.tag, proc2.waitLockMode, proc2.pid}; nDeadlockDetails=2. |

## Constants verified against headers

- `LOCKBIT_ON(m) = 1 << m` (lock.h:85) -> `lockbit_on`. MATCH.
- `LOCK_LOCKTAG` reads `tag.locktag_type` (lock.h:326). MATCH.
- `LOCKTAG_RELATION_EXTEND` = enum index 1 (lock.h:138-139) -> types-storage `= 1`. MATCH.
- `PROC_IS_AUTOVACUUM = 0x01` (proc.h:57) -> types-storage `0x01`. MATCH.
- `MAX_LOCKMODES = 10` (lock.h:83) -> types-storage `10`. MATCH.
- `MAX_BACKENDS_BITS = 18` (procnumber.h:38) -> types-storage `18`. MATCH.
- `ERRCODE_T_R_DEADLOCK_DETECTED = 40P01` (errcodes.txt:334) -> types-error make_sqlstate("40P01"). MATCH.
- `LOCKMASK`/`LOCKMODE` = i32; conflictTab indexed by lockmode. MATCH.

## Seam audit

Owned seam crate (C-source coverage of deadlock.c): `backend-storage-lmgr-deadlock-seams`.
All 5 declarations (init_dead_lock_checking, dead_lock_check,
get_blocking_auto_vacuum_pgproc, dead_lock_report, remember_simple_dead_lock) are
installed by init_seams() in lib.rs (only set() calls). seams-init::init_all()
calls backend_storage_lmgr_deadlock::init_seams() (seams-init/src/lib.rs:46). No
uninstalled declaration; no set() outside owner.

Outward seam calls (genuine externals owned by other units, real cycles), all thin
marshal+delegate, no branching/computation: max_backends (globals.c),
proc_lock_wakeup (proc.c), get_lock_method_table/get_lockmode_name (lock.c),
describe_lock_tag (lmgr.c), report_deadlock (pgstat), backend_current_activity
(pgstat/status). MyProc passed as explicit dead_lock_check parameter
(my_proc: Option<ProcId>), not an ambient-global seam. No function body replaced by
a seam call.

## Design conformance

- Shared LOCK/PROCLOCK/PGPROC graph -> LockSpace index-handle arena
  (ProcId/LockId/ProcLockId as shmem-address analogues): the prescribed faithful
  model for a cyclic identity-compared shmem graph; not invented opacity.
- Per-backend file-scope statics -> thread_local owned DeadlockState of Vecs
  (process-local, not a shared static for per-backend globals).
- InitDeadLockChecking allocations fallible (try_reserve_exact -> PgResult, OOM via
  mcx.oom); report builder owns a per-call MemoryContext and returns owned Strings.
- Both elog(FATAL) checks -> Err(PgError, FATAL); the ERROR report -> PgError
  (pg_noreturn preserved by returning the error to caller).
- No locks held across ?, no registry-shaped side tables, no unledgered divergence.

## Verdict: PASS

All 13 functions MATCH. Constants verified against headers. Seams fully installed
and thin; design rules satisfied. Build clean; 7 in-crate tests pass.
