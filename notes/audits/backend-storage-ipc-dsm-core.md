# Audit: backend-storage-ipc-dsm-core

Unit: `backend-storage-ipc-dsm-core` (dsm.c + dsm_impl.c, plus ipc.c as the
documented cycle partner).

- C sources: `src/backend/storage/ipc/dsm.c` (1303 lines),
  `src/backend/storage/ipc/dsm_impl.c` (1053 lines),
  `src/backend/storage/ipc/ipc.c` (446 lines), Postgres 18.3.
- c2rust cross-check: `c2rust-runs/backend-storage-ipc-dsm-core/src/{dsm,dsm_impl}.rs`
  and `c2rust-runs/backend-storage-ipc-small/src/ipc.rs` (ipc.c is bundled in
  other catalog units; both renderings inspected).
- Port: `crates/backend-storage-ipc-dsm-core/src/{dsm,dsm_impl,ipc}.rs`.
- Auditor: independent re-derivation from the C; constants verified against
  headers (`dsm_impl.h`, `dsm.h`, `pg_shmem.h`, `lwlocklist.h`, `freepage.h`,
  `file_perm.h`, `portability/mem.h`, generated wait events via c2rust).

## Inventory and per-function comparison

Every function definition in the three C files, including statics and inline
helpers. The c2rust rendering confirms which `#if` branches were in the build
config: `dsm_impl_windows`, `dsm_set_control_handle` (EXEC_BACKEND), the
EXEC_BACKEND body of `dsm_backend_startup`, and the PROFILE_PID_DIR block of
`proc_exit` are all absent from c2rust (excluded by config), matching the
port's platform scope.

### ipc.c

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `proc_exit` | ipc.c:104 | ipc.rs `proc_exit` | MATCH | getpid guard -> `elog(PANIC)`; PANIC verified to `abort()` in backend-utils-error (`stack.rs:277`), so control cannot continue past it, as in C. PROFILE_PID_DIR block config-excluded. DEBUG3 `exit(%d)` then `exit(code)`. Installed as the `backend-storage-ipc-seams::proc_exit` impl (`fn(i32) -> !`). |
| `proc_exit_prepare` (static) | ipc.c:165 | ipc.rs `proc_exit_prepare` | MATCH | Sets `proc_exit_inprogress` via the owning config; clears Interrupt/ProcDie/QueryCancel pending, holdoff=1, crit=0, error context stack, debug_query_string; `shmem_exit(code)`; DEBUG3 with live count; pops `on_proc_exit` newest-first, index zeroed after. The dropped `Err`s are unreachable: with `proc_exit_inprogress` set, errstart promotes ERROR->FATAL (`stack.rs:93`) and FATAL calls `proc_exit(1)` (`stack.rs:274`), reproducing the C re-entry exactly. |
| `shmem_exit` | ipc.c:228 | ipc.rs `shmem_exit` | MATCH | inprogress flag; `LWLockReleaseAll` seam; before_shmem list pop-then-call; hard-coded `dsm_backend_shutdown()`; on_shmem list; flag cleared. Callback `?` propagation = the C longjmp, with each callback unregistered before invocation so re-entry resumes correctly. |
| `atexit_callback` (static) | ipc.c:300 | ipc.rs `atexit_callback` | MATCH | `extern "C"`, registered via `libc::atexit`, calls `proc_exit_prepare(-1)`. |
| `on_proc_exit` | ipc.c:315 | ipc.rs `on_proc_exit` (via `register`) | MATCH | MAX_ON_EXITS=20; FATAL `ERRCODE_PROGRAM_LIMIT_EXCEEDED` (54000) `errmsg_internal("out of on_proc_exit slots")`; one-time atexit setup after registration, as in C. |
| `before_shmem_exit` | ipc.c:343 | ipc.rs `before_shmem_exit` | MATCH | Same shape, message "out of before_shmem_exit slots". |
| `on_shmem_exit` | ipc.c:371 | ipc.rs `on_shmem_exit` | MATCH | Same shape, message "out of on_shmem_exit slots". |
| `cancel_before_shmem_exit` | ipc.c:400 | ipc.rs `cancel_before_shmem_exit` | MATCH | Latest-entry-only removal comparing fn pointer + Datum; `elog(ERROR, "... is not the latest entry")` otherwise (`%p`/`0x%PRIxPTR` -> `{:#x}`). |
| `on_exit_reset` | ipc.c:422 | ipc.rs `on_exit_reset` | MATCH | Zeroes the three indexes (also clears slots — unobservable) and calls `reset_on_dsm_detach()`. |
| `check_on_shmem_exit_lists_are_empty` | ipc.c:438 | ipc.rs same name | MATCH | Two `elog(FATAL)` checks in C order. |

Statics: `proc_exit_inprogress` is owned by backend-utils-error's config (set
through its setter, read by the promotion logic — the actual C consumer);
`shmem_exit_inprogress`, `atexit_callback_setup`, and the three
list+index pairs are thread_locals here. MATCH.

### dsm_impl.c

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `dsm_impl_op` | dsm_impl.c:158 | dsm_impl.rs `dsm_impl_op` | MATCH | Both asserts as debug_asserts; dispatch on `dynamic_shared_memory_type` over posix/sysv/mmap; default arm `elog(ERROR, "unexpected dynamic shared memory type: %d")` then `false`. Windows arm config-excluded. |
| `dsm_impl_posix` (static) | dsm_impl.c:211 | dsm_impl.rs `dsm_impl_posix` | MATCH | Name `"/PostgreSQL.%u"`; teardown munmap + conditional shm_unlink with the exact messages; Reserve/ReleaseExternalFD seams around open; `O_RDWR | (CREATE ? O_CREAT|O_EXCL : 0)`, mode `PG_FILE_MODE_OWNER` (S_IRUSR|S_IWUSR, verified file_perm.h:38); silent return on CREATE+EEXIST; fstat path for ATTACH; resize for CREATE with unlink backout; mmap with `MAP_SHARED|MAP_HASSEMAPHORE|MAP_NOSYNC` (both 0-defaulted exactly as `portability/mem.h` does per-platform); every backout sequence (save errno, close, ReleaseExternalFD, conditional unlink, restore) in C order. |
| `dsm_impl_posix_resize` (static) | dsm_impl.c:350 | dsm_impl.rs same name | MATCH | `IsUnderPostmaster`-gated `sigprocmask(SIG_SETMASK, BlockSig, &save)`; wait event `WAIT_EVENT_DSM_ALLOCATE` = 0x0A000019 (=167772185, verified against c2rust); Linux: posix_fallocate EINTR loop + `errno = rc`; non-Linux: ftruncate EINTR loop; wait end; sigmask restore preserving errno. |
| `dsm_impl_sysv` (static) | dsm_impl.c:422 | dsm_impl.rs `dsm_impl_sysv` | MATCH | Name `"%u"`; key cast + negative flip; `IPC_PRIVATE` (=0) check with DEBUG4 + `errno = EEXIST`; ident cache as inline enum variant instead of the C TopMemoryContext `int` allocation (documented; the only behavioral delta is the C allocator's own OOM surface, which has no Rust counterpart — no logic lives there); shmget with `IPCProtection` (0600, mem.h:15) and conditional `IPC_CREAT|IPC_EXCL`+size; EEXIST-silent create failure; teardown shmdt + `shmctl(IPC_RMID)` with cache cleared first; `IPC_STAT` size for ATTACH; `shmat(..., PG_SHMAT_FLAGS=0)` with `(void*)-1` check and CREATE-only RMID backout. |
| `dsm_impl_windows` (static) | dsm_impl.c:609 | — | MATCH (config-excluded) | `USE_DSM_WINDOWS` not in build config (absent from c2rust); out of platform scope. |
| `dsm_impl_mmap` (static) | dsm_impl.c:791 | dsm_impl.rs `dsm_impl_mmap` | MATCH | Name `pg_dynshmem/mmap.%u` (verified dsm_impl.h:51-52); teardown munmap/unlink; OpenTransientFile seam, EEXIST-silent create; fstat for ATTACH; ZBUFFER_SIZE=8192 zero-fill loop with `WAIT_EVENT_DSM_FILL_ZERO_WRITE` (0x0A00001A=167772186 verified) bracketing each write, `errno = save ? save : ENOSPC` on failure; mmap + CREATE-only unlink backout; final CloseTransientFile failure path uses `errcode_for_file_access` and message "could not close ..." as in C. C pallocs the zero buffer for alignment; a stack array serves identically. |
| `dsm_impl_pin_segment` | dsm_impl.c:962 | dsm_impl.rs same name | MATCH | Non-Windows body is a no-op leaving the pm handle NULL/0; Windows branch config-excluded. |
| `dsm_impl_unpin_segment` | dsm_impl.c:1013 | dsm_impl.rs same name | MATCH | Same. |
| `errcode_for_dynamic_shared_memory` (static) | dsm_impl.c:1046 | dsm_impl.rs `sqlstate_for_dynamic_shared_memory` + `report_errno` | MATCH | EFBIG/ENOMEM -> `ERRCODE_OUT_OF_MEMORY` (53200) else `errcode_for_file_access()`; takes the saved errno explicitly (the C callers' errno is unclobbered at the call point, so equivalent). |

File-scope data: `dynamic_shared_memory_options` (posix/sysv/mmap, windows
entry config-excluded, NULL terminator -> slice); `dynamic_shared_memory_type`
default `DSM_IMPL_POSIX` (=1; constants 1/2/3/4 verified dsm_impl.h:17-20);
`min_dynamic_shared_memory` default 0; both thread_local with GUC setters.
MATCH.

### dsm.c

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `ResourceOwnerRememberDSM` (static inline) | dsm.c:159 | `DsmSegment` guard construction | MATCH | RAII translation per `docs/query-lifecycle-raii.md` (resowner.c is not ported). |
| `ResourceOwnerForgetDSM` (static inline) | dsm.c:164 | `DsmSegment::into_id` | MATCH | Consume-without-detach. |
| `dsm_postmaster_startup` | dsm.c:176 | dsm.rs same name | MATCH | mmap-type cleanup call; `maxitems = 64 + 5*MaxBackends` (PG_DYNSHMEM_FIXED_SLOTS/SLOTS_PER_BACKEND verified); DEBUG2; even-handle prng loop skipping `DSM_HANDLE_INVALID` (=0, dsm_impl.h:58) with CREATE at ERROR; globals stored; `on_shmem_exit(dsm_postmaster_shutdown, shim)`; DEBUG2; `shim->dsm_control = handle`; magic/nitems/maxitems init — exact C order. `PG_DYNSHMEM_CONTROL_MAGIC` 0x9a503d32 verified dsm.c:50. |
| `dsm_cleanup_using_control_segment` | dsm.c:237 | dsm.rs same name | MATCH | ATTACH at DEBUG1, quiet bail; sanity check else DETACH at LOG; per-item refcnt==0 / main-region skips, DEBUG2 with refcnt, DESTROY at LOG with junk outs; final DESTROY of the old control segment at LOG. |
| `dsm_cleanup_for_mmap` (static) | dsm.c:319 | dsm.rs same name | MATCH | AllocateDir/ReadDir/FreeDir via file seams (opaque token); prefix match on `mmap.`; DEBUG2; unlink failure -> `ereport(ERROR, errcode_for_file_access, "could not remove file \"%s\": %m")`; FreeDir skipped on the ERROR path exactly as the C longjmp does. |
| `dsm_postmaster_shutdown` (static) | dsm.c:357 | dsm.rs same name | MATCH | Registered via `on_shmem_exit` with the shim pointer as Datum; nitems read before the sanity check (C order); LOG "dynamic shared memory control segment is corrupt" + return; per-item destroy loop at LOG; control segment DESTROY at LOG with write-back of all three globals (`dsm_control = dsm_control_address` is written back here in C, unlike `dsm_detach_all`); `shim->dsm_control = 0`. |
| `dsm_backend_startup` (static) | dsm.c:422 | dsm.rs same name | MATCH | EXEC_BACKEND branch config-excluded (c2rust body is just the flag); `dsm_init_done = true`. |
| `dsm_set_control_handle` | dsm.c:458 | — | MATCH (config-excluded) | EXEC_BACKEND only; absent from c2rust. |
| `dsm_estimate_size` | dsm.c:469 | dsm.rs same name | MATCH | `1024*1024 * (size_t) min_dynamic_shared_memory`. |
| `dsm_shmem_init` | dsm.c:478 | dsm.rs same name | MATCH | size==0 early return; `ShmemInitStruct("Preallocated DSM", size)` seam returning (ptr, found); `!found`: first_page loop against `sizeof(FreePageManager)` (seam), `FreePageManagerInitialize(fpm, begin)`, `pages = size/FPM_PAGE_SIZE - first_page`, `FreePageManagerPut`. FPM_PAGE_SIZE=4096 verified freepage.h:30. |
| `dsm_create` | dsm.c:515 | dsm.rs same name | MATCH | Descriptor first (OOM surface via try_reserve = the C MemoryContextAlloc ereport); npages round-up; lock + `FreePageManagerGet` main-region carve-out with mapped fields set under the lock; else release (only if fpm), even-handle prng loop with CREATE at ERROR, re-acquire; slot scan for refcnt==0 writing handle/refcnt=2/pm_handle/pinned with main-region `make_main_region_dsm_handle(i)` + first_page/npages; full path: FreePageManagerPut (main) under lock, release, DESTROY at WARNING (non-main), descriptor destroyed, then `Ok(None)` iff `DSM_CREATE_NULL_IF_MAXSEGMENTS` (0x0001, dsm.h:20) else `ereport(ERROR, ERRCODE_INSUFFICIENT_RESOURCES=53000, "too many dynamic shared memory segments")`; new-slot path writes `item[nitems]` (C writes `item[i]` with i==nitems — identical), nitems++, release. Guard drop on the ERROR unwind = the C resowner cleanup. |
| `dsm_attach` | dsm.c:664 | dsm.rs same name | MATCH | init-done; duplicate-handle scan -> `elog(ERROR, "can't attach the same segment more than once")`; descriptor; lock; scan skipping refcnt<=1 (moribund-handle-reuse comment preserved), refcnt++, control_slot, main-region address/size from first_page/npages; release; INVALID_CONTROL_SLOT ((uint32)-1 = u32::MAX) -> detach + `Ok(None)`; non-main ATTACH at ERROR with guard-drop cleanup on unwind (C resowner). |
| `dsm_backend_shutdown` | dsm.c:756 | dsm.rs same name | MATCH | Detach list head (newest = Vec back) until empty. |
| `dsm_detach_all` | dsm.c:774 | dsm.rs same name | MATCH (after fix) | Initially DIVERGES: the port wrote the post-detach address back into the `dsm_control` global (nulling it), but C passes a *local* copy so `dsm_control` stays stale while `dsm_control_impl_private`/`dsm_control_mapped_size` (passed as globals) are updated — observable on any later read (e.g. a second `dsm_detach_all` retries and reports in C but would silently skip in the port). Fixed by dropping the `DSM_CONTROL` write-back; re-audited from scratch: capture-before-loop, head-first detach loop, conditional DETACH at ERROR with in-place global updates, `dsm_control` untouched — exact. |
| `dsm_detach` | dsm.c:802 | dsm.rs same name | MATCH | HOLD_INTERRUPTS; pop-before-invoke callback loop (`?` = the C longjmp leaving interrupts held, as C does); RESUME; unmap-if-mapped (non-main DETACH at WARNING, errors pretended away) and field reset before refcount work; lock, `--refcnt`, slot invalidated, release; refcnt==1: main-region or DESTROY-at-WARNING success gates the relock + FreePageManagerPut (main) + refcnt=0 + release; descriptor removed last. |
| `dsm_pin_mapping` | dsm.c:914 | dsm.rs same name | MATCH | Guard consumed = resowner forgotten/NULLed. |
| `dsm_unpin_mapping` | dsm.c:933 | dsm.rs same name | MATCH | Guard recreated; stale id traps where the C would deref freed memory. |
| `dsm_pin_segment` | dsm.c:954 | dsm.rs same name | MATCH | Lock; already-pinned `elog(ERROR)` with lock left held (C longjmp + recovery LWLockReleaseAll); non-main `dsm_impl_pin_segment`; pinned=true, refcnt++, pm_handle stored; release. |
| `dsm_unpin_segment` | dsm.c:987 | dsm.rs same name | MATCH | Lock; scan skipping refcnt<=1; not-found / not-pinned `elog(ERROR)`s (lock held, as C); non-main `dsm_impl_unpin_segment` before release; `--refcnt == 1` -> destroy flag; pinned=false; release; destroy path with junk outs at WARNING gating relock + FreePageManagerPut (main) + refcnt=0. |
| `dsm_find_mapping` | dsm.c:1075 | dsm.rs same name | MATCH | Linear scan by handle (at most one match — double attach is forbidden). |
| `dsm_segment_address` | dsm.c:1094 | dsm.rs same name | MATCH | Assert -> debug_assert. |
| `dsm_segment_map_length` | dsm.c:1104 | dsm.rs same name | MATCH | Same. |
| `dsm_segment_handle` | dsm.c:1122 | dsm.rs same name | MATCH | |
| `on_dsm_detach` | dsm.c:1131 | dsm.rs same name | MATCH | LIFO push; try_reserve = the C TopMemoryContext OOM surface. |
| `cancel_on_dsm_detach` | dsm.c:1146 | dsm.rs same name | MATCH | First match newest-first (`rposition` = slist head walk), single removal. |
| `reset_on_dsm_detach` | dsm.c:1169 | dsm.rs same name | MATCH | Clears callbacks and invalidates control slots for every attached segment. |
| `dsm_create_descriptor` (static) | dsm.c:1200 | dsm.rs same name | MATCH | Enlarge+alloc OOM first (try_reserve), push-head, slot/private/address/size init, guard returned (resowner remember). |
| `dsm_control_segment_sane` (static) | dsm.c:1236 | dsm.rs same name | MATCH | offsetof-header bound, magic, `dsm_control_bytes_needed(maxitems) > mapped_size`, `nitems > maxitems` — same order. |
| `dsm_control_bytes_needed` (static) | dsm.c:1254 | dsm.rs same name | MATCH | offsetof + sizeof(item) * nitems as u64; `dsm_control_item`/`dsm_control_header` are repr(C) with field-for-field layout (handle u32, refcnt u32, first_page/npages usize, pm_handle pointer-sized, pinned bool). |
| `make_main_region_dsm_handle` (static inline) | dsm.c:1261 | dsm.rs same name | MATCH | `1 | (slot<<1) | prng_u32 << (pg_leftmost_one_pos32(maxitems)+1)`; `pg_leftmost_one_pos32` re-derived (31-clz, undefined at 0 as in C); prng via the ported pg-prng xoroshiro128ss `next_u32` (high 32 bits, same as C). |
| `is_main_region_dsm_handle` (static inline) | dsm.c:1280 | dsm.rs same name | MATCH | `handle & 1`. |
| `ResOwnerReleaseDSM` (static) | dsm.c:1288 | `DsmSegment::drop` | MATCH | Drop detaches if still live; callback errors cannot propagate from Drop and are discarded (C relies on the surrounding abort machinery identically — RAII doc). |
| `ResOwnerPrintDSM` (static) | dsm.c:1296 | `DsmSegment: Debug` | MATCH | "dynamic shared memory segment %u". |

File-scope data: `dsm_init_done`, `dsm_main_space_begin`, `dsm_segment_list`
(Vec, head = back), `dsm_control_handle`, `dsm_control`,
`dsm_control_mapped_size`, `dsm_control_impl_private` — all thread_local
(per-backend in C). `dsm_resowner_desc` is subsumed by the guard. MATCH.

## Constants verified against headers

- `DSM_IMPL_POSIX/SYSV/WINDOWS/MMAP` = 1/2/3/4; default POSIX (dsm_impl.h:17-32).
- `DSM_HANDLE_INVALID` = 0 (dsm_impl.h:58); `DSM_CREATE_NULL_IF_MAXSEGMENTS` = 0x0001 (dsm.h:20).
- `PG_DYNSHMEM_DIR` "pg_dynshmem", `PG_DYNSHMEM_MMAP_FILE_PREFIX` "mmap." (dsm_impl.h:51-52).
- `PG_DYNSHMEM_CONTROL_MAGIC` 0x9a503d32, FIXED_SLOTS 64, SLOTS_PER_BACKEND 5, `INVALID_CONTROL_SLOT` (uint32)-1 (dsm.c:50-55).
- `ZBUFFER_SIZE` 8192 (dsm_impl.c:118); `FPM_PAGE_SIZE` 4096 (freepage.h:30).
- `PG_FILE_MODE_OWNER` S_IRUSR|S_IWUSR (file_perm.h:38); `IPCProtection` 0600 (mem.h:15); `PG_SHMAT_FLAGS` 0 non-Solaris (mem.h:20); MAP_HASSEMAPHORE/MAP_NOSYNC 0-defaults (mem.h:29-38).
- Wait events DSM_ALLOCATE 167772185 / DSM_FILL_ZERO_WRITE 167772186 (c2rust generated values).
- `DynamicSharedMemoryControlLock` = `&MainLWLockArray[34].lock` (lwlocklist.h:67); `PGShmemHeader` field-for-field with `PGShmemMagic` 679834894 (pg_shmem.h:31-40).
- SQLSTATEs: 53000 INSUFFICIENT_RESOURCES, 53200 OUT_OF_MEMORY, 54000 PROGRAM_LIMIT_EXCEEDED (types-error, matches errcodes.h).
- `MAX_ON_EXITS` 20 (ipc.c:72).

## Seam audit

Outward seam calls (all thin marshal + delegate; no branching or node
construction in any seam path):

- `backend-storage-lmgr-lwlock-seams`: new `lwlock_acquire_main`/
  `lwlock_release_main` (built-in lock by lwlocklist offset) and
  `lwlock_release_all`. Justified: lwlock.c is unported and lwlock <-> ipc/dsm
  is cyclic (LWLockReleaseAll runs inside shmem_exit; lwlock error paths reach
  elog -> proc_exit).
- `backend-storage-file-seams` (new crate): AllocateDir/ReadDir/FreeDir/
  OpenTransientFile/CloseTransientFile/Reserve+ReleaseExternalFD. Justified:
  fd.c is unported and depends on elog/ipc (cycle through proc_exit).
- `backend-storage-ipc-shmem-seams` (new crate): ShmemInitStruct. Justified:
  shmem.c unported; shmem.c itself calls ipc/dsm-adjacent machinery.
- `backend-utils-mmgr-freepage-seams` (new crate): FreePageManager init/get/
  put/sizeof. Justified: freepage.c unported; FPM lives in shared memory whose
  layout the owner controls; the `free_page_manager_size()` seam carries
  `sizeof(FreePageManager)` only.
- `backend-utils-activity-waitevent-seams` (new crate):
  pgstat_report_wait_start/end. Justified: wait_event.c unported, pgstat <->
  error/ipc cycle.
- `backend-utils-init-small-seams`: my_proc_pid, max_backends, interrupt-flag
  setters, hold/resume_interrupts. Justified: globals.c/miscadmin owner.
- `backend-tcop-postgres-seams`: reset_debug_query_string. Justified:
  debug_query_string is owned by tcop/postgres.c.
- `backend_utils_error::config`: proc_exit_inprogress / crit_section_count
  setters — direct dep (not a seam), correct owner.

Inward wiring: `crates/backend-storage-ipc-seams` declares only `proc_exit`;
this crate owns ipc.c and installs it — `init_seams()` contains exactly one
`set()` call (`lib.rs:40`), and `seams-init::init_all()` calls
`backend_storage_ipc_dsm_core::init_seams()`. No `set()` calls outside the
owner (grep-verified). The newly declared seam crates above are owned by their
(unported) units and correctly have no installer yet — calls panic loudly,
which is the accepted unported-callee behavior. No function body in this crate
was replaced by a seam call; all dsm.c/dsm_impl.c/ipc.c logic lives here.

## Findings and fixes

1. `dsm_detach_all` (DIVERGES -> fixed, commit on this branch): the port wrote
   the detached (null) control address back into the `dsm_control` global; C
   deliberately passes a local copy so the global retains its stale value
   while `dsm_control_impl_private`/`dsm_control_mapped_size` are updated in
   place. Fixed to match C exactly and re-audited from scratch.

Accepted, documented translation deltas (no behavioral divergence on any
defined input):

- ResourceOwner bookkeeping -> `DsmSegment` RAII guard per
  `docs/query-lifecycle-raii.md` (repo-wide convention; resowner.c unported).
- The System V ident cache is an inline enum variant instead of a
  heap-allocated `int` in TopMemoryContext; the only C-side behavior dropped
  is that allocation's own OOM ereport, which has no logic content.
- `dsm_segment *` -> `DsmSegmentId`; stale ids trap (panic) where the C
  dereferences freed memory (UB).
- Windows / EXEC_BACKEND / PROFILE_PID_DIR code is outside the build config
  (confirmed via c2rust) and outside the repo's platform scope.

## Spot-check of MATCH verdicts

Re-derived in full detail a second time: `dsm_create` (lock hold/release
choreography across the three exits, including the `item[i]` vs `item[nitems]`
aliasing in the new-slot path), `dsm_impl_posix` (all four backout sequences
and the EEXIST-silent create), `proc_exit_prepare` (promotion semantics traced
into backend-utils-error `stack.rs:93/274/277`), and
`make_main_region_dsm_handle` (bit layout against the c2rust rendering).
All confirmed.

## Verdict

**PASS** (after 1 fix round). Every function MATCH; seams justified, thin,
and correctly wired. `cargo build --workspace` and the crate's tests pass.
