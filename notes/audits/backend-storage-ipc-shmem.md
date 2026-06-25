# Audit: backend-storage-ipc-shmem

Unit: `backend-storage-ipc-shmem` (`src/backend/storage/ipc/shmem.c`)
C source: `../pgrust/postgres-18.3/src/backend/storage/ipc/shmem.c`
c2rust: `../pgrust/c2rust-runs/backend-storage-ipc-shmem/src/shmem.rs`
Port: `crates/backend-storage-ipc-shmem/src/lib.rs`
Seam crates audited: `backend-utils-hash-dynahash-seams`, `port-pg-numa-seams`,
`backend-port-sysv-shmem-seams`, plus owned `backend-storage-ipc-shmem-seams`.

## Function inventory and verdicts

| # | C function (loc) | Port loc | Verdict | Notes |
|---|---|---|---|---|
| 1 | `InitShmemAccess` (shmem.c:101) | lib.rs:121 | MATCH | seghdr/base/end set; end = base + totalsize. |
| 2 | `InitShmemAllocation` (shmem.c:114) | lib.rs:132 | MATCH | ShmemLock via ShmemAllocUnlocked(sizeof slock_t), SpinLockInit; `aligned = CACHELINEALIGN(absolute addr)`, freeoffset = aligned - hdr; index=NULL. Assert→assert!. |
| 3 | `ShmemAlloc` (shmem.c:151) | lib.rs:165 | MATCH | ShmemAllocRaw; NULL → ereport(ERROR, ERRCODE_OUT_OF_MEMORY, "out of shared memory (%zu bytes requested)"). |
| 4 | `ShmemAllocNoError` (shmem.c:171) | lib.rs:183 | MATCH | ShmemAllocRaw, returns NULL; installed as HASHCTL.alloc. |
| 5 | `ShmemAllocRaw` (static, shmem.c:185) | lib.rs:207 | MATCH | size=CACHELINEALIGN; *allocated_size=size; SpinLock guard; newFree<=totalsize → bump, else NULL. checked_add treats overflow as out-of-space (same observable result; C wrap would also exceed totalsize). |
| 6 | `ShmemAllocUnlocked` (shmem.c:237) | lib.rs:246 | MATCH | size=MAXALIGN; no lock; newFree>totalsize → ereport OOM; else bump. |
| 7 | `ShmemAddrIsValid` (shmem.c:273) | lib.rs:279 | MATCH | base <= addr < end. |
| 8 | `InitShmemIndex` (shmem.c:282) | lib.rs:285 | MATCH | keysize=SHMEM_INDEX_KEYSIZE(48), entrysize=sizeof(ShmemIndexEnt); ShmemInitHash("ShmemIndex",64,64,info,HASH_ELEM\|HASH_STRINGS). |
| 9 | `ShmemInitHash` (shmem.c:331) | lib.rs:310 | MATCH | dsize=max_dsize=hash_select_dirsize(max_size); alloc=ShmemAllocNoError; flags\|=HASH_SHARED_MEM\|HASH_ALLOC\|HASH_DIRSIZE; ShmemInitStruct(hash_get_shared_size); found→HASH_ATTACH; hctl=location; hash_create. dynahash via seam (cycle). |
| 10 | `ShmemInitStruct` (shmem.c:386) | lib.rs:357 | MATCH | LWLockAcquire(ShmemIndexLock,LW_EXCLUSIVE) RAII guard; null-index bootstrap (under-postmaster attach vs standalone ShmemAlloc); hash_search(HASH_ENTER_NULL); null→OOM ereport+remove not needed; found size-mismatch ereport; not-found ShmemAllocRaw→on-NULL HASH_REMOVE+OOM ereport; sets size/allocated_size/location. foundPtr folded into return tuple. |
| 11 | `add_size` (shmem.c:492) | lib.rs:473 | MATCH | checked_add; overflow → ereport(ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED). |
| 12 | `mul_size` (shmem.c:509) | lib.rs:488 | MATCH | zero-shortcut; checked_mul; overflow → same ereport. |
| 13 | `pg_get_shmem_allocations` (shmem.c:526) | lib.rs:515 | MATCH | InitMaterializedSRF; LW_SHARED; hash_seq over index; per-ent key/offset/size/allocated_size; anonymous row (nulls[1]); unused row. Reads freeoffset/totalsize after loop, as C. |
| 14 | `pg_get_shmem_allocations_numa` (shmem.c:583) | lib.rs:597 | MATCH | pg_numa_init==-1→elog; InitMaterializedSRF; max_node; nodes[max+2]; pagesize; total_page_count=total/pg+1; per-ent page-range align down/up, memset 0xff(-1), touch+CHECK_FOR_INTERRUPTS, query_pages -1→%m, node tally (s in [0,max], -2 ENOENT, else elog), per-node rows + no-node row. `values[1]=i` raw int → from_i64 (equivalent on LE64). |
| 15 | `pg_get_shmem_pagesize` (shmem.c:754) | lib.rs:769 | MATCH | sysconf(_SC_PAGESIZE); HUGE_PAGES_ON → GetHugePageSize seam. WIN32 branch n/a. huge_pages_status passed explicitly (no ambient global). |
| 16 | `pg_numa_available` (shmem.c:776) | lib.rs:789 | MATCH | pg_numa_init != -1. |
| H | `pg_numa_touch_mem_if_required` (pg_numa.h static inline) | lib.rs:585 | MATCH | volatile uint64 read; ported in-crate (inline helper, not seamed). |

## Constants verified against headers

- `SHMEM_INDEX_KEYSIZE = 48`, `SHMEM_INDEX_SIZE = 64` (storage/shmem.h:51,53). MATCH.
- `PG_CACHE_LINE_SIZE = 128` (pg_config_manual.h:212), `MAXIMUM_ALIGNOF = 8` (64-bit profile). MATCH.
- HASH flags: DIRSIZE 0x0004, ELEM 0x0008, STRINGS 0x0010, ALLOC 0x0200,
  SHARED_MEM 0x0800, ATTACH 0x1000 (hsearch.h:94-104). MATCH in types-hash.
- HASHACTION: FIND 0, ENTER 1, REMOVE 2, ENTER_NULL 3 (hsearch.h). MATCH.
- LW_EXCLUSIVE 0, LW_SHARED 1 (lwlock.h enum). MATCH in types-storage.
- `ShmemIndexLock = 1` (lwlocklist.h:34 `PG_LWLOCK(1, ShmemIndex)`); SHMEM_INDEX_LOCK=1. MATCH.
- HUGE_PAGES_OFF/ON/UNKNOWN (pg_shmem.h:53-56). MATCH.
- ERRCODE_OUT_OF_MEMORY / ERRCODE_PROGRAM_LIMIT_EXCEEDED used as in C.
- TYPEALIGN/TYPEALIGN_DOWN/CACHELINEALIGN/MAXALIGN (c.h:773-785). MATCH.

## Seam audit

Outward seams are all justified by real dependency cycles or unported owners,
and are thin marshal + delegate:

- `backend-utils-hash-dynahash-seams` (hash_create/search/select_dirsize/
  get_shared_size/seq_init/seq_search): genuine cycle — dynahash needs shmem's
  ShmemInitStruct/add_size/mul_size, so shmem cannot depend on dynahash
  directly. out-params/foundPtr folded into returns. Justified.
- `port-pg-numa-seams` (pg_numa_init/query_pages/get_max_node): owner is an
  unported port-batch unit. count + arrays folded into equal-length slices.
- `backend-port-sysv-shmem-seams` (get_huge_page_size): owner unported;
  two out-params folded into tuple.
- `backend-storage-ipc-shmem-seams` (owned): shmem_init_struct, add_size,
  mul_size, shmem_lock_acquire/release — all installed by this crate's
  `init_seams()`, which is `set()`-only, and `seams-init::init_all()` calls
  `backend_storage_ipc_shmem::init_seams()` (lib.rs:29).

Direct deps (not seamed): `backend-storage-lmgr-lwlock` (LWLockAcquireMain),
`backend-storage-lmgr-s-lock` (Spinlock), funcapi/varlena/tcop-postgres seams
for the SRFs. No computation/branching in any seam path. No uninstalled seam,
no `set()` outside the owner.

## Design conformance

- SRFs and allocating fns take `Mcx` and return `PgResult` (allocate via mcx
  PgVec / cstring_to_text seam). OK.
- `huge_pages_status` passed as an explicit param, not read as an ambient
  global. OK.
- ShmemIndexLock held across `?` only via the RAII `LWLockAcquireMain` guard;
  every error path calls `guard.release()` before ereport (matching C
  LWLockRelease-before-ereport). OK.
- Per-backend C globals (ShmemSegHdr/Base/End/Lock/Index, firstNumaTouch) are
  `thread_local!` (pointers into the shared mapping, inherited at fork), not
  shared statics. Pointee state synchronized by in-segment ShmemLock spinlock
  and ShmemIndexLock, as in C. OK.
- No invented opacity, no registry side tables, no unledgered divergence
  markers.

## Verdict: PASS

All 16 functions plus the inline helper MATCH. Every constant verified against
the 18.3 headers. Seams justified, thin, and fully wired. Build and the 4 crate
tests pass.
