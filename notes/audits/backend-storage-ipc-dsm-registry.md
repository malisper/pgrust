# Audit: backend-storage-ipc-dsm-registry

Catalog unit: `backend-storage-ipc-dsm-registry` (C source `src/backend/storage/ipc/dsm_registry.c`).
Crates audited: `backend-storage-ipc-dsm-registry`, `backend-utils-mmgr-dsa-seams`, `backend-lib-dshash-seams`.

Sources cross-referenced:
- C: `../pgrust/postgres-18.3/src/backend/storage/ipc/dsm_registry.c`
- c2rust: `../pgrust/c2rust-runs/backend-storage-ipc-dsm-registry/src/dsm_registry.rs`
- Port: `crates/backend-storage-ipc-dsm-registry/src/lib.rs`

## Function inventory

The C file defines exactly four functions plus one file-static helper. c2rust
confirms: `DSMRegistryShmemSize`, `DSMRegistryShmemInit`, `init_dsm_registry`
(static), `GetNamedDSMSegment`. No inline helpers, no `#if`-gated extras.
Data: `DSMRegistryCtxStruct`, `DSMRegistryEntry`, `dsh_params` (static const),
file statics `DSMRegistryCtx`, `dsm_registry_dsa`, `dsm_registry_table`.

| C symbol | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `DSMRegistryShmemSize` | dsm_registry.c:61 | lib.rs:148 | MATCH | `MAXALIGN(sizeof(DSMRegistryCtxStruct))`; MAXIMUM_ALIGNOF=8; sizeof=16 → 16. `max_align` impl matches C MAXALIGN. |
| `DSMRegistryShmemInit` | dsm_registry.c:66 | lib.rs:155 | MATCH | `ShmemInitStruct("DSM Registry Data", size, &found)` via shmem-seam; on `!found` resets `dsah=DSA_HANDLE_INVALID`, `dshh=DSHASH_HANDLE_INVALID`. Ctx pointer held in thread_local (backend-local, matches C file static). |
| `init_dsm_registry` (static) | dsm_registry.c:88 | lib.rs:181 | MATCH | Quick-exit on `dsm_registry_table != NULL`; LWLockAcquire(DSMRegistryLock=offset 50, LW_EXCLUSIVE); create branch (dsa_create+dshash_create+dsa_pin+dsa_pin_mapping, publish handles) vs attach branch (dsa_attach+dsa_pin_mapping+dshash_attach); LWLockRelease at end. Lock is RAII guard; success path calls `release()`, error path drops the guard (releases) — C leaks on its longjmp; observable lock state identical (released either way). |
| `GetNamedDSMSegment` | dsm_registry.c:124 | lib.rs:249 | MATCH | See detailed re-derivation below. |

### Data / statics

| C item | Port | Verdict | Notes |
|---|---|---|---|
| `struct DSMRegistryCtxStruct {dsa_handle dsah; dshash_table_handle dshh;}` | lib.rs:84 | MATCH | repr(C); size 16 (asserted in tests). |
| `struct DSMRegistryEntry {char name[64]; dsm_handle handle; size_t size;}` | lib.rs:96 | MATCH | repr(C); offsetof(handle)=64, size 80 (asserted). |
| `static const dshash_parameters dsh_params` | lib.rs:110 | MATCH | key_size=64 (=offsetof handle), entry_size=sizeof(DSMRegistryEntry), tranche=LWTRANCHE_DSM_REGISTRY_HASH, strcmp/strhash/strcpy conveyed via `DshashKeyKind::String`. |
| `DSMRegistryCtx`, `dsm_registry_dsa`, `dsm_registry_table` (file statics) | lib.rs:128-136 | MATCH | thread_locals (backend-local; correct per design — not shared statics). |

## `GetNamedDSMSegment` detailed re-derivation

- `Assert(found)` → guaranteed by `&mut bool`. MATCH.
- `!name || *name=='\0'` → empty-name ERROR. Port: `name.is_empty()`. `errmsg("DSM segment name cannot be empty")`, ERROR severity, default SQLSTATE XX000. MATCH.
- `strlen(name) >= offsetof(DSMRegistryEntry, handle)` (=64) → too-long ERROR. Port: `name.len() >= 64` (byte length, matches strlen). 63-byte name still fits with the NUL the substrate's strcpy writes. MATCH.
- `size == 0` → nonzero ERROR. MATCH.
- `MemoryContextSwitchTo(TopMemoryContext)` → threaded as `top_mcx` into `dsm_create`/`dsm_attach`; descriptors live in dsm-core's backend-local list (backend lifetime) so the C "make persistent" semantics hold. MATCH.
- `init_dsm_registry()`. MATCH.
- `entry = dshash_find_or_insert(table, name, found)` → returns `DshashEntryGuard` holding partition lock; `*found = guard.found`. MATCH.
- `if (!*found) { handle=DSM_HANDLE_INVALID; size=size; } else if (size != size) ERROR` → matches, including the size-mismatch ereport (XX000) which drops the guard (releases partition lock) rather than leaking it like the C longjmp. MATCH.
- `if (entry->handle == DSM_HANDLE_INVALID) { *found=false; seg=dsm_create(size,0); if(cb) cb(addr); dsm_pin_segment; dsm_pin_mapping; entry->handle=dsm_segment_handle; }` → port matches exactly; `dsm_create(size,0)` Option is `.expect`-ed (None only under DSM_CREATE_NULL_IF_MAXSEGMENTS, not set here) — faithful, the too-many path raises ERROR inside dsm_create. `*found` correctly reset to false even when entry pre-existed with invalid handle. MATCH.
- `else { seg=dsm_find_mapping(handle); if(!seg){ seg=dsm_attach(handle); if(!seg) elog(ERROR,"could not map..."); dsm_pin_mapping(seg);} }` → port matches; the elog uses `errmsg_internal` (correct for elog). MATCH.
- `ret=dsm_segment_address(seg); dshash_release_lock(table, entry); MemoryContextSwitchTo(old); return ret;` → `entry_guard.release()` then return. MATCH.

## Constants verified against headers

- DSMRegistryLock offset = 50 — `src/include/storage/lwlocklist.h:83` `PG_LWLOCK(50, DSMRegistry)`. Port `DSM_REGISTRY_LOCK=50`. MATCH.
- `LWTRANCHE_DSM_REGISTRY_DSA`/`_HASH` adjacency — `src/include/storage/lwlock.h:213-214`. types-storage chains them in the correct header order. MATCH.
- `DSA_HANDLE_INVALID = (dsa_handle) DSM_HANDLE_INVALID` (dsa.h:139); `DSM_HANDLE_INVALID=0`; `DSHASH_HANDLE_INVALID = (dshash_table_handle) InvalidDsaPointer` (dshash.h:27); `InvalidDsaPointer=0` (dsa.h:78). Port: `DSA_HANDLE_INVALID=DSM_HANDLE_INVALID`, `DSHASH_HANDLE_INVALID=INVALID_DSA_POINTER`. MATCH.
- `dsh_params.key_size = offsetof(DSMRegistryEntry, handle) = 64`. MATCH.
- `LW_EXCLUSIVE = 0`. MATCH.

## Seam audit

Outward seams (consumed; owners unported → calls panic loudly, acceptable per skill):
- `backend-utils-mmgr-dsa-seams`: `dsa_create` (macro for `dsa_create_ext(tranche, DSA_DEFAULT_INIT_SEGMENT_SIZE, DSA_MAX_SEGMENT_SIZE)` — the default sizes belong to dsa.c, correctly not duplicated here), `dsa_attach`, `dsa_pin`, `dsa_pin_mapping`, `dsa_get_handle`. All thin marshal+delegate; allocating ones return `PgResult`; pure getter `dsa_get_handle` returns a plain handle. No logic in seam paths.
- `backend-lib-dshash-seams`: `dshash_create`, `dshash_attach`, `dshash_get_hash_table_handle`, `dshash_find_or_insert` (returns lock-holding `DshashEntryGuard`), `dshash_release_lock`. Guard holds the partition lock across `?` and releases on drop/`release()` — satisfies the lock-guard rule. No logic in seam paths.
- `backend-storage-ipc-shmem-seams::shmem_init_struct`, `backend-storage-lmgr-lwlock-seams::lwlock_acquire_main` (returns `MainLWLockGuard`). Thin.
- DSM segment lifecycle (`dsm_create`/`dsm_attach`/`dsm_find_mapping`/`dsm_pin_*`/`dsm_segment_address`/`dsm_segment_handle`) is called DIRECTLY on the ported `backend-storage-ipc-dsm-core` crate, not seamed. Correct.

Inward seams: none. This crate has no cyclic caller, so there is no `-seams`
crate for it and no `init_seams()` / `seams-init` line. This matches the skill's
rule (a crate only needs `init_seams` if it owns declarations). No uninstalled
seam, no `set()` outside an owner (the dsa/dshash seam crates are pure
declarations; their owners install later).

## Design conformance

- Allocating entry point `GetNamedDSMSegment` takes `Mcx<'static>` (`top_mcx`) and returns `PgResult` — conforms.
- No invented opacity; `DsaArea`/`DshashTable` are opaque handles crossing seams as raw pointers (matching C's opaque types), never dereferenced by this crate.
- Per-backend globals are thread_locals, not shared statics.
- Locks held across `?` are RAII guards (MainLWLockGuard, DshashEntryGuard).
- No registry-shaped side tables, no ambient-global seams, no unledgered divergence markers.

## Build / tests

- `cargo build -p backend-storage-ipc-dsm-registry -p backend-utils-mmgr-dsa-seams -p backend-lib-dshash-seams`: clean.
- `cargo test -p backend-storage-ipc-dsm-registry -- --test-threads=1`: 5 passed (layout, validation, shmem-init reset, size-mismatch lock release, connect publishes handles + balances init lock).

## Verdict: PASS

Every function MATCH; constants verified against headers; seams are thin
marshal+delegate with correct RAII lock discipline; no MISSING/PARTIAL/DIVERGES;
zero seam findings; design rules satisfied.
