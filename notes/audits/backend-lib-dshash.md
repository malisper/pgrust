# Audit: backend-lib-dshash

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Claude Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)
- Branch: `port/backend-lib-dshash`
- Unit: `backend-lib-dshash` (CATALOG row 301), C source `src/backend/lib/dshash.c`
- Sources compared: C (`postgres-18.3/src/backend/lib/dshash.c`), c2rust
  (`c2rust-runs/backend-lib-no-ilist/src/dshash.rs`), port
  (`crates/backend-lib-dshash/src/lib.rs`).

dshash.c is a self-contained file with no `#if` build-config branches, so the
c2rust rendering covers the full function set; the inventory below was
enumerated directly from the C and cross-checked against c2rust (all 32
definitions present in both).

## 1. Function inventory and verdicts

| # | C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `dshash_create` | dshash.c:205 | lib.rs:317 | MATCH | Allocates local state + DSA control object, inits 128 partitions, allocates the initial bucket array with `DSA_ALLOC_NO_OOM\|DSA_ALLOC_ZERO`; on invalid pointer frees the control and raises the OOM ereport with the same SQLSTATE + `Failed on DSA request of size N.` detail. |
| 2 | `dshash_attach` | dshash.c:269 | lib.rs:377 | MATCH | Resolves control address, leaves `buckets=NULL`/`size_log2=0` for a later `ensure_valid_bucket_pointers`; magic checked via `debug_assert`. |
| 3 | `dshash_detach` | dshash.c:306 | lib.rs:408 | MATCH | Frees only the backend-local object (`Box::from_raw` + drop = `pfree`). |
| 4 | `dshash_destroy` | dshash.c:322 | lib.rs:417 | MATCH | `ensure_valid_bucket_pointers`, frees every item in every bucket, zeroes magic, frees the bucket array + control, drops local object. Loop bounds (`NUM_BUCKETS`) and item-walk match. |
| 5 | `dshash_get_hash_table_handle` | dshash.c:366 | lib.rs:453 | MATCH | Returns `control->handle`. |
| 6 | `dshash_find` | dshash.c:389 | lib.rs:466 | MATCH | Hash → partition, lock in shared/exclusive per flag, ensure-valid, `find_in_bucket` on the active bucket head; on miss releases and returns `None`, on hit returns the entry with the lock held. |
| 7 | `dshash_find_or_insert` | dshash.c:432 | lib.rs:502 | MATCH | `restart` loop = Rust `loop`/`continue`; load-factor check `count > MAX_COUNT_PER_PARTITION` → release+resize+restart; insert sets `item->hash` and bumps the partition count. Index/threshold math verified against c2rust (lib.rs:540 vs c2rust:512-523). |
| 8 | `dshash_delete_key` | dshash.c:502 | lib.rs:552 | MATCH | Lock exclusive, `delete_key_from_bucket` on the active bucket slot, decrement count on success, release. `BucketSlot::Array(index)` models the `&BUCKET_FOR_HASH` lvalue. |
| 9 | `dshash_delete_entry` | dshash.c:540 | lib.rs:579 | MATCH | `ITEM_FROM_ENTRY`, partition from `item->hash`, `delete_item`, then release the partition lock. |
| 10 | `dshash_release_lock` | dshash.c:557 | lib.rs:595 | MATCH | `ITEM_FROM_ENTRY`, release the partition lock for `item->hash`. |
| 11 | `dshash_memcmp` | dshash.c:571 | lib.rs:614 | MATCH | `memcmp` over `size` bytes, C-sign result. |
| 12 | `dshash_memhash` | dshash.c:580 | lib.rs:619 | SEAMED | Forwards to `tag_hash` via `common-hashfn-seams`. |
| 13 | `dshash_memcpy` | dshash.c:589 | lib.rs:624 | MATCH | `copy_from_slice` of `size` bytes. |
| 14 | `dshash_strcmp` | dshash.c:599 | lib.rs:629 | MATCH | `strlen<size` asserts as `debug_assert`, NUL-terminated compare. |
| 15 | `dshash_strhash` | dshash.c:610 | lib.rs:636 | SEAMED | Forwards to `string_hash` via `common-hashfn-seams`; `strlen<size` assert kept. |
| 16 | `dshash_strcpy` | dshash.c:621 | lib.rs:642 | MATCH | Copies `strlen(src)+1` bytes (incl. NUL), matching `strcpy`; `strlen<size` assert kept. |
| 17 | `dshash_seq_init` | dshash.c:637 | lib.rs:704 | MATCH | Field-for-field init (`curpartition=-1`, `pnextitem=InvalidDsaPointer`). |
| 18 | `dshash_seq_next` | dshash.c:656 | lib.rs:718 | MATCH | First-call lock of partition 0 + `nbuckets` from `control->size_log2`; bucket walk advances partitions by locking-next-then-releasing-current (resize lock order); `pnextitem` saved before return. Verified vs c2rust:708-779. |
| 19 | `dshash_seq_term` | dshash.c:746 | lib.rs:768 | MATCH | Releases the held partition if `curpartition >= 0`. |
| 20 | `dshash_delete_current` | dshash.c:756 | lib.rs:778 | MATCH | `delete_item` on `curitem` (lock retained, as in C — release is `seq_next`/`seq_term`). |
| 21 | `dshash_dump` | dshash.c:777 | lib.rs:797 | MATCH | Acquires all 128 partitions shared, prints size/partition/bucket key counts, releases all. C `fprintf(stderr,…)` rendered to a returned `String` with identical format strings/values (a non-behavioral I/O substitution for a debug-only dump). |
| 22 | `delete_item` (static) | dshash.c:831 | lib.rs:846 | MATCH | `delete_item_from_bucket` on the active bucket slot, decrement on success, `Assert(false)` → `debug_assert!(false, …)` on the impossible miss. |
| 23 | `resize` (static) | dshash.c:857 | lib.rs:863 | MATCH | Acquire-all loop with the `i==0 && size already grown` early return; `DSA_ALLOC_HUGE\|DSA_ALLOC_ZERO` new array; reinsert every item via `BUCKET_INDEX_FOR_HASH_AND_SIZE(item->hash,new_size_log2)`; swap control buckets/size_log2, set local `buckets`, free old, release all. See note A on the extra local `size_log2` write. |
| 24 | `ensure_valid_bucket_pointers` (static inline) | dshash.c:936 | lib.rs:920 | MATCH | Refresh local `buckets`/`size_log2` only when `size_log2 != control->size_log2`. |
| 25 | `find_in_bucket` (static inline) | dshash.c:950 | lib.rs:932 | MATCH | Walk the bucket; `equal_keys` against `ENTRY_FROM_ITEM`; return item or `None`. |
| 26 | `insert_item_into_bucket` (static) | dshash.c:969 | lib.rs:951 | MATCH | Prepend: `item->next = *bucket; *bucket = item_pointer`. (C `Assert(item == dsa_get_address(...))` dropped — debug-only sanity, no logic.) |
| 27 | `insert_into_bucket` (static) | dshash.c:985 | lib.rs:965 | MATCH | `dsa_allocate(entry_size + MAXALIGN(sizeof item))`, `copy_key` into the entry, prepend into the bucket-array slot. |
| 28 | `delete_key_from_bucket` (static) | dshash.c:1005 | lib.rs:1014 | MATCH | Walk `*bucket_head`, on key match splice out and `dsa_free`; `bucket_head` advances to `&item->next` via `BucketSlot::ItemNext`. |
| 29 | `delete_item_from_bucket` (static) | dshash.c:1034 | lib.rs:1038 | MATCH | Same walk but matches by item pointer identity. |
| 30 | `hash_key` (static inline) | dshash.c:1062 | lib.rs:1062 | MATCH | Dispatches on `DshashKeyKind` to `dshash_strhash`/`dshash_memhash` over `key_size` (see note B). |
| 31 | `equal_keys` (static inline) | dshash.c:1073 | lib.rs:1072 | MATCH | Reads `key_size` bytes of the entry, dispatches to `dshash_strcmp`/`dshash_memcmp == 0` (note B). |
| 32 | `copy_key` (static inline) | dshash.c:1084 | lib.rs:1084 | MATCH | Dispatches to `dshash_strcpy`/`dshash_memcpy` over `key_size` (note B). |

Index-math macros (`PARTITION_FOR_HASH`, `BUCKET_INDEX_FOR_HASH_AND_SIZE`,
`BUCKET_INDEX_FOR_PARTITION`, `PARTITION_FOR_BUCKET_INDEX`, `NUM_SPLITS`,
`NUM_BUCKETS`, `BUCKETS_PER_PARTITION`, `MAX_COUNT_PER_PARTITION`) transcribed as
`const fn`s (lib.rs:150-198) and re-derived against the C and c2rust shift
expressions — all match, including `HASH_BITS = sizeof(dshash_hash)*CHAR_BIT = 32`.

### Constants verified against C / c2rust (not from memory)

- `DSHASH_NUM_PARTITIONS_LOG2 = 7`, `DSHASH_NUM_PARTITIONS = 128` (c2rust:236).
- `DSHASH_MAGIC = 0x75ff6a20` (c2rust:239).
- `dshash_hash = u32`; `HASH_BITS = 32`.
- `LW_EXCLUSIVE = 0`, `LW_SHARED = 1` (c2rust:222-223; types-storage storage.rs:38-39).
- `DSA_ALLOC_HUGE = 0x1`, `DSA_ALLOC_NO_OOM = 0x2`, `DSA_ALLOC_ZERO = 0x4`,
  `InvalidDsaPointer = 0` (c2rust:232-235; types-execparallel lib.rs:44-57).
- `MAXALIGN(sizeof(dshash_table_item)) = 16` — confirmed by the
  `item_header_layout_matches_c` test and the `#[repr(C)] {u64,u32}` layout.
- `dshash_seq_status` field set/order matches c2rust:211-218.

## 2. Seam audit (step 3)

**Owned seam crates** (by C-source coverage: dshash.c):
`crates/backend-lib-dshash-seams` — the inward contract this unit installs.

- Declares 7 seams: `dshash_create`, `dshash_attach`,
  `dshash_get_hash_table_handle`, `dshash_find_or_insert`, `dshash_find`,
  `dshash_delete_key`, `dshash_release_lock`.
- `init_seams()` (lib.rs:1112) installs **all 7** with nothing but `set()`
  calls (each a thin marshal: borrow the by-value `params`, wrap the entry in a
  `DshashEntryGuard`, or panic-on-release at the infallible boundary). No
  branching/computation that belongs in the crate.
- `seams-init::init_all()` calls `backend_lib_dshash::init_seams()`
  (seams-init/src/lib.rs:33). No uninstalled declaration; no `set()` outside the
  owner.
- The inward consumer that creates the cycle is the already-merged
  `backend-replication-logical-launcher` (CATALOG row 380), which consumes this
  crate's `dshash_find`/`dshash_delete_key`/`dshash_find_or_insert` — a genuine
  dependency, justifying the inward seam crate.

**Outward seams** — all thin marshal+delegate, each a real cross-unit dep
(dshash sits below dsa.c / lwlock.c / hashfn.c):

- `backend-utils-mmgr-dsa-seams` — `dsa_allocate_extended`, `dsa_free_ptr`,
  `dsa_get_address_ptr` (the `dsa_area*`-keyed substrate calls; `dsa_get_address`
  returns the resolved backend-local address as `u64` under the blessed
  shared-memory exception). Reuses the existing dsa-seams crate.
- `backend-storage-lmgr-lwlock-seams` — `lwlock_initialize`, `lwlock_acquire`,
  `lwlock_release`.
- `common-hashfn-seams` — `tag_hash`, `string_hash`.
- `backend-utils-init-small-seams::my_proc_number` — the ambient `MyProcNumber`
  the lwlock owner needs, taken as an explicit value at the acquire site.

No outward seam carries logic; each is argument-convert → one call →
result-convert.

## 3b. Design conformance

- **Seam failure surface:** every allocating/erroring seam returns `PgResult`;
  `dshash_create`/`attach`/`find`/`find_or_insert`/`delete_key` surface the C
  `ereport(ERROR)` paths. The only infallible seam, `dshash_release_lock`,
  matches C (cannot fail on the no-error path) and panics on a release error at
  that boundary. Conforms.
- **Opacity:** `DshashTable`/`DsaArea`/`dshash_table_handle`/`DshashParameters`/
  `DshashKeyKind` are real `types-storage`/`types-execparallel` types, not
  invented stand-ins. The in-DSA `dshash_table_control`/`dshash_table_item`/
  `dshash_partition` are crate-local `#[repr(C)]` mirrors of dshash.c's
  file-private structs, addressed through the resolved DSA address — the same
  blessed `*mut`/`*const` substrate exception dsa.c and the in-segment `LWLock`
  take. No invented opacity.
- **Held locks across `?` (note C):** dshash's contract is to hand a partition
  lock back to the caller (C `dshash_find` returns locked; release is a later
  `dshash_release_lock`). At the **seam boundary** this is discharged with a
  RAII `DshashEntryGuard` that releases on drop. Internally, `lock_partition`
  acquires the lwlock guard and `mem::forget`s it, releasing later by recomputing
  the lock from the control — a faithful model of the C bare acquire/release
  pair. On the intermediate `?` error paths (e.g. `ensure_valid_bucket_pointers`,
  `dsa_get_address` raising) the lock is *not* hand-released; this mirrors C
  exactly, where the process-wide `LWLockReleaseAll` abort backstop (not a
  per-call release) cleans up — documented at lib.rs:243-256. This is the same
  established, merged pattern as `backend-access-transam-twophase`'s
  `TwoPhaseStateLock` mid-function acquire/release. Conforms (not a fresh
  finding).
- **OOM error (note D):** `dshash_create`'s invalid-bucket-pointer path builds
  `PgError::error("out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY)
  .with_detail("Failed on DSA request of size N.")`. The AGENTS `mcx.oom(size)`
  rule governs *palloc* failures; this is **not** a palloc path — it is the
  explicit `ereport(ERROR, errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of
  memory"), errdetail("Failed on DSA request of size %zu."))` the C author wrote
  for the `DSA_ALLOC_NO_OOM` → `InvalidDsaPointer` case (dshash.c:251-256), and
  there is no `Mcx` in scope (the allocation is in the DSA area). The hand-built
  error reproduces the C SQLSTATE + message + detail byte-for-byte. Conforms.

## Notes

- **Note A (resize local size_log2):** the port adds `t.size_log2 =
  new_size_log2;` (lib.rs:908), which C `resize` does not do (C leaves the local
  `hash_table->size_log2` stale, relying on the next
  `ensure_valid_bucket_pointers` to refresh it). Behavior is provably identical:
  `resize`'s sole caller (`dshash_find_or_insert`) does `goto restart` and runs
  `ensure_valid_bucket_pointers` under a fresh lock before any read of
  `size_log2`; with `t.buckets` already swapped, the refresh is idempotent. The
  end state after restart is the same in both. MATCH.
- **Note B (key-kind enum):** C `dshash_parameters` holds three function
  pointers (`hash_function`/`compare_function`/`copy_function`); the port models
  them as a `DshashKeyKind` enum (`String`/`Binary`) in `types-storage`,
  dispatching to the built-in `dshash_strhash`/`dshash_strcmp`/`dshash_strcpy` or
  `dshash_memhash`/`dshash_memcmp`/`dshash_memcpy` sets. This is the established
  repo-wide `types-storage` modeling (introduced with the merged dsm-registry
  unit, CATALOG row 418), and the only consumers (dsm-registry, launcher) use
  exactly these built-in helper sets with `arg = NULL`. Not introduced by this
  port; faithful for every reachable input.
- Debug-only `Assert`s requiring lwlock introspection
  (`ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME`, `LWLockHeldByMeInMode`,
  `LWLockHeldByMe`) and the `insert_item_into_bucket` address sanity `Assert` are
  dropped; they are not logic and need unported lwlock-introspection seams.

## Build & test

- `cargo build -p backend-lib-dshash` — clean.
- `cargo test -p backend-lib-dshash` — 8/8 pass (create/insert/find/mutate/
  delete, delete-entry on a locked entry, resize-keeps-all-keys, seq-scan with
  delete-current, handle round-trip, partition/bucket math, item-header layout,
  convenience callbacks).

## Verdict

Every function MATCH or properly SEAMED; seam wiring complete and installed;
zero seam findings; design conformance holds. **PASS.**
