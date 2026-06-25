# Audit: backend-storage-ipc-shm-toc

Unit: `backend-storage-ipc-shm-toc` — `src/backend/storage/ipc/shm_toc.c` plus the
estimator inlines of `src/include/storage/shm_toc.h`.

Sources compared:
- C: `pgrust/postgres-18.3/src/backend/storage/ipc/shm_toc.c`, `src/include/storage/shm_toc.h`
- c2rust: `pgrust/c2rust-runs/backend-storage-ipc-shm-toc/src/shm_toc.rs`
- Port: `crates/backend-storage-ipc-shm-toc/src/lib.rs`

## Function inventory and verdicts

Inventory cross-checked against the c2rust rendering: shm_toc.c defines exactly 7
functions (no statics, no inline helpers besides the transpile-only barrier shims);
shm_toc.h adds 3 function-like macros. Nothing else exists in either rendering.

| C function (location) | Port location | Verdict | Notes |
|---|---|---|---|
| `shm_toc_create` (shm_toc.c:64) | `ShmToc::create` (lib.rs:187) | MATCH | Assert → debug_assert; magic, `SpinLockInit` → `s_init_lock` (store 0), `toc_total_bytes = BUFFERALIGN_DOWN(nbytes)`, allocated=0, nentry=0, in the same order. |
| `shm_toc_attach` (shm_toc.c:86) | `ShmToc::attach` (lib.rs:216) | MATCH | magic mismatch → `None` (C: NULL); both Asserts kept as debug_asserts. |
| `shm_toc_allocate` (shm_toc.c:110) | `ShmToc::allocate` (lib.rs:239) | MATCH | `nbytes = BUFFERALIGN(nbytes)`; lock; same `toc_bytes = offsetof + nentry*16 + allocated`; exhaustion/overflow check `sum > total \|\| sum < toc_bytes` with `wrapping_add` reproducing C unsigned wrap; lock released **before** `ereport(ERROR, ERRCODE_OUT_OF_MEMORY, "out of shared memory")` exactly as C; result pointer `base + (total - allocated_pre - nbytes)` uses pre-increment `allocated_bytes`, matching C locals. |
| `shm_toc_freespace` (shm_toc.c:155) | `ShmToc::freespace` (lib.rs:281) | MATCH | Reads the 3 fields under the spinlock, releases, then computes `total - (allocated + BUFFERALIGN(offsetof + nentry*16))`; Assert kept as debug_assert. SpinLockAcquire cannot fail (s_lock spins; no error path), so the bare `Size` return is faithful. |
| `shm_toc_insert` (shm_toc.c:201) | `ShmToc::insert` (lib.rs:312) | MATCH | `Assert(address > toc)` strict → debug_assert strict; offset relativized first; same guard incl. `nentry >= PG_UINT32_MAX` (`u32::MAX as Size`, verified vs c2rust 4294967295); lock released before the same ereport; entry written, then `fence(Release)` for `pg_write_barrier()`, then `toc_nentry++`, then unlock — same order. |
| `shm_toc_lookup` (shm_toc.c:251) | `ShmToc::lookup` (lib.rs:365) | MATCH | Lock-free: reads `toc_nentry` once, `fence(Acquire)` for `pg_read_barrier()`, linear scan `0..nentry`, found → `base + offset` (always non-null since offset > header size); miss with `!noError` → `elog(ERROR, "could not find key {} in shm TOC at {:p}")` via `errmsg_internal`, matching C's elog path (`%llu`/`UINT64_FORMAT` → `{}`, `%p` → `{:p}`); miss with noError → `Ok(None)` (C: NULL). |
| `shm_toc_estimate` (shm_toc.c:283) | `shm_toc_estimate` (lib.rs:435) | MATCH | `offsetof + mul_size(number_of_keys, 16)` then `add_size(space_for_chunks)`, `BUFFERALIGN` of the sum; add/mul via the shmem owner's seam crate (overflow → PgResult, same surface as C's ereport in shmem.c). |
| `shm_toc_initialize_estimator` (shm_toc.h macro) | lib.rs:416 | MATCH | Zeroes both fields. |
| `shm_toc_estimate_chunk` (shm_toc.h macro) | lib.rs:422 | MATCH | `space_for_chunks = add_size(space_for_chunks, BUFFERALIGN(sz))`. |
| `shm_toc_estimate_keys` (shm_toc.h macro) | lib.rs:428 | MATCH | `number_of_keys = add_size(number_of_keys, cnt)`. |

Transpile-only shims `c2rust_pg_{memory,read,write}_barrier` exist only under
`#ifdef C2RUST_TRANSPILE`; the port's `fence(Release)`/`fence(Acquire)` map the real
`pg_write_barrier`/`pg_read_barrier`, identical to the orderings c2rust emitted.

## Constants verified

- `ALIGNOF_BUFFER = 32`: matches the literal 32 in the c2rust build output
  (post-preprocessor truth for this build config); `BUFFERALIGN`/`BUFFERALIGN_DOWN`
  match `c.h:782/793` TYPEALIGN forms.
- `offsetof(shm_toc, toc_entry) = 40`, `sizeof(shm_toc_entry) = 16`: `InSegmentShmToc`
  is `repr(C)` {u64, Spinlock(repr(transparent) AtomicI32 = 4-byte slock_t), Size, Size,
  u32} → offsets 0/8/16/24/32, size 40; c2rust hardcodes 40 and 16. A unit test
  (`header_layout_matches_c`) asserts the layout.
- `PG_UINT32_MAX = 4294967295` (c2rust: `UINT32_MAX`).
- `ERRCODE_OUT_OF_MEMORY = "53200"`: c2rust's MAKE_SQLSTATE expansion decodes to
  53200; `types-error` builds it from `*b"53200"`. Message "out of shared memory"
  byte-identical.
- elog miss message uses `errmsg_internal` in both c2rust and the port's `elog` helper.

## Edge cases

- Overflow guards in allocate/insert use `wrapping_add`, so the `sum < toc_bytes`
  branch behaves identically to C unsigned wraparound even in debug builds.
- `allocated_bytes += nbytes` cannot overflow post-guard (`allocated <= toc_bytes`,
  `toc_bytes + nbytes <= total_bytes`); the returned offset subtraction cannot
  underflow for the same reason.
- `BUFFERALIGN` uses `wrapping_add(ALIGNOF_BUFFER - 1)`, exactly mirroring C's
  `TYPEALIGN` uintptr_t wraparound (`c.h:773`) and the c2rust rendering's
  `wrapping_add`; for `len > SIZE_MAX-31` both wrap identically even in debug
  builds. (A re-audit fix: the prior code used a plain `+` here, which matched C
  in release but would have panicked in a debug build where C silently wraps.)
- Found-key pointer is provably non-null (offset strictly past the 40-byte header),
  so `Ok(NonNull::new(ptr))` cannot conflate "found at null" with "missing".

## Seams and wiring

- Outward: `add_size`/`mul_size` via `backend-storage-ipc-shmem-seams` — owner
  (shmem.c) unported; per the AGENTS.md neighbor table this is the sanctioned route
  for a neighbor's function. Both seam calls are bare delegate calls, no logic.
- Inward: none. `init_seams()` is empty and is still called by
  `seams-init::init_all()` (seams-init/src/lib.rs:29). No `set()` outside any owner.
- No function body was replaced by a seam; all seven cores live in this crate.
- Design conformance: `shm_toc` is opaque in C ("known only within shm_toc.c"), so the
  crate-private `InSegmentShmToc` + public `ShmToc` handle is the real type, not
  invented opacity; `shm_toc_estimator` is the real two-field struct in
  `types-storage::storage` with values per shm_toc.h. No allocation (no `Mcx`
  needed — the TOC is a bump allocator over caller memory), no per-backend statics,
  no registries. The spinlock is held via an RAII guard, and no `?` occurs while it
  is held (error paths `drop(guard)` explicitly first, mirroring C's
  release-before-ereport).

## Spot-check

Re-derived `shm_toc_insert` and `shm_toc_allocate` line-by-line against both the C
and the c2rust volatile-store rendering (locals snapshot, guard predicate ordering,
write-barrier placement, unlock ordering) — confirmed MATCH.

`cargo test -p backend-storage-ipc-shm-toc`: 10 passed.

## Verdict

**PASS** — all 10 inventory rows MATCH; zero seam or design findings.
