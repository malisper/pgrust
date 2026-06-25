# Audit: backend-utils-hash-dynahash

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Claude Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- Unit: `backend-utils-hash-dynahash`  (c_sources: `*/dynahash.c`)
- Branch: `port/backend-utils-hash-dynahash`
- Port: `crates/backend-utils-hash-dynahash/src/lib.rs`
- Owned seam crate: `crates/backend-utils-hash-dynahash-seams`

Independent re-derivation from `postgres-18.3/src/backend/utils/hash/dynahash.c`,
the c2rust rendering (`c2rust-runs/backend-utils-hash-dynahash/src/dynahash.rs`),
and the port. Logic verified branch-by-branch; constants verified against
`hsearch.h` / `pg_bitutils.h`, not from memory.

## 1. Function inventory

The c2rust public-symbol set (exported functions) matches the C exactly:
`hash_create, hash_search, hash_search_with_hash_value, hash_update_hash_key,
get_hash_value, hash_get_num_entries, hash_seq_init, hash_seq_init_with_hash_value,
hash_seq_search, hash_seq_term, hash_freeze, hash_estimate_size,
hash_select_dirsize, hash_get_shared_size, hash_destroy, hash_stats, my_log2,
AtEOXact_HashTables, AtEOSubXact_HashTables`. The file-local statics
(`DynaHashAlloc, string_compare, hdefault, choose_nelem_alloc, init_htab,
calc_bucket, get_hash_entry, expand_table, dir_realloc, seg_alloc, element_alloc,
hash_initial_lookup, hash_corrupted, next_pow2_long, next_pow2_int,
register_seq_scan, deregister_seq_scan, has_seq_scans`) are all present in the
port. Built-in key funcs (`string_hash`/`tag_hash`/`uint32_hash`/`oid_hash`)
live in `common/hashfn.c`, not in this unit; the port reaches them through
`common_hashfn_seams` (real dep).

## 2. Per-function table

| C func (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `DynaHashAlloc` (290) | `dyna_hash_alloc` / `hash_alloc` lib.rs:177,201 | MATCH | `MCXT_ALLOC_NO_OOM` NULL-on-OOM via `try_reserve_exact`; zeroed slab. The C ambient `CurrentDynaHashCxt` static is replaced by the per-table arena keyed on `HTAB*` (backend-local thread_local registry == the C private MemoryContext). |
| `string_compare` (306) | `string_compare` lib.rs:229 | MATCH | `strncmp(k1,k2,keysize-1)`: byte loop over `keysize-1`, unsigned-char diff, NUL short-circuit. |
| `hash_create` (351) | `hash_create` lib.rs:286 | MATCH | All flag branches reproduced (HASH_FUNCTION/BLOBS/STRINGS hash select; match/keycopy defaults; alloc; SHARED_MEM hctl+dir setup; ATTACH early copy; PARTITION/SEGMENT/DIRSIZE; init_htab; element preallocation incl. `nelem_alloc_first` excess logic; FIXED_SIZE). Asserts → `debug_assert`. The C `hashp->hash == string_hash` pointer compare is replaced by an `is_string_hash` flag set only in the HASH_STRINGS branch — exactly equivalent except in the documented-deprecated case of passing `string_hash` via HASH_FUNCTION (no in-tree caller does this; see §3b). |
| `hdefault` (629) | `hdefault` lib.rs:520 | MATCH | MemSet 0 + dsize/nsegs/num_partitions/max_dsize(NO_MAX_DSIZE)/ssize/sshift defaults. |
| `choose_nelem_alloc` (656) | `choose_nelem_alloc` lib.rs:533 | MATCH | `elementSize = MAXALIGN(sizeof HASHELEMENT)+MAXALIGN(entrysize)`; `allocSize=128`, do/while `<<1` until `nelem>=32`. |
| `init_htab` (689) | `init_htab` lib.rs:549 | MATCH | Mutex init when partitioned; `next_pow2_int(nelem)`; nbuckets>=num_partitions; mask computation; nsegs; dir-too-small → false; dir alloc; segment alloc loop; `nelem_alloc`. |
| `hash_estimate_size` (783) | `hash_estimate_size` lib.rs:613 | MATCH | nBuckets/nSegments/nDirEntries doubling; `MAXALIGN(sizeof HASHHDR)` + dir + `MAXALIGN(DEF_SEGSIZE*sizeof HASHBUCKET)` segments + element groups. add_size/mul_size are plain `usize` arithmetic (sizes bounded). |
| `hash_select_dirsize` (830) | `hash_select_dirsize` lib.rs:639 | MATCH | identical nDirEntries doubling. |
| `hash_get_shared_size` (854) | `hash_get_shared_size` lib.rs:651 | MATCH | `sizeof(HASHHDR) + info->dsize*sizeof(HASHSEGMENT)` (no MAXALIGN — matches C). |
| `hash_destroy` (865) | `hash_destroy` lib.rs:665 | MATCH | NULL-guard; drops the per-table arena (== `MemoryContextDelete(hcxt)`). Asserts on alloc==DynaHashAlloc / hcxt!=NULL are debug-only and not load-bearing. |
| `hash_stats` (884) | (none needed) | MATCH | Body is entirely under `#ifdef HASH_STATISTICS` (not in build config); a no-op. Correctly omitted. |
| `get_hash_value` (911) | `get_hash_value`/`do_hash` lib.rs:682 | MATCH | `hashp->hash(key, keysize)`. |
| `calc_bucket` (918) | `calc_bucket` lib.rs:712 | MATCH | `hash & high_mask`, then `& low_mask` if `> max_bucket`. |
| `hash_search` (955) | `hash_search` lib.rs:723 | MATCH | computes hash then delegates to `_with_hash_value`. |
| `hash_search_with_hash_value` (968) | lib.rs:733 | MATCH | split-on-insert predicate (`freeList[0].nentries > max_bucket && !partitioned && !frozen && !has_seq_scans`); initial lookup; collision chain; FIND/REMOVE(lock+nentries--+chain unlink+freelist push)/ENTER(frozen ERROR, get_hash_entry, ENTER_NULL→null, OOM ERROR shared/local msg, link+keycopy). The C trailing `elog("unrecognized hash action")` is unreachable (closed enum). |
| `hash_update_hash_key` (1145) | lib.rs:840 | MATCH | frozen ERROR; locate existing via saved hashvalue (`not in hashtable` ERROR); locate new chain; collision → `Ok(false)`; same-bucket skip of relink (preserves the C "don't corrupt last-element" guard); hashvalue+keycopy. |
| `get_hash_entry` (1258) | `get_hash_entry` lib.rs:915 | MATCH | freelist pop loop; element_alloc-or-borrow; partitioned borrow walk `(idx+1)%NUM_FREELISTS` with proper lock dance and nentries accounting; non-partitioned OOM → null. |
| `hash_get_num_entries` (1343) | lib.rs:983 | MATCH | `freeList[0].nentries` + (partitioned) sum 1..NUM_FREELISTS; no locking (matches C). |
| `hash_seq_init` (1387) | lib.rs:1001 | MATCH | zero state; register unless frozen. |
| `hash_seq_init_with_hash_value` (1407) | lib.rs:1014 | MATCH | sets hasHashvalue/hashvalue; curBucket+curEntry from initial lookup. |
| `hash_seq_search` (1422) | lib.rs:1031 | MATCH | hasHashvalue single-bucket scan with hashvalue filter+term; continue-curBucket fast path; segment/bucket advance loop with `++segment_ndx>=ssize` segment rollover; term on exhaustion. Returns `Ok(null)` to terminate, matching C return-NULL == scan done. |
| `hash_seq_term` (1516) | lib.rs:1111 (+`hash_seq_term_inner`) | MATCH | deregister unless frozen. |
| `hash_freeze` (1536) | lib.rs:1123 | MATCH | shared ERROR; active-scan ERROR; set frozen. |
| `expand_table` (1553) | lib.rs:1150 | MATCH | new_bucket/segnum/segndx; dir_realloc-on-full + seg_alloc + nsegs++; max_bucket++; old_bucket via low_mask; mask readjust on power-of-2 crossing; record relocation split keeping old/new chains; terminate both. `Assert(!partitioned)` → debug_assert. |
| `dir_realloc` (1650) | lib.rs:1215 | MATCH | NO_MAX_DSIZE guard → false; double dsize; alloc; memcpy old + zero tail; set dir/dsize. C `pfree(old_p)` intentionally retained in the bounded arena (documented); behavior identical (old dir never reused). |
| `seg_alloc` (1689) | lib.rs:1241 | MATCH | alloc `sizeof(HASHBUCKET)*ssize`, NULL passthrough, MemSet 0. |
| `element_alloc` (1708) | lib.rs:1253 | MATCH | isfixed→false; elementSize; alloc nelem*size; reverse-link chain; lock; splice onto freelist head; unlock. |
| `hash_initial_lookup` (1758) | lib.rs:1296 | MATCH | calc_bucket; segment_num/ndx; `dir[seg]`; NULL→`hash_corrupted`; returns `(&segp[ndx], bucket)`. |
| `hash_corrupted` (1782) | lib.rs:1311 | MATCH | shared→PANIC else FATAL; noreturn enforced via trailing `panic!` (PgResult elog returns; the added panic preserves the C `pg_noreturn` contract). |
| `my_log2` (1796) | lib.rs:1330 | MATCH | clamp `>LONG_MAX/2`; 64-bit path `pg_ceil_log2_64`. `pg_ceil_log2_64` helper verified: `num<=1→0`, else `64 - (num-1).leading_zeros()` == `pg_leftmost_one_pos64(num-1)+1`. |
| `next_pow2_long` (1814) | lib.rs:1349 | MATCH | `1<<my_log2(num)`. |
| `next_pow2_int` (1822) | lib.rs:1354 | MATCH | clamp `>INT_MAX/2`; `1<<my_log2`. |
| `register_seq_scan` (1867) | lib.rs:1372 | MATCH | MAX_SEQ_SCANS(100) ERROR; push (hashp, GetCurrentTransactionNestLevel via xact seam). |
| `deregister_seq_scan` (1879) | lib.rs:1391 | MATCH | backward search, swap-remove (swap with last + pop), not-found ERROR. |
| `has_seq_scans` (1900) | lib.rs:1411 | MATCH | linear membership. |
| `AtEOXact_HashTables` (1914) | lib.rs:1417 | MATCH | commit→WARNING per leaked scan; clear all. |
| `AtEOSubXact_HashTables` (1940) | lib.rs:1433 | MATCH | backward walk; `level>=nestDepth` → (commit WARNING) swap-remove. |

Helper macros (`MAXALIGN`, `IS_PARTITIONED`, `FREELIST_IDX`, `ELEMENTKEY`,
`ELEMENT_FROM_KEY`, `MOD`) verified MATCH; constants `DEF_SEGSIZE=256`,
`DEF_SEGSIZE_SHIFT=8`, `DEF_DIRSIZE=256`, `NUM_FREELISTS=32`,
`MAX_SEQ_SCANS=100`, `NO_MAX_DSIZE=-1`, and all `HASH_*` flag bits
(`0x0001..0x2000`) verified against `hsearch.h`. `HASHACTION` discriminants
(FIND=0/ENTER=1/REMOVE=2/ENTER_NULL=3) verified.

## 3. Seam audit

**Ownership.** The only C file in `c_sources` is `dynahash.c`, so the sole owned
seam crate is `backend-utils-hash-dynahash-seams`. It declares 8 seams
(`hash_create, hash_search, hash_select_dirsize, hash_get_shared_size,
hash_estimate_size, hash_seq_init, hash_seq_search, at_eoxact_hash_tables`). All
8 are installed by `crate::init_seams()` in `src/seam.rs`, which contains nothing
but `set()` calls. `seams-init/src/lib.rs:77` calls
`backend_utils_hash_dynahash::init_seams()`. No uninstalled seam; no `set()`
outside the owner. PASS.

**Outward seam calls** (all thin marshal+delegate, all real deps):
- `common_hashfn_seams::{string_hash, tag_hash, hash_bytes_uint32}` — built-in
  key hashing lives in `common/hashfn.c` (separate unit); thin one-call delegate.
- `backend_storage_lmgr_s_lock::{s_init_lock, s_lock, s_unlock}` — real spinlock
  primitives driving the in-segment `slock_t` freelist mutex; thin wrappers
  (`SpinLockInit/Acquire/Release`).
- `backend_access_transam_xact_seams::get_current_transaction_nest_level` —
  `GetCurrentTransactionNestLevel()` for `register_seq_scan`; thin delegate.

No branching/node-construction/computation occurs inside any outward seam path.
No function body was replaced by a "look elsewhere" seam — every dynahash
algorithm lives in this crate. PASS.

## 3b. Design conformance

- **Opacity (types.md 6-7):** No invented handles. `HTAB`/`HASHHDR`/`HASHELEMENT`/
  `HASHBUCKET`/`HASHSEGMENT`/`HASH_SEQ_STATUS`/`HASHCTL` are the real C structs in
  `types_hash::hsearch`, raw-pointer faithful (shared tables live in genuine
  shared memory and pointers must round-trip across backends). PASS.
- **Allocating funcs return on OOM:** `hash_create`/`hash_search` return
  `PgResult` and surface the C `ereport(ERROR)` OOM paths; the local allocator is
  NULL-on-OOM (`MCXT_ALLOC_NO_OOM`), not a panic. The seam signatures mirror the
  C failure surface (PgResult where the C can `ereport(ERROR+)`; infallible where
  it can't). PASS.
- **Per-backend globals, not shared statics:** the C file-scope `seq_scan_tables`/
  `seq_scan_level`/`num_seq_scans` and `CurrentDynaHashCxt` are genuine
  per-backend (process-local) state in C; modeled as `thread_local` registries
  (`SEQ_SCAN_TABLES`, `TABLES`), not cross-backend shared statics. The
  freelist mutexes that ARE shared remain real in-segment `slock_t`. PASS.
- **No locks held across `?`:** `SpinLockAcquire/Release` bracket only
  straight-line pointer updates (no `?` between acquire and release). PASS.
- **No ambient-global seam / registry side table:** the `TABLES` arena registry
  is the faithful model of the C private `MemoryContext` (the table's own
  allocation arena), keyed by the handle the API already hands back — it is the
  table's memory, not a registry-shaped substitute for ported logic. PASS.
- **Divergence ledger:** Two intentional, behavior-preserving deviations are
  documented in-port: (a) `dir_realloc` retains the old dir slab in the bounded
  arena instead of `pfree` (never reused; freed with the table); (b) the
  `hashp->hash == string_hash` pointer compare is modeled by an `is_string_hash`
  flag — observably identical for every supported caller (HASH_STRINGS), differs
  only in the deprecated/unsupported "pass string_hash via HASH_FUNCTION" case
  which no in-tree caller exercises. Neither affects logic parity for any real
  input. (c) `hash_corrupted` adds a `panic!` after `elog(FATAL/PANIC)` to honor
  the C `pg_noreturn`, since the repo's `elog` returns. PASS.

## 4. Verdict

Every C function is **MATCH**. No `MISSING`/`PARTIAL`/`DIVERGES`. Seam audit
clean (8/8 declarations installed by the owner; `seams-init` wires it; outward
calls thin + justified). Design conformance clean. Crate builds; 12 unit tests
pass.

**PASS.**
