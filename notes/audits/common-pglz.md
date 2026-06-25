# Audit: common-pglz

- **Verdict:** PASS
- **Date:** 2026-06-13
- **Model:** claude-opus-4-8[1m]
- **Branch:** port/common-pglz
- **Unit C sources:** `src/common/pg_lzcompress.c` (+ header `src/include/common/pg_lzcompress.h`)
- **c2rust reference:** `c2rust-runs/common-batch3/src/pg_lzcompress.rs`
- **Port:** `crates/common-pglz/src/lib.rs`
- **Owned seam crate:** `crates/common-pglz-seams` (maps to `pg_lzcompress.c`)

This audit is independent of the port: every function/macro re-derived from the
C and cross-checked against the c2rust rendering and the headers.

## 1. Function inventory and verdicts

The unit is a self-contained codec with no callees outside libc (`memcmp`,
`memcpy`, `memset`). Inventory taken from the C source; macros are listed
because they carry algorithm logic and were inlined by both c2rust and the port.

| # | C symbol (kind) | C loc | Port loc | Verdict | Notes |
|---|-----------------|-------|----------|---------|-------|
| 1 | `PGLZ_MAX_HISTORY_LISTS`/`PGLZ_HISTORY_SIZE`/`PGLZ_MAX_MATCH` (#define) | c:195-197 | lib.rs:38-42 | MATCH | 8192 / 4096 / 273, verified against C. |
| 2 | `INVALID_ENTRY` / `INVALID_ENTRY_PTR` (#define) | c:262-263 | lib.rs:44 (`INVALID_ENTRY=0`) | MATCH | Sentinel index 0; "ptr" form folded into index form (`entry_index == 0`). |
| 3 | `strategy_default_data` / `PGLZ_strategy_default` (static const) | c:223-236 | lib.rs:96-104 | MATCH | `{32, INT_MAX, 25, 1024, 128, 10}`. Exposed as `const` + accessor fn (no shared mutable static — design-clean). |
| 4 | `strategy_always_data` / `PGLZ_strategy_always` (static const) | c:239-248 | lib.rs:99-108 | MATCH | `{0, INT_MAX, 0, INT_MAX, 128, 6}`. |
| 5 | `pglz_hist_idx` (macro) | c:277-281 | lib.rs `hist_idx` 567-578 | MATCH | `<4` branch returns `(int)_s[0]`; else `s0<<6 ^ s1<<4 ^ s2<<2 ^ s3`, `& mask`. Port reproduces C's *signed*-`char` sign-extension (`u8 as i8 as i32`); c2rust reads `*const c_char` (signed) confirming. Three dedicated tests guard this. |
| 6 | `pglz_hist_add` (macro) | c:296-327 | lib.rs `History::add` 168-201 | MATCH | Recycle unlink (prev==NULL → patch hash head, else patch prev->next; patch next->prev), insert at list head, scribble `entries[head].prev` unconditionally (C's deliberate "scribble on unused entry 0" optimization preserved by indexing entry 0), `next` ring wrap at `>= HISTORY_SIZE+1` setting `recycle=true`. |
| 7 | `pglz_out_ctrl` (macro) | c:336-345 | lib.rs `out_ctrl` 519-536 | MATCH | `(ctrl & 0xff)==0` flush-and-realloc; writes pending ctrlb to its slot, opens new ctrl slot, `ctrlb=0`, `ctrl=1`. `Option<usize>` slot index replaces C's `ctrlp` back-pointer. |
| 8 | `pglz_out_literal` (macro) | c:355-360 | lib.rs:379-381 (inline in compress) | MATCH | out_ctrl, push byte, `ctrl <<= 1` (wrapping). |
| 9 | `pglz_out_tag` (macro) | c:371-387 | lib.rs:350-369 (inline `push2`/`push3`) | MATCH | out_ctrl; `ctrlb |= ctrl; ctrl <<= 1`; len>17 → 3-byte tag `[((off&0xf00)>>4)|0x0f, off&0xff, len-18]`; else 2-byte `[((off&0xf00)>>4)|(len-3), off&0xff]`. Byte formulas verified bit-for-bit. |
| 10 | `pglz_find_match` (static inline) | c:398-498 | lib.rs `History::find_match` 205-268 | MATCH | thisoff break `>= 0x0fff`; len>=16 memcmp fast path then char loop bounded by `PGLZ_MAX_MATCH`; best-update on `thislen>len`; good_match decay `good_match -= good_match*good_drop/100` applied only when not at list end; return iff `len>2`. Slice-bounded reads replace pointer arithmetic with identical bounds. |
| 11 | `pglz_compress` | c:508-676 | lib.rs `pglz_compress` 274-397 | MATCH | Null→default strategy; reject `match_size_good<=0 || slen<min || slen>max`; clamp good_match[17,273], good_drop[0,100], need_rate[0,99]; result_max with INT_MAX/100 overflow-avoidance branch; hashsz ladder 512/1024/2048/4096/8192; per-iter `output.len()>=result_max` fail; `!found_match && len>=first_success_by` fail (port clamps `first_success_by.max(0)` to reproduce C's signed compare for negative values — ledgered in a comment); tag vs literal emit; final ctrlb flush + `result_size>=result_max` fail. `slen` overflow of `int32` surfaces as `SizeOverflow` (C is called only with valid int32 lengths). |
| 12 | `pglz_decompress` | c:691-828 | lib.rs `pglz_decompress` 404-419 + `pglz_decompress_to_slice` 426-505 | MATCH | Outer `sp<srcend && dp<destend`; 8-item ctrl group with same inner guard; tag decode `len=(b0&0x0f)+3`, `off=((b0&0xf0)<<4)|b1`, `len==18 → len+=ext`; corrupt iff `off==0 || off>dp`; `len=min(len,destend-dp)`; overlap doubling copy then final copy; literal copy; `check_complete` requires exact fill. See §2 for the source-bounds-check equivalence. Wrapper allocates `rawsize` zeroed `PgVec`, truncates to returned len. |
| 13 | `pglz_maximum_compressed_size` | c:845-876 | lib.rs:510-515 | MATCH | `((i64)rawsize*9+7)/8 + 2`, capped at `total_compressed_size`, cast to i32. int64 intermediate preserved. Test covers (0,8,100) cases. |

No `MISSING` / `PARTIAL` / `DIVERGES`. No `#if`-gated C branch is excluded by
the build config (FRONTEND vs backend only changes the `postgres.h` include,
not codec logic).

## 2. Re-derived spot-checks (auditor self-check)

- **hist_idx sign extension (#5):** C reads input via `const char *` (signed on
  all PG targets); the c2rust rendering casts through `c_char` and the shifts
  are on the sign-extended `c_int`. Port casts `u8 as i8 as i32`. For input
  `0x80` four-byte branch the bucket is `((-128)<<6 ^ (-128)<<4 ^ (-128)<<2 ^
  -128) & 0x3FF = 384`, matching the test. A naive `u8` port would pick a
  different bucket and emit a valid-but-non-identical stream; the port is
  correct.
- **decompress source bounds (#12):** C reads `sp[0]`/`sp[1]` (and the ext byte)
  *before* checking `sp > srcend`, tolerating a 1-byte over-read of garbage and
  catching it post-hoc with `sp > srcend → return -1`. The port instead checks
  `sp+2 > source.len()` (and `sp >= len` for the ext byte) *before* reading and
  returns `CorruptInput`. In every case where C over-reads, the tag was
  truncated and C returns `-1`; the port returns `CorruptInput` for the same
  inputs. Return value is identical (`-1` ⇔ corrupt); the only difference is the
  port never performs the discarded over-read. Behavior-preserving.
- **overlap copy (#12):** `copy_within` is memmove-semantics, but each step
  copies exactly `off` non-overlapping bytes (`[dp-off, dp)` → `[dp, dp+off)`),
  so it matches C's `memcpy`; the final `copy_within(dp-off .. dp-off+len, dp)`
  has `off >= len` (loop exit) → also non-overlapping. Identical bytes emitted.
- **first_success_by negative (#11):** C compares the signed `bp-bstart` (≥0)
  against signed `first_success_by`; a negative strategy value fails the very
  first iteration. Port's `first_success_by.max(0) as usize` reproduces this
  (0 ≤ output.len() always true on iter 1). Guarded by
  `negative_match_size_drop_roundtrips` and the InvalidStrategy test.

## 3. Seam and wiring audit

**Owned seam crate (by C-source coverage):** `crates/common-pglz-seams` maps to
`pg_lzcompress.c`, this unit's only C source. It declares two seams:

- `pglz_decompress_to_slice(&[u8], &mut [u8], bool) -> PgResult<Option<usize>>`
- `pglz_maximum_compressed_size(i32, i32) -> i32`

Consumer: `backend-access-common-detoast` (`src/lib.rs:479,633,661`) calls both
via `common_pglz_seams::…::call`. The cycle is real — detoast is a TOAST-layer
crate above the codec leaf; routing through a seam keeps the dependency
direction clean. (Decompression is a pure byte transform, so these are not
allocating seams and need no `Mcx`; the `PgResult<Option<usize>>` carries C's
`-1`-as-`None` so the caller raises its own `ERRCODE_DATA_CORRUPTED`.)

**Finding (FIXED, was the sole FAIL):** on the as-received branch, both seam
declarations were **uninstalled** — `common-pglz` had no `init_seams()` and was
absent from `seams-init::init_all()`. Per SKILL §3 ("an empty installer with
owned seam crates outstanding is an automatic FAIL"; "every declaration in every
owned seam crate must be installed") this is a merge blocker.

**Fix applied on branch:**
- Added `common_pglz::init_seams()` (lib.rs) containing only two `set()` calls:
  `pglz_decompress_to_slice` (via a thin marshal adapter `Ok(len)→Ok(Some)`,
  `Err(_)→Ok(None)` — argument-passthrough + result-conversion only, no branching
  on data) and `pglz_maximum_compressed_size` (direct `fn`-pointer install).
- Added `common-pglz-seams` dep to `crates/common-pglz/Cargo.toml`.
- Added `common-pglz` dep + `common_pglz::init_seams();` line to `seams-init`
  (Cargo.toml + `init_all()`).

After the fix: every owned-seam declaration is installed by the owner's
`init_seams()`, which is `set()`-only and is reached from `init_all()`. No
uninstalled seams, no `set()` outside the owner, no computation in a seam path.
The adapter is a thin marshal+delegate. Zero seam findings.

## 3b. Design conformance

- **Opacity (types.md 6-7):** none introduced — `PGLZ_Strategy`/`HistEntry` are
  real concrete structs; no invented handles.
- **Mcx + PgResult on allocating fns:** `pglz_compress` / `pglz_decompress` /
  `History::new` / `push*` all take `Mcx` and return `PgResult`; allocation
  failure surfaces as the context OOM error, not an abort. Allocation-free pure
  paths (`pglz_decompress_to_slice`, `pglz_maximum_compressed_size`, `hist_idx`,
  `find_match`) correctly omit `Mcx`.
- **Shared statics:** C's process-static `hist_start`/`hist_entries` scratch
  arrays are replaced by a per-call `History` charged to the caller's `Mcx` — no
  shared backend-global static, which is the correct repo treatment for
  per-call scratch.
- **Ambient-global / registry-shaped seams, locks across `?`, unledgered
  divergence markers:** none. The one deliberate divergence (signed `first_success_by`
  clamp; SizeOverflow on >int32 input) is ledgered in code comments.

## 4. Verdict

All 13 inventory rows MATCH; the lone seam finding (uninstalled owned-seam
crate) was fixed on the branch and re-audited from scratch. Build of
`common-pglz` + `seams-init` is green; `cargo test -p common-pglz` = 14/14 pass.

**PASS.** CATALOG row set to `audited`. Not merged to main.
