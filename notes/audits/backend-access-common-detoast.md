# Audit: backend-access-common-detoast

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)
- Branch: `port/backend-access-common-detoast`
- Unit scope: `src/backend/access/common/detoast.c` (the detoast slice of the
  `backend-access-common-toastdesc` catalog row; `toast_internals.c` /
  `tupdesc.c` are separate ported units and not in scope here).
- Port crate: `crates/backend-access-common-detoast/src/lib.rs`
- C source: `../pgrust/postgres-18.3/src/backend/access/common/detoast.c`
- c2rust: `../pgrust/c2rust-runs/backend-access-common-toastdesc/src/detoast.rs`

Independent re-derivation: every function enumerated from the C, cross-checked
against the c2rust rendering and the headers (`varatt.h`,
`access/toast_compression.h`), not from the port's comments. Gate re-run:
`cargo check` of the crate + its four seam crates + `seams-init` passes; 23
in-crate tests pass.

## Function inventory (detoast.c)

| # | C function (detoast.c) | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `detoast_external_attr` | 44-102 | lib.rs:333 | MATCH | ondisk→fetch seam; indirect→deref seam + recurse-if-external else verbatim copy; expanded→EOH flat seam; plain→verbatim copy (always-owned deviation of C `result=attr`). |
| 2 | `detoast_attr` | 115-191 | lib.rs:374 | MATCH | ondisk→fetch then decompress-if-compressed (drop tmp); indirect→deref+recurse (C's copy-if-unchanged subsumed by always-owned recursion result); expanded→flatten via detoast_external_attr + extended assert; compressed→decompress; short→4B reheader; plain→verbatim copy. |
| 3 | `detoast_attr_slice` | 204-333 | lib.rs:430 | MATCH | sliceoffset<0 elog; slicelimit overflow→both -1; ondisk fast-path / pglz max_size / full fetch; indirect tail recurse; expanded flatten; decompress slice-or-full; short/4B attrdata+attrsize split; final reset/recompute and palloc+memcpy. All branches and the slice arithmetic match C and c2rust (lines 2079-2093) exactly. |
| 4 | `toast_fetch_datum` (static) | 342-382 | — | SEAMED | `toast_seam::toast_fetch_datum` (cycle partner `backend-access-common-toast-internals`); genuine dep (heap chunk fetch via tableam). Thin call. |
| 5 | `toast_fetch_datum_slice` (static) | 395-463 | — | SEAMED | `toast_seam::toast_fetch_datum_slice`. Same partner; thin call. |
| 6 | `toast_decompress_datum` (static) | 470-492 | lib.rs:573 | MATCH | `switch(cmid)`: PGLZ→in-crate `pglz_decompress_datum`; LZ4→`lz4_seam`; default→`elog(ERROR,"invalid compression method id %d")`. |
| 7 | `toast_decompress_datum_slice` (static) | 502-535 | lib.rs:593 | MATCH | `(uint32)slicelength >= TOAST_COMPRESS_EXTSIZE` short-circuit then same switch; PGLZ→in-crate slice, LZ4→seam, default→elog. |
| 8 | `toast_raw_datum_size` | 544-592 | lib.rs:681 | MATCH | ondisk→va_rawsize; indirect→deref+recurse; expanded→EOH flat; compressed→extsize+VARHDRSZ; short→normalize to VARHDRSZ; plain→VARSIZE. |
| 9 | `toast_datum_size` | 600-646 | lib.rs:709 | MATCH | ondisk→extsize; indirect→deref+recurse; expanded→EOH flat; short→VARSIZE_SHORT; plain→VARSIZE. |

### Inlined tails from sibling C files (not in this unit's c_sources)

| C function | C file | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `pglz_decompress_datum` | toast_compression.c:82 | lib.rs:624 | MATCH | palloc extsize+VARHDRSZ; `pglz_decompress` over `common-pglz-seams` (check_complete=true); rawsize<0→`ERRCODE_DATA_CORRUPTED "compressed pglz data is corrupt"`; SET_VARSIZE(rawsize+VARHDRSZ). |
| `pglz_decompress_datum_slice` | toast_compression.c:108 | lib.rs:649 | MATCH | palloc slicelength+VARHDRSZ; pglz seam check_complete=false; same corruption ereport. |
| `pg_detoast_datum` | fmgr.c | lib.rs:742 | MATCH | extended→detoast_attr else verbatim copy (always-owned deviation of `return datum`). |
| `pg_detoast_datum_copy` | fmgr.c | lib.rs:752 | MATCH | extended→detoast_attr else verbatim copy. |
| `pg_detoast_datum_slice` | fmgr.c | lib.rs:763 | MATCH | →detoast_attr_slice(first,count). |
| `pg_detoast_datum_packed` | fmgr.c | lib.rs:776 | MATCH | compressed||external→detoast_attr else verbatim copy (keeps short header short). |

These tails are correctly placed here: `toast_decompress_datum`'s `switch`
dispatches to the two `pglz_*` routines (the LZ4 peers stay seamed because LZ4
is a `#ifdef USE_LZ4` external build dep), and `backend-utils-fmgr-core`
explicitly defers the `pg_detoast_datum*` one-liners to "the varlena/Detoast
subsystem" (fmgr-core lib.rs:15-17). No duplication, no missing logic. The
decompression primitive itself (`pglz_decompress`) is seamed to `common-pglz`.

## Constants / header verification (against varatt.h, toast_compression.h)

All transcribed values verified against the headers, not memory:

- `VARTAG_INDIRECT=1`, `VARTAG_ONDISK=18`, `VARTAG_EXPANDED_RO=2`,
  `VARTAG_EXPANDED_RW=3` — varatt.h:86-89. MATCH.
- `VARLENA_EXTSIZE_BITS=30`, `VARLENA_EXTSIZE_MASK=(1<<30)-1=0x3FFFFFFF` —
  varatt.h:45-46. MATCH.
- `VARHDRSZ_EXTERNAL=2`, `VARHDRSZ_SHORT=1`, `VARHDRSZ_COMPRESSED=VARHDRSZ+4` —
  match the `offsetof` definitions (varatt.h:253-255). MATCH.
- `TOAST_PGLZ_COMPRESSION_ID=0`, `TOAST_LZ4_COMPRESSION_ID=1` —
  toast_compression.h:39-40. MATCH.
- `VARTAG_SIZE`: INDIRECT/EXPANDED → one in-memory pointer (`varatt_indirect` /
  `varatt_expanded` are each a single pointer field); ONDISK → 16
  (`varatt_external` = four packed 4-byte fields); else Trap. MATCH varatt.h:96-100.
- Bit-twiddling (LE): `VARSIZE_4B = (w>>2)&mask`, `VARSIZE_1B=(h>>1)&0x7F`,
  `VARATT_IS_1B_E = h==0x01`, `VARATT_IS_1B = h&1`, `VARATT_IS_4B_C = (h&3)==2`,
  `SET_VARSIZE_4B = len<<2`. All MATCH varatt.h:211-235. `varsize_4b`/`set_varsize`
  are `#[cfg]`-guarded per endianness and the BE forms also match (varatt.h:192-200).
- `varatt_is_4b` is documented as `VARATT_IS_4B` but implements
  `(b&0x03)==0` = `VARATT_IS_4B_U`; it is consumed only by `varatt_is_extended`
  (`!varatt_is_4b`), which is exactly C's `VARATT_IS_EXTENDED = !VARATT_IS_4B_U`.
  Behavior MATCH; the doc-comment label is the only inaccuracy (cosmetic).
- `VARATT_EXTERNAL_IS_COMPRESSED`: `extsize < (va_rawsize - VARHDRSZ)`. Port uses
  `u32 < va_rawsize.wrapping_sub(VARHDRSZ)`; for valid pointers va_rawsize≥VARHDRSZ
  so wrapping matches C's signed→unsigned promotion. MATCH.

## Seam audit (§3)

Owned seam crate (by C-source coverage — `detoast.c`): **`backend-access-common-detoast-seams`**.
- Declares exactly `detoast_external_attr`, `detoast_attr`.
- `init_seams()` (lib.rs:800) installs both via `set()` and nothing else.
- `seams-init::init_all()` calls `backend_access_common_detoast::init_seams()`. OK.

Outbound seams — each a genuine dependency cycle / external dep, thin
marshal+delegate, no branching or node construction in any seam path:
- `backend-access-common-toast-internals-seams`: `toast_fetch_datum`,
  `toast_fetch_datum_slice`, `indirect_pointer` (cycle partner, unported → panics).
- `backend-access-common-toast-compression-seams`: `lz4_decompress_datum`,
  `lz4_decompress_datum_slice` (`#ifdef USE_LZ4` external dep).
- `common-pglz-seams`: `pglz_decompress_to_slice`, `pglz_maximum_compressed_size`
  (pure byte transforms; `common-pglz` unported).
- `backend-utils-adt-misc2-seams`: `eoh_get_flat_size`, `eoh_flatten_into`
  (expanded-object subsystem; `ExpandedObjectRef` typed handle).

No function body was replaced by a "somewhere else" seam call — all detoast.c
logic lives in this crate. No uninstalled or misplaced `set()`.

## Design conformance (§3b)

- Allocating entry points all take `Mcx<'mcx>` and return `PgResult` (no
  `&'static mut`, no ambient allocation). OK.
- Opacity inherited, not invented: `ExpandedObjectRef` is a real types-datum
  handle (C `ExpandedObjectHeader *` via `DatumGetEOHP`); `VarattExternal`
  mirrors C `struct varatt_external` field-for-field. No invented handles
  (types.md rules 6-7). OK.
- No shared statics for per-backend globals; no registry-shaped side tables; no
  locks held across `?`. OK.
- Always-owned deviation (verbatim copy where C returns the input pointer) is
  documented at module level (lib.rs:21-29) and is behavior-preserving for the
  varlena contract; callers must not assume `result == input` identity. Ledgered.
- Error surface mirrors C: `invalid sliceoffset` / `invalid compression method
  id` elog→`PgError::error`; pglz corruption → `ERRCODE_DATA_CORRUPTED`. OK.

### Non-blocking note

The 1-byte header-form predicates (`varatt_is_external/_1b/_1b_e/_compressed`,
`varsize_1b`) hardcode the little-endian bit patterns and are not
`#[cfg(target_endian)]`-guarded (only the 4-byte word ops `varsize_4b` /
`set_varsize` are). Behavior is provably identical to C on the little-endian
build target; the big-endian forms would diverge. This matches the prevailing
pattern across already-merged ports and is latent only on an unsupported BE
target, so it is recorded rather than blocking.

## Conclusion

All 9 detoast.c functions are MATCH or properly SEAMED; the inlined
toast_compression.c / fmgr.c tails are MATCH and correctly placed; zero seam
findings; no §3b violations. **PASS.**
