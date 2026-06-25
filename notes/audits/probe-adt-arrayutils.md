# Audit: probe-adt-arrayutils

- **Verdict:** PASS
- **Date:** 2026-06-13
- **Model:** Claude Fable 5 (Opus 4.8 1M)
- **Branch:** port/probe-adt-arrayutils
- **C source:** `src/backend/utils/adt/arrayutils.c`
- **c2rust oracle:** `../pgrust/c2rust-runs/probe-adt-arrayutils/src/arrayutils.rs`
- **Port crate:** `crates/backend-utils-adt-arrayutils`
- **Seam crate:** `crates/backend-utils-adt-arrayutils-seams`

## Function inventory

The c2rust completeness oracle exposes 10 `#[no_mangle]` definitions
(ArrayGetNItems and ArrayCheckBounds are thin wrappers over their `*Safe`
forms). Every one is accounted for.

| # | C function (arrayutils.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ArrayGetOffset` (L31) | `array_get_offset` | MATCH | loop `i=n-1..0`, `offset += (indx-lb)*scale; scale*=dim`. Pure math, infallible. |
| 2 | `ArrayGetNItems` (L56) | `array_get_n_items` | MATCH | wraps `*_safe(ndim,dims,None)`; overflow throws (hard error). |
| 3 | `ArrayGetNItemsSafe` (L66) | `array_get_n_items_safe` | MATCH | ndim<=0 → 0; `ret=1`; per-dim: `dims[i]<0` → `ereturn(-1)`; `prod=(i64)ret*dims[i]`; `ret=(i32)prod`; `ret!=prod` → `ereturn(-1)`; final `ret>MaxArraySize` → `ereturn(-1)`. `debug_assert!(ret>=0)` mirrors C `Assert`. errcode `54000` (PROGRAM_LIMIT_EXCEEDED), msg `"array size exceeds the maximum allowed (%d)"`. |
| 4 | `ArrayCheckBounds` (L116) | `array_check_bounds` | MATCH | wraps `*_safe(...,None)`, discards bool. |
| 5 | `ArrayCheckBoundsSafe` (L126) | `array_check_bounds_safe` | MATCH | per-dim `pg_add_s32_overflow(dims[i],lb[i])` modeled by `checked_add().is_none()` → `ereturn(false)` with errcode `54000`, msg `"array lower bound is too large: %d"` (lb[i]); else return true. |
| 6 | `mda_get_range` (L152) | `mda_get_range` | MATCH | `span[i] = endp[i]-st[i]+1`. |
| 7 | `mda_get_prod` (L166) | `mda_get_prod` | MATCH | `prod[n-1]=1`; `i=n-2..0`: `prod[i]=prod[i+1]*range[i+1]`. |
| 8 | `mda_get_offset_values` (L182) | `mda_get_offset_values` | MATCH | `dist[n-1]=0`; outer `j=n-2..0`, inner `i=j+1..n` accumulate `dist[j]-=(span[i]-1)*prod[i]`. |
| 9 | `mda_next_tuple` (L207) | `mda_next_tuple` | MATCH | n<=0 → -1; `curr[n-1]=(+1)%span[n-1]`; carry loop `while i!=0 && curr[i]==0`; return `i` / `0` (if curr[0]) / `-1`. |
| 10 | `ArrayGetIntegerTypmods` (L232) | `array_get_integer_typmods` | SEAMED | 3 hard-error guards (elemtype != CSTRINGOID → `2202E` ARRAY_ELEMENT_ERROR; ndim != 1 → `2202E` ARRAY_SUBSCRIPT_ERROR; `array_contains_nulls` → `22004` NULL_VALUE_NOT_ALLOWED) then `deconstruct_array_builtin(arr, CSTRINGOID)` and `pg_strtoint32` per element. palloc → `Vec` with `try_reserve` (OUT_OF_MEMORY on failure). Element cstring decode (`DatumGetCString`) is delegated to the detoast/byref-payload owner via `detoast_seam::detoast_attr` — see seam audit. |

## Constants verified against c2rust / headers

- `MaxArraySize = MaxAllocSize / sizeof(Datum)` = `0x3fffffff / 8`; port `MAX_ARRAY_SIZE = MAX_ALLOC_SIZE / size_of::<usize>()` (reused from arrayfuncs `foundation`). ✓
- `CSTRINGOID = 2275`. ✓
- SQLSTATEs decoded from the c2rust `errcode(...)` bit-math: `PROGRAM_LIMIT_EXCEEDED=54000`, `ARRAY_ELEMENT_ERROR=2202E`, `ARRAY_SUBSCRIPT_ERROR=2202E` (PG aliases the two), `NULL_VALUE_NOT_ALLOWED=22004` — all match `types-error`. ✓
- All error message format strings verbatim. ✓

## Seam audit

Owned seam crate (by C-source coverage of `arrayutils.c`):
`crates/backend-utils-adt-arrayutils-seams`. It declares exactly 7 inward
seams — the pure-math / overflow-checking routines that `arrayfuncs.c` (and
other neighbors) call into:

`array_get_offset`, `array_get_n_items`, `array_check_bounds`,
`mda_get_range`, `mda_get_prod`, `mda_get_offset_values`, `mda_next_tuple`.

`init_seams()` installs all 7 with `set()` calls and nothing else; no seam is
left uninstalled and no extra `set()` exists. `ArrayGetIntegerTypmods` is a
direct public fn (not a declared seam) — correct, as it has no inward caller
needing the cycle-breaking indirection.

- **Inward seams:** the 7 above. `init_seams()` is wired into
  `seams-init::init_all()` (lib.rs L119) and the `recurrence_guard` test
  (`every_seam_installing_crate_is_wired_into_init_all`) passes.
- **Outward seam:** `array_get_integer_typmods` resolves the per-element
  `cstring` payload (`DatumGetCString`) through
  `backend_access_common_detoast_seams::detoast_attr`. This is the **inherited**
  byref-payload model used verbatim by the already-ported sibling
  `backend-utils-adt-arrayfuncs` (`datum_payload_bytes`,
  `deconstruct_text_array`, etc., all route byref element bytes through the
  same detoast seam with the same `datum_as_byte_window` stand-in). It is not
  opacity introduced here (memory rule "opacity inherited, never introduced"):
  the detoast subsystem is the real, unported owner of byref/varlena payload
  resolution across the whole array subsystem, and the seam panics loudly until
  it lands — mirroring the C pointer deref's dependence on real memory. The
  seam path is thin marshal + delegate (no branching/computation). Acceptable
  SEAMED.

No `set()` outside the owner; no logic relocated out of this crate; no
own-logic stub silently returning a value (the byref path faults via the
unimplemented detoast seam).

## Design conformance (3b)

- Allocating fn (`array_get_integer_typmods`) takes `Mcx` and returns
  `PgResult`. ✓
- Soft-error path uses `ereturn` into `Option<&mut SoftErrorContext>` (mirrors
  C `ereturn(escontext, ...)`); hard-error path uses `ereport(ERROR)`. ✓
- No shared statics, no ambient globals, no locks-across-`?`, no registry side
  tables, no unledgered divergence markers. The acyclic dep on arrayfuncs is
  real (arrayfuncs depends only on the `-seams` crate, not back on arrayutils).

## Gate

- `cargo check --workspace`: clean (warnings only).
- `cargo test --workspace`: all green; the 2 documented timeout flakes did not
  fire this run. `seams-init recurrence_guard`: pass. Crate unit tests: 8
  passed.

## Conclusion

Every function MATCH or (for `ArrayGetIntegerTypmods`) SEAMED per the inherited
detoast byref model; zero seam findings; all constants/SQLSTATEs verified;
wiring and recurrence guard confirmed; workspace gate green. **PASS.**
