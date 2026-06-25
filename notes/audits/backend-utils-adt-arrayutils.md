# Audit: backend-utils-adt-arrayutils (probe-adt-arrayutils)

C source: `src/backend/utils/adt/arrayutils.c`. Re-audited for the
seam-verification lane: focus on `ArrayGetIntegerTypmods`, full re-derivation of
the rest.

## Function inventory (10 C functions)

| C function (arrayutils.c) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `ArrayGetOffset` (L32) | `array_get_offset` | MATCH | reverse loop, `offset += (indx-lb)*scale; scale *= dim`. |
| `ArrayGetNItems` (L57) | `array_get_n_items` | MATCH | thin wrapper to safe form, `escontext=None`. |
| `ArrayGetNItemsSafe` (L67) | `array_get_n_items_safe` | MATCH | `ndim<=0 → 0`; negative-dim and i32-overflow and `>MaxArraySize` all `ereturn(-1, ERRCODE_PROGRAM_LIMIT_EXCEEDED)`. |
| `ArrayCheckBounds` (L117) | `array_check_bounds` | MATCH | wrapper to safe form. |
| `ArrayCheckBoundsSafe` (L127) | `array_check_bounds_safe` | MATCH | `pg_add_s32_overflow` → `checked_add`, `ereturn(false, ...)` with "array lower bound is too large". |
| `mda_get_range` (L153) | `mda_get_range` | MATCH | `span[i]=endp[i]-st[i]+1`. |
| `mda_get_prod` (L167) | `mda_get_prod` | MATCH | `prod[n-1]=1; prod[i]=prod[i+1]*range[i+1]`. |
| `mda_get_offset_values` (L183) | `mda_get_offset_values` | MATCH | `dist[n-1]=0`; inner subtraction loop matches. |
| `mda_next_tuple` (L208) | `mda_next_tuple` | MATCH | carry loop + return positions; `n<=0 → -1`. |
| `ArrayGetIntegerTypmods` (L233) | `array_get_integer_typmods` | MATCH (FIXED) | see below. |

## ArrayGetIntegerTypmods — fix detail

C:
```
if (ARR_ELEMTYPE != CSTRINGOID) ereport(ERRCODE_ARRAY_ELEMENT_ERROR);
if (ARR_NDIM != 1)              ereport(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
if (array_contains_nulls(arr))  ereport(ERRCODE_NULL_VALUE_NOT_ALLOWED);
deconstruct_array_builtin(arr, CSTRINGOID, &elem_values, NULL, n);
for i: result[i] = pg_strtoint32(DatumGetCString(elem_values[i]));
```

The three guards, the `deconstruct_array_builtin` call, and the
`pg_strtoint32` loop all match. The element decode `DatumGetCString(elem)` is
the only point of interest:

- `cstring` is `attlen == -2` / pass-by-reference. In this byte-model,
  `backend_utils_adt_arrayfuncs::foundation::fetch_att` records a by-ref element
  as `Datum::from_usize(off)` — the **in-buffer offset** into the deconstructed
  array bytes (the stand-in for C's `PointerGetDatum(T)` element address).
- `cstring` is NOT a varlena and is never TOASTed, so the bytes are read
  directly from `arr` at that offset: `datum_cstring(arr, ev)` = NUL-terminated
  read from `arr[off..]`. Raw bytes go straight to `pg_strtoint32` with no
  encoding gate, matching C.

### Mislabel that was fixed

Previously the body routed `DatumGetCString` through
`detoast_seam::detoast_attr::call(mcx, datum_as_byte_window(datum))` where
`datum_as_byte_window` returned `&'static []` unconditionally. That is a
disguised stub, not a real seam dispatch: (a) `detoast_attr` is the wrong owner
for a cstring (cstrings are not varlenas / never toasted), and (b) the
hard-empty window meant every typmod decoded to the empty string regardless of
input — the function could never return a correct result. Per the strict
no-deferred rule this was MISLABELED (effectively MISSING). It is now real
own-logic: an in-buffer offset read, with the detoast-seams dependency removed
from `Cargo.toml`. A roundtrip test (`integer_typmods_decode_and_guards`)
builds a real `cstring[]` `ArrayType` (`["10","0","255"]`) and asserts
`[10,0,255]`, plus the wrong-elemtype guard.

## Seam audit

Owned seam crate: `backend-utils-adt-arrayutils-seams` (maps to arrayutils.c).
Declares 7 inward seams; `init_seams()` installs exactly those 7 (`set()` only),
and `seams-init::init_all()` calls `backend_utils_adt_arrayutils::init_seams()`
(crates/seams-init/src/lib.rs:129). No uninstalled seam, no `set()` outside the
owner.

Outward seam calls: NONE remain. (The removed detoast call was the only outward
seam, and it was not a justified dependency-cycle call — arrayfuncs's
construct/deconstruct is a direct dependency.) Element decode now relies only on
direct deps (`backend-utils-adt-arrayfuncs` foundation/construct,
`backend-utils-adt-numutils`).

## Verdict: PASS

All 10 functions MATCH; the one previously-mislabeled SEAMED body is now real
own-logic. Zero `todo!()`/`unimplemented!()`/deferral-panic. Seam wiring clean.
`cargo check --workspace` green; crate tests pass (9/9). Workspace test run had
one unrelated flake (`common-checksum-helper::sha2_dispatch_lengths_and_pointer_lifetime`,
passes in isolation; not in this unit's dep graph).
