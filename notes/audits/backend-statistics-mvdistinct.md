# Audit: backend-statistics-mvdistinct (mvdistinct.c)

C source: `src/backend/statistics/mvdistinct.c` (PostgreSQL 18.3, 699 LOC).
Crate: `crates/backend-statistics-mvdistinct` (partial port — the ndistinct
slice of the combined `backend-statistics-core` unit). Sibling of
`backend-statistics-dependencies`; ported the same way.

This is a PARTIAL port. The per-combination estimator kernel
(`ndistinct_for_combination`) is seamed to the unported owner (it needs the
multi-sort support + the per-column `VacAttrStats` matrix + the value matrices
inside the opaque `StatsBuildData`); the `pg_statistic_ext_data` syscache read
of `statext_ndistinct_load` is also seamed. Everything else is ported in-crate.
Every C function is enumerated below with its disposition.

## Constants & macros (verified vs C / catalog headers)

| C symbol | Value | Where | Verified against |
|---|---|---|---|
| `STATS_MAX_DIMENSIONS` | 8 | types-statistics | statistics.h |
| `STATS_NDISTINCT_MAGIC` | `0xA352BFA4` (u32) | types-statistics (new) | statistics.h |
| `STATS_NDISTINCT_TYPE_BASIC` | 1 (u32) | types-statistics (new) | statistics.h |
| `STATS_EXT_NDISTINCT` | `'d'` (i8) | types-statistics | pg_statistic_ext.h |
| `SizeOfHeader` | `3*sizeof(uint32)` = 12 | lib.rs `SIZE_OF_HEADER` | mvdistinct.c:45 |
| `SizeOfItem(n)` | `sizeof(double)+sizeof(int)+n*sizeof(AttrNumber)` = `8+4+2n` | lib.rs `size_of_item` | mvdistinct.c:48-49 |
| `MinSizeOfItem` | `SizeOfItem(2)` = 16 | lib.rs `min_size_of_item` | mvdistinct.c:52 |
| `MinSizeOfItems(n)` | `SizeOfHeader + n*MinSizeOfItem` | lib.rs `min_size_of_items` | mvdistinct.c:55-56 |
| `VARHDRSZ` | 4 | lib.rs `VARHDRSZ` | varatt.h |
| `InvalidAttrNumber` | 0 | lib.rs `INVALID_ATTR_NUMBER` | attnum.h |
| `AttrNumber` | i16; `Oid` u32 | types-core | c.h / postgres_ext.h |

Verified `STATS_NDISTINCT_MAGIC == 0xA352BFA4` against
`pgrust-pg-ffi-fgram/src/statistics.rs:36` (independent c2rust extraction) and
the C header. The serialized form is native-endian (`to_ne_bytes` /
`from_ne_bytes`), matching the C `memcpy(&field, ...)`. NOTE the item layout uses
`sizeof(int)` (4 bytes) for `nattributes` even though `MVNDistinctItem.nattributes`
is an `int` in C (whereas the dependency item uses `sizeof(AttrNumber)`); the
written values are `AttrNumber` (i16) for the attribute array but i32 for the
count — matched exactly (`write_i32` for the count, `write_attnum`/i16 for each
attr). The varlena framing (`VARHDRSZ`/`SET_VARSIZE`) is included in the
serialized `Vec<u8>` (the C `bytea` includes its header); the deserializer
decodes and validates it before the body.

## Function-by-function

| C function | C lines | Disposition | Notes |
|---|---|---|---|
| `statext_ndistinct_build` | 87-141 | PORTED in-crate (kernel seamed) | Generator loop k=2..numattrs + `MVNDistinct` assembly; index->attnum translation via `data_attnums` (the owner-supplied `data->attnums`); `AttributeNumberIsValid` -> debug_assert. Each item's `ndistinct` comes from the `ndistinct_for_combination` seam. `palloc` -> `try_reserve` (OOM via `mcx.oom`). numattrs bounded to STATS_MAX_DIMENSIONS (allocation-safety: `1<<n` overflow guard). itemcnt == numcombs -> debug_assert. |
| `statext_ndistinct_load` | 147-172 | PORTED in-crate (syscache seamed) | `SearchSysCache2`+`SysCacheGetAttr`+`ReleaseSysCache` -> `statext_ndistinct_load_bytea` seam returning Some(bytes)/None(isnull)/Err(missing-tuple). isnull -> `elog(ERROR, "requested statistics kind \"d\" ... %u")` text in-crate. Then `statext_ndistinct_deserialize`. |
| `statext_ndistinct_serialize` | 178-243 | PORTED in-crate | Exact byte layout: VARHDRSZ + header (magic/type/nitems u32-ne) + per-item (ndistinct f64, nattributes i32, attributes i16 each). `Assert(magic/type)` + per-item `Assert(nmembers>=2)` + bound asserts -> debug_assert. `SET_VARSIZE` -> `set_varsize`. |
| `statext_ndistinct_deserialize` | 249-329 | PORTED in-crate | NULL -> None. All `elog(ERROR)` validity checks: too-small-for-header, bad magic (`%08x`), bad type (`%d`), zero nitems, min_size_of_items. Per-item nattributes range `[2,STATS_MAX_DIMENSIONS]` (C Assert -> hard recoverable check, it bounds an alloc). Allocation-safety: varlena length validated vs buffer before exhdr derivation + nitems abs cap. Bounds asserts -> debug_assert. Consumed-exactly -> debug_assert. |
| `pg_ndistinct_in` | 338-346 | PORTED in-crate | `Err(FEATURE_NOT_SUPPORTED, "cannot accept a value of type pg_ndistinct")`. |
| `pg_ndistinct_out` | 354-385 | PORTED in-crate | deserialize + StringInfo built in-crate. Format: `{` then per-item `"%s%d"` with `(j==0)?"\"":", "` prefix, then `"\": %d"` with `(int) ndistinct`. Items joined by `, `. cstring NUL-terminated. |
| `pg_ndistinct_recv` | 391-399 | PORTED in-crate | same as `_in`. |
| `pg_ndistinct_send` | 407-411 | PORTED in-crate | `return byteasend(fcinfo)` -> delegates to ported `backend_utils_adt_varlena::bytea::byteasend`. |
| `ndistinct_for_combination` | 424-517 | SEAMED | Needs `multi_sort_init`/`multi_sort_add_dimension`/`multi_sort_compare`/`qsort_interruptible` over `data->values`/`nulls`/`stats` matrices + `lookup_type_cache(...)->lt_opr`/`attrcollid`. All owner-side (extended_stats.c + vacuum). Reached via `backend_statistics_core_seams::ndistinct_for_combination`. Mirrors dependencies.c's `dependency_degree` seam. |
| `estimate_ndistinct` | 520-542 | PORTED in-crate (pub) | The Duj1 estimator `n*d/(n-f1+f1*n/N)` + clamp-to-`d` + clamp-to-totalrows + `floor(x+0.5)`. Kept `pub` so the seamed kernel owner reuses the exact arithmetic. |
| `n_choose_k` | 549-568 | PORTED in-crate | symmetry `k=min(k,n-k)` + the overflow-safe `r*=n--; r/=d` loop. `Assert((k>0)&&(n>=k))` -> debug_assert. |
| `num_combinations` | 574-578 | PORTED in-crate | `(1<<n)-(n+1)`. |
| `generator_init` | 588-617 | PORTED in-crate | `CombinationGenerator::init`. Pre-allocates `k*ncombinations` ints, generates, asserts current==ncombinations, resets current=0. |
| `generator_next` | 626-633 | PORTED in-crate | `CombinationGenerator::next` -> Option<start-index>; `combination(idx)` borrows the k-slice. |
| `generator_free` | 641-646 | PORTED in-crate | Drop of the `combinations` Vec. |
| `generate_combinations_recurse` | 656-685 | PORTED in-crate | ascending lexicographic recursion (index<k) + leaf `memcpy` into `combinations[k*current]` + current++. |
| `generate_combinations` | 691-699 | PORTED in-crate | `palloc0` scratch `current` -> zero Vec; recurse; `pfree` -> drop. |

## Types added (types-statistics)

- `MVNDistinct { magic:u32, r#type:u32, items:Vec<MVNDistinctItem> }` — owned
  mirror of the C FAM struct (`nitems == items.len()`).
- `MVNDistinctItem { ndistinct:f64, attributes:Vec<AttrNumber> }` — owned mirror
  (`nattributes == attributes.len()`).
- `STATS_NDISTINCT_MAGIC`, `STATS_NDISTINCT_TYPE_BASIC` constants.

Modeled exactly like the existing `MVDependencies`/`MVDependency` precedent
(dependencies.c). `StatsBuildDataHandle` (opaque identity handle) reused.

## Seams declared (backend-statistics-core-seams)

- `ndistinct_for_combination(totalrows:f64, data:StatsBuildDataHandle, k:i32,
  combination:&[i32]) -> PgResult<f64>` — the estimator kernel.
- `statext_ndistinct_load_bytea(mvoid:Oid, inh:bool) -> PgResult<Option<Vec<u8>>>`
  — the syscache read.

Both are OWNED by `backend-statistics-core` (CATALOG: in-progress/todo), so the
`every_declared_seam_is_installed_by_its_owner` guard exempts them until the
owner lands and installs them from its `init_seams()` (mirror-pg-and-panic: a
call panics loudly until then). This crate installs NO inward seams.

## Divergences

None of contract: all public signatures speak owned values
(`MVNDistinct`/`Vec<u8>`) and `Mcx`/`PgResult`, consistent with the dependencies
port and the repo model. The opaque `StatsBuildDataHandle` is INHERITED opacity
(the `VacAttrStats`/value matrices are unported), not introduced. No
todo!/unimplemented!/stubs.

## Tests

16 unit tests: constant/MAGIC verification, serialize byte-layout (offsets +
varlena header), serialize/deserialize round-trip, NULL->None, error paths
(short header, bad magic, bad type, zero nitems, too-small-for-nitems),
`num_combinations`/`n_choose_k` vs C, generator enumeration (2-of-3, 3-of-4,
lexicographic), Duj1 estimator (normal + both clamps), `pg_ndistinct_in/_recv`
disallowed, `pg_ndistinct_out` formatting.

## Gate

- `cargo check --workspace`: exit 0 (only pre-existing warnings in other crates).
- `cargo test -p backend-statistics-mvdistinct`: 16 passed.
- `no-todo-guard`, `seams-init` guards: pass.
