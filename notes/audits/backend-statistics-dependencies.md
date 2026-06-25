# Audit: backend-statistics-dependencies (dependencies.c)

C source: `src/backend/statistics/dependencies.c` (PostgreSQL 18.3, 1829 LOC).
Crate: `crates/backend-statistics-dependencies` (partial port — the
functional-dependency slice of the combined `backend-statistics-core` unit).

This is a PARTIAL port. The build-side validation kernel is seamed to the
unported owner; the planner-arena selectivity dispatch is deferred to the owner
(no in-repo caller). Every C function in the file is enumerated below with its
disposition: ported-in-crate / seamed / deferred-to-owner.

## Constants & macros (verified vs C)

| C symbol | Value | Where | Verified against |
|---|---|---|---|
| `STATS_MAX_DIMENSIONS` | 8 | types-statistics | statistics.h |
| `STATS_DEPS_MAGIC` | `0xB4549A2C` (u32) | types-statistics | statistics.h |
| `STATS_DEPS_TYPE_BASIC` | 1 (u32) | types-statistics | statistics.h |
| `STATS_EXT_DEPENDENCIES` | `'f'` (i8) | types-statistics | pg_statistic_ext.h |
| `STATS_EXT_NDISTINCT` | `'d'` | types-statistics | pg_statistic_ext.h |
| `STATS_EXT_MCV` | `'m'` | types-statistics | pg_statistic_ext.h |
| `SizeOfHeader` | `3*sizeof(uint32)` = 12 | lib.rs `SIZE_OF_HEADER` | dependencies.c:38 |
| `SizeOfItem(n)` | `sizeof(double)+sizeof(AttrNumber)*(1+n)` = `8+2*(1+n)` | lib.rs `size_of_item` | dependencies.c:41-42 |
| `CLAMP_PROBABILITY` | `if(p<0)0 elif(p>1)1 else p` | lib.rs `clamp_probability` | selfuncs.h |
| `InvalidAttrNumber` | 0 | inline literal in `pg_dependencies_out` indexing logic | attnum.h |
| `AttrNumber` | i16; `Oid` u32 | types-core | c.h / postgres_ext.h |

Byte layout of the serialized form is native-endian (`to_ne_bytes` /
`from_ne_bytes`), matching the C `memcpy(&field, ...)` which copies host-endian
machine representation. The varlena framing (`VARHDRSZ`/`SET_VARSIZE`) is a
storage concern; the in-crate (de)serializers operate on the
`VARDATA`/`VARSIZE_ANY_EXHDR` body, byte-for-byte equal to the C payload.

## Function-by-function

| C function | C lines | Disposition | Notes |
|---|---|---|---|
| `generate_dependencies_recurse` | 90-153 | PORTED in-crate | `DependencyGenerator::generate_recurse`. First (k-1) ascending recursion + last-element not-in-prefix check + per-tuple append. `repalloc`+`memcpy` -> `try_reserve`+push (OOM via `mcx.oom`). |
| `generate_dependencies` | 156-164 | PORTED in-crate | `DependencyGenerator::generate`. `palloc0` scratch `current` -> zero-filled Vec; `pfree` -> drop. |
| `DependencyGenerator_init` | 172-192 | PORTED in-crate | `DependencyGenerator::init`. `Assert((n>=k)&&(k>0))` -> debug_assert. Pre-generates all variations. |
| `DependencyGenerator_free` | 198-201 | PORTED in-crate | Plain `Drop` of the `dependencies` Vec (no explicit free fn needed). |
| `DependencyGenerator_next` | 203-210 | PORTED in-crate | `DependencyGenerator::next` returns the k-tuple start index; `current` cursor advance identical. |
| `dependency_degree` | 220-329 | **SEAMED** | `backend_statistics_core_seams::dependency_degree`. Body needs `multi_sort_init`/`multi_sort_add_dimension`/`build_sorted_items`/`multi_sort_compare_dim(s)` (extended_stats.c) + per-column `lookup_type_cache(...)->lt_opr` over the opaque `StatsBuildData` `VacAttrStats` matrix — all unported. Failure surface (`elog(ERROR,"cache lookup failed for ordering operator...")`) carried on `PgResult`. Panics until owner lands (mirror-pg-and-panic). |
| `statext_dependencies_build` | 347-437 | PORTED in-crate (control flow) | `statext_dependencies_build`. The generator loop (k=2..=nattnums), per-tuple `dependency_degree` call (seamed), `degree==0.0` skip, `MVDependency` assembly with `d->attributes[i]=data->attnums[dependency[i]]` (via the `data_attnums` slice), and the `MVDependencies` lazy-init with `magic`/`type`/`ndeps` are all in-crate. The transient `dependency_degree cxt` is modeled by `mcx`-charged transient Vecs reclaimed by drop. Returns `None` for the C `NULL` (no non-zero degree). |
| `statext_dependencies_serialize` | 443-493 | PORTED in-crate | `statext_dependencies_serialize`. Exact byte layout: header (magic/type/ndeps u32-ne), per-dep degree (f64-ne) + nattributes (i16-ne) + attributes (i16-ne each). `len` accumulation via `SizeOfItem`; both `Assert(tmp<=...)`/`Assert(tmp==output+len)` -> debug_assert. Returns the body `Vec<u8>` (varlena framing added by storage layer). |
| `statext_dependencies_deserialize` | 498-587 | PORTED in-crate | `statext_dependencies_deserialize`. NULL->None. Every `elog(ERROR)`: invalid size < SizeOfHeader; invalid magic (printed `%d` = signed reinterpret); invalid type (`%d`); zero-length item array; invalid size < `SizeOfItem(ndeps)` (`min_expected_size`). `Assert((k>=2)&&(k<=STATS_MAX_DIMENSIONS))` -> debug_assert. PLUS an allocation-safety bound (`MAX_REASONABLE_NDEPS`, HARD RULE) on the data-derived `ndeps` before reserve. Cursor walks the body identically; both trailing `Assert(tmp<=...)`/`Assert(tmp==data+VARSIZE)` -> debug_assert. |
| `dependency_is_fully_matched` | 594-612 | PORTED in-crate | `dependency_is_fully_matched`. `bms_is_member(attnum, attnums)` over the dependency's attributes -> membership test against the clause-attnum member slice (the caller owns the bitmapset). |
| `statext_dependencies_load` | 618-644 | DEFERRED to owner | NOT in this crate. `SearchSysCache2(STATEXTDATASTXOID,...)` + `SysCacheGetAttr(...stxddependencies...)` + the two `elog(ERROR)` ("cache lookup failed for statistics object %u" / "requested statistics kind \"%c\" is not yet built...") + `DatumGetByteaPP` + `ReleaseSysCache`. Its only C caller is `dependencies_clauselist_selectivity` (also deferred). Lands with `backend-statistics-core` (it will call the in-crate `statext_dependencies_deserialize`). |
| `pg_dependencies_in` | 652-664 | PORTED in-crate | `pg_dependencies_in`. `ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("cannot accept a value of type %s","pg_dependencies")))` -> `Err(PgError::error("cannot accept a value of type pg_dependencies").with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))`. SQLSTATE `0A000` verified. |
| `pg_dependencies_out` | 669-704 | PORTED in-crate | `pg_dependencies_out`. Deserialize the bytea body, then build the cstring in-crate (the C `StringInfo` formatting): `{`, per-dep `"`, attnums with ` => ` before the last / `, ` between, `%d` decimal, then `"`: %f` with `%f` = C printf default 6 fractional digits (`format!("{:.6}")`), `}`, trailing NUL. C unconditionally derefs the deserialize result (a non-NULL detoasted arg always deserializes); `None` mapped to a hard error to match the crash contract without an unsafe deref. |
| `pg_dependencies_recv` | 709-717 | PORTED in-crate | `pg_dependencies_recv`. Same FEATURE_NOT_SUPPORTED error as `_in`. |
| `pg_dependencies_send` | 725-729 | PORTED in-crate | `pg_dependencies_send` -> `return byteasend(fcinfo)`. Delegates to the ported `backend_utils_adt_varlena::bytea::byteasend` (value-typed: copies the payload bytes into the target context). |
| `dependency_is_compatible_clause` | 740-910 | DEFERRED to owner | NOT in this crate. Inspects planner `Node` structs (`IsA RestrictInfo/OpExpr/ScalarArrayOpExpr/Var/RelabelType`, `is_opclause`/`is_orclause`/`is_notclause`, `get_oprrest==F_EQSEL`, `is_pseudo_constant_clause`, `get_notclausearg`, `var->varno/varlevelsup/varattno`) over the planner arena. Only caller is `dependencies_clauselist_selectivity`. Lands with the owner. |
| `find_strongest_dependency` | 928-980 | PORTED in-crate | `find_strongest_dependency`. Returns `(stat_index, dep_index)`. `bms_num_members(attnums)` -> member-slice length; the cheap nattributes/degree skips + the final `dependency_is_fully_matched` check are identical. |
| `clauselist_apply_dependencies` | 1013-1157 | PARTIALLY PORTED | The selectivity-COMBINATION arithmetic (1080-1151: backward chain combine, `P(b\|a)` conditional, product, `CLAMP_PROBABILITY`) is ported as `combine_dependency_selectivities` (reusable kernel). The attnum extraction (1037-1046), per-column `clauselist_selectivity_ext` estimation + clause marking (1048-1078), and `bms_member_index` lookups are planner-arena work done by the owner-side driver (which feeds this kernel `attr_sel` + member-index lists + degrees). |
| `dependency_is_compatible_expression` | 1167-1340 | DEFERRED to owner | NOT in this crate. Like `_compatible_clause` but matches against `statlist` `StatisticExtInfo->exprs` via `equal()`. Planner-arena + node inspection. Lands with the owner. |
| `dependencies_clauselist_selectivity` | 1369-1829 | DEFERRED to owner | NOT in this crate. The full planner-arena driver: `planner_rt_fetch`, `has_stats_of_kind`, per-clause attnum extraction via the compatibility helpers, expression de-duplication with negative attnums + offset, `clauses_attnums` bitmapset, `statext_dependencies_load` per matching stat, attnum remapping, `find_strongest_dependency` loop with `bms_del_member`, and `clauselist_apply_dependencies`. Only C caller is the unported `statext_clauselist_selectivity` (extended_stats.c) dispatcher, itself a seam consumed by path-small — so there is NO ported in-repo consumer. Lands with `backend-statistics-core`, reusing the in-crate `find_strongest_dependency`, `dependency_is_fully_matched`, `combine_dependency_selectivities`, `statext_dependencies_deserialize`. |

## Summary

- Ported fully in-crate: 14 functions (generator x5, build control flow,
  serialize, deserialize, fully_matched, find_strongest, the apply
  combination-kernel, in/out/recv/send).
- Seamed to unported owner: `dependency_degree` (1 function).
- Deferred to the owner (planner-arena / syscache, no in-repo caller):
  `statext_dependencies_load`, `dependency_is_compatible_clause`,
  `dependency_is_compatible_expression`, `dependencies_clauselist_selectivity`
  (4 functions).

No `todo!()`/`unimplemented!()`. The single cross-crate gap
(`dependency_degree`) is a `seam!()` call into `backend-statistics-core-seams`
that panics loudly until the owner lands (mirror-pg-and-panic). The owner unit
is `in-progress`/`todo` in CATALOG.tsv, so the
`every_declared_seam_is_installed_by_its_owner` guard exempts the uninstalled
decl.

## Divergences found & resolved during the port

- `%f` formatting: C `printf("%f")` is fixed 6 fractional digits with no
  exponent; matched with `format!("{:.6}")` (verified by the `pg_dependencies_out`
  unit test producing `0.500000` / `1.000000`).
- Magic/type error messages: C prints the `uint32` operands via `%d`
  (dependencies.c:528/532), i.e. signed reinterpretation; matched via `as i32`.
- Allocation-safety: the deserializer bounds the data-derived `ndeps` before any
  reservation (HARD RULE) — beyond the C, which relies on `min_expected_size`
  alone; behavior-preserving (rejects only corrupt-yet-absurd `ndeps`).
