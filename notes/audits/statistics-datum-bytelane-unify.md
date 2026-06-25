# Audit: statistics value lane unified onto canonical byte-lane `Datum<'mcx>`

Keystone + contract-divergence fix. No new C logic is ported here; the change
re-types the extended-statistics value carriers from the contract-divergent
bare-word `types_datum::Datum(usize)` onto the canonical `'mcx` byte-lane enum
`types_tuple::backend_access_common_heaptuple::Datum<'mcx>` (`ByVal(usize)` /
`ByRef(PgVec<'mcx, u8>)`), re-signs the value-touching seams over the new
carrier, and adapts the one consumer (`backend-statistics-mcv`) whose code moves
the (now non-`Copy`) value type.

## Rationale (datum-vs-tuplevalue model)

PG's `Datum` is a tagless machine word; a by-reference `Datum` is literally a
pointer into live storage. This repo's bare-word `types_datum::Datum(usize)`
carries only the word — the referenced bytes do NOT live in it. Storing a
by-ref statistic value (text / numeric / varchar — i.e. most real columns) as a
`Datum(usize)` word into a temporary ANALYZE context is a silent dangling-pointer
corruption: when the context is reset the word points at freed memory. The safe
`'mcx` byte-lane enum carries the verbatim detoasted bytes in its `ByRef` arm, so
by-ref values round-trip correctly. This is the statistics slice of the
Datum-redesign Option-A plan (`by-ref values live in the byte lane, not a bare
usize`).

## Crates changed

### `crates/types-statistics` (carrier)
- import `types_tuple::Datum` (the byte-lane enum) instead of `types_datum::Datum`.
- `MCVItem`, `MCVList`, `SortItem` gain the `'mcx` lifetime.
- re-typed value fields to `Datum<'mcx>`:
  - `MCVItem.values: Vec<Datum<'mcx>>`
  - `SortItem.values: Vec<Datum<'mcx>>`
  - `VacAttrStats.stavalues: [Vec<Datum<'mcx>>; STATISTIC_NUM_SLOTS]`
  - `VacAttrStats.exprvals: Vec<Datum<'mcx>>`
  - `StatsBuildData.values: Vec<Vec<Datum<'mcx>>>`
  - `AnalyzeAttrFetchFunc` return type → `Datum<'mcx>`
- length fields (`numvalues` / `numnumbers`), `stanumbers` (`Vec<f32>`), and all
  other fields UNCHANGED. `MVDependency`/`MVNDistinct*` carry no `Datum` and are
  untouched.

### `crates/backend-statistics-core-seams` (seam decls; owner still `todo`)
Added `types-tuple` dep. Re-signed the value-touching seams:
- `statext_mcv_build` → returns `Option<MCVList<'mcx>>`
- `mcv_compare_scalars_simple<'mcx>(a: &Datum<'mcx>, b: &Datum<'mcx>, ...)`
  (the enum is non-`Copy`, so the seam borrows rather than forcing a clone)
- `mcv_value_to_serialized_bytes<'mcx>(..., value: &Datum<'mcx>, ...)`
- `mcv_serialized_bytes_to_value<'mcx>(...) -> Datum<'mcx>`
- `mcv_get_match_bitmap<'mcx>(..., mcvlist: &MCVList<'mcx>, ...)`
- the fmgr-return seams `pg_stats_ext_mcvlist_items` / `pg_mcv_list_out` keep
  bare-word `types_datum::Datum` — the irreducible `PGFunction`-return ABI edge
  (per the model, the two sanctioned bare-word survivors).
These seams are uninstalled (owner `backend-statistics-core` is `todo`); the
`every_declared_seam_is_installed_by_its_owner` guard exempts the crate.

### `crates/backend-statistics-mcv` (consumer; serialize/deserialize)
Added `types-tuple` dep; value type is now `types_tuple::Datum`.
- `statext_mcv_serialize` / `_deserialize` / `statext_mcv_build` / `_load`
  re-signed to `MCVList<'mcx>` tied to the `mcx` arena.
- the byte-layout logic is UNCHANGED. The only adaptations for the non-`Copy`
  enum, all behaviour-preserving:
  - `coll.push(item.values[dim].clone())` (C copied the word; we clone the enum)
  - `coll[ndistinct] = coll[i].clone()` (dedup compaction; C overwrote the word)
  - `values[d] = map[d][index].clone()` (index→value translation)
  - pass `&Datum` to the compare/codec seams (`&coll[i]`, `&base[mid]`,
    `&mcvitem.values[d]`, closure `|a, b|` instead of `*a, *b`)
  - `bsearch_index(value: &Datum, base: &[Datum], ...)`
- the two fmgr-return wrappers return `types_datum::Datum` (qualified).

### `backend-statistics-mvdistinct` / `-dependencies`
No code change needed — they reference the carriers only via `StatsBuildData<'mcx>`
(no direct value indexing). They recompile clean against the re-typed carrier.

## Faithfulness

- Serialize byte layout EXACTLY preserved: `STATS_MCV_MAGIC` / `STATS_MCV_TYPE_BASIC`
  / `SIZEOF_DIMENSION_INFO` / `ITEM_SIZE` / all header field offsets unchanged.
  The `constants_match_c` test still passes; the round-trip tests still pass.
- No stubs / `todo!` / `unimplemented!`. The `clone()`s mirror C's word copies
  exactly (a `ByVal` clone is a word copy; a `ByRef` clone is the `datumCopy` the
  C deserializer performs into the MCV list's single chunk).

## Gate

- `cargo check --workspace` — clean (pre-existing warnings only, no errors).
- `cargo test -p backend-statistics-mcv` — 14 passed.
- `cargo test -p backend-statistics-mvdistinct` — 15 passed.
- `cargo test -p backend-statistics-dependencies` — 16 passed.
- `cargo test -p no-todo-guard` — pass.
- `cargo test -p seams-init` — pass.
- The contract-reconcile ledger is unchanged (re-typing + re-signing
  uninstalled/owner-pending seams retires/adds no allowlist entry).

## Effect

`commands/analyze.c` is now re-fireable: its value flow (`datum_copy` /
`apply_sort_comparator` / `DeformedColumn`, all `Datum<'mcx>`-typed) is
type-compatible with the statistics carriers. The prerequisite keystone in
`analyze-c-blocked-on-bareword-datum-carrier.md` is satisfied.
