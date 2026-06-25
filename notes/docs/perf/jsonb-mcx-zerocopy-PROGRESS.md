# JsonbValue<'mcx> zero-copy migration — progress + scoped remainder

> **STATUS: Phase A COMPLETE — compiles end-to-end, regress-green, measured.**
> `cargo build --workspace` and `--bin postgres` link with 0 errors. The jsonb-family
> regress files (json, jsonb, json_encoding, jsonpath, jsonpath_encoding,
> jsonb_jsonpath, sqljson, sqljson_queryfuncs, sqljson_jsontable) are **0 difflines**
> on a RELEASE build, and `/private/tmp/recq.sql` returns the correct
> `[1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]`. The `<'static>` bridge aliases were
> removed (0 consumers remain). Companion to `jsonb-mcx-zerocopy-plan.md`.
>
> ## Measured payoff (recq.sql, profiling build, /usr/bin/sample 15s on the hot backend)
>
> | metric | before (per task brief) | after |
> |---|---|---|
> | system libmalloc self-% | ~60% | **~10.3%** |
> | recq wall vs C PG18.3 | ~32x | **~14x** (142ms C vs 1991ms; ~2.3x faster) |
>
> Remaining hot self-time (Phase-B candidates, NOT regressions): `JsonbValueData`
> deep-`clone` 21.9%, `Datum::clone` 14.3%, arena bump-alloc (`slice_in`/AllocSet)
> 22.4% (the intentional cheap allocation that *replaced* malloc), `jsonb_util`
> serialize/iterate 12.8%. The ~22% `JsonbValueData::clone` is a remaining working-tree
> deep-copy that a future Phase B could turn into a move/borrow.

## Done (compiles clean: `cargo build -p <crate>` = 0 errors)

- **`mcx`**: added `slice_borrow_in(mcx, src) -> &'mcx [u8]` (construction-side
  intern: copy fresh bytes into the arena, return a borrow tied to `'mcx` via
  `leak_in` of a one-shot arena box) and `McxOwned::with_mut_mcx(|mcx, &mut state|)`
  (mutable re-entry that also hands the owning context's `Mcx`, for the aggregate
  splice path).
- **`types_jsonb`** (Phase 0): `JsonbValue<'mcx>`, `JsonbValueData<'mcx>`
  (leaf runs `String/Numeric/Binary.data` are `&'mcx [u8]`; `Array/Object` spines
  `PgVec<'mcx,_>`), `JsonbPair<'mcx>`, `JsonbParseState<'mcx>`,
  `JsonbIterator<'mcx>` (now carries `mcx: Mcx<'mcx>` for the count-placeholder
  spines + `buf: &'mcx [u8]` instead of `Rc<[u8]>`). `<'static>` bridge aliases
  added (`JsonbValueStatic`, …). NO build.rs enum-parser exists here (the nodes-
  crate risk did not apply). `mcx` added as a dep.
- **`jsonb_util`** (Phase 1 + the build-stack of Phase 2): read path
  (`fillJsonbValue`, `JsonbToJsonbValue`, the iterator, `findJsonbValueFromContainer`,
  `getKeyJsonValueFromContainer`, `getIthJsonbValueFromContainer`,
  `compareJsonbContainers`, `JsonbDeepContains`, `binary_container`) is now
  ZERO-COPY (sub-slices the source, no `slice_to_vec`); `slice_to_vec`/`rc_from_slice`
  (Option-E `Rc<[u8]>`) DELETED. Construction (`pushJsonbValue` family,
  `appendKey/Value/Element`, `close_frame`, placeholders, `convertToJsonb`) is
  arena-allocated; `pushJsonbValue`/`compareJsonbContainers`/`JsonbDeepContains`/
  `JsonbIteratorInit` now take `mcx`.
- **`jsonb_op`**: all ops (`jsonb_contains/contained/cmp/eq/.../hash`) thread `mcx`
  (read-only — container args coerce `&'fcinfo → &'mcx`; the scratch arena only
  backs iterator placeholders). `jbv_string` borrows the search-key bytes.
  `mcx` moved dev-dep → dep.
- **`adt_jsonb`** (the largest consumer, incl. fmgr_builtins + agg_fmgr):
  - Parse callbacks (`jsonb_in_*`), `JsonbInState<'mcx>`, build path
    (`datum_to_jsonb_internal`, `composite_to_jsonb`, `array_*_to_jsonb`,
    `jsonb_object[_two_arg]`, `splice_jsonb_tokens`), casts (`jsonb_int*`,
    `jsonb_numeric`, `JsonbUnquote`, `cast_extract`), `JsonbExtractScalar`,
    `JsonbTypeName`/`JsonbContainerTypeName`/`jsonb_typeof` — all thread `mcx`,
    fresh bytes interned via `slice_borrow_in`.
  - **Aggregate state** (the §2.1 cross-yield case): `JsonbAggState<'mcx>` now
    lives in `McxOwned<JsonbAggBind>` (`JsonbAggOwned`), the C aggregate context.
    `agg_fmgr` stores `Box<JsonbAggOwned>` in the internal Datum; each transfn
    re-enters via `with_mut_mcx` (arg extraction + splice happen in the state's
    OWN arena, so a spliced element outlives the call exactly as C copies it in);
    finalfn runs in the same arena and copies the varlena out to an owned `Vec`
    for the by-ref result lane. `first`-call detection passes `None` to drive the
    worker's init branch. This REPLACES the prior "state is global-owned `'static`"
    note in agg_fmgr.rs (no longer possible once `JsonbValue` lost ownership).
- **`types_jsonfuncs`**: on the `<'static>` bridge (`JsonbValueStatic as JsonbValue`)
  — it stores `Box<JsonbValue>` in a struct field and is on the `datum-b` map;
  the bridge keeps the two campaigns independently stageable.
- **`adt_jsonfuncs/src/setops.rs`** (the recq.sql `||`, `- int`, `set` operators):
  fully converted — helpers (`push`, `iter_next`, `jbv_string`, `jbv_null`,
  `jsonb_to_jsonb_value`, `value_to_jsonb`), `iterator_concat`, `set_path`,
  `set_path_object/array`, `push_path`, `copy_nested_container`,
  `push_null_elements`, `parse_jsonb_index_flags` all thread `mcx`/`'mcx`.

## Borrow-checker hotspots actually hit (vs the plan's predictions)

The plan predicted the §4 hotspots "dissolve under the shared arena" — TRUE for
the functional read→build flows (concat/delete/set were borrow-clean once
threaded). The friction the plan UNDER-weighted:

1. **The iterator must carry `mcx`.** The count-only `Array/Object` placeholder
   spines (a pgrust-specific carrier of `nElems`, never read) need an allocator.
   There is no "current Mcx" accessor (the codebase passes `Mcx` down explicitly),
   and read-only callers (`jsonb_hash`, `compareJsonbContainers`) had no `mcx`.
   Resolution: `JsonbIterator<'mcx>` carries `mcx`; `JsonbIteratorInit` gains an
   `mcx` param; read-only fmgr wrappers make a `scratch_mcx()`. (~78 init + ~66
   next call sites threaded.)
2. **Local `PgVec` vs `&'mcx`** is the recurring papercut: a freshly serialized
   varlena (`JsonbValueToJsonb` result, a detoasted image) is a *local* `PgVec`,
   so `&local[..]` is `&'local`, not `&'mcx` — even though its bytes ARE in the
   arena. Whenever such bytes flow into the persistent tree (splice, JSONTYPE_JSONB,
   agg elements) they must be re-homed with `slice_borrow_in(mcx, &local)`. This
   is one extra arena copy at those boundaries (acceptable; not on the pure read
   path).
3. **The aggregate state genuinely needs `McxOwned`** (as the plan flagged in
   §2.1). `agg_fmgr` boxing `JsonbAggState<'mcx>` directly is impossible — the
   per-call scratch context would die while the boxed state still borrows it.
   `McxOwned<JsonbAggBind>` + `with_mut_mcx` was the bounded solution; it required
   adding `with_mut_mcx` to `mcx::owned`.

## Scoped remainder (the migration is ~60% landed; these crates still need the
## same mechanical `mcx`/`'mcx` threading + `slice_borrow_in` interning)

Per-crate residual (after its deps compile; cascades make the raw counts larger):

- **`adt_jsonfuncs`** — DOWN TO ~21 residual errors (from 78). `setops.rs` (the
  recq `||`/`- int`/`set`/`delete`/`insert` operators) is DONE; the residual is the
  read/SRF tail: `each.rs`, `getfield.rs`, `iterate.rs`, `keys.rs`, `populate.rs`,
  `recordset.rs`, `strip.rs` (`jsonb_each`, `jsonb_array_elements`,
  `jsonb_populate_record`, `jsonb_to_record(set)`, `jsonb_strip_nulls`). Remaining
  per-site work, no longer pure batch: a handful of construction sites build a
  `JsonbValue` over a *local* `Vec`/`PgVec` (`String(out.to_vec())`,
  `Binary { data: root.to_vec() }`, `s.clone()` in match arms) — each needs the
  source interned with `slice_borrow_in(mcx, …)` or the holding value's lifetime
  threaded; plus a couple `pushJsonbValue` arg-order/`mcx` fixes in `iterate.rs`,
  and `fmgr_builtins.rs` `jsonb_typeof`/`parse_jsonb_index_flags` callers need a
  `scratch_mcx()`. **GOTCHA**: do NOT script-promote `jb: &[u8] → &'mcx [u8]` on the
  small header-only helpers (`root_header`/`jb_root_count`/`jb_root_is_*`/
  `varsize_jsonb`) — they have no `<'mcx>` and only read the first word; leave them
  `&[u8]`. (A bulk regex did this and had to be reverted.)
  **NON-mechanical sub-blocker in `populate.rs`** (`jsonb_populate_record` /
  `jsonb_to_record`): it builds a `types_jsonfuncs::JsValue::Jsonb(Box<JsonbValue>)`
  tree. `JsValue` is the `datum-b`-overlapping `types_jsonfuncs` crate, currently
  on the `<'static>` bridge (`JsonbValueStatic`). Storing an arena-`'mcx` value
  into it is the §3.4 store-boundary crossing — it needs EITHER `types_jsonfuncs`
  migrated to `JsValue<'mcx>` (coordinate with datum-b) OR a re-home of the value
  into the JsValue's longer-lived context. This is the one spot in adt_jsonfuncs
  that is NOT a mechanical thread; ~6 of the ~14 residual errors trace to it
  (populate.rs:715, 1362, and the `JsValue::Jsonb` consumers). The other ~8 are
  mechanical (getfield `container: Vec<u8>` local, setops entry `jb`/`newjsonb`
  `&'mcx`, strip entry `jb` `&'mcx`).
- **`jsonpath_exec`** (~16): `->`/`@?`/`jsonb_path_*`/`.keyvalue()`/JSON_TABLE.
  Threads `mcx` through the executor; `JsonbExtractScalar`/`JsonbTypeName` callers
  (already converted in adt_jsonb) now take `mcx`. The `offset` doc-position field
  is unchanged (kept per plan §1.2).
- **`jsonb_gin`** (~2): `scalar_from_path_item` returns `JsonbValue<'_>`; a couple
  of iterator inits.
- **`jsonbsubs`** (~1 real): subscript assign/fetch.
- **`common/jsonapi`** (~7 real): the `JsonbSink`/`run_parse_to_jsonb` already carry
  `mcx`; just flip `JsonbInState` → `<'mcx>` and the parse-callback calls now pass
  `mcx` (done on the adt_jsonb side).
- **`execExprInterp/eval_json_xml.rs`**: imports `JsonbValue`/`JsonbValueData`;
  thread `'mcx` or use the `…Static` bridge if the value is stored.
- **Test modules** across `jsonb_util`, `jsonb_op`, `adt_jsonb`, `jsonb_gin`,
  `jsonpath_exec`: construct `JsonbValue` literally (`String(b"…".to_vec())`,
  `JsonbIteratorInit(bytes)`) — update to the arena forms.
- **Phase FINAL**: once all consumers name `<'mcx>`, drop the `…Static` aliases
  (only `types_jsonfuncs` uses one today; it can stay on the bridge if `datum-b`
  hasn't landed).

## Gate not yet runnable

Because the tree doesn't link yet, the per-phase regress gate (jsonb/json/
jsonpath/sqljson at baseline difflines + recq.sql correct) and the recq malloc-%
re-profile are PENDING the remainder. Baselines captured before starting:
`json=12, jsonb=468, jsonpath=0, jsonb_jsonpath=0, sqljson=0, sqljson_queryfuncs=0`
difflines (RELEASE-equiv, with the dev `max_stack_depth='100kB'` line neutralized),
and recq.sql = `[1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]`.
