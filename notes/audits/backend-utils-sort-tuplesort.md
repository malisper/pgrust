# Audit: backend-utils-sort-tuplesort (F3a — heap + datum variants)

Scope of this audit pass: the F3a additions to the already-landed F0/F1/F2 engine
(`tuplesort.c` core + external-merge tape engine). Audited vs
`../pgrust/postgres-18.3/src/backend/utils/sort/{tuplesort.c,tuplesortvariants.c}`
and the c2rust run `c2rust-runs/backend-utils-sort-tuplesort` for byte detail.

F3a = the heap + datum sort variants + the engine seam install + the
heap/datum-reachable seams. Index variants (btree/hash/gist), CLUSTER, and the
parallel coordinate legs are F3b/sanctioned seam-panics and are NOT in scope.

## Begin entry points (tuplesortvariants.c)

- `tuplesort_begin_heap` (`tuplesort_begin_heap_state` + `seam_begin_heap`):
  faithful. `tuplesort_begin_common` → `nKeys`, `haveDatum1 = true`,
  `arg = tupDesc`, per-column `SortSupportData` built via
  `PrepareSortSupportFromOrderingOp`, `abbreviate = (i==0 && haveDatum1)`, and
  the `onlyKey` set iff `nkeys==1 && !abbrev_converter`. The C
  `MemoryContextSwitchTo(base->maincontext)` (build sortKeys/arg in the sort
  context) is realized by building everything INSIDE the `OwnedSort::try_new`
  `for<'mcx>` closure over the bundle's own `sx` arena. KEY COST resolved: the
  caller's `'mcx`-lifetimed `TupleDesc`/params are snapshotted to a lifetime-free
  `TupleDescSnapshot` (+ plain `Vec`s) before the universal closure, then the
  TupleDesc is rebuilt (a deep clone — C's `/* assume we need not copy tupDesc */`
  becomes an explicit copy, behaviour-preserving). `constr` is asserted `None`
  (a sort/plan result TupleDesc never carries catalog constraints, and the sort
  never reads them).
- `tuplesort_begin_datum` (`tuplesort_begin_datum_state` + `seam_begin_datum`):
  faithful. `nKeys = 1`, `arg = {datumType, datumTypeLen}`,
  `base.tuples = !typbyval` (via the `get_typlenbyval` seam, called outside the
  universal closure since it is lifetime-agnostic), single `SortSupportData`
  with `abbreviate = !typbyval`, `PrepareSortSupportFromOrderingOp`, `onlyKey`
  set iff `!abbrev_converter`. Matches C exactly.

## Comparators (tuplesortvariants.c)

- `comparetup_heap` (pre-existing F1): leading key via `ApplySortComparator`,
  then `comparetup_heap_tiebreak`. Verified.
- `comparetup_heap_tiebreak` (F3a fill): faithful. If `sortKey->abbrev_converter`
  run `ApplySortAbbrevFullComparator` on the leading column; then walk
  `nkey = 1..nKeys`, `heap_getattr(attno)` each, `ApplySortComparator`, returning
  on the first non-zero. The C `heap_getattr` over a `HeapTupleData` laid on the
  MinimalTuple is realized as a single `heap_deform_sort_minimal` per side
  (deform-once, index-by-attno), behaviour-equivalent (a comparison may need any
  subset of columns). Verified end-to-end by `heap_multikey_tiebreak`.
- `comparetup_datum` (pre-existing F1) + `comparetup_datum_tiebreak` (F3a fill):
  faithful. The tiebreak runs `ApplySortAbbrevFullComparator(PointerGetDatum(
  a->tuple), a->isnull1, ...)` only when `abbrev_converter` is set; else 0. The
  original value is read from `TupleBody::Datum` (the C separately-stored copy).
- `ApplySortComparator` (pre-existing) + `ApplySortAbbrevFullComparator` (F3a):
  both mirror the sortsupport.h inlines field-for-field (NULL collation by
  `ssup_nulls_first`, comparator, then `ssup_reverse` sign flip). Verified.

## removeabbrev (tuplesortvariants.c)

- `remove_abbrev_all` dispatches by variant; `removeabbrev_heap` re-extracts
  `datum1` from `sortKeys[0].ssup_attno` of each stored MinimalTuple (deform +
  index), `removeabbrev_datum` rewrites `datum1 = PointerGetDatum(tuple)`. Both
  match C. Index/cluster removeabbrev = F4 seam-panic.

## Byte codecs (tuplesortvariants.c, pre-existing F2, re-verified)

- `writetup_heap`/`readtup_heap`: `u32 len` framing + the MinimalTuple body from
  `MINIMAL_TUPLE_DATA_OFFSET` (10) onward; trailing length word under
  `TUPLESORT_RANDOMACCESS`. `readtup_heap` reconstructs the flat blob, deforms
  for `datum1` at `ssup_attno`. Verified.
- `writetup_datum`/`readtup_datum`: NULL (len 0) / bare-word (`!tuples`) /
  by-ref bytes (`tuples`) cases, RANDOMACCESS trailer. Verified.

## put / get impls

- `tuplesort_puttupleslot_impl`: forms a MinimalTuple over the slot's deformed
  `tts_values`/`tts_isnull` (the owned-model `ExecCopySlotMinimalTuple`),
  extracts `datum1` at `ssup_attno`, `use_abbrev = abbrev_converter && !isnull1`,
  delegates to `tuplesort_puttuple_common`. Matches C.
- `tuplesort_putdatum_impl`: NULL / by-value → `datum1`, `tuple = None`; non-null
  by-ref → `datumCopy` into the engine arena, `tuple = Datum(copy)`,
  `datum1 = copy` (C aliases one copy; we hold the same bytes in two owned copies
  — behaviour-preserving). `use_abbrev = tuples && abbrev_converter && !isNull`.
  Matches C.
- `tuplesort_gettupleslot_impl`: `tuplesort_gettuple_common`; on a Minimal body
  deform into the slot's value arrays + mark stored (owned virtual-slot
  `ExecStoreMinimalTuple`); else clear the slot + return false. `copy` is a
  no-op (the fetched tuple is already an owned copy out of the engine arena).
- `tuplesort_getdatum_impl`: NULL/`!tuples` → `datum1`; else the original from
  `TupleBody::Datum` (C uses `stup.tuple` since `datum1` may be abbreviated).
  Matches C.

## Seams installed (init_seams, wired into seams-init::init_all)

13 inward seams: `tuplesort_begin_heap`, `tuplesort_begin_datum`,
`tuplesort_set_bound`, `tuplesort_puttupleslot`, `tuplesort_putdatum`,
`tuplesort_performsort`, `tuplesort_gettupleslot`, `tuplesort_getdatum`,
`tuplesort_get_stats`, `tuplesort_end`, `tuplesort_rescan`,
`tuplesort_markpos`, `tuplesort_restorepos`.

- `seam_get_stats`: the seam contract is `&Tuplesortstate`; the C
  `tuplesort_get_stats` mutates only via `tuplesort_updatemax` (persists the
  running max-space). The read-only core `tuplesort_get_stats_ref` computes the
  same value `updatemax` would settle on without persisting — observably
  identical for the stats report (the persisted fields exist only to compare
  against FUTURE updatemax calls). No `&`→`&mut` UB cast.
- `seam_end`: runs `tuplesort_free` (closes tape files via `LogicalTapeSetClose`)
  through the carrier, then drops the carrier (drops the engine bundle + its
  context = `MemoryContextDelete(maincontext)`).
- The put/get/datum seams re-tie the caller-`'mcx` slot/value borrows into the
  engine's universal `'mcx` via a `transmute` inside `with_sort_mut`; sound
  because the value is immediately cloned (datumCopy) into the engine arena (put)
  or freshly allocated there and written into the longer-lived slot (get) — no
  borrow escapes. Mirrors C's `void *` aliasing.

## NOT in scope (F3b / sanctioned seam-panic)

- Index variants: `begin_index_btree`/`begin_index_hash`/`begin_index_gist`,
  `putindextuplevalues`, `getindextuple`. The engine `comparetup_index_*` /
  `writetup_index` / `readtup_index` / `removeabbrev_index` arms loud-panic;
  the four CALLED begin/put/get index seams (btree+hash+put+get; gist has no
  live caller) are uninstalled and tracked in `CONTRACT_RECONCILE_PENDING`
  (`backend_utils_sort_tuplesort` × 4) — to be removed when F3b lands.
- CLUSTER variant + parallel coordinate legs (`worker_*`, `leader_takeover_tapes`,
  shared-sort estimate/init/attach): sanctioned 1:1 seam-panic. No
  `todo!`/`unimplemented!` anywhere.

## Tests

15 owner tests pass, incl. F3a `begin_datum_end_to_end` (real
begin_datum_state + putdatum/getdatum through the carrier) and
`heap_multikey_tiebreak` (2-column heap sort exercising
`comparetup_heap_tiebreak`'s deform path end-to-end). Pre-existing F1/F2 tests
(qsort, bounded top-N, external spill, stats) still green.
