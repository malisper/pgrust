# Audit: backend-utils-sort-sortsupport

- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) (claude-opus-4-8[1m])
- **Branch:** port/backend-utils-sort-sortsupport
- **Unit C sources:** `src/backend/utils/sort/sortsupport.c`
- **Port crate:** `crates/backend-utils-sort-sortsupport`
- **Owned seam crate:** `crates/backend-utils-sort-sortsupport-seams` (maps to sortsupport.c)

Independent re-derivation from the C source, the c2rust rendering
(`c2rust-runs/backend-utils-sort-small/src/sortsupport.rs`), and the Rust port.
The catalog unit `backend-utils-sort-small` combines `qsort_interruptible.c` +
`sortsupport.c`; this port unit covers `sortsupport.c` only (qsort_interruptible
is split out to its own catalog row 574) — confirmed against the C file list.

## 1. Function inventory

`sortsupport.c` defines exactly 6 functions (1 type `SortShimExtra`, 1 macro
`SizeForSortShimExtra`, no other helpers). c2rust confirms the same 6 (qsort
functions belong to qsort_interruptible.rs, out of scope).

| # | C function | C loc | Port loc | Verdict |
|---|-----------|-------|----------|---------|
| 1 | `comparison_shim` | sortsupport.c:42 | lib.rs:95 `comparison_shim` | MATCH |
| 2 | `PrepareSortSupportComparisonShim` | sortsupport.c:67 | lib.rs:127 | MATCH |
| 3 | `FinishSortSupportFunction` | sortsupport.c:93 | lib.rs:147 | MATCH |
| 4 | `PrepareSortSupportFromOrderingOp` | sortsupport.c:133 | lib.rs:206 | MATCH |
| 5 | `PrepareSortSupportFromIndexRel` | sortsupport.c:160 | lib.rs:238 | MATCH |
| 6 | `PrepareSortSupportFromGistIndexRel` | sortsupport.c:184 | lib.rs:269 | MATCH |

Supporting (non-C-function) port items, all justified:
- `oid_function_call1_sortsupport` (lib.rs:189) — the inlined
  `OidFunctionCall1(sortfunc, PointerGetDatum(ssup))` used by functions 3 and 6;
  installed as the `oid_function_call_1_sortsupport` inward seam so the
  token-model `SortSupportData` is reachable. Thin: one `oid_function_call1_coll`.
- `apply_sort_comparator` (lib.rs:307) — the `ApplySortComparator` non-null
  dispatch (sortsupport.h inline). It interprets the `SortComparatorId` token,
  which only this owner can do, so it is legitimately part of this unit's
  surface; installed as the `apply_sort_comparator` inward seam.

## 2. Per-function detail

**comparison_shim (MATCH).** C sets `fcinfo.args[0/1].value = x/y`, resets
`isnull=false`, `FunctionCallInvoke`, then `elog(ERROR, "function %u returned
NULL")` on null result, returns `result` as int. Port snapshots the resolved
`(resolution, finfo, collation)` from the token registry (releasing the borrow
to stay re-entrancy-safe), then `function_call2_coll(mcx, &resolution, finfo,
collation, x, y)?` and `.as_i32()`. `function_call2_coll` builds an fcinfo with
`args[].isnull=false` and applies the shared `null_check`, which raises
`"function {fn_oid} returned NULL"` (fmgr-core lib.rs:316) — same SQLSTATE
(default internal) and same fn_oid-named message as C line 58. Behaviorally
identical to the inlined reuse fcinfo.

**PrepareSortSupportComparisonShim (MATCH).** C `MemoryContextAlloc`s
`SortShimExtra` in `ssup_cxt`, `fmgr_info_cxt(cmpFunc, &flinfo, ssup_cxt)`,
`InitFunctionCallInfoData(.., 2, ssup_collation, ..)`, sets `ssup_extra` and
`comparator=comparison_shim`. Port: `fmgr_info_cxt(ssup.ssup_cxt, cmpFunc)?`,
registers `ShimState{resolved, collation: ssup.ssup_collation}` → token, sets
`ssup.comparator = Some(id)`. The token + per-backend registry is the owned-model
analog of `ssup_extra`-stored `SortShimExtra` (see §3b).

**FinishSortSupportFunction (MATCH).** `get_opfamily_proc(opfamily, opcintype,
opcintype, BTSORTSUPPORT_PROC)`; if valid call it (`OidFunctionCall1`); if
`comparator==NULL` afterward, `get_opfamily_proc(.., BTORDER_PROC)`, error
"missing support function %d(%u,%u) in opfamily %u" when invalid, else
`PrepareSortSupportComparisonShim`. Port mirrors branch-for-branch, with
`ssup.comparator.is_none()` for the NULL test and the same error string
(arg order opcintype,opcintype,opfamily preserved).

**PrepareSortSupportFromOrderingOp (MATCH).** `Assert(comparator==NULL)` →
`debug_assert!`. `get_ordering_op_properties` bool+3-out-params maps to the
seam's `Option<(opfamily, opcintype, cmptype)>`; `None` → error "operator %u is
not a valid ordering operator". `ssup_reverse = (cmptype == COMPARE_GT)`
(COMPARE_GT=5 verified). Tail-calls `FinishSortSupportFunction`.

**PrepareSortSupportFromIndexRel (MATCH).** `rd_opfamily[attno-1]` /
`rd_opcintype[attno-1]` delegated to relcache seams `rd_opfamily/rd_opcintype`
(the `-1` array index is `Relation`-field access, owned by relcache — correct
ownership, not seam computation). `!rd_indam->amcanorder` → error
"unexpected non-amcanorder AM: %u" with `rd_rel->relam`. `ssup_reverse=reverse`,
then `FinishSortSupportFunction`. Note: C reads opfamily/opcintype before the
Assert; port reads them then `debug_assert!` — no observable difference.

**PrepareSortSupportFromGistIndexRel (MATCH).** Same field reads;
`relam != GIST_AM_OID` (783, verified) → error "unexpected non-gist AM: %u";
`ssup_reverse=false`; `get_opfamily_proc(.., GIST_SORTSUPPORT_PROC)` (11,
verified), invalid → "missing support function %d(%u,%u) in opfamily %u";
else `OidFunctionCall1`. All present.

## 3. Constants (verified against C headers, not memory)

| Const | Port value | Header | C value |
|-------|-----------|--------|---------|
| BTORDER_PROC | 1 | access/nbtree.h:717 | 1 |
| BTSORTSUPPORT_PROC | 2 | access/nbtree.h:718 | 2 |
| GIST_SORTSUPPORT_PROC | 11 | access/gist.h:42 | 11 |
| COMPARE_EQ | 3 | access/cmptype.h:36 | 3 |
| COMPARE_GT | 5 | access/cmptype.h:38 | 5 |
| GIST_AM_OID | 783 | catalog/pg_am.dat:24 | 783 |

All match.

## 4. Seam audit

**Owned seam crate:** `backend-utils-sort-sortsupport-seams`. It declares 4
inward slots:
`oid_function_call_1_sortsupport`, `prepare_sort_support_comparison_shim`,
`apply_sort_comparator`, `prepare_sort_support_from_ordering_op`.
`init_seams()` (lib.rs:319) installs **all 4** with nothing but `set()` calls,
and `seams-init::init_all` calls it (seams-init/src/lib.rs:75). No uninstalled
slot; no `set()` outside the owner. No empty-installer FAIL.

**Outward seams** (justified by real dependency cycles, thin marshal+delegate):
- `lsyscache::get_ordering_op_properties` / `get_opfamily_proc` — pure catalog
  lookups owned by lsyscache; no branching/computation on the seam path.
- `relcache::rd_opfamily` / `rd_opcintype` / `rd_indam_amcanorder` /
  `rd_rel_relam` — `Relation`-field accessors owned by relcache (the `attno-1`
  index is the field-access logic, properly on relcache's side).

No function body was replaced by a "delegate elsewhere" call — every C
function's logic lives in this crate; only genuine cross-owner field/catalog
access and the not-yet-ported type-sortsupport builtin invocation cross seams.

`OidFunctionCall1(BTSORTSUPPORT/GIST_SORTSUPPORT_PROC)` is routed through
fmgr-core `oid_function_call1_coll` with a null `Datum` (the owned `Datum`
carries no `ssup` pointer, and the per-type sortsupport builtins are unported):
this is mirror-and-panic on an unported **callee**, which the skill explicitly
permits — the dispatching logic is present and faithful.

## 3b. Design conformance

- **Opacity inherited, not introduced (types.md rule 6).** The
  `SortComparatorId` comparator token + trimmed `SortSupportData` (dropped
  `ssup_extra`/abbrev hooks) were introduced by the earlier merged
  nodeMergejoin unit and are explicitly sanctioned in `docs/types.md` (the
  types-sortsupport row: "the opaque `SortComparatorId` comparator token the
  comparator owner installs"). This port does not invent any new handle or
  stand-in; it consumes the inherited vocabulary and resolves real types
  (`Relation`, `ResolvedFmgrInfo`) elsewhere.
- **thread_local, not shared static (AGENTS.md "Backend-global state").** The
  `SHIMS` registry — the owned-model storage for the C `SortShimExtra` that C
  kept in `ssup_cxt`-allocated memory reached via `ssup_extra` — is
  `thread_local!`, the sanctioned per-backend representation, not a shared
  `static`/`Atomic`. Because the inherited type intentionally dropped
  `ssup_extra`, the owner must hold the resolved fmgr lookup in side storage
  keyed by the token it hands out; this is the direct and required consequence
  of the inherited token model, not a registry-shaped side table standing in
  for absent logic. It is per-backend, single-keyed by the token, and read only
  by this unit's own comparator dispatch. (Lifetime note: the registry persists
  for the backend rather than freeing on a `ssup_cxt` reset; this is the known
  consequence of the token model and does not alter any observable comparison
  result — recorded, non-blocking.)
- **Mcx + PgResult on allocating/erroring functions.** Every public entry takes
  the `SortSupportData<'mcx>` (carrying `ssup_cxt: Mcx`) and returns
  `PgResult<()>` / `PgResult<i32>`; `fmgr_info_cxt`/`function_call2_coll`
  receive `Mcx`. No ambient current context; no zero-arg ambient-global getter
  seams; no locks held across `?`.
- **C enums/consts as verified newtype/consts (rule 7).** Constants live in
  types-sortsupport with header-cited values (verified above); no bare
  integer-alias positional-constant tables.

No §3b findings.

## 5. Build

`cargo build -p backend-utils-sort-sortsupport` compiles clean.

## Verdict

All 6 functions **MATCH**; the two supporting items are faithful and
seam-installed. Zero seam findings, zero §3b findings, constants verified
against headers. **PASS.**
