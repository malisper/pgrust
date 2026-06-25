# Audit: backend-executor-execExprInterp

- **Unit:** backend-executor-execExprInterp (the expression-evaluation *interpreter*)
- **C source:** `src/backend/executor/execExprInterp.c` (PostgreSQL 18.3)
- **c2rust reference:** `c2rust-runs/backend-executor-execExprInterp/src/execExprInterp.rs`
- **Branch audited:** `assemble/backend-executor-execExprInterp`
  (scaffold `scaffold/execexprinterp-fill` + the nine `decomp/execexprinterp-*`
  family sub-branches merged: core-dispatch, interp-loop, justs, eval-scalar,
  eval-composite, eval-array, eval-json-xml, eval-misc, eval-agg).
- **Date:** 2026-06-13
- **Model:** Claude Opus 4.8 (1M context, exact id `claude-opus-4-8[1m]`)
- **Verdict: PASS** (independently re-derived from the C + the owned model)

> **2026-06-13 re-audit addendum (`fix/saophash`):** the hashed-SAOP family
> (`ExecEvalHashedScalarArrayOp`, `saop_element_hash`, `saop_hash_element_match`,
> and the `saophash_*` simplehash instantiation) is now implemented with real
> bodies + real seam `::call`s; the prior pure-`panic!` stubs (which an earlier
> pass had treated as SEAMED) were correctly **MISSING** by this skill's rule
> ("a bare `panic!` standing in for own logic is MISSING, not SEAMED"). The
> re-derived verdicts are recorded in the
> [Hashed-SAOP family](#hashed-saop-family-re-audited-fixsaophash) section
> below; the overall crate verdict remains **PASS**.

This supersedes the prior **FAIL** audit on `assemble/expr-eval-keystone`, which
correctly flagged that only the `ExecJust*` fast-path family existed and the
keystone-core `ExecInterpExpr` plus all eight per-opcode eval families were
absent (MISSING). Those families are now present and fully assembled; the audit
below is re-derived from scratch against the current crate.

## Function inventory and verdicts

Every function defined in `execExprInterp.c` (81 definitions: the named
`Exec*`/`Check*`/`get_cached_rowtype`/`saop_*` functions; the `saophash_*`
simplehash family is macro-generated and lives in the SAOP hash path) has a
counterpart by exact name in the port (`grep` of C definitions vs port `fn`
names: zero C functions missing). One port-only helper, `dispatch_compare_ptr`,
backs the owned `bsearch`-shaped step dispatch and has no separate C function.

Verdict distribution after per-function comparison of C / c2rust / port:

- **MATCH** — control flow, branch predicates, loop bounds, null-default and
  first-nonnull adoption, early returns, and constant/flag handling reproduced
  exactly. Examples re-derived in detail:
  - `ExecJustConst` — `*isnull = constval.isnull; return constval.value` exact.
  - `ExecEvalRowNullInt` — `*resnull=false`; the `isnull` fast-path returns
    `BoolGetDatum(checkisnull)`; the per-field scan (dropped-column skip,
    `heap_attisnull`, IS NULL / IS NOT NULL disproof, vacuous all/none result)
    is transcribed; the composite-Datum decode is the only seam.
  - `ExecEvalMinMax` — null-default, `nelems` loop, NULL-input skip, first
    nonnull adoption, and `IS_LEAST`/`IS_GREATEST` comparison branches present;
    only the `FunctionCallInvoke(fcinfo)` comparison is seamed.
  - `ExecReadyInterpretedExpr` / `ExecInterpExprStillValid` /
    `CheckExprStillValid` — fast-path evalfunc selection ladder, the
    `evalfunc_private` decode, and the still-valid first-call check match the C
    `state->evalfunc` routing.
- **SEAMED** — body reaches an unported owner across a justified seam (thin
  marshal + delegate), or mirror-PG-and-panics on data a neighbor owns that is
  not yet in the shared model. See the panic ledger below.
- No **MISSING**, **PARTIAL**, or **DIVERGES** found.

## Panic / seam ledger — all mirror-PG-and-panic on unported owners or model gaps

Residual `panic!`/`unreachable!` (no `todo!()`/`unimplemented!()` in code — the
only `todo!` tokens are inside `//!` doc comments). Per the audit rule
"panicking on an unported callee is fine, absent logic is not", every panic is
on a genuine unported *owner* or a not-yet-modeled neighbor struct field, and
the surrounding own-logic (loop structure, branch arms, null handling, result
stores) is present up to that boundary:

1. **execTuples TupleTableSlot payload (decomp #113, pending).** The shared
   `types_nodes::TupleTableSlot` is keystone-trimmed to the header bits
   (`tts_flags`/`tts_tid`/`tts_ops`/`tts_tableOid`); it has no
   `tts_values`/`tts_isnull`/`tts_nvalid`/`tts_tupleDescriptor`. So
   `EEOP_*_FETCHSOME`, `EEOP_*_VAR`, `EEOP_ASSIGN_*_VAR`, `EEOP_ASSIGN_TMP[_MAKE_RO]`,
   `CheckVarSlotCompatibility`, and the `ExecJust*` Var/Assign/Hash-var arms
   seam-and-panic on the absent slot arrays. This is the **same model-layer
   blocker the already-audited (PASS) `backend-executor-execExpr` documents**
   for `sortslot->tts_values/tts_isnull`.
2. **fmgr-widened `FunctionCallInfoBaseData` (fmgr lineage).** The trimmed
   `fcinfo` carries only `resultinfo`, no `args[]`/`isnull`. Handlers that
   `FunctionCallInvoke(fcinfo)` — `EEOP_FUNCEXPR*`, `EEOP_IOCOERCE`,
   `EEOP_DISTINCT/NOT_DISTINCT/NULLIF`, `EEOP_ROWCOMPARE_STEP`, `EEOP_HASHDATUM_*`,
   `ExecEvalMinMax`, the agg deserialize/strict-input checks — seam-and-panic on
   the call frame, after reproducing the strict-NULL scan structure.
3. **composite-Datum / heaptuple bridge.** `DatumGetHeapTupleHeader` +
   `heap_attisnull`/`heap_deform_tuple` over row Datums (`ExecEvalRowNull[Int]`,
   `ExecEvalFieldSelect`, `ExecEvalFieldStore*`, `ExecEvalConvertRowtype`,
   `ExecEvalWholeRowVar`) are owned by the toast/heaptuple layer not modeled for
   composite Datums.
4. **nodeAgg / nodeWindowAgg / nodeSubplan owner state.** `EEOP_AGG_*`
   (`all_pergroups`, `ExecAggInitGroup`/`ExecAggPlainTransBy{Val,Ref}`,
   presorted-distinct), `EEOP_WINDOW_FUNC` (`wfstate->wfuncno` indexing
   `ecxt_aggvalues/ecxt_aggnulls`), and `EEOP_SUBPLAN` payloads delegate to
   their owner units.
5. **Extension-supplied subroutines.** `EEOP_PARAM_CALLBACK`,
   `EEOP_SBSREF_*`/`EEOP_SBSREF_SUBSCRIPTS` dispatch a caller-installed
   `ExecEvalSubroutine`/subscript handler — an unported callee.
6. **JSON/XML constructors and coercions** (eval_json_xml) delegate to the
   varlena/json owner functions (`json_build_*`, `JsonPath*`, `xmlelement`,
   etc.).
7. **Defensive guards (= C `Assert`/`elog(ERROR)`/`pg_unreachable`).** The
   `unreachable!`/`panic!` on a wrong `ExprEvalStepData` payload variant or an
   unrecognized `evalfunc_private` mirror C's `EEO_CASE` invariants — they fire
   under the same impossible-state predicates, not on real input.

## EEOP_* emit/consume contract cross-check

Cross-checked the compiler's opcode surface (`backend-executor-execExpr`) against
the interpreter's handlers:

- `types_nodes::execexpr::ExprEvalOp` defines exactly **121** enumerators
  (matches the PG-18 `enum ExprEvalOp` count documented in that file).
- All **121** are referenced by the interpreter (dispatch + `ExecInterpExpr`
  step walk): `comm -23 enum interp` is empty — **no opcode emitted-but-not-handled**.
- Every opcode the compiler emits by name is handled. The only `EEOP_*` tokens
  the diff surfaces as "extra" are comment-text prefix fragments
  (`EEOP_AGG[_STRICT]_DESERIALIZE`, `EEOP_SBSREF_*`, `EEOP_BOOL_`) — not enum
  variants — so there is **no opcode handled-but-never-emitted**.

## Seam / wiring audit

Owned seam crate (by C-source coverage of `execExprInterp.c`):
`crates/backend-executor-execExprInterp-seams`, declaring two seams:

- `exec_ready_interpreted_expr` (`ExecReadyInterpretedExpr`) — `&mut ExprState`
  in both seam and owned entry; **installed** by the crate's `init_seams()`
  (`set(dispatch::ExecReadyInterpretedExpr)`).
- `exec_eval_expr_switch_context` (`ExecEvalExprSwitchContext` / the
  `ExecInterpExpr` dispatch) — **tracked contract divergence**: the seam declares
  `&ExprState` (the C macro reads `state->evalfunc`) but the owned
  `ExecInterpExprStillValid` needs `&mut ExprState` (still-valid check +
  `ExecJust*`/`ExecInterpExpr` per-eval scratch). Recorded in
  `seams-init::CONTRACT_RECONCILE_PENDING` + DESIGN_DEBT for the
  seam-contract-reconcile lane. The `recurrence_guard` "declared seam installed
  by owner" test treats this as live allowlisted debt, not a regression.

`init_seams()` is wired into `seams-init::init_all()`; both `recurrence_guard`
tests pass. No `set()` outside the owner; no uninstalled non-allowlisted seam.

## Hashed-SAOP family (re-audited, `fix/saophash`)

C: `execExprInterp.c:195-235` (the `ScalarArrayOpExprHashEntry` /
`ScalarArrayOpExprHashTable` structs + the `SH_PREFIX saophash`
`lib/simplehash.h` instantiation) and `:4176-4402` (the two callbacks +
`ExecEvalHashedScalarArrayOp`). c2rust reference: the ~14 generated `saophash_*`
functions + `saop_element_hash` / `saop_hash_element_match`.

Per-function verdicts after re-deriving from the C + c2rust:

| Function | Port location | Verdict | Notes |
|---|---|---|---|
| `saophash_create` | `saophash.rs::saophash_create` | MATCH | `size = compute_size(min(SH_MAX_SIZE, nelements/0.9))`, zeroed `data`, `update_parameters`. |
| `saophash_compute_size` | `saophash.rs::saophash_compute_size` | MATCH | `pg_nextpower2_64(max(newsize,2))` + the `>= UINT64_MAX/2` over-large guard. Unit-tested. |
| `saophash_update_parameters` | `SaophashOps::update_parameters` | MATCH | `sizemask = size-1`; `grow_threshold = floor(size*0.9)` (0.98 only at `SH_MAX_SIZE`). |
| `saophash_initial_bucket` / `next` / `prev` | `SaophashOps::{initial_bucket,next,prev}` | MATCH | masked `hash & sizemask`, `(elem ± 1) & sizemask` wraparound. |
| `saophash_distance` | `SaophashOps::distance` | MATCH | `optimal<=bucket ? bucket-optimal : size+bucket-optimal`. Unit-tested. |
| `saophash_entry_hash` / `SH_GET_HASH` | inlined (`entry.hash`) | MATCH | `SH_STORE_HASH` cached hash read inline at the probe sites. |
| `saophash_allocate` / `free` | `Vec` allocation in create/grow | MATCH | `MCXT_ALLOC_HUGE|ZERO` ↦ zeroed `Vec`; `pfree` ↦ `Vec` drop (`mem::take` in grow). |
| `saophash_grow` | `saophash.rs::saophash_grow` | MATCH | two-phase ordered copy: phase-1 `startelem` choice (empty, or entry at its optimal bucket), phase-2 circular linear-probe re-insert. Exercised by the multi-grow round-trip test. |
| `saophash_insert` / `_insert_hash_internal` | `saophash.rs::saophash_insert{,_hash_internal}` | MATCH | Robin-Hood steal on `insertdist > curdist`; anti-clustering forced grow on `insertdist>25` / `emptydist>150` gated by `members/size >= 0.1`; forward shift then place. Unit-tested. |
| `saophash_lookup` / `_lookup_hash_internal` | `saophash.rs::saophash_lookup` | MATCH | masked probe to first EMPTY; `NULL`-test folded into the `bool` return (the sole C caller only `!= NULL`-tests). |
| `saop_element_hash` | `eval_scalar.rs::saop_element_hash` | SEAMED | real body: `function_call1_coll::call(hashfuncid, inputcollid, key)` → `DatumGetUInt32`. The fmgr dispatch is a real `backend_utils_fmgr_fmgr_seams` `::call` (F0 contract: `FmgrInfo` carries `fn_oid`, the seam re-resolves by OID — same pattern as the `ExecJustHashVar*` paths). |
| `saop_hash_element_match` | `eval_scalar.rs::saop_hash_element_match` | SEAMED | real body: `function_call2_coll::call(matchfuncid, inputcollid, key1, key2)` → `DatumGetBool`. Both keys non-null (the table never stores NULLs), matching `FunctionCall2Coll`'s contract. |
| `ExecEvalHashedScalarArrayOp` | `eval_scalar.rs::ExecEvalHashedScalarArrayOp` | SEAMED | real own-logic: strict-NULL short circuit; build-on-first-eval over `array_get_elemtype` + `get_typlenbyvalalign` (lsyscache seam) + `deconstruct_array` (arrayfuncs seam, which subsumes the C `ARR_DATA_PTR`/`ARR_NULLBITMAP`/`fetch_att` bitmap walk) → `saophash_create`/`saophash_insert` over non-NULL elements, recording `has_nulls`; `saophash_lookup` probe; IN/NOT-IN result; strict no-match-with-NULLs ⇒ NULL. **One** residual `panic!` (the only one): the **non-strict** no-match-with-NULLs branch dispatches the equality fn with `args[1].isnull = true` and reads back `fcinfo->isnull`, which `function_call2_coll` (`FunctionCall2Coll`, non-null args + non-null result) cannot model — mirror-PG-and-panic on the unported fmgr-widened nullable-arg call frame. That is a panic on a genuinely-unported owner capability, not absent own-logic. |

**Model changes (additive, keystone):**

- `types_nodes::saophash` (new) — the real `ScalarArrayOpExprHashEntry` /
  `SaophashHash` / `ScalarArrayOpExprHashTable` data structs, so the step
  payload carries the real typed table.
- `ExprEvalStepData::HashedScalarArrayOp.elements_tab`: `usize` →
  `Option<Box<ScalarArrayOpExprHashTable>>` (opacity-inherited: the C
  `ScalarArrayOpExprHashTable *`, `None` = C `NULL`). No other crate read the
  old `usize` (execExpr does not yet emit this opcode), so the change is local.
- `primnodes::ScalarArrayOpExpr`: added `hashfuncid` + `negfuncid` field-for-field
  (PG-18 `primnodes.h`), the hash function and hashed-NOT-IN equality function
  OIDs the build path reads.

**New deps:** `backend-utils-adt-arrayfuncs-seams`,
`backend-utils-cache-lsyscache-seams` (both leaf seam-declaration crates → no
dependency cycle).

No MISSING / PARTIAL / DIVERGES in this family: the simplehash is real own-logic
(unit-tested), the two callbacks and the build/probe are real seam `::call`s,
and the single residual `panic!` is on a genuinely-unported owner capability.

## Gate

- `cargo check --workspace`: clean (warnings only).
- `cargo test --workspace`: pass, zero failures.
- `cargo test -p seams-init recurrence_guard`: both tests pass.

## Verdict

**PASS.** All ~81 C functions present with C-faithful logic; zero
`todo!()`/`unimplemented!()` in own logic; every residual panic is
mirror-PG-and-panic on an unported owner or a not-yet-modeled neighbor struct
field (chiefly the execTuples slot payload, decomp #113, and the fmgr-widened
call frame — the same model-layer blockers the audited execExpr crate carries),
or a defensive guard matching a C `Assert`/`pg_unreachable`. The EEOP_* contract
is complete in both directions, and the owned seam surface is wired.
