# Audit: backend-optimizer-util-inherit-predtest

Unit: `optimizer/util/predtest.c` + `optimizer/util/inherit.c` (PostgreSQL 18.3).
Crate: `backend-optimizer-util-inherit-predtest` (+ seam crates
`backend-optimizer-util-predtest-seams`, `backend-optimizer-util-inherit-predtest-seams`).
Auditor: Claude Opus 4.8 (1M). Method: function-by-function 1:1 vs the C ground
truth.

Verdict: **PASS** for the ported surface. `predtest.c` is ported in full;
`inherit.c`'s pure attribute-set helpers are ported in full; `inherit.c`'s
inheritance/partition expansion entry points are **keystone-blocked** on the
parser's owned `RangeTblEntry`/`PlanRowMark`/`Query` value model and are
explicitly NOT ported (documented below — not a silent stub).

## Value-model adaptations (apply throughout)

* C planner expression nodes (`OpExpr`/`BoolExpr`/`ScalarArrayOpExpr`/`Const`/
  `NullTest`/`BooleanTest`/coercions/…) are the owned `types_nodes::primnodes::
  Expr` enum; `IsA(node,X)`/`nodeTag(node)` become `as_*`/`is_*` accessors;
  primnode field access is direct struct-field access.
* `equal(a,b)` is the established `backend_nodes_equalfuncs_seams::equal_expr`
  seam (the same `equal()` seam indxpath/equivclass use; equalfuncs.c unported
  → seam-and-panic).
* `List *` of nodes becomes `&[Expr]`/`Vec<Expr>`.
* The C `PredIterInfo` startup/next/cleanup function-pointer trio becomes
  `PredIterInfo { kind }` + `components()` which materialises the component set
  as an owned `Vec<Expr>` (the analogue of C's per-loop `startup()` walk). For
  List/BoolExpr the components are clones of the args; for the deconstructable
  SAOP cases they are freshly built dummy `OpExpr`s exactly as the C
  `arrayconst`/`arrayexpr` iterators build.
* Catalog probes cross through the lsyscache seam crate (PgResult-returning);
  the array deconstruction through the arrayfuncs seams; `CHECK_FOR_INTERRUPTS`
  through pathnode-seams. The executor const test and the pg_amop inval
  registration are OUTWARD seams in `-inherit-predtest-seams` (owners
  execExpr/inval; loud-panic until they land).

---

## predtest.c

### predicate_implied_by — MATCH
Public seam-facing entry. C `bool predicate_implied_by(List *predicate_list,
List *clause_list, bool weak)`. Empty-predicate ⇒ true (vacuous); empty-clause
⇒ false. Single-element list ⇒ lone member; multi-element ⇒ `wrap_list` (implicit
AND, see below). Recurses via `predicate_implied_by_impl`. Repo contract takes
arena handles (`&[NodeId]`) resolved through `root` (`RestrictInfo.clause`/
`indpred` are arena handles in this model) and returns `bool` (matching the
landed indxpath consumer); the catalog-leg `ereport(ERROR)` becomes a panic at
this infallible boundary, mirroring the established
`get_mergejoin_opfamilies`-style transient-context boundary in pathkeys.

### predicate_refuted_by — MATCH
Same shape; empty-predicate ⇒ false; empty-clause ⇒ false. Mirrors C exactly.

### wrap_list (List → implicit-AND BoolExpr) — MATCH (model adaptation)
C passes a multi-element `List *` as the `Node *`, and `predicate_classify`
treats a bare `List` as an implicit-AND (CLASS_AND, iterates members). The owned
`Expr` enum has no `List` variant; an implicit-AND list and an explicit AND
`BoolExpr` classify identically and iterate the same component set, so the list
is modelled as an AND `BoolExpr` — observationally identical for the proof
engine. The `IsA(clause, RestrictInfo)` skip in the recurse functions is vacuous
in this model (an `Expr` cannot hold a `RestrictInfo`; the consumer resolves
`RestrictInfo.clause` at the boundary) and is documented, not reproduced.

### predicate_implied_by_recurse — MATCH
All nine (clause-class × pred-class) arms transcribed 1:1: the AND/AND, AND/OR
(both legs, with the early `if result return` then the A-items-imply-B leg),
AND/ATOM, OR/OR (the nested presult loop), OR/{AND,ATOM}, ATOM/AND, ATOM/OR,
ATOM/ATOM (base case → `predicate_implied_by_simple_clause`). Loop break/continue
and the `result` accumulation match the C. C's `elog(ERROR,"predicate_classify
returned a bogus value")` fall-through is unreachable (the `match` over the
3-variant `PredClass` is exhaustive).

### predicate_refuted_by_recurse — MATCH
All arms transcribed 1:1, including the three NOT-clause rules:
* CLASS_ATOM predicate legs: `extract_not_arg(predicate)` then
  `predicate_implied_by_recurse(clause, not_arg, false)` (strong, weak=false).
* CLASS_ATOM clause leg (top of the ATOM arm): `extract_strong_not_arg(clause)`
  then `predicate_implied_by_recurse(predicate, not_arg, !weak)`.
The AND/AND (refute-any-of-B then any-A-refutes-B), AND/OR, OR/OR, OR/AND (nested
presult), ATOM/{AND,OR,ATOM} arms all match.

### predicate_classify — MATCH
AND/OR boolean clauses → CLASS_AND/CLASS_OR (List kind). SAOP gate:
non-null `Const` array with `array_const_nitems ≤ MAX_SAOP_ARRAY_SIZE` →
ArrayConst (useOr ? OR : AND); non-multidim `ArrayExpr` with `len ≤
MAX_SAOP_ARRAY_SIZE` → ArrayExpr. Otherwise ATOM. `MAX_SAOP_ARRAY_SIZE = 100`.
The bare-`List` arm is intentionally absent (see `wrap_list`).

### list_startup_fn / list_next_fn / list_cleanup_fn / boolexpr_startup_fn — MATCH
Collapsed into `PredIterKind::List` + `components()`: the BoolExpr/list `args`
are the component set, iterated in order. cleanup is a no-op (owned `Vec` drop).

### arrayconst_startup_fn / arrayconst_next_fn / arrayconst_cleanup_fn — MATCH
`arrayconst_components`: `array_get_elemtype` + `get_typlenbyvalalign(ARR_ELEMTYPE)`
+ `deconstruct_array(arrayval, elemtype, elmlen, elmbyval, elmalign)` (the exact
C `arrayconst_startup_fn` sequence), then one dummy `scalar saop_op elem_const`
`OpExpr` per element (the C `arrayconst_next_fn` yield). The dummy `Const` carries
`consttype = elemtype`, `consttypmod = -1`, `constcollid = arrayconst.constcollid`,
`constvalue`/`constisnull` per element — matching the C `state->const_expr`
stamping. The C `constlen`/`constbyval` decorations are not fields of this repo's
trimmed `Const`; only the downstream `eval_const_test` re-derives storage from
`test_op`, so this is behaviour-preserving. The dummy `OpExpr` carries the SAOP's
`opno`/`opfuncid`/`inputcollid`, `opresulttype = BOOLOID`, `opcollid =
InvalidOid` (C `arrayconst_startup_fn`). cleanup = owned drop.

### arrayexpr_startup_fn / arrayexpr_next_fn / arrayexpr_cleanup_fn — MATCH
`arrayexpr_components`: one dummy `scalar saop_op element` `OpExpr` per ArrayExpr
element (elements are already owned `Expr`s). Matches the C.

### predicate_implied_by_simple_clause — MATCH
`CHECK_FOR_INTERRUPTS`; `equal()` self-implication; the `BooleanEqualOperator`
(OID 91) "x = TRUE ⇒ x" / "x = FALSE ⇒ NOT x" rule (rightop a non-null Const,
`DatumGetBool`); the `IS_NOT_NULL` predicate ⇒ `clause_is_strict_for(clause, arg,
true)` under strong implication (`!weak && !argisrow`); finally
`operator_predicate_proof(predicate, clause, refute_it=false, weak)`.

### predicate_refuted_by_simple_clause — MATCH
`CHECK_FOR_INTERRUPTS`; the self-refute pointer-equality fast path
(`core::ptr::eq`, the C `(Node*)predicate == clause`); the clause-`IS_NULL` arm
(`foo IS NULL` refutes `foo IS NOT NULL` via `equal`; weak ⇒ strict-for); the
predicate-`IS_NULL` arm (`foo IS NOT NULL` refutes `foo IS NULL`; strict-for for
either strong/weak); the `argisrow` early-falses; finally
`operator_predicate_proof(predicate, clause, refute_it=true, weak)`. The
intermediate `return false` "we can't succeed below" early-outs match.

### extract_not_arg / extract_strong_not_arg — MATCH
`BoolExpr NOT_EXPR` ⇒ first arg; `BooleanTest` ⇒ arg for `{IS_NOT_TRUE, IS_FALSE,
IS_UNKNOWN}` (not_arg) / `{IS_FALSE}` (strong_not_arg). Matches the C
discriminant sets.

### clause_is_strict_for — MATCH
Made fallible (`PgResult<bool>`) because `op_strict`/`func_strict`/
`array_const_nitems` cross PgResult-returning seams (the C `op_strict`'s syscache
lookup `elog(ERROR)`s); errors propagate rather than being swallowed.
RelabelType look-through (both operands); `equal()` base case; strict op/func
arg recursion; CoerceViaIO/ArrayCoerceExpr/ConvertRowtypeExpr/CoerceToDomain
strictness; the SAOP special case (scalar strict + strict op ⇒ non-empty array
check, with `allow_false && useOr` early-true, constant-NULL early-true,
`array_const_nitems` / non-multidim `ArrayExpr` length, then array-input strict);
trailing NULL-`Const` ⇒ `constisnull`. All recursion `allow_false` arguments
match the C (only the SAOP `useOr` leg passes the caller's `allow_false`; every
other recursion passes `false`).

### operator implication tables — MATCH (verified cell-by-cell)
`RC_implies_table`, `RC_refutes_table`, `RC_implic_table`, `RC_refute_table`
transcribed cell-for-cell vs predtest.c:1672-1722. `RCNE = COMPARE_NE = 6`;
`none = 0/false`. cmptype numbering 1..=6 (LT LE EQ GE GT NE); table indexed
`[clause_cmptype-1][pred_cmptype-1]`.

### operator_predicate_proof — MATCH
Binary-opclause gate (both 2-arg OpExprs); collation equality gate; the
five subexpression-match cases with the exact commutation logic:
* L=L, R=R ⇒ `operator_same_subexprs_proof(pred_op, clause_op)`.
* L=L, R≠R ⇒ both right Consts (else fail).
* R=R ⇒ both left Consts, commute BOTH ops.
* L(pred)=R(clause), R(pred)=L(clause) ⇒ commute pred_op,
  `operator_same_subexprs_proof`.
* L(pred)=R(clause), else ⇒ pred_right/clause_left Consts, commute clause_op.
* R(pred)=L(clause) ⇒ pred_left/clause_right Consts, commute pred_op.
Each `get_commutator` invalid-OID short-circuits to false. Then the NULL-const
reasoning: clause_const NULL ⇒ require `op_strict(clause_op)`; `!(weak &&
!refute_it)` ⇒ true; else weak-implication NULL⇒NULL via
`pred_const NULL && op_strict(pred_op)`. pred_const NULL ⇒ `weak &&
op_strict(pred_op)` ⇒ true. Then `get_btree_test_op`; invalid ⇒ false; else
`eval_const_test` (OUTWARD executor seam) with None ⇒ non-proof (C's "null
predicate test result" DEBUG2 + false), Some(b) ⇒ b. The Const operands are
passed by reference to the eval seam (the C builds the test OpExpr and evaluates
it in a throwaway EState; that whole leg is the seam's responsibility — owner
execExpr). 1:1.

### operator_same_subexprs_proof — MATCH
refute ⇒ `get_negator(pred_op) == clause_op`; else `pred_op == clause_op`; else
`operator_same_subexprs_lookup`. Matches C.

### lookup_proof_cache — MATCH (cache as genuine in-module per-backend state)
The C `static HTAB *OprProofCacheHash` keyed by (pred_op, clause_op) becomes a
`thread_local!` `Option<HashMap<(Oid,Oid), OprProofCacheEntry>>` (per-backend, the
backend-global-state model; the C is per-process). First-use initialises the map
and registers the pg_amop inval callback (`register_oprproof_syscache_callback`,
AMOPOPID) — the exact C `if (OprProofCacheHash == NULL) { hash_create; Cache-
RegisterSyscacheCallback }` block. Pre-existing entry with the requested
direction already computed ⇒ return. Otherwise: `get_op_index_interpretation`
(clause first; pred only if clause non-empty, the C `if (clause_op_infos)` gate);
the nested pred×clause opfamily loop with same-opfamily/same-lefttype gates;
`same_subexprs |= RC_{refutes,implies}_table[...]`; `test_cmptype =
RC_{refute,implic}_table[...]`; 0 ⇒ continue; `RCNE` ⇒
`get_opfamily_member_for_cmptype(...,COMPARE_EQ)` then `get_negator`, else direct
`get_opfamily_member_for_cmptype(...,test_cmptype)`; invalid ⇒ continue;
`op_volatile(test_op) == PROVOLATILE_IMMUTABLE` ⇒ found+break. Post-loop:
not-found ⇒ test_op = InvalidOid; `same_subexprs && op_volatile(clause_op) !=
IMMUTABLE` ⇒ clear; cache results into the entry by direction. The C
`list_free_deep` is the owned `PgVec` drop. `Assert(clause_op_info.oplefttype ==
pred_op_info.oplefttype)` → `debug_assert!`.

### operator_same_subexprs_lookup / get_btree_test_op — MATCH
Thin direction-selecting reads of the cache entry. Match C.

### InvalidateOprProofCacheCallBack — MATCH
`invalidate_opr_proof_cache_callback(cacheid, hashvalue)`: reset every entry's
`have_implic`/`have_refute` (the C "reset all entries; hard to be smarter"). The
C `Datum arg` (always 0) is dropped at the seam (the callback fn type carries
only cacheid+hashvalue). The C `Assert(OprProofCacheHash != NULL)` is the
`if let Some(map)` guard.

---

## inherit.c

### translate_col_privs — MATCH
Pure `Relids`/attribute-number arithmetic. System-attribute loop
(`FirstLowInvalidHeapAttributeNumber+1 .. 0`, offset by `-FLIHAN`); whole-row
check (`InvalidAttrNumber - FLIHAN`); user-attribute loop over
`translated_vars` resolving each arena handle to a `Var` (dropped column =
`NodeId::default()` hole, the C `var == NULL`), adding `var->varattno - FLIHAN`
when whole_row or the parent bit is set. bms primitives cross the established
`relids_*` seams (relnode owner; restrictinfo/pathnode/joinrels use the same
facade). 1:1.

### translate_col_privs_multilevel — MATCH
Fast path `parent_cols == NULL ⇒ NULL`; recurse when `rel->parent !=
parent_rel` (the `rel->parent == NULL ⇒ elog(ERROR,"rel ... is not a child
rel")` path is a real `Err`); then `translate_col_privs` with
`append_rel_array[rel->relid]->translated_vars`. `Assert(append_rel_array !=
NULL)`/`Assert(appinfo != NULL)` → `debug_assert!`/real `Err`. 1:1.

### expand_inherited_rtentry — NOT PORTED (keystone-blocked)
### expand_partitioned_rtentry — NOT PORTED (keystone-blocked)
### expand_single_inheritance_child — NOT PORTED (keystone-blocked)
### expand_appendrel_subquery — NOT PORTED (keystone-blocked)
### get_rel_all_updated_cols — NOT PORTED (keystone-blocked)
### apply_child_basequals — NOT PORTED (keystone-blocked)

These require an owned, writable parser model the repo does not have:
`expand_single_inheritance_child` does `makeNode(RangeTblEntry);
memcpy(childrte, parentrte, sizeof(RangeTblEntry))`, mutates dozens of child RTE
fields, and `lappend`s it to `parse->rtable`; likewise `makeNode(PlanRowMark)` +
`lappend` to `root->rowMarks`. In this repo `RangeTblEntry`/`PlanRowMark`/`Query`
are opaque handles (`RangeTblEntryId`/`NodeId`/`QueryId`) owned by the unported
parser — there is no writable value type to copy/mutate, and `parse->rtable` is
not appendable through the opaque `QueryId`. The expansion further depends on a
long list of unported neighbours (`table_open`/`find_all_inheritors`/
`build_simple_rel`/`make_append_rel_info`/`expand_planner_arrays`/
`get_plan_rowmark`/`select_rowmark_type`/`add_row_identity_*`/
`add_vars_to_targetlist`/`prune_append_rel_partitions`/
`PartitionDirectoryLookup`/`get_dependent_generated_columns`/
`adjust_appendrel_attrs`/`make_restrictinfo`/`restriction_is_always_*`) and on
the `simple_rte_array`<->`rtable` / `append_rel_array`<->`append_rel_list`
aliasing convention. Porting these is a prerequisite parser-keystone, not work
expressible in this value model. They are NOT stubbed (no `panic!`/`todo!` body
in the crate) — they are simply absent, recorded here and in the CATALOG note.

---

## Seams

### Inward
* `backend-optimizer-util-predtest-seams::predicate_implied_by(root, &[NodeId],
  &[NodeId], weak) -> bool` — installed by `init_seams`, consumed by
  `backend-optimizer-path-indxpath` (the pre-existing, landed contract this port
  matches exactly).

### Outward (loud-panic until owners land; OUTWARD-excluded by the seam guard)
* `backend-optimizer-util-inherit-predtest-seams::eval_const_test` — owner
  execExpr (no standalone const-eval entry yet).
* `backend-optimizer-util-inherit-predtest-seams::register_oprproof_syscache_callback`
  — owner inval.c (only a plancache-specific registration seam exists today).

### Reused consumer seams (direct deps)
lsyscache (`get_commutator`/`get_negator`/`op_strict`/`op_volatile`/
`func_strict`/`get_opfamily_member_for_cmptype`/`get_op_index_interpretation`/
`get_typlenbyvalalign`), arrayfuncs (`array_get_elemtype`/`deconstruct_array`/
`array_const_nitems`), pathnode (`check_for_interrupts`), equalfuncs
(`equal_expr`), relnode (`relids_is_member`/`relids_add_member`).

## Divergences fixed during audit
* `clause_is_strict_for` made `PgResult<bool>` to propagate the
  `op_strict`/`func_strict`/`array_const_nitems` seam error surface instead of
  swallowing it (an earlier draft used `.unwrap_or(false)`).
* Added `AMOPOPID = 3` to `types-syscache::syscache_ids` (verified against the
  PG18 generated alphabetical numbering: the same offset that maps
  `AUTHMEMROLEMEM→9`, `RELOID→57` maps `AMOPOPID→3`; cross-checked vs the
  c2rust unit's `AMOPOPID: SysCacheIdentifier = 3`).

## Gate
`cargo check --workspace` clean; `cargo test -p no-todo-guard` pass;
`cargo test -p seams-init` (both recurrence guards) pass; crate unit tests 8/8
pass.
