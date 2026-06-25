# Audit note — backend-parser-parse-collate (parity-fix sweep)

Scope: the two `assign_collations_walker` arms the parity audit (wf_02a4ccb2)
flagged once the F0 #219 node fields landed. Other functions unchanged.

## Fixes

- **T_InferenceElem** (parse_collate.c:482-499): now grouped with
  `RangeTblRef`/`JoinExpr`/`FromExpr`/… behavior. `InferenceElem` is an `Expr`
  variant in this model, so it gets an explicit arm in the expression switch
  (`assign_collations_walker_expr`) that recurses children
  (`recurse_expr_children`) and returns `Ok(())` — it never bubbles collation
  state up to the parent via `merge_collation_state`. Previously it fell into
  the general `_` arm, which merged into the parent (a divergence).

- **T_TargetEntry sort/group throw** (parse_collate.c:471-480): the eager
  `ERRCODE_COLLATION_MISMATCH` is now reproduced. `TargetEntry` carries
  `ressortgroupref` (F0), so when `strength == COLLATE_CONFLICT &&
  ressortgroupref != 0` the arm throws via the shared `implicit_conflict_error`
  helper with `loccontext.collation`/`collation2`/`location2`, the COLLATE
  hint, and `parser_errposition(location2)` — exactly the C ereport. Previously
  the throw was skipped (the stale comment claimed `ressortgroupref` was
  trimmed).

## Gate

`cargo check --workspace` clean; `cargo test -p no-todo-guard` /
`-p seams-init` pass; `cargo test -p backend-parser-parse-collate` (6 tests)
pass; `cargo test --workspace` green except the pre-existing allowed flake
`backend-optimizer-path-small::range_pair_positive_combination`.

Verdict: PASS.
