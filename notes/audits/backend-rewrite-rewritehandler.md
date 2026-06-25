# backend-rewrite-rewritehandler audit

Date: 2026-06-16

Verdict: FAIL

Scope: `src/backend/rewrite/rewriteHandler.c`

References:
- C: `../pgrust/postgres-18.3/src/backend/rewrite/rewriteHandler.c`
- c2rust: `../pgrust/c2rust-runs/backend-rewrite-core/src/rewriteHandler.rs`
- Port: `crates/backend-rewrite-rewritehandler/src/{lib.rs,engine.rs,seams.rs}`
- Split helper port: `crates/backend-rewrite-core/src/manip_rule.rs`

This audit used the repo `audit-crate` workflow. It enumerates every function in
`rewriteHandler.c`; the verdict is fail because multiple owner functions are
missing or partial. Some failures are intentional loud boundaries, but they are
still not audit-passable for this unit because the C function body belongs to
`rewriteHandler.c`.

## Function Table

| C function | C line | Port location | Verdict | Notes |
|---|---:|---|---|---|
| `AcquireRewriteLocks` | 147 | `engine.rs:841` | PARTIAL | RTE relation/join/subquery and CTE recursion are present. SubLink descent returns an error on `hasSubLinks`; C recurses through `query_tree_walker(... acquireLocksOnSubLinks ...)`. |
| `acquireLocksOnSubLinks` | 309 | `engine.rs:1353` | PARTIAL | No-op unless a SubLink is detected, then errors on the `'static SubLink.subselect` keystone. C descends into the SubLink query and then walks left-hand args. |
| `rewriteRuleAction` | 350 | `engine.rs:976` | PARTIAL | Main rule-action shaping is present. It inherits the SubLink lock/descent gap through `acquire_locks_on_sublinks_node`; this changes behavior for rule quals containing SubLinks. Needs deeper re-check after the SubLink carrier lands. |
| `adjustJoinTreeList` | 712 | `backend-rewrite-core/src/manip_rule.rs:226` | MATCH | Split into `backend-rewrite-core`; ownership split is documented. Copies top-level jointree fromlist and optionally removes the result RTE. |
| `rewriteTargetListIU` | 774 | `engine.rs:351` | PARTIAL | INSERT/UPDATE target-list rewrite is present and used. MERGE action use is not reached because `RewriteQuery` errors for `CMD_MERGE`; ON CONFLICT update-set use is also blocked before calling this helper. |
| `process_matched_tle` | 1047 | `engine.rs:155` | MATCH* | Assignment merge helper appears structurally ported. Marked with a star because this fail audit did not exhaustively prove every FieldStore/SubscriptingRef edge after the caller-level failures were found. |
| `get_assignment_input` | 1200 | `engine.rs:273` | MATCH | Mirrors FieldStore/SubscriptingRef input extraction. |
| `build_column_default` | 1229 | `lib.rs:86` | MATCH* | Default/generation expression path is present. Not blocking this audit. |
| `searchForDefault` | 1300 | `engine.rs:289` | MATCH | Scans VALUES lists for `SetToDefault`. |
| `findDefaultOnlyColumns` | 1326 | `engine.rs:304` | MATCH | Builds/intersects default-only column bitmap across VALUES rows. |
| `rewriteValuesRTE` | 1414 | `engine.rs:588` | MATCH* | Main VALUES default replacement path is present. Not exhaustively re-proven after higher-level fail. |
| `rewriteValuesRTEToNulls` | 1599 | `engine.rs:718` | MATCH* | Present and used for product VALUES finalization. |
| `matchLocks` | 1637 | `engine.rs:752` | MATCH* | Rule filtering by event/result relation is present. |
| `ApplyRetrieveRule` | 1712 | `engine.rs:1497` | PARTIAL | Plain SELECT-from-view expansion is present. UPDATE/DELETE/MERGE view result relation errors because `makeWholeRowVar` is not ported; FOR UPDATE/SHARE view locking errors because `markQueryForLocking` is not ported. Security-barrier view reloption is hard-coded false. |
| `markQueryForLocking` | 1892 | missing | MISSING | C recursively applies locking clauses to relation/subquery RTEs and sets `ACL_SELECT_FOR_UPDATE`. Port has no implementation and errors when the path is reached. |
| `fireRIRonSubLink` | 1956 | missing | MISSING | C rewrites SubLink subselects in-place and propagates `hasRowSecurity`. Port errors on `hasSubLinks` in `fireRIRrules`. |
| `fireRIRrules` | 1992 | `engine.rs:1594` | PARTIAL | Relation/subquery/CTE RIR expansion is present. Missing SEARCH/CYCLE expansion (`rewriteSearchAndCycle`), SubLink RIR descent, and row-security policy application (`get_row_security_policies`). |
| `CopyAndAddInvertedQual` | 2321 | `engine.rs:1369` | PARTIAL | Main inversion path is present but inherits the SubLink lock/descent gap for quals containing SubLinks. |
| `fireRules` | 2392 | `engine.rs:1420` | MATCH* | Rule application loop is present. Needs re-audit after `rewriteRuleAction` gaps are fixed. |
| `get_view_query` | 2483 | `lib.rs:604` | MATCH* | Implemented over `relation_rules` projection; errors on missing/malformed `_RETURN`. |
| `view_has_instead_trigger` | 2522 | `lib.rs:198` | MATCH* | Trigger event checks present. |
| `view_col_is_auto_updatable` | 2586 | `lib.rs:255` | MATCH* | Column updatability checks present. |
| `view_query_is_auto_updatable` | 2634 | `lib.rs:286` | MATCH* | View shape checks present. |
| `view_cols_are_auto_updatable` | 2782 | `lib.rs:400` | MATCH* | Column bitmap/detail path present. |
| `relation_is_updatable` | 2865 | `lib.rs:642` | PARTIAL | Tables, unconditional INSTEAD rules, INSTEAD OF triggers, foreign-table seam, and recursive view support are present. Depends on `get_view_query` and auto-updatable column helpers. Needs complete re-check after `rewriteTargetView`/view reloptions/security-invoker carriers settle. |
| `adjust_view_column_set` | 3046 | `lib.rs:448` | MATCH* | Present for mapping view updatable columns to base relation columns. |
| `error_view_not_updatable` | 3120 | `lib.rs:512` | MATCH* | Error construction is present; SQLSTATEs/details need final proof in a clean audit. |
| `rewriteTargetView` | 3215 | missing | MISSING | C rewrites an automatically updatable view target to its base relation, including RTE/perminfo manipulation, CHECK OPTION handling, target-list rewrites, and RETURNING rewrites. Port errors when this path is reached. |
| `RewriteQuery` | 3881 | `engine.rs:1744` | PARTIAL | Non-SELECT rule driver is present for common INSERT/UPDATE/DELETE paths. Missing/blocked paths: ON CONFLICT DO UPDATE set-list rewrite, MERGE action target-list rewrite, automatic view-update rewrite, and updatable-view product-query ordering. |
| `expand_generated_columns_internal` | 4448 | `lib.rs:809` | MATCH* | Present and used by expression and query paths. |
| `expand_generated_columns_in_expr` | 4493 | `lib.rs:864` | MATCH* | Present and installed as a seam. |
| `build_generation_expression` | 4519 | `lib.rs:906` | MATCH* | Present, including COLLATE wrapper. |
| `QueryRewrite` | 4565 | `engine.rs:2143` | PARTIAL | Canonical value-typed entry is present and installed. It calls partial `RewriteQuery` and `fireRIRrules`, so it is not complete. |

`MATCH*` means the function appears structurally faithful in the inspected
paths, but this fail report did not perform final proof-grade parity for all
edge cases because earlier merge-blocking failures already prevent a PASS.

## Merge-Blocking Findings

1. `markQueryForLocking` is missing.

   C location: `rewriteHandler.c:1892`.

   Port evidence: `ApplyRetrieveRule` returns an error for `FOR [KEY]
   UPDATE/SHARE` of a view in `engine.rs:1548`.

   Behavioral impact: queries that lock a view should be rewritten by applying
   locking clauses to contained relation/subquery RTEs and setting
   `ACL_SELECT_FOR_UPDATE`; the port errors instead.

2. `fireRIRonSubLink` and SubLink lock descent are missing.

   C locations: `rewriteHandler.c:309`, `rewriteHandler.c:1956`.

   Port evidence: `AcquireRewriteLocks` errors on `parsetree.hasSubLinks`;
   `fireRIRrules` errors on `parsetree.hasSubLinks`.

   Behavioral impact: view/rule expansion inside scalar/EXISTS subqueries is
   not performed. This is a known carrier blocker (`SubLink.subselect` is
   modeled as `'static`), but it still makes the rewriteHandler unit partial.

3. `rewriteTargetView` is missing.

   C location: `rewriteHandler.c:3215`.

   Port evidence: `RewriteQuery` errors when an auto-updatable view target is
   reached.

   Behavioral impact: automatic INSERT/UPDATE/DELETE/MERGE rewriting of views
   to their base relation is absent.

4. `ApplyRetrieveRule` is partial for view result relations.

   C location: `rewriteHandler.c:1712`.

   Port evidence: UPDATE/DELETE/MERGE on a view result relation errors because
   `makeWholeRowVar` is absent.

   Behavioral impact: the C creates OLD whole-row Vars and adjusts the result
   relation path; the port stops.

5. `fireRIRrules` is partial for SEARCH/CYCLE and RLS.

   C location: `rewriteHandler.c:1992`.

   Port evidence: the port errors for recursive CTE SEARCH/CYCLE expansion and
   for queries already flagged as needing row-security policy expansion.

   Behavioral impact: recursive CTE SEARCH/CYCLE rewrite and
   `get_row_security_policies` are not applied.

6. `RewriteQuery` is partial for ON CONFLICT DO UPDATE and MERGE.

   C location: `rewriteHandler.c:3881`.

   Port evidence: explicit errors for `ON CONFLICT DO UPDATE` set-list rewrite
   and for `CMD_MERGE`.

   Behavioral impact: C rewrites these target lists and proceeds; the port
   stops.

7. `relation_is_updatable` and view security options need final proof.

   C location: `rewriteHandler.c:2865`.

   Port evidence: the function is present, but `ApplyRetrieveRule` hard-codes
   security-barrier view reloptions false, and related security-invoker/view
   reloption carriers are still unsettled elsewhere.

   Behavioral impact: view updatability and security semantics need a dedicated
   re-audit once the missing view rewrite paths land.

## Seam And Wiring Audit

- `backend_rewrite_rewritehandler::init_seams()` installs:
  - `build_column_default`
  - `expand_generated_columns_in_expr`
  - `view_query_is_auto_updatable`
  - `get_view_query`
  - `relation_is_updatable`
  - legacy `query_rewrite` as a K1 panic boundary
  - canonical `query_rewrite_canonical`
- The canonical `query_rewrite_canonical` seam is installed and delegates to
  `crate::QueryRewrite`.
- The legacy opaque `query_rewrite(portalcmds::Query)` seam intentionally
  panics on K1 Query-unification debt. This is acceptable as a boundary for
  current callers, but it is not a complete C `QueryRewrite` implementation.
- Outward seam use for `relation_rules` is appropriate: the relcache rule list
  is owned outside this crate and projected into the rewrite engine.

Seam verdict: FAIL, because the owner still installs several explicit panic/error
boundaries for logic owned by `rewriteHandler.c` itself.

## High-Risk Follow-Up Queue

These crates should be audited next because they sit on the same startup/query
execution path and are either large orchestration ports or value-carrier
keystones:

1. `backend-rewrite-rewritehandler` re-audit after fixes above.
2. `backend-tcop-postgres`.
3. `backend-parser-analyze`.
4. `backend-optimizer-util-plancat`.
5. `backend-executor-execMain`.
6. `backend-commands-indexcmds` once the current F2 CREATE INDEX runtime lane
   lands.
7. `backend-nodes-outfuncs` / `backend-nodes-readfuncs` after the node sweep.

