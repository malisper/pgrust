# Audit: backend-parser-cte (parse_cte.c) — STEP 4g

Unit `backend-parser-medium1` covers `parse_cte.c` + `parse_jsontable.c`. This
lane ports **parse_cte.c fully**; **parse_jsontable.c is deferred** (its raw
JSON-grammar node vocabulary is absent — see "parse_jsontable deferral" below).

Crate: `crates/backend-parser-cte` (new). Logic reference: src-idiomatic
`backend-parser-parse-cte` + the C source. Model: owned `mcx`/`PgVec`/`PgBox`
node tree (`types_nodes::nodes::Node`), split Expr/Node, no raw `*mut`.

## Per-function verdict vs parse_cte.c

| C function (parse_cte.c)                    | Rust item                          | Verdict |
| ------------------------------------------- | ---------------------------------- | ------- |
| `transformWithClause` (:110)                | `pub fn transformWithClause`       | PASS    |
| `analyzeCTE` (:243)                         | `fn analyzeCTE`                    | PASS    |
| `analyzeCTETargetList` (:571)               | `pub fn analyzeCTETargetList`      | PASS    |
| `makeDependencyGraph` (:648)                | `fn makeDependencyGraph`           | PASS    |
| `makeDependencyGraphWalker` (:670)          | `fn makeDependencyGraphWalker`     | PASS    |
| `WalkInnerWith` (:812)                      | `fn WalkInnerWith`                 | PASS    |
| `TopologicalSort` (:863)                    | `fn TopologicalSort`               | PASS    |
| `checkWellFormedRecursion` (:915)           | `fn checkWellFormedRecursion`      | PASS    |
| `checkWellFormedRecursionWalker` (:1027)    | `fn checkWellFormedRecursionWalker`| PASS    |
| `checkWellFormedSelectStmt` (:1207)         | `fn checkWellFormedSelectStmt`     | PASS    |

`RecursionContext` (:31) + `recursion_errormsgs[]` (:42) ported as the enum +
`recursion_errormsg()`. `CteItem` (:63) / `CteState` (:71) modelled as owned
working structs (`depends_on` = `BTreeSet<i32>`; `innerwiths` = stack of CTE-name
frames; CTEs owned in `CteState::ctes`, `CteItem.cte` an index).

## Branch / error parity

Every `ereport(ERROR, ...)` reproduced with the same SQLSTATE + errmsg/errhint
text and `parser_errposition(pstate, location)` cursor: `ERRCODE_DUPLICATE_ALIAS`,
`ERRCODE_FEATURE_NOT_SUPPORTED`, `ERRCODE_DATATYPE_MISMATCH`,
`ERRCODE_COLLATION_MISMATCH`, `ERRCODE_SYNTAX_ERROR`, `ERRCODE_DUPLICATE_COLUMN`,
`ERRCODE_INVALID_COLUMN_REFERENCE`, `ERRCODE_INVALID_RECURSION`,
`ERRCODE_UNDEFINED_FUNCTION`. The "shouldn't happen" `elog(ERROR, ...)` cases use
`errmsg_internal` (XX000). The NULL-collation `%s` renders glibc's literal
`(null)` (`collation_name_or_null`), matching C.

The `analyzeCTE` `!IsA(query, Query)` check (:323) is preserved explicitly
(`is_query` after the `parse_sub_analyze` store), even though the seam returns a
`Node::Query`, to keep the C control flow 1:1.

## Owned-tree adaptations (semantics preserved)

* The C walkers take `&Node` and mutate the `cstate` scratch; the owned tree's
  `raw_expression_tree_walker` takes a `&mut dyn FnMut(&Node)->bool` closure that
  cannot itself return a `PgResult`, so child recursion is driven via
  `collect_children` (clone the immediate children the raw walker would visit,
  then recurse threading `&mut ParseState`/`&mut CteState`). Branch order and the
  early-abort `bool` are preserved.
* **ctenamespace aliasing**: in C `p_ctenamespace` holds pointers to the very
  CTE objects `analyzeCTE` mutates in place; the owned model holds clones, so
  `refresh_ctenamespace_entry` updates the matching slot (by `ctename`) after
  each `analyzeCTE` — preserving forward visibility of analyzed columns.
* The cycle-mark value/default are stored as `Node::Expr` after `transformExpr`;
  they are threaded as `Expr` through `select_common_type`/`coerce_to_common_type`
  /`select_common_typmod`/`select_common_collation` (which take `Expr`/`&[Expr]`)
  and re-wrapped for storage. `select_common_collation` mutates exprs in place
  (`exprSetCollation`); `store_back_exprs` copies the result back, mirroring the
  C pointer aliasing.
* `analyzeCTETargetList`'s `tlist` is `&[TargetEntry]` (the repo's `Query`
  carries `targetList`/`returningList` as `PgVec<TargetEntry>`, not boxed Nodes),
  so the C `lfirst`/`resjunk`/`resno`/`resname`/`expr` reads map directly.
* `ctecolnames`/`search_col_list`/`cycle_col_list` are `PgVec<Node::String>`;
  `makeString`/`strVal`/`list_member` become `make_string_node`/`str_val`/linear
  `any(==)`.

## Deps and seams

Direct merged-owner calls (cycle-free): `transformExpr`
(backend-parser-parse-expr), `select_common_type`/`coerce_to_common_type`/
`select_common_typmod` (backend-parser-coerce), `select_common_collation`
(backend-parser-parse-collate), `format_type_be_owned`/`format_type_with_typemod`
(backend-utils-adt-format-type), `get_negator`/`get_collation_name`
(backend-utils-cache-lsyscache).

Outward seams:
* `parse_sub_analyze` (NEW in `backend-parser-analyze-seams`) — the CTE↔analyze
  recursion seam. analyze.c is unported, so a call panics loudly until it lands
  and installs it. This is `analyzeCTE`'s sole genuinely-external dependency.
* `lookup_type_cache_eq_opr` (NEW in `backend-utils-cache-typcache-seams`,
  **installed** by the ported typcache owner) — the trimmed `TypeCacheEntry`
  returned by `lookup_type_cache` lacks `eq_opr`, so this dedicated accessor
  reads the equality-operator OID from the full cache row
  (`lookup_type_cache(type_id, TYPECACHE_EQ_OPR)->eq_opr`), mirroring the
  existing `lookup_element_eq_opr` pattern.
* `parser_errposition` (parse_node.c, owned by parser-small1).

This crate owns **no inward seam**: `transformWithClause` / `analyzeCTETargetList`
are consumed by analyze.c / parse_clause.c (both unported), which will call them
directly as merged owners once landed (cf. parse_collate / parse_oper). So
`init_seams()` is empty and the crate is not wired into `init_all`.

## parse_jsontable deferral (noted, not stubbed)

parse_jsontable.c's raw input vocabulary — `JsonTable`, `JsonTablePathSpec`,
`JsonFuncExpr`, `JsonTableColumn`, `JsonTablePlan`, `JsonOutput` — is entirely
absent from `types-nodes` (only `JsonBehavior`/`JsonValueExpr` exist), i.e. the
SQL/JSON raw-grammar keystone (the parse_clause F3b family) is unbuilt. It also
depends on `transformJsonValueExpr`/`transformJsonBehavior`, themselves
seam-and-panic'd in the already-landed parse_expr (its SQL/JSON family). Per the
task's explicit guidance, parse_jsontable is deferred to a follow-on once those
nodes + `addRangeTableEntryForTableFunc`-driven JSON path land; nothing is
stubbed here (no `todo!`/`unimplemented!`). The catalog row stays `todo` to track
the remaining file.

## residual_own_todos = 0

No `todo!()`/`unimplemented!()`; the only out-of-crate calls are the three named
seams + the direct merged-owner calls above. `analyzeCTE` is fully reachable
except the `parse_sub_analyze` seam (analyze unported).

## Gates

`cargo check --workspace` clean; `no-todo-guard` PASS; `seams-init` PASS (both
recurrence guards green — `parse_sub_analyze` exempt as analyze-owned-unported;
`lookup_type_cache_eq_opr` installed by typcache); `cargo test --workspace` green
except the sanctioned `range_pair_*` flake. 10 crate unit tests pass (duplicate
name, dependency ordering, self-ref marking, mutual-recursion rejection,
recursive-must-be-UNION, recursive-ref-in-nonrecursive-term rejection, well-formed
acceptance, target-list derivation, too-many-aliases rejection, RangeTblRef
detection).

Verdict: **PASS** (parse_cte.c). parse_jsontable.c deferred.
