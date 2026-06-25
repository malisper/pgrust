# Audit: backend-optimizer-path-joinpath

- **Unit:** backend-optimizer-path-joinpath
- **C source:** `src/backend/optimizer/path/joinpath.c` (postgres-18.3)
- **Branch:** port/backend-optimizer-path-joinpath
- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) — claude-opus-4-8[1m]
- **Verdict:** **PASS**

Independent re-derivation from the C, the c2rust rendering
(`../pgrust/c2rust-runs/backend-optimizer-path-joinpath/src/joinpath.rs`), and
the Rust port (`crates/backend-optimizer-path-joinpath/src/lib.rs`). The catalog
already marked this unit `audited`; this is a fresh, independent re-audit.

## 1. Function inventory

All function *definitions* in joinpath.c (lines 49–88 are forward declarations,
not definitions). 19 definitions total: 17 statics + the public
`add_paths_to_joinrel`, with two `static inline` helpers.

| # | C function (C loc) | Port loc (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `add_paths_to_joinrel` (124) | `add_paths_to_joinrel` :188 | MATCH | joinrelids OTHER_JOINREL branch, inner_unique switch (SEMI/ANTI/UNIQUE_INNER/UNIQUE_OUTER/default), mergeclause gate (`enable_mergejoin \|\| FULL`), semifactors gate, `join_info_list` param_source_rels loop (RHS-overlap + symmetric FULL), lateral add, steps 1/2/4/5/6 all 1:1. Step 3 (`match_unsorted_inner`) is `#ifdef NOT_USED` — correctly omitted. enable GUCs passed as `JoinEnableFlags`. |
| 2 | `allow_star_schema_join` (363, inline) | `allow_star_schema_join` :344 | MATCH | `bms_overlap && bms_nonempty_difference`. `root` arg unused in C too. |
| 3 | `have_unsafe_outer_join_ref` (390, inline, `USE_ASSERT_CHECKING`) | `have_unsafe_outer_join_ref` :352 (`#[cfg(debug_assertions)]`) | MATCH | unsatisfied/satisfied diff/intersect, outer_join_rels overlap, join_info_list scan with ojrelid membership + RHS/LHS-FULL overlap. Called only from `debug_assert!` as in C (assert-only backstop). |
| 4 | `paraminfo_get_equal_hashops` (439) | `paraminfo_get_equal_hashops` :394 | MATCH | ppi_clauses loop: OpExpr-2-arg + sides-match bail; outer_is_left→arg0/left_hasheq else arg1/right_hasheq; InvalidOid bail; list_member dedup; non-hashable→binary_mode. lateral_vars = ph_lateral_vars ++ innerrel.lateral_vars (order preserved); volatile bail; hash+eq lookup bail; dedup; binary_mode=true. Returns None on any reject (C frees + returns false). |
| 5 | `extract_lateral_vars_from_PHVs` (584) | `extract_lateral_vars_from_PHVs` :498 | MATCH | hasLateralRTEs early-out; BMS_MULTIPLE early-out; per-PHV: empty-lateral skip, eval_at!=inner skip, no-overlap→phexpr directly, else pull_vars_of_level(0) with Var(varno∈ph_lateral) / PHV(eval_at⊆ph_lateral) filters. |
| 6 | `get_memoize_path` (675) | `get_memoize_path` :572 | MATCH | enable_memoize gate; `outer.parent.rows < 2` gate; cache-key presence gate; SEMI/ANTI non-unique gate; inner_unique whole-condition serial-subset gate (None param_info→bail); volatile target/baserestrict/ppi_clauses gates; hashops_outerrel = top_parent ?? outerrel; `singlerow=inner_unique`, `calls=outer_path.rows`. |
| 7 | `try_nestloop_path` (831) | `try_nestloop_path` :698 | MATCH | ojrelid-param bail; top_parent_relids fallback for inner/outerrelids; calc_nestloop_required_outer + star-schema override; assert backstop; reparam-by-child gate; precheck → create+add_path. |
| 8 | `try_partial_nestloop_path` (950) | `try_partial_nestloop_path` :801 | MATCH | asserts; inner-paramrels⊆outerrelids gate; reparam gate; cost + partial precheck → create(required_outer=NULL)+add_partial_path. |
| 9 | `try_mergejoin_path` (1029) | `try_mergejoin_path` :877 | MATCH | is_partial delegates to partial; ojrelid bail; calc_non_nestloop_required_outer gate; outersortkeys count_contained (presorted_keys) / innersortkeys contained → NIL; cost + precheck → create+add_path. |
| 10 | `try_partial_mergejoin_path` (1145) | `try_partial_mergejoin_path` :993 | MATCH | asserts + inner-req_outer bail; same sort-key shortcut; partial precheck → create(NULL)+add_partial_path. |
| 11 | `try_hashjoin_path` (1222) | `try_hashjoin_path` :1077 | MATCH | ojrelid bail; required_outer gate; cost(parallel_hash=false); precheck with NIL pathkeys → create(false)+add_path. |
| 12 | `try_partial_hashjoin_path` (1299) | `try_partial_hashjoin_path` :1149 | MATCH | asserts + inner-req_outer bail; cost(parallel_hash) + partial precheck NIL → create(NULL)+add_partial_path. |
| 13 | `sort_inner_and_outer` (1357) | `sort_inner_and_outer` :1207 | MATCH | empty-mergeclause early-out; cheapest-total in/out; cross-param bail; UNIQUE_OUTER/INNER unique-ify→INNER; partial-merge eligibility (5 guards); per-pathkey "front first then rest in original order" reorder; cur_mergeclauses/innerkeys/merge_pathkeys; try_merge + conditional try_partial_merge. |
| 14 | `generate_mergejoin_paths` (1564) | `generate_mergejoin_paths` :1351 | MATCH | UNIQUE→INNER; find_mergeclauses; FULL-clauseless corner; useallclauses len gate; innersortkeys; try_merge on cheapest; UNIQUE_INNER early-out; cheapest_startup/total init from pathkeys_contained; sortkeycnt truncation loop with total/startup branches, newclauses reuse, useallclauses break. Always-clone of trialsortkeys is observationally identical to the C list_copy-vs-share optimization. |
| 15 | `match_unsorted_outer` (1812) | `match_unsorted_outer` :1557 | MATCH | RIGHT_SEMI early-out; jointype switch (nestjoinOK/useallclauses, default→elog ERROR mapped to `PgError::error`); ICT cross-param clear; UNIQUE_INNER unique-ify / nestjoinOK material; per-outer-path loop (cross-param skip, UNIQUE_OUTER cheapest-only unique-ify, merge_pathkeys, UNIQUE_INNER nestloop / nestjoinOK param-loop+memoize+matpath, UNIQUE_OUTER continue, ICT-null continue, generate_mergejoin); parallel block (5 guards, parallel nestloop, safe-inner search, parallel mergejoin). |
| 16 | `consider_parallel_mergejoin` (2071) | `consider_parallel_mergejoin` :1779 | MATCH | per partial-outer: build_join_pathkeys + generate_mergejoin(is_partial=true, useallclauses=false). |
| 17 | `consider_parallel_nestloop` (2111) | `consider_parallel_nestloop` :1817 | MATCH | UNIQUE_INNER→INNER; 5-condition materialize; per partial-outer: pathkeys, inner cheapest_parameterized loop (parallel-safe skip, UNIQUE_INNER cheapest-only unique-ify, try_partial_nestloop + memoize), matpath. C deref of non-null ICT modeled with `if let Some` (same behavior under the established invariant). |
| 18 | `hash_inner_and_outer` (2220) | `hash_inner_and_outer` :1900 | MATCH | hashclause selection (outerjoin pushed-down skip, can_join/hashop, sides-match, inner-op-outer commutator); empty→return; cheapest startup/total in/out; cross-param bail; UNIQUE_OUTER/INNER unique-ify; else-branch startup-outer + param×param loop with already-tried skip; partial-hash block (3 guards, parallel-inner shared, safe-inner selection). |
| 19 | `select_mergejoin_clauses` (2501) | `select_mergejoin_clauses` :2123 | MATCH | RIGHT_SEMI→allowed=false; per-clause: pushed-down skip, can_join/mergeopfamilies (const→not nonmergeable), sides-match, commutator, update_eclasses + EC_MUST_BE_REDUNDANT left/right; allowed switch (RIGHT/RIGHT_ANTI/FULL → !have_nonmergeable, else true). |

Helper (not a C function, a macro port): `rinfo_is_pushed_down` :2212 implements
`RINFO_IS_PUSHED_DOWN` = `is_pushed_down \|\| !bms_is_subset(required_relids,
joinrelids)` — MATCH against pathnodes.h:2863.

### Constants / macros verified against headers (not memory)

- `IS_OUTER_JOIN` (nodes.h:344) = LEFT/FULL/RIGHT/ANTI/RIGHT_ANTI → `is_outer_join` :72 MATCH.
- `PATH_REQ_OUTER` (pathnodes.h:1804) → `path_req_outer` :152 MATCH.
- `RINFO_IS_PUSHED_DOWN` (pathnodes.h:2863) → `rinfo_is_pushed_down` MATCH.
- `PATH_PARAM_BY_REL/_SELF/_BY_PARENT` → :160–179 MATCH.

## 2. Seam audit

**Owned seam crates (by C-source coverage):** joinpath.c → `crates/backend-optimizer-path-joinpath-seams`.

`backend-optimizer-util-relnode-seams` is owned by `optimizer/util/relnode.c`
(a different unit); joinpath only *consumes* its `bms_*` declarations. Not in
scope for this unit's `init_seams()`.

**`init_seams()` is a no-op — and this is correct.** Every declaration in
`backend-optimizer-path-joinpath-seams` is an *outward* call into another
subsystem (pathnode.c / costsize.c / pathkeys.c / equivclass.c / joininfo.c /
restrictinfo.c / clauses.c / var.c / placeholder.c / nodeFuncs.c / typcache.c /
lsyscache.c / execAmi.c / FDW + extension hooks). Those declarations are
installed by their *owning* crates when ported, not by joinpath. joinpath has no
inward entry point: `add_paths_to_joinrel` is called directly by the join-search
driver (joinrels.c), not through a seam. So there is nothing for joinpath to
`set()`. No owned-but-uninstalled seam exists; the empty installer is not a
finding here.

**No body was replaced by a "call somewhere else."** All 19 C functions are
present in the crate with their full control flow. The seams reached are genuine
cross-subsystem callees (each its own unit). No MISSING-by-seam.

**Every outward seam is justified + thin.** Each is a marshal (arena handle ↔
node / Relids clone) + one delegate call + result conversion. No branching, node
construction, or computation in any seam path — confirmed by reading all
declarations in joinpath-seams and all 9 in relnode-seams. Cache-key
orchestration (`get_memoize_path` / `extract_lateral_vars_from_PHVs` /
`paraminfo_get_equal_hashops`) lives **in-crate**; only node-payload walks and
`create_memoize_path` cross. The catalog notes a prior audit fixed exactly this
(it had been elided behind a self-owned `get_memoize_path` seam); the current
tree has the logic in-crate — re-verified here from scratch.

All outward seams panic until their owners land; this is the expected pre-owner
state, not a finding. The crate builds clean (`cargo build -p
backend-optimizer-path-joinpath`).

## 3b. Design conformance

- **Mcx + PgResult on allocating functions.** Every fn that allocates transient
  lists / arena paths takes `Mcx<'mcx>` and returns `PgResult<…>`
  (add_paths_to_joinrel, the try_* / sort / generate / consider / hash /
  select / memoize family). Allocation is fallible (the C `palloc` OOM channel).
  Pure predicates return bare bools. Conforms.
- **No ambient-global seams for per-backend GUCs.** `enable_mergejoin /
  _hashjoin / _material / _parallel_hash / _memoize` are passed as an explicit
  `JoinEnableFlags` value, *not* zero-arg getter seams. Conforms to the
  no-ambient-global-seams rule.
- **No invented opacity.** All handles (`PathId`/`RelId`/`RinfoId`/`NodeId`/
  `PhInfoId`) are arena indices into the real `PlannerInfo` (types-pathnodes),
  mirroring the C `Path*`/`RelOptInfo*`/`RestrictInfo*`/`Node*`/`PlaceHolderInfo*`
  pointers — no stand-in types, no void* layering hacks. Conforms (types.md 6-7).
- **No shared statics, no held locks across `?`, no registry side tables.** The
  crate holds no statics; state flows through `&mut PlannerInfo`. None present.
- **Mirror PG + panic for unported deps.** Outward seams panic until owners
  install; structure mirrors the C 1:1. Conforms.
- **`elog(ERROR, "unrecognized join type")`** mapped to `PgError::error` with the
  same message shape (default SQLSTATE for elog ERROR). Conforms.

No design findings.

## Verdict

**PASS.** All 19 functions MATCH (helpers and macros included). Seams are thin
outward marshal+delegate, correctly owned, with a correct no-op `init_seams()`
(no inward entry). Zero seam findings, zero design findings. The unit's
`CATALOG.tsv` row remains `audited`.
