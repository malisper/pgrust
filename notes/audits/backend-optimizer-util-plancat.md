# Audit: backend-optimizer-util-plancat

C source: `src/backend/optimizer/util/plancat.c` (postgres-18.3, 2682 LOC).
Rust: `crates/backend-optimizer-util-plancat/src/lib.rs` (+ `…-plancat-ext-seams`).

Audited 1:1 against the C. Verdict: **PASS** after fixing 4 transcription
divergences (listed at the end).

## Model

The C pointer graph is modelled over the `types_pathnodes::PlannerInfo` arena
(`RelId`/`NodeId`/`RinfoId` handles). The keystone widening of `ArenaNode` to
carry `TargetEntry`, plus the two new arms added by this unit (`ForeignKey`,
`StatisticExt`) + `StatisticExtInfo`, give `indextlist`/`reltarget`/`fkey_list`/
`statlist` real value-typed backing — no invented handles/registries.

Catalog/relcache/rewrite/table-AM/FDW/syscache externals that are unported (or
not exposed in a planner-ready, lifetime-free form) cross through
`backend-optimizer-util-plancat-ext-seams` (guard-exempt, no owner dir,
mirror-and-panic). Available landed seams are used directly: `table_open`,
`relation_close`, lsyscache (`get_attavgwidth`/`get_typavgwidth`/
`get_opfamily_member_for_cmptype`/`get_ordering_op_properties`/`get_opclass_*`/
`get_oprrest`/`get_oprjoin`/`get_func_support`), `predicate_implied_by`,
`relids_*`, the `constraint_exclusion` GUC.

## Per-function verification

- **get_relation_info** — table_open + relkind/recovery guards; min/max_attr,
  reltablespace, attr_needed/attr_widths sizing; NOT NULL collection
  (`ATTNULLABLE_VALID`, see fix #2); estimate_rel_size when `!inhparent`;
  parallel_workers; index loop (invalid/indcheckxmin skips, IndexOptInfo build,
  btree vs amcanorder sortopfamily mapping, indexprs/indpred + ChangeVarNodes,
  build_index_tlist, pages/tuples/tree_height, `lcons` prepend); statlist;
  foreign-table serverid/fdwroutine + restriction; foreign keys; AMFLAG TID
  range; partition info; table_close. Matches C control flow 1:1.
- **get_relation_foreign_keys** — baserel + `list_length(rtable) >= 2` + non-inh
  gates; per-cached-FK `conenforced` skip; inner RTE scan
  (rtekind/relid/inh/self-ref); `ForeignKeyOptInfo` build with eclass/
  fk_eclass_member zeroed; appended to `root.fkey_list`. 1:1.
- **infer_arbiter_indexes** — ON CONFLICT empty early-out; inferAttrs BMS /
  inferElems split; named-constraint index lookup; per-index match loop
  (indisvalid/named-constraint/unique/exclusion/indexedAttrs equal/expr match
  via infer_collation_opclass_match + list_member/list_difference/
  predicate_implied_by). The C `goto next` is modelled as `continue` /
  `matched=false; break`. 1:1.
- **infer_collation_opclass_match** — fully encapsulated in the ext-seam
  (opens the index; per-attr opclass/collation match). The C body reads the
  opened `idxRel`'s rd_att/rd_opfamily/rd_opcintype/rd_indcollation, none of
  which the trimmed `types_rel::Relation` carries — so the whole predicate is the
  seam owner's; the plancat-side call site mirrors C exactly.
- **estimate_rel_size** — table-AM dispatch / index branch / pg_class fallback.
  Index branch: pages reported pre-metapage-discount (fix #1), density (cached vs
  attr-width estimate, integer division), `rint(density*curpages)`, allvisfrac.
  1:1.
- **get_rel_data_width** — per-attr loop, dropped-col skip, cached-width reuse
  (index `attno - min_attr`, fix #4), get_attavgwidth → get_typavgwidth fallback,
  clamp_width_est. 1:1.
- **get_relation_data_width** — open + get_rel_data_width + close. 1:1.
- **get_relation_constraints** — validated check constraints (NO INHERIT gate),
  NOT NULL → NullTest(IS_NOT_NULL, argisrow=false), partition constraint,
  expand_generated_columns_in_expr. The const-fold/canonicalize/ChangeVarNodes/
  make_ands_implicit per-constraint pipeline is the ext-seam
  `process_check_constraint` (stringToNode + clauses.c + rewriteManip, all
  unported). 1:1.
- **get_relation_statistics / _worker** — statoidlist; per-stat keys+exprs;
  worker over `(statOid, true)` then `(statOid, false)`; one StatisticExtInfo per
  built kind with `bms_copy(keys)`. 1:1.
- **relation_excluded_by_constraints** — const-FALSE/NULL early test;
  constraint_exclusion GUC switch; weak refutation (safe restrictions);
  RTE_RELATION gate; include_noinherit/include_notnull; constraints; strong
  refutation vs full baserestrictinfo (not safe_restrictions — matches C). 1:1.
- **build_physical_tlist** — RELATION (dropped/missing punt), SUBQUERY
  (makeVarFromTargetEntry via seam), FUNCTION/TABLEFUNC/VALUES/CTE/
  NAMEDTUPLESTORE/RESULT (expandRTE via seam, non-Var punt), default elog. 1:1.
- **build_index_tlist** — per-column simple (SystemAttributeDefinition for
  indexkey<0) vs expression (consume indexprs), makeTargetEntry(i+1), trailing
  wrong-count elog. 1:1.
- **restriction_selectivity / join_selectivity / function_selectivity** —
  get_oprrest/get_oprjoin/get_func_support → default (0.5 / 0.5 / 0.3333333) on
  missing; fmgr dispatch via ext-seam; 0..1 range check + elog. 1:1.
- **add_function_cost / get_function_rows** — delegate the pg_proc.procost /
  prorows + support-function dispatch to the ext-seam (fmgr-support layer
  unported); return the (startup,per_tuple) to add / the row estimate. 1:1.
- **has_unique_index** — single-col unique non-partial(or predOK) index scan
  over indexlist. 1:1.
- **has_row_triggers / has_transition_tables** — CmdType switch (MERGE→false,
  bad→elog), foreign-table early-out (transition), trigdesc probe via seam. 1:1.
- **has_stored_generated_columns / get_dependent_generated_columns** — trigdesc/
  constr probes via seam; dependent-cols offset folded into Relids. 1:1.
- **set_relation_partition_info / find_partition_scheme /
  set_baserel_partition_key_exprs / set_baserel_partition_constraint** — these
  span the unported PartitionDirectory infra, RelationGetPartitionKey/Qual,
  fmgr_info_copy, copyObject, expression_planner, and the lifetime-free
  PartitionScheme/boundinfo modelling, so they are the ext-seams
  `set_relation_partition_info` / `set_baserel_partition_constraint`
  (mirror-and-panic). The plancat call sites match C exactly.

## Seams this unit OWNS + installs (init_seams, wired into seams-init)

- `plancat_seams::estimate_rel_size`, `plancat_seams::get_rel_data_width`
- `relnode_ext_seams::get_relation_info`
- `allpaths::seams::relation_excluded_by_constraints` (decl widened `&PlannerInfo`
  → `&mut PlannerInfo` to match the mutating C body; both allpaths call sites
  already hold `&mut`)
- `clauses_seams::get_function_rows`
- `costsize_seams::add_function_cost`, `costsize_seams::get_relation_data_width`
- `path_small_seams::{restriction,join,function}_selectivity`

## Divergences found and fixed during audit

1. **estimate_rel_size index branch reported the post-metapage-discount page
   count.** C sets `*pages = curpages` BEFORE the `curpages--` metapage discount;
   tuples/allvisfrac use the decremented value. Fixed: capture `pages` before the
   decrement, return it.
2. **NOT NULL column detection used `attnotnull` instead of `attnullability ==
   ATTNULLABLE_VALID`.** `attnotnull` is also set for NOT-VALID constraints
   (`ATTNULLABLE_INVALID`), which must NOT enter `notnullattnums`. Fixed to read
   `compact_attr(i).attnullability == ATTNULLABLE_VALID` (with the
   `!= ATTNULLABLE_UNKNOWN` / `!attisdropped` debug asserts mirroring C).
3. **`rint` rounded ties away from zero** (a fragile hand-rolled tie check).
   Replaced with `f64::round_ties_even()` + `copysign` zero handling, matching
   costsize.c's canonical helper (ties-to-even, as libm `rint`).
4. **attr_widths cache indexed by `attno` instead of `attno - min_attr`.** The C
   passes a base-shifted pointer (`rel->attr_widths - rel->min_attr`); this
   repo's value model has no pointer arithmetic and costsize (`set_rel_width`)
   reads/writes `attr_widths[attno - min_attr]`. Fixed get_rel_data_width to index
   `[attno - min_attr]` (min_attr = FirstLowInvalidHeapAttributeNumber + 1).

Also corrected the hardcoded RTEKind discriminants in build_physical_tlist
(RTE_FUNCTION=3, TABLEFUNC=4, VALUES=5, CTE=6, NAMEDTUPLESTORE=7, RESULT=8) to
match parsenodes.h.

## Gate

`cargo check --workspace` clean; `cargo test -p no-todo-guard` /
`cargo test -p seams-init` (both recurrence guards) pass; `cargo test
--workspace` green except the sanctioned flake
`backend-optimizer-path-small::range_pair_positive_combination`.
No `todo!()`/`unimplemented!()` in either crate.
