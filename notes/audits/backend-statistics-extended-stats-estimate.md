# Audit: backend-statistics-extended-stats — estimate/consume leg

Scope: the estimate-side functions added in the stats-ext estimate lane
(`src/estimate.rs`). The ANALYZE/build-side functions in `src/lib.rs` were
audited when the crate originally landed and are unchanged by this lane.

C sources: `src/backend/statistics/extended_stats.c`,
`src/backend/statistics/dependencies.c` (estimate side); c2rust rendering under
`c2rust-runs/statistics_*`.

## Function table (estimate leg)

| C function (file:line) | Port (estimate.rs) | Verdict | Notes |
|---|---|---|---|
| `statext_clauselist_selectivity` (extended_stats.c:1981) | `statext_clauselist_selectivity` | MATCH (MCV leg SEAMED-pending) | MCV leg first then FD. The MCV match engine (`mcv_get_match_bitmap`, fmgr per-item eval) is owned by the MCV crate's match dispatcher and is not yet landed; until then the MCV leg contributes its neutral identity (1.0 AND / 0.0 OR) — the exact result a rel with no MCV stats yields (`has_stats_of_kind(MCV)==false`). FD leg fully ported. is_or short-circuit matches C. |
| `statext_mcv_clauselist_selectivity` (extended_stats.c:1693) | (neutral) | SEAMED-pending | Larger fmgr-coupled body; its own lane. Neutral identity until landed (see above). |
| `dependencies_clauselist_selectivity` (dependencies.c:1369) | `dependencies_clauselist_selectivity` | MATCH | has_stats_of_kind gate; per-clause attnum extraction (column + expression paths); attnum offset; BMS_MULTIPLE reject; stat loading with per-dep remap; greedy strongest-dependency loop; clauselist_apply_dependencies. |
| `dependency_is_compatible_clause` (dependencies.c:741) | `dependency_is_compatible_clause` | MATCH | RestrictInfo prologue (pseudoconstant + singleton clause_relids) hoisted to the driver's per-clause loop (we hold RinfoId, not a bare RestrictInfo node). OpExpr/SAOP/OR/NOT/bare-bool arms; RelabelType strip; Var varno/varlevelsup/user-attr checks; get_oprrest==F_EQSEL (OID 101). |
| `dependency_is_compatible_expression` (dependencies.c:1167) | `dependency_is_compatible_expression` | MATCH | Same arm structure; matches operand against the rel's dependency-stat expressions via `equal`. Empty in practice (stxexprs build leg deferred → stat exprs empty) but ported in full. |
| `clauselist_apply_dependencies` (dependencies.c:1013) | `clauselist_apply_dependencies` | MATCH | attnum set from all dep attributes; per-attnum simple selectivity via the `clauselist_selectivity` (RinfoId) seam, marking estimatedclauses; member-index mapping; combination kernel delegated to `backend-statistics-dependencies::combine_dependency_selectivities` (already audited). |
| `examine_opclause_args` (extended_stats.c:2032) | `strip_relabel` + inline Const/Expr split | MATCH | The Expr/Const split with RelabelType stripping; used inline in the compatibility tests (as in C, which calls it from statext_is_compatible_clause_internal / mcv_get_match_bitmap — MCV side pending). |
| `statext_dependencies_load` (dependencies.c:601) | `statext_dependencies_load` | MATCH | Ported over the crate's existing `table_open`/`genam`/`heap_deform_tuple` substrate (SysCache unported). Row keyed `(stxoid, stxdinherit)` via `pg_statistic_ext_data_stxoid_inh_index`. Errors on missing row / NULL column (C's two elogs). VARSIZE_ANY_EXHDR body stripped before `statext_dependencies_deserialize` (already audited). |
| `find_strongest_dependency` (dependencies.c:928) | `deps::find_strongest_dependency` | SEAMED (in-repo) | Lives in `backend-statistics-dependencies` (already audited); driven from here. |
| `combine_dependency_selectivities` / kernel (dependencies.c:1104-1151) | `deps::combine_dependency_selectivities` | SEAMED (in-repo) | Same; already audited. |

## Helpers (no direct C counterpart — faithful renderings)

- `collect_dependency_stat_exprs` / `collect_stat_oids`: the C inlines these
  inside `dependencies_clauselist_selectivity` (the `foreach(rel->statlist)`
  loop counting matching attrs + exprs, gating `nmatched+nexprs >= 2`).
- `remap_dependencies`: the C per-statistics attnum-offset/expression-translate
  block (dependencies.c:1657-1758), compacting kept deps.
- `bms_del_member`: clears one bit over the public `Bitmapset.words` (no relnode
  del-member seam); normalizes empty→None, matching `bms_del_member`.
- `relids_to_vec`/`varlena_body`/`clamp_probability`: thin local renderings.

## Seam / wiring audit

- Installs `backend_optimizer_path_small_seams::statext_clauselist_selectivity`
  in this crate's `init_seams()` (the seam was declared+called by path-small but
  uninstalled — would panic; now installed). seams-init aggregator already calls
  this crate's `init_seams()`.
- Outward calls are all thin marshal+delegate over real dependency cycles:
  `clauselist_selectivity` (RinfoId), `is_pseudo_constant_clause` (path-small),
  `get_oprrest` (lsyscache), `equal`/`get_notclausearg` (nodeFuncs), the
  `relids_*` bms ops (relnode), `genam`/`table_open`/`heaptuple` (catalog scan),
  `statext_dependencies_deserialize`/`find_strongest_dependency`/
  `combine_dependency_selectivities` (dependencies crate). No logic in seam
  closures.

## Companion fix (path-small / costsize)

- `clauselist_selectivity_rinfos` seam (costsize-seams) + body (path-small)
  installed; `set_baserel_size_estimates` now passes `rel->baserestrictinfo`
  (RestrictInfo list) instead of bare nodes, matching C — required so
  `find_single_rel_for_clauses` and the extended-stats path apply.
- `boolexpr_args_as_entries` now maps `Expr::RestrictInfo` OR args to
  `ListEntry::Rinfo` (the `rinfo->orclause` sub-RestrictInfo variant), fixing a
  pre-existing latent "unrecognized node type" on OR-in-AND clauses that the
  RestrictInfo baserel path exposed.

## Verdict: PASS (FD/estimate leg)

Every FD/estimate function MATCHes the C; the MCV match-engine leg is a separate
unported owner contributing its neutral identity (not absent logic in this
crate — the FD result is independent and correct). No seam findings; no design
violations (allocating fns take `Mcx`/return `PgResult`; no shared statics, no
ambient-global seams, no locks across `?`).

Verified end-to-end: `CREATE STATISTICS (dependencies) ON a,b; ANALYZE;
EXPLAIN ... WHERE a=1 AND b=1` improves rows 11→990 (PG 18.3: ~970), session
survives. stats_ext.sql: 987 → 771 difflines (−216), zero new errors.
