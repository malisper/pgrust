# Audit: backend-utils-adt-network-selfuncs

- **Date:** 2026-06-15
- **Model:** Opus (Opus 4.8, 1M)
- **Verdict:** PASS
- **C source:** `src/backend/utils/adt/network_selfuncs.c` (PostgreSQL 18.3)
- **Port crate:** `crates/backend-utils-adt-network-selfuncs` (`src/lib.rs`)

Selectivity estimators for the `inet`/`cidr` subnet inclusion (`<<`, `<<=`,
`>>`, `>>=`) and overlap (`&&`) operators. Every function in the C file is
ported branch-for-branch. The src-idiomatic base
(`src-idiomatic/crates/backend-utils-adt-network-selfuncs`) was used as the
reference and reconciled to this repo's value/seam model: explicit `Mcx<'mcx>`
threading, the repo's `VariableStatData`/`AttStatsSlot` (`nvalues` →
`values.len()`, `AttStatsSlot.numbers`), the lsyscache `get_attstatsslot` /
`get_opcode` / `get_commutator` seams, the fmgr `function_call2_coll` seam (the
cached `FmgrInfo` is faithfully replaced by re-resolving the opcode OID per
call — `get_opcode` once, then `function_call2_coll(opcode, ...)`), and a
`VarStatsGuard` RAII guard for `ReleaseVariableStats`.

## Function inventory

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `networksel` (78) | `networksel` | MATCH | opr guard → get_restriction_variable punt → IsA(Const)/constisnull/strict → statsTuple punt → stanullfrac → mcv_selectivity → histogram via get_attstatsslot with h_codenum commute → `mcv + (1-null-sumcommon)*non_mcv` → CLAMP. Const value pulled to bare word for the fmgr/detoast lane (range-selfuncs idiom). |
| `networkjoinsel` (203) | `networkjoinsel` | MATCH | opr guard → get_join_variables → jointype switch (INNER/LEFT/FULL→inner; SEMI/ANTI→semi w/ reversed get_commutator+`-opr_codenum`; default→elog) → CLAMP. RAII guards release both vardata. |
| `networkjoinsel_inner` (282) | `networkjoinsel_inner` | MATCH | per-side stanullfrac + MCV/HIST slots, mcv_length = Min(nvalues,1024), mcv_population; MCVxMCV, MCVxHIST (both directions, second commuted), HISTxHIST scaled sums; no-stats default branch. |
| `networkjoinsel_semi` (406) | `networkjoinsel_semi` | MATCH | same slot prologue; hist2_weight = `(1-null-sumcommon)*rel->rows` via `root.rel(rel_id).rows`; LHS-MCV loop; LHS-HIST decimated loop `k=(nv-3)/1024+1`, `i in 1..nv-1`, `selec += (1-null-sumcommon)*sum/n`; no-stats default. |
| `mcv_population` (553) | `mcv_population` | MATCH | sum of first `mcv_nvalues` numbers. |
| `inet_hist_value_sel` (618) | `inet_hist_value_sel` | MATCH | nvalues<=1→0; k=(nv-2)/1024+1; query+left detoast; bucket loop: both 0→full match; bracketing→`1/2^max(left_div,right_div)` when a divider≥0; shift; `match/n`. |
| `inet_mcv_join_sel` (687) | `inet_mcv_join_sel` | MATCH | O(N²) FunctionCall2(opcode,...) over MCV pairs, `selec += num1*num2` on true. |
| `inet_mcv_hist_sel` (719) | `inet_mcv_hist_sel` | MATCH | commute opr_codenum, `selec += num[i]*inet_hist_value_sel(hist, mcv[i], opr)`. |
| `inet_hist_inclusion_join_sel` (756) | `inet_hist_inclusion_join_sel` | MATCH | hist2_nvalues<=2→0; k=(nv-3)/1024+1; interior loop `match += inet_hist_value_sel(...)`; `match/n`. |
| `inet_semi_join_sel` (807) | `inet_semi_join_sel` | MATCH | MCV scan→1.0 on hit; else hist+weight>0 → `Min(1, weight*inet_hist_value_sel(hist, lhs, -opr))` when >0; else 0. `FmgrInfo*` carried as opcode OID. |
| `inet_opr_codenum` (853) | `inet_opr_codenum` | MATCH | sup=-2, supeq=-1, overlap=0, subeq=1, sub=2; default elog. |
| `inet_inclusion_cmp` (896) | `inet_inclusion_cmp` | MATCH | same family→bitncmp over Min(bits); nonzero returned; else masklen cmp. Diff family→family diff. |
| `inet_masklen_inclusion_cmp` (922) | `inet_masklen_inclusion_cmp` | MATCH | order=bits(l)-bits(r); accept rules `(>0&&≥0)\|(==0&&-1..1)\|(<0&&≤0)`→0; else opr_codenum. |
| `inet_hist_match_divider` (956) | `inet_hist_match_divider` | MATCH | same family && masklen cmp==0: decisive_bits by sign of opr; `decisive - bitncommon(...,min_bits)` when min_bits>0 else decisive_bits; else -1. |

## Deferred edges (seam-and-panic, mirror-PG-and-panic)

All are the unported-selfuncs-owner / fmgr-varlena-envelope boundary, identical
in kind to the already-merged `backend-utils-adt-range-selfuncs`:

- `backend-utils-adt-selfuncs-seams`: `get_restriction_variable`,
  **`get_join_variables`** (new), **`mcv_selectivity`** (new),
  `release_variable_stats`, `stats_tuple_stanullfrac` — owner `selfuncs.c`
  (examine/estimate F1-F7) unported; uninstalled, panic on call. The two new
  seams were declared on the owner `-seams` crate (the C signatures:
  `get_join_variables(root,args,sjinfo,&v1,&v2,&rev)`,
  `mcv_selectivity(vardata,opproc,collation,constval,varOnLeft,&sumcommon)`).
- `backend-utils-adt-network-seams::inet::datum_get_inet_pp` (new): `DatumGetInetPP`
  varlena detoast of a `pg_statistic` value / query `Const` word. `network.c`
  itself defers the fmgr/varlena envelope (its functions take decoded
  `inet_struct`s), so no ported unit installs it; uninstalled, panic on call.

## Installed edges (reachable now)

- `get_attstatsslot` (lsyscache) — installed.
- `get_opcode` / `get_commutator` (lsyscache) — installed.
- `function_call2_coll` (fmgr-core) — installed.
- `bitncmp` / `bitncommon` — direct dep on the ported `backend-utils-adt-network`.

## Constants verified

- Operator OIDs 931 (`<<`), 932 (`<<=`), 933 (`>>`), 934 (`>>=`), 3552 (`&&`)
  vs `pg_operator.dat`.
- `STATISTIC_KIND_MCV=1`, `STATISTIC_KIND_HISTOGRAM=2` vs `pg_statistic.h`.
- `DEFAULT_OVERLAP_SEL=0.01`, `DEFAULT_INCLUSION_SEL=0.005`,
  `MAX_CONSIDERED_ELEMS=1024`.

## Notes / faithful reconciliations

- `nvalues` parameters dropped where the C value equals the slice length
  (`hslot.nvalues` == `values.len()` for a `get_attstatsslot` result);
  `inet_hist_value_sel` / `inet_hist_inclusion_join_sel` compute it internally,
  preserving the exact guards (`<=1`, `<=2`).
- The C `FmgrInfo proc` (resolved once via `fmgr_info(get_opcode(operator))`) is
  modeled as the opcode OID threaded into `function_call2_coll`/`mcv_selectivity`;
  the seam re-resolves per call — behavior identical (the FmgrInfo only caches
  the opcode lookup).
- `vardata2->rel->rows` resolved as `root.rel(rel_id).rows` (the repo's `rel`
  field is a `RelId` index, not a `RelOptInfo*`); `root` is threaded into
  `networkjoinsel_semi` for this.
- `ReleaseVariableStats` / `free_attstatsslot` are RAII: `VarStatsGuard::drop`
  and `AttStatsSlot::drop`.

## Tests

16 unit tests pass: all pure comparators/divider/`mcv_population`/`clamp`/
`default_sel`/`opr_codenum` parity, plus the `inet_hist_value_sel` kernel
end-to-end via an installed test detoast seam (too-few/full-bucket/no-match),
and the `networksel` operator guard.

`residual_own_todos = 0` (no `todo!`/`unimplemented!`/own-logic stubs).
