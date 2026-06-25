# Unported-feature inventory

Definitive, ranked inventory of every **genuinely-unported feature** that surfaces as a
faithful seam-and-panic in our tree. Implementation lanes claim rows from here.

- **Built on** origin/main `36a92ccb8`.
- **Impact** = from the r15 regression corpus
  (`/private/tmp/qmeasure_sched/run-7a3503473-r15/*.out`): `occ` = total query-failure
  occurrences, `files` = distinct regress `.out` files that hit the error. Higher = more
  leverage.
- **Type axis (the tier-defining distinction):**
  - **STUBBED-idiomatic** — a real idiomatic body already exists in
    `../pgrust/src-idiomatic/crates/<crate>/src/`. Cheapest: copy + convert to per-owner
    seams + wire `init_seams()`. → **TIER 1**.
  - **STUBBED-c2rust** — only the mechanical `../pgrust/c2rust-runs/<unit>/src/*.rs`
    translation has the body. The C-logic reference exists (de-risks the port) but a full
    idiomatic port is still required. → TIER 2/3 by size.
  - **GENUINELY-UNPORTED** — no body in either tree; port from C
    (`../pgrust/postgres-18.3`). → TIER 2/3 by size.
- **NOT in this file (verified CORRECT PG behavior — do not "fix"):** `ORDER BY` /
  `OFFSET` / `FOR UPDATE` / mutual-recursion *in a recursive query*, `WHERE CURRENT OF` on
  a view, XQuery `x`-flag expanded regexes. Real PostgreSQL 18.3 errors identically on all
  of these (verified against `parse_cte.c`, `rewriteManip.c`, `jsonpath`). They are
  feature-not-supported errors by design, not missing ports.

## Tier counts

- **TIER 1 (STUBBED-idiomatic — wire the existing body):** 9 features, ~146 occ / ~13 files.
- **TIER 2 (small/medium genuine or c2rust-backed, high impact):** 13 features, ~250 occ.
- **TIER 3 (large subsystem ports):** 8 clusters, ~210 occ.

Ranked by impact/size (cheap + high-impact first).

---

## TIER 1 — STUBBED-idiomatic (wire the existing src-idiomatic body; do first)

These have a real idiomatic body in `../pgrust/src-idiomatic`. The work is: copy the body
into the owner crate, split declarations into `<crate>-seams`, install from
`init_seams()`, wire into `seams-init`. No fresh logic.

| Feature | impact (occ / files) | size | owner crate | src-idiomatic body | prereqs |
|---|---|---|---|---|---|
| **ResetTupleHashIterator** (execGrouping iterator reset; = `start_iterate(hashtable.hashtab)`) | 11 / 1 | **small** | backend-executor-execGrouping → backend-executor-nodeAgg | `execGrouping/src/tuplehash.rs` (`start_iterate`/`iterate`/`TuplehashIterator` all exist) | none — substrate present |
| **DefineCollation** (`DefineStmt kind Collation`) | 25 / 3 | medium | backend-commands-collationcmds | `backend-commands-collationcmds/src/lib.rs` (full body) | none |
| **ts_rewrite SPI-cursor leg** (tsquery rewrite via SPI) | 22 / 1 | medium | backend-utils-adt-tsquery-rewrite + backend-executor-spi | `backend-utils-adt-tsquery-rewrite/src/lib.rs` | SPI cursor leg wired |
| **COPY FROM … WHERE** | 10 / — | medium | backend-commands-copyfrom | `backend-commands-copyfrom/src/lib.rs` | none |
| **BeginCopyFrom binary-format input** (`getTypeBinaryInputInfo`) | 1 / 1 | small | backend-commands-copyfrom | `backend-commands-copyfrom/src/lib.rs` | none |
| **JSON_SERIALIZE() / FORMAT JSON / WITH UNIQUE KEYS / RETURNING bytea FORMAT JSON** | 14+11+7+3 / few | small | backend-parser-parse-expr | `types/src/json_parse_gen.rs` + `seams/.../backend_parser_parse_expr.rs:436` | none — parse nodes exist |
| **GET DIAGNOSTICS PG_ROUTINE_OID** | 2 / — | small | backend-pl-plpgsql | `gram.rs` (`K_PG_ROUTINE_OID`) + `funcs.rs` (`PLPGSQL_GETDIAG_ROUTINE_OID`) | none |
| **renameatt_internal** RENAME composite-type attr w/ dependent typed tables | 4 / — | medium | backend-commands-tablecmds | `backend-commands-tablecmds/src/rename.rs` | find_typed_table_dependencies |

TIER-1 total impact ≈ **146 occ across ~13 files** for ~9 cheap wires.

---

## TIER 2 — small/medium genuine ports, high impact

Bodies here are STUBBED-c2rust (mechanical reference exists in `c2rust-runs`) or
GENUINELY-UNPORTED but small. These need a real idiomatic port; the c2rust column tells
you whether a C-logic Rust rendering is available to crib from.

| Feature | impact (occ / files) | size | owner crate | c2rust ref | C source |
|---|---|---|---|---|---|
| **ALTER TABLE INHERIT / NO INHERIT** (ATPrepAddInherit/ATExecAddInherit/ATExecDropInherit) | 47+14 / 14 | medium | backend-commands-tablecmds | tablecmds.rs:36200 / :37673 | tablecmds.c |
| **ALTER COLUMN SET EXPRESSION** (ATExecSetExpression + ATRewriteTable) | 29 / 4 | medium | backend-commands-tablecmds | tablecmds.rs:20651 | tablecmds.c |
| **addFkRecurseReferenced** partitioned-table recursion | 24 / 4 | medium | backend-commands-tablecmds | tablecmds.rs:43107 | tablecmds.c |
| **extended-stats: stxexprs decode** (compute_expr_stats / serialize_expr_stats) | 16 / 1 | medium | backend-statistics-extended-stats | src-idiomatic `extended-stats/src/lib.rs` partial | extended_stats.c |
| **ALTER COLUMN ADD/SET/DROP IDENTITY** (ATExecAddIdentity etc + ATParseTransformCmd) | 20+9+8 / 2 | medium | backend-commands-tablecmds | tablecmds.rs:19495/19941/20315 | tablecmds.c |
| **SET LOGGED / SET UNLOGGED** (ATPrepChangePersistence) | 16 / 3 | medium | backend-commands-tablecmds | tablecmds.c ATPrepChangePersistence (287 ln); prep already in `at_phase.rs:676` | tablecmds.c |
| **VALIDATE CONSTRAINT (CHECK phase-3 revalidation)** | 15 / 4 | medium | backend-commands-tablecmds | tablecmds.rs ATExecValidateConstraint (243 ln) | tablecmds.c |
| **SET TABLESPACE** (ATPrepSetTableSpace/ATExecSetTableSpace) | 10 / 2 | small | backend-commands-tablecmds | partitioned-catalog leg in `at_phase.rs:1137`; storage leg in c2rust | tablecmds.c |
| **SET ACCESS METHOD** (ATPrepSetAccessMethod) | 10 / 2 | medium | backend-commands-tablecmds | partitioned leg `at_phase.rs:1123`; ATExecSetAccessMethodNoStorage c2rust | tablecmds.c |
| **OF / NOT OF** (ATExecAddOf/ATExecDropOf) | 10 / — | medium | backend-commands-tablecmds | tablecmds.c (401+102 ln) | tablecmds.c |
| **ALTER COLUMN DROP EXPRESSION / SET COMPRESSION** | 12+5 / — | medium | backend-commands-tablecmds | tablecmds.c ATExecDropExpression(278)/SetCompression(138) | tablecmds.c |
| **ALTER CONSTRAINT [NO] INHERIT** (ATExecAlterConstrInheritability) | 9 / — | medium | backend-commands-tablecmds | tablecmds.rs:28125 (114 ln) | tablecmds.c |

---

## TIER 3 — large subsystem ports

High-occ but large/keystone-blocked. Each is a campaign, not a single wire.

| Feature | impact (occ / files) | size | owner crate | type | C source / blocker |
|---|---|---|---|---|---|
| **Trigger firing front-half** (MakeTransitionCaptureState, ri_TrigWhenExprs WHEN-qual ExprState, GetTupleForTrigger = table_tuple_lock/heap_fetch/EvalPlanQual) | 61 / 5 | large | backend-commands-trigger | STUBBED-c2rust | trigger.c — the firing-front substrate; gates almost all row-trigger work |
| **REINDEX CONCURRENTLY** (ReindexRelationConcurrently) | 47 / 8 | large | backend-commands-indexcmds | GENUINELY-UNPORTED | indexcmds.c (`deferred.rs` panic) — needs concurrent index build + validate phases |
| **FOR EACH ROW triggers on partitioned tables** + CloneRowTriggersToPartition | 36+5 / 2 | medium | backend-commands-trigger / tablecmds | STUBBED-c2rust | trigger.c; tablecmds.rs:43566 — prereq: trigger firing front-half |
| **AfterTriggerFireDeferred** (deferred-trigger firing at commit; afterTriggerInvokeEvents + per-query EState + PushActiveSnapshot) | 18 / 3 | medium | backend-commands-trigger | STUBBED-c2rust | trigger.c:12545 — prereq: trigger firing front-half |
| **CopyFrom into table with triggers / FDW / partition routing** | 18+7+1 / 3 | large | backend-commands-copyfrom | STUBBED-idiomatic (front-half present) + trigger dep | copyfrom.c — prereq: trigger firing front-half |
| **extended MCV statistics build** (statext_mcv_build/build_mss/build_distinct_groups/build_column_frequencies) | 21 / 2 | large | backend-statistics-mcv | STUBBED-idiomatic | `backend-statistics-mcv/src/lib.rs` exists but large — promote toward TIER 1 once verified wireable |
| **DETACH PARTITION (with partitioned indexes / CONCURRENTLY)** | 20+3 / 4 | large | backend-commands-tablecmds | STUBBED-c2rust | tablecmds.rs:43839 |
| **ALTER COLUMN TYPE recurse to inheritance child / ATTypedTableRecursion** + ALTER TYPE composite ADD/DROP ATTRIBUTE | 12+12+7+6 / few | large | backend-commands-tablecmds | STUBBED-c2rust (recurse) / GENUINELY-UNPORTED (typed-table recursion) | tablecmds.c / typecmds.c |

### Smaller residual genuine ports (TIER 2/3 boundary)

| Feature | occ | owner | type |
|---|---|---|---|
| ATTACH/INHERIT merging inherited CHECK constraint | 13 | tablecmds | GENUINELY-UNPORTED |
| addFkRecurseReferencing partitioned recursion | 24 | tablecmds | STUBBED-c2rust |
| recurse_set_operations result reprojection (apply_projection_to_path) | 2 | prepunion | GENUINELY-UNPORTED (bignode keystone) |
| fmgr_sql: utility statements in SQL functions | 7 | backend-executor-functions | GENUINELY-UNPORTED |
| make_tuple_indirect (indirect-TOAST VARATT_INDIRECT substrate) | 1 | detoast/regress | GENUINELY-UNPORTED |
| RETURNING WITH (OLD/NEW AS …) executor leg (PG18) | 6 | nodeModifyTable | STUBBED-idiomatic parse/analyze; executor only INSERT |
| ALTER TABLE ADD COLUMN constrained-domain no default (CoerceToDomain(NULL)) | 1 | tablecmds | STUBBED-idiomatic (parser-transform leg seamed) |
| ATTACH PARTITION of/under TEMP rel; onto parent with row triggers | 4+1 | tablecmds | STUBBED-c2rust |

---

## Notes for implementation lanes

1. **`backend-commands-tablecmds` is the dominant owner** (~150 panic-stubs in our tree;
   ~14 distinct ALTER-TABLE features in the r15 corpus, ~250 occ combined). It has the
   highest aggregate leverage, but bodies are mostly **c2rust-only** (mechanical) — each AT
   sub-command is an independent medium port. Claim them individually.
2. **Trigger firing front-half (61 occ)** is the single highest-occ keystone and unblocks a
   cascade: partitioned row triggers, deferred triggers, COPY-with-triggers, FK enforcement
   front. Port it first among TIER-3.
3. **TIER-1 is the cheapest leverage**: ~146 occ for ~9 copy-and-wire jobs. Start with
   ResetTupleHashIterator (small, substrate present), then DefineCollation and the JSON
   parse-expr cluster.
4. The c2rust line numbers are in `../pgrust/c2rust-runs/<unit>/src/<file>.rs`; the C is in
   `../pgrust/postgres-18.3/src/backend/...`.
