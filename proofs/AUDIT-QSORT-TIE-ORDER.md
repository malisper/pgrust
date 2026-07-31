# AUDIT: qsort tie-order divergence (pg_qsort vs Rust sorts)

Date: 2026-07-30. Base: origin/main 69c3c7eb90. Branch: audit/qsort-tie-order.
Scope: read-only audit; no fixes, no ledger changes.

## 1. Mechanism — confirmed

`src/include/port.h:478` (`#define qsort(a,b,c,d) pg_qsort(a,b,c,d)`) routes every
backend `qsort` to `src/port/qsort.c`, which instantiates
`src/include/lib/sort_template.h`: the NetBSD Bentley–McIlroy quicksort
(median-of-3 / med3-of-9 pivot, presorted check, recurse-on-smaller, insertion
sort below n=7). No randomness, no address-dependent behavior — **fully
deterministic for a given input array + comparator**, identical across
platforms and libc versions. It is NOT stable for n>=7: for a non-total
comparator its tie order is a fixed but "scrambled" permutation of input
order. For n<7 it degenerates to insertion sort, which IS stable.

Consequences for pgrust:
- `sort_unstable_by` with the same non-total comparator produces a DIFFERENT
  (also deterministic) tie order than pg_qsort.
- Stable `sort_by` does NOT match pg_qsort either for n>=7 — pg_qsort is not
  stable there. (Below 7 elements a stable Rust sort DOES match C exactly;
  several small-array sites are shielded by this in practice but not by
  contract.)
- The only way to match C's tie order at all sizes is a faithful pg_qsort port.

Empirical (standalone pg_qsort compiled from the PG 18.3 sort_template.h vs
rustc, 1000 elements, keys = LCG mod 10 so ~100-way ties, identical input;
harnesses in session scratchpad `qsort_test/`):

| comparison | keys order | full (key, orig-index) order |
|---|---|---|
| pg_qsort run 1 vs run 2 | identical | **identical (deterministic)** |
| pg_qsort vs Rust `sort_unstable_by` | identical | differs at **793/1000** positions |
| pg_qsort vs Rust stable `sort_by` | identical | differs at **667/1000** positions |

So "both are unstable, therefore tie order is arbitrary" is wrong as a
*mechanism* claim: both orders are deterministic; they are deterministically
DIFFERENT. Whether that matters is purely a question of where the order is
observable — audited below.

A faithful Rust pg_qsort already exists in-tree — **six times over** (§5).

## 2. Critical calibration: which C surfaces are even deterministic?

Several of the surfaces people first look at are intentionally nondeterministic
in C itself:

- **GiST insertion**: `gistchoose()` (gistutil.c:414-430) uses `pg_prng` to
  randomly break equal-penalty subtree choices. Empirical (postgres:18
  docker): a 3k-row tsquery GiST (the size regime of the triggering finding)
  built 5x from identical data gave **14/12/14/14/14 pages**; 20k tsquery gave
  89/87/88/84/86; 20k tsvector gave 153 then 152. GiST insert-built page
  layout has NO C-identity target — C does not agree with itself.
- **SP-GiST**: `spgdoinsert.c` uses `pg_prng` on the allTheSame overflow path
  (:2200-2215) and random node split (:583, :896). Empirically size-stable in
  the shapes tested (30k dup-coord kd builds 2662400 x3; 30k identical-point
  builds 3309568 x3), but not guaranteed deterministic.
- **nbtree**: the "get tired" prng (nbtinsert.c:971) applies only to pre-v4
  `!heapkeyspace` indexes; modern PG 18 insertion is deterministic.
  Empirical: 50k-row insert-path B-tree build, 3 identical runs →
  `pg_relation_size` 581632 each time. **B-tree layout IS a deterministic C
  surface.**
- **ANALYZE / extended stats**: computed from a prng-drawn sample → not
  C-reproducible run-to-run at scale. Caveat: when the table fits the sample
  (small tables), the sample is effectively the whole table and the pipeline
  is deterministic — tie-order divergence in stats IS reproducibly visible
  there.

Direct consequence for the triggering finding: the `gtsquery_picksplit`
"pgrust 14 pages vs C 13 pages" comparison measured a surface where C itself
varies 12–14 pages run-to-run. The lane's non-surface conclusion stands, but
for a different reason than the one it gave: not "both sorts are unstable"
(false as stated) but "C GiST insert layout is prng-randomized" (true).

## 3. Call-site census

Full sweep of `qsort` / `qsort_arg` / `qsort_interruptible` call sites in the
PG 18.3 backend (~115 sites audited across ~80 files) intersected with the
ported tree. Headline counts:

- **NO EXPOSURE — comparator total or ties byte-identical**: majority (~75
  sites): TID/offset/oid/xid/LSN/blockno sorts, partition bounds (DDL-unique),
  GUC/constraint names, jsonb pairs (explicit `order` tie-break), acl items
  (all fields compared), dup-then-dedup sorts of identical values, sites with
  explicit unique tie-breaks reproduced in Rust (pruneheap offset, heapam
  ifirsttid, syncrep walsnd_index, indxpath argindex, tsrank compareDocR).
- **NO EXPOSURE — already C-exact via in-tree pg_qsort ports (6 copies)**:
  tuplesort (`utils/sort/tuplesort/src/qsort.rs`, tie-order pinning tests
  `mksort_ties_match_pgqsort_order`); gistproc (3 sites);
  brin_minmax_multi (4); rangetypes_gist (4) + rangetypes_spgist (2, re-export);
  commands/analyze `compute_scalar_stats`; statistics sortitem (mcv x3,
  mvdistinct, extended_stats — all 5 non-total sites bit-exact).
- **EXPOSED-UNOBSERVABLE**: ~20 sites (selected justifications §3.3).
- **EXPOSED-OBSERVABLE**: **15 sites/groups** (§3.1) + 4 message/scheduling
  borderline (§3.2).

### 3.1 EXPOSED-OBSERVABLE — ranked by likelihood someone notices

Tier 1 — deterministic C surface, ordinary workloads:

| rank | C site | pgrust site + primitive | what a user sees |
|---|---|---|---|
| 1 | `nbtsplitloc.c:587` `_bt_splitcmp` on int16 `curdelta` (ties routine) | `access/nbtree/nbtree/src/splitloc.rs:272` `sort_unstable_by_key` | `_bt_bestsplitloc`/`_bt_strategy` take the FIRST minimum in sorted order → different leaf split points on insert-path splits → different B-tree page counts / `pg_relation_size` / pageinspect on a surface that IS deterministic in C (§2). Every duplicate-heavy B-tree hits it. |
| 2 | `indxpath.c:1905` `path_usage_comparator` (cost, selectivity) | `optimizer/path/indxpath/src/lib.rs:2544` stable `sort_by` | `choose_bitmap_and` greedy walk order → BitmapAnd/Or child order, possibly different index set, in EXPLAIN. Empirical (postgres:18): identical-stat indexes on (a,b), `WHERE a=5 AND b=7` → C deterministically emits `bm_b` BEFORE `bm_a` (Recheck Cond `(b=7) AND (a=5)`), i.e. NOT input order; a stable Rust sort keeps input order → opposite EXPLAIN text. Trips any plan-diff gate against C. |
| 3 | `planner.c:6050` `common_prefix_cmp` (window clauses) | `optimizer/plan/planner/src/window.rs:147` stable `sort_by` | WindowAgg nesting order in EXPLAIN; column evaluation order in some cases. |
| 4 | `analyzejoins.c:2376` `self_join_candidates_cmp` (reloid only; ties are the design) | `optimizer/plan/planner/src/analyzejoins.rs:1511` stable | With 3+ self-joins of one table: which relid survives elimination → alias/varno + Filter attribution in EXPLAIN. |
| 5 | `geqo_pool.c:137` float `worth` | `optimizer/plan/planner/src/geqo/pool.rs:63` stable | GEQO is deterministic given `geqo_seed` in C; pgrust deterministic-but-different → different final plan for large join queries. |
| 6 | `tsrank.c:180` `SortAndUniqItems` `compareQueryOperand` (lexeme bytes only; operands differing only in weight/prefix tie) | `utils/adt/tsrank/src/rank.rs:98` stable + `dedup_by` | Dedup keeps first of tied run; survivor's weight mask is read in `calc_rank_cd` → `ts_rank_cd(v, 'a:A | a:B')` returns a DIFFERENT FLOAT. Deterministic function-output divergence. |
| 7 | `multirangetypes.c:487` `range_compare` | `utils/adt/multirangetypes/src/lib.rs:238` stable `sort_by` | Equal-but-physically-distinct ranges (numeric 1.0 vs 1.00, citext): surviving image differs → different multirange TEXT OUTPUT and on-disk bytes. Empirical: C output flips `{[1.00,2.0)}` vs `{[1.0,2.0)}` with argument order — order-dependent and deterministic. (n<7 shields the common tiny cases for stable Rust sort.) |

Tier 2 — deterministic but narrow inputs (dup entries in dictionaries, binary
protocol, equal-key exotic types):

| rank | C site | pgrust | surface |
|---|---|---|---|
| 8 | `spell.c:1812` `cmpspell`; `spell.c:1986` `cmpaffix`; `spell.c:1286` `cmpcmdflag` | `tsearch/spell/src/build.rs:1348,1353,656` stable sorts | ispell dictionaries with duplicated words/affix rules/compound flags: different lexemes from `ts_lexize`/`to_tsvector`. |
| 9 | `dict_synonym.c:204` `compareSyn` | `tsearch/dict/src/synonym.rs:108` unstable | Duplicated LHS word in synonym file: bsearch returns a different replacement. |
| 10 | `tsvector.c:551` `tsvector_recv` needSort path (no dedup after) | `tsvector_core/src/io.rs:282` stable | Binary-protocol tsvector with duplicate lexemes: duplicate order visible in text output / `unnest`. |
| 11 | `ginutil.c:561` `cmpEntries` (dedup keeps first) | `access/gin/gin/src/util.rs:396` stable | Compare-equal-but-byte-different GIN keys (numeric, nondet collations): different representative stored → different index bytes; `haveDups` detection is also comparison-set-dependent. |
| 12 | `spgkdtreeproc.c:124` x_cmp/y_cmp (dup coords are the motivating case) | `access/spgist/spgist_kdtree/src/lib.rs:112,114` stable | Which equal-coord points land left/right of kd split → SP-GiST shape/size/pageinspect. C empirically stable (§2) but allTheSame prng voids the guarantee. Query answers unaffected. |

Tier 3 — observable but the C surface is itself degraded (prng):

| rank | C site | pgrust | surface |
|---|---|---|---|
| 13 | `tsgistidx.c:699` `gtsvector_picksplit` `comparecost` | `utils/adt/tsgistidx/src/lib.rs:652` `sort_unstable_by_key(cost)` | tsvector GiST page assignment. Same class as the triggering `gtsquery_picksplit` (branch `port/gtsquery-gist-procs`; `tsquery_gist.c:224` NOT ported on main). Downgraded by §2: GiST layout is prng-randomized in C; divergence is statistical, not identity-breaking. |
| 14 | `array_typanalyze.c:503` + `ts_typanalyze.c:352` freq-desc sorts (equal freqs are the norm at the `num_mcelem` cut) | `commands/analyze/src/array_typanalyze.rs:230`, `ts_typanalyze.rs:118` unstable (ports comment the divergence) | Different equal-frequency subset kept in `pg_stats.most_common_elems` → different `@>`/`&&`/`@@` selectivity and plans. Degraded at scale by sampling prng (§2) but reproducible on small tables. |
| 15 | `rangetypes_typanalyze.c:282/284` bound sorts | `commands/analyze/src/range_typanalyze.rs:161-162` unstable | Which physically-distinct-equal bound lands at a histogram slot → `pg_stats` text / pg_statistic bytes. Same sampling caveat. |

### 3.2 Borderline (order visible only in messages/scheduling)

| C site | pgrust | surface |
|---|---|---|
| `dependency.c:921` (subflags not in key) | `catalog_dependency/src/lib.rs:509` stable | order/wording of DROP…CASCADE NOTICE/DETAIL lines |
| `pg_shdepend.c:810` | `catalog/pg_shdepend/src/lib.rs:672` stable | line order inside "cannot drop role" DETAIL |
| `autovacuum.c:1030` (score ties universal when idle) | `postmaster/autovacuum/src/launcher.rs:444` stable | autovacuum DB rotation order |
| `resowner.c:341` (same-kind resources always tie) | `utils/resowner/resowner/src/lib.rs:370` unstable | order of resource-leak WARNINGs |

### 3.3 EXPOSED-UNOBSERVABLE / notes (selected)

- Ties byte-identical then deduped/bsearched: tidbitmap, nodeTidscan,
  procarray, reorderbuffer, snapbuild, syncrep LSNs, pg_enum/pg_inherits/
  subscriptioncmds/syscache oids, bufmgr writeback, regex arc sorts (ties =
  fully duplicate arcs deleted next pass), nbtinsert `_bt_blk_cmp`, multixact
  members, xact xids, tsvector uniquePos/uniqueentry (tie runs are folded:
  max(weight) / concatenated position lists), to_tsany uniqueWORD,
  array_to_tsvector, tsquery_op value lists, acl aclmembers, xid8funcs.
- Output re-indexed by original position: `spgtextproc.c:393` (outputs written
  by `nodes[i].i`; any within-tie permutation byte-identical).
- Value-only reads: geo_spgist coordinate medians (only the value at n/2 is
  read), array_selfuncs (merge-count and top-n prefix of equal floats),
  orderedsetaggs pct_info (each entry writes to its own `idx`),
  `tsvector_op.c:1429` TS_execute positions (only WEP_GETPOS read).
- Skip-optimization only: `ginget.c:558` frequency sort (consistent fn still
  checked per candidate; perf-only).
- `nbtpreprocesskeys.c:2742` SAOP dedup: survivor used only as transient
  search key; equal-comparing keys select identical rows. (Side defect noted:
  Rust comparator swallows fmgr errors to `0` mid-sort — error still wins.)
- `QTNSort` (`tsquery_util.c:176`): compare-0 subtrees are recursively
  identical → swap inert. `dict_thesaurus.c:467`: full tie → duplicate-entry
  error path, unreachable survivor choice.
- Dead code: `spgquadtreeproc.c:186,188` under `#ifdef USE_MEDIAN`.
- Not ported: `gtsquery_picksplit` (`tsquery_gist.c:224`, on main);
  basebackup_incremental (total anyway); collationcmds
  `pg_import_system_collations` (NON-total on duplicate locale aliases — flag
  for the eventual port: tie decides pg_collation contents).
- `createplan.c:5477 order_qual_clauses`: C deliberately hand-writes a STABLE
  insertion sort ("qsort not guaranteed stable"); Rust stable `sort_by`
  (`createplan.rs:550`, also :2237, :2958) is exactly right there. Do NOT
  "fix" to pg_qsort.
- Non-site: `pathkeys.c:1796` — comment only; the code is a strict-`>`
  selection loop; any port must preserve strict-`>`.
- Divergent key order, same set: bufmgr `rlocator_comparator` — Rust sorts
  `(spc,db,rel)` vs C `(rel,db,spc)`; arrays used only for bsearch membership.
  Flag if traversal order ever becomes observable.
- `reorderbuffer.c:5509` mapping files: Rust ADDS a filename tie-break C
  lacks; apply order provably inert (disjoint hash inserts).
- `nbtpage.c`, vacuumlazy, pruneheap, heapam bottom-up, statscmds, partbounds,
  relcache, typcache, guc, syncrep priorities, jsonb, acl sort: total (see
  sweep transcripts; explicit tie-breaks reproduced in Rust).

## 4. Precedent check (GL-PARMERGE-1 / sorted-multiset law)

GL-PARMERGE-1's ratified non-surface is the final Sort node's WITHIN-TIE EMIT
ORDER in query output, gated by sorted-multiset comparison, for the parallel
merge path. It is precedent for "row order among genuinely equal keys in query
RESULTS is not a parity surface." It is NOT precedent for:
- on-disk layout of a deterministic C surface (rank 1: nbtree splits),
- EXPLAIN plan shape (ranks 2–5), exact-text-compared by regress/plan gates,
- deterministic function outputs (ranks 6–7: ts_rank_cd floats, multirange
  text).

The gtsquery lane's citation was therefore doubly wrong (wrong mechanism AND
wrong scope) — but its conclusion survives on independent grounds (§2: GiST
layout is prng-randomized in C). Ranks 1–12 are NOT covered by any existing
ruling.

## 5. Fix options and recommendation

The project has already voted with its feet: **six faithful pg_qsort ports
exist in-tree** (tuplesort with tie-order pinning tests; gistproc;
brin_minmax_multi; rangetypes_gist(+spgist); commands/analyze; statistics
sortitem), each written precisely because equal-key output order was a parity
requirement — one (analyze) also because user btree comparators can be
intransitive and std's driver may panic on non-total orders, an independent
robustness reason to prefer the port. The gtsquery lane is the outlier, not
the rule.

(a) **Faithful pg_qsort at exposed sites — RECOMMENDED for ranks 1–12.**
    Implementation cost is already sunk six times over. Consolidate one
    generic `pg_qsort<T>` (+ `pg_qsort_arg` fallible variant) into a shared
    `_support` crate, dedupe the six copies, route ranks 1–12 through it.
    Only option that achieves C-identity; also removes the risk of every new
    lane re-deciding this ad hoc (this audit exists because one did).

(b) **Per-site total-order tie-breaks — REJECTED as a C-parity fix.** A total
    comparator gives *a* deterministic order, but matches C only if pg_qsort's
    tie order coincides with the added key — it does not (793/1000 divergence
    even against the natural input-index tie-break). It changes the
    divergence, it does not close it. Legitimate only where the goal is mere
    determinism and the order is provably inert (the reorderbuffer filename
    tie-break is the one existing, correct use).

(c) **Ratify as non-surface — appropriate for Tier 3 only, with argument.**
    GiST layout (rank 13 + gtsquery): prng-randomized in C, page-count parity
    unattainable even C-vs-C → ratify, replacing page-count bars with
    logical-equivalence gates (same rows retrievable, amcheck-clean).
    Stats at scale (ranks 14–15): sampling prng degrades the surface, BUT
    small-table ANALYZE is deterministic and regress uses small tables — fix
    under (a) is still warranted there (and is cheap; the crate already
    carries pg_qsort for compute_scalar_stats). Do NOT extend (c) to Tier 1/2:
    those surfaces are deterministic in C and gate-visible.

Priority for a fix lane: rank 1 (nbtsplitloc — deterministic on-disk surface,
ordinary workloads), ranks 2–5 (planner — EXPLAIN-visible, trips plan gates),
ranks 6–7 (deterministic function outputs), ranks 8–12 (narrow-input
determinism), then ratify Tier 3 under (c). The `gtsquery_picksplit` port
(unlanded branch) should use the shared pg_qsort when it lands regardless —
it costs nothing and keeps the statistical distribution aligned.

## 6. Empirical appendix

All docker runs on `postgres:18` (server 18.4, aarch64), 2026-07-30.

1. pg_qsort standalone determinism + divergence: §1 table (harnesses in
   session scratchpad `qsort_test/`: `pgqsort_demo.c` compiled against the
   verbatim PG 18.3 `sort_template.h`, `rust_demo.rs`).
2. GiST nondeterminism in C: tsquery GiST 3k rows, 5 identical CREATE INDEX
   runs → 14/12/14/14/14 pages; 20k tsquery → 89/87/88/84/86; tsvector 20k →
   153 vs 152 on identical rebuild.
3. B-tree insert-path determinism in C: 50k rows, index pre-created, 3
   identical runs → pg_relation_size 581632 x3.
4. SP-GiST kd determinism in C: 30k dup-coord points → 2662400 x3; 30k
   identical points (allTheSame path) → 3309568 x3.
5. Bitmap AND tie in EXPLAIN: identical-stat indexes bm_a, bm_b;
   `WHERE a=5 AND b=7` → C deterministically orders bm_b before bm_a (EXPLAIN
   md5 identical across runs) — pg_qsort tie order, not input order; a stable
   sort (pgrust today) yields the opposite order.
6. Multirange survivor image: `nummultirange(numrange(1.0,2.0),
   numrange(1.00,2.0))` → `{[1.00,2.0)}`; swapped arguments → `{[1.0,2.0)}`.
   Deterministic, order-dependent, text-visible.
