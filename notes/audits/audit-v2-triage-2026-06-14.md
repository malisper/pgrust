# Audit v2 triage — 2026-06-14

Source: workflow wf_3a44bef7 (345 crates audited). Totals: 324 findings —
34 high / 51 med / 239 low; classified 169 divergence / 62 missing_or_stub / 14 registry.
Full output (GC-risk): /private/tmp/.../tasks/w6o8gtrhc.output

WRONG_CONSTANT findings spot-checked 4/4 against PG18 headers → audit is trustworthy.

## TIER A — WRONG_CONSTANT (silent corruption; VERIFIED vs headers)
- backend-access-hash-entry lib.rs:73-75 — REGBUF_STANDARD 0x04→**0x08**, REGBUF_NO_CHANGE 0x10→**0x20** (NO_IMAGE 0x02 ok). Live WAL at lib.rs:858 emits corrupt flags.
- backend-commands-explain lib.rs:170 — EXEC_FLAG_EXPLAIN_GENERIC 0x0040→**0x0002** (0x0040 = WITH_NO_DATA).
- types-plancache lib.rs:46 — CURSOR_OPT_GENERIC_PLAN 0x0800→**0x0200** (0x0800 = PARALLEL_OK).
- backend-utils-cache-relcache initfile.rs:105 — RELCACHE_INIT_FILEMAGIC 0x01337088→**0x573266**.

## TIER B — WRONG logic constant (execExpr fmgr resolution)
- backend-executor-execExpr execExpr_core.rs:565-584 — OpExpr/DistinctExpr/NullIfExpr arms pass op.opno (PG_OPERATOR oid) instead of op.opfuncid (PG_PROC oid) + InvalidOid instead of op.inputcollid → wrong fmgr function + dropped collation.

## TIER C — STUB_FILLABLE (dead panic, owner ALREADY ported → make live call)
- nbtree: bt_finish_split (search.rs:433 → insert.rs:2322 exists); _bt_getstackbuf (page.rs:2397 → insert.rs:2366 exists); index_getprocinfo_oid (_bt_first cached-proc path); _bt_leafbuild omits _bt_allequalimage(index,true) override (nbtsort).
- execProcnode: exec_shutdown_node_walker (flat panic; planstate_tree_walker + all 6 owners ported); exec_end_node (only 15 of ~33 arms; owners ported); multi_exec_proc_node (missing BitmapIndexScan/And/Or; owners ported, nodeBitmapHeapscan calls it); exec_init_node (med).
- execAmi: exec_mark_pos / exec_restr_pos (only Material; IndexScan/IndexOnly/Custom/Sort/Result owners pub); exec_re_scan (~41 cases, partial).
- execTuples: exec_store_all_null_tuple + exec_set_slot_descriptor (slot model #113 landed); slot_getsysattr missing TableOid/SelfItemPointer pre-dispatch (DIVERGENT).
- fmgr-core: input_function_call / input_function_call_safe / receive_function_call / oid_function_call0 — implemented in-crate but NEVER installed; ::called by merged copyfromparse/arrayfuncs/misc2 → runtime panic NOW.
- rangetypes: range_in/out/recv/send panic; element fmgr seams (input/output/receive/send_function_call) available now.
- pg-db-role-setting: 9 catalog-access seams unset (relcache/genam ARE landed).

## TIER D — DIVERGENT correctness
- backend-executor-execScan scan_scanrelid — missing IndexScan/SubqueryScan/CteScan/NamedTuplestoreScan arms (panics).
- backend-executor-nodeIndexonlyscan IndexOnlyNext — missing `ecxt_scantuple = slot` before recheckqual (lossy branch) → wrong recheck.
- backend-utils-cache-catcache SearchCatCacheList — missing concurrent-invalidation early-terminate + do/while restart loop.
- backend-executor-execPartition — PartitionTupleRouting crate-type vs seam-type carrier divergence (routing half unreachable; #14/#165 territory).

## TIER E — PARTIAL PORT (missing whole files)
- backend-optimizer-util-vars — appendinfo.c (1060 LOC: make_append_rel_info/adjust_appendrel_attrs*) + paramassign.c (761 LOC: replace_outer_*) ENTIRELY missing; make_pathtarget_from_tlist stub. (Blocks relnode/inherit which seam adjust_appendrel_attrs.)
- backend-optimizer-path-costsize — set_foreign_size_estimates / set_subquery_size_estimates missing.
- backend-utils-sort-storage — tuplestore_get_stats / tuplestore_puttuple missing (MinimalTuple-carrier keystone adjacent).
- backend-catalog-objectaddress — get_catalog_object_by_oid(_extended) missing.
- backend-utils-adt-cash — cash_recv/cash_send missing.
- backend-storage-ipc-latch — set_latch_for_procno/_proc_pid missing.

## TIER F — INTRODUCED registries to remove (north-star handle removal)
GENUINE (act): pg-db-role-setting SettingScan(u64) [high]; lib-hyperloglog [med]; tsearch-spell SpellHandle [med]; sort-storage logtape REGISTRY [med]; executor-tqueue RECEIVERS/READERS [low]; storage-ipc-shm-mq seam_layer Registry [low].
INHERITED/sanctioned (DO NOT touch — auditor pre-empted false positives): parallel ParallelContext, nodes-core params ParamListInfo, procarray GlobalVis, syncrep-scanner, typcache DomainConstraintRef, nodeSort/sortsupport shims, applyparallelworker shared_registry (DSM).

## Wave plan
W1 (launched 2026-06-14): const-sweep(A), execExpr-opfuncid(B), fmgr-core-installs(C-fmgr), executor-dispatch(C-exec), nbtree-stub-fills(C-nbt).
W2: execTuples fills, rangetypes fills, scan-correctness(D), util-vars appendinfo+paramassign(E-big).
W3: registry removals (F-genuine), remaining partials, execPartition carrier (coordinate w/ #165).
