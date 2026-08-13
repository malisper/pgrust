//! Feature-module registry and toggle vector. Modules are the swarm-testing
//! axis: each has an on/off toggle and a selection weight, sampled per
//! session (seeded) or pinned via `--modules`.

use crate::rng::Rng;

#[derive(Clone, Copy, Debug)]
pub struct ModuleSpec {
    pub name: &'static str,
    pub default_weight: f64,
}

/// All known statement modules (names parallel to `stmt::STMT_MODULES`;
/// a sync test enforces it). Later milestones append here (DML, DDL, ...).
pub const REGISTRY: &[ModuleSpec] = &[
    ModuleSpec { name: "expr", default_weight: 1.0 },
    ModuleSpec { name: "joins", default_weight: 1.0 },
    ModuleSpec { name: "subq", default_weight: 1.0 },
    ModuleSpec { name: "agg", default_weight: 1.0 },
    ModuleSpec { name: "win", default_weight: 1.0 },
    // Builtin window-FUNCTION argument/result edge probes (percent_rank,
    // cume_dist, and the NULL/error/out-of-range argument regimes the win
    // module steers around). Complements win; a distinct toggle so a swarm
    // can drive the edge regime independently.
    ModuleSpec { name: "winfunc", default_weight: 1.0 },
    ModuleSpec { name: "dml", default_weight: 1.0 },
    // MERGE is one statement per pick but touches the same shared-table
    // state as dml; below-default weight keeps the write mix balanced.
    ModuleSpec { name: "merge", default_weight: 0.7 },
    // txn emits a whole BEGIN..COMMIT bracket (typically ~5 statements)
    // per pick, so its selection weight sits below the one-statement
    // modules to keep the statement mix balanced.
    ModuleSpec { name: "txn", default_weight: 0.5 },
    // ddl churns persistent objects (and view/aggregate/trigger picks emit
    // 2-3 statement groups); below-default weight keeps the object churn
    // from starving query statements over the shared catalog.
    ModuleSpec { name: "ddl", default_weight: 0.7 },
    // Statement-level rich-type shapes (SRFs in FROM over literal inputs);
    // most T1 surface lives in the expr productions, so the module weight
    // stays low.
    ModuleSpec { name: "types", default_weight: 0.5 },
    ModuleSpec { name: "explain", default_weight: 1.0 },
    // util occasionally emits PREPARE/EXECUTE brackets and SET+SHOW
    // pairs; still ~1 statement per pick on average.
    ModuleSpec { name: "util", default_weight: 1.0 },
    // part creates whole partition trees (3-6 statement groups) and churns
    // persistent objects like ddl; the deep coverage comes from OTHER
    // modules targeting its parents, so the selection weight stays low.
    ModuleSpec { name: "part", default_weight: 0.6 },
    // partalt (W5-PART) drains the ALT-PATH residue around partition
    // pruning / partitionwise join+agg over its own self-contained,
    // group-local fz_pa_* fixtures (created and dropped in-group); no
    // persistent catalog growth, so the selection weight can sit near
    // the query-module default.
    ModuleSpec { name: "partalt", default_weight: 0.6 },
    // objddl churns cluster-global objects (roles) and per-db types/stats;
    // several picks emit 3-4 statement groups (SET ROLE brackets, stats
    // create+ANALYZE+probe, coltab), so the weight sits below default.
    ModuleSpec { name: "objddl", default_weight: 0.6 },
    // idx creates whole indexed-table populations (a create group is
    // ~8-20 statements of table + bulk load + indexes) and its query
    // brackets are 3-5 statements each; low selection weight keeps the
    // statement mix balanced.
    ModuleSpec { name: "idx", default_weight: 0.5 },
    // par creates big bulk-loaded tables (a create group is ~24k rows +
    // three index builds) and every query pick is a 5-10 statement GUC
    // bracket; low selection weight keeps the statement mix balanced.
    ModuleSpec { name: "par", default_weight: 0.5 },
    // tsdl churns text-search configs/dictionaries (usually 2-statement
    // groups: one DDL + one probe); the expression-level ts productions
    // carry most of the stemmer surface, so the module weight stays low.
    ModuleSpec { name: "tsdl", default_weight: 0.5 },
    // cursor emits whole transaction brackets (DECLARE + 2-5 FETCH/MOVE +
    // terminator) or PREPARE/EXECUTE bursts, so several statements per
    // pick; below-default weight keeps the statement mix balanced.
    ModuleSpec { name: "cursor", default_weight: 0.6 },
    // views creates whole view/base/rule-table object sets (2-8 statement
    // groups) and churns persistent objects like ddl; the deep rewriter
    // coverage comes from OTHER modules' DML through the registered views,
    // so the selection weight stays low.
    ModuleSpec { name: "views", default_weight: 0.6 },
    // matview complements `views`: group-local matview CONTENT/scannability/
    // match-merge/CTAS/SELECT INTO/security-option/OR-REPLACE-add-column/
    // RECURSIVE-view arms (createas.c + matview.c + view.c). Each pick is a
    // self-contained create->exercise->drop group of ~6-14 statements, so it
    // sits at a below-default weight like views.
    ModuleSpec { name: "matview", default_weight: 0.5 },
    // geo creates whole indexed-table populations (create groups are ~5
    // statements with bulk loads) but most picks are single scalar/query
    // statements; slightly below default keeps the create churn balanced.
    ModuleSpec { name: "geo", default_weight: 0.8 },
    // dtm emits one literal-only scalar SELECT per pick (A2 datetime
    // cross-type matrix); it targets a deep but narrow adt surface, so a
    // moderate weight buys its coverage without starving table-driven
    // modules.
    ModuleSpec { name: "dtm", default_weight: 0.8 },
    // dtx complements dtm with the set-returning / typmod / timezone-
    // function datetime surface (generate_series, isfinite, interval &
    // precision typmods, timezone(zone,src), to_timestamp epoch); one
    // statement per pick, so it shares dtm's moderate weight.
    ModuleSpec { name: "dtx", default_weight: 0.8 },
    // adtmisc emits mostly single literal-driven SELECTs (grant/denial
    // brackets are 3-4 statements); slightly below default keeps the
    // breadth families from crowding the stateful modules.
    ModuleSpec { name: "adtmisc", default_weight: 0.8 },
    // sqljson emits one SQL/JSON statement per pick (J1: constructors,
    // query functions, JSON_TABLE, jsonpath item methods) — a deep parser/
    // executor surface over mostly literal inputs, so a moderate weight
    // buys its coverage without starving table-driven modules.
    ModuleSpec { name: "sqljson", default_weight: 0.8 },
    // jsonpath drains the raw jsonpath execution engine the sqljson module
    // leaves dark (@?/@@ operators, vars/PASSING binding, filter predicate
    // grammar, unicode scanner, recursive accessor, cross-type datetime
    // compare, and the DefineIndex mutability walk). One statement per pick
    // except the self-contained mutidx bracket; moderate weight like sqljson.
    ModuleSpec { name: "jsonpath", default_weight: 0.8 },
    // jsonfuncs emits one json/jsonb function-or-operator statement per
    // pick (the builder/accessor/operator/expand/aggregate surface J1's
    // sqljson does not reach: jsonb_op.c operators, the jsonfuncs.c each/
    // elements/keys/extract SRFs and classic *_agg aggregates, jsonb
    // subscripting) over mostly literal inputs — sqljson-like weight.
    ModuleSpec { name: "jsonfuncs", default_weight: 0.8 },
    // plpg emits whole self-contained object groups (create + exercise +
    // probe + drop, typically 3-15 statements); below-default weight keeps
    // the group churn from crowding the single-statement modules.
    ModuleSpec { name: "plpg", default_weight: 0.6 },
    // coll (C3) emits self-contained collation groups (create collation +
    // table/index/range type, exercise, drop — typically 5-14 statements);
    // below-default weight keeps the group churn from crowding the
    // single-statement modules.
    ModuleSpec { name: "coll", default_weight: 0.6 },
    // mbconv is one statement per pick over pre-verified literal tables
    // (encoding-conversion sweep, Q2); moderate weight — the surface is
    // wide (128 conversion pairs) but each statement is cheap.
    ModuleSpec { name: "mbconv", default_weight: 0.7 },
    // xnum is one statement per pick over typed-literal operator
    // matrices (cross-type numeric sweep, Q2).
    ModuleSpec { name: "xnum", default_weight: 0.8 },
    // nodes emits whole debug-print bracket groups (SETs + 1-9 utility
    // statements + RESETs, ~4-13 statements per pick) and churns
    // persistent objects; below-default weight keeps the group churn from
    // crowding the single-statement modules.
    ModuleSpec { name: "nodes", default_weight: 0.5 },
    // obs emits observability probe groups (typically 1-10 wrapped
    // pg_stat probes per pick, occasionally with a table/function
    // create+drop); moderate weight — the surface is wide but cheap.
    ModuleSpec { name: "obs", default_weight: 0.6 },
    // admin is mostly one projected statement per pick (Q5 admin-funcs)
    // over a WIDE arm surface (11 families); default weight keeps its
    // statement share ~2% — multi-statement modules dilute per-pick
    // modules, and below 1.0 the sparse arms (hba/mcxt) never fire in a
    // 2000-statement leg. The backup/summarizer brackets still carry real
    // WAL cost (switch + CHECKPOINT) but sit at token family weights.
    ModuleSpec { name: "admin", default_weight: 1.0 },
    // objid emits whole hand-verified identity/deparse deck batteries
    // (60-200 statements per group — every OBJECT_* class + every
    // deparsable expr node, probed through pg_identify_object/
    // pg_describe_object/pg_get_*def); a low weight keeps the giant
    // groups from crowding the statement mix (each pick lands ~100-200
    // statements, so even a token weight buys a large statement share;
    // drain arms enable it explicitly via --modules objid=on:N).
    ModuleSpec { name: "objid", default_weight: 0.05 },
    // einterp emits whole self-contained drain groups (fixture create +
    // 1-10 statements + drop) targeting the ExecInterpExpr opcode arms and
    // the raw/analyzed tree-walker node kinds (LD2); below-default weight
    // keeps the object churn balanced like nodes/plpg.
    ModuleSpec { name: "einterp", default_weight: 0.5 },
    // exd emits EXPLAIN plan-node/option drain groups (LD4): each pick is
    // a self-contained fixture + 6-25 EXPLAIN probes over the missing
    // explain.c node/option arms. Moderate weight — groups are chunky
    // (the bitmap arm bulk-loads 20k rows) and the surface is fixed-shape.
    ModuleSpec { name: "exd", default_weight: 0.4 },
    // spill (LD5) emits GUC-bracketed spill/fallback groups (typically
    // 3-9 statements: SETs + 1-2 queries + RESETs) over one big bulk
    // table; below-default weight keeps the heavyweight spilling queries
    // from crowding the statement mix.
    ModuleSpec { name: "spill", default_weight: 0.5 },
    // earm emits whole self-contained ERROR-ARM drain groups (fixtures +
    // 6-14 deliberately-invalid probes + drops, ~15-40 statements per
    // pick) targeting the DDL/parser/catalog ereport validation arms
    // (LD6). The groups are the largest in the registry, so the selection
    // weight sits well below the bracket modules to keep the statement
    // mix balanced (at 0.5 the module carried >20% of a default stream).
    ModuleSpec { name: "earm", default_weight: 0.3 },
    // plansel (LD7) emits GUC-profile plan-selection sweeps: one
    // deterministic query repeated under 2-3 forced-plan GUC brackets
    // (~10-30 statements per pick), plus chunky fixture-set creates
    // (~35-statement groups with three bulk loads); below-default weight
    // keeps the sweep groups from crowding the statement mix.
    ModuleSpec { name: "plansel", default_weight: 0.4 },
    // earm2 (LD8) emits whole VERBATIM hand-verified ERROR-ARM round-2
    // sections (12-60 statements per pick, fixtures + probes + drops);
    // like earm the groups are large, so the weight sits low.
    ModuleSpec { name: "earm2", default_weight: 0.2 },
    ModuleSpec { name: "earm3", default_weight: 0.2 },
    // earm4 (ERR3) emits whole VERBATIM ERROR-ARM round-4 sections
    // (domain/constraint/tablespace/ownership/policy/rule/publication/
    // subscription/COMMENT+SECURITY LABEL/sequence-identity/parser
    // grammar arms); like earm2/earm3 the groups are large, low weight.
    ModuleSpec { name: "earm4", default_weight: 0.2 },
    // exr (LD9) emits executor-residue drain groups (runtime pruning
    // brackets, window frame-option probes, MERGE/ON CONFLICT rollback
    // brackets, transition-table trigger groups) over a persistent
    // fixture suite; below-default weight like spill — the create group
    // is chunky and the bracket groups run heavyweight nodes.
    ModuleSpec { name: "exr", default_weight: 0.5 },
    // exr2 (EXEC-RESIDUE) drains the rescan half of the executor nodes exr
    // only runs once (ExecReScanSetOp/RecursiveUnion/WindowAgg/
    // NamedTuplestoreScan via correlated LATERAL) plus setop hash/sort
    // internals and the plan-serialization toggle arms; below-default
    // weight like exr — the create group is chunky and the lateral-rescan
    // groups run heavyweight nodes per outer row.
    ModuleSpec { name: "exr2", default_weight: 0.5 },
    // numx (LD9) is one boundary-value statement per pick over the
    // numeric.c arithmetic/format surface (plus in-group sort/window
    // fixture families); xnum-like weight.
    ModuleSpec { name: "numx", default_weight: 0.7 },
    // LD10: publication/subscription DDL drain (never-connecting; see
    // crate::pubsub module docs).
    ModuleSpec { name: "pubsub", default_weight: 0.3 },
    // ddldeep (SQLcov-A) emits whole VERBATIM hand-verified DDL-deep
    // sections (15-75 statements per pick: AT_* pass lifecycles,
    // FK-bearing partition trees, partitioned-index attach, in-place
    // tablespaces); like earm2 the groups are large, so the weight
    // sits low.
    ModuleSpec { name: "ddldeep", default_weight: 0.2 },
    // pgram (SQLcov-B) emits self-contained parser rare-grammar batteries
    // (fixtures + valid-rare probes + deliberate analysis errors +
    // drops, ~20-60 statements per pick); earm-like low weight.
    ModuleSpec { name: "pgram", default_weight: 0.3 },
    // opt2 (SQLcov-B) emits optimizer round-2 sweep groups (in-group
    // fixture create + forced-profile query sweeps + drop, ~40-90
    // statements per pick); plansel-like low weight.
    ModuleSpec { name: "opt2", default_weight: 0.3 },
    // opt3 (W4-OPT) emits optimizer/executor residue sweep groups
    // (in-group fixture create + forced-profile query sweeps + drop,
    // ~40-120 statements per pick); opt2-like low weight.
    ModuleSpec { name: "opt3", default_weight: 0.3 },
    // cfgm (CFG lane) emits GUC set/reset/show brackets, custom-class
    // placeholders, timezone-abbreviation loads, SET TRANSACTION grammar
    // arms and exception-swallowed encoding-conversion byte sweeps; most
    // picks are 1-6 cheap statements, but the conv sweeps run ~400
    // subtransactions per statement, so the weight sits low (drain arms
    // enable it explicitly via --modules cfgm=on:N).
    ModuleSpec { name: "cfgm", default_weight: 0.3 },
    // btbrin (W5-BTBRIN) drains btree ALT-PATH (dedup/split/page-delete/
    // parallel-build/unique-check) and BRIN (summarize/desummarize/
    // autosummarize/inclusion/minmax_multi) residue behind GUC + data-
    // size forcing; groups run 20-60 statements over dedicated fz_bb_*
    // fixtures — opt2-like low weight, enabled explicitly for drain
    // legs via --modules btbrin=on:N.
    ModuleSpec { name: "btbrin", default_weight: 0.3 },
    // heap (W5-HEAP) emits single-session heapam alt-path drain groups over
    // a purpose-built fz_hp_N fixture (HOT/prune churn, aborted/locked-only
    // overwrite arms, toast write/read/delete, vacuumlazy freeze/truncate,
    // CLUSTER/VACUUM FULL, CIC validate scans, TID + serializable-read
    // entries). A create pick builds a churn-heavy table and vacuum/cluster
    // picks walk the whole heap, so it sits at a low weight like spill.
    ModuleSpec { name: "heap", default_weight: 0.5 },
    // stats (STATS lane) emits self-contained statistics build+estimation
    // drain groups (fixture create + CREATE STATISTICS + ANALYZE +
    // stats-consuming query sweeps + introspection + drop, ~20-60
    // statements per pick) targeting the extended-stats/ANALYZE/selfuncs
    // line mass. A create pick bulk-loads a few-thousand-row table and the
    // sweeps run heavyweight estimation, so it sits at a low weight like
    // plansel/opt2; drain legs enable it explicitly via --modules stats=on:N.
    ModuleSpec { name: "stats", default_weight: 0.4 },
    // tsrank (FTS-RANK lane) emits one self-contained literal-driven ts
    // ranking / headline / tsvector-op / ts_stat / tsquery-op probe SELECT
    // per pick — a deep but narrow adt surface over constant inputs, so a
    // moderate weight buys its coverage without starving table-driven
    // modules.
    ModuleSpec { name: "tsrank", default_weight: 0.8 },
    // planner (fuzz-planner) drains optimizer path/plan-shape residue
    // (SampleScan/TidRangeScan/sorted-Group/SetOp/window-run-condition/
    // GEQO/BitmapOr) behind GUC + query-shape forcing over self-contained
    // fz_pl_* fixtures; groups run ~8-30 statements per pick — opt2-like
    // low weight, enabled explicitly for drain legs via
    // --modules planner=on:N.
    ModuleSpec { name: "planner", default_weight: 0.3 },
    // partition (PARTITION lane) emits self-contained partition-surface
    // drain groups over dedicated fz_pt_* fixtures: partition-wise join/agg
    // bound MERGING (compatibly-partitioned parents with DIFFERENT bounds —
    // the merge_list_bounds / merge_range_bounds family that plansel's
    // identical-bound twins never reach), no-partition-found routing errors,
    // DEFAULT-partition ATTACH validation, attribute-mapped cross-partition
    // routing, prefix-equality runtime pruning and partition-constraint
    // deparse. Groups are chunky create/probe/drop batteries (~15-40
    // statements), so the weight sits low like plansel/opt2.
    ModuleSpec { name: "partition", default_weight: 0.3 },
    // triggers (Track-B) emits whole self-contained trigger/rule/event-trigger
    // EXECUTION-drain groups (fixtures + state-flips + a total-ordered
    // firing-log probe + drops, ~15-25 statements per pick) over fixed trg_*
    // names; earm-like low weight keeps the chunky groups from crowding the
    // single-statement modules, and event-trigger/session-role picks are
    // rigorously self-cleaning.
    ModuleSpec { name: "triggers", default_weight: 0.3 },
    // plpgsql (PLPGSQL lane) emits whole self-contained plpgsql/SPI
    // residual-arm drain groups (create + exercise + probe + drop,
    // typically 3-7 statements); below-default weight keeps the group
    // churn from crowding the single-statement modules, matching plpg.
    ModuleSpec { name: "plpgsql", default_weight: 0.6 },
    // regex drives the regexp_* function + operator surface (a SQL-reachable
    // crash/DoS engine) with crafted adversarial patterns; every pick is one
    // literal-driven statement (or a small SRF), so the weight can sit near
    // the query-module default.
    ModuleSpec { name: "regex", default_weight: 0.8 },
    // Self-contained rewrite/ModifyTable residue batteries (Track-B): each
    // pick builds, drains one arm family and drops its own objects, so the
    // weight only trades against generation budget, not shared table state.
    ModuleSpec { name: "mergex", default_weight: 0.6 },
    // aggwin (AGGWIN) drains the nodeWindowAgg.c / nodeAgg.c frame residue
    // the win/agg/par/spill modules leave uncovered: GROUPS frame mode +
    // EXCLUDE across all modes, typed RANGE-offset in_range, window rescan
    // (ExecReScanWindowAgg), WINDOW refinement chains, FILTER on window
    // aggregates, moving-frame inverse/restart transition, and HashAgg
    // grouping-sets disk spill. Each pick is a self-contained fixture (or
    // GUC-bracketed inline-source) group — a low weight like spill/heap.
    ModuleSpec { name: "aggwin", default_weight: 0.4 },
    // indexam (INDEXAM) deep-drains the GIN / GiST / SP-GiST operator-class
    // internals (multi-key scan keys, posting/pending lists, page-split
    // picksplit, KNN, vacuum) plus partial/expression/INCLUDE/multicolumn
    // and non-default-opclass btree paths, over dedicated fz_ix_* fixtures
    // at page-splitting volume; groups run 15-40 statements at a low weight
    // like btbrin — enabled explicitly for drain legs via
    // --modules indexam=on:N.
    ModuleSpec { name: "indexam", default_weight: 0.3 },
    // typeio drains the utils/adt type-I/O + operator residue (network /
    // mac / mac8 / varbit comparison+bitwise, float4 to_char pictures,
    // moving-aggregate inverse transitions, datetime out-of-range arms)
    // via literal-driven scalar SELECTs; near the query-module default.
    ModuleSpec { name: "typeio", default_weight: 0.8 },
    // aclrls emits self-contained ACL/RLS drain brackets (create roles +
    // tables + policies, exercise the privilege-decision / row-security
    // execution surface, drop everything — typically 15-30 statements per
    // pick); like the other bracket-heavy drain modules the selection
    // weight sits low so the large groups don't crowd the statement mix.
    ModuleSpec { name: "aclrls", default_weight: 0.3 },
    // lockcursor emits multi-statement, self-contained brackets (LOCK
    // TABLE all-modes, nested savepoints, NOTIFY-error arms) plus the
    // advisory arms adtmisc misses; a bracket is ~10-25 statements, so the
    // selection weight sits low like txn/pubsub.
    ModuleSpec { name: "lockcursor", default_weight: 0.4 },
    // largeobj (LARGEOBJECT) emits self-contained large-object drain groups
    // (create + write/seek/read/truncate/get/put + unlink, or an isolated
    // error probe) over the be-fsstubs.c/inv_api.c lo_* surface; every loid
    // is created and unlinked in-group, so no persistent state grows. Most
    // picks are 4-13 cheap statements, so it sits at a moderate weight like
    // the other single-session drain modules.
    ModuleSpec { name: "largeobj", default_weight: 0.5 },
    // plancache (PLANCACHE) emits self-contained plan-cache drain groups:
    // in-group fixture create + PREPARE + force_generic/force_custom
    // (plan_cache_mode) result-identity sweeps + generic-plan runtime
    // partition pruning + plancache DDL invalidation + pg_prepared_
    // statements introspection + drop (~15-40 statements per pick, one
    // create per group). Below-default weight like plansel/opt2 — the
    // groups carry a create+drop each.
    ModuleSpec { name: "plancache", default_weight: 0.3 },
    // vacuum (VACUUM lane) emits single-session, autocommit SQL-maintenance
    // option-matrix groups over purpose-built fz_vac_* fixtures (the full
    // VACUUM/ANALYZE/CLUSTER/REINDEX option grammar + partitioned
    // propagation + in-place-tablespace REINDEX). A create pick builds a
    // churn-heavy table and maintenance picks walk the whole relation, so it
    // sits at a low weight like heap/spill.
    ModuleSpec { name: "vacuum", default_weight: 0.5 },
    // seqident (SEQIDENT) emits self-contained sequence / GENERATED
    // IDENTITY / serial groups (create + exercise + capture + drop,
    // typically 5-14 statements) over group-local fz_si_* objects; only
    // name counters persist. Below-default weight keeps the group churn
    // from crowding the single-statement modules, like coll/plpg.
    ModuleSpec { name: "seqident", default_weight: 0.6 },
    // ritrig emits self-contained referential-integrity groups (create
    // parent+child fixture, cascading DELETE/UPDATE or a deliberate
    // violation, pk-ordered state probe, drop — typically 8-14 statements);
    // below-default weight like coll/plpg keeps the group churn from
    // crowding the single-statement modules.
    ModuleSpec { name: "ritrig", default_weight: 0.5 },
    // rangeops emits mostly single literal-driven SELECTs over the range /
    // multirange operator+function matrix (adtmisc/dtm-shaped); the
    // custom-range CREATE TYPE AS RANGE groups are self-contained 4-8
    // statement create/exercise/drop brackets, so the weight sits slightly
    // below default to keep the group churn balanced.
    ModuleSpec { name: "rangeops", default_weight: 0.7 },
    // udt (UDT lane) emits self-contained user-defined-type groups (enum /
    // composite / domain: create + exercise + probe + drop, typically
    // 12-28 statements per pick); below-default weight like coll/plpg keeps
    // the group churn from crowding the single-statement modules.
    ModuleSpec { name: "udt", default_weight: 0.6 },
    // arrayops is one statement per pick (the subscript-UPDATE family emits a
    // small create/mutate/select/drop group); operator/function matrix over
    // typed array literals, xnum-like weight.
    ModuleSpec { name: "arrayops", default_weight: 0.8 },
    // altertable (Track-B) emits self-contained ALTER TABLE rewrite/phase
    // EXECUTION groups (fixture create + ALTER + content/metadata verify +
    // drop, typically 5-9 statements) over group-local fz_at_* fixtures; it
    // drains the SUCCESSFUL ATRewriteTable/ATExecAlterColumnType/AT_PASS
    // surface the earm* lanes' error arms don't reach. Below-default weight
    // keeps the create+drop churn balanced like heap/spill.
    ModuleSpec { name: "altertable", default_weight: 0.5 },
    // byteaenc (BYTEAENC) is one boundary/error statement per pick over the
    // bytea + encode/decode + encoding-convert surface (the bytea_output
    // GUC bracket is the only multi-statement pick, 5 cheap statements);
    // cheap and self-contained, so it sits at the query-module default.
    ModuleSpec { name: "byteaenc", default_weight: 0.7 },
    // srf (Track-B SRF) is one self-contained statement per pick over
    // literal inputs (generate_series / generate_subscripts / unnest /
    // ROWS FROM / string_to_table + the SRF-context error guards); cheap
    // and stateless, so it sits near the query-module default.
    ModuleSpec { name: "srf", default_weight: 0.8 },
    // stringfunc (STRINGFUNC lane) emits one literal-driven scalar SELECT
    // per pick over the varlena/oracle_compat/formatting/ascii string
    // function surface (substring/overlay/pad/trim/translate/split/format/
    // normalize/hash ...); a deep-but-narrow adt surface like dtm/xnum, so
    // a moderate weight buys its coverage without starving table-driven
    // modules.
    ModuleSpec { name: "stringfunc", default_weight: 0.8 },
    // floatmath (Track-B) emits one literal-driven scalar SELECT per pick
    // over the float.c function/operator surface (trig/hyperbolic/exp-log/
    // rounding/arithmetic/comparison/casts/text-roundtrip/special values);
    // the aggregate family creates-and-drops a small fz_fma fixture in-
    // group. Most picks are single cheap statements over a deep-but-narrow
    // adt surface, so a moderate weight buys its coverage without starving
    // the table-driven modules (dtm/xnum/numx precedent).
    ModuleSpec { name: "floatmath", default_weight: 0.8 },
    // numeric (NUMERIC lane) drains the numeric.c residue no other module
    // reaches: integer statistical aggregates (the int128 "poly" path,
    // serial + parallel + moving-window) and extreme-precision arithmetic
    // (300-1500-digit operands -> mul_var/div_var/sqrt_var/ln_var). One
    // fixture create+probe+drop group (istat) or a single literal
    // statement (xprec) per pick; numx-like moderate weight.
    ModuleSpec { name: "numeric", default_weight: 0.6 },
    // ruleutils (Track-B deparse drain) emits self-contained schema-wrapped
    // groups (~6-12 statements: schema guard + creates + total-ordered
    // pg_get_*def sweeps + CASCADE drop) targeting ruleutils.c. Each group
    // rebuilds a throwaway schema, so keep the selection weight low like the
    // other object-churning drain modules; drain legs enable it explicitly
    // via --modules ruleutils=on:N.
    ModuleSpec { name: "ruleutils", default_weight: 0.4 },
    // cterec (Track-B) emits self-contained recursive-CTE SEARCH/CYCLE,
    // data-modifying-CTE BEGIN..ROLLBACK brackets, and sublink probes over
    // inline VALUES; single statement per pick except the dml bracket
    // (~7 statements). Moderate weight — the surface is deep but narrow.
    ModuleSpec { name: "cterec", default_weight: 0.6 },
    // castcoerce (CASTCOERCE) emits mostly single literal-driven SELECTs
    // (explicit/typmod/unify/unknown/array/bincoerce/failed) plus a handful
    // of self-contained CREATE-DOMAIN/TYPE/CAST + TEMP-table groups (5-11
    // statements, each dropping everything it creates); a moderate weight
    // like dtm buys the deep-but-narrow parser-coercion surface without
    // starving the table-driven modules.
    ModuleSpec { name: "castcoerce", default_weight: 0.8 },
    // intops (INTOPS) is one boundary-value statement per pick over the
    // int.c/int8.c arithmetic/bit/overflow/parse surface; a deep but narrow
    // adt surface reached only at boundary operands, so a moderate weight
    // like numx buys its coverage without starving table-driven modules.
    ModuleSpec { name: "intops", default_weight: 0.7 },
    // expreval saturates the scalar-expression opcode arms the mutation lane
    // flagged (hashed ScalarArrayOp, IS JSON, whole-row Var, IS DISTINCT,
    // GREATEST/LEAST, CASE, COALESCE/NULLIF, bool 3VL, ROW/array compare)
    // over inline VALUES operands; one deep-but-cheap SELECT group per pick,
    // so it sits near the query-module default.
    ModuleSpec { name: "expreval", default_weight: 0.8 },
    // like (LIKE lane) emits one literal-only scalar SELECT per pick (plus
    // one VALUES-driven non-constant-pattern shape) over the LIKE/ILIKE/
    // SIMILAR TO pattern-match surface; a deep-but-narrow adt target like
    // dtm/xnum, so a moderate weight buys its coverage without starving
    // the table-driven modules.
    ModuleSpec { name: "like", default_weight: 0.8 },
    // subplan (SUBPLAN-1) emits self-contained SubPlan/InitPlan execution
    // batteries: an uncorrelated/correlated subquery driven through every
    // clause context (SELECT/WHERE/HAVING/GROUP BY/ORDER BY/CASE/func-arg/
    // VALUES/LIMIT), the ANY/ALL/IN/EXISTS sublink family (hashed vs
    // rescanned), nested SubPlan-in-SubPlan, multi-param InitPlans, SubPlan
    // inside aggregate args, and SubPlan under a forced Gather. Groups are
    // ~10-15 statements over a purpose-built fz_sp_* fixture (create + probes
    // + drop) — opt2/plansel-like low weight.
    ModuleSpec { name: "subplan", default_weight: 0.3 },
    // scalartypes (Track-B) emits one self-contained probe group per pick
    // over the niche fixed-width scalar types (money, uuid, pg_lsn, tid,
    // xid8, macaddr8) and their system functions — literal-driven I/O,
    // comparison, arithmetic and cast surfaces the AST modules never reach.
    // Light like numx/xnum.
    ModuleSpec { name: "scalartypes", default_weight: 0.7 },
    // inherit (INHERIT lane) emits self-contained classic table-inheritance
    // groups (CREATE ... INHERITS single/multi-parent, column/constraint
    // merge, ALTER propagation, attach/detach, ONLY-recursion DML, DROP
    // dependency) with catalog-bookkeeping probes and drops; coll-like
    // moderate weight — groups are ~15-30 statements.
    ModuleSpec { name: "inherit", default_weight: 0.5 },
    // bitstring drains the bit/varbit (varbit.c) function + operator + typmod
    // + cast surface as single literal-driven scalar/inline-VALUES SELECTs
    // (complements the adtmisc adtm:bit* random arms and the TYPEIO operator
    // lane); each pick is one cheap statement over a deep-but-narrow adt
    // surface, so a moderate weight buys its coverage like dtm/adtmisc.
    ModuleSpec { name: "bitstring", default_weight: 0.8 },
    // groupingsets (GROUPINGSETS lane) emits self-contained grouping-set
    // execution-drain groups (fixture create + 1-3 GUC-bracketed probes +
    // drop) over deterministic small or fixed-shape bulk fixtures; each
    // pick is a handful of statements, so it sits at a moderate below-
    // default weight like the other query-drain modules.
    ModuleSpec { name: "groupingsets", default_weight: 0.5 },
    // tablesample emits self-contained TABLESAMPLE / LIMIT-FETCH / DISTINCT
    // ON drain groups over a purpose-built fz_tsm_N fixture (create groups
    // are ~10 statements with bulk loads; query picks are 1-3 statements).
    // A moderate below-default weight keeps the create churn balanced like
    // spill/plansel.
    ModuleSpec { name: "tablesample", default_weight: 0.5 },
];

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Toggle {
    pub on: bool,
    pub weight: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToggleVector {
    /// Parallel to `REGISTRY`.
    entries: Vec<Toggle>,
}

impl ToggleVector {
    pub fn all_on() -> ToggleVector {
        ToggleVector {
            entries: REGISTRY
                .iter()
                .map(|m| Toggle { on: true, weight: m.default_weight })
                .collect(),
        }
    }

    /// Swarm-random sampling: each module independently on with probability
    /// 1/2, re-rolled until at least one module is on. Draws only from the
    /// session PRNG.
    pub fn swarm(rng: &mut Rng) -> ToggleVector {
        loop {
            let entries: Vec<Toggle> = REGISTRY
                .iter()
                .map(|m| Toggle { on: rng.chance(1, 2), weight: m.default_weight })
                .collect();
            if entries.iter().any(|t| t.on) {
                return ToggleVector { entries };
            }
        }
    }

    /// Parse a `--modules` spec like `expr=on,joins=off` or
    /// `expr=on:2.5` (weight after the colon). Unknown modules are errors.
    pub fn parse(spec: &str) -> Result<ToggleVector, String> {
        let mut tv = ToggleVector::all_on();
        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (name, value) = part
                .split_once('=')
                .ok_or_else(|| format!("bad module spec {:?}: expected name=on|off", part))?;
            let idx = REGISTRY
                .iter()
                .position(|m| m.name == name)
                .ok_or_else(|| format!("unknown module {:?}", name))?;
            let (state, weight) = match value.split_once(':') {
                Some((s, w)) => {
                    let w: f64 = w
                        .parse()
                        .map_err(|_| format!("bad weight {:?} for module {:?}", w, name))?;
                    if !(w > 0.0 && w.is_finite()) {
                        return Err(format!("weight for module {:?} must be finite and > 0", name));
                    }
                    (s, Some(w))
                }
                None => (value, None),
            };
            let on = match state {
                "on" => true,
                "off" => false,
                _ => {
                    return Err(format!(
                        "bad state {:?} for module {:?}: expected on or off",
                        state, name
                    ))
                }
            };
            tv.entries[idx].on = on;
            if let Some(w) = weight {
                tv.entries[idx].weight = w;
            }
        }
        if !tv.entries.iter().any(|t| t.on) {
            return Err("toggle vector disables every module".to_string());
        }
        Ok(tv)
    }

    /// Canonical spec string; `parse(spec_string(tv)) == tv`.
    pub fn spec_string(&self) -> String {
        let mut out = String::new();
        for (m, t) in REGISTRY.iter().zip(&self.entries) {
            if !out.is_empty() {
                out.push(',');
            }
            out.push_str(m.name);
            out.push('=');
            out.push_str(if t.on { "on" } else { "off" });
            if t.weight != m.default_weight {
                out.push(':');
                out.push_str(&format!("{}", t.weight));
            }
        }
        out
    }

    pub fn is_on(&self, name: &str) -> bool {
        REGISTRY
            .iter()
            .position(|m| m.name == name)
            .is_some_and(|i| self.entries[i].on)
    }

    /// Weighted pick among enabled modules; returns the module name.
    pub fn pick_module(&self, rng: &mut Rng) -> &'static str {
        let enabled: Vec<(usize, f64)> = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, t)| t.on)
            .map(|(i, t)| (i, t.weight))
            .collect();
        debug_assert!(!enabled.is_empty());
        let total: f64 = enabled.iter().map(|(_, w)| w).sum();
        let mut x = rng.f64_unit() * total;
        for &(i, w) in &enabled {
            if x < w {
                return REGISTRY[i].name;
            }
            x -= w;
        }
        REGISTRY[enabled.last().unwrap().0].name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_round_trip() {
        let tv = ToggleVector::parse("expr=on,joins=off").unwrap();
        assert_eq!(ToggleVector::parse(&tv.spec_string()).unwrap(), tv);
        assert!(!tv.is_on("joins"));
        assert!(tv.is_on("subq"));
        let tv = ToggleVector::parse("expr=on:2.5").unwrap();
        assert_eq!(ToggleVector::parse(&tv.spec_string()).unwrap(), tv);
        assert!(tv.is_on("expr"));
    }

    #[test]
    fn parse_rejects_unknown_and_bad() {
        assert!(ToggleVector::parse("windows=on").is_err());
        assert!(ToggleVector::parse("expr=maybe").is_err());
        assert!(ToggleVector::parse("expr=on:0").is_err());
        // All-off is rejected.
        assert!(
            ToggleVector::parse(
                "expr=off,joins=off,subq=off,agg=off,win=off,winfunc=off,dml=off,merge=off,txn=off,ddl=off,types=off,explain=off,util=off,part=off,partalt=off,objddl=off,idx=off,par=off,tsdl=off,cursor=off,views=off,matview=off,geo=off,dtm=off,dtx=off,adtmisc=off,sqljson=off,jsonpath=off,jsonfuncs=off,plpg=off,coll=off,mbconv=off,xnum=off,nodes=off,obs=off,admin=off,objid=off,einterp=off,exd=off,spill=off,earm=off,plansel=off,earm2=off,earm3=off,earm4=off,exr=off,exr2=off,numx=off,pubsub=off,ddldeep=off,pgram=off,opt2=off,opt3=off,cfgm=off,btbrin=off,heap=off,stats=off,tsrank=off,planner=off,partition=off,triggers=off,plpgsql=off,regex=off,mergex=off,aggwin=off,indexam=off,typeio=off,aclrls=off,lockcursor=off,largeobj=off,plancache=off,vacuum=off,seqident=off,ritrig=off,rangeops=off,udt=off,arrayops=off,altertable=off,byteaenc=off,srf=off,stringfunc=off,floatmath=off,numeric=off,ruleutils=off,cterec=off,castcoerce=off,intops=off,expreval=off,like=off,subplan=off,scalartypes=off,inherit=off,bitstring=off,groupingsets=off,tablesample=off"
            )
            .is_err()
        );
    }

    #[test]
    fn disabled_modules_are_never_picked() {
        let tv = ToggleVector::parse(
            "expr=on,joins=off,subq=off,agg=off,win=off,winfunc=off,dml=off,merge=off,txn=off,ddl=off,types=off,explain=off,util=off,part=off,partalt=off,objddl=off,idx=off,par=off,tsdl=off,cursor=off,views=off,matview=off,geo=off,dtm=off,dtx=off,adtmisc=off,sqljson=off,jsonpath=off,jsonfuncs=off,plpg=off,coll=off,mbconv=off,xnum=off,nodes=off,obs=off,admin=off,objid=off,einterp=off,exd=off,spill=off,earm=off,plansel=off,earm2=off,earm3=off,earm4=off,exr=off,exr2=off,numx=off,pubsub=off,ddldeep=off,pgram=off,opt2=off,opt3=off,cfgm=off,btbrin=off,heap=off,stats=off,tsrank=off,planner=off,partition=off,triggers=off,plpgsql=off,regex=off,mergex=off,aggwin=off,indexam=off,typeio=off,aclrls=off,lockcursor=off,largeobj=off,plancache=off,vacuum=off,seqident=off,ritrig=off,rangeops=off,udt=off,arrayops=off,altertable=off,byteaenc=off,srf=off,stringfunc=off,floatmath=off,numeric=off,ruleutils=off,cterec=off,castcoerce=off,intops=off,expreval=off,like=off,subplan=off,scalartypes=off,inherit=off,bitstring=off,groupingsets=off,tablesample=off"
        )
        .unwrap();
        let mut rng = Rng::new(5);
        for _ in 0..64 {
            assert_eq!(tv.pick_module(&mut rng), "expr");
        }
    }

    #[test]
    fn swarm_is_seed_deterministic() {
        let a = ToggleVector::swarm(&mut Rng::new(9));
        let b = ToggleVector::swarm(&mut Rng::new(9));
        assert_eq!(a, b);
        assert!(a.entries.iter().any(|t| t.on));
    }

    #[test]
    fn pick_module_returns_enabled() {
        let tv = ToggleVector::all_on();
        let mut rng = Rng::new(3);
        for _ in 0..64 {
            let m = tv.pick_module(&mut rng);
            assert!(tv.is_on(m));
        }
    }
}
