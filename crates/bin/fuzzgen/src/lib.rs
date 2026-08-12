//! fuzzgen: seeded SQL generator for the coverage-differential fuzzer
//! (charter: docs/design/coverage-differential-fuzzer.md).
//!
//! A session is (seed, toggle vector, statement budget). All randomness
//! flows from one seeded deterministic PRNG — no OS entropy anywhere — so
//! the same seed and toggle vector reproduce a byte-identical SQL stream.
//! The seed is the reproducibility witness and appears in every output
//! mode. Each statement carries production-level self-coverage metadata
//! (which grammar productions fired), the generator-side half of the
//! coverage-gap loop.

//!
//! F1 adds the differential runner half (bin: diffrunner): a minimal wire
//! client, dual-server lockstep apply, divergence classification with a
//! ruled-divergence table, a ddmin stream reducer, and a live catalog
//! loader behind the `CatalogSource` seam.
//!
//! F2b adds the local coverage loop: per-production bias weights
//! (`weights`, settable via `--weight` or one-line table edits — the
//! coverage-steering surface consumed by scripts/covloop.sh) and a
//! single-engine statement applier (bin: covapply).
//!
//! F4a generalizes generation to statement modules (`stmt` owns the
//! registry and per-statement context; see its module docs for the
//! three-step add-a-module recipe), moves expressions onto lexical scopes
//! (`scope` — alias-qualified column references, correlated outer chains),
//! and adds the joins (`join`) and subqueries/CTE (`subq`) modules plus a
//! multi-table fixture and the ORDER BY/LIMIT/OFFSET suffix production.
//!
//! F4b adds the aggregates (`agg`) and window-functions (`win`) modules:
//! GROUP BY/HAVING/DISTINCT/grouping sets, the aggregate-call surface, and
//! windows with partitioning, frames and named WINDOW clauses — plus the
//! ruled-soft float-aggregate discipline (order-sensitive float aggregate
//! columns compare soft under the B1 ruling; render::soft_float_cols
//! carries the per-statement column mask into the differ).
//!
//! F4c makes the stream stateful: the DML module (`dml` — INSERT/UPDATE/
//! DELETE with ON CONFLICT, RETURNING, UPDATE..FROM, DELETE..USING, over a
//! pk-carrying fixture with session-persistent pk tracking) and the
//! transactions module (`txn` — BEGIN/COMMIT/ROLLBACK brackets with
//! savepoint windows). The runner grows state-sync probes (pk-ordered
//! SELECT * per table, strict-compared; divergence class STATE_DIFF) and
//! the reducer becomes bracket-aware (transaction brackets drop or stay
//! whole; a probe can be the reduction target).
//!
//! D1 adds the DDL module (`ddl`): CREATE/DROP/ALTER TABLE, indexes across
//! all five AMs, views, sequences, CREATE AGGREGATE, plpgsql triggers and
//! TRUNCATE, driven by a session-persistent generation-time catalog model
//! (`ddl::DdlState`). The session loop rebuilds an effective catalog after
//! every ddl group so the other modules target ddl-created tables, syncs
//! `DmlState` to it by name, and derives probe windows (`session::DdlWindow`)
//! so state probes cover exactly the ddl tables alive at probe time.
//!
//! E1 adds the EXPLAIN module (`explain` — read-only SELECTs wrapped in
//! EXPLAIN with COSTS OFF always, VERBOSE/FORMAT combos, low-weight
//! ANALYZE with TIMING/BUFFERS OFF; runtime resource counters masked by
//! the differ under Ruled("explain-counter")) and the utility module
//! (`util` — curated-GUC SET/RESET/SHOW, DISCARD PLANS/SEQUENCES, VACUUM/
//! ANALYZE, CHECKPOINT, COMMENT ON, PREPARE/EXECUTE/DEALLOCATE brackets).
//!
//! T2 adds the text-search language expansion (per-language word pools in
//! `rich` feeding every shipped snowball stemmer, ts_headline/rank arms)
//! and the text-search DDL module (`tsdl` — CREATE/ALTER/DROP TEXT SEARCH
//! CONFIGURATION/DICTIONARY with mapping surgery, driven by a
//! session-persistent `tsdl::TsState` object model, plus
//! to_tsvector/ts_debug/ts_lexize probe statements).
//! C1 adds the cursors + SQL-PREPARE module (`cursor`): DECLARE (BINARY /
//! INSENSITIVE / SCROLL / NO SCROLL / WITH HOLD) inside self-contained
//! transaction brackets, every FETCH/MOVE direction, CLOSE / CLOSE ALL,
//! WHERE CURRENT OF over FOR UPDATE cursors, and a parameterized
//! PREPARE/EXECUTE/DEALLOCATE pool whose execution counts accumulate
//! across groups so the plancache custom->generic plan flip happens
//! mid-stream (probed with EXPLAIN EXECUTE). Held cursors and the
//! prepared pool live in `cursor::CursorState`, swapped in and out by the
//! session loop like `DmlState`.
//! X1 adds the extended-protocol mode (`xproto` + `client::extended_query`):
//! a deterministic per-statement seeded choice between simple-query and
//! Parse/Bind/Execute/Sync paths (applied identically on both differential
//! sides; `--xproto <seed>` on diffrunner/covapply), with constant-literal
//! parameterization and Execute row limits + portal resume — plus small
//! generator gaps: recursive CTEs with SEARCH/CYCLE (`subq`), foreign keys
//! with referential actions (`ddl`), and deterministic system-view probes
//! (`util`).
//! V1 adds the views and rules module (`views`): CREATE [OR REPLACE] [TEMP]
//! VIEW with WITH [LOCAL|CASCADED] CHECK OPTION, materialized views with
//! REFRESH [CONCURRENTLY], explicit CREATE RULE on plain tables, and
//! pg_get_viewdef/pg_get_ruledef deparse probes. Registered views are
//! pushed into the effective catalog as DML targets, so the other modules'
//! DML flows through the view rewriter (rewriteTargetView).
//! A1 adds the geometric-types module (`geo`): literal pools for all seven
//! geo types with structured/degenerate coordinates, a data-driven operator
//! matrix enumerating C's pg_operator geo rows (verified identical on both
//! engines), the user-facing function and cast surfaces, and a small
//! deterministic geo data table with GiST polygon/circle indexes, KNN
//! ordering and churn — targeting geo_ops.c, the #1 uncovered adt file in
//! gap-report-005.
//! A2 adds the datetime cross-type matrix module (`dtm`): literal-only
//! scalar SELECTs spanning date/time/timestamp/timestamptz/interval/timetz
//! (+/- across every legal pair, overlaps, date_trunc/extract/date_part
//! over every field, date_bin, age, justify_*, make_*, AT TIME ZONE over a
//! fixed named-zone pool, to_char DCH incl. TZH/TZM, cross-casts + ::text
//! round-trips), richer datetime literal pools in `expr`, and the datetime
//! determinism session pin (runner::DATETIME_GUC_PIN: TimeZone/DateStyle/
//! IntervalStyle, applied to BOTH differential sides).
//! A3 adds the adt-misc breadth module (`adtmisc`): ACL/privilege
//! functions with self-contained GRANT/probe/REVOKE brackets over the
//! objddl role pool, the varbit + varlena/bytea long tail, multirange
//! completion (crossing objddl custom range types), and the float/numeric
//! edge matrix incl. money — all literal-driven Raw statements with
//! scalar outputs cast ::text for byte-exact comparison.
//! P2 adds the plpgsql-depth module (`plpg`): self-contained groups
//! covering plpgsql function bodies (control flow, loops with labels,
//! FOREACH, SELECT INTO STRICT, EXCEPTION blocks with SQLSTATE/SQLERRM,
//! GET DIAGNOSTICS, dynamic EXECUTE format()/USING, OUT/INOUT params,
//! RETURN NEXT/RETURN QUERY SRFs, procedures with in-proc COMMIT/ROLLBACK),
//! trigger depth (NEW/OLD mutation, row suppression, TG_OP/TG_ARGV, WHEN,
//! transition tables, deferred constraint triggers, INSTEAD OF on views,
//! alphabetical firing order, pg_get_triggerdef), and collation/operator/
//! opclass DDL (CREATE COLLATION libc C/POSIX forms with COLLATE probes,
//! CREATE OPERATOR int4 wrappers, CREATE OPERATOR CLASS/FAMILY in the
//! verified int4 form with indexes built through them). Every group drops
//! everything it creates and probes mutated tables pk-ordered in-group.
//! Q3 adds the node-serialization module (`nodes` — debug_print_parse/
//! rewritten/plan + compute_query_id brackets around the GEN-GAP utility
//! statements and the ordinary AST variety, plus stored-view bodies that
//! exercise readfuncs through pg_rewrite) and the observability module
//! (`obs` — the pg_stat_get_*/pg_stat_reset*/pgstat machinery surface
//! under a hand-verified deterministic-projection discipline), with the
//! matching `nodes-serial` config profile in scripts/lib/configprofiles.sh.
//! LD1 adds the object-identity + deparse drain module (`objid` — hand-
//! verified deck batteries instantiating every addressable OBJECT_* class
//! and every deparsable expression node, then driving pg_get_object_address/
//! pg_identify_object/pg_describe_object roundtrips and the pg_get_*def
//! deparse sweeps; line-drain-queue chunks objaddr-identity +
//! ruleutils-deparse).
//! LD5 adds the spill/alternate-arm module (`spill`): GUC-bracketed
//! forcing of the executor's memory-pressure arms (work_mem='64kB',
//! hash_mem_multiplier=1, enable_* toggles) over a purpose-built bulk
//! table — external merge sorts with scroll/hold cursors, serial
//! multi-batch and skew hash joins, HashAgg disk spill + sort-based
//! fallback, spilled tuplestores (window frames, materialized CTEs,
//! Materialize rescans), memoize eviction, incremental sort and the
//! CLUSTER tuplesort — draining the line-drain-queue `executor-spill`
//! chunk (docs/fuzzing/line-drain-queue.tsv).
//! LD7 adds the plan-selection sweep module (`plansel`): one
//! deterministic query emitted under a sweep of GUC plan profiles
//! (enable_* scan/join/agg toggles, cost-model extremes, geqo, collapse
//! limits, partitionwise/parallel forcing) over a path-rich fixture set
//! (multi-index + partial/expression indexes, co-partitioned twins,
//! generated columns, inlinable SQL functions) — the result set must be
//! identical across every forced plan, draining the line-drain-queue
//! `optimizer-arms` chunk.

pub mod admin;
pub mod adtmisc;
pub mod agg;
pub mod catalog;
pub mod client;
pub mod coll;
pub mod copybin;
pub mod copytext;
pub mod cursor;
pub mod dbddl;
pub mod ddl;
pub mod diff;
pub mod dml;
pub mod dtm;
pub mod dtmdec;
pub mod einterp;
pub mod exd;
pub mod earm;
pub mod earm2;
pub mod explain;
pub mod exr;
pub mod expr;
pub mod geo;
pub mod idx;
pub mod join;
pub mod livecat;
pub mod mbconv;
pub mod mbconv_data;
pub mod merge;
pub mod nodes;
pub mod numx;
pub mod objddl;
pub mod objid;
pub mod obs;
pub mod par;
pub mod part;
pub mod plansel;
pub mod plpg;
pub mod pubsub;
pub mod reduce;
pub mod render;
pub mod rich;
pub mod rng;
pub mod ruled;
pub mod runner;
pub mod scope;
pub mod session;
pub mod spill;
pub mod sqljson;
pub mod ssi;
pub mod stem_data;
pub mod stmt;
pub mod subq;
pub mod toggles;
pub mod tsdl;
pub mod txn;
pub mod types_stmt;
pub mod util;
pub mod views;
pub mod weights;
pub mod win;
pub mod xnum;
pub mod xproto;
