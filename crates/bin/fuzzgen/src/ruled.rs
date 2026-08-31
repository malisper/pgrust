//! Ruled-divergence table: known-acceptable divergence patterns mapped to
//! their ruling references, consulted before a diff becomes a finding. The
//! classifier (crate::diff) emits `DiffClass::Ruled` candidates carrying a
//! pattern id; entries here resolve them to a ruling. A candidate no entry
//! covers escalates back to a real finding — the table is load-bearing,
//! not decorative.

use crate::diff::{Classified, DiffClass};

/// Candidate pattern a table entry can match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuledPattern {
    /// Rowsets equal only within float ulp tolerance.
    FloatUlp,
    /// ORDER BY present, ordered compare differs, multiset compare equal
    /// (tie ordering under an underdetermined sort key).
    TieOrder,
    /// Same multiset-equal shape, but on a COPY statement: COPY/heap row
    /// order is ruled non-surface.
    CopyOrder,
    /// Rowsets equal outside generator-marked order-sensitive float
    /// aggregate columns (ruled-soft: plan-dependent accumulation order
    /// makes their divergence unbounded in ulp terms).
    FloatAggSoft,
    /// EXPLAIN output equal after masking runtime resource counters
    /// (Sort Method / Memory / Buckets / Batches / Disk): those are
    /// implementation state, not planner conformance. Plan structure
    /// still compares strictly (a structural diff is a real finding).
    ExplainCounter,
    /// EXPLAIN output equal after additionally masking wall-clock timing
    /// text ("actual time=" digits; the counter mask already covers
    /// Planning/Execution Time and Memory/Buffers). Opt-in only: the
    /// classifier emits the candidate solely when the statement's lane
    /// set DiffInput::mask_explain_timing (gramwalk grammar-derived
    /// EXPLAIN ANALYZE, --mask-explain-timing replays). H1.
    ExplainTiming,
    /// EXPLAIN output equal after additionally dropping the TEXT
    /// "Planning:" buffer-usage block (header + indented Buffers /
    /// I/O Timings / Memory children). Whether the block prints AT ALL is
    /// session cache state: C's ExplainOnePlan gates the whole TEXT group
    /// on any planning-time buffer being touched, which a warm backend
    /// avoids — the same C server answers 8 rows cold and 6 rows warm.
    /// pgrust's thread-shared catalog caches reach the warm state on
    /// different session histories than C's per-backend forks (r20
    /// update-rowcount, run 4ab3382e87..-59-13 seed 3878502244648856050).
    /// Plan structure, node-level Buffers, and Planning Time still
    /// compare strictly.
    ExplainPlanningBuffers,
    /// xml build-config divergence (F9 / LD1-N1): the pinned C oracle is
    /// a no-libxml build whose every XML path short-circuits with 0A000
    /// "unsupported XML feature"; pgrust deliberately dlopens libxml2
    /// (adt_xml "never a stub") and executes XML natively. The classifier
    /// emits the candidate only when the A side raised exactly that
    /// message and the B side did not panic (XX000 still escalates) —
    /// the oracle offers no behavioural signal on these statements.
    XmlConfig,
    /// SHOW ALL / pg_settings GUC-inventory row-count divergence (F4):
    /// pgrust deliberately ships extra `pgrust.*` GUCs and retuned
    /// defaults (docs/design/env-to-guc.md, jit-parallel-defaults.md
    /// DIVERGENCE NOTICEs; util module docs skip SHOW ALL for the same
    /// reason). Row-count / count(*) shape only — a wrong GUC *value*
    /// never produces this candidate and stays a finding.
    GucInventory,
    /// Text cells equal after masking `'<digits>'::oid` literals whose
    /// value is >= FirstNormalObjectId (round-7 FP-2): deparse functions
    /// (`pg_get_partition_constraintdef` → `satisfies_hash_partition`,
    /// `pg_get_expr` on partbound) embed user-object OIDs as literals,
    /// and user-range OIDs are arbitrary across two independently-
    /// evolving clusters. Builtin OID literals still compare exactly.
    OidLiteral,
    /// Text cells equal after masking user-range `pg_toast_<digits>`
    /// relation names (round-8): TOAST relations are named after the
    /// owning table's OID, so any surface printing them (reltoastrelid
    /// joins, pg_class scans, deparse) diverges across independently-
    /// evolving clusters. Catalog toast tables (builtin relids) still
    /// compare exactly.
    ToastName,
    /// Binary-mode (`*_send`) hex cells equal after masking embedded
    /// user-range type OIDs in the array/record wire images (round-7
    /// FP-5): `record_send` embeds each column's type OID, `array_send`
    /// the element type OID. Structural parse must consume the image
    /// exactly; every other byte still compares exactly.
    BinaryUdtOid,
    /// int4 results of a direct `*cmp()` builtin call equal in sign but
    /// not magnitude (round-7 FP-6): C's memcmp-convention comparators
    /// return arbitrary magnitude and SQL semantics only consume the
    /// sign. Emitted only when the statement calls a *cmp builtin and
    /// the sign-normalized rowsets agree.
    CmpMagnitude,
    /// A succeeded while B (the fault-injected SUT) errored exactly
    /// 55000 "parallel worker failed to initialize" (round-7 FP-4):
    /// Antithesis thread-pauses only the instrumented side, so worker
    /// bring-up can time out on B where the unfaulted in-container
    /// oracle sails through. Worker-acquisition liveness is owned (and
    /// passing) in the liveness campaign's parallel canary. Any other
    /// B-only 55000 message still escalates.
    ParallelWorkerInit,
    /// A succeeded while B errored exactly 57014 "canceling statement due
    /// to statement timeout" (round-20 soak): the FP-4 sibling — thread
    /// pauses hit only the instrumented SUT, so a deck-set symmetric
    /// statement_timeout can expire on B (stream statement or state
    /// probe) where the unfaulted oracle finishes. Any other B-only
    /// 57014 message (e.g. a cancel request) still escalates.
    FaultStmtTimeout,
    /// A succeeded while B errored exactly 40P01 "deadlock detected" on a
    /// DROP TABLE statement (round-20 soak): a hard lock cycle between
    /// the DROP and an autovacuum ANALYZE propagating partition stats to
    /// ancestors — genuine upstream C behavior (C cancels autovacuum only
    /// on cycle-free blocking), B-only because thread pauses widen the
    /// collision window on the instrumented side. pgrust's
    /// RemoveRelations lock order was verified against tablecmds.c.
    /// Any non-DROP-TABLE 40P01 asymmetry still escalates. r21 widened
    /// the statement scope to VACUUM FULL (same AccessExclusive-vs-
    /// autoanalyze hard cycle on cluster_rel).
    DropAutovacuumDeadlock,
    /// A succeeded while B errored exactly 23505 on
    /// pg_db_role_setting_databaseid_rol_index under an ALTER ROLE ALL
    /// SET statement (r21): concurrent batches race the (0, 0) singleton
    /// row; C's AlterSetting is scan-then-insert with no unique-violation
    /// recovery, so the same interleaving 23505s in C — B-only visibility
    /// is the shared instrumented SUT vs the idle dedicated oracle. Any
    /// other 23505 asymmetry still escalates.
    RoleSettingSharedRace,
    /// Rowset/row-count diff on a statement referencing an instance-config
    /// introspection view (round-9 FP-9): pg_hba_file_rules /
    /// pg_ident_file_mappings / pg_file_settings / pg_shmem_allocations*
    /// reflect the INSTANCE (config files on disk, shmem layout), not the
    /// schema — the Antithesis cluster pair provably runs different
    /// pg_hba.conf files (row count 6 vs 7, run b627b97f...-59-13).
    /// FP-9b widens the family to live-state views (pg_stat_progress_*
    /// by prefix, pg_stat_activity): concurrent sessions' in-flight
    /// commands legitimately appear on one cluster only. Round-10 FP-10
    /// adds pg_locks — cluster-global live lock state, same mechanism
    /// (a concurrent batch's ungranted lock showed up on one side only).
    /// The generators emit only existence shapes there; this class
    /// catches gramwalk-derived references. Error outcomes still compare
    /// strictly.
    InstanceConfig,
    /// B refused a statement with pgrust's ratified UTF-8-only
    /// server-encoding carve message (round-10 FP-11): only UTF8 and
    /// SQL_ASCII server encodings are accepted (RATIFIED 2026-08-18 by
    /// Michael, docs/design/carve-ratifications.md §11; gates:
    /// createdb.rs server_encoding_gate, postinit
    /// check_database_encoding_supported). C has no such carve, so it
    /// succeeds or fails for its own reasons (e.g. 22023 encoding-vs-
    /// locale mismatch) — either way the pair carries no conformance
    /// signal. Emitted only on the exact B-side 0A000 carve-citation
    /// message; any other encoding error still escalates, as does an
    /// A-side XX000.
    EncodingCarve,
    /// Rowset diff on a statement calling a backup-control function
    /// (pg_backup_start/stop, pg_switch_wal, pg_create_restore_point):
    /// their LSN/label results are cluster-local WAL positions (round-9
    /// covdiff: raw `SELECT pg_backup_start('fz_q5_dup', true)` returned
    /// A-only `0/16000028`). Generators project these; the class covers
    /// grammar-derived raw calls.
    InstanceLsn,
    /// lz4 build-config divergence, mirror of XmlConfig (round-9
    /// covdiff): an oracle configured --without-lz4 rejects every lz4
    /// surface 0A000 "compression method lz4 not supported" while pgrust
    /// is always lz4-capable and proceeds. Emitted only on that exact
    /// A-side message with B not panicking (XX000 still escalates).
    Lz4Config,
    /// B-only 22P02 "invalid input syntax for type tid" where A
    /// succeeded (round-9 covdiff, e.g. `SELECT '(0,)'::tid`): C 18.3's
    /// tidin strtol tolerance accepts malformed forms; reported upstream
    /// and fixed in later PostgreSQL versions — pgrust deliberately
    /// implements the fixed strict behavior. Engine stays unchanged.
    TidInputUpstream,
    /// EXPLAIN of DECLARE ... SCROLL CURSOR equal after stripping C's
    /// top-level Materialize wrap (round-9 RB-10): C's planner wraps a
    /// SCROLL cursor plan in Materialize when the plan cannot scan
    /// backwards (planner.c:444-451); pgrust deliberately deleted the
    /// wrap because every backward cursor read is served by the portal
    /// tuplestore (ratified strategy divergence, Michael 2026-07-17,
    /// notes/se-wave10-integration.md §5 item 2). Any other structural
    /// plan difference still escalates.
    ScrollMaterialize,
    /// One side raised C's catalog-row concurrency error XX000 "tuple
    /// concurrently updated/deleted" (simple_heap_update) on SHARED-catalog
    /// DDL (ALTER ROLE/DATABASE/TABLESPACE, shared COMMENT). Both engines
    /// raise it verbatim under a cross-session race, and shared catalogs
    /// are the one surface concurrent driver instances still share after
    /// the private-per-batch-DB split — timing, not conformance. Emitted
    /// only for that exact message; every other XX000 stays panic-class.
    SharedCatalogTcu,
    /// One side raised F0000 "could not parse contents of file
    /// \"postgresql.auto.conf\"" on an ALTER SYSTEM statement while the
    /// other did not (round-10 RB-14). postgresql.auto.conf is
    /// INSTANCE-global — every concurrent driver batch shares it — and
    /// both engines carry C 18's own self-poisoning behavior
    /// bug-for-bug (ALTER SYSTEM accepts 3+-component custom-GUC names
    /// that the config-file lexer's two-component QUALIFIED_ID cannot
    /// re-read; verified byte-identical across the full local A/B
    /// matrix, including reload no-op and restart FATAL). Whichever
    /// side a racing batch's poisoning write lands on first errors
    /// F0000 alone — interleaving, not conformance. Exact-message
    /// scope; any other F0000 or non-ALTER-SYSTEM statement escalates.
    AutoconfSharedRace,
}

#[derive(Clone, Debug)]
pub struct RuledEntry {
    pub id: &'static str,
    pub ruling: &'static str,
    pub pattern: RuledPattern,
}

/// The seeded table. Order matters: first match wins (copy-order shadows
/// the generic tie-order entry for COPY statements).
pub fn default_table() -> Vec<RuledEntry> {
    vec![
        RuledEntry {
            id: "b1-float-ulp",
            ruling: "B1 float-reassociation ruling: float surfaces compare by ulp, not text",
            pattern: RuledPattern::FloatUlp,
        },
        RuledEntry {
            id: "b1-float-agg-soft",
            ruling: "B1 float-reassociation ruling: order-sensitive float aggregate \
                     result columns are ruled-soft (accumulation order is plan-dependent \
                     and cancelling subsets exceed any fixed ulp bound)",
            pattern: RuledPattern::FloatAggSoft,
        },
        RuledEntry {
            id: "copy-order",
            ruling: "COPY order ruling: COPY/heap row order is nondeterministic, non-surface",
            pattern: RuledPattern::CopyOrder,
        },
        RuledEntry {
            id: "explain-counter",
            ruling: "EXPLAIN runtime resource counters (sort method, memory, hash \
                     buckets/batches, disk) are implementation state, never compared; \
                     plan structure under COSTS OFF still compares strictly",
            pattern: RuledPattern::ExplainCounter,
        },
        RuledEntry {
            id: "explain-timing",
            ruling: "EXPLAIN ANALYZE wall-clock timing text (actual time=) is \
                     never comparable between engines; masked only for opt-in \
                     lanes (gramwalk grammar-derived EXPLAIN ANALYZE cannot \
                     carry TIMING OFF); plan structure and actual rows still \
                     compare strictly",
            pattern: RuledPattern::ExplainTiming,
        },
        RuledEntry {
            id: "explain-planning-buffers",
            ruling: "EXPLAIN TEXT Planning: buffer-usage block presence is \
                     session cache state (a warm backend plans without \
                     touching buffers and C omits the whole group; pgrust's \
                     thread-shared caches warm on different histories than \
                     C's per-backend forks); plan structure, node-level \
                     Buffers, and Planning Time still compare strictly",
            pattern: RuledPattern::ExplainPlanningBuffers,
        },
        RuledEntry {
            id: "xml-config",
            ruling: "xml build-config ruling (LD1-N1, banked): the pinned C \
                     oracle is built without libxml while pgrust dlopens \
                     libxml2 by design; statements the oracle rejects 0A000 \
                     'unsupported XML feature' carry no oracle signal — a \
                     pgrust panic (XX000) on the same statement still \
                     escalates",
            pattern: RuledPattern::XmlConfig,
        },
        RuledEntry {
            id: "guc-inventory",
            ruling: "GUC-inventory ruling: pgrust deliberately diverges on the \
                     pg_settings / SHOW ALL inventory (extra pgrust.* GUCs, \
                     retuned defaults) — docs/design/env-to-guc.md and \
                     docs/design/jit-parallel-defaults.md DIVERGENCE NOTICEs; \
                     row-count shape only, GUC values still compare strictly",
            pattern: RuledPattern::GucInventory,
        },
        RuledEntry {
            id: "instance-config",
            ruling: "round-9 FP-9/FP-9b instance-config ruling: \
                     pg_hba_file_rules / pg_ident_file_mappings / \
                     pg_file_settings / pg_shmem_allocations reflect the \
                     instance (config files, shmem layout), not the \
                     schema, and the two clusters are provisioned \
                     independently; pg_stat_progress_* / pg_stat_activity \
                     / pg_locks (round-10 FP-10) are live instance state \
                     (concurrent sessions' commands and locks appear on \
                     one side only) — rowset shape only, error outcomes \
                     still compare strictly \
                     (notes/antithesis/round9-triage-2026-08-24.md, \
                     round10-60m-classification.md FP-10)",
            pattern: RuledPattern::InstanceConfig,
        },
        RuledEntry {
            id: "encoding-carve",
            ruling: "UTF-8-only server-encoding carve ruling (round-10 \
                     FP-11): only UTF8 and SQL_ASCII server encodings are \
                     accepted by pgrust — RATIFIED 2026-08-18 by Michael, \
                     docs/design/carve-ratifications.md §11 (gates: \
                     createdb.rs server_encoding_gate, postinit \
                     check_database_encoding_supported). A B-side 0A000 \
                     refusal carrying the carve citation is the carve \
                     operating as ratified and carries no oracle signal \
                     whatever C did with the same statement; exact-message \
                     scope, every other encoding error still escalates",
            pattern: RuledPattern::EncodingCarve,
        },
        RuledEntry {
            id: "instance-lsn",
            ruling: "round-9 instance-LSN ruling: backup-control results \
                     (pg_backup_start/stop LSNs, pg_switch_wal, \
                     pg_create_restore_point) are cluster-local WAL \
                     positions, never cross-cluster comparable; \
                     generators project them, this entry covers \
                     grammar-derived raw calls",
            pattern: RuledPattern::InstanceLsn,
        },
        RuledEntry {
            id: "lz4-config",
            ruling: "round-9 lz4 build-config ruling (mirror of \
                     xml-config): a --without-lz4 oracle rejects 0A000 \
                     'compression method lz4 not supported' where pgrust \
                     is always lz4-capable; statements the oracle rejects \
                     this way carry no oracle signal — a pgrust panic \
                     (XX000) on the same statement still escalates",
            pattern: RuledPattern::Lz4Config,
        },
        RuledEntry {
            id: "tid-input-upstream",
            ruling: "round-9 tid-input ruling (project owner): C 18.3 \
                     tidin accepts malformed forms like '(0,)' via strtol \
                     tolerance; reported upstream and fixed in later \
                     PostgreSQL versions — pgrust deliberately matches \
                     the fixed strict behavior; exact-signature scope \
                     (B-only 22P02 'invalid input syntax for type tid', \
                     A succeeded)",
            pattern: RuledPattern::TidInputUpstream,
        },
        RuledEntry {
            id: "shared-catalog-tcu",
            ruling: "shared-catalog concurrency ruling (2026-08-21 soak): \
                     XX000 'tuple concurrently updated/deleted' on shared-\
                     catalog DDL is C's own simple_heap_update race behavior \
                     under concurrent driver instances; single-session replay \
                     cannot reproduce it. Exact-message scope — any other \
                     XX000 escalates as panic-class",
            pattern: RuledPattern::SharedCatalogTcu,
        },
        RuledEntry {
            id: "autoconf-shared-race",
            ruling: "round-10 RB-14 autoconf shared-state ruling: \
                     postgresql.auto.conf is instance-global and both \
                     engines replicate C 18's multi-dot custom-GUC \
                     self-poisoning identically (local A/B sweep \
                     2026-08-25: acceptance SQLSTATEs, auto.conf bytes, \
                     keyword-segment grammar classes, reload and \
                     restart-FATAL all byte-identical), so an \
                     asymmetric F0000 'could not parse contents of \
                     file postgresql.auto.conf' on ALTER SYSTEM under \
                     concurrent batches is write/read interleaving, \
                     not conformance; exact-message scope \
                     (notes/internal classification notes)",
            pattern: RuledPattern::AutoconfSharedRace,
        },
        RuledEntry {
            id: "oid-literal",
            ruling: "round-7 OID ruling: user-object OIDs (>= 16384) are not \
                     comparable across two independently-evolving clusters; \
                     deparse text embedding them ('<n>'::oid literals) is \
                     masked, all other text compares exactly",
            pattern: RuledPattern::OidLiteral,
        },
        RuledEntry {
            id: "toast-name",
            ruling: "round-8 OID ruling: TOAST relation names embed the \
                     owning table's user-range OID (pg_toast_<n>), which is \
                     cluster-local allocator state — same family as the \
                     round-7 oid-literal ruling; builtin catalog toast \
                     names still compare exactly",
            pattern: RuledPattern::ToastName,
        },
        RuledEntry {
            id: "binary-udt-oid",
            ruling: "round-7 OID ruling: embedded user-range type oids in \
                     record_send/array_send binary images are cluster-local \
                     allocator state; masked structurally, every other byte \
                     of the image still compares exactly",
            pattern: RuledPattern::BinaryUdtOid,
        },
        RuledEntry {
            id: "cmp-magnitude",
            ruling: "round-7 cmp-magnitude ruling: C's memcmp-convention \
                     *cmp() comparators return arbitrary magnitude, sign \
                     always matches, and SQL semantics consume only the \
                     sign; scope is direct *cmp() select-list calls with \
                     sign-equal int4 results",
            pattern: RuledPattern::CmpMagnitude,
        },
        RuledEntry {
            id: "parallel-worker-init",
            ruling: "round-7 asymmetric-fault ruling: thread-pause faults \
                     hit only the instrumented SUT, so B-only 55000 \
                     'parallel worker failed to initialize' is injected \
                     scheduling, not conformance; exact-message scope, \
                     liveness owned by the parallel canary campaign",
            pattern: RuledPattern::ParallelWorkerInit,
        },
        RuledEntry {
            id: "fault-stmt-timeout",
            ruling: "round-20 asymmetric-fault ruling: thread-pause faults \
                     hit only the instrumented SUT, so B-only 57014 \
                     'canceling statement due to statement timeout' under \
                     the decks' symmetric statement_timeout SETs is \
                     injected scheduling, not conformance; exact-message \
                     scope, statement responsiveness owned by the \
                     liveness cancel-ladder campaign",
            pattern: RuledPattern::FaultStmtTimeout,
        },
        RuledEntry {
            id: "drop-autovacuum-deadlock",
            ruling: "round-20 symmetric-race ruling: DROP TABLE vs \
                     autovacuum-ANALYZE ancestor-stats propagation forms a \
                     hard lock cycle that 40P01s in C too (deadlock.c \
                     cancels autovacuum only when no cycle exists); B-only \
                     visibility is fault-widened autovacuum timing on the \
                     shared SUT; DROP TABLE / VACUUM FULL + exact-message \
                     scope (r21 widened to VACUUM FULL — same cycle on \
                     cluster_rel), detector correctness owned by the \
                     liveness deadlock campaign",
            pattern: RuledPattern::DropAutovacuumDeadlock,
        },
        RuledEntry {
            id: "role-setting-shared-race",
            ruling: "r21 symmetric-race ruling: concurrent ALTER ROLE ALL \
                     SET batches race the (0, 0) pg_db_role_setting \
                     singleton; C's AlterSetting is scan-then-insert with \
                     no unique-violation recovery, so the interleaving \
                     23505s in C too; B-only visibility is the shared \
                     instrumented SUT vs the idle dedicated oracle; ALTER \
                     ROLE ALL + exact constraint-name scope",
            pattern: RuledPattern::RoleSettingSharedRace,
        },
        RuledEntry {
            id: "scroll-materialize",
            ruling: "SCROLL-Materialize ruling (round-9 RB-10): pgrust \
                     deliberately omits C's planner Materialize wrap for \
                     SCROLL cursors — backward reads are served by the \
                     portal tuplestore (ratified strategy divergence, \
                     Michael 2026-07-17, notes/se-wave10-integration.md \
                     §5 item 2); EXPLAIN DECLARE ... SCROLL therefore \
                     shows the wrap only on the C side. Scope: the diff \
                     must vanish once the top-level Materialize wrap is \
                     stripped from A; any other plan-shape difference \
                     still escalates",
            pattern: RuledPattern::ScrollMaterialize,
        },
        RuledEntry {
            id: "tie-ordering",
            ruling: "docs/conformance/tie-ordering.md: tie order under underdetermined ORDER BY",
            pattern: RuledPattern::TieOrder,
        },
    ]
}

fn is_copy_stmt(sql: &str) -> bool {
    let head = sql.trim_start();
    head.len() >= 4 && head[..4].eq_ignore_ascii_case("COPY")
}

fn matches(entry: &RuledEntry, candidate: &str, sql: &str) -> bool {
    match entry.pattern {
        RuledPattern::FloatUlp => candidate == "float-ulp",
        RuledPattern::FloatAggSoft => candidate == "float-agg",
        RuledPattern::CopyOrder => candidate == "tie-order" && is_copy_stmt(sql),
        RuledPattern::TieOrder => candidate == "tie-order" && !is_copy_stmt(sql),
        RuledPattern::ExplainCounter => {
            candidate == "explain-counter" && crate::diff::is_explain_stmt(sql)
        }
        RuledPattern::ExplainTiming => {
            candidate == "explain-timing" && crate::diff::is_explain_stmt(sql)
        }
        RuledPattern::ExplainPlanningBuffers => {
            candidate == "explain-planning-buffers" && crate::diff::is_explain_stmt(sql)
        }
        RuledPattern::GucInventory => {
            candidate == "guc-inventory" && crate::diff::is_guc_inventory_stmt(sql)
        }
        // Emitted only when the oid-literal normalization itself made the
        // rowsets equal; no reliable SQL-text refinement exists (deparse
        // output reaches SELECTs through many catalog functions).
        RuledPattern::OidLiteral => candidate == "oid-literal",
        // Emitted only when the toast-name normalization itself made the
        // rowsets equal; toast names reach text through many surfaces.
        RuledPattern::ToastName => candidate == "toast-name",
        // Emitted only when the structural image parse + mask made the
        // cells equal — the parse IS the refinement.
        RuledPattern::BinaryUdtOid => candidate == "binary-udt-oid",
        RuledPattern::CmpMagnitude => {
            candidate == "cmp-magnitude" && crate::diff::calls_cmp_builtin(sql)
        }
        // Emitted only on the exact A-success/B-55000 message signature.
        RuledPattern::ParallelWorkerInit => candidate == "parallel-worker-init",
        // Emitted only on the exact A-success/B-57014 message signature.
        RuledPattern::FaultStmtTimeout => candidate == "fault-stmt-timeout",
        RuledPattern::DropAutovacuumDeadlock => {
            candidate == "drop-autovacuum-deadlock"
                && (crate::diff::is_drop_table_stmt(sql)
                    || crate::diff::is_vacuum_full_stmt(sql))
        }
        RuledPattern::RoleSettingSharedRace => {
            candidate == "role-setting-shared-race"
                && crate::diff::is_alter_role_all_stmt(sql)
        }
        // The candidate is emitted only on the A-side NO_XML_SUPPORT
        // message signature; there is no reliable SQL-text refinement
        // (xml reaches casts, xmlserialize, table functions, ...).
        RuledPattern::XmlConfig => candidate == "xml-config",
        RuledPattern::SharedCatalogTcu => {
            candidate == "shared-catalog-tcu" && crate::diff::is_shared_catalog_stmt(sql)
        }
        RuledPattern::AutoconfSharedRace => {
            candidate == "autoconf-shared-race" && crate::diff::is_alter_system_stmt(sql)
        }
        RuledPattern::InstanceConfig => {
            candidate == "instance-config" && crate::diff::is_instance_config_stmt(sql)
        }
        RuledPattern::InstanceLsn => {
            candidate == "instance-lsn" && crate::diff::calls_backup_control(sql)
        }
        // Emitted only on the exact B-side 0A000 carve-citation message
        // (round-10 FP-11); the message signature is the refinement, and
        // the carve is reachable without the token "encoding" in the SQL
        // (gramwalk numeric encoding operands), so no text refinement is
        // reliable.
        RuledPattern::EncodingCarve => candidate == "encoding-carve",
        // Emitted only on the exact A-side no-lz4 0A000 message signature
        // (build-config family; same emission-scoped rule as xml-config).
        RuledPattern::Lz4Config => candidate == "lz4-config",
        // Emitted only on the exact A-success/B-22P02-tid signature; tid
        // input can be reached without the token "tid" in the SQL (COPY,
        // insert into a tid column), so no text refinement is reliable.
        RuledPattern::TidInputUpstream => candidate == "tid-input-upstream",
        RuledPattern::ScrollMaterialize => {
            candidate == "scroll-materialize"
                && crate::diff::is_scroll_declare_explain_stmt(sql)
        }
    }
}

/// Resolve a raw classification against the table. `Ruled` candidates that
/// match an entry come back stamped with the entry id and ruling reference;
/// unmatched candidates escalate to ROWSET_DIFF so nothing is silently
/// accepted without a ruling on file.
pub fn apply_ruled(table: &[RuledEntry], sql: &str, raw: Classified) -> Classified {
    let DiffClass::Ruled(candidate) = &raw.class else {
        return raw;
    };
    for entry in table {
        if matches(entry, candidate, sql) {
            return Classified {
                class: DiffClass::Ruled(entry.id.to_string()),
                detail: format!("{} [{}]", raw.detail, entry.ruling),
            };
        }
    }
    Classified {
        class: DiffClass::RowsetDiff,
        detail: format!("unruled divergence candidate {candidate:?}: {}", raw.detail),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str) -> Classified {
        Classified { class: DiffClass::Ruled(id.to_string()), detail: "d".to_string() }
    }

    #[test]
    fn float_ulp_resolves_to_b1() {
        let out = apply_ruled(&default_table(), "SELECT f FROM t;", candidate("float-ulp"));
        assert_eq!(out.class, DiffClass::Ruled("b1-float-ulp".to_string()));
        assert!(out.detail.contains("B1"));
    }

    #[test]
    fn fault_stmt_timeout_resolves() {
        let out = apply_ruled(
            &default_table(),
            "SELECT * FROM fz_par_0 ORDER BY pk;",
            candidate("fault-stmt-timeout"),
        );
        assert_eq!(out.class, DiffClass::Ruled("fault-stmt-timeout".to_string()));
        assert!(out.detail.contains("round-20"));
    }

    #[test]
    fn float_agg_soft_resolves_to_b1_entry() {
        let out =
            apply_ruled(&default_table(), "SELECT sum(f) FROM t;", candidate("float-agg"));
        assert_eq!(out.class, DiffClass::Ruled("b1-float-agg-soft".to_string()));
        assert!(out.detail.contains("ruled-soft"));
        // Without the entry the candidate escalates to a finding.
        let out = apply_ruled(&[], "SELECT sum(f) FROM t;", candidate("float-agg"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn tie_order_resolves_to_tie_ordering_doc() {
        let out =
            apply_ruled(&default_table(), "SELECT c FROM t ORDER BY 1;", candidate("tie-order"));
        assert_eq!(out.class, DiffClass::Ruled("tie-ordering".to_string()));
        assert!(out.detail.contains("tie-ordering.md"));
    }

    #[test]
    fn copy_statement_shadows_tie_order() {
        let out = apply_ruled(&default_table(), "  copy t TO STDOUT;", candidate("tie-order"));
        assert_eq!(out.class, DiffClass::Ruled("copy-order".to_string()));
    }

    #[test]
    fn explain_counter_resolves_only_on_explain_statements() {
        let out = apply_ruled(
            &default_table(),
            "EXPLAIN (COSTS OFF, SUMMARY OFF, ANALYZE, TIMING OFF, BUFFERS OFF) SELECT 1;",
            candidate("explain-counter"),
        );
        assert_eq!(out.class, DiffClass::Ruled("explain-counter".to_string()));
        assert!(out.detail.contains("implementation state"));
        // A non-EXPLAIN statement carrying the candidate escalates.
        let out = apply_ruled(&default_table(), "SELECT 1;", candidate("explain-counter"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn guc_inventory_resolves_only_on_inventory_statements() {
        let out = apply_ruled(&default_table(), "show all ;", candidate("guc-inventory"));
        assert_eq!(out.class, DiffClass::Ruled("guc-inventory".to_string()));
        assert!(out.detail.contains("env-to-guc"));
        let out = apply_ruled(
            &default_table(),
            "select count(*) from pg_settings ;",
            candidate("guc-inventory"),
        );
        assert_eq!(out.class, DiffClass::Ruled("guc-inventory".to_string()));
        // Any other statement carrying the candidate escalates.
        let out = apply_ruled(&default_table(), "SHOW work_mem;", candidate("guc-inventory"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn explain_timing_resolves_only_on_explain_statements() {
        let out = apply_ruled(
            &default_table(),
            "explain analyze select 1 ;",
            candidate("explain-timing"),
        );
        assert_eq!(out.class, DiffClass::Ruled("explain-timing".to_string()));
        let out = apply_ruled(&default_table(), "SELECT 1;", candidate("explain-timing"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn explain_planning_buffers_resolves_only_on_explain_statements() {
        let out = apply_ruled(
            &default_table(),
            "explain analyse update fz_scalar * fz_scalar set k_text = default ;",
            candidate("explain-planning-buffers"),
        );
        assert_eq!(out.class, DiffClass::Ruled("explain-planning-buffers".to_string()));
        assert!(out.detail.contains("session cache state"));
        let out =
            apply_ruled(&default_table(), "SELECT 1;", candidate("explain-planning-buffers"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn autoconf_shared_race_resolves_only_on_alter_system() {
        let out = apply_ruled(
            &default_table(),
            "alter system reset flag . fz_scalar . trim ;",
            candidate("autoconf-shared-race"),
        );
        assert_eq!(out.class, DiffClass::Ruled("autoconf-shared-race".to_string()));
        assert!(out.detail.contains("RB-14"));
        // Any other statement carrying the candidate escalates.
        let out =
            apply_ruled(&default_table(), "SELECT 1;", candidate("autoconf-shared-race"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn unmatched_candidate_escalates() {
        let out = apply_ruled(&[], "SELECT f FROM t;", candidate("float-ulp"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
        assert!(out.detail.contains("unruled"));
    }

    #[test]
    fn non_candidates_pass_through() {
        let raw = Classified { class: DiffClass::Match, detail: String::new() };
        let out = apply_ruled(&default_table(), "SELECT 1;", raw.clone());
        assert_eq!(out.class, raw.class);
    }

    #[test]
    fn round7_oid_and_fault_candidates_resolve() {
        let out = apply_ruled(
            &default_table(),
            "select pg_get_partition_constraintdef(oid) from pg_class ;",
            candidate("oid-literal"),
        );
        assert_eq!(out.class, DiffClass::Ruled("oid-literal".to_string()));
        let out = apply_ruled(
            &default_table(),
            "select row('a','b')::fz_udt_c_0 ;",
            candidate("binary-udt-oid"),
        );
        assert_eq!(out.class, DiffClass::Ruled("binary-udt-oid".to_string()));
        let out = apply_ruled(
            &default_table(),
            "select count(*) from big ;",
            candidate("parallel-worker-init"),
        );
        assert_eq!(out.class, DiffClass::Ruled("parallel-worker-init".to_string()));
        let out = apply_ruled(
            &default_table(),
            "select relname from pg_class where relkind = 't' ;",
            candidate("toast-name"),
        );
        assert_eq!(out.class, DiffClass::Ruled("toast-name".to_string()));
    }

    #[test]
    fn cmp_magnitude_resolves_only_with_cmp_call() {
        let out = apply_ruled(
            &default_table(),
            "select uuid_cmp(a, b) from t ;",
            candidate("cmp-magnitude"),
        );
        assert_eq!(out.class, DiffClass::Ruled("cmp-magnitude".to_string()));
        // No *cmp call in the SQL: the candidate escalates.
        let out = apply_ruled(&default_table(), "select a from t ;", candidate("cmp-magnitude"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn instance_config_resolves_only_on_instance_view_statements() {
        for sql in [
            "SELECT rule_number FROM pg_hba_file_rules ORDER BY rule_number;",
            "select count(*) from PG_IDENT_FILE_MAPPINGS ;",
            "SELECT * FROM pg_file_settings;",
            "SELECT name FROM pg_shmem_allocations;",
            "SELECT name FROM pg_shmem_allocations_numa;",
            // FP-9b live-state family.
            "SELECT count(*) FROM pg_stat_progress_analyze;",
            "select phase from PG_STAT_PROGRESS_VACUUM ;",
            "SELECT state FROM pg_stat_activity;",
            // FP-10 (round-10) live lock state.
            "SELECT count(*) FROM pg_locks WHERE NOT granted;",
        ] {
            let out = apply_ruled(&default_table(), sql, candidate("instance-config"));
            assert_eq!(out.class, DiffClass::Ruled("instance-config".to_string()), "{sql}");
            assert!(out.detail.contains("round-9"), "{sql}");
        }
        // Any other statement carrying the candidate escalates.
        let out = apply_ruled(&default_table(), "SELECT * FROM t;", candidate("instance-config"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn instance_lsn_resolves_only_on_backup_control_statements() {
        let out = apply_ruled(
            &default_table(),
            "SELECT pg_backup_start('fz_q5_dup', true);",
            candidate("instance-lsn"),
        );
        assert_eq!(out.class, DiffClass::Ruled("instance-lsn".to_string()));
        let out = apply_ruled(
            &default_table(),
            "SELECT pg_switch_wal();",
            candidate("instance-lsn"),
        );
        assert_eq!(out.class, DiffClass::Ruled("instance-lsn".to_string()));
        let out = apply_ruled(&default_table(), "SELECT 1;", candidate("instance-lsn"));
        assert_eq!(out.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn round9_signature_scoped_candidates_resolve() {
        // Emission-scoped (like xml-config): the entry matches on any SQL.
        let out = apply_ruled(
            &default_table(),
            "ALTER TABLE dd_w ALTER COLUMN b SET COMPRESSION lz4;",
            candidate("lz4-config"),
        );
        assert_eq!(out.class, DiffClass::Ruled("lz4-config".to_string()));
        let out = apply_ruled(&default_table(), "SELECT '(0,)'::tid;", candidate("tid-input-upstream"));
        assert_eq!(out.class, DiffClass::Ruled("tid-input-upstream".to_string()));
        assert!(out.detail.contains("upstream"));
        // Without table entries both escalate — nothing silent.
        assert_eq!(apply_ruled(&[], "x;", candidate("lz4-config")).class, DiffClass::RowsetDiff);
        assert_eq!(
            apply_ruled(&[], "x;", candidate("tid-input-upstream")).class,
            DiffClass::RowsetDiff
        );
    }

    #[test]
    fn encoding_carve_candidate_resolves_with_charter_citation() {
        // Emission-scoped (like xml-config/lz4-config): the classifier
        // only emits the candidate on the exact B-side carve message,
        // so the entry matches on any SQL.
        let out = apply_ruled(
            &default_table(),
            "create database fuzz_gramwalk_210_1_json WITH encoding + 2 ;",
            candidate("encoding-carve"),
        );
        assert_eq!(out.class, DiffClass::Ruled("encoding-carve".to_string()));
        assert!(out.detail.contains("carve-ratifications.md"), "{}", out.detail);
        assert!(out.detail.contains("2026-08-18"), "{}", out.detail);
        // Without the table entry the candidate escalates — nothing silent.
        assert_eq!(
            apply_ruled(&[], "x;", candidate("encoding-carve")).class,
            DiffClass::RowsetDiff
        );
    }

    #[test]
    fn shared_catalog_tcu_resolves_on_shared_ddl_only() {
        let out = apply_ruled(
            &default_table(),
            "alter user current_user encrypted password 'x' ;",
            candidate("shared-catalog-tcu"),
        );
        assert_eq!(out.class, DiffClass::Ruled("shared-catalog-tcu".to_string()));
        // Database-local DDL never matches: the candidate escalates.
        let out = apply_ruled(
            &default_table(),
            "ALTER TABLE t ADD COLUMN c int4;",
            candidate("shared-catalog-tcu"),
        );
        assert_eq!(out.class, DiffClass::RowsetDiff);
        // Round-18: parameter-ACL DDL writes pg_parameter_acl (shared
        // catalog, RowExclusiveLock-only in C) — GRANT/REVOKE ... ON
        // PARAMETER qualifies.
        for sql in [
            "REVOKE SET ON PARAMETER work_mem FROM fz_role_3;",
            "  grant set, alter system on parameter jit to public;",
            "REVOKE GRANT OPTION FOR SET ON PARAMETER shared_buffers FROM r;",
        ] {
            let out = apply_ruled(&default_table(), sql, candidate("shared-catalog-tcu"));
            assert_eq!(
                out.class,
                DiffClass::Ruled("shared-catalog-tcu".to_string()),
                "{sql}"
            );
        }
        // Database-local grants never qualify.
        for sql in [
            "GRANT SELECT ON TABLE fz_scalar TO PUBLIC;",
            "REVOKE ALL ON SCHEMA public FROM r;",
            "GRANTED ON PARAMETER x;", // not the GRANT keyword
        ] {
            let out = apply_ruled(&default_table(), sql, candidate("shared-catalog-tcu"));
            assert_eq!(out.class, DiffClass::RowsetDiff, "{sql}");
        }
    }
}
