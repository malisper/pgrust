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
    /// Rowset/row-count diff on a statement referencing an instance-config
    /// introspection view (round-9 FP-9): pg_hba_file_rules /
    /// pg_ident_file_mappings / pg_file_settings / pg_shmem_allocations*
    /// reflect the INSTANCE (config files on disk, shmem layout), not the
    /// schema — the Antithesis cluster pair provably runs different
    /// pg_hba.conf files (row count 6 vs 7, run b627b97f...-59-13). The
    /// generators emit only existence shapes there; this class catches
    /// gramwalk-derived references. Error outcomes still compare strictly.
    InstanceConfig,
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
    /// One side raised C's catalog-row concurrency error XX000 "tuple
    /// concurrently updated/deleted" (simple_heap_update) on SHARED-catalog
    /// DDL (ALTER ROLE/DATABASE/TABLESPACE, shared COMMENT). Both engines
    /// raise it verbatim under a cross-session race, and shared catalogs
    /// are the one surface concurrent driver instances still share after
    /// the private-per-batch-DB split — timing, not conformance. Emitted
    /// only for that exact message; every other XX000 stays panic-class.
    SharedCatalogTcu,
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
            ruling: "round-9 FP-9 instance-config ruling: pg_hba_file_rules / \
                     pg_ident_file_mappings / pg_file_settings / \
                     pg_shmem_allocations reflect the instance (config \
                     files, shmem layout), not the schema, and the two \
                     clusters are provisioned independently — rowset \
                     shape only, error outcomes still compare strictly \
                     (notes/antithesis/round9-triage-2026-08-24.md)",
            pattern: RuledPattern::InstanceConfig,
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
        // The candidate is emitted only on the A-side NO_XML_SUPPORT
        // message signature; there is no reliable SQL-text refinement
        // (xml reaches casts, xmlserialize, table functions, ...).
        RuledPattern::XmlConfig => candidate == "xml-config",
        RuledPattern::SharedCatalogTcu => {
            candidate == "shared-catalog-tcu" && crate::diff::is_shared_catalog_stmt(sql)
        }
        RuledPattern::InstanceConfig => {
            candidate == "instance-config" && crate::diff::is_instance_config_stmt(sql)
        }
        RuledPattern::InstanceLsn => {
            candidate == "instance-lsn" && crate::diff::calls_backup_control(sql)
        }
        // Emitted only on the exact A-side no-lz4 0A000 message signature
        // (build-config family; same emission-scoped rule as xml-config).
        RuledPattern::Lz4Config => candidate == "lz4-config",
        // Emitted only on the exact A-success/B-22P02-tid signature; tid
        // input can be reached without the token "tid" in the SQL (COPY,
        // insert into a tid column), so no text refinement is reliable.
        RuledPattern::TidInputUpstream => candidate == "tid-input-upstream",
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
    }
}
