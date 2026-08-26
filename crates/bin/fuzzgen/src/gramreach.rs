//! gramreach: SQL-grammar reachability instrumentation for the Antithesis
//! sqldiff workload (direct Antithesis-team ask: "add reachability
//! assertions ... begin with the postgres grammar to see if we are able to
//! reach the whole of SQL syntaxes being offered").
//!
//! WHAT IS MEASURED — workload reach, at the generator layer: every gramwalk
//! derivation starts by expanding the grammar's `stmt` nonterminal through
//! exactly one of its direct productions (`SelectStmt`, `AlterTableStmt`,
//! `CreateTrigStmt`, ..., 124 named alternatives plus the empty statement in
//! the vendored PG 18.3 automaton). That first production IS the top-level
//! statement kind the workload offered the servers, so recording it makes
//! "which parts of the offered SQL grammar does the workload exercise"
//! directly measurable, with unreached territory visible in the triage
//! report and explicit targets for coverage steering.
//!
//! GRANULARITY (the cardinality decision) — one Antithesis property per
//! top-level statement kind would put ~125 rows in the property table and
//! the SDK requires every property name to be an inline string literal, so
//! kinds are bucketed into a curated set of 36 statement FAMILIES (select,
//! insert, drop, role, pubsub, ...), each a single `Reachable` property
//! whose details payload carries the exact production (`stmt_kind`) that
//! fired. Triage reads: a red (never reached) family is unreached grammar
//! territory; a green family's example details show which member kinds were
//! seen. The full per-kind resolution lives in the aggregate log line (see
//! below) and in each property's detail examples — without swamping the
//! property table. `family-other` is the drift catch-all: it fires only if
//! a future grammar bump adds a `stmt` alternative this table doesn't know,
//! so it doubles as a "mapping went stale" tripwire.
//!
//! MECHANICS — `gramwalk::walk_statement` calls [`record_stmt_rule`] with
//! the bison rule number of its first expansion (one call per generated
//! statement; a vector index + one match — cheap). All SDK calls and log
//! emission are compiled only under the `antithesis` cargo feature,
//! matching the crate's existing SDK gating (rng.rs); the bucketing logic
//! itself is feature-independent so it is unit-testable everywhere.
//!
//! AGGREGATE — whenever a new statement kind is first seen (and every 512
//! statements as a heartbeat) the workload log gets one line:
//! `gramwalk-reach: seen=<n>/<total> stmt kinds`, so overall grammar reach
//! is visible straight from the run logs without opening the property
//! table.

use gram_core::tables as gt;
use gram_core::tables::names::YYTNAME;
use pgsync::OnceLock;
#[cfg(any(test, feature = "antithesis"))]
use pgsync::Mutex;

/// Reported kind name for the empty `stmt` alternative (`stmt: /*EMPTY*/`).
pub const EMPTY_KIND: &str = "<empty>";

struct StmtRules {
    /// Rule number (1..=YYNRULES) -> dense kind id, for the direct
    /// productions of `stmt` only.
    kind_of_rule: Vec<Option<usize>>,
    /// Dense kind id -> kind name (the RHS nonterminal's YYTNAME, or
    /// [`EMPTY_KIND`]).
    kinds: Vec<&'static str>,
}

fn stmt_rules() -> &'static StmtRules {
    static RULES: OnceLock<StmtRules> = OnceLock::new();
    RULES.get_or_init(|| {
        let ntokens = gt::YYNTOKENS as usize;
        let stmt_sym = YYTNAME
            .iter()
            .position(|&n| n == "stmt")
            .expect("stmt nonterminal in YYTNAME");
        let mut kind_of_rule: Vec<Option<usize>> = vec![None; gt::YYNRULES + 1];
        let mut kinds: Vec<&'static str> = Vec::new();
        for r in 1..=gt::YYNRULES {
            if gt::YYR1[r] as usize != stmt_sym {
                continue;
            }
            let start = gt::YYPRHS[r] as usize;
            let kind = if gt::YYRHS[start] < 0 {
                EMPTY_KIND
            } else {
                // gram.y's `stmt` alternatives are each a single
                // nonterminal; take the first RHS symbol's name either way
                // so a future grammar bump cannot panic here.
                let sym = gt::YYRHS[start] as usize;
                debug_assert!(sym >= ntokens, "stmt production starts with a terminal?");
                YYTNAME[sym]
            };
            let id = kinds.iter().position(|&k| k == kind).unwrap_or_else(|| {
                kinds.push(kind);
                kinds.len() - 1
            });
            kind_of_rule[r] = Some(id);
        }
        assert!(!kinds.is_empty(), "no stmt productions found");
        StmtRules { kind_of_rule, kinds }
    })
}

/// Total number of distinct top-level statement kinds the grammar offers
/// (the denominator of the `gramwalk-reach` aggregate line).
pub fn total_stmt_kinds() -> usize {
    stmt_rules().kinds.len()
}

/// The top-level statement kind produced by bison rule `rule`, when that
/// rule is a direct production of `stmt`. Total over any input: unknown or
/// out-of-range rule numbers (including 0) return `None`, never panic.
pub fn stmt_kind_of_rule(rule: usize) -> Option<&'static str> {
    let sr = stmt_rules();
    sr.kind_of_rule
        .get(rule)
        .copied()
        .flatten()
        .map(|id| sr.kinds[id])
}

/// Curated family bucket for a top-level statement kind. Total: every input
/// maps somewhere ("other" is the drift catch-all for kinds a future
/// grammar bump might add). Family names are stable identifiers — they are
/// the suffixes of the Antithesis property names, so renaming one renames a
/// property and resets its history.
pub fn family_of(kind: &str) -> &'static str {
    match kind {
        "SelectStmt" => "select",
        "InsertStmt" => "insert",
        "UpdateStmt" => "update",
        "DeleteStmt" => "delete",
        "MergeStmt" => "merge",
        "TransactionStmt" | "LockStmt" | "ConstraintsSetStmt" => "txn",
        "DeclareCursorStmt" | "FetchStmt" | "ClosePortalStmt" => "cursor",
        "PrepareStmt" | "ExecuteStmt" | "DeallocateStmt" => "prepared",
        "ExplainStmt" => "explain",
        "CopyStmt" => "copy",
        "CreateStmt" | "CreateAsStmt" | "CreateForeignTableStmt" => "create-table",
        "AlterTableStmt" => "alter-table",
        "IndexStmt" | "ReindexStmt" => "index",
        "ViewStmt" | "CreateMatViewStmt" | "RefreshMatViewStmt" => "view",
        "CreateSeqStmt" | "AlterSeqStmt" => "sequence",
        "CreateSchemaStmt" => "schema",
        "DropStmt" | "DropOwnedStmt" | "RemoveAggrStmt" | "RemoveFuncStmt"
        | "RemoveOperStmt" | "DropCastStmt" | "DropTransformStmt" | "DropOpClassStmt"
        | "DropOpFamilyStmt" => "drop",
        "RenameStmt" | "AlterObjectSchemaStmt" | "AlterObjectDependsStmt"
        | "AlterOwnerStmt" => "alter-object",
        "AlterOperatorStmt" | "AlterTypeStmt" | "AlterCompositeTypeStmt" | "AlterEnumStmt"
        | "AlterDomainStmt" | "AlterCollationStmt" => "alter-type",
        "CreateDomainStmt" | "DefineStmt" | "CreateCastStmt" | "CreateTransformStmt"
        | "CreateConversionStmt" => "type-ddl",
        "CreateFunctionStmt" | "AlterFunctionStmt" | "DoStmt" | "CallStmt"
        | "CreatePLangStmt" => "function",
        "CreateTrigStmt" | "CreateEventTrigStmt" | "AlterEventTrigStmt" | "RuleStmt"
        | "CreateAssertionStmt" => "trigger-rule",
        "CreateRoleStmt" | "CreateUserStmt" | "CreateGroupStmt" | "AlterRoleStmt"
        | "AlterRoleSetStmt" | "AlterGroupStmt" | "DropRoleStmt" | "ReassignOwnedStmt" => "role",
        "GrantStmt" | "GrantRoleStmt" | "RevokeStmt" | "RevokeRoleStmt"
        | "AlterDefaultPrivilegesStmt" | "CreatePolicyStmt" | "AlterPolicyStmt"
        | "SecLabelStmt" => "privilege",
        "CreatedbStmt" | "DropdbStmt" | "AlterDatabaseStmt" | "AlterDatabaseSetStmt" => {
            "database"
        }
        "CreateTableSpaceStmt" | "DropTableSpaceStmt" | "AlterTblSpcStmt" => "tablespace",
        "CreateFdwStmt" | "AlterFdwStmt" | "CreateForeignServerStmt"
        | "AlterForeignServerStmt" | "CreateUserMappingStmt" | "AlterUserMappingStmt"
        | "DropUserMappingStmt" | "ImportForeignSchemaStmt" => "foreign",
        "CreateExtensionStmt" | "AlterExtensionStmt" | "AlterExtensionContentsStmt"
        | "CreateAmStmt" => "extension",
        "CreatePublicationStmt" | "AlterPublicationStmt" | "CreateSubscriptionStmt"
        | "AlterSubscriptionStmt" | "DropSubscriptionStmt" => "pubsub",
        "AlterTSConfigurationStmt" | "AlterTSDictionaryStmt" => "text-search",
        "CreateOpClassStmt" | "CreateOpFamilyStmt" | "AlterOpFamilyStmt" => "opclass",
        "CreateStatsStmt" | "AlterStatsStmt" => "statistics",
        "VacuumStmt" | "AnalyzeStmt" | "ClusterStmt" | "CheckPointStmt" | "TruncateStmt" => {
            "maintenance"
        }
        "VariableSetStmt" | "VariableResetStmt" | "VariableShowStmt" | "DiscardStmt"
        | "LoadStmt" | "AlterSystemStmt" => "session",
        "ListenStmt" | "UnlistenStmt" | "NotifyStmt" => "notify",
        "CommentStmt" => "comment",
        EMPTY_KIND => "empty",
        _ => "other",
    }
}

/// The full stable family roster (sorted, deduped) — the exact property
/// suffix set triage will see. Kept in one place so the unit tests pin it.
pub const FAMILIES: &[&str] = &[
    "alter-object",
    "alter-table",
    "alter-type",
    "comment",
    "copy",
    "create-table",
    "cursor",
    "database",
    "delete",
    "drop",
    "empty",
    "explain",
    "extension",
    "foreign",
    "function",
    "index",
    "insert",
    "maintenance",
    "merge",
    "notify",
    "opclass",
    "prepared",
    "privilege",
    "pubsub",
    "role",
    "schema",
    "select",
    "sequence",
    "session",
    "statistics",
    "tablespace",
    "text-search",
    "trigger-rule",
    "txn",
    "type-ddl",
    "update",
    "view",
];

/// Seen-kind tracker behind the aggregate log line. Feature-independent so
/// the accounting is unit-testable; only the emission side is
/// antithesis-gated.
#[cfg(any(test, feature = "antithesis"))]
struct Seen {
    seen: Vec<bool>,
    n_seen: usize,
    calls: u64,
}

#[cfg(any(test, feature = "antithesis"))]
fn seen() -> &'static Mutex<Seen> {
    static SEEN: OnceLock<Mutex<Seen>> = OnceLock::new();
    SEEN.get_or_init(|| {
        Mutex::new(Seen { seen: vec![false; total_stmt_kinds()], n_seen: 0, calls: 0 })
    })
}

/// Record + (`newly_seen`, `n_seen`, `calls`) for one observation of dense
/// kind id `id`. Split out of [`record_stmt_rule`] so the aggregate
/// accounting has direct unit tests.
#[cfg(any(test, feature = "antithesis"))]
fn note_seen(id: usize) -> (bool, usize, u64) {
    let mut s = pgsync::lock(seen());
    s.calls += 1;
    let newly = !s.seen[id];
    if newly {
        s.seen[id] = true;
        s.n_seen += 1;
    }
    (newly, s.n_seen, s.calls)
}

/// Heartbeat cadence of the aggregate line (statements between repeats when
/// no new kind shows up).
#[cfg(feature = "antithesis")]
const AGG_EVERY: u64 = 512;

/// Record the first expansion of a gramwalk derivation: `rule` is the bison
/// rule the walker chose for the `stmt` nonterminal. One call per generated
/// statement. Non-`stmt` rules are ignored (defensive; the walker only
/// passes its entry expansion). Everything observable — SDK assertions and
/// the aggregate log line — is compiled only under the `antithesis`
/// feature; without it this is a no-op.
pub fn record_stmt_rule(rule: usize) {
    let Some(kind) = stmt_kind_of_rule(rule) else { return };
    #[cfg(feature = "antithesis")]
    {
        let sr = stmt_rules();
        let id = sr.kind_of_rule[rule].expect("kind id for stmt rule");
        let (newly, n_seen, calls) = note_seen(id);
        fire_family(family_of(kind), kind);
        if newly || calls % AGG_EVERY == 0 {
            eprintln!("gramwalk-reach: seen={}/{} stmt kinds", n_seen, total_stmt_kinds());
        }
    }
    #[cfg(not(feature = "antithesis"))]
    let _ = kind;
}

/// One `Reachable` property per statement family. The SDK requires every
/// property name to be an inline string literal at its own callsite, so the
/// roster is spelled out here once; the details payload carries the exact
/// grammar production so triage can see per-kind resolution inside each
/// family without a per-kind property.
#[cfg(feature = "antithesis")]
fn fire_family(family: &'static str, kind: &'static str) {
    use antithesis_sdk::{assert_reachable, assert_unreachable, serde_json::json};
    let d = &json!({ "stmt_kind": kind, "family": family });
    match family {
        "select" => assert_reachable!("gramwalk reach: select", d),
        "insert" => assert_reachable!("gramwalk reach: insert", d),
        "update" => assert_reachable!("gramwalk reach: update", d),
        "delete" => assert_reachable!("gramwalk reach: delete", d),
        "merge" => assert_reachable!("gramwalk reach: merge", d),
        "txn" => assert_reachable!("gramwalk reach: txn", d),
        "cursor" => assert_reachable!("gramwalk reach: cursor", d),
        "prepared" => assert_reachable!("gramwalk reach: prepared", d),
        "explain" => assert_reachable!("gramwalk reach: explain", d),
        "copy" => assert_reachable!("gramwalk reach: copy", d),
        "create-table" => assert_reachable!("gramwalk reach: create-table", d),
        "alter-table" => assert_reachable!("gramwalk reach: alter-table", d),
        "index" => assert_reachable!("gramwalk reach: index", d),
        "view" => assert_reachable!("gramwalk reach: view", d),
        "sequence" => assert_reachable!("gramwalk reach: sequence", d),
        "schema" => assert_reachable!("gramwalk reach: schema", d),
        "drop" => assert_reachable!("gramwalk reach: drop", d),
        "alter-object" => assert_reachable!("gramwalk reach: alter-object", d),
        "alter-type" => assert_reachable!("gramwalk reach: alter-type", d),
        "type-ddl" => assert_reachable!("gramwalk reach: type-ddl", d),
        "function" => assert_reachable!("gramwalk reach: function", d),
        "trigger-rule" => assert_reachable!("gramwalk reach: trigger-rule", d),
        "role" => assert_reachable!("gramwalk reach: role", d),
        "privilege" => assert_reachable!("gramwalk reach: privilege", d),
        "database" => assert_reachable!("gramwalk reach: database", d),
        "tablespace" => assert_reachable!("gramwalk reach: tablespace", d),
        "foreign" => assert_reachable!("gramwalk reach: foreign", d),
        "extension" => assert_reachable!("gramwalk reach: extension", d),
        "pubsub" => assert_reachable!("gramwalk reach: pubsub", d),
        "text-search" => assert_reachable!("gramwalk reach: text-search", d),
        "opclass" => assert_reachable!("gramwalk reach: opclass", d),
        "statistics" => assert_reachable!("gramwalk reach: statistics", d),
        "maintenance" => assert_reachable!("gramwalk reach: maintenance", d),
        "session" => assert_reachable!("gramwalk reach: session", d),
        "notify" => assert_reachable!("gramwalk reach: notify", d),
        "comment" => assert_reachable!("gramwalk reach: comment", d),
        "empty" => assert_reachable!("gramwalk reach: empty", d),
        // Drift tripwire, INVERTED semantics vs the roster above: every
        // curated family is a `Reachable` property (red until seen), but
        // never-reaching "other" is the DESIRED state — it fires only when
        // a grammar bump adds a `stmt` alternative family_of() doesn't
        // know. Registered as `Unreachable` so a clean run shows it green
        // (round-11 polish: as `assert_reachable!` it sat Failing 0/0 on
        // every healthy run), while an actual drift hit turns it red with
        // the unmapped kind in the details payload.
        _ => assert_unreachable!("gramwalk reach: family-other (mapping drift)", d),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// The vendored 18.3 grammar offers the expected top-level surface:
    /// 124 named `stmt` alternatives plus the empty statement.
    #[test]
    fn stmt_kind_census() {
        let total = total_stmt_kinds();
        assert_eq!(total, 125, "stmt kind count moved — grammar bump? update the doc");
        let sr = stmt_rules();
        assert!(sr.kinds.contains(&EMPTY_KIND));
        assert!(sr.kinds.contains(&"SelectStmt"));
        assert!(sr.kinds.contains(&"MergeStmt"));
        // Kind names are unique (dense ids are a bijection).
        let set: BTreeSet<_> = sr.kinds.iter().collect();
        assert_eq!(set.len(), total);
    }

    /// `stmt_kind_of_rule` is total: no panic on ANY rule id, including 0,
    /// the whole valid range, and past-the-end.
    #[test]
    fn kind_lookup_total_no_panic() {
        let mut stmt_rules_n = 0usize;
        for r in 0..=(gt::YYNRULES + 10) {
            if stmt_kind_of_rule(r).is_some() {
                stmt_rules_n += 1;
            }
        }
        assert_eq!(stmt_rules_n, 125, "one kind per stmt production");
        assert!(stmt_kind_of_rule(0).is_none());
        assert!(stmt_kind_of_rule(usize::MAX).is_none());
    }

    /// Every kind the grammar actually offers maps to a curated family —
    /// none fall through to the "other" drift bucket — and the family
    /// roster is exactly the pinned [`FAMILIES`] set (property names are
    /// stable identifiers; a diff here renames Antithesis properties).
    #[test]
    fn family_mapping_total_and_stable() {
        let sr = stmt_rules();
        let mut seen_families: BTreeSet<&'static str> = BTreeSet::new();
        for &kind in &sr.kinds {
            let fam = family_of(kind);
            assert_ne!(fam, "other", "unmapped stmt kind: {kind}");
            seen_families.insert(fam);
        }
        let pinned: BTreeSet<&'static str> = FAMILIES.iter().copied().collect();
        assert_eq!(pinned.len(), FAMILIES.len(), "FAMILIES has a duplicate");
        assert_eq!(seen_families, pinned, "family roster drifted from FAMILIES");
        // Cardinality sanity: curated granularity, not per-production.
        assert!(FAMILIES.len() >= 30 && FAMILIES.len() <= 60, "{}", FAMILIES.len());
        // Unknown future kinds fall into the drift bucket instead of
        // panicking.
        assert_eq!(family_of("SomeFutureStmt"), "other");
    }

    /// Aggregate accounting: first sighting of a kind is `newly_seen`,
    /// repeats are not, and the seen counter is monotone and bounded by the
    /// total. (Process-global tracker: tolerate prior recordings from other
    /// tests by asserting deltas, not absolutes.)
    #[test]
    fn seen_tracker_counts() {
        let (_, n0, c0) = note_seen(0);
        let (newly, n1, c1) = note_seen(0);
        assert!(!newly, "second sighting is not new");
        assert_eq!(n1, n0);
        assert_eq!(c1, c0 + 1);
        let before = n1;
        // Walk every kind once: afterwards everything is seen.
        for id in 0..total_stmt_kinds() {
            note_seen(id);
        }
        let (newly, n2, _) = note_seen(1);
        assert!(!newly);
        assert_eq!(n2, total_stmt_kinds());
        assert!(n2 >= before);
    }
}
