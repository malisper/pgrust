//! Dual-server lockstep apply + finding records + state-sync probes.
//!
//! `Executor` abstracts one server session so the reducer and unit tests
//! can run against fakes; `ClientExecutor` is the real wire-client binding.
//! Findings are JSONL, tri-state triage: the runner auto-fills only
//! "ruled" — everything else stays "unclassified" for human/agent triage
//! (later: port-bug | c-bug | ruled).
//!
//! State probes (F4c): with DML in the stream, a silent state divergence
//! (both sides "succeed" but write different data) only surfaces when a
//! later query happens to read it. Every `ProbeSpec::every` applied
//! statements and at stream end, the runner injects `SELECT * FROM t ORDER
//! BY <pk>` per probeable table on both sides and compares strictly (pk
//! makes it totally ordered; float cells keep the ulp tolerance so the
//! ruled B1 surface doesn't false-fire). A mismatch is a STATE_DIFF
//! finding recording the table and first differing row. Probes are
//! runner-injected — never part of the generated stream or its budget —
//! and marked `"probe":true` in the JSONL. After a table first fires
//! STATE_DIFF its later probes are suppressed (the divergence persists;
//! one finding per root cause).

use std::collections::BTreeMap;

use crate::client::{Client, ConnLost, RawResult};
use crate::diff::{classify, tag_affected, Classified, DiffClass, DiffInput, Side, StmtOutcome};
use crate::ruled::{apply_ruled, RuledEntry};
use crate::session::json_escape;

/// C-parity GUC pin (P1-A ruling,
/// docs/fuzzing/findings-p1a-parallel-defaults-ruling.md): pgrust ships
/// deliberately re-tuned parallel/JIT defaults
/// (docs/design/jit-parallel-defaults.md, train-34 "defaults stand"), so
/// at stock defaults the differential re-discovers ratified plan-shape
/// divergences (class P1-A) instead of real bugs. The diffrunner applies
/// these SETs on BOTH sessions at setup: every value is the C default, so
/// the reference side is a no-op and the pgrust side runs a
/// C-parity-costed session — the parallel planner stays under
/// differential coverage instead of being masked. The six parallel GUCs
/// plus the three jit_*_cost thresholds are exactly the divergent set the
/// ruling names (max_worker_processes also diverges but is
/// postmaster-only and does not price plans).
pub const C_PARITY_GUC_PIN: &[(&str, &str)] = &[
    ("parallel_setup_cost", "1000"),
    ("parallel_tuple_cost", "0.1"),
    ("max_parallel_workers_per_gather", "2"),
    ("min_parallel_table_scan_size", "'8MB'"),
    ("min_parallel_index_scan_size", "'512kB'"),
    ("max_parallel_workers", "8"),
    ("jit_above_cost", "100000"),
    ("jit_optimize_above_cost", "500000"),
    ("jit_inline_above_cost", "500000"),
];

/// A2 datetime determinism pin: the three session GUCs that price every
/// datetime text rendering and literal interpretation. initdb derives
/// TimeZone (and DateStyle's order component) from the host environment,
/// so two clusters initdb'd identically usually — but not provably —
/// agree; a divergence here makes every timestamptz/timetz output diff an
/// environment artifact, not a defect. Pinned identically on BOTH sides
/// at setup and re-pinned by `GucPinned` after RESET/DISCARD, exactly like
/// the C-parity pin. UTC (no DST, offset 0) keeps zone-DEPENDENT surfaces
/// on the explicit AT TIME ZONE / make_timestamptz productions, whose zone
/// names come from a fixed pool (crate::dtm::TZ_NAMES).
///
/// tzdata parity is a harness precondition, not pinnable per-session: the
/// local rig points PGRUST_TZDIR at the reference install's own
/// share/timezone dir (see scripts/covloop.sh and the A2 findings record),
/// so both engines read the same compiled tzdata files by construction.
pub const DATETIME_GUC_PIN: &[(&str, &str)] = &[
    ("TimeZone", "'UTC'"),
    ("DateStyle", "'ISO, MDY'"),
    ("IntervalStyle", "'postgres'"),
];

/// RB-8 locale determinism pin (a fuzzing round, NEW-TOCHAR): the three
/// cluster-level lc_* GUCs that price locale-sensitive text rendering —
/// `to_char`'s L/D/G/S currency and separator patterns (lc_monetary +
/// lc_numeric) and its TM-prefixed localized day/month names (lc_time).
/// initdb derives each of them from the host environment, so the two sides
/// only agree when the two images were initdb'd under the same LANG: the
/// antithesis pgrust datadir is initdb'd under en_US.utf8 while the C
/// oracle is initdb'd under the C locale, and the per-batch CREATE DATABASE
/// pin (helper_diffrun) covers only ENCODING/LC_COLLATE/LC_CTYPE — lc_monetary
/// et al. still come from each cluster's own postgresql.conf. Every RB-8
/// "divergence" was this setup artifact: `to_char(-125.8, 'L99G999D99')`
/// rendered `$   -125.80` (en_US) vs `    -125.80` (C locale substitutes a
/// single space for the empty currency_symbol). pgrust itself is
/// byte-for-byte C-parity under both values (verified against PG 18,
/// lc_monetary=C and en_US.UTF-8, 2026-08-24; pinned by unit tests in
/// adt/formatting). Pinned identically on BOTH sides and re-pinned after
/// RESET/DISCARD, exactly like the datetime pin.
pub const LOCALE_GUC_PIN: &[(&str, &str)] = &[
    ("lc_monetary", "'C'"),
    ("lc_numeric", "'C'"),
    ("lc_time", "'C'"),
];

/// The full session pin (C-parity + datetime + locale determinism) as
/// session SET statements, applied on BOTH sessions at setup.
pub fn c_parity_pin_sql() -> Vec<String> {
    C_PARITY_GUC_PIN
        .iter()
        .chain(DATETIME_GUC_PIN)
        .chain(LOCALE_GUC_PIN)
        .map(|(name, value)| format!("SET {name} = {value};"))
        .collect()
}

/// True for statements that can clobber session-level SETs: `RESET ...`
/// (RESET ALL restores every GUC to its reset default) and `DISCARD ...`
/// (DISCARD ALL implies RESET ALL). The generator's util module emits
/// RESET ALL, which silently un-pinned mid-stream (observed: the s12 i531
/// P1-A signature reappearing past the stream's first RESET ALL).
/// Deliberately broad — re-pinning after a RESET/DISCARD that did not
/// touch a pinned GUC is invisible to the stream.
///
/// A pre-connect database-level pin (ALTER DATABASE ... SET) would also
/// hold across RESET on both sides (both resolve RESET to the
/// per-database setting, PGC_S_DATABASE — see the GUC-reset ruling,
/// PR #724); we keep the session-level re-pin because it needs no setup
/// step and covers freshly created databases.
pub fn clobbers_session_gucs(sql: &str) -> bool {
    let up = sql.trim_start().to_ascii_uppercase();
    up.starts_with("RESET") || up.starts_with("DISCARD")
}

/// Executor wrapper that keeps the C-parity pin invariant: after any
/// statement that can clobber session GUCs, the pin SETs are re-applied.
/// Wraps BOTH sides identically, so the re-pin itself can never introduce
/// an asymmetry. Re-pin outcomes are ignored (best-effort): on a lost
/// connection the next stream statement reports SESSION_DIVERGED anyway.
pub struct GucPinned<E: Executor>(pub E);

impl<E: Executor> Executor for GucPinned<E> {
    fn apply(&mut self, sql: &str) -> StmtOutcome {
        let out = self.0.apply(sql);
        if clobbers_session_gucs(sql) {
            for set in c_parity_pin_sql() {
                let _ = self.0.apply(&set);
            }
        }
        out
    }

    fn apply_copy_in(&mut self, sql: &str, data: &[u8]) -> StmtOutcome {
        // COPY never clobbers session GUCs; forward straight through.
        self.0.apply_copy_in(sql, data)
    }
}

/// One server session applying statements in order.
pub trait Executor {
    fn apply(&mut self, sql: &str) -> StmtOutcome;

    /// Apply a COPY ... FROM STDIN statement, feeding `data` as the copy
    /// payload (X2 COPY BINARY round-trips). The default ignores the data
    /// and rides `apply` (whose client fails CopyIn) — only the real
    /// wire-client executor overrides.
    fn apply_copy_in(&mut self, sql: &str, _data: &[u8]) -> StmtOutcome {
        self.apply(sql)
    }
}

/// Real-server executor. A lost connection is sticky: every later apply
/// reports ConnLost, which classification turns into SESSION_DIVERGED.
///
/// With an xproto seed set, every statement rides the deterministic
/// per-statement protocol-mode plan (crate::xproto): seeded choice between
/// simple query and extended Parse/Bind/Execute — the seed must be the
/// SAME on both sides of a differential pair so the wire path can never
/// itself be an asymmetry.
pub struct ClientExecutor {
    client: Result<Client, String>,
    xproto: Option<u64>,
}

impl ClientExecutor {
    pub fn connect(host: &str, port: u16, db: &str, user: &str) -> Result<ClientExecutor, String> {
        Self::connect_opts(host, port, db, user, None)
    }

    /// Connect with a protocol-mode seed (None = always simple query).
    pub fn connect_opts(
        host: &str,
        port: u16,
        db: &str,
        user: &str,
        xproto: Option<u64>,
    ) -> Result<ClientExecutor, String> {
        match Client::connect(host, port, db, user) {
            Ok(c) => Ok(ClientExecutor { client: Ok(c), xproto }),
            Err(ConnLost(e)) => Err(e),
        }
    }
}

/// Fold a simple-query exchange into one outcome: any error dominates
/// (matching how a one-statement stream surfaces), else the last result.
/// A COPY transfer folds to CopyOut carrying the raw payload (X2: the
/// byte-compare surface for COPY ... TO STDOUT).
pub fn fold_results(results: &[RawResult]) -> StmtOutcome {
    for r in results {
        if let Some((sqlstate, message)) = &r.error {
            return StmtOutcome::Error { sqlstate: sqlstate.clone(), message: message.clone() };
        }
    }
    match results.last() {
        None => StmtOutcome::Command { tag: String::new(), affected: None },
        Some(r) if r.was_copy => StmtOutcome::CopyOut {
            bytes: r.copy_out.clone(),
            tag: r.cmd_tag.clone(),
        },
        Some(r) if !r.col_oids.is_empty() => {
            StmtOutcome::Rows { col_oids: r.col_oids.clone(), rows: r.rows.clone() }
        }
        Some(r) => StmtOutcome::Command {
            tag: r.cmd_tag.clone(),
            affected: tag_affected(&r.cmd_tag),
        },
    }
}

impl Executor for ClientExecutor {
    fn apply(&mut self, sql: &str) -> StmtOutcome {
        let xproto = self.xproto;
        match &mut self.client {
            Err(e) => StmtOutcome::ConnLost { detail: e.clone() },
            Ok(c) => {
                let results = match xproto {
                    Some(seed) => crate::xproto::apply_moded(c, seed, sql),
                    None => c.simple_query(sql),
                };
                match results {
                    Ok(results) => fold_results(&results),
                    Err(ConnLost(e)) => {
                        self.client = Err(e.clone());
                        StmtOutcome::ConnLost { detail: e }
                    }
                }
            }
        }
    }

    /// COPY FROM STDIN always rides the simple protocol (the extended-path
    /// CopyIn is deliberately unsupported), regardless of the xproto seed.
    fn apply_copy_in(&mut self, sql: &str, data: &[u8]) -> StmtOutcome {
        match &mut self.client {
            Err(e) => StmtOutcome::ConnLost { detail: e.clone() },
            Ok(c) => match c.copy_in(sql, data) {
                Ok(results) => fold_results(&results),
                Err(ConnLost(e)) => {
                    self.client = Err(e.clone());
                    StmtOutcome::ConnLost { detail: e }
                }
            },
        }
    }
}

/// One probeable table with its existence window. Fixture tables live for
/// the whole stream (`from` 0, no `until`); ddl-created tables carry the
/// window of their CREATE/DROP statement indices (session::DdlWindow), so
/// a probe round never SELECTs a table that does not exist at that point.
#[derive(Clone, Debug)]
pub struct ProbeTable {
    pub name: String,
    pub pk: String,
    /// Statement index of the creating statement (0 = pre-existing).
    pub from: u32,
    /// Statement index of the dropping statement, when dropped.
    pub until: Option<u32>,
}

impl ProbeTable {
    /// Does the table exist after the statement at `after_index` applied?
    fn live_at(&self, after_index: u32) -> bool {
        after_index >= self.from && self.until.is_none_or(|u| after_index < u)
    }
}

/// State-probe configuration: probe cadence (in applied statements) and the
/// probeable tables with their pk sort keys and existence windows.
#[derive(Clone, Debug)]
pub struct ProbeSpec {
    pub every: u32,
    pub tables: Vec<ProbeTable>,
}

impl ProbeSpec {
    /// Probe every pk-carrying catalog table. None when nothing is
    /// probeable (e.g. live catalogs, which carry no pk metadata).
    pub fn from_catalog(catalog: &crate::catalog::Catalog, every: u32) -> Option<ProbeSpec> {
        let tables: Vec<ProbeTable> = catalog
            .tables
            .iter()
            .filter_map(|t| {
                t.pk.as_ref().map(|pk| ProbeTable {
                    name: t.name.clone(),
                    pk: pk.column.clone(),
                    from: 0,
                    until: None,
                })
            })
            .collect();
        if tables.is_empty() {
            None
        } else {
            Some(ProbeSpec { every, tables })
        }
    }
}

/// The injected probe statement: pk-ordered, so the rowset compare is
/// strict (total order), with only the ruled float-ulp slack.
pub fn probe_sql(table: &str, pk: &str) -> String {
    format!("SELECT * FROM {} ORDER BY {};", table, pk)
}

/// Classify one probe exchange. Rides the normal classifier (ordered
/// compare + ruled table), then renames any real divergence to STATE_DIFF
/// carrying the table: the probe's whole point is that the divergence is
/// *state*, whichever shape it surfaces as (row diff, one-side error,
/// column shape). Session loss stays SESSION_DIVERGED.
pub fn classify_probe(
    table_name: &str,
    a: &StmtOutcome,
    b: &StmtOutcome,
    table: &[RuledEntry],
    ulp_tol: u64,
) -> Classified {
    let sql = probe_sql(table_name, "pk"); // ORDER BY presence is what matters
    // Probes select raw table columns: no soft-float mask (float cells
    // still get the ruled ulp slack via the classifier itself).
    let raw = classify(&DiffInput { sql: &sql, a, b, ulp_tol, soft_cols: &[], mask_explain_timing: false });
    let resolved = apply_ruled(table, &sql, raw);
    match resolved.class {
        DiffClass::Match | DiffClass::Ruled(_) | DiffClass::SessionDiverged(_) => resolved,
        _ => Classified {
            class: DiffClass::StateDiff(table_name.to_string()),
            detail: format!("table {}: {}", table_name, resolved.detail),
        },
    }
}

/// One classified statement out of a run (only non-MATCH statements are
/// recorded; MATCHes are counted in `RunStats`).
#[derive(Clone, Debug)]
pub struct Record {
    pub stmt_index: u32,
    pub sql: String,
    pub class: DiffClass,
    pub detail: String,
    /// True for runner-injected state probes (`stmt_index` is then the
    /// last generated statement applied before the probe).
    pub probe: bool,
}

impl Record {
    /// Finding JSONL: ruled records carry triage "ruled" + the ruling id;
    /// everything else is "unclassified". Probe records carry
    /// `"probe":true` so triage can tell them from stream statements.
    pub fn to_jsonl(&self, seed: u64) -> String {
        let (triage, ruling) = match &self.class {
            DiffClass::Ruled(id) => ("ruled", Some(id.as_str())),
            _ => ("unclassified", None),
        };
        let mut out = format!(
            "{{\"seed\":{},\"stmt_index\":{},\"sql\":\"{}\",\"class\":\"{}\",\"detail\":\"{}\",\"triage\":\"{}\"",
            seed,
            self.stmt_index,
            json_escape(&self.sql),
            self.class.key(),
            json_escape(&self.detail),
            triage
        );
        if let Some(id) = ruling {
            out.push_str(&format!(",\"ruling\":\"{}\"", json_escape(id)));
        }
        if self.probe {
            out.push_str(",\"probe\":true");
        }
        out.push('}');
        out
    }

    pub fn is_finding(&self) -> bool {
        !matches!(self.class, DiffClass::Match | DiffClass::Ruled(_))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunStats {
    pub applied: u32,
    pub matches: u32,
    pub ruled: u32,
    pub findings: u32,
    /// Probe exchanges executed (all tables, all rounds).
    pub probes: u32,
    /// Statements (and probe rounds) collapsed into a root finding as
    /// asymmetric-25P02 cascade noise (round-18 soak: one side's
    /// transaction aborted, so every later in-transaction statement on
    /// that side reports 25P02 "current transaction is aborted" while the
    /// other side sails on — N downstream noise records for one root
    /// cause). Collapsed exchanges produce NO records of their own; the
    /// root record's detail carries the collapsed count.
    pub cascade_collapsed: u32,
    /// SQLSTATE histogram of erroring statements (side A's state when A
    /// errored, else side B's) — the error-class breakdown surface.
    pub error_states: BTreeMap<String, u32>,
}

/// One stream entry: the statement text plus the generator's per-statement
/// compare metadata (ruled-soft float-aggregate columns).
#[derive(Clone, Debug)]
pub struct StreamStmt {
    pub stmt_index: u32,
    pub sql: String,
    pub soft_float_cols: Vec<usize>,
    /// Opt-in H1 mask (DiffInput::mask_explain_timing): set for gramwalk
    /// statements and --mask-explain-timing replays only.
    pub mask_explain_timing: bool,
}

/// Apply one statement to both sides and classify, ruled table included.
pub fn apply_and_classify(
    a: &mut dyn Executor,
    b: &mut dyn Executor,
    sql: &str,
    soft_cols: &[usize],
    table: &[RuledEntry],
    ulp_tol: u64,
) -> Classified {
    let oa = a.apply(sql);
    let ob = b.apply(sql);
    let raw =
        classify(&DiffInput { sql, a: &oa, b: &ob, ulp_tol, soft_cols, mask_explain_timing: false });
    apply_ruled(table, sql, raw)
}

/// The side whose outcome is a 25P02 "current transaction is aborted"
/// error while the other side's is anything else: the signature of an
/// asymmetrically aborted transaction (exactly one side's transaction
/// died on some earlier statement). Symmetric 25P02 is None — both
/// transactions aborted, the statements compare as matches anyway.
fn aborted_txn_side(oa: &StmtOutcome, ob: &StmtOutcome) -> Option<Side> {
    let aborted = |o: &StmtOutcome| {
        matches!(o, StmtOutcome::Error { sqlstate, .. } if sqlstate == "25P02")
    };
    match (aborted(oa), aborted(ob)) {
        (true, false) => Some(Side::A),
        (false, true) => Some(Side::B),
        _ => None,
    }
}

/// The side that errored (with anything but 25P02) while the other side
/// did not error at all: the candidate transaction-aborting statement a
/// later asymmetric-25P02 cascade is rooted at. Both-sides-error is None
/// (both transactions abort together — no asymmetry follows).
fn one_sided_error_side(oa: &StmtOutcome, ob: &StmtOutcome) -> Option<Side> {
    let err = |o: &StmtOutcome| {
        matches!(o, StmtOutcome::Error { sqlstate, .. } if sqlstate != "25P02")
    };
    let any_err = |o: &StmtOutcome| matches!(o, StmtOutcome::Error { .. });
    match (err(oa), err(ob)) {
        (true, false) if !any_err(ob) => Some(Side::A),
        (false, true) if !any_err(oa) => Some(Side::B),
        _ => None,
    }
}

/// An active asymmetric-25P02 cascade: one side's transaction is aborted,
/// the other side's is live. Rooted at the record of the statement that
/// aborted the transaction (the first divergent statement); every later
/// noise exchange folds into that record's detail instead of minting its
/// own finding.
struct CascadeState {
    side: Side,
    /// Index into `records` of the root (the aborting statement's record —
    /// a finding or a ruled record; one-sided errors always record).
    root_pos: usize,
    /// The root record's detail before any cascade annotation.
    base_detail: String,
    collapsed: u32,
}

impl CascadeState {
    fn annotated_detail(&self) -> String {
        format!(
            "{} [25P02-cascade: {} downstream exchange(s) on side {:?} collapsed into this finding]",
            self.base_detail, self.collapsed, self.side
        )
    }
}

/// Lockstep run over a statement stream, with optional state probes.
/// Stops after a SESSION_DIVERGED record: with a side gone, every later
/// statement diverges vacuously.
///
/// Asymmetric-25P02 cascades collapse (round-18 soak, 15/52 findings were
/// this noise shape): once one side's transaction aborts while the
/// other's stays live, every later in-transaction statement on the dead
/// side answers 25P02 — each of which used to mint its own ERROR_DIFF /
/// STATE_DIFF record. Now the FIRST divergent statement (the one-sided
/// error that aborted the transaction — it always has a record, finding
/// or ruled) becomes the cascade root, and every following
/// asymmetric-25P02 exchange (statements and probe rounds alike) is
/// counted into that root record's detail instead. The cascade closes as
/// soon as the dead side stops answering 25P02 (ROLLBACK/COMMIT ended the
/// transaction). A cascade whose root precedes the stream window gets one
/// synthetic ERROR_DIFF root naming the condition.
pub fn run_stream(
    a: &mut dyn Executor,
    b: &mut dyn Executor,
    stmts: &[StreamStmt],
    table: &[RuledEntry],
    ulp_tol: u64,
    probes: Option<&ProbeSpec>,
) -> (Vec<Record>, RunStats) {
    let mut records: Vec<Record> = Vec::new();
    let mut stats = RunStats::default();
    let mut suppressed: Vec<String> = Vec::new();
    let mut since_probe = 0u32;
    let mut last_index = 0u32;
    // The most recent one-sided (non-25P02) error's side and record
    // position: the candidate cascade root. One-sided errors always push
    // a record (ErrorDiff/CountDiff finding or a Ruled record).
    let mut last_one_sided: Option<(Side, usize)> = None;
    let mut cascade: Option<CascadeState> = None;
    for StreamStmt { stmt_index, sql, soft_float_cols, mask_explain_timing } in stmts {
        last_index = *stmt_index;
        let oa = a.apply(sql);
        let ob = b.apply(sql);
        for o in [&oa, &ob] {
            if let StmtOutcome::Error { sqlstate, .. } = o {
                *stats.error_states.entry(sqlstate.clone()).or_default() += 1;
                break; // one histogram hit per statement
            }
        }
        let raw = classify(&DiffInput {
            sql,
            a: &oa,
            b: &ob,
            ulp_tol,
            soft_cols: soft_float_cols,
            mask_explain_timing: *mask_explain_timing,
        });
        let c = apply_ruled(table, sql, raw);
        stats.applied += 1;
        since_probe += 1;
        // Asymmetric-25P02 cascade handling: fold noise into the root.
        let collapsed = match aborted_txn_side(&oa, &ob) {
            Some(side) => {
                match cascade.as_mut() {
                    Some(cs) if cs.side == side => {
                        cs.collapsed += 1;
                        stats.cascade_collapsed += 1;
                        records[cs.root_pos].detail = cs.annotated_detail();
                        true
                    }
                    _ => {
                        // Open a cascade rooted at the aborting statement's
                        // record; synthesize a root when the abort predates
                        // this window (that synthetic record IS the one
                        // finding — this statement is not double-counted).
                        let (root_pos, folds) = match last_one_sided {
                            Some((s, pos)) if s == side => (pos, true),
                            _ => {
                                stats.findings += 1;
                                records.push(Record {
                                    stmt_index: *stmt_index,
                                    sql: sql.clone(),
                                    class: DiffClass::ErrorDiff,
                                    detail: format!(
                                        "asymmetric 25P02 cascade on side {:?} (transaction aborted by a statement before this window)",
                                        side
                                    ),
                                    probe: false,
                                });
                                (records.len() - 1, false)
                            }
                        };
                        let mut cs = CascadeState {
                            side,
                            root_pos,
                            base_detail: records[root_pos].detail.clone(),
                            collapsed: 0,
                        };
                        if folds {
                            cs.collapsed = 1;
                            stats.cascade_collapsed += 1;
                            records[root_pos].detail = cs.annotated_detail();
                        }
                        cascade = Some(cs);
                        true
                    }
                }
            }
            None => {
                // The dead side answered something other than 25P02: the
                // transaction ended, the cascade is over.
                cascade = None;
                false
            }
        };
        let diverged_session = matches!(c.class, DiffClass::SessionDiverged(_));
        if !collapsed {
            match &c.class {
                DiffClass::Match => stats.matches += 1,
                DiffClass::Ruled(_) => {
                    stats.ruled += 1;
                    records.push(Record {
                        stmt_index: *stmt_index,
                        sql: sql.clone(),
                        class: c.class,
                        detail: c.detail,
                        probe: false,
                    });
                }
                _ => {
                    stats.findings += 1;
                    records.push(Record {
                        stmt_index: *stmt_index,
                        sql: sql.clone(),
                        class: c.class,
                        detail: c.detail,
                        probe: false,
                    });
                }
            }
            if let Some(side) = one_sided_error_side(&oa, &ob) {
                // A record was pushed for every non-Match class; Match is
                // impossible with exactly one side erroring.
                if !records.is_empty() {
                    last_one_sided = Some((side, records.len() - 1));
                }
            }
        }
        if diverged_session {
            return (records, stats);
        }
        if let Some(spec) = probes {
            if spec.every > 0 && since_probe >= spec.every {
                since_probe = 0;
                if run_probe_round(
                    a, b, spec, table, ulp_tol, *stmt_index, &mut suppressed, &mut records,
                    &mut stats, &mut cascade, last_one_sided,
                ) {
                    return (records, stats);
                }
            }
        }
    }
    // Stream-end probe round (even when the cadence just ran: end state is
    // the one that matters most, and suppression keeps it cheap on repeats).
    if let Some(spec) = probes {
        if !stmts.is_empty() {
            run_probe_round(
                a, b, spec, table, ulp_tol, last_index, &mut suppressed, &mut records,
                &mut stats, &mut cascade, last_one_sided,
            );
        }
    }
    (records, stats)
}

/// One probe round over every non-suppressed table. Returns true when a
/// probe hit session divergence (the caller stops the run).
#[allow(clippy::too_many_arguments)]
fn run_probe_round(
    a: &mut dyn Executor,
    b: &mut dyn Executor,
    spec: &ProbeSpec,
    table: &[RuledEntry],
    ulp_tol: u64,
    after_index: u32,
    suppressed: &mut Vec<String>,
    records: &mut Vec<Record>,
    stats: &mut RunStats,
    cascade: &mut Option<CascadeState>,
    last_one_sided: Option<(Side, usize)>,
) -> bool {
    for pt in &spec.tables {
        let (t, pk) = (&pt.name, &pt.pk);
        if suppressed.contains(t) || !pt.live_at(after_index) {
            continue;
        }
        let sql = probe_sql(t, pk);
        let oa = a.apply(&sql);
        let ob = b.apply(&sql);
        stats.probes += 1;
        // Probes riding an asymmetric-25P02 cascade fold into the cascade
        // root like stream statements do (the dead side answers 25P02 to
        // the probe SELECT itself — pure noise, and transient, so the
        // table is NOT suppressed for later rounds). A probe can also be
        // the FIRST noise exchange after the aborting statement (cadence
        // fires before the next stream statement), so it may open the
        // cascade off the known root; without a known root it falls
        // through to normal classification.
        if let Some(side) = aborted_txn_side(&oa, &ob) {
            let matching_root = match (cascade.as_mut(), last_one_sided) {
                (Some(cs), _) if cs.side == side => Some(cs.root_pos),
                (Some(_), _) => None,
                (None, Some((s, pos))) if s == side => {
                    *cascade = Some(CascadeState {
                        side,
                        root_pos: pos,
                        base_detail: records[pos].detail.clone(),
                        collapsed: 0,
                    });
                    Some(pos)
                }
                (None, _) => None,
            };
            if matching_root.is_some() {
                let cs = cascade.as_mut().expect("cascade just ensured");
                cs.collapsed += 1;
                stats.cascade_collapsed += 1;
                records[cs.root_pos].detail = cs.annotated_detail();
                continue;
            }
        }
        let c = classify_probe(t, &oa, &ob, table, ulp_tol);
        match &c.class {
            DiffClass::Match => {}
            DiffClass::Ruled(_) => {
                stats.ruled += 1;
                records.push(Record {
                    stmt_index: after_index,
                    sql,
                    class: c.class,
                    detail: c.detail,
                    probe: true,
                });
            }
            DiffClass::SessionDiverged(_) => {
                stats.findings += 1;
                records.push(Record {
                    stmt_index: after_index,
                    sql,
                    class: c.class,
                    detail: c.detail,
                    probe: true,
                });
                return true;
            }
            _ => {
                stats.findings += 1;
                suppressed.push(t.clone());
                records.push(Record {
                    stmt_index: after_index,
                    sql,
                    class: c.class,
                    detail: c.detail,
                    probe: true,
                });
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scripted executor for runner-level tests: statements consume the
    /// script in order; probe SELECTs consume it too (the scripts below
    /// account for them).
    pub struct Scripted {
        pub outcomes: Vec<StmtOutcome>,
        pub next: usize,
    }

    impl Executor for Scripted {
        fn apply(&mut self, _sql: &str) -> StmtOutcome {
            let o = self.outcomes[self.next.min(self.outcomes.len() - 1)].clone();
            self.next += 1;
            o
        }
    }

    fn rows_of(v: &str) -> StmtOutcome {
        StmtOutcome::Rows { col_oids: vec![23], rows: vec![vec![Some(v.to_string())]] }
    }

    fn stmts(n: u32) -> Vec<StreamStmt> {
        (0..n)
            .map(|i| StreamStmt {
                stmt_index: i,
                sql: "SELECT c FROM t;".to_string(),
                soft_float_cols: Vec::new(),
                mask_explain_timing: false,
            })
            .collect()
    }

    #[test]
    fn run_stream_counts_and_records() {
        let table = crate::ruled::default_table();
        let mut a = Scripted { outcomes: vec![rows_of("1"), rows_of("2")], next: 0 };
        let mut b = Scripted { outcomes: vec![rows_of("1"), rows_of("9")], next: 0 };
        let (records, stats) = run_stream(&mut a, &mut b, &stmts(2), &table, 4, None);
        assert_eq!(stats.applied, 2);
        assert_eq!(stats.matches, 1);
        assert_eq!(stats.findings, 1);
        assert_eq!(stats.probes, 0);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].stmt_index, 1);
        assert_eq!(records[0].class, DiffClass::RowsetDiff);
        assert!(records[0].is_finding());
        assert!(!records[0].probe);
    }

    #[test]
    fn asymmetric_25p02_cascade_collapses_into_root_finding() {
        let table = crate::ruled::default_table();
        let err = |s: &str, m: &str| StmtOutcome::Error {
            sqlstate: s.to_string(),
            message: m.to_string(),
        };
        let aborted = || err("25P02", "current transaction is aborted");
        // Stream: s0 matches; s1 aborts B only (one-sided 42804); s2..s4
        // are cascade noise (A sails on, B answers 25P02); s5 both succeed
        // (ROLLBACK ended the transaction — cascade closes).
        let a_seq = vec![
            rows_of("1"),
            rows_of("2"),
            rows_of("3"),
            rows_of("4"),
            rows_of("5"),
            rows_of("6"),
        ];
        let b_seq = vec![
            rows_of("1"),
            err("42804", "datatype mismatch"),
            aborted(),
            aborted(),
            aborted(),
            rows_of("6"),
        ];
        let mut a = Scripted { outcomes: a_seq, next: 0 };
        let mut b = Scripted { outcomes: b_seq, next: 0 };
        let (records, stats) = run_stream(&mut a, &mut b, &stmts(6), &table, 4, None);
        // ONE finding: the aborting statement's ErrorDiff, carrying the
        // collapsed count; the three noise statements record nothing.
        assert_eq!(stats.findings, 1);
        assert_eq!(stats.cascade_collapsed, 3);
        assert_eq!(stats.matches, 2, "s0 and s5 match");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].stmt_index, 1, "root is the aborting statement");
        assert_eq!(records[0].class, DiffClass::ErrorDiff);
        assert!(
            records[0].detail.contains("25P02-cascade: 3 downstream exchange(s) on side B"),
            "detail: {}",
            records[0].detail
        );
    }

    #[test]
    fn cascade_without_in_stream_root_gets_one_synthetic_finding() {
        let table = crate::ruled::default_table();
        let aborted = || StmtOutcome::Error {
            sqlstate: "25P02".to_string(),
            message: "current transaction is aborted".to_string(),
        };
        // B is already inside an aborted transaction when the window
        // starts: 3 asymmetric-25P02 statements, no root in stream.
        let mut a = Scripted { outcomes: vec![rows_of("1"); 3], next: 0 };
        let mut b = Scripted { outcomes: vec![aborted(), aborted(), aborted()], next: 0 };
        let (records, stats) = run_stream(&mut a, &mut b, &stmts(3), &table, 4, None);
        assert_eq!(stats.findings, 1, "one synthetic root, not three findings");
        assert_eq!(stats.cascade_collapsed, 2);
        assert_eq!(records.len(), 1);
        assert!(records[0].detail.contains("transaction aborted by a statement before this window"));
    }

    #[test]
    fn symmetric_25p02_still_matches_and_probe_noise_collapses() {
        let table = crate::ruled::default_table();
        let err = |s: &str, m: &str| StmtOutcome::Error {
            sqlstate: s.to_string(),
            message: m.to_string(),
        };
        let aborted = || err("25P02", "current transaction is aborted");
        // Symmetric abort: both sides answer 25P02 — plain matches, no
        // cascade, no findings.
        let mut a = Scripted { outcomes: vec![aborted(), aborted()], next: 0 };
        let mut b = Scripted { outcomes: vec![aborted(), aborted()], next: 0 };
        let (records, stats) = run_stream(&mut a, &mut b, &stmts(2), &table, 4, None);
        assert_eq!(stats.matches, 2);
        assert_eq!(stats.findings, 0);
        assert_eq!(stats.cascade_collapsed, 0);
        assert!(records.is_empty());

        // Probe rounds inside an active cascade fold too: apply order per
        // side with every=1 is s0 P s1 P (end P suppressed by cadence
        // logic running right before). s0 roots the cascade; both probes
        // hit B's aborted transaction and collapse instead of minting
        // STATE_DIFFs — and the table is NOT suppressed.
        let a_seq = vec![rows_of("1"), rows_of("p"), rows_of("2"), rows_of("p")];
        let b_seq = vec![err("42804", "datatype mismatch"), aborted(), aborted(), aborted()];
        let mut a = Scripted { outcomes: a_seq, next: 0 };
        let mut b = Scripted { outcomes: b_seq, next: 0 };
        let (records, stats) =
            run_stream(&mut a, &mut b, &stmts(2), &table, 4, Some(&probe_spec(1)));
        assert_eq!(stats.findings, 1, "only the root");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].stmt_index, 0);
        // s1 collapsed + probe rounds collapsed (cadence after s0, after
        // s1, and the end round).
        assert!(stats.cascade_collapsed >= 3, "collapsed={}", stats.cascade_collapsed);
        assert!(records[0].detail.contains("25P02-cascade:"));
    }

    #[test]
    fn run_stream_stops_after_session_divergence() {
        let table = crate::ruled::default_table();
        let lost = StmtOutcome::ConnLost { detail: "gone".to_string() };
        let mut a = Scripted { outcomes: vec![lost], next: 0 };
        let mut b = Scripted { outcomes: vec![rows_of("1")], next: 0 };
        let (records, stats) = run_stream(&mut a, &mut b, &stmts(5), &table, 4, None);
        assert_eq!(stats.applied, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].class.key(), "SESSION_DIVERGED");
    }

    #[test]
    fn error_histogram_counts_matched_errors_once() {
        let table = crate::ruled::default_table();
        let err = |s: &str| StmtOutcome::Error {
            sqlstate: s.to_string(),
            message: "m".to_string(),
        };
        let mut a = Scripted { outcomes: vec![err("22012"), err("23505")], next: 0 };
        let mut b = Scripted { outcomes: vec![err("22012"), err("23505")], next: 0 };
        let (_, stats) = run_stream(&mut a, &mut b, &stmts(2), &table, 4, None);
        assert_eq!(stats.error_states.get("22012"), Some(&1));
        assert_eq!(stats.error_states.get("23505"), Some(&1));
        assert_eq!(stats.matches, 2, "same-SQLSTATE errors are matches");
    }

    fn probe_spec(every: u32) -> ProbeSpec {
        ProbeSpec {
            every,
            tables: vec![ProbeTable {
                name: "t".to_string(),
                pk: "pk".to_string(),
                from: 0,
                until: None,
            }],
        }
    }

    /// Windowed probe tables: a table is only probed while it exists —
    /// never before its CREATE index, never at or after its DROP index.
    #[test]
    fn c_parity_guc_pin_covers_the_ruled_divergent_set() {
        // The P1-A ruling names six parallel GUCs + three jit_*_cost
        // thresholds; the pin must cover exactly those, each once, at the
        // C default values (drift here would silently re-open the P1-A
        // findings-budget leak or, worse, pin a non-C value).
        let expected = [
            ("parallel_setup_cost", "1000"),
            ("parallel_tuple_cost", "0.1"),
            ("max_parallel_workers_per_gather", "2"),
            ("min_parallel_table_scan_size", "'8MB'"),
            ("min_parallel_index_scan_size", "'512kB'"),
            ("max_parallel_workers", "8"),
            ("jit_above_cost", "100000"),
            ("jit_optimize_above_cost", "500000"),
            ("jit_inline_above_cost", "500000"),
        ];
        assert_eq!(C_PARITY_GUC_PIN, &expected, "pin drifted from the ruled set");
        // A2: the datetime determinism pin rides the same setup/re-pin
        // path; the full pin SQL is C-parity then datetime, in order.
        let expected_dt = [
            ("TimeZone", "'UTC'"),
            ("DateStyle", "'ISO, MDY'"),
            ("IntervalStyle", "'postgres'"),
        ];
        assert_eq!(DATETIME_GUC_PIN, &expected_dt, "datetime pin drifted");
        // RB-8: the locale pin neutralizes the initdb-environment asymmetry
        // (pgrust datadir en_US.utf8 vs C oracle C locale); 'C' on both
        // sides keeps the reference arm a no-op relative to its own conf.
        let expected_lc = [
            ("lc_monetary", "'C'"),
            ("lc_numeric", "'C'"),
            ("lc_time", "'C'"),
        ];
        assert_eq!(LOCALE_GUC_PIN, &expected_lc, "locale pin drifted");
        let sql = c_parity_pin_sql();
        assert_eq!(sql.len(), expected.len() + expected_dt.len() + expected_lc.len());
        for ((name, value), s) in expected
            .iter()
            .chain(expected_dt.iter())
            .chain(expected_lc.iter())
            .zip(&sql)
        {
            assert_eq!(*s, format!("SET {name} = {value};"));
        }
    }

    #[test]
    fn guc_pinned_executor_reapplies_after_reset_and_discard() {
        // A RESET ALL mid-stream must not un-pin the session (the s12
        // i531 escape): the wrapper re-applies every pin SET after any
        // RESET/DISCARD statement, and only after those.
        struct Recording(Vec<String>);
        impl Executor for Recording {
            fn apply(&mut self, sql: &str) -> StmtOutcome {
                self.0.push(sql.to_string());
                StmtOutcome::Error { sqlstate: String::new(), message: String::new() }
            }
        }
        assert!(clobbers_session_gucs("RESET ALL;"));
        assert!(clobbers_session_gucs("  reset enable_sort;"));
        assert!(clobbers_session_gucs("DISCARD PLANS;"));
        assert!(!clobbers_session_gucs("SELECT 1;"));
        assert!(!clobbers_session_gucs("SET enable_sort TO off;"));

        let mut ex = GucPinned(Recording(Vec::new()));
        ex.apply("SELECT 1;");
        assert_eq!(ex.0 .0.len(), 1);
        ex.apply("RESET ALL;");
        // RESET ALL + the full re-pin SETs (C-parity + datetime + locale;
        // re-applied even though the inner executor reports errors:
        // best-effort, outcome ignored).
        let npin = c_parity_pin_sql().len();
        assert_eq!(ex.0 .0.len(), 1 + 1 + npin);
        assert_eq!(ex.0 .0[2], "SET parallel_setup_cost = 1000;");
        assert_eq!(ex.0 .0[1 + npin], "SET lc_time = 'C';");
        ex.apply("DISCARD SEQUENCES;");
        assert_eq!(ex.0 .0.len(), 2 * (1 + npin) + 1);
    }

    #[test]
    fn probe_windows_gate_rounds() {
        let table = crate::ruled::default_table();
        // Window [2, 4): probes fire only for after_index 2 and 3.
        let spec = ProbeSpec {
            every: 1,
            tables: vec![ProbeTable {
                name: "t".to_string(),
                pk: "pk".to_string(),
                from: 2,
                until: Some(4),
            }],
        };
        // 6 statements, all matching; every probe (when taken) matches too.
        let seq = vec![rows_of("1"); 16];
        let mut a = Scripted { outcomes: seq.clone(), next: 0 };
        let mut b = Scripted { outcomes: seq, next: 0 };
        let (_, stats) = run_stream(&mut a, &mut b, &stmts(6), &table, 4, Some(&spec));
        // Cadence rounds after stmts 0..5 plus the end round (after 5):
        // only after_index 2 and 3 are inside the window.
        assert_eq!(stats.probes, 2, "window gating failed");
        assert!(ProbeTable {
            name: "x".into(),
            pk: "pk".into(),
            from: 0,
            until: None
        }
        .live_at(0));
    }

    #[test]
    fn probes_surface_silent_state_divergence() {
        let table = crate::ruled::default_table();
        // Two matching statements, then the probe (3rd apply) disagrees:
        // silent state divergence surfaced only by the probe.
        let mut a = Scripted {
            outcomes: vec![rows_of("1"), rows_of("1"), rows_of("10")],
            next: 0,
        };
        let mut b = Scripted {
            outcomes: vec![rows_of("1"), rows_of("1"), rows_of("11")],
            next: 0,
        };
        let (records, stats) =
            run_stream(&mut a, &mut b, &stmts(2), &table, 4, Some(&probe_spec(0)));
        assert_eq!(stats.findings, 1);
        assert_eq!(stats.probes, 1);
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert!(r.probe);
        assert_eq!(r.class, DiffClass::StateDiff("t".to_string()));
        assert_eq!(r.stmt_index, 1, "probe records the last applied statement");
        assert_eq!(r.sql, "SELECT * FROM t ORDER BY pk;");
        let line = r.to_jsonl(7);
        assert!(line.contains("\"class\":\"STATE_DIFF\""));
        assert!(line.contains("\"probe\":true"));
    }

    #[test]
    fn probe_cadence_and_suppression() {
        let table = crate::ruled::default_table();
        // every=2 over 4 statements: probe rounds after stmt 1, stmt 3,
        // and at stream end. All probes diverge; only the FIRST becomes a
        // finding (later rounds suppressed for that table), and the
        // end-round costs nothing extra.
        // Apply order per side: s0 s1 P s2 s3 P (end P suppressed).
        let seq_a = vec![rows_of("1"), rows_of("1"), rows_of("10"), rows_of("1"), rows_of("1")];
        let seq_b = vec![rows_of("1"), rows_of("1"), rows_of("99"), rows_of("1"), rows_of("1")];
        let mut a = Scripted { outcomes: seq_a, next: 0 };
        let mut b = Scripted { outcomes: seq_b, next: 0 };
        let (records, stats) =
            run_stream(&mut a, &mut b, &stmts(4), &table, 4, Some(&probe_spec(2)));
        assert_eq!(stats.probes, 1, "suppressed table is not re-probed");
        assert_eq!(stats.findings, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].class, DiffClass::StateDiff("t".to_string()));
        assert_eq!(records[0].stmt_index, 1);
    }

    #[test]
    fn probe_float_ulp_is_ruled_not_state_diff() {
        let table = crate::ruled::default_table();
        let x = 0.1f64 + 0.2f64;
        let frow = |v: f64| StmtOutcome::Rows {
            col_oids: vec![crate::diff::FLOAT8_OID],
            rows: vec![vec![Some(format!("{v:?}"))]],
        };
        let c = classify_probe("t", &frow(x), &frow(0.3), &table, 4);
        assert_eq!(c.class, DiffClass::Ruled("b1-float-ulp".to_string()));
        // Beyond tolerance it is a STATE_DIFF.
        let c = classify_probe("t", &frow(1.0), &frow(1.001), &table, 4);
        assert_eq!(c.class, DiffClass::StateDiff("t".to_string()));
        assert!(c.detail.contains("table t:"));
    }

    #[test]
    fn probe_one_side_error_is_state_diff() {
        let table = crate::ruled::default_table();
        let err = StmtOutcome::Error {
            sqlstate: "25P02".to_string(),
            message: "aborted".to_string(),
        };
        // Same error on both sides: match (aborted txn on both, fine).
        let c = classify_probe("t", &err, &err, &table, 4);
        assert_eq!(c.class, DiffClass::Match);
        // One-sided error: the sides' session states differ = STATE_DIFF.
        let c = classify_probe("t", &rows_of("1"), &err, &table, 4);
        assert_eq!(c.class, DiffClass::StateDiff("t".to_string()));
    }

    #[test]
    fn jsonl_shapes() {
        let finding = Record {
            stmt_index: 7,
            sql: "SELECT \"x\";".to_string(),
            class: DiffClass::RowsetDiff,
            detail: "row 0".to_string(),
            probe: false,
        };
        let line = finding.to_jsonl(42);
        assert!(line.starts_with("{\"seed\":42,\"stmt_index\":7,"));
        assert!(line.contains("\"class\":\"ROWSET_DIFF\""));
        assert!(line.contains("\"triage\":\"unclassified\""));
        assert!(!line.contains("\"ruling\""));
        assert!(!line.contains("\"probe\""));

        let ruled = Record {
            stmt_index: 8,
            sql: "SELECT f;".to_string(),
            class: DiffClass::Ruled("b1-float-ulp".to_string()),
            detail: String::new(),
            probe: false,
        };
        let line = ruled.to_jsonl(42);
        assert!(line.contains("\"triage\":\"ruled\""));
        assert!(line.contains("\"ruling\":\"b1-float-ulp\""));
        assert!(!ruled.is_finding());
    }

    #[test]
    fn fold_results_error_dominates() {
        let ok = RawResult {
            col_oids: vec![23],
            rows: vec![vec![Some("1".to_string())]],
            cmd_tag: "SELECT 1".to_string(),
            error: None,
            copy_out: Vec::new(),
            was_copy: false,
        };
        let err = RawResult {
            col_oids: vec![],
            rows: vec![],
            cmd_tag: String::new(),
            error: Some(("22012".to_string(), "division by zero".to_string())),
            copy_out: Vec::new(),
            was_copy: false,
        };
        match fold_results(&[ok.clone(), err]) {
            StmtOutcome::Error { sqlstate, .. } => assert_eq!(sqlstate, "22012"),
            other => panic!("expected error outcome, got {other:?}"),
        }
        match fold_results(&[ok]) {
            StmtOutcome::Rows { col_oids, rows } => {
                assert_eq!(col_oids, vec![23]);
                assert_eq!(rows.len(), 1);
            }
            other => panic!("expected rows outcome, got {other:?}"),
        }
        let cmd = RawResult {
            col_oids: vec![],
            rows: vec![],
            cmd_tag: "UPDATE 3".to_string(),
            error: None,
            copy_out: Vec::new(),
            was_copy: false,
        };
        match fold_results(&[cmd]) {
            StmtOutcome::Command { affected, .. } => assert_eq!(affected, Some(3)),
            other => panic!("expected command outcome, got {other:?}"),
        }
    }

    /// The charter asks to verify RETURNING rides the same rowset path as
    /// SELECT output: a DML statement with RETURNING folds to Rows (col
    /// oids present), so no-ORDER-BY multiset compare applies.
    #[test]
    fn returning_rows_fold_and_compare_as_multisets() {
        let returning = RawResult {
            col_oids: vec![23],
            rows: vec![
                vec![Some("1".to_string())],
                vec![Some("2".to_string())],
            ],
            cmd_tag: "UPDATE 2".to_string(),
            error: None,
            copy_out: Vec::new(),
            was_copy: false,
        };
        let folded = fold_results(&[returning]);
        assert!(matches!(folded, StmtOutcome::Rows { .. }));
        // Reordered RETURNING rows still match (multiset semantics).
        let swapped = StmtOutcome::Rows {
            col_oids: vec![23],
            rows: vec![
                vec![Some("2".to_string())],
                vec![Some("1".to_string())],
            ],
        };
        let c = classify(&DiffInput {
            sql: "UPDATE t AS t0 SET c = c + 1 RETURNING t0.c;",
            a: &folded,
            b: &swapped,
            ulp_tol: 4,
            soft_cols: &[],
            mask_explain_timing: false,
        });
        assert_eq!(c.class, DiffClass::Match);
    }
}

// =====================================================================
// sitediff site runner (plan §3.1 "runner.rs + supervisor.rs +
// logtail.rs", §3.2 StepRecord / ObservationRecord emission, §4.3, §4.4,
// §6 cell consumption; lane L0.3)
//
// Everything above this line is the F1-era lockstep runner (Executor /
// run_stream / Record) that diffrunner, covapply, reduce and the module
// rigs still drive; it stays as the adapter surface. Below is the
// contracts-native runner: one `Observer` per session per side
// (`ClientObserver` binds it to lane L0.1's lossless `client::Client`:
// simple / extended / pipeline exchanges, the read deadline as the hang
// witness, BackendKeyData as the pid, ParameterStatus as the version), a
// `SideRig` per side (pool + log tailer + supervisor + optional OS
// driver), and `SiteRunner::run_stream` emitting a StepRecord line and
// one ObservationRecord per side per step through a `Sink`. M0
// integration wires the comparator here: once both sides' records for a
// step exist, `diff::compare_planes_with` runs under a per-side
// `CanonCtx` (libdir / pgdata / user type names probed from each side,
// refreshed after DDL brackets and restarts) and the rulings ledger
// (`docs/fuzzing/rulings.toml`, else the embedded copy); every
// `Divergence` becomes a `contracts::Finding` (NEW, or RULED with the
// rule id) deduplicated per signature within the run (the first
// `SIGNATURE_KEEP` kept in full, a count line at the end).
// =====================================================================

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::canon::{self, CanonCtx, CanonSide};
use crate::client::{
    ConnectError, ConnectOpts, Describe, Exchange as WireExchange, ExtendedStep, Fault as WireFault, Frame, WireParam,
};
use crate::contracts::json::Value;
use crate::diff::{compare_planes_with, CompareOpts, Divergence};
use crate::rulings::Ledger;
use crate::contracts::{
    self, AuditFields, Bytes, Cell, Class, Finding, ObservationRecord, Repro, Severity, Side as CSide, Status,
    StepKind, StepRecord, Verified, WireMsg,
};
use crate::logtail::{self, PrefixSpec, Source, Tail};
use crate::probes::{self, Deck, RenderCtx, Trigger};
use crate::supervisor::{
    deadline_ms_for_cell, Action, Event, HangLadder, OsDriver, Rung, Supervisor, LADDER_GRACE_MS,
};

/// One completed exchange: every backend message through ReadyForQuery,
/// lossless, plus the wall time the observer measured (the only clock
/// reading that reaches a record, and it is fed by the observer).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Exchange {
    pub wire: Vec<WireMsg>,
    pub ms: u64,
}

/// Why an exchange did not complete.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// The per-step deadline passed with the step still in flight; `ms`
    /// is the elapsed time. The session is still connected: the runner
    /// escalates the hang ladder and calls `resume`.
    Hang { ms: u64 },
    /// The connection is gone (I/O error, EOF, framing loss).
    Lost(String),
}

/// One server session as the site runner sees it. `ClientObserver`
/// below binds it to lane L0.1's lossless `client::Client`; tests and
/// `sitediff smoke --dry-run` script it.
pub trait Observer {
    /// Send the step (its `sql` / `xproto` / copy payload per `kind`) and
    /// collect every backend message through ReadyForQuery, or fail with
    /// `Hang` once `deadline_ms` elapses with the step still in flight.
    fn exchange(&mut self, step: &StepRecord, deadline_ms: u64) -> Result<Exchange, Fault>;
    /// Keep waiting for the in-flight step after a hang-ladder rung fired
    /// (pg_cancel_backend / pg_terminate_backend from the monitor).
    fn resume(&mut self, deadline_ms: u64) -> Result<Exchange, Fault>;
    /// Run one runner-injected statement on this session (probe deck
    /// statements, GUC pins, ladder rungs on the monitor).
    fn probe(&mut self, sql: &str, deadline_ms: u64) -> Result<Exchange, Fault>;
    /// The BackendKeyData pid of this session (pgrust: synthetic
    /// MyProcPid), None before the first ReadyForQuery.
    fn backend_pid(&self) -> Option<u32>;
    /// The OS pid behind this session when it differs from the backend
    /// pid (C: the same; pgrust thread model: the server process) — the
    /// ladder's SIGKILL target.
    fn os_pid(&self) -> Option<i32>;
    /// Drop and re-dial the connection (after a crash / restart).
    fn reconnect(&mut self) -> Result<(), String>;
    /// `SHOW server_version` text captured at connect.
    fn version(&self) -> String;
}

/// Where the records go.
pub trait Sink {
    fn step(&mut self, rec: &StepRecord);
    fn observation(&mut self, rec: &ObservationRecord);
    fn finding(&mut self, f: &Finding);
    /// A non-Finding line for findings.jsonl: the per-signature count
    /// rows written at stream end for signatures that overflowed the
    /// dedup budget (`{"signature", "count", "kept"}`) and the ledger's
    /// hit counts (`{"rulings_hits": {...}}`).
    fn note(&mut self, _v: &Value) {}
}

/// In-memory sink (tests, `sitediff smoke`).
#[derive(Default, Debug)]
pub struct VecSink {
    pub steps: Vec<String>,
    pub obs_a: Vec<String>,
    pub obs_b: Vec<String>,
    pub findings: Vec<String>,
}

impl Sink for VecSink {
    fn step(&mut self, rec: &StepRecord) {
        self.steps.push(rec.to_jsonl());
    }
    fn observation(&mut self, rec: &ObservationRecord) {
        let line = rec.to_jsonl().expect("probe results carry no non-finite floats");
        match rec.side {
            CSide::A => self.obs_a.push(line),
            CSide::B => self.obs_b.push(line),
        }
    }
    fn finding(&mut self, f: &Finding) {
        self.findings.push(f.to_file().expect("finding serializes"));
    }
    fn note(&mut self, v: &Value) {
        self.findings.push(contracts::json::to_canonical(v).expect("note serializes"));
    }
}

/// File sink: `<out>/steps.jsonl`, `<out>/obs-a.jsonl`, `<out>/obs-b.jsonl`,
/// `<out>/findings.jsonl` (CONTRACTS.md "observation JSONL").
pub struct FileSink {
    steps: std::fs::File,
    obs_a: std::fs::File,
    obs_b: std::fs::File,
    findings: std::fs::File,
}

impl FileSink {
    pub fn create(out: &Path) -> std::io::Result<FileSink> {
        std::fs::create_dir_all(out)?;
        let open = |name: &str| std::fs::OpenOptions::new().create(true).append(true).open(out.join(name));
        Ok(FileSink {
            steps: open("steps.jsonl")?,
            obs_a: open("obs-a.jsonl")?,
            obs_b: open("obs-b.jsonl")?,
            findings: open("findings.jsonl")?,
        })
    }
}

impl Sink for FileSink {
    fn step(&mut self, rec: &StepRecord) {
        use std::io::Write;
        let _ = writeln!(self.steps, "{}", rec.to_jsonl());
    }
    fn observation(&mut self, rec: &ObservationRecord) {
        use std::io::Write;
        if let Ok(line) = rec.to_jsonl() {
            let f = match rec.side {
                CSide::A => &mut self.obs_a,
                CSide::B => &mut self.obs_b,
            };
            let _ = writeln!(f, "{line}");
        }
    }
    fn finding(&mut self, f: &Finding) {
        use std::io::Write;
        if let Ok(text) = contracts::json::to_canonical(&f.to_json()) {
            let _ = writeln!(self.findings, "{text}");
        }
    }
    fn note(&mut self, v: &Value) {
        use std::io::Write;
        if let Ok(text) = contracts::json::to_canonical(v) {
            let _ = writeln!(self.findings, "{text}");
        }
    }
}

// ---------------------------------------------------------------------
// cell.json consumption (plan §6)
// ---------------------------------------------------------------------

/// What the runner takes from the cell (and the cell workdir).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SiteConfig {
    pub cell: Cell,
    pub cell_id: String,
    /// `conf.guc_pin` — false drops the C-parity/datetime/locale session
    /// pins entirely (`guc_pin: off`).
    pub guc_pin: bool,
    /// Per-step deadline (cell-scaled).
    pub deadline_ms: u64,
    pub prefix: PrefixSpec,
    /// stderr log per side.
    pub a_log: PathBuf,
    pub b_log: PathBuf,
    /// `logging.mode == collector`: also tail `<datadir>/log/*.csv|json`.
    pub collector: bool,
    pub a_log_dir: PathBuf,
    pub b_log_dir: PathBuf,
    /// binupgrade (`-b`) cell: raw OIDs in the class deck.
    pub raw_oids: bool,
    /// Data directories per side (`$WORK/dda`, `$WORK/ddb`): the
    /// `<PGDATA>` canonicalization root until the side reports
    /// `SHOW data_directory`.
    pub a_pgdata: PathBuf,
    pub b_pgdata: PathBuf,
    /// `$libdir` per side when known from the rig (cell.env `PGBIN` for
    /// A, the pgrust binary's install for B); the side's
    /// `pg_config PKGLIBDIR` overrides it once probed.
    pub a_libdir: Option<String>,
    pub b_libdir: Option<String>,
}

impl SiteConfig {
    /// From a contracts `Cell` and the cell workdir layout
    /// (`sitediff-cell.sh up`: `$WORK/a.log`, `$WORK/b.log`,
    /// `$WORK/dda`, `$WORK/ddb`). The id is the contracts hash
    /// (`Cell::cell_id`): this is the in-process construction path.
    pub fn from_cell(cell: Cell, work: &Path) -> SiteConfig {
        let cell_id = cell.cell_id();
        SiteConfig::with_id(cell, cell_id, work)
    }

    fn with_id(cell: Cell, cell_id: String, work: &Path) -> SiteConfig {
        SiteConfig {
            guc_pin: cell.conf_guc_pin,
            deadline_ms: deadline_ms_for_cell(&cell),
            prefix: PrefixSpec::compile(&cell.logging_prefix),
            a_log: work.join("a.log"),
            b_log: work.join("b.log"),
            collector: cell.logging_mode == "collector" || cell.logging_csvlog || cell.logging_jsonlog,
            a_log_dir: work.join("dda").join("log"),
            b_log_dir: work.join("ddb").join("log"),
            raw_oids: cell.server_flags.iter().any(|f| f == "-b"),
            a_pgdata: work.join("dda"),
            b_pgdata: work.join("ddb"),
            a_libdir: None,
            b_libdir: None,
            cell_id,
            cell,
        }
    }

    /// Parse a `cell.json` (CONTRACTS.md "Cell identity"):
    ///
    /// * the `sitediff-cell/1` shape lane L0.4's factory writes keeps the
    ///   FACTORY's identity: `cell_id = sha256(canonical(original JSON))`
    ///   with any `cell_id` key removed first (the factory never stores
    ///   the id inside the document; `sitediff-cell.sh id` prints it).
    ///   When the document does carry a `cell_id` key (a copy annotated
    ///   by hand or by a wrapper) it must equal the recomputed hash —
    ///   a mismatch is a hard failure, never a silent re-hash. The
    ///   factory JSON is then mapped onto the contracts `Cell`; the
    ///   contracts hash of that mapped form is NOT the cell's id.
    /// * the contracts shape (`cell.schema.json`) hashes through
    ///   `Cell::cell_id` (the same function `from_cell` uses).
    pub fn from_cell_json(text: &str, work: &Path) -> Result<SiteConfig, String> {
        let v = contracts::json::parse(text)?;
        if v.get("schema").and_then(|s| s.as_str()) == Some("sitediff-cell/1") {
            let id = factory_cell_id(&v)?;
            let cell = cell_from_factory_json(&v)?;
            return Ok(SiteConfig::with_id(cell, id, work));
        }
        let cell = Cell::from_json(&v)?;
        Ok(SiteConfig::from_cell(cell, work))
    }

    /// `from_cell_json` cross-checked against an id the rig recorded
    /// elsewhere (cell.env `CELL_ID`): a mismatch is a hard failure.
    pub fn from_cell_json_expecting(text: &str, work: &Path, expected_id: &str) -> Result<SiteConfig, String> {
        let cfg = SiteConfig::from_cell_json(text, work)?;
        if cfg.cell_id != expected_id {
            return Err(format!("cell_id mismatch: cell.json hashes to {} but the rig recorded {}", cfg.cell_id, expected_id));
        }
        Ok(cfg)
    }

    /// The session pin statements this cell applies (empty under
    /// `guc_pin: off`).
    pub fn pin_sql(&self) -> Vec<String> {
        if self.guc_pin {
            c_parity_pin_sql()
        } else {
            Vec::new()
        }
    }
}

/// The factory's own identity of a `sitediff-cell/1` document:
/// `sha256(canonical(JSON minus any "cell_id" key))`, exactly what
/// `scripts/sitediff-cell.sh id` prints (`cell_id_of`: sorted keys, no
/// whitespace, no trailing newline). A carried `cell_id` must agree.
pub fn factory_cell_id(v: &Value) -> Result<String, String> {
    let fields = v.as_obj().ok_or("cell.json must be an object")?;
    let carried = v.get("cell_id").map(|c| c.as_str().map(str::to_string).ok_or("cell_id must be a string")).transpose()?;
    let stripped = Value::Obj(fields.iter().filter(|(k, _)| k != "cell_id").cloned().collect());
    let canonical = contracts::json::to_canonical(&stripped)?;
    let id = contracts::hex(&pg_sha2::sha256(canonical.as_bytes()));
    if let Some(c) = carried {
        if c != id {
            return Err(format!("cell.json carries cell_id {c} but its canonical form hashes to {id}"));
        }
    }
    Ok(id)
}

/// Map lane L0.4's `sitediff-cell/1` JSON onto the contracts `Cell`.
pub fn cell_from_factory_json(v: &Value) -> Result<Cell, String> {
    let mut cell = Cell::base();
    let s = |v: &Value, k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string);
    if let Some(b) = s(v, "b_build") {
        cell.b_build = b;
    }
    if let Some(h) = s(v, "host") {
        cell.host = h;
    }
    if let Some(t) = s(v, "topology") {
        cell.topology = t;
    }
    if let Some(h) = s(v, "hba") {
        cell.hba_method = h;
    }
    if let Some(name) = s(v, "name") {
        cell.conf_profile = Some(name);
    }
    if let Some(o) = v.get("oracle") {
        if let Some(r) = s(o, "ref") {
            cell.oracle_version = r;
        }
        match s(o, "variant").as_deref() {
            Some("plain") | None => {}
            Some(other) => cell.oracle_variants = vec![other.to_string()],
        }
    }
    if let Some(i) = v.get("initdb") {
        if let Some(e) = s(i, "encoding") {
            cell.initdb_encoding = e;
        }
        if let Some(l) = s(i, "locale") {
            cell.initdb_locale = l;
        }
        if let Some(c) = i.get("checksums").and_then(|x| x.as_bool()) {
            cell.initdb_checksums = c;
        }
    }
    for key in ["conf", "conf_a", "conf_b"] {
        if let Some(obj) = v.get(key).and_then(|x| x.as_obj()) {
            for (k, val) in obj {
                let val = val.as_str().unwrap_or("").to_string();
                let name = match key {
                    "conf" => k.clone(),
                    "conf_a" => format!("a:{k}"),
                    _ => format!("b:{k}"),
                };
                if k == "log_min_messages" && key == "conf" {
                    cell.logging_min_messages = val.clone();
                }
                cell.conf_gucs.insert(name, val);
            }
        }
    }
    if let Some(p) = v.get("pins") {
        cell.conf_guc_pin = s(p, "guc_pin").as_deref() != Some("off");
        if let Some(io) = s(p, "io_method") {
            cell.conf_gucs.insert("b:io_method".into(), io);
        }
        if let Some(sp) = s(p, "max_stack_depth") {
            cell.conf_gucs.insert("b:max_stack_depth".into(), sp);
        }
    }
    if let Some(l) = v.get("logging") {
        let collector = l.get("collector").and_then(|x| x.as_bool()).unwrap_or(false);
        let dest = s(l, "log_destination").unwrap_or_else(|| "stderr".into());
        cell.logging_mode = if collector { "collector".into() } else { "stderr".into() };
        cell.logging_csvlog = dest.contains("csvlog");
        cell.logging_jsonlog = dest.contains("jsonlog");
        if let Some(p) = s(l, "log_line_prefix") {
            cell.logging_prefix = p;
        }
        if let Some(tz) = s(l, "log_timezone") {
            cell.logging_timezone = tz;
        }
    }
    if let Some(flags) = v.get("server_flags").and_then(|x| x.as_arr()) {
        cell.server_flags = flags.iter().filter_map(|f| f.as_str().map(str::to_string)).collect();
    }
    Ok(cell)
}

// ---------------------------------------------------------------------
// One side: pool + tailer + supervisor
// ---------------------------------------------------------------------

/// Connects observers for a side: `connect(session_name)`.
pub type Connector = Box<dyn FnMut(&str) -> Result<Box<dyn Observer>, String>>;

/// Per-session runner state.
struct SessionSlot {
    obs: Box<dyn Observer>,
    txn_open: bool,
    txn_had_ddl: bool,
    txn_had_dml: bool,
    /// Last DDL target while the transaction is open.
    last_target: Option<String>,
}

pub struct SideRig {
    pub side: CSide,
    connector: Connector,
    sessions: Vec<(String, SessionSlot)>,
    monitor: Option<Box<dyn Observer>>,
    tail: Tail,
    collector_tails: Vec<Tail>,
    pub supervisor: Supervisor,
    driver: Option<OsDriver>,
    /// Lines already fed to the supervisor (tail line count).
    fed: u64,
    last_stats: Option<Value>,
    version: String,
}

impl SideRig {
    pub fn new(side: CSide, cfg: &SiteConfig, connector: Connector, driver: Option<OsDriver>) -> SideRig {
        let (log, dir) = match side {
            CSide::A => (&cfg.a_log, &cfg.a_log_dir),
            CSide::B => (&cfg.b_log, &cfg.b_log_dir),
        };
        let collector_tails = if cfg.collector {
            logtail::collector_files(dir).into_iter().map(|(p, src)| Tail::new(&p, src, &cfg.prefix)).collect()
        } else {
            Vec::new()
        };
        SideRig {
            side,
            connector,
            sessions: Vec::new(),
            monitor: None,
            tail: Tail::new(log, Source::Stderr, &cfg.prefix),
            collector_tails,
            supervisor: Supervisor::new(side),
            driver,
            fed: 0,
            last_stats: None,
            version: String::new(),
        }
    }

    pub fn tail(&self) -> &Tail {
        &self.tail
    }

    pub fn tail_mut(&mut self) -> &mut Tail {
        &mut self.tail
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    /// Read every tailed file and feed the new stderr lines to the
    /// supervisor. Returns the supervisor's actions.
    pub fn poll_logs(&mut self) -> Vec<Action> {
        let _ = self.tail.poll();
        for t in &mut self.collector_tails {
            let _ = t.poll();
        }
        let mut actions = Vec::new();
        let lines = self.tail.parser().lines();
        let new: Vec<String> =
            lines.iter().filter(|l| l.line_no > self.fed).map(|l| String::from_utf8_lossy(&l.rec.raw.0).into_owned()).collect();
        self.fed = lines.len() as u64;
        for line in new {
            actions.extend(self.supervisor.observe(Event::Log(line)));
        }
        if let Some(d) = self.driver.as_mut() {
            if let Some(ev) = d.poll_exit() {
                actions.extend(self.supervisor.observe(ev));
            }
        }
        actions
    }

    fn connect_session(&mut self, name: &str, pins: &[String], deadline_ms: u64) -> Result<(), String> {
        let mut obs = (self.connector)(name)?;
        for sql in pins {
            let _ = obs.probe(sql, deadline_ms);
        }
        if self.version.is_empty() {
            self.version = obs.version();
        }
        let slot = SessionSlot { obs, txn_open: false, txn_had_ddl: false, txn_had_dml: false, last_target: None };
        match self.sessions.iter_mut().find(|(n, _)| n == name) {
            Some(s) => s.1 = slot,
            None => self.sessions.push((name.to_string(), slot)),
        }
        Ok(())
    }

    fn ensure_monitor(&mut self) -> Result<(), String> {
        if self.monitor.is_none() {
            let obs = (self.connector)("@monitor")?;
            if self.version.is_empty() {
                self.version = obs.version();
            }
            self.monitor = Some(obs);
        }
        Ok(())
    }

    fn session_index(&self, name: &str) -> Option<usize> {
        self.sessions.iter().position(|(n, _)| n == name)
    }

    /// Reconnect every session and the monitor (after a restart).
    fn reconnect_all(&mut self, pins: &[String], deadline_ms: u64) -> Result<(), String> {
        let names: Vec<String> = self.sessions.iter().map(|(n, _)| n.clone()).collect();
        for n in names {
            if let Some(i) = self.session_index(&n) {
                let slot = &mut self.sessions[i].1;
                slot.obs.reconnect()?;
                for sql in pins {
                    let _ = slot.obs.probe(sql, deadline_ms);
                }
                slot.txn_open = false;
                slot.txn_had_ddl = false;
                slot.txn_had_dml = false;
                slot.last_target = None;
            }
        }
        if let Some(m) = self.monitor.as_mut() {
            m.reconnect()?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------
// The site runner
// ---------------------------------------------------------------------

/// Run summary: counts plus the self-oracle rows and the prefix
/// invariant verdict (findings are also written to the sink).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SiteSummary {
    pub steps: u64,
    pub crashes_a: u32,
    pub crashes_b: u32,
    pub hangs: u32,
    pub panics: u32,
    pub probe_rounds: u32,
    /// invariants.sql rows on B (deck key, row JSON).
    pub invariant_rows: Vec<(String, String)>,
    /// invariants.sql statements that returned rows on A (deck key ->
    /// first row JSON, rows seen): the invariant is wrong, not B — a rig
    /// error, reported once per key and never as a B finding.
    pub invariant_rig_errors: BTreeMap<String, (String, u64)>,
    /// B lines breaking the prefix invariant (line number, raw).
    pub prefix_violations: Vec<(u64, String)>,
    pub findings: u32,
    /// Plane divergences the comparator reported (ruled ones included;
    /// `ruled` counts those covered by a ledger row).
    pub divergences: u32,
    pub ruled: u32,
    /// The boot self-test verdict, when run.
    pub self_test: Option<logtail::SelfTest>,
    /// Wall time (ms, runner clock — summary only, never a record field)
    /// spent in step exchanges (both sides, serial) and in probe rounds,
    /// plus per-deck totals and run counts (`deck -> (ms, runs)`).
    pub steps_ms: u64,
    pub probes_ms: u64,
    pub deck_ms: BTreeMap<String, (u64, u64)>,
}

/// Bounded wait for a restart to complete (polls of the logs / driver).
const RESTART_POLLS: u32 = 600;

/// Signature dedup within a run: the first this many findings per
/// signature are written in full, then a count line at stream end.
pub const SIGNATURE_KEEP: u32 = 5;

/// Forces the calling backend's pending cumulative stats out (run on
/// every stream session before the stats deck reads on the monitor).
pub const STATS_FLUSH_SQL: &str = "SELECT pg_stat_force_next_flush();";

/// The one-time user-type probe behind `CanonCtx::type_names`
/// (`u:<typname>` canonicalization of user OIDs, plan §4.1), re-run
/// after every DDL bracket and restart.
pub const TYPE_NAMES_SQL: &str = "SELECT oid, typname FROM pg_type WHERE oid >= 16384 ORDER BY oid;";

/// The rulings ledger the site runner resolves against: `SITEDIFF_RULINGS`
/// when set, else the repo's `docs/fuzzing/rulings.toml`, else the copy
/// compiled into the binary.
pub fn load_ledger() -> Ledger {
    let path = std::env::var_os("SITEDIFF_RULINGS")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../docs/fuzzing/rulings.toml"));
    Ledger::load(&path).unwrap_or_else(|_| Ledger::embedded())
}

pub struct SiteRunner {
    pub cfg: SiteConfig,
    pub scenario: String,
    pub a: SideRig,
    pub b: SideRig,
    pins: Vec<String>,
    ladder: HangLadder,
    pub summary: SiteSummary,
    /// Last step seq (for the stream-end probe record).
    last_seq: u64,
    /// Generated sequences for the invariants deck.
    pub sequences: Vec<String>,
    /// Columns per probeable table (name → (pk, columns)) for the
    /// physical probes.
    pub tables: Vec<(String, String, Vec<String>)>,
    /// GUCs the stream SET (the `show_guc` deck).
    set_gucs: Vec<String>,
    /// The rulings ledger; hits accumulate on it (`ledger.hits()`).
    pub ledger: Ledger,
    /// Per-side canonicalization contexts, (re)probed while `ctx_stale`.
    ctx_a: CanonCtx,
    ctx_b: CanonCtx,
    ctx_stale: bool,
    /// Signature -> occurrences this run (dedup, `SIGNATURE_KEEP`).
    sig_counts: BTreeMap<String, u32>,
    /// Float ulp budget handed to the comparator.
    pub ulp_tol: u64,
}

/// The per-step observation on one side, before it becomes a record.
#[allow(dead_code)]
struct StepObs {
    wire: Vec<WireMsg>,
    ms: u64,
    liveness: String,
    hang: Option<contracts::Hang>,
    lost: Option<String>,
}

impl SiteRunner {
    pub fn new(cfg: SiteConfig, scenario: &str, a: SideRig, b: SideRig) -> SiteRunner {
        let pins = cfg.pin_sql();
        let ladder = HangLadder::new(cfg.deadline_ms, LADDER_GRACE_MS);
        let path_text = |p: &Path| p.to_str().map(str::to_string);
        let ctx_a = CanonCtx {
            libdir: cfg.a_libdir.clone(),
            pgdata: path_text(&cfg.a_pgdata),
            dlsuffix: canon::host_dlsuffix().to_string(),
            type_names: BTreeMap::new(),
        };
        let ctx_b = CanonCtx { libdir: cfg.b_libdir.clone(), pgdata: path_text(&cfg.b_pgdata), ..ctx_a.clone() };
        SiteRunner {
            cfg,
            scenario: scenario.to_string(),
            a,
            b,
            pins,
            ladder,
            summary: SiteSummary::default(),
            last_seq: 0,
            sequences: Vec::new(),
            tables: Vec::new(),
            set_gucs: Vec::new(),
            ledger: load_ledger(),
            ctx_a,
            ctx_b,
            ctx_stale: true,
            sig_counts: BTreeMap::new(),
            ulp_tol: CompareOpts::default().ulp_tol,
        }
    }

    /// Resolve against this ledger instead of `load_ledger()`.
    pub fn with_ledger(mut self, ledger: Ledger) -> SiteRunner {
        self.ledger = ledger;
        self
    }

    /// The canonicalization contexts in force (after the last probe).
    pub fn canon_ctx(&self, side: CSide) -> &CanonCtx {
        match side {
            CSide::A => &self.ctx_a,
            CSide::B => &self.ctx_b,
        }
    }

    /// (Re)probe one side's `CanonCtx` on its monitor: `SHOW
    /// data_directory` (the `<PGDATA>` root), `pg_config` PKGLIBDIR (the
    /// `<LIBDIR>` root), and the user-range `pg_type` names. Values the
    /// side cannot answer keep the rig's defaults.
    fn refresh_ctx(&mut self, side: CSide) {
        let deadline = self.cfg.deadline_ms;
        let rig = match side {
            CSide::A => &mut self.a,
            CSide::B => &mut self.b,
        };
        if rig.ensure_monitor().is_err() {
            return;
        }
        let m = rig.monitor.as_mut().expect("monitor");
        let ctx = match side {
            CSide::A => &mut self.ctx_a,
            CSide::B => &mut self.ctx_b,
        };
        let path_answer = |ex: Result<Exchange, Fault>| -> Option<String> {
            let rows = wire_text_rows(&ex.ok()?.wire);
            match rows.first().and_then(|r| r.first()).cloned().flatten() {
                Some(p) if p.starts_with('/') => Some(p),
                _ => None,
            }
        };
        if let Some(p) = path_answer(m.probe("SHOW data_directory;", deadline)) {
            ctx.pgdata = Some(p);
        }
        if let Some(p) = path_answer(m.probe("SELECT setting FROM pg_config() WHERE name = 'PKGLIBDIR';", deadline)) {
            ctx.libdir = Some(p);
        }
        if let Ok(ex) = m.probe(TYPE_NAMES_SQL, deadline) {
            let mut names = BTreeMap::new();
            for row in wire_text_rows(&ex.wire) {
                if let [Some(oid), Some(name)] = row.as_slice() {
                    if let Ok(oid) = oid.parse::<u32>() {
                        names.insert(oid, name.clone());
                    }
                }
            }
            ctx.type_names = names;
        }
    }

    /// The comparator over one step's two records (plan §3.3): planes
    /// under the per-side `CanonCtx`, rulings resolved (hits recorded on
    /// the ledger), each divergence a Finding through the sink, deduped
    /// per signature.
    fn compare_step(&mut self, step: &StepRecord, a: &ObservationRecord, b: &ObservationRecord, sink: &mut dyn Sink) {
        if self.ctx_stale {
            self.refresh_ctx(CSide::A);
            self.refresh_ctx(CSide::B);
            self.ctx_stale = false;
        }
        let opts = CompareOpts {
            ulp_tol: self.ulp_tol,
            soft_cols: Vec::new(),
            mask_explain_timing: false,
            ctx_a: self.ctx_a.clone(),
            ctx_b: self.ctx_b.clone(),
        };
        let divs = compare_planes_with(a, b, step, &self.ledger, &opts);
        if divs.is_empty() {
            return;
        }
        let ca = canon::canonicalize(a, &opts.ctx_a);
        let cb = canon::canonicalize(b, &opts.ctx_b);
        for d in &divs {
            self.summary.divergences += 1;
            if d.is_ruled() {
                self.summary.ruled += 1;
            }
            let n = self.sig_counts.entry(d.signature.clone()).or_insert(0);
            *n += 1;
            // A probe deck is state: a persistent divergence keeps its
            // first (statement = repro) and counts the rest.
            let keep = if d.plane.starts_with("probe:") { 1 } else { SIGNATURE_KEEP };
            if *n > keep {
                continue;
            }
            let f = divergence_finding(&self.cfg, step, d, &ca, &cb);
            sink.finding(&f);
            self.summary.findings += 1;
        }
    }

    fn rig(&mut self, side: CSide) -> &mut SideRig {
        match side {
            CSide::A => &mut self.a,
            CSide::B => &mut self.b,
        }
    }

    /// Boot-time tailer self test on B (plan §4.3): a throwaway target
    /// session's vpid is crashed from the monitor with
    /// `pgrust: crash backend <vpid> quit`; the tailer must witness the
    /// death in the slice, and the supervisor absorbs any restart. A
    /// server without `PGRUST_CRASH_TEST` answers an ERROR and the
    /// verdict is `witnessed = false` (reported, not fatal).
    pub fn self_test(&mut self) -> Result<logtail::SelfTest, String> {
        let deadline = self.cfg.deadline_ms;
        let pins = self.pins.clone();
        self.b.ensure_monitor()?;
        self.b.connect_session("@selftest", &pins, deadline)?;
        let i = self.b.session_index("@selftest").expect("just connected");
        let vpid = self.b.sessions[i].1.obs.backend_pid().unwrap_or(0);
        let _ = self.b.poll_logs();
        let m0 = self.b.tail.mark();
        let sql = logtail::self_test_sql(vpid);
        let _ = self.b.monitor.as_mut().expect("monitor").probe(&sql, deadline);
        let _ = self.b.sessions[i].1.obs.probe("SELECT 1;", deadline);
        let actions = self.b.poll_logs();
        self.b.tail.parser_mut().finish();
        let m1 = self.b.tail.mark();
        let verdict = logtail::self_test_verdict(self.b.tail.parser(), m0, m1, vpid);
        self.handle_actions(CSide::B, actions);
        self.b.sessions.retain(|(n, _)| n != "@selftest");
        self.summary.self_test = Some(verdict.clone());
        Ok(verdict)
    }

    /// Carry out supervisor actions for a side: restart the dead server,
    /// wait for the redo/ready witnesses, reconnect the pools
    /// symmetrically, bump the generation.
    fn handle_actions(&mut self, side: CSide, actions: Vec<Action>) {
        let deadline = self.cfg.deadline_ms;
        let pins = self.pins.clone();
        let mut queue: VecDeque<Action> = actions.into();
        let mut polls = 0u32;
        while let Some(a) = queue.pop_front() {
            match a {
                Action::BankLogTail => {
                    match side {
                        CSide::A => self.summary.crashes_a += 1,
                        CSide::B => self.summary.crashes_b += 1,
                    }
                }
                Action::Restart => {
                    let rig = self.rig(side);
                    if let Some(d) = rig.driver.as_mut() {
                        if d.restart().is_ok() {
                            queue.extend(rig.supervisor.observe(Event::RestartIssued));
                        }
                    } else {
                        // No driver (tests / attached rig): the restart is
                        // external; still track the witnesses.
                        queue.extend(rig.supervisor.observe(Event::RestartIssued));
                    }
                }
                Action::ReconnectPools => {
                    // Symmetric: both pools reconnect so the session
                    // numbering and transaction state agree again.
                    let _ = self.a.reconnect_all(&pins, deadline);
                    let _ = self.b.reconnect_all(&pins, deadline);
                    let rig = self.rig(side);
                    queue.extend(rig.supervisor.observe(Event::PoolsReconnected));
                }
                Action::Generation(_) => {
                    // The stream resumes as post-crash; the restart probe
                    // round runs from the step loop.
                }
            }
            // While a restart is pending, keep polling the logs (bounded)
            // for the redo/ready witnesses.
            if queue.is_empty() {
                let rig = self.rig(side);
                if !rig.supervisor.is_serving() && polls < RESTART_POLLS {
                    polls += 1;
                    if rig.driver.is_some() {
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    let more = rig.poll_logs();
                    if more.is_empty() && rig.driver.is_none() {
                        // Without a driver there is nothing to wait for:
                        // the witnesses are either already in the log or
                        // will arrive with the next step's poll.
                        break;
                    }
                    queue.extend(more);
                }
            }
        }
    }

    /// Run the hang ladder for a session on one side. Returns the
    /// exchange if the step eventually completed, else the last fault.
    fn escalate(&mut self, side: CSide, session: usize, first_ms: u64) -> (Result<Exchange, Fault>, Option<contracts::Hang>) {
        self.ladder.reset();
        let mut elapsed = first_ms.max(self.cfg.deadline_ms);
        let mut last: Result<Exchange, Fault> = Err(Fault::Hang { ms: elapsed });
        while let Some(rung) = self.ladder.elapsed(elapsed) {
            let rig = self.rig(side);
            let backend_pid = rig.sessions[session].1.obs.backend_pid();
            match rung {
                Rung::Cancel | Rung::Terminate => {
                    if let (Some(pid), Ok(())) = (backend_pid, rig.ensure_monitor()) {
                        if let Some(sql) = rung.sql(pid) {
                            let _ = rig.monitor.as_mut().expect("monitor").probe(&sql, LADDER_GRACE_MS);
                        }
                    }
                    last = rig.sessions[session].1.obs.resume(LADDER_GRACE_MS);
                }
                Rung::SigKill => {
                    let target = rig.sessions[session].1.obs.os_pid().or(backend_pid.map(|p| p as i32));
                    if let Some(pid) = target {
                        OsDriver::sigkill(pid);
                    }
                    last = rig.sessions[session].1.obs.resume(LADDER_GRACE_MS);
                }
            }
            match &last {
                Ok(_) | Err(Fault::Lost(_)) => break,
                Err(Fault::Hang { ms }) => elapsed += (*ms).max(LADDER_GRACE_MS),
            }
        }
        let hang = self.ladder.hang_record();
        (last, hang)
    }

    /// Execute a step on one side (connect/disconnect handled here).
    fn exchange_on(&mut self, side: CSide, step: &StepRecord) -> StepObs {
        let deadline = self.cfg.deadline_ms;
        let pins = self.pins.clone();
        let rig = self.rig(side);
        rig.supervisor.note_statement(&step.session, step.sql.as_deref().unwrap_or(&step.kind.to_string_key()));
        let liveness_base = if rig.supervisor.post_crash() { "post-crash" } else { "ok" };
        match &step.kind {
            StepKind::Connect => {
                return match rig.connect_session(&step.session, &pins, deadline) {
                    Ok(()) => StepObs { wire: Vec::new(), ms: 0, liveness: liveness_base.into(), hang: None, lost: None },
                    Err(e) => StepObs { wire: Vec::new(), ms: 0, liveness: "dead".into(), hang: None, lost: Some(e) },
                };
            }
            StepKind::Disconnect => {
                rig.sessions.retain(|(n, _)| n != &step.session);
                return StepObs { wire: Vec::new(), ms: 0, liveness: liveness_base.into(), hang: None, lost: None };
            }
            _ => {}
        }
        if rig.session_index(&step.session).is_none() {
            if let Err(e) = rig.connect_session(&step.session, &pins, deadline) {
                return StepObs { wire: Vec::new(), ms: 0, liveness: "dead".into(), hang: None, lost: Some(e) };
            }
        }
        let i = rig.session_index(&step.session).expect("connected above");
        let mut liveness = liveness_base.to_string();
        let mut result = rig.sessions[i].1.obs.exchange(step, deadline);
        let mut hang = None;
        if let Err(Fault::Hang { ms }) = &result {
            let ms = *ms;
            let (r, h) = self.escalate(side, i, ms);
            result = r;
            hang = h;
            self.summary.hangs += 1;
        }
        let rig = self.rig(side);
        // Re-pin after RESET/DISCARD on both sides identically.
        if let (Some(sql), Ok(_)) = (step.sql.as_deref(), &result) {
            if clobbers_session_gucs(sql) {
                for p in &pins {
                    let _ = rig.sessions[i].1.obs.probe(p, deadline);
                }
            }
        }
        match result {
            Ok(ex) => {
                let slot = &mut rig.sessions[i].1;
                if let Some(sql) = step.sql.as_deref() {
                    track_txn(slot, sql);
                }
                StepObs { wire: ex.wire, ms: ex.ms, liveness, hang, lost: None }
            }
            Err(Fault::Lost(detail)) => {
                let _ = rig.supervisor.observe(Event::ConnLost { session: step.session.clone(), detail: detail.clone() });
                liveness = "dead".into();
                StepObs { wire: Vec::new(), ms: hang.as_ref().map(|h| h.ms).unwrap_or(0), liveness, hang, lost: Some(detail) }
            }
            Err(Fault::Hang { ms }) => {
                liveness = "dead".into();
                StepObs { wire: Vec::new(), ms, liveness, hang, lost: Some("hung past the ladder".into()) }
            }
        }
    }

    /// Run a probe deck on one side and fold it. Session-bound decks run
    /// on the issuing session; the rest on the monitor.
    fn run_deck(&mut self, side: CSide, deck: Deck, session: Option<&str>, ctx: &RenderCtx) -> Value {
        let deadline = self.cfg.deadline_ms;
        let rig = self.rig(side);
        let stmts = probes::render(deck, ctx);
        let mut results: Vec<(String, Vec<WireMsg>)> = Vec::new();
        let use_session = deck.session_bound().then(|| session).flatten().and_then(|s| rig.session_index(s));
        if use_session.is_none() && rig.ensure_monitor().is_err() {
            return Value::obj();
        }
        if deck == Deck::Stats {
            // Plan §4.4: exact-integer deltas after pg_stat_force_next_flush().
            // The flush is per backend (it forces the CALLING backend's
            // pending stats out before its ReadyForQuery), so it must run
            // on every stream session — the monitor's own flush in the
            // deck cannot publish another session's pending counters
            // (first LIVE smoke: B read as 0 where A had flushed by the
            // 1 s idle interval). A session inside an open transaction
            // keeps its pending stats on both engines alike.
            for i in 0..rig.sessions.len() {
                let _ = rig.sessions[i].1.obs.probe(STATS_FLUSH_SQL, deadline);
            }
        }
        for s in stmts {
            let r = match use_session {
                Some(i) => rig.sessions[i].1.obs.probe(&s.sql, deadline),
                None => rig.monitor.as_mut().expect("monitor").probe(&s.sql, deadline),
            };
            match r {
                Ok(ex) => results.push((s.key, ex.wire)),
                Err(Fault::Lost(d)) => {
                    let _ = rig.supervisor.observe(Event::ConnLost { session: "@monitor".into(), detail: d });
                    break;
                }
                Err(Fault::Hang { .. }) => break,
            }
        }
        let folded = probes::deck_result(&results);
        if deck == Deck::Stats {
            let prev = rig.last_stats.replace(folded.clone()).unwrap_or_else(Value::obj);
            return probes::stats_delta(&prev, &folded);
        }
        folded
    }

    /// Run a probe round (both sides) and return per-side deck results.
    fn probe_round(&mut self, trigger: &Trigger, session: Option<&str>, target: Option<&str>) -> (Vec<(String, Value)>, Vec<(String, Value)>) {
        self.summary.probe_rounds += 1;
        let ctx = self.render_ctx(target);
        let mut out_a = Vec::new();
        let mut out_b = Vec::new();
        for deck in probes::schedule(trigger) {
            let t0 = std::time::Instant::now();
            let ra = self.run_deck(CSide::A, deck, session, &ctx);
            let rb = self.run_deck(CSide::B, deck, session, &ctx);
            let ms = t0.elapsed().as_millis() as u64;
            self.summary.probes_ms += ms;
            let e = self.summary.deck_ms.entry(deck.name().to_string()).or_insert((0, 0));
            e.0 += ms;
            e.1 += 1;
            if deck.self_oracle() {
                // A self-oracle statement is validated on A first: a row
                // there means the invariant is wrong (rig error, once per
                // key), and B's rows for that key are not B findings.
                for (k, row) in probes::self_oracle_rows(&ra) {
                    let text = contracts::json::to_canonical(&row).unwrap_or_default();
                    let e = self.summary.invariant_rig_errors.entry(k).or_insert((text, 0));
                    e.1 += 1;
                }
                for (k, row) in probes::self_oracle_rows(&rb) {
                    if self.summary.invariant_rig_errors.contains_key(&k) {
                        continue;
                    }
                    let text = contracts::json::to_canonical(&row).unwrap_or_default();
                    self.summary.invariant_rows.push((k, text));
                }
            }
            out_a.push((deck.name().to_string(), ra));
            out_b.push((deck.name().to_string(), rb));
        }
        (out_a, out_b)
    }

    fn render_ctx(&self, target: Option<&str>) -> RenderCtx {
        let (pk, columns) = target
            .and_then(|t| self.tables.iter().find(|(n, _, _)| n == t))
            .map(|(_, pk, cols)| (Some(pk.clone()), cols.clone()))
            .unwrap_or((None, Vec::new()));
        RenderCtx {
            table: target.map(str::to_string),
            pk,
            columns,
            sequences: self.sequences.clone(),
            gucs: self.set_gucs.clone(),
            raw_oids: self.cfg.raw_oids,
        }
    }

    fn observation(&self, step_seq: u64, session: &str, side: CSide, obs: StepObs, log: Vec<contracts::LogLine>, panic: Option<contracts::Panic>, crash: Option<contracts::Crash>, probes: Vec<(String, Value)>) -> ObservationRecord {
        let rig = match side {
            CSide::A => &self.a,
            CSide::B => &self.b,
        };
        ObservationRecord {
            scenario: self.scenario.clone(),
            seq: step_seq,
            session: session.to_string(),
            side,
            wire: obs.wire,
            log,
            panic,
            crash,
            hang: obs.hang,
            probes: probes.into_iter().collect(),
            server: None,
            liveness: obs.liveness,
            ms: obs.ms,
            version: rig.version.clone(),
        }
    }

    /// One step on both sides: emit the StepRecord, then an
    /// ObservationRecord per side. A crash on either side does NOT end
    /// the stream: the supervisor restarts, both pools reconnect, the
    /// generation bumps and the next step runs as `post-crash`.
    pub fn run_step(&mut self, step: &StepRecord, sink: &mut dyn Sink) {
        sink.step(step);
        self.summary.steps += 1;
        self.last_seq = step.seq;
        if let Some(sql) = step.sql.as_deref() {
            if let Some(g) = set_guc_name(sql) {
                if !self.set_gucs.contains(&g) {
                    self.set_gucs.push(g);
                }
            }
        }
        let mut records = Vec::new();
        let mut crashed_sides = Vec::new();
        for side in [CSide::A, CSide::B] {
            let _ = self.rig(side).poll_logs();
            let m0 = self.rig(side).tail.mark();
            let t0 = std::time::Instant::now();
            let obs = self.exchange_on(side, step);
            self.summary.steps_ms += t0.elapsed().as_millis() as u64;
            let actions = self.rig(side).poll_logs();
            let crashed = actions.iter().any(|a| matches!(a, Action::BankLogTail));
            if crashed {
                crashed_sides.push(side);
            }
            let rig = self.rig(side);
            rig.tail.parser_mut().finish();
            let m1 = rig.tail.mark();
            let pid = rig.session_index(&step.session).and_then(|i| rig.sessions[i].1.obs.backend_pid());
            let lines = rig.tail.parser().lines();
            let mut log = match step.kind {
                // The auth phase has no BackendKeyData: the @mark slice.
                StepKind::Connect => logtail::slice_all(lines, m0, m1),
                _ => logtail::slice_for_pid(lines, m0, m1, pid),
            };
            for t in &rig.collector_tails {
                log.extend(t.parser().lines().iter().filter(|l| l.rec.pid == pid || pid.is_none()).map(|l| l.rec.clone()));
            }
            let panic = logtail::panics_in(rig.tail.parser(), m0, m1).into_iter().next();
            if panic.is_some() {
                self.summary.panics += 1;
            }
            self.handle_actions(side, actions);
            // The crash record carries the generation the stream resumed
            // under, so it is read after the restart completed.
            let crash = if crashed { self.rig(side).supervisor.crash_record() } else { None };
            let post = self.rig(side).supervisor.post_crash();
            let mut obs = obs;
            if crashed || (post && obs.liveness == "dead") {
                obs.liveness = "post-crash".into();
            }
            records.push((side, obs, log, panic, crash));
        }
        // Probe scheduling after the step (plan §4.4).
        let mut probes_a = Vec::new();
        let mut probes_b = Vec::new();
        let mut triggers: Vec<(Trigger, Option<String>)> = Vec::new();
        if !crashed_sides.is_empty() {
            triggers.push((Trigger::Restart, None));
        }
        if let Some(sql) = step.sql.as_deref() {
            let (in_txn, bracket_ddl_end, bracket_dml_end) = {
                let rig = &self.b;
                match rig.session_index(&step.session) {
                    Some(i) => {
                        let s = &rig.sessions[i].1;
                        let ended = probes::txn_control(sql) == Some("end");
                        (s.txn_open, ended && s.txn_had_ddl, ended && s.txn_had_dml)
                    }
                    None => (false, false, false),
                }
            };
            if probes::is_ddl(sql) {
                let target = probes::target_table(sql);
                triggers.push((Trigger::DdlStatement { in_txn, takes_lock: probes::takes_relation_lock(sql) }, target));
                if !in_txn {
                    triggers.push((Trigger::DdlBracketEnd, None));
                }
            } else if probes::is_dml(sql) && !in_txn {
                triggers.push((Trigger::DmlBracketEnd, probes::target_table(sql)));
            }
            if bracket_ddl_end {
                triggers.push((Trigger::DdlBracketEnd, None));
            }
            if bracket_dml_end {
                let target = self.b.session_index(&step.session).and_then(|i| self.b.sessions[i].1.last_target.clone());
                triggers.push((Trigger::DmlBracketEnd, target));
            }
            // Bracket bookkeeping closes after the triggers computed above.
            for rig in [&mut self.a, &mut self.b] {
                if let Some(i) = rig.session_index(&step.session) {
                    let s = &mut rig.sessions[i].1;
                    if probes::txn_control(sql) == Some("end") {
                        s.txn_had_ddl = false;
                        s.txn_had_dml = false;
                        s.last_target = None;
                    }
                }
            }
        }
        if let StepKind::Probe(deck) = &step.kind {
            if let Some(d) = Deck::parse(deck) {
                triggers.push((Trigger::Explicit(d), None));
            }
        }
        if triggers.iter().any(|(t, _)| matches!(t, Trigger::DdlBracketEnd | Trigger::Restart)) {
            // User types may have appeared / oids moved: re-probe the
            // canon contexts before this step's compare.
            self.ctx_stale = true;
        }
        for (t, target) in triggers {
            let (pa, pb) = self.probe_round(&t, Some(&step.session), target.as_deref());
            probes_a.extend(pa);
            probes_b.extend(pb);
        }
        let mut emitted: Vec<ObservationRecord> = Vec::with_capacity(2);
        for (side, obs, log, panic, crash) in records {
            let probes = match side {
                CSide::A => std::mem::take(&mut probes_a),
                CSide::B => std::mem::take(&mut probes_b),
            };
            let rec = self.observation(step.seq, &step.session, side, obs, log, panic, crash, probes);
            sink.observation(&rec);
            emitted.push(rec);
        }
        if let [ra, rb] = emitted.as_slice() {
            self.compare_step(step, ra, rb, sink);
        }
        // The restart deck confirmed both sides again: post-crash ends.
        if !crashed_sides.is_empty() {
            self.a.supervisor.clear_post_crash();
            self.b.supervisor.clear_post_crash();
        }
    }

    /// Stream end: the final probe round (one ObservationRecord per side,
    /// seq = last + 1, session `@end`), then the prefix invariant over
    /// B's whole log file, as findings.
    pub fn finish(&mut self, sink: &mut dyn Sink) -> SiteSummary {
        let seq = self.last_seq + 1;
        let (pa, pb) = self.probe_round(&Trigger::StreamEnd, None, None);
        for (side, probes) in [(CSide::A, pa), (CSide::B, pb)] {
            let liveness = if self.rig(side).supervisor.post_crash() { "post-crash" } else { "ok" };
            let obs = StepObs { wire: Vec::new(), ms: 0, liveness: liveness.into(), hang: None, lost: None };
            let rec = self.observation(seq, "@end", side, obs, Vec::new(), None, None, probes);
            sink.observation(&rec);
        }
        let _ = self.b.poll_logs();
        self.b.tail.parser_mut().finish();
        let violations = logtail::prefix_violations(self.b.tail.parser().prefix(), self.b.tail.parser().lines());
        for v in violations {
            let raw = String::from_utf8_lossy(&v.raw.0).into_owned();
            self.summary.prefix_violations.push((v.line_no, raw.clone()));
            let f = prefix_finding(&self.cfg, &self.scenario, v.line_no, &v.raw);
            sink.finding(&f);
            self.summary.findings += 1;
        }
        for (k, row) in self.summary.invariant_rows.clone() {
            let f = invariant_finding(&self.cfg, &self.scenario, &k, &row);
            sink.finding(&f);
            self.summary.findings += 1;
        }
        for (k, (row, n)) in self.summary.invariant_rig_errors.clone() {
            let f = invariant_rig_error_finding(&self.cfg, &self.scenario, &k, &row, n);
            sink.finding(&f);
            self.summary.findings += 1;
        }
        // Signature dedup tail: one count line per overflowed signature.
        for (sig, n) in &self.sig_counts {
            if *n > SIGNATURE_KEEP {
                sink.note(
                    &Value::obj()
                        .with("signature", Value::from(sig.as_str()))
                        .with("count", Value::from(*n))
                        .with("kept", Value::from(SIGNATURE_KEEP)),
                );
            }
        }
        // Ledger hits this run (the `rulings audit` input).
        let hits: Vec<(String, Value)> =
            self.ledger.hits().into_iter().filter(|(_, n)| *n > 0).map(|(id, n)| (id, Value::from(n))).collect();
        if !hits.is_empty() {
            sink.note(&Value::obj().with("rulings_hits", Value::Obj(hits)));
        }
        self.summary.clone()
    }

    /// The whole stream: every step, then `finish`.
    pub fn run_stream(&mut self, steps: &[StepRecord], sink: &mut dyn Sink) -> SiteSummary {
        for s in steps {
            self.run_step(s, sink);
        }
        self.finish(sink)
    }
}

fn track_txn(slot: &mut SessionSlot, sql: &str) {
    match probes::txn_control(sql) {
        Some("begin") => {
            slot.txn_open = true;
            slot.txn_had_ddl = false;
            slot.txn_had_dml = false;
        }
        Some("end") => {
            slot.txn_open = false;
        }
        _ => {
            if probes::is_ddl(sql) {
                slot.txn_had_ddl = true;
                if let Some(t) = probes::target_table(sql) {
                    slot.last_target = Some(t);
                }
            } else if probes::is_dml(sql) {
                slot.txn_had_dml = true;
                if let Some(t) = probes::target_table(sql) {
                    slot.last_target = Some(t);
                }
            }
        }
    }
}

/// `SET name ...` / `SET LOCAL name ...` → the GUC name (for the
/// `show_guc` deck).
fn set_guc_name(sql: &str) -> Option<String> {
    let mut words = sql.trim_start().split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("SET") {
        return None;
    }
    let mut w = words.next()?;
    if w.eq_ignore_ascii_case("LOCAL") || w.eq_ignore_ascii_case("SESSION") {
        w = words.next()?;
    }
    if w.eq_ignore_ascii_case("TRANSACTION") || w.eq_ignore_ascii_case("CONSTRAINTS") || w.eq_ignore_ascii_case("ROLE") {
        return None;
    }
    let name = w.trim_end_matches(';').split('=').next().unwrap_or("").to_string();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn base_finding(cfg: &SiteConfig, scenario: &str, id: String, plane: &str, class: Class, severity: Severity, signature: String, b: Value) -> Finding {
    Finding {
        id,
        plane: plane.to_string(),
        class,
        severity,
        signature,
        units: Vec::new(),
        recipe: None,
        seed: None,
        cell_id: cfg.cell_id.clone(),
        a: Value::Null,
        b,
        log_marks: vec![scenario.to_string()],
        repro: Repro { cases_sql: "cases.sql".into(), cell_json: "cell.json".into(), spec: None },
        verified: Verified { fresh_runs: 0, independent: None },
        timing: None,
        rule: None,
        status: Status::New,
        audit: AuditFields::default(),
    }
}

/// A prefix-invariant violation as a Finding (plane `log`).
pub fn prefix_finding(cfg: &SiteConfig, scenario: &str, line_no: u64, raw: &Bytes) -> Finding {
    let text = String::from_utf8_lossy(&raw.0).into_owned();
    base_finding(
        cfg,
        scenario,
        format!("log-prefix-{line_no}"),
        "log",
        Class::WrongMessage,
        Severity::Low,
        format!("log | prefix-invariant | {} | line {line_no}", text.chars().take(80).collect::<String>()),
        Value::obj().with("line", Value::from(line_no)).with("raw", raw.to_json()),
    )
}

/// `seed-<n>/cell-<name>` -> n.
pub fn scenario_seed(scenario: &str) -> Option<u64> {
    scenario.strip_prefix("seed-")?.split('/').next()?.parse().ok()
}

/// DataRow cells of a wire as text rows (lossless rendering).
pub fn wire_text_rows(wire: &[WireMsg]) -> Vec<Vec<Option<String>>> {
    wire.iter()
        .filter_map(|m| match m {
            WireMsg::DataRow(cells) => Some(cells.iter().map(|c| c.as_ref().map(|b| canon::bytes_text(&b.0))).collect()),
            _ => None,
        })
        .collect()
}

fn err_tuple_json(t: &canon::ErrTuple) -> Value {
    Value::Obj(t.fields.iter().map(|(c, v)| (c.to_string(), Value::from(v.as_str()))).collect())
}

/// Rows kept in a finding's `a{}`/`b{}` per result group.
const FINDING_ROW_CAP: usize = 20;

/// One side's canonical view of a plane, as the finding's `a{}` / `b{}`:
/// always the differing field and its value, plus the plane's content.
pub fn plane_json(s: &CanonSide, plane: &str, field: &str, value: Option<&str>) -> Value {
    let mut v = Value::obj().with("field", Value::from(field)).with("value", contracts::json::opt(value));
    match plane {
        "wire:E" => v.set("error", s.error.as_ref().map(err_tuple_json).unwrap_or(Value::Null)),
        "wire:N" => {
            v.set("count", Value::from(s.notices.len() as u64));
            v.set("first", s.notices.first().map(err_tuple_json).unwrap_or(Value::Null));
        }
        "rows" | "explain" | "meta" | "tag" | "copy" => {
            let groups = s
                .groups
                .iter()
                .map(|g| {
                    let cols = g.columns.as_ref().map(|c| contracts::json::str_arr(&c.iter().map(|m| m.render()).collect::<Vec<_>>()));
                    let rows: Vec<Value> = g
                        .text_rows()
                        .iter()
                        .take(FINDING_ROW_CAP)
                        .map(|r| Value::Arr(r.iter().map(|c| contracts::json::opt(c.as_deref())).collect()))
                        .collect();
                    Value::obj()
                        .with("columns", cols.unwrap_or(Value::Null))
                        .with("rows", Value::Arr(rows))
                        .with("row_count", Value::from(g.rows.len() as u64))
                        .with("tag", contracts::json::opt(g.tag.as_deref()))
                })
                .collect();
            v.set("groups", Value::Arr(groups));
            if plane == "copy" {
                v.set("copy_out", Bytes(s.copy_out.clone()).to_json());
            }
            if plane == "meta" {
                v.set("params", Value::Obj(s.params.iter().map(|(k, x)| (k.clone(), Value::from(x.as_str()))).collect()));
            }
        }
        "notify" => v.set(
            "notify",
            Value::Arr(
                s.notify
                    .iter()
                    .map(|((ch, p), n)| Value::obj().with("channel", Value::from(ch.as_str())).with("payload", Value::from(p.as_str())).with("count", Value::from(*n)))
                    .collect(),
            ),
        ),
        "log" => v.set("log", contracts::json::str_arr(&s.log.iter().map(|l| l.raw.clone()).collect::<Vec<_>>())),
        "session" => {
            v.set("liveness", Value::from(s.liveness.as_str()));
            v.set("ready", Value::from(s.ready.iter().collect::<String>()));
        }
        "panic" => v.set(
            "panic",
            s.panic
                .as_ref()
                .map(|p| {
                    Value::obj()
                        .with("site", Value::from(p.site.as_str()))
                        .with("message", p.message.to_json())
                        .with("query", p.query.as_ref().map(Bytes::to_json).unwrap_or(Value::Null))
                })
                .unwrap_or(Value::Null),
        ),
        "crash" => v.set(
            "crash",
            s.crash
                .as_ref()
                .map(|c| {
                    Value::obj()
                        .with("side", Value::from(c.side.as_str()))
                        .with("generation", Value::from(c.generation))
                        .with("signal", contracts::json::opt(c.signal.as_deref()))
                        .with("log_tail", Value::Arr(c.log_tail.iter().map(Bytes::to_json).collect()))
                })
                .unwrap_or(Value::Null),
        ),
        "hang" => v.set(
            "hang",
            s.hang
                .as_ref()
                .map(|h| Value::obj().with("ms", Value::from(h.ms)).with("ladder", contracts::json::opt(h.ladder.as_deref())))
                .unwrap_or(Value::Null),
        ),
        p => {
            if p.strip_prefix("probe:").is_some() {
                // The divergence is per statement key: `value` is this
                // side's result for that key (not the whole deck).
                v.set("stmt", Value::from(field));
                v.set("probe", value.and_then(|t| contracts::json::parse(t).ok()).unwrap_or(Value::Null));
            }
        }
    }
    v
}

/// A comparator `Divergence` as a Finding (plan §3.3 "Finding |
/// Ruled(id)"): NEW, or RULED with the ledger id; `units` stay empty
/// until the witness pass; `repro` carries the step's SQL.
pub fn divergence_finding(cfg: &SiteConfig, step: &StepRecord, d: &Divergence, ca: &CanonSide, cb: &CanonSide) -> Finding {
    let sql = step.sql.clone().unwrap_or_default();
    let slug = |s: &str| s.replace(['/', ':'], "-");
    Finding {
        id: format!("{}-s{}-{}", slug(&step.scenario), step.seq, slug(&d.plane)),
        plane: d.plane.clone(),
        class: d.class,
        severity: d.severity,
        signature: d.signature.clone(),
        units: Vec::new(),
        recipe: step.recipe.clone(),
        seed: scenario_seed(&step.scenario),
        cell_id: cfg.cell_id.clone(),
        a: plane_json(ca, &d.plane, &d.field, d.a.as_deref()),
        b: plane_json(cb, &d.plane, &d.field, d.b.as_deref()),
        log_marks: Vec::new(),
        repro: Repro { cases_sql: sql.clone(), cell_json: "cell.json".into(), spec: None },
        verified: Verified { fresh_runs: 0, independent: None },
        timing: None,
        rule: d.rule.clone(),
        status: if d.rule.is_some() { Status::Ruled } else { Status::New },
        audit: AuditFields { repro: Some(sql), ..AuditFields::default() },
    }
}

/// An invariants.sql statement that returned rows on the C oracle: the
/// rig's invariant is wrong (plane `probe:invariants`, one per key, id
/// `invariant-rig-error-<key>`); `a` carries the first row and the count.
pub fn invariant_rig_error_finding(cfg: &SiteConfig, scenario: &str, key: &str, row: &str, rows: u64) -> Finding {
    let mut f = base_finding(
        cfg,
        scenario,
        format!("invariant-rig-error-{key}"),
        "probe:invariants",
        Class::Cosmetic,
        Severity::Low,
        format!("probe:invariants | {key} | - | rig-error"),
        Value::Null,
    );
    f.a = Value::obj()
        .with("stmt", Value::from(key))
        .with("row", Value::from(row))
        .with("rows", Value::from(rows))
        .with("rig_error", Value::from("invariant returned rows on the C oracle; the invariant is wrong, not B"));
    f
}

/// An invariants.sql row on B as a Finding (plane `probe:invariants`).
pub fn invariant_finding(cfg: &SiteConfig, scenario: &str, key: &str, row: &str) -> Finding {
    base_finding(
        cfg,
        scenario,
        format!("invariant-{key}"),
        "probe:invariants",
        Class::WrongResult,
        Severity::High,
        format!("probe:invariants | {key} | - | row"),
        Value::obj().with("stmt", Value::from(key)).with("row", Value::from(row)),
    )
}

// ---------------------------------------------------------------------
// Observer over lane L0.1's client
// ---------------------------------------------------------------------

/// `Observer` over `client::Client` (lane L0.1): every backend message
/// of an exchange is kept verbatim in `wire`; the per-step deadline is
/// the socket read timeout, so a hang surfaces as `Fault::Hang` with the
/// messages received so far retained and prepended to the resumed
/// exchange (`resume` = `drain` to ReadyForQuery after a ladder rung);
/// `Fault::Lost` poisons the observer until `reconnect` re-dials the
/// stored `ConnectOpts`. `backend_pid` is BackendKeyData; `version` the
/// `server_version` ParameterStatus of the handshake.
pub struct ClientObserver {
    opts: ConnectOpts,
    client: Option<Client>,
    dead: Option<String>,
    /// Messages received before a hang, carried into the resumed exchange.
    pending: Vec<WireMsg>,
    pending_ms: u64,
    version: String,
    os_pid: Option<i32>,
}

impl ClientObserver {
    pub fn connect(opts: &ConnectOpts) -> Result<ClientObserver, String> {
        let c = Client::connect_with(opts).map_err(|e: ConnectError| e.detail)?;
        let version = c.parameter("server_version").map(canon::bytes_text).unwrap_or_default();
        Ok(ClientObserver { opts: opts.clone(), client: Some(c), dead: None, pending: Vec::new(), pending_ms: 0, version, os_pid: None })
    }

    /// The OS pid the hang ladder's SIGKILL targets when it differs from
    /// the backend pid (pgrust's thread model: the server process).
    pub fn with_os_pid(mut self, pid: Option<i32>) -> ClientObserver {
        self.os_pid = pid;
        self
    }

    /// A connector closure for `SideRig::new`: one connection per
    /// session, `application_name` = the session name (the `%a` slot of
    /// the log prefix) unless the options already carry one.
    pub fn connector(opts: ConnectOpts, os_pid: Option<i32>) -> Connector {
        Box::new(move |session| {
            let mut o = opts.clone();
            if !o.extra.iter().any(|(k, _)| k == "application_name") {
                o = o.param("application_name", session);
            }
            ClientObserver::connect(&o).map(|obs| Box::new(obs.with_os_pid(os_pid)) as Box<dyn Observer>)
        })
    }

    /// The live client with the deadline armed as its read timeout.
    fn armed(&mut self, deadline_ms: u64) -> Result<&mut Client, Fault> {
        if let Some(d) = &self.dead {
            return Err(Fault::Lost(d.clone()));
        }
        let c = self.client.as_mut().ok_or_else(|| Fault::Lost("not connected".to_string()))?;
        c.set_read_timeout(Some(Duration::from_millis(deadline_ms.max(1)))).map_err(|e| Fault::Lost(format!("set_read_timeout: {e}")))?;
        Ok(c)
    }

    /// Map the client's exchange onto the runner's: pending messages
    /// first; a read deadline is `Hang` (messages kept for `resume`), a
    /// dead connection is `Lost`.
    fn finish(&mut self, ex: WireExchange) -> Result<Exchange, Fault> {
        let mut wire = std::mem::take(&mut self.pending);
        let ms = self.pending_ms + ex.ms;
        self.pending_ms = 0;
        wire.extend(ex.wire);
        match ex.fault {
            None => Ok(Exchange { wire, ms }),
            Some(WireFault::Hang { .. }) => {
                self.pending = wire;
                self.pending_ms = ms;
                Err(Fault::Hang { ms })
            }
            Some(WireFault::Lost(d)) => {
                self.dead = Some(d.clone());
                Err(Fault::Lost(d))
            }
        }
    }
}

/// The frames of an `xproto` step (contracts `XProto`): Parse /
/// [Describe S] / Bind / [Describe P] / Execute, no Sync — `pipeline`
/// appends one, `extended` drives Flush + Sync itself.
pub fn xproto_frames(sql: &str, x: &contracts::XProto) -> Vec<Frame> {
    let params: Vec<WireParam> = x.params.iter().map(|p| WireParam { bytes: p.as_ref().map(|b| b.0.clone()), binary: false }).collect();
    let mut out = vec![Frame::Parse { stmt: x.stmt.clone(), sql: sql.to_string(), param_oids: Vec::new() }];
    if x.describe == "S" {
        out.push(Frame::Describe { kind: Describe::Statement, name: x.stmt.clone() });
    }
    if x.mode == "describe_only" {
        return out;
    }
    out.push(Frame::Bind { portal: x.portal.clone(), stmt: x.stmt.clone(), params, result_fmts: Vec::new() });
    if x.describe == "P" {
        out.push(Frame::Describe { kind: Describe::Portal, name: x.portal.clone() });
    }
    out.push(Frame::Execute { portal: x.portal.clone(), limit: x.limit });
    out
}

/// The `ExtendedStep` of an `xproto` step (modes other than `pipeline`
/// / `describe_only`): a suspended portal is resumed so the rowset is
/// complete.
pub fn xproto_step(sql: &str, x: &contracts::XProto) -> ExtendedStep {
    let mut s = ExtendedStep::new(sql);
    s.stmt = x.stmt.clone();
    s.portal = x.portal.clone();
    s.params = x.params.iter().map(|p| WireParam { bytes: p.as_ref().map(|b| b.0.clone()), binary: false }).collect();
    s.limit = x.limit;
    s.describe = match x.describe.as_str() {
        "S" => Some(Describe::Statement),
        "P" => Some(Describe::Portal),
        _ => None,
    };
    s
}

impl Observer for ClientObserver {
    fn exchange(&mut self, step: &StepRecord, deadline_ms: u64) -> Result<Exchange, Fault> {
        self.pending.clear();
        self.pending_ms = 0;
        let sql = step.sql.clone().unwrap_or_default();
        let ex = match &step.kind {
            StepKind::Sql => self.armed(deadline_ms)?.simple(&sql),
            StepKind::CopyIn => {
                let data = step.slots.get("copy_data").cloned().unwrap_or_default();
                self.armed(deadline_ms)?.simple_with_copy(&sql, Some(data.as_bytes()))
            }
            StepKind::Xproto => {
                let x = step.xproto.clone().unwrap_or(contracts::XProto {
                    mode: "parse_bind_execute".into(),
                    params: Vec::new(),
                    stmt: String::new(),
                    portal: String::new(),
                    limit: 0,
                    describe: "P".into(),
                });
                let c = self.armed(deadline_ms)?;
                match x.mode.as_str() {
                    "pipeline" | "describe_only" => {
                        let mut frames = xproto_frames(&sql, &x);
                        frames.push(Frame::Sync);
                        c.pipeline(&frames, None)
                    }
                    _ => c.extended(&xproto_step(&sql, &x)),
                }
            }
            StepKind::Cancel => {
                let c = self.armed(deadline_ms)?;
                c.cancel().map_err(Fault::Lost)?;
                return Ok(Exchange { wire: Vec::new(), ms: 0 });
            }
            // connect / disconnect are the rig's (SideRig); env / storm /
            // pressure / settle / sleep_until_blocked / probe:<deck> are
            // driven by the runner outside the session exchange.
            _ => return Ok(Exchange { wire: Vec::new(), ms: 0 }),
        };
        self.finish(ex)
    }

    fn resume(&mut self, deadline_ms: u64) -> Result<Exchange, Fault> {
        let ex = self.armed(deadline_ms)?.drain();
        self.finish(ex)
    }

    fn probe(&mut self, sql: &str, deadline_ms: u64) -> Result<Exchange, Fault> {
        self.pending.clear();
        self.pending_ms = 0;
        let ex = self.armed(deadline_ms)?.simple(sql);
        self.finish(ex)
    }

    fn backend_pid(&self) -> Option<u32> {
        self.client.as_ref().map(Client::backend_pid).filter(|p| *p != 0)
    }

    fn os_pid(&self) -> Option<i32> {
        self.os_pid
    }

    fn reconnect(&mut self) -> Result<(), String> {
        if let Some(mut old) = self.client.take() {
            if self.dead.is_none() {
                old.terminate();
            }
        }
        let c = Client::connect_with(&self.opts).map_err(|e| e.detail)?;
        self.version = c.parameter("server_version").map(canon::bytes_text).unwrap_or_default();
        self.client = Some(c);
        self.dead = None;
        self.pending.clear();
        self.pending_ms = 0;
        Ok(())
    }

    fn version(&self) -> String {
        self.version.clone()
    }
}

#[cfg(test)]
mod site_tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    const PREFIX_LINE: &str = "2026-09-02 10:20:41.902 PDT";

    /// Scripted observer: each exchange pops the next result and appends
    /// the paired log text to the side's log file (so the real tailer
    /// path is exercised).
    struct FakeObserver {
        script: Rc<RefCell<VecDeque<(Result<Exchange, Fault>, String)>>>,
        log: PathBuf,
        pid: u32,
        probes: Rc<RefCell<Vec<String>>>,
        reconnects: Rc<RefCell<u32>>,
    }

    impl Observer for FakeObserver {
        fn exchange(&mut self, _step: &StepRecord, _deadline_ms: u64) -> Result<Exchange, Fault> {
            let (r, log) = self.script.borrow_mut().pop_front().unwrap_or((Ok(Exchange { wire: vec![WireMsg::ReadyForQuery { status: 'I' }], ms: 1 }), String::new()));
            if !log.is_empty() {
                use std::io::Write;
                let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&self.log).unwrap();
                f.write_all(log.as_bytes()).unwrap();
            }
            r
        }
        fn resume(&mut self, _deadline_ms: u64) -> Result<Exchange, Fault> {
            let (r, _) = self.script.borrow_mut().pop_front().unwrap_or((Err(Fault::Hang { ms: 5_000 }), String::new()));
            r
        }
        fn probe(&mut self, sql: &str, _deadline_ms: u64) -> Result<Exchange, Fault> {
            self.probes.borrow_mut().push(sql.to_string());
            Ok(Exchange {
                wire: vec![
                    WireMsg::RowDescription(vec![contracts::ColDesc { name: Bytes::text("x"), tableoid: 0, attnum: 0, typoid: 25, typlen: -1, typmod: -1, fmt: 0 }]),
                    WireMsg::DataRow(vec![Some(Bytes::text("v"))]),
                    WireMsg::CommandComplete(Bytes::text("SELECT 1")),
                    WireMsg::ReadyForQuery { status: 'I' },
                ],
                ms: 1,
            })
        }
        fn backend_pid(&self) -> Option<u32> {
            Some(self.pid)
        }
        fn os_pid(&self) -> Option<i32> {
            None
        }
        fn reconnect(&mut self) -> Result<(), String> {
            *self.reconnects.borrow_mut() += 1;
            Ok(())
        }
        fn version(&self) -> String {
            "18.6 (fake)".into()
        }
    }

    struct Harness {
        dir: PathBuf,
        cfg: SiteConfig,
        script_a: Rc<RefCell<VecDeque<(Result<Exchange, Fault>, String)>>>,
        script_b: Rc<RefCell<VecDeque<(Result<Exchange, Fault>, String)>>>,
        probes_a: Rc<RefCell<Vec<String>>>,
        probes_b: Rc<RefCell<Vec<String>>>,
        reconnects: Rc<RefCell<u32>>,
        /// Log text the B connector appends when session `s2` connects
        /// (the auth-phase WARNING of a connect step).
        connect_log: Rc<RefCell<Option<String>>>,
    }

    impl Harness {
        fn new(name: &str, guc_pin: bool) -> Harness {
            let dir = std::env::temp_dir().join(format!("fuzzgen-site-{}-{}", name, std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let mut cell = Cell::base();
            cell.conf_guc_pin = guc_pin;
            let cfg = SiteConfig::from_cell(cell, &dir);
            std::fs::write(&cfg.a_log, format!("{PREFIX_LINE} postmaster[100] LOG:  database system is ready to accept connections\n")).unwrap();
            std::fs::write(&cfg.b_log, format!("{PREFIX_LINE} postmaster[200] LOG:  database system is ready to accept connections\n")).unwrap();
            Harness {
                dir,
                cfg,
                script_a: Rc::new(RefCell::new(VecDeque::new())),
                script_b: Rc::new(RefCell::new(VecDeque::new())),
                probes_a: Rc::new(RefCell::new(Vec::new())),
                probes_b: Rc::new(RefCell::new(Vec::new())),
                reconnects: Rc::new(RefCell::new(0)),
                connect_log: Rc::new(RefCell::new(None)),
            }
        }

        fn runner(&self) -> SiteRunner {
            let connect_log = self.connect_log.clone();
            let mk = |script: &Rc<RefCell<VecDeque<(Result<Exchange, Fault>, String)>>>, log: &PathBuf, pid: u32, probes: &Rc<RefCell<Vec<String>>>, rec: &Rc<RefCell<u32>>| -> Connector {
                let (s, l, p, r, cl) = (script.clone(), log.clone(), probes.clone(), rec.clone(), connect_log.clone());
                Box::new(move |name| {
                    if name == "s2" {
                        if let Some(text) = cl.borrow().as_ref() {
                            use std::io::Write;
                            let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&l).unwrap();
                            f.write_all(text.as_bytes()).unwrap();
                        }
                    }
                    Ok(Box::new(FakeObserver { script: s.clone(), log: l.clone(), pid, probes: p.clone(), reconnects: r.clone() }) as Box<dyn Observer>)
                })
            };
            let a = SideRig::new(CSide::A, &self.cfg, mk(&self.script_a, &self.cfg.a_log, 120, &self.probes_a, &self.reconnects), None);
            let b = SideRig::new(CSide::B, &self.cfg, mk(&self.script_b, &self.cfg.b_log, 19, &self.probes_b, &self.reconnects), None);
            SiteRunner::new(self.cfg.clone(), "seed-1/cell-base", a, b)
        }

        fn push(&self, side: CSide, r: Result<Exchange, Fault>, log: &str) {
            let s = match side {
                CSide::A => &self.script_a,
                CSide::B => &self.script_b,
            };
            s.borrow_mut().push_back((r, log.to_string()));
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn step(seq: u64, sql: &str) -> StepRecord {
        StepRecord {
            scenario: "seed-1/cell-base".into(),
            seq,
            session: "s1".into(),
            role: "superuser".into(),
            kind: StepKind::Sql,
            sql: Some(sql.into()),
            xproto: None,
            productions: Vec::new(),
            targets: Vec::new(),
            ordered: contracts::Ordered::None,
            expect_c: None,
            bracket: None,
            recipe: None,
            mutant: None,
            slots: Default::default(),
        }
    }

    fn fixture_wire(name: &str) -> Vec<WireMsg> {
        let text = match name {
            "error" => include_str!("../fixtures/contracts/observation-error.json"),
            "notice" => include_str!("../fixtures/contracts/observation-notice.json"),
            "crash" => include_str!("../fixtures/contracts/observation-crash.json"),
            _ => panic!("no fixture {name}"),
        };
        ObservationRecord::from_jsonl(&contracts::json::to_canonical(&contracts::json::parse(text).unwrap()).unwrap()).unwrap().wire
    }

    fn ok(wire: Vec<WireMsg>) -> Result<Exchange, Fault> {
        Ok(Exchange { wire, ms: 7 })
    }

    #[test]
    fn emits_step_and_observation_jsonl_from_fixture_wire() {
        let h = Harness::new("emit", true);
        let wire = fixture_wire("error");
        h.push(CSide::A, ok(wire.clone()), &format!("{PREFIX_LINE} client backend[120] [unknown] ERROR:  boom\n{PREFIX_LINE} client backend[120] [unknown] STATEMENT:  SELECT 1/0\n"));
        h.push(CSide::B, ok(wire.clone()), &format!("{PREFIX_LINE} client backend[19] [unknown] ERROR:  boom\n{PREFIX_LINE} client backend[77] [unknown] LOG:  other session\n"));
        let mut r = h.runner();
        let mut sink = VecSink::default();
        let summary = r.run_stream(&[step(1, "SELECT 1/0")], &mut sink);
        assert_eq!(summary.steps, 1);
        assert_eq!(sink.steps.len(), 1);
        let sr = StepRecord::from_jsonl(&sink.steps[0]).unwrap();
        assert_eq!(sr.seq, 1);
        // One observation per side for the step, plus the @end probe record.
        assert_eq!(sink.obs_a.len(), 2);
        assert_eq!(sink.obs_b.len(), 2);
        let oa = ObservationRecord::from_jsonl(&sink.obs_a[0]).unwrap();
        let ob = ObservationRecord::from_jsonl(&sink.obs_b[0]).unwrap();
        assert_eq!(oa.wire, wire);
        assert_eq!(ob.wire, wire);
        assert_eq!(oa.side, CSide::A);
        assert_eq!(ob.side, CSide::B);
        assert_eq!(oa.liveness, "ok");
        assert_eq!(oa.ms, 7);
        assert_eq!(oa.version, "18.6 (fake)");
        assert_eq!(oa.log.len(), 2, "pid-attributed slice: both 120 lines");
        assert_eq!(oa.log[1].level.as_deref(), Some("STATEMENT"));
        assert_eq!(ob.log.len(), 1, "the pid-77 line belongs to another session");
        assert_eq!(ob.log[0].pid, Some(19));
        assert!(ob.panic.is_none() && ob.crash.is_none() && ob.hang.is_none());
        let end = ObservationRecord::from_jsonl(&sink.obs_b[1]).unwrap();
        assert_eq!(end.seq, 2);
        assert_eq!(end.session, "@end");
        assert_eq!(end.probes.keys().cloned().collect::<Vec<_>>(), ["catalog", "invariants", "physical", "stats"]);
        // Pins applied at connect on both sides (guc_pin on).
        assert!(h.probes_a.borrow().iter().any(|s| s.starts_with("SET TimeZone")));
        assert!(h.probes_b.borrow().iter().any(|s| s.starts_with("SET parallel_setup_cost")));
        assert_eq!(summary.prefix_violations.len(), 0);
    }

    #[test]
    fn guc_pin_off_drops_the_pins_and_factory_cell_json_maps() {
        let h = Harness::new("nopin", false);
        let mut r = h.runner();
        let mut sink = VecSink::default();
        r.run_stream(&[step(1, "RESET ALL")], &mut sink);
        assert!(h.probes_a.borrow().iter().all(|s| !s.starts_with("SET ")), "{:?}", h.probes_a.borrow());
        assert!(h.probes_b.borrow().iter().all(|s| !s.starts_with("SET ")));
        // L0.4's cell.json shape.
        let factory = r#"{"b_build":"dev","conf":{"autovacuum":"off","log_min_messages":"debug1"},"conf_a":{},"conf_b":{"max_stack_depth":"60000"},"hba":"trust","host":"linux","initdb":{"checksums":true,"encoding":"UTF8","locale":"C"},"logging":{"collector":true,"log_destination":"stderr,csvlog,jsonlog","log_error_verbosity_a":"verbose","log_line_prefix":"%m %b[%p] %q%a ","log_timezone":"America/New_York"},"name":"elog","oracle":{"ref":"REL_18_6","server_version_num":180006,"sha":"724edf9b","variant":"plain"},"pins":{"guc_pin":"off","io_method":"sync","max_stack_depth":"60000"},"schema":"sitediff-cell/1","server_flags":["-b"],"topology":"single"}"#;
        let cfg = SiteConfig::from_cell_json(factory, Path::new("/w")).unwrap();
        assert!(!cfg.guc_pin);
        assert!(cfg.pin_sql().is_empty());
        assert!(cfg.collector);
        assert!(cfg.raw_oids);
        assert_eq!(cfg.a_log, PathBuf::from("/w/a.log"));
        assert_eq!(cfg.b_log_dir, PathBuf::from("/w/ddb/log"));
        assert_eq!(cfg.cell.logging_min_messages, "debug1");
        assert_eq!(cfg.cell.conf_gucs.get("b:max_stack_depth").map(String::as_str), Some("60000"));
        assert_eq!(cfg.cell.conf_profile.as_deref(), Some("elog"));
        assert_eq!(cfg.deadline_ms, 60_000, "dev x2, debug x1.5");
        // The contracts shape.
        let base = SiteConfig::from_cell_json(include_str!("../fixtures/contracts/cell-base.json"), Path::new("/w")).unwrap();
        assert!(base.guc_pin && !base.collector);
        assert_eq!(base.cell_id, "570fe9f2f4618c9e5b693d955cfe040e02dded5edb2309ddb18a4d7ea2d7b96e");
        assert_eq!(base.pin_sql().len(), c_parity_pin_sql().len());
    }

    #[test]
    fn crash_on_b_restarts_bumps_generation_and_never_ends_the_stream() {
        let h = Harness::new("crash", true);
        let crash_wire = fixture_wire("crash");
        let b_log = format!(
            "thread 'backend-19' panicked at crates/backend/commands/analyze/src/lib.rs:475:17:\n\
             index has 2 expressions but indexprs lists 1\n\
             panicking backend query: ANALYZE t\n\
             {PREFIX_LINE} postmaster[200] LOG:  server process (PID 19) was terminated by signal 6: Abort trap\n\
             {PREFIX_LINE} postmaster[200] LOG:  all server processes terminated; reinitializing\n\
             {PREFIX_LINE} startup[201] LOG:  database system was not properly shut down; automatic recovery in progress\n\
             {PREFIX_LINE} startup[201] LOG:  redo starts at 0/1A2B3C4\n\
             {PREFIX_LINE} postmaster[200] LOG:  database system is ready to accept connections\n"
        );
        h.push(CSide::A, ok(vec![WireMsg::CommandComplete(Bytes::text("ANALYZE")), WireMsg::ReadyForQuery { status: 'I' }]), "");
        h.push(CSide::B, Err(Fault::Lost("EOF".into())), &b_log);
        // Second step: both sides answer.
        h.push(CSide::A, ok(vec![WireMsg::CommandComplete(Bytes::text("SELECT 1")), WireMsg::ReadyForQuery { status: 'I' }]), "");
        h.push(CSide::B, ok(vec![WireMsg::CommandComplete(Bytes::text("SELECT 1")), WireMsg::ReadyForQuery { status: 'I' }]), "");
        let mut r = h.runner();
        let mut sink = VecSink::default();
        let summary = r.run_stream(&[step(118, "ANALYZE t"), step(119, "SELECT 1")], &mut sink);
        assert_eq!(summary.steps, 2, "the stream did not end at the crash");
        assert_eq!(summary.crashes_b, 1);
        assert_eq!(summary.crashes_a, 0);
        assert_eq!(summary.panics, 1);
        let ob = ObservationRecord::from_jsonl(&sink.obs_b[0]).unwrap();
        assert_eq!(ob.liveness, "post-crash");
        let p = ob.panic.as_ref().expect("panic pair");
        assert_eq!(p.site, "crates/backend/commands/analyze/src/lib.rs:475:17");
        assert_eq!(p.query, Some(Bytes::text("ANALYZE t")));
        assert_eq!(p.message, Bytes::text("index has 2 expressions but indexprs lists 1"));
        let c = ob.crash.as_ref().expect("crash record");
        assert_eq!(c.side, CSide::B);
        assert_eq!(c.signal.as_deref(), Some("SIGABRT"));
        assert!(c.log_tail.iter().any(|l| String::from_utf8_lossy(&l.0).contains("reinitializing")));
        assert!(ob.probes.contains_key("catalog"), "restart deck ran: {:?}", ob.probes.keys());
        // The log slice carries the panic block (no pid) and the death lines.
        assert!(ob.log.iter().any(|l| String::from_utf8_lossy(&l.raw.0).starts_with("thread 'backend-19' panicked")));
        // Both pools reconnected symmetrically: s1 on A and B + monitors.
        assert!(*h.reconnects.borrow() >= 2, "{}", h.reconnects.borrow());
        assert_eq!(r.b.supervisor.generation(), 2);
        assert_eq!(r.a.supervisor.generation(), 1);
        assert_eq!(r.b.supervisor.redo_witness(), Some("0/1A2B3C4"));
        // Crash fixture parity: the wire the fixture recorded is what a
        // FATAL-then-death exchange carries; the crash generation matches.
        let fixture = ObservationRecord::from_jsonl(&contracts::json::to_canonical(&contracts::json::parse(include_str!("../fixtures/contracts/observation-crash.json")).unwrap()).unwrap()).unwrap();
        assert_eq!(fixture.crash.as_ref().unwrap().generation, c.generation);
        assert_eq!(fixture.panic, ob.panic);
        assert_eq!(crash_wire.len(), 1);
        // Next step is post-crash on B until the deck cleared it, then ok.
        let ob2 = ObservationRecord::from_jsonl(&sink.obs_b[1]).unwrap();
        assert_eq!(ob2.seq, 119);
        assert_eq!(ob2.liveness, "ok");
        assert!(ob2.crash.is_none());
    }

    #[test]
    fn crash_on_a_is_symmetric() {
        let h = Harness::new("crasha", true);
        let a_log = format!(
            "{PREFIX_LINE} postmaster[100] LOG:  server process (PID 120) was terminated by signal 9: Killed\n\
             {PREFIX_LINE} postmaster[100] LOG:  all server processes terminated; reinitializing\n\
             {PREFIX_LINE} startup[130] LOG:  redo starts at 0/2000028\n\
             {PREFIX_LINE} postmaster[100] LOG:  database system is ready to accept connections\n"
        );
        h.push(CSide::A, Err(Fault::Lost("EOF".into())), &a_log);
        h.push(CSide::B, ok(vec![WireMsg::CommandComplete(Bytes::text("SELECT 1")), WireMsg::ReadyForQuery { status: 'I' }]), "");
        let mut r = h.runner();
        let mut sink = VecSink::default();
        let summary = r.run_stream(&[step(1, "SELECT pg_sleep(100)")], &mut sink);
        assert_eq!(summary.crashes_a, 1);
        let oa = ObservationRecord::from_jsonl(&sink.obs_a[0]).unwrap();
        assert_eq!(oa.liveness, "post-crash");
        assert_eq!(oa.crash.as_ref().unwrap().side, CSide::A);
        assert_eq!(oa.crash.as_ref().unwrap().signal.as_deref(), Some("SIGKILL"));
        assert_eq!(r.a.supervisor.generation(), 2);
        let ob = ObservationRecord::from_jsonl(&sink.obs_b[0]).unwrap();
        assert_eq!(ob.liveness, "ok");
        assert!(ob.crash.is_none());
    }

    #[test]
    fn hang_ladder_records_the_rung_reached() {
        let h = Harness::new("hang", true);
        // A hangs past the deadline; cancel rung completes it.
        h.push(CSide::A, Err(Fault::Hang { ms: 40_000 }), "");
        h.push(CSide::A, ok(vec![WireMsg::CommandComplete(Bytes::text("SELECT 1")), WireMsg::ReadyForQuery { status: 'I' }]), "");
        h.push(CSide::B, ok(vec![WireMsg::CommandComplete(Bytes::text("SELECT 1")), WireMsg::ReadyForQuery { status: 'I' }]), "");
        let mut r = h.runner();
        let mut sink = VecSink::default();
        let summary = r.run_stream(&[step(1, "SELECT pg_sleep(100)")], &mut sink);
        assert_eq!(summary.hangs, 1);
        let oa = ObservationRecord::from_jsonl(&sink.obs_a[0]).unwrap();
        let hang = oa.hang.as_ref().expect("hang recorded");
        assert_eq!(hang.ms, 40_000, "dev cell: 20 s x2");
        assert_eq!(hang.ladder.as_deref(), Some("cancel"));
        assert_eq!(oa.liveness, "ok", "the cancel completed the step");
        assert!(h.probes_a.borrow().iter().any(|s| s == "SELECT pg_cancel_backend(120);"), "{:?}", h.probes_a.borrow());
        assert!(!oa.wire.is_empty());

        // A session that never comes back walks every rung and is dead.
        let h2 = Harness::new("hang2", true);
        h2.push(CSide::A, Err(Fault::Hang { ms: 40_000 }), "");
        h2.push(CSide::A, Err(Fault::Hang { ms: 5_000 }), "");
        h2.push(CSide::A, Err(Fault::Hang { ms: 5_000 }), "");
        h2.push(CSide::A, Err(Fault::Hang { ms: 5_000 }), "");
        h2.push(CSide::B, ok(vec![WireMsg::ReadyForQuery { status: 'I' }]), "");
        let mut r2 = h2.runner();
        let mut sink2 = VecSink::default();
        r2.run_stream(&[step(1, "SELECT 1")], &mut sink2);
        let oa2 = ObservationRecord::from_jsonl(&sink2.obs_a[0]).unwrap();
        assert_eq!(oa2.hang.as_ref().unwrap().ladder.as_deref(), Some("sigkill"));
        assert_eq!(oa2.liveness, "dead");
        let p = h2.probes_a.borrow();
        assert!(p.iter().any(|s| s == "SELECT pg_terminate_backend(120);"), "{p:?}");
    }

    #[test]
    fn probe_scheduling_after_ddl_brackets_and_stream_end() {
        let h = Harness::new("probes", true);
        let mut r = h.runner();
        r.tables.push(("t".into(), "pk".into(), vec!["pk".into(), "b".into()]));
        r.sequences.push("s1".into());
        let mut sink = VecSink::default();
        let steps = vec![
            step(1, "CREATE TABLE t (pk int primary key, b text)"),
            step(2, "BEGIN"),
            step(3, "ALTER TABLE t ADD COLUMN c int"),
            step(4, "INSERT INTO t VALUES (1, 'x', 2)"),
            step(5, "COMMIT"),
            step(6, "SET work_mem = '64kB'"),
            step(7, "SELECT 1"),
        ];
        r.run_stream(&steps, &mut sink);
        let obs: Vec<ObservationRecord> = sink.obs_b.iter().map(|l| ObservationRecord::from_jsonl(l).unwrap()).collect();
        let keys = |i: usize| obs[i].probes.keys().cloned().collect::<Vec<_>>();
        // Standalone DDL: conname (per statement) + the bracket-end decks.
        assert_eq!(keys(0), ["catalog", "conname", "invariants"]);
        assert!(obs[0].probes["conname"].get("conname").is_some());
        // BEGIN: nothing.
        assert!(keys(1).is_empty());
        // In-txn lock-taking DDL: conname + locks, bound to s1.
        assert_eq!(keys(2), ["conname", "locks"]);
        // In-txn DML: nothing yet.
        assert!(keys(3).is_empty());
        // COMMIT closes a bracket with DDL and DML.
        assert_eq!(keys(4), ["catalog", "invariants", "physical", "stats"]);
        // SET is neither; SELECT neither.
        assert!(keys(5).is_empty() && keys(6).is_empty());
        // Stream end deck carries the SET GUC's SHOW and the physical shape
        // of the tracked table; invariants rendered the sequence check.
        let end = &obs[7];
        assert_eq!(end.session, "@end");
        assert!(end.probes["catalog"].get("show_guc:work_mem").is_some());
        assert!(end.probes["invariants"].get("sequence_monotone:s1").is_some());
        // The stats deck reports deltas (fake rows: non-integer cells only).
        assert!(end.probes["stats"].get("user_tables").is_some());
        // Probe SQL that ran on B includes the bound locks probe.
        let p = h.probes_b.borrow();
        assert!(p.iter().any(|s| s.contains("l.pid = pg_backend_pid()")));
        assert!(p.iter().any(|s| s.contains("c.conrelid = 't'::regclass")));
        assert!(p.iter().any(|s| s.contains("pg_column_size(b)")));
        // The fake answers one row to everything on BOTH sides: every
        // invariant statement fails on A too, which is a rig error (one
        // finding per key, never a B finding).
        assert!(r.summary.invariant_rows.is_empty());
        assert!(!r.summary.invariant_rig_errors.is_empty());
        assert!(!sink.findings.is_empty());
        let f = Finding::from_file(&sink.findings[0]).unwrap();
        assert_eq!(f.plane, "probe:invariants");
        assert!(f.id.starts_with("invariant-rig-error-"), "{}", f.id);
        assert!(f.signature.ends_with("| rig-error"), "{}", f.signature);
        assert_eq!(f.status, Status::New);
        let rig: Vec<&String> = sink.findings.iter().filter(|l| l.contains("invariant-rig-error-")).collect();
        assert_eq!(rig.len(), r.summary.invariant_rig_errors.len(), "one finding per key");
        // The stats deck forced a flush on the stream session before the
        // monitor read (plan §4.4), on both sides.
        assert!(h.probes_a.borrow().iter().any(|s| s == STATS_FLUSH_SQL));
        assert!(h.probes_b.borrow().iter().any(|s| s == STATS_FLUSH_SQL));
        // Timing was accounted.
        assert!(r.summary.deck_ms.contains_key("catalog"));
    }

    #[test]
    fn prefix_invariant_over_the_whole_b_file_becomes_a_finding() {
        let h = Harness::new("prefix", true);
        // A boot line before the first mark, written without the prefix.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&h.cfg.b_log).unwrap();
            f.write_all(b"memwatchdog: armed at 512 MiB\n").unwrap();
        }
        h.push(CSide::A, ok(vec![WireMsg::ReadyForQuery { status: 'I' }]), "");
        h.push(CSide::B, ok(vec![WireMsg::ReadyForQuery { status: 'I' }]), &format!("{PREFIX_LINE} client backend[19] [unknown] LOG:  fine\n"));
        let mut r = h.runner();
        let mut sink = VecSink::default();
        let summary = r.run_stream(&[step(1, "SELECT 1")], &mut sink);
        assert_eq!(summary.prefix_violations, vec![(2, "memwatchdog: armed at 512 MiB".to_string())]);
        let findings: Vec<Finding> = sink.findings.iter().filter_map(|l| Finding::from_file(l).ok()).collect();
        let f = findings.iter().find(|f| f.signature.starts_with("log | prefix-invariant")).expect("prefix finding");
        assert_eq!(f.class, Class::WrongMessage);
        assert!(f.signature.starts_with("log | prefix-invariant | memwatchdog"));
        assert_eq!(f.cell_id, h.cfg.cell_id);
        // B logged a line for this step that A did not: the per-pid log
        // plane (on in every cell) reports it as a comparator finding.
        assert!(findings.iter().any(|f| f.plane == "log" && f.b.get("log").is_some()), "{:?}", sink.findings);
    }

    #[test]
    fn connect_step_uses_the_mark_slice_and_disconnect_drops_the_session() {
        let h = Harness::new("connect", true);
        let mut r = h.runner();
        let mut sink = VecSink::default();
        let mut connect = step(1, "");
        connect.kind = StepKind::Connect;
        connect.sql = None;
        connect.session = "s2".into();
        // The auth-phase WARNING is server-log only, no pid known yet:
        // written while the connect is in flight.
        *h.connect_log.borrow_mut() = Some(format!("{PREFIX_LINE} client backend[33] [unknown] WARNING:  auth phase warning\n"));
        let mut disconnect = step(2, "");
        disconnect.kind = StepKind::Disconnect;
        disconnect.sql = None;
        disconnect.session = "s2".into();
        r.run_stream(&[connect, disconnect], &mut sink);
        let ob = ObservationRecord::from_jsonl(&sink.obs_b[0]).unwrap();
        assert_eq!(ob.log.len(), 1);
        assert_eq!(ob.log[0].pid, Some(33));
        assert!(r.b.sessions.iter().all(|(n, _)| n != "s2"));
        assert_eq!(ob.liveness, "ok");
    }

    #[test]
    fn set_guc_name_shapes() {
        assert_eq!(set_guc_name("SET LOCAL work_mem = '1MB'"), Some("work_mem".into()));
        assert_eq!(set_guc_name("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"), None);
        assert_eq!(set_guc_name("SELECT 1"), None);
    }

    /// M0 integration: the comparator runs over each step's two records.
    /// analyze-1 through the whole runner: the notice fixture's wire on
    /// A, the same minus the INFO stream on B -> one NEW wire:N finding
    /// built from the divergence (signature, cell_id, seed, repro SQL,
    /// a{}/b{} planes), and a matching step yields nothing.
    #[test]
    fn comparator_emits_analyze_1_from_fixture_wire() {
        let h = Harness::new("compare", true);
        let notice = fixture_wire("notice");
        let no_notices: Vec<WireMsg> = notice.iter().filter(|m| !matches!(m, WireMsg::NoticeResponse(_))).cloned().collect();
        let error = fixture_wire("error");
        h.push(CSide::A, ok(notice.clone()), "");
        h.push(CSide::B, ok(no_notices), "");
        h.push(CSide::A, ok(error.clone()), "");
        h.push(CSide::B, ok(error), "");
        let mut r = h.runner();
        let mut sink = VecSink::default();
        let mut s1 = step(1, "ANALYZE VERBOSE t;");
        s1.scenario = "seed-42/cell-base".into();
        let summary = r.run_stream(&[s1, step(2, "INSERT INTO t VALUES (NULL, 1);")], &mut sink);
        assert_eq!(summary.divergences, 1, "{:?}", sink.findings);
        assert_eq!(summary.ruled, 0);
        // The fake answers one row to every probe, so the invariants deck
        // adds self-oracle findings at stream end; the comparator's own
        // finding is the wire:N one.
        let wire: Vec<Finding> = sink.findings.iter().filter_map(|l| Finding::from_file(l).ok()).filter(|f| f.plane.starts_with("wire:")).collect();
        assert_eq!(wire.len(), 1);
        let f = &wire[0];
        assert_eq!(f.plane, "wire:N");
        assert_eq!(f.class, Class::WrongMessage);
        assert_eq!(f.status, Status::New);
        assert!(f.rule.is_none());
        assert_eq!(f.signature, "wire:N | analyzing \"%s\" | 00000/- | N.presence");
        assert_eq!(f.cell_id, h.cfg.cell_id);
        assert_eq!(f.seed, Some(42));
        assert_eq!(f.id, "seed-42-cell-base-s1-wire-N");
        assert_eq!(f.repro.cases_sql, "ANALYZE VERBOSE t;");
        assert_eq!(f.audit.repro.as_deref(), Some("ANALYZE VERBOSE t;"));
        assert_eq!(f.a.get("count").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(f.b.get("count").and_then(|v| v.as_i64()), Some(0));
        assert_eq!(f.a.get("first").and_then(|v| v.get("M")).and_then(|v| v.as_str()), Some("analyzing \"public.t\""));
        assert!(f.b.get("first").is_some_and(|v| v.is_null()));
        // The canon contexts were probed once on both monitors (pgdata /
        // libdir / user type names): the fake answers "v" to everything,
        // so nothing path-like or oid-like was adopted.
        assert!(h.probes_a.borrow().iter().any(|s| s == TYPE_NAMES_SQL));
        assert!(h.probes_b.borrow().iter().any(|s| s == TYPE_NAMES_SQL));
        assert!(r.canon_ctx(CSide::A).type_names.is_empty());
        assert_eq!(r.canon_ctx(CSide::B).pgdata.as_deref(), h.cfg.b_pgdata.to_str());
    }

    /// Signature dedup: the first SIGNATURE_KEEP occurrences are written
    /// in full, then one count line at stream end.
    #[test]
    fn signature_dedup_keeps_five_then_counts() {
        let h = Harness::new("dedup", true);
        let notice = fixture_wire("notice");
        let no_notices: Vec<WireMsg> = notice.iter().filter(|m| !matches!(m, WireMsg::NoticeResponse(_))).cloned().collect();
        let n = SIGNATURE_KEEP + 3;
        let steps: Vec<StepRecord> = (1..=n as u64).map(|i| step(i, "ANALYZE VERBOSE t;")).collect();
        for _ in 0..n {
            h.push(CSide::A, ok(notice.clone()), "");
            h.push(CSide::B, ok(no_notices.clone()), "");
        }
        let mut r = h.runner();
        let mut sink = VecSink::default();
        let summary = r.run_stream(&steps, &mut sink);
        assert_eq!(summary.divergences, n);
        let full = sink.findings.iter().filter_map(|l| Finding::from_file(l).ok()).filter(|f| f.plane == "wire:N").count();
        assert_eq!(full as u32, SIGNATURE_KEEP);
        let count_line = sink.findings.iter().find(|l| l.contains("\"kept\"")).expect("count line");
        let v = contracts::json::parse(count_line).unwrap();
        assert_eq!(v.get("count").and_then(|c| c.as_i64()), Some(n as i64));
        assert_eq!(v.get("kept").and_then(|c| c.as_i64()), Some(SIGNATURE_KEEP as i64));
        assert_eq!(v.get("signature").and_then(|c| c.as_str()), Some("wire:N | analyzing \"%s\" | 00000/- | N.presence"));
    }

    /// Cell identity (CONTRACTS.md "Cell identity"): a `sitediff-cell/1`
    /// document from the real factory keeps the factory's id
    /// (`sitediff-cell.sh id`), which is NOT the contracts hash of the
    /// mapped Cell; a carried `cell_id` is verified, a wrong one is a
    /// hard failure; the contracts shape hashes through `Cell::cell_id`.
    #[test]
    fn factory_cell_json_keeps_the_factory_id() {
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../scripts/sitediff-cell.sh");
        let run = |args: &[&str]| -> String {
            let o = std::process::Command::new("bash").arg(&script).args(args).output().expect("bash sitediff-cell.sh");
            assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
            String::from_utf8(o.stdout).unwrap()
        };
        let rendered = run(&["render", "--cell", "base"]);
        let printed_id = run(&["id", "--cell", "base"]).trim().to_string();
        assert_eq!(printed_id.len(), 64);
        let cfg = SiteConfig::from_cell_json(&rendered, Path::new("/w")).unwrap();
        assert_eq!(cfg.cell_id, printed_id, "from_cell_json keeps the factory's own id");
        assert_ne!(cfg.cell.cell_id(), printed_id, "the mapped contracts Cell hashes differently and is not the id");
        assert_eq!(cfg.cell.conf_profile.as_deref(), Some("base"));
        assert!(cfg.guc_pin);
        assert_eq!(cfg.cell.conf_gucs.get("b:io_method").map(String::as_str), Some("sync"));
        assert_eq!(cfg.a_pgdata, PathBuf::from("/w/dda"));
        // A carried cell_id is verified against the canonical form of the
        // rest of the document (the key itself is not hashed).
        let v = contracts::json::parse(&rendered).unwrap();
        let annotated = contracts::json::to_pretty(&v.clone().with("cell_id", Value::from(printed_id.as_str()))).unwrap();
        assert_eq!(SiteConfig::from_cell_json(&annotated, Path::new("/w")).unwrap().cell_id, printed_id);
        let tampered = contracts::json::to_pretty(&v.with("cell_id", Value::from("0".repeat(64).as_str()))).unwrap();
        let err = SiteConfig::from_cell_json(&tampered, Path::new("/w")).unwrap_err();
        assert!(err.contains("carries cell_id"), "{err}");
        // The rig's recorded id (cell.env CELL_ID) is cross-checked too.
        assert!(SiteConfig::from_cell_json_expecting(&rendered, Path::new("/w"), &printed_id).is_ok());
        assert!(SiteConfig::from_cell_json_expecting(&rendered, Path::new("/w"), &cfg.cell.cell_id()).unwrap_err().contains("mismatch"));
        // In-process construction and the contracts shape use the contracts hash.
        assert_eq!(SiteConfig::from_cell(Cell::base(), Path::new("/w")).cell_id, Cell::base().cell_id());
        let contracts_shape = SiteConfig::from_cell_json(&Cell::base().to_file(), Path::new("/w")).unwrap();
        assert_eq!(contracts_shape.cell_id, Cell::base().cell_id());
    }

    /// The xproto step -> client frames mapping.
    #[test]
    fn xproto_step_maps_to_frames() {
        let x = contracts::XProto { mode: "named_portal".into(), params: vec![Some(Bytes::text("2")), None], stmt: "ps1".into(), portal: "p1".into(), limit: 5, describe: "P".into() };
        let s = xproto_step("SELECT $1", &x);
        assert_eq!((s.stmt.as_str(), s.portal.as_str(), s.limit, s.describe), ("ps1", "p1", 5, Some(Describe::Portal)));
        assert_eq!(s.params.len(), 2);
        assert_eq!(s.params[1].bytes, None);
        assert!(s.resume);
        let frames = xproto_frames("SELECT $1", &x);
        assert!(matches!(&frames[0], Frame::Parse { stmt, .. } if stmt == "ps1"));
        assert!(matches!(&frames[1], Frame::Bind { portal, .. } if portal == "p1"));
        assert!(matches!(&frames[2], Frame::Describe { kind: Describe::Portal, .. }));
        assert!(matches!(&frames[3], Frame::Execute { limit: 5, .. }));
        let d = contracts::XProto { mode: "describe_only".into(), params: vec![], stmt: "".into(), portal: "".into(), limit: 0, describe: "S".into() };
        let frames = xproto_frames("SELECT 1", &d);
        assert_eq!(frames.len(), 2);
        assert!(matches!(&frames[1], Frame::Describe { kind: Describe::Statement, .. }));
        assert_eq!(scenario_seed("seed-3878502244648856050/cell-base"), Some(3878502244648856050));
        assert_eq!(scenario_seed("bogus"), None);
    }
}
