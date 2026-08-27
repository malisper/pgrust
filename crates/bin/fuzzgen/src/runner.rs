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
