//! Per-statement outcome capture and divergence classification.
//!
//! Rowsets compare as multisets unless the statement carries ORDER BY
//! (COPY/heap order is ruled non-surface); float-typed columns compare
//! with a ulp tolerance (B1 ruling: float surfaces get ulp comparators,
//! not exact text equality); columns the generator marked as
//! order-sensitive float aggregates compare ruled-soft — any two float
//! values agree, because plan-dependent accumulation order makes their
//! divergence unbounded in ulp terms (crate::agg module docs). Everything
//! else is exact. Ulp-only, soft-only and tie-order-only differences are
//! surfaced as candidates for the ruled table (crate::ruled), not
//! silently swallowed here.

pub const FLOAT4_OID: u32 = 700;
pub const FLOAT8_OID: u32 = 701;

/// Geometric composite types whose text output embeds float8 fields
/// (point/lseg/path/box/polygon/line/circle). Soak-3 N1: CI cluster
/// gcc/glibc build-flag float divergence (ratified B1 surface) lands
/// inside these composites' text, out of reach of the bare-float ulp
/// comparator — they get token-wise ulp comparison instead.
pub const GEO_OIDS: [u32; 7] = [600, 601, 602, 603, 604, 628, 718];

/// Multiplier widening the ulp budget for geometry composites: distance
/// chains (dist_cpoly-style) accumulate up to ~15 ulp on the ratified B1
/// build-flag surface (soak-3 N1), so the geo budget is ulp_tol * 8
/// (default 4 -> 32) rather than the bare-float budget.
pub const GEO_ULP_FACTOR: u64 = 8;

/// What one side produced for one statement.
#[derive(Clone, Debug)]
pub enum StmtOutcome {
    Rows { col_oids: Vec<u32>, rows: Vec<Vec<Option<String>>> },
    Command { tag: String, affected: Option<u64> },
    /// A COPY data transfer: the raw payload of COPY ... TO STDOUT (empty
    /// for COPY FROM STDIN feeds) plus the command tag. X2: with FORMAT
    /// binary the payload is copyto.c's binary emit — compared
    /// byte-for-byte, any byte diff is a finding.
    CopyOut { bytes: Vec<u8>, tag: String },
    Error { sqlstate: String, message: String },
    ConnLost { detail: String },
}

/// Divergence class per charter §3.2. `Ruled` carries the matched ruling id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiffClass {
    Match,
    Ruled(String),
    RowsetDiff,
    ErrorDiff,
    CountDiff,
    /// A runner-injected state probe (`SELECT * FROM t ORDER BY <pk>`)
    /// found the two sides holding different table contents: a silent
    /// state divergence some earlier statement wrote. Carries the table.
    StateDiff(String),
    SessionDiverged(Side),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    A,
    B,
    Both,
}

impl DiffClass {
    /// Stable key for JSONL output and reducer target matching.
    pub fn key(&self) -> &'static str {
        match self {
            DiffClass::Match => "MATCH",
            DiffClass::Ruled(_) => "RULED",
            DiffClass::RowsetDiff => "ROWSET_DIFF",
            DiffClass::ErrorDiff => "ERROR_DIFF",
            DiffClass::CountDiff => "COUNT_DIFF",
            DiffClass::StateDiff(_) => "STATE_DIFF",
            DiffClass::SessionDiverged(_) => "SESSION_DIVERGED",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Classified {
    pub class: DiffClass,
    pub detail: String,
}

/// Per-column compare mode: exact text, float-with-ulp-tolerance, or
/// ruled-soft float (order-sensitive float aggregate — any two float
/// values agree).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ColCmp {
    Exact,
    FloatUlp,
    FloatSoft,
    /// Geometric composite text (point/lseg/path/box/polygon/line/circle):
    /// token-wise compare, numeric fields by ulp under the widened geo
    /// budget (ulp_tol * GEO_ULP_FACTOR), structure exactly.
    GeoUlp,
}

/// C's FirstNormalObjectId: everything at or above it is a user-created
/// object whose OID came off the shared allocator.
const FIRST_NORMAL_OBJECT_ID: u32 = 16384;

/// Column-type-oid equivalence for A-vs-B rowsets. Builtin OIDs (below
/// FirstNormalObjectId) must match exactly. User-range OIDs are NOT
/// comparable by value: user-object OIDs come off each cluster's shared
/// allocator, and on independently-evolving clusters (crashes, voided
/// batches, concurrent DDL over a 24h soak) the per-type deltas diverge
/// arbitrarily (round-7 FP-3: observed [37725,37726,37726] vs
/// [37661,37667,37667]). The earlier consistent-per-resultset-delta
/// invariant only held on freshly-initdb'd clusters, so user-range pairs
/// with user-range unconditionally; user-range against builtin is still
/// a real descriptor divergence.
fn col_oids_equivalent(oa: &[u32], ob: &[u32]) -> bool {
    if oa.len() != ob.len() {
        return false;
    }
    for (&a, &b) in oa.iter().zip(ob) {
        match (a >= FIRST_NORMAL_OBJECT_ID, b >= FIRST_NORMAL_OBJECT_ID) {
            (false, false) => {
                if a != b {
                    return false;
                }
            }
            (true, true) => {}
            _ => return false,
        }
    }
    true
}

/// Compare modes from the result-column type oids plus the generator's
/// soft-column mask (soft applies only where the column really is float —
/// a lying mask never weakens a non-float column).
pub fn col_cmp_modes(col_oids: &[u32], soft_cols: &[usize]) -> Vec<ColCmp> {
    col_oids
        .iter()
        .enumerate()
        .map(|(i, &o)| {
            let is_float = o == FLOAT4_OID || o == FLOAT8_OID;
            if is_float && soft_cols.contains(&i) {
                ColCmp::FloatSoft
            } else if is_float {
                ColCmp::FloatUlp
            } else if GEO_OIDS.contains(&o) {
                ColCmp::GeoUlp
            } else {
                ColCmp::Exact
            }
        })
        .collect()
}

/// How two cells compared: exactly, within float ulp tolerance, under the
/// ruled-soft float-aggregate mode, or not at all.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CellCmp {
    Equal,
    EqualUlp,
    EqualSoft,
    Diff,
}

/// How two rowsets compared under a given order discipline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowsetCmp {
    Equal,
    /// Equal only because float cells matched within ulp tolerance.
    EqualUlp,
    /// Equal only under the ruled-soft float-aggregate column mode.
    EqualSoft,
    Diff(String),
}

fn ulp_distance(a: f64, b: f64) -> u64 {
    // Map to a monotone integer line (negative floats reflected) so ulp
    // distance is a plain absolute difference; the sign boundary is handled
    // by the reflection.
    fn key(x: f64) -> i64 {
        let bits = x.to_bits() as i64;
        // bits < 0 keeps MIN - bits in [MIN + 1, 0]: no overflow.
        if bits < 0 {
            i64::MIN - bits
        } else {
            bits
        }
    }
    let (ka, kb) = (key(a), key(b));
    ka.abs_diff(kb)
}

/// One token of a geometry composite's text form: a structural separator,
/// a numeric field, or any other chunk (compared exactly).
#[derive(Clone, Debug, PartialEq)]
enum GeoTok {
    Sep(char),
    Num(f64),
    Text(String),
}

/// Tokenize geometry composite text (`<(1,2),3>`, `((0,0),(1,1))`,
/// `{1,-2,3e+10}`, ...) into separators and fields. Fields that parse as
/// pg floats (incl. Infinity/NaN) become Num; anything else stays Text.
fn geo_tokens(s: &str) -> Vec<GeoTok> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut flush = |cur: &mut String, out: &mut Vec<GeoTok>| {
        if !cur.is_empty() {
            match parse_pg_float(cur) {
                Some(f) => out.push(GeoTok::Num(f)),
                None => out.push(GeoTok::Text(std::mem::take(cur))),
            }
            cur.clear();
        }
    };
    for c in s.chars() {
        if matches!(c, '(' | ')' | '[' | ']' | '{' | '}' | '<' | '>' | ',' | ' ') {
            flush(&mut cur, &mut out);
            out.push(GeoTok::Sep(c));
        } else {
            cur.push(c);
        }
    }
    flush(&mut cur, &mut out);
    out
}

/// Token-wise geometry compare: structure exact, numeric fields by ulp
/// under the caller-widened budget. NaN==NaN counts as a (tolerant) match
/// only when both sides are NaN in the same field.
fn cmp_geo_text(x: &str, y: &str, geo_tol: u64) -> CellCmp {
    let (ta, tb) = (geo_tokens(x), geo_tokens(y));
    if ta.len() != tb.len() {
        return CellCmp::Diff;
    }
    let mut any_ulp = false;
    for (a, b) in ta.iter().zip(tb.iter()) {
        match (a, b) {
            (GeoTok::Sep(ca), GeoTok::Sep(cb)) if ca == cb => {}
            (GeoTok::Text(sa), GeoTok::Text(sb)) if sa == sb => {}
            (GeoTok::Num(fa), GeoTok::Num(fb)) => {
                if fa.is_nan() || fb.is_nan() {
                    if !(fa.is_nan() && fb.is_nan()) {
                        return CellCmp::Diff;
                    }
                    any_ulp = true;
                } else if fa == fb {
                    // exact numeric match (possibly different text spellings —
                    // still counts tolerant so a formatting divergence with
                    // identical value surfaces as ulp-matched, not silent)
                    // NOTE: identical text never reaches here (fast path).
                    any_ulp = true;
                } else if ulp_distance(*fa, *fb) <= geo_tol {
                    any_ulp = true;
                } else {
                    return CellCmp::Diff;
                }
            }
            _ => return CellCmp::Diff,
        }
    }
    if any_ulp { CellCmp::EqualUlp } else { CellCmp::Equal }
}

fn parse_pg_float(s: &str) -> Option<f64> {
    match s {
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        _ => s.parse().ok(),
    }
}

fn cmp_cell(a: &Option<String>, b: &Option<String>, mode: ColCmp, ulp_tol: u64) -> CellCmp {
    match (a, b) {
        (None, None) => CellCmp::Equal,
        (Some(x), Some(y)) => {
            if x == y {
                return CellCmp::Equal;
            }
            match mode {
                ColCmp::Exact => {}
                ColCmp::FloatUlp => {
                    if let (Some(fx), Some(fy)) = (parse_pg_float(x), parse_pg_float(y)) {
                        if fx.is_nan() && fy.is_nan() {
                            return CellCmp::EqualUlp;
                        }
                        if !fx.is_nan() && !fy.is_nan() && ulp_distance(fx, fy) <= ulp_tol {
                            return CellCmp::EqualUlp;
                        }
                    }
                }
                ColCmp::GeoUlp => {
                    return cmp_geo_text(x, y, ulp_tol.saturating_mul(GEO_ULP_FACTOR));
                }
                ColCmp::FloatSoft => {
                    // Ruled-soft: both sides being float values is enough —
                    // plan-dependent accumulation order makes the divergence
                    // unbounded in ulp terms. NULL vs value stays a diff.
                    if parse_pg_float(x).is_some() && parse_pg_float(y).is_some() {
                        return CellCmp::EqualSoft;
                    }
                }
            }
            CellCmp::Diff
        }
        _ => CellCmp::Diff,
    }
}

fn cmp_row(
    a: &[Option<String>],
    b: &[Option<String>],
    modes: &[ColCmp],
    ulp_tol: u64,
) -> CellCmp {
    if a.len() != b.len() {
        return CellCmp::Diff;
    }
    let mut worst = CellCmp::Equal;
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        match cmp_cell(x, y, modes.get(i).copied().unwrap_or(ColCmp::Exact), ulp_tol) {
            CellCmp::Diff => return CellCmp::Diff,
            CellCmp::EqualSoft => worst = CellCmp::EqualSoft,
            CellCmp::EqualUlp => {
                if worst == CellCmp::Equal {
                    worst = CellCmp::EqualUlp;
                }
            }
            CellCmp::Equal => {}
        }
    }
    worst
}

fn render_row(row: &[Option<String>]) -> String {
    let cells: Vec<String> = row
        .iter()
        .map(|c| match c {
            None => "NULL".to_string(),
            Some(s) => s.clone(),
        })
        .collect();
    cells.join("|")
}

/// Canonical text encoding of a row for multiset sorting: NULL and cell
/// values are separated by bytes that cannot appear in text output.
fn canon_row(row: &[Option<String>]) -> String {
    let mut out = String::new();
    for c in row {
        match c {
            None => out.push('\u{1}'),
            Some(s) => out.push_str(s),
        }
        out.push('\u{1f}');
    }
    out
}

/// Ordered (positional) rowset compare.
pub fn cmp_rows_ordered(
    a: &[Vec<Option<String>>],
    b: &[Vec<Option<String>>],
    modes: &[ColCmp],
    ulp_tol: u64,
) -> RowsetCmp {
    if a.len() != b.len() {
        return RowsetCmp::Diff(format!("row count {} vs {}", a.len(), b.len()));
    }
    let mut worst = RowsetCmp::Equal;
    for (i, (ra, rb)) in a.iter().zip(b.iter()).enumerate() {
        match cmp_row(ra, rb, modes, ulp_tol) {
            CellCmp::Diff => {
                return RowsetCmp::Diff(format!(
                    "row {}: [{}] vs [{}]",
                    i,
                    render_row(ra),
                    render_row(rb)
                ))
            }
            CellCmp::EqualSoft => worst = RowsetCmp::EqualSoft,
            CellCmp::EqualUlp => {
                if worst == RowsetCmp::Equal {
                    worst = RowsetCmp::EqualUlp;
                }
            }
            CellCmp::Equal => {}
        }
    }
    worst
}

/// Multiset rowset compare: exact path sorts canonical encodings; when
/// that fails, an O(n^2) tolerance-aware greedy matching decides whether
/// the remaining differences are float-ulp/ruled-soft only.
pub fn cmp_rows_multiset(
    a: &[Vec<Option<String>>],
    b: &[Vec<Option<String>>],
    modes: &[ColCmp],
    ulp_tol: u64,
) -> RowsetCmp {
    if a.len() != b.len() {
        return RowsetCmp::Diff(format!("row count {} vs {}", a.len(), b.len()));
    }
    let mut ca: Vec<String> = a.iter().map(|r| canon_row(r)).collect();
    let mut cb: Vec<String> = b.iter().map(|r| canon_row(r)).collect();
    ca.sort();
    cb.sort();
    if ca == cb {
        return RowsetCmp::Equal;
    }
    let mut used = vec![false; b.len()];
    let mut any_ulp = false;
    let mut any_soft = false;
    for ra in a {
        let mut matched = false;
        for (j, rb) in b.iter().enumerate() {
            if used[j] {
                continue;
            }
            let c = cmp_row(ra, rb, modes, ulp_tol);
            match c {
                CellCmp::Equal => {
                    used[j] = true;
                    matched = true;
                    break;
                }
                CellCmp::EqualUlp | CellCmp::EqualSoft => {
                    any_ulp = true;
                    any_soft |= c == CellCmp::EqualSoft;
                    used[j] = true;
                    matched = true;
                    break;
                }
                CellCmp::Diff => {}
            }
        }
        if !matched {
            return RowsetCmp::Diff(format!("unmatched row on A: [{}]", render_row(ra)));
        }
    }
    debug_assert!(any_ulp, "exact multiset differed but greedy match used no tolerant cell");
    if any_soft {
        RowsetCmp::EqualSoft
    } else {
        RowsetCmp::EqualUlp
    }
}

/// Parse the affected-row count out of a CommandComplete tag ("UPDATE 3",
/// "INSERT 0 1", "DELETE 0"); None for tags without one ("BEGIN", "SET").
pub fn tag_affected(tag: &str) -> Option<u64> {
    let last = tag.rsplit(' ').next()?;
    if last == tag {
        return None;
    }
    last.parse().ok()
}

/// Statement-level ORDER BY detection over the rendered SQL. Generated
/// statements spell it exactly this way; reduced/replayed scripts do too.
pub fn has_order_by(sql: &str) -> bool {
    sql.contains(" ORDER BY ")
}

/// EXPLAIN statement detection (rendered SQL or replayed scripts).
pub fn is_explain_stmt(sql: &str) -> bool {
    let head = sql.trim_start();
    head.len() >= 7 && head[..7].eq_ignore_ascii_case("EXPLAIN")
}

/// EXPLAIN of DECLARE ... SCROLL CURSOR detection (round-9 RB-10 gate).
/// Looks for a SCROLL option token (not preceded by NO) between DECLARE
/// and the cursor's FOR. Deliberately loose — a cursor merely NAMED
/// "scroll" can gate the candidate in, but the ruling only fires when
/// the diff is exactly C's top-level Materialize wrap, which only a real
/// SCROLL cursor plan produces.
pub fn is_scroll_declare_explain_stmt(sql: &str) -> bool {
    if !is_explain_stmt(sql) {
        return false;
    }
    let lower = sql.to_ascii_lowercase();
    let mut toks = lower
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|t| !t.is_empty());
    // Skip to DECLARE.
    if !toks.any(|t| t == "declare") {
        return false;
    }
    // The token right after DECLARE is the cursor NAME, never an option.
    // SCROLL/NO are unreserved words, so a cursor can be named "no" —
    // `declare no scroll cursor for select` is DECLARE "no" SCROLL (round-14
    // gramwalk seed 4606538290375852325): scanning the name as an option
    // read it as NO SCROLL and missed the ratified Materialize divergence.
    if toks.next().is_none() {
        return false;
    }
    let mut prev = "";
    for t in toks {
        if t == "for" {
            // Options end at CURSOR ... FOR; never scan the query body.
            return false;
        }
        if t == "scroll" && prev != "no" {
            return true;
        }
        prev = t;
    }
    false
}

/// Strip a top-level Materialize wrap from EXPLAIN TEXT rows (one line
/// per single-cell row): drop the "Materialize ..." header row and its
/// 2-space-indented attribute rows (Output/Storage), then de-indent the
/// remaining plan-tree rows by the 6-column "  ->  " marker width.
/// Margin-level summary rows (Planning/Execution Time, Planning:,
/// "  Buffers:" etc.) pass through untouched. Returns None when the
/// rows do not have that exact shape.
fn strip_top_materialize_rows(
    rows: &[Vec<Option<String>>],
) -> Option<Vec<Vec<Option<String>>>> {
    let cell = |r: &Vec<Option<String>>| -> Option<String> {
        if r.len() != 1 {
            return None;
        }
        r[0].clone()
    };
    let first = cell(rows.first()?)?;
    if !first.starts_with("Materialize") {
        return None;
    }
    // Skip the wrap's attribute rows up to the child marker.
    let mut i = 1;
    loop {
        let s = cell(rows.get(i)?)?;
        if s.starts_with("  ->  ") {
            break;
        }
        if !s.starts_with("  ") {
            // Summary line before any child: not the expected shape.
            return None;
        }
        i += 1;
    }
    let mut out = Vec::with_capacity(rows.len() - i);
    let mut in_tree = true;
    for r in &rows[i..] {
        let s = cell(r)?;
        if in_tree && (s.starts_with("      ") || s.starts_with("  ->  ")) {
            out.push(vec![Some(s[6..].to_string())]);
        } else {
            // First margin-level row ends the de-indented plan tree;
            // everything after (summary sections) passes through.
            in_tree = false;
            out.push(vec![Some(s)]);
        }
    }
    Some(out)
}

/// SHOW ALL / pg_settings GUC-inventory statement detection (F4).
/// pgrust deliberately ships a different GUC inventory than C — extra
/// `pgrust.*` rows (docs/design/env-to-guc.md DIVERGENCE NOTICE) and
/// retuned defaults (docs/design/jit-parallel-defaults.md) — so a
/// row-COUNT difference on these statements is config surface, not
/// conformance. Scope is deliberately tight: only SHOW ALL and SELECTs
/// referencing pg_settings qualify; a wrong GUC *value* (equal-count
/// rowset diff, SHOW <guc>, pg_settings value probes) stays a finding.
pub fn is_guc_inventory_stmt(sql: &str) -> bool {
    let head = sql.trim_start();
    if head.len() >= 8 && head[..8].eq_ignore_ascii_case("SHOW ALL") {
        return true;
    }
    (head.len() >= 6 && head[..6].eq_ignore_ascii_case("SELECT"))
        && sql.to_ascii_lowercase().contains("pg_settings")
}

/// The no-libxml oracle's NO_XML_SUPPORT primary message (xml.c:235),
/// verbatim and complete — the detail line rides a separate field.
fn is_no_libxml_error(message: &str) -> bool {
    message == "unsupported XML feature"
}

/// The no-lz4 oracle's NOT-SUPPORTED primary message
/// (toast_compression.c NO_LZ4_SUPPORT), verbatim and complete — same
/// build-config family as is_no_libxml_error: an oracle configured
/// --without-lz4 rejects every lz4 surface pgrust (always lz4-capable)
/// executes natively, so the oracle offers no behavioural signal there.
fn is_no_lz4_error(message: &str) -> bool {
    message == "compression method lz4 not supported"
}

/// pgrust's ratified UTF-8-only server-encoding carve refusal
/// (docs/design/carve-ratifications.md §11, RATIFIED 2026-08-18 by
/// Michael): only UTF8 and SQL_ASCII server encodings are accepted, and
/// both engine gates — createdb.rs `server_encoding_gate` (0A000 at
/// CREATE DATABASE) and postinit `check_database_encoding_supported`
/// (FATAL 0A000 at connect) — cite the carve doc verbatim in their
/// message. Signature scope is the citation suffix, unique to those two
/// gates; any OTHER encoding-related error (including pgrust bugs
/// raising different text) does not qualify and still escalates.
fn is_encoding_carve_refusal(sqlstate: &str, message: &str) -> bool {
    sqlstate == "0A000"
        && message.ends_with(
            "is not supported by pgrust; only \"UTF8\" and \"SQL_ASCII\" server \
             encodings are accepted (UTF-8-only carve, \
             docs/design/carve-ratifications.md)",
        )
}

/// Instance-config introspection views (round-9 FP-9): these views
/// reflect the INSTANCE — config files on disk (pg_hba_file_rules,
/// pg_ident_file_mappings, pg_file_settings) or the shared-memory layout
/// (pg_shmem_allocations family) — not the schema, so two independently
/// provisioned clusters legitimately differ (the Antithesis pair runs
/// different pg_hba.conf files: row count 6 vs 7 in run
/// b627b97fb4ea57851b123676de30f2ab-59-13).
///
/// Round-9 FP-9b extends the family to live-state views: the
/// pg_stat_progress_* views and pg_stat_activity report OTHER sessions'
/// in-flight commands, so a concurrent driver's ANALYZE on one cluster
/// legitimately appears on that side only (`SELECT count(*) FROM
/// pg_stat_progress_analyze` returned A=[0] with an unmatched B row,
/// same run). The generators no longer project any of this content
/// (existence shapes only); this predicate is the defensive net for
/// gramwalk-derived references. Error outcomes on these statements
/// still compare strictly.
///
/// Round-10 FP-10 extends the live-state family with pg_locks: the lock
/// table is cluster-global live instance state exactly like
/// pg_stat_activity (a concurrent driver batch's ungranted lock showed
/// up on one side only: `SELECT count(*) FROM pg_locks WHERE NOT
/// granted;` → unmatched B row, run f13de995...-59-13, seeds
/// 1341620565954279792 / 3898244892804067832). Of the sibling live
/// views only pg_locks is emitted by the curated pools (util sysview /
/// lockcursor / adtmisc / obs); pg_prepared_statements is SESSION-local
/// and deterministic (plancache probes compare it deliberately), and
/// pg_cursors / pg_prepared_xacts are never emitted, so none of them
/// belongs here.
pub fn is_instance_config_stmt(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    [
        "pg_hba_file_rules",
        "pg_ident_file_mappings",
        "pg_file_settings",
        "pg_shmem_allocations",
        // FP-9b: whole pg_stat_progress_* family by prefix.
        "pg_stat_progress_",
        "pg_stat_activity",
        // FP-10 (round-10): cluster-global live lock state.
        "pg_locks",
        // FP-13 (round-13, run 72b2e74701d0e1310d59d39345e716da-59-13,
        // seed 4146339408691531505): the pg_stat_get_backend_*(integer)
        // family addresses live backend slots by BACKEND ID — an
        // argument that does not name the caller's own backend (the old
        // adtmisc probe passed pg_backend_pid(), a PID) reads whichever
        // session happens to occupy that slot, a function of each
        // cluster's live backend population exactly like
        // pg_stat_activity. The curated pools now self-address through
        // pg_stat_get_backend_idset(); this entry is the defensive net
        // for gramwalk-derived raw calls.
        "pg_stat_get_backend_",
    ]
    .iter()
    .any(|v| lower.contains(v))
}

/// Backup-control functions whose results are cluster-local WAL
/// positions / restore-point acks (round-9 covdiff): pg_backup_start
/// returns the instance's current LSN, which is never cross-cluster
/// comparable. The generators project these (`IS NOT NULL`); this
/// predicate scopes the defensive `instance-lsn` ruled class for
/// grammar-derived raw calls.
pub fn calls_backup_control(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    ["pg_backup_start", "pg_backup_stop", "pg_switch_wal", "pg_create_restore_point"]
        .iter()
        .any(|f| lower.contains(f))
}

/// Shared-catalog DDL: the only statement surface where two CONCURRENT
/// driver instances (each in its own private per-batch database) still
/// race each other — pg_authid / pg_database / pg_shdescription rows are
/// cluster-global. Scope is deliberately tight; database-local DDL never
/// qualifies.
pub fn is_shared_catalog_stmt(sql: &str) -> bool {
    let head = sql.trim_start();
    for kw in [
        "ALTER USER ",
        "ALTER ROLE ",
        "ALTER GROUP ",
        "ALTER DATABASE ",
        "ALTER TABLESPACE ",
        "COMMENT ON DATABASE ",
        "COMMENT ON ROLE ",
        "COMMENT ON TABLESPACE ",
        // Round-14 (seed 1777368629953620144): CREATE/DROP ROLE and DROP
        // OWNED touch pg_authid / pg_auth_members / pg_shdepend — shared
        // catalogs. C's DropRole membership/shdep cleanup goes through
        // CatalogTupleDelete -> simple_heap_delete and raises the identical
        // XX000 "tuple concurrently deleted" when two sessions race the
        // same rows, so the hazard is symmetric (the deck rebase removes
        // the deck-role race; this covers grammar-derived stragglers).
        // Only the tuple-concurrency XX000 messages are ruled — any other
        // XX000 on these statements still escalates.
        "CREATE ROLE ",
        "CREATE USER ",
        "CREATE GROUP ",
        "DROP ROLE ",
        "DROP USER ",
        "DROP GROUP ",
        "DROP OWNED ",
    ] {
        if head.len() >= kw.len() && head[..kw.len()].eq_ignore_ascii_case(kw) {
            return true;
        }
    }
    // Round-18 soak (seed 1550471152172160129): GRANT/REVOKE ... ON
    // PARAMETER writes pg_parameter_acl — a BKI_SHARED_RELATION, so
    // per-batch private databases do not isolate concurrent drivers. C's
    // ExecGrant_Parameter (aclchk.c:2432/2453/2526/2540) takes only
    // RowExclusiveLock on the catalog, operates on a syscache tuple, and
    // goes through CatalogTupleUpdate/Delete -> simple_heap_update/delete,
    // which raise the identical XX000 tcu messages under the same race —
    // there is NO LockSharedObject in this path (unlike role membership,
    // user.c:1703), so the hazard is symmetric and pgrust's port is
    // faithful. A bare GRANT/REVOKE prefix would be far too broad
    // (table/schema grants are database-local): the shape requires the
    // GRANT/REVOKE head keyword AND the "ON PARAMETER" object clause,
    // the only grammar route to OBJECT_PARAMETER_ACL. The
    // tcu-messages-only gate is untouched — any other XX000 on these
    // statements still escalates.
    let is_kw = |kw: &str| {
        head.len() > kw.len()
            && head[..kw.len()].eq_ignore_ascii_case(kw)
            && !head.as_bytes()[kw.len()].is_ascii_alphanumeric()
    };
    if is_kw("GRANT") || is_kw("REVOKE") {
        let lower = sql.to_ascii_lowercase();
        let mut tokens = lower.split_whitespace().peekable();
        while let Some(t) = tokens.next() {
            if t == "on" && tokens.peek() == Some(&"parameter") {
                return true;
            }
        }
    }
    false
}

/// ALTER SYSTEM statements: the gramwalk surface that read-modify-writes
/// INSTANCE-global state (postgresql.auto.conf is one file per cluster,
/// shared by every concurrent driver batch). Scope for the RB-14
/// autoconf-shared-race ruling only — see is_autoconf_parse_error.
/// `DROP TABLE ...` head, for the round-20 autovacuum-deadlock ruling.
pub fn is_drop_table_stmt(sql: &str) -> bool {
    let head = sql.trim_start();
    if head.len() < 4 || !head[..4].eq_ignore_ascii_case("DROP") {
        return false;
    }
    let rest = head[4..].trim_start();
    rest.len() >= 5 && rest[..5].eq_ignore_ascii_case("TABLE")
}

pub fn is_alter_system_stmt(sql: &str) -> bool {
    let head = sql.trim_start();
    if head.len() < 5 || !head[..5].eq_ignore_ascii_case("ALTER") {
        return false;
    }
    let rest = head[5..].trim_start();
    rest.len() >= 6 && rest[..6].eq_ignore_ascii_case("SYSTEM")
}

/// AlterSystemSetConfigFile's re-parse failure (guc.c:4734,
/// ERRCODE_CONFIG_FILE_ERROR): raised when the CURRENT contents of
/// postgresql.auto.conf do not lex as `name = value` lines. Verbatim in
/// both engines. Under concurrent driver batches this fires on whichever
/// side a racing batch's accepted multi-dot custom-GUC write (a C 18
/// self-poisoning behavior pgrust replicates bug-for-bug: the config-file
/// lexer's QUALIFIED_ID is two components while valid_custom_variable_name
/// accepts 2+, so `a.b.c` writes an entry neither engine can re-read)
/// landed first — timing, not conformance (RB-14, round-10: the local A/B
/// sweep found zero per-statement asymmetry across the whole
/// name-validation/serialization/re-parse matrix).
fn is_autoconf_parse_error(sqlstate: &str, message: &str) -> bool {
    sqlstate == "F0000"
        && message == "could not parse contents of file \"postgresql.auto.conf\""
}

/// C's simple_heap_update / CatalogTupleUpdate concurrency error
/// (ERRCODE_INTERNAL_ERROR, heapam.c "tuple concurrently updated" /
/// "tuple concurrently deleted"): both engines raise it verbatim when two
/// sessions race an update to the same catalog row, so on shared-catalog
/// DDL under concurrent drivers it is timing, not conformance. Any other
/// XX000 stays panic-class and escalates.
fn is_tuple_concurrency_error(sqlstate: &str, message: &str) -> bool {
    sqlstate == "XX000"
        && (message == "tuple concurrently updated" || message == "tuple concurrently deleted")
}

fn int_cell(c: &Option<String>) -> bool {
    matches!(c, Some(s) if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
}

/// Ruled candidate for the GUC-inventory signature: a row-count diff on a
/// SHOW ALL / pg_settings statement, or the `count(*)` rendering of the
/// same inventory (both sides a single all-digit cell). Anything else on
/// these statements — same-count rowsets with differing values — is NOT
/// a candidate and escalates normally.
fn guc_inventory_candidate(
    sql: &str,
    ra: &[Vec<Option<String>>],
    rb: &[Vec<Option<String>>],
) -> Option<Classified> {
    if !is_guc_inventory_stmt(sql) {
        return None;
    }
    if ra.len() != rb.len() {
        return Some(Classified {
            class: DiffClass::Ruled("guc-inventory".to_string()),
            detail: format!("GUC-inventory row counts differ: {} vs {}", ra.len(), rb.len()),
        });
    }
    if sql.to_ascii_lowercase().contains("count")
        && ra.len() == 1
        && ra[0].len() == 1
        && rb.len() == 1
        && rb[0].len() == 1
        && int_cell(&ra[0][0])
        && int_cell(&rb[0][0])
    {
        return Some(Classified {
            class: DiffClass::Ruled("guc-inventory".to_string()),
            detail: format!(
                "GUC-inventory counts differ: {:?} vs {:?}",
                ra[0][0], rb[0][0]
            ),
        });
    }
    None
}

/// Runtime resource counters inside EXPLAIN ANALYZE output whose values
/// (and value-adjacent text, e.g. quicksort vs external merge) are
/// implementation state, not planner conformance: masked before the
/// EXPLAIN rowset compare. Cost/row *estimates* are never printed at all
/// (COSTS OFF always); "actual rows=" stays compared — under matching
/// plans it is a real conformance signal. Longest-first: prefixes such as
/// "Memory" vs "Memory Usage" must try the longer token first.
const EXPLAIN_COUNTER_TOKENS: &[&str] = &[
    // Buffer-usage counters (G2, show_buffer_usage): in TEXT format the
    // whole "Buffers:" tail is masked (WHICH counters print depends on
    // which are nonzero — implementation state), so line PRESENCE and
    // indentation stay the compared surface; JSON/YAML print every block
    // counter unconditionally under per-node keys, masked value-wise.
    // "I/O Timings"/"I/O * Time" only appear under track_io_timing
    // (observability profile) and are wall-clock.
    "Shared Hit Blocks",
    "Shared Read Blocks",
    "Shared Dirtied Blocks",
    "Shared Written Blocks",
    "Local Hit Blocks",
    "Local Read Blocks",
    "Local Dirtied Blocks",
    "Local Written Blocks",
    "Temp Read Blocks",
    "Temp Written Blocks",
    "Shared I/O Read Time",
    "Shared I/O Write Time",
    "Local I/O Read Time",
    "Local I/O Write Time",
    "Temp I/O Read Time",
    "Temp I/O Write Time",
    "I/O Timings",
    "I/O Read Time",
    "I/O Write Time",
    "Buffers",
    "Average Sort Space Used",
    "Peak Sort Space Used",
    "Original Hash Buckets",
    "Original Hash Batches",
    "Peak Memory Usage",
    "Sort Space Used",
    "Sort Space Type",
    "Average Memory",
    "Memory Usage",
    "Sort Methods",
    "Sort Method",
    "Peak Memory",
    // Tuplestore lines (WindowAgg/Materialize/CTE): `Storage: Memory
    // Maximum Storage: NNkB` — the high-water mark is allocator state
    // (U1-F2). "Maximum Storage" must precede "Storage" so the longer
    // token wins.
    "Maximum Storage",
    "Storage",
    "Hash Buckets",
    "Hash Batches",
    "Disk Usage",
    "Buckets",
    "Batches",
    // WAL usage (LD4, show_wal_usage): record/fpi/byte counts are storage
    // state (page fullness, checkpoint history decides fpi). JSON/YAML
    // print every key unconditionally, masked value-wise; the TEXT "WAL:"
    // line prints only the nonzero counters, so it masks to end-of-line
    // like "Buffers:" (the exd module additionally keeps TEXT WAL out of
    // differential legs).
    "WAL Records",
    "WAL FPI",
    "WAL Bytes",
    "WAL Buffers Full",
    "WAL",
    // Planning/execution summary (LD4, SUMMARY ON arms): wall clock.
    "Planning Time",
    "Execution Time",
    // Planner memory (LD4, MEMORY option): allocator state. The TEXT line
    // is "Memory: used=NkB  allocated=NkB" — both pairs are one value
    // tail, so bare "Memory" masks to end-of-line (its other TEXT
    // appearances, "Sort Method: .."/"Buffers: ..", are already inside
    // end-of-line masks; JSON/YAML use the longer keys below).
    "Memory Used",
    "Memory Allocated",
    // HashAgg planner-estimate partition count (memory-model state).
    "Planned Partitions",
    "Memory",
    "Disk",
];

/// Mask counter values in one EXPLAIN output cell: after `<token>:` (or
/// `<token>":` in JSON), everything up to the next value delimiter
/// (comma, newline, double space, or end) becomes `X`. Applied to BOTH
/// sides identically, so masked-equal is well-defined; TEXT, JSON and
/// YAML forms all terminate their values at one of these delimiters.
pub fn normalize_explain_cell(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    'outer: while i < bytes.len() {
        for tok in EXPLAIN_COUNTER_TOKENS {
            let t = tok.as_bytes();
            if bytes[i..].starts_with(t) {
                // Word boundary on the left (start, or non-alphanumeric).
                let left_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                // Separator: optional closing quote (JSON key), then ':',
                // then optional spaces.
                let mut j = i + t.len();
                if j < bytes.len() && bytes[j] == b'"' {
                    j += 1;
                }
                if left_ok && j < bytes.len() && bytes[j] == b':' {
                    j += 1;
                    while j < bytes.len() && bytes[j] == b' ' {
                        j += 1;
                    }
                    // Copy token + separator, mask the value. "Sort
                    // Method" masks to end-of-line: in TEXT format the
                    // method decides which counter label follows on the
                    // same line (quicksort -> Memory, external merge ->
                    // Disk), so the whole tail is implementation state.
                    // Likewise "Buffers"/"I/O Timings" TEXT lines: which
                    // name=NN pairs print depends on which counters are
                    // nonzero.
                    let to_eol = tok.starts_with("Sort Method")
                        || *tok == "Buffers"
                        || *tok == "I/O Timings"
                        || *tok == "WAL"
                        || *tok == "Memory";
                    out.push_str(&s[i..j]);
                    out.push('X');
                    while j < bytes.len() {
                        if bytes[j] == b'\n' {
                            break;
                        }
                        if !to_eol {
                            if bytes[j] == b',' {
                                break;
                            }
                            if bytes[j] == b' '
                                && j + 1 < bytes.len()
                                && bytes[j + 1] == b' '
                            {
                                break;
                            }
                        }
                        j += 1;
                    }
                    i = j;
                    continue 'outer;
                }
            }
        }
        let c = s[i..].chars().next().unwrap();
        out.push(c);
        i += c.len_utf8();
    }
    out
}

/// Opt-in H1 mask: wall-clock digits after "actual time=" in EXPLAIN
/// ANALYZE node lines. Deliberately NOT part of EXPLAIN_COUNTER_TOKENS —
/// the explain module's ANALYZE arms always carry TIMING OFF so the text
/// never prints there, and keeping it compared by default preserves that
/// hygiene as a checked invariant. Only grammar-derived EXPLAIN ANALYZE
/// (gramwalk, or --mask-explain-timing replays) opts in, and only when
/// the remaining diff disappears under the mask ("actual rows=" and plan
/// structure still compare strictly).
pub fn normalize_explain_timing_cell(s: &str) -> String {
    const TOK: &str = "actual time=";
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find(TOK) {
        out.push_str(&rest[..pos + TOK.len()]);
        out.push('X');
        let after = &rest[pos + TOK.len()..];
        let end = after
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(after.len());
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

fn normalize_explain_timing_rows(
    rows: &[Vec<Option<String>>],
) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|r| {
            r.iter()
                .map(|c| c.as_ref().map(|s| normalize_explain_timing_cell(s)))
                .collect()
        })
        .collect()
}

fn normalize_explain_rows(rows: &[Vec<Option<String>>]) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|r| {
            r.iter()
                .map(|c| c.as_ref().map(|s| normalize_explain_cell(s)))
                .collect()
        })
        .collect()
}

/// EXPLAIN TEXT "Planning:" buffer-usage block: whether the section prints
/// AT ALL depends on whether the planner touched any buffer, which is
/// session cache state, not conformance — the SAME C server prints it on a
/// cold backend (catalog reads miss the syscache) and omits it once the
/// session has planned the relation before (18.3 explain.c ExplainOnePlan:
/// `if (peek_buffer_usage(es, bufusage) || mem_counters)` gates the whole
/// group in TEXT). pgrust's thread-shared catalog caches reach the warm
/// state on different session histories than C's per-backend forks, so the
/// block's PRESENCE (r20 run 4ab3382e87..-59-13 seed 3878502244648856050:
/// "row count 8 vs 6") is implementation state exactly like the counter
/// values inside it. Strip the header row plus its more-indented counter
/// children ("Buffers:", "I/O Timings:", "Memory:") from TEXT rowsets;
/// JSON/YAML need no strip (peek_buffer_usage returns true for non-text
/// formats whenever BUFFERS is on, so group presence is deterministic and
/// the per-key values are already masked). Plan structure, node-level
/// Buffers presence, and "Planning Time" still compare strictly.
pub fn strip_planning_buffer_rows(rows: &[Vec<Option<String>>]) -> Vec<Vec<Option<String>>> {
    let indent_of = |s: &str| s.len() - s.trim_start_matches(' ').len();
    let mut out: Vec<Vec<Option<String>>> = Vec::with_capacity(rows.len());
    let mut i = 0;
    while i < rows.len() {
        let header = rows[i].len() == 1
            && rows[i][0].as_deref().is_some_and(|s| s.trim() == "Planning:");
        if !header {
            out.push(rows[i].clone());
            i += 1;
            continue;
        }
        let hdr_indent = indent_of(rows[i][0].as_deref().unwrap());
        i += 1;
        while i < rows.len() {
            let child = rows[i].len() == 1
                && rows[i][0].as_deref().is_some_and(|s| {
                    indent_of(s) > hdr_indent
                        && [
                            "Buffers:",
                            "I/O Timings:",
                            "Memory:",
                        ]
                        .iter()
                        .any(|p| s.trim_start().starts_with(p))
                });
            if !child {
                break;
            }
            i += 1;
        }
    }
    out
}

/// FP-2 (round-7): deparse text embeds user-object OIDs as literals —
/// `pg_get_partition_constraintdef` renders
/// `satisfies_hash_partition('48594'::oid, ...)`, and user-range OIDs are
/// arbitrary across two independently-evolving clusters. Normalize every
/// `'<digits>'::oid` token whose value is >= FirstNormalObjectId to
/// `'<oid>'::oid`; builtin OID literals stay compared exactly. Returns
/// None when nothing changed (so the caller can skip the re-compare).
pub fn normalize_oid_literal_cell(s: &str) -> Option<String> {
    const SUFFIX: &str = "'::oid";
    if !s.contains(SUFFIX) {
        return None;
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    let mut changed = false;
    while let Some(pos) = rest.find(SUFFIX) {
        // Walk back over the digits to the opening quote.
        let head = &rest[..pos];
        let digits_start = head
            .rfind(|c: char| !c.is_ascii_digit())
            .map(|i| i + 1)
            .unwrap_or(0);
        let digits = &head[digits_start..];
        let is_user_oid = digits_start > 0
            && head[..digits_start].ends_with('\'')
            && !digits.is_empty()
            && digits.parse::<u64>().is_ok_and(|v| v >= u64::from(FIRST_NORMAL_OBJECT_ID));
        if is_user_oid {
            out.push_str(&head[..digits_start]);
            out.push_str("<oid>");
            changed = true;
        } else {
            out.push_str(head);
        }
        out.push_str(SUFFIX);
        rest = &rest[pos + SUFFIX.len()..];
    }
    out.push_str(rest);
    changed.then_some(out)
}

/// Round-8: TOAST relation names embed the owning table's OID
/// (`pg_toast.pg_toast_48594`, index `pg_toast_48594_index`) — the same
/// user-object-OID-drift family as the round-7 FP classes, surfacing
/// through reltoastrelid joins, pg_class scans, and deparse output.
/// Normalize `pg_toast_<digits>` to `pg_toast_<oid>` when the digits are
/// a user-range OID; catalog toast tables (`pg_toast_2619`, ...) carry
/// stable builtin relids and still compare exactly. Returns None when
/// nothing changed.
pub fn normalize_toast_name_cell(s: &str) -> Option<String> {
    const PREFIX: &str = "pg_toast_";
    if !s.contains(PREFIX) {
        return None;
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    let mut changed = false;
    while let Some(pos) = rest.find(PREFIX) {
        let after = &rest[pos + PREFIX.len()..];
        let dig_end = after
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after.len());
        let digits = &after[..dig_end];
        // Word boundary on the left: `x_pg_toast_9` is not a toast name.
        let left_ok = {
            let head = &rest[..pos];
            head.is_empty()
                || !head
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        };
        out.push_str(&rest[..pos + PREFIX.len()]);
        if left_ok
            && !digits.is_empty()
            && digits
                .parse::<u64>()
                .is_ok_and(|v| v >= u64::from(FIRST_NORMAL_OBJECT_ID))
        {
            out.push_str("<oid>");
            changed = true;
        } else {
            out.push_str(digits);
        }
        rest = &after[dig_end..];
    }
    out.push_str(rest);
    changed.then_some(out)
}

fn normalize_toast_name_rows(rows: &[Vec<Option<String>>]) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|r| {
            r.iter()
                .map(|c| {
                    c.as_ref()
                        .map(|s| normalize_toast_name_cell(s).unwrap_or_else(|| s.clone()))
                })
                .collect()
        })
        .collect()
}

fn normalize_oid_literal_rows(rows: &[Vec<Option<String>>]) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|r| {
            r.iter()
                .map(|c| {
                    c.as_ref()
                        .map(|s| normalize_oid_literal_cell(s).unwrap_or_else(|| s.clone()))
                })
                .collect()
        })
        .collect()
}

// ---------------------------------------------------------------------
// FP-5 (round-7): binary-mode cells for non-decoded types compare as raw
// `\x` hex, but record_send embeds each column's type OID and array_send
// embeds the element type OID — user-range values differ across clusters,
// so every composite/array-of-UDT binary image diverges at those bytes.
// The maskers below structurally parse the two container wire formats and
// replace embedded user-range OIDs with FirstNormalObjectId; parsing must
// consume the image EXACTLY (every length walk lands on the end) or the
// cell is left untouched. Nested containers are not recursed into (the
// sampled hits are all top-level; a nested divergence stays a finding).
// ---------------------------------------------------------------------

fn be_i32(b: &[u8], at: usize) -> Option<i32> {
    Some(i32::from_be_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

fn mask_user_oid(b: &mut [u8], at: usize) -> bool {
    let oid = u32::from_be_bytes(b[at..at + 4].try_into().unwrap());
    if oid >= FIRST_NORMAL_OBJECT_ID {
        b[at..at + 4].copy_from_slice(&FIRST_NORMAL_OBJECT_ID.to_be_bytes());
        true
    } else {
        false
    }
}

/// array_send image: ndim(i32) flags(i32) elemtype(u32), per-dim
/// (dim,lbound), then per-element len(i32)+data (-1 = NULL). Masks the
/// elemtype when user-range; returns whether anything changed.
fn mask_array_image(b: &mut [u8]) -> Option<bool> {
    let ndim = be_i32(b, 0)?;
    if !(0..=6).contains(&ndim) {
        return None;
    }
    let flags = be_i32(b, 4)?;
    if flags != 0 && flags != 1 {
        return None;
    }
    let mut at = 12usize;
    let mut nitems: u64 = 1;
    for _ in 0..ndim {
        let dim = be_i32(b, at)?;
        be_i32(b, at + 4)?; // lbound: any value is wire-legal
        if dim < 0 {
            return None;
        }
        nitems = nitems.checked_mul(dim as u64)?;
        at += 8;
    }
    if ndim == 0 {
        nitems = 0;
    }
    for _ in 0..nitems {
        let len = be_i32(b, at)?;
        at += 4;
        if len >= 0 {
            at = at.checked_add(len as usize)?;
            if at > b.len() {
                return None;
            }
        } else if len != -1 {
            return None;
        }
    }
    if at != b.len() {
        return None;
    }
    Some(mask_user_oid(b, 8))
}

/// record_send image: ncols(i32), then per column typoid(u32) len(i32)
/// data (-1 = NULL). Masks every user-range column typoid.
fn mask_record_image(b: &mut [u8]) -> Option<bool> {
    let ncols = be_i32(b, 0)?;
    // MaxTupleAttributeNumber is 1664.
    if !(0..=1664).contains(&ncols) {
        return None;
    }
    let mut at = 4usize;
    let mut oid_offsets = Vec::new();
    for _ in 0..ncols {
        be_i32(b, at)?; // typoid readable
        oid_offsets.push(at);
        let len = be_i32(b, at + 4)?;
        at += 8;
        if len >= 0 {
            at = at.checked_add(len as usize)?;
            if at > b.len() {
                return None;
            }
        } else if len != -1 {
            return None;
        }
    }
    if at != b.len() {
        return None;
    }
    let mut changed = false;
    for off in oid_offsets {
        changed |= mask_user_oid(b, off);
    }
    Some(changed)
}

fn parse_hex_cell(s: &str) -> Option<Vec<u8>> {
    let hex = s.strip_prefix("\\x")?;
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for pair in bytes.chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push((hi * 16 + lo) as u8);
    }
    Some(out)
}

fn hex_cell(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + 2 * b.len());
    s.push_str("\\x");
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

/// Mask embedded user-range type OIDs in one `\x`-hex binary cell when it
/// structurally parses as an array or record image. None = not a container
/// image / nothing user-range to mask.
pub fn normalize_binary_udt_cell(s: &str) -> Option<String> {
    let mut b = parse_hex_cell(s)?;
    let changed = mask_record_image(&mut b).or_else(|| {
        b = parse_hex_cell(s).unwrap();
        mask_array_image(&mut b)
    })?;
    changed.then(|| hex_cell(&b))
}

fn normalize_binary_udt_rows(rows: &[Vec<Option<String>>]) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|r| {
            r.iter()
                .map(|c| {
                    c.as_ref()
                        .map(|s| normalize_binary_udt_cell(s).unwrap_or_else(|| s.clone()))
                })
                .collect()
        })
        .collect()
}

// ---------------------------------------------------------------------
// FP-6 (round-7): C's memcmp-convention comparators (`btint4cmp`,
// `uuid_cmp`, ...) return arbitrary magnitude; SQL semantics only consume
// the sign, so a `SELECT uuid_cmp(...)` differential compares an
// unspecified value. When the statement's select list calls a *cmp
// builtin directly and every int4 cell agrees in SIGN, the magnitude
// difference is the known-benign residual (ruled `cmp-magnitude`).
// ORDER BY / index paths are unaffected — they consume the sign only by
// construction.
// ---------------------------------------------------------------------

/// Does the statement call a `*cmp` builtin directly? Word-boundary scan
/// for an identifier ending in `cmp` immediately followed by `(`.
pub fn calls_cmp_builtin(sql: &str) -> bool {
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i].is_ascii_alphabetic() || b[i] == b'_' {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            let word = &sql[start..i];
            let mut j = i;
            while j < b.len() && b[j] == b' ' {
                j += 1;
            }
            if word.len() > 3
                && word.to_ascii_lowercase().ends_with("cmp")
                && j < b.len()
                && b[j] == b'('
            {
                return true;
            }
        } else {
            i += 1;
        }
    }
    false
}

/// One int4 cell's numeric value: text form, or — round-13 (run
/// 72b2e74701d0e1310d59d39345e716da-59-13, seeds 451588928933415389 /
/// 1836375092275758948) — the 4-byte `\x`-hex image an xproto
/// binary-result batch renders. Both findings were FP-6 shapes the text
/// path already rules (`uuid_cmp` over the scalartypes literal deck: the
/// glibc-amd64 oracle's memcmp returns the byte difference, `\xffffff60`
/// = -160, where pgrust — like macOS memcmp — returns -1), escaping only
/// because the sign normalizer could not read a binary cell.
fn int4_cell_value(s: &str) -> Option<i64> {
    if let Ok(v) = s.parse::<i64>() {
        return Some(v);
    }
    let b = parse_hex_cell(s)?;
    if b.len() != 4 {
        return None;
    }
    Some(i32::from_be_bytes([b[0], b[1], b[2], b[3]]) as i64)
}

/// Map int4 cells to their sign ("-"/"0"/"+"); other columns unchanged.
fn normalize_int4_sign_rows(
    rows: &[Vec<Option<String>>],
    col_oids: &[u32],
) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|r| {
            r.iter()
                .enumerate()
                .map(|(i, c)| {
                    if col_oids.get(i) != Some(&23) {
                        return c.clone();
                    }
                    c.as_ref().map(|s| match int4_cell_value(s) {
                        Some(v) if v < 0 => "-".to_string(),
                        Some(0) => "0".to_string(),
                        Some(_) => "+".to_string(),
                        None => s.clone(),
                    })
                })
                .collect()
        })
        .collect()
}

/// Short shape name for mixed-outcome diagnostics.
fn outcome_shape(o: &StmtOutcome) -> String {
    match o {
        StmtOutcome::Rows { rows, .. } => format!("a rowset ({} rows)", rows.len()),
        StmtOutcome::Command { tag, .. } => format!("command tag {tag:?}"),
        StmtOutcome::CopyOut { tag, .. } => format!("a COPY transfer ({tag})"),
        StmtOutcome::Error { sqlstate, .. } => format!("error {sqlstate}"),
        StmtOutcome::ConnLost { .. } => "a lost connection".to_string(),
    }
}

/// First-divergence detail for two COPY payloads: offset, lengths, and a
/// bounded hex window around the first differing byte on each side.
fn copy_diff_detail(a: &[u8], b: &[u8]) -> String {
    let off = a
        .iter()
        .zip(b.iter())
        .position(|(x, y)| x != y)
        .unwrap_or_else(|| a.len().min(b.len()));
    let win = |s: &[u8]| -> String {
        let lo = off.saturating_sub(8);
        let hi = (off + 8).min(s.len());
        s[lo..hi].iter().map(|byte| format!("{byte:02x}")).collect()
    };
    format!(
        "COPY payloads differ at byte {} (len {} vs {}): ..{}.. vs ..{}..",
        off,
        a.len(),
        b.len(),
        win(a),
        win(b)
    )
}

pub struct DiffInput<'a> {
    pub sql: &'a str,
    pub a: &'a StmtOutcome,
    pub b: &'a StmtOutcome,
    pub ulp_tol: u64,
    /// Output columns the generator marked as order-sensitive float
    /// aggregates (session::Statement::soft_float_cols): compared
    /// ruled-soft. Empty for replayed scripts without metadata.
    pub soft_cols: &'a [usize],
    /// Opt-in (H1): additionally mask "actual time=" wall-clock digits on
    /// EXPLAIN statements before ruling out a diff. Set only for gramwalk
    /// statements (grammar-derived EXPLAIN ANALYZE cannot carry the
    /// explain module's TIMING OFF hygiene) and --mask-explain-timing
    /// replays; every other lane compares timing text strictly.
    pub mask_explain_timing: bool,
}

/// Raw classification, before the ruled-divergence table is consulted
/// (crate::ruled::apply_ruled does that pass). Ulp-only and tie-order-only
/// rowset agreements come out as `Ruled` candidates with placeholder ids
/// resolved by the ruled table.
pub fn classify(input: &DiffInput) -> Classified {
    let DiffInput { sql, a, b, ulp_tol, soft_cols, mask_explain_timing } = *input;
    use StmtOutcome::*;
    match (a, b) {
        (ConnLost { detail }, ConnLost { .. }) => Classified {
            class: DiffClass::SessionDiverged(Side::Both),
            detail: format!("both connections lost; A: {detail}"),
        },
        (ConnLost { detail }, _) => Classified {
            class: DiffClass::SessionDiverged(Side::A),
            detail: format!("A connection lost: {detail}"),
        },
        (_, ConnLost { detail }) => Classified {
            class: DiffClass::SessionDiverged(Side::B),
            detail: format!("B connection lost: {detail}"),
        },
        (Error { sqlstate: sa, message: ma }, Error { sqlstate: sb, message: mb }) => {
            // gramwalk special rule (rig law addendum): a pgrust-side
            // unimplemented-grammar-action fence error is ALWAYS a finding
            // naming the rule — never noise — even when C errors too with
            // the same SQLSTATE (the fence raises C's 0A000, so plain
            // SQLSTATE identity would swallow the gap).
            if let Some(rest) =
                mb.split("not yet implemented (grammar rule ").nth(1)
            {
                let rule: String =
                    rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                return Classified {
                    class: DiffClass::ErrorDiff,
                    detail: format!(
                        "UNPORTED grammar rule {rule}: pgrust errored {sb} ({mb}); \
                         A: {sa} ({ma})"
                    ),
                };
            }
            if sa == sb {
                Classified {
                    class: DiffClass::Match,
                    detail: if ma == mb {
                        format!("both error {sa}")
                    } else {
                        format!("both error {sa} (messages differ: {ma:?} vs {mb:?})")
                    },
                }
            } else if is_no_libxml_error(ma) && sb != "XX000" {
                // F9/LD1-N1 xml build-config class: the pinned oracle is a
                // no-libxml build (its parse/exec paths short-circuit with
                // 0A000 "unsupported XML feature"); pgrust deliberately
                // dlopens libxml2 and proceeds, so any statement C rejects
                // this way compares against a side the oracle cannot
                // execute. A pgrust XX000 (caught panic) still escalates.
                Classified {
                    class: DiffClass::Ruled("xml-config".to_string()),
                    detail: format!(
                        "A no-libxml oracle rejected 0A000 unsupported XML \
                         feature; B (libxml build) errored {sb} ({mb})"
                    ),
                }
            } else if is_no_lz4_error(ma) && sb != "XX000" {
                // Round-9 covdiff lz4 build-config class, mirror of
                // xml-config: an oracle built --without-lz4 rejects 0A000
                // "compression method lz4 not supported" where pgrust
                // (always lz4-capable) proceeds. A pgrust XX000 (caught
                // panic) still escalates.
                Classified {
                    class: DiffClass::Ruled("lz4-config".to_string()),
                    detail: format!(
                        "A no-lz4 oracle rejected 0A000 compression method \
                         lz4 not supported; B (lz4 build) errored {sb} ({mb})"
                    ),
                }
            } else if is_encoding_carve_refusal(sb, mb) && sa != "XX000" {
                // Round-10 FP-11: pgrust refused the statement per the
                // ratified UTF-8-only server-encoding carve
                // (docs/design/carve-ratifications.md §11) while C, which
                // has no such carve, failed differently (e.g. 22023
                // encoding-vs-locale mismatch on `CREATE DATABASE ...
                // ENCODING 'EUC_CN'`). The refusal is the carve operating
                // as ratified, so the pair carries no conformance signal.
                // An A-side XX000 (oracle panic) still escalates.
                Classified {
                    class: DiffClass::Ruled("encoding-carve".to_string()),
                    detail: format!(
                        "B refused per the UTF-8-only server-encoding carve: \
                         {sb} ({mb}); A: {sa} ({ma})"
                    ),
                }
            } else if is_shared_catalog_stmt(sql)
                && (is_tuple_concurrency_error(sa, ma) || is_tuple_concurrency_error(sb, mb))
            {
                Classified {
                    class: DiffClass::Ruled("shared-catalog-tcu".to_string()),
                    detail: format!("SQLSTATE {sa} ({ma}) vs {sb} ({mb})"),
                }
            } else if is_alter_system_stmt(sql)
                && (is_autoconf_parse_error(sa, ma) || is_autoconf_parse_error(sb, mb))
            {
                // RB-14: one side's instance-global postgresql.auto.conf was
                // poisoned by a concurrent batch's write racing this
                // statement; the other side's wasn't (yet).
                Classified {
                    class: DiffClass::Ruled("autoconf-shared-race".to_string()),
                    detail: format!("SQLSTATE {sa} ({ma}) vs {sb} ({mb})"),
                }
            } else {
                Classified {
                    class: DiffClass::ErrorDiff,
                    detail: format!("SQLSTATE {sa} ({ma}) vs {sb} ({mb})"),
                }
            }
        }
        (Error { sqlstate, message }, _) => {
            if is_shared_catalog_stmt(sql) && is_tuple_concurrency_error(sqlstate, message) {
                return Classified {
                    class: DiffClass::Ruled("shared-catalog-tcu".to_string()),
                    detail: format!("A errored {sqlstate} ({message}); B succeeded"),
                };
            }
            if is_alter_system_stmt(sql) && is_autoconf_parse_error(sqlstate, message) {
                // RB-14 autoconf shared-state race (see is_autoconf_parse_error).
                return Classified {
                    class: DiffClass::Ruled("autoconf-shared-race".to_string()),
                    detail: format!("A errored {sqlstate} ({message}); B succeeded"),
                };
            }
            if is_no_lz4_error(message) {
                // Round-9 covdiff lz4 build-config class (see the
                // Error/Error arm above).
                return Classified {
                    class: DiffClass::Ruled("lz4-config".to_string()),
                    detail: "A no-lz4 oracle rejected 0A000 compression \
                             method lz4 not supported; B (lz4 build) \
                             succeeded"
                        .to_string(),
                };
            }
            if is_no_libxml_error(message) {
                Classified {
                    class: DiffClass::Ruled("xml-config".to_string()),
                    detail: "A no-libxml oracle rejected 0A000 unsupported XML \
                             feature; B (libxml build) succeeded"
                        .to_string(),
                }
            } else {
                Classified {
                    class: DiffClass::ErrorDiff,
                    detail: format!("A errored {sqlstate} ({message}); B succeeded"),
                }
            }
        }
        (_, Error { sqlstate, message }) => {
            if is_shared_catalog_stmt(sql) && is_tuple_concurrency_error(sqlstate, message) {
                return Classified {
                    class: DiffClass::Ruled("shared-catalog-tcu".to_string()),
                    detail: format!("A succeeded; B errored {sqlstate} ({message})"),
                };
            }
            // Round-10 FP-11: pgrust's ratified UTF-8-only server-encoding
            // carve refusal (docs/design/carve-ratifications.md §11) where
            // C — which has no such carve — accepted the statement is the
            // carve operating as ratified, not a conformance gap.
            // Signature scope: only the exact carve-citation message.
            if is_encoding_carve_refusal(sqlstate, message) {
                return Classified {
                    class: DiffClass::Ruled("encoding-carve".to_string()),
                    detail: format!(
                        "A succeeded; B refused per the UTF-8-only \
                         server-encoding carve: {sqlstate} ({message})"
                    ),
                };
            }
            if is_alter_system_stmt(sql) && is_autoconf_parse_error(sqlstate, message) {
                // RB-14 autoconf shared-state race (see is_autoconf_parse_error).
                return Classified {
                    class: DiffClass::Ruled("autoconf-shared-race".to_string()),
                    detail: format!("A succeeded; B errored {sqlstate} ({message})"),
                };
            }
            // FP-4 (round-7): Antithesis thread-pauses the instrumented
            // SUT only — the in-container C oracle is not symmetrically
            // faulted, so pgrust's parallel-worker bring-up can time out
            // where A sails through. Exact-signature scope; a B-only
            // 55000 with any OTHER message still escalates. Worker-
            // acquisition liveness is owned by the liveness campaign.
            if sqlstate == "55000" && message == "parallel worker failed to initialize" {
                return Classified {
                    class: DiffClass::Ruled("parallel-worker-init".to_string()),
                    detail: format!("A succeeded; B errored {sqlstate} ({message})"),
                };
            }
            // Round-20 soak sibling of FP-4: the same asymmetric
            // thread-pause faults can run a statement (or a state probe)
            // into pgrust's statement_timeout while the unfaulted A oracle
            // sails through. The timeouts in play are the module decks'
            // own symmetric SETs, so B-only expiry is injected scheduling,
            // not conformance; exact-signature scope, and statement
            // responsiveness is owned by the liveness cancel ladders.
            if sqlstate == "57014" && message == "canceling statement due to statement timeout" {
                return Classified {
                    class: DiffClass::Ruled("fault-stmt-timeout".to_string()),
                    detail: format!("A succeeded; B errored {sqlstate} ({message})"),
                };
            }
            // Round-20 soak: DROP TABLE deadlocking against an autovacuum
            // ANALYZE of a partition (the worker propagates stats to
            // ancestors, closing a lock cycle with the DROP) is genuine
            // upstream C behavior — a hard cycle involving autovacuum
            // 40P01s in C too (deadlock.c returns DS_BLOCKED_BY_AUTOVACUUM
            // only when there is NO cycle). B-only because thread-pause
            // faults widen the collision window on the instrumented side
            // while the dedicated A oracle's autovacuum idles. Scope:
            // DROP TABLE statements with the exact deadlock message;
            // detector correctness is owned by the liveness deadlock
            // campaign (victim-or-commit, detector-chose-victim).
            if sqlstate == "40P01"
                && message == "deadlock detected"
                && is_drop_table_stmt(sql)
            {
                return Classified {
                    class: DiffClass::Ruled("drop-autovacuum-deadlock".to_string()),
                    detail: format!("A succeeded; B errored {sqlstate} ({message})"),
                };
            }
            // Round-9 covdiff tid-input class: C 18.3's tidin parses via
            // strtol and tolerates forms like '(0,)'; that laxity was
            // REPORTED UPSTREAM and fixed in later PostgreSQL versions,
            // and pgrust deliberately implements the fixed (strict)
            // behavior. Exact-signature scope: A succeeded, B raised
            // 22P02 naming type tid; any other 22P02 still escalates.
            if sqlstate == "22P02" && message.contains("invalid input syntax for type tid") {
                return Classified {
                    class: DiffClass::Ruled("tid-input-upstream".to_string()),
                    detail: format!("A succeeded; B errored {sqlstate} ({message})"),
                };
            }
            Classified {
                class: DiffClass::ErrorDiff,
                detail: format!("A succeeded; B errored {sqlstate} ({message})"),
            }
        }
        (CopyOut { bytes: ba, tag: ta }, CopyOut { bytes: bb, tag: tb }) => {
            if ba == bb {
                if ta == tb {
                    Classified { class: DiffClass::Match, detail: String::new() }
                } else {
                    Classified {
                        class: DiffClass::CountDiff,
                        detail: format!("COPY tags differ: {ta:?} vs {tb:?}"),
                    }
                }
            } else {
                Classified {
                    class: DiffClass::RowsetDiff,
                    detail: copy_diff_detail(ba, bb),
                }
            }
        }
        (CopyOut { tag, .. }, other) => Classified {
            class: DiffClass::RowsetDiff,
            detail: format!(
                "A produced a COPY transfer ({tag}); B produced {}",
                outcome_shape(other)
            ),
        },
        (other, CopyOut { tag, .. }) => Classified {
            class: DiffClass::RowsetDiff,
            detail: format!(
                "A produced {}; B produced a COPY transfer ({tag})",
                outcome_shape(other)
            ),
        },
        (Command { tag: ta, affected: ca }, Command { tag: tb, affected: cb }) => {
            if ca != cb {
                Classified {
                    class: DiffClass::CountDiff,
                    detail: format!("affected {ca:?} ({ta}) vs {cb:?} ({tb})"),
                }
            } else {
                Classified { class: DiffClass::Match, detail: String::new() }
            }
        }
        (Rows { .. }, Command { tag, .. }) => Classified {
            class: DiffClass::RowsetDiff,
            detail: format!("A returned rows; B returned command tag {tag:?}"),
        },
        (Command { tag, .. }, Rows { .. }) => Classified {
            class: DiffClass::RowsetDiff,
            detail: format!("A returned command tag {tag:?}; B returned rows"),
        },
        (Rows { col_oids: oa, rows: ra }, Rows { col_oids: ob, rows: rb }) => {
            if !col_oids_equivalent(oa, ob) {
                return Classified {
                    class: DiffClass::RowsetDiff,
                    detail: format!("column type oids differ: {oa:?} vs {ob:?}"),
                };
            }
            let modes = col_cmp_modes(oa, soft_cols);
            let ordered = has_order_by(sql);
            // EXPLAIN statements: a raw diff that disappears once runtime
            // resource counters are masked is Ruled("explain-counter"),
            // not a finding — plan *structure* still compares strictly.
            let explain_counter_only = |d: &str| -> Option<Classified> {
                if !is_explain_stmt(sql) {
                    return None;
                }
                let na = normalize_explain_rows(ra);
                let nb = normalize_explain_rows(rb);
                let norm = if ordered {
                    cmp_rows_ordered(&na, &nb, &modes, ulp_tol)
                } else {
                    cmp_rows_multiset(&na, &nb, &modes, ulp_tol)
                };
                if matches!(norm, RowsetCmp::Diff(_)) {
                    None
                } else {
                    Some(Classified {
                        class: DiffClass::Ruled("explain-counter".to_string()),
                        detail: format!(
                            "equal after masking runtime resource counters: {d}"
                        ),
                    })
                }
            };
            // Opt-in H1 mask: counters + "actual time=" wall-clock digits.
            // Tried only after the counter-only mask failed, so the ruled
            // id records that timing text was load-bearing for the match.
            let explain_timing_only = |d: &str| -> Option<Classified> {
                if !mask_explain_timing || !is_explain_stmt(sql) {
                    return None;
                }
                let na = normalize_explain_timing_rows(&normalize_explain_rows(ra));
                let nb = normalize_explain_timing_rows(&normalize_explain_rows(rb));
                let norm = if ordered {
                    cmp_rows_ordered(&na, &nb, &modes, ulp_tol)
                } else {
                    cmp_rows_multiset(&na, &nb, &modes, ulp_tol)
                };
                if matches!(norm, RowsetCmp::Diff(_)) {
                    None
                } else {
                    Some(Classified {
                        class: DiffClass::Ruled("explain-timing".to_string()),
                        detail: format!(
                            "equal after masking wall-clock timing text: {d}"
                        ),
                    })
                }
            };
            // r20 update-rowcount: the TEXT "Planning:" buffer-usage
            // block's PRESENCE is session cache state (see
            // strip_planning_buffer_rows). Tried after the counter and
            // timing masks, so the ruled id records that the block's
            // presence was load-bearing for the match; any other
            // structural difference still escalates.
            let explain_planning_only = |d: &str| -> Option<Classified> {
                if !is_explain_stmt(sql) {
                    return None;
                }
                let na = normalize_explain_rows(&strip_planning_buffer_rows(ra));
                let nb = normalize_explain_rows(&strip_planning_buffer_rows(rb));
                let (na, nb) = if mask_explain_timing {
                    (
                        normalize_explain_timing_rows(&na),
                        normalize_explain_timing_rows(&nb),
                    )
                } else {
                    (na, nb)
                };
                let norm = if ordered {
                    cmp_rows_ordered(&na, &nb, &modes, ulp_tol)
                } else {
                    cmp_rows_multiset(&na, &nb, &modes, ulp_tol)
                };
                if matches!(norm, RowsetCmp::Diff(_)) {
                    None
                } else {
                    Some(Classified {
                        class: DiffClass::Ruled("explain-planning-buffers".to_string()),
                        detail: format!(
                            "equal after dropping the TEXT Planning: buffer-usage \
                             block (presence is session cache state): {d}"
                        ),
                    })
                }
            };
            // Round-9 instance-state candidates: rowset diffs on
            // statements referencing instance-config views (FP-9) or
            // calling backup-control functions are cluster-local state,
            // never cross-cluster comparable. Emitted as Ruled candidates
            // so the acceptance still resolves through the ruled table.
            let instance_candidate = |d: &str| -> Option<Classified> {
                if is_instance_config_stmt(sql) {
                    return Some(Classified {
                        class: DiffClass::Ruled("instance-config".to_string()),
                        detail: format!("instance-config view rowset differs: {d}"),
                    });
                }
                if calls_backup_control(sql) {
                    return Some(Classified {
                        class: DiffClass::Ruled("instance-lsn".to_string()),
                        detail: format!("backup-control instance-LSN surface differs: {d}"),
                    });
                }
                None
            };
            // Round-9 RB-10: EXPLAIN of DECLARE ... SCROLL CURSOR. C's
            // planner wraps a SCROLL cursor's plan in Materialize when
            // !ExecSupportsBackwardScan (planner.c:444-451); pgrust
            // deliberately deleted that wrap — every backward cursor read
            // is served by the portal tuplestore (ratified strategy
            // divergence, notes/se-wave10-integration.md §5 item 2,
            // 2026-07-17). The candidate fires only when stripping a
            // top-level Materialize wrap from the A side makes the plans
            // equal under the same masks the lane already applies; any
            // other structural difference still escalates.
            let scroll_materialize_only = |d: &str| -> Option<Classified> {
                if !is_scroll_declare_explain_stmt(sql) {
                    return None;
                }
                let stripped = strip_top_materialize_rows(ra)?;
                let na = normalize_explain_rows(&stripped);
                let nb = normalize_explain_rows(rb);
                let (na, nb) = if mask_explain_timing {
                    (
                        normalize_explain_timing_rows(&na),
                        normalize_explain_timing_rows(&nb),
                    )
                } else {
                    (na, nb)
                };
                let norm = if ordered {
                    cmp_rows_ordered(&na, &nb, &modes, ulp_tol)
                } else {
                    cmp_rows_multiset(&na, &nb, &modes, ulp_tol)
                };
                if matches!(norm, RowsetCmp::Diff(_)) {
                    None
                } else {
                    Some(Classified {
                        class: DiffClass::Ruled("scroll-materialize".to_string()),
                        detail: format!(
                            "equal after stripping C's top-level Materialize \
                             SCROLL wrap: {d}"
                        ),
                    })
                }
            };
            // Round-7 OID/comparator fallbacks, tried in order after the
            // EXPLAIN/GUC masks: each normalizes BOTH sides identically
            // and only fires when the whole remaining diff disappears.
            let recmp = |na: &[Vec<Option<String>>], nb: &[Vec<Option<String>>]| {
                let c = if ordered {
                    cmp_rows_ordered(na, nb, &modes, ulp_tol)
                } else {
                    cmp_rows_multiset(na, nb, &modes, ulp_tol)
                };
                !matches!(c, RowsetCmp::Diff(_))
            };
            let structural_fallbacks = |d: &str| -> Option<Classified> {
                // FP-2: user-range OID literals inside deparse text.
                if recmp(&normalize_oid_literal_rows(ra), &normalize_oid_literal_rows(rb)) {
                    return Some(Classified {
                        class: DiffClass::Ruled("oid-literal".to_string()),
                        detail: format!(
                            "equal after masking user-range '<n>'::oid literals: {d}"
                        ),
                    });
                }
                // Round-8: TOAST relation names embedding the owning
                // table's user-range OID.
                if recmp(&normalize_toast_name_rows(ra), &normalize_toast_name_rows(rb)) {
                    return Some(Classified {
                        class: DiffClass::Ruled("toast-name".to_string()),
                        detail: format!(
                            "equal after masking user-range pg_toast_<n> relation names: {d}"
                        ),
                    });
                }
                // FP-5: embedded type OIDs in binary container images.
                if recmp(&normalize_binary_udt_rows(ra), &normalize_binary_udt_rows(rb)) {
                    return Some(Classified {
                        class: DiffClass::Ruled("binary-udt-oid".to_string()),
                        detail: format!(
                            "equal after masking embedded user-range type oids \
                             in binary container images: {d}"
                        ),
                    });
                }
                // FP-6: *cmp() builtin magnitude with matching sign.
                if calls_cmp_builtin(sql)
                    && recmp(
                        &normalize_int4_sign_rows(ra, oa),
                        &normalize_int4_sign_rows(rb, ob),
                    )
                {
                    return Some(Classified {
                        class: DiffClass::Ruled("cmp-magnitude".to_string()),
                        detail: format!(
                            "int4 *cmp() results equal in sign, magnitude differs: {d}"
                        ),
                    });
                }
                None
            };
            if ordered {
                match cmp_rows_ordered(ra, rb, &modes, ulp_tol) {
                    RowsetCmp::Equal => {
                        Classified { class: DiffClass::Match, detail: String::new() }
                    }
                    RowsetCmp::EqualUlp => Classified {
                        class: DiffClass::Ruled("float-ulp".to_string()),
                        detail: "equal within float ulp tolerance".to_string(),
                    },
                    RowsetCmp::EqualSoft => Classified {
                        class: DiffClass::Ruled("float-agg".to_string()),
                        detail: "equal outside ruled-soft float-aggregate columns".to_string(),
                    },
                    RowsetCmp::Diff(d) => {
                        if let Some(c) = explain_counter_only(&d) {
                            return c;
                        }
                        if let Some(c) = explain_timing_only(&d) {
                            return c;
                        }
                        if let Some(c) = explain_planning_only(&d) {
                            return c;
                        }
                        if let Some(c) = scroll_materialize_only(&d) {
                            return c;
                        }
                        if let Some(c) = guc_inventory_candidate(sql, ra, rb) {
                            return c;
                        }
                        if let Some(c) = instance_candidate(&d) {
                            return c;
                        }
                        if let Some(c) = structural_fallbacks(&d) {
                            return c;
                        }
                        match cmp_rows_multiset(ra, rb, &modes, ulp_tol) {
                            RowsetCmp::Equal | RowsetCmp::EqualUlp => Classified {
                                class: DiffClass::Ruled("tie-order".to_string()),
                                detail: format!("multiset-equal, order differs: {d}"),
                            },
                            // A soft column may itself have steered the sort;
                            // the soft ruling subsumes the order difference.
                            RowsetCmp::EqualSoft => Classified {
                                class: DiffClass::Ruled("float-agg".to_string()),
                                detail: format!(
                                    "multiset-equal outside ruled-soft float-aggregate \
                                     columns, order differs: {d}"
                                ),
                            },
                            RowsetCmp::Diff(_) => {
                                Classified { class: DiffClass::RowsetDiff, detail: d }
                            }
                        }
                    }
                }
            } else {
                match cmp_rows_multiset(ra, rb, &modes, ulp_tol) {
                    RowsetCmp::Equal => {
                        Classified { class: DiffClass::Match, detail: String::new() }
                    }
                    RowsetCmp::EqualUlp => Classified {
                        class: DiffClass::Ruled("float-ulp".to_string()),
                        detail: "multiset-equal within float ulp tolerance".to_string(),
                    },
                    RowsetCmp::EqualSoft => Classified {
                        class: DiffClass::Ruled("float-agg".to_string()),
                        detail: "multiset-equal outside ruled-soft float-aggregate columns"
                            .to_string(),
                    },
                    RowsetCmp::Diff(d) => {
                        if let Some(c) = explain_counter_only(&d) {
                            return c;
                        }
                        if let Some(c) = explain_timing_only(&d) {
                            return c;
                        }
                        if let Some(c) = explain_planning_only(&d) {
                            return c;
                        }
                        if let Some(c) = scroll_materialize_only(&d) {
                            return c;
                        }
                        if let Some(c) = guc_inventory_candidate(sql, ra, rb) {
                            return c;
                        }
                        if let Some(c) = instance_candidate(&d) {
                            return c;
                        }
                        if let Some(c) = structural_fallbacks(&d) {
                            return c;
                        }
                        Classified { class: DiffClass::RowsetDiff, detail: d }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(cells: &[&[Option<&str>]]) -> Vec<Vec<Option<String>>> {
        cells
            .iter()
            .map(|r| r.iter().map(|c| c.map(|s| s.to_string())).collect())
            .collect()
    }

    fn rowset(col_oids: Vec<u32>, r: Vec<Vec<Option<String>>>) -> StmtOutcome {
        StmtOutcome::Rows { col_oids, rows: r }
    }

    fn classify_sql(sql: &str, a: &StmtOutcome, b: &StmtOutcome) -> Classified {
        classify(&DiffInput { sql, a, b, ulp_tol: 4, soft_cols: &[], mask_explain_timing: false })
    }

    /// gramwalk special rule: a pgrust-side unimplemented-grammar-action
    /// fence error is a finding naming the rule even when C errors with the
    /// SAME SQLSTATE (plain identity would swallow the gap as MATCH).
    #[test]
    fn unported_grammar_rule_error_is_always_a_finding() {
        let a = StmtOutcome::Error {
            sqlstate: "0A000".to_string(),
            message: "some feature is not supported".to_string(),
        };
        let b = StmtOutcome::Error {
            sqlstate: "0A000".to_string(),
            message: "this SQL construct is not yet implemented (grammar rule 2445: \
                      AexprConst, gram.y:17387)"
                .to_string(),
        };
        let c = classify_sql("SELECT int4(1) '42';", &a, &b);
        assert_eq!(c.class, DiffClass::ErrorDiff);
        assert!(c.detail.contains("UNPORTED grammar rule 2445"), "{}", c.detail);
        // Ordinary matched errors still MATCH.
        let c = classify_sql("SELECT;", &a, &a);
        assert_eq!(c.class, DiffClass::Match);
    }

    /// F9/LD1-N1: the no-libxml oracle's 0A000 "unsupported XML feature"
    /// rejections are xml-config ruled candidates whatever the libxml-built
    /// B side did — except a caught panic (XX000), which still escalates.
    #[test]
    fn no_libxml_oracle_rejection_is_xml_config_candidate() {
        let a = StmtOutcome::Error {
            sqlstate: "0A000".to_string(),
            message: "unsupported XML feature".to_string(),
        };
        let ok = rowset(vec![142], rows(&[&[Some("<foo/>")]]));
        let c = classify_sql("select xmlelement(name foo);", &a, &ok);
        assert_eq!(c.class, DiffClass::Ruled("xml-config".to_string()));
        let b = StmtOutcome::Error {
            sqlstate: "42601".to_string(),
            message: "DEFAULT is not allowed in this context".to_string(),
        };
        let c = classify_sql("select xmlelement(name nchar, - default);", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("xml-config".to_string()));
        // A pgrust caught panic is never absorbed.
        let b = StmtOutcome::Error {
            sqlstate: "XX000".to_string(),
            message: "panicked at ...".to_string(),
        };
        let c = classify_sql("select xmlelement(name foo);", &a, &b);
        assert_eq!(c.class, DiffClass::ErrorDiff);
        // Any other A-side 0A000 stays a finding when B succeeds.
        let a = StmtOutcome::Error {
            sqlstate: "0A000".to_string(),
            message: "some other unsupported feature".to_string(),
        };
        let c = classify_sql("select 1;", &a, &ok);
        assert_eq!(c.class, DiffClass::ErrorDiff);
    }

    /// Round-10 RB-14: an asymmetric F0000 auto.conf re-parse failure on
    /// ALTER SYSTEM is the instance-global-file race, ruled in every
    /// direction — but only that exact message, and only on ALTER SYSTEM.
    #[test]
    fn alter_system_autoconf_parse_race_is_ruled_candidate() {
        let f0000 = StmtOutcome::Error {
            sqlstate: "F0000".to_string(),
            message: "could not parse contents of file \"postgresql.auto.conf\"".to_string(),
        };
        let ok = StmtOutcome::Command { tag: "ALTER SYSTEM".to_string(), affected: None };
        let ruled = DiffClass::Ruled("autoconf-shared-race".to_string());
        // A-only, B-only, and F0000-vs-other-error shapes are all candidates.
        let c = classify_sql("alter system reset flag . fz_scalar . trim ;", &f0000, &ok);
        assert_eq!(c.class, ruled);
        let c = classify_sql("alter system set xmlparse . k_int . fz_wide = false ;", &ok, &f0000);
        assert_eq!(c.class, ruled);
        let other = StmtOutcome::Error {
            sqlstate: "42704".to_string(),
            message: "unrecognized configuration parameter \"nope\"".to_string(),
        };
        let c = classify_sql("ALTER SYSTEM RESET nope;", &f0000, &other);
        assert_eq!(c.class, ruled);
        // Symmetric F0000 stays MATCH.
        let c = classify_sql("alter system set a.b.c = 1;", &f0000, &f0000);
        assert_eq!(c.class, DiffClass::Match);
        // The same error on a non-ALTER-SYSTEM statement escalates.
        let c = classify_sql("SELECT pg_reload_conf();", &f0000, &ok);
        assert_eq!(c.class, DiffClass::ErrorDiff);
        // Any other F0000 message on ALTER SYSTEM escalates.
        let noisy = StmtOutcome::Error {
            sqlstate: "F0000".to_string(),
            message: "could not parse contents of file \"postgresql.conf\"".to_string(),
        };
        let c = classify_sql("ALTER SYSTEM SET work_mem = '1MB';", &noisy, &ok);
        assert_eq!(c.class, DiffClass::ErrorDiff);
    }

    /// Round-9 covdiff: an oracle built --without-lz4 rejecting 0A000
    /// "compression method lz4 not supported" is the lz4-config
    /// candidate; a pgrust panic or any other 0A000 still escalates.
    #[test]
    fn no_lz4_oracle_rejection_is_lz4_config_candidate() {
        let a = StmtOutcome::Error {
            sqlstate: "0A000".to_string(),
            message: "compression method lz4 not supported".to_string(),
        };
        let ok = StmtOutcome::Command { tag: "ALTER TABLE".to_string(), affected: None };
        let sql = "ALTER TABLE dd_w ALTER COLUMN b SET COMPRESSION lz4;";
        let c = classify_sql(sql, &a, &ok);
        assert_eq!(c.class, DiffClass::Ruled("lz4-config".to_string()));
        // Both-error variant with differing SQLSTATEs still absorbs.
        let b = StmtOutcome::Error {
            sqlstate: "42704".to_string(),
            message: "column data type does not support compression".to_string(),
        };
        let c = classify_sql(sql, &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("lz4-config".to_string()));
        // A pgrust caught panic is never absorbed.
        let b = StmtOutcome::Error {
            sqlstate: "XX000".to_string(),
            message: "panicked at ...".to_string(),
        };
        let c = classify_sql(sql, &a, &b);
        assert_eq!(c.class, DiffClass::ErrorDiff);
        // Any other A-side 0A000 stays a finding when B succeeds.
        let a = StmtOutcome::Error {
            sqlstate: "0A000".to_string(),
            message: "some other unsupported feature".to_string(),
        };
        let c = classify_sql(sql, &a, &ok);
        assert_eq!(c.class, DiffClass::ErrorDiff);
    }

    /// Round-10 FP-11: a B-side refusal carrying the ratified UTF-8-only
    /// server-encoding carve citation (docs/design/carve-ratifications.md
    /// §11) is the encoding-carve candidate whether A errored differently
    /// or succeeded; any other encoding error and an A-side oracle panic
    /// still escalate.
    #[test]
    fn b_side_encoding_carve_refusal_is_ruled_candidate() {
        let carve = StmtOutcome::Error {
            sqlstate: "0A000".to_string(),
            message: "server encoding \"EUC_CN\" is not supported by pgrust; \
                      only \"UTF8\" and \"SQL_ASCII\" server encodings are \
                      accepted (UTF-8-only carve, \
                      docs/design/carve-ratifications.md)"
                .to_string(),
        };
        let sql = "create database fuzz_gramwalk_210_1_json WITH encoding + 2 ;";
        // Both-error, differing SQLSTATEs (the observed round-10 shape:
        // A 22023 encoding-vs-locale mismatch vs the B carve refusal).
        let a = StmtOutcome::Error {
            sqlstate: "22023".to_string(),
            message: "encoding \"EUC_CN\" does not match locale \"C.UTF-8\"".to_string(),
        };
        let c = classify_sql(sql, &a, &carve);
        assert_eq!(c.class, DiffClass::Ruled("encoding-carve".to_string()));
        assert!(c.detail.contains("carve"), "{}", c.detail);
        // A succeeded (a C oracle with a matching locale would create the
        // database): the carve refusal is still the candidate.
        let ok = StmtOutcome::Command { tag: "CREATE DATABASE".to_string(), affected: None };
        let c = classify_sql(sql, &ok, &carve);
        assert_eq!(c.class, DiffClass::Ruled("encoding-carve".to_string()));
        // Any OTHER B-side 0A000 — even encoding-flavored — escalates.
        let other = StmtOutcome::Error {
            sqlstate: "0A000".to_string(),
            message: "encoding conversion from EUC_CN to UTF8 not supported".to_string(),
        };
        assert_eq!(classify_sql(sql, &a, &other).class, DiffClass::ErrorDiff);
        assert_eq!(classify_sql(sql, &ok, &other).class, DiffClass::ErrorDiff);
        // The carve message under a different SQLSTATE escalates too.
        let StmtOutcome::Error { message, .. } = &carve else { unreachable!() };
        let wrong_state = StmtOutcome::Error {
            sqlstate: "22023".to_string(),
            message: message.clone(),
        };
        assert_eq!(classify_sql(sql, &ok, &wrong_state).class, DiffClass::ErrorDiff);
        // An A-side oracle panic is never absorbed.
        let panic = StmtOutcome::Error {
            sqlstate: "XX000".to_string(),
            message: "panicked at ...".to_string(),
        };
        assert_eq!(classify_sql(sql, &panic, &carve).class, DiffClass::ErrorDiff);
        // The A-side carrying the carve message is NOT the candidate
        // (only pgrust's own refusal qualifies).
        assert_eq!(classify_sql(sql, &carve, &a).class, DiffClass::ErrorDiff);
    }

    /// Round-9 covdiff: B-only 22P02 on tid input where A succeeded is
    /// the tid-input-upstream candidate (pgrust matches the upstream
    /// FIXED strict behavior); any other 22P02 still escalates.
    #[test]
    fn b_only_tid_input_error_is_upstream_candidate() {
        let ok = rowset(vec![27], rows(&[&[Some("(0,0)")]]));
        let b = StmtOutcome::Error {
            sqlstate: "22P02".to_string(),
            message: "invalid input syntax for type tid: \"(0,)\"".to_string(),
        };
        let c = classify_sql("SELECT '(0,)'::tid::text;", &ok, &b);
        assert_eq!(c.class, DiffClass::Ruled("tid-input-upstream".to_string()));
        // A different type's 22P02 escalates.
        let b = StmtOutcome::Error {
            sqlstate: "22P02".to_string(),
            message: "invalid input syntax for type integer: \"x\"".to_string(),
        };
        let c = classify_sql("SELECT 'x'::int4;", &ok, &b);
        assert_eq!(c.class, DiffClass::ErrorDiff);
        // An A-only tid error is NOT the candidate (pgrust laxer than C
        // would be a real conformance finding).
        let a = StmtOutcome::Error {
            sqlstate: "22P02".to_string(),
            message: "invalid input syntax for type tid: \"(0,)\"".to_string(),
        };
        let c = classify_sql("SELECT '(0,)'::tid::text;", &a, &ok);
        assert_eq!(c.class, DiffClass::ErrorDiff);
    }

    /// Round-9 FP-9: any rowset/row-count diff on a statement referencing
    /// an instance-config view is the instance-config candidate; the same
    /// diff on any other statement stays a finding, and error outcomes on
    /// instance-config statements still compare strictly.
    #[test]
    fn instance_config_view_diff_is_ruled_candidate() {
        let a = rowset(
            vec![25],
            rows(&[&[Some("local")], &[Some("host")], &[Some("host")]]),
        );
        let b = rowset(vec![25], rows(&[&[Some("local")], &[Some("host")]]));
        let c = classify_sql(
            "SELECT type FROM pg_hba_file_rules ORDER BY rule_number;",
            &a,
            &b,
        );
        assert_eq!(c.class, DiffClass::Ruled("instance-config".to_string()));
        let c = classify_sql("SELECT name FROM pg_file_settings;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("instance-config".to_string()));
        let c = classify_sql("SELECT name FROM pg_shmem_allocations;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("instance-config".to_string()));
        // FP-9b: pg_stat_progress_* (prefix) and pg_stat_activity are
        // live INSTANCE state — a concurrent session's command shows up
        // on one side only.
        let c = classify_sql("SELECT count(*) FROM pg_stat_progress_analyze;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("instance-config".to_string()));
        let c = classify_sql(
            "SELECT relid FROM pg_stat_progress_create_index;",
            &a,
            &b,
        );
        assert_eq!(c.class, DiffClass::Ruled("instance-config".to_string()));
        let c = classify_sql("SELECT state FROM pg_stat_activity;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("instance-config".to_string()));
        // FP-10 (round-10): pg_locks is the same cluster-global live
        // state — a concurrent batch's ungranted lock appears on one
        // side only.
        let c = classify_sql("SELECT count(*) FROM pg_locks WHERE NOT granted;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("instance-config".to_string()));
        // Same shape elsewhere escalates.
        let c = classify_sql("SELECT t FROM fz_rich;", &a, &b);
        assert_eq!(c.class, DiffClass::RowsetDiff);
        // Error outcomes on instance-config statements are not absorbed.
        let e = StmtOutcome::Error {
            sqlstate: "42703".to_string(),
            message: "column \"bogus\" does not exist".to_string(),
        };
        let c = classify_sql("SELECT bogus FROM pg_hba_file_rules;", &a, &e);
        assert_eq!(c.class, DiffClass::ErrorDiff);
    }

    /// Round-9: a rowset diff on a raw backup-control call is the
    /// instance-lsn candidate; the same diff elsewhere escalates.
    #[test]
    fn backup_control_diff_is_instance_lsn_candidate() {
        let a = rowset(vec![3220], rows(&[&[Some("0/16000028")]]));
        let b = rowset(vec![3220], rows(&[&[Some("0/17000060")]]));
        let c = classify_sql("SELECT pg_backup_start('fz_q5_dup', true);", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("instance-lsn".to_string()));
        let c = classify_sql("SELECT '0/1'::pg_lsn;", &a, &b);
        assert_eq!(c.class, DiffClass::RowsetDiff);
    }

    /// F4: SHOW ALL / pg_settings row-count divergence is a guc-inventory
    /// ruled candidate; a wrong GUC VALUE (equal counts) stays a finding.
    #[test]
    fn guc_inventory_row_count_is_ruled_candidate_value_diff_is_not() {
        // SHOW ALL: 2 rows vs 3 rows -> candidate.
        let a = rowset(
            vec![25, 25, 25],
            rows(&[
                &[Some("work_mem"), Some("4MB"), Some("d")],
                &[Some("jit"), Some("on"), Some("d")],
            ]),
        );
        let b = rowset(
            vec![25, 25, 25],
            rows(&[
                &[Some("work_mem"), Some("4MB"), Some("d")],
                &[Some("jit"), Some("on"), Some("d")],
                &[Some("pgrust.runtime"), Some("native"), Some("d")],
            ]),
        );
        let c = classify_sql("show all ;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("guc-inventory".to_string()));
        // count(*) over pg_settings: 1x1 integer cells -> candidate.
        let a = rowset(vec![20], rows(&[&[Some("398")]]));
        let b = rowset(vec![20], rows(&[&[Some("460")]]));
        let c = classify_sql("select count(*) from pg_settings ;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("guc-inventory".to_string()));
        // Equal-count pg_settings rowset with a differing VALUE: finding.
        let a = rowset(vec![25], rows(&[&[Some("4MB")]]));
        let b = rowset(vec![25], rows(&[&[Some("64MB")]]));
        let c = classify_sql(
            "select setting from pg_settings where name = 'work_mem' ;",
            &a,
            &b,
        );
        assert_eq!(c.class, DiffClass::RowsetDiff);
        // A row-count diff on a NON-inventory statement stays a finding.
        let a = rowset(vec![25], rows(&[&[Some("x")]]));
        let b = rowset(vec![25], rows(&[&[Some("x")], &[Some("y")]]));
        let c = classify_sql("select t from tbl ;", &a, &b);
        assert_eq!(c.class, DiffClass::RowsetDiff);
    }

    /// Round-9 RB-10: EXPLAIN DECLARE ... SCROLL rules out only when the
    /// whole diff is C's top-level Materialize wrap; non-SCROLL DECLAREs
    /// and any residual plan difference stay findings.
    #[test]
    fn explain_declare_scroll_materialize_wrap() {
        let scroll_sql =
            "explain verbose declare xmlforest scroll asensitive scroll binary \
             cursor for select ;";
        // Seed 3435819938407035103 shape: C wraps, pgrust does not.
        let a = rowset(
            vec![25],
            rows(&[
                &[Some("Materialize  (cost=0.00..0.01 rows=1 width=0)")],
                &[Some("  ->  Result  (cost=0.00..0.01 rows=1 width=0)")],
            ]),
        );
        let b = rowset(
            vec![25],
            rows(&[&[Some("Result  (cost=0.00..0.01 rows=1 width=0)")]]),
        );
        let c = classify_sql(scroll_sql, &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("scroll-materialize".to_string()));
        // Same rowsets on a NON-scroll DECLARE: finding.
        let c = classify_sql(
            "explain declare c no scroll cursor for select ;",
            &a,
            &b,
        );
        assert_eq!(c.class, DiffClass::RowsetDiff);
        // VERBOSE attribute rows: wrap's Output row drops, child rows
        // de-indent, margin summary rows pass through untouched.
        let a = rowset(
            vec![25],
            rows(&[
                &[Some("Materialize (actual time=0.001..0.001 rows=1.00 loops=1)")],
                &[Some("  Output: 1")],
                &[Some("  ->  Result (actual time=0.000..0.000 rows=1.00 loops=1)")],
                &[Some("        Output: 1")],
                &[Some("Planning Time: 0.030 ms")],
                &[Some("Execution Time: 0.003 ms")],
            ]),
        );
        let b = rowset(
            vec![25],
            rows(&[
                &[Some("Result (actual time=0.002..0.005 rows=1.00 loops=1)")],
                &[Some("  Output: 1")],
                &[Some("Planning Time: 0.051 ms")],
                &[Some("Execution Time: 0.009 ms")],
            ]),
        );
        let scroll_analyze_sql =
            "explain analyse verbose declare close binary insensitive scroll \
             insensitive cursor for select 1 ;";
        // Wall-clock digits differ -> needs the timing opt-in (gramwalk).
        let c = classify(&DiffInput {
            sql: scroll_analyze_sql,
            a: &a,
            b: &b,
            ulp_tol: 4,
            soft_cols: &[],
            mask_explain_timing: true,
        });
        assert_eq!(c.class, DiffClass::Ruled("scroll-materialize".to_string()));
        // Residual structural diff under the wrap still escalates.
        let b2 = rowset(
            vec![25],
            rows(&[
                &[Some("Result (actual time=0.002..0.005 rows=1.00 loops=1)")],
                &[Some("  Output: 2")],
                &[Some("Planning Time: 0.051 ms")],
                &[Some("Execution Time: 0.009 ms")],
            ]),
        );
        let c = classify(&DiffInput {
            sql: scroll_analyze_sql,
            a: &a,
            b: &b2,
            ulp_tol: 4,
            soft_cols: &[],
            mask_explain_timing: true,
        });
        assert_eq!(c.class, DiffClass::RowsetDiff);
    }

    /// r20 update-rowcount (run 4ab3382e87..-59-13, gramwalk seed
    /// 3878502244648856050, "row count 8 vs 6"): the TEXT "Planning:"
    /// buffer-usage block prints only when planning touched a buffer —
    /// session cache state, not conformance (the same C server answers 8
    /// rows on a cold backend and 6 on a warm one). Its presence-only
    /// diff resolves as a ruled candidate; any residual structural diff
    /// still escalates.
    #[test]
    fn explain_planning_buffer_block_presence_is_ruled_candidate() {
        // A: cold backend — Planning: block present (8 rows).
        let a = rowset(
            vec![25],
            rows(&[
                &[Some(
                    "Update on fz_scalar  (cost=0.00..14.00 rows=0 width=0) \
                     (actual time=0.034..0.035 rows=0.00 loops=1)",
                )],
                &[Some("  Buffers: shared hit=17")],
                &[Some(
                    "  ->  Seq Scan on fz_scalar  (cost=0.00..14.00 rows=400 width=38) \
                     (actual time=0.005..0.006 rows=8.00 loops=1)",
                )],
                &[Some("        Buffers: shared hit=1")],
                &[Some("Planning:")],
                &[Some("  Buffers: shared hit=109")],
                &[Some("Planning Time: 0.231 ms")],
                &[Some("Execution Time: 0.351 ms")],
            ]),
        );
        // B: warm session — zero planning buffer touches, block absent
        // (6 rows).
        let b = rowset(
            vec![25],
            rows(&[
                &[Some(
                    "Update on fz_scalar  (cost=0.00..14.00 rows=0 width=0) \
                     (actual time=0.031..0.031 rows=0.00 loops=1)",
                )],
                &[Some("  Buffers: shared hit=17")],
                &[Some(
                    "  ->  Seq Scan on fz_scalar  (cost=0.00..14.00 rows=400 width=38) \
                     (actual time=0.004..0.004 rows=8.00 loops=1)",
                )],
                &[Some("        Buffers: shared hit=1")],
                &[Some("Planning Time: 0.114 ms")],
                &[Some("Execution Time: 0.076 ms")],
            ]),
        );
        let sql = "explain analyse update fz_scalar * fz_scalar set k_text = default ;";
        let c = classify(&DiffInput {
            sql,
            a: &a,
            b: &b,
            ulp_tol: 4,
            soft_cols: &[],
            mask_explain_timing: true,
        });
        assert_eq!(c.class, DiffClass::Ruled("explain-planning-buffers".to_string()));
        // Also resolves without the timing opt-in when the timing text
        // happens to match (plain EXPLAIN under BUFFERS).
        let a2 = rowset(
            vec![25],
            rows(&[
                &[Some("Seq Scan on t  (cost=0.00..1.00 rows=1 width=4)")],
                &[Some("Planning:")],
                &[Some("  Buffers: shared hit=5 read=2")],
            ]),
        );
        let b2 = rowset(
            vec![25],
            rows(&[&[Some("Seq Scan on t  (cost=0.00..1.00 rows=1 width=4)")]]),
        );
        let c = classify_sql("explain select * from t ;", &a2, &b2);
        assert_eq!(c.class, DiffClass::Ruled("explain-planning-buffers".to_string()));
        // Residual structural diff still escalates: dropping the block
        // must not hide a real plan-shape divergence.
        let b3 = rowset(
            vec![25],
            rows(&[&[Some("Index Scan using t_pkey on t  (cost=0.00..1.00 rows=1 width=4)")]]),
        );
        let c = classify_sql("explain select * from t ;", &a2, &b3);
        assert_eq!(c.class, DiffClass::RowsetDiff);
        // Non-EXPLAIN statements never receive the candidate.
        let c = classify_sql("select * from t ;", &a2, &b2);
        assert_eq!(c.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn planning_buffer_strip_shape() {
        let r = rows(&[
            &[Some("Result")],
            &[Some("Planning:")],
            &[Some("  Buffers: shared hit=3")],
            &[Some("  I/O Timings: shared read=0.1")],
            &[Some("  Memory: used=8kB  allocated=16kB")],
            &[Some("Planning Time: 0.1 ms")],
        ]);
        let stripped = strip_planning_buffer_rows(&r);
        assert_eq!(
            stripped,
            rows(&[&[Some("Result")], &[Some("Planning Time: 0.1 ms")]])
        );
        // A row spelled "Planning:" deeper in a plan tree strips with its
        // own children only; unrelated rows survive.
        let r = rows(&[
            &[Some("Planning:")],
            &[Some("  Buffers: shared hit=3")],
            &[Some("Execution Time: 1 ms")],
        ]);
        assert_eq!(
            strip_planning_buffer_rows(&r),
            rows(&[&[Some("Execution Time: 1 ms")]])
        );
        // Non-counter children terminate the block.
        let r = rows(&[
            &[Some("Planning:")],
            &[Some("  Something Else: 3")],
        ]);
        assert_eq!(
            strip_planning_buffer_rows(&r),
            rows(&[&[Some("  Something Else: 3")]])
        );
    }

    #[test]
    fn scroll_declare_gate_detection() {
        assert!(is_scroll_declare_explain_stmt(
            "explain verbose declare xmlforest scroll asensitive scroll binary cursor for select ;"
        ));
        assert!(is_scroll_declare_explain_stmt(
            "EXPLAIN (COSTS OFF) DECLARE c SCROLL CURSOR FOR SELECT 1;"
        ));
        // NO SCROLL is not SCROLL.
        assert!(!is_scroll_declare_explain_stmt(
            "explain declare c no scroll cursor for select ;"
        ));
        // A cursor NAMED "no" followed by SCROLL is SCROLL (round-14
        // gramwalk seed 4606538290375852325: the name token was scanned as
        // an option and swallowed the SCROLL that followed it).
        assert!(is_scroll_declare_explain_stmt(
            "explain ( analyze on ) declare no scroll cursor for select ;"
        ));
        // ... and a cursor merely NAMED "scroll" carries no option.
        assert!(!is_scroll_declare_explain_stmt(
            "explain declare scroll cursor for select 1 ;"
        ));
        // "scroll" in the query body is not an option.
        assert!(!is_scroll_declare_explain_stmt(
            "explain declare c cursor for select * from scroll ;"
        ));
        // Not a DECLARE at all.
        assert!(!is_scroll_declare_explain_stmt("explain select 1 ;"));
        // Not an EXPLAIN at all.
        assert!(!is_scroll_declare_explain_stmt(
            "declare c scroll cursor for select 1 ;"
        ));
    }

    /// H1: "actual time=" digits rule out only under the opt-in flag, and
    /// only when nothing else differs; plan-shape diffs stay findings.
    #[test]
    fn explain_timing_mask_is_opt_in_and_shape_strict() {
        let mk = |t: &str| {
            rowset(
                vec![25],
                rows(&[
                    &[Some(t)],
                    &[Some("Planning Time: 0.100 ms")],
                    &[Some("Execution Time: 0.200 ms")],
                ]),
            )
        };
        let a = mk("Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.003..0.004 rows=1.00 loops=1)");
        let b = mk("Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.011..0.190 rows=1.00 loops=1)");
        let sql = "explain analyze select 1 ;";
        // Without the opt-in: a finding (counter mask alone can't absorb it).
        let c = classify_sql(sql, &a, &b);
        assert_eq!(c.class, DiffClass::RowsetDiff);
        // With the opt-in: ruled candidate.
        let c = classify(&DiffInput {
            sql,
            a: &a,
            b: &b,
            ulp_tol: 4,
            soft_cols: &[],
            mask_explain_timing: true,
        });
        assert_eq!(c.class, DiffClass::Ruled("explain-timing".to_string()));
        // Plan-shape divergence is NOT absorbed even with the opt-in.
        let b2 = mk("Materialize  (cost=0.00..0.01 rows=1 width=4) (actual time=0.011..0.190 rows=1.00 loops=1)");
        let c = classify(&DiffInput {
            sql,
            a: &a,
            b: &b2,
            ulp_tol: 4,
            soft_cols: &[],
            mask_explain_timing: true,
        });
        assert_eq!(c.class, DiffClass::RowsetDiff);
    }

    fn classify_soft(
        sql: &str,
        a: &StmtOutcome,
        b: &StmtOutcome,
        soft_cols: &[usize],
    ) -> Classified {
        classify(&DiffInput { sql, a, b, ulp_tol: 4, soft_cols, mask_explain_timing: false })
    }

    #[test]
    fn identical_rowsets_match() {
        let a = rowset(vec![23], rows(&[&[Some("1")], &[Some("2")]]));
        let b = rowset(vec![23], rows(&[&[Some("1")], &[Some("2")]]));
        assert_eq!(classify_sql("SELECT c FROM t;", &a, &b).class, DiffClass::Match);
    }

    #[test]
    fn multiset_vs_ordered_compare() {
        let a = rowset(vec![23], rows(&[&[Some("1")], &[Some("2")]]));
        let b = rowset(vec![23], rows(&[&[Some("2")], &[Some("1")]]));
        // No ORDER BY: multiset compare, reordering is a match.
        assert_eq!(classify_sql("SELECT c FROM t;", &a, &b).class, DiffClass::Match);
        // ORDER BY: ordered compare fails, multiset succeeds -> tie-order
        // ruled candidate, not a raw finding.
        let c = classify_sql("SELECT c FROM t ORDER BY 1;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("tie-order".to_string()));
    }

    #[test]
    fn real_rowset_diff_flags() {
        let a = rowset(vec![23], rows(&[&[Some("1")]]));
        let b = rowset(vec![23], rows(&[&[Some("3")]]));
        assert_eq!(
            classify_sql("SELECT c FROM t ORDER BY 1;", &a, &b).class,
            DiffClass::RowsetDiff
        );
        assert_eq!(classify_sql("SELECT c FROM t;", &a, &b).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn null_vs_value_diffs() {
        let a = rowset(vec![25], rows(&[&[None]]));
        let b = rowset(vec![25], rows(&[&[Some("")]]));
        assert_eq!(classify_sql("SELECT c FROM t;", &a, &b).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn geo_composite_last_digit_float_is_ruled_candidate() {
        // soak-3 N1 witness shape: circle radius differing in the last digit
        // (1 ulp) inside the composite text — must resolve as the ruled
        // float-ulp candidate, not RowsetDiff.
        let a = rowset(vec![718], rows(&[&[Some("<(1,2),2.6925824035672523>")]]));
        let b = rowset(vec![718], rows(&[&[Some("<(1,2),2.692582403567252>")]]));
        let c = classify_sql("SELECT circle(p) FROM t;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("float-ulp".to_string()));
    }

    #[test]
    fn geo_composite_within_widened_budget_is_ruled_candidate() {
        // dist-chain accumulation up to ~15 ulp (soak-3 N1) fits the widened
        // geo budget (ulp_tol 4 * factor 8 = 32) inside a path composite.
        let x = 2.6925824035672523f64;
        let y = f64::from_bits(x.to_bits() + 15);
        let a = rowset(vec![602], rows(&[&[Some(&format!("(({x:?},1),(2,3))"))]]));
        let b = rowset(vec![602], rows(&[&[Some(&format!("(({y:?},1),(2,3))"))]]));
        let c = classify_sql("SELECT pth FROM t;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("float-ulp".to_string()));
    }

    #[test]
    fn geo_composite_real_diff_still_flags() {
        // Structure diff and beyond-budget numeric diff both stay findings.
        let a = rowset(vec![718], rows(&[&[Some("<(1,2),3>")]]));
        let b = rowset(vec![718], rows(&[&[Some("<(1,2),4>")]]));
        assert_eq!(classify_sql("SELECT c FROM t;", &a, &b).class, DiffClass::RowsetDiff);
        let a = rowset(vec![604], rows(&[&[Some("((0,0),(1,1))")]]));
        let b = rowset(vec![604], rows(&[&[Some("((0,0),(1,1),(2,2))")]]));
        assert_eq!(classify_sql("SELECT g FROM t;", &a, &b).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn geo_composite_nan_matches_nan_only() {
        let a = rowset(vec![600], rows(&[&[Some("(NaN,1)")]]));
        let b = rowset(vec![600], rows(&[&[Some("(NaN,1)")]]));
        // identical text: exact match fast path
        assert_eq!(classify_sql("SELECT p FROM t;", &a, &b).class, DiffClass::Match);
        let c = rowset(vec![600], rows(&[&[Some("(NaN,1)")]]));
        let d = rowset(vec![600], rows(&[&[Some("(0,1)")]]));
        assert_eq!(classify_sql("SELECT p FROM t;", &c, &d).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn float_ulp_within_tolerance_is_ruled_candidate() {
        let x = 0.1f64 + 0.2f64;
        let y = 0.3f64; // 1 ulp away from x
        let a = rowset(vec![FLOAT8_OID], rows(&[&[Some(&format!("{x:?}"))]]));
        let b = rowset(vec![FLOAT8_OID], rows(&[&[Some(&format!("{y:?}"))]]));
        let c = classify_sql("SELECT f FROM t ORDER BY 1;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("float-ulp".to_string()));
    }

    #[test]
    fn float_beyond_tolerance_diffs() {
        let a = rowset(vec![FLOAT8_OID], rows(&[&[Some("1.0")]]));
        let b = rowset(vec![FLOAT8_OID], rows(&[&[Some("1.001")]]));
        assert_eq!(
            classify_sql("SELECT f FROM t ORDER BY 1;", &a, &b).class,
            DiffClass::RowsetDiff
        );
    }

    #[test]
    fn soft_columns_accept_any_float_divergence() {
        // Far beyond any ulp tolerance: a ruled-soft column still agrees.
        let a = rowset(vec![FLOAT8_OID], rows(&[&[Some("1e-10")]]));
        let b = rowset(vec![FLOAT8_OID], rows(&[&[Some("0")]]));
        let c = classify_soft("SELECT sum(f) FROM t;", &a, &b, &[0]);
        assert_eq!(c.class, DiffClass::Ruled("float-agg".to_string()));
        // Without the soft mask the same divergence is a real finding.
        assert_eq!(
            classify_soft("SELECT sum(f) FROM t;", &a, &b, &[]).class,
            DiffClass::RowsetDiff
        );
        // NaN vs value agrees under soft (both are float values).
        let n = rowset(vec![FLOAT8_OID], rows(&[&[Some("NaN")]]));
        assert_eq!(
            classify_soft("SELECT sum(f) FROM t;", &a, &n, &[0]).class,
            DiffClass::Ruled("float-agg".to_string())
        );
        // NULL vs value stays a diff even in a soft column.
        let nul = rowset(vec![FLOAT8_OID], rows(&[&[None]]));
        assert_eq!(
            classify_soft("SELECT sum(f) FROM t;", &a, &nul, &[0]).class,
            DiffClass::RowsetDiff
        );
    }

    #[test]
    fn soft_mask_never_weakens_nonfloat_or_other_columns() {
        // Soft index pointing at a non-float column: exact compare stands.
        let a = rowset(vec![1700], rows(&[&[Some("1.0")]]));
        let b = rowset(vec![1700], rows(&[&[Some("1.00")]]));
        assert_eq!(
            classify_soft("SELECT sum(n) FROM t;", &a, &b, &[0]).class,
            DiffClass::RowsetDiff
        );
        // Soft on column 1 leaves column 0 float-ulp: beyond-ulp diff in
        // column 0 is still a finding.
        let a = rowset(
            vec![FLOAT8_OID, FLOAT8_OID],
            rows(&[&[Some("1.0"), Some("5.0")]]),
        );
        let b = rowset(
            vec![FLOAT8_OID, FLOAT8_OID],
            rows(&[&[Some("1.001"), Some("6.0")]]),
        );
        assert_eq!(
            classify_soft("SELECT f, sum(g) FROM t;", &a, &b, &[1]).class,
            DiffClass::RowsetDiff
        );
    }

    #[test]
    fn float_ulp_ignored_for_nonfloat_columns() {
        // Same numeric distance, but a numeric (1700) column: exact compare.
        let a = rowset(vec![1700], rows(&[&[Some("0.30000000000000004")]]));
        let b = rowset(vec![1700], rows(&[&[Some("0.3")]]));
        assert_eq!(
            classify_sql("SELECT n FROM t ORDER BY 1;", &a, &b).class,
            DiffClass::RowsetDiff
        );
    }

    #[test]
    fn float_specials() {
        let a = rowset(vec![FLOAT8_OID], rows(&[&[Some("NaN")], &[Some("Infinity")]]));
        let b = rowset(vec![FLOAT8_OID], rows(&[&[Some("NaN")], &[Some("Infinity")]]));
        assert_eq!(
            classify_sql("SELECT f FROM t ORDER BY 1;", &a, &b).class,
            DiffClass::Match
        );
        let c = rowset(vec![FLOAT8_OID], rows(&[&[Some("Infinity")]]));
        let d = rowset(vec![FLOAT8_OID], rows(&[&[Some("-Infinity")]]));
        assert_eq!(
            classify_sql("SELECT f FROM t ORDER BY 1;", &c, &d).class,
            DiffClass::RowsetDiff
        );
    }

    #[test]
    fn column_oid_mismatch_is_rowset_diff() {
        let a = rowset(vec![23], rows(&[&[Some("1")]]));
        let b = rowset(vec![20], rows(&[&[Some("1")]]));
        let c = classify_sql("SELECT c FROM t;", &a, &b);
        assert_eq!(c.class, DiffClass::RowsetDiff);
        assert!(c.detail.contains("column type oids"));
    }

    #[test]
    fn user_range_oids_match_under_one_consistent_offset() {
        // User-range descriptor OIDs are cluster-local allocator state; a
        // consistent per-resultset offset is a match, builtin columns
        // still compare exactly.
        let a = rowset(vec![23, 16385, 16401], rows(&[&[Some("1"), Some("x"), Some("y")]]));
        let b = rowset(vec![23, 16394, 16410], rows(&[&[Some("1"), Some("x"), Some("y")]]));
        let c = classify_sql("SELECT a, b, c FROM t;", &a, &b);
        assert_eq!(c.class, DiffClass::Match);
    }

    #[test]
    fn user_range_oids_with_inconsistent_offsets_still_match() {
        // Round-7 FP-3: after hours of independent allocation (crashes,
        // voided batches, concurrent DDL) per-type deltas diverge; user-
        // range pairs with user-range unconditionally.
        let a = rowset(vec![16385, 16401], rows(&[&[Some("x"), Some("y")]]));
        let b = rowset(vec![16394, 16411], rows(&[&[Some("x"), Some("y")]]));
        let c = classify_sql("SELECT b, c FROM t;", &a, &b);
        assert_eq!(c.class, DiffClass::Match);
    }

    #[test]
    fn user_range_oid_against_builtin_is_rowset_diff() {
        let a = rowset(vec![16385], rows(&[&[Some("x")]]));
        let b = rowset(vec![25], rows(&[&[Some("x")]]));
        let c = classify_sql("SELECT b FROM t;", &a, &b);
        assert_eq!(c.class, DiffClass::RowsetDiff);
    }

    #[test]
    fn sqlstate_compare() {
        let e1 = StmtOutcome::Error {
            sqlstate: "22012".to_string(),
            message: "division by zero".to_string(),
        };
        let e2 = StmtOutcome::Error {
            sqlstate: "22012".to_string(),
            message: "division by zero somewhere".to_string(),
        };
        let e3 = StmtOutcome::Error {
            sqlstate: "22003".to_string(),
            message: "out of range".to_string(),
        };
        // Same SQLSTATE, different message text: MATCH.
        assert_eq!(classify_sql("SELECT 1/0;", &e1, &e2).class, DiffClass::Match);
        assert_eq!(classify_sql("SELECT 1/0;", &e1, &e3).class, DiffClass::ErrorDiff);
        let ok = rowset(vec![23], rows(&[&[Some("1")]]));
        assert_eq!(classify_sql("SELECT 1;", &e1, &ok).class, DiffClass::ErrorDiff);
        assert_eq!(classify_sql("SELECT 1;", &ok, &e1).class, DiffClass::ErrorDiff);
    }

    #[test]
    fn count_diff() {
        let a = StmtOutcome::Command { tag: "UPDATE 3".to_string(), affected: Some(3) };
        let b = StmtOutcome::Command { tag: "UPDATE 2".to_string(), affected: Some(2) };
        assert_eq!(classify_sql("UPDATE t SET c = 1;", &a, &b).class, DiffClass::CountDiff);
        let c = StmtOutcome::Command { tag: "UPDATE 3".to_string(), affected: Some(3) };
        assert_eq!(classify_sql("UPDATE t SET c = 1;", &a, &c).class, DiffClass::Match);
    }

    #[test]
    fn session_diverged_records_side() {
        let ok = rowset(vec![23], rows(&[&[Some("1")]]));
        let lost = StmtOutcome::ConnLost { detail: "server closed".to_string() };
        assert_eq!(
            classify_sql("SELECT 1;", &lost, &ok).class,
            DiffClass::SessionDiverged(Side::A)
        );
        assert_eq!(
            classify_sql("SELECT 1;", &ok, &lost).class,
            DiffClass::SessionDiverged(Side::B)
        );
        assert_eq!(
            classify_sql("SELECT 1;", &lost, &lost).class,
            DiffClass::SessionDiverged(Side::Both)
        );
    }

    #[test]
    fn copy_out_byte_compare() {
        let co = |bytes: &[u8], tag: &str| StmtOutcome::CopyOut {
            bytes: bytes.to_vec(),
            tag: tag.to_string(),
        };
        let sql = "COPY t TO STDOUT (FORMAT binary);";
        // Identical payload + tag: match.
        assert_eq!(
            classify_sql(sql, &co(b"PGCOPY\n\xff\x0d\x0a\x00abc", "COPY 2"),
                         &co(b"PGCOPY\n\xff\x0d\x0a\x00abc", "COPY 2")).class,
            DiffClass::Match
        );
        // One byte off: RowsetDiff with offset detail.
        let c = classify_sql(sql, &co(b"PGCOPY\n\xff\x0d\x0a\x00abc", "COPY 2"),
                             &co(b"PGCOPY\n\xff\x0d\x0a\x00abd", "COPY 2"));
        assert_eq!(c.class, DiffClass::RowsetDiff);
        assert!(c.detail.contains("differ at byte 13"), "{}", c.detail);
        // Prefix relationship: diverges at the shorter length.
        let c = classify_sql(sql, &co(b"PGCOPY", "COPY 2"), &co(b"PGCOPY\n", "COPY 2"));
        assert_eq!(c.class, DiffClass::RowsetDiff);
        assert!(c.detail.contains("len 6 vs 7"), "{}", c.detail);
        // Same bytes, different tag: CountDiff.
        let c = classify_sql(sql, &co(b"x", "COPY 2"), &co(b"x", "COPY 3"));
        assert_eq!(c.class, DiffClass::CountDiff);
        // COPY vs non-COPY shapes.
        let cmd = StmtOutcome::Command { tag: "COPY 2".to_string(), affected: Some(2) };
        assert_eq!(classify_sql(sql, &co(b"x", "COPY 2"), &cmd).class, DiffClass::RowsetDiff);
        assert_eq!(classify_sql(sql, &cmd, &co(b"x", "COPY 2")).class, DiffClass::RowsetDiff);
        // Error on one side stays an ERROR_DIFF.
        let err = StmtOutcome::Error {
            sqlstate: "42601".to_string(),
            message: "m".to_string(),
        };
        assert_eq!(classify_sql(sql, &err, &co(b"x", "COPY 2")).class, DiffClass::ErrorDiff);
    }

    #[test]
    fn tag_affected_parses() {
        assert_eq!(tag_affected("UPDATE 3"), Some(3));
        assert_eq!(tag_affected("INSERT 0 1"), Some(1));
        assert_eq!(tag_affected("SELECT 12"), Some(12));
        assert_eq!(tag_affected("BEGIN"), None);
        assert_eq!(tag_affected("CREATE TABLE"), None);
    }

    #[test]
    fn explain_counter_masking() {
        // TEXT format: value runs end at double-space or end-of-cell;
        // "Sort Method" masks to end-of-line (the method decides whether
        // Memory or Disk follows).
        assert_eq!(
            normalize_explain_cell("Sort Method: quicksort  Memory: 25kB"),
            "Sort Method: X"
        );
        assert_eq!(
            normalize_explain_cell("Sort Method: external merge  Disk: 48kB"),
            "Sort Method: X"
        );
        assert_eq!(
            normalize_explain_cell(
                "Buckets: 1024 (originally 1024)  Batches: 1 (originally 1)  Memory Usage: 9kB"
            ),
            "Buckets: X  Batches: X  Memory Usage: X"
        );
        // JSON format: quoted keys; Sort Method masks to end-of-line,
        // numeric counter values to the comma.
        assert_eq!(
            normalize_explain_cell("\"Sort Method\": \"external merge\",\n\"Sort Space Used\": 2408,"),
            "\"Sort Method\": X\n\"Sort Space Used\": X,"
        );
        // Not a counter context: "rows=" and plan node text untouched.
        assert_eq!(
            normalize_explain_cell("Seq Scan on fz_scalar t0 (actual rows=5 loops=1)"),
            "Seq Scan on fz_scalar t0 (actual rows=5 loops=1)"
        );
        // Left word boundary: "NotMemory: 3" is not the Memory token.
        assert_eq!(normalize_explain_cell("NotMemory: 3"), "NotMemory: 3");
        // Tuplestore lines (U1-F2): the high-water mark differs across
        // allocators; "Maximum Storage" wins over the shorter "Storage".
        assert_eq!(
            normalize_explain_cell("Storage: Memory  Maximum Storage: 17kB"),
            "Storage: X  Maximum Storage: X"
        );
        assert_eq!(
            normalize_explain_cell("\"Storage\": \"Memory\",\n\"Maximum Storage\": 17,"),
            "\"Storage\": X,\n\"Maximum Storage\": X,"
        );
        // Buffer-usage lines (G2): TEXT masks the whole tail (which
        // name=NN pairs print depends on which counters are nonzero);
        // multi-line cells keep per-line structure.
        assert_eq!(
            normalize_explain_cell("Buffers: shared hit=4 read=2 dirtied=1, temp read=5 written=6"),
            "Buffers: X"
        );
        assert_eq!(
            normalize_explain_cell("   Buffers: shared hit=120\n   ->  Seq Scan on t"),
            "   Buffers: X\n   ->  Seq Scan on t"
        );
        assert_eq!(
            normalize_explain_cell("I/O Timings: shared read=0.05 write=0.01"),
            "I/O Timings: X"
        );
        // JSON/YAML block counters mask value-wise; every key survives.
        assert_eq!(
            normalize_explain_cell("\"Shared Hit Blocks\": 111,\n\"Temp Read Blocks\": 0,"),
            "\"Shared Hit Blocks\": X,\n\"Temp Read Blocks\": X,"
        );
        assert_eq!(
            normalize_explain_cell("Shared Hit Blocks: 1\nShared Read Blocks: 0"),
            "Shared Hit Blocks: X\nShared Read Blocks: X"
        );
        // "Buffers" left-boundary: a plan node name containing the word
        // without the colon separator stays untouched.
        assert_eq!(normalize_explain_cell("SharedBuffers: 3"), "SharedBuffers: 3");
        // LD4 additions — WAL usage: TEXT tail masks to end-of-line
        // (which counters print is runtime state), JSON keys value-wise.
        assert_eq!(
            normalize_explain_cell("WAL: records=22 bytes=1441\n->  Seq Scan on t"),
            "WAL: X\n->  Seq Scan on t"
        );
        assert_eq!(
            normalize_explain_cell("\"WAL Records\": 22,\n\"WAL FPI\": 0,\n\"WAL Bytes\": 1441,"),
            "\"WAL Records\": X,\n\"WAL FPI\": X,\n\"WAL Bytes\": X,"
        );
        // Planning/execution summary + planner memory (SUMMARY ON /
        // MEMORY arms): values masked, line presence compared. The TEXT
        // planning-memory line is one end-of-line value tail.
        assert_eq!(
            normalize_explain_cell("Planning Time: 0.005 ms"),
            "Planning Time: X"
        );
        assert_eq!(
            normalize_explain_cell("Execution Time: 0.019 ms"),
            "Execution Time: X"
        );
        assert_eq!(
            normalize_explain_cell("Memory: used=10kB  allocated=16kB"),
            "Memory: X"
        );
        assert_eq!(
            normalize_explain_cell("\"Memory Used\": 10,\n\"Memory Allocated\": 16"),
            "\"Memory Used\": X,\n\"Memory Allocated\": X"
        );
    }

    #[test]
    fn explain_counter_only_diff_is_ruled_candidate() {
        let mk = |lines: &[&str]| {
            rowset(vec![25], lines.iter().map(|l| vec![Some(l.to_string())]).collect())
        };
        let sql = "EXPLAIN (COSTS OFF, SUMMARY OFF, ANALYZE, TIMING OFF, BUFFERS OFF) \
                   SELECT t0.s_int4 FROM fz_scalar t0;";
        let a = mk(&["Sort (actual rows=5 loops=1)", "  Sort Method: quicksort  Memory: 25kB"]);
        let b = mk(&["Sort (actual rows=5 loops=1)", "  Sort Method: external merge  Disk: 48kB"]);
        // Counter-only divergence: ruled candidate, resolved by the table.
        let c = classify_sql(sql, &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("explain-counter".to_string()));
        // Structural divergence stays a real finding even on EXPLAIN.
        let s = mk(&["Index Scan using fz_pk on fz_scalar t0 (actual rows=5 loops=1)"]);
        assert_eq!(classify_sql(sql, &a, &s).class, DiffClass::RowsetDiff);
        // The same counter rows on a non-EXPLAIN statement: real finding.
        assert_eq!(
            classify_sql("SELECT c FROM t;", &a, &b).class,
            DiffClass::RowsetDiff
        );
    }

    /// Round-7 FP-2: user-range `'<n>'::oid` literals in deparse text are
    /// masked; builtin OID literals and any other text diff still flag.
    #[test]
    fn oid_literal_diff_is_ruled_candidate() {
        let sql = "select pg_get_partition_constraintdef(oid) from pg_class ;";
        let a = rowset(
            vec![25],
            rows(&[&[Some("satisfies_hash_partition('48594'::oid, 8, 5, k)")]]),
        );
        let b = rowset(
            vec![25],
            rows(&[&[Some("satisfies_hash_partition('37725'::oid, 8, 5, k)")]]),
        );
        let c = classify_sql(sql, &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("oid-literal".to_string()));
        // Builtin OID literal: stays a finding.
        let a = rowset(vec![25], rows(&[&[Some("f('23'::oid)")]]));
        let b = rowset(vec![25], rows(&[&[Some("f('25'::oid)")]]));
        assert_eq!(classify_sql(sql, &a, &b).class, DiffClass::RowsetDiff);
        // Text differing beyond the oid literal: stays a finding.
        let a = rowset(vec![25], rows(&[&[Some("f('48594'::oid, 8)")]]));
        let b = rowset(vec![25], rows(&[&[Some("f('37725'::oid, 9)")]]));
        assert_eq!(classify_sql(sql, &a, &b).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn oid_literal_cell_normalization() {
        assert_eq!(
            normalize_oid_literal_cell("satisfies_hash_partition('48594'::oid, 8)").as_deref(),
            Some("satisfies_hash_partition('<oid>'::oid, 8)")
        );
        // Builtin value: unchanged (None).
        assert_eq!(normalize_oid_literal_cell("x('123'::oid)"), None);
        // Not a quoted literal: unchanged.
        assert_eq!(normalize_oid_literal_cell("48594'::oid"), None);
        assert_eq!(normalize_oid_literal_cell("no oid here"), None);
    }

    /// Round-7 FP-5: embedded user-range type OIDs in record/array binary
    /// images are masked; any other byte diff stays a finding.
    #[test]
    fn binary_udt_oid_diff_is_ruled_candidate() {
        // record_send image, 2 text-ish columns, differing only in the
        // (user-range) column type oids: ncols=2, per col typoid/len/data.
        let rec = |typoid: u32| {
            let mut b = Vec::new();
            b.extend_from_slice(&2i32.to_be_bytes());
            for data in [b"ab".as_slice(), b"ok".as_slice()] {
                b.extend_from_slice(&typoid.to_be_bytes());
                b.extend_from_slice(&(data.len() as i32).to_be_bytes());
                b.extend_from_slice(data);
            }
            let mut s = String::from("\\x");
            for byte in &b {
                s.push_str(&format!("{byte:02x}"));
            }
            s
        };
        let sql = "select row('ab','ok')::fz_udt_c_0 ;";
        let a = rowset(vec![16389], rows(&[&[Some(&rec(16401))]]));
        let b = rowset(vec![16410], rows(&[&[Some(&rec(37725))]]));
        let c = classify_sql(sql, &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("binary-udt-oid".to_string()), "{}", c.detail);
        // array_send image of 2 int-ish elements: only elemtype differs.
        let arr = |elemtype: u32| {
            let mut b = Vec::new();
            b.extend_from_slice(&1i32.to_be_bytes()); // ndim
            b.extend_from_slice(&0i32.to_be_bytes()); // flags
            b.extend_from_slice(&elemtype.to_be_bytes());
            b.extend_from_slice(&2i32.to_be_bytes()); // dim
            b.extend_from_slice(&1i32.to_be_bytes()); // lbound
            for v in [5i32, 5i32] {
                b.extend_from_slice(&4i32.to_be_bytes());
                b.extend_from_slice(&v.to_be_bytes());
            }
            let mut s = String::from("\\x");
            for byte in &b {
                s.push_str(&format!("{byte:02x}"));
            }
            s
        };
        let a = rowset(vec![16390], rows(&[&[Some(&arr(16400))]]));
        let b = rowset(vec![16411], rows(&[&[Some(&arr(37700))]]));
        let c = classify_sql("select array[5,5]::fz_udt_d_0[] ;", &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("binary-udt-oid".to_string()), "{}", c.detail);
        // A DATA byte differing alongside the oid: stays a finding.
        let mut bad = rec(37725);
        let fixed = bad.len() - 1;
        bad.replace_range(fixed.., "f");
        let a = rowset(vec![16389], rows(&[&[Some(&rec(16401))]]));
        let b = rowset(vec![16410], rows(&[&[Some(&bad)]]));
        assert_eq!(classify_sql(sql, &a, &b).class, DiffClass::RowsetDiff);
        // Builtin element type: images with different builtin oids are a
        // real finding (nothing user-range to mask).
        let a = rowset(vec![1007], rows(&[&[Some(&arr(23))]]));
        let b = rowset(vec![1007], rows(&[&[Some(&arr(20))]]));
        assert_eq!(
            classify_sql("select x from t ;", &a, &b).class,
            DiffClass::RowsetDiff
        );
    }

    /// Round-7 FP-6: direct *cmp() call, int4 results equal in sign only.
    #[test]
    fn cmp_magnitude_diff_is_ruled_candidate() {
        let sql = "select uuid_cmp(a, b) from t ;";
        let a = rowset(vec![23], rows(&[&[Some("-238")]]));
        let b = rowset(vec![23], rows(&[&[Some("-1")]]));
        let c = classify_sql(sql, &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("cmp-magnitude".to_string()));
        // Sign flip: stays a finding.
        let b2 = rowset(vec![23], rows(&[&[Some("238")]]));
        assert_eq!(classify_sql(sql, &a, &b2).class, DiffClass::RowsetDiff);
        // No *cmp call in the statement: stays a finding.
        let c = classify_sql("select a - b from t ;", &a, &b);
        assert_eq!(c.class, DiffClass::RowsetDiff);
        // Non-int4 column: exact compare stands even with a *cmp call.
        let a8 = rowset(vec![20], rows(&[&[Some("-238")]]));
        let b8 = rowset(vec![20], rows(&[&[Some("-1")]]));
        assert_eq!(classify_sql(sql, &a8, &b8).class, DiffClass::RowsetDiff);
    }

    /// Round-13: the same FP-6 shape under xproto binary result format —
    /// int4 cells arrive as 4-byte `\x`-hex images (run
    /// 72b2e74701d0e1310d59d39345e716da-59-13, seeds 451588928933415389 /
    /// 1836375092275758948: glibc-amd64 uuid_cmp answered \xffffff60 =
    /// -160 where pgrust answered \xffffffff = -1).
    #[test]
    fn cmp_magnitude_diff_is_ruled_for_binary_cells() {
        let sql = "select uuid_cmp(a, b) from t ;";
        let a = rowset(vec![23], rows(&[&[Some("\\xffffff60")]]));
        let b = rowset(vec![23], rows(&[&[Some("\\xffffffff")]]));
        let c = classify_sql(sql, &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("cmp-magnitude".to_string()));
        // Sign flip in binary form: stays a finding.
        let b2 = rowset(vec![23], rows(&[&[Some("\\x000000ee")]]));
        assert_eq!(classify_sql(sql, &a, &b2).class, DiffClass::RowsetDiff);
        // Mixed text/binary cells still agree in sign: ruled.
        let bt = rowset(vec![23], rows(&[&[Some("-1")]]));
        assert_eq!(
            classify_sql(sql, &a, &bt).class,
            DiffClass::Ruled("cmp-magnitude".to_string())
        );
        // A non-int4-sized hex image is left alone: finding.
        let a5 = rowset(vec![23], rows(&[&[Some("\\xffffff60aa")]]));
        let b5 = rowset(vec![23], rows(&[&[Some("\\xffffffffaa")]]));
        assert_eq!(classify_sql(sql, &a5, &b5).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn calls_cmp_builtin_scans_words() {
        assert!(calls_cmp_builtin("select uuid_cmp(a,b);"));
        assert!(calls_cmp_builtin("select btint4cmp (1, 2);"));
        assert!(!calls_cmp_builtin("select cmp(1,2);")); // too short
        assert!(!calls_cmp_builtin("select uuid_cmp;")); // no call
        assert!(!calls_cmp_builtin("select compare(a,b);"));
    }

    /// Round-7 FP-4: B-only 55000 with the exact worker-init message is a
    /// ruled candidate; other messages and A-side errors escalate.
    #[test]
    fn parallel_worker_init_is_ruled_candidate() {
        let ok = rowset(vec![23], rows(&[&[Some("1")]]));
        let err = |m: &str| StmtOutcome::Error {
            sqlstate: "55000".to_string(),
            message: m.to_string(),
        };
        let c = classify_sql("select count(*) from big ;", &ok,
                             &err("parallel worker failed to initialize"));
        assert_eq!(c.class, DiffClass::Ruled("parallel-worker-init".to_string()));
        // Different 55000 message: finding.
        let c = classify_sql("select 1 ;", &ok, &err("object not in prerequisite state"));
        assert_eq!(c.class, DiffClass::ErrorDiff);
        // A-side worker-init error (the UNfaulted oracle failing): finding.
        let c = classify_sql("select 1 ;", &err("parallel worker failed to initialize"), &ok);
        assert_eq!(c.class, DiffClass::ErrorDiff);
    }

    /// Round-20 FP-4 sibling: B-only 57014 with the exact
    /// statement_timeout message is a ruled candidate; other 57014
    /// messages and A-side timeouts escalate.
    #[test]
    fn fault_stmt_timeout_is_ruled_candidate() {
        let ok = rowset(vec![23], rows(&[&[Some("1")]]));
        let err = |m: &str| StmtOutcome::Error {
            sqlstate: "57014".to_string(),
            message: m.to_string(),
        };
        let c = classify_sql("SELECT * FROM fz_par_0 ORDER BY pk;", &ok,
                             &err("canceling statement due to statement timeout"));
        assert_eq!(c.class, DiffClass::Ruled("fault-stmt-timeout".to_string()));
        // Different 57014 message (user cancel): finding.
        let c = classify_sql("select 1 ;", &ok, &err("canceling statement due to user request"));
        assert_eq!(c.class, DiffClass::ErrorDiff);
        // A-side timeout (the UNfaulted oracle stalling): finding.
        let c =
            classify_sql("select 1 ;", &err("canceling statement due to statement timeout"), &ok);
        assert_eq!(c.class, DiffClass::ErrorDiff);
    }

    /// Round-20: B-only 40P01 on a DROP TABLE is a ruled candidate
    /// (autovacuum-deadlock symmetric race); other statements and other
    /// messages escalate.
    #[test]
    fn drop_autovacuum_deadlock_is_ruled_candidate() {
        let ok = rowset(vec![23], rows(&[&[Some("1")]]));
        let err = StmtOutcome::Error {
            sqlstate: "40P01".to_string(),
            message: "deadlock detected".to_string(),
        };
        let c = classify_sql("DROP TABLE fz_pa_t1, fz_pa_t2, fz_pa_t3;", &ok, &err);
        assert_eq!(c.class, DiffClass::Ruled("drop-autovacuum-deadlock".to_string()));
        // Non-DROP-TABLE statement: finding.
        let c = classify_sql("update fz_one set k_int = 2 ;", &ok, &err);
        assert_eq!(c.class, DiffClass::ErrorDiff);
        // A-side deadlock: finding.
        let c = classify_sql("DROP TABLE fz_pa_ml;", &err, &ok);
        assert_eq!(c.class, DiffClass::ErrorDiff);
    }

    /// Round-8: user-range pg_toast_<n> names mask; builtin catalog toast
    /// names and any other text diff still flag.
    #[test]
    fn toast_name_diff_is_ruled_candidate() {
        let sql = "select relname from pg_class where relkind = 't' ;";
        let a = rowset(vec![19], rows(&[&[Some("pg_toast_48594")]]));
        let b = rowset(vec![19], rows(&[&[Some("pg_toast_37725")]]));
        let c = classify_sql(sql, &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("toast-name".to_string()));
        // Schema-qualified and _index forms mask too.
        let a = rowset(vec![25], rows(&[&[Some("pg_toast.pg_toast_48594_index")]]));
        let b = rowset(vec![25], rows(&[&[Some("pg_toast.pg_toast_37725_index")]]));
        let c = classify_sql(sql, &a, &b);
        assert_eq!(c.class, DiffClass::Ruled("toast-name".to_string()));
        // Builtin catalog toast relids stay compared exactly.
        let a = rowset(vec![19], rows(&[&[Some("pg_toast_2619")]]));
        let b = rowset(vec![19], rows(&[&[Some("pg_toast_2620")]]));
        assert_eq!(classify_sql(sql, &a, &b).class, DiffClass::RowsetDiff);
        // A diff beyond the toast name stays a finding.
        let a = rowset(vec![25], rows(&[&[Some("pg_toast_48594 ok")]]));
        let b = rowset(vec![25], rows(&[&[Some("pg_toast_37725 no")]]));
        assert_eq!(classify_sql(sql, &a, &b).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn toast_name_cell_normalization() {
        assert_eq!(
            normalize_toast_name_cell("pg_toast.pg_toast_48594_index").as_deref(),
            Some("pg_toast.pg_toast_<oid>_index")
        );
        // Builtin relid: unchanged (None).
        assert_eq!(normalize_toast_name_cell("pg_toast_2619"), None);
        // No digits / not a toast name / mid-word: unchanged.
        assert_eq!(normalize_toast_name_cell("pg_toast_x"), None);
        assert_eq!(normalize_toast_name_cell("not_pg_toast_48594"), None);
        assert_eq!(normalize_toast_name_cell("plain text"), None);
    }

    #[test]
    fn ulp_distance_sane() {
        assert_eq!(ulp_distance(1.0, 1.0), 0);
        assert_eq!(ulp_distance(1.0, f64::from_bits(1.0f64.to_bits() + 1)), 1);
        // Across the sign boundary.
        let tiny = f64::from_bits(1);
        assert_eq!(ulp_distance(-tiny, tiny), 2);
        assert_eq!(ulp_distance(0.0, -0.0), 0);
        assert!(ulp_distance(1.0, 2.0) > 1_000_000);
    }
}
