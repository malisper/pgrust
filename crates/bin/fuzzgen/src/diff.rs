//! Plane comparator and divergence classification (plan §4.1).
//!
//! `compare_planes` canonicalizes both sides' `ObservationRecord`
//! (`crate::canon`) and compares plane by plane — `wire:E`, `wire:N`,
//! `rows`, `explain`, `meta`, `tag`, `notify`, `copy`, `log`,
//! `probe:<deck>`, plus the liveness planes `session` / `panic` /
//! `crash` / `hang` — producing one `Divergence` per plane whose class is
//! decided by the first differing field. Masks come from the ledger
//! (`crate::rulings`, `docs/fuzzing/rulings.toml`) plus the structural
//! masks the ledger names (float ulp, EXPLAIN counters, user-OID
//! literals, ...); a ruled hit is still recorded, with `ruled:<id>`.
//! Rows compare ordered iff the step's `ordered: total`; a multiset-equal
//! reorder under a total key is `wrong-result`, never tie-order.
//!
//! The legacy entry point `classify(&DiffInput)` (per-statement
//! `StmtOutcome`s, used by runner/reduce/copy*/dbddl/diffrunner) is an
//! adapter over the same comparator: outcomes become a synthetic wire,
//! `ordered` is `partial` when the statement carries ORDER BY (the only
//! thing a caller without a StepRecord can assert) and `none` otherwise,
//! and the plane verdict maps back onto `DiffClass`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use crate::canon::{self, CanonCtx, CanonSide, ErrTuple, ResultGroup, ERR_FIELD_ORDER};
use crate::contracts::{self, json, Class, ColDesc, ObservationRecord, Ordered, Severity, Status, StepKind, StepRecord, WireMsg};
use crate::rulings::{normalize_stmt, Candidate, Ledger};

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

// Column-type-oid equivalence (builtin exact, user-range pairs with
// user-range) now lives in canon::canon_typ and the meta plane.

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

/// Statement-level ORDER BY detection over the rendered SQL. No longer
/// decides ordered-vs-multiset compare (that is the step's `ordered`
/// field, plan §4.1); it survives only as the legacy adapter's mapping
/// of a bare statement onto `Ordered::Partial` (ordered compare with the
/// tie-order ruling in play) versus `Ordered::None`.
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

// ---------------------------------------------------------------------
// Plane comparator (plan §4.1)
// ---------------------------------------------------------------------

/// Compare options beyond the two records: float ulp budget, the
/// generator's soft-float columns, the H1 EXPLAIN timing opt-in, and the
/// per-side canonicalization contexts (libdir / pgdata / type names).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompareOpts {
    pub ulp_tol: u64,
    pub soft_cols: Vec<usize>,
    pub mask_explain_timing: bool,
    pub ctx_a: CanonCtx,
    pub ctx_b: CanonCtx,
}

impl Default for CompareOpts {
    fn default() -> CompareOpts {
        CompareOpts {
            ulp_tol: 4,
            soft_cols: Vec::new(),
            mask_explain_timing: false,
            ctx_a: CanonCtx::default(),
            ctx_b: CanonCtx::default(),
        }
    }
}

/// One plane's divergence for one step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Divergence {
    /// `wire:E` | `wire:N` | `rows` | `explain` | `meta` | `tag` | `notify`
    /// | `copy` | `log` | `probe:<deck>` | `session` | `panic` | `crash` | `hang`.
    pub plane: String,
    pub class: Class,
    pub severity: Severity,
    /// The first differing field (or the structural mask that absorbed the diff).
    pub field: String,
    /// Every differing field, first-differing order.
    pub differing: Vec<String>,
    /// Canonical value of `field` on each side (None = absent).
    pub a: Option<String>,
    pub b: Option<String>,
    /// SQLSTATE pair behind the signature (`-` when absent).
    pub sqlstate_a: Option<String>,
    pub sqlstate_b: Option<String>,
    pub detail: String,
    /// `plane | unit-or-message-template | sqlstate pair | field delta`.
    pub signature: String,
    /// Ledger id when a ruling covers it (still recorded).
    pub rule: Option<String>,
    /// `NEW`, or `RULED` when `rule` is set.
    pub status: Status,
}

impl Divergence {
    pub fn is_ruled(&self) -> bool {
        self.rule.is_some()
    }

    /// `ruled:<id>` when ruled (the findings.jsonl marker), else the class.
    pub fn triage(&self) -> String {
        match &self.rule {
            Some(id) => format!("ruled:{}", id),
            None => self.class.as_str().to_string(),
        }
    }

    pub fn to_json(&self) -> json::Value {
        json::Value::obj()
            .with("plane", json::Value::from(self.plane.as_str()))
            .with("class", json::Value::from(self.class.as_str()))
            .with("severity", json::Value::from(self.severity.as_str()))
            .with("field", json::Value::from(self.field.as_str()))
            .with("differing", json::str_arr(&self.differing))
            .with("a", json::opt(self.a.as_deref()))
            .with("b", json::opt(self.b.as_deref()))
            .with("sqlstate_a", json::opt(self.sqlstate_a.as_deref()))
            .with("sqlstate_b", json::opt(self.sqlstate_b.as_deref()))
            .with("detail", json::Value::from(self.detail.as_str()))
            .with("signature", json::Value::from(self.signature.as_str()))
            .with("rule", json::opt(self.rule.as_deref()))
            .with("status", json::Value::from(self.status.as_str()))
            .with("triage", json::Value::Str(self.triage()))
    }
}

/// Precedence rank (VOCABULARY.md "Rules of precedence"): lower wins.
pub fn class_rank(c: Class) -> u8 {
    match c {
        Class::Crash => 0,
        Class::Hang => 1,
        Class::MissingError | Class::SpuriousError => 2,
        Class::WrongSqlstate => 3,
        Class::WrongResult => 4,
        Class::WrongMessage => 5,
        Class::WrongPosition => 6,
        Class::PlanShape => 7,
        Class::MissingFeature => 8,
        Class::Cosmetic => 9,
        Class::Leak | Class::AllocPolicy => 10,
    }
}

/// Default severity per class (VOCABULARY.md table).
pub fn default_severity(c: Class) -> Severity {
    match c {
        Class::Crash => Severity::Critical,
        Class::MissingError | Class::SpuriousError | Class::WrongResult | Class::WrongSqlstate | Class::Hang => {
            Severity::High
        }
        Class::WrongMessage | Class::WrongPosition | Class::PlanShape | Class::MissingFeature => Severity::Medium,
        Class::Cosmetic => Severity::Low,
        Class::Leak => Severity::High,
        Class::AllocPolicy => Severity::Medium,
    }
}

const PLANE_ORDER: &[&str] =
    &["session", "panic", "crash", "hang", "wire:E", "wire:N", "rows", "explain", "copy", "meta", "notify", "log"];

pub fn plane_order(plane: &str) -> usize {
    PLANE_ORDER.iter().position(|p| *p == plane).unwrap_or(PLANE_ORDER.len())
}

/// `plane | unit-or-message-template | sqlstate pair | field delta`.
pub fn signature(plane: &str, unit: &str, sa: Option<&str>, sb: Option<&str>, delta: &str) -> String {
    format!("{} | {} | {}/{} | {}", plane, unit, sa.unwrap_or("-"), sb.unwrap_or("-"), delta)
}

/// The step's verdict: the highest-precedence unruled divergence, else
/// the highest ruled one, else None (a match).
pub fn step_verdict(divs: &[Divergence]) -> Option<&Divergence> {
    divs.iter().find(|d| !d.is_ruled()).or_else(|| divs.first())
}

/// The explicit missing-feature text rule on a B-side message: the
/// gramwalk grammar fence, `not yet implemented`, `unported`,
/// `not supported`. The generic phrases apply only when A's message (if
/// any) does not carry the same phrase — then the wording differs and
/// it is a wrong-message; the fence always applies.
pub fn missing_feature_rule(mb: &str, ma: Option<&str>) -> Option<String> {
    if let Some(rest) = mb.split("not yet implemented (grammar rule ").nth(1) {
        let rule: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        return Some(format!("UNPORTED grammar rule {}", rule));
    }
    let lower = mb.to_ascii_lowercase();
    for phrase in ["not yet implemented", "unported", "not supported"] {
        if lower.contains(phrase) && !ma.is_some_and(|m| m.to_ascii_lowercase().contains(phrase)) {
            return Some(format!("missing-feature text rule ({})", phrase));
        }
    }
    None
}

/// Compare two sides of one step with default options.
pub fn compare_planes(a: &ObservationRecord, b: &ObservationRecord, step: &StepRecord, rulings: &Ledger) -> Vec<Divergence> {
    compare_planes_with(a, b, step, rulings, &CompareOpts::default())
}

/// Compare two sides of one step; divergences come back sorted by class
/// precedence, then plane order, one per plane.
pub fn compare_planes_with(
    a: &ObservationRecord,
    b: &ObservationRecord,
    step: &StepRecord,
    rulings: &Ledger,
    opts: &CompareOpts,
) -> Vec<Divergence> {
    let ca = canon::canonicalize(a, &opts.ctx_a);
    let cb = canon::canonicalize(b, &opts.ctx_b);
    let cmp = Cmp { ca: &ca, cb: &cb, step, opts, ledger: rulings, sql: step.sql.clone().unwrap_or_default() };
    let mut out = Vec::new();
    let liveness_planes = cmp.liveness_planes();
    let wire_ok = liveness_planes.is_empty();
    out.extend(liveness_planes);
    if wire_ok {
        let err = cmp.plane_error();
        let results_ok = err.is_none();
        out.extend(err);
        out.extend(cmp.plane_notice());
        if results_ok {
            let (rows, explain) = cmp.plane_rows_and_explain();
            out.extend(rows);
            out.extend(explain);
            out.extend(cmp.plane_meta());
            out.extend(cmp.plane_tag());
            out.extend(cmp.plane_copy());
        }
        out.extend(cmp.plane_notify());
    }
    out.extend(cmp.plane_log());
    out.extend(cmp.plane_probes());
    out.sort_by_key(|d| (class_rank(d.class), plane_order(&d.plane), d.plane.clone()));
    out
}

/// A plane's raw finding before the ledger and the signature are applied.
struct PlaneDiff {
    plane: String,
    class: Class,
    field: String,
    differing: Vec<String>,
    a: Option<String>,
    b: Option<String>,
    detail: String,
    severity: Option<Severity>,
    /// False for divergences the ledger must never absorb (reorder under a total key).
    ruleable: bool,
    /// The divergence is a structural mask that made the plane equal; an
    /// unruled one escalates to its plane's base class with a note.
    structural: bool,
}

impl PlaneDiff {
    fn new(plane: &str, class: Class, field: &str, a: Option<String>, b: Option<String>, detail: String) -> PlaneDiff {
        PlaneDiff {
            plane: plane.to_string(),
            class,
            field: field.to_string(),
            differing: vec![field.to_string()],
            a,
            b,
            detail,
            severity: None,
            ruleable: true,
            structural: false,
        }
    }
}

struct Cmp<'a> {
    ca: &'a CanonSide,
    cb: &'a CanonSide,
    step: &'a StepRecord,
    opts: &'a CompareOpts,
    ledger: &'a Ledger,
    sql: String,
}

fn short(s: &str) -> bool {
    s.chars().count() <= 48 && !s.contains('\n')
}

fn render_rows(rows: &[Vec<Option<String>>]) -> String {
    rows.iter().map(|r| format!("[{}]", render_row(r))).collect::<Vec<_>>().join(" ")
}

impl<'a> Cmp<'a> {
    fn unit(&self, message: Option<&str>) -> String {
        if let Some(t) = self.step.targets.first() {
            return t.clone();
        }
        if let Some(m) = message {
            return canon::message_template(m);
        }
        let head = normalize_stmt(&self.sql);
        if head.is_empty() {
            return self.step.kind.to_string_key();
        }
        head.chars().take(48).collect()
    }

    fn sqlstates(&self) -> (Option<String>, Option<String>) {
        (
            self.ca.error.as_ref().and_then(|e| e.get('C')).map(str::to_string),
            self.cb.error.as_ref().and_then(|e| e.get('C')).map(str::to_string),
        )
    }

    /// Ledger lookup + signature: the one path every plane goes through.
    fn emit(
        &self,
        pd: PlaneDiff,
        va: &dyn Fn(&str) -> Option<String>,
        vb: &dyn Fn(&str) -> Option<String>,
        message: Option<&str>,
        sqlstates: (Option<String>, Option<String>),
    ) -> Divergence {
        let rule = if pd.ruleable {
            self.ledger.resolve(&Candidate {
                plane: &pd.plane,
                differing: &pd.differing,
                stmt: &self.sql,
                value_a: va,
                value_b: vb,
            })
        } else {
            None
        };
        let mut detail = pd.detail;
        if pd.structural && rule.is_none() {
            detail = format!("unruled structural mask {:?}: {}", pd.field, detail);
        }
        let prefix = match pd.plane.as_str() {
            "wire:E" => "E.",
            "wire:N" => "N.",
            _ => "",
        };
        let mut delta = format!("{}{}", prefix, pd.field);
        if let (Some(x), Some(y)) = (&pd.a, &pd.b) {
            if short(x) && short(y) {
                delta.push_str(&format!(" {}→{}", x, y));
            }
        }
        let sig = signature(&pd.plane, &self.unit(message), sqlstates.0.as_deref(), sqlstates.1.as_deref(), &delta);
        Divergence {
            severity: pd.severity.unwrap_or_else(|| default_severity(pd.class)),
            plane: pd.plane,
            class: pd.class,
            field: pd.field,
            differing: pd.differing,
            a: pd.a,
            b: pd.b,
            sqlstate_a: sqlstates.0,
            sqlstate_b: sqlstates.1,
            detail,
            signature: sig,
            status: if rule.is_some() { Status::Ruled } else { Status::New },
            rule,
        }
    }

    /// Emit with no per-field values beyond the divergence's own a/b.
    fn emit_plain(&self, pd: PlaneDiff) -> Divergence {
        let (a, b, f) = (pd.a.clone(), pd.b.clone(), pd.field.clone());
        let va = move |x: &str| if x == f { a.clone() } else { None };
        let f2 = pd.field.clone();
        let vb = move |x: &str| if x == f2 { b.clone() } else { None };
        let ss = self.sqlstates();
        self.emit(pd, &va, &vb, None, ss)
    }

    // ---- liveness: session / panic / crash / hang -------------------

    fn liveness_planes(&self) -> Vec<Divergence> {
        let mut out = Vec::new();
        let dead = |l: &str| l == "dead";
        if self.ca.liveness != self.cb.liveness || dead(&self.ca.liveness) {
            let mut pd = PlaneDiff::new(
                "session",
                Class::Crash,
                "liveness",
                Some(self.ca.liveness.clone()),
                Some(self.cb.liveness.clone()),
                format!("liveness A {} vs B {}", self.ca.liveness, self.cb.liveness),
            );
            pd.severity = Some(Severity::High);
            out.push(self.emit_plain(pd));
        }
        for (side, p) in [("A", &self.ca.panic), ("B", &self.cb.panic)] {
            if let Some(p) = p {
                let pd = PlaneDiff::new(
                    "panic",
                    Class::Crash,
                    "panicked",
                    (side == "A").then(|| p.site.clone()),
                    (side == "B").then(|| p.site.clone()),
                    format!("{} panicked at {}: {}", side, p.site, canon::bytes_text(&p.message.0)),
                );
                out.push(self.emit_plain(pd));
            }
        }
        for (side, c) in [("A", &self.ca.crash), ("B", &self.cb.crash)] {
            if let Some(c) = c {
                let sig = c.signal.clone().unwrap_or_else(|| "?".to_string());
                let pd = PlaneDiff::new(
                    "crash",
                    Class::Crash,
                    "died",
                    (side == "A").then(|| sig.clone()),
                    (side == "B").then(|| sig.clone()),
                    format!("{} died (signal {}), restarted as generation {}", side, sig, c.generation),
                );
                out.push(self.emit_plain(pd));
            }
        }
        for (side, h) in [("A", &self.ca.hang), ("B", &self.cb.hang)] {
            if let Some(h) = h {
                let ladder = h.ladder.clone().unwrap_or_else(|| "none".to_string());
                let mut pd = PlaneDiff::new(
                    "hang",
                    Class::Hang,
                    "deadline",
                    (side == "A").then(|| ladder.clone()),
                    (side == "B").then(|| ladder.clone()),
                    format!("{} exceeded the {} ms step deadline; ladder reached {}", side, h.ms, ladder),
                );
                if h.ladder.as_deref().is_some_and(|l| l != "cancel") {
                    pd.severity = Some(Severity::Critical);
                }
                out.push(self.emit_plain(pd));
            }
        }
        out
    }

    // ---- wire:E ----------------------------------------------------

    fn plane_error(&self) -> Option<Divergence> {
        let (ea, eb) = (self.ca.error.as_ref(), self.cb.error.as_ref());
        let va = move |f: &str| ea.and_then(|e| e.value(f));
        let vb = move |f: &str| eb.and_then(|e| e.value(f));
        let ss = self.sqlstates();
        let pd = match (ea, eb) {
            (None, None) => return None,
            (Some(a), None) => PlaneDiff::new(
                "wire:E",
                Class::MissingError,
                "presence",
                Some(a.cm()),
                None,
                format!("A errored {} ({}); B succeeded", a.get('C').unwrap_or(""), a.get('M').unwrap_or("")),
            ),
            (None, Some(b)) => PlaneDiff::new(
                "wire:E",
                Class::SpuriousError,
                "presence",
                None,
                Some(b.cm()),
                format!("A succeeded; B errored {} ({})", b.get('C').unwrap_or(""), b.get('M').unwrap_or("")),
            ),
            (Some(a), Some(b)) => {
                let (differing, first) = err_diff(a, b);
                let (code, x, y) = first?;
                let class = err_field_class(code, x.is_none());
                let detail = if code == 'C' {
                    format!(
                        "SQLSTATE {} ({}) vs {} ({})",
                        a.get('C').unwrap_or(""),
                        a.get('M').unwrap_or(""),
                        b.get('C').unwrap_or(""),
                        b.get('M').unwrap_or("")
                    )
                } else {
                    format!("both {}: field {} differs: {:?} vs {:?}", a.get('C').unwrap_or(""), code, x, y)
                };
                let mut pd = PlaneDiff::new("wire:E", class, &code.to_string(), x, y, detail);
                pd.differing = differing;
                pd
            }
        };
        let pd = self.apply_missing_feature(pd, ea, eb);
        let msg = eb.or(ea).and_then(|e| e.get('M')).map(str::to_string);
        Some(self.emit(pd, &va, &vb, msg.as_deref(), ss))
    }

    fn apply_missing_feature(&self, mut pd: PlaneDiff, ea: Option<&ErrTuple>, eb: Option<&ErrTuple>) -> PlaneDiff {
        if let Some(mb) = eb.and_then(|e| e.get('M')) {
            if let Some(label) = missing_feature_rule(mb, ea.and_then(|e| e.get('M'))) {
                pd.class = Class::MissingFeature;
                pd.detail = format!("{}: {}", label, pd.detail);
            }
        }
        pd
    }

    // ---- wire:N ----------------------------------------------------

    fn plane_notice(&self) -> Option<Divergence> {
        let (na, nb) = (&self.ca.notices, &self.cb.notices);
        if na == nb {
            return None;
        }
        // Same multiset, different order: an order divergence.
        let order_only = na.len() == nb.len() && {
            let mut sa: Vec<_> = na.iter().map(|t| format!("{:?}", t.fields)).collect();
            let mut sb: Vec<_> = nb.iter().map(|t| format!("{:?}", t.fields)).collect();
            sa.sort();
            sb.sort();
            sa == sb
        };
        let none = |_: &str| None;
        if order_only {
            let first = na.iter().zip(nb).position(|(x, y)| x != y).unwrap_or(0);
            let pd = PlaneDiff::new(
                "wire:N",
                Class::WrongMessage,
                "order",
                na[first].get('M').map(str::to_string),
                nb[first].get('M').map(str::to_string),
                format!("notice stream order differs at index {}", first),
            );
            let msg = na[first].get('M').map(str::to_string);
            return Some(self.emit(pd, &none, &none, msg.as_deref(), self.sqlstates()));
        }
        for i in 0..na.len().max(nb.len()) {
            match (na.get(i), nb.get(i)) {
                (Some(x), None) => {
                    let pd = PlaneDiff::new(
                        "wire:N",
                        Class::WrongMessage,
                        "presence",
                        Some(x.get('M').unwrap_or("").to_string()),
                        None,
                        format!("notice #{} present on A only: {} {}", i, x.get('S').unwrap_or(""), x.get('M').unwrap_or("")),
                    );
                    let va = move |f: &str| x.value(f);
                    let msg = x.get('M').map(str::to_string);
                    let ss = (x.get('C').map(str::to_string), None);
                    return Some(self.emit(pd, &va, &none, msg.as_deref(), ss));
                }
                (None, Some(y)) => {
                    let pd = PlaneDiff::new(
                        "wire:N",
                        Class::WrongMessage,
                        "presence",
                        None,
                        Some(y.get('M').unwrap_or("").to_string()),
                        format!("notice #{} present on B only: {} {}", i, y.get('S').unwrap_or(""), y.get('M').unwrap_or("")),
                    );
                    let vb = move |f: &str| y.value(f);
                    let msg = y.get('M').map(str::to_string);
                    let ss = (None, y.get('C').map(str::to_string));
                    return Some(self.emit(pd, &none, &vb, msg.as_deref(), ss));
                }
                (Some(x), Some(y)) => {
                    let (differing, first) = err_diff(x, y);
                    let Some((code, a, b)) = first else { continue };
                    let class = match code {
                        'C' | 'S' | 'V' => Class::WrongSqlstate,
                        'H' if a.is_none() => Class::Cosmetic,
                        _ => Class::WrongMessage,
                    };
                    let mut pd = PlaneDiff::new(
                        "wire:N",
                        class,
                        &code.to_string(),
                        a.clone(),
                        b.clone(),
                        format!("notice #{}: field {} differs: {:?} vs {:?}", i, code, a, b),
                    );
                    pd.differing = differing;
                    let va = move |f: &str| x.value(f);
                    let vb = move |f: &str| y.value(f);
                    let msg = x.get('M').map(str::to_string);
                    let ss = (x.get('C').map(str::to_string), y.get('C').map(str::to_string));
                    return Some(self.emit(pd, &va, &vb, msg.as_deref(), ss));
                }
                (None, None) => {}
            }
        }
        None
    }

    // ---- rows / explain --------------------------------------------

    fn plane_rows_and_explain(&self) -> (Option<Divergence>, Option<Divergence>) {
        let (ga, gb) = (&self.ca.groups, &self.cb.groups);
        if ga.len() != gb.len() {
            let pd = PlaneDiff::new(
                "rows",
                Class::WrongResult,
                "groups",
                Some(ga.len().to_string()),
                Some(gb.len().to_string()),
                format!("result groups: A {} vs B {} (shapes {:?} vs {:?})", ga.len(), gb.len(), self.ca.shape, self.cb.shape),
            );
            return (Some(self.emit_plain(pd)), None);
        }
        let explain = is_explain_stmt(&self.sql);
        let mut rows_div = None;
        let mut explain_div = None;
        for (x, y) in ga.iter().zip(gb) {
            match (&x.columns, &y.columns) {
                (None, None) => continue,
                (Some(_), None) | (None, Some(_)) => {
                    if rows_div.is_none() {
                        let shape = |g: &ResultGroup| {
                            if g.columns.is_some() {
                                format!("rows ({})", g.rows.len())
                            } else {
                                format!("command tag {:?}", g.tag.clone().unwrap_or_default())
                            }
                        };
                        let pd = PlaneDiff::new(
                            "rows",
                            Class::WrongResult,
                            "shape",
                            Some(shape(x)),
                            Some(shape(y)),
                            format!("A returned {}; B returned {}", shape(x), shape(y)),
                        );
                        rows_div = Some(self.emit_plain(pd));
                    }
                }
                (Some(cx), Some(cy)) => {
                    if cx.len() != cy.len() {
                        continue; // the meta plane reports the column-count divergence
                    }
                    if explain {
                        if let (Some(la), Some(lb)) = (x.explain_lines(), y.explain_lines()) {
                            if explain_div.is_none() {
                                explain_div = self.compare_explain(&la, &lb).map(|pd| self.emit_plain(pd));
                            }
                            continue;
                        }
                    }
                    if rows_div.is_none() {
                        rows_div = self.compare_rows(x, y).map(|pd| self.emit_plain(pd));
                    }
                }
            }
        }
        (rows_div, explain_div)
    }

    fn compare_rows(&self, x: &ResultGroup, y: &ResultGroup) -> Option<PlaneDiff> {
        let (ra, rb) = (x.text_rows(), y.text_rows());
        let oids = x.col_oids();
        let modes = col_cmp_modes(&oids, &self.opts.soft_cols);
        let tol = self.opts.ulp_tol;
        let ordered = self.step.ordered != Ordered::None;
        let cmp = |na: &[Vec<Option<String>>], nb: &[Vec<Option<String>>]| {
            if ordered {
                cmp_rows_ordered(na, nb, &modes, tol)
            } else {
                cmp_rows_multiset(na, nb, &modes, tol)
            }
        };
        let (fa, fb, at) = first_cell_diff(&ra, &rb);
        let structural = |name: &str, detail: String| {
            let mut pd = PlaneDiff::new("rows", Class::WrongResult, name, fa.clone(), fb.clone(), detail);
            pd.structural = true;
            pd
        };
        let d = match cmp(&ra, &rb) {
            RowsetCmp::Equal => return None,
            RowsetCmp::EqualUlp => return Some(structural("ulp", "equal within float ulp tolerance".to_string())),
            RowsetCmp::EqualSoft => {
                return Some(structural("soft", "equal outside ruled-soft float-aggregate columns".to_string()))
            }
            RowsetCmp::Diff(d) => d,
        };
        let equal = |na: &[Vec<Option<String>>], nb: &[Vec<Option<String>>]| !matches!(cmp(na, nb), RowsetCmp::Diff(_));
        if equal(&normalize_oid_literal_rows(&ra), &normalize_oid_literal_rows(&rb)) {
            return Some(structural("oid-literal", format!("equal after masking user-range '<n>'::oid literals: {}", d)));
        }
        if equal(&normalize_toast_name_rows(&ra), &normalize_toast_name_rows(&rb)) {
            return Some(structural("toast-name", format!("equal after masking user-range pg_toast_<n> names: {}", d)));
        }
        if equal(&normalize_binary_udt_rows(&ra), &normalize_binary_udt_rows(&rb)) {
            return Some(structural(
                "binary-udt-oid",
                format!("equal after masking embedded user-range type oids in binary container images: {}", d),
            ));
        }
        if calls_cmp_builtin(&self.sql)
            && equal(&normalize_int4_sign_rows(&ra, &oids), &normalize_int4_sign_rows(&rb, &oids))
        {
            return Some(structural("cmp-sign", format!("int4 *cmp() results equal in sign, magnitude differs: {}", d)));
        }
        if ordered && !matches!(cmp_rows_multiset(&ra, &rb, &modes, tol), RowsetCmp::Diff(_)) {
            let total = self.step.ordered == Ordered::Total;
            let mut pd = PlaneDiff::new(
                "rows",
                Class::WrongResult,
                "order",
                Some(render_rows(&ra)),
                Some(render_rows(&rb)),
                if total {
                    format!("multiset-equal reorder under ordered: total (a wrong result, never tie-order): {}", d)
                } else {
                    format!("multiset-equal, order differs under ordered: partial: {}", d)
                },
            );
            pd.ruleable = !total;
            return Some(pd);
        }
        if ra.len() != rb.len() {
            return Some(PlaneDiff::new(
                "rows",
                Class::WrongResult,
                "count",
                Some(ra.len().to_string()),
                Some(rb.len().to_string()),
                d,
            ));
        }
        let mut pd = PlaneDiff::new("rows", Class::WrongResult, "cell", fa, fb, d);
        if let Some(at) = at {
            pd.detail = format!("{} ({})", pd.detail, at);
        }
        if let Some(suf) = pd.b.as_deref().and_then(|b| canon::foreign_dlsuffix(b, &self.opts.ctx_b)) {
            pd.detail = format!("{}; B names a {} shared object where the host DLSUFFIX is {}", pd.detail, suf, self.opts.ctx_b.dlsuffix);
        }
        Some(pd)
    }

    fn compare_explain(&self, la: &[String], lb: &[String]) -> Option<PlaneDiff> {
        if la == lb {
            return None;
        }
        let lines = |l: &[String]| -> Vec<Vec<Option<String>>> { l.iter().map(|s| vec![Some(s.clone())]).collect() };
        let (ra, rb) = (lines(la), lines(lb));
        let equal = |na: &[Vec<Option<String>>], nb: &[Vec<Option<String>>]| {
            matches!(cmp_rows_ordered(na, nb, &[], 0), RowsetCmp::Equal)
        };
        let timing = |r: &[Vec<Option<String>>]| {
            if self.opts.mask_explain_timing {
                normalize_explain_timing_rows(r)
            } else {
                r.to_vec()
            }
        };
        let (fa, fb, _) = first_cell_diff(&normalize_explain_rows(&ra), &normalize_explain_rows(&rb));
        let structural = |name: &str, detail: &str| {
            let mut pd = PlaneDiff::new("explain", Class::PlanShape, name, fa.clone(), fb.clone(), detail.to_string());
            pd.structural = true;
            pd
        };
        if equal(&normalize_explain_rows(&ra), &normalize_explain_rows(&rb)) {
            return Some(structural("counter", "equal after masking runtime resource counters"));
        }
        if self.opts.mask_explain_timing
            && equal(&timing(&normalize_explain_rows(&ra)), &timing(&normalize_explain_rows(&rb)))
        {
            return Some(structural("timing", "equal after masking wall-clock timing text"));
        }
        if equal(
            &timing(&normalize_explain_rows(&strip_planning_buffer_rows(&ra))),
            &timing(&normalize_explain_rows(&strip_planning_buffer_rows(&rb))),
        ) {
            return Some(structural(
                "planning-buffers",
                "equal after dropping the TEXT Planning: buffer-usage block (presence is session cache state)",
            ));
        }
        if is_scroll_declare_explain_stmt(&self.sql) {
            if let Some(stripped) = strip_top_materialize_rows(&ra) {
                if equal(&timing(&normalize_explain_rows(&stripped)), &timing(&normalize_explain_rows(&rb))) {
                    return Some(structural("materialize", "equal after stripping C's top-level Materialize SCROLL wrap"));
                }
            }
        }
        let detail = match &fa {
            Some(x) => format!("plan shape differs: {:?} vs {:?}", x, fb.clone().unwrap_or_default()),
            None => format!("plan shape differs: {} vs {} lines", la.len(), lb.len()),
        };
        Some(PlaneDiff::new("explain", Class::PlanShape, "shape", fa, fb, detail))
    }

    // ---- meta / tag / copy / notify --------------------------------

    fn plane_meta(&self) -> Option<Divergence> {
        for (x, y) in self.ca.groups.iter().zip(&self.cb.groups) {
            let (Some(cx), Some(cy)) = (&x.columns, &y.columns) else { continue };
            if cx.len() != cy.len() {
                let pd = PlaneDiff::new(
                    "meta",
                    Class::WrongResult,
                    "columns",
                    Some(cx.len().to_string()),
                    Some(cy.len().to_string()),
                    format!("column count differs: {} vs {}", cx.len(), cy.len()),
                );
                return Some(self.emit_plain(pd));
            }
            for (i, (a, b)) in cx.iter().zip(cy).enumerate() {
                let field = if a.name != b.name {
                    ("name", a.name.clone(), b.name.clone())
                } else if a.typ != b.typ {
                    ("typ", a.typ.clone(), b.typ.clone())
                } else if a.typlen != b.typlen {
                    ("typlen", a.typlen.to_string(), b.typlen.to_string())
                } else if a.typmod != b.typmod {
                    ("typmod", a.typmod.to_string(), b.typmod.to_string())
                } else if a.fmt != b.fmt {
                    ("fmt", a.fmt.to_string(), b.fmt.to_string())
                } else {
                    continue;
                };
                let detail = if field.0 == "typ" {
                    format!("column {} type oids differ: {} vs {} ({} vs {})", i, field.1, field.2, a.render(), b.render())
                } else {
                    format!("column {} {} differs: {} vs {} ({} vs {})", i, field.0, field.1, field.2, a.render(), b.render())
                };
                let pd = PlaneDiff::new("meta", Class::WrongResult, field.0, Some(field.1), Some(field.2), detail);
                return Some(self.emit_plain(pd));
            }
        }
        let ka: BTreeSet<&String> = self.ca.params.keys().collect();
        let kb: BTreeSet<&String> = self.cb.params.keys().collect();
        if ka != kb {
            let j = |k: &BTreeSet<&String>| k.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",");
            let pd = PlaneDiff::new(
                "meta",
                Class::WrongResult,
                "params",
                Some(j(&ka)),
                Some(j(&kb)),
                format!("ParameterStatus key set differs: {{{}}} vs {{{}}}", j(&ka), j(&kb)),
            );
            return Some(self.emit_plain(pd));
        }
        if self.ca.param_types != self.cb.param_types {
            let pd = PlaneDiff::new(
                "meta",
                Class::WrongResult,
                "param_types",
                Some(format!("{:?}", self.ca.param_types)),
                Some(format!("{:?}", self.cb.param_types)),
                "ParameterDescription differs".to_string(),
            );
            return Some(self.emit_plain(pd));
        }
        None
    }

    fn plane_tag(&self) -> Option<Divergence> {
        let ta: Vec<Option<String>> = self.ca.groups.iter().map(|g| g.tag.clone()).collect();
        let tb: Vec<Option<String>> = self.cb.groups.iter().map(|g| g.tag.clone()).collect();
        if ta == tb {
            return None;
        }
        let i = ta.iter().zip(&tb).position(|(x, y)| x != y).unwrap_or(ta.len().min(tb.len()));
        let a = ta.get(i).cloned().flatten();
        let b = tb.get(i).cloned().flatten();
        // The full CommandComplete text is meta (plan §4.1 `meta|tag`):
        // count-only compare is retired.
        let pd = PlaneDiff::new(
            "meta",
            Class::WrongResult,
            "tag",
            a.clone(),
            b.clone(),
            format!("command tag #{} differs: {:?} vs {:?}", i, a, b),
        );
        Some(self.emit_plain(pd))
    }

    fn plane_copy(&self) -> Option<Divergence> {
        if self.ca.copy_out != self.cb.copy_out {
            let pd = PlaneDiff::new(
                "copy",
                Class::WrongResult,
                "bytes",
                None,
                None,
                copy_diff_detail(&self.ca.copy_out, &self.cb.copy_out),
            );
            return Some(self.emit_plain(pd));
        }
        if self.ca.copy_out_fmt != self.cb.copy_out_fmt || self.ca.copy_in_fmt != self.cb.copy_in_fmt {
            let pd = PlaneDiff::new(
                "copy",
                Class::WrongResult,
                "fmt",
                Some(format!("{:?}/{:?}", self.ca.copy_in_fmt, self.ca.copy_out_fmt)),
                Some(format!("{:?}/{:?}", self.cb.copy_in_fmt, self.cb.copy_out_fmt)),
                "COPY response formats differ".to_string(),
            );
            return Some(self.emit_plain(pd));
        }
        None
    }

    fn plane_notify(&self) -> Option<Divergence> {
        if self.ca.notify == self.cb.notify {
            return None;
        }
        let render = |m: &BTreeMap<(String, String), u32>| {
            m.iter().map(|((c, p), n)| format!("{}:{}x{}", c, p, n)).collect::<Vec<_>>().join(" ")
        };
        let pd = PlaneDiff::new(
            "notify",
            Class::WrongResult,
            "channel",
            Some(render(&self.ca.notify)),
            Some(render(&self.cb.notify)),
            format!("notification multiset differs: {{{}}} vs {{{}}}", render(&self.ca.notify), render(&self.cb.notify)),
        );
        Some(self.emit_plain(pd))
    }

    // ---- log / probes ----------------------------------------------

    fn plane_log(&self) -> Option<Divergence> {
        let (la, lb) = (&self.ca.log, &self.cb.log);
        if la == lb {
            return None;
        }
        for i in 0..la.len().max(lb.len()) {
            let (x, y) = match (la.get(i), lb.get(i)) {
                (Some(x), Some(y)) => (x, y),
                (x, y) => {
                    let pd = PlaneDiff::new(
                        "log",
                        Class::WrongMessage,
                        "presence",
                        x.map(|l| l.raw.clone()),
                        y.map(|l| l.raw.clone()),
                        format!("log line #{} present on {} only", i, if x.is_some() { "A" } else { "B" }),
                    );
                    return Some(self.emit_plain(pd));
                }
            };
            if x == y {
                continue;
            }
            let (field, class) = if x.message != y.message {
                ("message", Class::WrongMessage)
            } else if x.level != y.level {
                ("level", Class::WrongSqlstate)
            } else if x.sqlstate != y.sqlstate {
                ("sqlstate", Class::WrongSqlstate)
            } else if x.zone != y.zone {
                ("zone", Class::Cosmetic)
            } else if x.backend_type != y.backend_type {
                ("backend_type", Class::Cosmetic)
            } else if x.app != y.app {
                ("app", Class::Cosmetic)
            } else if x.level.is_none() && y.level.is_none() {
                ("raw", Class::WrongMessage)
            } else {
                ("prefix", Class::Cosmetic)
            };
            let pd = PlaneDiff::new(
                "log",
                class,
                field,
                x.value(field),
                y.value(field),
                format!("log line #{}: {} differs: {:?} vs {:?}", i, field, x.value(field), y.value(field)),
            );
            let va = move |f: &str| x.value(f);
            let vb = move |f: &str| y.value(f);
            let msg = x.message.clone();
            let ss = (x.sqlstate.clone(), y.sqlstate.clone());
            return Some(self.emit(pd, &va, &vb, msg.as_deref(), ss));
        }
        None
    }

    /// Probe decks compare per statement key (plan §4.4). A deck is
    /// state, not a per-statement answer, so its signature must not embed
    /// the triggering statement (the first LIVE smoke reported one
    /// persistent catalog delta 111 times): `probe:<deck> | <key> | -/- |
    /// rows <hash>` where the hash is over the row delta (rows on one side
    /// only, plus the column lists when they differ), so a persistent
    /// divergence is one signature for the whole run and a changed delta
    /// is a new one. `field` / `differing` carry the statement key, so a
    /// delta ruling `plane = "probe:<deck>", field = "<key>"` covers it.
    fn plane_probes(&self) -> Vec<Divergence> {
        let decks: BTreeSet<&String> = self.ca.probes.keys().chain(self.cb.probes.keys()).collect();
        let mut out = Vec::new();
        for deck in decks {
            let (a, b) = (self.ca.probes.get(deck), self.cb.probes.get(deck));
            if a == b {
                continue;
            }
            let plane = format!("probe:{}", deck);
            let parse = |s: Option<&String>| s.and_then(|t| json::parse(t).ok()).unwrap_or(json::Value::Null);
            let (va, vb) = (parse(a), parse(b));
            for (key, ka, kb) in probe_key_deltas(&va, &vb) {
                let sa = ka.as_ref().and_then(|v| json::to_canonical(v).ok());
                let sb = kb.as_ref().and_then(|v| json::to_canonical(v).ok());
                let hash = probe_delta_hash(ka.as_ref(), kb.as_ref());
                let mut pd = PlaneDiff::new(
                    &plane,
                    Class::WrongResult,
                    &key,
                    sa,
                    sb,
                    format!("probe deck {} statement {} differs (delta {})", deck, key, hash),
                );
                pd.severity = Some(Severity::Critical);
                let mut d = self.emit_plain(pd);
                d.signature = signature(&plane, &key, None, None, &format!("rows {}", hash));
                out.push(d);
            }
        }
        out
    }
}

/// The statement keys of a deck whose results differ, with each side's
/// per-key result (None = the side has no such key).
pub fn probe_key_deltas(a: &json::Value, b: &json::Value) -> Vec<(String, Option<json::Value>, Option<json::Value>)> {
    let keys: BTreeSet<String> = a
        .as_obj()
        .into_iter()
        .flatten()
        .chain(b.as_obj().into_iter().flatten())
        .map(|(k, _)| k.clone())
        .collect();
    let mut out = Vec::new();
    for k in keys {
        let (x, y) = (a.get(&k), b.get(&k));
        if x != y {
            out.push((k, x.cloned(), y.cloned()));
        }
    }
    if out.is_empty() && a != b {
        // Not both objects: the whole value is the delta.
        out.push(("deck".into(), Some(a.clone()), Some(b.clone())));
    }
    out
}

/// 16 hex chars of sha256 over the row delta of one statement: the rows
/// present on exactly one side (each tagged with its side), the column
/// lists when they differ, and the error / tag objects when a side has
/// no rows. Independent of the triggering statement and of rows both
/// sides agree on.
pub fn probe_delta_hash(a: Option<&json::Value>, b: Option<&json::Value>) -> String {
    let rows = |v: Option<&json::Value>| -> BTreeSet<String> {
        v.and_then(|v| v.get("rows"))
            .and_then(|r| r.as_arr())
            .map(|rs| rs.iter().filter_map(|r| json::to_canonical(r).ok()).collect())
            .unwrap_or_default()
    };
    let (ra, rb) = (rows(a), rows(b));
    let mut parts: Vec<String> = Vec::new();
    for r in ra.difference(&rb) {
        parts.push(format!("A{}", r));
    }
    for r in rb.difference(&ra) {
        parts.push(format!("B{}", r));
    }
    let cols = |v: Option<&json::Value>| v.and_then(|v| v.get("columns")).and_then(|c| json::to_canonical(c).ok());
    if cols(a) != cols(b) {
        parts.push(format!("cols {:?}→{:?}", cols(a), cols(b)));
    }
    let non_rows = |v: Option<&json::Value>| -> Option<String> {
        let v = v?;
        if v.get("rows").is_some() {
            return None;
        }
        json::to_canonical(v).ok()
    };
    if non_rows(a) != non_rows(b) {
        parts.push(format!("value {:?}→{:?}", non_rows(a), non_rows(b)));
    }
    let digest = pg_sha2::sha256(parts.join("\n").as_bytes());
    digest.iter().take(8).map(|b| format!("{:02x}", b)).collect()
}

/// Differing E/N fields in classification order, plus the first one with
/// its (pairwise-masked) values.
fn err_diff(a: &ErrTuple, b: &ErrTuple) -> (Vec<String>, Option<(char, Option<String>, Option<String>)>) {
    let mut differing = Vec::new();
    let mut first = None;
    let extra: Vec<char> = a
        .fields
        .keys()
        .chain(b.fields.keys())
        .copied()
        .filter(|c| !ERR_FIELD_ORDER.contains(c))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    for &code in ERR_FIELD_ORDER.iter().chain(extra.iter()) {
        let (x, y) = ErrTuple::pair(a, b, code);
        if x != y {
            differing.push(code.to_string());
            if first.is_none() {
                first = Some((code, x, y));
            }
        }
    }
    (differing, first)
}

/// Class by first differing E field; `hint_added` = A had no H.
fn err_field_class(code: char, hint_added: bool) -> Class {
    match code {
        'C' => Class::WrongSqlstate,
        'P' | 'p' => Class::WrongPosition,
        'H' if hint_added => Class::Cosmetic,
        _ => Class::WrongMessage,
    }
}

/// First differing cell pair: positional when the shapes allow, else the
/// first unmatched row on each side (multiset view).
fn first_cell_diff(ra: &[Vec<Option<String>>], rb: &[Vec<Option<String>>]) -> (Option<String>, Option<String>, Option<String>) {
    let text = |c: &Option<String>| c.clone().unwrap_or_else(|| "NULL".to_string());
    if ra.len() == rb.len() {
        for (i, (x, y)) in ra.iter().zip(rb).enumerate() {
            for (j, (cx, cy)) in x.iter().zip(y).enumerate() {
                if cx != cy {
                    return (Some(text(cx)), Some(text(cy)), Some(format!("row {} col {}", i, j)));
                }
            }
            if x.len() != y.len() {
                return (Some(render_row(x)), Some(render_row(y)), Some(format!("row {} width", i)));
            }
        }
        return (None, None, None);
    }
    let mut sa: Vec<String> = ra.iter().map(|r| render_row(r)).collect();
    let mut sb: Vec<String> = rb.iter().map(|r| render_row(r)).collect();
    sa.sort();
    sb.sort();
    let only_a = sa.iter().find(|r| !sb.contains(r)).cloned();
    let only_b = sb.iter().find(|r| !sa.contains(r)).cloned();
    (only_a, only_b, Some("unmatched rows".to_string()))
}

// ---------------------------------------------------------------------
// Legacy adapter: StmtOutcome pairs through the plane comparator
// ---------------------------------------------------------------------

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
    /// statements and --mask-explain-timing replays.
    pub mask_explain_timing: bool,
}

/// The compiled-in ledger every legacy caller classifies against.
pub fn embedded_ledger() -> &'static Ledger {
    static LEDGER: OnceLock<Ledger> = OnceLock::new();
    LEDGER.get_or_init(Ledger::embedded)
}

/// A per-statement outcome as the wire it would have produced.
pub fn outcome_record(o: &StmtOutcome, side: contracts::Side) -> ObservationRecord {
    let mut wire = Vec::new();
    let mut liveness = "ok".to_string();
    match o {
        StmtOutcome::Rows { col_oids, rows } => {
            wire.push(WireMsg::RowDescription(
                col_oids
                    .iter()
                    .enumerate()
                    .map(|(i, &oid)| ColDesc {
                        name: contracts::Bytes::text(&format!("c{}", i)),
                        tableoid: 0,
                        attnum: 0,
                        typoid: oid,
                        typlen: -1,
                        typmod: -1,
                        fmt: 0,
                    })
                    .collect(),
            ));
            for r in rows {
                wire.push(WireMsg::DataRow(r.iter().map(|c| c.as_ref().map(|s| contracts::Bytes::text(s))).collect()));
            }
            // No row count in the tag: a legacy rowset carries none, and a
            // synthetic count would double-report every row-count diff.
            wire.push(WireMsg::CommandComplete(contracts::Bytes::text("SELECT")));
            wire.push(WireMsg::ReadyForQuery { status: 'I' });
        }
        StmtOutcome::Command { tag, .. } => {
            wire.push(WireMsg::CommandComplete(contracts::Bytes::text(tag)));
            wire.push(WireMsg::ReadyForQuery { status: 'I' });
        }
        StmtOutcome::CopyOut { bytes, tag } => {
            wire.push(WireMsg::CopyOutResponse { fmt: 0, col_fmts: Vec::new() });
            if !bytes.is_empty() {
                wire.push(WireMsg::CopyData(contracts::Bytes(bytes.clone())));
            }
            wire.push(WireMsg::CopyDone);
            wire.push(WireMsg::CommandComplete(contracts::Bytes::text(tag)));
            wire.push(WireMsg::ReadyForQuery { status: 'I' });
        }
        StmtOutcome::Error { sqlstate, message } => {
            let mut f = contracts::ErrFields::new();
            f.insert('S', contracts::Bytes::text("ERROR"));
            f.insert('V', contracts::Bytes::text("ERROR"));
            f.insert('C', contracts::Bytes::text(sqlstate));
            f.insert('M', contracts::Bytes::text(message));
            wire.push(WireMsg::ErrorResponse(f));
            wire.push(WireMsg::ReadyForQuery { status: 'E' });
        }
        StmtOutcome::ConnLost { .. } => liveness = "dead".to_string(),
    }
    ObservationRecord {
        scenario: "legacy".to_string(),
        seq: 0,
        session: "s1".to_string(),
        side,
        wire,
        log: Vec::new(),
        panic: None,
        crash: None,
        hang: None,
        probes: BTreeMap::new(),
        server: None,
        liveness,
        ms: 0,
        version: String::new(),
    }
}

/// The StepRecord a bare statement implies: `ordered: partial` with an
/// ORDER BY (tie-order ruling in play), `none` otherwise — never `total`.
pub fn legacy_step(sql: &str) -> StepRecord {
    StepRecord {
        scenario: "legacy".to_string(),
        seq: 0,
        session: "s1".to_string(),
        role: "superuser".to_string(),
        kind: StepKind::Sql,
        sql: Some(sql.to_string()),
        xproto: None,
        productions: Vec::new(),
        targets: Vec::new(),
        ordered: if has_order_by(sql) { Ordered::Partial } else { Ordered::None },
        expect_c: None,
        bracket: None,
        recipe: None,
        mutant: None,
        slots: BTreeMap::new(),
    }
}

/// Map the plane verdict back onto the legacy class vocabulary.
pub fn legacy_class(divs: &[Divergence], a: &StmtOutcome, b: &StmtOutcome) -> Classified {
    let Some(d) = step_verdict(divs) else {
        return Classified { class: DiffClass::Match, detail: String::new() };
    };
    if d.plane == "session" {
        let (la, lb) = (matches!(a, StmtOutcome::ConnLost { .. }), matches!(b, StmtOutcome::ConnLost { .. }));
        let detail_of = |o: &StmtOutcome| match o {
            StmtOutcome::ConnLost { detail } => detail.clone(),
            _ => String::new(),
        };
        let (side, detail) = match (la, lb) {
            (true, true) => (Side::Both, format!("both connections lost; A: {}", detail_of(a))),
            (true, false) => (Side::A, format!("A connection lost: {}", detail_of(a))),
            _ => (Side::B, format!("B connection lost: {}", detail_of(b))),
        };
        return Classified { class: DiffClass::SessionDiverged(side), detail };
    }
    if let Some(id) = &d.rule {
        return Classified { class: DiffClass::Ruled(id.clone()), detail: d.detail.clone() };
    }
    let class = match (d.plane.as_str(), d.field.as_str()) {
        ("wire:E" | "wire:N", _) => DiffClass::ErrorDiff,
        ("meta", "tag") => DiffClass::CountDiff,
        _ => DiffClass::RowsetDiff,
    };
    Classified { class, detail: d.detail.clone() }
}

/// Legacy classification of one statement's two outcomes: the plane
/// comparator over a synthetic wire, against the embedded ledger. Ruled
/// divergences come back as `Ruled(<ledger id>)`.
pub fn classify(input: &DiffInput) -> Classified {
    let DiffInput { sql, a, b, ulp_tol, soft_cols, mask_explain_timing } = *input;
    let ra = outcome_record(a, contracts::Side::A);
    let rb = outcome_record(b, contracts::Side::B);
    let step = legacy_step(sql);
    let opts = CompareOpts {
        ulp_tol,
        soft_cols: soft_cols.to_vec(),
        mask_explain_timing,
        ..CompareOpts::default()
    };
    let divs = compare_planes_with(&ra, &rb, &step, embedded_ledger(), &opts);
    legacy_class(&divs, a, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{Bytes, ErrFields, LogLine, Side as CSide};

    fn rows(cells: &[&[Option<&str>]]) -> Vec<Vec<Option<String>>> {
        cells.iter().map(|r| r.iter().map(|c| c.map(|s| s.to_string())).collect()).collect()
    }

    fn rowset(col_oids: Vec<u32>, r: Vec<Vec<Option<String>>>) -> StmtOutcome {
        StmtOutcome::Rows { col_oids, rows: r }
    }

    fn classify_sql(sql: &str, a: &StmtOutcome, b: &StmtOutcome) -> Classified {
        classify(&DiffInput { sql, a, b, ulp_tol: 4, soft_cols: &[], mask_explain_timing: false })
    }

    fn classify_timing(sql: &str, a: &StmtOutcome, b: &StmtOutcome) -> Classified {
        classify(&DiffInput { sql, a, b, ulp_tol: 4, soft_cols: &[], mask_explain_timing: true })
    }

    fn classify_soft(sql: &str, a: &StmtOutcome, b: &StmtOutcome, soft_cols: &[usize]) -> Classified {
        classify(&DiffInput { sql, a, b, ulp_tol: 4, soft_cols, mask_explain_timing: false })
    }

    fn err(sqlstate: &str, message: &str) -> StmtOutcome {
        StmtOutcome::Error { sqlstate: sqlstate.to_string(), message: message.to_string() }
    }

    fn ok1() -> StmtOutcome {
        rowset(vec![23], rows(&[&[Some("1")]]))
    }

    fn ruled(id: &str) -> DiffClass {
        DiffClass::Ruled(id.to_string())
    }

    // ---- plane comparator: builders --------------------------------

    fn fields(kv: &[(char, &str)]) -> ErrFields {
        kv.iter().map(|(k, v)| (*k, Bytes::text(v))).collect()
    }

    fn obs(side: CSide, wire: Vec<WireMsg>) -> ObservationRecord {
        ObservationRecord {
            scenario: "t".into(),
            seq: 1,
            session: "s1".into(),
            side,
            wire,
            log: vec![],
            panic: None,
            crash: None,
            hang: None,
            probes: BTreeMap::new(),
            server: None,
            liveness: "ok".into(),
            ms: 0,
            version: "18.6".into(),
        }
    }

    fn step(sql: &str, ordered: Ordered) -> StepRecord {
        let mut s = legacy_step(sql);
        s.ordered = ordered;
        s
    }

    fn e(kv: &[(char, &str)]) -> WireMsg {
        WireMsg::ErrorResponse(fields(kv))
    }

    fn n(kv: &[(char, &str)]) -> WireMsg {
        WireMsg::NoticeResponse(fields(kv))
    }

    fn c(tag: &str) -> WireMsg {
        WireMsg::CommandComplete(Bytes::text(tag))
    }

    fn t(cols: &[(&str, u32)]) -> WireMsg {
        WireMsg::RowDescription(
            cols.iter()
                .map(|(name, oid)| ColDesc { name: Bytes::text(name), tableoid: 0, attnum: 0, typoid: *oid, typlen: -1, typmod: -1, fmt: 0 })
                .collect(),
        )
    }

    fn d(cells: &[Option<&str>]) -> WireMsg {
        WireMsg::DataRow(cells.iter().map(|c| c.map(Bytes::text)).collect())
    }

    fn compare(a: Vec<WireMsg>, b: Vec<WireMsg>, st: &StepRecord) -> Vec<Divergence> {
        compare_planes(&obs(CSide::A, a), &obs(CSide::B, b), st, embedded_ledger())
    }

    // ---- audit pairs -----------------------------------------------

    /// analyze-1: the INFO stream is present on A and absent on B.
    #[test]
    fn analyze_1_notice_stream_presence_is_wrong_message() {
        let a = vec![
            n(&[('S', "INFO"), ('V', "INFO"), ('C', "00000"), ('M', "analyzing \"public.t\""), ('F', "analyze.c"), ('L', "319"), ('R', "do_analyze_rel")]),
            n(&[('S', "INFO"), ('V', "INFO"), ('C', "00000"), ('M', "\"t\": scanned 1 of 1 pages, containing 10 live rows and 0 dead rows; 10 rows in sample, 10 estimated total rows")]),
            c("ANALYZE"),
        ];
        let b = vec![c("ANALYZE")];
        let divs = compare(a.clone(), b, &step("analyze verbose t;", Ordered::None));
        assert_eq!(divs.len(), 1, "{:?}", divs);
        let dv = &divs[0];
        assert_eq!((dv.plane.as_str(), dv.class, dv.field.as_str()), ("wire:N", Class::WrongMessage, "presence"));
        assert_eq!(dv.signature, "wire:N | analyzing \"%s\" | 00000/- | N.presence");
        assert_eq!(dv.severity, Severity::Medium);
        assert!(dv.rule.is_none());
        // A different INFO text on B (a row count) is wrong-message on M,
        // not a masked digit run.
        let b2 = vec![
            n(&[('S', "INFO"), ('V', "INFO"), ('C', "00000"), ('M', "analyzing \"public.t\""), ('F', "lib.rs"), ('L', "1"), ('R', "do_analyze_rel")]),
            n(&[('S', "INFO"), ('V', "INFO"), ('C', "00000"), ('M', "\"t\": scanned 1 of 1 pages, containing 9 live rows and 0 dead rows; 10 rows in sample, 10 estimated total rows")]),
            c("ANALYZE"),
        ];
        let divs = compare(a.clone(), b2, &step("analyze verbose t;", Ordered::None));
        assert_eq!(divs.len(), 1, "{:?}", divs);
        assert_eq!((divs[0].plane.as_str(), divs[0].field.as_str()), ("wire:N", "M"));
        // Identical stream modulo F/L: no divergence.
        let b3 = vec![
            n(&[('S', "INFO"), ('V', "INFO"), ('C', "00000"), ('M', "analyzing \"public.t\""), ('F', "lib.rs"), ('L', "1"), ('R', "do_analyze_rel")]),
            n(&[('S', "INFO"), ('V', "INFO"), ('C', "00000"), ('M', "\"t\": scanned 1 of 1 pages, containing 10 live rows and 0 dead rows; 10 rows in sample, 10 estimated total rows")]),
            c("ANALYZE"),
        ];
        assert!(compare(a, b3, &step("analyze verbose t;", Ordered::None)).is_empty());
    }

    /// aclchk-2 / sequence-1: same C and M, P missing on B.
    #[test]
    fn position_only_delta_is_wrong_position() {
        for (code, msg, pos) in [
            ("42501", "permission denied for schema d_sch", "15"),
            ("42601", "invalid sequence option SEQUENCE NAME", "32"),
        ] {
            let a = vec![e(&[('S', "ERROR"), ('V', "ERROR"), ('C', code), ('M', msg), ('P', pos), ('F', "aclchk.c"), ('L', "2793")])];
            let b = vec![e(&[('S', "ERROR"), ('V', "ERROR"), ('C', code), ('M', msg), ('F', "x.rs"), ('L', "1")])];
            let divs = compare(a, b, &step("SELECT * FROM d_sch.t;", Ordered::None));
            assert_eq!(divs.len(), 1, "{:?}", divs);
            let dv = &divs[0];
            assert_eq!((dv.plane.as_str(), dv.class, dv.field.as_str()), ("wire:E", Class::WrongPosition, "P"));
            assert_eq!(dv.a.as_deref(), Some(pos));
            assert_eq!(dv.b, None);
            assert!(dv.signature.ends_with(&format!("| {}/{} | E.P", code, code)), "{}", dv.signature);
        }
    }

    /// catalog_namespace-2: an OID/name where C prints a name is a finding;
    /// user OIDs at the same template position are masked.
    #[test]
    fn message_digits_masked_only_at_same_template_position() {
        let a = vec![e(&[('S', "ERROR"), ('C', "42601"), ('M', "improper qualified name (too many dotted names): a.b.c.d.+")])];
        let b = vec![e(&[('S', "ERROR"), ('C', "42601"), ('M', "improper qualified name (too many dotted names): a.b.c.d")])];
        let divs = compare(a, b, &step("SELECT 1 OPERATOR(a.b.c.d.+) 1;", Ordered::None));
        assert_eq!(divs.len(), 1);
        assert_eq!((divs[0].class, divs[0].field.as_str()), (Class::WrongMessage, "M"));
        // Same template, user-range OIDs on both sides: masked, no divergence.
        let a = vec![e(&[('S', "ERROR"), ('C', "42704"), ('M', "type with OID 16401 does not exist")])];
        let b = vec![e(&[('S', "ERROR"), ('C', "42704"), ('M', "type with OID 16455 does not exist")])];
        assert!(compare(a, b, &step("SELECT 1;", Ordered::None)).is_empty());
        // Builtin OID on one side stays compared.
        let a = vec![e(&[('S', "ERROR"), ('C', "42704"), ('M', "type with OID 23 does not exist")])];
        let b = vec![e(&[('S', "ERROR"), ('C', "42704"), ('M', "type with OID 16455 does not exist")])];
        assert_eq!(compare(a, b, &step("SELECT 1;", Ordered::None)).len(), 1);
    }

    /// dfmgr-5 / dfmgr-9: paths canonicalize to <LIBDIR>/<file name>; the
    /// DLSUFFIX is a per-host expectation, so both stay findings.
    #[test]
    fn path_canonicalization_keeps_file_name_and_dlsuffix() {
        let mut opts = CompareOpts::default();
        opts.ctx_a.libdir = Some("/home/dev/dev/pg186-install/lib/postgresql".into());
        opts.ctx_a.dlsuffix = ".dylib".into();
        opts.ctx_b.libdir = Some("/opt/pgrust/lib".into());
        opts.ctx_b.dlsuffix = ".dylib".into();
        let a = vec![e(&[('S', "ERROR"), ('C', "42883"), ('M', "could not find function \"no_such_symbol\" in file \"/home/dev/dev/pg186-install/lib/postgresql/pg_trgm.dylib\"")])];
        let b = vec![e(&[('S', "ERROR"), ('C', "42883"), ('M', "could not find function \"no_such_symbol\" in file \"$libdir/pg_trgm\"")])];
        let st = step("CREATE FUNCTION f() RETURNS int AS '$libdir/pg_trgm', 'no_such_symbol' LANGUAGE C;", Ordered::None);
        let divs = compare_planes_with(&obs(CSide::A, a), &obs(CSide::B, b), &st, embedded_ledger(), &opts);
        assert_eq!(divs.len(), 1, "{:?}", divs);
        assert_eq!((divs[0].class, divs[0].field.as_str()), (Class::WrongMessage, "M"));
        assert_eq!(divs[0].a.as_deref(), Some("could not find function \"no_such_symbol\" in file \"<LIBDIR>/pg_trgm.dylib\""));
        // Same file under each side's own libdir: no divergence.
        let a = vec![e(&[('S', "ERROR"), ('C', "42883"), ('M', "could not find function \"x\" in file \"/home/dev/dev/pg186-install/lib/postgresql/pg_trgm.dylib\"")])];
        let b = vec![e(&[('S', "ERROR"), ('C', "42883"), ('M', "could not find function \"x\" in file \"/opt/pgrust/lib/pg_trgm.dylib\"")])];
        assert!(compare_planes_with(&obs(CSide::A, a), &obs(CSide::B, b), &st, embedded_ledger(), &opts).is_empty());
        // dfmgr-9: file_name pg_trgm.dylib vs pg_trgm.so stays a wrong-result.
        let a = vec![t(&[("file_name", 25)]), d(&[Some("pg_trgm.dylib")]), c("SELECT 1")];
        let b = vec![t(&[("file_name", 25)]), d(&[Some("pg_trgm.so")]), c("SELECT 1")];
        let st = step("SELECT file_name FROM pg_get_loaded_modules() ORDER BY 1;", Ordered::Total);
        let divs = compare_planes_with(&obs(CSide::A, a), &obs(CSide::B, b), &st, embedded_ledger(), &opts);
        assert_eq!(divs.len(), 1, "{:?}", divs);
        assert_eq!((divs[0].plane.as_str(), divs[0].class, divs[0].field.as_str()), ("rows", Class::WrongResult, "cell"));
        assert!(divs[0].detail.contains("host DLSUFFIX is .dylib"), "{}", divs[0].detail);
    }

    /// ISO-OBS-1b / postgres-7: %m masked, the zone token compared.
    #[test]
    fn log_timestamp_masked_but_zone_token_compared() {
        let line = |ts: &str, raw: &str| LogLine {
            source: "stderr".into(),
            raw: Bytes::text(raw),
            ts: Some(ts.into()),
            backend_type: Some("client backend".into()),
            pid: Some(41233),
            app: Some("fuzz".into()),
            level: Some("LOG".into()),
            sqlstate: Some("00000".into()),
            message: Some(Bytes::text("disconnection: session time: 0:00:00.001 user=postgres database=postgres host=[local]")),
            location: None,
        };
        let mut a = obs(CSide::A, vec![c("SELECT 1")]);
        a.log = vec![line("2026-09-02 08:25:09.562 PDT", "2026-09-02 08:25:09.562 PDT [41233] LOG:  disconnection: session time: 0:00:00.001 user=postgres database=postgres host=[local]")];
        let mut b = obs(CSide::B, vec![c("SELECT 1")]);
        b.log = vec![line("2026-09-02 15:25:09.569 GMT", "2026-09-02 15:25:09.569 GMT [41240] LOG:  disconnection: session time: 0:00:00.002 user=postgres database=postgres host=[local]")];
        let st = step("SELECT 1;", Ordered::None);
        let divs = compare_planes(&a, &b, &st, embedded_ledger());
        assert_eq!(divs.len(), 1, "{:?}", divs);
        assert_eq!((divs[0].plane.as_str(), divs[0].class, divs[0].field.as_str()), ("log", Class::Cosmetic, "zone"));
        assert_eq!((divs[0].a.as_deref(), divs[0].b.as_deref()), (Some("PDT"), Some("GMT")));
        // Same zone, different digits and pid: nothing.
        b.log = vec![line("2026-09-02 08:25:10.001 PDT", "2026-09-02 08:25:10.001 PDT [41240] LOG:  disconnection: session time: 0:00:00.002 user=postgres database=postgres host=[local]")];
        assert!(compare_planes(&a, &b, &st, embedded_ledger()).is_empty());
    }

    #[test]
    fn tag_text_with_equal_counts_is_wrong_result_meta_tag() {
        let a = vec![c("INSERT 0 3")];
        let b = vec![c("INSERT 3")];
        let divs = compare(a, b, &step("INSERT INTO t SELECT 1;", Ordered::None));
        assert_eq!(divs.len(), 1, "{:?}", divs);
        assert_eq!((divs[0].plane.as_str(), divs[0].class, divs[0].field.as_str()), ("meta", Class::WrongResult, "tag"));
        assert!(divs[0].signature.starts_with("meta | INSERT INTO t SELECT 1; | -/- | tag INSERT 0 3→INSERT 3"), "{}", divs[0].signature);
        // RowDescription typmod / ParameterStatus key set are meta too.
        let a = vec![WireMsg::RowDescription(vec![ColDesc { name: Bytes::text("v"), tableoid: 0, attnum: 0, typoid: 1043, typlen: -1, typmod: 36, fmt: 0 }]), d(&[Some("x")]), c("SELECT 1")];
        let b = vec![WireMsg::RowDescription(vec![ColDesc { name: Bytes::text("v"), tableoid: 0, attnum: 0, typoid: 1043, typlen: -1, typmod: -1, fmt: 0 }]), d(&[Some("x")]), c("SELECT 1")];
        let divs = compare(a, b, &step("SELECT v FROM t;", Ordered::None));
        assert_eq!(divs.len(), 1);
        assert_eq!(divs[0].field, "typmod");
        let a = vec![WireMsg::ParameterStatus { name: Bytes::text("search_path"), value: Bytes::text("x") }, c("SET")];
        let b = vec![c("SET")];
        let divs = compare(a, b, &step("SET search_path = x;", Ordered::None));
        assert_eq!((divs.len(), divs[0].field.as_str()), (1, "params"));
    }

    #[test]
    fn ruled_hit_is_still_recorded_and_counted() {
        let ledger = Ledger::embedded();
        let a = vec![t(&[("count", 20)]), d(&[Some("1")]), c("SELECT 1")];
        let b = vec![e(&[('S', "ERROR"), ('C', "55000"), ('M', "parallel worker failed to initialize")])];
        let st = step("select count(*) from big;", Ordered::None);
        let divs = compare_planes(&obs(CSide::A, a), &obs(CSide::B, b), &st, &ledger);
        assert_eq!(divs.len(), 1, "{:?}", divs);
        let dv = &divs[0];
        assert_eq!((dv.plane.as_str(), dv.class), ("wire:E", Class::SpuriousError));
        assert_eq!(dv.rule.as_deref(), Some("parallel-worker-init"));
        assert_eq!(dv.status, Status::Ruled);
        assert_eq!(dv.triage(), "ruled:parallel-worker-init");
        assert_eq!(ledger.hits().get("parallel-worker-init"), Some(&1));
        assert!(json::to_canonical(&dv.to_json()).unwrap().contains("\"triage\":\"ruled:parallel-worker-init\""));
        assert_eq!(step_verdict(&divs).map(|d| d.rule.is_some()), Some(true));
    }

    #[test]
    fn ordered_mode_decides_reorder_class() {
        let a = vec![t(&[("c", 23)]), d(&[Some("1")]), d(&[Some("2")]), c("SELECT 2")];
        let b = vec![t(&[("c", 23)]), d(&[Some("2")]), d(&[Some("1")]), c("SELECT 2")];
        // total: a multiset-equal reorder is a wrong result, never ruled.
        let divs = compare(a.clone(), b.clone(), &step("SELECT c FROM t ORDER BY c;", Ordered::Total));
        assert_eq!(divs.len(), 1, "{:?}", divs);
        assert_eq!((divs[0].plane.as_str(), divs[0].class, divs[0].field.as_str()), ("rows", Class::WrongResult, "order"));
        assert!(divs[0].rule.is_none());
        // partial: the tie-order ruling absorbs it (still recorded).
        let divs = compare(a.clone(), b.clone(), &step("SELECT c FROM t ORDER BY c;", Ordered::Partial));
        assert_eq!(divs.len(), 1);
        assert_eq!(divs[0].rule.as_deref(), Some("tie-ordering"));
        // ... unless the statement is a COPY: copy-order comes first.
        let divs = compare(a.clone(), b.clone(), &step("COPY t TO STDOUT;", Ordered::Partial));
        assert_eq!(divs[0].rule.as_deref(), Some("copy-order"));
        // none: multiset compare, a match.
        assert!(compare(a, b, &step("SELECT c FROM t;", Ordered::None)).is_empty());
    }

    #[test]
    fn missing_feature_text_rule() {
        let a = vec![e(&[('S', "ERROR"), ('C', "0A000"), ('M', "some feature is not supported")])];
        let b = vec![e(&[('S', "ERROR"), ('C', "0A000"), ('M', "this SQL construct is not yet implemented (grammar rule 2445: AexprConst, gram.y:17387)")])];
        let divs = compare(a, b, &step("SELECT int4(1) '42';", Ordered::None));
        assert_eq!(divs.len(), 1);
        assert_eq!(divs[0].class, Class::MissingFeature);
        assert!(divs[0].detail.contains("UNPORTED grammar rule 2445"), "{}", divs[0].detail);
        // B-only "unported" error: missing-feature, not spurious-error.
        let b = vec![e(&[('S', "ERROR"), ('C', "0A000"), ('M', "xmltable is unported")])];
        let divs = compare(vec![c("SELECT 1")], b, &step("SELECT 1;", Ordered::None));
        assert_eq!(divs[0].class, Class::MissingFeature);
        assert_eq!(divs[0].field, "presence");
        // Both say "not supported" with different wording: wrong-message.
        let a = vec![e(&[('S', "ERROR"), ('C', "0A000"), ('M', "X is not supported")])];
        let b = vec![e(&[('S', "ERROR"), ('C', "0A000"), ('M', "Y is not supported")])];
        assert_eq!(compare(a, b, &step("SELECT 1;", Ordered::None))[0].class, Class::WrongMessage);
    }

    #[test]
    fn hint_and_routine_fields() {
        // A pgrust-only HINT is cosmetic until ruled.
        let a = vec![e(&[('S', "ERROR"), ('C', "42P01"), ('M', "relation \"t\" does not exist")])];
        let b = vec![e(&[('S', "ERROR"), ('C', "42P01"), ('M', "relation \"t\" does not exist"), ('H', "Did you mean \"tt\"?")])];
        let divs = compare(a.clone(), b, &step("SELECT * FROM t;", Ordered::None));
        assert_eq!((divs[0].class, divs[0].field.as_str(), divs[0].rule.is_none()), (Class::Cosmetic, "H", true));
        // An R-only delta is ruled by r-field (expiring, still recorded).
        let a = vec![e(&[('S', "ERROR"), ('C', "42P01"), ('M', "m"), ('R', "parserOpenTable")])];
        let b = vec![e(&[('S', "ERROR"), ('C', "42P01"), ('M', "m"), ('R', "parse_relation::open")])];
        let divs = compare(a, b, &step("SELECT * FROM t;", Ordered::None));
        assert_eq!((divs.len(), divs[0].rule.as_deref()), (1, Some("r-field")));
        // R plus P differing: the P delta is not hidden by r-field.
        let a = vec![e(&[('S', "ERROR"), ('C', "42P01"), ('M', "m"), ('R', "x"), ('P', "5")])];
        let b = vec![e(&[('S', "ERROR"), ('C', "42P01"), ('M', "m"), ('R', "y")])];
        let divs = compare(a, b, &step("SELECT * FROM t;", Ordered::None));
        assert_eq!((divs[0].rule.is_none(), divs[0].differing.clone()), (true, vec!["R".to_string(), "P".to_string()]));
        // C differs: wrong-sqlstate whatever else differs.
        let a = vec![e(&[('S', "ERROR"), ('C', "42P01"), ('M', "m")])];
        let b = vec![e(&[('S', "ERROR"), ('C', "3F000"), ('M', "n")])];
        assert_eq!(compare(a, b, &step("SELECT 1;", Ordered::None))[0].class, Class::WrongSqlstate);
    }

    #[test]
    fn notify_and_liveness_planes() {
        let a = vec![c("NOTIFY"), WireMsg::NotificationResponse { pid: 1, channel: Bytes::text("ch"), payload: Bytes::text("p") }];
        let b = vec![c("NOTIFY"), WireMsg::NotificationResponse { pid: 9, channel: Bytes::text("ch"), payload: Bytes::text("p") }];
        assert!(compare(a.clone(), b, &step("NOTIFY ch, 'p';", Ordered::None)).is_empty());
        let divs = compare(a, vec![c("NOTIFY")], &step("NOTIFY ch, 'p';", Ordered::None));
        assert_eq!((divs[0].plane.as_str(), divs[0].field.as_str()), ("notify", "channel"));
        // A B-side panic marker is a crash even with the same XX000 on both.
        let a = obs(CSide::A, vec![e(&[('S', "ERROR"), ('C', "XX000"), ('M', "too few entries in indexprs list")])]);
        let mut b = obs(CSide::B, vec![e(&[('S', "ERROR"), ('C', "XX000"), ('M', "too few entries in indexprs list")])]);
        b.panic = Some(contracts::Panic { site: "crates/backend/commands/analyze/src/lib.rs:606:44".into(), message: Bytes::text("too few entries in indexprs list"), query: None });
        let divs = compare_planes(&a, &b, &step("analyze s475_t;", Ordered::None), embedded_ledger());
        assert_eq!(divs.len(), 1);
        assert_eq!((divs[0].plane.as_str(), divs[0].class, divs[0].severity), ("panic", Class::Crash, Severity::Critical));
        // Precedence: a crash outranks everything.
        assert_eq!(class_rank(Class::Crash) < class_rank(Class::Hang), true);
        assert!(class_rank(Class::WrongSqlstate) < class_rank(Class::WrongResult));
        assert!(class_rank(Class::MissingFeature) < class_rank(Class::Cosmetic));
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

    #[test]
    fn calls_cmp_builtin_scans_words() {
        assert!(calls_cmp_builtin("select uuid_cmp(a,b);"));
        assert!(calls_cmp_builtin("select btint4cmp (1, 2);"));
        assert!(!calls_cmp_builtin("select cmp(1,2);")); // too short
        assert!(!calls_cmp_builtin("select uuid_cmp;")); // no call
        assert!(!calls_cmp_builtin("select compare(a,b);"));
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

    // ---- legacy adapter --------------------------------------------

    #[test]
    fn legacy_equal_sqlstate_match_arm_is_retired() {
        let e1 = err("22012", "division by zero");
        let e2 = err("22012", "division by zero somewhere");
        let e3 = err("22003", "out of range");
        assert_eq!(classify_sql("SELECT 1/0;", &e1, &e1).class, DiffClass::Match);
        // Same SQLSTATE, drifted message: a finding (wrong-message), no longer MATCH.
        let c = classify_sql("SELECT 1/0;", &e1, &e2);
        assert_eq!(c.class, DiffClass::ErrorDiff);
        assert!(c.detail.contains("field M differs"), "{}", c.detail);
        assert_eq!(classify_sql("SELECT 1/0;", &e1, &e3).class, DiffClass::ErrorDiff);
        assert_eq!(classify_sql("SELECT 1;", &e1, &ok1()).class, DiffClass::ErrorDiff);
        assert_eq!(classify_sql("SELECT 1;", &ok1(), &e1).class, DiffClass::ErrorDiff);
    }

    #[test]
    fn legacy_unported_grammar_rule_error_is_always_a_finding() {
        let a = err("0A000", "some feature is not supported");
        let b = err("0A000", "this SQL construct is not yet implemented (grammar rule 2445: AexprConst, gram.y:17387)");
        let c = classify_sql("SELECT int4(1) '42';", &a, &b);
        assert_eq!(c.class, DiffClass::ErrorDiff);
        assert!(c.detail.contains("UNPORTED grammar rule 2445"), "{}", c.detail);
    }

    #[test]
    fn legacy_build_config_and_carve_rulings() {
        let xml = err("0A000", "unsupported XML feature");
        assert_eq!(classify_sql("SELECT xml '<a/>';", &xml, &ok1()).class, ruled("xml-config"));
        assert_eq!(classify_sql("SELECT xml '<a/>';", &xml, &err("22000", "other")).class, ruled("xml-config"));
        assert_eq!(classify_sql("SELECT xml '<a/>';", &xml, &err("XX000", "panicked")).class, DiffClass::ErrorDiff);
        let lz4 = err("0A000", "compression method lz4 not supported");
        assert_eq!(classify_sql("ALTER TABLE t ALTER c SET COMPRESSION lz4;", &lz4, &ok1()).class, ruled("lz4-config"));
        assert_eq!(classify_sql("x;", &lz4, &err("XX000", "boom")).class, DiffClass::ErrorDiff);
        let carve = err("0A000", "server encoding \"EUC_CN\" is not supported by pgrust; only \"UTF8\" and \"SQL_ASCII\" server encodings are accepted (UTF-8-only carve, docs/design/carve-ratifications.md)");
        assert_eq!(classify_sql("CREATE DATABASE d ENCODING 'EUC_CN';", &ok1(), &carve).class, ruled("encoding-carve"));
        assert_eq!(classify_sql("CREATE DATABASE d ENCODING 'EUC_CN';", &err("22023", "mismatch"), &carve).class, ruled("encoding-carve"));
        assert_eq!(classify_sql("x;", &err("XX000", "boom"), &carve).class, DiffClass::ErrorDiff);
        // Other encoding errors escalate.
        assert_eq!(classify_sql("x;", &ok1(), &err("0A000", "encoding X is not supported")).class, DiffClass::ErrorDiff);
        let tid = err("22P02", "invalid input syntax for type tid: \"(0,)\"");
        assert_eq!(classify_sql("SELECT '(0,)'::tid;", &ok1(), &tid).class, ruled("tid-input-upstream"));
        assert_eq!(classify_sql("SELECT '(0,)'::tid;", &tid, &ok1()).class, DiffClass::ErrorDiff);
    }

    #[test]
    fn legacy_asymmetric_fault_rulings() {
        let pw = err("55000", "parallel worker failed to initialize");
        assert_eq!(classify_sql("select count(*) from big ;", &ok1(), &pw).class, ruled("parallel-worker-init"));
        assert_eq!(classify_sql("select 1 ;", &ok1(), &err("55000", "object not in prerequisite state")).class, DiffClass::ErrorDiff);
        assert_eq!(classify_sql("select 1 ;", &pw, &ok1()).class, DiffClass::ErrorDiff);
        let to = err("57014", "canceling statement due to statement timeout");
        assert_eq!(classify_sql("SELECT * FROM fz_par_0 ORDER BY pk;", &ok1(), &to).class, ruled("fault-stmt-timeout"));
        assert_eq!(classify_sql("select 1 ;", &ok1(), &err("57014", "canceling statement due to user request")).class, DiffClass::ErrorDiff);
        assert_eq!(classify_sql("select 1 ;", &to, &ok1()).class, DiffClass::ErrorDiff);
        let dl = err("40P01", "deadlock detected");
        assert_eq!(classify_sql("DROP TABLE fz_pa_t1, fz_pa_t2;", &ok1(), &dl).class, ruled("drop-autovacuum-deadlock"));
        assert_eq!(classify_sql("VACUUM (ANALYZE, FULL) t;", &ok1(), &dl).class, ruled("drop-autovacuum-deadlock"));
        assert_eq!(classify_sql("vacuum full t;", &ok1(), &dl).class, ruled("drop-autovacuum-deadlock"));
        assert_eq!(classify_sql("update fz_one set k_int = 2 ;", &ok1(), &dl).class, DiffClass::ErrorDiff);
        assert_eq!(classify_sql("DROP TABLE t;", &dl, &ok1()).class, DiffClass::ErrorDiff);
        let dup = err("23505", "duplicate key value violates unique constraint \"pg_db_role_setting_databaseid_rol_index\"");
        assert_eq!(classify_sql("ALTER ROLE ALL SET work_mem = '1MB';", &ok1(), &dup).class, ruled("role-setting-shared-race"));
        assert_eq!(classify_sql("alter user all reset work_mem;", &ok1(), &dup).class, ruled("role-setting-shared-race"));
        assert_eq!(classify_sql("ALTER ROLE r SET work_mem = '1MB';", &ok1(), &dup).class, DiffClass::ErrorDiff);
    }

    #[test]
    fn legacy_shared_state_races_rule_either_side() {
        let tcu = err("XX000", "tuple concurrently updated");
        assert_eq!(classify_sql("alter user current_user encrypted password 'x' ;", &ok1(), &tcu).class, ruled("shared-catalog-tcu"));
        assert_eq!(classify_sql("ALTER ROLE r SET x = 1;", &tcu, &ok1()).class, ruled("shared-catalog-tcu-a"));
        assert_eq!(classify_sql("REVOKE SET ON PARAMETER work_mem FROM r;", &ok1(), &err("XX000", "tuple concurrently deleted")).class, ruled("shared-catalog-tcu"));
        assert_eq!(classify_sql("ALTER TABLE t ADD COLUMN c int4;", &ok1(), &tcu).class, DiffClass::ErrorDiff);
        assert_eq!(classify_sql("GRANT SELECT ON TABLE t TO PUBLIC;", &ok1(), &tcu).class, DiffClass::ErrorDiff);
        assert_eq!(classify_sql("ALTER ROLE r SET x = 1;", &ok1(), &err("XX000", "other")).class, DiffClass::ErrorDiff);
        let ac = err("F0000", "could not parse contents of file \"postgresql.auto.conf\"");
        assert_eq!(classify_sql("alter system reset flag . fz_scalar . trim ;", &ok1(), &ac).class, ruled("autoconf-shared-race"));
        assert_eq!(classify_sql("ALTER SYSTEM SET x = 1;", &ac, &err("42704", "unrecognized")).class, ruled("autoconf-shared-race-a"));
        assert_eq!(classify_sql("SELECT 1;", &ok1(), &ac).class, DiffClass::ErrorDiff);
    }

    #[test]
    fn legacy_rowset_compare_and_order_mapping() {
        let a = rowset(vec![23], rows(&[&[Some("1")], &[Some("2")]]));
        let b = rowset(vec![23], rows(&[&[Some("2")], &[Some("1")]]));
        assert_eq!(classify_sql("SELECT c FROM t;", &a, &a).class, DiffClass::Match);
        // No ORDER BY: multiset (ordered: none).
        assert_eq!(classify_sql("SELECT c FROM t;", &a, &b).class, DiffClass::Match);
        // ORDER BY: ordered: partial — tie-order ruling, never a total key.
        assert_eq!(legacy_step("SELECT c FROM t ORDER BY 1;").ordered, Ordered::Partial);
        assert_eq!(classify_sql("SELECT c FROM t ORDER BY 1;", &a, &b).class, ruled("tie-ordering"));
        assert_eq!(classify_sql("COPY t TO STDOUT;", &a, &b).class, DiffClass::Match);
        let c3 = rowset(vec![23], rows(&[&[Some("3")]]));
        assert_eq!(classify_sql("SELECT c FROM t ORDER BY 1;", &a, &c3).class, DiffClass::RowsetDiff);
        let nul = rowset(vec![25], rows(&[&[None]]));
        let emp = rowset(vec![25], rows(&[&[Some("")]]));
        assert_eq!(classify_sql("SELECT c FROM t;", &nul, &emp).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn legacy_float_ulp_soft_and_geometry() {
        let x = 0.1f64 + 0.2f64;
        let fa = rowset(vec![FLOAT8_OID], rows(&[&[Some(&format!("{x:?}"))]]));
        let fb = rowset(vec![FLOAT8_OID], rows(&[&[Some("0.3")]]));
        assert_eq!(classify_sql("SELECT f FROM t ORDER BY 1;", &fa, &fb).class, ruled("b1-float-ulp"));
        assert_eq!(classify_sql("SELECT f FROM t;", &fa, &fb).class, ruled("b1-float-ulp"));
        let one = rowset(vec![FLOAT8_OID], rows(&[&[Some("1.0")]]));
        let off = rowset(vec![FLOAT8_OID], rows(&[&[Some("1.001")]]));
        assert_eq!(classify_sql("SELECT f FROM t ORDER BY 1;", &one, &off).class, DiffClass::RowsetDiff);
        // numeric columns compare exactly.
        let na = rowset(vec![1700], rows(&[&[Some("0.30000000000000004")]]));
        let nb = rowset(vec![1700], rows(&[&[Some("0.3")]]));
        assert_eq!(classify_sql("SELECT n FROM t ORDER BY 1;", &na, &nb).class, DiffClass::RowsetDiff);
        // Specials.
        let inf = rowset(vec![FLOAT8_OID], rows(&[&[Some("Infinity")]]));
        let ninf = rowset(vec![FLOAT8_OID], rows(&[&[Some("-Infinity")]]));
        assert_eq!(classify_sql("SELECT f FROM t;", &inf, &inf).class, DiffClass::Match);
        assert_eq!(classify_sql("SELECT f FROM t;", &inf, &ninf).class, DiffClass::RowsetDiff);
        // Soft columns accept any float divergence, never NULL vs value or non-float columns.
        let tiny = rowset(vec![FLOAT8_OID], rows(&[&[Some("1e-10")]]));
        let zero = rowset(vec![FLOAT8_OID], rows(&[&[Some("0")]]));
        assert_eq!(classify_soft("SELECT sum(f) FROM t;", &tiny, &zero, &[0]).class, ruled("b1-float-agg-soft"));
        assert_eq!(classify_soft("SELECT sum(f) FROM t;", &tiny, &zero, &[]).class, DiffClass::RowsetDiff);
        let nul = rowset(vec![FLOAT8_OID], rows(&[&[None]]));
        assert_eq!(classify_soft("SELECT sum(f) FROM t;", &tiny, &nul, &[0]).class, DiffClass::RowsetDiff);
        assert_eq!(classify_soft("SELECT sum(n) FROM t;", &na, &nb, &[0]).class, DiffClass::RowsetDiff);
        let two_a = rowset(vec![FLOAT8_OID, FLOAT8_OID], rows(&[&[Some("1.0"), Some("5.0")]]));
        let two_b = rowset(vec![FLOAT8_OID, FLOAT8_OID], rows(&[&[Some("1.001"), Some("6.0")]]));
        assert_eq!(classify_soft("SELECT f, sum(g) FROM t;", &two_a, &two_b, &[1]).class, DiffClass::RowsetDiff);
        // Geometry composites: token-wise ulp under the widened budget.
        let ga = rowset(vec![718], rows(&[&[Some("<(1,2),2.6925824035672523>")]]));
        let gb = rowset(vec![718], rows(&[&[Some("<(1,2),2.692582403567252>")]]));
        assert_eq!(classify_sql("SELECT circle(p) FROM t;", &ga, &gb).class, ruled("b1-float-ulp"));
        let y = f64::from_bits(2.6925824035672523f64.to_bits() + 15);
        let pa = rowset(vec![602], rows(&[&[Some(&format!("(({:?},1),(2,3))", 2.6925824035672523f64))]]));
        let pb = rowset(vec![602], rows(&[&[Some(&format!("(({y:?},1),(2,3))"))]]));
        assert_eq!(classify_sql("SELECT pth FROM t;", &pa, &pb).class, ruled("b1-float-ulp"));
        let ca = rowset(vec![718], rows(&[&[Some("<(1,2),3>")]]));
        let cb = rowset(vec![718], rows(&[&[Some("<(1,2),4>")]]));
        assert_eq!(classify_sql("SELECT c FROM t;", &ca, &cb).class, DiffClass::RowsetDiff);
        let pa = rowset(vec![604], rows(&[&[Some("((0,0),(1,1))")]]));
        let pb = rowset(vec![604], rows(&[&[Some("((0,0),(1,1),(2,2))")]]));
        assert_eq!(classify_sql("SELECT g FROM t;", &pa, &pb).class, DiffClass::RowsetDiff);
        let nan = rowset(vec![600], rows(&[&[Some("(NaN,1)")]]));
        let pt = rowset(vec![600], rows(&[&[Some("(0,1)")]]));
        assert_eq!(classify_sql("SELECT p FROM t;", &nan, &nan).class, DiffClass::Match);
        assert_eq!(classify_sql("SELECT p FROM t;", &nan, &pt).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn legacy_column_descriptor_compare() {
        let a = rowset(vec![23], rows(&[&[Some("1")]]));
        let b = rowset(vec![20], rows(&[&[Some("1")]]));
        let c = classify_sql("SELECT c FROM t;", &a, &b);
        assert_eq!(c.class, DiffClass::RowsetDiff);
        assert!(c.detail.contains("column 0 type oids"), "{}", c.detail);
        // User-range oids pair with user-range unconditionally (round-7 FP-3).
        let a = rowset(vec![23, 16385, 16401], rows(&[&[Some("1"), Some("x"), Some("y")]]));
        let b = rowset(vec![23, 16394, 16411], rows(&[&[Some("1"), Some("x"), Some("y")]]));
        assert_eq!(classify_sql("SELECT a, b, c FROM t;", &a, &b).class, DiffClass::Match);
        let a = rowset(vec![16385], rows(&[&[Some("x")]]));
        let b = rowset(vec![25], rows(&[&[Some("x")]]));
        assert_eq!(classify_sql("SELECT b FROM t;", &a, &b).class, DiffClass::RowsetDiff);
        // Rows vs command tag: a rowset diff.
        let cmd = StmtOutcome::Command { tag: "UPDATE 1".to_string(), affected: Some(1) };
        assert_eq!(classify_sql("x;", &ok1(), &cmd).class, DiffClass::RowsetDiff);
        assert_eq!(classify_sql("x;", &cmd, &ok1()).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn legacy_count_copy_and_session() {
        let a = StmtOutcome::Command { tag: "UPDATE 3".to_string(), affected: Some(3) };
        let b = StmtOutcome::Command { tag: "UPDATE 2".to_string(), affected: Some(2) };
        assert_eq!(classify_sql("UPDATE t SET c = 1;", &a, &b).class, DiffClass::CountDiff);
        assert_eq!(classify_sql("UPDATE t SET c = 1;", &a, &a.clone()).class, DiffClass::Match);
        // Full tag text compares: equal counts, different text is a CountDiff.
        let m = StmtOutcome::Command { tag: "MERGE 3".to_string(), affected: Some(3) };
        assert_eq!(classify_sql("MERGE INTO t ...;", &a, &m).class, DiffClass::CountDiff);
        let co = |bytes: &[u8], tag: &str| StmtOutcome::CopyOut { bytes: bytes.to_vec(), tag: tag.to_string() };
        let sql = "COPY t TO STDOUT (FORMAT binary);";
        assert_eq!(classify_sql(sql, &co(b"PGCOPY\n\xff\x0d\x0a\x00abc", "COPY 2"), &co(b"PGCOPY\n\xff\x0d\x0a\x00abc", "COPY 2")).class, DiffClass::Match);
        let c = classify_sql(sql, &co(b"PGCOPY\n\xff\x0d\x0a\x00abc", "COPY 2"), &co(b"PGCOPY\n\xff\x0d\x0a\x00abd", "COPY 2"));
        assert_eq!(c.class, DiffClass::RowsetDiff);
        assert!(c.detail.contains("differ at byte 13"), "{}", c.detail);
        let c = classify_sql(sql, &co(b"PGCOPY", "COPY 2"), &co(b"PGCOPY\n", "COPY 2"));
        assert!(c.detail.contains("len 6 vs 7"), "{}", c.detail);
        assert_eq!(classify_sql(sql, &co(b"x", "COPY 2"), &co(b"x", "COPY 3")).class, DiffClass::CountDiff);
        let cmd = StmtOutcome::Command { tag: "COPY 2".to_string(), affected: Some(2) };
        assert_eq!(classify_sql(sql, &co(b"x", "COPY 2"), &cmd).class, DiffClass::RowsetDiff);
        assert_eq!(classify_sql(sql, &cmd, &co(b"x", "COPY 2")).class, DiffClass::RowsetDiff);
        assert_eq!(classify_sql(sql, &err("42601", "m"), &co(b"x", "COPY 2")).class, DiffClass::ErrorDiff);
        let lost = StmtOutcome::ConnLost { detail: "server closed".to_string() };
        assert_eq!(classify_sql("SELECT 1;", &lost, &ok1()).class, DiffClass::SessionDiverged(Side::A));
        assert_eq!(classify_sql("SELECT 1;", &ok1(), &lost).class, DiffClass::SessionDiverged(Side::B));
        assert_eq!(classify_sql("SELECT 1;", &lost, &lost).class, DiffClass::SessionDiverged(Side::Both));
    }

    #[test]
    fn legacy_explain_masks() {
        let mk = |lines: &[&str]| rowset(vec![25], lines.iter().map(|l| vec![Some(l.to_string())]).collect());
        let sql = "EXPLAIN (COSTS OFF, SUMMARY OFF, ANALYZE, TIMING OFF, BUFFERS OFF) SELECT t0.s_int4 FROM fz_scalar t0;";
        let a = mk(&["Sort (actual rows=5 loops=1)", "  Sort Method: quicksort  Memory: 25kB"]);
        let b = mk(&["Sort (actual rows=5 loops=1)", "  Sort Method: external merge  Disk: 48kB"]);
        assert_eq!(classify_sql(sql, &a, &b).class, ruled("explain-counter"));
        let s = mk(&["Index Scan using fz_pk on fz_scalar t0 (actual rows=5 loops=1)"]);
        assert_eq!(classify_sql(sql, &a, &s).class, DiffClass::RowsetDiff);
        assert_eq!(classify_sql("SELECT c FROM t;", &a, &b).class, DiffClass::RowsetDiff);
        // Timing: opt-in only, shape strict.
        let mkt = |t: &str| mk(&[t, "Planning Time: 0.100 ms", "Execution Time: 0.200 ms"]);
        let ta = mkt("Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.003..0.004 rows=1.00 loops=1)");
        let tb = mkt("Result  (cost=0.00..0.01 rows=1 width=4) (actual time=0.011..0.190 rows=1.00 loops=1)");
        assert_eq!(classify_sql("explain analyze select 1 ;", &ta, &tb).class, DiffClass::RowsetDiff);
        assert_eq!(classify_timing("explain analyze select 1 ;", &ta, &tb).class, ruled("explain-timing"));
        let tb2 = mkt("Materialize  (cost=0.00..0.01 rows=1 width=4) (actual time=0.011..0.190 rows=1.00 loops=1)");
        assert_eq!(classify_timing("explain analyze select 1 ;", &ta, &tb2).class, DiffClass::RowsetDiff);
        // Planning: block presence.
        let pa = mk(&["Seq Scan on t  (cost=0.00..1.00 rows=1 width=4)", "Planning:", "  Buffers: shared hit=5 read=2"]);
        let pb = mk(&["Seq Scan on t  (cost=0.00..1.00 rows=1 width=4)"]);
        assert_eq!(classify_sql("explain select * from t ;", &pa, &pb).class, ruled("explain-planning-buffers"));
        let pb3 = mk(&["Index Scan using t_pkey on t  (cost=0.00..1.00 rows=1 width=4)"]);
        assert_eq!(classify_sql("explain select * from t ;", &pa, &pb3).class, DiffClass::RowsetDiff);
        assert_eq!(classify_sql("select * from t ;", &pa, &pb).class, DiffClass::RowsetDiff);
        // SCROLL Materialize wrap.
        let scroll_sql = "explain verbose declare xmlforest scroll asensitive scroll binary cursor for select ;";
        let ma = mk(&["Materialize  (cost=0.00..0.01 rows=1 width=0)", "  ->  Result  (cost=0.00..0.01 rows=1 width=0)"]);
        let mb = mk(&["Result  (cost=0.00..0.01 rows=1 width=0)"]);
        assert_eq!(classify_sql(scroll_sql, &ma, &mb).class, ruled("scroll-materialize"));
        assert_eq!(classify_sql("explain declare c no scroll cursor for select ;", &ma, &mb).class, DiffClass::RowsetDiff);
        let va = mk(&[
            "Materialize (actual time=0.001..0.001 rows=1.00 loops=1)",
            "  Output: 1",
            "  ->  Result (actual time=0.000..0.000 rows=1.00 loops=1)",
            "        Output: 1",
            "Planning Time: 0.030 ms",
            "Execution Time: 0.003 ms",
        ]);
        let vb = mk(&["Result (actual time=0.002..0.005 rows=1.00 loops=1)", "  Output: 1", "Planning Time: 0.051 ms", "Execution Time: 0.009 ms"]);
        let scroll_analyze_sql = "explain analyse verbose declare close binary insensitive scroll insensitive cursor for select 1 ;";
        assert_eq!(classify_timing(scroll_analyze_sql, &va, &vb).class, ruled("scroll-materialize"));
        let vb2 = mk(&["Result (actual time=0.002..0.005 rows=1.00 loops=1)", "  Output: 2", "Planning Time: 0.051 ms", "Execution Time: 0.009 ms"]);
        assert_eq!(classify_timing(scroll_analyze_sql, &va, &vb2).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn legacy_instance_state_and_guc_inventory() {
        let one = rowset(vec![23], rows(&[&[Some("1")]]));
        let two = rowset(vec![23], rows(&[&[Some("1")], &[Some("2")]]));
        for sql in [
            "SELECT rule_number FROM pg_hba_file_rules ORDER BY rule_number;",
            "select count(*) from PG_IDENT_FILE_MAPPINGS ;",
            "SELECT count(*) FROM pg_stat_progress_analyze;",
            "SELECT count(*) FROM pg_locks WHERE NOT granted;",
            "SELECT pg_stat_get_backend_pid(1);",
        ] {
            assert_eq!(classify_sql(sql, &one, &two).class, ruled("instance-config"), "{sql}");
        }
        assert_eq!(classify_sql("SELECT * FROM t;", &one, &two).class, DiffClass::RowsetDiff);
        let la = rowset(vec![3220], rows(&[&[Some("0/16000028")]]));
        let lb = rowset(vec![3220], rows(&[&[Some("0/1A000060")]]));
        assert_eq!(classify_sql("SELECT pg_backup_start('fz_q5_dup', true);", &la, &lb).class, ruled("instance-lsn"));
        assert_eq!(classify_sql("SELECT pg_switch_wal();", &la, &lb).class, ruled("instance-lsn"));
        assert_eq!(classify_sql("SELECT lsn FROM t;", &la, &lb).class, DiffClass::RowsetDiff);
        // GUC inventory: row-count shape only; a value diff stays a finding.
        assert_eq!(classify_sql("show all ;", &one, &two).class, ruled("guc-inventory"));
        assert_eq!(classify_sql("select name from pg_settings ;", &one, &two).class, ruled("guc-inventory"));
        let c1 = rowset(vec![20], rows(&[&[Some("370")]]));
        let c2 = rowset(vec![20], rows(&[&[Some("412")]]));
        assert_eq!(classify_sql("select count(*) from pg_settings ;", &c1, &c2).class, ruled("guc-inventory-count"));
        let v1 = rowset(vec![25], rows(&[&[Some("4MB")]]));
        let v2 = rowset(vec![25], rows(&[&[Some("8MB")]]));
        assert_eq!(classify_sql("select setting from pg_settings where name = 'work_mem';", &v1, &v2).class, DiffClass::RowsetDiff);
        assert_eq!(classify_sql("SHOW work_mem;", &v1, &v2).class, DiffClass::RowsetDiff);
    }

    #[test]
    fn legacy_oid_masks_and_cmp_sign() {
        let sql = "select pg_get_partition_constraintdef(oid) from pg_class ;";
        let a = rowset(vec![25], rows(&[&[Some("satisfies_hash_partition('48594'::oid, 8, 5, k)")]]));
        let b = rowset(vec![25], rows(&[&[Some("satisfies_hash_partition('37725'::oid, 8, 5, k)")]]));
        assert_eq!(classify_sql(sql, &a, &b).class, ruled("oid-literal"));
        let a = rowset(vec![25], rows(&[&[Some("f('23'::oid)")]]));
        let b = rowset(vec![25], rows(&[&[Some("f('25'::oid)")]]));
        assert_eq!(classify_sql(sql, &a, &b).class, DiffClass::RowsetDiff);
        let a = rowset(vec![25], rows(&[&[Some("f('48594'::oid, 8)")]]));
        let b = rowset(vec![25], rows(&[&[Some("f('37725'::oid, 9)")]]));
        assert_eq!(classify_sql(sql, &a, &b).class, DiffClass::RowsetDiff);
        let sql = "select relname from pg_class where relkind = 't' ;";
        let a = rowset(vec![19], rows(&[&[Some("pg_toast_48594")]]));
        let b = rowset(vec![19], rows(&[&[Some("pg_toast_37725")]]));
        assert_eq!(classify_sql(sql, &a, &b).class, ruled("toast-name"));
        let a = rowset(vec![19], rows(&[&[Some("pg_toast_2619")]]));
        let b = rowset(vec![19], rows(&[&[Some("pg_toast_2620")]]));
        assert_eq!(classify_sql(sql, &a, &b).class, DiffClass::RowsetDiff);
        // Binary container images.
        let rec = |typoid: u32| {
            let mut b = Vec::new();
            b.extend_from_slice(&2i32.to_be_bytes());
            for data in [b"ab".as_slice(), b"ok".as_slice()] {
                b.extend_from_slice(&typoid.to_be_bytes());
                b.extend_from_slice(&(data.len() as i32).to_be_bytes());
                b.extend_from_slice(data);
            }
            hex_cell(&b)
        };
        let sql = "select row('ab','ok')::fz_udt_c_0 ;";
        let a = rowset(vec![16389], rows(&[&[Some(&rec(16401))]]));
        let b = rowset(vec![16410], rows(&[&[Some(&rec(37725))]]));
        assert_eq!(classify_sql(sql, &a, &b).class, ruled("binary-udt-oid"));
        let mut bad = rec(37725);
        let fixed = bad.len() - 1;
        bad.replace_range(fixed.., "f");
        let b = rowset(vec![16410], rows(&[&[Some(&bad)]]));
        assert_eq!(classify_sql(sql, &a, &b).class, DiffClass::RowsetDiff);
        // *cmp() magnitude, text and binary int4 cells.
        let sql = "select uuid_cmp(a, b) from t ;";
        let a = rowset(vec![23], rows(&[&[Some("-238")]]));
        let b = rowset(vec![23], rows(&[&[Some("-1")]]));
        assert_eq!(classify_sql(sql, &a, &b).class, ruled("cmp-magnitude"));
        let flip = rowset(vec![23], rows(&[&[Some("238")]]));
        assert_eq!(classify_sql(sql, &a, &flip).class, DiffClass::RowsetDiff);
        assert_eq!(classify_sql("select a - b from t ;", &a, &b).class, DiffClass::RowsetDiff);
        let a8 = rowset(vec![20], rows(&[&[Some("-238")]]));
        let b8 = rowset(vec![20], rows(&[&[Some("-1")]]));
        assert_eq!(classify_sql(sql, &a8, &b8).class, DiffClass::RowsetDiff);
        let ha = rowset(vec![23], rows(&[&[Some("\\xffffff60")]]));
        let hb = rowset(vec![23], rows(&[&[Some("\\xffffffff")]]));
        assert_eq!(classify_sql(sql, &ha, &hb).class, ruled("cmp-magnitude"));
        assert_eq!(classify_sql(sql, &ha, &b).class, ruled("cmp-magnitude"));
        let h5a = rowset(vec![23], rows(&[&[Some("\\xffffff60aa")]]));
        let h5b = rowset(vec![23], rows(&[&[Some("\\xffffffffaa")]]));
        assert_eq!(classify_sql(sql, &h5a, &h5b).class, DiffClass::RowsetDiff);
    }
}
