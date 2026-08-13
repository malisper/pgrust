//! COPY options-matrix drain (lane COPYOPTS): the option-parse, validation
//! and formatting surface that the framing suites (crate::copybin field/line
//! decode, crate::copytext's positive matrix) leave hollow.
//!
//! copytext (Q8) drives the *valid* option matrix plus a handful of error
//! cases; this suite is deliberately complementary and error-arm-dense. It
//! targets the still-unhit spans reported for copy.c / copyto.c /
//! copyfromparse.c:
//!
//! - ProcessCopyOptions (copy.c) branch drain: the "specified more than
//!   once" / conflicting-option arm for EVERY option, the cross-option
//!   validation rejects (BINARY vs text/csv-only options, DELIMITER/NULL
//!   newline+CR rejects, delimiter-in-NULL, QUOTE/ESCAPE single-byte,
//!   FORCE_* mode/direction rejects, REJECT_LIMIT-requires-ON_ERROR,
//!   DEFAULT==NULL), and the unrecognized-option arm.
//! - defGetCopyHeaderChoice / defGetCopyOnErrorChoice /
//!   defGetCopyLogVerbosityChoice / defGetCopyRejectLimitOption illegal-value
//!   arms (copytext only ever feeds these helpers *valid* values).
//! - CopyAttributeOutText / CopyAttributeOutCSV formatting edges: the
//!   backslash-escape ladder (\b \f \n \r \t \v \\) and the
//!   custom-delimiter-in-value escape, plus the CSV force-quote decision
//!   (value == NULL string, value carries quote/delimiter/CR/NL). These are
//!   reached by round-tripping a control-char-hostile source table across an
//!   option matrix.
//!
//! Mechanism — differential round-trip identity. For a valid (TO opts, FROM
//! opts) pair: A (the reference engine) emits the source table with the TO
//! options; A's own bytes are then fed through COPY FROM into a fresh clone
//! on BOTH engines, the reloaded clone state is compared A-vs-B (total
//! ORDER BY), and the clone is re-emitted and byte-compared. Because A round-
//! trips its own output, any divergence in B's option parse, formatting, or
//! attribute decode surfaces as a state or payload diff. Option-error cases
//! are matched rejects: both engines must fail identically (SQLSTATE), and A
//! executing the reject drives the C validation arm regardless.
//!
//! Determinism: the source deck is insert-only and control-char content is
//! spelled with explicit E'' escapes (no locale/encoding drift); every clone
//! probe carries ORDER BY pk; the seeded arm draws all randomness from the
//! session PRNG (same seed => byte-identical option sets and payloads) and
//! only ever emits VALID option combinations (delimiter single-byte and
//! absent from the NULL string, NULL free of CR/NL, quote != delimiter), so
//! the seeded round-trips are self-consistent on the reference engine.

use crate::diff::{classify, DiffClass, DiffInput, StmtOutcome};
use crate::rng::Rng;
use crate::ruled::{apply_ruled, RuledEntry};
use crate::runner::{Executor, Record};

#[derive(Default, Debug)]
pub struct CopyOptsStats {
    pub cases: u32,
    pub matches: u32,
    pub ruled: u32,
    pub findings: u32,
}

// ------------------------------------------------------------- fixtures ----

/// Control-char / quote / delimiter-hostile source rows. Spelled with E''
/// so the bytes are exact and locale-independent. Every value is valid UTF-8
/// (the encoding-error surface belongs to copytext); the point here is the
/// out-function escape ladder and the CSV quoting decision.
const HOSTILE_V: &[&str] = &[
    r"plain",
    r"",
    r"tab\there",     // \t escape
    r"nl\nhere",      // \n escape
    r"cr\rhere",      // \r escape (distinct from \n)
    r"bs\bhere",      // \b backspace escape
    r"ff\fhere",      // \f form-feed escape
    r"vt\x0Bhere",    // \v vertical-tab escape
    r"back\\slash",   // literal backslash -> doubled
    "quote\"here",    // embeds the CSV quote char
    r"comma,here",    // embeds a common delimiter
    r"pipe|here",     // embeds the '|' custom delimiter
    r"semi;here",     // embeds the ';' custom delimiter
    r"NULL",          // NULL-string lookalike
    r"\N",            // text default-NULL lookalike
    r"  padded  ",    // leading/trailing spaces (CSV/csv trim edges)
    "caf\u{00e9}",    // 2-byte UTF-8 out path
];

/// Deck DDL + seed rows: one control-char text column (v), one delimiter-
/// bearing text column (w), an int and a nullable numeric so non-text
/// out-funcs ride the round-trip too. Insert-only forever after.
pub fn suite_ddl() -> Vec<String> {
    let mut v: Vec<String> = vec![
        "CREATE TABLE fz_co_src (pk int4 PRIMARY KEY, v text, w text, i int4, n numeric);"
            .to_string(),
    ];
    for (idx, s) in HOSTILE_V.iter().enumerate() {
        // v is spelled as an E'' literal (already escaped in the corpus); w
        // carries a delimiter-rich but escape-free string.
        v.push(format!(
            "INSERT INTO fz_co_src VALUES ({pk}, E'{v}', 'w|{pk},{pk};end', {i}, {n});",
            pk = idx + 1,
            v = s.replace('\'', "''"),
            i = (idx as i64) * 13 - 5,
            n = if idx % 5 == 0 {
                "NULL".to_string()
            } else {
                format!("{}.{:02}", idx, (idx * 7) % 100)
            },
        ));
    }
    // An all-NULL row (drives the NULL-print / NULL-quote arms on every col).
    v.push(
        format!(
            "INSERT INTO fz_co_src VALUES ({}, NULL, NULL, NULL, NULL);",
            HOSTILE_V.len() + 1
        ),
    );
    // Clone target for the round-trips (same shape incl. any defaults).
    v.push("CREATE TABLE fz_co_dst (LIKE fz_co_src INCLUDING ALL);".to_string());
    v
}

// -------------------------------------------------- option-error arms ------

/// Matched option-error deck: both engines must reject identically. A
/// executing the reject drives the corresponding ProcessCopyOptions /
/// defGetCopy* arm. Grouped by the branch each targets.
pub fn error_cases() -> Vec<String> {
    vec![
        // "specified more than once" / conflicting-option arm, per option.
        "COPY fz_co_src TO STDOUT (FORMAT text, FORMAT csv);".into(),
        "COPY fz_co_src TO STDOUT (DELIMITER '|', DELIMITER ';');".into(),
        "COPY fz_co_src TO STDOUT (NULL 'a', NULL 'b');".into(),
        "COPY fz_co_src TO STDOUT (HEADER, HEADER false);".into(),
        "COPY fz_co_src TO STDOUT (FORMAT csv, QUOTE '\"', QUOTE '#');".into(),
        "COPY fz_co_src TO STDOUT (FORMAT csv, ESCAPE '\\', ESCAPE '#');".into(),
        "COPY fz_co_src TO STDOUT (FORMAT csv, FORCE_QUOTE (v), FORCE_QUOTE (w));".into(),
        "COPY fz_co_src TO STDOUT (ENCODING 'UTF8', ENCODING 'LATIN1');".into(),
        "COPY fz_co_dst FROM STDIN (FORMAT csv, FORCE_NOT_NULL (v), FORCE_NOT_NULL (w));".into(),
        "COPY fz_co_dst FROM STDIN (FORMAT csv, FORCE_NULL (v), FORCE_NULL (w));".into(),
        "COPY fz_co_dst FROM STDIN (ON_ERROR ignore, ON_ERROR stop);".into(),
        "COPY fz_co_dst FROM STDIN (LOG_VERBOSITY verbose, LOG_VERBOSITY silent);".into(),
        "COPY fz_co_dst FROM STDIN (DEFAULT '\\D', DEFAULT '\\E');".into(),
        "COPY fz_co_dst FROM STDIN (FREEZE, FREEZE);".into(),
        "COPY fz_co_dst FROM STDIN (ON_ERROR ignore, REJECT_LIMIT 2, REJECT_LIMIT 3);".into(),
        // defGetCopy* illegal-value arms.
        "COPY fz_co_src TO STDOUT (FORMAT nonesuch);".into(),
        "COPY fz_co_src TO STDOUT (HEADER bogus);".into(),
        "COPY fz_co_dst FROM STDIN (ON_ERROR bogus);".into(),
        "COPY fz_co_dst FROM STDIN (ON_ERROR ignore, LOG_VERBOSITY bogus);".into(),
        "COPY fz_co_dst FROM STDIN (ON_ERROR ignore, REJECT_LIMIT 0);".into(),
        "COPY fz_co_dst FROM STDIN (ON_ERROR ignore, REJECT_LIMIT -1);".into(),
        // BINARY-mode cross-option rejects.
        "COPY fz_co_src TO STDOUT (FORMAT binary, DELIMITER '|');".into(),
        "COPY fz_co_src TO STDOUT (FORMAT binary, NULL 'x');".into(),
        "COPY fz_co_src TO STDOUT (FORMAT binary, QUOTE '\"');".into(),
        "COPY fz_co_src TO STDOUT (FORMAT binary, ESCAPE '\\');".into(),
        "COPY fz_co_dst FROM STDIN (FORMAT binary, DEFAULT '\\D');".into(),
        // DELIMITER / NULL newline+CR rejects.
        "COPY fz_co_src TO STDOUT (DELIMITER E'\\n');".into(),
        "COPY fz_co_src TO STDOUT (DELIMITER E'\\r');".into(),
        "COPY fz_co_src TO STDOUT (NULL E'x\\ny');".into(),
        "COPY fz_co_src TO STDOUT (NULL E'x\\ry');".into(),
        // delimiter must not appear in NULL.
        "COPY fz_co_src TO STDOUT (DELIMITER ',', NULL 'a,b');".into(),
        // QUOTE / ESCAPE single-one-byte-char rejects (csv).
        "COPY fz_co_src TO STDOUT (FORMAT csv, QUOTE 'ab');".into(),
        "COPY fz_co_src TO STDOUT (FORMAT csv, ESCAPE 'ab');".into(),
        "COPY fz_co_src TO STDOUT (FORMAT csv, QUOTE '');".into(),
        // ESCAPE / FORCE_* require-CSV rejects (non-csv text mode).
        "COPY fz_co_src TO STDOUT (FORMAT text, ESCAPE '\\');".into(),
        "COPY fz_co_dst FROM STDIN (FORMAT text, FORCE_NOT_NULL (v));".into(),
        "COPY fz_co_dst FROM STDIN (FORMAT text, FORCE_NULL (v));".into(),
        // FORCE_QUOTE direction/mode rejects.
        "COPY fz_co_dst FROM STDIN (FORMAT csv, FORCE_QUOTE (v));".into(),
        "COPY fz_co_src TO STDOUT (FORMAT text, FORCE_QUOTE (v));".into(),
        // REJECT_LIMIT requires ON_ERROR ignore.
        "COPY fz_co_dst FROM STDIN (REJECT_LIMIT 3);".into(),
        // DEFAULT == NULL collision.
        "COPY fz_co_dst FROM STDIN (NULL '\\D', DEFAULT '\\D');".into(),
        // unrecognized option name.
        "COPY fz_co_src TO STDOUT (FROBNICATE 1);".into(),
    ]
}

// ---------------------------------------------------- round-trip matrix ----

/// One round-trip: emit fz_co_src with `to_opts`, reload into fz_co_dst with
/// `from_opts` (which must be able to read what `to_opts` wrote).
pub struct RoundTrip {
    pub label: &'static str,
    pub to_opts: &'static str,
    pub from_opts: &'static str,
}

/// Valid (TO, FROM) pairs spanning the text/csv formatting surface. The
/// control-char source drives the out-function escape ladder on TO and the
/// attribute decode on FROM.
pub fn roundtrips() -> Vec<RoundTrip> {
    vec![
        RoundTrip { label: "text-default", to_opts: "", from_opts: "" },
        RoundTrip {
            label: "text-delim-pipe-null-na",
            to_opts: "(FORMAT text, DELIMITER '|', NULL 'NA')",
            from_opts: "(FORMAT text, DELIMITER '|', NULL 'NA')",
        },
        RoundTrip {
            label: "text-delim-tab",
            to_opts: "(FORMAT text, DELIMITER E'\\t')",
            from_opts: "(FORMAT text, DELIMITER E'\\t')",
        },
        RoundTrip {
            label: "text-null-empty",
            to_opts: "(FORMAT text, NULL '')",
            from_opts: "(FORMAT text, NULL '')",
        },
        RoundTrip { label: "csv-default", to_opts: "(FORMAT csv)", from_opts: "(FORMAT csv)" },
        RoundTrip {
            label: "csv-header-match",
            to_opts: "(FORMAT csv, HEADER)",
            from_opts: "(FORMAT csv, HEADER match)",
        },
        RoundTrip {
            label: "csv-custom-quote-escape",
            to_opts: "(FORMAT csv, DELIMITER ';', QUOTE '''', ESCAPE '\\')",
            from_opts: "(FORMAT csv, DELIMITER ';', QUOTE '''', ESCAPE '\\')",
        },
        RoundTrip {
            label: "csv-force-quote-star",
            to_opts: "(FORMAT csv, FORCE_QUOTE *)",
            from_opts: "(FORMAT csv)",
        },
        RoundTrip {
            label: "csv-null-token",
            to_opts: "(FORMAT csv, NULL 'NULLTOK')",
            from_opts: "(FORMAT csv, NULL 'NULLTOK')",
        },
        RoundTrip {
            label: "csv-quote-hash-delim-caret",
            to_opts: "(FORMAT csv, DELIMITER '^', QUOTE '#')",
            from_opts: "(FORMAT csv, DELIMITER '^', QUOTE '#')",
        },
    ]
}

// --------------------------------------------------------- seeded arm ------

/// One seeded round-trip request: a valid option string usable both as the
/// TO tail and (for `from_opts`) the FROM tail.
pub struct SeededRT {
    pub to_opts: String,
    pub from_opts: String,
}

/// Build a deterministic list of valid seeded round-trip option sets. Only
/// valid combinations are emitted (see the module invariants); option errors
/// live in the hand-verified error deck.
pub fn seeded_roundtrips(seed: u64, n: u32) -> Vec<SeededRT> {
    let mut rng = Rng::new(seed ^ 0xc0_5e_ed_09);
    let mut out = Vec::new();
    // Single-byte delimiters that are never CR/NL and never appear in the
    // null strings below.
    let delims = ['\t', '|', ',', ';', '^', '~'];
    let null_strs = ["", "NA", "NULLTOK", "\\N", "@@"];
    for _ in 0..n {
        let csv = rng.chance(1, 2);
        let delim = *rng.pick(&delims);
        // Pick a null string that does not contain the delimiter.
        let mut nullstr = null_strs[rng.below_usize(null_strs.len())];
        if nullstr.contains(delim) {
            nullstr = "NA";
        }
        if nullstr.contains(delim) {
            nullstr = "";
        }
        let mut opts: Vec<String> =
            vec![format!("FORMAT {}", if csv { "csv" } else { "text" })];
        if delim != '\t' || rng.chance(1, 3) {
            opts.push(format!("DELIMITER E'{}'", esc_ch(delim)));
        }
        if !nullstr.is_empty() || rng.chance(1, 4) {
            opts.push(format!("NULL E'{}'", esc_str(nullstr)));
        }
        // CSV quote/escape: keep quote != delimiter.
        let mut to_extra = String::new();
        if csv {
            let quote = if delim == '"' { '#' } else { '"' };
            if rng.chance(1, 2) {
                opts.push(format!("QUOTE '{}'", if quote == '\'' { "''" } else { "\"" }));
            }
            if rng.chance(1, 3) {
                to_extra = ", FORCE_QUOTE *".to_string();
            }
        }
        let base = opts.join(", ");
        let to_opts = format!("({base}{to_extra})");
        let from_opts = format!("({base})");
        out.push(SeededRT { to_opts, from_opts });
    }
    out
}

fn esc_ch(c: char) -> String {
    match c {
        '\t' => "\\t".to_string(),
        '\'' => "''".to_string(),
        '\\' => "\\\\".to_string(),
        c => c.to_string(),
    }
}

fn esc_str(s: &str) -> String {
    s.chars().map(esc_ch).collect()
}

// ------------------------------------------------------------- running ----

fn is_ok(o: &StmtOutcome) -> bool {
    !matches!(o, StmtOutcome::Error { .. } | StmtOutcome::ConnLost { .. })
}

#[allow(clippy::too_many_arguments)]
fn record_case(
    sql: &str,
    oa: &StmtOutcome,
    ob: &StmtOutcome,
    table: &[RuledEntry],
    ulp_tol: u64,
    case_index: u32,
    records: &mut Vec<Record>,
    stats: &mut CopyOptsStats,
) -> DiffClass {
    let raw = classify(&DiffInput { sql, a: oa, b: ob, ulp_tol, soft_cols: &[] });
    let c = apply_ruled(table, sql, raw);
    stats.cases += 1;
    match &c.class {
        DiffClass::Match => stats.matches += 1,
        DiffClass::Ruled(_) => {
            stats.ruled += 1;
            records.push(Record {
                stmt_index: case_index,
                sql: sql.to_string(),
                class: c.class.clone(),
                detail: c.detail.clone(),
                probe: false,
            });
        }
        _ => {
            stats.findings += 1;
            records.push(Record {
                stmt_index: case_index,
                sql: sql.to_string(),
                class: c.class.clone(),
                detail: c.detail.clone(),
                probe: false,
            });
        }
    }
    c.class
}

/// Run the full differential suite. `a` is the reference engine; its COPY TO
/// payloads are the spec fed back into both engines on the round-trips.
pub fn run_suite(
    a: &mut dyn Executor,
    b: &mut dyn Executor,
    table: &[RuledEntry],
    ulp_tol: u64,
    seed: u64,
    seeded_n: u32,
) -> (Vec<Record>, CopyOptsStats) {
    let mut records = Vec::new();
    let mut stats = CopyOptsStats::default();
    let mut idx = 0u32;
    let mut next = || {
        idx += 1;
        idx - 1
    };

    let duo = |sql: &str,
               a: &mut dyn Executor,
               b: &mut dyn Executor,
               records: &mut Vec<Record>,
               stats: &mut CopyOptsStats,
               next: &mut dyn FnMut() -> u32| {
        let oa = a.apply(sql);
        let ob = b.apply(sql);
        record_case(sql, &oa, &ob, table, ulp_tol, next(), records, stats)
    };

    // 0. DDL.
    for sql in suite_ddl() {
        duo(&sql, a, b, &mut records, &mut stats, &mut next);
    }

    // 1. Matched option-error arms.
    for sql in error_cases() {
        duo(&sql, a, b, &mut records, &mut stats, &mut next);
    }

    // 2. Fixed round-trip matrix: A emits, both reload A's bytes, probe, re-emit.
    for rt in roundtrips() {
        run_one_roundtrip(
            rt.to_opts, rt.from_opts, a, b, table, ulp_tol, &mut records, &mut stats, &mut next,
        );
    }

    // 3. Seeded round-trips.
    for rt in seeded_roundtrips(seed, seeded_n) {
        run_one_roundtrip(
            &rt.to_opts, &rt.from_opts, a, b, table, ulp_tol, &mut records, &mut stats, &mut next,
        );
    }

    (records, stats)
}

/// Execute one round-trip against both engines, using A's emitted bytes as
/// the shared spec. Skips the reload/probe if A could not emit (e.g. an
/// option combination the reference rejects — the emit diff is still banked).
#[allow(clippy::too_many_arguments)]
fn run_one_roundtrip(
    to_opts: &str,
    from_opts: &str,
    a: &mut dyn Executor,
    b: &mut dyn Executor,
    table: &[RuledEntry],
    ulp_tol: u64,
    records: &mut Vec<Record>,
    stats: &mut CopyOptsStats,
    next: &mut dyn FnMut() -> u32,
) {
    let sep_to = if to_opts.is_empty() { "" } else { " " };
    let copy_to = format!("COPY fz_co_src TO STDOUT{sep_to}{to_opts};");
    let oa = a.apply(&copy_to);
    let ob = b.apply(&copy_to);
    record_case(&copy_to, &oa, &ob, table, ulp_tol, next(), records, stats);
    let StmtOutcome::CopyOut { bytes, .. } = &oa else { return };
    let spec = bytes.clone();

    // Fresh clone state on both sides.
    let trunc = "TRUNCATE fz_co_dst;";
    let ta = a.apply(trunc);
    let tb = b.apply(trunc);
    record_case(trunc, &ta, &tb, table, ulp_tol, next(), records, stats);

    let sep_from = if from_opts.is_empty() { "" } else { " " };
    let copy_from = format!("COPY fz_co_dst FROM STDIN{sep_from}{from_opts};");
    let fa = a.apply_copy_in(&copy_from, &spec);
    let fb = b.apply_copy_in(&copy_from, &spec);
    record_case(&copy_from, &fa, &fb, table, ulp_tol, next(), records, stats);

    if is_ok(&fa) && is_ok(&fb) {
        // Round-trip identity witness: A reloaded its own bytes, so the clone
        // state must match the source on A; the A-vs-B compare then exposes
        // any divergence in B's parse/format path.
        let probe = "SELECT * FROM fz_co_dst ORDER BY pk;";
        let pa = a.apply(probe);
        let pb = b.apply(probe);
        record_case(probe, &pa, &pb, table, ulp_tol, next(), records, stats);
        // Symmetric-difference against the source (0 on the reference; a
        // non-zero on either side is a lossy round-trip).
        let symdiff = "SELECT count(*) FROM (SELECT * FROM fz_co_src EXCEPT SELECT * FROM fz_co_dst) x;";
        let sa = a.apply(symdiff);
        let sb = b.apply(symdiff);
        record_case(symdiff, &sa, &sb, table, ulp_tol, next(), records, stats);
        // Re-emit the reloaded clone with the same TO options.
        let re_emit = format!("COPY fz_co_dst TO STDOUT{sep_to}{to_opts};");
        let ra = a.apply(&re_emit);
        let rb = b.apply(&re_emit);
        record_case(&re_emit, &ra, &rb, table, ulp_tol, next(), records, stats);
    }
}

/// Single-engine deck (coverage runs): same cases, engine's own payloads.
pub fn run_single(engine: &mut dyn Executor, seed: u64, seeded_n: u32) -> (u32, u32) {
    let (mut applied, mut errors) = (0u32, 0u32);
    let run = |ex: &mut dyn Executor, sql: &str, applied: &mut u32, errors: &mut u32| {
        *applied += 1;
        let o = ex.apply(sql);
        if !is_ok(&o) {
            *errors += 1;
        }
        o
    };

    for sql in suite_ddl() {
        run(engine, &sql, &mut applied, &mut errors);
    }
    for sql in error_cases() {
        run(engine, &sql, &mut applied, &mut errors);
    }

    let do_rt = |engine: &mut dyn Executor, to_opts: &str, from_opts: &str, applied: &mut u32, errors: &mut u32| {
        let sep_to = if to_opts.is_empty() { "" } else { " " };
        let o = run(
            engine,
            &format!("COPY fz_co_src TO STDOUT{sep_to}{to_opts};"),
            applied,
            errors,
        );
        let StmtOutcome::CopyOut { bytes, .. } = o else { return };
        run(engine, "TRUNCATE fz_co_dst;", applied, errors);
        let sep_from = if from_opts.is_empty() { "" } else { " " };
        *applied += 1;
        let fo = engine
            .apply_copy_in(&format!("COPY fz_co_dst FROM STDIN{sep_from}{from_opts};"), &bytes);
        if !is_ok(&fo) {
            *errors += 1;
            return;
        }
        run(engine, "SELECT * FROM fz_co_dst ORDER BY pk;", applied, errors);
        run(
            engine,
            "SELECT count(*) FROM (SELECT * FROM fz_co_src EXCEPT SELECT * FROM fz_co_dst) x;",
            applied,
            errors,
        );
        run(
            engine,
            &format!("COPY fz_co_dst TO STDOUT{sep_to}{to_opts};"),
            applied,
            errors,
        );
    };

    for rt in roundtrips() {
        do_rt(engine, rt.to_opts, rt.from_opts, &mut applied, &mut errors);
    }
    for rt in seeded_roundtrips(seed, seeded_n) {
        do_rt(engine, &rt.to_opts, &rt.from_opts, &mut applied, &mut errors);
    }
    (applied, errors)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decks_are_deterministic() {
        assert_eq!(suite_ddl(), suite_ddl());
        assert_eq!(error_cases(), error_cases());
        let a = roundtrips();
        let b = roundtrips();
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.to_opts, y.to_opts);
            assert_eq!(x.from_opts, y.from_opts);
        }
    }

    #[test]
    fn error_deck_is_all_copy_on_the_fixture() {
        for sql in error_cases() {
            assert!(sql.starts_with("COPY fz_co_"), "not a fixture COPY: {sql}");
            assert!(sql.trim_end().ends_with(';'), "unterminated: {sql}");
        }
    }

    #[test]
    fn seeded_arm_is_seed_stable_and_seed_sensitive() {
        let render = |seed: u64| {
            seeded_roundtrips(seed, 40)
                .iter()
                .map(|r| format!("{}<<{}", r.to_opts, r.from_opts))
                .collect::<Vec<_>>()
        };
        assert_eq!(render(3), render(3));
        assert_ne!(render(3), render(4));
    }

    /// The seeded arm only ever emits valid combinations: the DELIMITER byte
    /// never appears inside the NULL string (the ProcessCopyOptions reject we
    /// exercise deliberately in the hand-written error deck, never by
    /// accident in the round-trip arm).
    #[test]
    fn seeded_null_never_contains_delimiter() {
        for seed in 0..8u64 {
            for rt in seeded_roundtrips(seed, 100) {
                let s = &rt.to_opts;
                if let Some(d) = s.find("DELIMITER E'") {
                    let rest = &s[d + 12..];
                    let delim = &rest[..rest.find('\'').unwrap()];
                    if let Some(n) = s.find("NULL E'") {
                        let nrest = &s[n + 7..];
                        let nul = &nrest[..nrest.find('\'').unwrap()];
                        assert!(
                            !nul.contains(delim),
                            "NULL {nul:?} contains delimiter {delim:?}: {s}"
                        );
                    }
                }
            }
        }
    }

    /// Every fixed round-trip whose TO writes a CSV header reloads with a
    /// header-consuming FROM (HEADER match), so the round-trip does not shift
    /// by one row.
    #[test]
    fn header_roundtrips_consume_the_header() {
        for rt in roundtrips() {
            if rt.to_opts.contains("HEADER") {
                assert!(
                    rt.from_opts.contains("HEADER"),
                    "TO writes header but FROM does not skip it: {}",
                    rt.label
                );
            }
        }
    }
}
