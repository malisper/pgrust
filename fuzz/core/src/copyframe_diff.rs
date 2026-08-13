//! copyframe_{text,binary}_diff — SHIPPED Rust COPY field parse
//! (copy_cmd::fromparse, reached through `bench_internals`) vs VERBATIM
//! vendored PostgreSQL 18.3 C (csrc/pg_copyframe_io.c: CopyReadAttributesText
//! + CopyReadBinaryData/CopyGetInt/CopyReadBinaryAttribute framing @ upstream
//! sha 62d6c7d3df). The Q8-F1 memory-safety surface EDGE2 flagged as having
//! NO verbatim-C oracle: the COPY text de-escape/delimiter split and the
//! binary per-field length framing (the `-1` NULL sentinel vs illegal
//! negatives / oversize length words — the `COPY_FIELD_LEN` bank).
//!
//! THE BAR (findings-edge2.md): pgrust must ACCEPT-or-REJECT each malformed /
//! empty input IDENTICALLY to the verbatim C. A pgrust PANIC / OOB / assert
//! where C cleanly rejects (a framing error) is HIGH (the Q8 class); a
//! both-reject / both-accept image mismatch is also HIGH; a both-reject
//! sqlstate-CLASS refinement is tracked separately (see COPY-1 in
//! findings-vendor-copy.md).
//!
//! Comparison planes: (1) accept-vs-reject, (2) full field image when both
//! accept — per field: SQL NULL, or the exact bytes (text: cstring up to the
//! first NUL, the `fields_of` contract; binary: the raw field bytes), (3)
//! errcode CLASS when both reject. A pgrust panic unwinds through the shipped
//! parse and is caught by the edge campaign's `catch_unwind` as a Finding.
//!
//! Encoding is carved: the shipped side runs under the default server
//! encoding PG_SQL_ASCII (pg_verify_mbstr is then a no-op), and the C shim's
//! pg_verifymbstr is likewise a no-op — so the invalid-encoding plane agrees
//! trivially and the de-escape/framing surface is isolated (see the C header).

use std::ffi::{c_char, c_int};

use copy_cmd::bench_internals;

extern "C" {
    fn cpf_copy_read_attrs_text(
        line: *const c_char,
        line_len: c_int,
        delim: c_char,
        null_print: *const c_char,
        null_print_len: c_int,
        max_fields: c_int,
        out_isnull: *mut c_int,
        out_bytes: *mut u8,
        out_bytes_cap: c_int,
        out_off: *mut c_int,
        out_len: *mut c_int,
        out_fields_cap: c_int,
        out_errclass: *mut c_int,
    ) -> c_int;

    fn cpf_copy_read_binary_fields(
        data: *const c_char,
        len: c_int,
        nattrs: c_int,
        out_isnull: *mut c_int,
        out_bytes: *mut u8,
        out_bytes_cap: c_int,
        out_off: *mut c_int,
        out_len: *mut c_int,
        out_fields_cap: c_int,
        out_errclass: *mut c_int,
    ) -> c_int;
}

const OUT_BYTES_CAP: usize = 1 << 16; // > any bounded field payload
const OUT_FIELDS_CAP: usize = 4096;
const NULL_PRINT: &str = "\\N"; // the default COPY null marker (must be &'static)

/// A field-parse result on either side: reject with an errcode class, or a
/// field vector (each `None` = SQL NULL, `Some(bytes)` = the field image).
type ParseResult = Result<Vec<Option<Vec<u8>>>, i32>;

/// Call the C text oracle and marshal its flat output into a [`ParseResult`].
fn c_text(line: &[u8], delim: u8, max_fields: usize) -> ParseResult {
    let mut isn = vec![0 as c_int; OUT_FIELDS_CAP];
    let mut off = vec![0 as c_int; OUT_FIELDS_CAP];
    let mut flen = vec![0 as c_int; OUT_FIELDS_CAP];
    let mut bytes = vec![0u8; OUT_BYTES_CAP];
    let mut errclass: c_int = 0;
    let np = NULL_PRINT.as_bytes();
    let n = unsafe {
        cpf_copy_read_attrs_text(
            line.as_ptr() as *const c_char,
            line.len() as c_int,
            delim as c_char,
            np.as_ptr() as *const c_char,
            np.len() as c_int,
            max_fields as c_int,
            isn.as_mut_ptr(),
            bytes.as_mut_ptr(),
            OUT_BYTES_CAP as c_int,
            off.as_mut_ptr(),
            flen.as_mut_ptr(),
            OUT_FIELDS_CAP as c_int,
            &mut errclass,
        )
    };
    marshal(n, errclass, &isn, &off, &flen, &bytes)
}

/// Call the C binary oracle and marshal its output.
fn c_binary(data: &[u8], nattrs: usize) -> ParseResult {
    let mut isn = vec![0 as c_int; OUT_FIELDS_CAP];
    let mut off = vec![0 as c_int; OUT_FIELDS_CAP];
    let mut flen = vec![0 as c_int; OUT_FIELDS_CAP];
    let mut bytes = vec![0u8; OUT_BYTES_CAP];
    let mut errclass: c_int = 0;
    let n = unsafe {
        cpf_copy_read_binary_fields(
            data.as_ptr() as *const c_char,
            data.len() as c_int,
            nattrs as c_int,
            isn.as_mut_ptr(),
            bytes.as_mut_ptr(),
            OUT_BYTES_CAP as c_int,
            off.as_mut_ptr(),
            flen.as_mut_ptr(),
            OUT_FIELDS_CAP as c_int,
            &mut errclass,
        )
    };
    marshal(n, errclass, &isn, &off, &flen, &bytes)
}

fn marshal(
    n: c_int,
    errclass: c_int,
    isn: &[c_int],
    off: &[c_int],
    flen: &[c_int],
    bytes: &[u8],
) -> ParseResult {
    if n < 0 {
        return Err(errclass);
    }
    let n = n as usize;
    let mut out = Vec::with_capacity(n);
    for i in 0..n.min(OUT_FIELDS_CAP) {
        if isn[i] != 0 {
            out.push(None);
        } else {
            let o = off[i] as usize;
            let l = flen[i] as usize;
            out.push(Some(bytes[o..o + l].to_vec()));
        }
    }
    Ok(out)
}

/// The verdict of one differential exec.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Cmp {
    /// Same accept/reject, same field image (or same reject class).
    Agree,
    /// Both reject but the errcode CLASS differs (a softer divergence; both
    /// correctly reject the malformed frame). Carries (c_class, rust_class).
    ClassDivergence(i32, i32),
    /// HIGH: accept-vs-reject disagreement, or both-accept image mismatch.
    HighDivergence(String),
}

fn compare(c: &ParseResult, r: &ParseResult) -> Cmp {
    match (c, r) {
        (Ok(cf), Ok(rf)) => {
            if cf == rf {
                Cmp::Agree
            } else {
                Cmp::HighDivergence(format!("field image mismatch: C={cf:?} rust={rf:?}"))
            }
        }
        (Err(cc), Err(rc)) => {
            if cc == rc {
                Cmp::Agree
            } else {
                Cmp::ClassDivergence(*cc, *rc)
            }
        }
        (Ok(cf), Err(rc)) => Cmp::HighDivergence(format!(
            "accept/reject disagreement: C accepted {cf:?}, rust rejected class={rc}"
        )),
        (Err(cc), Ok(rf)) => Cmp::HighDivergence(format!(
            "accept/reject disagreement: C rejected class={cc}, rust accepted {rf:?}"
        )),
    }
}

/// A classified verdict: `Agree`, one of the two DOCUMENTED conformance
/// divergences (findings-vendor-copy.md COPY-1 / COPY-2 — pinned, non-fatal),
/// or `Fatal` (a memory-safety HIGH or any new/unexpected divergence — the
/// campaign gate must be 0 of these).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Agree,
    /// COPY-1: oversize binary field length (>= MaxAllocSize) — C rejects
    /// 54000 (enlargeStringInfo cap), pgrust rejects 22P04 (incremental EOF).
    KnownClassDivergence,
    /// COPY-2: trailing zero-length binary field at exact end-of-input — C
    /// accepts an empty field, pgrust rejects 22P04 (unexpected EOF).
    KnownZeroLenAtEof,
    /// Anything else, including a pgrust panic (`c_panicked`) — the gate.
    Fatal(String),
}

/// Classify a binary-side outcome (the two known divergences are binary-only).
/// `r` is `None` when the shipped parse PANICKED (the Q8 memory-safety class).
fn classify_binary(c: &ParseResult, r: &Option<ParseResult>) -> Verdict {
    let Some(r) = r else {
        return Verdict::Fatal("pgrust PANICKED (memory-safety HIGH / Q8 class)".into());
    };
    match compare(c, r) {
        Cmp::Agree => Verdict::Agree,
        Cmp::ClassDivergence(3, 5) => Verdict::KnownClassDivergence,
        Cmp::HighDivergence(_) => {
            // COPY-2: C accepts with an empty trailing field; pgrust rejects 22P04.
            if let (Ok(cf), Err(5)) = (c, r) {
                if cf.last() == Some(&Some(Vec::new())) {
                    return Verdict::KnownZeroLenAtEof;
                }
            }
            Verdict::Fatal(format!("binary divergence: C={c:?} rust={r:?}"))
        }
        Cmp::ClassDivergence(cc, rc) => {
            Verdict::Fatal(format!("unexpected class divergence C={cc} rust={rc}"))
        }
    }
}

/// Classify a text-side outcome (no known divergences: text must fully agree).
fn classify_text(c: &ParseResult, r: &Option<ParseResult>) -> Verdict {
    let Some(r) = r else {
        return Verdict::Fatal("pgrust PANICKED (memory-safety HIGH / Q8 class)".into());
    };
    match compare(c, r) {
        Cmp::Agree => Verdict::Agree,
        other => Verdict::Fatal(format!("text divergence: {other:?} C={c:?} rust={r:?}")),
    }
}

// ---- Rust side (shipped parse via a fresh memory context per case) ----

fn rust_text(line: &[u8], delim: u8, max_fields: usize) -> ParseResult {
    let cx = mcx::MemoryContext::new("copyframe_text_fuzz");
    bench_internals::parse_text(cx.mcx(), delim, NULL_PRINT, max_fields, line)
}

fn rust_binary(data: &[u8], nattrs: usize) -> ParseResult {
    let cx = mcx::MemoryContext::new("copyframe_binary_fuzz");
    bench_internals::parse_binary(cx.mcx(), data, nattrs)
}

/// Run the shipped parse catching a panic/OOB (the Q8 memory-safety class):
/// `None` == the shipped side panicked.
fn caught<F: FnOnce() -> ParseResult>(f: F) -> Option<ParseResult> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).ok()
}

// ===========================================================================
// Crate-root drivers (registered in edge::edge_drivers). Input layout:
//   [selector: 1 byte][payload...]
// selector chooses delim (text) / nattrs (binary); payload is the line /
// binary stream. A HighDivergence panics (the edge campaign minimizes it);
// a ClassDivergence is intentionally NOT fatal here (tallied by the dedicated
// campaign) so a known softer divergence does not mask the memory-safety bar.
// A pgrust panic/OOB unwinds and is caught by the campaign as a Finding.
// ===========================================================================

const DELIMS: &[u8] = &[b'\t', b',', b'|', b';', b' ', b':'];

/// COPY text field split differential.
pub fn copyframe_text_diff(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let sel = data[0];
    let line = &data[1..];
    let delim = DELIMS[(sel as usize) % DELIMS.len()];
    let max_fields = 1 + ((sel >> 4) as usize % 8);
    let c = c_text(line, delim, max_fields);
    let r = caught(|| rust_text(line, delim, max_fields));
    if let Verdict::Fatal(m) = classify_text(&c, &r) {
        panic!("copyframe_text_diff: {m} (delim={delim:#x} max_fields={max_fields} line={line:?})");
    }
}

/// COPY binary per-field framing differential (the Q8-F1 length-word surface).
pub fn copyframe_binary_diff(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let sel = data[0];
    let stream = &data[1..];
    let nattrs = (sel as usize) % 5; // 0..4 columns
    let c = c_binary(stream, nattrs);
    let r = caught(|| rust_binary(stream, nattrs));
    if let Verdict::Fatal(m) = classify_binary(&c, &r) {
        panic!("copyframe_binary_diff: {m} (nattrs={nattrs} stream={stream:?})");
    }
}

// ===========================================================================
// Dedicated campaign — full comparison (incl. errcode class), witness,
// planted-bug detection control. Mirrors EDGE2's wire campaign discipline:
// a 0-finding sweep that cannot report a finding is worthless.
// ===========================================================================

/// One driver's execution witness (EDGE2 vacuity accounting).
#[derive(Debug, Clone)]
pub struct DriverStat {
    pub driver: &'static str,
    pub cases: usize,
    pub elapsed_ns: u128,
    /// COPY-1: oversize-length class divergence (pinned, non-fatal).
    pub known_class: usize,
    /// COPY-2: trailing zero-length field at EOF (pinned, non-fatal).
    pub known_zerolen: usize,
    /// FATAL: pgrust panic/OOB or any new/unexpected divergence — gate is 0.
    pub fatal: usize,
}

impl DriverStat {
    pub fn ns_per_case(&self) -> f64 {
        if self.cases == 0 {
            0.0
        } else {
            self.elapsed_ns as f64 / self.cases as f64
        }
    }
}

/// A captured HIGH divergence.
#[derive(Debug, Clone)]
pub struct CopyFinding {
    pub driver: &'static str,
    pub payload: Vec<u8>,
    pub message: String,
}

/// i32 length word big-endian bytes.
fn be_i32(v: i32) -> [u8; 4] {
    v.to_be_bytes()
}

/// Build the binary framing bank: for every `COPY_FIELD_LEN` value and a set
/// of trailing payloads / field counts, a `[i16 count][i32 len][payload]`
/// frame — the exact `-1` NULL sentinel vs illegal-negative / oversize shape.
fn binary_frames() -> Vec<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let tails: &[&[u8]] = &[&[], b"X", b"XY", b"\x00", b"payloadbytes", &[0u8; 300]];
    for &len in crate::edge::COPY_FIELD_LEN {
        // single field, nattrs=1: [00 01][len][tail]
        for tail in tails {
            let mut f = vec![0u8, 1];
            f.extend_from_slice(&be_i32(len));
            f.extend_from_slice(tail);
            out.push((f, 1));
        }
        // two fields, nattrs=2: a valid first field then the edge length
        for tail in tails {
            let mut f = vec![0u8, 2];
            f.extend_from_slice(&be_i32(1));
            f.push(b'A');
            f.extend_from_slice(&be_i32(len));
            f.extend_from_slice(tail);
            out.push((f, 2));
        }
    }
    // Field-count header edge cases (i16 count word) + truncations.
    let counts: &[i16] = &[-1, 0, 1, 2, 3, 100, i16::MIN, i16::MAX];
    for &c in counts {
        for nattrs in 0..4usize {
            let mut f = c.to_be_bytes().to_vec();
            f.extend_from_slice(&be_i32(1));
            f.push(b'Z');
            out.push((f, nattrs));
        }
    }
    // Truncated headers (empty / 1..7 bytes) — a recv reading a fixed prefix
    // underflows (the empty-slice class).
    for k in 0..8usize {
        out.push((vec![0x7fu8; k], 1));
    }
    out
}

/// Build the text line bank: delimiter / escape / null-marker / octal-hex /
/// backslash-at-EOL / embedded-NUL edge lines.
fn text_lines() -> Vec<Vec<u8>> {
    let mut v: Vec<Vec<u8>> = vec![
        b"".to_vec(),
        b"a\tb\tc".to_vec(),
        b"\\N".to_vec(),
        b"\\\\N".to_vec(),
        b"a\\tb\t\\N\t\\\\x".to_vec(),
        b"\\101\\x41".to_vec(),
        b"trailing\\".to_vec(),        // backslash at EOL
        b"\\".to_vec(),                // lone backslash
        b"\\x".to_vec(),               // \x with no hex
        b"\\0".to_vec(),               // NUL via octal (embedded NUL de-escape)
        b"\\000\\001\\377".to_vec(),   // octal incl NUL + high bit
        b"\\xff\\x00".to_vec(),        // hex incl high bit + NUL
        b"a\t".to_vec(),               // trailing empty field
        b"\ta".to_vec(),               // leading empty field
        b"\t\t\t".to_vec(),            // all-empty fields
        b"\\b\\f\\n\\r\\t\\v".to_vec(), // named escapes
        b"\\9\\8".to_vec(),            // non-octal after backslash (literal)
        b"no delimiters here".to_vec(),
        vec![0x80, 0x81, 0xff],        // raw high-bit bytes (no escape)
    ];
    // some random-ish structured lines with delimiters and backslashes
    for seed in 0u8..32 {
        let mut l = Vec::new();
        for k in 0..12u8 {
            let b = seed.wrapping_mul(31).wrapping_add(k.wrapping_mul(7));
            l.push(match b % 6 {
                0 => b'\t',
                1 => b'\\',
                2 => b'N',
                3 => b'x',
                4 => (b % 8) + b'0',
                _ => b,
            });
        }
        v.push(l);
    }
    v
}

/// Full report of a campaign run.
pub struct CampaignReport {
    pub stats: Vec<DriverStat>,
    /// FATAL findings only (pgrust panic/OOB or any new/unexpected divergence).
    /// The two DOCUMENTED conformance divergences are counted in `DriverStat`,
    /// not here — the gate is `fatal.is_empty()`.
    pub fatal: Vec<CopyFinding>,
}

/// Run the full COPY-framing campaign across `reps` rounds. Every case is
/// classified; the shipped side runs under `catch_unwind` so a panic/OOB is a
/// FATAL finding rather than a crash.
pub fn run_copyframe_campaign(reps: usize) -> CampaignReport {
    let mut stats = Vec::new();
    let mut fatal = Vec::new();

    // --- binary driver (the Q8-F1 length-word surface) ---
    let frames = binary_frames();
    let mut cases = 0usize;
    let (mut kc, mut kz) = (0usize, 0usize);
    let t0 = std::time::Instant::now();
    for _ in 0..reps {
        for (frame, nattrs) in &frames {
            cases += 1;
            let c = c_binary(frame, *nattrs);
            let r = caught(|| rust_binary(frame, *nattrs));
            match classify_binary(&c, &r) {
                Verdict::Agree => {}
                Verdict::KnownClassDivergence => kc += 1,
                Verdict::KnownZeroLenAtEof => kz += 1,
                Verdict::Fatal(m) => {
                    if fatal.len() < 64 {
                        fatal.push(CopyFinding {
                            driver: "copyframe_binary_diff",
                            payload: frame.clone(),
                            message: m,
                        });
                    }
                }
            }
        }
    }
    stats.push(DriverStat {
        driver: "copyframe_binary_diff",
        cases,
        elapsed_ns: t0.elapsed().as_nanos(),
        known_class: kc,
        known_zerolen: kz,
        fatal: fatal.len(),
    });

    // --- text driver (de-escape / delimiter / null-marker / embedded NUL) ---
    let lines = text_lines();
    let mut cases = 0usize;
    let fatal_before = fatal.len();
    let t0 = std::time::Instant::now();
    for _ in 0..reps {
        for line in &lines {
            for &delim in DELIMS {
                for max_fields in 1..=6usize {
                    cases += 1;
                    let c = c_text(line, delim, max_fields);
                    let r = caught(|| rust_text(line, delim, max_fields));
                    if let Verdict::Fatal(m) = classify_text(&c, &r) {
                        if fatal.len() < 64 {
                            fatal.push(CopyFinding {
                                driver: "copyframe_text_diff",
                                payload: line.clone(),
                                message: m,
                            });
                        }
                    }
                }
            }
        }
    }
    stats.push(DriverStat {
        driver: "copyframe_text_diff",
        cases,
        elapsed_ns: t0.elapsed().as_nanos(),
        known_class: 0,
        known_zerolen: 0,
        fatal: fatal.len() - fatal_before,
    });

    CampaignReport { stats, fatal }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke: known-good text and binary frames agree on the value plane.
    #[test]
    fn copyframe_known_good_agree() {
        let _g = crate::c_oracle_serial();
        assert_eq!(rust_text(b"a\tb\t\\N", b'\t', 8), c_text(b"a\tb\t\\N", b'\t', 8));
        assert!(matches!(rust_text(b"a\tb\t\\N", b'\t', 8), Ok(ref f) if f.len() == 3 && f[2].is_none()));
        // binary: [count=2][len=1 'X'][len=-1 NULL]
        let frame = vec![0u8, 2, 0, 0, 0, 1, b'X', 0xff, 0xff, 0xff, 0xff];
        assert_eq!(rust_binary(&frame, 2), c_binary(&frame, 2));
        assert!(matches!(rust_binary(&frame, 2), Ok(ref f) if f.len()==2 && f[0]==Some(b"X".to_vec()) && f[1].is_none()));
    }

    /// The COPY_FIELD_LEN legal NULL sentinel (-1) vs illegal negatives:
    /// -1 => NULL, every other negative => reject. With a trailing payload
    /// byte (so no zero-length-at-EOF), the accept/reject must agree.
    #[test]
    fn copyframe_length_sentinel_matches() {
        let _g = crate::c_oracle_serial();
        for &len in crate::edge::COPY_FIELD_LEN {
            let mut frame = vec![0u8, 1];
            frame.extend_from_slice(&len.to_be_bytes());
            frame.push(b'q'); // trailing byte
            let c = c_binary(&frame, 1);
            let r = caught(|| rust_binary(&frame, 1));
            match classify_binary(&c, &r) {
                Verdict::Fatal(m) => panic!("length sentinel FATAL at len={len}: {m}"),
                _ => {}
            }
        }
    }

    /// DETECTION CONTROL (EDGE2/PARSER law): a deliberately-wrong "shipped"
    /// parse MUST be caught as a divergence. Proves the comparator has
    /// detection power; a 0-fatal campaign is only meaningful because this
    /// fires. Three plants: dropped field, wrong bytes, a simulated panic.
    #[test]
    fn copyframe_detects_planted_bug() {
        let _g = crate::c_oracle_serial();
        let frame = vec![0u8, 2, 0, 0, 0, 1, b'A', 0, 0, 0, 1, b'B'];
        let good = c_binary(&frame, 2);
        assert!(matches!(good, Ok(ref f) if f.len() == 2), "oracle should accept: {good:?}");

        // (1) product drops the last field.
        let dropped: Option<ParseResult> = Some(match &good {
            Ok(f) => Ok(f[..f.len().saturating_sub(1)].to_vec()),
            Err(e) => Err(*e),
        });
        assert!(
            matches!(classify_binary(&good, &dropped), Verdict::Fatal(_)),
            "planted dropped-field not caught"
        );

        // (2) product returns wrong field bytes.
        let wrong: Option<ParseResult> =
            Some(Ok(vec![Some(b"Z".to_vec()), Some(b"B".to_vec())]));
        assert!(
            matches!(classify_binary(&good, &wrong), Verdict::Fatal(_)),
            "planted wrong-bytes not caught"
        );

        // (3) product PANICKED (the Q8 memory-safety class): r == None.
        assert!(
            matches!(classify_binary(&good, &None), Verdict::Fatal(m) if m.contains("PANIC")),
            "planted panic not caught"
        );
    }

    /// Witness + sweep at reps=1. Asserts 0 FATAL and prints the per-driver
    /// witness (a vacuous 0 ns/case driver is visible). The two documented
    /// conformance divergences (COPY-1/COPY-2) are expected and non-fatal.
    #[test]
    fn copyframe_campaign_smoke() {
        let _g = crate::c_oracle_serial();
        let rep = run_copyframe_campaign(1);
        eprintln!("--- copyframe campaign (smoke, reps=1) ---");
        for s in &rep.stats {
            eprintln!(
                "  {:22} cases={:6} ns/case={:8.1} fatal={} known_class(COPY-1)={} known_zerolen(COPY-2)={}",
                s.driver, s.cases, s.ns_per_case(), s.fatal, s.known_class, s.known_zerolen
            );
            assert!(s.cases > 0, "driver {} ran no cases", s.driver);
            assert!(s.ns_per_case() > 1.0, "driver {} vacuous (ns/case ~ 0)", s.driver);
        }
        assert!(rep.fatal.is_empty(), "FATAL divergence(s): {:#?}", rep.fatal);
        // The two documented divergences must still be observed (regression
        // witness: if a fix lands, update findings-vendor-copy.md + this pin).
        let bin = &rep.stats[0];
        assert!(bin.known_class > 0, "COPY-1 (oversize-length class) no longer observed");
        assert!(bin.known_zerolen > 0, "COPY-2 (zero-length-at-EOF) no longer observed");
    }

    /// Heavy campaign: tens of thousands of cases. Run with
    /// `cargo test -p decoder_fuzz --lib copyframe_campaign_full -- --ignored --nocapture`.
    #[test]
    #[ignore = "heavy sweep; run explicitly"]
    fn copyframe_campaign_full() {
        let _g = crate::c_oracle_serial();
        let rep = run_copyframe_campaign(400);
        let total: usize = rep.stats.iter().map(|s| s.cases).sum();
        eprintln!("=== copyframe campaign (full) — {total} cases ===");
        for s in &rep.stats {
            eprintln!(
                "  {:22} cases={:7} ns/case={:8.1} fatal={} known_class(COPY-1)={} known_zerolen(COPY-2)={}",
                s.driver, s.cases, s.ns_per_case(), s.fatal, s.known_class, s.known_zerolen
            );
        }
        for f in &rep.fatal {
            eprintln!("FATAL {} :: {} :: payload={:?}", f.driver, f.message, f.payload);
        }
        assert!(rep.fatal.is_empty(), "{} FATAL divergence(s)", rep.fatal.len());
    }
}
