//! copyrow_diff — SHIPPED Rust COPY line/row framing (copy_cmd::fromparse
//! `copy_read_line` / `copy_read_line_text`, reached through `bench_internals`)
//! vs VERBATIM vendored PostgreSQL 18.3 C (csrc/pg_copyframe_io.c
//! cpf_copy_read_lines -> CopyReadLine + CopyReadLineText @ upstream sha
//! 62d6c7d3df). The raw-line reader that splits COPY input into ROWS before
//! field parsing — the explicit VENDOR-COPY follow-on (#937 left the row/line
//! layer undone; #943 fixed the field-layer COPY-1/COPY-2, which are NOT
//! re-banked here).
//!
//! WHY THIS TARGET: `CopyReadLineText` is a hand-rolled state machine over an
//! attacker-controlled stream — quote/escape state (CSV), embedded CR/LF/CRLF
//! with first-line EOL-style latching, the `\.` end-of-copy marker (alone vs
//! mid-line, matching the latched newline style), non-CSV backslash escaping
//! (so `\\.` is data, not a marker), a trailing unterminated line, and the
//! SQL_ASCII embedded-NUL rejection at the encoding layer. Every one of those
//! is a branch where a port can drift by one byte or one lookahead.
//!
//! THE BAR: pgrust must split rows and set the end-of-copy/line verdict
//! IDENTICALLY to the verbatim C — the same bytes per emitted line, the same
//! EOF/marker verdict, the same errcode CLASS on a malformed stream. A pgrust
//! PANIC / OOB / assert where C cleanly handles = HIGH (Q8 class); an
//! accept/reject or line-split divergence = MED (COPY-2 class). The comparator
//! has three planes: (1) accept-vs-reject, (2) the full line-image sequence +
//! the saw-EOF verdict when both accept, (3) errcode CLASS when both reject.
//!
//! Encoding is SQL_ASCII (default server encoding, no transcoding) on BOTH
//! sides: every non-NUL byte (incl. high-bit) is a valid 1-byte char, and an
//! embedded NUL is rejected with an encoding error (class 7) at line-load time
//! — the C shim's CopyLoadInputBuf mirrors the shipped no-transcoding path
//! (crates/common/wchar pg_ascii_verifystr), so the embedded-NUL case is a live
//! differential rather than a plumbing artifact (see the C file header).

use std::ffi::{c_char, c_int};

use copy_cmd::bench_internals;

extern "C" {
    fn cpf_copy_read_lines(
        data: *const c_char,
        len: c_int,
        is_csv: c_int,
        delim: c_char,
        quote: c_char,
        escape: c_char,
        out_bytes: *mut u8,
        out_bytes_cap: c_int,
        out_off: *mut c_int,
        out_len: *mut c_int,
        out_lines_cap: c_int,
        out_errclass: *mut c_int,
        out_saw_eof: *mut c_int,
    ) -> c_int;
}

const OUT_BYTES_CAP: usize = 1 << 20; // > any bounded stream (CPF_PHYS_CAP)
const OUT_LINES_CAP: usize = 4096;

/// A line/row-framing result on either side: reject with an errcode class, or
/// the emitted line sequence (each an EOL-stripped line image) plus the
/// saw-EOF verdict of the terminating read.
type RowResult = Result<(Vec<Vec<u8>>, bool), i32>;

/// Call the C line oracle and marshal its flat output into a [`RowResult`].
fn c_lines(data: &[u8], is_csv: bool, delim: u8, quote: u8, escape: u8) -> RowResult {
    let mut off = vec![0 as c_int; OUT_LINES_CAP];
    let mut llen = vec![0 as c_int; OUT_LINES_CAP];
    let mut bytes = vec![0u8; OUT_BYTES_CAP];
    let mut errclass: c_int = 0;
    let mut saw_eof: c_int = 0;
    let n = unsafe {
        cpf_copy_read_lines(
            data.as_ptr() as *const c_char,
            data.len() as c_int,
            is_csv as c_int,
            delim as c_char,
            quote as c_char,
            escape as c_char,
            bytes.as_mut_ptr(),
            OUT_BYTES_CAP as c_int,
            off.as_mut_ptr(),
            llen.as_mut_ptr(),
            OUT_LINES_CAP as c_int,
            &mut errclass,
            &mut saw_eof,
        )
    };
    if n < 0 {
        return Err(errclass);
    }
    let n = n as usize;
    let mut lines = Vec::with_capacity(n);
    for i in 0..n.min(OUT_LINES_CAP) {
        let o = off[i] as usize;
        let l = llen[i] as usize;
        lines.push(bytes[o..o + l].to_vec());
    }
    Ok((lines, saw_eof != 0))
}

// ---- Rust side (shipped line reader via a fresh memory context per case) ----

fn rust_lines(data: &[u8], is_csv: bool, delim: u8, quote: u8, escape: u8) -> RowResult {
    let cx = mcx::MemoryContext::new("copyrow_fuzz");
    bench_internals::parse_lines(cx.mcx(), data, is_csv, delim, quote, escape)
}

/// Run the shipped line reader catching a panic/OOB (the Q8 memory-safety
/// class): `None` == the shipped side panicked.
fn caught<F: FnOnce() -> RowResult>(f: F) -> Option<RowResult> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).ok()
}

/// The verdict of one differential exec.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Cmp {
    /// Same accept/reject, same line-image sequence + saw-EOF (or same class).
    Agree,
    /// Both reject but the errcode CLASS differs (softer). (c_class, r_class).
    ClassDivergence(i32, i32),
    /// HIGH/MED: accept-vs-reject, or both-accept line-image / saw-EOF mismatch.
    Divergence(String),
}

fn compare(c: &RowResult, r: &RowResult) -> Cmp {
    match (c, r) {
        (Ok(cl), Ok(rl)) => {
            if cl == rl {
                Cmp::Agree
            } else {
                Cmp::Divergence(format!("line-image/EOF mismatch: C={cl:?} rust={rl:?}"))
            }
        }
        (Err(cc), Err(rc)) => {
            if cc == rc {
                Cmp::Agree
            } else {
                Cmp::ClassDivergence(*cc, *rc)
            }
        }
        (Ok(cl), Err(rc)) => Cmp::Divergence(format!(
            "accept/reject disagreement: C accepted {cl:?}, rust rejected class={rc}"
        )),
        (Err(cc), Ok(rl)) => Cmp::Divergence(format!(
            "accept/reject disagreement: C rejected class={cc}, rust accepted {rl:?}"
        )),
    }
}

/// A classified verdict. The line/row layer has NO documented conformance
/// divergence (COPY-1/COPY-2 are field-layer, already fixed in #943 and NOT
/// exercised here), so anything but `Agree` — a pgrust panic, an accept/reject
/// split, a line-image mismatch, or a both-reject class divergence — is
/// `Fatal`: the campaign gate is 0 of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Agree,
    Fatal(String),
}

fn classify(c: &RowResult, r: &Option<RowResult>) -> Verdict {
    let Some(r) = r else {
        return Verdict::Fatal("pgrust PANICKED (memory-safety HIGH / Q8 class)".into());
    };
    match compare(c, r) {
        Cmp::Agree => Verdict::Agree,
        Cmp::ClassDivergence(cc, rc) => {
            Verdict::Fatal(format!("both reject, class divergence C={cc} rust={rc} (MED, COPY-2 class)"))
        }
        Cmp::Divergence(m) => Verdict::Fatal(format!("row-framing divergence: {m}")),
    }
}

// ===========================================================================
// Crate-root driver (registered in edge::edge_drivers). Input layout:
//   [selector: 1 byte][payload...]
// selector chooses is_csv + delim/quote/escape; payload is the raw stream. A
// FATAL divergence panics (the edge campaign minimizes it); a pgrust panic/OOB
// unwinds and is caught by the campaign as a Finding.
// ===========================================================================

const DELIMS: &[u8] = &[b'\t', b',', b'|', b';'];
const QUOTES: &[u8] = &[b'"', b'\''];

/// COPY line/row framing differential.
pub fn copyrow_diff(data: &[u8]) {
    // Hold the process-wide oracle lock on the libFuzzer entry frame: the C
    // helpers below reach the holder-checked vendored-C oracle
    // (csrc/pg_copyframe_io.c). Without this the runtime holder check aborts
    // (the trgmrx arm-9 vacuous-crash class); it also satisfies
    // scripts/lint-oracle-serial.py for this fuzz_target! entry. Reentrant
    // (thread-local depth), so run_copyrow_campaign taking it too is a no-op.
    let _serial = crate::c_oracle_serial();
    if data.is_empty() {
        return;
    }
    let sel = data[0];
    let stream = &data[1..];
    let is_csv = sel & 1 == 1;
    let delim = DELIMS[(sel as usize >> 1) % DELIMS.len()];
    let quote = QUOTES[(sel as usize >> 3) % QUOTES.len()];
    // Exercise both the escape==quote (toggle) and escape!=quote paths.
    let escape = if sel & 0x40 != 0 { b'\\' } else { quote };
    let c = c_lines(stream, is_csv, delim, quote, escape);
    let r = caught(|| rust_lines(stream, is_csv, delim, quote, escape));
    if let Verdict::Fatal(m) = classify(&c, &r) {
        panic!(
            "copyrow_diff: {m} (is_csv={is_csv} delim={delim:#x} quote={quote:#x} \
             escape={escape:#x} stream={stream:?})"
        );
    }
}

// ===========================================================================
// Dedicated campaign — full comparison (line sequence + saw-EOF + errcode
// class), execution witness, planted-bug detection control. Mirrors the
// copyframe / EDGE2 discipline: a 0-finding sweep that cannot report a finding
// is worthless.
// ===========================================================================

/// One driver's execution witness (vacuity accounting).
#[derive(Debug, Clone)]
pub struct DriverStat {
    pub driver: &'static str,
    pub cases: usize,
    pub elapsed_ns: u128,
    /// FATAL: pgrust panic/OOB or any divergence — the gate is 0.
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

/// A captured divergence.
#[derive(Debug, Clone)]
pub struct RowFinding {
    pub payload: Vec<u8>,
    pub is_csv: bool,
    pub delim: u8,
    pub quote: u8,
    pub escape: u8,
    pub message: String,
}

/// Build the line/row stream bank: every framing edge the target spec names —
/// embedded quotes/escapes (CSV), bare CR / bare LF / CRLF mixes, an
/// unterminated final line, embedded NUL, `\.` marker alone vs mid-line vs at
/// each newline style, a backslash at EOF, a huge line (buffer-growth path),
/// empty lines, and a line exactly at the 64 KiB input-buffer boundary.
fn row_streams() -> Vec<Vec<u8>> {
    let mut v: Vec<Vec<u8>> = vec![
        // --- empty / trivial ---
        b"".to_vec(),
        b"\n".to_vec(),                       // one empty line
        b"\n\n\n".to_vec(),                   // several empty lines
        b"a".to_vec(),                        // unterminated single line (no EOL at EOF)
        b"a\n".to_vec(),                      // terminated single line
        b"a\nb\nc".to_vec(),                  // unterminated final line after two
        b"a\nb\nc\n".to_vec(),                // all terminated
        // --- CR / LF / CRLF mixes (first-line EOL-style latch) ---
        b"a\r".to_vec(),                      // bare CR at EOF (latches EOL_CR)
        b"a\rb\rc\r".to_vec(),                // consistent bare CR lines
        b"a\r\nb\r\n".to_vec(),               // consistent CRLF
        b"a\r\nb\n".to_vec(),                 // CRLF then bare LF -> literal-nl error
        b"a\nb\r\n".to_vec(),                 // LF then CRLF -> literal-cr error
        b"a\nb\rc".to_vec(),                  // LF latched then bare CR -> error
        b"a\rb\n".to_vec(),                   // CR latched then bare LF -> error
        b"\r".to_vec(),                       // lone CR at EOF
        b"\r\n".to_vec(),                     // lone CRLF
        b"a\r\n\r\nb\r\n".to_vec(),           // empty CRLF lines
        // --- end-of-copy marker \. ---
        b"\\.\n".to_vec(),                    // marker alone (LF)
        b"\\.\r\n".to_vec(),                  // marker alone (CRLF)
        b"a\n\\.\n".to_vec(),                 // data line then marker
        b"a\n\\.\nb\n".to_vec(),              // marker then trailing (ignored for File src)
        b"x\\.\n".to_vec(),                   // marker not alone (data before) -> error
        b"\\.x\n".to_vec(),                   // marker not alone (data after) -> error
        b"\\.".to_vec(),                      // backslash-dot at EOF, no newline
        b"a\r\n\\.\r\n".to_vec(),             // marker after CRLF data (style match)
        b"a\n\\.\r\n".to_vec(),               // marker style mismatch (LF vs CRLF)
        // --- non-CSV backslash escaping ---
        b"\\\\.\n".to_vec(),                  // backslash-backslash-dot: data, not marker
        b"a\\\nb\n".to_vec(),                 // backslash before newline (escapes it)
        b"a\\".to_vec(),                      // backslash at EOF (treated as data)
        b"\\".to_vec(),                       // lone backslash at EOF
        b"a\\tb\tc\n".to_vec(),              // backslash-escaped char mid-line
        // --- embedded NUL (SQL_ASCII rejects at line load) ---
        b"a\x00b\n".to_vec(),
        vec![0x00],
        b"a\nb\x00\n".to_vec(),
        // --- high-bit bytes (SQL_ASCII: valid single bytes) ---
        vec![0x80, 0x81, 0xff, b'\n'],
        vec![0xff, 0xfe, 0x0a, 0xff],
    ];

    // --- CSV quote/escape state ---
    v.push(b"\"a\nb\"\nc\n".to_vec());        // newline inside a quoted field
    v.push(b"\"a\"\"b\"\n".to_vec());         // doubled quote (escape==quote toggle)
    v.push(b"\"a\rb\"\r\n".to_vec());         // CR inside quoted field (CRLF outer)
    v.push(b"\"unterminated".to_vec());       // quote opened, never closed, EOF
    v.push(b"\"a\\\"b\"\n".to_vec());         // backslash-escaped quote (escape=\\ case)
    v.push(b"\"\n\"\n".to_vec());             // quoted lone newline
    v.push(b"a,\"b\nc\",d\n".to_vec());       // quoted field with embedded newline among fields

    // --- huge line (buffer-growth path) + boundary cases ---
    v.push({
        let mut s = vec![b'x'; 200_000]; // > CPF_PHYS_CAP? no — bounded well under 1 MiB
        s.push(b'\n');
        s
    });
    // Line exactly at the 64 KiB INPUT_BUF_SIZE boundary (± a few bytes), so
    // the reader's refill lands right at a line terminator / lookahead.
    for delta in [-2i64, -1, 0, 1, 2] {
        let n = (65536i64 + delta).max(0) as usize;
        let mut s = vec![b'a'; n];
        s.push(b'\n');
        s.push(b'b');
        s.push(b'\n');
        v.push(s);
    }
    // A backslash / CR landing exactly on the boundary (lookahead across refill).
    for tail in [b"\\\n".as_slice(), b"\r\n".as_slice(), b"\\.".as_slice()] {
        let mut s = vec![b'a'; 65535];
        s.extend_from_slice(tail);
        v.push(s);
    }

    // Structured pseudo-random streams mixing terminators, backslashes, quotes.
    for seed in 0u16..64 {
        let mut l = Vec::new();
        for k in 0..40u16 {
            let b = seed.wrapping_mul(131).wrapping_add(k.wrapping_mul(17)) as u8;
            l.push(match b % 8 {
                0 => b'\n',
                1 => b'\r',
                2 => b'\\',
                3 => b'.',
                4 => b'"',
                5 => b',',
                6 => b'a',
                _ => b,
            });
        }
        v.push(l);
    }
    v
}

/// Full report of a campaign run.
pub struct CampaignReport {
    pub stat: DriverStat,
    /// FATAL findings (pgrust panic/OOB or any divergence). Gate = empty.
    pub fatal: Vec<RowFinding>,
}

/// Run the full COPY line/row-framing campaign across `reps` rounds. Every
/// stream is run in text mode and CSV mode over several delim/quote/escape
/// configs; the shipped side runs under `catch_unwind` so a panic/OOB is a
/// FATAL finding rather than a crash.
pub fn run_copyrow_campaign(reps: usize) -> CampaignReport {
    let streams = row_streams();
    let mut fatal = Vec::new();
    let mut cases = 0usize;
    let t0 = std::time::Instant::now();

    // (is_csv, delim, quote, escape) configuration matrix.
    let mut configs: Vec<(bool, u8, u8, u8)> = Vec::new();
    for &delim in DELIMS {
        // text mode: quote/escape irrelevant but passed through.
        configs.push((false, delim, b'"', b'"'));
    }
    for &delim in DELIMS {
        for &quote in QUOTES {
            configs.push((true, delim, quote, quote)); // escape == quote (toggle)
            configs.push((true, delim, quote, b'\\')); // escape != quote
        }
    }

    for _ in 0..reps {
        for stream in &streams {
            for &(is_csv, delim, quote, escape) in &configs {
                cases += 1;
                let c = c_lines(stream, is_csv, delim, quote, escape);
                let r = caught(|| rust_lines(stream, is_csv, delim, quote, escape));
                if let Verdict::Fatal(m) = classify(&c, &r) {
                    if fatal.len() < 64 {
                        fatal.push(RowFinding {
                            payload: stream.clone(),
                            is_csv,
                            delim,
                            quote,
                            escape,
                            message: m,
                        });
                    }
                }
            }
        }
    }

    let stat = DriverStat {
        driver: "copyrow_diff",
        cases,
        elapsed_ns: t0.elapsed().as_nanos(),
        fatal: fatal.len(),
    };
    CampaignReport { stat, fatal }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke: known-good text and CSV streams split identically on both sides.
    #[test]
    fn copyrow_known_good_agree() {
        let _g = crate::c_oracle_serial();
        // three LF-terminated lines
        let c = c_lines(b"a\nb\nc\n", false, b'\t', b'"', b'"');
        let r = rust_lines(b"a\nb\nc\n", false, b'\t', b'"', b'"');
        assert_eq!(c, r, "text 3-line split");
        assert!(matches!(&c, Ok((l, true)) if l.len() == 3 && l[0] == b"a"));

        // unterminated final line (no newline at EOF)
        let c = c_lines(b"a\nb", false, b'\t', b'"', b'"');
        let r = rust_lines(b"a\nb", false, b'\t', b'"', b'"');
        assert_eq!(c, r, "unterminated tail");
        assert!(matches!(&c, Ok((l, true)) if l.len() == 2 && l[1] == b"b"));

        // CSV newline inside a quoted field: one logical line
        let c = c_lines(b"\"a\nb\"\nc\n", true, b',', b'"', b'"');
        let r = rust_lines(b"\"a\nb\"\nc\n", true, b',', b'"', b'"');
        assert_eq!(c, r, "csv quoted newline");
    }

    /// Embedded NUL: SQL_ASCII rejects at line load (class 7) on BOTH sides.
    #[test]
    fn copyrow_embedded_nul_rejects_both() {
        let _g = crate::c_oracle_serial();
        let c = c_lines(b"a\x00b\n", false, b'\t', b'"', b'"');
        let r = rust_lines(b"a\x00b\n", false, b'\t', b'"', b'"');
        assert_eq!(c, r, "embedded NUL verdict must match");
        assert!(matches!(c, Err(_)), "embedded NUL should reject: {c:?}");
    }

    /// DETECTION CONTROL (EDGE2/PARSER law): a deliberately-wrong "shipped"
    /// result MUST be caught. Proves the comparator has detection power; a
    /// 0-fatal campaign is only meaningful because these fire. Four plants:
    /// dropped line, wrong bytes, wrong saw-EOF, a simulated panic.
    #[test]
    fn copyrow_detects_planted_bug() {
        let _g = crate::c_oracle_serial();
        let good = c_lines(b"a\nb\nc\n", false, b'\t', b'"', b'"');
        assert!(matches!(good, Ok((ref l, _)) if l.len() == 3), "oracle: {good:?}");

        // (1) product drops the last line.
        let dropped: Option<RowResult> = Some(match &good {
            Ok((l, e)) => Ok((l[..l.len() - 1].to_vec(), *e)),
            Err(e) => Err(*e),
        });
        assert!(matches!(classify(&good, &dropped), Verdict::Fatal(_)), "dropped-line not caught");

        // (2) product returns wrong line bytes.
        let wrong: Option<RowResult> =
            Some(Ok((vec![b"a".to_vec(), b"ZZ".to_vec(), b"c".to_vec()], true)));
        assert!(matches!(classify(&good, &wrong), Verdict::Fatal(_)), "wrong-bytes not caught");

        // (3) product flips the saw-EOF verdict.
        let eofflip: Option<RowResult> = Some(match &good {
            Ok((l, e)) => Ok((l.clone(), !e)),
            Err(e) => Err(*e),
        });
        assert!(matches!(classify(&good, &eofflip), Verdict::Fatal(_)), "saw-EOF flip not caught");

        // (4) product PANICKED (the Q8 memory-safety class): r == None.
        assert!(
            matches!(classify(&good, &None), Verdict::Fatal(m) if m.contains("PANIC")),
            "planted panic not caught"
        );
    }

    /// Witness + sweep at reps=1. Asserts 0 FATAL and prints the witness (a
    /// vacuous 0 ns/case driver is visible).
    #[test]
    fn copyrow_campaign_smoke() {
        let _g = crate::c_oracle_serial();
        let rep = run_copyrow_campaign(1);
        let s = &rep.stat;
        eprintln!("--- copyrow campaign (smoke, reps=1) ---");
        eprintln!(
            "  {:14} cases={:6} ns/case={:8.1} fatal={}",
            s.driver, s.cases, s.ns_per_case(), s.fatal
        );
        assert!(s.cases > 0, "driver ran no cases");
        assert!(s.ns_per_case() > 1.0, "driver vacuous (ns/case ~ 0)");
        assert!(rep.fatal.is_empty(), "FATAL divergence(s): {:#?}", rep.fatal);
    }

    /// Heavy campaign: tens of thousands of cases. Run with
    /// `cargo test -p decoder_fuzz --lib copyrow_campaign_full -- --ignored --nocapture`.
    #[test]
    #[ignore = "heavy sweep; run explicitly"]
    fn copyrow_campaign_full() {
        let _g = crate::c_oracle_serial();
        let rep = run_copyrow_campaign(300);
        let s = &rep.stat;
        eprintln!("=== copyrow campaign (full) — {} cases ===", s.cases);
        eprintln!("  {:14} cases={:7} ns/case={:8.1} fatal={}", s.driver, s.cases, s.ns_per_case(), s.fatal);
        for f in &rep.fatal {
            eprintln!(
                "FATAL :: {} :: is_csv={} delim={:#x} quote={:#x} escape={:#x} payload={:?}",
                f.message, f.is_csv, f.delim, f.quote, f.escape, f.payload
            );
        }
        assert!(rep.fatal.is_empty(), "{} FATAL divergence(s)", rep.fatal.len());
    }
}
