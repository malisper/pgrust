//! The regrown read-path fuzzer — the standing CI gate (§5 M3-K; the
//! #66/#340 incident class). Corpus: REAL parts written by D's writer with
//! C's codecs (every fixture arm of `corpus::standard_corpus`). Every
//! structure-aware mutation must yield a TYPED refusal or a correct read —
//! NEVER a panic/UB/wrong silent result:
//!
//! - raw tier (checksums not repaired): typed refusal, or the byte-identical
//!   pristine answer (the flip landed in covered-by-nothing padding);
//! - semantic tier (checksum chain repaired — hostile frame tables /
//!   directory entries arrive checksummed): typed refusal or an Ok answer
//!   that passes the arena-containment invariant and the
//!   `decode_sel ≡ decode_full ∘ select` cross-check;
//! - manifest/CURRENT mutations: `resolve_effective` refuses typed or
//!   answers correctly, never panics.
//!
//! Born-RED, two teeth: (1) a seeded CRC flip MUST produce the typed CRC
//! refusal — the detector detects; (2) the iteration witness — the run
//! count must equal the chartered grid, so a not-run battery cannot pass.
//! Findings are COLLECTED (fuzzing continues past a failure) and reported
//! together with their (fixture, part, seed) minimized-repro coordinates.

mod common;

use pgrc2_qa::adapters::{full_binding, memdir_of, Probe};
use pgrc2_qa::corpus::{
    build_standard_corpus, decode_part, decode_sel_granule, open_part_bytes, verify_manifest,
    OracleCol, QaIssue,
};
use pgrc2_qa::mutate::{mutate, MutationDesc};
use pgrc2_read::manifest_walk::{resolve_effective, TableExpect};
use pgrc2_write::publish::TxnVerdict;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[derive(Debug)]
#[allow(dead_code)] // fields are read via Debug in the findings report
enum Finding {
    Panic { fx: &'static str, part: String, desc: MutationDesc },
    OutOfArena { fx: &'static str, part: String, desc: MutationDesc },
    WrongSilentResult { fx: &'static str, part: String, desc: MutationDesc, what: String },
    SelDiverged { fx: &'static str, part: String, desc: MutationDesc },
}

fn part_names(files: &std::collections::BTreeMap<String, Vec<u8>>) -> Vec<String> {
    files
        .keys()
        .filter(|n| pgrc2_format::dirlayout::parse_part_file_name(n).is_some())
        .cloned()
        .collect()
}

#[test]
fn read_fuzzer() {
    let s = common::scale();
    let corpus = build_standard_corpus();

    // Pristine leg: the corpus itself must verify end-to-end (writer parts
    // + codec kernels + reader dispatch vs oracle) — the composite gate.
    for b in &corpus {
        let rows = verify_manifest(&b.files, &b.manifest, &b.fx)
            .unwrap_or_else(|e| panic!("pristine corpus broken ({}): {e:?}", b.fx.name));
        assert_eq!(rows, b.fx.rows(), "pristine row count ({})", b.fx.name);
    }

    // Canary tooth 1: a raw flip in the middle of a part's first Stream
    // section MUST be refused typed (CRC) — the detector detects.
    {
        let b = &corpus[0];
        let name = &part_names(&b.files)[0];
        let pristine = &b.files[name];
        // Flip one byte at 60% of the file (inside stream sections for this
        // fixture shape) without CRC repair.
        let mut evil = pristine.clone();
        let at = evil.len() * 6 / 10;
        evil[at] ^= 0x40;
        match decode_part(&evil, &b.fx.schema, 9_999) {
            Err(QaIssue::Read(_)) => {}
            other => panic!(
                "canary CRC flip was not refused typed: {:?}",
                other.map(|_| "Ok(decoded)")
            ),
        }
        // Comparator tooth: a tampered oracle MUST be caught by the
        // comparison (the checker itself has teeth).
        let cols = decode_part(pristine, &b.fx.schema, 9_998).expect("pristine decodes");
        let mut tampered: Vec<OracleCol> = cols.clone();
        if let Some(v) = tampered[0].iter_mut().find(|v| v.is_some()) {
            *v = Some(pgrc2_qa::corpus::OracleVal::Word(0xDEAD_BEEF_0BAD_F00D));
        }
        assert_ne!(cols, tampered, "comparator tooth: tampering was invisible");
    }

    // The grid.
    let mut findings: Vec<Finding> = Vec::new();
    let mut executed: u64 = 0;
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {})); // silence expected-catch noise
    for b in &corpus {
        for name in part_names(&b.files) {
            let pristine = &b.files[&name];
            let pristine_cols =
                decode_part(pristine, &b.fx.schema, 5_000).expect("pristine part decodes");
            for i in 0..s.fuzz_per_part {
                let seed = (b.fx.relfilenumber << 40)
                    ^ (u64::from(name.len() as u32) << 24)
                    ^ i.wrapping_mul(0x9E37_79B9);
                let (mutant, desc) = mutate(pristine, seed);
                executed += 1;
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    decode_part(&mutant, &b.fx.schema, 6_000 + i)
                }));
                match outcome {
                    Err(_) => findings.push(Finding::Panic {
                        fx: b.fx.name,
                        part: name.clone(),
                        desc,
                    }),
                    Ok(Err(QaIssue::Read(_))) => {} // typed refusal — the contract
                    Ok(Err(QaIssue::OutOfArena { .. })) => findings.push(Finding::OutOfArena {
                        fx: b.fx.name,
                        part: name.clone(),
                        desc,
                    }),
                    Ok(Err(other)) => findings.push(Finding::WrongSilentResult {
                        fx: b.fx.name,
                        part: name.clone(),
                        desc,
                        what: format!("unexpected issue class: {other:?}"),
                    }),
                    Ok(Ok(cols)) => {
                        if !desc.crc_fixed {
                            // Raw tier Ok ⇒ the mutation must be INVISIBLE.
                            if cols != pristine_cols {
                                findings.push(Finding::WrongSilentResult {
                                    fx: b.fx.name,
                                    part: name.clone(),
                                    desc,
                                    what: "raw mutation changed values without a refusal"
                                        .to_string(),
                                });
                            }
                        } else {
                            // Semantic tier Ok ⇒ metamorphic cross-check.
                            let cross = catch_unwind(AssertUnwindSafe(|| {
                                sel_crosscheck(&mutant, &b.fx.schema, &cols)
                            }));
                            match cross {
                                Err(_) => findings.push(Finding::Panic {
                                    fx: b.fx.name,
                                    part: name.clone(),
                                    desc,
                                }),
                                Ok(Ok(())) => {}
                                // The sel face may refuse typed where full
                                // succeeded (stricter validation) — fine.
                                Ok(Err(QaIssue::Read(_))) => {}
                                Ok(Err(QaIssue::Mismatch { .. })) => {
                                    findings.push(Finding::SelDiverged {
                                        fx: b.fx.name,
                                        part: name.clone(),
                                        desc,
                                    })
                                }
                                Ok(Err(QaIssue::OutOfArena { .. })) => {
                                    findings.push(Finding::OutOfArena {
                                        fx: b.fx.name,
                                        part: name.clone(),
                                        desc,
                                    })
                                }
                                Ok(Err(other)) => {
                                    findings.push(Finding::WrongSilentResult {
                                        fx: b.fx.name,
                                        part: name.clone(),
                                        desc,
                                        what: format!("cross-check issue: {other:?}"),
                                    })
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    std::panic::set_hook(prev_hook);

    // Manifest / CURRENT fuzz: the walk must refuse typed or answer, never
    // panic.
    let mut walk_runs = 0u64;
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for b in &corpus {
        let probe = Probe::new(TxnVerdict::Committed);
        for i in 0..(s.fuzz_per_part / 10).max(20) {
            for target in ["CURRENT", "manifest"] {
                let mut files = b.files.clone();
                let key = if target == "CURRENT" {
                    pgrc2_format::dirlayout::CURRENT_FILE_NAME.to_string()
                } else {
                    files
                        .keys()
                        .find(|k| pgrc2_format::dirlayout::parse_manifest_file_name(k).is_some())
                        .cloned()
                        .expect("a manifest file exists")
                };
                let bytes = files.get_mut(&key).expect("target file");
                if bytes.is_empty() {
                    continue;
                }
                let mut rng = pgrc2_qa::XorShift::new(i ^ (b.fx.relfilenumber << 17));
                match rng.below(3) {
                    0 => {
                        let at = rng.below(bytes.len() as u64) as usize;
                        bytes[at] ^= 1 << rng.below(8);
                    }
                    1 => {
                        let keep = rng.below(bytes.len() as u64) as usize;
                        bytes.truncate(keep);
                    }
                    _ => {
                        let extra = 1 + rng.below(64);
                        for k in 0..extra {
                            bytes.push((rng.next() ^ k) as u8);
                        }
                    }
                }
                let dir = memdir_of(&files);
                walk_runs += 1;
                let out = catch_unwind(AssertUnwindSafe(|| {
                    resolve_effective(&dir, &probe, &TableExpect::default())
                }));
                match out {
                    Err(_) => findings.push(Finding::Panic {
                        fx: b.fx.name,
                        part: key.clone(),
                        desc: MutationDesc {
                            seed: i,
                            kind: "walk_mutation",
                            at: 0,
                            crc_fixed: false,
                        },
                    }),
                    Ok(Err(_typed)) => {}
                    Ok(Ok(_maybe)) => {} // a still-valid walk answer is fine
                }
            }
        }
    }
    std::panic::set_hook(prev_hook);

    // Tooth 2: the grid actually ran.
    let expected: u64 = corpus
        .iter()
        .map(|b| part_names(&b.files).len() as u64 * s.fuzz_per_part)
        .sum();
    assert_eq!(executed, expected, "fuzzer iteration witness broken");
    assert!(walk_runs > 0, "manifest-walk fuzz did not run");
    println!(
        "read-fuzzer: {executed} part mutations + {walk_runs} walk mutations, {} findings",
        findings.len()
    );

    // Verdict: every finding is a product bug to file — report and fail.
    if !findings.is_empty() {
        for f in &findings {
            eprintln!("FUZZ FINDING: {f:?}");
        }
        panic!(
            "read-fuzzer found {} product defects (repro coordinates above)",
            findings.len()
        );
    }
}

/// decode_sel ≡ decode_full ∘ select on granule 0 of every column, against
/// the ALREADY-DECODED full answer for the same mutant.
fn sel_crosscheck(
    bytes: &[u8],
    schema: &[pgrc2_format::class::ColSchema],
    full: &[OracleCol],
) -> Result<(), QaIssue> {
    let part = open_part_bytes(bytes, 7_777)?;
    let binding = full_binding();
    for (c, col_schema) in schema.iter().enumerate() {
        let g0_rows = full[c].len().min(8192);
        if g0_rows == 0 {
            continue;
        }
        let sel: Vec<u16> = (0..g0_rows as u16).step_by(3).collect();
        let got = decode_sel_granule(&part, binding, col_schema, 0, &sel)?;
        for (k, &r) in sel.iter().enumerate() {
            if got[k] != full[c][r as usize] {
                return Err(QaIssue::Mismatch {
                    col: c as u32,
                    row: r as u64,
                    what: "decode_sel diverged from decode_full".to_string(),
                });
            }
        }
    }
    Ok(())
}
