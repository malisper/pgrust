//! [q5-fulldomain] Ungrouped byval distinct over the FULL signed domain
//! (the ClickBench Q5 refusal — `SELECT COUNT(DISTINCT UserID) FROM hits`
//! refused `tier/step/group` on every real hits bank because UserID is a
//! full-range u64 hash stored signed, and the int_set arm's admission
//! demanded a witnessed NON-NEGATIVE domain for its key+1 slot sentinel):
//!
//!   - the negative-domain shape ADMITS on both layouts (unsorted and
//!     sorted/clustered — the run-collapse arm) and answers the exact
//!     distinct count and distinct sum, escape value `-1` included
//!     (its all-ones word is the key+1 encoding's one colliding key —
//!     now diverted to the escape flag, folded exactly once);
//!   - layout identity: sorted and unsorted banks over the same multiset
//!     answer byte-identically (the admission-fragility regression guard);
//!   - the spill-forced twin (int_set_spill) answers identically and its
//!     engagement counters prove the legs ran (no vacuous green);
//!   - `-1`-free negative domains and `-1`-only banks stay exact.

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::elect::CodecCandidates;
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::planner::{check_server_grouped, plan_from_ap, AAgg, AFamily, APlan, AValExpr};
use sqe::render::to_lines;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 40_000;

fn wcol8(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::ByvalWord { width: 8, signed: true },
        typlen: 8,
        typbyval: true,
        typalign: b'd',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::SignedInt,
    }
}

/// The row law: full-range signed "UserID"s — large positives AND
/// negatives (top-bit-set hashes), the escape value -1, i64::MIN/MAX,
/// zero, and heavy-hitter runs so the sorted cut carries runs longer
/// than a frame.
fn values() -> Vec<i64> {
    let mut v: Vec<i64> = Vec::with_capacity(ROWS as usize);
    for i in 0..ROWS {
        let x = match i % 10 {
            // 40% heavy hitters (two of them: one negative, one positive).
            0 | 1 => -4_611_686_018_427_387_905i64,
            2 | 3 => 4_611_686_018_427_387_777i64,
            // 10% the escape value: the all-ones word.
            4 => -1i64,
            // extremes + zero
            5 => i64::MIN,
            6 => i64::MAX,
            7 => 0i64,
            // spread hashes over the FULL signed range
            _ => i.wrapping_mul(0x9E37_79B9_7F4A_7C15) as i64,
        };
        v.push(x);
    }
    v
}

fn seal_bank(dir: &str, sorted: bool) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let cands = CodecCandidates::new(pgrc2_write::elect::ColumnPosture {
        cold: true,
        ..Default::default()
    });
    let resolver = pgrc2_write::seal::CodecResolver;
    let mut w = TableWriter::open(
        dir.to_string(),
        vec![wcol8(1)],
        1663,
        7,
        797,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    let mut vals = values();
    if sorted {
        vals.sort_unstable();
    }
    for x in vals {
        let datums = [RawDatum::Word(x as u64)];
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&datums, &mut kit.ext, &mut env).expect("append");
    }
    let probe = Probe::new(TxnVerdict::Committed);
    {
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.finish(&mut env).expect("finish");
    }
    w.publish(&mut vfs, &probe).expect("publish");
}

fn engine(dir: &str, budget: Option<u64>) -> Engine {
    sqe::spill::register_std_store();
    let bank = Bank::open(
        dir,
        vec![ColMeta::new(1, "user", TypMeta::INT8)],
        &OpenOpts { bankstats: false, threads: 0 },
    );
    Engine::new(
        bank,
        SqeConfig {
            threads: 3,
            spill: true,
            grouped_budget_override: budget,
            ..SqeConfig::default()
        },
    )
}

fn distinct_ap(legs: Vec<AAgg>) -> APlan {
    APlan {
        sortagg_keys: Vec::new(),
        win: None,
        q: 5,
        family: AFamily::DistinctPipeline,
        tags: Vec::new(),
        cols: vec![1],
        pred: None,
        group: Vec::new(),
        agg: legs,
        agg_filters: Vec::new(),
        order: None,
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: String::new(),
        flags: Vec::new(),
    }
}

fn run_lines(eng: &Engine, legs: Vec<AAgg>) -> Vec<String> {
    let node = plan_from_ap(&eng.bank, &eng.faces, &distinct_ap(legs)).expect("lower");
    check_server_grouped(&eng.bank, &eng.faces, &node).expect("admission");
    to_lines(&eng.run(&node))
}

fn bank_dir(tag: &str, sorted: bool) -> String {
    let dir = std::env::temp_dir().join(format!(
        "sqe_dfulldom_{tag}_{}_{}",
        if sorted { "s" } else { "u" },
        std::process::id()
    ));
    let d = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// Both layouts admit, answer the exact oracle (escape value included),
/// spill-forced twin identical + engaged.
#[test]
fn negative_domain_distinct_admits_and_answers() {
    use std::collections::BTreeSet;
    use std::sync::atomic::Ordering;
    let set: BTreeSet<i64> = values().into_iter().collect();
    let want_n = set.len() as i64;
    let want_sum: i128 = set.iter().map(|&x| x as i128).sum();
    assert!(set.contains(&-1), "row law must exercise the escape value");
    assert!(*set.iter().next().unwrap() < 0, "row law must be negative-domain");

    let legs = || vec![
        AAgg::CountDistinct { e: AValExpr::Col(1) },
        AAgg::SumDistinct { e: AValExpr::Col(1) },
    ];
    let mut arms: Vec<Vec<String>> = Vec::new();
    for sorted in [false, true] {
        let d = bank_dir("main", sorted);
        seal_bank(&d, sorted);
        // Tuned arm (budget never crossed at this scale).
        let eng = engine(&d, None);
        let lines = run_lines(&eng, legs());
        assert_eq!(lines.len(), 1, "one answer row");
        let cells: Vec<&str> = lines[0].split('\t').collect();
        assert_eq!(cells[0], want_n.to_string(), "distinct count (sorted={sorted})");
        assert_eq!(cells[1], want_sum.to_string(), "distinct sum (sorted={sorted})");
        // Spill-forced twin: same answer, engaged legs.
        let f0 = sqe::stencils::distinct::SSPILL_FLUSHES.load(Ordering::Relaxed);
        let eng2 = engine(&d, Some(64 << 10));
        let lines2 = run_lines(&eng2, legs());
        assert_eq!(lines2, lines, "spill twin identity (sorted={sorted})");
        assert!(
            sqe::stencils::distinct::SSPILL_FLUSHES.load(Ordering::Relaxed) > f0,
            "spill leg must actually flush (sorted={sorted})"
        );
        arms.push(lines);
    }
    assert_eq!(arms[0], arms[1], "layout identity: sorted == unsorted");
}

/// A bank whose ONLY value is the escape key stays exact (count 1, sum -1),
/// and an all-negative -1-free bank is exact too.
#[test]
fn escape_edge_banks_exact() {
    for (tag, vals, want) in [
        ("neg1", vec![-1i64; 5000], ("1", "-1")),
        ("nofn1", (0..5000).map(|i| -2 - (i % 7) as i64).collect::<Vec<_>>(), ("7", "-35")),
    ] {
        let d = bank_dir(tag, false);
        {
            // seal a tiny bespoke bank
            std::fs::create_dir_all(&d).unwrap();
            let mut vfs = RealVfs;
            let mut kit = Kit::new();
            let cands = CodecCandidates::new(Default::default());
            let resolver = pgrc2_write::seal::CodecResolver;
            let mut w = TableWriter::open(
                d.clone(),
                vec![wcol8(1)],
                1663,
                7,
                797,
                TxnStamp { fxid: 100, cid: 1 },
                &Default::default(),
                PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
            )
            .expect("open writer");
            for x in &vals {
                let datums = [RawDatum::Word(*x as u64)];
                let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&cands];
                let mut env = SealEnv {
                    vfs: &mut vfs,
                    sources: &sources,
                    resolver: &resolver,
                    shred: &mut kit.shred,
                    shred_opts: &kit.opts,
                };
                w.append_row(&datums, &mut kit.ext, &mut env).expect("append");
            }
            let probe = Probe::new(TxnVerdict::Committed);
            {
                let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&cands];
                let mut env = SealEnv {
                    vfs: &mut vfs,
                    sources: &sources,
                    resolver: &resolver,
                    shred: &mut kit.shred,
                    shred_opts: &kit.opts,
                };
                w.finish(&mut env).expect("finish");
            }
            w.publish(&mut vfs, &probe).expect("publish");
        }
        let eng = engine(&d, None);
        let lines = run_lines(&eng, vec![
            AAgg::CountDistinct { e: AValExpr::Col(1) },
            AAgg::SumDistinct { e: AValExpr::Col(1) },
        ]);
        let cells: Vec<&str> = lines[0].split('\t').collect();
        assert_eq!((cells[0], cells[1]), want, "{tag}");
    }
}
