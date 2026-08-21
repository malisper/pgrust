//! [scan-cap-retire] The ClickBench q19 unrefusal (the q1/q5 idiom:
//! extend the served vocabulary by making the arm lawful): an UNBOUNDED
//! row-returning scan whose zone planes cannot witness the survivor
//! count under the answer cap is SERVED under the answer-face law — the
//! stencil counts the TRUE survivors and refuses typed
//! (`scan-answer-over-cap`) the moment they exceed the cap, bounded
//! state, never a truncated answer, never an OOM. Provenance:
//! `SELECT UserID FROM hits WHERE UserID = <const>` refused
//! `scan-rows-unwitnessed:bound-over-cap` on every 100m cell (r14 tax
//! cells 20260821T035653Z/035759Z, M2 `Q19|refused`): equality on a
//! full-range hash column prunes NO granule by zone, so the sound
//! pre-scan bound is rows_total — the witness can never exist on real
//! data, exactly the q5 admission-gate disease.
//!
//! Pins (cap dialed through `SqeConfig.scan_answer_cap_override` — the
//! pricing/gate-arm dial, env is process-global):
//!  1. serve-unwitnessed: zone-bound OVER the cap, true survivors under
//!     it — admission passes (was `ScanRowsUnwitnessed`) and the answer
//!     equals the brute-force oracle at widths 1 and 8.
//!  2. typed answer-face refusal: true survivors OVER the cap — the run
//!     raises `RunRefusal(ScanAnswerOverCap)`, the same unwind the
//!     grouped finalize answer-bytes law rides.
//!  3. kill switch: `scan_cap_retire=false` reproduces the legacy
//!     admission refusal byte-for-byte
//!     (`scan-rows-unwitnessed:bound-over-cap`).

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::planner::{check_server_scan, plan_from_ap, AAgg, AFamily, APlan, APred, AValExpr};
use sqe::refuse::{Refuse, RunRefusal};
use sqe::render::to_lines;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 30_000;

fn wcol(attno: u32, width: u8, typlen: i16) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::ByvalWord { width, signed: true },
        typlen,
        typbyval: true,
        typalign: if width == 8 { b'd' } else { b'i' },
        collation_class: CollationClass::C,
        semantics: TypeSemantics::SignedInt,
    }
}

/// Row law: `a` unique (the emit column); `b = i % 7` — every granule
/// spans the whole 0..=6 domain, so an equality conjunct on `b` prunes
/// NOTHING by zone (the q19 shape: the sound pre-scan survivor bound is
/// rows_total).
fn row(i: u64) -> (i64, i64) {
    (9_000_000_000 + i as i64, (i % 7) as i64)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        793,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (a, b) = row(i);
        let datums = [RawDatum::Word(a as u64), RawDatum::Word(b as u64)];
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&datums, &mut kit.ext, &mut env).expect("append");
    }
    let probe = Probe::new(TxnVerdict::Committed);
    {
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.finish(&mut env).expect("finish");
    }
    w.publish(&mut vfs, &probe).expect("publish");
}

fn open_engine(dir: &str, threads: usize, cap: u64, retire: bool) -> Engine {
    let schema =
        vec![ColMeta::new(1, "a", TypMeta::INT8), ColMeta::new(2, "b", TypMeta::INT4)];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: true, threads: 0 });
    Engine::new(
        bank,
        SqeConfig {
            threads,
            scan_answer_cap_override: Some(cap),
            scan_cap_retire: retire,
            ..SqeConfig::default()
        },
    )
}

/// `SELECT a, b WHERE b = 3` — unbounded (no LIMIT), unkeyed.
fn scan_plan() -> APlan {
    APlan {
        sortagg_keys: Vec::new(),
        win: None,
        q: 19,
        family: AFamily::ScanServe,
        tags: Vec::new(),
        cols: vec![1, 2],
        pred: Some(APred::Range { col: 2, lo: "3".into(), hi: "3".into(), fp: None }),
        group: Vec::new(),
        agg: vec![AAgg::Emit { e: AValExpr::Col(1) }, AAgg::Emit { e: AValExpr::Col(2) }],
        order: None,
        having: None,
        agg_filters: Vec::new(),
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "scan-cap-retire".into(),
        flags: Vec::new(),
    }
}

fn bank_dir(tag: &str) -> String {
    format!("{}/sqe-scan-cap-retire-{tag}-{}", std::env::temp_dir().display(), std::process::id())
}

#[test]
fn unwitnessed_scan_serves_and_caps_typed() {
    let dir = bank_dir("main");
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let survivors: u64 = (0..ROWS).filter(|&i| row(i).1 == 3).count() as u64;
    assert!(survivors > 1000, "row law drifted: {survivors}");

    // Pin 1: serve-unwitnessed. Zone bound = ROWS (no granule prunes),
    // cap 5000 < ROWS, true survivors ~4286 < cap: admission must pass
    // and the answer must equal the oracle at both widths.
    let mut oracle: Vec<String> = (0..ROWS)
        .filter(|&i| row(i).1 == 3)
        .map(|i| {
            let (a, b) = row(i);
            format!("{a}\t{b}")
        })
        .collect();
    oracle.sort();
    for threads in [1usize, 8] {
        let e = open_engine(&dir, threads, 5_000, true);
        let node = plan_from_ap(&e.bank, &e.faces, &scan_plan()).expect("plan scan-serve");
        check_server_scan(&e.bank, &e.faces, &node)
            .expect("cap-retire arm must admit the unwitnessed unbounded scan");
        let a = e.run(&node);
        let mut got = to_lines(&a);
        got.sort();
        assert_eq!(got, oracle, "served answer vs oracle at width {threads}");
    }

    // Pin 2: typed answer-face refusal. Cap 1000 < true survivors: the
    // run must raise RunRefusal(ScanAnswerOverCap) — typed, never a
    // truncated answer.
    {
        let e = open_engine(&dir, 8, 1_000, true);
        let node = plan_from_ap(&e.bank, &e.faces, &scan_plan()).expect("plan scan-serve");
        check_server_scan(&e.bank, &e.faces, &node).expect("admission still passes");
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| e.run(&node)));
        let payload = r.expect_err("over-cap scan must refuse at the answer face");
        match payload.downcast::<RunRefusal>() {
            Ok(rr) => match rr.0 {
                Refuse::ScanAnswerOverCap { got, cap } => {
                    assert_eq!(cap, 1_000);
                    assert!(got > cap, "counted survivors over the cap: got={got}");
                }
                other => panic!("wrong typed refusal: {other}"),
            },
            Err(p) => std::panic::resume_unwind(p),
        }
    }

    // Pin 3: kill switch. The legacy admission gate is one env away:
    // scan_cap_retire=false reproduces scan-rows-unwitnessed verbatim.
    {
        let e = open_engine(&dir, 2, 1_000, false);
        let node = plan_from_ap(&e.bank, &e.faces, &scan_plan()).expect("plan scan-serve");
        match check_server_scan(&e.bank, &e.faces, &node) {
            Err(Refuse::ScanRowsUnwitnessed { what }) => assert_eq!(what, "bound-over-cap"),
            other => panic!("kill switch must restore the witness gate, got {other:?}"),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
