//! [spill-5] The VAR lane's fp128 distinct plane (spill-design §6 v5).

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::Family;
use sqe::planner::check_server_grouped;
use sqe::refuse::Refuse;
use sqe::render::to_lines;
use sqe::rig::lower::{lower, parse_sql};
use sqe::rig::oracle::run_oracle;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 60_000;

fn wcol(attno: u32) -> ColSchema {
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

fn tcol(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::VarlenaVerbatim,
        typlen: -1,
        typbyval: false,
        typalign: b'i',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::TextCollated,
    }
}

/// s = key (17); t = LIKE target; u = distinct (~9013, over every share).
fn row(i: u64) -> (i64, String, String, String) {
    let s = format!("s{:02}", i % 17);
    let t = if i % 2 == 0 { format!("alpha{:03}", i % 500) } else { format!("beta{:03}", i % 37) };
    let u = format!("did:plc:{:06}", (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 13) % 9013);
    (i as i64, s, t, u)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let cands = pgrc2_write::elect::CodecCandidates::new(Default::default());
    let resolver = pgrc2_write::seal::CodecResolver;
    let schema = vec![wcol(1), tcol(2), tcol(3), tcol(4)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        9,
        811,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (k, s, t, u) = row(i);
        let (si, ti, ui) = (img_4b_u(s.as_bytes()), img_4b_u(t.as_bytes()), img_4b_u(u.as_bytes()));
        let datums = [
            RawDatum::Word(k as u64),
            RawDatum::Bytes(&si),
            RawDatum::Bytes(&ti),
            RawDatum::Bytes(&ui),
        ];
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
    w.publish(&mut vfs, &Probe::new(TxnVerdict::Committed)).expect("publish");
}

fn schema_meta() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "k", TypMeta::INT8),
        ColMeta::new(2, "s", TypMeta::TEXT_C),
        ColMeta::new(3, "t", TypMeta::TEXT_C),
        ColMeta::new(4, "u", TypMeta::TEXT_C),
    ]
}

fn bank_dir() -> String {
    let dir = std::env::temp_dir().join(format!("sqe_wrspill_{}", std::process::id()));
    let d = dir.to_str().unwrap().to_string();
    static SEALED: std::sync::Once = std::sync::Once::new();
    SEALED.call_once(|| {
        let _ = std::fs::remove_dir_all(&d);
        seal_bank(&d);
    });
    d
}

/// `None` = the E18 default (the resident twin); `Some` = forced-low.
fn engine(budget: Option<u64>, spill: bool) -> Engine {
    sqe::spill::register_std_store();
    let bank = Bank::open(&bank_dir(), schema_meta(), &OpenOpts { bankstats: false, threads: 0 });
    Engine::new(
        bank,
        SqeConfig { threads: 3, spill, grouped_budget_override: budget, ..SqeConfig::default() },
    )
}

const Q2: &str = "SELECT s, COUNT(*), COUNT(DISTINCT u) FROM wv \
     WHERE t LIKE 'alpha%' GROUP BY s ORDER BY COUNT(*) DESC LIMIT 20;";

fn run_q2(eng: &Engine, q: u32) -> Vec<String> {
    let node = lower(&eng.ctx(), q, Q2, "wrspill").expect("lower");
    assert_eq!(node.family, Family::WindowReplay, "the q2 shape must elect the VAR lane");
    to_lines(&eng.run(&node))
}

#[test]
fn wr_spill_vs_resident_identity() {
    let twin = engine(None, true);
    let want = run_q2(&twin, 1);
    // the twin also matches the scalar oracle (row-law truth)
    let ast = parse_sql(&twin.bank, Q2).expect("parse");
    assert_eq!(to_lines(&run_oracle(&twin.bank, &ast)), want, "resident twin vs oracle");
    for (qi, budget) in [(10u32, 4096u64), (11, 1 << 16), (12, 1 << 20)] {
        let eng = engine(Some(budget), true);
        let got = run_q2(&eng, qi);
        assert_eq!(got, want, "spill identity: budget={budget}");
    }
}

#[test]
fn wr_spill_engagement_and_determinism() {
    use std::sync::atomic::Ordering::Relaxed;
    use sqe::stencils::window_replay::{WRSPILL_MERGES, WRSPILL_SCATTERS};
    let eng = engine(Some(4096), true);
    let (s0, m0) = (WRSPILL_SCATTERS.load(Relaxed), WRSPILL_MERGES.load(Relaxed));
    let a = run_q2(&eng, 20);
    assert!(WRSPILL_SCATTERS.load(Relaxed) > s0, "worker set drains engaged");
    assert!(WRSPILL_MERGES.load(Relaxed) > m0, "finalize dedupe-merges engaged");
    // warm replay (the published plane) + rerun determinism, spilled
    let b = run_q2(&eng, 20);
    assert_eq!(a, b, "spilled rerun must be byte-identical");
}

/// Kill switch / no substrate: freeze at the share, typed refusal.
#[test]
fn wr_kill_switch_refuses_typed() {
    let eng = engine(Some(4096), false);
    let node = lower(&eng.ctx(), 30, Q2, "wrspill").expect("lower");
    let p = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| eng.run(&node)))
        .expect_err("over-share without a substrate must refuse at finalize");
    let rr = p.downcast::<sqe::refuse::RunRefusal>().expect("typed RunRefusal payload");
    match rr.0 {
        Refuse::GroupedSpillUnavailable { what: "no-substrate", est, budget } => {
            assert_eq!(budget, 4096, "the E18b share is the refusal's budget");
            assert!(est > budget, "the latch fires past the share");
        }
        other => panic!("expected no-substrate refusal, got {other:?}"),
    }
}

/// Over budget admits only with a substrate and the switch on.
#[test]
fn wr_budget_admission_verdicts() {
    let over = engine(Some(4096), true);
    let node = lower(&over.ctx(), 40, Q2, "wrspill").expect("lower");
    check_server_grouped(&over.bank, &over.faces, &node)
        .expect("over budget with a substrate admits (spill-served)");
    let killed = engine(Some(4096), false);
    match check_server_grouped(&killed.bank, &killed.faces, &node) {
        Err(Refuse::GroupedSpillUnavailable { what: "no-substrate", .. }) => {}
        other => panic!("kill switch must stay fail-closed on this plane, got {other:?}"),
    }
    let dflt = engine(None, true);
    check_server_grouped(&dflt.bank, &dflt.faces, &node).expect("default budget admits");
}
