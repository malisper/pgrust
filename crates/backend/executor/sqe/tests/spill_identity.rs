//! [P6-1 spill] Grouped-agg spill gates (spill-design.md):
//!
//!   - BYTE identity spill vs no-spill across the SoA vocabulary — high-
//!     NDV wide payload, packed payload, nullable keys, two-key packs,
//!     pushed top-k, offset/limit — the spill arm forced by a tiny
//!     PGRUST_SQE_GROUPED_BUDGET-class override, the twin on the default
//!     law (which also crosses the tuned-arm elections: count-only rides
//!     int_key on the twin and the SoA spill arm when forced);
//!   - an independent scalar oracle on the run-merge leg;
//!   - determinism: the spill arm reruns byte-identical;
//!   - the budget law's admission verdicts: spillable shapes admit, the
//!     no-spill-arm residue refuses `grouped-spill-unavailable` typed,
//!     the kill switch reverts to the legacy verdicts.

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::OrderBy;
use sqe::planner::{check_server_grouped, plan_from_ap, AAgg, AFamily, AKeyExpr, APlan, AValExpr};
use sqe::refuse::Refuse;
use sqe::render::to_lines;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 60_000;

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

/// The row law. Columns:
///   1 k    int4 group key, null-free, 500 distinct;
///   2 kh   int8 group key, null-free, HIGH NDV (~unique per row) — the
///          run-merge leg's key;
///   3 b    int4 sum input in {0,1} (the packed payload witness);
///   4 q    int4 fold input in [1,50] (wide sums, packed avg);
///   5 kn   int4 group key, NULLABLE (the 3VL pack lanes);
///   6 vn   int4 fold input, NULLABLE (a no-spill-arm shape — residue).
fn row(i: u64) -> (i64, i64, i64, i64, Option<i64>, Option<i64>) {
    let k = ((i.wrapping_mul(0x9E37_79B9)) >> 5) as i64 % 500;
    let kh = (i.wrapping_mul(0x517c_c1b7) >> 3) as i64 % 50_000_000;
    let b = (i % 3 == 0) as i64;
    let q = (i % 50) as i64 + 1;
    let kn = (i % 11 != 4).then_some(((i.wrapping_mul(0xD6E8_FEB8)) >> 7) as i64 % 40);
    let vn = (i % 7 != 2).then_some((i % 1000) as i64);
    (k, kh, b, q, kn, vn)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![
        wcol(1, 4, 4),
        wcol(2, 8, 8),
        wcol(3, 4, 4),
        wcol(4, 4, 4),
        wcol(5, 4, 4),
        wcol(6, 4, 4),
    ];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        6,
        793,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (k, kh, b, q, kn, vn) = row(i);
        let datums = [
            RawDatum::Word(k as u64),
            RawDatum::Word(kh as u64),
            RawDatum::Word(b as u64),
            RawDatum::Word(q as u64),
            kn.map(|x| RawDatum::Word(x as u64)).unwrap_or(RawDatum::Null),
            vn.map(|x| RawDatum::Word(x as u64)).unwrap_or(RawDatum::Null),
        ];
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

fn schema_meta() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "k", TypMeta::INT4),
        ColMeta::new(2, "kh", TypMeta::INT8),
        ColMeta::new(3, "b", TypMeta::INT4),
        ColMeta::new(4, "q", TypMeta::INT4),
        ColMeta::new(5, "kn", TypMeta::INT4),
        ColMeta::new(6, "vn", TypMeta::INT4),
    ]
}

fn bank_dir() -> String {
    let dir = std::env::temp_dir().join(format!("sqe_spillid_{}", std::process::id()));
    let d = dir.to_str().unwrap().to_string();
    static SEALED: std::sync::Once = std::sync::Once::new();
    SEALED.call_once(|| {
        let _ = std::fs::remove_dir_all(&d);
        seal_bank(&d);
    });
    d
}

/// `budget`: `None` = the E18 default law (never crossed at this bank —
/// the no-spill twin); `Some(bytes)` = the forced-low pricing arm.
fn engine(budget: Option<u64>, spill: bool) -> Engine {
    sqe::spill::register_std_store();
    let bank = Bank::open(&bank_dir(), schema_meta(), &OpenOpts { bankstats: false, threads: 0 });
    Engine::new(
        bank,
        SqeConfig { threads: 3, spill, grouped_budget_override: budget, ..SqeConfig::default() },
    )
}

fn ap(group: Vec<u32>, agg: Vec<AAgg>) -> APlan {
    let mut cols = group.clone();
    for a in &agg {
        if let AAgg::Sum { e: AValExpr::Col(c) }
        | AAgg::Avg { e: AValExpr::Col(c) }
        | AAgg::Min { e: AValExpr::Col(c) }
        | AAgg::Max { e: AValExpr::Col(c) } = a
        {
            if !cols.contains(c) {
                cols.push(*c);
            }
        }
    }
    APlan {
        sortagg_keys: Vec::new(),
        win: None,
        q: 0,
        family: AFamily::HashPlaneOwnedGroup,
        tags: Vec::new(),
        cols,
        pred: None,
        group: group.into_iter().map(AKeyExpr::Col).collect(),
        agg,
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

/// `bound`: `Some((limit, offset))` arms the CountDesc/limit selection
/// (the down-pass contract the spill merge must feed).
fn run_lines(
    eng: &Engine,
    group: Vec<u32>,
    agg: Vec<AAgg>,
    bound: Option<(usize, usize)>,
) -> Vec<String> {
    let mut node = plan_from_ap(&eng.bank, &eng.faces, &ap(group, agg)).expect("lower");
    if let Some((limit, offset)) = bound {
        node.params.order = OrderBy::CountDesc;
        node.params.limit = limit;
        node.params.offset = offset;
    }
    let mut l = to_lines(&eng.run(&node));
    if bound.is_none() {
        l.sort();
    }
    l
}

// ---------------------------------------------------------------------------
// identity gates: spill-forced vs the default-law twin
// ---------------------------------------------------------------------------

/// The full identity sweep. Tiny budgets force pass-1 scatter spill on
/// every leg and pass-2 run merges on the high-NDV legs; the twin runs
/// today's in-memory arms (including the int_key/tuned elections the
/// spill arm bypasses).
#[test]
fn spill_vs_resident_identity() {
    let twin = engine(None, true);
    for budget in [1 << 12, 1 << 16, 1 << 20] {
        let sp = engine(Some(budget), true);
        type Case = (Vec<u32>, Vec<AAgg>, Option<(usize, usize)>);
        let cases: Vec<Case> = vec![
            // high-NDV, wide payload (run-merge leg)
            (vec![2], vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(4) }], None),
            // high-NDV count-only (twin rides int_key; spill arm rides SoA)
            (vec![2], vec![AAgg::CountStar], None),
            // packed payload: sum{0,1} + avg[1,50]
            (
                vec![1],
                vec![
                    AAgg::CountStar,
                    AAgg::Sum { e: AValExpr::Col(3) },
                    AAgg::Avg { e: AValExpr::Col(4) },
                ],
                None,
            ),
            // nullable key: the 3VL pack lanes
            (vec![5], vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(4) }], None),
            // two-key pack (byval pair), one nullable
            (vec![1, 5], vec![AAgg::CountStar], None),
            // pushed top-k over the high-NDV key (the down-pass leg)
            (vec![2], vec![AAgg::CountStar], Some((7, 0))),
            // offset/limit window
            (vec![1], vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(4) }], Some((5, 3))),
        ];
        for (g, mix, bound) in cases {
            let a = run_lines(&sp, g.clone(), mix.clone(), bound);
            let b = run_lines(&twin, g.clone(), mix.clone(), bound);
            assert_eq!(a, b, "spill identity: budget={budget} group={g:?} {mix:?} {bound:?}");
        }
    }
}

/// The run-merge leg vs an independent scalar oracle (BTreeMap over the
/// row law — no engine code shared).
#[test]
fn spill_merge_vs_oracle() {
    use std::sync::atomic::Ordering::Relaxed;
    use sqe::stencils::hash_plane::{SPILL_FLUSHES, SPILL_MERGES};
    let sp = engine(Some(1 << 12), true);
    let (f0, m0) = (SPILL_FLUSHES.load(Relaxed), SPILL_MERGES.load(Relaxed));
    let lines = run_lines(
        &sp,
        vec![2],
        vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(4) }],
        None,
    );
    // The leg must EXERCISE the spill lanes, not pass vacuously.
    assert!(SPILL_FLUSHES.load(Relaxed) > f0, "pass-1 scatter spill engaged");
    assert!(SPILL_MERGES.load(Relaxed) > m0, "pass-2 run merge engaged");
    let mut m: std::collections::BTreeMap<i64, (u64, i128)> = Default::default();
    for i in 0..ROWS {
        let (_, kh, _, q, _, _) = row(i);
        let g = m.entry(kh).or_default();
        g.0 += 1;
        g.1 += q as i128;
    }
    let mut want: Vec<String> =
        m.iter().map(|(k, (c, s))| format!("{k}\t{c}\t{s}")).collect();
    want.sort();
    assert_eq!(lines, want, "spill merge vs scalar oracle");
}

/// The spill arm is deterministic: rerunning the same forced-low shape
/// answers byte-identically (parked state and file reuse included).
#[test]
fn spill_rerun_determinism() {
    let sp = engine(Some(1 << 12), true);
    let mix = vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(4) }];
    let a = run_lines(&sp, vec![2], mix.clone(), None);
    let b = run_lines(&sp, vec![2], mix.clone(), None);
    let c = run_lines(&sp, vec![2], mix, Some((7, 0)));
    let d = run_lines(&sp, vec![2], vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(4) }], Some((7, 0)));
    assert_eq!(a, b, "spill rerun determinism (full)");
    assert_eq!(c, d, "spill rerun determinism (top-k)");
}

// ---------------------------------------------------------------------------
// admission verdicts: the budget law's three outcomes
// ---------------------------------------------------------------------------

fn verdict(eng: &Engine, group: Vec<u32>, agg: Vec<AAgg>) -> Result<(), Refuse> {
    let node = plan_from_ap(&eng.bank, &eng.faces, &ap(group, agg)).expect("lower");
    check_server_grouped(&eng.bank, &eng.faces, &node)
}

/// Over budget: spillable shapes admit; the nullable-fold-input shape
/// (no spill arm this rung) refuses typed; the kill switch restores the
/// legacy verdict.
#[test]
fn budget_admission_verdicts() {
    let sp = engine(Some(1 << 12), true);
    // spillable: admitted (the spill arm bounds it)
    verdict(&sp, vec![1], vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(4) }])
        .expect("spillable shape admits over budget");
    // nullable fold INPUT -> foundation-cells route: no spill arm yet
    match verdict(&sp, vec![1], vec![AAgg::Sum { e: AValExpr::Col(6) }]) {
        Err(Refuse::GroupedSpillUnavailable { what: "no-spill-arm", .. }) => {}
        other => panic!("expected no-spill-arm refusal, got {other:?}"),
    }
    // min/max legs likewise ride the residue
    match verdict(&sp, vec![2], vec![AAgg::Min { e: AValExpr::Col(4) }]) {
        Err(Refuse::GroupedSpillUnavailable { what: "no-spill-arm", .. }) => {}
        other => panic!("expected no-spill-arm refusal, got {other:?}"),
    }
    // kill switch: the legacy arm admits (no accounting, no refusal)
    let legacy = engine(Some(1 << 12), false);
    verdict(&legacy, vec![1], vec![AAgg::Sum { e: AValExpr::Col(6) }])
        .expect("kill switch restores the legacy verdict");
    // default law: nothing changes at this bank's scale
    let dflt = engine(None, true);
    verdict(&dflt, vec![1], vec![AAgg::Sum { e: AValExpr::Col(6) }])
        .expect("default budget admits as today");
}

/// Direct-array shapes stay Bounded under the law (elected, no spill),
/// and under a forced-tiny budget the election yields to the spillable
/// hash arm with identical answers.
#[test]
fn direct_array_budget_election() {
    let sp = engine(Some(1 << 12), true);
    let dflt = engine(None, true);
    // col 1 has a dense witnessed domain (0..499) — direct-array eligible
    let mix = vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(4) }];
    let a = run_lines(&sp, vec![1], mix.clone(), None);
    let b = run_lines(&dflt, vec![1], mix, None);
    assert_eq!(a, b, "direct-array vs budget-elected spill arm identity");
}

// ---------------------------------------------------------------------------
// [cap-retire] the 2^20 witness-cap retirement + the finalize answer law
// (RULED Michael 2026-08-19, spill-design.md §5: correctness-first)
// ---------------------------------------------------------------------------

/// The former `GroupCountUnwitnessed { "bound-over-cap" }` class: the
/// high-NDV kh shape (witnessed bound 50M >> 2^20) now ADMITS — the
/// spill arm bounds its state — and answers its TRUE group set, byte-
/// identical across the spill-forced and default arms and equal to an
/// independent distinct-count oracle (no truncation, ever). The kill
/// switch restores the legacy witness gate verbatim.
#[test]
fn cap_retire_admission_and_identity() {
    let dflt = engine(None, true);
    let mix = vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(4) }];
    verdict(&dflt, vec![2], mix.clone())
        .expect("cap-retire: the bound-over-cap kh shape must admit (spill-served)");
    let sp = engine(Some(1 << 12), true);
    let a = run_lines(&sp, vec![2], mix.clone(), None);
    let b = run_lines(&dflt, vec![2], mix.clone(), None);
    assert_eq!(a, b, "cap-retire identity: spill-forced vs default arm");
    let mut oracle = std::collections::BTreeSet::new();
    for i in 0..ROWS {
        oracle.insert(row(i).1);
    }
    assert_eq!(a.len(), oracle.len(), "the FULL true group set emits (no truncation)");
    // kill switch: the legacy gate verbatim (typed witness refusal)
    let legacy = engine(Some(1 << 12), false);
    match verdict(&legacy, vec![2], mix) {
        Err(Refuse::GroupCountUnwitnessed { what: "bound-over-cap" }) => {}
        other => panic!("kill switch must restore the witness gate, got {other:?}"),
    }
}

/// The finalize answer-bytes law (spill-design.md §3.4): under a tiny
/// answer-budget override the flipped shape refuses TYPED at finalize
/// with the exact counted account carried — `got` is the true group
/// count times this arm's exact staging + render widths (Row staging 48
/// B; kh key lane 8 B + CountStar lane 8 B), never an estimate. A
/// generous budget serves the same statement.
#[test]
fn answer_bytes_law_exact_accounting() {
    sqe::spill::register_std_store();
    // grouped budget forced low: the refusal leg prices the SoA spill
    // arm's exact staging (Row = 48 B) rather than a tuned-arm layout.
    let mk = |answer: Option<u64>, grouped: Option<u64>| {
        let bank = Bank::open(&bank_dir(), schema_meta(), &OpenOpts { bankstats: false, threads: 0 });
        Engine::new(
            bank,
            SqeConfig {
                threads: 3,
                spill: true,
                grouped_budget_override: grouped,
                answer_budget_override: answer,
                ..SqeConfig::default()
            },
        )
    };
    let g_true = {
        let mut s = std::collections::BTreeSet::new();
        for i in 0..ROWS {
            s.insert(row(i).1);
        }
        s.len() as u64
    };
    // The arm's exact answer-plane widths for `kh, count(*)`:
    const STAGE_ROW: u64 = 48; // size_of::<hash_plane::Row>()
    const RENDER_ROW: u64 = 8 + 8; // key i64 lane + CountStar i64 lane
    let tiny = mk(Some(4096), Some(1 << 12));
    let node = plan_from_ap(&tiny.bank, &tiny.faces, &ap(vec![2], vec![AAgg::CountStar]))
        .expect("lower");
    check_server_grouped(&tiny.bank, &tiny.faces, &node).expect("cap-retire admits");
    // unwind-ok: asserting the typed runtime-refusal payload itself
    let p = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tiny.run(&node)))
        .expect_err("tiny answer budget must refuse at finalize");
    let rr = p
        .downcast::<sqe::refuse::RunRefusal>()
        .expect("payload must be the typed RunRefusal");
    match rr.0 {
        Refuse::GroupAnswerOverBudget { got, budget } => {
            assert_eq!(budget, 4096, "the override is the effective budget");
            assert_eq!(
                got,
                g_true * (STAGE_ROW + RENDER_ROW),
                "exact counted account: true groups x exact plane widths"
            );
        }
        other => panic!("expected GroupAnswerOverBudget, got {other:?}"),
    }
    // a generous budget serves the same statement whole
    let wide = mk(None, None);
    let node2 = plan_from_ap(&wide.bank, &wide.faces, &ap(vec![2], vec![AAgg::CountStar]))
        .expect("lower");
    let a = wide.run(&node2);
    assert_eq!(a.nrows as u64, g_true, "served whole under the default law");
}

// ---------------------------------------------------------------------------
// [spill-2] the dense-distinct route's pair-plane spill (spill-design.md
// §6 rung: distinct pipeline, hash-plane form)
// ---------------------------------------------------------------------------

/// Identity spill-forced vs the resident twin across the distinct-leg
/// vocabulary (count/sum/avg DISTINCT + mixed plain folds), plus the
/// scalar oracle, engagement counters and the kill switch.
#[test]
fn distinct_pair_spill_identity() {
    use std::sync::atomic::Ordering::Relaxed;
    use sqe::stencils::hash_group::{DSPILL_DRAINS, DSPILL_FLUSHES, DSPILL_MERGES};
    let twin = engine(None, true);
    let mk = |ags: Vec<AAgg>| -> Vec<AAgg> { ags };
    type Case = (Vec<u32>, Vec<AAgg>, Option<(usize, usize)>);
    let cases: Vec<Case> = vec![
        // count(distinct kh) by k: high-NDV distinct values (drain leg)
        (vec![1], mk(vec![AAgg::CountStar, AAgg::CountDistinct { e: AValExpr::Col(2) }]), None),
        // count(distinct q) by k + a plain sum lane
        (
            vec![1],
            mk(vec![
                AAgg::CountDistinct { e: AValExpr::Col(4) },
                AAgg::Sum { e: AValExpr::Col(4) },
            ]),
            None,
        ),
        // sum(distinct)/avg(distinct): the first-seen value fold
        (
            vec![1],
            mk(vec![
                AAgg::SumDistinct { e: AValExpr::Col(4) },
                AAgg::AvgDistinct { e: AValExpr::Col(4) },
            ]),
            None,
        ),
        // bounded answer over the distinct fold
        (vec![1], mk(vec![AAgg::CountDistinct { e: AValExpr::Col(2) }]), Some((7, 2))),
    ];
    // Budgets sit ABOVE the (unspillable) dense-array floor for k's
    // 500-slot domain (~48 KB priced) and BELOW the pair plane — the
    // spill arm engages; a budget under the dense floor refuses typed
    // (asserted in `distinct_budget_admission_flips`).
    for budget in [1 << 16, 1 << 17] {
        let sp = engine(Some(budget), true);
        for (g, mix, bound) in &cases {
            let a = run_lines(&sp, g.clone(), mix.clone(), *bound);
            let b = run_lines(&twin, g.clone(), mix.clone(), *bound);
            assert_eq!(a, b, "distinct pair-spill identity: budget={budget} {mix:?} {bound:?}");
        }
    }
    // Engagement: a 50-slot group domain (q) keeps the unspillable
    // dense floor UNDER an 8 KiB budget while the ~60k distinct
    // (q, kh) pairs blow the per-owner dedupe cap — the leg must flush,
    // drain and merge (identity vs the twin AND the scalar oracle).
    let sp = engine(Some(8192), true);
    let (f0, d0, m0) =
        (DSPILL_FLUSHES.load(Relaxed), DSPILL_DRAINS.load(Relaxed), DSPILL_MERGES.load(Relaxed));
    let mix = vec![AAgg::CountStar, AAgg::CountDistinct { e: AValExpr::Col(2) }];
    let lines = run_lines(&sp, vec![4], mix.clone(), None);
    assert!(DSPILL_FLUSHES.load(Relaxed) > f0, "pair scatter spill engaged");
    assert!(DSPILL_DRAINS.load(Relaxed) > d0, "dedupe-table drain engaged");
    assert!(DSPILL_MERGES.load(Relaxed) > m0, "pair run dedupe-merge engaged");
    assert_eq!(
        lines,
        run_lines(&twin, vec![4], mix, None),
        "drain-leg identity vs the resident twin"
    );
    // Scalar oracle: BTreeSet dedupe over the row law.
    let mut m: std::collections::BTreeMap<i64, (u64, std::collections::BTreeSet<i64>)> =
        Default::default();
    for i in 0..ROWS {
        let (_, kh, _, q, _, _) = row(i);
        let g = m.entry(q).or_default();
        g.0 += 1;
        g.1.insert(kh);
    }
    let mut want: Vec<String> =
        m.iter().map(|(k, (c, s))| format!("{k}\t{c}\t{}", s.len())).collect();
    want.sort();
    assert_eq!(lines, want, "distinct pair spill vs scalar oracle");
}

/// The budget law's distinct verdicts: the dense-safe distinct shape
/// ADMITS over budget (the pair plane spills — the former no-spill-arm
/// refusal flips to served); a nullable distinct input stays residue
/// (typed refusal); the kill switch restores the legacy verdict.
#[test]
fn distinct_budget_admission_flips() {
    let sp = engine(Some(1 << 16), true);
    verdict(&sp, vec![1], vec![AAgg::CountDistinct { e: AValExpr::Col(4) }])
        .expect("dense-safe distinct shape admits over budget (pair plane spills)");
    // A nullable distinct input refuses TYPED at lowering (the 3VL
    // lattice) — it never reaches the spill arm.
    match plan_from_ap(&sp.bank, &sp.faces, &ap(vec![1], vec![AAgg::CountDistinct { e: AValExpr::Col(6) }])) {
        Err(Refuse::NullableUnsupported { attno: 6, .. }) => {}
        other => panic!("nullable distinct input must refuse typed, got {other:?}"),
    }
    // A budget UNDER the dense-array floor refuses typed too — the
    // dense planes cannot spill (the §3.5 direct-array posture).
    let under = engine(Some(1 << 12), true);
    match verdict(&under, vec![1], vec![AAgg::CountDistinct { e: AValExpr::Col(4) }]) {
        Err(Refuse::GroupedSpillUnavailable { what: "no-spill-arm", .. }) => {}
        other => panic!("dense arrays over budget must refuse typed, got {other:?}"),
    }
    // Kill switch: the legacy arm never prices the distinct shape.
    let legacy = engine(Some(1 << 16), false);
    verdict(&legacy, vec![1], vec![AAgg::CountDistinct { e: AValExpr::Col(4) }])
        .expect("kill switch restores the legacy verdict");
}
