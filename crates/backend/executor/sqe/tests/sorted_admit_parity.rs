//! [sorted-arm] Admission parity across the ClickBench SORTED-CUT layout
//! (docs/design/sqe/sorted-arm-readiness.md — the CB_SORT=1 resubmission
//! readiness probe).
//!
//! The ONE admission witness family whose value/availability is
//! permutation-VARIANT is the per-part dict plane: `Witness::key_count`
//! on a varlena key is `Some(Σ_p ncodes + 1)` only when EVERY part
//! elected a dict, and the writer's `DictPolicy.ndv_cap` is a per-part
//! NDV gate — so re-clustering the load (the sorted cut) can flip dict
//! election in BOTH directions (the jsonbench lane-dict lesson). This
//! probe seals twin banks through the REAL writer (the
//! `distinct_fulldomain.rs` idiom) where the text column's dict election
//! GENUINELY flips (asserted — no vacuous parity), then proves the
//! ClickBench-shaped grouped admissions verdict IDENTICALLY on both:
//!
//!   - text-key COUNT(*) with the pushed native top-k (every text-keyed
//!     CB query carries ORDER BY .. LIMIT 10): k_bounded admits, the
//!     dict witness never consulted;
//!   - text-key COUNT(*) over the E18 budget (the 100m posture): the
//!     byte-key spill arm (`SpillableBytes` — SHAPE facts only) serves
//!     both twins;
//!   - (int,text)-key COUNT(*) with NO pushed bound (the q17 shape):
//!     witnessed-over-cap on one twin, unwitnessed on the other — BOTH
//!     serve through the same shape-fact spill route;
//!   - grouped COUNT(DISTINCT int8) under a text key with top-k (the
//!     q13 shape): admits on both;
//!   - the ONE genuinely layout-flippable class — TWO text keys + a
//!     word-fold agg mix, NO pushed bound (no ClickBench query is in
//!     it) — is DEMONSTRATED to flip (admit on the clustered twin,
//!     `group-count-unwitnessed` on the de-clustered twin);
//!   - and the q28 genus (single text key, mixed aggs, no bound)
//!     refuses on BOTH layouts — no serve flip — with only the typed
//!     CAUSE drifting (shape gate vs witness gate), pinned so a
//!     cause-keyed triage never misreads the drift as a serving change.

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::dict::TextSemantics;
use pgrc2_write::elect::{CodecCandidates, ColumnPosture, DictPolicy};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::{TopK, TopKKey};
use sqe::planner::{
    check_server_grouped, plan_from_ap, AAgg, AFamily, AKeyExpr, APlan, AValExpr,
};
use sqe::refuse::Refuse;
use sqe::typmeta::TypMeta;
use sqe::witness::Witness;

const ROWS: u64 = 40_000;
const PART_ROWS: u64 = 4096;
/// Per-part dict NDV cap: clustered runs (~105 distinct/part) elect a
/// dict on every part; the counter-sorted shuffle (~990 distinct/part)
/// drops it — the witness-availability flip under test.
const NDV_CAP: u64 = 128;
/// Text runs of 40 rows → 1000 distinct phrases total.
const RUN: u64 = 40;

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

/// Row i: `counter` = full-range signed hash (the CounterID stand-in the
/// sorted cut clusters by; also the "UserID-class" huge exact domain),
/// `phrase` = run-clustered text in INSERTION order (de-clustered by the
/// counter sort), `user` = full-range signed hash (distinct input).
fn row(i: u64) -> (i64, String, i64, String) {
    let counter = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) as i64;
    let phrase = format!("ph{:04}", i / RUN);
    let user = i.wrapping_mul(0xC2B2_AE3D_27D4_EB4F) as i64;
    let word = format!("qz{:04}", i / RUN);
    (counter, phrase, user, word)
}

fn seal_bank(dir: &str, sorted: bool) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let dictp = || ColumnPosture {
        dict: Some(DictPolicy {
            ndv_cap: NDV_CAP,
            exec_ok: true,
            sem: TextSemantics::Utf8Chars,
        }),
        ..Default::default()
    };
    let cands = CodecCandidates::new(ColumnPosture::default())
        .with_column(2, 0, dictp())
        .with_column(4, 0, dictp());
    let resolver = pgrc2_write::seal::CodecResolver;
    let mut w = TableWriter::open(
        dir.to_string(),
        vec![wcol8(1), tcol(2), wcol8(3), tcol(4)],
        1663,
        9,
        907,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy {
            max_rows: PART_ROWS,
            max_bytes: u64::MAX,
            cut_granule_rows: 1024,
        },
    )
    .expect("open writer");
    let mut rows: Vec<(i64, String, i64, String)> = (0..ROWS).map(row).collect();
    if sorted {
        // The sorted-cut law: cluster by the leading sort key. (The
        // real cut's tiebreakers don't matter here — the flip under
        // test is the leading-key shuffle of the text column.)
        rows.sort_by_key(|r| r.0);
    }
    for (counter, phrase, user, word) in rows {
        let img = img_4b_u(phrase.as_bytes());
        let img2 = img_4b_u(word.as_bytes());
        let datums = [
            RawDatum::Word(counter as u64),
            RawDatum::Bytes(&img),
            RawDatum::Word(user as u64),
            RawDatum::Bytes(&img2),
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
        vec![
            ColMeta::new(1, "counter", TypMeta::INT8),
            ColMeta::new(2, "phrase", TypMeta::TEXT_C),
            ColMeta::new(3, "user", TypMeta::INT8),
            ColMeta::new(4, "word", TypMeta::TEXT_C),
        ],
        &OpenOpts { bankstats: false, threads: 0 },
    );
    Engine::new(
        bank,
        SqeConfig { threads: 3, spill: true, grouped_budget_override: budget, ..SqeConfig::default() },
    )
}

fn ap(family: AFamily, group: Vec<u32>, agg: Vec<AAgg>) -> APlan {
    APlan {
        sortagg_keys: Vec::new(),
        win: None,
        q: 0,
        family,
        tags: Vec::new(),
        cols: group.clone(),
        pred: None,
        group: group.into_iter().map(AKeyExpr::Col).collect(),
        agg,
        agg_filters: Vec::new(),
        order: None,
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "sorted-admit-parity".into(),
        flags: Vec::new(),
    }
}

/// Lower + optionally push the seam's native (count DESC, k) top-k, then
/// run the grouped admission gate — the exact server verdict surface.
fn verdict(
    eng: &Engine,
    family: AFamily,
    group: Vec<u32>,
    agg: Vec<AAgg>,
    topk_count_col: Option<u32>,
) -> Result<(), Refuse> {
    let ngroup = group.len() as u32;
    let mut node =
        plan_from_ap(&eng.bank, &eng.faces, &ap(family, group, agg)).expect("lower");
    if let Some(c) = topk_count_col {
        node.params.topk = Some(TopK {
            keys: vec![TopKKey {
                col: ngroup + c,
                desc: true,
                nulls_first: false,
                lo: None,
                trim: false,
            }],
            n: 10,
            native: true,
        });
    }
    check_server_grouped(&eng.bank, &eng.faces, &node)
}

fn bank_dir(sorted: bool) -> String {
    let dir = std::env::temp_dir().join(format!(
        "sqe_sortparity_{}_{}",
        if sorted { "s" } else { "u" },
        std::process::id()
    ));
    let d = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&d);
    d
}

#[test]
fn sorted_cut_admission_parity() {
    let ud = bank_dir(false);
    let sd = bank_dir(true);
    seal_bank(&ud, false);
    seal_bank(&sd, true);

    // ---- teeth: the layout flip is REAL --------------------------------
    // Insertion order clusters the phrase runs under the per-part NDV cap
    // (every part dict-backed → witnessed key count); the counter sort
    // de-clusters them over the cap (some part loses its dict → the
    // witness is GONE). Without this, every parity below is vacuous.
    let ue = engine(&ud, None);
    let se = engine(&sd, None);
    let uw = Witness::key_count(&ue.bank, &ue.faces, 2);
    let sw = Witness::key_count(&se.bank, &se.faces, 2);
    assert!(
        uw.is_some(),
        "clustered twin must witness the text key count (dict on every part)"
    );
    assert!(
        sw.is_none(),
        "de-clustered twin must LOSE the text key-count witness (per-part \
         NDV over DictPolicy.ndv_cap drops the dict) — got {:?}",
        sw.map(|w| w.value())
    );
    // The byval witnesses the CB admissions actually consume are
    // permutation-INVARIANT (same multiset → same exact global domain).
    assert_eq!(
        Witness::exact_domain(&ue.faces.stats(&ue.bank, 1)).map(|w| w.value()),
        Witness::exact_domain(&se.faces.stats(&se.bank, 1)).map(|w| w.value()),
        "exact byval domain is a permutation invariant"
    );

    let hp = AFamily::HashPlaneOwnedGroup;
    let count = || vec![AAgg::CountStar];

    // ---- CB text-grouped posture: pushed native top-k (LIMIT 10) ------
    // (q12/q13/q14/q33-class; every text-keyed served CB query carries
    // ORDER BY .. LIMIT: k_bounded admits WITHOUT the dict witness.)
    for eng in [&ue, &se] {
        verdict(eng, hp, vec![2], count(), Some(0))
            .expect("text-key top-k COUNT must admit on both layouts");
    }

    // ---- 100m budget posture: over-E18, byte-key spill arm ------------
    // (Shape facts only — [0] key, count-only, null-free, no HAVING —
    // so the verdict cannot move with the layout.)
    let ut = engine(&ud, Some(1 << 12));
    let st = engine(&sd, Some(1 << 12));
    for eng in [&ut, &st] {
        verdict(eng, hp, vec![2], count(), Some(0))
            .expect("over-budget text COUNT serves via the byte-key spill arm on both layouts");
    }

    // ---- q17 shape: (int8, text) key, COUNT(*), NO pushed bound -------
    // Witnessed-over-cap (clustered twin: huge exact int domain × dict
    // sum) vs unwitnessed (de-clustered twin): BOTH serve through the
    // same [w,0] SpillableBytes route — cap-retire, shape facts only.
    for (eng, tag) in [(&ue, "clustered"), (&se, "de-clustered")] {
        verdict(eng, hp, vec![1, 2], count(), None)
            .unwrap_or_else(|r| panic!("q17 shape must serve on the {tag} twin, got {r:?}"));
    }

    // ---- q13 shape: text key, COUNT(DISTINCT int8), top-k -------------
    for eng in [&ue, &se] {
        verdict(
            eng,
            AFamily::DistinctPipeline,
            vec![2],
            vec![AAgg::CountDistinct { e: AValExpr::Col(3) }],
            Some(0),
        )
        .expect("grouped distinct under a text key with top-k admits on both layouts");
    }

    // ---- the ONE flippable class ([0,0] mixed — NOT in the served 42) --
    // Two text keys + a word-fold agg mix, no pushed bound: the only
    // grouped vocabulary whose admission rides the dict witness ITSELF
    // (count-only [0,0] is spill-route exempt; single-text mixed refuses
    // at the SHAPE gate on every layout). The layout flips it: witnessed
    // under-cap serves on the clustered twin, unwitnessed refuses on the
    // de-clustered one. No ClickBench query is in this class (every CB
    // two-text-key query is count-only with a pushed LIMIT).
    let mixed2 = || vec![AAgg::CountStar, AAgg::Min { e: AValExpr::Col(3) }];
    verdict(&ue, hp, vec![2, 4], mixed2(), None)
        .expect("clustered twin: dict-witnessed under-cap [0,0] mixed shape admits");
    match verdict(&se, hp, vec![2, 4], mixed2(), None) {
        Err(Refuse::GroupCountUnwitnessed { .. }) => {}
        other => panic!(
            "de-clustered twin must refuse the unwitnessed [0,0] mixed shape \
             (the flip class the sorted arm must keep out of its vocabulary), got {other:?}"
        ),
    }

    // ---- q28 genus: single text key, mixed aggs, no bound -------------
    // REFUSED on both layouts — no serve flip — but the typed CAUSE
    // drifts with the layout (witness gate fires before the shape gate
    // when the dict witness is gone). Pinned so a future cause-keyed
    // triage doesn't misread the drift as a serving change.
    let mixed1 = || vec![AAgg::CountStar, AAgg::Min { e: AValExpr::Col(3) }];
    match verdict(&ue, hp, vec![2], mixed1(), None) {
        Err(Refuse::GroupServeUnsupported { what: "grouped-text-agg" }) => {}
        other => panic!("clustered twin: shape-gate refusal expected, got {other:?}"),
    }
    match verdict(&se, hp, vec![2], mixed1(), None) {
        Err(Refuse::GroupCountUnwitnessed { .. }) => {}
        other => panic!("de-clustered twin: witness-gate refusal expected, got {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&ud);
    let _ = std::fs::remove_dir_all(&sd);
}
