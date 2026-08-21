//! [spill-2] Byte-key grouped spill gates (spill-design.md §6 residue
//! rung 1 — the hash_group tuned text arms):
//!
//!   - EXACT-ORDER identity spill-forced vs the tuned-arm twin across the
//!     varlena-key vocabulary — text128 ([0]), int_gid ([w,0], CountDesc
//!     and KeyAsc), int_gid_filtered (empty-key drop), gid_pair ([0,0],
//!     with and without the drop) — unsorted line compare, so the tie
//!     ORDERS are pinned, not just the sets;
//!   - the scalar SQL oracle on the dict and raw lanes;
//!   - engagement counters: the forced legs must FLUSH scatter, DRAIN
//!     byte-key runs and MERGE them (no vacuous green);
//!   - determinism: the spill arm reruns byte-identical;
//!   - [cap-retire growth] the former GroupCountUnwitnessed classes on
//!     byte-key shapes — over-cap dict products and unwitnessed raw text
//!     keys — now ADMIT (spill-served) and answer the true group set;
//!     the kill switch restores the witness gate verbatim;
//!   - the finalize answer-bytes law on the byte-key arm (typed 53400
//!     runtime refusal, exact counted account);
//!   - nullable text keys stay OUTSIDE the arm (typed refusal, never a
//!     null-blind answer).

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
use sqe::ir::OrderBy;
use sqe::planner::{check_server_grouped, plan_from_ap, AAgg, AFamily, AKeyExpr, APlan, APred, AValExpr};
use sqe::refuse::Refuse;
use sqe::render::to_lines;
use sqe::rig::lower::{SelExpr, SqlAgg, SqlExpr, SqlQuery};
use sqe::rig::oracle::run_oracle;
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

/// The row law. Columns:
///   1 v  int8  key in [0, 50)                      (witnessed int lane)
///   2 t  text  DICT, ~30k distinct, ~1% empty      (high-NDV text key)
///   3 u  text  DICT, 100 distinct, ~4% empty       (low-NDV text key)
///   4 s  text  DICT, 40 distinct                   (pair partner)
///   5 r  text  RAW (no dict), 5k distinct          (unwitnessed key)
fn row(i: u64) -> (i64, String, String, String, String) {
    let v = ((i * 7919) % 50) as i64;
    let tj = (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 13) % 30_000;
    let t = if tj % 100 == 0 { String::new() } else { format!("t_{tj:05}") };
    let uk = (i.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 7) % 100;
    let u = if uk % 25 == 0 { String::new() } else { format!("u_{uk:03}") };
    let s = format!("s_{:02}", (i >> 3) % 40);
    let r = format!("r_{:04}", (i.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 9) % 5_000);
    (v, t, u, s, r)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let dict = |ndv_cap: u64| ColumnPosture {
        dict: Some(DictPolicy { ndv_cap, exec_ok: true, sem: TextSemantics::Utf8Chars }),
        ..Default::default()
    };
    let cands = CodecCandidates::new(ColumnPosture::default())
        .with_column(2, 0, dict(1 << 16))
        .with_column(3, 0, dict(1 << 12))
        .with_column(4, 0, dict(1 << 12));
    let resolver = pgrc2_write::seal::CodecResolver;
    let schema = vec![wcol(1, 8, 8), tcol(2), tcol(3), tcol(4), tcol(5)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        7,
        797,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (v, t, u, s, r) = row(i);
        let (ti, ui, si, ri) = (
            img_4b_u(t.as_bytes()),
            img_4b_u(u.as_bytes()),
            img_4b_u(s.as_bytes()),
            img_4b_u(r.as_bytes()),
        );
        let datums = [
            RawDatum::Word(v as u64),
            RawDatum::Bytes(&ti),
            RawDatum::Bytes(&ui),
            RawDatum::Bytes(&si),
            RawDatum::Bytes(&ri),
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

fn schema_meta() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "v", TypMeta::INT8),
        ColMeta::new(2, "t", TypMeta::TEXT_C),
        ColMeta::new(3, "u", TypMeta::TEXT_C),
        ColMeta::new(4, "s", TypMeta::TEXT_C),
        ColMeta::new(5, "r", TypMeta::TEXT_C),
    ]
}

fn bank_dir() -> String {
    let dir = std::env::temp_dir().join(format!("sqe_bspillid_{}", std::process::id()));
    let d = dir.to_str().unwrap().to_string();
    static SEALED: std::sync::Once = std::sync::Once::new();
    SEALED.call_once(|| {
        let _ = std::fs::remove_dir_all(&d);
        seal_bank(&d);
    });
    d
}

/// `budget`: `None` = the E18 default law (never crossed at this bank —
/// the tuned-arm twin); `Some(bytes)` = the forced-low spill arm.
fn engine(budget: Option<u64>, spill: bool) -> Engine {
    sqe::spill::register_std_store();
    let bank = Bank::open(&bank_dir(), schema_meta(), &OpenOpts { bankstats: false, threads: 0 });
    Engine::new(
        bank,
        SqeConfig { threads: 3, spill, grouped_budget_override: budget, ..SqeConfig::default() },
    )
}

fn ap(group: Vec<u32>, pred: Option<APred>) -> APlan {
    APlan {
        sortagg_keys: Vec::new(),
        win: None,
        q: 0,
        family: AFamily::HashPlaneOwnedGroup,
        tags: Vec::new(),
        cols: group.clone(),
        pred,
        group: group.into_iter().map(AKeyExpr::Col).collect(),
        agg: vec![AAgg::CountStar],
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

/// `bound`: None = full set (order None); Some((order, limit, offset)).
fn run_lines(
    eng: &Engine,
    group: Vec<u32>,
    pred: Option<APred>,
    bound: Option<(OrderBy, usize, usize)>,
) -> Vec<String> {
    let mut node = plan_from_ap(&eng.bank, &eng.faces, &ap(group, pred)).expect("lower");
    if let Some((order, limit, offset)) = bound {
        node.params.order = order;
        node.params.limit = limit;
        node.params.offset = offset;
    }
    to_lines(&eng.run(&node))
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

// ---------------------------------------------------------------------------
// identity gates: byte-key spill-forced vs the tuned-arm twin
// ---------------------------------------------------------------------------

/// EXACT-ORDER identity across the tuned text vocabulary. The twin (E18
/// default budget, witnessed-under-cap shapes) rides text128 / int_gid /
/// int_gid_filtered / gid_pair; the forced-low engines ride the byte-key
/// spill arm. Lines compare UNSORTED — the per-shape tie orders are the
/// gate.
#[test]
fn byte_spill_vs_tuned_exact_order_identity() {
    let twin = engine(None, true);
    for budget in [1 << 12, 1 << 16, 1 << 20] {
        let sp = engine(Some(budget), true);
        type Case = (Vec<u32>, Option<APred>, Option<(OrderBy, usize, usize)>, &'static str);
        let ne = |c: u32| Some(APred::NeEmpty { col: c, fp: None });
        let cases: Vec<Case> = vec![
            // text128: (count DESC, bytes ASC), full set + topk + offset
            // (col 3 = the dict text key: witnessed under cap, so the
            // twin rides the TUNED text128 — the meta-gate below proves
            // it; col 2's high-NDV non-dict twin rides the byte arm on
            // both sides and gates the run-merge vs resident identity)
            (vec![3], None, None, "text128-full"),
            (vec![3], None, Some((OrderBy::CountDesc, 10, 0)), "text128-topk"),
            (vec![3], None, Some((OrderBy::CountDesc, 7, 3)), "text128-offset"),
            (vec![2], None, None, "text128-highndv-runs-vs-resident"),
            (vec![2], None, Some((OrderBy::CountDesc, 10, 0)), "text128-highndv-topk"),
            // int_gid: (count DESC, v ASC, gid-order ASC) and KeyAsc
            (vec![1, 3], None, None, "int-gid-full"),
            (vec![1, 3], None, Some((OrderBy::KeyAsc, usize::MAX, 0)), "int-gid-keyasc"),
            (vec![1, 3], None, Some((OrderBy::CountDesc, 9, 0)), "int-gid-topk"),
            // int_gid_filtered: the empty-key drop lane
            (vec![1, 3], ne(3), None, "int-gid-filtered"),
            (vec![1, 3], ne(3), Some((OrderBy::CountDesc, 9, 2)), "int-gid-filtered-topk"),
            // gid_pair: (count DESC, bytes0 ASC, bytes1 ASC) + the drop
            (vec![3, 4], None, None, "gid-pair-full"),
            (vec![3, 4], ne(3), None, "gid-pair-dropped"),
            (vec![3, 4], None, Some((OrderBy::CountDesc, 11, 0)), "gid-pair-topk"),
        ];
        for (g, pred, bound, name) in cases {
            let a = run_lines(&sp, g.clone(), pred.clone(), bound);
            let b = run_lines(&twin, g.clone(), pred.clone(), bound);
            assert_eq!(a, b, "byte-spill exact-order identity: budget={budget} {name}");
        }
    }
}

/// The dict + raw lanes vs the scalar SQL oracle (independent code path),
/// spill forced.
#[test]
fn byte_spill_vs_oracle() {
    let sp = engine(Some(1 << 12), true);
    let oracle = |keys: &[u32]| -> Vec<String> {
        let mut sel: Vec<(SelExpr, Option<String>)> =
            keys.iter().map(|&c| (SelExpr::Expr(SqlExpr::Col(c)), None)).collect();
        sel.push((SelExpr::Agg(SqlAgg::CountStar), None));
        let ast = SqlQuery {
            select: sel,
            preds: Vec::new(),
            group: keys.iter().map(|&c| SqlExpr::Col(c)).collect(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        sorted(to_lines(&run_oracle(&sp.bank, &ast)))
    };
    for keys in [vec![2u32], vec![1, 3], vec![3, 4], vec![5]] {
        let got = sorted(run_lines(&sp, keys.clone(), None, None));
        assert_eq!(got, oracle(&keys), "byte-spill vs oracle: keys={keys:?}");
    }
}

/// The forced legs must ENGAGE the spill machinery — scatter flushes,
/// byte-key run drains, k-way merges — never a vacuous green.
#[test]
fn byte_spill_engagement_counters() {
    use sqe::stencils::hash_group::{BSPILL_DRAINS, BSPILL_FLUSHES, BSPILL_MERGES};
    use std::sync::atomic::Ordering::Relaxed;
    let sp = engine(Some(1 << 12), true);
    let (f0, d0, m0) =
        (BSPILL_FLUSHES.load(Relaxed), BSPILL_DRAINS.load(Relaxed), BSPILL_MERGES.load(Relaxed));
    let _ = run_lines(&sp, vec![2], None, None);
    assert!(BSPILL_FLUSHES.load(Relaxed) > f0, "pass-1 byte scatter spill engaged");
    assert!(BSPILL_DRAINS.load(Relaxed) > d0, "pass-2 byte-key run drain engaged");
    assert!(BSPILL_MERGES.load(Relaxed) > m0, "pass-2 byte-key run merge engaged");
}

/// The byte-key spill arm is deterministic: rerunning the same forced-low
/// shape answers byte-identically (parked/file state included).
#[test]
fn byte_spill_rerun_determinism() {
    let sp = engine(Some(1 << 12), true);
    let a = run_lines(&sp, vec![2], None, None);
    let b = run_lines(&sp, vec![2], None, None);
    assert_eq!(a, b, "byte-spill rerun determinism (full)");
    let c = run_lines(&sp, vec![1, 3], None, Some((OrderBy::CountDesc, 9, 0)));
    let d = run_lines(&sp, vec![1, 3], None, Some((OrderBy::CountDesc, 9, 0)));
    assert_eq!(c, d, "byte-spill rerun determinism (top-k)");
}

// ---------------------------------------------------------------------------
// [cap-retire growth] byte-key shapes flip refusal -> served
// ---------------------------------------------------------------------------

fn verdict(eng: &Engine, group: Vec<u32>) -> Result<(), Refuse> {
    let node = plan_from_ap(&eng.bank, &eng.faces, &ap(group, None)).expect("lower");
    check_server_grouped(&eng.bank, &eng.faces, &node)
}

/// The former GroupCountUnwitnessed classes on byte-key shapes:
/// `(v, t)` — a witnessed bound over 2^20 (dict-entry sum x int domain)
/// — and `r` — an unwitnessed raw text key — now ADMIT (the byte-key
/// spill arm bounds their state) and answer the TRUE group set against
/// the oracle. The kill switch restores the witness gate verbatim.
#[test]
fn cap_retire_byte_key_flips() {
    let dflt = engine(None, true);
    verdict(&dflt, vec![1, 2]).expect("over-cap (v,t) dict product must admit (spill-served)");
    verdict(&dflt, vec![5]).expect("unwitnessed raw text key must admit (spill-served)");
    // True-group-set identity, both budget arms, vs the oracle.
    let sp = engine(Some(1 << 12), true);
    for keys in [vec![1u32, 2], vec![5]] {
        let a = sorted(run_lines(&dflt, keys.clone(), None, None));
        let b = sorted(run_lines(&sp, keys.clone(), None, None));
        assert_eq!(a, b, "cap-retire byte-key identity across budgets: {keys:?}");
        let mut sel: Vec<(SelExpr, Option<String>)> =
            keys.iter().map(|&c| (SelExpr::Expr(SqlExpr::Col(c)), None)).collect();
        sel.push((SelExpr::Agg(SqlAgg::CountStar), None));
        let ast = SqlQuery {
            select: sel,
            preds: Vec::new(),
            group: keys.iter().map(|&c| SqlExpr::Col(c)).collect(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        let want = sorted(to_lines(&run_oracle(&dflt.bank, &ast)));
        assert_eq!(a, want, "cap-retire byte-key true group set: {keys:?}");
    }
    // Kill switch: the legacy witness gate verbatim.
    let legacy = engine(None, false);
    // (the exact `what` depends on whether every part published a dict
    // for t — either way the WITNESS GATE is the restored verdict)
    match verdict(&legacy, vec![1, 2]) {
        Err(Refuse::GroupCountUnwitnessed { .. }) => {}
        other => panic!("kill switch must restore the witness gate, got {other:?}"),
    }
    match verdict(&legacy, vec![5]) {
        Err(Refuse::GroupCountUnwitnessed { what: "no-witness" }) => {}
        other => panic!("kill switch must restore no-witness, got {other:?}"),
    }
}

/// The finalize answer-bytes law on the byte-key arm: a tiny answer
/// budget refuses TYPED at finalize with the counted account carried;
/// a bounded (top-k) answer under the same budget still serves.
#[test]
fn byte_spill_answer_bytes_law() {
    sqe::spill::register_std_store();
    let bank = Bank::open(&bank_dir(), schema_meta(), &OpenOpts { bankstats: false, threads: 0 });
    let tiny = Engine::new(
        bank,
        SqeConfig {
            threads: 3,
            spill: true,
            grouped_budget_override: Some(1 << 12),
            answer_budget_override: Some(4096),
            ..SqeConfig::default()
        },
    );
    let node = plan_from_ap(&tiny.bank, &tiny.faces, &ap(vec![2], None)).expect("lower");
    // unwind-ok: asserting the typed runtime-refusal payload itself
    let p = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tiny.run(&node)))
        .expect_err("tiny answer budget must refuse at finalize");
    let rr = p.downcast::<sqe::refuse::RunRefusal>().expect("typed RunRefusal payload");
    match rr.0 {
        Refuse::GroupAnswerOverBudget { got, budget } => {
            assert_eq!(budget, 4096, "the override is the effective budget");
            assert!(got > budget, "counted account over budget (got={got})");
        }
        other => panic!("expected GroupAnswerOverBudget, got {other:?}"),
    }
    // A k-bounded answer under the SAME budgets serves (O(k) plane).
    let mut node2 = plan_from_ap(&tiny.bank, &tiny.faces, &ap(vec![2], None)).expect("lower");
    node2.params.order = OrderBy::CountDesc;
    node2.params.limit = 10;
    let a = tiny.run(&node2);
    assert_eq!(a.nrows, 10, "bounded answer serves under the tiny answer budget");
}

/// Nullable varlena keys stay OUTSIDE the byte-key arm: the shape must
/// refuse typed somewhere on the admission path (never a null-blind
/// answer through the spill arm).
#[test]
fn nullable_text_key_stays_residue() {
    // A nullable RAW text column: seal a tiny twin bank with nulls.
    let dir = std::env::temp_dir().join(format!("sqe_bspillnull_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    {
        std::fs::create_dir_all(&dir).expect("mkdir");
        let mut vfs = RealVfs;
        let mut kit = Kit::new();
        let cands = CodecCandidates::new(ColumnPosture::default());
        let resolver = pgrc2_write::seal::CodecResolver;
        let schema = vec![wcol(1, 8, 8), tcol(2)];
        let mut w = TableWriter::open(
            dir.clone(),
            schema,
            1663,
            8,
            799,
            TxnStamp { fxid: 100, cid: 1 },
            &Default::default(),
            PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
        )
        .expect("open writer");
        for i in 0..20_000u64 {
            let s = format!("n_{:03}", i % 500);
            let img = img_4b_u(s.as_bytes());
            let datums = [
                RawDatum::Word(i % 50),
                if i % 7 == 0 { RawDatum::Null } else { RawDatum::Bytes(&img) },
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
    sqe::spill::register_std_store();
    let bank = Bank::open(
        &dir,
        vec![ColMeta::new(1, "v", TypMeta::INT8), ColMeta::new(2, "t", TypMeta::TEXT_C)],
        &OpenOpts { bankstats: false, threads: 0 },
    );
    let eng = Engine::new(
        bank,
        SqeConfig {
            threads: 3,
            spill: true,
            grouped_budget_override: Some(1 << 12),
            ..SqeConfig::default()
        },
    );
    // Whatever step refuses (lowering's nullable lattice or the grouped
    // admission), the shape must NOT serve through the byte-key arm.
    let served = plan_from_ap(&eng.bank, &eng.faces, &ap(vec![2], None))
        .and_then(|node| check_server_grouped(&eng.bank, &eng.faces, &node).map(|()| node));
    assert!(
        served.is_err(),
        "nullable text key must refuse typed, got a served plan: {served:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// [q1-dictgroup] The dict-witnessed Bounded arm (spill-design.md §3.6):
/// a NULLABLE single varlena key that is dict-backed in EVERY part
/// ADMITS over the row-priced E18 budget when the witnessed dict-entry
/// sum prices the text128 dict-arm plane under the budget (and under the
/// emit cap) — the jsonbench q1 100M shape. A budget under even the
/// entry-priced plane keeps the typed refusal.
#[test]
fn nullable_dict_key_bounded_admits() {
    let dir = std::env::temp_dir().join(format!("sqe_bspilldictn_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    {
        std::fs::create_dir_all(&dir).expect("mkdir");
        let mut vfs = RealVfs;
        let mut kit = Kit::new();
        let cands = CodecCandidates::new(ColumnPosture::default()).with_column(
            2,
            0,
            ColumnPosture {
                dict: Some(DictPolicy {
                    ndv_cap: 1 << 12,
                    exec_ok: true,
                    sem: TextSemantics::Utf8Chars,
                }),
                ..Default::default()
            },
        );
        let resolver = pgrc2_write::seal::CodecResolver;
        let schema = vec![wcol(1, 8, 8), tcol(2)];
        let mut w = TableWriter::open(
            dir.clone(),
            schema,
            1663,
            8,
            801,
            TxnStamp { fxid: 100, cid: 1 },
            &Default::default(),
            PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
        )
        .expect("open writer");
        for i in 0..20_000u64 {
            let s = format!("d_{:03}", i % 500);
            let img = img_4b_u(s.as_bytes());
            let datums = [
                RawDatum::Word(i % 50),
                if i % 7 == 0 { RawDatum::Null } else { RawDatum::Bytes(&img) },
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
    sqe::spill::register_std_store();
    let mk = |budget: u64| {
        let bank = Bank::open(
            &dir,
            vec![ColMeta::new(1, "v", TypMeta::INT8), ColMeta::new(2, "t", TypMeta::TEXT_C)],
            &OpenOpts { bankstats: false, threads: 0 },
        );
        Engine::new(
            bank,
            SqeConfig {
                threads: 3,
                spill: true,
                grouped_budget_override: Some(budget),
                ..SqeConfig::default()
            },
        )
    };
    // rows × SCATTER_ROW_BYTES = 20_000 × 28 = 560_000 > 400_000 (the
    // row price refuses); entries ≈ 5 parts × 500 codes + 1, priced
    // ~320KB under 400_000 → the dict witness admits.
    let eng = mk(400_000);
    let node = plan_from_ap(&eng.bank, &eng.faces, &ap(vec![2], None)).expect("lower");
    check_server_grouped(&eng.bank, &eng.faces, &node)
        .expect("dict-witnessed nullable text key must ADMIT over the row-priced budget");
    // A budget under the ENTRY-priced plane keeps the typed refusal.
    let tiny = mk(1 << 12);
    let node = plan_from_ap(&tiny.bank, &tiny.faces, &ap(vec![2], None)).expect("lower");
    match check_server_grouped(&tiny.bank, &tiny.faces, &node) {
        Err(Refuse::GroupedSpillUnavailable { what: "no-spill-arm", .. }) => {}
        other => panic!("expected no-spill-arm under the entry-priced budget, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Meta-gate: the exact-order identity above is only meaningful if the
/// DEFAULT-budget twin actually rides the TUNED arms — i.e. the tuned
/// legs' shapes are witnessed under the emit cap (so `bytes_spill_engaged`
/// is false on the twin) and the byte counters do NOT move on a twin run.
#[test]
fn twin_rides_tuned_arms_not_vacuous() {
    use sqe::stencils::hash_plane::bytes_spill_engaged;
    let twin = engine(None, true);
    let legacy = engine(None, false);
    // The dict text key must actually be dict-backed for the witness
    // (the 30k-NDV col 2 deliberately is NOT — the writer refuses an
    // uncompressive dict — and rides the byte arm on both sides).
    let dicts =
        (0..legacy.bank.parts.len()).filter(|&pi| sqe::scan::is_dict(&legacy.bank, pi, 3)).count();
    assert_eq!(dicts, legacy.bank.parts.len(), "col 3 must be dict in every part");
    for keys in [vec![3u32], vec![1, 3], vec![3, 4]] {
        verdict(&legacy, keys.clone())
            .unwrap_or_else(|r| panic!("tuned leg {keys:?} must be witnessed (got {r:?})"));
        // The dispatch authority itself: the twin must NOT elect the
        // byte-key spill arm on the tuned legs...
        let node = plan_from_ap(&twin.bank, &twin.faces, &ap(keys.clone(), None)).expect("lower");
        assert!(
            !bytes_spill_engaged(&twin.bank, &twin.faces, &node),
            "twin must ride the tuned arm for {keys:?}"
        );
        // ...and the forced-low engine MUST.
        let sp = engine(Some(1 << 12), true);
        let node = plan_from_ap(&sp.bank, &sp.faces, &ap(keys.clone(), None)).expect("lower");
        assert!(
            bytes_spill_engaged(&sp.bank, &sp.faces, &node),
            "forced budget must elect the byte-key spill arm for {keys:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// [spill-2] the two-level merge-join route's slot-scatter spill
// ---------------------------------------------------------------------------

/// The predicate merge-join route (varlena key + int frame terms) under
/// forced-low budgets: exact-order identity vs the resident twin, the
/// scalar row-law oracle, engagement counters, and the family admission
/// law (over budget serves with a substrate; never a lost shape).
#[test]
fn two_level_slot_spill_identity() {
    use sqe::planner::AFamily;
    use sqe::stencils::two_level::{TLSPILL_FLUSHES, TLSPILL_FOLDS};
    use std::sync::atomic::Ordering::Relaxed;
    let mk_node = |eng: &Engine| {
        let mut p = ap(vec![3], None);
        p.family = AFamily::TwoLevelCodeAgg;
        p.pred = Some(APred::Range {
            col: 1,
            lo: "10".into(),
            hi: "39".into(),
            fp: None,
        });
        if !p.cols.contains(&1) {
            p.cols.push(1);
        }
        plan_from_ap(&eng.bank, &eng.faces, &p).expect("lower two_level")
    };
    let lines_of = |eng: &Engine| -> Vec<String> {
        let a = eng.run(&mk_node(eng));
        let mut l = to_lines(&a);
        if a.note.is_some() {
            l.pop(); // the groups/rows footer (identical across arms,
                     // excluded only from the oracle compare below)
        }
        l
    };
    let twin = engine(None, true);
    let base = lines_of(&twin);
    let (f0, d0) = (TLSPILL_FLUSHES.load(Relaxed), TLSPILL_FOLDS.load(Relaxed));
    for budget in [1 << 12, 1 << 16] {
        let sp = engine(Some(budget), true);
        assert_eq!(lines_of(&sp), base, "two-level slot-spill identity: budget={budget}");
    }
    assert!(TLSPILL_FLUSHES.load(Relaxed) > f0, "slot scatter spill engaged");
    assert!(TLSPILL_FOLDS.load(Relaxed) > d0, "slot chunk fold engaged");
    // Scalar oracle over the row law (10 <= v <= 39, count by u).
    let mut m: std::collections::BTreeMap<String, u64> = Default::default();
    for i in 0..ROWS {
        let (v, _, u, _, _) = row(i);
        if (10..=39).contains(&v) {
            *m.entry(u).or_insert(0) += 1;
        }
    }
    let mut got = base.clone();
    got.sort();
    let mut want: Vec<String> = m.iter().map(|(k, c)| format!("{k}\t{c}")).collect();
    want.sort();
    assert_eq!(got, want, "two-level slot spill vs scalar oracle");
}

/// Directional spill-forced overhead probe (reporting only, debug-build
/// wall clock — run with --ignored --nocapture).
#[test]
#[ignore]
fn byte_spill_overhead_directional() {
    let twin = engine(None, true);
    let sp = engine(Some(1 << 16), true);
    for (keys, name) in [(vec![2u32], "text-highndv"), (vec![1, 3], "int-gid"), (vec![3, 4], "gid-pair")] {
        // warm
        let _ = run_lines(&twin, keys.clone(), None, None);
        let _ = run_lines(&sp, keys.clone(), None, None);
        let t0 = std::time::Instant::now();
        for _ in 0..3 {
            let _ = run_lines(&twin, keys.clone(), None, None);
        }
        let tw = t0.elapsed().as_secs_f64() / 3.0;
        let t1 = std::time::Instant::now();
        for _ in 0..3 {
            let _ = run_lines(&sp, keys.clone(), None, None);
        }
        let sf = t1.elapsed().as_secs_f64() / 3.0;
        println!("SPILLOVH|{name}|resident_ms={:.1}|forced_ms={:.1}|x={:.2}", tw * 1e3, sf * 1e3, sf / tw);
    }
}

/// [spill-2] The distinct FAMILY's E18 admission law: its arms are
/// row-scaled with no spill rung, so over budget the family refuses
/// typed at admission; the default budget and the kill switch admit.
#[test]
fn distinct_family_budget_law() {
    let mk = |eng: &Engine| {
        let mut p = ap(vec![], None);
        p.family = AFamily::DistinctPipeline;
        p.agg = vec![AAgg::CountDistinct { e: AValExpr::Col(1) }];
        plan_from_ap(&eng.bank, &eng.faces, &p).expect("lower distinct")
    };
    let dflt = engine(None, true);
    check_server_grouped(&dflt.bank, &dflt.faces, &mk(&dflt))
        .expect("under the default budget nothing changes");
    let tiny = engine(Some(1 << 16), true);
    match check_server_grouped(&tiny.bank, &tiny.faces, &mk(&tiny)) {
        Err(Refuse::GroupedSpillUnavailable { what: "no-spill-arm", .. }) => {}
        other => panic!("row-scaled distinct arm over budget must refuse typed, got {other:?}"),
    }
    let legacy = engine(Some(1 << 16), false);
    check_server_grouped(&legacy.bank, &legacy.faces, &mk(&legacy))
        .expect("kill switch: the legacy arm, verbatim");
}

// ---------------------------------------------------------------------------
// [q3334 textgroup] the fp128-first spilled text128 arm (k-bounded [0])
// ---------------------------------------------------------------------------

/// The q33/q34 class at bank grain: a k-bounded single-text group-by
/// whose E18 scatter price crosses the budget. The forced engine rides
/// `text128_spill` (28 B fp128 records + lazy byte resolve); the twin
/// rides the tuned arms. EXACT-ORDER identity + engagement + the kill
/// switch (which restores the byte_spill route, itself identity-gated
/// above). `l2_bytes` shrinks so the partition law spreads this small
/// bank far enough for the arm's pass-2 residency floor
/// (`ceil(rows/p) × 184 ≤ share`): at 60k rows, l2 = 32 KiB, ndv ~30k
/// → p = 64, 938 × 184 = 173 KB ≤ share = (1 MiB)/3 = 349 KB.
#[test]
fn text128_spill_arm_identity_and_engagement() {
    use sqe::stencils::hash_group::{TSPILL_ENGAGED, TSPILL_FLUSHES};
    use std::sync::atomic::Ordering::Relaxed;
    let run = |eng: &Engine, g: Vec<u32>, bound: (OrderBy, usize, usize)| -> Vec<String> {
        let mut node = plan_from_ap(&eng.bank, &eng.faces, &ap(g, None)).expect("lower");
        node.params.l2_bytes = 32 * 1024;
        (node.params.order, node.params.limit, node.params.offset) = bound;
        to_lines(&eng.run(&node))
    };
    let twin = engine(None, true);
    let sp = engine(Some(1 << 20), true);
    let f0 = TSPILL_FLUSHES.load(Relaxed);
    // engagement cases: high-NDV dict (30k) and raw (5k) text keys
    for (g, bound, name) in [
        (vec![2u32], (OrderBy::CountDesc, 10, 0), "highndv-topk"),
        (vec![2], (OrderBy::CountDesc, 7, 3), "highndv-offset"),
        (vec![5], (OrderBy::CountDesc, 10, 0), "raw-topk"),
    ] {
        let e0 = TSPILL_ENGAGED.load(Relaxed);
        let a = run(&sp, g.clone(), bound);
        assert!(TSPILL_ENGAGED.load(Relaxed) > e0, "arm must engage: {name}");
        let b = run(&twin, g, bound);
        assert_eq!(a, b, "text128_spill exact-order identity: {name}");
    }
    assert!(TSPILL_FLUSHES.load(Relaxed) > f0, "pass-1 fp128 scatter spill engaged");
    // determinism: rerun byte-identical (parked/file state included)
    let a = run(&sp, vec![2], (OrderBy::CountDesc, 10, 0));
    let b = run(&sp, vec![2], (OrderBy::CountDesc, 10, 0));
    assert_eq!(a, b, "text128_spill rerun determinism");
    // low-NDV dict key (p at the width floor): the residency floor
    // declines, the byte arm serves — identity holds either way
    let c = run(&sp, vec![3], (OrderBy::CountDesc, 10, 0));
    assert_eq!(c, run(&twin, vec![3], (OrderBy::CountDesc, 10, 0)), "floor-fallback identity");
    // kill switch: PGRUST_SQE_TEXT128_SPILL=0 restores byte_spill (read
    // per statement; set/unset inside the one test that asserts on it)
    std::env::set_var("PGRUST_SQE_TEXT128_SPILL", "0");
    let e1 = TSPILL_ENGAGED.load(Relaxed);
    let k = run(&sp, vec![2], (OrderBy::CountDesc, 10, 0));
    std::env::remove_var("PGRUST_SQE_TEXT128_SPILL");
    assert_eq!(TSPILL_ENGAGED.load(Relaxed), e1, "kill switch: the arm stays silent");
    assert_eq!(k, run(&twin, vec![2], (OrderBy::CountDesc, 10, 0)), "kill-switch twin identity");
}
