//! [sqe-tpch-mech] Mechanism gates (tpch-convergence-1 §6 items 1+2):
//!
//!   - direct-array grouped state vs the hash arm — BYTE identity over
//!     real sealed banks, both arms forced via SqeConfig::direct_array
//!     (same render laws, same fold law), plus an independent scalar
//!     oracle recomputed from the row law;
//!   - generalized fused HAVING (sum/min/max/count legs, every cmp op,
//!     boundary constants, empty survivor sets) vs the oracle;
//!   - the witnessed-domain admission twins: a big-domain grouped shape
//!     refuses `bound-over-cap` bare, ADMITS having-fused under the
//!     direct-array witness, and keeps refusing when the domain exceeds
//!     the direct-array budget.

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::{HavingCmp, HvOp};
use sqe::planner::{
    check_server_grouped, direct_array_bytes_cap, plan_from_ap, AAgg, AFamily, AKeyExpr, APlan,
    AValExpr, GROUP_ROW_CAP,
};
use sqe::refuse::Refuse;
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

/// The row law. Columns:
///   1 k    int4 group key, null-free, dense witnessed domain (-50..449)
///          — signed values exercise the raw-word reconstruction;
///   2 v    int4 fold input, NULLABLE (3VL lanes; group k=-50 all-NULL);
///   3 kw   int8 group key, null-free, WIDE domain (0..~3M sparse) — the
///          over-cap admission twin (documents > GROUP_ROW_CAP);
///   4 q    int4 fold input, null-free, small positive (quantity-like);
///   5 kx   int8 group key, null-free, domain over the UNBOUNDED direct
///          budget but inside the bounded (huge-domain) band — sparse
///          occupancy (30k keys over 40M) exercises the lazily-zeroed
///          sweep;
///   6 kh   int8 group key, null-free, domain over the BOUNDED budget.
fn row(i: u64) -> (i64, Option<i64>, i64, i64, i64, i64) {
    let k = ((i.wrapping_mul(0x9E37_79B9)) >> 5) as i64 % 500 - 50;
    let v = if k == -50 {
        None
    } else if i % 7 == 3 {
        None
    } else {
        Some(((i.wrapping_mul(0x517c_c1b7)) >> 9) as i64 % 4001 - 2000)
    };
    let kw = ((i.wrapping_mul(0xD6E8_FEB8)) >> 3) as i64 % 3_000_000;
    let q = (i % 50) as i64 + 1;
    let kx = ((i.wrapping_mul(0x2545_F491)) >> 2) as i64 % 40_000_000;
    let kh = ((i.wrapping_mul(0x9E6D_62CD)) >> 1) as i64 % 400_000_000;
    (k, v, kw, q, kx, kh)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![
        wcol(1, 4, 4),
        wcol(2, 4, 4),
        wcol(3, 8, 8),
        wcol(4, 4, 4),
        wcol(5, 8, 8),
        wcol(6, 8, 8),
    ];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        791,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (k, v, kw, q, kx, kh) = row(i);
        let datums = [
            RawDatum::Word(k as u64),
            v.map(|x| RawDatum::Word(x as u64)).unwrap_or(RawDatum::Null),
            RawDatum::Word(kw as u64),
            RawDatum::Word(q as u64),
            RawDatum::Word(kx as u64),
            RawDatum::Word(kh as u64),
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
        ColMeta::new(2, "v", TypMeta::INT4),
        ColMeta::new(3, "kw", TypMeta::INT8),
        ColMeta::new(4, "q", TypMeta::INT4),
        ColMeta::new(5, "kx", TypMeta::INT8),
        ColMeta::new(6, "kh", TypMeta::INT8),
    ]
}

fn bank_dir() -> String {
    let dir = std::env::temp_dir().join(format!("sqe_tpchmech_{}", std::process::id()));
    let d = dir.to_str().unwrap().to_string();
    static SEALED: std::sync::Once = std::sync::Once::new();
    SEALED.call_once(|| {
        let _ = std::fs::remove_dir_all(&d);
        seal_bank(&d);
    });
    d
}

fn engine(direct_array: bool) -> Engine {
    let bank = Bank::open(&bank_dir(), schema_meta(), &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(bank, SqeConfig { threads: 3, direct_array, ..SqeConfig::default() })
}

fn ap(group: u32, agg: Vec<AAgg>) -> APlan {
    let mut cols = vec![group];
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
        agg_filters: Vec::new(),
        q: 0,
        family: AFamily::HashPlaneOwnedGroup,
        tags: Vec::new(),
        cols,
        pred: None,
        group: vec![AKeyExpr::Col(group)],
        agg,
        order: None,
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: String::new(),
        flags: Vec::new(),
    }
}

fn run_lines(
    eng: &Engine,
    group: u32,
    agg: Vec<AAgg>,
    having: Option<HavingCmp>,
) -> Vec<String> {
    let mut node = plan_from_ap(&eng.bank, &eng.faces, &ap(group, agg)).expect("lower");
    node.params.having = having;
    let mut l = to_lines(&eng.run(&node));
    l.sort();
    l
}

// ---------------------------------------------------------------------------
// independent scalar oracle (row law, BTreeMap groups — no cells)
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct OG {
    c: u64,
    sum: Option<i128>,
    min: Option<i64>,
    max: Option<i64>,
}

fn oracle_groups(key_of: impl Fn(u64) -> i64, val_of: impl Fn(u64) -> Option<i64>) ->
    std::collections::BTreeMap<i64, OG>
{
    let mut m: std::collections::BTreeMap<i64, OG> = Default::default();
    for i in 0..ROWS {
        let g = m.entry(key_of(i)).or_default();
        g.c += 1;
        if let Some(v) = val_of(i) {
            g.sum = Some(g.sum.unwrap_or(0) + v as i128);
            g.min = Some(g.min.map_or(v, |m| m.min(v)));
            g.max = Some(g.max.map_or(v, |m| m.max(v)));
        }
    }
    m
}

// ---------------------------------------------------------------------------
// gates
// ---------------------------------------------------------------------------

/// Direct-array vs hash identity across the fold vocabulary (nullable
/// inputs, min/max legs, avg's ratio lane, count-star-only + having).
#[test]
fn direct_array_vs_hash_identity() {
    let (ed, eh) = (engine(true), engine(false));
    let mixes: Vec<Vec<AAgg>> = vec![
        vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(2) }],
        vec![
            AAgg::Sum { e: AValExpr::Col(2) },
            AAgg::Min { e: AValExpr::Col(2) },
            AAgg::Max { e: AValExpr::Col(2) },
        ],
        vec![AAgg::Sum { e: AValExpr::Col(4) }],
        vec![AAgg::Avg { e: AValExpr::Col(4) }, AAgg::CountStar],
        vec![AAgg::Min { e: AValExpr::Col(2) }],
    ];
    for mix in mixes {
        let a = run_lines(&ed, 1, mix.clone(), None);
        let b = run_lines(&eh, 1, mix.clone(), None);
        assert_eq!(a, b, "direct-array vs hash arm identity: {mix:?}");
    }
}

/// The direct-array sum answer vs the independent scalar oracle
/// (nullable inputs: NULL folds nothing; all-NULL groups answer NULL).
#[test]
fn direct_array_vs_oracle() {
    let ed = engine(true);
    let lines = run_lines(
        &ed,
        1,
        vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(2) },
            AAgg::Min { e: AValExpr::Col(2) },
            AAgg::Max { e: AValExpr::Col(2) },
        ],
        None,
    );
    let m = oracle_groups(|i| row(i).0, |i| row(i).1);
    let mut want: Vec<String> = m
        .iter()
        .map(|(k, g)| {
            format!(
                "{k}\t{}\t{}\t{}\t{}",
                g.c,
                g.sum.map(|s| s.to_string()).unwrap_or_default(),
                g.min.map(|s| s.to_string()).unwrap_or_default(),
                g.max.map(|s| s.to_string()).unwrap_or_default(),
            )
        })
        .collect();
    want.sort();
    assert_eq!(lines, want, "direct-array vs scalar oracle");
}

/// Fused HAVING legs (sum/min/max/count(*), every cmp op) — both arms,
/// vs the oracle's row-law filter; boundary constants sit ON an actual
/// aggregate value; one leg leaves an EMPTY survivor set.
#[test]
fn having_fused_legs_vs_oracle() {
    let (ed, eh) = (engine(true), engine(false));
    let m = oracle_groups(|i| row(i).0, |i| row(i).1);
    // a REAL group sum for the boundary legs
    let some_sum = m.values().find_map(|g| g.sum).expect("a non-null sum") as i64;
    let cases: Vec<(Vec<AAgg>, HavingCmp, Box<dyn Fn(&OG) -> Option<i128>>)> = vec![
        (
            vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(2) }],
            HavingCmp { agg: 1, op: HvOp::Gt, rhs: some_sum },
            Box::new(|g| g.sum),
        ),
        (
            vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(2) }],
            HavingCmp { agg: 1, op: HvOp::Ge, rhs: some_sum },
            Box::new(|g| g.sum),
        ),
        (
            vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(2) }],
            HavingCmp { agg: 1, op: HvOp::Le, rhs: -100_000 },
            Box::new(|g| g.sum),
        ),
        (
            vec![AAgg::CountStar],
            HavingCmp { agg: 0, op: HvOp::Gt, rhs: 70 },
            Box::new(|g| Some(g.c as i128)),
        ),
        (
            vec![AAgg::Min { e: AValExpr::Col(2) }],
            HavingCmp { agg: 0, op: HvOp::Lt, rhs: -1990 },
            Box::new(|g| g.min.map(|x| x as i128)),
        ),
        (
            vec![AAgg::Max { e: AValExpr::Col(2) }],
            HavingCmp { agg: 0, op: HvOp::Eq, rhs: 1999 },
            Box::new(|g| g.max.map(|x| x as i128)),
        ),
        // empty survivor set (no group sums past the total)
        (
            vec![AAgg::Sum { e: AValExpr::Col(4) }],
            HavingCmp { agg: 0, op: HvOp::Gt, rhs: i64::MAX / 2 },
            Box::new(|_| Some(0)),
        ),
    ];
    for (mix, h, val) in cases {
        let a = run_lines(&ed, 1, mix.clone(), Some(h));
        let b = run_lines(&eh, 1, mix.clone(), Some(h));
        assert_eq!(a, b, "having arms identity: {mix:?} {h:?}");
        // survivor KEY set vs the oracle (3VL: NULL aggregate drops)
        let keep = |v: Option<i128>| -> bool {
            let Some(v) = v else { return false };
            let r = h.rhs as i128;
            match h.op {
                HvOp::Gt => v > r,
                HvOp::Ge => v >= r,
                HvOp::Lt => v < r,
                HvOp::Le => v <= r,
                HvOp::Eq => v == r,
                HvOp::Ne => v != r,
            }
        };
        let mut want: Vec<i64> =
            m.iter().filter(|(_, g)| keep(val(g))).map(|(k, _)| *k).collect();
        want.sort_unstable();
        let mut got: Vec<i64> = a
            .iter()
            .map(|l| l.split('\t').next().unwrap().parse::<i64>().unwrap())
            .collect();
        got.sort_unstable();
        assert_eq!(got, want, "having survivors vs oracle: {mix:?} {h:?}");
    }
}

/// Witness-cap admission twins under the [sqe-hugedom] answer-bound
/// budget law: the wide-domain grouped shape (documents > GROUP_ROW_CAP,
/// domain inside the unbounded budget) refuses bare and ADMITS
/// having-fused; the HUGE domain (over the unbounded budget, inside the
/// bounded band) refuses bare, ADMITS having-fused with zero-init lanes,
/// and keeps refusing with a Min/Max lane (sentinel fill is not
/// occupancy-priced); the over-bounded-budget domain refuses always.
#[test]
fn witness_cap_admission_twins() {
    let eng = engine(true);
    assert!(GROUP_ROW_CAP < 3_000_000, "fixture domain must exceed the cap");
    assert!(
        40_000_000usize * 8 * 3 > direct_array_bytes_cap(false)
            && 40_000_000usize * 8 * 3 <= direct_array_bytes_cap(true),
        "fixture kx domain must sit in the bounded (huge-domain) band"
    );
    assert!(
        400_000_000usize * 8 * 3 > direct_array_bytes_cap(true),
        "fixture kh domain must exceed the bounded budget"
    );
    let mk = |group: u32, agg: AAgg, having: Option<HavingCmp>| {
        let mut node =
            plan_from_ap(&eng.bank, &eng.faces, &ap(group, vec![agg])).expect("lower");
        node.params.having = having;
        check_server_grouped(&eng.bank, &eng.faces, &node)
    };
    let sum_q = || AAgg::Sum { e: AValExpr::Col(4) };
    let h300 = Some(HavingCmp { agg: 0, op: HvOp::Gt, rhs: 300 });
    // [cap-retire] bare big-domain grouped: RULED (2026-08-19) — the
    // direct-array election holds this shape's state under budget
    // (`SpillClass::Bounded`), so the emit-cap witness RETIRES for it:
    // it admits and answers its TRUE group set (the finalize
    // answer-bytes law is the only remaining refusal, at real scale).
    mk(3, sum_q(), None).expect("bare big-domain bounded-state shape must admit (cap-retire)");
    // having-fused + witnessed dense domain inside the budget: admitted
    mk(3, sum_q(), h300).expect("having-fused big-domain shape must admit");
    // [sqe-hugedom] having-fused HUGE domain (over the unbounded budget,
    // inside the bounded band, zero-init lanes): now ADMITTED
    mk(5, sum_q(), h300).expect("having-fused huge-domain sum shape must admit");
    // ... but the bare huge-domain shape keeps the emit-cap refusal:
    // over the unbounded direct-array budget it classifies Spillable,
    // and THIS binary registers no spill substrate (fail-closed — the
    // [cap-retire] flip demands a bounded-state arm, never a stumble).
    match mk(5, sum_q(), None) {
        Err(Refuse::GroupCountUnwitnessed { what: "bound-over-cap" }) => {}
        other => panic!("expected bound-over-cap, got {other:?}"),
    }
    // ... and a Min lane (sentinel fill, not occupancy-priced) refuses
    match mk(5, AAgg::Min { e: AValExpr::Col(4) }, h300) {
        Err(Refuse::GroupCountUnwitnessed { what: "bound-over-cap" }) => {}
        other => panic!("expected min-lane budget refusal, got {other:?}"),
    }
    // over the BOUNDED budget: refused even having-fused with sum lanes
    match mk(6, sum_q(), h300) {
        Err(Refuse::GroupCountUnwitnessed { what: "bound-over-cap" }) => {}
        other => panic!("expected budget refusal, got {other:?}"),
    }
    // and the admitted shapes ANSWER identically on both arms
    let (ed, eh) = (engine(true), engine(false));
    let h = Some(HavingCmp { agg: 0, op: HvOp::Gt, rhs: 45 });
    let a = run_lines(&ed, 3, vec![sum_q()], h);
    let b = run_lines(&eh, 3, vec![sum_q()], h);
    assert_eq!(a, b, "big-domain having answers identical across arms");
    assert!(!a.is_empty(), "fixture must keep some survivors");
}

/// [sqe-hugedom] The huge-domain (40M-wide, 30k-occupied) having-fused
/// grouped fold: dense arm vs hash arm BYTE identity, plus the empty
/// survivor set — the lazily-zeroed sweep over a mostly-untouched
/// domain answers exactly and fast (occupancy edge of the width gates).
#[test]
fn hugedom_having_identity_sparse_occupancy() {
    let (ed, eh) = (engine(true), engine(false));
    for rhs in [45i64, 1_000_000] {
        let h = Some(HavingCmp { agg: 0, op: HvOp::Gt, rhs });
        let a = run_lines(&ed, 5, vec![AAgg::Sum { e: AValExpr::Col(4) }], h);
        let b = run_lines(&eh, 5, vec![AAgg::Sum { e: AValExpr::Col(4) }], h);
        assert_eq!(a, b, "huge-domain having rhs={rhs} identical across arms");
        if rhs == 1_000_000 {
            assert!(a.is_empty(), "boundary rhs must empty the survivor set");
        } else {
            assert!(!a.is_empty(), "fixture must keep survivors at rhs=45");
        }
    }
}

/// The survivor key set materializes sorted/deduped from the HAVING
/// answer (the agg-result-as-set producer half).
#[test]
fn survivor_keyset_materialization() {
    let ed = engine(true);
    let mut node = plan_from_ap(
        &ed.bank,
        &ed.faces,
        &ap(1, vec![AAgg::Sum { e: AValExpr::Col(4) }]),
    )
    .expect("lower");
    node.params.having = Some(HavingCmp { agg: 0, op: HvOp::Gt, rhs: 1500 });
    let a = ed.run(&node);
    let set = sqe::answer::survivor_keyset(&a);
    assert!(set.windows(2).all(|w| w[0] < w[1]), "sorted + deduped");
    let m = oracle_groups(|i| row(i).0, |i| Some(row(i).3));
    let want: Vec<i64> = m
        .iter()
        .filter(|(_, g)| g.sum.unwrap_or(0) > 1500)
        .map(|(k, _)| *k)
        .collect();
    assert_eq!(set, want, "survivor keyset vs oracle");
}
