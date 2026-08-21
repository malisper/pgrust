//! Native pushed-bound gate for the hash-plane packed-byval foundation:
//! a `Params::topk { native }` bound must be served by the stencil's own
//! (count DESC, key ASC) bounded selection — the answer leaves the
//! stencil ALREADY bounded, before `answer::apply_topk` runs. The
//! regression this pins: the stencil ignoring the pushed bound and
//! flattening/sorting/rendering the full group set (a serial tail that
//! scales with the group count — catastrophic in the near-unique-group
//! regime). Both fold routes are pinned (tuned payload lanes and the
//! Cells128 general fold), each against its own full-set twin.

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::answer::AnswerSet;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::{PlanNode, TopK};
use sqe::planner::{plan_from_ap, AAgg, AFamily, AKeyExpr, APlan, AValExpr};
use sqe::render::to_lines;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 30_000;
const K: usize = 10;

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

/// Row law: `a` unique (every (a,b) pair is its own group — the
/// near-unique regime), `b` a correlated 4-byte key, `s` in {0,1},
/// `r` in [1000, 1599].
fn row(i: u64) -> (i64, i64, i64, i64) {
    let a = 9_000_000_000 + i as i64;
    let b = (i % 3_000) as i64;
    let s = (i % 10 == 0) as i64;
    let r = 1_000 + (i % 600) as i64;
    (a, b, s, r)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), wcol(3, 2, 2), wcol(4, 4, 4)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        791,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 8192, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (a, b, s, r) = row(i);
        let datums = [
            RawDatum::Word(a as u64),
            RawDatum::Word(b as u64),
            RawDatum::Word(s as u64),
            RawDatum::Word(r as u64),
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

fn open_engine(dir: &str) -> Engine {
    let schema = vec![
        ColMeta::new(1, "a", TypMeta::INT8),
        ColMeta::new(2, "b", TypMeta::INT4),
        ColMeta::new(3, "s", TypMeta::INT2),
        ColMeta::new(4, "r", TypMeta::INT4),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: true, threads: 1 });
    Engine::new(bank, SqeConfig { threads: 2, ..SqeConfig::default() })
}

fn node(engine: &Engine, aggs: Vec<AAgg>) -> PlanNode {
    let p = APlan {
        sortagg_keys: Vec::new(),
        win: None,
        q: 0,
        family: AFamily::HashPlaneOwnedGroup,
        tags: Vec::new(),
        cols: vec![1, 2, 3, 4],
        pred: None,
        group: vec![AKeyExpr::Col(1), AKeyExpr::Col(2)],
        agg: aggs,
        order: None,
        agg_filters: Vec::new(),
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "topk-native-bound".into(),
        flags: Vec::new(),
    };
    plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan")
}

/// Run the STENCIL alone (no answer-boundary trim): the pre-`apply_topk`
/// answer is the gate's subject.
fn run_stencil(engine: &Engine, node: &PlanNode) -> AnswerSet {
    let ctx = engine.ctx();
    let _pool = sqe::pool::PoolScope::enter(ctx.pool);
    sqe::engine::reset_per_query(ctx.faces);
    let f = sqe::exec::lookup(node.family).expect("registered family");
    f(&ctx, node)
}

fn gate(engine: &Engine, aggs: Vec<AAgg>, label: &str) {
    // Full-set twin: the canonical (count DESC, key ASC) whole answer.
    let full = node(engine, aggs.clone());
    let a_full = run_stencil(engine, &full);
    assert_eq!(a_full.nrows, ROWS as usize, "{label}: pair keys are unique");

    // Native pushed bound: the stencil must pre-bound its own answer.
    let mut bounded = node(engine, aggs);
    bounded.params.topk = Some(TopK { keys: Vec::new(), n: K, native: true });
    let a_bound = run_stencil(engine, &bounded);
    assert_eq!(
        a_bound.nrows, K,
        "{label}: native bound must be served INSIDE the stencil (pre-trim)"
    );

    // Identity: the bounded answer IS the full answer's top-K window.
    let want: Vec<String> = to_lines(&a_full).into_iter().take(K).collect();
    assert_eq!(to_lines(&a_bound), want, "{label}: bounded window identity");
}

#[test]
fn native_bound_rides_foundation_selection() {
    let dir = std::env::temp_dir().join(format!("sqe_topknb_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);

    // Tuned payload lanes (CountStar + Sum + Avg over witnessed domains).
    gate(
        &engine,
        vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(3) },
            AAgg::Avg { e: AValExpr::Col(4) },
        ],
        "payload-lane",
    );
    // Cells128 general fold route (Min forces the cell fold).
    gate(
        &engine,
        vec![AAgg::CountStar, AAgg::Min { e: AValExpr::Col(3) }],
        "cells",
    );

    let _ = std::fs::remove_dir_all(&dir);
}
