//! [E18-M] The grouped budget's MACHINE floor (the wave-3 ClickBench
//! submission unrefusal): on a box whose vCPU:memory ratio starves the
//! width law (c6a.4xlarge — 16 threads x 64 MiB = 1 GiB against a 100m
//! scatter plane of 2.8 GB), the six spill-unavailable/hashgroup nulls
//! (1-based Q11/Q12/Q14/Q19/Q36/Q40) refused shapes the c8g.16xlarge tax
//! rig served RESIDENT under its 4 GiB width budget. The floor prices the
//! budget against the MACHINE (max(width law, RAM/8)) so the same
//! statement admits memory-resident wherever the plane truly fits.
//!
//! Red-before-green: with the floor disarmed (machine_mem_bytes = 0, the
//! kill-switch arm) the failing shapes refuse `grouped-spill-unavailable`
//! exactly as the submission cell witnessed; armed, they admit and serve.

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::planner::{
    check_server_grouped, plan_from_ap, AAgg, AFamily, AKeyExpr, APlan, AValExpr,
};
use sqe::refuse::Refuse;
use sqe::typmeta::TypMeta;

/// Enough rows that the scatter price rows x SCATTER_ROW_BYTES (28)
/// exceeds the width-1 budget (64 MiB): 2.6M x 28 = 72.8 MB.
const ROWS: u64 = 2_600_000;

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

/// Columns: 1 k int4 (500-distinct dense key), 2 v int8 (high-NDV lane).
fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let cands = pgrc2_write::elect::CodecCandidates::new(Default::default());
    let resolver = pgrc2_write::seal::CodecResolver;
    let schema = vec![wcol(1, 4, 4), wcol(2, 8, 8)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        784,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 1 << 20, max_bytes: u64::MAX, cut_granule_rows: 1 << 14 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let k = ((i.wrapping_mul(0x9E37_79B9)) >> 5) % 500;
        let v = i.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 1;
        let datums = [RawDatum::Word(k), RawDatum::Word(v)];
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

fn engine(dir: &str, machine_mem_bytes: u64) -> Engine {
    let schema =
        vec![ColMeta::new(1, "k", TypMeta::INT4), ColMeta::new(2, "v", TypMeta::INT8)];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(
        bank,
        SqeConfig {
            threads: 1,
            spill: true,
            grouped_budget_override: None,
            machine_mem_bytes,
            ..SqeConfig::default()
        },
    )
}

/// The Q36 failing shape (key_exprs = no spill arm): GROUP BY k, k - 1.
fn q36_shape(eng: &Engine) -> Result<(), Refuse> {
    let ap = APlan {
        q: 36,
        family: AFamily::HashPlaneOwnedGroup,
        tags: Vec::new(),
        cols: vec![1],
        pred: None,
        group: vec![AKeyExpr::Col(1), AKeyExpr::MinusConst { col: 1, k: 1 }],
        agg: vec![AAgg::CountStar],
        order: None,
        sortagg_keys: Vec::new(),
        win: None,
        agg_filters: Vec::new(),
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "mem-floor gate".into(),
        flags: Vec::new(),
    };
    let node = plan_from_ap(&eng.bank, &eng.faces, &ap)?;
    check_server_grouped(&eng.bank, &eng.faces, &node)
}

/// The one budget authority: override wins; else max(width law, RAM/8);
/// machine_mem_bytes = 0 (probe miss or kill switch) reproduces the
/// width law verbatim.
#[test]
fn budget_law_arithmetic() {
    let base = SqeConfig { threads: 1, ..SqeConfig::default() };
    let width1 = 64u64 * 1024 * 1024;
    let cfg = SqeConfig { machine_mem_bytes: 0, ..base.clone() };
    assert_eq!(cfg.grouped_budget_bytes(), width1, "floor disarmed = width law");
    let cfg = SqeConfig { machine_mem_bytes: 32 << 30, ..base.clone() };
    assert_eq!(cfg.grouped_budget_bytes(), 4 << 30, "c6a.4xlarge: RAM/8 floor wins");
    let cfg = SqeConfig { machine_mem_bytes: 64 << 20, ..base.clone() };
    assert_eq!(cfg.grouped_budget_bytes(), width1, "tiny box: width law stands");
    let cfg = SqeConfig {
        machine_mem_bytes: 32 << 30,
        grouped_budget_override: Some(4096),
        ..base
    };
    assert_eq!(cfg.grouped_budget_bytes(), 4096, "override beats the floor");
}

/// Red-before-green on the failing shape class: floor disarmed refuses
/// `grouped-spill-unavailable` (the submission cell's exact verdict);
/// floor armed at the c6a.4xlarge geometry admits the same statement.
#[test]
fn mem_floor_admits_the_submission_null_class() {
    let dir = std::env::temp_dir().join(format!("sqe_memfloor_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);

    // RED (the pre-fix arm): width law only — over budget, no spill arm.
    let bare = engine(&dir, 0);
    match q36_shape(&bare) {
        Err(Refuse::GroupedSpillUnavailable { .. }) => {}
        other => panic!("expected grouped-spill-unavailable without the floor, got {other:?}"),
    }

    // GREEN: the machine floor (32 GiB box -> 4 GiB budget) admits.
    let floored = engine(&dir, 32 << 30);
    q36_shape(&floored).expect("machine floor admits the Q36 class resident");

    // The distinct class (Q11/Q14: COUNT(DISTINCT v) GROUP BY k) rides
    // the same est law: refused bare, admitted under the floor.
    let distinct_shape = |eng: &Engine| -> Result<(), Refuse> {
        let ap = APlan {
            q: 11,
            family: AFamily::DistinctPipeline,
            tags: Vec::new(),
            cols: vec![1],
            pred: None,
            group: vec![AKeyExpr::Col(1)],
            agg: vec![AAgg::CountDistinct { e: AValExpr::Col(2) }],
            order: None,
            sortagg_keys: Vec::new(),
            win: None,
            agg_filters: Vec::new(),
            having: None,
            params: Vec::new(),
            fingerprints: Vec::new(),
            kernel_oracle: String::new(),
            notes: "mem-floor gate".into(),
            flags: Vec::new(),
        };
        let node = plan_from_ap(&eng.bank, &eng.faces, &ap)?;
        check_server_grouped(&eng.bank, &eng.faces, &node)
    };
    match distinct_shape(&bare) {
        Err(Refuse::GroupedSpillUnavailable { .. }) => {}
        other => panic!("expected distinct-class refusal without the floor, got {other:?}"),
    }
    distinct_shape(&floored).expect("machine floor admits the distinct class resident");
}
