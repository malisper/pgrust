//! Cold-open phase attribution gate: the bank open's cost decomposes into
//! NAMED phases (manifest walk / part opens / bankstats consult / schema
//! reconcile), every phase carries a declared face-build reason, and the
//! open faults EXACTLY the four fixed part structures — so a future plane
//! or face that grows eager open-time reads surfaces here, not on the
//! CI cluster's cold probe. Also pins the parallel-open identity law: a bank
//! opened at width N is byte-identical to the serial open (schema classes,
//! packed-scale witnesses, part order, null-freedom proofs).

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_read::openpart::FaultTag;
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::typmeta::TypMeta;

const ROWS: u64 = 30_000;

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![
        ColSchema {
            attno: 1,
            class: StorageClass::ByvalWord { width: 8, signed: true },
            typlen: 8,
            typbyval: true,
            typalign: b'd',
            collation_class: CollationClass::C,
            semantics: TypeSemantics::SignedInt,
        },
        ColSchema {
            attno: 2,
            class: StorageClass::ByvalWord { width: 4, signed: true },
            typlen: 4,
            typbyval: true,
            typalign: b'i',
            collation_class: CollationClass::C,
            semantics: TypeSemantics::SignedInt,
        },
        ColSchema {
            attno: 3,
            class: StorageClass::VarlenaVerbatim,
            typlen: -1,
            typbyval: false,
            typalign: b'i',
            collation_class: CollationClass::C,
            semantics: TypeSemantics::TextCollated,
        },
    ];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        777,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let k = ((i.wrapping_mul(0x9E37_79B9_7F4A_7C15)) >> 17) as i64 % 1000;
        let s = format!("s{:02}", i % 23);
        let img = img_4b_u(s.as_bytes());
        let datums = [
            RawDatum::Word(k.unsigned_abs()),
            RawDatum::Word((i % 16) as u64),
            RawDatum::Bytes(&img),
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
    let probe = Probe::new(TxnVerdict::Committed);
    w.publish(&mut vfs, &probe).expect("publish");
}

fn schema() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "k", TypMeta::INT8),
        ColMeta::new(2, "g", TypMeta::INT4),
        ColMeta::new(3, "s", TypMeta::TEXT_C),
    ]
}

#[test]
fn cold_open_phase_attribution_and_parallel_identity() {
    let dir = std::env::temp_dir().join(format!("sqe_coldopen_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);

    sqe::coldledger::set_main_thread();
    let _ = sqe::coldledger::drain_lines(0, "reset");

    // Parallel open (the production posture: pool-width fan-out).
    let par = Bank::open(&dir, schema(), &OpenOpts { bankstats: true, threads: 4 });
    assert!(par.parts.len() > 1, "want a multi-part bank, got {}", par.parts.len());

    // --- Phase attribution: the open decomposes into the four named
    // phases, once each, every one carrying a declared reason (the
    // FACE-BUILD LAW's open-time leg: reason=none is the gate's failure).
    let lines = sqe::coldledger::drain_lines(0, "open");
    let phase = |kind: &str| -> Vec<&String> {
        lines
            .iter()
            .filter(|l| l.contains(&format!("|kind={kind}|")) && !l.contains("|detail|"))
            .collect()
    };
    for kind in ["open_manifest", "open_parts", "open_bankstats", "open_schema"] {
        let v = phase(kind);
        assert_eq!(v.len(), 1, "phase {kind} attributed exactly once: {lines:?}");
        assert!(!v[0].contains("|reason=none"), "phase {kind} must declare a reason");
    }

    // --- Open-fault law: opening the bank faults EXACTLY the four fixed
    // structures per part (tail, footer, section table, header), plus the
    // FIRST part's stream-directory section (the schema-reconcile
    // consult), and NOTHING else — an eagerly-built data face at open
    // would show up as extra faults here.
    for (pi, p) in par.parts.iter().enumerate() {
        let faults = p.faults();
        let tags: Vec<FaultTag> = faults.iter().map(|f| f.tag).collect();
        assert_eq!(
            &tags[..4],
            &[FaultTag::Tail, FaultTag::Footer, FaultTag::SectionTable, FaultTag::Header],
            "part {pi}: the fixed four-read open set"
        );
        let extra = &tags[4..];
        if pi == 0 {
            // reconcile_classes consults p0's stream directory (kind 1).
            assert!(
                extra.iter().all(|t| matches!(
                    t,
                    FaultTag::ListedSection { kind: 1, .. }
                )) && extra.len() <= 1,
                "part 0: only the stream-directory reconcile consult may follow: {extra:?}"
            );
        } else {
            assert!(extra.is_empty(), "part {pi}: open faulted beyond the fixed set: {extra:?}");
        }
    }
    // The open_parts ledger bytes equal the summed open faults (the
    // attribution is honest, not estimated).
    let open_bytes: u64 = par
        .parts
        .iter()
        .map(|p| {
            p.faults()
                .iter()
                .filter(|f| !matches!(f.tag, FaultTag::ListedSection { .. }))
                .map(|f| f.len)
                .sum::<u64>()
        })
        .sum();
    let l = phase("open_parts")[0].clone();
    assert!(
        l.contains(&format!("|bytes={open_bytes}|")),
        "open_parts bytes must equal the summed part-open faults: {l} vs {open_bytes}"
    );

    // --- Parallel-open identity: serial and width-4 opens agree on every
    // engine-visible fact (the census/memo law for the open fan-out).
    let ser = Bank::open(&dir, schema(), &OpenOpts { bankstats: true, threads: 1 });
    assert_eq!(ser.schema, par.schema, "reconciled schema (classes + witnesses)");
    assert_eq!(ser.parts.len(), par.parts.len());
    for (a, b) in ser.parts.iter().zip(par.parts.iter()) {
        assert_eq!(a.header().part_no, b.header().part_no, "manifest part order preserved");
        assert_eq!(a.rows(), b.rows());
    }
    assert_eq!(ser.rows_total(), par.rows_total());
    assert_eq!(ser.manifest.header.gen, par.manifest.header.gen);
    for attno in [1u32, 2, 3] {
        assert_eq!(ser.null_free(attno), par.null_free(attno), "null-freedom proof attno {attno}");
        assert_eq!(ser.face(attno), par.face(attno), "decode face attno {attno}");
    }
    assert_eq!(ser.stats_plane.is_some(), par.stats_plane.is_some(), "bankstats plane posture");

    // --- [rung2] Demand-time proof attribution: the null-freedom builds
    // above landed in the ledger as a named kind with a declared reason
    // (no dark once-per-open work), serial and parallel alike.
    let lines = sqe::coldledger::drain_lines(0, "post");
    let nf: Vec<&String> = lines
        .iter()
        .filter(|l| l.contains("|kind=nullfree|") && !l.contains("|detail|"))
        .collect();
    assert!(!nf.is_empty(), "null-freedom proofs must be ledger-attributed: {lines:?}");
    assert!(
        nf.iter().all(|l| !l.contains("|reason=none")),
        "nullfree must declare a reason: {nf:?}"
    );

    std::fs::remove_dir_all(&dir).ok();
}
