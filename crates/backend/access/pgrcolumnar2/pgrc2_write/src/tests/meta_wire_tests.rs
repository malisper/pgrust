//! The metadata-plane wiring gate (born-RED, two teeth): M3-E's real
//! builders are what the seal drives, and the stand-in's poverty is what
//! that fixed.
//!
//! Tooth 1 (the defect is real and visible): seal the SAME columns through
//! [`crate::meta_standin::StandinMetaBuilder`] and every zone key is
//! `Absent`, every min/max is 0, sortedness is `Unknown`, and there is not
//! one Psma/Bloom/NdvRegisters section in the part. That is what every
//! blessed bank was carrying, on data sorted on exactly the columns
//! ClickBench filters — granule pruning over it is not slow, it is
//! impossible.
//!
//! Tooth 2 (the fix is real and visible): the same rows through
//! [`crate::meta_wire`] carry exact per-granule/band/part min/max, the true
//! sortedness, nonzero NDV, and the aux sections — and the two-witness null
//! law still cross-checks at every grain.
//!
//! And the law that makes all of it safe: ANSWER IDENTITY. Statistics are
//! advisory metadata. A part sealed with real stats must decode to the exact
//! same values, through the real reader, as the same part sealed with the
//! stand-in. Only the work a prober may skip is allowed to differ.

use super::*;
use crate::meta_standin::StandinMetaBuilder;
use crate::seal::{seal_part, PartSpec, ReferenceResolver, SealReport, SealedPart};
use pgrc2_format::abi::{ByteArena, ColumnMetaBuilder, DecodeOut};
use pgrc2_format::class::{ColSchema, CollationClass, StorageClass, TypeSemantics};
use pgrc2_format::geom::{self, GRANULE_ROWS};
use pgrc2_format::meta::{KeyKind, Sortedness, StatsRecord, STATS_RECORD_LEN};
use pgrc2_format::part::SectionKind;
use pgrc2_format::wire::Cur;
use pgrc2_meta::verdict as meta_verdict;
use pgrc2_read::io::MemPartIo;
use pgrc2_read::{CodecBinding, OpenPart, PartExpect, SectionUnwrapper, StreamCursor};
use std::sync::Arc;

fn spec() -> PartSpec {
    PartSpec {
        spc: SPC,
        db: DB,
        relfilenumber: RELFILENUMBER,
        schema_fingerprint: 0x5EA1,
    }
}

/// Rows chosen to span several granules AND more than one band, so the
/// granule / band / part grains are all exercised (not just aliased).
const ROWS: u64 = 70_000;

/// int8 column, strictly ascending nonnull values with a null every 7th row.
/// Ascending is the ClickBench-relevant shape: the banks are `sorted-v2`.
fn ascending_int8(schema: ColSchema) -> crate::ingest::ColBuffer {
    let mut c = crate::ingest::ColBuffer::new(schema);
    for i in 0..ROWS {
        if i % 7 == 0 {
            c.append_null();
        } else {
            c.append_word(i).expect("word");
        }
    }
    c
}

fn text_payloads(schema: ColSchema) -> crate::ingest::ColBuffer {
    let mut c = crate::ingest::ColBuffer::new(schema);
    for i in 0..ROWS {
        // 7919 distinct values (prime): still well under the row count, so
        // a real NDV estimate is distinguishable from both 0 and ROWS —
        // and above the OD-11 4096-NDV bloom arming floor per granule
        // (~7919 distinct in any 8192-row window, wrap breaks sortedness),
        // so the aux-plane arming assertions keep their teeth.
        let img = img_4b_u(format!("v{:04}", i % 7919).as_bytes());
        c.append_varlena_payload(&img[4..]).expect("payload");
    }
    c
}

fn seal_with(
    vfs: &mut MemVfs,
    cols: &[crate::ingest::ColBuffer],
    builders: &mut [Box<dyn ColumnMetaBuilder>],
    seq: u32,
) -> (SealedPart, SealReport) {
    seal_part(
        vfs,
        DIR,
        &spec(),
        cols,
        &[],
        builders,
        &[],
        &ReferenceResolver,
        &crate::structural::StructuralPolicy::default(),
        7,
        seq,
    )
    .expect("seal")
}

fn standin_builders(n: usize) -> Vec<Box<dyn ColumnMetaBuilder>> {
    (0..n)
        .map(|_| Box::new(StandinMetaBuilder::new()) as Box<dyn ColumnMetaBuilder>)
        .collect()
}

/// Decode a column's Stats section into (granule records, band records,
/// part record) — the spec §8.1 layout the seal writes.
fn stats_of(pv: &PartView, attno: u32) -> (Vec<StatsRecord>, Vec<StatsRecord>, StatsRecord) {
    let body = pv
        .meta_body(SectionKind::Stats, attno, 0)
        .expect("Stats section");
    let gc = geom::granule_count(ROWS);
    let bc = geom::band_count(ROWS);
    assert_eq!(
        body.len(),
        (gc as usize + bc as usize + 1) * STATS_RECORD_LEN,
        "stats section shape"
    );
    let mut c = Cur::new(&body);
    let g: Vec<StatsRecord> = (0..gc).map(|_| StatsRecord::decode(&mut c).expect("g")).collect();
    let b: Vec<StatsRecord> = (0..bc).map(|_| StatsRecord::decode(&mut c).expect("b")).collect();
    let p = StatsRecord::decode(&mut c).expect("part record");
    (g, b, p)
}

// ---------------------------------------------------------------------------
// TOOTH 1 — what the stand-in was actually writing
// ---------------------------------------------------------------------------

#[test]
fn standin_writes_no_usable_metadata_at_all() {
    let mut vfs = mem_with_dir();
    let cols = vec![
        ascending_int8(int8_col(1)),
        text_payloads(text_col(2)),
    ];
    let mut builders = standin_builders(2);
    let (sealed, _) = seal_with(&mut vfs, &cols, &mut builders, 0);
    let pv = PartView::open(&mut vfs, &sealed.tmp_name);

    for attno in [1u32, 2] {
        let (g, b, p) = stats_of(&pv, attno);
        for rec in g.iter().chain(b.iter()).chain(std::iter::once(&p)) {
            assert_eq!(rec.key_kind, KeyKind::Absent.as_u8(), "attno {attno}");
            assert_eq!(rec.min_key, 0);
            assert_eq!(rec.max_key, 0);
            assert_eq!(rec.sortedness, Sortedness::Unknown.as_u8());
            assert_eq!(rec.ndv_est, 0);
            assert_eq!(rec.sum_i128, 0);
            assert_eq!(rec.byte_len_sum, 0);
        }
        // `nonnull` was the ONE honest field (the two-witness leg).
        assert!(p.nonnull > 0, "nonnull was always exact");
        // No aux sections whatsoever.
        for kind in [SectionKind::Psma, SectionKind::Bloom, SectionKind::NdvRegisters] {
            assert!(
                pv.section_bytes(kind, attno, 0).is_none(),
                "stand-in emitted {kind:?} for attno {attno}"
            );
        }
    }
}

/// BORN-RED (#598 leg 1): the stand-in-sealed PART FIXTURE at the answer
/// face. Every part sealed before the metadata wiring (#554) carries this
/// exact vintage — EXACT `nonnull`, `sum_i128 == 0`, `zero_count == 0`,
/// `StatsRecord.flags == 0` — and stays admissible forever (TypeSemantics
/// is deliberately outside schema_fingerprint; blessed v1 bank keys are
/// frozen). The probe side derives the column's REAL profile from the
/// catalog (`SumKind::SignedWord` for int8), so without an on-part witness
/// the word-lane accessors read the zeroed fields as facts: the moment
/// `meta_aggregate` gains a production caller, SUM over this part answers
/// 0 over 60_000 real rows. The answer face must DECLINE (None → the
/// caller decodes). The COUNT kinds stay answerable withOUT any witness —
/// CountStar is closed-form (part rows), CountNonNull is the universally
/// exact `nonnull` leg of the two-witness law — which is the asymmetry
/// that makes the COUNT metadata-answer slice shippable before SUM.
#[test]
fn standin_sealed_part_declines_sum_and_zero_count_at_the_answer_face() {
    let mut vfs = mem_with_dir();
    let cols = vec![ascending_int8(int8_col(1))];
    let mut builders = standin_builders(1);
    let (sealed, _) = seal_with(&mut vfs, &cols, &mut builders, 0);
    let pv = PartView::open(&mut vfs, &sealed.tmp_name);
    let (_, _, p) = stats_of(&pv, 1);

    // The pre-wire vintage, as it exists on disk in every blessed v1 bank.
    let expect_nonnull = (0..ROWS).filter(|i| i % 7 != 0).count() as u32;
    assert_eq!(p.nonnull, expect_nonnull, "stand-in nonnull is exact");
    assert_eq!(p.sum_i128, 0, "stand-in sum is an uncomputed zero");
    assert_eq!(p.zero_count, 0);

    // The probe side derives the REAL catalog profile — the skew this
    // fixture exists to exercise.
    let profile = crate::meta_wire::profile_for(&int8_col(1));
    assert_eq!(profile.sum, pgrc2_meta::profile::SumKind::SignedWord);
    assert_eq!(profile.zero, pgrc2_meta::profile::ZeroKind::Word);

    // The true SUM over these rows is ~2.1e9: an answered Some(0) is the
    // wrong-results class, not a stale estimate.
    let true_sum: i128 = (0..ROWS).filter(|i| i % 7 != 0).map(|i| i as i128).sum();
    assert_ne!(true_sum, 0);

    // THE TOOTH: decline, don't answer.
    assert_eq!(
        meta_verdict::sum_answer(&profile, &p),
        None,
        "a pre-witness part must DECLINE the metadata SUM (true sum {true_sum}), \
         not answer Sum(0) over {expect_nonnull} nonnull rows"
    );
    assert_eq!(
        meta_verdict::zero_count_answer(&profile, &p),
        None,
        "a pre-witness part must DECLINE the metadata ZeroCount"
    );

    // The COUNT kinds survive the decline — the shippable asymmetry:
    // CountNonNull rides the universally-exact nonnull witness, CountStar
    // rides closed-form part rows (never a stats record at all).
    assert_eq!(
        meta_verdict::count_nonnull_answer(&p),
        expect_nonnull as u64,
        "CountNonNull stays answerable over a pre-witness part"
    );
    assert_eq!(sealed.rows, ROWS, "CountStar's leg: closed-form part rows");
}

// ---------------------------------------------------------------------------
// TOOTH 2 — what the real builders write through the identical trait
// ---------------------------------------------------------------------------

#[test]
fn real_builder_writes_exact_keys_sortedness_and_aggregates() {
    let mut vfs = mem_with_dir();
    let cols = vec![ascending_int8(int8_col(1))];
    let mut builders = crate::meta_wire::builders_for_streams(&cols, &[]);
    let (sealed, report) = seal_with(&mut vfs, &cols, &mut builders, 0);
    let pv = PartView::open(&mut vfs, &sealed.tmp_name);
    let (g, b, p) = stats_of(&pv, 1);

    // The two-witness law still ran at every grain (it is the stand-in's
    // old job, and the real builder inherits it): granules + bands + 1.
    let gc = geom::granule_count(ROWS);
    let bc = geom::band_count(ROWS);
    assert_eq!(report.nonnull_crosschecks, gc as u64 + bc as u64 + 1);

    // Per-granule keys: int8 is an EXACT key derivation, and the values are
    // the row ordinals, so min/max are closed-form per granule.
    for (gi, rec) in g.iter().enumerate() {
        let lo = gi as u64 * GRANULE_ROWS as u64;
        let hi = (lo + GRANULE_ROWS as u64).min(ROWS);
        let expect_min = (lo..hi).find(|i| i % 7 != 0).expect("a nonnull row");
        let expect_max = (lo..hi).filter(|i| i % 7 != 0).next_back().expect("a nonnull row");
        assert_eq!(rec.key_kind, KeyKind::Exact.as_u8(), "granule {gi}");
        assert_eq!(rec.min_key, expect_min as i64, "granule {gi} min");
        assert_eq!(rec.max_key, expect_max as i64, "granule {gi} max");
        // Strictly increasing nonnull sequence.
        assert_eq!(rec.sortedness, Sortedness::Ascending.as_u8(), "granule {gi}");
        assert!(rec.ndv_est > 0, "granule {gi} NDV");
    }

    // Band + part grains agree with the granules they cover — the merge law.
    for (bi, rec) in b.iter().enumerate() {
        let first = bi * geom::GRANULES_PER_BAND as usize;
        let last = (first + geom::GRANULES_PER_BAND as usize).min(g.len()) - 1;
        assert_eq!(rec.min_key, g[first].min_key, "band {bi} min");
        assert_eq!(rec.max_key, g[last].max_key, "band {bi} max");
        assert_eq!(rec.key_kind, KeyKind::Exact.as_u8());
    }
    assert_eq!(p.min_key, g[0].min_key, "part min");
    assert_eq!(p.max_key, g[g.len() - 1].max_key, "part max");
    assert_eq!(p.min_key, 1, "first nonnull row is 1 (row 0 is null)");
    let last_nonnull = (0..ROWS).filter(|i| i % 7 != 0).next_back().expect("nonnull");
    assert_eq!(p.max_key, last_nonnull as i64);
    assert_eq!(p.sortedness, Sortedness::Ascending.as_u8());

    // The typed SUM is computed for signed ints (SumKind::SignedWord).
    let expect_sum: i128 = (0..ROWS).filter(|i| i % 7 != 0).map(|i| i as i128).sum();
    assert_eq!(p.sum_i128, expect_sum, "part sum");

    // #598: the real builder mints the computed-stats witness at EVERY
    // grain of the SEALED PART, and the answer face unlocks: SUM answers
    // and matches the decode-side oracle (the same rows the decode path
    // would fold). This is tooth (b) to the stand-in fixture's tooth (a).
    for rec in g.iter().chain(b.iter()).chain(std::iter::once(&p)) {
        assert!(
            meta_verdict::computed_stats_witness(rec),
            "a real-built record must carry the witness"
        );
    }
    let profile = crate::meta_wire::profile_for(&int8_col(1));
    assert_eq!(
        meta_verdict::sum_answer(&profile, &p),
        Some(expect_sum),
        "witnessed part answers SUM == decode"
    );
    assert_eq!(
        meta_verdict::zero_count_answer(&profile, &p),
        Some(0),
        "no nonnull zero in this column (row 0 is null): computed zero, not uncomputed"
    );
    let expect_nonnull = (0..ROWS).filter(|i| i % 7 != 0).count() as u64;
    assert_eq!(meta_verdict::count_nonnull_answer(&p), expect_nonnull);
}

#[test]
fn real_builder_emits_aux_sections_for_a_text_column() {
    let mut vfs = mem_with_dir();
    let cols = vec![text_payloads(text_col(2))];
    let mut builders = crate::meta_wire::builders_for_streams(&cols, &[]);
    let (sealed, _) = seal_with(&mut vfs, &cols, &mut builders, 0);
    let pv = PartView::open(&mut vfs, &sealed.tmp_name);

    // text COLLATE "C" is bloomable AND key-derivable (coarse prefix keys):
    // the whole aux plane arms.
    for kind in [SectionKind::Psma, SectionKind::Bloom, SectionKind::NdvRegisters] {
        let body = pv
            .meta_body(kind, 2, 0)
            .unwrap_or_else(|| panic!("{kind:?} missing — the aux plane did not arm"));
        assert!(!body.is_empty(), "{kind:?} body empty");
        assert!(body.iter().any(|&x| x != 0), "{kind:?} body is all zeroes");
    }

    let (g, _, p) = stats_of(&pv, 2);
    // Coarse keys (prefix): present, and NEVER claimed Exact.
    assert_eq!(p.key_kind, KeyKind::Coarse.as_u8());
    for rec in &g {
        assert_ne!(rec.key_kind, KeyKind::Exact.as_u8(), "prefix keys are coarse");
    }
    // Byte-length stats carry (LenStats::BytesAndChars for text).
    assert_eq!(p.byte_len_min, 5, "\"vNNNN\" is 5 bytes");
    assert_eq!(p.byte_len_max, 5);
    assert!(p.byte_len_sum > 0);
    // 7919 distinct values: the estimate is in the right neighbourhood
    // (HLL p=10, ~3.25% RSE), not 0
    // and not the row count.
    assert!(
        p.ndv_est >= 7000 && p.ndv_est <= 8800,
        "part NDV estimate {} is not near the true 7919",
        p.ndv_est
    );
}

// ---------------------------------------------------------------------------
// THE ANSWER-IDENTITY GATE
// ---------------------------------------------------------------------------

fn binding() -> CodecBinding<'static> {
    static UNWRAPPERS: [&dyn SectionUnwrapper; 0] = [];
    CodecBinding {
        registry: pgrc2_codec::registry(),
        unwrappers: &UNWRAPPERS,
    }
}

/// Every value of one column, decoded through the REAL reader, as canonical
/// bytes per row (None = null slot, which is not value-compared).
fn decode_all(vfs: &mut MemVfs, name: &str, attno: u32, class: StorageClass) -> Vec<Vec<u8>> {
    let bytes = vfs.read_full(&format!("{DIR}/{name}")).expect("part bytes");
    let part = Arc::new(
        OpenPart::open(Box::new(MemPartIo::new(bytes, 4242, 7)), &PartExpect::none())
            .expect("open part"),
    );
    let b = binding();
    let mut cur = StreamCursor::open(Arc::clone(&part), &b, attno, 0).expect("cursor");
    let mut out = Vec::new();
    for g in 0..cur.granule_count() {
        let n = cur.values_in_granule(g).expect("values") as usize;
        let mut datums = vec![0u64; n];
        let mut arena_buf = vec![0u8; 8 << 20];
        {
            let mut o = DecodeOut {
                datums: &mut datums,
                arena: ByteArena::new(&mut arena_buf),
            };
            let wrote = cur.decode_full(g, &mut o).expect("decode_full");
            assert_eq!(wrote as usize, n);
        }
        for &d in &datums {
            let mut scratch = [0u8; 8];
            // SAFETY: the datums were just produced by the reader against a
            // live arena that outlives this loop iteration — the same
            // contract `abi::verify_roundtrip` rides.
            let v = unsafe { pgrc2_format::abi::datum_canonical_bytes(class, d, &mut scratch) }
                .expect("canonical bytes");
            out.push(v.to_vec());
        }
    }
    out
}

/// THE LAW: stats are metadata. The same rows sealed with real stats and
/// with the stand-in must answer IDENTICALLY through the shipped reader —
/// only the work a prober may skip differs.
#[test]
fn real_stats_do_not_change_a_single_decoded_value() {
    let mut vfs = mem_with_dir();
    let cols = vec![ascending_int8(int8_col(1)), text_payloads(text_col(2))];

    let mut standin = standin_builders(2);
    let (a, _) = seal_with(&mut vfs, &cols, &mut standin, 0);
    let mut real = crate::meta_wire::builders_for_streams(&cols, &[]);
    let (bp, _) = seal_with(&mut vfs, &cols, &mut real, 1);

    for (attno, class) in [
        (1u32, StorageClass::ByvalWord { width: 8, signed: true }),
        (2u32, StorageClass::VarlenaVerbatim),
    ] {
        let va = decode_all(&mut vfs, &a.tmp_name, attno, class);
        let vb = decode_all(&mut vfs, &bp.tmp_name, attno, class);
        assert_eq!(va.len(), ROWS as usize, "attno {attno} row count");
        assert_eq!(va, vb, "attno {attno}: real stats changed a decoded value");
    }

    // Sharper than value identity: the VALUE STREAM BYTES are identical.
    // Stats live in their own sections, so adding them must not perturb one
    // byte of the data plane — which is why the answer-identity claim holds
    // for every query, not just the two columns decoded above.
    let pa = PartView::open(&mut vfs, &a.tmp_name);
    let pb = PartView::open(&mut vfs, &bp.tmp_name);
    for attno in [1u32, 2] {
        let (ea, exa) = pa.stream(attno, 0, StreamRole::Values).expect("stream a");
        let (eb, exb) = pb.stream(attno, 0, StreamRole::Values).expect("stream b");
        assert_eq!(ea.encoding, eb.encoding, "attno {attno} election changed");
        assert_eq!(exa.len(), exb.len(), "attno {attno} extent count");
        for (xa, xb) in exa.iter().zip(exb.iter()) {
            assert_eq!(xa.values, xb.values, "attno {attno} extent value count");
            assert_eq!(
                &pa.bytes[xa.file_off as usize..(xa.file_off + xa.len) as usize],
                &pb.bytes[xb.file_off as usize..(xb.file_off + xb.len) as usize],
                "attno {attno}: a value extent's bytes changed"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// DEGRADE, NEVER GUESS
// ---------------------------------------------------------------------------

fn col_with(attno: u32, class: StorageClass, semantics: TypeSemantics) -> ColSchema {
    ColSchema {
        attno,
        class,
        typlen: if matches!(class, StorageClass::VarlenaVerbatim) { -1 } else { 8 },
        typbyval: !matches!(class, StorageClass::VarlenaVerbatim),
        typalign: b'd',
        collation_class: CollationClass::C,
        semantics,
    }
}

/// An UNDECLARED column (the sound default) gets exactly the stand-in's
/// shape for keys: no key, no bloom, no wrong answer. This is what makes
/// adding a semantics row to `pgrc2_am::schema` a pure upside — an unmapped
/// type cannot regress.
#[test]
fn opaque_semantics_emits_no_keys() {
    let mut vfs = mem_with_dir();
    let schema = col_with(
        1,
        StorageClass::ByvalWord { width: 8, signed: true },
        TypeSemantics::Opaque,
    );
    let cols = vec![ascending_int8(schema)];
    let mut builders = crate::meta_wire::builders_for_streams(&cols, &[]);
    let (sealed, _) = seal_with(&mut vfs, &cols, &mut builders, 0);
    let pv = PartView::open(&mut vfs, &sealed.tmp_name);
    let (_, _, p) = stats_of(&pv, 1);
    assert_eq!(p.key_kind, KeyKind::Absent.as_u8());
    assert_eq!(p.sortedness, Sortedness::Unknown.as_u8());
    assert!(p.nonnull > 0, "counts are still exact");
    assert!(pv.section_bytes(SectionKind::Bloom, 1, 0).is_none());
}

/// A MISDECLARED column (semantics inconsistent with its storage class)
/// degrades to the Opaque profile — it does not refuse the ingest, and it
/// does not mint a key from a derivation the bytes do not satisfy.
#[test]
fn inconsistent_semantics_degrade_rather_than_refuse() {
    let mut vfs = mem_with_dir();
    // `TextCollated` requires VarlenaVerbatim; this column is an int8 word.
    let schema = col_with(
        1,
        StorageClass::ByvalWord { width: 8, signed: true },
        TypeSemantics::TextCollated,
    );
    let cols = vec![ascending_int8(schema)];
    let mut builders = crate::meta_wire::builders_for_streams(&cols, &[]);
    // Does not panic, does not error: the seal completes.
    let (sealed, _) = seal_with(&mut vfs, &cols, &mut builders, 0);
    let pv = PartView::open(&mut vfs, &sealed.tmp_name);
    let (_, _, p) = stats_of(&pv, 1);
    assert_eq!(
        p.key_kind,
        KeyKind::Absent.as_u8(),
        "a mislabelled column must never emit a key"
    );
    assert!(p.nonnull > 0);
}

/// Two independent runs of the real-builder factory + seal over the same
/// column must produce identical bytes — the metadata plane is a pure
/// function of the observed values.
///
/// This drives ONE seal call site twice; it does NOT witness serial-vs-
/// parallel identity (both seals below go through the same `seal_with`
/// helper, never `crate::par`'s `seal_one`). The cross-SITE law — serial
/// `TableWriter::cut_part` vs the parallel engine's `seal_one`, metadata
/// plane included — is witnessed at the part-file level by
/// `par_determinism.rs`, whose parallel runs go through the real engine.
#[test]
fn real_builders_are_deterministic_at_the_seal_face() {
    let mut va = mem_with_dir();
    let mut vb = mem_with_dir();
    let cols = vec![ascending_int8(int8_col(1))];

    let mut b1 = crate::meta_wire::builders_for_streams(&cols, &[]);
    let (a, _) = seal_with(&mut va, &cols, &mut b1, 0);
    // A SECOND factory + builder vector over the same column isolates the
    // metadata plane itself: any hidden state in the builders would split
    // the bytes here.
    let mut b2 = crate::meta_wire::builders_for_streams(&cols, &[]);
    let (b, _) = seal_with(&mut vb, &cols, &mut b2, 0);

    let pa = va.read_full(&format!("{DIR}/{}", a.tmp_name)).expect("a");
    let pb = vb.read_full(&format!("{DIR}/{}", b.tmp_name)).expect("b");
    assert_eq!(pa, pb, "the metadata plane is not deterministic");
}
