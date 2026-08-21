//! SEAL-SPEED-2 fold fusion — the switch-invisibility proof at unit grain:
//! for every fused encoder family, encode_granule_observed (folds riding
//! the emit loop) must produce BYTE-IDENTICAL sections/overflow AND
//! IDENTICAL witness records/aux sections to the classic
//! encode-then-observe pair over a REAL `ColumnMeta` builder. The rig's
//! dirsha gates prove the same law at bank grain; these pin it per family
//! with nulls, empties, oversize varlenas, and multi-granule geometry.

use crate::ingest::ColBuffer;
use crate::meta_wire::builder_for;
use pgrc2_format::abi::{ColumnMetaBuilder, EncodeInput, GranuleEncoder};
use pgrc2_format::class::{ColSchema, CollationClass, StorageClass, TypeSemantics};
use pgrc2_format::enc::Wrapper;
use pgrc2_format::geom::{self, GranuleGrain, OVERSIZE_THRESHOLD};
use pgrc2_format::part::{OverflowSink, StreamSectionWriter};
use pgrc2_format::verbatim::VerbatimEncoder;

fn text_schema() -> ColSchema {
    ColSchema {
        attno: 1,
        class: StorageClass::VarlenaVerbatim,
        typlen: -1,
        typbyval: false,
        typalign: b'i',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::TextCollated,
    }
}

fn word_schema() -> ColSchema {
    ColSchema {
        attno: 1,
        class: StorageClass::ByvalWord { width: 8, signed: true },
        typlen: 8,
        typbyval: true,
        typalign: b'i',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::SignedInt,
    }
}

/// Run one arm over every granule: returns (section bytes, overflow bytes,
/// granule records, band record, part record, aux sections).
#[allow(clippy::type_complexity)]
fn run_arm(
    col: &ColBuffer,
    mut enc: Box<dyn GranuleEncoder>,
    fused: bool,
) -> (
    Vec<u8>,
    Vec<u8>,
    Vec<pgrc2_format::meta::StatsRecord>,
    pgrc2_format::meta::StatsRecord,
    pgrc2_format::meta::StatsRecord,
    Vec<(pgrc2_format::part::SectionKind, Vec<u8>)>,
) {
    let rows = col.rows();
    let flat = col.part_ptrs();
    let grain = GranuleGrain::DEFAULT;
    let gc = geom::granule_count(rows);
    let mut builder = builder_for(&col.schema);
    let mut sec: Vec<u8> = Vec::new();
    let mut ovf_buf: Vec<u8> = Vec::new();
    {
        let key = enc.key();
        let mut w =
            StreamSectionWriter::begin(&mut sec, key.encoding, key.width, Wrapper::None)
                .expect("begin");
        let mut ovf = OverflowSink::new(&mut ovf_buf);
        for g in 0..gc {
            let start = g as usize * grain.rows() as usize;
            let rows_g = geom::rows_in_granule(rows, g);
            let input: EncodeInput<'_> =
                col.encode_input(g, grain, &flat[start..start + rows_g as usize], rows_g);
            if fused {
                enc.encode_granule_observed(&input, &mut w, &mut ovf, builder.as_mut(), g)
                    .expect("fused encode");
            } else {
                enc.encode_granule(&input, &mut w, &mut ovf).expect("encode");
                builder.observe_granule(&input, g);
            }
        }
        enc.finish_stream(&mut w).expect("finish stream");
        w.finish(false).expect("finish section");
    }
    let grecs: Vec<_> = (0..gc).map(|g| builder.seal_granule(g)).collect();
    let band = builder.seal_band(0);
    let part = builder.seal_part();
    let aux = builder.aux_sections();
    (sec, ovf_buf, grecs, band, part, aux)
}

fn assert_arms_identical(col: &ColBuffer, mk: impl Fn() -> Box<dyn GranuleEncoder>, what: &str) {
    let a = run_arm(col, mk(), false);
    let b = run_arm(col, mk(), true);
    assert_eq!(a.0, b.0, "{what}: section bytes");
    assert_eq!(a.1, b.1, "{what}: overflow bytes");
    assert_eq!(a.2, b.2, "{what}: granule records");
    assert_eq!(a.3, b.3, "{what}: band record");
    assert_eq!(a.4, b.4, "{what}: part record");
    assert_eq!(a.5, b.5, "{what}: aux sections");
}

#[test]
fn fused_fold_identical_verbatim_varlena() {
    let mut col = ColBuffer::new(text_schema());
    for i in 0..9000u32 {
        match i % 7 {
            0 => col.append_null(),
            1 => col.append_varlena_payload(b"").expect("append"),
            2 => col
                .append_varlena_payload("z\u{00E9}bra-\u{1F600}".as_bytes())
                .expect("append"),
            3 if i == 3 => {
                // One oversize value: exercises the overflow routing under
                // the fused observer.
                let big = vec![0xAB; OVERSIZE_THRESHOLD as usize + 17];
                col.append_varlena_payload(&big).expect("append");
            }
            _ => col
                .append_varlena_payload(format!("value-{:05}", i % 300).as_bytes())
                .expect("append"),
        }
    }
    assert_arms_identical(
        &col,
        || Box::new(VerbatimEncoder { class: StorageClass::VarlenaVerbatim }),
        "verbatim varlena",
    );
}

#[test]
fn fused_fold_identical_bytefor() {
    let mut col = ColBuffer::new(word_schema());
    let mut seed = 0xF01Du64;
    for i in 0..9000u32 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        if i % 13 == 0 {
            col.append_null();
        } else {
            col.append_word(1_000_000 + (seed % 60_000)).expect("append");
        }
    }
    assert_arms_identical(
        &col,
        || {
            Box::new(pgrc2_codec::bytefor::ByteForEncoder::new_bytefor(
                8, 2, true,
            ))
        },
        "byte_for",
    );
}

#[test]
fn fused_fold_identical_deltafor() {
    let mut col = ColBuffer::new(word_schema());
    let mut seed = 0xDE17_Au64;
    for i in 0..9000u64 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        if i % 17 == 0 {
            col.append_null();
        } else {
            col.append_word(i * 3 + seed % 4).expect("append");
        }
    }
    assert_arms_identical(
        &col,
        || Box::new(pgrc2_codec::deltafor::DeltaForEncoder::default()),
        "delta_for",
    );
}

/// Word-class verbatim routes to the classic pair inside the override (the
/// bulk-copy law) — same identity must hold through that path too.
#[test]
fn fused_fold_identical_verbatim_words() {
    let mut col = ColBuffer::new(word_schema());
    for i in 0..5000u64 {
        if i % 11 == 0 {
            col.append_null();
        } else {
            col.append_word(i.wrapping_mul(0x9E37_79B9)).expect("append");
        }
    }
    assert_arms_identical(
        &col,
        || {
            Box::new(VerbatimEncoder {
                class: StorageClass::ByvalWord { width: 8, signed: true },
            })
        },
        "verbatim words",
    );
}

// ---------------------------------------------------------------------------
// D-STATS dict fold-from-codes at the write grain (seal-fusion charter §4
// cut 3): entries + codes minted by the REAL DictBuilder — the byte-rank
// order certificate exactly as production mints it — and the codes-face
// fold must be OUTPUT-IDENTICAL to the classic hydrated-value walk on both
// shells. The seal driver's plumbing hands the codes-currency EncodeInput
// (rows + validity + global codes) to the face; this pins that contract.
// ---------------------------------------------------------------------------

#[allow(clippy::type_complexity)]
fn seal_out(
    mut b: pgrc2_meta::builder::ColumnMeta,
    gc: u32,
) -> (
    Vec<pgrc2_format::meta::StatsRecord>,
    Vec<pgrc2_format::meta::StatsRecord>,
    pgrc2_format::meta::StatsRecord,
    Vec<(pgrc2_format::part::SectionKind, Vec<u8>)>,
    Option<pgrc2_format::sidecar::ColDistribution>,
) {
    use pgrc2_format::geom::GRANULES_PER_BAND;
    let grecs: Vec<_> = (0..gc).map(|g| b.seal_granule(g)).collect();
    let bands: Vec<_> = (0..gc.div_ceil(GRANULES_PER_BAND))
        .map(|band| b.seal_band(band))
        .collect();
    let part = b.seal_part();
    let aux = b.aux_sections();
    let dist = b.distribution();
    (grecs, bands, part, aux, dist)
}

#[test]
fn dict_codes_fold_identical_to_value_walk_via_real_dict_build() {
    use crate::dict::DictBuilder;
    use pgrc2_meta::builder::ColumnMeta;

    let mut col = ColBuffer::new(text_schema());
    for i in 0..9000u32 {
        match i % 6 {
            0 => col.append_null(),
            1 => col
                .append_varlena_payload("з-товар-😀".as_bytes())
                .expect("append"),
            2 => col.append_varlena_payload(b"").expect("append"),
            _ => col
                .append_varlena_payload(format!("value-{:03}", i % 250).as_bytes())
                .expect("append"),
        }
    }
    // The real dict build over the non-null payload sequence (row order —
    // the same walk the election arm runs).
    let mut db = DictBuilder::default();
    for row in 0..col.rows() {
        if let Some(p) = col.varlena_payload(row).expect("payload") {
            db.observe(p);
        }
    }
    let built = db.build();
    // Codes per granule, row-dense, null slots 0 — the elect.rs walk shape
    // (observe order == row-dense granule order).
    let rows = col.rows();
    let flat = col.part_ptrs();
    let grain = GranuleGrain::DEFAULT;
    let gc = geom::granule_count(rows);
    let ginputs: Vec<EncodeInput<'_>> = (0..gc)
        .map(|g| {
            let start = g as usize * grain.rows() as usize;
            let rows_g = geom::rows_in_granule(rows, g);
            col.encode_input(g, grain, &flat[start..start + rows_g as usize], rows_g)
        })
        .collect();
    let mut observed = 0usize;
    let code_granules: Vec<Vec<u64>> = ginputs
        .iter()
        .map(|gi| {
            (0..gi.rows)
                .map(|r| {
                    if gi.valid(r) {
                        let c = built.code_of_observed(observed).expect("dict value") as u64;
                        observed += 1;
                        c
                    } else {
                        0
                    }
                })
                .collect()
        })
        .collect();
    let entries = built
        .into_counted_entries()
        .expect("fresh build carries counts");

    let profile = crate::meta_wire::profile_for(&col.schema);
    // Arm A: the codes face (batched shell + feed — the seal driver's
    // dict-stream path when the face is offered).
    let mut a = ColumnMeta::with_batch_fold(profile, true);
    a.set_distribution_feed(entries.clone());
    assert!(
        a.dict_code_observe_supported(),
        "the codes face must be offered on a feed-armed dict text column"
    );
    for (g, gi) in ginputs.iter().enumerate() {
        let ci = EncodeInput {
            class: col.schema.class,
            rows: gi.rows,
            datums: &code_granules[g],
            validity: gi.validity,
        };
        a.observe_granule_dict_codes(&ci, g as u32);
    }
    // Arms B/C: the hydrated value walk on both shells, feed-armed the
    // same way (the classic pair the driver falls back to).
    let mut bb = ColumnMeta::with_batch_fold(profile, true);
    bb.set_distribution_feed(entries.clone());
    let mut cc = ColumnMeta::with_batch_fold(profile, false);
    cc.set_distribution_feed(entries);
    for (g, gi) in ginputs.iter().enumerate() {
        bb.observe_granule(gi, g as u32);
        cc.observe_granule(gi, g as u32);
    }
    let out_a = seal_out(a, gc);
    let out_b = seal_out(bb, gc);
    let out_c = seal_out(cc, gc);
    assert_eq!(out_a, out_b, "codes face vs batched value walk");
    assert_eq!(out_a, out_c, "codes face vs per-value walk");
}
