//! Global dict build (M3-D slice leg 4): byte-rank-sorted invariant, stored
//! byte/char lengths, round-trip vs oracle through the FORMAT's own
//! `dict_entry` reader (spec §7).

use crate::dict::{DictBuilder, TextSemantics};
use pgrc2_format::dict::{utf8_char_count, DictCharLenForm};
use pgrc2_format::dict::{dict_entry, DictSections};
use pgrc2_format::geom::DICT_FRAME_ENTRIES;
use pgrc2_format::part::{StreamSectionHdr, STREAM_SECTION_HDR_LEN};

fn corpus() -> Vec<Vec<u8>> {
    // Deliberately unsorted, with duplicates, empties, multibyte UTF-8, and
    // byte-rank-vs-codepoint traps (0xF0… sorts after ASCII by bytes).
    let mut v: Vec<Vec<u8>> = vec![
        b"pear".to_vec(),
        b"apple".to_vec(),
        b"apple".to_vec(),
        b"".to_vec(),
        "z\u{00E9}bra".as_bytes().to_vec(),   // é = 2 bytes
        "\u{1F600}grin".as_bytes().to_vec(), // 4-byte emoji lead
        b"apple2".to_vec(),
        b"Apple".to_vec(),
        b"banana".to_vec(),
    ];
    // Bulk entries to cross a dict-frame boundary (1024 entries/frame).
    for i in 0..1500u32 {
        v.push(format!("bulk-{i:05}").into_bytes());
    }
    v
}

#[test]
fn byte_rank_sorted_codes_and_stored_lengths() {
    let mut b = DictBuilder::new(TextSemantics::Utf8Chars);
    for e in corpus() {
        b.observe(&e);
    }
    let built = b.build();
    assert!(built.is_byte_rank_sorted(), "byte-rank order is contractual");
    // Distinct: 8 named + 1500 bulk ("apple" duplicated).
    assert_eq!(built.entry_count(), 1508);
    // code compare == byte compare (order embedding).
    assert!(built.code_of(b"Apple") < built.code_of(b"apple"));
    assert!(built.code_of(b"apple") < built.code_of(b"apple2"));
    assert!(built.code_of(b"").unwrap() == 0, "empty string is byte-rank first");
    assert!(built.code_of(b"nope").is_none());

    let images = built.emit_sections(DictCharLenForm::Absolute).expect("emit");
    // Round-trip vs oracle through the format's own reader.
    let verified = built.verify_sections(&images).expect("round-trip");
    assert_eq!(verified, 1508);

    // char_len law: UTF-8 chars for text semantics.
    let d = DictSections {
        index: &images.index_section
            [STREAM_SECTION_HDR_LEN..images.index_section.len()],
        payload: payload_of(&images.payload_section),
        entry_count: images.entry_count,
        charlen_form: images.charlen_form,
    };
    let zebra = "z\u{00E9}bra".as_bytes();
    let code = built.code_of(zebra).expect("zebra code");
    let e = dict_entry(&d, code).expect("entry");
    assert_eq!(e.byte_len, 6);
    assert_eq!(e.char_len, 5);
    // Framing: 1508 entries ⇒ 2 dict frames marked in the payload section.
    let hdr = StreamSectionHdr::decode(&images.payload_section).expect("hdr");
    assert_eq!(
        hdr.frame_count,
        1508u32.div_ceil(DICT_FRAME_ENTRIES),
        "dict frames at DICT_FRAME_ENTRIES grain"
    );
}

#[test]
fn bytes_semantics_char_len_equals_byte_len() {
    let mut b = DictBuilder::new(TextSemantics::BytesOnly);
    b.observe(&[0xFF, 0xFE, 0x00, 0x41]);
    let built = b.build();
    let images = built.emit_sections(DictCharLenForm::Absolute).expect("emit");
    built.verify_sections(&images).expect("round-trip");
    let d = DictSections {
        index: &images.index_section[STREAM_SECTION_HDR_LEN..],
        payload: payload_of(&images.payload_section),
        entry_count: 1,
        charlen_form: images.charlen_form,
    };
    let e = dict_entry(&d, 0).expect("entry");
    assert_eq!(e.byte_len, 4);
    assert_eq!(e.char_len, 4);
}

#[test]
fn determinism_two_builds_identical_sections() {
    let build = || {
        let mut b = DictBuilder::new(TextSemantics::Utf8Chars);
        for e in corpus() {
            b.observe(&e);
        }
        b.build().emit_sections(DictCharLenForm::Absolute).expect("emit")
    };
    let a = build();
    let b = build();
    assert!(a.index_section == b.index_section);
    assert!(a.payload_section == b.payload_section);
}

// ---------------------------------------------------------------------------
// M5d.char-len-form teeth (born-RED lineage): the delta form must be a
// PURE re-expression — absolute `char_len` reconstructed exactly at decode,
// stored-form bytes zero on ASCII, and a corrupt delta refused TYPED.
// ---------------------------------------------------------------------------

/// Raw third field of index entry `code` (the stored `char_field` bytes,
/// straight off the wire — the form witness the reconstruction teeth pin).
fn raw_char_field(images: &crate::dict::DictSectionImages, code: u32) -> u32 {
    let idx = &images.index_section[STREAM_SECTION_HDR_LEN..];
    let off = code as usize * pgrc2_format::dict::DICT_INDEX_ENTRY_LEN;
    u32::from_le_bytes(idx[off + 8..off + 12].try_into().expect("len 4"))
}

/// Delta-form round-trip (the S8 §2(a) remedy): multibyte entries store
/// `byte_len − char_len`, pure-ASCII entries store 0, and BOTH the
/// write-side oracle (`verify_sections`) and the format reader
/// (`dict_entry`) hand back ABSOLUTE `char_len` — the seal-time-verified
/// fact survives the re-expression bit-exactly.
#[test]
fn charlen_delta_form_roundtrips_and_zeroes_ascii() {
    let mut b = DictBuilder::new(TextSemantics::Utf8Chars);
    for e in corpus() {
        b.observe(&e);
    }
    let built = b.build();
    let images = built.emit_sections(DictCharLenForm::Delta).expect("emit");
    assert_eq!(images.charlen_form, DictCharLenForm::Delta, "the emitted form is recorded");
    // The write-side round-trip oracle must reconstruct through the form.
    let verified = built.verify_sections(&images).expect("delta round-trip");
    assert_eq!(verified, 1508);
    let d = DictSections {
        index: &images.index_section[STREAM_SECTION_HDR_LEN..],
        payload: payload_of(&images.payload_section),
        entry_count: images.entry_count,
        charlen_form: DictCharLenForm::Delta,
    };
    // Multibyte: é is 2 bytes/1 char → stored delta 1, absolute 5 of 6.
    let zebra = "z\u{00E9}bra".as_bytes();
    let zc = built.code_of(zebra).expect("zebra code");
    let e = dict_entry(&d, zc).expect("entry");
    assert_eq!((e.byte_len, e.char_len), (6, 5), "absolute char_len reconstructed");
    assert_eq!(raw_char_field(&images, zc), 1, "stored form is the delta");
    // 4-byte emoji lead: 8 bytes, 5 chars → delta 3.
    let grin = "\u{1F600}grin".as_bytes();
    let gc = built.code_of(grin).expect("grin code");
    let e = dict_entry(&d, gc).expect("entry");
    assert_eq!((e.byte_len, e.char_len), (8, 5));
    assert_eq!(raw_char_field(&images, gc), 3);
    // Pure ASCII stores ZERO — the entropy kill the census must witness.
    for probe in [&b"apple"[..], &b"bulk-00042"[..], &b""[..]] {
        let c = built.code_of(probe).expect("ascii code");
        assert_eq!(raw_char_field(&images, c), 0, "ASCII delta is zero");
        let e = dict_entry(&d, c).expect("entry");
        assert_eq!(e.char_len, probe.len() as u32);
        assert_eq!(e.byte_len, probe.len() as u32);
    }
}

/// The dirsha posture at unit grain: the DISARMED form is byte-for-byte
/// the pre-M5d lineage (absolute `char_len` in the third field), and the
/// two forms differ ONLY in that field — payload sections identical.
#[test]
fn charlen_delta_disarmed_is_byte_identical_lineage() {
    let mut b = DictBuilder::new(TextSemantics::Utf8Chars);
    for e in corpus() {
        b.observe(&e);
    }
    let built = b.build();
    let abs = built.emit_sections(DictCharLenForm::Absolute).expect("emit abs");
    let delta = built.emit_sections(DictCharLenForm::Delta).expect("emit delta");
    assert_eq!(abs.charlen_form, DictCharLenForm::Absolute);
    assert!(abs.payload_section == delta.payload_section, "payload untouched");
    let d = DictSections {
        index: &abs.index_section[STREAM_SECTION_HDR_LEN..],
        payload: payload_of(&abs.payload_section),
        entry_count: abs.entry_count,
        charlen_form: DictCharLenForm::Absolute,
    };
    for probe in [&b"apple"[..], "z\u{00E9}bra".as_bytes()] {
        let c = built.code_of(probe).expect("code");
        let e = dict_entry(&d, c).expect("entry");
        // Absolute form stores char_len itself — the pre-M5d bytes.
        assert_eq!(raw_char_field(&abs, c), e.char_len);
    }
}

/// Guard tooth (AD-4 class, seeded violating input): a stored delta larger
/// than `byte_len` can only be corruption (UTF-8 chars are never longer
/// than their bytes) — the reader refuses TYPED, never wraps.
#[test]
fn charlen_delta_underflow_refuses_typed() {
    let mut b = DictBuilder::new(TextSemantics::Utf8Chars);
    b.observe(b"abc");
    let built = b.build();
    let mut images = built.emit_sections(DictCharLenForm::Delta).expect("emit");
    // Seed the violation: third field of code 0 ← byte_len + 1.
    let off = STREAM_SECTION_HDR_LEN + 8;
    images.index_section[off..off + 4].copy_from_slice(&4u32.to_le_bytes());
    let d = DictSections {
        index: &images.index_section[STREAM_SECTION_HDR_LEN..],
        payload: payload_of(&images.payload_section),
        entry_count: 1,
        charlen_form: DictCharLenForm::Delta,
    };
    match dict_entry(&d, 0) {
        Err(pgrc2_format::FormatError::Corrupt { at }) => {
            assert_eq!(at, "dict entry char_len delta")
        }
        other => panic!("corrupt delta must refuse typed, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Option-C ABSENT-form teeth (M5d package §9, the S8 §2(b) lengths-prepass
// probe): NOTHING stored — the third field is ZERO on every entry, and the
// decode funnels recompute absolute char_len from the entry bytes.
// ---------------------------------------------------------------------------

/// Absent-form round-trip: zeros on the wire, exact recompute at decode
/// (multibyte + ASCII + empty), the write-side oracle passes through the
/// recompute, and the payload section is byte-identical to the blessed
/// lineage's (only the index third field moves).
#[test]
fn charlen_absent_stores_zeros_and_recomputes() {
    let mut b = DictBuilder::new(TextSemantics::Utf8Chars);
    for e in corpus() {
        b.observe(&e);
    }
    let built = b.build();
    let images = built.emit_sections(DictCharLenForm::Absent).expect("emit");
    assert_eq!(images.charlen_form, DictCharLenForm::Absent);
    // The write-side round-trip oracle must recompute through the form.
    let verified = built.verify_sections(&images).expect("absent round-trip");
    assert_eq!(verified, 1508);
    let abs = built.emit_sections(DictCharLenForm::Absolute).expect("emit abs");
    assert!(images.payload_section == abs.payload_section, "payload untouched");
    let d = DictSections {
        index: &images.index_section[STREAM_SECTION_HDR_LEN..],
        payload: payload_of(&images.payload_section),
        entry_count: images.entry_count,
        charlen_form: DictCharLenForm::Absent,
    };
    for probe in [
        &b"apple"[..],
        "z\u{00E9}bra".as_bytes(),
        "\u{1F600}grin".as_bytes(),
        &b""[..],
        &b"bulk-00042"[..],
    ] {
        let c = built.code_of(probe).expect("code");
        assert_eq!(raw_char_field(&images, c), 0, "NOTHING stored — zeros on the wire");
        let e = dict_entry(&d, c).expect("entry");
        assert_eq!(e.byte_len, probe.len() as u32);
        assert_eq!(
            e.char_len,
            utf8_char_count(probe),
            "absolute char_len recomputed from entry bytes"
        );
    }
}

/// Payload region of the emitted payload section (past header, before the
/// frame table).
fn payload_of(section: &[u8]) -> &[u8] {
    let hdr = StreamSectionHdr::decode(section).expect("hdr");
    let end = if hdr.frame_table_off != 0 {
        hdr.frame_table_off as usize
    } else {
        section.len()
    };
    &section[STREAM_SECTION_HDR_LEN..end]
}

// ---------------------------------------------------------------------------
// DICT-DEDUP equivalence battery (born-RED charter; the #971 pattern)
// ---------------------------------------------------------------------------
//
// The hash arm must be OUTPUT-IDENTICAL to the BTreeMap reference arm on
// every corpus: emitted section bytes (dict payloads are part bytes — the
// byte-identical-parts law), per-row observe handles/ranks, AND the counted
// distinct set (it becomes Stats-sidecar bytes through the distribution
// feed). Any divergence is a byte-law break, not a tuning delta.

/// xorshift64* — deterministic corpus generator (no external entropy).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

fn assert_arms_equal(corpus: &[Vec<u8>], sem: TextSemantics, what: &str) {
    let mut h = DictBuilder::with_hash_arm(sem, true);
    let mut r = DictBuilder::with_hash_arm(sem, false);
    for v in corpus {
        h.observe(v);
        r.observe(v);
    }
    assert_eq!(h.distinct(), r.distinct(), "distinct diverged: {what}");
    assert_eq!(h.observed_rows(), r.observed_rows());
    let (hb, rb) = (h.build(), r.build());
    // Per-row handles: same rank on every observed row (row_handles are
    // observation-order facts; both arms assign insert ids in
    // first-observation order by construction).
    for k in 0..corpus.len() {
        assert_eq!(
            hb.code_of_observed(k),
            rb.code_of_observed(k),
            "row {k} rank diverged: {what}"
        );
    }
    // Section bytes: the part-byte surface.
    let hi = hb.emit_sections(DictCharLenForm::Absolute).expect("hash emit");
    let ri = rb.emit_sections(DictCharLenForm::Absolute).expect("btree emit");
    hb.verify_sections(&hi).expect("hash round-trip");
    rb.verify_sections(&ri).expect("btree round-trip");
    assert!(
        hi.index_section == ri.index_section && hi.payload_section == ri.payload_section,
        "section bytes diverged: {what}"
    );
    // The counted distinct set: the Stats-sidecar distribution feed.
    assert_eq!(
        hb.into_counted_entries(),
        rb.into_counted_entries(),
        "counted set diverged: {what}"
    );
}

#[test]
fn arms_equal_text_families() {
    assert_arms_equal(&corpus(), TextSemantics::Utf8Chars, "module corpus");

    let mut rng = Rng(0x5EED_D1C7);
    let zipf: Vec<Vec<u8>> = (0..20_000)
        .map(|_| format!("url/{}/page", rng.next() % 4000).into_bytes())
        .collect();
    assert_arms_equal(&zipf, TextSemantics::Utf8Chars, "zipf-ish text");

    let mut rng = Rng(0x5EED_D1C8);
    let bytes: Vec<Vec<u8>> = (0..5_000)
        .map(|_| rng.next().to_le_bytes().to_vec())
        .collect();
    assert_arms_equal(&bytes, TextSemantics::BytesOnly, "all-distinct bytea");
}

#[test]
fn arms_equal_adversarial_probe_chains() {
    // Values filtered to share low h1 bits: they pile into the same
    // initial buckets, forcing long linear-probe chains and rehash
    // relocations — the collision paths the reference arm never has.
    use pgrc2_meta::hash::meta_hash128;
    let mut corpus = Vec::new();
    let mut i: u64 = 0;
    while corpus.len() < 3000 {
        let v = format!("probe-{i}").into_bytes();
        let (h1, _) = meta_hash128(&v);
        if h1 & 0x3F == 0 {
            corpus.push(v);
        }
        i += 1;
    }
    // Duplicate a slice heavily so hit-confirms (and their counts)
    // traverse the chains too.
    let dups: Vec<Vec<u8>> = corpus.iter().take(50).cloned().collect();
    for _ in 0..40 {
        corpus.extend(dups.iter().cloned());
    }
    assert_arms_equal(
        &corpus,
        TextSemantics::Utf8Chars,
        "same-bucket probe chains + dup hits",
    );
}

#[test]
fn arms_equal_growth_ladder() {
    // Enough distinct values to force many slot-table doublings from the
    // lazy 16-slot start; facts must survive every rehash.
    let corpus: Vec<Vec<u8>> = (0..60_000u64)
        .map(|i| format!("grow-{i:06}").into_bytes())
        .collect();
    assert_arms_equal(&corpus, TextSemantics::Utf8Chars, "growth ladder");
}

#[test]
fn counted_entries_are_exact_and_byte_ordered() {
    // The feed contract distribution_from_sorted_counts leans on: strictly
    // byte-ascending entries, exact per-value counts, Σ counts == observed
    // (non-null) rows.
    let mut b = DictBuilder::new(TextSemantics::Utf8Chars);
    let corpus = corpus();
    for v in &corpus {
        b.observe(v);
    }
    let observed = b.observed_rows();
    let entries = b
        .build()
        .into_counted_entries()
        .expect("rebuild path carries counts");
    assert!(entries.windows(2).all(|w| w[0].0 < w[1].0), "strict byte order");
    assert_eq!(entries.iter().map(|(_, c)| c).sum::<u64>(), observed);
    for (val, count) in &entries {
        let direct = corpus.iter().filter(|v| *v == val).count() as u64;
        assert_eq!(*count, direct, "count exactness for {val:?}");
    }
}

#[test]
fn inherit_path_offers_no_counts() {
    // D2: from_sorted_entries never observed rows — the feed must decline
    // (its distribution stays with the meta accumulator), never invent
    // counts.
    let built = crate::dict::BuiltDict::from_sorted_entries(
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        TextSemantics::Utf8Chars,
    )
    .expect("sorted entries");
    assert!(built.into_counted_entries().is_none());
}

#[test]
fn observe_handles_equal_binary_search_codes() {
    // SEAL-FUSION (walk #7b): the observe-time row handle's rank read must
    // equal the payload binary search on EVERY observed row — duplicates,
    // out-of-order arrivals, empty strings included.
    let mut b = DictBuilder::new(TextSemantics::Utf8Chars);
    let mut rows: Vec<Vec<u8>> = Vec::new();
    let mut seed = 0xD1C7_u64;
    let mut next = move || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        seed
    };
    for i in 0..5000u64 {
        let r = next();
        let payload: Vec<u8> = match r % 5 {
            0 => Vec::new(),
            1 => format!("dup-{}", r % 7).into_bytes(),
            2 => format!("zz-{:04}", r % 300).into_bytes(),
            3 => vec![0xFF, (r % 256) as u8],
            _ => format!("row-{i}").into_bytes(),
        };
        b.observe(&payload);
        rows.push(payload);
    }
    let built = b.build();
    assert!(built.is_byte_rank_sorted());
    for (k, payload) in rows.iter().enumerate() {
        assert_eq!(
            built.code_of_observed(k),
            built.code_of(payload),
            "row {k} handle rank != binary-search code"
        );
    }
    assert!(built.code_of_observed(rows.len()).is_none());
}
