//! Match-on-compressed differential teeth (M5d.fsst-moc / AP-2): the
//! compressed-domain probe must EQUAL hydrate-then-match on every corpus
//! (exactness subsumes the never-under-match law), and refuse TYPED on the
//! decode surface's corruption classes.
//!
//! Born-RED seeds (the specimens that bite any code-LOCAL matcher — one
//! that adjudicates each symbol's expansion alone):
//! - a needle LONGER than [`crate::fsst::FSST_MAX_SYMBOL_LEN`] (8 B) can
//!   never sit inside one symbol, so every one of its matches spans a code
//!   boundary;
//! - an escape-heavy corpus (table built from a FOREIGN sample) whose
//!   matches span escape/symbol seams;
//! - prefix needles longer than any first symbol.

use super::roundtrip::varlena_image;
use super::{build_stream, Built, GranuleData};
use crate::fsst::{
    contains_scalar, moc_compile, moc_probe, moc_probe_reference, FsstEncoder, FsstMatchOp,
    FsstSymbolTable,
};
use pgrc2_format::abi::EncodeInput;
use pgrc2_format::class::StorageClass;

/// Build one fsst stream from per-granule payloads (+ optional per-granule
/// validity masks), table sampled over the given sample set (the writer's
/// per-(column,part) grain).
fn build_fsst(
    payloads: &[Vec<Vec<u8>>],
    validity: &[Option<Vec<u64>>],
    sample: &[&[u8]],
) -> Built {
    let images: Vec<Vec<Vec<u8>>> = payloads
        .iter()
        .map(|g| g.iter().map(|p| varlena_image(p)).collect())
        .collect();
    let gs: Vec<GranuleData> = images
        .iter()
        .zip(validity.iter())
        .map(|(g, v)| GranuleData {
            rows: g.len() as u32,
            datums: g.iter().map(|b| b.as_ptr() as u64).collect(),
            validity: v.clone(),
        })
        .collect();
    let inputs: Vec<EncodeInput<'_>> = gs
        .iter()
        .map(|g| g.input(StorageClass::VarlenaVerbatim))
        .collect();
    let table = FsstSymbolTable::build(sample);
    let mut enc = FsstEncoder::new(table);
    build_stream(&mut enc, &inputs, 0, false)
}

/// The three-way law on one built stream: moc == hydrate-reference ==
/// direct truth over the input payloads, per granule, row-exact.
fn assert_three_way(
    built: &Built,
    payloads: &[Vec<Vec<u8>>],
    validity: &[Option<Vec<u64>>],
    op: FsstMatchOp,
    needle: &[u8],
    what: &str,
) -> u32 {
    let mut total = 0u32;
    for g in 0..payloads.len() {
        let ctx = built.ctx(g as u32);
        let prog = moc_compile(&ctx, op, needle).expect("moc compile");
        let mut moc_rows = vec![0u16; payloads[g].len()];
        let moc_n = moc_probe(&prog, &ctx, &mut moc_rows).expect("moc probe");
        let mut ref_rows = vec![0u16; payloads[g].len()];
        let mut arena_buf = vec![0u8; 1 << 20];
        let ref_n = moc_probe_reference(op, needle, &ctx, &mut arena_buf, &mut ref_rows)
            .expect("reference probe");
        assert_eq!(
            (moc_n, &moc_rows[..moc_n as usize]),
            (ref_n, &ref_rows[..ref_n as usize]),
            "{what}: granule {g} moc != hydrate-reference (op {op:?} needle {:?})",
            String::from_utf8_lossy(needle)
        );
        // Direct truth over the INPUT payloads (validity-gated).
        let truth: Vec<u16> = (0..payloads[g].len())
            .filter(|&r| {
                let valid = match &validity[g] {
                    None => true,
                    Some(words) => (words[r / 64] >> (r % 64)) & 1 == 1,
                };
                valid
                    && match op {
                        FsstMatchOp::NeEmpty => !payloads[g][r].is_empty(),
                        FsstMatchOp::Prefix => payloads[g][r].starts_with(needle),
                        FsstMatchOp::Contains => contains_scalar(&payloads[g][r], needle),
                    }
            })
            .map(|r| r as u16)
            .collect();
        assert_eq!(
            &moc_rows[..moc_n as usize],
            truth.as_slice(),
            "{what}: granule {g} moc != direct truth (op {op:?} needle {:?})",
            String::from_utf8_lossy(needle)
        );
        total += moc_n;
    }
    total
}

/// URL-shaped corpus: ASCII with planted needles, incl. the >8B
/// cross-boundary specimen. Two granules (1024 + 700 rows).
fn url_corpus() -> (Vec<Vec<Vec<u8>>>, Vec<Option<Vec<u64>>>) {
    let gran = |rows: usize, base: usize| -> Vec<Vec<u8>> {
        (0..rows)
            .map(|i| {
                let k = base + i;
                match k % 7 {
                    0 => format!("http://example{k}.ru/search?q=google+maps&p={k}"),
                    1 => format!("http://clickbenchmarks{k}.org/rank/{k}/details"),
                    2 => format!("https://cdn{k}.example.com/assets/{k}/logo.png"),
                    3 => String::new(), // empty value
                    4 => format!("http://m{k}.ru/path/clickbenchmarks?id={k}"),
                    5 => format!("gopher://old{k}.net/goggle-not-google-{k}"),
                    _ => format!("http://site{k}.ru/index-{k}.html"),
                }
                .into_bytes()
            })
            .collect()
    };
    (vec![gran(1024, 0), gran(700, 5000)], vec![None, None])
}

#[test]
fn moc_contains_crosses_code_boundaries() {
    let (payloads, validity) = url_corpus();
    let sample: Vec<&[u8]> = payloads.iter().flatten().map(|p| p.as_slice()).collect();
    let built = build_fsst(&payloads, &validity, &sample);
    // The born-RED specimen: "clickbenchmarks" is 15 B — no ≤8B symbol
    // can hold it, so EVERY hit spans a code boundary.
    let hits =
        assert_three_way(&built, &payloads, &validity, FsstMatchOp::Contains, b"clickbenchmarks", "url/cross-boundary");
    assert!(hits > 0, "the cross-boundary specimen must have matches to bite on");
    // The q20 literal; the goggle-not-google row guards over-matching.
    let hits =
        assert_three_way(&built, &payloads, &validity, FsstMatchOp::Contains, b"google", "url/google");
    assert!(hits > 0);
    // Absent needle: zero hits (the over-match direction).
    let hits =
        assert_three_way(&built, &payloads, &validity, FsstMatchOp::Contains, b"zzz-absent-needle", "url/absent");
    assert_eq!(hits, 0);
    // Self-overlapping needle (KMP fallback correctness).
    assert_three_way(&built, &payloads, &validity, FsstMatchOp::Contains, b"ogog", "url/overlap");
}

#[test]
fn moc_prefix_and_neempty_agree() {
    let (payloads, validity) = url_corpus();
    let sample: Vec<&[u8]> = payloads.iter().flatten().map(|p| p.as_slice()).collect();
    let built = build_fsst(&payloads, &validity, &sample);
    // Prefix longer than any plausible first symbol (12 B).
    let hits =
        assert_three_way(&built, &payloads, &validity, FsstMatchOp::Prefix, b"http://exampl", "url/prefix-long");
    assert!(hits > 0);
    assert_three_way(&built, &payloads, &validity, FsstMatchOp::Prefix, b"https://", "url/prefix-https");
    let hits = assert_three_way(&built, &payloads, &validity, FsstMatchOp::Prefix, b"ftp://", "url/prefix-absent");
    assert_eq!(hits, 0);
    let ne = assert_three_way(&built, &payloads, &validity, FsstMatchOp::NeEmpty, b"", "url/neempty");
    assert!(ne > 0 && ne < 1724, "empties exist and are excluded");
    // Empty needle: LIKE '%%' — every valid row matches (empties included).
    let all = assert_three_way(&built, &payloads, &validity, FsstMatchOp::Contains, b"", "url/empty-needle");
    assert_eq!(all, 1724);
}

#[test]
fn moc_cyrillic_and_validity_gate() {
    // Cyrillic (2-byte chars): "поиск" is a 10-B needle; nulls interleaved
    // (validity Mixed) must never match any op.
    let phrases: Vec<Vec<u8>> = (0..900usize)
        .map(|i| match i % 5 {
            0 => format!("яндекс поиск картинок {i}").into_bytes(),
            1 => format!("новости дня {i}").into_bytes(),
            2 => format!("поиск-{i}").into_bytes(),
            3 => Vec::new(),
            _ => format!("прогноз погоды на {i} дней").into_bytes(),
        })
        .collect();
    let mut words = vec![u64::MAX; 900usize.div_ceil(64)];
    for r in (0..900).step_by(9) {
        words[r / 64] &= !(1u64 << (r % 64)); // every 9th row null
    }
    let payloads = vec![phrases];
    let validity = vec![Some(words)];
    let sample: Vec<&[u8]> = payloads.iter().flatten().map(|p| p.as_slice()).collect();
    let built = build_fsst(&payloads, &validity, &sample);
    let hits = assert_three_way(
        &built, &payloads, &validity, FsstMatchOp::Contains,
        "поиск".as_bytes(), "cyr/contains",
    );
    assert!(hits > 0);
    assert_three_way(&built, &payloads, &validity, FsstMatchOp::Prefix, "пои".as_bytes(), "cyr/prefix");
    assert_three_way(&built, &payloads, &validity, FsstMatchOp::NeEmpty, b"", "cyr/neempty");
}

#[test]
fn moc_escape_heavy_matches_span_escapes() {
    // Table built from a FOREIGN sample: the values encode mostly as
    // escapes, so matches span escape/symbol seams.
    let foreign: Vec<Vec<u8>> = (0..64usize)
        .map(|i| format!("unrelated-sample-{i}").into_bytes())
        .collect();
    let sample: Vec<&[u8]> = foreign.iter().map(|p| p.as_slice()).collect();
    let payloads = vec![(0..512usize)
        .map(|i| {
            if i % 3 == 0 {
                format!("\u{1}\u{2}needle{i}\u{3}").into_bytes()
            } else {
                format!("\u{4}\u{5}ndl-{i}").into_bytes()
            }
        })
        .collect::<Vec<_>>()];
    let validity = vec![None];
    let built = build_fsst(&payloads, &validity, &sample);
    let hits =
        assert_three_way(&built, &payloads, &validity, FsstMatchOp::Contains, b"needle", "esc/contains");
    assert!(hits > 0);
}

#[test]
fn moc_needle_cap_refuses_typed() {
    let (payloads, validity) = url_corpus();
    let sample: Vec<&[u8]> = payloads.iter().flatten().map(|p| p.as_slice()).collect();
    let built = build_fsst(&payloads, &validity, &sample);
    let ctx = built.ctx(0);
    let long = vec![b'x'; crate::fsst::FSST_MOC_MAX_NEEDLE + 1];
    assert!(
        moc_compile(&ctx, FsstMatchOp::Contains, &long).is_err(),
        "needle beyond the cap must decline typed (the hydrate arm owns it)"
    );
}

/// Corruption seeds (AD-4 class): the probe walks the same validated
/// surface as decode — an out-of-table code and a dangling escape refuse
/// TYPED, never UB, never a silent verdict.
#[test]
fn moc_corrupt_stream_refuses_typed() {
    // All-nonempty single granule so the LAST byte of the frame is the
    // final value's comp tail (mutating it corrupts exactly one stream).
    let payloads = vec![(0..256usize)
        .map(|i| format!("value-{i}-payload").into_bytes())
        .collect::<Vec<_>>()];
    let validity = vec![None];
    let sample: Vec<&[u8]> = payloads.iter().flatten().map(|p| p.as_slice()).collect();
    let built = build_fsst(&payloads, &validity, &sample);
    let ctx = built.ctx(0);
    // An ABSENT needle: the walk must consume EVERY comp byte (no early
    // exit), so the corrupted tail is reached — the -297e job's lesson:
    // a matching needle exits before the tail and the seed never fires.
    let prog = moc_compile(&ctx, FsstMatchOp::Contains, b"zz-absent-zz").expect("compile");
    // Sanity: clean probe passes with zero hits.
    let mut rows = vec![0u16; 256];
    assert_eq!(moc_probe(&prog, &ctx, &mut rows).expect("clean probe"), 0);
    // The last PAYLOAD byte is the final value's comp tail (the payload
    // region ends at the frame table). Two-mutation parity law: if that
    // byte is a CODE position, := 255 leaves a dangling escape; if it is
    // an escape's LITERAL, its predecessor IS the escape (a code
    // position), so := out-of-table there fires the code refusal. One of
    // the two must refuse typed.
    let hdr = pgrc2_format::part::StreamSectionHdr::decode(built.section()).expect("hdr");
    let last = if hdr.frame_table_off != 0 {
        hdr.frame_table_off as usize - 1
    } else {
        built.section().len() - 1
    };
    let sample2: Vec<&[u8]> = payloads.iter().flatten().map(|p| p.as_slice()).collect();
    let nsym = FsstSymbolTable::build(&sample2).nsymbols() as u8;
    assert!(nsym < 255, "corpus keeps the code domain under the escape");
    let mut c1 = built.section().to_vec();
    c1[last] = 255;
    let e1 = {
        let cctx = built.ctx_with(0, built.key(), &c1, built.frame_table.as_deref());
        moc_probe(&prog, &cctx, &mut rows)
    };
    let mut c2 = built.section().to_vec();
    c2[last - 1] = nsym; // out-of-table, non-escape
    let e2 = {
        let cctx = built.ctx_with(0, built.key(), &c2, built.frame_table.as_deref());
        moc_probe(&prog, &cctx, &mut rows)
    };
    assert!(
        e1.is_err() || e2.is_err(),
        "a corrupt tail must refuse typed on at least one parity arm (got {e1:?} / {e2:?})"
    );
}
