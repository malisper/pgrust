//! C0 proof tests: the round-trip property `reconstruct(shred(manifest, x))
//! == x` (byte-exact against jsonb_in's image) over generated corpora for
//! both hint-elected and frequency-elected manifests, election determinism,
//! per-granule exception-mask accuracy, and the residual's bucketed
//! partial-access promise. Every batch also asserts the §2.4 clean-granule
//! whole-truth invariant, and coverage counters keep the volume run
//! non-vacuous (gate-blindness law: a gate that can't see is VOID).

use std::sync::Once;

use crate::chunk::ShredChunk;
use crate::elect::{elect_manifest, PathHint, ShredBudgets};
use crate::manifest::{Lane, ManifestError, ShredManifest};
use crate::path::JsonPath;
use crate::reconstruct::{reconstruct_row, reconstruct_rows};
use crate::testgen::{gen_doc, gen_s4, gen_scalar, gen_uuid, lcg, volume_hints};
use crate::shred::shred_chunk;
use adt_jsonb::getfield;
use adt_jsonb::io;
use mbutils::SetDatabaseEncoding;
use mcx::{Mcx, MemoryContext};
use wchar::PG_UTF8;

fn setup() {
    let _ = SetDatabaseEncoding(PG_UTF8);
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        mbutils::init_seams();
    });
}

fn jsonb_image(mcx: Mcx<'_>, doc: &str) -> Vec<u8> {
    io::jsonb_in(mcx, doc.as_bytes(), None)
        .unwrap_or_else(|e| panic!("jsonb_in failed on {doc:?}: {}", e.message()))
        .expect("hard path returns Some")[..]
        .to_vec()
}

#[derive(Default, Debug)]
struct Coverage {
    rows: u64,
    text_held: u64,
    uuid_held: u64,
    num_held: u64,
    bool_held: u64,
    exception_granules: u64,
    residual_entries: u64,
    root_rows: u64,
    all_residual_rows: u64,
}

/// Full pipeline over one batch with per-row byte-exactness, random-access
/// parity, and the clean-granule whole-truth invariant.
fn roundtrip_batch(
    docs: &[String],
    manifest: &mut ShredManifest,
    granule_rows: u32,
    cov: &mut Coverage,
) -> ShredChunk {
    let ctx = MemoryContext::new("shred-test");
    let mcx = ctx.mcx();
    let images: Vec<Vec<u8>> = docs.iter().map(|d| jsonb_image(mcx, d)).collect();
    let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();

    let chunk = shred_chunk(mcx, manifest, &payloads, granule_rows).expect("shred");
    let recon = reconstruct_rows(mcx, manifest, &chunk).expect("reconstruct");
    assert_eq!(recon.len(), images.len());
    for (r, (got, want)) in recon.iter().zip(&images).enumerate() {
        assert_eq!(
            got, want,
            "round-trip mismatch at row {r}: doc {:?}",
            docs[r]
        );
    }
    // Random-access parity on a sample.
    for probe in [0, docs.len() / 2, docs.len() - 1] {
        let got = reconstruct_row(mcx, manifest, &chunk, probe as u32).expect("row");
        assert_eq!(got, images[probe], "random-access row {probe}");
    }

    // §2.4 whole-truth: a clean granule's typed lane + validity are the
    // entire story for that path — no residual entry may exist there.
    for (li, lane) in chunk.lanes.iter().enumerate() {
        assert_eq!(
            lane.validity.count(),
            lane.values.count(),
            "dense values must match validity"
        );
        assert_eq!(lane.exceptions.len(), chunk.n_granules());
        let path = manifest.path(manifest.elected[li].path_id).clone();
        for r in 0..chunk.nrows {
            let g = r / chunk.granule_rows;
            let hit = chunk.residual_get(manifest, r, &path).is_some();
            if !lane.exceptions.get(g) {
                assert!(
                    !hit,
                    "clean granule {g} holds a residual entry for elected path {path:?} at row {r}"
                );
            }
            assert!(
                !(hit && lane.validity.get(r)),
                "row {r} is both lane-valid and residual for {path:?}"
            );
        }
    }

    // Coverage accounting.
    cov.rows += chunk.nrows as u64;
    for (li, lane) in chunk.lanes.iter().enumerate() {
        let held = lane.validity.count() as u64;
        match manifest.elected[li].lane {
            Lane::Text => cov.text_held += held,
            Lane::Uuid16 => cov.uuid_held += held,
            Lane::NumericFs => cov.num_held += held,
            Lane::Bool => cov.bool_held += held,
        }
        cov.exception_granules += lane.exceptions.count() as u64;
    }
    let root = JsonPath::root();
    for r in 0..chunk.nrows {
        let mut nres = 0u64;
        for b in &chunk.buckets {
            nres += b.sizes[r as usize] as u64;
        }
        cov.residual_entries += nres;
        if chunk.residual_get(manifest, r, &root).is_some() {
            cov.root_rows += 1;
        }
        if nres > 0 && chunk.lanes.iter().all(|l| !l.validity.get(r)) {
            cov.all_residual_rows += 1;
        }
    }
    chunk
}

fn payload_vecs(mcx: Mcx<'_>, docs: &[String]) -> Vec<Vec<u8>> {
    docs.iter().map(|d| jsonb_image(mcx, d)).collect()
}

/// The volume gate: >= 100k randomized documents through the round-trip,
/// alternating manifest modes: frequency-elected, hint-elected (election
/// saw NO rows — everything late-interns), and conform (elected over the
/// batch's first half + hints, shredding the whole batch — the later-RG
/// story).
#[test]
fn roundtrip_property_100k() {
    setup();
    let mut st: u64 = 0x243F6A8885A308D3;
    let budgets = ShredBudgets::default();
    let hints = volume_hints();
    let mut cov = Coverage::default();
    const BATCHES: usize = 64;
    const BATCH_ROWS: usize = 1600;
    for batch in 0..BATCHES {
        let docs: Vec<String> = (0..BATCH_ROWS).map(|_| gen_doc(&mut st)).collect();
        let mut manifest = {
            let ctx = MemoryContext::new("elect");
            let mcx = ctx.mcx();
            let images = payload_vecs(mcx, &docs);
            let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();
            match batch % 3 {
                0 => elect_manifest(&payloads, &[], &budgets),
                1 => elect_manifest(&[], &hints, &budgets),
                _ => elect_manifest(&payloads[..payloads.len() / 2], &hints, &budgets),
            }
        };
        roundtrip_batch(&docs, &mut manifest, 512, &mut cov);
    }
    assert!(cov.rows >= 100_000, "volume floor: {}", cov.rows);
    eprintln!("volume coverage: {cov:?}");
    // Anti-vacuity: every lane class engaged, exceptions fired, residual
    // carried rows, non-object roots occurred, all-residual rows occurred.
    assert!(cov.text_held > 0, "{cov:?}");
    assert!(cov.uuid_held > 0, "{cov:?}");
    assert!(cov.num_held > 0, "{cov:?}");
    assert!(cov.bool_held > 0, "{cov:?}");
    assert!(cov.exception_granules > 0, "{cov:?}");
    assert!(cov.residual_entries > 0, "{cov:?}");
    assert!(cov.root_rows > 0, "{cov:?}");
    assert!(cov.all_residual_rows > 0, "{cov:?}");
}

/// Real granule geometry: one multi-granule chunk at GRANULE_ROWS = 8192.
#[test]
fn roundtrip_multi_granule_real_geometry() {
    setup();
    let mut st: u64 = 0x13198A2E03707344;
    let docs: Vec<String> = (0..20_000)
        .map(|i| {
            if i % 3 == 0 {
                gen_s4(&mut st)
            } else {
                gen_doc(&mut st)
            }
        })
        .collect();
    let mut manifest = {
        let ctx = MemoryContext::new("elect");
        let mcx = ctx.mcx();
        let images = payload_vecs(mcx, &docs);
        let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();
        elect_manifest(&payloads, &volume_hints(), &ShredBudgets::default())
    };
    let mut cov = Coverage::default();
    let chunk = roundtrip_batch(&docs, &mut manifest, crate::chunk::GRANULE_ROWS, &mut cov);
    assert_eq!(chunk.n_granules(), 3);
}

#[test]
fn targeted_shapes_roundtrip() {
    setup();
    let docs: Vec<String> = [
        "{}",
        "[]",
        "\"x\"",
        "5",
        "-0",
        "true",
        "null",
        "[1, {\"a\": 2}, []]",
        "{\"a\": {}}",
        "{\"a\": []}",
        "{\"a\": 1, \"a\": 2}",
        "{\"b\": 1, \"a\": 1, \"b\": {\"c\": 2}}",
        "{\"日本語\": {\"ключ\": \"значение\", \"🚀\": [1, 2]}}",
        "{\"a\": {\"b\": {\"c\": {\"d\": {\"e\": {\"f\": 1}}}}}}",
        "{\"n\": 1.50, \"m\": 0.10}",
        "{\"n\": 1e-15}",
        "{\"n\": 123456789012345678901234567890123456}",
        "{\"a\": null}",
        r#"{"source": "sequence", "sequenceId": "0708c9e6-08c1-4a44-a4a4-3d1eb1e78563", "sequenceStateId": "b9f22563-1b1c-4c7e-98e6-425f0d5b194e", "sequenceStepId": "step_1"}"#,
        r#"{"k": "quote\"and\\backslash\tandé"}"#,
        "{\"\": 1}",
        "{\"a\": [[[1], []], {\"x\": [true, null]}]}",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let mut cov = Coverage::default();
    // Frequency election over the tiny corpus (floor = 1).
    let mut freq = {
        let ctx = MemoryContext::new("elect");
        let mcx = ctx.mcx();
        let images = payload_vecs(mcx, &docs);
        let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();
        elect_manifest(&payloads, &[], &ShredBudgets::default())
    };
    roundtrip_batch(&docs, &mut freq, 4, &mut cov);
    // Hint-elected (election saw nothing; hints only).
    let mut hinted = elect_manifest(&[], &volume_hints(), &ShredBudgets::default());
    roundtrip_batch(&docs, &mut hinted, 4, &mut cov);
    assert!(cov.root_rows > 0);
}

/// S4 shape (memo §5): hinted manifests elect identically regardless of
/// skew; the residual carries only what election didn't cover; type-stable
/// data produces zero exceptions.
#[test]
fn s4_shape_hinted_and_frequency() {
    setup();
    let mut st: u64 = 0xA4093822299F31D0;
    let docs: Vec<String> = (0..2000).map(|_| gen_s4(&mut st)).collect();

    let s4_hints = vec![
        PathHint {
            path: JsonPath::from_dotted("source"),
            lane: Lane::Text,
        },
        PathHint {
            path: JsonPath::from_dotted("sequenceId"),
            lane: Lane::Uuid16,
        },
        PathHint {
            path: JsonPath::from_dotted("sequenceStateId"),
            lane: Lane::Uuid16,
        },
        PathHint {
            path: JsonPath::from_dotted("sequenceStepId"),
            lane: Lane::Text,
        },
    ];

    // Hinted: elected from hints alone.
    let mut cov = Coverage::default();
    let mut hinted = elect_manifest(&[], &s4_hints, &ShredBudgets::default());
    let chunk = roundtrip_batch(&docs, &mut hinted, 512, &mut cov);
    assert!(cov.uuid_held > 0 && cov.text_held > 0);
    // Type-stable data: no exceptions anywhere.
    assert_eq!(cov.exception_granules, 0);
    // Residual carries exactly the import rows' importId.
    let import_id = JsonPath::from_dotted("importId");
    let n_import = docs.iter().filter(|d| d.contains("\"import\"")).count() as u64;
    assert!(n_import > 0, "generator must produce import rows");
    assert_eq!(cov.residual_entries, n_import);
    let mut import_hits = 0u64;
    for r in 0..chunk.nrows {
        if chunk.residual_get(&hinted, r, &import_id).is_some() {
            import_hits += 1;
        }
    }
    assert_eq!(import_hits, n_import);

    // Frequency: importId (5% presence) clears the floor too — the memo §5
    // walk-through — and the residual is completely empty.
    let mut freq = {
        let ctx = MemoryContext::new("elect");
        let mcx = ctx.mcx();
        let images = payload_vecs(mcx, &docs);
        let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();
        elect_manifest(&payloads, &[], &ShredBudgets::default())
    };
    assert!(freq
        .elected
        .iter()
        .any(|e| freq.path(e.path_id) == &import_id));
    let mut cov2 = Coverage::default();
    let chunk2 = roundtrip_batch(&docs, &mut freq, 512, &mut cov2);
    assert_eq!(cov2.residual_entries, 0);
    assert_eq!(cov2.exception_granules, 0);
    for b in &chunk2.buckets {
        assert!(b.sizes.iter().all(|&s| s == 0));
    }
}

#[test]
fn election_is_deterministic() {
    setup();
    let mut st: u64 = 0x082EFA98EC4E6C89;
    let docs: Vec<String> = (0..500).map(|_| gen_doc(&mut st)).collect();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let images = payload_vecs(mcx, &docs);
    let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();
    let budgets = ShredBudgets::default();
    let hints = volume_hints();

    let m1 = elect_manifest(&payloads, &hints, &budgets);
    let m2 = elect_manifest(&payloads, &hints, &budgets);
    assert_eq!(m1, m2);
    assert_eq!(m1.serialize(), m2.serialize());

    // Identical inputs shred to identical chunks (serial re-ingest parity).
    let (mut ma, mut mb) = (m1.clone(), m2.clone());
    let ca = shred_chunk(mcx, &mut ma, &payloads, 512).expect("shred");
    let cb = shred_chunk(mcx, &mut mb, &payloads, 512).expect("shred");
    assert_eq!(ca, cb);
    assert_eq!(ma, mb);
}

#[test]
fn election_caps_hold() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    // Prefix-freeness: hinting both a and a.b admits only the first.
    let hints = vec![
        PathHint {
            path: JsonPath::from_dotted("a"),
            lane: Lane::Text,
        },
        PathHint {
            path: JsonPath::from_dotted("a.b"),
            lane: Lane::Text,
        },
    ];
    let m = elect_manifest(&[], &hints, &ShredBudgets::default());
    assert_eq!(m.elected.len(), 1);
    assert_eq!(m.path(m.elected[0].path_id), &JsonPath::from_dotted("a"));

    // Depth budget refuses too-deep hints.
    let deep_hint = vec![PathHint {
        path: JsonPath::from_dotted("a.b.c.d.e"),
        lane: Lane::Text,
    }];
    let m = elect_manifest(&[], &deep_hint, &ShredBudgets::default());
    assert!(m.elected.is_empty());

    // Path budget caps frequency election.
    let docs: Vec<String> = (0..64)
        .map(|_| {
            let body: Vec<String> = (0..20).map(|k| format!("\"k{k}\": 1")).collect();
            format!("{{{}}}", body.join(", "))
        })
        .collect();
    let images = payload_vecs(mcx, &docs);
    let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();
    let m = elect_manifest(
        &payloads,
        &[],
        &ShredBudgets {
            max_paths: 5,
            ..ShredBudgets::default()
        },
    );
    assert_eq!(m.elected.len(), 5);

    // The presence floor (anti-CH-Object): unique-path spam elects nothing.
    let mut st: u64 = 1;
    let spam: Vec<String> = (0..256)
        .map(|i| format!("{{\"unique_{i}_{}\": 1}}", lcg(&mut st)))
        .collect();
    let images = payload_vecs(mcx, &spam);
    let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();
    let m = elect_manifest(&payloads, &[], &ShredBudgets::default());
    assert!(m.elected.is_empty());
}

#[test]
fn manifest_serde_roundtrip_and_validation() {
    setup();
    let mut st: u64 = 0x452821E638D01377;
    let docs: Vec<String> = (0..300).map(|_| gen_doc(&mut st)).collect();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let images = payload_vecs(mcx, &docs);
    let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();
    let mut m = elect_manifest(&payloads, &volume_hints(), &ShredBudgets::default());
    assert!(!m.elected.is_empty());

    let bytes = m.serialize();
    let back = ShredManifest::deserialize(&bytes).expect("deserialize");
    assert_eq!(m, back);

    // A manifest grown by late interning (the conform story) still
    // round-trips.
    let extra: Vec<String> = (0..50)
        .map(|i| format!("{{\"late_{i}\": [1]}}"))
        .collect();
    let extra_images = payload_vecs(mcx, &extra);
    let extra_payloads: Vec<&[u8]> = extra_images.iter().map(|i| &i[4..]).collect();
    let dict_before = m.dict().len();
    shred_chunk(mcx, &mut m, &extra_payloads, 512).expect("shred");
    assert!(m.dict().len() > dict_before, "late paths must intern");
    let bytes2 = m.serialize();
    assert_eq!(ShredManifest::deserialize(&bytes2).expect("de"), m);

    // Validation refusals.
    assert_eq!(
        ShredManifest::deserialize(&bytes[..bytes.len() - 1]),
        Err(ManifestError::Truncated)
    );
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(
        ShredManifest::deserialize(&trailing),
        Err(ManifestError::TrailingBytes)
    );
    let mut bad_version = bytes.clone();
    bad_version[0] = 99;
    assert_eq!(
        ShredManifest::deserialize(&bad_version),
        Err(ManifestError::BadVersion(99))
    );
    // Handcrafted: elected path_id out of dictionary range.
    let mut craft = vec![1u8, 8, 0, 0, 4, 0, 0, 0];
    craft.extend_from_slice(&0u32.to_le_bytes()); // empty dict
    craft.extend_from_slice(&1u32.to_le_bytes()); // one elected
    craft.extend_from_slice(&0u32.to_le_bytes()); // path_id 0 >= dict len 0
    craft.extend_from_slice(&[0u8, 0, 0, 0, 0]); // lane, idx, hinted, reserved
    craft.extend_from_slice(&0u64.to_le_bytes()); // presence
    craft.extend_from_slice(&0u16.to_le_bytes()); // residual base
    assert_eq!(
        ShredManifest::deserialize(&craft),
        Err(ManifestError::BadPathId(0))
    );
}

/// Exception masks are granule-exact, and exceptions coexist with clean
/// granules of the same lane (memo §2.4).
#[test]
fn exception_masks_are_granule_exact() {
    setup();
    // granule_rows = 4; 12 rows in 3 granules. Path p: strings except rows
    // 5 and 6 (numbers — exceptions in granule 1 only). Path q: bool lane
    // via hints, with a jsonb null at row 9 (exception in granule 2).
    let mut docs: Vec<String> = Vec::new();
    for i in 0..12 {
        let p = if i == 5 || i == 6 {
            "42".to_string()
        } else {
            format!("\"s{i}\"")
        };
        let q = if i == 9 { "null" } else { "true" };
        docs.push(format!("{{\"p\": {p}, \"q\": {q}}}"));
    }
    let hints = vec![
        PathHint {
            path: JsonPath::from_dotted("p"),
            lane: Lane::Text,
        },
        PathHint {
            path: JsonPath::from_dotted("q"),
            lane: Lane::Bool,
        },
    ];
    let mut manifest = elect_manifest(&[], &hints, &ShredBudgets::default());
    let mut cov = Coverage::default();
    let chunk = roundtrip_batch(&docs, &mut manifest, 4, &mut cov);

    let p_idx = manifest
        .elected
        .iter()
        .position(|e| manifest.path(e.path_id) == &JsonPath::from_dotted("p"))
        .unwrap();
    let q_idx = 1 - p_idx;
    let p_lane = &chunk.lanes[p_idx];
    let q_lane = &chunk.lanes[q_idx];
    assert_eq!(
        (0..3).map(|g| p_lane.exceptions.get(g)).collect::<Vec<_>>(),
        vec![false, true, false]
    );
    assert_eq!(
        (0..3).map(|g| q_lane.exceptions.get(g)).collect::<Vec<_>>(),
        vec![false, false, true]
    );
    assert_eq!(p_lane.validity.count(), 10);
    assert_eq!(q_lane.validity.count(), 11);
    // The exceptions are IN the residual at exactly their paths.
    assert!(chunk
        .residual_get(&manifest, 5, &JsonPath::from_dotted("p"))
        .is_some());
    assert!(chunk
        .residual_get(&manifest, 9, &JsonPath::from_dotted("q"))
        .is_some());
    assert!(chunk
        .residual_get(&manifest, 0, &JsonPath::from_dotted("p"))
        .is_none());
}

/// The §2.5 bucketed promise: probing one path reads exactly one bucket.
#[test]
fn residual_partial_access_is_bucket_isolated() {
    setup();
    let mut st: u64 = 0xBE5466CF34E90C6C;
    // 30 distinct keys per row, nothing elected (max_paths = 0).
    let keys: Vec<String> = (0..30).map(|i| format!("rk{i}")).collect();
    let docs: Vec<String> = (0..64)
        .map(|_| {
            let body: Vec<String> = keys
                .iter()
                .map(|k| format!("\"{k}\": {}", gen_scalar(&mut st)))
                .collect();
            format!("{{{}}}", body.join(", "))
        })
        .collect();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let images = payload_vecs(mcx, &docs);
    let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();
    let mut manifest = elect_manifest(
        &payloads,
        &[],
        &ShredBudgets {
            max_paths: 0,
            ..ShredBudgets::default()
        },
    );
    assert!(manifest.elected.is_empty());
    let chunk = shred_chunk(mcx, &mut manifest, &payloads, 512).expect("shred");

    // Oracle: the stored residual image at a top-level key must equal the
    // `->` operator's output image over the original document.
    for (r, payload) in payloads.iter().enumerate() {
        for k in &keys {
            let path = JsonPath::new(vec![k.as_bytes().to_vec()]);
            let got = chunk
                .residual_get(&manifest, r as u32, &path)
                .unwrap_or_else(|| panic!("row {r} missing {k}"));
            let want = getfield::object_field(mcx, payload, k.as_bytes())
                .expect("object_field")
                .expect("key present");
            assert_eq!(got, &want[..], "residual image vs -> at row {r} key {k}");
        }
    }

    // Bucket isolation witness: clobber every OTHER bucket; the probe of the
    // target path is unaffected (it never touches them), and a path living
    // in a clobbered bucket now misreads — proof the clobber had teeth.
    let target = JsonPath::new(vec![keys[0].as_bytes().to_vec()]);
    let tb = target.bucket(manifest.nbuckets) as usize;
    let other = keys
        .iter()
        .find(|k| {
            JsonPath::new(vec![k.as_bytes().to_vec()]).bucket(manifest.nbuckets) as usize != tb
        })
        .expect("30 keys span more than one bucket");
    let other_path = JsonPath::new(vec![other.as_bytes().to_vec()]);

    let clean = chunk
        .residual_get(&manifest, 0, &target)
        .unwrap()
        .to_vec();
    let other_clean = chunk
        .residual_get(&manifest, 0, &other_path)
        .unwrap()
        .to_vec();
    let mut clobbered = chunk.clone();
    for (b, bucket) in clobbered.buckets.iter_mut().enumerate() {
        if b != tb {
            for byte in bucket.val_bytes.iter_mut() {
                *byte = 0xAA;
            }
        }
    }
    assert_eq!(
        clobbered.residual_get(&manifest, 0, &target).unwrap(),
        &clean[..],
        "probe touched a foreign bucket"
    );
    assert_ne!(
        clobbered.residual_get(&manifest, 0, &other_path).unwrap(),
        &other_clean[..],
        "clobber witness is vacuous"
    );
}

/// Numeric lane specifics: uniform dscale packs whole rows; mixed dscale and
/// mantissa overflow are per-value exceptions; display scale survives
/// byte-exactly (the A6b contract through jsonb).
#[test]
fn numeric_lane_dscale_and_exceptions() {
    setup();
    let uniform: Vec<String> = (0..8).map(|i| format!("{{\"n\": {i}.25}}")).collect();
    let hints = vec![PathHint {
        path: JsonPath::from_dotted("n"),
        lane: Lane::NumericFs,
    }];
    let mut manifest = elect_manifest(&[], &hints, &ShredBudgets::default());
    let mut cov = Coverage::default();
    let chunk = roundtrip_batch(&uniform, &mut manifest, 4, &mut cov);
    assert_eq!(chunk.lanes[0].validity.count(), 8);
    assert_eq!(cov.exception_granules, 0);

    // Mixed dscale + overflow: held where the chunk scale fits, exceptions
    // elsewhere; every row still byte-exact.
    let mixed: Vec<String> = vec![
        "{\"n\": 1.50}".into(),                                 // elects S=2
        "{\"n\": 2.75}".into(),                                 // fits
        "{\"n\": 1.5}".into(),                                  // dscale 1: exception
        "{\"n\": 3}".into(),                                    // dscale 0: exception
        "{\"n\": 123456789012345678901234567890123456}".into(), // overflow: exception
        "{\"n\": 99.99}".into(),                                // fits
    ];
    let mut manifest = elect_manifest(&[], &hints, &ShredBudgets::default());
    let mut cov = Coverage::default();
    let chunk = roundtrip_batch(&mixed, &mut manifest, 4, &mut cov);
    assert_eq!(chunk.lanes[0].validity.count(), 3);
    assert!(cov.exception_granules > 0);
    match &chunk.lanes[0].values {
        crate::chunk::LaneValues::NumericFs { scale, packed } => {
            assert_eq!(*scale, 2);
            assert_eq!(packed, &vec![150, 275, 9999]);
        }
        other => panic!("wrong lane values: {other:?}"),
    }
}

/// The electable-scale cap (C1 storage contract, C2 memo §7 ask 3): a first
/// numeric occurrence whose dscale exceeds FS_ELECT_SCALE_MAX must be an
/// exception WITHOUT electing the chunk scale — the next electable
/// occurrence elects instead, and the elected scale always fits a u8. The
/// over-cap rows still round-trip byte-exactly (residual).
#[test]
fn numeric_lane_scale_cap() {
    setup();
    // dscale 300 (> 255): 0.000...0 with 300 fractional digits.
    let over_cap = format!("{{\"n\": 0.{}}}", "0".repeat(300));
    let docs: Vec<String> = vec![
        over_cap.clone(),      // dscale 300: exception, must NOT elect
        "{\"n\": 1.50}".into(), // elects S=2
        "{\"n\": 2.25}".into(), // fits
        over_cap,              // still an exception at S=2
    ];
    let hints = vec![PathHint {
        path: JsonPath::from_dotted("n"),
        lane: Lane::NumericFs,
    }];
    let mut manifest = elect_manifest(&[], &hints, &ShredBudgets::default());
    let mut cov = Coverage::default();
    let chunk = roundtrip_batch(&docs, &mut manifest, 4, &mut cov);
    assert_eq!(chunk.lanes[0].validity.count(), 2, "two fitting rows held");
    assert!(cov.exception_granules > 0, "over-cap rows are exceptions");
    match &chunk.lanes[0].values {
        crate::chunk::LaneValues::NumericFs { scale, packed } => {
            assert_eq!(*scale, 2, "election skipped the over-cap dscale");
            assert!(*scale <= crate::shred::FS_ELECT_SCALE_MAX);
            assert_eq!(packed, &vec![150, 225]);
        }
        other => panic!("wrong lane values: {other:?}"),
    }
}

/// Uuid lane specifics: only canonical lowercase-hyphenated forms enter
/// Fixed16; uppercase/braced/garbage forms are residual exceptions; all
/// reconstruct byte-exactly.
#[test]
fn uuid_lane_canonicality() {
    setup();
    let mut st: u64 = 0xC0AC29B7C97C50DD;
    let canon = gen_uuid(&mut st, 0);
    let upper = gen_uuid(&mut st, 1);
    let braced = gen_uuid(&mut st, 2);
    let docs: Vec<String> = vec![
        format!("{{\"id\": \"{canon}\"}}"),
        format!("{{\"id\": \"{upper}\"}}"),
        format!("{{\"id\": \"{braced}\"}}"),
        "{\"id\": \"not-a-uuid\"}".to_string(),
        "{\"id\": 7}".to_string(),
    ];
    let hints = vec![PathHint {
        path: JsonPath::from_dotted("id"),
        lane: Lane::Uuid16,
    }];
    let mut manifest = elect_manifest(&[], &hints, &ShredBudgets::default());
    let mut cov = Coverage::default();
    let chunk = roundtrip_batch(&docs, &mut manifest, 4, &mut cov);
    assert_eq!(chunk.lanes[0].validity.count(), 1, "only the canonical form");
    assert!(chunk.lanes[0].validity.get(0));
    assert!(cov.exception_granules > 0);
}

/// jsonb null at an elected path: never in the typed lane (the validity bit
/// cannot carry three states), always a residual exception whose stored
/// image equals the `->` output — while a truly ABSENT path has neither.
#[test]
fn jsonb_null_is_always_an_exception() {
    setup();
    let docs: Vec<String> = vec![
        "{\"k\": \"a\"}".into(),
        "{\"k\": null}".into(),
        "{\"k\": \"b\"}".into(),
        "{\"other\": 1}".into(),
    ];
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let images = payload_vecs(mcx, &docs);
    let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();
    let mut manifest = elect_manifest(&payloads, &[], &ShredBudgets::default());
    let k = JsonPath::from_dotted("k");
    assert!(manifest
        .elected
        .iter()
        .any(|e| manifest.path(e.path_id) == &k));
    let mut cov = Coverage::default();
    let chunk = roundtrip_batch(&docs, &mut manifest, 4, &mut cov);
    let li = manifest
        .elected
        .iter()
        .position(|e| manifest.path(e.path_id) == &k)
        .unwrap();
    assert!(!chunk.lanes[li].validity.get(1), "null is not lane-valid");
    let img = chunk
        .residual_get(&manifest, 1, &k)
        .expect("null rides the residual");
    let want = getfield::object_field(mcx, payloads[1], b"k")
        .expect("ok")
        .expect("-> finds the null");
    assert_eq!(img, &want[..]);
    // Row 3: k truly absent — validity 0 AND no residual entry (the absent
    // vs jsonb-null distinction).
    assert!(!chunk.lanes[li].validity.get(3));
    assert!(chunk.residual_get(&manifest, 3, &k).is_none());
}

/// An object value AT an elected path is one whole exception (memo §2.4) —
/// the subtree is a single residual image, not scattered descendants.
#[test]
fn object_at_elected_path_is_one_exception() {
    setup();
    let docs: Vec<String> = vec![
        "{\"a\": \"scalar\"}".into(),
        "{\"a\": {\"b\": 1, \"c\": [2]}}".into(),
    ];
    let hints = vec![PathHint {
        path: JsonPath::from_dotted("a"),
        lane: Lane::Text,
    }];
    let mut manifest = elect_manifest(&[], &hints, &ShredBudgets::default());
    let mut cov = Coverage::default();
    let chunk = roundtrip_batch(&docs, &mut manifest, 4, &mut cov);
    let a = JsonPath::from_dotted("a");
    assert!(chunk.residual_get(&manifest, 1, &a).is_some());
    // No descendant entries exist anywhere.
    assert!(manifest.lookup(&JsonPath::from_dotted("a.b")).is_none());
    let total_row1: u32 = chunk.buckets.iter().map(|b| b.sizes[1]).sum();
    assert_eq!(total_row1, 1);
}
