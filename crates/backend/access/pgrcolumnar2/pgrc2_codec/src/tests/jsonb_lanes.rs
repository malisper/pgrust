//! JSONB shred-lane law compliance (§5 M3-C + charter §7 jsonb row): the
//! typed path lanes survive a full shred → lane-encode → stream-decode →
//! lane-rebuild → reconstruct loop BYTE-IDENTICALLY, with the NULL
//! trichotomy intact:
//!
//! - SQL NULL — the ROOT stream's validity (the image lane; reference
//!   verbatim suite owns that path);
//! - jsonb `null` at a path — presence WITHOUT lane occupancy (rides the
//!   residual so `->` distinguishes it from absence — the error-identity
//!   family: a wrong `->>` site is a correctness bug);
//! - absent path — lane validity 0, no residual entry.
//!
//! Exceptions are NEVER discriminators: a value the lane cannot hold rides
//! the residual whole; the reconstruct equality below would break on any
//! blurring of the three states or any exception mishandling.

use super::*;
use crate::boolbm::BoolBitmapEncoder;
use crate::bytefor::{granule_min_width, ByteForEncoder};
use crate::jsonbshred::{budgets_from_reloptions, lane_from_row_aligned, lane_to_row_aligned};
use adt_jsonb_shred::{
    elect_manifest, reconstruct_rows, shred_chunk, Lane, LaneValues, PathHint, ShredChunk,
};
use mbutils::SetDatabaseEncoding;
use mcx::MemoryContext;
use pgrc2_format::class::StorageClass;
use pgrc2_format::verbatim::VerbatimEncoder;
use std::sync::Once;
use wchar::PG_UTF8;

fn setup() {
    let _ = SetDatabaseEncoding(PG_UTF8);
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        mbutils::init_seams();
    });
}

fn docs() -> Vec<String> {
    let mut docs = Vec::new();
    for i in 0..600u32 {
        let doc = match i % 8 {
            // The typed-lane happy rows (uniform dscale 2 numerics).
            0 => format!(
                r#"{{"t":"str{i}","n":{}.25,"b":{},"u":"00000000-0000-4000-8000-{:012}"}}"#,
                i,
                i % 2 == 0,
                i
            ),
            // jsonb null at "t": presence without lane occupancy.
            1 => format!(r#"{{"t":null,"n":{}.25,"b":true}}"#, i),
            // absent "t".
            2 => format!(r#"{{"n":{}.25,"b":false}}"#, i),
            // Exceptions: wrong-typed "t", mixed-dscale "n", subtree at "b".
            3 => format!(r#"{{"t":{},"n":{}.5,"b":{{"k":1}}}}"#, i, i),
            // Non-canonical uuid (exception on the hinted uuid lane).
            4 => r#"{"t":"x","u":"00000000-0000-4000-8000-AAAAAAAAAAAA"}"#.to_string(),
            // Residual-only rows + non-object root.
            5 => format!(r#"{{"extra{}":[1,2,{}]}}"#, i % 13, i),
            6 => format!("{i}"),
            // Duplicate keys (jsonb_in normalizes last-wins).
            _ => format!(r#"{{"t":"dup","t":"kept{i}","n":{}.25}}"#, i),
        };
        docs.push(doc);
    }
    docs
}

/// Encode one lane through its mapped stream codec, decode it back, and
/// rebuild the dense lane. The exercised path is exactly the production
/// pair the module table in `jsonbshred.rs` declares.
fn lane_loop(lane: &adt_jsonb_shred::TypedLane, rows: u32) -> adt_jsonb_shred::TypedLane {
    let ra = lane_to_row_aligned(lane, rows).expect("row-align");
    let input = ra.encode_input(rows);
    let inputs = [input];
    let built = match ra.class {
        StorageClass::VarlenaVerbatim => {
            let mut enc = VerbatimEncoder {
                class: StorageClass::VarlenaVerbatim,
            };
            build_stream(&mut enc, &inputs, 0, false)
        }
        StorageClass::Fixed { len } => {
            let mut enc = VerbatimEncoder {
                class: StorageClass::Fixed { len },
            };
            build_stream(&mut enc, &inputs, len, false)
        }
        StorageClass::ByvalWord { .. } => {
            let width = granule_min_width(&inputs[0], true);
            let mut enc = ByteForEncoder::new_bytefor(8, width, true);
            build_stream(&mut enc, &inputs, 0, true)
        }
        StorageClass::Bool => {
            let mut enc = BoolBitmapEncoder;
            build_stream(&mut enc, &inputs, 0, false)
        }
        other => panic!("unexpected lane class {other:?}"),
    };
    let (datums, arena) = dec_full(&built, 0);
    let rebuilt = lane_from_row_aligned(
        &lane.values,
        rows,
        &ra.validity,
        &datums,
        lane.exceptions.clone(),
        |datum| match ra.class {
            StorageClass::VarlenaVerbatim => {
                // SAFETY: arena-backed varlena datum from the decode above.
                let payload = unsafe { crate::section::varlena_payload(datum)? };
                let mut img = Vec::from(
                    pgrc2_format::wire::varlena_header_4b_u(payload.len() as u32).to_le_bytes(),
                );
                img.extend_from_slice(payload);
                Ok(img)
            }
            StorageClass::Fixed { len } => {
                // SAFETY: arena-backed fixed image from the decode above.
                Ok(unsafe {
                    core::slice::from_raw_parts(datum as *const u8, len as usize).to_vec()
                })
            }
            _ => Ok(Vec::new()),
        },
        ra.scale,
    )
    .expect("rebuild");
    drop(arena);
    rebuilt
}

#[test]
fn shred_lanes_survive_stream_codecs_byte_identically() {
    setup();
    let ctx = MemoryContext::new("m3c-jsonb-lanes");
    let mcx = ctx.mcx();
    let docs = docs();
    let images: Vec<Vec<u8>> = docs
        .iter()
        .map(|d| {
            adt_jsonb::io::jsonb_in(mcx, d.as_bytes(), None)
                .unwrap_or_else(|e| panic!("jsonb_in {d:?}: {}", e.message()))
                .expect("hard path")[..]
                .to_vec()
        })
        .collect();
    let payloads: Vec<&[u8]> = images.iter().map(|i| &i[4..]).collect();

    let budgets = budgets_from_reloptions(64);
    let hints = [
        PathHint {
            path: adt_jsonb_shred::JsonPath::from_dotted("u"),
            lane: Lane::Uuid16,
        },
        PathHint {
            path: adt_jsonb_shred::JsonPath::from_dotted("t"),
            lane: Lane::Text,
        },
    ];
    let mut manifest = elect_manifest(&payloads, &hints, &budgets);
    assert!(
        !manifest.elected.is_empty(),
        "corpus must elect typed lanes"
    );
    let chunk = shred_chunk(mcx, &mut manifest, &payloads, 128).expect("shred");

    // Trichotomy witnesses on the "t" lane (rows 0/1/2 shapes).
    let t_idx = manifest
        .elected
        .iter()
        .position(|e| manifest.path(e.path_id).segments() == [b"t".to_vec()])
        .expect("t lane elected");
    let t_lane = &chunk.lanes[t_idx];
    assert!(t_lane.validity.get(0), "present value occupies the lane");
    assert!(
        !t_lane.validity.get(1),
        "jsonb null must NOT occupy the lane (trichotomy)"
    );
    assert!(
        !t_lane.validity.get(2),
        "absent path must not occupy the lane"
    );

    // Push every typed lane through its stream codec and rebuild.
    let mut lanes2 = Vec::new();
    let mut coverage = [0usize; 4];
    for lane in &chunk.lanes {
        match lane.values {
            LaneValues::Text { .. } => coverage[0] += 1,
            LaneValues::Uuid16 { .. } => coverage[1] += 1,
            LaneValues::NumericFs { .. } => coverage[2] += 1,
            LaneValues::Bool { .. } => coverage[3] += 1,
        }
        lanes2.push(lane_loop(lane, chunk.nrows));
    }
    assert!(
        coverage[0] > 0 && coverage[1] > 0 && coverage[2] > 0 && coverage[3] > 0,
        "all four lane classes must be exercised: {coverage:?}"
    );
    for (i, (a, b)) in chunk.lanes.iter().zip(&lanes2).enumerate() {
        assert_eq!(a, b, "lane {i} changed across the stream codec loop");
    }

    // The whole-loop law: reconstruct from the REBUILT lanes must equal the
    // original jsonb_in images byte-for-byte.
    let chunk2 = ShredChunk {
        nrows: chunk.nrows,
        granule_rows: chunk.granule_rows,
        lanes: lanes2,
        buckets: chunk.buckets.clone(),
    };
    let recon = reconstruct_rows(mcx, &manifest, &chunk2).expect("reconstruct");
    for (r, (got, want)) in recon.iter().zip(&images).enumerate() {
        assert_eq!(
            got, want,
            "row {r} ({:?}) not byte-identical after the lane codec loop",
            docs[r]
        );
    }
}
