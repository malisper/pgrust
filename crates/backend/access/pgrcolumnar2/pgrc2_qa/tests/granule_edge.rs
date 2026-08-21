//! Granule-edge decode differentials (§5 M3-K): row counts at every
//! geometry edge (frame 1024, granule 8192, band 65,536, ±1 each), full
//! decode vs oracle, `decode_sel ≡ decode_full ∘ select` at edge
//! selections, and the geometry witnesses (granule count, per-granule row
//! sums). An oversize (overflow-region) value pinned AT a granule edge
//! rides along in the text arm.

use pgrc2_qa::adapters::{full_binding, ForcedPlan};
use pgrc2_qa::corpus::{
    build_fixture, decode_column, decode_sel_granule, int8_fixture, open_part_bytes,
    verify_manifest, Fixture, OracleVal,
};
use pgrc2_qa::text_col;
use pgrc2_format::dirlayout::part_file_name;
use pgrc2_format::geom::GRANULE_ROWS;
use pgrc2_read::cursor::StreamCursor;
use pgrc2_write::writer::PartCutPolicy;
use std::sync::Arc;

const EDGE_ROWS: [u64; 10] = [1, 1023, 1024, 1025, 8191, 8192, 8193, 65_535, 65_536, 65_537];

fn edge_sel(rows_in_granule: u32) -> Vec<Vec<u16>> {
    let last = (rows_in_granule - 1) as u16;
    let mut sels = vec![vec![], vec![0], vec![last]];
    if last > 0 {
        sels.push(vec![0, last]);
    }
    sels.push((0..rows_in_granule as u16).step_by(97).collect());
    sels
}

#[test]
fn int8_edges_full_and_sel() {
    for (k, &rows) in EDGE_ROWS.iter().enumerate() {
        let fx = int8_fixture(
            "edge_int8",
            600 + k as u64,
            rows,
            vec![ForcedPlan::ByteFor {
                delta_width: 2, // re-armed: #465 fixed by M3-A2
                signed: true,
            }],
            PartCutPolicy::default(),
            |i| {
                if i % 6 == 5 {
                    None
                } else {
                    Some(50_000 + (i % 40_000) as i64)
                }
            },
        );
        let built = build_fixture(fx);
        // Full decode vs oracle.
        let n = verify_manifest(&built.files, &built.manifest, &built.fx)
            .unwrap_or_else(|e| panic!("edge rows={rows}: {e:?}"));
        assert_eq!(n, rows);
        // Geometry witnesses + sel differentials on first/last granule.
        let part_bytes = built
            .files
            .get(&part_file_name(built.manifest.parts[0].part_no))
            .expect("part");
        let part = open_part_bytes(part_bytes, 42).expect("open");
        let binding = full_binding();
        let schema = &built.fx.schema[0];
        let cur = StreamCursor::open(Arc::clone(&part), binding, 1, 0).expect("cursor");
        let gcount = cur.granule_count();
        assert_eq!(
            gcount as u64,
            rows.div_ceil(GRANULE_ROWS as u64),
            "granule count at rows={rows}"
        );
        let mut sum = 0u64;
        for g in 0..gcount {
            sum += cur.rows_in_granule(g) as u64;
        }
        assert_eq!(sum, rows, "granule row sum at rows={rows}");
        drop(cur);
        let full = decode_column(&part, binding, schema).expect("full decode");
        for g in [0, gcount - 1] {
            let base = g as usize * GRANULE_ROWS as usize;
            let rows_g = {
                let cur = StreamCursor::open(Arc::clone(&part), binding, 1, 0).expect("cursor");
                cur.rows_in_granule(g)
            };
            for sel in edge_sel(rows_g) {
                let got =
                    decode_sel_granule(&part, binding, schema, g, &sel).expect("sel decode");
                for (i, &r) in sel.iter().enumerate() {
                    assert_eq!(
                        got[i],
                        full[base + r as usize],
                        "sel != full at rows={rows} g={g} r={r}"
                    );
                }
            }
        }
    }
}

#[test]
fn text_overflow_at_granule_edge() {
    // Oversize values EXACTLY at granule boundaries (rows 0, 8191, 8192)
    // plus edges of the row count itself.
    let rows: u64 = 16_385; // 2 granules + 1
    let fx = Fixture {
        name: "edge_text_overflow",
        dir: "/qa/t650".to_string(),
        spc: 1663,
        db: 5,
        relfilenumber: 650,
        schema: vec![text_col(1)],
        oracle: vec![(0..rows)
            .map(|i| {
                if i % 4 == 3 {
                    None
                } else if i == 0 || i == 8_191 || i == 8_192 || i == rows - 1 {
                    let mut v = vec![0u8; 33_000];
                    for (k, b) in v.iter_mut().enumerate() {
                        *b = ((i as usize + k) & 0xFF) as u8;
                    }
                    Some(OracleVal::Bytes(v))
                } else {
                    Some(OracleVal::Bytes(vec![(i & 0xFF) as u8; (i % 37) as usize]))
                }
            })
            .collect()],
        plans: Vec::new(),
        policy: PartCutPolicy::default(),
    };
    let built = build_fixture(fx);
    let n = verify_manifest(&built.files, &built.manifest, &built.fx).expect("decode");
    assert_eq!(n, rows);
    // Sel across the overflow rows specifically.
    let part_bytes = built
        .files
        .get(&part_file_name(built.manifest.parts[0].part_no))
        .expect("part");
    let part = open_part_bytes(part_bytes, 43).expect("open");
    let binding = full_binding();
    let schema = &built.fx.schema[0];
    let full = decode_column(&part, binding, schema).expect("full");
    // granule 0: rows 0 and 8191 are oversize.
    let sel: Vec<u16> = vec![0, 1, 8190, 8191];
    let got = decode_sel_granule(&part, binding, schema, 0, &sel).expect("sel g0");
    for (i, &r) in sel.iter().enumerate() {
        assert_eq!(got[i], full[r as usize], "overflow-edge sel g0 r={r}");
    }
    // granule 1: row 0 (global 8192) is oversize.
    let sel1: Vec<u16> = vec![0, 1, 4_000];
    let got1 = decode_sel_granule(&part, binding, schema, 1, &sel1).expect("sel g1");
    for (i, &r) in sel1.iter().enumerate() {
        assert_eq!(
            got1[i],
            full[8_192 + r as usize],
            "overflow-edge sel g1 r={r}"
        );
    }
}
