//! SB-10 byte-bounded granule geometry at the writer (lanev4; OD-13 RULED):
//! the seal elects the LARGEST ladder grain whose per-(column,granule)
//! value bytes clear the provisional 10 MiB bound; narrow schemas keep the
//! default 8192; a wide-value column drags the whole part down the ladder
//! (the grain is per PART — uniform across columns). The elected grain
//! lands in the footer and the sealed part round-trips through the real
//! reader at that grain.

use super::*;
use pgrc2_format::geom;

/// ~8 KiB deterministic payload (the jb_widerow class: ~10KB docs make
/// ~80MB per (column,granule) at the fixed 8192 grain — the OD-13 census
/// shape).
fn wide_payload(i: u64) -> Vec<u8> {
    let mut v = vec![0u8; 8 * 1024];
    for (k, b) in v.iter_mut().enumerate() {
        *b = (i.wrapping_mul(31).wrapping_add(k as u64) & 0xFF) as u8;
    }
    v
}

fn seal_two_col(rows: u64, wide: bool) -> (crate::wvfs::MemVfs, u32) {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer(vec![int8_col(1), text_col(2)], stamp(51, 1));
    for i in 0..rows {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let payload = if wide {
            wide_payload(i)
        } else {
            format!("narrow-{i}").into_bytes()
        };
        let img = img_4b_u(&payload);
        w.append_row(
            &[RawDatum::Word(i), RawDatum::Bytes(&img)],
            &mut kit.ext,
            &mut env,
        )
        .expect("append");
    }
    let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
    let mut env = SealEnv {
        vfs: &mut vfs,
        sources: &sources,
        resolver: &kit.resolver,
        shred: &mut kit.shred,
        shred_opts: &kit.opts,
    };
    w.finish(&mut env).expect("finish");
    let probe = Probe::new(TxnVerdict::InProgress).set(51, TxnVerdict::Committed);
    w.publish(&mut vfs, &probe).expect("publish");
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let grain = pv.footer().granule_rows;
    (vfs, grain)
}

#[test]
fn narrow_schema_keeps_the_default_grain() {
    let (mut vfs, grain) = seal_two_col(20_000, false);
    assert_eq!(grain, geom::GRANULE_ROWS, "narrow columns stay at 8192");
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let f = pv.footer();
    assert_eq!(f.granule_count, geom::granule_count(20_000));
    assert_eq!(f.band_count, geom::band_count(20_000));
}

#[test]
fn wide_values_force_a_smaller_grain_and_the_part_round_trips() {
    // 2,048 rows × ~8 KiB ≈ 16 MiB of value bytes in ONE part:
    //   grain 8192 → 1 granule → 16 MiB > 10 MiB   ✗
    //   grain 4096 → 1 granule → 16 MiB            ✗
    //   grain 2048 → 1 granule → 16 MiB            ✗
    //   grain 1024 → 2 granules → 8 MiB ≤ 10 MiB   ✓
    let rows: u64 = 2_048;
    let (mut vfs, grain) = seal_two_col(rows, true);
    assert_eq!(grain, 1_024, "the wide column drags the part to 1024");
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let f = pv.footer();
    let g = f.grain().expect("ladder grain");
    assert_eq!(f.granule_count as u64, geom::granule_count_at(rows, g));
    assert_eq!(f.band_count as u64, geom::band_count_at(rows, g));
    assert_eq!(f.granule_count, 2);
    assert_eq!(f.band_count, 1);

    // Round-trip through the REAL reader at the non-default grain (the
    // int8 word stream AND the varlena stream both carry root gcount
    // tables at this grain — the grain-proof addressing arm).
    use pgrc2_format::abi::{ByteArena, DecodeOut, ValidityVerdict};
    use std::sync::Arc;
    let part = Arc::new(
        pgrc2_read::openpart::OpenPart::open(
            Box::new(pgrc2_read::io::MemPartIo::new(pv.bytes.clone(), 7, 902)),
            &pgrc2_read::openpart::PartExpect::none(),
        )
        .expect("open"),
    );
    assert_eq!(part.grain().rows(), 1_024);
    let binding = pgrc2_read::cursor::reference_binding_leaked();
    for attno in [1u32, 2] {
        let mut cur = pgrc2_read::cursor::StreamCursor::open(Arc::clone(&part), binding, attno, 0)
            .expect("cursor");
        assert_eq!(cur.granule_count(), 2);
        let mut row: u64 = 0;
        for g in 0..cur.granule_count() {
            let rows_g = cur.rows_in_granule(g) as usize;
            assert_eq!(rows_g, 1_024, "grain-shaped granules");
            let mut datums = vec![0u64; rows_g];
            // 8-aligned arena backing (abi.rs §19.4 base-alignment law).
            let mut arena_buf = ArenaBuf::new(16 << 20);
            let mut out = DecodeOut {
                datums: &mut datums,
                arena: ByteArena::new(arena_buf.bytes_mut()),
            };
            cur.decode_full(g, &mut out).expect("decode");
            let mut vwords = vec![0u64; rows_g.div_ceil(64)];
            let verdict = cur.validity(g, &mut vwords).expect("validity");
            assert!(matches!(verdict, ValidityVerdict::AllValid));
            for r in 0..rows_g {
                match attno {
                    1 => assert_eq!(datums[r], row, "int8 row {row}"),
                    _ => {
                        let d = datums[r];
                        let hdr = u32::from_le_bytes(unsafe { *(d as *const [u8; 4]) });
                        let total = (hdr >> 2) as usize;
                        let got = unsafe {
                            std::slice::from_raw_parts((d + 4) as *const u8, total - 4)
                        };
                        assert_eq!(got, wide_payload(row).as_slice(), "text row {row}");
                    }
                }
                row += 1;
            }
        }
        assert_eq!(row, rows);
    }
}
