//! TY-1 ArrayDual writer wiring (lanev4): array columns route through the
//! codec's `elect_array_split` accept/refuse at seal. Corpus-shaped feeds:
//! dense int arrays ACCEPT (v4_allowed singular `array_dual` on
//! ai4_dense-class columns — empty-heavy and null-ROW shapes included);
//! null-ELEMENT and multidim shapes REFUSE (`verbatim` MUST). The accepted
//! part is read back through the real pgrc2_read cursors over the
//! Sizes/ChildValues substreams and composed with the codec's
//! `assemble_array_datums` — byte-exact against the fed images.

use super::*;
use pgrc2_codec::arraydual::{assemble_array_datums, ArrayElemFacts};
use pgrc2_format::abi::{ByteArena, DecodeOut};
use pgrc2_format::enc::EncodingId;
use std::sync::Arc;

const INT4_OID: u32 = 23;

fn array_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::VarlenaVerbatim,
        typlen: -1,
        typbyval: false,
        typalign: b'i',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::Opaque,
    }
}

/// A PG 1-D int4 array PAYLOAD (past the varlena header): ndim, dataoffset,
/// elemtype, dims[0], lbound[0], elements. Empty arrays are the canonical
/// ndim-0 image.
fn arr_i4(elems: &[i32]) -> Vec<u8> {
    let mut p = Vec::new();
    if elems.is_empty() {
        p.extend_from_slice(&0i32.to_le_bytes());
        p.extend_from_slice(&0i32.to_le_bytes());
        p.extend_from_slice(&INT4_OID.to_le_bytes());
        return p;
    }
    p.extend_from_slice(&1i32.to_le_bytes());
    p.extend_from_slice(&0i32.to_le_bytes());
    p.extend_from_slice(&INT4_OID.to_le_bytes());
    p.extend_from_slice(&(elems.len() as i32).to_le_bytes());
    p.extend_from_slice(&1i32.to_le_bytes());
    for e in elems {
        p.extend_from_slice(&e.to_le_bytes());
    }
    p
}

/// A nulls-in-elements shape: `dataoffset != 0` (the refuse predicate).
fn arr_i4_nullelem() -> Vec<u8> {
    let mut p = arr_i4(&[1, 2]);
    p[4..8].copy_from_slice(&24i32.to_le_bytes()); // dataoffset != 0
    p
}

/// A 2-D shape (`ndim == 2` refuses).
fn arr_i4_multidim() -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&2i32.to_le_bytes());
    p.extend_from_slice(&0i32.to_le_bytes());
    p.extend_from_slice(&INT4_OID.to_le_bytes());
    p.extend_from_slice(&2i32.to_le_bytes()); // dims[0]
    p.extend_from_slice(&1i32.to_le_bytes()); // lbound[0]
    p.extend_from_slice(&2i32.to_le_bytes()); // dims[1]
    p.extend_from_slice(&1i32.to_le_bytes()); // lbound[1]
    for e in [1i32, 2, 3, 4] {
        p.extend_from_slice(&e.to_le_bytes());
    }
    p
}

/// The corpus-shaped dense feed (ai4-class): ragged lengths, empty-heavy
/// stripe, null ROWS — all ACCEPT shapes.
fn dense_payload(i: u64) -> Option<Vec<u8>> {
    if i % 19 == 7 {
        return None; // null row (ai4_nullrows: still array_dual)
    }
    if i % 5 == 0 {
        return Some(arr_i4(&[])); // empty (ai4_empty_mix: still array_dual)
    }
    let n = (i % 8) as i32 + 1;
    Some(arr_i4(
        &(0..n).map(|k| (i as i32) * 31 + k).collect::<Vec<i32>>(),
    ))
}

fn seal_arrays(
    payload_of: impl Fn(u64) -> Option<Vec<u8>>,
    declare: bool,
) -> (crate::wvfs::MemVfs, u16) {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer(vec![array_col(1)], stamp(31, 1));
    if declare {
        w.set_structural(crate::structural::StructuralPolicy::new().with_array(
            1,
            ArrayElemFacts {
                elemtype: INT4_OID,
                elem_len: 4,
            },
        ));
    }
    for i in 0..600u64 {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let img = payload_of(i).map(|p| img_4b_u(&p));
        let d = match &img {
            None => RawDatum::Null,
            Some(b) => RawDatum::Bytes(b),
        };
        w.append_row(&[d], &mut kit.ext, &mut env).expect("append");
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
    let probe = Probe::new(TxnVerdict::InProgress).set(31, TxnVerdict::Committed);
    w.publish(&mut vfs, &probe).expect("publish");
    // The witness rides the seal report (the corpus assertion surface).
    let witness = w
        .seal_reports()
        .iter()
        .flat_map(|r| r.elections.iter())
        .find(|e| e.attno == 1 && e.path_ord == 0)
        .copied()
        .expect("witness for attno 1");
    (vfs, witness.encoding)
}

#[test]
fn dense_int_arrays_seal_as_dual_substreams_and_compose_byte_exactly() {
    let (mut vfs, witness) = seal_arrays(dense_payload, true);
    // The corpus assertion surface: dense int arrays MUST elect array_dual.
    assert_eq!(witness, EncodingId::ArrayDual.as_u16());
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");

    // Parent: the structural marker — encoding ArrayDual, ZERO extents,
    // elem facts echoed in width/aux32.
    let (parent, pexts) = pv.stream(1, 0, StreamRole::Values).expect("parent entry");
    assert_eq!(parent.encoding, EncodingId::ArrayDual.as_u16());
    assert_eq!(parent.extent_count, 0);
    assert!(pexts.is_empty());
    assert_eq!(parent.width, 4);
    assert_eq!(parent.aux32, INT4_OID);
    assert_eq!(parent.values, 600);
    // The dual substreams (VERBATIM word layout).
    let (sizes, _) = pv.stream(1, 0, StreamRole::Sizes).expect("sizes");
    assert_eq!(sizes.encoding, EncodingId::Verbatim.as_u16());
    assert_eq!(sizes.values, 600);
    let (elems, _) = pv.stream(1, 0, StreamRole::ChildValues).expect("elems");
    assert_eq!(elems.encoding, EncodingId::Verbatim.as_u16());
    assert_eq!(elems.width, 4);
    // Null rows exist, so the parent validity stream rides along.
    assert!(pv.stream(1, 0, StreamRole::Validity).is_some());

    // ---- read-side composition through the REAL cursors -------------------
    let part = Arc::new(
        pgrc2_read::openpart::OpenPart::open(
            Box::new(pgrc2_read::io::MemPartIo::new(pv.bytes.clone(), 7, 900)),
            &pgrc2_read::openpart::PartExpect::none(),
        )
        .expect("open part"),
    );
    let binding = pgrc2_read::cursor::reference_binding_leaked();
    // The structural parent refuses a value cursor TYPED (composition law).
    assert!(matches!(
        pgrc2_read::cursor::StreamCursor::open(Arc::clone(&part), binding, 1, 0),
        Err(pgrc2_read::ReadError::Unsupported { .. })
    ));
    let mut sizes_cur = pgrc2_read::cursor::StreamCursor::open_role(
        Arc::clone(&part),
        binding,
        1,
        0,
        StreamRole::Sizes,
    )
    .expect("sizes cursor");
    let mut elems_cur = pgrc2_read::cursor::StreamCursor::open_role(
        Arc::clone(&part),
        binding,
        1,
        0,
        StreamRole::ChildValues,
    )
    .expect("elems cursor");
    let facts = ArrayElemFacts {
        elemtype: INT4_OID,
        elem_len: 4,
    };
    let mut row: u64 = 0;
    for g in 0..sizes_cur.granule_count() {
        let rows_g = sizes_cur.rows_in_granule(g) as usize;
        // Sizes (row-aligned). Byval decodes never allocate, but the arena
        // BASE must still be 8-aligned (abi.rs §19.4) — ArenaBuf, never a
        // stack byte array.
        let mut sizes = vec![0u64; rows_g];
        let mut no_arena = ArenaBuf::new(0);
        let mut out = DecodeOut {
            datums: &mut sizes,
            arena: ByteArena::new(no_arena.bytes_mut()),
        };
        assert_eq!(sizes_cur.decode_full(g, &mut out).expect("sizes"), rows_g as u32);
        // Elements (dense).
        let n_elems = elems_cur.values_in_granule(g).expect("elem count") as usize;
        let mut elems = vec![0u64; n_elems.max(1)];
        let mut no_arena2 = ArenaBuf::new(0);
        let mut out2 = DecodeOut {
            datums: &mut elems[..n_elems],
            arena: ByteArena::new(no_arena2.bytes_mut()),
        };
        assert_eq!(
            elems_cur.decode_full(g, &mut out2).expect("elems"),
            n_elems as u32
        );
        // Validity from the row-aligned face.
        let mut vwords = vec![0u64; rows_g.div_ceil(64).max(1)];
        let verdict = sizes_cur.validity(g, &mut vwords).expect("validity");
        let valid = |r: u32| match verdict {
            pgrc2_format::abi::ValidityVerdict::AllValid => true,
            pgrc2_format::abi::ValidityVerdict::Mixed { .. } => {
                (vwords[(r / 64) as usize] >> (r % 64)) & 1 == 1
            }
        };
        // Compose and compare against the fed payloads (8-aligned backing).
        let mut arena_buf =
            ArenaBuf::new(pgrc2_codec::arraydual::assembled_arena_bytes(facts, &sizes) + 64);
        let mut arena = ByteArena::new(arena_buf.bytes_mut());
        let mut datums = vec![0u64; rows_g];
        assemble_array_datums(facts, &sizes, &elems[..n_elems], valid, &mut datums, &mut arena)
            .expect("assemble");
        for r in 0..rows_g {
            let want = dense_payload(row);
            if let Some(want) = want {
                assert!(valid(r as u32), "row {row} unexpectedly null");
                let d = datums[r];
                let hdr = u32::from_le_bytes(unsafe { *(d as *const [u8; 4]) });
                let total = (hdr >> 2) as usize;
                let got =
                    unsafe { std::slice::from_raw_parts((d + 4) as *const u8, total - 4) };
                assert_eq!(got, want.as_slice(), "row {row} payload");
            } else {
                assert!(!valid(r as u32), "row {row} should be null");
            }
            row += 1;
        }
    }
    assert_eq!(row, 600);
}

#[test]
fn refuse_shapes_demote_to_verbatim() {
    // nulls-in-elements (dataoffset != 0) → verbatim MUST.
    let (mut vfs, witness) = seal_arrays(
        |i| {
            if i % 10 == 0 {
                Some(arr_i4_nullelem())
            } else {
                Some(arr_i4(&[i as i32]))
            }
        },
        true,
    );
    assert_eq!(witness, EncodingId::Verbatim.as_u16());
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (parent, _) = pv.stream(1, 0, StreamRole::Values).expect("parent");
    assert_eq!(parent.encoding, EncodingId::Verbatim.as_u16());
    assert!(parent.extent_count > 0, "ordinary value stream");
    assert!(pv.stream(1, 0, StreamRole::Sizes).is_none());
    assert!(pv.stream(1, 0, StreamRole::ChildValues).is_none());

    // multidim → verbatim MUST.
    let (mut vfs, witness) = seal_arrays(
        |i| {
            if i % 7 == 3 {
                Some(arr_i4_multidim())
            } else {
                Some(arr_i4(&[i as i32, 2]))
            }
        },
        true,
    );
    assert_eq!(witness, EncodingId::Verbatim.as_u16());
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (parent, _) = pv.stream(1, 0, StreamRole::Values).expect("parent");
    assert_eq!(parent.encoding, EncodingId::Verbatim.as_u16());

    // An UNDECLARED array column never enters the arm at all.
    let (mut vfs, witness) = seal_arrays(dense_payload, false);
    assert_eq!(witness, EncodingId::Verbatim.as_u16());
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (parent, _) = pv.stream(1, 0, StreamRole::Values).expect("parent");
    assert_eq!(parent.encoding, EncodingId::Verbatim.as_u16());
}
