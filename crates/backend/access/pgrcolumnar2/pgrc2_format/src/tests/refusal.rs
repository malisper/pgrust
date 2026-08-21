//! Born-RED typed refusals (M3-A exit slice: "unknown-encoding-ID → typed
//! refusal (born-RED with a seeded unknown ID)"). Convention: each gate has
//! both teeth — it FIRES on the seeded defect, and its PASS arm demands
//! positive evidence (the exhaustive sweep, the real registration), so a
//! not-run gate cannot report clean.

use crate::abi::{CodecRegistry, Face, KernelKey};
use crate::class::{ClassHint, StorageClass, CLASS_BYVAL};
use crate::dml::DvReader;
use crate::enc::{EncodingId, Wrapper};
use crate::manifest::Manifest;
use crate::part::{PartHeader, PartTail, SectionEntry, SectionKind, StreamRole, SECTION_OPTIONAL};
use crate::sortkey::SortKeyRecord;
use crate::verbatim::reference_vtables;
use crate::FormatError;

// ---------------------------------------------------------------------------
// encoding-ID adjudication (spec §4)
// ---------------------------------------------------------------------------

/// TOOTH 2 (cannot pass vacuously): the EXHAUSTIVE id sweep — every id in
/// 0..=512 must land on exactly its assigned verdict. A new assignment
/// without a spec §4 row fails here.
#[test]
fn encoding_id_sweep_is_exhaustive() {
    // 0..=12 are assigned (FSST id 12 first-class per SB-4/OD-5; the
    // RESERVED verdict class is empty as of that activation); everything
    // above refuses UNKNOWN.
    for id in 0u16..=512 {
        let verdict = EncodingId::resolve(id);
        match id {
            0..=12 => {
                let e = verdict.expect("assigned id resolves");
                assert_eq!(e.as_u16(), id);
            }
            _ => assert_eq!(verdict, Err(FormatError::UnknownEncoding { id })),
        }
    }
    assert_eq!(EncodingId::resolve(12), Ok(EncodingId::Fsst));
}

/// TOOTH 1 (fires): a seeded unknown ID refuses typed — through the
/// registry path a real reader takes.
#[test]
fn seeded_unknown_encoding_id_fires() {
    let vts = Box::leak(Box::new(reference_vtables()));
    let reg = CodecRegistry::new(vts);
    let seeded = KernelKey {
        encoding: 0xBEEF,
        class: CLASS_BYVAL,
        width: 4,
    };
    assert_eq!(
        reg.resolve(seeded).err(),
        Some(FormatError::UnknownEncoding { id: 0xBEEF })
    );
}

/// FSST id 12 is FIRST-CLASS (SB-4/OD-5 activation): it resolves, and a
/// binding without the codec's kernel refuses KernelMissing — an ASSIGNED
/// verdict, distinct from unknown. The born-RED reserved-refusal seed
/// shifts to a still-unassigned id in 13..=127.
#[test]
fn fsst_id_is_first_class_and_unassigned_band_refuses() {
    assert_eq!(EncodingId::resolve(12), Ok(EncodingId::Fsst));
    assert_eq!(EncodingId::Fsst.as_u16(), 12);
    let vts = Box::leak(Box::new(reference_vtables()));
    let reg = CodecRegistry::new(vts);
    // Assigned-but-not-registered here (the M3-C binding carries the
    // kernel): typed KernelMissing, never a reserved/unknown refusal.
    let key = KernelKey {
        encoding: EncodingId::Fsst.as_u16(),
        class: CLASS_BYVAL,
        width: 0,
    };
    assert_eq!(
        reg.resolve(key).err(),
        Some(FormatError::KernelMissing {
            encoding: 12,
            class: CLASS_BYVAL,
            width: 0
        })
    );
    // The shifted born-RED seed: an id inside the 13..=127 A-lane band
    // refuses UNKNOWN through the registry path a real reader takes, and
    // stays distinguishable from ids beyond the band by its payload.
    let seeded = KernelKey {
        encoding: 42,
        class: CLASS_BYVAL,
        width: 0,
    };
    assert_eq!(
        reg.resolve(seeded).err(),
        Some(FormatError::UnknownEncoding { id: 42 })
    );
    assert!(42 <= crate::enc::ENC_RESERVED_MAX);
    assert_ne!(
        EncodingId::resolve(42),
        EncodingId::resolve(0xBEEF),
        "distinct seeded ids carry distinct typed payloads"
    );
}

/// Assigned encoding, no kernel: typed KernelMissing (not a fallback).
/// PASS-arm evidence: the same registry RESOLVES the registered key.
#[test]
fn kernel_missing_vs_registered() {
    let vts = Box::leak(Box::new(reference_vtables()));
    let reg = CodecRegistry::new(vts);
    // BYTE_FOR is assigned (spec §4) but M3-A registers no kernel for it.
    let missing = KernelKey {
        encoding: EncodingId::ByteFor.as_u16(),
        class: CLASS_BYVAL,
        width: 2,
    };
    assert_eq!(
        reg.resolve(missing).err(),
        Some(FormatError::KernelMissing {
            encoding: 2,
            class: CLASS_BYVAL,
            width: 2
        })
    );
    // Evidence the gate can pass: the reference key resolves.
    let present = KernelKey {
        encoding: EncodingId::Verbatim.as_u16(),
        class: CLASS_BYVAL,
        width: 2,
    };
    let vt = reg.resolve(present).expect("registered kernel resolves");
    assert_eq!(vt.key, present);
}

/// Face refusals are typed with the face named (spec §19.3).
#[test]
fn unsupported_faces_refuse_typed() {
    let vts = Box::leak(Box::new(reference_vtables()));
    let reg = CodecRegistry::new(vts);
    let key = KernelKey {
        encoding: EncodingId::Verbatim.as_u16(),
        class: CLASS_BYVAL,
        width: 4,
    };
    let vt = reg.resolve(key).expect("resolves");
    let ctx = crate::abi::KernelCtx {
        key,
        flags: 0,
        fixed_len: 0,
        bytes: &[],
        frame_table: None,
        granule: 0,
        granule_in_extent: 0,
        rows: 0,
        values: 0,
        validity_bytes: None,
        overflow: None,
        dict: None,
    };
    let mut codes = [0u32; 4];
    assert_eq!(
        (vt.decode_codes)(&ctx, &mut codes),
        Err(FormatError::FaceUnsupported {
            face: Face::DecodeCodes,
            encoding: 0
        })
    );
    assert_eq!(
        (vt.dict_handle)(&ctx),
        Err(FormatError::FaceUnsupported {
            face: Face::DictHandle,
            encoding: 0
        })
    );
}

// ---------------------------------------------------------------------------
// structural refusals: sections, roles, classes, wrappers
// ---------------------------------------------------------------------------

#[test]
fn unknown_section_kind_refuses_unless_optional() {
    let required = SectionEntry {
        off: 64,
        len: 10,
        kind: 999,
        flags: 0,
        attno: 0,
        path_ord: 0,
        crc: 0,
    };
    assert_eq!(
        required.known_kind(),
        Err(FormatError::UnknownSectionKind { kind: 999 })
    );
    let optional = SectionEntry {
        flags: SECTION_OPTIONAL,
        ..required
    };
    assert_eq!(optional.known_kind(), Ok(None));
    let known = SectionEntry {
        kind: SectionKind::Stats.as_u16(),
        ..required
    };
    assert_eq!(known.known_kind(), Ok(Some(SectionKind::Stats)));
}

#[test]
fn unknown_stream_role_and_class_refuse() {
    assert_eq!(
        StreamRole::from_u8(200),
        Err(FormatError::UnknownStreamRole { role: 200 })
    );
    assert_eq!(
        StorageClass::from_parts(77, 0, false, 0),
        Err(FormatError::UnknownStorageClass { class: 77 })
    );
    // Hint/property mismatches (spec §3).
    assert!(matches!(
        StorageClass::derive(2, true, b's', ClassHint::Float),
        Err(FormatError::BadClassHint { .. })
    ));
    assert!(matches!(
        StorageClass::derive(-2, false, b'c', ClassHint::None),
        Err(FormatError::BadClassHint { .. })
    ));
    assert!(matches!(
        StorageClass::derive(4, true, b'i', ClassHint::Bool),
        Err(FormatError::BadClassHint { .. })
    ));
}

#[test]
fn wrapper_vocabulary() {
    assert_eq!(Wrapper::from_u8(0), Ok(Wrapper::None));
    assert_eq!(Wrapper::from_u8(1), Ok(Wrapper::Lz4));
    assert_eq!(Wrapper::from_u8(2), Ok(Wrapper::Zstd));
    assert!(Wrapper::from_u8(3).is_err());
    // The format-crate section writer refuses wrappers (M3-C owns block
    // assembly, spec §6.4).
    let mut buf = Vec::new();
    assert_eq!(
        crate::part::StreamSectionWriter::begin(&mut buf, 0, 0, Wrapper::Lz4).err(),
        Some(FormatError::WrapperUnsupported { wrapper: 1 })
    );
}

// ---------------------------------------------------------------------------
// corruption refusals (per-section CRC teeth at format grain)
// ---------------------------------------------------------------------------

#[test]
fn manifest_corruption_refuses() {
    let m = crate::tests::golden_sample_manifest();
    let good = m.encode();
    assert!(Manifest::decode(&good).is_ok());
    // Seeded corruption: any flipped byte fails the CRC.
    let mut bad = good.clone();
    bad[16] ^= 0x01;
    assert_eq!(
        Manifest::decode(&bad),
        Err(FormatError::CrcMismatch { at: "Manifest" })
    );
    // Truncation is typed.
    assert_eq!(
        Manifest::decode(&good[..10]),
        Err(FormatError::Truncated { at: "Manifest" })
    );
    // Part-record order violations are typed (rebuild the image with the
    // records swapped, CRC recomputed — structural validation must fire
    // even when the checksum is honest).
    let mut swapped = m.clone();
    swapped.parts.swap(0, 1);
    let img = swapped.encode();
    assert_eq!(
        Manifest::decode(&img),
        Err(FormatError::Corrupt {
            at: "PartRecord part_no order"
        })
    );
}

#[test]
fn part_header_and_tail_refuse_corruption() {
    let h = PartHeader::new(1, 0xABCD, 1663, 5, 90210);
    let mut enc = Vec::new();
    h.encode_into(&mut enc);
    assert!(PartHeader::decode(&enc).is_ok());
    let mut bad = enc.clone();
    bad[0] ^= 0xFF;
    assert_eq!(
        PartHeader::decode(&bad),
        Err(FormatError::BadMagic { at: "PartHeader" })
    );
    let mut bad = enc.clone();
    bad[20] ^= 0x01; // flags byte — caught by the header CRC
    assert_eq!(
        PartHeader::decode(&bad),
        Err(FormatError::CrcMismatch { at: "PartHeader" })
    );

    // Tail: bad magic, bad footer_off bounds.
    let mut file = vec![0u8; 200];
    let tail = PartTail::new(88);
    let mut tb = Vec::new();
    tail.encode_into(&mut tb);
    file[184..200].copy_from_slice(&tb);
    assert!(PartTail::decode_at_eof(&file).is_ok());
    let mut bad = file.clone();
    bad[196] ^= 0xFF;
    assert_eq!(
        PartTail::decode_at_eof(&bad),
        Err(FormatError::BadMagic { at: "PartTail" })
    );
    let bad_tail = PartTail::new(150); // footer would run past EOF
    let mut tb = Vec::new();
    bad_tail.encode_into(&mut tb);
    let mut bad = file.clone();
    bad[184..200].copy_from_slice(&tb);
    assert_eq!(
        PartTail::decode_at_eof(&bad),
        Err(FormatError::Bounds {
            at: "PartTail footer_off"
        })
    );
}

#[test]
fn dv_corruption_refuses() {
    let rows: Vec<u16> = vec![1, 2, 3];
    let img =
        crate::dml::encode_dv(1, 1, &[(0, crate::dml::DvBlockKind::List, &rows)]).expect("encodes");
    assert!(DvReader::open(&img).is_ok());
    let mut bad = img.clone();
    bad[40] ^= 0x01;
    assert_eq!(
        DvReader::open(&bad).err(),
        Some(FormatError::CrcMismatch { at: "DeleteVector" })
    );
}

#[test]
fn sortkey_corruption_refuses() {
    let good = SortKeyRecord {
        keys: vec![crate::sortkey::SortKeyEntry {
            attno: 1,
            dir: 0,
            nulls: 0,
            collation_class: 0,
            pad: 0,
        }],
    }
    .encode();
    assert!(SortKeyRecord::decode(&good).is_ok());
    let mut bad = good.clone();
    bad[12] = 9; // dir out of range
    assert_eq!(
        SortKeyRecord::decode(&bad),
        Err(FormatError::Corrupt {
            at: "SortKeyEntry dir/nulls"
        })
    );
    assert!(SortKeyRecord::decode(&good[..7]).is_err());
}
