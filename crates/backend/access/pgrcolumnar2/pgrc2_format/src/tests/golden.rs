//! Golden encode/decode vectors (M3-A exit slice: "part manifest +
//! part-identity golden encode/decode tests"). The manifest/commit-pointer
//! goldens hand-assemble the expected bytes INDEPENDENTLY of the encoder —
//! two encodings must agree, so a silent field reorder fails loudly. The
//! identity hashes pin exact values (any drift is a cache-identity break).

use crate::class::{ClassHint, ColSchema, CollationClass, StorageClass, TypeSemantics};
use crate::dirlayout::*;
use crate::dml::{encode_dv, DvBlockKind, DvReader};
use crate::ident::{part_uuid, schema_fingerprint, PartIdent};
use crate::manifest::{
    CommitPointer, Manifest, ManifestHeader, PartRecord, CURRENT_MAGIC, MANIFEST_MAGIC,
};
use crate::meta::{KeyKind, Sortedness, StatsRecord};
use crate::rowid::pack_rowid;
use crate::sidecar::SidecarKind;
use crate::sortkey::{SortKeyEntry, SortKeyRecord};
use crate::wire::{crc32c, varlena_4b_u_payload_len, varlena_header_4b_u};
use crate::{FormatError, FORMAT_VERSION};

pub(crate) fn sample_manifest() -> Manifest {
    Manifest {
        header: ManifestHeader {
            gen: 7,
            prev_gen: 6,
            publisher_fxid: 0x0000_0002_0000_1234,
            relfilenumber: 90210,
            schema_fingerprint: 0x1122_3344_5566_7788,
            magic: MANIFEST_MAGIC,
            format_version: FORMAT_VERSION,
            spc: 1663,
            db: 5,
            part_count: 2,
            next_part_no: 4,
            flags: 0,
            reserved: 0,
        },
        parts: vec![
            PartRecord {
                rows: 100_000,
                file_len: 4_000_000,
                footer_off: 3_999_888,
                dv_gen: 0,
                dv_len: 0,
                part_no: 1,
                flags: 0,
                granule_count: crate::geom::granule_count(100_000),
                band_count: crate::geom::band_count(100_000),
                dv_crc: 0,
                granule_rows: 0,
            },
            PartRecord {
                rows: 65_536,
                file_len: 2_000_000,
                footer_off: 1_999_888,
                dv_gen: 3,
                dv_len: 2048,
                part_no: 3,
                flags: 0,
                granule_count: 8,
                band_count: 1,
                dv_crc: 0xABCD_EF01,
                granule_rows: 0,
            },
        ],
    }
}

/// Independent hand assembly of the sample manifest — MUST mirror spec
/// §13.1 field-by-field, NOT the encoder.
fn hand_assembled_manifest() -> Vec<u8> {
    let mut b: Vec<u8> = Vec::new();
    // ManifestHeader (72 B): gen, prev_gen, publisher_fxid, relfilenumber,
    // schema_fingerprint, magic, format_version, spc, db, part_count,
    // next_part_no, flags, reserved.
    b.extend_from_slice(&7u64.to_le_bytes());
    b.extend_from_slice(&6u64.to_le_bytes());
    b.extend_from_slice(&0x0000_0002_0000_1234u64.to_le_bytes());
    b.extend_from_slice(&90210u64.to_le_bytes());
    b.extend_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
    b.extend_from_slice(b"PRCM");
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1663u32.to_le_bytes());
    b.extend_from_slice(&5u32.to_le_bytes());
    b.extend_from_slice(&2u32.to_le_bytes());
    b.extend_from_slice(&4u32.to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes());
    assert_eq!(b.len(), 72);
    // PartRecord (64 B): rows, file_len, footer_off, dv_gen, dv_len,
    // part_no, flags, granule_count, band_count, dv_crc, pad.
    for (rows, file_len, footer_off, dv_gen, dv_len, part_no, gc, bc, dv_crc) in [
        (
            100_000u64,
            4_000_000u64,
            3_999_888u64,
            0u64,
            0u64,
            1u32,
            13u32,
            2u32,
            0u32,
        ),
        (65_536, 2_000_000, 1_999_888, 3, 2048, 3, 8, 1, 0xABCD_EF01),
    ] {
        b.extend_from_slice(&rows.to_le_bytes());
        b.extend_from_slice(&file_len.to_le_bytes());
        b.extend_from_slice(&footer_off.to_le_bytes());
        b.extend_from_slice(&dv_gen.to_le_bytes());
        b.extend_from_slice(&dv_len.to_le_bytes());
        b.extend_from_slice(&part_no.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes());
        b.extend_from_slice(&gc.to_le_bytes());
        b.extend_from_slice(&bc.to_le_bytes());
        b.extend_from_slice(&dv_crc.to_le_bytes());
        b.extend_from_slice(&0u32.to_le_bytes());
    }
    let crc = crc32c(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

#[test]
fn manifest_golden_encode_decode() {
    let m = sample_manifest();
    let encoded = m.encode();
    assert_eq!(
        encoded,
        hand_assembled_manifest(),
        "manifest wire layout drifted from spec §13.1"
    );
    let decoded = Manifest::decode(&encoded).expect("golden manifest decodes");
    assert_eq!(decoded, m);
}

#[test]
fn commit_pointer_golden_encode_decode() {
    let p = CommitPointer::new(9, 1234, 0x5566_7788);
    let encoded = p.encode();
    // Hand assembly per spec §13.2: gen, manifest_len, magic, version,
    // manifest_crc, crc.
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(&9u64.to_le_bytes());
    b.extend_from_slice(&1234u64.to_le_bytes());
    b.extend_from_slice(b"PRCC");
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&0x5566_7788u32.to_le_bytes());
    let crc = crc32c(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    assert_eq!(
        encoded.as_slice(),
        b.as_slice(),
        "commit pointer layout drifted from spec §13.2"
    );
    let d = CommitPointer::decode(&encoded).expect("golden pointer decodes");
    assert_eq!(d.gen, 9);
    assert_eq!(d.manifest_len, 1234);
    assert_eq!(d.magic, CURRENT_MAGIC);
    assert_eq!(d.manifest_crc, 0x5566_7788);
}

#[test]
fn stats_record_golden() {
    let r = StatsRecord {
        min_key: -5,
        max_key: 900,
        sum_i128: 123_456_789_012_345,
        zero_count: 3,
        byte_len_sum: 4096,
        nonnull: 8000,
        byte_len_min: 1,
        byte_len_max: 77,
        char_len_min: 1,
        char_len_max: 70,
        ndv_est: 42,
        key_kind: KeyKind::Exact.as_u8(),
        sortedness: Sortedness::Ascending.as_u8(),
        flags: 0,
        pad: 0,
    };
    let mut enc = Vec::new();
    r.encode_into(&mut enc);
    // Hand assembly per spec §8.1 order.
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(&(-5i64).to_le_bytes());
    b.extend_from_slice(&900i64.to_le_bytes());
    b.extend_from_slice(&123_456_789_012_345i128.to_le_bytes());
    b.extend_from_slice(&3u64.to_le_bytes());
    b.extend_from_slice(&4096u64.to_le_bytes());
    b.extend_from_slice(&8000u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&77u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&70u32.to_le_bytes());
    b.extend_from_slice(&42u32.to_le_bytes());
    b.push(1);
    b.push(1);
    b.extend_from_slice(&0u16.to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes());
    assert_eq!(enc, b, "stats record layout drifted from spec §8.1");
    let back = StatsRecord::decode(&mut crate::wire::Cur::new(&enc)).expect("decodes");
    assert_eq!(back, r);
}

#[test]
fn dv_golden_roundtrip() {
    let rows_g2: Vec<u16> = vec![0, 5, 8191];
    let rows_g7: Vec<u16> = (0..2000).map(|i| i * 4).collect();
    let img = encode_dv(
        11,
        4,
        &[
            (2, DvBlockKind::List, &rows_g2),
            (7, DvBlockKind::Bitmap, &rows_g7),
        ],
    )
    .expect("encodes");
    let mut rd = DvReader::open(&img).expect("opens");
    assert_eq!(rd.header.part_no, 11);
    assert_eq!(rd.header.gen, 4);
    assert_eq!(rd.header.deleted_rows, 3 + 2000);
    assert_eq!(rd.header.block_count, 2);
    let b1 = rd.next_block().expect("block").expect("some");
    assert_eq!(b1.granule, 2);
    assert_eq!(b1.kind, DvBlockKind::List);
    assert_eq!(b1.count, 3);
    assert_eq!(b1.payload, &[0u8, 0, 5, 0, 0xFF, 0x1F]);
    let b2 = rd.next_block().expect("block").expect("some");
    assert_eq!(b2.granule, 7);
    assert_eq!(b2.kind, DvBlockKind::Bitmap);
    assert_eq!(b2.count, 2000);
    assert_eq!(b2.payload.len(), 1024);
    // Bit 8 set (row 8), bit 1 clear.
    assert_eq!(b2.payload[1] & 1, 1);
    assert!(rd.next_block().expect("end").is_none());
    // Out-of-order granules refuse at encode.
    assert_eq!(
        encode_dv(
            1,
            1,
            &[
                (5, DvBlockKind::List, &rows_g2),
                (2, DvBlockKind::List, &rows_g2)
            ]
        ),
        Err(FormatError::EncodeContract {
            detail: "DV granule order"
        })
    );
}

// ---------------------------------------------------------------------------
// identity pins (spec §11/§5.5/§18) — exact values; drift = identity break
// ---------------------------------------------------------------------------

#[test]
fn part_uuid_golden() {
    let ident = PartIdent {
        dev: 64768,
        ino: 9_437_301,
        len: 4_000_000,
        footer_off: 3_999_888,
    };
    let uuid = part_uuid(&ident);
    // Determinism + sensitivity.
    assert_eq!(uuid, part_uuid(&ident));
    let mut other = ident;
    other.footer_off += 1;
    assert_ne!(uuid, part_uuid(&other));
    // Exact pin (frozen chain, spec §11).
    assert_eq!(
        uuid,
        [
            0xbe, 0x99, 0x48, 0x62, 0x21, 0x6d, 0xc4, 0x4a, 0xbf, 0x63, 0x77, 0x70, 0x98, 0xef,
            0xbc, 0x4c
        ],
        "part_uuid chain drifted — every cache identity breaks"
    );
}

#[test]
fn schema_fingerprint_golden() {
    // `semantics` is deliberately NOT a fingerprint input (see `ColSchema`):
    // both fixtures stay `Opaque` so the pin below stays a pure STORAGE-shape
    // fact and cannot drift on a metadata-policy change.
    let cols = [
        ColSchema {
            attno: 1,
            class: StorageClass::derive(8, true, b'd', ClassHint::Signed).expect("int8"),
            typlen: 8,
            typbyval: true,
            typalign: b'd',
            collation_class: CollationClass::C,
            semantics: TypeSemantics::Opaque,
        },
        ColSchema {
            attno: 2,
            class: StorageClass::derive(-1, false, b'i', ClassHint::None).expect("text"),
            typlen: -1,
            typbyval: false,
            typalign: b'i',
            collation_class: CollationClass::OtherDeterministic,
            semantics: TypeSemantics::Opaque,
        },
    ];
    let fp = schema_fingerprint(&cols);
    assert_eq!(fp, schema_fingerprint(&cols));
    // Column order matters; class facts matter.
    let swapped = [cols[1], cols[0]];
    assert_ne!(fp, schema_fingerprint(&swapped));
    // Exact pin (spec §5.5).
    assert_eq!(
        fp, 2_674_705_044_352_826_955,
        "schema fingerprint chain drifted"
    );
}

#[test]
fn logical_col_hash_golden_and_order_insensitive() {
    use crate::bank::LogicalColHash;
    let rows: [&[u8]; 4] = [b"alpha", b"beta", b"", b"alpha"];
    // Whole, forward.
    let mut a = LogicalColHash::new();
    for r in rows {
        a.observe(r);
    }
    a.observe_null();
    // Split + reversed + merged (the COPY-order ruling shape).
    let mut b1 = LogicalColHash::new();
    let mut b2 = LogicalColHash::new();
    b2.observe_null();
    for r in rows.iter().rev().take(2) {
        b2.observe(r);
    }
    for r in rows.iter().take(2) {
        b1.observe(r);
    }
    let mut b = LogicalColHash::new();
    b.merge(&b2);
    b.merge(&b1);
    assert_eq!(
        a.digest(),
        b.digest(),
        "logical hash must be partition/order-insensitive"
    );
    // Exact pin (spec §18).
    assert_eq!(
        a.digest(),
        (5_106_915_900_938_007_069, 3_191_542_252_960_352_835, 5),
        "logical column hash drifted — bank identities break"
    );
}

// ---------------------------------------------------------------------------
// names, rowid, varlena headers
// ---------------------------------------------------------------------------

#[test]
fn dirlayout_names_golden() {
    assert_eq!(table_dir_name(90210), "pgrc2_90210");
    assert_eq!(part_file_name(42), "part-42.pgrc2");
    assert_eq!(manifest_file_name(7), "manifest-7.pgrc2m");
    assert_eq!(CURRENT_FILE_NAME, "CURRENT");
    assert_eq!(CURRENT_TMP_FILE_NAME, "CURRENT.tmp");
    assert_eq!(
        sidecar_file_name(42, SidecarKind::Dv, 3),
        "part-42-dv-3.pgrc2s"
    );
    assert_eq!(temp_file_name(0x2_0000_1234, 7), "tmp-8589939252-7.pgrc2t");
    // Parse inverses.
    assert_eq!(parse_part_file_name("part-42.pgrc2"), Some(42));
    assert_eq!(parse_manifest_file_name("manifest-7.pgrc2m"), Some(7));
    assert_eq!(
        parse_sidecar_file_name("part-42-dv-3.pgrc2s"),
        Some((42, SidecarKind::Dv, 3))
    );
    assert!(is_temp_file_name("tmp-8589938228-7.pgrc2t"));
    // Canonical-only parsing: leading zeros, junk, wrong suffixes refuse.
    assert_eq!(parse_part_file_name("part-042.pgrc2"), None);
    assert_eq!(parse_part_file_name("part-42.pgrc2m"), None);
    assert_eq!(parse_manifest_file_name("manifest-.pgrc2m"), None);
    assert_eq!(parse_sidecar_file_name("part-42-xx-3.pgrc2s"), None);
    assert!(!is_temp_file_name("part-42.pgrc2"));
}

#[test]
fn rowid_pack_golden() {
    assert_eq!(pack_rowid(3, 5, 9), 0x3_0000_A009);
}

#[test]
fn varlena_header_golden() {
    // 4B-U: total length (payload + 4) << 2, low bits 00 (spec §1).
    assert_eq!(varlena_header_4b_u(0), 16);
    assert_eq!(varlena_header_4b_u(5), 36);
    assert_eq!(varlena_4b_u_payload_len(16, "t").expect("valid"), 0);
    assert_eq!(varlena_4b_u_payload_len(36, "t").expect("valid"), 5);
    // Flag bits set = not a plain 4B-U header.
    assert!(varlena_4b_u_payload_len(0xFFFF_FFFF, "t").is_err());
    assert!(varlena_4b_u_payload_len(0b01, "t").is_err());
}

#[test]
fn sortkey_record_golden() {
    let rec = SortKeyRecord {
        keys: vec![
            SortKeyEntry {
                attno: 2,
                dir: 0,
                nulls: 1,
                collation_class: 0,
                pad: 0,
            },
            SortKeyEntry {
                attno: 5,
                dir: 1,
                nulls: 0,
                collation_class: 1,
                pad: 0,
            },
        ],
    };
    let enc = rec.encode();
    // Hand assembly per spec §9.
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(&2u16.to_le_bytes());
    b.extend_from_slice(&0u16.to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes());
    b.extend_from_slice(&2u32.to_le_bytes());
    b.extend_from_slice(&[0, 1, 0, 0]);
    b.extend_from_slice(&5u32.to_le_bytes());
    b.extend_from_slice(&[1, 0, 1, 0]);
    assert_eq!(enc, b, "sort-key layout drifted from spec §9");
    assert_eq!(SortKeyRecord::decode(&enc).expect("decodes"), rec);
}
