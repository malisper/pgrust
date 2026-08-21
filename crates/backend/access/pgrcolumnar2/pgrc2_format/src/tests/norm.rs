//! `enc::stream_kernel_key` pins (A-lane amendment M3-A2): the ONE
//! normalization from §6.3 stream-entry vocabulary to the §19.5 dispatch
//! key. Born-RED shape: the DICT_CODES row proves the width byte is a stats
//! fact (ANY value normalizes to dispatch width 0 — a raw-field key with a
//! real max-code-width byte finds no kernel), and the structural ids refuse.

use crate::class::{CLASS_BOOL, CLASS_BYVAL, CLASS_F32, CLASS_F64, CLASS_FIXED, CLASS_VARLENA};
use crate::enc::{stream_kernel_key, EncodingId};
use crate::FormatError;

#[test]
fn dict_codes_width_byte_never_keys_dispatch() {
    // The §6.3 width byte carries the max code width (0..=32) — every legal
    // value maps to the SAME registered key {DICT_CODES, class, 0}.
    for w in [0u8, 1, 7, 8, 17, 32] {
        let k = stream_kernel_key(EncodingId::DictCodes.as_u16(), CLASS_VARLENA, w)
            .expect("dict stream normalizes");
        assert_eq!((k.encoding, k.class, k.width), (8, CLASS_VARLENA, 0));
        let kf = stream_kernel_key(EncodingId::DictCodes.as_u16(), CLASS_FIXED, w)
            .expect("fixed dict stream normalizes");
        assert_eq!(kf.width, 0);
    }
}

#[test]
fn verbatim_const_key_on_class_width() {
    for enc in [EncodingId::Verbatim, EncodingId::Const] {
        for w in [1u8, 2, 4, 8] {
            let k = stream_kernel_key(enc.as_u16(), CLASS_BYVAL, w).expect("byval");
            assert_eq!(k.width, w);
        }
        for bad in [0u8, 3, 5, 16] {
            assert!(
                stream_kernel_key(enc.as_u16(), CLASS_BYVAL, bad).is_err(),
                "corrupt byval width {bad} must refuse"
            );
        }
        assert_eq!(
            stream_kernel_key(enc.as_u16(), CLASS_F32, 4).unwrap().width,
            4
        );
        assert_eq!(
            stream_kernel_key(enc.as_u16(), CLASS_F64, 8).unwrap().width,
            8
        );
        assert_eq!(
            stream_kernel_key(enc.as_u16(), CLASS_BOOL, 1)
                .unwrap()
                .width,
            1
        );
        assert_eq!(
            stream_kernel_key(enc.as_u16(), CLASS_VARLENA, 0)
                .unwrap()
                .width,
            0
        );
    }
}

#[test]
fn elected_width_families_and_self_describing() {
    // BYTE_FOR / PACKED_NUMERIC key on the elected width, exactly — the
    // full {1..=8} byte-aligned ladder (SB-3 widened the v3 pow2 set).
    for enc in [EncodingId::ByteFor, EncodingId::PackedNumeric] {
        for w in 1u8..=8 {
            assert_eq!(
                stream_kernel_key(enc.as_u16(), CLASS_BYVAL, w)
                    .unwrap()
                    .width,
                w
            );
        }
        for bad in [0u8, 9, 16] {
            assert!(
                stream_kernel_key(enc.as_u16(), CLASS_BYVAL, bad).is_err(),
                "corrupt delta width {bad} must refuse"
            );
        }
    }
    // Self-describing families key on 0 regardless of the byte.
    for enc in [
        EncodingId::FforInterleave,
        EncodingId::DeltaFor,
        EncodingId::Alp,
        EncodingId::AlpRd,
    ] {
        assert_eq!(
            stream_kernel_key(enc.as_u16(), CLASS_F64, 9).unwrap().width,
            0
        );
    }
    // BOOL_BITMAP normalizes to the registered width 1.
    assert_eq!(
        stream_kernel_key(EncodingId::BoolBitmap.as_u16(), CLASS_BOOL, 0)
            .unwrap()
            .width,
        1
    );
    // FSST (first-class per SB-4/OD-5) is section-self-describing: its
    // symbol table rides the section header bytes, so the entry width byte
    // never keys dispatch.
    for w in [0u8, 5, 200] {
        assert_eq!(
            stream_kernel_key(EncodingId::Fsst.as_u16(), CLASS_VARLENA, w)
                .unwrap()
                .width,
            0
        );
    }
}

#[test]
fn structural_ids_refuse_in_stream_entries() {
    // Ratified (spec §4): ARRAY_DUAL/JSONB_SHRED are election-grain markers
    // — they never stamp an on-disk encoding field.
    for enc in [EncodingId::ArrayDual, EncodingId::JsonbShred] {
        assert!(matches!(
            stream_kernel_key(enc.as_u16(), CLASS_VARLENA, 0),
            Err(FormatError::Corrupt { .. })
        ));
    }
    // Unknown ids refuse BEFORE any width logic. (The old reserved-FSST
    // seed at id 12 resolves first-class as of SB-4/OD-5; the born-RED
    // seed shifts into the unassigned 13..=127 band.)
    assert!(matches!(
        stream_kernel_key(0xBEEF, CLASS_BYVAL, 8),
        Err(FormatError::UnknownEncoding { .. })
    ));
    assert!(matches!(
        stream_kernel_key(13, CLASS_VARLENA, 0),
        Err(FormatError::UnknownEncoding { id: 13 })
    ));
}

#[test]
fn reserved_shred_roles_refuse_until_activation() {
    use crate::part::{
        StreamRole, STREAM_ROLE_SHRED_EXCEPTION_MASK_RESERVED, STREAM_ROLE_SHRED_RESIDUAL_RESERVED,
    };
    assert_eq!(STREAM_ROLE_SHRED_EXCEPTION_MASK_RESERVED, 7);
    assert_eq!(STREAM_ROLE_SHRED_RESIDUAL_RESERVED, 8);
    for v in [7u8, 8] {
        assert!(
            matches!(
                StreamRole::from_u8(v),
                Err(FormatError::UnknownStreamRole { .. })
            ),
            "reserved shred role {v} must refuse until its activation amendment"
        );
    }
}
