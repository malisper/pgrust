//! The asm-check leg's compile-shape half (S4 pow2-switch law, §5 M3-C:
//! "dispatch is fn-pointer tables, no width switch in hot paths"): the
//! per-width kernels are DISTINCT monomorphized functions behind the
//! vtable — a width-switch regression (one shared body dispatching on a
//! runtime width) would collapse them to one pointer and fail these pins.
//! The runtime half (actual codegen inspection) rides the standing
//! asm-doctrine harness at M3-L's CI cluster legs.

use crate::bytefor::VT_BYTE_FOR;
use crate::dispatch::stream_kernel_key;
use crate::packednum::VT_PACKED_NUMERIC;
use pgrc2_format::abi::KernelKey;
use pgrc2_format::class::{CLASS_BOOL, CLASS_BYVAL, CLASS_F32, CLASS_F64, CLASS_VARLENA};
use pgrc2_format::enc::EncodingId;
use pgrc2_format::FormatError;

#[test]
fn per_width_kernels_are_distinct_functions() {
    // The SB-3 widening: the full {1..=8} ladder, every width its own
    // monomorphized body.
    assert_eq!(VT_BYTE_FOR.len(), 8);
    assert_eq!(VT_PACKED_NUMERIC.len(), 8);
    for (i, vt) in VT_BYTE_FOR.iter().enumerate() {
        assert_eq!(vt.key.width, i as u8 + 1, "widths ladder 1..=8 in order");
    }
    let bf: Vec<usize> = VT_BYTE_FOR.iter().map(|v| v.decode_full as usize).collect();
    for i in 0..bf.len() {
        for j in i + 1..bf.len() {
            assert_ne!(
                bf[i], bf[j],
                "BYTE_FOR widths {i}/{j} share a decode_full body (width-switch regression)"
            );
        }
    }
    let bfs: Vec<usize> = VT_BYTE_FOR.iter().map(|v| v.decode_sel as usize).collect();
    for i in 0..bfs.len() {
        for j in i + 1..bfs.len() {
            assert_ne!(bfs[i], bfs[j], "BYTE_FOR decode_sel widths {i}/{j} shared");
        }
    }
    let pn: Vec<usize> = VT_PACKED_NUMERIC
        .iter()
        .map(|v| v.decode_full as usize)
        .collect();
    for i in 0..pn.len() {
        for j in i + 1..pn.len() {
            assert_ne!(pn[i], pn[j], "PACKED_NUMERIC widths {i}/{j} shared");
        }
    }
}

#[test]
fn registry_resolves_every_shipped_key() {
    let reg = crate::registry();
    let mut keys = vec![
        KernelKey {
            encoding: EncodingId::DeltaFor.as_u16(),
            class: CLASS_BYVAL,
            width: 0,
        },
        KernelKey {
            encoding: EncodingId::FforInterleave.as_u16(),
            class: CLASS_BYVAL,
            width: 0,
        },
        KernelKey {
            encoding: EncodingId::Alp.as_u16(),
            class: CLASS_F64,
            width: 0,
        },
        KernelKey {
            encoding: EncodingId::AlpRd.as_u16(),
            class: CLASS_F64,
            width: 0,
        },
        KernelKey {
            encoding: EncodingId::BoolBitmap.as_u16(),
            class: CLASS_BOOL,
            width: 1,
        },
        KernelKey {
            encoding: EncodingId::DictCodes.as_u16(),
            class: CLASS_VARLENA,
            width: 0,
        },
        // The SB-5 f32 ALP arm and the SB-4 first-class FSST arm.
        KernelKey {
            encoding: EncodingId::Alp.as_u16(),
            class: CLASS_F32,
            width: 0,
        },
        KernelKey {
            encoding: EncodingId::Fsst.as_u16(),
            class: CLASS_VARLENA,
            width: 0,
        },
    ];
    // BYTE_FOR / PACKED_NUMERIC: the full SB-3 width ladder.
    for w in 1u8..=8 {
        keys.push(KernelKey {
            encoding: EncodingId::ByteFor.as_u16(),
            class: CLASS_BYVAL,
            width: w,
        });
        keys.push(KernelKey {
            encoding: EncodingId::PackedNumeric.as_u16(),
            class: CLASS_VARLENA,
            width: w,
        });
    }
    for w in [1u8, 2, 4, 8] {
        keys.push(KernelKey {
            encoding: EncodingId::Verbatim.as_u16(),
            class: CLASS_BYVAL,
            width: w,
        });
        keys.push(KernelKey {
            encoding: EncodingId::Const.as_u16(),
            class: CLASS_BYVAL,
            width: w,
        });
    }
    for k in keys {
        reg.resolve(k)
            .unwrap_or_else(|e| panic!("key {k:?} must resolve: {e}"));
    }
}

#[test]
fn stream_key_normalization() {
    // BYTE_FOR keys on the delta width byte — the full SB-3 ladder.
    for w in 1u8..=8 {
        let k = stream_kernel_key(EncodingId::ByteFor.as_u16(), CLASS_BYVAL, w).expect("ok");
        assert_eq!(k.width, w);
        crate::registry().resolve(k).expect("resolves");
    }
    // Dict streams normalize their max-code-width byte to 0.
    let k = stream_kernel_key(EncodingId::DictCodes.as_u16(), CLASS_VARLENA, 17).expect("ok");
    assert_eq!(k.width, 0);
    crate::registry().resolve(k).expect("resolves");
    // ALP family keys on width 0 regardless of the width byte — both
    // float classes (the SB-5 f32 arm registers at CLASS_F32).
    let k = stream_kernel_key(EncodingId::Alp.as_u16(), CLASS_F64, 0).expect("ok");
    crate::registry().resolve(k).expect("resolves");
    let k = stream_kernel_key(EncodingId::Alp.as_u16(), CLASS_F32, 7).expect("ok");
    assert_eq!(k.width, 0);
    crate::registry().resolve(k).expect("resolves");
    // FSST (first-class, SB-4): section-self-describing, width byte never
    // keys dispatch; the varlena kernel resolves.
    let k = stream_kernel_key(EncodingId::Fsst.as_u16(), CLASS_VARLENA, 9).expect("ok");
    assert_eq!(k.width, 0);
    crate::registry().resolve(k).expect("resolves");
    // Structural elections refuse in stream entries.
    for id in [EncodingId::ArrayDual, EncodingId::JsonbShred] {
        assert!(matches!(
            stream_kernel_key(id.as_u16(), CLASS_VARLENA, 0),
            Err(FormatError::Corrupt { .. })
        ));
    }
    // Unknown IDs refuse typed BEFORE any table walk (the old reserved-12
    // seed resolves first-class now; the seed shifts to the 13..=127 band).
    assert!(matches!(
        stream_kernel_key(13, CLASS_VARLENA, 0),
        Err(FormatError::UnknownEncoding { id: 13 })
    ));
    assert!(matches!(
        stream_kernel_key(0x7F, CLASS_BYVAL, 8),
        Err(FormatError::UnknownEncoding { .. })
    ));
    // A corrupt width byte on a width-keyed encoding refuses (widths 3 and
    // 5..7 are LEGAL as of SB-3; 0 and 9+ stay corrupt).
    for bad in [0u8, 9, 16] {
        assert!(matches!(
            stream_kernel_key(EncodingId::ByteFor.as_u16(), CLASS_BYVAL, bad),
            Err(FormatError::Corrupt { .. })
        ));
    }
}
