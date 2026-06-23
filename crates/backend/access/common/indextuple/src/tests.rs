//! Round-trip and layout tests for the index form/deform core.
//!
//! These exercise the common path (fixed-width by-value + plain varlena), which
//! never reaches the detoast/compress seams. Every test threads a
//! `MemoryContext` handle, the translation of C's `CurrentMemoryContext`.

use alloc::vec;
use alloc::vec::Vec;

use mcx::{slice_in, Mcx, MemoryContext};
use types_tuple::heaptuple::Datum;
use types_tuple::heaptuple::{
    CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_CHAR, TYPALIGN_DOUBLE,
    TYPALIGN_INT, TYPALIGN_SHORT, TYPSTORAGE_PLAIN,
};

use crate::{
    index_deform_tuple, index_form_tuple, index_truncate_tuple, nocache_index_getattr,
    CopyIndexTuple,
};

fn byval(attlen: i16, attalignby: u8) -> CompactAttribute {
    CompactAttribute {
        attcacheoff: -1,
        attlen,
        attbyval: true,
        attispackable: false,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: 0,
        attalignby,
    }
}

fn varlena_attr() -> CompactAttribute {
    CompactAttribute {
        attcacheoff: -1,
        attlen: -1,
        attbyval: false,
        attispackable: true,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: 0,
        attalignby: 4, // TYPALIGN_INT
    }
}

/// A `FormData_pg_attribute` matching a compact attribute, with PLAIN storage
/// (so the compress branch is never taken for the varlena tests).
/// `CreateTupleDescTruncatedCopy` re-derives `compact_attrs` from these, so the
/// full record must carry consistent `attalign`/`attbyval`/`attlen`.
fn full_attr(ca: &CompactAttribute) -> FormData_pg_attribute {
    let attalign = match ca.attalignby {
        1 => TYPALIGN_CHAR,
        2 => TYPALIGN_SHORT,
        8 => TYPALIGN_DOUBLE,
        _ => TYPALIGN_INT,
    };
    FormData_pg_attribute {
        attlen: ca.attlen,
        attbyval: ca.attbyval,
        attalign,
        attstorage: TYPSTORAGE_PLAIN,
        attcompression: 0,
        ..FormData_pg_attribute::default()
    }
}

fn tupdesc<'mcx>(mcx: Mcx<'mcx>, attrs: &[CompactAttribute]) -> TupleDescData<'mcx> {
    let full: Vec<FormData_pg_attribute> = attrs.iter().map(full_attr).collect();
    TupleDescData {
        natts: attrs.len() as i32,
        tdtypeid: 2249, // RECORDOID
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: slice_in(mcx, attrs).unwrap(),
        attrs: slice_in(mcx, &full).unwrap(),
    }
}

fn byref<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> Datum<'mcx> {
    Datum::ByRef(slice_in(mcx, bytes).unwrap())
}

/// A 4-byte-header varlena carrying `payload` (length word includes the 4-byte
/// header, little-endian `len << 2`).
fn varlena_4b(payload: &[u8]) -> Vec<u8> {
    let total = 4 + payload.len();
    let mut v = vec![0u8; total];
    let word = (total as u32) << 2;
    v[0..4].copy_from_slice(&word.to_ne_bytes());
    v[4..].copy_from_slice(payload);
    v
}

#[test]
fn form_deform_all_byval_no_nulls() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let td = tupdesc(mcx, &[byval(4, 4), byval(2, 2), byval(4, 4)]);
    let values = vec![
        Datum::from_i32(0x01020304),
        Datum::from_i16(0x0506),
        Datum::from_i32(-1),
    ];
    let isnull = vec![false, false, false];

    let itup = index_form_tuple(mcx, &td, &values, &isnull).expect("form");
    assert!(!itup.has_nulls());
    assert!(itup.bits.is_empty());
    // data offset == MAXALIGN(8) == 8 (no nulls).
    assert_eq!(itup.data_offset(), 8);

    let cols = index_deform_tuple(mcx, &itup, &td).expect("deform");
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0], (Datum::from_i32(0x01020304), false));
    assert_eq!(cols[1], (Datum::from_i16(0x0506), false));
    assert_eq!(cols[2], (Datum::from_i32(-1), false));

    // nocache_index_getattr fetches a single (1-based) attribute.
    let (v, isn) = nocache_index_getattr(mcx, &itup, 3, &td).expect("getattr");
    assert!(!isn);
    assert_eq!(v, Datum::from_i32(-1));
}

#[test]
fn form_deform_with_nulls_sets_bitmap() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let td = tupdesc(mcx, &[byval(4, 4), byval(8, 8), byval(4, 4)]);
    let values = vec![
        Datum::from_i32(11),
        Datum::null(),
        Datum::from_i32(33),
    ];
    let isnull = vec![false, true, false];

    let itup = index_form_tuple(mcx, &td, &values, &isnull).expect("form");
    assert!(itup.has_nulls());
    assert!(!itup.bits.is_empty());
    // data offset == MAXALIGN(8 + 4) == 16 (with nulls).
    assert_eq!(itup.data_offset(), 16);

    let cols = index_deform_tuple(mcx, &itup, &td).expect("deform");
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0], (Datum::from_i32(11), false));
    assert_eq!(cols[1].1, true);
    assert_eq!(cols[2], (Datum::from_i32(33), false));
}

#[test]
fn form_deform_varlena_roundtrip() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let td = tupdesc(mcx, &[byval(4, 4), varlena_attr()]);
    let payload = b"hello index";
    let v = varlena_4b(payload);
    let values = vec![
        Datum::from_i32(7),
        byref(mcx, &v),
    ];
    let isnull = vec![false, false];

    let itup = index_form_tuple(mcx, &td, &values, &isnull).expect("form");
    assert!(itup.has_varwidths());

    let cols = index_deform_tuple(mcx, &itup, &td).expect("deform");
    assert_eq!(cols[0], (Datum::from_i32(7), false));
    // The varlena column round-trips as a short varlena (the fill path converts
    // a small 4-byte-header packable varlena to a 1-byte header).
    let Datum::ByRef(bytes) = &cols[1].0 else {
        panic!("expected ByRef varlena column");
    };
    // short header: low bit set, length = (header >> 1).
    assert_eq!(bytes[0] & 0x01, 0x01);
    let slen = ((bytes[0] >> 1) & 0x7F) as usize;
    assert_eq!(&bytes[1..slen], payload);
}

#[test]
fn copy_index_tuple_is_verbatim() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let td = tupdesc(mcx, &[byval(4, 4), byval(4, 4)]);
    let values = vec![
        Datum::from_i32(100),
        Datum::from_i32(200),
    ];
    let isnull = vec![false, false];

    let itup = index_form_tuple(mcx, &td, &values, &isnull).expect("form");
    let copy = CopyIndexTuple(mcx, &itup).expect("copy");
    assert_eq!(copy.header, itup.header);
    assert_eq!(copy.data.as_slice(), itup.data.as_slice());
    assert_eq!(copy.size(), itup.size());
}

#[test]
fn truncate_drops_trailing_attrs_and_keeps_tid() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let td = tupdesc(mcx, &[byval(4, 4), byval(4, 4), byval(4, 4)]);
    let values = vec![
        Datum::from_i32(1),
        Datum::from_i32(2),
        Datum::from_i32(3),
    ];
    let isnull = vec![false, false, false];

    let mut itup = index_form_tuple(mcx, &td, &values, &isnull).expect("form");
    itup.header.t_tid.ip_posid = 42;

    let trunc = index_truncate_tuple(mcx, &td, &itup, 2).expect("truncate");
    // t_tid is preserved across truncation.
    assert_eq!(trunc.header.t_tid.ip_posid, 42);
    // No larger than the original.
    assert!(trunc.size() <= itup.size());

    // Deform with a 2-attr descriptor: only the first two columns survive.
    let td2 = tupdesc(mcx, &[byval(4, 4), byval(4, 4)]);
    let cols = index_deform_tuple(mcx, &trunc, &td2).expect("deform");
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0], (Datum::from_i32(1), false));
    assert_eq!(cols[1], (Datum::from_i32(2), false));

    // No-truncation case returns a copy.
    let same = index_truncate_tuple(mcx, &td, &itup, 3).expect("truncate-noop");
    assert_eq!(same.size(), itup.size());
}

#[test]
fn on_disk_image_round_trips_header() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let td = tupdesc(mcx, &[byval(8, 8)]);
    let values = vec![Datum::from_i64(0x1122_3344_5566_7788)];
    let isnull = vec![false];

    let mut itup = index_form_tuple(mcx, &td, &values, &isnull).expect("form");
    itup.header.t_tid.ip_blkid.bi_hi = 0xABCD;
    itup.header.t_tid.ip_posid = 7;
    let image = itup.on_disk_image(mcx).expect("image");
    assert_eq!(image.len(), itup.size());
    // Header bytes 0..2 = bi_hi.
    assert_eq!(u16::from_ne_bytes([image[0], image[1]]), 0xABCD);
    // Bytes 4..6 = ip_posid.
    assert_eq!(u16::from_ne_bytes([image[4], image[5]]), 7);
    // Bytes 6..8 = t_info.
    assert_eq!(u16::from_ne_bytes([image[6], image[7]]), itup.header.t_info);
}
