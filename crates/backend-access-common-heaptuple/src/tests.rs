//! Round-trip and layout tests for the form/deform core.
//!
//! These exercise the common catalog path (fixed-width by-value + plain
//! varlena/cstring), which never touches the expanded-object seams.

use alloc::vec;
use alloc::vec::Vec;

use types_tuple::heaptuple::{
    CompactAttribute, TupleDescData, TYPALIGN_CHAR, TYPALIGN_DOUBLE, TYPALIGN_INT,
};
use types_datum::Datum;

use crate::{
    heap_compute_data_size, heap_deform_tuple, heap_form_tuple, TupleValue,
};

/// Build a `CompactAttribute` for a by-value type of the given `attlen`/align.
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

/// Build a `CompactAttribute` for a varlena (`attlen == -1`) type.
fn varlena() -> CompactAttribute {
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

fn tupdesc(attrs: Vec<CompactAttribute>) -> TupleDescData {
    TupleDescData {
        natts: attrs.len() as i32,
        tdtypeid: 2249, // RECORDOID
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: attrs,
        // form/deform read only `compact_attrs`; the full attribute array is
        // not consulted, so the test descriptors leave it empty.
        attrs: Vec::new(),
    }
}

/// A 4-byte-header varlena datum carrying `payload` (length-word includes the
/// 4-byte header, little-endian `len << 2`).
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
    // int4, int2, int4
    let td = tupdesc(vec![
        byval(4, 4),
        byval(2, 2),
        byval(4, 4),
    ]);
    let values = vec![
        TupleValue::ByVal(Datum::from_i32(0x01020304)),
        TupleValue::ByVal(Datum::from_i16(0x0506)),
        TupleValue::ByVal(Datum::from_i32(-1)),
    ];
    let isnull = vec![false, false, false];

    let formed = heap_form_tuple(&td, &values, &isnull).expect("form");
    // No nulls => HEAP_HASNULL not set, t_bits empty.
    let td_hdr = formed.tuple.t_data.as_ref().unwrap();
    assert_eq!(td_hdr.t_infomask & types_tuple::heaptuple::HEAP_HASNULL, 0);
    assert!(td_hdr.t_bits.is_empty());
    // t_hoff == MAXALIGN(23) == 24.
    assert_eq!(td_hdr.t_hoff, 24);

    let cols = heap_deform_tuple(&formed.tuple, &td, &formed.data);
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0], (TupleValue::ByVal(Datum::from_i32(0x01020304)), false));
    assert_eq!(cols[1], (TupleValue::ByVal(Datum::from_i16(0x0506)), false));
    assert_eq!(cols[2], (TupleValue::ByVal(Datum::from_i32(-1)), false));
}

#[test]
fn form_deform_with_nulls_sets_bitmap() {
    let td = tupdesc(vec![byval(4, 4), byval(8, 8), byval(4, 4)]);
    let values = vec![
        TupleValue::ByVal(Datum::from_i32(11)),
        TupleValue::ByVal(Datum::null()),
        TupleValue::ByVal(Datum::from_i32(33)),
    ];
    let isnull = vec![false, true, false];

    let formed = heap_form_tuple(&td, &values, &isnull).expect("form");
    let hdr = formed.tuple.t_data.as_ref().unwrap();
    assert_ne!(hdr.t_infomask & types_tuple::heaptuple::HEAP_HASNULL, 0);
    // BITMAPLEN(3) == 1 byte; bit 0 and bit 2 set (non-null), bit 1 clear.
    assert_eq!(hdr.t_bits.len(), 1);
    assert_eq!(hdr.t_bits[0] & 0b0000_0001, 0b0000_0001);
    assert_eq!(hdr.t_bits[0] & 0b0000_0010, 0);
    assert_eq!(hdr.t_bits[0] & 0b0000_0100, 0b0000_0100);

    let cols = heap_deform_tuple(&formed.tuple, &td, &formed.data);
    assert_eq!(cols[0], (TupleValue::ByVal(Datum::from_i32(11)), false));
    assert_eq!(cols[1].1, true);
    assert_eq!(cols[2], (TupleValue::ByVal(Datum::from_i32(33)), false));
}

#[test]
fn form_deform_varlena_roundtrip() {
    // int4, text (varlena). The payload must be long enough that the datum
    // CANNOT be packed into a short (1-byte) header (VARATT_CAN_MAKE_SHORT
    // requires VARSIZE - VARHDRSZ + VARHDRSZ_SHORT <= 0x7F == 127), so the
    // 4-byte header survives verbatim.  A 130-byte payload => total 134 =>
    // 134 - 4 + 1 = 131 > 127 => not packable.
    let td = tupdesc(vec![byval(4, 4), varlena()]);
    let payload = vec![b'x'; 130];
    let vl = varlena_4b(&payload);
    let values = vec![
        TupleValue::ByVal(Datum::from_i32(42)),
        TupleValue::ByRef(vl.clone()),
    ];
    let isnull = vec![false, false];

    let formed = heap_form_tuple(&td, &values, &isnull).expect("form");
    let hdr = formed.tuple.t_data.as_ref().unwrap();
    assert_ne!(hdr.t_infomask & types_tuple::heaptuple::HEAP_HASVARWIDTH, 0);

    let cols = heap_deform_tuple(&formed.tuple, &td, &formed.data);
    assert_eq!(cols[0], (TupleValue::ByVal(Datum::from_i32(42)), false));
    // The non-packable varlena keeps its 4-byte header verbatim.
    match &cols[1].0 {
        TupleValue::ByRef(b) => assert_eq!(b, &vl),
        other => panic!("expected ByRef, got {other:?}"),
    }
    assert_eq!(cols[1].1, false);
}

#[test]
fn non_packable_varlena_short_header_preserved() {
    // A varlena type that is NOT packable (attispackable == false) keeps even a
    // 4-byte header for a small payload — no short conversion happens.
    let mut vl_att = varlena();
    vl_att.attispackable = false;
    let td = tupdesc(vec![vl_att]);
    let vl = varlena_4b(b"tiny");
    let values = vec![TupleValue::ByRef(vl.clone())];
    let isnull = vec![false];

    let formed = heap_form_tuple(&td, &values, &isnull).expect("form");
    let cols = heap_deform_tuple(&formed.tuple, &td, &formed.data);
    match &cols[0].0 {
        TupleValue::ByRef(b) => assert_eq!(b, &vl),
        other => panic!("expected ByRef 4-byte varlena, got {other:?}"),
    }
}

#[test]
fn short_varlena_conversion() {
    // A short (<= 0x7F payload incl. short header) packable varlena is
    // converted to a 1-byte header on the way in.
    let td = tupdesc(vec![varlena()]);
    let payload = b"short";
    let vl = varlena_4b(payload); // 4-byte header on the way in
    let values = vec![TupleValue::ByRef(vl)];
    let isnull = vec![false];

    let formed = heap_form_tuple(&td, &values, &isnull).expect("form");
    // After conversion the stored datum has a 1-byte header: total = payload+1.
    let cols = heap_deform_tuple(&formed.tuple, &td, &formed.data);
    match &cols[0].0 {
        TupleValue::ByRef(b) => {
            // 1-byte header (low bit set), length = payload.len() + 1.
            assert_eq!(b[0] & 0x01, 0x01);
            assert_eq!(((b[0] >> 1) & 0x7F) as usize, payload.len() + 1);
            assert_eq!(&b[1..], payload);
        }
        other => panic!("expected ByRef short varlena, got {other:?}"),
    }
}

#[test]
fn compute_data_size_matches_form() {
    let td = tupdesc(vec![byval(4, 4), byval(8, 8), varlena()]);
    let vl = varlena_4b(b"payload-bytes");
    let values = vec![
        TupleValue::ByVal(Datum::from_i32(1)),
        TupleValue::ByVal(Datum::from_usize(2)),
        TupleValue::ByRef(vl),
    ];
    let isnull = vec![false, false, false];

    let size = heap_compute_data_size(&td, &values, &isnull).unwrap();
    let formed = heap_form_tuple(&td, &values, &isnull).expect("form");
    assert_eq!(formed.data.len(), size);
    // t_len == t_hoff + data_len.
    let hdr = formed.tuple.t_data.as_ref().unwrap();
    assert_eq!(formed.tuple.t_len as usize, hdr.t_hoff as usize + size);
}

#[test]
fn cstring_roundtrip() {
    // attlen == -2 cstring.
    let mut att = byval(-2, 1);
    att.attbyval = false;
    let td = tupdesc(vec![att]);
    let cstr = b"abc\0".to_vec();
    let values = vec![TupleValue::ByRef(cstr.clone())];
    let isnull = vec![false];

    let formed = heap_form_tuple(&td, &values, &isnull).expect("form");
    let cols = heap_deform_tuple(&formed.tuple, &td, &formed.data);
    match &cols[0].0 {
        TupleValue::ByRef(b) => assert_eq!(b, &cstr),
        other => panic!("expected cstring ByRef, got {other:?}"),
    }
}

#[test]
fn too_many_columns_errors() {
    // natts beyond MaxTupleAttributeNumber (1664) -> error. We don't allocate
    // 1665 attrs; just set natts high with a small attrs vec is not valid for
    // the body, so guard only checks natts before touching arrays.
    let td = TupleDescData {
        natts: types_tuple::heaptuple::MaxTupleAttributeNumber + 1,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: Vec::new(),
        // form/deform read only `compact_attrs`; the full attribute array is
        // not consulted, and the natts guard fires before any array access.
        attrs: Vec::new(),
    };
    let err = heap_form_tuple(&td, &[], &[]).unwrap_err();
    assert_eq!(
        err,
        crate::HeapTupleError::TooManyColumns {
            columns: types_tuple::heaptuple::MaxTupleAttributeNumber + 1,
            limit: types_tuple::heaptuple::MaxTupleAttributeNumber,
        }
    );
}

#[test]
fn alignment_padding_double() {
    // char(1 byte, char-align) then a double-aligned 8-byte byval => the second
    // field must start at offset 8 (7 pad bytes).
    let td = tupdesc(vec![byval(1, 1), byval(8, 8)]);
    let values = vec![
        TupleValue::ByVal(Datum::from_usize(0x7f)),
        TupleValue::ByVal(Datum::from_usize(0x1122_3344_5566_7788)),
    ];
    let isnull = vec![false, false];
    let formed = heap_form_tuple(&td, &values, &isnull).expect("form");
    // data_len = 8 (double-align padding) + 8 = 16.
    assert_eq!(formed.data.len(), 16);
    let cols = heap_deform_tuple(&formed.tuple, &td, &formed.data);
    assert_eq!(cols[0], (TupleValue::ByVal(Datum::from_usize(0x7f)), false));
    assert_eq!(
        cols[1],
        (TupleValue::ByVal(Datum::from_usize(0x1122_3344_5566_7788)), false)
    );
    // silence unused-const import lints for the align consts.
    let _ = (TYPALIGN_CHAR, TYPALIGN_INT, TYPALIGN_DOUBLE);
}

#[test]
fn heap_modify_tuple_overlays_replaced_columns() {
    use crate::heap_modify_tuple;
    use types_tuple::heaptuple::{BlockIdData, ItemPointerData};

    // (int4, int4, int4): replace only the middle column; the others must come
    // from the old tuple. Also check the identity (t_ctid/t_self/t_tableOid) is
    // carried over from the old tuple to the new one, as C does.
    let td = tupdesc(vec![byval(4, 4), byval(4, 4), byval(4, 4)]);
    let old_values = vec![
        TupleValue::ByVal(Datum::from_i32(10)),
        TupleValue::ByVal(Datum::from_i32(20)),
        TupleValue::ByVal(Datum::from_i32(30)),
    ];
    let old_isnull = vec![false, false, false];
    let mut old = heap_form_tuple(&td, &old_values, &old_isnull).expect("form old");

    // Stamp a distinctive identity on the old tuple so the copy is observable.
    let stamped_ctid = ItemPointerData {
        ip_blkid: BlockIdData { bi_hi: 0, bi_lo: 7 },
        ip_posid: 3,
    };
    old.tuple.t_self = stamped_ctid;
    old.tuple.t_tableOid = 1259; // pg_class
    if let Some(h) = old.tuple.t_data.as_mut() {
        h.t_ctid = stamped_ctid;
    }

    // Replace column 2 (index 1) with 99; leave columns 1 and 3 untouched.
    let repl_values = vec![
        TupleValue::ByVal(Datum::from_i32(0)), // ignored (doReplace=false)
        TupleValue::ByVal(Datum::from_i32(99)),
        TupleValue::ByVal(Datum::from_i32(0)), // ignored
    ];
    let repl_isnull = vec![false, false, false];
    let do_replace = vec![false, true, false];

    let new = heap_modify_tuple(&old, &td, &repl_values, &repl_isnull, &do_replace)
        .expect("modify");

    let cols = heap_deform_tuple(&new.tuple, &td, &new.data);
    assert_eq!(cols[0], (TupleValue::ByVal(Datum::from_i32(10)), false)); // from old
    assert_eq!(cols[1], (TupleValue::ByVal(Datum::from_i32(99)), false)); // replaced
    assert_eq!(cols[2], (TupleValue::ByVal(Datum::from_i32(30)), false)); // from old

    // Identity carried over from the old tuple (heaptuple.c:1258-1260).
    assert_eq!(new.tuple.t_self, stamped_ctid);
    assert_eq!(new.tuple.t_tableOid, 1259);
    assert_eq!(new.tuple.t_data.as_ref().unwrap().t_ctid, stamped_ctid);
}

#[test]
fn heap_modify_tuple_can_set_column_null() {
    use crate::heap_modify_tuple;

    // Replacing a column with isnull=true must set the null bitmap bit.
    let td = tupdesc(vec![byval(4, 4), byval(4, 4)]);
    let old = heap_form_tuple(
        &td,
        &[
            TupleValue::ByVal(Datum::from_i32(1)),
            TupleValue::ByVal(Datum::from_i32(2)),
        ],
        &[false, false],
    )
    .expect("form old");
    // old tuple had no nulls.
    assert_eq!(
        old.tuple.t_data.as_ref().unwrap().t_infomask & types_tuple::heaptuple::HEAP_HASNULL,
        0
    );

    // Replace column 1 (index 0) with NULL; leave column 2 from the old tuple.
    let new = heap_modify_tuple(
        &old,
        &td,
        &[
            TupleValue::ByVal(Datum::null()),
            TupleValue::ByVal(Datum::from_i32(0)),
        ],
        &[true, false],
        &[true, false],
    )
    .expect("modify");

    let hdr = new.tuple.t_data.as_ref().unwrap();
    assert_ne!(hdr.t_infomask & types_tuple::heaptuple::HEAP_HASNULL, 0);
    let cols = heap_deform_tuple(&new.tuple, &td, &new.data);
    assert_eq!(cols[0].1, true); // now null
    assert_eq!(cols[1], (TupleValue::ByVal(Datum::from_i32(2)), false)); // from old
}

// ===========================================================================
// modify / copy / free / form-minimal tests
//
// (crate:: paths used throughout so these tests do not touch the shared `use`
// block at the top of this file.)
// ===========================================================================

/// Form a simple int4/int4/int4 tuple and set a recognizable identity on it.
fn formed_three_ints(a: i32, b: i32, c: i32) -> crate::FormedTuple {
    let td = tupdesc(vec![byval(4, 4), byval(4, 4), byval(4, 4)]);
    let values = vec![
        TupleValue::ByVal(Datum::from_i32(a)),
        TupleValue::ByVal(Datum::from_i32(b)),
        TupleValue::ByVal(Datum::from_i32(c)),
    ];
    let isnull = vec![false, false, false];
    crate::heap_form_tuple(&td, &values, &isnull).expect("form")
}

#[test]
fn copytuple_is_deep_and_equal() {
    let mut orig = formed_three_ints(10, 20, 30);
    // Give it a distinguishable identity (t_self/t_tableOid/t_ctid).
    orig.tuple.t_self = types_tuple::heaptuple::ItemPointerData::new(7, 3);
    orig.tuple.t_tableOid = 12345;
    if let Some(td) = orig.tuple.t_data.as_mut() {
        td.t_ctid = types_tuple::heaptuple::ItemPointerData::new(7, 3);
    }

    let copy = crate::heap_copytuple(Some(&orig)).expect("copy is Some");
    // Same content + identity.
    assert_eq!(copy.tuple.t_len, orig.tuple.t_len);
    assert_eq!(copy.tuple.t_self, orig.tuple.t_self);
    assert_eq!(copy.tuple.t_tableOid, orig.tuple.t_tableOid);
    assert_eq!(copy.data, orig.data);
    assert_eq!(
        copy.tuple.t_data.as_ref().unwrap().t_ctid,
        orig.tuple.t_data.as_ref().unwrap().t_ctid
    );

    // Deep: mutating the copy's data must not change the original.
    let mut copy2 = copy.clone();
    copy2.data[0] ^= 0xFF;
    assert_ne!(copy2.data, orig.data);

    // C returns NULL on a NULL input; here None.
    assert!(crate::heap_copytuple(None).is_none());
}

#[test]
fn copytuple_none_on_no_t_data() {
    let mut orig = formed_three_ints(1, 2, 3);
    orig.tuple.t_data = None;
    assert!(crate::heap_copytuple(Some(&orig)).is_none());
}

#[test]
fn copytuple_with_tuple_sets_null_data_on_invalid_src() {
    // Valid src => deep copy.
    let orig = formed_three_ints(5, 6, 7);
    let dest = crate::heap_copytuple_with_tuple(Some(&orig));
    assert_eq!(dest.tuple.t_len, orig.tuple.t_len);
    assert_eq!(dest.data, orig.data);
    assert!(dest.tuple.t_data.is_some());

    // Invalid src (None) => dest->t_data == NULL.
    let dest_null = crate::heap_copytuple_with_tuple(None);
    assert!(dest_null.tuple.t_data.is_none());
    assert!(dest_null.data.is_empty());
}

#[test]
fn freetuple_consumes() {
    // heap_freetuple just drops; assert it accepts the value by-move (compiles &
    // runs without leaking the borrow).
    let t = formed_three_ints(1, 2, 3);
    crate::heap_freetuple(t);
}

#[test]
fn modify_tuple_replaces_selected_columns_and_keeps_identity() {
    let mut orig = formed_three_ints(10, 20, 30);
    orig.tuple.t_self = types_tuple::heaptuple::ItemPointerData::new(9, 4);
    orig.tuple.t_tableOid = 999;
    if let Some(td) = orig.tuple.t_data.as_mut() {
        td.t_ctid = types_tuple::heaptuple::ItemPointerData::new(9, 4);
    }

    let td = tupdesc(vec![byval(4, 4), byval(4, 4), byval(4, 4)]);
    // Replace only column 1 (0-based) with 222.
    let repl_values = vec![
        TupleValue::ByVal(Datum::null()),
        TupleValue::ByVal(Datum::from_i32(222)),
        TupleValue::ByVal(Datum::null()),
    ];
    let repl_isnull = vec![false, false, false];
    let do_replace = vec![false, true, false];

    let new_t = crate::heap_modify_tuple(&orig, &td, &repl_values, &repl_isnull, &do_replace)
        .expect("modify");

    let cols = crate::heap_deform_tuple(&new_t.tuple, &td, &new_t.data);
    assert_eq!(cols[0], (TupleValue::ByVal(Datum::from_i32(10)), false));
    assert_eq!(cols[1], (TupleValue::ByVal(Datum::from_i32(222)), false));
    assert_eq!(cols[2], (TupleValue::ByVal(Datum::from_i32(30)), false));

    // identity copied from the old tuple.
    assert_eq!(new_t.tuple.t_self, orig.tuple.t_self);
    assert_eq!(new_t.tuple.t_tableOid, orig.tuple.t_tableOid);
    assert_eq!(
        new_t.tuple.t_data.as_ref().unwrap().t_ctid,
        orig.tuple.t_data.as_ref().unwrap().t_ctid
    );
}

#[test]
fn modify_tuple_can_set_a_column_null() {
    let orig = formed_three_ints(10, 20, 30);
    let td = tupdesc(vec![byval(4, 4), byval(4, 4), byval(4, 4)]);
    let repl_values = vec![
        TupleValue::ByVal(Datum::null()),
        TupleValue::ByVal(Datum::null()),
        TupleValue::ByVal(Datum::null()),
    ];
    let repl_isnull = vec![false, true, false];
    let do_replace = vec![false, true, false];

    let new_t = crate::heap_modify_tuple(&orig, &td, &repl_values, &repl_isnull, &do_replace)
        .expect("modify");
    let cols = crate::heap_deform_tuple(&new_t.tuple, &td, &new_t.data);
    assert_eq!(cols[0], (TupleValue::ByVal(Datum::from_i32(10)), false));
    assert_eq!(cols[1].1, true); // now null
    assert_eq!(cols[2], (TupleValue::ByVal(Datum::from_i32(30)), false));
    // HEAP_HASNULL now set on the new tuple.
    assert_ne!(
        new_t.tuple.t_data.as_ref().unwrap().t_infomask & types_tuple::heaptuple::HEAP_HASNULL,
        0
    );
}

#[test]
fn modify_tuple_by_cols_replaces_by_attnum() {
    let mut orig = formed_three_ints(10, 20, 30);
    orig.tuple.t_tableOid = 42;
    let td = tupdesc(vec![byval(4, 4), byval(4, 4), byval(4, 4)]);

    // Replace columns 1 and 3 (1-based) with 111 and 333.
    let repl_cols = vec![1, 3];
    let repl_values = vec![
        TupleValue::ByVal(Datum::from_i32(111)),
        TupleValue::ByVal(Datum::from_i32(333)),
    ];
    let repl_isnull = vec![false, false];

    let new_t = crate::heap_modify_tuple_by_cols(
        &orig, &td, 2, &repl_cols, &repl_values, &repl_isnull,
    )
    .expect("modify_by_cols");

    let cols = crate::heap_deform_tuple(&new_t.tuple, &td, &new_t.data);
    assert_eq!(cols[0], (TupleValue::ByVal(Datum::from_i32(111)), false));
    assert_eq!(cols[1], (TupleValue::ByVal(Datum::from_i32(20)), false));
    assert_eq!(cols[2], (TupleValue::ByVal(Datum::from_i32(333)), false));
    assert_eq!(new_t.tuple.t_tableOid, 42);
}

#[test]
fn modify_tuple_by_cols_rejects_out_of_range_column() {
    let orig = formed_three_ints(1, 2, 3);
    let td = tupdesc(vec![byval(4, 4), byval(4, 4), byval(4, 4)]);

    // attnum 0 is invalid (1-based).
    let err = crate::heap_modify_tuple_by_cols(
        &orig,
        &td,
        1,
        &[0],
        &[TupleValue::ByVal(Datum::from_i32(9))],
        &[false],
    )
    .unwrap_err();
    assert_eq!(err, crate::HeapTupleError::InvalidColumnNumber { attnum: 0 });

    // attnum natts+1 is invalid.
    let err2 = crate::heap_modify_tuple_by_cols(
        &orig,
        &td,
        1,
        &[4],
        &[TupleValue::ByVal(Datum::from_i32(9))],
        &[false],
    )
    .unwrap_err();
    assert_eq!(err2, crate::HeapTupleError::InvalidColumnNumber { attnum: 4 });
}

#[test]
fn form_minimal_tuple_no_nulls_layout() {
    // int4, int4 => no nulls. SizeofMinimalTupleHeader == 15;
    // hoff = MAXALIGN(15) == 16; t_hoff = 16 + MINIMAL_TUPLE_OFFSET(8) == 24.
    let td = tupdesc(vec![byval(4, 4), byval(4, 4)]);
    let values = vec![
        TupleValue::ByVal(Datum::from_i32(100)),
        TupleValue::ByVal(Datum::from_i32(200)),
    ];
    let isnull = vec![false, false];

    let mt = crate::heap_form_minimal_tuple(&td, &values, &isnull, 0).expect("form_minimal");
    // No nulls.
    assert_eq!(mt.tuple.t_infomask & types_tuple::heaptuple::HEAP_HASNULL, 0);
    assert!(mt.tuple.t_bits.is_empty());
    // t_hoff includes MINIMAL_TUPLE_OFFSET.
    assert_eq!(mt.tuple.t_hoff, 24);
    // natts encoded in t_infomask2.
    assert_eq!(mt.tuple.t_infomask2 & types_tuple::heaptuple::HEAP_NATTS_MASK, 2);
    // data_len: int4 at 0, int4 at 4 => 8.  t_len == hoff(16) + 8 == 24.
    assert_eq!(mt.data.len(), 8);
    assert_eq!(mt.tuple.t_len as usize, 16 + 8);
}

#[test]
fn form_minimal_tuple_with_nulls_sets_bitmap() {
    let td = tupdesc(vec![byval(4, 4), byval(4, 4), byval(4, 4)]);
    let values = vec![
        TupleValue::ByVal(Datum::from_i32(1)),
        TupleValue::ByVal(Datum::null()),
        TupleValue::ByVal(Datum::from_i32(3)),
    ];
    let isnull = vec![false, true, false];

    let mt = crate::heap_form_minimal_tuple(&td, &values, &isnull, 0).expect("form_minimal");
    assert_ne!(mt.tuple.t_infomask & types_tuple::heaptuple::HEAP_HASNULL, 0);
    // BITMAPLEN(3) == 1 byte; bit0 & bit2 set, bit1 clear.
    assert_eq!(mt.tuple.t_bits.len(), 1);
    assert_eq!(mt.tuple.t_bits[0] & 0b0000_0001, 0b0000_0001);
    assert_eq!(mt.tuple.t_bits[0] & 0b0000_0010, 0);
    assert_eq!(mt.tuple.t_bits[0] & 0b0000_0100, 0b0000_0100);
}

#[test]
fn form_minimal_tuple_too_many_columns_errors() {
    let td = TupleDescData {
        natts: types_tuple::heaptuple::MaxTupleAttributeNumber + 1,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: Vec::new(),
        attrs: Vec::new(),
    };
    let err = crate::heap_form_minimal_tuple(&td, &[], &[], 0).unwrap_err();
    assert_eq!(
        err,
        crate::HeapTupleError::TooManyColumns {
            columns: types_tuple::heaptuple::MaxTupleAttributeNumber + 1,
            limit: types_tuple::heaptuple::MaxTupleAttributeNumber,
        }
    );
}

// ===========================================================================
// attisnull / getsysattr / nocachegetattr / expand / minimal round-trip tests
// ===========================================================================

#[test]
fn attisnull_reports_null_and_present() {
    let td = tupdesc(vec![byval(4, 4), byval(4, 4), byval(4, 4)]);
    let values = vec![
        TupleValue::ByVal(Datum::from_i32(1)),
        TupleValue::ByVal(Datum::null()),
        TupleValue::ByVal(Datum::from_i32(3)),
    ];
    let isnull = vec![false, true, false];
    let formed = heap_form_tuple(&td, &values, &isnull).expect("form");

    assert!(!crate::heap_attisnull(&formed.tuple, 1, Some(&td)));
    assert!(crate::heap_attisnull(&formed.tuple, 2, Some(&td)));
    assert!(!crate::heap_attisnull(&formed.tuple, 3, Some(&td)));

    // System columns are never null.
    assert!(!crate::heap_attisnull(
        &formed.tuple,
        types_tuple::heaptuple::TableOidAttributeNumber as i32,
        Some(&td)
    ));
}

#[test]
fn attisnull_beyond_natts_is_null_without_missing() {
    let td = tupdesc(vec![byval(4, 4), byval(4, 4)]);
    let one = heap_form_tuple(
        &tupdesc(vec![byval(4, 4)]),
        &[TupleValue::ByVal(Datum::from_i32(7))],
        &[false],
    )
    .expect("form one");
    assert!(crate::heap_attisnull(&one.tuple, 2, Some(&td)));
}

#[test]
fn getsysattr_tableoid_and_ctid() {
    let mut formed = formed_three_ints(1, 2, 3);
    formed.tuple.t_tableOid = 1259;
    formed.tuple.t_self = types_tuple::heaptuple::ItemPointerData::new(5, 9);

    let (val, isnull) =
        crate::heap_getsysattr(&formed.tuple, types_tuple::heaptuple::TableOidAttributeNumber as i32);
    assert!(!isnull);
    assert_eq!(val, TupleValue::ByVal(Datum::from_oid(1259)));

    let (ctid, isnull2) = crate::heap_getsysattr(
        &formed.tuple,
        types_tuple::heaptuple::SelfItemPointerAttributeNumber as i32,
    );
    assert!(!isnull2);
    match ctid {
        TupleValue::ByRef(b) => assert_eq!(b.len(), 6),
        other => panic!("expected ByRef ctid, got {other:?}"),
    }
}

#[test]
fn nocachegetattr_matches_deform() {
    let td = tupdesc(vec![byval(4, 4), byval(8, 8), byval(4, 4)]);
    let values = vec![
        TupleValue::ByVal(Datum::from_i32(11)),
        TupleValue::ByVal(Datum::from_usize(22)),
        TupleValue::ByVal(Datum::from_i32(33)),
    ];
    let isnull = vec![false, false, false];
    let formed = heap_form_tuple(&td, &values, &isnull).expect("form");

    let v = crate::nocachegetattr(&formed.tuple, 3, &td, &formed.data);
    assert_eq!(v, TupleValue::ByVal(Datum::from_i32(33)));
}

#[test]
fn heap_expand_tuple_appends_nulls_for_absent_attrs() {
    let src = heap_form_tuple(
        &tupdesc(vec![byval(4, 4)]),
        &[TupleValue::ByVal(Datum::from_i32(42))],
        &[false],
    )
    .expect("form src");

    let td = tupdesc(vec![byval(4, 4), byval(4, 4), byval(4, 4)]);
    let expanded = crate::heap_expand_tuple(&src, &td).unwrap();

    let hdr = expanded.tuple.t_data.as_ref().unwrap();
    assert_ne!(hdr.t_infomask & types_tuple::heaptuple::HEAP_HASNULL, 0);
    assert_eq!(types_tuple::heaptuple::HeapTupleHeaderGetNatts(hdr), 3);

    let cols = heap_deform_tuple(&expanded.tuple, &td, &expanded.data);
    assert_eq!(cols[0], (TupleValue::ByVal(Datum::from_i32(42)), false));
    assert!(cols[1].1);
    assert!(cols[2].1);
}

#[test]
fn minimal_expand_tuple_appends_nulls() {
    let src = heap_form_tuple(
        &tupdesc(vec![byval(4, 4)]),
        &[TupleValue::ByVal(Datum::from_i32(9))],
        &[false],
    )
    .expect("form src");
    let td = tupdesc(vec![byval(4, 4), byval(4, 4)]);
    let mt = crate::minimal_expand_tuple(&src, &td).unwrap();
    assert_ne!(mt.tuple.t_infomask & types_tuple::heaptuple::HEAP_HASNULL, 0);
    assert_eq!(mt.tuple.t_infomask2 & types_tuple::heaptuple::HEAP_NATTS_MASK, 2);
}

#[test]
fn minimal_heap_roundtrip() {
    let td = tupdesc(vec![byval(4, 4), byval(4, 4)]);
    let h = heap_form_tuple(
        &td,
        &[
            TupleValue::ByVal(Datum::from_i32(100)),
            TupleValue::ByVal(Datum::from_i32(200)),
        ],
        &[false, false],
    )
    .expect("form");

    let mt = crate::minimal_tuple_from_heap_tuple(&h, 0);
    assert_eq!(
        mt.tuple.t_len as usize,
        h.tuple.t_len as usize - types_tuple::heaptuple::MINIMAL_TUPLE_OFFSET
    );
    assert_eq!(mt.data, h.data);

    let back = crate::heap_tuple_from_minimal_tuple(&mt);
    assert_eq!(back.tuple.t_len, h.tuple.t_len);
    assert_eq!(back.data, h.data);
    let cols = heap_deform_tuple(&back.tuple, &td, &back.data);
    assert_eq!(cols[0], (TupleValue::ByVal(Datum::from_i32(100)), false));
    assert_eq!(cols[1], (TupleValue::ByVal(Datum::from_i32(200)), false));
}

#[test]
fn copy_minimal_tuple_is_deep() {
    let td = tupdesc(vec![byval(4, 4)]);
    let h = heap_form_tuple(&td, &[TupleValue::ByVal(Datum::from_i32(5))], &[false])
        .expect("form");
    let mt = crate::minimal_tuple_from_heap_tuple(&h, 0);
    let copy = crate::heap_copy_minimal_tuple(&mt, 0);
    assert_eq!(copy.data, mt.data);
    assert_eq!(copy.tuple.t_len, mt.tuple.t_len);
    crate::heap_free_minimal_tuple(copy);
}

#[test]
fn copy_tuple_as_datum_sets_composite_header() {
    let td = tupdesc(vec![byval(4, 4), byval(4, 4)]);
    let h = heap_form_tuple(
        &td,
        &[
            TupleValue::ByVal(Datum::from_i32(1)),
            TupleValue::ByVal(Datum::from_i32(2)),
        ],
        &[false, false],
    )
    .expect("form");

    let d = crate::heap_copy_tuple_as_datum(&h, &td).unwrap();
    match &d.tuple.t_data.as_ref().unwrap().t_choice {
        types_tuple::heaptuple::HeapTupleHeaderChoice::TDatum(f) => {
            assert_eq!(f.datum_len_, h.tuple.t_len as i32);
            assert_eq!(f.datum_typeid, td.tdtypeid);
            assert_eq!(f.datum_typmod, td.tdtypmod);
        }
        other => panic!("expected TDatum header, got {other:?}"),
    }
    assert_eq!(d.data, h.data);
}

// ===========================================================================
// Flat MinimalTuple blob codec (src/flat.rs) round-trips.
// ===========================================================================

mod flat_codec {
    use super::{byval, tupdesc, varlena, varlena_4b};
    use crate::flat::{
        heap_deform_minimal_tuple_flat, heap_form_minimal_tuple_flat, minimal_tuple_from_flat,
        minimal_tuple_from_heap_tuple_flat, minimal_tuple_to_flat, MinimalTupleFlatError,
    };
    use crate::TupleValue;
    use alloc::vec;
    use types_datum::Datum;

    /// form -> flat -> deform identity over byval + varlena + NULL columns.
    #[test]
    fn form_flat_deform_identity() {
        let td = tupdesc(vec![byval(4, 4), varlena(), byval(8, 8)]);
        let values = vec![
            TupleValue::ByVal(Datum::from_i32(7)),
            TupleValue::ByRef(varlena_4b(b"hello world")),
            TupleValue::ByVal(Datum::from_i64(-42)),
        ];
        let isnull = vec![false, false, false];

        let blob = heap_form_minimal_tuple_flat(&td, &values, &isnull).expect("form flat");
        // First four bytes are t_len == blob length.
        assert_eq!(
            u32::from_ne_bytes([blob[0], blob[1], blob[2], blob[3]]) as usize,
            blob.len()
        );

        let cols = heap_deform_minimal_tuple_flat(&blob, &td).expect("deform flat");
        assert_eq!(cols.len(), 3);
        assert!(!cols[0].1 && !cols[1].1 && !cols[2].1);
        assert_eq!(cols[0].0, TupleValue::ByVal(Datum::from_i32(7)));
        // The varlena was short-packed by fill_val; compare its payload.
        match &cols[1].0 {
            TupleValue::ByRef(bytes) => {
                // Short-packed varlena: 1-byte header, then the payload.
                assert_eq!(bytes.len(), 1 + b"hello world".len());
                assert_eq!(&bytes[1..], b"hello world");
            }
            other => panic!("expected ByRef varlena, got {other:?}"),
        }
        assert_eq!(cols[2].0, TupleValue::ByVal(Datum::from_i64(-42)));
    }

    /// form -> flat -> from_flat -> to_flat byte identity, with NULLs (bitmap).
    #[test]
    fn flat_struct_flat_identity_with_nulls() {
        let td = tupdesc(vec![byval(4, 4), byval(4, 4), varlena()]);
        let values = vec![
            TupleValue::ByVal(Datum::from_i32(1)),
            TupleValue::ByVal(Datum::null()),
            TupleValue::ByRef(varlena_4b(b"x")),
        ];
        let isnull = vec![false, true, false];

        let formed = crate::heap_form_minimal_tuple(&td, &values, &isnull, 0).expect("form");
        let blob = minimal_tuple_to_flat(&formed);
        let parsed = minimal_tuple_from_flat(&blob).expect("parse");
        assert_eq!(parsed.tuple.t_len, formed.tuple.t_len);
        assert_eq!(parsed.tuple.t_infomask, formed.tuple.t_infomask);
        assert_eq!(parsed.tuple.t_infomask2, formed.tuple.t_infomask2);
        assert_eq!(parsed.tuple.t_hoff, formed.tuple.t_hoff);
        assert_eq!(parsed.tuple.t_bits, formed.tuple.t_bits);
        assert_eq!(parsed.data, formed.data);
        // Byte identity through a second serialize.
        assert_eq!(minimal_tuple_to_flat(&parsed), blob);

        // NULL deforms back as NULL.
        let cols = heap_deform_minimal_tuple_flat(&blob, &td).expect("deform");
        assert!(cols[1].1, "column 2 is NULL");
    }

    /// minimal_tuple_from_heap_tuple over the t_user_data-carrying heap tuple
    /// yields the same flat blob as forming the minimal tuple directly.
    #[test]
    fn from_heap_tuple_matches_direct_form() {
        let td = tupdesc(vec![byval(4, 4), byval(2, 2)]);
        let values = vec![
            TupleValue::ByVal(Datum::from_i32(0x0A0B0C0D)),
            TupleValue::ByVal(Datum::from_i16(99)),
        ];
        let isnull = vec![false, false];

        let heap = crate::heap_form_tuple_heaptuple(&td, &values, &isnull).expect("form heap");
        let via_heap = minimal_tuple_from_heap_tuple_flat(&heap).expect("from heap");

        let direct = heap_form_minimal_tuple_flat(&td, &values, &isnull).expect("form minimal");
        // The data areas and tail header fields must agree; C documents the
        // two routes as producing the same minimal tuple.
        assert_eq!(via_heap, direct);
    }

    /// Corrupt blobs are rejected loudly, never silently decoded.
    #[test]
    fn corrupt_blobs_rejected() {
        // Too short.
        assert_eq!(
            minimal_tuple_from_flat(&[0u8; 4]).unwrap_err(),
            MinimalTupleFlatError::TooShort { len: 4 }
        );

        // Length word disagreeing with the slice length.
        let td = tupdesc(vec![byval(4, 4)]);
        let blob = heap_form_minimal_tuple_flat(
            &td,
            &[TupleValue::ByVal(Datum::from_i32(1))],
            &[false],
        )
        .expect("form");
        let mut bad = blob.clone();
        bad.push(0);
        assert!(matches!(
            minimal_tuple_from_flat(&bad),
            Err(MinimalTupleFlatError::LengthMismatch { .. })
        ));

        // t_hoff pointing past the end.
        let mut bad2 = blob.clone();
        bad2[14] = 0xFF;
        assert!(matches!(
            minimal_tuple_from_flat(&bad2),
            Err(MinimalTupleFlatError::BadHoff { .. })
        ));

        // Heap tuple without its user-data bytes cannot be encoded.
        let formed = crate::heap_form_tuple(
            &td,
            &[TupleValue::ByVal(Datum::from_i32(1))],
            &[false],
        )
        .expect("form");
        let headless = types_tuple::heaptuple::HeapTupleData {
            t_user_data: None,
            ..(*formed.tuple).clone()
        };
        assert!(matches!(
            minimal_tuple_from_heap_tuple_flat(&headless),
            Err(MinimalTupleFlatError::UserDataLength { .. })
        ));
    }
}
