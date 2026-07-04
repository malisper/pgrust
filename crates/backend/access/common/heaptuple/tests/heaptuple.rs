use core::cell::Cell;

use datum::Datum;
use heaptuple::*;
use mcx::{MemoryContext, Mcx, PgVec};
use types_tuple::varatt::set_varsize_4b_word;
use types_tuple::*;

fn attr(attlen: i16, attbyval: bool, attalignby: u8) -> CompactAttribute {
    CompactAttribute {
        attcacheoff: Cell::new(-1),
        attlen,
        attbyval,
        attispackable: attlen == -1,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: ATTNULLABLE_UNRESTRICTED,
        attalignby,
    }
}

fn make_desc<'m>(mcx: Mcx<'m>, cols: &[CompactAttribute]) -> TupleDescData<'m> {
    let mut compact: PgVec<CompactAttribute> = PgVec::new_in(mcx);
    let mut attrs: PgVec<FormData_pg_attribute> = PgVec::new_in(mcx);
    for c in cols {
        compact.push(c.clone());
        attrs.push(FormData_pg_attribute::default());
    }
    TupleDescData {
        natts: cols.len() as i32,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    }
}

// Hand-assembled C heap-tuple image: DatumTupleFields as heap_form_tuple sets
// them, invalid ctid, natts/infomask/hoff, bitmap iff hasnull, MAXALIGN pad, data.
fn expected_heap_image(
    natts: u16,
    infomask: u16,
    nullbits: Option<&[u8]>,
    data: &[u8],
) -> Vec<u8> {
    let bitmap_len = nullbits.map_or(0, <[u8]>::len);
    let hoff = MAXALIGN(23 + bitmap_len);
    let len = hoff + data.len();
    let mut img = vec![0u8; len];
    img[0..4].copy_from_slice(&set_varsize_4b_word(len as u32).to_ne_bytes());
    img[4..8].copy_from_slice(&(-1i32).to_ne_bytes()); // datum_typmod
    img[8..12].copy_from_slice(&2249u32.to_ne_bytes()); // datum_typeid
    let invalid = ItemPointerData::invalid();
    img[12..14].copy_from_slice(&invalid.ip_blkid.bi_hi.to_ne_bytes());
    img[14..16].copy_from_slice(&invalid.ip_blkid.bi_lo.to_ne_bytes());
    img[16..18].copy_from_slice(&invalid.ip_posid.to_ne_bytes());
    img[18..20].copy_from_slice(&natts.to_ne_bytes());
    let infomask = infomask | if nullbits.is_some() { HEAP_HASNULL } else { 0 };
    img[20..22].copy_from_slice(&infomask.to_ne_bytes());
    img[22] = hoff as u8;
    if let Some(bits) = nullbits {
        img[23..23 + bits.len()].copy_from_slice(bits);
    }
    img[hoff..].copy_from_slice(data);
    img
}

fn expected_minimal_image(
    natts: u16,
    infomask: u16,
    nullbits: Option<&[u8]>,
    data: &[u8],
) -> Vec<u8> {
    let bitmap_len = nullbits.map_or(0, <[u8]>::len);
    let hoff = MAXALIGN(15 + bitmap_len);
    let len = hoff + data.len();
    let mut img = vec![0u8; len];
    img[0..4].copy_from_slice(&(len as u32).to_ne_bytes());
    img[10..12].copy_from_slice(&natts.to_ne_bytes());
    let infomask = infomask | if nullbits.is_some() { HEAP_HASNULL } else { 0 };
    img[12..14].copy_from_slice(&infomask.to_ne_bytes());
    img[14] = (hoff + MINIMAL_TUPLE_OFFSET) as u8;
    if let Some(bits) = nullbits {
        img[15..15 + bits.len()].copy_from_slice(bits);
    }
    img[hoff..].copy_from_slice(data);
    img
}

fn varlena_4b(payload: &[u8]) -> Vec<u8> {
    let mut v = vec![0u8; 4 + payload.len()];
    v[0..4].copy_from_slice(&set_varsize_4b_word((4 + payload.len()) as u32).to_ne_bytes());
    v[4..].copy_from_slice(payload);
    v
}

fn varlena_short(payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() + 1 <= 0x7F);
    let mut v = vec![0u8; 1 + payload.len()];
    v[0] = (((1 + payload.len()) as u8) << 1) | 1;
    v[1..].copy_from_slice(payload);
    v
}

#[test]
fn form_fixed_cols_matches_c_image_and_deforms() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let desc = make_desc(
        mcx,
        &[attr(4, true, 4), attr(8, true, 8), attr(2, true, 2), attr(1, true, 1)],
    );
    let values = [
        Datum::from_i32(-7),
        Datum::from_i64(0x1122_3344_5566_7788),
        Datum::from_i16(-2),
        Datum::from_bool(true),
    ];
    let isnull = [false; 4];

    let tup = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();

    // data area ends at the last byte (C never pads the tail): 4+pad+8+2+1 = 19
    let mut data = [0u8; 19];
    data[0..4].copy_from_slice(&(-7i32).to_ne_bytes());
    data[8..16].copy_from_slice(&0x1122_3344_5566_7788i64.to_ne_bytes());
    data[16..18].copy_from_slice(&(-2i16).to_ne_bytes());
    data[18] = 1;
    assert_eq!(tup.image(), expected_heap_image(4, 0, None, &data));
    assert_eq!(tup.t_len as usize, tup.image().len());
    assert_eq!(tup.t_tableOid, 0);
    assert!(!ItemPointerIsValid(&tup.t_self));

    assert_eq!(heap_compute_data_size(&desc, &values, &isnull), 19);

    let mut out = [Datum::null(); 4];
    let mut nulls = [true; 4];
    heap_deform_tuple(tup.as_tuple(), &desc, &mut out, &mut nulls);
    assert_eq!(out[0].as_i32(), -7);
    assert_eq!(out[1].as_i64(), 0x1122_3344_5566_7788);
    assert_eq!(out[2].as_i16(), -2);
    assert!(out[3].as_bool());
    assert_eq!(nulls, [false; 4]);
}

#[test]
fn form_with_nulls_sets_bitmap() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let desc = make_desc(mcx, &[attr(4, true, 4), attr(8, true, 8), attr(4, true, 4)]);
    let values = [Datum::from_i32(5), Datum::null(), Datum::from_i32(9)];
    let isnull = [false, true, false];

    let tup = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();

    let mut data = [0u8; 8];
    data[0..4].copy_from_slice(&5i32.to_ne_bytes());
    data[4..8].copy_from_slice(&9i32.to_ne_bytes());
    // bits: attr0 set, attr1 clear (null), attr2 set -> 0b101
    assert_eq!(tup.image(), expected_heap_image(3, 0, Some(&[0b101]), &data));

    let mut out = [Datum::null(); 3];
    let mut nulls = [false; 3];
    heap_deform_tuple(tup.as_tuple(), &desc, &mut out, &mut nulls);
    assert_eq!(nulls, [false, true, false]);
    assert_eq!(out[0].as_i32(), 5);
    assert_eq!(out[2].as_i32(), 9);
}

#[test]
fn form_varlena_short_conversion_and_verbatim() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let desc = make_desc(
        mcx,
        &[attr(4, true, 4), attr(-1, false, 4), attr(-1, false, 4), attr(8, true, 8)],
    );

    let long4b = varlena_4b(b"hello world"); // packable -> converted short
    let short_in = varlena_short(b"xyz"); // already short -> verbatim
    let values = [
        Datum::from_i32(3),
        Datum::from_usize(long4b.as_ptr() as usize),
        Datum::from_usize(short_in.as_ptr() as usize),
        Datum::from_i64(-1),
    ];
    let isnull = [false; 4];

    let tup = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();

    let mut data = Vec::new();
    data.extend_from_slice(&3i32.to_ne_bytes());
    data.extend_from_slice(&varlena_short(b"hello world")); // 12 bytes, no align
    data.extend_from_slice(&short_in); // 4 bytes
    while data.len() % 8 != 0 {
        data.push(0); // MAXALIGN pad before int8
    }
    data.extend_from_slice(&(-1i64).to_ne_bytes());
    assert_eq!(tup.image(), expected_heap_image(4, HEAP_HASVARWIDTH, None, &data));
    assert_eq!(heap_compute_data_size(&desc, &values, &isnull), data.len());

    let mut out = [Datum::null(); 4];
    let mut nulls = [true; 4];
    heap_deform_tuple(tup.as_tuple(), &desc, &mut out, &mut nulls);
    assert_eq!(out[0].as_i32(), 3);
    unsafe {
        let p = out[1].as_usize() as *const u8;
        assert_eq!(types_tuple::varatt::varsize_any(p), 12);
        assert_eq!(core::slice::from_raw_parts(p.add(1), 11), b"hello world");
    }
    assert_eq!(out[3].as_i64(), -1);
}

#[test]
fn form_varlena_4b_kept_when_not_packable() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut cols = [attr(4, true, 4), attr(-1, false, 4)];
    cols[1].attispackable = false; // plain storage: no short conversion
    let desc = make_desc(mcx, &cols);

    let v = varlena_4b(b"abcdef");
    let values = [Datum::from_i32(1), Datum::from_usize(v.as_ptr() as usize)];
    let isnull = [false; 2];
    let tup = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();

    let mut data = Vec::new();
    data.extend_from_slice(&1i32.to_ne_bytes());
    data.extend_from_slice(&v); // aligned at 4, full 4B header
    assert_eq!(tup.image(), expected_heap_image(2, HEAP_HASVARWIDTH, None, &data));
}

#[test]
fn form_external_toast_pointer_sets_hasexternal() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let desc = make_desc(mcx, &[attr(4, true, 4), attr(-1, false, 4)]);

    let mut ext = [0u8; 18]; // varattrib_1b_e, VARTAG_ONDISK (16-byte payload)
    ext[0] = 0x01;
    ext[1] = 18;
    for (i, b) in ext[2..].iter_mut().enumerate() {
        *b = i as u8;
    }
    let values = [Datum::from_i32(2), Datum::from_usize(ext.as_ptr() as usize)];
    let isnull = [false; 2];
    let tup = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();

    let mut data = Vec::new();
    data.extend_from_slice(&2i32.to_ne_bytes());
    data.extend_from_slice(&ext); // no alignment
    assert_eq!(
        tup.image(),
        expected_heap_image(2, HEAP_HASVARWIDTH | HEAP_HASEXTERNAL, None, &data)
    );
    assert!(tup.has_external());
}

#[test]
fn form_cstring_and_fixed_byref() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut cols = [attr(-2, false, 1), attr(16, false, 8)];
    cols[0].attispackable = false;
    let desc = make_desc(mcx, &cols);

    let cs = b"name\0";
    let fixed: [u8; 16] = *b"0123456789abcdef";
    let values = [
        Datum::from_usize(cs.as_ptr() as usize),
        Datum::from_usize(fixed.as_ptr() as usize),
    ];
    let isnull = [false; 2];
    let tup = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();

    let mut data = Vec::new();
    data.extend_from_slice(cs);
    while data.len() % 8 != 0 {
        data.push(0);
    }
    data.extend_from_slice(&fixed);
    assert_eq!(tup.image(), expected_heap_image(2, HEAP_HASVARWIDTH, None, &data));
}

#[test]
fn form_minimal_tuple_layout_and_roundtrip() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let desc = make_desc(mcx, &[attr(4, true, 4), attr(8, true, 8)]);
    let values = [Datum::from_i32(42), Datum::from_i64(7)];
    let isnull = [false, false];

    let mtup = heap_form_minimal_tuple(mcx, &desc, &values, &isnull, 0).unwrap();

    let mut data = [0u8; 16];
    data[0..4].copy_from_slice(&42i32.to_ne_bytes());
    data[8..16].copy_from_slice(&7i64.to_ne_bytes());
    assert_eq!(mtup.as_bytes(), expected_minimal_image(2, 0, None, &data));
    assert_eq!(mtup.data().t_hoff as usize, 16 + MINIMAL_TUPLE_OFFSET);
    assert_eq!(mtup.data().natts(), 2);

    let htup = heap_tuple_from_minimal_tuple(mcx, mtup.as_bytes()).unwrap();
    assert_eq!(htup.t_len, mtup.t_len() + MINIMAL_TUPLE_OFFSET as u32);
    let mut out = [Datum::null(); 2];
    let mut nulls = [true; 2];
    heap_deform_tuple(htup.as_tuple(), &desc, &mut out, &mut nulls);
    assert_eq!(out[0].as_i32(), 42);
    assert_eq!(out[1].as_i64(), 7);
    assert_eq!(nulls, [false, false]);
}

#[test]
fn minimal_from_heap_and_back() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let desc = make_desc(mcx, &[attr(4, true, 4), attr(-1, false, 4)]);
    let v = varlena_4b(b"payload");
    let values = [Datum::from_i32(6), Datum::from_usize(v.as_ptr() as usize)];
    let isnull = [false, false];

    let htup = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();
    let mtup = minimal_tuple_from_heap_tuple(mcx, htup.as_tuple(), 0).unwrap();
    assert_eq!(mtup.t_len(), htup.t_len - MINIMAL_TUPLE_OFFSET as u32);
    // Shared tail (t_infomask2..data) is byte-identical past the length word/padding.
    assert_eq!(
        &mtup.as_bytes()[MINIMAL_TUPLE_DATA_OFFSET..],
        &htup.image()[MINIMAL_TUPLE_OFFSET + MINIMAL_TUPLE_DATA_OFFSET..]
    );

    let back = heap_tuple_from_minimal_tuple(mcx, mtup.as_bytes()).unwrap();
    assert_eq!(&back.image()[18..], &htup.image()[18..]);
    assert_eq!(&back.image()[..18], &[0u8; 18]); // system columns zeroed

    let direct = heap_form_minimal_tuple(mcx, &desc, &values, &isnull, 0).unwrap();
    assert_eq!(direct.as_bytes()[10..], mtup.as_bytes()[10..]);
}

#[test]
fn copy_minimal_tuple_with_extra() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let desc = make_desc(mcx, &[attr(4, true, 4)]);
    let values = [Datum::from_i32(11)];
    let isnull = [false];
    let src = heap_form_minimal_tuple(mcx, &desc, &values, &isnull, 0).unwrap();

    let mut copy = heap_copy_minimal_tuple(mcx, src.as_bytes(), 16).unwrap();
    assert_eq!(copy.as_bytes(), src.as_bytes());
    assert_eq!(copy.extra_mut(), &[0u8; 16]);

    let with_extra = heap_form_minimal_tuple(mcx, &desc, &values, &isnull, 8).unwrap();
    assert_eq!(with_extra.as_bytes(), src.as_bytes());
}

#[test]
fn copytuple_and_copy_as_datum() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let desc = make_desc(mcx, &[attr(4, true, 4), attr(2, true, 2)]);
    let values = [Datum::from_i32(-9), Datum::from_i16(3)];
    let isnull = [false, false];

    let mut orig = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();
    orig.t_self = ItemPointerData::new(7, 2);
    orig.t_tableOid = 1259;
    orig.t_data_mut().set_natts(2);

    let copy = heap_copytuple(mcx, orig.as_tuple()).unwrap();
    assert_eq!(copy.image(), orig.image());
    assert_eq!(copy.t_self, orig.t_self);
    assert_eq!(copy.t_tableOid, 1259);

    let d = heap_copy_tuple_as_datum(mcx, orig.as_tuple(), &desc).unwrap();
    let td = d.as_usize() as *const u8;
    let img = unsafe { core::slice::from_raw_parts(td, orig.t_len as usize) };
    // composite header fields re-stamped, rest identical
    assert_eq!(&img[..4], &set_varsize_4b_word(orig.t_len).to_ne_bytes());
    assert_eq!(&img[4..8], &(-1i32).to_ne_bytes());
    assert_eq!(&img[8..12], &2249u32.to_ne_bytes());
    assert_eq!(&img[12..], &orig.image()[12..]);
}

#[test]
fn modify_tuple_replaces_and_keeps_identity() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let desc = make_desc(mcx, &[attr(4, true, 4), attr(8, true, 8), attr(4, true, 4)]);
    let values = [Datum::from_i32(1), Datum::from_i64(2), Datum::from_i32(3)];
    let isnull = [false; 3];
    let mut orig = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();
    orig.t_self = ItemPointerData::new(4, 9);
    orig.t_tableOid = 16384;
    ItemPointerSet(&mut orig.t_data_mut().t_ctid, 5, 1);

    let repl = [Datum::null(), Datum::from_i64(-100), Datum::null()];
    let replnull = [false, false, true];
    let dorepl = [false, true, true];
    let new = heap_modify_tuple(mcx, orig.as_tuple(), &desc, &repl, &replnull, &dorepl).unwrap();

    let mut out = [Datum::null(); 3];
    let mut nulls = [false; 3];
    heap_deform_tuple(new.as_tuple(), &desc, &mut out, &mut nulls);
    assert_eq!(out[0].as_i32(), 1);
    assert_eq!(out[1].as_i64(), -100);
    assert_eq!(nulls, [false, false, true]);
    assert_eq!(new.t_self, orig.t_self);
    assert_eq!(new.t_tableOid, 16384);
    assert_eq!(
        ItemPointerCompare(&new.t_data().t_ctid, &orig.t_data().t_ctid),
        0
    );

    let by_cols = heap_modify_tuple_by_cols(
        mcx,
        orig.as_tuple(),
        &desc,
        &[2],
        &[Datum::from_i64(55)],
        &[false],
    )
    .unwrap();
    heap_deform_tuple(by_cols.as_tuple(), &desc, &mut out, &mut nulls);
    assert_eq!(out[1].as_i64(), 55);
    assert_eq!(out[0].as_i32(), 1);
    assert_eq!(out[2].as_i32(), 3);
}

fn desc_with_missing<'m>(mcx: Mcx<'m>, missing_val: &'m [u8]) -> TupleDescData<'m> {
    let mut desc = make_desc(
        mcx,
        &[attr(4, true, 4), attr(8, true, 8), attr(8, true, 8), attr(4, true, 4)],
    );
    desc.compact_attrs[2].atthasmissing = true;
    let mut missing: PgVec<AttrMissing> = PgVec::new_in(mcx);
    missing.push(AttrMissing { am_present: false, am_value: Datum::null() });
    missing.push(AttrMissing { am_present: false, am_value: Datum::null() });
    missing.push(AttrMissing {
        am_present: true,
        am_value: Datum::from_i64(i64::from_ne_bytes(missing_val.try_into().unwrap())),
    });
    missing.push(AttrMissing { am_present: false, am_value: Datum::null() });
    let constr = TupleConstr {
        defval: PgVec::new_in(mcx),
        check: PgVec::new_in(mcx),
        missing,
        num_defval: 0,
        num_check: 0,
        has_not_null: false,
        has_generated_stored: false,
        has_generated_virtual: false,
    };
    desc.constr = Some(mcx::box_new_in(mcx, constr));
    desc
}

#[test]
fn expand_tuple_fills_missing_and_nulls() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let missing_bytes = 0x0102_0304_0506_0708i64.to_ne_bytes();
    let desc = desc_with_missing(mcx, &missing_bytes);
    let short_desc = make_desc(mcx, &[attr(4, true, 4), attr(8, true, 8)]);

    let values = [Datum::from_i32(21), Datum::from_i64(-3)];
    let isnull = [false, false];
    let src = heap_form_tuple(mcx, &short_desc, &values, &isnull).unwrap();
    assert_eq!(src.t_data().natts(), 2);

    let wide = heap_expand_tuple(mcx, src.as_tuple(), &desc).unwrap();
    assert_eq!(wide.t_data().natts(), 4);
    let mut out = [Datum::null(); 4];
    let mut nulls = [false; 4];
    heap_deform_tuple(wide.as_tuple(), &desc, &mut out, &mut nulls);
    assert_eq!(out[0].as_i32(), 21);
    assert_eq!(out[1].as_i64(), -3);
    assert_eq!(out[2].as_i64(), 0x0102_0304_0506_0708);
    assert_eq!(nulls, [false, false, false, true]); // col 4 has no missing value

    // bits: attrs 1-3 present, attr 4 null -> 0b0111
    let mut data = [0u8; 24];
    data[0..4].copy_from_slice(&21i32.to_ne_bytes());
    data[8..16].copy_from_slice(&(-3i64).to_ne_bytes());
    data[16..24].copy_from_slice(&missing_bytes);
    let expected = expected_heap_image(4, 0, Some(&[0b0111]), &data);
    // heap_expand_tuple keeps the source ctid bytes unset (invalid) and doesn't
    // stamp datum fields from scratch on the copied infomask; compare tail.
    assert_eq!(&wide.image()[18..], &expected[18..]);

    let mwide = minimal_expand_tuple(mcx, src.as_tuple(), &desc).unwrap();
    let hback = heap_tuple_from_minimal_tuple(mcx, mwide.as_bytes()).unwrap();
    heap_deform_tuple(hback.as_tuple(), &desc, &mut out, &mut nulls);
    assert_eq!(out[2].as_i64(), 0x0102_0304_0506_0708);
    assert_eq!(nulls, [false, false, false, true]);
}

#[test]
fn expand_tuple_source_with_nulls_copies_bitmap() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let missing_bytes = 99i64.to_ne_bytes();
    let desc = desc_with_missing(mcx, &missing_bytes);
    let short_desc = make_desc(mcx, &[attr(4, true, 4), attr(8, true, 8)]);

    let values = [Datum::from_i32(1), Datum::null()];
    let isnull = [false, true];
    let src = heap_form_tuple(mcx, &short_desc, &values, &isnull).unwrap();
    assert!(src.has_nulls());

    let wide = heap_expand_tuple(mcx, src.as_tuple(), &desc).unwrap();
    let mut out = [Datum::null(); 4];
    let mut nulls = [false; 4];
    heap_deform_tuple(wide.as_tuple(), &desc, &mut out, &mut nulls);
    assert_eq!(out[0].as_i32(), 1);
    assert_eq!(nulls, [false, true, false, true]);
    assert_eq!(out[2].as_i64(), 99);
}

#[test]
fn too_many_columns_is_sqlstate_54011() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let cols: Vec<CompactAttribute> = (0..1665).map(|_| attr(4, true, 4)).collect();
    let desc = make_desc(mcx, &cols);
    let values = vec![Datum::from_i32(0); 1665];
    let isnull = vec![false; 1665];
    let Err(err) = heap_form_tuple(mcx, &desc, &values, &isnull) else {
        panic!("expected error")
    };
    assert_eq!(err.sqlstate(), types_error::ERRCODE_TOO_MANY_COLUMNS);
    let Err(err) = heap_form_minimal_tuple(mcx, &desc, &values, &isnull, 0) else {
        panic!("expected error")
    };
    assert_eq!(err.sqlstate(), types_error::ERRCODE_TOO_MANY_COLUMNS);
}

#[test]
fn wide_bitmap_form_roundtrip() {
    // 11 attrs -> 2-byte bitmap, hoff = MAXALIGN(25) = 32.
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let cols: Vec<CompactAttribute> = (0..11).map(|_| attr(4, true, 4)).collect();
    let desc = make_desc(mcx, &cols);
    let values: Vec<Datum> = (0..11).map(Datum::from_i32).collect();
    let isnull: Vec<bool> = (0..11).map(|i| i % 3 == 1).collect();

    let tup = heap_form_tuple(mcx, &desc, &values, &isnull).unwrap();
    assert_eq!(tup.t_data().t_hoff, 32);

    let mut out = vec![Datum::null(); 11];
    let mut nulls = vec![false; 11];
    heap_deform_tuple(tup.as_tuple(), &desc, &mut out, &mut nulls);
    for i in 0..11 {
        assert_eq!(nulls[i], i % 3 == 1);
        if !nulls[i] {
            assert_eq!(out[i].as_i32(), i as i32);
        }
    }
}

#[test]
fn planned_form_matches_generic() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let shapes: &[&[CompactAttribute]] = &[
        &[attr(4, true, 4)],
        &[attr(4, true, 4), attr(8, true, 8)],
        &[attr(8, true, 8), attr(1, true, 1), attr(2, true, 2), attr(4, true, 4)],
        &[
            attr(1, true, 1),
            attr(8, true, 8),
            attr(2, true, 2),
            attr(4, true, 4),
            attr(4, true, 4),
            attr(1, true, 1),
            attr(2, true, 2),
            attr(8, true, 8),
        ],
    ];
    let pool = [
        Datum::from_i64(-1),
        Datum::from_i64(0x0102_0304_0506_0708),
        Datum::from_i32(42),
        Datum::from_i16(-7),
        Datum::from_char(9),
        Datum::from_i64(i64::MIN),
        Datum::from_i32(i32::MAX),
        Datum::from_i16(i16::MIN),
    ];
    for cols in shapes {
        let desc = make_desc(mcx, cols);
        let plan = MinimalFormPlan::try_new(&desc).expect("all-byval shape must plan");
        assert_eq!(plan.natts(), cols.len());
        let values = &pool[..cols.len()];
        let isnull = vec![false; cols.len()];
        for extra in [0usize, 16] {
            let generic = heap_form_minimal_tuple(mcx, &desc, values, &isnull, extra).unwrap();
            let planned = heap_form_minimal_tuple_planned(mcx, &plan, values, extra).unwrap();
            assert_eq!(planned.as_bytes(), generic.as_bytes());
            assert_eq!(planned.t_len(), generic.t_len());
        }
    }
}

#[test]
fn form_plan_gates() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert!(MinimalFormPlan::try_new(&make_desc(mcx, &[attr(-1, false, 4)])).is_none());
    assert!(MinimalFormPlan::try_new(&make_desc(mcx, &[attr(-2, false, 1)])).is_none());
    assert!(MinimalFormPlan::try_new(&make_desc(mcx, &[attr(16, false, 8)])).is_none());
    assert!(MinimalFormPlan::try_new(&make_desc(mcx, &[])).is_none());
    let nine = vec![attr(4, true, 4); 9];
    assert!(MinimalFormPlan::try_new(&make_desc(mcx, &nine)).is_none());
    let mut dropped = attr(4, true, 4);
    dropped.attisdropped = true;
    assert!(MinimalFormPlan::try_new(&make_desc(mcx, &[dropped])).is_none());
}
