use core::cell::Cell;

use datum::Datum;
use mcx::{MemoryContext, Mcx, PgVec};
use types_tuple::varatt::{varsize_any, VARTAG_ONDISK};
use types_tuple::*;

// C header ground truth (htup_details.h / itemptr.h / off.h / sysattr.h).
#[test]
fn constants_match_c_headers() {
    assert_eq!(HEAP_HASNULL, 0x0001);
    assert_eq!(HEAP_HASVARWIDTH, 0x0002);
    assert_eq!(HEAP_HASEXTERNAL, 0x0004);
    assert_eq!(HEAP_HASOID_OLD, 0x0008);
    assert_eq!(HEAP_XMAX_KEYSHR_LOCK, 0x0010);
    assert_eq!(HEAP_COMBOCID, 0x0020);
    assert_eq!(HEAP_XMAX_EXCL_LOCK, 0x0040);
    assert_eq!(HEAP_XMAX_LOCK_ONLY, 0x0080);
    assert_eq!(HEAP_XMAX_SHR_LOCK, 0x0050);
    assert_eq!(HEAP_LOCK_MASK, 0x0050);
    assert_eq!(HEAP_XMIN_COMMITTED, 0x0100);
    assert_eq!(HEAP_XMIN_INVALID, 0x0200);
    assert_eq!(HEAP_XMIN_FROZEN, 0x0300);
    assert_eq!(HEAP_XMAX_COMMITTED, 0x0400);
    assert_eq!(HEAP_XMAX_INVALID, 0x0800);
    assert_eq!(HEAP_XMAX_IS_MULTI, 0x1000);
    assert_eq!(HEAP_UPDATED, 0x2000);
    assert_eq!(HEAP_MOVED, 0xC000);
    assert_eq!(HEAP_XACT_MASK, 0xFFF0);
    assert_eq!(HEAP_XMAX_BITS, 0x1CD0);
    assert_eq!(HEAP_NATTS_MASK, 0x07FF);
    assert_eq!(HEAP_KEYS_UPDATED, 0x2000);
    assert_eq!(HEAP_HOT_UPDATED, 0x4000);
    assert_eq!(HEAP_ONLY_TUPLE, 0x8000);
    assert_eq!(HEAP2_XACT_MASK, 0xE000);
    assert_eq!(HEAP_TUPLE_HAS_MATCH, 0x8000);
    assert_eq!(MaxTupleAttributeNumber, 1664);
    assert_eq!(MaxHeapAttributeNumber, 1600);
    assert_eq!(SizeofHeapTupleHeader, 23);
    assert_eq!(SizeofMinimalTupleHeader, 15);
    assert_eq!(MINIMAL_TUPLE_OFFSET, 8);
    assert_eq!(MINIMAL_TUPLE_PADDING, 6);
    assert_eq!(MINIMAL_TUPLE_DATA_OFFSET, 10);
    assert_eq!(SelfItemPointerAttributeNumber, -1);
    assert_eq!(MinTransactionIdAttributeNumber, -2);
    assert_eq!(MinCommandIdAttributeNumber, -3);
    assert_eq!(MaxTransactionIdAttributeNumber, -4);
    assert_eq!(MaxCommandIdAttributeNumber, -5);
    assert_eq!(TableOidAttributeNumber, -6);
    assert_eq!(FirstLowInvalidHeapAttributeNumber, -7);
    assert_eq!(InvalidOffsetNumber, 0);
    assert_eq!(FirstOffsetNumber, 1);
    assert_eq!(MaxOffsetNumber, 2048);
    assert_eq!(SpecTokenOffsetNumber, 0xfffe);
    assert_eq!(MovedPartitionsOffsetNumber, 0xfffd);
    assert_eq!(MovedPartitionsBlockNumber, 0xFFFF_FFFF);
    assert_eq!(core::mem::size_of::<ItemPointerData>(), 6);
    assert_eq!(core::mem::size_of::<CompactAttribute>(), 16);
    assert_eq!(BITMAPLEN(1), 1);
    assert_eq!(BITMAPLEN(8), 1);
    assert_eq!(BITMAPLEN(9), 2);
    assert_eq!(BITMAPLEN(1600), 200);
}

#[test]
fn infomask_predicates() {
    assert!(HEAP_XMAX_IS_LOCKED_ONLY(HEAP_XMAX_LOCK_ONLY));
    assert!(HEAP_XMAX_IS_LOCKED_ONLY(HEAP_XMAX_EXCL_LOCK));
    assert!(!HEAP_XMAX_IS_LOCKED_ONLY(HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_IS_MULTI));
    assert!(HEAP_LOCKED_UPGRADED(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY));
    assert!(!HEAP_LOCKED_UPGRADED(
        HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK
    ));
    assert!(HEAP_XMAX_IS_SHR_LOCKED(HEAP_XMAX_SHR_LOCK));
    assert!(HEAP_XMAX_IS_EXCL_LOCKED(HEAP_XMAX_EXCL_LOCK));
    assert!(HEAP_XMAX_IS_KEYSHR_LOCKED(HEAP_XMAX_KEYSHR_LOCK));
}

#[test]
fn item_pointer_ops() {
    let mut p = ItemPointerData::new(0x0001_0002, 5);
    assert_eq!(p.ip_blkid.bi_hi, 1);
    assert_eq!(p.ip_blkid.bi_lo, 2);
    assert!(ItemPointerIsValid(&p));
    assert_eq!(ItemPointerGetBlockNumber(&p), 0x0001_0002);
    assert_eq!(ItemPointerGetOffsetNumber(&p), 5);

    ItemPointerSetInvalid(&mut p);
    assert!(!ItemPointerIsValid(&p));
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&p), 0xFFFF_FFFF);

    let a = ItemPointerData::new(1, 2);
    let b = ItemPointerData::new(1, 3);
    let c = ItemPointerData::new(2, 1);
    assert_eq!(ItemPointerCompare(&a, &a), 0);
    assert_eq!(ItemPointerCompare(&a, &b), -1);
    assert_eq!(ItemPointerCompare(&b, &a), 1);
    assert_eq!(ItemPointerCompare(&b, &c), -1);
    assert!(ItemPointerEquals(&a, &ItemPointerData::new(1, 2)));
    assert!(!ItemPointerEquals(&a, &b));

    let mut q = ItemPointerData::new(7, u16::MAX);
    ItemPointerInc(&mut q);
    assert_eq!((ItemPointerGetBlockNumberNoCheck(&q), q.ip_posid), (8, 0));
    let mut r = ItemPointerData::new(0xFFFF_FFFF, u16::MAX);
    ItemPointerInc(&mut r);
    assert_eq!(
        (ItemPointerGetBlockNumberNoCheck(&r), r.ip_posid),
        (0xFFFF_FFFF, u16::MAX)
    );
    let mut s = ItemPointerData::new(8, 0);
    ItemPointerDec(&mut s);
    assert_eq!((ItemPointerGetBlockNumberNoCheck(&s), s.ip_posid), (7, u16::MAX));
    let mut t = ItemPointerData::new(0, 0);
    ItemPointerDec(&mut t);
    assert_eq!((ItemPointerGetBlockNumberNoCheck(&t), t.ip_posid), (0, 0));

    let mut m = ItemPointerData::new(3, 4);
    ItemPointerSetMovedPartitions(&mut m);
    assert!(ItemPointerIndicatesMovedPartitions(&m));
    assert_eq!(OffsetNumberNext(1), 2);
    assert_eq!(OffsetNumberPrev(2), 1);
    assert!(OffsetNumberIsValid(1) && OffsetNumberIsValid(2048));
    assert!(!OffsetNumberIsValid(0) && !OffsetNumberIsValid(2049));
}

#[repr(C, align(8))]
struct Image([u8; 256]);

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

// Hand-assembled image: 23-byte header, bitmap iff hasnull, MAXALIGN'd t_hoff, data.
fn build_tuple_mask(
    image: &mut Image,
    natts: u16,
    nullbits: Option<u8>,
    data: &[u8],
    mask: u16,
) -> (u32, u16) {
    let bitmap_len = if nullbits.is_some() { 1 } else { 0 };
    let hoff = MAXALIGN(23 + bitmap_len);
    let t_len = (hoff + data.len()) as u32;
    let buf = &mut image.0;
    buf.fill(0);
    buf[18..20].copy_from_slice(&natts.to_ne_bytes());
    let infomask: u16 = mask | if nullbits.is_some() { HEAP_HASNULL } else { 0 };
    buf[20..22].copy_from_slice(&infomask.to_ne_bytes());
    buf[22] = hoff as u8;
    if let Some(bits) = nullbits {
        buf[23] = bits;
    }
    buf[hoff..hoff + data.len()].copy_from_slice(data);
    (t_len, infomask)
}

fn build_tuple(image: &mut Image, natts: u16, nullbits: Option<u8>, data: &[u8]) -> (u32, u16) {
    build_tuple_mask(image, natts, nullbits, data, 0)
}

fn tuple_from<'a>(image: &'a Image, t_len: u32) -> HeapTupleData<'a> {
    unsafe {
        HeapTupleData::from_raw_parts(image.0.as_ptr(), t_len, ItemPointerData::new(11, 3), 1259)
    }
}

fn tuple_from_mut<'a>(image: &'a mut Image, t_len: u32) -> HeapTupleData<'a> {
    unsafe {
        HeapTupleData::from_raw_parts(
            image.0.as_mut_ptr(),
            t_len,
            ItemPointerData::new(11, 3),
            1259,
        )
    }
}

#[test]
fn deform_fixed_width_and_attcacheoff() {
    let ctx = MemoryContext::new("t");
    let desc = make_desc(
        ctx.mcx(),
        &[attr(4, true, 4), attr(8, true, 8), attr(2, true, 2), attr(1, true, 1)],
    );

    let mut data = [0u8; 24];
    data[0..4].copy_from_slice(&(-7i32).to_ne_bytes());
    data[8..16].copy_from_slice(&0x1122_3344_5566_7788i64.to_ne_bytes());
    data[16..18].copy_from_slice(&(-2i16).to_ne_bytes());
    data[18] = 1;

    let mut image = Image([0; 256]);
    let (t_len, _) = build_tuple(&mut image, 4, None, &data);
    let tup = tuple_from(&image, t_len);

    let mut values = [Datum::null(); 4];
    let mut nulls = [false; 4];
    heap_deform_tuple(&tup, &desc, &mut values, &mut nulls);
    assert_eq!(values[0].as_i32(), -7);
    assert_eq!(values[1].as_i64(), 0x1122_3344_5566_7788);
    assert_eq!(values[2].as_i16(), -2);
    assert!(values[3].as_bool());
    assert_eq!(nulls, [false; 4]);

    let offs: Vec<i32> = desc.compact_attrs.iter().map(|a| a.attcacheoff.get()).collect();
    assert_eq!(offs, [0, 8, 16, 18]);

    let mut isnull = true;
    let v = unsafe { heap_getattr(&tup, 2, &desc, &mut isnull) };
    assert!(!isnull);
    assert_eq!(v.as_i64(), 0x1122_3344_5566_7788);
    let v = unsafe { fastgetattr(&tup, 4, &desc, &mut isnull) };
    assert!(v.as_bool());
}

#[test]
fn nocachegetattr_fills_leading_fixed_offsets() {
    let ctx = MemoryContext::new("t");
    let desc = make_desc(ctx.mcx(), &[attr(4, true, 4), attr(4, true, 4), attr(8, true, 8)]);
    let mut data = [0u8; 16];
    data[0..4].copy_from_slice(&1i32.to_ne_bytes());
    data[4..8].copy_from_slice(&2i32.to_ne_bytes());
    data[8..16].copy_from_slice(&3i64.to_ne_bytes());
    let mut image = Image([0; 256]);
    let (t_len, _) = build_tuple(&mut image, 3, None, &data);
    let tup = tuple_from(&image, t_len);

    assert_eq!(unsafe { nocachegetattr(&tup, 3, &desc) }.as_i64(), 3);
    let offs: Vec<i32> = desc.compact_attrs.iter().map(|a| a.attcacheoff.get()).collect();
    assert_eq!(offs, [0, 4, 8]);
    assert_eq!(unsafe { nocachegetattr(&tup, 2, &desc) }.as_i32(), 2);
}

#[test]
fn deform_varlena_short_and_aligned() {
    let ctx = MemoryContext::new("t");
    // (int4, text, int8): text stored as a 1-byte-header short varlena.
    let desc = make_desc(ctx.mcx(), &[attr(4, true, 4), attr(-1, false, 4), attr(8, true, 8)]);

    let payload = b"hello";
    let short_hdr: u8 = (((1 + payload.len()) as u8) << 1) | 0x01;
    let mut data = [0u8; 32];
    data[0..4].copy_from_slice(&42i32.to_ne_bytes());
    data[4] = short_hdr;
    data[5..10].copy_from_slice(payload);
    // off = 10 -> align 8 -> 16 for the int8.
    data[16..24].copy_from_slice(&99i64.to_ne_bytes());

    let mut image = Image([0; 256]);
    let (t_len, _) = build_tuple_mask(&mut image, 3, None, &data[..24], HEAP_HASVARWIDTH);
    let tup = tuple_from(&image, t_len);

    let mut values = [Datum::null(); 3];
    let mut nulls = [true; 3];
    heap_deform_tuple(&tup, &desc, &mut values, &mut nulls);
    assert_eq!(values[0].as_i32(), 42);
    assert_eq!(nulls, [false; 3]);
    let vp = values[1].as_usize() as *const u8;
    unsafe {
        assert_eq!(varsize_any(vp), 6);
        assert_eq!(core::slice::from_raw_parts(vp.add(1), 5), payload);
    }
    assert_eq!(values[2].as_i64(), 99);

    // col1 cacheable (already aligned when reached); col2 behind a varlena.
    assert_eq!(desc.compact_attrs[0].attcacheoff.get(), 0);
    assert_eq!(desc.compact_attrs[1].attcacheoff.get(), 4);
    assert_eq!(desc.compact_attrs[2].attcacheoff.get(), -1);

    assert_eq!(unsafe { nocachegetattr(&tup, 3, &desc) }.as_i64(), 99);
    let mut isnull = false;
    assert_eq!(unsafe { fastgetattr(&tup, 1, &desc, &mut isnull) }.as_i32(), 42);
}

#[test]
fn deform_4b_varlena_with_padding() {
    let ctx = MemoryContext::new("t");
    // (int2, text-not-packable): 4-byte header text aligned to 4 with zero pad.
    let mut text_att = attr(-1, false, 4);
    text_att.attispackable = false;
    let desc = make_desc(ctx.mcx(), &[attr(2, true, 2), text_att]);

    let payload = b"abcdef";
    let mut data = [0u8; 32];
    data[0..2].copy_from_slice(&5i16.to_ne_bytes());
    let vl_len = (4 + payload.len()) as u32;
    data[4..8].copy_from_slice(&(vl_len << 2).to_ne_bytes());
    data[8..8 + payload.len()].copy_from_slice(payload);

    let mut image = Image([0; 256]);
    let (t_len, _) =
        build_tuple_mask(&mut image, 2, None, &data[..8 + payload.len()], HEAP_HASVARWIDTH);
    let tup = tuple_from(&image, t_len);

    let mut values = [Datum::null(); 2];
    let mut nulls = [false; 2];
    heap_deform_tuple(&tup, &desc, &mut values, &mut nulls);
    assert_eq!(values[0].as_i16(), 5);
    let vp = values[1].as_usize() as *const u8;
    unsafe {
        assert_eq!(varsize_any(vp), 10);
        assert_eq!(core::slice::from_raw_parts(vp.add(4), 6), payload);
    }
    // Not cacheable: off (2) needed pad to reach 4, so C leaves attcacheoff -1.
    assert_eq!(desc.compact_attrs[1].attcacheoff.get(), -1);
}

#[test]
fn deform_with_nulls_and_attisnull() {
    let ctx = MemoryContext::new("t");
    let desc = make_desc(ctx.mcx(), &[attr(4, true, 4), attr(8, true, 8), attr(4, true, 4)]);

    // Null bitmap: bits 0 and 2 set (non-null), bit 1 clear (null).
    let mut data = [0u8; 16];
    data[0..4].copy_from_slice(&123i32.to_ne_bytes());
    data[4..8].copy_from_slice(&456i32.to_ne_bytes());
    let mut image = Image([0; 256]);
    let (t_len, _) = build_tuple(&mut image, 3, Some(0b101), &data[..8]);
    let tup = tuple_from(&image, t_len);

    let mut values = [Datum::null(); 3];
    let mut nulls = [false; 3];
    heap_deform_tuple(&tup, &desc, &mut values, &mut nulls);
    assert_eq!(values[0].as_i32(), 123);
    assert!(nulls[1]);
    assert_eq!(values[2].as_i32(), 456);
    assert_eq!(nulls, [false, true, false]);

    assert!(!heap_attisnull(&tup, 1, Some(&desc)));
    assert!(heap_attisnull(&tup, 2, Some(&desc)));
    assert!(!heap_attisnull(&tup, 3, Some(&desc)));
    assert!(!heap_attisnull(&tup, -1, Some(&desc)));

    let mut isnull = false;
    let v = unsafe { fastgetattr(&tup, 3, &desc, &mut isnull) };
    assert!(!isnull);
    assert_eq!(v.as_i32(), 456);
    assert!(unsafe { fastgetattr(&tup, 2, &desc, &mut isnull) }.as_usize() == 0 && isnull);
}

#[test]
fn deform_cstring_walk() {
    let ctx = MemoryContext::new("t");
    // (int4, cstring, int8 NULL, short text, int2): the cstring hands the
    // rest of the walk to the cold continuation.
    let desc = make_desc(
        ctx.mcx(),
        &[
            attr(4, true, 4),
            attr(-2, false, 1),
            attr(8, true, 8),
            attr(-1, false, 4),
            attr(2, true, 2),
        ],
    );

    let mut data = [0u8; 16];
    data[0..4].copy_from_slice(&42i32.to_ne_bytes());
    data[4..8].copy_from_slice(b"abc\0");
    // col3 is null; short varlena "xyz" directly at 8 (packed), int2 at 12.
    data[8] = ((1 + 3) as u8) << 1 | 0x01;
    data[9..12].copy_from_slice(b"xyz");
    data[12..14].copy_from_slice(&7i16.to_ne_bytes());

    let mut image = Image([0; 256]);
    let (t_len, _) =
        build_tuple_mask(&mut image, 5, Some(0b11011), &data[..14], HEAP_HASVARWIDTH);
    let tup = tuple_from(&image, t_len);

    let mut values = [Datum::null(); 5];
    let mut nulls = [false; 5];
    heap_deform_tuple(&tup, &desc, &mut values, &mut nulls);
    assert_eq!(values[0].as_i32(), 42);
    let cp = values[1].as_usize() as *const u8;
    unsafe {
        assert_eq!(core::slice::from_raw_parts(cp, 4), b"abc\0");
    }
    assert!(nulls[2]);
    let vp = values[3].as_usize() as *const u8;
    unsafe {
        assert_eq!(varsize_any(vp), 4);
        assert_eq!(core::slice::from_raw_parts(vp.add(1), 3), b"xyz");
    }
    assert_eq!(values[4].as_i16(), 7);
    assert_eq!(nulls, [false, false, true, false, false]);

    let offs: Vec<i32> = desc.compact_attrs.iter().map(|a| a.attcacheoff.get()).collect();
    assert_eq!(offs, [0, 4, -1, -1, -1]);
}

#[test]
fn deform_cstring_then_missing_tail() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut desc = make_desc(
        mcx,
        &[attr(4, true, 4), attr(-2, false, 1), attr(2, true, 2), attr(4, true, 4)],
    );
    desc.compact_attrs[3].atthasmissing = true;
    let mut missing: PgVec<AttrMissing> = PgVec::new_in(mcx);
    for _ in 0..3 {
        missing.push(AttrMissing { am_present: false, am_value: Datum::null() });
    }
    missing.push(AttrMissing { am_present: true, am_value: Datum::from_i32(777) });
    desc.constr = Some(mcx::alloc_in(
        mcx,
        TupleConstr {
            defval: PgVec::new_in(mcx),
            check: PgVec::new_in(mcx),
            missing,
            num_defval: 0,
            num_check: 0,
            has_not_null: false,
            has_generated_stored: false,
            has_generated_virtual: false,
        },
    )
    .unwrap());

    let mut data = [0u8; 8];
    data[0..4].copy_from_slice(&5i32.to_ne_bytes());
    data[4..6].copy_from_slice(b"q\0");
    data[6..8].copy_from_slice(&33i16.to_ne_bytes());

    let mut image = Image([0; 256]);
    let (t_len, _) = build_tuple_mask(&mut image, 3, None, &data, HEAP_HASVARWIDTH);
    let tup = tuple_from(&image, t_len);

    let mut values = [Datum::null(); 4];
    let mut nulls = [true; 4];
    heap_deform_tuple(&tup, &desc, &mut values, &mut nulls);
    assert_eq!(values[0].as_i32(), 5);
    let cp = values[1].as_usize() as *const u8;
    unsafe {
        assert_eq!(core::slice::from_raw_parts(cp, 2), b"q\0");
    }
    assert_eq!(values[2].as_i16(), 33);
    assert_eq!(values[3].as_i32(), 777);
    assert_eq!(nulls, [false; 4]);
}

#[test]
fn missing_and_absent_attributes() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut desc = make_desc(mcx, &[attr(4, true, 4), attr(4, true, 4), attr(4, true, 4)]);
    desc.compact_attrs[2].atthasmissing = true;
    let mut missing: PgVec<AttrMissing> = PgVec::new_in(mcx);
    missing.push(AttrMissing { am_present: false, am_value: Datum::null() });
    missing.push(AttrMissing { am_present: false, am_value: Datum::null() });
    missing.push(AttrMissing { am_present: true, am_value: Datum::from_i32(777) });
    desc.constr = Some(mcx::alloc_in(
        mcx,
        TupleConstr {
            defval: PgVec::new_in(mcx),
            check: PgVec::new_in(mcx),
            missing,
            num_defval: 0,
            num_check: 0,
            has_not_null: false,
            has_generated_stored: false,
            has_generated_virtual: false,
        },
    )
    .unwrap());

    let mut data = [0u8; 8];
    data[0..4].copy_from_slice(&1i32.to_ne_bytes());
    data[4..8].copy_from_slice(&2i32.to_ne_bytes());
    let mut image = Image([0; 256]);
    let (t_len, _) = build_tuple(&mut image, 2, None, &data);
    let tup = tuple_from(&image, t_len);

    let mut values = [Datum::null(); 3];
    let mut nulls = [false; 3];
    heap_deform_tuple(&tup, &desc, &mut values, &mut nulls);
    assert_eq!(values[0].as_i32(), 1);
    assert_eq!(values[1].as_i32(), 2);
    assert_eq!(values[2].as_i32(), 777);
    assert!(!nulls[2]);

    let mut isnull = false;
    assert_eq!(unsafe { heap_getattr(&tup, 3, &desc, &mut isnull) }.as_i32(), 777);
    assert!(!isnull);
    assert!(!heap_attisnull(&tup, 3, Some(&desc)));

    desc.compact_attrs[2].atthasmissing = false;
    let v = unsafe { heap_getattr(&tup, 3, &desc, &mut isnull) };
    assert!(isnull && v.as_usize() == 0);
    assert!(heap_attisnull(&tup, 3, Some(&desc)));
    assert!(heap_attisnull(&tup, 3, None));
}

#[test]
fn sysattrs() {
    let mut image = Image([0; 256]);
    let (t_len, _) = build_tuple(&mut image, 1, None, &4i32.to_ne_bytes());
    image.0[0..4].copy_from_slice(&100u32.to_ne_bytes());
    image.0[4..8].copy_from_slice(&200u32.to_ne_bytes());
    image.0[8..12].copy_from_slice(&9u32.to_ne_bytes());
    let tup = tuple_from(&image, t_len);

    let mut isnull = true;
    assert_eq!(heap_getsysattr(&tup, -2, &mut isnull).as_u32(), 100);
    assert_eq!(heap_getsysattr(&tup, -4, &mut isnull).as_u32(), 200);
    assert_eq!(heap_getsysattr(&tup, -3, &mut isnull).as_u32(), 9);
    assert_eq!(heap_getsysattr(&tup, -5, &mut isnull).as_u32(), 9);
    assert_eq!(heap_getsysattr(&tup, -6, &mut isnull).as_oid(), 1259);
    let tid = heap_getsysattr(&tup, -1, &mut isnull).as_usize() as *const ItemPointerData;
    assert!(!isnull);
    unsafe {
        assert!(ItemPointerEquals(&*tid, &ItemPointerData::new(11, 3)));
    }
}

#[test]
fn header_accessor_round_trips() {
    let mut image = Image([0; 256]);
    let (t_len, _) = build_tuple(&mut image, 2, None, &[0u8; 8]);
    let mut tup = tuple_from_mut(&mut image, t_len);

    let hdr = tup.t_data_mut();
    hdr.set_xmin(1234);
    hdr.set_xmax(5678);
    hdr.set_cmin(4);
    assert_eq!(hdr.xmin_raw(), 1234);
    assert_eq!(hdr.xmax_raw(), 5678);
    assert_eq!(hdr.raw_command_id(), 4);
    assert_eq!(hdr.xmin(), 1234);
    hdr.set_xmin_frozen();
    assert!(hdr.xmin_frozen());
    assert_eq!(hdr.xmin(), 2);
    assert_eq!(hdr.xvac(), 0);

    hdr.set_cmax(9, true);
    assert!((hdr.t_infomask & HEAP_COMBOCID) != 0);
    hdr.set_cmax(9, false);
    assert!((hdr.t_infomask & HEAP_COMBOCID) == 0);

    hdr.set_natts(2);
    assert_eq!(hdr.natts(), 2);
    hdr.t_infomask2 |= HEAP_HOT_UPDATED;
    assert!(hdr.is_hot_updated());
    hdr.t_infomask |= HEAP_XMAX_INVALID;
    assert!(!hdr.is_hot_updated());
    hdr.t_infomask &= !HEAP_XMAX_INVALID;
    hdr.t_infomask &= !HEAP_XMIN_COMMITTED;
    assert!(!hdr.is_hot_updated());
    hdr.t_infomask &= !HEAP_XMIN_FROZEN;
    assert!(hdr.is_hot_updated());
    hdr.set_heap_only();
    assert!(hdr.is_heap_only());
    hdr.clear_heap_only();
    assert!(!hdr.is_heap_only());

    hdr.set_datum_length(64);
    assert_eq!(hdr.datum_length(), 64);
    assert_eq!(unsafe { varsize_any(tup.header_ptr()) }, 64);
    let hdr = tup.t_data_mut();
    hdr.set_type_id(2249);
    hdr.set_typmod(-1);
    assert_eq!(hdr.type_id(), 2249);
    assert_eq!(hdr.typmod(), -1);

    hdr.set_speculative_token(77);
    assert!(hdr.is_speculative());
    assert_eq!(hdr.speculative_token(), 77);
    hdr.set_moved_partitions();
    assert!(hdr.indicates_moved_partitions());
}

#[test]
fn minimal_tuple_bits() {
    let mut m = MinimalTupleData {
        t_len: 32,
        mt_padding: [0; 6],
        t_infomask2: 3,
        t_infomask: 0,
        t_hoff: (MAXALIGN(SizeofMinimalTupleHeader) + MINIMAL_TUPLE_OFFSET) as u8,
        t_bits: [],
    };
    assert_eq!(m.natts(), 3);
    assert!(!m.has_match());
    m.set_match();
    assert!(m.has_match());
    m.clear_match();
    assert!(!m.has_match());
    m.set_natts(7);
    assert_eq!(m.natts(), 7);
    assert_eq!(m.t_hoff, 24);
}

#[test]
fn varatt_forms() {
    let mut buf = [0u8; 20];
    buf[0] = 0x01;
    buf[1] = VARTAG_ONDISK;
    unsafe {
        assert_eq!(varsize_any(buf.as_ptr()), 18);
    }
    let short: [u8; 2] = [(2u8 << 1) | 1, b'x'];
    unsafe {
        assert_eq!(varsize_any(short.as_ptr()), 2);
    }
    let mut four = [0u8; 8];
    four[0..4].copy_from_slice(&(8u32 << 2).to_ne_bytes());
    unsafe {
        assert_eq!(varsize_any(four.as_ptr()), 8);
    }
}

#[test]
fn populate_compact_attribute_matches_c_mapping() {
    let mut f = FormData_pg_attribute::default();
    f.attlen = -1;
    f.attbyval = false;
    f.attalign = TYPALIGN_INT;
    f.attstorage = TYPSTORAGE_EXTENDED;
    f.attnotnull = true;
    let c = CompactAttribute::populate_from(&f);
    assert_eq!(c.attcacheoff.get(), -1);
    assert_eq!(c.attlen, -1);
    assert!(c.attispackable);
    assert_eq!(c.attalignby, 4);
    assert_eq!(c.attnullability, ATTNULLABLE_UNKNOWN);

    f.attstorage = TYPSTORAGE_PLAIN;
    f.attalign = TYPALIGN_DOUBLE;
    f.attnotnull = false;
    f.attgenerated = ATTRIBUTE_GENERATED_STORED;
    let c = CompactAttribute::populate_from(&f);
    assert!(!c.attispackable);
    assert!(c.attgenerated);
    assert_eq!(c.attalignby, 8);
    assert_eq!(c.attnullability, ATTNULLABLE_UNRESTRICTED);

    f.attalign = TYPALIGN_CHAR;
    assert_eq!(CompactAttribute::populate_from(&f).attalignby, 1);
    f.attalign = TYPALIGN_SHORT;
    assert_eq!(CompactAttribute::populate_from(&f).attalignby, 2);
}

const ATTRIBUTE_GENERATED_STORED: i8 = b's' as i8;
