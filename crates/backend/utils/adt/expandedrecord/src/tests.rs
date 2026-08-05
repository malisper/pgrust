use super::*;
use datum::expandeddatum::{datum_get_eohp, eoh_flatten_into, eoh_get_flat_size};
use mcx::vec_with_capacity_in;
use types_tuple::{FormData_pg_attribute, HEAP_HASNULL, HEAP_HASVARWIDTH};

const INT4OID: Oid = 23;
const TEXTOID: Oid = 25;

fn att(name: &str, num: i16, typid: Oid, len: i16, byval: bool, align: i8, storage: i8) -> FormData_pg_attribute {
    let mut a = FormData_pg_attribute::default();
    a.attname.namestrcpy(name);
    a.attnum = num;
    a.atttypid = typid;
    a.attlen = len;
    a.attbyval = byval;
    a.attalign = align;
    a.attstorage = storage;
    a.atttypmod = -1;
    a
}

fn int_text_desc(mcx: Mcx<'_>) -> TupleDescData<'_> {
    tupdesc::CreateTupleDesc(
        mcx,
        &[
            att("a", 1, INT4OID, 4, true, b'i' as i8, b'p' as i8),
            att("b", 2, TEXTOID, -1, false, b'i' as i8, b'x' as i8),
        ],
    )
    .unwrap()
}

fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> Datum {
    let mut v = vec_with_capacity_in(mcx, 4 + s.len()).unwrap();
    mcx::vec_append_bytes(&mut v, &datum::varlena::set_varsize_4b(4 + s.len())).unwrap();
    mcx::vec_append_bytes(&mut v, s.as_bytes()).unwrap();
    let d = Datum::from_usize(v.as_ptr() as usize);
    core::mem::forget(v);
    d
}

fn registered_record(parent: &MemoryContext) -> *mut ExpandedRecordHeader {
    let mcx = parent.mcx();
    let mut td = int_text_desc(mcx);
    typcache::assign_record_type_typmod(&mut td).unwrap();
    make_expanded_record_from_tupdesc(&td, parent).unwrap()
}

unsafe fn text_bytes(d: Datum) -> &'static [u8] {
    let p = d.as_usize() as *const u8;
    // Deformed in-tuple fields can carry the packed 1-byte header form.
    let hdr = if varatt_is_1b(p) { 1 } else { 4 };
    core::slice::from_raw_parts(p.add(hdr), varsize_any(p) - hdr)
}

#[test]
fn empty_record_reads_null() {
    let parent = MemoryContext::new("t");
    let p = registered_record(&parent);
    let erh = unsafe { &mut *p };
    assert!(erh.is_empty());
    assert_eq!(erh.nfields, 2);
    assert_ne!(erh.er_tupdesc_id, typcache::INVALID_TUPLEDESC_IDENTIFIER);
    assert_eq!(expanded_record_fetch_field(erh, 1).unwrap(), (Datum::null(), true));
    assert_eq!(expanded_record_fetch_field(erh, 99).unwrap(), (Datum::null(), true));
    assert_eq!(expanded_record_fetch_field(erh, -1).unwrap(), (Datum::null(), true));
}

#[test]
fn set_fields_fetch_and_flatten_matches_hand_layout() {
    let parent = MemoryContext::new("t");
    let mcx = parent.mcx();
    let p = registered_record(&parent);
    let erh = unsafe { &mut *p };

    let vals = [Datum::from_i32(42), text_datum(mcx, "hello")];
    expanded_record_set_fields(erh, &vals, &[false, false], false).unwrap();

    let (d, isnull) = expanded_record_get_field(erh, 1).unwrap();
    assert!(!isnull);
    assert_eq!(d.as_i32(), 42);
    let (d, isnull) = expanded_record_get_field(erh, 2).unwrap();
    assert!(!isnull);
    assert_eq!(unsafe { text_bytes(d) }, b"hello");

    let eoh = unsafe { datum_get_eohp(expanded_record_rw_datum(p)) };
    // Flatten reaches the header through its own images: re-derive refs after.
    let n = unsafe { eoh_get_flat_size(eoh) };
    let erh = unsafe { &*p };
    // int4 at 0..4, short-varlena text 1+5 at 4..10; hoff = MAXALIGN(23) = 24.
    assert_eq!(n, 24 + 10);
    assert_eq!(erh.hoff, 24);
    assert!(!erh.hasnull);
    assert_eq!(erh.data_len, 10);

    let mut buf64 = vec![0u64; n.div_ceil(8)];
    let bufp = buf64.as_mut_ptr() as *mut u8;
    unsafe { eoh_flatten_into(eoh, bufp, n) };
    let buf = unsafe { core::slice::from_raw_parts(bufp, n) };
    let erh = unsafe { &*p };
    let hdr = unsafe { &*(bufp as *const HeapTupleHeaderData) };
    assert_eq!(hdr.datum_length(), n as u32);
    assert_eq!(hdr.type_id(), RECORDOID);
    assert_eq!(hdr.typmod(), erh.er_typmod);
    assert!(erh.er_typmod >= 0);
    assert_eq!(hdr.natts(), 2);
    assert_eq!(hdr.t_hoff, 24);
    assert_eq!(hdr.t_infomask & HEAP_HASNULL, 0);
    assert_ne!(hdr.t_infomask & HEAP_HASVARWIDTH, 0);
    assert_eq!(&buf[24..28], &42i32.to_ne_bytes());
    assert_eq!(buf[28], 0x0D);
    assert_eq!(&buf[29..34], b"hello");

    let cached = unsafe { eoh_get_flat_size(eoh) };
    assert_eq!(cached, n);
}

#[test]
fn null_field_sets_bitmap() {
    let parent = MemoryContext::new("t");
    let p = registered_record(&parent);
    let erh = unsafe { &mut *p };

    expanded_record_set_fields(erh, &[Datum::from_i32(7), Datum::null()], &[false, true], false)
        .unwrap();

    let eoh = unsafe { datum_get_eohp(expanded_record_rw_datum(p)) };
    let n = unsafe { eoh_get_flat_size(eoh) };
    let erh = unsafe { &*p };
    // hoff = MAXALIGN(23 + BITMAPLEN(2)) = 24; data = int4 only.
    assert_eq!(n, 24 + 4);
    assert!(erh.hasnull);

    let mut buf64 = vec![0u64; n.div_ceil(8)];
    let bufp = buf64.as_mut_ptr() as *mut u8;
    unsafe { eoh_flatten_into(eoh, bufp, n) };
    let buf = unsafe { core::slice::from_raw_parts(bufp, n) };
    let hdr = unsafe { &*(bufp as *const HeapTupleHeaderData) };
    assert_ne!(hdr.t_infomask & HEAP_HASNULL, 0);
    assert_eq!(buf[23], 0b01);
    assert_eq!(&buf[24..28], &7i32.to_ne_bytes());
}

#[test]
fn set_tuple_deconstruct_and_replace_field() {
    let parent = MemoryContext::new("t");
    let mcx = parent.mcx();
    let p = registered_record(&parent);
    let erh = unsafe { &mut *p };

    let td = int_text_desc(mcx);
    let vals = [Datum::from_i32(1), text_datum(mcx, "abc")];
    let tup = heap_form_tuple(mcx, &td, &vals, &[false, false]).unwrap();

    unsafe { expanded_record_set_tuple(erh, Some(tup.as_tuple()), true, false).unwrap() };
    assert!(erh.flags & ER_FLAG_FVALUE_VALID != 0);
    assert!(erh.flags & ER_FLAG_FVALUE_ALLOCED != 0);

    let (d, isnull) = expanded_record_fetch_field(erh, 2).unwrap();
    assert!(!isnull);
    assert_eq!(unsafe { text_bytes(d) }, b"abc");
    let ptr = d.as_usize() as *const u8;
    assert!(ptr >= erh.fstartptr && ptr < erh.fendptr);

    expanded_record_set_field(erh, 2, text_datum(mcx, "wxyz"), false, false).unwrap();
    assert_eq!(erh.flags & ER_FLAG_FVALUE_VALID, 0);
    assert!(erh.flags & ER_FLAG_DVALUES_ALLOCED != 0);
    let (d, _) = expanded_record_get_field(erh, 2).unwrap();
    assert_eq!(unsafe { text_bytes(d) }, b"wxyz");
    let ptr = d.as_usize() as *const u8;
    assert!(ptr < erh.fstartptr || ptr >= erh.fendptr);

    let formed = expanded_record_get_tuple(mcx, erh).unwrap().unwrap();
    let mut vals2 = [Datum::null(); 2];
    let mut nulls2 = [false; 2];
    heap_deform_tuple(formed.tuple(), &td, &mut vals2, &mut nulls2);
    assert_eq!(vals2[0].as_i32(), 1);
    assert_eq!(unsafe { text_bytes(vals2[1]) }, b"wxyz");

    unsafe { expanded_record_set_tuple(erh, None, false, false).unwrap() };
    assert!(erh.is_empty());
    assert!(expanded_record_get_tuple(mcx, erh).unwrap().is_none());
}

#[test]
fn from_datum_roundtrip_and_rw_reuse() {
    let parent = MemoryContext::new("t");
    let mcx = parent.mcx();
    let mut td = int_text_desc(mcx);
    typcache::assign_record_type_typmod(&mut td).unwrap();

    let vals = [Datum::from_i32(5), text_datum(mcx, "xy")];
    let tup = heap_form_tuple(mcx, &td, &vals, &[false, false]).unwrap();
    let comp = heaptuple::heap_copy_tuple_as_datum(mcx, tup.as_tuple(), &td).unwrap();

    let d = make_expanded_record_from_datum(comp, &parent).unwrap();
    let p = unsafe { datum_get_expanded_record(d, &parent).unwrap() };
    assert_eq!(unsafe { datum_get_expanded_record(d, &parent).unwrap() }, p);
    let erh = unsafe { &mut *p };
    assert_eq!(erh.er_typeid, RECORDOID);
    assert_eq!(erh.er_typmod, td.tdtypmod);
    assert!(erh.er_tupdesc.is_none());

    let (v, isnull) = expanded_record_fetch_field(erh, 1).unwrap();
    assert!(!isnull);
    assert_eq!(v.as_i32(), 5);
    assert!(erh.er_tupdesc.is_some());
    assert_ne!(erh.er_tupdesc_id, typcache::INVALID_TUPLEDESC_IDENTIFIER);
    let (v, _) = expanded_record_fetch_field(erh, 2).unwrap();
    assert_eq!(unsafe { text_bytes(v) }, b"xy");

    let rw = unsafe { expanded_record_rw_datum(p) };
    let ro = unsafe { expanded_record_ro_datum(p) };
    let _ = erh;

    // datum_copy flattens a R/W expanded datum through the ER methods table.
    let flat = adt_scalar::datum_copy(mcx, rw, false, -1).unwrap();
    let fp = flat.as_usize() as *const u8;
    let n = unsafe { varsize_any(fp) };
    assert_eq!(n, tup.as_tuple().t_len as usize);
    let fh = unsafe { &*(fp as *const HeapTupleHeaderData) };
    assert_eq!(fh.type_id(), RECORDOID);
    assert_eq!(fh.typmod(), td.tdtypmod);

    // Expanding a read-only image goes through the flatten-and-copy path.
    let p2 = unsafe { datum_get_expanded_record(ro, &parent).unwrap() };
    assert_ne!(p2, p);
    let (v, _) = expanded_record_fetch_field(unsafe { &mut *p2 }, 1).unwrap();
    assert_eq!(v.as_i32(), 5);
}

#[test]
fn from_exprecord_copies_rowtype_only() {
    let parent = MemoryContext::new("t");
    let mcx = parent.mcx();
    let p = registered_record(&parent);
    let erh = unsafe { &mut *p };
    expanded_record_set_fields(erh, &[Datum::from_i32(9), Datum::null()], &[false, true], false)
        .unwrap();
    erh.flags |= ER_FLAG_IS_DOMAIN;
    let _ = erh;

    // from_exprecord re-derives its own &mut from p; ours must be dead.
    let p2 = unsafe { make_expanded_record_from_exprecord(p, &parent).unwrap() };
    let erh = unsafe { &*p };
    let new = unsafe { &mut *p2 };
    assert!(new.is_empty());
    assert!(new.is_domain());
    assert_eq!(new.er_typeid, erh.er_typeid);
    assert_eq!(new.er_typmod, erh.er_typmod);
    assert_eq!(new.er_tupdesc_id, erh.er_tupdesc_id);
    assert_eq!(new.nfields, 2);
    assert_eq!(new.flags & ER_FLAG_FVALUE_VALID, 0);
}

#[test]
fn lookup_field_by_name_and_sysattr() {
    let parent = MemoryContext::new("t");
    let p = registered_record(&parent);
    let erh = unsafe { &mut *p };

    let f = expanded_record_lookup_field(erh, "b").unwrap().unwrap();
    assert_eq!(f.fnumber, 2);
    assert_eq!(f.ftypeid, TEXTOID);
    assert_eq!(f.ftypmod, -1);

    let f = expanded_record_lookup_field(erh, "ctid").unwrap().unwrap();
    assert_eq!(f.fnumber, -1);

    assert!(expanded_record_lookup_field(erh, "nope").unwrap().is_none());
}

#[test]
fn set_field_bounds_error() {
    let parent = MemoryContext::new("t");
    let p = registered_record(&parent);
    let erh = unsafe { &mut *p };
    let e = expanded_record_set_field(erh, 3, Datum::from_i32(1), false, false).unwrap_err();
    assert!(e.message().contains("cannot assign to field 3"));
    let e = expanded_record_set_field(erh, 0, Datum::from_i32(1), false, false).unwrap_err();
    assert!(e.message().contains("cannot assign to field 0"));
}

fn fake_domain_check(
    value: Datum,
    isnull: bool,
    _domain_type: Oid,
    escontext: Option<&mut types_error::SoftErrorContext>,
) -> PgResult<()> {
    let violation = if isnull {
        true
    } else {
        // Flatten the (possibly expanded) composite and read int4 field 1.
        let ctx = MemoryContext::new("fake domain check");
        let flat = adt_scalar::datum_copy(ctx.mcx(), value, false, -1)?;
        let p = flat.as_usize() as *const u8;
        // Field 1 (int4, never null in these tests) sits at t_hoff.
        let field1 = unsafe {
            let hoff = *p.add(22) as usize;
            core::ptr::read_unaligned(p.add(hoff) as *const i32)
        };
        field1 < 0
    };
    if violation {
        let err = PgError::error("value for domain violates check constraint")
            .with_sqlstate(types_error::ERRCODE_CHECK_VIOLATION);
        return types_error::ereturn(escontext, (), err);
    }
    Ok(())
}

#[test]
fn domain_checks_gate_all_mutation_paths() {
    typcache_seams::domain_check_input::set(fake_domain_check);
    let parent = MemoryContext::new("t");
    let mcx = parent.mcx();
    let p = registered_record(&parent);
    let erh = unsafe { &mut *p };
    erh.flags |= ER_FLAG_IS_DOMAIN;
    erh.er_decltypeid = 90001;

    // NULL composite rejected by the domain.
    assert!(unsafe { expanded_record_set_tuple(erh, None, false, false) }.is_err());
    assert!(erh.is_empty());

    expanded_record_set_fields(erh, &[Datum::from_i32(3), Datum::null()], &[false, true], false)
        .unwrap();
    let (v, _) = expanded_record_get_field(erh, 1).unwrap();
    assert_eq!(v.as_i32(), 3);

    // Rejected set_field leaves the record untouched (checked via dummy header).
    assert!(expanded_record_set_field(erh, 1, Datum::from_i32(-5), false, false).is_err());
    let (v, isnull) = expanded_record_get_field(erh, 1).unwrap();
    assert!(!isnull);
    assert_eq!(v.as_i32(), 3);

    expanded_record_set_field(erh, 1, Datum::from_i32(8), false, false).unwrap();
    let (v, _) = expanded_record_get_field(erh, 1).unwrap();
    assert_eq!(v.as_i32(), 8);

    // Dummy header is cached across checks.
    let dummy1 = erh.er_dummy_header;
    assert!(!dummy1.is_null());
    expanded_record_set_field(erh, 1, Datum::from_i32(9), false, false).unwrap();
    assert_eq!(erh.er_dummy_header, dummy1);

    // set_tuple path checks the proposed tuple through the dummy fvalue.
    let td = int_text_desc(mcx);
    let bad = heap_form_tuple(mcx, &td, &[Datum::from_i32(-1), Datum::null()], &[false, true])
        .unwrap();
    assert!(unsafe { expanded_record_set_tuple(erh, Some(bad.as_tuple()), true, false) }.is_err());
    let (v, _) = expanded_record_get_field(erh, 1).unwrap();
    assert_eq!(v.as_i32(), 9);

    let good = heap_form_tuple(mcx, &td, &[Datum::from_i32(4), Datum::null()], &[false, true])
        .unwrap();
    unsafe { expanded_record_set_tuple(erh, Some(good.as_tuple()), true, false).unwrap() };
    let (v, _) = expanded_record_get_field(erh, 1).unwrap();
    assert_eq!(v.as_i32(), 4);

    // set_fields runs the domain check on the record itself, post-mutation.
    assert!(expanded_record_set_fields(
        erh,
        &[Datum::from_i32(-2), Datum::null()],
        &[false, true],
        false
    )
    .is_err());
}
