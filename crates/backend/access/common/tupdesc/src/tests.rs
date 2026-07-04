use super::*;
use ::mcx::MemoryContext;
use ::types_core::{FirstUnpinnedObjectId, InvalidOid as TypcacheInvalidOid, INT2OID};
use ::types_tuple::{
    ATTNULLABLE_UNKNOWN, ATTNULLABLE_UNRESTRICTED, TYPALIGN_SHORT, TYPSTORAGE_MAIN,
};
use std::sync::Once;

static SEAMS: Once = Once::new();
static FORMAT_TYPE_SEAMS: Once = Once::new();

// Dummy composite type oids used only so format_type_be() can name the
// "returned"/"expected" rowtype in attmap error details.
const OUT_ROWTYPE_OID: Oid = 20001;
const IN_ROWTYPE_OID: Oid = 20002;

fn typcache_shape(name: &str) -> syscache_seams::PgTypeTypcacheShape {
    let mut typname = NameData::default();
    typname.namestrcpy(name);
    syscache_seams::PgTypeTypcacheShape {
        typname,
        typlen: -1,
        typbyval: false,
        typalign: TYPALIGN_INT,
        typstorage: TYPSTORAGE_MAIN,
        typtype: b'c' as i8,
        typisdefined: true,
        typrelid: TypcacheInvalidOid,
        typsubscript: TypcacheInvalidOid,
        typelem: TypcacheInvalidOid,
        typarray: TypcacheInvalidOid,
        typcollation: TypcacheInvalidOid,
    }
}

fn scalar_typcache_shape(name: &str) -> syscache_seams::PgTypeTypcacheShape {
    let mut s = typcache_shape(name);
    s.typtype = b'b' as i8;
    s.typstorage = if name == "text" { TYPSTORAGE_EXTENDED } else { TYPSTORAGE_MAIN };
    s
}

fn install_format_type_seams() {
    FORMAT_TYPE_SEAMS.call_once(|| {
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            Ok(match typid {
                OUT_ROWTYPE_OID => Some(typcache_shape("out_row")),
                IN_ROWTYPE_OID => Some(typcache_shape("in_row")),
                INT4OID => Some(scalar_typcache_shape("int4")),
                TEXTOID => Some(scalar_typcache_shape("text")),
                _ => None,
            })
        });
        namespace_seams::type_is_visible::set(|_| Ok(true));
    });
}

fn install_seams() {
    SEAMS.call_once(|| {
        catalog_seams::is_catalog_relation_oid::set(|relid| relid < FirstUnpinnedObjectId);
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                TEXTOID => Some(PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_EXTENDED,
                    typcollation: DEFAULT_COLLATION_OID,
                }),
                INT4OID => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: InvalidOid,
                }),
                INT2OID => Some(PgTypeShape {
                    typlen: 2,
                    typbyval: true,
                    typalign: TYPALIGN_SHORT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: InvalidOid,
                }),
                _ => None,
            })
        });
    });
}

fn attr(name: &str, typid: Oid, attnum: i16, attlen: i16, byval: bool, align: i8) -> FormData_pg_attribute {
    let mut a = FormData_pg_attribute::default();
    a.attname.namestrcpy(name);
    a.atttypid = typid;
    a.attlen = attlen;
    a.attnum = attnum;
    a.atttypmod = -1;
    a.attbyval = byval;
    a.attalign = align;
    a.attstorage = if attlen < 0 { TYPSTORAGE_EXTENDED } else { TYPSTORAGE_PLAIN };
    a.attislocal = true;
    a
}

fn two_col_desc<'m>(mcx: Mcx<'m>) -> TupleDescData<'m> {
    let attrs = [
        attr("a", INT4OID, 1, 4, true, TYPALIGN_INT),
        attr("b", TEXTOID, 2, -1, false, TYPALIGN_INT),
    ];
    CreateTupleDesc(mcx, &attrs).unwrap()
}

#[test]
fn template_matches_c_initial_state() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let desc = CreateTemplateTupleDesc(ctx.mcx(), 3).unwrap();
    assert_eq!(desc.natts, 3);
    assert_eq!(desc.tdtypeid, RECORDOID);
    assert_eq!(desc.tdtypmod, -1);
    assert_eq!(desc.tdrefcount, -1);
    assert!(desc.constr.is_none());
    for i in 0..3 {
        assert_eq!(desc.compact_attr(i).attcacheoff.get(), -1);
    }
}

#[test]
fn populate_compact_attribute_matches_c() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let mut desc = two_col_desc(ctx.mcx());

    let c0 = desc.compact_attr(0);
    assert_eq!(c0.attlen, 4);
    assert!(c0.attbyval);
    assert!(!c0.attispackable);
    assert_eq!(c0.attnullability, ATTNULLABLE_UNRESTRICTED);
    assert_eq!(c0.attalignby, 4);
    let c1 = desc.compact_attr(1);
    assert!(c1.attispackable);
    assert_eq!(c1.attalignby, 4);

    desc.attr_mut(0).attnotnull = true;
    desc.attr_mut(0).attrelid = 16384;
    populate_compact_attribute(&mut desc, 0);
    assert_eq!(desc.compact_attr(0).attnullability, ATTNULLABLE_UNKNOWN);

    desc.attr_mut(0).attrelid = 1259;
    populate_compact_attribute(&mut desc, 0);
    assert_eq!(desc.compact_attr(0).attnullability, ATTNULLABLE_VALID);

    verify_compact_attribute(&desc, 0);
    verify_compact_attribute(&desc, 1);
}

#[test]
#[should_panic(expected = "stale CompactAttribute")]
fn verify_detects_stale_compact_attribute() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let mut desc = two_col_desc(ctx.mcx());
    desc.attr_mut(0).attlen = 8;
    verify_compact_attribute(&desc, 0);
}

#[test]
fn init_entry_and_collation() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let mut desc = CreateTemplateTupleDesc(ctx.mcx(), 2).unwrap();
    TupleDescInitEntry(&mut desc, 1, Some("id"), INT4OID, -1, 0).unwrap();
    TupleDescInitEntry(&mut desc, 2, None, TEXTOID, 30, 0).unwrap();

    let a = desc.attr(0);
    assert_eq!(a.attname.name_str(), b"id");
    assert_eq!(a.attnum, 1);
    assert_eq!(a.attlen, 4);
    assert!(a.attbyval);
    assert_eq!(a.attcollation, InvalidOid);
    assert!(a.attislocal);
    assert_eq!(a.attcompression, InvalidCompressionMethod);

    let b = desc.attr(1);
    assert_eq!(b.attname.name_str(), b"");
    assert_eq!(b.atttypmod, 30);
    assert_eq!(b.attcollation, DEFAULT_COLLATION_OID);

    TupleDescInitEntryCollation(&mut desc, 2, 950);
    assert_eq!(desc.attr(1).attcollation, 950);

    let err = TupleDescInitEntry(&mut desc, 1, Some("x"), 99999, -1, 0).unwrap_err();
    assert!(err.message().contains("cache lookup failed for type 99999"));
}

#[test]
fn builtin_entry_matches_c_table() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let mut desc = CreateTemplateTupleDesc(ctx.mcx(), 5).unwrap();
    TupleDescInitBuiltinEntry(&mut desc, 1, "t", TEXTOID, -1, 0).unwrap();
    TupleDescInitBuiltinEntry(&mut desc, 2, "b", BOOLOID, -1, 0).unwrap();
    TupleDescInitBuiltinEntry(&mut desc, 3, "i4", INT4OID, -1, 0).unwrap();
    TupleDescInitBuiltinEntry(&mut desc, 4, "i8", INT8OID, -1, 0).unwrap();
    TupleDescInitBuiltinEntry(&mut desc, 5, "o", OIDOID, -1, 0).unwrap();

    assert_eq!(desc.attr(0).attlen, -1);
    assert_eq!(desc.attr(0).attcollation, DEFAULT_COLLATION_OID);
    assert_eq!(desc.attr(1).attlen, 1);
    assert_eq!(desc.attr(1).attalign, TYPALIGN_CHAR);
    assert_eq!(desc.attr(2).attlen, 4);
    assert_eq!(desc.attr(3).attlen, 8);
    assert!(desc.attr(3).attbyval);
    assert_eq!(desc.attr(3).attalign, TYPALIGN_DOUBLE);
    assert_eq!(desc.attr(4).attlen, 4);
    assert_eq!(desc.compact_attr(1).attalignby, 1);
    assert_eq!(desc.compact_attr(3).attalignby, 8);

    let err = TupleDescInitBuiltinEntry(&mut desc, 1, "x", INT2OID, -1, 0).unwrap_err();
    assert!(err.message().contains("unsupported type 21"));
}

fn with_constraints<'m>(mcx: Mcx<'m>) -> TupleDescData<'m> {
    let mut desc = two_col_desc(mcx);
    desc.attr_mut(0).attnotnull = true;
    desc.attr_mut(1).atthasdef = true;
    desc.attr_mut(1).atthasmissing = true;
    populate_compact_attribute(&mut desc, 0);
    populate_compact_attribute(&mut desc, 1);

    let mut defval: PgVec<AttrDefault> = PgVec::new_in(mcx);
    defval.push(AttrDefault {
        adnum: 2,
        adbin: Some(PgString::from_str_in("{CONST :val 42}", mcx).unwrap()),
    });
    let mut check: PgVec<ConstrCheck> = PgVec::new_in(mcx);
    check.push(ConstrCheck {
        ccname: Some(PgString::from_str_in("c1", mcx).unwrap()),
        ccbin: Some(PgString::from_str_in("{OPEXPR}", mcx).unwrap()),
        ccenforced: true,
        ccvalid: true,
        ccnoinherit: false,
    });
    let mut missing: PgVec<AttrMissing> = vec_with_capacity_in(mcx, 2).unwrap();
    missing.push(AttrMissing { am_present: true, am_value: Datum::from_i32(7) });
    let varlena: &'static [u8] = &[0x1D, b'h', b'e', b'y'];
    missing.push(AttrMissing {
        am_present: true,
        am_value: Datum::from_usize(varlena.as_ptr() as usize),
    });
    desc.constr = Some(
        alloc_in(
            mcx,
            TupleConstr {
                defval,
                check,
                missing,
                num_defval: 1,
                num_check: 1,
                has_not_null: true,
                has_generated_stored: false,
                has_generated_virtual: false,
            },
        )
        .unwrap(),
    );
    desc
}

#[test]
fn flat_copy_clears_constraint_fields() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let src = with_constraints(ctx.mcx());
    let copy = CreateTupleDescCopy(ctx.mcx(), &src).unwrap();

    assert_eq!(copy.natts, 2);
    assert_eq!(copy.tdtypeid, src.tdtypeid);
    assert_eq!(copy.tdtypmod, src.tdtypmod);
    assert!(copy.constr.is_none());
    assert!(!copy.attr(0).attnotnull);
    assert!(!copy.attr(1).atthasdef);
    assert!(!copy.attr(1).atthasmissing);
    assert_eq!(copy.compact_attr(0).attnullability, ATTNULLABLE_UNRESTRICTED);
    assert_eq!(copy.attr(0).attname.name_str(), b"a");

    let trunc = CreateTupleDescTruncatedCopy(ctx.mcx(), &src, 1).unwrap();
    assert_eq!(trunc.natts, 1);
    assert_eq!(trunc.attr(0).attname.name_str(), b"a");
}

#[test]
fn copy_constr_deep_copies() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let src = with_constraints(ctx.mcx());
    let copy = CreateTupleDescCopyConstr(ctx.mcx(), &src).unwrap();

    assert!(copy.attr(0).attnotnull);
    assert_eq!(copy.compact_attr(0).attnullability, src.compact_attr(0).attnullability);
    let c = copy.constr.as_deref().unwrap();
    let s = src.constr.as_deref().unwrap();
    assert_eq!(c.num_defval, 1);
    assert_eq!(c.defval[0].adbin.as_ref().unwrap().as_str(), "{CONST :val 42}");
    assert!(!std::ptr::eq(
        c.defval[0].adbin.as_ref().unwrap().as_bytes().as_ptr(),
        s.defval[0].adbin.as_ref().unwrap().as_bytes().as_ptr(),
    ));
    assert_eq!(c.missing[0].am_value, s.missing[0].am_value);
    let (cp, sp) = (c.missing[1].am_value.as_usize(), s.missing[1].am_value.as_usize());
    assert_ne!(cp, sp);
    // SAFETY: both point at 4-byte short-varlena images built above/copied.
    unsafe {
        assert_eq!(
            std::slice::from_raw_parts(cp as *const u8, 4),
            std::slice::from_raw_parts(sp as *const u8, 4),
        );
    }
    assert!(equalTupleDescs(&src, &copy));
}

#[test]
fn tupledesc_copy_into_dst() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let src = with_constraints(ctx.mcx());
    let mut dst = CreateTemplateTupleDesc(ctx.mcx(), 2).unwrap();
    dst.tdrefcount = 3;
    TupleDescCopy(&mut dst, &src);
    assert_eq!(dst.tdtypeid, src.tdtypeid);
    assert_eq!(dst.tdrefcount, -1);
    assert!(dst.constr.is_none());
    assert!(!dst.attr(0).attnotnull);
    assert_eq!(dst.attr(1).attname.name_str(), b"b");
}

#[test]
fn copy_entry_resets_attnum_and_constraints() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let src = with_constraints(ctx.mcx());
    let mut dst = CreateTemplateTupleDesc(ctx.mcx(), 3).unwrap();
    TupleDescCopyEntry(&mut dst, 3, &src, 1);
    let a = dst.attr(2);
    assert_eq!(a.attname.name_str(), b"a");
    assert_eq!(a.attnum, 3);
    assert!(!a.attnotnull);
    assert_eq!(dst.compact_attr(2).attlen, 4);
}

#[test]
fn equal_tupledescs_field_sensitivity() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let d1 = two_col_desc(ctx.mcx());
    let mut d2 = two_col_desc(ctx.mcx());
    assert!(equalTupleDescs(&d1, &d2));
    assert!(equalRowTypes(&d1, &d2));
    assert_eq!(hashRowType(&d1), hashRowType(&d2));

    d2.attr_mut(1).atttypmod = 12;
    assert!(!equalTupleDescs(&d1, &d2));
    assert!(!equalRowTypes(&d1, &d2));
    assert_eq!(hashRowType(&d1), hashRowType(&d2));

    let mut d3 = two_col_desc(ctx.mcx());
    d3.tdtypmod = 55;
    assert!(equalTupleDescs(&d1, &d3));

    let mut d4 = two_col_desc(ctx.mcx());
    d4.attr_mut(0).attnotnull = true;
    d4.attr_mut(0).attrelid = 16384;
    populate_compact_attribute(&mut d4, 0);
    assert!(!equalTupleDescs(&d1, &d4));
    assert_eq!(d4.compact_attr(0).attnullability, ATTNULLABLE_UNKNOWN);

    let mut d5 = two_col_desc(ctx.mcx());
    d5.attr_mut(0).attnotnull = true;
    d5.attr_mut(0).attrelid = 16384;
    populate_compact_attribute(&mut d5, 0);
    assert!(equalTupleDescs(&d4, &d5));
    d5.compact_attrs[0].attnullability = ATTNULLABLE_VALID;
    assert!(!equalTupleDescs(&d4, &d5));

    let with_c = with_constraints(ctx.mcx());
    assert!(!equalTupleDescs(&d1, &with_c));
    let with_c2 = CreateTupleDescCopyConstr(ctx.mcx(), &with_c).unwrap();
    assert!(equalTupleDescs(&with_c, &with_c2));
}

#[test]
fn equal_row_types_ignores_storage_fields() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let d1 = two_col_desc(ctx.mcx());
    let mut d2 = two_col_desc(ctx.mcx());
    d2.attr_mut(0).attstorage = TYPSTORAGE_MAIN;
    d2.attr_mut(0).attnotnull = true;
    assert!(equalRowTypes(&d1, &d2));
    assert!(!equalTupleDescs(&d1, &d2));

    d2.attr_mut(0).attname.namestrcpy("z");
    assert!(!equalRowTypes(&d1, &d2));
}

#[test]
fn build_desc_from_lists_and_default_bin() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let desc = BuildDescFromLists(
        ctx.mcx(),
        &["x", "y"],
        &[INT4OID, TEXTOID],
        &[-1, 64],
        &[InvalidOid, 950],
    )
    .unwrap();
    assert_eq!(desc.natts, 2);
    assert_eq!(desc.attr(0).attname.name_str(), b"x");
    assert_eq!(desc.attr(1).attcollation, 950);
    assert_eq!(desc.attr(1).atttypmod, 64);

    assert!(TupleDescGetDefaultBin(&desc, 1).is_none());
    let with_c = with_constraints(ctx.mcx());
    assert_eq!(TupleDescGetDefaultBin(&with_c, 2).unwrap().as_str(), "{CONST :val 42}");
    assert!(TupleDescGetDefaultBin(&with_c, 1).is_none());
}

#[test]
fn refcount_and_free() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let mut desc = two_col_desc(ctx.mcx());
    desc.tdrefcount = 0;
    let rc = Rc::new(desc);
    let pin = IncrTupleDescRefCount(&rc);
    assert_eq!(Rc::strong_count(&rc), 2);
    DecrTupleDescRefCount(pin);
    assert_eq!(Rc::strong_count(&rc), 1);

    let owned = two_col_desc(ctx.mcx());
    FreeTupleDesc(owned);
}

// ---- attmap.c / tupconvert.c residue -----------------------------------

fn dropped_attr(attnum: i16, attlen: i16, align: i8) -> FormData_pg_attribute {
    let mut a = FormData_pg_attribute::default();
    a.attnum = attnum;
    a.attisdropped = true;
    a.attlen = attlen;
    a.attalign = align;
    a.atttypid = InvalidOid;
    a.atttypmod = -1;
    a
}

fn desc_from<'m>(mcx: Mcx<'m>, attrs: &[FormData_pg_attribute], tdtypeid: Oid) -> TupleDescData<'m> {
    let mut desc = CreateTupleDesc(mcx, attrs).unwrap();
    desc.tdtypeid = tdtypeid;
    desc
}

#[test]
fn build_attrmap_by_position_identity_returns_none() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let indesc = two_col_desc(ctx.mcx());
    let outdesc = two_col_desc(ctx.mcx());
    let map = build_attrmap_by_position(ctx.mcx(), &indesc, &outdesc, "test msg").unwrap();
    assert!(map.is_none());
}

#[test]
fn build_attrmap_by_position_handles_dropped_columns() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let in_attrs = [
        attr("a", INT4OID, 1, 4, true, TYPALIGN_INT),
        attr("b", TEXTOID, 2, -1, false, TYPALIGN_INT),
    ];
    let indesc = CreateTupleDesc(ctx.mcx(), &in_attrs).unwrap();
    let out_attrs = [
        attr("a", INT4OID, 1, 4, true, TYPALIGN_INT),
        dropped_attr(2, 4, TYPALIGN_INT),
        attr("b", TEXTOID, 3, -1, false, TYPALIGN_INT),
    ];
    let outdesc = CreateTupleDesc(ctx.mcx(), &out_attrs).unwrap();

    let map = build_attrmap_by_position(ctx.mcx(), &indesc, &outdesc, "test msg")
        .unwrap()
        .expect("column-count differs, conversion required");
    assert_eq!(&map[..], &[1, 0, 2]);
}

#[test]
fn build_attrmap_by_position_mismatch_ereport_text() {
    install_seams();
    install_format_type_seams();
    let ctx = MemoryContext::new("t");
    let indesc = desc_from(
        ctx.mcx(),
        &[attr("b", INT4OID, 1, 4, true, TYPALIGN_INT)],
        IN_ROWTYPE_OID,
    );
    let outdesc = desc_from(
        ctx.mcx(),
        &[attr("b", TEXTOID, 1, -1, false, TYPALIGN_INT)],
        OUT_ROWTYPE_OID,
    );

    let err = build_attrmap_by_position(ctx.mcx(), &indesc, &outdesc, "test msg").unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DATATYPE_MISMATCH);
    assert_eq!(err.message(), "test msg");
    assert_eq!(
        err.detail().unwrap(),
        "Returned type integer does not match expected type text in column \"b\" (position 1)."
    );
}

#[test]
fn build_attrmap_by_position_column_count_ereport_text() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let indesc = CreateTupleDesc(
        ctx.mcx(),
        &[
            attr("a", INT4OID, 1, 4, true, TYPALIGN_INT),
            attr("b", INT4OID, 2, 4, true, TYPALIGN_INT),
        ],
    )
    .unwrap();
    let outdesc = CreateTupleDesc(ctx.mcx(), &[attr("a", INT4OID, 1, 4, true, TYPALIGN_INT)]).unwrap();

    let err = build_attrmap_by_position(ctx.mcx(), &indesc, &outdesc, "test msg").unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DATATYPE_MISMATCH);
    assert_eq!(
        err.detail().unwrap(),
        "Number of returned columns (2) does not match expected column count (1)."
    );
}

#[test]
fn build_attrmap_by_name_handles_dropped_columns() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let indesc = CreateTupleDesc(
        ctx.mcx(),
        &[
            attr("a", INT4OID, 1, 4, true, TYPALIGN_INT),
            attr("b", TEXTOID, 2, -1, false, TYPALIGN_INT),
        ],
    )
    .unwrap();
    let outdesc = CreateTupleDesc(
        ctx.mcx(),
        &[
            attr("a", INT4OID, 1, 4, true, TYPALIGN_INT),
            dropped_attr(2, 4, TYPALIGN_INT),
            attr("b", TEXTOID, 3, -1, false, TYPALIGN_INT),
        ],
    )
    .unwrap();

    let map = build_attrmap_by_name(ctx.mcx(), &indesc, &outdesc).unwrap();
    assert_eq!(&map[..], &[1, 0, 2]);
}

#[test]
fn build_attrmap_by_name_mismatch_ereport_text() {
    install_seams();
    install_format_type_seams();
    let ctx = MemoryContext::new("t");
    let indesc = desc_from(
        ctx.mcx(),
        &[attr("b", INT4OID, 1, 4, true, TYPALIGN_INT)],
        IN_ROWTYPE_OID,
    );
    let outdesc = desc_from(
        ctx.mcx(),
        &[attr("b", TEXTOID, 1, -1, false, TYPALIGN_INT)],
        OUT_ROWTYPE_OID,
    );

    let err = build_attrmap_by_name(ctx.mcx(), &indesc, &outdesc).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DATATYPE_MISMATCH);
    assert_eq!(err.message(), "could not convert row type");
    assert_eq!(
        err.detail().unwrap(),
        "Attribute \"b\" of type out_row does not match corresponding attribute of type in_row."
    );
}

#[test]
fn build_attrmap_by_name_missing_attribute_ereport_text() {
    install_seams();
    install_format_type_seams();
    let ctx = MemoryContext::new("t");
    let indesc = desc_from(
        ctx.mcx(),
        &[attr("a", INT4OID, 1, 4, true, TYPALIGN_INT)],
        IN_ROWTYPE_OID,
    );
    let outdesc = desc_from(
        ctx.mcx(),
        &[attr("b", TEXTOID, 1, -1, false, TYPALIGN_INT)],
        OUT_ROWTYPE_OID,
    );

    let err = build_attrmap_by_name(ctx.mcx(), &indesc, &outdesc).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DATATYPE_MISMATCH);
    assert_eq!(err.message(), "could not convert row type");
    assert_eq!(
        err.detail().unwrap(),
        "Attribute \"b\" of type out_row does not exist in type in_row."
    );

    // missing_ok=true: no error, mapped entry stays 0.
    let map = build_attrmap_by_name_if_req(ctx.mcx(), &indesc, &outdesc, true)
        .unwrap()
        .expect("natts differ in general, but here 1==1 with a 0 entry: still Some");
    assert_eq!(&map[..], &[0]);
}

#[test]
fn build_attrmap_by_name_if_req_identity_returns_none() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let indesc = two_col_desc(ctx.mcx());
    let outdesc = two_col_desc(ctx.mcx());
    let map = build_attrmap_by_name_if_req(ctx.mcx(), &indesc, &outdesc, false).unwrap();
    assert!(map.is_none());

    let map = convert_tuples_by_name(ctx.mcx(), &indesc, &outdesc).unwrap();
    assert!(map.is_none());
}

#[test]
fn convert_tuples_by_position_matches_build_attrmap() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let indesc = two_col_desc(ctx.mcx());
    let outdesc = two_col_desc(ctx.mcx());
    assert!(convert_tuples_by_position(ctx.mcx(), &indesc, &outdesc, "test msg")
        .unwrap()
        .is_none());
}

#[test]
fn convert_tuples_by_name_attrmap_is_identity() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let indesc = CreateTupleDesc(
        ctx.mcx(),
        &[
            attr("a", INT4OID, 1, 4, true, TYPALIGN_INT),
            attr("b", TEXTOID, 2, -1, false, TYPALIGN_INT),
        ],
    )
    .unwrap();
    let outdesc = CreateTupleDesc(
        ctx.mcx(),
        &[attr("b", TEXTOID, 1, -1, false, TYPALIGN_INT)],
    )
    .unwrap();
    let attmap = build_attrmap_by_name(ctx.mcx(), &indesc, &outdesc).unwrap();
    let expected: PgVec<'_, i16> = {
        let mut v = vec_with_capacity_in(ctx.mcx(), attmap.len()).unwrap();
        v.extend(attmap.iter().copied());
        v
    };
    let wrapped = convert_tuples_by_name_attrmap(&indesc, &outdesc, attmap);
    assert_eq!(&wrapped[..], &expected[..]);
}
