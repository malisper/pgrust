use ::datum::array_build::ArrayBuildState;
use ::datum::Datum;
use ::mcx::{vec_append_bytes, vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use ::stringinfo::StringInfo;
use ::types_core::{FLOAT8OID, INT4OID, TEXTOID};
use ::types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, LocalFcinfo};
use ::types_error::PgResult;

use crate::build::{accum_array_result, make_array_result};
use crate::construct::{construct_array, construct_md_array, deconstruct_array};
use crate::foundation::{varsize_any, TYPALIGN_DOUBLE, TYPALIGN_INT};
use crate::io::{array_in, array_out, array_recv, array_send, ArrayIoMeta};

// Local identity text codec (avoids depending on the sibling `varlena` crate,
// which a concurrent session may have mid-edit). Exercises the by-ref lane;
// array-level quoting/escaping is entirely array_in/array_out's job.
std::thread_local! {
    static TEXT_SCRATCH: core::cell::RefCell<std::vec::Vec<u8>> = const { core::cell::RefCell::new(std::vec::Vec::new()) };
}

fn build_varlena<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Datum> {
    let total = ::datum::VARHDRSZ + payload.len();
    let mut img = vec_with_capacity_in(mcx, total)?;
    img.extend_from_slice(&::datum::varlena::set_varsize_4b(total));
    img.extend_from_slice(payload);
    let d = Datum::from_usize(img.as_ptr() as usize);
    core::mem::forget(img);
    Ok(d)
}

fn varlena_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    let total = varsize_any(p);
    unsafe { core::slice::from_raw_parts(p.add(::datum::VARHDRSZ), total - ::datum::VARHDRSZ) }
}

fn fc_mytextin(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes().to_vec();
    build_varlena(fcinfo.result_mcx(), &s)
}
fn fc_mytextout(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let payload = varlena_payload(fcinfo.arg(0)).to_vec();
    TEXT_SCRATCH.with(|c| {
        let mut b = c.borrow_mut();
        b.clear();
        b.extend_from_slice(&payload);
        b.push(0);
        Ok(Datum::from_usize(b.as_ptr() as usize))
    })
}
fn fc_mytextrecv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let buf = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut StringInfo<'_>) };
    let n = buf.len() - buf.cursor;
    let bytes = ::pqformat::pq_getmsgbytes(buf, n)?.to_vec();
    build_varlena(fcinfo.result_mcx(), &bytes)
}
fn fc_mytextsend(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let payload = varlena_payload(fcinfo.arg(0)).to_vec();
    let mut b = ::pqformat::pq_begintypsend(fcinfo.result_mcx())?;
    ::pqformat::pq_sendbytes(&mut b, &payload)?;
    Ok(::types_fmgr::varlena_result(::pqformat::pq_endtypsend(b)))
}

fn meta_int4() -> ArrayIoMeta {
    ArrayIoMeta { element_type: INT4OID, typlen: 4, typbyval: true, typalign: b'i', typdelim: b',', typioparam: INT4OID }
}
fn meta_text() -> ArrayIoMeta {
    ArrayIoMeta { element_type: TEXTOID, typlen: -1, typbyval: false, typalign: b'i', typdelim: b',', typioparam: TEXTOID }
}

fn int4_in() -> FmgrInfo { FmgrInfo::new(adt_int::builtins::fc_int4in, 42, 1, true, false) }
fn int4_out() -> FmgrInfo { FmgrInfo::new(adt_int::builtins::fc_int4out, 43, 1, true, false) }
fn text_in() -> FmgrInfo { FmgrInfo::new(fc_mytextin, 46, 1, true, false) }
fn text_out() -> FmgrInfo { FmgrInfo::new(fc_mytextout, 47, 1, true, false) }

fn as_str(v: &[u8]) -> &str {
    core::str::from_utf8(&v[..v.len() - 1]).unwrap()
}

fn rt_int4(mcx: Mcx<'_>, lit: &str) -> String {
    let m = meta_int4();
    let mut ip = int4_in();
    let img = array_in(mcx, lit, &m, &mut ip, -1, None).unwrap().unwrap();
    let mut op = int4_out();
    as_str(&array_out(mcx, &img, &m, &mut op).unwrap()).to_string()
}

fn rt_text(mcx: Mcx<'_>, lit: &str) -> String {
    let m = meta_text();
    let mut ip = text_in();
    let img = array_in(mcx, lit, &m, &mut ip, -1, None).unwrap().unwrap();
    let mut op = text_out();
    as_str(&array_out(mcx, &img, &m, &mut op).unwrap()).to_string()
}

#[test]
fn int4_roundtrips() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    assert_eq!(rt_int4(mcx, "{1,2,3}"), "{1,2,3}");
    assert_eq!(rt_int4(mcx, "{-5,0,2147483647}"), "{-5,0,2147483647}");
    assert_eq!(rt_int4(mcx, "{}"), "{}");
    assert_eq!(rt_int4(mcx, "  { 42 }  "), "{42}");
}

#[test]
fn int4_multidim_and_nulls() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    assert_eq!(rt_int4(mcx, "{{1,2},{3,4}}"), "{{1,2},{3,4}}");
    assert_eq!(rt_int4(mcx, "{{{1},{2}},{{3},{4}}}"), "{{{1},{2}},{{3},{4}}}");
    assert_eq!(rt_int4(mcx, "{1,NULL,3}"), "{1,NULL,3}");
    assert_eq!(rt_int4(mcx, "{NULL}"), "{NULL}");
}

#[test]
fn int4_explicit_dims() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    assert_eq!(rt_int4(mcx, "[2:4]={7,8,9}"), "[2:4]={7,8,9}");
    assert_eq!(rt_int4(mcx, "[0:1]={1,2}"), "[0:1]={1,2}");
    assert_eq!(rt_int4(mcx, "[1:3]={1,2,3}"), "{1,2,3}");
}

#[test]
fn text_quoting_and_escapes() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    assert_eq!(rt_text(mcx, "{a,b,c}"), "{a,b,c}");
    assert_eq!(rt_text(mcx, r#"{"a,b","c d"}"#), r#"{"a,b","c d"}"#);
    assert_eq!(rt_text(mcx, r#"{"",x}"#), r#"{"",x}"#);
    assert_eq!(rt_text(mcx, r#"{"NULL",NULL}"#), r#"{"NULL",NULL}"#);
    assert_eq!(rt_text(mcx, r#"{"a\"b","c\\d"}"#), r#"{"a\"b","c\\d"}"#);
    assert_eq!(rt_text(mcx, r#"{a\,b}"#), r#"{"a,b"}"#);
}

#[test]
fn text_multidim() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    assert_eq!(rt_text(mcx, r#"{{a,b},{c,d}}"#), r#"{{a,b},{c,d}}"#);
}

#[test]
fn construct_deconstruct_int4() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let elems = [Datum::from_i32(10), Datum::from_i32(20), Datum::from_i32(30)];
    let img = construct_md_array(mcx, &elems, None, 1, &[3], &[1], INT4OID, 4, true, b'i').unwrap();
    let (out, nulls) = deconstruct_array(mcx, &img, 4, true, b'i', true).unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0].as_i32(), 10);
    assert_eq!(out[2].as_i32(), 30);
    assert!(nulls.iter().all(|&n| !n));
}

#[test]
fn builder_accumulates_int4() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    // Manual meta (accum's Some-path never touches lsyscache).
    let mut st = ArrayBuildState::new(mcx, INT4OID, true).unwrap();
    st.typlen = 4;
    st.typbyval = true;
    st.typalign = b'i';
    let mut astate = Some(st);
    for v in [5i32, 6, 7] {
        astate = Some(accum_array_result(mcx, astate.take(), Datum::from_i32(v), false, INT4OID).unwrap());
    }
    let img = make_array_result(mcx, astate.as_ref().unwrap()).unwrap();
    let m = meta_int4();
    let mut op = int4_out();
    assert_eq!(as_str(&array_out(mcx, &img, &m, &mut op).unwrap()), "{5,6,7}");
}

#[test]
fn text_send_recv_roundtrip() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let m = meta_text();
    let mut ip = text_in();
    let img = array_in(mcx, r#"{a,"b,c",d}"#, &m, &mut ip, -1, None).unwrap().unwrap();
    let mut sp = FmgrInfo::new(fc_mytextsend, 47, 1, true, false);
    let sent = array_send(mcx, &img, &m, &mut sp).unwrap();
    let payload = sent.data().to_vec();
    let mut buf = StringInfo::with_capacity_in(mcx, payload.len()).unwrap();
    buf.append_bytes(&payload).unwrap();
    let mut rp = FmgrInfo::new(fc_mytextrecv, 46, 1, true, false);
    let img2 = array_recv(mcx, &mut buf, &m, &mut rp, -1).unwrap();
    let mut op = text_out();
    assert_eq!(as_str(&array_out(mcx, &img2, &m, &mut op).unwrap()), r#"{a,"b,c",d}"#);
}

// KNOWN-DIV-1 (p1-lanewire): C array_recv complains (42804) when the element
// type OID recorded in the wire image differs from the expected element type
// and BOTH OIDs are in the built-in range (< FirstGenbkiObjectId); for a
// mismatch involving a non-built-in OID it carries on with the expected type
// (arrayfuncs.c array_recv).
#[test]
fn recv_wrong_element_type() {
    // The 42804 message path renders both type names via
    // format_type_extended(..., ALLOW_INVALID); unknown types print "???".
    {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            ::syscache_seams::lookup_pg_type_typcache_shape::set(|_typid| Ok(None));
        });
    }
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let m = meta_text();

    // 1-D one-element array wire image with an overridable element-type word.
    let build = |elem_oid: u32| {
        let mut w: std::vec::Vec<u8> = std::vec::Vec::new();
        w.extend_from_slice(&1i32.to_be_bytes()); // ndim
        w.extend_from_slice(&0i32.to_be_bytes()); // flags
        w.extend_from_slice(&elem_oid.to_be_bytes()); // element type
        w.extend_from_slice(&1i32.to_be_bytes()); // dim[0]
        w.extend_from_slice(&1i32.to_be_bytes()); // lbound[0]
        w.extend_from_slice(&2i32.to_be_bytes()); // itemlen
        w.extend_from_slice(b"ab");
        w
    };

    // Built-in vs built-in mismatch (int4 in the wire image, text expected).
    let payload = build(INT4OID);
    let mut buf = StringInfo::with_capacity_in(mcx, payload.len()).unwrap();
    buf.append_bytes(&payload).unwrap();
    let mut rp = FmgrInfo::new(fc_mytextrecv, 46, 1, true, false);
    let e = array_recv(mcx, &mut buf, &m, &mut rp, -1).unwrap_err();
    assert_eq!(
        core::str::from_utf8(&::types_error::unpack_sqlstate(e.sqlstate())).unwrap(),
        "42804"
    );
    assert_eq!(
        e.message(),
        "binary data has array element type 23 (???) instead of expected 25 (???)"
    );

    // Mismatch where the wire OID is outside the built-in range: C carries on
    // with the expected element type and decodes normally.
    let payload = build(20000);
    let mut buf = StringInfo::with_capacity_in(mcx, payload.len()).unwrap();
    buf.append_bytes(&payload).unwrap();
    let mut rp = FmgrInfo::new(fc_mytextrecv, 46, 1, true, false);
    let img = array_recv(mcx, &mut buf, &m, &mut rp, -1).unwrap();
    let mut op = text_out();
    assert_eq!(as_str(&array_out(mcx, &img, &m, &mut op).unwrap()), "{ab}");
}

#[test]
fn element_fetch_and_slice() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let m = meta_int4();
    let mut ip = int4_in();
    let img = array_in(mcx, "{10,20,NULL,40}", &m, &mut ip, -1, None).unwrap().unwrap();

    let (d, isnull) = crate::element::array_get_element(&img, &[2], -1, 4, true, b'i');
    assert!(!isnull);
    assert_eq!(d.as_i32(), 20);
    let (_, isnull) = crate::element::array_get_element(&img, &[3], -1, 4, true, b'i');
    assert!(isnull);
    let (_, isnull) = crate::element::array_get_element(&img, &[99], -1, 4, true, b'i');
    assert!(isnull);

    // Slice [2:99] silently truncates to the array bound (C shape).
    let mut upper = [99i32, 0, 0, 0, 0, 0];
    let mut lower = [2i32, 0, 0, 0, 0, 0];
    let provided = [true, false, false, false, false, false];
    let slice = crate::element::array_get_slice(
        mcx, &img, 1, &mut upper, &mut lower, &provided, &provided, -1, 4, b'i',
    )
    .unwrap();
    let mut op = int4_out();
    assert_eq!(as_str(&array_out(mcx, &slice, &m, &mut op).unwrap()), "{20,NULL,40}");
}

#[test]
fn element_set_replaces_and_extends() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let m = meta_int4();
    let mut ip = int4_in();
    let img = array_in(mcx, "{1,2,3}", &m, &mut ip, -1, None).unwrap().unwrap();
    let mut op = int4_out();

    let set = crate::element::array_set_element(
        mcx, &img, &[2], Datum::from_i32(99), false, -1, 4, true, b'i',
    )
    .unwrap();
    assert_eq!(as_str(&array_out(mcx, &set, &m, &mut op).unwrap()), "{1,99,3}");

    // 1-D extension past the end inserts intervening NULLs (C shape).
    let ext = crate::element::array_set_element(
        mcx, &img, &[5], Datum::from_i32(7), false, -1, 4, true, b'i',
    )
    .unwrap();
    assert_eq!(as_str(&array_out(mcx, &ext, &m, &mut op).unwrap()), "{1,2,3,NULL,7}");

    // Extension below the lower bound shifts it (renders with explicit dims).
    let low = crate::element::array_set_element(
        mcx, &img, &[-1], Datum::from_i32(0), false, -1, 4, true, b'i',
    )
    .unwrap();
    assert_eq!(as_str(&array_out(mcx, &low, &m, &mut op).unwrap()), "[-1:3]={0,NULL,1,2,3}");
}

#[test]
fn slice_set_replaces_extends_and_nulls() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let m = meta_int4();
    let mut ip = int4_in();
    let mut op = int4_out();
    let img = array_in(mcx, "{1,2,3,4,5}", &m, &mut ip, -1, None).unwrap().unwrap();
    let one = [true, false, false, false, false, false];

    // Replace [2:4].
    let src = array_in(mcx, "{20,30,40}", &m, &mut ip, -1, None).unwrap().unwrap();
    let mut upper = [4i32, 0, 0, 0, 0, 0];
    let mut lower = [2i32, 0, 0, 0, 0, 0];
    let set = crate::element::array_set_slice(
        mcx, &img, 1, &mut upper, &mut lower, &one, &one, &src, -1, 4, true, b'i',
    )
    .unwrap();
    assert_eq!(as_str(&array_out(mcx, &set, &m, &mut op).unwrap()), "{1,20,30,40,5}");

    // Extension past the end with a NULL gap.
    let src = array_in(mcx, "{80,90}", &m, &mut ip, -1, None).unwrap().unwrap();
    let mut upper = [9i32, 0, 0, 0, 0, 0];
    let mut lower = [8i32, 0, 0, 0, 0, 0];
    let ext = crate::element::array_set_slice(
        mcx, &img, 1, &mut upper, &mut lower, &one, &one, &src, -1, 4, true, b'i',
    )
    .unwrap();
    assert_eq!(
        as_str(&array_out(mcx, &ext, &m, &mut op).unwrap()),
        "{1,2,3,4,5,NULL,NULL,80,90}"
    );

    // NULL-carrying source keeps its bitmap.
    let src = array_in(mcx, "{NULL,99}", &m, &mut ip, -1, None).unwrap().unwrap();
    let mut upper = [2i32, 0, 0, 0, 0, 0];
    let mut lower = [1i32, 0, 0, 0, 0, 0];
    let n = crate::element::array_set_slice(
        mcx, &img, 1, &mut upper, &mut lower, &one, &one, &src, -1, 4, true, b'i',
    )
    .unwrap();
    assert_eq!(as_str(&array_out(mcx, &n, &m, &mut op).unwrap()), "{NULL,99,3,4,5}");

    // ndim == 0: empty target needs both bounds; builds from the source.
    let all = [true, true, false, false, false, false];
    let empty = crate::construct::construct_empty_array(mcx, INT4OID).unwrap();
    let src = array_in(mcx, "{7,8}", &m, &mut ip, -1, None).unwrap().unwrap();
    let mut upper = [2i32, 0, 0, 0, 0, 0];
    let mut lower = [1i32, 0, 0, 0, 0, 0];
    let built = crate::element::array_set_slice(
        mcx, &empty, 1, &mut upper, &mut lower, &all, &all, &src, -1, 4, true, b'i',
    )
    .unwrap();
    assert_eq!(as_str(&array_out(mcx, &built, &m, &mut op).unwrap()), "{7,8}");
    let nope = [false, false, false, false, false, false];
    let mut upper = [2i32, 0, 0, 0, 0, 0];
    let mut lower = [1i32, 0, 0, 0, 0, 0];
    let err = crate::element::array_set_slice(
        mcx, &empty, 1, &mut upper, &mut lower, &all, &nope, &src, -1, 4, true, b'i',
    )
    .unwrap_err();
    assert!(err.message().contains("must provide both boundaries"));

    // Source too small.
    let mut upper = [4i32, 0, 0, 0, 0, 0];
    let mut lower = [1i32, 0, 0, 0, 0, 0];
    let err = crate::element::array_set_slice(
        mcx, &img, 1, &mut upper, &mut lower, &one, &one, &src, -1, 4, true, b'i',
    )
    .unwrap_err();
    assert!(err.message().contains("source array too small"));
}

#[test]
fn slice_set_multidim_insert() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let m = meta_int4();
    let mut ip = int4_in();
    let mut op = int4_out();
    let img = array_in(mcx, "{{1,2,3},{4,5,6},{7,8,9}}", &m, &mut ip, -1, None)
        .unwrap()
        .unwrap();
    let two = [true, true, false, false, false, false];

    let src = array_in(mcx, "{{50,60},{80,90}}", &m, &mut ip, -1, None).unwrap().unwrap();
    let mut upper = [3i32, 3, 0, 0, 0, 0];
    let mut lower = [2i32, 2, 0, 0, 0, 0];
    let set = crate::element::array_set_slice(
        mcx, &img, 2, &mut upper, &mut lower, &two, &two, &src, -1, 4, true, b'i',
    )
    .unwrap();
    assert_eq!(
        as_str(&array_out(mcx, &set, &m, &mut op).unwrap()),
        "{{1,2,3},{4,50,60},{7,80,90}}"
    );

    // NULLs riding through the multidim insert path.
    let imgn = array_in(mcx, "{{1,NULL},{3,4}}", &m, &mut ip, -1, None).unwrap().unwrap();
    let src = array_in(mcx, "{NULL}", &m, &mut ip, -1, None).unwrap().unwrap();
    let mut upper = [2i32, 1, 0, 0, 0, 0];
    let mut lower = [2i32, 1, 0, 0, 0, 0];
    let set = crate::element::array_set_slice(
        mcx, &imgn, 2, &mut upper, &mut lower, &two, &two, &src, -1, 4, true, b'i',
    )
    .unwrap();
    assert_eq!(
        as_str(&array_out(mcx, &set, &m, &mut op).unwrap()),
        "{{1,NULL},{NULL,4}}"
    );
}

mod expanded {
    use super::*;
    use crate::expanded::{
        datum_get_expanded_array, datum_get_expanded_array_x, deconstruct_expanded_array,
        expand_array, ArrayMetaState, EA_MAGIC,
    };
    use ::datum::expandeddatum::{
        datum_get_eohp, datum_is_external_expanded, datum_is_external_expanded_rw,
        eoh_flatten_into, eoh_get_flat_size, make_expanded_object_read_only_internal,
    };

    fn int4_meta() -> ArrayMetaState {
        ArrayMetaState { element_type: INT4OID, typlen: 4, typbyval: true, typalign: b'i' }
    }

    fn int4_array<'m>(mcx: Mcx<'m>, vals: &[i32], nulls: Option<&[bool]>) -> ::mcx::PgVec<'m, u8> {
        let elems: std::vec::Vec<Datum> = vals.iter().map(|v| Datum::from_i32(*v)).collect();
        construct_md_array(
            mcx, &elems, nulls, 1, &[vals.len() as i32], &[1], INT4OID, 4, true, b'i',
        )
        .unwrap()
    }

    #[test]
    fn expand_flat_and_flatten_round_trip() {
        let parent = MemoryContext::new("t");
        let img = int4_array(parent.mcx(), &[7, 8, 9], None);
        let mut meta = int4_meta();
        let d = expand_array(Datum::from_usize(img.as_ptr() as usize), &parent, Some(&mut meta))
            .unwrap();
        unsafe {
            assert!(datum_is_external_expanded_rw(d));
            let eah = &*(datum_get_eohp(d) as *const crate::expanded::ExpandedArrayHeader);
            assert_eq!(eah.ea_magic, EA_MAGIC);
            assert_eq!(eah.ndims, 1);
            assert_eq!(eah.dims[0], 3);
            assert_eq!(eah.lbound[0], 1);
            assert_eq!(eah.element_type, INT4OID);
            assert_eq!((eah.typlen, eah.typbyval, eah.typalign), (4, true, b'i'));
            assert_eq!(eah.fvalue().unwrap(), img.as_slice());

            let hdr = datum_get_eohp(d);
            let n = eoh_get_flat_size(hdr);
            assert_eq!(n, img.len());
            let mut out = std::vec![0u8; n];
            eoh_flatten_into(hdr, out.as_mut_ptr(), n);
            assert_eq!(out.as_slice(), img.as_slice());

            let ro = make_expanded_object_read_only_internal(d);
            assert!(datum_is_external_expanded(ro));
            assert!(!datum_is_external_expanded_rw(ro));
        }
    }

    #[test]
    fn deconstruct_and_reexpand_byval() {
        let parent = MemoryContext::new("t");
        let img = int4_array(parent.mcx(), &[1, 2, 3, 4], None);
        let d = expand_array(
            Datum::from_usize(img.as_ptr() as usize),
            &parent,
            Some(&mut int4_meta()),
        )
        .unwrap();
        unsafe {
            {
                let eah = &mut *(datum_get_eohp(d) as *mut crate::expanded::ExpandedArrayHeader);
                assert!(eah.dvalues().is_none());
                deconstruct_expanded_array(eah).unwrap();
                let (vals, nulls) = eah.dvalues().unwrap();
                assert!(nulls.is_none());
                assert_eq!(
                    vals.iter().map(|v| v.as_i32()).collect::<std::vec::Vec<_>>(),
                    [1, 2, 3, 4]
                );
                assert_eq!(eah.nelems, 4);
            }

            // copy_byval path: source is expanded with a Datum-array representation.
            let mut meta = ArrayMetaState::invalid();
            let d2 = expand_array(d, &parent, Some(&mut meta)).unwrap();
            assert_eq!(meta.element_type, INT4OID);
            {
                let eah2 = &*(datum_get_eohp(d2) as *const crate::expanded::ExpandedArrayHeader);
                assert!(eah2.fvalue().is_none());
                let (vals2, _) = eah2.dvalues().unwrap();
                assert_eq!(
                    vals2.iter().map(|v| v.as_i32()).collect::<std::vec::Vec<_>>(),
                    [1, 2, 3, 4]
                );
            }

            // dvalues-only flatten reproduces the original image.
            let hdr2 = datum_get_eohp(d2);
            let n = eoh_get_flat_size(hdr2);
            assert_eq!(n, img.len());
            let mut out = std::vec![0u8; n];
            eoh_flatten_into(hdr2, out.as_mut_ptr(), n);
            assert_eq!(out.as_slice(), img.as_slice());
        }
    }

    #[test]
    fn with_nulls_round_trip() {
        let parent = MemoryContext::new("t");
        let img = int4_array(parent.mcx(), &[5, 0, 6], Some(&[false, true, false]));
        let d = expand_array(
            Datum::from_usize(img.as_ptr() as usize),
            &parent,
            Some(&mut int4_meta()),
        )
        .unwrap();
        unsafe {
            {
                let eah = &mut *(datum_get_eohp(d) as *mut crate::expanded::ExpandedArrayHeader);
                deconstruct_expanded_array(eah).unwrap();
                let (vals, nulls) = eah.dvalues().unwrap();
                assert_eq!(nulls.unwrap(), &[false, true, false]);
                assert_eq!(vals[0].as_i32(), 5);
                assert_eq!(vals[2].as_i32(), 6);
            }

            let d2 = expand_array(d, &parent, None).unwrap();
            let hdr2 = datum_get_eohp(d2);
            let n = eoh_get_flat_size(hdr2);
            assert_eq!(n, img.len());
            let mut out = std::vec![0u8; n];
            eoh_flatten_into(hdr2, out.as_mut_ptr(), n);
            assert_eq!(out.as_slice(), img.as_slice());
        }
    }

    #[test]
    fn datum_get_expanded_array_identity_and_expand() {
        let parent = MemoryContext::new("t");
        let img = int4_array(parent.mcx(), &[42], None);
        unsafe {
            // Flat-source expansion via the metacache variant (the bare
            // variant's catalog lookup needs an installed syscache seam).
            let mut meta = int4_meta();
            let p1 = datum_get_expanded_array_x(
                Datum::from_usize(img.as_ptr() as usize),
                &parent,
                Some(&mut meta),
            )
            .unwrap();
            assert_eq!((*p1).ea_magic, EA_MAGIC);
            let rw = ::datum::expandeddatum::eohp_get_rw_datum(&raw const (*p1).hdr);
            let p2 = datum_get_expanded_array(rw, &parent).unwrap();
            assert_eq!(p1, p2);
            let mut meta = ArrayMetaState::invalid();
            let p3 = datum_get_expanded_array_x(rw, &parent, Some(&mut meta)).unwrap();
            assert_eq!(p1, p3);
            assert_eq!(meta.element_type, INT4OID);
            assert_eq!(meta.typlen, 4);
        }
    }

    #[test]
    fn parent_reset_reclaims_objects() {
        let mut parent = MemoryContext::new("t");
        let img: std::vec::Vec<u8> = {
            let tmp = MemoryContext::new("img");
            let v = int4_array(tmp.mcx(), &[1, 2], None);
            let out = v.as_slice().to_vec();
            drop(v);
            out
        };
        let _ = expand_array(
            Datum::from_usize(img.as_ptr() as usize),
            &parent,
            Some(&mut int4_meta()),
        )
        .unwrap();
        parent.reset();
    }
}

mod ops_tests {
    use super::*;
    use crate::ops::{
        array_cmp_core, array_eq_loop, array_fill_core, contain_core, dims_text,
        fc_width_bucket_array, hash_array_core, replace_core, width_bucket_array_fixed,
        width_bucket_array_float8, width_bucket_array_variable, ElemMeta, FlatIter,
    };
    use ::mcx::PgVec;

    const INT4_META: ElemMeta = ElemMeta { typlen: 4, typbyval: true, typalign: b'i' };

    fn fc_i4eq(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        Ok(Datum::from_bool(fcinfo.arg(0).as_i32() == fcinfo.arg(1).as_i32()))
    }
    fn fc_i4cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        Ok(Datum::from_i32(fcinfo.arg(0).as_i32().cmp(&fcinfo.arg(1).as_i32()) as i32))
    }
    fn fc_i4hash(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        Ok(Datum::from_u64(fcinfo.arg(0).as_i32() as u32 as u64))
    }

    fn finfo(f: ::types_fmgr::PGFunction) -> FmgrInfo {
        FmgrInfo::new(f, 1, 2, true, false)
    }

    fn int4_arr<'m>(mcx: Mcx<'m>, vals: &[Option<i32>]) -> PgVec<'m, u8> {
        int4_arr_md(mcx, vals, 1, &[vals.len() as i32], &[1])
    }

    fn int4_arr_md<'m>(
        mcx: Mcx<'m>,
        vals: &[Option<i32>],
        ndims: i32,
        dims: &[i32],
        lbs: &[i32],
    ) -> PgVec<'m, u8> {
        let elems: std::vec::Vec<Datum> =
            vals.iter().map(|v| Datum::from_i32(v.unwrap_or(0))).collect();
        let nulls: std::vec::Vec<bool> = vals.iter().map(|v| v.is_none()).collect();
        construct_md_array(mcx, &elems, Some(&nulls), ndims, dims, lbs, INT4OID, 4, true, b'i')
            .unwrap()
    }

    fn int4_arr_vals(img: &[u8]) -> std::vec::Vec<Option<i32>> {
        let (ndim, dims, _lbs) = crate::foundation::read_dims_lbounds(img);
        let n = ::arrayutils::array_get_n_items(ndim, &dims).unwrap();
        let mut it = FlatIter::new(img);
        (0..n)
            .map(|_| {
                let (d, isnull) = it.next(4, true, b'i');
                if isnull { None } else { Some(d.as_i32()) }
            })
            .collect()
    }

    #[test]
    fn eq_and_cmp_cores() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let a = int4_arr(mcx, &[Some(1), None, Some(3)]);
        let b = int4_arr(mcx, &[Some(1), None, Some(3)]);
        let c = int4_arr(mcx, &[Some(1), Some(2), Some(3)]);
        let mut eq = finfo(fc_i4eq);
        assert!(array_eq_loop(mcx, &a, &b, 0, INT4_META, &mut eq).unwrap());
        assert!(!array_eq_loop(mcx, &a, &c, 0, INT4_META, &mut eq).unwrap());

        let mut cmp = finfo(fc_i4cmp);
        assert_eq!(array_cmp_core(mcx, &a, &b, 0, INT4_META, &mut cmp).unwrap(), 0);
        // NULL sorts greater than any value
        assert_eq!(array_cmp_core(mcx, &a, &c, 0, INT4_META, &mut cmp).unwrap(), 1);
        let short = int4_arr(mcx, &[Some(1)]);
        assert_eq!(array_cmp_core(mcx, &short, &c, 0, INT4_META, &mut cmp).unwrap(), -1);
        // same data, different lower bounds
        let lb2 = int4_arr_md(mcx, &[Some(1), Some(2), Some(3)], 1, &[3], &[2]);
        assert_eq!(array_cmp_core(mcx, &c, &lb2, 0, INT4_META, &mut cmp).unwrap(), -1);
    }

    #[test]
    fn hash_core_combines_like_c() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let a = int4_arr(mcx, &[None]);
        let mut h = finfo(fc_i4hash);
        assert_eq!(hash_array_core(mcx, &a, 0, INT4_META, &mut h, None).unwrap(), 31);
        let b = int4_arr(mcx, &[Some(7), Some(9)]);
        // ((1*31 + 7) * 31) + 9 = 1187
        assert_eq!(hash_array_core(mcx, &b, 0, INT4_META, &mut h, None).unwrap(), 1187);
        let seeded =
            hash_array_core(mcx, &b, 0, INT4_META, &mut h, Some(Datum::from_i64(0))).unwrap();
        assert_eq!(seeded, 1187);
    }

    #[test]
    fn contain_cores() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let a = int4_arr(mcx, &[Some(1), Some(2)]);
        let b = int4_arr(mcx, &[Some(2), Some(3), Some(1)]);
        let n = int4_arr(mcx, &[Some(1), None]);
        let mut eq = finfo(fc_i4eq);
        // overlap: any-match
        assert!(contain_core(mcx, &a, &b, 0, false, INT4_META, &mut eq).unwrap());
        // contains: a ⊆ b
        assert!(contain_core(mcx, &a, &b, 0, true, INT4_META, &mut eq).unwrap());
        assert!(!contain_core(mcx, &b, &a, 0, true, INT4_META, &mut eq).unwrap());
        // NULL can't match: matchall fails, any-match skips
        assert!(!contain_core(mcx, &n, &b, 0, true, INT4_META, &mut eq).unwrap());
        assert!(contain_core(mcx, &n, &b, 0, false, INT4_META, &mut eq).unwrap());
    }

    #[test]
    fn replace_and_remove_cores() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let mut eq = finfo(fc_i4eq);

        let a = int4_arr(mcx, &[Some(1), Some(2), None, Some(2)]);
        let out = replace_core(
            mcx, a, Datum::from_i32(2), false, Datum::from_i32(9), false, false, 0, INT4_META,
            &mut eq,
        )
        .unwrap();
        assert_eq!(int4_arr_vals(&out), vec![Some(1), Some(9), None, Some(9)]);

        // replace NULLs with a value
        let a = int4_arr(mcx, &[Some(1), None]);
        let out = replace_core(
            mcx, a, Datum::null(), true, Datum::from_i32(0), false, false, 0, INT4_META, &mut eq,
        )
        .unwrap();
        assert_eq!(int4_arr_vals(&out), vec![Some(1), Some(0)]);

        // remove matches and NULL search removes NULLs
        let a = int4_arr(mcx, &[Some(1), Some(2), None, Some(2)]);
        let out = replace_core(
            mcx, a, Datum::from_i32(2), false, Datum::null(), true, true, 0, INT4_META, &mut eq,
        )
        .unwrap();
        assert_eq!(int4_arr_vals(&out), vec![Some(1), None]);

        // unchanged input returned as-is
        let a = int4_arr(mcx, &[Some(1)]);
        let out = replace_core(
            mcx, a, Datum::from_i32(5), false, Datum::null(), true, true, 0, INT4_META, &mut eq,
        )
        .unwrap();
        assert_eq!(int4_arr_vals(&out), vec![Some(1)]);

        // removing everything yields an empty array
        let a = int4_arr(mcx, &[Some(5), Some(5)]);
        let out = replace_core(
            mcx, a, Datum::from_i32(5), false, Datum::null(), true, true, 0, INT4_META, &mut eq,
        )
        .unwrap();
        assert_eq!(crate::foundation::arr_ndim(&out), 0);
    }

    #[test]
    fn fill_core() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let dims = int4_arr(mcx, &[Some(2), Some(3)]);
        let lbs = int4_arr(mcx, &[Some(0), Some(-1)]);
        let out = array_fill_core(
            mcx, &dims, Some(&lbs), Datum::from_i32(7), false, INT4OID, INT4_META,
        )
        .unwrap();
        let (ndim, dv, lv) = crate::foundation::read_dims_lbounds(&out);
        assert_eq!((ndim, dv[0], dv[1], lv[0], lv[1]), (2, 2, 3, 0, -1));
        assert_eq!(int4_arr_vals(&out), vec![Some(7); 6]);
        assert_eq!(dims_text(ndim, &dv, &lv), "[0:1][-1:1]");

        // null fill value → all-null bitmap
        let out =
            array_fill_core(mcx, &dims, None, Datum::null(), true, INT4OID, INT4_META).unwrap();
        assert_eq!(int4_arr_vals(&out), vec![None; 6]);

        // empty dims → empty array
        let nodims = int4_arr(mcx, &[]);
        let out = array_fill_core(
            mcx, &nodims, None, Datum::from_i32(7), false, INT4OID, INT4_META,
        )
        .unwrap();
        assert_eq!(crate::foundation::arr_ndim(&out), 0);

        // error arms
        let md = int4_arr_md(mcx, &[Some(1), Some(2)], 2, &[1, 2], &[1, 1]);
        let e = array_fill_core(mcx, &md, None, Datum::from_i32(7), false, INT4OID, INT4_META)
            .unwrap_err();
        assert_eq!(e.message(), "wrong number of array subscripts");
        let withnull = int4_arr(mcx, &[Some(1), None]);
        let e = array_fill_core(
            mcx, &withnull, None, Datum::from_i32(7), false, INT4OID, INT4_META,
        )
        .unwrap_err();
        assert_eq!(e.message(), "dimension values cannot be null");
        let lbs1 = int4_arr(mcx, &[Some(1)]);
        let e = array_fill_core(
            mcx, &dims, Some(&lbs1), Datum::from_i32(7), false, INT4OID, INT4_META,
        )
        .unwrap_err();
        assert_eq!(e.message(), "wrong number of array subscripts");
    }

    fn install_identity_detoast() {
        crate::tests::detoast_construct::install_test_detoast();
    }

    fn float8_arr<'m>(mcx: Mcx<'m>, vals: &[f64]) -> PgVec<'m, u8> {
        let elems: std::vec::Vec<Datum> = vals.iter().map(|&v| Datum::from_f64(v)).collect();
        construct_array(mcx, &elems, FLOAT8OID, 8, true, TYPALIGN_DOUBLE).unwrap()
    }

    fn text_arr<'m>(mcx: Mcx<'m>, vals: &[&str]) -> PgVec<'m, u8> {
        let elems: std::vec::Vec<Datum> =
            vals.iter().map(|v| build_varlena(mcx, v.as_bytes()).unwrap()).collect();
        construct_array(mcx, &elems, TEXTOID, -1, false, TYPALIGN_INT).unwrap()
    }

    fn fc_text_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
        let a = varlena_payload(fcinfo.arg(0));
        let b = varlena_payload(fcinfo.arg(1));
        Ok(Datum::from_i32(a.cmp(b) as i32))
    }

    #[test]
    fn width_bucket_array_float8_matches_c() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let thresholds = float8_arr(mcx, &[1.0, 5.0, 10.0]);
        assert_eq!(width_bucket_array_float8(Datum::from_f64(0.5), &thresholds, 3), 0);
        assert_eq!(width_bucket_array_float8(Datum::from_f64(1.0), &thresholds, 3), 1);
        assert_eq!(width_bucket_array_float8(Datum::from_f64(7.0), &thresholds, 3), 2);
        assert_eq!(width_bucket_array_float8(Datum::from_f64(11.0), &thresholds, 3), 3);
        // NaN sorts as greater than every threshold, so it needs no search.
        assert_eq!(width_bucket_array_float8(Datum::from_f64(f64::NAN), &thresholds, 3), 3);
    }

    #[test]
    fn width_bucket_array_fixed_int4() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let thresholds = int4_arr(mcx, &[Some(1), Some(5), Some(10)]);
        let mut cmp = finfo(fc_i4cmp);
        let mut r = |op: i32| {
            width_bucket_array_fixed(mcx, Datum::from_i32(op), &thresholds, 0, INT4_META, &mut cmp, 3)
                .unwrap()
        };
        assert_eq!(r(0), 0);
        assert_eq!(r(5), 2);
        assert_eq!(r(11), 3);
    }

    #[test]
    fn width_bucket_array_variable_text() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let thresholds = text_arr(mcx, &["b", "m", "t"]);
        let meta = ElemMeta { typlen: -1, typbyval: false, typalign: TYPALIGN_INT };
        let mut cmp = finfo(fc_text_cmp);
        let mut r = |op: &str| {
            let operand = build_varlena(mcx, op.as_bytes()).unwrap();
            width_bucket_array_variable(mcx, operand, &thresholds, 0, meta, &mut cmp, 3).unwrap()
        };
        assert_eq!(r("a"), 0);
        assert_eq!(r("n"), 2);
        assert_eq!(r("z"), 3);
    }

    #[test]
    fn width_bucket_array_top_level_errors_and_dispatch() {
        install_identity_detoast();
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let mut fcinfo = LocalFcinfo::<2>::new(0);
        // SAFETY: mcx outlives the call.
        unsafe { fcinfo.set_result_mcx(mcx) };
        fcinfo.set_arg(0, Datum::from_f64(0.0));

        let md = construct_md_array(
            mcx,
            &[Datum::from_f64(1.0); 4],
            None,
            2,
            &[2, 2],
            &[1, 1],
            FLOAT8OID,
            8,
            true,
            TYPALIGN_DOUBLE,
        )
        .unwrap();
        fcinfo.set_arg(1, Datum::from_usize(md.as_ptr() as usize));
        let e = fc_width_bucket_array(None, &mut fcinfo).unwrap_err();
        assert_eq!(e.message(), "thresholds must be one-dimensional array");

        let withnull = construct_md_array(
            mcx,
            &[Datum::from_f64(1.0), Datum::null()],
            Some(&[false, true]),
            1,
            &[2],
            &[1],
            FLOAT8OID,
            8,
            true,
            TYPALIGN_DOUBLE,
        )
        .unwrap();
        fcinfo.set_arg(1, Datum::from_usize(withnull.as_ptr() as usize));
        let e = fc_width_bucket_array(None, &mut fcinfo).unwrap_err();
        assert_eq!(e.message(), "thresholds array must not contain NULLs");

        let thresholds = float8_arr(mcx, &[1.0, 5.0, 10.0]);
        fcinfo.set_arg(0, Datum::from_f64(7.0));
        fcinfo.set_arg(1, Datum::from_usize(thresholds.as_ptr() as usize));
        let d = fc_width_bucket_array(None, &mut fcinfo).unwrap();
        assert_eq!(d.as_i32(), 2);
    }
}

mod agg_serial {
    use super::*;
    use crate::build::{
        array_agg_combine_append, array_agg_combine_clone, array_agg_deserialize_state,
        array_agg_serialize_state,
    };

    fn text_send() -> FmgrInfo { FmgrInfo::new(fc_mytextsend, 48, 1, true, false) }
    fn text_recv() -> FmgrInfo { FmgrInfo::new(fc_mytextrecv, 49, 1, true, false) }

    fn int4_state<'m>(mcx: Mcx<'m>, elems: &[Option<i32>]) -> ArrayBuildState<'m> {
        let mut st = ArrayBuildState::new(mcx, INT4OID, false).unwrap();
        st.typlen = 4;
        st.typbyval = true;
        st.typalign = b'i';
        let mut out = Some(st);
        for e in elems {
            let (d, isnull) = match e {
                Some(v) => (Datum::from_i32(*v), false),
                None => (Datum::null(), true),
            };
            out = Some(accum_array_result(mcx, out, d, isnull, INT4OID).unwrap());
        }
        out.unwrap()
    }

    fn text_state<'m>(mcx: Mcx<'m>, elems: &[Option<&str>]) -> ArrayBuildState<'m> {
        let mut st = ArrayBuildState::new(mcx, TEXTOID, false).unwrap();
        st.typlen = -1;
        st.typbyval = false;
        st.typalign = b'i';
        let mut out = Some(st);
        for e in elems {
            let (d, isnull) = match e {
                Some(s) => (build_varlena(mcx, s.as_bytes()).unwrap(), false),
                None => (Datum::null(), true),
            };
            out = Some(accum_array_result(mcx, out, d, isnull, TEXTOID).unwrap());
        }
        out.unwrap()
    }

    fn int4_result(mcx: Mcx<'_>, st: &ArrayBuildState<'_>) -> std::vec::Vec<Option<i32>> {
        let img = make_array_result(mcx, st).unwrap();
        let (elems, nulls) = deconstruct_array(mcx, &img, 4, true, b'i', true).unwrap();
        elems
            .iter()
            .zip(nulls.iter())
            .map(|(d, &n)| if n { None } else { Some(d.as_i32()) })
            .collect()
    }

    // Hand-derived from the C wire layout: elemtype(i32 BE), nelems(i64 BE),
    // typlen(i16 BE), typbyval, typalign, dnulls raw, byval Datums raw.
    #[test]
    fn serialize_golden_int4() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let st = int4_state(mcx, &[Some(1), None, Some(2)]);
        let out = array_agg_serialize_state(mcx, &st, None).unwrap();
        let mut expected: std::vec::Vec<u8> = std::vec::Vec::new();
        expected.extend_from_slice(&23u32.to_be_bytes());
        expected.extend_from_slice(&3i64.to_be_bytes());
        expected.extend_from_slice(&4i16.to_be_bytes());
        expected.push(1);
        expected.push(b'i');
        expected.extend_from_slice(&[0, 1, 0]);
        expected.extend_from_slice(&1u64.to_ne_bytes());
        expected.extend_from_slice(&0u64.to_ne_bytes());
        expected.extend_from_slice(&2u64.to_ne_bytes());
        assert_eq!(out.data(), &expected[..]);
    }

    #[test]
    fn roundtrip_int4_with_nulls() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let st = int4_state(mcx, &[Some(7), None, Some(-1), Some(0)]);
        let img = array_agg_serialize_state(mcx, &st, None).unwrap();
        let back = array_agg_deserialize_state(mcx, img.data(), None).unwrap();
        assert_eq!(back.element_type, INT4OID);
        assert_eq!(back.nelems, 4);
        assert_eq!((back.typlen, back.typbyval, back.typalign), (4, true, b'i'));
        assert_eq!(int4_result(mcx, &back), vec![Some(7), None, Some(-1), Some(0)]);
    }

    #[test]
    fn roundtrip_text_with_nulls() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let st = text_state(mcx, &[Some("ab"), None, Some(""), Some("hello world")]);
        let mut sp = text_send();
        let img = array_agg_serialize_state(mcx, &st, Some(&mut sp)).unwrap();
        let mut rp = text_recv();
        let back = array_agg_deserialize_state(mcx, img.data(), Some((&mut rp, TEXTOID))).unwrap();
        assert_eq!(back.nelems, 4);
        assert!(!back.typbyval);
        let out = make_array_result(mcx, &back).unwrap();
        let (elems, nulls) = deconstruct_array(mcx, &out, -1, false, b'i', true).unwrap();
        let got: std::vec::Vec<Option<std::string::String>> = elems
            .iter()
            .zip(nulls.iter())
            .map(|(d, &n)| {
                if n {
                    None
                } else {
                    Some(std::string::String::from_utf8(varlena_payload(*d).to_vec()).unwrap())
                }
            })
            .collect();
        assert_eq!(
            got,
            vec![
                Some("ab".to_string()),
                None,
                Some("".to_string()),
                Some("hello world".to_string())
            ]
        );
    }

    #[test]
    fn combine_clone_and_append() {
        let ctx1 = MemoryContext::new_bump("agg");
        let ctx2 = MemoryContext::new_bump("worker");
        let aggmcx = ctx1.mcx();
        let mcx2 = ctx2.mcx();
        let s2 = int4_state(mcx2, &[Some(3), None]);
        // NULL-state1 arm: clone into the agg context.
        let mut s1 = array_agg_combine_clone(aggmcx, &s2).unwrap();
        assert_eq!(int4_result(aggmcx, &s1), vec![Some(3), None]);
        // Append arm.
        let s3 = int4_state(mcx2, &[Some(9)]);
        array_agg_combine_append(&mut s1, &s3).unwrap();
        assert_eq!(s1.nelems, 3);
        assert_eq!(int4_result(aggmcx, &s1), vec![Some(3), None, Some(9)]);
    }

    #[test]
    fn combine_clone_copies_byref_payloads() {
        let ctx1 = MemoryContext::new_bump("agg");
        let aggmcx = ctx1.mcx();
        let cloned = {
            let ctx2 = MemoryContext::new_bump("worker");
            let mcx2 = ctx2.mcx();
            let s2 = text_state(mcx2, &[Some("deep"), None]);
            array_agg_combine_clone(aggmcx, &s2).unwrap()
        };
        // Source context dropped; clone must own its payloads.
        assert_eq!(cloned.nelems, 2);
        assert_eq!(varlena_payload(cloned.dvalues[0]), b"deep");
        assert!(cloned.dnulls[1]);
    }
}

mod bitmap_copy_bounds {
    use crate::element::array_bitmap_copy;

    // A copy ending exactly on the last bit of an exactly-sized bitmap must
    // not read or write the byte past it (C guards the byte-advance reads on
    // items remaining and the tail writeback on a partial byte).
    #[test]
    fn dest_ends_on_final_byte_boundary() {
        let mut dest = vec![0u8; 4];
        array_bitmap_copy(&mut dest, 0, 0, None, 0, 32);
        assert_eq!(dest, vec![0xFF; 4]);

        // Appending the final bit alone (accumArrayResultArr's per-item feed).
        let mut dest = vec![0u8; 4];
        array_bitmap_copy(&mut dest, 0, 0, None, 0, 31);
        array_bitmap_copy(&mut dest, 0, 31, None, 0, 1);
        assert_eq!(dest, vec![0xFF; 4]);
    }

    #[test]
    fn src_ends_on_final_byte_boundary() {
        let src = vec![0b1010_1010u8; 2];
        let mut dest = vec![0u8; 2];
        array_bitmap_copy(&mut dest, 0, 0, Some((&src, 0)), 0, 16);
        assert_eq!(dest, src);
    }

    #[test]
    fn partial_final_byte_still_written() {
        let mut dest = vec![0u8; 2];
        array_bitmap_copy(&mut dest, 0, 0, None, 0, 11);
        assert_eq!(dest, vec![0xFF, 0x07]);

        // Cross-byte src copy at an unaligned dest offset keeps neighbors.
        let src = vec![0b0110_0110u8, 0b0000_0101u8];
        let mut dest = vec![0u8; 2];
        array_bitmap_copy(&mut dest, 0, 3, Some((&src, 0)), 0, 10);
        assert_eq!(dest[0], 0b0011_0000);
        assert_eq!(dest[1], 0b0000_1011);
    }
}

// Hand-built toasted element images proving the construct_md_array detoast
// law (C arrayfuncs.c:3534-3538): an external toast pointer, an inline
// compressed image, and a short-header varlena must each be expanded to a
// plain 4B-header value before being packed into the array — the built image
// must be byte-identical to one built from already-flat elements. The detoast
// seam gets the REAL detoast crate; on-disk pointers resolve against a canned
// in-test toast store keyed on va_valueid.
pub(crate) mod detoast_construct {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    static TOAST_STORE: Mutex<Option<HashMap<u32, std::vec::Vec<u8>>>> = Mutex::new(None);

    pub(crate) fn install_test_detoast() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            ::detoast_seams::detoast_attr::set(::detoast::detoast_attr);
            ::toast_internals_seams::toast_fetch_datum::set(test_toast_fetch);
        });
    }

    fn test_toast_fetch<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
        // On-disk pointer image: 0x01, tag 0x12, va_rawsize i32, va_extinfo
        // u32, va_valueid Oid, va_toastrelid Oid — 18 bytes.
        assert_eq!((attr[0], attr[1], attr.len()), (0x01, 0x12, 18));
        let valueid = u32::from_ne_bytes(attr[10..14].try_into().unwrap());
        let store = TOAST_STORE.lock().unwrap();
        let payload = store
            .as_ref()
            .and_then(|m| m.get(&valueid))
            .expect("test toast store: unknown va_valueid");
        let mut out = vec_with_capacity_in(mcx, payload.len())?;
        out.extend_from_slice(payload);
        Ok(out)
    }

    // 1B short-header image — the shape a small text column value has when
    // read straight out of a heap tuple, so ARRAY[col] sees exactly this.
    fn short(mcx: Mcx<'_>, payload: &[u8]) -> Datum {
        assert!(payload.len() <= 126);
        let total = 1 + payload.len();
        let mut v: PgVec<u8> = vec_with_capacity_in(mcx, total).unwrap();
        v.push(((total as u8) << 1) | 1);
        v.extend_from_slice(payload);
        let p = v.as_ptr();
        core::mem::forget(v);
        Datum::from_usize(p as usize)
    }

    // Inline pglz image (4B_C header + va_tcinfo + compressed bytes).
    fn pglz_img(mcx: Mcx<'_>, payload: &[u8]) -> Datum {
        use core::mem::MaybeUninit;
        let mut dst: std::vec::Vec<MaybeUninit<u8>> =
            std::vec![MaybeUninit::uninit(); pglz::pglz_max_output(payload.len())];
        let clen = pglz::pglz_compress_into(payload, &mut dst, &pglz::PGLZ_STRATEGY_DEFAULT)
            .expect("test payload must compress");
        let total = 8 + clen;
        let mut v: PgVec<u8> = vec_with_capacity_in(mcx, total).unwrap();
        v.extend_from_slice(&(((total as u32) << 2) | 0x02).to_ne_bytes());
        // va_tcinfo: raw payload size | compression method (pglz = 0) in the
        // top bits.
        v.extend_from_slice(&(payload.len() as u32).to_ne_bytes());
        // SAFETY: pglz_compress_into initialized the first clen bytes.
        v.extend_from_slice(unsafe {
            core::slice::from_raw_parts(dst.as_ptr().cast::<u8>(), clen)
        });
        let p = v.as_ptr();
        core::mem::forget(v);
        Datum::from_usize(p as usize)
    }

    // Hand-built ON-DISK external toast pointer whose value lives in the
    // canned store — the exact 18-byte image that dangles in the field bug.
    fn ondisk(mcx: Mcx<'_>, valueid: u32, payload: &[u8]) -> Datum {
        {
            let mut full = std::vec::Vec::with_capacity(4 + payload.len());
            full.extend_from_slice(&::datum::varlena::set_varsize_4b(4 + payload.len()));
            full.extend_from_slice(payload);
            let mut store = TOAST_STORE.lock().unwrap();
            store.get_or_insert_with(HashMap::new).insert(valueid, full);
        }
        let rawsize = (4 + payload.len()) as u32;
        let mut v: PgVec<u8> = vec_with_capacity_in(mcx, 18).unwrap();
        v.push(0x01);
        v.push(0x12); // VARTAG_ONDISK
        v.extend_from_slice(&rawsize.to_ne_bytes()); // va_rawsize
        v.extend_from_slice(&(rawsize - 4).to_ne_bytes()); // va_extinfo
        v.extend_from_slice(&valueid.to_ne_bytes()); // va_valueid
        v.extend_from_slice(&0u32.to_ne_bytes()); // va_toastrelid
        let p = v.as_ptr();
        core::mem::forget(v);
        Datum::from_usize(p as usize)
    }

    #[test]
    fn construct_array_detoasts_every_extended_element_shape() {
        install_test_detoast();
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let a = b"plain element".to_vec();
        let b = b"short header element".to_vec();
        let c: std::vec::Vec<u8> =
            b"compressible ".iter().copied().cycle().take(300).collect();
        let d: std::vec::Vec<u8> =
            b"external payload ".iter().copied().cycle().take(2900).collect();

        let toasted = [
            build_varlena(mcx, &a).unwrap(),
            short(mcx, &b),
            pglz_img(mcx, &c),
            ondisk(mcx, 7001, &d),
        ];
        let flats = [
            build_varlena(mcx, &a).unwrap(),
            build_varlena(mcx, &b).unwrap(),
            build_varlena(mcx, &c).unwrap(),
            build_varlena(mcx, &d).unwrap(),
        ];

        let got = construct_array(mcx, &toasted, TEXTOID, -1, false, TYPALIGN_INT).unwrap();
        let want = construct_array(mcx, &flats, TEXTOID, -1, false, TYPALIGN_INT).unwrap();
        assert_eq!(&got[..], &want[..], "toasted-element build must equal all-flat build");

        // Every element in the image is a plain 4B header now.
        let (elems, _nulls) = deconstruct_array(mcx, &got, -1, false, TYPALIGN_INT, true).unwrap();
        for (i, want_payload) in [&a, &b, &c, &d].into_iter().enumerate() {
            let p = elems[i].as_usize() as *const u8;
            // SAFETY: element datum points into the live array image.
            let img = unsafe { core::slice::from_raw_parts(p, varsize_any(p)) };
            assert_eq!(img[0] & 0x03, 0, "element {i} must be 4B uncompressed");
            assert_eq!(&img[4..], &want_payload[..], "element {i} payload");
        }
    }

    #[test]
    fn construct_md_array_detoasts_with_nulls_multidim() {
        install_test_detoast();
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let c: std::vec::Vec<u8> = b"md compressible ".iter().copied().cycle().take(400).collect();
        let d: std::vec::Vec<u8> = b"md external ".iter().copied().cycle().take(1500).collect();

        let toasted = [
            ondisk(mcx, 7002, &d),
            Datum::null(),
            pglz_img(mcx, &c),
            short(mcx, b"tail"),
        ];
        let flats = [
            build_varlena(mcx, &d).unwrap(),
            Datum::null(),
            build_varlena(mcx, &c).unwrap(),
            build_varlena(mcx, b"tail").unwrap(),
        ];
        let nulls = [false, true, false, false];
        let dims = [2, 2];
        let lbs = [1, 1];

        let got = construct_md_array(
            mcx, &toasted, Some(&nulls), 2, &dims, &lbs, TEXTOID, -1, false, TYPALIGN_INT,
        )
        .unwrap();
        let want = construct_md_array(
            mcx, &flats, Some(&nulls), 2, &dims, &lbs, TEXTOID, -1, false, TYPALIGN_INT,
        )
        .unwrap();
        assert_eq!(&got[..], &want[..], "toasted md build must equal all-flat md build");
    }
}

#[test]
fn array_nulls_guc_governs_unquoted_null() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    // Default (on): unquoted NULL is a null element; quoted stays literal.
    assert_eq!(rt_text(mcx, r#"{NULL,"NULL"}"#), r#"{NULL,"NULL"}"#);
    // array_nulls=off (pre-8.2 compat): unquoted NULL is the literal string
    // (ReadArrayToken's Array_nulls arm, arrayfuncs.c) — array_out then
    // quotes it like any other NULL-spelled value.
    crate::set_array_nulls(false);
    let out = rt_text(mcx, r#"{NULL,"NULL"}"#);
    crate::set_array_nulls(true);
    assert_eq!(out, r#"{"NULL","NULL"}"#);
}

// ---- C-locale whitespace conformance for array_in / array_out -------------
//
// C's array_in and array_out both decide "is this whitespace?" with
// scanner_isspace (src/backend/parser/scansup.c), whose set is scan.l's
// {space} class == the C-locale isspace set {HT, LF, VT, FF, CR, SP}.  Rust's
// `u8::is_ascii_whitespace` omits VT (0x0b), so any local re-derivation of the
// set silently drops VT at every position below.  Expectations here are the
// EXECUTED output of PostgreSQL 18.4 (Debian, aarch64) over a generated case
// matrix; control characters crossed the wire only as chr(N).  Each row is
// `id, literal, expected`, where expected is `OK:<hex of array_out bytes>` or
// `ERR:<sqlstate>`.  The NBSP (U+00A0) and NEL (U+0085) rows are negative
// controls: they are whitespace to `char::is_whitespace` but not to C.
mod c_locale_whitespace {
    use super::*;

    fn hex(b: &[u8]) -> String {
        let mut s = String::new();
        for x in b {
            s.push_str(&format!("{x:02x}"));
        }
        s
    }

    fn run_in(mcx: Mcx<'_>, lit: &str, int4: bool) -> String {
        let m = if int4 { meta_int4() } else { meta_text() };
        let mut ip = if int4 { int4_in() } else { text_in() };
        match array_in(mcx, lit, &m, &mut ip, -1, None) {
            Err(e) => format!(
                "ERR:{}",
                core::str::from_utf8(&::types_error::unpack_sqlstate(e.sqlstate())).unwrap()
            ),
            Ok(None) => "ERR:soft".to_string(),
            Ok(Some(img)) => {
                let mut op = if int4 { int4_out() } else { text_out() };
                let o = array_out(mcx, &img, &m, &mut op).unwrap();
                format!("OK:{}", hex(&o[..o.len() - 1]))
            }
        }
    }

    // array_out an array whose single element is `elem`, then read the literal
    // back: `OUT:<hex of literal> RT:EQ` when the element survives verbatim.
    fn run_out(mcx: Mcx<'_>, elem: &str) -> String {
        let m = meta_text();
        let d = build_varlena(mcx, elem.as_bytes()).unwrap();
        let img = construct_array(mcx, &[d], ::types_core::TEXTOID, -1, false, b'i').unwrap();
        let mut op = text_out();
        let lit_c = array_out(mcx, &img, &m, &mut op).unwrap();
        let lit_b = &lit_c[..lit_c.len() - 1];
        let outhex = hex(lit_b);
        let lit = core::str::from_utf8(lit_b).unwrap();
        let mut ip = text_in();
        match array_in(mcx, lit, &m, &mut ip, -1, None) {
            Err(e) => format!(
                "OUT:{outhex} RT:ERR:{}",
                core::str::from_utf8(&::types_error::unpack_sqlstate(e.sqlstate())).unwrap()
            ),
            Ok(None) => format!("OUT:{outhex} RT:ERR:soft"),
            Ok(Some(back)) => {
                let (ds, ns) =
                    deconstruct_array(mcx, &back, -1, false, b'i', true).unwrap();
                let got: &[u8] = if ds.len() == 1 && !ns[0] { varlena_payload(ds[0]) } else { b"" };
                if got == elem.as_bytes() {
                    format!("OUT:{outhex} RT:EQ")
                } else {
                    format!("OUT:{outhex} RT:NE:{}", hex(got))
                }
            }
        }
    }

    #[test]
    fn array_in_text_whitespace_matrix_matches_c() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        #[rustfmt::skip]
        let cases: &[(&str, &str, &str)] = &[
            ("t_lead_SP", " {a}", "OK:7b617d"),
            ("t_lead_HT", "\u{9}{a}", "OK:7b617d"),
            ("t_lead_LF", "\u{a}{a}", "OK:7b617d"),
            ("t_lead_VT", "\u{b}{a}", "OK:7b617d"),
            ("t_lead_FF", "\u{c}{a}", "OK:7b617d"),
            ("t_lead_CR", "\u{d}{a}", "OK:7b617d"),
            ("t_lead_NBSP", "\u{a0}{a}", "ERR:22P02"),
            ("t_lead_NEL", "\u{85}{a}", "ERR:22P02"),
            ("t_trail_SP", "{a} ", "OK:7b617d"),
            ("t_trail_HT", "{a}\u{9}", "OK:7b617d"),
            ("t_trail_LF", "{a}\u{a}", "OK:7b617d"),
            ("t_trail_VT", "{a}\u{b}", "OK:7b617d"),
            ("t_trail_FF", "{a}\u{c}", "OK:7b617d"),
            ("t_trail_CR", "{a}\u{d}", "OK:7b617d"),
            ("t_trail_NBSP", "{a}\u{a0}", "ERR:22P02"),
            ("t_trail_NEL", "{a}\u{85}", "ERR:22P02"),
            ("t_after_lbrace_SP", "{ a}", "OK:7b617d"),
            ("t_after_lbrace_HT", "{\u{9}a}", "OK:7b617d"),
            ("t_after_lbrace_LF", "{\u{a}a}", "OK:7b617d"),
            ("t_after_lbrace_VT", "{\u{b}a}", "OK:7b617d"),
            ("t_after_lbrace_FF", "{\u{c}a}", "OK:7b617d"),
            ("t_after_lbrace_CR", "{\u{d}a}", "OK:7b617d"),
            ("t_after_lbrace_NBSP", "{\u{a0}a}", "OK:7bc2a0617d"),
            ("t_after_lbrace_NEL", "{\u{85}a}", "OK:7bc285617d"),
            ("t_before_rbrace_SP", "{a }", "OK:7b617d"),
            ("t_before_rbrace_HT", "{a\u{9}}", "OK:7b617d"),
            ("t_before_rbrace_LF", "{a\u{a}}", "OK:7b617d"),
            ("t_before_rbrace_VT", "{a\u{b}}", "OK:7b617d"),
            ("t_before_rbrace_FF", "{a\u{c}}", "OK:7b617d"),
            ("t_before_rbrace_CR", "{a\u{d}}", "OK:7b617d"),
            ("t_before_rbrace_NBSP", "{a\u{a0}}", "OK:7b61c2a07d"),
            ("t_before_rbrace_NEL", "{a\u{85}}", "OK:7b61c2857d"),
            ("t_around_delim_SP", "{a , b}", "OK:7b612c627d"),
            ("t_around_delim_HT", "{a\u{9},\u{9}b}", "OK:7b612c627d"),
            ("t_around_delim_LF", "{a\u{a},\u{a}b}", "OK:7b612c627d"),
            ("t_around_delim_VT", "{a\u{b},\u{b}b}", "OK:7b612c627d"),
            ("t_around_delim_FF", "{a\u{c},\u{c}b}", "OK:7b612c627d"),
            ("t_around_delim_CR", "{a\u{d},\u{d}b}", "OK:7b612c627d"),
            ("t_around_delim_NBSP", "{a\u{a0},\u{a0}b}", "OK:7b61c2a02cc2a0627d"),
            ("t_around_delim_NEL", "{a\u{85},\u{85}b}", "OK:7b61c2852cc285627d"),
            ("t_in_quotes_SP", "{\"a b\"}", "OK:7b22612062227d"),
            ("t_in_quotes_HT", "{\"a\u{9}b\"}", "OK:7b22610962227d"),
            ("t_in_quotes_LF", "{\"a\u{a}b\"}", "OK:7b22610a62227d"),
            ("t_in_quotes_VT", "{\"a\u{b}b\"}", "OK:7b22610b62227d"),
            ("t_in_quotes_FF", "{\"a\u{c}b\"}", "OK:7b22610c62227d"),
            ("t_in_quotes_CR", "{\"a\u{d}b\"}", "OK:7b22610d62227d"),
            ("t_in_quotes_NBSP", "{\"a\u{a0}b\"}", "OK:7b61c2a0627d"),
            ("t_in_quotes_NEL", "{\"a\u{85}b\"}", "OK:7b61c285627d"),
            ("t_after_quote_SP", "{\"a\" ,b}", "OK:7b612c627d"),
            ("t_after_quote_HT", "{\"a\"\u{9},b}", "OK:7b612c627d"),
            ("t_after_quote_LF", "{\"a\"\u{a},b}", "OK:7b612c627d"),
            ("t_after_quote_VT", "{\"a\"\u{b},b}", "OK:7b612c627d"),
            ("t_after_quote_FF", "{\"a\"\u{c},b}", "OK:7b612c627d"),
            ("t_after_quote_CR", "{\"a\"\u{d},b}", "OK:7b612c627d"),
            ("t_after_quote_NBSP", "{\"a\"\u{a0},b}", "ERR:22P02"),
            ("t_after_quote_NEL", "{\"a\"\u{85},b}", "ERR:22P02"),
            ("t_quoted_only_SP", "{\" \"}", "OK:7b2220227d"),
            ("t_quoted_only_HT", "{\"\u{9}\"}", "OK:7b2209227d"),
            ("t_quoted_only_LF", "{\"\u{a}\"}", "OK:7b220a227d"),
            ("t_quoted_only_VT", "{\"\u{b}\"}", "OK:7b220b227d"),
            ("t_quoted_only_FF", "{\"\u{c}\"}", "OK:7b220c227d"),
            ("t_quoted_only_CR", "{\"\u{d}\"}", "OK:7b220d227d"),
            ("t_quoted_only_NBSP", "{\"\u{a0}\"}", "OK:7bc2a07d"),
            ("t_quoted_only_NEL", "{\"\u{85}\"}", "OK:7bc2857d"),
            ("t_brace_only_SP", "{ }", "OK:7b7d"),
            ("t_brace_only_HT", "{\u{9}}", "OK:7b7d"),
            ("t_brace_only_LF", "{\u{a}}", "OK:7b7d"),
            ("t_brace_only_VT", "{\u{b}}", "OK:7b7d"),
            ("t_brace_only_FF", "{\u{c}}", "OK:7b7d"),
            ("t_brace_only_CR", "{\u{d}}", "OK:7b7d"),
            ("t_brace_only_NBSP", "{\u{a0}}", "OK:7bc2a07d"),
            ("t_brace_only_NEL", "{\u{85}}", "OK:7bc2857d"),
            ("t_empty_elem_SP", "{a, ,b}", "ERR:22P02"),
            ("t_empty_elem_HT", "{a,\u{9},b}", "ERR:22P02"),
            ("t_empty_elem_LF", "{a,\u{a},b}", "ERR:22P02"),
            ("t_empty_elem_VT", "{a,\u{b},b}", "ERR:22P02"),
            ("t_empty_elem_FF", "{a,\u{c},b}", "ERR:22P02"),
            ("t_empty_elem_CR", "{a,\u{d},b}", "ERR:22P02"),
            ("t_empty_elem_NBSP", "{a,\u{a0},b}", "OK:7b612cc2a02c627d"),
            ("t_empty_elem_NEL", "{a,\u{85},b}", "OK:7b612cc2852c627d"),
            ("t_esc_mid_SP", "{a\\ b}", "OK:7b22612062227d"),
            ("t_esc_mid_HT", "{a\\\u{9}b}", "OK:7b22610962227d"),
            ("t_esc_mid_LF", "{a\\\u{a}b}", "OK:7b22610a62227d"),
            ("t_esc_mid_VT", "{a\\\u{b}b}", "OK:7b22610b62227d"),
            ("t_esc_mid_FF", "{a\\\u{c}b}", "OK:7b22610c62227d"),
            ("t_esc_mid_CR", "{a\\\u{d}b}", "OK:7b22610d62227d"),
            ("t_esc_mid_NBSP", "{a\\\u{a0}b}", "OK:7b61c2a0627d"),
            ("t_esc_mid_NEL", "{a\\\u{85}b}", "OK:7b61c285627d"),
            ("t_esc_trail_SP", "{a\\ }", "OK:7b226120227d"),
            ("t_esc_trail_HT", "{a\\\u{9}}", "OK:7b226109227d"),
            ("t_esc_trail_LF", "{a\\\u{a}}", "OK:7b22610a227d"),
            ("t_esc_trail_VT", "{a\\\u{b}}", "OK:7b22610b227d"),
            ("t_esc_trail_FF", "{a\\\u{c}}", "OK:7b22610c227d"),
            ("t_esc_trail_CR", "{a\\\u{d}}", "OK:7b22610d227d"),
            ("t_esc_trail_NBSP", "{a\\\u{a0}}", "OK:7b61c2a07d"),
            ("t_esc_trail_NEL", "{a\\\u{85}}", "OK:7b61c2857d"),
            ("t_esc_in_quotes_SP", "{\"a\\ b\"}", "OK:7b22612062227d"),
            ("t_esc_in_quotes_HT", "{\"a\\\u{9}b\"}", "OK:7b22610962227d"),
            ("t_esc_in_quotes_LF", "{\"a\\\u{a}b\"}", "OK:7b22610a62227d"),
            ("t_esc_in_quotes_VT", "{\"a\\\u{b}b\"}", "OK:7b22610b62227d"),
            ("t_esc_in_quotes_FF", "{\"a\\\u{c}b\"}", "OK:7b22610c62227d"),
            ("t_esc_in_quotes_CR", "{\"a\\\u{d}b\"}", "OK:7b22610d62227d"),
            ("t_esc_in_quotes_NBSP", "{\"a\\\u{a0}b\"}", "OK:7b61c2a0627d"),
            ("t_esc_in_quotes_NEL", "{\"a\\\u{85}b\"}", "OK:7b61c285627d"),
            ("t_nested_SP", "{{a } ,{b}}", "OK:7b7b617d2c7b627d7d"),
            ("t_nested_HT", "{{a\u{9}}\u{9},{b}}", "OK:7b7b617d2c7b627d7d"),
            ("t_nested_LF", "{{a\u{a}}\u{a},{b}}", "OK:7b7b617d2c7b627d7d"),
            ("t_nested_VT", "{{a\u{b}}\u{b},{b}}", "OK:7b7b617d2c7b627d7d"),
            ("t_nested_FF", "{{a\u{c}}\u{c},{b}}", "OK:7b7b617d2c7b627d7d"),
            ("t_nested_CR", "{{a\u{d}}\u{d},{b}}", "OK:7b7b617d2c7b627d7d"),
            ("t_nested_NBSP", "{{a\u{a0}}\u{a0},{b}}", "ERR:22P02"),
            ("t_nested_NEL", "{{a\u{85}}\u{85},{b}}", "ERR:22P02"),
            ("t_null_ws_SP", "{NULL }", "OK:7b4e554c4c7d"),
            ("t_null_ws_HT", "{NULL\u{9}}", "OK:7b4e554c4c7d"),
            ("t_null_ws_LF", "{NULL\u{a}}", "OK:7b4e554c4c7d"),
            ("t_null_ws_VT", "{NULL\u{b}}", "OK:7b4e554c4c7d"),
            ("t_null_ws_FF", "{NULL\u{c}}", "OK:7b4e554c4c7d"),
            ("t_null_ws_CR", "{NULL\u{d}}", "OK:7b4e554c4c7d"),
            ("t_null_ws_NBSP", "{NULL\u{a0}}", "OK:7b4e554c4cc2a07d"),
            ("t_null_ws_NEL", "{NULL\u{85}}", "OK:7b4e554c4cc2857d"),
            ("t_null_ws_lead_SP", "{ NULL}", "OK:7b4e554c4c7d"),
            ("t_null_ws_lead_HT", "{\u{9}NULL}", "OK:7b4e554c4c7d"),
            ("t_null_ws_lead_LF", "{\u{a}NULL}", "OK:7b4e554c4c7d"),
            ("t_null_ws_lead_VT", "{\u{b}NULL}", "OK:7b4e554c4c7d"),
            ("t_null_ws_lead_FF", "{\u{c}NULL}", "OK:7b4e554c4c7d"),
            ("t_null_ws_lead_CR", "{\u{d}NULL}", "OK:7b4e554c4c7d"),
            ("t_null_ws_lead_NBSP", "{\u{a0}NULL}", "OK:7bc2a04e554c4c7d"),
            ("t_null_ws_lead_NEL", "{\u{85}NULL}", "OK:7bc2854e554c4c7d"),
            ("t_elem_all_ws_SP", "{a, }", "ERR:22P02"),
            ("t_elem_all_ws_HT", "{a,\u{9}}", "ERR:22P02"),
            ("t_elem_all_ws_LF", "{a,\u{a}}", "ERR:22P02"),
            ("t_elem_all_ws_VT", "{a,\u{b}}", "ERR:22P02"),
            ("t_elem_all_ws_FF", "{a,\u{c}}", "ERR:22P02"),
            ("t_elem_all_ws_CR", "{a,\u{d}}", "ERR:22P02"),
            ("t_elem_all_ws_NBSP", "{a,\u{a0}}", "OK:7b612cc2a07d"),
            ("t_elem_all_ws_NEL", "{a,\u{85}}", "OK:7b612cc2857d"),
            ("t_dim_lead_SP", " [1:2]={a,b}", "OK:7b612c627d"),
            ("t_dim_lead_HT", "\u{9}[1:2]={a,b}", "OK:7b612c627d"),
            ("t_dim_lead_LF", "\u{a}[1:2]={a,b}", "OK:7b612c627d"),
            ("t_dim_lead_VT", "\u{b}[1:2]={a,b}", "OK:7b612c627d"),
            ("t_dim_lead_FF", "\u{c}[1:2]={a,b}", "OK:7b612c627d"),
            ("t_dim_lead_CR", "\u{d}[1:2]={a,b}", "OK:7b612c627d"),
            ("t_dim_lead_NBSP", "\u{a0}[1:2]={a,b}", "ERR:22P02"),
            ("t_dim_lead_NEL", "\u{85}[1:2]={a,b}", "ERR:22P02"),
            ("t_dim_before_eq_SP", "[1:2] ={a,b}", "OK:7b612c627d"),
            ("t_dim_before_eq_HT", "[1:2]\u{9}={a,b}", "OK:7b612c627d"),
            ("t_dim_before_eq_LF", "[1:2]\u{a}={a,b}", "OK:7b612c627d"),
            ("t_dim_before_eq_VT", "[1:2]\u{b}={a,b}", "OK:7b612c627d"),
            ("t_dim_before_eq_FF", "[1:2]\u{c}={a,b}", "OK:7b612c627d"),
            ("t_dim_before_eq_CR", "[1:2]\u{d}={a,b}", "OK:7b612c627d"),
            ("t_dim_before_eq_NBSP", "[1:2]\u{a0}={a,b}", "ERR:22P02"),
            ("t_dim_before_eq_NEL", "[1:2]\u{85}={a,b}", "ERR:22P02"),
            ("t_dim_after_eq_SP", "[1:2]= {a,b}", "OK:7b612c627d"),
            ("t_dim_after_eq_HT", "[1:2]=\u{9}{a,b}", "OK:7b612c627d"),
            ("t_dim_after_eq_LF", "[1:2]=\u{a}{a,b}", "OK:7b612c627d"),
            ("t_dim_after_eq_VT", "[1:2]=\u{b}{a,b}", "OK:7b612c627d"),
            ("t_dim_after_eq_FF", "[1:2]=\u{c}{a,b}", "OK:7b612c627d"),
            ("t_dim_after_eq_CR", "[1:2]=\u{d}{a,b}", "OK:7b612c627d"),
            ("t_dim_after_eq_NBSP", "[1:2]=\u{a0}{a,b}", "ERR:22P02"),
            ("t_dim_after_eq_NEL", "[1:2]=\u{85}{a,b}", "ERR:22P02"),
            ("t_dim_in_colon_SP", "[1 :2]={a,b}", "ERR:22P02"),
            ("t_dim_in_colon_HT", "[1\u{9}:2]={a,b}", "ERR:22P02"),
            ("t_dim_in_colon_LF", "[1\u{a}:2]={a,b}", "ERR:22P02"),
            ("t_dim_in_colon_VT", "[1\u{b}:2]={a,b}", "ERR:22P02"),
            ("t_dim_in_colon_FF", "[1\u{c}:2]={a,b}", "ERR:22P02"),
            ("t_dim_in_colon_CR", "[1\u{d}:2]={a,b}", "ERR:22P02"),
            ("t_dim_in_colon_NBSP", "[1\u{a0}:2]={a,b}", "ERR:22P02"),
            ("t_dim_in_colon_NEL", "[1\u{85}:2]={a,b}", "ERR:22P02"),
            ("t_dim_after_lbrk_SP", "[ 1:2]={a,b}", "ERR:22P02"),
            ("t_dim_after_lbrk_HT", "[\u{9}1:2]={a,b}", "ERR:22P02"),
            ("t_dim_after_lbrk_LF", "[\u{a}1:2]={a,b}", "ERR:22P02"),
            ("t_dim_after_lbrk_VT", "[\u{b}1:2]={a,b}", "ERR:22P02"),
            ("t_dim_after_lbrk_FF", "[\u{c}1:2]={a,b}", "ERR:22P02"),
            ("t_dim_after_lbrk_CR", "[\u{d}1:2]={a,b}", "ERR:22P02"),
            ("t_dim_after_lbrk_NBSP", "[\u{a0}1:2]={a,b}", "ERR:22P02"),
            ("t_dim_after_lbrk_NEL", "[\u{85}1:2]={a,b}", "ERR:22P02"),
            ("t_dim_before_rbrk_SP", "[1:2 ]={a,b}", "ERR:22P02"),
            ("t_dim_before_rbrk_HT", "[1:2\u{9}]={a,b}", "ERR:22P02"),
            ("t_dim_before_rbrk_LF", "[1:2\u{a}]={a,b}", "ERR:22P02"),
            ("t_dim_before_rbrk_VT", "[1:2\u{b}]={a,b}", "ERR:22P02"),
            ("t_dim_before_rbrk_FF", "[1:2\u{c}]={a,b}", "ERR:22P02"),
            ("t_dim_before_rbrk_CR", "[1:2\u{d}]={a,b}", "ERR:22P02"),
            ("t_dim_before_rbrk_NBSP", "[1:2\u{a0}]={a,b}", "ERR:22P02"),
            ("t_dim_before_rbrk_NEL", "[1:2\u{85}]={a,b}", "ERR:22P02"),
            ("t_dim_between_SP", "[1:1] [1:2]={{a,b}}", "OK:7b7b612c627d7d"),
            ("t_dim_between_HT", "[1:1]\u{9}[1:2]={{a,b}}", "OK:7b7b612c627d7d"),
            ("t_dim_between_LF", "[1:1]\u{a}[1:2]={{a,b}}", "OK:7b7b612c627d7d"),
            ("t_dim_between_VT", "[1:1]\u{b}[1:2]={{a,b}}", "OK:7b7b612c627d7d"),
            ("t_dim_between_FF", "[1:1]\u{c}[1:2]={{a,b}}", "OK:7b7b612c627d7d"),
            ("t_dim_between_CR", "[1:1]\u{d}[1:2]={{a,b}}", "OK:7b7b612c627d7d"),
            ("t_dim_between_NBSP", "[1:1]\u{a0}[1:2]={{a,b}}", "ERR:22P02"),
            ("t_dim_between_NEL", "[1:1]\u{85}[1:2]={{a,b}}", "ERR:22P02"),
            ("t_dim_both_ends_SP", "[1:2]= {a,b} ", "OK:7b612c627d"),
            ("t_dim_both_ends_HT", "[1:2]=\u{9}{a,b}\u{9}", "OK:7b612c627d"),
            ("t_dim_both_ends_LF", "[1:2]=\u{a}{a,b}\u{a}", "OK:7b612c627d"),
            ("t_dim_both_ends_VT", "[1:2]=\u{b}{a,b}\u{b}", "OK:7b612c627d"),
            ("t_dim_both_ends_FF", "[1:2]=\u{c}{a,b}\u{c}", "OK:7b612c627d"),
            ("t_dim_both_ends_CR", "[1:2]=\u{d}{a,b}\u{d}", "OK:7b612c627d"),
            ("t_dim_both_ends_NBSP", "[1:2]=\u{a0}{a,b}\u{a0}", "ERR:22P02"),
            ("t_dim_both_ends_NEL", "[1:2]=\u{85}{a,b}\u{85}", "ERR:22P02"),
        ];
        let mut bad = Vec::new();
        for (id, lit, want) in cases {
            let got = run_in(mcx, lit, false);
            if got != *want {
                bad.push(format!("{id}: want {want}, got {got}"));
            }
        }
        assert!(bad.is_empty(), "{} cell(s) diverge from PostgreSQL 18.4:\n{}", bad.len(), bad.join("\n"));
    }

    #[test]
    fn array_in_int4_whitespace_matrix_matches_c() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        #[rustfmt::skip]
        let cases: &[(&str, &str, &str)] = &[
            ("i_lead_SP", " {1}", "OK:7b317d"),
            ("i_lead_HT", "\u{9}{1}", "OK:7b317d"),
            ("i_lead_LF", "\u{a}{1}", "OK:7b317d"),
            ("i_lead_VT", "\u{b}{1}", "OK:7b317d"),
            ("i_lead_FF", "\u{c}{1}", "OK:7b317d"),
            ("i_lead_CR", "\u{d}{1}", "OK:7b317d"),
            ("i_lead_NBSP", "\u{a0}{1}", "ERR:22P02"),
            ("i_lead_NEL", "\u{85}{1}", "ERR:22P02"),
            ("i_trail_SP", "{1} ", "OK:7b317d"),
            ("i_trail_HT", "{1}\u{9}", "OK:7b317d"),
            ("i_trail_LF", "{1}\u{a}", "OK:7b317d"),
            ("i_trail_VT", "{1}\u{b}", "OK:7b317d"),
            ("i_trail_FF", "{1}\u{c}", "OK:7b317d"),
            ("i_trail_CR", "{1}\u{d}", "OK:7b317d"),
            ("i_trail_NBSP", "{1}\u{a0}", "ERR:22P02"),
            ("i_trail_NEL", "{1}\u{85}", "ERR:22P02"),
            ("i_before_rbrace_SP", "{1 }", "OK:7b317d"),
            ("i_before_rbrace_HT", "{1\u{9}}", "OK:7b317d"),
            ("i_before_rbrace_LF", "{1\u{a}}", "OK:7b317d"),
            ("i_before_rbrace_VT", "{1\u{b}}", "OK:7b317d"),
            ("i_before_rbrace_FF", "{1\u{c}}", "OK:7b317d"),
            ("i_before_rbrace_CR", "{1\u{d}}", "OK:7b317d"),
            ("i_before_rbrace_NBSP", "{1\u{a0}}", "ERR:22P02"),
            ("i_before_rbrace_NEL", "{1\u{85}}", "ERR:22P02"),
            ("i_around_delim_SP", "{1 , 2}", "OK:7b312c327d"),
            ("i_around_delim_HT", "{1\u{9},\u{9}2}", "OK:7b312c327d"),
            ("i_around_delim_LF", "{1\u{a},\u{a}2}", "OK:7b312c327d"),
            ("i_around_delim_VT", "{1\u{b},\u{b}2}", "OK:7b312c327d"),
            ("i_around_delim_FF", "{1\u{c},\u{c}2}", "OK:7b312c327d"),
            ("i_around_delim_CR", "{1\u{d},\u{d}2}", "OK:7b312c327d"),
            ("i_around_delim_NBSP", "{1\u{a0},\u{a0}2}", "ERR:22P02"),
            ("i_around_delim_NEL", "{1\u{85},\u{85}2}", "ERR:22P02"),
            ("i_in_quotes_SP", "{\"1 \"}", "OK:7b317d"),
            ("i_in_quotes_HT", "{\"1\u{9}\"}", "OK:7b317d"),
            ("i_in_quotes_LF", "{\"1\u{a}\"}", "OK:7b317d"),
            ("i_in_quotes_VT", "{\"1\u{b}\"}", "OK:7b317d"),
            ("i_in_quotes_FF", "{\"1\u{c}\"}", "OK:7b317d"),
            ("i_in_quotes_CR", "{\"1\u{d}\"}", "OK:7b317d"),
            ("i_in_quotes_NBSP", "{\"1\u{a0}\"}", "ERR:22P02"),
            ("i_in_quotes_NEL", "{\"1\u{85}\"}", "ERR:22P02"),
            ("i_quoted_lead_SP", "{\" 1\"}", "OK:7b317d"),
            ("i_quoted_lead_HT", "{\"\u{9}1\"}", "OK:7b317d"),
            ("i_quoted_lead_LF", "{\"\u{a}1\"}", "OK:7b317d"),
            ("i_quoted_lead_VT", "{\"\u{b}1\"}", "OK:7b317d"),
            ("i_quoted_lead_FF", "{\"\u{c}1\"}", "OK:7b317d"),
            ("i_quoted_lead_CR", "{\"\u{d}1\"}", "OK:7b317d"),
            ("i_quoted_lead_NBSP", "{\"\u{a0}1\"}", "ERR:22P02"),
            ("i_quoted_lead_NEL", "{\"\u{85}1\"}", "ERR:22P02"),
        ];
        let mut bad = Vec::new();
        for (id, lit, want) in cases {
            let got = run_in(mcx, lit, true);
            if got != *want {
                bad.push(format!("{id}: want {want}, got {got}"));
            }
        }
        assert!(bad.is_empty(), "{} cell(s) diverge from PostgreSQL 18.4:\n{}", bad.len(), bad.join("\n"));
    }

    #[test]
    fn array_out_quotes_every_c_whitespace_and_round_trips() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        #[rustfmt::skip]
        let cases: &[(&str, &str, &str)] = &[
            ("o_elem_SP", " ", "OUT:7b2220227d RT:EQ"),
            ("o_mid_SP", "a b", "OUT:7b22612062227d RT:EQ"),
            ("o_lead_SP", " a", "OUT:7b222061227d RT:EQ"),
            ("o_trail_SP", "a ", "OUT:7b226120227d RT:EQ"),
            ("o_elem_HT", "\u{9}", "OUT:7b2209227d RT:EQ"),
            ("o_mid_HT", "a\u{9}b", "OUT:7b22610962227d RT:EQ"),
            ("o_lead_HT", "\u{9}a", "OUT:7b220961227d RT:EQ"),
            ("o_trail_HT", "a\u{9}", "OUT:7b226109227d RT:EQ"),
            ("o_elem_LF", "\u{a}", "OUT:7b220a227d RT:EQ"),
            ("o_mid_LF", "a\u{a}b", "OUT:7b22610a62227d RT:EQ"),
            ("o_lead_LF", "\u{a}a", "OUT:7b220a61227d RT:EQ"),
            ("o_trail_LF", "a\u{a}", "OUT:7b22610a227d RT:EQ"),
            ("o_elem_VT", "\u{b}", "OUT:7b220b227d RT:EQ"),
            ("o_mid_VT", "a\u{b}b", "OUT:7b22610b62227d RT:EQ"),
            ("o_lead_VT", "\u{b}a", "OUT:7b220b61227d RT:EQ"),
            ("o_trail_VT", "a\u{b}", "OUT:7b22610b227d RT:EQ"),
            ("o_elem_FF", "\u{c}", "OUT:7b220c227d RT:EQ"),
            ("o_mid_FF", "a\u{c}b", "OUT:7b22610c62227d RT:EQ"),
            ("o_lead_FF", "\u{c}a", "OUT:7b220c61227d RT:EQ"),
            ("o_trail_FF", "a\u{c}", "OUT:7b22610c227d RT:EQ"),
            ("o_elem_CR", "\u{d}", "OUT:7b220d227d RT:EQ"),
            ("o_mid_CR", "a\u{d}b", "OUT:7b22610d62227d RT:EQ"),
            ("o_lead_CR", "\u{d}a", "OUT:7b220d61227d RT:EQ"),
            ("o_trail_CR", "a\u{d}", "OUT:7b22610d227d RT:EQ"),
            ("o_elem_NBSP", "\u{a0}", "OUT:7bc2a07d RT:EQ"),
            ("o_mid_NBSP", "a\u{a0}b", "OUT:7b61c2a0627d RT:EQ"),
            ("o_lead_NBSP", "\u{a0}a", "OUT:7bc2a0617d RT:EQ"),
            ("o_trail_NBSP", "a\u{a0}", "OUT:7b61c2a07d RT:EQ"),
            ("o_elem_NEL", "\u{85}", "OUT:7bc2857d RT:EQ"),
            ("o_mid_NEL", "a\u{85}b", "OUT:7b61c285627d RT:EQ"),
            ("o_lead_NEL", "\u{85}a", "OUT:7bc285617d RT:EQ"),
            ("o_trail_NEL", "a\u{85}", "OUT:7b61c2857d RT:EQ"),
        ];
        let mut bad = Vec::new();
        for (id, lit, want) in cases {
            let got = run_out(mcx, lit);
            if got != *want {
                bad.push(format!("{id}: want {want}, got {got}"));
            }
        }
        assert!(bad.is_empty(), "{} cell(s) diverge from PostgreSQL 18.4:\n{}", bad.len(), bad.join("\n"));
    }

}

// Malformed array images whose header ndim field is outside 0..=MAXDIM: a
// corrupt page or a crafted binary-format value, unreachable from any array
// pgrust can construct (ArrayCheckBounds caps ndim at MAXDIM). Every one of
// these used to PANIC inside read_dims_lbounds, which looped `0..ndim` before
// the wrappers' sanity check ever ran; C returns SQL NULL (or, for
// array_cardinality, a value / an error — it has no sanity check at all).
//
// Expected values are the EXECUTED output of the vendored PG 18 bodies
// (utils/adt/arrayfuncs.c + arrayutils.c @ 62d6c7d3df, run as a standalone
// oracle over byte-identical images):
//
//   ndim         ndims  lower/upper/length  dims   cardinality
//   -1           NULL   NULL                NULL   0
//   INT_MIN      NULL   NULL                NULL   0
//    0           NULL   NULL                NULL   0
//    1              1   1 / 2 / 2           [1:2]  2
//    6              6   1 / 2 / 2           [1:2]… 6
//    7           NULL   NULL                NULL   6 (product of the 7 dim
//    1000        NULL   NULL                NULL   words C happens to read
//                                                  past the dims area, or
//                                                  the array-size error —
//                                                  undefined, see below)
mod corruption_plane {
    use super::*;
    use crate::foundation::{read_dims, read_dims_lbounds, MAXDIM};

    // Flat 4B-header image, PACKED on-disk layout: dims[0..n] right after the
    // 16-byte header, lbounds[0..n] right after the dims (ARR_LBOUND is
    // base + 16 + 4*ndim — ndim-dependent). `ndim` is written to the header
    // verbatim; `n` is how many dim/lbound pairs are actually materialized,
    // so a corrupt header can claim more dimensions than the body carries.
    fn mk_image<'m>(mcx: Mcx<'m>, ndim: i32, dims: &[i32], lbs: &[i32]) -> PgVec<'m, u8> {
        let n = dims.len();
        assert_eq!(n, lbs.len());
        let total = 16 + 8 * n;
        let mut img = vec_with_capacity_in(mcx, total).unwrap();
        vec_append_bytes(&mut img, &::datum::varlena::set_varsize_4b(total)).unwrap();
        vec_append_bytes(&mut img, &ndim.to_ne_bytes()).unwrap();
        vec_append_bytes(&mut img, &0i32.to_ne_bytes()).unwrap(); // dataoffset: no nulls
        vec_append_bytes(&mut img, &INT4OID.to_ne_bytes()).unwrap();
        for d in dims {
            vec_append_bytes(&mut img, &d.to_ne_bytes()).unwrap();
        }
        for l in lbs {
            vec_append_bytes(&mut img, &l.to_ne_bytes()).unwrap();
        }
        assert_eq!(img.len(), total);
        img
    }

    // Every wrapper's verdict for one image: (ndims, lower(1), upper(1),
    // length(1), dims, cardinality); None = SQL NULL, Err = ereport.
    struct Verdicts {
        ndims: Option<i32>,
        lower1: Option<i32>,
        upper1: Option<i32>,
        length1: Option<i32>,
        dims: Option<String>,
        cardinality: Result<i32, String>,
    }

    fn call1(f: ::types_fmgr::PGFunction, mcx: Mcx<'_>, img: &[u8]) -> (PgResult<Datum>, bool) {
        let mut fcinfo = LocalFcinfo::<2>::new(0);
        // SAFETY: mcx outlives the call.
        unsafe { fcinfo.set_result_mcx(mcx) };
        fcinfo.set_arg(0, Datum::from_usize(img.as_ptr() as usize));
        fcinfo.set_arg(1, Datum::from_i32(1)); // reqdim = 1 for the 2-arg members
        let r = f(None, &mut fcinfo);
        (r, fcinfo.isnull)
    }

    fn verdicts(mcx: Mcx<'_>, img: &[u8]) -> Verdicts {
        let int_of = |f: ::types_fmgr::PGFunction| -> Option<i32> {
            let (r, isnull) = call1(f, mcx, img);
            let d = r.expect("header readers never ereport on this plane");
            if isnull {
                None
            } else {
                Some(d.as_i32())
            }
        };
        let dims = {
            let (r, isnull) = call1(crate::ops::fc_array_dims, mcx, img);
            let d = r.unwrap();
            if isnull {
                None
            } else {
                Some(as_str_lossy(varlena_payload(d)))
            }
        };
        let cardinality = match call1(crate::ops::fc_array_cardinality, mcx, img) {
            (Ok(d), false) => Ok(d.as_i32()),
            (Ok(_), true) => panic!("C array_cardinality never returns NULL"),
            (Err(e), _) => Err(e.message().to_string()),
        };
        Verdicts {
            ndims: int_of(crate::ops::fc_array_ndims),
            lower1: int_of(crate::ops::fc_array_lower),
            upper1: int_of(crate::ops::fc_array_upper),
            length1: int_of(crate::builtins::fc_array_length),
            dims,
            cardinality,
        }
    }

    fn as_str_lossy(v: &[u8]) -> String {
        String::from_utf8_lossy(v).into_owned()
    }

    fn setup() -> MemoryContext {
        detoast_construct::install_test_detoast();
        MemoryContext::new_bump("corruption-plane")
    }

    // read_dims_lbounds is the first thing every dims-reading wrapper does
    // with the image; ndim=7 indexed dims[6] on a [i32; 6] (panic), ndim<0
    // made `0..ndim as usize` a ~2^64 range (panic on the first arr_dim
    // slice read). It must now come back clean, ndim RAW and dims zeroed.
    #[test]
    fn read_dims_lbounds_survives_out_of_range_ndim() {
        let ctx = setup();
        let mcx = ctx.mcx();
        for ndim in [-1, i32::MIN, 7, 1000, i32::MAX] {
            let img = mk_image(mcx, ndim, &[2, 3, 1, 1, 1, 1], &[1; 6]);
            let (got, dims, lbs) = read_dims_lbounds(&img);
            assert_eq!(got, ndim, "ndim must come back RAW, never clamped");
            assert_eq!(dims, [0; MAXDIM], "out-of-range ndim fills nothing");
            assert_eq!(lbs, [0; MAXDIM]);
        }
    }

    // read_dims (the dims-only sibling the unnest/selectivity/hstore sites
    // used to open-code) carries the same contract.
    #[test]
    fn read_dims_survives_out_of_range_ndim() {
        let ctx = setup();
        let mcx = ctx.mcx();
        for ndim in [-1, i32::MIN, 7, 1000, i32::MAX] {
            let img = mk_image(mcx, ndim, &[2, 3, 1, 1, 1, 1], &[1; 6]);
            assert_eq!(read_dims(&img), (ndim, [0; MAXDIM]));
        }
        for n in 0..=MAXDIM {
            let dims: std::vec::Vec<i32> = (0..n as i32).map(|i| i + 2).collect();
            let img = mk_image(mcx, n as i32, &dims, &vec![1; n]);
            let (got_n, got_dims) = read_dims(&img);
            assert_eq!(got_n, n as i32);
            assert_eq!(&got_dims[..n], &dims[..], "ndim={n}");
        }
    }

    // The valid plane, including both boundaries (0 and MAXDIM), must be
    // untouched by the reordering.
    #[test]
    fn read_dims_lbounds_valid_plane_unchanged() {
        let ctx = setup();
        let mcx = ctx.mcx();
        // ndim = 0: nothing to fill, and that IS a valid header field value.
        let img = mk_image(mcx, 0, &[], &[]);
        assert_eq!(read_dims_lbounds(&img), (0, [0; MAXDIM], [0; MAXDIM]));
        // 1..=MAXDIM: every dim/lbound pair read, MAXDIM included.
        for n in 1..=MAXDIM {
            let dims: std::vec::Vec<i32> = (0..n as i32).map(|i| i + 2).collect();
            let lbs: std::vec::Vec<i32> = (0..n as i32).map(|i| i - 3).collect();
            let img = mk_image(mcx, n as i32, &dims, &lbs);
            let (got_n, got_dims, got_lbs) = read_dims_lbounds(&img);
            assert_eq!(got_n, n as i32);
            assert_eq!(&got_dims[..n], &dims[..], "ndim={n}");
            assert_eq!(&got_lbs[..n], &lbs[..], "ndim={n}");
            assert_eq!(&got_dims[n..], &[0; MAXDIM][n..], "tail must stay zero");
        }
    }

    // C, executed: ndims/lower/upper/length/dims = NULL, cardinality = 0.
    #[test]
    fn negative_ndim_matches_c() {
        let ctx = setup();
        let mcx = ctx.mcx();
        for ndim in [-1, i32::MIN] {
            let img = mk_image(mcx, ndim, &[2, 3, 1, 1, 1, 1], &[1; 6]);
            let v = verdicts(mcx, &img);
            assert_eq!(v.ndims, None, "array_ndims ndim={ndim}");
            assert_eq!(v.lower1, None, "array_lower ndim={ndim}");
            assert_eq!(v.upper1, None, "array_upper ndim={ndim}");
            assert_eq!(v.length1, None, "array_length ndim={ndim}");
            assert_eq!(v.dims, None, "array_dims ndim={ndim}");
            // ArrayGetNItems' own `ndim <= 0 -> 0` arm: a VALUE, not a NULL.
            assert_eq!(v.cardinality, Ok(0), "array_cardinality ndim={ndim}");
        }
    }

    // C, executed: same NULLs. cardinality is the undefined cell — C reads
    // dim words past the dims area (it returned 6 for a 7-dim body and the
    // array-size error for ndim=1000, both byte-dependent), so pgrust raises
    // the dimension-count error instead of inventing a number. What matters:
    // an Err, not a panic.
    #[test]
    fn over_maxdim_ndim_matches_c() {
        let ctx = setup();
        let mcx = ctx.mcx();
        for ndim in [7, 1000, i32::MAX] {
            let img = mk_image(mcx, ndim, &[2, 3, 1, 1, 1, 1], &[1; 6]);
            let v = verdicts(mcx, &img);
            assert_eq!(v.ndims, None, "array_ndims ndim={ndim}");
            assert_eq!(v.lower1, None, "array_lower ndim={ndim}");
            assert_eq!(v.upper1, None, "array_upper ndim={ndim}");
            assert_eq!(v.length1, None, "array_length ndim={ndim}");
            assert_eq!(v.dims, None, "array_dims ndim={ndim}");
            assert_eq!(
                v.cardinality,
                Err(alloc::format!(
                    "number of array dimensions ({ndim}) exceeds the maximum allowed ({MAXDIM})"
                )),
                "array_cardinality ndim={ndim}"
            );
        }
    }

    // The boundary values C accepts must keep working — the fix must not
    // over-tighten. ndim=0 is a VALID header field (C still nulls the
    // wrappers via `<= 0`, and cardinality returns 0); ndim=MAXDIM is the
    // last accepted dimension count and must return real values.
    #[test]
    fn boundary_ndim_0_and_maxdim_match_c() {
        let ctx = setup();
        let mcx = ctx.mcx();

        // ndim = 0
        let img = mk_image(mcx, 0, &[], &[]);
        let v = verdicts(mcx, &img);
        assert_eq!(v.ndims, None);
        assert_eq!(v.lower1, None);
        assert_eq!(v.upper1, None);
        assert_eq!(v.length1, None);
        assert_eq!(v.dims, None);
        assert_eq!(v.cardinality, Ok(0));

        // ndim = 1 (the ordinary case, as a control)
        let img = mk_image(mcx, 1, &[2], &[1]);
        let v = verdicts(mcx, &img);
        assert_eq!(v.ndims, Some(1));
        assert_eq!(v.lower1, Some(1));
        assert_eq!(v.upper1, Some(2));
        assert_eq!(v.length1, Some(2));
        assert_eq!(v.dims.as_deref(), Some("[1:2]"));
        assert_eq!(v.cardinality, Ok(2));

        // ndim = MAXDIM
        let img = mk_image(mcx, 6, &[2, 3, 1, 1, 1, 1], &[1; 6]);
        let v = verdicts(mcx, &img);
        assert_eq!(v.ndims, Some(6));
        assert_eq!(v.lower1, Some(1));
        assert_eq!(v.upper1, Some(2));
        assert_eq!(v.length1, Some(2));
        assert_eq!(v.dims.as_deref(), Some("[1:2][1:3][1:1][1:1][1:1][1:1]"));
        assert_eq!(v.cardinality, Ok(6));
    }

    // Every OTHER read_dims_lbounds caller that feeds the raw ndim to
    // array_get_n_items had the same panic (dims[i] on a 6-long slice); the
    // central guard turns all of them into the same catchable error.
    #[test]
    fn array_get_n_items_rejects_ndim_wider_than_dims() {
        assert_eq!(::arrayutils::array_get_n_items(0, &[]).unwrap(), 0);
        assert_eq!(::arrayutils::array_get_n_items(-5, &[]).unwrap(), 0);
        assert_eq!(::arrayutils::array_get_n_items(6, &[1; 6]).unwrap(), 1);
        let e = ::arrayutils::array_get_n_items(7, &[1i32; 6]).unwrap_err();
        assert_eq!(
            e.message(),
            "number of array dimensions (7) exceeds the maximum allowed (6)"
        );
        // A soft-error context gets the same verdict softly (C's ereturn).
        let mut soft = ::types_error::SoftErrorContext::new(false);
        assert_eq!(
            ::arrayutils::array_get_n_items_safe(7, &[1i32; 6], Some(&mut soft)).unwrap(),
            -1
        );
        assert!(soft.error_occurred());
    }
}

// ---- p1-lanex regressions (arrayfuncs_diff findings, 2026-07-31) ----------
mod p1_lanex_regressions {
    use super::*;
    use crate::construct::construct_md_array;
    use crate::foundation::fetch_att;
    use ::mcx::MemoryContext;

    fn sqlstate_str(e: &::types_error::PgError) -> String {
        core::str::from_utf8(&::types_error::unpack_sqlstate(e.sqlstate()))
            .unwrap()
            .to_string()
    }

    // KNOWN-DIV-2: C's fetch_att sign-extends byval words (Int32GetDatum);
    // the zero-extending version made array-fetched datums bit-unequal to
    // Datum::from_i32 of the same value (full-word consumers like
    // datum_is_equal misfire).
    #[test]
    fn fetch_att_sign_extends_like_c() {
        let v32: i32 = -1;
        let b4 = v32.to_ne_bytes();
        let d = fetch_att(b4.as_ptr(), true, 4);
        assert_eq!(d.as_usize(), Datum::from_i32(v32).as_usize());
        let v16: i16 = -2;
        let b2 = v16.to_ne_bytes();
        let d = fetch_att(b2.as_ptr(), true, 2);
        assert_eq!(d.as_usize(), Datum::from_i16(v16).as_usize());
        let v8: i8 = -3;
        let b1 = v8.to_ne_bytes();
        let d = fetch_att(b1.as_ptr(), true, 1);
        assert_eq!(d.as_usize(), Datum::from_i8(v8).as_usize());
        // Positive values stay zero-high-bits either way.
        let b4 = 7i32.to_ne_bytes();
        assert_eq!(fetch_att(b4.as_ptr(), true, 4).as_usize(), 7);
    }

    // KNOWN-DIV-1: C raises 22023 for ndims < 0 (arrayfuncs.c 3508..3511).
    #[test]
    fn construct_md_array_negative_ndims_sqlstate() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let e = construct_md_array(mcx, &[], None, -1, &[], &[], INT4OID, 4, true, b'i')
            .unwrap_err();
        assert_eq!(sqlstate_str(&e), "22023");
        assert_eq!(e.message(), "invalid number of dimensions: -1");
    }

    // KNOWN-DIV-3: bare sign in the dimension section must surface C's
    // 22P02 "Missing array dimension value." (strtol consumes nothing),
    // not a later 2202E bound error.
    #[test]
    fn array_in_bare_sign_dimension_is_22p02() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let m = meta_int4();
        for lit in ["[1:-]={1,2,3}", "[-:1]={1,2,3}", "[1:+]={1,2,3}"] {
            let mut ip = int4_in();
            let e = array_in(mcx, lit, &m, &mut ip, -1, None).unwrap_err();
            assert_eq!(sqlstate_str(&e), "22P02", "literal {lit:?}");
        }
        // Signed dimensions with digits still parse.
        let mut ip = int4_in();
        let img = array_in(mcx, "[-2:0]={1,2,3}", &m, &mut ip, -1, None)
            .unwrap()
            .unwrap();
        let mut op = int4_out();
        assert_eq!(as_str(&array_out(mcx, &img, &m, &mut op).unwrap()), "[-2:0]={1,2,3}");
    }
}

// ---- p1-lanex round 2: builtin-table asymmetry (RATIFIED Michael 2026-07-31)
mod p1_lanex_builtin_tables {
    use super::*;
    use crate::construct::{builtin_meta, deconstruct_builtin_meta, deconstruct_array_builtin};
    use ::mcx::MemoryContext;
    use ::types_core::{BOOLOID, FLOAT4OID, INT8OID, NAMEOID, REGTYPEOID, XIDOID, CSTRINGOID};

    // KNOWN-DIV-5: an unlisted oid must ERROR (C elog XX000), never panic.
    #[test]
    fn unlisted_oid_errors_not_panics() {
        let e = builtin_meta(BOOLOID).unwrap_err();
        assert_eq!(e.message(), "type 16 not supported by construct_array_builtin()");
        let e = deconstruct_builtin_meta(BOOLOID).unwrap_err();
        assert_eq!(e.message(), "type 16 not supported by deconstruct_array_builtin()");
    }

    // KNOWN-DIV-4: C's deconstruct table is a strict subset of construct's.
    // The 5 construct-only types must error through deconstruct_array_builtin
    // exactly as C's default arm does.
    #[test]
    fn deconstruct_table_matches_c_asymmetry() {
        for oid in [FLOAT4OID, INT8OID, NAMEOID, REGTYPEOID, XIDOID] {
            assert!(builtin_meta(oid).is_ok(), "oid {oid} in construct table");
            let e = deconstruct_builtin_meta(oid).unwrap_err();
            assert_eq!(
                e.message(),
                format!("type {oid} not supported by deconstruct_array_builtin()"),
            );
        }
        // ... and the 8 deconstruct types still work end-to-end.
        for oid in [CSTRINGOID] {
            assert!(deconstruct_builtin_meta(oid).is_ok());
        }
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let m = meta_int4();
        let mut ip = int4_in();
        let img = array_in(mcx, "{1,2,3}", &m, &mut ip, -1, None).unwrap().unwrap();
        let (vals, nulls) = deconstruct_array_builtin(mcx, &img, INT4OID, true).unwrap();
        assert_eq!(vals.len(), 3);
        assert!(nulls.iter().all(|n| !n));
        let e = deconstruct_array_builtin(mcx, &img, INT8OID, true).unwrap_err();
        assert_eq!(e.message(), "type 20 not supported by deconstruct_array_builtin()");
    }
}

/// Boundary-guard audit findings 5/7 (array arm): array_out accumulated into
/// an unceilinged PgVec, so an over-1GB output consumed gigabytes and
/// succeeded where C raises an immediate ERROR at the allocation ceiling.
/// array_out now streams into the StringInfo port; pre-fix this test FAILS
/// because the over-ceiling output succeeds. (~537MB of '"' doubles under
/// array_out quoting, crossing 1GB.)
#[test]
fn array_out_over_ceiling_output_raises_stringinfo_error() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let n = ::mcx::MAX_ALLOC_SIZE / 2 + 16;
    let payload = std::vec![b'"'; n];
    let d = build_varlena(mcx, &payload).unwrap();
    drop(payload);
    let img = construct_array(mcx, &[d], TEXTOID, -1, false, TYPALIGN_INT).unwrap();
    let m = meta_text();
    let mut op = text_out();
    let err = array_out(mcx, &img, &m, &mut op)
        .expect_err("array_out output above MaxAllocSize must raise the StringInfo ceiling error");
    assert_eq!(
        err.message(),
        std::format!(
            "string buffer exceeds maximum allowed length ({} bytes)",
            ::mcx::MAX_ALLOC_SIZE
        )
    );
}

// pseudotypes.c: anyarray_out/anycompatiblearray_out are `return
// array_out(fcinfo)`, anyarray_send/anycompatiblearray_send are `return
// array_send(fcinfo)`; the aliases must resolve to the same fc body.
#[test]
fn pseudotype_aliases_delegate_to_array_io() {
    let by_oid = |oid: types_core::Oid| {
        crate::builtins::ARRAYFUNCS_BUILTINS
            .iter()
            .find(|b| b.foid == oid)
            .unwrap_or_else(|| panic!("oid {oid} not registered"))
    };
    assert_eq!(by_oid(2297).func as usize, crate::builtins::fc_array_out as usize);
    assert_eq!(by_oid(5089).func as usize, crate::builtins::fc_array_out as usize);
    assert_eq!(by_oid(2503).func as usize, crate::builtins::fc_array_send as usize);
    assert_eq!(by_oid(5091).func as usize, crate::builtins::fc_array_send as usize);
    assert_eq!(by_oid(2503).name, "anyarray_send");
    assert_eq!(by_oid(5089).name, "anycompatiblearray_out");
    assert_eq!(by_oid(5091).name, "anycompatiblearray_send");
}
