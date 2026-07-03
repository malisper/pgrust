use ::datum::array_build::ArrayBuildState;
use ::datum::Datum;
use ::mcx::{vec_with_capacity_in, Mcx, MemoryContext};
use ::stringinfo::StringInfo;
use ::types_core::{INT4OID, TEXTOID};
use ::types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use ::types_error::PgResult;

use crate::build::{accum_array_result, make_array_result};
use crate::construct::{construct_md_array, deconstruct_array};
use crate::foundation::varsize_any;
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
