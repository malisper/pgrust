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
            let p1 =
                datum_get_expanded_array(Datum::from_usize(img.as_ptr() as usize), &parent)
                    .unwrap();
            assert_eq!((*p1).ea_magic, EA_MAGIC);
            let rw = ::datum::expandeddatum::eohp_get_rw_datum(&raw const (*p1).hdr);
            let mut meta = ArrayMetaState::invalid();
            let p2 = datum_get_expanded_array_x(rw, &parent, Some(&mut meta)).unwrap();
            assert_eq!(p1, p2);
            assert_eq!(meta.element_type, INT4OID);
            assert_eq!(meta.typlen, 4);
        }
    }

    #[test]
    fn parent_reset_reclaims_objects() {
        let mut parent = MemoryContext::new("t");
        let img: std::vec::Vec<u8> = {
            let tmp = MemoryContext::new("img");
            int4_array(tmp.mcx(), &[1, 2], None).as_slice().to_vec()
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
