//! fmgr wrappers (`fc_*`) + `XML_BUILTINS` for fmgr-core.

use ::datum::{Datum, Varlena};
use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::XmlStandaloneType;

fn arg_text<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    // SAFETY: catalog arg i is a non-null text/xml varlena (strict fn).
    Ok(unsafe { fcinfo.arg_varlena_packed(i)? }.data())
}

fn ret_xml<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Datum> {
    Ok(varlena_result(payload_varlena(mcx, payload)?))
}

fn payload_varlena<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Varlena<'mcx>> {
    let mut image: ::mcx::PgVec<u8> =
        ::mcx::vec_with_capacity_in(mcx, payload.len() + ::datum::VARHDRSZ)?;
    image.resize(::datum::VARHDRSZ, 0);
    ::mcx::vec_append_bytes(&mut image, payload)?;
    Ok(Varlena::from_image(image))
}

pub fn fc_xml_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of the in-function is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes().to_vec();
    // SAFETY: context, if set, rides per the ErrorSaveNode contract.
    let esc = unsafe { fcinfo.soft_error_context() };
    match crate::xml_in(&s, esc)? {
        Some(v) => {
            let mcx = fcinfo.result_mcx();
            ret_xml(mcx, &v)
        }
        None => Ok(fcinfo.return_null()),
    }
}

struct OutBuf(Vec<u8>);

// textout's fn_extra scratch convention: cstring results live on the resolved
// FmgrInfo, so const-folding callers without an armed result mcx work too.
pub fn fc_xml_out(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let out = crate::xml_out(arg_text(fcinfo, 0)?)?;
    let Some(flinfo) = flinfo else {
        panic!("xml_out: cstring result needs a resolved FmgrInfo's scratch")
    };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutBuf(Vec::new()));
    }
    let buf = &mut flinfo.fn_extra_mut::<OutBuf>().unwrap().0;
    buf.clear();
    buf.reserve(out.len() + 1);
    buf.extend_from_slice(&out);
    buf.push(0);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_xml_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let cursor = buf.cursor;
    let raw = buf.as_bytes()[cursor..].to_vec();
    buf.cursor = buf.len();
    let mcx = fcinfo.result_mcx();
    let converted = crate::xml_recv(mcx, &raw)?;
    ret_xml(mcx, &converted)
}

pub fn fc_xml_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let x = arg_text(fcinfo, 0)?;
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::xml_send(mcx, x)?))
}

pub fn fc_xmlcomment(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let out = crate::xmlcomment(arg_text(fcinfo, 0)?)?;
    ret_xml(fcinfo.result_mcx(), &out)
}

pub fn fc_texttoxml(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let out = crate::texttoxml(arg_text(fcinfo, 0)?)?;
    ret_xml(fcinfo.result_mcx(), &out)
}

pub fn fc_xmltotext(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let out = crate::xmltotext(arg_text(fcinfo, 0)?)?;
    ret_xml(fcinfo.result_mcx(), &out)
}

pub fn fc_xmlvalidate(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(crate::xmlvalidate().into())
}

pub fn fc_xmltext(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let out = crate::xmltext(arg_text(fcinfo, 0)?)?;
    ret_xml(fcinfo.result_mcx(), &out)
}

pub fn fc_xmlconcat2(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a1 = if fcinfo.argisnull(0) { None } else { Some(arg_text(fcinfo, 0)?) };
    let a2 = if fcinfo.argisnull(1) { None } else { Some(arg_text(fcinfo, 1)?) };
    match crate::xmlconcat2(a1, a2)? {
        Some(v) => ret_xml(fcinfo.result_mcx(), &v),
        None => Ok(fcinfo.return_null()),
    }
}

fn wellformed(
    fcinfo: &mut Fcinfo,
    f: fn(&[u8]) -> PgResult<bool>,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(f(arg_text(fcinfo, 0)?)?))
}

pub fn fc_xml_is_well_formed(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    wellformed(fcinfo, crate::xml_is_well_formed)
}

pub fn fc_xml_is_well_formed_document(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    wellformed(fcinfo, crate::xml_is_well_formed_document)
}

pub fn fc_xml_is_well_formed_content(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    wellformed(fcinfo, crate::xml_is_well_formed_content)
}

use ::types_core::catalog::XMLOID;

fn arg_array_image<'a>(fcinfo: &'a Fcinfo, i: usize) -> &'a [u8] {
    // SAFETY: strict fn — arg i is a non-null array varlena; regular arrays
    // are 4B-header images (detoast handled by arg_varlena_packed upstream of
    // deconstruct would lose the header, so read the raw image).
    unsafe {
        let p = fcinfo.arg_ptr(i);
        let total = ::types_tuple::varatt::varsize_any(p);
        core::slice::from_raw_parts(p, total)
    }
}

pub fn fc_xpath(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let xpath_expr = arg_text(fcinfo, 0)?;
    let data = arg_text(fcinfo, 1)?;
    let namespaces = arg_array_image(fcinfo, 2);
    let mcx = fcinfo.result_mcx();

    let mut items: Vec<Vec<u8>> = Vec::new();
    crate::xpath::xpath_internal(xpath_expr, data, Some(namespaces), Some(&mut items))?;

    let mut astate = arrayfuncs::init_array_result(mcx, XMLOID, true)?;
    for item in &items {
        let v = payload_varlena(mcx, item)?;
        let d = varlena_result(v);
        astate = arrayfuncs::accum_array_result(mcx, Some(astate), d, false, XMLOID)?;
    }
    let image = arrayfuncs::make_array_result(mcx, &astate)?;
    let d = Datum::from_usize(image.as_ptr() as usize);
    core::mem::forget(image);
    Ok(d)
}

pub fn fc_xpath_exists(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let xpath_expr = arg_text(fcinfo, 0)?;
    let data = arg_text(fcinfo, 1)?;
    let namespaces = arg_array_image(fcinfo, 2);
    let n = crate::xpath::xpath_internal(xpath_expr, data, Some(namespaces), None)?;
    Ok(Datum::from_bool(n > 0))
}

pub fn fc_xmlexists(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let xpath_expr = arg_text(fcinfo, 0)?;
    let data = arg_text(fcinfo, 1)?;
    let n = crate::xpath::xpath_internal(xpath_expr, data, None, None)?;
    Ok(Datum::from_bool(n > 0))
}

/// `XmlStandaloneType` decode for executor XMLROOT eval (primnodes int arg).
pub fn standalone_from_int(v: i32) -> XmlStandaloneType {
    match v {
        0 => XmlStandaloneType::XML_STANDALONE_YES,
        1 => XmlStandaloneType::XML_STANDALONE_NO,
        2 => XmlStandaloneType::XML_STANDALONE_NO_VALUE,
        _ => XmlStandaloneType::XML_STANDALONE_OMITTED,
    }
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

// pg_proc.dat rows for xml.c (schema-mapping family lives in the xmlmap crate).
pub const XML_BUILTINS: &[FmgrBuiltin] = &[
    b(2614, "xmlexists", 2, fc_xmlexists),
    b(2893, "xml_in", 1, fc_xml_in),
    b(2894, "xml_out", 1, fc_xml_out),
    b(2895, "xmlcomment", 1, fc_xmlcomment),
    b(2896, "texttoxml", 1, fc_texttoxml),
    b(2897, "xmlvalidate", 2, fc_xmlvalidate),
    b(2898, "xml_recv", 1, fc_xml_recv),
    b(2899, "xml_send", 1, fc_xml_send),
    FmgrBuiltin {
        foid: 2900,
        name: "xmlconcat2",
        nargs: 2,
        strict: false,
        retset: false,
        func: fc_xmlconcat2,
    },
    b(2922, "xmltotext", 1, fc_xmltotext),
    b(2931, "xpath", 3, fc_xpath),
    b(3049, "xpath_exists", 3, fc_xpath_exists),
    b(3051, "xml_is_well_formed", 1, fc_xml_is_well_formed),
    b(3052, "xml_is_well_formed_document", 1, fc_xml_is_well_formed_document),
    b(3053, "xml_is_well_formed_content", 1, fc_xml_is_well_formed_content),
    b(3813, "xmltext", 1, fc_xmltext),
];
