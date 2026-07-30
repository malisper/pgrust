//! EEOP_XMLEXPR (execExprInterp.c ExecEvalXmlExpr) + the fmgr-needing half of
//! xml.c's map_sql_value_to_xml_value; the libxml value cores live in adt_xml.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::ptr::NonNull;

use ::datum::{Datum, NullableDatum};
use ::mcx::Mcx;
use ::types_core::catalog::{
    BOOLOID, BYTEAOID, DATEOID, TIMESTAMPOID, TIMESTAMPTZOID, XMLOID,
};
use ::types_core::{InvalidOid, Oid};
use ::types_error::{PgError, PgResult, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE};
use ::types_nodes::primnodes::{XmlExpr, XmlExprOp};

use alloc::boxed::Box;

use crate::arrayops::{res_mcx, ResMcx};

pub struct XmlExprState {
    // Plan-lived XmlExpr, lifetime-erased (WholeRowState tupdesc precedent).
    pub xexpr: NonNull<XmlExpr<'static>>,
    pub named_slots: NonNull<NullableDatum>,
    pub arg_slots: NonNull<NullableDatum>,
    pub n_named: u16,
    pub n_args: u16,
    pub resmcx: ResMcx,
}

fn xml_datum(mcx: Mcx<'_>, payload: &[u8]) -> PgResult<Datum> {
    let total = payload.len() + ::datum::VARHDRSZ;
    let mut image: ::mcx::PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, total)?;
    ::mcx::vec_append_bytes(&mut image, &::datum::set_varsize_4b(total))?;
    ::mcx::vec_append_bytes(&mut image, payload)?;
    Ok(Datum::from_usize(image.leak().as_ptr() as usize))
}

// VARDATA_ANY over a possibly toasted/packed varlena datum.
fn varlena_payload<'a>(d: Datum, slot: &ResMcx) -> PgResult<&'a [u8]> {
    use ::types_tuple::varatt;
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null by-ref datum addresses a live varlena.
    unsafe {
        if varatt::varatt_is_4b_u(p) {
            let total = varatt::varsize_any(p);
            Ok(core::slice::from_raw_parts(p.add(::datum::VARHDRSZ), total - ::datum::VARHDRSZ))
        } else if varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p) {
            let total = varatt::varsize_1b(p);
            Ok(core::slice::from_raw_parts(p.add(1), total - 1))
        } else {
            let mcx = res_mcx(slot);
            let raw = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            let img = ::detoast_seams::detoast_attr::call(mcx, raw)?;
            let img: &'a [u8] = &*(img.leak() as *const [u8]);
            Ok(&img[::datum::VARHDRSZ..])
        }
    }
}

pub fn eval_xml_expr(st: &XmlExprState) -> PgResult<(Datum, bool)> {
    // SAFETY: compile-time XmlExpr reference, plan-lived.
    let x: &XmlExpr<'_> = unsafe { st.xexpr.as_ref() };
    // SAFETY: compile-allocated slot arrays sized to the arg lists.
    let named =
        unsafe { core::slice::from_raw_parts(st.named_slots.as_ptr(), st.n_named as usize) };
    let args = unsafe { core::slice::from_raw_parts(st.arg_slots.as_ptr(), st.n_args as usize) };
    let mcx = res_mcx(&st.resmcx);

    match x.op {
        XmlExprOp::IS_XMLCONCAT => {
            let mut vals: ::mcx::PgVec<&[u8]> = ::mcx::vec_with_capacity_in(mcx, args.len())?;
            for nd in args {
                if !nd.isnull {
                    vals.push(varlena_payload(nd.value, &st.resmcx)?);
                }
            }
            if vals.is_empty() {
                return Ok((Datum::null(), true));
            }
            let out = adt_xml::xmlconcat(&vals)?;
            Ok((xml_datum(mcx, &out)?, false))
        }
        XmlExprOp::IS_XMLFOREST => {
            let mut buf: ::mcx::PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, 64)?;
            let mut any = false;
            for (i, nd) in named.iter().enumerate() {
                if nd.isnull {
                    continue;
                }
                let argname = x
                    .arg_names
                    .nth(i)
                    .as_string()
                    .expect("arg_names cell is String")
                    .sval;
                let argtype = crate::compile::expr_type(x.named_args.nth(i));
                let mapped =
                    map_sql_value_to_xml_value(nd.value, argtype, true, &st.resmcx)?;
                ::mcx::vec_append_bytes(&mut buf, b"<")?;
                ::mcx::vec_append_bytes(&mut buf, argname.as_bytes())?;
                ::mcx::vec_append_bytes(&mut buf, b">")?;
                ::mcx::vec_append_bytes(&mut buf, mapped.as_bytes())?;
                ::mcx::vec_append_bytes(&mut buf, b"</")?;
                ::mcx::vec_append_bytes(&mut buf, argname.as_bytes())?;
                ::mcx::vec_append_bytes(&mut buf, b">")?;
                any = true;
            }
            if !any {
                return Ok((Datum::null(), true));
            }
            Ok((xml_datum(mcx, &buf)?, false))
        }
        XmlExprOp::IS_XMLELEMENT => {
            let mut named_strs: Vec<(String, Option<String>)> =
                Vec::with_capacity(named.len());
            for (i, nd) in named.iter().enumerate() {
                let argname = x
                    .arg_names
                    .nth(i)
                    .as_string()
                    .expect("arg_names cell is String")
                    .sval
                    .to_string();
                let v = if nd.isnull {
                    None
                } else {
                    let argtype = crate::compile::expr_type(x.named_args.nth(i));
                    Some(map_sql_value_to_xml_value(nd.value, argtype, false, &st.resmcx)?)
                };
                named_strs.push((argname, v));
            }
            let mut content: Vec<String> = Vec::with_capacity(args.len());
            for (i, nd) in args.iter().enumerate() {
                if nd.isnull {
                    continue;
                }
                let argtype = crate::compile::expr_type(x.args.nth(i));
                content.push(map_sql_value_to_xml_value(nd.value, argtype, true, &st.resmcx)?);
            }
            let out = adt_xml::xmlelement(
                x.name.expect("XMLELEMENT carries a name"),
                &named_strs,
                &content,
            )?;
            Ok((xml_datum(mcx, &out)?, false))
        }
        XmlExprOp::IS_XMLPARSE => {
            debug_assert_eq!(args.len(), 2);
            if args[0].isnull || args[1].isnull {
                return Ok((Datum::null(), true));
            }
            let data = varlena_payload(args[0].value, &st.resmcx)?;
            let preserve = args[1].value.as_bool();
            let out = adt_xml::xmlparse(data, xml_opt(x.xmloption), preserve)?;
            Ok((xml_datum(mcx, &out)?, false))
        }
        XmlExprOp::IS_XMLPI => {
            let name = x.name.expect("XMLPI carries a name");
            if args.is_empty() {
                // No-argument form: adt_xml::xmlpi validates the target but
                // conflates it with the NULL argument; build "<?name?>" here.
                let _ = adt_xml::xmlpi(name, None)?;
                let mut buf: ::mcx::PgVec<u8> =
                    ::mcx::vec_with_capacity_in(mcx, name.len() + 4)?;
                ::mcx::vec_append_bytes(&mut buf, b"<?")?;
                ::mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
                ::mcx::vec_append_bytes(&mut buf, b"?>")?;
                return Ok((xml_datum(mcx, &buf)?, false));
            }
            let arg = if args[0].isnull {
                None
            } else {
                Some(varlena_payload(args[0].value, &st.resmcx)?)
            };
            match adt_xml::xmlpi(name, arg)? {
                Some(out) => Ok((xml_datum(mcx, &out)?, false)),
                None => Ok((Datum::null(), true)),
            }
        }
        XmlExprOp::IS_XMLROOT => {
            debug_assert_eq!(args.len(), 3);
            if args[0].isnull {
                return Ok((Datum::null(), true));
            }
            let data = varlena_payload(args[0].value, &st.resmcx)?;
            let version = if args[1].isnull {
                None
            } else {
                Some(varlena_payload(args[1].value, &st.resmcx)?)
            };
            debug_assert!(!args[2].isnull);
            let standalone = match args[2].value.as_i32() {
                0 => adt_xml::XmlStandaloneType::XML_STANDALONE_YES,
                1 => adt_xml::XmlStandaloneType::XML_STANDALONE_NO,
                2 => adt_xml::XmlStandaloneType::XML_STANDALONE_NO_VALUE,
                _ => adt_xml::XmlStandaloneType::XML_STANDALONE_OMITTED,
            };
            let out = adt_xml::xmlroot(data, version, standalone)?;
            Ok((xml_datum(mcx, &out)?, false))
        }
        XmlExprOp::IS_XMLSERIALIZE => {
            debug_assert_eq!(args.len(), 1);
            if args[0].isnull {
                return Ok((Datum::null(), true));
            }
            let data = varlena_payload(args[0].value, &st.resmcx)?;
            let out = adt_xml::xmltotext_with_options(data, xml_opt(x.xmloption), x.indent)?;
            Ok((xml_datum(mcx, &out)?, false))
        }
        XmlExprOp::IS_DOCUMENT => {
            debug_assert_eq!(args.len(), 1);
            if args[0].isnull {
                return Ok((Datum::null(), true));
            }
            let data = varlena_payload(args[0].value, &st.resmcx)?;
            Ok((Datum::from_bool(adt_xml::xml_is_document(data)?), false))
        }
    }
}

// C map_sql_value_to_xml_value (xml.c:2562): the fmgr/lsyscache half lives
// here; escape_xml/encode_binary/XSD scalar rules come from adt_xml + datetime.
pub fn map_sql_value_to_xml_value(
    value: Datum,
    type_: Oid,
    xml_escape_strings: bool,
    resmcx: &ResMcx,
) -> PgResult<String> {
    let mcx = res_mcx(resmcx);

    let elmtype = ::lsyscache::typ::get_base_element_type(type_)?;
    if elmtype != InvalidOid {
        let (elmlen, elmbyval, elmalign) = ::lsyscache::typ::get_typlenbyvalalign(elmtype)?;
        let img = array_image(value, resmcx)?;
        let (elems, nulls) =
            ::arrayfuncs::construct::deconstruct_array(mcx, img, elmlen as i32, elmbyval, elmalign as u8, true)?;
        let mut buf = String::new();
        for (elem, isnull) in elems.iter().zip(nulls.iter()) {
            if *isnull {
                continue;
            }
            buf.push_str("<element>");
            buf.push_str(&map_sql_value_to_xml_value(*elem, elmtype, true, resmcx)?);
            buf.push_str("</element>");
        }
        return Ok(buf);
    }

    let type_ = ::lsyscache::typ::getBaseType(type_)?;

    match type_ {
        BOOLOID => {
            return Ok(if value.as_bool() { "true".to_string() } else { "false".to_string() })
        }
        DATEOID => {
            let date = value.as_i32();
            if ::adt_date::DATE_NOT_FINITE(date) {
                return Err(xsd_infinite("date"));
            }
            let mut tm = ::adt_datetime::pg_tm::default();
            ::adt_datetime::calendar::j2date(
                date + ::adt_datetime::consts::POSTGRES_EPOCH_JDATE,
                &mut tm.tm_year,
                &mut tm.tm_mon,
                &mut tm.tm_mday,
            );
            let mut buf = [0u8; ::adt_datetime::consts::MAXDATELEN + 1];
            let n = ::adt_datetime::EncodeDateOnly(
                &tm,
                ::adt_datetime::consts::USE_XSD_DATES,
                &mut buf,
            );
            return Ok(core::str::from_utf8(&buf[..n]).expect("date encodes ASCII").to_string());
        }
        TIMESTAMPOID => {
            let ts = value.as_i64();
            if ::adt_timestamp::TIMESTAMP_NOT_FINITE(ts) {
                return Err(xsd_infinite("timestamp"));
            }
            let mut tm = ::adt_datetime::pg_tm::default();
            let mut fsec = 0;
            if ::adt_timestamp::timestamp2tm(ts, None, &mut tm, &mut fsec, None, None).is_err() {
                return Err(ts_out_of_range());
            }
            let mut buf = [0u8; ::adt_datetime::consts::MAXDATELEN + 1];
            let n = ::adt_datetime::EncodeDateTime(
                &mut tm,
                fsec,
                false,
                0,
                None,
                ::adt_datetime::consts::USE_XSD_DATES,
                &mut buf,
            );
            return Ok(core::str::from_utf8(&buf[..n]).expect("ts encodes ASCII").to_string());
        }
        TIMESTAMPTZOID => {
            let ts = value.as_i64();
            if ::adt_timestamp::TIMESTAMP_NOT_FINITE(ts) {
                return Err(xsd_infinite("timestamp"));
            }
            let mut tm = ::adt_datetime::pg_tm::default();
            let mut fsec = 0;
            let mut tz = 0;
            let mut tzn: Option<&'static str> = None;
            if ::adt_timestamp::timestamp2tm(
                ts,
                Some(&mut tz),
                &mut tm,
                &mut fsec,
                Some(&mut tzn),
                None,
            )
            .is_err()
            {
                return Err(ts_out_of_range());
            }
            let mut buf = [0u8; ::adt_datetime::consts::MAXDATELEN + 1];
            let n = ::adt_datetime::EncodeDateTime(
                &mut tm,
                fsec,
                true,
                tz,
                tzn.map(|s| s.as_bytes()),
                ::adt_datetime::consts::USE_XSD_DATES,
                &mut buf,
            );
            return Ok(core::str::from_utf8(&buf[..n]).expect("ts encodes ASCII").to_string());
        }
        BYTEAOID => {
            let payload = varlena_payload(value, resmcx)?;
            let out = adt_xml::encode_binary(payload, adt_xml::xmlbinary())?;
            return Ok(String::from_utf8(out).expect("base64/hex is ASCII"));
        }
        _ => {}
    }

    let (typeout, _isvarlena) = ::lsyscache::typ::getTypeOutputInfo(type_)?;
    let mut finfo = ::fmgr_core::fmgr_info(typeout)?;
    let mut fcinfo = ::types_fmgr::LocalFcinfo::<1>::fresh(InvalidOid);
    // SAFETY: the armed per-eval context outlives this single output call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(0, value);
    let out = finfo.invoke(&mut fcinfo)?;
    // SAFETY: output functions return a NUL-terminated cstring datum.
    let cstr = unsafe { core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) };
    let s = core::str::from_utf8(cstr.to_bytes()).expect("typoutput yields server encoding");

    if type_ == XMLOID || !xml_escape_strings {
        Ok(s.to_string())
    } else {
        Ok(String::from_utf8(adt_xml::escape_xml(s.as_bytes())).expect("escape keeps encoding"))
    }
}

// DatumGetArrayTypeP: full flat image (arrayops::datum_array_image shape).
fn array_image<'a>(d: Datum, slot: &ResMcx) -> PgResult<&'a [u8]> {
    use ::types_tuple::varatt;
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null array datum addresses a live varlena.
    unsafe {
        if varatt::varatt_is_4b_u(p) {
            Ok(core::slice::from_raw_parts(p, varatt::varsize_any(p)))
        } else {
            let mcx = res_mcx(slot);
            let raw = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            let img = ::detoast_seams::detoast_attr::call(mcx, raw)?;
            Ok(&*(img.leak() as *const [u8]))
        }
    }
}

#[track_caller]
#[cold]
fn xsd_infinite(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{what} out of range"))
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
            .with_detail(format!("XML does not support infinite {what} values.")),
    )
}

#[track_caller]
#[cold]
fn ts_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("timestamp out of range".to_string())
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

fn xml_opt(v: types_nodes::XmlOptionType) -> adt_xml::XmlOptionType {
    match v {
        types_nodes::XmlOptionType::XMLOPTION_DOCUMENT => adt_xml::XmlOptionType::XMLOPTION_DOCUMENT,
        types_nodes::XmlOptionType::XMLOPTION_CONTENT => adt_xml::XmlOptionType::XMLOPTION_CONTENT,
    }
}
