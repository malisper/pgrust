//! jsonfuncs.c SRF slice: jsonb_object_keys, jsonb_array_elements[_text],
//! jsonb_each[_text] over the funcapi ValuePerCall frame. C materializes
//! rows up front (tuplestore / first-call collection); the owned row vectors
//! here are the same cost shape.

extern crate alloc;

use alloc::vec::Vec;

use crate::build::item_to_jsonb_image;
use crate::container::*;
use crate::getfield::value_as_text;
use crate::iter::{JsonbIterator, WjbToken};
use datum::Datum;
use mcx::Mcx;
use types_core::catalog::{JSONBOID, RECORDOID, TEXTOID};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{byref_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_param(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

// Owned cross-call rows (per-call memory resets between SRF calls).
enum SrfRows {
    Texts(Vec<Option<Vec<u8>>>),
    Images(Vec<Vec<u8>>),
    Tuples(Vec<Vec<u8>>),
}

fn detoast_owned(fcinfo: &Fcinfo) -> PgResult<Vec<u8>> {
    let mcx = fcinfo.result_mcx();
    let jb = super::builtins::arg_jsonb(fcinfo, 0, mcx)?;
    Ok(jb.as_bytes().to_vec())
}

fn srf_drive(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    name: &'static str,
    collect: impl FnOnce(&Fcinfo) -> PgResult<SrfRows>,
) -> PgResult<Datum> {
    let flinfo = flinfo.unwrap_or_else(|| panic!("{name}: NULL flinfo"));
    if !flinfo.has_fn_extra() {
        let rows = collect(fcinfo)?;
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("SRF rows set at first call")
        .downcast_ref::<SrfRows>()
        .expect("user_fctx is SrfRows");
    let mcx = fcinfo.result_mcx();
    let out: Option<Option<Datum>> = match rows {
        SrfRows::Texts(v) => v.get(idx).map(|r| match r {
            None => None,
            Some(bytes) => Some(
                varlena::cstring_to_text(mcx, bytes)
                    .map(varlena_result)
                    .expect("text result"),
            ),
        }),
        SrfRows::Images(v) => v
            .get(idx)
            .map(|img| Some(byref_result(mcx, img).expect("image result"))),
        SrfRows::Tuples(v) => v
            .get(idx)
            .map(|img| Some(byref_result(mcx, img).expect("tuple result"))),
    };
    match out {
        Some(Some(d)) => Ok(funcapi::srf_return_next(flinfo, fcinfo, d)),
        Some(None) => Ok(funcapi::srf_return_next_null(flinfo, fcinfo)),
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

fn require_object(payload: &[u8], name: &str) -> PgResult<()> {
    if container_is_object(payload) {
        return Ok(());
    }
    Err(invalid_param(alloc::format!(
        "cannot call {name} on a non-object"
    )))
}

pub fn fc_jsonb_object_keys(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "jsonb_object_keys", |fcinfo| {
        let image = detoast_owned(fcinfo)?;
        let payload = &image[..];
        if container_is_scalar(payload) {
            return Err(invalid_param(
                "cannot call jsonb_object_keys on a scalar".into(),
            ));
        }
        if container_is_array(payload) {
            return Err(invalid_param(
                "cannot call jsonb_object_keys on an array".into(),
            ));
        }
        let mcx = fcinfo.result_mcx();
        let mut it = JsonbIterator::init(mcx, payload)?;
        let mut keys: Vec<Option<Vec<u8>>> = Vec::new();
        loop {
            let (tok, v) = it.next(true);
            match tok {
                WjbToken::Done => break,
                WjbToken::Key => {
                    let JsonbItem::String(s) = v else {
                        panic!("object key is not a string")
                    };
                    keys.push(Some(s.to_vec()));
                }
                _ => {}
            }
        }
        Ok(SrfRows::Texts(keys))
    })
}

fn elements_check(payload: &[u8]) -> PgResult<()> {
    // C: the scalar check runs only per the array check order.
    if container_is_scalar(payload) {
        return Err(invalid_param("cannot extract elements from a scalar".into()));
    }
    if !container_is_array(payload) {
        return Err(invalid_param(
            "cannot extract elements from an object".into(),
        ));
    }
    Ok(())
}

pub fn fc_jsonb_array_elements(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "jsonb_array_elements", |fcinfo| {
        let image = detoast_owned(fcinfo)?;
        let payload = &image[..];
        elements_check(payload)?;
        let mcx = fcinfo.result_mcx();
        let mut it = JsonbIterator::init(mcx, payload)?;
        let mut rows: Vec<Vec<u8>> = Vec::new();
        loop {
            let (tok, v) = it.next(true);
            match tok {
                WjbToken::Done => break,
                WjbToken::Elem => rows.push(item_to_jsonb_image(mcx, v)?[..].to_vec()),
                _ => {}
            }
        }
        Ok(SrfRows::Images(rows))
    })
}

pub fn fc_jsonb_array_elements_text(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "jsonb_array_elements_text", |fcinfo| {
        let image = detoast_owned(fcinfo)?;
        let payload = &image[..];
        elements_check(payload)?;
        let mcx = fcinfo.result_mcx();
        let mut it = JsonbIterator::init(mcx, payload)?;
        let mut rows: Vec<Option<Vec<u8>>> = Vec::new();
        loop {
            let (tok, v) = it.next(true);
            match tok {
                WjbToken::Done => break,
                WjbToken::Elem => rows.push(text_row(mcx, &v)?),
                _ => {}
            }
        }
        Ok(SrfRows::Texts(rows))
    })
}

fn text_row(mcx: Mcx<'_>, v: &JsonbItem<'_>) -> PgResult<Option<Vec<u8>>> {
    Ok(value_as_text(mcx, v)?.map(|t| {
        let img = t.as_bytes();
        varlena_data(img).to_vec()
    }))
}

fn varlena_data(image: &[u8]) -> &[u8] {
    debug_assert!(image[0] & 0x01 == 0);
    &image[4..]
}

// One (key text, value jsonb-or-text) row formed into a composite datum
// image with a freshly built 2-column rowtype.
fn each_rows(fcinfo: &Fcinfo, as_text: bool, name: &str) -> PgResult<SrfRows> {
    let image = detoast_owned(fcinfo)?;
    let payload = &image[..];
    require_object(payload, name)?;
    let mcx = fcinfo.result_mcx();

    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, 2)?;
    tupdesc::TupleDescInitEntry(&mut desc, 1, Some("key"), TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(
        &mut desc,
        2,
        Some("value"),
        if as_text { TEXTOID } else { JSONBOID },
        -1,
        0,
    )?;
    desc.tdtypeid = RECORDOID;
    desc.tdtypmod = -1;
    // C: BlessTupleDesc — the rows are anonymous record datums; record_out
    // needs the registered typmod stamped into each tuple header.
    ::typcache_seams::assign_record_type_typmod::call(&mut desc)?;

    let mut it = JsonbIterator::init(mcx, payload)?;
    let mut rows: Vec<Vec<u8>> = Vec::new();
    loop {
        let (tok, k) = it.next(true);
        match tok {
            WjbToken::Done => break,
            WjbToken::Key => {
                let JsonbItem::String(key) = k else {
                    panic!("object key is not a string")
                };
                let key_datum = varlena_result(varlena::cstring_to_text(mcx, key)?);
                let (vtok, v) = it.next(true);
                debug_assert_eq!(vtok, WjbToken::Value);
                let (val_datum, val_null) = if as_text {
                    match value_as_text(mcx, &v)? {
                        Some(t) => (varlena_result(t), false),
                        None => (Datum::null(), true),
                    }
                } else {
                    (
                        super::builtins::image_result(item_to_jsonb_image(mcx, v)?),
                        false,
                    )
                };
                let tuple = heaptuple::heap_form_tuple(
                    mcx,
                    &desc,
                    &[key_datum, val_datum],
                    &[false, val_null],
                )?;
                rows.push(tuple.image().to_vec());
            }
            _ => {}
        }
    }
    Ok(SrfRows::Tuples(rows))
}

pub fn fc_jsonb_each(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "jsonb_each", |fcinfo| {
        each_rows(fcinfo, false, "jsonb_each")
    })
}

pub fn fc_jsonb_each_text(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "jsonb_each_text", |fcinfo| {
        each_rows(fcinfo, true, "jsonb_each_text")
    })
}
