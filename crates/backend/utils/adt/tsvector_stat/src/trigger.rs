//! tsvector_op.c tsvector_update_trigger (oid 3752, config named in the
//! trigger arguments; the regconfig-column variant 3759 stays loud).

use ::datum::{Datum, Varlena};
use ::mcx::Mcx;
use ::to_tsany::env::CacheEnv;
use ::to_tsany::vector::make_tsvector;
use ::ts_parse::{parsetext, ParsedText};
use ::types_core::Oid;
use ::types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_UNDEFINED_COLUMN,
};
use ::types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use ::types_trigger::{
    TRIGGER_EVENT_BEFORE, TRIGGER_EVENT_TIMINGMASK, TRIGGER_FIRED_BY_INSERT,
    TRIGGER_FIRED_BY_UPDATE, TRIGGER_FIRED_FOR_ROW,
};
use ::types_trigger_call::trigger_data_from_fcinfo;
use ::types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
use ::types_tuple::HeapTupleData;

use crate::{detoasted_image, TEXTOID, TSVECTOROID};

#[track_caller]
#[cold]
#[inline(never)]
fn internal_err(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("tsvector_update_trigger: {msg}")))
}

#[track_caller]
#[cold]
#[inline(never)]
fn col_err(sqlstate: ::types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

pub fn fc_tsvector_update_trigger_byid(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    tsvector_update_trigger(fcinfo)
}

fn tsvector_update_trigger(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the trigger call machinery keeps the TriggerData live for the
    // duration of the call.
    let Some(td) = (unsafe { trigger_data_from_fcinfo(fcinfo) }) else {
        return Err(internal_err("not fired by trigger manager"));
    };
    if !TRIGGER_FIRED_FOR_ROW(td.tg_event) {
        return Err(internal_err("must be fired for row"));
    }
    if td.tg_event & TRIGGER_EVENT_TIMINGMASK != TRIGGER_EVENT_BEFORE {
        return Err(internal_err("must be fired BEFORE event"));
    }
    let (rettuple_nn, mut update_needed) = if TRIGGER_FIRED_BY_INSERT(td.tg_event) {
        (td.tg_trigtuple.expect("INSERT row trigger has a tuple"), true)
    } else if TRIGGER_FIRED_BY_UPDATE(td.tg_event) {
        (td.tg_newtuple.expect("UPDATE row trigger has a new tuple"), false)
    } else {
        return Err(internal_err("must be fired for INSERT or UPDATE"));
    };
    // SAFETY: live tuple per the trigger call contract.
    let rettuple: &HeapTupleData<'_> = unsafe { rettuple_nn.as_ref() };

    let trigger = td.tg_trigger;
    let tupdesc = td.tg_relation.descr();
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    if trigger.tgnargs < 3 {
        return Err(internal_err(
            "arguments must be tsvector_field, ts_config, text_field1, ...)",
        ));
    }

    let tsvector_attr_num = ::spi::SPI_fnumber(tupdesc, trigger.tgargs[0].as_str());
    if tsvector_attr_num == ::spi::SPI_ERROR_NOATTRIBUTE {
        return Err(col_err(
            ERRCODE_UNDEFINED_COLUMN,
            format!("tsvector column \"{}\" does not exist", trigger.tgargs[0].as_str()),
        ));
    }
    if !::coerce::IsBinaryCoercible(
        ::spi::SPI_gettypeid(tupdesc, tsvector_attr_num),
        TSVECTOROID,
    )? {
        return Err(col_err(
            ERRCODE_DATATYPE_MISMATCH,
            format!("column \"{}\" is not of tsvector type", trigger.tgargs[0].as_str()),
        ));
    }

    // Config named in the trigger args; schema qualification is required so
    // results are not search_path dependent.
    let names = ::varlena::textToQualifiedNameList(mcx, trigger.tgargs[1].as_str())?;
    if names.len() < 2 {
        return Err(col_err(
            ERRCODE_INVALID_PARAMETER_VALUE,
            format!(
                "text search configuration name \"{}\" must be schema-qualified",
                trigger.tgargs[1].as_str()
            ),
        ));
    }
    let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let cfg_id: Oid = ::ts_cache::get_ts_config_oid(&name_refs, false)?;

    let mut prs = ParsedText::with_capacity(mcx, 32)?;
    // Lazy env: resolution happens at parsetext (C ts_parse.c), same as C's
    // tsvector_update_trigger which passes cfg_id straight to parsetext.
    let mut env = CacheEnv::new(mcx, cfg_id);

    for k in 2..trigger.tgnargs as usize {
        let colname = trigger.tgargs[k].as_str();
        let numattr = ::spi::SPI_fnumber(tupdesc, colname);
        if numattr == ::spi::SPI_ERROR_NOATTRIBUTE {
            return Err(col_err(
                ERRCODE_UNDEFINED_COLUMN,
                format!("column \"{colname}\" does not exist"),
            ));
        }
        if !::coerce::IsBinaryCoercible(::spi::SPI_gettypeid(tupdesc, numattr), TEXTOID)? {
            return Err(col_err(
                ERRCODE_DATATYPE_MISMATCH,
                format!("column \"{colname}\" is not of a character type"),
            ));
        }

        if td.tg_updatedcols != 0 {
            // SAFETY: tg_updatedcols is a live Bitmapset for the call (set
            // by the row-trigger driver).
            let cols = unsafe { &*(td.tg_updatedcols as *const ::types_nodes::Bitmapset<'_>) };
            if cols.is_member(numattr - FirstLowInvalidHeapAttributeNumber) {
                update_needed = true;
            }
        }

        let (d, isnull) = ::spi::SPI_getbinval(rettuple, tupdesc, numattr);
        if isnull {
            continue;
        }
        let img = detoasted_image(mcx, d)?;
        parsetext(mcx, &mut env, &mut prs, &img[4..])?;
    }

    if !update_needed {
        return Ok(Datum::from_usize(rettuple_nn.as_ptr() as usize));
    }

    let vec_img = make_tsvector(mcx, &mut prs)?;
    let tsdatum = varlena_result(Varlena::from_image(vec_img));
    let newtuple = ::heaptuple::heap_modify_tuple_by_cols(
        mcx,
        rettuple,
        tupdesc,
        &[tsvector_attr_num],
        &[tsdatum],
        &[false],
    )?;
    let ht = newtuple.as_tuple();
    // SAFETY: the image is mcx-owned and leaked below; the descriptor copy
    // rides in the same mcx so it outlives the trigger driver's copy-out.
    let htd = unsafe {
        HeapTupleData::from_raw_parts(ht.header_ptr(), ht.t_len, ht.t_self, ht.t_tableOid)
    };
    let slot = ::mcx::leak_in(::mcx::alloc_in(mcx, htd)?);
    core::mem::forget(newtuple);
    Ok(Datum::from_usize(core::ptr::from_mut(slot) as usize))
}
