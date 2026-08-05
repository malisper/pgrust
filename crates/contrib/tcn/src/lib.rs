//! `contrib/tcn` — the `triggered_change_notification` AFTER-row trigger that
//! sends a LISTEN/NOTIFY payload carrying the primary-key values of the
//! changed row. Payload format: `"table",O,"col"='val'` (quotes doubled).

use datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_trigger::{
    TRIGGER_FIRED_AFTER, TRIGGER_FIRED_BY_DELETE, TRIGGER_FIRED_BY_INSERT, TRIGGER_FIRED_BY_UPDATE,
    TRIGGER_FIRED_FOR_ROW,
};
use types_trigger_call::trigger_data_from_fcinfo;

const LIBRARY: &str = "tcn";

#[track_caller]
#[cold]
#[inline(never)]
fn protocol_err(msg: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("triggered_change_notification: {msg}"))
            .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
    )
}

// C strcpy_quoted: wrap `s` in `q`, doubling any embedded `q`. Operates on raw
// bytes (multibyte-safe; the char cast would re-encode Latin-1).
fn strcpy_quoted(out: &mut Vec<u8>, s: &[u8], q: u8) {
    out.push(q);
    for &c in s {
        if c == q {
            out.push(q);
        }
        out.push(c);
    }
    out.push(q);
}

fn fc_triggered_change_notification(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: the trigger call machinery keeps the TriggerData live for the call.
    let Some(td) = (unsafe { trigger_data_from_fcinfo(fcinfo) }) else {
        return Err(protocol_err("must be called as trigger"));
    };
    if !TRIGGER_FIRED_AFTER(td.tg_event) {
        return Err(protocol_err("must be called after the change"));
    }
    if !TRIGGER_FIRED_FOR_ROW(td.tg_event) {
        return Err(protocol_err("must be called for each row"));
    }

    let operation = if TRIGGER_FIRED_BY_INSERT(td.tg_event) {
        'I'
    } else if TRIGGER_FIRED_BY_UPDATE(td.tg_event) {
        'U'
    } else if TRIGGER_FIRED_BY_DELETE(td.tg_event) {
        'D'
    } else {
        return Err(Box::new(PgError::error(
            "triggered_change_notification: trigger fired by unrecognized operation".to_string(),
        )));
    };

    let trigger = td.tg_trigger;
    if trigger.tgnargs > 1 {
        return Err(protocol_err("must not be called with more than one parameter"));
    }
    let channel = if trigger.tgnargs == 0 {
        "tcn"
    } else {
        trigger.tgargs[0].as_str()
    };

    let trigtuple = td.tg_trigtuple.expect("row trigger has an original tuple");
    let rel = td.tg_relation;
    let tupdesc = rel.descr();

    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: live tuple per the trigger call contract.
    let trigtuple_ref = unsafe { trigtuple.as_ref() };

    let mut found_pk = false;
    let index_oids = relcache::RelationGetIndexList(mcx, rel.rd_id)?;
    for &indexoid in index_oids.iter() {
        let Some(index_rel) = relcache::RelationIdGetRelation(indexoid)? else {
            continue;
        };
        let index = index_rel
            .rd_index
            .as_ref()
            .expect("an index relation carries rd_index");
        if index.indisprimary && index.indisvalid {
            let indnkeyatts = index.indnkeyatts as usize;
            if indnkeyatts > 0 {
                found_pk = true;
                let mut payload: Vec<u8> = Vec::new();
                strcpy_quoted(&mut payload, rel.rd_rel.relname.name_str(), b'"');
                payload.push(b',');
                payload.push(operation as u8);

                for i in 0..indnkeyatts {
                    let colno = index.indkey[i] as i32;
                    let attr = &tupdesc.attrs[colno as usize - 1];
                    payload.push(b',');
                    strcpy_quoted(&mut payload, attr.attname.name_str(), b'"');
                    payload.push(b'=');
                    let val = spi::SPI_getvalue(mcx, trigtuple_ref, tupdesc, colno)?;
                    strcpy_quoted(&mut payload, val.unwrap_or(b""), b'\'');
                }

                let payload = String::from_utf8(payload).expect("PK payload is valid UTF-8");
                commands_async::Async_Notify(channel, Some(&payload))?;
            }
            break;
        }
    }

    if !found_pk {
        return Err(protocol_err("must be called on a table with a primary key"));
    }

    Ok(Datum::null())
}

fn lookup(function: &str) -> Option<PGFunction> {
    match function {
        "triggered_change_notification" => Some(fc_triggered_change_notification as PGFunction),
        _ => None,
    }
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}

#[cfg(test)]
mod tests {
    use super::strcpy_quoted;

    fn q(s: &[u8], quote: u8) -> String {
        let mut v = Vec::new();
        strcpy_quoted(&mut v, s, quote);
        String::from_utf8(v).unwrap()
    }

    #[test]
    fn quoting_doubles() {
        assert_eq!(q(b"mytable", b'"'), "\"mytable\"");
        assert_eq!(q(b"a\"b", b'"'), "\"a\"\"b\"");
        assert_eq!(q(b"o'brien", b'\''), "'o''brien'");
    }

    // Payload shape from contrib/tcn expected/tcn.out:
    //   "mytable",I,"key"='1'
    #[test]
    fn payload_shape() {
        let mut p: Vec<u8> = Vec::new();
        strcpy_quoted(&mut p, b"mytable", b'"');
        p.push(b',');
        p.push(b'I');
        p.push(b',');
        strcpy_quoted(&mut p, b"key", b'"');
        p.push(b'=');
        strcpy_quoted(&mut p, b"1", b'\'');
        assert_eq!(String::from_utf8(p).unwrap(), "\"mytable\",I,\"key\"='1'");
    }
}
