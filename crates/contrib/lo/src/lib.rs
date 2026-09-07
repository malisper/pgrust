//! `contrib/lo` — the `lo_manage` trigger unlinking a managed column's large
//! object on row delete or column change (the `lo` type is a SQL-side domain).

use datum::Datum;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_tuple::htup::FirstLowInvalidHeapAttributeNumber as FLIA;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_trigger::{TRIGGER_FIRED_BY_DELETE, TRIGGER_FIRED_BY_UPDATE, TRIGGER_FIRED_FOR_ROW};
use types_trigger_call::trigger_data_from_fcinfo;
use types_tuple::HeapTupleData;

const LIBRARY: &str = "lo";

#[track_caller]
#[cold]
#[inline(never)]
fn internal_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg))
}

// C's atooid (postgres_ext.h:43): (Oid) strtoul(x, NULL, 10).  glibc
// strtoul skips C-locale isspace, takes one optional '+'/'-' (a negative
// value wraps modulo 2^64), returns ULONG_MAX when the magnitude overflows
// unsigned long, and ignores trailing garbage; the Oid cast then truncates.
fn atooid(s: &[u8]) -> Oid {
    let mut i = 0;
    while i < s.len() && matches!(s[i], b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r') {
        i += 1;
    }
    let mut neg = false;
    match s.get(i) {
        Some(b'-') => {
            neg = true;
            i += 1;
        }
        Some(b'+') => i += 1,
        _ => {}
    }
    let mut acc: u64 = 0;
    let mut overflow = false;
    while i < s.len() && s[i].is_ascii_digit() {
        if !overflow {
            match acc.checked_mul(10).and_then(|v| v.checked_add(u64::from(s[i] - b'0'))) {
                Some(v) => acc = v,
                None => overflow = true,
            }
        }
        i += 1;
    }
    let val = if overflow {
        u64::MAX
    } else if neg {
        acc.wrapping_neg()
    } else {
        acc
    };
    val as Oid
}

fn fc_lo_manage(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the trigger call machinery keeps the TriggerData live for the call.
    let Some(td) = (unsafe { trigger_data_from_fcinfo(fcinfo) }) else {
        return Err(internal_err("lo_manage: not fired by trigger manager".to_string()));
    };
    let tgname = td.tg_trigger.tgname.as_str();
    if !TRIGGER_FIRED_FOR_ROW(td.tg_event) {
        return Err(internal_err(format!("{tgname}: must be fired for row")));
    }

    let newtuple = td.tg_newtuple;
    let trigtuple = td.tg_trigtuple.expect("row trigger has an original tuple");
    let tupdesc = td.tg_relation.descr();
    let args = &td.tg_trigger.tgargs;

    if args.is_empty() {
        return Err(internal_err(format!(
            "{tgname}: no column name provided in the trigger definition"
        )));
    }

    let rettuple = if TRIGGER_FIRED_BY_UPDATE(td.tg_event) {
        newtuple.expect("UPDATE row trigger has a new tuple")
    } else {
        trigtuple
    };

    let isdelete = TRIGGER_FIRED_BY_DELETE(td.tg_event);

    let attnum = spi::SPI_fnumber(tupdesc, args[0].as_str());
    if attnum <= 0 {
        return Err(internal_err(format!(
            "{tgname}: column \"{}\" does not exist",
            args[0].as_str()
        )));
    }

    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: live tuple per the trigger call contract.
    let trigtuple_ref: &HeapTupleData<'_> = unsafe { trigtuple.as_ref() };

    if let Some(newtuple) = newtuple {
        let updated = td.tg_updatedcols != 0 && {
            // SAFETY: non-NULL tg_updatedcols is a live Bitmapset (attnums off FLIA).
            let cols = unsafe { &*(td.tg_updatedcols as *const types_nodes::Bitmapset<'_>) };
            cols.is_member(attnum - FLIA)
        };
        if updated {
            // SAFETY: live tuple per the trigger call contract.
            let newtuple_ref: &HeapTupleData<'_> = unsafe { newtuple.as_ref() };
            let orig = spi::SPI_getvalue(mcx, trigtuple_ref, tupdesc, attnum)?;
            let newv = spi::SPI_getvalue(mcx, newtuple_ref, tupdesc, attnum)?;
            if let Some(orig) = orig {
                if newv != Some(orig) {
                    be_fsstubs::be_lo_unlink(mcx, atooid(orig))?;
                }
            }
        }
    }

    if isdelete {
        let orig = spi::SPI_getvalue(mcx, trigtuple_ref, tupdesc, attnum)?;
        if let Some(orig) = orig {
            be_fsstubs::be_lo_unlink(mcx, atooid(orig))?;
        }
    }

    Ok(Datum::from_usize(rettuple.as_ptr() as usize))
}

fn lookup(function: &str) -> Option<PGFunction> {
    match function {
        "lo_manage" => Some(fc_lo_manage as PGFunction),
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
    use super::atooid;

    // postgres_ext.h:43 — atooid(x) is (Oid) strtoul(x, NULL, 10): C-locale
    // isspace skip, one optional sign, ULONG_MAX on overflow, then the Oid
    // truncation.  lo.c:87/104 feed it SPI_getvalue() text of any column type.
    #[test]
    fn atooid_has_strtoul_semantics() {
        assert_eq!(atooid(b"43210"), 43210);
        assert_eq!(atooid(b" \t43210"), 43210);
        assert_eq!(atooid(b"+43210"), 43210);
        assert_eq!(atooid(b"\n\x0b\x0c\r43210"), 43210);
        assert_eq!(atooid(b" +43210xyz"), 43210);
        assert_eq!(atooid(b"-1"), 4294967295);
        assert_eq!(atooid(b"-4294967296"), 0);
        assert_eq!(atooid(b"4294967296"), 0);
        assert_eq!(atooid(b"99999999999999999999999"), 4294967295);
        assert_eq!(atooid(b"-99999999999999999999999"), 4294967295);
        assert_eq!(atooid(b"abc"), 0);
        assert_eq!(atooid(b""), 0);
        assert_eq!(atooid(b"+"), 0);
        assert_eq!(atooid(b"- 5"), 0);
    }
}
