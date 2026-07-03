use crate::fcinfo::FunctionCallInfoBaseData as Fcinfo;
use ::datum::Datum;
use types_error::PgResult;

// SQL OVERLAPS frame; non-strict, argument normalization per spec (nulls
// swapped toward ts, ordering swapped so ts <= te).
pub fn overlaps_common(
    fcinfo: &mut Fcinfo,
    gt: impl Fn(&Fcinfo, usize, usize) -> bool,
) -> PgResult<Datum> {
    let mut s1 = 0usize;
    let mut e1 = 1usize;
    let mut s2 = 2usize;
    let mut e2 = 3usize;
    let mut e1_null = fcinfo.argisnull(e1);
    let mut e2_null = fcinfo.argisnull(e2);

    if fcinfo.argisnull(s1) {
        if e1_null {
            return Ok(fcinfo.return_null());
        }
        core::mem::swap(&mut s1, &mut e1);
        e1_null = true;
    } else if !e1_null && gt(fcinfo, s1, e1) {
        core::mem::swap(&mut s1, &mut e1);
    }

    if fcinfo.argisnull(s2) {
        if e2_null {
            return Ok(fcinfo.return_null());
        }
        core::mem::swap(&mut s2, &mut e2);
        e2_null = true;
    } else if !e2_null && gt(fcinfo, s2, e2) {
        core::mem::swap(&mut s2, &mut e2);
    }

    if gt(fcinfo, s1, s2) {
        if e2_null {
            return Ok(fcinfo.return_null());
        }
        if gt(fcinfo, e2, s1) {
            return Ok(Datum::from_bool(true));
        }
        if e1_null {
            return Ok(fcinfo.return_null());
        }
        Ok(Datum::from_bool(false))
    } else if gt(fcinfo, s2, s1) {
        if e1_null {
            return Ok(fcinfo.return_null());
        }
        if gt(fcinfo, e1, s2) {
            return Ok(Datum::from_bool(true));
        }
        if e2_null {
            return Ok(fcinfo.return_null());
        }
        Ok(Datum::from_bool(false))
    } else {
        if e1_null || e2_null {
            return Ok(fcinfo.return_null());
        }
        Ok(Datum::from_bool(true))
    }
}
