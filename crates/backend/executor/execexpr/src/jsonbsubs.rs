//! jsonbsubs.c exec half: JsonbSubWorkspace + check_subscripts (the INT4→text
//! conversion and expectArray determination); fetch/assign primitives live in
//! adt_jsonb::subs.

use core::ptr::NonNull;

use datum::{Datum, NullableDatum};
use types_core::{Oid, INT4OID};
use types_error::{PgError, PgResult, ERRCODE_NULL_VALUE_NOT_ALLOWED};

use crate::arrayops::{res_mcx, ResMcx};

pub struct JsonbSbsState {
    pub isassignment: bool,
    pub expect_array: bool,
    pub nupper: u32,
    pub upperindex: NonNull<NullableDatum>,
    pub index_oids: NonNull<Oid>,
    pub index: NonNull<Datum>,
    pub replace: NullableDatum,
    pub resmcx: ResMcx,
}

#[track_caller]
#[cold]
fn null_subscript_error() -> Box<PgError> {
    Box::new(
        PgError::error("jsonb subscript in assignment must not be null".to_string())
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

pub fn check_subscripts(st: &mut JsonbSbsState) -> PgResult<bool> {
    let n = st.nupper as usize;
    // SAFETY: compile allocated n slots in each array; single-threaded.
    let upper = unsafe { core::slice::from_raw_parts(st.upperindex.as_ptr(), n) };
    let oids = unsafe { core::slice::from_raw_parts(st.index_oids.as_ptr(), n) };
    let index = unsafe { core::slice::from_raw_parts_mut(st.index.as_ptr(), n) };

    if n > 0 && !upper[0].isnull && oids[0] == INT4OID {
        st.expect_array = true;
    }

    let mcx = res_mcx(&st.resmcx);
    for i in 0..n {
        if upper[i].isnull {
            if st.isassignment {
                return Err(null_subscript_error());
            }
            return Ok(false);
        }
        if oids[i] == INT4OID {
            let mut buf = [0u8; 12];
            let len = numutils::pg_ltoa(upper[i].value.as_i32(), &mut buf);
            let t = varlena::cstring_to_text(mcx, &buf[..len])?;
            index[i] = types_fmgr::varlena_result(t);
        } else {
            index[i] = upper[i].value;
        }
    }
    Ok(true)
}

pub fn fetch(st: &mut JsonbSbsState, cur: NullableDatum) -> PgResult<NullableDatum> {
    debug_assert!(!cur.isnull);
    let mcx = res_mcx(&st.resmcx);
    // SAFETY: as check_subscripts.
    let index = unsafe { core::slice::from_raw_parts(st.index.as_ptr(), st.nupper as usize) };
    adt_jsonb::subs::subscript_fetch(mcx, cur.value, index)
}

pub fn assign(st: &mut JsonbSbsState, cur: NullableDatum) -> PgResult<NullableDatum> {
    let mcx = res_mcx(&st.resmcx);
    // SAFETY: as check_subscripts.
    let index = unsafe { core::slice::from_raw_parts(st.index.as_ptr(), st.nupper as usize) };
    let d = adt_jsonb::subs::subscript_assign(mcx, cur, st.expect_array, index, st.replace)?;
    Ok(NullableDatum { value: d, isnull: false })
}
