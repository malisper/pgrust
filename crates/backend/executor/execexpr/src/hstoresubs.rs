//! hstore_subs.c exec half: single text subscript, fetch/assign bodies
//! reached through hstore_subs_seams (contrib crate installs them).

use datum::{Datum, NullableDatum};
use types_error::{PgError, PgResult, ERRCODE_NULL_VALUE_NOT_ALLOWED};

use crate::arrayops::{res_mcx, ResMcx};

pub struct HstoreSbsState {
    pub isassignment: bool,
    pub subscript: NullableDatum,
    pub replace: NullableDatum,
    pub resmcx: ResMcx,
}

#[track_caller]
#[cold]
fn null_subscript_error() -> Box<PgError> {
    Box::new(
        PgError::error("hstore subscript in assignment must not be null".to_string())
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

pub fn fetch(st: &mut HstoreSbsState, cur: NullableDatum) -> PgResult<NullableDatum> {
    debug_assert!(!cur.isnull);
    if st.subscript.isnull {
        return Ok(NullableDatum::null());
    }
    let mcx = res_mcx(&st.resmcx);
    hstore_subs_seams::hstore_subs_fetch::call(mcx, cur.value, st.subscript.value)
}

pub fn assign(st: &mut HstoreSbsState, cur: NullableDatum) -> PgResult<NullableDatum> {
    if st.subscript.isnull {
        return Err(null_subscript_error());
    }
    let mcx = res_mcx(&st.resmcx);
    let d = hstore_subs_seams::hstore_subs_assign::call(mcx, cur, st.subscript.value, st.replace)?;
    Ok(NullableDatum { value: d, isnull: false })
}
