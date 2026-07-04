use ::datum::array_build::ArrayBuildState;
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;

use crate::construct::construct_md_array;
use crate::foundation::varsize_any;

// initArrayResult: allocate a build state in the caller-owned context `mcx`
// (the C private subcontext is the caller's child bump arena — deferred.md).
// element storage triple resolved via lsyscache get_typlenbyvalalign.
pub fn init_array_result<'mcx>(
    mcx: Mcx<'mcx>,
    element_type: Oid,
    private_cxt: bool,
) -> PgResult<ArrayBuildState<'mcx>> {
    let mut astate = ArrayBuildState::new(mcx, element_type, private_cxt)?;
    let (typlen, typbyval, typalign) = ::lsyscache::get_typlenbyvalalign(element_type)?;
    astate.typlen = typlen;
    astate.typbyval = typbyval;
    astate.typalign = typalign as u8;
    Ok(astate)
}

// accumArrayResult: append one Datum, copying pass-by-ref payloads into the
// build context (datumCopy/detoast) so the caller's input is never damaged.
pub fn accum_array_result<'mcx>(
    mcx: Mcx<'mcx>,
    astate: Option<ArrayBuildState<'mcx>>,
    dvalue: Datum,
    disnull: bool,
    element_type: Oid,
) -> PgResult<ArrayBuildState<'mcx>> {
    let mut astate = match astate {
        Some(a) => {
            debug_assert_eq!(a.element_type, element_type);
            a
        }
        None => init_array_result(mcx, element_type, true)?,
    };

    let stored = if !disnull && !astate.typbyval {
        let p = dvalue.as_usize() as *const u8;
        let n = if astate.typlen == -1 {
            varsize_any(p)
        } else {
            astate.typlen as usize
        };
        // SAFETY: by-ref datum points at n live bytes.
        let bytes = unsafe { core::slice::from_raw_parts(p, n) };
        // C accumArrayResult PG_DETOAST_DATUMs varlena elements before copy.
        if astate.typlen == -1 && (bytes[0] == 0x01 || (bytes[0] & 0x03) == 0x02) {
            let flat = ::detoast_seams::detoast_attr::call(mcx, bytes)?;
            astate.copy_byref(&flat)?
        } else {
            astate.copy_byref(bytes)?
        }
    } else {
        dvalue
    };

    astate.dvalues.push(stored);
    astate.dnulls.push(disnull);
    astate.nelems += 1;
    Ok(astate)
}

// makeArrayResult: 1-D final result (empty array if no elements accumulated).
pub fn make_array_result<'mcx>(
    mcx: Mcx<'mcx>,
    astate: &ArrayBuildState<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    let ndims = if astate.nelems > 0 { 1 } else { 0 };
    let dims = [astate.nelems];
    let lbs = [1i32];
    make_md_array_result(mcx, astate, ndims, &dims, &lbs)
}

pub fn make_md_array_result<'mcx>(
    mcx: Mcx<'mcx>,
    astate: &ArrayBuildState<'mcx>,
    ndims: i32,
    dims: &[i32],
    lbs: &[i32],
) -> PgResult<PgVec<'mcx, u8>> {
    construct_md_array(
        mcx,
        astate.dvalues.as_slice(),
        Some(astate.dnulls.as_slice()),
        ndims,
        dims,
        lbs,
        astate.element_type,
        astate.typlen as i32,
        astate.typbyval,
        astate.typalign,
    )
}
