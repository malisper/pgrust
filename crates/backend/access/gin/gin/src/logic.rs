//! ginlogic.c: bool/tri-state consistent dispatch. The closed opclass set
//! provides both consistent and triConsistent, so the shim arms exist only
//! for the EVERYTHING key (true fns). `tmp` is C's so->tempCtx, reset after
//! every call.

use ::gin_vocab::*;
use ::mcx::MemoryContext;
use ::types_error::PgResult;

use crate::opclass;

/// directBoolConsistentFn / trueConsistentFn dispatch.
pub(crate) fn bool_consistent(
    tmp: &mut MemoryContext,
    state: &GinState,
    key: &mut GinScanKeyData,
) -> PgResult<bool> {
    if key.searchMode == GIN_SEARCH_MODE_EVERYTHING {
        key.recheckCurItem = false;
        return Ok(true);
    }
    // Force recheck unless the consistent fn says otherwise (C initializes
    // *recheck = true before the call).
    key.recheckCurItem = true;
    let mut recheck = true;
    let res = opclass::consistent(
        tmp.mcx(),
        state.col(key.attnum),
        key.entryRes.as_slice(),
        key.strategy,
        key.query,
        key.nuserentries as usize,
        key.queryValues.as_slice(),
        key.queryCategories.as_slice(),
        key.jspOps.as_slice(),
        key.mapItemOperand.as_slice(),
        key.trgmGraph.as_mut(),
        &mut recheck,
    )?;
    key.recheckCurItem = recheck;
    tmp.reset();
    Ok(res)
}

/// directTriConsistentFn / trueTriConsistentFn dispatch.
pub(crate) fn tri_consistent(
    tmp: &mut MemoryContext,
    state: &GinState,
    key: &mut GinScanKeyData,
) -> PgResult<GinTernaryValue> {
    if key.searchMode == GIN_SEARCH_MODE_EVERYTHING {
        return Ok(GIN_TRUE);
    }
    let res = opclass::tri_consistent(
        tmp.mcx(),
        state.col(key.attnum),
        key.entryRes.as_slice(),
        key.strategy,
        key.query,
        key.nuserentries as usize,
        key.queryValues.as_slice(),
        key.queryCategories.as_slice(),
        key.jspOps.as_slice(),
        key.mapItemOperand.as_slice(),
        key.trgmGraph.as_mut(),
    )?;
    tmp.reset();
    Ok(res)
}
