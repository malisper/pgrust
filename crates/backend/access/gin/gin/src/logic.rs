//! ginlogic.c: bool/tri-state consistent dispatch. The closed opclass set
//! provides both consistent and triConsistent, so the shim arms exist only
//! for the EVERYTHING key (true fns); the C shim fns are kept for parity with
//! opclasses lacking one of the pair (unreachable today, loud).

use ::gin_vocab::*;

use crate::opclass;

/// directBoolConsistentFn / trueConsistentFn dispatch.
pub(crate) fn bool_consistent(state: &GinState, key: &mut GinScanKeyData) -> bool {
    if key.searchMode == GIN_SEARCH_MODE_EVERYTHING {
        key.recheckCurItem = false;
        return true;
    }
    // Force recheck unless the consistent fn says otherwise (C initializes
    // *recheck = true before the call).
    key.recheckCurItem = true;
    let mut recheck = true;
    let res = opclass::consistent(
        state,
        key.entryRes.as_slice(),
        key.strategy,
        key.query,
        key.nuserentries as usize,
        key.queryValues.as_slice(),
        key.queryCategories.as_slice(),
        &mut recheck,
    );
    key.recheckCurItem = recheck;
    res
}

/// directTriConsistentFn / trueTriConsistentFn dispatch.
pub(crate) fn tri_consistent(state: &GinState, key: &mut GinScanKeyData) -> GinTernaryValue {
    if key.searchMode == GIN_SEARCH_MODE_EVERYTHING {
        return GIN_TRUE;
    }
    opclass::tri_consistent(
        state,
        key.entryRes.as_slice(),
        key.strategy,
        key.query,
        key.nuserentries as usize,
        key.queryValues.as_slice(),
        key.queryCategories.as_slice(),
    )
}
