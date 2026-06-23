//! Inward-seam declarations for the `contrib-amcheck-verify-nbtree` unit
//! (`contrib/amcheck/verify_nbtree.c`): the two SQL-callable B-tree
//! verification entry points the function-call dispatcher reaches by Oid.
//!
//! `verify_nbtree.c` is a leaf consumer — no backend code calls *into* it
//! except the fmgr/SQL dispatch for these `PG_FUNCTION_INFO_V1` entry points.
//! The owning unit installs both from its `init_seams()`; until the unit lands
//! a call panics loudly.
//!
//! The fmgr `Datum`/`FunctionCallInfo` boundary is not reproduced here; the
//! seams take the already-extracted SQL arguments (`indrelid` + the boolean
//! options) and return `PgResult<()>` so the verifier's `ereport`s propagate.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `bt_index_check(index regclass, heapallindexed boolean, checkunique
    /// boolean)` (verify_nbtree.c): light-weight check under AccessShareLock.
    /// Defaults are `false` when the corresponding SQL argument is omitted.
    pub fn bt_index_check(
        indrelid: Oid,
        heapallindexed: bool,
        checkunique: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `bt_index_parent_check(index regclass, heapallindexed boolean,
    /// rootdescend boolean, checkunique boolean)` (verify_nbtree.c): the
    /// thorough check under ShareLock that also verifies parent/child downlink
    /// invariants. Defaults are `false` when the SQL argument is omitted.
    pub fn bt_index_parent_check(
        indrelid: Oid,
        heapallindexed: bool,
        rootdescend: bool,
        checkunique: bool,
    ) -> PgResult<()>
);
