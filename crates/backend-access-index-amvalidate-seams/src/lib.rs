//! Seam declarations for the `backend-access-index-amvalidate` unit
//! (`access/index/amvalidate.c`) — the signature-check / opclass-lookup
//! library shared by every AM opclass validator.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_amvalidate::backend_access_index_amvalidate::{AmopRow, AmprocRow, OpFamilyOpFuncGroup};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `check_amproc_signature(funcid, restype, exact, minargs, maxargs, ...)`
    /// (amvalidate.c): validate the signature of an opclass support function.
    /// The C signature is variadic (exactly `maxargs` OIDs); here the OIDs are
    /// a slice. `PgResult` carries the C `elog(ERROR, "cache lookup failed for
    /// function %u")` on a `pg_proc` cache miss.
    pub fn check_amproc_signature(
        funcid: Oid,
        restype: Oid,
        exact: bool,
        minargs: i32,
        maxargs: i32,
        argtypes: &[Oid],
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `check_amoptsproc_signature(funcid)` (amvalidate.c): validate the
    /// signature of an opclass options support function (`void(internal)`).
    pub fn check_amoptsproc_signature(funcid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `check_amop_signature(opno, restype, lefttype, righttype)`
    /// (amvalidate.c): validate the signature of an opclass operator.
    /// `PgResult` carries the C `elog(ERROR, "cache lookup failed for operator
    /// %u")` on a `pg_operator` cache miss.
    pub fn check_amop_signature(
        opno: Oid,
        restype: Oid,
        lefttype: Oid,
        righttype: Oid,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `opclass_for_family_datatype(amoid, opfamilyoid, datatypeoid)`
    /// (amvalidate.c): the OID of the opclass belonging to the opfamily and
    /// accepting the type as input type, or `InvalidOid` if none.
    pub fn opclass_for_family_datatype(amoid: Oid, opfamilyoid: Oid, datatypeoid: Oid) -> Oid
);

seam_core::seam!(
    /// `identify_opfamily_groups(oprlist, proclist)` (amvalidate.c): group the
    /// opfamily's `AMOPSTRATEGY`/`AMPROCNUM` member rows by datatype pair and
    /// set the operator/function presence bitmaps.
    pub fn identify_opfamily_groups(
        oprlist: &[AmopRow],
        proclist: &[AmprocRow],
    ) -> Vec<OpFamilyOpFuncGroup>
);
