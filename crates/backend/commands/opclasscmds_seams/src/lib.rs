//! Seam declarations for the `backend-commands-opclasscmds` unit
//! (`commands/opclasscmds.c`): the operator-class / operator-family name-list
//! lookups the `get_object_address_opcf` resolution path needs.
//!
//! The owning unit installs these from its `init_seams()`. The full-name-list
//! `get_opclass_oid(amid, name, missing_ok)` / `get_opfamily_oid(...)` are the
//! `objectaddress.c` C signatures — NOT the lower-level syscache `(amid,
//! opcname, nsp)` probes (those are a different lookup with their own
//! consumers in `backend-utils-cache-syscache-seams`). The qualified name list
//! crosses as the repo-universal `&[&str]` (the installer rebuilds the owner's
//! `&[StringNode]` image).

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `get_opclass_oid(am_id, opclassname, missing_ok)` (opclasscmds.c):
    /// search-path-aware lookup of the operator class named `opclassname`
    /// within access method `am_id`. With `missing_ok = false` a miss raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`); else `InvalidOid`.
    pub fn get_opclass_oid(
        mcx: Mcx<'_>,
        am_id: Oid,
        opclassname: &[&str],
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_opfamily_oid(am_id, opfamilyname, missing_ok)` (opclasscmds.c):
    /// search-path-aware lookup of the operator family named `opfamilyname`
    /// within access method `am_id`. With `missing_ok = false` a miss raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`); else `InvalidOid`.
    pub fn get_opfamily_oid(
        mcx: Mcx<'_>,
        am_id: Oid,
        opfamilyname: &[&str],
        missing_ok: bool,
    ) -> PgResult<Oid>
);
