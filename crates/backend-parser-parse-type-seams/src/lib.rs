//! Seam declarations for the `backend-parser-parse-type` unit
//! (`parser/parse_type.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsestmt::TypeName;

seam_core::seam!(
    /// `typenameTypeId(pstate, typeName)` (parse_type.c) — resolve a
    /// `TypeName` to its type OID. `source_text` is `pstate->p_sourcetext`
    /// (used for error positions). Can `ereport(ERROR)` (unknown type).
    pub fn typename_type_id<'mcx>(
        source_text: &str,
        type_name: &TypeName<'mcx>,
    ) -> PgResult<Oid>
);
