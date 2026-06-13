//! Seam declarations for the `backend-parser-parse-type` unit
//! (`parser/parse_type.c`): name-list rendering and type-name resolution.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;
use types_opclass::TypeName;

seam_core::seam!(
    /// `NameListToString(names)` (parse_type.c): render a possibly-qualified
    /// name (`List *` of `String`/`A_Star` nodes, here the name components)
    /// into a dotted string, allocated in `mcx` (C: `StringInfo` in the
    /// current context).
    pub fn name_list_to_string<'mcx>(
        mcx: Mcx<'mcx>,
        names: &[PgString<'_>],
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `typenameTypeId(NULL, typeName)` (parse_type.c): resolve a `TypeName`
    /// to its type OID, raising if the type does not exist or is only a shell.
    /// `Err` carries that `ereport(ERROR)` surface.
    pub fn typename_type_id(type_name: &TypeName) -> PgResult<Oid>
);

seam_core::seam!(
    /// `TypeNameToString(typeName)` (parse_type.c): the type name rendered for
    /// an error message, palloc'd in the caller's current context (`mcx`).
    /// `Err` includes OOM from the construction.
    pub fn typename_to_string<'mcx>(
        mcx: Mcx<'mcx>,
        type_name: &TypeName,
    ) -> PgResult<PgString<'mcx>>
);
