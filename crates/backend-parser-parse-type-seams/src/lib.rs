//! Seam declarations for the `backend-parser-parse-type` unit
//! (`parser/parse_type.c`): type-name string parsing.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `parseTypeString(str, &typeid, &typmod, escontext)` (parse_type.c):
    /// invoke the full grammar to resolve a type name (handling array syntax,
    /// `DOUBLE PRECISION`, decoration, etc.) to its type OID and typmod.
    ///
    /// The owned model folds the C out-parameters and boolean return into the
    /// result: `Ok(Some((typeid, typmod)))` is the C `true` return, and
    /// `Ok(None)` is the C `false` return — only reachable when a soft-error
    /// `ErrorSaveContext` was supplied (modeled by `soft = true`). With
    /// `soft = false` (no soft-error context) a bad type name propagates as a
    /// hard error on `Err`.
    pub fn parse_type_string(
        string: &str,
        soft: bool,
    ) -> PgResult<Option<(Oid, i32)>>
);
