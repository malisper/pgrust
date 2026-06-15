//! Seam declarations for the `backend-parser-driver` unit
//! (`parser/parser.c`'s `raw_parser`), the slice consumed by `parse_type.c`'s
//! `typeStringToTypeName`.
//!
//! `raw_parser(str, RAW_PARSE_TYPE_NAME)` returns a one-element `List` whose
//! single member is a `TypeName` node. This seam wraps that drive and the
//! `linitial_node(TypeName, ...)` extraction, handing back the decoded
//! `TypeName`. The real `raw_parser` lives in the driver crate, but the
//! grammar it drives (`base_yyparse`, `gram.y`) is not yet ported, so a call
//! reaches the still-unported grammar and panics there (mirror-PG-and-panic).
//!
//! The owning unit installs this from its `init_seams()`.

extern crate alloc;

use alloc::string::String;

use types_error::PgResult;
use types_parsenodes::TypeName;

seam_core::seam!(
    /// `raw_parser(str, RAW_PARSE_TYPE_NAME)` +
    /// `linitial_node(TypeName, raw_parsetree_list)` (parse_type.c
    /// `typeStringToTypeName`): parse a type-name string and return the single
    /// `TypeName` node it produces. A grammar/syntax error is raised inside the
    /// parser (with the `pts_error_callback` "invalid type name" errcontext)
    /// and propagates on `Err`; this seam never returns on a malformed string.
    pub fn raw_parse_type_name(str_: String) -> PgResult<TypeName>
);
