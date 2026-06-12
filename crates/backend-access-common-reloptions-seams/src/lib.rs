//! Seam declarations for the `backend-access-common-reloptions` unit
//! (`access/common/reloptions.c`), the relation-options parser.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! Each parser takes the raw `text[]` options datum (the verbatim varlena
//! array bytes a `SysCacheGetAttr` read produced) and returns the parsed
//! fixed-size options struct by value (C: a freshly palloc'd `bytea *`).
//! `Err` carries the C `ereport(ERROR)` surface of option validation.

use types_error::PgResult;
use types_reloptions::{AttributeOpts, TableSpaceOpts};

seam_core::seam!(
    /// `attribute_reloptions(reloptions, validate)` (reloptions.c).
    pub fn attribute_reloptions(reloptions: &[u8], validate: bool) -> PgResult<AttributeOpts>
);

seam_core::seam!(
    /// `tablespace_reloptions(reloptions, validate)` (reloptions.c).
    pub fn tablespace_reloptions(reloptions: &[u8], validate: bool) -> PgResult<TableSpaceOpts>
);
