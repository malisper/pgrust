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
use types_reloptions::{local_relopts, AttributeOpts, TableSpaceOpts};

seam_core::seam!(
    /// `attribute_reloptions(reloptions, validate)` (reloptions.c).
    pub fn attribute_reloptions(reloptions: &[u8], validate: bool) -> PgResult<AttributeOpts>
);

seam_core::seam!(
    /// `tablespace_reloptions(reloptions, validate)` (reloptions.c).
    pub fn tablespace_reloptions(reloptions: &[u8], validate: bool) -> PgResult<TableSpaceOpts>
);

seam_core::seam!(
    /// `init_local_reloptions(relopts, relopt_struct_size)` (reloptions.c) —
    /// initialize a `local_relopts` for an index-AM `options` support function.
    pub fn init_local_reloptions(relopts: &mut local_relopts, relopt_struct_size: usize)
);

seam_core::seam!(
    /// `add_local_int_reloption(relopts, name, desc, default, min, max, offset)`
    /// (reloptions.c) — register an integer local reloption.
    pub fn add_local_int_reloption(
        relopts: &mut local_relopts,
        name: &str,
        desc: Option<&str>,
        default_val: i32,
        min_val: i32,
        max_val: i32,
        offset: i32,
    )
);
