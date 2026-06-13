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

seam_core::seam!(
    /// The `local_relopts` tail of `index_opclass_options` (indexam.c),
    /// batched: `init_local_reloptions(&relopts, 0)` +
    /// `FunctionCall1(procinfo, PointerGetDatum(&relopts))` (the opclass's
    /// options-parsing support procedure registers its local options) +
    /// `build_local_reloptions(&relopts, attoptions, validate)` returning the
    /// serialized `bytea *` (or `None` for the C NULL). The fmgr invocation of
    /// `procinfo` and the option-validation `ereport(ERROR)`s are carried on
    /// `Err`; OOM from the built varlena too.
    pub fn index_build_local_reloptions(
        procinfo: types_core::fmgr::FmgrInfo,
        attoptions: types_datum::Datum,
        validate: bool,
    ) -> PgResult<Option<std::vec::Vec<u8>>>
);
