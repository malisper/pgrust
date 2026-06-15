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

seam_core::seam!(
    /// The `local_relopts` tail of `index_opclass_options` (indexam.c),
    /// batched: `init_local_reloptions(&relopts, 0)` +
    /// `FunctionCall1(procinfo, PointerGetDatum(&relopts))` (the opclass's
    /// options-parsing support procedure registers its local options) +
    /// `build_local_reloptions(&relopts, attoptions, validate)` returning the
    /// serialized `bytea *` (or `None` for the C NULL). The fmgr invocation of
    /// `procinfo` and the option-validation `ereport(ERROR)`s are carried on
    /// `Err`; OOM from the built varlena too.
    pub fn index_build_local_reloptions<'mcx>(
        procinfo: types_core::fmgr::FmgrInfo,
        attoptions: types_tuple::Datum<'mcx>,
        validate: bool,
    ) -> PgResult<Option<std::vec::Vec<u8>>>
);

seam_core::seam!(
    /// `build_reloptions(reloptions, validate, RELOPT_KIND_HASH,
    /// sizeof(HashOptions), tab, lengthof(tab))` — the hash AM's `hashoptions`
    /// (hashutil.c), whose only option is `fillfactor` at
    /// `offsetof(HashOptions, fillfactor)`. The relopt-table layout and parse
    /// are the reloptions owner's; the seam takes the raw `reloptions` varlena
    /// bytes (`None` for a NULL datum) and returns the serialized `HashOptions`
    /// `bytea` (`None` when no options apply). `Err` carries the validation
    /// `ereport(ERROR)`s.
    pub fn build_reloptions_hash(
        reloptions: Option<&[u8]>,
        validate: bool,
    ) -> PgResult<Option<std::vec::Vec<u8>>>
);

seam_core::seam!(
    /// `build_reloptions(reloptions, validate, RELOPT_KIND_SPGIST,
    /// sizeof(SpGistOptions), tab, lengthof(tab))` — the SP-GiST AM's
    /// `spgoptions` (spgutils.c), whose only option is `fillfactor` at
    /// `offsetof(SpGistOptions, fillfactor)`. The relopt-table layout and parse
    /// are the reloptions owner's; the seam takes the raw `reloptions` varlena
    /// bytes (`None` for a NULL datum) and returns the serialized
    /// `SpGistOptions` `bytea` (`None` when no options apply). `Err` carries the
    /// validation `ereport(ERROR)`s.
    pub fn build_reloptions_spgist(
        reloptions: Option<&[u8]>,
        validate: bool,
    ) -> PgResult<Option<std::vec::Vec<u8>>>
);

seam_core::seam!(
    /// `build_reloptions(reloptions, validate, RELOPT_KIND_GIST,
    /// sizeof(GiSTOptions), tab, lengthof(tab))` — the GiST AM's `gistoptions`
    /// (gistutil.c), whose options are `fillfactor` (INT) at
    /// `offsetof(GiSTOptions, fillfactor)` and `buffering` (ENUM) at
    /// `offsetof(GiSTOptions, buffering_mode)`. The relopt-table layout and
    /// parse are the reloptions owner's; the seam takes the raw `reloptions`
    /// varlena bytes (`None` for a NULL datum) and returns the serialized
    /// `GiSTOptions` `bytea` (`None` when no options apply). `Err` carries the
    /// validation `ereport(ERROR)`s.
    pub fn build_reloptions_gist(
        reloptions: Option<&[u8]>,
        validate: bool,
    ) -> PgResult<Option<std::vec::Vec<u8>>>
);
