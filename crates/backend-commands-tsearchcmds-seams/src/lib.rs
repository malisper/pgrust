//! Seam declarations for the `backend-commands-tsearchcmds` unit
//! (`commands/tsearchcmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_cache::DefElemString;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `deserialize_deflist(txt)` (tsearchcmds.c): build a `List` of
    /// `DefElem` from a stored `text` datum. `txt` is the verbatim varlena
    /// bytes a `SysCacheGetAttr` read produced (including the header,
    /// possibly compressed); the owner performs the C `TextDatumGetCString`
    /// detoast + conversion. Every produced `DefElem` has a `String`-node
    /// argument, so the list crosses as typed rows allocated in `mcx`. `Err`
    /// carries the C `ereport(ERROR, "invalid deserialize_deflist syntax")`
    /// and OOM.
    pub fn deserialize_deflist<'mcx>(
        mcx: Mcx<'mcx>,
        txt: &[u8],
    ) -> PgResult<PgVec<'mcx, DefElemString<'mcx>>>
);

seam_core::seam!(
    /// `RemoveTSConfigurationById(cfgId)` (commands/tsearchcmds.c): the
    /// per-class `OCLASS_TSCONFIG` drop handler dependency.c's `doDeletion`
    /// invokes for a `pg_ts_config` object. Removes the text-search
    /// configuration's catalog rows. Can `ereport(ERROR)`, carried on `Err`.
    pub fn RemoveTSConfigurationById(cfgId: Oid) -> PgResult<()>
);
