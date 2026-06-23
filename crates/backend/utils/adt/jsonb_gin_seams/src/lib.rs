//! Seam declarations for the jsonb GIN opclass support procedures
//! (`utils/adt/jsonb_gin.c`) — the `jsonb_ops` and `jsonb_path_ops` opclasses.
//!
//! The GIN AM dispatches its opclass support procedures (`compare`,
//! `extractValue`, `extractQuery`, `consistent`, `triConsistent`) by OID. Unlike
//! GiST — which has a single by-OID dispatcher (`backend-access-gist-proc`) over
//! `backend-access-gist-dispatch-seams` — the GIN core's existing dispatch seams
//! (`backend-access-gin-ginutil-seams::gin_extract_value` / `gin_extract_query` /
//! `gin_compare_entries`, `backend-access-gin-core-probe-seams::gin_consistent_*`)
//! take a runtime `FmgrInfo` carrying `fn_oid` and canonical `Datum`s, and no
//! production code installs them yet (a GIN-proc by-OID dispatcher, the analog of
//! `backend-access-gist-proc`, has not been built). When that dispatcher lands it
//! will route the jsonb GIN support-proc OIDs
//!
//!  * `gin_compare_jsonb`            = 3480 (amprocnum 1, jsonb_ops)
//!  * `gin_extract_jsonb`            = 3482 (amprocnum 2, jsonb_ops)
//!  * `gin_extract_jsonb_query`      = 3483 (amprocnum 3, jsonb_ops)
//!  * `gin_consistent_jsonb`         = 3484 (amprocnum 4, jsonb_ops)
//!  * `gin_triconsistent_jsonb`      = 3488 (amprocnum 6, jsonb_ops)
//!  * `gin_extract_jsonb_path`       = 3485 (amprocnum 2, jsonb_path_ops)
//!  * `gin_extract_jsonb_query_path` = 3486 (amprocnum 3, jsonb_path_ops)
//!  * `gin_consistent_jsonb_path`    = 3487 (amprocnum 4, jsonb_path_ops)
//!  * `gin_triconsistent_jsonb_path` = 3489 (amprocnum 6, jsonb_path_ops)
//!
//! to these typed bodies, marshaling the `Datum`s (detoasting jsonb / jsonpath
//! varlenas into their root bytes and wrapping the produced key bytes back into
//! GIN-key Datums). The opaque GIN `extra_data[0]` round-trip is the
//! [`::types_jsonb::jsonb_gin::JsonPathGinNode`] expression tree.
//!
//! `backend-utils-adt-jsonb-gin` is the single installer of these seams (it
//! never calls them); they panic loudly until that owner's `init_seams()` runs
//! — the mirror-PG-and-panic contract for an unwired genuine external.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_jsonb::jsonb_gin::{GinJsonbQuery, GinQueryExtraction, JsonPathGinNode};
use ::types_jsonb::jsonb::GinTernaryValue;

seam_core::seam!(
    /// `gin_compare_jsonb` (jsonb_gin.c:203, OID 3480) — compare two GIN keys
    /// (`text` payloads) as `bttextcmp` does but always under the C collation,
    /// which reduces to a plain unsigned byte compare. `a` / `b` are the
    /// detoasted payload bytes (`VARDATA_ANY` / `VARSIZE_ANY_EXHDR`). Returns
    /// the `int32` comparison sign.
    pub fn gin_compare_jsonb(a: &[u8], b: &[u8]) -> i32
);

seam_core::seam!(
    /// `gin_extract_jsonb` (jsonb_gin.c:228, OID 3482) — the `jsonb_ops`
    /// `extractValue` proc. `jb_root` is the jsonb argument's root container
    /// bytes. Returns the assembled GIN key payloads (C `*nentries` is the
    /// vector length; an empty root returns an empty vector).
    pub fn gin_extract_jsonb<'mcx>(
        mcx: Mcx<'mcx>,
        jb_root: &[u8],
    ) -> PgResult<Vec<Vec<u8>>>
);

seam_core::seam!(
    /// `gin_extract_jsonb_query` (jsonb_gin.c:847, OID 3483) — the `jsonb_ops`
    /// `extractQuery` proc.
    pub fn gin_extract_jsonb_query<'mcx, 'a>(
        mcx: Mcx<'mcx>,
        query: GinJsonbQuery<'a>,
        strategy: u16,
    ) -> PgResult<GinQueryExtraction>
);

seam_core::seam!(
    /// `gin_consistent_jsonb` (jsonb_gin.c:928, OID 3484) — the `jsonb_ops`
    /// boolean `consistent` proc. `check` is the per-entry result vector,
    /// `extra_data` is the root expression node (jsonpath strategies only).
    /// Returns `(res, recheck)`.
    pub fn gin_consistent_jsonb(
        check: &[bool],
        strategy: u16,
        nkeys: i32,
        extra_data: Option<&JsonPathGinNode>,
    ) -> PgResult<(bool, bool)>
);

seam_core::seam!(
    /// `gin_triconsistent_jsonb` (jsonb_gin.c:1012, OID 3488) — the `jsonb_ops`
    /// ternary `triConsistent` proc. Never returns `GIN_TRUE`.
    pub fn gin_triconsistent_jsonb(
        check: &[GinTernaryValue],
        strategy: u16,
        nkeys: i32,
        extra_data: Option<&JsonPathGinNode>,
    ) -> PgResult<GinTernaryValue>
);

seam_core::seam!(
    /// `gin_extract_jsonb_path` (jsonb_gin.c:1089, OID 3485) — the
    /// `jsonb_path_ops` `extractValue` proc. The GIN keys are `uint32` hashes
    /// (one per JSON value, with the leading key(s) folded into each hash).
    pub fn gin_extract_jsonb_path(jb_root: &[u8]) -> PgResult<Vec<Vec<u8>>>
);

seam_core::seam!(
    /// `gin_extract_jsonb_query_path` (jsonb_gin.c:1179, OID 3486) — the
    /// `jsonb_path_ops` `extractQuery` proc.
    pub fn gin_extract_jsonb_query_path<'mcx, 'a>(
        mcx: Mcx<'mcx>,
        query: GinJsonbQuery<'a>,
        strategy: u16,
    ) -> PgResult<GinQueryExtraction>
);

seam_core::seam!(
    /// `gin_consistent_jsonb_path` (jsonb_gin.c:1219, OID 3487) — the
    /// `jsonb_path_ops` boolean `consistent` proc. Returns `(res, recheck)`.
    pub fn gin_consistent_jsonb_path(
        check: &[bool],
        strategy: u16,
        nkeys: i32,
        extra_data: Option<&JsonPathGinNode>,
    ) -> PgResult<(bool, bool)>
);

seam_core::seam!(
    /// `gin_triconsistent_jsonb_path` (jsonb_gin.c:1271, OID 3489) — the
    /// `jsonb_path_ops` ternary `triConsistent` proc.
    pub fn gin_triconsistent_jsonb_path(
        check: &[GinTernaryValue],
        strategy: u16,
        nkeys: i32,
        extra_data: Option<&JsonPathGinNode>,
    ) -> PgResult<GinTernaryValue>
);
