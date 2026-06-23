//! Seam declarations for the inet (`inet_ops`) GiST opclass support procedures
//! (`utils/adt/network_gist.c`).
//!
//! The GiST AM dispatches its opclass support procedures by OID through
//! `backend-access-gist-dispatch-seams`; `backend-access-gist-proc` is the
//! single installer of that by-OID dispatch and routes the inet support-proc
//! OIDs (`inet_gist_consistent` = 3553, `inet_gist_union` = 3554,
//! `inet_gist_compress` = 3555, `inet_gist_penalty` = 3557,
//! `inet_gist_picksplit` = 3558, `inet_gist_same` = 3559,
//! `inet_gist_fetch` = 3573) to these typed bodies, marshaling the `Datum`s.
//!
//! `backend-utils-adt-network-gist` is the single installer of these seams (it
//! never calls them); they panic loudly until that owner's `init_seams()` runs.
//! Until then a GiST dispatch to an inet OID hits an uninstalled seam — the
//! mirror-PG-and-panic contract.

use ::types_error::PgResult;
use types_network::{inet_struct, GistInetKey, GistInetSplitVec};

/// `StrategyNumber` (access/stratnum.h).
pub type StrategyNumber = u16;

seam_core::seam!(
    /// `inet_gist_consistent` (network_gist.c:114) — the GiST query consistency
    /// check. `key` is `DatumGetInetKeyP(ent->key)`, `query` is
    /// `PG_GETARG_INET_PP(1)`, `is_leaf` is `GIST_LEAF(ent)`. Returns
    /// `(matched, recheck)`; `Err` is the `elog(ERROR)` "unknown strategy"
    /// surface.
    pub fn inet_gist_consistent(
        key: GistInetKey,
        query: inet_struct,
        strategy: StrategyNumber,
        is_leaf: bool,
    ) -> PgResult<(bool, bool)>
);

seam_core::seam!(
    /// `inet_gist_union` (network_gist.c:504) — the GiST union function. `keys`
    /// are the entry keys `entryvec->vector[0..entryvec->n]`. Returns the union
    /// key.
    pub fn inet_gist_union(keys: Vec<GistInetKey>) -> GistInetKey
);

seam_core::seam!(
    /// `inet_gist_compress` (network_gist.c:541) — convert a leaf `inet` to a
    /// `GistInetKey`. `None` reflects a NULL key.
    pub fn inet_gist_compress(in_: Option<inet_struct>) -> Option<GistInetKey>
);

seam_core::seam!(
    /// `inet_gist_fetch` (network_gist.c:589) — reconstruct the original `inet`
    /// payload from a `GistInetKey`.
    pub fn inet_gist_fetch(key: GistInetKey) -> inet_struct
);

seam_core::seam!(
    /// `inet_gist_penalty` (network_gist.c:619) — the page-split penalty.
    pub fn inet_gist_penalty(orig: GistInetKey, new_: GistInetKey) -> f32
);

seam_core::seam!(
    /// `inet_gist_picksplit` (network_gist.c:662) — the PickSplit method.
    /// `keys` are `entryvec->vector[0..entryvec->n]` (index 0 unused). `Err`
    /// carries the C `palloc` OOM surface.
    pub fn inet_gist_picksplit(keys: Vec<GistInetKey>) -> PgResult<GistInetSplitVec>
);

seam_core::seam!(
    /// `inet_gist_same` (network_gist.c:796) — the GiST equality function.
    pub fn inet_gist_same(left: GistInetKey, right: GistInetKey) -> bool
);
