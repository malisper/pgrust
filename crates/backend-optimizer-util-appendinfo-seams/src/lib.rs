#![forbid(unsafe_code)]

//! Seam declarations for `optimizer/util/appendinfo.c` — the append-relation
//! attribute-translation routines the partitionwise-join machinery
//! (`joinrels.c:try_partitionwise_join` / `build_child_join_sjinfo`) drives.
//!
//! `find_appinfos_by_relids` collects the `AppendRelInfo`s for a child relid
//! set (the C `AppendRelInfo **` + `nappinfos`, here a freshly-owned `Vec`).
//! `adjust_child_relids` translates a parent `Relids` into the corresponding
//! child `Relids`. `adjust_appendrel_attrs_restrictlist` is the
//! `(List *) adjust_appendrel_attrs(root, (Node *) restrictlist, ...)`
//! specialization joinrels.c uses on a `RestrictInfo` list — carried as a
//! `RinfoId` handle list. The owning crate
//! (`backend-optimizer-util-appendinfo`) installs these from its `init_seams()`
//! once it lands; until then a call panics loudly.
//!
//! **Failure surface.** The collectors / translators `palloc` (and so can
//! `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`), so they return [`PgResult`].

extern crate alloc;

use alloc::vec::Vec;

use types_error::PgResult;
use types_pathnodes::{AppendRelInfo, PlannerInfo, Relids, RinfoId};

seam_core::seam!(
    /// `find_appinfos_by_relids(root, relids, &nappinfos)` (appendinfo.c) — the
    /// `AppendRelInfo`s for the child relations named in `relids` (a freshly
    /// owned vector; the C `AppendRelInfo **` array + its length).
    pub fn find_appinfos_by_relids(root: &PlannerInfo, relids: &Relids) -> PgResult<Vec<AppendRelInfo>>
);
seam_core::seam!(
    /// `adjust_child_relids(relids, nappinfos, appinfos)` (appendinfo.c) —
    /// translate a parent `Relids` to the corresponding child `Relids`.
    pub fn adjust_child_relids(relids: &Relids, appinfos: &[AppendRelInfo]) -> Relids
);
seam_core::seam!(
    /// `(List *) adjust_appendrel_attrs(root, (Node *) restrictlist, nappinfos,
    /// appinfos)` (appendinfo.c) — translate a parent-join `RestrictInfo` list
    /// (carried as `RinfoId` handles) into the child join's restrictlist.
    pub fn adjust_appendrel_attrs_restrictlist(
        root: &mut PlannerInfo,
        restrictlist: &[RinfoId],
        appinfos: &[AppendRelInfo],
    ) -> PgResult<Vec<RinfoId>>
);
seam_core::seam!(
    /// `distribute_row_identity_vars(root)` (appendinfo.c) — distribute any
    /// UPDATE/DELETE/MERGE row-identity variables to the target relations once
    /// appendrel expansion is finished. `query_planner` (planmain.c) calls this
    /// on the general join path after `add_other_rels_to_query`. Void in C; can
    /// `palloc`, so it returns [`PgResult`].
    pub fn distribute_row_identity_vars(root: &mut PlannerInfo) -> PgResult<()>
);
