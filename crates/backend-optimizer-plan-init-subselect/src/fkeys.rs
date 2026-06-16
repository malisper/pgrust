//! FOREIGN KEYS (initsplan.c) — `match_foreign_keys_to_quals`.
//!
//! # Trimmed-struct-field keystone (STOP, reported — not widened here)
//!
//! `match_foreign_keys_to_quals` (initsplan.c:3631) annotates each
//! `root->fkey_list` `ForeignKeyOptInfo` with which equivalence classes and
//! loose join quals match its columns, writing the results back into the
//! struct's `rinfos[colno]` / `nmatched_ec` / `nconst_ec` / `nmatched_ri` /
//! `nmatched_rcols` fields, then discards FKs that aren't fully matched.
//!
//! This repo's [`types_pathnodes::ForeignKeyOptInfo`] is trimmed to the
//! identity/key fields the join-path enumerator reads
//! (`con_relid`/`ref_relid`/`nkeys`/`conkey`/`confkey`/`conpfeqop`/`eclass`/
//! `fk_eclass_member`) and OMITS the five match-result fields this function
//! must populate:
//!
//!   * `List **rinfos` — per-column matched loose-qual RestrictInfos
//!   * `int nmatched_ec` — # columns matched to an EC
//!   * `int nconst_ec` — # of those ECs that have a pseudoconstant member
//!   * `int nmatched_ri` — # loose RestrictInfos matched
//!   * `int nmatched_rcols` — # columns matched to ≥1 loose qual
//!
//! Per the porting policy this is a genuine keystone (a missing carrier on a
//! shared types crate), not something this crate may widen. Until
//! `ForeignKeyOptInfo` regains these fields, the function cannot store its
//! results, so it is a loud panic ("mirror PG and panic"), exactly mirroring an
//! absent-subsystem boundary. The match logic itself (EC match via
//! `match_eclasses_to_foreign_key_col`, loose-qual scan over `con_rel->joininfo`
//! with the RelabelType-stripping Var match, `get_commutator` for the reverse
//! form) is fully understood and ports 1:1 once the carrier lands.

extern crate alloc;

use types_pathnodes::PlannerInfo;

/// `match_foreign_keys_to_quals` (initsplan.c:3631).
///
/// KEYSTONE-BLOCKED: see module docs — `ForeignKeyOptInfo` lacks the
/// `rinfos`/`nmatched_ec`/`nconst_ec`/`nmatched_ri`/`nmatched_rcols` result
/// fields this function must populate.
pub fn match_foreign_keys_to_quals(_root: &mut PlannerInfo) {
    if _root.fkey_list.is_empty() {
        // No foreign keys: faithful no-op (C loops over an empty list and
        // assigns the empty newlist back).
        return;
    }
    panic!(
        "backend-optimizer-plan-init-subselect::match_foreign_keys_to_quals is KEYSTONE-BLOCKED: \
         types_pathnodes::ForeignKeyOptInfo is trimmed and omits the match-result fields \
         rinfos/nmatched_ec/nconst_ec/nmatched_ri/nmatched_rcols that this function must \
         populate. Unblocks when the ForeignKeyOptInfo carrier regains those fields."
    );
}
