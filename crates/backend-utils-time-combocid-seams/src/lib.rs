//! Seam declarations for the `backend-utils-time-combocid` unit
//! (`utils/time/combocid.c`), the combo-CID resolution used by the
//! `HeapTupleSatisfies*` visibility predicates.
//!
//! `HeapTupleHeaderGetCmin`/`Cmax` are macros over the file-scope combo-CID
//! state (`comboCids`/`comboHash`). In this repo that state is the owned
//! `ComboCidState<'mcx>` threaded from the transaction owner (xact.c), so a
//! visibility predicate — which has no `ComboCidState` in hand — reaches the
//! resolved cmin/cmax through these seams. The owner that holds the live
//! per-transaction `ComboCidState` installs them; see DESIGN_DEBT.

#![allow(non_snake_case)]

use types_core::CommandId;
use types_error::PgResult;
use types_tuple::heaptuple::HeapTupleHeaderData;

seam_core::seam!(
    /// `HeapTupleHeaderAdjustCmax(tup, &cmax, &iscombo)` (combocid.c) — given a
    /// tuple we are about to delete/update, determine the command id to store
    /// into its `t_cid`: `(cmax, false)` if no combo CID is needed, or
    /// `(combo_cid, true)` if the tuple was inserted by (a subtransaction of)
    /// our own transaction. Resolves against the current transaction's combo-CID
    /// state. Returns C's `(*cmax, *iscombo)`. `Err` carries the OOM
    /// `ereport(ERROR)` surface (it runs before the critical section for that
    /// reason).
    pub fn heap_tuple_header_adjust_cmax(
        tuple: &HeapTupleHeaderData<'_>,
        cmax: CommandId,
    ) -> PgResult<(CommandId, bool)>
);

seam_core::seam!(
    /// `HeapTupleHeaderGetCmin(tup)` (combocid.c) — the command id that
    /// inserted the tuple, resolving a combo CID against the current
    /// transaction's combo-CID state. Only valid when the tuple was inserted by
    /// the current transaction.
    pub fn heap_tuple_header_get_cmin(tuple: &HeapTupleHeaderData<'_>) -> CommandId
);

seam_core::seam!(
    /// `HeapTupleHeaderGetCmax(tup)` (combocid.c) — the command id that deleted
    /// the tuple, resolving a combo CID against the current transaction's
    /// combo-CID state.
    pub fn heap_tuple_header_get_cmax(tuple: &HeapTupleHeaderData<'_>) -> CommandId
);

seam_core::seam!(
    /// `AtEOXact_ComboCid()` (combocid.c) — discard the current transaction's
    /// combo-CID state at end of transaction (commit/prepare/abort). Combo CIDs
    /// are only interesting to the inserting/deleting transaction, so the
    /// backend-local state is reset. Consumed by xact.c's commit/prepare/abort
    /// cleanup, mirroring C's `AtEOXact_ComboCid()` calls.
    pub fn at_eoxact_combocid()
);
