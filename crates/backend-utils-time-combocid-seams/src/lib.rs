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
use types_tuple::heaptuple::HeapTupleHeaderData;

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
