//! Scaffold for `src/backend/executor/execTuples.c` — the `TupleTableSlot`
//! machinery: the slot payload model, the four `Virtual/Heap/Minimal/Buffer`
//! slot-ops vtables, slot deform, the `ExecStore*`/`ExecFetch*` accessors,
//! slot creation/reset, and the `ExecTypeFromTL*` tuple-descriptor / tuple
//! output constructors.
//!
//! # Decomposition
//!
//! execTuples.c is ~2245 LOC and is split into six family modules so the
//! whole can be ported in passes that compile independently:
//!
//! * [`slot_payload_model`] — `TupleTableSlot` + the four payload subtype
//!   structs + the `tts_values`/`tts_isnull` arrays + flag/TID helpers.
//! * [`slot_ops_vtables`] — the per-kind `TupleTableSlotOps`
//!   (`init`/`release`/`clear`/`getsomeattrs`/`getsysattr`/`materialize`/
//!   `copyslot`/`get_*_tuple`/`copy_*_tuple`) for virtual/heap/minimal/buffer.
//! * [`slot_deform`] — `slot_deform_heap_tuple` /
//!   `slot_getsomeattrs[_int]` / `slot_getmissingattrs`.
//! * [`slot_store_fetch`] — `ExecStore*Tuple`/`ExecStoreVirtual`/
//!   `ExecStoreBufferHeapTuple`, `ExecFetchSlot*`, `ExecClearTuple`,
//!   `ExecCopySlot`.
//! * [`exec_init_slots`] — `ExecInitScanTupleSlot`/`ExecInitExtraTupleSlot`/
//!   `MakeTupleTableSlot`/`ExecAllocTableSlot`/`ExecResetTupleTable`/
//!   `ExecDropSingleTupleTableSlot` plus the tableam slot create. **This
//!   family owns and installs the `backend-executor-execTuples-seams`.**
//! * [`exectype_tupoutput`] — `ExecTypeFromTL`/`ExecCleanTypeFromTL`/
//!   `ExecTypeFromExprList`/`BlessTupleDesc`/`TupleDescGetAttInMetadata` plus
//!   the `begin/do/end_tup_output` convenience routines.
//!
//! STATUS: every routine has its real signature (mirroring the C and verified
//! against `executor/tuptable.h` / `funcapi.h` / `executor.h`). The
//! `SlotData` payload model — slot creation/teardown, the clear/store
//! callbacks, the byte-deform state machine, the per-kind `materialize` /
//! `get_*_tuple` / `copy_*_tuple` / `store_tuple` / `copyslot` ops, the
//! `ExecForceStore*`/`ExecStore*`/`ExecFetch*`/`ExecCopy*` entry points, the
//! tuple-descriptor constructors and the `begin/do/end_tup_output` family — is
//! implemented as own-logic over the body-bearing carriers: the slot fields
//! carry the full `FormedTuple` / `FormedMinimalTuple` (header + data-area
//! bytes), `tts_values` carries the by-reference `Datum::ByRef` lane, and
//! the op return / store-param types are the body-bearing carriers so no data
//! bytes are dropped at any boundary.
//!
//! The slot attribute reads (`slot_getattr` / `slot_getsysattr` and their
//! `tts_ops` dispatch) now return the canonical unified `Datum<'mcx>` value
//! (`ByVal` for a scalar column, `ByRef` carrying the owned column bytes for a
//! by-reference column), threaded through `'mcx`. A by-reference column no
//! longer has to collapse to a bare machine word — it crosses verbatim as
//! `ByRef`, so the former by-reference→bare-`Datum` projection panic is gone.
//!
//! One genuinely-unported dependency remains mirror-PG-and-panic:
//! * the **composite-`Datum` bridge** (`DatumGetHeapTupleHeader` /
//!   `HeapTupleGetDatum`) used by `ExecStoreHeapTupleDatum` /
//!   `ExecFetchSlotHeapTupleDatum` — the bare-word owned `Datum` has no
//!   pointer-to-tuple lane and the decode/mint is unported workspace-wide (the
//!   same bridge execExprInterp's row steps panic on).
//!
//! The `SlotId`/`es_tupleTable` pool bridge seams in [`exec_init_slots`] stay
//! provisional (the separate pool-convergence campaign); they are unrelated to
//! the `SlotData` payload bodies above.

#![allow(non_snake_case)]

pub mod slot_payload_model;
pub mod slot_ops_vtables;
pub mod slot_deform;
pub mod slot_store_fetch;
pub mod exec_init_slots;
pub mod exectype_tupoutput;

#[cfg(test)]
mod tests;

/// Install every seam this unit owns (the
/// `backend-executor-execTuples-seams` declarations). Called once at startup
/// from `seams-init`.
pub fn init_seams() {
    exec_init_slots::init_seams();
    exectype_tupoutput::install_guc_funcs_show_seams();
}
