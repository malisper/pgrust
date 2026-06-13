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
//! SCAFFOLD STAGE: every routine has its real signature (mirroring the C and
//! verified against `executor/tuptable.h` / `funcapi.h` / `executor.h`) and a
//! `todo!()` body. The crate compiles and wires `init_seams()`; the logic
//! lands in follow-up passes.

#![allow(non_snake_case)]

pub mod slot_payload_model;
pub mod slot_ops_vtables;
pub mod slot_deform;
pub mod slot_store_fetch;
pub mod exec_init_slots;
pub mod exectype_tupoutput;

/// Install every seam this unit owns (the
/// `backend-executor-execTuples-seams` declarations). Called once at startup
/// from `seams-init`.
pub fn init_seams() {
    exec_init_slots::init_seams();
}
