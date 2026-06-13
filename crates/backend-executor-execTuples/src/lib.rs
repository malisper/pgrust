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
//! against `executor/tuptable.h` / `funcapi.h` / `executor.h`). The control
//! flow that the landed payload model supports — slot creation/teardown, the
//! clear/store-header callbacks, the byte-deform state machine, the
//! `ExecForceStore*`/`ExecStore*`/`ExecFetch*` dispatch, the tuple-descriptor
//! constructors and the `begin/do/end_tup_output` family — is implemented.
//!
//! The tuple-bearing bodies that must store, copy, form, return, or deform a
//! *physical heap/minimal tuple with its user-data bytes* are blocked on a
//! genuine carrier gap in the landed model and `panic!` (mirror-PG-and-panic):
//! the slot fields hold a header-only `HeapTuple`/`MinimalTuple`
//! (`HeapTupleData.t_data` is the header struct, no trailing data area), while
//! `heap_form_tuple`/`heap_copytuple` produce a `FormedTuple { tuple, data }`
//! whose `data: PgVec<u8>` body has nowhere to live in the slot; and
//! `tts_values: PgVec<Datum>` carries bare machine words that cannot hold a
//! by-reference column (`TupleValue::ByRef`). Closing these requires expanding
//! the slot structs (a `FormedTuple`-shaped body carrier + a by-reference lane
//! in `tts_values`), which is the separate workspace-wide
//! `TupleTableSlot`-header / `es_tupleTable` convergence campaign; the
//! provisional bridge seams in [`exec_init_slots`] stay until it lands.

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
