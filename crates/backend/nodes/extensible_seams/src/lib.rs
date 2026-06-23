//! Seam declarations for the `backend-nodes-extensible` unit
//! (`nodes/extensible.c`): the extension-defined-node and custom-scan registry.
//!
//! The copy/equal/out/read node dispatch (`copyfuncs.c`/`equalfuncs.c`/
//! `outfuncs.c`/`readfuncs.c`) and the custom-scan executor
//! (`nodeCustom.c`) look these tables up across a cycle. The owning crate
//! installs all four from its `init_seams()`; until then a call panics loudly.
//!
//! Method tables and the `extnodename`/`CustomName` keys cross the boundary as
//! owned values: C's `const char *` key is a `&str`, and C's `const
//! ExtensibleNodeMethods *` / `const CustomScanMethods *` are the owned,
//! `Clone` method tables passed by reference (register) or returned by clone
//! (get) — the owned-tree analogue of C's raw-pointer ABI.

#![allow(non_snake_case)]

use ::types_error::PgResult;
use ::types_extensible::{CustomScanMethods, ExtensibleNodeMethods};

seam_core::seam!(
    /// `RegisterExtensibleNodeMethods(const ExtensibleNodeMethods *methods)`
    /// (extensible.c): register a new type of extensible node.
    pub fn RegisterExtensibleNodeMethods(methods: &ExtensibleNodeMethods) -> PgResult<()>
);

seam_core::seam!(
    /// `RegisterCustomScanMethods(const CustomScanMethods *methods)`
    /// (extensible.c): register a new type of custom scan node.
    pub fn RegisterCustomScanMethods(methods: &CustomScanMethods) -> PgResult<()>
);

seam_core::seam!(
    /// `GetExtensibleNodeMethods(const char *extnodename, bool missing_ok)`
    /// (extensible.c): look up an extensible-node method table by name. `None`
    /// when `missing_ok` and not found, else `ERRCODE_UNDEFINED_OBJECT`.
    pub fn GetExtensibleNodeMethods(
        extnodename: &str,
        missing_ok: bool,
    ) -> PgResult<Option<ExtensibleNodeMethods>>
);

seam_core::seam!(
    /// `GetCustomScanMethods(const char *CustomName, bool missing_ok)`
    /// (extensible.c): look up a custom-scan method table by name. `None` when
    /// `missing_ok` and not found, else `ERRCODE_UNDEFINED_OBJECT`.
    pub fn GetCustomScanMethods(
        CustomName: &str,
        missing_ok: bool,
    ) -> PgResult<Option<CustomScanMethods>>
);

// ===========================================================================
// Custom-scan PROVIDER callbacks (nodes/extensible.h's `CustomScanMethods` /
// `CustomExecMethods`). These are installed by a custom-scan-provider
// extension; there is no in-tree owner, so — like the FDW provider callbacks
// in `backend-foreign-foreign-seams` — they remain seam-and-panic: a call
// panics loudly until an extension installs the real callback. `nodeCustom.c`
// invokes them through these seams; the owned `CustomScanState` /
// `ParallelContext` cross by value.
// ===========================================================================

use ::mcx::Mcx;
use ::nodes::{
    CustomScan, CustomScanState, EStateData, ParallelContext, ParallelWorkerContext, SlotId,
};

seam_core::seam!(
    /// `cscan->methods->CreateCustomScanState(cscan)` (extensible.h): the
    /// provider allocates and tag/methods-initializes the `CustomScanState`
    /// (it may embed it as the first field of a larger object).
    pub fn create_custom_scan_state<'mcx>(
        mcx: Mcx<'mcx>,
        cscan: &CustomScan<'mcx>,
    ) -> PgResult<CustomScanState<'mcx>>
);

seam_core::seam!(
    /// `css->methods->BeginCustomScan(css, estate, eflags)` (extensible.h):
    /// the provider's final node-state initialization.
    pub fn begin_custom_scan<'mcx>(
        node: &mut CustomScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        eflags: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->methods->ExecCustomScan(node)` (extensible.h): fetch the next
    /// tuple into the node's result slot. `Some(slot)` when a tuple is
    /// available, `None` when the scan is exhausted (the C `NULL`).
    pub fn exec_custom_scan<'mcx>(
        node: &mut CustomScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<SlotId>>
);

seam_core::seam!(
    /// `node->methods->EndCustomScan(node)` (extensible.h): tear down provider
    /// state.
    pub fn end_custom_scan<'mcx>(
        node: &mut CustomScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->methods->ReScanCustomScan(node)` (extensible.h): rescan.
    pub fn rescan_custom_scan<'mcx>(
        node: &mut CustomScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->methods->MarkPosCustomScan(node)` (extensible.h): mark position
    /// (optional; the node checks presence before calling).
    pub fn mark_pos_custom_scan<'mcx>(
        node: &mut CustomScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->methods->RestrPosCustomScan(node)` (extensible.h): restore
    /// position (optional).
    pub fn restr_pos_custom_scan<'mcx>(
        node: &mut CustomScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->methods->EstimateDSMCustomScan(node, pcxt)` (extensible.h):
    /// returns the parallel coordination size; the node then reserves the TOC
    /// chunk/key. The `shm_toc_estimate_*` of the chunk is folded into the
    /// seam (it receives `pcxt`).
    pub fn estimate_dsm_custom_scan<'mcx>(
        node: &mut CustomScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        pcxt: &mut ParallelContext,
    ) -> PgResult<usize>
);

seam_core::seam!(
    /// `node->methods->InitializeDSMCustomScan(node, pcxt, coordinate)`
    /// (extensible.h): allocate the coordination area in the TOC, let the
    /// provider initialize it, and publish it keyed by `plan_node_id`. The
    /// allocate / init / insert collapse into the seam (it receives `pcxt` and
    /// the node's `pscan_len` / `plan_node_id`).
    pub fn initialize_dsm_custom_scan<'mcx>(
        node: &mut CustomScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        pcxt: &mut ParallelContext,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->methods->ReInitializeDSMCustomScan(node, pcxt, coordinate)`
    /// (extensible.h): re-find the coordination area in the TOC and let the
    /// provider reinitialize. The TOC lookup folds into the seam.
    pub fn reinitialize_dsm_custom_scan<'mcx>(
        node: &mut CustomScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        pcxt: &mut ParallelContext,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->methods->InitializeWorkerCustomScan(node, toc, coordinate)`
    /// (extensible.h): in a parallel worker, re-find the coordination area in
    /// the worker TOC and let the provider attach. The TOC lookup folds into
    /// the seam.
    pub fn initialize_worker_custom_scan<'mcx>(
        node: &mut CustomScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        pwcxt: &mut ParallelWorkerContext,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->methods->ShutdownCustomScan(node)` (extensible.h): release
    /// parallel-worker-held resources before the workers exit (optional).
    pub fn shutdown_custom_scan<'mcx>(
        node: &mut CustomScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);
