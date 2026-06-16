//! `access/tablesample/{system,bernoulli,tablesample}.c` — the in-tree
//! TABLESAMPLE method handlers (SYSTEM, BERNOULLI) and the `GetTsmRoutine`
//! support helper.
//!
//! This crate is the owner of the tablesample-method layer that sits *below*
//! `nodeSamplescan.c` (the executor SampleScan node, already ported and landed)
//! and the planner's `set_tablesample_rel_size` path. It ports three C files:
//!
//!   * `system.c` — the SYSTEM (block-level Bernoulli) method.
//!   * `bernoulli.c` — the BERNOULLI (tuple-level) method.
//!   * `tablesample.c` — the `GetTsmRoutine` convenience helper.
//!
//! ## Faithfulness of the sampling math
//!
//! The numeric core of both methods is ported branch-for-branch from PostgreSQL
//! 18.3 with identical control flow, loop bounds, sentinels
//! ([`InvalidOffsetNumber`] / [`InvalidBlockNumber`]), and constants. The cutoff
//! is `rint(((double) PG_UINT32_MAX + 1) * percent / 100)` (C `rint` rounds to
//! nearest, ties-to-even — matched with [`f64::round_ties_even`], *not* Rust's
//! `round` which rounds ties away from zero). Block/tuple selection hashes a
//! `uint32[]` array of the candidate identifiers together with the seed via
//! `hash_any`, reproduced with [`common_hashfn::hash_bytes`] (the same Bob
//! Jenkins hash, fed the native-endian bytes of the array exactly as the C
//! `(const unsigned char *) hashinput` cast exposes them) — giving the identical
//! machine-independent results PostgreSQL's regression tests rely on.
//!
//! ## TsmRoutine vtable and how the executor reaches the methods
//!
//! Each handler ([`tsm_system_handler`], [`tsm_bernoulli_handler`]) builds a
//! [`TsmRoutine`] (tag `T_TsmRoutine`) whose callback slots are real Rust
//! function pointers, mirroring the C handler that installs C function pointers
//! (`tsm->BeginSampleScan = system_beginsamplescan`, …). The callback ABI is the
//! one `types-samplescan` (the landed node-state crate) declares: each callback
//! takes `&mut SampleScanState<'mcx>` and operates *in place* on the node,
//! exactly like the table-AM / index-AM vtable convention. The method-private
//! scratch state (C `node->tsm_state`, a `void *` to a `palloc0`'d
//! `SystemSamplerData` / `BernoulliSamplerData`) is modelled as the owned
//! [`Opaque`] (`Box<dyn Any>`) the node carries; the callbacks downcast it.
//!
//! `nodeSamplescan` reaches the routine through its own `-seams` crate. This
//! crate **installs** those seams in [`init_seams`]:
//!
//!   * `get_tsm_routine_oid` — the `GetTsmRoutine(handlerOid)` registry: maps a
//!     handler-function OID (`3313` bernoulli, `3314` system) to its
//!     [`TsmRoutine`], allocated in the caller's `Mcx`.
//!   * `tsm_has_init_sample_scan` / `tsm_init_sample_scan` /
//!     `tsm_begin_sample_scan` / `tsm_has_next_sample_block` /
//!     `tsm_has_end_sample_scan` / `tsm_end_sample_scan` — the vtable dispatch
//!     wrappers (`tsm->InitSampleScan != NULL`, `tsm->BeginSampleScan(...)`, …),
//!     which read `scanstate->tsmroutine`'s function pointers and call them.
//!
//! `tsm->NextSampleBlock` / `tsm->NextSampleTuple` are invoked by the table
//! access method (`table_scan_sample_next_block` / `..._next_tuple`,
//! heapam_handler.c), which is a separate owner; they live in the routine vtable
//! so that owner can dispatch them once it lands.
//!
//! ## What stays seam-and-panic / unreached
//!
//! `SampleScanGetSampleSize` is a *planner*-facing callback. The planner
//! (`set_tablesample_rel_size`, allpaths.c) reaches it through the
//! `backend-optimizer-path-allpaths` `tsm_get_sample_size` seam, which must first
//! navigate `rte->tablesample->{tsmhandler,args}` — RTE navigation owned by the
//! still-unported planner-entry crate (the `Query<'mcx>` owner) and its
//! `backend-optimizer-rte-seams`. That seam is therefore **not** installed here;
//! it stays a loud panic (mirror-PG-and-panic) until the planner owner lands.
//! The faithful estimation bodies live in [`system_samplescangetsamplesize`] /
//! [`bernoulli_samplescangetsamplesize`] (operating on the owned [`Expr`] args,
//! calling [`estimate_expression_value`] and [`clamp_row_est`] directly), ready
//! for that owner to call. The [`TsmRoutine`] vtable's `SampleScanGetSampleSize`
//! slot carries an ABI-matching shim (see [`system_samplescangetsamplesize_cb`])
//! that takes C's default-`else` branch, because the landed `types-samplescan`
//! fn-pointer signature carries neither an `Mcx` nor the walkable args needed for
//! the constant-folding path.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use backend_executor_nodeSamplescan_seams as seam;
use backend_optimizer_path_costsize::clamp_row_est;
use backend_optimizer_util_clauses::estimate_expression_value;
use common_hashfn::hash_bytes;
use mcx::{Mcx, PgBox};
use types_core::primitive::{
    uint16, uint32, BlockNumber, Cardinality, OffsetNumber, Oid, InvalidBlockNumber,
};
use types_datum::datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_INVALID_TABLESAMPLE_ARGUMENT};
use types_nodes::nodes::{Node, NodeTag};
use types_nodes::primnodes::Expr;
use types_pathnodes::{PlannerInfo, RelOptInfo};
use types_samplescan::{SampleScanState, TsmRoutine};

// ===========================================================================
// Constants mirroring C headers.
// ===========================================================================

/// `T_TsmRoutine = 440` (`nodes/nodetags.h`) — the tag `makeNode(TsmRoutine)`
/// stamps onto the routine.
const T_TsmRoutine: NodeTag = NodeTag(440);

/// `FirstOffsetNumber = 1` (`storage/off.h`).
const FirstOffsetNumber: OffsetNumber = 1;
/// `InvalidOffsetNumber = 0` (`storage/off.h`).
const InvalidOffsetNumber: OffsetNumber = 0;

/// `FLOAT4OID = 700` (`catalog/pg_type_d.h`) — the datatype of the PERCENT
/// argument of the TABLESAMPLE clause.
const FLOAT4OID: Oid = 700;

/// `PG_UINT32_MAX = 0xFFFFFFFF` (`c.h`).
const PG_UINT32_MAX: u32 = 0xFFFF_FFFF;

/// `tsm_bernoulli_handler` pg_proc OID (`pg_proc.dat`).
const F_TSM_BERNOULLI_HANDLER: Oid = 3313;
/// `tsm_system_handler` pg_proc OID (`pg_proc.dat`).
const F_TSM_SYSTEM_HANDLER: Oid = 3314;

// ===========================================================================
// DatumGetFloat4 (postgres.h) — reinterpret the low 32 bits of a Datum as the
// IEEE-754 bit pattern of a float4. A pure value conversion (not the
// PG_FUNCTION_ARGS fmgr boundary), so it is done in-crate.
// ===========================================================================

#[inline]
fn DatumGetFloat4(d: Datum) -> f32 {
    f32::from_bits(d.as_u32())
}

// ===========================================================================
// system.c — private state.
// ===========================================================================

/// `SystemSamplerData` (system.c) — the SYSTEM method's private state. In C this
/// is the struct `node->tsm_state` points at (a `palloc0`'d block); here it is
/// the owned value held behind the node's [`Opaque`]. Field order/types
/// preserved 1:1.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SystemSamplerData {
    /// `uint64 cutoff` — select blocks with hash less than this.
    pub cutoff: u64,
    /// `uint32 seed` — random seed.
    pub seed: uint32,
    /// `BlockNumber nextblock` — next block to consider sampling.
    pub nextblock: BlockNumber,
    /// `OffsetNumber lt` — last tuple returned from current block.
    pub lt: OffsetNumber,
}

// ===========================================================================
// bernoulli.c — private state.
// ===========================================================================

/// `BernoulliSamplerData` (bernoulli.c) — the BERNOULLI method's private state
/// (`node->tsm_state`). Field order matches the C struct exactly:
/// `cutoff`, `seed`, `lt`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BernoulliSamplerData {
    /// `uint64 cutoff` — select tuples with hash less than this.
    pub cutoff: u64,
    /// `uint32 seed` — random seed.
    pub seed: uint32,
    /// `OffsetNumber lt` — last tuple returned from current block.
    pub lt: OffsetNumber,
}

// ===========================================================================
// hash_any helpers — `hash_any((const unsigned char *) hashinput,
// sizeof(hashinput))`, fed the native-endian bytes of the uint32 array exactly
// as the C cast exposes them, then `DatumGetUInt32`.
// ===========================================================================

#[inline]
fn hash_any_u32_array2(hashinput: &[uint32; 2]) -> uint32 {
    let mut bytes = [0u8; 8];
    bytes[0..4].copy_from_slice(&hashinput[0].to_ne_bytes());
    bytes[4..8].copy_from_slice(&hashinput[1].to_ne_bytes());
    hash_bytes(&bytes)
}

#[inline]
fn hash_any_u32_array3(hashinput: &[uint32; 3]) -> uint32 {
    let mut bytes = [0u8; 12];
    bytes[0..4].copy_from_slice(&hashinput[0].to_ne_bytes());
    bytes[4..8].copy_from_slice(&hashinput[1].to_ne_bytes());
    bytes[8..12].copy_from_slice(&hashinput[2].to_ne_bytes());
    hash_bytes(&bytes)
}

// ===========================================================================
// Method-private state accessors over the node's `Opaque` tsm_state.
//
// C: `SystemSamplerData *sampler = (SystemSamplerData *) node->tsm_state;`.
// Here `node->tsm_state` is an `Opaque(Option<Box<dyn Any>>)`; the cast becomes
// a downcast, with a loud panic if the wrong method's state is present (which
// would be a bug in `InitSampleScan` dispatch, exactly as a bad C cast would be
// undefined behaviour).
// ===========================================================================

fn system_sampler_mut<'a, 'mcx>(node: &'a mut SampleScanState<'mcx>) -> &'a mut SystemSamplerData {
    node.tsm_state
        .as_mut()
        .and_then(|o| o.0.as_mut())
        .and_then(|b| b.downcast_mut::<SystemSamplerData>())
        .expect("system tablesample: node->tsm_state is not a SystemSamplerData")
}

fn bernoulli_sampler_mut<'a, 'mcx>(
    node: &'a mut SampleScanState<'mcx>,
) -> &'a mut BernoulliSamplerData {
    node.tsm_state
        .as_mut()
        .and_then(|o| o.0.as_mut())
        .and_then(|b| b.downcast_mut::<BernoulliSamplerData>())
        .expect("bernoulli tablesample: node->tsm_state is not a BernoulliSamplerData")
}

// ===========================================================================
// system.c — handler.
// ===========================================================================

/// `tsm_system_handler(PG_FUNCTION_ARGS)` — create a [`TsmRoutine`] descriptor
/// for the SYSTEM method. The C fmgr entry `PG_RETURN_POINTER(tsm)` becomes the
/// owned value; `makeNode(TsmRoutine)` becomes the tagged struct and
/// `list_make1_oid(FLOAT4OID)` the single-element `Vec<Oid>`.
pub fn tsm_system_handler() -> TsmRoutine {
    TsmRoutine {
        // tsm = makeNode(TsmRoutine);
        type_: T_TsmRoutine,
        // tsm->parameterTypes = list_make1_oid(FLOAT4OID);
        parameterTypes: alloc::vec![FLOAT4OID],
        // tsm->repeatable_across_queries = true;
        repeatable_across_queries: true,
        // tsm->repeatable_across_scans = true;
        repeatable_across_scans: true,
        // tsm->SampleScanGetSampleSize = system_samplescangetsamplesize;
        SampleScanGetSampleSize: Some(system_samplescangetsamplesize_cb),
        // tsm->InitSampleScan = system_initsamplescan;
        InitSampleScan: Some(system_initsamplescan),
        // tsm->BeginSampleScan = system_beginsamplescan;
        BeginSampleScan: Some(system_beginsamplescan),
        // tsm->NextSampleBlock = system_nextsampleblock;
        NextSampleBlock: Some(system_nextsampleblock),
        // tsm->NextSampleTuple = system_nextsampletuple;
        NextSampleTuple: Some(system_nextsampletuple),
        // tsm->EndSampleScan = NULL;
        EndSampleScan: None,
    }
}

/// `system_samplescangetsamplesize` (system.c) — sample size estimation, the
/// faithful body over the owned [`Expr`] args. `paramexprs[0]` is the PERCENT
/// argument. Returns the C out-parameters `(*pages, *tuples)`.
pub fn system_samplescangetsamplesize<'mcx>(
    mcx: Mcx<'mcx>,
    baserel: &RelOptInfo,
    paramexprs: &[Expr],
) -> PgResult<(BlockNumber, f64)> {
    // Node *pctnode = (Node *) linitial(paramexprs);
    // pctnode = estimate_expression_value(root, pctnode);
    let pctnode = estimate_expression_value(mcx, paramexprs[0].clone())?;

    // if (IsA(pctnode, Const) && !((Const *) pctnode)->constisnull)
    let samplefract: f32 = match &pctnode {
        Expr::Const(c) if !c.constisnull => {
            // samplefract = DatumGetFloat4(((Const *) pctnode)->constvalue);
            let samplefract = c.constvalue.as_f32();
            if samplefract >= 0.0 && samplefract <= 100.0 && !samplefract.is_nan() {
                samplefract / 100.0f32
            } else {
                // Default samplefract if the value is bogus.
                0.1f32
            }
        }
        // Default samplefract if we didn't obtain a non-null Const.
        _ => 0.1f32,
    };

    // We'll visit a sample of the pages ...
    // *pages = clamp_row_est(baserel->pages * samplefract);
    //
    // C does the multiply in `float` (uint32 baserel->pages promotes to float
    // against the float `samplefract`), then widens the float product to the
    // double clamp_row_est takes. Mirror that: multiply in f32, then widen.
    let pages = clamp_row_est((baserel.pages as f32 * samplefract) as f64) as BlockNumber;

    // ... and hopefully get a representative number of tuples from them.
    // *tuples = clamp_row_est(baserel->tuples * samplefract);
    let tuples = clamp_row_est(baserel.tuples * samplefract as Cardinality);

    Ok((pages, tuples))
}

/// `system_initsamplescan(node, eflags)` — initialize during executor setup.
/// C: `node->tsm_state = palloc0(sizeof(SystemSamplerData));`. `eflags` unused.
fn system_initsamplescan<'mcx>(node: &mut SampleScanState<'mcx>, _eflags: i32) {
    node.tsm_state = Some(types_nodes::execnodes::Opaque(Some(Box::new(
        SystemSamplerData::default(),
    ))));
}

/// `system_beginsamplescan(node, params, nparams, seed)` — examine parameters
/// and prepare for a sample scan. `nparams` unused (as in C).
fn system_beginsamplescan<'mcx>(
    node: &mut SampleScanState<'mcx>,
    params: &[Datum],
    _nparams: i32,
    seed: uint32,
) {
    // double percent = DatumGetFloat4(params[0]);
    let percent: f64 = DatumGetFloat4(params[0]) as f64;

    // if (percent < 0 || percent > 100 || isnan(percent))
    //     ereport(ERROR, ... "sample percentage must be between 0 and 100");
    if percent < 0.0 || percent > 100.0 || percent.is_nan() {
        // The C callback raises ereport(ERROR); this callback ABI carries no
        // error channel, so propagate the same error loudly (matches the
        // panic-on-ereport convention the other vtable callbacks must use).
        panic_sample_percentage();
    }

    let sampler = system_sampler_mut(node);

    // dcutoff = rint(((double) PG_UINT32_MAX + 1) * percent / 100);
    let dcutoff: f64 = (((PG_UINT32_MAX as f64) + 1.0) * percent / 100.0).round_ties_even();
    sampler.cutoff = dcutoff as u64;
    sampler.seed = seed;
    sampler.nextblock = 0;
    sampler.lt = InvalidOffsetNumber;

    // node->use_bulkread = (percent >= 1);
    // node->use_pagemode = true;
    node.use_bulkread = percent >= 1.0;
    node.use_pagemode = true;
}

/// `system_nextsampleblock(node, nblocks)` — select next block to sample.
fn system_nextsampleblock<'mcx>(node: &mut SampleScanState<'mcx>, nblocks: BlockNumber) -> BlockNumber {
    let sampler = system_sampler_mut(node);
    // BlockNumber nextblock = sampler->nextblock;
    let mut nextblock: BlockNumber = sampler.nextblock;
    // uint32 hashinput[2];
    let mut hashinput: [uint32; 2] = [0; 2];

    // These words in the hash input are the same throughout the block:
    // hashinput[1] = sampler->seed;
    hashinput[1] = sampler.seed;

    // for (; nextblock < nblocks; nextblock++)
    while nextblock < nblocks {
        // hashinput[0] = nextblock;
        hashinput[0] = nextblock;

        // hash = DatumGetUInt32(hash_any(hashinput, sizeof(hashinput)));
        let hash: uint32 = hash_any_u32_array2(&hashinput);
        // if (hash < sampler->cutoff) break;
        if (hash as u64) < sampler.cutoff {
            break;
        }
        nextblock += 1;
    }

    if nextblock < nblocks {
        // Found a suitable block; remember where we should start next time.
        // sampler->nextblock = nextblock + 1;
        // return nextblock;
        sampler.nextblock = nextblock + 1;
        return nextblock;
    }

    // Done, but let's reset nextblock to 0 for safety.
    // sampler->nextblock = 0;
    // return InvalidBlockNumber;
    sampler.nextblock = 0;
    InvalidBlockNumber
}

/// `system_nextsampletuple(node, blockno, maxoffset)` — select next sampled
/// tuple in current block. In block sampling we sample all tuples in each
/// selected block.
fn system_nextsampletuple<'mcx>(
    node: &mut SampleScanState<'mcx>,
    _blockno: BlockNumber,
    maxoffset: OffsetNumber,
) -> uint16 {
    let sampler = system_sampler_mut(node);
    // OffsetNumber tupoffset = sampler->lt;
    let mut tupoffset: OffsetNumber = sampler.lt;

    // Advance to next possible offset on page.
    if tupoffset == InvalidOffsetNumber {
        tupoffset = FirstOffsetNumber;
    } else {
        tupoffset += 1;
    }

    // Done?
    if tupoffset > maxoffset {
        tupoffset = InvalidOffsetNumber;
    }

    // sampler->lt = tupoffset;
    sampler.lt = tupoffset;

    tupoffset
}

// ===========================================================================
// bernoulli.c — handler.
// ===========================================================================

/// `tsm_bernoulli_handler(PG_FUNCTION_ARGS)` — create a [`TsmRoutine`]
/// descriptor for the BERNOULLI method.
pub fn tsm_bernoulli_handler() -> TsmRoutine {
    TsmRoutine {
        // tsm = makeNode(TsmRoutine);
        type_: T_TsmRoutine,
        // tsm->parameterTypes = list_make1_oid(FLOAT4OID);
        parameterTypes: alloc::vec![FLOAT4OID],
        repeatable_across_queries: true,
        repeatable_across_scans: true,
        SampleScanGetSampleSize: Some(bernoulli_samplescangetsamplesize_cb),
        InitSampleScan: Some(bernoulli_initsamplescan),
        BeginSampleScan: Some(bernoulli_beginsamplescan),
        // tsm->NextSampleBlock = NULL;
        NextSampleBlock: None,
        NextSampleTuple: Some(bernoulli_nextsampletuple),
        // tsm->EndSampleScan = NULL;
        EndSampleScan: None,
    }
}

/// `bernoulli_samplescangetsamplesize` (bernoulli.c) — sample size estimation,
/// the faithful body over the owned [`Expr`] args. Returns `(*pages, *tuples)`.
pub fn bernoulli_samplescangetsamplesize<'mcx>(
    mcx: Mcx<'mcx>,
    baserel: &RelOptInfo,
    paramexprs: &[Expr],
) -> PgResult<(BlockNumber, f64)> {
    // pctnode = (Node *) linitial(paramexprs);
    // pctnode = estimate_expression_value(root, pctnode);
    let pctnode = estimate_expression_value(mcx, paramexprs[0].clone())?;

    let samplefract: f32 = match &pctnode {
        // if (IsA(pctnode, Const) && !((Const *) pctnode)->constisnull)
        Expr::Const(c) if !c.constisnull => {
            // samplefract = DatumGetFloat4(((Const *) pctnode)->constvalue);
            let samplefract = c.constvalue.as_f32();
            if samplefract >= 0.0 && samplefract <= 100.0 && !samplefract.is_nan() {
                samplefract / 100.0f32
            } else {
                // Default samplefract if the value is bogus.
                0.1f32
            }
        }
        // Default samplefract if we didn't obtain a non-null Const.
        _ => 0.1f32,
    };

    // We'll visit all pages of the baserel.
    // *pages = baserel->pages;
    let pages = baserel.pages;

    // *tuples = clamp_row_est(baserel->tuples * samplefract);
    let tuples = clamp_row_est(baserel.tuples * samplefract as Cardinality);

    Ok((pages, tuples))
}

/// `bernoulli_initsamplescan(node, eflags)` — initialize during executor setup.
/// C: `node->tsm_state = palloc0(sizeof(BernoulliSamplerData));`. `eflags`
/// unused.
fn bernoulli_initsamplescan<'mcx>(node: &mut SampleScanState<'mcx>, _eflags: i32) {
    node.tsm_state = Some(types_nodes::execnodes::Opaque(Some(Box::new(
        BernoulliSamplerData::default(),
    ))));
}

/// `bernoulli_beginsamplescan(node, params, nparams, seed)` — examine parameters
/// and prepare for a sample scan. `nparams` unused (as in C).
fn bernoulli_beginsamplescan<'mcx>(
    node: &mut SampleScanState<'mcx>,
    params: &[Datum],
    _nparams: i32,
    seed: uint32,
) {
    // double percent = DatumGetFloat4(params[0]);
    let percent: f64 = DatumGetFloat4(params[0]) as f64;

    if percent < 0.0 || percent > 100.0 || percent.is_nan() {
        panic_sample_percentage();
    }

    let sampler = bernoulli_sampler_mut(node);

    // dcutoff = rint(((double) PG_UINT32_MAX + 1) * percent / 100);
    let dcutoff: f64 = (((PG_UINT32_MAX as f64) + 1.0) * percent / 100.0).round_ties_even();
    sampler.cutoff = dcutoff as u64;
    sampler.seed = seed;
    sampler.lt = InvalidOffsetNumber;

    // Use bulkread, since we're scanning all pages.  But pagemode visibility
    // checking is a win only at larger sampling fractions.  The 25% cutoff here
    // is based on very limited experimentation.
    // node->use_bulkread = true;
    // node->use_pagemode = (percent >= 25);
    node.use_bulkread = true;
    node.use_pagemode = percent >= 25.0;
}

/// `bernoulli_nextsampletuple(node, blockno, maxoffset)` — select next sampled
/// tuple in current block (per-tuple coin flip).
fn bernoulli_nextsampletuple<'mcx>(
    node: &mut SampleScanState<'mcx>,
    blockno: BlockNumber,
    maxoffset: OffsetNumber,
) -> uint16 {
    let sampler = bernoulli_sampler_mut(node);
    // OffsetNumber tupoffset = sampler->lt;
    let mut tupoffset: OffsetNumber = sampler.lt;
    // uint32 hashinput[3];
    let mut hashinput: [uint32; 3] = [0; 3];

    // Advance to first/next tuple in block.
    if tupoffset == InvalidOffsetNumber {
        tupoffset = FirstOffsetNumber;
    } else {
        tupoffset += 1;
    }

    // These words in the hash input are the same throughout the block:
    // hashinput[0] = blockno;
    // hashinput[2] = sampler->seed;
    hashinput[0] = blockno;
    hashinput[2] = sampler.seed;

    // for (; tupoffset <= maxoffset; tupoffset++)
    while tupoffset <= maxoffset {
        // hashinput[1] = tupoffset;
        hashinput[1] = tupoffset as uint32;

        // hash = DatumGetUInt32(hash_any(hashinput, sizeof(hashinput)));
        let hash: uint32 = hash_any_u32_array3(&hashinput);
        // if (hash < sampler->cutoff) break;
        if (hash as u64) < sampler.cutoff {
            break;
        }
        tupoffset += 1;
    }

    if tupoffset > maxoffset {
        tupoffset = InvalidOffsetNumber;
    }

    // sampler->lt = tupoffset;
    sampler.lt = tupoffset;

    tupoffset
}

// ===========================================================================
// SampleScanGetSampleSize vtable shims.
//
// The landed `types-samplescan` `SampleScanGetSampleSize_function` ABI takes
// `paramexprs: Vec<Node<'static>>` (tag-only nodes) and out-params by value, with
// neither an `Mcx` nor an error channel. A tag-only Node can never present as a
// non-null Const, and there is no Mcx to run the constant-folder, so this shim
// takes exactly C's default `else` branch (`samplefract = 0.1`) and reproduces
// the rest of the body. The constant-folding path is the real ported body
// (`{system,bernoulli}_samplescangetsamplesize`), reached by the planner over
// the `tsm_get_sample_size` seam once the RTE/Mcx owner lands.
// ===========================================================================

fn system_samplescangetsamplesize_cb(
    _root: Option<Box<PlannerInfo>>,
    baserel: Option<Box<RelOptInfo>>,
    _paramexprs: Vec<Node<'static>>,
    pages: &mut BlockNumber,
    tuples: &mut f64,
) {
    let Some(baserel) = baserel else {
        return;
    };
    // C's default `else`: samplefract = 0.1 (no usable non-null Const).
    let samplefract: f32 = 0.1f32;
    *pages = clamp_row_est((baserel.pages as f32 * samplefract) as f64) as BlockNumber;
    *tuples = clamp_row_est(baserel.tuples * samplefract as Cardinality);
}

fn bernoulli_samplescangetsamplesize_cb(
    _root: Option<Box<PlannerInfo>>,
    baserel: Option<Box<RelOptInfo>>,
    _paramexprs: Vec<Node<'static>>,
    pages: &mut BlockNumber,
    tuples: &mut f64,
) {
    let Some(baserel) = baserel else {
        return;
    };
    let samplefract: f32 = 0.1f32;
    // We'll visit all pages of the baserel.
    *pages = baserel.pages;
    *tuples = clamp_row_est(baserel.tuples * samplefract as Cardinality);
}

/// The `ereport(ERROR, ERRCODE_INVALID_TABLESAMPLE_ARGUMENT, "sample percentage
/// must be between 0 and 100")` raised by `{system,bernoulli}_beginsamplescan`.
/// The TsmRoutine `BeginSampleScan` callback ABI (`types-samplescan`) carries no
/// error channel, so the error surfaces as a panic carrying the exact message —
/// the same loud-failure convention the vtable callbacks must follow.
fn panic_sample_percentage() -> ! {
    // Build the PgError so its message/SQLSTATE match the C ereport exactly,
    // then panic with it (the callback ABI cannot return it).
    let err = PgError::error("sample percentage must be between 0 and 100")
        .with_sqlstate(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT);
    panic!("{err}");
}

// ===========================================================================
// tablesample.c — GetTsmRoutine registry.
// ===========================================================================

/// `TsmRoutine *GetTsmRoutine(Oid tsmhandler)` (tablesample.c) — get a
/// [`TsmRoutine`] by invoking the handler.
///
/// C performs `datum = OidFunctionCall1(tsmhandler, NULL)` (fmgr dispatch of the
/// handler function) then validates `routine != NULL && IsA(routine,
/// TsmRoutine)`, `elog(ERROR)` otherwise. The in-tree handlers
/// (`tsm_system_handler` OID 3314, `tsm_bernoulli_handler` OID 3313) are C
/// built-ins; their fmgr dispatch is reproduced here by mapping the handler OID
/// to the ported handler function. An unknown handler OID is the C
/// `routine == NULL` case (a non-handler / non-TsmRoutine-returning function),
/// reported with the exact C error message. The routine is allocated in `mcx`
/// (the caller's context, matching the C result lifetime).
pub fn GetTsmRoutine<'mcx>(mcx: Mcx<'mcx>, tsmhandler: Oid) -> PgResult<PgBox<'mcx, TsmRoutine>> {
    // datum = OidFunctionCall1(tsmhandler, PointerGetDatum(NULL));
    // routine = (TsmRoutine *) DatumGetPointer(datum);
    let routine = match tsmhandler {
        F_TSM_SYSTEM_HANDLER => Some(tsm_system_handler()),
        F_TSM_BERNOULLI_HANDLER => Some(tsm_bernoulli_handler()),
        _ => None,
    };

    // if (routine == NULL || !IsA(routine, TsmRoutine))
    //     elog(ERROR, "tablesample handler function %u did not return a
    //                  TsmRoutine struct", tsmhandler);
    match routine {
        Some(routine) if routine.type_ == T_TsmRoutine => mcx::alloc_in(mcx, routine),
        _ => Err(PgError::error(alloc::format!(
            "tablesample handler function {tsmhandler} did not return a TsmRoutine struct"
        ))),
    }
}

// ===========================================================================
// Seam installation — the executor (nodeSamplescan) reaches the routine + its
// callbacks through `backend-executor-nodeSamplescan-seams`. This crate owns and
// installs those slots.
// ===========================================================================

/// Install the tablesample-method seams consumed by `nodeSamplescan`:
/// `get_tsm_routine_oid` (the `GetTsmRoutine` registry) and the `tsm_*` vtable
/// dispatch wrappers. Wired into `seams-init::init_all`.
pub fn init_seams() {
    // get_tsm_routine_oid(mcx, handler_oid) — GetTsmRoutine(handler_oid).
    seam::get_tsm_routine_oid::set(|mcx, handler_oid| GetTsmRoutine(mcx, handler_oid));

    // tsm->InitSampleScan != NULL
    seam::tsm_has_init_sample_scan::set(|scanstate| {
        Ok(tsmroutine(scanstate)?.InitSampleScan.is_some())
    });
    // tsm->InitSampleScan(scanstate, eflags)
    seam::tsm_init_sample_scan::set(|scanstate, eflags| {
        let f = tsmroutine(scanstate)?
            .InitSampleScan
            .expect("tsm_init_sample_scan called but tsm->InitSampleScan is NULL");
        f(scanstate, eflags);
        Ok(())
    });
    // tsm->BeginSampleScan(scanstate, params, nparams, seed)
    seam::tsm_begin_sample_scan::set(|scanstate, params, seed| {
        let f = tsmroutine(scanstate)?
            .BeginSampleScan
            .expect("tsm_begin_sample_scan called but tsm->BeginSampleScan is NULL");
        let nparams = params.len() as i32;
        // `params` borrows from the seam call; copy into an owned buffer so the
        // callback's `&mut scanstate` borrow does not overlap it.
        let owned: Vec<Datum> = params.to_vec();
        f(scanstate, &owned, nparams, seed);
        Ok(())
    });
    // tsm->NextSampleBlock != NULL (the allow_sync test)
    seam::tsm_has_next_sample_block::set(|scanstate| {
        Ok(tsmroutine(scanstate)?.NextSampleBlock.is_some())
    });
    // tsm->EndSampleScan != NULL
    seam::tsm_has_end_sample_scan::set(|node| Ok(tsmroutine(node)?.EndSampleScan.is_some()));
    // tsm->EndSampleScan(node)
    seam::tsm_end_sample_scan::set(|node| {
        let f = tsmroutine(node)?
            .EndSampleScan
            .expect("tsm_end_sample_scan called but tsm->EndSampleScan is NULL");
        f(node);
        Ok(())
    });
}

/// `node->tsmroutine` — the routine resolved by `GetTsmRoutine` in
/// `ExecInitSampleScan`. A missing routine is a node-setup bug.
fn tsmroutine<'a, 'mcx>(
    node: &'a SampleScanState<'mcx>,
) -> PgResult<&'a TsmRoutine> {
    Ok(node
        .tsmroutine
        .as_ref()
        .expect("tablesample dispatch: node->tsmroutine is NULL")
        .as_ref())
}

#[cfg(test)]
mod tests;
