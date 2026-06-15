//! Generalized routines for temporary tuple storage.
//!
//! Port of `src/backend/utils/sort/tuplestore.c` from PostgreSQL 18.3.
//!
//! Temporary storage of tuples for Materialize nodes, CTE/recursive-union work
//! tables, table-function scans, etc. A dumbed-down `tuplesort.c`: it does no
//! sorting, only stores and regurgitates a sequence of tuples. Tuples stay in
//! an in-memory array (`memtuples`) while the `work_mem` budget is not
//! exceeded; once exceeded they spill to a `BufFile` temp file.
//!
//! # Representation vs. the C source
//!
//! `Tuplestorestate` is opaque in C (private to `tuplestore.c`), threaded
//! everywhere as a `Tuplestorestate *`. The owned model carries it as the
//! type-erased [`types_nodes::Tuplestorestate`] carrier; the concrete engine
//! state ([`TuplestorestateState`]) lives inside it, downcast here. The C
//! `void **memtuples` (array of `palloc`'d `MinimalTuple` pointers) becomes a
//! `PgVec<Option<PgVec<u8>>>` of flat `MinimalTuple` blobs (the byte-for-byte
//! C-ABI image, `t_len` first; `None` is a slot a `tuplestore_trim` released,
//! exactly as C nulls out the released pointers). The on-disk byte format is
//! preserved exactly (`writetup_heap` writes a leading 4-byte length word, the
//! body, then if `backward` a trailing length word).
//!
//! The engine and its working memory live in a self-owned [`McxOwned`] bundle
//! (the C `state->context` captured at `tuplestore_begin_common`): every blob
//! is charged to that context, and the spill decision is `ctx.used() > limit`,
//! mirroring the C `LACKMEM()` budget against `allowedMem`.
//!
//! # Seams
//!
//! `BufFile` temp-file I/O (`storage/file/buffile.c`), the executor's slot
//! routines (`executor/execTuples.c`), and `PrepareTempTablespaces`
//! (`commands/tablespace.c`) cross owner seams; `MinimalTuple` construction
//! (`access/common/heaptuple.c` + the flat codec) is called directly.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::collapsible_if)]

use backend_utils_error::{elog, ereport};
use mcx::{Mcx, McxOwned, MemoryContext, PgBox, PgVec};
use types_error::{PgError, PgResult, ERROR};
use types_nodes::nodehashjoin::BufFile;
use types_nodes::{EStateData, SlotId};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::TupleDescData;

use backend_access_common_heaptuple::flat;
use backend_access_common_heaptuple as heaptuple;
use backend_commands_tablespace_seams as tablespace;
use backend_executor_execTuples_seams as execTuples;
use backend_storage_file_buffile_seams as buffile;
use backend_tcop_postgres_seams as postgres;

/// `EXEC_FLAG_REWIND` (executor.h): caller may rewind and re-read.
const EXEC_FLAG_REWIND: i32 = 0x0004;
/// `EXEC_FLAG_BACKWARD` (executor.h): caller may read backward.
const EXEC_FLAG_BACKWARD: i32 = 0x0008;

/// `SEEK_SET` / `SEEK_CUR` (whence values passed to `BufFileSeek`).
const SEEK_SET: i32 = 0;
const SEEK_CUR: i32 = 1;

/// `offsetof(MinimalTupleData, t_infomask2)` — the flat-blob data offset
/// (`t_len` (4) + `mt_padding` (6)).
const MINIMAL_TUPLE_DATA_OFFSET: usize = 10;

/// `sizeof(unsigned int)` — the on-tape length word.
const LEN_WORD_SIZE: usize = 4;

/// `sizeof(void *)` — the C `memtuples` array element size.
const POINTER_SIZE: usize = 8;

/// `ALLOCSET_SEPARATE_THRESHOLD` (memutils.h) — startup `memtuples` sizing.
const ALLOCSET_SEPARATE_THRESHOLD: usize = 8192;

/// Read the `t_len` (total length) out of a flat `MinimalTuple` blob's first
/// four bytes — `((MinimalTuple) tuple)->t_len` in C.
#[inline]
fn blob_t_len(blob: &[u8]) -> u32 {
    u32::from_ne_bytes([blob[0], blob[1], blob[2], blob[3]])
}

/// `TupStoreStatus` (tuplestore.c) — persistent states between calls.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TupStoreStatus {
    /// Tuples still fit in memory.
    TSS_INMEM,
    /// Writing to temp file.
    TSS_WRITEFILE,
    /// Reading from temp file.
    TSS_READFILE,
}
use TupStoreStatus::{TSS_INMEM, TSS_READFILE, TSS_WRITEFILE};

/// `TSReadPointer` (tuplestore.c) — state for a single read pointer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TSReadPointer {
    eflags: i32,
    eof_reached: bool,
    current: i32,
    file: i32,
    offset: i64,
}

/// `copytup`/`writetup`/`readtup` trio. Only the heap (MinimalTuple) variant
/// exists, exactly as in C.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TupleMethods {
    Heap,
}

/// The private engine state (`struct Tuplestorestate`).
pub struct TuplestorestateState<'mcx> {
    status: TupStoreStatus,
    eflags: i32,
    backward: bool,
    interXact: bool,
    truncated: bool,
    usedDisk: bool,
    maxSpace: i64,
    tuples: i64,
    myfile: Option<PgBox<'mcx, BufFile>>,
    #[allow(dead_code)]
    methods: TupleMethods,

    /// Array of stored flat-blob tuples (state INMEM); `None` slots are
    /// trimmed. `memtupcount` includes the deleted slots.
    memtuples: PgVec<'mcx, Option<PgVec<'mcx, u8>>>,
    memtupdeleted: i32,
    memtupcount: i32,

    readptrs: Vec<TSReadPointer>,
    activeptr: i32,
    readptrcount: i32,
    readptrsize: i32,

    writepos_file: i32,
    writepos_offset: i64,
}

mcx::bind!(pub TuplestorestateBind => TuplestorestateState<'mcx>);
/// The self-owned engine bundle (context + state); stored type-erased in the
/// [`types_nodes::Tuplestorestate`] carrier.
pub type OwnedStore = McxOwned<TuplestorestateBind>;

impl<'mcx> TuplestorestateState<'mcx> {
    /// The store's own context handle (`state->context`), recovered from a
    /// charged member so allocations land in the engine bundle's context with
    /// the correct `'mcx` (matching `myfile`/`memtuples`).
    #[inline]
    fn mcx(&self) -> Mcx<'mcx> {
        *self.memtuples.allocator()
    }

    #[inline]
    fn readptr(&mut self, idx: i32) -> &mut TSReadPointer {
        &mut self.readptrs[idx as usize]
    }
    #[inline]
    fn readptr_ref(&self, idx: i32) -> &TSReadPointer {
        &self.readptrs[idx as usize]
    }
    #[inline]
    fn file(&mut self) -> PgResult<&mut BufFile> {
        self.myfile
            .as_deref_mut()
            .ok_or_else(|| PgError::error("tuplestore: temp file not open"))
    }
}

// ----------------------------------------------------------------------------
// Carrier helpers
// ----------------------------------------------------------------------------

/// Downcast the carrier's type-erased payload to the engine bundle, running
/// `f` against its state and owning context.
fn with_store<R>(
    carrier: &mut types_nodes::Tuplestorestate<'_>,
    f: impl for<'mcx> FnOnce(&mut TuplestorestateState<'mcx>, &MemoryContext) -> R,
) -> R {
    let any = carrier
        .payload_mut()
        .expect("tuplestore: operation on a NULL Tuplestorestate");
    let owned = any
        .downcast_mut::<OwnedStore>()
        .expect("tuplestore: carrier payload is not this unit's engine");
    // `with_mut` gives `&mut state`; `context()` borrows the same bundle, so
    // capture the context's accounting view separately first.
    //
    // SAFETY of the split: `with_mut` and `context()` borrow disjoint fields of
    // the bundle, but Rust cannot see that through the closure boundary, so we
    // briefly re-borrow the context immutably (its accounting uses `Cell`).
    owned.with_mut(|state| {
        // The context handle is reconstructed from one of the state's vecs (all
        // charged to the bundle context), giving the accounting view
        // (`used()`/`limit()`) without re-borrowing the bundle.
        let ctx: &MemoryContext = state.memtuples.allocator().context();
        f(state, ctx)
    })
}

/// Immutable [`with_store`] for the read-only seams.
fn with_store_ref<R>(
    carrier: &types_nodes::Tuplestorestate<'_>,
    f: impl for<'mcx> FnOnce(&TuplestorestateState<'mcx>) -> R,
) -> R {
    let any = carrier
        .payload()
        .expect("tuplestore: operation on a NULL Tuplestorestate");
    let owned = any
        .downcast_ref::<OwnedStore>()
        .expect("tuplestore: carrier payload is not this unit's engine");
    owned.with(|state| f(state))
}

// ----------------------------------------------------------------------------
// tuplestore_begin_xxx
// ----------------------------------------------------------------------------

fn tuplestore_begin_common<'mcx>(
    mcx: Mcx<'mcx>,
    eflags: i32,
    interXact: bool,
    maxKBytes: i32,
) -> PgResult<types_nodes::Tuplestorestate<'mcx>> {
    let limit = if maxKBytes > 0 {
        maxKBytes as usize * 1024
    } else {
        0
    };

    let owned = OwnedStore::try_new(MemoryContext::new("tuplestore").with_limit(limit), |sx| {
        let memtupsize = core::cmp::max(
            16384 / POINTER_SIZE,
            ALLOCSET_SEPARATE_THRESHOLD / POINTER_SIZE + 1,
        );
        let mut memtuples: PgVec<Option<PgVec<u8>>> = PgVec::new_in(sx);
        memtuples
            .try_reserve(memtupsize)
            .map_err(|_| oom("tuplestore memtuples"))?;

        let readptrsize: i32 = 8;
        let mut readptrs: Vec<TSReadPointer> = Vec::new();
        readptrs
            .try_reserve(readptrsize as usize)
            .map_err(|_| oom("tuplestore readptrs"))?;
        readptrs.push(TSReadPointer {
            eflags,
            eof_reached: false,
            current: 0,
            file: 0,
            offset: 0,
        });

        Ok(TuplestorestateState {
            status: TSS_INMEM,
            eflags,
            backward: false,
            interXact,
            truncated: false,
            usedDisk: false,
            maxSpace: 0,
            tuples: 0,
            myfile: None,
            methods: TupleMethods::Heap,
            memtuples,
            memtupdeleted: 0,
            memtupcount: 0,
            readptrs,
            activeptr: 0,
            readptrcount: 1,
            readptrsize,
            writepos_file: 0,
            writepos_offset: 0,
        })
    })?;

    types_nodes::Tuplestorestate::begin(mcx, owned)
}

/// `tuplestore_begin_heap(randomAccess, interXact, maxKBytes)`.
pub fn tuplestore_begin_heap<'mcx>(
    mcx: Mcx<'mcx>,
    randomAccess: bool,
    interXact: bool,
    maxKBytes: i32,
) -> PgResult<PgBox<'mcx, types_nodes::Tuplestorestate<'mcx>>> {
    let eflags = if randomAccess {
        EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND
    } else {
        EXEC_FLAG_REWIND
    };
    let carrier = tuplestore_begin_common(mcx, eflags, interXact, maxKBytes)?;
    mcx::alloc_in(mcx, carrier)
}

/// `tuplestore_set_eflags(state, eflags)`.
pub fn tuplestore_set_eflags(
    carrier: &mut types_nodes::Tuplestorestate<'_>,
    eflags: i32,
) -> PgResult<()> {
    with_store(carrier, |state, _ctx| {
        if state.status != TSS_INMEM || state.memtupcount != 0 {
            return elog(ERROR, "too late to call tuplestore_set_eflags");
        }
        state.readptr(0).eflags = eflags;
        let mut eflags = eflags;
        for i in 1..state.readptrcount {
            eflags |= state.readptr(i).eflags;
        }
        state.eflags = eflags;
        Ok(())
    })
}

/// `tuplestore_alloc_read_pointer(state, eflags)`.
pub fn tuplestore_alloc_read_pointer(
    carrier: &mut types_nodes::Tuplestorestate<'_>,
    eflags: i32,
) -> PgResult<i32> {
    with_store(carrier, |state, _ctx| {
        if state.status != TSS_INMEM || state.memtupcount != 0 {
            if (state.eflags | eflags) != state.eflags {
                return Err(elog_err("too late to require new tuplestore eflags"));
            }
        }

        if state.readptrcount >= state.readptrsize {
            let newcnt = state.readptrsize * 2;
            let extra = (newcnt - state.readptrsize) as usize;
            state
                .readptrs
                .try_reserve(extra)
                .map_err(|_| oom("tuplestore readptrs"))?;
            state.readptrsize = newcnt;
        }

        let mut newptr = *state.readptr(0);
        newptr.eflags = eflags;
        state.readptrs.push(newptr);
        state.eflags |= eflags;

        let result = state.readptrcount;
        state.readptrcount += 1;
        Ok(result)
    })
}

/// `tuplestore_clear(state)`.
pub fn tuplestore_clear(carrier: &mut types_nodes::Tuplestorestate<'_>) {
    let r: PgResult<()> = with_store(carrier, |state, ctx| {
        tuplestore_updatemax(state, ctx)?;
        if let Some(file) = state.myfile.take() {
            buffile::buf_file_close::call(file)?;
        }
        state.memtuples.clear();
        state.status = TSS_INMEM;
        state.truncated = false;
        state.memtupdeleted = 0;
        state.memtupcount = 0;
        state.tuples = 0;
        for i in 0..state.readptrcount {
            let rp = state.readptr(i);
            rp.eof_reached = false;
            rp.current = 0;
        }
        Ok(())
    });
    // tuplestore_clear is `void` in C; BufFileClose does not ereport(ERROR).
    r.expect("tuplestore_clear: BufFileClose failed");
}

/// `tuplestore_end(state)`: release resources. Consumes the carrier.
pub fn tuplestore_end(mut carrier: PgBox<'_, types_nodes::Tuplestorestate<'_>>) {
    let r: PgResult<()> = with_store(&mut carrier, |state, _ctx| {
        if let Some(file) = state.myfile.take() {
            buffile::buf_file_close::call(file)?;
        }
        Ok(())
    });
    r.expect("tuplestore_end: BufFileClose failed");
    // Dropping `carrier` drops the engine bundle (context + state), the
    // MemoryContextDelete(state->context) + pfree's analog.
    drop(carrier);
}

/// `tuplestore_select_read_pointer(state, ptr)`.
pub fn tuplestore_select_read_pointer(
    carrier: &mut types_nodes::Tuplestorestate<'_>,
    ptr: i32,
) -> PgResult<()> {
    with_store(carrier, |state, _ctx| select_read_pointer(state, ptr))
}

fn select_read_pointer(state: &mut TuplestorestateState<'_>, ptr: i32) -> PgResult<()> {
    debug_assert!(ptr >= 0 && ptr < state.readptrcount);

    if ptr == state.activeptr {
        return Ok(());
    }
    let activeptr = state.activeptr;

    match state.status {
        TSS_INMEM | TSS_WRITEFILE => {}
        TSS_READFILE => {
            if !state.readptr(activeptr).eof_reached {
                let (file, offset) = buffile::buf_file_tell::call(state.file()?);
                let oldptr = state.readptr(activeptr);
                oldptr.file = file;
                oldptr.offset = offset;
            }
            if state.readptr(ptr).eof_reached {
                let (wf, wo) = (state.writepos_file, state.writepos_offset);
                seek(state.file()?, wf, wo, SEEK_SET)?;
            } else {
                let (file, offset) = {
                    let rp = state.readptr(ptr);
                    (rp.file, rp.offset)
                };
                seek(state.file()?, file, offset, SEEK_SET)?;
            }
        }
    }
    state.activeptr = ptr;
    Ok(())
}

/// `tuplestore_tuple_count(state)`.
pub fn tuplestore_tuple_count(carrier: &mut types_nodes::Tuplestorestate<'_>) -> i64 {
    with_store(carrier, |state, _ctx| state.tuples)
}

/// `tuplestore_ateof(state)`.
pub fn tuplestore_ateof(carrier: &types_nodes::Tuplestorestate<'_>) -> bool {
    with_store_ref(carrier, |state| {
        state.readptr_ref(state.activeptr).eof_reached
    })
}

// ----------------------------------------------------------------------------
// puttuple family
// ----------------------------------------------------------------------------

/// `tuplestore_puttupleslot(state, slot)`: collect data from a slot. The
/// MinimalTuple is formed from the live slot in `mcx` (the caller's
/// `es_query_cxt`) via the execTuples seam, then copied into the store.
pub fn tuplestore_puttupleslot<'mcx>(
    carrier: &mut types_nodes::Tuplestorestate<'mcx>,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let tuple = execTuples::exec_copy_slot_minimal_tuple::call(mcx, estate, slot)?;
    let blob = flat::minimal_tuple_to_flat(mcx, &tuple).map_err(flat_err)?;
    tuplestore_puttuple_common(carrier, &blob)
}

/// `tuplestore_putvalues(state, tdesc, values, isnull)`: form a MinimalTuple
/// from the value/null arrays and append it.
pub fn tuplestore_putvalues(
    carrier: &mut types_nodes::Tuplestorestate<'_>,
    tdesc: &TupleDescData<'_>,
    values: &[Datum<'_>],
    isnull: &[bool],
) -> PgResult<()> {
    // The tuple is formed into the store's own context (where the blob lives).
    with_store(carrier, |state, ctx| {
        let mcx = state.mcx();
        let blob =
            flat::heap_form_minimal_tuple_flat(mcx, tdesc, values, isnull).map_err(heap_err)?;
        puttuple_common_inner(state, ctx, &blob)
    })
}

/// `tuplestore_puttuple_common(state, tuple)` over a flat blob already formed
/// in the caller's context. Copies the blob into the store's context.
fn tuplestore_puttuple_common(
    carrier: &mut types_nodes::Tuplestorestate<'_>,
    tuple: &[u8],
) -> PgResult<()> {
    with_store(carrier, |state, ctx| puttuple_common_inner(state, ctx, tuple))
}

fn puttuple_common_inner(
    state: &mut TuplestorestateState<'_>,
    ctx: &MemoryContext,
    tuple: &[u8],
) -> PgResult<()> {
    state.tuples += 1;

    match state.status {
        TSS_INMEM => {
            for i in 0..state.readptrcount {
                let activeptr = state.activeptr;
                let memtupcount = state.memtupcount;
                let rp = state.readptr(i);
                if rp.eof_reached && i != activeptr {
                    rp.eof_reached = false;
                    rp.current = memtupcount;
                }
            }

            let mcx = state.mcx();
            let blob = mcx::slice_in(mcx, tuple).map_err(|_| oom("tuplestore puttuple"))?;
            state.memtuples.push(Some(blob));
            state.memtupcount += 1;

            if !over_limit(ctx) {
                return Ok(());
            }

            // Spill: switch to tape-based operation.
            tablespace::prepare_temp_tablespaces::call()?;
            let interXact = state.interXact;
            state.myfile = Some(buffile::buf_file_create_temp::call(mcx, interXact)?);
            state.backward = (state.eflags & EXEC_FLAG_BACKWARD) != 0;
            tuplestore_updatemax(state, ctx)?;
            state.status = TSS_WRITEFILE;
            dumptuples(state)?;
        }
        TSS_WRITEFILE => {
            for i in 0..state.readptrcount {
                let activeptr = state.activeptr;
                if state.readptr(i).eof_reached && i != activeptr {
                    let (file, offset) = buffile::buf_file_tell::call(state.file()?);
                    let rp = state.readptr(i);
                    rp.eof_reached = false;
                    rp.file = file;
                    rp.offset = offset;
                }
            }
            writetup_heap(state, tuple)?;
        }
        TSS_READFILE => {
            let activeptr = state.activeptr;
            if !state.readptr(activeptr).eof_reached {
                let (file, offset) = buffile::buf_file_tell::call(state.file()?);
                let rp = state.readptr(activeptr);
                rp.file = file;
                rp.offset = offset;
            }
            let (wf, wo) = (state.writepos_file, state.writepos_offset);
            seek(state.file()?, wf, wo, SEEK_SET)?;
            state.status = TSS_WRITEFILE;

            for i in 0..state.readptrcount {
                let activeptr = state.activeptr;
                let wf = state.writepos_file;
                let wo = state.writepos_offset;
                let rp = state.readptr(i);
                if rp.eof_reached && i != activeptr {
                    rp.eof_reached = false;
                    rp.file = wf;
                    rp.offset = wo;
                }
            }
            writetup_heap(state, tuple)?;
        }
    }
    Ok(())
}

// ----------------------------------------------------------------------------
// gettuple family
// ----------------------------------------------------------------------------

/// `tuplestore_gettuple(state, forward, &should_free)`. Returns the fetched
/// flat blob (allocated in `mcx`, the caller's context) and `should_free`.
fn tuplestore_gettuple<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut TuplestorestateState<'_>,
    forward: bool,
) -> PgResult<Option<(PgVec<'mcx, u8>, bool)>> {
    let activeptr = state.activeptr;
    debug_assert!(forward || (state.readptr(activeptr).eflags & EXEC_FLAG_BACKWARD) != 0);

    match state.status {
        TSS_INMEM => {
            let memtupcount = state.memtupcount;
            let memtupdeleted = state.memtupdeleted;
            let truncated = state.truncated;
            if forward {
                if state.readptr(activeptr).eof_reached {
                    return Ok(None);
                }
                if state.readptr(activeptr).current < memtupcount {
                    let cur = state.readptr(activeptr).current;
                    state.readptr(activeptr).current = cur + 1;
                    let blob = inmem_blob(mcx, state, cur)?;
                    return Ok(Some((blob, false)));
                }
                state.readptr(activeptr).eof_reached = true;
                Ok(None)
            } else {
                let rp = state.readptr(activeptr);
                if rp.eof_reached {
                    rp.current = memtupcount;
                    rp.eof_reached = false;
                } else {
                    if rp.current <= memtupdeleted {
                        debug_assert!(!truncated);
                        return Ok(None);
                    }
                    rp.current -= 1;
                }
                let cur = state.readptr(activeptr).current;
                if cur <= memtupdeleted {
                    debug_assert!(!truncated);
                    return Ok(None);
                }
                let blob = inmem_blob(mcx, state, cur - 1)?;
                Ok(Some((blob, false)))
            }
        }
        TSS_WRITEFILE | TSS_READFILE => {
            if state.status == TSS_WRITEFILE {
                if state.readptr(activeptr).eof_reached && forward {
                    return Ok(None);
                }
                let (wf, wo) = buffile::buf_file_tell::call(state.file()?);
                state.writepos_file = wf;
                state.writepos_offset = wo;
                if !state.readptr(activeptr).eof_reached {
                    let (file, offset) = {
                        let rp = state.readptr(activeptr);
                        (rp.file, rp.offset)
                    };
                    seek(state.file()?, file, offset, SEEK_SET)?;
                }
                state.status = TSS_READFILE;
            }

            // TSS_READFILE — should_free = true.
            if forward {
                let tuplen = getlen(state, true)?;
                if tuplen != 0 {
                    let tup = readtup_heap(mcx, state, tuplen)?;
                    return Ok(Some((tup, true)));
                } else {
                    state.readptr(activeptr).eof_reached = true;
                    return Ok(None);
                }
            }

            // Backward.
            if seek_ok(state.file()?, 0, -(LEN_WORD_SIZE as i64), SEEK_CUR)? != 0 {
                state.readptr(activeptr).eof_reached = false;
                debug_assert!(!state.truncated);
                return Ok(None);
            }
            let mut tuplen = getlen(state, false)?;

            if state.readptr(activeptr).eof_reached {
                state.readptr(activeptr).eof_reached = false;
            } else {
                if seek_ok(
                    state.file()?,
                    0,
                    -((tuplen as i64) + 2 * LEN_WORD_SIZE as i64),
                    SEEK_CUR,
                )? != 0
                {
                    seek(
                        state.file()?,
                        0,
                        -((tuplen as i64) + LEN_WORD_SIZE as i64),
                        SEEK_CUR,
                    )?;
                    debug_assert!(!state.truncated);
                    return Ok(None);
                }
                tuplen = getlen(state, false)?;
            }

            seek(state.file()?, 0, -(tuplen as i64), SEEK_CUR)?;
            let tup = readtup_heap(mcx, state, tuplen)?;
            Ok(Some((tup, true)))
        }
    }
}

/// Copy the live in-memory blob at array index `idx` into `mcx` (C returns the
/// borrowed pointer; we copy so the caller never aliases the store).
fn inmem_blob<'mcx>(
    mcx: Mcx<'mcx>,
    state: &TuplestorestateState<'_>,
    idx: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let src = state.memtuples[idx as usize]
        .as_ref()
        .ok_or_else(|| PgError::error("tuplestore: read of released in-memory tuple"))?;
    mcx::slice_in(mcx, src).map_err(|_| oom("tuplestore gettuple"))
}

/// `tuplestore_gettupleslot(state, forward, copy, slot)`: fetch a MinimalTuple
/// into the pool slot resolved against `estate`.
pub fn tuplestore_gettupleslot<'mcx>(
    carrier: &mut types_nodes::Tuplestorestate<'mcx>,
    forward: bool,
    copy: bool,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let got = with_store(carrier, |state, _ctx| tuplestore_gettuple(mcx, state, forward))?;
    match got {
        Some((blob, _should_free)) => {
            // The flat blob is already a fresh `mcx` allocation; `copy` only
            // matters in C for the in-mem borrow, which we already copied.
            let _ = copy;
            let mtup = flat::minimal_tuple_from_flat(mcx, &blob).map_err(flat_err)?;
            // shouldFree = true: the slot takes ownership of the formed tuple.
            execTuples::exec_store_minimal_tuple::call(estate, mtup, slot, true)?;
            Ok(true)
        }
        None => {
            execTuples::exec_clear_tuple_by_id::call(estate, slot)?;
            Ok(false)
        }
    }
}

/// `tuplestore_advance(state, forward)`.
pub fn tuplestore_advance(
    carrier: &mut types_nodes::Tuplestorestate<'_>,
    forward: bool,
) -> PgResult<bool> {
    with_store(carrier, |state, _ctx| {
        let mcx = state.mcx();
        match tuplestore_gettuple(mcx, state, forward)? {
            Some((_blob, _should_free)) => Ok(true),
            None => Ok(false),
        }
    })
}

/// `tuplestore_skiptuples(state, ntuples, forward)`.
pub fn tuplestore_skiptuples(
    carrier: &mut types_nodes::Tuplestorestate<'_>,
    ntuples: i64,
    forward: bool,
) -> PgResult<bool> {
    with_store(carrier, |state, _ctx| {
        let activeptr = state.activeptr;
        debug_assert!(forward || (state.readptr(activeptr).eflags & EXEC_FLAG_BACKWARD) != 0);

        if ntuples <= 0 {
            return Ok(true);
        }

        match state.status {
            TSS_INMEM => {
                let memtupcount = state.memtupcount;
                let memtupdeleted = state.memtupdeleted;
                let truncated = state.truncated;
                let rp = state.readptr(activeptr);
                if forward {
                    if rp.eof_reached {
                        return Ok(false);
                    }
                    if (memtupcount - rp.current) as i64 >= ntuples {
                        rp.current += ntuples as i32;
                        return Ok(true);
                    }
                    rp.current = memtupcount;
                    rp.eof_reached = true;
                    Ok(false)
                } else {
                    let mut ntuples = ntuples;
                    if rp.eof_reached {
                        rp.current = memtupcount;
                        rp.eof_reached = false;
                        ntuples -= 1;
                    }
                    if (rp.current - memtupdeleted) as i64 > ntuples {
                        rp.current -= ntuples as i32;
                        return Ok(true);
                    }
                    debug_assert!(!truncated);
                    state.readptr(activeptr).current = memtupdeleted;
                    Ok(false)
                }
            }
            _ => {
                let mcx = state.mcx();
                let mut ntuples = ntuples;
                while ntuples > 0 {
                    ntuples -= 1;
                    match tuplestore_gettuple(mcx, state, forward)? {
                        None => return Ok(false),
                        Some((_blob, _should_free)) => {}
                    }
                    postgres::check_for_interrupts::call()?;
                }
                Ok(true)
            }
        }
    })
}

/// `dumptuples(state)`: write the in-memory tuples to tape.
fn dumptuples(state: &mut TuplestorestateState<'_>) -> PgResult<()> {
    let mut i = state.memtupdeleted;
    loop {
        for j in 0..state.readptrcount {
            let rp = state.readptr(j);
            if i == rp.current && !rp.eof_reached {
                let (file, offset) = buffile::buf_file_tell::call(state.file()?);
                let rp = state.readptr(j);
                rp.file = file;
                rp.offset = offset;
            }
        }
        if i >= state.memtupcount {
            break;
        }
        let tuple = state.memtuples[i as usize]
            .take()
            .ok_or_else(|| PgError::error("tuplestore: dump of released tuple"))?;
        writetup_heap(state, &tuple)?;
        i += 1;
    }
    state.memtuples.clear();
    state.memtupdeleted = 0;
    state.memtupcount = 0;
    Ok(())
}

/// `tuplestore_rescan(state)`.
pub fn tuplestore_rescan(carrier: &mut types_nodes::Tuplestorestate<'_>) -> PgResult<()> {
    with_store(carrier, |state, _ctx| {
        let activeptr = state.activeptr;
        debug_assert!(state.readptr(activeptr).eflags & EXEC_FLAG_REWIND != 0);
        debug_assert!(!state.truncated);

        match state.status {
            TSS_INMEM => {
                let rp = state.readptr(activeptr);
                rp.eof_reached = false;
                rp.current = 0;
            }
            TSS_WRITEFILE => {
                let rp = state.readptr(activeptr);
                rp.eof_reached = false;
                rp.file = 0;
                rp.offset = 0;
            }
            TSS_READFILE => {
                state.readptr(activeptr).eof_reached = false;
                seek(state.file()?, 0, 0, SEEK_SET)?;
            }
        }
        Ok(())
    })
}

/// `tuplestore_copy_read_pointer(state, srcptr, destptr)`.
pub fn tuplestore_copy_read_pointer(
    carrier: &mut types_nodes::Tuplestorestate<'_>,
    srcptr: i32,
    destptr: i32,
) -> PgResult<()> {
    with_store(carrier, |state, _ctx| {
        debug_assert!(srcptr >= 0 && srcptr < state.readptrcount);
        debug_assert!(destptr >= 0 && destptr < state.readptrcount);

        if srcptr == destptr {
            return Ok(());
        }

        let sptr = *state.readptr(srcptr);
        let dptr_eflags = state.readptr(destptr).eflags;
        if dptr_eflags != sptr.eflags {
            *state.readptr(destptr) = sptr;
            let mut eflags = state.readptr(0).eflags;
            for i in 1..state.readptrcount {
                eflags |= state.readptr(i).eflags;
            }
            state.eflags = eflags;
        } else {
            *state.readptr(destptr) = sptr;
        }

        match state.status {
            TSS_INMEM | TSS_WRITEFILE => {}
            TSS_READFILE => {
                if destptr == state.activeptr {
                    let dptr = *state.readptr(destptr);
                    if dptr.eof_reached {
                        let (wf, wo) = (state.writepos_file, state.writepos_offset);
                        seek(state.file()?, wf, wo, SEEK_SET)?;
                    } else {
                        seek(state.file()?, dptr.file, dptr.offset, SEEK_SET)?;
                    }
                } else if srcptr == state.activeptr {
                    if !state.readptr(destptr).eof_reached {
                        let (file, offset) = buffile::buf_file_tell::call(state.file()?);
                        let dptr = state.readptr(destptr);
                        dptr.file = file;
                        dptr.offset = offset;
                    }
                }
            }
        }
        Ok(())
    })
}

/// `tuplestore_trim(state)`: remove no-longer-needed tuples.
pub fn tuplestore_trim(carrier: &mut types_nodes::Tuplestorestate<'_>) -> PgResult<()> {
    with_store(carrier, |state, ctx| {
        if state.eflags & EXEC_FLAG_REWIND != 0 {
            return Ok(());
        }
        if state.status != TSS_INMEM {
            return Ok(());
        }

        let mut oldest = state.memtupcount;
        for i in 0..state.readptrcount {
            let rp = state.readptr(i);
            if !rp.eof_reached {
                oldest = core::cmp::min(oldest, rp.current);
            }
        }

        let nremove = oldest - 1;
        if nremove <= 0 {
            return Ok(());
        }
        debug_assert!(nremove >= state.memtupdeleted);
        debug_assert!(nremove <= state.memtupcount);

        tuplestore_updatemax(state, ctx)?;

        for i in state.memtupdeleted..nremove {
            state.memtuples[i as usize] = None;
        }
        state.memtupdeleted = nremove;
        state.truncated = true;

        if nremove < state.memtupcount / 8 {
            return Ok(());
        }

        // Slide the array down: drain the leading `nremove` (now-None) slots.
        state.memtuples.drain(0..nremove as usize);
        state.memtupdeleted = 0;
        state.memtupcount -= nremove;
        for i in 0..state.readptrcount {
            let rp = state.readptr(i);
            if !rp.eof_reached {
                rp.current -= nremove;
            }
        }
        Ok(())
    })
}

/// `tuplestore_updatemax(state)`.
fn tuplestore_updatemax(state: &mut TuplestorestateState<'_>, ctx: &MemoryContext) -> PgResult<()> {
    if state.status == TSS_INMEM {
        state.maxSpace = core::cmp::max(state.maxSpace, ctx.used() as i64);
    } else {
        let size = buffile::buf_file_size::call(state.file()?)?;
        state.maxSpace = core::cmp::max(state.maxSpace, size);
        state.usedDisk = true;
    }
    Ok(())
}

/// `tuplestore_in_memory(state)`.
pub fn tuplestore_in_memory(carrier: &types_nodes::Tuplestorestate<'_>) -> bool {
    with_store_ref(carrier, |state| state.status == TSS_INMEM)
}

// ----------------------------------------------------------------------------
// Tape interface routines
// ----------------------------------------------------------------------------

fn getlen(state: &mut TuplestorestateState<'_>, eofOK: bool) -> PgResult<u32> {
    let mut bytes = [0u8; LEN_WORD_SIZE];
    let nbytes = buffile::buf_file_read_maybe_eof::call(state.file()?, &mut bytes, eofOK)?;
    if nbytes == 0 {
        Ok(0)
    } else {
        Ok(u32::from_ne_bytes(bytes))
    }
}

/// `writetup_heap(state, tuple)`: write the flat MinimalTuple to tape.
fn writetup_heap(state: &mut TuplestorestateState<'_>, tuple: &[u8]) -> PgResult<()> {
    let t_len = blob_t_len(tuple) as usize;
    let tupbody = &tuple[MINIMAL_TUPLE_DATA_OFFSET..t_len];
    let tupbodylen = (t_len - MINIMAL_TUPLE_DATA_OFFSET) as u32;
    let tuplen: u32 = tupbodylen + LEN_WORD_SIZE as u32;

    let backward = state.backward;
    let file = state.file()?;
    buffile::buf_file_write::call(file, &tuplen.to_ne_bytes())?;
    buffile::buf_file_write::call(file, tupbody)?;
    if backward {
        buffile::buf_file_write::call(file, &tuplen.to_ne_bytes())?;
    }
    Ok(())
}

/// `readtup_heap(state, len)`: read a flat MinimalTuple back from tape.
fn readtup_heap<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut TuplestorestateState<'_>,
    len: u32,
) -> PgResult<PgVec<'mcx, u8>> {
    let tupbodylen = (len as usize) - LEN_WORD_SIZE;
    let tuplen = tupbodylen + MINIMAL_TUPLE_DATA_OFFSET;

    let mut tuple: PgVec<'mcx, u8> =
        mcx::vec_with_capacity_in(mcx, tuplen).map_err(|_| oom("tuplestore readtup"))?;
    tuple.resize(tuplen, 0);
    tuple[0..4].copy_from_slice(&(tuplen as u32).to_ne_bytes());

    let backward = state.backward;
    let file = state.file()?;
    buffile::buf_file_read_exact::call(
        file,
        &mut tuple[MINIMAL_TUPLE_DATA_OFFSET..MINIMAL_TUPLE_DATA_OFFSET + tupbodylen],
    )?;
    if backward {
        let mut trail = [0u8; LEN_WORD_SIZE];
        buffile::buf_file_read_exact::call(file, &mut trail)?;
    }
    Ok(tuple)
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

/// `BufFileSeek` that ereport(ERROR)s on the C "impossible seek" non-zero
/// return (the tuplestore.c `if (BufFileSeek(...) != 0) ereport(ERROR, ...)`).
fn seek(file: &mut BufFile, fileno: i32, offset: i64, whence: i32) -> PgResult<()> {
    if buffile::buf_file_seek::call(file, fileno, offset, whence)? != 0 {
        return Err(seek_error());
    }
    Ok(())
}

/// `BufFileSeek` returning the C status (0 == success) for the backward-read
/// path that tolerates a failed seek.
fn seek_ok(file: &mut BufFile, fileno: i32, offset: i64, whence: i32) -> PgResult<i32> {
    buffile::buf_file_seek::call(file, fileno, offset, whence)
}

/// Is the store's working-memory budget exceeded? (`LACKMEM()`: limit 0 means
/// unlimited.)
fn over_limit(ctx: &MemoryContext) -> bool {
    let limit = ctx.limit();
    limit != 0 && ctx.used() > limit
}

fn elog_err(msg: &'static str) -> PgError {
    match elog(ERROR, msg) {
        Err(e) => e,
        Ok(()) => unreachable!("elog(ERROR, ..) always returns Err"),
    }
}

fn oom(what: &'static str) -> PgError {
    ereport(ERROR)
        .errcode(types_error::ERRCODE_OUT_OF_MEMORY)
        .errmsg("out of memory")
        .errdetail_internal(what)
        .into_error()
}

fn seek_error() -> PgError {
    const EIO: i32 = 5;
    ereport(ERROR)
        .with_saved_errno(EIO)
        .errcode_for_file_access()
        .errmsg("could not seek in tuplestore temporary file")
        .into_error()
}

/// Map a flat-codec error into a `PgError` (the OOM variant carries its own).
fn flat_err(e: flat::MinimalTupleFlatError) -> PgError {
    match e {
        flat::MinimalTupleFlatError::Pg(err) => err,
        other => PgError::error(alloc_format(other)),
    }
}

fn heap_err(e: heaptuple::HeapTupleError) -> PgError {
    match e {
        heaptuple::HeapTupleError::Pg(err) => err,
        other => PgError::error(format!("tuplestore: heap_form_minimal_tuple: {other:?}")),
    }
}

fn alloc_format(e: flat::MinimalTupleFlatError) -> String {
    format!("tuplestore: corrupt MinimalTuple blob: {e:?}")
}

#[cfg(test)]
mod tests;
